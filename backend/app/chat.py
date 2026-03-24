"""LLM chat: Groq OpenAI-compatible API, grounded on tool/SQL results."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from . import analytics
from .config import GROQ_API_KEY, GROQ_API_URL, GROQ_MODEL
from .db import list_tables, table_schema_summary
from .sql_guard import run_guarded_select


GUARDRAIL_MSG = (
    "This system is designed to answer questions related to the provided Order-to-Cash dataset only."
)

# Returned as chat reply when Groq cannot be reached (matches frontend offline wording).
LLM_UNREACHABLE_MSG = (
    "No internet connection. Check your network and try again."
)


# Named tools = reliable, deterministic paths. The planner may still choose {"intent":"sql"} for ad-hoc questions.
SYSTEM_PLANNER = """You are a planner for an SAP Order-to-Cash analytics API. The user message is the latest question.

Respond with ONE JSON object only (no markdown fences).

1) Off-topic (general knowledge, creative writing, unrelated topics):
{"intent":"off_topic"}

2) Structured tool — use when the question matches (extract IDs from the user text):
{"intent":"tool","tool":"<name>","args":{...}}

Available tools and args:
- trace_billing_flow: {"billing_document":"<id>"} — full path billing→delivery→sales order→journal; journal for a billing/ref doc.
- trace_sales_order: {"sales_order":"<id>"} — SO→deliveries→billing→journal.
- trace_delivery_document: {"delivery_document":"<id>"} — delivery→SO lines→billing→journal.
- journal_for_billing_reference: {"reference_document":"<billing_or_ref_id>"} — journal lines where referenceDocument matches (e.g. billing doc number).
- top_products_by_billing: {} — all materials by billing line count (full ranked list).
- top_material_groups_by_billing: {} — all product groups by billing line count.
- top_customers_by_orders: {} — all customers by order count.
- top_customers_by_billing_revenue: {} — all customers by summed billing net (non-cancelled).
- payments_summary_by_customer: {} — all customers by payment aggregates.
- delivery_lines_by_plant: {} — all plants by delivery line count.
- broken_flows: {} — delivery lines not billed; billing refs missing delivery header.
- billing_without_accounting: {} — all billing headers with no accountingDocument.
- cancelled_billing_documents: {} — all cancelled billing headers.
- open_sales_orders: {} — all sales orders where delivery status is not complete (not 'C').
- sales_pipeline_status_summary: {} — counts of sales orders by delivery/billing header status fields.

3) Read-only SQL (ad-hoc filters, lists, joins not covered above):
{"intent":"sql","sql":"SELECT ..."}

Rules:
- Prefer a tool when it fits; use sql for other data questions (listing rows, specific filters, counts by columns).
- Billing document numbers and delivery document numbers are numeric strings in this dataset.
- If unsure between two tools, pick the more specific trace_* tool when the user gave a document id.
- If the user asks to trace or follow the end-to-end flow for a billing document that must first be found
  (e.g. highest amount, latest, top by net), use {"intent":"sql",...} to SELECT the billing document id(s).
  The server will automatically attach full trace_billing_flow evidence to those rows—do not answer that
  the trace is impossible just because SQL only returned header fields.
- If the question is primarily off-topic, return off_topic even if it mentions "SAP" in passing.
"""


SYSTEM_ANSWER = """You are Dodge AI, a Graph Agent for Order-to-Cash data.

Answer using ONLY the JSON facts in `evidence`. Do not invent IDs, amounts, or entities.
If `billing_flow_traces` is present, each object contains a full billing→delivery→sales order→journal trace
in `path`; use that as the primary source for flow questions. The `rows` field may only identify which
billing document(s) were selected (e.g. highest amount)—that is not insufficient if traces are included.
If evidence is empty or insufficient, say what is missing in one sentence.
You may use Markdown only: **bold**, lists with - or 1., ### headings, and blank lines for paragraphs.
Never use HTML tags (no <br>, <p>, <b>, etc.); use real newlines and Markdown instead.

Never mention graph highlighting, visualization, or the UI. Do not list "nodes" to highlight, do not say
things can be "highlighted" or used to "visualize" a flow — the app does that automatically without being told.
Do not use internal graph node id syntax (e.g. billh:, billi:, delh:, deli:, so:, soi:, je:, bp:, mat:, plant:).
Cite business keys in plain language (e.g. billing document 90504239, accounting document 9400000240).
"""


def _evidence_for_answer(evidence: dict[str, Any]) -> dict[str, Any]:
    """Drop fields the answer model should not echo (highlights are applied in the UI only)."""
    if not isinstance(evidence, dict):
        return evidence
    out = {k: v for k, v in evidence.items() if k != "highlight_node_ids"}
    bft = out.get("billing_flow_traces")
    if isinstance(bft, list):
        cleaned: list[Any] = []
        for t in bft:
            if isinstance(t, dict):
                cleaned.append({k: v for k, v in t.items() if k != "highlight_node_ids"})
            else:
                cleaned.append(t)
        out = {**out, "billing_flow_traces": cleaned}
    return out


def _row_get(row: dict[str, Any], *names: str) -> Any:
    lower = {str(k).lower(): v for k, v in row.items()}
    for n in names:
        v = lower.get(n.lower())
        if v is not None and str(v).strip() != "":
            return v
    return None


def _highlights_from_sql_rows(rows: list[dict[str, Any]]) -> list[str]:
    """Map common query result columns to graph node ids (best-effort)."""
    ids: list[str] = []
    for row in rows:
        m = _row_get(
            row,
            "material",
            "product",
            "material_group",
            "materialgroup",
            "productGroup",
            "productgroup",
        )
        if m:
            ids.append(f"mat:{m}")
        so = _row_get(row, "salesOrder", "salesorder")
        soi = _row_get(row, "salesOrderItem", "salesorderitem")
        if so and soi:
            ni = analytics._norm_item(str(soi))
            if ni:
                ids.append(f"soi:{so}:{ni}")
        elif so:
            ids.append(f"so:{so}")
        bd = _row_get(row, "billingDocument", "billingdocument")
        bdi = _row_get(row, "billingDocumentItem", "billingdocumentitem")
        if bd and bdi:
            ni = analytics._norm_item(str(bdi))
            if ni:
                ids.append(f"billi:{bd}:{ni}")
        elif bd:
            ids.append(f"billh:{bd}")
        dd = _row_get(row, "deliveryDocument", "deliverydocument")
        di = _row_get(row, "deliveryDocumentItem", "deliverydocumentitem")
        if not dd:
            dd = _row_get(row, "referenceSdDocument", "referencesddocument")
        if not di:
            di = _row_get(row, "referenceSdDocumentItem", "referencesddocumentitem")
        if dd and di:
            ni = analytics._norm_item(str(di))
            if ni:
                ids.append(f"deli:{dd}:{ni}")
        elif dd:
            ids.append(f"delh:{dd}")
        cust = _row_get(row, "soldToParty", "soldtoparty", "customer")
        if cust:
            ids.append(f"bp:{cust}")
        pl = _row_get(row, "plant")
        if pl:
            ids.append(f"plant:{pl}")
        cc = _row_get(row, "companyCode", "companycode")
        fy = _row_get(row, "fiscalYear", "fiscalyear")
        ad = _row_get(row, "accountingDocument", "accountingdocument")
        ai = _row_get(row, "accountingDocumentItem", "accountingdocumentitem")
        if all([cc, fy, ad, ai]):
            ids.append(f"je:{cc}:{fy}:{ad}:{ai}")
    return list(dict.fromkeys(ids))


def _rows_tied_for_top_rank(rows: list[dict[str, Any]], *value_keys: str) -> list[dict[str, Any]]:
    """Keep only rows whose numeric metric ties for #1 (handles single winner or many-way ties)."""
    if not rows:
        return []
    if len(value_keys) == 0:
        return rows

    def score(row: dict[str, Any]) -> float | None:
        for key in value_keys:
            v = _row_get(row, key)
            if v is None:
                continue
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
        return None

    scored = [(r, score(r)) for r in rows]
    valid_scores = [s for _, s in scored if s is not None]
    if not valid_scores:
        return rows[:1]
    best = max(valid_scores)
    eps = 1e-8
    return [r for r, s in scored if s is not None and abs(s - best) <= eps]


def _highlights_from_sql_rows_ranked(rows: list[dict[str, Any]]) -> list[str]:
    """Like _highlights_from_sql_rows but for ranked aggregate results, highlight only top-tier ties."""
    if len(rows) <= 1:
        return _highlights_from_sql_rows(rows)
    keys_lower = {str(k).lower() for k in rows[0].keys()}
    if "billing_line_count" in keys_lower and (
        "material" in keys_lower
        or "material_group" in keys_lower
        or "productgroup" in keys_lower
    ):
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "billing_line_count"))
    if "order_count" in keys_lower and "customer" in keys_lower:
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "order_count"))
    if "total_billed_net" in keys_lower and "customer" in keys_lower:
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "total_billed_net"))
    if "sum_txn_amount" in keys_lower and "customer" in keys_lower:
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "sum_txn_amount"))
    if "delivery_line_count" in keys_lower and "plant" in keys_lower:
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "delivery_line_count"))
    return _highlights_from_sql_rows(rows)


def _highlights_for_tool_evidence(tool: str, evidence: Any) -> list[str]:
    """Graph ids for list-style tool results (trace tools already set highlights)."""
    if not isinstance(evidence, dict) or evidence.get("error"):
        return []
    rows = list(evidence.get("rows") or [])

    if tool == "top_products_by_billing":
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "billing_line_count"))
    if tool == "top_material_groups_by_billing":
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "billing_line_count"))
    if tool == "top_customers_by_orders":
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "order_count"))
    if tool == "top_customers_by_billing_revenue":
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "total_billed_net"))
    if tool == "payments_summary_by_customer":
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "sum_txn_amount"))
    if tool == "delivery_lines_by_plant":
        return _highlights_from_sql_rows(_rows_tied_for_top_rank(rows, "delivery_line_count"))
    if tool in ("billing_without_accounting", "cancelled_billing_documents", "open_sales_orders"):
        return _highlights_from_sql_rows(rows)
    if tool == "broken_flows":
        ids: list[str] = []
        for item in evidence.get("delivered_not_billed_lines") or []:
            if not isinstance(item, dict):
                continue
            dd = item.get("deliveryDocument")
            di = analytics._norm_item(item.get("deliveryDocumentItem"))
            if dd and di:
                ids.append(f"deli:{dd}:{di}")
        for item in evidence.get("billed_but_delivery_header_missing") or []:
            if not isinstance(item, dict):
                continue
            bd = item.get("billingDocument")
            bdi = analytics._norm_item(item.get("billingDocumentItem"))
            rd = item.get("referenceSdDocument")
            if bd:
                ids.append(f"billh:{bd}")
            if bd and bdi:
                ids.append(f"billi:{bd}:{bdi}")
            if rd:
                ids.append(f"delh:{rd}")
        return list(dict.fromkeys(ids))
    return []


def _run_tool(conn, tool: str, args: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Execute named tool; return (evidence_dict, highlight_node_ids)."""
    args = args or {}

    if tool == "trace_billing_flow":
        ev = analytics.trace_billing_flow(conn, str(args.get("billing_document", "")))
        return ev, list(ev.get("highlight_node_ids") or [])
    if tool == "trace_sales_order":
        ev = analytics.trace_sales_order(conn, str(args.get("sales_order", "")))
        return ev, list(ev.get("highlight_node_ids") or [])
    if tool == "trace_delivery_document":
        ev = analytics.trace_delivery_document(conn, str(args.get("delivery_document", "")))
        return ev, list(ev.get("highlight_node_ids") or [])
    if tool == "journal_for_billing_reference":
        ev = analytics.journal_for_billing_reference(conn, str(args.get("reference_document", "")))
        return ev, list(ev.get("highlight_node_ids") or [])
    if tool == "top_products_by_billing":
        rows = analytics.top_products_by_billing_count(conn)
        return {"rows": rows}, []
    if tool == "top_material_groups_by_billing":
        rows = analytics.top_material_groups_by_billing_lines(conn)
        return {"rows": rows}, []
    if tool == "top_customers_by_orders":
        rows = analytics.top_customers_by_order_count(conn)
        return {"rows": rows}, []
    if tool == "top_customers_by_billing_revenue":
        rows = analytics.top_customers_by_billing_revenue(conn)
        return {"rows": rows}, []
    if tool == "payments_summary_by_customer":
        rows = analytics.payments_summary_by_customer(conn)
        return {"rows": rows}, []
    if tool == "delivery_lines_by_plant":
        rows = analytics.delivery_lines_by_plant(conn)
        return {"rows": rows}, []
    if tool == "broken_flows":
        return analytics.broken_flows(conn), []
    if tool == "billing_without_accounting":
        rows = analytics.billing_without_accounting(conn)
        return {"rows": rows}, []
    if tool == "cancelled_billing_documents":
        rows = analytics.cancelled_billing_documents(conn)
        return {"rows": rows}, []
    if tool == "open_sales_orders":
        rows = analytics.open_or_incomplete_sales_orders(conn)
        return {"rows": rows}, []
    if tool == "sales_pipeline_status_summary":
        return analytics.sales_order_delivery_billing_status_summary(conn), []

    return {"error": f"Unknown tool {tool!r}"}, []


async def groq_chat(messages: list[dict[str, str]], temperature: float = 0.2) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY is not set. Copy backend/.env.example to backend/.env and add your key.")

    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": temperature,
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                GROQ_API_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json=payload,
            )
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        if code == 401:
            raise RuntimeError(
                "The AI API rejected the key. Check GROQ_API_KEY in backend/.env."
            ) from None
        if code == 429:
            raise RuntimeError("The AI service rate limit was hit. Wait a moment and try again.") from None
        raise RuntimeError(f"The AI service returned an error ({code}). Try again later.") from None
    except httpx.RequestError:
        # ConnectError (DNS / no route), timeouts, TLS, etc.
        raise RuntimeError(LLM_UNREACHABLE_MSG) from None

    try:
        return str(data["choices"][0]["message"]["content"]).strip()
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError("The AI service sent an unexpected response. Try again.") from e


MAX_CHAINED_BILLING_TRACES = 5


def _user_wants_billing_trace(user_text: str) -> bool:
    """True when the user is asking for an end-to-end billing document flow, not just a scalar lookup."""
    t = user_text.lower()
    flowish = bool(
        re.search(
            r"\b(trace|tracing|flows?|downstream|chain|end[\s-]to[\s-]end|e2e)\b",
            t,
        )
    )
    billingish = bool(re.search(r"\b(billing|billed|invoice)\b", t))
    return flowish and billingish


def _billing_document_ids_from_rows(
    rows: list[dict[str, Any]],
    limit: int = MAX_CHAINED_BILLING_TRACES,
) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for row in rows:
        bd = _row_get(row, "billingDocument", "billingdocument", "billing_document")
        if not bd:
            continue
        s = str(bd).strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
        if len(out) >= limit:
            break
    return out


def _append_billing_traces_after_sql(
    conn: Any,
    user_text: str,
    rows: list[dict[str, Any]],
    evidence: dict[str, Any],
    highlights: list[str],
) -> None:
    """If SQL picked billing document(s) and the user asked for a flow, run trace_billing_flow for each."""
    if not rows or not _user_wants_billing_trace(user_text):
        return
    bids = _billing_document_ids_from_rows(rows)
    if not bids:
        return
    traces: list[dict[str, Any]] = []
    extra_hi: list[str] = []
    for bid in bids:
        try:
            ev, hi = _run_tool(conn, "trace_billing_flow", {"billing_document": bid})
        except Exception as e:  # noqa: BLE001
            traces.append({"billing_document": bid, "error": str(e)})
            continue
        traces.append({"billing_document": bid, **ev})
        extra_hi.extend(hi)
    if traces:
        evidence["billing_flow_traces"] = traces
    highlights[:] = list(dict.fromkeys([*highlights, *extra_hi]))


def _parse_json_obj(text: str) -> dict[str, Any] | None:
    text = text.strip()
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


async def run_turn(
    conn,
    user_text: str,
    history: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    history = history or []
    tables = {t.lower() for t in list_tables(conn)}
    schema = table_schema_summary(conn)

    plan_messages = [
        {"role": "system", "content": SYSTEM_PLANNER + "\n\nSchema:\n" + schema},
        *history[-8:],
        {"role": "user", "content": user_text},
    ]
    raw_plan = await groq_chat(plan_messages, temperature=0.1)
    plan = _parse_json_obj(raw_plan)

    if not plan or plan.get("intent") == "off_topic":
        return {
            "reply": GUARDRAIL_MSG,
            "highlight_node_ids": [],
            "evidence": None,
            "plan_raw": raw_plan,
        }

    evidence: dict[str, Any] = {}
    highlights: list[str] = []

    if plan.get("intent") == "tool":
        tool = plan.get("tool")
        tool_s = str(tool)
        args = plan.get("args") if isinstance(plan.get("args"), dict) else {}
        try:
            evidence, highlights = _run_tool(conn, tool_s, args)
            extra = _highlights_for_tool_evidence(tool_s, evidence)
            highlights = list(dict.fromkeys([*highlights, *extra]))
        except Exception as e:  # noqa: BLE001
            evidence = {"error": str(e)}

    elif plan.get("intent") == "sql":
        sql = plan.get("sql") or ""
        rows, err = run_guarded_select(conn, sql, tables)
        if err:
            evidence = {"sql_error": err, "sql": sql}
            highlights = []
        else:
            evidence = {"rows": rows, "sql": sql}
            highlights = _highlights_from_sql_rows_ranked(rows)
            _append_billing_traces_after_sql(conn, user_text, rows, evidence, highlights)

    else:
        return {
            "reply": GUARDRAIL_MSG,
            "highlight_node_ids": [],
            "evidence": {"parse_error": raw_plan},
            "plan_raw": raw_plan,
        }

    ans_messages = [
        {"role": "system", "content": SYSTEM_ANSWER},
        {
            "role": "user",
            "content": json.dumps(
                {"question": user_text, "evidence": _evidence_for_answer(evidence)},
                default=str,
            ),
        },
    ]
    reply = await groq_chat(ans_messages, temperature=0.3)

    return {
        "reply": reply,
        "highlight_node_ids": highlights,
        "evidence": evidence,
        "plan_raw": raw_plan,
    }
