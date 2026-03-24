"""Deterministic analytics for common O2C questions (data-backed)."""

from __future__ import annotations

import sqlite3
from typing import Any


def _norm_item(x: str | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return s
    try:
        return str(int(s))
    except ValueError:
        s2 = s.lstrip("0")
        return s2 if s2 else "0"


def trace_billing_flow(conn: sqlite3.Connection, billing_document: str) -> dict[str, Any]:
    billing_document = str(billing_document).strip()
    hdr = conn.execute(
        'SELECT * FROM billing_document_headers WHERE billingDocument = ? LIMIT 1',
        (billing_document,),
    ).fetchone()
    if not hdr:
        return {"error": f"No billing document {billing_document}", "path": []}

    hdr_d = dict(hdr)
    items = [
        dict(r)
        for r in conn.execute(
            'SELECT * FROM billing_document_items WHERE billingDocument = ?',
            (billing_document,),
        )
    ]

    path: list[dict[str, Any]] = [
        {"step": "billing_header", "data": hdr_d},
        {"step": "billing_items", "data": items},
    ]

    deliveries: dict[str, dict] = {}
    sales_orders: dict[str, dict] = {}
    so_items: list[dict] = []

    for it in items:
        rd, ri = it.get("referenceSdDocument"), _norm_item(it.get("referenceSdDocumentItem"))
        if not rd or not ri:
            continue
        drow = None
        for cand in conn.execute(
            "SELECT * FROM outbound_delivery_items WHERE deliveryDocument = ?",
            (rd,),
        ):
            cd = dict(cand)
            if _norm_item(cd.get("deliveryDocumentItem")) == ri:
                drow = cd
                break
        if drow:
            dd = drow
            deliveries[str(rd)] = dd
            rso, rsi = dd.get("referenceSdDocument"), _norm_item(dd.get("referenceSdDocumentItem"))
            if rso and rsi:
                sor = None
                for cand in conn.execute(
                    "SELECT * FROM sales_order_items WHERE salesOrder = ?",
                    (rso,),
                ):
                    sd = dict(cand)
                    if _norm_item(sd.get("salesOrderItem")) == rsi:
                        sor = sd
                        break
                if sor:
                    so_items.append(sor)
                    if rso not in sales_orders:
                        sho = conn.execute(
                            "SELECT * FROM sales_order_headers WHERE salesOrder = ? LIMIT 1",
                            (rso,),
                        ).fetchone()
                        if sho:
                            sales_orders[str(rso)] = dict(sho)

    if deliveries:
        path.append({"step": "delivery_items", "data": list(deliveries.values())})
    if sales_orders:
        path.append({"step": "sales_order_headers", "data": list(sales_orders.values())})
    if so_items:
        path.append({"step": "sales_order_items", "data": so_items})

    acc = hdr_d.get("accountingDocument")
    fy = hdr_d.get("fiscalYear")
    cc = hdr_d.get("companyCode")
    journal = []
    if acc and fy and cc:
        journal = [
            dict(r)
            for r in conn.execute(
                """
                SELECT * FROM journal_entry_items_accounts_receivable
                WHERE accountingDocument = ? AND fiscalYear = ? AND companyCode = ?
                """,
                (acc, fy, cc),
            )
        ]
    if journal:
        path.append({"step": "journal_entry_lines", "data": journal})

    highlight_ids = [f"billh:{billing_document}"]
    for it in items:
        bi = _norm_item(it.get("billingDocumentItem"))
        if bi:
            highlight_ids.append(f"billi:{billing_document}:{bi}")
    for d in deliveries.values():
        dd, di = d.get("deliveryDocument"), _norm_item(d.get("deliveryDocumentItem"))
        if dd and di:
            highlight_ids.extend([f"delh:{dd}", f"deli:{dd}:{di}"])
    for so, sho in sales_orders.items():
        highlight_ids.append(f"so:{so}")
    if acc and fy and cc:
        for j in journal:
            jid = f"je:{cc}:{fy}:{j.get('accountingDocument')}:{j.get('accountingDocumentItem')}"
            highlight_ids.append(jid)

    return {"billing_document": billing_document, "path": path, "highlight_node_ids": list(dict.fromkeys(highlight_ids))}


def top_products_by_billing_count(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT material, COUNT(*) AS billing_line_count
        FROM billing_document_items
        WHERE material IS NOT NULL AND material != ''
        GROUP BY material
        ORDER BY billing_line_count DESC
        """
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        mat = d.get("material")
        label = None
        if mat:
            pr = conn.execute(
                "SELECT product, productOldId FROM products WHERE product = ? LIMIT 1",
                (mat,),
            ).fetchone()
            if pr:
                label = dict(pr)
        out.append({"material": mat, "billing_line_count": d.get("billing_line_count"), "product": label})
    return out


def broken_flows(conn: sqlite3.Connection) -> dict[str, Any]:
    """Delivery lines without a billing line referencing them; billing lines whose delivery ref is missing."""

    del_keys: set[tuple[str, str]] = set()
    for r in conn.execute("SELECT deliveryDocument, deliveryDocumentItem FROM outbound_delivery_items"):
        d = dict(r)
        dd, di = d.get("deliveryDocument"), _norm_item(d.get("deliveryDocumentItem"))
        if dd and di:
            del_keys.add((str(dd), di))

    bill_refs: set[tuple[str, str]] = set()
    for r in conn.execute(
        "SELECT referenceSdDocument, referenceSdDocumentItem FROM billing_document_items WHERE referenceSdDocument IS NOT NULL"
    ):
        d = dict(r)
        rd, ri = d.get("referenceSdDocument"), _norm_item(d.get("referenceSdDocumentItem"))
        if rd and ri:
            bill_refs.add((str(rd), ri))

    delivered_not_billed = [
        {"deliveryDocument": a, "deliveryDocumentItem": b} for a, b in sorted(del_keys - bill_refs)
    ]

    del_existing = {
        str(dict(r)["deliveryDocument"])
        for r in conn.execute("SELECT deliveryDocument FROM outbound_delivery_headers")
    }

    billed_missing_delivery = []
    for r in conn.execute(
        "SELECT billingDocument, billingDocumentItem, referenceSdDocument, referenceSdDocumentItem FROM billing_document_items WHERE referenceSdDocument IS NOT NULL"
    ):
        d = dict(r)
        ref = str(d["referenceSdDocument"])
        if ref not in del_existing:
            billed_missing_delivery.append(
                {
                    "billingDocument": d.get("billingDocument"),
                    "billingDocumentItem": d.get("billingDocumentItem"),
                    "referenceSdDocument": ref,
                }
            )

    return {
        "delivered_not_billed_lines": delivered_not_billed,
        "billed_but_delivery_header_missing": billed_missing_delivery,
        "counts": {
            "delivered_not_billed_lines": len(delivered_not_billed),
            "billed_but_delivery_header_missing": len(billed_missing_delivery),
        },
    }


def _je_node_id(row: dict[str, Any]) -> str | None:
    cc, fy = row.get("companyCode"), row.get("fiscalYear")
    ad, ai = row.get("accountingDocument"), row.get("accountingDocumentItem")
    if not all([cc, fy, ad, ai]):
        return None
    return f"je:{cc}:{fy}:{ad}:{ai}"


def trace_sales_order(conn: sqlite3.Connection, sales_order: str) -> dict[str, Any]:
    """Sales order → delivery lines → billing lines → journal (via billing accounting doc)."""
    sales_order = str(sales_order).strip()
    hdr = conn.execute(
        "SELECT * FROM sales_order_headers WHERE salesOrder = ? LIMIT 1",
        (sales_order,),
    ).fetchone()
    if not hdr:
        return {"error": f"No sales order {sales_order}", "path": [], "highlight_node_ids": []}

    hdr_d = dict(hdr)
    items = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM sales_order_items WHERE salesOrder = ?",
            (sales_order,),
        )
    ]
    path: list[dict[str, Any]] = [
        {"step": "sales_order_header", "data": hdr_d},
        {"step": "sales_order_items", "data": items},
    ]
    highlights: list[str] = [f"so:{sales_order}"]
    for it in items:
        itn = _norm_item(it.get("salesOrderItem"))
        if itn:
            highlights.append(f"soi:{sales_order}:{itn}")

    sold = hdr_d.get("soldToParty")
    if sold:
        highlights.append(f"bp:{sold}")

    del_items: list[dict[str, Any]] = []
    for r in conn.execute("SELECT * FROM outbound_delivery_items"):
        d = dict(r)
        if str(d.get("referenceSdDocument") or "") == sales_order:
            del_items.append(d)

    del_headers: dict[str, dict] = {}
    for d in del_items:
        dd = str(d.get("deliveryDocument") or "")
        if dd and dd not in del_headers:
            h = conn.execute(
                "SELECT * FROM outbound_delivery_headers WHERE deliveryDocument = ? LIMIT 1",
                (dd,),
            ).fetchone()
            if h:
                del_headers[dd] = dict(h)
        din = _norm_item(d.get("deliveryDocumentItem"))
        if dd and din:
            highlights.extend([f"delh:{dd}", f"deli:{dd}:{din}"])

    if del_headers:
        path.append({"step": "delivery_headers", "data": list(del_headers.values())})
    if del_items:
        path.append({"step": "delivery_items", "data": del_items})

    bill_items: list[dict[str, Any]] = []
    seen_bi: set[tuple[str, str]] = set()
    bill_hdrs: dict[str, dict] = {}

    for d in del_items:
        dd = str(d.get("deliveryDocument") or "")
        dni = _norm_item(d.get("deliveryDocumentItem"))
        if not dd or not dni:
            continue
        for r in conn.execute(
            "SELECT * FROM billing_document_items WHERE referenceSdDocument = ?",
            (dd,),
        ):
            bi = dict(r)
            if _norm_item(bi.get("referenceSdDocumentItem")) != dni:
                continue
            key = (str(bi.get("billingDocument")), str(bi.get("billingDocumentItem")))
            if key in seen_bi:
                continue
            seen_bi.add(key)
            bill_items.append(bi)
            bd = str(bi.get("billingDocument") or "")
            if bd and bd not in bill_hdrs:
                bh = conn.execute(
                    "SELECT * FROM billing_document_headers WHERE billingDocument = ? LIMIT 1",
                    (bd,),
                ).fetchone()
                if bh:
                    bill_hdrs[bd] = dict(bh)

    if bill_hdrs:
        path.append({"step": "billing_headers", "data": list(bill_hdrs.values())})
    if bill_items:
        path.append({"step": "billing_items", "data": bill_items})

    for bd, bh in bill_hdrs.items():
        highlights.append(f"billh:{bd}")
        acc, fy, cc = bh.get("accountingDocument"), bh.get("fiscalYear"), bh.get("companyCode")
        if acc and fy and cc:
            for jr in conn.execute(
                """
                SELECT * FROM journal_entry_items_accounts_receivable
                WHERE accountingDocument = ? AND fiscalYear = ? AND companyCode = ?
                """,
                (acc, fy, cc),
            ):
                jid = _je_node_id(dict(jr))
                if jid:
                    highlights.append(jid)
    for bi in bill_items:
        bd, bitem = bi.get("billingDocument"), _norm_item(bi.get("billingDocumentItem"))
        if bd and bitem:
            highlights.append(f"billi:{bd}:{bitem}")

    journal_all: list[dict] = []
    for bh in bill_hdrs.values():
        acc, fy, cc = bh.get("accountingDocument"), bh.get("fiscalYear"), bh.get("companyCode")
        if not (acc and fy and cc):
            continue
        journal_all.extend(
            dict(r)
            for r in conn.execute(
                """
                SELECT * FROM journal_entry_items_accounts_receivable
                WHERE accountingDocument = ? AND fiscalYear = ? AND companyCode = ?
                """,
                (acc, fy, cc),
            )
        )
    if journal_all:
        path.append({"step": "journal_entry_lines", "data": journal_all})

    return {
        "sales_order": sales_order,
        "path": path,
        "highlight_node_ids": list(dict.fromkeys(highlights)),
    }


def trace_delivery_document(conn: sqlite3.Connection, delivery_document: str) -> dict[str, Any]:
    """Delivery → related SO lines → billing → journal."""
    delivery_document = str(delivery_document).strip()
    dh = conn.execute(
        "SELECT * FROM outbound_delivery_headers WHERE deliveryDocument = ? LIMIT 1",
        (delivery_document,),
    ).fetchone()
    if not dh:
        return {"error": f"No delivery {delivery_document}", "path": [], "highlight_node_ids": []}

    items = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM outbound_delivery_items WHERE deliveryDocument = ?",
            (delivery_document,),
        )
    ]
    path: list[dict[str, Any]] = [
        {"step": "delivery_header", "data": dict(dh)},
        {"step": "delivery_items", "data": items},
    ]
    highlights = [f"delh:{delivery_document}"]
    sales_orders: dict[str, dict] = {}
    so_items: list[dict] = []
    for d in items:
        din = _norm_item(d.get("deliveryDocumentItem"))
        if din:
            highlights.append(f"deli:{delivery_document}:{din}")
        rso, rsi = d.get("referenceSdDocument"), _norm_item(d.get("referenceSdDocumentItem"))
        if not rso or not rsi:
            continue
        sor = None
        for cand in conn.execute("SELECT * FROM sales_order_items WHERE salesOrder = ?", (rso,)):
            sd = dict(cand)
            if _norm_item(sd.get("salesOrderItem")) == rsi:
                sor = sd
                break
        if sor:
            so_items.append(sor)
            if str(rso) not in sales_orders:
                sho = conn.execute(
                    "SELECT * FROM sales_order_headers WHERE salesOrder = ? LIMIT 1",
                    (rso,),
                ).fetchone()
                if sho:
                    sales_orders[str(rso)] = dict(sho)
            highlights.append(f"soi:{rso}:{rsi}")
            highlights.append(f"so:{rso}")

    if sales_orders:
        path.append({"step": "sales_order_headers", "data": list(sales_orders.values())})
    if so_items:
        path.append({"step": "sales_order_items", "data": so_items})

    bill_items: list[dict] = []
    bill_hdrs: dict[str, dict] = {}
    for r in conn.execute(
        "SELECT * FROM billing_document_items WHERE referenceSdDocument = ?",
        (delivery_document,),
    ):
        bi = dict(r)
        bill_items.append(bi)
        bd = str(bi.get("billingDocument") or "")
        if bd and bd not in bill_hdrs:
            bh = conn.execute(
                "SELECT * FROM billing_document_headers WHERE billingDocument = ? LIMIT 1",
                (bd,),
            ).fetchone()
            if bh:
                bill_hdrs[bd] = dict(bh)
        bim = _norm_item(bi.get("billingDocumentItem"))
        if bd and bim:
            highlights.append(f"billi:{bd}:{bim}")
            highlights.append(f"billh:{bd}")

    if bill_hdrs:
        path.append({"step": "billing_headers", "data": list(bill_hdrs.values())})
    if bill_items:
        path.append({"step": "billing_items", "data": bill_items})

    journal_all: list[dict] = []
    for bh in bill_hdrs.values():
        acc, fy, cc = bh.get("accountingDocument"), bh.get("fiscalYear"), bh.get("companyCode")
        if not (acc and fy and cc):
            continue
        for jr in conn.execute(
            """
            SELECT * FROM journal_entry_items_accounts_receivable
            WHERE accountingDocument = ? AND fiscalYear = ? AND companyCode = ?
            """,
            (acc, fy, cc),
        ):
            jd = dict(jr)
            journal_all.append(jd)
            jid = _je_node_id(jd)
            if jid:
                highlights.append(jid)
    if journal_all:
        path.append({"step": "journal_entry_lines", "data": journal_all})

    return {
        "delivery_document": delivery_document,
        "path": path,
        "highlight_node_ids": list(dict.fromkeys(highlights)),
    }


def top_customers_by_order_count(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT soldToParty AS customer, COUNT(*) AS order_count
        FROM sales_order_headers
        WHERE soldToParty IS NOT NULL AND soldToParty != ''
        GROUP BY soldToParty
        ORDER BY order_count DESC
        """
    ).fetchall()
    return _enrich_customers(conn, rows)


def top_customers_by_billing_revenue(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT soldToParty AS customer,
               SUM(CAST(COALESCE(NULLIF(TRIM(totalNetAmount), ''), '0') AS REAL)) AS total_billed_net
        FROM billing_document_headers
        WHERE LOWER(TRIM(CAST(billingDocumentIsCancelled AS TEXT))) NOT IN ('true','1')
           OR billingDocumentIsCancelled IS NULL
        GROUP BY soldToParty
        HAVING customer IS NOT NULL AND customer != ''
        ORDER BY total_billed_net DESC
        """
    ).fetchall()
    return _enrich_customers(conn, rows)


def _enrich_customers(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        cust = d.get("customer")
        name = None
        if cust:
            bp = conn.execute(
                "SELECT businessPartnerName, businessPartnerFullName FROM business_partners WHERE businessPartner = ? LIMIT 1",
                (cust,),
            ).fetchone()
            if bp:
                name = dict(bp)
        out.append({**d, "business_partner": name})
    return out


def billing_without_accounting(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT billingDocument, billingDocumentDate, soldToParty, totalNetAmount, companyCode
        FROM billing_document_headers
        WHERE accountingDocument IS NULL OR TRIM(accountingDocument) = ''
        """
    ).fetchall()
    return [dict(r) for r in rows]


def cancelled_billing_documents(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT billingDocument, billingDocumentDate, soldToParty, totalNetAmount,
               accountingDocument, cancelledBillingDocument
        FROM billing_document_headers
        WHERE LOWER(CAST(billingDocumentIsCancelled AS TEXT)) IN ('true','1')
        ORDER BY billingDocumentDate DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def journal_for_billing_reference(conn: sqlite3.Connection, reference_document: str) -> dict[str, Any]:
    """Journal AR lines where referenceDocument equals billing document number (or other ref)."""
    ref = str(reference_document).strip()
    lines = [
        dict(r)
        for r in conn.execute(
            """
            SELECT * FROM journal_entry_items_accounts_receivable
            WHERE referenceDocument = ?
            """,
            (ref,),
        )
    ]
    highlights = []
    for ln in lines:
        jid = _je_node_id(ln)
        if jid:
            highlights.append(jid)
        highlights.append(f"billh:{ref}")
    return {
        "reference_document": ref,
        "journal_lines": lines,
        "highlight_node_ids": list(dict.fromkeys(highlights)),
    }


def payments_summary_by_customer(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT customer,
               COUNT(*) AS payment_line_count,
               SUM(CAST(COALESCE(NULLIF(TRIM(amountInTransactionCurrency), ''), '0') AS REAL)) AS sum_txn_amount
        FROM payments_accounts_receivable
        WHERE customer IS NOT NULL AND customer != ''
        GROUP BY customer
        ORDER BY sum_txn_amount DESC
        """
    ).fetchall()
    return _enrich_customers(conn, rows)


def delivery_lines_by_plant(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT plant, COUNT(*) AS delivery_line_count
        FROM outbound_delivery_items
        WHERE plant IS NOT NULL AND plant != ''
        GROUP BY plant
        ORDER BY delivery_line_count DESC
        """
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        pl = d.get("plant")
        pname = None
        if pl:
            pr = conn.execute(
                "SELECT plantName FROM plants WHERE plant = ? LIMIT 1",
                (pl,),
            ).fetchone()
            if pr:
                pname = dict(pr).get("plantName")
        out.append({**d, "plant_name": pname})
    return out


def sales_order_delivery_billing_status_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    """Counts of sales orders by header delivery / billing status fields."""
    del_counts: dict[str, int] = {}
    for r in conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(overallDeliveryStatus), ''), '(blank)') AS s, COUNT(*) AS c
        FROM sales_order_headers GROUP BY s
        """
    ):
        d = dict(r)
        del_counts[str(d["s"])] = int(d["c"])

    bill_counts: dict[str, int] = {}
    for r in conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(overallOrdReltdBillgStatus), ''), '(blank)') AS s, COUNT(*) AS c
        FROM sales_order_headers GROUP BY s
        """
    ):
        d = dict(r)
        bill_counts[str(d["s"])] = int(d["c"])

    return {"by_overall_delivery_status": del_counts, "by_overall_billing_status": bill_counts}


def open_or_incomplete_sales_orders(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Orders where delivery status is not complete (not 'C') — common 'stuck pipeline' view."""
    rows = conn.execute(
        """
        SELECT salesOrder, soldToParty, overallDeliveryStatus, overallOrdReltdBillgStatus,
               totalNetAmount, creationDate
        FROM sales_order_headers
        WHERE overallDeliveryStatus IS NULL OR TRIM(overallDeliveryStatus) = '' OR overallDeliveryStatus != 'C'
        ORDER BY creationDate DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def top_material_groups_by_billing_lines(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT p.productGroup AS material_group, COUNT(*) AS billing_line_count
        FROM billing_document_items b
        JOIN products p ON p.product = b.material
        WHERE b.material IS NOT NULL AND b.material != ''
        GROUP BY p.productGroup
        ORDER BY billing_line_count DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]
