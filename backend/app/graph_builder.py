"""Build an in-memory graph (nodes/edges) from SQLite O2C tables."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any


def _norm_sd_item(x: str | None) -> str | None:
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


def _short_label(etype: str, key: str) -> str:
    if etype == "sales_order":
        return f"SO {key}"
    if etype == "sales_order_item":
        so, it = key.split(":", 1)
        return f"Item {it}"
    if etype == "delivery":
        return f"Del {key}"
    if etype == "delivery_item":
        return f"DIt {key.split(':')[-1]}"
    if etype == "billing":
        return f"Bill {key}"
    if etype == "billing_item":
        return f"BIt {key.split(':')[-1]}"
    if etype == "journal_entry":
        return "Journal"
    if etype == "customer":
        return f"Cust {key[-6:]}"
    if etype == "product":
        return key[:10] + "…" if len(key) > 10 else key
    if etype == "plant":
        return f"Plant {key}"
    return etype


@dataclass
class GraphPayload:
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)
    meta_by_id: dict[str, dict[str, Any]] = field(default_factory=dict)


def _fetchall(conn: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    try:
        return list(conn.execute(f'SELECT * FROM "{table}"'))
    except sqlite3.Error:
        return []


def _row_dict(r: sqlite3.Row) -> dict[str, Any]:
    return {k: r[k] for k in r.keys()}


def build_graph(conn: sqlite3.Connection) -> GraphPayload:
    out = GraphPayload()
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    meta: dict[str, dict] = {}

    def add_node(nid: str, etype: str, label: str, raw: dict | None = None) -> None:
        if nid in nodes:
            return
        grp = etype
        nodes[nid] = {"id": nid, "label": label, "group": grp}
        if raw is not None:
            meta[nid] = {"entity": etype.replace("_", " ").title(), **raw}

    def add_edge(src: str, tgt: str, label: str) -> None:
        if src not in nodes or tgt not in nodes:
            return
        edges.append({"source": src, "target": tgt, "label": label})

    # --- Sales orders ---
    for r in _fetchall(conn, "sales_order_headers"):
        d = _row_dict(r)
        so = d.get("salesOrder")
        if not so:
            continue
        nid = f"so:{so}"
        add_node(nid, "sales_order", _short_label("sales_order", so), d)
        sold = d.get("soldToParty")
        if sold:
            bid = f"bp:{sold}"
            add_node(bid, "customer", _short_label("customer", sold), None)
            add_edge(nid, bid, "sold_to")

    for r in _fetchall(conn, "sales_order_items"):
        d = _row_dict(r)
        so, it = d.get("salesOrder"), d.get("salesOrderItem")
        if not so or not it:
            continue
        itn = _norm_sd_item(it) or str(it)
        nid = f"soi:{so}:{itn}"
        add_node(nid, "sales_order_item", _short_label("sales_order_item", f"{so}:{itn}"), d)
        add_edge(nid, f"so:{so}", "order_line")
        mat = d.get("material")
        if mat:
            mid = f"mat:{mat}"
            add_node(mid, "product", _short_label("product", mat), None)
            add_edge(nid, mid, "material")
        pl = d.get("productionPlant")
        if pl:
            pid = f"plant:{pl}"
            add_node(pid, "plant", _short_label("plant", pl), None)
            add_edge(nid, pid, "production_plant")

    # --- Deliveries ---
    for r in _fetchall(conn, "outbound_delivery_headers"):
        d = _row_dict(r)
        dd = d.get("deliveryDocument")
        if not dd:
            continue
        hid = f"delh:{dd}"
        add_node(hid, "delivery", _short_label("delivery", dd), d)

    for r in _fetchall(conn, "outbound_delivery_items"):
        d = _row_dict(r)
        dd, di = d.get("deliveryDocument"), d.get("deliveryDocumentItem")
        if not dd or not di:
            continue
        din = _norm_sd_item(di) or str(di)
        iid = f"deli:{dd}:{din}"
        add_node(iid, "delivery_item", _short_label("delivery_item", f"{dd}:{din}"), d)
        add_edge(iid, f"delh:{dd}", "delivery_line")
        ref_so = d.get("referenceSdDocument")
        ref_it = _norm_sd_item(d.get("referenceSdDocumentItem"))
        if ref_so and ref_it:
            soi = f"soi:{ref_so}:{ref_it}"
            add_edge(iid, soi, "fulfills_order_line")
        pl = d.get("plant")
        if pl:
            pid = f"plant:{pl}"
            add_node(pid, "plant", _short_label("plant", pl), None)
            add_edge(iid, pid, "ship_from_plant")

    # --- Billing ---
    for r in _fetchall(conn, "billing_document_headers"):
        d = _row_dict(r)
        bd = d.get("billingDocument")
        if not bd:
            continue
        hid = f"billh:{bd}"
        add_node(hid, "billing", _short_label("billing", bd), d)
        cust = d.get("soldToParty")
        if cust:
            bid = f"bp:{cust}"
            add_node(bid, "customer", _short_label("customer", cust), None)
            add_edge(hid, bid, "bill_to")

    for r in _fetchall(conn, "billing_document_items"):
        d = _row_dict(r)
        bd, bi = d.get("billingDocument"), d.get("billingDocumentItem")
        if not bd or not bi:
            continue
        bin_ = _norm_sd_item(bi) or str(bi)
        iid = f"billi:{bd}:{bin_}"
        add_node(iid, "billing_item", _short_label("billing_item", f"{bd}:{bin_}"), d)
        add_edge(iid, f"billh:{bd}", "billing_line")
        ref_del = d.get("referenceSdDocument")
        ref_di = _norm_sd_item(d.get("referenceSdDocumentItem"))
        if ref_del and ref_di:
            deli = f"deli:{ref_del}:{ref_di}"
            add_edge(iid, deli, "bills_delivery_line")
        mat = d.get("material")
        if mat:
            mid = f"mat:{mat}"
            add_node(mid, "product", _short_label("product", mat), None)
            add_edge(iid, mid, "material")

    # --- Journal (AR items) ---
    for r in _fetchall(conn, "journal_entry_items_accounts_receivable"):
        d = _row_dict(r)
        cc, fy = d.get("companyCode"), d.get("fiscalYear")
        ad, ai = d.get("accountingDocument"), d.get("accountingDocumentItem")
        if not all([cc, fy, ad, ai]):
            continue
        jid = f"je:{cc}:{fy}:{ad}:{ai}"
        add_node(jid, "journal_entry", _short_label("journal_entry", ad), d)
        ref = d.get("referenceDocument")
        if ref:
            add_edge(jid, f"billh:{ref}", "references_billing")
        cust = d.get("customer")
        if cust:
            bid = f"bp:{cust}"
            add_node(bid, "customer", _short_label("customer", cust), None)
            add_edge(jid, bid, "customer")

    # --- Link billing header accounting document to journal lines ---
    for r in _fetchall(conn, "billing_document_headers"):
        d = _row_dict(r)
        bd = d.get("billingDocument")
        ad, fy, cc = d.get("accountingDocument"), d.get("fiscalYear"), d.get("companyCode")
        if not bd or not ad or not fy or not cc:
            continue
        # connect header to any journal line with same accounting doc
        for jr in _fetchall(conn, "journal_entry_items_accounts_receivable"):
            jd = _row_dict(jr)
            if (
                jd.get("accountingDocument") == ad
                and str(jd.get("fiscalYear")) == str(fy)
                and jd.get("companyCode") == cc
            ):
                jid = f"je:{cc}:{fy}:{ad}:{jd.get('accountingDocumentItem')}"
                add_edge(f"billh:{bd}", jid, "posted_as")

    out.nodes = list(nodes.values())
    out.edges = edges
    out.meta_by_id = meta
    return out


def neighbors_of(payload: GraphPayload, node_id: str) -> tuple[list[dict], list[dict]]:
    """Return (nodes, edges) for 1-hop neighborhood including center."""
    ids = {node_id}
    sub_e: list[dict] = []
    for e in payload.edges:
        if e["source"] == node_id:
            ids.add(e["target"])
            sub_e.append(e)
        elif e["target"] == node_id:
            ids.add(e["source"])
            sub_e.append(e)
    idset = set(ids)
    sub_n = [n for n in payload.nodes if n["id"] in idset]
    return sub_n, sub_e
