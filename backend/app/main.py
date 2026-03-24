from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from . import graph_builder
from .config import REPO_ROOT, SQLITE_PATH
from .db import ensure_db, get_connection, list_tables
from .chat import run_turn

_conn = None
_graph: graph_builder.GraphPayload | None = None


def _reload_graph_sync() -> None:
    global _graph
    if _conn is None:
        return
    _graph = graph_builder.build_graph(_conn)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _conn
    ensure_db()
    _conn = get_connection()
    _reload_graph_sync()
    yield
    if _conn:
        _conn.close()
        _conn = None


app = FastAPI(title="Dodge AI O2C Graph API", lifespan=lifespan)

# MVP / demo: allow any origin. Browsers require allow_credentials=False with "*".
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"ok": True, "db": str(SQLITE_PATH), "repo": str(REPO_ROOT)}


@app.post("/api/admin/reingest")
def reingest() -> dict[str, Any]:
    global _conn
    from .ingest import ingest

    counts = ingest()
    if _conn:
        _conn.close()
    _conn = get_connection()
    _reload_graph_sync()
    return {"ingested": counts}


@app.get("/api/graph")
def get_graph() -> dict[str, Any]:
    if not _graph:
        raise HTTPException(503, "Graph not ready")
    return {
        "nodes": _graph.nodes,
        "edges": _graph.edges,
        "totals": {"nodes": len(_graph.nodes), "edges": len(_graph.edges)},
    }


class NeighborReq(BaseModel):
    node_id: str


@app.post("/api/graph/neighbors")
def neighbors(body: NeighborReq) -> dict[str, Any]:
    if not _graph:
        raise HTTPException(503, "Graph not ready")
    n, e = graph_builder.neighbors_of(_graph, body.node_id)
    if not n:
        raise HTTPException(404, "Unknown node")
    return {"nodes": n, "edges": e}


@app.get("/api/node/{node_id:path}")
def node_meta(node_id: str) -> dict[str, Any]:
    if not _graph:
        raise HTTPException(503, "Graph not ready")
    meta = _graph.meta_by_id.get(node_id)
    if not meta:
        # still return minimal label from nodes list
        for n in _graph.nodes:
            if n["id"] == node_id:
                return {"id": node_id, "label": n.get("label"), "group": n.get("group"), "metadata": {}}
        raise HTTPException(404, "Unknown node")
    return {"id": node_id, "metadata": meta}


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatReq(BaseModel):
    messages: list[ChatMessage] = Field(default_factory=list)


@app.post("/api/chat")
async def chat(body: ChatReq) -> dict[str, Any]:
    if not _conn:
        raise HTTPException(503, "DB not ready")
    if not body.messages:
        raise HTTPException(400, "messages required")
    last = body.messages[-1]
    if last.role != "user":
        raise HTTPException(400, "Last message must be user")

    hist = [{"role": m.role, "content": m.content} for m in body.messages[:-1]]
    try:
        result = await run_turn(_conn, last.content, hist)
    except RuntimeError as e:
        return {
            "reply": str(e),
            "highlight_node_ids": [],
            "evidence": None,
        }
    return result


@app.get("/api/schema")
def schema() -> dict[str, Any]:
    if not _conn:
        raise HTTPException(503, "DB not ready")
    from .db import table_schema_summary

    return {"tables": list_tables(_conn), "summary": table_schema_summary(_conn)}
