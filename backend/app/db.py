from __future__ import annotations

import sqlite3
from typing import Any

from .config import SAP_DATA_DIR, SCHEMA_MAX_COLS_PER_TABLE, SQLITE_PATH
from .ingest import ingest


def get_connection() -> sqlite3.Connection:
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SQLITE_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_db() -> None:
    if not SQLITE_PATH.is_file() or SQLITE_PATH.stat().st_size < 8:
        if not SAP_DATA_DIR.is_dir():
            raise RuntimeError(f"Missing data: {SAP_DATA_DIR}")
        ingest()


def list_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [r[0] for r in cur.fetchall()]


def table_schema_summary(
    conn: sqlite3.Connection,
    max_cols: int | None = None,
) -> str:
    """List tables and column names for the planner. By default includes every column (no cap)."""
    cap = max_cols if max_cols is not None else SCHEMA_MAX_COLS_PER_TABLE
    lines: list[str] = []
    for t in list_tables(conn):
        info = conn.execute(f'PRAGMA table_info("{t}")').fetchall()
        if cap is not None:
            cols = [row[1] for row in info[:cap]]
            extra = len(info) - len(cols)
            suf = f" (+{extra} more)" if extra > 0 else ""
        else:
            cols = [row[1] for row in info]
            suf = ""
        lines.append(f"- {t}: {', '.join(cols)}{suf}")
    return "\n".join(lines)


def run_select(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    cur = conn.execute(sql)
    return [dict(row) for row in cur.fetchall()]
