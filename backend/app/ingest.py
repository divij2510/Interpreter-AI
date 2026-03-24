"""Load sap-o2c JSONL folders into SQLite (dynamic TEXT columns per entity folder)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .config import SAP_DATA_DIR, SQLITE_PATH


def _flatten_value(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"))
    return str(v)


def _sanitize_table(name: str) -> str:
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    if not safe or safe[0].isdigit():
        safe = "t_" + safe
    return safe.lower()


def ingest(data_dir: Path | None = None, db_path: Path | None = None) -> dict[str, int]:
    data_dir = data_dir or SAP_DATA_DIR
    db_path = db_path or SQLITE_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if not data_dir.is_dir():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    counts: dict[str, int] = {}
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        for sub in sorted(p for p in data_dir.iterdir() if p.is_dir()):
            table = _sanitize_table(sub.name)
            rows: list[dict[str, Any]] = []
            for fp in sorted(sub.glob("*.jsonl")):
                with fp.open(encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        rows.append(json.loads(line))

            if not rows:
                continue

            keys: set[str] = set()
            for r in rows:
                keys.update(r.keys())
            cols = sorted(keys)
            col_sql = ", ".join(f'"{c}" TEXT' for c in cols)
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" ({col_sql})')

            placeholders = ", ".join("?" for _ in cols)
            insert_sql = f'INSERT INTO "{table}" ({", ".join(chr(34)+c+chr(34) for c in cols)}) VALUES ({placeholders})'

            batch = []
            for r in rows:
                batch.append(tuple(_flatten_value(r.get(c)) for c in cols))
            conn.executemany(insert_sql, batch)
            counts[table] = len(rows)

        conn.commit()
    finally:
        conn.close()

    return counts


if __name__ == "__main__":
    c = ingest()
    print(json.dumps(c, indent=2))
