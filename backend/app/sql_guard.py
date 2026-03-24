from __future__ import annotations

import re
import sqlite3

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ATTACH|PRAGMA|VACUUM|CREATE|ALTER|REPLACE|TRIGGER|BEGIN|COMMIT|ROLLBACK|SAVEPOINT)\b",
    re.IGNORECASE,
)


def validate_select_sql(sql: str, allowed_tables: set[str]) -> tuple[bool, str]:
    s = (sql or "").strip()
    if not s:
        return False, "Empty SQL."
    if ";" in s.rstrip(";"):
        return False, "Multiple statements are not allowed."
    s = s.rstrip().rstrip(";")

    if not re.match(r"^\s*SELECT\b", s, re.IGNORECASE):
        return False, "Only SELECT queries are allowed."

    if _FORBIDDEN.search(s):
        return False, "Disallowed keyword in query."

    referenced = set()
    for m in re.finditer(r'\b(?:FROM|JOIN)\s+(?:"([^"]+)"|(\w+))', s, re.IGNORECASE):
        name = m.group(1) or m.group(2)
        if name:
            referenced.add(name.lower())

    unknown = referenced - allowed_tables
    if unknown:
        return False, f"Unknown table(s): {', '.join(sorted(unknown))}. Allowed: {', '.join(sorted(allowed_tables))}."

    return True, s


def run_guarded_select(conn: sqlite3.Connection, sql: str, allowed_tables: set[str]) -> tuple[list[dict], str | None]:
    ok, msg = validate_select_sql(sql, allowed_tables)
    if not ok:
        return [], msg
    try:
        cur = conn.execute(msg)  # msg is possibly modified SQL with LIMIT
        rows = [dict(r) for r in cur.fetchall()]
        return rows, None
    except sqlite3.Error as e:
        return [], str(e)
