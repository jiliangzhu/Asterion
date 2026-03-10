from __future__ import annotations

import os
import re
import sqlite3
import time

_WRITE_FIRST = {"INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "REPLACE"}


def default_audit_db_path() -> str:
    return os.getenv("ASTERION_WRITE_GUARD_AUDIT_DB", "data/meta/write_guard_audit.sqlite")


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS write_guard_events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts_ms BIGINT NOT NULL,
          guard_mode TEXT NOT NULL,
          reason TEXT NOT NULL,
          statement_snippet TEXT
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_write_guard_events_ts ON write_guard_events(created_ts_ms)")


def record_write_guard_block(*, guard_mode: str, reason: str, statement: str | None) -> None:
    path = default_audit_db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    snippet = (statement or "").strip().replace("\n", " ")
    if len(snippet) > 400:
        snippet = snippet[:400]
    now_ms = int(time.time() * 1000)
    con = sqlite3.connect(path, timeout=3)
    try:
        _ensure_schema(con)
        con.execute(
            """
            INSERT INTO write_guard_events (created_ts_ms, guard_mode, reason, statement_snippet)
            VALUES (?, ?, ?, ?)
            """,
            [now_ms, guard_mode, reason[:400], snippet],
        )
        con.commit()
    finally:
        con.close()


def count_write_guard_blocks_since(*, since_ts_ms: int) -> int:
    path = default_audit_db_path()
    if not os.path.exists(path):
        return 0
    con = sqlite3.connect(path, timeout=3)
    try:
        _ensure_schema(con)
        row = con.execute(
            "SELECT COUNT(1) FROM write_guard_events WHERE created_ts_ms >= ?",
            [int(since_ts_ms)],
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        con.close()


def _sql_tokens(sql: str) -> list[str]:
    no_line = re.sub(r"--[^\n]*", " ", sql)
    no_comments = re.sub(r"/\*.*?\*/", " ", no_line, flags=re.S)
    return re.findall(r"[A-Za-z_]+", no_comments.upper())


def _is_write_intent_event(*, reason: str, statement_snippet: str | None) -> bool:
    reason_u = str(reason or "").upper()
    snippet = str(statement_snippet or "")
    toks = _sql_tokens(snippet)
    if toks:
        first = toks[0]
        if first in _WRITE_FIRST:
            return True
        if first == "EXPLAIN" and len(toks) > 1 and toks[1] in _WRITE_FIRST:
            return True
    return any(keyword in reason_u for keyword in _WRITE_FIRST) or "WRITE" in reason_u


def count_write_guard_write_attempts_since(*, since_ts_ms: int) -> int:
    path = default_audit_db_path()
    if not os.path.exists(path):
        return 0
    con = sqlite3.connect(path, timeout=3)
    try:
        _ensure_schema(con)
        rows = con.execute(
            """
            SELECT reason, statement_snippet
            FROM write_guard_events
            WHERE created_ts_ms >= ?
            """,
            [int(since_ts_ms)],
        ).fetchall()
        return sum(
            1
            for reason, statement_snippet in rows
            if _is_write_intent_event(reason=str(reason or ""), statement_snippet=statement_snippet)
        )
    finally:
        con.close()

