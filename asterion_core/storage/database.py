from __future__ import annotations

import dataclasses
import os
import re
import time
from typing import Any

from .logger import get_logger
from .utils import env_flag, safe_json_dumps
from .write_guard_audit import record_write_guard_block

log = get_logger(__name__)


@dataclasses.dataclass(frozen=True)
class DuckDBConfig:
    db_path: str
    ddl_path: str | None = None


_READER_ALLOWED_FIRST = {"SELECT", "WITH", "EXPLAIN", "DESCRIBE", "SHOW"}
_READER_ALLOWED_EXPLAIN_NEXT = {"SELECT", "WITH", "DESCRIBE", "SHOW"}
_WRITE_STATEMENT_FIRST = {"INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE"}
_READER_FORBIDDEN_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "REPLACE",
    "TRUNCATE",
    "CREATE",
    "ALTER",
    "DROP",
    "ATTACH",
    "DETACH",
    "COPY",
    "EXPORT",
    "IMPORT",
    "INSTALL",
    "LOAD",
    "PRAGMA",
    "VACUUM",
    "CALL",
}
_HARD_FORBIDDEN_RUNTIME_KEYWORDS = {
    "ATTACH",
    "DETACH",
    "COPY",
    "EXPORT",
    "IMPORT",
    "INSTALL",
    "LOAD",
    "PRAGMA",
    "VACUUM",
    "CALL",
}


def _resolve_single_writer_mode() -> tuple[bool, bool]:
    strict = env_flag("ASTERION_STRICT_SINGLE_WRITER", True)
    if not strict:
        return False, False
    writerd = env_flag("WRITERD") or env_flag("ASTERION_WRITERD")
    db_role = os.getenv("ASTERION_DB_ROLE", "reader").strip().lower()
    return True, writerd and db_role == "writer"


def _sql_tokens(sql: str) -> list[str]:
    no_line = re.sub(r"--[^\n]*", " ", sql)
    no_comments = re.sub(r"/\*.*?\*/", " ", no_line, flags=re.S)
    return re.findall(r"[A-Za-z_]+", no_comments.upper())


def _validate_reader_sql(query: Any) -> None:
    if not isinstance(query, str):
        return
    for statement in query.split(";"):
        s = statement.strip()
        if not s:
            continue
        tokens = _sql_tokens(s)
        if not tokens:
            continue
        first = tokens[0]
        if first not in _READER_ALLOWED_FIRST:
            raise PermissionError(f"Reader connection rejects SQL statement type: {first}")
        if first == "EXPLAIN":
            if len(tokens) < 2 or tokens[1] not in _READER_ALLOWED_EXPLAIN_NEXT:
                nxt = tokens[1] if len(tokens) > 1 else "<missing>"
                raise PermissionError(f"Reader connection rejects EXPLAIN target: {nxt}")
        for tok in tokens:
            if tok in _READER_FORBIDDEN_KEYWORDS:
                raise PermissionError(f"Reader connection rejects forbidden SQL keyword: {tok}")


class GuardedConnection:
    def __init__(self, con, *, guard_mode: str) -> None:
        self._con = con
        self._guard_mode = guard_mode

    def execute(self, query: Any, *args: Any, **kwargs: Any):
        self._validate_guard(query)
        return self._con.execute(query, *args, **kwargs)

    def executemany(self, query: Any, *args: Any, **kwargs: Any):
        self._validate_guard(query)
        return self._con.executemany(query, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)

    def _validate_guard(self, query: Any) -> None:
        try:
            if self._guard_mode == "reader":
                _validate_reader_sql(query)
            elif self._guard_mode == "debug_writer":
                _validate_debug_writer_sql(query)
        except PermissionError as exc:
            statement = query if isinstance(query, str) else None
            record_write_guard_block(guard_mode=self._guard_mode, reason=str(exc), statement=statement)
            log.warning("WRITE_GUARD_BLOCK mode=%s reason=%s", self._guard_mode, exc)
            raise


def connect_duckdb(cfg: DuckDBConfig):
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Missing dependency: duckdb. Install with: pip install duckdb") from exc

    strict, is_writer = _resolve_single_writer_mode()
    requested_apply_schema = env_flag("ASTERION_APPLY_SCHEMA")
    debug_direct_write = (
        env_flag("ALLOW_DIRECT_DB_WRITE")
        and os.getenv("ASTERION_DB_ROLE", "reader").strip().lower() == "writer"
        and os.getenv("ASTERION_ENV", "").strip().lower() == "dev"
        and not env_flag("WRITERD")
    )
    guard_mode = "none"

    if strict:
        read_only = not is_writer
        if debug_direct_write:
            read_only = False
            guard_mode = "debug_writer"
        apply_schema = is_writer and requested_apply_schema
        if requested_apply_schema and not is_writer:
            raise ValueError("apply_schema is only allowed for WRITERD=1 and ASTERION_DB_ROLE=writer.")
        if read_only:
            guard_mode = "reader"
    else:
        read_only = env_flag("ASTERION_DB_READ_ONLY", False)
        apply_schema = requested_apply_schema if "ASTERION_APPLY_SCHEMA" in os.environ else True

    if read_only and apply_schema:
        raise ValueError("Invalid DB config: read_only=True with apply_schema=True.")

    lock_retries = int(os.getenv("ASTERION_DUCKDB_LOCK_RETRIES", "10"))
    lock_sleep_s = float(os.getenv("ASTERION_DUCKDB_LOCK_SLEEP_S", "0.1"))
    lock_sleep_max_s = float(os.getenv("ASTERION_DUCKDB_LOCK_SLEEP_MAX_S", "2.0"))

    def _is_lock_error(err: BaseException) -> bool:
        msg = str(err)
        return (
            "Could not set lock on file" in msg
            or "Conflicting lock is held" in msg
            or "database is locked" in msg.lower()
        )

    con = None
    for attempt in range(lock_retries + 1):
        try:
            con = duckdb.connect(cfg.db_path, read_only=read_only)
            break
        except Exception as exc:  # noqa: BLE001
            if _is_lock_error(exc) and attempt < lock_retries:
                time.sleep(min(lock_sleep_max_s, lock_sleep_s * (2**attempt)))
                continue
            raise
    assert con is not None

    if apply_schema and cfg.ddl_path:
        with open(cfg.ddl_path, "r", encoding="utf-8") as handle:
            ddl_sql = handle.read().strip()
        if ddl_sql:
            con.execute(ddl_sql)

    return GuardedConnection(con, guard_mode=guard_mode)


def _schema_prefix_allowed(schema_name: str) -> bool:
    s = schema_name.lower()
    return s.startswith("scratch_") or s.startswith("dev_")


def _validate_debug_writer_sql(query: Any) -> None:
    if not isinstance(query, str):
        return
    for statement in query.split(";"):
        s = statement.strip()
        if not s:
            continue
        tokens = _sql_tokens(s)
        if not tokens:
            continue
        first = tokens[0]
        if first in _READER_ALLOWED_FIRST:
            if first == "EXPLAIN" and (len(tokens) < 2 or tokens[1] not in _READER_ALLOWED_EXPLAIN_NEXT):
                raise PermissionError("Debug writer rejects EXPLAIN for non-read statement.")
            continue
        if first not in _WRITE_STATEMENT_FIRST:
            raise PermissionError(f"Debug writer rejects statement type: {first}")
        for tok in tokens:
            if tok in _HARD_FORBIDDEN_RUNTIME_KEYWORDS:
                raise PermissionError(f"Debug writer rejects forbidden keyword: {tok}")
        schemas: set[str] = set()
        for match in re.finditer(r"(?is)\bCREATE\s+SCHEMA(?:\s+IF\s+NOT\s+EXISTS)?\s+([A-Za-z_][A-Za-z0-9_]*)", s):
            schemas.add(match.group(1))
        for match in re.finditer(r"(?is)\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)", s):
            schemas.add(match.group(1))
        if not schemas:
            raise PermissionError("Debug writer requires schema-qualified writes to scratch_/dev_ schemas.")
        for schema in schemas:
            if not _schema_prefix_allowed(schema):
                raise PermissionError(f"Debug writer rejects schema: {schema}")


def meta_start_run(con, *, run_id: str, job_name: str, source: str, params: dict[str, Any]) -> None:
    con.execute(
        """
        INSERT INTO meta.ingest_runs (
          run_id, job_name, source, started_at, status, params_json
        ) VALUES (?, ?, ?, now(), 'running', ?)
        """,
        [run_id, job_name, source, safe_json_dumps(params)],
    )


def meta_finish_run(
    con,
    *,
    run_id: str,
    status: str,
    rows_written: int | None = None,
    error_message: str | None = None,
) -> None:
    con.execute(
        """
        UPDATE meta.ingest_runs
        SET finished_at = now(),
            status = ?,
            rows_written = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        [status, rows_written, error_message, run_id],
    )


def meta_get_watermark_ms(
    con,
    *,
    source: str,
    endpoint: str,
    market_id: str,
    cursor_name: str = "max_timestamp_ms",
) -> int | None:
    row = con.execute(
        """
        SELECT cursor_value_ms
        FROM meta.watermarks
        WHERE source = ? AND endpoint = ? AND market_id = ? AND cursor_name = ?
        """,
        [source, endpoint, market_id, cursor_name],
    ).fetchone()
    if not row:
        return None
    return int(row[0])


def meta_set_watermark_ms(
    con,
    *,
    source: str,
    endpoint: str,
    market_id: str,
    value_ms: int,
    cursor_name: str = "max_timestamp_ms",
) -> None:
    con.execute(
        """
        DELETE FROM meta.watermarks
        WHERE source = ? AND endpoint = ? AND market_id = ? AND cursor_name = ?
        """,
        [source, endpoint, market_id, cursor_name],
    )
    con.execute(
        """
        INSERT INTO meta.watermarks (
          source, endpoint, market_id, cursor_name, cursor_value, cursor_value_ms, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, now())
        """,
        [source, endpoint, market_id, cursor_name, str(value_ms), int(value_ms)],
    )

