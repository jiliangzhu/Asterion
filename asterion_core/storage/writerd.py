from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Any

from .database import DuckDBConfig, connect_duckdb
from .logger import get_logger
from .write_queue import (
    WriteQueueConfig,
    WriteTask,
    claim_next_tasks,
    mark_task_failed,
    mark_task_succeeded,
)

log = get_logger(__name__)

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_ALLOWED_TABLES = {
    "meta.ingest_runs",
    "meta.watermarks",
}


def _allowed_upsert_tables() -> set[str]:
    raw = os.getenv("ASTERION_WRITERD_ALLOWED_TABLES", "").strip()
    extra = {value.strip() for value in raw.split(",") if value.strip()}
    return DEFAULT_ALLOWED_TABLES | extra


@contextmanager
def _writer_env_scope(*, apply_schema: bool):
    keys = [
        "ASTERION_DB_READ_ONLY",
        "ASTERION_APPLY_SCHEMA",
        "ASTERION_STRICT_SINGLE_WRITER",
        "WRITERD",
        "ASTERION_WRITERD",
        "ASTERION_DB_ROLE",
    ]
    old = {key: os.environ.get(key) for key in keys}
    os.environ["ASTERION_DB_READ_ONLY"] = "0"
    os.environ["ASTERION_APPLY_SCHEMA"] = "1" if apply_schema else "0"
    os.environ["ASTERION_STRICT_SINGLE_WRITER"] = "1"
    os.environ["WRITERD"] = "1"
    os.environ["ASTERION_WRITERD"] = "1"
    os.environ["ASTERION_DB_ROLE"] = "writer"
    try:
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _validate_identifier(identifier: str, *, label: str) -> None:
    if not _SAFE_IDENTIFIER_RE.match(identifier):
        raise ValueError(f"invalid {label}: {identifier!r}")


def _validate_schema_table(table: str) -> None:
    if table not in _allowed_upsert_tables():
        raise ValueError(f"Refusing to write to table outside allow-list: {table}")
    if "." not in table:
        raise ValueError("table must be schema-qualified")
    schema, name = table.split(".", 1)
    _validate_identifier(schema, label="schema")
    _validate_identifier(name, label="table")


def _dedup_rows_by_pk(columns: list[str], pk_cols: list[str], rows: list[list[Any]]) -> list[list[Any]]:
    if not rows:
        return rows
    pk_idx = [columns.index(col) for col in pk_cols]
    latest_by_key: dict[tuple[Any, ...], list[Any]] = {}
    for row in rows:
        key = tuple(row[index] for index in pk_idx)
        latest_by_key[key] = row
    return list(latest_by_key.values())


def upsert_rows(con, *, table: str, pk_cols: list[str], columns: list[str], rows: list[list[Any]]) -> int:
    if not rows:
        return 0
    _validate_schema_table(table)
    for pk_col in pk_cols:
        if pk_col not in columns:
            raise ValueError(f"pk col missing from columns: {pk_col}")
        _validate_identifier(pk_col, label="pk col")
    for column in columns:
        _validate_identifier(column, label="column")

    rows = _dedup_rows_by_pk(columns, pk_cols, rows)
    tmp = f"tmp_writerd_{table.replace('.', '_')}_{os.getpid()}"
    _validate_identifier(tmp, label="temp table")
    col_sql = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    join_cond = " AND ".join([f"t.{pk} IS NOT DISTINCT FROM tmp.{pk}" for pk in pk_cols])
    set_cols = [column for column in columns if column not in pk_cols]
    set_sql = ", ".join([f"{column} = tmp.{column}" for column in set_cols])

    con.execute("BEGIN;")
    try:
        con.execute(f"CREATE TEMP TABLE {tmp} AS SELECT {col_sql} FROM {table} LIMIT 0;")
        con.executemany(f"INSERT INTO {tmp} ({col_sql}) VALUES ({placeholders});", rows)
        if set_cols:
            con.execute(f"UPDATE {table} t SET {set_sql} FROM {tmp} tmp WHERE {join_cond};")
        con.execute(
            f"""
            INSERT INTO {table} ({col_sql})
            SELECT {col_sql}
            FROM {tmp} tmp
            WHERE NOT EXISTS (
              SELECT 1
              FROM {table} t
              WHERE {join_cond}
            )
            """
        )
        con.execute(f"DROP TABLE {tmp};")
        con.execute("COMMIT;")
        return len(rows)
    except Exception:
        con.execute("ROLLBACK;")
        raise


def update_rows(con, *, table: str, pk_cols: list[str], columns: list[str], rows: list[list[Any]]) -> int:
    if not rows:
        return 0
    _validate_schema_table(table)
    for pk_col in pk_cols:
        if pk_col not in columns:
            raise ValueError(f"pk col missing from columns: {pk_col}")
        _validate_identifier(pk_col, label="pk col")
    for column in columns:
        _validate_identifier(column, label="column")

    rows = _dedup_rows_by_pk(columns, pk_cols, rows)
    set_cols = [column for column in columns if column not in pk_cols]
    if not set_cols:
        return 0
    tmp = f"tmp_writerd_update_{table.replace('.', '_')}_{os.getpid()}"
    _validate_identifier(tmp, label="temp table")
    col_sql = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    join_cond = " AND ".join([f"t.{pk} IS NOT DISTINCT FROM tmp.{pk}" for pk in pk_cols])
    set_sql = ", ".join([f"{column} = tmp.{column}" for column in set_cols])

    con.execute("BEGIN;")
    try:
        con.execute(f"CREATE TEMP TABLE {tmp} AS SELECT {col_sql} FROM {table} LIMIT 0;")
        con.executemany(f"INSERT INTO {tmp} ({col_sql}) VALUES ({placeholders});", rows)
        con.execute(f"UPDATE {table} t SET {set_sql} FROM {tmp} tmp WHERE {join_cond};")
        con.execute(f"DROP TABLE {tmp};")
        con.execute("COMMIT;")
        return len(rows)
    except Exception:
        con.execute("ROLLBACK;")
        raise


def _execute_task(con, task: WriteTask) -> tuple[str, str, int]:
    payload = task.payload
    if task.task_type == "UPSERT_ROWS_V1":
        table = str(payload["table"])
        pk_cols = [str(item) for item in payload["pk_cols"]]
        columns = [str(item) for item in payload["columns"]]
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            raise ValueError("rows must be a list")
        count = upsert_rows(con, table=table, pk_cols=pk_cols, columns=columns, rows=rows)
        return ("upsert", table, count)
    if task.task_type == "UPDATE_ROWS_V1":
        table = str(payload["table"])
        pk_cols = [str(item) for item in payload["pk_cols"]]
        columns = [str(item) for item in payload["columns"]]
        rows = payload.get("rows") or []
        if not isinstance(rows, list):
            raise ValueError("rows must be a list")
        count = update_rows(con, table=table, pk_cols=pk_cols, columns=columns, rows=rows)
        return ("update", table, count)
    raise ValueError(f"Unsupported task_type: {task.task_type}")


def _task_merge_signature(task: WriteTask) -> tuple[Any, ...] | None:
    if task.task_type not in {"UPSERT_ROWS_V1", "UPDATE_ROWS_V1"}:
        return None
    payload = task.payload
    try:
        table = str(payload["table"])
        pk_cols = tuple(str(item) for item in payload["pk_cols"])
        columns = tuple(str(item) for item in payload["columns"])
    except Exception:  # noqa: BLE001
        return None
    return (task.task_type, table, pk_cols, columns)


def _process_claimed_task(con, qcfg: WriteQueueConfig, task: WriteTask) -> None:
    try:
        action, table, count = _execute_task(con, task)
        mark_task_succeeded(qcfg, task_id=task.task_id)
        log.info("writerd %s ok task_id=%s table=%s rows=%s", action, task.task_id, table, count)
    except Exception as exc:  # noqa: BLE001
        log.exception("writerd task failed task_id=%s type=%s", task.task_id, task.task_type)
        mark_task_failed(qcfg, task_id=task.task_id, error_message=str(exc))


def _process_claimed_tasks(con, qcfg: WriteQueueConfig, tasks: list[WriteTask]) -> int:
    grouped: dict[tuple[Any, ...] | None, list[WriteTask]] = defaultdict(list)
    for task in tasks:
        grouped[_task_merge_signature(task)].append(task)

    processed = 0
    for signature, group in grouped.items():
        if signature is None or len(group) == 1:
            for task in group:
                _process_claimed_task(con, qcfg, task)
                processed += 1
            continue

        first = group[0]
        payload = first.payload
        table = str(payload["table"])
        pk_cols = [str(item) for item in payload["pk_cols"]]
        columns = [str(item) for item in payload["columns"]]
        merged_rows: list[list[Any]] = []
        try:
            for task in group:
                rows = task.payload.get("rows") or []
                if not isinstance(rows, list):
                    raise ValueError("rows must be a list")
                merged_rows.extend(rows)
            if first.task_type == "UPSERT_ROWS_V1":
                upsert_rows(con, table=table, pk_cols=pk_cols, columns=columns, rows=merged_rows)
            else:
                update_rows(con, table=table, pk_cols=pk_cols, columns=columns, rows=merged_rows)
            for task in group:
                mark_task_succeeded(qcfg, task_id=task.task_id)
            processed += len(group)
        except Exception as exc:  # noqa: BLE001
            log.warning("writerd batch failed; fallback single-task mode table=%s err=%s", table, exc)
            for task in group:
                _process_claimed_task(con, qcfg, task)
                processed += 1
    return processed


def process_batch(
    *,
    queue_path: str,
    batch_size: int = 32,
    db_path: str | None = None,
    ddl_path: str | None = None,
    apply_schema: bool = False,
) -> int:
    qcfg = WriteQueueConfig(path=queue_path)
    try:
        tasks = claim_next_tasks(qcfg, limit=max(1, int(batch_size)))
    except Exception as exc:  # noqa: BLE001
        log.exception("writerd claim_next_tasks failed: %s", exc)
        time.sleep(0.5)
        return 0
    if not tasks:
        return 0

    resolved_db_path = db_path or os.getenv("ASTERION_DB_PATH")
    if not resolved_db_path:
        raise ValueError("db_path is required")

    try:
        with _writer_env_scope(apply_schema=bool(apply_schema)):
            con = connect_duckdb(DuckDBConfig(db_path=resolved_db_path, ddl_path=ddl_path))
            try:
                return _process_claimed_tasks(con, qcfg, tasks)
            finally:
                con.close()
    except Exception as exc:  # noqa: BLE001
        log.exception("writerd batch failed before task processing: %s", exc)
        for task in tasks:
            try:
                mark_task_failed(qcfg, task_id=task.task_id, error_message=str(exc))
            except Exception:  # noqa: BLE001
                pass
        return len(tasks)


def process_one(
    *,
    queue_path: str,
    db_path: str | None = None,
    ddl_path: str | None = None,
    apply_schema: bool = False,
) -> bool:
    return process_batch(
        queue_path=queue_path,
        batch_size=1,
        db_path=db_path,
        ddl_path=ddl_path,
        apply_schema=apply_schema,
    ) > 0
