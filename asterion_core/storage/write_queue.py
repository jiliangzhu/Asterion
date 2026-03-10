from __future__ import annotations

import dataclasses
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .utils import ensure_dir, safe_json_dumps


@dataclasses.dataclass(frozen=True)
class WriteQueueConfig:
    path: str


@dataclasses.dataclass(frozen=True)
class WriteTask:
    task_id: str
    task_type: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    created_ts_ms: int
    run_id: str | None


def default_write_queue_path() -> str:
    return os.getenv("ASTERION_WRITE_QUEUE", "data/meta/write_queue.sqlite")


def _now_ms() -> int:
    return int(time.time() * 1000)


@contextmanager
def _connect(path: str) -> Iterator[sqlite3.Connection]:
    ensure_dir(os.path.dirname(path) or ".")
    con = sqlite3.connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_queue(cfg: WriteQueueConfig) -> None:
    with _connect(cfg.path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS write_queue_tasks (
              task_id TEXT PRIMARY KEY,
              task_type TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              status TEXT NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 0,
              max_attempts INTEGER NOT NULL,
              created_ts_ms INTEGER NOT NULL,
              started_ts_ms INTEGER,
              ended_ts_ms INTEGER,
              error_message TEXT,
              run_id TEXT
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_write_queue_status_created
            ON write_queue_tasks(status, created_ts_ms)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_write_queue_status_started
            ON write_queue_tasks(status, started_ts_ms)
            """
        )


def enqueue_task(
    cfg: WriteQueueConfig,
    *,
    task_type: str,
    payload: dict[str, Any],
    run_id: str | None = None,
    max_attempts: int = 5,
    task_id: str | None = None,
) -> str:
    init_queue(cfg)
    tid = task_id or uuid.uuid4().hex
    with _connect(cfg.path) as con:
        con.execute(
            """
            INSERT OR REPLACE INTO write_queue_tasks (
              task_id, task_type, payload_json, status, attempts, max_attempts, created_ts_ms, run_id
            ) VALUES (?, ?, ?, 'PENDING', 0, ?, ?, ?)
            """,
            [tid, task_type, safe_json_dumps(payload), int(max_attempts), _now_ms(), run_id],
        )
    return tid


def _row_to_task(row: sqlite3.Row) -> WriteTask:
    payload = json.loads(str(row["payload_json"]))
    if not isinstance(payload, dict):
        raise ValueError("payload_json must decode to an object")
    return WriteTask(
        task_id=str(row["task_id"]),
        task_type=str(row["task_type"]),
        payload=payload,
        status=str(row["status"]),
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        created_ts_ms=int(row["created_ts_ms"]),
        run_id=str(row["run_id"]) if row["run_id"] is not None else None,
    )


def claim_next_tasks(cfg: WriteQueueConfig, *, limit: int = 1) -> list[WriteTask]:
    init_queue(cfg)
    n = max(1, int(limit))
    with _connect(cfg.path) as con:
        con.execute("BEGIN IMMEDIATE;")
        rows = con.execute(
            """
            SELECT task_id
            FROM write_queue_tasks
            WHERE status = 'PENDING'
            ORDER BY created_ts_ms ASC
            LIMIT ?
            """,
            [n],
        ).fetchall()
        if not rows:
            con.execute("COMMIT;")
            return []
        task_ids = [str(row["task_id"]) for row in rows]
        qmarks = ",".join(["?"] * len(task_ids))
        con.execute(
            f"""
            UPDATE write_queue_tasks
            SET status = 'RUNNING',
                started_ts_ms = ?
            WHERE status = 'PENDING'
              AND task_id IN ({qmarks})
            """,
            [_now_ms(), *task_ids],
        )
        con.execute("COMMIT;")
        full_rows = con.execute(
            f"""
            SELECT task_id, task_type, payload_json, status, attempts, max_attempts, created_ts_ms, run_id
            FROM write_queue_tasks
            WHERE status = 'RUNNING'
              AND task_id IN ({qmarks})
            ORDER BY created_ts_ms ASC
            """,
            task_ids,
        ).fetchall()
    return [_row_to_task(row) for row in full_rows]


def claim_next_task(cfg: WriteQueueConfig) -> WriteTask | None:
    tasks = claim_next_tasks(cfg, limit=1)
    return tasks[0] if tasks else None


def mark_task_succeeded(cfg: WriteQueueConfig, *, task_id: str) -> None:
    with _connect(cfg.path) as con:
        con.execute(
            """
            UPDATE write_queue_tasks
            SET status = 'SUCCEEDED',
                ended_ts_ms = ?,
                error_message = NULL
            WHERE task_id = ?
            """,
            [_now_ms(), task_id],
        )


def mark_task_failed(cfg: WriteQueueConfig, *, task_id: str, error_message: str) -> None:
    with _connect(cfg.path) as con:
        row = con.execute(
            "SELECT attempts, max_attempts FROM write_queue_tasks WHERE task_id = ?",
            [task_id],
        ).fetchone()
        if not row:
            return
        attempts = int(row["attempts"]) + 1
        max_attempts = int(row["max_attempts"])
        status = "DEAD" if attempts >= max_attempts else "FAILED"
        con.execute(
            """
            UPDATE write_queue_tasks
            SET status = ?,
                attempts = ?,
                ended_ts_ms = ?,
                error_message = ?
            WHERE task_id = ?
            """,
            [status, attempts, _now_ms(), error_message[:4000], task_id],
        )


def retry_failed(cfg: WriteQueueConfig, *, include_dead: bool = False) -> int:
    statuses = ["FAILED"] + (["DEAD"] if include_dead else [])
    with _connect(cfg.path) as con:
        qmarks = ",".join(["?"] * len(statuses))
        cur = con.execute(
            f"""
            UPDATE write_queue_tasks
            SET status = 'PENDING',
                started_ts_ms = NULL,
                ended_ts_ms = NULL,
                error_message = NULL
            WHERE status IN ({qmarks})
            """,
            statuses,
        )
        return int(cur.rowcount or 0)


def retry_stale_running(cfg: WriteQueueConfig, *, stale_ms: int = 120_000) -> int:
    cutoff_ms = _now_ms() - int(max(1, stale_ms))
    with _connect(cfg.path) as con:
        cur = con.execute(
            """
            UPDATE write_queue_tasks
            SET status = 'PENDING',
                started_ts_ms = NULL,
                ended_ts_ms = NULL,
                error_message = NULL
            WHERE status = 'RUNNING'
              AND started_ts_ms IS NOT NULL
              AND started_ts_ms <= ?
            """,
            [cutoff_ms],
        )
        return int(cur.rowcount or 0)


def get_task_statuses(cfg: WriteQueueConfig, *, task_ids: list[str]) -> dict[str, str]:
    if not task_ids:
        return {}
    init_queue(cfg)
    with _connect(cfg.path) as con:
        qmarks = ",".join(["?"] * len(task_ids))
        rows = con.execute(
            f"""
            SELECT task_id, status
            FROM write_queue_tasks
            WHERE task_id IN ({qmarks})
            """,
            task_ids,
        ).fetchall()
    return {str(row["task_id"]): str(row["status"]) for row in rows}

