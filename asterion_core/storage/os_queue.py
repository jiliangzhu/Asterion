from __future__ import annotations

from typing import Any

from .write_queue import WriteQueueConfig, enqueue_task


def enqueue_upsert_rows_v1(
    queue_cfg: WriteQueueConfig,
    *,
    table: str,
    pk_cols: list[str],
    columns: list[str],
    rows: list[list[Any]],
    run_id: str | None = None,
) -> str:
    return enqueue_task(
        queue_cfg,
        task_type="UPSERT_ROWS_V1",
        payload={"table": table, "pk_cols": pk_cols, "columns": columns, "rows": rows},
        run_id=run_id,
    )


def enqueue_update_rows_v1(
    queue_cfg: WriteQueueConfig,
    *,
    table: str,
    pk_cols: list[str],
    columns: list[str],
    rows: list[list[Any]],
    run_id: str | None = None,
) -> str:
    return enqueue_task(
        queue_cfg,
        task_type="UPDATE_ROWS_V1",
        payload={"table": table, "pk_cols": pk_cols, "columns": columns, "rows": rows},
        run_id=run_id,
    )

