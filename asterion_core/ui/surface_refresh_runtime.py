from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb


@dataclass(frozen=True)
class OperatorSurfaceRefreshRunRecord:
    refresh_run_id: str
    job_name: str
    trigger_mode: str
    source_db_path: str
    ui_replica_ok: bool
    ui_lite_ok: bool
    truth_check_fail_count: int
    degraded_surface_count: int
    read_error_surface_count: int
    refreshed_at: datetime
    error: str | None = None


def persist_operator_surface_refresh_run(target: str | Path | Any, record: OperatorSurfaceRefreshRunRecord) -> None:
    if getattr(target, "_guard_mode", None) == "reader":
        return
    owns_connection = isinstance(target, (str, Path))
    con = duckdb.connect(str(target), read_only=False) if owns_connection else getattr(target, "_con", target)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS runtime")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime.operator_surface_refresh_runs (
                refresh_run_id TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                trigger_mode TEXT NOT NULL,
                source_db_path TEXT NOT NULL,
                ui_replica_ok BOOLEAN NOT NULL,
                ui_lite_ok BOOLEAN NOT NULL,
                truth_check_fail_count BIGINT NOT NULL,
                degraded_surface_count BIGINT NOT NULL,
                read_error_surface_count BIGINT NOT NULL,
                refreshed_at TIMESTAMP NOT NULL,
                error TEXT
            )
            """
        )
        con.execute(
            """
            INSERT OR REPLACE INTO runtime.operator_surface_refresh_runs (
                refresh_run_id,
                job_name,
                trigger_mode,
                source_db_path,
                ui_replica_ok,
                ui_lite_ok,
                truth_check_fail_count,
                degraded_surface_count,
                read_error_surface_count,
                refreshed_at,
                error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record.refresh_run_id,
                record.job_name,
                record.trigger_mode,
                record.source_db_path,
                record.ui_replica_ok,
                record.ui_lite_ok,
                int(record.truth_check_fail_count),
                int(record.degraded_surface_count),
                int(record.read_error_surface_count),
                _sql_ts(record.refreshed_at),
                record.error,
            ],
        )
    finally:
        if owns_connection:
            con.close()


def _sql_ts(value: datetime) -> str:
    normalized = value.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
