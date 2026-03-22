#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.write_queue import WriteQueueConfig, init_queue
from dagster_asterion.handlers import run_weather_operator_surface_refresh_job
from dagster_asterion.resources import AsterionColdPathSettings, LivePrereqReadinessRuntimeResource


def main() -> int:
    settings = AsterionColdPathSettings.from_env()
    db_path = Path(settings.db_path)
    if not db_path.exists():
        print(
            json.dumps(
                {
                    "status": "skipped",
                    "reason": "canonical_db_missing",
                    "db_path": str(db_path),
                },
                ensure_ascii=True,
            )
        )
        return 0

    ui_runtime = LivePrereqReadinessRuntimeResource(settings)
    queue_cfg = WriteQueueConfig(path=settings.write_queue_path)
    init_queue(queue_cfg)

    old_db_role = os.environ.get("ASTERION_DB_ROLE")
    old_db_read_only = os.environ.get("ASTERION_DB_READ_ONLY")
    old_apply_schema = os.environ.get("ASTERION_APPLY_SCHEMA")
    os.environ["ASTERION_DB_ROLE"] = "reader"
    os.environ["ASTERION_DB_READ_ONLY"] = "1"
    os.environ.pop("ASTERION_APPLY_SCHEMA", None)

    try:
        con = connect_duckdb(DuckDBConfig(db_path=str(db_path), ddl_path=None))
        try:
            result = run_weather_operator_surface_refresh_job(
                con,
                queue_cfg,
                ui_replica_db_path=ui_runtime.resolve_ui_replica_db_path(),
                ui_replica_meta_path=ui_runtime.resolve_ui_replica_meta_path(),
                ui_lite_db_path=ui_runtime.resolve_ui_lite_db_path(),
                ui_lite_meta_path=ui_runtime.resolve_ui_lite_meta_path(),
                readiness_report_json_path=ui_runtime.resolve_readiness_report_json_path(),
                readiness_evidence_json_path=ui_runtime.resolve_readiness_evidence_json_path(),
            )
        finally:
            con.close()
    finally:
        if old_db_role is None:
            os.environ.pop("ASTERION_DB_ROLE", None)
        else:
            os.environ["ASTERION_DB_ROLE"] = old_db_role
        if old_db_read_only is None:
            os.environ.pop("ASTERION_DB_READ_ONLY", None)
        else:
            os.environ["ASTERION_DB_READ_ONLY"] = old_db_read_only
        if old_apply_schema is None:
            os.environ.pop("ASTERION_APPLY_SCHEMA", None)
        else:
            os.environ["ASTERION_APPLY_SCHEMA"] = old_apply_schema

    print(
        json.dumps(
            {
                "status": "ok",
                "job_name": result.job_name,
                "metadata": result.metadata,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {
                    "status": "error",
                    "error": str(exc),
                },
                ensure_ascii=True,
            ),
            file=sys.stderr,
        )
        raise
