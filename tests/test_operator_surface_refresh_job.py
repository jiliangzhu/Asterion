from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb

from asterion_core.storage.write_queue import WriteQueueConfig
from dagster_asterion.handlers import run_weather_operator_surface_refresh_job


def _write_ui_lite_contract(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE SCHEMA ui")
        con.execute(
            """
            CREATE TABLE ui.truth_source_checks (
                check_id TEXT,
                surface_id TEXT,
                table_name TEXT,
                check_status TEXT,
                issues_json TEXT,
                checked_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            INSERT INTO ui.truth_source_checks VALUES
            ('check_home', 'home', 'ui.action_queue_summary', 'ok', '[]', TIMESTAMP '2026-03-21 10:00:00'),
            ('check_system', 'system', 'ui.system_runtime_summary', 'ok', '[]', TIMESTAMP '2026-03-21 10:00:00')
            """
        )
        con.execute(
            """
            CREATE TABLE ui.surface_delivery_summary (
                surface_id TEXT,
                primary_table TEXT,
                delivery_status TEXT,
                primary_source TEXT,
                fallback_origin TEXT,
                truth_check_status TEXT,
                truth_check_issue_count BIGINT,
                row_count BIGINT,
                last_refresh_ts TIMESTAMP,
                degraded_reason_codes_json TEXT,
                primary_score_label TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO ui.surface_delivery_summary VALUES
            ('home', 'ui.action_queue_summary', 'ok', 'ui_lite', NULL, 'ok', 0, 3, TIMESTAMP '2026-03-21 10:00:00', '[]', 'surface_delivery_status'),
            ('system', 'ui.system_runtime_summary', 'degraded_source', 'ui_lite', 'runtime_db', 'warn', 1, 1, TIMESTAMP '2026-03-21 10:00:00', '["fallback:runtime_db"]', 'surface_delivery_status')
            """
        )
    finally:
        con.close()


class OperatorSurfaceRefreshJobTest(unittest.TestCase):
    def test_operator_surface_refresh_job_persists_runtime_audit_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            canonical_db_path = Path(tmpdir) / "asterion.duckdb"
            ui_lite_db_path = Path(tmpdir) / "ui_lite.duckdb"
            canonical_con = duckdb.connect(str(canonical_db_path))
            try:
                canonical_con.execute("CREATE TABLE bootstrap (id INTEGER)")
            finally:
                canonical_con.close()

            queue_cfg = WriteQueueConfig(path=str(Path(tmpdir) / "write_queue.sqlite"))

            def _build_ui_lite(*args, **kwargs):
                _write_ui_lite_contract(ui_lite_db_path)
                return SimpleNamespace(ok=True, error=None)

            with patch(
                "dagster_asterion.handlers.refresh_ui_db_replica_once",
                return_value=SimpleNamespace(ok=True, error=None),
            ), patch(
                "dagster_asterion.handlers.build_ui_lite_db_once",
                side_effect=_build_ui_lite,
            ), patch(
                "dagster_asterion.handlers._resolve_connection_db_path",
                return_value=str(canonical_db_path),
            ):
                con = duckdb.connect(str(canonical_db_path), read_only=False)
                try:
                    result = run_weather_operator_surface_refresh_job(
                        con,
                        queue_cfg,
                        ui_replica_db_path=str(Path(tmpdir) / "ui_replica.duckdb"),
                        ui_replica_meta_path=str(Path(tmpdir) / "ui_replica.meta.json"),
                        ui_lite_db_path=str(ui_lite_db_path),
                        ui_lite_meta_path=str(Path(tmpdir) / "ui_lite.meta.json"),
                        readiness_report_json_path=str(Path(tmpdir) / "readiness.json"),
                        readiness_evidence_json_path=str(Path(tmpdir) / "readiness_evidence.json"),
                        run_id="refresh_run_test",
                    )
                finally:
                    con.close()

            self.assertEqual(result.job_name, "weather_operator_surface_refresh")
            self.assertEqual(result.metadata["surface_refresh_run_id"], "refresh_run_test")
            self.assertEqual(result.metadata["truth_check_fail_count"], 0)
            self.assertEqual(result.metadata["degraded_surface_count"], 1)
            self.assertEqual(result.metadata["read_error_surface_count"], 0)

            read_con = duckdb.connect(str(canonical_db_path), read_only=True)
            try:
                row = read_con.execute(
                    """
                    SELECT job_name, trigger_mode, ui_replica_ok, ui_lite_ok, truth_check_fail_count, degraded_surface_count
                    FROM runtime.operator_surface_refresh_runs
                    WHERE refresh_run_id = 'refresh_run_test'
                    """
                ).fetchone()
            finally:
                read_con.close()
            self.assertEqual(tuple(row), ("weather_operator_surface_refresh", "scheduled", True, True, 0, 1))


if __name__ == "__main__":
    unittest.main()
