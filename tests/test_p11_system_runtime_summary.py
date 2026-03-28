from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ui.data_access import load_system_runtime_status


class P11SystemRuntimeSummaryTest(unittest.TestCase):
    def test_load_system_runtime_status_reads_persisted_triage_runtime_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.system_runtime_summary(
                        generated_at TIMESTAMP,
                        latest_surface_refresh_run_id TEXT,
                        latest_surface_refresh_status TEXT,
                        ui_replica_status TEXT,
                        ui_lite_status TEXT,
                        readiness_status TEXT,
                        weather_chain_status TEXT,
                        degraded_surface_count BIGINT,
                        read_error_surface_count BIGINT,
                        calibration_hard_gate_market_count BIGINT,
                        pending_operator_review_count BIGINT,
                        triage_latest_run_id TEXT,
                        triage_latest_run_status TEXT,
                        triage_latest_evaluation_method TEXT,
                        triage_advisory_gate_status TEXT,
                        triage_last_evaluated_at TIMESTAMP,
                        triage_failed_count BIGINT,
                        triage_pending_review_count BIGINT,
                        triage_accepted_count BIGINT,
                        triage_deferred_count BIGINT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.system_runtime_summary VALUES
                    (
                        '2026-03-23 10:05:00', 'refresh_1', 'ok', 'ok', 'ok', 'GO', 'ok', 0, 0, 0, 0,
                        'inv_1', 'success', 'replay_backtest', 'enabled', '2026-03-23 10:04:00', 1, 2, 3, 4
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.surface_delivery_summary(
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
                    ('system', 'ui.system_runtime_summary', 'ok', 'ui_lite', NULL, 'ok', 0, 1, '2026-03-23 10:05:00', '[]', 'surface_delivery_status')
                    """
                )
                con.execute("CREATE TABLE ui.market_opportunity_summary(market_id TEXT, actionability_status TEXT)")
                con.execute("INSERT INTO ui.market_opportunity_summary VALUES ('mkt_1', 'actionable')")
                con.execute("CREATE TABLE ui.agent_review_summary(agent_type TEXT)")
                con.execute("CREATE TABLE ui.opportunity_triage_summary(market_id TEXT, effective_triage_status TEXT, updated_at TIMESTAMP)")
                con.execute("INSERT INTO ui.opportunity_triage_summary VALUES ('mkt_1', 'accepted', '2026-03-23 10:04:00')")
                con.execute(
                    """
                    CREATE TABLE ui.calibration_health_summary(
                        station_id TEXT,
                        materialized_at TIMESTAMP,
                        impacted_market_count BIGINT,
                        hard_gate_market_count BIGINT,
                        review_required_market_count BIGINT,
                        research_only_market_count BIGINT
                    )
                    """
                )
            finally:
                con.close()

            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(db_path),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                    "ASTERION_READINESS_EVIDENCE_JSON_PATH": str(Path(tmpdir) / "missing_evidence.json"),
                    "ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH": str(Path(tmpdir) / "missing_manifest.json"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(Path(tmpdir) / "missing_smoke_report.json"),
                    "ASTERION_DB_PATH": str(Path(tmpdir) / "missing_runtime.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "missing_smoke.duckdb"),
                },
                clear=False,
            ):
                status = load_system_runtime_status()

        self.assertEqual(status["triage_latest_run_id"], "inv_1")
        self.assertEqual(status["triage_latest_run_status"], "success")
        self.assertEqual(status["triage_latest_evaluation_method"], "replay_backtest")
        self.assertEqual(status["triage_advisory_gate_status"], "enabled")
        self.assertEqual(str(status["triage_last_evaluated_at"]), "2026-03-23 10:04:00")
        self.assertEqual(status["triage_failed_count"], 1)
        self.assertEqual(status["triage_pending_review_count"], 2)
        self.assertEqual(status["triage_accepted_count"], 3)
        self.assertEqual(status["triage_deferred_count"], 4)


if __name__ == "__main__":
    unittest.main()
