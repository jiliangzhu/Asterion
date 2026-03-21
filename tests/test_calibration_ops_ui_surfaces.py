from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from asterion_core.ui.ui_lite_db import _create_calibration_health_summary
from ui.data_access import load_system_runtime_status
from ui.pages.system import _build_component_rows


class CalibrationOpsUiSurfacesTest(unittest.TestCase):
    def test_calibration_health_summary_reads_profile_v2_table(self) -> None:
        con = duckdb.connect(":memory:")
        try:
            con.execute("ATTACH ':memory:' AS src")
            con.execute("CREATE SCHEMA ui")
            con.execute("CREATE SCHEMA src.weather")
            con.execute(
                """
                CREATE TABLE src.weather.forecast_calibration_profiles_v2 (
                    profile_key TEXT,
                    station_id TEXT,
                    source TEXT,
                    metric TEXT,
                    forecast_horizon_bucket TEXT,
                    season_bucket TEXT,
                    regime_bucket TEXT,
                    sample_count BIGINT,
                    mean_bias DOUBLE,
                    mean_abs_residual DOUBLE,
                    p90_abs_residual DOUBLE,
                    empirical_coverage_50 DOUBLE,
                    empirical_coverage_80 DOUBLE,
                    empirical_coverage_95 DOUBLE,
                    regime_stability_score DOUBLE,
                    residual_quantiles_json TEXT,
                    threshold_probability_profile_json TEXT,
                    calibration_health_status TEXT,
                    window_start TIMESTAMP,
                    window_end TIMESTAMP,
                    materialized_at TIMESTAMP
                )
                """
            )
            con.execute(
                """
                INSERT INTO src.weather.forecast_calibration_profiles_v2 VALUES (
                    'cp_1','KNYC','openmeteo','temperature_max','0-1','spring','warm',12,0.2,1.1,2.0,
                    0.5,0.8,0.95,0.9,'{\"p50\":1.0}','{\"60-75\":{\"quality_status\":\"healthy\"}}',
                    'healthy','2025-09-19 03:15:00','2026-03-18 02:00:00','2026-03-18 03:15:00'
                )
                """
            )
            counts: dict[str, int] = {}
            _create_calibration_health_summary(con, table_row_counts=counts)
            row = con.execute(
                """
                SELECT regime_bucket, threshold_profile_present, calibration_freshness_status, hard_gate_market_count
                FROM ui.calibration_health_summary
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(counts["ui.calibration_health_summary"], 1)
        self.assertEqual(row[0], "warm")
        self.assertTrue(row[1])
        self.assertIn(row[2], {"fresh", "stale", "degraded_or_missing"})
        self.assertEqual(int(row[3]), 0)

    def test_system_runtime_status_and_component_rows_expose_calibration_materialization_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            report_path = Path(tmpdir) / "readiness.json"
            manifest_path = Path(tmpdir) / "manifest.json"
            report_path.write_text(
                json.dumps(
                    {
                        "target": "p4_live_prerequisites",
                        "go_decision": "GO",
                        "decision_reason": "ok",
                        "evaluated_at": "2026-03-18T04:00:00+00:00",
                        "capability_manifest_status": "valid",
                    }
                ),
                encoding="utf-8",
            )
            manifest_path.write_text(json.dumps({"manifest_status": "valid"}), encoding="utf-8")
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.calibration_health_summary(
                        station_id TEXT,
                        source TEXT,
                        metric TEXT,
                        forecast_horizon_bucket TEXT,
                        season_bucket TEXT,
                        regime_bucket TEXT,
                        sample_count BIGINT,
                        mean_abs_residual DOUBLE,
                        p90_abs_residual DOUBLE,
                        calibration_health_status TEXT,
                        threshold_profile_present BOOLEAN,
                        window_end TIMESTAMP,
                        materialized_at TIMESTAMP,
                        calibration_freshness_status TEXT,
                        profile_age_hours DOUBLE,
                        impacted_market_count BIGINT,
                        hard_gate_market_count BIGINT,
                        review_required_market_count BIGINT,
                        research_only_market_count BIGINT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.calibration_health_summary VALUES
                    ('KNYC','openmeteo','temperature_max','0-1','spring','warm',12,1.1,2.0,'healthy',TRUE,'2026-03-18 02:00:00','2026-03-18 03:15:00','fresh',1.0,3,2,1,1)
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        question TEXT,
                        location_name TEXT,
                        market_close_time TIMESTAMP,
                        accepting_orders BOOLEAN,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        actionability_status TEXT
                    )
                    """
                )
                con.execute("INSERT INTO ui.market_opportunity_summary VALUES ('m1','q','NYC','2026-03-18 12:00:00',TRUE,'BUY',0.5,'actionable')")
                con.execute(
                    """
                    CREATE TABLE ui.agent_review_summary(
                        agent_type TEXT, subject_type TEXT, subject_id TEXT, invocation_status TEXT,
                        verdict TEXT, confidence DOUBLE, summary TEXT, human_review_required BOOLEAN, updated_at TIMESTAMP
                    )
                    """
                )
            finally:
                con.close()

            with patch.dict(
                "os.environ",
                {
                    "ASTERION_UI_LITE_DB_PATH": str(db_path),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(report_path),
                    "ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH": str(manifest_path),
                },
                clear=False,
            ):
                status = load_system_runtime_status()
                rows = _build_component_rows(status, {"report": {"target": "p4_live_prerequisites"}})

        self.assertEqual(status["latest_calibration_freshness_status"], "fresh")
        self.assertEqual(str(status["latest_calibration_materialized_at"]), "2026-03-18 03:15:00")
        self.assertEqual(status["calibration_hard_gate_market_count"], 2)
        self.assertTrue(any(row["组件"] == "Calibration Profiles v2" for row in rows))


if __name__ == "__main__":
    unittest.main()
