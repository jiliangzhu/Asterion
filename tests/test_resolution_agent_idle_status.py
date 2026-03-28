from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ui.data_access import load_system_runtime_status


class ResolutionAgentIdleStatusTest(unittest.TestCase):
    def test_load_system_runtime_status_reads_resolution_idle_no_subjects_from_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lite_path = Path(tmpdir) / "ui_lite.duckdb"
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            con = duckdb.connect(str(lite_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE TABLE ui.system_runtime_summary(generated_at TIMESTAMP)")
                con.execute("INSERT INTO ui.system_runtime_summary VALUES ('2026-03-23 12:00:00')")
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
                    ('system', 'ui.system_runtime_summary', 'ok', 'ui_lite', NULL, 'ok', 0, 1, '2026-03-23 12:00:00', '[]', 'surface_delivery_status')
                    """
                )
                con.execute("CREATE TABLE ui.market_opportunity_summary(market_id TEXT, actionability_status TEXT)")
                con.execute("CREATE TABLE ui.agent_review_summary(agent_type TEXT)")
                con.execute("CREATE TABLE ui.opportunity_triage_summary(market_id TEXT, effective_triage_status TEXT, updated_at TIMESTAMP)")
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
            report_path.write_text(
                json.dumps(
                    {
                        "runtime_chain": {
                            "opportunity_triage": {"status": "idle_no_subjects"},
                            "resolution_review": {"status": "idle_no_subjects"},
                        },
                        "roi_status": {
                            "path_closed": False,
                            "has_deployable_signals": False,
                            "has_empirical_feedback": False,
                            "agents_have_useful_output": False,
                        },
                        "truth_source": {"canonical_db_path": str(Path(tmpdir) / "canonical.duckdb"), "source_split_brain": False},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(lite_path),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                    "ASTERION_READINESS_EVIDENCE_JSON_PATH": str(Path(tmpdir) / "missing_evidence.json"),
                    "ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH": str(Path(tmpdir) / "missing_manifest.json"),
                    "ASTERION_DB_PATH": str(Path(tmpdir) / "canonical.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "legacy_smoke.duckdb"),
                },
                clear=False,
            ):
                status = load_system_runtime_status()

        self.assertEqual(status["triage_runtime_status"], "idle_no_subjects")
        self.assertEqual(status["resolution_runtime_status"], "idle_no_subjects")


if __name__ == "__main__":
    unittest.main()
