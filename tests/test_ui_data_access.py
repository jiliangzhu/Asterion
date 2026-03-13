from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ui.data_access import (
    build_ops_console_overview,
    load_agent_review_data,
    load_market_chain_analysis_data,
    load_readiness_summary,
    load_system_runtime_status,
    load_ui_lite_snapshot,
)
from ui.pages.system import _build_component_rows


class UiDataAccessTest(unittest.TestCase):
    def test_load_readiness_summary_uses_json_when_ui_lite_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "readiness.json"
            report_path.write_text(
                json.dumps(
                    {
                        "target": "p4_live_prerequisites",
                        "go_decision": "GO",
                        "decision_reason": "all readiness gates passed; ready for controlled live rollout decision",
                        "evaluated_at": "2026-03-13T10:00:00+00:00",
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing.duckdb"),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(report_path),
                },
                clear=False,
            ):
                summary = load_readiness_summary()
            self.assertEqual(summary["go_decision"], "GO")
            self.assertEqual(summary["target"], "p4_live_prerequisites")
            self.assertEqual(summary["failed_gate_names"], [])

    def test_load_ui_lite_snapshot_reads_existing_tables_and_tolerates_missing_ones(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE TABLE ui.execution_ticket_summary(ticket_id TEXT, wallet_id TEXT)")
                con.execute("INSERT INTO ui.execution_ticket_summary VALUES ('tt_1', 'wallet_weather_1')")
                con.execute("CREATE TABLE ui.phase_readiness_summary(gate_name TEXT, status TEXT)")
                con.execute("INSERT INTO ui.phase_readiness_summary VALUES ('live_prereq_operator_surface', 'PASS')")
                con.execute(
                    "CREATE TABLE ui.agent_review_summary(agent_type TEXT, subject_type TEXT, subject_id TEXT, invocation_status TEXT, verdict TEXT, confidence DOUBLE, summary TEXT, human_review_required BOOLEAN, updated_at TIMESTAMP)"
                )
                con.execute(
                    "INSERT INTO ui.agent_review_summary VALUES ('rule2spec','weather_market','mkt_1','success','review',0.9,'station-first looks good',false,'2026-03-13 10:00:00')"
                )
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                snapshot = load_ui_lite_snapshot()
            self.assertTrue(snapshot["exists"])
            self.assertEqual(snapshot["table_row_counts"]["execution_ticket_summary"], 1)
            self.assertEqual(snapshot["table_row_counts"]["live_prereq_wallet_summary"], 0)
            self.assertEqual(snapshot["table_row_counts"]["agent_review_summary"], 1)

    def test_load_agent_review_data_falls_back_to_smoke_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "timestamp": "2026-03-13T10:00:00+00:00",
                        "market_discovery": {
                            "selected_markets": [
                                {
                                    "market_id": "mkt_seattle_1",
                                    "question": "Seattle weather",
                                    "rule2spec_status": "success",
                                    "rule2spec_verdict": "review",
                                    "rule2spec_summary": "rule2spec completed",
                                    "data_qa_status": "not_run",
                                    "data_qa_summary": "no canonical forecast replay inputs in smoke chain",
                                    "resolution_status": "not_run",
                                    "resolution_summary": "no canonical resolution inputs in smoke chain",
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_DB_PATH": str(Path(tmpdir) / "missing_runtime.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "missing_smoke_runtime.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                },
                clear=False,
            ):
                payload = load_agent_review_data()
            self.assertEqual(payload["source"], "smoke_report")
            self.assertEqual(len(payload["frame"].index), 3)
            self.assertIn("rule2spec", payload["frame"]["agent_type"].tolist())

    def test_load_market_chain_analysis_data_merges_per_market_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "market_discovery": {
                            "selected_markets": [
                                {
                                    "market_id": "mkt_seattle_1",
                                    "question": "Seattle question",
                                    "location_name": "Seattle",
                                    "station_id": "KSEA",
                                    "accepting_orders": True,
                                    "rule2spec_status": "success",
                                    "rule2spec_verdict": "review",
                                    "rule2spec_summary": "rule2spec ok",
                                }
                            ]
                        },
                        "rule_parse": {
                            "selected_specs": [
                                {
                                    "market_id": "mkt_seattle_1",
                                    "location_name": "Seattle",
                                    "station_id": "KSEA",
                                    "bucket_min_value": 36,
                                    "bucket_max_value": 37,
                                    "metric": "highest_temperature_f",
                                    "authoritative_source": "weather.com",
                                }
                            ]
                        },
                        "forecast_service": {
                            "markets": [
                                {
                                    "market_id": "mkt_seattle_1",
                                    "forecast_run_id": "frun_1",
                                    "source_used": "nws",
                                    "source_trace": ["nws"],
                                }
                            ]
                        },
                        "pricing_engine": {
                            "markets": [
                                {
                                    "market_id": "mkt_seattle_1",
                                    "market_prices": {"YES": 0.4, "NO": 0.6},
                                    "fair_values": [{"outcome": "YES", "fair_value": 0.7}],
                                }
                            ]
                        },
                        "opportunity_discovery": {
                            "markets": [
                                {
                                    "market_id": "mkt_seattle_1",
                                    "signals": [{"outcome": "YES", "decision": "TAKE"}],
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                },
                clear=False,
            ):
                payload = load_market_chain_analysis_data()
            self.assertEqual(len(payload["market_rows"]), 1)
            self.assertEqual(payload["market_rows"][0]["spec"]["station_id"], "KSEA")
            self.assertEqual(payload["market_rows"][0]["forecast"]["source_used"], "nws")

    def test_console_overview_and_system_rows_handle_partial_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "weather_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "chain_status": "ok",
                        "market_discovery": {
                            "question": "Will the highest temperature in Seattle be between 36-37°F on March 13?",
                            "market_source": "gamma_events_api",
                            "selected_horizon_days": 14,
                            "market_id": "1557669",
                        }
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                },
                clear=False,
            ):
                overview = build_ops_console_overview()
                system_rows = _build_component_rows(load_system_runtime_status(), overview["readiness"])

            self.assertEqual(overview["metrics"]["weather_chain_status"], "ok")
            self.assertEqual(overview["metrics"]["weather_market_question"], "Will the highest temperature in Seattle be between 36-37°F on March 13?")
            self.assertEqual(overview["metrics"]["weather_market_count"], 0)
            self.assertEqual(len(system_rows), 6)


if __name__ == "__main__":
    unittest.main()
