from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import ui.data_access as data_access_module

from ui.data_access import (
    build_ops_console_overview,
    load_agent_review_data,
    load_market_chain_analysis_data,
    load_market_opportunity_data,
    load_home_decision_snapshot,
    load_operator_surface_status,
    load_predicted_vs_realized_data,
    load_readiness_evidence_bundle,
    load_readiness_summary,
    load_system_runtime_status,
    load_ui_lite_snapshot,
)
from ui.pages.system import _build_component_rows


class UiDataAccessTest(unittest.TestCase):
    def test_load_readiness_summary_uses_json_when_ui_lite_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "readiness.json"
            manifest_path = Path(tmpdir) / "capability_manifest.json"
            report_path.write_text(
                json.dumps(
                    {
                        "target": "p4_live_prerequisites",
                        "go_decision": "GO",
                        "decision_reason": "all readiness gates passed; ready for controlled live rollout decision",
                        "evaluated_at": "2026-03-13T10:00:00+00:00",
                        "capability_boundary_summary": {
                            "manual_only": True,
                            "default_off": True,
                            "approve_usdc_only": True,
                            "shadow_submitter_only": True,
                            "manifest_status": "valid",
                        },
                        "capability_manifest_status": "valid",
                    }
                ),
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps({"manifest_status": "valid"}),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing.duckdb"),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(report_path),
                    "ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH": str(manifest_path),
                },
                clear=False,
            ):
                summary = load_readiness_summary()
            self.assertEqual(summary["go_decision"], "GO")
            self.assertEqual(summary["target"], "p4_live_prerequisites")
            self.assertEqual(summary["failed_gate_names"], [])
            self.assertEqual(summary["capability_manifest_status"], "valid")

    def test_load_ui_lite_snapshot_reads_existing_tables_and_tolerates_missing_ones(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE TABLE ui.execution_ticket_summary(ticket_id TEXT, wallet_id TEXT)")
                con.execute("INSERT INTO ui.execution_ticket_summary VALUES ('tt_1', 'wallet_weather_1')")
                con.execute(
                    "CREATE TABLE ui.market_opportunity_summary(market_id TEXT, question TEXT, location_name TEXT, market_close_time TIMESTAMP, accepting_orders BOOLEAN, best_side TEXT, market_price DOUBLE, fair_value DOUBLE, edge_bps DOUBLE, liquidity_proxy DOUBLE, confidence_proxy DOUBLE, agent_review_status TEXT, live_prereq_status TEXT, opportunity_bucket TEXT, opportunity_score DOUBLE, actionability_status TEXT)"
                )
                con.execute(
                    "INSERT INTO ui.market_opportunity_summary VALUES ('mkt_1','Seattle question','Seattle','2026-03-13 12:00:00',true,'BUY',0.41,0.67,1300,72,85,'passed','not_started','medium_edge',84.5,'actionable')"
                )
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
            self.assertEqual(snapshot["table_row_counts"]["market_opportunity_summary"], 1)
            self.assertEqual(snapshot["table_row_counts"]["live_prereq_wallet_summary"], 0)
            self.assertEqual(snapshot["table_row_counts"]["agent_review_summary"], 1)

    def test_load_market_opportunity_data_prefers_ui_lite_and_sorts_by_actionability_then_score(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    "CREATE TABLE ui.market_opportunity_summary(market_id TEXT, question TEXT, location_name TEXT, market_close_time TIMESTAMP, accepting_orders BOOLEAN, best_side TEXT, market_price DOUBLE, fair_value DOUBLE, edge_bps DOUBLE, liquidity_proxy DOUBLE, confidence_proxy DOUBLE, agent_review_status TEXT, live_prereq_status TEXT, opportunity_bucket TEXT, opportunity_score DOUBLE, actionability_status TEXT)"
                )
                con.execute(
                    "INSERT INTO ui.market_opportunity_summary VALUES "
                    "('mkt_review','Review market','Seattle','2026-03-13 12:00:00',true,'BUY',0.41,0.67,1400,70,60,'review_required','not_started','medium_edge',82.0,'review_required'),"
                    "('mkt_actionable','Actionable market','Atlanta','2026-03-13 14:00:00',true,'BUY',0.38,0.74,1100,78,85,'passed','not_started','medium_edge',83.0,'actionable'),"
                    "('mkt_no_trade','No trade market','Miami','2026-03-14 14:00:00',true,NULL,0.49,0.49,0,55,50,'no_agent_signal','not_started','negative_edge',19.0,'no_trade')"
                )
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                payload = load_market_opportunity_data()

            self.assertEqual(payload["source"], "ui_lite")
            self.assertEqual(payload["frame"].iloc[0]["market_id"], "mkt_actionable")
            self.assertEqual(payload["frame"].iloc[1]["market_id"], "mkt_review")

    def test_build_opportunity_row_keeps_sell_edge_negative_and_reduced_by_costs(self) -> None:
        row = data_access_module._build_opportunity_row(
            market_id="mkt_sell",
            question="Will Seattle stay below threshold?",
            location_name="Seattle",
            station_id="KSEA",
            market_close_time="2026-03-20T12:00:00+00:00",
            accepting_orders=True,
            enable_order_book=True,
            token_id="tok_sell",
            outcome="NO",
            reference_price=0.70,
            model_fair_value=0.50,
            threshold_bps=500,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=82.0,
            latest_run_source="nws",
            latest_forecast_target_time="2026-03-20T00:00:00+00:00",
            signal_created_at="2026-03-19T00:00:00+00:00",
            mapping_confidence=0.92,
            source_freshness_status="fresh",
            price_staleness_ms=60_000,
            spread_bps=40,
            calibration_health_status="limited_samples",
            sample_count=7,
            calibration_multiplier=0.75,
            calibration_reason_codes=["calibration_limited_samples"],
        )
        self.assertEqual(row["best_side"], "SELL")
        self.assertLess(row["edge_bps_executable"], 0)
        self.assertLess(abs(row["edge_bps_executable"]), abs(row["edge_bps_model"]))
        self.assertGreater(row["ranking_score"], 0)
        self.assertEqual(row["calibration_health_status"], "limited_samples")
        self.assertEqual(row["sample_count"], 7)
        self.assertLess(row["uncertainty_multiplier"], 1.0)
        self.assertIn("calibration_limited_samples", row["ranking_penalty_reasons"])
        self.assertGreater(row["expected_dollar_pnl"], 0.0)
        self.assertGreater(row["capture_probability"], 0.0)
        self.assertEqual(row["why_ranked_json"]["version"], "ranking_v2")
        self.assertEqual(row["why_ranked_json"]["mode"], "fallback_heuristic")

    def test_load_market_opportunity_data_preserves_ranking_v2_fields_from_ui_lite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        question TEXT,
                        location_name TEXT,
                        market_close_time TIMESTAMP,
                        accepting_orders BOOLEAN,
                        best_side TEXT,
                        market_price DOUBLE,
                        fair_value DOUBLE,
                        edge_bps DOUBLE,
                        ranking_score DOUBLE,
                        opportunity_score DOUBLE,
                        actionability_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        is_degraded_source BOOLEAN,
                        primary_score_label TEXT,
                        expected_dollar_pnl DOUBLE,
                        capture_probability DOUBLE,
                        risk_penalty DOUBLE,
                        capital_efficiency DOUBLE,
                        feedback_penalty DOUBLE,
                        feedback_status TEXT,
                        cohort_prior_version TEXT,
                        why_ranked_json TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES (
                        'mkt_why', 'Seattle weather', 'Seattle', '2026-03-18 12:00:00', TRUE, 'BUY',
                        0.40, 0.55, 900, 0.235, 0.235, 'actionable', 'canonical', 'canonical', FALSE,
                        'ranking_score', 0.15, 0.72, 0.02, 1.8, 0.18, 'watch', 'feedback_v1', ?
                    )
                    """,
                    [json.dumps({"version": "ranking_v2", "mode": "prior_backed", "ranking_score": 0.235, "feedback_penalty": 0.18})],
                )
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                payload = load_market_opportunity_data()

        row = payload["frame"].iloc[0].to_dict()
        self.assertEqual(row["primary_score_label"], "ranking_score")
        self.assertEqual(row["ranking_score"], 0.235)
        self.assertEqual(json.loads(row["why_ranked_json"])["mode"], "prior_backed")
        self.assertEqual(row["feedback_status"], "watch")
        self.assertEqual(row["cohort_prior_version"], "feedback_v1")
        self.assertEqual(row["feedback_penalty"], 0.18)

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

    def test_load_agent_review_data_tolerates_locked_runtime_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "timestamp": "2026-03-15T00:00:00+00:00",
                        "market_discovery": {
                            "selected_markets": [
                                {
                                    "market_id": "mkt_1",
                                    "rule2spec_status": "success",
                                    "rule2spec_verdict": "review",
                                    "rule2spec_summary": "fallback row",
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
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "locked_smoke_runtime.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                },
                clear=False,
            ):
                with patch.object(data_access_module.duckdb, "connect", side_effect=RuntimeError("lock conflict")):
                    payload = load_agent_review_data()
            self.assertEqual(payload["source"], "smoke_report")
            self.assertEqual(len(payload["frame"].index), 1)

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
                                    "signals": [
                                        {
                                            "outcome": "YES",
                                            "decision": "TAKE",
                                            "mapping_confidence": 0.92,
                                            "source_freshness_status": "fresh",
                                            "price_staleness_ms": 60000,
                                            "market_quality_status": "review_required",
                                        }
                                    ],
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
            self.assertEqual(payload["market_rows"][0]["actionability_status"], "review_required")
            self.assertEqual(payload["market_opportunity_source"], "smoke_report")

    def test_load_readiness_evidence_bundle_prefers_ui_lite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.readiness_evidence_summary(
                        generated_at TIMESTAMP,
                        go_decision TEXT,
                        decision_reason TEXT,
                        capability_manifest_status TEXT,
                        capability_boundary_summary_json TEXT,
                        dependency_statuses_json TEXT,
                        artifact_freshness_json TEXT,
                        latest_verification_summary_json TEXT,
                        stale_dependencies_json TEXT,
                        blockers_json TEXT,
                        warnings_json TEXT,
                        evidence_paths_json TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.readiness_evidence_summary VALUES (
                        '2026-03-15 10:00:00',
                        'GO',
                        'ready with evidence',
                        'valid',
                        '{"manual_only": true}',
                        '{"ui_lite_db":{"status":"ok"}}',
                        '{"ui_lite_db":{"age_seconds":12}}',
                        '{"gate_count":1}',
                        '[]',
                        '[]',
                        '["warn:smoke"]',
                        '{"readiness_report_json":"data/ui/asterion_readiness_p4.json"}'
                    )
                    """
                )
            finally:
                con.close()
            with patch.dict(os.environ, {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                payload = load_readiness_evidence_bundle()
            self.assertEqual(payload["source"], "ui_lite")
            self.assertEqual(payload["go_decision"], "GO")
            self.assertEqual(payload["capability_boundary_summary"]["manual_only"], True)
            self.assertEqual(payload["warnings"], ["warn:smoke"])

    def test_load_predicted_vs_realized_data_and_market_executed_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        question TEXT,
                        location_name TEXT,
                        market_close_time TIMESTAMP,
                        accepting_orders BOOLEAN,
                        best_side TEXT,
                        market_price DOUBLE,
                        fair_value DOUBLE,
                        edge_bps DOUBLE,
                        liquidity_proxy DOUBLE,
                        confidence_proxy DOUBLE,
                        agent_review_status TEXT,
                        live_prereq_status TEXT,
                        opportunity_bucket TEXT,
                        opportunity_score DOUBLE,
                        actionability_status TEXT,
                        ranking_score DOUBLE
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_1','Seattle weather','Seattle','2026-03-15 12:00:00',true,'BUY',0.41,0.68,900,80,75,'passed','shadow_aligned','medium_edge',88.0,'actionable',88.0)
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.predicted_vs_realized_summary(
                        ticket_id TEXT,
                        run_id TEXT,
                        wallet_id TEXT,
                        strategy_id TEXT,
                        market_id TEXT,
                        order_id TEXT,
                        outcome TEXT,
                        predicted_edge_bps DOUBLE,
                        expected_fill_price DOUBLE,
                        realized_fill_price DOUBLE,
                        filled_quantity DOUBLE,
                        realized_notional DOUBLE,
                        realized_pnl DOUBLE,
                        resolution_value DOUBLE,
                        forecast_freshness TEXT,
                        source_disagreement TEXT,
                        post_trade_error DOUBLE,
                        evaluation_status TEXT,
                        latest_fill_at TIMESTAMP,
                        latest_resolution_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.predicted_vs_realized_summary VALUES (
                        'tt_1','run_1','wallet_weather_1','weather_primary','mkt_1','ord_1','YES',
                        900,0.40,0.42,10,4.2,5.7,1.0,'fresh','different',4.8,'resolved',
                        '2026-03-15 09:01:00','2026-03-15 10:00:00'
                    )
                    """
                )
            finally:
                con.close()
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            report_path.write_text(
                json.dumps({"market_discovery": {"selected_markets": [{"market_id": "mkt_1", "question": "Seattle weather"}]}}),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(db_path),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                },
                clear=False,
            ):
                predicted = load_predicted_vs_realized_data()
                market_payload = load_market_chain_analysis_data()
            self.assertEqual(predicted["frame"].iloc[0]["ticket_id"], "tt_1")
            executed = market_payload["market_rows"][0]["executed_evidence"]
            self.assertEqual(executed["evaluation_status"], "resolved")
            self.assertEqual(executed["source_disagreement"], "different")

    def test_load_market_chain_analysis_data_keeps_discovered_rows_when_pricing_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "chain_status": "degraded",
                        "market_discovery": {
                            "selected_markets": [
                                {
                                    "market_id": "mkt_nyc_1",
                                    "question": "NYC question",
                                    "location_name": "New York City",
                                    "station_id": "KNYC",
                                    "accepting_orders": True,
                                    "rule2spec_status": "success",
                                    "rule2spec_verdict": "review",
                                    "rule2spec_summary": "rule2spec ok",
                                    "forecast_status": "failure",
                                    "forecast_summary": "forecast_fetch_failed:tls eof",
                                }
                            ]
                        },
                        "rule_parse": {
                            "selected_specs": [
                                {
                                    "market_id": "mkt_nyc_1",
                                    "location_name": "New York City",
                                    "station_id": "KNYC",
                                }
                            ]
                        },
                        "forecast_service": {
                            "status": "degraded",
                            "markets": [],
                        },
                        "pricing_engine": {
                            "status": "degraded",
                            "markets": [],
                        },
                        "opportunity_discovery": {
                            "status": "degraded",
                            "markets": [],
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
            self.assertEqual(payload["market_rows"][0]["forecast_status"], "failure")
            self.assertEqual(len(payload["market_opportunities"]), 1)
            self.assertEqual(payload["market_opportunities"].iloc[0]["market_id"], "mkt_nyc_1")

    def test_load_market_chain_analysis_data_falls_back_to_runtime_smoke_db_when_report_is_initializing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "chain_status": "initializing",
                        "refresh_state": "initializing",
                        "market_discovery": {
                            "selected_markets": [],
                            "discovered_count": 0,
                        },
                    }
                ),
                encoding="utf-8",
            )
            runtime_path = Path(tmpdir) / "real_weather_chain.duckdb"
            con = duckdb.connect(str(runtime_path))
            try:
                con.execute("CREATE SCHEMA weather")
                con.execute("CREATE SCHEMA agent")
                con.execute(
                    """
                    CREATE TABLE weather.weather_markets(
                        market_id TEXT, condition_id TEXT, event_id TEXT, slug TEXT, title TEXT, description TEXT, rules TEXT,
                        status TEXT, active BOOLEAN, closed BOOLEAN, archived BOOLEAN, accepting_orders BOOLEAN, enable_order_book BOOLEAN,
                        tags_json TEXT, outcomes_json TEXT, token_ids_json TEXT, close_time TIMESTAMP, end_date TIMESTAMP,
                        raw_market_json TEXT, created_at TIMESTAMP, updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_markets VALUES
                    ('mkt_sea_1','cond_1','evt_1','slug_1','Seattle market',NULL,NULL,'active',true,false,false,true,true,'[]','[]','[]','2026-03-15 12:00:00','2026-03-15 12:00:00','{}','2026-03-15 00:00:00','2026-03-15 00:00:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE weather.weather_market_specs(
                        market_id TEXT, condition_id TEXT, location_name TEXT, station_id TEXT, latitude DOUBLE, longitude DOUBLE, timezone TEXT,
                        observation_date DATE, observation_window_local TEXT, metric TEXT, unit TEXT, bucket_min_value DOUBLE, bucket_max_value DOUBLE,
                        authoritative_source TEXT, fallback_sources TEXT, rounding_rule TEXT, inclusive_bounds BOOLEAN, spec_version TEXT,
                        parse_confidence DOUBLE, risk_flags_json TEXT, created_at TIMESTAMP, updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_market_specs VALUES
                    ('mkt_sea_1','cond_1','Seattle','KSEA',47.6,-122.3,'America/Los_Angeles','2026-03-15','daily_max','temperature_max','fahrenheit',36,37,'weather.com','[]','identity',true,'spec_1',0.9,'[]','2026-03-15 00:00:00','2026-03-15 00:00:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE weather.weather_station_map(
                        map_id TEXT, market_id TEXT, location_name TEXT, location_key TEXT, station_id TEXT, station_name TEXT,
                        latitude DOUBLE, longitude DOUBLE, timezone TEXT, source TEXT, authoritative_source TEXT, is_override BOOLEAN,
                        mapping_method TEXT, mapping_confidence DOUBLE, override_reason TEXT, metadata_json TEXT,
                        created_at TIMESTAMP, updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_station_map VALUES
                    ('map_1','mkt_sea_1','Seattle','seattle','KSEA','Seattle TAC',47.6,-122.3,'America/Los_Angeles','operator_override','weather.com',TRUE,'market_override',0.92,'runtime_fixture','{}','2026-03-15 00:00:00','2026-03-15 00:00:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE weather.source_health_snapshots(
                        snapshot_id TEXT, market_id TEXT, station_id TEXT, source TEXT, market_updated_at TIMESTAMP,
                        forecast_created_at TIMESTAMP, snapshot_created_at TIMESTAMP, price_staleness_ms BIGINT,
                        forecast_age_ms BIGINT, source_freshness_status TEXT, degraded_reason_codes_json TEXT, created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.source_health_snapshots VALUES
                    ('health_1','mkt_sea_1','KSEA','openmeteo','2026-03-15 00:00:00','2026-03-15 00:10:00','2026-03-15 00:11:00',600000,60000,'fresh','[]','2026-03-15 00:11:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.invocations(
                        invocation_id TEXT, agent_type TEXT, agent_version TEXT, prompt_version TEXT, subject_type TEXT, subject_id TEXT,
                        input_hash TEXT, input_payload_json TEXT, model_provider TEXT, model_name TEXT, status TEXT, error_message TEXT,
                        started_at TIMESTAMP, ended_at TIMESTAMP, latency_ms BIGINT, force_rerun BOOLEAN, force_rerun_token TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO agent.invocations VALUES
                    ('inv_1','rule2spec','v1','p1','weather_market','mkt_sea_1','hash','{}','openai_compatible','glm-5','success',NULL,'2026-03-15 00:01:00','2026-03-15 00:01:05',5000,false,NULL)
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.outputs(
                        output_id TEXT, invocation_id TEXT, verdict TEXT, confidence DOUBLE, summary TEXT, findings_json TEXT,
                        structured_output_json TEXT, human_review_required BOOLEAN, created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO agent.outputs VALUES
                    ('out_1','inv_1','review',0.8,'station-first review required','[]','{}',true,'2026-03-15 00:01:05')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.reviews(
                        review_id TEXT, invocation_id TEXT, review_status TEXT, reviewer_id TEXT, review_payload_json TEXT, reviewed_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.evaluations(
                        evaluation_id TEXT, invocation_id TEXT, confidence DOUBLE, human_review_required BOOLEAN, score_json TEXT, created_at TIMESTAMP
                    )
                    """
                )
            finally:
                con.close()

            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(runtime_path),
                },
                clear=False,
            ):
                payload = load_market_chain_analysis_data()

            self.assertEqual(payload["market_opportunity_source"], "weather_smoke_db")
            self.assertEqual(len(payload["market_rows"]), 1)
            self.assertEqual(payload["market_rows"][0]["location_name"], "Seattle")
            self.assertEqual(payload["market_rows"][0]["rule2spec_status"], "success")
            self.assertEqual(payload["market_rows"][0]["forecast_status"], "not_started")

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
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "missing_smoke_runtime.duckdb"),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                },
                clear=False,
            ):
                overview = build_ops_console_overview()
                system_rows = _build_component_rows(load_system_runtime_status(), overview["readiness"])

            self.assertEqual(overview["metrics"]["weather_chain_status"], "ok")
            self.assertEqual(overview["metrics"]["weather_market_question"], "Will the highest temperature in Seattle be between 36-37°F on March 13?")
            self.assertEqual(overview["metrics"]["weather_market_count"], 0)
            self.assertEqual(len(system_rows), 7)

    def test_load_home_decision_snapshot_surfaces_largest_blocker_and_recent_agent_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "readiness.json"
            report_path.write_text(
                json.dumps(
                    {
                        "target": "p4_live_prerequisites",
                        "go_decision": "NO-GO",
                        "decision_reason": "failed gates: signer_path_health; not ready for controlled live rollout decision",
                        "evaluated_at": "2026-03-13T10:00:00+00:00",
                    }
                ),
                encoding="utf-8",
            )
            ui_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(ui_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE TABLE ui.phase_readiness_summary(gate_name TEXT, status TEXT)")
                con.execute("INSERT INTO ui.phase_readiness_summary VALUES ('signer_path_health', 'FAIL')")
                con.execute(
                    "CREATE TABLE ui.market_opportunity_summary(market_id TEXT, question TEXT, location_name TEXT, market_close_time TIMESTAMP, accepting_orders BOOLEAN, best_side TEXT, market_price DOUBLE, fair_value DOUBLE, edge_bps DOUBLE, liquidity_proxy DOUBLE, confidence_proxy DOUBLE, agent_review_status TEXT, live_prereq_status TEXT, opportunity_bucket TEXT, opportunity_score DOUBLE, actionability_status TEXT)"
                )
                con.execute(
                    "INSERT INTO ui.market_opportunity_summary VALUES ('mkt_1','Seattle question','Seattle','2026-03-13 12:00:00',true,'BUY',0.41,0.67,1300,72,85,'passed','not_started','medium_edge',84.5,'actionable')"
                )
                con.execute(
                    "CREATE TABLE ui.agent_review_summary(agent_type TEXT, subject_type TEXT, subject_id TEXT, invocation_status TEXT, verdict TEXT, confidence DOUBLE, summary TEXT, human_review_required BOOLEAN, updated_at TIMESTAMP)"
                )
                con.execute(
                    "INSERT INTO ui.agent_review_summary VALUES ('rule2spec','weather_market','mkt_1','success','review',0.9,'station-first looks good',true,'2026-03-13 10:00:00')"
                )
            finally:
                con.close()

            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(ui_path),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(report_path),
                },
                clear=False,
            ):
                snapshot = load_home_decision_snapshot()

            self.assertEqual(snapshot["largest_blocker"]["source"], "readiness")
            self.assertEqual(snapshot["recent_agent_summary"]["agent_type"], "rule2spec")

    def test_load_operator_surface_status_reports_no_data_when_everything_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(Path(tmpdir) / "missing_weather_report.json"),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "missing_weather_runtime.duckdb"),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                },
                clear=False,
            ):
                status = load_operator_surface_status()

        self.assertEqual(status["readiness"]["status"], "no_data")
        self.assertEqual(status["market_chain"]["status"], "no_data")
        self.assertEqual(status["agent_review"]["status"], "no_data")
        self.assertEqual(status["overall"]["status"], "no_data")

    def test_load_operator_surface_status_reports_refresh_in_progress_from_market_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "weather_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "chain_status": "initializing",
                        "refresh_state": "initializing",
                        "refresh_note": "refresh in progress",
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "missing_weather_runtime.duckdb"),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                },
                clear=False,
            ):
                status = load_operator_surface_status()

        self.assertEqual(status["market_chain"]["status"], "refresh_in_progress")
        self.assertEqual(status["overall"]["status"], "refresh_in_progress")

    def test_load_operator_surface_status_reports_read_error_for_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "weather_report.json"
            report_path.write_text("{invalid", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "missing_weather_runtime.duckdb"),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                },
                clear=False,
            ):
                status = load_operator_surface_status()

        self.assertEqual(status["market_chain"]["status"], "read_error")
        self.assertEqual(status["overall"]["status"], "read_error")

    def test_load_operator_surface_status_reports_degraded_source_when_runtime_fallback_is_used(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_path = Path(tmpdir) / "real_weather_chain.duckdb"
            con = duckdb.connect(str(runtime_path))
            try:
                con.execute("CREATE SCHEMA weather")
                con.execute("CREATE SCHEMA agent")
                con.execute(
                    """
                    CREATE TABLE weather.weather_markets(
                        market_id TEXT, title TEXT, close_time TIMESTAMP, end_date TIMESTAMP,
                        active BOOLEAN, closed BOOLEAN, archived BOOLEAN, accepting_orders BOOLEAN
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE weather.weather_market_specs(
                        market_id TEXT, location_name TEXT, station_id TEXT, authoritative_source TEXT,
                        metric TEXT, bucket_min_value DOUBLE, bucket_max_value DOUBLE, observation_window_local TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_markets VALUES
                    ('mkt_1','Seattle weather','2026-03-15 12:00:00','2026-03-15 12:00:00',TRUE,FALSE,FALSE,TRUE)
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_market_specs VALUES
                    ('mkt_1','Seattle','KSEA','weather.com','temperature_max',36,37,'daily_max')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE weather.weather_station_map(
                        map_id TEXT, market_id TEXT, location_name TEXT, location_key TEXT, station_id TEXT, station_name TEXT,
                        latitude DOUBLE, longitude DOUBLE, timezone TEXT, source TEXT, authoritative_source TEXT, is_override BOOLEAN,
                        mapping_method TEXT, mapping_confidence DOUBLE, override_reason TEXT, metadata_json TEXT,
                        created_at TIMESTAMP, updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.weather_station_map VALUES
                    ('map_1','mkt_1','Seattle','seattle','KSEA','Seattle TAC',47.6,-122.3,'America/Los_Angeles','operator_override','weather.com',TRUE,'market_override',0.88,'runtime_fixture','{}','2026-03-15 09:00:00','2026-03-15 09:00:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE weather.source_health_snapshots(
                        snapshot_id TEXT, market_id TEXT, station_id TEXT, source TEXT, market_updated_at TIMESTAMP,
                        forecast_created_at TIMESTAMP, snapshot_created_at TIMESTAMP, price_staleness_ms BIGINT,
                        forecast_age_ms BIGINT, source_freshness_status TEXT, degraded_reason_codes_json TEXT, created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO weather.source_health_snapshots VALUES
                    ('health_1','mkt_1','KSEA','openmeteo','2026-03-15 09:00:00','2026-03-15 09:00:00','2026-03-15 09:01:00',120000,60000,'fresh','[]','2026-03-15 09:01:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.invocations(
                        invocation_id TEXT, agent_type TEXT, subject_type TEXT, subject_id TEXT, status TEXT,
                        model_provider TEXT, model_name TEXT, started_at TIMESTAMP, ended_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.outputs(
                        output_id TEXT, invocation_id TEXT, verdict TEXT, confidence DOUBLE, summary TEXT,
                        human_review_required BOOLEAN, output_payload_json TEXT, created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.reviews(
                        review_id TEXT, invocation_id TEXT, review_status TEXT, reviewer_id TEXT, review_payload_json TEXT, reviewed_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE agent.evaluations(
                        evaluation_id TEXT, invocation_id TEXT, verification_method TEXT, is_verified BOOLEAN, created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO agent.invocations VALUES
                    ('inv_1','rule2spec','weather_market','mkt_1','success','stub','stub-model','2026-03-15 09:00:00','2026-03-15 09:01:00')
                    """
                )
                con.execute(
                    """
                    INSERT INTO agent.outputs VALUES
                    ('out_1','inv_1','review',0.9,'runtime fallback row',FALSE,'{}','2026-03-15 09:01:00')
                    """
                )
            finally:
                con.close()

            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(Path(tmpdir) / "missing_weather_report.json"),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(runtime_path),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(Path(tmpdir) / "missing_readiness.json"),
                    "ASTERION_DB_PATH": str(Path(tmpdir) / "missing_runtime.duckdb"),
                },
                clear=False,
            ):
                status = load_operator_surface_status()

        self.assertEqual(status["market_chain"]["status"], "degraded_source")
        self.assertEqual(status["agent_review"]["status"], "degraded_source")

    def test_load_operator_surface_status_marks_readiness_degraded_when_manifest_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "readiness.json"
            manifest_path = Path(tmpdir) / "capability_manifest.json"
            report_path.write_text(
                json.dumps(
                    {
                        "target": "p4_live_prerequisites",
                        "go_decision": "GO",
                        "decision_reason": "all readiness gates passed; ready for controlled live rollout decision",
                    }
                ),
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps({"manifest_status": "blocked"}),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(Path(tmpdir) / "missing_weather_report.json"),
                    "ASTERION_REAL_WEATHER_CHAIN_DB_PATH": str(Path(tmpdir) / "missing_weather_runtime.duckdb"),
                    "ASTERION_READINESS_REPORT_JSON_PATH": str(report_path),
                    "ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH": str(manifest_path),
                },
                clear=False,
            ):
                status = load_operator_surface_status()
        self.assertEqual(status["readiness"]["status"], "degraded_source")


if __name__ == "__main__":
    unittest.main()
