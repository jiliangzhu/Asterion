from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from ui.loaders.execution_loader import load_execution_console_data
from ui.loaders.home_loader import load_home_decision_snapshot
from ui.loaders.markets_loader import load_market_chain_analysis_data


class OperatorWorkflowAcceptanceTest(unittest.TestCase):
    def test_home_markets_and_execution_share_persisted_operator_workflow_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_workflow.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        location_name TEXT,
                        question TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        edge_bps DOUBLE,
                        expected_dollar_pnl DOUBLE,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
                        actionability_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        source_freshness_status TEXT,
                        market_quality_status TEXT,
                        agent_review_status TEXT,
                        calibration_freshness_status TEXT,
                        market_close_time TIMESTAMP,
                        accepting_orders BOOLEAN,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_ready', 'Seattle', 'Ready market', 'BUY', 0.91, 950.0, 2.4, 5.0, 'approved', 'actionable', 'canonical', 'canonical', 'fresh', 'pass', 'passed', 'fresh', '2026-03-20 12:00:00', TRUE, 'ranking_score'),
                    ('mkt_risk', 'Boston', 'Risk market', 'BUY', 0.84, 830.0, 1.8, 3.0, 'resized', 'actionable', 'canonical', 'canonical', 'fresh', 'pass', 'passed', 'stale', '2026-03-20 14:00:00', TRUE, 'ranking_score'),
                    ('mkt_review', 'Miami', 'Review market', 'BUY', 0.73, 620.0, 1.0, 2.0, 'approved', 'review_required', 'canonical', 'canonical', 'fresh', 'pass', 'review_required', 'fresh', '2026-03-20 16:00:00', TRUE, 'ranking_score'),
                    ('mkt_blocked', 'Denver', 'Blocked market', 'BUY', 0.60, 300.0, 0.4, 0.0, 'blocked', 'blocked', 'canonical', 'canonical', 'fresh', 'watch', 'passed', 'fresh', '2026-03-20 18:00:00', TRUE, 'ranking_score'),
                    ('mkt_research', 'Austin', 'Research market', 'BUY', 0.50, 0.0, 0.1, NULL, NULL, 'no_trade', 'canonical', 'canonical', 'fresh', 'pass', 'passed', 'fresh', '2026-03-20 20:00:00', TRUE, 'ranking_score')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.action_queue_summary(
                        queue_item_id TEXT,
                        market_id TEXT,
                        wallet_id TEXT,
                        strategy_id TEXT,
                        location_name TEXT,
                        question TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        expected_dollar_pnl DOUBLE,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
                        actionability_status TEXT,
                        agent_review_status TEXT,
                        feedback_status TEXT,
                        feedback_penalty DOUBLE,
                        calibration_freshness_status TEXT,
                        market_quality_status TEXT,
                        source_freshness_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        operator_bucket TEXT,
                        queue_priority BIGINT,
                        queue_reason_codes_json TEXT,
                        binding_limit_scope TEXT,
                        remaining_run_budget DOUBLE,
                        allocation_decision_id TEXT,
                        updated_at TIMESTAMP,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.action_queue_summary VALUES
                    ('queue_1', 'mkt_ready', 'wallet_weather_1', 'weather_primary', 'Seattle', 'Ready market', 'BUY', 0.91, 2.4, 5.0, 'approved', 'actionable', 'passed', 'healthy', 0.0, 'fresh', 'pass', 'fresh', 'canonical', 'canonical', 'ready_now', 1, '["allocation:approved"]', 'market', 12.0, 'alloc_1', '2026-03-19 12:00:00', 'ranking_score'),
                    ('queue_2', 'mkt_risk', 'wallet_weather_1', 'weather_primary', 'Boston', 'Risk market', 'BUY', 0.84, 1.8, 3.0, 'resized', 'actionable', 'passed', 'watch', 0.1, 'stale', 'pass', 'fresh', 'canonical', 'canonical', 'high_risk', 2, '["calibration_freshness:stale"]', 'station', 8.0, 'alloc_2', '2026-03-19 12:01:00', 'ranking_score'),
                    ('queue_3', 'mkt_review', 'wallet_weather_1', 'weather_primary', 'Miami', 'Review market', 'BUY', 0.73, 1.0, 2.0, 'approved', 'review_required', 'review_required', 'healthy', 0.0, 'fresh', 'pass', 'fresh', 'canonical', 'canonical', 'review_required', 3, '["actionability:review_required"]', NULL, 6.0, 'alloc_3', '2026-03-19 12:02:00', 'ranking_score'),
                    ('queue_4', 'mkt_blocked', 'wallet_weather_1', 'weather_primary', 'Denver', 'Blocked market', 'BUY', 0.60, 0.4, 0.0, 'blocked', 'blocked', 'passed', 'healthy', 0.0, 'fresh', 'watch', 'fresh', 'canonical', 'canonical', 'blocked', 4, '["allocation:blocked"]', 'market', 0.0, 'alloc_4', '2026-03-19 12:03:00', 'ranking_score'),
                    ('queue_5', 'mkt_research', 'wallet_weather_1', 'weather_primary', 'Austin', 'Research market', 'BUY', 0.50, 0.1, NULL, NULL, 'no_trade', 'passed', 'healthy', 0.0, 'fresh', 'pass', 'fresh', 'canonical', 'canonical', 'research_only', 5, '["research_only"]', NULL, NULL, NULL, '2026-03-19 12:04:00', 'ranking_score')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.cohort_history_summary(
                        history_row_id TEXT,
                        run_id TEXT,
                        market_id TEXT,
                        strategy_id TEXT,
                        side TEXT,
                        ranking_decile BIGINT,
                        top_k_bucket TEXT,
                        evaluation_status TEXT,
                        window_start TIMESTAMP,
                        window_end TIMESTAMP,
                        submitted_capture_ratio DOUBLE,
                        fill_capture_ratio DOUBLE,
                        resolution_capture_ratio DOUBLE,
                        avg_ranking_score DOUBLE,
                        avg_edge_bps_executable DOUBLE,
                        avg_realized_pnl DOUBLE,
                        avg_predicted_vs_realized_gap DOUBLE,
                        forecast_replay_change_rate DOUBLE,
                        top_rank_share_of_realized_pnl DOUBLE,
                        feedback_status TEXT,
                        feedback_penalty DOUBLE,
                        cohort_prior_version TEXT,
                        calibration_freshness_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        updated_at TIMESTAMP,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.cohort_history_summary VALUES
                    ('hist_1', 'retro_1', 'mkt_ready', 'weather_primary', 'BUY', 1, 'top_5', 'resolved', '2026-03-10 00:00:00', '2026-03-18 00:00:00', 1.0, 0.9, 0.8, 0.91, 950.0, 12.5, 0.02, 0.0, 0.75, 'healthy', 0.0, 'feedback_v1', 'fresh', 'canonical', 'canonical', '2026-03-19 12:05:00', 'ranking_score'),
                    ('hist_2', 'retro_1', 'mkt_risk', 'weather_primary', 'BUY', 2, 'top_10', 'resolved', '2026-03-10 00:00:00', '2026-03-18 00:00:00', 0.8, 0.7, 0.6, 0.84, 830.0, 8.0, 0.03, 0.05, 0.55, 'watch', 0.1, 'feedback_v1', 'stale', 'canonical', 'canonical', '2026-03-19 12:05:00', 'ranking_score')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.execution_science_summary(
                        cohort_type TEXT,
                        cohort_key TEXT,
                        ticket_count BIGINT,
                        submission_capture_ratio DOUBLE,
                        fill_capture_ratio DOUBLE,
                        resolution_capture_ratio DOUBLE,
                        feedback_status TEXT,
                        feedback_penalty DOUBLE,
                        cohort_prior_version TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.execution_science_summary VALUES
                    ('strategy', 'weather_primary', 2, 0.9, 0.8, 0.7, 'healthy', 0.0, 'feedback_v1', 'derived', 'derived', 'ranking_score')
                    """
                )
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                with patch("ui.data_access.load_readiness_summary", return_value={"go_decision": "GO", "failed_gate_names": [], "decision_reason": "ok"}), \
                    patch("ui.data_access.load_readiness_evidence_bundle", return_value={"stale_dependencies": [], "blockers": [], "warnings": [], "capability_manifest_status": "valid"}), \
                    patch("ui.data_access.load_wallet_readiness_data", return_value=pd.DataFrame()), \
                    patch("ui.data_access.load_market_watch_data", return_value={"weather_smoke_report": {}, "market_watch": pd.DataFrame()}), \
                    patch("ui.data_access.load_agent_review_data", return_value={"frame": pd.DataFrame(), "source": "ui_lite"}), \
                    patch("ui.data_access.load_predicted_vs_realized_data", return_value={"frame": pd.DataFrame()}), \
                    patch("ui.data_access.load_operator_surface_status", return_value={}), \
                    patch("ui.data_access.load_boundary_sidebar_truth", return_value={}):
                    home_payload = load_home_decision_snapshot()
                    markets_payload = load_market_chain_analysis_data()
                    execution_payload = load_execution_console_data()

        self.assertEqual(home_payload["metrics"]["ready_now_count"], 1)
        self.assertEqual(home_payload["metrics"]["high_risk_count"], 1)
        self.assertEqual(home_payload["metrics"]["review_required_count"], 1)
        self.assertEqual(home_payload["metrics"]["blocked_count"], 1)
        self.assertEqual(home_payload["metrics"]["research_only_count"], 1)
        self.assertEqual(home_payload["action_queue"]["operator_bucket"].tolist(), ["ready_now", "high_risk", "review_required"])
        self.assertEqual(markets_payload["market_rows"][0]["operator_bucket"], "ready_now")
        self.assertTrue(markets_payload["market_rows"][0]["queue_reason_codes"])
        self.assertEqual(markets_payload["market_rows"][0]["cohort_history"][0]["history_row_id"], "hist_1")
        self.assertEqual(execution_payload["cohort_history"]["history_row_id"].tolist(), ["hist_1", "hist_2"])


if __name__ == "__main__":
    unittest.main()
