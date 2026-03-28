from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui.builders.opportunity_builder import _create_action_queue_summary


class UiActionQueueSummaryTest(unittest.TestCase):
    def test_action_queue_summary_classifies_and_sorts_operator_buckets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "action_queue.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("ATTACH ':memory:' AS src")
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE SCHEMA src.runtime")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        location_name TEXT,
                        question TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        base_ranking_score DOUBLE,
                        expected_dollar_pnl DOUBLE,
                        deployable_expected_pnl DOUBLE,
                        deployable_notional DOUBLE,
                        max_deployable_size DOUBLE,
                        actionability_status TEXT,
                        agent_review_status TEXT,
                        feedback_status TEXT,
                        feedback_penalty DOUBLE,
                        calibration_freshness_status TEXT,
                        calibration_gate_status TEXT,
                        calibration_gate_reason_codes TEXT,
                        calibration_impacted_market BOOLEAN,
                        market_quality_status TEXT,
                        source_freshness_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        live_prereq_status TEXT,
                        signal_created_at TIMESTAMP,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_ready', 'Seattle', 'Ready market', 'BUY', 0.91, 0.91, 2.4, 2.4, 5.0, 5.0, 'actionable', 'passed', 'healthy', 0.0, 'fresh', 'clear', '[]', FALSE, 'pass', 'fresh', 'canonical', 'canonical', 'ready', '2026-03-19 10:00:00', 'ranking_score'),
                    ('mkt_risk', 'Boston', 'Risk market', 'BUY', 0.84, 0.84, 2.0, 1.2, 3.0, 4.0, 'actionable', 'passed', 'degraded', 0.1, 'stale', 'clear', '[]', FALSE, 'pass', 'fresh', 'canonical', 'canonical', 'ready', '2026-03-19 10:01:00', 'ranking_score'),
                    ('mkt_review', 'Miami', 'Review market', 'BUY', 0.73, 0.73, 1.2, 1.2, 2.0, 2.0, 'review_required', 'review_required', 'healthy', 0.0, 'fresh', 'review_required', '[\"calibration_freshness_stale\"]', TRUE, 'pass', 'fresh', 'canonical', 'canonical', 'ready', '2026-03-19 10:02:00', 'ranking_score'),
                    ('mkt_blocked', 'Denver', 'Blocked market', 'BUY', 0.62, 0.62, 1.0, 0.0, 0.0, 1.0, 'actionable', 'passed', 'healthy', 0.0, 'fresh', 'clear', '[]', FALSE, 'pass', 'fresh', 'canonical', 'canonical', 'attention_required', '2026-03-19 10:03:00', 'ranking_score'),
                    ('mkt_research', 'Austin', 'Research market', 'BUY', 0.55, 0.55, 0.5, 0.0, 0.0, 0.0, 'no_trade', 'passed', 'healthy', 0.0, 'fresh', 'research_only', '[\"calibration_health_lookup_missing\"]', TRUE, 'pass', 'fresh', 'canonical', 'canonical', 'ready', '2026-03-19 10:04:00', 'ranking_score')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE src.runtime.allocation_decisions(
                        market_id TEXT,
                        wallet_id TEXT,
                        strategy_id TEXT,
                        allocation_decision_id TEXT,
                        ranking_score DOUBLE,
                        base_ranking_score DOUBLE,
                        deployable_expected_pnl DOUBLE,
                        deployable_notional DOUBLE,
                        max_deployable_size DOUBLE,
                        capital_scarcity_penalty DOUBLE,
                        concentration_penalty DOUBLE,
                        allocation_status TEXT,
                        recommended_size DOUBLE,
                        reason_codes_json TEXT,
                        budget_impact_json TEXT,
                        binding_limit_scope TEXT,
                        binding_limit_key TEXT,
                        capital_scaling_reason_codes_json TEXT,
                        created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.runtime.allocation_decisions VALUES
                    ('mkt_ready', 'wallet_weather_1', 'weather_primary', 'alloc_1', 2.4, 0.91, 2.4, 5.0, 5.0, 0.0, 0.0, 'approved', 5.0, '["allocation:approved"]', '{"remaining_run_budget":12.0,"binding_limit_scope":"market","binding_limit_key":"mkt_ready"}', 'market', 'mkt_ready', '[]', '2026-03-19 11:00:00'),
                    ('mkt_risk', 'wallet_weather_1', 'weather_primary', 'alloc_2', 1.2, 0.84, 1.2, 3.0, 4.0, 0.2, 0.0, 'resized', 3.0, '["feedback_status:degraded"]', '{"remaining_run_budget":10.0,"binding_limit_scope":"station","binding_limit_key":"KSEA"}', 'station', 'KSEA', '["execution_intelligence_tighten"]', '2026-03-19 11:01:00'),
                    ('mkt_review', 'wallet_weather_1', 'weather_primary', 'alloc_3', 1.2, 0.73, 1.2, 2.0, 2.0, 0.0, 0.0, 'approved', 2.0, '[]', '{"remaining_run_budget":8.0}', NULL, NULL, '[]', '2026-03-19 11:02:00'),
                    ('mkt_blocked', 'wallet_weather_1', 'weather_primary', 'alloc_4', 0.0, 0.62, 0.0, 0.0, 1.0, 1.0, 0.0, 'blocked', 0.0, '["buy_budget_exhausted"]', '{"remaining_run_budget":0.0,"binding_limit_scope":"run_budget","binding_limit_key":"policy_exact"}', 'run_budget', 'policy_exact', '["uncertainty_sizing_tighten"]', '2026-03-19 11:03:00')
                    """
                )

                counts: dict[str, int] = {}
                _create_action_queue_summary(con, table_row_counts=counts)
                frame = con.execute(
                    """
                    SELECT market_id, operator_bucket, queue_priority, queue_reason_codes_json, capital_scaling_reason_codes_json, remaining_run_budget
                    FROM ui.action_queue_summary
                    ORDER BY queue_priority, ranking_score DESC
                    """
                ).df()
            finally:
                con.close()

        self.assertEqual(counts["ui.action_queue_summary"], 5)
        self.assertEqual(frame["market_id"].tolist(), ["mkt_ready", "mkt_risk", "mkt_review", "mkt_blocked", "mkt_research"])
        self.assertEqual(frame["operator_bucket"].tolist(), ["ready_now", "high_risk", "review_required", "blocked", "research_only"])
        self.assertIn("calibration_gate:review_required", json.loads(frame.iloc[2]["queue_reason_codes_json"]))
        self.assertIn("calibration_gate:research_only", json.loads(frame.iloc[4]["queue_reason_codes_json"]))
        self.assertIn("allocation:approved", json.loads(frame.iloc[0]["queue_reason_codes_json"]))
        self.assertIn("feedback_status:degraded", json.loads(frame.iloc[1]["queue_reason_codes_json"]))
        self.assertIn("buy_budget_exhausted", json.loads(frame.iloc[3]["queue_reason_codes_json"]))
        self.assertIn("execution_intelligence_tighten", json.loads(frame.iloc[1]["capital_scaling_reason_codes_json"]))
        self.assertIn("uncertainty_sizing_tighten", json.loads(frame.iloc[3]["capital_scaling_reason_codes_json"]))
        self.assertAlmostEqual(float(frame.iloc[0]["remaining_run_budget"]), 12.0)

    def test_action_queue_summary_handles_null_allocation_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "action_queue_nulls.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("ATTACH ':memory:' AS src")
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE SCHEMA src.runtime")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        location_name TEXT,
                        question TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        base_ranking_score DOUBLE,
                        expected_dollar_pnl DOUBLE,
                        deployable_expected_pnl DOUBLE,
                        deployable_notional DOUBLE,
                        max_deployable_size DOUBLE,
                        actionability_status TEXT,
                        agent_review_status TEXT,
                        feedback_status TEXT,
                        feedback_penalty DOUBLE,
                        calibration_freshness_status TEXT,
                        calibration_gate_status TEXT,
                        calibration_gate_reason_codes TEXT,
                        calibration_impacted_market BOOLEAN,
                        market_quality_status TEXT,
                        source_freshness_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        live_prereq_status TEXT,
                        signal_created_at TIMESTAMP,
                        primary_score_label TEXT,
                        allocation_status TEXT,
                        budget_impact TEXT,
                        surface_delivery_status TEXT,
                        surface_delivery_reason_codes_json TEXT,
                        surface_last_refresh_ts TIMESTAMP,
                        allocation_decision_id TEXT,
                        capital_policy_id TEXT,
                        capital_policy_version TEXT,
                        regime_bucket TEXT,
                        capital_scaling_reason_codes TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    (
                        'mkt_nullable', 'Chicago', 'Nullable market', 'BUY',
                        0.81, 0.81, 1.1, 1.1, 2.5, 2.5,
                        'actionable', 'passed', NULL, NULL, 'fresh', 'clear', '[]', FALSE,
                        'pass', 'fresh', 'canonical', 'canonical', 'ready',
                        '2026-03-19 12:00:00', 'ranking_score',
                        NULL, NULL, 'ok', NULL, NULL, NULL, NULL, NULL, NULL, NULL
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE src.runtime.allocation_decisions(
                        market_id TEXT,
                        wallet_id TEXT,
                        strategy_id TEXT,
                        allocation_decision_id TEXT,
                        ranking_score DOUBLE,
                        base_ranking_score DOUBLE,
                        deployable_expected_pnl DOUBLE,
                        deployable_notional DOUBLE,
                        max_deployable_size DOUBLE,
                        capital_scarcity_penalty DOUBLE,
                        concentration_penalty DOUBLE,
                        allocation_status TEXT,
                        recommended_size DOUBLE,
                        reason_codes_json TEXT,
                        budget_impact_json TEXT,
                        binding_limit_scope TEXT,
                        binding_limit_key TEXT,
                        capital_scaling_reason_codes_json TEXT,
                        rerank_reason_codes_json TEXT,
                        created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.runtime.allocation_decisions VALUES
                    (
                        'mkt_nullable', 'wallet_weather_1', 'weather_primary', 'alloc_nullable',
                        1.1, 0.81, 1.1, 2.5, 2.5, 0.0, 0.0, NULL, 2.0,
                        NULL, NULL, NULL, NULL, NULL, NULL, '2026-03-19 12:05:00'
                    )
                    """
                )

                counts: dict[str, int] = {}
                _create_action_queue_summary(con, table_row_counts=counts)
                row = con.execute(
                    """
                    SELECT market_id, allocation_status, operator_bucket, surface_delivery_reason_codes_json
                    FROM ui.action_queue_summary
                    WHERE market_id = 'mkt_nullable'
                    """
                ).fetchone()
            finally:
                con.close()

        self.assertEqual(counts["ui.action_queue_summary"], 1)
        assert row is not None
        self.assertEqual(row[0], "mkt_nullable")
        self.assertIsNone(row[1])
        self.assertEqual(row[2], "research_only")
        self.assertEqual(row[3], "[]")


if __name__ == "__main__":
    unittest.main()
