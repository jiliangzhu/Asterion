from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui.builders.opportunity_builder import _create_action_queue_summary


class UiDeployableActionQueueTest(unittest.TestCase):
    def test_action_queue_surfaces_deployable_metrics_and_binding_explanation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "deployable_queue.duckdb"
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
                        pre_budget_deployable_size DOUBLE,
                        pre_budget_deployable_notional DOUBLE,
                        pre_budget_deployable_expected_pnl DOUBLE,
                        preview_binding_limit_scope TEXT,
                        preview_binding_limit_key TEXT,
                        requested_size DOUBLE,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
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
                    ('mkt_1', 'Seattle', 'Deployable queue row', 'BUY', 1.44, 0.82, 0.24, 1.44, 6.0, 6.0, 6.0, 6.0, 1.44, 'per_ticket', 'policy_exact', 8.0, 6.0, 'approved', 'actionable', 'passed', 'healthy', 0.0, 'fresh', 'clear', '[]', FALSE, 'pass', 'fresh', 'canonical', 'canonical', 'ready', '2026-03-19 10:00:00', 'ranking_score')
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
                        pre_budget_deployable_size DOUBLE,
                        pre_budget_deployable_notional DOUBLE,
                        pre_budget_deployable_expected_pnl DOUBLE,
                        capital_scarcity_penalty DOUBLE,
                        concentration_penalty DOUBLE,
                        rerank_position BIGINT,
                        rerank_reason_codes_json TEXT,
                        requested_size DOUBLE,
                        requested_notional DOUBLE,
                        allocation_status TEXT,
                        recommended_size DOUBLE,
                        reason_codes_json TEXT,
                        budget_impact_json TEXT,
                        binding_limit_scope TEXT,
                        binding_limit_key TEXT,
                        capital_policy_id TEXT,
                        capital_policy_version TEXT,
                        regime_bucket TEXT,
                        calibration_gate_status TEXT,
                        capital_scaling_reason_codes_json TEXT,
                        created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.runtime.allocation_decisions VALUES
                    ('mkt_1', 'wallet_weather_1', 'weather_primary', 'alloc_1', 1.44, 0.82, 1.44, 6.0, 6.0, 6.0, 6.0, 1.44, 0.10, 0.00, 1, '["reranked_vs_base_order"]', 8.0, 8.0, 'approved', 6.0, '[]', '{"remaining_run_budget":8.0,"binding_limit_scope":"run_budget","binding_limit_key":"policy_exact","preview":{"requested_size":8.0,"pre_budget_deployable_size":6.0,"pre_budget_deployable_notional":6.0,"pre_budget_deployable_expected_pnl":1.44,"preview_binding_limit_scope":"per_ticket","preview_binding_limit_key":"policy_exact"}}', 'run_budget', 'policy_exact', 'cap_review', 'cap_v1', 'warm', 'clear', '[]', '2026-03-19 11:00:00')
                    """
                )
                counts: dict[str, int] = {}
                _create_action_queue_summary(con, table_row_counts=counts)
                row = con.execute(
                    """
                    SELECT
                        ranking_score,
                        base_ranking_score,
                        deployable_expected_pnl,
                        deployable_notional,
                        max_deployable_size,
                        pre_budget_deployable_expected_pnl,
                        preview_binding_limit_scope,
                        preview_binding_limit_key,
                        requested_size,
                        binding_limit_scope,
                        binding_limit_key,
                        capital_policy_id,
                        calibration_gate_status,
                        capital_scarcity_penalty,
                        rerank_position,
                        rerank_reason_codes_json
                    FROM ui.action_queue_summary
                    """
                ).fetchone()
            finally:
                con.close()

        self.assertEqual(counts["ui.action_queue_summary"], 1)
        self.assertEqual(float(row[0]), 1.44)
        self.assertEqual(float(row[1]), 0.82)
        self.assertEqual(float(row[2]), 1.44)
        self.assertEqual(float(row[3]), 6.0)
        self.assertEqual(float(row[4]), 6.0)
        self.assertEqual(float(row[5]), 1.44)
        self.assertEqual(row[6], "per_ticket")
        self.assertEqual(row[7], "policy_exact")
        self.assertEqual(float(row[8]), 8.0)
        self.assertEqual(row[9], "run_budget")
        self.assertEqual(row[10], "policy_exact")
        self.assertEqual(row[11], "cap_review")
        self.assertEqual(row[12], "clear")
        self.assertAlmostEqual(float(row[13]), 0.10)
        self.assertEqual(int(row[14]), 1)
        self.assertIn("reranked_vs_base_order", row[15])


if __name__ == "__main__":
    unittest.main()
