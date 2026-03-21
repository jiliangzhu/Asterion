from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui.builders.opportunity_builder import _create_action_queue_summary


class CapitalAwareRankingAcceptanceTest(unittest.TestCase):
    def test_deployable_value_can_outrank_higher_base_score(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "capital_aware_queue.duckdb"
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
                        live_prereq_status TEXT,
                        signal_created_at TIMESTAMP,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_base_high', 'Seattle', 'Base high / deployable low', 'BUY', 0.95, 0.95, 0.30, 0.30, 1.0, 'resized', 'actionable', 'passed', 'healthy', 0.0, 'fresh', 'pass', 'fresh', 'canonical', 'canonical', 'ready', '2026-03-19 10:00:00', 'ranking_score'),
                    ('mkt_base_low', 'Boston', 'Base lower / deployable high', 'BUY', 0.80, 0.80, 0.22, 0.22, 6.0, 'approved', 'actionable', 'passed', 'healthy', 0.0, 'fresh', 'pass', 'fresh', 'canonical', 'canonical', 'ready', '2026-03-19 10:01:00', 'ranking_score')
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
                        created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.runtime.allocation_decisions VALUES
                    ('mkt_base_high', 'wallet_weather_1', 'weather_primary', 'alloc_1', 0.30, 0.95, 0.30, 1.0, 2.0, 0.70, 0.00, 'resized', 1.0, '["buy_budget_exhausted"]', '{"remaining_run_budget":0.0,"binding_limit_scope":"run_budget","binding_limit_key":"policy_exact"}', 'run_budget', 'policy_exact', '2026-03-19 11:00:00'),
                    ('mkt_base_low', 'wallet_weather_1', 'weather_primary', 'alloc_2', 1.32, 0.80, 1.32, 6.0, 6.0, 0.00, 0.00, 'approved', 6.0, '[]', '{"remaining_run_budget":8.0}', NULL, NULL, '2026-03-19 11:01:00')
                    """
                )

                counts: dict[str, int] = {}
                _create_action_queue_summary(con, table_row_counts=counts)
                rows = con.execute(
                    """
                    SELECT market_id, ranking_score, base_ranking_score, deployable_expected_pnl
                    FROM ui.action_queue_summary
                    ORDER BY queue_priority, ranking_score DESC
                    """
                ).fetchall()
            finally:
                con.close()

        self.assertEqual(counts["ui.action_queue_summary"], 2)
        self.assertEqual(rows[0][0], "mkt_base_low")
        self.assertGreater(float(rows[0][1]), float(rows[1][1]))
        self.assertLess(float(rows[0][2]), float(rows[1][2]))
        self.assertGreater(float(rows[0][3]), float(rows[1][3]))


if __name__ == "__main__":
    unittest.main()
