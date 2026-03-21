from __future__ import annotations

import unittest
from datetime import datetime

from asterion_core.contracts import RankingRetrospectiveRow, RankingRetrospectiveSummary
from domains.weather.opportunity import compare_retrospective_uplift


class RetrospectiveUpliftAcceptanceTest(unittest.TestCase):
    def test_compare_retrospective_uplift_is_deterministic(self) -> None:
        baseline_summary = RankingRetrospectiveSummary(
            baseline_version="baseline_v1",
            snapshot_count=10,
            row_count=2,
            top_decile_submitted_capture_ratio=0.5,
            top_decile_fill_capture_ratio=0.4,
            top_decile_resolution_capture_ratio=0.3,
            top_decile_realized_pnl=0.10,
            top_decile_realized_pnl_share=0.25,
        )
        candidate_summary = RankingRetrospectiveSummary(
            baseline_version="candidate_v2",
            snapshot_count=10,
            row_count=2,
            top_decile_submitted_capture_ratio=0.6,
            top_decile_fill_capture_ratio=0.55,
            top_decile_resolution_capture_ratio=0.45,
            top_decile_realized_pnl=0.18,
            top_decile_realized_pnl_share=0.42,
        )
        baseline_rows = [
            RankingRetrospectiveRow(
                row_id="row_base",
                run_id="run_base",
                market_id="mkt_1",
                strategy_id="weather_primary",
                side="BUY",
                ranking_decile=1,
                top_k_bucket="top_1",
                evaluation_status="resolved",
                submitted_capture_ratio=0.5,
                fill_capture_ratio=0.4,
                resolution_capture_ratio=0.3,
                avg_ranking_score=0.4,
                avg_edge_bps_executable=800.0,
                avg_realized_pnl=0.10,
                avg_predicted_vs_realized_gap=0.03,
                forecast_replay_change_rate=0.1,
                top_rank_share_of_realized_pnl=0.25,
                window_start=datetime(2026, 3, 1),
                window_end=datetime(2026, 3, 15),
                created_at=datetime(2026, 3, 16),
            )
        ]
        candidate_rows = [
            RankingRetrospectiveRow(
                row_id="row_candidate",
                run_id="run_candidate",
                market_id="mkt_1",
                strategy_id="weather_primary",
                side="BUY",
                ranking_decile=1,
                top_k_bucket="top_1",
                evaluation_status="resolved",
                submitted_capture_ratio=0.6,
                fill_capture_ratio=0.55,
                resolution_capture_ratio=0.45,
                avg_ranking_score=0.55,
                avg_edge_bps_executable=820.0,
                avg_realized_pnl=0.18,
                avg_predicted_vs_realized_gap=0.02,
                forecast_replay_change_rate=0.1,
                top_rank_share_of_realized_pnl=0.42,
                window_start=datetime(2026, 3, 1),
                window_end=datetime(2026, 3, 15),
                created_at=datetime(2026, 3, 16),
            )
        ]

        uplift = compare_retrospective_uplift(
            baseline_summary=baseline_summary,
            candidate_summary=candidate_summary,
            baseline_rows=baseline_rows,
            candidate_rows=candidate_rows,
        )

        self.assertEqual(uplift["baseline_version"], "baseline_v1")
        self.assertEqual(uplift["candidate_version"], "candidate_v2")
        self.assertAlmostEqual(uplift["top_decile_fill_capture_uplift"], 0.15)
        self.assertAlmostEqual(uplift["top_decile_resolution_capture_uplift"], 0.15)
        self.assertAlmostEqual(uplift["top_decile_realized_pnl_uplift"], 0.08)
        self.assertAlmostEqual(uplift["top_decile_realized_share_uplift"], 0.17)
        self.assertTrue(uplift["candidate_outperformed"])


if __name__ == "__main__":
    unittest.main()
