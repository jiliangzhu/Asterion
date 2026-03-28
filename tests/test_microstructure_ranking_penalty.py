from __future__ import annotations

import unittest
from datetime import UTC, datetime

from asterion_core.contracts import ExecutionIntelligenceSummary, ExecutionPriorKey, ExecutionPriorSummary
from domains.weather.opportunity import build_weather_opportunity_assessment


def _prior_summary() -> ExecutionPriorSummary:
    return ExecutionPriorSummary(
        prior_key=ExecutionPriorKey(
            market_id="mkt_micro",
            strategy_id=None,
            wallet_id=None,
            side="BUY",
            horizon_bucket="0-1",
            liquidity_bucket="deep",
        ),
        sample_count=12,
        submit_ack_rate=0.96,
        fill_rate=0.92,
        resolution_rate=0.91,
        partial_fill_rate=0.05,
        cancel_rate=0.04,
        adverse_fill_slippage_bps_p50=18.0,
        adverse_fill_slippage_bps_p90=35.0,
        submit_latency_ms_p50=8_000.0,
        submit_latency_ms_p90=12_000.0,
        fill_latency_ms_p50=12_000.0,
        fill_latency_ms_p90=24_000.0,
        realized_edge_retention_bps_p50=420.0,
        realized_edge_retention_bps_p90=390.0,
        avg_realized_pnl=0.08,
        avg_post_trade_error=0.01,
        prior_quality_status="ready",
        prior_lookup_mode="exact_market",
        prior_feature_scope={"lookup_mode": "exact_market"},
    )


def _execution_intelligence(*, stable: bool) -> ExecutionIntelligenceSummary:
    return ExecutionIntelligenceSummary(
        summary_id=f"ei_{'stable' if stable else 'unstable'}",
        run_id="eirun_1",
        market_id="mkt_micro",
        side="BUY",
        quote_imbalance_score=0.32 if stable else -0.18,
        top_of_book_stability=0.88 if stable else 0.22,
        book_update_intensity=0.45 if stable else 0.92,
        spread_regime="tight" if stable else "wide",
        visible_size_shock_flag=not stable,
        book_pressure_side="BUY" if stable else "neutral",
        expected_capture_regime="high" if stable else "low",
        expected_slippage_regime="low" if stable else "high",
        execution_intelligence_score=0.86 if stable else 0.18,
        reason_codes=["microstructure_balanced"] if stable else ["spread_regime:wide", "visible_size_shock", "capture_regime:low"],
        source_window_start=datetime(2026, 3, 1, tzinfo=UTC),
        source_window_end=datetime(2026, 3, 22, tzinfo=UTC),
        materialized_at=datetime(2026, 3, 22, tzinfo=UTC),
    )


class MicrostructureRankingPenaltyTest(unittest.TestCase):
    def test_unstable_book_is_penalized_in_ranking_v2(self) -> None:
        stable = build_weather_opportunity_assessment(
            market_id="mkt_stable",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.66,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            execution_prior_summary=_prior_summary(),
            execution_intelligence_summary=_execution_intelligence(stable=True),
        )
        unstable = build_weather_opportunity_assessment(
            market_id="mkt_unstable",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.66,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            execution_prior_summary=_prior_summary(),
            execution_intelligence_summary=_execution_intelligence(stable=False),
        )

        self.assertGreater(stable.ranking_score, unstable.ranking_score)
        self.assertGreater(unstable.why_ranked_json["microstructure_penalty"], stable.why_ranked_json["microstructure_penalty"])
        self.assertEqual(stable.why_ranked_json["spread_regime"], "tight")
        self.assertEqual(unstable.why_ranked_json["spread_regime"], "wide")
        self.assertTrue(unstable.why_ranked_json["visible_size_shock_flag"])
        self.assertIn("spread_regime:wide", unstable.why_ranked_json["microstructure_reason_codes"])


if __name__ == "__main__":
    unittest.main()
