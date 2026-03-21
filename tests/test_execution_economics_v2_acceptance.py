from __future__ import annotations

import unittest

from asterion_core.contracts import ExecutionPriorKey, ExecutionPriorSummary
from domains.weather.opportunity import build_weather_opportunity_assessment


def _prior_summary(*, sample_count: int, prior_quality_status: str) -> ExecutionPriorSummary:
    return ExecutionPriorSummary(
        prior_key=ExecutionPriorKey(
            market_id="mkt_empirical",
            strategy_id=None,
            wallet_id=None,
            side="BUY",
            horizon_bucket="0-1",
            liquidity_bucket="deep",
        ),
        sample_count=sample_count,
        submit_ack_rate=0.95,
        fill_rate=0.90,
        resolution_rate=0.92,
        partial_fill_rate=0.06,
        cancel_rate=0.03,
        adverse_fill_slippage_bps_p50=12.0,
        adverse_fill_slippage_bps_p90=26.0,
        prior_quality_status=prior_quality_status,
    )


class ExecutionEconomicsV2AcceptanceTest(unittest.TestCase):
    def test_ready_prior_uses_empirical_primary_path(self) -> None:
        ready = build_weather_opportunity_assessment(
            market_id="mkt_empirical",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="review_required",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(sample_count=20, prior_quality_status="ready"),
        )
        fallback = build_weather_opportunity_assessment(
            market_id="mkt_empirical",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="review_required",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(sample_count=4, prior_quality_status="missing"),
        )

        empirical_capture = round(0.95 * 0.90 * 0.92, 6)
        self.assertEqual(ready.why_ranked_json["economics_path"], "empirical_primary")
        self.assertAlmostEqual(ready.capture_probability, empirical_capture, places=6)
        self.assertEqual(fallback.why_ranked_json["economics_path"], "heuristic_fallback")
        self.assertAlmostEqual(fallback.capture_probability, 0.5, places=6)
        self.assertGreater(ready.capture_probability, fallback.capture_probability)


if __name__ == "__main__":
    unittest.main()
