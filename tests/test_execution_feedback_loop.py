from __future__ import annotations

import unittest

from asterion_core.contracts import ExecutionFeedbackPrior, ExecutionPriorKey, ExecutionPriorSummary
from domains.weather.opportunity import build_weather_opportunity_assessment


def _prior_summary(*, feedback_penalty: float | None = None, feedback_status: str = "ready") -> ExecutionPriorSummary:
    feedback_prior = None
    if feedback_penalty is not None:
        feedback_prior = ExecutionFeedbackPrior(
            feedback_penalty=feedback_penalty,
            feedback_status=feedback_status,
            cohort_prior_version="feedback_v1",
            dominant_miss_reason_bucket="working_unfilled",
            dominant_distortion_reason_bucket="execution_distortion",
            scope_breakdown={
                "market": {
                    "weight": 1.0,
                    "sample_count": 24,
                    "feedback_penalty": feedback_penalty,
                    "feedback_status": feedback_status,
                    "dominant_miss_reason_bucket": "working_unfilled",
                    "dominant_distortion_reason_bucket": "execution_distortion",
                }
            },
        )
    return ExecutionPriorSummary(
        prior_key=ExecutionPriorKey(
            market_id="mkt_feedback",
            strategy_id="weather_primary",
            wallet_id="wallet_weather_1",
            side="BUY",
            horizon_bucket="0-1",
            liquidity_bucket="deep",
        ),
        sample_count=24,
        submit_ack_rate=0.95,
        fill_rate=0.90,
        resolution_rate=0.88,
        partial_fill_rate=0.08,
        cancel_rate=0.04,
        adverse_fill_slippage_bps_p50=16.0,
        adverse_fill_slippage_bps_p90=34.0,
        avg_realized_pnl=0.08,
        avg_post_trade_error=0.01,
        prior_quality_status="ready",
        feedback_prior=feedback_prior,
    )


class ExecutionFeedbackLoopTest(unittest.TestCase):
    def test_feedback_penalty_suppresses_ranking_score(self) -> None:
        base = build_weather_opportunity_assessment(
            market_id="mkt_feedback",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            execution_prior_summary=_prior_summary(feedback_penalty=None),
        )
        suppressed = build_weather_opportunity_assessment(
            market_id="mkt_feedback",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            execution_prior_summary=_prior_summary(feedback_penalty=0.35, feedback_status="degraded"),
        )

        self.assertGreater(base.ranking_score, suppressed.ranking_score)
        self.assertEqual(suppressed.feedback_status, "degraded")
        self.assertEqual(suppressed.cohort_prior_version, "feedback_v1")
        self.assertAlmostEqual(suppressed.feedback_penalty, 0.35)
        self.assertGreater(suppressed.why_ranked_json["pre_feedback_ranking_score"], suppressed.ranking_score)
        self.assertIn("market", suppressed.why_ranked_json["feedback_scope_breakdown"])
        self.assertIn("quality_confidence_multiplier", suppressed.why_ranked_json)

    def test_missing_feedback_prior_keeps_heuristic_only_status(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_feedback",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.41,
            model_fair_value=0.64,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(feedback_penalty=None),
        )

        self.assertEqual(assessment.feedback_status, "heuristic_only")
        self.assertEqual(assessment.feedback_penalty, 0.0)
        self.assertIsNone(assessment.cohort_prior_version)


if __name__ == "__main__":
    unittest.main()
