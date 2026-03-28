from __future__ import annotations

import unittest

from asterion_core.contracts import ExecutionFeedbackPrior, ExecutionPriorKey, ExecutionPriorSummary
from domains.weather.opportunity import build_weather_opportunity_assessment
from domains.weather.opportunity.execution_priors import build_execution_prior_summary_from_context
from domains.weather.opportunity.resolved_execution_projection import build_resolved_execution_projection


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
    def test_resolved_execution_projection_uses_quantity_and_fee_in_realized_pnl(self) -> None:
        projection = build_resolved_execution_projection(
            outcome="NO",
            side="BUY",
            expected_outcome="NO",
            filled_quantity=10.0,
            ticket_size=10.0,
            expected_fill_price=0.61,
            realized_fill_price=0.61,
            total_fee=0.14,
            predicted_edge_bps=3800.0,
            execution_result=None,
            order_status="filled",
            latest_submit_status="accepted",
            live_prereq_execution_status=None,
            external_order_status="filled",
            gate_allowed=True,
            latest_sign_attempt_id="sign_1",
            latest_submit_attempt_id="submit_1",
            latest_fill_at="2026-03-28T10:00:00+00:00",
            latest_resolution_at="2026-03-28T11:00:00+00:00",
        )

        self.assertEqual(projection.evaluation_status, "resolved")
        self.assertAlmostEqual(projection.resolution_value or 0.0, 0.0)
        self.assertAlmostEqual(projection.realized_pnl or 0.0, 3.76)
        self.assertAlmostEqual(projection.post_trade_error or 0.0, -0.04)
        self.assertEqual(projection.execution_lifecycle_stage, "resolved")

    def test_build_execution_prior_summary_from_context_prefers_explicit_feedback_penalty(self) -> None:
        summary = build_execution_prior_summary_from_context(
            {
                "execution_prior_key": "eprior_test",
                "execution_prior_market_id": "mkt_feedback",
                "execution_prior_side": "BUY",
                "execution_prior_sample_count": 67,
                "execution_prior_submit_ack_rate": 0.0,
                "execution_prior_fill_rate": 0.0,
                "execution_prior_resolution_rate": 0.0,
                "execution_prior_partial_fill_rate": 0.0,
                "execution_prior_cancel_rate": 0.0,
                "execution_prior_quality_status": "ready",
                "execution_prior_lookup_mode": "exact_market",
                "execution_prior_feedback_status": "degraded",
                "execution_prior_feedback_penalty": 0.45,
                "execution_prior_miss_rate": None,
                "execution_prior_distortion_rate": None,
                "execution_prior_slippage_p50": float("nan"),
                "execution_prior_feedback_scope_breakdown": {
                    "market": {"feedback_penalty": 0.45, "feedback_status": "degraded"}
                },
                "execution_prior_dominant_miss_reason_bucket": "not_submitted",
                "execution_prior_dominant_distortion_reason_bucket": "none",
                "execution_prior_cohort_prior_version": "feedback_v1",
            }
        )

        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertIsNotNone(summary.feedback_prior)
        assert summary.feedback_prior is not None
        self.assertEqual(summary.feedback_prior.feedback_status, "degraded")
        self.assertAlmostEqual(summary.feedback_prior.feedback_penalty, 0.45)

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
