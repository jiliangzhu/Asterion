from __future__ import annotations

import unittest

from asterion_core.contracts import ExecutionFeedbackPrior, ExecutionPriorKey, ExecutionPriorSummary
from domains.weather.opportunity import build_weather_opportunity_assessment


def _prior_summary(
    *,
    market_id: str = "mkt_rank",
    side: str = "BUY",
    horizon_bucket: str = "0-1",
    liquidity_bucket: str = "deep",
    sample_count: int = 12,
    submit_ack_rate: float = 0.95,
    fill_rate: float = 0.90,
    resolution_rate: float = 0.92,
    partial_fill_rate: float = 0.10,
    cancel_rate: float = 0.05,
    adverse_fill_slippage_bps_p50: float | None = 18.0,
    submit_latency_ms_p90: float | None = 30_000.0,
    fill_latency_ms_p90: float | None = 60_000.0,
    realized_edge_retention_bps_p50: float | None = 420.0,
    prior_lookup_mode: str = "exact_market",
    prior_feature_scope: dict | None = None,
    feedback_prior: ExecutionFeedbackPrior | None = None,
) -> ExecutionPriorSummary:
    return ExecutionPriorSummary(
        prior_key=ExecutionPriorKey(
            market_id=market_id,
            strategy_id=None,
            wallet_id=None,
            side=side,
            horizon_bucket=horizon_bucket,
            liquidity_bucket=liquidity_bucket,
        ),
        sample_count=sample_count,
        submit_ack_rate=submit_ack_rate,
        fill_rate=fill_rate,
        resolution_rate=resolution_rate,
        partial_fill_rate=partial_fill_rate,
        cancel_rate=cancel_rate,
        adverse_fill_slippage_bps_p50=adverse_fill_slippage_bps_p50,
        adverse_fill_slippage_bps_p90=42.0,
        submit_latency_ms_p50=submit_latency_ms_p90,
        submit_latency_ms_p90=submit_latency_ms_p90,
        fill_latency_ms_p50=fill_latency_ms_p90,
        fill_latency_ms_p90=fill_latency_ms_p90,
        realized_edge_retention_bps_p50=realized_edge_retention_bps_p50,
        realized_edge_retention_bps_p90=realized_edge_retention_bps_p50,
        avg_realized_pnl=0.07,
        avg_post_trade_error=0.01,
        prior_quality_status="ready" if sample_count >= 10 else "sparse",
        prior_lookup_mode=prior_lookup_mode,
        prior_feature_scope=prior_feature_scope or {"lookup_mode": prior_lookup_mode},
        feedback_prior=feedback_prior,
    )


class RankingScoreV2Test(unittest.TestCase):
    def test_prior_backed_ranking_populates_v2_fields(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_rank",
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
        )

        self.assertGreater(assessment.ranking_score, 0.0)
        self.assertGreater(assessment.expected_dollar_pnl, 0.0)
        self.assertGreater(assessment.capture_probability, 0.0)
        self.assertGreaterEqual(assessment.risk_penalty, 0.0)
        self.assertGreater(assessment.capital_efficiency, 0.0)
        self.assertEqual(assessment.why_ranked_json["version"], "ranking_v2")
        self.assertEqual(assessment.why_ranked_json["mode"], "prior_backed")
        self.assertEqual(assessment.execution_prior_key, assessment.why_ranked_json["execution_prior_key"]["prior_key"])
        self.assertEqual(assessment.ops_readiness_score, 0.001)
        self.assertIn("latency_penalty", assessment.why_ranked_json)
        self.assertIn("tail_slippage_penalty", assessment.why_ranked_json)
        self.assertIn("edge_retention_penalty", assessment.why_ranked_json)
        self.assertIn("quality_confidence_multiplier", assessment.why_ranked_json)
        self.assertEqual(assessment.why_ranked_json["prior_lookup_mode"], "exact_market")

    def test_ops_tie_breaker_does_not_override_materially_better_ev(self) -> None:
        low_ev = build_weather_opportunity_assessment(
            market_id="mkt_low",
            token_id="tok_low",
            outcome="YES",
            reference_price=0.45,
            model_fair_value=0.52,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            execution_prior_summary=_prior_summary(market_id="mkt_low", submit_ack_rate=0.60, fill_rate=0.55, resolution_rate=0.60),
        )
        high_ev = build_weather_opportunity_assessment(
            market_id="mkt_high",
            token_id="tok_high",
            outcome="YES",
            reference_price=0.36,
            model_fair_value=0.66,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(market_id="mkt_high", submit_ack_rate=0.95, fill_rate=0.92, resolution_rate=0.94),
        )

        self.assertGreater(high_ev.expected_dollar_pnl, low_ev.expected_dollar_pnl)
        self.assertLess(high_ev.ops_readiness_score, low_ev.ops_readiness_score)
        self.assertGreater(high_ev.ranking_score, low_ev.ranking_score)

    def test_missing_prior_uses_fallback_heuristic_mode(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_fallback",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.66,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
        )

        self.assertEqual(assessment.why_ranked_json["mode"], "fallback_heuristic")
        self.assertIsNone(assessment.execution_prior_key)
        self.assertGreater(assessment.capture_probability, 0.0)
        self.assertEqual(assessment.why_ranked_json["prior_lookup_mode"], "heuristic_fallback")

    def test_feedback_suppression_applies_after_ranking_v2(self) -> None:
        degraded_feedback = ExecutionFeedbackPrior(
            feedback_penalty=0.4,
            feedback_status="degraded",
            cohort_prior_version="feedback_v1",
            dominant_miss_reason_bucket="working_unfilled",
            dominant_distortion_reason_bucket="execution_distortion",
            scope_breakdown={"market": {"weight": 1.0, "feedback_penalty": 0.4, "feedback_status": "degraded"}},
        )
        baseline = build_weather_opportunity_assessment(
            market_id="mkt_base",
            token_id="tok_base",
            outcome="YES",
            reference_price=0.36,
            model_fair_value=0.66,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(market_id="mkt_base"),
        )
        degraded = build_weather_opportunity_assessment(
            market_id="mkt_base",
            token_id="tok_base",
            outcome="YES",
            reference_price=0.36,
            model_fair_value=0.66,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(market_id="mkt_base", feedback_prior=degraded_feedback),
        )

        self.assertLess(degraded.ranking_score, baseline.ranking_score)
        self.assertEqual(degraded.feedback_status, "degraded")
        self.assertAlmostEqual(degraded.feedback_penalty, 0.4)
        self.assertEqual(degraded.why_ranked_json["pre_feedback_ranking_score"], baseline.ranking_score)
        self.assertIn("quality_confidence_multiplier", degraded.why_ranked_json)

    def test_latency_and_edge_retention_penalties_lower_ranking(self) -> None:
        fast = build_weather_opportunity_assessment(
            market_id="mkt_fast",
            token_id="tok_fast",
            outcome="YES",
            reference_price=0.38,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(
                market_id="mkt_fast",
                submit_latency_ms_p90=5_000.0,
                fill_latency_ms_p90=10_000.0,
                realized_edge_retention_bps_p50=700.0,
            ),
        )
        slow = build_weather_opportunity_assessment(
            market_id="mkt_slow",
            token_id="tok_slow",
            outcome="YES",
            reference_price=0.38,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
            execution_prior_summary=_prior_summary(
                market_id="mkt_slow",
                submit_latency_ms_p90=120_000.0,
                fill_latency_ms_p90=240_000.0,
                realized_edge_retention_bps_p50=120.0,
            ),
        )

        self.assertGreater(fast.ranking_score, slow.ranking_score)
        self.assertGreater(slow.why_ranked_json["latency_penalty"], fast.why_ranked_json["latency_penalty"])
        self.assertGreater(slow.why_ranked_json["edge_retention_penalty"], fast.why_ranked_json["edge_retention_penalty"])


if __name__ == "__main__":
    unittest.main()
