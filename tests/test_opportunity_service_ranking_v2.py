from __future__ import annotations

import unittest

from asterion_core.contracts import ExecutionFeedbackPrior, ExecutionPriorKey, ExecutionPriorSummary
from domains.weather.opportunity import build_weather_opportunity_assessment


class OpportunityServiceRankingV2Test(unittest.TestCase):
    def test_assessment_exposes_why_ranked_and_prior_key(self) -> None:
        prior_summary = ExecutionPriorSummary(
            prior_key=ExecutionPriorKey(
                market_id="mkt_1",
                strategy_id=None,
                wallet_id=None,
                side="BUY",
                horizon_bucket="0-1",
                liquidity_bucket="deep",
            ),
            sample_count=12,
            submit_ack_rate=0.95,
            fill_rate=0.90,
            resolution_rate=0.88,
            partial_fill_rate=0.08,
            cancel_rate=0.04,
            adverse_fill_slippage_bps_p50=15.0,
            adverse_fill_slippage_bps_p90=30.0,
            avg_realized_pnl=0.08,
            avg_post_trade_error=0.01,
            prior_quality_status="ready",
            feedback_prior=ExecutionFeedbackPrior(
                feedback_penalty=0.2,
                feedback_status="watch",
                cohort_prior_version="feedback_v1",
                dominant_miss_reason_bucket="working_unfilled",
                dominant_distortion_reason_bucket="execution_distortion",
                scope_breakdown={"market": {"weight": 1.0, "feedback_penalty": 0.2, "feedback_status": "watch"}},
            ),
        )

        assessment = build_weather_opportunity_assessment(
            market_id="mkt_1",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.39,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            execution_prior_summary=prior_summary,
        )

        self.assertEqual(assessment.why_ranked_json["version"], "ranking_v2")
        self.assertEqual(assessment.why_ranked_json["mode"], "prior_backed")
        self.assertEqual(assessment.execution_prior_key, assessment.assessment_context_json["execution_prior_key"])
        self.assertEqual(assessment.why_ranked_json["prior_quality_status"], "ready")
        self.assertGreater(assessment.expected_dollar_pnl, 0.0)
        self.assertGreater(assessment.capture_probability, 0.0)
        self.assertEqual(assessment.feedback_status, "watch")
        self.assertAlmostEqual(assessment.feedback_penalty, 0.2)
        self.assertEqual(assessment.cohort_prior_version, "feedback_v1")
        self.assertIn("feedback_scope_breakdown", assessment.why_ranked_json)
        self.assertIn("prior_lookup_mode", assessment.why_ranked_json)
        self.assertIn("latency_penalty", assessment.why_ranked_json)
        self.assertIn("edge_retention_penalty", assessment.why_ranked_json)

    def test_assessment_context_carries_feedback_why_ranked_fields(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_2",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.41,
            model_fair_value=0.69,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
        )
        self.assertIn("why_ranked_json", assessment.assessment_context_json)
        self.assertIn("expected_dollar_pnl", assessment.assessment_context_json)
        self.assertIn("capture_probability", assessment.assessment_context_json)
        self.assertEqual(assessment.assessment_context_json["why_ranked_json"]["version"], "ranking_v2")
        self.assertEqual(assessment.assessment_context_json["ranking_score"], assessment.ranking_score)

    def test_calibration_freshness_fields_flow_into_assessment_context_and_why_ranked(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_cal",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.41,
            model_fair_value=0.69,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="not_started",
            source_context={
                "calibration_health_status": "healthy",
                "calibration_reason_codes": ["calibration_profile_stale"],
                "calibration_freshness_status": "stale",
                "profile_materialized_at": "2026-03-18T03:15:00+00:00",
                "profile_window_end": "2026-03-18T02:00:00+00:00",
                "profile_age_hours": 48.0,
            },
        )
        self.assertEqual(assessment.assessment_context_json["calibration_freshness_status"], "stale")
        self.assertEqual(assessment.why_ranked_json["calibration_freshness_status"], "stale")
        self.assertEqual(assessment.why_ranked_json["calibration_profile_materialized_at"], "2026-03-18T03:15:00+00:00")
        self.assertIn("calibration_profile_stale", assessment.ranking_penalty_reasons)
        self.assertEqual(assessment.calibration_gate_status, "review_required")
        self.assertEqual(assessment.assessment_context_json["calibration_gate_status"], "review_required")
        self.assertEqual(assessment.why_ranked_json["calibration_gate_status"], "review_required")

    def test_injected_allocation_fields_flow_into_assessment_and_why_ranked(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_alloc",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.42,
            model_fair_value=0.70,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            recommended_size=4.0,
            allocation_status="resized",
            budget_impact={"remaining_run_budget": 0.0, "binding_limit_scope": "market"},
            allocation_decision_id="alloc_1",
            policy_id="policy_exact",
            policy_version="alloc_v1",
            base_ranking_score=0.42,
            deployable_expected_pnl=1.68,
            deployable_notional=1.68,
            max_deployable_size=5.0,
            capital_scarcity_penalty=0.2,
            concentration_penalty=0.1,
            deployable_ranking_score=1.68,
            capital_policy_id="cap_review",
            capital_policy_version="cap_v1",
            capital_scaling_reason_codes=["capital_open_markets_cap"],
            source_context={
                "preview_binding_limit_scope": "per_ticket",
                "preview_binding_limit_key": "policy_exact",
                "requested_size": 6.0,
                "requested_notional": 2.52,
                "regime_bucket": "warm",
            },
        )
        self.assertEqual(assessment.recommended_size, 4.0)
        self.assertEqual(assessment.allocation_status, "resized")
        self.assertEqual(assessment.base_ranking_score, 0.42)
        self.assertEqual(assessment.deployable_expected_pnl, 1.68)
        self.assertEqual(assessment.budget_impact["binding_limit_scope"], "market")
        self.assertEqual(assessment.why_ranked_json["allocation_decision_id"], "alloc_1")
        self.assertEqual(assessment.why_ranked_json["policy_id"], "policy_exact")
        self.assertEqual(assessment.why_ranked_json["deployable_expected_pnl"], 1.68)
        self.assertEqual(assessment.why_ranked_json["base_ranking_score"], 0.42)
        self.assertEqual(assessment.why_ranked_json["preview_binding_limit_scope"], "per_ticket")
        self.assertEqual(assessment.why_ranked_json["preview_binding_limit_key"], "policy_exact")
        self.assertEqual(assessment.why_ranked_json["capital_policy_id"], "cap_review")
        self.assertEqual(assessment.why_ranked_json["capital_policy_version"], "cap_v1")
        self.assertEqual(assessment.why_ranked_json["capital_scaling_reason_codes"], ["capital_open_markets_cap"])
        self.assertEqual(assessment.assessment_context_json["requested_size"], 6.0)
        self.assertEqual(assessment.assessment_context_json["ranking_score"], 1.68)
        self.assertEqual(assessment.assessment_context_json["allocation_status"], "resized")
        self.assertEqual(assessment.capital_policy_id, "cap_review")
        self.assertEqual(assessment.regime_bucket, "warm")


if __name__ == "__main__":
    unittest.main()
