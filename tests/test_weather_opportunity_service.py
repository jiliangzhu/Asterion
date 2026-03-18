from __future__ import annotations

import unittest

from domains.weather.opportunity import build_market_quality_assessment, build_weather_opportunity_assessment


class WeatherOpportunityServiceTest(unittest.TestCase):
    def test_builds_actionable_assessment_with_execution_adjusted_edge(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_1",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.41,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=88.0,
        )

        self.assertEqual(assessment.actionability_status, "actionable")
        self.assertGreater(assessment.edge_bps_model, assessment.edge_bps_executable)
        self.assertGreater(assessment.edge_bps_executable, 0)
        self.assertGreater(assessment.ranking_score, 0)
        self.assertEqual(assessment.assessment_context_json["best_side"], "BUY")

    def test_marks_attention_required_market_as_blocked(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_2",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.30,
            model_fair_value=0.70,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            agent_review_status="passed",
            live_prereq_status="attention_required",
            confidence_score=90.0,
        )
        self.assertEqual(assessment.actionability_status, "blocked")
        self.assertEqual(assessment.ops_readiness_score, 0.0)
        self.assertAlmostEqual(assessment.fill_probability, 0.25)

    def test_review_required_without_agent_pass(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_3",
            token_id="tok_no",
            outcome="NO",
            reference_price=0.25,
            model_fair_value=0.60,
            accepting_orders=True,
            enable_order_book=False,
            threshold_bps=300,
            agent_review_status="review_required",
            live_prereq_status="not_started",
            confidence_score=60.0,
        )
        self.assertEqual(assessment.actionability_status, "review_required")
        self.assertGreater(assessment.expected_pnl_score, 0.0)

    def test_marks_non_accepting_market_blocked_with_large_penalty(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_4",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.20,
            model_fair_value=0.80,
            accepting_orders=False,
            enable_order_book=False,
            threshold_bps=300,
            agent_review_status="passed",
            live_prereq_status="not_started",
            confidence_score=75.0,
        )
        self.assertEqual(assessment.actionability_status, "blocked")
        self.assertEqual(assessment.liquidity_penalty_bps, 999_999)
        self.assertEqual(assessment.fill_probability, 0.0)

    def test_mapping_confidence_and_staleness_enter_quality_context(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_5",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.44,
            model_fair_value=0.71,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=300,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=82.0,
            mapping_confidence=0.62,
            price_staleness_ms=120_000,
            source_freshness_status="fresh",
        )
        self.assertEqual(assessment.actionability_status, "review_required")
        self.assertEqual(assessment.assessment_context_json["market_quality_status"], "review_required")
        self.assertEqual(assessment.assessment_context_json["mapping_confidence"], 0.62)

    def test_market_quality_builder_blocks_missing_source(self) -> None:
        quality = build_market_quality_assessment(
            market_id="mkt_6",
            accepting_orders=True,
            enable_order_book=True,
            reference_price=0.52,
            mapping_confidence=0.9,
            price_staleness_ms=0,
            source_freshness_status="missing",
            depth_proxy=0.85,
        )
        self.assertEqual(quality.market_quality_status, "blocked")
        self.assertIn("source_missing", quality.market_quality_reason_codes)

    def test_sell_side_costs_reduce_absolute_executable_edge(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_sell_1",
            token_id="tok_no",
            outcome="NO",
            reference_price=0.70,
            model_fair_value=0.50,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=84.0,
        )
        self.assertEqual(assessment.assessment_context_json["model_side"], "SELL")
        self.assertEqual(assessment.assessment_context_json["best_side"], "SELL")
        self.assertLess(assessment.edge_bps_model, 0)
        self.assertLess(assessment.edge_bps_executable, 0)
        self.assertLess(abs(assessment.edge_bps_executable), abs(assessment.edge_bps_model))
        self.assertGreater(assessment.execution_adjusted_fair_value, assessment.model_fair_value)

    def test_buy_and_sell_same_absolute_mispricing_both_shrink_after_costs(self) -> None:
        buy = build_weather_opportunity_assessment(
            market_id="mkt_buy_sym",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.30,
            model_fair_value=0.50,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
        )
        sell = build_weather_opportunity_assessment(
            market_id="mkt_sell_sym",
            token_id="tok_no",
            outcome="NO",
            reference_price=0.70,
            model_fair_value=0.50,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
        )
        self.assertLess(abs(buy.edge_bps_executable), abs(buy.edge_bps_model))
        self.assertLess(abs(sell.edge_bps_executable), abs(sell.edge_bps_model))
        self.assertEqual(buy.assessment_context_json["best_side"], "BUY")
        self.assertEqual(sell.assessment_context_json["best_side"], "SELL")

    def test_sell_edge_crossing_zero_becomes_no_trade(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_sell_cross",
            token_id="tok_no",
            outcome="NO",
            reference_price=0.51,
            model_fair_value=0.50,
            accepting_orders=True,
            enable_order_book=False,
            threshold_bps=10,
            fees_bps=20,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
        )
        self.assertEqual(assessment.edge_bps_model, -100)
        self.assertEqual(assessment.edge_bps_executable, 0)
        self.assertIsNone(assessment.assessment_context_json["best_side"])
        self.assertEqual(assessment.actionability_status, "no_trade")

    def test_calibration_lookup_missing_penalizes_ranking_without_rewriting_edge(self) -> None:
        baseline = build_weather_opportunity_assessment(
            market_id="mkt_cal_baseline",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.41,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=88.0,
            calibration_health_status="healthy",
            sample_count=24,
        )
        missing = build_weather_opportunity_assessment(
            market_id="mkt_cal_missing",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.41,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=88.0,
            calibration_health_status="lookup_missing",
            sample_count=0,
            calibration_reason_codes=["calibration_lookup_missing"],
        )
        self.assertEqual(baseline.edge_bps_executable, missing.edge_bps_executable)
        self.assertGreater(baseline.ranking_score, missing.ranking_score)
        self.assertEqual(missing.calibration_health_status, "lookup_missing")
        self.assertEqual(missing.sample_count, 0)
        self.assertLess(missing.uncertainty_multiplier, baseline.uncertainty_multiplier)
        self.assertIn("calibration_lookup_missing", missing.ranking_penalty_reasons)

    def test_degraded_calibration_and_source_quality_compose_penalty(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_cal_degraded",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.38,
            model_fair_value=0.70,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=400,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=84.0,
            calibration_health_status="degraded",
            sample_count=32,
            calibration_reason_codes=["calibration_degraded"],
            mapping_confidence=0.62,
            source_freshness_status="stale",
            price_staleness_ms=120_000,
        )
        self.assertGreater(assessment.edge_bps_executable, 0)
        self.assertGreater(assessment.uncertainty_penalty_bps, 0)
        self.assertLess(assessment.uncertainty_multiplier, 1.0)
        self.assertIn("calibration_degraded", assessment.ranking_penalty_reasons)
        self.assertIn("freshness_stale", assessment.ranking_penalty_reasons)
        self.assertIn("mapping_confidence_reduced", assessment.ranking_penalty_reasons)
        self.assertEqual(assessment.assessment_context_json["uncertainty_multiplier"], assessment.uncertainty_multiplier)

    def test_phase13_quality_statuses_enter_assessment(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_phase13_context",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.69,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=400,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            calibration_health_status="healthy",
            calibration_bias_quality="watch",
            threshold_probability_quality="degraded",
            sample_count=18,
            forecast_distribution_summary_v2={"regime_stability_score": 0.58},
        )
        self.assertEqual(assessment.calibration_bias_quality, "watch")
        self.assertEqual(assessment.threshold_probability_quality, "degraded")
        self.assertEqual(assessment.assessment_context_json["calibration_bias_quality"], "watch")
        self.assertEqual(assessment.assessment_context_json["threshold_probability_quality"], "degraded")
        self.assertLess(assessment.uncertainty_multiplier, 1.0)
        self.assertEqual(assessment.assessment_context_json["ranking_penalty_reasons"], assessment.ranking_penalty_reasons)


if __name__ == "__main__":
    unittest.main()
