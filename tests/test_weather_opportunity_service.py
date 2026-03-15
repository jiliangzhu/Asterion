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


if __name__ == "__main__":
    unittest.main()
