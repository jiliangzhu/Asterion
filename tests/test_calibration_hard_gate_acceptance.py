from __future__ import annotations

import unittest

from domains.weather.opportunity import build_weather_opportunity_assessment


class CalibrationHardGateAcceptanceTest(unittest.TestCase):
    def test_stale_calibration_escalates_to_review_required(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_stale",
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
            source_context={
                "calibration_health_status": "healthy",
                "threshold_probability_quality": "healthy",
                "calibration_freshness_status": "stale",
                "sample_count": 24,
            },
        )
        self.assertEqual(assessment.calibration_gate_status, "review_required")
        self.assertEqual(assessment.actionability_status, "review_required")
        self.assertIn("calibration_freshness_stale", assessment.calibration_gate_reason_codes)

    def test_degraded_or_missing_calibration_escalates_to_review_required(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_missing",
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
            source_context={
                "calibration_health_status": "healthy",
                "threshold_probability_quality": "healthy",
                "calibration_freshness_status": "degraded_or_missing",
                "sample_count": 24,
            },
        )
        self.assertEqual(assessment.calibration_gate_status, "review_required")
        self.assertEqual(assessment.actionability_status, "review_required")
        self.assertTrue(assessment.calibration_impacted_market)

    def test_degraded_or_missing_plus_sparse_becomes_research_only(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_sparse",
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
            source_context={
                "calibration_health_status": "lookup_missing",
                "threshold_probability_quality": "sparse",
                "calibration_freshness_status": "degraded_or_missing",
                "sample_count": 0,
            },
        )
        self.assertEqual(assessment.calibration_gate_status, "research_only")
        self.assertEqual(assessment.actionability_status, "no_trade")
        self.assertIn("threshold_probability_quality_sparse", assessment.calibration_gate_reason_codes)


if __name__ == "__main__":
    unittest.main()
