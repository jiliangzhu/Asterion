from __future__ import annotations

import unittest

from domains.weather.opportunity import build_weather_opportunity_assessment


class Phase13CalibrationPenaltyIntegrationTest(unittest.TestCase):
    def test_bias_threshold_and_regime_penalties_reduce_ranking(self) -> None:
        healthy = build_weather_opportunity_assessment(
            market_id="mkt_phase13_healthy",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.42,
            model_fair_value=0.68,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=400,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            calibration_health_status="healthy",
            calibration_bias_quality="healthy",
            threshold_probability_quality="healthy",
            sample_count=32,
            forecast_distribution_summary_v2={"regime_stability_score": 0.92},
        )
        degraded = build_weather_opportunity_assessment(
            market_id="mkt_phase13_degraded",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.42,
            model_fair_value=0.68,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=400,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            calibration_health_status="watch",
            calibration_bias_quality="degraded",
            threshold_probability_quality="degraded",
            sample_count=32,
            forecast_distribution_summary_v2={"regime_stability_score": 0.55},
        )
        self.assertEqual(healthy.edge_bps_executable, degraded.edge_bps_executable)
        self.assertGreater(healthy.uncertainty_multiplier, degraded.uncertainty_multiplier)
        self.assertGreater(healthy.ranking_score, degraded.ranking_score)
        self.assertIn("calibration_bias_degraded", degraded.ranking_penalty_reasons)
        self.assertIn("threshold_probability_degraded", degraded.ranking_penalty_reasons)
        self.assertIn("regime_unstable", degraded.ranking_penalty_reasons)

    def test_why_ranked_json_contains_phase13_fields(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_phase13_json",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.43,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=400,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            calibration_health_status="healthy",
            calibration_bias_quality="watch",
            threshold_probability_quality="watch",
            sample_count=22,
            forecast_distribution_summary_v2={"regime_stability_score": 0.74},
            source_context={
                "corrected_mean": 61.3,
                "corrected_std_dev": 3.2,
                "threshold_probability_summary_json": {"40-60": {"quality_status": "watch"}},
                "regime_bucket": "warm",
            },
        )
        self.assertEqual(assessment.why_ranked_json["bias_quality_status"], "watch")
        self.assertEqual(assessment.why_ranked_json["threshold_probability_quality_status"], "watch")
        self.assertEqual(assessment.why_ranked_json["regime_bucket"], "warm")
        self.assertAlmostEqual(float(assessment.why_ranked_json["corrected_mean"]), 61.3)


if __name__ == "__main__":
    unittest.main()
