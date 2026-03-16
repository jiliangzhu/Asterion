from __future__ import annotations

import unittest

from domains.weather.forecast import calibration_confidence_from_metrics
from domains.weather.opportunity import build_weather_opportunity_assessment


class Phase7RankingPenaltyTest(unittest.TestCase):
    def test_calibration_confidence_tiers_are_deterministic(self) -> None:
        missing = calibration_confidence_from_metrics(
            sample_count=0,
            mean_abs_residual=None,
            p90_abs_residual=None,
            lookup_hit=False,
        )
        limited = calibration_confidence_from_metrics(
            sample_count=8,
            mean_abs_residual=1.2,
            p90_abs_residual=2.0,
            lookup_hit=True,
        )
        healthy = calibration_confidence_from_metrics(
            sample_count=24,
            mean_abs_residual=1.1,
            p90_abs_residual=2.1,
            lookup_hit=True,
        )
        degraded = calibration_confidence_from_metrics(
            sample_count=24,
            mean_abs_residual=3.4,
            p90_abs_residual=5.2,
            lookup_hit=True,
        )
        self.assertEqual(missing.calibration_health_status, "lookup_missing")
        self.assertEqual(missing.calibration_confidence_multiplier, 0.50)
        self.assertEqual(limited.calibration_health_status, "limited_samples")
        self.assertEqual(limited.calibration_confidence_multiplier, 0.75)
        self.assertEqual(healthy.calibration_health_status, "healthy")
        self.assertEqual(healthy.calibration_confidence_multiplier, 1.0)
        self.assertEqual(degraded.calibration_health_status, "degraded")
        self.assertEqual(degraded.calibration_confidence_multiplier, 0.60)

    def test_combined_penalties_reduce_ranking_score(self) -> None:
        healthy = build_weather_opportunity_assessment(
            market_id="mkt_healthy",
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
            calibration_health_status="healthy",
            sample_count=24,
            mapping_confidence=0.95,
            source_freshness_status="fresh",
            price_staleness_ms=10_000,
        )
        penalized = build_weather_opportunity_assessment(
            market_id="mkt_penalized",
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
            calibration_health_status="degraded",
            sample_count=24,
            calibration_reason_codes=["calibration_degraded"],
            mapping_confidence=0.60,
            source_freshness_status="degraded",
            price_staleness_ms=10_000,
        )
        self.assertEqual(healthy.edge_bps_executable, penalized.edge_bps_executable)
        self.assertGreater(healthy.ranking_score, penalized.ranking_score)
        self.assertLess(penalized.uncertainty_multiplier, healthy.uncertainty_multiplier)
        self.assertIn("calibration_degraded", penalized.ranking_penalty_reasons)
        self.assertIn("freshness_degraded", penalized.ranking_penalty_reasons)
        self.assertIn("mapping_confidence_reduced", penalized.ranking_penalty_reasons)


if __name__ == "__main__":
    unittest.main()
