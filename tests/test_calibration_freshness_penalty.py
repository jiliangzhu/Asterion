from __future__ import annotations

import unittest

from domains.weather.opportunity import build_weather_opportunity_assessment


class CalibrationFreshnessPenaltyTest(unittest.TestCase):
    def _assessment(self, freshness: str, *, reason_codes: list[str] | None = None):
        return build_weather_opportunity_assessment(
            market_id="mkt_1",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.68,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            source_context={
                "calibration_health_status": "healthy",
                "calibration_bias_quality": "healthy",
                "threshold_probability_quality_status": "healthy",
                "sample_count": 20,
                "calibration_reason_codes": list(reason_codes or []),
                "calibration_freshness_status": freshness,
                "profile_materialized_at": "2026-03-18T03:15:00+00:00",
                "profile_window_end": "2026-03-18T02:00:00+00:00",
                "profile_age_hours": 12.0 if freshness == "fresh" else 72.0 if freshness == "stale" else 120.0,
            },
        )

    def test_stale_profile_lowers_uncertainty_multiplier_and_records_reason(self) -> None:
        fresh = self._assessment("fresh")
        stale = self._assessment("stale", reason_codes=["calibration_profile_stale"])
        degraded = self._assessment("degraded_or_missing", reason_codes=["calibration_profile_missing_or_degraded"])

        self.assertGreater(fresh.uncertainty_multiplier, stale.uncertainty_multiplier)
        self.assertGreater(stale.uncertainty_multiplier, degraded.uncertainty_multiplier)
        self.assertIn("calibration_profile_stale", stale.ranking_penalty_reasons)
        self.assertIn("calibration_profile_missing_or_degraded", degraded.ranking_penalty_reasons)
        self.assertEqual(stale.assessment_context_json["calibration_freshness_status"], "stale")
        self.assertEqual(degraded.why_ranked_json["calibration_freshness_status"], "degraded_or_missing")


if __name__ == "__main__":
    unittest.main()
