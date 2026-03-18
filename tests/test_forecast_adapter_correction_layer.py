from __future__ import annotations

import unittest
from datetime import UTC, date, datetime

from domains.weather.forecast.adapters import resolve_distribution_summary_v2


class _Provider:
    def resolve_std_dev(self, **kwargs) -> float | None:  # noqa: ANN003
        del kwargs
        return 3.0

    def resolve_confidence_summary(self, **kwargs):  # noqa: ANN003
        del kwargs
        return None

    def resolve_profile_v2(self, **kwargs):  # noqa: ANN003
        del kwargs
        return type(
            "Profile",
            (),
            {
                "mean_bias": 2.5,
                "p90_abs_residual": 5.0,
                "mean_abs_residual": 3.0,
                "sample_count": 24,
                "regime_bucket": "warm",
                "regime_stability_score": 0.55,
                "empirical_coverage_50": 0.46,
                "empirical_coverage_80": 0.62,
                "empirical_coverage_95": 0.88,
                "threshold_probability_profile_json": {
                    "40-60": {
                        "sample_count": 16,
                        "predicted_prob_mean": 0.52,
                        "realized_hit_rate": 0.61,
                        "brier_score": 0.20,
                        "reliability_gap": 0.09,
                        "quality_status": "watch",
                    }
                },
                "calibration_health_status": "watch",
            },
        )()


class _Request:
    station_id = "KSEA"
    source = "openmeteo"
    observation_date = date(2026, 3, 11)
    forecast_target_time = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    metric = "temperature_max"


class ForecastAdapterCorrectionLayerTest(unittest.TestCase):
    def test_bias_correction_adjusts_mean_and_spread(self) -> None:
        summary = resolve_distribution_summary_v2(
            _Request(),
            _Provider(),
            raw_distribution={50: 0.2, 52: 0.6, 54: 0.2},
        )
        self.assertTrue(summary.lookup_hit)
        self.assertGreater(summary.corrected_mean, summary.raw_mean)
        self.assertGreater(summary.corrected_std_dev, summary.raw_std_dev)
        self.assertEqual(summary.bias_quality_status, "degraded")
        self.assertEqual(summary.threshold_probability_quality_status, "watch")
        self.assertIn("regime_unstable", summary.reason_codes)

    def test_lookup_miss_falls_back_without_bias_shift(self) -> None:
        summary = resolve_distribution_summary_v2(
            _Request(),
            None,
            raw_distribution={50: 0.2, 52: 0.6, 54: 0.2},
        )
        self.assertFalse(summary.lookup_hit)
        self.assertAlmostEqual(summary.corrected_mean, summary.raw_mean)
        self.assertEqual(summary.bias_quality_status, "lookup_missing")
        self.assertIn("calibration_v2_lookup_missing", summary.reason_codes)


if __name__ == "__main__":
    unittest.main()
