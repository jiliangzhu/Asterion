from __future__ import annotations

import unittest

from domains.weather.forecast.calibration import (
    threshold_probability_bucket,
    threshold_probability_profile_for_probability,
)


class ThresholdProbabilityProfileTest(unittest.TestCase):
    def test_probability_bucket_boundaries(self) -> None:
        self.assertEqual(threshold_probability_bucket(0.05), "0-10")
        self.assertEqual(threshold_probability_bucket(0.10), "10-25")
        self.assertEqual(threshold_probability_bucket(0.59), "40-60")
        self.assertEqual(threshold_probability_bucket(0.91), "90-100")

    def test_selects_profile_for_current_probability(self) -> None:
        profile = threshold_probability_profile_for_probability(
            {
                "40-60": {
                    "sample_count": 18,
                    "predicted_prob_mean": 0.52,
                    "realized_hit_rate": 0.58,
                    "brier_score": 0.21,
                    "reliability_gap": 0.06,
                    "quality_status": "watch",
                }
            },
            0.55,
        )
        self.assertIsNotNone(profile)
        self.assertEqual(profile.threshold_bucket, "40-60")
        self.assertEqual(profile.sample_count, 18)
        self.assertAlmostEqual(profile.reliability_gap or 0.0, 0.06)
        self.assertEqual(profile.quality_status, "watch")


if __name__ == "__main__":
    unittest.main()
