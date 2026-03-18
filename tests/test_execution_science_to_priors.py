from __future__ import annotations

import unittest

import pandas as pd

from domains.weather.opportunity import build_execution_science_cohort_summaries


class ExecutionScienceToPriorsTest(unittest.TestCase):
    def test_shared_helper_produces_canonical_market_strategy_wallet_summaries(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "ticket_id": "tt_1",
                    "market_id": "mkt_1",
                    "strategy_id": "weather_primary",
                    "wallet_id": "wallet_weather_1",
                    "execution_lifecycle_stage": "resolved",
                    "filled_quantity": 10.0,
                    "evaluation_status": "resolved",
                    "miss_reason_bucket": "captured_resolved",
                    "distortion_reason_codes_json": '["forecast_realized_pnl_negative"]',
                },
                {
                    "ticket_id": "tt_2",
                    "market_id": "mkt_1",
                    "strategy_id": "weather_primary",
                    "wallet_id": "wallet_weather_1",
                    "execution_lifecycle_stage": "submit_rejected",
                    "filled_quantity": 0.0,
                    "evaluation_status": "pending_resolution",
                    "miss_reason_bucket": "submit_rejected",
                    "distortion_reason_codes_json": '["execution_unfilled"]',
                },
            ]
        )

        summaries = build_execution_science_cohort_summaries(frame)
        keyed = {(item.cohort_type, item.cohort_key): item for item in summaries}

        self.assertIn(("market", "mkt_1"), keyed)
        self.assertIn(("strategy", "weather_primary"), keyed)
        self.assertIn(("wallet", "wallet_weather_1"), keyed)
        market = keyed[("market", "mkt_1")]
        self.assertEqual(market.ticket_count, 2)
        self.assertEqual(market.dominant_miss_reason_bucket, "submit_rejected")
        self.assertEqual(market.dominant_distortion_reason_bucket, "execution_distortion")
        self.assertAlmostEqual(market.miss_rate, 0.5)


if __name__ == "__main__":
    unittest.main()
