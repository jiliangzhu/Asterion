from __future__ import annotations

import unittest
from datetime import UTC, date, datetime

from asterion_core.contracts import ForecastRunRecord, WeatherMarket, WeatherMarketSpecRecord
from domains.weather.pricing import build_binary_fair_values, build_forecast_calibration_pricing_context


def _forecast_run() -> ForecastRunRecord:
    return ForecastRunRecord(
        run_id="frun_phase13",
        market_id="mkt_1",
        condition_id="cond_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-10T12:00Z",
        forecast_target_time=datetime(2026, 3, 10, 12, 0, tzinfo=UTC),
        observation_date=date(2026, 3, 11),
        metric="temperature_max",
        latitude=40.7128,
        longitude=-74.0060,
        timezone="America/New_York",
        spec_version="spec_v1",
        cache_key="cache_1",
        source_trace=["openmeteo"],
        fallback_used=False,
        from_cache=False,
        confidence=0.72,
        forecast_payload={
            "temperature_distribution": {58: 0.15, 60: 0.35, 62: 0.35, 64: 0.15},
            "distribution_summary_v2": {
                "corrected_mean": 61.2,
                "corrected_std_dev": 2.9,
                "bias_quality_status": "watch",
                "threshold_probability_quality_status": "watch",
                "regime_bucket": "warm",
                "regime_stability_score": 0.72,
                "lookup_hit": True,
                "threshold_probability_summary_json": {
                    "60-75": {
                        "sample_count": 14,
                        "predicted_prob_mean": 0.68,
                        "realized_hit_rate": 0.74,
                        "brier_score": 0.18,
                        "reliability_gap": 0.06,
                        "quality_status": "watch",
                    }
                },
            },
        },
        raw_payload={},
    )


def _market() -> WeatherMarket:
    return WeatherMarket(
        market_id="mkt_1",
        condition_id="cond_1",
        event_id="evt_1",
        slug="weather-mkt-1",
        title="NYC high temp 60-69F",
        description=None,
        rules=None,
        status="active",
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        tags=["Weather"],
        outcomes=["YES", "NO"],
        token_ids=["tok_yes", "tok_no"],
        close_time=datetime(2026, 3, 11, 23, 59, tzinfo=UTC),
        end_date=datetime(2026, 3, 11, 23, 59, tzinfo=UTC),
        raw_market={},
    )


def _spec() -> WeatherMarketSpecRecord:
    return WeatherMarketSpecRecord(
        market_id="mkt_1",
        condition_id="cond_1",
        location_name="New York City",
        station_id="KNYC",
        latitude=40.7128,
        longitude=-74.0060,
        timezone="America/New_York",
        observation_date=date(2026, 3, 11),
        observation_window_local="daily_max",
        metric="temperature_max",
        unit="fahrenheit",
        bucket_min_value=60.0,
        bucket_max_value=69.0,
        authoritative_source="weather.com",
        fallback_sources=["openmeteo"],
        rounding_rule="identity",
        inclusive_bounds=True,
        spec_version="spec_v1",
        parse_confidence=0.95,
        risk_flags=[],
    )


class WeatherThresholdPricingQualityTest(unittest.TestCase):
    def test_builds_fair_values_from_corrected_distribution_and_context(self) -> None:
        run = _forecast_run()
        fair_values = build_binary_fair_values(market=_market(), spec=_spec(), forecast_run=run)
        self.assertEqual(len(fair_values), 2)
        yes = next(item for item in fair_values if item.outcome == "YES")
        self.assertAlmostEqual(yes.fair_value, 0.85)

        context = build_forecast_calibration_pricing_context(
            forecast_run=run,
            outcome="YES",
            fair_value=yes.fair_value,
        )
        self.assertEqual(context["threshold_probability_quality_status"], "watch")
        self.assertEqual(context["bias_quality_status"], "watch")
        self.assertEqual(context["regime_bucket"], "warm")
        self.assertAlmostEqual(context["corrected_mean"], 61.2)


if __name__ == "__main__":
    unittest.main()
