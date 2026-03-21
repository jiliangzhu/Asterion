from __future__ import annotations

import unittest
from datetime import date

from asterion_core.contracts import Rule2SpecDraft, StationMetadata, WeatherMarketSpecRecord
from domains.weather.spec import validate_rule2spec_draft


class Rule2SpecValidationTest(unittest.TestCase):
    def test_validation_passes_for_consistent_station_first_spec(self) -> None:
        draft = Rule2SpecDraft(
            market_id="mkt_weather_1",
            condition_id="cond_weather_1",
            location_name="New York City",
            observation_date=date(2026, 3, 8),
            observation_window_local="daily_max",
            metric="temperature_max",
            unit="fahrenheit",
            bucket_min_value=50.0,
            bucket_max_value=59.0,
            authoritative_source="weather.com",
            fallback_sources=["nws"],
            rounding_rule="identity",
            inclusive_bounds=True,
            parse_confidence=0.95,
            risk_flags=[],
        )
        spec = WeatherMarketSpecRecord(
            market_id="mkt_weather_1",
            condition_id="cond_weather_1",
            location_name="New York City",
            station_id="KNYC",
            latitude=40.7128,
            longitude=-74.0060,
            timezone="America/New_York",
            observation_date=date(2026, 3, 8),
            observation_window_local="daily_max",
            metric="temperature_max",
            unit="fahrenheit",
            bucket_min_value=50.0,
            bucket_max_value=59.0,
            authoritative_source="weather.com",
            fallback_sources=["nws"],
            rounding_rule="identity",
            inclusive_bounds=True,
            spec_version="spec_1",
            parse_confidence=0.95,
            risk_flags=[],
        )
        station = StationMetadata(
            station_id="KNYC",
            location_name="New York City",
            latitude=40.7128,
            longitude=-74.0060,
            timezone="America/New_York",
            source="nws",
        )
        result = validate_rule2spec_draft(draft, current_spec=spec, station_metadata=station)
        self.assertEqual(result.verdict, "pass")
        self.assertFalse(result.human_review_required)

    def test_validation_requires_review_for_station_or_spec_drift(self) -> None:
        draft = Rule2SpecDraft(
            market_id="mkt_weather_1",
            condition_id="cond_weather_1",
            location_name="Boston",
            observation_date=date(2026, 3, 8),
            observation_window_local="daily_max",
            metric="temperature_max",
            unit="fahrenheit",
            bucket_min_value=60.0,
            bucket_max_value=69.0,
            authoritative_source="weather.com",
            fallback_sources=["nws"],
            rounding_rule="identity",
            inclusive_bounds=True,
            parse_confidence=0.7,
            risk_flags=[],
        )
        spec = WeatherMarketSpecRecord(
            market_id="mkt_weather_1",
            condition_id="cond_weather_1",
            location_name="Boston",
            station_id="KBOS",
            latitude=42.0,
            longitude=-71.0,
            timezone="America/New_York",
            observation_date=date(2026, 3, 8),
            observation_window_local="daily_max",
            metric="temperature_min",
            unit="fahrenheit",
            bucket_min_value=60.0,
            bucket_max_value=69.0,
            authoritative_source="weather.com",
            fallback_sources=["nws"],
            rounding_rule="identity",
            inclusive_bounds=True,
            spec_version="spec_2",
            parse_confidence=0.95,
            risk_flags=[],
        )
        result = validate_rule2spec_draft(draft, current_spec=spec, station_metadata=None)
        self.assertEqual(result.verdict, "review")
        self.assertTrue(result.human_review_required)
        self.assertIn("missing_station_metadata", result.risk_flags)

    def test_validation_blocks_invalid_metric_and_threshold_range(self) -> None:
        draft = Rule2SpecDraft(
            market_id="mkt_weather_2",
            condition_id="cond_weather_2",
            location_name="Chicago",
            observation_date=date(2026, 3, 8),
            observation_window_local="daily_max",
            metric="wind_speed",
            unit="mph",
            bucket_min_value=70.0,
            bucket_max_value=60.0,
            authoritative_source="weather.com",
            fallback_sources=["nws"],
            rounding_rule="identity",
            inclusive_bounds=True,
            parse_confidence=0.45,
            risk_flags=[],
        )
        station = StationMetadata(
            station_id="KORD",
            location_name="Chicago",
            latitude=41.9742,
            longitude=-87.9073,
            timezone="America/Chicago",
            source="nws",
        )
        result = validate_rule2spec_draft(draft, current_spec=None, station_metadata=station)
        self.assertEqual(result.verdict, "block")
        self.assertTrue(result.human_review_required)
        self.assertIn("invalid_metric", result.risk_flags)
        self.assertIn("invalid_threshold_range", result.risk_flags)
        self.assertTrue(any("parse_confidence" in item for item in result.violations))


if __name__ == "__main__":
    unittest.main()
