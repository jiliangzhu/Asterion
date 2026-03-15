from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from asterion_core.contracts import ResolutionSpec, build_forecast_cache_key
from domains.weather.forecast import (
    AdapterRouter,
    ForecastService,
    InMemoryForecastCache,
    NWSAdapter,
    OpenMeteoAdapter,
    build_forecast_request,
)


class _RoutingClient:
    def __init__(self, routes: dict[str, object]) -> None:
        self.routes = routes
        self.calls: list[str] = []

    def get_json(self, url: str, *, context: dict) -> object:
        self.calls.append(url)
        for pattern, payload in self.routes.items():
            if pattern in url:
                if isinstance(payload, Exception):
                    raise payload
                return payload
        raise AssertionError(f"unexpected url: {url}")


def _resolution_spec() -> ResolutionSpec:
    return ResolutionSpec(
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
        authoritative_source="weather.com",
        fallback_sources=["nws", "open-meteo"],
        rounding_rule="identity",
        inclusive_bounds=True,
        spec_version="spec_abc123",
    )


class ForecastServiceTest(unittest.TestCase):
    def test_build_forecast_request_from_resolution_spec(self) -> None:
        resolution_spec = _resolution_spec()
        forecast_target_time = datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc)
        request = build_forecast_request(
            resolution_spec,
            source="openmeteo",
            model_run="2026-03-07T12:00Z",
            forecast_target_time=forecast_target_time,
        )
        self.assertEqual(request.station_id, "KNYC")
        self.assertEqual(request.latitude, 40.7128)
        self.assertEqual(request.source, "openmeteo")

    def test_openmeteo_fetch_and_cache_hit(self) -> None:
        client = _RoutingClient(
            {
                "api.open-meteo.com": {
                    "daily": {
                        "temperature_2m_max": [55.4],
                    }
                }
            }
        )
        service = ForecastService(
            adapter_router=AdapterRouter([OpenMeteoAdapter(client=client)]),
            cache=InMemoryForecastCache(),
        )
        resolution_spec = _resolution_spec()
        forecast_target_time = datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc)

        first = service.get_forecast(
            resolution_spec,
            source="openmeteo",
            model_run="2026-03-07T12:00Z",
            forecast_target_time=forecast_target_time,
        )
        second = service.get_forecast(
            resolution_spec,
            source="open-meteo",
            model_run="2026-03-07T12:00Z",
            forecast_target_time=forecast_target_time,
        )

        expected_key = build_forecast_cache_key(
            market_id="mkt_weather_1",
            station_id="KNYC",
            spec_version="spec_abc123",
            source="openmeteo",
            model_run="2026-03-07T12:00Z",
            forecast_target_time=forecast_target_time,
        )
        # Verify normal distribution centered around 55°F
        self.assertAlmostEqual(sum(first.temperature_distribution.values()), 1.0, places=6)
        self.assertIn(55, first.temperature_distribution)
        self.assertGreater(first.temperature_distribution[55], 0.1)  # Peak probability at mean
        self.assertFalse(first.from_cache)
        self.assertTrue(second.from_cache)
        self.assertEqual(second.cache_key, expected_key)
        self.assertEqual(len(client.calls), 1)

    def test_fallback_to_nws_is_observable(self) -> None:
        client = _RoutingClient(
            {
                "api.open-meteo.com": RuntimeError("upstream down"),
                "/points/40.7128,-74.006": {
                    "properties": {"forecast": "https://api.weather.gov/gridpoints/OKX/33,37/forecast"}
                },
                "/gridpoints/OKX/33,37/forecast": {
                    "properties": {
                        "periods": [
                            {"temperature": 53},
                            {"temperature": 58},
                            {"temperature": 56},
                        ]
                    }
                },
            }
        )
        service = ForecastService(
            adapter_router=AdapterRouter(
                [
                    OpenMeteoAdapter(client=client),
                    NWSAdapter(client=client),
                ]
            ),
            cache=InMemoryForecastCache(),
        )
        resolution_spec = _resolution_spec()
        forecast = service.get_forecast(
            resolution_spec,
            source="openmeteo",
            model_run="2026-03-07T12:00Z",
            forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(forecast.source, "nws")
        self.assertTrue(forecast.fallback_used)
        self.assertEqual(forecast.source_trace, ["openmeteo", "nws"])
        # Verify normal distribution centered around 58°F
        self.assertAlmostEqual(sum(forecast.temperature_distribution.values()), 1.0, places=6)
        self.assertIn(58, forecast.temperature_distribution)
        self.assertGreater(forecast.temperature_distribution[58], 0.1)  # Peak probability at mean


if __name__ == "__main__":
    unittest.main()
