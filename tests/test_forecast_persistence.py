from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import ResolutionSpec
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.forecast import (
    AdapterRouter,
    ForecastService,
    InMemoryForecastCache,
    NWSAdapter,
    OpenMeteoAdapter,
    build_forecast_run_record,
    enqueue_forecast_run_upserts,
)
from domains.weather.forecast.adapters import build_normal_distribution


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class _RoutingClient:
    def __init__(self, routes: dict[str, object]) -> None:
        self.routes = routes

    def get_json(self, url: str, *, context: dict) -> object:
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


class ForecastPersistenceUnitTest(unittest.TestCase):
    def test_build_forecast_run_record_keeps_cache_and_trace(self) -> None:
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
        distribution = service.get_forecast(
            _resolution_spec(),
            source="openmeteo",
            model_run="2026-03-07T12:00Z",
            forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
        )

        record = build_forecast_run_record(distribution)
        expected_distribution = build_normal_distribution(55.4, 3.0)

        self.assertTrue(record.run_id.startswith("frun_"))
        self.assertEqual(record.cache_key, distribution.cache_key)
        self.assertEqual(record.source_trace, ["openmeteo"])
        self.assertAlmostEqual(sum(record.forecast_payload["temperature_distribution"].values()), 1.0, places=9)
        self.assertAlmostEqual(
            record.forecast_payload["temperature_distribution"][55],
            expected_distribution[55],
            places=12,
        )
        self.assertFalse(record.from_cache)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for forecast persistence tests")
class ForecastPersistenceDuckDBTest(unittest.TestCase):
    def test_forecast_run_persistence_writes_cache_and_trace(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")

            with patch.dict(
                os.environ,
                {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "writer",
                    "WRITERD": "1",
                },
                clear=False,
            ):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))

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
                            ]
                        }
                    },
                }
            )
            service = ForecastService(
                adapter_router=AdapterRouter([OpenMeteoAdapter(client=client), NWSAdapter(client=client)]),
                cache=InMemoryForecastCache(),
            )
            distribution = service.get_forecast(
                _resolution_spec(),
                source="openmeteo",
                model_run="2026-03-07T12:00Z",
                forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
            )
            record = build_forecast_run_record(distribution)
            queue_cfg = WriteQueueConfig(path=queue_path)
            task_id = enqueue_forecast_run_upserts(queue_cfg, forecast_runs=[record], run_id="run_forecast_persist")
            self.assertIsNotNone(task_id)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": "weather.weather_forecast_runs",
                },
                clear=False,
            ):
                processed = process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False)
            self.assertTrue(processed)

            import duckdb

            con = duckdb.connect(db_path, read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT
                        run_id,
                        source,
                        cache_key,
                        source_trace_json,
                        fallback_used,
                        from_cache,
                        confidence,
                        forecast_payload_json,
                        raw_payload_json
                    FROM weather.weather_forecast_runs
                    WHERE run_id = ?
                    """,
                    [record.run_id],
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(row[0], record.run_id)
            self.assertEqual(row[1], "nws")
            self.assertEqual(row[2], record.cache_key)
            self.assertEqual(json.loads(row[3]), ["openmeteo", "nws"])
            self.assertTrue(row[4])
            self.assertFalse(row[5])
            expected_distribution = build_normal_distribution(58.0, 3.0)
            self.assertAlmostEqual(row[6], max(expected_distribution.values()))
            self.assertEqual(json.loads(row[7])["temperature_distribution"], {str(k): v for k, v in expected_distribution.items()})
            self.assertIn("forecast", json.loads(row[8]))


if __name__ == "__main__":
    unittest.main()
