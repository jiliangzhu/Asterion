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
    OpenMeteoAdapter,
    build_forecast_run_record,
    enqueue_forecast_run_upserts,
)
from domains.weather.pricing import (
    build_binary_fair_values,
    build_watch_only_snapshot,
    enqueue_fair_value_upserts,
    enqueue_watch_only_snapshot_upserts,
    load_forecast_run,
    load_weather_market,
    load_weather_market_spec,
    probability_in_bucket,
)
from domains.weather.scout import run_weather_market_discovery
from domains.weather.spec import (
    StationMapper,
    build_station_mapping_record,
    build_weather_market_spec_record_via_station_mapper,
    enqueue_station_mapping_upserts,
    enqueue_weather_market_spec_upserts,
    load_weather_markets_for_rule2spec,
    parse_rule2spec_draft,
)


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


class _GammaClient:
    def __init__(self, pages: list[list[dict]]) -> None:
        self._pages = pages

    def get_json(self, url: str, *, context: dict) -> dict:
        page = int(context["page"])
        return {"markets": self._pages[page] if page < len(self._pages) else []}


def _raw_weather_market() -> dict:
    return {
        "id": "mkt_weather_1",
        "conditionId": "cond_weather_1",
        "question": "Will the high temperature in New York City on March 8, 2026 be 50-59°F?",
        "description": "Template weather market",
        "rules": "Resolve to Yes if the observed high temperature is within range.",
        "slug": "nyc-high-temp-mar-8",
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
        "tags": ["Weather", "Temperature"],
        "outcomes": "[\"Yes\", \"No\"]",
        "clobTokenIds": "[\"tok_yes\", \"tok_no\"]",
        "closeTime": "2026-03-08T23:59:59Z",
        "endDate": "2026-03-08T23:59:59Z",
        "createdAt": "2026-03-01T00:00:00Z",
        "event": {"id": "evt_weather_1", "category": "Weather"},
    }


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


class WeatherPricingUnitTest(unittest.TestCase):
    def test_probability_in_bucket_and_snapshot_decision(self) -> None:
        probability = probability_in_bucket(
            {49: 0.2, 50: 0.3, 55: 0.4, 60: 0.1},
            bucket_min=50.0,
            bucket_max=59.0,
            inclusive_bounds=True,
        )
        self.assertAlmostEqual(probability, 0.7)

    def test_snapshot_uses_fair_value_edge(self) -> None:
        from asterion_core.contracts import WeatherFairValueRecord

        fair_value = WeatherFairValueRecord(
            fair_value_id="fv1",
            run_id="run1",
            market_id="m1",
            condition_id="c1",
            token_id="tok_yes",
            outcome="YES",
            fair_value=0.70,
            confidence=0.9,
        )
        snapshot = build_watch_only_snapshot(
            fair_value=fair_value,
            reference_price=0.55,
            threshold_bps=500,
            pricing_context={"source": "unit_test"},
        )
        self.assertEqual(snapshot.decision, "TAKE")
        self.assertEqual(snapshot.side, "BUY")
        self.assertGreater(snapshot.edge_bps, 0)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for weather pricing tests")
class WeatherPricingDuckDBTest(unittest.TestCase):
    def test_forecast_run_drives_fair_values_and_watch_only_snapshots(self) -> None:
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

            allow_tables = ",".join(
                [
                    "weather.weather_markets",
                    "weather.weather_station_map",
                    "weather.weather_market_specs",
                    "weather.weather_forecast_runs",
                    "weather.weather_fair_values",
                    "weather.weather_watch_only_snapshots",
                ]
            )

            gamma_client = _GammaClient([[_raw_weather_market()]])
            queue_cfg = WriteQueueConfig(path=queue_path)
            discovery = run_weather_market_discovery(
                base_url="https://gamma.example",
                markets_endpoint="/markets",
                page_limit=100,
                max_pages=1,
                sleep_s=0.0,
                active_only=True,
                closed=False,
                archived=False,
                client=gamma_client,
                queue_cfg=queue_cfg,
                run_id="run_market_discovery",
            )
            self.assertEqual(discovery.discovered_count, 1)

            station_mapping = build_station_mapping_record(
                market_id="mkt_weather_1",
                location_name="New York City",
                station_id="KNYC",
                station_name="Central Park",
                latitude=40.7128,
                longitude=-74.0060,
                timezone="America/New_York",
                source="operator_override",
                authoritative_source="unknown",
                is_override=True,
                metadata={"kind": "manual"},
            )
            enqueue_station_mapping_upserts(queue_cfg, mappings=[station_mapping], run_id="run_station_map")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                reader_env = {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                }
                from asterion_core.storage.database import DuckDBConfig, connect_duckdb

                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        weather_market = load_weather_markets_for_rule2spec(con, active_only=True, limit=10)[0]
                        draft = parse_rule2spec_draft(weather_market)
                        mapper = StationMapper()
                        spec_record = build_weather_market_spec_record_via_station_mapper(draft, mapper=mapper, con=con)
                    finally:
                        con.close()

            enqueue_weather_market_spec_upserts(queue_cfg, specs=[spec_record], run_id="run_spec")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            forecast_client = _RoutingClient(
                {
                    "api.open-meteo.com": {
                        "daily": {
                            "temperature_2m_max": [55.0],
                        }
                    }
                }
            )
            service = ForecastService(
                adapter_router=AdapterRouter([OpenMeteoAdapter(client=forecast_client)]),
                cache=InMemoryForecastCache(),
            )
            distribution = service.get_forecast(
                _resolution_spec(),
                source="openmeteo",
                model_run="2026-03-07T12:00Z",
                forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
            )
            forecast_run = build_forecast_run_record(distribution)
            enqueue_forecast_run_upserts(queue_cfg, forecast_runs=[forecast_run], run_id="run_forecast")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                from asterion_core.storage.database import DuckDBConfig, connect_duckdb

                reader_env = {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                }
                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        loaded_market = load_weather_market(con, market_id="mkt_weather_1")
                        loaded_spec = load_weather_market_spec(con, market_id="mkt_weather_1")
                        loaded_run = load_forecast_run(con, run_id=forecast_run.run_id)
                    finally:
                        con.close()

            fair_values = build_binary_fair_values(
                market=loaded_market,
                spec=loaded_spec,
                forecast_run=loaded_run,
            )
            yes_value = next(item for item in fair_values if item.outcome == "YES")
            no_value = next(item for item in fair_values if item.outcome == "NO")
            self.assertEqual(yes_value.fair_value, 1.0)
            self.assertEqual(no_value.fair_value, 0.0)

            snapshots = [
                build_watch_only_snapshot(
                    fair_value=yes_value,
                    reference_price=0.72,
                    threshold_bps=300,
                    pricing_context={"forecast_run_id": loaded_run.run_id, "source_trace": loaded_run.source_trace},
                ),
                build_watch_only_snapshot(
                    fair_value=no_value,
                    reference_price=0.20,
                    threshold_bps=300,
                    pricing_context={"forecast_run_id": loaded_run.run_id, "source_trace": loaded_run.source_trace},
                ),
            ]

            enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id="run_fair_values")
            enqueue_watch_only_snapshot_upserts(queue_cfg, snapshots=snapshots, run_id="run_watch_snapshots")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            import duckdb

            con = duckdb.connect(db_path, read_only=True)
            try:
                fv_rows = con.execute(
                    """
                    SELECT outcome, fair_value, run_id
                    FROM weather.weather_fair_values
                    WHERE market_id = 'mkt_weather_1'
                    ORDER BY outcome
                    """
                ).fetchall()
                snap_rows = con.execute(
                    """
                    SELECT outcome, decision, side, pricing_context_json
                    FROM weather.weather_watch_only_snapshots
                    WHERE market_id = 'mkt_weather_1'
                    ORDER BY outcome
                    """
                ).fetchall()
            finally:
                con.close()

            self.assertEqual(fv_rows, [("NO", 0.0, forecast_run.run_id), ("YES", 1.0, forecast_run.run_id)])
            self.assertEqual(snap_rows[0][0], "NO")
            self.assertEqual(snap_rows[0][1], "TAKE")
            self.assertEqual(snap_rows[0][2], "SELL")
            self.assertEqual(json.loads(snap_rows[0][3])["forecast_run_id"], forecast_run.run_id)
            self.assertEqual(snap_rows[1][0], "YES")
            self.assertEqual(snap_rows[1][1], "TAKE")
            self.assertEqual(snap_rows[1][2], "BUY")


if __name__ == "__main__":
    unittest.main()
