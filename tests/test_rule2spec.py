from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import StationMetadata
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.scout import run_weather_market_discovery
from domains.weather.spec import (
    build_resolution_spec_from_draft,
    build_resolution_spec_via_station_mapper,
    build_rule2spec_review_payload,
    build_station_mapping_record,
    build_weather_market_spec_record,
    build_weather_market_spec_record_via_station_mapper,
    enqueue_station_mapping_upserts,
    enqueue_weather_market_spec_upserts,
    load_weather_markets_for_rule2spec,
    parse_rule2spec_draft,
    StationMapper,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class _FakeClient:
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


class Rule2SpecDraftTest(unittest.TestCase):
    def test_parse_rule2spec_draft_from_weather_market(self) -> None:
        client = _FakeClient([[_raw_weather_market()]])
        market = run_weather_market_discovery(
            base_url="https://gamma.example",
            markets_endpoint="/markets",
            page_limit=100,
            max_pages=1,
            sleep_s=0.0,
            active_only=True,
            closed=False,
            archived=False,
            client=client,
        ).discovered_markets[0]

        draft = parse_rule2spec_draft(market)

        self.assertEqual(draft.market_id, "mkt_weather_1")
        self.assertEqual(draft.location_name, "New York City")
        self.assertEqual(draft.metric, "temperature_max")
        self.assertEqual(draft.unit, "fahrenheit")
        self.assertEqual(draft.bucket_min_value, 50.0)
        self.assertEqual(draft.bucket_max_value, 59.0)
        self.assertEqual(draft.authoritative_source, "unknown")
        self.assertIn("missing_authoritative_source", draft.risk_flags)

    def test_build_resolution_spec_from_draft_and_station_metadata(self) -> None:
        client = _FakeClient([[_raw_weather_market()]])
        market = run_weather_market_discovery(
            base_url="https://gamma.example",
            markets_endpoint="/markets",
            page_limit=100,
            max_pages=1,
            sleep_s=0.0,
            active_only=True,
            closed=False,
            archived=False,
            client=client,
        ).discovered_markets[0]
        draft = parse_rule2spec_draft(market)
        station = StationMetadata(
            station_id="KNYC",
            location_name="New York City",
            latitude=40.7128,
            longitude=-74.0060,
            timezone="America/New_York",
            source="operator_override",
        )

        spec = build_resolution_spec_from_draft(draft, station_metadata=station)
        review_payload = build_rule2spec_review_payload(draft)

        self.assertEqual(spec.station_id, "KNYC")
        self.assertEqual(spec.latitude, 40.7128)
        self.assertEqual(spec.metric, "temperature_max")
        self.assertTrue(spec.spec_version.startswith("spec_"))
        self.assertEqual(review_payload["location_name"], "New York City")
        self.assertEqual(review_payload["bucket_min_value"], 50.0)

    def test_build_resolution_spec_via_station_mapper(self) -> None:
        client = _FakeClient([[_raw_weather_market()]])
        market = run_weather_market_discovery(
            base_url="https://gamma.example",
            markets_endpoint="/markets",
            page_limit=100,
            max_pages=1,
            sleep_s=0.0,
            active_only=True,
            closed=False,
            archived=False,
            client=client,
        ).discovered_markets[0]
        draft = parse_rule2spec_draft(market)

        class _Mapper:
            def resolve_from_spec_inputs(self, con, *, market_id: str, location_name: str, authoritative_source: str) -> StationMetadata:
                self.last = (market_id, location_name, authoritative_source)
                return StationMetadata(
                    station_id="KNYC",
                    location_name=location_name,
                    latitude=40.7128,
                    longitude=-74.0060,
                    timezone="America/New_York",
                    source="station_registry",
                )

        mapper = _Mapper()
        spec = build_resolution_spec_via_station_mapper(draft, mapper=mapper, con=None)
        spec_record = build_weather_market_spec_record_via_station_mapper(draft, mapper=mapper, con=None)

        self.assertEqual(spec.station_id, "KNYC")
        self.assertEqual(spec_record.station_id, "KNYC")
        self.assertEqual(mapper.last, ("mkt_weather_1", "New York City", "unknown"))


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for rule2spec persistence tests")
class Rule2SpecPersistenceTest(unittest.TestCase):
    def test_weather_markets_feed_rule2spec_and_persist_weather_market_specs(self) -> None:
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

            market_queue_cfg = WriteQueueConfig(path=queue_path)
            client = _FakeClient([[_raw_weather_market()]])
            discovery = run_weather_market_discovery(
                base_url="https://gamma.example",
                markets_endpoint="/markets",
                page_limit=100,
                max_pages=2,
                sleep_s=0.0,
                active_only=True,
                closed=False,
                archived=False,
                client=client,
                queue_cfg=market_queue_cfg,
                run_id="run_market_discovery",
            )
            self.assertEqual(discovery.discovered_count, 1)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": "weather.weather_markets,weather.weather_station_map,weather.weather_market_specs",
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                reader_env = {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                }
                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        weather_markets = load_weather_markets_for_rule2spec(con, active_only=True, limit=10)
                    finally:
                        con.close()

                self.assertEqual(len(weather_markets), 1)
                draft = parse_rule2spec_draft(weather_markets[0])
                station_mapping = build_station_mapping_record(
                    location_name="New York City",
                    station_id="KNYC",
                    station_name="Central Park",
                    latitude=40.7128,
                    longitude=-74.0060,
                    timezone="America/New_York",
                    source="operator_override",
                    market_id="mkt_weather_1",
                    is_override=True,
                    metadata={"kind": "manual"},
                )
                station_task_id = enqueue_station_mapping_upserts(
                    spec_queue_cfg := WriteQueueConfig(path=queue_path),
                    mappings=[station_mapping],
                    run_id="run_station_mapper",
                )
                self.assertIsNotNone(station_task_id)
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        mapper = StationMapper()
                        spec = build_resolution_spec_via_station_mapper(draft, mapper=mapper, con=con)
                        spec_record = build_weather_market_spec_record_via_station_mapper(draft, mapper=mapper, con=con)
                    finally:
                        con.close()

                self.assertEqual(spec.station_id, "KNYC")
                spec_queue_cfg = WriteQueueConfig(path=queue_path)
                task_id = enqueue_weather_market_spec_upserts(
                    spec_queue_cfg,
                    specs=[spec_record],
                    run_id="run_rule2spec",
                )
                self.assertIsNotNone(task_id)
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            import duckdb

            read_con = duckdb.connect(db_path, read_only=True)
            try:
                row = read_con.execute(
                    """
                    SELECT
                        market_id,
                        station_id,
                        metric,
                        bucket_min_value,
                        bucket_max_value,
                        parse_confidence,
                        risk_flags_json
                    FROM weather.weather_market_specs
                    WHERE market_id = 'mkt_weather_1'
                    """
                ).fetchone()
            finally:
                read_con.close()

            self.assertEqual(row[0], "mkt_weather_1")
            self.assertEqual(row[1], "KNYC")
            self.assertEqual(row[2], "temperature_max")
            self.assertEqual(row[3], 50.0)
            self.assertEqual(row[4], 59.0)
            self.assertGreaterEqual(row[5], 0.0)
            self.assertEqual(json.loads(row[6]), ["missing_authoritative_source"])


if __name__ == "__main__":
    unittest.main()
