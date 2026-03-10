from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.scout import (
    discover_weather_markets,
    enqueue_weather_market_upserts,
    normalize_weather_market,
    run_weather_market_discovery,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class _FakeClient:
    def __init__(self, pages: list[list[dict]]) -> None:
        self._pages = pages
        self.calls: list[dict] = []

    def get_json(self, url: str, *, context: dict) -> dict:
        self.calls.append({"url": url, "context": context})
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


class WeatherMarketDiscoveryTest(unittest.TestCase):
    def test_normalize_weather_market_extracts_structured_fields(self) -> None:
        market = normalize_weather_market(_raw_weather_market())
        assert market is not None
        self.assertEqual(market.market_id, "mkt_weather_1")
        self.assertEqual(market.condition_id, "cond_weather_1")
        self.assertEqual(market.event_id, "evt_weather_1")
        self.assertEqual(market.tags, ["Weather", "Temperature"])
        self.assertEqual(market.outcomes, ["Yes", "No"])
        self.assertEqual(market.token_ids, ["tok_yes", "tok_no"])
        self.assertEqual(market.status, "active")

    def test_discover_weather_markets_filters_non_weather_and_dedupes(self) -> None:
        pages = [
            [
                _raw_weather_market(),
                {
                    "id": "mkt_non_weather",
                    "conditionId": "cond_non_weather",
                    "question": "Will BTC be above 100k?",
                    "tags": ["Crypto"],
                    "active": True,
                },
                dict(_raw_weather_market()),
            ]
        ]
        client = _FakeClient(pages)
        markets = discover_weather_markets(
            base_url="https://gamma.example",
            markets_endpoint="/markets",
            page_limit=100,
            max_pages=2,
            sleep_s=0.0,
            active_only=True,
            closed=False,
            archived=False,
            client=client,
        )
        self.assertEqual(len(markets), 1)
        self.assertEqual(markets[0].market_id, "mkt_weather_1")


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for market discovery persistence tests")
class WeatherMarketPersistenceTest(unittest.TestCase):
    def test_run_discovery_and_persist_weather_markets(self) -> None:
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

            client = _FakeClient([[_raw_weather_market()]])
            queue_cfg = WriteQueueConfig(path=queue_path)
            result = run_weather_market_discovery(
                base_url="https://gamma.example",
                markets_endpoint="/markets",
                page_limit=100,
                max_pages=2,
                sleep_s=0.0,
                active_only=True,
                closed=False,
                archived=False,
                client=client,
                queue_cfg=queue_cfg,
                run_id="run_weather_discovery",
            )

            self.assertEqual(result.discovered_count, 1)
            self.assertIsNotNone(result.task_id)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": "weather.weather_markets",
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
                    SELECT market_id, condition_id, title, tags_json, outcomes_json, token_ids_json
                    FROM weather.weather_markets
                    WHERE market_id = 'mkt_weather_1'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(row[0], "mkt_weather_1")
            self.assertEqual(row[1], "cond_weather_1")
            self.assertIn("New York City", row[2])
            self.assertEqual(json.loads(row[3]), ["Weather", "Temperature"])
            self.assertEqual(json.loads(row[4]), ["Yes", "No"])
            self.assertEqual(json.loads(row[5]), ["tok_yes", "tok_no"])

    def test_enqueue_weather_market_upserts_noops_on_empty_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_cfg = WriteQueueConfig(path=os.path.join(tmpdir, "queue.sqlite"))
            task_id = enqueue_weather_market_upserts(queue_cfg, markets=[], run_id="empty")
            self.assertIsNone(task_id)


if __name__ == "__main__":
    unittest.main()
