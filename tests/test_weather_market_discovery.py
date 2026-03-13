from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
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


def _raw_weather_market_with_token_outcomes_only() -> dict:
    market = _raw_weather_market()
    market.pop("outcomes", None)
    market["tokens"] = [
        {"token_id": "tok_yes", "outcome": "Yes"},
        {"token_id": "tok_no", "outcome": "No"},
    ]
    return market


def _raw_recent_open_weather_market() -> dict:
    market = _raw_weather_market()
    market["id"] = "mkt_weather_recent"
    market["conditionId"] = "cond_weather_recent"
    market["question"] = "Will the high temperature in New York City on March 15, 2026 be 50-59°F?"
    market["closeTime"] = "2026-03-15T23:59:59Z"
    market["endDate"] = "2026-03-15T23:59:59Z"
    market["createdAt"] = "2026-03-10T00:00:00Z"
    return market


def _raw_event_payload_with_nested_weather_markets() -> dict:
    return {
        "id": "evt_weather_daily_seattle",
        "title": "Highest temperature in Seattle on March 13?",
        "slug": "highest-temperature-in-seattle-on-march-13-2026",
        "category": "Weather",
        "subcategory": "Temperature",
        "tags": ["Weather"],
        "markets": [
            {
                "id": "mkt_seattle_1",
                "question": "Will the highest temperature in Seattle be between 36-37°F on March 13?",
                "conditionId": "cond_seattle_1",
                "slug": "highest-temperature-in-seattle-on-march-13-2026-36to37f",
                "active": True,
                "closed": False,
                "archived": False,
                "acceptingOrders": True,
                "enableOrderBook": True,
                "outcomes": "[\"Yes\", \"No\"]",
                "clobTokenIds": "[\"tok_yes\", \"tok_no\"]",
                "closeTime": "2026-03-13T12:00:00Z",
                "endDate": "2026-03-13T12:00:00Z",
                "createdAt": "2026-03-12T00:00:00Z",
            }
        ],
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

    def test_normalize_weather_market_falls_back_to_token_outcomes(self) -> None:
        market = normalize_weather_market(_raw_weather_market_with_token_outcomes_only())
        assert market is not None
        self.assertEqual(market.outcomes, ["Yes", "No"])
        self.assertEqual(market.token_ids, ["tok_yes", "tok_no"])

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

    def test_discover_weather_markets_filters_to_open_recent_window(self) -> None:
        recent = _raw_recent_open_weather_market()
        stale = _raw_weather_market()
        stale["active"] = False
        stale["closed"] = True
        stale["closeTime"] = "2021-11-02T23:59:59Z"
        stale["endDate"] = "2021-11-02T23:59:59Z"
        far_future = _raw_weather_market()
        far_future["id"] = "mkt_weather_far_future"
        far_future["conditionId"] = "cond_weather_far_future"
        far_future["question"] = "Will the high temperature in New York City on April 30, 2026 be 50-59°F?"
        far_future["closeTime"] = "2026-04-30T23:59:59Z"
        far_future["endDate"] = "2026-04-30T23:59:59Z"
        client = _FakeClient([[recent, stale, far_future]])
        markets = discover_weather_markets(
            base_url="https://gamma.example",
            markets_endpoint="/markets",
            page_limit=100,
            max_pages=1,
            sleep_s=0.0,
            active_only=True,
            closed=False,
            archived=False,
            recent_within_days=14,
            asof=datetime(2026, 3, 12, tzinfo=timezone.utc),
            client=client,
        )
        self.assertEqual([market.market_id for market in markets], ["mkt_weather_recent"])

    def test_discover_weather_markets_flattens_event_payload(self) -> None:
        client = _FakeClient([[ _raw_event_payload_with_nested_weather_markets() ]])
        markets = discover_weather_markets(
            base_url="https://gamma.example",
            markets_endpoint="/events",
            page_limit=100,
            max_pages=1,
            sleep_s=0.0,
            active_only=True,
            closed=False,
            archived=False,
            recent_within_days=14,
            tag_slug="weather",
            asof=datetime(2026, 3, 12, tzinfo=timezone.utc),
            client=client,
        )
        self.assertEqual(len(markets), 1)
        self.assertEqual(markets[0].market_id, "mkt_seattle_1")
        self.assertEqual(markets[0].event_id, "evt_weather_daily_seattle")
        self.assertEqual(markets[0].title, "Will the highest temperature in Seattle be between 36-37°F on March 13?")


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
