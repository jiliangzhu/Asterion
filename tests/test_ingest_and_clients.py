from __future__ import annotations

import tempfile
import unittest
import sqlite3
from pathlib import Path

from asterion_core.clients.data_api import fetch_all_pages
from asterion_core.clients.gamma import extract_event_id, infer_condition_id, scan_gamma_markets
from asterion_core.ingest.bronze import BronzeJsonlRollingWriter
from asterion_core.ws.ws_subscribe import (
    collect_token_ids,
    load_token_ids_from_market_table,
)
from asterion_core.ws.ws_agg_v3 import QuoteStateRow, aggregate_quote_minute


class _FakeClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get_json(self, url, context=None):
        self.calls.append((url, context))
        if not self._responses:
            return []
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class BronzeWriterTest(unittest.TestCase):
    def test_bronze_writer_rolls_and_finalizes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = BronzeJsonlRollingWriter(root_dir=tmpdir, subdir="gamma", part_prefix="markets")
            writer.write({"a": 1}, ts_ms=0)
            writer.write({"a": 2}, ts_ms=1_000)
            writer.write({"a": 3}, ts_ms=60_000)
            writer.close()

            files = sorted(Path(tmpdir).glob("gamma/date=*/minute=*/*.jsonl"))
            self.assertEqual(len(files), 2)
            self.assertIn('"a":1', files[0].read_text(encoding="utf-8"))
            self.assertIn('"a":3', files[1].read_text(encoding="utf-8"))


class GammaClientTest(unittest.TestCase):
    def test_infer_condition_id(self) -> None:
        self.assertEqual(infer_condition_id({"conditionId": "abc"}), "abc")
        self.assertEqual(infer_condition_id({"condition_id": "def"}), "def")
        self.assertIsNone(infer_condition_id({"x": "y"}))

    def test_extract_event_id(self) -> None:
        self.assertEqual(extract_event_id({"event_id": "evt1"}), "evt1")
        self.assertEqual(extract_event_id({"id": 12}), "12")

    def test_scan_gamma_markets_filters_to_universe(self) -> None:
        client = _FakeClient(
            [
                {
                    "markets": [
                        {"conditionId": "keep", "events": [{"id": "evt-1"}]},
                        {"conditionId": "skip", "events": [{"id": "evt-2"}]},
                    ]
                },
                [],
            ]
        )
        found, raw_events = scan_gamma_markets(
            base_url="https://gamma.example",
            markets_endpoint="/markets",
            page_limit=100,
            max_pages=2,
            sleep_s=0,
            active_only=True,
            closed=None,
            archived=None,
            universe_ids={"keep"},
            client=client,
        )
        self.assertEqual(set(found.keys()), {"keep"})
        self.assertEqual(set(raw_events.keys()), {"evt-1"})


class DataApiClientTest(unittest.TestCase):
    def test_fetch_all_pages_uses_fallback_market_param(self) -> None:
        client = _FakeClient(
            [
                RuntimeError("first param rejected"),
                {"items": [{"id": "x", "ts": 100}, {"id": "y", "ts": 80}]},
                {"items": []},
            ]
        )

        def timestamp_fn(item, _keys):
            return int(item["ts"])

        items, max_seen = fetch_all_pages(
            base_url="https://data.example",
            endpoint="/fills",
            market_param="condition_id",
            market_id="m1",
            limit=2,
            max_pages=2,
            sleep_s=0,
            watermark_ms=90,
            since_ms=None,
            extra_params={},
            param_candidates=["market_id"],
            client=client,
            timestamp_fn=timestamp_fn,
            require_timestamp=True,
            timestamp_keys=["ts"],
        )
        self.assertEqual([item["id"] for item in items], ["x"])
        self.assertEqual(max_seen, 100)


class WsSubscribeTest(unittest.TestCase):
    def test_collect_token_ids_handles_nested_raw_and_outcomes(self) -> None:
        rows = [
            {
                "raw": {
                    "markets": [{"tokenId": "123"}, {"clobTokenIds": ["456", "789"]}],
                },
                "outcomes": '[{"token_id":"999"}]',
            },
            {
                "raw_json": '"{\\"nested\\": {\\"clobTokenId\\": \\"321\\"}}"',
            },
        ]
        self.assertEqual(
            collect_token_ids(rows),
            ["123", "321", "456", "789", "999"],
        )

    def test_load_token_ids_from_market_table_filters_tradable_and_market(self) -> None:
        con = sqlite3.connect(":memory:")
        con.execute(
            """
            CREATE TABLE market_capabilities (
                token_id TEXT,
                market_id TEXT,
                condition_id TEXT,
                tradable BOOLEAN
            )
            """
        )
        con.executemany(
            "INSERT INTO market_capabilities VALUES (?, ?, ?, ?)",
            [
                ("tok-1", "m1", "c1", True),
                ("tok-2", "m1", "c1", False),
                ("tok-3", "m2", "c2", True),
            ],
        )

        token_ids = load_token_ids_from_market_table(
            con,
            table_name="market_capabilities",
            token_id_column="token_id",
            market_id_column="market_id",
            condition_id_column="condition_id",
            tradable_column="tradable",
            market_ids=["m1"],
            tradable_only=True,
        )
        self.assertEqual(token_ids, ["tok-1"])


class WsAggTest(unittest.TestCase):
    def test_aggregate_quote_minute_carries_forward_and_updates_state(self) -> None:
        result = aggregate_quote_minute(
            minute_ts_ms=60_000,
            prior_state=[
                QuoteStateRow(
                    market_id="m0",
                    token_id="t0",
                    best_bid=0.40,
                    best_ask=0.60,
                    last_received_at_ms=50_000,
                )
            ],
            assets_total=3,
            events=[
                {
                    "market_id": "m1",
                    "token_id": "t1",
                    "received_at_ms": 61_000,
                    "timestamp_ms": 60_500,
                    "best_bid": 0.45,
                    "best_ask": 0.55,
                },
                {
                    "market_id": "m1",
                    "token_id": "t1",
                    "received_at_ms": 62_000,
                    "timestamp_ms": 61_000,
                    "best_bid": 0.46,
                    "best_ask": 0.56,
                },
            ],
        )

        self.assertEqual(len(result.bbo_rows), 2)
        self.assertEqual(result.coverage_row.assets_seen_minute, 1)
        self.assertEqual(result.coverage_row.assets_seen, 2)
        self.assertAlmostEqual(result.coverage_row.ws_coverage or 0.0, 2 / 3)
        latest = [row for row in result.bbo_rows if row.token_id == "t1"][0]
        self.assertEqual(latest.updates_count, 2)
        self.assertAlmostEqual(latest.best_bid or 0.0, 0.46)
        self.assertAlmostEqual(latest.best_ask or 0.0, 0.56)

    def test_aggregate_quote_minute_derives_bbo_from_raw_levels(self) -> None:
        result = aggregate_quote_minute(
            minute_ts_ms=120_000,
            prior_state=[],
            assets_total=None,
            events=[
                {
                    "market_id": "m2",
                    "asset_id": "t2",
                    "received_at_ms": 121_000,
                    "timestamp_ms": 120_000,
                    "raw": {
                        "bids": [{"price": "0.41"}, {"price": "0.39"}],
                        "asks": [{"price": "0.59"}, {"price": "0.61"}],
                    },
                }
            ],
        )

        self.assertEqual(len(result.bbo_rows), 1)
        row = result.bbo_rows[0]
        self.assertAlmostEqual(row.best_bid or 0.0, 0.41)
        self.assertAlmostEqual(row.best_ask or 0.0, 0.59)
        self.assertAlmostEqual(row.mid or 0.0, 0.50)


if __name__ == "__main__":
    unittest.main()
