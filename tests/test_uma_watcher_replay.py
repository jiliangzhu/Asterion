from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import ProposalStatus
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.resolution import (
    BlockWatermarkRecord,
    FallbackRpcPool,
    PolygonRealtimeWatcherRpcClient,
    RpcEndpointConfig,
    UMAEvent,
    build_backfill_request,
    evaluate_continuity,
    enqueue_uma_replay_writes,
    load_block_watermark,
    load_last_processed_block,
    load_processed_event_ids,
    load_uma_proposals,
    persist_watcher_backfill,
    replay_uma_events,
    run_watcher_backfill,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


def _proposal_created() -> UMAEvent:
    return UMAEvent(
        tx_hash="0xaaa",
        log_index=1,
        block_number=100,
        event_type="proposal_created",
        proposal_id="prop_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        proposer="0xproposer",
        proposed_outcome="YES",
        proposal_bond=100.0,
        dispute_bond=None,
        proposal_timestamp=datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc),
        on_chain_settled_at=None,
        safe_redeem_after=None,
        human_review_required=False,
    )


def _proposal_settled() -> UMAEvent:
    return UMAEvent(
        tx_hash="0xbbb",
        log_index=2,
        block_number=110,
        event_type="proposal_settled",
        proposal_id="prop_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        proposer=None,
        proposed_outcome=None,
        proposal_bond=None,
        dispute_bond=None,
        proposal_timestamp=None,
        on_chain_settled_at=datetime(2026, 3, 9, 1, 0, tzinfo=timezone.utc),
        safe_redeem_after=datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc),
        human_review_required=True,
    )


class _StaticRpcClient:
    def __init__(
        self,
        *,
        finalized_block: int | None = None,
        events: list[UMAEvent] | None = None,
        fail_finalized: bool = False,
        fail_events: bool = False,
    ) -> None:
        self._finalized_block = finalized_block
        self._events = list(events or [])
        self._fail_finalized = fail_finalized
        self._fail_events = fail_events

    def get_finalized_block_number(self) -> int:
        if self._fail_finalized:
            raise TimeoutError("finalized read failed")
        if self._finalized_block is None:
            raise ValueError("missing finalized block")
        return self._finalized_block

    def get_events(self, from_block: int, to_block: int) -> list[UMAEvent]:
        if self._fail_events:
            raise TimeoutError("events read failed")
        return [item for item in self._events if from_block <= item.block_number <= to_block]

    def get_proposal_state(self, *, proposal_id=None, tx_hash=None, condition_id=None):
        return None


def _rpc_pool(*clients: _StaticRpcClient) -> FallbackRpcPool:
    endpoints = []
    for index, client in enumerate(clients, start=1):
        endpoints.append(
            (
                RpcEndpointConfig(
                    name=f"rpc{index}",
                    url=f"https://rpc{index}.example.test",
                    priority=index,
                    timeout_seconds=2.0,
                ),
                client,
            )
        )
    return FallbackRpcPool(endpoints)


class UMAWatcherReplayUnitTest(unittest.TestCase):
    def test_replay_is_idempotent_and_records_old_new_status(self) -> None:
        proposals, transitions, processed = replay_uma_events(events=[_proposal_created(), _proposal_settled()])
        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].status, ProposalStatus.SETTLED)
        self.assertEqual(len(transitions), 2)
        self.assertEqual(transitions[0].old_status, ProposalStatus.PENDING)
        self.assertEqual(transitions[0].new_status, ProposalStatus.PROPOSED)
        self.assertEqual(transitions[1].old_status, ProposalStatus.PROPOSED)
        self.assertEqual(transitions[1].new_status, ProposalStatus.SETTLED)
        self.assertEqual(len(processed), 2)

        proposals2, transitions2, processed2 = replay_uma_events(
            events=[_proposal_created(), _proposal_settled()],
            existing_proposals={item.proposal_id: item for item in proposals},
            processed_event_ids=set(processed),
        )
        self.assertEqual(len(proposals2), 1)
        self.assertEqual(len(transitions2), 0)
        self.assertEqual(processed2, [])

    def test_build_backfill_request_starts_from_last_finalized_plus_one(self) -> None:
        with patch(
            "domains.weather.resolution.backfill.load_block_watermark",
            return_value=BlockWatermarkRecord(chain_id=137, last_processed_block=110, last_finalized_block=110),
        ):
            request = build_backfill_request(
                object(),
                chain_id=137,
                finalized_block=120,
                replay_reason="restart",
                max_block_span=5,
            )
        self.assertEqual(request.from_block, 111)
        self.assertEqual(request.to_block, 115)

    def test_build_backfill_request_without_watermark_uses_recent_span(self) -> None:
        with patch("domains.weather.resolution.backfill.load_block_watermark", return_value=None):
            request = build_backfill_request(
                object(),
                chain_id=137,
                finalized_block=120,
                replay_reason="realtime_only",
                max_block_span=5,
            )
        self.assertEqual(request.from_block, 116)
        self.assertEqual(request.to_block, 120)

    def test_realtime_rpc_retries_rate_limited_requests(self) -> None:
        class _Response:
            def __init__(self, status_code: int, payload: dict[str, object]) -> None:
                self.status_code = status_code
                self._payload = payload
                self.request = object()

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    import httpx

                    raise httpx.HTTPStatusError("rate limited", request=self.request, response=self)

            def json(self) -> dict[str, object]:
                return self._payload

        rpc = PolygonRealtimeWatcherRpcClient(rpc_url="https://rpc.example.test", min_request_interval_seconds=0.0)
        rpc._max_retries = 1
        calls = {"count": 0}

        def _fake_post(url: str, *, headers=None, timeout=None, json=None) -> _Response:
            del url, headers, timeout, json
            calls["count"] += 1
            if calls["count"] == 1:
                return _Response(429, {"error": "rate limited"})
            return _Response(200, {"result": "0x2a"})

        with patch("domains.weather.resolution.realtime_rpc.httpx.post", side_effect=_fake_post):
            result = rpc._rpc("eth_blockNumber", [])
        self.assertEqual(result, "0x2a")

    def test_realtime_rpc_uses_seeded_market_refs_without_remote_lookup(self) -> None:
        rpc = PolygonRealtimeWatcherRpcClient(rpc_url="https://rpc.example.test", allow_remote_market_lookup=False)
        rpc.seed_market_refs(
            {
                "0xquestion": {
                    "market_id": "1701747",
                    "condition_id": "0xcondition",
                }
            }
        )

        with patch.object(rpc._client, "get", side_effect=AssertionError("unexpected remote lookup")):
            ref = rpc._load_market_by_question_id("0xquestion")
            missing = rpc._load_market_by_question_id("0xmissing")

        self.assertIsNotNone(ref)
        self.assertEqual(ref.market_id, "1701747")
        self.assertEqual(ref.condition_id, "0xcondition")
        self.assertIsNone(missing)

    def test_primary_rpc_failure_falls_back_to_secondary(self) -> None:
        pool = _rpc_pool(
            _StaticRpcClient(finalized_block=110, events=[_proposal_created()], fail_finalized=True),
            _StaticRpcClient(finalized_block=110, events=[_proposal_created()]),
        )

        finalized_block, trace = pool.get_finalized_block_number()
        events, events_trace = pool.get_events(100, 110)

        self.assertEqual(finalized_block, 110)
        self.assertEqual(trace.selected_endpoint, "rpc2")
        self.assertTrue(trace.fallback_used)
        self.assertEqual(events_trace.selected_endpoint, "rpc1")
        self.assertEqual(len(events), 1)

    def test_rpc_trace_records_selected_endpoint_and_errors(self) -> None:
        pool = _rpc_pool(
            _StaticRpcClient(finalized_block=110, fail_events=True),
            _StaticRpcClient(finalized_block=110, events=[_proposal_created()]),
        )

        events, trace = pool.get_events(100, 110)

        self.assertEqual(len(events), 1)
        self.assertEqual(trace.selected_endpoint, "rpc2")
        self.assertEqual(trace.attempted_endpoints, ["rpc1", "rpc2"])
        self.assertEqual(len(trace.errors), 1)
        self.assertIn("rpc1", trace.errors[0])

    def test_continuity_check_marks_block_gap_when_range_skips_watermark(self) -> None:
        continuity = evaluate_continuity(
            chain_id=137,
            from_block=103,
            to_block=110,
            watermark=BlockWatermarkRecord(chain_id=137, last_processed_block=100, last_finalized_block=100),
            events=[_proposal_settled()],
            processed_event_ids=set(),
        )

        self.assertEqual(continuity.check.status, "GAP_DETECTED")
        self.assertTrue(any(item.gap_type == "BLOCK_GAP" for item in continuity.gaps))

    def test_continuity_check_marks_duplicate_range_when_replaying_old_blocks(self) -> None:
        continuity = evaluate_continuity(
            chain_id=137,
            from_block=100,
            to_block=105,
            watermark=BlockWatermarkRecord(chain_id=137, last_processed_block=110, last_finalized_block=110),
            events=[_proposal_created()],
            processed_event_ids=set(),
        )

        self.assertEqual(continuity.check.status, "GAP_DETECTED")
        self.assertTrue(any(item.gap_type == "DUPLICATE_RANGE" for item in continuity.gaps))

    def test_continuity_check_marks_rpc_incomplete_when_all_endpoints_fail(self) -> None:
        with patch("domains.weather.resolution.backfill.load_block_watermark", return_value=None):
            result = run_watcher_backfill(
                object(),
                _rpc_pool(
                    _StaticRpcClient(finalized_block=110, fail_finalized=True),
                    _StaticRpcClient(finalized_block=110, fail_finalized=True),
                ),
                chain_id=137,
                replay_reason="rpc_failure",
            )

        assert result.continuity is not None
        self.assertEqual(result.continuity.check.status, "RPC_INCOMPLETE")
        self.assertEqual(result.processed_events_written, 0)
        self.assertIsNone(result.next_last_processed_block)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for UMA watcher replay tests")
class UMAWatcherReplayDuckDBTest(unittest.TestCase):
    def test_replay_persists_processed_events_and_watermark(self) -> None:
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

            proposals, transitions, _ = replay_uma_events(events=[_proposal_created(), _proposal_settled()])
            queue_cfg = WriteQueueConfig(path=queue_path)
            task_ids = enqueue_uma_replay_writes(
                queue_cfg,
                chain_id=137,
                proposals=proposals,
                transitions=transitions,
                processed_events=[_proposal_created(), _proposal_settled()],
                last_processed_block=110,
                last_finalized_block=110,
                run_id="run_uma_replay",
            )
            self.assertGreaterEqual(len(task_ids), 4)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": ",".join(
                        [
                            "resolution.uma_proposals",
                            "resolution.proposal_state_transitions",
                            "resolution.processed_uma_events",
                            "resolution.block_watermarks",
                        ]
                    ),
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
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
                        stored_proposals = load_uma_proposals(con)
                        processed_ids = load_processed_event_ids(con)
                        watermark = load_block_watermark(con, chain_id=137)
                    finally:
                        con.close()

            self.assertEqual(stored_proposals["prop_1"].status, ProposalStatus.SETTLED)
            self.assertEqual(len(processed_ids), 2)
            assert watermark is not None
            self.assertEqual(watermark.last_processed_block, 110)
            self.assertEqual(watermark.last_finalized_block, 110)

            proposals2, transitions2, processed2 = replay_uma_events(
                events=[_proposal_created(), _proposal_settled()],
                existing_proposals=stored_proposals,
                processed_event_ids=processed_ids,
            )
            self.assertEqual(len(proposals2), 1)
            self.assertEqual(len(transitions2), 0)
            self.assertEqual(processed2, [])

    def test_backfill_replays_block_range_and_advances_watermark(self) -> None:
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

            pool = _rpc_pool(_StaticRpcClient(finalized_block=110, events=[_proposal_created(), _proposal_settled()]))
            queue_cfg = WriteQueueConfig(path=queue_path)

            from asterion_core.storage.database import DuckDBConfig, connect_duckdb

            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict(os.environ, reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    result = run_watcher_backfill(con, pool, chain_id=137, replay_reason="backfill")
                finally:
                    con.close()

            task_ids = persist_watcher_backfill(queue_cfg, result)
            self.assertGreaterEqual(len(task_ids), 5)

            allow = ",".join(
                [
                    "resolution.uma_proposals",
                    "resolution.proposal_state_transitions",
                    "resolution.processed_uma_events",
                    "resolution.block_watermarks",
                    "resolution.watcher_continuity_checks",
                    "resolution.watcher_continuity_gaps",
                ]
            )
            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow},
                clear=False,
            ):
                for _ in range(8):
                    if not process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                        break

            import duckdb

            con = duckdb.connect(db_path, read_only=True)
            try:
                proposal_row = con.execute(
                    "SELECT status FROM resolution.uma_proposals WHERE proposal_id = 'prop_1'"
                ).fetchone()
                watermark_row = con.execute(
                    "SELECT last_processed_block, last_finalized_block FROM resolution.block_watermarks WHERE chain_id = 137"
                ).fetchone()
                continuity_row = con.execute(
                    "SELECT status, gap_count FROM resolution.watcher_continuity_checks"
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(proposal_row[0], ProposalStatus.SETTLED.value)
            self.assertEqual(watermark_row[0], 110)
            self.assertEqual(watermark_row[1], 110)
            self.assertEqual(continuity_row[0], "OK")
            self.assertEqual(continuity_row[1], 0)

    def test_backfill_is_idempotent_for_processed_events(self) -> None:
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

            pool = _rpc_pool(_StaticRpcClient(finalized_block=110, events=[_proposal_created(), _proposal_settled()]))
            queue_cfg = WriteQueueConfig(path=queue_path)

            from asterion_core.storage.database import DuckDBConfig, connect_duckdb

            def _drain() -> None:
                allow = ",".join(
                    [
                        "resolution.uma_proposals",
                        "resolution.proposal_state_transitions",
                        "resolution.processed_uma_events",
                        "resolution.block_watermarks",
                        "resolution.watcher_continuity_checks",
                        "resolution.watcher_continuity_gaps",
                    ]
                )
                with patch.dict(
                    os.environ,
                    {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow},
                    clear=False,
                ):
                    for _ in range(8):
                        if not process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                            break

            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict(os.environ, reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    first = run_watcher_backfill(con, pool, chain_id=137, replay_reason="first")
                finally:
                    con.close()
            persist_watcher_backfill(queue_cfg, first)
            _drain()

            import duckdb

            con = duckdb.connect(db_path)
            try:
                con.execute(
                    """
                    UPDATE resolution.block_watermarks
                    SET last_processed_block = 99, last_finalized_block = 99
                    WHERE chain_id = 137
                    """
                )
            finally:
                con.close()

            with patch.dict(os.environ, reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    second = run_watcher_backfill(con, pool, chain_id=137, replay_reason="retry_same_range")
                finally:
                    con.close()

            self.assertEqual(second.transitions_written, 0)
            self.assertEqual(second.processed_events_written, 0)
            persist_watcher_backfill(queue_cfg, second)
            _drain()

            con = duckdb.connect(db_path, read_only=True)
            try:
                transitions_count = con.execute(
                    "SELECT COUNT(*) FROM resolution.proposal_state_transitions"
                ).fetchone()[0]
                processed_count = con.execute(
                    "SELECT COUNT(*) FROM resolution.processed_uma_events"
                ).fetchone()[0]
                last_processed = load_last_processed_block(con, chain_id=137)
            finally:
                con.close()

            self.assertEqual(transitions_count, 2)
            self.assertEqual(processed_count, 2)
            self.assertEqual(last_processed, 110)


if __name__ == "__main__":
    unittest.main()
