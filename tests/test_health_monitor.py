from __future__ import annotations

import json
import os
import tempfile
import time
import unittest

from asterion_core.monitoring import (
    collect_degrade_status,
    collect_queue_health,
    collect_quote_health,
    collect_system_health,
    collect_ws_health,
)
from asterion_core.storage.write_queue import WriteQueueConfig, enqueue_task, init_queue, mark_task_failed, mark_task_succeeded


class _Quote:
    def __init__(self, last_updated_ms: int) -> None:
        self.last_updated_ms = last_updated_ms


class _StateStore:
    def __init__(self, now_ms: int) -> None:
        self._ws_delay_samples_ms = [50, 100, 150]
        self.reconnect_count_1h = 2
        self.latest_quote_by_market_token = {
            "m1:t1": _Quote(now_ms - 100),
            "m2:t2": _Quote(now_ms - 10_000),
        }


class HealthMonitorTest(unittest.TestCase):
    def test_collect_ws_health_and_quote_health(self) -> None:
        now_ms = int(time.time() * 1000)
        state_store = _StateStore(now_ms)

        ws = collect_ws_health(state_store)
        quote = collect_quote_health(state_store, stale_threshold_ms=5_000)

        self.assertTrue(ws.connected)
        self.assertEqual(ws.reconnect_count_1h, 2)
        self.assertEqual(quote.active_markets, 2)
        self.assertEqual(quote.stale_markets, 1)

    def test_collect_queue_health_and_degrade_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = os.path.join(tmpdir, "write_queue.sqlite")
            flag_path = os.path.join(tmpdir, "watch_only.json")
            cfg = WriteQueueConfig(path=queue_path)
            init_queue(cfg)

            task_id = enqueue_task(cfg, task_type="UPSERT_ROWS_V1", payload={"table": "x"})
            succeeded = enqueue_task(cfg, task_type="UPSERT_ROWS_V1", payload={"table": "y"})
            dead = enqueue_task(cfg, task_type="UPSERT_ROWS_V1", payload={"table": "z"}, max_attempts=1)
            mark_task_succeeded(cfg, task_id=succeeded)
            mark_task_failed(cfg, task_id=dead, error_message="boom")

            with open(flag_path, "w", encoding="utf-8") as handle:
                json.dump({"reason": "degraded", "since_ts_ms": 123, "watch_only": True}, handle)

            queue = collect_queue_health(queue_path)
            degrade = collect_degrade_status(flag_path)

            self.assertEqual(queue.pending_tasks, 1)
            self.assertGreaterEqual(queue.write_rate_per_min, 0.0)
            self.assertEqual(queue.dead_tasks_1h, 1)
            self.assertTrue(degrade.active)
            self.assertTrue(degrade.watch_only)
            self.assertEqual(degrade.reason, "degraded")

            system = collect_system_health(_StateStore(int(time.time() * 1000)), queue_path, flag_path, "unused.duckdb")
            self.assertTrue(system.degrade_status.active)


if __name__ == "__main__":
    unittest.main()
