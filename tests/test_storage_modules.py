from __future__ import annotations

import importlib.util
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.storage.database import DuckDBConfig, connect_duckdb, meta_get_watermark_ms, meta_set_watermark_ms
from asterion_core.storage.determinism import canonical_json_dumps, stable_payload_sha256
from asterion_core.storage.os_queue import enqueue_update_rows_v1, enqueue_upsert_rows_v1
from asterion_core.storage.write_guard_audit import (
    count_write_guard_blocks_since,
    count_write_guard_write_attempts_since,
    record_write_guard_block,
)
from asterion_core.storage.write_queue import (
    WriteQueueConfig,
    claim_next_task,
    enqueue_task,
    get_task_statuses,
    mark_task_failed,
    mark_task_succeeded,
    retry_failed,
    retry_stale_running,
)
from asterion_core.storage.writerd import process_one

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class DeterminismTest(unittest.TestCase):
    def test_canonical_json_dumps_is_stable(self) -> None:
        left = canonical_json_dumps({"b": 2, "a": 1.23456789})
        right = canonical_json_dumps({"a": 1.23456789123, "b": 2})
        self.assertEqual(left, right)

    def test_stable_payload_sha256_is_stable(self) -> None:
        left = stable_payload_sha256({"x": [1, 2], "y": {"a": "b"}})
        right = stable_payload_sha256({"y": {"a": "b"}, "x": [1, 2]})
        self.assertEqual(left, right)


class WriteGuardAuditTest(unittest.TestCase):
    def test_record_and_count_write_guard_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "audit.sqlite")
            with patch.dict(os.environ, {"ASTERION_WRITE_GUARD_AUDIT_DB": db_path}, clear=False):
                record_write_guard_block(guard_mode="reader", reason="rejects SQL statement type: INSERT", statement="INSERT INTO t VALUES (1)")
                self.assertEqual(count_write_guard_blocks_since(since_ts_ms=0), 1)
                self.assertEqual(count_write_guard_write_attempts_since(since_ts_ms=0), 1)


class WriteQueueTest(unittest.TestCase):
    def test_enqueue_claim_and_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = WriteQueueConfig(path=os.path.join(tmpdir, "queue.sqlite"))
            task_id = enqueue_task(cfg, task_type="TEST", payload={"hello": "world"})
            task = claim_next_task(cfg)
            self.assertIsNotNone(task)
            assert task is not None
            self.assertEqual(task.task_id, task_id)
            mark_task_succeeded(cfg, task_id=task_id)
            statuses = get_task_statuses(cfg, task_ids=[task_id])
            self.assertEqual(statuses[task_id], "SUCCEEDED")

    def test_failed_task_can_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = WriteQueueConfig(path=os.path.join(tmpdir, "queue.sqlite"))
            task_id = enqueue_task(cfg, task_type="TEST", payload={"hello": "world"})
            claim_next_task(cfg)
            mark_task_failed(cfg, task_id=task_id, error_message="boom")
            self.assertEqual(retry_failed(cfg), 1)
            statuses = get_task_statuses(cfg, task_ids=[task_id])
            self.assertEqual(statuses[task_id], "PENDING")

    def test_stale_running_task_can_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = WriteQueueConfig(path=os.path.join(tmpdir, "queue.sqlite"))
            task_id = enqueue_task(cfg, task_type="TEST", payload={"hello": "world"})
            claim_next_task(cfg)
            time.sleep(0.01)
            self.assertEqual(retry_stale_running(cfg, stale_ms=1), 1)
            statuses = get_task_statuses(cfg, task_ids=[task_id])
            self.assertEqual(statuses[task_id], "PENDING")


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for database/writerd tests")
class DatabaseAndWriterdTest(unittest.TestCase):
    def _write_test_ddl(self, root: str) -> str:
        ddl_path = os.path.join(root, "schema.sql")
        Path(ddl_path).write_text(
            """
            CREATE SCHEMA IF NOT EXISTS meta;
            CREATE TABLE IF NOT EXISTS meta.ingest_runs (
                run_id TEXT,
                job_name TEXT,
                source TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status TEXT,
                rows_written BIGINT,
                error_message TEXT,
                params_json TEXT
            );
            CREATE TABLE IF NOT EXISTS meta.watermarks (
                source TEXT,
                endpoint TEXT,
                market_id TEXT,
                cursor_name TEXT,
                cursor_value TEXT,
                cursor_value_ms BIGINT,
                updated_at TIMESTAMP
            );
            CREATE SCHEMA IF NOT EXISTS dev_test;
            CREATE TABLE IF NOT EXISTS dev_test.rows (
                id TEXT PRIMARY KEY,
                value TEXT,
                updated_ts_ms BIGINT
            );
            """,
            encoding="utf-8",
        )
        return ddl_path

    def test_reader_guard_blocks_insert(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            ddl_path = self._write_test_ddl(tmpdir)
            env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
                "ASTERION_APPLY_SCHEMA": "1",
            }
            with patch.dict(os.environ, env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=ddl_path))
                con.close()

            env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict(os.environ, env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    with self.assertRaises(PermissionError):
                        con.execute("INSERT INTO dev_test.rows VALUES ('1', 'x', 1)")
                finally:
                    con.close()

    def test_meta_watermark_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            ddl_path = self._write_test_ddl(tmpdir)
            env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
                "ASTERION_APPLY_SCHEMA": "1",
            }
            with patch.dict(os.environ, env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=ddl_path))
                try:
                    meta_set_watermark_ms(con, source="gamma", endpoint="markets", market_id="m1", value_ms=123)
                    self.assertEqual(meta_get_watermark_ms(con, source="gamma", endpoint="markets", market_id="m1"), 123)
                finally:
                    con.close()

    def test_writerd_processes_upsert_and_update_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            ddl_path = self._write_test_ddl(tmpdir)
            queue_path = os.path.join(tmpdir, "queue.sqlite")
            env = {
                "ASTERION_DB_PATH": db_path,
                "ASTERION_WRITERD_ALLOWED_TABLES": "dev_test.rows",
            }
            qcfg = WriteQueueConfig(path=queue_path)
            with patch.dict(os.environ, env, clear=False):
                enqueue_upsert_rows_v1(
                    qcfg,
                    table="dev_test.rows",
                    pk_cols=["id"],
                    columns=["id", "value", "updated_ts_ms"],
                    rows=[["1", "hello", 1]],
                    run_id="run1",
                )
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=ddl_path, apply_schema=True))

                enqueue_update_rows_v1(
                    qcfg,
                    table="dev_test.rows",
                    pk_cols=["id"],
                    columns=["id", "value", "updated_ts_ms"],
                    rows=[["1", "world", 2]],
                    run_id="run2",
                )
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=ddl_path, apply_schema=False))

                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_STRICT_SINGLE_WRITER": "1",
                        "ASTERION_DB_ROLE": "reader",
                        "WRITERD": "0",
                    },
                    clear=False,
                ):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        row = con.execute("SELECT id, value, updated_ts_ms FROM dev_test.rows WHERE id = '1'").fetchone()
                    finally:
                        con.close()
                self.assertEqual(tuple(row), ("1", "world", 2))


if __name__ == "__main__":
    unittest.main()
