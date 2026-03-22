from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.ui import (
    DEFAULT_UI_DB_REPLICA_PATH,
    default_ui_db_replica_path,
    default_ui_replica_meta_path,
    load_ui_replica_meta,
    refresh_ui_db_replica_once,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class UiDbReplicaHelpersTest(unittest.TestCase):
    def test_default_paths_use_asterion_names(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            self.assertEqual(default_ui_db_replica_path(), DEFAULT_UI_DB_REPLICA_PATH)
            self.assertEqual(
                default_ui_replica_meta_path(replica_db_path="data/ui/asterion_ui.duckdb"),
                "data/ui/asterion_ui.meta.json",
            )

    def test_load_meta_returns_none_for_missing_or_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            meta_path = Path(tmpdir) / "replica.meta.json"
            self.assertIsNone(load_ui_replica_meta(str(meta_path)))
            meta_path.write_text("{bad json", encoding="utf-8")
            self.assertIsNone(load_ui_replica_meta(str(meta_path)))

    def test_refresh_missing_source_emits_failure_meta(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = os.path.join(tmpdir, "missing.duckdb")
            dst_path = os.path.join(tmpdir, "ui.duckdb")
            meta_path = os.path.join(tmpdir, "ui.meta.json")

            result = refresh_ui_db_replica_once(
                src_db_path=src_path,
                dst_db_path=dst_path,
                meta_path=meta_path,
                refresh_interval_s=5.0,
            )

            self.assertFalse(result.ok)
            self.assertIn("source db not found", result.error or "")
            meta = load_ui_replica_meta(meta_path)
            assert meta is not None
            self.assertEqual(meta["consecutive_failures"], 1)
            self.assertEqual(meta["replica_db_path"], dst_path)
            self.assertEqual(meta["source_db_path"], src_path)
            self.assertEqual(meta["refresh_interval_s"], 5.0)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for ui replica tests")
class UiDbReplicaDuckDBTest(unittest.TestCase):
    def test_refresh_copies_and_validates_replica(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = os.path.join(tmpdir, "source.duckdb")
            dst_path = os.path.join(tmpdir, "ui.duckdb")
            meta_path = os.path.join(tmpdir, "ui.meta.json")

            con = duckdb.connect(src_path)
            try:
                con.execute("CREATE TABLE sample (id INTEGER, value VARCHAR)")
                con.execute("INSERT INTO sample VALUES (1, 'hello')")
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_REPLICA_COPY_MODE": "copy"}, clear=False):
                result = refresh_ui_db_replica_once(
                    src_db_path=src_path,
                    dst_db_path=dst_path,
                    meta_path=meta_path,
                    refresh_interval_s=2.5,
                )

            self.assertTrue(result.ok, result.error)
            self.assertTrue(os.path.exists(dst_path))
            meta = load_ui_replica_meta(meta_path)
            assert meta is not None
            self.assertEqual(meta["consecutive_failures"], 0)
            self.assertIsNone(meta["last_error"])
            self.assertEqual(meta["refresh_interval_s"], 2.5)

            read_con = duckdb.connect(dst_path, read_only=True)
            try:
                row = read_con.execute("SELECT id, value FROM sample").fetchone()
            finally:
                read_con.close()
            self.assertEqual(tuple(row), (1, "hello"))

    def test_refresh_skips_recopy_when_source_unchanged(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = os.path.join(tmpdir, "source.duckdb")
            dst_path = os.path.join(tmpdir, "ui.duckdb")
            meta_path = os.path.join(tmpdir, "ui.meta.json")

            con = duckdb.connect(src_path)
            try:
                con.execute("CREATE TABLE sample (id INTEGER)")
                con.execute("INSERT INTO sample VALUES (1)")
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_REPLICA_COPY_MODE": "copy"}, clear=False):
                first = refresh_ui_db_replica_once(
                    src_db_path=src_path,
                    dst_db_path=dst_path,
                    meta_path=meta_path,
                )
                first_stat = os.stat(dst_path)
                second = refresh_ui_db_replica_once(
                    src_db_path=src_path,
                    dst_db_path=dst_path,
                    meta_path=meta_path,
                )
                second_stat = os.stat(dst_path)

            self.assertTrue(first.ok, first.error)
            self.assertTrue(second.ok, second.error)
            self.assertEqual(int(first_stat.st_mtime * 1000), int(second_stat.st_mtime * 1000))
            meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
            self.assertEqual(meta["consecutive_failures"], 0)
            self.assertIsNone(meta["last_error"])

    def test_refresh_success_meta_matches_operator_surface_refresh_seam(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            src_path = os.path.join(tmpdir, "source.duckdb")
            dst_path = os.path.join(tmpdir, "ui.duckdb")
            meta_path = os.path.join(tmpdir, "ui.meta.json")

            con = duckdb.connect(src_path)
            try:
                con.execute("CREATE TABLE sample (id INTEGER)")
                con.execute("INSERT INTO sample VALUES (1)")
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_REPLICA_COPY_MODE": "copy"}, clear=False):
                result = refresh_ui_db_replica_once(
                    src_db_path=src_path,
                    dst_db_path=dst_path,
                    meta_path=meta_path,
                    refresh_interval_s=7.5,
                )

            self.assertTrue(result.ok, result.error)
            meta = load_ui_replica_meta(meta_path)
            assert meta is not None
            self.assertEqual(meta["source_db_path"], src_path)
            self.assertEqual(meta["replica_db_path"], dst_path)
            self.assertEqual(meta["refresh_interval_s"], 7.5)
            self.assertEqual(result.src_db_path, src_path)
            self.assertEqual(result.dst_db_path, dst_path)


if __name__ == "__main__":
    unittest.main()
