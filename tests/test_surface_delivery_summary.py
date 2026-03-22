from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui.ui_lite_db import _create_surface_delivery_summary


class SurfaceDeliverySummaryTest(unittest.TestCase):
    def test_surface_delivery_summary_materializes_delivery_states(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "surface_delivery.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.truth_source_checks (
                        check_id TEXT,
                        surface_id TEXT,
                        table_name TEXT,
                        check_status TEXT,
                        issues_json TEXT,
                        checked_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.truth_source_checks VALUES
                    ('home_check', 'home', 'ui.action_queue_summary', 'ok', '[]', TIMESTAMP '2026-03-21 11:00:00'),
                    ('markets_check', 'markets', 'ui.market_opportunity_summary', 'warn', '["fallback:runtime_db"]', TIMESTAMP '2026-03-21 11:05:00'),
                    ('execution_check', 'execution', 'ui.execution_science_summary', 'fail', '["table_missing:ui.execution_science_summary"]', TIMESTAMP '2026-03-21 11:10:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.action_queue_summary (
                        queue_item_id TEXT,
                        surface_delivery_status TEXT,
                        surface_fallback_origin TEXT,
                        surface_last_refresh_ts TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.action_queue_summary VALUES
                    ('queue_1', 'ok', NULL, TIMESTAMP '2026-03-21 11:00:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary (
                        market_id TEXT,
                        surface_delivery_status TEXT,
                        surface_fallback_origin TEXT,
                        surface_last_refresh_ts TIMESTAMP,
                        source_truth_status TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_1', 'degraded_source', 'runtime_db', TIMESTAMP '2026-03-21 11:05:00', 'fallback')
                    """
                )

                table_row_counts = {
                    "ui.action_queue_summary": 1,
                    "ui.market_opportunity_summary": 1,
                    "ui.execution_science_summary": 0,
                }
                _create_surface_delivery_summary(con, table_row_counts=table_row_counts)
                rows = con.execute(
                    """
                    SELECT surface_id, delivery_status, fallback_origin, truth_check_status, truth_check_issue_count
                    FROM ui.surface_delivery_summary
                    ORDER BY surface_id
                    """
                ).fetchall()
            finally:
                con.close()

        self.assertEqual(
            rows,
            [
                ("execution", "missing", None, "fail", 1),
                ("home", "ok", None, "ok", 0),
                ("markets", "degraded_source", "runtime_db", "warn", 1),
            ],
        )


if __name__ == "__main__":
    unittest.main()
