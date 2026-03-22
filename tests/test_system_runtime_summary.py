from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui.ui_lite_db import _create_system_runtime_summary


class SystemRuntimeSummaryTest(unittest.TestCase):
    def test_system_runtime_summary_aggregates_refresh_delivery_and_gate_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "system_runtime.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute("CREATE TABLE ui.surface_delivery_summary (surface_id TEXT, delivery_status TEXT)")
                con.execute(
                    """
                    INSERT INTO ui.surface_delivery_summary VALUES
                    ('home', 'ok'),
                    ('markets', 'degraded_source'),
                    ('execution', 'read_error')
                    """
                )
                con.execute("CREATE TABLE ui.phase_readiness_summary (go_decision TEXT)")
                con.execute("INSERT INTO ui.phase_readiness_summary VALUES ('GO')")
                con.execute("CREATE TABLE ui.market_opportunity_summary (market_id TEXT, source_truth_status TEXT)")
                con.execute("INSERT INTO ui.market_opportunity_summary VALUES ('mkt_1', 'fallback')")
                con.execute("CREATE TABLE ui.calibration_health_summary (hard_gate_market_count BIGINT)")
                con.execute("INSERT INTO ui.calibration_health_summary VALUES (2)")
                con.execute("CREATE TABLE ui.proposal_resolution_summary (effective_redeem_status TEXT)")
                con.execute(
                    """
                    INSERT INTO ui.proposal_resolution_summary VALUES
                    ('pending_operator_review'),
                    ('ready_for_redeem_review')
                    """
                )
                con.execute("ATTACH ':memory:' AS src")
                con.execute("CREATE SCHEMA src.runtime")
                con.execute(
                    """
                    CREATE TABLE src.runtime.operator_surface_refresh_runs (
                        refresh_run_id TEXT,
                        ui_replica_ok BOOLEAN,
                        ui_lite_ok BOOLEAN,
                        degraded_surface_count BIGINT,
                        read_error_surface_count BIGINT,
                        error TEXT,
                        refreshed_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.runtime.operator_surface_refresh_runs VALUES
                    ('refresh_1', TRUE, TRUE, 1, 1, NULL, TIMESTAMP '2026-03-21 12:00:00')
                    """
                )
                table_row_counts = {"ui.surface_delivery_summary": 3}
                _create_system_runtime_summary(con, table_row_counts=table_row_counts)
                row = con.execute(
                    """
                    SELECT
                        latest_surface_refresh_run_id,
                        latest_surface_refresh_status,
                        ui_replica_status,
                        ui_lite_status,
                        readiness_status,
                        weather_chain_status,
                        degraded_surface_count,
                        read_error_surface_count,
                        calibration_hard_gate_market_count,
                        pending_operator_review_count
                    FROM ui.system_runtime_summary
                    """
                ).fetchone()
            finally:
                con.close()

        self.assertEqual(
            tuple(row),
            ("refresh_1", "read_error", "ok", "ok", "GO", "degraded", 1, 1, 2, 1),
        )


if __name__ == "__main__":
    unittest.main()
