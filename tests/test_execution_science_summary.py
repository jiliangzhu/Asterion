from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.ui import build_ui_lite_db_once
from tests import test_predicted_vs_realized_summary as predicted_vs_realized_summary_test


HAS_DUCKDB = predicted_vs_realized_summary_test.HAS_DUCKDB


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class ExecutionScienceSummaryTest(unittest.TestCase):
    def test_projection_builds_market_strategy_wallet_cohorts(self) -> None:
        helper = predicted_vs_realized_summary_test.PredictedVsRealizedSummaryTest()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_path = str(Path(tmpdir) / "ui_lite.duckdb")
            helper._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                helper._insert_ticket_case(
                    con,
                    ticket_id="tt_resolved",
                    market_id="mkt_1",
                    watch_snapshot_id="snap_1",
                    order_id="ord_resolved",
                    order_status="filled",
                    fill_size=10.0,
                    with_resolution=True,
                )
                helper._insert_ticket_case(
                    con,
                    ticket_id="tt_partial",
                    market_id="mkt_1",
                    watch_snapshot_id="snap_2",
                    order_id="ord_partial",
                    order_status="partial_filled",
                    fill_size=4.0,
                    with_resolution=False,
                )
                helper._insert_ticket_case(
                    con,
                    ticket_id="tt_submit_rejected",
                    market_id="mkt_2",
                    watch_snapshot_id="snap_3",
                    sign_status="signed",
                    submit_status="rejected",
                    external_order_status="rejected",
                )
            finally:
                con.close()

            result = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_path)
            self.assertTrue(result.ok, result.error)

            con = duckdb.connect(lite_path)
            try:
                rows = con.execute(
                    """
                    SELECT cohort_type, cohort_key, ticket_count, filled_ticket_count, resolved_ticket_count,
                           dominant_miss_reason_bucket, dominant_distortion_reason_bucket,
                           feedback_status, feedback_penalty
                    FROM ui.execution_science_summary
                    ORDER BY cohort_type, cohort_key
                    """
                ).fetchall()
            finally:
                con.close()

        self.assertIn(("market", "mkt_1", 2, 2, 2, "captured_resolved", "execution_distortion", "heuristic_only", 0.0), rows)
        self.assertIn(("market", "mkt_2", 1, 0, 0, "submit_rejected", "execution_distortion", "heuristic_only", 0.0), rows)
        self.assertIn(("strategy", "weather_primary", 3, 2, 2, "captured_resolved", "execution_distortion", "heuristic_only", 0.0), rows)
        self.assertIn(("wallet", "wallet_weather_1", 3, 2, 2, "captured_resolved", "execution_distortion", "heuristic_only", 0.0), rows)


if __name__ == "__main__":
    unittest.main()
