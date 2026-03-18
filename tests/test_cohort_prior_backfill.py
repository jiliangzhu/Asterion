from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import duckdb

from asterion_core.storage.write_queue import WriteQueueConfig
from dagster_asterion.handlers import run_weather_execution_priors_refresh_job
from tests import test_predicted_vs_realized_summary as predicted_vs_realized_summary_test


HAS_DUCKDB = predicted_vs_realized_summary_test.HAS_DUCKDB


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required")
class CohortPriorBackfillTest(unittest.TestCase):
    def test_handler_enqueues_market_strategy_wallet_priors_and_audit(self) -> None:
        helper = predicted_vs_realized_summary_test.PredictedVsRealizedSummaryTest()
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            helper._apply_migrations(db_path)
            con = duckdb.connect(db_path)
            try:
                for index in range(6):
                    helper._insert_ticket_case(
                        con,
                        ticket_id=f"tt_{index}",
                        market_id="mkt_1",
                        watch_snapshot_id=f"snap_{index}",
                        order_id=f"ord_{index}",
                        order_status="filled" if index < 4 else "posted",
                        fill_size=10.0 if index < 4 else 0.0,
                        with_resolution=index < 3,
                        external_order_status="accepted" if index < 5 else "rejected",
                        submit_status="accepted" if index < 5 else "rejected",
                    )
            finally:
                result = run_weather_execution_priors_refresh_job(
                    con,
                    WriteQueueConfig(path=queue_path),
                    lookback_days=90,
                    run_id="run_feedback_refresh",
                )
                con.close()

            self.assertGreaterEqual(result.item_count, 3)
            self.assertIn("materialization_id", result.metadata)
            self.assertIn("degraded_prior_count", result.metadata)

            qcon = sqlite3.connect(queue_path)
            try:
                rows = qcon.execute(
                    "SELECT task_type, payload_json FROM write_queue_tasks ORDER BY created_ts_ms ASC"
                ).fetchall()
            finally:
                qcon.close()

            self.assertEqual(len(rows), 2)
            priors_payload = json.loads(rows[0][1])
            audit_payload = json.loads(rows[1][1])
            prior_rows = priors_payload["rows"]
            cohort_types = {row[18] for row in prior_rows}
            self.assertEqual(cohort_types, {"market", "strategy", "wallet"})
            self.assertEqual(audit_payload["table"], "runtime.execution_feedback_materializations")
            self.assertEqual(audit_payload["rows"][0][0], result.metadata["materialization_id"])


if __name__ == "__main__":
    unittest.main()
