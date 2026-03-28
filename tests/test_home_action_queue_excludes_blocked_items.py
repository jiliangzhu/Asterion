from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from ui.loaders.home_loader import load_home_decision_snapshot


class HomeActionQueueExcludesBlockedItemsTest(unittest.TestCase):
    def test_home_snapshot_keeps_blocked_rows_out_of_primary_action_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "home_queue.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.action_queue_summary (
                        queue_item_id TEXT,
                        market_id TEXT,
                        location_name TEXT,
                        question TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        operator_bucket TEXT,
                        queue_priority BIGINT,
                        updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.action_queue_summary VALUES
                    ('q_ready', 'mkt_ready', 'Seattle', 'Ready market', 'BUY', 0.91, 'ready_now', 1, TIMESTAMP '2026-03-21 10:00:00'),
                    ('q_review', 'mkt_review', 'Miami', 'Review market', 'BUY', 0.83, 'review_required', 2, TIMESTAMP '2026-03-21 10:01:00'),
                    ('q_blocked', 'mkt_blocked', 'Denver', 'Blocked market', 'BUY', 0.72, 'blocked', 3, TIMESTAMP '2026-03-21 10:02:00')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary (
                        market_id TEXT,
                        ranking_score DOUBLE,
                        actionability_status TEXT,
                        location_name TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_ready', 0.91, 'actionable', 'Seattle'),
                    ('mkt_review', 0.83, 'review_required', 'Miami'),
                    ('mkt_blocked', 0.72, 'blocked', 'Denver')
                    """
                )
            finally:
                con.close()

            with patch.dict(os.environ, {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                with patch("ui.data_access.load_readiness_summary", return_value={"go_decision": "GO", "failed_gate_names": [], "decision_reason": "ok"}), \
                    patch("ui.data_access.load_readiness_evidence_bundle", return_value={"stale_dependencies": [], "blockers": [], "warnings": [], "capability_manifest_status": "valid"}), \
                    patch("ui.data_access.load_wallet_readiness_data", return_value=pd.DataFrame()), \
                    patch("ui.data_access.load_market_watch_data", return_value={"weather_smoke_report": {}, "market_watch": pd.DataFrame()}), \
                    patch("ui.data_access.load_agent_review_data", return_value={"frame": pd.DataFrame(), "source": "ui_lite"}), \
                    patch("ui.data_access.load_predicted_vs_realized_data", return_value={"frame": pd.DataFrame()}), \
                    patch("ui.data_access.load_operator_surface_status", return_value={}), \
                    patch("ui.data_access.load_boundary_sidebar_truth", return_value={}), \
                    patch("ui.loaders.execution_loader.load_execution_console_data", return_value={"watch_only_vs_executed": pd.DataFrame(), "execution_science": pd.DataFrame(), "calibration_health": pd.DataFrame(), "live_prereq": pd.DataFrame(), "exceptions": pd.DataFrame()}), \
                    patch("ui.loaders.markets_loader.load_market_chain_analysis_data", return_value={"market_opportunities": pd.DataFrame(), "market_opportunity_source": "ui_lite"}):
                    payload = load_home_decision_snapshot()

        self.assertEqual(payload["action_queue"]["operator_bucket"].tolist(), ["ready_now", "review_required"])
        self.assertEqual(payload["blocked_backlog"]["operator_bucket"].tolist(), ["blocked"])
        self.assertEqual(payload["metrics"]["blocked_count"], 1)


if __name__ == "__main__":
    unittest.main()
