from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ui.data_access import load_market_opportunity_data


class UiWhyRankedTest(unittest.TestCase):
    def test_ui_lite_market_rows_preserve_persisted_why_ranked_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        question TEXT,
                        location_name TEXT,
                        market_close_time TIMESTAMP,
                        accepting_orders BOOLEAN,
                        best_side TEXT,
                        market_price DOUBLE,
                        fair_value DOUBLE,
                        edge_bps DOUBLE,
                        ranking_score DOUBLE,
                        opportunity_score DOUBLE,
                        actionability_status TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        is_degraded_source BOOLEAN,
                        primary_score_label TEXT,
                        expected_dollar_pnl DOUBLE,
                        capture_probability DOUBLE,
                        risk_penalty DOUBLE,
                        capital_efficiency DOUBLE,
                        why_ranked_json TEXT
                    )
                    """
                )
                why_ranked = {
                    "version": "ranking_v2",
                    "mode": "prior_backed",
                    "capture_probability": 0.72,
                    "expected_dollar_pnl": 0.15,
                    "risk_penalty": 0.02,
                    "capital_efficiency": 1.8,
                    "ranking_score": 0.235,
                }
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES (
                        'mkt_1', 'Seattle weather', 'Seattle', '2026-03-18 12:00:00', TRUE, 'BUY',
                        0.40, 0.55, 900, 0.235, 0.235, 'actionable', 'canonical', 'canonical', FALSE,
                        'ranking_score', 0.15, 0.72, 0.02, 1.8, ?
                    )
                    """,
                    [json.dumps(why_ranked)],
                )
            finally:
                con.close()

            with patch.dict("os.environ", {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                payload = load_market_opportunity_data()

        self.assertEqual(payload["source"], "ui_lite")
        row = payload["frame"].iloc[0].to_dict()
        self.assertEqual(row["primary_score_label"], "ranking_score")
        self.assertEqual(row["ranking_score"], 0.235)
        self.assertEqual(json.loads(row["why_ranked_json"])["mode"], "prior_backed")
        self.assertEqual(json.loads(row["why_ranked_json"])["expected_dollar_pnl"], 0.15)


if __name__ == "__main__":
    unittest.main()
