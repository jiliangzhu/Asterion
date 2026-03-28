from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ui.data_access import load_opportunity_triage_data


class OpportunityTriageSummaryTest(unittest.TestCase):
    def test_loader_merges_triage_summary_with_market_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.opportunity_triage_summary(
                        market_id TEXT,
                        latest_agent_invocation_id TEXT,
                        latest_agent_status TEXT,
                        latest_triage_status TEXT,
                        priority_band TEXT,
                        recommended_operator_action TEXT,
                        confidence_band TEXT,
                        triage_reason_codes_json TEXT,
                        execution_risk_flags_json TEXT,
                        supporting_evidence_refs_json TEXT,
                        latest_operator_review_status TEXT,
                        latest_operator_action TEXT,
                        effective_triage_status TEXT,
                        updated_at TIMESTAMP,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        question TEXT,
                        location_name TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        allocation_status TEXT,
                        calibration_gate_status TEXT,
                        capital_policy_id TEXT,
                        surface_delivery_status TEXT,
                        surface_fallback_origin TEXT,
                        surface_last_refresh_ts TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.action_queue_summary(
                        market_id TEXT,
                        operator_bucket TEXT,
                        recommended_size DOUBLE,
                        queue_reason_codes_json TEXT,
                        queue_priority BIGINT,
                        ranking_score DOUBLE,
                        updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.opportunity_triage_summary VALUES
                    ('mkt_1','inv_1','success','review','high','manual_review','medium','[\"delivery_degraded\"]','[\"size_shock\"]','[\"ui.market_opportunity_summary:mkt_1\"]','accepted','manual_review','accepted','2026-03-22 10:05:00','ui_lite','ok','ranking_score')
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_1','Will Seattle be cold?','Seattle','BUY',0.82,'resized','review_required','cap_policy_1','degraded_source','runtime_db','2026-03-22 10:04:00')
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.action_queue_summary VALUES
                    ('mkt_1','review_required',4.0,'[\"allocation:resized\"]',2,0.82,'2026-03-22 10:03:00')
                    """
                )
            finally:
                con.close()
            with patch.dict("os.environ", {"ASTERION_UI_LITE_DB_PATH": str(db_path)}, clear=False):
                payload = load_opportunity_triage_data()
        self.assertEqual(payload["source"], "ui_lite")
        row = payload["frame"].iloc[0].to_dict()
        self.assertEqual(row["question"], "Will Seattle be cold?")
        self.assertEqual(row["location_name"], "Seattle")
        self.assertEqual(row["operator_bucket"], "review_required")
        self.assertEqual(row["effective_triage_status"], "accepted")


if __name__ == "__main__":
    unittest.main()
