from __future__ import annotations

import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb

from ui.data_access import write_opportunity_triage_operator_review_decision


class OpportunityTriageOperatorReviewTest(unittest.TestCase):
    def test_operator_review_decision_is_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "asterion.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA agent")
                con.execute(
                    """
                    CREATE TABLE agent.operator_review_decisions(
                        review_decision_id TEXT,
                        invocation_id TEXT,
                        agent_type TEXT,
                        subject_type TEXT,
                        subject_id TEXT,
                        decision_status TEXT,
                        operator_action TEXT,
                        reason TEXT,
                        actor TEXT,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP
                    )
                    """
                )
            finally:
                con.close()
            with (
                patch.dict(os.environ, {"ASTERION_DB_PATH": str(db_path)}, clear=False),
                patch("ui.data_access._iso_now", return_value=datetime(2026, 3, 22, 10, 5, tzinfo=UTC)),
            ):
                record = write_opportunity_triage_operator_review_decision(
                    market_id="mkt_1",
                    invocation_id="inv_1",
                    decision_status="accepted",
                    operator_action="manual_review",
                    actor="operator",
                    reason="triage accepted",
                )
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute(
                    """
                    SELECT agent_type, subject_type, subject_id, decision_status, operator_action, actor
                    FROM agent.operator_review_decisions
                    """
                ).fetchone()
            finally:
                con.close()
        self.assertEqual(record.agent_type.value, "opportunity_triage")
        self.assertEqual(row, ("opportunity_triage", "weather_market", "mkt_1", "accepted", "manual_review", "operator"))


if __name__ == "__main__":
    unittest.main()
