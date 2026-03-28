from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from agents.common import AgentInvocationStatus
from agents.weather.opportunity_triage_agent import OpportunityTriageAgentRequest, run_opportunity_triage_agent_review
from asterion_core.ui.ui_lite_db import _create_opportunity_triage_summary


class _TimeoutAgentClient:
    def invoke(self, **kwargs):  # noqa: ANN003
        raise TimeoutError("triage timed out")


def _request() -> OpportunityTriageAgentRequest:
    return OpportunityTriageAgentRequest(
        market_id="mkt_1",
        question="Will Seattle be cold?",
        location_name="Seattle",
        best_side="BUY",
        ranking_score=0.82,
        recommended_size=4.0,
        allocation_status="resized",
        operator_bucket="review_required",
        queue_reason_codes=["allocation:resized"],
        execution_intelligence_score=0.31,
        top_of_book_stability=0.22,
        spread_regime="wide",
        expected_capture_regime="low",
        expected_slippage_regime="high",
        microstructure_reason_codes=["size_shock"],
        calibration_gate_status="review_required",
        capital_policy_id="cap_policy_1",
        surface_delivery_status="degraded_source",
        surface_fallback_origin="runtime_db",
        source_provenance={"primary_source": "ui_replica", "subject_type": "weather_market"},
        source_freshness="fresh",
        is_degraded_source=True,
        why_ranked_json={"mode": "ranking_v2"},
        pricing_context_json={"ranking_score": 0.82},
    )


class OpportunityTriageTimeoutIsolationTest(unittest.TestCase):
    def test_timeout_maps_to_timeout_status_without_output(self) -> None:
        artifacts = run_opportunity_triage_agent_review(
            _TimeoutAgentClient(),
            _request(),
            now=datetime(2026, 3, 22, 10, 0, tzinfo=UTC),
        )
        self.assertEqual(artifacts.invocation.status, AgentInvocationStatus.TIMEOUT)
        self.assertIsNone(artifacts.output)
        self.assertIsNone(artifacts.review)
        self.assertIsNone(artifacts.evaluation)

    def test_timeout_rolls_up_to_failed_overlay_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "triage_timeout.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute("ATTACH ':memory:' AS src")
                con.execute("CREATE SCHEMA src.agent")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        source_badge TEXT,
                        source_truth_status TEXT,
                        primary_score_label TEXT
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_1', 'ui_lite', 'ok', 'ranking_score')
                    """
                )
                con.execute(
                    """
                    CREATE TABLE src.agent.invocations(
                        invocation_id TEXT,
                        agent_type TEXT,
                        subject_type TEXT,
                        subject_id TEXT,
                        status TEXT,
                        started_at TIMESTAMP,
                        ended_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO src.agent.invocations VALUES
                    ('inv_1', 'opportunity_triage', 'weather_market', 'mkt_1', 'timeout', '2026-03-22 10:00:00', '2026-03-22 10:01:00')
                    """
                )
                con.execute("CREATE TABLE src.agent.outputs(invocation_id TEXT, structured_output_json TEXT, created_at TIMESTAMP)")
                con.execute("CREATE TABLE src.agent.reviews(invocation_id TEXT, review_status TEXT, reviewed_at TIMESTAMP)")
                con.execute(
                    """
                    CREATE TABLE src.agent.evaluations(
                        evaluation_id TEXT,
                        invocation_id TEXT,
                        verification_method TEXT,
                        score_json TEXT,
                        is_verified BOOLEAN,
                        created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE src.agent.operator_review_decisions(
                        invocation_id TEXT,
                        subject_id TEXT,
                        agent_type TEXT,
                        subject_type TEXT,
                        decision_status TEXT,
                        operator_action TEXT,
                        updated_at TIMESTAMP
                    )
                    """
                )
                counts: dict[str, int] = {}
                _create_opportunity_triage_summary(con, table_row_counts=counts)
                row = con.execute(
                    """
                    SELECT latest_agent_status, effective_triage_status
                    FROM ui.opportunity_triage_summary
                    WHERE market_id = 'mkt_1'
                    """
                ).fetchone()
            finally:
                con.close()
        self.assertEqual(row, ("timeout", "agent_timeout"))


if __name__ == "__main__":
    unittest.main()
