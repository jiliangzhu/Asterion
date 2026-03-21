from __future__ import annotations

import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import ui.data_access as data_access_module

from ui.data_access import write_resolution_operator_review_decision


def _build_resolution_review_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE SCHEMA resolution")
        con.execute("CREATE SCHEMA agent")
        con.execute(
            """
            CREATE TABLE resolution.uma_proposals(
                proposal_id TEXT,
                market_id TEXT,
                condition_id TEXT,
                proposer TEXT,
                proposed_outcome TEXT,
                proposal_bond DOUBLE,
                dispute_bond DOUBLE,
                proposal_tx_hash TEXT,
                proposal_block_number BIGINT,
                proposal_timestamp TIMESTAMP,
                status TEXT,
                on_chain_settled_at TIMESTAMP,
                safe_redeem_after TIMESTAMP,
                human_review_required BOOLEAN
            )
            """
        )
        con.execute(
            """
            CREATE TABLE resolution.settlement_verifications(
                verification_id TEXT,
                proposal_id TEXT,
                market_id TEXT,
                proposed_outcome TEXT,
                expected_outcome TEXT,
                is_correct BOOLEAN,
                confidence DOUBLE,
                discrepancy_details TEXT,
                sources_checked TEXT,
                evidence_package TEXT,
                created_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE resolution.redeem_readiness_suggestions(
                suggestion_id TEXT,
                proposal_id TEXT,
                decision TEXT,
                reason TEXT,
                on_chain_settled_at TIMESTAMP,
                safe_redeem_after TIMESTAMP,
                human_review_required BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE resolution.watcher_continuity_checks(
                check_id TEXT,
                status TEXT,
                from_block BIGINT,
                to_block BIGINT,
                created_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE resolution.operator_review_decisions(
                review_decision_id TEXT,
                proposal_id TEXT,
                invocation_id TEXT,
                suggestion_id TEXT,
                decision_status TEXT,
                operator_action TEXT,
                reason TEXT,
                actor TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE agent.invocations(
                invocation_id TEXT,
                agent_type TEXT,
                agent_version TEXT,
                prompt_version TEXT,
                subject_type TEXT,
                subject_id TEXT,
                input_payload_json TEXT,
                model_provider TEXT,
                model_name TEXT,
                status TEXT,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                latency_ms BIGINT,
                force_rerun BOOLEAN,
                error_message TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE agent.outputs(
                output_id TEXT,
                invocation_id TEXT,
                verdict TEXT,
                confidence DOUBLE,
                summary TEXT,
                findings_json TEXT,
                structured_output_json TEXT,
                human_review_required BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE TABLE agent.reviews(
                review_id TEXT,
                invocation_id TEXT,
                human_review_required BOOLEAN,
                review_payload_json TEXT,
                reviewed_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            INSERT INTO resolution.uma_proposals VALUES
            ('prop_1', 'mkt_weather_1', 'cond_weather_1', '0xabc', 'YES', 100.0, NULL, '0xhash', 100, '2026-03-10 00:00:00', 'settled', '2026-03-10 01:00:00', '2026-03-11 01:00:00', FALSE)
            """
        )
        con.execute(
            """
            INSERT INTO resolution.settlement_verifications VALUES
            ('verify_1', 'prop_1', 'mkt_weather_1', 'YES', 'YES', TRUE, 0.95, NULL, '["weather.com"]', '{"evidence_package_id":"evidence_1"}', '2026-03-10 02:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO resolution.redeem_readiness_suggestions VALUES
            ('suggest_1', 'prop_1', 'ready_for_redeem', 'deterministic baseline ready', '2026-03-10 01:00:00', '2026-03-11 01:00:00', FALSE, '2026-03-10 02:30:00')
            """
        )
        con.execute(
            """
            INSERT INTO resolution.watcher_continuity_checks VALUES
            ('continuity_1', 'OK', 1, 2, '2026-03-10 03:00:00')
            """
        )
        con.execute(
            """
            INSERT INTO agent.invocations VALUES
            ('inv_1', 'resolution', 'resolution_agent_v1', 'resolution_prompt_v1', 'uma_proposal', 'prop_1', '{}', 'fake', 'fake-agent-client', 'success', '2026-03-10 04:00:00', '2026-03-10 04:00:01', 1000, FALSE, NULL)
            """
        )
        con.execute(
            """
            INSERT INTO agent.outputs VALUES
            ('out_1', 'inv_1', 'review', 0.84, 'manual review suggested', '[]', '{"recommended_operator_action":"manual_review"}', TRUE, '2026-03-10 04:00:01')
            """
        )
        con.execute(
            """
            INSERT INTO agent.reviews VALUES
            ('review_1', 'inv_1', TRUE, '{"recommended_operator_action":"manual_review","settlement_risk_score":0.7}', '2026-03-10 04:00:01')
            """
        )
    finally:
        con.close()


class ResolutionOperatorReviewClosureTest(unittest.TestCase):
    def test_effective_redeem_status_tracks_operator_review_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "resolution_review.duckdb"
            _build_resolution_review_db(db_path)

            initial = data_access_module._read_proposal_resolution_summary_result(db_path)
            initial_row = initial["frame"].iloc[0].to_dict()
            self.assertEqual(initial_row["effective_redeem_status"], "pending_operator_review")

            with (
                patch.dict(os.environ, {"ASTERION_DB_PATH": str(db_path)}, clear=False),
                patch("ui.data_access._iso_now", return_value=datetime(2026, 3, 10, 4, 5, tzinfo=UTC)),
            ):
                accepted = write_resolution_operator_review_decision(
                    proposal_id="prop_1",
                    invocation_id="inv_1",
                    suggestion_id="suggest_1",
                    decision_status="accepted",
                    operator_action="ready_for_redeem_review",
                    actor="operator",
                    reason="evidence bundle complete",
                )
            self.assertEqual(accepted.operator_action, "ready_for_redeem_review")
            after_accept = data_access_module._read_proposal_resolution_summary_result(db_path)
            accepted_row = after_accept["frame"].iloc[0].to_dict()
            self.assertEqual(accepted_row["latest_operator_review_status"], "accepted")
            self.assertEqual(accepted_row["effective_redeem_status"], "ready_for_redeem_review")

            with (
                patch.dict(os.environ, {"ASTERION_DB_PATH": str(db_path)}, clear=False),
                patch("ui.data_access._iso_now", return_value=datetime(2026, 3, 10, 4, 6, tzinfo=UTC)),
            ):
                write_resolution_operator_review_decision(
                    proposal_id="prop_1",
                    invocation_id="inv_1",
                    suggestion_id="suggest_1",
                    decision_status="rejected",
                    operator_action="manual_review",
                    actor="operator",
                    reason="fallback to deterministic baseline",
                )
            after_reject = data_access_module._read_proposal_resolution_summary_result(db_path)
            rejected_row = after_reject["frame"].iloc[0].to_dict()
            self.assertEqual(rejected_row["latest_operator_review_status"], "rejected")
            self.assertEqual(rejected_row["effective_redeem_status"], "ready_for_redeem")

            with (
                patch.dict(os.environ, {"ASTERION_DB_PATH": str(db_path)}, clear=False),
                patch("ui.data_access._iso_now", return_value=datetime(2026, 3, 10, 4, 7, tzinfo=UTC)),
            ):
                write_resolution_operator_review_decision(
                    proposal_id="prop_1",
                    invocation_id="inv_1",
                    suggestion_id="suggest_1",
                    decision_status="deferred",
                    operator_action="manual_review",
                    actor="operator",
                    reason="need more evidence",
                )
            after_defer = data_access_module._read_proposal_resolution_summary_result(db_path)
            deferred_row = after_defer["frame"].iloc[0].to_dict()
            self.assertEqual(deferred_row["latest_operator_review_status"], "deferred")
            self.assertEqual(deferred_row["effective_redeem_status"], "pending_operator_review")


if __name__ == "__main__":
    unittest.main()
