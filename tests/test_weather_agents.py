from __future__ import annotations

import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from agents.common import AgentVerdict, FakeAgentClient
from agents.weather import ResolutionAgentRequest, run_resolution_agent_review
from asterion_core.contracts import (
    EvidencePackageLinkRecord,
    ProposalStatus,
    RedeemDecision,
    RedeemReadinessRecord,
    SettlementVerificationRecord,
    UMAProposal,
    WatcherContinuityCheck,
)
from asterion_core.storage.write_queue import WriteQueueConfig
from dagster_asterion.handlers import run_weather_resolution_review_job


def _request(*, decision: RedeemDecision = RedeemDecision.READY_FOR_REDEEM, continuity_status: str = "OK") -> ResolutionAgentRequest:
    return ResolutionAgentRequest(
        proposal=UMAProposal(
            proposal_id="prop_1",
            market_id="mkt_weather_1",
            condition_id="cond_weather_1",
            proposer="0xabc",
            proposed_outcome="YES",
            proposal_bond=100.0,
            dispute_bond=None,
            proposal_tx_hash="0xhash",
            proposal_block_number=100,
            proposal_timestamp=datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
            status=ProposalStatus.SETTLED,
            on_chain_settled_at=datetime(2026, 3, 9, 1, 0, tzinfo=UTC),
            safe_redeem_after=datetime(2026, 3, 10, 1, 0, tzinfo=UTC),
            human_review_required=False,
        ),
        verification=SettlementVerificationRecord(
            verification_id="verify_1",
            proposal_id="prop_1",
            market_id="mkt_weather_1",
            proposed_outcome="YES",
            expected_outcome="YES",
            is_correct=True,
            confidence=0.95,
            discrepancy_details=None,
            sources_checked=["weather.com"],
            evidence_package_id="evidence_1",
            created_at=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
        ),
        evidence_link=EvidencePackageLinkRecord(
            proposal_id="prop_1",
            verification_id="verify_1",
            evidence_package_id="evidence_1",
            linked_at=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
        ),
        redeem_readiness=RedeemReadinessRecord(
            suggestion_id="redeem_1",
            proposal_id="prop_1",
            decision=decision,
            reason="ready",
            on_chain_settled_at=datetime(2026, 3, 9, 1, 0, tzinfo=UTC),
            safe_redeem_after=datetime(2026, 3, 10, 1, 0, tzinfo=UTC),
            human_review_required=False,
            created_at=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
        ),
        continuity_check=WatcherContinuityCheck(
            check_id="continuity_1",
            chain_id=137,
            from_block=1,
            to_block=2,
            last_known_finalized_block=2,
            status=continuity_status,
            gap_count=0 if continuity_status == "OK" else 1,
            details_json={},
            created_at=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
        ),
    )


class ResolutionAgentTest(unittest.TestCase):
    def test_resolution_agent_returns_structured_review_artifacts(self) -> None:
        client = FakeAgentClient(
            responses={
                "resolution": {
                    "verdict": "review",
                    "confidence": 0.82,
                    "summary": "watch continuity gap suggests manual review",
                    "settlement_risk_score": 0.7,
                    "recommended_operator_action": "manual_review",
                    "human_review_required": True,
                    "findings": [],
                }
            }
        )
        artifacts = run_resolution_agent_review(
            client,
            _request(decision=RedeemDecision.BLOCKED_PENDING_REVIEW, continuity_status="GAP_DETECTED"),
        )
        self.assertEqual(artifacts.output.verdict, AgentVerdict.REVIEW)
        self.assertTrue(artifacts.output.human_review_required)
        self.assertEqual(artifacts.review.review_payload_json["recommended_operator_action"], "manual_review")

    def test_resolution_review_handler_stays_on_agent_pipeline(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        request = _request()
        client = FakeAgentClient(
            responses={
                "resolution": {
                    "verdict": "pass",
                    "confidence": 0.9,
                    "summary": "safe to observe",
                    "settlement_risk_score": 0.2,
                    "recommended_operator_action": "observe",
                    "human_review_required": False,
                    "findings": [],
                }
            }
        )
        artifacts = run_resolution_agent_review(client, request)
        with (
            patch("dagster_asterion.handlers.load_resolution_agent_requests", return_value=[request]) as load_requests,
            patch("dagster_asterion.handlers.run_resolution_agent_review", return_value=artifacts) as run_agent,
            patch("dagster_asterion.handlers.enqueue_agent_artifact_upserts", return_value=["task_agent"]) as enqueue_agent,
        ):
            result = run_weather_resolution_review_job(object(), queue_cfg, client=client)
        load_requests.assert_called_once()
        run_agent.assert_called_once()
        enqueue_agent.assert_called_once()
        self.assertEqual(result.task_ids, ["task_agent"])


if __name__ == "__main__":
    unittest.main()
