from __future__ import annotations

from datetime import UTC, datetime

from asterion_core.contracts import (
    EvidencePackageLinkRecord,
    ProposalStatus,
    RedeemDecision,
    RedeemReadinessRecord,
    RedeemScheduleInput,
    RedeemScheduleOutput,
    SettlementVerificationRecord,
    UMAProposal,
    stable_object_id,
)


class RedeemScheduler:
    def decide(self, schedule_input: RedeemScheduleInput, now: datetime) -> RedeemScheduleOutput:
        if schedule_input.proposal_status not in {ProposalStatus.SETTLED, ProposalStatus.REDEEMED}:
            return RedeemScheduleOutput(
                decision=RedeemDecision.NOT_REDEEMABLE,
                reason="proposal not settled on chain",
            )
        if schedule_input.proposal_status == ProposalStatus.REDEEMED:
            return RedeemScheduleOutput(
                decision=RedeemDecision.NOT_REDEEMABLE,
                reason="already redeemed",
            )
        if schedule_input.human_review_required:
            return RedeemScheduleOutput(
                decision=RedeemDecision.BLOCKED_PENDING_REVIEW,
                reason="human review required",
            )
        if schedule_input.safe_redeem_after is not None and now < schedule_input.safe_redeem_after:
            return RedeemScheduleOutput(
                decision=RedeemDecision.WAIT,
                reason="waiting until safe redeem time",
            )
        return RedeemScheduleOutput(
            decision=RedeemDecision.READY_FOR_REDEEM,
            reason="settled on chain and safe redeem window reached",
        )


def build_settlement_verification(
    *,
    proposal: UMAProposal,
    expected_outcome: str,
    confidence: float,
    sources_checked: list[str],
    evidence_payload: dict,
    discrepancy_details: str | None = None,
    created_at: datetime | None = None,
) -> SettlementVerificationRecord:
    now = _normalize_ts(created_at) or datetime.now(UTC).replace(tzinfo=None)
    evidence_package_id = stable_object_id(
        "evidence",
        {
            "expected_outcome": expected_outcome,
            "proposal_id": proposal.proposal_id,
            "sources_checked": list(sources_checked),
        },
    )
    return SettlementVerificationRecord(
        verification_id=stable_object_id(
            "verify",
            {
                "evidence_package_id": evidence_package_id,
                "proposal_id": proposal.proposal_id,
                "proposed_outcome": proposal.proposed_outcome,
            },
        ),
        proposal_id=proposal.proposal_id,
        market_id=proposal.market_id,
        proposed_outcome=proposal.proposed_outcome,
        expected_outcome=expected_outcome,
        is_correct=proposal.proposed_outcome == expected_outcome,
        confidence=float(confidence),
        discrepancy_details=discrepancy_details,
        sources_checked=list(sources_checked),
        evidence_package_id=evidence_package_id,
        created_at=now,
    )


def build_evidence_package_link(
    verification: SettlementVerificationRecord,
    *,
    linked_at: datetime | None = None,
) -> EvidencePackageLinkRecord:
    return EvidencePackageLinkRecord(
        proposal_id=verification.proposal_id,
        verification_id=verification.verification_id,
        evidence_package_id=verification.evidence_package_id,
        linked_at=_normalize_ts(linked_at) or verification.created_at,
    )


def build_redeem_readiness_record(
    proposal: UMAProposal,
    *,
    scheduler: RedeemScheduler,
    now: datetime,
) -> RedeemReadinessRecord:
    output = scheduler.decide(
        RedeemScheduleInput(
            proposal_status=proposal.status,
            on_chain_settled_at=proposal.on_chain_settled_at,
            safe_redeem_after=proposal.safe_redeem_after,
            human_review_required=proposal.human_review_required,
        ),
        now=_normalize_ts(now) or now,
    )
    created_at = _normalize_ts(now) or now
    return RedeemReadinessRecord(
        suggestion_id=stable_object_id(
            "redeem",
            {
                "decision": output.decision.value,
                "proposal_id": proposal.proposal_id,
                "safe_redeem_after": proposal.safe_redeem_after.isoformat() if proposal.safe_redeem_after else None,
            },
        ),
        proposal_id=proposal.proposal_id,
        decision=output.decision,
        reason=output.reason,
        on_chain_settled_at=proposal.on_chain_settled_at,
        safe_redeem_after=proposal.safe_redeem_after,
        human_review_required=proposal.human_review_required,
        created_at=created_at,
    )


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value
