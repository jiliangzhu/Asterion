from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from agents.common import (
    AgentClient,
    AgentExecutionArtifacts,
    AgentFinding,
    AgentInvocationStatus,
    AgentType,
    AgentVerdict,
    build_agent_evaluation_record,
    build_agent_invocation_record,
    build_agent_output_record,
    build_agent_review_record,
)
from asterion_core.contracts import (
    EvidencePackageLinkRecord,
    ProposalStatus,
    RedeemDecision,
    RedeemReadinessRecord,
    SettlementVerificationRecord,
    UMAProposal,
    WatcherContinuityCheck,
)


AGENT_VERSION = "resolution_agent_v1"
PROMPT_VERSION = "resolution_prompt_v1"
ALLOWED_OPERATOR_ACTIONS = {
    "observe",
    "manual_review",
    "consider_dispute",
    "hold_redeem",
    "ready_for_redeem_review",
}


@dataclass(frozen=True)
class ResolutionAgentRequest:
    proposal: UMAProposal
    verification: SettlementVerificationRecord
    evidence_link: EvidencePackageLinkRecord
    redeem_readiness: RedeemReadinessRecord
    continuity_check: WatcherContinuityCheck | None


@dataclass(frozen=True)
class ResolutionAgentOutput:
    verdict: AgentVerdict
    confidence: float
    summary: str
    settlement_risk_score: float
    recommended_operator_action: str
    findings: list[AgentFinding]
    human_review_required: bool

    def __post_init__(self) -> None:
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")
        if not (0.0 <= float(self.settlement_risk_score) <= 1.0):
            raise ValueError("settlement_risk_score must be between 0 and 1")
        if self.recommended_operator_action not in ALLOWED_OPERATOR_ACTIONS:
            raise ValueError("unsupported recommended_operator_action")
        if not self.summary:
            raise ValueError("summary is required")


def load_resolution_agent_requests(
    con,
    *,
    proposal_ids: list[str] | None = None,
    limit: int | None = None,
) -> list[ResolutionAgentRequest]:
    sql = """
        SELECT
            p.proposal_id,
            p.market_id,
            p.condition_id,
            p.proposer,
            p.proposed_outcome,
            p.proposal_bond,
            p.dispute_bond,
            p.proposal_tx_hash,
            p.proposal_block_number,
            p.proposal_timestamp,
            p.status,
            p.on_chain_settled_at,
            p.safe_redeem_after,
            p.human_review_required,
            v.verification_id,
            v.expected_outcome,
            v.is_correct,
            v.confidence,
            v.discrepancy_details,
            v.sources_checked,
            v.evidence_package,
            v.created_at,
            r.suggestion_id,
            r.decision,
            r.reason,
            r.on_chain_settled_at,
            r.safe_redeem_after,
            r.human_review_required,
            r.created_at
        FROM resolution.uma_proposals p
        JOIN resolution.settlement_verifications v ON v.proposal_id = p.proposal_id
        JOIN resolution.redeem_readiness_suggestions r ON r.proposal_id = p.proposal_id
    """
    params: list[Any] = []
    if proposal_ids:
        placeholders = ",".join(["?"] * len(proposal_ids))
        sql += f" WHERE p.proposal_id IN ({placeholders})"
        params.extend(proposal_ids)
    sql += " ORDER BY p.proposal_block_number DESC, p.proposal_id DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    rows = con.execute(sql, params).fetchall()
    requests: list[ResolutionAgentRequest] = []
    continuity = _load_latest_continuity_check(con)
    for row in rows:
        proposal = UMAProposal(
            proposal_id=str(row[0]),
            market_id=str(row[1]),
            condition_id=str(row[2]),
            proposer=str(row[3]),
            proposed_outcome=str(row[4]),
            proposal_bond=float(row[5]),
            dispute_bond=float(row[6]) if row[6] is not None else None,
            proposal_tx_hash=str(row[7]),
            proposal_block_number=int(row[8]),
            proposal_timestamp=row[9],
            status=ProposalStatus(str(row[10])),
            on_chain_settled_at=row[11],
            safe_redeem_after=row[12],
            human_review_required=bool(row[13]),
        )
        evidence_payload = _json_dict(row[20])
        verification = SettlementVerificationRecord(
            verification_id=str(row[14]),
            proposal_id=proposal.proposal_id,
            market_id=proposal.market_id,
            proposed_outcome=proposal.proposed_outcome,
            expected_outcome=str(row[15]),
            is_correct=bool(row[16]),
            confidence=float(row[17]),
            discrepancy_details=str(row[18]) if row[18] is not None else None,
            sources_checked=_json_list(row[19]),
            evidence_package_id=str(evidence_payload.get("evidence_package_id") or ""),
            created_at=row[21],
        )
        evidence_link = _load_evidence_link(con, verification_id=verification.verification_id)
        redeem = RedeemReadinessRecord(
            suggestion_id=str(row[22]),
            proposal_id=proposal.proposal_id,
            decision=RedeemDecision(str(row[23])),
            reason=str(row[24]),
            on_chain_settled_at=row[25],
            safe_redeem_after=row[26],
            human_review_required=bool(row[27]),
            created_at=row[28],
        )
        requests.append(
            ResolutionAgentRequest(
                proposal=proposal,
                verification=verification,
                evidence_link=evidence_link,
                redeem_readiness=redeem,
                continuity_check=continuity,
            )
        )
    return requests


def run_resolution_agent_review(
    client: AgentClient,
    request: ResolutionAgentRequest,
    *,
    force_rerun: bool = False,
    now: datetime | None = None,
) -> AgentExecutionArtifacts:
    started_at = _normalize_ts(now) or datetime.now(UTC)
    payload = build_resolution_agent_input_payload(request)
    try:
        response = client.invoke(
            system_prompt=_system_prompt(),
            user_prompt=_user_prompt(),
            input_payload_json=payload,
            metadata={"agent_type": AgentType.RESOLUTION.value, "subject_type": "uma_proposal", "subject_id": request.proposal.proposal_id},
        )
        output = parse_resolution_agent_output(response.structured_output_json, request=request)
        ended_at = datetime.now(UTC)
        invocation = build_agent_invocation_record(
            agent_type=AgentType.RESOLUTION,
            agent_version=AGENT_VERSION,
            prompt_version=PROMPT_VERSION,
            subject_type="uma_proposal",
            subject_id=request.proposal.proposal_id,
            input_payload_json=payload,
            model_provider=response.model_provider,
            model_name=response.model_name,
            status=AgentInvocationStatus.SUCCESS,
            started_at=started_at,
            ended_at=ended_at,
            latency_ms=_latency_ms(started_at, ended_at),
            force_rerun=force_rerun,
        )
        output_record = build_agent_output_record(
            invocation_id=invocation.invocation_id,
            verdict=output.verdict,
            confidence=output.confidence,
            summary=output.summary,
            findings=output.findings,
            structured_output_json=resolution_output_to_json(output),
            human_review_required=output.human_review_required,
            created_at=ended_at,
        )
        review_record = build_agent_review_record(
            invocation_id=invocation.invocation_id,
            human_review_required=output.human_review_required,
            review_payload_json={
                "recommended_operator_action": output.recommended_operator_action,
                "settlement_risk_score": output.settlement_risk_score,
                "subject_id": request.proposal.proposal_id,
            },
            reviewed_at=ended_at,
        )
        evaluation_record = build_agent_evaluation_record(
            invocation_id=invocation.invocation_id,
            confidence=output.confidence,
            human_review_required=output.human_review_required,
            score_json={
                "continuity_status": request.continuity_check.status if request.continuity_check is not None else None,
                "is_correct": request.verification.is_correct,
                "recommended_operator_action": output.recommended_operator_action,
                "settlement_risk_score": output.settlement_risk_score,
            },
            created_at=ended_at,
        )
        return AgentExecutionArtifacts(invocation=invocation, output=output_record, review=review_record, evaluation=evaluation_record)
    except TimeoutError as exc:
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.TIMEOUT, error_message=str(exc), force_rerun=force_rerun)
    except ValueError as exc:
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.PARSE_ERROR, error_message=str(exc), force_rerun=force_rerun)
    except Exception as exc:  # noqa: BLE001
        return _failed_artifacts(request=request, payload=payload, started_at=started_at, status=AgentInvocationStatus.FAILURE, error_message=str(exc), force_rerun=force_rerun)


def build_resolution_agent_input_payload(request: ResolutionAgentRequest) -> dict[str, Any]:
    continuity = None
    if request.continuity_check is not None:
        continuity = {
            "check_id": request.continuity_check.check_id,
            "details_json": dict(request.continuity_check.details_json),
            "gap_count": request.continuity_check.gap_count,
            "status": request.continuity_check.status,
        }
    return {
        "continuity_check": continuity,
        "evidence_link": {
            "evidence_package_id": request.evidence_link.evidence_package_id,
            "linked_at": request.evidence_link.linked_at.isoformat(),
            "proposal_id": request.evidence_link.proposal_id,
            "verification_id": request.evidence_link.verification_id,
        },
        "proposal": {
            "condition_id": request.proposal.condition_id,
            "human_review_required": request.proposal.human_review_required,
            "market_id": request.proposal.market_id,
            "proposal_id": request.proposal.proposal_id,
            "proposed_outcome": request.proposal.proposed_outcome,
            "safe_redeem_after": request.proposal.safe_redeem_after.isoformat() if request.proposal.safe_redeem_after else None,
            "status": request.proposal.status.value,
        },
        "redeem_readiness": {
            "decision": request.redeem_readiness.decision.value,
            "human_review_required": request.redeem_readiness.human_review_required,
            "reason": request.redeem_readiness.reason,
            "suggestion_id": request.redeem_readiness.suggestion_id,
        },
        "verification": {
            "confidence": request.verification.confidence,
            "discrepancy_details": request.verification.discrepancy_details,
            "evidence_package_id": request.verification.evidence_package_id,
            "expected_outcome": request.verification.expected_outcome,
            "is_correct": request.verification.is_correct,
            "proposal_id": request.verification.proposal_id,
            "sources_checked": list(request.verification.sources_checked),
            "verification_id": request.verification.verification_id,
        },
    }


def parse_resolution_agent_output(payload: dict[str, Any], *, request: ResolutionAgentRequest) -> ResolutionAgentOutput:
    findings = _parse_findings(payload.get("findings"), default_entity_type="uma_proposal", default_entity_id=request.proposal.proposal_id)
    verdict = AgentVerdict(str(payload["verdict"]))
    confidence = float(payload["confidence"])
    summary = str(payload["summary"])
    human_review_required = bool(
        payload.get(
            "human_review_required",
            verdict is not AgentVerdict.PASS
            or (request.continuity_check is not None and request.continuity_check.status != "OK")
            or not request.verification.is_correct,
        )
    )
    return ResolutionAgentOutput(
        verdict=verdict,
        confidence=confidence,
        summary=summary,
        settlement_risk_score=float(payload["settlement_risk_score"]),
        recommended_operator_action=str(payload["recommended_operator_action"]),
        findings=findings,
        human_review_required=human_review_required,
    )


def resolution_output_to_json(output: ResolutionAgentOutput) -> dict[str, Any]:
    return {
        "confidence": output.confidence,
        "findings": [finding.__dict__ for finding in output.findings],
        "human_review_required": output.human_review_required,
        "recommended_operator_action": output.recommended_operator_action,
        "settlement_risk_score": output.settlement_risk_score,
        "summary": output.summary,
        "verdict": output.verdict.value,
    }


def _load_evidence_link(con, *, verification_id: str) -> EvidencePackageLinkRecord:
    row = con.execute(
        """
        SELECT proposal_id, verification_id, evidence_package_id, linked_at
        FROM resolution.proposal_evidence_links
        WHERE verification_id = ?
        ORDER BY linked_at DESC
        LIMIT 1
        """,
        [verification_id],
    ).fetchone()
    if row is None:
        raise LookupError(f"proposal evidence link not found for verification_id={verification_id}")
    return EvidencePackageLinkRecord(
        proposal_id=str(row[0]),
        verification_id=str(row[1]),
        evidence_package_id=str(row[2]),
        linked_at=row[3],
    )


def _load_latest_continuity_check(con) -> WatcherContinuityCheck | None:
    row = con.execute(
        """
        SELECT
            check_id,
            chain_id,
            from_block,
            to_block,
            last_known_finalized_block,
            status,
            gap_count,
            details_json,
            created_at
        FROM resolution.watcher_continuity_checks
        ORDER BY created_at DESC, check_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return WatcherContinuityCheck(
        check_id=str(row[0]),
        chain_id=int(row[1]),
        from_block=int(row[2]),
        to_block=int(row[3]),
        last_known_finalized_block=int(row[4]),
        status=str(row[5]),
        gap_count=int(row[6]),
        details_json=_json_dict(row[7]),
        created_at=row[8],
    )


def _system_prompt() -> str:
    return (
        "You review deterministic settlement verification and redeem suggestions. "
        "Do not trigger disputes or redeems. Return only JSON."
    )


def _user_prompt() -> str:
    return (
        "Review the proposal, settlement verification, evidence link, redeem readiness, and optional continuity check. "
        "Return verdict, confidence, summary, settlement_risk_score, recommended_operator_action, findings, "
        "and human_review_required."
    )


def _parse_findings(raw: Any, *, default_entity_type: str, default_entity_id: str) -> list[AgentFinding]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("findings must be a list")
    findings: list[AgentFinding] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("finding entry must be an object")
        findings.append(
            AgentFinding(
                finding_code=str(item["finding_code"]),
                severity=str(item["severity"]).lower(),
                entity_type=str(item.get("entity_type") or default_entity_type),
                entity_id=str(item.get("entity_id") or default_entity_id),
                field_name=str(item["field_name"]) if item.get("field_name") is not None else None,
                summary=str(item["summary"]),
                suggested_action=str(item["suggested_action"]) if item.get("suggested_action") is not None else None,
            )
        )
    return findings


def _failed_artifacts(
    *,
    request: ResolutionAgentRequest,
    payload: dict[str, Any],
    started_at: datetime,
    status: AgentInvocationStatus,
    error_message: str,
    force_rerun: bool,
) -> AgentExecutionArtifacts:
    ended_at = datetime.now(UTC)
    invocation = build_agent_invocation_record(
        agent_type=AgentType.RESOLUTION,
        agent_version=AGENT_VERSION,
        prompt_version=PROMPT_VERSION,
        subject_type="uma_proposal",
        subject_id=request.proposal.proposal_id,
        input_payload_json=payload,
        model_provider="agent_runtime",
        model_name="failed_before_output",
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        latency_ms=_latency_ms(started_at, ended_at),
        error_message=error_message,
        force_rerun=force_rerun,
    )
    return AgentExecutionArtifacts(invocation=invocation, output=None, review=None, evaluation=None)


def _json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    decoded = json.loads(str(value))
    if not isinstance(decoded, dict):
        raise ValueError("json value must decode to an object")
    return decoded


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    decoded = json.loads(str(value))
    if not isinstance(decoded, list):
        raise ValueError("json value must decode to a list")
    return [str(item) for item in decoded]


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(UTC).replace(microsecond=0)


def _latency_ms(started_at: datetime, ended_at: datetime) -> int:
    return max(0, int((ended_at - started_at).total_seconds() * 1000))
