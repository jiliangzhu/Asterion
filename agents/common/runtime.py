from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from asterion_core.contracts.ids import stable_object_id
from asterion_core.storage.determinism import stable_payload_sha256


class AgentType(str, Enum):
    RULE2SPEC = "rule2spec"
    DATA_QA = "data_qa"
    RESOLUTION = "resolution"


class AgentInvocationStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    PARSE_ERROR = "parse_error"
    SKIPPED = "skipped"


class AgentVerdict(str, Enum):
    PASS = "pass"
    REVIEW = "review"
    BLOCK = "block"


class AgentReviewStatus(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_FOLLOWUP = "needs_followup"


@dataclass(frozen=True)
class AgentFinding:
    finding_code: str
    severity: str
    entity_type: str
    entity_id: str
    field_name: str | None
    summary: str
    suggested_action: str | None = None

    def __post_init__(self) -> None:
        if not self.finding_code or not self.entity_type or not self.entity_id:
            raise ValueError("finding_code, entity_type, and entity_id are required")
        if self.severity not in {"info", "warn", "error"}:
            raise ValueError("severity must be info, warn, or error")
        if not self.summary:
            raise ValueError("summary is required")


@dataclass(frozen=True)
class AgentInvocationRecord:
    invocation_id: str
    agent_type: AgentType
    agent_version: str
    prompt_version: str
    subject_type: str
    subject_id: str
    input_hash: str
    model_provider: str
    model_name: str
    status: AgentInvocationStatus
    started_at: datetime
    ended_at: datetime | None
    latency_ms: int | None
    error_message: str | None
    input_payload_json: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.invocation_id:
            raise ValueError("invocation_id is required")
        if not self.agent_version or not self.prompt_version:
            raise ValueError("agent_version and prompt_version are required")
        if not self.subject_type or not self.subject_id:
            raise ValueError("subject_type and subject_id are required")
        if not self.input_hash or not self.model_provider or not self.model_name:
            raise ValueError("input_hash, model_provider, and model_name are required")
        if self.latency_ms is not None and self.latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")
        if not isinstance(self.input_payload_json, dict):
            raise ValueError("input_payload_json must be a dictionary")


@dataclass(frozen=True)
class AgentOutputRecord:
    output_id: str
    invocation_id: str
    verdict: AgentVerdict
    confidence: float
    summary: str
    findings_json: list[dict[str, Any]]
    structured_output_json: dict[str, Any]
    human_review_required: bool
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.output_id or not self.invocation_id:
            raise ValueError("output_id and invocation_id are required")
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")
        if not self.summary:
            raise ValueError("summary is required")
        if not isinstance(self.findings_json, list):
            raise ValueError("findings_json must be a list")
        if not isinstance(self.structured_output_json, dict):
            raise ValueError("structured_output_json must be a dictionary")


@dataclass(frozen=True)
class AgentReviewRecord:
    review_id: str
    invocation_id: str
    review_status: AgentReviewStatus
    reviewer_id: str
    review_notes: str | None
    review_payload_json: dict[str, Any]
    reviewed_at: datetime

    def __post_init__(self) -> None:
        if not self.review_id or not self.invocation_id:
            raise ValueError("review_id and invocation_id are required")
        if not self.reviewer_id:
            raise ValueError("reviewer_id is required")
        if not isinstance(self.review_payload_json, dict):
            raise ValueError("review_payload_json must be a dictionary")


@dataclass(frozen=True)
class AgentEvaluationRecord:
    evaluation_id: str
    invocation_id: str
    verification_method: str
    score_json: dict[str, Any]
    is_verified: bool
    notes: str | None
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.evaluation_id or not self.invocation_id:
            raise ValueError("evaluation_id and invocation_id are required")
        if not self.verification_method:
            raise ValueError("verification_method is required")
        if not isinstance(self.score_json, dict):
            raise ValueError("score_json must be a dictionary")


@dataclass(frozen=True)
class AgentExecutionArtifacts:
    invocation: AgentInvocationRecord
    output: AgentOutputRecord | None
    review: AgentReviewRecord | None
    evaluation: AgentEvaluationRecord | None


def stable_agent_input_hash(payload: dict[str, Any]) -> str:
    return stable_payload_sha256(payload)


def build_agent_invocation_record(
    *,
    agent_type: AgentType,
    agent_version: str,
    prompt_version: str,
    subject_type: str,
    subject_id: str,
    input_payload_json: dict[str, Any],
    model_provider: str,
    model_name: str,
    status: AgentInvocationStatus,
    started_at: datetime,
    ended_at: datetime | None = None,
    latency_ms: int | None = None,
    error_message: str | None = None,
    force_rerun: bool = False,
    force_rerun_token: str | None = None,
) -> AgentInvocationRecord:
    input_hash = stable_agent_input_hash(input_payload_json)
    invocation_payload = {
        "agent_type": agent_type.value,
        "agent_version": agent_version,
        "input_hash": input_hash,
        "prompt_version": prompt_version,
        "subject_id": subject_id,
        "subject_type": subject_type,
    }
    if force_rerun:
        invocation_payload["force_rerun_token"] = force_rerun_token or _timestamp_token(started_at)
    invocation_id = stable_object_id("aginv", invocation_payload)
    return AgentInvocationRecord(
        invocation_id=invocation_id,
        agent_type=agent_type,
        agent_version=agent_version,
        prompt_version=prompt_version,
        subject_type=subject_type,
        subject_id=subject_id,
        input_hash=input_hash,
        model_provider=model_provider,
        model_name=model_name,
        status=status,
        started_at=_normalize_datetime(started_at),
        ended_at=_normalize_datetime(ended_at),
        latency_ms=latency_ms,
        error_message=error_message,
        input_payload_json=input_payload_json,
    )


def build_agent_output_record(
    *,
    invocation_id: str,
    verdict: AgentVerdict,
    confidence: float,
    summary: str,
    findings: list[AgentFinding],
    structured_output_json: dict[str, Any],
    human_review_required: bool,
    created_at: datetime | None = None,
) -> AgentOutputRecord:
    timestamp = _normalize_datetime(created_at) or datetime.now(UTC)
    return AgentOutputRecord(
        output_id=stable_object_id("agout", {"invocation_id": invocation_id, "verdict": verdict.value}),
        invocation_id=invocation_id,
        verdict=verdict,
        confidence=float(confidence),
        summary=summary,
        findings_json=[finding_to_json(item) for item in findings],
        structured_output_json=structured_output_json,
        human_review_required=bool(human_review_required),
        created_at=timestamp,
    )


def build_agent_review_record(
    *,
    invocation_id: str,
    human_review_required: bool,
    review_payload_json: dict[str, Any],
    reviewed_at: datetime | None = None,
) -> AgentReviewRecord:
    timestamp = _normalize_datetime(reviewed_at) or datetime.now(UTC)
    if human_review_required:
        status = AgentReviewStatus.NEEDS_FOLLOWUP
        reviewer_id = "system_hook"
        review_notes = "human review pending"
    else:
        status = AgentReviewStatus.APPROVED
        reviewer_id = "system_rule_validation"
        review_notes = "auto-approved by deterministic rule validation"
    return AgentReviewRecord(
        review_id=stable_object_id("agrev", {"invocation_id": invocation_id, "review_status": status.value}),
        invocation_id=invocation_id,
        review_status=status,
        reviewer_id=reviewer_id,
        review_notes=review_notes,
        review_payload_json=review_payload_json,
        reviewed_at=timestamp,
    )


def build_agent_evaluation_record(
    *,
    invocation_id: str,
    confidence: float,
    human_review_required: bool,
    created_at: datetime | None = None,
    score_json: dict[str, Any] | None = None,
) -> AgentEvaluationRecord:
    timestamp = _normalize_datetime(created_at) or datetime.now(UTC)
    if human_review_required:
        verification_method = "pending_human_review"
        is_verified = False
        notes = "awaiting human review"
    else:
        verification_method = "rule_validation"
        is_verified = True
        notes = "auto-verified by deterministic rule validation"
    return AgentEvaluationRecord(
        evaluation_id=stable_object_id("ageval", {"invocation_id": invocation_id, "verification_method": verification_method}),
        invocation_id=invocation_id,
        verification_method=verification_method,
        score_json=score_json or {"confidence": float(confidence)},
        is_verified=is_verified,
        notes=notes,
        created_at=timestamp,
    )


def finding_to_json(finding: AgentFinding) -> dict[str, Any]:
    return {
        "entity_id": finding.entity_id,
        "entity_type": finding.entity_type,
        "field_name": finding.field_name,
        "finding_code": finding.finding_code,
        "severity": finding.severity,
        "suggested_action": finding.suggested_action,
        "summary": finding.summary,
    }


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _timestamp_token(value: datetime) -> str:
    return _normalize_datetime(value).isoformat()
