from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig

from .runtime import (
    AgentEvaluationRecord,
    AgentExecutionArtifacts,
    AgentInvocationRecord,
    AgentOutputRecord,
    AgentReviewRecord,
    OperatorReviewDecisionRecord,
)


AGENT_INVOCATION_COLUMNS = [
    "invocation_id",
    "agent_type",
    "agent_version",
    "prompt_version",
    "subject_type",
    "subject_id",
    "input_hash",
    "model_provider",
    "model_name",
    "status",
    "started_at",
    "ended_at",
    "latency_ms",
    "error_message",
    "input_payload_json",
]

AGENT_OUTPUT_COLUMNS = [
    "output_id",
    "invocation_id",
    "verdict",
    "confidence",
    "summary",
    "findings_json",
    "structured_output_json",
    "human_review_required",
    "created_at",
]

AGENT_REVIEW_COLUMNS = [
    "review_id",
    "invocation_id",
    "review_status",
    "reviewer_id",
    "review_notes",
    "review_payload_json",
    "reviewed_at",
]

AGENT_EVALUATION_COLUMNS = [
    "evaluation_id",
    "invocation_id",
    "verification_method",
    "score_json",
    "is_verified",
    "notes",
    "created_at",
]

AGENT_OPERATOR_REVIEW_DECISION_COLUMNS = [
    "review_decision_id",
    "invocation_id",
    "agent_type",
    "subject_type",
    "subject_id",
    "decision_status",
    "operator_action",
    "reason",
    "actor",
    "created_at",
    "updated_at",
]


def enqueue_agent_invocation_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    invocations: list[AgentInvocationRecord],
    run_id: str | None = None,
) -> str | None:
    if not invocations:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="agent.invocations",
        pk_cols=["invocation_id"],
        columns=list(AGENT_INVOCATION_COLUMNS),
        rows=[agent_invocation_to_row(item) for item in invocations],
        run_id=run_id,
    )


def enqueue_agent_output_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    outputs: list[AgentOutputRecord],
    run_id: str | None = None,
) -> str | None:
    if not outputs:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="agent.outputs",
        pk_cols=["output_id"],
        columns=list(AGENT_OUTPUT_COLUMNS),
        rows=[agent_output_to_row(item) for item in outputs],
        run_id=run_id,
    )


def enqueue_agent_review_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    reviews: list[AgentReviewRecord],
    run_id: str | None = None,
) -> str | None:
    if not reviews:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="agent.reviews",
        pk_cols=["review_id"],
        columns=list(AGENT_REVIEW_COLUMNS),
        rows=[agent_review_to_row(item) for item in reviews],
        run_id=run_id,
    )


def enqueue_agent_evaluation_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    evaluations: list[AgentEvaluationRecord],
    run_id: str | None = None,
) -> str | None:
    if not evaluations:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="agent.evaluations",
        pk_cols=["evaluation_id"],
        columns=list(AGENT_EVALUATION_COLUMNS),
        rows=[agent_evaluation_to_row(item) for item in evaluations],
        run_id=run_id,
    )


def enqueue_agent_operator_review_decision_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    decisions: list[OperatorReviewDecisionRecord],
    run_id: str | None = None,
) -> str | None:
    if not decisions:
        return None
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="agent.operator_review_decisions",
        pk_cols=["review_decision_id"],
        columns=list(AGENT_OPERATOR_REVIEW_DECISION_COLUMNS),
        rows=[agent_operator_review_decision_to_row(item) for item in decisions],
        run_id=run_id,
    )


def enqueue_agent_artifact_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    artifacts: list[AgentExecutionArtifacts],
    run_id: str | None = None,
) -> list[str]:
    task_ids: list[str] = []
    invocations = [item.invocation for item in artifacts]
    outputs = [item.output for item in artifacts if item.output is not None]
    reviews = [item.review for item in artifacts if item.review is not None]
    evaluations = [item.evaluation for item in artifacts if item.evaluation is not None]
    for task_id in [
        enqueue_agent_invocation_upserts(queue_cfg, invocations=invocations, run_id=run_id),
        enqueue_agent_output_upserts(queue_cfg, outputs=outputs, run_id=run_id),
        enqueue_agent_review_upserts(queue_cfg, reviews=reviews, run_id=run_id),
        enqueue_agent_evaluation_upserts(queue_cfg, evaluations=evaluations, run_id=run_id),
    ]:
        if task_id is not None:
            task_ids.append(task_id)
    return task_ids


def agent_invocation_to_row(record: AgentInvocationRecord) -> list[Any]:
    return [
        record.invocation_id,
        record.agent_type.value,
        record.agent_version,
        record.prompt_version,
        record.subject_type,
        record.subject_id,
        record.input_hash,
        record.model_provider,
        record.model_name,
        record.status.value,
        _sql_ts(record.started_at),
        _sql_ts(record.ended_at),
        record.latency_ms,
        record.error_message,
        safe_json_dumps(record.input_payload_json),
    ]


def agent_output_to_row(record: AgentOutputRecord) -> list[Any]:
    return [
        record.output_id,
        record.invocation_id,
        record.verdict.value,
        record.confidence,
        record.summary,
        safe_json_dumps(record.findings_json),
        safe_json_dumps(record.structured_output_json),
        record.human_review_required,
        _sql_ts(record.created_at),
    ]


def agent_review_to_row(record: AgentReviewRecord) -> list[Any]:
    return [
        record.review_id,
        record.invocation_id,
        record.review_status.value,
        record.reviewer_id,
        record.review_notes,
        safe_json_dumps(record.review_payload_json),
        _sql_ts(record.reviewed_at),
    ]


def agent_evaluation_to_row(record: AgentEvaluationRecord) -> list[Any]:
    return [
        record.evaluation_id,
        record.invocation_id,
        record.verification_method,
        safe_json_dumps(record.score_json),
        record.is_verified,
        record.notes,
        _sql_ts(record.created_at),
    ]


def agent_operator_review_decision_to_row(record: OperatorReviewDecisionRecord) -> list[Any]:
    return [
        record.review_decision_id,
        record.invocation_id,
        record.agent_type.value,
        record.subject_type,
        record.subject_id,
        record.decision_status,
        record.operator_action,
        record.reason,
        record.actor,
        _sql_ts(record.created_at),
        _sql_ts(record.updated_at),
    ]


def _sql_ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
