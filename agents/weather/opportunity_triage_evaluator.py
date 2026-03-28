from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from agents.common.runtime import AgentEvaluationRecord, build_agent_evaluation_record

from .opportunity_triage_agent import OpportunityTriageAgentOutput, OpportunityTriageAgentRequest


REPLAY_BACKTEST_METHOD = "replay_backtest"
OPERATOR_OUTCOME_PROXY_METHOD = "operator_outcome_proxy"

_MIN_PRIORITY_PRECISION_PROXY = 0.5
_MAX_FALSE_ESCALATION_RATE = 0.2
_MIN_OPERATOR_THROUGHPUT_DELTA = 0.05
_MIN_QUEUE_CLEANLINESS_DELTA = 0.0


def build_replay_backtest_evaluation_record(
    *,
    invocation_id: str,
    request: OpportunityTriageAgentRequest,
    output: OpportunityTriageAgentOutput,
    created_at: datetime | None = None,
) -> AgentEvaluationRecord:
    timestamp = _normalize_ts(created_at) or datetime.now(UTC)
    score_json = build_replay_backtest_score_json(request=request, output=output, created_at=timestamp)
    return build_agent_evaluation_record(
        invocation_id=invocation_id,
        confidence=output.confidence,
        human_review_required=output.human_review_required,
        verification_method=REPLAY_BACKTEST_METHOD,
        is_verified=_replay_scores_pass(score_json),
        notes="deterministic replay backtest overlay vs baseline queue comparison",
        score_json=score_json,
        created_at=timestamp,
    )


def build_replay_backtest_score_json(
    *,
    request: OpportunityTriageAgentRequest,
    output: OpportunityTriageAgentOutput,
    created_at: datetime,
) -> dict[str, Any]:
    baseline_queue_size = 1 if request.operator_bucket in {"ready_now", "high_risk", "review_required"} else 0
    overlay_queue_size = 1 if output.priority_band.lower() in {"critical", "high"} else 0
    priority_precision_proxy = round(min(1.0, max(0.0, output.confidence * max(request.ranking_score, 0.0))), 4)
    queue_cleanliness_delta = round(
        0.2 if overlay_queue_size >= baseline_queue_size and request.surface_delivery_status == "ok" else (-0.1 if request.is_degraded_source else 0.05),
        4,
    )
    false_escalation_rate = round(0.0 if request.surface_delivery_status == "ok" and request.calibration_gate_status == "clear" else 0.25, 4)
    operator_throughput_delta = round(0.1 if overlay_queue_size == 1 else 0.0, 4)
    window_end = created_at.astimezone(UTC).isoformat()
    return {
        "queue_cleanliness_delta": queue_cleanliness_delta,
        "priority_precision_proxy": priority_precision_proxy,
        "false_escalation_rate": false_escalation_rate,
        "operator_throughput_delta": operator_throughput_delta,
        "baseline_queue_size": baseline_queue_size,
        "overlay_queue_size": overlay_queue_size,
        "evaluation_window_start": window_end,
        "evaluation_window_end": window_end,
        "baseline_operator_bucket": request.operator_bucket,
        "overlay_priority_band": output.priority_band,
    }


def _replay_scores_pass(score_json: dict[str, Any]) -> bool:
    return (
        float(score_json.get("queue_cleanliness_delta") or 0.0) >= _MIN_QUEUE_CLEANLINESS_DELTA
        and float(score_json.get("priority_precision_proxy") or 0.0) >= _MIN_PRIORITY_PRECISION_PROXY
        and float(score_json.get("false_escalation_rate") or 1.0) <= _MAX_FALSE_ESCALATION_RATE
        and float(score_json.get("operator_throughput_delta") or 0.0) >= _MIN_OPERATOR_THROUGHPUT_DELTA
    )


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
