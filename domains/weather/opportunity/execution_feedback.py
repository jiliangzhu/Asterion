from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from asterion_core.contracts import CohortDistortionSummary, ExecutionFeedbackMaterializationStatus, ExecutionFeedbackPrior
from asterion_core.contracts import stable_object_id
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.write_queue import WriteQueueConfig


EXECUTION_FEEDBACK_MATERIALIZATION_COLUMNS = [
    "materialization_id",
    "run_id",
    "job_name",
    "prior_version",
    "status",
    "lookback_days",
    "source_window_start",
    "source_window_end",
    "input_ticket_count",
    "output_prior_count",
    "degraded_prior_count",
    "materialized_at",
    "error",
]

_LIFECYCLE_PRIORITY = {
    "resolved": 0,
    "filled_unresolved": 1,
    "partially_filled": 2,
    "cancelled": 3,
    "working_unfilled": 4,
    "submitted_ack": 5,
    "submit_rejected": 6,
    "sign_rejected": 7,
    "gate_rejected": 8,
    "signed_not_submitted": 9,
    "ticket_created": 10,
}
_MARKET_MISS_PRIORITY = {
    "gate_rejected": 0,
    "sign_rejected": 1,
    "submit_rejected": 2,
    "working_unfilled": 3,
    "cancelled": 4,
    "partial_fill": 5,
    "captured_unresolved": 6,
    "captured_resolved": 7,
}
_DISTORTION_PRIORITY = {
    "execution_distortion": 0,
    "forecast_distortion": 1,
    "ranking_distortion": 2,
    "none": 3,
}
_SUBMITTED_ACK_STAGES = {
    "submitted_ack",
    "working_unfilled",
    "partially_filled",
    "filled_unresolved",
    "resolved",
    "cancelled",
}
_REJECTED_STAGES = {"submit_rejected", "sign_rejected", "gate_rejected"}


def build_feedback_materialization_id(*, run_id: str, as_of: datetime, prior_version: str) -> str:
    normalized_as_of = _normalize_datetime(as_of)
    return stable_object_id(
        "efmat",
        {
            "run_id": str(run_id),
            "prior_version": str(prior_version),
            "as_of": normalized_as_of.isoformat(sep=" "),
        },
    )


def execution_feedback_penalty(
    *,
    miss_rate: float,
    resolution_rate: float,
    partial_fill_rate: float,
    cancel_rate: float,
    distortion_rate: float,
    adverse_fill_slippage_bps_p50: float | None,
) -> float:
    slippage_component = _clamp(float(adverse_fill_slippage_bps_p50 or 0.0) / 200.0, 0.0, 0.20)
    penalty = (
        0.40 * _clamp(float(miss_rate), 0.0, 1.0)
        + 0.25 * _clamp(float(distortion_rate), 0.0, 1.0)
        + 0.15 * slippage_component
        + 0.10 * _clamp(float(partial_fill_rate), 0.0, 1.0)
        + 0.05 * _clamp(float(cancel_rate), 0.0, 1.0)
        + 0.05 * _clamp(1.0 - float(resolution_rate), 0.0, 1.0)
    )
    return round(_clamp(penalty, 0.0, 0.75), 6)


def execution_feedback_status(*, sample_count: int, feedback_penalty: float) -> str:
    if int(sample_count) < 5:
        return "missing"
    if int(sample_count) < 20:
        return "sparse"
    penalty = float(feedback_penalty)
    if penalty < 0.15:
        return "ready"
    if penalty < 0.30:
        return "watch"
    return "degraded"


def build_execution_feedback_prior(
    *,
    sample_count: int,
    miss_rate: float,
    distortion_rate: float,
    resolution_rate: float,
    partial_fill_rate: float,
    cancel_rate: float,
    adverse_fill_slippage_bps_p50: float | None,
    dominant_miss_reason_bucket: str,
    dominant_distortion_reason_bucket: str,
    cohort_prior_version: str,
    scope_breakdown: dict[str, Any] | None = None,
) -> ExecutionFeedbackPrior:
    penalty = execution_feedback_penalty(
        miss_rate=miss_rate,
        resolution_rate=resolution_rate,
        partial_fill_rate=partial_fill_rate,
        cancel_rate=cancel_rate,
        distortion_rate=distortion_rate,
        adverse_fill_slippage_bps_p50=adverse_fill_slippage_bps_p50,
    )
    return ExecutionFeedbackPrior(
        feedback_penalty=penalty,
        feedback_status=execution_feedback_status(sample_count=sample_count, feedback_penalty=penalty),
        cohort_prior_version=str(cohort_prior_version),
        dominant_miss_reason_bucket=str(dominant_miss_reason_bucket or "not_submitted"),
        dominant_distortion_reason_bucket=str(dominant_distortion_reason_bucket or "none"),
        scope_breakdown=dict(scope_breakdown or {}),
    )


def aggregate_feedback_priors(
    priors: dict[str, ExecutionFeedbackPrior],
    *,
    weights: dict[str, float] | None = None,
) -> ExecutionFeedbackPrior:
    active = {scope: prior for scope, prior in priors.items() if prior is not None}
    if not active:
        return ExecutionFeedbackPrior(
            feedback_penalty=0.0,
            feedback_status="heuristic_only",
            cohort_prior_version=None,
            dominant_miss_reason_bucket="not_submitted",
            dominant_distortion_reason_bucket="none",
            scope_breakdown={},
        )
    normalized_weights = dict(weights or {"market": 0.50, "strategy": 0.30, "wallet": 0.20})
    total_weight = sum(max(0.0, float(normalized_weights.get(scope, 0.0))) for scope in active)
    if total_weight <= 0.0:
        total_weight = float(len(active))
        normalized_weights = {scope: 1.0 for scope in active}
    scope_breakdown: dict[str, Any] = {}
    combined_penalty = 0.0
    statuses: list[str] = []
    versions: list[str] = []
    miss_values: list[str] = []
    distortion_values: list[str] = []
    for scope, prior in active.items():
        weight = max(0.0, float(normalized_weights.get(scope, 0.0))) / total_weight
        combined_penalty += float(prior.feedback_penalty) * weight
        statuses.append(str(prior.feedback_status))
        if prior.cohort_prior_version:
            versions.append(str(prior.cohort_prior_version))
        miss_values.append(str(prior.dominant_miss_reason_bucket))
        distortion_values.append(str(prior.dominant_distortion_reason_bucket))
        scope_breakdown[scope] = {
            "weight": round(weight, 6),
            "feedback_penalty": float(prior.feedback_penalty),
            "feedback_status": prior.feedback_status,
            "cohort_prior_version": prior.cohort_prior_version,
            "dominant_miss_reason_bucket": prior.dominant_miss_reason_bucket,
            "dominant_distortion_reason_bucket": prior.dominant_distortion_reason_bucket,
            **dict(prior.scope_breakdown or {}),
        }
    return ExecutionFeedbackPrior(
        feedback_penalty=round(_clamp(combined_penalty, 0.0, 0.75), 6),
        feedback_status=_aggregate_feedback_status(statuses),
        cohort_prior_version=max(versions) if versions else None,
        dominant_miss_reason_bucket=_dominant_bucket(miss_values, priority={**_MARKET_MISS_PRIORITY, "not_submitted": 3}, default="not_submitted"),
        dominant_distortion_reason_bucket=_dominant_bucket(distortion_values, priority=_DISTORTION_PRIORITY, default="none"),
        scope_breakdown=scope_breakdown,
    )


def build_execution_science_cohort_summaries(frame: pd.DataFrame) -> list[CohortDistortionSummary]:
    if frame.empty:
        return []
    summaries: list[CohortDistortionSummary] = []
    for cohort_type, key_column in [("market", "market_id"), ("strategy", "strategy_id"), ("wallet", "wallet_id")]:
        grouped = frame.groupby(key_column, dropna=False)
        for cohort_key, cohort_frame in grouped:
            ticket_count = int(len(cohort_frame.index))
            submitted_ack_count = int(cohort_frame["execution_lifecycle_stage"].isin(list(_SUBMITTED_ACK_STAGES)).sum())
            filled_ticket_count = int((pd.to_numeric(cohort_frame["filled_quantity"], errors="coerce").fillna(0) > 0).sum())
            resolved_ticket_count = int((cohort_frame["evaluation_status"] == "resolved").sum())
            partial_fill_count = int((cohort_frame["execution_lifecycle_stage"] == "partially_filled").sum())
            cancelled_count = int((cohort_frame["execution_lifecycle_stage"] == "cancelled").sum())
            rejected_count = int(cohort_frame["execution_lifecycle_stage"].isin(list(_REJECTED_STAGES)).sum())
            working_unfilled_count = int((cohort_frame["execution_lifecycle_stage"] == "working_unfilled").sum())
            distortion_values = [_distortion_bucket_for_ticket_row(ticket_row) for _, ticket_row in cohort_frame.iterrows()]
            summaries.append(
                CohortDistortionSummary(
                    cohort_type=str(cohort_type),
                    cohort_key=str(cohort_key),
                    ticket_count=ticket_count,
                    submitted_ack_count=submitted_ack_count,
                    filled_ticket_count=filled_ticket_count,
                    resolved_ticket_count=resolved_ticket_count,
                    partial_fill_count=partial_fill_count,
                    cancelled_count=cancelled_count,
                    rejected_count=rejected_count,
                    working_unfilled_count=working_unfilled_count,
                    miss_rate=round(max(0.0, 1.0 - (filled_ticket_count / ticket_count if ticket_count > 0 else 0.0)), 6),
                    distortion_rate=round(
                        len([value for value in distortion_values if value != "none"]) / ticket_count if ticket_count > 0 else 0.0,
                        6,
                    ),
                    dominant_miss_reason_bucket=_dominant_bucket(
                        [str(value) for value in cohort_frame["miss_reason_bucket"].dropna().tolist()],
                        priority={**_MARKET_MISS_PRIORITY, "captured_resolved": 8, "not_submitted": 3},
                        default="not_submitted",
                    ),
                    dominant_distortion_reason_bucket=_dominant_bucket(
                        distortion_values,
                        priority=_DISTORTION_PRIORITY,
                        default="none",
                    ),
                )
            )
    return summaries


def feedback_summary_rows(summaries: list[CohortDistortionSummary], *, source_origin: str = "ui_lite") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary in summaries:
        rows.append(
            {
                "cohort_type": summary.cohort_type,
                "cohort_key": summary.cohort_key,
                "ticket_count": summary.ticket_count,
                "submitted_ack_count": summary.submitted_ack_count,
                "filled_ticket_count": summary.filled_ticket_count,
                "resolved_ticket_count": summary.resolved_ticket_count,
                "partial_fill_count": summary.partial_fill_count,
                "cancelled_count": summary.cancelled_count,
                "rejected_count": summary.rejected_count,
                "working_unfilled_count": summary.working_unfilled_count,
                "submission_capture_ratio": round(summary.submitted_ack_count / summary.ticket_count, 6)
                if summary.ticket_count > 0
                else 0.0,
                "fill_capture_ratio": round(summary.filled_ticket_count / summary.ticket_count, 6)
                if summary.ticket_count > 0
                else 0.0,
                "resolution_capture_ratio": round(summary.resolved_ticket_count / summary.ticket_count, 6)
                if summary.ticket_count > 0
                else 0.0,
                "dominant_miss_reason_bucket": summary.dominant_miss_reason_bucket,
                "dominant_distortion_reason_bucket": summary.dominant_distortion_reason_bucket,
                "source_origin": source_origin,
            }
        )
    return rows


def build_execution_feedback_materialization_status(
    *,
    materialization_id: str,
    run_id: str,
    job_name: str,
    prior_version: str,
    status: str,
    lookback_days: int,
    source_window_start: datetime,
    source_window_end: datetime,
    input_ticket_count: int,
    output_prior_count: int,
    degraded_prior_count: int,
    materialized_at: datetime,
    error: str | None = None,
) -> ExecutionFeedbackMaterializationStatus:
    return ExecutionFeedbackMaterializationStatus(
        materialization_id=str(materialization_id),
        run_id=str(run_id),
        job_name=str(job_name),
        prior_version=str(prior_version),
        status=str(status),
        lookback_days=int(lookback_days),
        source_window_start=_normalize_datetime(source_window_start),
        source_window_end=_normalize_datetime(source_window_end),
        input_ticket_count=int(input_ticket_count),
        output_prior_count=int(output_prior_count),
        degraded_prior_count=int(degraded_prior_count),
        materialized_at=_normalize_datetime(materialized_at),
        error=None if error in {None, ""} else str(error),
    )


def execution_feedback_materialization_row_to_row(record: ExecutionFeedbackMaterializationStatus) -> list[Any]:
    return [
        record.materialization_id,
        record.run_id,
        record.job_name,
        record.prior_version,
        record.status,
        record.lookback_days,
        _sql_timestamp(record.source_window_start),
        _sql_timestamp(record.source_window_end),
        record.input_ticket_count,
        record.output_prior_count,
        record.degraded_prior_count,
        _sql_timestamp(record.materialized_at),
        record.error,
    ]


def enqueue_execution_feedback_materialization_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    records: list[ExecutionFeedbackMaterializationStatus],
    run_id: str | None = None,
) -> str | None:
    if not records:
        return None
    rows = [execution_feedback_materialization_row_to_row(item) for item in records]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="runtime.execution_feedback_materializations",
        pk_cols=["materialization_id"],
        columns=list(EXECUTION_FEEDBACK_MATERIALIZATION_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def _aggregate_feedback_status(statuses: list[str]) -> str:
    normalized = [str(item or "") for item in statuses]
    if not normalized:
        return "heuristic_only"
    if "degraded" in normalized:
        return "degraded"
    if "sparse" in normalized:
        return "sparse"
    if "watch" in normalized:
        return "watch"
    if all(item == "ready" for item in normalized):
        return "ready"
    return normalized[0] or "heuristic_only"


def _distortion_bucket_for_ticket_row(ticket_row: Any) -> str:
    stage = str(ticket_row.get("execution_lifecycle_stage") or "")
    try:
        values = json.loads(str(ticket_row.get("distortion_reason_codes_json") or "[]"))
    except Exception:  # noqa: BLE001
        values = []
    if stage in {"working_unfilled", "partially_filled", "cancelled", "submit_rejected"}:
        return "execution_distortion"
    if any(str(item).startswith("execution_") for item in values):
        return "execution_distortion"
    if any(str(item).startswith("forecast_") for item in values):
        return "forecast_distortion"
    if any(str(item).startswith("ranking_") for item in values):
        return "ranking_distortion"
    return "none"


def _dominant_bucket(values: list[str], *, priority: dict[str, int], default: str) -> str:
    if not values:
        return default
    counts = pd.Series(values, dtype="string").value_counts(dropna=True)
    if counts.empty:
        return default
    best_count = int(counts.max())
    candidates = [str(index) for index, value in counts.items() if int(value) == best_count]
    candidates.sort(key=lambda item: priority.get(item, 999))
    return candidates[0] if candidates else default


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(float(value), float(lower)), float(upper))


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(microsecond=0)
    return value.astimezone(UTC).replace(tzinfo=None, microsecond=0)


def _sql_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = _normalize_datetime(value)
    return normalized.isoformat(sep=" ")


__all__ = [
    "EXECUTION_FEEDBACK_MATERIALIZATION_COLUMNS",
    "aggregate_feedback_priors",
    "build_execution_feedback_materialization_status",
    "build_execution_feedback_prior",
    "build_execution_science_cohort_summaries",
    "build_feedback_materialization_id",
    "enqueue_execution_feedback_materialization_upserts",
    "execution_feedback_materialization_row_to_row",
    "execution_feedback_penalty",
    "execution_feedback_status",
    "feedback_summary_rows",
]
