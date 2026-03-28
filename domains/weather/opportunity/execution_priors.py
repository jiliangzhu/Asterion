from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from statistics import mean
from typing import Any

import pandas as pd

from asterion_core.contracts import ExecutionFeedbackPrior, ExecutionPriorKey, ExecutionPriorSummary, stable_object_id
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.write_queue import WriteQueueConfig
from .execution_feedback import aggregate_feedback_priors, build_execution_feedback_prior, execution_feedback_status
from .resolved_execution_projection import build_resolved_execution_projection


WEATHER_EXECUTION_PRIOR_COLUMNS = [
    "prior_key",
    "market_id",
    "strategy_id",
    "wallet_id",
    "station_id",
    "metric",
    "side",
    "horizon_bucket",
    "liquidity_bucket",
    "market_age_bucket",
    "hours_to_close_bucket",
    "calibration_quality_bucket",
    "source_freshness_bucket",
    "sample_count",
    "submit_ack_rate",
    "fill_rate",
    "resolution_rate",
    "partial_fill_rate",
    "cancel_rate",
    "adverse_fill_slippage_bps_p50",
    "adverse_fill_slippage_bps_p90",
    "submit_latency_ms_p50",
    "submit_latency_ms_p90",
    "fill_latency_ms_p50",
    "fill_latency_ms_p90",
    "realized_edge_retention_bps_p50",
    "realized_edge_retention_bps_p90",
    "avg_realized_pnl",
    "avg_post_trade_error",
    "prior_quality_status",
    "cohort_type",
    "cohort_key",
    "feedback_status",
    "feedback_penalty",
    "cohort_prior_version",
    "miss_rate",
    "distortion_rate",
    "dominant_miss_reason_bucket",
    "dominant_distortion_reason_bucket",
    "last_feedback_materialization_id",
    "source_window_start",
    "source_window_end",
    "materialized_at",
]

_SUBMIT_ACK_STATUSES = {"accepted", "working", "posted", "partial_filled", "filled", "cancelled"}
_CANCELLED_ORDER_STATUSES = {"cancelled", "canceled"}
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


@dataclass(frozen=True)
class ExecutionPriorRow:
    prior_key: str
    market_id: str
    strategy_id: str | None
    wallet_id: str | None
    station_id: str | None
    metric: str | None
    side: str
    horizon_bucket: str
    liquidity_bucket: str
    market_age_bucket: str
    hours_to_close_bucket: str
    calibration_quality_bucket: str
    source_freshness_bucket: str
    sample_count: int
    submit_ack_rate: float
    fill_rate: float
    resolution_rate: float
    partial_fill_rate: float
    cancel_rate: float
    adverse_fill_slippage_bps_p50: float | None
    adverse_fill_slippage_bps_p90: float | None
    submit_latency_ms_p50: float | None
    submit_latency_ms_p90: float | None
    fill_latency_ms_p50: float | None
    fill_latency_ms_p90: float | None
    realized_edge_retention_bps_p50: float | None
    realized_edge_retention_bps_p90: float | None
    avg_realized_pnl: float | None
    avg_post_trade_error: float | None
    prior_quality_status: str
    cohort_type: str
    cohort_key: str
    feedback_status: str
    feedback_penalty: float
    cohort_prior_version: str | None
    miss_rate: float
    distortion_rate: float
    dominant_miss_reason_bucket: str
    dominant_distortion_reason_bucket: str
    last_feedback_materialization_id: str | None
    source_window_start: datetime
    source_window_end: datetime
    materialized_at: datetime


def execution_prior_liquidity_bucket(*, depth_proxy: float | None, spread_bps: int | None) -> str:
    normalized_depth = max(0.0, float(depth_proxy or 0.0))
    normalized_spread = max(0, int(spread_bps or 0))
    if normalized_depth >= 0.80 and normalized_spread < 100:
        return "deep"
    if normalized_depth >= 0.50:
        return "medium"
    if normalized_depth > 0.0 or normalized_spread > 0:
        return "thin"
    return "unknown"


def build_execution_prior_key(
    *,
    market_id: str,
    side: str,
    forecast_target_time: datetime | None = None,
    observation_date: date | None = None,
    depth_proxy: float | None = None,
    spread_bps: int | None = None,
    strategy_id: str | None = None,
    wallet_id: str | None = None,
    station_id: str | None = None,
    metric: str | None = None,
    market_age_bucket: str | None = None,
    hours_to_close_bucket: str | None = None,
    calibration_quality_bucket: str | None = None,
    source_freshness_bucket: str | None = None,
) -> ExecutionPriorKey:
    horizon = "unknown"
    if forecast_target_time is not None and observation_date is not None:
        horizon = _forecast_horizon_bucket(
            observation_date=observation_date,
            forecast_target_time=forecast_target_time,
        )
    return ExecutionPriorKey(
        market_id=str(market_id),
        strategy_id=_coerce_optional_text(strategy_id),
        wallet_id=_coerce_optional_text(wallet_id),
        station_id=_coerce_optional_text(station_id),
        metric=_coerce_optional_text(metric),
        side=str(side).upper(),
        horizon_bucket=horizon,
        liquidity_bucket=execution_prior_liquidity_bucket(depth_proxy=depth_proxy, spread_bps=spread_bps),
        market_age_bucket=_coerce_optional_text(market_age_bucket),
        hours_to_close_bucket=_coerce_optional_text(hours_to_close_bucket),
        calibration_quality_bucket=_coerce_optional_text(calibration_quality_bucket),
        source_freshness_bucket=_coerce_optional_text(source_freshness_bucket),
    )


def execution_prior_key_id(prior_key: ExecutionPriorKey) -> str:
    return stable_object_id(
        "eprior",
        {
            "market_id": prior_key.market_id,
            "strategy_id": prior_key.strategy_id,
            "wallet_id": prior_key.wallet_id,
            "side": prior_key.side,
            "horizon_bucket": prior_key.horizon_bucket,
            "liquidity_bucket": prior_key.liquidity_bucket,
            "market_age_bucket": prior_key.market_age_bucket,
            "hours_to_close_bucket": prior_key.hours_to_close_bucket,
            "calibration_quality_bucket": prior_key.calibration_quality_bucket,
            "source_freshness_bucket": prior_key.source_freshness_bucket,
        },
    )


def execution_prior_row_id(*, prior_key: ExecutionPriorKey, cohort_type: str, cohort_key: str) -> str:
    return stable_object_id(
        "epriorrow",
        {
            "market_id": prior_key.market_id,
            "strategy_id": prior_key.strategy_id,
            "wallet_id": prior_key.wallet_id,
            "side": prior_key.side,
            "horizon_bucket": prior_key.horizon_bucket,
            "liquidity_bucket": prior_key.liquidity_bucket,
            "market_age_bucket": prior_key.market_age_bucket,
            "hours_to_close_bucket": prior_key.hours_to_close_bucket,
            "calibration_quality_bucket": prior_key.calibration_quality_bucket,
            "source_freshness_bucket": prior_key.source_freshness_bucket,
            "cohort_type": str(cohort_type),
            "cohort_key": str(cohort_key),
        },
    )


def build_execution_prior_summary_from_context(source_context: dict[str, Any] | None) -> ExecutionPriorSummary | None:
    if not isinstance(source_context, dict):
        return None
    execution_prior_key = source_context.get("execution_prior_key")
    if not execution_prior_key:
        return None
    prior_key = ExecutionPriorKey(
        market_id=_coerce_optional_text(source_context.get("execution_prior_market_id")),
        strategy_id=_coerce_optional_text(source_context.get("execution_prior_strategy_id")),
        wallet_id=_coerce_optional_text(source_context.get("execution_prior_wallet_id")),
        station_id=_coerce_optional_text(source_context.get("execution_prior_station_id")),
        metric=_coerce_optional_text(source_context.get("execution_prior_metric")),
        side=_coerce_optional_text(source_context.get("execution_prior_side")),
        horizon_bucket=_coerce_optional_text(source_context.get("execution_prior_horizon_bucket")),
        liquidity_bucket=_coerce_optional_text(source_context.get("execution_prior_liquidity_bucket")),
        market_age_bucket=_coerce_optional_text(source_context.get("execution_prior_market_age_bucket")),
        hours_to_close_bucket=_coerce_optional_text(source_context.get("execution_prior_hours_to_close_bucket")),
        calibration_quality_bucket=_coerce_optional_text(source_context.get("execution_prior_calibration_quality_bucket")),
        source_freshness_bucket=_coerce_optional_text(source_context.get("execution_prior_source_freshness_bucket")),
    )
    explicit_feedback_penalty = _coerce_optional_float(source_context.get("execution_prior_feedback_penalty"))
    explicit_feedback_status = _coerce_optional_text(source_context.get("execution_prior_feedback_status"))
    explicit_feedback_scope_breakdown = _json_dict(source_context.get("execution_prior_feedback_scope_breakdown"))
    feedback_prior = None
    if explicit_feedback_penalty is not None or explicit_feedback_status is not None:
        normalized_penalty = max(0.0, min(1.0, float(explicit_feedback_penalty or 0.0)))
        feedback_prior = ExecutionFeedbackPrior(
            feedback_penalty=normalized_penalty,
            feedback_status=explicit_feedback_status or execution_feedback_status(
                sample_count=int(source_context.get("execution_prior_sample_count") or 0),
                feedback_penalty=normalized_penalty,
            ),
            cohort_prior_version=_coerce_optional_text(source_context.get("execution_prior_cohort_prior_version")),
            dominant_miss_reason_bucket=str(source_context.get("execution_prior_dominant_miss_reason_bucket") or "not_submitted"),
            dominant_distortion_reason_bucket=str(
                source_context.get("execution_prior_dominant_distortion_reason_bucket") or "none"
            ),
            scope_breakdown=explicit_feedback_scope_breakdown or {},
        )
    elif source_context.get("execution_prior_feedback_status"):
        feedback_prior = build_execution_feedback_prior(
            sample_count=int(source_context.get("execution_prior_sample_count") or 0),
            miss_rate=float(_coerce_optional_float(source_context.get("execution_prior_miss_rate")) or 0.0),
            distortion_rate=float(_coerce_optional_float(source_context.get("execution_prior_distortion_rate")) or 0.0),
            resolution_rate=float(_coerce_optional_float(source_context.get("execution_prior_resolution_rate")) or 0.0),
            partial_fill_rate=float(_coerce_optional_float(source_context.get("execution_prior_partial_fill_rate")) or 0.0),
            cancel_rate=float(_coerce_optional_float(source_context.get("execution_prior_cancel_rate")) or 0.0),
            adverse_fill_slippage_bps_p50=_coerce_optional_float(source_context.get("execution_prior_slippage_p50")),
            dominant_miss_reason_bucket=str(source_context.get("execution_prior_dominant_miss_reason_bucket") or "not_submitted"),
            dominant_distortion_reason_bucket=str(
                source_context.get("execution_prior_dominant_distortion_reason_bucket") or "none"
            ),
            cohort_prior_version=str(source_context.get("execution_prior_cohort_prior_version") or "feedback_v1"),
            scope_breakdown={},
        )
    return ExecutionPriorSummary(
        prior_key=prior_key,
        sample_count=int(source_context.get("execution_prior_sample_count") or 0),
        submit_ack_rate=float(source_context.get("execution_prior_submit_ack_rate") or 0.0),
        fill_rate=float(source_context.get("execution_prior_fill_rate") or 0.0),
        resolution_rate=float(source_context.get("execution_prior_resolution_rate") or 0.0),
        partial_fill_rate=float(source_context.get("execution_prior_partial_fill_rate") or 0.0),
        cancel_rate=float(source_context.get("execution_prior_cancel_rate") or 0.0),
        adverse_fill_slippage_bps_p50=_coerce_optional_float(source_context.get("execution_prior_slippage_p50")),
        adverse_fill_slippage_bps_p90=_coerce_optional_float(source_context.get("execution_prior_slippage_p90")),
        submit_latency_ms_p50=_coerce_optional_float(source_context.get("execution_prior_submit_latency_ms_p50")),
        submit_latency_ms_p90=_coerce_optional_float(source_context.get("execution_prior_submit_latency_ms_p90")),
        fill_latency_ms_p50=_coerce_optional_float(source_context.get("execution_prior_fill_latency_ms_p50")),
        fill_latency_ms_p90=_coerce_optional_float(source_context.get("execution_prior_fill_latency_ms_p90")),
        realized_edge_retention_bps_p50=_coerce_optional_float(source_context.get("execution_prior_edge_retention_bps_p50")),
        realized_edge_retention_bps_p90=_coerce_optional_float(source_context.get("execution_prior_edge_retention_bps_p90")),
        avg_realized_pnl=_coerce_optional_float(source_context.get("execution_prior_avg_realized_pnl")),
        avg_post_trade_error=_coerce_optional_float(source_context.get("execution_prior_avg_post_trade_error")),
        prior_quality_status=str(source_context.get("execution_prior_quality_status") or "sparse"),
        prior_lookup_mode=str(source_context.get("execution_prior_lookup_mode") or "exact_market"),
        prior_feature_scope=_json_dict(source_context.get("execution_prior_feature_scope")),
        feedback_prior=feedback_prior,
    )


def execution_prior_context_fields(summary: ExecutionPriorSummary | None) -> dict[str, Any]:
    if summary is None:
        return {}
    payload = {
        "execution_prior_key": execution_prior_key_id(summary.prior_key),
        "execution_prior_market_id": summary.prior_key.market_id,
        "execution_prior_strategy_id": summary.prior_key.strategy_id,
        "execution_prior_wallet_id": summary.prior_key.wallet_id,
        "execution_prior_station_id": summary.prior_key.station_id,
        "execution_prior_metric": summary.prior_key.metric,
        "execution_prior_side": summary.prior_key.side,
        "execution_prior_horizon_bucket": summary.prior_key.horizon_bucket,
        "execution_prior_liquidity_bucket": summary.prior_key.liquidity_bucket,
        "execution_prior_market_age_bucket": summary.prior_key.market_age_bucket,
        "execution_prior_hours_to_close_bucket": summary.prior_key.hours_to_close_bucket,
        "execution_prior_calibration_quality_bucket": summary.prior_key.calibration_quality_bucket,
        "execution_prior_source_freshness_bucket": summary.prior_key.source_freshness_bucket,
        "execution_prior_sample_count": summary.sample_count,
        "execution_prior_submit_ack_rate": summary.submit_ack_rate,
        "execution_prior_fill_rate": summary.fill_rate,
        "execution_prior_resolution_rate": summary.resolution_rate,
        "execution_prior_partial_fill_rate": summary.partial_fill_rate,
        "execution_prior_cancel_rate": summary.cancel_rate,
        "execution_prior_slippage_p50": summary.adverse_fill_slippage_bps_p50,
        "execution_prior_slippage_p90": summary.adverse_fill_slippage_bps_p90,
        "execution_prior_submit_latency_ms_p50": summary.submit_latency_ms_p50,
        "execution_prior_submit_latency_ms_p90": summary.submit_latency_ms_p90,
        "execution_prior_fill_latency_ms_p50": summary.fill_latency_ms_p50,
        "execution_prior_fill_latency_ms_p90": summary.fill_latency_ms_p90,
        "execution_prior_edge_retention_bps_p50": summary.realized_edge_retention_bps_p50,
        "execution_prior_edge_retention_bps_p90": summary.realized_edge_retention_bps_p90,
        "execution_prior_avg_realized_pnl": summary.avg_realized_pnl,
        "execution_prior_avg_post_trade_error": summary.avg_post_trade_error,
        "execution_prior_quality_status": summary.prior_quality_status,
        "execution_prior_lookup_mode": summary.prior_lookup_mode,
        "execution_prior_feature_scope": dict(summary.prior_feature_scope or {}),
    }
    if summary.feedback_prior is not None:
        payload.update(
            {
                "execution_prior_feedback_status": summary.feedback_prior.feedback_status,
                "execution_prior_feedback_penalty": summary.feedback_prior.feedback_penalty,
                "execution_prior_cohort_prior_version": summary.feedback_prior.cohort_prior_version,
                "execution_prior_miss_rate": summary.feedback_prior.scope_breakdown.get("miss_rate"),
                "execution_prior_distortion_rate": summary.feedback_prior.scope_breakdown.get("distortion_rate"),
                "execution_prior_dominant_miss_reason_bucket": summary.feedback_prior.dominant_miss_reason_bucket,
                "execution_prior_dominant_distortion_reason_bucket": summary.feedback_prior.dominant_distortion_reason_bucket,
                "execution_prior_feedback_scope_breakdown": summary.feedback_prior.scope_breakdown,
            }
        )
    return payload


def load_execution_prior_summary(
    con,
    *,
    market_id: str,
    side: str,
    forecast_target_time: datetime | None = None,
    observation_date: date | None = None,
    depth_proxy: float | None = None,
    spread_bps: int | None = None,
    strategy_id: str | None = None,
    wallet_id: str | None = None,
    station_id: str | None = None,
    metric: str | None = None,
    market_age_bucket: str | None = None,
    hours_to_close_bucket: str | None = None,
    calibration_quality_bucket: str | None = None,
    source_freshness_bucket: str | None = None,
) -> ExecutionPriorSummary | None:
    if not _table_exists(con, "weather.weather_execution_priors"):
        return None
    key = build_execution_prior_key(
        market_id=market_id,
        side=side,
        forecast_target_time=forecast_target_time,
        observation_date=observation_date,
        depth_proxy=depth_proxy,
        spread_bps=spread_bps,
        strategy_id=strategy_id,
        wallet_id=wallet_id,
        station_id=station_id,
        metric=metric,
        market_age_bucket=market_age_bucket,
        hours_to_close_bucket=hours_to_close_bucket,
        calibration_quality_bucket=calibration_quality_bucket,
        source_freshness_bucket=source_freshness_bucket,
    )
    for horizon_bucket, liquidity_bucket in [
        (key.horizon_bucket, key.liquidity_bucket),
        (key.horizon_bucket, "unknown"),
        ("unknown", key.liquidity_bucket),
        ("unknown", "unknown"),
    ]:
        scope_rows = _load_execution_prior_scope_rows(
            con,
            market_id=key.market_id or market_id,
            side=key.side or side,
            horizon_bucket=horizon_bucket,
            liquidity_bucket=liquidity_bucket,
            strategy_id=strategy_id,
            wallet_id=wallet_id,
            market_age_bucket=key.market_age_bucket,
            hours_to_close_bucket=key.hours_to_close_bucket,
            calibration_quality_bucket=key.calibration_quality_bucket,
            source_freshness_bucket=key.source_freshness_bucket,
        )
        market_summary = scope_rows.get("market")
        if market_summary is not None:
            feedback_prior = aggregate_feedback_priors(
                {
                    "market": market_summary.feedback_prior,
                    "strategy": scope_rows.get("strategy").feedback_prior if scope_rows.get("strategy") is not None else None,
                    "wallet": scope_rows.get("wallet").feedback_prior if scope_rows.get("wallet") is not None else None,
                }
            )
            return ExecutionPriorSummary(
                prior_key=market_summary.prior_key,
                sample_count=market_summary.sample_count,
                submit_ack_rate=market_summary.submit_ack_rate,
                fill_rate=market_summary.fill_rate,
                resolution_rate=market_summary.resolution_rate,
                partial_fill_rate=market_summary.partial_fill_rate,
                cancel_rate=market_summary.cancel_rate,
                adverse_fill_slippage_bps_p50=market_summary.adverse_fill_slippage_bps_p50,
                adverse_fill_slippage_bps_p90=market_summary.adverse_fill_slippage_bps_p90,
                submit_latency_ms_p50=market_summary.submit_latency_ms_p50,
                submit_latency_ms_p90=market_summary.submit_latency_ms_p90,
                fill_latency_ms_p50=market_summary.fill_latency_ms_p50,
                fill_latency_ms_p90=market_summary.fill_latency_ms_p90,
                realized_edge_retention_bps_p50=market_summary.realized_edge_retention_bps_p50,
                realized_edge_retention_bps_p90=market_summary.realized_edge_retention_bps_p90,
                avg_realized_pnl=market_summary.avg_realized_pnl,
                avg_post_trade_error=market_summary.avg_post_trade_error,
                prior_quality_status=market_summary.prior_quality_status,
                prior_lookup_mode="exact_market",
                prior_feature_scope=_build_prior_feature_scope(market_summary.prior_key, lookup_mode="exact_market"),
                feedback_prior=feedback_prior,
            )
        strategy_or_wallet = _best_scope_summary(scope_rows)
        if strategy_or_wallet is not None:
            scope_name, selected_summary = strategy_or_wallet
            feedback_prior = aggregate_feedback_priors(
                {
                    "strategy": scope_rows.get("strategy").feedback_prior if scope_rows.get("strategy") is not None else None,
                    "wallet": scope_rows.get("wallet").feedback_prior if scope_rows.get("wallet") is not None else None,
                }
            )
            return ExecutionPriorSummary(
                prior_key=selected_summary.prior_key,
                sample_count=selected_summary.sample_count,
                submit_ack_rate=selected_summary.submit_ack_rate,
                fill_rate=selected_summary.fill_rate,
                resolution_rate=selected_summary.resolution_rate,
                partial_fill_rate=selected_summary.partial_fill_rate,
                cancel_rate=selected_summary.cancel_rate,
                adverse_fill_slippage_bps_p50=selected_summary.adverse_fill_slippage_bps_p50,
                adverse_fill_slippage_bps_p90=selected_summary.adverse_fill_slippage_bps_p90,
                submit_latency_ms_p50=selected_summary.submit_latency_ms_p50,
                submit_latency_ms_p90=selected_summary.submit_latency_ms_p90,
                fill_latency_ms_p50=selected_summary.fill_latency_ms_p50,
                fill_latency_ms_p90=selected_summary.fill_latency_ms_p90,
                realized_edge_retention_bps_p50=selected_summary.realized_edge_retention_bps_p50,
                realized_edge_retention_bps_p90=selected_summary.realized_edge_retention_bps_p90,
                avg_realized_pnl=selected_summary.avg_realized_pnl,
                avg_post_trade_error=selected_summary.avg_post_trade_error,
                prior_quality_status=selected_summary.prior_quality_status,
                prior_lookup_mode=f"exact_{scope_name}",
                prior_feature_scope=_build_prior_feature_scope(selected_summary.prior_key, lookup_mode=f"exact_{scope_name}"),
                feedback_prior=feedback_prior,
            )
        station_metric_summary = None
        if station_id and metric:
            for horizon_bucket, liquidity_bucket in [
                (horizon_bucket, liquidity_bucket),
            ]:
                station_metric_summary = _load_station_metric_fallback_summary(
                    con,
                    station_id=str(station_id),
                    metric=str(metric),
                    side=key.side or side,
                    horizon_bucket=horizon_bucket,
                    liquidity_bucket=liquidity_bucket,
                    market_age_bucket=key.market_age_bucket,
                    hours_to_close_bucket=key.hours_to_close_bucket,
                    calibration_quality_bucket=key.calibration_quality_bucket,
                    source_freshness_bucket=key.source_freshness_bucket,
                )
                if station_metric_summary is not None:
                    return station_metric_summary
    if station_id and metric:
        for horizon_bucket, liquidity_bucket in [
            (key.horizon_bucket, key.liquidity_bucket),
            (key.horizon_bucket, "unknown"),
            ("unknown", key.liquidity_bucket),
            ("unknown", "unknown"),
        ]:
            summary = _load_station_metric_fallback_summary(
                con,
                station_id=str(station_id),
                metric=str(metric),
                side=key.side or side,
                horizon_bucket=horizon_bucket,
                liquidity_bucket=liquidity_bucket,
                market_age_bucket=key.market_age_bucket,
                hours_to_close_bucket=key.hours_to_close_bucket,
                calibration_quality_bucket=key.calibration_quality_bucket,
                source_freshness_bucket=key.source_freshness_bucket,
            )
            if summary is not None:
                return summary
    return None


def materialize_execution_priors(
    con,
    *,
    as_of: datetime | None = None,
    lookback_days: int = 90,
    materialization_id: str | None = None,
    prior_version: str = "feedback_v1",
) -> list[ExecutionPriorRow]:
    materialized_at = _normalize_datetime(as_of) if as_of is not None else datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    window_end = materialized_at.replace(tzinfo=None)
    window_start = (materialized_at - timedelta(days=max(1, int(lookback_days)))).replace(tzinfo=None)

    tickets = con.execute(
        """
        SELECT
            ticket_id,
            strategy_id,
            wallet_id,
            market_id,
            forecast_run_id,
            outcome,
            side,
            reference_price,
            size,
            provenance_json,
            created_at
        FROM runtime.trade_tickets
        WHERE created_at >= ?
        """,
        [window_start],
    ).fetchdf()
    if tickets.empty:
        return []

    submit_attempts = con.execute(
        """
        SELECT ticket_id, order_id, attempt_id, attempt_kind, status, created_at
        FROM runtime.submit_attempts
        """
    ).fetchdf()
    submit_order_attempts = submit_attempts[submit_attempts["attempt_kind"] == "submit_order"] if not submit_attempts.empty else submit_attempts
    sign_attempts = submit_attempts[submit_attempts["attempt_kind"] == "sign_order"] if not submit_attempts.empty else submit_attempts
    latest_submit_by_ticket = _latest_status_by_ticket(submit_order_attempts)
    latest_submit_order_by_ticket = _latest_value_by_ticket(submit_order_attempts, value_col="order_id")
    latest_submit_created_at_by_ticket = _latest_value_by_ticket(submit_order_attempts, value_col="created_at")
    latest_submit_attempt_id_by_ticket = _latest_value_by_ticket(submit_order_attempts, value_col="attempt_id")
    latest_sign_attempt_id_by_ticket = _latest_value_by_ticket(sign_attempts, value_col="attempt_id")

    external_orders = con.execute(
        """
        SELECT ticket_id, order_id, external_status AS status, observed_at
        FROM runtime.external_order_observations
        """
    ).fetchdf()
    latest_external_order_by_ticket = _latest_status_by_ticket(external_orders, status_col="status", ts_col="observed_at")
    latest_external_order_id_by_ticket = _latest_value_by_ticket(external_orders, value_col="order_id", ts_col="observed_at")

    orders = con.execute(
        """
        SELECT order_id, status, submitted_at
        FROM trading.orders
        """
    ).fetchdf()
    order_status_by_id = {str(row["order_id"]): str(row["status"]).lower() for _, row in orders.iterrows()}
    order_submitted_at_by_id = {
        str(row["order_id"]): _coerce_optional_datetime(row["submitted_at"])
        for _, row in orders.iterrows()
        if row["order_id"] is not None
    }

    fills = con.execute(
        """
        SELECT
            order_id,
            SUM(size) AS filled_size,
            SUM(price * size) / NULLIF(SUM(size), 0) AS avg_fill_price,
            MIN(filled_at) AS first_fill_at,
            SUM(fee) AS total_fee
        FROM trading.fills
        GROUP BY order_id
        """
    ).fetchdf()
    fills_by_order = {
        str(row["order_id"]): {
            "filled_size": float(row["filled_size"] or 0.0),
            "avg_fill_price": _coerce_optional_float(row["avg_fill_price"]),
            "first_fill_at": _coerce_optional_datetime(row["first_fill_at"]),
            "total_fee": _coerce_optional_float(row["total_fee"]),
        }
        for _, row in fills.iterrows()
    }

    latest_gate_allowed_by_ticket: dict[str, bool | None] = {}
    if _table_exists(con, "runtime.gate_decisions"):
        gate_frame = con.execute(
            """
            SELECT ticket_id, allowed, created_at
            FROM runtime.gate_decisions
            """
        ).fetchdf()
        latest_gate_allowed_by_ticket = {
            key: (bool(value) if value is not None else None)
            for key, value in _latest_value_by_ticket(gate_frame, value_col="allowed").items()
        }

    market_outcome_resolution = {}
    if _table_exists(con, "resolution.settlement_verifications"):
        resolution_rows = con.execute(
            """
            SELECT market_id, expected_outcome, created_at
            FROM resolution.settlement_verifications
            QUALIFY ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY created_at DESC, verification_id DESC) = 1
            """
        ).fetchdf()
        market_outcome_resolution = {
            str(row["market_id"]): str(row["expected_outcome"])
            for _, row in resolution_rows.iterrows()
            if row["market_id"] is not None and row["expected_outcome"] is not None
        }

    market_observation_dates = {}
    market_station_ids = {}
    market_metrics = {}
    forecast_target_times_by_run_id = {}
    if _table_exists(con, "weather.weather_market_specs"):
        spec_rows = con.execute("SELECT market_id, station_id, observation_date, metric FROM weather.weather_market_specs").fetchdf()
        market_observation_dates = {
            str(row["market_id"]): row["observation_date"]
            for _, row in spec_rows.iterrows()
            if row["market_id"] is not None
        }
        market_station_ids = {
            str(row["market_id"]): _coerce_optional_text(row["station_id"])
            for _, row in spec_rows.iterrows()
            if row["market_id"] is not None
        }
    if _table_exists(con, "weather.weather_forecast_runs"):
        forecast_rows = con.execute("SELECT run_id, forecast_target_time FROM weather.weather_forecast_runs").fetchdf()
        forecast_target_times_by_run_id = {
            str(row["run_id"]): _coerce_optional_datetime(row["forecast_target_time"])
            for _, row in forecast_rows.iterrows()
            if row["run_id"] is not None
        }
        market_metrics = {
            str(row["market_id"]): _coerce_optional_text(row["metric"])
            for _, row in spec_rows.iterrows()
            if row["market_id"] is not None
        }

    market_created_ats: dict[str, datetime | None] = {}
    market_close_times: dict[str, datetime | None] = {}
    if _table_exists(con, "weather.weather_markets"):
        market_rows = con.execute("SELECT market_id, created_at, close_time FROM weather.weather_markets").fetchdf()
        market_created_ats = {
            str(row["market_id"]): _coerce_optional_datetime(row["created_at"])
            for _, row in market_rows.iterrows()
            if row["market_id"] is not None
        }
        market_close_times = {
            str(row["market_id"]): _coerce_optional_datetime(row["close_time"])
            for _, row in market_rows.iterrows()
            if row["market_id"] is not None
        }

    grouped: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for _, ticket in tickets.iterrows():
        provenance = _json_dict(ticket.get("provenance_json"))
        pricing_context = _json_dict((provenance.get("pricing_context") or {}))
        side = str(ticket.get("side") or "").upper()
        market_id = str(ticket.get("market_id"))
        forecast_target_time = _coerce_optional_datetime(
            pricing_context.get("forecast_target_time")
            or provenance.get("forecast_target_time")
            or pricing_context.get("latest_forecast_target_time")
        )
        if forecast_target_time is None:
            forecast_target_time = forecast_target_times_by_run_id.get(str(ticket.get("forecast_run_id") or ""))
        observation_date = _coerce_optional_date(market_observation_dates.get(market_id))
        prior_key = build_execution_prior_key(
            market_id=market_id,
            side=side,
            forecast_target_time=forecast_target_time,
            observation_date=observation_date,
            depth_proxy=_coerce_optional_float(pricing_context.get("depth_proxy")),
            spread_bps=_coerce_optional_int(pricing_context.get("spread_bps")),
            station_id=market_station_ids.get(market_id),
            metric=market_metrics.get(market_id),
            market_age_bucket=_market_age_bucket(
                ticket_created_at=_coerce_optional_datetime(ticket.get("created_at")),
                market_created_at=market_created_ats.get(market_id),
            ),
            hours_to_close_bucket=_hours_to_close_bucket(
                ticket_created_at=_coerce_optional_datetime(ticket.get("created_at")),
                market_close_time=market_close_times.get(market_id),
            ),
            calibration_quality_bucket=_calibration_quality_bucket(pricing_context),
            source_freshness_bucket=_source_freshness_bucket(pricing_context),
        )
        sample = _ticket_prior_sample(
            ticket=ticket,
            prior_key=prior_key,
            latest_submit_by_ticket=latest_submit_by_ticket,
            latest_submit_order_by_ticket=latest_submit_order_by_ticket,
            latest_submit_created_at_by_ticket=latest_submit_created_at_by_ticket,
            latest_submit_attempt_id_by_ticket=latest_submit_attempt_id_by_ticket,
            latest_sign_attempt_id_by_ticket=latest_sign_attempt_id_by_ticket,
            latest_external_order_by_ticket=latest_external_order_by_ticket,
            latest_external_order_id_by_ticket=latest_external_order_id_by_ticket,
            latest_gate_allowed_by_ticket=latest_gate_allowed_by_ticket,
            order_status_by_id=order_status_by_id,
            order_submitted_at_by_id=order_submitted_at_by_id,
            fills_by_order=fills_by_order,
            market_outcome_resolution=market_outcome_resolution,
            pricing_context=pricing_context,
        )
        strategy_id = _coerce_optional_text(ticket.get("strategy_id"))
        wallet_id_value = _coerce_optional_text(ticket.get("wallet_id"))
        for cohort_type, cohort_key in [
            ("market", market_id),
            ("strategy", strategy_id),
            ("wallet", wallet_id_value),
        ]:
            if cohort_key is None:
                continue
            grouped[
                (
                    cohort_type,
                    str(cohort_key),
                    market_id,
                    side,
                    prior_key.horizon_bucket or "unknown",
                    prior_key.liquidity_bucket or "unknown",
                    prior_key.market_age_bucket or "unknown",
                    prior_key.hours_to_close_bucket or "unknown",
                    prior_key.calibration_quality_bucket or "sparse_or_missing",
                    prior_key.source_freshness_bucket or "degraded_or_missing",
                )
            ].append(sample)

    rows: list[ExecutionPriorRow] = []
    for group_key, samples in grouped.items():
        if not samples:
            continue
        cohort_type, cohort_key = group_key[0], group_key[1]
        first = samples[0]
        prior_key = first["prior_key_obj"]
        slippages = [float(item["adverse_fill_slippage_bps"]) for item in samples if item["adverse_fill_slippage_bps"] is not None]
        submit_latencies = [float(item["submit_latency_ms"]) for item in samples if item["submit_latency_ms"] is not None]
        fill_latencies = [float(item["fill_latency_ms"]) for item in samples if item["fill_latency_ms"] is not None]
        edge_retentions = [float(item["realized_edge_retention_bps"]) for item in samples if item["realized_edge_retention_bps"] is not None]
        realized_pnls = [float(item["realized_pnl"]) for item in samples if item["realized_pnl"] is not None]
        post_trade_errors = [float(item["post_trade_error"]) for item in samples if item["post_trade_error"] is not None]
        sample_count = len(samples)
        miss_rate = round(max(0.0, 1.0 - _mean(item["filled"] for item in samples)), 6)
        distortion_rate = round(_mean(item["distorted"] for item in samples), 6)
        dominant_miss_reason_bucket = _dominant_bucket(
            [str(item["miss_reason_bucket"]) for item in samples if item.get("miss_reason_bucket")],
            priority={**_MARKET_MISS_PRIORITY, "not_submitted": 3},
            default="not_submitted",
        )
        dominant_distortion_reason_bucket = _dominant_bucket(
            [str(item["distortion_reason_bucket"]) for item in samples if item.get("distortion_reason_bucket")],
            priority={"execution_distortion": 0, "forecast_distortion": 1, "ranking_distortion": 2, "none": 3},
            default="none",
        )
        feedback_prior = build_execution_feedback_prior(
            sample_count=sample_count,
            miss_rate=miss_rate,
            distortion_rate=distortion_rate,
            resolution_rate=round(_mean(item["resolved"] for item in samples), 6),
            partial_fill_rate=round(_mean(item["partial_fill"] for item in samples), 6),
            cancel_rate=round(_mean(item["cancelled"] for item in samples), 6),
            adverse_fill_slippage_bps_p50=_percentile(slippages, 0.50),
            dominant_miss_reason_bucket=dominant_miss_reason_bucket,
            dominant_distortion_reason_bucket=dominant_distortion_reason_bucket,
            cohort_prior_version=prior_version,
            scope_breakdown={
                "sample_count": sample_count,
                "miss_rate": miss_rate,
                "distortion_rate": distortion_rate,
            },
        )
        if feedback_prior.feedback_status == "missing":
            continue
        rows.append(
            ExecutionPriorRow(
                prior_key=execution_prior_row_id(
                    prior_key=prior_key,
                    cohort_type=cohort_type,
                    cohort_key=str(cohort_key),
                ),
                market_id=str(prior_key.market_id),
                strategy_id=None if cohort_type != "strategy" else str(cohort_key),
                wallet_id=None if cohort_type != "wallet" else str(cohort_key),
                station_id=prior_key.station_id,
                metric=prior_key.metric,
                side=str(prior_key.side),
                horizon_bucket=str(prior_key.horizon_bucket or "unknown"),
                liquidity_bucket=str(prior_key.liquidity_bucket or "unknown"),
                market_age_bucket=str(prior_key.market_age_bucket or "unknown"),
                hours_to_close_bucket=str(prior_key.hours_to_close_bucket or "unknown"),
                calibration_quality_bucket=str(prior_key.calibration_quality_bucket or "sparse_or_missing"),
                source_freshness_bucket=str(prior_key.source_freshness_bucket or "degraded_or_missing"),
                sample_count=sample_count,
                submit_ack_rate=round(_mean(item["submit_ack"] for item in samples), 6),
                fill_rate=round(_mean(item["filled"] for item in samples), 6),
                resolution_rate=round(_mean(item["resolved"] for item in samples), 6),
                partial_fill_rate=round(_mean(item["partial_fill"] for item in samples), 6),
                cancel_rate=round(_mean(item["cancelled"] for item in samples), 6),
                adverse_fill_slippage_bps_p50=_percentile(slippages, 0.50),
                adverse_fill_slippage_bps_p90=_percentile(slippages, 0.90),
                submit_latency_ms_p50=_percentile(submit_latencies, 0.50),
                submit_latency_ms_p90=_percentile(submit_latencies, 0.90),
                fill_latency_ms_p50=_percentile(fill_latencies, 0.50),
                fill_latency_ms_p90=_percentile(fill_latencies, 0.90),
                realized_edge_retention_bps_p50=_percentile(edge_retentions, 0.50),
                realized_edge_retention_bps_p90=_percentile(edge_retentions, 0.90),
                avg_realized_pnl=round(mean(realized_pnls), 6) if realized_pnls else None,
                avg_post_trade_error=round(mean(post_trade_errors), 6) if post_trade_errors else None,
                prior_quality_status="ready" if sample_count >= 10 else "sparse",
                cohort_type=cohort_type,
                cohort_key=str(cohort_key),
                feedback_status=feedback_prior.feedback_status,
                feedback_penalty=feedback_prior.feedback_penalty,
                cohort_prior_version=feedback_prior.cohort_prior_version,
                miss_rate=miss_rate,
                distortion_rate=distortion_rate,
                dominant_miss_reason_bucket=dominant_miss_reason_bucket,
                dominant_distortion_reason_bucket=dominant_distortion_reason_bucket,
                last_feedback_materialization_id=materialization_id,
                source_window_start=window_start,
                source_window_end=window_end,
                materialized_at=window_end,
            )
        )
    return rows


def enqueue_execution_prior_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    priors: list[ExecutionPriorRow],
    run_id: str | None = None,
) -> str | None:
    if not priors:
        return None
    rows = [execution_prior_row_to_row(item) for item in priors]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_execution_priors",
        pk_cols=["prior_key"],
        columns=list(WEATHER_EXECUTION_PRIOR_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def execution_prior_row_to_row(record: ExecutionPriorRow) -> list[Any]:
    return [
        record.prior_key,
        record.market_id,
        record.strategy_id,
        record.wallet_id,
        record.station_id,
        record.metric,
        record.side,
        record.horizon_bucket,
        record.liquidity_bucket,
        record.market_age_bucket,
        record.hours_to_close_bucket,
        record.calibration_quality_bucket,
        record.source_freshness_bucket,
        record.sample_count,
        record.submit_ack_rate,
        record.fill_rate,
        record.resolution_rate,
        record.partial_fill_rate,
        record.cancel_rate,
        record.adverse_fill_slippage_bps_p50,
        record.adverse_fill_slippage_bps_p90,
        record.submit_latency_ms_p50,
        record.submit_latency_ms_p90,
        record.fill_latency_ms_p50,
        record.fill_latency_ms_p90,
        record.realized_edge_retention_bps_p50,
        record.realized_edge_retention_bps_p90,
        record.avg_realized_pnl,
        record.avg_post_trade_error,
        record.prior_quality_status,
        record.cohort_type,
        record.cohort_key,
        record.feedback_status,
        record.feedback_penalty,
        record.cohort_prior_version,
        record.miss_rate,
        record.distortion_rate,
        record.dominant_miss_reason_bucket,
        record.dominant_distortion_reason_bucket,
        record.last_feedback_materialization_id,
        _sql_timestamp(record.source_window_start),
        _sql_timestamp(record.source_window_end),
        _sql_timestamp(record.materialized_at),
    ]


def _ticket_prior_sample(
    *,
    ticket,
    prior_key: ExecutionPriorKey,
    latest_submit_by_ticket: dict[str, str],
    latest_submit_order_by_ticket: dict[str, str],
    latest_submit_created_at_by_ticket: dict[str, Any],
    latest_submit_attempt_id_by_ticket: dict[str, str],
    latest_sign_attempt_id_by_ticket: dict[str, str],
    latest_external_order_by_ticket: dict[str, str],
    latest_external_order_id_by_ticket: dict[str, str],
    latest_gate_allowed_by_ticket: dict[str, bool | None],
    order_status_by_id: dict[str, str],
    order_submitted_at_by_id: dict[str, datetime | None],
    fills_by_order: dict[str, dict[str, float | None]],
    market_outcome_resolution: dict[str, str],
    pricing_context: dict[str, Any],
) -> dict[str, Any]:
    ticket_id = str(ticket["ticket_id"])
    order_id = (
        _coerce_optional_text(latest_submit_order_by_ticket.get(ticket_id))
        or _coerce_optional_text(latest_external_order_id_by_ticket.get(ticket_id))
    )
    latest_submit_status = latest_submit_by_ticket.get(ticket_id)
    external_order_status = latest_external_order_by_ticket.get(ticket_id)
    order_status = order_status_by_id.get(order_id or "")
    fill = fills_by_order.get(order_id or "", {"filled_size": 0.0, "avg_fill_price": None, "total_fee": 0.0})
    filled_size = float(fill["filled_size"] or 0.0)
    size = max(0.0, float(ticket.get("size") or 0.0))
    avg_fill_price = _coerce_optional_float(fill.get("avg_fill_price"))
    total_fee = _coerce_optional_float(fill.get("total_fee")) or 0.0
    ticket_created_at = _coerce_optional_datetime(ticket.get("created_at"))
    latest_submit_created_at = _coerce_optional_datetime(latest_submit_created_at_by_ticket.get(ticket_id))
    order_submitted_at = order_submitted_at_by_id.get(order_id or "")
    first_fill_at = _coerce_optional_datetime(fill.get("first_fill_at"))
    reference_price = float(ticket.get("reference_price") or pricing_context.get("reference_price") or 0.0)
    side = str(ticket.get("side") or "").upper()
    submit_ack = _is_submit_ack(latest_submit_status, external_order_status)
    filled = 1.0 if filled_size > 0 else 0.0
    partial_fill = 1.0 if size > 0 and 0.0 < filled_size < size else 0.0
    cancelled = 1.0 if (order_status or "").lower() in _CANCELLED_ORDER_STATUSES else 0.0
    resolved_outcome = market_outcome_resolution.get(str(ticket.get("market_id")))
    resolved = 1.0 if resolved_outcome else 0.0
    submit_latency_ms = _latency_ms(ticket_created_at, latest_submit_created_at or order_submitted_at)
    fill_latency_ms = _latency_ms(latest_submit_created_at or order_submitted_at or ticket_created_at, first_fill_at)
    edge_bps = _coerce_optional_float(pricing_context.get("edge_bps_executable"))
    realized_edge_retention_bps = None
    projection = build_resolved_execution_projection(
        outcome=ticket.get("outcome"),
        side=side,
        expected_outcome=resolved_outcome,
        filled_quantity=filled_size,
        ticket_size=size,
        expected_fill_price=reference_price,
        realized_fill_price=avg_fill_price,
        total_fee=total_fee,
        predicted_edge_bps=edge_bps,
        execution_result=None,
        order_status=order_status,
        latest_submit_status=latest_submit_status,
        live_prereq_execution_status=None,
        external_order_status=external_order_status,
        gate_allowed=latest_gate_allowed_by_ticket.get(ticket_id),
        latest_sign_attempt_id=latest_sign_attempt_id_by_ticket.get(ticket_id),
        latest_submit_attempt_id=latest_submit_attempt_id_by_ticket.get(ticket_id),
        latest_fill_at=first_fill_at,
        latest_resolution_at=None,
    )
    if projection.realized_pnl is not None and edge_bps is not None:
        realized_edge_retention_bps = min(max(float(projection.realized_pnl) * 10_000.0, 0.0), abs(float(edge_bps)))
    miss_reason_bucket = _miss_reason_bucket_for_stage(projection.execution_lifecycle_stage)
    distortion_reason_bucket = _distortion_reason_bucket(
        stage=projection.execution_lifecycle_stage,
        realized_pnl=projection.realized_pnl,
        adverse_fill_slippage_bps=projection.adverse_fill_slippage_bps,
    )

    return {
        "prior_key_obj": prior_key,
        "submit_ack": submit_ack,
        "filled": filled,
        "resolved": resolved,
        "partial_fill": partial_fill,
        "cancelled": cancelled,
        "adverse_fill_slippage_bps": projection.adverse_fill_slippage_bps,
        "submit_latency_ms": submit_latency_ms,
        "fill_latency_ms": fill_latency_ms,
        "realized_edge_retention_bps": realized_edge_retention_bps,
        "realized_pnl": projection.realized_pnl,
        "post_trade_error": projection.post_trade_error,
        "miss_reason_bucket": miss_reason_bucket,
        "distortion_reason_bucket": distortion_reason_bucket,
        "distorted": 0.0 if distortion_reason_bucket == "none" else 1.0,
    }


def _load_execution_prior_scope_rows(
    con,
    *,
    market_id: str,
    side: str,
    horizon_bucket: str,
    liquidity_bucket: str,
    strategy_id: str | None,
    wallet_id: str | None,
    market_age_bucket: str | None,
    hours_to_close_bucket: str | None,
    calibration_quality_bucket: str | None,
    source_freshness_bucket: str | None,
) -> dict[str, ExecutionPriorSummary]:
    base_query = """
        SELECT
            market_id,
            strategy_id,
            wallet_id,
            station_id,
            metric,
            side,
            horizon_bucket,
            liquidity_bucket,
            market_age_bucket,
            hours_to_close_bucket,
            calibration_quality_bucket,
            source_freshness_bucket,
            sample_count,
            submit_ack_rate,
            fill_rate,
            resolution_rate,
            partial_fill_rate,
            cancel_rate,
            adverse_fill_slippage_bps_p50,
            adverse_fill_slippage_bps_p90,
            submit_latency_ms_p50,
            submit_latency_ms_p90,
            fill_latency_ms_p50,
            fill_latency_ms_p90,
            realized_edge_retention_bps_p50,
            realized_edge_retention_bps_p90,
            avg_realized_pnl,
            avg_post_trade_error,
            prior_quality_status,
            cohort_type,
            cohort_key,
            feedback_status,
            feedback_penalty,
            cohort_prior_version,
            miss_rate,
            distortion_rate,
            dominant_miss_reason_bucket,
            dominant_distortion_reason_bucket,
            materialized_at
        FROM weather.weather_execution_priors
        WHERE side = ?
          AND horizon_bucket = ?
          AND liquidity_bucket = ?
          AND cohort_type = ?
          AND cohort_key = ?
    """
    scopes = {
        "market": str(market_id),
        "strategy": _coerce_optional_text(strategy_id),
        "wallet": _coerce_optional_text(wallet_id),
    }
    rows: dict[str, ExecutionPriorSummary] = {}
    for scope, cohort_key in scopes.items():
        if cohort_key is None:
            continue
        query = base_query
        params: list[Any] = [side, horizon_bucket, liquidity_bucket, scope, cohort_key]
        if scope == "market":
            query += " AND market_id = ?"
            params.append(market_id)
        query += " ORDER BY materialized_at DESC"
        rows_frame = con.execute(
            query,
            params,
        ).fetchdf()
        if rows_frame.empty:
            continue
        row = _select_best_prior_row(
            rows_frame,
            market_age_bucket=market_age_bucket,
            hours_to_close_bucket=hours_to_close_bucket,
            calibration_quality_bucket=calibration_quality_bucket,
            source_freshness_bucket=source_freshness_bucket,
        )
        if row is None:
            continue
        row_key = ExecutionPriorKey(
            market_id=str(row["market_id"]),
            strategy_id=_coerce_optional_text(row["strategy_id"]),
            wallet_id=_coerce_optional_text(row["wallet_id"]),
            station_id=_coerce_optional_text(row["station_id"]),
            metric=_coerce_optional_text(row["metric"]),
            side=str(row["side"]),
            horizon_bucket=str(row["horizon_bucket"]),
            liquidity_bucket=str(row["liquidity_bucket"]),
            market_age_bucket=_coerce_optional_text(row["market_age_bucket"]),
            hours_to_close_bucket=_coerce_optional_text(row["hours_to_close_bucket"]),
            calibration_quality_bucket=_coerce_optional_text(row["calibration_quality_bucket"]),
            source_freshness_bucket=_coerce_optional_text(row["source_freshness_bucket"]),
        )
        rows[scope] = ExecutionPriorSummary(
            prior_key=row_key,
            sample_count=int(row["sample_count"]),
            submit_ack_rate=float(row["submit_ack_rate"]),
            fill_rate=float(row["fill_rate"]),
            resolution_rate=float(row["resolution_rate"]),
            partial_fill_rate=float(row["partial_fill_rate"]),
            cancel_rate=float(row["cancel_rate"]),
            adverse_fill_slippage_bps_p50=_coerce_optional_float(row["adverse_fill_slippage_bps_p50"]),
            adverse_fill_slippage_bps_p90=_coerce_optional_float(row["adverse_fill_slippage_bps_p90"]),
            submit_latency_ms_p50=_coerce_optional_float(row["submit_latency_ms_p50"]),
            submit_latency_ms_p90=_coerce_optional_float(row["submit_latency_ms_p90"]),
            fill_latency_ms_p50=_coerce_optional_float(row["fill_latency_ms_p50"]),
            fill_latency_ms_p90=_coerce_optional_float(row["fill_latency_ms_p90"]),
            realized_edge_retention_bps_p50=_coerce_optional_float(row["realized_edge_retention_bps_p50"]),
            realized_edge_retention_bps_p90=_coerce_optional_float(row["realized_edge_retention_bps_p90"]),
            avg_realized_pnl=_coerce_optional_float(row["avg_realized_pnl"]),
            avg_post_trade_error=_coerce_optional_float(row["avg_post_trade_error"]),
            prior_quality_status=str(row["prior_quality_status"]),
            prior_lookup_mode="exact_market",
            prior_feature_scope=_build_prior_feature_scope(row_key, lookup_mode="exact_market"),
            feedback_prior=_build_feedback_prior_from_persisted_row(row),
        )
    return rows


def _build_feedback_prior_from_persisted_row(row: pd.Series) -> ExecutionFeedbackPrior:
    sample_count = int(row["sample_count"])
    feedback_penalty = _coerce_optional_float(row["feedback_penalty"])
    normalized_penalty = max(0.0, min(1.0, float(feedback_penalty or 0.0)))
    normalized_status = str(row["feedback_status"] or "").strip().lower()
    if normalized_status not in {"heuristic_only", "ready", "watch", "sparse", "degraded", "missing"}:
        normalized_status = execution_feedback_status(
            sample_count=sample_count,
            feedback_penalty=normalized_penalty,
        )
    return ExecutionFeedbackPrior(
        feedback_penalty=normalized_penalty,
        feedback_status=normalized_status or "heuristic_only",
        cohort_prior_version=str(row["cohort_prior_version"] or "feedback_v1"),
        dominant_miss_reason_bucket=str(row["dominant_miss_reason_bucket"] or "not_submitted"),
        dominant_distortion_reason_bucket=str(row["dominant_distortion_reason_bucket"] or "none"),
        scope_breakdown={
            "cohort_type": str(row["cohort_type"]),
            "cohort_key": str(row["cohort_key"]),
            "sample_count": sample_count,
            "miss_rate": float(row["miss_rate"] or 0.0),
            "distortion_rate": float(row["distortion_rate"] or 0.0),
            "feedback_status": normalized_status or "heuristic_only",
            "feedback_penalty": normalized_penalty,
        },
    )


def _load_station_metric_fallback_summary(
    con,
    *,
    station_id: str,
    metric: str,
    side: str,
    horizon_bucket: str,
    liquidity_bucket: str,
    market_age_bucket: str | None,
    hours_to_close_bucket: str | None,
    calibration_quality_bucket: str | None,
    source_freshness_bucket: str | None,
) -> ExecutionPriorSummary | None:
    frame = con.execute(
        """
        SELECT
            sample_count,
            submit_ack_rate,
            fill_rate,
            resolution_rate,
            partial_fill_rate,
            cancel_rate,
            adverse_fill_slippage_bps_p50,
            adverse_fill_slippage_bps_p90,
            submit_latency_ms_p50,
            submit_latency_ms_p90,
            fill_latency_ms_p50,
            fill_latency_ms_p90,
            realized_edge_retention_bps_p50,
            realized_edge_retention_bps_p90,
            avg_realized_pnl,
            avg_post_trade_error,
            miss_rate,
            distortion_rate,
            dominant_miss_reason_bucket,
            dominant_distortion_reason_bucket,
            feedback_status,
            feedback_penalty,
            cohort_prior_version,
            market_id
        FROM weather.weather_execution_priors
        WHERE station_id = ?
          AND metric = ?
          AND side = ?
          AND horizon_bucket = ?
          AND liquidity_bucket = ?
          AND cohort_type = 'market'
        """,
        [station_id, metric, side, horizon_bucket, liquidity_bucket],
    ).fetchdf()
    if frame.empty:
        return None
    frame = _filter_prior_rows_by_feature_buckets(
        frame,
        market_age_bucket=market_age_bucket,
        hours_to_close_bucket=hours_to_close_bucket,
        calibration_quality_bucket=calibration_quality_bucket,
        source_freshness_bucket=source_freshness_bucket,
    )
    if frame.empty:
        return None
    sample_count = int(frame["sample_count"].fillna(0).sum())
    if sample_count <= 0:
        return None
    feature_scope = _build_prior_feature_scope(
        ExecutionPriorKey(
            market_id=None,
            strategy_id=None,
            wallet_id=None,
            station_id=station_id,
            metric=metric,
            side=side,
            horizon_bucket=horizon_bucket,
            liquidity_bucket=liquidity_bucket,
            market_age_bucket=market_age_bucket,
            hours_to_close_bucket=hours_to_close_bucket,
            calibration_quality_bucket=calibration_quality_bucket,
            source_freshness_bucket=source_freshness_bucket,
        ),
        lookup_mode="station_metric_fallback",
        matched_market_count=int(frame["market_id"].fillna("").astype(str).replace("", pd.NA).dropna().nunique()),
    )
    dominant_miss_reason_bucket = _dominant_bucket(
        [str(item) for item in frame["dominant_miss_reason_bucket"].dropna().tolist()],
        priority={**_MARKET_MISS_PRIORITY, "not_submitted": 3},
        default="not_submitted",
    )
    dominant_distortion_reason_bucket = _dominant_bucket(
        [str(item) for item in frame["dominant_distortion_reason_bucket"].dropna().tolist()],
        priority={"execution_distortion": 0, "forecast_distortion": 1, "ranking_distortion": 2, "none": 3},
        default="none",
    )
    feedback_prior = build_execution_feedback_prior(
        sample_count=sample_count,
        miss_rate=_weighted_mean(frame, "miss_rate"),
        distortion_rate=_weighted_mean(frame, "distortion_rate"),
        resolution_rate=_weighted_mean(frame, "resolution_rate"),
        partial_fill_rate=_weighted_mean(frame, "partial_fill_rate"),
        cancel_rate=_weighted_mean(frame, "cancel_rate"),
        adverse_fill_slippage_bps_p50=_weighted_mean_optional(frame, "adverse_fill_slippage_bps_p50"),
        dominant_miss_reason_bucket=dominant_miss_reason_bucket,
        dominant_distortion_reason_bucket=dominant_distortion_reason_bucket,
        cohort_prior_version="feedback_v1",
        scope_breakdown={
            "cohort_type": "station_metric_fallback",
            "sample_count": sample_count,
            "matched_market_count": feature_scope["matched_market_count"],
        },
    )
    return ExecutionPriorSummary(
        prior_key=ExecutionPriorKey(
            market_id=None,
            strategy_id=None,
            wallet_id=None,
            station_id=station_id,
            metric=metric,
            side=side,
            horizon_bucket=horizon_bucket,
            liquidity_bucket=liquidity_bucket,
            market_age_bucket=market_age_bucket,
            hours_to_close_bucket=hours_to_close_bucket,
            calibration_quality_bucket=calibration_quality_bucket,
            source_freshness_bucket=source_freshness_bucket,
        ),
        sample_count=sample_count,
        submit_ack_rate=_weighted_mean(frame, "submit_ack_rate"),
        fill_rate=_weighted_mean(frame, "fill_rate"),
        resolution_rate=_weighted_mean(frame, "resolution_rate"),
        partial_fill_rate=_weighted_mean(frame, "partial_fill_rate"),
        cancel_rate=_weighted_mean(frame, "cancel_rate"),
        adverse_fill_slippage_bps_p50=_weighted_mean_optional(frame, "adverse_fill_slippage_bps_p50"),
        adverse_fill_slippage_bps_p90=_weighted_mean_optional(frame, "adverse_fill_slippage_bps_p90"),
        submit_latency_ms_p50=_weighted_mean_optional(frame, "submit_latency_ms_p50"),
        submit_latency_ms_p90=_weighted_mean_optional(frame, "submit_latency_ms_p90"),
        fill_latency_ms_p50=_weighted_mean_optional(frame, "fill_latency_ms_p50"),
        fill_latency_ms_p90=_weighted_mean_optional(frame, "fill_latency_ms_p90"),
        realized_edge_retention_bps_p50=_weighted_mean_optional(frame, "realized_edge_retention_bps_p50"),
        realized_edge_retention_bps_p90=_weighted_mean_optional(frame, "realized_edge_retention_bps_p90"),
        avg_realized_pnl=_weighted_mean_optional(frame, "avg_realized_pnl"),
        avg_post_trade_error=_weighted_mean_optional(frame, "avg_post_trade_error"),
        prior_quality_status="ready" if sample_count >= 10 else "sparse",
        prior_lookup_mode="station_metric_fallback",
        prior_feature_scope=feature_scope,
        feedback_prior=feedback_prior if feedback_prior.feedback_status != "missing" else None,
    )


def _build_prior_feature_scope(
    prior_key: ExecutionPriorKey,
    *,
    lookup_mode: str,
    matched_market_count: int | None = None,
) -> dict[str, Any]:
    payload = {
        "lookup_mode": lookup_mode,
        "market_id": prior_key.market_id,
        "station_id": prior_key.station_id,
        "metric": prior_key.metric,
        "side": prior_key.side,
        "horizon_bucket": prior_key.horizon_bucket,
        "liquidity_bucket": prior_key.liquidity_bucket,
        "market_age_bucket": prior_key.market_age_bucket,
        "hours_to_close_bucket": prior_key.hours_to_close_bucket,
        "calibration_quality_bucket": prior_key.calibration_quality_bucket,
        "source_freshness_bucket": prior_key.source_freshness_bucket,
    }
    if matched_market_count is not None:
        payload["matched_market_count"] = int(matched_market_count)
    return payload


def _select_best_prior_row(
    frame: pd.DataFrame,
    *,
    market_age_bucket: str | None,
    hours_to_close_bucket: str | None,
    calibration_quality_bucket: str | None,
    source_freshness_bucket: str | None,
):
    filtered = _filter_prior_rows_by_feature_buckets(
        frame,
        market_age_bucket=market_age_bucket,
        hours_to_close_bucket=hours_to_close_bucket,
        calibration_quality_bucket=calibration_quality_bucket,
        source_freshness_bucket=source_freshness_bucket,
    )
    if filtered.empty:
        return None
    working = filtered.copy()
    working["feature_match_score"] = working.apply(
        lambda row: _feature_match_score(
            row,
            market_age_bucket=market_age_bucket,
            hours_to_close_bucket=hours_to_close_bucket,
            calibration_quality_bucket=calibration_quality_bucket,
            source_freshness_bucket=source_freshness_bucket,
        ),
        axis=1,
    )
    working["quality_ready_rank"] = working["prior_quality_status"].astype(str).eq("ready").astype(int)
    working["materialized_at"] = pd.to_datetime(working["materialized_at"], errors="coerce")
    working = working.sort_values(
        by=["feature_match_score", "quality_ready_rank", "sample_count", "materialized_at"],
        ascending=[False, False, False, False],
        na_position="last",
    )
    if working.empty:
        return None
    return working.iloc[0]


def _filter_prior_rows_by_feature_buckets(
    frame: pd.DataFrame,
    *,
    market_age_bucket: str | None,
    hours_to_close_bucket: str | None,
    calibration_quality_bucket: str | None,
    source_freshness_bucket: str | None,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    working = frame.copy()
    filters = [
        ("market_age_bucket", market_age_bucket, {"unknown"}),
        ("hours_to_close_bucket", hours_to_close_bucket, {"unknown"}),
        ("calibration_quality_bucket", calibration_quality_bucket, {"sparse_or_missing"}),
        ("source_freshness_bucket", source_freshness_bucket, {"degraded_or_missing"}),
    ]
    for column, requested, generic_values in filters:
        if requested in {None, ""} or column not in working.columns:
            continue
        normalized = str(requested)
        working[column] = working[column].fillna("")
        allowed = {normalized, *generic_values}
        working = working[working[column].astype(str).isin(allowed)]
        if working.empty:
            return working
    return working


def _feature_match_score(
    row,
    *,
    market_age_bucket: str | None,
    hours_to_close_bucket: str | None,
    calibration_quality_bucket: str | None,
    source_freshness_bucket: str | None,
) -> int:
    score = 0
    for column, requested, generic_values in [
        ("market_age_bucket", market_age_bucket, {"unknown"}),
        ("hours_to_close_bucket", hours_to_close_bucket, {"unknown"}),
        ("calibration_quality_bucket", calibration_quality_bucket, {"sparse_or_missing"}),
        ("source_freshness_bucket", source_freshness_bucket, {"degraded_or_missing"}),
    ]:
        if requested in {None, ""}:
            continue
        actual = str(row.get(column) or "")
        normalized = str(requested)
        if actual == normalized:
            score += 2
        elif actual in generic_values:
            score += 1
    return score


def _best_scope_summary(scope_rows: dict[str, ExecutionPriorSummary]) -> tuple[str, ExecutionPriorSummary] | None:
    candidates: list[tuple[str, ExecutionPriorSummary]] = []
    for scope_name in ("strategy", "wallet"):
        summary = scope_rows.get(scope_name)
        if summary is not None:
            candidates.append((scope_name, summary))
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            1 if str(item[1].prior_quality_status) == "ready" else 0,
            int(item[1].sample_count),
            1 if item[0] == "strategy" else 0,
        ),
    )


def _latest_status_by_ticket(frame, *, status_col: str = "status", ts_col: str = "created_at") -> dict[str, str]:
    if frame.empty:
        return {}
    ordered = frame.sort_values([ts_col, "ticket_id"], ascending=[False, True], na_position="last")
    deduped = ordered.drop_duplicates(subset=["ticket_id"], keep="first")
    return {
        str(row["ticket_id"]): str(row[status_col]).lower()
        for _, row in deduped.iterrows()
        if row["ticket_id"] is not None and row[status_col] is not None
    }


def _execution_lifecycle_stage(
    *,
    resolved: bool,
    filled_quantity: float,
    fill_ratio: float,
    order_status: str | None,
    latest_submit_status: str | None,
    external_order_status: str | None,
) -> str:
    normalized_order = str(order_status or "").lower()
    normalized_submit = str(latest_submit_status or "").lower()
    normalized_external = str(external_order_status or "").lower()
    if resolved:
        return "resolved"
    if 0.0 < fill_ratio < 1.0:
        return "partially_filled"
    if filled_quantity > 0:
        return "filled_unresolved"
    if normalized_order in _CANCELLED_ORDER_STATUSES:
        return "cancelled"
    if normalized_order == "posted":
        return "working_unfilled"
    if normalized_submit == "accepted":
        return "submitted_ack"
    if normalized_external == "rejected":
        return "submit_rejected"
    return "ticket_created"


def _miss_reason_bucket_for_stage(stage: str) -> str:
    if stage == "resolved":
        return "captured_resolved"
    if stage == "filled_unresolved":
        return "captured_unresolved"
    if stage == "partially_filled":
        return "partial_fill"
    if stage == "cancelled":
        return "cancelled"
    if stage == "working_unfilled":
        return "working_unfilled"
    if stage == "submit_rejected":
        return "submit_rejected"
    if stage == "sign_rejected":
        return "sign_rejected"
    if stage == "gate_rejected":
        return "gate_rejected"
    return "not_submitted"


def _distortion_reason_bucket(
    *,
    stage: str,
    realized_pnl: float | None,
    adverse_fill_slippage_bps: float | None,
) -> str:
    if stage in {"working_unfilled", "partially_filled", "cancelled", "submit_rejected"}:
        return "execution_distortion"
    if (adverse_fill_slippage_bps or 0.0) > 0:
        return "execution_distortion"
    if realized_pnl is not None and realized_pnl < 0:
        return "forecast_distortion"
    return "none"


def _latest_value_by_ticket(frame, *, value_col: str, ts_col: str = "created_at") -> dict[str, str]:
    if frame.empty or value_col not in frame.columns:
        return {}
    ordered = frame.sort_values([ts_col, "ticket_id"], ascending=[False, True], na_position="last")
    deduped = ordered.drop_duplicates(subset=["ticket_id"], keep="first")
    return {
        str(row["ticket_id"]): str(row[value_col])
        for _, row in deduped.iterrows()
        if row["ticket_id"] is not None and row[value_col] not in {None, ""}
    }


def _is_submit_ack(submit_status: str | None, external_status: str | None) -> float:
    if submit_status and str(submit_status).lower() in _SUBMIT_ACK_STATUSES:
        return 1.0
    if external_status and str(external_status).lower() in _SUBMIT_ACK_STATUSES:
        return 1.0
    return 0.0


def _mean(values) -> float:
    normalized = [float(value) for value in values]
    return sum(normalized) / len(normalized) if normalized else 0.0


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = max(0.0, min(1.0, float(quantile))) * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return round((ordered[lower] * (1.0 - weight)) + (ordered[upper] * weight), 6)


def _dominant_bucket(values: list[str], *, priority: dict[str, int], default: str) -> str:
    if not values:
        return default
    counts = defaultdict(int)
    for value in values:
        counts[str(value)] += 1
    if not counts:
        return default
    best_count = max(counts.values())
    candidates = [item for item, count in counts.items() if count == best_count]
    candidates.sort(key=lambda item: priority.get(item, 999))
    return candidates[0] if candidates else default


def _weighted_mean(frame, value_col: str, *, weight_col: str = "sample_count") -> float:
    if frame.empty or value_col not in frame.columns:
        return 0.0
    subset = frame[[value_col, weight_col]].copy()
    subset[value_col] = pd.to_numeric(subset[value_col], errors="coerce")
    subset[weight_col] = pd.to_numeric(subset[weight_col], errors="coerce").fillna(0.0)
    subset = subset.dropna(subset=[value_col])
    total_weight = float(subset[weight_col].sum())
    if total_weight <= 0.0 or subset.empty:
        return 0.0
    return round(float((subset[value_col] * subset[weight_col]).sum()) / total_weight, 6)


def _weighted_mean_optional(frame, value_col: str, *, weight_col: str = "sample_count") -> float | None:
    if frame.empty or value_col not in frame.columns:
        return None
    subset = frame[[value_col, weight_col]].copy()
    subset[value_col] = pd.to_numeric(subset[value_col], errors="coerce")
    subset[weight_col] = pd.to_numeric(subset[weight_col], errors="coerce").fillna(0.0)
    subset = subset.dropna(subset=[value_col])
    total_weight = float(subset[weight_col].sum())
    if total_weight <= 0.0 or subset.empty:
        return None
    return round(float((subset[value_col] * subset[weight_col]).sum()) / total_weight, 6)


def _latency_ms(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max((end - start).total_seconds() * 1000.0, 0.0), 6)


def _market_age_bucket(*, ticket_created_at: datetime | None, market_created_at: datetime | None) -> str:
    if ticket_created_at is None or market_created_at is None:
        return "unknown"
    age_hours = max((ticket_created_at - market_created_at).total_seconds() / 3600.0, 0.0)
    if age_hours < 12.0:
        return "new"
    if age_hours <= 72.0:
        return "active"
    return "late"


def _hours_to_close_bucket(*, ticket_created_at: datetime | None, market_close_time: datetime | None) -> str:
    if ticket_created_at is None or market_close_time is None:
        return "unknown"
    hours_to_close = max((market_close_time - ticket_created_at).total_seconds() / 3600.0, 0.0)
    if hours_to_close <= 6.0:
        return "0-6"
    if hours_to_close <= 24.0:
        return "6-24"
    if hours_to_close <= 72.0:
        return "24-72"
    return "72+"


def _calibration_quality_bucket(pricing_context: dict[str, Any]) -> str:
    quality = str(
        pricing_context.get("calibration_bias_quality")
        or pricing_context.get("bias_quality_status")
        or pricing_context.get("threshold_probability_quality")
        or pricing_context.get("threshold_probability_quality_status")
        or pricing_context.get("calibration_health_status")
        or ""
    ).strip()
    if quality in {"healthy", "watch", "degraded"}:
        return quality
    return "sparse_or_missing"


def _source_freshness_bucket(pricing_context: dict[str, Any]) -> str:
    freshness = str(pricing_context.get("source_freshness_status") or "").strip()
    if freshness == "fresh":
        return "fresh"
    if freshness == "stale":
        return "stale"
    return "degraded_or_missing"


def _table_exists(con, table_name: str) -> bool:
    schema_name, table = table_name.split(".", 1)
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema_name, table],
    ).fetchone()
    return row is not None


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw in {None, ""}:
        return {}
    try:
        value = json.loads(str(raw))
    except Exception:  # noqa: BLE001
        return {}
    return value if isinstance(value, dict) else {}


def _coerce_optional_text(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        out = float(value)
        if not math.isfinite(out):
            return None
        return out
    except Exception:  # noqa: BLE001
        return None


def _coerce_optional_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(float(value))
    except Exception:  # noqa: BLE001
        return None


def _coerce_optional_datetime(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:  # noqa: BLE001
        return None
    return _normalize_datetime(parsed)


def _coerce_optional_date(value: Any) -> date | None:
    if value in {None, ""}:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value))
    except Exception:  # noqa: BLE001
        return None


def _forecast_horizon_bucket(*, observation_date: date, forecast_target_time: datetime) -> str:
    horizon_days = max(0, (observation_date - forecast_target_time.date()).days)
    if horizon_days <= 1:
        return "0-1"
    if horizon_days <= 3:
        return "2-3"
    if horizon_days <= 7:
        return "4-7"
    return "8+"


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
    "WEATHER_EXECUTION_PRIOR_COLUMNS",
    "ExecutionPriorRow",
    "build_execution_prior_key",
    "build_execution_prior_summary_from_context",
    "enqueue_execution_prior_upserts",
    "execution_prior_context_fields",
    "execution_prior_key_id",
    "execution_prior_liquidity_bucket",
    "execution_prior_row_id",
    "execution_prior_row_to_row",
    "load_execution_prior_summary",
    "materialize_execution_priors",
]
