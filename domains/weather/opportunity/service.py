from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import MarketQualityAssessment, OpportunityAssessment, SourceHealthSnapshotRecord, stable_object_id


_REVIEW_FAILURE_STATUSES = {"agent_failure"}
_REVIEW_REQUIRED_STATUSES = {"review_required", "no_agent_signal"}


def derive_opportunity_side(edge_bps: int) -> str | None:
    if edge_bps > 0:
        return "BUY"
    if edge_bps < 0:
        return "SELL"
    return None


def build_weather_opportunity_assessment(
    *,
    market_id: str,
    token_id: str,
    outcome: str,
    reference_price: float,
    model_fair_value: float,
    accepting_orders: bool,
    enable_order_book: bool | None,
    threshold_bps: int = 0,
    fees_bps: int | None = None,
    agent_review_status: str = "no_agent_signal",
    live_prereq_status: str = "not_started",
    confidence_score: float | None = None,
    mapping_confidence: float = 1.0,
    price_staleness_ms: int = 0,
    source_freshness_status: str = "fresh",
    spread_bps: int | None = None,
    source_context: dict[str, Any] | None = None,
) -> OpportunityAssessment:
    normalized_fees_bps = max(0, int(fees_bps or 0))
    normalized_reference_price = float(reference_price)
    normalized_model_fair_value = float(model_fair_value)
    normalized_threshold_bps = max(0, int(threshold_bps))
    normalized_confidence_score = max(0.0, float(confidence_score if confidence_score is not None else 50.0))

    slippage_bps = _slippage_bps(
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
    )
    liquidity_penalty_bps = _liquidity_penalty_bps(
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
    )
    fill_probability = _fill_probability(
        accepting_orders=accepting_orders,
        agent_review_status=agent_review_status,
        live_prereq_status=live_prereq_status,
    )
    depth_proxy = _depth_proxy(
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
    )
    ops_readiness_score = _ops_readiness_score(live_prereq_status)
    market_quality = build_market_quality_assessment(
        market_id=market_id,
        accepting_orders=accepting_orders,
        enable_order_book=enable_order_book,
        reference_price=normalized_reference_price,
        mapping_confidence=mapping_confidence,
        price_staleness_ms=price_staleness_ms,
        source_freshness_status=source_freshness_status,
        depth_proxy=depth_proxy,
        spread_bps=spread_bps,
    )

    edge_bps_model = int(round((normalized_model_fair_value - normalized_reference_price) * 10_000))
    execution_adjusted_fair_value = normalized_model_fair_value - (
        (normalized_fees_bps + slippage_bps + liquidity_penalty_bps) / 10_000.0
    )
    execution_adjusted_fair_value = max(0.0, min(1.0, execution_adjusted_fair_value))
    edge_bps_executable = int(round((execution_adjusted_fair_value - normalized_reference_price) * 10_000))
    opportunity_side = derive_opportunity_side(edge_bps_executable)
    effective_edge_bps = abs(edge_bps_executable)

    expected_value_score = round(effective_edge_bps * fill_probability, 4)
    expected_pnl_score = round(expected_value_score * depth_proxy, 4)
    ranking_score = round(expected_pnl_score + ops_readiness_score, 4)
    actionability_status = _actionability_status(
        edge_bps_executable=edge_bps_executable,
        threshold_bps=normalized_threshold_bps,
        opportunity_side=opportunity_side,
        accepting_orders=accepting_orders,
        live_prereq_status=live_prereq_status,
        agent_review_status=agent_review_status,
        market_quality_status=market_quality.market_quality_status,
    )
    rationale = (
        f"model_edge_bps={edge_bps_model}, executable_edge_bps={edge_bps_executable}, "
        f"fees_bps={normalized_fees_bps}, slippage_bps={slippage_bps}, "
        f"liquidity_penalty_bps={liquidity_penalty_bps}, fill_probability={fill_probability:.2f}, "
        f"market_quality_status={market_quality.market_quality_status}"
    )
    context = {
        "accepting_orders": bool(accepting_orders),
        "actionability_status": actionability_status,
        "agent_review_status": agent_review_status,
        "best_side": opportunity_side,
        "confidence_score": normalized_confidence_score,
        "depth_proxy": depth_proxy,
        "edge_bps_executable": edge_bps_executable,
        "edge_bps_model": edge_bps_model,
        "enable_order_book": bool(enable_order_book),
        "execution_adjusted_fair_value": execution_adjusted_fair_value,
        "expected_pnl_score": expected_pnl_score,
        "expected_value_score": expected_value_score,
        "fees_bps": normalized_fees_bps,
        "fill_probability": fill_probability,
        "liquidity_penalty_bps": liquidity_penalty_bps,
        "live_prereq_status": live_prereq_status,
        "market_id": market_id,
        "market_quality_reason_codes": list(market_quality.market_quality_reason_codes),
        "market_quality_status": market_quality.market_quality_status,
        "mapping_confidence": max(0.0, min(1.0, float(mapping_confidence))),
        "model_fair_value": normalized_model_fair_value,
        "ops_readiness_score": ops_readiness_score,
        "price_staleness_ms": max(0, int(price_staleness_ms)),
        "ranking_score": ranking_score,
        "reference_price": normalized_reference_price,
        "source_freshness_status": source_freshness_status,
        "spread_bps": market_quality.spread_bps,
        "slippage_bps": slippage_bps,
        "threshold_bps": normalized_threshold_bps,
    }
    if source_context:
        context.update(source_context)

    assessment_id = stable_object_id(
        "opp",
        {
            "market_id": market_id,
            "token_id": token_id,
            "outcome": outcome,
            "reference_price": normalized_reference_price,
            "model_fair_value": normalized_model_fair_value,
            "fees_bps": normalized_fees_bps,
            "slippage_bps": slippage_bps,
            "liquidity_penalty_bps": liquidity_penalty_bps,
            "agent_review_status": agent_review_status,
            "live_prereq_status": live_prereq_status,
            "mapping_confidence": round(float(mapping_confidence), 4),
            "source_freshness_status": source_freshness_status,
            "price_staleness_ms": max(0, int(price_staleness_ms)),
        },
    )
    return OpportunityAssessment(
        assessment_id=assessment_id,
        market_id=market_id,
        token_id=token_id,
        outcome=outcome,
        reference_price=normalized_reference_price,
        model_fair_value=normalized_model_fair_value,
        execution_adjusted_fair_value=execution_adjusted_fair_value,
        fees_bps=normalized_fees_bps,
        slippage_bps=slippage_bps,
        fill_probability=fill_probability,
        depth_proxy=depth_proxy,
        liquidity_penalty_bps=liquidity_penalty_bps,
        edge_bps_model=edge_bps_model,
        edge_bps_executable=edge_bps_executable,
        confidence_score=normalized_confidence_score,
        ops_readiness_score=ops_readiness_score,
        expected_value_score=expected_value_score,
        expected_pnl_score=expected_pnl_score,
        ranking_score=ranking_score,
        actionability_status=actionability_status,
        rationale=rationale,
        assessment_context_json=context,
    )


def build_market_quality_assessment(
    *,
    market_id: str,
    accepting_orders: bool,
    enable_order_book: bool | None,
    reference_price: float,
    mapping_confidence: float,
    price_staleness_ms: int,
    source_freshness_status: str,
    depth_proxy: float,
    spread_bps: int | None = None,
) -> MarketQualityAssessment:
    normalized_mapping_confidence = max(0.0, min(1.0, float(mapping_confidence)))
    normalized_staleness = max(0, int(price_staleness_ms))
    normalized_freshness = source_freshness_status if source_freshness_status in {"fresh", "stale", "degraded", "missing"} else "degraded"
    effective_spread_bps = max(0, int(spread_bps if spread_bps is not None else _spread_bps(enable_order_book=enable_order_book, reference_price=reference_price)))
    reasons: list[str] = []
    status = "pass"

    if not accepting_orders:
        reasons.append("accepting_orders_false")
        status = "blocked"
    if normalized_freshness == "missing":
        reasons.append("source_missing")
        status = "blocked"
    elif normalized_freshness == "degraded" and status != "blocked":
        reasons.append("source_degraded")
        status = "review_required"
    elif normalized_freshness == "stale" and status == "pass":
        reasons.append("source_stale")
        status = "review_required"

    if normalized_mapping_confidence < 0.35:
        reasons.append("mapping_confidence_low")
        status = "blocked"
    elif normalized_mapping_confidence < 0.75 and status == "pass":
        reasons.append("mapping_confidence_review")
        status = "review_required"

    if normalized_staleness >= 3_600_000:
        reasons.append("price_staleness_blocked")
        status = "blocked"
    elif normalized_staleness >= 900_000 and status == "pass":
        reasons.append("price_staleness_review")
        status = "review_required"

    if effective_spread_bps >= 200:
        reasons.append("spread_too_wide")
        status = "blocked"
    elif effective_spread_bps >= 100 and status == "pass":
        reasons.append("spread_review")
        status = "review_required"

    if float(depth_proxy) < 0.30 and status == "pass":
        reasons.append("depth_low")
        status = "review_required"

    return MarketQualityAssessment(
        market_id=market_id,
        price_staleness_ms=normalized_staleness,
        source_freshness_status=normalized_freshness,
        mapping_confidence=normalized_mapping_confidence,
        spread_bps=effective_spread_bps,
        depth_proxy=float(depth_proxy),
        market_quality_status=status,
        market_quality_reason_codes=reasons,
    )


def build_source_health_snapshot(
    *,
    market_id: str,
    station_id: str,
    source: str,
    market_updated_at: datetime | None,
    forecast_created_at: datetime | None,
    snapshot_created_at: datetime | None,
    now: datetime | None = None,
) -> SourceHealthSnapshotRecord:
    observed_now = _normalize_datetime(now) or datetime.now(UTC).replace(tzinfo=None)
    latest_market = _normalize_datetime(market_updated_at)
    latest_forecast = _normalize_datetime(forecast_created_at)
    latest_snapshot = _normalize_datetime(snapshot_created_at)
    reason_codes: list[str] = []
    if latest_market is None:
        price_staleness_ms = 0
        reason_codes.append("market_timestamp_missing")
    else:
        price_staleness_ms = max(0, int((observed_now - latest_market).total_seconds() * 1000))
    if latest_forecast is None:
        freshness = "missing"
        reason_codes.append("forecast_missing")
    else:
        age_ms = max(0, int((observed_now - latest_forecast).total_seconds() * 1000))
        if age_ms <= 15 * 60 * 1000:
            freshness = "fresh"
        elif age_ms <= 60 * 60 * 1000:
            freshness = "stale"
            reason_codes.append("forecast_stale")
        else:
            freshness = "degraded"
            reason_codes.append("forecast_degraded")
    if latest_snapshot is None:
        reason_codes.append("snapshot_missing")
    return SourceHealthSnapshotRecord(
        snapshot_id=stable_object_id(
            "shs",
            {
                "market_id": market_id,
                "source": source,
                "forecast_created_at": latest_forecast.isoformat() if latest_forecast is not None else None,
                "snapshot_created_at": latest_snapshot.isoformat() if latest_snapshot is not None else None,
            },
        ),
        market_id=market_id,
        station_id=station_id,
        source=source,
        latest_market_updated_at=latest_market,
        latest_forecast_created_at=latest_forecast,
        latest_snapshot_created_at=latest_snapshot,
        price_staleness_ms=price_staleness_ms,
        source_freshness_status=freshness,
        degraded_reason_codes=reason_codes,
        created_at=observed_now,
    )


def _slippage_bps(*, accepting_orders: bool, enable_order_book: bool | None, reference_price: float) -> int:
    if not accepting_orders:
        return 0
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 40
    return 80


def _liquidity_penalty_bps(*, accepting_orders: bool, enable_order_book: bool | None, reference_price: float) -> int:
    if not accepting_orders:
        return 999_999
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 25
    return 60


def _fill_probability(*, accepting_orders: bool, agent_review_status: str, live_prereq_status: str) -> float:
    if not accepting_orders:
        return 0.0
    if live_prereq_status == "attention_required" or agent_review_status in _REVIEW_FAILURE_STATUSES:
        return 0.25
    if agent_review_status in _REVIEW_REQUIRED_STATUSES:
        return 0.50
    if agent_review_status == "passed" or live_prereq_status == "shadow_aligned":
        return 0.75
    return 0.60


def _depth_proxy(*, accepting_orders: bool, enable_order_book: bool | None, reference_price: float) -> float:
    if not accepting_orders:
        return 0.25
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 0.85
    return 0.55


def _ops_readiness_score(live_prereq_status: str) -> float:
    if live_prereq_status == "shadow_aligned":
        return 20.0
    if live_prereq_status == "not_started":
        return 10.0
    return 0.0


def _actionability_status(
    *,
    edge_bps_executable: int,
    threshold_bps: int,
    opportunity_side: str | None,
    accepting_orders: bool,
    live_prereq_status: str,
    agent_review_status: str,
    market_quality_status: str,
) -> str:
    if opportunity_side is None or abs(int(edge_bps_executable)) <= max(0, int(threshold_bps)):
        return "no_trade"
    if not accepting_orders or live_prereq_status == "attention_required" or market_quality_status == "blocked":
        return "blocked"
    if agent_review_status != "passed" or market_quality_status == "review_required":
        return "review_required"
    return "actionable"


def _spread_bps(*, enable_order_book: bool | None, reference_price: float) -> int:
    if bool(enable_order_book) and 0.10 <= reference_price <= 0.90:
        return 35
    return 125


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


__all__ = [
    "build_market_quality_assessment",
    "build_source_health_snapshot",
    "build_weather_opportunity_assessment",
    "derive_opportunity_side",
]
