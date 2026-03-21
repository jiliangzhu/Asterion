from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import (
    ExecutionPriorSummary,
    ForecastRunRecord,
    OpportunityAssessment,
    WatchOnlySnapshotRecord,
    WeatherFairValueRecord,
    WeatherMarket,
    WeatherMarketSpecRecord,
    stable_object_id,
)
from domains.weather.forecast.calibration import calibration_v2_context_for_probability
from domains.weather.opportunity import build_weather_opportunity_assessment


def build_binary_fair_values(
    *,
    market: WeatherMarket,
    spec: WeatherMarketSpecRecord,
    forecast_run: ForecastRunRecord,
) -> list[WeatherFairValueRecord]:
    token_ids = list(market.token_ids)
    outcomes = [_normalize_outcome(value) for value in market.outcomes]
    if len(token_ids) != 2 or len(outcomes) != 2:
        raise ValueError("binary weather pricing requires exactly 2 token_ids and 2 outcomes")

    yes_probability = probability_in_bucket(
        forecast_run.forecast_payload.get("temperature_distribution") or {},
        bucket_min=spec.bucket_min_value,
        bucket_max=spec.bucket_max_value,
        inclusive_bounds=spec.inclusive_bounds,
    )
    no_probability = max(0.0, 1.0 - yes_probability)
    confidence = float(forecast_run.confidence)

    out: list[WeatherFairValueRecord] = []
    for token_id, outcome in zip(token_ids, outcomes, strict=True):
        fair_value = yes_probability if outcome == "YES" else no_probability
        out.append(
            WeatherFairValueRecord(
                fair_value_id=stable_object_id(
                    "fval",
                    {
                        "condition_id": forecast_run.condition_id,
                        "outcome": outcome,
                        "run_id": forecast_run.run_id,
                        "token_id": token_id,
                    },
                ),
                run_id=forecast_run.run_id,
                market_id=forecast_run.market_id,
                condition_id=forecast_run.condition_id,
                token_id=token_id,
                outcome=outcome,
                fair_value=fair_value,
                confidence=confidence,
            )
        )
    return out


def build_forecast_calibration_pricing_context(
    *,
    forecast_run: ForecastRunRecord,
    outcome: str,
    fair_value: float,
) -> dict[str, Any]:
    summary = forecast_run.forecast_payload.get("distribution_summary_v2")
    if not isinstance(summary, dict):
        return {}
    normalized_outcome = _normalize_outcome(outcome)
    yes_probability = float(fair_value) if normalized_outcome == "YES" else max(0.0, min(1.0, 1.0 - float(fair_value)))
    return calibration_v2_context_for_probability(summary, probability=yes_probability)


def build_watch_only_snapshot(
    *,
    assessment: OpportunityAssessment | None = None,
    fair_value: WeatherFairValueRecord | None = None,
    reference_price: float,
    threshold_bps: int,
    accepting_orders: bool = True,
    enable_order_book: bool | None = None,
    fees_bps: int | None = None,
    agent_review_status: str = "no_agent_signal",
    live_prereq_status: str = "not_started",
    execution_prior_summary: ExecutionPriorSummary | None = None,
    pricing_context: dict[str, Any] | None = None,
) -> WatchOnlySnapshotRecord:
    price = float(reference_price)
    if not (0.0 <= price <= 1.0):
        raise ValueError("reference_price must be between 0 and 1")
    threshold = max(0, int(threshold_bps))
    if assessment is None:
        if fair_value is None:
            raise ValueError("assessment or fair_value is required")
        effective_pricing_context = dict(pricing_context or {})
        assessment = build_weather_opportunity_assessment(
            market_id=fair_value.market_id,
            token_id=fair_value.token_id,
            outcome=fair_value.outcome,
            reference_price=price,
            model_fair_value=fair_value.fair_value,
            accepting_orders=accepting_orders,
            enable_order_book=enable_order_book,
            threshold_bps=threshold,
            fees_bps=fees_bps,
            agent_review_status=agent_review_status,
            live_prereq_status=live_prereq_status,
            confidence_score=float(fair_value.confidence) * 100.0,
            mapping_confidence=float(effective_pricing_context.get("mapping_confidence") or 1.0),
            price_staleness_ms=int(effective_pricing_context.get("price_staleness_ms") or 0),
            source_freshness_status=str(effective_pricing_context.get("source_freshness_status") or "fresh"),
            spread_bps=int(effective_pricing_context.get("spread_bps") or 0) or None,
            calibration_health_status=(
                str(effective_pricing_context.get("calibration_health_status"))
                if effective_pricing_context.get("calibration_health_status") is not None
                else None
            ),
            calibration_bias_quality=effective_pricing_context.get("calibration_bias_quality")
            or effective_pricing_context.get("bias_quality_status"),
            threshold_probability_quality=effective_pricing_context.get("threshold_probability_quality")
            or effective_pricing_context.get("threshold_probability_quality_status"),
            sample_count=(
                int(effective_pricing_context.get("sample_count"))
                if effective_pricing_context.get("sample_count") is not None
                else None
            ),
            calibration_multiplier=effective_pricing_context.get("calibration_multiplier"),
            calibration_reason_codes=effective_pricing_context.get("calibration_reason_codes")
            if isinstance(effective_pricing_context.get("calibration_reason_codes"), list)
            else None,
            execution_prior_summary=execution_prior_summary,
            forecast_distribution_summary_v2=effective_pricing_context.get("distribution_summary_v2")
            if isinstance(effective_pricing_context.get("distribution_summary_v2"), dict)
            else None,
            source_context=effective_pricing_context,
        )
    edge_bps = int(assessment.edge_bps_executable)
    best_side = str(assessment.assessment_context_json.get("best_side") or "") or None

    if assessment.actionability_status == "actionable" and best_side and abs(edge_bps) > threshold:
        decision = "TAKE"
        side = best_side
        if best_side == "BUY":
            rationale = (
                f"market_price={price:.4f} below execution_adjusted_fair_value="
                f"{assessment.execution_adjusted_fair_value:.4f}"
            )
        else:
            rationale = (
                f"market_price={price:.4f} above execution_adjusted_fair_value="
                f"{assessment.execution_adjusted_fair_value:.4f}"
            )
    else:
        decision = "NO_TRADE"
        side = "HOLD"
        rationale = (
            f"actionability_status={assessment.actionability_status}, "
            f"best_side={best_side}, edge_bps={edge_bps}, threshold_bps={threshold}"
        )

    context = dict(assessment.assessment_context_json)
    context.update(
        {
            "confidence": context.get("confidence_score"),
            "decision": decision,
            "edge_bps": edge_bps,
            "fair_value": assessment.execution_adjusted_fair_value,
            "model_fair_value": assessment.model_fair_value,
            "outcome": assessment.outcome,
            "reference_price": price,
            "threshold_bps": threshold,
        }
    )
    if pricing_context:
        context.update(pricing_context)
    fair_value_id = fair_value.fair_value_id if fair_value is not None else stable_object_id(
        "fval",
        {
            "assessment_id": assessment.assessment_id,
            "condition_id": "",
            "outcome": assessment.outcome,
            "token_id": assessment.token_id,
        },
    )
    run_id = fair_value.run_id if fair_value is not None else str(context.get("forecast_run_id") or context.get("run_id") or "")
    market_id = fair_value.market_id if fair_value is not None else assessment.market_id
    condition_id = fair_value.condition_id if fair_value is not None else str(context.get("condition_id") or "")
    return WatchOnlySnapshotRecord(
        snapshot_id=stable_object_id(
            "wsnap",
            {
                "fair_value_id": fair_value_id,
                "reference_price": price,
                "threshold_bps": threshold,
                "assessment_id": assessment.assessment_id,
            },
        ),
        fair_value_id=fair_value_id,
        run_id=run_id,
        market_id=market_id,
        condition_id=condition_id,
        token_id=assessment.token_id,
        outcome=assessment.outcome,
        reference_price=price,
        fair_value=assessment.execution_adjusted_fair_value,
        edge_bps=edge_bps,
        threshold_bps=threshold,
        decision=decision,
        side=side,
        rationale=rationale,
        pricing_context=context,
    )


def probability_in_bucket(
    distribution: dict[Any, Any],
    *,
    bucket_min: float | None,
    bucket_max: float | None,
    inclusive_bounds: bool,
) -> float:
    if bucket_min is None or bucket_max is None:
        raise ValueError("bucket_min and bucket_max are required for binary weather pricing")
    total = 0.0
    for raw_temp, raw_prob in distribution.items():
        temp = float(raw_temp)
        prob = float(raw_prob)
        lower_ok = temp >= float(bucket_min) if inclusive_bounds else temp > float(bucket_min)
        upper_ok = temp <= float(bucket_max) if inclusive_bounds else temp < float(bucket_max)
        if lower_ok and upper_ok:
            total += prob
    return max(0.0, min(1.0, total))


def load_weather_market_spec(con, *, market_id: str) -> WeatherMarketSpecRecord:
    row = con.execute(
        """
        SELECT
            market_id,
            condition_id,
            location_name,
            station_id,
            latitude,
            longitude,
            timezone,
            observation_date,
            observation_window_local,
            metric,
            unit,
            bucket_min_value,
            bucket_max_value,
            authoritative_source,
            fallback_sources,
            rounding_rule,
            inclusive_bounds,
            spec_version,
            parse_confidence,
            risk_flags_json
        FROM weather.weather_market_specs
        WHERE market_id = ?
        """,
        [market_id],
    ).fetchone()
    if row is None:
        raise LookupError(f"weather market spec not found for market_id={market_id}")
    return WeatherMarketSpecRecord(
        market_id=row[0],
        condition_id=row[1],
        location_name=row[2],
        station_id=row[3],
        latitude=float(row[4]),
        longitude=float(row[5]),
        timezone=row[6],
        observation_date=row[7],
        observation_window_local=row[8],
        metric=row[9],
        unit=row[10],
        bucket_min_value=row[11],
        bucket_max_value=row[12],
        authoritative_source=row[13],
        fallback_sources=_json_list(row[14]),
        rounding_rule=row[15],
        inclusive_bounds=bool(row[16]),
        spec_version=row[17],
        parse_confidence=float(row[18]),
        risk_flags=_json_list(row[19]),
    )


def load_weather_market(con, *, market_id: str) -> WeatherMarket:
    row = con.execute(
        """
        SELECT
            market_id,
            condition_id,
            event_id,
            slug,
            title,
            description,
            rules,
            status,
            active,
            closed,
            archived,
            accepting_orders,
            enable_order_book,
            tags_json,
            outcomes_json,
            token_ids_json,
            close_time,
            end_date,
            raw_market_json
        FROM weather.weather_markets
        WHERE market_id = ?
        """,
        [market_id],
    ).fetchone()
    if row is None:
        raise LookupError(f"weather market not found for market_id={market_id}")
    return WeatherMarket(
        market_id=row[0],
        condition_id=row[1],
        event_id=row[2],
        slug=row[3],
        title=row[4],
        description=row[5],
        rules=row[6],
        status=row[7],
        active=bool(row[8]),
        closed=bool(row[9]),
        archived=bool(row[10]),
        accepting_orders=row[11],
        enable_order_book=row[12],
        tags=_json_list(row[13]),
        outcomes=_json_list(row[14]),
        token_ids=_json_list(row[15]),
        close_time=row[16],
        end_date=row[17],
        raw_market=_json_dict(row[18]),
    )


def load_forecast_run(con, *, run_id: str) -> ForecastRunRecord:
    row = con.execute(
        """
        SELECT
            run_id,
            market_id,
            condition_id,
            station_id,
            source,
            model_run,
            forecast_target_time,
            observation_date,
            metric,
            latitude,
            longitude,
            timezone,
            spec_version,
            cache_key,
            source_trace_json,
            fallback_used,
            from_cache,
            confidence,
            forecast_payload_json,
            raw_payload_json
        FROM weather.weather_forecast_runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()
    if row is None:
        raise LookupError(f"forecast run not found for run_id={run_id}")
    forecast_payload = _normalize_forecast_payload(_json_dict(row[18]))
    return ForecastRunRecord(
        run_id=row[0],
        market_id=row[1],
        condition_id=row[2],
        station_id=row[3],
        source=row[4],
        model_run=row[5],
        forecast_target_time=row[6],
        observation_date=row[7],
        metric=row[8],
        latitude=float(row[9]),
        longitude=float(row[10]),
        timezone=row[11],
        spec_version=row[12],
        cache_key=row[13],
        source_trace=_json_list(row[14]),
        fallback_used=bool(row[15]),
        from_cache=bool(row[16]),
        confidence=float(row[17]),
        forecast_payload=forecast_payload,
        raw_payload=_json_dict(row[19]),
    )


def _normalize_outcome(value: str) -> str:
    text = str(value).strip().upper()
    if text in {"YES", "NO"}:
        return text
    raise ValueError(f"unsupported binary outcome:{value!r}")


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(parsed, list):
            return parsed
    return []


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _normalize_forecast_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    distribution = normalized.get("temperature_distribution")
    if isinstance(distribution, dict):
        normalized["temperature_distribution"] = {
            _normalize_distribution_key(key): value
            for key, value in distribution.items()
        }
    return normalized


def _normalize_distribution_key(value: Any) -> Any:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if number.is_integer():
        return int(number)
    return number
