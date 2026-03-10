from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import (
    ForecastRunRecord,
    WatchOnlySnapshotRecord,
    WeatherFairValueRecord,
    WeatherMarket,
    WeatherMarketSpecRecord,
    stable_object_id,
)


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


def build_watch_only_snapshot(
    *,
    fair_value: WeatherFairValueRecord,
    reference_price: float,
    threshold_bps: int,
    pricing_context: dict[str, Any] | None = None,
) -> WatchOnlySnapshotRecord:
    price = float(reference_price)
    if not (0.0 <= price <= 1.0):
        raise ValueError("reference_price must be between 0 and 1")
    threshold = max(0, int(threshold_bps))
    edge_bps = int(round((fair_value.fair_value - price) * 10_000))

    if edge_bps > threshold:
        decision = "TAKE"
        side = "BUY"
        rationale = f"market_price={price:.4f} below fair_value={fair_value.fair_value:.4f}"
    elif edge_bps < -threshold:
        decision = "TAKE"
        side = "SELL"
        rationale = f"market_price={price:.4f} above fair_value={fair_value.fair_value:.4f}"
    else:
        decision = "NO_TRADE"
        side = "HOLD"
        rationale = f"edge_bps={edge_bps} within threshold_bps={threshold}"

    context = {
        "confidence": fair_value.confidence,
        "edge_bps": edge_bps,
        "reference_price": price,
        "threshold_bps": threshold,
    }
    if pricing_context:
        context.update(pricing_context)
    return WatchOnlySnapshotRecord(
        snapshot_id=stable_object_id(
            "wsnap",
            {
                "fair_value_id": fair_value.fair_value_id,
                "reference_price": price,
                "threshold_bps": threshold,
            },
        ),
        fair_value_id=fair_value.fair_value_id,
        run_id=fair_value.run_id,
        market_id=fair_value.market_id,
        condition_id=fair_value.condition_id,
        token_id=fair_value.token_id,
        outcome=fair_value.outcome,
        reference_price=price,
        fair_value=fair_value.fair_value,
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
        forecast_payload=_json_dict(row[18]),
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
