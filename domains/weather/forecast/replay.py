from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import (
    ForecastReplayDiffRecord,
    ForecastReplayRecord,
    ForecastReplayRequest,
    ForecastReplayResult,
    ForecastRunRecord,
    WatchOnlySnapshotRecord,
    WeatherFairValueRecord,
    WeatherMarket,
    WeatherMarketSpecRecord,
    stable_object_id,
)
from asterion_core.storage.utils import safe_json_dumps
from domains.weather.pricing.engine import (
    build_binary_fair_values,
    build_watch_only_snapshot,
    load_weather_market,
    load_weather_market_spec,
)

from .persistence import build_forecast_run_record
from .service import AdapterRouter, ForecastCache, ForecastService, normalize_forecast_source_name


def build_forecast_replay_request(
    con,
    *,
    market_id: str,
    station_id: str,
    source: str,
    model_run: str,
    forecast_target_time: datetime,
    spec_version: str,
    replay_reason: str,
) -> ForecastReplayRequest:
    spec = load_weather_market_spec(con, market_id=market_id)
    if spec.station_id != station_id:
        raise ValueError("station_id must match persisted weather_market_specs")
    if spec.spec_version != spec_version:
        raise ValueError("spec_version must match persisted weather_market_specs")
    return ForecastReplayRequest(
        market_id=spec.market_id,
        condition_id=spec.condition_id,
        station_id=spec.station_id,
        source=normalize_forecast_source_name(source),
        model_run=model_run,
        forecast_target_time=forecast_target_time,
        spec_version=spec.spec_version,
        replay_reason=replay_reason,
    )


def load_replay_inputs(
    con,
    request: ForecastReplayRequest,
) -> tuple[WeatherMarketSpecRecord, ForecastRunRecord, WeatherMarket]:
    spec = load_weather_market_spec(con, market_id=request.market_id)
    if spec.condition_id != request.condition_id:
        raise ValueError("condition_id must match persisted weather_market_specs")
    if spec.station_id != request.station_id:
        raise ValueError("station_id must match persisted weather_market_specs")
    if spec.spec_version != request.spec_version:
        raise ValueError("spec_version must match persisted weather_market_specs")
    original_run = _load_original_forecast_run(con, request=request)
    market = load_weather_market(con, market_id=request.market_id)
    if market.condition_id != request.condition_id:
        raise ValueError("condition_id must match persisted weather_markets")
    return spec, original_run, market


def load_original_pricing_outputs(
    con,
    *,
    run_id: str,
) -> tuple[list[WeatherFairValueRecord], list[WatchOnlySnapshotRecord]]:
    return _load_weather_fair_values(con, run_id=run_id), _load_watch_only_snapshots(con, run_id=run_id)


def recompute_forecast_run(
    forecast_service: ForecastService,
    spec: WeatherMarketSpecRecord,
    request: ForecastReplayRequest,
    *,
    replay_id: str,
) -> ForecastRunRecord:
    distribution = forecast_service.get_forecast(
        _spec_to_resolution_like(spec),
        source=request.source,
        model_run=request.model_run,
        forecast_target_time=request.forecast_target_time,
    )
    base = build_forecast_run_record(distribution)
    replayed_run_id = stable_object_id(
        "frunr",
        {
            "condition_id": request.condition_id,
            "forecast_target_time": request.forecast_target_time.isoformat(),
            "model_run": request.model_run,
            "replay_id": replay_id,
            "source": request.source,
            "spec_version": request.spec_version,
            "station_id": request.station_id,
        },
    )
    return ForecastRunRecord(
        run_id=replayed_run_id,
        market_id=base.market_id,
        condition_id=base.condition_id,
        station_id=base.station_id,
        source=base.source,
        model_run=base.model_run,
        forecast_target_time=base.forecast_target_time,
        observation_date=base.observation_date,
        metric=base.metric,
        latitude=base.latitude,
        longitude=base.longitude,
        timezone=base.timezone,
        spec_version=base.spec_version,
        cache_key=base.cache_key,
        source_trace=list(base.source_trace),
        fallback_used=base.fallback_used,
        from_cache=base.from_cache,
        confidence=base.confidence,
        forecast_payload=dict(base.forecast_payload),
        raw_payload=dict(base.raw_payload),
    )


def recompute_pricing_outputs(
    *,
    market: WeatherMarket,
    spec: WeatherMarketSpecRecord,
    forecast_run: ForecastRunRecord,
    original_snapshots: list[WatchOnlySnapshotRecord],
) -> tuple[list[WeatherFairValueRecord], list[WatchOnlySnapshotRecord]]:
    fair_values = build_binary_fair_values(
        market=market,
        spec=spec,
        forecast_run=forecast_run,
    )
    snapshot_inputs = {
        (item.token_id, item.outcome): item
        for item in original_snapshots
    }
    snapshots: list[WatchOnlySnapshotRecord] = []
    for fair_value in fair_values:
        original = snapshot_inputs.get((fair_value.token_id, fair_value.outcome))
        if original is None:
            continue
        snapshots.append(
            build_watch_only_snapshot(
                fair_value=fair_value,
                reference_price=original.reference_price,
                threshold_bps=original.threshold_bps,
            )
        )
    return fair_values, snapshots


def run_forecast_replay(
    con,
    *,
    adapter_router: AdapterRouter,
    cache: ForecastCache,
    market_id: str,
    station_id: str,
    source: str,
    model_run: str,
    forecast_target_time: datetime,
    spec_version: str,
    replay_reason: str,
) -> ForecastReplayResult:
    request = build_forecast_replay_request(
        con,
        market_id=market_id,
        station_id=station_id,
        source=source,
        model_run=model_run,
        forecast_target_time=forecast_target_time,
        spec_version=spec_version,
        replay_reason=replay_reason,
    )
    spec, original_run, market = load_replay_inputs(con, request)
    _, original_snapshots = load_original_pricing_outputs(con, run_id=original_run.run_id)
    replay_id = stable_object_id(
        "freplay",
        {
            "condition_id": request.condition_id,
            "forecast_target_time": request.forecast_target_time.isoformat(),
            "market_id": request.market_id,
            "model_run": request.model_run,
            "replay_key": request.replay_key,
            "replay_reason": request.replay_reason,
            "source": request.source,
            "spec_version": request.spec_version,
            "station_id": request.station_id,
        },
    )
    forecast_service = ForecastService(adapter_router=adapter_router, cache=cache)
    replayed_run = recompute_forecast_run(
        forecast_service,
        spec,
        request,
        replay_id=replay_id,
    )
    fair_values, snapshots = recompute_pricing_outputs(
        market=market,
        spec=spec,
        forecast_run=replayed_run,
        original_snapshots=original_snapshots,
    )
    return ForecastReplayResult(
        replay_id=replay_id,
        request=request,
        forecast_run=replayed_run,
        fair_values=fair_values,
        watch_only_snapshots=snapshots,
    )


def build_forecast_replay_record(
    result: ForecastReplayResult,
    *,
    original_run_id: str,
    created_at: datetime | None = None,
) -> ForecastReplayRecord:
    return ForecastReplayRecord(
        replay_id=result.replay_id,
        market_id=result.request.market_id,
        condition_id=result.request.condition_id,
        station_id=result.request.station_id,
        source=result.request.source,
        model_run=result.request.model_run,
        forecast_target_time=result.request.forecast_target_time,
        spec_version=result.request.spec_version,
        replay_key=result.request.replay_key,
        replay_reason=result.request.replay_reason,
        original_run_id=original_run_id,
        replayed_run_id=result.forecast_run.run_id,
        created_at=created_at or datetime.now(UTC),
    )


def build_forecast_replay_diff_records(
    *,
    replay_result: ForecastReplayResult,
    original_run: ForecastRunRecord,
    original_fair_values: list[WeatherFairValueRecord],
    original_watch_only_snapshots: list[WatchOnlySnapshotRecord],
    created_at: datetime | None = None,
) -> list[ForecastReplayDiffRecord]:
    recorded_at = created_at or datetime.now(UTC)
    diffs: list[ForecastReplayDiffRecord] = []
    diffs.append(
        _build_forecast_run_diff(
            replay_id=replay_result.replay_id,
            request=replay_result.request,
            original=original_run,
            replayed=replay_result.forecast_run,
            created_at=recorded_at,
        )
    )
    diffs.extend(
        _build_weather_fair_value_diffs(
            replay_id=replay_result.replay_id,
            original_items=original_fair_values,
            replayed_items=replay_result.fair_values,
            created_at=recorded_at,
        )
    )
    diffs.extend(
        _build_watch_only_snapshot_diffs(
            replay_id=replay_result.replay_id,
            original_items=original_watch_only_snapshots,
            replayed_items=replay_result.watch_only_snapshots,
            created_at=recorded_at,
        )
    )
    return diffs


def _build_forecast_run_diff(
    *,
    replay_id: str,
    request: ForecastReplayRequest,
    original: ForecastRunRecord,
    replayed: ForecastRunRecord,
    created_at: datetime,
) -> ForecastReplayDiffRecord:
    changed_fields: dict[str, dict[str, Any]] = {}
    _compare_scalar(changed_fields, "source", original.source, replayed.source)
    _compare_scalar(changed_fields, "model_run", original.model_run, replayed.model_run)
    _compare_scalar(
        changed_fields,
        "forecast_target_time",
        _canonical_datetime(original.forecast_target_time),
        _canonical_datetime(replayed.forecast_target_time),
    )
    _compare_scalar(changed_fields, "spec_version", original.spec_version, replayed.spec_version)
    _compare_scalar(changed_fields, "cache_key", original.cache_key, replayed.cache_key)
    _compare_json(
        changed_fields,
        "temperature_distribution",
        original.forecast_payload.get("temperature_distribution") or {},
        replayed.forecast_payload.get("temperature_distribution") or {},
    )
    _compare_float(changed_fields, "confidence", original.confidence, replayed.confidence)
    _compare_json(changed_fields, "source_trace", original.source_trace, replayed.source_trace)
    _compare_scalar(changed_fields, "fallback_used", original.fallback_used, replayed.fallback_used)
    return ForecastReplayDiffRecord(
        diff_id=stable_object_id("frdiff", {"replay_id": replay_id, "entity_type": "forecast_run", "entity_key": request.replay_key}),
        replay_id=replay_id,
        entity_type="forecast_run",
        entity_key=request.replay_key,
        original_entity_id=original.run_id,
        replayed_entity_id=replayed.run_id,
        status="MATCH" if not changed_fields else "DIFFERENT",
        diff_summary_json=_build_diff_summary(changed_fields),
        created_at=created_at,
    )


def _build_weather_fair_value_diffs(
    *,
    replay_id: str,
    original_items: list[WeatherFairValueRecord],
    replayed_items: list[WeatherFairValueRecord],
    created_at: datetime,
) -> list[ForecastReplayDiffRecord]:
    original_map = {(item.token_id, item.outcome): item for item in original_items}
    replayed_map = {(item.token_id, item.outcome): item for item in replayed_items}
    out: list[ForecastReplayDiffRecord] = []
    for key in sorted(set(original_map) | set(replayed_map)):
        entity_key = f"{key[0]}:{key[1]}"
        original = original_map.get(key)
        replayed = replayed_map.get(key)
        out.append(
            _build_entity_diff(
                replay_id=replay_id,
                entity_type="fair_value",
                entity_key=entity_key,
                original=original,
                replayed=replayed,
                created_at=created_at,
                compare_fields=lambda changed, left, right: (
                    _compare_scalar(changed, "token_id", left.token_id, right.token_id),
                    _compare_scalar(changed, "outcome", left.outcome, right.outcome),
                    _compare_float(changed, "fair_value", left.fair_value, right.fair_value),
                    _compare_float(changed, "confidence", left.confidence, right.confidence),
                ),
            )
        )
    return out


def _build_watch_only_snapshot_diffs(
    *,
    replay_id: str,
    original_items: list[WatchOnlySnapshotRecord],
    replayed_items: list[WatchOnlySnapshotRecord],
    created_at: datetime,
) -> list[ForecastReplayDiffRecord]:
    original_map = {(item.token_id, item.outcome): item for item in original_items}
    replayed_map = {(item.token_id, item.outcome): item for item in replayed_items}
    out: list[ForecastReplayDiffRecord] = []
    for key in sorted(set(original_map) | set(replayed_map)):
        entity_key = f"{key[0]}:{key[1]}"
        original = original_map.get(key)
        replayed = replayed_map.get(key)
        out.append(
            _build_entity_diff(
                replay_id=replay_id,
                entity_type="watch_only_snapshot",
                entity_key=entity_key,
                original=original,
                replayed=replayed,
                created_at=created_at,
                compare_fields=lambda changed, left, right: (
                    _compare_scalar(changed, "token_id", left.token_id, right.token_id),
                    _compare_scalar(changed, "outcome", left.outcome, right.outcome),
                    _compare_float(changed, "reference_price", left.reference_price, right.reference_price),
                    _compare_float(changed, "fair_value", left.fair_value, right.fair_value),
                    _compare_scalar(changed, "edge_bps", left.edge_bps, right.edge_bps),
                    _compare_scalar(changed, "threshold_bps", left.threshold_bps, right.threshold_bps),
                    _compare_scalar(changed, "decision", left.decision, right.decision),
                    _compare_scalar(changed, "side", left.side, right.side),
                ),
            )
        )
    return out


def _build_entity_diff(
    *,
    replay_id: str,
    entity_type: str,
    entity_key: str,
    original,
    replayed,
    created_at: datetime,
    compare_fields,
) -> ForecastReplayDiffRecord:
    changed_fields: dict[str, dict[str, Any]] = {}
    if original is None:
        status = "MISSING_ORIGINAL"
        summary = {"missing": "original"}
        original_entity_id = None
        replayed_entity_id = getattr(replayed, "fair_value_id", None) or getattr(replayed, "snapshot_id", None)
    elif replayed is None:
        status = "MISSING_REPLAY"
        summary = {"missing": "replayed"}
        original_entity_id = getattr(original, "fair_value_id", None) or getattr(original, "snapshot_id", None)
        replayed_entity_id = None
    else:
        compare_fields(changed_fields, original, replayed)
        status = "MATCH" if not changed_fields else "DIFFERENT"
        summary = _build_diff_summary(changed_fields)
        original_entity_id = getattr(original, "fair_value_id", None) or getattr(original, "snapshot_id", None)
        replayed_entity_id = getattr(replayed, "fair_value_id", None) or getattr(replayed, "snapshot_id", None)
    return ForecastReplayDiffRecord(
        diff_id=stable_object_id("frdiff", {"replay_id": replay_id, "entity_type": entity_type, "entity_key": entity_key}),
        replay_id=replay_id,
        entity_type=entity_type,
        entity_key=entity_key,
        original_entity_id=original_entity_id,
        replayed_entity_id=replayed_entity_id,
        status=status,
        diff_summary_json=summary,
        created_at=created_at,
    )


def _build_diff_summary(changed_fields: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if not changed_fields:
        return {"result": "match"}
    return {"changed_fields": changed_fields}


def _compare_scalar(changed_fields: dict[str, dict[str, Any]], name: str, original: Any, replayed: Any) -> None:
    if original != replayed:
        changed_fields[name] = {"original": original, "replayed": replayed}


def _compare_float(changed_fields: dict[str, dict[str, Any]], name: str, original: float, replayed: float) -> None:
    if abs(float(original) - float(replayed)) > 1e-9:
        changed_fields[name] = {"original": float(original), "replayed": float(replayed)}


def _compare_json(changed_fields: dict[str, dict[str, Any]], name: str, original: Any, replayed: Any) -> None:
    canonical_original = _canonical_json_value(original)
    canonical_replayed = _canonical_json_value(replayed)
    if safe_json_dumps(canonical_original) != safe_json_dumps(canonical_replayed):
        changed_fields[name] = {"original": original, "replayed": replayed}


def _canonical_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            _canonical_json_key(key): _canonical_json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, float):
        return round(value, 12)
    return value


def _canonical_json_key(key: Any) -> Any:
    try:
        number = float(key)
    except (TypeError, ValueError):
        return key
    if number.is_integer():
        return int(number)
    return round(number, 12)


def _load_original_forecast_run(con, *, request: ForecastReplayRequest) -> ForecastRunRecord:
    row = con.execute(
        """
        SELECT
            fr.run_id,
            fr.market_id,
            fr.condition_id,
            fr.station_id,
            fr.source,
            fr.model_run,
            fr.forecast_target_time,
            fr.observation_date,
            fr.metric,
            fr.latitude,
            fr.longitude,
            fr.timezone,
            fr.spec_version,
            fr.cache_key,
            fr.source_trace_json,
            fr.fallback_used,
            fr.from_cache,
            fr.confidence,
            fr.forecast_payload_json,
            fr.raw_payload_json
        FROM weather.weather_forecast_runs fr
        WHERE fr.market_id = ?
          AND fr.condition_id = ?
          AND fr.station_id = ?
          AND fr.source = ?
          AND fr.model_run = ?
          AND fr.forecast_target_time = ?
          AND fr.spec_version = ?
          AND fr.run_id NOT IN (
              SELECT replayed_run_id
              FROM weather.weather_forecast_replays
          )
        ORDER BY fr.created_at DESC
        LIMIT 1
        """,
        [
            request.market_id,
            request.condition_id,
            request.station_id,
            request.source,
            request.model_run,
            _sql_timestamp(request.forecast_target_time),
            request.spec_version,
        ],
    ).fetchone()
    if row is None:
        raise LookupError(f"original forecast run not found for replay_key={request.replay_key}")
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


def _load_weather_fair_values(con, *, run_id: str) -> list[WeatherFairValueRecord]:
    rows = con.execute(
        """
        SELECT
            fair_value_id,
            run_id,
            market_id,
            condition_id,
            token_id,
            outcome,
            fair_value,
            confidence
        FROM weather.weather_fair_values
        WHERE run_id = ?
        ORDER BY token_id, outcome
        """,
        [run_id],
    ).fetchall()
    return [
        WeatherFairValueRecord(
            fair_value_id=row[0],
            run_id=row[1],
            market_id=row[2],
            condition_id=row[3],
            token_id=row[4],
            outcome=row[5],
            fair_value=float(row[6]),
            confidence=float(row[7]),
        )
        for row in rows
    ]


def _load_watch_only_snapshots(con, *, run_id: str) -> list[WatchOnlySnapshotRecord]:
    rows = con.execute(
        """
        SELECT
            snapshot_id,
            fair_value_id,
            run_id,
            market_id,
            condition_id,
            token_id,
            outcome,
            reference_price,
            fair_value,
            edge_bps,
            threshold_bps,
            decision,
            side,
            rationale,
            pricing_context_json
        FROM weather.weather_watch_only_snapshots
        WHERE run_id = ?
        ORDER BY token_id, outcome
        """,
        [run_id],
    ).fetchall()
    return [
        WatchOnlySnapshotRecord(
            snapshot_id=row[0],
            fair_value_id=row[1],
            run_id=row[2],
            market_id=row[3],
            condition_id=row[4],
            token_id=row[5],
            outcome=row[6],
            reference_price=float(row[7]),
            fair_value=float(row[8]),
            edge_bps=int(row[9]),
            threshold_bps=int(row[10]),
            decision=row[11],
            side=row[12],
            rationale=row[13],
            pricing_context=_json_dict(row[14]),
        )
        for row in rows
    ]


def _spec_to_resolution_like(spec: WeatherMarketSpecRecord):
    from asterion_core.contracts import ResolutionSpec

    return ResolutionSpec(
        market_id=spec.market_id,
        condition_id=spec.condition_id,
        location_name=spec.location_name,
        station_id=spec.station_id,
        latitude=spec.latitude,
        longitude=spec.longitude,
        timezone=spec.timezone,
        observation_date=spec.observation_date,
        observation_window_local=spec.observation_window_local,
        metric=spec.metric,
        unit=spec.unit,
        authoritative_source=spec.authoritative_source,
        fallback_sources=list(spec.fallback_sources),
        rounding_rule=spec.rounding_rule,
        inclusive_bounds=spec.inclusive_bounds,
        spec_version=spec.spec_version,
    )


def _json_list(value: Any) -> list[Any]:
    import json

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
    import json

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


def _sql_timestamp(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")


def _canonical_datetime(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=UTC)
    return normalized.astimezone(UTC).isoformat().replace("+00:00", "Z")
