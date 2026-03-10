from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import ForecastReplayDiffRecord, ForecastReplayRecord, ForecastRunRecord, stable_object_id
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig

from .service import ForecastDistribution


WEATHER_FORECAST_RUN_COLUMNS = [
    "run_id",
    "market_id",
    "condition_id",
    "station_id",
    "source",
    "model_run",
    "forecast_target_time",
    "observation_date",
    "metric",
    "latitude",
    "longitude",
    "timezone",
    "spec_version",
    "cache_key",
    "source_trace_json",
    "fallback_used",
    "from_cache",
    "confidence",
    "forecast_payload_json",
    "raw_payload_json",
    "created_at",
]

WEATHER_FORECAST_REPLAY_COLUMNS = [
    "replay_id",
    "market_id",
    "condition_id",
    "station_id",
    "source",
    "model_run",
    "forecast_target_time",
    "spec_version",
    "replay_key",
    "replay_reason",
    "original_run_id",
    "replayed_run_id",
    "created_at",
]

WEATHER_FORECAST_REPLAY_DIFF_COLUMNS = [
    "diff_id",
    "replay_id",
    "entity_type",
    "entity_key",
    "original_entity_id",
    "replayed_entity_id",
    "status",
    "diff_summary_json",
    "created_at",
]


def build_forecast_run_record(distribution: ForecastDistribution) -> ForecastRunRecord:
    payload = {
        "cache_key": distribution.cache_key,
        "condition_id": distribution.condition_id,
        "forecast_target_time": distribution.forecast_target_time.isoformat(),
        "metric": distribution.metric,
        "model_run": distribution.model_run,
        "source": distribution.source,
        "source_trace": list(distribution.source_trace),
        "spec_version": distribution.spec_version,
        "station_id": distribution.station_id,
    }
    return ForecastRunRecord(
        run_id=stable_object_id("frun", payload),
        market_id=distribution.market_id,
        condition_id=distribution.condition_id,
        station_id=distribution.station_id,
        source=distribution.source,
        model_run=distribution.model_run,
        forecast_target_time=distribution.forecast_target_time,
        observation_date=distribution.observation_date,
        metric=distribution.metric,
        latitude=distribution.latitude,
        longitude=distribution.longitude,
        timezone=distribution.timezone,
        spec_version=distribution.spec_version,
        cache_key=distribution.cache_key,
        source_trace=list(distribution.source_trace),
        fallback_used=distribution.fallback_used,
        from_cache=distribution.from_cache,
        confidence=distribution.confidence,
        forecast_payload={"temperature_distribution": dict(distribution.temperature_distribution)},
        raw_payload=dict(distribution.raw_payload),
    )


def enqueue_forecast_run_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    forecast_runs: list[ForecastRunRecord],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> str | None:
    if not forecast_runs:
        return None
    now = (observed_at or datetime.now(UTC).replace(tzinfo=None)).replace(microsecond=0)
    rows = [forecast_run_to_row(item, observed_at=now) for item in forecast_runs]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_forecast_runs",
        pk_cols=["run_id"],
        columns=list(WEATHER_FORECAST_RUN_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def enqueue_forecast_replay_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    replays: list[ForecastReplayRecord],
    run_id: str | None = None,
) -> str | None:
    if not replays:
        return None
    rows = [forecast_replay_to_row(item) for item in replays]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_forecast_replays",
        pk_cols=["replay_id"],
        columns=list(WEATHER_FORECAST_REPLAY_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def enqueue_forecast_replay_diff_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    diffs: list[ForecastReplayDiffRecord],
    run_id: str | None = None,
) -> str | None:
    if not diffs:
        return None
    rows = [forecast_replay_diff_to_row(item) for item in diffs]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_forecast_replay_diffs",
        pk_cols=["diff_id"],
        columns=list(WEATHER_FORECAST_REPLAY_DIFF_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def forecast_run_to_row(record: ForecastRunRecord, *, observed_at: datetime) -> list[Any]:
    return [
        record.run_id,
        record.market_id,
        record.condition_id,
        record.station_id,
        record.source,
        record.model_run,
        _sql_timestamp(record.forecast_target_time),
        record.observation_date.isoformat(),
        record.metric,
        record.latitude,
        record.longitude,
        record.timezone,
        record.spec_version,
        record.cache_key,
        safe_json_dumps(record.source_trace),
        record.fallback_used,
        record.from_cache,
        record.confidence,
        safe_json_dumps(record.forecast_payload),
        safe_json_dumps(record.raw_payload),
        _sql_timestamp(observed_at),
    ]


def forecast_replay_to_row(record: ForecastReplayRecord) -> list[Any]:
    return [
        record.replay_id,
        record.market_id,
        record.condition_id,
        record.station_id,
        record.source,
        record.model_run,
        _sql_timestamp(record.forecast_target_time),
        record.spec_version,
        record.replay_key,
        record.replay_reason,
        record.original_run_id,
        record.replayed_run_id,
        _sql_timestamp(record.created_at),
    ]


def forecast_replay_diff_to_row(record: ForecastReplayDiffRecord) -> list[Any]:
    return [
        record.diff_id,
        record.replay_id,
        record.entity_type,
        record.entity_key,
        record.original_entity_id,
        record.replayed_entity_id,
        record.status,
        safe_json_dumps(record.diff_summary_json),
        _sql_timestamp(record.created_at),
    ]


def _sql_timestamp(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
