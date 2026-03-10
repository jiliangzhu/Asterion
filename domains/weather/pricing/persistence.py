from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import WatchOnlySnapshotRecord, WeatherFairValueRecord
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


WEATHER_FAIR_VALUE_COLUMNS = [
    "fair_value_id",
    "run_id",
    "market_id",
    "condition_id",
    "token_id",
    "outcome",
    "fair_value",
    "confidence",
    "priced_at",
]

WEATHER_WATCH_ONLY_SNAPSHOT_COLUMNS = [
    "snapshot_id",
    "fair_value_id",
    "run_id",
    "market_id",
    "condition_id",
    "token_id",
    "outcome",
    "reference_price",
    "fair_value",
    "edge_bps",
    "threshold_bps",
    "decision",
    "side",
    "rationale",
    "pricing_context_json",
    "created_at",
]


def enqueue_fair_value_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    fair_values: list[WeatherFairValueRecord],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> str | None:
    if not fair_values:
        return None
    now = (observed_at or datetime.now(UTC).replace(tzinfo=None)).replace(microsecond=0)
    rows = [fair_value_to_row(item, observed_at=now) for item in fair_values]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_fair_values",
        pk_cols=["fair_value_id"],
        columns=list(WEATHER_FAIR_VALUE_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def enqueue_watch_only_snapshot_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    snapshots: list[WatchOnlySnapshotRecord],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> str | None:
    if not snapshots:
        return None
    now = (observed_at or datetime.now(UTC).replace(tzinfo=None)).replace(microsecond=0)
    rows = [watch_only_snapshot_to_row(item, observed_at=now) for item in snapshots]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_watch_only_snapshots",
        pk_cols=["snapshot_id"],
        columns=list(WEATHER_WATCH_ONLY_SNAPSHOT_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def fair_value_to_row(record: WeatherFairValueRecord, *, observed_at: datetime) -> list[Any]:
    return [
        record.fair_value_id,
        record.run_id,
        record.market_id,
        record.condition_id,
        record.token_id,
        record.outcome,
        record.fair_value,
        record.confidence,
        _sql_timestamp(observed_at),
    ]


def watch_only_snapshot_to_row(record: WatchOnlySnapshotRecord, *, observed_at: datetime) -> list[Any]:
    return [
        record.snapshot_id,
        record.fair_value_id,
        record.run_id,
        record.market_id,
        record.condition_id,
        record.token_id,
        record.outcome,
        record.reference_price,
        record.fair_value,
        record.edge_bps,
        record.threshold_bps,
        record.decision,
        record.side,
        record.rationale,
        safe_json_dumps(record.pricing_context),
        _sql_timestamp(observed_at),
    ]


def _sql_timestamp(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
