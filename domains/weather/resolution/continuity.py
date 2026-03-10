from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import WatcherContinuityCheck, WatcherContinuityGap, stable_object_id
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig

from .watcher_replay import BlockWatermarkRecord, UMAEvent


WATCHER_CONTINUITY_CHECK_COLUMNS = [
    "check_id",
    "chain_id",
    "from_block",
    "to_block",
    "last_known_finalized_block",
    "status",
    "gap_count",
    "details_json",
    "created_at",
]

WATCHER_CONTINUITY_GAP_COLUMNS = [
    "gap_id",
    "check_id",
    "gap_type",
    "severity",
    "block_start",
    "block_end",
    "entity_ref",
    "details_json",
    "created_at",
]


@dataclass(frozen=True)
class ContinuityEvaluation:
    check: WatcherContinuityCheck
    gaps: list[WatcherContinuityGap]


def load_last_processed_block(con, *, chain_id: int) -> int | None:
    row = con.execute(
        """
        SELECT last_processed_block
        FROM resolution.block_watermarks
        WHERE chain_id = ?
        """,
        [int(chain_id)],
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def evaluate_continuity(
    *,
    chain_id: int,
    from_block: int,
    to_block: int,
    watermark: BlockWatermarkRecord | None,
    events: list[UMAEvent],
    processed_event_ids: set[str] | None = None,
    rpc_trace: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> ContinuityEvaluation:
    observed_at = _normalize_created_at(created_at)
    gaps: list[WatcherContinuityGap] = []
    details: dict[str, Any] = {
        "event_count": len(events),
        "rpc_trace": rpc_trace or {},
    }
    last_known_finalized = watermark.last_finalized_block if watermark is not None else 0
    last_processed = watermark.last_processed_block if watermark is not None else -1

    if from_block > to_block:
        details["reason"] = "invalid_range"
        check = WatcherContinuityCheck(
            check_id=_build_check_id(chain_id=chain_id, from_block=from_block, to_block=to_block, status="INVALID_RANGE"),
            chain_id=chain_id,
            from_block=from_block,
            to_block=to_block,
            last_known_finalized_block=max(last_known_finalized, 0),
            status="INVALID_RANGE",
            gap_count=0,
            details_json=details,
            created_at=observed_at,
        )
        return ContinuityEvaluation(check=check, gaps=[])

    if watermark is not None and from_block <= last_processed:
        gaps.append(
            _build_gap(
                check_seed=(chain_id, from_block, to_block, "WATERMARK_REGRESSION"),
                gap_type="WATERMARK_REGRESSION",
                severity="ERROR",
                block_start=from_block,
                block_end=min(last_processed, to_block),
                entity_ref=str(chain_id),
                details_json={
                    "from_block": from_block,
                    "last_processed_block": last_processed,
                },
                created_at=observed_at,
            )
        )

    if watermark is not None and from_block != watermark.last_finalized_block + 1:
        gaps.append(
            _build_gap(
                check_seed=(chain_id, from_block, to_block, "BLOCK_GAP"),
                gap_type="BLOCK_GAP",
                severity="ERROR",
                block_start=min(from_block, watermark.last_finalized_block + 1),
                block_end=max(from_block, watermark.last_finalized_block + 1),
                entity_ref=str(chain_id),
                details_json={
                    "expected_from_block": watermark.last_finalized_block + 1,
                    "from_block": from_block,
                },
                created_at=observed_at,
            )
        )

    if watermark is not None and to_block <= last_processed:
        gaps.append(
            _build_gap(
                check_seed=(chain_id, from_block, to_block, "DUPLICATE_RANGE"),
                gap_type="DUPLICATE_RANGE",
                severity="WARN",
                block_start=from_block,
                block_end=to_block,
                entity_ref=str(chain_id),
                details_json={
                    "last_processed_block": last_processed,
                    "to_block": to_block,
                },
                created_at=observed_at,
            )
        )

    processed = processed_event_ids or set()
    last_log_index_by_stream: dict[tuple[int, str], int] = {}
    raw_seen_event_ids: set[str] = set()
    for event in events:
        stream_key = (event.block_number, event.tx_hash)
        previous_log_index = last_log_index_by_stream.get(stream_key)
        if previous_log_index is not None and event.log_index <= previous_log_index:
            gaps.append(
                _build_gap(
                    check_seed=(chain_id, event.block_number, event.log_index, "EVENT_GAP", event.tx_hash),
                    gap_type="EVENT_GAP",
                    severity="ERROR",
                    block_start=event.block_number,
                    block_end=event.block_number,
                    entity_ref=event.tx_hash,
                    details_json={
                        "event_id": event.event_id,
                        "log_index": event.log_index,
                        "previous_log_index": previous_log_index,
                    },
                    created_at=observed_at,
                )
            )
        last_log_index_by_stream[stream_key] = event.log_index

        if event.event_id in raw_seen_event_ids and event.event_id not in processed:
            gaps.append(
                _build_gap(
                    check_seed=(chain_id, event.block_number, event.log_index, "EVENT_DUPLICATE", event.event_id),
                    gap_type="EVENT_GAP",
                    severity="ERROR",
                    block_start=event.block_number,
                    block_end=event.block_number,
                    entity_ref=event.event_id,
                    details_json={
                        "event_id": event.event_id,
                        "duplicate_raw_event": True,
                    },
                    created_at=observed_at,
                )
            )
        raw_seen_event_ids.add(event.event_id)

    status = "OK" if not gaps else "GAP_DETECTED"
    check = WatcherContinuityCheck(
        check_id=_build_check_id(chain_id=chain_id, from_block=from_block, to_block=to_block, status=status),
        chain_id=chain_id,
        from_block=from_block,
        to_block=to_block,
        last_known_finalized_block=max(last_known_finalized, 0),
        status=status,
        gap_count=len(gaps),
        details_json=details,
        created_at=observed_at,
    )
    materialized_gaps = [
        WatcherContinuityGap(
            gap_id=item.gap_id,
            check_id=check.check_id,
            gap_type=item.gap_type,
            severity=item.severity,
            block_start=item.block_start,
            block_end=item.block_end,
            entity_ref=item.entity_ref,
            details_json=item.details_json,
            created_at=item.created_at,
        )
        for item in gaps
    ]
    return ContinuityEvaluation(check=check, gaps=materialized_gaps)


def build_rpc_incomplete_continuity(
    *,
    chain_id: int,
    from_block: int,
    to_block: int,
    watermark: BlockWatermarkRecord | None,
    rpc_trace: dict[str, Any],
    reason: str,
    created_at: datetime | None = None,
) -> ContinuityEvaluation:
    observed_at = _normalize_created_at(created_at)
    last_known_finalized = watermark.last_finalized_block if watermark is not None else 0
    check_id = _build_check_id(chain_id=chain_id, from_block=from_block, to_block=to_block, status="RPC_INCOMPLETE")
    gap = WatcherContinuityGap(
        gap_id=stable_object_id(
            "wcgap",
            {
                "chain_id": chain_id,
                "check_id": check_id,
                "gap_type": "RPC_INCOMPLETE",
                "reason": reason,
            },
        ),
        check_id=check_id,
        gap_type="RPC_INCOMPLETE",
        severity="ERROR",
        block_start=max(from_block, 0),
        block_end=max(to_block, max(from_block, 0)),
        entity_ref=str(chain_id),
        details_json={
            "reason": reason,
            "rpc_trace": rpc_trace,
        },
        created_at=observed_at,
    )
    check = WatcherContinuityCheck(
        check_id=check_id,
        chain_id=chain_id,
        from_block=max(from_block, 0),
        to_block=max(to_block, max(from_block, 0)),
        last_known_finalized_block=max(last_known_finalized, 0),
        status="RPC_INCOMPLETE",
        gap_count=1,
        details_json={
            "reason": reason,
            "rpc_trace": rpc_trace,
        },
        created_at=observed_at,
    )
    return ContinuityEvaluation(check=check, gaps=[gap])


def enqueue_continuity_check_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    checks: list[WatcherContinuityCheck],
    run_id: str | None = None,
) -> str | None:
    if not checks:
        return None
    rows = [watcher_continuity_check_to_row(item) for item in checks]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="resolution.watcher_continuity_checks",
        pk_cols=["check_id"],
        columns=list(WATCHER_CONTINUITY_CHECK_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def enqueue_continuity_gap_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    gaps: list[WatcherContinuityGap],
    run_id: str | None = None,
) -> str | None:
    if not gaps:
        return None
    rows = [watcher_continuity_gap_to_row(item) for item in gaps]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="resolution.watcher_continuity_gaps",
        pk_cols=["gap_id"],
        columns=list(WATCHER_CONTINUITY_GAP_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def watcher_continuity_check_to_row(record: WatcherContinuityCheck) -> list[Any]:
    return [
        record.check_id,
        record.chain_id,
        record.from_block,
        record.to_block,
        record.last_known_finalized_block,
        record.status,
        record.gap_count,
        safe_json_dumps(record.details_json),
        _sql_ts(record.created_at),
    ]


def watcher_continuity_gap_to_row(record: WatcherContinuityGap) -> list[Any]:
    return [
        record.gap_id,
        record.check_id,
        record.gap_type,
        record.severity,
        record.block_start,
        record.block_end,
        record.entity_ref,
        safe_json_dumps(record.details_json),
        _sql_ts(record.created_at),
    ]


def _build_check_id(*, chain_id: int, from_block: int, to_block: int, status: str) -> str:
    return stable_object_id(
        "wcck",
        {
            "chain_id": chain_id,
            "from_block": from_block,
            "status": status,
            "to_block": to_block,
        },
    )


def _build_gap(
    *,
    check_seed: tuple[Any, ...],
    gap_type: str,
    severity: str,
    block_start: int,
    block_end: int,
    entity_ref: str | None,
    details_json: dict[str, Any],
    created_at: datetime,
) -> WatcherContinuityGap:
    return WatcherContinuityGap(
        gap_id=stable_object_id(
            "wcgap",
            {
                "check_seed": list(check_seed),
                "entity_ref": entity_ref,
                "gap_type": gap_type,
            },
        ),
        check_id="pending",
        gap_type=gap_type,
        severity=severity,
        block_start=max(block_start, 0),
        block_end=max(block_end, 0),
        entity_ref=entity_ref,
        details_json=details_json,
        created_at=created_at,
    )


def _normalize_created_at(value: datetime | None) -> datetime:
    observed_at = value or datetime.now(UTC)
    if observed_at.tzinfo is None:
        return observed_at
    return observed_at.astimezone(UTC).replace(tzinfo=None)


def _sql_ts(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
