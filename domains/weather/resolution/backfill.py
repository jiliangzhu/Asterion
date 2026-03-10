from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import StateTransition, UMAProposal, new_request_id
from asterion_core.storage.write_queue import WriteQueueConfig

from .continuity import (
    ContinuityEvaluation,
    build_rpc_incomplete_continuity,
    enqueue_continuity_check_upserts,
    enqueue_continuity_gap_upserts,
    evaluate_continuity,
)
from .rpc_fallback import FallbackRpcPool, RpcFallbackError, rpc_trace_to_json
from .watcher_replay import (
    UMAEvent,
    enqueue_uma_replay_writes,
    load_block_watermark,
    load_processed_event_ids,
    load_uma_proposals,
    replay_uma_events,
)


@dataclass(frozen=True)
class WatcherBackfillRequest:
    chain_id: int
    from_block: int
    to_block: int
    replay_reason: str

    def __post_init__(self) -> None:
        if self.chain_id < 0:
            raise ValueError("chain_id must be non-negative")
        if self.from_block < 0 or self.to_block < 0:
            raise ValueError("block numbers must be non-negative")
        if not self.replay_reason:
            raise ValueError("replay_reason is required")


@dataclass(frozen=True)
class WatcherBackfillResult:
    run_id: str
    chain_id: int
    from_block: int
    to_block: int
    finalized_block: int
    events_loaded: int
    proposals_upserted: int
    transitions_written: int
    processed_events_written: int
    continuity_check_id: str
    rpc_trace: dict[str, Any]
    proposals: list[UMAProposal] = field(repr=False, default_factory=list)
    transitions: list[StateTransition] = field(repr=False, default_factory=list)
    processed_events: list[UMAEvent] = field(repr=False, default_factory=list)
    continuity: ContinuityEvaluation | None = field(repr=False, default=None)
    next_last_processed_block: int | None = field(repr=False, default=None)
    next_last_finalized_block: int | None = field(repr=False, default=None)


def build_backfill_request(
    con,
    *,
    chain_id: int,
    finalized_block: int,
    replay_reason: str,
    max_block_span: int | None = None,
) -> WatcherBackfillRequest:
    watermark = load_block_watermark(con, chain_id=chain_id)
    from_block = 0 if watermark is None else watermark.last_finalized_block + 1
    to_block = int(finalized_block)
    if max_block_span is not None:
        span = max(1, int(max_block_span))
        to_block = min(to_block, from_block + span - 1)
    return WatcherBackfillRequest(
        chain_id=int(chain_id),
        from_block=int(from_block),
        to_block=int(to_block),
        replay_reason=replay_reason,
    )


def run_watcher_backfill(
    con,
    rpc_pool: FallbackRpcPool,
    *,
    chain_id: int,
    replay_reason: str,
    max_block_span: int | None = None,
    observed_at: datetime | None = None,
) -> WatcherBackfillResult:
    now = _normalize_observed_at(observed_at)
    run_id = new_request_id()
    watermark = load_block_watermark(con, chain_id=chain_id)
    fallback_from_block = 0 if watermark is None else watermark.last_finalized_block + 1

    try:
        finalized_block, finalized_trace = rpc_pool.get_finalized_block_number()
    except RpcFallbackError as exc:
        continuity = build_rpc_incomplete_continuity(
            chain_id=chain_id,
            from_block=fallback_from_block,
            to_block=fallback_from_block,
            watermark=watermark,
            rpc_trace={"finalized_block": rpc_trace_to_json(exc.trace)},
            reason="finalized_block_read_failed",
            created_at=now,
        )
        return WatcherBackfillResult(
            run_id=run_id,
            chain_id=chain_id,
            from_block=fallback_from_block,
            to_block=fallback_from_block,
            finalized_block=watermark.last_finalized_block if watermark is not None else 0,
            events_loaded=0,
            proposals_upserted=0,
            transitions_written=0,
            processed_events_written=0,
            continuity_check_id=continuity.check.check_id,
            rpc_trace={"finalized_block": rpc_trace_to_json(exc.trace)},
            continuity=continuity,
        )

    request = build_backfill_request(
        con,
        chain_id=chain_id,
        finalized_block=finalized_block,
        replay_reason=replay_reason,
        max_block_span=max_block_span,
    )

    if request.from_block > request.to_block:
        continuity = evaluate_continuity(
            chain_id=chain_id,
            from_block=request.from_block,
            to_block=request.to_block,
            watermark=watermark,
            events=[],
            processed_event_ids=set(),
            rpc_trace={"finalized_block": rpc_trace_to_json(finalized_trace)},
            created_at=now,
        )
        return WatcherBackfillResult(
            run_id=run_id,
            chain_id=chain_id,
            from_block=request.from_block,
            to_block=request.to_block,
            finalized_block=finalized_block,
            events_loaded=0,
            proposals_upserted=0,
            transitions_written=0,
            processed_events_written=0,
            continuity_check_id=continuity.check.check_id,
            rpc_trace={"finalized_block": rpc_trace_to_json(finalized_trace)},
            continuity=continuity,
        )

    try:
        events, events_trace = rpc_pool.get_events(request.from_block, request.to_block)
    except RpcFallbackError as exc:
        continuity = build_rpc_incomplete_continuity(
            chain_id=chain_id,
            from_block=request.from_block,
            to_block=request.to_block,
            watermark=watermark,
            rpc_trace={
                "finalized_block": rpc_trace_to_json(finalized_trace),
                "events": rpc_trace_to_json(exc.trace),
            },
            reason="event_range_read_failed",
            created_at=now,
        )
        return WatcherBackfillResult(
            run_id=run_id,
            chain_id=chain_id,
            from_block=request.from_block,
            to_block=request.to_block,
            finalized_block=finalized_block,
            events_loaded=0,
            proposals_upserted=0,
            transitions_written=0,
            processed_events_written=0,
            continuity_check_id=continuity.check.check_id,
            rpc_trace={
                "finalized_block": rpc_trace_to_json(finalized_trace),
                "events": rpc_trace_to_json(exc.trace),
            },
            continuity=continuity,
        )

    proposals = load_uma_proposals(con)
    processed_event_ids = load_processed_event_ids(con)
    replayed_proposals, transitions, new_event_ids = replay_uma_events(
        events=events,
        existing_proposals=proposals,
        processed_event_ids=processed_event_ids,
    )
    continuity = evaluate_continuity(
        chain_id=chain_id,
        from_block=request.from_block,
        to_block=request.to_block,
        watermark=watermark,
        events=events,
        processed_event_ids=processed_event_ids,
        rpc_trace={
            "finalized_block": rpc_trace_to_json(finalized_trace),
            "events": rpc_trace_to_json(events_trace),
        },
        created_at=now,
    )
    processed_events = [event for event in sorted(events, key=lambda item: (item.block_number, item.log_index)) if event.event_id in set(new_event_ids)]
    return WatcherBackfillResult(
        run_id=run_id,
        chain_id=chain_id,
        from_block=request.from_block,
        to_block=request.to_block,
        finalized_block=finalized_block,
        events_loaded=len(events),
        proposals_upserted=len(replayed_proposals),
        transitions_written=len(transitions),
        processed_events_written=len(processed_events),
        continuity_check_id=continuity.check.check_id,
        rpc_trace={
            "finalized_block": rpc_trace_to_json(finalized_trace),
            "events": rpc_trace_to_json(events_trace),
        },
        proposals=replayed_proposals,
        transitions=transitions,
        processed_events=processed_events,
        continuity=continuity,
        next_last_processed_block=request.to_block,
        next_last_finalized_block=finalized_block,
    )


def persist_watcher_backfill(
    queue_cfg: WriteQueueConfig,
    result: WatcherBackfillResult,
    *,
    observed_at: datetime | None = None,
) -> list[str]:
    now = _normalize_observed_at(observed_at)
    task_ids: list[str] = []
    if result.next_last_processed_block is not None and result.next_last_finalized_block is not None:
        task_ids.extend(
            enqueue_uma_replay_writes(
                queue_cfg,
                chain_id=result.chain_id,
                proposals=result.proposals,
                transitions=result.transitions,
                processed_events=result.processed_events,
                last_processed_block=result.next_last_processed_block,
                last_finalized_block=result.next_last_finalized_block,
                run_id=result.run_id,
                observed_at=now,
            )
        )
    if result.continuity is not None:
        check_task_id = enqueue_continuity_check_upserts(
            queue_cfg,
            checks=[result.continuity.check],
            run_id=result.run_id,
        )
        if check_task_id is not None:
            task_ids.append(check_task_id)
        gap_task_id = enqueue_continuity_gap_upserts(
            queue_cfg,
            gaps=result.continuity.gaps,
            run_id=result.run_id,
        )
        if gap_task_id is not None:
            task_ids.append(gap_task_id)
    return task_ids


def _normalize_observed_at(value: datetime | None) -> datetime:
    observed_at = value or datetime.now(UTC)
    if observed_at.tzinfo is None:
        return observed_at.replace(microsecond=0)
    return observed_at.astimezone(UTC).replace(tzinfo=None, microsecond=0)
