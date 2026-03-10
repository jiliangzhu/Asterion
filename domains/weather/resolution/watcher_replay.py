from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import ProposalStatus, StateTransition, UMAProposal, new_event_id, stable_object_id
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.utils import safe_json_dumps


UMA_PROPOSAL_COLUMNS = [
    "proposal_id",
    "market_id",
    "condition_id",
    "proposer",
    "proposed_outcome",
    "proposal_bond",
    "dispute_bond",
    "proposal_tx_hash",
    "proposal_block_number",
    "proposal_timestamp",
    "status",
    "on_chain_settled_at",
    "safe_redeem_after",
    "human_review_required",
    "created_at",
    "updated_at",
]

PROPOSAL_TRANSITION_COLUMNS = [
    "transition_id",
    "proposal_id",
    "old_status",
    "new_status",
    "tx_hash",
    "block_number",
    "event_type",
    "recorded_at",
]

PROCESSED_EVENT_COLUMNS = [
    "event_id",
    "tx_hash",
    "log_index",
    "block_number",
    "processed_at",
]

BLOCK_WATERMARK_COLUMNS = [
    "chain_id",
    "last_processed_block",
    "last_finalized_block",
    "updated_at",
]


@dataclass(frozen=True)
class UMAEvent:
    tx_hash: str
    log_index: int
    block_number: int
    event_type: str
    proposal_id: str
    market_id: str
    condition_id: str
    proposer: str | None
    proposed_outcome: str | None
    proposal_bond: float | None
    dispute_bond: float | None
    proposal_timestamp: datetime | None
    on_chain_settled_at: datetime | None
    safe_redeem_after: datetime | None
    human_review_required: bool

    @property
    def event_id(self) -> str:
        return stable_object_id(
            "umaevt",
            {
                "block_number": self.block_number,
                "log_index": self.log_index,
                "tx_hash": self.tx_hash,
            },
        )


@dataclass(frozen=True)
class BlockWatermarkRecord:
    chain_id: int
    last_processed_block: int
    last_finalized_block: int


def replay_uma_events(
    *,
    events: list[UMAEvent],
    existing_proposals: dict[str, UMAProposal] | None = None,
    processed_event_ids: set[str] | None = None,
) -> tuple[list[UMAProposal], list[StateTransition], list[str]]:
    projections = dict(existing_proposals or {})
    seen = set(processed_event_ids or set())
    transitions: list[StateTransition] = []
    new_event_ids: list[str] = []

    for event in sorted(events, key=lambda item: (item.block_number, item.log_index)):
        event_id = event.event_id
        if event_id in seen:
            continue
        previous = projections.get(event.proposal_id)
        proposal, transition = apply_uma_event(event, previous)
        projections[event.proposal_id] = proposal
        if transition is not None:
            transitions.append(transition)
        seen.add(event_id)
        new_event_ids.append(event_id)

    return list(projections.values()), transitions, new_event_ids


def apply_uma_event(event: UMAEvent, current: UMAProposal | None) -> tuple[UMAProposal, StateTransition | None]:
    old_status = current.status if current is not None else ProposalStatus.PENDING
    new_status = _status_from_event_type(event.event_type, fallback=old_status)
    created_at = _normalize_ts(event.proposal_timestamp) or datetime.now(UTC).replace(tzinfo=None)

    proposal = UMAProposal(
        proposal_id=event.proposal_id,
        market_id=event.market_id,
        condition_id=event.condition_id,
        proposer=(event.proposer or (current.proposer if current is not None else "unknown")),
        proposed_outcome=(event.proposed_outcome or (current.proposed_outcome if current is not None else "unknown")),
        proposal_bond=float(event.proposal_bond if event.proposal_bond is not None else (current.proposal_bond if current is not None else 0.0)),
        dispute_bond=float(
            event.dispute_bond
            if event.dispute_bond is not None
            else (current.dispute_bond if current is not None and current.dispute_bond is not None else 0.0)
        ),
        proposal_tx_hash=current.proposal_tx_hash if current is not None and old_status != ProposalStatus.PENDING else event.tx_hash,
        proposal_block_number=current.proposal_block_number if current is not None and old_status != ProposalStatus.PENDING else event.block_number,
        proposal_timestamp=current.proposal_timestamp if current is not None and old_status != ProposalStatus.PENDING else created_at,
        status=new_status,
        on_chain_settled_at=_normalize_ts(event.on_chain_settled_at) or (current.on_chain_settled_at if current is not None else None),
        safe_redeem_after=_normalize_ts(event.safe_redeem_after) or (current.safe_redeem_after if current is not None else None),
        human_review_required=bool(
            event.human_review_required if event.human_review_required else (current.human_review_required if current is not None else False)
        ),
    )

    transition = None
    if current is None or old_status != new_status:
        transition = StateTransition(
            proposal_id=proposal.proposal_id,
            old_status=old_status,
            new_status=new_status,
            block_number=event.block_number,
            tx_hash=event.tx_hash,
            event_type=event.event_type,
            recorded_at=_normalize_ts(event.on_chain_settled_at) or created_at,
        )
    return proposal, transition


def enqueue_uma_replay_writes(
    queue_cfg: WriteQueueConfig,
    *,
    chain_id: int,
    proposals: list[UMAProposal],
    transitions: list[StateTransition],
    processed_events: list[UMAEvent],
    last_processed_block: int,
    last_finalized_block: int,
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> list[str]:
    now = (observed_at or datetime.now(UTC).replace(tzinfo=None)).replace(microsecond=0)
    task_ids: list[str] = []

    proposal_rows = [uma_proposal_to_row(item, observed_at=now) for item in proposals]
    if proposal_rows:
        task_id = enqueue_upsert_rows_v1(
            queue_cfg,
            table="resolution.uma_proposals",
            pk_cols=["proposal_id"],
            columns=list(UMA_PROPOSAL_COLUMNS),
            rows=proposal_rows,
            run_id=run_id,
        )
        task_ids.append(task_id)

    transition_rows = [proposal_transition_to_row(item) for item in transitions]
    if transition_rows:
        task_id = enqueue_upsert_rows_v1(
            queue_cfg,
            table="resolution.proposal_state_transitions",
            pk_cols=["transition_id"],
            columns=list(PROPOSAL_TRANSITION_COLUMNS),
            rows=transition_rows,
            run_id=run_id,
        )
        task_ids.append(task_id)

    processed_rows = [processed_event_to_row(item, observed_at=now) for item in processed_events]
    if processed_rows:
        task_id = enqueue_upsert_rows_v1(
            queue_cfg,
            table="resolution.processed_uma_events",
            pk_cols=["event_id"],
            columns=list(PROCESSED_EVENT_COLUMNS),
            rows=processed_rows,
            run_id=run_id,
        )
        task_ids.append(task_id)

    watermark = BlockWatermarkRecord(
        chain_id=int(chain_id),
        last_processed_block=int(last_processed_block),
        last_finalized_block=int(last_finalized_block),
    )
    task_id = enqueue_upsert_rows_v1(
        queue_cfg,
        table="resolution.block_watermarks",
        pk_cols=["chain_id"],
        columns=list(BLOCK_WATERMARK_COLUMNS),
        rows=[block_watermark_to_row(watermark, observed_at=now)],
        run_id=run_id,
    )
    task_ids.append(task_id)
    return task_ids


def load_processed_event_ids(con) -> set[str]:
    rows = con.execute("SELECT event_id FROM resolution.processed_uma_events").fetchall()
    return {str(row[0]) for row in rows}


def load_uma_proposals(con) -> dict[str, UMAProposal]:
    rows = con.execute(
        """
        SELECT
            proposal_id,
            market_id,
            condition_id,
            proposer,
            proposed_outcome,
            proposal_bond,
            dispute_bond,
            proposal_tx_hash,
            proposal_block_number,
            proposal_timestamp,
            status,
            on_chain_settled_at,
            safe_redeem_after,
            human_review_required
        FROM resolution.uma_proposals
        """
    ).fetchall()
    out: dict[str, UMAProposal] = {}
    for row in rows:
        out[str(row[0])] = UMAProposal(
            proposal_id=row[0],
            market_id=row[1],
            condition_id=row[2],
            proposer=row[3],
            proposed_outcome=row[4],
            proposal_bond=float(row[5]),
            dispute_bond=float(row[6]) if row[6] is not None else None,
            proposal_tx_hash=row[7],
            proposal_block_number=int(row[8]),
            proposal_timestamp=row[9],
            status=ProposalStatus(row[10]),
            on_chain_settled_at=row[11],
            safe_redeem_after=row[12],
            human_review_required=bool(row[13]),
        )
    return out


def load_block_watermark(con, *, chain_id: int) -> BlockWatermarkRecord | None:
    row = con.execute(
        """
        SELECT chain_id, last_processed_block, last_finalized_block
        FROM resolution.block_watermarks
        WHERE chain_id = ?
        """,
        [int(chain_id)],
    ).fetchone()
    if row is None:
        return None
    return BlockWatermarkRecord(
        chain_id=int(row[0]),
        last_processed_block=int(row[1]),
        last_finalized_block=int(row[2]),
    )


def uma_proposal_to_row(proposal: UMAProposal, *, observed_at: datetime) -> list[Any]:
    created_at = _sql_ts(proposal.proposal_timestamp)
    return [
        proposal.proposal_id,
        proposal.market_id,
        proposal.condition_id,
        proposal.proposer,
        proposal.proposed_outcome,
        proposal.proposal_bond,
        proposal.dispute_bond,
        proposal.proposal_tx_hash,
        proposal.proposal_block_number,
        created_at,
        proposal.status.value,
        _sql_ts(proposal.on_chain_settled_at),
        _sql_ts(proposal.safe_redeem_after),
        proposal.human_review_required,
        created_at,
        _sql_ts(observed_at),
    ]


def proposal_transition_to_row(transition: StateTransition) -> list[Any]:
    return [
        stable_object_id(
            "trn",
            {
                "block_number": transition.block_number,
                "event_type": transition.event_type,
                "proposal_id": transition.proposal_id,
                "tx_hash": transition.tx_hash,
            },
        ),
        transition.proposal_id,
        transition.old_status.value,
        transition.new_status.value,
        transition.tx_hash,
        transition.block_number,
        transition.event_type,
        _sql_ts(transition.recorded_at),
    ]


def processed_event_to_row(event: UMAEvent, *, observed_at: datetime) -> list[Any]:
    return [
        event.event_id,
        event.tx_hash,
        event.log_index,
        event.block_number,
        _sql_ts(observed_at),
    ]


def block_watermark_to_row(record: BlockWatermarkRecord, *, observed_at: datetime) -> list[Any]:
    return [
        record.chain_id,
        record.last_processed_block,
        record.last_finalized_block,
        _sql_ts(observed_at),
    ]


def _status_from_event_type(event_type: str, *, fallback: ProposalStatus) -> ProposalStatus:
    text = event_type.strip().lower()
    if text in {"proposal_created", "proposed"}:
        return ProposalStatus.PROPOSED
    if text in {"proposal_disputed", "disputed"}:
        return ProposalStatus.DISPUTED
    if text in {"proposal_settled", "settled"}:
        return ProposalStatus.SETTLED
    if text in {"proposal_redeemed", "redeemed"}:
        return ProposalStatus.REDEEMED
    return fallback


def _normalize_ts(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _sql_ts(value: datetime | None) -> str | None:
    normalized = _normalize_ts(value)
    if normalized is None:
        return None
    return normalized.isoformat(sep=" ", timespec="seconds")
