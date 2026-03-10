from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from asterion_core.contracts import EvidencePackageLinkRecord, RedeemReadinessRecord, SettlementVerificationRecord
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.utils import safe_json_dumps


SETTLEMENT_VERIFICATION_COLUMNS = [
    "verification_id",
    "proposal_id",
    "market_id",
    "proposed_outcome",
    "expected_outcome",
    "is_correct",
    "confidence",
    "discrepancy_details",
    "sources_checked",
    "evidence_package",
    "created_at",
]

PROPOSAL_EVIDENCE_LINK_COLUMNS = [
    "proposal_id",
    "verification_id",
    "evidence_package_id",
    "linked_at",
]

REDEEM_READINESS_COLUMNS = [
    "suggestion_id",
    "proposal_id",
    "decision",
    "reason",
    "on_chain_settled_at",
    "safe_redeem_after",
    "human_review_required",
    "created_at",
]


def enqueue_settlement_verification_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    verifications: list[SettlementVerificationRecord],
    run_id: str | None = None,
) -> str | None:
    if not verifications:
        return None
    rows = [settlement_verification_to_row(item) for item in verifications]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="resolution.settlement_verifications",
        pk_cols=["verification_id"],
        columns=list(SETTLEMENT_VERIFICATION_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def enqueue_evidence_link_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    links: list[EvidencePackageLinkRecord],
    run_id: str | None = None,
) -> str | None:
    if not links:
        return None
    rows = [evidence_link_to_row(item) for item in links]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="resolution.proposal_evidence_links",
        pk_cols=["proposal_id", "verification_id"],
        columns=list(PROPOSAL_EVIDENCE_LINK_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def enqueue_redeem_readiness_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    suggestions: list[RedeemReadinessRecord],
    run_id: str | None = None,
) -> str | None:
    if not suggestions:
        return None
    rows = [redeem_readiness_to_row(item) for item in suggestions]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="resolution.redeem_readiness_suggestions",
        pk_cols=["suggestion_id"],
        columns=list(REDEEM_READINESS_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def settlement_verification_to_row(record: SettlementVerificationRecord) -> list[Any]:
    return [
        record.verification_id,
        record.proposal_id,
        record.market_id,
        record.proposed_outcome,
        record.expected_outcome,
        record.is_correct,
        record.confidence,
        record.discrepancy_details,
        safe_json_dumps(record.sources_checked),
        safe_json_dumps({"evidence_package_id": record.evidence_package_id}),
        _sql_ts(record.created_at),
    ]


def evidence_link_to_row(record: EvidencePackageLinkRecord) -> list[Any]:
    return [
        record.proposal_id,
        record.verification_id,
        record.evidence_package_id,
        _sql_ts(record.linked_at),
    ]


def redeem_readiness_to_row(record: RedeemReadinessRecord) -> list[Any]:
    return [
        record.suggestion_id,
        record.proposal_id,
        record.decision.value,
        record.reason,
        _sql_ts(record.on_chain_settled_at),
        _sql_ts(record.safe_redeem_after),
        record.human_review_required,
        _sql_ts(record.created_at),
    ]


def _sql_ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    normalized = value
    if normalized.tzinfo is not None:
        normalized = normalized.astimezone(UTC).replace(tzinfo=None)
    return normalized.isoformat(sep=" ", timespec="seconds")
