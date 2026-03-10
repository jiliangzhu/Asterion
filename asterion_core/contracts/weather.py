from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any

from .ids import build_forecast_cache_key


@dataclass(frozen=True)
class WeatherMarket:
    market_id: str
    condition_id: str
    event_id: str | None
    slug: str | None
    title: str
    description: str | None
    rules: str | None
    status: str
    active: bool
    closed: bool
    archived: bool
    accepting_orders: bool | None
    enable_order_book: bool | None
    tags: list[str]
    outcomes: list[str]
    token_ids: list[str]
    close_time: datetime | None
    end_date: datetime | None
    raw_market: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.market_id or not self.condition_id:
            raise ValueError("market_id and condition_id are required")
        if not self.title:
            raise ValueError("title is required")
        if not self.status:
            raise ValueError("status is required")


@dataclass(frozen=True)
class Rule2SpecDraft:
    market_id: str
    condition_id: str
    location_name: str
    observation_date: date
    observation_window_local: str
    metric: str
    unit: str
    bucket_min_value: float | None
    bucket_max_value: float | None
    authoritative_source: str
    fallback_sources: list[str]
    rounding_rule: str
    inclusive_bounds: bool
    parse_confidence: float
    risk_flags: list[str]

    def __post_init__(self) -> None:
        if not self.market_id or not self.condition_id:
            raise ValueError("market_id and condition_id are required")
        if not self.location_name:
            raise ValueError("location_name is required")
        if not self.observation_window_local:
            raise ValueError("observation_window_local is required")
        if not self.metric or not self.unit:
            raise ValueError("metric and unit are required")
        if not self.authoritative_source:
            raise ValueError("authoritative_source is required")
        if not (0.0 <= float(self.parse_confidence) <= 1.0):
            raise ValueError("parse_confidence must be between 0 and 1")


@dataclass(frozen=True)
class WeatherMarketSpecRecord:
    market_id: str
    condition_id: str
    location_name: str
    station_id: str
    latitude: float
    longitude: float
    timezone: str
    observation_date: date
    observation_window_local: str
    metric: str
    unit: str
    bucket_min_value: float | None
    bucket_max_value: float | None
    authoritative_source: str
    fallback_sources: list[str]
    rounding_rule: str
    inclusive_bounds: bool
    spec_version: str
    parse_confidence: float
    risk_flags: list[str]

    def __post_init__(self) -> None:
        _require_station_first_fields(
            station_id=self.station_id,
            latitude=self.latitude,
            longitude=self.longitude,
            timezone=self.timezone,
        )
        if not self.market_id or not self.condition_id:
            raise ValueError("market_id and condition_id are required")
        if not self.location_name or not self.metric or not self.unit:
            raise ValueError("location_name, metric, and unit are required")
        if not self.observation_window_local:
            raise ValueError("observation_window_local is required")
        if not self.authoritative_source:
            raise ValueError("authoritative_source is required")
        if not self.spec_version:
            raise ValueError("spec_version is required")
        if not (0.0 <= float(self.parse_confidence) <= 1.0):
            raise ValueError("parse_confidence must be between 0 and 1")


@dataclass(frozen=True)
class ForecastRunRecord:
    run_id: str
    market_id: str
    condition_id: str
    station_id: str
    source: str
    model_run: str
    forecast_target_time: datetime
    observation_date: date
    metric: str
    latitude: float
    longitude: float
    timezone: str
    spec_version: str
    cache_key: str
    source_trace: list[str]
    fallback_used: bool
    from_cache: bool
    confidence: float
    forecast_payload: dict[str, Any]
    raw_payload: dict[str, Any]

    def __post_init__(self) -> None:
        _require_station_first_fields(
            station_id=self.station_id,
            latitude=self.latitude,
            longitude=self.longitude,
            timezone=self.timezone,
        )
        if not self.run_id:
            raise ValueError("run_id is required")
        if not self.market_id or not self.condition_id:
            raise ValueError("market_id and condition_id are required")
        if not self.source or not self.model_run or not self.spec_version:
            raise ValueError("source, model_run, and spec_version are required")
        if not self.cache_key:
            raise ValueError("cache_key is required")
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True)
class WeatherFairValueRecord:
    fair_value_id: str
    run_id: str
    market_id: str
    condition_id: str
    token_id: str
    outcome: str
    fair_value: float
    confidence: float

    def __post_init__(self) -> None:
        if not self.fair_value_id or not self.run_id:
            raise ValueError("fair_value_id and run_id are required")
        if not self.market_id or not self.condition_id or not self.token_id:
            raise ValueError("market_id, condition_id, and token_id are required")
        if not self.outcome:
            raise ValueError("outcome is required")
        if not (0.0 <= float(self.fair_value) <= 1.0):
            raise ValueError("fair_value must be between 0 and 1")
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True)
class WatchOnlySnapshotRecord:
    snapshot_id: str
    fair_value_id: str
    run_id: str
    market_id: str
    condition_id: str
    token_id: str
    outcome: str
    reference_price: float
    fair_value: float
    edge_bps: int
    threshold_bps: int
    decision: str
    side: str
    rationale: str
    pricing_context: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.snapshot_id or not self.fair_value_id or not self.run_id:
            raise ValueError("snapshot_id, fair_value_id, and run_id are required")
        if not self.market_id or not self.condition_id or not self.token_id:
            raise ValueError("market_id, condition_id, and token_id are required")
        if not self.outcome:
            raise ValueError("outcome is required")
        if not (0.0 <= float(self.reference_price) <= 1.0):
            raise ValueError("reference_price must be between 0 and 1")
        if not (0.0 <= float(self.fair_value) <= 1.0):
            raise ValueError("fair_value must be between 0 and 1")
        if not self.decision or not self.side:
            raise ValueError("decision and side are required")


@dataclass(frozen=True)
class ForecastResolutionContract:
    market_id: str
    condition_id: str
    station_id: str
    location_name: str
    latitude: float
    longitude: float
    timezone: str
    observation_window_local: str
    authoritative_source: str
    fallback_sources: list[str]
    rounding_rule: str
    inclusive_bounds: bool
    spec_version: str

    def __post_init__(self) -> None:
        _require_station_first_fields(
            station_id=self.station_id,
            latitude=self.latitude,
            longitude=self.longitude,
            timezone=self.timezone,
        )
        if not self.market_id or not self.condition_id:
            raise ValueError("market_id and condition_id are required")
        if not self.authoritative_source:
            raise ValueError("authoritative_source is required")
        if not self.spec_version:
            raise ValueError("spec_version is required")


@dataclass(frozen=True)
class ResolutionSpec:
    market_id: str
    condition_id: str
    location_name: str
    station_id: str
    latitude: float
    longitude: float
    timezone: str
    observation_date: date
    observation_window_local: str
    metric: str
    unit: str
    authoritative_source: str
    fallback_sources: list[str]
    rounding_rule: str
    inclusive_bounds: bool
    spec_version: str

    def __post_init__(self) -> None:
        _require_station_first_fields(
            station_id=self.station_id,
            latitude=self.latitude,
            longitude=self.longitude,
            timezone=self.timezone,
        )
        if not self.market_id or not self.condition_id:
            raise ValueError("market_id and condition_id are required")
        if not self.metric or not self.unit:
            raise ValueError("metric and unit are required")
        if not self.authoritative_source:
            raise ValueError("authoritative_source is required")
        if not self.spec_version:
            raise ValueError("spec_version is required")


@dataclass(frozen=True)
class ForecastRequest:
    market_id: str
    condition_id: str
    station_id: str
    source: str
    model_run: str
    forecast_target_time: datetime
    observation_date: date
    metric: str
    latitude: float
    longitude: float
    timezone: str
    spec_version: str

    def __post_init__(self) -> None:
        _require_station_first_fields(
            station_id=self.station_id,
            latitude=self.latitude,
            longitude=self.longitude,
            timezone=self.timezone,
        )
        if not self.market_id or not self.condition_id:
            raise ValueError("market_id and condition_id are required")
        if not self.source or not self.model_run:
            raise ValueError("source and model_run are required")
        if not self.metric or not self.spec_version:
            raise ValueError("metric and spec_version are required")


@dataclass(frozen=True)
class ForecastReplayRequest:
    market_id: str
    condition_id: str
    station_id: str
    source: str
    model_run: str
    forecast_target_time: datetime
    spec_version: str
    replay_reason: str
    replay_key: str = field(init=False)

    def __post_init__(self) -> None:
        if not self.market_id or not self.condition_id or not self.station_id:
            raise ValueError("market_id, condition_id, and station_id are required")
        if not self.source or not self.model_run or not self.spec_version:
            raise ValueError("source, model_run, and spec_version are required")
        if not self.replay_reason:
            raise ValueError("replay_reason is required")
        object.__setattr__(
            self,
            "replay_key",
            build_forecast_cache_key(
                market_id=self.market_id,
                station_id=self.station_id,
                spec_version=self.spec_version,
                source=self.source,
                model_run=self.model_run,
                forecast_target_time=self.forecast_target_time,
            ),
        )


@dataclass(frozen=True)
class ForecastReplayResult:
    replay_id: str
    request: ForecastReplayRequest
    forecast_run: ForecastRunRecord
    fair_values: list[WeatherFairValueRecord]
    watch_only_snapshots: list[WatchOnlySnapshotRecord]

    def __post_init__(self) -> None:
        if not self.replay_id:
            raise ValueError("replay_id is required")
        if self.forecast_run.market_id != self.request.market_id:
            raise ValueError("forecast_run.market_id must match replay request")
        if self.forecast_run.station_id != self.request.station_id:
            raise ValueError("forecast_run.station_id must match replay request")


@dataclass(frozen=True)
class ForecastReplayRecord:
    replay_id: str
    market_id: str
    condition_id: str
    station_id: str
    source: str
    model_run: str
    forecast_target_time: datetime
    spec_version: str
    replay_key: str
    replay_reason: str
    original_run_id: str
    replayed_run_id: str
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.replay_id or not self.market_id or not self.condition_id:
            raise ValueError("replay_id, market_id, and condition_id are required")
        if not self.station_id or not self.source or not self.model_run:
            raise ValueError("station_id, source, and model_run are required")
        if not self.spec_version or not self.replay_key or not self.replay_reason:
            raise ValueError("spec_version, replay_key, and replay_reason are required")
        if not self.original_run_id or not self.replayed_run_id:
            raise ValueError("original_run_id and replayed_run_id are required")


@dataclass(frozen=True)
class ForecastReplayDiffRecord:
    diff_id: str
    replay_id: str
    entity_type: str
    entity_key: str
    original_entity_id: str | None
    replayed_entity_id: str | None
    status: str
    diff_summary_json: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.diff_id or not self.replay_id:
            raise ValueError("diff_id and replay_id are required")
        if self.entity_type not in {"forecast_run", "fair_value", "watch_only_snapshot"}:
            raise ValueError("entity_type must be one of forecast_run/fair_value/watch_only_snapshot")
        if not self.entity_key:
            raise ValueError("entity_key is required")
        if self.status not in {"MATCH", "DIFFERENT", "MISSING_ORIGINAL", "MISSING_REPLAY"}:
            raise ValueError("unsupported diff status")
        if not isinstance(self.diff_summary_json, dict):
            raise ValueError("diff_summary_json must be a dictionary")


@dataclass(frozen=True)
class WatcherContinuityCheck:
    check_id: str
    chain_id: int
    from_block: int
    to_block: int
    last_known_finalized_block: int
    status: str
    gap_count: int
    details_json: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.check_id:
            raise ValueError("check_id is required")
        if self.chain_id < 0:
            raise ValueError("chain_id must be non-negative")
        if self.from_block < 0 or self.to_block < 0 or self.last_known_finalized_block < 0:
            raise ValueError("block numbers must be non-negative")
        if self.status not in {"OK", "GAP_DETECTED", "INVALID_RANGE", "RPC_INCOMPLETE"}:
            raise ValueError("unsupported continuity status")
        if self.gap_count < 0:
            raise ValueError("gap_count must be non-negative")
        if not isinstance(self.details_json, dict):
            raise ValueError("details_json must be a dictionary")


@dataclass(frozen=True)
class WatcherContinuityGap:
    gap_id: str
    check_id: str
    gap_type: str
    severity: str
    block_start: int
    block_end: int
    entity_ref: str | None
    details_json: dict[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.gap_id or not self.check_id:
            raise ValueError("gap_id and check_id are required")
        if self.gap_type not in {"BLOCK_GAP", "EVENT_GAP", "WATERMARK_REGRESSION", "DUPLICATE_RANGE", "RPC_INCOMPLETE"}:
            raise ValueError("unsupported gap_type")
        if self.severity not in {"INFO", "WARN", "ERROR"}:
            raise ValueError("unsupported severity")
        if self.block_start < 0 or self.block_end < 0:
            raise ValueError("block_start and block_end must be non-negative")
        if not isinstance(self.details_json, dict):
            raise ValueError("details_json must be a dictionary")


@dataclass(frozen=True)
class StationMetadata:
    station_id: str
    location_name: str
    latitude: float
    longitude: float
    timezone: str
    source: str

    def __post_init__(self) -> None:
        _require_station_first_fields(
            station_id=self.station_id,
            latitude=self.latitude,
            longitude=self.longitude,
            timezone=self.timezone,
        )
        if not self.source:
            raise ValueError("source is required")


class ProposalStatus(str, Enum):
    PENDING = "pending"
    PROPOSED = "proposed"
    DISPUTED = "disputed"
    SETTLED = "settled"
    REDEEMED = "redeemed"


class RedeemDecision(str, Enum):
    WAIT = "wait"
    READY_FOR_REDEEM = "ready_for_redeem"
    BLOCKED_PENDING_REVIEW = "blocked_pending_review"
    NOT_REDEEMABLE = "not_redeemable"


@dataclass(frozen=True)
class RedeemScheduleInput:
    proposal_status: ProposalStatus
    on_chain_settled_at: datetime | None
    safe_redeem_after: datetime | None
    human_review_required: bool


@dataclass(frozen=True)
class RedeemScheduleOutput:
    decision: RedeemDecision
    reason: str


@dataclass(frozen=True)
class SettlementVerificationRecord:
    verification_id: str
    proposal_id: str
    market_id: str
    proposed_outcome: str
    expected_outcome: str
    is_correct: bool
    confidence: float
    discrepancy_details: str | None
    sources_checked: list[str]
    evidence_package_id: str
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.verification_id or not self.proposal_id or not self.market_id:
            raise ValueError("verification_id, proposal_id, and market_id are required")
        if not self.proposed_outcome or not self.expected_outcome:
            raise ValueError("proposed_outcome and expected_outcome are required")
        if not self.evidence_package_id:
            raise ValueError("evidence_package_id is required")
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True)
class EvidencePackageLinkRecord:
    proposal_id: str
    verification_id: str
    evidence_package_id: str
    linked_at: datetime

    def __post_init__(self) -> None:
        if not self.proposal_id or not self.verification_id or not self.evidence_package_id:
            raise ValueError("proposal_id, verification_id, and evidence_package_id are required")


@dataclass(frozen=True)
class RedeemReadinessRecord:
    suggestion_id: str
    proposal_id: str
    decision: RedeemDecision
    reason: str
    on_chain_settled_at: datetime | None
    safe_redeem_after: datetime | None
    human_review_required: bool
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.suggestion_id or not self.proposal_id:
            raise ValueError("suggestion_id and proposal_id are required")
        if not self.reason:
            raise ValueError("reason is required")


@dataclass(frozen=True)
class UMAProposal:
    proposal_id: str
    market_id: str
    condition_id: str
    proposer: str
    proposed_outcome: str
    proposal_bond: float
    dispute_bond: float | None
    proposal_tx_hash: str
    proposal_block_number: int
    proposal_timestamp: datetime
    status: ProposalStatus
    on_chain_settled_at: datetime | None
    safe_redeem_after: datetime | None
    human_review_required: bool

    def __post_init__(self) -> None:
        if not self.proposal_id or not self.market_id or not self.condition_id:
            raise ValueError("proposal_id, market_id, and condition_id are required")
        if not self.proposer:
            raise ValueError("proposer is required")
        if not self.proposed_outcome:
            raise ValueError("proposed_outcome is required")
        if self.proposal_bond < 0:
            raise ValueError("proposal_bond must be non-negative")
        if self.dispute_bond is not None and self.dispute_bond < 0:
            raise ValueError("dispute_bond must be non-negative")
        if not self.proposal_tx_hash:
            raise ValueError("proposal_tx_hash is required")
        if self.proposal_block_number < 0:
            raise ValueError("proposal_block_number must be non-negative")


@dataclass(frozen=True)
class StateTransition:
    proposal_id: str
    old_status: ProposalStatus
    new_status: ProposalStatus
    block_number: int
    tx_hash: str
    event_type: str
    recorded_at: datetime

    def __post_init__(self) -> None:
        if not self.proposal_id:
            raise ValueError("proposal_id is required")
        if self.block_number < 0:
            raise ValueError("block_number must be non-negative")
        if not self.tx_hash or not self.event_type:
            raise ValueError("tx_hash and event_type are required")


def _require_station_first_fields(*, station_id: str, latitude: float, longitude: float, timezone: str) -> None:
    if not station_id:
        raise ValueError("station_id is required")
    if not timezone:
        raise ValueError("timezone is required")
    if latitude != latitude or longitude != longitude:
        raise ValueError("latitude and longitude must be valid numbers")
