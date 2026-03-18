from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
from typing import Any

from web3 import Web3

from agents.common import build_agent_client_from_env, enqueue_agent_artifact_upserts
from agents.weather import (
    load_data_qa_agent_requests,
    load_resolution_agent_requests,
    load_rule2spec_agent_requests,
    run_data_qa_agent_review,
    run_resolution_agent_review,
    run_rule2spec_agent_review,
)
from asterion_core.blockchain import (
    ChainTxKind,
    ChainTxMode,
    ChainTxServiceShell,
    build_approve_usdc_request,
    build_chain_tx_attempt_record,
    controlled_live_wallet_secret_env_var,
    load_controlled_live_smoke_policy,
    build_transaction_signer_request,
    build_wallet_state_observations,
    enqueue_chain_tx_attempt_upserts,
    load_observable_account_capabilities,
    load_latest_wallet_state_gate,
    load_polygon_chain_registry,
)
from asterion_core.contracts import (
    BalanceType,
    ForecastReplayRequest,
    ExternalBalanceObservation,
    ExternalBalanceObservationKind,
    ExternalFillObservation,
    Order,
    OrderSide,
    OrderStatus,
    ProposalStatus,
    ResolutionSpec,
    RouteAction,
    SubmitterBoundaryInputs,
    TimeInForce,
    UMAProposal,
    stable_object_id,
    new_request_id,
)
from asterion_core.execution import (
    DisabledSubmitterBackend,
    SubmitMode,
    SubmitterServiceShell,
    ShadowFillMode,
    SafeDefaultChainAccountCapabilityReader,
    apply_fills_to_order,
    bind_trade_ticket_handoff,
    build_execution_context,
    build_execution_context_record,
    build_external_fill_observations,
    build_external_order_observation,
    build_order_from_intent,
    build_paper_order,
    build_signal_order_intent_from_handoff,
    build_submit_attempt_from_signed_payload,
    build_submit_order_request_from_sign_attempt,
    build_trade_ticket,
    canonical_order_router_hash,
    canonical_order_router_payload,
    canonical_order_handoff_payload,
    enqueue_account_capability_upserts,
    enqueue_execution_context_upserts,
    enqueue_external_fill_observation_upserts,
    enqueue_external_order_observation_upserts,
    enqueue_market_capability_upserts,
    evaluate_execution_gate,
    external_order_observation_to_row,
    fill_journal_payload,
    gate_rejection_journal_payload,
    load_account_trading_capability,
    load_execution_context_record,
    load_market_capability,
    load_trade_ticket,
    refresh_account_capabilities,
    refresh_market_capabilities,
    order_status_journal_payload,
    paper_order_journal_payload_with_status,
    route_trade_ticket,
    route_trade_ticket_from_handoff,
    simulate_quote_based_fill,
    hydrate_execution_context,
    transition_order_to_posted,
)
from asterion_core.journal import (
    build_journal_event,
    enqueue_external_balance_observation_upserts,
    enqueue_exposure_snapshot_upserts,
    enqueue_fill_upserts,
    enqueue_gate_decision_upserts,
    enqueue_inventory_position_upserts,
    enqueue_journal_event_upserts,
    enqueue_order_state_transition_upserts,
    enqueue_order_upserts,
    enqueue_reconciliation_result_upserts,
    enqueue_reservation_upserts,
    enqueue_strategy_run_upserts,
    enqueue_trade_ticket_upserts,
)
from asterion_core.monitoring import (
    ReadinessReport,
    ReadinessConfig,
    build_live_side_effect_guard,
    build_readiness_evidence_bundle,
    load_controlled_live_capability_manifest,
    evaluate_p4_live_prereq_readiness,
    write_readiness_evidence_bundle,
    write_readiness_report,
)
from asterion_core.risk import (
    available_inventory_quantity_for_ticket,
    apply_fill_to_inventory,
    apply_fill_to_reservation,
    apply_reservation_to_inventory,
    build_external_execution_reconciliation_result,
    build_exposure_snapshot,
    build_reservation,
    build_reconciliation_result,
    classify_external_execution_reconciliation_status,
    finalize_reservation,
    classify_reconciliation_status,
    load_inventory_positions,
    load_reservation_for_order,
    reconciliation_journal_payload,
    release_reservation_to_inventory,
)
from asterion_core.signer import (
    SubmitAttemptRecord,
    SignatureAuditStatus,
    build_sign_order_request_from_routed_order,
    load_sign_only_attempts,
    SignerRequest,
    SignerServiceShell,
    SigningPurpose,
    build_submit_attempt_record,
    build_signing_context_from_account_capability,
    enqueue_submit_attempt_upserts,
)
from asterion_core.runtime import (
    StrategyContext,
    StrategyRegistration,
    load_selected_watch_only_snapshots,
    run_strategy_engine,
)
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.ui import build_ui_lite_db_once, refresh_ui_db_replica_once
from domains.weather.forecast import (
    AdapterRouter,
    ForecastService,
    build_forecast_calibration_sample,
    enqueue_forecast_calibration_profile_v2_upserts,
    build_forecast_replay_diff_records,
    build_forecast_replay_record,
    build_forecast_run_record,
    enqueue_forecast_calibration_sample_upserts,
    enqueue_forecast_replay_diff_upserts,
    enqueue_forecast_replay_upserts,
    enqueue_forecast_run_upserts,
    load_original_pricing_outputs,
    materialize_forecast_calibration_profiles_v2,
    load_replay_inputs,
    run_forecast_replay,
)
from domains.weather.forecast.service import ForecastCache
from domains.weather.opportunity import (
    build_execution_feedback_materialization_status,
    build_feedback_materialization_id,
    enqueue_execution_feedback_materialization_upserts,
    enqueue_execution_prior_upserts,
    materialize_execution_priors,
)
from domains.weather.pricing import (
    enqueue_fair_value_upserts,
    enqueue_watch_only_snapshot_upserts,
    load_forecast_run,
    load_weather_market_spec,
)
from domains.weather.scout import run_weather_market_discovery
from domains.weather.resolution import (
    RedeemScheduler,
    build_evidence_package_link,
    build_redeem_readiness_record,
    build_settlement_verification,
    persist_watcher_backfill,
    run_watcher_backfill,
)
from domains.weather.resolution.persistence import (
    enqueue_evidence_link_upserts,
    enqueue_redeem_readiness_upserts,
    enqueue_settlement_verification_upserts,
)
from domains.weather.spec import (
    StationMapper,
    build_weather_market_spec_record_via_station_mapper,
    enqueue_weather_market_spec_upserts,
    load_weather_markets_for_rule2spec,
    parse_rule2spec_draft,
)


@dataclass(frozen=True)
class ColdPathHandlerResult:
    job_name: str
    run_id: str
    task_ids: list[str]
    item_count: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SettlementVerificationInput:
    proposal_id: str
    expected_outcome: str
    confidence: float
    sources_checked: list[str]
    evidence_payload: dict[str, Any]
    discrepancy_details: str | None = None


@dataclass(frozen=True)
class PaperExecutionBatchRequest:
    wallet_id: str
    strategy_registrations: list[StrategyRegistration]
    snapshot_ids: list[str] | None
    market_ids: list[str] | None
    snapshot_limit: int | None
    dq_level: str
    data_snapshot_id: str | None
    universe_snapshot_id: str | None
    asof_ts_ms: int | None
    quote_snapshot_refs: list[str]

    def __post_init__(self) -> None:
        if not self.wallet_id:
            raise ValueError("wallet_id is required")
        if not self.strategy_registrations:
            raise ValueError("strategy_registrations are required")
        has_snapshot_selector = bool(self.snapshot_ids)
        has_market_selector = bool(self.market_ids)
        if has_snapshot_selector == has_market_selector:
            raise ValueError("exactly one of snapshot_ids or market_ids must be provided")
        if has_market_selector and self.snapshot_limit is None:
            raise ValueError("snapshot_limit is required when selecting by market_ids")
        if self.snapshot_limit is not None and int(self.snapshot_limit) <= 0:
            raise ValueError("snapshot_limit must be positive")
        if self.universe_snapshot_id == "":
            raise ValueError("universe_snapshot_id must be None or non-empty")
        if self.asof_ts_ms is not None and int(self.asof_ts_ms) < 0:
            raise ValueError("asof_ts_ms must be non-negative")


@dataclass(frozen=True)
class SignerAuditSmokeRequest:
    wallet_id: str
    requester: str
    signing_purpose: SigningPurpose
    payload_json: dict[str, Any]
    token_id: str | None
    fee_rate_bps: int | None

    def __post_init__(self) -> None:
        if not self.wallet_id or not self.requester:
            raise ValueError("wallet_id and requester are required")
        if not isinstance(self.payload_json, dict) or not self.payload_json:
            raise ValueError("payload_json must be a non-empty object")
        if self.signing_purpose is SigningPurpose.ORDER:
            if not self.token_id:
                raise ValueError("token_id is required for order signing")
            if self.fee_rate_bps is None or self.fee_rate_bps < 0:
                raise ValueError("fee_rate_bps is required for order signing")


@dataclass(frozen=True)
class OrderSigningSmokeRequest:
    ticket_ids: list[str]
    requester: str

    def __post_init__(self) -> None:
        if not self.ticket_ids:
            raise ValueError("ticket_ids are required")
        if not self.requester:
            raise ValueError("requester is required")


@dataclass(frozen=True)
class SubmitterSmokeRequest:
    attempt_ids: list[str]
    requester: str
    submit_mode: SubmitMode
    shadow_fill_mode: ShadowFillMode = ShadowFillMode.NONE
    approval_token: str | None = None

    def __post_init__(self) -> None:
        if not self.attempt_ids:
            raise ValueError("attempt_ids are required")
        if not self.requester:
            raise ValueError("requester is required")
        if self.submit_mode is SubmitMode.DRY_RUN and self.shadow_fill_mode is not ShadowFillMode.NONE:
            raise ValueError("shadow_fill_mode requires submit_mode=shadow_submit")
        if self.submit_mode is SubmitMode.LIVE_SUBMIT and self.shadow_fill_mode is not ShadowFillMode.NONE:
            raise ValueError("shadow_fill_mode is not supported for submit_mode=live_submit")
        if self.submit_mode is SubmitMode.LIVE_SUBMIT and not str(self.approval_token or "").strip():
            raise ValueError("approval_token is required for submit_mode=live_submit")


@dataclass(frozen=True)
class ChainTxSmokeRequest:
    wallet_id: str
    requester: str
    tx_kind: ChainTxKind
    tx_mode: ChainTxMode
    spender: str
    amount: Decimal

    def __post_init__(self) -> None:
        if not self.wallet_id or not self.requester:
            raise ValueError("wallet_id and requester are required")
        if self.tx_kind is not ChainTxKind.APPROVE_USDC:
            raise ValueError("weather_chain_tx_smoke only supports approve_usdc in P4-07")
        if not self.spender:
            raise ValueError("spender is required")
        if self.amount <= 0:
            raise ValueError("amount must be positive")


@dataclass(frozen=True)
class ControlledLiveSmokeRequest:
    wallet_id: str
    requester: str
    approval_id: str
    approval_reason: str
    approval_token: str
    tx_kind: ChainTxKind
    spender: str
    amount: Decimal

    def __post_init__(self) -> None:
        if not self.wallet_id or not self.requester:
            raise ValueError("wallet_id and requester are required")
        if not self.approval_id or not self.approval_reason or not self.approval_token:
            raise ValueError("approval_id, approval_reason, and approval_token are required")
        if self.tx_kind is not ChainTxKind.APPROVE_USDC:
            raise ValueError("weather_controlled_live_smoke only supports approve_usdc in P4-11")
        if not self.spender:
            raise ValueError("spender is required")
        if self.amount <= 0:
            raise ValueError("amount must be positive")


def run_weather_market_discovery_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    client,
    base_url: str,
    markets_endpoint: str,
    page_limit: int,
    max_pages: int,
    sleep_s: float,
    active_only: bool,
    closed: bool | None,
    archived: bool | None,
    tag_slug: str | None = "weather",
    recent_within_days: int | None = 14,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    del con
    request_id = run_id or new_request_id()
    result = run_weather_market_discovery(
        base_url=base_url,
        markets_endpoint=markets_endpoint,
        page_limit=int(page_limit),
        max_pages=int(max_pages),
        sleep_s=float(sleep_s),
        active_only=bool(active_only),
        closed=closed,
        archived=archived,
        tag_slug=tag_slug,
        recent_within_days=recent_within_days,
        client=client,
        queue_cfg=queue_cfg,
        run_id=request_id,
    )
    return ColdPathHandlerResult(
        job_name="weather_market_discovery",
        run_id=request_id,
        task_ids=_append_task_id(result.task_id),
        item_count=result.discovered_count,
        metadata={
            "discovered_count": result.discovered_count,
            "market_ids": [market.market_id for market in result.discovered_markets],
        },
    )


def run_weather_capability_refresh_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    clob_client,
    wallet_registry_path: str,
    chain_reader=None,
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)
    market_capabilities = refresh_market_capabilities(
        con,
        clob_client=clob_client,
        observed_at=normalized_observed_at,
    )
    account_capabilities = refresh_account_capabilities(
        con,
        wallet_registry_path=wallet_registry_path,
        chain_reader=chain_reader or SafeDefaultChainAccountCapabilityReader(),
    )
    task_ids: list[str] = []
    task_ids.extend(
        _append_task_id(
            enqueue_market_capability_upserts(
                queue_cfg,
                capabilities=market_capabilities,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_account_capability_upserts(
                queue_cfg,
                capabilities=account_capabilities,
                observed_at=normalized_observed_at,
                run_id=request_id,
            )
        )
    )
    market_ids = sorted({item.market_id for item in market_capabilities})
    token_ids = sorted({item.token_id for item in market_capabilities})
    wallet_ids = sorted({item.wallet_id for item in account_capabilities})
    return ColdPathHandlerResult(
        job_name="weather_capability_refresh",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(market_capabilities) + len(account_capabilities),
        metadata={
            "market_capability_count": len(market_capabilities),
            "account_capability_count": len(account_capabilities),
            "market_ids": market_ids,
            "wallet_ids": wallet_ids,
            "token_ids": token_ids,
        },
    )


def run_weather_wallet_state_refresh_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    chain_registry_path: str,
    wallet_state_reader,
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)
    try:
        account_capabilities = load_observable_account_capabilities(con)
        chain_registry = load_polygon_chain_registry(chain_registry_path)
        observations: list[ExternalBalanceObservation] = []
        journal_events = []
        wallet_ids: list[str] = []
        funder_addresses: list[str] = []
        allowance_target_count = 0
        for account_capability in account_capabilities:
            wallet_observations = build_wallet_state_observations(
                account_capability=account_capability,
                chain_registry=chain_registry,
                reader=wallet_state_reader,
                observed_at=normalized_observed_at,
            )
            observations.extend(wallet_observations)
            wallet_ids.append(account_capability.wallet_id)
            funder_addresses.append(account_capability.funder)
            allowance_count = sum(1 for item in wallet_observations if item.allowance_target is not None)
            allowance_target_count += allowance_count
            journal_events.append(
                build_journal_event(
                    event_type="wallet_state.observed",
                    entity_type="wallet",
                    entity_id=account_capability.wallet_id,
                    run_id=request_id,
                    payload_json={
                        "wallet_id": account_capability.wallet_id,
                        "funder": account_capability.funder,
                        "chain_id": chain_registry.chain_id,
                        "observation_count": len(wallet_observations),
                        "allowance_target_count": allowance_count,
                    },
                    created_at=normalized_observed_at,
                )
            )
    except Exception as exc:  # noqa: BLE001
        enqueue_journal_event_upserts(
            queue_cfg,
            journal_events=[
                build_journal_event(
                    event_type="wallet_state.refresh_failed",
                    entity_type="wallet_state_refresh",
                    entity_id=request_id,
                    run_id=request_id,
                    payload_json={"error": str(exc)},
                    created_at=normalized_observed_at,
                )
            ],
            run_id=request_id,
        )
        raise

    task_ids: list[str] = []
    task_ids.extend(
        _append_task_id(
            enqueue_external_balance_observation_upserts(
                queue_cfg,
                observations=observations,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_journal_event_upserts(
                queue_cfg,
                journal_events=journal_events,
                run_id=request_id,
            )
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_wallet_state_refresh",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(observations),
        metadata={
            "wallet_count": len(account_capabilities),
            "observation_count": len(observations),
            "wallet_ids": sorted(wallet_ids),
            "funder_addresses": sorted(funder_addresses),
            "allowance_target_count": allowance_target_count,
        },
    )


def run_weather_signer_audit_smoke_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    signer_service: SignerServiceShell,
    chain_id: int,
    params_json: dict[str, Any],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_request = _normalize_signer_audit_smoke_request(params_json)
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)
    account_capability = load_account_trading_capability(con, wallet_id=normalized_request.wallet_id)
    signing_context = build_signing_context_from_account_capability(
        account_capability,
        signing_purpose=normalized_request.signing_purpose,
        chain_id=chain_id,
        token_id=normalized_request.token_id,
        fee_rate_bps=normalized_request.fee_rate_bps,
    )
    signer_request = SignerRequest(
        request_id=request_id,
        requester=normalized_request.requester,
        timestamp=normalized_observed_at,
        context=signing_context,
        wallet_id=normalized_request.wallet_id,
        payload=normalized_request.payload_json,
    )
    if normalized_request.signing_purpose is SigningPurpose.TRANSACTION:
        invocation = signer_service.sign_transaction(signer_request, queue_cfg=queue_cfg, run_id=request_id)
    else:
        invocation = signer_service.derive_api_credentials(signer_request, queue_cfg=queue_cfg, run_id=request_id)
    return ColdPathHandlerResult(
        job_name="weather_signer_audit_smoke",
        run_id=request_id,
        task_ids=list(invocation.task_ids),
        item_count=1,
        metadata={
            "request_id": signer_request.request_id,
            "wallet_id": normalized_request.wallet_id,
            "signing_purpose": normalized_request.signing_purpose.value,
            "payload_hash": invocation.payload_hash,
            "status": invocation.response.status,
        },
    )


def run_weather_order_signing_smoke_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    signer_service: SignerServiceShell,
    params_json: dict[str, Any],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_request = _normalize_order_signing_smoke_request(params_json)
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)

    routed_orders = []
    hydrated_contexts = {}
    ticket_ids = list(normalized_request.ticket_ids)
    wallet_ids: set[str] = set()
    request_ids: list[str] = []
    payload_hashes: list[str] = []
    attempt_ids: list[str] = []
    task_ids: list[str] = []
    signed_count = 0
    rejected_count = 0
    submit_attempts = []

    for ticket_id in ticket_ids:
        ticket = load_trade_ticket(con, ticket_id=ticket_id)
        if ticket.wallet_id is None or ticket.execution_context_id is None:
            raise ValueError("ticket.wallet_id and ticket.execution_context_id are required for order signing smoke")
        wallet_ids.add(ticket.wallet_id)
        if ticket.execution_context_id not in hydrated_contexts:
            record = load_execution_context_record(con, execution_context_id=ticket.execution_context_id)
            hydrated_contexts[ticket.execution_context_id] = hydrate_execution_context(con, record=record)
        routed_orders.append(route_trade_ticket_from_handoff(con, ticket_id=ticket_id))

    if len(wallet_ids) != 1:
        raise ValueError("weather_order_signing_smoke only supports a single wallet per invocation")
    wallet_id = next(iter(wallet_ids))

    for routed_order in routed_orders:
        request_item = build_sign_order_request_from_routed_order(
            routed_order,
            hydrated_contexts[routed_order.execution_context_id],
            requester=normalized_request.requester,
            request_id=stable_object_id("sigreq", {"run_id": request_id, "ticket_id": routed_order.ticket_id}),
            timestamp=normalized_observed_at,
        )
        invocation = signer_service.sign_order(request_item, queue_cfg=queue_cfg, run_id=request_id)
        submit_attempt = build_submit_attempt_record(
            request_item,
            invocation.response,
            wallet_id=routed_order.wallet_id,
        )
        submit_attempts.append(submit_attempt)
        task_ids.extend(invocation.task_ids)
        request_ids.append(request_item.request_id)
        payload_hashes.append(invocation.payload_hash)
        attempt_ids.append(submit_attempt.attempt_id)
        if invocation.response.status == "signed":
            signed_count += 1
        else:
            rejected_count += 1

    task_ids.extend(
        _append_task_id(
            enqueue_submit_attempt_upserts(
                queue_cfg,
                attempts=submit_attempts,
                run_id=request_id,
            )
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_order_signing_smoke",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(ticket_ids),
        metadata={
            "request_ids": request_ids,
            "ticket_ids": ticket_ids,
            "wallet_id": wallet_id,
            "attempt_count": len(submit_attempts),
            "signed_count": signed_count,
            "rejected_count": rejected_count,
            "payload_hashes": payload_hashes,
            "attempt_ids": attempt_ids,
        },
    )


def run_weather_submitter_smoke_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    submitter_service: SubmitterServiceShell,
    params_json: dict[str, Any],
    controlled_live_capability_manifest_path: str | None = None,
    readiness_report_json_path: str | None = None,
    ui_lite_db_path: str | None = None,
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_request = _normalize_submitter_smoke_request(params_json)
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)
    sign_attempts = load_sign_only_attempts(con, attempt_ids=list(normalized_request.attempt_ids))
    wallet_ids = {item.wallet_id for item in sign_attempts}
    if len(wallet_ids) != 1:
        raise ValueError("weather_submitter_smoke only supports a single wallet per invocation")
    wallet_id = next(iter(wallet_ids))

    request_ids: list[str] = []
    payload_hashes: list[str] = []
    observation_ids: list[str] = []
    task_ids: list[str] = []
    submit_attempts = []
    observations = []
    fill_observations: list[ExternalFillObservation] = []
    preview_count = 0
    accepted_count = 0
    rejected_count = 0
    live_guard = None
    boundary_inputs = None

    def _blocked(reason: str, *, metadata_extra: dict[str, Any] | None = None) -> ColdPathHandlerResult:
        metadata = {
            "request_ids": request_ids,
            "attempt_ids": list(normalized_request.attempt_ids),
            "wallet_id": wallet_id,
            "submit_mode": normalized_request.submit_mode.value,
            "shadow_fill_mode": normalized_request.shadow_fill_mode.value,
            "submit_count": len(submit_attempts),
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "preview_count": preview_count,
            "external_fill_count": len(fill_observations),
            "payload_hashes": payload_hashes,
            "observation_ids": observation_ids,
            "status": "blocked",
            "reason": reason,
        }
        if metadata_extra:
            metadata.update(metadata_extra)
        return ColdPathHandlerResult(
            job_name="weather_submitter_smoke",
            run_id=request_id,
            task_ids=task_ids,
            item_count=len(submit_attempts),
            metadata=metadata,
        )

    if normalized_request.submit_mode is SubmitMode.LIVE_SUBMIT:
        manifest = None
        if controlled_live_capability_manifest_path:
            manifest = load_controlled_live_capability_manifest(controlled_live_capability_manifest_path)
        readiness_report = None
        readiness_payload = None
        if readiness_report_json_path:
            readiness_report = _load_readiness_report_or_none(readiness_report_json_path)
            if readiness_report is not None:
                readiness_payload = readiness_report.to_dict()
        wallet_readiness_status = None
        if ui_lite_db_path and os.path.exists(ui_lite_db_path):
            wallet_readiness_status = _load_live_prereq_wallet_status(ui_lite_db_path, wallet_id=wallet_id)
        live_boundary = submitter_service.describe_live_boundary()
        is_armed = str(os.getenv("ASTERION_CONTROLLED_LIVE_SECRET_ARMED") or "").strip().lower() == "true"
        expected_token = str(os.getenv("ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN") or "").strip()
        approval_token_matches = bool(expected_token) and normalized_request.approval_token == expected_token
        live_guard = build_live_side_effect_guard(mode="live_submit", armed=is_armed)
        boundary_inputs = SubmitterBoundaryInputs(
            request_id="",
            wallet_id=wallet_id,
            source_attempt_id=None,
            ticket_id=None,
            execution_context_id=None,
            submit_mode=normalized_request.submit_mode.value,
            submitter_backend_kind=str(live_boundary.get("submitter_backend_kind") or ""),
            signer_backend_kind=str((manifest or {}).get("signer_backend_kind") or ""),
            chain_tx_backend_kind=str((manifest or {}).get("chain_tx_backend_kind") or ""),
            submitter_endpoint_fingerprint=live_boundary.get("submitter_endpoint_fingerprint"),
            manifest_payload=dict(manifest) if isinstance(manifest, dict) else None,
            manifest_path=controlled_live_capability_manifest_path,
            readiness_report_payload=readiness_payload,
            wallet_readiness_status=wallet_readiness_status,
            approval_token_matches=approval_token_matches,
            armed=is_armed,
            evaluated_at=normalized_observed_at,
        )

    for sign_attempt in sign_attempts:
        submit_request = build_submit_order_request_from_sign_attempt(
            sign_attempt,
            requester=normalized_request.requester,
            request_id=stable_object_id(
                "subreq",
                {"run_id": request_id, "source_attempt_id": sign_attempt.attempt_id},
            ),
            timestamp=normalized_observed_at,
            submit_mode=normalized_request.submit_mode,
            shadow_fill_mode=normalized_request.shadow_fill_mode,
        )
        invocation = submitter_service.submit_order(
            submit_request,
            live_guard=live_guard,
            boundary_inputs=(
                SubmitterBoundaryInputs(
                    request_id=submit_request.request_id,
                    wallet_id=wallet_id,
                    source_attempt_id=submit_request.source_attempt_id,
                    ticket_id=submit_request.ticket_id,
                    execution_context_id=submit_request.execution_context_id,
                    submit_mode=boundary_inputs.submit_mode,
                    submitter_backend_kind=boundary_inputs.submitter_backend_kind,
                    signer_backend_kind=boundary_inputs.signer_backend_kind,
                    chain_tx_backend_kind=boundary_inputs.chain_tx_backend_kind,
                    submitter_endpoint_fingerprint=boundary_inputs.submitter_endpoint_fingerprint,
                    manifest_payload=boundary_inputs.manifest_payload,
                    manifest_path=boundary_inputs.manifest_path,
                    readiness_report_payload=boundary_inputs.readiness_report_payload,
                    wallet_readiness_status=boundary_inputs.wallet_readiness_status,
                    approval_token_matches=boundary_inputs.approval_token_matches,
                    armed=boundary_inputs.armed,
                    evaluated_at=boundary_inputs.evaluated_at,
                )
                if boundary_inputs is not None
                else None
            ),
            queue_cfg=queue_cfg,
            run_id=request_id,
        )
        submit_attempt = build_submit_attempt_from_signed_payload(submit_request, invocation.response)
        observation = build_external_order_observation(
            submit_attempt,
            observed_at=invocation.response.completed_at,
        )
        fill_observations.extend(
            build_external_fill_observations(
                submit_attempt,
                observed_at=invocation.response.completed_at,
            )
        )
        submit_attempts.append(submit_attempt)
        observations.append(observation)
        task_ids.extend(invocation.task_ids)
        request_ids.append(submit_request.request_id)
        payload_hashes.append(invocation.payload_hash)
        observation_ids.append(observation.observation_id)
        if invocation.response.status == "previewed":
            preview_count += 1
        elif invocation.response.status == "accepted":
            accepted_count += 1
        else:
            rejected_count += 1

    task_ids.extend(
        _append_task_id(
            enqueue_submit_attempt_upserts(
                queue_cfg,
                attempts=submit_attempts,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_external_order_observation_upserts(
                queue_cfg,
                observations=observations,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_external_fill_observation_upserts(
                queue_cfg,
                observations=fill_observations,
                run_id=request_id,
            )
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_submitter_smoke",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(submit_attempts),
        metadata={
            "request_ids": request_ids,
            "attempt_ids": list(normalized_request.attempt_ids),
            "wallet_id": wallet_id,
            "submit_mode": normalized_request.submit_mode.value,
            "shadow_fill_mode": normalized_request.shadow_fill_mode.value,
            "approval_token_provided": normalized_request.submit_mode is SubmitMode.LIVE_SUBMIT,
            "submit_count": len(submit_attempts),
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "preview_count": preview_count,
            "external_fill_count": len(fill_observations),
            "payload_hashes": payload_hashes,
            "observation_ids": observation_ids,
        },
    )


def run_weather_chain_tx_smoke_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    signer_service: SignerServiceShell,
    chain_tx_service: ChainTxServiceShell,
    chain_registry_path: str,
    chain_tx_reader,
    params_json: dict[str, Any],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_request = _normalize_chain_tx_smoke_request(params_json)
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)
    account_capability = load_account_trading_capability(con, wallet_id=normalized_request.wallet_id)
    chain_registry = load_polygon_chain_registry(chain_registry_path)
    load_latest_wallet_state_gate(
        con,
        wallet_id=normalized_request.wallet_id,
        chain_registry=chain_registry,
        spender=normalized_request.spender,
    )

    chain_tx_request = build_approve_usdc_request(
        account_capability=account_capability,
        chain_registry=chain_registry,
        chain_tx_reader=chain_tx_reader,
        requester=normalized_request.requester,
        request_id=request_id,
        timestamp=normalized_observed_at,
        tx_mode=normalized_request.tx_mode,
        spender=normalized_request.spender,
        amount=normalized_request.amount,
    )
    signer_request = build_transaction_signer_request(chain_tx_request, account_capability)
    signer_invocation = signer_service.sign_transaction(
        signer_request,
        queue_cfg=queue_cfg,
        run_id=request_id,
    )

    signed_payload_json = getattr(signer_invocation.response, "signed_payload_json", None) or {
        "backend_kind": "tx_stub" if signer_invocation.response.status == "succeeded" else "disabled",
        "request_id": signer_request.request_id,
        "wallet_id": normalized_request.wallet_id,
        "tx_kind": chain_tx_request.tx_kind.value,
        "tx_mode": chain_tx_request.tx_mode.value,
        "signature": signer_invocation.response.signature,
        "signed_payload_ref": signer_invocation.response.signed_payload_ref,
        "unsigned_tx": dict(signer_request.payload),
    }
    if signer_invocation.response.status != "succeeded":
        signed_payload_json = dict(signed_payload_json)
        signed_payload_json["error"] = signer_invocation.response.error

    chain_tx_invocation = chain_tx_service.submit_transaction(
        chain_tx_request,
        signed_payload_json=signed_payload_json,
        queue_cfg=queue_cfg,
        run_id=request_id,
    )
    attempt = build_chain_tx_attempt_record(
        chain_tx_request,
        chain_tx_invocation.response,
        signed_payload_ref=signer_invocation.response.signed_payload_ref,
    )
    task_ids = list(signer_invocation.task_ids)
    task_ids.extend(chain_tx_invocation.task_ids)
    task_ids.extend(
        _append_task_id(
            enqueue_chain_tx_attempt_upserts(
                queue_cfg,
                attempts=[attempt],
                run_id=request_id,
            )
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_chain_tx_smoke",
        run_id=request_id,
        task_ids=task_ids,
        item_count=1,
        metadata={
            "request_id": request_id,
            "wallet_id": normalized_request.wallet_id,
            "tx_kind": chain_tx_request.tx_kind.value,
            "tx_mode": chain_tx_request.tx_mode.value,
            "attempt_id": attempt.attempt_id,
            "status": attempt.status,
            "payload_hash": attempt.payload_hash,
            "tx_hash": attempt.tx_hash,
        },
    )


def run_weather_controlled_live_smoke_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    signer_service: SignerServiceShell,
    chain_tx_service: ChainTxServiceShell,
    chain_registry_path: str,
    controlled_live_smoke_policy_path: str,
    controlled_live_capability_manifest_path: str,
    readiness_report_json_path: str,
    ui_lite_db_path: str,
    chain_tx_reader,
    params_json: dict[str, Any],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_request = _normalize_controlled_live_smoke_request(params_json)
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)
    task_ids: list[str] = []
    live_guard = None

    requested_event = build_journal_event(
        event_type="controlled_live_smoke.requested",
        entity_type="controlled_live_smoke",
        entity_id=normalized_request.approval_id,
        run_id=request_id,
        payload_json={
            "approval_id": normalized_request.approval_id,
            "wallet_id": normalized_request.wallet_id,
            "tx_kind": normalized_request.tx_kind.value,
            "spender": normalized_request.spender,
            "amount": str(normalized_request.amount),
        },
        created_at=normalized_observed_at,
    )
    task_ids.extend(
        _append_task_id(
            enqueue_journal_event_upserts(queue_cfg, journal_events=[requested_event], run_id=request_id)
        )
    )

    def _blocked(reason: str, *, metadata_extra: dict[str, Any] | None = None) -> ColdPathHandlerResult:
        blocked_event = build_journal_event(
            event_type="controlled_live_smoke.blocked",
            entity_type="controlled_live_smoke",
            entity_id=normalized_request.approval_id,
            run_id=request_id,
            payload_json={
                "approval_id": normalized_request.approval_id,
                "wallet_id": normalized_request.wallet_id,
                "tx_kind": normalized_request.tx_kind.value,
                "reason": reason,
            },
            created_at=normalized_observed_at,
        )
        blocked_task_ids = list(task_ids)
        blocked_task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(queue_cfg, journal_events=[blocked_event], run_id=request_id)
            )
        )
        metadata = {
            "request_id": request_id,
            "approval_id": normalized_request.approval_id,
            "wallet_id": normalized_request.wallet_id,
            "tx_kind": normalized_request.tx_kind.value,
            "tx_mode": ChainTxMode.CONTROLLED_LIVE.value,
            "status": "blocked",
            "tx_hash": None,
            "payload_hash": None,
            "reason": reason,
            "capability_manifest_path": controlled_live_capability_manifest_path,
        }
        if metadata_extra:
            metadata.update(metadata_extra)
        return ColdPathHandlerResult(
            job_name="weather_controlled_live_smoke",
            run_id=request_id,
            task_ids=blocked_task_ids,
            item_count=0,
            metadata=metadata,
        )

    try:
        manifest = load_controlled_live_capability_manifest(controlled_live_capability_manifest_path)
    except Exception as exc:  # noqa: BLE001
        return _blocked(
            "invalid_controlled_live_capability_manifest",
            metadata_extra={"manifest_error": str(exc)},
        )
    if manifest is None:
        return _blocked("missing_controlled_live_capability_manifest")
    manifest_status = str(manifest.get("manifest_status") or "").strip()
    if manifest_status != "valid":
        return _blocked(
            "invalid_controlled_live_capability_manifest",
            metadata_extra={
                "manifest_status": manifest_status or "missing",
                "manifest_blockers": list(manifest.get("blockers") or []),
            },
        )
    if str(manifest.get("controlled_live_mode") or "") != "manual_only":
        return _blocked("controlled_live_mode_not_manual_only")
    if str(manifest.get("chain_tx_backend_kind") or "") != "real_broadcast":
        return _blocked("chain_tx_backend_not_real_broadcast")
    if str(manifest.get("signer_backend_kind") or "") != "env_private_key_tx":
        return _blocked("signer_backend_not_env_private_key_tx")

    policy = load_controlled_live_smoke_policy(controlled_live_smoke_policy_path)
    if policy.chain_id <= 0:
        return _blocked("invalid_controlled_live_policy")

    try:
        wallet_policy = policy.wallet_policy(normalized_request.wallet_id)
    except ValueError:
        return _blocked("wallet_not_allowlisted")
    allowed_wallet_ids = {str(item) for item in list(manifest.get("allowed_wallet_ids") or [])}
    if normalized_request.wallet_id not in allowed_wallet_ids:
        return _blocked("wallet_not_allowlisted")
    if normalized_request.tx_kind.value not in wallet_policy.allowed_tx_kinds:
        return _blocked("tx_kind_not_allowlisted")
    allowed_tx_kinds = {str(item) for item in list(manifest.get("allowed_tx_kinds") or [])}
    if normalized_request.tx_kind.value not in allowed_tx_kinds:
        return _blocked("tx_kind_not_allowlisted")
    normalized_spender = Web3.to_checksum_address(normalized_request.spender.strip())
    if normalized_spender not in wallet_policy.allowed_spenders:
        return _blocked("spender_not_allowlisted")
    allowed_spenders_by_wallet = {
        str(key): [Web3.to_checksum_address(str(value)) for value in list(values or [])]
        for key, values in dict(manifest.get("allowed_spenders_by_wallet") or {}).items()
    }
    if normalized_spender not in allowed_spenders_by_wallet.get(normalized_request.wallet_id, []):
        return _blocked("spender_not_allowlisted")
    if normalized_request.amount > wallet_policy.max_approve_amount:
        return _blocked("amount_cap_exceeded")
    manifest_amount_cap = Decimal(
        str((manifest.get("max_approve_amount_by_wallet") or {}).get(normalized_request.wallet_id) or "0")
    )
    if manifest_amount_cap <= 0 or normalized_request.amount > manifest_amount_cap:
        return _blocked("amount_cap_exceeded")

    private_key_env_var = controlled_live_wallet_secret_env_var(normalized_request.wallet_id)
    is_armed = str(os.getenv("ASTERION_CONTROLLED_LIVE_SECRET_ARMED") or "").strip().lower() == "true"
    if not is_armed:
        return _blocked("controlled_live_smoke_not_armed")
    live_guard = build_live_side_effect_guard(mode="controlled_live", armed=True)
    expected_token = str(os.getenv("ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN") or "").strip()
    if not expected_token or normalized_request.approval_token != expected_token:
        return _blocked("approval_token_mismatch")
    if not str(os.getenv(private_key_env_var) or "").strip():
        return _blocked(
            "controlled_live_wallet_secret_missing",
            metadata_extra={"wallet_secret_env_var": private_key_env_var},
        )

    readiness_report = _load_readiness_report_or_none(readiness_report_json_path)
    if readiness_report is None or readiness_report.target.value != "p4_live_prerequisites":
        return _blocked("missing_p4_readiness_report")
    if readiness_report.go_decision != "GO":
        return _blocked("p4_live_prereq_not_go")
    wallet_readiness_status = _load_live_prereq_wallet_status(ui_lite_db_path, wallet_id=normalized_request.wallet_id)
    if wallet_readiness_status != "ready":
        return _blocked("wallet_not_ready", metadata_extra={"wallet_readiness_status": wallet_readiness_status})

    account_capability = load_account_trading_capability(con, wallet_id=normalized_request.wallet_id)
    chain_registry = load_polygon_chain_registry(chain_registry_path)
    load_latest_wallet_state_gate(
        con,
        wallet_id=normalized_request.wallet_id,
        chain_registry=chain_registry,
        spender=normalized_spender,
    )
    chain_tx_request = build_approve_usdc_request(
        account_capability=account_capability,
        chain_registry=chain_registry,
        chain_tx_reader=chain_tx_reader,
        requester=normalized_request.requester,
        request_id=request_id,
        timestamp=normalized_observed_at,
        tx_mode=ChainTxMode.CONTROLLED_LIVE,
        spender=normalized_spender,
        amount=normalized_request.amount,
        approval_id=normalized_request.approval_id,
        approval_reason=normalized_request.approval_reason,
    )
    signer_request = build_transaction_signer_request(
        chain_tx_request,
        account_capability,
    )
    signer_request = SignerRequest(
        request_id=signer_request.request_id,
        requester=signer_request.requester,
        timestamp=signer_request.timestamp,
        context=signer_request.context,
        wallet_id=signer_request.wallet_id,
        payload={
            **signer_request.payload,
            "approval_id": normalized_request.approval_id,
            "approval_reason": normalized_request.approval_reason,
        },
    )
    signer_invocation = signer_service.sign_transaction(
        signer_request,
        queue_cfg=queue_cfg,
        run_id=request_id,
    )
    signed_payload_json = getattr(signer_invocation.response, "signed_payload_json", None) or {
        "backend_kind": "disabled",
        "request_id": signer_request.request_id,
        "signed_payload_ref": signer_invocation.response.signed_payload_ref,
        "unsigned_tx": dict(signer_request.payload),
    }
    if signer_invocation.response.status != SignatureAuditStatus.SUCCEEDED.value:
        signed_payload_json = dict(signed_payload_json)
        signed_payload_json["error"] = signer_invocation.response.error
    chain_tx_invocation = chain_tx_service.submit_transaction(
        chain_tx_request,
        signed_payload_json=signed_payload_json,
        live_guard=live_guard,
        queue_cfg=queue_cfg,
        run_id=request_id,
    )
    attempt = build_chain_tx_attempt_record(
        chain_tx_request,
        chain_tx_invocation.response,
        signed_payload_ref=signer_invocation.response.signed_payload_ref,
    )
    task_ids.extend(signer_invocation.task_ids)
    task_ids.extend(chain_tx_invocation.task_ids)
    task_ids.extend(
        _append_task_id(
            enqueue_chain_tx_attempt_upserts(
                queue_cfg,
                attempts=[attempt],
                run_id=request_id,
            )
        )
    )
    if attempt.status == "broadcasted":
        task_ids.extend(
            _append_task_id(
                enqueue_journal_event_upserts(
                    queue_cfg,
                    journal_events=[
                        build_journal_event(
                            event_type="controlled_live_smoke.broadcasted",
                            entity_type="controlled_live_smoke",
                            entity_id=normalized_request.approval_id,
                            run_id=request_id,
                            payload_json={
                                "approval_id": normalized_request.approval_id,
                                "wallet_id": normalized_request.wallet_id,
                                "tx_kind": normalized_request.tx_kind.value,
                                "attempt_id": attempt.attempt_id,
                                "tx_hash": attempt.tx_hash,
                            },
                            created_at=normalized_observed_at,
                        )
                    ],
                    run_id=request_id,
                )
            )
        )
    return ColdPathHandlerResult(
        job_name="weather_controlled_live_smoke",
        run_id=request_id,
        task_ids=task_ids,
        item_count=1,
        metadata={
            "request_id": request_id,
            "approval_id": normalized_request.approval_id,
            "wallet_id": normalized_request.wallet_id,
            "tx_kind": chain_tx_request.tx_kind.value,
            "tx_mode": chain_tx_request.tx_mode.value,
            "attempt_id": attempt.attempt_id,
            "status": attempt.status,
            "payload_hash": attempt.payload_hash,
            "tx_hash": attempt.tx_hash,
            "capability_manifest_path": controlled_live_capability_manifest_path,
            "manifest_status": manifest_status,
        },
    )


def run_weather_external_execution_reconciliation_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    normalized_observed_at = _normalize_datetime(observed_at) or datetime.now(UTC).replace(tzinfo=None)
    submit_attempts = _load_shadow_submit_attempts_for_external_reconciliation(con)

    reconciliation_results = []
    journal_events = []
    for attempt in submit_attempts:
        if not attempt.order_id:
            raise ValueError("external execution reconciliation requires submit attempts with order_id")
        order = _load_order_for_reconciliation(con, order_id=attempt.order_id)
        if order is None:
            raise ValueError(f"external execution reconciliation missing local order for {attempt.order_id}")
        external_order_observation = _load_latest_external_order_observation(con, attempt_id=attempt.attempt_id)
        external_fill_observations = _load_latest_external_fill_observations(con, attempt_id=attempt.attempt_id)
        wallet_observation_ref = _load_latest_wallet_observation_reference(con, wallet_id=attempt.wallet_id)
        external_fill_observation_id = (
            stable_object_id("efillagg", {"attempt_id": attempt.attempt_id})
            if external_fill_observations
            else None
        )
        reconciliation_result = build_external_execution_reconciliation_result(
            order=order,
            ticket_id=attempt.ticket_id,
            execution_context_id=attempt.execution_context_id,
            external_order_observation_id=external_order_observation.observation_id if external_order_observation else None,
            external_fill_observation_id=external_fill_observation_id,
            external_balance_observation_id=wallet_observation_ref.observation_id if wallet_observation_ref else None,
            external_order_status=external_order_observation.external_status if external_order_observation else None,
            external_fill_observations=external_fill_observations,
            wallet_observation_ref=wallet_observation_ref,
            created_at=normalized_observed_at,
        )
        reconciliation_results.append(reconciliation_result)
        journal_payload = reconciliation_journal_payload(
            result=reconciliation_result,
            order=order,
            ticket_id=attempt.ticket_id,
            request_id=attempt.request_id,
        )
        journal_events.append(
            build_journal_event(
                event_type="reconciliation.checked",
                entity_type="reconciliation",
                entity_id=reconciliation_result.reconciliation_id,
                run_id=request_id,
                payload_json=journal_payload,
                created_at=normalized_observed_at,
            )
        )
        if reconciliation_result.status.value != "ok":
            journal_events.append(
                build_journal_event(
                    event_type="reconciliation.mismatch",
                    entity_type="reconciliation",
                    entity_id=reconciliation_result.reconciliation_id,
                    run_id=request_id,
                    payload_json=journal_payload,
                    created_at=normalized_observed_at,
                )
            )

    task_ids: list[str] = []
    task_ids.extend(
        _append_task_id(
            enqueue_reconciliation_result_upserts(
                queue_cfg,
                results=reconciliation_results,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_journal_event_upserts(
                queue_cfg,
                journal_events=journal_events,
                run_id=request_id,
            )
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_external_execution_reconciliation",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(reconciliation_results),
        metadata={
            "attempt_count": len(submit_attempts),
            "reconciliation_count": len(reconciliation_results),
            "reconciliation_mismatch_count": sum(1 for item in reconciliation_results if item.status.value != "ok"),
            "wallet_ids": sorted({item.wallet_id for item in submit_attempts}),
        },
    )


def run_weather_live_prereq_readiness_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    ui_replica_db_path: str,
    ui_replica_meta_path: str,
    ui_lite_db_path: str,
    ui_lite_meta_path: str,
    readiness_report_json_path: str,
    readiness_report_markdown_path: str,
    readiness_evidence_json_path: str,
    controlled_live_smoke_policy_path: str,
    controlled_live_capability_manifest_path: str,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    db_path = _resolve_connection_db_path(con)
    report = evaluate_p4_live_prereq_readiness(
        ReadinessConfig(
            db_path=db_path,
            ui_replica_db_path=ui_replica_db_path,
            ui_replica_meta_path=ui_replica_meta_path,
            ui_lite_db_path=ui_lite_db_path,
            ui_lite_meta_path=ui_lite_meta_path,
            readiness_report_json_path=readiness_report_json_path,
            readiness_report_markdown_path=readiness_report_markdown_path,
            controlled_live_smoke_policy_path=controlled_live_smoke_policy_path,
            controlled_live_capability_manifest_path=controlled_live_capability_manifest_path,
            signer_backend_kind=os.getenv("ASTERION_SIGNER_BACKEND_KIND", "disabled"),
            submitter_backend_kind=os.getenv("ASTERION_SUBMITTER_BACKEND_KIND", "disabled"),
            chain_tx_backend_kind=os.getenv("ASTERION_CHAIN_TX_BACKEND_KIND", "disabled"),
            write_queue_path=queue_cfg.path,
        )
    )
    write_readiness_report(
        report,
        json_path=readiness_report_json_path,
        markdown_path=readiness_report_markdown_path,
    )
    evidence_bundle = build_readiness_evidence_bundle(
        report,
        readiness_report_json_path=readiness_report_json_path,
        readiness_report_markdown_path=readiness_report_markdown_path,
        capability_manifest_path=controlled_live_capability_manifest_path,
        ui_lite_db_path=ui_lite_db_path,
        ui_lite_meta_path=ui_lite_meta_path,
        ui_replica_db_path=ui_replica_db_path,
        ui_replica_meta_path=ui_replica_meta_path,
        weather_smoke_report_path=os.getenv("ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH", "data/dev/real_weather_chain/real_weather_chain_report.json"),
        weather_smoke_db_path=os.getenv("ASTERION_REAL_WEATHER_CHAIN_DB_PATH", "data/dev/real_weather_chain/real_weather_chain.duckdb"),
    )
    write_readiness_evidence_bundle(
        evidence_bundle,
        json_path=readiness_evidence_json_path,
    )
    replica_result = refresh_ui_db_replica_once(
        src_db_path=db_path,
        dst_db_path=ui_replica_db_path,
        meta_path=ui_replica_meta_path,
    )
    lite_result = build_ui_lite_db_once(
        src_db_path=ui_replica_db_path,
        dst_db_path=ui_lite_db_path,
        meta_path=ui_lite_meta_path,
        readiness_report_json_path=readiness_report_json_path,
        readiness_evidence_json_path=readiness_evidence_json_path,
    )
    failed_gate_names = [item.gate_name for item in report.gate_results if not item.passed]
    return ColdPathHandlerResult(
        job_name="weather_live_prereq_readiness",
        run_id=request_id,
        task_ids=[],
        item_count=len(report.gate_results),
        metadata={
            "target": report.target.value,
            "go_decision": report.go_decision,
            "decision_reason": report.decision_reason,
            "gate_count": len(report.gate_results),
            "failed_gate_names": failed_gate_names,
            "report_json_path": readiness_report_json_path,
            "report_markdown_path": readiness_report_markdown_path,
            "readiness_evidence_json_path": readiness_evidence_json_path,
            "capability_manifest_path": report.capability_manifest_path,
            "capability_manifest_status": report.capability_manifest_status,
            "evidence_blocker_count": len(evidence_bundle.blockers),
            "evidence_warning_count": len(evidence_bundle.warnings),
            "ui_replica_ok": replica_result.ok,
            "ui_lite_ok": lite_result.ok,
        },
    )


def run_weather_spec_sync(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    mapper: StationMapper | None = None,
    active_only: bool = True,
    limit: int | None = None,
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    active_mapper = mapper or StationMapper()
    request_id = run_id or new_request_id()
    markets = load_weather_markets_for_rule2spec(con, active_only=active_only, limit=limit)
    records = [
        build_weather_market_spec_record_via_station_mapper(
            parse_rule2spec_draft(market),
            mapper=active_mapper,
            con=con,
        )
        for market in markets
    ]
    task_ids = _append_task_id(
        enqueue_weather_market_spec_upserts(
            queue_cfg,
            specs=records,
            run_id=request_id,
            observed_at=observed_at,
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_spec_sync",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(records),
        metadata={"market_count": len(markets)},
    )


def run_weather_forecast_refresh(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    forecast_service: ForecastService,
    source: str,
    model_run: str,
    forecast_target_time: datetime,
    market_ids: list[str] | None = None,
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    specs = _load_weather_market_specs(con, market_ids=market_ids)
    records = []
    for spec_record in specs:
        distribution = forecast_service.get_forecast(
            _resolution_spec_from_record(spec_record),
            source=source,
            model_run=model_run,
            forecast_target_time=forecast_target_time,
        )
        records.append(build_forecast_run_record(distribution))
    task_ids = _append_task_id(
        enqueue_forecast_run_upserts(
            queue_cfg,
            forecast_runs=records,
            run_id=request_id,
            observed_at=observed_at,
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_forecast_refresh",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(records),
        metadata={"market_ids": [item.market_id for item in specs]},
    )


def run_weather_forecast_replay_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    adapter_router: AdapterRouter,
    cache: ForecastCache,
    replay_requests: list[ForecastReplayRequest | dict[str, Any]],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    if not replay_requests:
        raise ValueError("replay_requests are required for weather_forecast_replay")
    request_id = run_id or new_request_id()
    replay_records = []
    replay_diff_records = []
    forecast_runs = []
    fair_values = []
    snapshots = []

    for raw in replay_requests:
        replay_result = run_forecast_replay(
            con,
            adapter_router=adapter_router,
            cache=cache,
            market_id=_get_replay_value(raw, "market_id"),
            station_id=_get_replay_value(raw, "station_id"),
            source=_get_replay_value(raw, "source"),
            model_run=_get_replay_value(raw, "model_run"),
            forecast_target_time=_coerce_datetime(_get_replay_value(raw, "forecast_target_time")),
            spec_version=_get_replay_value(raw, "spec_version"),
            replay_reason=_get_replay_value(raw, "replay_reason"),
        )
        _spec_record, original_run, _ = load_replay_inputs(con, replay_result.request)
        original_fair_values, original_snapshots = load_original_pricing_outputs(con, run_id=original_run.run_id)
        replay_records.append(build_forecast_replay_record(replay_result, original_run_id=original_run.run_id))
        replay_diff_records.extend(
            build_forecast_replay_diff_records(
                replay_result=replay_result,
                original_run=original_run,
                original_fair_values=original_fair_values,
                original_watch_only_snapshots=original_snapshots,
            )
        )
        forecast_runs.append(replay_result.forecast_run)
        fair_values.extend(replay_result.fair_values)
        snapshots.extend(replay_result.watch_only_snapshots)
    task_ids: list[str] = []
    task_ids.extend(
        _append_task_id(
            enqueue_forecast_run_upserts(
                queue_cfg,
                forecast_runs=forecast_runs,
                run_id=request_id,
                observed_at=observed_at,
            )
        )
    )
    task_ids.extend(_append_task_id(enqueue_fair_value_upserts(queue_cfg, fair_values=fair_values, run_id=request_id, observed_at=observed_at)))
    task_ids.extend(
        _append_task_id(
            enqueue_watch_only_snapshot_upserts(
                queue_cfg,
                snapshots=snapshots,
                run_id=request_id,
                observed_at=observed_at,
            )
        )
    )
    task_ids.extend(_append_task_id(enqueue_forecast_replay_upserts(queue_cfg, replays=replay_records, run_id=request_id)))
    task_ids.extend(_append_task_id(enqueue_forecast_replay_diff_upserts(queue_cfg, diffs=replay_diff_records, run_id=request_id)))
    return ColdPathHandlerResult(
        job_name="weather_forecast_replay",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(replay_records),
        metadata={"replay_ids": [item.replay_id for item in replay_records]},
    )


def run_weather_watcher_backfill_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    rpc_pool,
    chain_id: int,
    replay_reason: str,
    max_block_span: int | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    result = run_watcher_backfill(
        con,
        rpc_pool,
        chain_id=chain_id,
        replay_reason=replay_reason,
        max_block_span=max_block_span,
        observed_at=observed_at,
    )
    task_ids = persist_watcher_backfill(queue_cfg, result, observed_at=observed_at)
    return ColdPathHandlerResult(
        job_name="weather_watcher_backfill",
        run_id=result.run_id,
        task_ids=task_ids,
        item_count=result.processed_events_written,
        metadata={
            "continuity_check_id": result.continuity_check_id,
            "finalized_block": result.finalized_block,
            "rpc_trace": result.rpc_trace,
        },
    )


def run_weather_resolution_reconciliation(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    verification_inputs: list[SettlementVerificationInput] | None = None,
    proposal_ids: list[str] | None = None,
    scheduler: RedeemScheduler | None = None,
    now: datetime | None = None,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    active_scheduler = scheduler or RedeemScheduler()
    observed_now = _normalize_datetime(now) or datetime.now(UTC).replace(tzinfo=None)
    proposals = _load_uma_proposals_for_reconciliation(con, proposal_ids=proposal_ids)
    proposal_map = {item.proposal_id: item for item in proposals}

    verifications = []
    links = []
    for item in verification_inputs or []:
        proposal = proposal_map.get(item.proposal_id)
        if proposal is None:
            raise LookupError(f"proposal not found for reconciliation: {item.proposal_id}")
        verification = build_settlement_verification(
            proposal=proposal,
            expected_outcome=item.expected_outcome,
            confidence=item.confidence,
            sources_checked=list(item.sources_checked),
            evidence_payload=dict(item.evidence_payload),
            discrepancy_details=item.discrepancy_details,
            created_at=observed_now,
        )
        verifications.append(verification)
        links.append(build_evidence_package_link(verification, linked_at=observed_now))

    suggestions = [
        build_redeem_readiness_record(
            proposal,
            scheduler=active_scheduler,
            now=observed_now,
        )
        for proposal in proposals
    ]
    calibration_samples = _build_forecast_calibration_samples_for_verifications(
        con,
        verification_inputs=verification_inputs or [],
        proposal_map=proposal_map,
    )

    task_ids: list[str] = []
    task_ids.extend(_append_task_id(enqueue_settlement_verification_upserts(queue_cfg, verifications=verifications, run_id=request_id)))
    task_ids.extend(
        _append_task_id(
            enqueue_forecast_calibration_sample_upserts(
                queue_cfg,
                samples=calibration_samples,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(_append_task_id(enqueue_evidence_link_upserts(queue_cfg, links=links, run_id=request_id)))
    task_ids.extend(_append_task_id(enqueue_redeem_readiness_upserts(queue_cfg, suggestions=suggestions, run_id=request_id)))
    return ColdPathHandlerResult(
        job_name="weather_resolution_reconciliation",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(suggestions),
        metadata={
            "proposal_count": len(proposals),
            "verification_count": len(verifications),
            "calibration_sample_count": len(calibration_samples),
        },
    )


def run_weather_execution_priors_refresh_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    lookback_days: int = 90,
    as_of: datetime | None = None,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    effective_as_of = (as_of or datetime.now(UTC)).astimezone(UTC).replace(tzinfo=None, microsecond=0)
    source_window_start = effective_as_of - timedelta(days=max(1, int(lookback_days)))
    source_window_end = effective_as_of
    prior_version = "feedback_v1"
    materialization_id = build_feedback_materialization_id(
        run_id=request_id,
        as_of=effective_as_of,
        prior_version=prior_version,
    )
    input_ticket_count = int(
        (
            con.execute(
                "SELECT COUNT(*) FROM runtime.trade_tickets WHERE created_at >= ?",
                [source_window_start],
            ).fetchone()
            or [0]
        )[0]
    )
    task_ids: list[str] = []
    try:
        materialized = materialize_execution_priors(
            con,
            as_of=effective_as_of,
            lookback_days=lookback_days,
            materialization_id=materialization_id,
            prior_version=prior_version,
        )
        degraded_prior_count = sum(1 for item in materialized if item.feedback_status == "degraded")
        task_ids.extend(
            _append_task_id(
                enqueue_execution_prior_upserts(
                    queue_cfg,
                    priors=materialized,
                    run_id=request_id,
                )
            )
        )
        task_ids.extend(
            _append_task_id(
                enqueue_execution_feedback_materialization_upserts(
                    queue_cfg,
                    records=[
                        build_execution_feedback_materialization_status(
                            materialization_id=materialization_id,
                            run_id=request_id,
                            job_name="weather_execution_priors_refresh",
                            prior_version=prior_version,
                            status="ok",
                            lookback_days=lookback_days,
                            source_window_start=source_window_start,
                            source_window_end=source_window_end,
                            input_ticket_count=input_ticket_count,
                            output_prior_count=len(materialized),
                            degraded_prior_count=degraded_prior_count,
                            materialized_at=effective_as_of,
                        )
                    ],
                    run_id=request_id,
                )
            )
        )
    except Exception as exc:
        task_ids.extend(
            _append_task_id(
                enqueue_execution_feedback_materialization_upserts(
                    queue_cfg,
                    records=[
                        build_execution_feedback_materialization_status(
                            materialization_id=materialization_id,
                            run_id=request_id,
                            job_name="weather_execution_priors_refresh",
                            prior_version=prior_version,
                            status="error",
                            lookback_days=lookback_days,
                            source_window_start=source_window_start,
                            source_window_end=source_window_end,
                            input_ticket_count=input_ticket_count,
                            output_prior_count=0,
                            degraded_prior_count=0,
                            materialized_at=effective_as_of,
                            error=str(exc),
                        )
                    ],
                    run_id=request_id,
                )
            )
        )
        raise
    return ColdPathHandlerResult(
        job_name="weather_execution_priors_refresh",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(materialized),
        metadata={
            "lookback_days": int(lookback_days),
            "prior_count": len(materialized),
            "degraded_prior_count": degraded_prior_count,
            "materialization_id": materialization_id,
        },
    )


def run_weather_forecast_calibration_profiles_v2_refresh_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    lookback_days: int = 180,
    as_of: datetime | None = None,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    materialized = materialize_forecast_calibration_profiles_v2(
        con,
        as_of=as_of or datetime.now(UTC),
        lookback_days=lookback_days,
    )
    task_ids = _append_task_id(
        enqueue_forecast_calibration_profile_v2_upserts(
            queue_cfg,
            profiles=materialized,
            run_id=request_id,
        )
    )
    return ColdPathHandlerResult(
        job_name="weather_forecast_calibration_profiles_v2_refresh",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(materialized),
        metadata={
            "lookback_days": int(lookback_days),
            "profile_count": len(materialized),
        },
    )


def run_weather_paper_execution_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    params_json: dict[str, Any],
    run_id: str | None = None,
    observed_at: datetime | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    batch_request = _normalize_paper_execution_request(params_json)
    if batch_request.snapshot_ids:
        snapshots, signal_ts_lookup, created_at_lookup = load_selected_watch_only_snapshots(
            con,
            snapshot_ids=batch_request.snapshot_ids,
        )
    else:
        snapshots, signal_ts_lookup, created_at_lookup = load_selected_watch_only_snapshots(
            con,
            market_ids=batch_request.market_ids,
            limit=batch_request.snapshot_limit,
        )

    selected_snapshot_ids = [snapshot.snapshot_id for snapshot in snapshots]
    selected_market_ids = _stable_unique_values([snapshot.market_id for snapshot in snapshots])
    resolved_asof_ts_ms = batch_request.asof_ts_ms
    if resolved_asof_ts_ms is None:
        resolved_asof_ts_ms = _resolve_paper_execution_asof_ts_ms(
            snapshot_ids=selected_snapshot_ids,
            signal_ts_lookup=signal_ts_lookup,
            created_at_lookup=created_at_lookup,
        )
    ctx = StrategyContext(
        data_snapshot_id=batch_request.data_snapshot_id
        or stable_object_id("dsnap", {"snapshot_ids": selected_snapshot_ids}),
        universe_snapshot_id=batch_request.universe_snapshot_id
        or stable_object_id("usnap", {"market_ids": selected_market_ids}),
        asof_ts_ms=resolved_asof_ts_ms,
        dq_level=batch_request.dq_level,
        quote_snapshot_refs=list(batch_request.quote_snapshot_refs),
    )

    account_capability = load_account_trading_capability(con, wallet_id=batch_request.wallet_id)
    current_inventory_positions = load_inventory_positions(con, wallet_id=batch_request.wallet_id)
    strategy_run, decisions = run_strategy_engine(
        ctx=ctx,
        snapshots=snapshots,
        strategies=list(batch_request.strategy_registrations),
        snapshot_signal_ts_ms=signal_ts_lookup,
        created_at=observed_at or datetime.now(UTC),
    )

    tickets = [build_trade_ticket(decision, created_at=observed_at or datetime.now(UTC)) for decision in decisions]
    market_capabilities = {
        token_id: load_market_capability(con, token_id=token_id)
        for token_id in _stable_unique_values([ticket.token_id for ticket in tickets])
    }
    execution_context_records_by_id = {}
    ticket_execution_context_ids: dict[str, str] = {}
    enriched_tickets = []
    for ticket in tickets:
        execution_context = build_execution_context(
            market_capability=market_capabilities[ticket.token_id],
            account_capability=account_capability,
            route_action=ticket.route_action,
            risk_gate_result="pending_gate",
        )
        record = build_execution_context_record(
            wallet_id=batch_request.wallet_id,
            execution_context=execution_context,
            created_at=observed_at or datetime.now(UTC),
        )
        execution_context_records_by_id.setdefault(record.execution_context_id, record)
        enriched_ticket = bind_trade_ticket_handoff(
            ticket,
            wallet_id=batch_request.wallet_id,
            execution_context_id=record.execution_context_id,
        )
        enriched_tickets.append(enriched_ticket)
        ticket_execution_context_ids[enriched_ticket.ticket_id] = record.execution_context_id

    gate_decisions = []
    paper_orders = []
    reservations = []
    fills = []
    inventory_positions_to_persist = list(current_inventory_positions)
    exposure_snapshots = []
    reconciliation_results = []
    order_transitions = []
    order_ids: list[str] = []
    rejected_ticket_ids: list[str] = []
    journal_events = [
        build_journal_event(
            event_type="strategy_run.created",
            entity_type="strategy_run",
            entity_id=strategy_run.run_id,
            run_id=strategy_run.run_id,
            payload_json={"decision_count": strategy_run.decision_count},
            created_at=observed_at or datetime.now(UTC),
        )
    ]
    for ticket in enriched_tickets:
        hydrated_execution_context = execution_context_records_by_id[ticket.execution_context_id or ""].execution_context
        signal_order_intent = build_signal_order_intent_from_handoff(
            ticket,
            execution_context=hydrated_execution_context,
        )
        preview_order = build_order_from_intent(
            signal_order_intent,
            wallet_id=ticket.wallet_id or "",
            created_at=observed_at or datetime.now(UTC),
        )
        existing_reservation = load_reservation_for_order(con, order_id=preview_order.order_id)
        available_quantity = available_inventory_quantity_for_ticket(
            current_inventory_positions,
            ticket=ticket,
        )
        if existing_reservation is not None:
            available_quantity = max(available_quantity, existing_reservation.reserved_quantity)
        gate_decision = evaluate_execution_gate(
            ticket=ticket,
            intent=signal_order_intent,
            watch_only_active=batch_request.dq_level != "PASS",
            degrade_active=False,
            available_quantity=available_quantity,
            created_at=observed_at or datetime.now(UTC),
        )
        gate_decisions.append(gate_decision)
        routed_order = route_trade_ticket(ticket, hydrated_execution_context)
        journal_events.append(
            build_journal_event(
                event_type="trade_ticket.created",
                entity_type="trade_ticket",
                entity_id=ticket.ticket_id,
                run_id=strategy_run.run_id,
                payload_json={
                    "request_id": ticket.request_id,
                    "ticket_id": ticket.ticket_id,
                    "wallet_id": ticket.wallet_id,
                    "execution_context_id": ticket.execution_context_id,
                    "ticket_hash": ticket.ticket_hash,
                },
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="gate.decision",
                entity_type="gate_decision",
                entity_id=gate_decision.gate_id,
                run_id=strategy_run.run_id,
                payload_json={
                    "ticket_id": ticket.ticket_id,
                    "request_id": ticket.request_id,
                    "allowed": gate_decision.allowed,
                    "reason": gate_decision.reason,
                    "reason_codes": list(gate_decision.reason_codes),
                    "metrics": dict(gate_decision.metrics_json),
                },
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="canonical_order.routed",
                entity_type="canonical_order",
                entity_id=ticket.request_id,
                run_id=strategy_run.run_id,
                payload_json=canonical_order_router_payload(routed_order),
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="signal_order_intent.created",
                entity_type="signal_order_intent",
                entity_id=ticket.request_id,
                run_id=strategy_run.run_id,
                payload_json={
                    "ticket_id": ticket.ticket_id,
                    "request_id": ticket.request_id,
                    "wallet_id": ticket.wallet_id,
                    "execution_context_id": ticket.execution_context_id,
                    **canonical_order_handoff_payload(signal_order_intent),
                    "canonical_order_hash": canonical_order_router_hash(routed_order),
                },
                created_at=observed_at or datetime.now(UTC),
            )
        )
        if not gate_decision.allowed:
            rejected_ticket_ids.append(ticket.ticket_id)
            journal_events.append(
                build_journal_event(
                    event_type="order.rejected_by_gate",
                    entity_type="order",
                    entity_id=ticket.request_id,
                    run_id=strategy_run.run_id,
                    payload_json=gate_rejection_journal_payload(
                        ticket_id=ticket.ticket_id,
                        request_id=ticket.request_id,
                        wallet_id=ticket.wallet_id or "",
                        gate_decision=gate_decision,
                    ),
                    created_at=observed_at or datetime.now(UTC),
                )
            )
            continue
        created_order = build_paper_order(
            intent=signal_order_intent,
            wallet_id=ticket.wallet_id or "",
            gate_decision=gate_decision,
            created_at=observed_at or datetime.now(UTC),
        )
        reservation = existing_reservation or build_reservation(
            created_order,
            created_at=observed_at or datetime.now(UTC),
        )
        if existing_reservation is None:
            current_inventory_positions = apply_reservation_to_inventory(
                current_inventory_positions,
                reservation,
                observed_at=observed_at or datetime.now(UTC),
            )
        posted_order, posted_transition = transition_order_to_posted(
            created_order,
            timestamp=observed_at or datetime.now(UTC),
        )
        fill_result = simulate_quote_based_fill(
            order=posted_order,
            ticket=ticket,
            observed_at=observed_at or datetime.now(UTC),
        )
        current_reservation = reservation
        if existing_reservation is None:
            for fill in fill_result.fills:
                current_inventory_positions = apply_fill_to_inventory(
                    current_inventory_positions,
                    order=posted_order,
                    reservation=current_reservation,
                    fill=fill,
                    observed_at=fill.filled_at,
                )
                current_reservation = apply_fill_to_reservation(
                    current_reservation,
                    fill,
                    observed_at=fill.filled_at,
                )
        final_order, final_transition = apply_fills_to_order(
            posted_order,
            fills=fill_result.fills,
            timestamp=fill_result.observed_at,
        )
        if existing_reservation is None:
            if final_order.status is OrderStatus.CANCELLED and current_reservation.remaining_quantity > 0:
                current_inventory_positions = release_reservation_to_inventory(
                    current_inventory_positions,
                    current_reservation,
                    observed_at=fill_result.observed_at,
                )
            final_reservation = finalize_reservation(
                current_reservation,
                order_status=final_order.status,
                observed_at=fill_result.observed_at,
            )
        else:
            final_reservation = current_reservation
        exposure_snapshot = build_exposure_snapshot(
            final_order,
            positions=current_inventory_positions,
            reservation=final_reservation,
            captured_at=fill_result.observed_at,
        )
        reconciliation_result = build_reconciliation_result(
            order=final_order,
            reservation=final_reservation,
            fills=fill_result.fills,
            positions=current_inventory_positions,
            exposure_snapshot=exposure_snapshot,
            created_at=fill_result.observed_at,
        )
        paper_orders.append(final_order)
        reservations.append(final_reservation)
        fills.extend(fill_result.fills)
        inventory_positions_to_persist = list(current_inventory_positions)
        exposure_snapshots.append(exposure_snapshot)
        reconciliation_results.append(reconciliation_result)
        order_ids.append(final_order.order_id)
        order_transitions.append(posted_transition)
        if final_transition is not None:
            order_transitions.append(final_transition)
        journal_events.append(
            build_journal_event(
                event_type="order.created",
                entity_type="order",
                entity_id=created_order.order_id,
                run_id=strategy_run.run_id,
                payload_json=paper_order_journal_payload_with_status(
                    order=created_order,
                    ticket_id=ticket.ticket_id,
                    request_id=ticket.request_id,
                    status=OrderStatus.CREATED,
                ),
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="reservation.created",
                entity_type="reservation",
                entity_id=reservation.reservation_id,
                run_id=strategy_run.run_id,
                payload_json={
                    "reservation_id": reservation.reservation_id,
                    "order_id": reservation.order_id,
                    "ticket_id": ticket.ticket_id,
                    "request_id": ticket.request_id,
                    "asset_type": reservation.asset_type,
                    "reserved_quantity": str(reservation.reserved_quantity),
                    "remaining_quantity": str(reservation.remaining_quantity),
                    "status": reservation.status.value,
                },
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="order.posted",
                entity_type="order",
                entity_id=posted_order.order_id,
                run_id=strategy_run.run_id,
                payload_json=paper_order_journal_payload_with_status(
                    order=posted_order,
                    ticket_id=ticket.ticket_id,
                    request_id=ticket.request_id,
                    status=OrderStatus.POSTED,
                ),
                created_at=observed_at or datetime.now(UTC),
            )
        )
        for fill in fill_result.fills:
            journal_events.append(
                build_journal_event(
                    event_type="fill.created",
                    entity_type="fill",
                    entity_id=fill.fill_id,
                    run_id=strategy_run.run_id,
                    payload_json=fill_journal_payload(
                        fill=fill,
                        ticket_id=ticket.ticket_id,
                        request_id=ticket.request_id,
                    ),
                    created_at=observed_at or datetime.now(UTC),
                )
            )
        if final_order.status is OrderStatus.PARTIAL_FILLED:
            journal_events.append(
                build_journal_event(
                    event_type="order.partial_filled",
                    entity_type="order",
                    entity_id=final_order.order_id,
                    run_id=strategy_run.run_id,
                    payload_json=order_status_journal_payload(
                        order=final_order,
                        ticket_id=ticket.ticket_id,
                        request_id=ticket.request_id,
                        reason=fill_result.outcome_reason,
                    ),
                    created_at=observed_at or datetime.now(UTC),
                )
            )
        elif final_order.status is OrderStatus.FILLED:
            journal_events.append(
                build_journal_event(
                    event_type="order.filled",
                    entity_type="order",
                    entity_id=final_order.order_id,
                    run_id=strategy_run.run_id,
                    payload_json=order_status_journal_payload(
                        order=final_order,
                        ticket_id=ticket.ticket_id,
                        request_id=ticket.request_id,
                        reason=fill_result.outcome_reason,
                    ),
                    created_at=observed_at or datetime.now(UTC),
                )
            )
        elif final_order.status is OrderStatus.CANCELLED:
            journal_events.append(
                build_journal_event(
                    event_type="order.cancelled",
                    entity_type="order",
                    entity_id=final_order.order_id,
                    run_id=strategy_run.run_id,
                    payload_json=order_status_journal_payload(
                        order=final_order,
                        ticket_id=ticket.ticket_id,
                        request_id=ticket.request_id,
                        reason=fill_result.outcome_reason,
                    ),
                    created_at=observed_at or datetime.now(UTC),
                )
            )
        if final_reservation.status.value == "partially_consumed":
            reservation_event_type = "reservation.partially_consumed"
        elif final_reservation.status.value == "converted":
            reservation_event_type = "reservation.converted"
        elif final_reservation.status.value == "released":
            reservation_event_type = "reservation.released"
        else:
            reservation_event_type = "reservation.updated"
        journal_events.append(
            build_journal_event(
                event_type=reservation_event_type,
                entity_type="reservation",
                entity_id=final_reservation.reservation_id,
                run_id=strategy_run.run_id,
                payload_json={
                    "reservation_id": final_reservation.reservation_id,
                    "order_id": final_reservation.order_id,
                    "ticket_id": ticket.ticket_id,
                    "request_id": ticket.request_id,
                    "remaining_quantity": str(final_reservation.remaining_quantity),
                    "status": final_reservation.status.value,
                },
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="inventory.updated",
                entity_type="inventory_position",
                entity_id=final_order.order_id,
                run_id=strategy_run.run_id,
                payload_json={
                    "order_id": final_order.order_id,
                    "ticket_id": ticket.ticket_id,
                    "request_id": ticket.request_id,
                    "position_count": len(current_inventory_positions),
                },
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="exposure.snapshot",
                entity_type="exposure_snapshot",
                entity_id=exposure_snapshot.snapshot_id,
                run_id=strategy_run.run_id,
                payload_json={
                    "snapshot_id": exposure_snapshot.snapshot_id,
                    "order_id": final_order.order_id,
                    "ticket_id": ticket.ticket_id,
                    "request_id": ticket.request_id,
                    "open_order_size": str(exposure_snapshot.open_order_size),
                    "reserved_notional_usdc": str(exposure_snapshot.reserved_notional_usdc),
                    "filled_position_size": str(exposure_snapshot.filled_position_size),
                    "settled_position_size": str(exposure_snapshot.settled_position_size),
                    "redeemable_size": str(exposure_snapshot.redeemable_size),
                },
                created_at=observed_at or datetime.now(UTC),
            )
        )
        journal_events.append(
            build_journal_event(
                event_type="reconciliation.checked",
                entity_type="reconciliation",
                entity_id=reconciliation_result.reconciliation_id,
                run_id=strategy_run.run_id,
                payload_json=reconciliation_journal_payload(
                    result=reconciliation_result,
                    order=final_order,
                    ticket_id=ticket.ticket_id,
                    request_id=ticket.request_id,
                ),
                created_at=observed_at or datetime.now(UTC),
            )
        )
        if reconciliation_result.status.value != "ok":
            journal_events.append(
                build_journal_event(
                    event_type="reconciliation.mismatch",
                    entity_type="reconciliation",
                    entity_id=reconciliation_result.reconciliation_id,
                    run_id=strategy_run.run_id,
                    payload_json=reconciliation_journal_payload(
                        result=reconciliation_result,
                        order=final_order,
                        ticket_id=ticket.ticket_id,
                        request_id=ticket.request_id,
                    ),
                    created_at=observed_at or datetime.now(UTC),
                )
            )

    task_ids: list[str] = []
    task_ids.extend(_append_task_id(enqueue_strategy_run_upserts(queue_cfg, runs=[strategy_run], run_id=request_id)))
    task_ids.extend(_append_task_id(enqueue_trade_ticket_upserts(queue_cfg, tickets=enriched_tickets, run_id=request_id)))
    task_ids.extend(_append_task_id(enqueue_gate_decision_upserts(queue_cfg, gate_decisions=gate_decisions, run_id=request_id)))
    task_ids.extend(_append_task_id(enqueue_order_upserts(queue_cfg, orders=paper_orders, run_id=request_id)))
    task_ids.extend(_append_task_id(enqueue_reservation_upserts(queue_cfg, reservations=reservations, run_id=request_id)))
    task_ids.extend(_append_task_id(enqueue_fill_upserts(queue_cfg, fills=fills, run_id=request_id)))
    task_ids.extend(
        _append_task_id(
            enqueue_inventory_position_upserts(
                queue_cfg,
                positions=inventory_positions_to_persist,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_exposure_snapshot_upserts(
                queue_cfg,
                snapshots=exposure_snapshots,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_reconciliation_result_upserts(
                queue_cfg,
                results=reconciliation_results,
                run_id=request_id,
            )
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_order_state_transition_upserts(queue_cfg, transitions=order_transitions, run_id=request_id)
        )
    )
    task_ids.extend(
        _append_task_id(
            enqueue_execution_context_upserts(
                queue_cfg,
                execution_contexts=list(execution_context_records_by_id.values()),
                run_id=request_id,
            )
        )
    )
    task_ids.extend(_append_task_id(enqueue_journal_event_upserts(queue_cfg, journal_events=journal_events, run_id=request_id)))
    return ColdPathHandlerResult(
        job_name="weather_paper_execution",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(enriched_tickets),
        metadata={
            "wallet_id": batch_request.wallet_id,
            "selected_snapshot_ids": selected_snapshot_ids,
            "selected_market_ids": selected_market_ids,
            "strategy_ids": [item.strategy_id for item in batch_request.strategy_registrations],
            "decision_count": len(decisions),
            "ticket_count": len(enriched_tickets),
            "gate_count": len(gate_decisions),
            "allowed_order_count": len(paper_orders),
            "reservation_count": len(reservations),
            "fill_count": len(fills),
            "inventory_position_count": len(inventory_positions_to_persist),
            "exposure_snapshot_count": len(exposure_snapshots),
            "reconciliation_count": len(reconciliation_results),
            "reconciliation_mismatch_count": sum(1 for item in reconciliation_results if item.status.value != "ok"),
            "order_ids": order_ids,
            "rejected_ticket_ids": rejected_ticket_ids,
            "ticket_ids": [item.ticket_id for item in enriched_tickets],
            "execution_context_count": len(execution_context_records_by_id),
            "ticket_execution_context_ids": ticket_execution_context_ids,
            "strategy_run_id": strategy_run.run_id,
        },
    )


def run_weather_rule2spec_review_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    client=None,
    mapper: StationMapper | None = None,
    market_ids: list[str] | None = None,
    active_only: bool = False,
    limit: int | None = None,
    force_rerun: bool = False,
    now: datetime | None = None,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    active_client = client or build_agent_client_from_env()
    requests = load_rule2spec_agent_requests(
        con,
        mapper=mapper,
        market_ids=market_ids,
        active_only=active_only,
        limit=limit,
    )
    artifacts = [
        run_rule2spec_agent_review(
            active_client,
            request,
            force_rerun=force_rerun,
            now=now,
        )
        for request in requests
    ]
    task_ids = enqueue_agent_artifact_upserts(queue_cfg, artifacts=artifacts, run_id=request_id)
    return ColdPathHandlerResult(
        job_name="weather_rule2spec_review",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(artifacts),
        metadata={
            "output_count": sum(1 for item in artifacts if item.output is not None),
            "subject_ids": [item.invocation.subject_id for item in artifacts],
        },
    )


def run_weather_data_qa_review_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    client=None,
    replay_ids: list[str] | None = None,
    limit: int | None = None,
    force_rerun: bool = False,
    now: datetime | None = None,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    active_client = client or build_agent_client_from_env()
    requests = load_data_qa_agent_requests(con, replay_ids=replay_ids, limit=limit)
    artifacts = [
        run_data_qa_agent_review(
            active_client,
            request,
            force_rerun=force_rerun,
            now=now,
        )
        for request in requests
    ]
    task_ids = enqueue_agent_artifact_upserts(queue_cfg, artifacts=artifacts, run_id=request_id)
    return ColdPathHandlerResult(
        job_name="weather_data_qa_review",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(artifacts),
        metadata={
            "output_count": sum(1 for item in artifacts if item.output is not None),
            "subject_ids": [item.invocation.subject_id for item in artifacts],
        },
    )


def run_weather_resolution_review_job(
    con,
    queue_cfg: WriteQueueConfig,
    *,
    client=None,
    proposal_ids: list[str] | None = None,
    limit: int | None = None,
    force_rerun: bool = False,
    now: datetime | None = None,
    run_id: str | None = None,
) -> ColdPathHandlerResult:
    request_id = run_id or new_request_id()
    active_client = client or build_agent_client_from_env()
    requests = load_resolution_agent_requests(con, proposal_ids=proposal_ids, limit=limit)
    artifacts = [
        run_resolution_agent_review(
            active_client,
            request,
            force_rerun=force_rerun,
            now=now,
        )
        for request in requests
    ]
    task_ids = enqueue_agent_artifact_upserts(queue_cfg, artifacts=artifacts, run_id=request_id)
    return ColdPathHandlerResult(
        job_name="weather_resolution_review",
        run_id=request_id,
        task_ids=task_ids,
        item_count=len(artifacts),
        metadata={
            "output_count": sum(1 for item in artifacts if item.output is not None),
            "subject_ids": [item.invocation.subject_id for item in artifacts],
        },
    )


def _load_weather_market_specs(con, *, market_ids: list[str] | None = None) -> list:
    sql = """
        SELECT market_id
        FROM weather.weather_market_specs
    """
    params: list[Any] = []
    if market_ids:
        placeholders = ",".join(["?"] * len(market_ids))
        sql += f" WHERE market_id IN ({placeholders})"
        params.extend(market_ids)
    sql += " ORDER BY market_id"
    rows = con.execute(sql, params).fetchall()
    return [load_weather_market_spec(con, market_id=str(row[0])) for row in rows]


def _resolution_spec_from_record(record) -> ResolutionSpec:
    return ResolutionSpec(
        market_id=record.market_id,
        condition_id=record.condition_id,
        location_name=record.location_name,
        station_id=record.station_id,
        latitude=record.latitude,
        longitude=record.longitude,
        timezone=record.timezone,
        observation_date=record.observation_date,
        observation_window_local=record.observation_window_local,
        metric=record.metric,
        unit=record.unit,
        authoritative_source=record.authoritative_source,
        fallback_sources=list(record.fallback_sources),
        rounding_rule=record.rounding_rule,
        inclusive_bounds=record.inclusive_bounds,
        spec_version=record.spec_version,
    )


def _load_uma_proposals_for_reconciliation(con, *, proposal_ids: list[str] | None = None) -> list[UMAProposal]:
    sql = """
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
    params: list[Any] = []
    if proposal_ids:
        placeholders = ",".join(["?"] * len(proposal_ids))
        sql += f" WHERE proposal_id IN ({placeholders})"
        params.extend(proposal_ids)
    sql += " ORDER BY proposal_block_number, proposal_id"
    rows = con.execute(sql, params).fetchall()
    return [
        UMAProposal(
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
        for row in rows
    ]


def _build_forecast_calibration_samples_for_verifications(
    con,
    *,
    verification_inputs: list[SettlementVerificationInput],
    proposal_map: dict[str, UMAProposal],
) -> list:
    if con is None:
        return []
    samples = []
    for item in verification_inputs:
        proposal = proposal_map.get(item.proposal_id)
        if proposal is None or not proposal.market_id:
            continue
        observed_value = item.evidence_payload.get("observed_value")
        try:
            observed_value_float = float(observed_value)
        except (TypeError, ValueError):
            continue
        try:
            row = con.execute(
                """
                SELECT run_id
                FROM weather.weather_forecast_runs
                WHERE market_id = ?
                ORDER BY forecast_target_time DESC, created_at DESC, run_id DESC
                LIMIT 1
                """,
                [proposal.market_id],
            ).fetchone()
        except Exception:  # noqa: BLE001
            continue
        if row is None or row[0] is None:
            continue
        try:
            forecast_run = load_forecast_run(con, run_id=str(row[0]))
        except Exception:  # noqa: BLE001
            continue
        samples.append(
            build_forecast_calibration_sample(
                forecast_run=forecast_run,
                observed_value=observed_value_float,
            )
        )
    return samples


def _get_replay_value(raw: ForecastReplayRequest | dict[str, Any], key: str) -> Any:
    if isinstance(raw, ForecastReplayRequest):
        return getattr(raw, key)
    return raw[key]


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    raise TypeError("forecast_target_time must be datetime or ISO timestamp string")


def _append_task_id(task_id: str | None) -> list[str]:
    return [task_id] if task_id is not None else []


def _load_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value is None:
        return {}
    if isinstance(value, str):
        loaded = json.loads(value)
        if not isinstance(loaded, dict):
            raise ValueError("expected JSON object")
        return dict(loaded)
    raise TypeError("expected dict or JSON object string")


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _normalize_paper_execution_request(params_json: dict[str, Any]) -> PaperExecutionBatchRequest:
    if not isinstance(params_json, dict):
        raise ValueError("params_json must be a dictionary")
    raw_strategies = params_json.get("strategy_registrations")
    if not isinstance(raw_strategies, list) or not raw_strategies:
        raise ValueError("strategy_registrations are required")
    strategy_registrations = [_parse_strategy_registration(item) for item in raw_strategies]
    raw_snapshot_ids = params_json.get("snapshot_ids")
    raw_market_ids = params_json.get("market_ids")
    snapshot_ids = _coerce_optional_str_list(raw_snapshot_ids)
    market_ids = _coerce_optional_str_list(raw_market_ids)
    snapshot_limit = params_json.get("snapshot_limit")
    return PaperExecutionBatchRequest(
        wallet_id=str(params_json.get("wallet_id") or ""),
        strategy_registrations=strategy_registrations,
        snapshot_ids=snapshot_ids,
        market_ids=market_ids,
        snapshot_limit=int(snapshot_limit) if snapshot_limit is not None else None,
        dq_level=str(params_json.get("dq_level") or "PASS"),
        data_snapshot_id=_coerce_optional_non_empty_string(params_json.get("data_snapshot_id")),
        universe_snapshot_id=_coerce_optional_non_empty_string(params_json.get("universe_snapshot_id")),
        asof_ts_ms=int(params_json["asof_ts_ms"]) if params_json.get("asof_ts_ms") is not None else None,
        quote_snapshot_refs=_coerce_optional_str_list(params_json.get("quote_snapshot_refs")) or [],
    )


def _normalize_signer_audit_smoke_request(params_json: dict[str, Any]) -> SignerAuditSmokeRequest:
    if not isinstance(params_json, dict):
        raise ValueError("params_json must be a dictionary")
    raw_signing_purpose = str(params_json.get("signing_purpose") or "").strip().lower()
    if raw_signing_purpose not in {"transaction", "l2_auth"}:
        raise ValueError(
            "weather_signer_audit_smoke only supports transaction or l2_auth; use weather_order_signing_smoke for order signing"
        )
    raw_payload = params_json.get("payload_json")
    if not isinstance(raw_payload, dict) or not raw_payload:
        raise ValueError("payload_json must be a non-empty object")
    fee_rate_bps = params_json.get("fee_rate_bps")
    return SignerAuditSmokeRequest(
        wallet_id=str(params_json.get("wallet_id") or ""),
        requester=str(params_json.get("requester") or ""),
        signing_purpose=SigningPurpose(raw_signing_purpose),
        payload_json=dict(raw_payload),
        token_id=_coerce_optional_non_empty_string(params_json.get("token_id")),
        fee_rate_bps=int(fee_rate_bps) if fee_rate_bps is not None else None,
    )


def _normalize_order_signing_smoke_request(params_json: dict[str, Any]) -> OrderSigningSmokeRequest:
    if not isinstance(params_json, dict):
        raise ValueError("params_json must be a dictionary")
    ticket_ids = _coerce_optional_str_list(params_json.get("ticket_ids"))
    return OrderSigningSmokeRequest(
        ticket_ids=ticket_ids or [],
        requester=str(params_json.get("requester") or ""),
    )


def _normalize_submitter_smoke_request(params_json: dict[str, Any]) -> SubmitterSmokeRequest:
    if not isinstance(params_json, dict):
        raise ValueError("params_json must be a dictionary")
    attempt_ids = _coerce_optional_str_list(params_json.get("attempt_ids"))
    raw_mode = str(params_json.get("submit_mode") or "").strip().lower() or SubmitMode.DRY_RUN.value
    raw_shadow_fill_mode = str(params_json.get("shadow_fill_mode") or "").strip().lower() or ShadowFillMode.NONE.value
    return SubmitterSmokeRequest(
        attempt_ids=attempt_ids or [],
        requester=str(params_json.get("requester") or ""),
        submit_mode=SubmitMode(raw_mode),
        shadow_fill_mode=ShadowFillMode(raw_shadow_fill_mode),
        approval_token=str(params_json.get("approval_token") or "").strip() or None,
    )


def _normalize_chain_tx_smoke_request(params_json: dict[str, Any]) -> ChainTxSmokeRequest:
    if not isinstance(params_json, dict):
        raise ValueError("params_json must be a dictionary")
    raw_tx_kind = str(params_json.get("tx_kind") or "").strip().lower()
    raw_tx_mode = str(params_json.get("tx_mode") or "").strip().lower() or ChainTxMode.DRY_RUN.value
    return ChainTxSmokeRequest(
        wallet_id=str(params_json.get("wallet_id") or ""),
        requester=str(params_json.get("requester") or ""),
        tx_kind=ChainTxKind(raw_tx_kind),
        tx_mode=ChainTxMode(raw_tx_mode),
        spender=str(params_json.get("spender") or "").strip(),
        amount=Decimal(str(params_json.get("amount") or "0")),
    )


def _normalize_controlled_live_smoke_request(params_json: dict[str, Any]) -> ControlledLiveSmokeRequest:
    if not isinstance(params_json, dict):
        raise ValueError("params_json must be a dictionary")
    raw_tx_kind = str(params_json.get("tx_kind") or "").strip().lower()
    return ControlledLiveSmokeRequest(
        wallet_id=str(params_json.get("wallet_id") or ""),
        requester=str(params_json.get("requester") or ""),
        approval_id=str(params_json.get("approval_id") or "").strip(),
        approval_reason=str(params_json.get("approval_reason") or "").strip(),
        approval_token=str(params_json.get("approval_token") or "").strip(),
        tx_kind=ChainTxKind(raw_tx_kind),
        spender=str(params_json.get("spender") or "").strip(),
        amount=Decimal(str(params_json.get("amount") or "0")),
    )


def _load_readiness_report_or_none(path: str) -> ReadinessReport | None:
    if not path or not os.path.exists(path):
        return None
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return ReadinessReport.from_dict(payload)


def _load_live_prereq_wallet_status(ui_lite_db_path: str, *, wallet_id: str) -> str | None:
    import duckdb

    con = duckdb.connect(ui_lite_db_path, read_only=True)
    try:
        row = con.execute(
            """
            SELECT wallet_readiness_status
            FROM ui.live_prereq_wallet_summary
            WHERE wallet_id = ?
            """,
            [wallet_id],
        ).fetchone()
    finally:
        con.close()
    return None if row is None or row[0] is None else str(row[0])


def _load_shadow_submit_attempts_for_external_reconciliation(con) -> list[SubmitAttemptRecord]:
    rows = con.execute(
        """
        SELECT
            attempt_id,
            request_id,
            ticket_id,
            order_id,
            wallet_id,
            execution_context_id,
            exchange,
            attempt_kind,
            attempt_mode,
            canonical_order_hash,
            payload_hash,
            submit_payload_json,
            signed_payload_ref,
            status,
            error,
            created_at
        FROM runtime.submit_attempts
        WHERE attempt_kind = 'submit_order'
          AND attempt_mode IN ('shadow_submit', 'live_submit')
        ORDER BY created_at ASC, attempt_id ASC
        """
    ).fetchall()
    return [_submit_attempt_from_row(row) for row in rows]


def _submit_attempt_from_row(row: tuple[Any, ...]) -> SubmitAttemptRecord:
    return SubmitAttemptRecord(
        attempt_id=str(row[0]),
        request_id=str(row[1]),
        ticket_id=str(row[2]),
        order_id=str(row[3]) if row[3] is not None else None,
        wallet_id=str(row[4]),
        execution_context_id=str(row[5]),
        exchange=str(row[6]),
        attempt_kind=str(row[7]),
        attempt_mode=str(row[8]),
        canonical_order_hash=str(row[9]),
        payload_hash=str(row[10]),
        submit_payload_json=_load_json_dict(row[11]),
        signed_payload_ref=str(row[12]) if row[12] is not None else "",
        status=str(row[13]),
        error=str(row[14]) if row[14] is not None else None,
        created_at=_normalize_datetime(row[15]) or datetime.now(UTC).replace(tzinfo=None),
    )


def _load_latest_external_order_observation(con, *, attempt_id: str):
    row = con.execute(
        """
        SELECT
            observation_id,
            attempt_id,
            request_id,
            ticket_id,
            order_id,
            wallet_id,
            execution_context_id,
            exchange,
            observation_kind,
            submit_mode,
            canonical_order_hash,
            external_order_id,
            external_status,
            observed_at,
            error,
            raw_observation_json
        FROM runtime.external_order_observations
        WHERE attempt_id = ?
        ORDER BY observed_at DESC, observation_id DESC
        LIMIT 1
        """,
        [attempt_id],
    ).fetchone()
    if row is None:
        return None
    return type(
        "ExternalOrderObservationView",
        (),
        {
            "observation_id": str(row[0]),
            "attempt_id": str(row[1]),
            "request_id": str(row[2]),
            "ticket_id": str(row[3]),
            "order_id": str(row[4]) if row[4] is not None else None,
            "wallet_id": str(row[5]),
            "execution_context_id": str(row[6]),
            "exchange": str(row[7]),
            "observation_kind": str(row[8]),
            "submit_mode": str(row[9]),
            "canonical_order_hash": str(row[10]),
            "external_order_id": str(row[11]) if row[11] is not None else None,
            "external_status": str(row[12]),
            "observed_at": _normalize_datetime(row[13]) or datetime.now(UTC).replace(tzinfo=None),
            "error": str(row[14]) if row[14] is not None else None,
            "raw_observation_json": _load_json_dict(row[15]),
        },
    )()


def _load_latest_external_fill_observations(con, *, attempt_id: str) -> list[ExternalFillObservation]:
    rows = con.execute(
        """
        SELECT
            observation_id,
            attempt_id,
            request_id,
            ticket_id,
            order_id,
            wallet_id,
            execution_context_id,
            exchange,
            observation_kind,
            external_order_id,
            external_trade_id,
            market_id,
            token_id,
            outcome,
            side,
            price,
            size,
            fee,
            fee_rate_bps,
            external_status,
            observed_at,
            error,
            raw_observation_json
        FROM runtime.external_fill_observations
        WHERE attempt_id = ?
        ORDER BY observed_at DESC, observation_id DESC
        """,
        [attempt_id],
    ).fetchall()
    latest_by_trade_id: dict[str, ExternalFillObservation] = {}
    for row in rows:
        trade_id = str(row[10])
        if trade_id in latest_by_trade_id:
            continue
        latest_by_trade_id[trade_id] = ExternalFillObservation(
            observation_id=str(row[0]),
            attempt_id=str(row[1]),
            request_id=str(row[2]),
            ticket_id=str(row[3]),
            order_id=str(row[4]) if row[4] is not None else None,
            wallet_id=str(row[5]),
            execution_context_id=str(row[6]),
            exchange=str(row[7]),
            observation_kind=ExternalFillObservationKind(str(row[8])),
            external_order_id=str(row[9]) if row[9] is not None else None,
            external_trade_id=trade_id,
            market_id=str(row[11]),
            token_id=str(row[12]),
            outcome=str(row[13]),
            side=str(row[14]),
            price=Decimal(str(row[15])),
            size=Decimal(str(row[16])),
            fee=Decimal(str(row[17])),
            fee_rate_bps=int(row[18]),
            external_status=str(row[19]),
            observed_at=_normalize_datetime(row[20]) or datetime.now(UTC).replace(tzinfo=None),
            error=str(row[21]) if row[21] is not None else None,
            raw_observation_json=_load_json_dict(row[22]),
        )
    return list(latest_by_trade_id.values())


def _load_latest_wallet_observation_reference(con, *, wallet_id: str) -> ExternalBalanceObservation | None:
    row = con.execute(
        """
        SELECT
            observation_id,
            wallet_id,
            funder,
            signature_type,
            asset_type,
            token_id,
            market_id,
            outcome,
            observation_kind,
            allowance_target,
            chain_id,
            block_number,
            observed_quantity,
            source,
            observed_at,
            raw_observation_json
        FROM runtime.external_balance_observations
        WHERE wallet_id = ?
        ORDER BY observed_at DESC, observation_id DESC
        LIMIT 1
        """,
        [wallet_id],
    ).fetchone()
    if row is None:
        return None
    return ExternalBalanceObservation(
        observation_id=str(row[0]),
        wallet_id=str(row[1]),
        funder=str(row[2]),
        signature_type=int(row[3]),
        asset_type=str(row[4]),
        token_id=str(row[5]) if row[5] is not None else None,
        market_id=str(row[6]) if row[6] is not None else None,
        outcome=str(row[7]) if row[7] is not None else None,
        observation_kind=ExternalBalanceObservationKind(str(row[8])),
        allowance_target=str(row[9]) if row[9] is not None else None,
        chain_id=int(row[10]),
        block_number=int(row[11]) if row[11] is not None else None,
        observed_quantity=Decimal(str(row[12])),
        source=str(row[13]),
        observed_at=_normalize_datetime(row[14]) or datetime.now(UTC).replace(tzinfo=None),
        raw_observation_json=_load_json_dict(row[15]),
    )


def _load_order_for_reconciliation(con, *, order_id: str) -> Order | None:
    row = con.execute(
        """
        SELECT
            order_id,
            client_order_id,
            wallet_id,
            market_id,
            token_id,
            outcome,
            side,
            price,
            size,
            route_action,
            time_in_force,
            expiration,
            fee_rate_bps,
            signature_type,
            funder,
            status,
            filled_size,
            remaining_size,
            avg_fill_price,
            reservation_id,
            exchange_order_id,
            created_at,
            updated_at
        FROM trading.orders
        WHERE order_id = ?
        """,
        [order_id],
    ).fetchone()
    if row is None:
        return None
    expiration = _normalize_datetime(row[11])
    avg_fill_price = Decimal(str(row[18])) if row[18] is not None else None
    return Order(
        order_id=str(row[0]),
        client_order_id=str(row[1]),
        wallet_id=str(row[2]),
        market_id=str(row[3]),
        token_id=str(row[4]),
        outcome=str(row[5]),
        side=OrderSide(str(row[6])),
        price=Decimal(str(row[7])),
        size=Decimal(str(row[8])),
        route_action=RouteAction(str(row[9])),
        time_in_force=TimeInForce(str(row[10])),
        expiration=expiration,
        fee_rate_bps=int(row[12]),
        signature_type=int(row[13]),
        funder=str(row[14]),
        status=OrderStatus(str(row[15])),
        filled_size=Decimal(str(row[16])),
        remaining_size=Decimal(str(row[17])),
        avg_fill_price=avg_fill_price,
        reservation_id=str(row[19]) if row[19] is not None else None,
        exchange_order_id=str(row[20]) if row[20] is not None else None,
        created_at=_normalize_datetime(row[21]) or datetime.now(UTC).replace(tzinfo=None),
        updated_at=_normalize_datetime(row[22]) or datetime.now(UTC).replace(tzinfo=None),
    )


def _parse_strategy_registration(raw: Any) -> StrategyRegistration:
    if not isinstance(raw, dict):
        raise ValueError("each strategy registration must be a dictionary")
    raw_route_action = raw.get("route_action")
    return StrategyRegistration(
        strategy_id=str(raw.get("strategy_id") or ""),
        strategy_version=str(raw.get("strategy_version") or ""),
        priority=int(raw.get("priority", 0)),
        route_action=_coerce_route_action(raw_route_action),
        size=Decimal(str(raw.get("size"))),
        min_edge_bps=int(raw["min_edge_bps"]) if raw.get("min_edge_bps") is not None else None,
        params=dict(raw.get("params") or {}),
    )


def _coerce_route_action(value: Any) -> RouteAction:
    if isinstance(value, RouteAction):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("route_action is required")
    try:
        return RouteAction(text.lower())
    except ValueError:
        return RouteAction[text.upper()]


def _resolve_paper_execution_asof_ts_ms(
    *,
    snapshot_ids: list[str],
    signal_ts_lookup: dict[str, int],
    created_at_lookup: dict[str, datetime | None],
) -> int:
    signal_values = [int(signal_ts_lookup[snapshot_id]) for snapshot_id in snapshot_ids if int(signal_ts_lookup.get(snapshot_id, 0)) > 0]
    if signal_values:
        return max(signal_values)
    created_values = []
    for snapshot_id in snapshot_ids:
        created_at = created_at_lookup.get(snapshot_id)
        if created_at is None:
            continue
        created_values.append(int(created_at.replace(tzinfo=UTC).timestamp() * 1000))
    if created_values:
        return max(created_values)
    return 0


def _coerce_optional_str_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("selector fields must be lists when provided")
    ordered = _stable_unique_values([str(item) for item in value if str(item)])
    return ordered or None


def _coerce_optional_non_empty_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_connection_db_path(con) -> str:
    rows = con.execute("PRAGMA database_list").fetchall()
    for row in rows:
        if len(row) >= 3 and str(row[1]) == "main" and str(row[2]):
            return str(row[2])
    raise ValueError("unable to resolve active DuckDB main database path from connection")


def _stable_unique_values(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
