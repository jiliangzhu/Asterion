from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from agents.common import build_agent_client_from_env, enqueue_agent_artifact_upserts
from agents.weather import (
    load_data_qa_agent_requests,
    load_resolution_agent_requests,
    load_rule2spec_agent_requests,
    run_data_qa_agent_review,
    run_resolution_agent_review,
    run_rule2spec_agent_review,
)
from asterion_core.contracts import (
    ForecastReplayRequest,
    ProposalStatus,
    ResolutionSpec,
    RouteAction,
    UMAProposal,
    stable_object_id,
    new_request_id,
)
from asterion_core.execution import (
    apply_fills_to_order,
    bind_trade_ticket_handoff,
    build_execution_context,
    build_execution_context_record,
    build_order_from_intent,
    build_paper_order,
    build_signal_order_intent_from_handoff,
    build_trade_ticket,
    canonical_order_router_hash,
    canonical_order_router_payload,
    canonical_order_handoff_payload,
    enqueue_execution_context_upserts,
    evaluate_execution_gate,
    fill_journal_payload,
    gate_rejection_journal_payload,
    load_account_trading_capability,
    load_market_capability,
    order_status_journal_payload,
    paper_order_journal_payload_with_status,
    route_trade_ticket,
    simulate_quote_based_fill,
    transition_order_to_posted,
)
from asterion_core.journal import (
    build_journal_event,
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
from asterion_core.risk import (
    available_inventory_quantity_for_ticket,
    apply_fill_to_inventory,
    apply_fill_to_reservation,
    apply_reservation_to_inventory,
    build_exposure_snapshot,
    build_reservation,
    build_reconciliation_result,
    finalize_reservation,
    classify_reconciliation_status,
    load_inventory_positions,
    load_reservation_for_order,
    reconciliation_journal_payload,
    release_reservation_to_inventory,
)
from asterion_core.contracts import OrderStatus
from asterion_core.runtime import (
    StrategyContext,
    StrategyRegistration,
    load_selected_watch_only_snapshots,
    run_strategy_engine,
)
from asterion_core.storage.write_queue import WriteQueueConfig
from domains.weather.forecast import (
    AdapterRouter,
    ForecastService,
    build_forecast_replay_diff_records,
    build_forecast_replay_record,
    build_forecast_run_record,
    enqueue_forecast_replay_diff_upserts,
    enqueue_forecast_replay_upserts,
    enqueue_forecast_run_upserts,
    load_original_pricing_outputs,
    load_replay_inputs,
    run_forecast_replay,
)
from domains.weather.forecast.service import ForecastCache
from domains.weather.pricing import enqueue_fair_value_upserts, enqueue_watch_only_snapshot_upserts, load_weather_market_spec
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
            market_specs=records,
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

    task_ids: list[str] = []
    task_ids.extend(_append_task_id(enqueue_settlement_verification_upserts(queue_cfg, verifications=verifications, run_id=request_id)))
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


def _stable_unique_values(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
