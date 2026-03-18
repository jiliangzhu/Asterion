from __future__ import annotations

import dataclasses
import json
import importlib.util
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import (
    AccountTradingCapability,
    BalanceType,
    Fill,
    GateDecision,
    InventoryPosition,
    MarketCapability,
    OrderStatus,
    ReconciliationStatus,
    RouteAction,
    WatchOnlySnapshotRecord,
)
from asterion_core.execution import (
    apply_fills_to_order,
    bind_trade_ticket_handoff,
    build_paper_order,
    build_execution_context,
    build_execution_context_record,
    build_order_from_intent,
    build_signal_order_intent,
    build_signal_order_intent_from_handoff,
    build_trade_ticket,
    canonical_order_router_hash,
    canonical_order_router_payload,
    canonical_order_handoff_hash,
    canonical_order_handoff_payload,
    enqueue_execution_context_upserts,
    evaluate_execution_gate,
    execution_context_record_to_row,
    fill_journal_payload,
    gate_rejection_journal_payload,
    hydrate_execution_context,
    load_execution_context_record,
    load_account_trading_capability,
    load_market_capability,
    load_trade_ticket,
    order_status_journal_payload,
    route_trade_ticket,
    route_trade_ticket_from_handoff,
    simulate_quote_based_fill,
    transition_order_to_posted,
    validate_order_transition,
)
from asterion_core.journal import (
    build_journal_event,
    enqueue_exposure_snapshot_upserts,
    enqueue_fill_upserts,
    enqueue_gate_decision_upserts,
    enqueue_inventory_position_upserts,
    enqueue_journal_event_upserts,
    enqueue_order_upserts,
    enqueue_order_state_transition_upserts,
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
    build_reconciliation_result,
    build_reservation,
    classify_reconciliation_status,
    finalize_reservation,
    reconciliation_journal_payload,
    release_reservation_to_inventory,
)
from asterion_core.runtime import StrategyContext, StrategyRegistration, load_watch_only_snapshots, run_strategy_engine
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from asterion_core.ui import build_ui_lite_db_once
from dagster_asterion.handlers import run_weather_paper_execution_job
from domains.weather.opportunity import build_weather_opportunity_assessment
from domains.weather.pricing.engine import build_watch_only_snapshot

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


def _watch_snapshot(
    *,
    snapshot_id: str,
    token_id: str,
    outcome: str,
    side: str,
    edge_bps: int,
    decision: str = "TAKE",
    signal_ts_ms: int = 0,
) -> WatchOnlySnapshotRecord:
    fair_value = 0.63 if side == "BUY" else 0.37
    reference_price = 0.55 if side == "BUY" else 0.45
    return WatchOnlySnapshotRecord(
        snapshot_id=snapshot_id,
        fair_value_id=f"fv_{snapshot_id}",
        run_id="frun_weather_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id=token_id,
        outcome=outcome,
        reference_price=reference_price,
        fair_value=fair_value,
        edge_bps=edge_bps,
        threshold_bps=500,
        decision=decision,
        side=side,
        rationale="unit_test",
        pricing_context={"signal_ts_ms": signal_ts_ms},
    )


def _market_capability() -> MarketCapability:
    return MarketCapability(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id="tok_yes",
        outcome="YES",
        tick_size=Decimal("0.01"),
        fee_rate_bps=30,
        neg_risk=False,
        min_order_size=Decimal("1"),
        tradable=True,
        fees_enabled=True,
        data_sources=["gamma", "clob_public"],
        updated_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
    )


def _account_capability() -> AccountTradingCapability:
    return AccountTradingCapability(
        wallet_id="wallet_weather_1",
        wallet_type="eoa",
        signature_type=1,
        funder="0xfunder",
        allowance_targets=["0xrelayer"],
        can_use_relayer=True,
        can_trade=True,
        restricted_reason=None,
    )


class StrategyEngineTest(unittest.TestCase):
    def test_strategy_engine_filters_hold_and_stably_orders_decisions(self) -> None:
        ctx = StrategyContext(
            data_snapshot_id="snap_weather_1",
            universe_snapshot_id="uni_weather_1",
            asof_ts_ms=1_710_000_000_000,
            dq_level="PASS",
            quote_snapshot_refs=[],
        )
        snapshots = [
            _watch_snapshot(snapshot_id="snap_hold", token_id="tok_hold", outcome="YES", side="HOLD", edge_bps=0, decision="NO_TRADE"),
            _watch_snapshot(snapshot_id="snap_late", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=800, signal_ts_ms=200),
            _watch_snapshot(snapshot_id="snap_early", token_id="tok_no", outcome="NO", side="SELL", edge_bps=-900, signal_ts_ms=100),
        ]
        strategies = [
            StrategyRegistration(
                strategy_id="weather_primary",
                strategy_version="v1",
                priority=2,
                route_action=RouteAction.FAK,
                size=Decimal("10"),
            ),
            StrategyRegistration(
                strategy_id="weather_fast",
                strategy_version="v1",
                priority=1,
                route_action=RouteAction.FOK,
                size=Decimal("5"),
            ),
        ]
        run_a, decisions_a = run_strategy_engine(ctx=ctx, snapshots=snapshots, strategies=strategies)
        run_b, decisions_b = run_strategy_engine(ctx=ctx, snapshots=snapshots, strategies=strategies)

        self.assertEqual(run_a.run_id, run_b.run_id)
        self.assertEqual([item.decision_id for item in decisions_a], [item.decision_id for item in decisions_b])
        self.assertEqual(len(decisions_a), 4)
        self.assertEqual([item.strategy_id for item in decisions_a[:2]], ["weather_fast", "weather_fast"])
        self.assertEqual([item.watch_snapshot_id for item in decisions_a[:2]], ["snap_early", "snap_late"])
        self.assertTrue(all(item.watch_snapshot_id != "snap_hold" for item in decisions_a))

    def test_strategy_engine_orders_side_aware_sell_snapshot_by_absolute_executable_edge(self) -> None:
        ctx = StrategyContext(
            data_snapshot_id="snap_weather_1",
            universe_snapshot_id="uni_weather_1",
            asof_ts_ms=1_710_000_000_000,
            dq_level="PASS",
            quote_snapshot_refs=[],
        )
        sell_assessment = build_weather_opportunity_assessment(
            market_id="mkt_weather_sell",
            token_id="tok_sell",
            outcome="NO",
            reference_price=0.70,
            model_fair_value=0.50,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=82.0,
        )
        buy_assessment = build_weather_opportunity_assessment(
            market_id="mkt_weather_buy",
            token_id="tok_buy",
            outcome="YES",
            reference_price=0.40,
            model_fair_value=0.48,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            confidence_score=82.0,
        )
        sell_snapshot = dataclasses.replace(
            build_watch_only_snapshot(
                assessment=sell_assessment,
                reference_price=0.70,
                threshold_bps=500,
                pricing_context={"run_id": "frun_sell", "condition_id": "cond_sell"},
            ),
            snapshot_id="snap_sell",
            fair_value_id="fval_sell",
            run_id="frun_sell",
            market_id="mkt_weather_sell",
            condition_id="cond_sell",
        )
        buy_snapshot = dataclasses.replace(
            build_watch_only_snapshot(
                assessment=buy_assessment,
                reference_price=0.40,
                threshold_bps=500,
                pricing_context={"run_id": "frun_buy", "condition_id": "cond_buy"},
            ),
            snapshot_id="snap_buy",
            fair_value_id="fval_buy",
            run_id="frun_buy",
            market_id="mkt_weather_buy",
            condition_id="cond_buy",
        )
        self.assertEqual(sell_snapshot.side, "SELL")
        self.assertLess(sell_snapshot.edge_bps, 0)
        self.assertGreater(abs(sell_snapshot.edge_bps), abs(buy_snapshot.edge_bps))

    def test_strategy_engine_prefers_higher_ranking_score_over_absolute_edge(self) -> None:
        ctx = StrategyContext(
            data_snapshot_id="snap_weather_1",
            universe_snapshot_id="uni_weather_1",
            asof_ts_ms=1_710_000_000_000,
            dq_level="PASS",
            quote_snapshot_refs=[],
        )
        high_edge_low_rank = dataclasses.replace(
            _watch_snapshot(
                snapshot_id="snap_high_edge",
                token_id="tok_high_edge",
                outcome="YES",
                side="BUY",
                edge_bps=1200,
                signal_ts_ms=200,
            ),
            pricing_context={"signal_ts_ms": 200, "ranking_score": 0.18},
        )
        lower_edge_high_rank = dataclasses.replace(
            _watch_snapshot(
                snapshot_id="snap_high_rank",
                token_id="tok_high_rank",
                outcome="YES",
                side="BUY",
                edge_bps=900,
                signal_ts_ms=100,
            ),
            pricing_context={"signal_ts_ms": 100, "ranking_score": 0.26},
        )
        strategy = StrategyRegistration(
            strategy_id="weather_primary",
            strategy_version="v1",
            priority=1,
            route_action=RouteAction.FAK,
            size=Decimal("10"),
        )

        _, decisions = run_strategy_engine(
            ctx=ctx,
            snapshots=[high_edge_low_rank, lower_edge_high_rank],
            strategies=[strategy],
        )

        self.assertEqual(decisions[0].watch_snapshot_id, "snap_high_rank")

    def test_strategy_engine_prefers_penalty_aware_ranking_score_over_raw_edge(self) -> None:
        ctx = StrategyContext(
            data_snapshot_id="snap_weather_1",
            universe_snapshot_id="uni_weather_1",
            asof_ts_ms=1_710_000_000_000,
            dq_level="PASS",
            quote_snapshot_refs=[],
        )
        higher_raw_edge = _watch_snapshot(
            snapshot_id="snap_high_edge",
            token_id="tok_high",
            outcome="YES",
            side="BUY",
            edge_bps=1500,
            signal_ts_ms=100,
        )
        lower_raw_edge_higher_rank = _watch_snapshot(
            snapshot_id="snap_high_rank",
            token_id="tok_rank",
            outcome="NO",
            side="SELL",
            edge_bps=-900,
            signal_ts_ms=200,
        )
        higher_raw_edge.pricing_context["ranking_score"] = 55.0
        lower_raw_edge_higher_rank.pricing_context["ranking_score"] = 90.0

        _, decisions = run_strategy_engine(
            ctx=ctx,
            snapshots=[higher_raw_edge, lower_raw_edge_higher_rank],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.FAK,
                    size=Decimal("10"),
                )
            ],
        )
        self.assertEqual([item.watch_snapshot_id for item in decisions], ["snap_high_rank", "snap_high_edge"])


class TicketAndOrderHandoffTest(unittest.TestCase):
    def test_trade_ticket_hash_is_stable_and_provenance_is_closed(self) -> None:
        _, decisions = run_strategy_engine(
            ctx=StrategyContext(
                data_snapshot_id="snap_weather_1",
                universe_snapshot_id=None,
                asof_ts_ms=1_710_000_000_000,
                dq_level="PASS",
                quote_snapshot_refs=[],
            ),
            snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.FAK,
                    size=Decimal("10"),
                )
            ],
        )
        ticket_a = build_trade_ticket(decisions[0], created_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC))
        ticket_b = build_trade_ticket(decisions[0], created_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC))

        self.assertEqual(ticket_a.ticket_hash, ticket_b.ticket_hash)
        self.assertEqual(ticket_a.request_id, ticket_b.request_id)
        self.assertEqual(ticket_a.provenance_json["forecast_run_id"], "frun_weather_1")
        self.assertEqual(ticket_a.provenance_json["watch_snapshot_id"], "snap_yes")
        self.assertEqual(ticket_a.provenance_json["strategy_id"], "weather_primary")
        self.assertEqual(ticket_a.provenance_json["pricing_context"]["signal_ts_ms"], 0)

        bound_ticket = bind_trade_ticket_handoff(
            ticket_a,
            wallet_id="wallet_weather_1",
            execution_context_id="ectx_1",
        )
        self.assertEqual(bound_ticket.ticket_hash, ticket_a.ticket_hash)
        self.assertEqual(bound_ticket.request_id, ticket_a.request_id)
        self.assertEqual(bound_ticket.wallet_id, "wallet_weather_1")
        self.assertEqual(bound_ticket.execution_context_id, "ectx_1")

    def test_signal_to_order_builds_canonical_order_and_execution_context(self) -> None:
        _, decisions = run_strategy_engine(
            ctx=StrategyContext(
                data_snapshot_id="snap_weather_1",
                universe_snapshot_id=None,
                asof_ts_ms=1_710_000_000_000,
                dq_level="PASS",
                quote_snapshot_refs=[],
            ),
            snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.FAK,
                    size=Decimal("10"),
                )
            ],
        )
        ticket = build_trade_ticket(decisions[0])
        intent = build_signal_order_intent(
            ticket,
            market_capability=_market_capability(),
            account_capability=_account_capability(),
        )

        self.assertEqual(intent.canonical_order.token_id, "tok_yes")
        self.assertEqual(intent.canonical_order.side, "buy")
        self.assertEqual(intent.canonical_order.fee_rate_bps, 30)
        self.assertEqual(intent.execution_context.tick_size, Decimal("0.01"))
        self.assertEqual(intent.execution_context.signature_type, 1)
        self.assertEqual(intent.execution_context.funder, "0xfunder")

    def test_execution_context_record_is_stable_and_sensitive_to_capability_changes(self) -> None:
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        record_a = build_execution_context_record(
            wallet_id="wallet_weather_1",
            execution_context=execution_context,
            created_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
        )
        record_b = build_execution_context_record(
            wallet_id="wallet_weather_1",
            execution_context=execution_context,
            created_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
        )
        changed_context = build_execution_context(
            market_capability=MarketCapability(
                market_id="mkt_weather_1",
                condition_id="cond_weather_1",
                token_id="tok_yes",
                outcome="YES",
                tick_size=Decimal("0.01"),
                fee_rate_bps=35,
                neg_risk=False,
                min_order_size=Decimal("1"),
                tradable=True,
                fees_enabled=True,
                data_sources=["gamma", "clob_public"],
                updated_at=datetime(2026, 3, 10, 10, 5, tzinfo=UTC),
            ),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        record_c = build_execution_context_record(
            wallet_id="wallet_weather_1",
            execution_context=changed_context,
            created_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
        )

        self.assertEqual(record_a.execution_context_id, record_b.execution_context_id)
        self.assertNotEqual(record_a.execution_context_id, record_c.execution_context_id)
        self.assertEqual(execution_context_record_to_row(record_a)[1], "wallet_weather_1")

    def test_signal_order_intent_from_handoff_is_rebuildable_and_blocks_gtd(self) -> None:
        _, decisions = run_strategy_engine(
            ctx=StrategyContext(
                data_snapshot_id="snap_weather_1",
                universe_snapshot_id=None,
                asof_ts_ms=1_710_000_000_000,
                dq_level="PASS",
                quote_snapshot_refs=[],
            ),
            snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.FAK,
                    size=Decimal("10"),
                )
            ],
        )
        ticket = bind_trade_ticket_handoff(
            build_trade_ticket(decisions[0]),
            wallet_id="wallet_weather_1",
            execution_context_id="ectx_1",
        )
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        intent = build_signal_order_intent_from_handoff(ticket, execution_context=execution_context)
        payload = canonical_order_handoff_payload(intent)
        routed = route_trade_ticket(ticket, execution_context)

        self.assertEqual(intent.request_id, ticket.request_id)
        self.assertEqual(payload["route_action"], "fak")
        self.assertEqual(payload["time_in_force"], "fak")
        self.assertEqual(payload["post_only"], False)
        self.assertEqual(canonical_order_handoff_hash(intent), canonical_order_router_hash(routed))

        _, gtd_decisions = run_strategy_engine(
            ctx=StrategyContext(
                data_snapshot_id="snap_weather_1",
                universe_snapshot_id=None,
                asof_ts_ms=1_710_000_000_000,
                dq_level="PASS",
                quote_snapshot_refs=[],
            ),
            snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.POST_ONLY_GTD,
                    size=Decimal("10"),
                )
            ],
        )
        gtd_ticket = bind_trade_ticket_handoff(
            build_trade_ticket(gtd_decisions[0]),
            wallet_id="wallet_weather_1",
            execution_context_id="ectx_gtd",
        )
        gtd_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.POST_ONLY_GTD,
        )
        with self.assertRaisesRegex(ValueError, "POST_ONLY_GTD remains blocked in P3-03"):
            build_signal_order_intent_from_handoff(gtd_ticket, execution_context=gtd_context)

    def test_route_trade_ticket_normalizes_supported_actions(self) -> None:
        market_capability = _market_capability()
        account_capability = _account_capability()
        cases = [
            (RouteAction.FAK, "fak", False),
            (RouteAction.FOK, "fok", False),
            (RouteAction.POST_ONLY_GTC, "gtc", True),
        ]
        for route_action, expected_tif, expected_post_only in cases:
            with self.subTest(route_action=route_action.value):
                _, decisions = run_strategy_engine(
                    ctx=StrategyContext(
                        data_snapshot_id="snap_weather_1",
                        universe_snapshot_id=None,
                        asof_ts_ms=1_710_000_000_000,
                        dq_level="PASS",
                        quote_snapshot_refs=[],
                    ),
                    snapshots=[_watch_snapshot(snapshot_id=f"snap_{route_action.value}", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
                    strategies=[
                        StrategyRegistration(
                            strategy_id="weather_primary",
                            strategy_version="v1",
                            priority=1,
                            route_action=route_action,
                            size=Decimal("10"),
                        )
                    ],
                )
                ticket = bind_trade_ticket_handoff(
                    build_trade_ticket(decisions[0]),
                    wallet_id="wallet_weather_1",
                    execution_context_id=f"ectx_{route_action.value}",
                )
                execution_context = build_execution_context(
                    market_capability=market_capability,
                    account_capability=account_capability,
                    route_action=route_action,
                )
                routed = route_trade_ticket(ticket, execution_context)
                payload = canonical_order_router_payload(routed)
                self.assertEqual(payload["time_in_force"], expected_tif)
                self.assertEqual(payload["post_only"], expected_post_only)
                self.assertIsNone(routed.expiration)

    def test_route_trade_ticket_validates_tick_size_min_size_and_context_match(self) -> None:
        _, decisions = run_strategy_engine(
            ctx=StrategyContext(
                data_snapshot_id="snap_weather_1",
                universe_snapshot_id=None,
                asof_ts_ms=1_710_000_000_000,
                dq_level="PASS",
                quote_snapshot_refs=[],
            ),
            snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.FAK,
                    size=Decimal("10"),
                )
            ],
        )
        base_ticket = bind_trade_ticket_handoff(
            build_trade_ticket(decisions[0]),
            wallet_id="wallet_weather_1",
            execution_context_id="ectx_1",
        )
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        bad_price_ticket = dataclasses.replace(base_ticket, reference_price=Decimal("0.555"))
        with self.assertRaisesRegex(ValueError, "tick_size"):
            route_trade_ticket(bad_price_ticket, execution_context)
        bad_size_ticket = dataclasses.replace(base_ticket, size=Decimal("0.5"))
        with self.assertRaisesRegex(ValueError, "min_order_size"):
            route_trade_ticket(bad_size_ticket, execution_context)
        mismatched_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FOK,
        )
        with self.assertRaisesRegex(ValueError, "route_action"):
            route_trade_ticket(base_ticket, mismatched_context)

    def test_paper_adapter_builds_created_order_and_oms_posts_it(self) -> None:
        _, decisions = run_strategy_engine(
            ctx=StrategyContext(
                data_snapshot_id="snap_weather_1",
                universe_snapshot_id=None,
                asof_ts_ms=1_710_000_000_000,
                dq_level="PASS",
                quote_snapshot_refs=[],
            ),
            snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.FAK,
                    size=Decimal("10"),
                )
            ],
        )
        ticket = bind_trade_ticket_handoff(
            build_trade_ticket(decisions[0]),
            wallet_id="wallet_weather_1",
            execution_context_id="ectx_1",
        )
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        intent = build_signal_order_intent_from_handoff(ticket, execution_context=execution_context)
        gate = GateDecision(
            gate_id="gate_1",
            ticket_id=ticket.ticket_id,
            allowed=True,
            reason="allowed",
            reason_codes=[],
            metrics_json={},
            created_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
        )
        order = build_paper_order(
            intent=intent,
            wallet_id="wallet_weather_1",
            gate_decision=gate,
            created_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
        )
        posted_order, transition = transition_order_to_posted(
            order,
            timestamp=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
        )
        rejection_payload = gate_rejection_journal_payload(
            ticket_id=ticket.ticket_id,
            request_id=ticket.request_id,
            wallet_id="wallet_weather_1",
            gate_decision=GateDecision(
                gate_id="gate_reject",
                ticket_id=ticket.ticket_id,
                allowed=False,
                reason="market_not_tradable",
                reason_codes=["market_not_tradable"],
                metrics_json={"market_gate": "fail"},
                created_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
            ),
        )

        self.assertEqual(order.status, OrderStatus.CREATED)
        self.assertEqual(order.remaining_size, Decimal("10"))
        self.assertEqual(posted_order.status, OrderStatus.POSTED)
        self.assertEqual(transition.from_status, OrderStatus.CREATED)
        self.assertEqual(transition.to_status, OrderStatus.POSTED)
        self.assertEqual(rejection_payload["reason"], "market_not_tradable")

    def test_quote_based_fill_simulator_and_oms_cover_full_partial_cancel_and_resting_paths(self) -> None:
        market_capability = _market_capability()
        account_capability = _account_capability()

        def _build_posted_order(route_action: RouteAction, edge_bps: int) -> tuple:
            _, decisions = run_strategy_engine(
                ctx=StrategyContext(
                    data_snapshot_id="snap_weather_1",
                    universe_snapshot_id=None,
                    asof_ts_ms=1_710_000_000_000,
                    dq_level="PASS",
                    quote_snapshot_refs=[],
                ),
                snapshots=[_watch_snapshot(snapshot_id=f"snap_{route_action.value}_{edge_bps}", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=edge_bps)],
                strategies=[
                    StrategyRegistration(
                        strategy_id="weather_primary",
                        strategy_version="v1",
                        priority=1,
                        route_action=route_action,
                        size=Decimal("10"),
                    )
                ],
            )
            ticket = bind_trade_ticket_handoff(
                build_trade_ticket(decisions[0]),
                wallet_id="wallet_weather_1",
                execution_context_id=f"ectx_{route_action.value}_{edge_bps}",
            )
            execution_context = build_execution_context(
                market_capability=market_capability,
                account_capability=account_capability,
                route_action=route_action,
            )
            intent = build_signal_order_intent_from_handoff(ticket, execution_context=execution_context)
            order = build_paper_order(
                intent=intent,
                wallet_id="wallet_weather_1",
                gate_decision=GateDecision(
                    gate_id=f"gate_{route_action.value}_{edge_bps}",
                    ticket_id=ticket.ticket_id,
                    allowed=True,
                    reason="allowed",
                    reason_codes=[],
                    metrics_json={},
                    created_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
                ),
                created_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
            )
            posted_order, _ = transition_order_to_posted(
                order,
                timestamp=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
            )
            return ticket, posted_order

        full_ticket, full_order = _build_posted_order(RouteAction.FAK, 800)
        full_result = simulate_quote_based_fill(
            order=full_order,
            ticket=full_ticket,
            observed_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        self.assertEqual(len(full_result.fills), 1)
        full_order_after, full_transition = apply_fills_to_order(
            full_order,
            fills=full_result.fills,
            timestamp=full_result.observed_at,
        )
        self.assertEqual(full_order_after.status, OrderStatus.FILLED)
        self.assertEqual(full_order_after.remaining_size, Decimal("0"))
        self.assertIsNotNone(full_transition)
        self.assertEqual(full_transition.to_status, OrderStatus.FILLED)

        partial_ticket, partial_order = _build_posted_order(RouteAction.FAK, 600)
        partial_result = simulate_quote_based_fill(
            order=partial_order,
            ticket=partial_ticket,
            observed_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        self.assertEqual(partial_result.fills[0].size, Decimal("5.00000000"))
        partial_order_after, partial_transition = apply_fills_to_order(
            partial_order,
            fills=partial_result.fills,
            timestamp=partial_result.observed_at,
        )
        self.assertEqual(partial_order_after.status, OrderStatus.PARTIAL_FILLED)
        self.assertEqual(partial_transition.to_status, OrderStatus.PARTIAL_FILLED)
        self.assertEqual(
            order_status_journal_payload(
                order=partial_order_after,
                ticket_id=partial_ticket.ticket_id,
                request_id=partial_ticket.request_id,
                reason=partial_result.outcome_reason,
            )["status"],
            "partial_filled",
        )

        fok_ticket, fok_order = _build_posted_order(RouteAction.FOK, 600)
        fok_result = simulate_quote_based_fill(
            order=fok_order,
            ticket=fok_ticket,
            observed_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        self.assertEqual(fok_result.fills, [])
        fok_order_after, fok_transition = apply_fills_to_order(
            fok_order,
            fills=fok_result.fills,
            timestamp=fok_result.observed_at,
        )
        self.assertEqual(fok_order_after.status, OrderStatus.CANCELLED)
        self.assertIsNotNone(fok_transition)
        self.assertEqual(fok_transition.to_status, OrderStatus.CANCELLED)

        resting_ticket, resting_order = _build_posted_order(RouteAction.POST_ONLY_GTC, 900)
        resting_result = simulate_quote_based_fill(
            order=resting_order,
            ticket=resting_ticket,
            observed_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        self.assertEqual(resting_result.fills, [])
        resting_order_after, resting_transition = apply_fills_to_order(
            resting_order,
            fills=resting_result.fills,
            timestamp=resting_result.observed_at,
        )
        self.assertEqual(resting_order_after.status, OrderStatus.POSTED)
        self.assertIsNone(resting_transition)
        self.assertIn("fill_id", fill_journal_payload(fill=full_result.fills[0], ticket_id=full_ticket.ticket_id, request_id=full_ticket.request_id))

    def test_oms_state_machine_rejects_invalid_transitions(self) -> None:
        validate_order_transition(OrderStatus.CREATED, OrderStatus.POSTED)
        with self.assertRaisesRegex(ValueError, "invalid OMS transition"):
            validate_order_transition(OrderStatus.CREATED, OrderStatus.FILLED)


class ExecutionGateAndPortfolioTest(unittest.TestCase):
    def test_execution_gate_rejects_watch_only_and_inventory_failures(self) -> None:
        _, decisions = run_strategy_engine(
            ctx=StrategyContext(
                data_snapshot_id="snap_weather_1",
                universe_snapshot_id=None,
                asof_ts_ms=1_710_000_000_000,
                dq_level="PASS",
                quote_snapshot_refs=[],
            ),
            snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=200)],
            strategies=[
                StrategyRegistration(
                    strategy_id="weather_primary",
                    strategy_version="v1",
                    priority=1,
                    route_action=RouteAction.FAK,
                    size=Decimal("10"),
                )
            ],
        )
        ticket = build_trade_ticket(decisions[0])
        intent = build_signal_order_intent(
            ticket,
            market_capability=_market_capability(),
            account_capability=_account_capability(),
        )
        decision = evaluate_execution_gate(
            ticket=ticket,
            intent=intent,
            watch_only_active=True,
            degrade_active=False,
            available_quantity=Decimal("1"),
        )
        self.assertFalse(decision.allowed)
        self.assertIn("watch_only_active", decision.reason_codes)
        self.assertIn("insufficient_inventory", decision.reason_codes)
        self.assertIn("economic_edge_below_threshold", decision.reason_codes)

    def test_portfolio_buy_and_sell_reservations_follow_inventory_semantics(self) -> None:
        buy_intent = build_signal_order_intent(
            build_trade_ticket(
                run_strategy_engine(
                    ctx=StrategyContext(
                        data_snapshot_id="snap_weather_1",
                        universe_snapshot_id=None,
                        asof_ts_ms=1_710_000_000_000,
                        dq_level="PASS",
                        quote_snapshot_refs=[],
                    ),
                    snapshots=[_watch_snapshot(snapshot_id="snap_yes", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
                    strategies=[
                        StrategyRegistration(
                            strategy_id="weather_primary",
                            strategy_version="v1",
                            priority=1,
                            route_action=RouteAction.FAK,
                            size=Decimal("10"),
                        )
                    ],
                )[1][0]
            ),
            market_capability=_market_capability(),
            account_capability=_account_capability(),
        )
        buy_order = build_order_from_intent(buy_intent, wallet_id="wallet_weather_1")
        buy_reservation = build_reservation(buy_order)
        self.assertEqual(buy_reservation.asset_type, "usdc_e")
        self.assertEqual(buy_reservation.reserved_quantity, Decimal("5.50"))

        positions = [
            InventoryPosition(
                wallet_id="wallet_weather_1",
                asset_type="usdc_e",
                token_id=None,
                market_id=None,
                outcome=None,
                balance_type=BalanceType.AVAILABLE,
                quantity=Decimal("100"),
                funder="0xfunder",
                signature_type=1,
                updated_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
            )
        ]
        updated_positions = apply_reservation_to_inventory(positions, buy_reservation)
        available = next(item for item in updated_positions if item.balance_type is BalanceType.AVAILABLE)
        reserved = next(item for item in updated_positions if item.balance_type is BalanceType.RESERVED)
        self.assertEqual(available.quantity, Decimal("94.50"))
        self.assertEqual(reserved.quantity, Decimal("5.50"))

        fill = Fill(
            fill_id="fill_buy_1",
            order_id=buy_order.order_id,
            wallet_id=buy_order.wallet_id,
            market_id=buy_order.market_id,
            token_id=buy_order.token_id,
            outcome=buy_order.outcome,
            side=buy_order.side,
            price=Decimal("0.55"),
            size=Decimal("10"),
            fee=Decimal("0.10"),
            fee_rate_bps=30,
            trade_id="trade_buy_1",
            exchange_order_id="ex_1",
            filled_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
        )
        converted = apply_fill_to_reservation(buy_reservation, fill)
        self.assertEqual(converted.remaining_quantity, Decimal("0"))
        self.assertEqual(str(converted.status), "ReservationStatus.CONVERTED")

        sell_ticket = build_trade_ticket(
            run_strategy_engine(
                ctx=StrategyContext(
                    data_snapshot_id="snap_weather_1",
                    universe_snapshot_id=None,
                    asof_ts_ms=1_710_000_000_000,
                    dq_level="PASS",
                    quote_snapshot_refs=[],
                ),
                snapshots=[_watch_snapshot(snapshot_id="snap_no", token_id="tok_no", outcome="NO", side="SELL", edge_bps=-900)],
                strategies=[
                    StrategyRegistration(
                        strategy_id="weather_primary",
                        strategy_version="v1",
                        priority=1,
                        route_action=RouteAction.FOK,
                        size=Decimal("7"),
                    )
                ],
            )[1][0]
        )
        sell_intent = build_signal_order_intent(
            sell_ticket,
            market_capability=MarketCapability(
                market_id="mkt_weather_1",
                condition_id="cond_weather_1",
                token_id="tok_no",
                outcome="NO",
                tick_size=Decimal("0.01"),
                fee_rate_bps=30,
                neg_risk=False,
                min_order_size=Decimal("1"),
                tradable=True,
                fees_enabled=True,
                data_sources=["gamma"],
                updated_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
            ),
            account_capability=_account_capability(),
        )
        sell_order = build_order_from_intent(sell_intent, wallet_id="wallet_weather_1")
        sell_reservation = build_reservation(sell_order)
        released = finalize_reservation(sell_reservation, order_status=OrderStatus.CANCELLED)
        self.assertEqual(sell_reservation.asset_type, "outcome_token")
        self.assertEqual(sell_reservation.token_id, "tok_no")
        self.assertEqual(released.remaining_quantity, Decimal("0"))
        self.assertEqual(str(released.status), "ReservationStatus.RELEASED")

    def test_inventory_helpers_cover_buy_sell_fill_and_release_paths(self) -> None:
        buy_ticket = bind_trade_ticket_handoff(
            build_trade_ticket(
                run_strategy_engine(
                    ctx=StrategyContext(
                        data_snapshot_id="snap_weather_1",
                        universe_snapshot_id=None,
                        asof_ts_ms=1_710_000_000_000,
                        dq_level="PASS",
                        quote_snapshot_refs=[],
                    ),
                    snapshots=[_watch_snapshot(snapshot_id="snap_yes_fill", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
                    strategies=[
                        StrategyRegistration(
                            strategy_id="weather_primary",
                            strategy_version="v1",
                            priority=1,
                            route_action=RouteAction.FAK,
                            size=Decimal("10"),
                        )
                    ],
                )[1][0]
            ),
            wallet_id="wallet_weather_1",
            execution_context_id="ectx_buy_fill",
        )
        buy_intent = build_signal_order_intent(
            buy_ticket,
            market_capability=_market_capability(),
            account_capability=_account_capability(),
        )
        buy_order = build_order_from_intent(buy_intent, wallet_id="wallet_weather_1")
        buy_reservation = build_reservation(buy_order)
        buy_positions = [
            InventoryPosition(
                wallet_id="wallet_weather_1",
                asset_type="usdc_e",
                token_id=None,
                market_id=None,
                outcome=None,
                balance_type=BalanceType.AVAILABLE,
                quantity=Decimal("100"),
                funder="0xfunder",
                signature_type=1,
                updated_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
            )
        ]
        self.assertEqual(
            available_inventory_quantity_for_ticket(buy_positions, ticket=buy_ticket),
            Decimal("100"),
        )
        buy_reserved_positions = apply_reservation_to_inventory(
            buy_positions,
            buy_reservation,
            observed_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
        )
        buy_fill = Fill(
            fill_id="fill_buy_inventory",
            order_id=buy_order.order_id,
            wallet_id=buy_order.wallet_id,
            market_id=buy_order.market_id,
            token_id=buy_order.token_id,
            outcome=buy_order.outcome,
            side=buy_order.side,
            price=Decimal("0.55"),
            size=Decimal("10"),
            fee=Decimal("0.10"),
            fee_rate_bps=30,
            trade_id="trade_buy_inventory",
            exchange_order_id="ex_buy_inventory",
            filled_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        buy_after_fill = apply_fill_to_inventory(
            buy_reserved_positions,
            order=buy_order,
            reservation=buy_reservation,
            fill=buy_fill,
            observed_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        buy_reserved_cash = next(item for item in buy_after_fill if item.asset_type == "usdc_e" and item.balance_type is BalanceType.RESERVED)
        buy_settled_token = next(item for item in buy_after_fill if item.asset_type == "outcome_token" and item.balance_type is BalanceType.SETTLED)
        self.assertEqual(buy_reserved_cash.quantity, Decimal("0.00"))
        self.assertEqual(buy_settled_token.quantity, Decimal("10"))

        sell_ticket = bind_trade_ticket_handoff(
            build_trade_ticket(
                run_strategy_engine(
                    ctx=StrategyContext(
                        data_snapshot_id="snap_weather_1",
                        universe_snapshot_id=None,
                        asof_ts_ms=1_710_000_000_000,
                        dq_level="PASS",
                        quote_snapshot_refs=[],
                    ),
                    snapshots=[_watch_snapshot(snapshot_id="snap_no_inventory", token_id="tok_no", outcome="NO", side="SELL", edge_bps=-900)],
                    strategies=[
                        StrategyRegistration(
                            strategy_id="weather_primary",
                            strategy_version="v1",
                            priority=1,
                            route_action=RouteAction.FOK,
                            size=Decimal("7"),
                        )
                    ],
                )[1][0]
            ),
            wallet_id="wallet_weather_1",
            execution_context_id="ectx_sell_inventory",
        )
        sell_intent = build_signal_order_intent(
            sell_ticket,
            market_capability=MarketCapability(
                market_id="mkt_weather_1",
                condition_id="cond_weather_1",
                token_id="tok_no",
                outcome="NO",
                tick_size=Decimal("0.01"),
                fee_rate_bps=30,
                neg_risk=False,
                min_order_size=Decimal("1"),
                tradable=True,
                fees_enabled=True,
                data_sources=["gamma"],
                updated_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
            ),
            account_capability=_account_capability(),
        )
        sell_order = build_order_from_intent(sell_intent, wallet_id="wallet_weather_1")
        sell_reservation = build_reservation(sell_order)
        sell_positions = [
            InventoryPosition(
                wallet_id="wallet_weather_1",
                asset_type="outcome_token",
                token_id="tok_no",
                market_id="mkt_weather_1",
                outcome="NO",
                balance_type=BalanceType.AVAILABLE,
                quantity=Decimal("7"),
                funder="0xfunder",
                signature_type=1,
                updated_at=datetime(2026, 3, 10, 10, 0, tzinfo=UTC),
            )
        ]
        sell_reserved_positions = apply_reservation_to_inventory(
            sell_positions,
            sell_reservation,
            observed_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
        )
        released_positions = release_reservation_to_inventory(
            sell_reserved_positions,
            sell_reservation,
            observed_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        sell_available = next(item for item in released_positions if item.asset_type == "outcome_token" and item.balance_type is BalanceType.AVAILABLE)
        sell_reserved = next(item for item in released_positions if item.asset_type == "outcome_token" and item.balance_type is BalanceType.RESERVED)
        self.assertEqual(sell_available.quantity, Decimal("7"))
        self.assertEqual(sell_reserved.quantity, Decimal("0"))

    def test_reconciliation_builder_classifies_ok_and_inventory_mismatch(self) -> None:
        order = build_order_from_intent(
            build_signal_order_intent(
                bind_trade_ticket_handoff(
                    build_trade_ticket(
                        run_strategy_engine(
                            ctx=StrategyContext(
                                data_snapshot_id="snap_weather_1",
                                universe_snapshot_id=None,
                                asof_ts_ms=1_710_000_000_000,
                                dq_level="PASS",
                                quote_snapshot_refs=[],
                            ),
                            snapshots=[_watch_snapshot(snapshot_id="snap_recon", token_id="tok_yes", outcome="YES", side="BUY", edge_bps=900)],
                            strategies=[
                                StrategyRegistration(
                                    strategy_id="weather_primary",
                                    strategy_version="v1",
                                    priority=1,
                                    route_action=RouteAction.FAK,
                                    size=Decimal("10"),
                                )
                            ],
                        )[1][0]
                    ),
                    wallet_id="wallet_weather_1",
                    execution_context_id="ectx_recon",
                ),
                market_capability=_market_capability(),
                account_capability=_account_capability(),
            ),
            wallet_id="wallet_weather_1",
        )
        fill = Fill(
            fill_id="fill_recon_1",
            order_id=order.order_id,
            wallet_id=order.wallet_id,
            market_id=order.market_id,
            token_id=order.token_id,
            outcome=order.outcome,
            side=order.side,
            price=Decimal("0.55"),
            size=Decimal("10"),
            fee=Decimal("0.10"),
            fee_rate_bps=30,
            trade_id="trade_recon_1",
            exchange_order_id="ex_recon_1",
            filled_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        filled_order = dataclasses.replace(
            order,
            status=OrderStatus.FILLED,
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
            avg_fill_price=Decimal("0.55"),
            exchange_order_id="ex_recon_1",
        )
        reservation = finalize_reservation(
            apply_fill_to_reservation(build_reservation(order), fill),
            order_status=OrderStatus.FILLED,
            observed_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        positions = [
            InventoryPosition(
                wallet_id="wallet_weather_1",
                asset_type="outcome_token",
                token_id="tok_yes",
                market_id="mkt_weather_1",
                outcome="YES",
                balance_type=BalanceType.SETTLED,
                quantity=Decimal("10"),
                funder="0xfunder",
                signature_type=1,
                updated_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
            )
        ]
        exposure = build_exposure_snapshot(
            filled_order,
            positions=positions,
            reservation=reservation,
            captured_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        ok_result = build_reconciliation_result(
            order=filled_order,
            reservation=reservation,
            fills=[fill],
            positions=positions,
            exposure_snapshot=exposure,
            created_at=datetime(2026, 3, 10, 10, 2, tzinfo=UTC),
        )
        self.assertEqual(ok_result.status, ReconciliationStatus.OK)
        self.assertEqual(
            reconciliation_journal_payload(
                result=ok_result,
                order=filled_order,
                ticket_id="tt_recon",
                request_id="req_recon",
            )["status"],
            "ok",
        )

        bad_positions = [dataclasses.replace(positions[0], quantity=Decimal("9"))]
        bad_status = classify_reconciliation_status(
            order=filled_order,
            reservation=reservation,
            fills=[fill],
            positions=bad_positions,
            exposure_snapshot=exposure,
            local_quantity=Decimal("9"),
            remote_quantity=Decimal("10"),
        )
        self.assertEqual(bad_status, ReconciliationStatus.INVENTORY_MISMATCH)

    def test_journal_event_ids_are_stable(self) -> None:
        event_a = build_journal_event(
            event_type="trade_ticket.created",
            entity_type="trade_ticket",
            entity_id="tt_1",
            run_id="srun_1",
            payload_json={"ticket_hash": "abc"},
        )
        event_b = build_journal_event(
            event_type="trade_ticket.created",
            entity_type="trade_ticket",
            entity_id="tt_1",
            run_id="srun_1",
            payload_json={"ticket_hash": "abc"},
        )
        self.assertEqual(event_a.event_id, event_b.event_id)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for execution foundation integration tests")
class ExecutionFoundationDuckDBTest(unittest.TestCase):
    def test_weather_paper_execution_job_persists_strategy_ticket_and_context_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                        """
                        INSERT INTO capability.market_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "tok_yes",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "YES",
                            Decimal("0.01"),
                            30,
                            False,
                            Decimal("1"),
                            True,
                            True,
                            "[\"gamma\",\"clob_public\"]",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "wallet_weather_1",
                            "eoa",
                            1,
                            "0xfunder",
                            "[\"0xrelayer\"]",
                            True,
                            True,
                            None,
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "snap_yes",
                            "fv_yes",
                            "frun_weather_1",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "tok_yes",
                            "YES",
                            0.55,
                            0.63,
                            800,
                            500,
                            "TAKE",
                            "BUY",
                            "paper chain",
                            "{\"signal_ts_ms\":1710000000000}",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "snap_hold",
                            "fv_hold",
                            "frun_weather_1",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "tok_yes",
                            "YES",
                            0.55,
                            0.56,
                            50,
                            500,
                            "NO_TRADE",
                            "BUY",
                            "hold path",
                            "{\"signal_ts_ms\":1710000000100}",
                            datetime(2026, 3, 10, 10, 5),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "wallet_weather_1",
                            "usdc_e",
                            "usdc_e",
                            "cash",
                            "cash",
                            "available",
                            Decimal("100"),
                            "0xfunder",
                            1,
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                finally:
                    con.close()

            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            queue_cfg = WriteQueueConfig(path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    result = run_weather_paper_execution_job(
                        con,
                        queue_cfg,
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes", "snap_hold"],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()

            self.assertEqual(result.metadata["selected_snapshot_ids"], ["snap_yes", "snap_hold"])
            self.assertEqual(result.metadata["decision_count"], 1)
            self.assertEqual(result.metadata["ticket_count"], 1)
            self.assertEqual(result.metadata["gate_count"], 1)
            self.assertEqual(result.metadata["allowed_order_count"], 1)
            self.assertEqual(result.metadata["reservation_count"], 1)
            self.assertEqual(result.metadata["fill_count"], 1)
            self.assertEqual(result.metadata["inventory_position_count"], 3)
            self.assertEqual(result.metadata["exposure_snapshot_count"], 1)
            self.assertEqual(result.metadata["reconciliation_count"], 1)
            self.assertEqual(result.metadata["reconciliation_mismatch_count"], 0)
            self.assertEqual(result.metadata["execution_context_count"], 1)

            allow_tables = ",".join(
                [
                    "runtime.strategy_runs",
                    "runtime.trade_tickets",
                    "runtime.gate_decisions",
                    "trading.orders",
                    "trading.reservations",
                    "trading.fills",
                    "trading.inventory_positions",
                    "trading.order_state_transitions",
                    "trading.exposure_snapshots",
                    "trading.reconciliation_results",
                    "capability.execution_contexts",
                    "runtime.journal_events",
                ]
            )
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass

            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.strategy_runs").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.trade_tickets").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.gate_decisions").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.orders").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.reservations").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.fills").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.inventory_positions").fetchone()[0], 3)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.order_state_transitions").fetchone()[0], 2)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.exposure_snapshots").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.reconciliation_results").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM capability.execution_contexts").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.journal_events").fetchone()[0], 14)
                    row = con.execute(
                        """
                        SELECT wallet_id, token_id, route_action, risk_gate_result
                        FROM capability.execution_contexts
                        """
                    ).fetchone()
                    ticket = load_trade_ticket(con, ticket_id=result.metadata["ticket_ids"][0])
                    persisted_record = load_execution_context_record(
                        con,
                        execution_context_id=ticket.execution_context_id or "",
                    )
                    hydrated_context = hydrate_execution_context(con, record=persisted_record)
                    rebuilt_intent = build_signal_order_intent_from_handoff(ticket, execution_context=hydrated_context)
                    routed_order = route_trade_ticket_from_handoff(con, ticket_id=ticket.ticket_id)
                    handoff_event = con.execute(
                        """
                        SELECT payload_json
                        FROM runtime.journal_events
                        WHERE event_type = 'signal_order_intent.created'
                        """
                    ).fetchone()
                    routed_event = con.execute(
                        """
                        SELECT payload_json
                        FROM runtime.journal_events
                        WHERE event_type = 'canonical_order.routed'
                        """
                    ).fetchone()
                    order_event = con.execute(
                        """
                        SELECT payload_json
                        FROM runtime.journal_events
                        WHERE event_type = 'order.posted'
                        """
                    ).fetchone()
                    fill_event = con.execute(
                        """
                        SELECT payload_json
                        FROM runtime.journal_events
                        WHERE event_type = 'fill.created'
                        """
                    ).fetchone()
                    reservation_event = con.execute(
                        """
                        SELECT payload_json
                        FROM runtime.journal_events
                        WHERE event_type = 'reservation.converted'
                        """
                    ).fetchone()
                    exposure_event = con.execute(
                        """
                        SELECT payload_json
                        FROM runtime.journal_events
                        WHERE event_type = 'exposure.snapshot'
                        """
                    ).fetchone()
                    reconciliation_event = con.execute(
                        """
                        SELECT payload_json
                        FROM runtime.journal_events
                        WHERE event_type = 'reconciliation.checked'
                        """
                    ).fetchone()
                    order_row = con.execute(
                        """
                        SELECT status
                        FROM trading.orders
                        """
                    ).fetchone()
                    latest_transition = con.execute(
                        """
                        SELECT from_status, to_status
                        FROM trading.order_state_transitions
                        ORDER BY timestamp DESC, transition_id DESC
                        LIMIT 1
                        """
                    ).fetchone()
                finally:
                    con.close()
            handoff_payload = json.loads(handoff_event[0])
            routed_payload = json.loads(routed_event[0])
            order_payload = json.loads(order_event[0])
            fill_payload = json.loads(fill_event[0])
            reservation_payload = json.loads(reservation_event[0])
            exposure_payload = json.loads(exposure_event[0])
            reconciliation_payload = json.loads(reconciliation_event[0])
            self.assertEqual(tuple(row), ("wallet_weather_1", "tok_yes", "fak", "pending_gate"))
            self.assertEqual(ticket.wallet_id, "wallet_weather_1")
            self.assertIsNotNone(ticket.execution_context_id)
            self.assertEqual(rebuilt_intent.request_id, ticket.request_id)
            self.assertEqual(handoff_payload["ticket_id"], ticket.ticket_id)
            self.assertEqual(handoff_payload["request_id"], ticket.request_id)
            self.assertEqual(handoff_payload["execution_context_id"], ticket.execution_context_id)
            self.assertEqual(handoff_payload["market_id"], rebuilt_intent.canonical_order.market_id)
            self.assertEqual(handoff_payload["route_action"], rebuilt_intent.canonical_order.route_action.value)
            self.assertEqual(handoff_payload["time_in_force"], rebuilt_intent.canonical_order.time_in_force.value)
            self.assertEqual(handoff_payload["canonical_order_hash"], routed_payload["canonical_order_hash"])
            self.assertEqual(routed_payload["ticket_id"], ticket.ticket_id)
            self.assertEqual(routed_payload["request_id"], ticket.request_id)
            self.assertEqual(routed_payload["router_reason"], "route_action_normalized")
            self.assertEqual(routed_payload["time_in_force"], routed_order.time_in_force.value)
            self.assertEqual(order_payload["ticket_id"], ticket.ticket_id)
            self.assertEqual(order_payload["request_id"], ticket.request_id)
            self.assertEqual(order_payload["status"], "posted")
            self.assertEqual(fill_payload["ticket_id"], ticket.ticket_id)
            self.assertEqual(fill_payload["request_id"], ticket.request_id)
            self.assertEqual(reservation_payload["status"], "converted")
            self.assertEqual(exposure_payload["filled_position_size"], "0")
            self.assertEqual(exposure_payload["settled_position_size"], "10.00000000")
            self.assertEqual(reconciliation_payload["status"], "ok")
            self.assertEqual(reconciliation_payload["discrepancy"], "0.00000000")
            self.assertEqual(order_row[0], "filled")
            self.assertEqual(tuple(latest_transition), ("posted", "filled"))
            self.assertTrue(handoff_payload["canonical_order_hash"].startswith("coh_"))

    def test_ui_execution_ticket_summary_surfaces_reconciliation_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            report_json = str(Path(tmpdir) / "readiness.json")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                        "INSERT INTO capability.market_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["tok_yes", "mkt_weather_1", "cond_weather_1", "YES", Decimal("0.01"), 30, False, Decimal("1"), True, True, "[\"gamma\",\"clob_public\"]", datetime(2026, 3, 10, 10, 0)],
                    )
                    con.execute(
                        "INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["wallet_weather_1", "eoa", 1, "0xfunder", "[\"0xrelayer\"]", True, True, None, datetime(2026, 3, 10, 10, 0)],
                    )
                    con.execute(
                        "INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["snap_yes", "fv_yes", "frun_weather_1", "mkt_weather_1", "cond_weather_1", "tok_yes", "YES", 0.55, 0.63, 800, 500, "TAKE", "BUY", "ui recon", "{\"signal_ts_ms\":1710000000000}", datetime(2026, 3, 10, 10, 0)],
                    )
                    con.execute(
                        "INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["wallet_weather_1", "usdc_e", "usdc_e", "cash", "cash", "available", Decimal("100"), "0xfunder", 1, datetime(2026, 3, 10, 10, 0)],
                    )
                finally:
                    con.close()
            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            queue_cfg = WriteQueueConfig(path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    run_weather_paper_execution_job(
                        con,
                        queue_cfg,
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes"],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()
            allow_tables = ",".join(
                [
                    "runtime.strategy_runs",
                    "runtime.trade_tickets",
                    "runtime.gate_decisions",
                    "trading.orders",
                    "trading.reservations",
                    "trading.fills",
                    "trading.inventory_positions",
                    "trading.order_state_transitions",
                    "trading.exposure_snapshots",
                    "trading.reconciliation_results",
                    "capability.execution_contexts",
                    "runtime.journal_events",
                ]
            )
            with patch.dict("os.environ", {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables}, clear=False):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass
            built = build_ui_lite_db_once(
                src_db_path=db_path,
                dst_db_path=lite_db,
                readiness_report_json_path=report_json,
            )
            self.assertTrue(built.ok, built.error)
            con = connect_duckdb(DuckDBConfig(db_path=lite_db, ddl_path=None))
            try:
                ticket_row = con.execute(
                    """
                    SELECT
                        reconciliation_status,
                        reconciliation_discrepancy,
                        execution_result,
                        operator_attention_required,
                        fill_count,
                        filled_size,
                        latest_transition_to_status,
                        paper_fill_mode
                    FROM ui.execution_ticket_summary
                    """
                ).fetchone()
                run_row = con.execute(
                    """
                    SELECT
                        ticket_count,
                        gate_allowed_count,
                        filled_count,
                        reconciliation_ok_count,
                        attention_required_count
                    FROM ui.execution_run_summary
                    """
                ).fetchone()
                exception_count = con.execute("SELECT COUNT(*) FROM ui.execution_exception_summary").fetchone()[0]
                journal_row = con.execute(
                    """
                    SELECT event_count, ticket_count, fill_ticket_count, mismatch_event_count
                    FROM ui.paper_run_journal_summary
                    """
                ).fetchone()
                ops_row = con.execute(
                    """
                    SELECT ticket_count, filled_count, rejected_count, reconciliation_mismatch_count, attention_required_count
                    FROM ui.daily_ops_summary
                    """
                ).fetchone()
                review_row = con.execute(
                    """
                    SELECT execution_result, operator_attention_required, summary_json
                    FROM ui.daily_review_input
                    """
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(
                ticket_row,
                ("ok", Decimal("0E-8"), "filled", False, 1, Decimal("10.00000000"), "filled", "quote_based"),
            )
            self.assertEqual(run_row, (1, 1, 1, 1, 0))
            self.assertEqual(exception_count, 0)
            self.assertEqual(journal_row, (14, 1, 1, 0))
            self.assertEqual(ops_row, (1, 1, 0, 0, 0))
            self.assertEqual(review_row[0], "filled")
            self.assertFalse(review_row[1])
            self.assertIn('"execution_result":"filled"', review_row[2])

    def test_ui_execution_exception_summary_surfaces_reconciliation_mismatches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            report_json = str(Path(tmpdir) / "readiness.json")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                        "INSERT INTO capability.market_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["tok_yes", "mkt_weather_1", "cond_weather_1", "YES", Decimal("0.01"), 30, False, Decimal("1"), True, True, "[\"gamma\",\"clob_public\"]", datetime(2026, 3, 10, 10, 0)],
                    )
                    con.execute(
                        "INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["wallet_weather_1", "eoa", 1, "0xfunder", "[\"0xrelayer\"]", True, True, None, datetime(2026, 3, 10, 10, 0)],
                    )
                    con.execute(
                        "INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["snap_yes", "fv_yes", "frun_weather_1", "mkt_weather_1", "cond_weather_1", "tok_yes", "YES", 0.55, 0.63, 800, 500, "TAKE", "BUY", "ui mismatch", "{\"signal_ts_ms\":1710000000000}", datetime(2026, 3, 10, 10, 0)],
                    )
                    con.execute(
                        "INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        ["wallet_weather_1", "usdc_e", "usdc_e", "cash", "cash", "available", Decimal("100"), "0xfunder", 1, datetime(2026, 3, 10, 10, 0)],
                    )
                finally:
                    con.close()
            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            queue_cfg = WriteQueueConfig(path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    run_weather_paper_execution_job(
                        con,
                        queue_cfg,
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes"],
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()
            allow_tables = ",".join(
                [
                    "runtime.strategy_runs",
                    "runtime.trade_tickets",
                    "runtime.gate_decisions",
                    "trading.orders",
                    "trading.reservations",
                    "trading.fills",
                    "trading.inventory_positions",
                    "trading.order_state_transitions",
                    "trading.exposure_snapshots",
                    "trading.reconciliation_results",
                    "capability.execution_contexts",
                    "runtime.journal_events",
                ]
            )
            with patch.dict("os.environ", {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables}, clear=False):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass
            with patch.dict("os.environ", writer_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                        """
                        UPDATE trading.reconciliation_results
                        SET status = 'inventory_mismatch',
                            discrepancy = ?,
                            resolution = 'manual_review_required'
                        """,
                        [Decimal("1.00000000")],
                    )
                finally:
                    con.close()
            built = build_ui_lite_db_once(
                src_db_path=db_path,
                dst_db_path=lite_db,
                readiness_report_json_path=report_json,
            )
            self.assertTrue(built.ok, built.error)
            con = connect_duckdb(DuckDBConfig(db_path=lite_db, ddl_path=None))
            try:
                ticket_row = con.execute(
                    """
                    SELECT execution_result, operator_attention_required
                    FROM ui.execution_ticket_summary
                    """
                ).fetchone()
                exception_row = con.execute(
                    """
                    SELECT execution_result, reconciliation_status, reconciliation_discrepancy
                    FROM ui.execution_exception_summary
                    """
                ).fetchone()
                run_row = con.execute(
                    """
                    SELECT reconciliation_mismatch_count, attention_required_count
                    FROM ui.execution_run_summary
                    """
                ).fetchone()
                journal_row = con.execute(
                    """
                    SELECT mismatch_event_count
                    FROM ui.paper_run_journal_summary
                    """
                ).fetchone()
                ops_row = con.execute(
                    """
                    SELECT rejected_count, reconciliation_mismatch_count, attention_required_count
                    FROM ui.daily_ops_summary
                    """
                ).fetchone()
                review_row = con.execute(
                    """
                    SELECT execution_result, operator_attention_required, summary_json
                    FROM ui.daily_review_input
                    WHERE operator_attention_required
                    """
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(ticket_row, ("reconciliation_mismatch", True))
            self.assertEqual(exception_row, ("reconciliation_mismatch", "inventory_mismatch", Decimal("1.00000000")))
            self.assertEqual(run_row, (1, 1))
            self.assertEqual(journal_row, (0,))
            self.assertEqual(ops_row, (0, 1, 1))
            self.assertEqual(review_row[0], "reconciliation_mismatch")
            self.assertTrue(review_row[1])
            self.assertIn('"reconciliation_status":"inventory_mismatch"', review_row[2])

    def test_weather_paper_execution_rerun_keeps_row_counts_and_latest_journal_stable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                        """
                        INSERT INTO capability.market_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "tok_yes",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "YES",
                            Decimal("0.01"),
                            30,
                            False,
                            Decimal("1"),
                            True,
                            True,
                            "[\"gamma\",\"clob_public\"]",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "wallet_weather_1",
                            "eoa",
                            1,
                            "0xfunder",
                            "[\"0xrelayer\"]",
                            True,
                            True,
                            None,
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "snap_yes",
                            "fv_yes",
                            "frun_weather_1",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "tok_yes",
                            "YES",
                            0.55,
                            0.63,
                            800,
                            500,
                            "TAKE",
                            "BUY",
                            "paper rerun",
                            "{\"signal_ts_ms\":1710000000000}",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "wallet_weather_1",
                            "usdc_e",
                            "usdc_e",
                            "cash",
                            "cash",
                            "available",
                            Decimal("100"),
                            "0xfunder",
                            1,
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                finally:
                    con.close()

            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            queue_cfg = WriteQueueConfig(path=queue_path)
            params_json = {
                "wallet_id": "wallet_weather_1",
                "strategy_registrations": [
                    {
                        "strategy_id": "weather_primary",
                        "strategy_version": "v1",
                        "priority": 1,
                        "route_action": "FAK",
                        "size": "10",
                        "min_edge_bps": 500,
                    }
                ],
                "snapshot_ids": ["snap_yes"],
            }

            latest_after_runs = []
            for observed_at in [
                datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                datetime(2026, 3, 10, 10, 7, tzinfo=UTC),
            ]:
                with patch.dict("os.environ", reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        result = run_weather_paper_execution_job(
                            con,
                            queue_cfg,
                            params_json=params_json,
                            observed_at=observed_at,
                        )
                    finally:
                        con.close()
                allow_tables = ",".join(
                    [
                        "runtime.strategy_runs",
                        "runtime.trade_tickets",
                        "runtime.gate_decisions",
                        "trading.orders",
                        "trading.reservations",
                        "trading.fills",
                        "trading.inventory_positions",
                        "trading.order_state_transitions",
                        "trading.exposure_snapshots",
                        "trading.reconciliation_results",
                        "capability.execution_contexts",
                        "runtime.journal_events",
                    ]
                )
                with patch.dict(
                    "os.environ",
                    {
                        "ASTERION_DB_PATH": db_path,
                        "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                    },
                    clear=False,
                ):
                    while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                        pass
                with patch.dict("os.environ", reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        latest_after_runs.append(
                            con.execute(
                                """
                                SELECT event_id, event_type, entity_id
                                FROM runtime.journal_events
                                ORDER BY created_at DESC, event_id DESC
                                LIMIT 1
                                """
                            ).fetchone()
                        )
                    finally:
                        con.close()

            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.strategy_runs").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.trade_tickets").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.gate_decisions").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.orders").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.reservations").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.fills").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.inventory_positions").fetchone()[0], 3)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.order_state_transitions").fetchone()[0], 2)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.exposure_snapshots").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.reconciliation_results").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM capability.execution_contexts").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.journal_events").fetchone()[0], 14)
                    by_type = dict(
                        con.execute(
                            """
                            SELECT event_type, COUNT(*)
                            FROM runtime.journal_events
                            GROUP BY event_type
                            ORDER BY event_type
                            """
                        ).fetchall()
                    )
                    latest = con.execute(
                        """
                        SELECT event_id, event_type, entity_id
                        FROM runtime.journal_events
                        ORDER BY created_at DESC, event_id DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    ticket = load_trade_ticket(con, ticket_id=result.metadata["ticket_ids"][0])
                finally:
                    con.close()

            self.assertEqual(
                by_type,
                {
                    "canonical_order.routed": 1,
                    "exposure.snapshot": 1,
                    "fill.created": 1,
                    "gate.decision": 1,
                    "inventory.updated": 1,
                    "order.created": 1,
                    "order.filled": 1,
                    "order.posted": 1,
                    "reconciliation.checked": 1,
                    "reservation.converted": 1,
                    "reservation.created": 1,
                    "signal_order_intent.created": 1,
                    "strategy_run.created": 1,
                    "trade_ticket.created": 1,
                },
            )
            self.assertEqual(latest_after_runs[0], latest_after_runs[1])
            self.assertEqual(latest, latest_after_runs[1])
            self.assertEqual(ticket.wallet_id, "wallet_weather_1")
            self.assertIsNotNone(ticket.execution_context_id)

    def test_weather_paper_execution_rejected_gate_writes_no_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                        """
                        INSERT INTO capability.market_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "tok_yes",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "YES",
                            Decimal("0.01"),
                            30,
                            False,
                            Decimal("1"),
                            True,
                            True,
                            "[\"gamma\",\"clob_public\"]",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "wallet_weather_1",
                            "eoa",
                            1,
                            "0xfunder",
                            "[\"0xrelayer\"]",
                            True,
                            True,
                            None,
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "snap_yes",
                            "fv_yes",
                            "frun_weather_1",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "tok_yes",
                            "YES",
                            0.55,
                            0.63,
                            800,
                            500,
                            "TAKE",
                            "BUY",
                            "paper reject",
                            "{\"signal_ts_ms\":1710000000000}",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                finally:
                    con.close()

            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            queue_cfg = WriteQueueConfig(path=queue_path)
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    result = run_weather_paper_execution_job(
                        con,
                        queue_cfg,
                        params_json={
                            "wallet_id": "wallet_weather_1",
                            "strategy_registrations": [
                                {
                                    "strategy_id": "weather_primary",
                                    "strategy_version": "v1",
                                    "priority": 1,
                                    "route_action": "FAK",
                                    "size": "10",
                                    "min_edge_bps": 500,
                                }
                            ],
                            "snapshot_ids": ["snap_yes"],
                            "dq_level": "WARN",
                        },
                        observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
                    )
                finally:
                    con.close()

            allow_tables = ",".join(
                [
                    "runtime.strategy_runs",
                    "runtime.trade_tickets",
                    "runtime.gate_decisions",
                    "trading.orders",
                    "trading.fills",
                    "trading.order_state_transitions",
                    "capability.execution_contexts",
                    "runtime.journal_events",
                ]
            )
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass

            self.assertEqual(result.metadata["allowed_order_count"], 0)
            self.assertEqual(result.metadata["reservation_count"], 0)
            self.assertEqual(result.metadata["fill_count"], 0)
            self.assertEqual(result.metadata["exposure_snapshot_count"], 0)
            self.assertEqual(result.metadata["reconciliation_count"], 0)
            self.assertEqual(len(result.metadata["rejected_ticket_ids"]), 1)

            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.gate_decisions").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.orders").fetchone()[0], 0)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.reservations").fetchone()[0], 0)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.fills").fetchone()[0], 0)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.inventory_positions").fetchone()[0], 0)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.order_state_transitions").fetchone()[0], 0)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.exposure_snapshots").fetchone()[0], 0)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.reconciliation_results").fetchone()[0], 0)
                    by_type = dict(
                        con.execute(
                            """
                            SELECT event_type, COUNT(*)
                            FROM runtime.journal_events
                            GROUP BY event_type
                            ORDER BY event_type
                            """
                        ).fetchall()
                    )
                finally:
                    con.close()

            self.assertEqual(
                by_type,
                {
                    "canonical_order.routed": 1,
                    "gate.decision": 1,
                    "order.rejected_by_gate": 1,
                    "signal_order_intent.created": 1,
                    "strategy_run.created": 1,
                    "trade_ticket.created": 1,
                },
            )

    def test_paper_chain_persists_runtime_and_trading_ledgers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                        """
                        INSERT INTO capability.market_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "tok_yes",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "YES",
                            Decimal("0.01"),
                            30,
                            False,
                            Decimal("1"),
                            True,
                            True,
                            "[\"gamma\",\"clob_public\"]",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "wallet_weather_1",
                            "eoa",
                            1,
                            "0xfunder",
                            "[\"0xrelayer\"]",
                            True,
                            True,
                            None,
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                    con.execute(
                        """
                        INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            "snap_yes",
                            "fv_yes",
                            "frun_weather_1",
                            "mkt_weather_1",
                            "cond_weather_1",
                            "tok_yes",
                            "YES",
                            0.55,
                            0.63,
                            800,
                            500,
                            "TAKE",
                            "BUY",
                            "paper chain",
                            "{\"signal_ts_ms\":1710000000000}",
                            datetime(2026, 3, 10, 10, 0),
                        ],
                    )
                finally:
                    con.close()

            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    snapshots, signal_ts_lookup = load_watch_only_snapshots(con)
                    strategy_run, decisions = run_strategy_engine(
                        ctx=StrategyContext(
                            data_snapshot_id="snap_weather_1",
                            universe_snapshot_id="uni_weather_1",
                            asof_ts_ms=1_710_000_000_000,
                            dq_level="PASS",
                            quote_snapshot_refs=[],
                        ),
                        snapshots=snapshots,
                        strategies=[
                            StrategyRegistration(
                                strategy_id="weather_primary",
                                strategy_version="v1",
                                priority=1,
                                route_action=RouteAction.FAK,
                                size=Decimal("10"),
                            )
                        ],
                        snapshot_signal_ts_ms=signal_ts_lookup,
                    )
                    ticket = build_trade_ticket(decisions[0], created_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC))
                    market_capability = load_market_capability(con, token_id="tok_yes")
                    account_capability = load_account_trading_capability(con, wallet_id="wallet_weather_1")
                finally:
                    con.close()

            intent = build_signal_order_intent(ticket, market_capability=market_capability, account_capability=account_capability)
            order = build_order_from_intent(intent, wallet_id="wallet_weather_1", created_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC))
            gate = evaluate_execution_gate(
                ticket=ticket,
                intent=intent,
                watch_only_active=False,
                degrade_active=False,
                available_quantity=Decimal("100"),
            )
            self.assertTrue(gate.allowed)
            reservation = build_reservation(order, created_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC))
            starting_positions = [
                InventoryPosition(
                    wallet_id="wallet_weather_1",
                    asset_type="usdc_e",
                    token_id=None,
                    market_id=None,
                    outcome=None,
                    balance_type=BalanceType.AVAILABLE,
                    quantity=Decimal("100"),
                    funder="0xfunder",
                    signature_type=1,
                    updated_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
                )
            ]
            updated_positions = apply_reservation_to_inventory(starting_positions, reservation, observed_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC))
            exposure = build_exposure_snapshot(
                order,
                positions=updated_positions,
                reservation=reservation,
                captured_at=datetime(2026, 3, 10, 10, 1, tzinfo=UTC),
            )
            journal_events = [
                build_journal_event(
                    event_type="strategy_run.created",
                    entity_type="strategy_run",
                    entity_id=strategy_run.run_id,
                    run_id=strategy_run.run_id,
                    payload_json={"decision_count": strategy_run.decision_count},
                ),
                build_journal_event(
                    event_type="trade_ticket.created",
                    entity_type="trade_ticket",
                    entity_id=ticket.ticket_id,
                    run_id=strategy_run.run_id,
                    payload_json={"ticket_hash": ticket.ticket_hash, "request_id": ticket.request_id, "ticket_id": ticket.ticket_id},
                ),
                build_journal_event(
                    event_type="gate.evaluated",
                    entity_type="gate_decision",
                    entity_id=gate.gate_id,
                    run_id=strategy_run.run_id,
                    payload_json={"allowed": gate.allowed, "reason_codes": gate.reason_codes},
                ),
                build_journal_event(
                    event_type="order.created",
                    entity_type="order",
                    entity_id=order.order_id,
                    run_id=strategy_run.run_id,
                    payload_json={
                        "client_order_id": order.client_order_id,
                        "request_id": ticket.request_id,
                        "status": str(order.status),
                        "ticket_id": ticket.ticket_id,
                    },
                ),
                build_journal_event(
                    event_type="reservation.created",
                    entity_type="reservation",
                    entity_id=reservation.reservation_id,
                    run_id=strategy_run.run_id,
                    payload_json={
                        "asset_type": reservation.asset_type,
                        "request_id": ticket.request_id,
                        "reserved_quantity": str(reservation.reserved_quantity),
                        "ticket_id": ticket.ticket_id,
                    },
                ),
            ]

            queue_cfg = WriteQueueConfig(path=queue_path)
            enqueue_strategy_run_upserts(queue_cfg, runs=[strategy_run], run_id=strategy_run.run_id)
            enqueue_trade_ticket_upserts(queue_cfg, tickets=[ticket], run_id=strategy_run.run_id)
            enqueue_gate_decision_upserts(queue_cfg, gate_decisions=[gate], run_id=strategy_run.run_id)
            enqueue_order_upserts(queue_cfg, orders=[order], run_id=strategy_run.run_id)
            enqueue_reservation_upserts(queue_cfg, reservations=[reservation], run_id=strategy_run.run_id)
            enqueue_inventory_position_upserts(queue_cfg, positions=updated_positions, run_id=strategy_run.run_id)
            enqueue_exposure_snapshot_upserts(queue_cfg, snapshots=[exposure], run_id=strategy_run.run_id)
            enqueue_journal_event_upserts(queue_cfg, journal_events=journal_events, run_id=strategy_run.run_id)

            allow_tables = ",".join(
                [
                    "runtime.strategy_runs",
                    "runtime.trade_tickets",
                    "runtime.gate_decisions",
                    "runtime.journal_events",
                    "trading.orders",
                    "trading.reservations",
                    "trading.inventory_positions",
                    "trading.exposure_snapshots",
                ]
            )
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables,
                },
                clear=False,
            ):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass

            with patch.dict("os.environ", reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.strategy_runs").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.trade_tickets").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.gate_decisions").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.journal_events").fetchone()[0], 5)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.orders").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.reservations").fetchone()[0], 1)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.inventory_positions").fetchone()[0], 2)
                    self.assertEqual(con.execute("SELECT COUNT(*) FROM trading.exposure_snapshots").fetchone()[0], 1)
                finally:
                    con.close()


if __name__ == "__main__":
    unittest.main()
