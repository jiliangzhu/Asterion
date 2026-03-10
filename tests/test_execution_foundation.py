from __future__ import annotations

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
    InventoryPosition,
    MarketCapability,
    OrderStatus,
    RouteAction,
    WatchOnlySnapshotRecord,
)
from asterion_core.execution import (
    build_order_from_intent,
    build_signal_order_intent,
    build_trade_ticket,
    evaluate_execution_gate,
    load_account_trading_capability,
    load_market_capability,
)
from asterion_core.journal import (
    build_journal_event,
    enqueue_exposure_snapshot_upserts,
    enqueue_gate_decision_upserts,
    enqueue_inventory_position_upserts,
    enqueue_journal_event_upserts,
    enqueue_order_upserts,
    enqueue_reservation_upserts,
    enqueue_strategy_run_upserts,
    enqueue_trade_ticket_upserts,
)
from asterion_core.risk import (
    apply_fill_to_reservation,
    apply_reservation_to_inventory,
    build_exposure_snapshot,
    build_reservation,
    finalize_reservation,
)
from asterion_core.runtime import StrategyContext, StrategyRegistration, load_watch_only_snapshots, run_strategy_engine
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one

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
