from __future__ import annotations

import unittest
from datetime import datetime
from decimal import Decimal

from asterion_core.contracts import (
    BalanceType,
    ExternalBalanceObservation,
    ExternalBalanceObservationKind,
    ExternalFillObservation,
    ExternalFillObservationKind,
    Order,
    OrderSide,
    OrderStatus,
    ReconciliationStatus,
    RouteAction,
    TimeInForce,
)
from asterion_core.risk import (
    build_external_execution_reconciliation_result,
    classify_external_execution_reconciliation_status,
)


def _order(*, status: OrderStatus = OrderStatus.FILLED, filled_size: str = "5") -> Order:
    return Order(
        order_id="ordr_1",
        client_order_id="corder_1",
        wallet_id="wallet_weather_1",
        market_id="mkt_weather_1",
        token_id="tok_yes",
        outcome="YES",
        side=OrderSide.BUY,
        price=Decimal("0.63"),
        size=Decimal("10"),
        route_action=RouteAction.FAK,
        time_in_force=TimeInForce.FAK,
        expiration=None,
        fee_rate_bps=30,
        signature_type=1,
        funder="0x1111111111111111111111111111111111111111",
        status=status,
        filled_size=Decimal(filled_size),
        remaining_size=Decimal("5") if Decimal(filled_size) < Decimal("10") else Decimal("0"),
        avg_fill_price=Decimal("0.63"),
        reservation_id="rsv_1",
        exchange_order_id="extord_1",
        created_at=datetime(2026, 3, 12, 10, 0),
        updated_at=datetime(2026, 3, 12, 10, 1),
    )


def _wallet_observation() -> ExternalBalanceObservation:
    return ExternalBalanceObservation(
        observation_id="ebal_1",
        wallet_id="wallet_weather_1",
        funder="0x1111111111111111111111111111111111111111",
        signature_type=1,
        asset_type="usdc_e",
        token_id="usdc_e",
        market_id=None,
        outcome=None,
        observation_kind=ExternalBalanceObservationKind.WALLET_BALANCE,
        allowance_target=None,
        chain_id=137,
        block_number=123,
        observed_quantity=Decimal("100"),
        source="polygon_rpc",
        observed_at=datetime(2026, 3, 12, 10, 2),
        raw_observation_json={"kind": "wallet_balance"},
    )


def _external_fill(*, size: str = "5") -> ExternalFillObservation:
    return ExternalFillObservation(
        observation_id="efill_1",
        attempt_id="satt_submit_1",
        request_id="subreq_1",
        ticket_id="tt_1",
        order_id="ordr_1",
        wallet_id="wallet_weather_1",
        execution_context_id="ectx_1",
        exchange="polymarket_clob",
        observation_kind=ExternalFillObservationKind.SHADOW_FILL_PARTIAL,
        external_order_id="extord_1",
        external_trade_id="exttrade_1",
        market_id="mkt_weather_1",
        token_id="tok_yes",
        outcome="YES",
        side="buy",
        price=Decimal("0.63"),
        size=Decimal(size),
        fee=Decimal("0.00945"),
        fee_rate_bps=30,
        external_status="partial_filled" if Decimal(size) < Decimal("10") else "filled",
        observed_at=datetime(2026, 3, 12, 10, 3),
        error=None,
        raw_observation_json={"kind": "fill"},
    )


class ExternalExecutionReconciliationTest(unittest.TestCase):
    def test_external_execution_ok(self) -> None:
        order = _order(status=OrderStatus.PARTIAL_FILLED, filled_size="5")
        result = build_external_execution_reconciliation_result(
            order=order,
            ticket_id="tt_1",
            execution_context_id="ectx_1",
            external_order_observation_id="eordobs_1",
            external_fill_observation_id="efillagg_1",
            external_balance_observation_id="ebal_1",
            external_order_status="accepted",
            external_fill_observations=[_external_fill(size="5")],
            wallet_observation_ref=_wallet_observation(),
            created_at=datetime(2026, 3, 12, 10, 4),
        )
        self.assertEqual(result.reconciliation_scope, "external_execution")
        self.assertEqual(result.source_system, "polymarket_clob")
        self.assertEqual(result.status, ReconciliationStatus.OK)
        self.assertEqual(result.local_quantity, Decimal("5.00000000"))
        self.assertEqual(result.remote_quantity, Decimal("5.00000000"))

    def test_external_order_mismatch(self) -> None:
        status = classify_external_execution_reconciliation_status(
            order=_order(status=OrderStatus.POSTED, filled_size="0"),
            external_order_status="rejected",
            external_fill_observations=[],
            wallet_observation_ref=_wallet_observation(),
        )
        self.assertEqual(status, ReconciliationStatus.EXTERNAL_ORDER_MISMATCH)

    def test_external_fill_mismatch(self) -> None:
        status = classify_external_execution_reconciliation_status(
            order=_order(status=OrderStatus.PARTIAL_FILLED, filled_size="5"),
            external_order_status="accepted",
            external_fill_observations=[_external_fill(size="3")],
            wallet_observation_ref=_wallet_observation(),
        )
        self.assertEqual(status, ReconciliationStatus.EXTERNAL_FILL_MISMATCH)

    def test_external_state_unverified(self) -> None:
        status = classify_external_execution_reconciliation_status(
            order=_order(status=OrderStatus.POSTED, filled_size="0"),
            external_order_status=None,
            external_fill_observations=[],
            wallet_observation_ref=None,
        )
        self.assertEqual(status, ReconciliationStatus.EXTERNAL_STATE_UNVERIFIED)

    def test_external_reconciliation_id_is_stable_per_order_scope(self) -> None:
        order = _order(status=OrderStatus.FILLED, filled_size="10")
        first = build_external_execution_reconciliation_result(
            order=order,
            ticket_id="tt_1",
            execution_context_id="ectx_1",
            external_order_observation_id="eordobs_1",
            external_fill_observation_id="efillagg_1",
            external_balance_observation_id="ebal_1",
            external_order_status="accepted",
            external_fill_observations=[_external_fill(size="10")],
            wallet_observation_ref=_wallet_observation(),
            created_at=datetime(2026, 3, 12, 10, 4),
        )
        second = build_external_execution_reconciliation_result(
            order=order,
            ticket_id="tt_1",
            execution_context_id="ectx_1",
            external_order_observation_id="eordobs_2",
            external_fill_observation_id="efillagg_2",
            external_balance_observation_id="ebal_2",
            external_order_status="accepted",
            external_fill_observations=[_external_fill(size="10")],
            wallet_observation_ref=_wallet_observation(),
            created_at=datetime(2026, 3, 12, 10, 5),
        )
        self.assertEqual(first.reconciliation_id, second.reconciliation_id)


if __name__ == "__main__":
    unittest.main()
