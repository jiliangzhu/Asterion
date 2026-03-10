from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from asterion_core.contracts.execution import (
    AccountTradingCapability,
    CanonicalOrderContract,
    ExecutionContext,
    MarketCapability,
    RouteAction,
    TimeInForce,
)
from asterion_core.contracts.ids import (
    build_forecast_cache_key,
    new_client_order_id,
    new_event_id,
    new_proposal_id,
    new_request_id,
    new_reservation_id,
    stable_object_id,
)
from asterion_core.contracts.inventory import (
    BalanceType,
    InventoryPosition,
    Order,
    OrderSide,
    OrderStatus,
)
from asterion_core.contracts.weather import ForecastRequest, ResolutionSpec


class IdsAndContractsTest(unittest.TestCase):
    def test_prefixed_ids_have_stable_prefixes(self) -> None:
        self.assertTrue(new_request_id().startswith("req_"))
        self.assertTrue(new_client_order_id().startswith("ord_"))
        self.assertTrue(new_reservation_id().startswith("res_"))
        self.assertTrue(new_proposal_id().startswith("prop_"))
        self.assertTrue(new_event_id().startswith("evt_"))

    def test_stable_object_id_ignores_dict_order(self) -> None:
        left = stable_object_id("obj", {"a": 1, "b": {"x": 2, "y": 3}})
        right = stable_object_id("obj", {"b": {"y": 3, "x": 2}, "a": 1})
        self.assertEqual(left, right)

    def test_forecast_cache_key_changes_when_dimensions_change(self) -> None:
        base = build_forecast_cache_key(
            market_id="m1",
            station_id="s1",
            spec_version="v1",
            source="openmeteo",
            model_run="run-a",
            forecast_target_time=datetime(2026, 3, 9, tzinfo=timezone.utc),
        )
        changed = build_forecast_cache_key(
            market_id="m1",
            station_id="s1",
            spec_version="v1",
            source="nws",
            model_run="run-a",
            forecast_target_time=datetime(2026, 3, 9, tzinfo=timezone.utc),
        )
        self.assertNotEqual(base, changed)

    def test_execution_context_requires_capability_consistency(self) -> None:
        market = MarketCapability(
            market_id="m1",
            condition_id="c1",
            token_id="t1",
            outcome="YES",
            tick_size=Decimal("0.01"),
            fee_rate_bps=25,
            neg_risk=False,
            min_order_size=Decimal("1"),
            tradable=True,
            fees_enabled=True,
            data_sources=["gamma", "clob"],
            updated_at=datetime.now(timezone.utc),
        )
        account = AccountTradingCapability(
            wallet_id="w1",
            wallet_type="EOA",
            signature_type=1,
            funder="0xabc",
            allowance_targets=["0xrelayer"],
            can_use_relayer=True,
            can_trade=True,
            restricted_reason=None,
        )
        ctx = ExecutionContext(
            market_capability=market,
            account_capability=account,
            token_id="t1",
            route_action=RouteAction.FAK,
            fee_rate_bps=25,
            tick_size=Decimal("0.01"),
            signature_type=1,
            funder="0xabc",
            risk_gate_result="pass",
        )
        self.assertEqual(ctx.token_id, "t1")

    def test_canonical_order_contract_validates_route_action_time_in_force(self) -> None:
        with self.assertRaises(ValueError):
            CanonicalOrderContract(
                market_id="m1",
                token_id="t1",
                outcome="YES",
                side="buy",
                price=Decimal("0.42"),
                size=Decimal("10"),
                route_action=RouteAction.FOK,
                time_in_force=TimeInForce.GTC,
                expiration=None,
                fee_rate_bps=25,
                signature_type=1,
                funder="0xabc",
            )

    def test_station_first_contracts_require_station_id(self) -> None:
        with self.assertRaises(ValueError):
            ResolutionSpec(
                market_id="m1",
                condition_id="c1",
                location_name="Austin",
                station_id="",
                latitude=1.0,
                longitude=2.0,
                timezone="America/Chicago",
                observation_date=date(2026, 3, 9),
                observation_window_local="2026-03-09 00:00/23:59",
                metric="max_temp",
                unit="F",
                authoritative_source="NWS",
                fallback_sources=["OpenMeteo"],
                rounding_rule="half_up",
                inclusive_bounds=True,
                spec_version="v1",
            )

    def test_forecast_request_is_constructible(self) -> None:
        request = ForecastRequest(
            market_id="m1",
            condition_id="c1",
            station_id="s1",
            source="openmeteo",
            model_run="run1",
            forecast_target_time=datetime(2026, 3, 9, tzinfo=timezone.utc),
            observation_date=date(2026, 3, 9),
            metric="max_temp",
            latitude=1.0,
            longitude=2.0,
            timezone="America/Chicago",
            spec_version="v1",
        )
        self.assertEqual(request.station_id, "s1")

    def test_inventory_position_key_shape_is_closed(self) -> None:
        position = InventoryPosition(
            wallet_id="w1",
            asset_type="usdc_e",
            token_id=None,
            market_id=None,
            outcome=None,
            balance_type=BalanceType.AVAILABLE,
            quantity=Decimal("10"),
            funder="0xabc",
            signature_type=1,
            updated_at=datetime.now(timezone.utc),
        )
        order = Order(
            order_id="o1",
            client_order_id="co1",
            wallet_id="w1",
            market_id="m1",
            token_id="t1",
            outcome="YES",
            side=OrderSide.BUY,
            price=Decimal("0.42"),
            size=Decimal("10"),
            route_action=RouteAction.FAK,
            time_in_force=TimeInForce.FAK,
            expiration=None,
            fee_rate_bps=25,
            signature_type=1,
            funder="0xabc",
            status=OrderStatus.CREATED,
            filled_size=Decimal("0"),
            remaining_size=Decimal("10"),
            avg_fill_price=None,
            reservation_id=None,
            exchange_order_id=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self.assertEqual(position.wallet_id, order.wallet_id)


if __name__ == "__main__":
    unittest.main()
