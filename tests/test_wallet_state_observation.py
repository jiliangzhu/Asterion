from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from asterion_core.blockchain.wallet_state_v1 import (
    PolygonChainRegistry,
    build_wallet_state_observations,
    load_polygon_chain_registry,
)
from asterion_core.contracts import (
    AccountTradingCapability,
    ExternalBalanceObservation,
    ExternalBalanceObservationKind,
)
from asterion_core.journal.journal_v3 import external_balance_observation_to_row


@dataclass(frozen=True)
class _Read:
    observed_quantity: Decimal
    block_number: int | None
    raw_observation_json: dict[str, object]
    source: str


class _Reader:
    def read_native_balance(self, funder: str, *, decimals: int) -> _Read:
        self._assert_decimal_args(funder, decimals)
        return _Read(
            observed_quantity=Decimal("1.5"),
            block_number=123,
            raw_observation_json={"method": "eth_getBalance"},
            source="polygon_rpc",
        )

    def read_erc20_balance(self, funder: str, token_address: str, *, decimals: int) -> _Read:
        self._assert_decimal_args(funder, decimals)
        self._assert_address(token_address)
        return _Read(
            observed_quantity=Decimal("25"),
            block_number=123,
            raw_observation_json={"method": "erc20.balanceOf"},
            source="polygon_rpc",
        )

    def read_erc20_allowance(
        self,
        funder: str,
        spender: str,
        token_address: str,
        *,
        decimals: int,
    ) -> _Read:
        self._assert_decimal_args(funder, decimals)
        self._assert_address(spender)
        self._assert_address(token_address)
        return _Read(
            observed_quantity=Decimal("100"),
            block_number=123,
            raw_observation_json={"method": "erc20.allowance"},
            source="polygon_rpc",
        )

    @staticmethod
    def _assert_decimal_args(funder: str, decimals: int) -> None:
        if not funder.startswith("0x"):
            raise AssertionError("expected address-like funder")
        if decimals < 0:
            raise AssertionError("expected non-negative decimals")

    @staticmethod
    def _assert_address(value: str) -> None:
        if not value.startswith("0x"):
            raise AssertionError("expected address-like value")


def _chain_registry() -> PolygonChainRegistry:
    return PolygonChainRegistry(
        chain_id=137,
        native_gas_asset_type="native_gas",
        native_gas_symbol="POL",
        native_gas_decimals=18,
        usdc_e_asset_type="usdc_e",
        usdc_e_token_id="usdc_e",
        usdc_e_contract_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        usdc_e_decimals=6,
        allowance_targets={"relayer": "0x2222222222222222222222222222222222222222"},
    )


def _account_capability(*, allowance_targets: list[str] | None = None) -> AccountTradingCapability:
    return AccountTradingCapability(
        wallet_id="wallet_weather_1",
        wallet_type="eoa",
        signature_type=1,
        funder="0x1111111111111111111111111111111111111111",
        allowance_targets=list(allowance_targets or ["0x2222222222222222222222222222222222222222"]),
        can_use_relayer=True,
        can_trade=True,
        restricted_reason=None,
    )


class WalletStateObservationUnitTest(unittest.TestCase):
    def test_load_chain_registry_parses_repo_shape(self) -> None:
        payload = {
            "chain_id": 137,
            "native_gas": {"asset_type": "native_gas", "symbol": "POL", "decimals": 18},
            "usdc_e": {
                "asset_type": "usdc_e",
                "token_id": "usdc_e",
                "contract_address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                "decimals": 6,
            },
            "allowance_targets": {"relayer": "0x2222222222222222222222222222222222222222"},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "chain_registry.polygon.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            registry = load_polygon_chain_registry(path)
        self.assertEqual(registry.chain_id, 137)
        self.assertEqual(registry.native_gas_symbol, "POL")
        self.assertEqual(registry.allowance_targets["relayer"], "0x2222222222222222222222222222222222222222")

    def test_load_chain_registry_rejects_missing_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "chain_registry.polygon.json"
            path.write_text(json.dumps({"chain_id": 137, "native_gas": {}}), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_polygon_chain_registry(path)

    def test_build_wallet_state_observations_is_stable(self) -> None:
        observed_at = datetime(2026, 3, 11, 11, 0, tzinfo=timezone.utc)
        first = build_wallet_state_observations(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            reader=_Reader(),
            observed_at=observed_at,
        )
        second = build_wallet_state_observations(
            account_capability=_account_capability(),
            chain_registry=_chain_registry(),
            reader=_Reader(),
            observed_at=observed_at,
        )
        self.assertEqual([item.observation_id for item in first], [item.observation_id for item in second])
        self.assertEqual(len(first), 3)
        self.assertEqual(first[0].observation_kind, ExternalBalanceObservationKind.WALLET_BALANCE)
        self.assertEqual(first[2].observation_kind, ExternalBalanceObservationKind.TOKEN_ALLOWANCE)

    def test_build_wallet_state_observations_rejects_unknown_allowance_target(self) -> None:
        with self.assertRaises(ValueError):
            build_wallet_state_observations(
                account_capability=_account_capability(
                    allowance_targets=["0x3333333333333333333333333333333333333333"]
                ),
                chain_registry=_chain_registry(),
                reader=_Reader(),
                observed_at=datetime(2026, 3, 11, 11, 0, tzinfo=timezone.utc),
            )

    def test_external_balance_observation_row_builder(self) -> None:
        observation = ExternalBalanceObservation(
            observation_id="obs_1",
            wallet_id="wallet_weather_1",
            funder="0x1111111111111111111111111111111111111111",
            signature_type=1,
            asset_type="usdc_e",
            token_id="usdc_e",
            market_id=None,
            outcome=None,
            observation_kind=ExternalBalanceObservationKind.TOKEN_ALLOWANCE,
            allowance_target="0x2222222222222222222222222222222222222222",
            chain_id=137,
            block_number=123,
            observed_quantity=Decimal("42"),
            source="polygon_rpc",
            observed_at=datetime(2026, 3, 11, 11, 0),
            raw_observation_json={"quantity_raw": "42000000"},
        )
        row = external_balance_observation_to_row(observation)
        self.assertEqual(row[0], "obs_1")
        self.assertEqual(row[8], "token_allowance")
        self.assertEqual(row[12], "42")


if __name__ == "__main__":
    unittest.main()
