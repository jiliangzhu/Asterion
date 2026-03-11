from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from web3 import Web3

from asterion_core.contracts import (
    AccountTradingCapability,
    ExternalBalanceObservation,
    ExternalBalanceObservationKind,
    stable_object_id,
)


ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass(frozen=True)
class PolygonChainRegistry:
    chain_id: int
    native_gas_asset_type: str
    native_gas_symbol: str
    native_gas_decimals: int
    usdc_e_asset_type: str
    usdc_e_token_id: str
    usdc_e_contract_address: str
    usdc_e_decimals: int
    allowance_targets: dict[str, str]

    def __post_init__(self) -> None:
        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if not self.native_gas_asset_type or not self.native_gas_symbol:
            raise ValueError("native gas registry fields are required")
        if self.native_gas_decimals < 0:
            raise ValueError("native gas decimals must be non-negative")
        if not self.usdc_e_asset_type or not self.usdc_e_token_id or not self.usdc_e_contract_address:
            raise ValueError("usdc_e registry fields are required")
        if self.usdc_e_decimals < 0:
            raise ValueError("usdc_e decimals must be non-negative")
        if not self.allowance_targets:
            raise ValueError("allowance_targets are required")


@dataclass(frozen=True)
class ObservationRead:
    observed_quantity: Decimal
    block_number: int | None
    raw_observation_json: dict[str, Any]
    source: str


class WalletStateObservationReader(Protocol):
    def read_native_balance(self, funder: str, *, decimals: int) -> ObservationRead:
        ...

    def read_erc20_balance(self, funder: str, token_address: str, *, decimals: int) -> ObservationRead:
        ...

    def read_erc20_allowance(
        self,
        funder: str,
        spender: str,
        token_address: str,
        *,
        decimals: int,
    ) -> ObservationRead:
        ...


class PolygonWalletStateReader:
    def __init__(self, *, chain_id: int, rpc_urls: list[str], source: str = "polygon_rpc") -> None:
        if chain_id <= 0:
            raise ValueError("chain_id must be positive")
        urls = [str(item).strip() for item in rpc_urls if str(item).strip()]
        if not urls:
            raise ValueError("capability_rpc_urls are required for wallet state observation")
        self._chain_id = int(chain_id)
        self._rpc_urls = urls
        self._source = source

    def read_native_balance(self, funder: str, *, decimals: int) -> ObservationRead:
        funder_address = Web3.to_checksum_address(funder)
        return self._with_web3(
            lambda web3: self._build_balance_read(
                quantity_raw=int(web3.eth.get_balance(funder_address)),
                decimals=decimals,
                block_number=int(web3.eth.block_number),
                raw_payload={
                    "method": "eth_getBalance",
                    "funder": funder_address,
                },
            )
        )

    def read_erc20_balance(self, funder: str, token_address: str, *, decimals: int) -> ObservationRead:
        funder_address = Web3.to_checksum_address(funder)
        token = Web3.to_checksum_address(token_address)
        return self._with_web3(
            lambda web3: self._build_balance_read(
                quantity_raw=int(web3.eth.contract(address=token, abi=ERC20_ABI).functions.balanceOf(funder_address).call()),
                decimals=decimals,
                block_number=int(web3.eth.block_number),
                raw_payload={
                    "method": "erc20.balanceOf",
                    "funder": funder_address,
                    "token_address": token,
                },
            )
        )

    def read_erc20_allowance(
        self,
        funder: str,
        spender: str,
        token_address: str,
        *,
        decimals: int,
    ) -> ObservationRead:
        funder_address = Web3.to_checksum_address(funder)
        spender_address = Web3.to_checksum_address(spender)
        token = Web3.to_checksum_address(token_address)
        return self._with_web3(
            lambda web3: self._build_balance_read(
                quantity_raw=int(
                    web3.eth.contract(address=token, abi=ERC20_ABI).functions.allowance(
                        funder_address,
                        spender_address,
                    ).call()
                ),
                decimals=decimals,
                block_number=int(web3.eth.block_number),
                raw_payload={
                    "method": "erc20.allowance",
                    "funder": funder_address,
                    "spender": spender_address,
                    "token_address": token,
                },
            )
        )

    def _with_web3(self, fn) -> ObservationRead:
        last_error: Exception | None = None
        for rpc_url in self._rpc_urls:
            try:
                web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
                observed_chain_id = int(web3.eth.chain_id)
                if observed_chain_id != self._chain_id:
                    raise RuntimeError(
                        f"polygon wallet reader chain_id mismatch: expected {self._chain_id}, got {observed_chain_id}"
                    )
                return fn(web3)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise RuntimeError("wallet state observation failed for all configured rpc urls") from last_error

    def _build_balance_read(
        self,
        *,
        quantity_raw: int,
        decimals: int,
        block_number: int | None,
        raw_payload: dict[str, Any],
    ) -> ObservationRead:
        scale = Decimal(10) ** int(decimals)
        return ObservationRead(
            observed_quantity=Decimal(quantity_raw) / scale,
            block_number=block_number,
            raw_observation_json={
                **raw_payload,
                "quantity_raw": str(quantity_raw),
                "decimals": int(decimals),
            },
            source=self._source,
        )


def load_polygon_chain_registry(path: str | Path) -> PolygonChainRegistry:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    allowance_targets = payload.get("allowance_targets")
    if not isinstance(allowance_targets, dict) or not allowance_targets:
        raise ValueError("chain_registry allowance_targets must be a non-empty object")
    native_gas = payload.get("native_gas")
    usdc_e = payload.get("usdc_e")
    if not isinstance(native_gas, dict) or not isinstance(usdc_e, dict):
        raise ValueError("chain_registry native_gas and usdc_e sections are required")
    return PolygonChainRegistry(
        chain_id=int(payload["chain_id"]),
        native_gas_asset_type=str(native_gas["asset_type"]).strip(),
        native_gas_symbol=str(native_gas["symbol"]).strip(),
        native_gas_decimals=int(native_gas["decimals"]),
        usdc_e_asset_type=str(usdc_e["asset_type"]).strip(),
        usdc_e_token_id=str(usdc_e["token_id"]).strip(),
        usdc_e_contract_address=str(usdc_e["contract_address"]).strip(),
        usdc_e_decimals=int(usdc_e["decimals"]),
        allowance_targets={str(key): str(value).strip() for key, value in allowance_targets.items()},
    )


def load_observable_account_capabilities(con) -> list[AccountTradingCapability]:
    rows = con.execute(
        """
        SELECT
            wallet_id,
            wallet_type,
            signature_type,
            funder,
            allowance_targets,
            can_use_relayer,
            can_trade,
            restricted_reason
        FROM capability.account_trading_capabilities
        WHERE can_trade = TRUE
        ORDER BY wallet_id
        """
    ).fetchall()
    return [
        AccountTradingCapability(
            wallet_id=str(row[0]),
            wallet_type=str(row[1]),
            signature_type=int(row[2]),
            funder=str(row[3]),
            allowance_targets=_json_list(row[4]),
            can_use_relayer=bool(row[5]),
            can_trade=bool(row[6]),
            restricted_reason=str(row[7]) if row[7] is not None else None,
        )
        for row in rows
    ]


def build_wallet_state_observations(
    *,
    account_capability: AccountTradingCapability,
    chain_registry: PolygonChainRegistry,
    reader: WalletStateObservationReader,
    observed_at: datetime,
) -> list[ExternalBalanceObservation]:
    normalized_observed_at = _normalize_timestamp(observed_at)
    allowance_targets = _stable_unique_values(account_capability.allowance_targets)
    registry_targets = set(chain_registry.allowance_targets.values())
    unknown_targets = sorted(target for target in allowance_targets if target not in registry_targets)
    if unknown_targets:
        raise ValueError(
            "account capability allowance targets missing from chain registry: "
            + ", ".join(unknown_targets)
        )

    observations = [
        _build_external_observation(
            account_capability=account_capability,
            asset_type=chain_registry.native_gas_asset_type,
            token_id=None,
            market_id=None,
            outcome=None,
            observation_kind=ExternalBalanceObservationKind.WALLET_BALANCE,
            allowance_target=None,
            chain_id=chain_registry.chain_id,
            observation_read=reader.read_native_balance(
                account_capability.funder,
                decimals=chain_registry.native_gas_decimals,
            ),
            observed_at=normalized_observed_at,
        ),
        _build_external_observation(
            account_capability=account_capability,
            asset_type=chain_registry.usdc_e_asset_type,
            token_id=chain_registry.usdc_e_token_id,
            market_id=None,
            outcome=None,
            observation_kind=ExternalBalanceObservationKind.WALLET_BALANCE,
            allowance_target=None,
            chain_id=chain_registry.chain_id,
            observation_read=reader.read_erc20_balance(
                account_capability.funder,
                chain_registry.usdc_e_contract_address,
                decimals=chain_registry.usdc_e_decimals,
            ),
            observed_at=normalized_observed_at,
        ),
    ]

    for allowance_target in allowance_targets:
        observations.append(
            _build_external_observation(
                account_capability=account_capability,
                asset_type=chain_registry.usdc_e_asset_type,
                token_id=chain_registry.usdc_e_token_id,
                market_id=None,
                outcome=None,
                observation_kind=ExternalBalanceObservationKind.TOKEN_ALLOWANCE,
                allowance_target=allowance_target,
                chain_id=chain_registry.chain_id,
                observation_read=reader.read_erc20_allowance(
                    account_capability.funder,
                    allowance_target,
                    chain_registry.usdc_e_contract_address,
                    decimals=chain_registry.usdc_e_decimals,
                ),
                observed_at=normalized_observed_at,
            )
        )
    return observations


def _build_external_observation(
    *,
    account_capability: AccountTradingCapability,
    asset_type: str,
    token_id: str | None,
    market_id: str | None,
    outcome: str | None,
    observation_kind: ExternalBalanceObservationKind,
    allowance_target: str | None,
    chain_id: int,
    observation_read: ObservationRead,
    observed_at: datetime,
) -> ExternalBalanceObservation:
    normalized_observed_at = _normalize_timestamp(observed_at)
    observation_id = stable_object_id(
        "ebalobs",
        {
            "wallet_id": account_capability.wallet_id,
            "funder": account_capability.funder,
            "signature_type": account_capability.signature_type,
            "asset_type": asset_type,
            "token_id": token_id,
            "market_id": market_id,
            "outcome": outcome,
            "observation_kind": observation_kind.value,
            "allowance_target": allowance_target,
            "chain_id": chain_id,
            "observed_at": normalized_observed_at.isoformat(sep=" ", timespec="seconds"),
        },
    )
    return ExternalBalanceObservation(
        observation_id=observation_id,
        wallet_id=account_capability.wallet_id,
        funder=account_capability.funder,
        signature_type=account_capability.signature_type,
        asset_type=asset_type,
        token_id=token_id,
        market_id=market_id,
        outcome=outcome,
        observation_kind=observation_kind,
        allowance_target=allowance_target,
        chain_id=chain_id,
        block_number=observation_read.block_number,
        observed_quantity=observation_read.observed_quantity,
        source=observation_read.source,
        observed_at=normalized_observed_at,
        raw_observation_json=observation_read.raw_observation_json,
    )


def _json_list(value: object) -> list[str]:
    if value is None:
        return []
    decoded = json.loads(str(value))
    if not isinstance(decoded, list):
        raise ValueError("expected JSON array")
    return [str(item) for item in decoded]


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _stable_unique_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
