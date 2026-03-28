from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from asterion_core.clients.clob_public import (
    parse_fee_rate_bps,
    parse_min_order_size,
    parse_neg_risk,
    parse_tick_size,
)
from asterion_core.contracts import AccountTradingCapability, MarketCapability, WeatherMarket
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig
from domains.weather.spec import load_weather_markets_for_rule2spec


CAPABILITY_MARKET_COLUMNS = [
    "token_id",
    "market_id",
    "condition_id",
    "outcome",
    "tick_size",
    "fee_rate_bps",
    "neg_risk",
    "min_order_size",
    "tradable",
    "fees_enabled",
    "data_sources",
    "updated_at",
]

CAPABILITY_ACCOUNT_COLUMNS = [
    "wallet_id",
    "wallet_type",
    "signature_type",
    "funder",
    "allowance_targets",
    "can_use_relayer",
    "can_trade",
    "restricted_reason",
    "updated_at",
]

_SUPPORTED_MARKET_OVERRIDE_FIELDS = {
    "tradable",
    "tick_size",
    "fee_rate_bps",
    "min_order_size",
    "neg_risk",
    "fees_enabled",
}
_SUPPORTED_ACCOUNT_OVERRIDE_FIELDS = {"signature_type", "can_use_relayer", "blocked", "reason"}


@dataclass(frozen=True)
class WalletRegistryEntry:
    wallet_id: str
    wallet_type: str
    signature_type: int
    funder: str
    can_use_relayer: bool
    allowance_targets: list[str]
    enabled: bool

    def __post_init__(self) -> None:
        if not self.wallet_id or not self.wallet_type or not self.funder:
            raise ValueError("wallet registry entry requires wallet_id, wallet_type, and funder")
        if self.signature_type < 0:
            raise ValueError("wallet registry signature_type must be non-negative")


@dataclass(frozen=True)
class ChainAccountCapabilityState:
    approved_targets: list[str]
    can_trade: bool
    restricted_reason: str | None


class ChainAccountCapabilityReader(Protocol):
    def read_account_state(self, wallet_entry: WalletRegistryEntry) -> ChainAccountCapabilityState:
        ...


class SafeDefaultChainAccountCapabilityReader:
    def read_account_state(self, wallet_entry: WalletRegistryEntry) -> ChainAccountCapabilityState:
        del wallet_entry
        return ChainAccountCapabilityState(
            approved_targets=[],
            can_trade=False,
            restricted_reason="chain_read_unconfigured",
        )


def load_wallet_registry(path: str | Path) -> list[WalletRegistryEntry]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    wallets = payload.get("wallets")
    if not isinstance(wallets, list):
        raise ValueError("wallet_registry wallets must be a list")
    entries: list[WalletRegistryEntry] = []
    seen: set[str] = set()
    for raw in wallets:
        if not isinstance(raw, dict):
            raise ValueError("wallet_registry entries must be objects")
        entry = WalletRegistryEntry(
            wallet_id=str(raw["wallet_id"]).strip(),
            wallet_type=str(raw["wallet_type"]).strip(),
            signature_type=int(raw["signature_type"]),
            funder=str(raw["funder"]).strip(),
            can_use_relayer=bool(raw["can_use_relayer"]),
            allowance_targets=_normalize_str_list(raw.get("allowance_targets")),
            enabled=bool(raw.get("enabled", True)),
        )
        if entry.wallet_id in seen:
            raise ValueError(f"duplicate wallet_registry wallet_id={entry.wallet_id}")
        seen.add(entry.wallet_id)
        entries.append(entry)
    return entries


def load_weather_markets_for_capability_refresh(con) -> list[WeatherMarket]:
    markets = load_weather_markets_for_rule2spec(con, active_only=True)
    return [market for market in markets if market.active and not market.closed and not market.archived]


def load_capability_overrides(con, *, scope: str) -> dict[str, dict[str, str]]:
    rows = con.execute(
        """
        SELECT scope_key, field_name, value
        FROM capability.capability_overrides
        WHERE scope = ?
        ORDER BY created_at DESC, override_id DESC
        """,
        [scope],
    ).fetchall()
    overrides: dict[str, dict[str, str]] = {}
    for scope_key, field_name, value in rows:
        bucket = overrides.setdefault(str(scope_key), {})
        bucket.setdefault(str(field_name), str(value))
    return overrides


def build_market_capability_from_sources(
    *,
    market: WeatherMarket,
    token_id: str,
    outcome: str,
    book_summary: dict[str, Any],
    fee_rate_payload: dict[str, Any],
    override_values: dict[str, str] | None,
    observed_at: datetime,
) -> MarketCapability:
    overrides = override_values or {}
    _validate_override_fields(overrides, supported_fields=_SUPPORTED_MARKET_OVERRIDE_FIELDS, scope="token_id")
    normalized_outcome = _normalize_binary_outcome(outcome)
    raw_market = market.raw_market if isinstance(market.raw_market, dict) else {}
    used_gamma_market_fallback = False
    fees_enabled = _override_bool(overrides.get("fees_enabled"))
    if fees_enabled is None:
        fees_enabled = _extract_bool(raw_market, "feesEnabled", "fees_enabled")
    if fees_enabled is None:
        fees_enabled = True

    tick_size = _override_decimal(overrides.get("tick_size"))
    if tick_size is None:
        try:
            tick_size = parse_tick_size(book_summary)
        except Exception:  # noqa: BLE001
            tick_size = _extract_decimal(raw_market, "orderPriceMinTickSize", "tick_size", "tickSize")
            if tick_size is not None:
                used_gamma_market_fallback = True
    if tick_size is None:
        raise ValueError("tick_size is required for market capability")

    fee_rate_bps = _override_int(overrides.get("fee_rate_bps"))
    if fee_rate_bps is None:
        try:
            fee_rate_bps = parse_fee_rate_bps(fee_rate_payload)
        except Exception:  # noqa: BLE001
            fee_rate_bps = _extract_int(raw_market, "fee_rate_bps", "feeRateBps", "fee_rate")
            if fee_rate_bps is None and not fees_enabled:
                fee_rate_bps = 0
            if fee_rate_bps is not None:
                used_gamma_market_fallback = True
    if fee_rate_bps is None:
        raise ValueError("fee_rate_bps is required for market capability")

    min_order_size = _override_decimal(overrides.get("min_order_size"))
    if min_order_size is None:
        try:
            min_order_size = parse_min_order_size(book_summary)
        except Exception:  # noqa: BLE001
            min_order_size = _extract_decimal(raw_market, "orderMinSize", "min_order_size", "minOrderSize")
            if min_order_size is not None:
                used_gamma_market_fallback = True
    if min_order_size is None:
        raise ValueError("min_order_size is required for market capability")

    neg_risk = _override_bool(overrides.get("neg_risk"))
    if neg_risk is None:
        try:
            neg_risk = parse_neg_risk(book_summary)
        except Exception:  # noqa: BLE001
            neg_risk = _extract_bool(raw_market, "negRisk", "neg_risk")
            if neg_risk is not None:
                used_gamma_market_fallback = True
    if neg_risk is None:
        raise ValueError("neg_risk is required for market capability")
    tradable_override = _override_bool(overrides.get("tradable"))
    tradable = (
        market.active
        and not market.closed
        and not market.archived
        and bool(market.accepting_orders)
        and bool(market.enable_order_book)
    )
    if tradable_override is not None:
        tradable = tradable_override
    data_sources = ["gamma", "clob_public"]
    if used_gamma_market_fallback:
        data_sources.append("gamma_market_fallback")
    if overrides:
        data_sources.append("capability_overrides")
    return MarketCapability(
        market_id=market.market_id,
        condition_id=market.condition_id,
        token_id=token_id,
        outcome=normalized_outcome,
        tick_size=tick_size,
        fee_rate_bps=fee_rate_bps,
        neg_risk=neg_risk,
        min_order_size=min_order_size,
        tradable=tradable,
        fees_enabled=fees_enabled,
        data_sources=data_sources,
        updated_at=_normalize_timestamp(observed_at),
    )


def build_account_capability_from_sources(
    *,
    wallet_entry: WalletRegistryEntry,
    chain_state: ChainAccountCapabilityState,
    override_values: dict[str, str] | None,
) -> AccountTradingCapability:
    overrides = override_values or {}
    _validate_override_fields(overrides, supported_fields=_SUPPORTED_ACCOUNT_OVERRIDE_FIELDS, scope="wallet_id")
    signature_type = _override_int(overrides.get("signature_type"))
    can_use_relayer = _override_bool(overrides.get("can_use_relayer"))
    blocked = _override_bool(overrides.get("blocked")) or False
    restricted_reason = overrides.get("reason") or chain_state.restricted_reason or None
    return AccountTradingCapability(
        wallet_id=wallet_entry.wallet_id,
        wallet_type=wallet_entry.wallet_type,
        signature_type=signature_type if signature_type is not None else wallet_entry.signature_type,
        funder=wallet_entry.funder,
        allowance_targets=list(chain_state.approved_targets),
        can_use_relayer=can_use_relayer if can_use_relayer is not None else wallet_entry.can_use_relayer,
        can_trade=wallet_entry.enabled and chain_state.can_trade and not blocked,
        restricted_reason=restricted_reason,
    )


def refresh_market_capabilities(
    con,
    *,
    clob_client,
    observed_at: datetime | None = None,
) -> list[MarketCapability]:
    now = _normalize_timestamp(observed_at or datetime.now(UTC))
    token_overrides = load_capability_overrides(con, scope="token_id")
    capabilities: list[MarketCapability] = []
    for market in load_weather_markets_for_capability_refresh(con):
        pairs = expand_market_tokens(market)
        for token_id, outcome in pairs:
            raw_market = market.raw_market if isinstance(market.raw_market, dict) else {}
            override_values = token_overrides.get(token_id)
            if _raw_market_capability_is_sufficient(raw_market, override_values=override_values):
                book_summary = {}
                fee_rate_payload = {}
            else:
                try:
                    book_summary = clob_client.fetch_book_summary(token_id)
                except Exception:  # noqa: BLE001
                    book_summary = {}
                try:
                    fee_rate_payload = clob_client.fetch_fee_rate(token_id)
                except Exception:  # noqa: BLE001
                    fee_rate_payload = {}
            capabilities.append(
                build_market_capability_from_sources(
                    market=market,
                    token_id=token_id,
                    outcome=outcome,
                    book_summary=book_summary,
                    fee_rate_payload=fee_rate_payload,
                    override_values=override_values,
                    observed_at=now,
                )
            )
    return capabilities


def refresh_account_capabilities(
    con,
    *,
    wallet_registry_path: str | Path,
    chain_reader: ChainAccountCapabilityReader,
) -> list[AccountTradingCapability]:
    wallet_overrides = load_capability_overrides(con, scope="wallet_id")
    capabilities: list[AccountTradingCapability] = []
    for wallet_entry in load_wallet_registry(wallet_registry_path):
        chain_state = chain_reader.read_account_state(wallet_entry)
        capabilities.append(
            build_account_capability_from_sources(
                wallet_entry=wallet_entry,
                chain_state=chain_state,
                override_values=wallet_overrides.get(wallet_entry.wallet_id),
            )
        )
    return capabilities


def enqueue_market_capability_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    capabilities: list[MarketCapability],
    run_id: str | None = None,
) -> str | None:
    if not capabilities:
        return None
    rows = [market_capability_to_row(capability) for capability in capabilities]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="capability.market_capabilities",
        pk_cols=["token_id"],
        columns=list(CAPABILITY_MARKET_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def enqueue_account_capability_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    capabilities: list[AccountTradingCapability],
    observed_at: datetime | None = None,
    run_id: str | None = None,
) -> str | None:
    if not capabilities:
        return None
    now = _normalize_timestamp(observed_at or datetime.now(UTC))
    rows = [account_capability_to_row(capability, observed_at=now) for capability in capabilities]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="capability.account_trading_capabilities",
        pk_cols=["wallet_id"],
        columns=list(CAPABILITY_ACCOUNT_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def market_capability_to_row(record: MarketCapability) -> list[object]:
    return [
        record.token_id,
        record.market_id,
        record.condition_id,
        record.outcome,
        str(record.tick_size),
        record.fee_rate_bps,
        record.neg_risk,
        str(record.min_order_size),
        record.tradable,
        record.fees_enabled,
        safe_json_dumps(record.data_sources),
        _sql_timestamp(record.updated_at),
    ]


def account_capability_to_row(record: AccountTradingCapability, *, observed_at: datetime) -> list[object]:
    return [
        record.wallet_id,
        record.wallet_type,
        record.signature_type,
        record.funder,
        safe_json_dumps(record.allowance_targets),
        record.can_use_relayer,
        record.can_trade,
        record.restricted_reason,
        _sql_timestamp(observed_at),
    ]


def expand_market_tokens(market: WeatherMarket) -> list[tuple[str, str]]:
    outcomes = [_normalize_binary_outcome(value) for value in market.outcomes]
    token_ids = [str(value).strip() for value in market.token_ids if str(value).strip()]
    if len(outcomes) != len(token_ids):
        raise ValueError(f"weather market outcomes/token_ids mismatch for market_id={market.market_id}")
    return list(zip(token_ids, outcomes, strict=True))


def _validate_override_fields(values: dict[str, str], *, supported_fields: set[str], scope: str) -> None:
    unknown = sorted(set(values) - supported_fields)
    if unknown:
        raise ValueError(f"unsupported capability override fields for {scope}: {', '.join(unknown)}")


def _normalize_binary_outcome(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in {"YES", "NO"}:
        raise ValueError(f"unsupported binary outcome={value!r}")
    return normalized


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _sql_timestamp(value: datetime) -> str:
    normalized = _normalize_timestamp(value)
    return normalized.isoformat(sep=" ", timespec="seconds")


def _normalize_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        out = [str(item).strip() for item in value if str(item).strip()]
        return out
    return []


def _override_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean override value={value!r}")


def _override_int(value: str | None) -> int | None:
    if value is None:
        return None
    return int(value.strip())


def _extract_decimal(payload: dict[str, Any], *field_names: str) -> Decimal | None:
    for field_name in field_names:
        value = payload.get(field_name)
        if value is None:
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float, Decimal)):
            return Decimal(str(value))
        if isinstance(value, str) and value.strip():
            return Decimal(value.strip())
    return None


def _extract_int(payload: dict[str, Any], *field_names: str) -> int | None:
    parsed = _extract_decimal(payload, *field_names)
    if parsed is None:
        return None
    return int(parsed)


def _extract_bool(payload: dict[str, Any], *field_names: str) -> bool | None:
    for field_name in field_names:
        value = payload.get(field_name)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
    return None


def _raw_market_capability_is_sufficient(
    raw_market: dict[str, Any],
    *,
    override_values: dict[str, str] | None,
) -> bool:
    if not raw_market:
        return False
    if _override_decimal((override_values or {}).get("tick_size")) is None and _extract_decimal(
        raw_market, "orderPriceMinTickSize", "tick_size", "tickSize"
    ) is None:
        return False
    if _override_decimal((override_values or {}).get("min_order_size")) is None and _extract_decimal(
        raw_market, "orderMinSize", "min_order_size", "minOrderSize"
    ) is None:
        return False
    if _override_bool((override_values or {}).get("neg_risk")) is None and _extract_bool(
        raw_market, "negRisk", "neg_risk"
    ) is None:
        return False
    if _override_int((override_values or {}).get("fee_rate_bps")) is not None:
        return True
    fees_enabled = _override_bool((override_values or {}).get("fees_enabled"))
    if fees_enabled is None:
        fees_enabled = _extract_bool(raw_market, "feesEnabled", "fees_enabled")
    if fees_enabled is False:
        return True
    return _extract_int(raw_market, "fee_rate_bps", "feeRateBps", "fee_rate") is not None


    parsed = int(value)
    if parsed < 0:
        raise ValueError("override integer value must be non-negative")
    return parsed


def _override_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    parsed = Decimal(value)
    if parsed <= 0:
        raise ValueError("override decimal value must be positive")
    return parsed
