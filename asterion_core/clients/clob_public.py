from __future__ import annotations

from decimal import Decimal
from typing import Any

from .shared import build_url


class ClobPublicClient:
    def __init__(
        self,
        *,
        client: Any,
        base_url: str = "https://clob.polymarket.com",
        book_endpoint: str = "/book",
        fee_rate_endpoint: str = "/fee-rate",
    ) -> None:
        self._client = client
        self._base_url = base_url
        self._book_endpoint = book_endpoint
        self._fee_rate_endpoint = fee_rate_endpoint

    def fetch_book_summary(self, token_id: str) -> dict[str, Any]:
        url = build_url(self._base_url, self._book_endpoint, {"token_id": token_id})
        payload = self._client.get_json(url, context={"endpoint": self._book_endpoint, "token_id": token_id})
        if not isinstance(payload, dict):
            raise ValueError("clob book summary payload must be a dictionary")
        return payload

    def fetch_fee_rate(self, token_id: str) -> dict[str, Any]:
        url = build_url(self._base_url, self._fee_rate_endpoint, {"token_id": token_id})
        payload = self._client.get_json(url, context={"endpoint": self._fee_rate_endpoint, "token_id": token_id})
        if not isinstance(payload, dict):
            raise ValueError("clob fee-rate payload must be a dictionary")
        return payload


def parse_tick_size(payload: dict[str, Any]) -> Decimal:
    return _parse_decimal_field(payload, "tick_size", "tickSize")


def parse_min_order_size(payload: dict[str, Any]) -> Decimal:
    return _parse_decimal_field(payload, "min_order_size", "minOrderSize")


def parse_neg_risk(payload: dict[str, Any]) -> bool:
    value = _find_field(payload, "neg_risk", "negRisk")
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
    raise ValueError("clob neg_risk field is required")


def parse_fee_rate_bps(payload: dict[str, Any]) -> int:
    value = _find_field(payload, "fee_rate_bps", "feeRateBps", "fee_rate")
    if isinstance(value, bool):
        raise ValueError("clob fee rate must be numeric")
    if isinstance(value, int):
        if value < 0:
            raise ValueError("clob fee rate must be non-negative")
        return value
    if isinstance(value, float):
        if value < 0:
            raise ValueError("clob fee rate must be non-negative")
        return int(value)
    if isinstance(value, str) and value.strip():
        parsed = Decimal(value.strip())
        if parsed < 0:
            raise ValueError("clob fee rate must be non-negative")
        return int(parsed)
    raise ValueError("clob fee rate field is required")


def _parse_decimal_field(payload: dict[str, Any], *field_names: str) -> Decimal:
    value = _find_field(payload, *field_names)
    if isinstance(value, bool):
        raise ValueError(f"clob field must be numeric: {field_names[0]}")
    if isinstance(value, (int, float, Decimal)):
        parsed = Decimal(str(value))
    elif isinstance(value, str) and value.strip():
        parsed = Decimal(value.strip())
    else:
        raise ValueError(f"clob field is required: {field_names[0]}")
    if parsed <= 0:
        raise ValueError(f"clob field must be positive: {field_names[0]}")
    return parsed


def _find_field(payload: dict[str, Any], *field_names: str) -> Any:
    for name in field_names:
        if name in payload:
            return payload[name]
    for container_key in ("book", "data", "result"):
        nested = payload.get(container_key)
        if isinstance(nested, dict):
            for name in field_names:
                if name in nested:
                    return nested[name]
    return None
