from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Any

from asterion_core.clients.shared import as_str


@dataclass(frozen=True)
class QuoteStateRow:
    market_id: str
    token_id: str
    best_bid: float | None
    best_ask: float | None
    last_received_at_ms: int | None


@dataclass(frozen=True)
class MinuteBBOQuoteRow:
    minute_ts_ms: int
    market_id: str
    token_id: str
    best_bid: float | None
    best_ask: float | None
    mid: float | None
    spread: float | None
    updates_count: int
    last_received_at_ms: int | None
    quote_delay_p50_ms: float | None
    quote_delay_p90_ms: float | None
    missing_partition: bool


@dataclass(frozen=True)
class MinuteCoverageRow:
    minute_ts_ms: int
    assets_total: int | None
    assets_seen_minute: int
    assets_seen: int
    ws_coverage: float | None
    events_count: int
    quote_delay_p50_ms: float | None
    quote_delay_p90_ms: float | None
    missing_partition: bool


@dataclass(frozen=True)
class MinuteAggregationResult:
    minute_ts_ms: int
    bbo_rows: list[MinuteBBOQuoteRow]
    coverage_row: MinuteCoverageRow
    next_state: list[QuoteStateRow]


def floor_minute_ts_ms(ts_ms: int) -> int:
    return (int(ts_ms) // 60_000) * 60_000


def utc_date_from_ms(ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ms / 1000.0))


def utc_minute_hhmm(ms: int) -> str:
    return time.strftime("%H%M", time.gmtime(ms / 1000.0))


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_price_levels(raw: Any, *, side: str) -> list[float]:
    if not isinstance(raw, dict):
        return []
    levels = raw.get(side)
    if not isinstance(levels, list):
        return []
    prices: list[float] = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = _to_float(level.get("price"))
        if price is not None:
            prices.append(price)
    return prices


def _extract_best_bid(event: dict[str, Any]) -> float | None:
    direct = _to_float(event.get("best_bid"))
    if direct is not None:
        return direct
    raw = event.get("raw")
    prices = _extract_price_levels(raw, side="bids")
    return max(prices) if prices else None


def _extract_best_ask(event: dict[str, Any]) -> float | None:
    direct = _to_float(event.get("best_ask"))
    if direct is not None:
        return direct
    raw = event.get("raw")
    prices = _extract_price_levels(raw, side="asks")
    return min(prices) if prices else None


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return ordered[lower]
    weight = pos - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _normalize_quote_event(event: dict[str, Any]) -> dict[str, Any] | None:
    market_id = as_str(event.get("market_id"))
    token_id = as_str(event.get("token_id")) or as_str(event.get("asset_id"))
    received_at_ms = _to_int(event.get("received_at_ms"))
    timestamp_ms = _to_int(event.get("timestamp_ms"))
    effective_ts = received_at_ms if received_at_ms is not None else timestamp_ms
    if not market_id or not token_id or effective_ts is None:
        return None

    return {
        "market_id": market_id,
        "token_id": token_id,
        "received_at_ms": received_at_ms,
        "timestamp_ms": timestamp_ms,
        "effective_ts": effective_ts,
        "best_bid": _extract_best_bid(event),
        "best_ask": _extract_best_ask(event),
    }


def aggregate_quote_minute(
    *,
    minute_ts_ms: int,
    events: list[dict[str, Any]],
    prior_state: list[QuoteStateRow] | None = None,
    assets_total: int | None = None,
) -> MinuteAggregationResult:
    normalized_events = []
    for event in events:
        normalized = _normalize_quote_event(event)
        if normalized is None:
            continue
        if floor_minute_ts_ms(normalized["effective_ts"]) != int(minute_ts_ms):
            continue
        normalized_events.append(normalized)

    state_map = {(row.market_id, row.token_id): row for row in (prior_state or [])}
    per_key_events: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for event in normalized_events:
        key = (event["market_id"], event["token_id"])
        per_key_events.setdefault(key, []).append(event)

    for key, key_events in per_key_events.items():
        latest = max(key_events, key=lambda item: item["effective_ts"])
        previous = state_map.get(key)
        last_received_at_ms = latest["received_at_ms"] or latest["timestamp_ms"]
        next_row = QuoteStateRow(
            market_id=key[0],
            token_id=key[1],
            best_bid=latest["best_bid"],
            best_ask=latest["best_ask"],
            last_received_at_ms=last_received_at_ms,
        )
        if previous and previous.last_received_at_ms is not None and last_received_at_ms is not None:
            if last_received_at_ms < previous.last_received_at_ms:
                continue
        state_map[key] = next_row

    next_state = sorted(state_map.values(), key=lambda row: (row.market_id, row.token_id))
    bbo_rows: list[MinuteBBOQuoteRow] = []
    minute_delay_values: list[float] = []

    for row in next_state:
        key = (row.market_id, row.token_id)
        key_events = per_key_events.get(key, [])
        delay_values = [
            float(event["received_at_ms"] - event["timestamp_ms"])
            for event in key_events
            if event["received_at_ms"] is not None and event["timestamp_ms"] is not None
        ]
        minute_delay_values.extend(delay_values)
        mid = None
        spread = None
        if row.best_bid is not None and row.best_ask is not None:
            mid = (row.best_bid + row.best_ask) / 2.0
            spread = row.best_ask - row.best_bid
        bbo_rows.append(
            MinuteBBOQuoteRow(
                minute_ts_ms=int(minute_ts_ms),
                market_id=row.market_id,
                token_id=row.token_id,
                best_bid=row.best_bid,
                best_ask=row.best_ask,
                mid=mid,
                spread=spread,
                updates_count=len(key_events),
                last_received_at_ms=row.last_received_at_ms,
                quote_delay_p50_ms=_quantile(delay_values, 0.5),
                quote_delay_p90_ms=_quantile(delay_values, 0.9),
                missing_partition=False,
            )
        )

    assets_seen_minute = len({event["token_id"] for event in normalized_events})
    assets_seen = sum(1 for row in next_state if row.best_bid is not None and row.best_ask is not None)
    ws_coverage = None
    if assets_total is not None and assets_total > 0:
        ws_coverage = float(assets_seen) / float(assets_total)

    coverage_row = MinuteCoverageRow(
        minute_ts_ms=int(minute_ts_ms),
        assets_total=assets_total,
        assets_seen_minute=assets_seen_minute,
        assets_seen=assets_seen,
        ws_coverage=ws_coverage,
        events_count=len(normalized_events),
        quote_delay_p50_ms=_quantile(minute_delay_values, 0.5),
        quote_delay_p90_ms=_quantile(minute_delay_values, 0.9),
        missing_partition=not next_state,
    )

    return MinuteAggregationResult(
        minute_ts_ms=int(minute_ts_ms),
        bbo_rows=bbo_rows,
        coverage_row=coverage_row,
        next_state=next_state,
    )
