from __future__ import annotations

import time
from typing import Any

from .shared import as_str, build_url, extract_items


def infer_condition_id(market: dict[str, Any]) -> str | None:
    for key in ("conditionId", "condition_id", "conditionID"):
        value = market.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def iter_event_dicts(market: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key in ("event", "events"):
        value = market.get(key)
        if isinstance(value, dict):
            out.append(value)
        elif isinstance(value, list):
            out.extend(item for item in value if isinstance(item, dict))
    return out


def extract_event_id(event: dict[str, Any]) -> str | None:
    for key in ("id", "eventId", "event_id"):
        value = event.get(key)
        s = as_str(value)
        if s:
            return s
    return None


def scan_gamma_markets(
    *,
    base_url: str,
    markets_endpoint: str,
    page_limit: int,
    max_pages: int,
    sleep_s: float,
    active_only: bool,
    closed: bool | None,
    archived: bool | None,
    universe_ids: set[str],
    client,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    found: dict[str, dict[str, Any]] = {}
    raw_events: dict[str, dict[str, Any]] = {}

    for page in range(max_pages):
        params = {
            "limit": page_limit,
            "offset": page * page_limit,
            "active": True if active_only else None,
            "closed": closed,
            "archived": archived,
        }
        url = build_url(base_url, markets_endpoint, params)
        try:
            payload = client.get_json(url, context={"endpoint": markets_endpoint, "params": params, "page": page})
            items = extract_items(payload)
        except Exception:
            continue
        if not items:
            break

        for market in items:
            market_id = infer_condition_id(market)
            if not market_id or market_id not in universe_ids or market_id in found:
                continue
            found[market_id] = market
            events = iter_event_dicts(market)
            if events:
                event = events[0]
                event_id = extract_event_id(event)
                if event_id and event_id not in raw_events:
                    raw_events[event_id] = event

        if len(found) >= len(universe_ids):
            break
        if sleep_s > 0:
            time.sleep(sleep_s)

    return found, raw_events
