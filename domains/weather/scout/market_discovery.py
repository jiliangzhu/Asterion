from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from asterion_core.clients.gamma import extract_event_id, infer_condition_id, iter_event_dicts
from asterion_core.clients.shared import as_str, build_url, extract_items
from asterion_core.contracts import WeatherMarket
from asterion_core.storage.os_queue import enqueue_upsert_rows_v1
from asterion_core.storage.utils import safe_json_dumps
from asterion_core.storage.write_queue import WriteQueueConfig


WEATHER_MARKET_COLUMNS = [
    "market_id",
    "condition_id",
    "event_id",
    "slug",
    "title",
    "description",
    "rules",
    "status",
    "active",
    "closed",
    "archived",
    "accepting_orders",
    "enable_order_book",
    "tags_json",
    "outcomes_json",
    "token_ids_json",
    "close_time",
    "end_date",
    "raw_market_json",
    "created_at",
    "updated_at",
]


@dataclass(frozen=True)
class WeatherMarketDiscoveryResult:
    discovered_markets: list[WeatherMarket]
    task_id: str | None

    @property
    def discovered_count(self) -> int:
        return len(self.discovered_markets)


def discover_weather_markets(
    *,
    base_url: str,
    markets_endpoint: str,
    page_limit: int,
    max_pages: int,
    sleep_s: float,
    active_only: bool,
    closed: bool | None,
    archived: bool | None,
    tag_slug: str | None = None,
    recent_within_days: int | None = None,
    asof: datetime | None = None,
    client,
) -> list[WeatherMarket]:
    found: dict[str, WeatherMarket] = {}
    now = asof or datetime.now(timezone.utc)

    for page in range(max_pages):
        params = {
            "limit": int(page_limit),
            "offset": page * int(page_limit),
            "active": True if active_only else None,
            "closed": closed,
            "archived": archived,
            "tag_slug": tag_slug,
        }
        url = build_url(base_url, markets_endpoint, params)
        payload = client.get_json(url, context={"endpoint": markets_endpoint, "params": params, "page": page})
        items = _extract_candidate_markets(payload)
        if not items:
            break

        for market in items:
            normalized = normalize_weather_market(market)
            if normalized is None:
                continue
            if not _is_recent_market(normalized, asof=now, recent_within_days=recent_within_days):
                continue
            found.setdefault(normalized.market_id, normalized)

        if len(items) < int(page_limit):
            break
        if sleep_s > 0:
            import time

            time.sleep(sleep_s)

    return [found[key] for key in sorted(found)]


def normalize_weather_market(market: dict[str, Any]) -> WeatherMarket | None:
    market_id = _extract_market_id(market)
    condition_id = infer_condition_id(market)
    title = _extract_title(market)
    if not market_id or not condition_id or not title:
        return None

    tags = _extract_tags(market)
    if not _is_weather_market(title=title, tags=tags, market=market):
        return None

    active = _coerce_bool(market.get("active"), default=False)
    closed = _coerce_bool(market.get("closed"), default=False)
    archived = _coerce_bool(market.get("archived"), default=False)
    status = _derive_status(market=market, active=active, closed=closed, archived=archived)

    return WeatherMarket(
        market_id=market_id,
        condition_id=condition_id,
        event_id=_extract_event_id_from_market(market),
        slug=_first_non_empty(market, "slug", "market_slug"),
        title=title,
        description=_first_non_empty(market, "description", "details"),
        rules=_first_non_empty(market, "rules", "resolutionCriteria", "resolution_criteria"),
        status=status,
        active=active,
        closed=closed,
        archived=archived,
        accepting_orders=_coerce_optional_bool(market.get("acceptingOrders") if "acceptingOrders" in market else market.get("accepting_orders")),
        enable_order_book=_coerce_optional_bool(market.get("enableOrderBook") if "enableOrderBook" in market else market.get("enable_order_book")),
        tags=tags,
        outcomes=_extract_outcomes(market),
        token_ids=_extract_token_ids(market),
        close_time=_parse_datetime(_first_non_empty(market, "closeTime", "close_time", "endDate", "end_date")),
        end_date=_parse_datetime(_first_non_empty(market, "endDate", "end_date", "end_date_iso")),
        raw_market=dict(market),
    )


def enqueue_weather_market_upserts(
    queue_cfg: WriteQueueConfig,
    *,
    markets: list[WeatherMarket],
    run_id: str | None = None,
    asof: datetime | None = None,
) -> str | None:
    if not markets:
        return None
    ts = (asof or datetime.now(timezone.utc)).replace(tzinfo=None)
    rows = [weather_market_to_row(market, observed_at=ts) for market in markets]
    return enqueue_upsert_rows_v1(
        queue_cfg,
        table="weather.weather_markets",
        pk_cols=["market_id"],
        columns=list(WEATHER_MARKET_COLUMNS),
        rows=rows,
        run_id=run_id,
    )


def run_weather_market_discovery(
    *,
    base_url: str,
    markets_endpoint: str,
    page_limit: int,
    max_pages: int,
    sleep_s: float,
    active_only: bool,
    closed: bool | None,
    archived: bool | None,
    tag_slug: str | None = None,
    recent_within_days: int | None = None,
    asof: datetime | None = None,
    client,
    queue_cfg: WriteQueueConfig | None = None,
    run_id: str | None = None,
) -> WeatherMarketDiscoveryResult:
    markets = discover_weather_markets(
        base_url=base_url,
        markets_endpoint=markets_endpoint,
        page_limit=page_limit,
        max_pages=max_pages,
        sleep_s=sleep_s,
        active_only=active_only,
        closed=closed,
        archived=archived,
        tag_slug=tag_slug,
        recent_within_days=recent_within_days,
        asof=asof,
        client=client,
    )
    task_id = None
    if queue_cfg is not None:
        task_id = enqueue_weather_market_upserts(queue_cfg, markets=markets, run_id=run_id)
    return WeatherMarketDiscoveryResult(discovered_markets=markets, task_id=task_id)


def weather_market_to_row(market: WeatherMarket, *, observed_at: datetime) -> list[Any]:
    created_at = _parse_datetime(_first_non_empty(market.raw_market, "createdAt", "created_at"))
    return [
        market.market_id,
        market.condition_id,
        market.event_id,
        market.slug,
        market.title,
        market.description,
        market.rules,
        market.status,
        market.active,
        market.closed,
        market.archived,
        market.accepting_orders,
        market.enable_order_book,
        safe_json_dumps(market.tags),
        safe_json_dumps(market.outcomes),
        safe_json_dumps(market.token_ids),
        _timestamp_sql_value(market.close_time),
        _timestamp_sql_value(market.end_date),
        safe_json_dumps(market.raw_market),
        _timestamp_sql_value(created_at) or _timestamp_sql_value(observed_at),
        _timestamp_sql_value(observed_at),
    ]


def _extract_market_id(market: dict[str, Any]) -> str | None:
    return _first_non_empty(market, "id", "marketId", "market_id")


def _extract_title(market: dict[str, Any]) -> str | None:
    return _first_non_empty(market, "question", "title", "name")


def _extract_event_id_from_market(market: dict[str, Any]) -> str | None:
    events = iter_event_dicts(market)
    if not events:
        return None
    return extract_event_id(events[0])


def _extract_tags(market: dict[str, Any]) -> list[str]:
    raw_values: list[Any] = []
    for key in ("tags", "tag", "categories"):
        if key in market:
            raw_values.append(market.get(key))
    for event in iter_event_dicts(market):
        for key in ("tags", "tag", "category", "subcategory"):
            if key in event:
                raw_values.append(event.get(key))
    tags: list[str] = []
    for value in raw_values:
        tags.extend(_coerce_str_list(value))
    deduped: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        norm = tag.strip()
        if not norm:
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(norm)
    return deduped


def _extract_candidate_markets(payload: Any) -> list[dict[str, Any]]:
    items = extract_items(payload)
    if not items:
        return []
    flattened: list[dict[str, Any]] = []
    for item in items:
        nested_markets = item.get("markets")
        if isinstance(nested_markets, list):
            event_summary = {
                "id": extract_event_id(item),
                "title": _first_non_empty(item, "title", "question", "name"),
                "slug": _first_non_empty(item, "slug"),
                "category": _first_non_empty(item, "category"),
                "subcategory": _first_non_empty(item, "subcategory"),
                "tags": item.get("tags"),
                "description": _first_non_empty(item, "description"),
            }
            for market in nested_markets:
                if not isinstance(market, dict):
                    continue
                merged = dict(market)
                existing_events = iter_event_dicts(merged)
                if existing_events:
                    merged["events"] = [event_summary, *existing_events]
                else:
                    merged["event"] = event_summary
                flattened.append(merged)
            continue
        flattened.append(item)
    return flattened


def _extract_outcomes(market: dict[str, Any]) -> list[str]:
    outcomes = _coerce_str_list(market.get("outcomes"))
    if outcomes:
        return outcomes
    tokens = market.get("tokens")
    if isinstance(tokens, list):
        extracted: list[str] = []
        for item in tokens:
            if not isinstance(item, dict):
                continue
            outcome = _first_non_empty(item, "outcome", "name", "label")
            if outcome:
                extracted.append(outcome)
        if extracted:
            return extracted
    return []


def _extract_token_ids(market: dict[str, Any]) -> list[str]:
    for key in ("clobTokenIds", "clobTokenIdsJson", "tokenIds", "token_ids"):
        if key in market:
            values = _coerce_str_list(market.get(key))
            if values:
                return values
    tokens = market.get("tokens")
    if isinstance(tokens, list):
        extracted: list[str] = []
        for item in tokens:
            if isinstance(item, dict):
                token_id = _first_non_empty(item, "token_id", "tokenId", "id")
                if token_id:
                    extracted.append(token_id)
            else:
                token_id = as_str(item)
                if token_id:
                    extracted.append(token_id)
        return extracted
    return []


def _is_weather_market(*, title: str, tags: list[str], market: dict[str, Any]) -> bool:
    if any(_is_weather_tag(tag) for tag in tags):
        return True
    for event in iter_event_dicts(market):
        for key in ("title", "slug", "category", "subcategory", "description"):
            value = as_str(event.get(key)).lower()
            if _contains_weather_signal(value):
                return True
    return _matches_weather_title(title)


_WEATHER_TITLE_PATTERNS = (
    re.compile(r"(?i)\b(high(?:est)?|low(?:est)?) temperature in .+ on [A-Za-z]+ \d{1,2}\b"),
    re.compile(r"(?i)\bwill the (high(?:est)?|low(?:est)?) temperature in .+\b"),
    re.compile(r"(?i)\bprecipitation in .+ in [A-Za-z]+\b"),
)


def _matches_weather_title(title: str) -> bool:
    lowered = title.lower()
    if "weather" in lowered:
        return True
    return any(pattern.search(title) for pattern in _WEATHER_TITLE_PATTERNS)


def _is_weather_tag(tag: str) -> bool:
    lowered = tag.lower()
    return "weather" in lowered or lowered in {"global temp", "temperature", "weather & science"}


def _contains_weather_signal(value: str) -> bool:
    lowered = value.lower()
    return (
        "weather" in lowered
        or "temperature" in lowered
        or "precipitation" in lowered
        or "hurricane" in lowered
        or "sea ice" in lowered
    )


def _derive_status(*, market: dict[str, Any], active: bool, closed: bool, archived: bool) -> str:
    raw = _first_non_empty(market, "status")
    if raw:
        return raw.lower()
    if archived:
        return "archived"
    if closed:
        return "closed"
    if active:
        return "active"
    return "inactive"


def _is_recent_market(market: WeatherMarket, *, asof: datetime, recent_within_days: int | None) -> bool:
    if recent_within_days is None:
        return True
    if recent_within_days <= 0:
        raise ValueError("recent_within_days must be positive when provided")
    target = market.close_time or market.end_date
    if target is None:
        return False
    normalized_target = _normalize_aware_utc(target)
    normalized_asof = _normalize_aware_utc(asof)
    if normalized_target < normalized_asof:
        return False
    horizon = normalized_asof + timedelta(days=int(recent_within_days))
    return normalized_target <= horizon


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
            except Exception:  # noqa: BLE001
                parsed = None
            if parsed is not None:
                return _coerce_str_list(parsed)
        return [item.strip() for item in stripped.split(",") if item.strip()]
    if isinstance(value, dict):
        label = _first_non_empty(value, "label", "name", "id")
        return [label] if label else []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(_coerce_str_list(item))
        return out
    s = as_str(value)
    return [s] if s else []


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    raw = as_str(value).lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return _coerce_bool(value, default=False)


def _first_non_empty(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        text = as_str(value)
        if text:
            return text
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = as_str(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_naive_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _timestamp_sql_value(value: datetime | None) -> str | None:
    normalized = _to_naive_utc(value)
    if normalized is None:
        return None
    return normalized.isoformat(sep=" ", timespec="seconds")
