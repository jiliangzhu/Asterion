from __future__ import annotations

import time
from typing import Any, Callable

from .shared import build_url, extract_items

TimestampFn = Callable[[dict[str, Any], list[str] | None], int | None]


def _try_fetch_items(url: str, *, client, context: dict[str, Any]) -> list[dict[str, Any]]:
    payload = client.get_json(url, context=context)
    return extract_items(payload)


def fetch_all_pages(
    *,
    base_url: str,
    endpoint: str,
    market_param: str,
    market_id: str,
    limit: int,
    max_pages: int,
    sleep_s: float,
    watermark_ms: int | None,
    since_ms: int | None,
    extra_params: dict[str, Any] | None,
    param_candidates: list[str],
    client,
    timestamp_fn: TimestampFn,
    require_timestamp: bool = True,
    timestamp_keys: list[str] | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    accepted_param = market_param
    items_accum: list[dict[str, Any]] = []
    max_seen_ts: int | None = None
    params_base = dict(extra_params or {})
    floor_ts = max(value for value in [watermark_ms, since_ms] if value is not None) if any(
        value is not None for value in [watermark_ms, since_ms]
    ) else None

    for candidate in [market_param, *[param for param in param_candidates if param != market_param]]:
        accepted_param = candidate
        items_accum = []
        max_seen_ts = None
        accepted = False

        for page in range(max_pages):
            params = dict(params_base)
            params[accepted_param] = market_id
            params["limit"] = int(limit)
            params["offset"] = page * int(limit)
            url = build_url(base_url, endpoint, params)
            context = {"endpoint": endpoint, "params": params, "page": page, "market_id": market_id}

            try:
                items = _try_fetch_items(url, client=client, context=context)
                accepted = True
            except Exception:
                items_accum = []
                max_seen_ts = None
                accepted = False
                break

            if not items:
                break

            for item in items:
                ts_ms = timestamp_fn(item, timestamp_keys)
                if ts_ms is None:
                    if require_timestamp:
                        continue
                    items_accum.append(item)
                    continue
                if floor_ts is not None and ts_ms < floor_ts:
                    continue
                items_accum.append(item)
                if max_seen_ts is None or ts_ms > max_seen_ts:
                    max_seen_ts = ts_ms

            if len(items) < int(limit):
                break
            if sleep_s > 0:
                time.sleep(sleep_s)

        if accepted:
            break

    return items_accum, max_seen_ts
