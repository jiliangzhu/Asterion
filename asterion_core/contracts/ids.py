from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def new_request_id() -> str:
    return _new_prefixed_id("req")


def new_client_order_id() -> str:
    return _new_prefixed_id("ord")


def new_reservation_id() -> str:
    return _new_prefixed_id("res")


def new_proposal_id() -> str:
    return _new_prefixed_id("prop")


def new_event_id() -> str:
    return _new_prefixed_id("evt")


def stable_object_id(prefix: str, payload: dict[str, Any]) -> str:
    digest = hashlib.sha256(_stable_json_bytes(payload)).hexdigest()[:24]
    clean_prefix = prefix.strip().lower()
    if not clean_prefix:
        raise ValueError("prefix is required")
    return f"{clean_prefix}_{digest}"


@dataclass(frozen=True)
class ForecastCacheKey:
    market_id: str
    station_id: str
    spec_version: str
    source: str
    model_run: str
    forecast_target_time: datetime

    def as_string(self) -> str:
        payload = {
            "forecast_target_time": _utc_isoformat(self.forecast_target_time),
            "market_id": self.market_id,
            "model_run": self.model_run,
            "source": self.source,
            "spec_version": self.spec_version,
            "station_id": self.station_id,
        }
        return stable_object_id("fck", payload)


def build_forecast_cache_key(
    *,
    market_id: str,
    station_id: str,
    spec_version: str,
    source: str,
    model_run: str,
    forecast_target_time: datetime,
) -> str:
    return ForecastCacheKey(
        market_id=market_id,
        station_id=station_id,
        spec_version=spec_version,
        source=source,
        model_run=model_run,
        forecast_target_time=forecast_target_time,
    ).as_string()


def _new_prefixed_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _stable_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        _normalize_for_json(payload),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _normalize_for_json(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return _utc_isoformat(value)
    if isinstance(value, dict):
        return {str(k): _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_json(item) for item in value]
    return str(value)


def _utc_isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

