from __future__ import annotations

from typing import Any
from urllib.parse import urlencode


def as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def build_url(base_url: str, endpoint: str, params: dict[str, Any] | None = None) -> str:
    base = str(base_url or "").rstrip("/")
    path = str(endpoint or "").lstrip("/")
    url = f"{base}/{path}" if path else base
    if not params:
        return url
    filtered = {key: value for key, value in params.items() if value is not None}
    if not filtered:
        return url
    return f"{url}?{urlencode(filtered, doseq=True)}"


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("items", "data", "markets", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []
