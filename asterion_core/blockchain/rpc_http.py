from __future__ import annotations

import json
import os
from typing import Any


def load_polygon_rpc_headers() -> dict[str, str]:
    raw_headers_json = str(os.getenv("ASTERION_POLYGON_RPC_HEADERS_JSON") or "").strip()
    if raw_headers_json:
        payload = json.loads(raw_headers_json)
        if not isinstance(payload, dict) or not payload:
            raise ValueError("ASTERION_POLYGON_RPC_HEADERS_JSON must be a non-empty JSON object")
        headers: dict[str, str] = {}
        for key, value in payload.items():
            header_name = str(key).strip()
            header_value = str(value).strip()
            if not header_name or not header_value:
                raise ValueError("ASTERION_POLYGON_RPC_HEADERS_JSON cannot contain empty header keys or values")
            headers[header_name] = header_value
        return headers

    header_name = str(os.getenv("ASTERION_POLYGON_RPC_HEADER_NAME") or "").strip()
    header_value = str(os.getenv("ASTERION_POLYGON_RPC_HEADER_VALUE") or "").strip()
    api_key = str(os.getenv("ASTERION_POLYGON_RPC_API_KEY") or "").strip()
    if api_key:
        if header_name and header_name.lower() != "x-api-key":
            raise ValueError("ASTERION_POLYGON_RPC_API_KEY cannot be combined with a non x-api-key header name")
        header_name = "x-api-key"
        header_value = api_key

    if not header_name and not header_value:
        return {}
    if not header_name or not header_value:
        raise ValueError("ASTERION_POLYGON_RPC_HEADER_NAME and ASTERION_POLYGON_RPC_HEADER_VALUE must both be set")
    return {header_name: header_value}


def build_polygon_rpc_request_kwargs(*, timeout_seconds: float = 10.0) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"timeout": float(timeout_seconds)}
    headers = load_polygon_rpc_headers()
    if headers:
        kwargs["headers"] = headers
    return kwargs
