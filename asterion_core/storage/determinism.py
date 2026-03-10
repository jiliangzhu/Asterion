from __future__ import annotations

import hashlib
import json
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def now_ts_ms() -> int:
    return int(time.time() * 1000)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonicalize_json(obj: Any, *, float_ndigits: int = 6) -> Any:
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        return obj
    if isinstance(obj, float):
        return round(obj, float_ndigits)
    if isinstance(obj, Decimal):
        return str(obj.normalize())
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return {str(k): _canonicalize_json(v, float_ndigits=float_ndigits) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_canonicalize_json(v, float_ndigits=float_ndigits) for v in obj]
    return str(obj)


def canonical_json_dumps(obj: Any, *, float_ndigits: int = 6) -> str:
    canon = _canonicalize_json(obj, float_ndigits=float_ndigits)
    return json.dumps(canon, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def canonical_json_bytes(obj: Any, *, float_ndigits: int = 6) -> bytes:
    return canonical_json_dumps(obj, float_ndigits=float_ndigits).encode("utf-8")


def stable_payload_sha256(obj: Any, *, float_ndigits: int = 6) -> str:
    return sha256_hex(canonical_json_bytes(obj, float_ndigits=float_ndigits))

