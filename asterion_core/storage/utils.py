from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def ensure_dir(path: str) -> None:
    if path:
        Path(path).mkdir(parents=True, exist_ok=True)


def safe_json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

