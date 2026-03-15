from __future__ import annotations

import argparse
import os
import shlex
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT_ENV_PATH = ROOT / ".env"

_ALLOWED_EXACT_KEYS = {
    "ASTERION_AGENT_MODEL",
    "ASTERION_AGENT_PROVIDER",
    "ASTERION_CONTROLLED_LIVE_CAPABILITY_MANIFEST_PATH",
    "ASTERION_DB_PATH",
    "ASTERION_OPENAI_COMPATIBLE_MODEL",
    "QWEN_MODEL",
}
_ALLOWED_PREFIXES = (
    "ASTERION_UI_",
    "ASTERION_READINESS_",
    "ASTERION_REAL_WEATHER_CHAIN_",
)


def is_allowed_ui_env_key(key: str) -> bool:
    return key in _ALLOWED_EXACT_KEYS or any(key.startswith(prefix) for prefix in _ALLOWED_PREFIXES)


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}
    payload: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue
        payload[normalized_key] = value.strip().strip('"').strip("'")
    return payload


def resolve_ui_runtime_env(*, env_path: str | Path | None = None) -> dict[str, str]:
    path = Path(env_path) if env_path is not None else DEFAULT_PROJECT_ENV_PATH
    payload = {
        key: value
        for key, value in _parse_env_file(path).items()
        if is_allowed_ui_env_key(key)
    }
    for key, value in os.environ.items():
        if is_allowed_ui_env_key(key):
            payload[key] = value
    return dict(sorted(payload.items()))


def export_ui_runtime_env_shell(*, env_path: str | Path | None = None) -> str:
    exports = []
    for key, value in resolve_ui_runtime_env(env_path=env_path).items():
        quoted = shlex.quote(str(value))
        if not quoted.startswith("'"):
            quoted = f"'{quoted}'"
        exports.append(f"export {key}={quoted}")
    return "\n".join(exports)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export the minimal UI runtime environment.")
    parser.add_argument("--env-path", default=str(DEFAULT_PROJECT_ENV_PATH))
    parser.add_argument("--export", action="store_true", help="Print shell export statements.")
    args = parser.parse_args()

    if args.export:
        print(export_ui_runtime_env_shell(env_path=args.env_path))
        return 0
    for key, value in resolve_ui_runtime_env(env_path=args.env_path).items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
