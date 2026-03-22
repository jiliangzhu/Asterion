from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
import shlex
from pathlib import Path
from typing import Any


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
_BANNED_PREFIXES = (
    "ASTERION_CONTROLLED_LIVE_SECRET_",
)
_BANNED_EXACT_KEYS = {
    "ALIBABA_API_KEY",
    "ASTERION_OPENAI_COMPATIBLE_API_KEY",
    "OPENAI_API_KEY",
    "QWEN_API_KEY",
}
_DEFAULT_BIND_ADDRESS = "127.0.0.1"


@dataclass(frozen=True)
class UiRuntimeBoundaryStatus:
    status: str
    bind_address: str
    bind_scope: str
    public_bind_opt_in: bool
    reason_codes: list[str]
    banned_env_categories: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "bind_address": self.bind_address,
            "bind_scope": self.bind_scope,
            "public_bind_opt_in": self.public_bind_opt_in,
            "reason_codes": list(self.reason_codes),
            "banned_env_categories": list(self.banned_env_categories),
        }


def is_allowed_ui_env_key(key: str) -> bool:
    return key in _ALLOWED_EXACT_KEYS or any(key.startswith(prefix) for prefix in _ALLOWED_PREFIXES)


def _is_loopback_bind_address(bind_address: str) -> bool:
    normalized = str(bind_address or "").strip().lower()
    return normalized in {"127.0.0.1", "localhost", "::1"}


def _resolve_bind_address(*, env_map: dict[str, str] | None = None) -> str:
    payload = env_map or {}
    value = str(payload.get("ASTERION_UI_BIND_ADDRESS") or os.getenv("ASTERION_UI_BIND_ADDRESS") or "").strip()
    return value or _DEFAULT_BIND_ADDRESS


def _env_flag_true(value: str | None) -> bool:
    return str(value or "").strip().lower() == "true"


def detect_banned_ui_env(*, env_path: str | Path | None = None) -> dict[str, list[str]]:
    _ = env_path
    sources = dict(os.environ)
    categories: dict[str, list[str]] = {
        "controlled_live_secrets_present": [],
        "agent_provider_secrets_present": [],
        "signer_or_wallet_secrets_present": [],
    }
    for key, value in sources.items():
        if not str(value).strip():
            continue
        if key in _BANNED_EXACT_KEYS:
            categories["agent_provider_secrets_present"].append(key)
            continue
        if any(key.startswith(prefix) for prefix in _BANNED_PREFIXES):
            categories["controlled_live_secrets_present"].append(key)
            continue
        upper_key = key.upper()
        if "PRIVATE_KEY" in upper_key or "WALLET_SECRET" in upper_key:
            categories["signer_or_wallet_secrets_present"].append(key)
    return {name: sorted(values) for name, values in categories.items() if values}


def load_ui_runtime_boundary_status(*, env_path: str | Path | None = None) -> UiRuntimeBoundaryStatus:
    env_map = resolve_ui_runtime_env(env_path=env_path)
    bind_address = _resolve_bind_address(env_map=env_map)
    public_bind_opt_in = _env_flag_true(env_map.get("ASTERION_UI_ALLOW_PUBLIC_BIND") or os.getenv("ASTERION_UI_ALLOW_PUBLIC_BIND"))
    bind_scope = "loopback" if _is_loopback_bind_address(bind_address) else "public"
    banned = detect_banned_ui_env(env_path=env_path)
    reason_codes: list[str] = []
    if banned:
        reason_codes.append("banned_env_present")
    if bind_scope == "public" and not public_bind_opt_in:
        reason_codes.append("public_bind_requires_opt_in")
    status = "blocked" if reason_codes else "ok"
    return UiRuntimeBoundaryStatus(
        status=status,
        bind_address=bind_address,
        bind_scope=bind_scope,
        public_bind_opt_in=public_bind_opt_in,
        reason_codes=reason_codes,
        banned_env_categories=sorted(banned.keys()),
    )


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
    bind_address = _resolve_bind_address(env_map=payload)
    if "ASTERION_UI_BIND_ADDRESS" in os.environ or "ASTERION_UI_BIND_ADDRESS" in payload:
        payload["ASTERION_UI_BIND_ADDRESS"] = bind_address
    if "ASTERION_UI_ALLOW_PUBLIC_BIND" in os.environ:
        payload["ASTERION_UI_ALLOW_PUBLIC_BIND"] = os.environ["ASTERION_UI_ALLOW_PUBLIC_BIND"]
    return dict(sorted(payload.items()))


def hydrate_ui_runtime_env(*, env_path: str | Path | None = None, override_existing: bool = False) -> dict[str, str]:
    payload = resolve_ui_runtime_env(env_path=env_path)
    for key, value in payload.items():
        if override_existing or key not in os.environ:
            os.environ[key] = value
    return payload


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
