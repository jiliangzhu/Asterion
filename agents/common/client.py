from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


QWEN_OPENAI_COMPATIBLE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"


@dataclass(frozen=True)
class AgentClientResponse:
    model_provider: str
    model_name: str
    structured_output_json: dict[str, Any]
    raw_response_json: dict[str, Any]
    output_text: str | None = None


class AgentClient(Protocol):
    def invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        input_payload_json: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> AgentClientResponse: ...


class FakeAgentClient:
    def __init__(
        self,
        *,
        responses: dict[str, dict[str, Any]] | None = None,
        response_factory=None,
        model_provider: str = "fake",
        model_name: str = "fake-agent-client",
    ) -> None:
        self._responses = dict(responses or {})
        self._response_factory = response_factory
        self._model_provider = model_provider
        self._model_name = model_name

    def invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        input_payload_json: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> AgentClientResponse:
        del system_prompt, user_prompt, timeout_seconds
        meta = dict(metadata or {})
        key = str(meta.get("agent_type") or meta.get("subject_type") or "default")
        if self._response_factory is not None:
            payload = self._response_factory(input_payload_json, meta)
        else:
            payload = self._responses.get(key, self._responses.get("default"))
        if payload is None:
            raise LookupError(f"no fake agent response configured for key={key}")
        if not isinstance(payload, dict):
            raise ValueError("fake agent payload must be a dictionary")
        return AgentClientResponse(
            model_provider=self._model_provider,
            model_name=self._model_name,
            structured_output_json=payload,
            raw_response_json={"fake": True, "metadata": meta, "payload": payload},
            output_text=json.dumps(payload, sort_keys=True),
        )


class AnthropicAgentClient:
    def __init__(
        self,
        *,
        api_key: str,
        model_name: str,
        base_url: str | None = None,
        anthropic_version: str = "2023-06-01",
        http_client=None,
    ) -> None:
        self._api_key = api_key
        self._model_name = model_name
        self._base_url = base_url or "https://api.anthropic.com/v1/messages"
        self._anthropic_version = anthropic_version
        self._http_client = http_client or _build_httpx_client()

    def invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        input_payload_json: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> AgentClientResponse:
        payload = {
            "model": self._model_name,
            "max_tokens": 1024,
            "system": system_prompt,
            "messages": [
                {
                    "role": "user",
                    "content": _compose_user_content(user_prompt, input_payload_json, metadata),
                }
            ],
        }
        response = self._http_client.post(
            self._base_url,
            headers={
                "content-type": "application/json",
                "x-api-key": self._api_key,
                "anthropic-version": self._anthropic_version,
            },
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        raw = response.json()
        text = _extract_anthropic_text(raw)
        return AgentClientResponse(
            model_provider="anthropic",
            model_name=self._model_name,
            structured_output_json=_parse_structured_output(text, raw),
            raw_response_json=raw,
            output_text=text,
        )


class OpenAICompatibleAgentClient:
    def __init__(
        self,
        *,
        api_key: str,
        model_name: str,
        base_url: str | None = None,
        http_client=None,
    ) -> None:
        self._api_key = api_key
        self._model_name = model_name
        self._base_url = base_url or "https://api.openai.com/v1/chat/completions"
        self._http_client = http_client or _build_httpx_client()

    def invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        input_payload_json: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> AgentClientResponse:
        payload = {
            "model": self._model_name,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": _compose_user_content(user_prompt, input_payload_json, metadata),
                },
            ],
        }
        raw = self._post_payload(payload, timeout_seconds=timeout_seconds)
        text = _extract_openai_text(raw)
        return AgentClientResponse(
            model_provider="openai_compatible",
            model_name=self._model_name,
            structured_output_json=_parse_structured_output(text, raw),
            raw_response_json=raw,
            output_text=text,
        )

    def _post_payload(self, payload: dict[str, Any], *, timeout_seconds: float | None) -> dict[str, Any]:
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self._api_key}",
        }
        try:
            response = self._http_client.post(
                self._base_url,
                headers=headers,
                json=payload,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except Exception:
            if not _should_use_curl_fallback(self._base_url):
                raise
            return _post_json_with_curl(
                self._base_url,
                headers=headers,
                payload=payload,
                timeout_seconds=timeout_seconds,
            )


def build_agent_client_from_env(http_client=None) -> AgentClient:
    _maybe_load_project_dotenv()
    provider = os.getenv("ASTERION_AGENT_PROVIDER", "").strip().lower()
    if not provider:
        if os.getenv("ASTERION_ANTHROPIC_API_KEY"):
            provider = "anthropic"
        elif os.getenv("ASTERION_OPENAI_COMPATIBLE_API_KEY") or os.getenv("ALIBABA_API_KEY") or os.getenv("QWEN_API_KEY"):
            provider = "openai_compatible"
        else:
            raise ValueError("ASTERION_AGENT_PROVIDER is required when no provider credentials are configured")
    if provider == "anthropic":
        api_key = os.getenv("ASTERION_ANTHROPIC_API_KEY", "").strip()
        model_name = os.getenv("ASTERION_ANTHROPIC_MODEL", "").strip() or os.getenv("ASTERION_AGENT_MODEL", "").strip()
        if not api_key or not model_name:
            raise ValueError("ASTERION_ANTHROPIC_API_KEY and ASTERION_ANTHROPIC_MODEL are required")
        return AnthropicAgentClient(
            api_key=api_key,
            model_name=model_name,
            base_url=os.getenv("ASTERION_ANTHROPIC_BASE_URL") or None,
            http_client=http_client,
        )
    if provider == "openai_compatible":
        api_key = (
            os.getenv("ASTERION_OPENAI_COMPATIBLE_API_KEY", "").strip()
            or os.getenv("ALIBABA_API_KEY", "").strip()
            or os.getenv("QWEN_API_KEY", "").strip()
        )
        model_name = (
            os.getenv("ASTERION_OPENAI_COMPATIBLE_MODEL", "").strip()
            or os.getenv("ASTERION_AGENT_MODEL", "").strip()
            or os.getenv("QWEN_MODEL", "").strip()
        )
        if not api_key or not model_name:
            raise ValueError(
                "ASTERION_OPENAI_COMPATIBLE_API_KEY/ALIBABA_API_KEY and "
                "QWEN_API_KEY and ASTERION_OPENAI_COMPATIBLE_MODEL/ASTERION_AGENT_MODEL/QWEN_MODEL are required"
            )
        base_url = (
            os.getenv("ASTERION_OPENAI_COMPATIBLE_BASE_URL")
            or os.getenv("ALIBABA_OPENAI_BASE_URL")
            or (QWEN_OPENAI_COMPATIBLE_BASE_URL if (os.getenv("ALIBABA_API_KEY") or os.getenv("QWEN_API_KEY")) else "")
            or None
        )
        return OpenAICompatibleAgentClient(
            api_key=api_key,
            model_name=model_name,
            base_url=base_url,
            http_client=http_client,
        )
    if provider == "fake":
        return FakeAgentClient()
    raise ValueError(f"unsupported ASTERION_AGENT_PROVIDER={provider!r}")


def _maybe_load_project_dotenv() -> None:
    root = Path(__file__).resolve().parents[2]
    _load_env_file(root / ".env")


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        os.environ.setdefault(key, value)


def _build_httpx_client():
    try:
        import httpx
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Missing dependency: httpx. Install with: pip install httpx") from exc
    return httpx.Client(timeout=30.0)


def _should_use_curl_fallback(base_url: str | None) -> bool:
    if not base_url:
        return False
    return "dashscope.aliyuncs.com" in base_url


def _post_json_with_curl(
    url: str,
    *,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout_seconds: float | None,
) -> dict[str, Any]:
    timeout = max(1, int(timeout_seconds or 60))
    cmd = ["curl", "-sS", "-L", "--max-time", str(timeout)]
    for key, value in headers.items():
        cmd.extend(["-H", f"{key}: {value}"])
    cmd.extend([url, "-d", json.dumps(payload, ensure_ascii=False)])
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = proc.stderr.strip() or "curl request failed"
        raise RuntimeError(stderr)
    try:
        raw = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError("curl fallback returned non-JSON response") from exc
    if isinstance(raw, dict) and raw.get("error"):
        raise RuntimeError(json.dumps(raw["error"], ensure_ascii=False, sort_keys=True))
    if not isinstance(raw, dict):
        raise ValueError("curl fallback returned invalid JSON payload")
    return raw


def _compose_user_content(
    user_prompt: str,
    input_payload_json: dict[str, Any],
    metadata: dict[str, Any] | None,
) -> str:
    meta = dict(metadata or {})
    return (
        f"{user_prompt}\n\n"
        "Return a single JSON object only.\n\n"
        f"metadata={json.dumps(meta, sort_keys=True)}\n"
        f"input={json.dumps(input_payload_json, sort_keys=True)}"
    )


def _extract_anthropic_text(raw: dict[str, Any]) -> str:
    content = raw.get("content")
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        if parts:
            return "\n".join(parts)
    raise ValueError("anthropic response missing text content")


def _extract_openai_text(raw: dict[str, Any]) -> str:
    choices = raw.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("openai-compatible response missing choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        raise ValueError("openai-compatible response missing message")
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        if parts:
            return "\n".join(parts)
    raise ValueError("openai-compatible response missing content text")


def _parse_structured_output(text: str, raw: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        maybe_json = raw.get("output_json") if isinstance(raw, dict) else None
        if isinstance(maybe_json, dict):
            return maybe_json
        raise ValueError("agent response text is not valid JSON")
    if not isinstance(parsed, dict):
        raise ValueError("agent structured output must decode to an object")
    return parsed
