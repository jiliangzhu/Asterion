from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from agents.common import (
    AgentInvocationStatus,
    AgentType,
    AnthropicAgentClient,
    FakeAgentClient,
    OpenAICompatibleAgentClient,
    build_agent_client_from_env,
    build_agent_invocation_record,
    stable_agent_input_hash,
)
import agents.common.client as client_module


HAS_HTTPX = importlib.util.find_spec("httpx") is not None


class AgentRuntimeContractsTest(unittest.TestCase):
    def test_stable_agent_input_hash_and_invocation_id(self) -> None:
        payload = {"market_id": "mkt_weather_1", "station_id": "KNYC"}
        started_at = datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc)
        invocation_1 = build_agent_invocation_record(
            agent_type=AgentType.RULE2SPEC,
            agent_version="v1",
            prompt_version="p1",
            subject_type="weather_market",
            subject_id="mkt_weather_1",
            input_payload_json=payload,
            model_provider="fake",
            model_name="fake-model",
            status=AgentInvocationStatus.SUCCESS,
            started_at=started_at,
        )
        invocation_2 = build_agent_invocation_record(
            agent_type=AgentType.RULE2SPEC,
            agent_version="v1",
            prompt_version="p1",
            subject_type="weather_market",
            subject_id="mkt_weather_1",
            input_payload_json=dict(payload),
            model_provider="fake",
            model_name="fake-model",
            status=AgentInvocationStatus.SUCCESS,
            started_at=started_at,
        )
        self.assertEqual(stable_agent_input_hash(payload), invocation_1.input_hash)
        self.assertEqual(invocation_1.invocation_id, invocation_2.invocation_id)

    def test_force_rerun_changes_invocation_id(self) -> None:
        payload = {"proposal_id": "prop_1"}
        started_at = datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc)
        original = build_agent_invocation_record(
            agent_type=AgentType.RESOLUTION,
            agent_version="v1",
            prompt_version="p1",
            subject_type="uma_proposal",
            subject_id="prop_1",
            input_payload_json=payload,
            model_provider="fake",
            model_name="fake-model",
            status=AgentInvocationStatus.SUCCESS,
            started_at=started_at,
        )
        rerun = build_agent_invocation_record(
            agent_type=AgentType.RESOLUTION,
            agent_version="v1",
            prompt_version="p1",
            subject_type="uma_proposal",
            subject_id="prop_1",
            input_payload_json=payload,
            model_provider="fake",
            model_name="fake-model",
            status=AgentInvocationStatus.SUCCESS,
            started_at=started_at,
            force_rerun=True,
            force_rerun_token="rerun_1",
        )
        self.assertNotEqual(original.invocation_id, rerun.invocation_id)

    def test_fake_agent_client_can_drive_multiple_agent_types(self) -> None:
        client = FakeAgentClient(
            responses={
                "rule2spec": {"verdict": "pass"},
                "data_qa": {"verdict": "review"},
                "resolution": {"verdict": "block"},
            }
        )
        self.assertEqual(
            client.invoke(system_prompt="", user_prompt="", input_payload_json={}, metadata={"agent_type": "rule2spec"}).structured_output_json["verdict"],
            "pass",
        )
        self.assertEqual(
            client.invoke(system_prompt="", user_prompt="", input_payload_json={}, metadata={"agent_type": "data_qa"}).structured_output_json["verdict"],
            "review",
        )
        self.assertEqual(
            client.invoke(system_prompt="", user_prompt="", input_payload_json={}, metadata={"agent_type": "resolution"}).structured_output_json["verdict"],
            "block",
        )

    def test_build_agent_client_from_env_supports_qwen_aliases(self) -> None:
        env = {
            "ALIBABA_API_KEY": "test-aliyun-key",
            "QWEN_MODEL": "qwen-max",
            "ASTERION_OPENAI_COMPATIBLE_API_KEY": "",
            "ASTERION_OPENAI_COMPATIBLE_MODEL": "",
            "ASTERION_OPENAI_COMPATIBLE_BASE_URL": "",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            client = build_agent_client_from_env()
        self.assertIsInstance(client, OpenAICompatibleAgentClient)
        self.assertEqual(client._model_name, "qwen-max")
        self.assertEqual(client._base_url, "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions")

    def test_build_agent_client_from_env_supports_qwen_api_key_alias(self) -> None:
        env = {
            "QWEN_API_KEY": "test-qwen-key",
            "QWEN_MODEL": "qwen-max",
            "ASTERION_OPENAI_COMPATIBLE_API_KEY": "",
            "ASTERION_OPENAI_COMPATIBLE_MODEL": "",
            "ASTERION_OPENAI_COMPATIBLE_BASE_URL": "",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            client = build_agent_client_from_env()
        self.assertIsInstance(client, OpenAICompatibleAgentClient)
        self.assertEqual(client._model_name, "qwen-max")
        self.assertEqual(client._base_url, "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions")

    def test_build_agent_client_from_env_supports_explicit_openai_compatible_endpoint(self) -> None:
        env = {
            "ASTERION_AGENT_PROVIDER": "openai_compatible",
            "ASTERION_OPENAI_COMPATIBLE_API_KEY": "test-healwrap-key",
            "ASTERION_OPENAI_COMPATIBLE_MODEL": "glm-5",
            "ASTERION_OPENAI_COMPATIBLE_BASE_URL": "https://llm-api.healwrap.cn/v1/chat/completions",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            client = build_agent_client_from_env()
        self.assertIsInstance(client, OpenAICompatibleAgentClient)
        self.assertEqual(client._model_name, "glm-5")
        self.assertEqual(client._base_url, "https://llm-api.healwrap.cn/v1/chat/completions")

    def test_load_env_file_sets_defaults_without_overriding_existing_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_file = Path(tmpdir) / ".env"
            env_file.write_text(
                "ALIBABA_API_KEY=file-key\nQWEN_MODEL=qwen-plus\nASTERION_AGENT_PROVIDER=openai_compatible\n",
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"QWEN_MODEL": "qwen-max"}, clear=True):
                client_module._load_env_file(env_file)
                self.assertEqual(os.environ["ALIBABA_API_KEY"], "file-key")
                self.assertEqual(os.environ["QWEN_MODEL"], "qwen-max")
                self.assertEqual(os.environ["ASTERION_AGENT_PROVIDER"], "openai_compatible")

    def test_openai_compatible_client_uses_curl_fallback_for_dashscope(self) -> None:
        class BrokenHttpClient:
            def post(self, *args, **kwargs):
                raise RuntimeError("simulated httpx transport failure")

        client = OpenAICompatibleAgentClient(
            api_key="test-key",
            model_name="qwen-max",
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
            http_client=BrokenHttpClient(),
        )

        completed = subprocess.CompletedProcess(
            args=["curl"],
            returncode=0,
            stdout=json.dumps({"choices": [{"message": {"content": "{\"ok\": true}"}}]}),
            stderr="",
        )
        with mock.patch.object(client_module.subprocess, "run", return_value=completed) as run_mock:
            response = client.invoke(
                system_prompt="system",
                user_prompt="user",
                input_payload_json={"foo": "bar"},
                metadata={"agent_type": "rule2spec"},
            )
        self.assertEqual(response.structured_output_json["ok"], True)
        self.assertTrue(run_mock.called)

    def test_openai_compatible_client_retries_without_response_format_on_400(self) -> None:
        class Response400Then200HttpClient:
            def __init__(self) -> None:
                self.payloads = []

            def post(self, url, *, headers, json, timeout):
                del url, headers, timeout
                self.payloads.append(dict(json))

                class Response:
                    def __init__(self, status_code, payload):
                        self.status_code = status_code
                        self._payload = payload

                    def raise_for_status(self):
                        if self.status_code >= 400:
                            raise RuntimeError(f"http {self.status_code}")

                    def json(self):
                        return self._payload

                if len(self.payloads) == 1:
                    return Response(400, {"error": {"message": "response_format unsupported"}})
                return Response(200, {"choices": [{"message": {"content": "{\"verdict\":\"pass\"}"}}]})

        http_client = Response400Then200HttpClient()
        client = OpenAICompatibleAgentClient(
            api_key="test-key",
            model_name="glm-5",
            base_url="https://llm-api.healwrap.cn/v1/chat/completions",
            http_client=http_client,
        )
        response = client.invoke(
            system_prompt="system",
            user_prompt="user",
            input_payload_json={"foo": "bar"},
            metadata={"agent_type": "rule2spec"},
        )
        self.assertEqual(response.structured_output_json["verdict"], "pass")
        self.assertIn("response_format", http_client.payloads[0])
        self.assertNotIn("response_format", http_client.payloads[1])

    def test_openai_compatible_client_retries_on_502(self) -> None:
        class Response502Then200HttpClient:
            def __init__(self) -> None:
                self.calls = 0

            def post(self, url, *, headers, json, timeout):
                del url, headers, json, timeout
                self.calls += 1

                class Response:
                    def __init__(self, status_code, payload):
                        self.status_code = status_code
                        self._payload = payload

                    def raise_for_status(self):
                        if self.status_code >= 400:
                            raise RuntimeError(f"http {self.status_code}")

                    def json(self):
                        return self._payload

                if self.calls == 1:
                    return Response(502, {"error": {"message": "bad gateway"}})
                return Response(200, {"choices": [{"message": {"content": "{\"verdict\":\"review\"}"}}]})

        http_client = Response502Then200HttpClient()
        client = OpenAICompatibleAgentClient(
            api_key="test-key",
            model_name="glm-5",
            base_url="https://llm-api.healwrap.cn/v1/chat/completions",
            http_client=http_client,
        )
        response = client.invoke(
            system_prompt="system",
            user_prompt="user",
            input_payload_json={"foo": "bar"},
            metadata={"agent_type": "rule2spec"},
        )
        self.assertEqual(response.structured_output_json["verdict"], "review")
        self.assertEqual(http_client.calls, 2)

    def test_parse_structured_output_accepts_markdown_fenced_json(self) -> None:
        parsed = client_module._parse_structured_output(
            "```json\n{\"ok\": true, \"message\": \"hi\"}\n```",
            raw={},
        )
        self.assertEqual(parsed["ok"], True)


@unittest.skipUnless(HAS_HTTPX, "httpx is required for provider adapter tests")
class AgentProviderAdaptersTest(unittest.TestCase):
    def test_openai_compatible_client_parses_mock_http_response(self) -> None:
        import httpx

        def handler(request: httpx.Request) -> httpx.Response:
            self.assertIn("/v1/chat/completions", str(request.url))
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {
                                "content": "{\"verdict\":\"pass\",\"confidence\":0.9}",
                            }
                        }
                    ]
                },
            )

        client = OpenAICompatibleAgentClient(
            api_key="test-key",
            model_name="gpt-test",
            base_url="https://example.test/v1/chat/completions",
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        response = client.invoke(
            system_prompt="system",
            user_prompt="user",
            input_payload_json={"foo": "bar"},
            metadata={"agent_type": "data_qa"},
        )
        self.assertEqual(response.model_provider, "openai_compatible")
        self.assertEqual(response.structured_output_json["verdict"], "pass")

    def test_anthropic_client_parses_mock_http_response(self) -> None:
        import httpx

        def handler(request: httpx.Request) -> httpx.Response:
            self.assertIn("/v1/messages", str(request.url))
            return httpx.Response(
                200,
                json={
                    "content": [
                        {
                            "type": "text",
                            "text": "{\"verdict\":\"review\",\"confidence\":0.7}",
                        }
                    ]
                },
            )

        client = AnthropicAgentClient(
            api_key="test-key",
            model_name="claude-test",
            base_url="https://example.test/v1/messages",
            http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
        response = client.invoke(
            system_prompt="system",
            user_prompt="user",
            input_payload_json={"foo": "bar"},
            metadata={"agent_type": "resolution"},
        )
        self.assertEqual(response.model_provider, "anthropic")
        self.assertEqual(response.structured_output_json["verdict"], "review")


if __name__ == "__main__":
    unittest.main()
