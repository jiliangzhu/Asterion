from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timezone

from agents.common import (
    AgentInvocationStatus,
    AgentType,
    AnthropicAgentClient,
    FakeAgentClient,
    OpenAICompatibleAgentClient,
    build_agent_invocation_record,
    stable_agent_input_hash,
)


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
