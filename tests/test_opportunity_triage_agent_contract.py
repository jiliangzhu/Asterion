from __future__ import annotations

import unittest
from datetime import UTC, datetime

from agents.common import AgentInvocationStatus, FakeAgentClient
from agents.weather.opportunity_triage_agent import (
    OpportunityTriageAgentRequest,
    _system_prompt,
    _user_prompt,
    run_opportunity_triage_agent_review,
)


def _request() -> OpportunityTriageAgentRequest:
    return OpportunityTriageAgentRequest(
        market_id="mkt_1",
        question="Will Seattle be cold?",
        location_name="Seattle",
        best_side="BUY",
        ranking_score=0.82,
        recommended_size=4.0,
        allocation_status="resized",
        operator_bucket="review_required",
        queue_reason_codes=["allocation:resized"],
        execution_intelligence_score=0.31,
        top_of_book_stability=0.22,
        spread_regime="wide",
        expected_capture_regime="low",
        expected_slippage_regime="high",
        microstructure_reason_codes=["size_shock", "unstable_book"],
        calibration_gate_status="review_required",
        capital_policy_id="cap_policy_1",
        surface_delivery_status="degraded_source",
        surface_fallback_origin="runtime_db",
        source_provenance={"primary_source": "ui_replica", "subject_type": "weather_market"},
        source_freshness="fresh",
        is_degraded_source=True,
        why_ranked_json={"mode": "ranking_v2"},
        pricing_context_json={"ranking_score": 0.82},
    )


class OpportunityTriageAgentContractTest(unittest.TestCase):
    def test_structured_output_contract_is_persisted(self) -> None:
        client = FakeAgentClient(
            responses={
                "opportunity_triage": {
                    "triage_status": "review",
                    "priority_band": "high",
                    "triage_reason_codes": ["delivery_degraded", "book_unstable"],
                    "execution_risk_flags": ["size_shock"],
                    "recommended_operator_action": "manual_review",
                    "confidence_band": "medium",
                    "supporting_evidence_refs": ["ui.market_opportunity_summary:mkt_1"],
                    "summary": "degraded delivery and unstable book justify review",
                    "confidence": 0.67,
                    "human_review_required": True,
                }
            }
        )
        artifacts = run_opportunity_triage_agent_review(client, _request(), now=datetime(2026, 3, 22, 10, 0, tzinfo=UTC))
        self.assertEqual(artifacts.invocation.agent_type.value, "opportunity_triage")
        self.assertEqual(artifacts.output.structured_output_json["priority_band"], "high")
        self.assertIn("execution_risk_flags", artifacts.output.structured_output_json)
        self.assertEqual(artifacts.review.review_payload_json["triage_status"], "review")
        self.assertEqual(artifacts.evaluation.verification_method, "operator_outcome_proxy")
        self.assertIn("queue_cleanliness_delta", artifacts.evaluation.score_json)

    def test_parse_failure_maps_to_parse_error_without_output(self) -> None:
        client = FakeAgentClient(
            responses={
                "opportunity_triage": {
                    "priority_band": "high",
                    "triage_reason_codes": [],
                    "execution_risk_flags": [],
                    "recommended_operator_action": "manual_review",
                    "confidence_band": "medium",
                    "supporting_evidence_refs": [],
                    "summary": "missing triage status should fail",
                }
            }
        )
        artifacts = run_opportunity_triage_agent_review(client, _request(), now=datetime(2026, 3, 22, 10, 0, tzinfo=UTC))
        self.assertEqual(artifacts.invocation.status, AgentInvocationStatus.PARSE_ERROR)
        self.assertIsNone(artifacts.output)
        self.assertIsNone(artifacts.review)

    def test_model_input_is_compact_but_persisted_input_remains_full(self) -> None:
        seen: dict[str, object] = {}

        def response_factory(input_payload_json, _meta):
            seen.update(input_payload_json)
            return {
                "triage_status": "review",
                "priority_band": "medium",
                "triage_reason_codes": ["operator_review_required"],
                "execution_risk_flags": ["tight_spread"],
                "recommended_operator_action": "manual_review",
                "confidence_band": "medium",
                "supporting_evidence_refs": ["ui.market_opportunity_summary:mkt_1"],
                "summary": "compact payload still produced a valid response",
                "confidence": 0.61,
                "human_review_required": True,
            }

        client = FakeAgentClient(response_factory=response_factory)
        artifacts = run_opportunity_triage_agent_review(client, _request(), now=datetime(2026, 3, 22, 10, 0, tzinfo=UTC))
        self.assertIn("why_ranked", seen)
        self.assertIn("pricing_context", seen)
        self.assertNotIn("why_ranked_json", seen)
        self.assertNotIn("pricing_context_json", seen)
        self.assertIn("why_ranked_json", artifacts.invocation.input_payload_json)
        self.assertIn("pricing_context_json", artifacts.invocation.input_payload_json)

    def test_prompts_require_chinese_operator_summary_without_changing_contract_fields(self) -> None:
        self.assertIn("Simplified Chinese", _system_prompt())
        self.assertIn("structured enum fields in English", _system_prompt())
        self.assertIn("summary must be concise Simplified Chinese", _user_prompt())
        self.assertIn("supporting_evidence_refs must remain unchanged", _user_prompt())


if __name__ == "__main__":
    unittest.main()
