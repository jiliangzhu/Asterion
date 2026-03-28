from __future__ import annotations

import unittest

from agents.common import FakeAgentClient
from agents.weather.opportunity_triage_agent import parse_opportunity_triage_output, run_opportunity_triage_agent_review
from agents.weather.opportunity_triage_evaluator import build_replay_backtest_evaluation_record
from tests.test_opportunity_triage_agent_contract import _request


class OpportunityTriageEvaluationTest(unittest.TestCase):
    def test_evaluation_score_json_contains_required_metrics(self) -> None:
        client = FakeAgentClient(
            responses={
                "opportunity_triage": {
                    "triage_status": "review",
                    "priority_band": "high",
                    "triage_reason_codes": ["delivery_degraded"],
                    "execution_risk_flags": ["size_shock"],
                    "recommended_operator_action": "manual_review",
                    "confidence_band": "medium",
                    "supporting_evidence_refs": ["ui.market_opportunity_summary:mkt_1"],
                    "summary": "review this market",
                    "confidence": 0.66,
                    "human_review_required": True,
                }
            }
        )
        artifacts = run_opportunity_triage_agent_review(client, _request())
        self.assertEqual(artifacts.evaluation.verification_method, "operator_outcome_proxy")
        self.assertEqual(
            set(artifacts.evaluation.score_json),
            {
                "queue_cleanliness_delta",
                "priority_precision_proxy",
                "false_escalation_rate",
                "operator_throughput_delta",
                "baseline_operator_bucket",
                "overlay_priority_band",
                "verification_method_hint",
            },
        )

    def test_replay_backtest_metrics_cover_baseline_vs_overlay(self) -> None:
        client = FakeAgentClient(
            responses={
                "opportunity_triage": {
                    "triage_status": "review",
                    "priority_band": "high",
                    "triage_reason_codes": ["delivery_degraded"],
                    "execution_risk_flags": ["size_shock"],
                    "recommended_operator_action": "manual_review",
                    "confidence_band": "medium",
                    "supporting_evidence_refs": ["ui.market_opportunity_summary:mkt_1"],
                    "summary": "review this market",
                    "confidence": 0.66,
                    "human_review_required": True,
                }
            }
        )
        artifacts = run_opportunity_triage_agent_review(client, _request())
        replay = build_replay_backtest_evaluation_record(
            invocation_id=artifacts.invocation.invocation_id,
            request=_request(),
            output=parse_opportunity_triage_output(artifacts.output.structured_output_json, request=_request()),
            created_at=artifacts.output.created_at,
        )
        self.assertEqual(replay.verification_method, "replay_backtest")
        self.assertIn("baseline_queue_size", replay.score_json)
        self.assertIn("overlay_queue_size", replay.score_json)
        self.assertIn("evaluation_window_start", replay.score_json)
        self.assertIn("evaluation_window_end", replay.score_json)


if __name__ == "__main__":
    unittest.main()
