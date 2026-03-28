from __future__ import annotations

import unittest
from datetime import UTC, datetime

from agents.common import FakeAgentClient
from agents.weather.opportunity_triage_agent import parse_opportunity_triage_output, run_opportunity_triage_agent_review
from agents.weather.opportunity_triage_evaluator import build_replay_backtest_evaluation_record
from tests.test_opportunity_triage_agent_contract import _request


class OpportunityTriageReplayEvaluationTest(unittest.TestCase):
    def test_replay_backtest_evaluation_record_is_materialized_with_required_metrics(self) -> None:
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
        artifacts = run_opportunity_triage_agent_review(client, _request(), now=datetime(2026, 3, 22, 10, 0, tzinfo=UTC))
        output = parse_opportunity_triage_output(artifacts.output.structured_output_json, request=_request())
        replay = build_replay_backtest_evaluation_record(
            invocation_id=artifacts.invocation.invocation_id,
            request=_request(),
            output=output,
            created_at=artifacts.output.created_at,
        )
        self.assertEqual(replay.verification_method, "replay_backtest")
        self.assertEqual(
            set(replay.score_json),
            {
                "queue_cleanliness_delta",
                "priority_precision_proxy",
                "false_escalation_rate",
                "operator_throughput_delta",
                "baseline_queue_size",
                "overlay_queue_size",
                "evaluation_window_start",
                "evaluation_window_end",
                "baseline_operator_bucket",
                "overlay_priority_band",
            },
        )
        self.assertIsInstance(replay.is_verified, bool)


if __name__ == "__main__":
    unittest.main()
