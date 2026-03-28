from __future__ import annotations

import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from agents.common import FakeAgentClient
from agents.weather.opportunity_triage_agent import OpportunityTriageAgentRequest, run_opportunity_triage_agent_review
from asterion_core.storage.write_queue import WriteQueueConfig
from dagster_asterion.handlers import run_weather_opportunity_triage_review_job


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
        microstructure_reason_codes=["size_shock"],
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


class OpportunityTriageJobTest(unittest.TestCase):
    def test_batch_and_manual_rerun_share_same_handler(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
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
        with (
            patch("dagster_asterion.handlers.load_opportunity_triage_agent_requests", return_value=[_request()]) as load_requests,
            patch("dagster_asterion.handlers.run_opportunity_triage_agent_review", return_value=artifacts) as run_agent,
            patch("dagster_asterion.handlers.enqueue_agent_artifact_upserts", return_value=["task_triage"]) as enqueue_agent,
            patch("dagster_asterion.handlers.enqueue_agent_evaluation_upserts", return_value="task_replay_eval") as enqueue_eval,
            patch("dagster_asterion.handlers.default_ui_lite_db_path", return_value="data/ui/asterion_ui_lite.duckdb"),
            patch(
                "dagster_asterion.handlers.run_operator_surface_refresh",
                return_value={
                    "surface_refresh_run_id": "triage_refresh",
                    "ui_replica_ok": True,
                    "ui_lite_ok": True,
                    "truth_check_fail_count": 0,
                    "degraded_surface_count": 0,
                    "read_error_surface_count": 0,
                    "surface_refresh_error": None,
                    "surface_refresh_refreshed_at": "2026-03-22T10:05:00+00:00",
                },
            ),
        ):
            result = run_weather_opportunity_triage_review_job(
                object(),
                queue_cfg,
                client=client,
                market_ids=["mkt_1"],
                limit=1,
                force_rerun=True,
            )
        load_requests.assert_called_once_with(
            "data/ui/asterion_ui_lite.duckdb",
            market_ids=["mkt_1"],
            limit=1,
            primary_source="ui_lite",
        )
        run_agent.assert_called_once()
        enqueue_agent.assert_called_once()
        enqueue_eval.assert_called_once()
        self.assertEqual(result.task_ids, ["task_triage", "task_replay_eval"])
        self.assertEqual(result.metadata["subject_ids"], ["mkt_1"])
        self.assertEqual(result.metadata["surface_refresh_run_id"], "triage_refresh")

    def test_provider_forbidden_uses_deterministic_fallback_output(self) -> None:
        class ForbiddenClient:
            def invoke(self, **kwargs):
                raise RuntimeError("upstream returned HTTP 403: forbidden")

        artifacts = run_opportunity_triage_agent_review(
            ForbiddenClient(),
            _request(),
            now=datetime(2026, 3, 22, 10, 0, tzinfo=UTC),
        )

        self.assertEqual(artifacts.invocation.status.value, "success")
        self.assertEqual(artifacts.invocation.model_provider, "deterministic_fallback")
        self.assertIsNotNone(artifacts.output)
        self.assertIsNotNone(artifacts.review)
        self.assertIsNotNone(artifacts.evaluation)
        assert artifacts.output is not None
        self.assertEqual(artifacts.output.structured_output_json["confidence_band"], "low")
        self.assertIn("provider_forbidden", artifacts.output.structured_output_json["triage_reason_codes"])

    def test_provider_unauthorized_also_uses_deterministic_fallback_output(self) -> None:
        class UnauthorizedClient:
            def invoke(self, **kwargs):
                raise RuntimeError("upstream returned HTTP 401: unauthorized")

        artifacts = run_opportunity_triage_agent_review(
            UnauthorizedClient(),
            _request(),
            now=datetime(2026, 3, 22, 10, 0, tzinfo=UTC),
        )

        self.assertEqual(artifacts.invocation.status.value, "success")
        self.assertEqual(artifacts.invocation.model_provider, "deterministic_fallback")
        self.assertIsNotNone(artifacts.output)
        assert artifacts.output is not None
        self.assertIn("provider_unauthorized", artifacts.output.structured_output_json["triage_reason_codes"])


if __name__ == "__main__":
    unittest.main()
