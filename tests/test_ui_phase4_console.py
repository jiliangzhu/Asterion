from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from ui.pages import agents, execution, markets, system


class _DummyContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class UiPhase4ConsoleSmokeTest(unittest.TestCase):
    def test_execution_show_renders_predicted_vs_realized(self) -> None:
        payload = {
            "tickets": pd.DataFrame([{"ticket_id": "tt_1", "wallet_id": "wallet_weather_1", "market_id": "mkt_1"}]),
            "runs": pd.DataFrame(),
            "exceptions": pd.DataFrame(),
            "live_prereq": pd.DataFrame(),
            "journal": pd.DataFrame(),
            "daily_ops": pd.DataFrame(),
            "predicted_vs_realized": pd.DataFrame(
                [
                    {
                        "ticket_id": "tt_1",
                        "wallet_id": "wallet_weather_1",
                        "strategy_id": "weather_primary",
                        "market_id": "mkt_1",
                        "source_badge": "derived",
                        "predicted_edge_bps": 900.0,
                        "realized_pnl": 5.7,
                        "evaluation_status": "resolved",
                    }
                ]
            ),
            "watch_only_vs_executed": pd.DataFrame(
                [
                    {
                        "market_id": "mkt_1",
                        "source_badge": "derived",
                        "avg_executable_edge_bps": 850.0,
                        "submission_capture_ratio": 0.5,
                        "fill_capture_ratio": 0.5,
                        "resolution_capture_ratio": 0.5,
                        "executed_ticket_count": 1,
                        "dominant_lifecycle_stage": "resolved",
                        "miss_reason_bucket": "captured",
                        "distortion_reason_bucket": "none",
                    }
                ]
            ),
            "execution_science": pd.DataFrame(
                [
                    {
                        "cohort_type": "strategy",
                        "cohort_key": "weather_primary",
                        "source_badge": "derived",
                        "ticket_count": 1,
                        "submission_capture_ratio": 1.0,
                        "fill_capture_ratio": 1.0,
                        "resolution_capture_ratio": 1.0,
                        "dominant_miss_reason_bucket": "captured_resolved",
                        "dominant_distortion_reason_bucket": "none",
                    }
                ]
            ),
            "cohort_history": pd.DataFrame(
                [
                    {
                        "run_id": "retro_1",
                        "market_id": "mkt_1",
                        "strategy_id": "weather_primary",
                        "ranking_decile": 1,
                        "top_k_bucket": "top_5",
                        "evaluation_status": "resolved",
                        "submitted_capture_ratio": 1.0,
                        "fill_capture_ratio": 1.0,
                        "resolution_capture_ratio": 1.0,
                        "avg_ranking_score": 0.42,
                        "avg_realized_pnl": 5.7,
                        "feedback_status": "healthy",
                        "calibration_freshness_status": "fresh",
                        "source_badge": "derived",
                    }
                ]
            ),
            "market_research": pd.DataFrame(),
            "calibration_health": pd.DataFrame(),
        }
        with patch("ui.pages.execution.load_execution_console_data", return_value=payload), \
            patch.object(execution.st, "markdown"), \
            patch.object(execution.st, "caption"), \
            patch.object(execution.st, "metric"), \
            patch.object(execution.st, "info"), \
            patch.object(execution.st, "success"), \
            patch.object(execution.st, "dataframe", new=MagicMock()) as dataframe_mock, \
            patch.object(execution.st, "selectbox", side_effect=["全部", "全部", "全部", "全部"]), \
            patch.object(execution.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]):
            execution.show()
        self.assertTrue(dataframe_mock.called)
        rendered_frames = [call.args[0] for call in dataframe_mock.call_args_list if call.args]
        self.assertTrue(any("source_badge" in getattr(frame, "columns", []) for frame in rendered_frames))

    def test_system_show_renders_evidence_bundle(self) -> None:
        readiness = {
            "go_decision": "GO",
            "decision_reason": "ready",
            "target": "p4_live_prerequisites",
            "phase_table": pd.DataFrame(),
            "capability_boundary_summary": {"manual_only": True, "default_off": True, "approve_usdc_only": True, "shadow_submitter_only": True},
            "capability_manifest_status": "valid",
        }
        evidence = {
            "exists": True,
            "decision_reason": "ready with evidence",
            "capability_manifest_status": "valid",
            "capability_boundary_summary": readiness["capability_boundary_summary"],
            "dependency_statuses": {"ui_lite_db": {"status": "ok", "updated_at": "2026-03-15T10:00:00+00:00", "path": "data/ui/asterion_ui_lite.duckdb"}},
            "evidence_paths": {"readiness_report_json": "data/ui/asterion_readiness_p4.json"},
            "blockers": [],
            "warnings": ["warn:smoke"],
        }
        status = {
            "ui_lite_exists": True,
            "ui_lite_db_path": "data/ui/asterion_ui_lite.duckdb",
            "ui_replica_db_path": "data/ui/asterion_ui.duckdb",
            "ui_replica_exists": True,
            "readiness_report_path": "data/ui/asterion_readiness_p4.json",
            "readiness_report_markdown_path": "data/ui/asterion_readiness_p4.md",
            "readiness_evidence_path": "data/ui/asterion_readiness_evidence_p4.json",
            "capability_manifest_path": "data/meta/controlled_live_capability_manifest.json",
            "weather_smoke_report_path": "data/dev/real_weather_chain/real_weather_chain_report.json",
            "capability_manifest_status": "valid",
            "weather_smoke_status": "ok",
            "opportunity_row_count": 1,
            "actionable_market_count": 1,
            "agent_row_count": 1,
        }
        surface = {
            "readiness": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None},
            "market_chain": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None},
            "agent_review": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None},
            "execution": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None},
            "overall": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None, "surface": "readiness"},
        }
        with patch("ui.pages.system.load_readiness_summary", return_value=readiness), \
            patch("ui.pages.system.load_readiness_evidence_bundle", return_value=evidence), \
            patch("ui.pages.system.load_system_runtime_status", return_value=status), \
            patch("ui.pages.system.load_operator_surface_status", return_value=surface), \
            patch.object(system.st, "markdown"), \
            patch.object(system.st, "caption"), \
            patch.object(system.st, "metric"), \
            patch.object(system.st, "info"), \
            patch.object(system.st, "warning"), \
            patch.object(system.st, "success"), \
            patch.object(system.st, "error"), \
            patch.object(system.st, "dataframe", new=MagicMock()) as dataframe_mock, \
            patch.object(system.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(system.st, "expander", return_value=_DummyContext()):
            system.show()
        self.assertTrue(dataframe_mock.called)

    def test_agents_show_renders_resolution_review_layout(self) -> None:
        review = pd.DataFrame(
            [
                {
                    "proposal_id": "prop_1",
                    "market_id": "mkt_1",
                    "proposal_status": "settled",
                    "redeem_decision": "ready_for_redeem",
                    "suggestion_id": "suggest_1",
                    "latest_agent_invocation_id": "inv_1",
                    "latest_agent_verdict": "review",
                    "latest_agent_summary": "needs review",
                    "latest_recommended_operator_action": "manual_review",
                    "latest_settlement_risk_score": 0.8,
                    "latest_operator_review_status": None,
                    "latest_operator_action": None,
                    "effective_redeem_status": "pending_operator_review",
                }
            ]
        )
        with patch("ui.pages.agents.load_agent_runtime_status", return_value={"provider": "openai_compatible", "model": "glm-5", "configured": True, "key_source": "env", "agents": []}), \
            patch("ui.pages.agents.load_resolution_review_data", return_value={"source": "ui_lite", "frame": review}), \
            patch("ui.pages.agents.write_resolution_operator_review_decision"), \
            patch.object(agents.st, "markdown"), \
            patch.object(agents.st, "caption"), \
            patch.object(agents.st, "metric"), \
            patch.object(agents.st, "info"), \
            patch.object(agents.st, "success"), \
            patch.object(agents.st, "error"), \
            patch.object(agents.st, "dataframe", new=MagicMock()) as dataframe_mock, \
            patch.object(agents.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(agents.st, "expander", return_value=_DummyContext()), \
            patch.object(agents.st, "text_input", side_effect=["operator", ""]), \
            patch.object(agents.st, "button", return_value=False):
            agents.show()
        self.assertTrue(dataframe_mock.called)

    def test_markets_show_renders_ranking_score_and_source_badge(self) -> None:
        payload = {
            "weather_smoke_report": {"chain_status": "ok", "market_discovery": {"market_source": "ui_lite", "selected_market_count": 1}},
            "market_opportunities": pd.DataFrame(
                [
                    {
                        "market_id": "mkt_1",
                        "location_name": "Seattle",
                        "question": "Seattle weather",
                        "best_side": "BUY",
                        "edge_bps": 850.0,
                        "edge_bps_model": 1000.0,
                        "ranking_score": 88.0,
                        "operator_bucket": "ready_now",
                        "source_badge": "canonical",
                        "source_truth_status": "canonical",
                        "liquidity_proxy": 75.0,
                        "mapping_confidence": 0.91,
                        "source_freshness_status": "fresh",
                        "market_quality_status": "pass",
                        "agent_review_status": "passed",
                        "actionability_status": "actionable",
                        "accepting_orders": True,
                        "market_close_time": "2026-03-18T12:00:00+00:00",
                        "expected_value_score": 65.0,
                        "expected_pnl_score": 55.0,
                    }
                ]
            ),
            "market_rows": [
                {
                    "market_id": "mkt_1",
                    "location_name": "Seattle",
                    "question": "Seattle weather",
                    "best_side": "BUY",
                    "edge_bps": 850.0,
                    "edge_bps_model": 1000.0,
                    "ranking_score": 88.0,
                    "source_badge": "canonical",
                    "source_truth_status": "canonical",
                    "operator_bucket": "ready_now",
                    "liquidity_proxy": 75.0,
                    "mapping_confidence": 0.91,
                    "source_freshness_status": "fresh",
                    "market_quality_status": "pass",
                    "agent_review_status": "passed",
                    "actionability_status": "actionable",
                    "accepting_orders": True,
                    "market_close_time": "2026-03-18T12:00:00+00:00",
                    "spec": {},
                    "forecast": {},
                    "pricing": {},
                    "signals": {},
                    "executed_evidence": {"has_executed_evidence": False},
                    "watch_only_vs_executed": {"source_badge": "derived"},
                    "market_research": {},
                    "queue_reason_codes": ["allocation:approved"],
                    "cohort_history": [],
                }
            ],
            "market_opportunity_source": "ui_lite",
            "predicted_vs_realized": pd.DataFrame(),
            "watch_only_vs_executed": pd.DataFrame(),
            "market_research": pd.DataFrame(),
            "cohort_history": pd.DataFrame(),
        }
        surface = {
            "market_chain": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None},
        }
        with patch("ui.pages.markets.load_market_chain_analysis_data", return_value=payload), \
            patch("ui.pages.markets.load_operator_surface_status", return_value=surface), \
            patch.object(markets.st, "markdown"), \
            patch.object(markets.st, "caption"), \
            patch.object(markets.st, "metric"), \
            patch.object(markets.st, "info"), \
            patch.object(markets.st, "success"), \
            patch.object(markets.st, "warning"), \
            patch.object(markets.st, "error"), \
            patch.object(markets.st, "dataframe", new=MagicMock()) as dataframe_mock, \
            patch.object(markets.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(markets.st, "selectbox", side_effect=["All", "All", "All", "All", "All", "mkt_1"]), \
            patch.object(markets.st, "checkbox", return_value=False), \
            patch.object(markets.st, "expander", return_value=_DummyContext()), \
            patch.object(markets.st, "code"):
            markets.show()
        rendered_frames = [call.args[0] for call in dataframe_mock.call_args_list if call.args]
        self.assertTrue(any("source_badge" in getattr(frame, "columns", []) for frame in rendered_frames))


if __name__ == "__main__":
    unittest.main()
