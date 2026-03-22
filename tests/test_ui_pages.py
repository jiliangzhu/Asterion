from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from ui.pages import home
from ui.pages import markets, system


class _DummyContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class UiPagesSmokeTest(unittest.TestCase):
    def test_home_show_renders_surface_warning_without_crashing(self) -> None:
        fake_snapshot = {
            "surface_status": {
                "readiness": {"status": "ok", "label": "Readiness 正常", "detail": "", "source": "ui_lite", "updated_at": None},
                "market_chain": {"status": "degraded_source", "label": "Market 降级", "detail": "using fallback", "source": "weather_smoke_db", "updated_at": None},
                "agent_review": {"status": "no_data", "label": "Agent 暂无数据", "detail": "no agent rows", "source": "smoke_report", "updated_at": None},
                "execution": {"status": "ok", "label": "Execution 正常", "detail": "", "source": "ui_lite", "updated_at": None},
                "overall": {"status": "degraded_source", "label": "Market 降级", "detail": "using fallback", "source": "weather_smoke_db", "updated_at": None, "surface": "market_chain"},
            },
            "readiness": {"target": "p4_live_prerequisites", "decision_reason": "ok", "failed_gate_names": []},
            "execution": {"exceptions": pd.DataFrame()},
            "market_data": {"weather_smoke_report": {}, "market_opportunity_source": "weather_smoke_db"},
            "metrics": {
                "go_decision": "GO",
                "actionable_market_count": 0,
                "weather_market_count": 0,
                "top_opportunity_score": 0.0,
                "action_queue_count": 1,
                "ready_now_count": 1,
                "high_risk_count": 0,
                "review_required_count": 0,
                "blocked_count": 0,
                "research_only_count": 0,
                "liquidity_ready_count": 0,
                "highest_edge_bps": 0.0,
                "weather_locations": [],
                "agent_activity_count": 0,
                "agent_review_required_count": 0,
                "resolved_trade_count": 0,
                "pending_resolution_count": 0,
                "avg_predicted_edge_bps": 0.0,
                "avg_realized_pnl": 0.0,
            },
            "wallet_attention": pd.DataFrame(),
            "top_opportunities": pd.DataFrame(
                [
                    {
                        "location_name": "Seattle",
                        "question": "Seattle weather",
                        "best_side": "BUY",
                        "edge_bps": 900.0,
                        "edge_bps_model": 1100.0,
                        "ranking_score": 88.0,
                        "capital_policy_id": "cap_policy_1",
                        "calibration_gate_status": "review_required",
                        "source_badge": "fallback",
                        "source_truth_status": "fallback",
                        "mapping_confidence": 0.92,
                        "source_freshness_status": "fresh",
                        "market_quality_status": "pass",
                        "agent_review_status": "passed",
                        "actionability_status": "actionable",
                    }
                ]
            ),
            "action_queue": pd.DataFrame(
                [
                    {
                        "location_name": "Seattle",
                        "question": "Seattle weather",
                        "best_side": "BUY",
                        "ranking_score": 88.0,
                        "base_ranking_score": 82.0,
                        "pre_budget_deployable_expected_pnl": 4.2,
                        "deployable_expected_pnl": 3.8,
                        "operator_bucket": "ready_now",
                        "recommended_size": 4.0,
                        "allocation_status": "resized",
                        "actionability_status": "actionable",
                        "calibration_gate_status": "review_required",
                        "surface_delivery_status": "degraded_source",
                        "surface_fallback_origin": "runtime_db",
                        "surface_last_refresh_ts": "2026-03-21T11:00:00+00:00",
                        "capital_policy_id": "cap_policy_1",
                        "capital_scaling_reason_codes_json": '["regime_bucket:tight"]',
                        "queue_reason_codes_json": '["allocation:resized"]',
                    }
                ]
            ),
            "largest_blocker": {"source": "clear", "summary": "No material blocker"},
            "recent_agent_summary": {},
            "agent_data": {"frame": pd.DataFrame()},
            "readiness_evidence": {"exists": False, "blockers": [], "warnings": [], "decision_reason": "", "capability_manifest_status": "missing"},
            "predicted_vs_realized_snapshot": pd.DataFrame(),
            "degraded_inputs": ["market_source:weather_smoke_db"],
            "uncaptured_high_edge_markets": pd.DataFrame(
                [
                    {
                        "market_id": "mkt_1",
                        "avg_executable_edge_bps": 700.0,
                        "submission_capture_ratio": 0.0,
                        "fill_capture_ratio": 0.0,
                        "resolution_capture_ratio": 0.0,
                        "source_badge": "derived",
                        "miss_reason_bucket": "not_submitted",
                        "distortion_reason_bucket": "ranking_distortion",
                    }
                ]
            ),
        }

        with patch("ui.pages.home.load_home_decision_snapshot", return_value=fake_snapshot), \
            patch.object(home.st, "markdown", new=MagicMock()) as markdown_mock, \
            patch.object(home.st, "caption"), \
            patch.object(home.st, "subheader", new=MagicMock()) as subheader_mock, \
            patch.object(home.st, "metric"), \
            patch.object(home.st, "info"), \
            patch.object(home.st, "success"), \
            patch.object(home.st, "error"), \
            patch.object(home.st, "write", new=MagicMock()) as write_mock, \
            patch.object(home.st, "dataframe", new=MagicMock()) as dataframe_mock, \
            patch.object(home.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(home.st, "warning", new=MagicMock()) as warning_mock:
            home.show()

        warning_mock.assert_called()
        self.assertTrue(any("Decision Console" in str(call.args[0]) for call in subheader_mock.call_args_list if call.args))
        rendered_frames = [call.args[0] for call in dataframe_mock.call_args_list if call.args]
        self.assertTrue(any("source_badge" in getattr(frame, "columns", []) for frame in rendered_frames))
        self.assertTrue(any("calibration_gate_status" in getattr(frame, "columns", []) for frame in rendered_frames))
        self.assertTrue(any("cap_policy_1" in str(call.args[0]) for call in write_mock.call_args_list if call.args))

    def test_markets_show_renders_calibration_freshness_without_crashing(self) -> None:
        fake_payload = {
            "weather_smoke_report": {},
            "market_opportunities": pd.DataFrame(
                [
                    {
                        "market_id": "mkt_1",
                        "location_name": "Seattle",
                        "question": "Seattle weather",
                        "best_side": "BUY",
                        "edge_bps": 900.0,
                        "edge_bps_model": 1100.0,
                        "ranking_score": 0.42,
                        "operator_bucket": "high_risk",
                        "source_badge": "canonical",
                        "source_truth_status": "canonical",
                        "mapping_confidence": 0.92,
                        "source_freshness_status": "fresh",
                        "calibration_freshness_status": "stale",
                        "calibration_gate_status": "review_required",
                        "calibration_gate_reason_codes": ["calibration_freshness_stale"],
                        "calibration_impacted_market": True,
                        "capital_policy_id": "cap_policy_1",
                        "capital_scaling_reason_codes": ["gate_blocked:review_required"],
                        "pre_budget_deployable_expected_pnl": 1.8,
                        "base_ranking_score": 0.57,
                        "calibration_profile_materialized_at": "2026-03-18T03:15:00+00:00",
                        "calibration_profile_window_end": "2026-03-18T02:00:00+00:00",
                        "calibration_profile_age_hours": 48.0,
                        "market_quality_status": "pass",
                        "agent_review_status": "passed",
                        "actionability_status": "actionable",
                        "accepting_orders": True,
                        "market_close_time": "2026-03-19T12:00:00+00:00",
                        "queue_reason_codes": ["feedback_status:degraded"],
                        "cohort_history": [
                            {
                                "run_id": "retro_1",
                                "strategy_id": "weather_primary",
                                "ranking_decile": 1,
                                "top_k_bucket": "top_5",
                                "evaluation_status": "resolved",
                                "submitted_capture_ratio": 1.0,
                                "fill_capture_ratio": 1.0,
                                "resolution_capture_ratio": 1.0,
                                "avg_ranking_score": 0.42,
                                "avg_realized_pnl": 0.08,
                                "forecast_replay_change_rate": 0.0,
                                "feedback_status": "degraded",
                                "calibration_freshness_status": "stale",
                                "source_badge": "canonical",
                            }
                        ],
                    }
                ]
            ),
            "market_rows": [],
        }
        fake_surface = {
            "market_chain": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None}
        }
        dataframe_mock = MagicMock()
        markdown_mock = MagicMock()
        with patch("ui.pages.markets.load_market_chain_analysis_data", return_value=fake_payload), \
            patch("ui.pages.markets.load_operator_surface_status", return_value=fake_surface), \
            patch.object(markets.st, "markdown", new=markdown_mock), \
            patch.object(markets.st, "caption"), \
            patch.object(markets.st, "subheader", new=MagicMock()) as subheader_mock, \
            patch.object(markets.st, "info"), \
            patch.object(markets.st, "warning"), \
            patch.object(markets.st, "write", new=MagicMock()) as write_mock, \
            patch.object(markets.st, "dataframe", new=dataframe_mock), \
            patch.object(markets.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(markets.st, "selectbox", side_effect=lambda label, options, **kwargs: options[0] if options else "All"), \
            patch.object(markets.st, "checkbox", return_value=False), \
            patch.object(markets.st, "tabs", side_effect=lambda names: [_DummyContext() for _ in names]):
            markets.show()
        self.assertTrue(any("Opportunity Terminal" in str(call.args[0]) for call in subheader_mock.call_args_list if call.args))
        rendered_frames = [call.args[0] for call in dataframe_mock.call_args_list if call.args]
        self.assertTrue(any("calibration_freshness_status" in getattr(frame, "columns", []) for frame in rendered_frames))
        self.assertTrue(any("cap_policy_1" in str(call.args[0]) for call in write_mock.call_args_list if call.args))

    def test_system_component_rows_include_calibration_profiles_component(self) -> None:
        rows = system._build_component_rows(
            {
                "capability_manifest_status": "valid",
                "capability_manifest_path": "manifest.json",
                "ui_lite_exists": True,
                "ui_lite_db_path": "lite.duckdb",
                "ui_replica_exists": True,
                "ui_replica_db_path": "replica.duckdb",
                "readiness_report_exists": True,
                "readiness_report_path": "readiness.json",
                "readiness_report_markdown_exists": True,
                "readiness_report_markdown_path": "readiness.md",
                "weather_smoke_status": "ok",
                "weather_smoke_report_path": "smoke.json",
                "latest_calibration_freshness_status": "fresh",
                "latest_calibration_materialized_at": "2026-03-18 03:15:00",
                "latest_calibration_window_end": "2026-03-18 02:00:00",
                "latest_calibration_profile_age_hours": 1.0,
            },
            {"report": {"target": "p4_live_prerequisites"}},
        )
        self.assertTrue(any(row["组件"] == "Calibration Profiles v2" for row in rows))

    def test_system_show_renders_calibration_gate_counts_without_crashing(self) -> None:
        readiness = {"go_decision": "GO", "target": "p4_live_prerequisites", "failed_gate_names": [], "phase_table": pd.DataFrame()}
        evidence = {
            "decision_reason": "ready",
            "capability_boundary_summary": {"manual_only": True, "default_off": True, "approve_usdc_only": True, "shadow_submitter_only": True, "constrained_real_submit_enabled": False, "manifest_status": "valid"},
            "capability_manifest_status": "valid",
            "dependency_statuses": {},
            "evidence_paths": {},
            "blockers": [],
            "warnings": [],
        }
        status = {
            "ui_lite_exists": True,
            "ui_lite_db_path": "lite.duckdb",
            "weather_smoke_status": "ok",
            "latest_surface_refresh_status": "degraded_source",
            "degraded_surface_count": 1,
            "read_error_surface_count": 0,
            "calibration_impacted_market_count": 3,
            "calibration_hard_gate_market_count": 2,
            "calibration_review_required_market_count": 1,
            "calibration_research_only_market_count": 1,
            "capability_manifest_status": "valid",
            "capability_manifest_path": "manifest.json",
            "ui_replica_exists": True,
            "ui_replica_db_path": "replica.duckdb",
            "readiness_report_exists": True,
            "readiness_report_path": "readiness.json",
            "readiness_report_markdown_exists": True,
            "readiness_report_markdown_path": "readiness.md",
            "weather_smoke_report_path": "smoke.json",
            "latest_calibration_freshness_status": "fresh",
            "latest_calibration_materialized_at": "2026-03-18 03:15:00",
            "latest_calibration_window_end": "2026-03-18 02:00:00",
            "latest_calibration_profile_age_hours": 1.0,
            "table_row_counts": {},
            "readiness_evidence_path": "evidence.json",
            "opportunity_row_count": 1,
            "actionable_market_count": 1,
            "agent_row_count": 0,
            "pending_operator_review_count": 0,
            "blocked_by_operator_review_count": 0,
            "ready_for_redeem_review_count": 0,
            "surface_delivery_summary": pd.DataFrame(
                [
                    {
                        "surface_id": "markets",
                        "delivery_status": "degraded_source",
                        "fallback_origin": "runtime_db",
                        "last_refresh_ts": "2026-03-21T10:00:00+00:00",
                    }
                ]
            ),
        }
        dataframe_mock = MagicMock()
        markdown_mock = MagicMock()
        with patch("ui.pages.system.load_readiness_summary", return_value=readiness), \
            patch("ui.pages.system.load_readiness_evidence_bundle", return_value=evidence), \
            patch("ui.pages.system.load_system_runtime_status", return_value=status), \
            patch("ui.pages.system.load_operator_surface_status", return_value={"overall": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite"}, "market_chain": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite"}}), \
            patch.object(system.st, "markdown", new=markdown_mock), \
            patch.object(system.st, "caption"), \
            patch.object(system.st, "subheader", new=MagicMock()) as subheader_mock, \
            patch.object(system.st, "metric"), \
            patch.object(system.st, "info"), \
            patch.object(system.st, "warning"), \
            patch.object(system.st, "success"), \
            patch.object(system.st, "error"), \
            patch.object(system.st, "dataframe", new=dataframe_mock), \
            patch.object(system.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(system.st, "expander", side_effect=lambda *args, **kwargs: _DummyContext()), \
            patch.object(system.st, "tabs", side_effect=lambda names: [_DummyContext() for _ in names]):
            system.show()
        self.assertTrue(any("Readiness Evidence" in str(call.args[0]) for call in subheader_mock.call_args_list if call.args))
        rendered_frames = [call.args[0] for call in dataframe_mock.call_args_list if call.args]
        self.assertTrue(
            any(
                "Metric" in getattr(frame, "columns", [])
                and {"Hard-Gated Markets", "Review Required Markets", "Research Only Markets"}.issubset(set(frame["Metric"].tolist()))
                for frame in rendered_frames
            )
        )
        self.assertTrue(
            any(
                "组件" in getattr(frame, "columns", [])
                and {"Latest Surface Refresh", "Degraded Surfaces", "Read Error Surfaces"}.issubset(set(frame["组件"].tolist()))
                for frame in rendered_frames
            )
        )


if __name__ == "__main__":
    unittest.main()
