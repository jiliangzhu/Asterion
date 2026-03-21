from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from ui.pages import home, markets, system


class _DummyContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class P8OperatorSurfaceAcceptanceTest(unittest.TestCase):
    def test_home_markets_and_system_consume_persisted_calibration_gate_fields(self) -> None:
        home_payload = {
            "surface_status": {"overall": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None}},
            "readiness": {"target": "p4_live_prerequisites", "decision_reason": "ok", "failed_gate_names": []},
            "execution": {"exceptions": pd.DataFrame()},
            "market_data": {"weather_smoke_report": {}, "market_opportunity_source": "ui_lite"},
            "metrics": {
                "go_decision": "GO",
                "actionable_market_count": 1,
                "weather_market_count": 1,
                "top_opportunity_score": 0.8,
                "action_queue_count": 1,
                "ready_now_count": 0,
                "high_risk_count": 0,
                "review_required_count": 1,
                "blocked_count": 0,
                "research_only_count": 0,
                "liquidity_ready_count": 1,
                "highest_edge_bps": 900.0,
                "weather_locations": ["Seattle"],
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
                        "ranking_score": 0.8,
                        "base_ranking_score": 0.4,
                        "pre_budget_deployable_expected_pnl": 1.4,
                        "recommended_size": 0.0,
                        "allocation_status": "blocked",
                        "calibration_gate_status": "review_required",
                        "capital_policy_id": "cap_review",
                        "budget_impact": {"preview": {"requested_size": 5.0}, "capital_scaling_reason_codes": ["calibration_gate_review_required"]},
                    }
                ]
            ),
            "action_queue": pd.DataFrame(
                [
                    {
                        "location_name": "Seattle",
                        "question": "Seattle weather",
                        "best_side": "BUY",
                        "ranking_score": 0.8,
                        "base_ranking_score": 0.4,
                        "recommended_size": 0.0,
                        "allocation_status": "blocked",
                        "operator_bucket": "review_required",
                        "actionability_status": "review_required",
                        "calibration_gate_status": "review_required",
                        "queue_reason_codes_json": '["calibration_gate:review_required"]',
                        "capital_scaling_reason_codes_json": '["calibration_gate_review_required"]',
                    }
                ]
            ),
            "largest_blocker": {"source": "calibration", "summary": "hard gate"},
            "recent_agent_summary": {},
            "agent_data": {"frame": pd.DataFrame()},
            "readiness_evidence": {"exists": False, "blockers": [], "warnings": [], "decision_reason": "", "capability_manifest_status": "missing"},
            "predicted_vs_realized_snapshot": pd.DataFrame(),
            "degraded_inputs": [],
            "uncaptured_high_edge_markets": pd.DataFrame(),
        }
        markets_payload = {
            "weather_smoke_report": {},
            "market_opportunities": pd.DataFrame(
                [
                    {
                        "market_id": "mkt_1",
                        "location_name": "Seattle",
                        "question": "Seattle weather",
                        "best_side": "BUY",
                        "ranking_score": 0.8,
                        "operator_bucket": "review_required",
                        "calibration_gate_status": "review_required",
                        "calibration_gate_reason_codes": ["calibration_freshness_stale"],
                        "capital_policy_id": "cap_review",
                        "capital_scaling_reason_codes": ["calibration_gate_review_required"],
                        "calibration_freshness_status": "stale",
                        "accepting_orders": True,
                        "market_close_time": "2026-03-19T12:00:00+00:00",
                        "actionability_status": "review_required",
                        "queue_reason_codes": ["calibration_gate:review_required"],
                        "cohort_history": [],
                    }
                ]
            ),
            "market_rows": [],
        }
        system_status = {
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
            "latest_calibration_freshness_status": "stale",
            "latest_calibration_materialized_at": "2026-03-18 03:15:00",
            "latest_calibration_window_end": "2026-03-18 02:00:00",
            "latest_calibration_profile_age_hours": 48.0,
            "calibration_impacted_market_count": 4,
            "calibration_hard_gate_market_count": 3,
            "calibration_review_required_market_count": 2,
            "calibration_research_only_market_count": 1,
            "table_row_counts": {},
            "ui_lite_exists": True,
            "ui_replica_exists": True,
        }

        with patch("ui.pages.home.load_home_decision_snapshot", return_value=home_payload), \
            patch.object(home.st, "markdown"), patch.object(home.st, "caption"), patch.object(home.st, "metric"), \
            patch.object(home.st, "info"), patch.object(home.st, "success"), patch.object(home.st, "error"), patch.object(home.st, "write"), \
            patch.object(home.st, "warning"), patch.object(home.st, "dataframe", new=MagicMock()), \
            patch.object(home.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]):
            home.show()

        with patch("ui.pages.markets.load_market_chain_analysis_data", return_value=markets_payload), \
            patch("ui.pages.markets.load_operator_surface_status", return_value={"market_chain": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None}}), \
            patch.object(markets.st, "markdown"), patch.object(markets.st, "caption"), patch.object(markets.st, "info"), patch.object(markets.st, "warning"), \
            patch.object(markets.st, "dataframe", new=MagicMock()), \
            patch.object(markets.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(markets.st, "selectbox", side_effect=lambda label, options, **kwargs: options[0] if options else "All"), \
            patch.object(markets.st, "checkbox", return_value=False):
            markets.show()

        rows = system._build_component_rows(system_status, {"report": {"target": "p4_live_prerequisites"}})
        self.assertTrue(any(row["组件"] == "Calibration Profiles v2" for row in rows))
        self.assertIn("hard_gate=3", next(row["详情"] for row in rows if row["组件"] == "Calibration Profiles v2"))


if __name__ == "__main__":
    unittest.main()
