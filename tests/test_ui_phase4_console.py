from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from ui.pages import agents, execution, system


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
                        "predicted_edge_bps": 900.0,
                        "realized_pnl": 5.7,
                        "evaluation_status": "resolved",
                    }
                ]
            ),
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

    def test_agents_show_renders_exception_review_layout(self) -> None:
        review = pd.DataFrame(
            [
                {
                    "agent_type": "rule2spec",
                    "subject_id": "mkt_1",
                    "invocation_status": "failure",
                    "verdict": "review",
                    "summary": "needs review",
                    "human_review_required": True,
                    "updated_at": "2026-03-15T10:00:00+00:00",
                }
            ]
        )
        with patch("ui.pages.agents.load_agent_runtime_status", return_value={"provider": "openai_compatible", "model": "glm-5", "configured": True, "key_source": "env", "agents": []}), \
            patch("ui.pages.agents.load_agent_review_data", return_value={"source": "ui_lite", "frame": review}), \
            patch.object(agents.st, "markdown"), \
            patch.object(agents.st, "caption"), \
            patch.object(agents.st, "metric"), \
            patch.object(agents.st, "info"), \
            patch.object(agents.st, "success"), \
            patch.object(agents.st, "error"), \
            patch.object(agents.st, "dataframe", new=MagicMock()) as dataframe_mock, \
            patch.object(agents.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(agents.st, "expander", return_value=_DummyContext()):
            agents.show()
        self.assertTrue(dataframe_mock.called)


if __name__ == "__main__":
    unittest.main()
