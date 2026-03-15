from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from ui.pages import home


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
            "top_opportunities": pd.DataFrame(),
            "largest_blocker": {"source": "clear", "summary": "No material blocker"},
            "recent_agent_summary": {},
            "agent_data": {"frame": pd.DataFrame()},
            "readiness_evidence": {"exists": False, "blockers": [], "warnings": [], "decision_reason": "", "capability_manifest_status": "missing"},
            "predicted_vs_realized_snapshot": pd.DataFrame(),
            "degraded_inputs": ["market_source:weather_smoke_db"],
        }

        with patch("ui.pages.home.load_home_decision_snapshot", return_value=fake_snapshot), \
            patch.object(home.st, "markdown"), \
            patch.object(home.st, "caption"), \
            patch.object(home.st, "metric"), \
            patch.object(home.st, "info"), \
            patch.object(home.st, "success"), \
            patch.object(home.st, "error"), \
            patch.object(home.st, "write"), \
            patch.object(home.st, "dataframe"), \
            patch.object(home.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(home.st, "warning", new=MagicMock()) as warning_mock:
            home.show()

        warning_mock.assert_called()


if __name__ == "__main__":
    unittest.main()
