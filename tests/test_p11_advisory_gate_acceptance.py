from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from ui.loaders.home_loader import build_ops_console_overview


class P11AdvisoryGateAcceptanceTest(unittest.TestCase):
    def test_home_does_not_merge_experimental_triage_into_action_queue(self) -> None:
        opportunities = pd.DataFrame(
            [
                {
                    "market_id": "mkt_1",
                    "question": "Will Seattle be cold?",
                    "location_name": "Seattle",
                    "actionability_status": "actionable",
                    "ranking_score": 0.82,
                    "edge_bps": 800.0,
                    "accepting_orders": True,
                    "liquidity_proxy": 75.0,
                }
            ]
        )
        queue = pd.DataFrame(
            [
                {
                    "market_id": "mkt_1",
                    "queue_item_id": "queue:mkt_1",
                    "operator_bucket": "review_required",
                    "queue_priority": 1,
                    "ranking_score": 0.82,
                    "updated_at": "2026-03-23T10:00:00+00:00",
                    "surface_delivery_status": "ok",
                }
            ]
        )
        triage = pd.DataFrame(
            [
                {
                    "market_id": "mkt_1",
                    "priority_band": "high",
                    "recommended_operator_action": "manual_review",
                    "effective_triage_status": "review",
                    "latest_agent_status": "success",
                    "advisory_gate_status": "experimental",
                }
            ]
        )
        empty = pd.DataFrame()
        with (
            patch("ui.data_access.load_readiness_summary", return_value={"go_decision": "GO", "failed_gate_names": []}),
            patch("ui.data_access.load_readiness_evidence_bundle", return_value={"capability_manifest_status": "valid", "blockers": [], "stale_dependencies": []}),
            patch("ui.loaders.home_loader.load_execution_console_data", return_value={"watch_only_vs_executed": empty, "execution_science": empty, "calibration_health": empty, "live_prereq": empty, "exceptions": empty}),
            patch("ui.data_access.load_wallet_readiness_data", return_value=empty),
            patch("ui.data_access.load_market_watch_data", return_value={"weather_smoke_report": {}}),
            patch("ui.loaders.home_loader.load_market_chain_analysis_data", return_value={"market_opportunities": opportunities, "market_opportunity_source": "ui_lite"}),
            patch("ui.data_access.load_agent_review_data", return_value={"frame": empty}),
            patch("ui.data_access.load_opportunity_triage_data", return_value={"frame": triage}),
            patch("ui.data_access.load_predicted_vs_realized_data", return_value={"frame": empty}),
            patch("ui.data_access.load_ui_lite_snapshot", return_value={"tables": {"action_queue_summary": queue}, "table_row_counts": {}, "read_error": None}),
            patch("ui.data_access.load_operator_surface_status", return_value={"overall": {"status": "ok", "label": "ok", "detail": "", "source": "ui_lite", "updated_at": None}}),
            patch("ui.data_access.load_surface_delivery_summary", return_value=empty),
            patch("ui.data_access.load_boundary_sidebar_truth", return_value={}),
            patch("ui.data_access.load_primary_score_descriptor", return_value=SimpleNamespace(primary_score_label="ranking_score")),
        ):
            payload = build_ops_console_overview()

        action_row = payload["action_queue"].iloc[0].to_dict()
        self.assertNotIn("priority_band", action_row)
        self.assertEqual(payload["triage_data"]["frame"].iloc[0]["advisory_gate_status"], "experimental")


if __name__ == "__main__":
    unittest.main()
