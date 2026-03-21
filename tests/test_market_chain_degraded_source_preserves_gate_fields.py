from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from ui.loaders.markets_loader import load_market_chain_analysis_data


class MarketChainDegradedSourceGateFieldsTest(unittest.TestCase):
    def test_degraded_source_rows_keep_persisted_gate_and_scaling_fields(self) -> None:
        watch_payload = {
            "market_watch": pd.DataFrame(),
            "weather_smoke_report": {
                "market_discovery": {
                    "selected_markets": [
                        {
                            "market_id": "mkt_1",
                            "question": "Seattle weather",
                            "location_name": "Seattle",
                            "station_id": "KSEA",
                            "accepting_orders": True,
                            "forecast_status": "failure",
                            "forecast_summary": "pricing unavailable",
                        }
                    ]
                }
            }
        }
        empty_opportunities = {"frame": pd.DataFrame(), "source": "weather_smoke_db"}
        empty_predicted = {"frame": pd.DataFrame(), "source": "ui_lite"}
        execution_payload = {
            "watch_only_vs_executed": pd.DataFrame(),
            "market_research": pd.DataFrame(),
            "cohort_history": pd.DataFrame(),
        }
        action_queue = pd.DataFrame(
            [
                {
                    "market_id": "mkt_1",
                    "operator_bucket": "review_required",
                    "queue_reason_codes_json": '["calibration_gate:research_only"]',
                    "calibration_gate_status": "research_only",
                    "calibration_gate_reason_codes_json": '["threshold_probability_quality_sparse"]',
                    "calibration_impacted_market": True,
                    "capital_policy_id": "cap_policy_1",
                    "capital_policy_version": "v1",
                    "capital_scaling_reason_codes_json": '["gate_blocked:research_only"]',
                }
            ]
        )
        with (
            patch("ui.data_access.load_market_watch_data", return_value=watch_payload),
            patch("ui.data_access.load_market_opportunity_data", return_value=empty_opportunities),
            patch("ui.data_access.load_predicted_vs_realized_data", return_value=empty_predicted),
            patch("ui.loaders.markets_loader.load_execution_console_data", return_value=execution_payload),
            patch("ui.data_access.load_market_validation_overlays", return_value={}),
            patch(
                "ui.data_access.load_ui_lite_snapshot",
                return_value={"tables": {"action_queue_summary": action_queue}},
            ),
        ):
            payload = load_market_chain_analysis_data()

        row = payload["market_rows"][0]
        self.assertEqual(row["calibration_gate_status"], "research_only")
        self.assertEqual(row["calibration_gate_reason_codes"], ["threshold_probability_quality_sparse"])
        self.assertTrue(row["calibration_impacted_market"])
        self.assertEqual(row["capital_policy_id"], "cap_policy_1")
        self.assertEqual(row["capital_scaling_reason_codes"], ["gate_blocked:research_only"])


if __name__ == "__main__":
    unittest.main()
