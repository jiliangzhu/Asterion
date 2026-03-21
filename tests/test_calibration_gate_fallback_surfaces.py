from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ui.data_access import load_market_chain_analysis_data


class CalibrationGateFallbackSurfacesTest(unittest.TestCase):
    def test_partial_report_rows_keep_deterministic_gate_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "real_weather_chain_report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "chain_status": "degraded",
                        "market_discovery": {
                            "selected_markets": [
                                {
                                    "market_id": "mkt_den_1",
                                    "question": "Denver weather",
                                    "location_name": "Denver",
                                    "station_id": "KDEN",
                                    "accepting_orders": True,
                                    "forecast_status": "failure",
                                    "forecast_summary": "pricing missing",
                                }
                            ]
                        },
                        "pricing_engine": {"status": "degraded", "markets": []},
                        "opportunity_discovery": {
                            "status": "degraded",
                            "markets": [
                                {
                                    "market_id": "mkt_den_1",
                                    "signals": [
                                        {
                                            "outcome": "YES",
                                            "decision": "TAKE",
                                            "calibration_freshness_status": "degraded_or_missing",
                                            "calibration_health_status": "lookup_missing",
                                            "threshold_probability_quality": "sparse",
                                            "sample_count": 0,
                                        }
                                    ],
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "ASTERION_UI_LITE_DB_PATH": str(Path(tmpdir) / "missing_ui_lite.duckdb"),
                    "ASTERION_REAL_WEATHER_CHAIN_REPORT_PATH": str(report_path),
                },
                clear=False,
            ):
                payload = load_market_chain_analysis_data()

        opportunity_row = payload["market_opportunities"].iloc[0].to_dict()
        market_row = payload["market_rows"][0]
        self.assertEqual(opportunity_row["calibration_gate_status"], "research_only")
        self.assertIn("threshold_probability_quality_sparse", opportunity_row["calibration_gate_reason_codes"])
        self.assertTrue(opportunity_row["calibration_impacted_market"])
        self.assertEqual(market_row["calibration_gate_status"], "research_only")
        self.assertIn("calibration_sample_count_low", market_row["calibration_gate_reason_codes"])
        self.assertTrue(market_row["calibration_impacted_market"])


if __name__ == "__main__":
    unittest.main()
