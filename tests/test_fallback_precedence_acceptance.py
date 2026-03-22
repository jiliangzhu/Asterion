from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from ui import data_access


class FallbackPrecedenceAcceptanceTest(unittest.TestCase):
    def test_market_opportunity_data_prefers_ui_lite_over_runtime_and_smoke_report(self) -> None:
        ui_lite_frame = pd.DataFrame([{"market_id": "mkt_ui", "ranking_score": 9.0, "source_badge": "canonical"}])
        runtime_frame = pd.DataFrame([{"market_id": "mkt_runtime", "ranking_score": 7.0}])
        with patch(
            "ui.data_access.load_ui_lite_snapshot",
            return_value={"tables": {"market_opportunity_summary": ui_lite_frame}, "exists": True, "read_error": None},
        ), patch(
            "ui.data_access._read_weather_market_rows_from_runtime_result",
            return_value={"frame": runtime_frame, "error": None},
        ), patch(
            "ui.data_access.load_real_weather_smoke_report",
            return_value={"chain_status": "ok", "markets": [{"market_id": "mkt_smoke"}]},
        ):
            payload = data_access.load_market_opportunity_data()
        self.assertEqual(payload["source"], "ui_lite")
        self.assertEqual(payload["frame"].iloc[0]["market_id"], "mkt_ui")

    def test_market_opportunity_data_prefers_runtime_db_before_smoke_report(self) -> None:
        runtime_frame = pd.DataFrame([{"market_id": "mkt_runtime", "ranking_score": 7.0, "source_badge": "fallback"}])
        with patch(
            "ui.data_access.load_ui_lite_snapshot",
            return_value={"tables": {"market_opportunity_summary": pd.DataFrame()}, "exists": False, "read_error": None},
        ), patch(
            "ui.data_access._read_weather_market_rows_from_runtime_result",
            return_value={"frame": runtime_frame, "error": None},
        ), patch(
            "ui.data_access.load_real_weather_smoke_report",
            return_value={"chain_status": "ok", "markets": [{"market_id": "mkt_smoke"}]},
        ):
            payload = data_access.load_market_opportunity_data()
        self.assertEqual(payload["source"], "weather_smoke_db")
        self.assertEqual(payload["frame"].iloc[0]["market_id"], "mkt_runtime")


if __name__ == "__main__":
    unittest.main()
