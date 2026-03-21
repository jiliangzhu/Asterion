from __future__ import annotations

import unittest

from domains.weather.opportunity import build_weather_opportunity_assessment


class CalibrationGateDefaultClearTest(unittest.TestCase):
    def test_assessment_without_calibration_context_keeps_gate_clear(self) -> None:
        assessment = build_weather_opportunity_assessment(
            market_id="mkt_weather_1",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.44,
            model_fair_value=0.62,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            agent_review_status="passed",
            source_context={},
        )
        self.assertEqual(assessment.calibration_gate_status, "clear")
        self.assertEqual(assessment.calibration_gate_reason_codes, [])


if __name__ == "__main__":
    unittest.main()
