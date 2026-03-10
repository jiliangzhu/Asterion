from __future__ import annotations

import unittest

from asterion_core.execution import decide_watch_only


class WatchOnlyGateTest(unittest.TestCase):
    def test_backlog_crossing_threshold_enables_watch_only(self) -> None:
        result = decide_watch_only(
            was_watch_only=False,
            rolling_backlog_p95_ms=1200,
            backlog_p95_ms_max=1000,
            backlog_p95_ms_recover=500,
            dq_pass_rate_5m=1.0,
            dq_pass_rate_min=0.95,
            ws_coverage_5m=1.0,
            ws_coverage_min=0.9,
            risk_source_prior_share=0.0,
            risk_source_prior_share_max=0.2,
            risk_source_sample_n=10,
            risk_source_min_samples=5,
        )
        self.assertTrue(result["watch_only"])
        self.assertIn("backlog", result["reason_codes"])

    def test_recover_threshold_applies_when_already_watch_only(self) -> None:
        result = decide_watch_only(
            was_watch_only=True,
            rolling_backlog_p95_ms=700,
            backlog_p95_ms_max=1000,
            backlog_p95_ms_recover=500,
            dq_pass_rate_5m=1.0,
            dq_pass_rate_min=0.95,
            ws_coverage_5m=1.0,
            ws_coverage_min=0.9,
            risk_source_prior_share=0.0,
            risk_source_prior_share_max=0.2,
            risk_source_sample_n=10,
            risk_source_min_samples=5,
        )
        self.assertTrue(result["watch_only"])
        self.assertIn("backlog", result["reason_codes"])

    def test_dq_ws_and_risk_signals_can_trigger_watch_only(self) -> None:
        result = decide_watch_only(
            was_watch_only=False,
            rolling_backlog_p95_ms=0,
            backlog_p95_ms_max=1000,
            backlog_p95_ms_recover=500,
            dq_pass_rate_5m=0.80,
            dq_pass_rate_min=0.95,
            ws_coverage_5m=0.70,
            ws_coverage_min=0.9,
            risk_source_prior_share=0.40,
            risk_source_prior_share_max=0.2,
            risk_source_sample_n=10,
            risk_source_min_samples=5,
        )
        self.assertTrue(result["watch_only"])
        self.assertIn("dq", result["reason_codes"])
        self.assertIn("ws", result["reason_codes"])
        self.assertIn("risk_source_prior", result["reason_codes"])


if __name__ == "__main__":
    unittest.main()
