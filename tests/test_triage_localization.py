from __future__ import annotations

import unittest

from ui.triage_localization import localize_reason_codes, localize_triage_frame, localize_triage_value


class TriageLocalizationTest(unittest.TestCase):
    def test_localize_triage_value_preserves_contract_but_translates_operator_labels(self) -> None:
        self.assertEqual(localize_triage_value("priority_band", "high"), "高优先级")
        self.assertEqual(localize_triage_value("recommended_operator_action", "manual_review"), "人工复核")
        self.assertEqual(localize_triage_value("effective_triage_status", "accepted"), "已接受")
        self.assertEqual(localize_triage_value("triage_latest_run_status", "idle_no_subjects"), "当前无对象")
        self.assertEqual(localize_triage_value("latest_evaluation_verified", True), "已验证")

    def test_localize_reason_codes_translates_common_triage_codes(self) -> None:
        self.assertEqual(
            localize_reason_codes(
                ["provider_forbidden", "execution_intelligence_weak"],
                empty_label="triage:none",
            ),
            ["外部分诊服务拒绝访问", "执行情报偏弱"],
        )
        self.assertEqual(
            localize_reason_codes([], empty_label="triage_gate:enabled"),
            ["分诊建议已启用"],
        )

    def test_localize_triage_frame_only_touches_known_triage_columns(self) -> None:
        import pandas as pd

        frame = pd.DataFrame(
            [
                {
                    "market_id": "mkt_1",
                    "priority_band": "medium",
                    "recommended_operator_action": "manual_review",
                    "effective_triage_status": "review",
                    "latest_agent_status": "success",
                    "unchanged": "raw",
                }
            ]
        )
        localized = localize_triage_frame(frame)
        self.assertEqual(localized.iloc[0]["priority_band"], "中优先级")
        self.assertEqual(localized.iloc[0]["recommended_operator_action"], "人工复核")
        self.assertEqual(localized.iloc[0]["effective_triage_status"], "待人工复核")
        self.assertEqual(localized.iloc[0]["latest_agent_status"], "成功")
        self.assertEqual(localized.iloc[0]["unchanged"], "raw")


if __name__ == "__main__":
    unittest.main()
