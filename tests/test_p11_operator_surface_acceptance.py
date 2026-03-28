from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from ui.pages import agents, home, markets


class P11OperatorSurfaceAcceptanceTest(unittest.TestCase):
    def test_home_only_reads_triage_overlay(self) -> None:
        self.assertFalse(hasattr(home, "write_opportunity_triage_operator_review_decision"))

    def test_markets_triage_action_writes_operator_decision(self) -> None:
        row = {
            "market_id": "mkt_1",
            "triage_latest_agent_invocation_id": "inv_1",
            "triage_recommended_operator_action": "manual_review",
        }
        with patch("ui.pages.markets.write_opportunity_triage_operator_review_decision") as writer:
            markets._submit_triage_action(row, decision_status="accepted", actor="operator", reason="accept")
        writer.assert_called_once_with(
            market_id="mkt_1",
            invocation_id="inv_1",
            decision_status="accepted",
            operator_action="manual_review",
            actor="operator",
            reason="accept",
        )

    def test_agents_triage_action_writes_operator_decision(self) -> None:
        row = {
            "market_id": "mkt_1",
            "latest_agent_invocation_id": "inv_1",
            "recommended_operator_action": "manual_review",
        }
        with patch("ui.pages.agents.write_opportunity_triage_operator_review_decision") as writer:
            agents._submit_triage_action(row, decision_status="ignored", actor="operator", reason="ignore")
        writer.assert_called_once_with(
            market_id="mkt_1",
            invocation_id="inv_1",
            decision_status="ignored",
            operator_action="manual_review",
            actor="operator",
            reason="ignore",
        )

    def test_same_market_triage_state_is_visible_on_markets_and_agents(self) -> None:
        triage = pd.DataFrame(
            [
                {
                    "market_id": "mkt_1",
                    "location_name": "Seattle",
                    "question": "Will Seattle be cold?",
                    "priority_band": "high",
                    "recommended_operator_action": "manual_review",
                    "effective_triage_status": "accepted",
                    "advisory_gate_status": "experimental",
                }
            ]
        )
        self.assertEqual(triage.iloc[0]["effective_triage_status"], "accepted")
        self.assertEqual(triage.iloc[0]["recommended_operator_action"], "manual_review")
        self.assertEqual(triage.iloc[0]["advisory_gate_status"], "experimental")


if __name__ == "__main__":
    unittest.main()
