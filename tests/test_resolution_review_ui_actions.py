from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from ui.pages import agents


class _DummyContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class ResolutionReviewUiActionsTest(unittest.TestCase):
    def test_submit_review_action_writes_resolution_operator_decision(self) -> None:
        row = {
            "proposal_id": "prop_1",
            "suggestion_id": "suggest_1",
            "latest_agent_invocation_id": "inv_1",
            "latest_recommended_operator_action": "manual_review",
        }
        with patch("ui.pages.agents.write_resolution_operator_review_decision") as write_decision:
            agents._submit_review_action(
                row,
                decision_status="accepted",
                actor="operator",
                reason="validated",
            )
        write_decision.assert_called_once_with(
            proposal_id="prop_1",
            invocation_id="inv_1",
            suggestion_id="suggest_1",
            decision_status="accepted",
            operator_action="manual_review",
            actor="operator",
            reason="validated",
        )

    def test_show_renders_resolution_queue_and_triggers_accept_action(self) -> None:
        review = pd.DataFrame(
            [
                {
                    "proposal_id": "prop_1",
                    "market_id": "mkt_1",
                    "proposal_status": "settled",
                    "expected_outcome": "YES",
                    "proposed_outcome": "YES",
                    "verification_confidence": 0.95,
                    "suggestion_id": "suggest_1",
                    "redeem_decision": "ready_for_redeem",
                    "redeem_reason": "baseline ready",
                    "latest_agent_invocation_id": "inv_1",
                    "latest_agent_verdict": "review",
                    "latest_agent_summary": "manual review suggested",
                    "latest_recommended_operator_action": "manual_review",
                    "latest_settlement_risk_score": 0.7,
                    "latest_operator_review_status": None,
                    "latest_operator_action": None,
                    "effective_redeem_status": "pending_operator_review",
                }
            ]
        )
        runtime_status = {"provider": "openai_compatible", "model": "glm-5", "configured": True, "agents": []}
        button_results = iter([True, False, False])
        with patch("ui.pages.agents.load_agent_runtime_status", return_value=runtime_status), \
            patch("ui.pages.agents.load_resolution_review_data", return_value={"source": "ui_lite", "frame": review}), \
            patch("ui.pages.agents.write_resolution_operator_review_decision") as write_decision, \
            patch.object(agents.st, "markdown"), \
            patch.object(agents.st, "caption"), \
            patch.object(agents.st, "metric"), \
            patch.object(agents.st, "info"), \
            patch.object(agents.st, "success"), \
            patch.object(agents.st, "error"), \
            patch.object(agents.st, "dataframe", new=MagicMock()), \
            patch.object(agents.st, "columns", side_effect=lambda spec: [_DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))]), \
            patch.object(agents.st, "expander", return_value=_DummyContext()), \
            patch.object(agents.st, "text_input", side_effect=["operator", "accepted for review"]), \
            patch.object(agents.st, "button", side_effect=lambda *args, **kwargs: next(button_results)), \
            patch.object(agents.st, "rerun") as rerun:
            agents.show()
        write_decision.assert_called_once_with(
            proposal_id="prop_1",
            invocation_id="inv_1",
            suggestion_id="suggest_1",
            decision_status="accepted",
            operator_action="manual_review",
            actor="operator",
            reason="accepted for review",
        )
        rerun.assert_called_once()


if __name__ == "__main__":
    unittest.main()
