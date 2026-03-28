from __future__ import annotations

import importlib.util
import types
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_real_weather_chain_smoke.py"


def _load_smoke_module():
    spec = importlib.util.spec_from_file_location("real_weather_chain_smoke_profitability", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load run_real_weather_chain_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RealWeatherChainProfitabilityReportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.smoke = _load_smoke_module()

    def test_agent_pipeline_reports_resolution_and_triage_breakdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            self.smoke.apply_schema(db_path)
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    INSERT INTO agent.invocations (
                        invocation_id, agent_type, agent_version, prompt_version, subject_type, subject_id, input_hash,
                        model_provider, model_name, status, started_at, ended_at, latency_ms, error_message, input_payload_json
                    )
                    VALUES
                    ('triage_1', 'opportunity_triage', 'v1', 'p1', 'weather_market', 'mkt_1', 'hash_triage_1', 'fake', 'fake-model', 'success', '2026-03-23 10:00:00', '2026-03-23 10:00:05', 500, NULL, '{}'),
                    ('resolution_1', 'resolution', 'v1', 'p1', 'uma_proposal', 'prop_1', 'hash_resolution_1', 'fake', 'fake-model', 'success', '2026-03-23 10:01:00', '2026-03-23 10:01:05', 500, NULL, '{}')
                    """
                )
                con.execute(
                    """
                    INSERT INTO agent.outputs (
                        output_id, invocation_id, verdict, confidence, summary, findings_json, structured_output_json, human_review_required, created_at
                    )
                    VALUES
                    ('out_triage_1', 'triage_1', 'review', 0.8, 'triage output', '[]', '{"triage_status":"review","confidence_band":"medium"}', FALSE, '2026-03-23 10:00:06'),
                    ('out_resolution_1', 'resolution_1', 'review', 0.9, 'resolution output', '[]', '{"status":"pending"}', FALSE, '2026-03-23 10:01:06')
                    """
                )
                con.execute(
                    """
                    INSERT INTO agent.evaluations (evaluation_id, invocation_id, verification_method, score_json, is_verified, created_at)
                    VALUES
                    ('eval_triage_1', 'triage_1', 'replay_backtest', '{"queue_cleanliness_delta":0.2,"priority_precision_proxy":0.8,"false_escalation_rate":0.1,"operator_throughput_delta":0.2}', TRUE, '2026-03-23 10:00:07'),
                    ('eval_resolution_1', 'resolution_1', 'operator_outcome_proxy', '{"review_precision":1.0}', TRUE, '2026-03-23 10:01:07')
                    """
                )
                con.execute(
                    """
                    INSERT INTO agent.operator_review_decisions (
                        review_decision_id, invocation_id, agent_type, subject_type, subject_id, decision_status, operator_action, reason, actor, created_at, updated_at
                    ) VALUES
                    ('dec_1', 'triage_1', 'opportunity_triage', 'weather_market', 'mkt_1', 'accepted', 'take_review', 'good signal', 'operator', '2026-03-23 10:02:00', '2026-03-23 10:02:00')
                    """
                )
                con.execute(
                    """
                    INSERT INTO resolution.uma_proposals (
                        proposal_id, market_id, condition_id, proposer, proposed_outcome, proposal_bond, dispute_bond,
                        proposal_tx_hash, proposal_block_number, proposal_timestamp, status, on_chain_settled_at,
                        safe_redeem_after, human_review_required, created_at, updated_at
                    ) VALUES (
                        'prop_1', 'mkt_1', 'cond_1', '0xabc', 'YES', 100.0, NULL, '0xhash', 100,
                        '2026-03-23 09:00:00', 'settled', '2026-03-23 09:05:00', '2026-03-23 10:05:00', FALSE,
                        '2026-03-23 09:00:00', '2026-03-23 09:05:00'
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO resolution.settlement_verifications (
                        verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                        confidence, discrepancy_details, sources_checked, evidence_package, created_at
                    ) VALUES (
                        'ver_1', 'prop_1', 'mkt_1', 'YES', 'YES', TRUE, 0.99, NULL, '["weather.com"]', '{"evidence_package_id":"evidence_1"}', '2026-03-23 09:10:00'
                    )
                    """
                )
            finally:
                con.close()

            pipeline = self.smoke._build_agent_pipeline(
                db_path=db_path,
                ui_counts={
                    "ui.opportunity_triage_summary": 1,
                    "ui.proposal_resolution_summary": 1,
                },
                runtime_chain={
                    "opportunity_triage": {"status": "ok", "subject_ids": ["mkt_1"], "output_count": 1},
                    "resolution_review": {"status": "idle_no_subjects"},
                },
            )

        self.assertEqual(pipeline["triage_invocation_count"], 1)
        self.assertEqual(pipeline["triage_output_count"], 1)
        self.assertEqual(pipeline["latest_triage_output_count"], 1)
        self.assertEqual(pipeline["latest_triage_status"], "ok")
        self.assertEqual(pipeline["latest_triage_non_fallback_output_count"], 1)
        self.assertEqual(pipeline["latest_triage_medium_or_high_confidence_count"], 1)
        self.assertEqual(pipeline["latest_triage_operator_review_count"], 1)
        self.assertEqual(pipeline["triage_evaluation_count"], 1)
        self.assertEqual(pipeline["resolution_invocation_count"], 1)
        self.assertEqual(pipeline["resolution_output_count"], 1)
        self.assertEqual(pipeline["resolution_evaluation_count"], 1)
        self.assertEqual(pipeline["latest_resolution_status"], "idle_no_subjects")
        self.assertEqual(pipeline["triage_accepted_count"], 1)
        self.assertEqual(pipeline["triage_ignored_count"], 0)
        self.assertEqual(pipeline["triage_deferred_count"], 0)
        self.assertEqual(pipeline["resolution_proposal_count"], 1)
        self.assertEqual(pipeline["resolution_verification_count"], 1)
        self.assertEqual(pipeline["agent_running_status"], "ok")
        self.assertEqual(pipeline["agent_value_status"], "useful")

    def test_ui_market_opportunity_metrics_use_persisted_deployable_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lite_db = Path(tmpdir) / "ui_lite.duckdb"
            con = duckdb.connect(str(lite_db))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary (
                        market_id TEXT,
                        best_decision TEXT,
                        feedback_status TEXT,
                        execution_prior_key TEXT,
                        execution_intelligence_summary_id TEXT,
                        execution_intelligence_score DOUBLE
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_1', 'TAKE', 'sparse', 'eprior_1', 'eisum_1', 0.33),
                    ('mkt_2', 'TAKE', 'heuristic_only', NULL, 'eisum_2', 0.25),
                    ('mkt_3', 'NO_TRADE', 'heuristic_only', NULL, NULL, NULL)
                    """
                )
            finally:
                con.close()

            with mock.patch.object(self.smoke, "default_ui_lite_db_path", return_value=str(lite_db)):
                metrics = self.smoke._load_ui_market_opportunity_metrics()

        self.assertEqual(
            metrics,
            {
                "deployable_snapshot_count": 2,
                "execution_intelligence_covered_snapshot_count": 2,
                "active_market_prior_hit_count": 1,
            },
        )

    def test_prior_lookup_breakdown_uses_execution_prior_key_when_mode_missing(self) -> None:
        snapshots = [
            types.SimpleNamespace(pricing_context={"execution_prior_key": "eprior_1"}),
            types.SimpleNamespace(pricing_context={"prior_lookup_mode": "heuristic_fallback"}),
            types.SimpleNamespace(pricing_context={}),
        ]
        breakdown = self.smoke._prior_lookup_breakdown(snapshots)
        self.assertEqual(
            breakdown,
            {
                "empirical_primary": 1,
                "heuristic_fallback": 1,
                "missing": 1,
            },
        )

    def test_execution_intelligence_coverage_counts_summary_id(self) -> None:
        snapshots = [
            types.SimpleNamespace(
                decision="TAKE",
                side="BUY",
                pricing_context={"execution_intelligence_summary_id": "eisum_1"},
            ),
            types.SimpleNamespace(
                decision="TAKE",
                side="SELL",
                pricing_context={},
            ),
        ]
        self.assertEqual(self.smoke._execution_intelligence_covered_snapshot_count(snapshots), 1)

    def test_roi_status_distinguishes_waiting_for_resolution_from_closed_feedback_loop(self) -> None:
        roi_waiting = self.smoke._build_roi_status(
            source_split_brain=False,
            db_path=ROOT / "data" / "asterion.duckdb",
            ui_counts={
                "ui.market_opportunity_summary": 1,
                "ui.action_queue_summary": 1,
                "ui.market_microstructure_summary": 0,
            },
            signal_pipeline={
                "calibration_profile_count": 1,
                "non_no_trade_snapshot_count": 1,
            },
            execution_pipeline={
                "strategy_runs": 1,
                "trade_tickets": 1,
                "allocation_decisions": 1,
                "execution_intelligence_summaries": 0,
                "deployable_snapshot_count": 1,
                "execution_intelligence_covered_snapshot_count": 0,
                "paper_orders": 1,
                "fills": 1,
                "predicted_vs_realized_rows": 0,
            },
            agent_pipeline={
                "latest_triage_status": "idle_no_subjects",
            },
            settlement_feedback_pipeline={
                "pending_resolution_ticket_count": 2,
                "resolved_ticket_count": 0,
                "latest_feedback_writeback_status": "waiting_for_resolution",
            },
        )
        self.assertEqual(roi_waiting["settlement_feedback_closure_status"], "waiting_for_resolution")

        roi_closed = self.smoke._build_roi_status(
            source_split_brain=False,
            db_path=ROOT / "data" / "asterion.duckdb",
            ui_counts={
                "ui.market_opportunity_summary": 1,
                "ui.action_queue_summary": 1,
                "ui.market_microstructure_summary": 1,
            },
            signal_pipeline={
                "calibration_profile_count": 1,
                "non_no_trade_snapshot_count": 1,
            },
            execution_pipeline={
                "strategy_runs": 1,
                "trade_tickets": 1,
                "allocation_decisions": 1,
                "execution_intelligence_summaries": 1,
                "deployable_snapshot_count": 1,
                "execution_intelligence_covered_snapshot_count": 1,
                "paper_orders": 1,
                "fills": 1,
                "predicted_vs_realized_rows": 1,
            },
            agent_pipeline={
                "latest_triage_status": "ok",
                "latest_triage_non_fallback_output_count": 1,
            },
            settlement_feedback_pipeline={
                "pending_resolution_ticket_count": 0,
                "resolved_ticket_count": 1,
                "latest_feedback_writeback_status": "ok",
            },
        )
        self.assertEqual(roi_closed["settlement_feedback_closure_status"], "closed")


if __name__ == "__main__":
    unittest.main()
