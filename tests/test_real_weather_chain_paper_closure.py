from __future__ import annotations

import importlib.util
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_real_weather_chain_smoke.py"


def _load_smoke_module():
    spec = importlib.util.spec_from_file_location("real_weather_chain_smoke_closure", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load run_real_weather_chain_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RealWeatherChainPaperClosureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.smoke = _load_smoke_module()

    def test_path_closed_requires_predicted_vs_realized_rows(self) -> None:
        signal_pipeline = {
            "calibration_profile_count": 2,
            "non_no_trade_snapshot_count": 3,
        }
        execution_pipeline = {
            "strategy_runs": 1,
            "trade_tickets": 1,
            "allocation_decisions": 1,
            "execution_intelligence_summaries": 0,
            "deployable_snapshot_count": 1,
            "execution_intelligence_covered_snapshot_count": 0,
            "paper_orders": 1,
            "fills": 1,
            "predicted_vs_realized_rows": 0,
        }
        agent_pipeline = {
            "triage_output_count": 1,
            "latest_triage_output_count": 0,
            "latest_triage_non_fallback_output_count": 0,
            "latest_triage_medium_or_high_confidence_count": 0,
            "latest_triage_operator_review_count": 0,
            "triage_row_count": 1,
            "triage_status": "ok",
            "latest_triage_status": "ok",
        }
        roi_status = self.smoke._build_roi_status(
            source_split_brain=False,
            db_path=Path("data/asterion.duckdb"),
            ui_counts={
                "ui.market_opportunity_summary": 1,
                "ui.action_queue_summary": 1,
                "ui.market_microstructure_summary": 0,
            },
            signal_pipeline=signal_pipeline,
            execution_pipeline=execution_pipeline,
            agent_pipeline=agent_pipeline,
        )
        self.assertFalse(roi_status["path_closed"])
        self.assertEqual(roi_status["execution_closure_status"], "partial")
        self.assertEqual(roi_status["intelligence_closure_status"], "degraded")
        self.assertFalse(roi_status["has_empirical_feedback"])
        self.assertFalse(roi_status["agents_have_useful_output"])

        execution_pipeline["predicted_vs_realized_rows"] = 2
        execution_pipeline["execution_intelligence_summaries"] = 1
        execution_pipeline["execution_intelligence_covered_snapshot_count"] = 1
        agent_pipeline["latest_triage_output_count"] = 1
        agent_pipeline["latest_triage_non_fallback_output_count"] = 1
        roi_status = self.smoke._build_roi_status(
            source_split_brain=False,
            db_path=Path("data/asterion.duckdb"),
            ui_counts={
                "ui.market_opportunity_summary": 1,
                "ui.action_queue_summary": 1,
                "ui.market_microstructure_summary": 1,
            },
            signal_pipeline=signal_pipeline,
            execution_pipeline=execution_pipeline,
            agent_pipeline=agent_pipeline,
        )
        self.assertTrue(roi_status["path_closed"])
        self.assertEqual(roi_status["execution_closure_status"], "closed")
        self.assertEqual(roi_status["intelligence_closure_status"], "closed")
        self.assertTrue(roi_status["has_empirical_feedback"])
        self.assertTrue(roi_status["agents_have_useful_output"])

    def test_load_latest_snapshot_ids_uses_latest_per_market_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            self.smoke.apply_schema(db_path)
            con = duckdb.connect(str(db_path))
            try:
                created_at = datetime.now(UTC).replace(tzinfo=None)
                rows = [
                    (
                        "snap_old_buy",
                        "fv_1",
                        "run_1",
                        "mkt_1",
                        "cond_1",
                        "tok_yes",
                        "YES",
                        0.40,
                        0.60,
                        2000,
                        300,
                        "BUY",
                        "BUY",
                        "older deployable",
                        "{}",
                        created_at - timedelta(minutes=2),
                    ),
                    (
                        "snap_new_blocked",
                        "fv_2",
                        "run_2",
                        "mkt_1",
                        "cond_1",
                        "tok_yes",
                        "YES",
                        0.40,
                        0.55,
                        1500,
                        300,
                        "NO_TRADE",
                        "HOLD",
                        "latest not deployable",
                        "{}",
                        created_at - timedelta(minutes=1),
                    ),
                    (
                        "snap_other_buy",
                        "fv_3",
                        "run_3",
                        "mkt_2",
                        "cond_2",
                        "tok_no",
                        "NO",
                        0.30,
                        0.62,
                        3200,
                        300,
                        "BUY",
                        "BUY",
                        "latest deployable",
                        "{}",
                        created_at,
                    ),
                ]
                con.executemany(
                    """
                    INSERT INTO weather.weather_watch_only_snapshots (
                        snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome,
                        reference_price, fair_value, edge_bps, threshold_bps, decision, side, rationale,
                        pricing_context_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
            finally:
                con.close()

            snapshot_ids = self.smoke._load_latest_snapshot_ids(db_path, limit=10)
            self.assertEqual(snapshot_ids, ["snap_other_buy"])


if __name__ == "__main__":
    unittest.main()
