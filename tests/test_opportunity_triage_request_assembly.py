from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from agents.weather.opportunity_triage_agent import load_opportunity_triage_agent_requests


class OpportunityTriageRequestAssemblyTest(unittest.TestCase):
    def test_request_assembly_uses_persisted_ui_surfaces(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_replica.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        question TEXT,
                        location_name TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
                        calibration_gate_status TEXT,
                        capital_policy_id TEXT,
                        surface_delivery_status TEXT,
                        surface_fallback_origin TEXT,
                        source_freshness_status TEXT,
                        source_truth_status TEXT,
                        is_degraded_source BOOLEAN,
                        why_ranked_json TEXT,
                        pricing_context_json TEXT,
                        source_badge TEXT,
                        signal_created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.action_queue_summary(
                        market_id TEXT,
                        operator_bucket TEXT,
                        queue_reason_codes_json TEXT,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
                        calibration_gate_status TEXT,
                        capital_policy_id TEXT,
                        surface_delivery_status TEXT,
                        surface_fallback_origin TEXT,
                        queue_priority BIGINT,
                        updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.market_microstructure_summary(
                        summary_id TEXT,
                        market_id TEXT,
                        execution_intelligence_score DOUBLE,
                        top_of_book_stability DOUBLE,
                        spread_regime TEXT,
                        expected_capture_regime TEXT,
                        expected_slippage_regime TEXT,
                        reason_codes_json TEXT,
                        materialized_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES (
                        'mkt_1',
                        'Will Seattle be cold?',
                        'Seattle',
                        'BUY',
                        0.82,
                        4.0,
                        'resized',
                        'review_required',
                        'cap_policy_1',
                        'degraded_source',
                        'runtime_db',
                        'fresh',
                        'fallback',
                        TRUE,
                        ?,
                        ?,
                        'fallback',
                        '2026-03-22 10:00:00'
                    )
                    """,
                    [
                        json.dumps({"mode": "ranking_v2", "ranking_score": 0.82, "microstructure_reason_codes": ["size_shock"]}),
                        json.dumps({"ranking_score": 0.82, "expected_dollar_pnl": 2.1}),
                    ],
                )
                con.execute(
                    """
                    INSERT INTO ui.action_queue_summary VALUES (
                        'mkt_1',
                        'review_required',
                        '["allocation:resized","delivery:degraded_source"]',
                        4.0,
                        'resized',
                        'review_required',
                        'cap_policy_1',
                        'degraded_source',
                        'runtime_db',
                        2,
                        '2026-03-22 10:01:00'
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_microstructure_summary VALUES (
                        'micro_1',
                        'mkt_1',
                        0.31,
                        0.22,
                        'wide',
                        'low',
                        'high',
                        '["size_shock","unstable_book"]',
                        '2026-03-22 09:59:00'
                    )
                    """
                )
            finally:
                con.close()

            requests = load_opportunity_triage_agent_requests(db_path)

        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertEqual(request.market_id, "mkt_1")
        self.assertEqual(request.question, "Will Seattle be cold?")
        self.assertEqual(request.operator_bucket, "review_required")
        self.assertEqual(request.surface_delivery_status, "degraded_source")
        self.assertEqual(request.surface_fallback_origin, "runtime_db")
        self.assertTrue(request.is_degraded_source)
        self.assertEqual(request.queue_reason_codes, ["allocation:resized", "delivery:degraded_source"])
        self.assertEqual(request.microstructure_reason_codes, ["size_shock", "unstable_book"])
        self.assertEqual(request.source_provenance["primary_source"], "ui_replica")
        self.assertEqual(request.source_provenance["subject_type"], "weather_market")

    def test_request_assembly_skips_rows_without_best_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ui_replica.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA ui")
                con.execute(
                    """
                    CREATE TABLE ui.market_opportunity_summary(
                        market_id TEXT,
                        question TEXT,
                        location_name TEXT,
                        best_side TEXT,
                        ranking_score DOUBLE,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
                        calibration_gate_status TEXT,
                        capital_policy_id TEXT,
                        surface_delivery_status TEXT,
                        surface_fallback_origin TEXT,
                        source_freshness_status TEXT,
                        source_truth_status TEXT,
                        is_degraded_source BOOLEAN,
                        why_ranked_json TEXT,
                        pricing_context_json TEXT,
                        source_badge TEXT,
                        signal_created_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.action_queue_summary(
                        market_id TEXT,
                        operator_bucket TEXT,
                        queue_reason_codes_json TEXT,
                        recommended_size DOUBLE,
                        allocation_status TEXT,
                        calibration_gate_status TEXT,
                        capital_policy_id TEXT,
                        surface_delivery_status TEXT,
                        surface_fallback_origin TEXT,
                        queue_priority BIGINT,
                        updated_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    CREATE TABLE ui.market_microstructure_summary(
                        summary_id TEXT,
                        market_id TEXT,
                        execution_intelligence_score DOUBLE,
                        top_of_book_stability DOUBLE,
                        spread_regime TEXT,
                        expected_capture_regime TEXT,
                        expected_slippage_regime TEXT,
                        reason_codes_json TEXT,
                        materialized_at TIMESTAMP
                    )
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.market_opportunity_summary VALUES
                    ('mkt_bad', 'Will Seattle be cold?', 'Seattle', NULL, 0.1, 0.0, NULL, 'review_required', NULL, 'ok', NULL, 'fresh', 'ok', FALSE, '{}', '{}', 'ui_lite', '2026-03-22 10:00:00'),
                    ('mkt_good', 'Will Seattle be warmer?', 'Seattle', 'BUY', 0.8, 4.0, 'approved', 'clear', 'cap_policy_1', 'ok', NULL, 'fresh', 'ok', FALSE, '{}', '{}', 'ui_lite', '2026-03-22 10:01:00')
                    """
                )
                con.execute(
                    """
                    INSERT INTO ui.action_queue_summary VALUES
                    ('mkt_bad', 'review_required', '[]', 0.0, NULL, 'review_required', NULL, 'ok', NULL, 2, '2026-03-22 10:02:00'),
                    ('mkt_good', 'ready_now', '[]', 4.0, 'approved', 'clear', 'cap_policy_1', 'ok', NULL, 1, '2026-03-22 10:03:00')
                    """
                )
            finally:
                con.close()

            requests = load_opportunity_triage_agent_requests(db_path)

        self.assertEqual([request.market_id for request in requests], ["mkt_good"])


if __name__ == "__main__":
    unittest.main()
