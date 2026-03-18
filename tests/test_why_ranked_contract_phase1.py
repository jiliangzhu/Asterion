from __future__ import annotations

import unittest
from decimal import Decimal

from asterion_core.contracts import ExecutionPriorKey, ExecutionPriorSummary, RouteAction, StrategyDecision
from asterion_core.execution.trade_ticket_v1 import build_trade_ticket
from domains.weather.opportunity import build_weather_opportunity_assessment
from domains.weather.pricing.engine import build_watch_only_snapshot


class WhyRankedContractPhase1Test(unittest.TestCase):
    def test_phase1_why_ranked_fields_propagate_across_assessment_pricing_and_ticket(self) -> None:
        prior_summary = ExecutionPriorSummary(
            prior_key=ExecutionPriorKey(
                market_id="mkt_phase1",
                strategy_id=None,
                wallet_id=None,
                station_id="KSEA",
                metric="temperature_max",
                side="BUY",
                horizon_bucket="0-1",
                liquidity_bucket="deep",
                market_age_bucket="new",
                hours_to_close_bucket="24-72",
                calibration_quality_bucket="healthy",
                source_freshness_bucket="fresh",
            ),
            sample_count=24,
            submit_ack_rate=0.96,
            fill_rate=0.91,
            resolution_rate=0.93,
            partial_fill_rate=0.06,
            cancel_rate=0.04,
            adverse_fill_slippage_bps_p50=12.0,
            adverse_fill_slippage_bps_p90=26.0,
            submit_latency_ms_p50=4_000.0,
            submit_latency_ms_p90=7_000.0,
            fill_latency_ms_p50=18_000.0,
            fill_latency_ms_p90=30_000.0,
            realized_edge_retention_bps_p50=450.0,
            realized_edge_retention_bps_p90=380.0,
            avg_realized_pnl=0.07,
            avg_post_trade_error=0.01,
            prior_quality_status="ready",
            prior_lookup_mode="station_metric_fallback",
            prior_feature_scope={
                "lookup_mode": "station_metric_fallback",
                "station_id": "KSEA",
                "metric": "temperature_max",
                "matched_market_count": 3,
            },
        )

        assessment = build_weather_opportunity_assessment(
            market_id="mkt_phase1",
            token_id="tok_yes",
            outcome="YES",
            reference_price=0.39,
            model_fair_value=0.67,
            accepting_orders=True,
            enable_order_book=True,
            threshold_bps=500,
            fees_bps=30,
            agent_review_status="passed",
            live_prereq_status="shadow_aligned",
            execution_prior_summary=prior_summary,
        )
        snapshot = build_watch_only_snapshot(
            assessment=assessment,
            reference_price=0.39,
            threshold_bps=500,
            pricing_context={"run_id": "frun_phase1", "condition_id": "cond_phase1"},
        )
        decision = StrategyDecision(
            decision_id="dec_phase1",
            run_id="run_phase1",
            decision_rank=1,
            strategy_id="weather_primary",
            strategy_version="v2",
            market_id=assessment.market_id,
            token_id=assessment.token_id,
            outcome=assessment.outcome,
            side="buy",
            signal_ts_ms=1710000000000,
            reference_price=Decimal("0.39"),
            fair_value=Decimal(str(assessment.execution_adjusted_fair_value)),
            edge_bps=assessment.edge_bps_executable,
            threshold_bps=assessment.threshold_bps if hasattr(assessment, "threshold_bps") else 500,
            route_action=RouteAction.FAK,
            size=Decimal("5"),
            forecast_run_id="frun_phase1",
            watch_snapshot_id=snapshot.snapshot_id,
            pricing_context_json=dict(snapshot.pricing_context),
        )
        ticket = build_trade_ticket(decision)

        fields = (
            "latency_penalty",
            "tail_slippage_penalty",
            "edge_retention_penalty",
            "quality_confidence_multiplier",
            "prior_lookup_mode",
            "prior_feature_scope",
            "retrospective_baseline_version",
        )
        for field in fields:
            self.assertIn(field, assessment.why_ranked_json)
            self.assertEqual(
                assessment.why_ranked_json[field],
                assessment.assessment_context_json["why_ranked_json"][field],
            )
            self.assertEqual(
                assessment.why_ranked_json[field],
                snapshot.pricing_context["why_ranked_json"][field],
            )
            self.assertEqual(
                assessment.why_ranked_json[field],
                ticket.provenance_json["pricing_context"]["why_ranked_json"][field],
            )


if __name__ == "__main__":
    unittest.main()
