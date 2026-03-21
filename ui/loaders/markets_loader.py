from __future__ import annotations

from typing import Any

from ui.loaders.execution_loader import load_execution_console_data
from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_market_chain_analysis_data() -> dict[str, Any]:
    from ui import data_access as compat

    market_payload = compat.load_market_watch_data()
    opportunity_payload = compat.load_market_opportunity_data()
    predicted_vs_realized_payload = compat.load_predicted_vs_realized_data()
    execution_payload = load_execution_console_data()
    report = market_payload["weather_smoke_report"] or {}
    discovery = report.get("market_discovery") or {}
    selected_markets = discovery.get("selected_markets") or []
    specs_by_market = {item.get("market_id"): item for item in (report.get("rule_parse") or {}).get("selected_specs") or []}
    forecasts_by_market = {item.get("market_id"): item for item in (report.get("forecast_service") or {}).get("markets") or []}
    pricing_by_market = {item.get("market_id"): item for item in (report.get("pricing_engine") or {}).get("markets") or []}
    signals_by_market = {item.get("market_id"): item for item in (report.get("opportunity_discovery") or {}).get("markets") or []}
    detail_rows: list[dict[str, Any]] = []
    for market in selected_markets:
        market_id = market.get("market_id")
        detail_rows.append(
            {
                **market,
                "spec": specs_by_market.get(market_id) or {},
                "forecast": forecasts_by_market.get(market_id) or {},
                "pricing": pricing_by_market.get(market_id) or {},
                "signals": signals_by_market.get(market_id) or {},
            }
        )
    if not detail_rows:
        runtime_rows = compat._read_weather_market_rows_from_runtime(compat._resolve_real_weather_chain_db_path())
        if not runtime_rows.empty:
            detail_rows = [
                {
                    **row.to_dict(),
                    "spec": {
                        "location_name": row.get("location_name"),
                        "station_id": row.get("station_id"),
                        "authoritative_source": row.get("authoritative_source"),
                        "metric": row.get("metric"),
                        "bucket_min_value": row.get("bucket_min_value"),
                        "bucket_max_value": row.get("bucket_max_value"),
                        "observation_window_local": row.get("observation_window_local"),
                    },
                    "forecast": {},
                    "pricing": {},
                    "signals": {},
                    "forecast_status": "not_started",
                    "forecast_summary": "forecast stage has not completed yet",
                }
                for _, row in runtime_rows.iterrows()
            ]
    details_by_market = {str(item.get("market_id")): item for item in detail_rows if item.get("market_id") is not None}
    opportunities = opportunity_payload["frame"]
    predicted_vs_realized = predicted_vs_realized_payload["frame"]
    watch_only_vs_executed = execution_payload["watch_only_vs_executed"]
    market_research = execution_payload["market_research"]
    cohort_history = execution_payload["cohort_history"]
    action_queue = compat._sort_desc(compat.load_ui_lite_snapshot()["tables"]["action_queue_summary"], "queue_priority", "ranking_score", "updated_at")
    validation_by_market = compat.load_market_validation_overlays()

    execution_summary_by_market: dict[str, dict[str, Any]] = {}
    if not predicted_vs_realized.empty and "market_id" in predicted_vs_realized.columns:
        for market_id, frame in predicted_vs_realized.groupby("market_id", dropna=False):
            sorted_frame = compat._sort_desc(frame, "latest_fill_at", "latest_resolution_at")
            latest = sorted_frame.iloc[0].to_dict() if not sorted_frame.empty else {}
            execution_summary_by_market[str(market_id)] = {
                "has_executed_evidence": not sorted_frame.empty,
                "latest_ticket_id": latest.get("ticket_id"),
                "latest_order_id": latest.get("order_id"),
                "predicted_edge_bps": latest.get("predicted_edge_bps"),
                "expected_fill_price": latest.get("expected_fill_price"),
                "realized_fill_price": latest.get("realized_fill_price"),
                "realized_pnl": latest.get("realized_pnl"),
                "resolution_value": latest.get("resolution_value"),
                "post_trade_error": latest.get("post_trade_error"),
                "source_disagreement": latest.get("source_disagreement"),
                "evaluation_status": latest.get("evaluation_status"),
                "execution_lifecycle_stage": latest.get("execution_lifecycle_stage"),
                "fill_ratio": latest.get("fill_ratio"),
                "adverse_fill_slippage_bps": latest.get("adverse_fill_slippage_bps"),
                "resolution_lag_hours": latest.get("resolution_lag_hours"),
                "miss_reason_bucket": latest.get("miss_reason_bucket"),
                "distortion_reason_codes_json": latest.get("distortion_reason_codes_json"),
                "source_badge": latest.get("source_badge"),
                "source_truth_status": latest.get("source_truth_status"),
                "is_degraded_source": latest.get("is_degraded_source"),
                "latest_fill_at": latest.get("latest_fill_at"),
                "latest_resolution_at": latest.get("latest_resolution_at"),
            }
    watch_only_by_market = _frame_to_mapping(watch_only_vs_executed, "market_id")
    research_by_market = _frame_to_mapping(market_research, "market_id")
    queue_by_market = _frame_to_mapping(action_queue, "market_id")
    cohort_history_by_market: dict[str, list[dict[str, Any]]] = {}
    if not cohort_history.empty and "market_id" in cohort_history.columns:
        for market_id, frame in cohort_history.groupby("market_id", dropna=False):
            cohort_history_by_market[str(market_id)] = [
                row.to_dict()
                for _, row in frame.sort_values(
                    by=["updated_at", "ranking_decile", "avg_ranking_score"],
                    ascending=[False, True, False],
                    na_position="last",
                )
                .head(5)
                .iterrows()
            ]

    rows: list[dict[str, Any]] = []
    if not opportunities.empty:
        for _, row in opportunities.iterrows():
            market_id = str(row.get("market_id"))
            details = details_by_market.get(market_id, {})
            payload = row.to_dict()
            queue_payload = queue_by_market.get(market_id) or {}
            payload.update(
                {
                    "spec": details.get("spec") or {},
                    "forecast": details.get("forecast") or {},
                    "pricing": details.get("pricing") or {},
                    "signals": details.get("signals") or {},
                    "forecast_status": details.get("forecast_status"),
                    "forecast_summary": details.get("forecast_summary"),
                    "rule2spec_status": (validation_by_market.get(market_id) or {}).get("rule2spec_status") or details.get("rule2spec_status"),
                    "rule2spec_verdict": (validation_by_market.get(market_id) or {}).get("rule2spec_verdict") or details.get("rule2spec_verdict"),
                    "rule2spec_summary": (validation_by_market.get(market_id) or {}).get("rule2spec_summary") or details.get("rule2spec_summary"),
                    "data_qa_status": (validation_by_market.get(market_id) or {}).get("data_qa_status") or details.get("data_qa_status"),
                    "data_qa_verdict": (validation_by_market.get(market_id) or {}).get("data_qa_verdict") or details.get("data_qa_verdict"),
                    "data_qa_summary": (validation_by_market.get(market_id) or {}).get("data_qa_summary") or details.get("data_qa_summary"),
                    "resolution_status": details.get("resolution_status"),
                    "resolution_verdict": details.get("resolution_verdict"),
                    "resolution_summary": details.get("resolution_summary"),
                    "executed_evidence": execution_summary_by_market.get(market_id) or {"has_executed_evidence": False},
                    "watch_only_vs_executed": watch_only_by_market.get(market_id) or {},
                    "market_research": research_by_market.get(market_id) or {},
                    "operator_bucket": queue_payload.get("operator_bucket"),
                    "queue_reason_codes": compat._json_list(queue_payload.get("queue_reason_codes_json")),
                    "queue_priority": queue_payload.get("queue_priority"),
                    "calibration_gate_status": queue_payload.get("calibration_gate_status") or payload.get("calibration_gate_status") or "clear",
                    "calibration_gate_reason_codes": compat._json_list(queue_payload.get("calibration_gate_reason_codes_json"))
                    or payload.get("calibration_gate_reason_codes")
                    or [],
                    "calibration_impacted_market": queue_payload.get("calibration_impacted_market")
                    if queue_payload.get("calibration_impacted_market") is not None
                    else bool(payload.get("calibration_impacted_market")),
                    "capital_policy_id": queue_payload.get("capital_policy_id") or payload.get("capital_policy_id"),
                    "capital_policy_version": queue_payload.get("capital_policy_version") or payload.get("capital_policy_version"),
                    "capital_scaling_reason_codes": compat._json_list(queue_payload.get("capital_scaling_reason_codes_json"))
                    or payload.get("capital_scaling_reason_codes")
                    or [],
                    "cohort_history": cohort_history_by_market.get(market_id) or [],
                }
            )
            rows.append(payload)
    else:
        fallback_badge = compat.build_opportunity_row_source_badge(
            source_origin=opportunity_payload["source"],
            source_freshness_status="missing",
            derived=False,
        )
        rows = [
            {
                **item,
                "source_badge": item.get("source_badge") or fallback_badge.source_badge,
                "source_truth_status": item.get("source_truth_status") or fallback_badge.source_truth_status,
                "is_degraded_source": item.get("is_degraded_source")
                if item.get("is_degraded_source") is not None
                else fallback_badge.is_degraded_source,
                "primary_score_label": item.get("primary_score_label") or "ranking_score",
                "executed_evidence": execution_summary_by_market.get(str(item.get("market_id"))) or {"has_executed_evidence": False},
                "watch_only_vs_executed": watch_only_by_market.get(str(item.get("market_id"))) or {},
                "market_research": research_by_market.get(str(item.get("market_id"))) or {},
                "operator_bucket": (queue_by_market.get(str(item.get("market_id"))) or {}).get("operator_bucket"),
                "queue_reason_codes": compat._json_list((queue_by_market.get(str(item.get("market_id"))) or {}).get("queue_reason_codes_json")),
                "calibration_gate_status": (queue_by_market.get(str(item.get("market_id"))) or {}).get("calibration_gate_status")
                or item.get("calibration_gate_status")
                or "clear",
                "calibration_gate_reason_codes": compat._json_list((queue_by_market.get(str(item.get("market_id"))) or {}).get("calibration_gate_reason_codes_json"))
                or item.get("calibration_gate_reason_codes")
                or [],
                "calibration_impacted_market": (queue_by_market.get(str(item.get("market_id"))) or {}).get("calibration_impacted_market")
                if (queue_by_market.get(str(item.get("market_id"))) or {}).get("calibration_impacted_market") is not None
                else bool(item.get("calibration_impacted_market")),
                "capital_policy_id": (queue_by_market.get(str(item.get("market_id"))) or {}).get("capital_policy_id")
                or item.get("capital_policy_id"),
                "capital_policy_version": (queue_by_market.get(str(item.get("market_id"))) or {}).get("capital_policy_version")
                or item.get("capital_policy_version"),
                "capital_scaling_reason_codes": compat._json_list((queue_by_market.get(str(item.get("market_id"))) or {}).get("capital_scaling_reason_codes_json"))
                or item.get("capital_scaling_reason_codes")
                or [],
                "rule2spec_status": (validation_by_market.get(str(item.get("market_id"))) or {}).get("rule2spec_status") or item.get("rule2spec_status"),
                "rule2spec_verdict": (validation_by_market.get(str(item.get("market_id"))) or {}).get("rule2spec_verdict") or item.get("rule2spec_verdict"),
                "rule2spec_summary": (validation_by_market.get(str(item.get("market_id"))) or {}).get("rule2spec_summary") or item.get("rule2spec_summary"),
                "data_qa_status": (validation_by_market.get(str(item.get("market_id"))) or {}).get("data_qa_status") or item.get("data_qa_status"),
                "data_qa_verdict": (validation_by_market.get(str(item.get("market_id"))) or {}).get("data_qa_verdict") or item.get("data_qa_verdict"),
                "data_qa_summary": (validation_by_market.get(str(item.get("market_id"))) or {}).get("data_qa_summary") or item.get("data_qa_summary"),
                "cohort_history": cohort_history_by_market.get(str(item.get("market_id"))) or [],
            }
            for item in detail_rows
        ]
    return {
        "market_watch": market_payload["market_watch"],
        "market_opportunities": opportunities,
        "market_opportunity_source": opportunity_payload["source"],
        "predicted_vs_realized": predicted_vs_realized,
        "watch_only_vs_executed": watch_only_vs_executed,
        "market_research": market_research,
        "cohort_history": cohort_history,
        "weather_smoke_report": report,
        "market_rows": rows,
    }


def load_markets_surface_contract() -> SurfaceLoaderContract:
    payload = load_market_chain_analysis_data()
    contract = SurfaceLoaderContract(
        surface_id="markets",
        primary_dataframe_name="market_opportunities",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="markets",
            primary_table="ui.market_opportunity_summary",
            source=payload.get("market_opportunity_source") or "missing",
            supports_source_badges=True,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract


def _frame_to_mapping(frame, key_column: str) -> dict[str, dict[str, Any]]:
    if frame.empty or key_column not in frame.columns:
        return {}
    return {
        str(row[key_column]): row.to_dict()
        for _, row in frame.iterrows()
        if row.get(key_column) is not None
    }
