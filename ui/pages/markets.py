from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from ui.data_access import load_market_chain_analysis_data, load_operator_surface_status


def _format_value(value: object) -> str:
    if value is None or value == "" or pd.isna(value):
        return "N/A"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def _format_bucket_range(min_value: object, max_value: object) -> str:
    if min_value is None and max_value is None:
        return "N/A"
    return f"{_format_value(min_value)} - {_format_value(max_value)}"


def _detail_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame([{"字段": row["字段"], "值": _format_value(row["值"])} for row in rows])


def _json_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if value in {None, ""}:
        return {}
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _json_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if value in {None, ""}:
        return []
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    return payload if isinstance(payload, list) else []


def _preview_summary(selected_market: dict[str, object]) -> dict[str, object]:
    budget_impact = _json_dict(selected_market.get("budget_impact"))
    preview_budget = _json_dict(budget_impact.get("preview"))
    return {
        "requested_size": selected_market.get("requested_size") if selected_market.get("requested_size") not in {None, ""} else preview_budget.get("requested_size"),
        "requested_notional": selected_market.get("requested_notional") if selected_market.get("requested_notional") not in {None, ""} else preview_budget.get("requested_notional"),
        "preview_binding_limit_scope": selected_market.get("preview_binding_limit_scope") or preview_budget.get("preview_binding_limit_scope"),
        "preview_binding_limit_key": selected_market.get("preview_binding_limit_key") or preview_budget.get("preview_binding_limit_key"),
        "rerank_reason_codes": selected_market.get("rerank_reason_codes") or _json_list(selected_market.get("rerank_reason_codes_json")) or _json_list(budget_impact.get("rerank_reason_codes")),
    }


def _opportunity_thesis(selected_market: dict[str, object]) -> str:
    actionability = selected_market.get("actionability_status")
    if actionability == "actionable":
        return "当前市场具备正向 edge、可执行性正常，且没有显性 agent/live blocker，应优先进入 operator review。"
    if actionability == "review_required":
        return "当前市场存在机会，但 agent 结论仍需人工确认，或当前缺少足够 agent 证据。"
    if actionability == "blocked":
        return "当前市场有潜在 edge，但执行或 live-prereq 边界存在 blocker，不应直接推进。"
    return "当前市场暂时不构成交易机会，应保持观察而不是推进执行。"


def _agent_review_frame(selected_market: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "check": "rule2spec_validation",
                "status": selected_market.get("rule2spec_status") or selected_market.get("agent_review_status"),
                "verdict": selected_market.get("rule2spec_verdict"),
                "summary": selected_market.get("rule2spec_summary"),
            },
            {
                "check": "replay_validation",
                "status": selected_market.get("data_qa_status"),
                "verdict": selected_market.get("data_qa_verdict"),
                "summary": selected_market.get("data_qa_summary"),
            },
            {
                "check": "resolution_review",
                "status": selected_market.get("resolution_status"),
                "verdict": selected_market.get("resolution_verdict"),
                "summary": selected_market.get("resolution_summary"),
            },
        ]
    )


def _build_market_table_rows(
    opportunities: pd.DataFrame,
    market_rows: list[dict[str, object]],
) -> pd.DataFrame:
    if not opportunities.empty:
        return opportunities.copy()
    if not market_rows:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "market_id": row.get("market_id"),
                "location_name": row.get("location_name") or (row.get("spec") or {}).get("location_name"),
                "question": row.get("question"),
                "best_side": row.get("best_side"),
                "edge_bps": row.get("edge_bps"),
                "edge_bps_model": row.get("edge_bps_model"),
                "ranking_score": row.get("ranking_score"),
                "source_badge": row.get("source_badge"),
                "source_truth_status": row.get("source_truth_status"),
                "liquidity_proxy": row.get("liquidity_proxy"),
                "mapping_confidence": row.get("mapping_confidence"),
                "source_freshness_status": row.get("source_freshness_status"),
                "market_quality_status": row.get("market_quality_status"),
                "agent_review_status": row.get("agent_review_status") or row.get("rule2spec_status"),
                "actionability_status": row.get("actionability_status") or ("review_required" if row.get("rule2spec_status") else "no_trade"),
                "accepting_orders": row.get("accepting_orders"),
                "market_close_time": row.get("market_close_time") or row.get("close_time"),
                "station_id": row.get("station_id") or (row.get("spec") or {}).get("station_id"),
                "forecast_status": row.get("forecast_status"),
            }
            for row in market_rows
        ]
    )


def show() -> None:
    payload = load_market_chain_analysis_data()
    market_surface = load_operator_surface_status()["market_chain"]
    report = payload["weather_smoke_report"] or {}
    opportunities = payload["market_opportunities"]
    market_rows = payload["market_rows"]
    display_rows = _build_market_table_rows(opportunities, market_rows)
    discovery = report.get("market_discovery") or {}
    chain_status = report.get("chain_status") or ("initializing" if not report else "unknown")
    refresh_state = report.get("refresh_state")
    refresh_note = report.get("refresh_note") or discovery.get("refresh_note")

    st.markdown("### Opportunity Terminal")
    st.caption("Markets 现在首先回答哪些市场最值得优先 review、最可执行、最值得研究，但它仍处于 constrained execution boundary 内，不暗示自动推进执行。")

    if market_surface["status"] in {"read_error", "degraded_source", "refresh_in_progress", "no_data"}:
        if market_surface["status"] == "read_error":
            st.error(market_surface["detail"])
        elif market_surface["status"] == "degraded_source":
            st.warning(market_surface["detail"])
        else:
            st.info(market_surface["detail"])

    actionable = opportunities[opportunities["actionability_status"] == "actionable"] if ("actionability_status" in opportunities.columns and not opportunities.empty) else opportunities.iloc[0:0]
    top_row = opportunities.iloc[0] if not opportunities.empty else {}

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        st.metric("Actionable Markets", int(len(actionable.index)), delta=f"open={len(display_rows.index)}")
    with top2:
        st.metric("Top Ranking Score", _format_value(top_row.get("ranking_score")), delta=_format_value(top_row.get("location_name")))
    with top3:
        st.metric("Highest Edge", _format_value(pd.to_numeric(opportunities["edge_bps"], errors="coerce").abs().max()) if ("edge_bps" in opportunities.columns and not opportunities.empty) else "0", delta="bps")
    with top4:
        liquidity_ready = int(((pd.to_numeric(opportunities["liquidity_proxy"], errors="coerce").fillna(0) >= 60.0) & (opportunities["accepting_orders"] == True)).sum()) if ({"liquidity_proxy", "accepting_orders"} <= set(opportunities.columns)) else 0  # noqa: E712
        st.metric("Liquidity-Ready Markets", liquidity_ready, delta=discovery.get("market_source") or payload.get("market_opportunity_source"))

    if refresh_state == "initializing" or chain_status == "initializing":
        st.info(refresh_note or "市场链路正在生成首份或最新一轮报告，请稍候刷新。")
    elif chain_status == "transport_error":
        st.error(discovery.get("note") or "当前是 transport error，不是无市场。")
    elif chain_status == "no_open_recent_markets":
        st.warning(discovery.get("note") or "当前没有命中的开盘近期天气市场。")
    elif chain_status == "degraded":
        st.warning((report.get("forecast_service") or {}).get("note") or report.get("note") or "市场链路部分降级；已尽量保留可用的 discovery/spec/agent 结果。")
    elif chain_status != "ok" and report:
        st.error(discovery.get("note") or "市场链路当前失败。")

    overview_left, overview_right = st.columns([1.3, 1])
    with overview_left:
        st.markdown("#### Market Coverage")
        st.caption(
            " | ".join(
                [
                    f"source={discovery.get('market_source') or payload.get('market_opportunity_source')}",
                    f"horizon={discovery.get('selected_horizon_days') or 'n/a'}",
                    f"selected={discovery.get('selected_market_count') or len(opportunities.index)}",
                ]
            )
        )
        if display_rows.empty:
            st.info("No actionable weather markets yet. 等待 open recent markets 或下一轮 report。")
        else:
            coverage_columns = [
                column
                for column in [
                    "market_id",
                    "location_name",
                    "question",
                    "best_side",
                    "edge_bps",
                    "edge_bps_model",
                    "ranking_score",
                    "deployable_expected_pnl",
                    "operator_bucket",
                    "recommended_size",
                    "allocation_status",
                    "source_badge",
                    "liquidity_proxy",
                    "mapping_confidence",
                    "source_freshness_status",
                    "calibration_freshness_status",
                    "market_quality_status",
                    "agent_review_status",
                    "actionability_status",
                    "forecast_status",
                ]
                if column in display_rows.columns
            ]
            st.dataframe(display_rows[coverage_columns], width="stretch", hide_index=True)

    with overview_right:
        st.markdown("#### Top Opportunity")
        if display_rows.empty:
            st.caption("当前没有市场进入机会排序。")
        else:
            top_display_row = dict(top_row.to_dict()) if not opportunities.empty else dict(display_rows.iloc[0].to_dict())
            st.info(_opportunity_thesis(top_display_row))
            st.dataframe(
                _detail_frame(
                    [
                        {"字段": "Location", "值": top_display_row.get("location_name")},
                        {"字段": "Question", "值": top_display_row.get("question")},
                        {"字段": "Best Side", "值": top_display_row.get("best_side")},
                        {"字段": "Operator Bucket", "值": top_display_row.get("operator_bucket")},
                        {"字段": "Executable Edge (bps)", "值": top_display_row.get("edge_bps")},
                        {"字段": "Model Edge (bps)", "值": top_display_row.get("edge_bps_model")},
                        {"字段": "Mapping Confidence", "值": top_display_row.get("mapping_confidence")},
                        {"字段": "Source Freshness", "值": top_display_row.get("source_freshness_status")},
                        {"字段": "Calibration Freshness", "值": top_display_row.get("calibration_freshness_status")},
                        {"字段": "Calibration Gate", "值": top_display_row.get("calibration_gate_status")},
                        {"字段": "Calibration Gate Reasons", "值": ", ".join(str(item) for item in (top_display_row.get("calibration_gate_reason_codes") or [])) or "N/A"},
                        {"字段": "Calibration Materialized At", "值": top_display_row.get("calibration_profile_materialized_at")},
                        {"字段": "Calibration Profile Age (h)", "值": top_display_row.get("calibration_profile_age_hours")},
                        {"字段": "Quality Status", "值": top_display_row.get("market_quality_status")},
                        {"字段": "Source Badge", "值": top_display_row.get("source_badge")},
                        {"字段": "Deployable Ranking Score", "值": top_display_row.get("ranking_score")},
                        {"字段": "Base Ranking Score", "值": top_display_row.get("base_ranking_score")},
                        {"字段": "Pre-Budget Deployable PnL", "值": top_display_row.get("pre_budget_deployable_expected_pnl")},
                        {"字段": "Expected Dollar PnL", "值": top_display_row.get("expected_dollar_pnl")},
                        {"字段": "Deployable Expected PnL", "值": top_display_row.get("deployable_expected_pnl")},
                        {"字段": "Deployable Notional", "值": top_display_row.get("deployable_notional")},
                        {"字段": "Max Deployable Size", "值": top_display_row.get("max_deployable_size")},
                        {"字段": "Capture Probability", "值": top_display_row.get("capture_probability")},
                        {"字段": "Feedback Status", "值": top_display_row.get("feedback_status")},
                        {"字段": "Feedback Penalty", "值": top_display_row.get("feedback_penalty")},
                        {"字段": "Recommended Size", "值": top_display_row.get("recommended_size")},
                        {"字段": "Allocation Status", "值": top_display_row.get("allocation_status")},
                        {"字段": "Allocation Decision", "值": top_display_row.get("allocation_decision_id")},
                        {"字段": "Actionability", "值": top_display_row.get("actionability_status")},
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
            budget_impact = _json_dict(top_display_row.get("budget_impact"))
            preview = _preview_summary(top_display_row)
            if budget_impact:
                st.caption(
                    "allocation: "
                    f"preview_limit={preview.get('preview_binding_limit_scope') or 'none'}, "
                    f"preview_key={preview.get('preview_binding_limit_key') or 'none'}, "
                    f"binding_limit={budget_impact.get('binding_limit_scope') or 'none'}, "
                    f"binding_key={budget_impact.get('binding_limit_key') or 'none'}, "
                    f"remaining_budget={_format_value(budget_impact.get('remaining_run_budget'))}"
                )
            st.caption(
                "deployable ladder: "
                f"base={_format_value(top_display_row.get('base_ranking_score'))} | "
                f"pre_budget={_format_value(top_display_row.get('pre_budget_deployable_expected_pnl'))} | "
                f"final={_format_value(top_display_row.get('ranking_score'))} | "
                f"rerank={', '.join(str(item) for item in preview.get('rerank_reason_codes') or []) or 'none'}"
            )

    if display_rows.empty:
        return

    filter_left, filter_date, filter_actionability, filter_side, filter_agent, filter_orders = st.columns(6)
    with filter_left:
        location_filter = st.selectbox("City / Location", ["All", *sorted({str(value) for value in display_rows["location_name"].dropna().tolist()})] if "location_name" in display_rows.columns else ["All"])
    with filter_date:
        date_filter = st.selectbox("Date", ["All", *sorted({str(value)[:10] for value in display_rows["market_close_time"].dropna().tolist()})] if "market_close_time" in display_rows.columns else ["All"])
    with filter_actionability:
        actionability_filter = st.selectbox("Actionability", ["All", "actionable", "review_required", "blocked", "no_trade"])
    with filter_side:
        side_filter = st.selectbox("Best Side", ["All", *sorted({str(value) for value in display_rows["best_side"].dropna().tolist()})] if "best_side" in display_rows.columns else ["All"])
    with filter_agent:
        agent_filter = st.selectbox("Review Status", ["All", *sorted({str(value) for value in display_rows["agent_review_status"].dropna().tolist()})] if "agent_review_status" in display_rows.columns else ["All"])
    with filter_orders:
        accepting_only = st.checkbox("Accepting Orders Only", value=False)

    filtered = display_rows.copy()
    if location_filter != "All" and "location_name" in filtered.columns:
        filtered = filtered[filtered["location_name"] == location_filter]
    if date_filter != "All" and "market_close_time" in filtered.columns:
        filtered = filtered[filtered["market_close_time"].astype(str).str[:10] == date_filter]
    if actionability_filter != "All" and "actionability_status" in filtered.columns:
        filtered = filtered[filtered["actionability_status"] == actionability_filter]
    if side_filter != "All" and "best_side" in filtered.columns:
        filtered = filtered[filtered["best_side"] == side_filter]
    if agent_filter != "All" and "agent_review_status" in filtered.columns:
        filtered = filtered[filtered["agent_review_status"] == agent_filter]
    if accepting_only and "accepting_orders" in filtered.columns:
        filtered = filtered[filtered["accepting_orders"] == True]  # noqa: E712

    st.markdown("#### Opportunity Table")
    if filtered.empty:
        st.info("当前筛选条件下没有市场。")
        return

    table_left, detail_right = st.columns([1.35, 1])
    with table_left:
        table_columns = [
            column
            for column in [
                "market_id",
                "location_name",
                "question",
                "best_side",
                "edge_bps",
                "edge_bps_model",
                "ranking_score",
                "source_badge",
                "liquidity_proxy",
                "mapping_confidence",
                "source_freshness_status",
                "calibration_freshness_status",
                "market_quality_status",
                "agent_review_status",
                "actionability_status",
            ]
            if column in filtered.columns
        ]
        st.dataframe(filtered[table_columns], width="stretch", hide_index=True)

    selected_market_ids = filtered["market_id"].astype(str).tolist() if "market_id" in filtered.columns else []
    with detail_right:
        selected_market_id = st.selectbox(
            "Selected Market Intelligence",
            selected_market_ids,
            format_func=lambda market_id: next(
                (
                    f"{row.get('location_name') or 'Unknown'} · {row.get('question')}"
                    for row in market_rows
                    if str(row.get("market_id")) == market_id
                ),
                market_id,
            ),
        )
        selected_market = next((row for row in market_rows if str(row.get("market_id")) == selected_market_id), {})
        if not selected_market:
            selected_market = filtered[filtered["market_id"].astype(str) == selected_market_id].iloc[0].to_dict()
        preview = _preview_summary(selected_market)

        spec_detail = selected_market.get("spec") or {}
        forecast_detail = selected_market.get("forecast") or {}
        pricing_detail = selected_market.get("pricing") or {}
        signals_detail = selected_market.get("signals") or {}

        st.markdown("#### Opportunity Thesis")
        st.info(_opportunity_thesis(selected_market))
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Actionability", "值": selected_market.get("actionability_status")},
                    {"字段": "Operator Bucket", "值": selected_market.get("operator_bucket")},
                    {"字段": "Deployable Ranking Score", "值": selected_market.get("ranking_score")},
                    {"字段": "Base Ranking Score", "值": selected_market.get("base_ranking_score")},
                    {"字段": "Source Badge", "值": selected_market.get("source_badge")},
                    {"字段": "Source Truth", "值": selected_market.get("source_truth_status")},
                    {"字段": "Best Side", "值": selected_market.get("best_side")},
                    {"字段": "Best Decision", "值": selected_market.get("best_decision")},
                    {"字段": "Quality Status", "值": selected_market.get("market_quality_status")},
                    {"字段": "Review Status", "值": selected_market.get("agent_review_status")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        st.markdown("#### Operator Decision Summary")
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Operator Bucket", "值": selected_market.get("operator_bucket")},
                    {"字段": "Queue Priority", "值": selected_market.get("queue_priority")},
                    {"字段": "Queue Reason Codes", "值": ", ".join(str(item) for item in (selected_market.get("queue_reason_codes") or [])) or "N/A"},
                    {"字段": "Pre-Budget Deployable PnL", "值": selected_market.get("pre_budget_deployable_expected_pnl")},
                    {"字段": "Deployable Expected PnL", "值": selected_market.get("deployable_expected_pnl")},
                    {"字段": "Deployable Notional", "值": selected_market.get("deployable_notional")},
                    {"字段": "Max Deployable Size", "值": selected_market.get("max_deployable_size")},
                    {"字段": "Requested Size", "值": preview.get("requested_size")},
                    {"字段": "Recommended Size", "值": selected_market.get("recommended_size")},
                    {"字段": "Allocation Status", "值": selected_market.get("allocation_status")},
                    {"字段": "Allocation Decision", "值": selected_market.get("allocation_decision_id")},
                    {"字段": "Preview Binding Limit Scope", "值": preview.get("preview_binding_limit_scope")},
                    {"字段": "Preview Binding Limit Key", "值": preview.get("preview_binding_limit_key")},
                    {"字段": "Binding Limit Scope", "值": selected_market.get("binding_limit_scope")},
                    {"字段": "Binding Limit Key", "值": selected_market.get("binding_limit_key")},
                    {"字段": "Rerank Position", "值": selected_market.get("rerank_position")},
                    {"字段": "Rerank Reasons", "值": ", ".join(str(item) for item in (preview.get("rerank_reason_codes") or [])) or "N/A"},
                    {"字段": "Capital Scarcity Penalty", "值": selected_market.get("capital_scarcity_penalty")},
                    {"字段": "Concentration Penalty", "值": selected_market.get("concentration_penalty")},
                    {"字段": "Feedback Status", "值": selected_market.get("feedback_status")},
                    {"字段": "Calibration Freshness", "值": selected_market.get("calibration_freshness_status")},
                    {"字段": "Calibration Gate", "值": selected_market.get("calibration_gate_status")},
                    {"字段": "Calibration Gate Reasons", "值": ", ".join(str(item) for item in (selected_market.get("calibration_gate_reason_codes") or [])) or "N/A"},
                    {"字段": "Capital Policy", "值": selected_market.get("capital_policy_id")},
                    {"字段": "Capital Scaling Reasons", "值": ", ".join(str(item) for item in (selected_market.get("capital_scaling_reason_codes") or [])) or "N/A"},
                    {"字段": "Source Badge", "值": selected_market.get("source_badge")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        st.markdown("#### Market Structure")
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Market ID", "值": selected_market.get("market_id")},
                    {"字段": "Question", "值": selected_market.get("question")},
                    {"字段": "Location", "值": selected_market.get("location_name") or spec_detail.get("location_name")},
                    {"字段": "Station", "值": selected_market.get("station_id") or spec_detail.get("station_id")},
                    {"字段": "Mapping Confidence", "值": selected_market.get("mapping_confidence")},
                    {"字段": "Close Time", "值": selected_market.get("market_close_time") or selected_market.get("close_time")},
                    {"字段": "Accepting Orders", "值": selected_market.get("accepting_orders")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        st.markdown("#### Pricing & Edge")
        st.caption(
            "deployable ladder: "
            f"base={_format_value(selected_market.get('base_ranking_score'))} | "
            f"pre_budget={_format_value(selected_market.get('pre_budget_deployable_expected_pnl'))} | "
            f"final={_format_value(selected_market.get('ranking_score'))} | "
            f"preview_limit={preview.get('preview_binding_limit_scope') or 'none'} | "
            f"final_limit={selected_market.get('binding_limit_scope') or 'none'}"
        )
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Market Price", "值": selected_market.get("market_price")},
                    {"字段": "Model Fair Value", "值": selected_market.get("model_fair_value")},
                    {"字段": "Execution-Adjusted Fair Value", "值": selected_market.get("execution_adjusted_fair_value") or selected_market.get("fair_value")},
                    {"字段": "Model Edge (bps)", "值": selected_market.get("edge_bps_model")},
                    {"字段": "Executable Edge (bps)", "值": selected_market.get("edge_bps_executable") or selected_market.get("edge_bps")},
                    {"字段": "Fees (bps)", "值": selected_market.get("fees_bps")},
                    {"字段": "Slippage (bps)", "值": selected_market.get("slippage_bps")},
                    {"字段": "Liquidity Penalty (bps)", "值": selected_market.get("liquidity_penalty_bps")},
                    {"字段": "Fill Probability", "值": selected_market.get("fill_probability")},
                    {"字段": "Liquidity Proxy", "值": selected_market.get("liquidity_proxy")},
                    {"字段": "Confidence Score", "值": selected_market.get("confidence_score") or selected_market.get("confidence_proxy")},
                        {"字段": "Expected Value Score", "值": selected_market.get("expected_value_score")},
                        {"字段": "Expected PnL Score", "值": selected_market.get("expected_pnl_score")},
                        {"字段": "Expected Dollar PnL", "值": selected_market.get("expected_dollar_pnl")},
                        {"字段": "Pre-Budget Deployable PnL", "值": selected_market.get("pre_budget_deployable_expected_pnl")},
                        {"字段": "Deployable Expected PnL", "值": selected_market.get("deployable_expected_pnl")},
                        {"字段": "Capture Probability", "值": selected_market.get("capture_probability")},
                        {"字段": "Risk Penalty", "值": selected_market.get("risk_penalty")},
                        {"字段": "Capital Efficiency", "值": selected_market.get("capital_efficiency")},
                    {"字段": "Feedback Status", "值": selected_market.get("feedback_status")},
                    {"字段": "Feedback Penalty", "值": selected_market.get("feedback_penalty")},
                    {"字段": "Cohort Prior Version", "值": selected_market.get("cohort_prior_version")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )
        fair_values = pricing_detail.get("fair_values") or []
        if fair_values:
            st.dataframe(pd.DataFrame(fair_values), width="stretch", hide_index=True)

        st.markdown("#### Input Integrity")
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Source Freshness", "值": selected_market.get("source_freshness_status")},
                    {"字段": "Calibration Freshness", "值": selected_market.get("calibration_freshness_status")},
                    {"字段": "Calibration Gate", "值": selected_market.get("calibration_gate_status")},
                    {"字段": "Calibration Gate Reasons", "值": ", ".join(str(item) for item in (selected_market.get("calibration_gate_reason_codes") or [])) or "N/A"},
                    {"字段": "Calibration Materialized At", "值": selected_market.get("calibration_profile_materialized_at")},
                    {"字段": "Calibration Profile Window End", "值": selected_market.get("calibration_profile_window_end")},
                    {"字段": "Calibration Profile Age (h)", "值": selected_market.get("calibration_profile_age_hours")},
                    {"字段": "Price Staleness (ms)", "值": selected_market.get("price_staleness_ms")},
                    {"字段": "Mapping Confidence", "值": selected_market.get("mapping_confidence")},
                    {"字段": "Market Quality Status", "值": selected_market.get("market_quality_status")},
                    {"字段": "Forecast Status", "值": selected_market.get("forecast_status") or forecast_detail.get("status")},
                    {"字段": "Forecast Source", "值": forecast_detail.get("source_used") or selected_market.get("latest_run_source")},
                    {"字段": "Forecast Items", "值": forecast_detail.get("forecast_item_count")},
                    {"字段": "Authoritative Source", "值": spec_detail.get("authoritative_source")},
                    {"字段": "Threshold (bps)", "值": selected_market.get("threshold_bps")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        why_ranked = _json_dict(selected_market.get("why_ranked_json"))
        if why_ranked:
            st.markdown("#### Why Ranked")
            st.dataframe(
                _detail_frame(
                    [
                        {"字段": "Mode", "值": why_ranked.get("mode")},
                        {"字段": "Prior Quality", "值": why_ranked.get("prior_quality_status")},
                        {"字段": "Capture Probability", "值": why_ranked.get("capture_probability")},
                        {"字段": "Expected Dollar PnL", "值": why_ranked.get("expected_dollar_pnl")},
                        {"字段": "Deployable Expected PnL", "值": why_ranked.get("deployable_expected_pnl")},
                        {"字段": "Pre-Budget Deployable PnL", "值": why_ranked.get("pre_budget_deployable_expected_pnl")},
                        {"字段": "Base Ranking Score", "值": why_ranked.get("base_ranking_score")},
                        {"字段": "Deployable Ranking Score", "值": why_ranked.get("ranking_score")},
                        {"字段": "Calibration Gate", "值": why_ranked.get("calibration_gate_status")},
                        {"字段": "Calibration Gate Reasons", "值": ", ".join(str(item) for item in (why_ranked.get("calibration_gate_reason_codes") or [])) or "N/A"},
                        {"字段": "Requested Size", "值": why_ranked.get("requested_size")},
                        {"字段": "Max Deployable Size", "值": why_ranked.get("max_deployable_size")},
                        {"字段": "Preview Binding Limit Scope", "值": why_ranked.get("preview_binding_limit_scope")},
                        {"字段": "Preview Binding Limit Key", "值": why_ranked.get("preview_binding_limit_key")},
                        {"字段": "Binding Limit Scope", "值": why_ranked.get("binding_limit_scope")},
                        {"字段": "Binding Limit Key", "值": why_ranked.get("binding_limit_key")},
                        {"字段": "Rerank Position", "值": why_ranked.get("rerank_position")},
                        {"字段": "Rerank Reasons", "值": ", ".join(str(item) for item in (why_ranked.get("rerank_reason_codes") or [])) or "N/A"},
                        {"字段": "Risk Penalty", "值": why_ranked.get("risk_penalty")},
                        {"字段": "Capital Efficiency", "值": why_ranked.get("capital_efficiency")},
                        {"字段": "Capital Scarcity Penalty", "值": why_ranked.get("capital_scarcity_penalty")},
                        {"字段": "Concentration Penalty", "值": why_ranked.get("concentration_penalty")},
                        {"字段": "Capital Policy", "值": why_ranked.get("capital_policy_id")},
                        {"字段": "Capital Scaling Reasons", "值": ", ".join(str(item) for item in (why_ranked.get("capital_scaling_reason_codes") or [])) or "N/A"},
                        {"字段": "Feedback Status", "值": why_ranked.get("feedback_status")},
                        {"字段": "Feedback Penalty", "值": why_ranked.get("feedback_penalty")},
                        {"字段": "Pre-Feedback Ranking Score", "值": why_ranked.get("pre_feedback_ranking_score")},
                        {"字段": "Ops Tie-Breaker", "值": why_ranked.get("ops_tie_breaker")},
                        {"字段": "Ranking Score", "值": why_ranked.get("ranking_score")},
                    ]
                ),
                width="stretch",
                hide_index=True,
            )

        st.markdown("#### Execution Reality")
        executed_evidence = selected_market.get("executed_evidence") or {}
        watch_only_vs_executed = selected_market.get("watch_only_vs_executed") or {}
        market_research = selected_market.get("market_research") or {}
        cohort_history = selected_market.get("cohort_history") or []
        if not executed_evidence.get("has_executed_evidence"):
            st.info("no executed evidence yet")
        else:
            st.caption("这里展示的是 executed evidence 与 research decomposition，不代表 execution certainty。")
            st.dataframe(
                _detail_frame(
                    [
                        {"字段": "Latest Ticket", "值": executed_evidence.get("latest_ticket_id")},
                        {"字段": "Latest Order", "值": executed_evidence.get("latest_order_id")},
                        {"字段": "Predicted Edge (bps)", "值": executed_evidence.get("predicted_edge_bps")},
                        {"字段": "Expected Fill Price", "值": executed_evidence.get("expected_fill_price")},
                        {"字段": "Realized Fill Price", "值": executed_evidence.get("realized_fill_price")},
                        {"字段": "Resolution Value", "值": executed_evidence.get("resolution_value")},
                        {"字段": "Realized PnL", "值": executed_evidence.get("realized_pnl")},
                        {"字段": "Post-Trade Error", "值": executed_evidence.get("post_trade_error")},
                        {"字段": "Lifecycle Stage", "值": executed_evidence.get("execution_lifecycle_stage")},
                        {"字段": "Fill Ratio", "值": executed_evidence.get("fill_ratio")},
                        {"字段": "Adverse Fill Slippage (bps)", "值": executed_evidence.get("adverse_fill_slippage_bps")},
                        {"字段": "Resolution Lag (hrs)", "值": executed_evidence.get("resolution_lag_hours")},
                        {"字段": "Miss Reason", "值": executed_evidence.get("miss_reason_bucket")},
                        {"字段": "Source Disagreement", "值": executed_evidence.get("source_disagreement")},
                        {"字段": "Evaluation Status", "值": executed_evidence.get("evaluation_status")},
                    ]
                ),
                width="stretch",
                hide_index=True,
            )
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Executed Evidence Status", "值": market_research.get("executed_evidence_status")},
                    {"字段": "Source Badge", "值": watch_only_vs_executed.get("source_badge") or executed_evidence.get("source_badge")},
                    {"字段": "Submission Capture Ratio", "值": watch_only_vs_executed.get("submission_capture_ratio")},
                    {"字段": "Fill Capture Ratio", "值": watch_only_vs_executed.get("fill_capture_ratio")},
                    {"字段": "Resolution Capture Ratio", "值": watch_only_vs_executed.get("resolution_capture_ratio")},
                    {"字段": "Executed Ticket Count", "值": watch_only_vs_executed.get("executed_ticket_count")},
                    {"字段": "Dominant Lifecycle", "值": watch_only_vs_executed.get("dominant_lifecycle_stage")},
                    {"字段": "Miss Reason", "值": watch_only_vs_executed.get("miss_reason_bucket")},
                    {"字段": "Distortion Reason", "值": watch_only_vs_executed.get("distortion_reason_bucket")},
                    {"字段": "Feedback Status", "值": watch_only_vs_executed.get("feedback_status")},
                    {"字段": "Feedback Penalty", "值": watch_only_vs_executed.get("feedback_penalty")},
                    {"字段": "Resolved Trade Count", "值": market_research.get("resolved_trade_count")},
                    {"字段": "Avg Post-Trade Error", "值": market_research.get("avg_post_trade_error")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        st.markdown("#### Cohort History")
        if not cohort_history:
            st.info("当前没有 retrospective cohort history rows。")
        else:
            cohort_frame = pd.DataFrame(cohort_history)
            preferred_columns = [
                column
                for column in [
                    "run_id",
                    "strategy_id",
                    "ranking_decile",
                    "top_k_bucket",
                    "evaluation_status",
                    "submitted_capture_ratio",
                    "fill_capture_ratio",
                    "resolution_capture_ratio",
                    "avg_ranking_score",
                    "avg_realized_pnl",
                    "forecast_replay_change_rate",
                    "feedback_status",
                    "calibration_freshness_status",
                    "source_badge",
                ]
                if column in cohort_frame.columns
            ]
            st.dataframe(cohort_frame[preferred_columns], width="stretch", hide_index=True)

        st.markdown("#### Execution Readiness")
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Live-Prereq", "值": selected_market.get("live_prereq_status")},
                    {"字段": "Accepting Orders", "值": selected_market.get("accepting_orders")},
                    {"字段": "Best Side", "值": selected_market.get("best_side")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        st.markdown("#### Validation and Review")
        st.caption("Rule2Spec / Data QA 现在来自 deterministic validation；只有 resolution 仍是 agent-assisted review。")
        st.dataframe(_agent_review_frame(selected_market), width="stretch", hide_index=True)

        with st.expander("Diagnostic Details", expanded=False):
            diag_payload = {
                "spec": spec_detail,
                "forecast": forecast_detail,
                "pricing": pricing_detail,
                "signals": signals_detail,
            }
            st.code(json.dumps(diag_payload, ensure_ascii=False, indent=2, default=str), language="json")
