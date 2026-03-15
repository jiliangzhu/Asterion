from __future__ import annotations

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
                "agent": "rule2spec",
                "status": selected_market.get("rule2spec_status") or selected_market.get("agent_review_status"),
                "verdict": selected_market.get("rule2spec_verdict"),
                "summary": selected_market.get("rule2spec_summary"),
            },
            {
                "agent": "data_qa",
                "status": selected_market.get("data_qa_status"),
                "verdict": selected_market.get("data_qa_verdict"),
                "summary": selected_market.get("data_qa_summary"),
            },
            {
                "agent": "resolution",
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
                "opportunity_score": row.get("opportunity_score"),
                "liquidity_proxy": row.get("liquidity_proxy"),
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
    st.caption("Markets 现在首先回答哪些市场最值得优先看、最可执行、最值得赚钱，而不是只展示链路 debug 细节。")

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
        st.metric("Top Opportunity Score", _format_value(top_row.get("opportunity_score")), delta=_format_value(top_row.get("location_name")))
    with top3:
        st.metric("Highest Edge", _format_value(pd.to_numeric(opportunities["edge_bps"], errors="coerce").max()) if ("edge_bps" in opportunities.columns and not opportunities.empty) else "0", delta="bps")
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
                    "opportunity_score",
                    "liquidity_proxy",
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
                        {"字段": "Edge (bps)", "值": top_display_row.get("edge_bps")},
                        {"字段": "Opportunity Score", "值": top_display_row.get("opportunity_score")},
                        {"字段": "Actionability", "值": top_display_row.get("actionability_status")},
                    ]
                ),
                width="stretch",
                hide_index=True,
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
        agent_filter = st.selectbox("Agent Review", ["All", *sorted({str(value) for value in display_rows["agent_review_status"].dropna().tolist()})] if "agent_review_status" in display_rows.columns else ["All"])
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
                "opportunity_score",
                "liquidity_proxy",
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
                    {"字段": "Opportunity Score", "值": selected_market.get("opportunity_score")},
                    {"字段": "Best Side", "值": selected_market.get("best_side")},
                    {"字段": "Best Decision", "值": selected_market.get("best_decision")},
                    {"字段": "Agent Review", "值": selected_market.get("agent_review_status")},
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
                    {"字段": "Close Time", "值": selected_market.get("market_close_time") or selected_market.get("close_time")},
                    {"字段": "Accepting Orders", "值": selected_market.get("accepting_orders")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        st.markdown("#### Pricing & Edge")
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Market Price", "值": selected_market.get("market_price")},
                    {"字段": "Fair Value", "值": selected_market.get("fair_value")},
                    {"字段": "Edge (bps)", "值": selected_market.get("edge_bps")},
                    {"字段": "Liquidity Proxy", "值": selected_market.get("liquidity_proxy")},
                    {"字段": "Confidence Proxy", "值": selected_market.get("confidence_proxy")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )
        fair_values = pricing_detail.get("fair_values") or []
        if fair_values:
            st.dataframe(pd.DataFrame(fair_values), width="stretch", hide_index=True)

        st.markdown("#### Execution Readiness")
        st.dataframe(
            _detail_frame(
                [
                    {"字段": "Live-Prereq", "值": selected_market.get("live_prereq_status")},
                    {"字段": "Forecast Status", "值": selected_market.get("forecast_status") or forecast_detail.get("status")},
                    {"字段": "Threshold (bps)", "值": selected_market.get("threshold_bps")},
                    {"字段": "Forecast Source", "值": forecast_detail.get("source_used") or selected_market.get("latest_run_source")},
                    {"字段": "Forecast Items", "值": forecast_detail.get("forecast_item_count")},
                    {"字段": "Authoritative Source", "值": spec_detail.get("authoritative_source")},
                ]
            ),
            width="stretch",
            hide_index=True,
        )

        st.markdown("#### Agent Review")
        st.dataframe(_agent_review_frame(selected_market), width="stretch", hide_index=True)

        with st.expander("Diagnostic Details", expanded=False):
            diag_payload = {
                "spec": spec_detail,
                "forecast": forecast_detail,
                "pricing": pricing_detail,
                "signals": signals_detail,
            }
            st.code(json.dumps(diag_payload, ensure_ascii=False, indent=2, default=str), language="json")
