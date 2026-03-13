from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.data_access import load_market_chain_analysis_data


def _format_detail_value(value: object) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".") if value != int(value) else str(int(value))
    return str(value)


def _format_bucket_range(min_value: object, max_value: object) -> str:
    if min_value is None and max_value is None:
        return "N/A"
    return f"{_format_detail_value(min_value)} - {_format_detail_value(max_value)}"


def _detail_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"字段": str(row.get("字段") or ""), "值": _format_detail_value(row.get("值"))} for row in rows]
    )


def show() -> None:
    payload = load_market_chain_analysis_data()
    market_watch = payload["market_watch"]
    report = payload["weather_smoke_report"] or {}
    market_rows = payload["market_rows"]
    discovery = report.get("market_discovery") or {}
    rule_parse = report.get("rule_parse") or {}
    forecast = report.get("forecast_service") or {}
    pricing = report.get("pricing_engine") or {}
    opportunity = report.get("opportunity_discovery") or {}

    st.markdown("### 实时天气市场链路")
    st.caption("这里优先展示当前 open recent weather market 的 discovery/spec/forecast/pricing/watch-only 结果。")

    report_exists = bool(report)
    chain_status = report.get("chain_status") or ("initializing" if not report_exists else "unknown")
    left, right = st.columns([1.2, 1])
    with left:
        st.metric("市场链状态", chain_status, delta=discovery.get("market_source") or "report")
        if not report_exists:
            st.write("市场链路正在初始化，等待首轮报告生成。")
        else:
            st.write(discovery.get("question") or "当前没有命中的开盘近期天气市场。")
        st.caption(
            " | ".join(
                [
                    f"selected={discovery.get('selected_market_count') or discovery.get('discovered_count') or 0}",
                    f"horizon={discovery.get('selected_horizon_days') or 'n/a'}",
                    f"market_id={discovery.get('market_id') or 'n/a'}",
                    f"close_time={discovery.get('close_time') or 'n/a'}",
                ]
            )
        )
    with right:
        st.metric("Spec Station", rule_parse.get("station_id") or "N/A", delta=rule_parse.get("location_name") or "station-first")
        st.metric("Forecast Source", forecast.get("source_used") or "N/A", delta=" / ".join(forecast.get("source_trace") or []))

    if chain_status == "initializing":
        st.info("市场链路正在生成首份或最新一轮报告，请稍候刷新。")
    elif chain_status == "transport_error":
        st.error(discovery.get("note") or "当前是 transport error，不是无市场。")
    elif chain_status == "no_open_recent_markets":
        st.warning(discovery.get("note") or "当前没有命中的开盘近期天气市场。")
    elif chain_status != "ok":
        st.error(discovery.get("note") or "市场链路当前失败。")

    detail_rows = [
        {"字段": "Market Source", "值": discovery.get("market_source")},
        {"字段": "Selected Horizon", "值": discovery.get("selected_horizon_days")},
        {"字段": "Location", "值": rule_parse.get("location_name")},
        {
            "字段": "Bucket Range",
            "值": _format_bucket_range(rule_parse.get("bucket_min_value"), rule_parse.get("bucket_max_value")),
        },
        {"字段": "Forecast Source", "值": forecast.get("source_used")},
        {"字段": "Forecast Item Count", "值": forecast.get("forecast_item_count")},
    ]
    st.markdown("#### Discovery / Spec / Forecast")
    st.dataframe(_detail_frame(detail_rows), width="stretch", hide_index=True)

    selected_markets = discovery.get("selected_markets") or []
    if selected_markets:
        st.markdown("#### Open Recent Weather Markets")
        market_frame = pd.DataFrame(selected_markets)
        filter_left, filter_right, filter_status, filter_orders = st.columns(4)
        with filter_left:
            location_filter = st.selectbox(
                "Location",
                ["All", *sorted({str(item.get("location_name")) for item in selected_markets if item.get("location_name")})],
                index=0,
            )
        with filter_right:
            station_filter = st.selectbox(
                "Station",
                ["All", *sorted({str(item.get("station_id")) for item in selected_markets if item.get("station_id")})],
                index=0,
            )
        with filter_status:
            agent_filter = st.selectbox(
                "Agent Status",
                ["All", "success", "failure", "not_run", "skipped"],
                index=0,
            )
        with filter_orders:
            accepting_only = st.checkbox("Accepting Orders Only", value=False)

        def _market_matches(item: dict[str, object]) -> bool:
            if location_filter != "All" and item.get("location_name") != location_filter:
                return False
            if station_filter != "All" and item.get("station_id") != station_filter:
                return False
            if agent_filter != "All" and item.get("rule2spec_status") != agent_filter:
                return False
            if accepting_only and not bool(item.get("accepting_orders")):
                return False
            return True

        filtered_rows = [item for item in market_rows if _market_matches(item)]
        filtered_frame = pd.DataFrame(
            [
                {
                    "market_id": item.get("market_id"),
                    "question": item.get("question"),
                    "location_name": item.get("location_name"),
                    "station_id": item.get("station_id"),
                    "accepting_orders": item.get("accepting_orders"),
                    "rule2spec_status": item.get("rule2spec_status"),
                    "data_qa_status": item.get("data_qa_status"),
                    "resolution_status": item.get("resolution_status"),
                    "close_time": item.get("close_time"),
                }
                for item in filtered_rows
            ]
        )
        if filtered_frame.empty:
            st.info("当前筛选条件下没有市场。")
        else:
            selector_left, selector_right = st.columns([1.2, 1.1])
            with selector_left:
                st.dataframe(filtered_frame, width="stretch", hide_index=True)
            with selector_right:
                selected_market_id = st.selectbox(
                    "Selected Market Detail",
                    [str(item["market_id"]) for item in filtered_rows],
                    format_func=lambda market_id: next(
                        (
                            f"{item.get('location_name') or 'Unknown'} · {item.get('question')}"
                            for item in filtered_rows
                            if str(item.get("market_id")) == market_id
                        ),
                        market_id,
                    ),
                )
                selected_market = next(item for item in filtered_rows if str(item.get("market_id")) == selected_market_id)

                spec_detail = selected_market.get("spec") or {}
                forecast_detail = selected_market.get("forecast") or {}
                pricing_detail = selected_market.get("pricing") or {}
                signals_detail = selected_market.get("signals") or {}

                st.markdown("#### Selected Market Detail")
                st.markdown("##### 1. Discovery")
                st.dataframe(
                    _detail_frame(
                        [
                            {"字段": "Market ID", "值": selected_market.get("market_id")},
                            {"字段": "Question", "值": selected_market.get("question")},
                            {"字段": "Close Time", "值": selected_market.get("close_time")},
                            {"字段": "Source", "值": discovery.get("market_source")},
                            {"字段": "Accepting Orders", "值": selected_market.get("accepting_orders")},
                        ]
                    ),
                    width="stretch",
                    hide_index=True,
                )

                st.markdown("##### 2. Spec")
                st.dataframe(
                    _detail_frame(
                        [
                            {"字段": "Location", "值": spec_detail.get("location_name")},
                            {"字段": "Station", "值": spec_detail.get("station_id")},
                            {"字段": "Metric", "值": spec_detail.get("metric")},
                            {
                                "字段": "Bucket Range",
                                "值": _format_bucket_range(
                                    spec_detail.get("bucket_min_value"), spec_detail.get("bucket_max_value")
                                ),
                            },
                            {"字段": "Authoritative Source", "值": spec_detail.get("authoritative_source")},
                        ]
                    ),
                    width="stretch",
                    hide_index=True,
                )

                st.markdown("##### 3. Forecast")
                st.dataframe(
                    _detail_frame(
                        [
                            {"字段": "Forecast Run", "值": forecast_detail.get("forecast_run_id")},
                            {"字段": "Source Used", "值": forecast_detail.get("source_used")},
                            {"字段": "Source Trace", "值": " / ".join(forecast_detail.get("source_trace") or [])},
                            {"字段": "Requested Source", "值": forecast.get("source_requested")},
                            {"字段": "Forecast Items", "值": forecast.get("forecast_item_count")},
                        ]
                    ),
                    width="stretch",
                    hide_index=True,
                )

                st.markdown("##### 4. Fair Value")
                fair_values = pricing_detail.get("fair_values") or []
                if fair_values:
                    st.dataframe(pd.DataFrame(fair_values), width="stretch", hide_index=True)
                else:
                    st.info("当前市场没有 fair value 结果。")

                st.markdown("##### 5. Opportunity")
                signals = signals_detail.get("signals") or []
                if signals:
                    st.dataframe(pd.DataFrame(signals), width="stretch", hide_index=True)
                else:
                    st.info("当前市场没有 watch-only signal。")

                st.markdown("##### 6. Agent Review")
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "agent": "rule2spec",
                                "status": selected_market.get("rule2spec_status"),
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
                    ),
                    width="stretch",
                    hide_index=True,
                )

    st.markdown("#### Fair Value")
    fair_values = pricing.get("fair_values") or []
    if fair_values:
        st.dataframe(pd.DataFrame(fair_values), width="stretch", hide_index=True)
    else:
        st.info("当前没有 fair value 结果。")

    st.markdown("#### Watch-Only Signals")
    signals = opportunity.get("signals") or []
    if signals:
        st.dataframe(pd.DataFrame(signals), width="stretch", hide_index=True)
    else:
        st.info("当前没有生成 watch-only signals。")

    st.markdown("#### Canonical Market Watch Summary")
    if market_watch.empty:
        st.info("当前 `ui.market_watch_summary` 不存在或为空；页面使用 weather smoke report 作为辅助视图。")
    else:
        preferred_columns = [
            column
            for column in [
                "market_id",
                "question",
                "snapshot_id",
                "decision",
                "side",
                "edge_bps",
                "reference_price",
                "fair_value",
                "forecast_source",
            ]
            if column in market_watch.columns
        ]
        st.dataframe(market_watch[preferred_columns].head(20), width="stretch", hide_index=True)

    st.caption("默认不会展示 2021 冻结样本，除非报告明确进入 frozen fallback mode。")
