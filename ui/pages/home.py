from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.data_access import load_home_decision_snapshot


def _format_metric_value(value: object) -> str:
    if value is None or value == "" or pd.isna(value):
        return "N/A"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def show() -> None:
    overview = load_home_decision_snapshot()
    surface_status = overview.get("surface_status", {})
    readiness = overview.get("readiness", {})
    execution = overview.get("execution", {"exceptions": pd.DataFrame()})
    market_data = overview.get("market_data", {})
    metrics = overview.get("metrics", {})
    wallet_attention = overview.get("wallet_attention", pd.DataFrame())
    top_opportunities = overview.get("top_opportunities", pd.DataFrame())
    largest_blocker = overview.get("largest_blocker", {"source": "unknown", "summary": "unknown"})
    recent_agent = overview.get("recent_agent_summary", {})
    agent_data = overview.get("agent_data", {}).get("frame", pd.DataFrame())
    evidence = overview.get("readiness_evidence", {})
    predicted_vs_realized = overview.get("predicted_vs_realized_snapshot", pd.DataFrame())
    degraded_inputs = overview.get("degraded_inputs", [])
    uncaptured_high_edge = overview.get("uncaptured_high_edge_markets", pd.DataFrame())

    st.markdown("### Decision Console")
    st.caption("首页优先回答 readiness decision、最大 blocker、当前最佳机会，以及 execution-path evidence；agent 产出只作为 exception-review evidence，不作为主排序输入。")

    degraded_surfaces = [
        f"{name}: {payload['label']}"
        for name, payload in surface_status.items()
        if name != "overall" and payload["status"] in {"read_error", "refresh_in_progress", "degraded_source", "no_data"}
    ]
    if degraded_surfaces:
        st.warning("当前关键数据面状态: " + " | ".join(degraded_surfaces))

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        st.metric("Readiness Decision", metrics.get("go_decision", "UNKNOWN"), delta=readiness.get("target") or "p4_live_prerequisites")
    with top2:
        st.metric("Actionable Markets", metrics.get("actionable_market_count", 0), delta=f"open={metrics.get('weather_market_count', 0)}")
    with top3:
        st.metric("Top Opportunity Score", _format_metric_value(metrics.get("top_opportunity_score")), delta="opportunity-first")
    with top4:
        st.metric("Largest Current Blocker", largest_blocker["source"], delta=largest_blocker["summary"])

    row1_left, row1_right = st.columns([1.2, 1.05])
    with row1_left:
        st.markdown("#### Readiness Decision")
        st.info(readiness.get("decision_reason") or "尚未生成 P4 readiness 报告。")
        failed_gate_names = readiness.get("failed_gate_names") or []
        if failed_gate_names:
            st.error("当前 blocker: " + " / ".join(failed_gate_names))
        else:
            st.success("当前没有 gate-level blocker。")
        st.caption(
            "当前仓库状态是 `post-P4 remediation active / closeout pending objective verification`，"
            "当前系统定位是 `operator console + constrained execution infra`，这不表示 unattended live。"
        )

    with row1_right:
        st.markdown("#### Largest Current Blocker")
        if largest_blocker["source"] == "clear":
            st.success("No material blocker")
        else:
            st.warning(largest_blocker["summary"])
        st.caption(f"来源: {largest_blocker['source']}")
        st.metric("Liquidity-Ready", metrics.get("liquidity_ready_count", 0), delta=f"highest_edge={_format_metric_value(metrics.get('highest_edge_bps'))}bps")

    row2_left, row2_right = st.columns([1.35, 1])
    with row2_left:
        st.markdown("#### Top Opportunities")
        if top_opportunities.empty:
            st.info("当前还没有进入机会排序的市场。")
        else:
            columns = [
                column
                for column in [
                    "location_name",
                    "question",
                    "best_side",
                    "edge_bps",
                    "edge_bps_model",
                    "opportunity_score",
                    "ranking_score",
                    "mapping_confidence",
                    "source_freshness_status",
                    "market_quality_status",
                    "agent_review_status",
                    "actionability_status",
                ]
                if column in top_opportunities.columns
            ]
            st.dataframe(top_opportunities[columns].head(5), width="stretch", hide_index=True)

    with row2_right:
        st.markdown("#### Predicted vs Realized Snapshot")
        st.metric("Resolved Trades", metrics.get("resolved_trade_count", 0), delta=f"pending={metrics.get('pending_resolution_count', 0)}")
        st.write(f"avg predicted edge: `{_format_metric_value(metrics.get('avg_predicted_edge_bps'))} bps`")
        st.write(f"avg realized pnl: `{_format_metric_value(metrics.get('avg_realized_pnl'))}`")
        if predicted_vs_realized.empty:
            st.info("当前还没有 executed-only predicted-vs-realized rows。")
        else:
            columns = [
                column
                for column in [
                    "ticket_id",
                    "market_id",
                    "predicted_edge_bps",
                    "realized_pnl",
                    "evaluation_status",
                    "source_disagreement",
                ]
                if column in predicted_vs_realized.columns
            ]
            st.dataframe(predicted_vs_realized[columns], width="stretch", hide_index=True)

    row3_left, row3_right = st.columns([1.1, 1.1])
    with row3_left:
        st.markdown("#### Market Coverage")
        weather_locations = metrics.get("weather_locations") or []
        st.metric("Cities Covered", len(weather_locations), delta=f"source={(market_data.get('weather_smoke_report') or {}).get('market_discovery', {}).get('market_source') or market_data.get('market_opportunity_source')}")
        if weather_locations:
            st.caption(" / ".join(weather_locations[:12]))
        else:
            st.caption("当前没有命中的开盘近期天气市场。")

    with row3_right:
        st.markdown("#### Edge Capture")
        st.write(f"submission: `{_format_metric_value(metrics.get('submission_capture_ratio'))}`")
        st.write(f"fill: `{_format_metric_value(metrics.get('fill_capture_ratio'))}`")
        st.write(f"resolution: `{_format_metric_value(metrics.get('resolution_capture_ratio'))}`")
        st.caption(f"uncaptured={metrics.get('uncaptured_high_edge_count', 0)}")
        if uncaptured_high_edge.empty:
            st.success("当前没有 uncaptured high-edge markets。")
        else:
            columns = [
                column
                for column in [
                    "market_id",
                    "avg_executable_edge_bps",
                    "submission_capture_ratio",
                    "fill_capture_ratio",
                    "resolution_capture_ratio",
                    "miss_reason_bucket",
                    "distortion_reason_bucket",
                ]
                if column in uncaptured_high_edge.columns
            ]
            st.dataframe(uncaptured_high_edge[columns].head(5), width="stretch", hide_index=True)

    row4_left, row4_right = st.columns([1.1, 1.1])
    with row4_left:
        st.markdown("#### Wallet & Execution Attention")
        execution_exceptions = execution["exceptions"]
        if wallet_attention.empty and execution_exceptions.empty:
            st.success("当前没有 wallet / execution attention rows。")
        else:
            frames = []
            if not wallet_attention.empty:
                frames.append(
                    wallet_attention[[column for column in ["wallet_id", "wallet_readiness_status", "latest_chain_tx_status"] if column in wallet_attention.columns]].head(4)
                )
            if not execution_exceptions.empty:
                frames.append(
                    execution_exceptions[[column for column in ["ticket_id", "live_prereq_execution_status", "external_reconciliation_status"] if column in execution_exceptions.columns]].head(4)
                )
            combined = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
            st.dataframe(combined, width="stretch", hide_index=True)

    with row4_right:
        st.markdown("#### Readiness Evidence")
        st.metric("Evidence Bundle", "READY" if evidence.get("exists") else "MISSING", delta=evidence.get("capability_manifest_status") or "missing")
        if evidence.get("blockers"):
            st.error(" / ".join(str(item) for item in evidence.get("blockers")[:4]))
        elif evidence.get("warnings"):
            st.warning(" / ".join(str(item) for item in evidence.get("warnings")[:4]))
        else:
            st.success(evidence.get("decision_reason") or "当前 evidence bundle 无 blocker。")

    st.markdown("#### Degraded Inputs")
    if degraded_inputs:
        for item in degraded_inputs[:8]:
            st.write(f"- `{item}`")
    else:
        st.success("当前没有 degraded input summary。")

    st.markdown("#### Recent Agent Work")
    st.metric("Agent Rows", metrics.get("agent_activity_count", 0), delta=f"review_required={metrics.get('agent_review_required_count', 0)}")
    st.caption("这里展示的是 exception-review evidence，不是 execution driver，也不进入 readiness gate。")
    if recent_agent.get("agent_type"):
        st.write(f"最新 agent: `{recent_agent['agent_type']}`")
        st.write(f"verdict: `{recent_agent.get('verdict') or 'n/a'}`")
        st.caption(recent_agent.get("summary") or "最近一次 agent 产出暂无摘要。")
    else:
        st.info("当前没有 agent activity；运行 weather smoke 后会在这里显示最新产出。")
    if not agent_data.empty:
        columns = [
            column
            for column in [
                "agent_type",
                "subject_id",
                "invocation_status",
                "verdict",
                "summary",
                "updated_at",
            ]
            if column in agent_data.columns
        ]
        st.dataframe(agent_data[columns].head(6), width="stretch", hide_index=True)
