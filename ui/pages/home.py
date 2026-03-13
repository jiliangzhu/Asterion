from __future__ import annotations

import streamlit as st

from ui.data_access import build_ops_console_overview


def show() -> None:
    overview = build_ops_console_overview()
    readiness = overview["readiness"]
    execution = overview["execution"]
    market_data = overview["market_data"]
    metrics = overview["metrics"]
    wallet_attention = overview["wallet_attention"]
    agent_data = overview["agent_data"]["frame"]

    st.markdown("### 总控台")
    st.caption("目标是在一屏内回答 readiness、最大 blocker 和当前天气链路是否健康。")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("P4 Readiness", metrics["go_decision"], delta=readiness.get("target") or "p4_live_prerequisites")
    with col2:
        st.metric("失败 Gate", metrics["failed_gate_count"], delta="P4 live-prereq")
    with col3:
        st.metric("钱包 Ready", f'{metrics["wallet_ready_count"]}/{metrics["wallet_total_count"]}', delta="live-prereq wallets")
    with col4:
        st.metric("执行注意项", metrics["live_prereq_attention_count"], delta=f'{metrics["exception_count"]} exception rows')

    top_left, top_right = st.columns([1.5, 1.1])
    with top_left:
        st.markdown("#### Controlled Rollout Decision")
        decision_reason = readiness.get("decision_reason") or "尚未生成 P4 readiness 报告。"
        st.info(decision_reason)
        failed_gate_names = readiness.get("failed_gate_names") or []
        if failed_gate_names:
            st.error("当前 blocker: " + " / ".join(failed_gate_names))
        else:
            st.success("当前没有 gate-level blocker。")
        st.caption(f"来源: {readiness.get('report_path')}")

    with top_right:
        smoke_report = market_data.get("weather_smoke_report") or {}
        discovery = smoke_report.get("market_discovery") or {}
        chain_status = smoke_report.get("chain_status") or "unknown"
        st.markdown("#### Real Weather Chain")
        st.metric("链路状态", chain_status, delta=discovery.get("market_source") or "report")
        st.write(discovery.get("question") or "当前没有命中的开盘近期天气市场。")
        st.caption(
            " | ".join(
                [
                    f"selected={metrics['weather_market_count']}",
                    f"horizon={discovery.get('selected_horizon_days') or 'n/a'}",
                    f"source={discovery.get('market_source') or 'n/a'}",
                    f"market_id={discovery.get('market_id') or 'n/a'}",
                ]
            )
        )
        if metrics["weather_locations"]:
            st.caption("城市覆盖: " + ", ".join(metrics["weather_locations"][:8]))

    lower_left, lower_right = st.columns([1.15, 1])
    with lower_left:
        st.markdown("#### 最新异常与执行状态")
        exception_frame = execution["exceptions"]
        if exception_frame.empty:
            st.success("当前 `ui.execution_exception_summary` 没有记录异常。")
        else:
            columns = [
                column
                for column in [
                    "ticket_id",
                    "execution_result",
                    "latest_transition_to_status",
                    "reconciliation_status",
                    "external_reconciliation_status",
                    "live_prereq_execution_status",
                ]
                if column in exception_frame.columns
            ]
            st.dataframe(exception_frame[columns].head(8), width="stretch", hide_index=True)

    with lower_right:
        st.markdown("#### Wallet Readiness")
        if wallet_attention.empty:
            st.success("当前 `can_trade=true` 钱包没有 live-prereq blocker。")
        else:
            columns = [
                column
                for column in [
                    "wallet_id",
                    "wallet_readiness_status",
                    "wallet_readiness_blockers_json",
                    "latest_chain_tx_status",
                ]
                if column in wallet_attention.columns
            ]
            st.dataframe(wallet_attention[columns].head(6), width="stretch", hide_index=True)

    bottom_left, bottom_right = st.columns([1.1, 1])
    with bottom_left:
        st.markdown("#### Market Coverage")
        st.metric("Open Recent Markets", metrics["weather_market_count"], delta="当前可映射城市市场")
        if metrics["weather_locations"]:
            st.write("当前覆盖城市:")
            st.caption(" / ".join(metrics["weather_locations"][:12]))
        else:
            st.caption("当前没有命中的开盘近期天气市场。")

    with bottom_right:
        st.markdown("#### Recent Agent Activity")
        st.metric("Agent Rows", metrics["agent_activity_count"], delta=f"review_required={metrics['agent_review_required_count']}")
        if agent_data.empty:
            st.info("当前没有 agent activity；若运行 weather smoke，将默认触发 rule2spec agent。")
        else:
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

    st.markdown("#### 快速入口")
    st.info("使用左侧导航可快速切换到 Markets、Execution、Agents、System。")

    st.markdown("#### 关键提示")
    st.caption(
        "当前仓库状态是 `P4 closed / ready for controlled live rollout decision`。"
        "这不表示 unattended live，controlled live 仍必须保持 manual-only、default-off、approve_usdc only。"
    )
