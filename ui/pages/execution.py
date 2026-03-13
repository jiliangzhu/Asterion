from __future__ import annotations

import streamlit as st

from ui.data_access import load_execution_console_data


def _filter_frame(frame, *, wallet: str, market: str, live_status: str):
    filtered = frame
    if wallet != "全部" and "wallet_id" in filtered.columns:
        filtered = filtered[filtered["wallet_id"] == wallet]
    if market != "全部" and "market_id" in filtered.columns:
        filtered = filtered[filtered["market_id"] == market]
    if live_status != "全部" and "live_prereq_execution_status" in filtered.columns:
        filtered = filtered[filtered["live_prereq_execution_status"] == live_status]
    return filtered


def show() -> None:
    payload = load_execution_console_data()
    tickets = payload["tickets"]
    runs = payload["runs"]
    exceptions = payload["exceptions"]
    live_prereq = payload["live_prereq"]
    daily_ops = payload["daily_ops"]

    st.markdown("### Execution & Live-Prereq")
    st.caption("execution page 现在优先消费 `ui.execution_ticket_summary` 和 `ui.live_prereq_execution_summary`。")

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        st.metric("Ticket Rows", len(tickets.index), delta="ui.execution_ticket_summary")
    with top2:
        aligned_count = int((live_prereq["live_prereq_execution_status"] == "shadow_aligned").sum()) if "live_prereq_execution_status" in live_prereq.columns else 0
        st.metric("Shadow Aligned", aligned_count, delta="live-prereq execution")
    with top3:
        st.metric("Exception Rows", len(exceptions.index), delta="execution_exception_summary")
    with top4:
        preview_count = int((live_prereq["latest_submit_mode"] == "dry_run").sum()) if "latest_submit_mode" in live_prereq.columns else 0
        st.metric("Preview Only", preview_count, delta="submit dry-run")

    filters = st.columns(3)
    wallet_values = ["全部"]
    market_values = ["全部"]
    live_values = ["全部"]
    if "wallet_id" in tickets.columns:
        wallet_values += sorted({str(value) for value in tickets["wallet_id"].dropna().tolist()})
    if "market_id" in tickets.columns:
        market_values += sorted({str(value) for value in tickets["market_id"].dropna().tolist()})
    if "live_prereq_execution_status" in live_prereq.columns:
        live_values += sorted({str(value) for value in live_prereq["live_prereq_execution_status"].dropna().tolist()})

    with filters[0]:
        selected_wallet = st.selectbox("Wallet", wallet_values)
    with filters[1]:
        selected_market = st.selectbox("Market", market_values)
    with filters[2]:
        selected_live_status = st.selectbox("Live-Prereq Status", live_values)

    filtered_tickets = _filter_frame(tickets, wallet=selected_wallet, market=selected_market, live_status=selected_live_status)
    filtered_live = _filter_frame(live_prereq, wallet=selected_wallet, market=selected_market, live_status=selected_live_status)

    st.markdown("#### Ticket Summary")
    if filtered_tickets.empty:
        st.info("当前没有 execution ticket 数据，或 UI lite DB 尚未生成。")
    else:
        preferred_columns = [
            column
            for column in [
                "ticket_id",
                "wallet_id",
                "market_id",
                "strategy_id",
                "execution_result",
                "order_status",
                "reconciliation_status",
                "external_reconciliation_status",
                "operator_attention_required",
                "live_prereq_execution_status",
                "live_prereq_attention_required",
            ]
            if column in filtered_tickets.columns
        ]
        st.dataframe(filtered_tickets[preferred_columns], use_container_width=True, hide_index=True)

    st.markdown("#### Live-Prereq Execution Summary")
    if filtered_live.empty:
        st.info("当前没有进入 signer / submitter / external reconciliation 的 execution rows。")
    else:
        preferred_columns = [
            column
            for column in [
                "ticket_id",
                "wallet_id",
                "order_id",
                "latest_sign_attempt_status",
                "latest_submit_mode",
                "latest_submit_status",
                "external_order_status",
                "external_reconciliation_status",
                "live_prereq_execution_status",
                "live_prereq_attention_required",
            ]
            if column in filtered_live.columns
        ]
        st.dataframe(filtered_live[preferred_columns], use_container_width=True, hide_index=True)

    lower_left, lower_right = st.columns([1.15, 1])
    with lower_left:
        st.markdown("#### Run Summary")
        if runs.empty:
            st.info("当前没有 `ui.execution_run_summary` 数据。")
        else:
            st.dataframe(runs.head(12), use_container_width=True, hide_index=True)

    with lower_right:
        st.markdown("#### Exception Summary")
        if exceptions.empty:
            st.success("当前 execution 没有异常行。")
        else:
            st.dataframe(exceptions.head(12), use_container_width=True, hide_index=True)

    st.markdown("#### Daily Ops Projection")
    if daily_ops.empty:
        st.info("当前没有 `ui.daily_ops_summary` 数据。")
    else:
        st.dataframe(daily_ops.head(10), use_container_width=True, hide_index=True)
