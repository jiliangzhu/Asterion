from __future__ import annotations

import pandas as pd
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
    tickets = payload.get("tickets", pd.DataFrame())
    runs = payload.get("runs", pd.DataFrame())
    exceptions = payload.get("exceptions", pd.DataFrame())
    live_prereq = payload.get("live_prereq", pd.DataFrame())
    daily_ops = payload.get("daily_ops", pd.DataFrame())
    predicted_vs_realized = payload.get("predicted_vs_realized", pd.DataFrame())
    watch_only_vs_executed = payload.get("watch_only_vs_executed", pd.DataFrame())
    execution_science = payload.get("execution_science", pd.DataFrame())
    calibration_health = payload.get("calibration_health", pd.DataFrame())

    st.markdown("### Execution Reality")
    st.caption("Execution 页面现在优先展示 execution science 与 execution-path evidence，再下沉 cohort capture、live-prereq exceptions 和 ticket attention。")

    top1, top2, top3, top4 = st.columns(4)
    resolved_count = int((predicted_vs_realized["evaluation_status"] == "resolved").sum()) if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0
    pending_resolution_count = int((predicted_vs_realized["evaluation_status"] == "pending_resolution").sum()) if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0
    with top1:
        st.metric("Resolved Trades", resolved_count, delta="execution evidence")
    with top2:
        st.metric("Pending Resolution", pending_resolution_count, delta="lifecycle open")
    with top3:
        avg_predicted_edge = float(pd.to_numeric(predicted_vs_realized["predicted_edge_bps"], errors="coerce").dropna().mean()) if ("predicted_edge_bps" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0.0
        st.metric("Avg Predicted Edge", f"{avg_predicted_edge:.1f}", delta="bps")
    with top4:
        resolved_frame = predicted_vs_realized[predicted_vs_realized["evaluation_status"] == "resolved"] if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else predicted_vs_realized.iloc[0:0]
        avg_realized_pnl = float(pd.to_numeric(resolved_frame["realized_pnl"], errors="coerce").dropna().mean()) if ("realized_pnl" in resolved_frame.columns and not resolved_frame.empty) else 0.0
        st.metric("Avg Realized PnL", f"{avg_realized_pnl:.4f}", delta="resolved only")

    st.markdown("#### Cohort Summary")
    cohort_left, cohort_mid, cohort_right = st.columns(3)
    with cohort_left:
        submission_capture = float(pd.to_numeric(watch_only_vs_executed["submission_capture_ratio"], errors="coerce").dropna().mean()) if ("submission_capture_ratio" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
        st.metric("Submission Capture", f"{submission_capture:.2f}", delta="opportunity lifecycle")
    with cohort_mid:
        fill_capture = float(pd.to_numeric(watch_only_vs_executed["fill_capture_ratio"], errors="coerce").dropna().mean()) if ("fill_capture_ratio" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
        st.metric("Fill Capture", f"{fill_capture:.2f}", delta="opportunity lifecycle")
    with cohort_right:
        resolution_capture = float(pd.to_numeric(watch_only_vs_executed["resolution_capture_ratio"], errors="coerce").dropna().mean()) if ("resolution_capture_ratio" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
        st.metric("Resolution Capture", f"{resolution_capture:.2f}", delta="opportunity lifecycle")

    filters = st.columns(4)
    wallet_values = ["全部"]
    market_values = ["全部"]
    strategy_values = ["全部"]
    evaluation_values = ["全部"]
    source_frame = predicted_vs_realized if not predicted_vs_realized.empty else tickets
    if "wallet_id" in source_frame.columns:
        wallet_values += sorted({str(value) for value in source_frame["wallet_id"].dropna().tolist()})
    if "market_id" in source_frame.columns:
        market_values += sorted({str(value) for value in source_frame["market_id"].dropna().tolist()})
    if "strategy_id" in source_frame.columns:
        strategy_values += sorted({str(value) for value in source_frame["strategy_id"].dropna().tolist()})
    if "evaluation_status" in predicted_vs_realized.columns:
        evaluation_values += sorted({str(value) for value in predicted_vs_realized["evaluation_status"].dropna().tolist()})

    with filters[0]:
        selected_wallet = st.selectbox("Wallet", wallet_values)
    with filters[1]:
        selected_market = st.selectbox("Market", market_values)
    with filters[2]:
        selected_strategy = st.selectbox("Strategy", strategy_values)
    with filters[3]:
        selected_evaluation = st.selectbox("Evaluation", evaluation_values)

    filtered_tickets = _filter_frame(tickets, wallet=selected_wallet, market=selected_market, live_status="全部")
    filtered_live = _filter_frame(live_prereq, wallet=selected_wallet, market=selected_market, live_status="全部")
    filtered_pvr = predicted_vs_realized.copy()
    if selected_wallet != "全部" and "wallet_id" in filtered_pvr.columns:
        filtered_pvr = filtered_pvr[filtered_pvr["wallet_id"] == selected_wallet]
    if selected_market != "全部" and "market_id" in filtered_pvr.columns:
        filtered_pvr = filtered_pvr[filtered_pvr["market_id"] == selected_market]
    if selected_strategy != "全部" and "strategy_id" in filtered_pvr.columns:
        filtered_pvr = filtered_pvr[filtered_pvr["strategy_id"] == selected_strategy]
    if selected_evaluation != "全部" and "evaluation_status" in filtered_pvr.columns:
        filtered_pvr = filtered_pvr[filtered_pvr["evaluation_status"] == selected_evaluation]
    if not filtered_tickets.empty:
        operator_attention = (
            filtered_tickets["operator_attention_required"] == True  # noqa: E712
            if "operator_attention_required" in filtered_tickets.columns
            else False
        )
        live_attention = (
            filtered_tickets["live_prereq_attention_required"] == True  # noqa: E712
            if "live_prereq_attention_required" in filtered_tickets.columns
            else False
        )
        attention_tickets = filtered_tickets[operator_attention | live_attention] if ("operator_attention_required" in filtered_tickets.columns or "live_prereq_attention_required" in filtered_tickets.columns) else filtered_tickets.iloc[0:0]
    else:
        attention_tickets = filtered_tickets

    st.markdown("#### Predicted vs Realized")
    if filtered_pvr.empty:
        st.info("当前没有 execution-path evidence rows。")
    else:
        columns = [
            column
            for column in [
                "ticket_id",
                "wallet_id",
                "strategy_id",
                "market_id",
                "predicted_edge_bps",
                "expected_fill_price",
                "realized_fill_price",
                "realized_pnl",
                "resolution_value",
                "post_trade_error",
                "source_disagreement",
                "evaluation_status",
                "latest_fill_at",
            ]
            if column in filtered_pvr.columns
        ]
        st.dataframe(filtered_pvr[columns], width="stretch", hide_index=True)

    st.markdown("#### Attention Queue")
    st.caption("这里聚合 execution / live-prereq exceptions，不是新的 execution gate。")
    if attention_tickets.empty and exceptions.empty:
        st.success("当前没有 execution / live-prereq attention rows。")
    else:
        attention_columns = [
            column
            for column in [
                "ticket_id",
                "wallet_id",
                "market_id",
                "execution_result",
                "external_reconciliation_status",
                "live_prereq_execution_status",
                "operator_attention_required",
                "live_prereq_attention_required",
            ]
            if column in attention_tickets.columns
        ]
        if attention_columns:
            st.dataframe(attention_tickets[attention_columns].head(20), width="stretch", hide_index=True)
        if not exceptions.empty:
            st.dataframe(exceptions.head(12), width="stretch", hide_index=True)

    details_left, details_right = st.columns([1.2, 1.05])
    with details_left:
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
                    "live_prereq_execution_status",
                ]
                if column in filtered_tickets.columns
            ]
            st.dataframe(filtered_tickets[preferred_columns], width="stretch", hide_index=True)

    with details_right:
        st.markdown("#### Executed Evidence Detail")
        if filtered_pvr.empty:
            st.info("当前筛选下没有 executed evidence。")
        else:
            latest = filtered_pvr.iloc[0]
            detail_rows = pd.DataFrame(
                [
                    {"字段": "Ticket", "值": latest.get("ticket_id")},
                    {"字段": "Predicted Edge (bps)", "值": latest.get("predicted_edge_bps")},
                    {"字段": "Expected Fill Price", "值": latest.get("expected_fill_price")},
                    {"字段": "Realized Fill Price", "值": latest.get("realized_fill_price")},
                    {"字段": "Resolution Value", "值": latest.get("resolution_value")},
                    {"字段": "Realized PnL", "值": latest.get("realized_pnl")},
                    {"字段": "Post-Trade Error", "值": latest.get("post_trade_error")},
                    {"字段": "Lifecycle Stage", "值": latest.get("execution_lifecycle_stage")},
                    {"字段": "Fill Ratio", "值": latest.get("fill_ratio")},
                    {"字段": "Adverse Fill Slippage (bps)", "值": latest.get("adverse_fill_slippage_bps")},
                    {"字段": "Resolution Lag (hrs)", "值": latest.get("resolution_lag_hours")},
                    {"字段": "Miss Reason", "值": latest.get("miss_reason_bucket")},
                    {"字段": "Source Disagreement", "值": latest.get("source_disagreement")},
                ]
            )
            st.dataframe(detail_rows, width="stretch", hide_index=True)

        st.markdown("#### Live-Prereq Execution")
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
                    "latest_submit_status",
                    "external_order_status",
                    "external_reconciliation_status",
                    "live_prereq_execution_status",
                ]
                if column in filtered_live.columns
            ]
            st.dataframe(filtered_live[preferred_columns], width="stretch", hide_index=True)

    lower_left, lower_right = st.columns([1.15, 1])
    with lower_left:
        st.markdown("#### Execution Science Cohorts")
        if execution_science.empty:
            st.info("当前没有 `ui.execution_science_summary` 数据。")
        else:
            strategy_science = execution_science[execution_science["cohort_type"] == "strategy"] if "cohort_type" in execution_science.columns else execution_science
            preferred_columns = [
                column
                for column in [
                    "cohort_key",
                    "ticket_count",
                    "submission_capture_ratio",
                    "fill_capture_ratio",
                    "resolution_capture_ratio",
                    "dominant_miss_reason_bucket",
                    "dominant_distortion_reason_bucket",
                ]
                if column in strategy_science.columns
            ]
            st.dataframe(strategy_science[preferred_columns].head(10), width="stretch", hide_index=True)

        st.markdown("#### Run Summary")
        if runs.empty:
            st.info("当前没有 `ui.execution_run_summary` 数据。")
        else:
            st.dataframe(runs.head(12), width="stretch", hide_index=True)

    with lower_right:
        st.markdown("#### Watch-Only vs Executed")
        if watch_only_vs_executed.empty:
            st.info("当前没有 `ui.watch_only_vs_executed_summary` 数据。")
        else:
            preferred_columns = [
                column
                for column in [
                    "market_id",
                    "avg_executable_edge_bps",
                    "submission_capture_ratio",
                    "fill_capture_ratio",
                    "resolution_capture_ratio",
                    "executed_ticket_count",
                    "dominant_lifecycle_stage",
                    "miss_reason_bucket",
                    "distortion_reason_bucket",
                ]
                if column in watch_only_vs_executed.columns
            ]
            st.dataframe(watch_only_vs_executed[preferred_columns].head(10), width="stretch", hide_index=True)

    st.markdown("#### Daily Ops Projection")
    if daily_ops.empty:
        st.info("当前没有 `ui.daily_ops_summary` 数据。")
    else:
        st.dataframe(daily_ops.head(10), width="stretch", hide_index=True)
