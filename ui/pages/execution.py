from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.components import (
    render_detail_key_value,
    render_empty_state,
    render_kpi_band,
    render_page_intro,
    render_section_header,
    render_state_card,
)
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
    cohort_history = payload.get("cohort_history", pd.DataFrame())

    render_page_intro(
        "Execution Reality",
        "Execution 页面现在优先展示 execution science 与 execution-path evidence，再下沉 cohort capture、live-prereq exceptions 和 ticket attention。",
        kicker="Evidence lab",
        badges=[
            ("execution science", "info"),
            ("execution-path evidence", "ok"),
        ],
    )

    resolved_count = int((predicted_vs_realized["evaluation_status"] == "resolved").sum()) if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0
    pending_resolution_count = int((predicted_vs_realized["evaluation_status"] == "pending_resolution").sum()) if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0
    avg_predicted_edge = float(pd.to_numeric(predicted_vs_realized["predicted_edge_bps"], errors="coerce").dropna().mean()) if ("predicted_edge_bps" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0.0
    resolved_frame = predicted_vs_realized[predicted_vs_realized["evaluation_status"] == "resolved"] if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else predicted_vs_realized.iloc[0:0]
    avg_realized_pnl = float(pd.to_numeric(resolved_frame["realized_pnl"], errors="coerce").dropna().mean()) if ("realized_pnl" in resolved_frame.columns and not resolved_frame.empty) else 0.0

    render_kpi_band(
        [
            {"label": "Resolved Trades", "value": resolved_count, "delta": "execution evidence"},
            {"label": "Pending Resolution", "value": pending_resolution_count, "delta": "lifecycle open"},
            {"label": "Avg Predicted Edge", "value": f"{avg_predicted_edge:.1f}", "delta": "bps diagnostic"},
            {"label": "Avg Realized PnL", "value": f"{avg_realized_pnl:.4f}", "delta": "resolved only"},
        ]
    )

    render_section_header("Cohort summary", subtitle="把 capture reality 固定在顶部，作为 execution quality 的第一层事实。")
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
        if "operator_attention_required" in filtered_tickets.columns or "live_prereq_attention_required" in filtered_tickets.columns:
            attention_tickets = filtered_tickets[operator_attention | live_attention]
        else:
            attention_tickets = filtered_tickets.iloc[0:0]
    else:
        attention_tickets = filtered_tickets

    evidence_tab, attention_tab, science_tab, history_tab, live_tab = st.tabs(
        ["Predicted vs realized", "Attention queue", "Execution science", "Cohort history", "Live prereq"]
    )

    with evidence_tab:
        render_section_header("Predicted vs realized", subtitle="这里是 execution-path evidence 主表，不再和其他 attention/debug rows 混在一起。")
        if filtered_pvr.empty:
            render_empty_state("No execution evidence", "当前没有 execution-path evidence rows。")
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
                    "source_badge",
                    "post_trade_error",
                    "source_disagreement",
                    "evaluation_status",
                    "latest_fill_at",
                    "feedback_status",
                    "feedback_penalty",
                ]
                if column in filtered_pvr.columns
            ]
            st.dataframe(filtered_pvr[columns], width="stretch", hide_index=True)

        render_section_header("Executed evidence detail", subtitle="把单个 executed case 组织成 case card，而不是纯 dataframe。")
        if filtered_pvr.empty:
            render_empty_state("No selected evidence", "当前筛选下没有 executed evidence。")
        else:
            latest = filtered_pvr.iloc[0]
            render_detail_key_value(
                [
                    ("Ticket", latest.get("ticket_id")),
                    ("Predicted Edge (bps)", latest.get("predicted_edge_bps")),
                    ("Expected Fill Price", latest.get("expected_fill_price")),
                    ("Realized Fill Price", latest.get("realized_fill_price")),
                    ("Resolution Value", latest.get("resolution_value")),
                    ("Realized PnL", latest.get("realized_pnl")),
                    ("Post-Trade Error", latest.get("post_trade_error")),
                    ("Lifecycle Stage", latest.get("execution_lifecycle_stage")),
                    ("Fill Ratio", latest.get("fill_ratio")),
                    ("Adverse Fill Slippage (bps)", latest.get("adverse_fill_slippage_bps")),
                    ("Resolution Lag (hrs)", latest.get("resolution_lag_hours")),
                    ("Miss Reason", latest.get("miss_reason_bucket")),
                    ("Source Disagreement", latest.get("source_disagreement")),
                ]
            )

    with attention_tab:
        render_section_header("Attention queue", subtitle="这里聚合 execution / live-prereq exceptions，不是新的 execution gate。")
        if attention_tickets.empty and exceptions.empty:
            render_state_card("attention", "当前没有 execution / live-prereq attention rows。", tone="ok")
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

        render_section_header("Ticket summary", subtitle="保留 ticket 表，但作为 attention queue 的支撑层，而不是默认主画面。")
        if filtered_tickets.empty:
            render_empty_state("No execution tickets", "当前没有 execution ticket 数据，或 UI lite DB 尚未生成。")
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

    with science_tab:
        render_section_header("Execution science cohorts", subtitle="保留 cohort 指标，但用它回答 capture 与 distortion，而不是当成杂表。")
        if execution_science.empty:
            render_empty_state("No execution science rows", "当前没有 `ui.execution_science_summary` 数据。")
        else:
            strategy_science = execution_science[execution_science["cohort_type"] == "strategy"] if "cohort_type" in execution_science.columns else execution_science
            preferred_columns = [
                column
                for column in [
                    "cohort_key",
                    "source_badge",
                    "ticket_count",
                    "submission_capture_ratio",
                    "fill_capture_ratio",
                    "resolution_capture_ratio",
                    "feedback_status",
                    "feedback_penalty",
                    "dominant_miss_reason_bucket",
                    "dominant_distortion_reason_bucket",
                ]
                if column in strategy_science.columns
            ]
            st.dataframe(strategy_science[preferred_columns].head(10), width="stretch", hide_index=True)

        render_section_header("Watch-only vs executed", subtitle="保留 watch-only 对 executed 的对照，作为 capture reality 的补充。")
        if watch_only_vs_executed.empty:
            render_empty_state("No watch-only comparison rows", "当前没有 `ui.watch_only_vs_executed_summary` 数据。")
        else:
            preferred_columns = [
                column
                for column in [
                    "market_id",
                    "source_badge",
                    "avg_executable_edge_bps",
                    "submission_capture_ratio",
                    "fill_capture_ratio",
                    "resolution_capture_ratio",
                    "feedback_status",
                    "feedback_penalty",
                    "executed_ticket_count",
                    "dominant_lifecycle_stage",
                    "miss_reason_bucket",
                    "distortion_reason_bucket",
                ]
                if column in watch_only_vs_executed.columns
            ]
            st.dataframe(watch_only_vs_executed[preferred_columns].head(10), width="stretch", hide_index=True)

    with history_tab:
        render_section_header("Cohort history", subtitle="历史 cohort rows 和 run summary 继续保留，但不占默认主叙事。")
        if cohort_history.empty:
            render_empty_state("No cohort history rows", "当前没有 retrospective cohort history rows。")
        else:
            filtered_history = cohort_history.copy()
            if selected_market != "全部" and "market_id" in filtered_history.columns:
                filtered_history = filtered_history[filtered_history["market_id"] == selected_market]
            if selected_strategy != "全部" and "strategy_id" in filtered_history.columns:
                filtered_history = filtered_history[filtered_history["strategy_id"] == selected_strategy]
            preferred_columns = [
                column
                for column in [
                    "run_id",
                    "market_id",
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
                if column in filtered_history.columns
            ]
            st.dataframe(filtered_history[preferred_columns].head(20), width="stretch", hide_index=True)

        render_section_header("Run summary")
        if runs.empty:
            render_empty_state("No run summary rows", "当前没有 `ui.execution_run_summary` 数据。")
        else:
            st.dataframe(runs.head(12), width="stretch", hide_index=True)

        render_section_header("Daily ops projection")
        if daily_ops.empty:
            render_empty_state("No daily ops rows", "当前没有 `ui.daily_ops_summary` 数据。")
        else:
            st.dataframe(daily_ops.head(10), width="stretch", hide_index=True)

    with live_tab:
        render_section_header("Live-prereq execution", subtitle="signer / submitter / reconciliation rows 继续可见，但不和主 evidence 混排。")
        if filtered_live.empty:
            render_empty_state("No live-prereq execution rows", "当前没有进入 signer / submitter / external reconciliation 的 execution rows。")
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
