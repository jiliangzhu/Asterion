from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.data_access import load_agent_review_data, load_agent_runtime_status


def _build_agent_type_rows(review_frame: pd.DataFrame) -> pd.DataFrame:
    if review_frame.empty:
        return pd.DataFrame()
    rows = []
    for agent_type, frame in review_frame.groupby("agent_type", dropna=False):
        rows.append(
            {
                "agent_type": agent_type,
                "run_count": int(len(frame.index)),
                "success_count": int((frame["invocation_status"] == "success").sum()) if "invocation_status" in frame.columns else 0,
                "failure_count": int((frame["invocation_status"] == "failure").sum()) if "invocation_status" in frame.columns else 0,
                "review_required_count": int((frame["human_review_required"] == True).sum()) if "human_review_required" in frame.columns else 0,  # noqa: E712
                "latest_subjects": ", ".join(frame["subject_id"].astype(str).head(5).tolist()) if "subject_id" in frame.columns else "",
                "latest_verdict": frame["verdict"].dropna().astype(str).iloc[0] if ("verdict" in frame.columns and not frame["verdict"].dropna().empty) else "n/a",
            }
        )
    return pd.DataFrame(rows)


def show() -> None:
    status = load_agent_runtime_status()
    review_data = load_agent_review_data()
    review_frame = review_data["frame"]
    human_review_queue = review_frame[review_frame["human_review_required"] == True] if ("human_review_required" in review_frame.columns and not review_frame.empty) else review_frame.iloc[0:0]  # noqa: E712

    st.markdown("### Exception Review")
    st.caption("Agent 页面现在只承担 exception review / human queue；它不参与主排序、不参与 readiness 判定，也不会进入 execution path。")

    top1, top2, top3 = st.columns(3)
    with top1:
        st.metric("Human Review Queue", int(len(human_review_queue.index)), delta="review-required")
    with top2:
        failure_count = int((review_frame["invocation_status"] == "failure").sum()) if ("invocation_status" in review_frame.columns and not review_frame.empty) else 0
        st.metric("Latest Exceptions", failure_count, delta="invocation_status=failure")
    with top3:
        st.metric("Declared Runtime", status["provider"], delta=status["model"])

    st.markdown("#### Human Review Queue")
    if human_review_queue.empty:
        st.success("当前没有 agent human review queue。")
    else:
        columns = [
            column
            for column in [
                "agent_type",
                "subject_id",
                "verdict",
                "summary",
                "updated_at",
            ]
            if column in human_review_queue.columns
        ]
        st.dataframe(human_review_queue[columns], width="stretch", hide_index=True)

    st.markdown("#### Latest Agent Exceptions")
    st.caption(f"来源: {review_data['source']}")
    if review_frame.empty:
        st.info("当前没有 agent work rows。运行 real weather smoke 后，这里会展示 rule2spec / data_qa / resolution 的实际产出。")
    else:
        exception_frame = review_frame[
            (review_frame["invocation_status"] == "failure")
            | (review_frame["human_review_required"] == True)  # noqa: E712
        ] if {"invocation_status", "human_review_required"} <= set(review_frame.columns) else review_frame
        if exception_frame.empty:
            exception_frame = review_frame.head(20)
        columns = [
            column
            for column in [
                "agent_type",
                "subject_type",
                "subject_id",
                "invocation_status",
                "verdict",
                "confidence",
                "summary",
                "human_review_required",
                "updated_at",
            ]
            if column in exception_frame.columns
        ]
        st.dataframe(exception_frame[columns].head(20), width="stretch", hide_index=True)

    st.markdown("#### Agent Work by Type")
    st.caption("这些统计只描述 review activity，不代表 agent 成为 execution driver。")
    grouped = _build_agent_type_rows(review_frame)
    if grouped.empty:
        st.info("当前还没有可聚合的 agent work。")
    else:
        card_cols = st.columns(3)
        for index, agent_name in enumerate(["rule2spec", "data_qa", "resolution"]):
            frame = grouped[grouped["agent_type"] == agent_name]
            with card_cols[index]:
                if frame.empty:
                    st.metric(agent_name, "not_run", delta="当前链路没有输入")
                else:
                    row = frame.iloc[0]
                    st.metric(agent_name, int(row["run_count"]), delta=f"success={row['success_count']} failure={row['failure_count']}")
                    st.caption(f"latest_verdict={row['latest_verdict']}")
                    st.caption(f"latest_subjects={row['latest_subjects'] or 'n/a'}")
        st.dataframe(grouped, width="stretch", hide_index=True)

    st.markdown("#### Runtime Boundary")
    st.info(
        "Agents 只在执行路径之外提供 exception review、规则解析、数据质量审查、结算分析与复核建议。"
        "它们不会直接下单、撤单、改写 canonical execution tables，也不会进入机会主排序或 readiness gate。"
    )

    with st.expander("Runtime Configuration", expanded=False):
        st.caption("这里只保留 declared runtime visibility，不暴露 key presence 或 secret-adjacent 配置。")
        runtime_rows = [
            {"字段": "provider", "值": status["provider"]},
            {"字段": "model", "值": status["model"]},
            {"字段": "configured", "值": "yes" if status["configured"] else "no"},
        ]
        st.dataframe(pd.DataFrame(runtime_rows), width="stretch", hide_index=True)
        st.dataframe(pd.DataFrame(status["agents"]), width="stretch", hide_index=True)
