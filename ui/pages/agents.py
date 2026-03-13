from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from ui.data_access import load_agent_review_data, load_agent_runtime_status


def show() -> None:
    status = load_agent_runtime_status()
    review_data = load_agent_review_data()
    review_frame = review_data["frame"]

    st.markdown("### Agent Workbench")
    st.caption("这里重点展示 agent 实际做了什么、产出了什么，以及哪些条目需要人工复核。配置仅作为辅助信息。")

    top1, top2, top3 = st.columns(3)
    with top1:
        st.metric("Provider", status["provider"], delta="effective agent provider")
    with top2:
        st.metric("Model", status["model"], delta="来自 .env / env vars")
    with top3:
        st.metric("API Key", "configured" if status["configured"] else "missing", delta=status["key_source"])

    if status["configured"]:
        st.success("当前 weather agents 已检测到可用模型配置。")
    else:
        st.error("当前未检测到可用 agent API key/model 配置。请检查 `.env`。")

    st.markdown("#### Latest Agent Activity")
    st.caption(f"来源: {review_data['source']}")
    if review_frame.empty:
        st.info("当前没有 agent work rows。运行 real weather smoke 后，这里会展示 rule2spec / data_qa / resolution 的实际产出。")
    else:
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
            if column in review_frame.columns
        ]
        st.dataframe(review_frame[columns], use_container_width=True, hide_index=True)

    st.markdown("#### Agent Work by Type")
    if review_frame.empty:
        st.info("当前还没有可聚合的 agent work。")
    else:
        grouped_rows = []
        for agent_type, frame in review_frame.groupby("agent_type", dropna=False):
            grouped_rows.append(
                {
                    "agent_type": agent_type,
                    "row_count": int(len(frame.index)),
                    "success_count": int((frame["invocation_status"] == "success").sum()) if "invocation_status" in frame.columns else 0,
                    "failure_count": int((frame["invocation_status"] != "success").sum()) if "invocation_status" in frame.columns else 0,
                    "human_review_required_count": int((frame["human_review_required"] == True).sum()) if "human_review_required" in frame.columns else 0,  # noqa: E712
                    "latest_subjects": ", ".join(frame["subject_id"].astype(str).head(5).tolist()) if "subject_id" in frame.columns else "",
                }
            )
        st.dataframe(pd.DataFrame(grouped_rows), use_container_width=True, hide_index=True)

    st.markdown("#### Weather Agents")
    st.dataframe(pd.DataFrame(status["agents"]), use_container_width=True, hide_index=True)

    st.markdown("#### Runtime Boundary")
    st.info(
        "当前 agent 仅用于规则解析、数据质量审查、结算分析等辅助面。"
        "它们不会直接下单、撤单、改写 canonical execution tables，也不会绕过人工审批。"
    )

    with st.expander("当前环境读取结果", expanded=False):
        env_rows = [
            {"键": "QWEN_MODEL", "值": os.getenv("QWEN_MODEL", "") or "未配置"},
            {"键": "QWEN_API_KEY", "值": "已配置" if os.getenv("QWEN_API_KEY") else "未配置"},
            {"键": "ALIBABA_API_KEY", "值": "已配置" if os.getenv("ALIBABA_API_KEY") else "未配置"},
            {"键": "ASTERION_AGENT_PROVIDER", "值": os.getenv("ASTERION_AGENT_PROVIDER", "") or "默认自动选择"},
        ]
        st.dataframe(pd.DataFrame(env_rows), use_container_width=True, hide_index=True)
