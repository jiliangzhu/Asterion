from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.data_access import (
    load_agent_runtime_status,
    load_resolution_review_data,
    write_resolution_operator_review_decision,
)


def _display_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    visible = [column for column in columns if column in frame.columns]
    return frame[visible] if visible else frame


def _submit_review_action(
    row: dict[str, object],
    *,
    decision_status: str,
    actor: str,
    reason: str | None,
) -> None:
    invocation_id = str(row.get("latest_agent_invocation_id") or "")
    suggestion_id = str(row.get("suggestion_id") or "")
    operator_action = str(row.get("latest_recommended_operator_action") or row.get("redeem_decision") or "observe")
    if not invocation_id or not suggestion_id:
        raise RuntimeError("missing invocation_id or suggestion_id for operator review")
    write_resolution_operator_review_decision(
        proposal_id=str(row.get("proposal_id") or ""),
        invocation_id=invocation_id,
        suggestion_id=suggestion_id,
        decision_status=decision_status,
        operator_action=operator_action,
        actor=actor,
        reason=reason,
    )


def show() -> None:
    status = load_agent_runtime_status()
    review_data = load_resolution_review_data()
    review_frame = review_data["frame"]

    pending = (
        review_frame[review_frame["effective_redeem_status"] == "pending_operator_review"]
        if ("effective_redeem_status" in review_frame.columns and not review_frame.empty)
        else review_frame.iloc[0:0]
    )
    blocked = (
        review_frame[review_frame["effective_redeem_status"] == "blocked_by_operator_review"]
        if ("effective_redeem_status" in review_frame.columns and not review_frame.empty)
        else review_frame.iloc[0:0]
    )
    ready = (
        review_frame[review_frame["effective_redeem_status"] == "ready_for_redeem_review"]
        if ("effective_redeem_status" in review_frame.columns and not review_frame.empty)
        else review_frame.iloc[0:0]
    )

    st.markdown("### Resolution Review")
    st.caption(
        "Agents 页面现在只承担 Resolution Agent 的 human review queue。"
        "Rule2Spec 和 Data QA 已收口为 deterministic validation，不再作为 LLM workbench 展示。"
    )

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        st.metric("Pending Review", int(len(pending.index)), delta="pending_operator_review")
    with top2:
        st.metric("Blocked", int(len(blocked.index)), delta="blocked_by_operator_review")
    with top3:
        st.metric("Ready", int(len(ready.index)), delta="ready_for_redeem_review")
    with top4:
        st.metric("Declared Runtime", status["provider"], delta=status["model"])

    st.markdown("#### Resolution Queue")
    st.caption(f"来源: {review_data['source']}")
    if review_frame.empty:
        st.info("当前没有 resolution review rows。")
    else:
        st.dataframe(
            _display_frame(
                review_frame,
                [
                    "proposal_id",
                    "market_id",
                    "proposal_status",
                    "redeem_decision",
                    "latest_agent_verdict",
                    "latest_recommended_operator_action",
                    "latest_settlement_risk_score",
                    "latest_operator_review_status",
                    "latest_operator_action",
                    "effective_redeem_status",
                ],
            ),
            width="stretch",
            hide_index=True,
        )

    st.markdown("#### Operator Actions")
    st.caption("真正有约束力的是 operator review state；agent 建议本身不会直接触发 dispute 或 redeem。")
    if review_frame.empty:
        st.success("当前没有需要人工 review 的 resolution proposals。")
    else:
        actor = st.text_input("Operator", value="operator", key="resolution_review_actor")
        for _, raw_row in review_frame.head(10).iterrows():
            row = raw_row.to_dict()
            proposal_id = str(row.get("proposal_id") or "")
            with st.expander(f"{proposal_id} · {row.get('effective_redeem_status') or 'unknown'}", expanded=False):
                st.dataframe(
                    _display_frame(
                        pd.DataFrame([row]),
                        [
                            "proposal_id",
                            "market_id",
                            "proposal_status",
                            "expected_outcome",
                            "proposed_outcome",
                            "verification_confidence",
                            "redeem_decision",
                            "redeem_reason",
                            "latest_agent_verdict",
                            "latest_agent_summary",
                            "latest_recommended_operator_action",
                            "latest_settlement_risk_score",
                            "latest_operator_review_status",
                            "latest_operator_action",
                            "effective_redeem_status",
                        ],
                    ),
                    width="stretch",
                    hide_index=True,
                )
                reason = st.text_input("Reason", key=f"resolution_reason_{proposal_id}")
                action_cols = st.columns(3)
                actions = [
                    ("Accept", "accepted"),
                    ("Reject", "rejected"),
                    ("Defer", "deferred"),
                ]
                for col, (label, decision_status) in zip(action_cols, actions, strict=True):
                    with col:
                        if st.button(label, key=f"{decision_status}_{proposal_id}", use_container_width=True):
                            try:
                                _submit_review_action(
                                    row,
                                    decision_status=decision_status,
                                    actor=actor or "operator",
                                    reason=reason or None,
                                )
                            except Exception as exc:  # noqa: BLE001
                                st.error(str(exc))
                            else:
                                st.success(f"{proposal_id} -> {decision_status}")
                                st.rerun()

    st.markdown("#### Runtime Boundary")
    st.info(
        "Resolution Agent 继续只提供结构化建议。"
        "系统不会让 agent 直接改写 canonical execution tables，也不会让 LLM 输出直接驱动 redeem/dispute。"
    )

    with st.expander("Runtime Configuration", expanded=False):
        runtime_rows = [
            {"字段": "provider", "值": status["provider"]},
            {"字段": "model", "值": status["model"]},
            {"字段": "configured", "值": "yes" if status["configured"] else "no"},
        ]
        st.dataframe(pd.DataFrame(runtime_rows), width="stretch", hide_index=True)
        st.dataframe(pd.DataFrame(status["agents"]), width="stretch", hide_index=True)
