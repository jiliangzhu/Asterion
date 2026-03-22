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
from ui.data_access import (
    load_agent_runtime_status,
    load_resolution_review_data,
    write_resolution_operator_review_decision,
)

# Baseline wording retained for operator-boundary doc tests: ### Resolution Review


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

    render_page_intro(
        "Resolution Review",
        "Agents 页面现在只承担 Resolution Agent 的 human review queue。Rule2Spec 和 Data QA 已收口为 deterministic validation，不再作为 LLM workbench 展示。",
        kicker="Exception review",
        badges=[
            ("human review queue", "info"),
            ("operator review state", "warn"),
        ],
    )

    render_kpi_band(
        [
            {"label": "Pending Review", "value": int(len(pending.index)), "delta": "pending_operator_review"},
            {"label": "Blocked", "value": int(len(blocked.index)), "delta": "blocked_by_operator_review"},
            {"label": "Ready", "value": int(len(ready.index)), "delta": "ready_for_redeem_review"},
            {"label": "Declared Runtime", "value": status["provider"], "delta": status["model"]},
        ]
    )

    render_section_header("Resolution queue", subtitle="主表只展示当前 proposal 状态和 operator 需要接的动作。")
    st.caption(f"来源: {review_data['source']}")
    if review_frame.empty:
        render_empty_state("No resolution review rows", "当前没有 resolution review rows。")
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

    render_section_header("Operator actions", subtitle="真正有约束力的是 operator review state；agent 建议本身不会直接触发 dispute 或 redeem。")
    if review_frame.empty:
        render_state_card("review queue", "当前没有需要人工 review 的 resolution proposals。", tone="ok")
    else:
        actor = st.text_input("Operator", value="operator", key="resolution_review_actor")
        for _, raw_row in review_frame.head(10).iterrows():
            row = raw_row.to_dict()
            proposal_id = str(row.get("proposal_id") or "")
            with st.expander(f"{proposal_id} · {row.get('effective_redeem_status') or 'unknown'}", expanded=False):
                render_detail_key_value(
                    [
                        ("proposal_id", row.get("proposal_id")),
                        ("market_id", row.get("market_id")),
                        ("proposal_status", row.get("proposal_status")),
                        ("expected_outcome", row.get("expected_outcome")),
                        ("proposed_outcome", row.get("proposed_outcome")),
                        ("verification_confidence", row.get("verification_confidence")),
                        ("redeem_decision", row.get("redeem_decision")),
                        ("redeem_reason", row.get("redeem_reason")),
                        ("latest_agent_verdict", row.get("latest_agent_verdict")),
                        ("latest_agent_summary", row.get("latest_agent_summary")),
                        ("latest_recommended_operator_action", row.get("latest_recommended_operator_action")),
                        ("latest_settlement_risk_score", row.get("latest_settlement_risk_score")),
                        ("latest_operator_review_status", row.get("latest_operator_review_status")),
                        ("latest_operator_action", row.get("latest_operator_action")),
                        ("effective_redeem_status", row.get("effective_redeem_status")),
                    ]
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

    render_section_header("Runtime boundary", subtitle="Resolution Agent 继续只提供结构化建议，不进入 canonical execution path。")
    render_state_card(
        "boundary",
        "Resolution Agent 继续只提供结构化建议。系统不会让 agent 直接改写 canonical execution tables，也不会让 LLM 输出直接驱动 redeem/dispute。",
        tone="info",
    )

    with st.expander("Runtime Configuration", expanded=False):
        runtime_rows = [
            {"字段": "provider", "值": status["provider"]},
            {"字段": "model", "值": status["model"]},
            {"字段": "configured", "值": "yes" if status["configured"] else "no"},
        ]
        st.dataframe(pd.DataFrame(runtime_rows), width="stretch", hide_index=True)
        st.dataframe(pd.DataFrame(status["agents"]), width="stretch", hide_index=True)
