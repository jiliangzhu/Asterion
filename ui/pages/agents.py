from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.components import (
    render_detail_key_value,
    render_empty_state,
    render_kpi_band,
    render_page_intro,
    render_reason_chip_row,
    render_section_header,
    render_state_card,
)
from ui.data_access import (
    load_agent_runtime_status,
    load_opportunity_triage_data,
    load_resolution_review_data,
    write_opportunity_triage_operator_review_decision,
    write_resolution_operator_review_decision,
)
from ui.triage_localization import localize_reason_codes, localize_triage_frame, localize_triage_value

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


def _submit_triage_action(
    row: dict[str, object],
    *,
    decision_status: str,
    actor: str,
    reason: str | None,
) -> None:
    invocation_id = str(row.get("latest_agent_invocation_id") or "")
    operator_action = str(row.get("recommended_operator_action") or "manual_review")
    market_id = str(row.get("market_id") or "")
    if not invocation_id or not market_id:
        raise RuntimeError("missing invocation_id or market_id for triage operator review")
    write_opportunity_triage_operator_review_decision(
        market_id=market_id,
        invocation_id=invocation_id,
        decision_status=decision_status,
        operator_action=operator_action,
        actor=actor,
        reason=reason,
    )


def show() -> None:
    status = load_agent_runtime_status()
    review_data = load_resolution_review_data()
    review_frame = review_data["frame"]
    triage_data = load_opportunity_triage_data()
    triage_frame = triage_data["frame"]
    localized_triage_frame = localize_triage_frame(triage_frame) if not triage_frame.empty else triage_frame

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
            {"label": "Triage Rows", "value": int(len(triage_frame.index)), "delta": triage_data["source"]},
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

    render_section_header("Opportunity Triage", subtitle="Opportunity Triage Agent 是 advisory-only overlay；Markets 和 Agents 都允许接 accept / ignore / defer。")
    if triage_frame.empty:
        render_empty_state("No triage rows", "当前还没有 persisted opportunity triage rows。")
    else:
        st.caption(f"来源: {triage_data['source']}")
        st.dataframe(
            _display_frame(
                localized_triage_frame.rename(
                    columns={
                        "market_id": "市场ID",
                        "location_name": "地点",
                        "question": "问题",
                        "priority_band": "优先级",
                        "recommended_operator_action": "建议动作",
                        "effective_triage_status": "当前状态",
                        "latest_operator_review_status": "最近人工复核",
                        "latest_operator_action": "最近人工动作",
                        "surface_delivery_status": "读面交付状态",
                    }
                ),
                [
                    "市场ID",
                    "地点",
                    "问题",
                    "优先级",
                    "建议动作",
                    "当前状态",
                    "最近人工复核",
                    "最近人工动作",
                    "读面交付状态",
                ],
            ),
            width="stretch",
            hide_index=True,
        )
        triage_actor = st.text_input("处理人", value="operator", key="triage_review_actor")
        for _, raw_row in triage_frame.head(10).iterrows():
            row = raw_row.to_dict()
            localized_row = {key: localize_triage_value(key, value) for key, value in row.items()}
            market_id = str(row.get("market_id") or "")
            with st.expander(f"{market_id} · {localized_row.get('effective_triage_status') or '暂无分诊'}", expanded=False):
                render_detail_key_value(
                    [
                        ("market_id", row.get("market_id")),
                        ("location_name", row.get("location_name")),
                        ("question", row.get("question")),
                        ("优先级", localized_row.get("priority_band")),
                        ("建议动作", localized_row.get("recommended_operator_action")),
                        ("最近一次 triage 状态", localized_row.get("latest_triage_status")),
                        ("最近一次 agent 状态", localized_row.get("latest_agent_status")),
                        ("当前生效状态", localized_row.get("effective_triage_status")),
                        ("建议门控", localized_row.get("advisory_gate_status")),
                        ("最近一次评估方法", localized_row.get("latest_evaluation_method")),
                        ("最近一次评估结果", localized_row.get("latest_evaluation_verified")),
                        ("最近一次人工复核", localized_row.get("latest_operator_review_status")),
                        ("最近一次人工动作", localized_row.get("latest_operator_action")),
                        ("surface_delivery_status", row.get("surface_delivery_status")),
                        ("surface_fallback_origin", row.get("surface_fallback_origin")),
                        ("calibration_gate_status", row.get("calibration_gate_status")),
                        ("capital_policy_id", row.get("capital_policy_id")),
                    ]
                )
                render_reason_chip_row(
                    localize_reason_codes(row.get("advisory_gate_reason_codes") or [], empty_label="triage_gate:enabled"),
                    empty_label="分诊建议已启用",
                )
                triage_reason = st.text_input("处理说明", key=f"triage_reason_{market_id}")
                triage_cols = st.columns(3)
                triage_actions = [
                    ("接受", "accepted"),
                    ("忽略", "ignored"),
                    ("延后", "deferred"),
                ]
                for col, (label, decision_status) in zip(triage_cols, triage_actions, strict=True):
                    with col:
                        if st.button(label, key=f"{decision_status}_triage_{market_id}", use_container_width=True):
                            try:
                                _submit_triage_action(
                                    row,
                                    decision_status=decision_status,
                                    actor=triage_actor or "operator",
                                    reason=triage_reason or None,
                                )
                            except Exception as exc:  # noqa: BLE001
                                st.error(str(exc))
                            else:
                                st.success(f"{market_id} -> {localize_triage_value('latest_operator_review_status', decision_status)}")
                                st.rerun()

    render_section_header("Runtime boundary", subtitle="Resolution Agent 继续只提供结构化建议，不进入 canonical execution path。")
    render_state_card(
        "boundary",
        "Agent surfaces 继续只提供结构化建议。系统不会让 agent 直接改写 canonical execution tables，也不会让 LLM 输出直接驱动 execution / redeem / dispute。",
        tone="info",
    )

    with st.expander("Runtime Configuration", expanded=False):
        runtime_rows = [
            {"字段": "provider", "值": status["provider"]},
            {"字段": "model", "值": status["model"]},
            {"字段": "configured", "值": "yes" if status["configured"] else "no"},
            {"字段": "key_source", "值": status.get("key_source")},
            {"字段": "provider_runtime_status", "值": status.get("provider_runtime_status")},
            {"字段": "provider_runtime_detail", "值": status.get("provider_runtime_detail")},
        ]
        st.dataframe(pd.DataFrame(runtime_rows), width="stretch", hide_index=True)
        st.dataframe(pd.DataFrame(status["agents"]), width="stretch", hide_index=True)
