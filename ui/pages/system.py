from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from ui.data_access import (
    load_operator_surface_status,
    load_readiness_evidence_bundle,
    load_readiness_summary,
    load_system_runtime_status,
)


def _build_component_rows(status: dict[str, object], readiness: dict[str, object]) -> list[dict[str, object]]:
    report = readiness.get("report") or {}
    return [
        {
            "组件": "P4 Readiness Contract",
            "状态": readiness.get("go_decision") or "UNKNOWN",
            "来源": report.get("target") or "p4_live_prerequisites",
            "详情": readiness.get("decision_reason") or "未生成 readiness 报告",
        },
        {
            "组件": "Controlled-Live Capability Manifest",
            "状态": (status.get("capability_manifest_status") or "MISSING").upper(),
            "来源": status.get("capability_manifest_path"),
            "详情": f"boundary={readiness.get('capability_boundary_summary') or {}}",
        },
        {
            "组件": "UI Lite DB",
            "状态": "READY" if status.get("ui_lite_exists") else "MISSING",
            "来源": status.get("ui_lite_db_path"),
            "详情": f"tables={sum((status.get('table_row_counts') or {}).values())}",
        },
        {
            "组件": "UI Replica DB",
            "状态": "READY" if status.get("ui_replica_exists") else "MISSING",
            "来源": status.get("ui_replica_db_path"),
            "详情": "只读 replica source",
        },
        {
            "组件": "P4 Readiness JSON",
            "状态": "READY" if status.get("readiness_report_exists") else "MISSING",
            "来源": status.get("readiness_report_path"),
            "详情": "ui.phase_readiness_summary 输入",
        },
        {
            "组件": "P4 Readiness Markdown",
            "状态": "READY" if status.get("readiness_report_markdown_exists") else "MISSING",
            "来源": status.get("readiness_report_markdown_path"),
            "详情": "operator-readable report",
        },
        {
            "组件": "Real Weather Chain Report",
            "状态": status.get("weather_smoke_status") or "UNKNOWN",
            "来源": status.get("weather_smoke_report_path"),
            "详情": "real ingress smoke 辅助视图",
        },
        {
            "组件": "Calibration Profiles v2",
            "状态": (status.get("latest_calibration_freshness_status") or "MISSING").upper(),
            "来源": status.get("latest_calibration_materialized_at"),
            "详情": (
                f"window_end={status.get('latest_calibration_window_end')} "
                f"profile_age_hours={status.get('latest_calibration_profile_age_hours')} "
                f"impacted={status.get('calibration_impacted_market_count')} "
                f"hard_gate={status.get('calibration_hard_gate_market_count')}"
            ),
        },
    ]


def show() -> None:
    readiness = load_readiness_summary()
    evidence = load_readiness_evidence_bundle()
    status = load_system_runtime_status()
    surface_status = load_operator_surface_status()

    st.markdown("### Readiness Evidence")
    st.caption("System 页面主叙事是 readiness evidence 与 constrained execution boundary，而不是单一的 GO/NO-GO 口号或 file-path console。")

    surface_rows = [
        {
            "Surface": name,
            "Status": payload["status"],
            "Label": payload["label"],
            "Source": payload["source"],
            "Detail": payload["detail"],
        }
        for name, payload in surface_status.items()
        if name != "overall"
    ]
    worst_surface = surface_status["overall"]
    if worst_surface["status"] != "ok":
        if worst_surface["status"] == "read_error":
            st.error(f"{worst_surface['label']}: {worst_surface['detail']}")
        elif worst_surface["status"] == "degraded_source":
            st.warning(f"{worst_surface['label']}: {worst_surface['detail']}")
        else:
            st.info(f"{worst_surface['label']}: {worst_surface['detail']}")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Readiness", readiness.get("go_decision") or "UNKNOWN", delta=readiness.get("target") or "p4_live_prerequisites")
    with c2:
        st.metric("Failed Gates", len(readiness.get("failed_gate_names") or []), delta="P4 readiness")
    with c3:
        st.metric("UI Lite DB", "READY" if status["ui_lite_exists"] else "MISSING", delta=Path(status["ui_lite_db_path"]).name)
    with c4:
        st.metric(
            "Weather Smoke",
            status.get("weather_smoke_status") or "unknown",
            delta=f"hard_gate={status.get('calibration_hard_gate_market_count', 0)}",
        )

    st.markdown("#### Decision")
    st.info(evidence.get("decision_reason") or readiness.get("decision_reason") or "尚未生成 readiness 证据包。")
    boundary = evidence.get("capability_boundary_summary") or readiness.get("capability_boundary_summary") or {}
    if boundary:
        st.caption(
            "Capability boundary: "
            f"manual_only={boundary.get('manual_only')} · "
            f"default_off={boundary.get('default_off')} · "
            f"approve_usdc_only={boundary.get('approve_usdc_only')} · "
            f"shadow_submitter_only={boundary.get('shadow_submitter_only')} · "
            f"constrained_real_submit_enabled={boundary.get('constrained_real_submit_enabled')} · "
            f"manifest_status={boundary.get('manifest_status')}"
        )
    st.markdown("#### Capability Boundary")
    boundary_rows = [
        {"字段": "manifest_status", "值": evidence.get("capability_manifest_status") or readiness.get("capability_manifest_status")},
        {"字段": "manual_only", "值": boundary.get("manual_only")},
        {"字段": "default_off", "值": boundary.get("default_off")},
        {"字段": "approve_usdc_only", "值": boundary.get("approve_usdc_only")},
        {"字段": "shadow_submitter_only", "值": boundary.get("shadow_submitter_only")},
        {"字段": "constrained_real_submit_enabled", "值": boundary.get("constrained_real_submit_enabled")},
    ]
    st.dataframe(pd.DataFrame(boundary_rows), width="stretch", hide_index=True)

    st.markdown("#### Dependency Freshness")
    dependency_rows = []
    for name, payload in (evidence.get("dependency_statuses") or {}).items():
        dependency_rows.append(
            {
                "Dependency": name,
                "Status": payload.get("status"),
                "Updated At": payload.get("updated_at"),
                "Path": payload.get("path"),
            }
        )
    if dependency_rows:
        st.dataframe(pd.DataFrame(dependency_rows), width="stretch", hide_index=True)
    else:
        st.info("当前还没有 readiness evidence dependency rows。")

    st.markdown("#### Calibration Gate Summary")
    gate_rows = [
        {"Metric": "Impacted Markets", "Value": status.get("calibration_impacted_market_count", 0)},
        {"Metric": "Hard-Gated Markets", "Value": status.get("calibration_hard_gate_market_count", 0)},
        {"Metric": "Review Required Markets", "Value": status.get("calibration_review_required_market_count", 0)},
        {"Metric": "Research Only Markets", "Value": status.get("calibration_research_only_market_count", 0)},
    ]
    st.dataframe(pd.DataFrame(gate_rows), width="stretch", hide_index=True)

    st.markdown("#### Evidence Paths")
    path_rows = [{"路径类型": key, "路径": value} for key, value in (evidence.get("evidence_paths") or {}).items()]
    if path_rows:
        st.dataframe(pd.DataFrame(path_rows), width="stretch", hide_index=True)
    else:
        st.info("当前还没有 evidence path rows。")

    st.markdown("#### Blockers / Warnings")
    blocker_rows = [{"type": "blocker", "value": item} for item in (evidence.get("blockers") or [])]
    blocker_rows.extend({"type": "warning", "value": item} for item in (evidence.get("warnings") or []))
    if blocker_rows:
        st.dataframe(pd.DataFrame(blocker_rows), width="stretch", hide_index=True)
    else:
        st.success("当前 evidence bundle 没有 blockers / warnings。")

    st.markdown("#### Runtime Component Surface")
    rows = _build_component_rows(status, readiness)
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    st.markdown("#### Surface Status Summary")
    st.dataframe(pd.DataFrame(surface_rows), width="stretch", hide_index=True)

    st.markdown("#### Minimal Health Summary")
    health_rows = [
        {"组件": "Opportunity Surface", "值": status.get("opportunity_row_count"), "说明": "当前机会排序读面行数"},
        {"组件": "Actionable Markets", "值": status.get("actionable_market_count"), "说明": "当前可优先 review 的市场数"},
        {"组件": "Resolution Review Rows", "值": status.get("agent_row_count"), "说明": "Resolution Agent 当前可见 review rows"},
        {"组件": "Pending Operator Review", "值": status.get("pending_operator_review_count"), "说明": "建议 hold/manual/dispute 且尚未 operator 接纳的 proposal"},
        {"组件": "Blocked By Operator", "值": status.get("blocked_by_operator_review_count"), "说明": "operator 已明确阻断的 proposal"},
        {"组件": "Ready For Redeem Review", "值": status.get("ready_for_redeem_review_count"), "说明": "operator 已放行到 redeem review 的 proposal"},
        {"组件": "Calibration Freshness", "值": status.get("latest_calibration_freshness_status"), "说明": "最新 calibration profile materialization freshness"},
    ]
    st.dataframe(pd.DataFrame(health_rows), width="stretch", hide_index=True)

    phase_table = readiness["phase_table"]
    if not phase_table.empty:
        with st.expander("Readiness Gate Details", expanded=False):
            st.dataframe(phase_table, width="stretch", hide_index=True)

    with st.expander("File Paths", expanded=False):
        path_rows = [
            {"路径类型": "UI Lite DB", "路径": status["ui_lite_db_path"]},
            {"路径类型": "UI Replica DB", "路径": status["ui_replica_db_path"]},
            {"路径类型": "Readiness JSON", "路径": status["readiness_report_path"]},
            {"路径类型": "Readiness Markdown", "路径": status["readiness_report_markdown_path"]},
            {"路径类型": "Readiness Evidence", "路径": status["readiness_evidence_path"]},
            {"路径类型": "Capability Manifest", "路径": status["capability_manifest_path"]},
            {"路径类型": "Weather Smoke Report", "路径": status["weather_smoke_report_path"]},
        ]
        st.dataframe(pd.DataFrame(path_rows), width="stretch", hide_index=True)

    st.caption(
        "当前 UI 保持 constrained execution boundary：`GO` 只表示 ready for controlled live rollout decision，"
        "不表示 ready for unattended live，也不表示 unrestricted live。"
    )
