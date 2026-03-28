from __future__ import annotations

from pathlib import Path

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
    load_operator_surface_status,
    load_readiness_evidence_bundle,
    load_readiness_summary,
    load_system_runtime_status,
)
from ui.triage_localization import localize_triage_value


def _build_component_rows(status: dict[str, object], readiness: dict[str, object]) -> list[dict[str, object]]:
    report = readiness.get("report") or {}
    return [
        {
            "组件": "Operator Surface Refresh",
            "状态": (status.get("latest_surface_refresh_status") or "UNKNOWN").upper(),
            "来源": status.get("latest_surface_refresh_run_id"),
            "详情": (
                f"degraded={status.get('degraded_surface_count', 0)} "
                f"read_error={status.get('read_error_surface_count', 0)}"
            ),
        },
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
            "详情": (
                f"canonical_db={status.get('canonical_db_path') or 'missing'} "
                f"split_brain={status.get('source_split_brain')}"
            ),
        },
        {
            "组件": "Calibration Profiles v2",
            "状态": (status.get("latest_calibration_freshness_status") or "MISSING").upper(),
            "来源": status.get("latest_calibration_materialized_at"),
            "详情": (
                f"window_end={status.get('latest_calibration_window_end')} "
                f"profile_age_hours={status.get('latest_calibration_profile_age_hours')} "
                f"samples={status.get('calibration_sample_count')} "
                f"profiles={status.get('calibration_profile_count')} "
                f"impacted={status.get('calibration_impacted_market_count')} "
                f"hard_gate={status.get('calibration_hard_gate_market_count')}"
            ),
        },
        {
            "组件": "Calibration Bootstrap",
            "状态": (status.get("calibration_bootstrap_status") or "NOT_RUN").upper(),
            "来源": "real_weather_chain_report.runtime_chain",
            "详情": f"refresh={status.get('calibration_refresh_status') or 'not_run'}",
        },
        {
            "组件": "Resolution Agent Runtime",
            "状态": (status.get("resolution_runtime_status") or status.get("resolution_latest_run_status") or "NOT_RUN").upper(),
            "来源": "runtime_chain / agent.invocations",
            "详情": f"subjects={status.get('resolution_subject_count', 0)} latest={status.get('resolution_latest_run_status') or 'not_run'}",
        },
        {
            "组件": "Paper Execution Runtime",
            "状态": (status.get("paper_execution_status") or ("CLOSED" if int(status.get("fill_count") or 0) > 0 else "OPEN")).upper(),
            "来源": "runtime_chain / runtime.strategy_runs / trading.orders / trading.fills",
            "详情": (
                f"strategy_runs={status.get('strategy_run_count', 0)} "
                f"tickets={status.get('trade_ticket_count', 0)} "
                f"allocations={status.get('allocation_decision_count', 0)} "
                f"orders={status.get('paper_order_count', 0)} "
                f"fills={status.get('fill_count', 0)}"
            ),
        },
        {
            "组件": "Settlement Feedback Closure",
            "状态": (status.get("profitability_settlement_feedback_closure_status") or "OPEN").upper(),
            "来源": "real_weather_chain_report.settlement_feedback_pipeline",
            "详情": (
                f"pending={status.get('profitability_pending_resolution_ticket_count', 0)} "
                f"resolved={status.get('profitability_resolved_ticket_count', 0)} "
                f"realized_rows={status.get('profitability_realized_pnl_row_count', 0)} "
                f"writeback={status.get('profitability_latest_feedback_writeback_status') or 'not_run'}"
            ),
        },
    ]


def show() -> None:
    readiness = load_readiness_summary()
    evidence = load_readiness_evidence_bundle()
    status = load_system_runtime_status()
    surface_status = load_operator_surface_status()

    render_page_intro(
        "Readiness Evidence",
        "System 页面主叙事是 readiness evidence 与 constrained execution boundary，而不是单一的 GO/NO-GO 口号或 file-path console。",
        kicker="Runtime evidence wall",
        badges=[
            ("readiness evidence", "info"),
            ("constrained execution boundary", "warn"),
        ],
    )

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

    render_kpi_band(
        [
            {"label": "Readiness", "value": readiness.get("go_decision") or "UNKNOWN", "delta": readiness.get("target") or "p4_live_prerequisites"},
            {"label": "Failed Gates", "value": len(readiness.get("failed_gate_names") or []), "delta": "P4 readiness"},
            {"label": "UI Lite DB", "value": status.get("ui_lite_status") or ("READY" if status["ui_lite_exists"] else "MISSING"), "delta": Path(status["ui_lite_db_path"]).name},
            {"label": "Surface Refresh", "value": status.get("latest_surface_refresh_status") or "unknown", "delta": f"hard_gate={status.get('calibration_hard_gate_market_count', 0)}"},
        ]
    )

    render_section_header("Decision", subtitle="先回答 readiness decision，再下沉 boundary、delivery 和 evidence 细节。")
    render_state_card(
        "decision",
        evidence.get("decision_reason") or readiness.get("decision_reason") or "尚未生成 readiness 证据包。",
        tone="info",
    )
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
    delivery_left, delivery_right = st.columns([1.1, 1.05])
    with delivery_left:
        render_section_header("Capability boundary", subtitle="当前系统不是 unattended live；所有真实 side effects 保持 default-off + auditable。")
        render_detail_key_value(
            [
                ("manifest_status", evidence.get("capability_manifest_status") or readiness.get("capability_manifest_status")),
                ("manual_only", boundary.get("manual_only")),
                ("default_off", boundary.get("default_off")),
                ("approve_usdc_only", boundary.get("approve_usdc_only")),
                ("shadow_submitter_only", boundary.get("shadow_submitter_only")),
                ("constrained_real_submit_enabled", boundary.get("constrained_real_submit_enabled")),
            ]
        )

    with delivery_right:
        render_section_header("Calibration gate summary", subtitle="把 calibration impacted market counts 固定放在 system 顶部，而不是埋在底部表格。")
        gate_rows = [
            {"Metric": "Impacted Markets", "Value": status.get("calibration_impacted_market_count", 0)},
            {"Metric": "Hard-Gated Markets", "Value": status.get("calibration_hard_gate_market_count", 0)},
            {"Metric": "Review Required Markets", "Value": status.get("calibration_review_required_market_count", 0)},
            {"Metric": "Research Only Markets", "Value": status.get("calibration_research_only_market_count", 0)},
        ]
        st.dataframe(pd.DataFrame(gate_rows), width="stretch", hide_index=True)

    runtime_tab, evidence_tab, debug_tab = st.tabs(["Runtime delivery", "Evidence & blockers", "Debug paths"])

    with runtime_tab:
        render_section_header("Runtime delivery", subtitle="主叙事切到 persisted runtime summary + surface delivery summary，不再依赖 file-path console。")
        surface_delivery = status.get("surface_delivery_summary")
        if isinstance(surface_delivery, pd.DataFrame) and not surface_delivery.empty:
            st.dataframe(surface_delivery, width="stretch", hide_index=True)
        else:
            render_empty_state("No surface delivery rows", "当前还没有 persisted surface delivery summary rows。")

        render_section_header("Runtime component surface")
        rows = _build_component_rows(status, readiness)
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

        render_section_header("Surface status summary")
        st.dataframe(pd.DataFrame(surface_rows), width="stretch", hide_index=True)

        render_section_header("Minimal health summary")
        health_rows = [
            {"组件": "Latest Surface Refresh", "值": status.get("latest_surface_refresh_status"), "说明": "persisted operator surface refresh seam"},
            {"组件": "Canonical DB", "值": status.get("canonical_db_path"), "说明": "single canonical runtime DB expected by loop / UI / report"},
            {"组件": "Source Split-Brain", "值": status.get("source_split_brain"), "说明": "true means report / UI / runtime are still reading divergent DBs"},
            {"组件": "Degraded Surfaces", "值": status.get("degraded_surface_count"), "说明": "persisted delivery surfaces currently degraded"},
            {"组件": "Read Error Surfaces", "值": status.get("read_error_surface_count"), "说明": "persisted delivery surfaces currently failing"},
            {"组件": "Capability Refresh", "值": status.get("capability_refresh_status"), "说明": "capability refresh job status from real weather chain runtime"},
            {"组件": "Resolution Reconciliation", "值": status.get("resolution_reconciliation_status"), "说明": "resolution reconciliation job status in current loop"},
            {"组件": "Calibration Bootstrap", "值": status.get("calibration_bootstrap_status"), "说明": "bootstrap samples from matured forecasts + archive observations"},
            {"组件": "Calibration Refresh", "值": status.get("calibration_refresh_status"), "说明": "v2 calibration materialization job status"},
            {"组件": "Allocation Preview", "值": status.get("allocation_preview_status"), "说明": "allocation preview explicitly reports skipped when no deployable snapshots exist"},
            {"组件": "Paper Execution", "值": status.get("paper_execution_status"), "说明": "paper execution job status from current loop"},
            {"组件": "Settlement Feedback Closure", "值": status.get("profitability_settlement_feedback_closure_status"), "说明": "成交之后是否已经走到真实结算、realized pnl 和反馈回写"},
            {"组件": "Pending Resolution Tickets", "值": status.get("profitability_pending_resolution_ticket_count"), "说明": "已有成交但仍在等待真实结算验证的票据数"},
            {"组件": "Resolved Tickets", "值": status.get("profitability_resolved_ticket_count"), "说明": "已有真实 settlement verification 的票据数"},
            {"组件": "Realized PnL Rows", "值": status.get("profitability_realized_pnl_row_count"), "说明": "predicted_vs_realized 中已形成真实 realized pnl 的行数"},
            {"组件": "Latest Resolution Markets", "值": status.get("profitability_latest_resolution_market_count"), "说明": "本轮 reconciliation 真正核验到的市场数"},
            {"组件": "Feedback Writeback", "值": status.get("profitability_latest_feedback_writeback_status"), "说明": "execution priors / feedback materialization 最新回写状态"},
            {"组件": "Feedback Materializations", "值": status.get("profitability_latest_feedback_materialization_count"), "说明": "runtime.execution_feedback_materializations 当前累计条数"},
            {"组件": "Latest Triage Run", "值": status.get("triage_latest_run_id"), "说明": "latest persisted opportunity triage invocation/run id"},
            {"组件": "Latest Triage Status", "值": status.get("triage_latest_run_status"), "说明": "latest persisted opportunity triage invocation status"},
            {"组件": "Triage Runtime Status", "值": status.get("triage_runtime_status"), "说明": "current loop triage runtime status, including idle_no_subjects"},
            {"组件": "Resolution Latest Status", "值": status.get("resolution_latest_run_status"), "说明": "resolution agent runtime status or idle_no_subjects"},
            {"组件": "Resolution Subjects", "值": status.get("resolution_subject_count"), "说明": "current proposal subjects visible to resolution review"},
            {"组件": "Triage Advisory Gate", "值": status.get("triage_advisory_gate_status"), "说明": "enabled 仅在 replay evaluation verified 后出现"},
            {"组件": "Latest Triage Evaluation", "值": status.get("triage_latest_evaluation_method"), "说明": "latest persisted triage evaluation method"},
            {"组件": "Triage Last Evaluated", "值": status.get("triage_last_evaluated_at"), "说明": "latest persisted triage evaluation timestamp"},
            {"组件": "Calibration Samples", "值": status.get("calibration_sample_count"), "说明": "persisted calibration samples feeding v2 profiles"},
            {"组件": "Calibration Profiles", "值": status.get("calibration_profile_count"), "说明": "persisted v2 calibration profiles"},
            {"组件": "Calibration Materializations", "值": status.get("calibration_materialization_count"), "说明": "runtime calibration materialization records"},
            {"组件": "Opportunity Surface", "值": status.get("opportunity_row_count"), "说明": "当前机会排序读面行数"},
            {"组件": "Actionable Markets", "值": status.get("actionable_market_count"), "说明": "当前可优先 review 的市场数"},
            {"组件": "Resolution Review Rows", "值": status.get("agent_row_count"), "说明": "Resolution Agent 当前可见 review rows"},
            {"组件": "Triage Rows", "值": status.get("triage_row_count"), "说明": "Opportunity Triage overlay 当前可见 rows"},
            {"组件": "Triage Subjects", "值": status.get("triage_subject_count"), "说明": "triage overlay current persisted subject rows"},
            {"组件": "Triage Failed", "值": status.get("triage_failed_count"), "说明": "timeout / parse_error / failure rows do not affect canonical queue"},
            {"组件": "Triage Accepted", "值": status.get("triage_accepted_count"), "说明": "operator 已接受的 triage overlay rows"},
            {"组件": "Triage Deferred", "值": status.get("triage_deferred_count"), "说明": "operator 已延后的 triage overlay rows"},
            {"组件": "Pending Operator Review", "值": status.get("pending_operator_review_count"), "说明": "建议 hold/manual/dispute 且尚未 operator 接纳的 proposal"},
            {"组件": "Blocked By Operator", "值": status.get("blocked_by_operator_review_count"), "说明": "operator 已明确阻断的 proposal"},
            {"组件": "Ready For Redeem Review", "值": status.get("ready_for_redeem_review_count"), "说明": "operator 已放行到 redeem review 的 proposal"},
            {"组件": "Calibration Freshness", "值": status.get("latest_calibration_freshness_status"), "说明": "最新 calibration profile materialization freshness"},
            {"组件": "Strategy Runs", "值": status.get("strategy_run_count"), "说明": "canonical paper strategy runs"},
            {"组件": "Trade Tickets", "值": status.get("trade_ticket_count"), "说明": "canonical trade tickets"},
            {"组件": "Allocation Decisions", "值": status.get("allocation_decision_count"), "说明": "capital allocation outputs"},
            {"组件": "Paper Orders", "值": status.get("paper_order_count"), "说明": "canonical paper orders"},
            {"组件": "Paper Fills", "值": status.get("fill_count"), "说明": "canonical paper fills"},
            {"组件": "Profitability Path Closed", "值": status.get("profitability_path_closed"), "说明": "only true when signal -> execution -> feedback closure is genuinely non-empty"},
            {"组件": "Execution Closure Status", "值": status.get("profitability_execution_closure_status"), "说明": "separates signal+paper closure from still-missing intelligence surfaces"},
            {"组件": "Intelligence Closure Status", "值": status.get("profitability_intelligence_closure_status"), "说明": "tracks execution intelligence + triage runtime honesty instead of only path_closed"},
            {"组件": "Deployable Signals", "值": status.get("profitability_has_deployable_signals"), "说明": "at least one non-NO_TRADE snapshot selected into deployable set"},
            {"组件": "Empirical Feedback", "值": status.get("profitability_has_empirical_feedback"), "说明": "predicted vs realized rows from current canonical runtime"},
            {"组件": "Active Priors Hit", "值": status.get("profitability_active_market_prior_hit_count"), "说明": "当前活跃机会里真正命中 execution priors 的数量"},
            {"组件": "Deployable Snapshots", "值": status.get("profitability_deployable_snapshot_count"), "说明": "当前轮进入可执行集合的 snapshot 数量"},
            {"组件": "Exec-Intel Covered", "值": status.get("profitability_execution_intelligence_covered_snapshot_count"), "说明": "当前可执行机会里已有 execution intelligence 覆盖的数量"},
            {"组件": "Agent Running Status", "值": status.get("profitability_agent_running_status"), "说明": "只看 triage invocation/output/evaluation 是否真实落库"},
            {"组件": "Agent Value Status", "值": status.get("profitability_agent_value_status"), "说明": "区分真实有效输出与 fallback-only defer"},
            {"组件": "Useful Agent Output", "值": status.get("profitability_agents_have_useful_output"), "说明": "triage/output/evaluation non-empty and not only idle/failure"},
            {"组件": "Latest Non-fallback Triage", "值": status.get("profitability_latest_triage_non_fallback_output_count"), "说明": "最新一轮真正走到 provider 输出而非 fallback 的 triage 数量"},
        ]
        for row in health_rows:
            row["值"] = localize_triage_value(
                {
                    "Settlement Feedback Closure": "profitability_settlement_feedback_closure_status",
                    "Latest Triage Status": "triage_latest_run_status",
                    "Triage Runtime Status": "triage_runtime_status",
                    "Resolution Latest Status": "resolution_latest_run_status",
                    "Triage Advisory Gate": "triage_advisory_gate_status",
                    "Latest Triage Evaluation": "triage_latest_evaluation_method",
                    "Agent Running Status": "profitability_agent_running_status",
                    "Agent Value Status": "profitability_agent_value_status",
                    "Useful Agent Output": "profitability_agents_have_useful_output",
                }.get(str(row["组件"]), ""),
                row["值"],
            )
        st.dataframe(pd.DataFrame(health_rows), width="stretch", hide_index=True)

    with evidence_tab:
        render_section_header("Dependency freshness", subtitle="dependency rows 继续保留，但下沉到 evidence tab。")
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
            render_empty_state("No dependency rows", "当前还没有 readiness evidence dependency rows。")

        render_section_header("Blockers / warnings")
        blocker_rows = [{"type": "blocker", "value": item} for item in (evidence.get("blockers") or [])]
        blocker_rows.extend({"type": "warning", "value": item} for item in (evidence.get("warnings") or []))
        if blocker_rows:
            st.dataframe(pd.DataFrame(blocker_rows), width="stretch", hide_index=True)
        else:
            render_state_card("evidence", "当前 evidence bundle 没有 blockers / warnings。", tone="ok")

        phase_table = readiness["phase_table"]
        if not phase_table.empty:
            with st.expander("Readiness Gate Details", expanded=False):
                st.dataframe(phase_table, width="stretch", hide_index=True)

    with debug_tab:
        render_section_header("Evidence paths", subtitle="文件路径和 debug rows 继续保留，但不占默认主视图叙事。")
        path_rows = [{"路径类型": key, "路径": value} for key, value in (evidence.get("evidence_paths") or {}).items()]
        if path_rows:
            st.dataframe(pd.DataFrame(path_rows), width="stretch", hide_index=True)
        else:
            render_empty_state("No evidence path rows", "当前还没有 evidence path rows。")

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
