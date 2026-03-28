from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from ui.components import (
    render_delivery_badge,
    render_detail_key_value,
    render_empty_state,
    render_kpi_band,
    render_page_intro,
    render_reason_chip_row,
    render_section_header,
    render_state_card,
)
from ui.data_access import load_home_decision_snapshot
from ui.triage_localization import localize_reason_codes, localize_triage_frame, localize_triage_value


def _format_metric_value(value: object) -> str:
    if value is None or value == "" or pd.isna(value):
        return "N/A"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def _json_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    if isinstance(value, str) and value == "":
        return {}
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except Exception:
            pass
    try:
        parsed = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if value is None:
        return []
    if isinstance(value, str) and value == "":
        return []
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, dict)):
        try:
            converted = value.tolist()
        except Exception:
            converted = None
        if isinstance(converted, list):
            return converted
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except Exception:
            pass
    try:
        parsed = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    return parsed if isinstance(parsed, list) else []


def _allocation_overlay_summary(row: dict[str, object]) -> list[str]:
    budget_impact = _json_dict(row.get("budget_impact"))
    preview_budget = _json_dict(budget_impact.get("preview"))
    rerank_reason_codes = _json_list(row.get("rerank_reason_codes_json")) or _json_list(budget_impact.get("rerank_reason_codes"))
    requested_size = row.get("requested_size")
    if requested_size in {None, ""}:
        requested_size = preview_budget.get("requested_size")
    return [
        f"base_score={_format_metric_value(row.get('base_ranking_score'))}",
        f"pre_budget_pnl={_format_metric_value(row.get('pre_budget_deployable_expected_pnl'))}",
        f"final_score={_format_metric_value(row.get('ranking_score'))}",
        f"calibration_gate={row.get('calibration_gate_status') or 'clear'}",
        f"delivery={row.get('surface_delivery_status') or 'ok'}",
        f"preview_limit={preview_budget.get('preview_binding_limit_scope') or row.get('preview_binding_limit_scope') or 'none'}",
        f"final_limit={row.get('binding_limit_scope') or budget_impact.get('binding_limit_scope') or 'none'}",
        f"rerank={', '.join(str(item) for item in rerank_reason_codes) or 'none'}",
        f"size={_format_metric_value(requested_size)} -> {_format_metric_value(row.get('recommended_size'))}",
    ]


def show() -> None:
    overview = load_home_decision_snapshot()
    surface_status = overview.get("surface_status", {})
    readiness = overview.get("readiness", {})
    execution = overview.get("execution", {"exceptions": pd.DataFrame()})
    market_data = overview.get("market_data", {})
    metrics = overview.get("metrics", {})
    wallet_attention = overview.get("wallet_attention", pd.DataFrame())
    top_opportunities = overview.get("top_opportunities", pd.DataFrame())
    action_queue = overview.get("action_queue", pd.DataFrame())
    blocked_backlog = overview.get("blocked_backlog", pd.DataFrame())
    largest_blocker = overview.get("largest_blocker", {"source": "unknown", "summary": "unknown"})
    recent_agent = overview.get("recent_agent_summary", {})
    agent_data = overview.get("agent_data", {}).get("frame", pd.DataFrame())
    triage_data = overview.get("triage_data", {}).get("frame", pd.DataFrame())
    evidence = overview.get("readiness_evidence", {})
    predicted_vs_realized = overview.get("predicted_vs_realized_snapshot", pd.DataFrame())
    degraded_inputs = overview.get("degraded_inputs", [])
    uncaptured_high_edge = overview.get("uncaptured_high_edge_markets", pd.DataFrame())
    localized_action_queue = localize_triage_frame(action_queue) if not action_queue.empty else action_queue
    localized_top_opportunities = localize_triage_frame(top_opportunities) if not top_opportunities.empty else top_opportunities
    localized_triage_data = localize_triage_frame(triage_data) if not triage_data.empty else triage_data

    render_page_intro(
        "Decision Console",
        "首页优先回答 readiness decision、最大 blocker、当前最佳机会，以及 execution-path evidence；agent 产出只作为 exception-review evidence，不作为主排序输入。",
        kicker="Operator research desk",
        badges=[
            ("v2.0 implementation active", "ok"),
            ("constrained execution boundary", "info"),
        ],
    )

    degraded_surfaces = [
        f"{name}: {payload['label']}"
        for name, payload in surface_status.items()
        if name != "overall" and payload["status"] in {"read_error", "refresh_in_progress", "degraded_source", "no_data"}
    ]
    if degraded_surfaces:
        st.warning("当前关键数据面状态: " + " | ".join(degraded_surfaces))

    render_kpi_band(
        [
            {"label": "Readiness Decision", "value": metrics.get("go_decision", "UNKNOWN"), "delta": readiness.get("target") or "p4_live_prerequisites"},
            {"label": "Actionable Markets", "value": metrics.get("actionable_market_count", 0), "delta": f"open={metrics.get('weather_market_count', 0)}"},
            {"label": "Ranking Score", "value": _format_metric_value(metrics.get("top_ranking_score", metrics.get("top_opportunity_score"))), "delta": "primary score"},
            {"label": "Largest Current Blocker", "value": largest_blocker["source"], "delta": largest_blocker["summary"]},
        ]
    )

    row1_left, row1_right = st.columns([1.2, 1.05])
    with row1_left:
        render_section_header("Readiness Decision", subtitle="先回答当前是否 ready for controlled rollout decision，而不是先下沉到诊断表。")
        render_state_card(
            "decision",
            readiness.get("decision_reason") or "尚未生成 P4 readiness 报告。",
            tone="info",
            meta=readiness.get("target") or "p4_live_prerequisites",
        )
        failed_gate_names = readiness.get("failed_gate_names") or []
        if failed_gate_names:
            st.error("当前 blocker: " + " / ".join(failed_gate_names))
        else:
            render_state_card("gate status", "当前没有 gate-level blocker。", tone="ok")
        st.caption(
            "当前仓库状态是 `P4 accepted; post-P4 remediation accepted; v2.0 implementation active`，"
            "当前系统定位是 `operator console + constrained execution infra`，这不表示 unattended live。"
        )

    with row1_right:
        render_section_header("Largest Current Blocker", subtitle="把当前最限制 operator 动作的约束放到主视野，而不是埋在 evidence rows 里。")
        if largest_blocker["source"] == "clear":
            render_state_card("blocker", "No material blocker", tone="ok")
        else:
            render_state_card("blocker", largest_blocker["summary"], tone="warn", meta=f"来源: {largest_blocker['source']}")
        if largest_blocker["source"] == "clear":
            st.caption(f"来源: {largest_blocker['source']}")
        st.metric("Liquidity-Ready", metrics.get("liquidity_ready_count", 0), delta=f"highest_edge={_format_metric_value(metrics.get('highest_edge_bps'))}bps")

    row2_left, row2_right = st.columns([1.35, 1])
    with row2_left:
        render_section_header("Top opportunities", subtitle="默认先看最值得 review 的机会，而不是完整研究明细。")
        if top_opportunities.empty:
            render_empty_state("No ranked markets", "当前还没有进入机会排序的市场。", tone="muted")
        else:
            columns = [
                column
                for column in [
                    "location_name",
                    "question",
                    "best_side",
                    "edge_bps",
                    "edge_bps_model",
                    "ranking_score",
                    "base_ranking_score",
                    "expected_dollar_pnl",
                    "deployable_expected_pnl",
                    "deployable_notional",
                    "max_deployable_size",
                    "capture_probability",
                    "execution_intelligence_score",
                    "spread_regime",
                    "expected_capture_regime",
                    "recommended_size",
                    "allocation_status",
                    "surface_delivery_status",
                    "surface_fallback_origin",
                    "surface_last_refresh_ts",
                    "source_badge",
                    "source_truth_status",
                    "mapping_confidence",
                    "source_freshness_status",
                    "market_quality_status",
                    "agent_review_status",
                    "actionability_status",
                ]
                if column in top_opportunities.columns
            ]
            st.dataframe(top_opportunities[columns].head(5), width="stretch", hide_index=True)
            top_row = top_opportunities.iloc[0].to_dict()
            why_ranked = _json_dict(top_row.get("why_ranked_json"))
            if why_ranked:
                st.caption(
                    "why-ranked: "
                    f"mode={why_ranked.get('mode')}, "
                    f"capture={_format_metric_value(why_ranked.get('capture_probability'))}, "
                    f"ev={_format_metric_value(why_ranked.get('expected_dollar_pnl'))}, "
                    f"risk={_format_metric_value(why_ranked.get('risk_penalty'))}, "
                    f"micro={_format_metric_value(why_ranked.get('execution_intelligence_score'))}, "
                    f"feedback={_format_metric_value(why_ranked.get('feedback_penalty'))}"
                )
            st.caption("deployable: " + " | ".join(_allocation_overlay_summary(top_row)))

    with row2_right:
        render_section_header("Predicted vs realized", subtitle="保留 execution-path evidence，但把它放在 top opportunity 旁边作为质量旁证。")
        st.metric("Resolved Trades", metrics.get("resolved_trade_count", 0), delta=f"pending={metrics.get('pending_resolution_count', 0)}")
        st.write(f"avg predicted edge: `{_format_metric_value(metrics.get('avg_predicted_edge_bps'))} bps`")
        st.write(f"avg realized pnl: `{_format_metric_value(metrics.get('avg_realized_pnl'))}`")
        if predicted_vs_realized.empty:
            render_empty_state("No execution evidence", "当前还没有 execution-path evidence rows。")
        else:
            columns = [
                column
                for column in [
                    "ticket_id",
                    "market_id",
                    "predicted_edge_bps",
                    "realized_pnl",
                    "evaluation_status",
                    "source_disagreement",
                ]
                if column in predicted_vs_realized.columns
            ]
            st.dataframe(predicted_vs_realized[columns], width="stretch", hide_index=True)

    row3_left, row3_right = st.columns([1.1, 1.1])
    with row3_left:
        render_section_header("Market coverage", subtitle="用城市和市场覆盖回答研究面是否足够宽，而不是先下沉到链路细节。")
        weather_locations = metrics.get("weather_locations") or []
        st.metric("Cities Covered", len(weather_locations), delta=f"source={(market_data.get('weather_smoke_report') or {}).get('market_discovery', {}).get('market_source') or market_data.get('market_opportunity_source')}")
        if weather_locations:
            st.caption(" / ".join(weather_locations[:12]))
        else:
            st.caption("当前没有命中的开盘近期天气市场。")

    with row3_right:
        render_section_header("Edge capture", subtitle="这里展示的是 capture 现实，而不是对 execution certainty 的承诺。")
        st.write(f"submission: `{_format_metric_value(metrics.get('submission_capture_ratio'))}`")
        st.write(f"fill: `{_format_metric_value(metrics.get('fill_capture_ratio'))}`")
        st.write(f"resolution: `{_format_metric_value(metrics.get('resolution_capture_ratio'))}`")
        st.caption(f"uncaptured={metrics.get('uncaptured_high_edge_count', 0)}")
        if uncaptured_high_edge.empty:
            render_state_card("capture", "当前没有 uncaptured high-edge markets。", tone="ok")
        else:
            columns = [
                column
                for column in [
                    "market_id",
                    "avg_executable_edge_bps",
                    "submission_capture_ratio",
                    "fill_capture_ratio",
                    "resolution_capture_ratio",
                    "source_badge",
                    "miss_reason_bucket",
                    "distortion_reason_bucket",
                ]
                if column in uncaptured_high_edge.columns
            ]
            st.dataframe(uncaptured_high_edge[columns].head(5), width="stretch", hide_index=True)

    row3b_left, row3b_right = st.columns([1.2, 1.0])
    with row3b_left:
        render_section_header("Action queue", subtitle="按 deployable value 和 operator bucket 组织默认注意力，而不是只显示 persisted workflow rows。")
        if action_queue.empty:
            render_empty_state("No queued actions", "当前没有进入 operator action queue 的 persisted rows。")
        else:
            bucket_counts = (
                action_queue["operator_bucket"].value_counts().to_dict()
                if "operator_bucket" in action_queue.columns
                else {}
            )
            st.caption(
                " | ".join(
                    [
                        f"ready_now={bucket_counts.get('ready_now', 0)}",
                        f"high_risk={bucket_counts.get('high_risk', 0)}",
                        f"review_required={bucket_counts.get('review_required', 0)}",
                    ]
                )
            )
            columns = [
                column
                for column in [
                    "operator_bucket",
                    "location_name",
                    "question",
                    "best_side",
                    "ranking_score",
                    "base_ranking_score",
                    "pre_budget_deployable_expected_pnl",
                    "deployable_expected_pnl",
                    "calibration_gate_status",
                    "surface_delivery_status",
                    "surface_fallback_origin",
                    "surface_last_refresh_ts",
                    "priority_band",
                    "recommended_operator_action",
                    "effective_triage_status",
                    "requested_size",
                    "recommended_size",
                    "preview_binding_limit_scope",
                    "allocation_status",
                    "binding_limit_scope",
                    "rerank_position",
                    "actionability_status",
                    "queue_reason_codes_json",
                ]
                if column in action_queue.columns
            ]
            st.dataframe(
                localized_action_queue[columns]
                .head(10)
                .rename(
                    columns={
                        "priority_band": "分诊优先级",
                        "recommended_operator_action": "分诊建议动作",
                        "effective_triage_status": "分诊状态",
                        "advisory_gate_status": "分诊门控",
                    }
                ),
                width="stretch",
                hide_index=True,
            )
    with row3b_right:
        render_section_header("Allocation overlay", subtitle="这里解释推荐 size、当前 gate 与 capital policy，不替代主排序。")
        render_detail_key_value(
            [
                ("Queued Actions", metrics.get("action_queue_count", 0)),
                ("ready_now", _format_metric_value(metrics.get("ready_now_count"))),
                ("high_risk", _format_metric_value(metrics.get("high_risk_count"))),
                ("review_required", _format_metric_value(metrics.get("review_required_count"))),
                ("blocked", _format_metric_value(metrics.get("blocked_count"))),
                ("research_only", _format_metric_value(metrics.get("research_only_count"))),
                ("triage_rows", _format_metric_value(len(localized_triage_data.index))),
                ("triage_enabled", _format_metric_value((triage_data["advisory_gate_status"] == "enabled").sum() if ("advisory_gate_status" in triage_data.columns and not triage_data.empty) else 0)),
                ("triage_experimental", _format_metric_value((triage_data["advisory_gate_status"] == "experimental").sum() if ("advisory_gate_status" in triage_data.columns and not triage_data.empty) else 0)),
            ]
        )
        if not top_opportunities.empty:
            top_row = localized_top_opportunities.iloc[0].to_dict()
            render_detail_key_value(
                [
                    ("recommended_size", _format_metric_value(top_row.get("recommended_size"))),
                    ("allocation_status", _format_metric_value(top_row.get("allocation_status"))),
                    ("deployable_expected_pnl", _format_metric_value(top_row.get("deployable_expected_pnl"))),
                    ("max_deployable_size", _format_metric_value(top_row.get("max_deployable_size"))),
                    ("base_ranking_score", _format_metric_value(top_row.get("base_ranking_score"))),
                    ("pre_budget_deployable_expected_pnl", _format_metric_value(top_row.get("pre_budget_deployable_expected_pnl"))),
                    ("calibration_gate_status", _format_metric_value(top_row.get("calibration_gate_status"))),
                    ("capital_policy_id", _format_metric_value(top_row.get("capital_policy_id"))),
                    ("execution_intelligence_score", _format_metric_value(top_row.get("execution_intelligence_score"))),
                    ("triage_advisory_gate_status", _format_metric_value(top_row.get("advisory_gate_status"))),
                    ("spread_regime", _format_metric_value(top_row.get("spread_regime"))),
                    ("expected_capture_regime", _format_metric_value(top_row.get("expected_capture_regime"))),
                    ("requested_size", _format_metric_value(top_row.get("requested_size") or _json_dict(_json_dict(top_row.get("budget_impact")).get("preview")).get("requested_size"))),
                ]
            )
            budget_impact = _json_dict(top_row.get("budget_impact"))
            if budget_impact:
                preview_budget = _json_dict(budget_impact.get("preview"))
                rerank_reason_codes = _json_list(top_row.get("rerank_reason_codes_json")) or _json_list(budget_impact.get("rerank_reason_codes"))
                scaling_reason_codes = _json_list(top_row.get("capital_scaling_reason_codes_json")) or _json_list(budget_impact.get("capital_scaling_reason_codes"))
                st.caption(
                    " | ".join(
                        [
                            f"preview_binding_limit={preview_budget.get('preview_binding_limit_scope') or top_row.get('preview_binding_limit_scope') or 'none'}",
                            f"preview_binding_key={preview_budget.get('preview_binding_limit_key') or top_row.get('preview_binding_limit_key') or 'none'}",
                            f"binding_limit={budget_impact.get('binding_limit_scope') or 'none'}",
                            f"binding_key={budget_impact.get('binding_limit_key') or 'none'}",
                            f"remaining_budget={_format_metric_value(budget_impact.get('remaining_run_budget'))}",
                        ]
                    )
                )
                render_reason_chip_row(rerank_reason_codes, empty_label="rerank:none")
                render_reason_chip_row(scaling_reason_codes, empty_label="scaling:none")

    render_section_header("Blocked backlog", subtitle="阻断项保留在首页，但下沉成次级 backlog，不污染主 action queue。")
    if blocked_backlog.empty:
        render_state_card("blocked backlog", "当前没有 blocked rows。", tone="ok")
    else:
        columns = [
            column
            for column in [
                "location_name",
                "question",
                "best_side",
                "ranking_score",
                "allocation_status",
                "calibration_gate_status",
                "surface_delivery_status",
                "binding_limit_scope",
                "queue_reason_codes_json",
                "updated_at",
            ]
            if column in blocked_backlog.columns
        ]
        st.dataframe(blocked_backlog[columns].head(10), width="stretch", hide_index=True)

    row4_left, row4_right = st.columns([1.1, 1.1])
    with row4_left:
        render_section_header("Wallet & execution attention", subtitle="聚合 wallet 与 live-prereq exceptions，但不制造新的 execution gate。")
        execution_exceptions = execution["exceptions"]
        if wallet_attention.empty and execution_exceptions.empty:
            render_state_card("attention", "当前没有 wallet / execution attention rows。", tone="ok")
        else:
            frames = []
            if not wallet_attention.empty:
                frames.append(
                    wallet_attention[[column for column in ["wallet_id", "wallet_readiness_status", "latest_chain_tx_status"] if column in wallet_attention.columns]].head(4)
                )
            if not execution_exceptions.empty:
                frames.append(
                    execution_exceptions[[column for column in ["ticket_id", "live_prereq_execution_status", "external_reconciliation_status"] if column in execution_exceptions.columns]].head(4)
                )
            combined = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
            st.dataframe(combined, width="stretch", hide_index=True)

    with row4_right:
        render_section_header("Readiness evidence", subtitle="把 evidence bundle 放在首屏可见区，但不让 file-path 细节抢走主叙事。")
        st.metric("Evidence Bundle", "READY" if evidence.get("exists") else "MISSING", delta=evidence.get("capability_manifest_status") or "missing")
        if evidence.get("blockers"):
            st.error(" / ".join(str(item) for item in evidence.get("blockers")[:4]))
        elif evidence.get("warnings"):
            st.warning(" / ".join(str(item) for item in evidence.get("warnings")[:4]))
        else:
            render_state_card("evidence", evidence.get("decision_reason") or "当前 evidence bundle 无 blocker。", tone="ok")

    render_section_header("Degraded inputs", subtitle="这里显式列出降级输入，避免 fallback 被误看成 canonical 正常态。")
    if degraded_inputs:
        render_reason_chip_row(degraded_inputs[:8], empty_label="none")
    else:
        render_state_card("inputs", "当前没有 degraded input summary。", tone="ok")

    render_section_header("Recent agent work", subtitle="这里展示的是 exception-review evidence，不是 execution driver，也不进入 readiness gate。")
    st.metric("Agent Rows", metrics.get("agent_activity_count", 0), delta=f"review_required={metrics.get('agent_review_required_count', 0)}")
    if recent_agent.get("agent_type"):
        render_detail_key_value(
            [
                ("最新 agent", recent_agent["agent_type"]),
                ("verdict", recent_agent.get("verdict") or "n/a"),
                ("summary", recent_agent.get("summary") or "最近一次 agent 产出暂无摘要。"),
            ]
        )
    else:
        render_empty_state("No recent agent work", "当前没有 agent activity；运行 weather smoke 后会在这里显示最新产出。")
    if not agent_data.empty:
        columns = [
            column
            for column in [
                "agent_type",
                "subject_id",
                "invocation_status",
                "verdict",
                "summary",
                "updated_at",
            ]
            if column in agent_data.columns
        ]
        st.dataframe(agent_data[columns].head(6), width="stretch", hide_index=True)
