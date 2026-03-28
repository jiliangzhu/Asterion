from __future__ import annotations

from typing import Any

import pandas as pd

from ui.loaders.execution_loader import load_execution_console_data
from ui.loaders.markets_loader import load_market_chain_analysis_data
from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def build_ops_console_overview() -> dict[str, Any]:
    from ui import data_access as compat

    readiness = compat.load_readiness_summary()
    evidence = compat.load_readiness_evidence_bundle()
    execution = load_execution_console_data()
    wallets = compat.load_wallet_readiness_data()
    market_watch_data = compat.load_market_watch_data()
    market_analysis = load_market_chain_analysis_data()
    agent_data = compat.load_agent_review_data()
    triage_data = compat.load_opportunity_triage_data()
    predicted_vs_realized = compat.load_predicted_vs_realized_data()["frame"]
    watch_only_vs_executed = execution["watch_only_vs_executed"]
    execution_science = execution["execution_science"]
    calibration_health = execution["calibration_health"]

    live_execution = execution["live_prereq"]
    exceptions = execution["exceptions"]
    weather_report = market_watch_data["weather_smoke_report"] or {}
    opportunities = market_analysis["market_opportunities"]
    action_queue_full = compat.load_ui_lite_snapshot()["tables"]["action_queue_summary"]
    if not action_queue_full.empty:
        action_queue_full = action_queue_full.sort_values(
            by=["queue_priority", "ranking_score", "updated_at"],
            ascending=[True, False, False],
            na_position="last",
        ).reset_index(drop=True)
        if "surface_delivery_status" in action_queue_full.columns:
            action_queue_full = action_queue_full[
                action_queue_full["surface_delivery_status"].fillna("ok").isin(["ok", "degraded_source", "stale", "read_error", "missing"])
            ]
    wallet_attention = (
        wallets[wallets["attention_required"] == True]  # noqa: E712
        if "attention_required" in wallets.columns
        else wallets.iloc[0:0]
    )
    actionable = (
        opportunities[opportunities["actionability_status"] == "actionable"]
        if "actionability_status" in opportunities.columns
        else opportunities.iloc[0:0]
    )
    triage_frame = triage_data["frame"]
    triage_overlay = (
        triage_frame[
            [
                column
                for column in [
                    "market_id",
                    "priority_band",
                    "recommended_operator_action",
                    "effective_triage_status",
                    "latest_agent_status",
                    "advisory_gate_status",
                ]
                if column in triage_frame.columns
            ]
        ]
        if (not triage_frame.empty and "market_id" in triage_frame.columns)
        else triage_frame.iloc[0:0]
    )
    home_triage_overlay = triage_overlay
    if not home_triage_overlay.empty and "advisory_gate_status" in home_triage_overlay.columns:
        home_triage_overlay = home_triage_overlay[home_triage_overlay["advisory_gate_status"].fillna("experimental") == "enabled"]
    if not triage_overlay.empty and not opportunities.empty and "market_id" in opportunities.columns:
        opportunities = opportunities.merge(triage_overlay, on="market_id", how="left")
        actionable = (
            opportunities[opportunities["actionability_status"] == "actionable"]
            if "actionability_status" in opportunities.columns
            else opportunities.iloc[0:0]
        )
    top_opportunities = actionable.head(5) if not actionable.empty else opportunities.head(5)
    blocked_backlog = action_queue_full.iloc[0:0]
    if "operator_bucket" in action_queue_full.columns and not action_queue_full.empty:
        if not home_triage_overlay.empty and "market_id" in action_queue_full.columns:
            action_queue_full = action_queue_full.merge(home_triage_overlay, on="market_id", how="left")
        action_queue = action_queue_full[
            action_queue_full["operator_bucket"].isin(["ready_now", "high_risk", "review_required"])
        ].head(10)
        blocked_backlog = action_queue_full[action_queue_full["operator_bucket"] == "blocked"].head(10)
    else:
        action_queue = action_queue_full.head(10)
    resolved_rows = (
        predicted_vs_realized[predicted_vs_realized["evaluation_status"] == "resolved"]
        if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty)
        else predicted_vs_realized.iloc[0:0]
    )
    uncaptured_high_edge = watch_only_vs_executed.iloc[0:0]
    if not watch_only_vs_executed.empty:
        uncaptured_high_edge = watch_only_vs_executed[
            (pd.to_numeric(watch_only_vs_executed["avg_executable_edge_bps"], errors="coerce").fillna(0) > 0)
            & (pd.to_numeric(watch_only_vs_executed["submission_capture_ratio"], errors="coerce").fillna(0) <= 0)
        ]
    total_opportunities = float(pd.to_numeric(watch_only_vs_executed["opportunity_count"], errors="coerce").fillna(0).sum()) if ("opportunity_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    total_submitted = float(pd.to_numeric(watch_only_vs_executed["submitted_ticket_count"], errors="coerce").fillna(0).sum()) if ("submitted_ticket_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    total_filled = float(pd.to_numeric(watch_only_vs_executed["filled_ticket_count"], errors="coerce").fillna(0).sum()) if ("filled_ticket_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    total_resolved = float(pd.to_numeric(watch_only_vs_executed["resolved_ticket_count"], errors="coerce").fillna(0).sum()) if ("resolved_ticket_count" in watch_only_vs_executed.columns and not watch_only_vs_executed.empty) else 0.0
    submission_capture_ratio = (total_submitted / total_opportunities) if total_opportunities > 0 else 0.0
    fill_capture_ratio = (total_filled / total_opportunities) if total_opportunities > 0 else 0.0
    resolution_capture_ratio = (total_resolved / total_opportunities) if total_opportunities > 0 else 0.0
    degraded_inputs: list[str] = []
    if evidence.get("stale_dependencies"):
        degraded_inputs.extend([f"stale:{item}" for item in evidence.get("stale_dependencies") or []])
    if market_analysis.get("market_opportunity_source") in {"smoke_report", "weather_smoke_db"}:
        degraded_inputs.append(f"market_source:{market_analysis.get('market_opportunity_source')}")
    if evidence.get("capability_manifest_status") not in {None, "valid"}:
        degraded_inputs.append(f"manifest:{evidence.get('capability_manifest_status') or 'missing'}")
    if readiness.get("failed_gate_names"):
        largest_blocker = " / ".join(readiness.get("failed_gate_names") or [])
        blocker_source = "readiness"
    elif evidence.get("blockers"):
        largest_blocker = " / ".join(str(item) for item in evidence.get("blockers") or [])
        blocker_source = "evidence"
    elif not wallet_attention.empty and "wallet_readiness_status" in wallet_attention.columns:
        largest_blocker = compat._ensure_text(wallet_attention.iloc[0].get("wallet_readiness_status")) or "wallet blocker"
        blocker_source = "wallet"
    elif not exceptions.empty:
        largest_blocker = (
            compat._ensure_text(exceptions.iloc[0].get("live_prereq_execution_status"))
            or compat._ensure_text(exceptions.iloc[0].get("execution_result"))
            or "execution attention required"
        )
        blocker_source = "execution"
    else:
        largest_blocker = "No material blocker"
        blocker_source = "clear"

    bucket_counts = {}
    if not action_queue_full.empty and "operator_bucket" in action_queue_full.columns:
        bucket_counts = {str(key): int(value) for key, value in action_queue_full["operator_bucket"].value_counts(dropna=False).items()}

    return {
        "readiness": readiness,
        "execution": execution,
        "wallets": wallets,
        "market_data": market_analysis,
        "market_watch_data": market_watch_data,
        "agent_data": agent_data,
        "triage_data": triage_data,
        "readiness_evidence": evidence,
        "predicted_vs_realized": predicted_vs_realized,
        "watch_only_vs_executed_summary": watch_only_vs_executed,
        "execution_science_summary": execution_science,
        "calibration_health_summary": calibration_health,
        "uncaptured_high_edge_markets": uncaptured_high_edge,
        "surface_status": compat.load_operator_surface_status(),
        "surface_delivery_summary": compat.load_surface_delivery_summary(),
        "boundary_sidebar_summary": compat.load_boundary_sidebar_truth(),
        "top_opportunities": top_opportunities,
        "action_queue": action_queue,
        "action_queue_full": action_queue_full,
        "blocked_backlog": blocked_backlog,
        "degraded_inputs": degraded_inputs,
        "largest_blocker": {"summary": largest_blocker, "source": blocker_source},
        "metrics": {
            "go_decision": readiness.get("go_decision") or "UNKNOWN",
            "failed_gate_count": len(readiness.get("failed_gate_names") or []),
            "wallet_ready_count": int((wallets["wallet_readiness_status"] == "ready").sum()) if "wallet_readiness_status" in wallets.columns else 0,
            "wallet_total_count": int(len(wallets.index)),
            "live_prereq_attention_count": int((live_execution["live_prereq_attention_required"] == True).sum()) if "live_prereq_attention_required" in live_execution.columns else 0,  # noqa: E712
            "exception_count": int(len(exceptions.index)),
            "weather_chain_status": weather_report.get("chain_status") or "unknown",
            "weather_market_question": ((weather_report.get("market_discovery") or {}).get("question") or "未发现实时市场"),
            "weather_market_count": int(len(opportunities.index)),
            "actionable_market_count": int(len(actionable.index)),
            "action_queue_count": int(len(action_queue.index)),
            "ready_now_count": int(bucket_counts.get("ready_now", 0)),
            "high_risk_count": int(bucket_counts.get("high_risk", 0)),
            "review_required_count": int(bucket_counts.get("review_required", 0)),
            "blocked_count": int(bucket_counts.get("blocked", 0)),
            "research_only_count": int(bucket_counts.get("research_only", 0)),
            "top_ranking_score": float(top_opportunities.iloc[0]["ranking_score"]) if (not top_opportunities.empty and "ranking_score" in top_opportunities.columns) else 0.0,
            "top_opportunity_score": float(top_opportunities.iloc[0]["ranking_score"]) if (not top_opportunities.empty and "ranking_score" in top_opportunities.columns) else 0.0,
            "highest_edge_bps": float(pd.to_numeric(opportunities["edge_bps"], errors="coerce").abs().max()) if ("edge_bps" in opportunities.columns and not opportunities.empty) else 0.0,
            "liquidity_ready_count": int(((pd.to_numeric(opportunities["liquidity_proxy"], errors="coerce").fillna(0) >= 60.0) & (opportunities["accepting_orders"] == True)).sum()) if ({"liquidity_proxy", "accepting_orders"} <= set(opportunities.columns)) else 0,  # noqa: E712
            "weather_locations": sorted({str(value) for value in opportunities["location_name"].dropna().tolist()}) if "location_name" in opportunities.columns else [],
            "agent_activity_count": int(len(agent_data["frame"].index)),
            "agent_review_required_count": int((agent_data["frame"]["human_review_required"] == True).sum()) if ("human_review_required" in agent_data["frame"].columns and not agent_data["frame"].empty) else 0,  # noqa: E712
            "predicted_vs_realized_count": int(len(predicted_vs_realized.index)),
            "resolved_trade_count": int(len(resolved_rows.index)),
            "pending_resolution_count": int((predicted_vs_realized["evaluation_status"] == "pending_resolution").sum()) if ("evaluation_status" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0,
            "avg_predicted_edge_bps": float(pd.to_numeric(predicted_vs_realized["predicted_edge_bps"], errors="coerce").dropna().mean()) if ("predicted_edge_bps" in predicted_vs_realized.columns and not predicted_vs_realized.empty) else 0.0,
            "avg_realized_pnl": float(pd.to_numeric(resolved_rows["realized_pnl"], errors="coerce").dropna().mean()) if ("realized_pnl" in resolved_rows.columns and not resolved_rows.empty) else 0.0,
            "submission_capture_ratio": submission_capture_ratio,
            "fill_capture_ratio": fill_capture_ratio,
            "resolution_capture_ratio": resolution_capture_ratio,
            "execution_capture_ratio": fill_capture_ratio,
            "uncaptured_high_edge_count": int(len(uncaptured_high_edge.index)),
            "primary_score_label": compat.load_primary_score_descriptor().primary_score_label,
        },
        "wallet_attention": wallet_attention,
    }


def load_home_decision_snapshot() -> dict[str, Any]:
    overview = build_ops_console_overview()
    agent_payload = overview["agent_data"]
    agent_frame = agent_payload["frame"]
    resolution_frame = (
        agent_frame[agent_frame["agent_type"] == "resolution"]
        if (
            agent_payload.get("source") == "ui_lite"
            and "agent_type" in agent_frame.columns
            and not agent_frame.empty
        )
        else agent_frame.iloc[0:0]
    )
    top_agent_row = resolution_frame.iloc[0].to_dict() if not resolution_frame.empty else {}
    return {
        **overview,
        "recent_agent_summary": {
            "agent_type": top_agent_row.get("agent_type"),
            "verdict": top_agent_row.get("verdict"),
            "summary": top_agent_row.get("summary"),
            "updated_at": top_agent_row.get("updated_at"),
        },
        "predicted_vs_realized_snapshot": overview["predicted_vs_realized"].head(5),
    }


def load_home_surface_contract() -> SurfaceLoaderContract:
    payload = load_home_decision_snapshot()
    contract = SurfaceLoaderContract(
        surface_id="home",
        primary_dataframe_name="top_opportunities",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="home",
            primary_table="ui.market_opportunity_summary",
            source=(payload.get("market_data") or {}).get("market_opportunity_source") or "missing",
            supports_source_badges=True,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
