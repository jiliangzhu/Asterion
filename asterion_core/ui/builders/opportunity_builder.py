from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pandas as pd

from asterion_core.ui.surface_truth_shared import annotate_frame_with_source_truth, ensure_primary_score_fields


BUILDER_NAME = "opportunity_builder"
TABLES = (
    "ui.market_watch_summary",
    "ui.market_opportunity_summary",
    "ui.calibration_health_summary",
    "ui.action_queue_summary",
)


def build_opportunity_tables(
    con,
    *,
    table_row_counts: dict[str, int],
    create_market_watch_summary: Callable[[], None],
    create_market_opportunity_summary: Callable[[], None],
    create_calibration_health_summary: Callable[[], None],
) -> None:
    create_market_watch_summary()
    create_market_opportunity_summary()
    create_calibration_health_summary()
    _create_action_queue_summary(con, table_row_counts=table_row_counts)


def _create_action_queue_summary(con, *, table_row_counts: dict[str, int]) -> None:
    if not _table_exists(con, "ui.market_opportunity_summary"):
        _create_empty_action_queue_summary(con, table_row_counts=table_row_counts)
        return

    base = con.execute("SELECT * FROM ui.market_opportunity_summary").df()
    if base.empty:
        _create_empty_action_queue_summary(con, table_row_counts=table_row_counts)
        return

    allocation_by_market: dict[str, dict[str, Any]] = {}
    if _table_exists(con, "src.runtime.allocation_decisions"):
        allocation_frame = con.execute(
            """
            SELECT * FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY market_id
                        ORDER BY created_at DESC, allocation_decision_id DESC
                    ) AS rn
                FROM src.runtime.allocation_decisions
            )
            WHERE rn = 1
            """
        ).df()
        allocation_by_market = {
            str(row["market_id"]): row.to_dict()
            for _, row in allocation_frame.iterrows()
            if row.get("market_id") is not None
        }

    watch_only_by_market = _table_dict_by_key(con, "ui.watch_only_vs_executed_summary", "market_id")
    retrospective_by_market = _load_latest_retrospective_rows(con)

    rows: list[dict[str, Any]] = []
    for _, row in base.iterrows():
        market_id = str(row.get("market_id") or "")
        allocation = allocation_by_market.get(market_id) or {}
        watch_only = watch_only_by_market.get(market_id) or {}
        retrospective = retrospective_by_market.get(market_id) or {}

        allocation_status = str(
            allocation.get("allocation_status")
            or row.get("allocation_status")
            or ""
        )
        actionability_status = str(row.get("actionability_status") or "")
        agent_review_status = str(row.get("agent_review_status") or "")
        market_quality_status = str(row.get("market_quality_status") or "")
        calibration_freshness_status = str(row.get("calibration_freshness_status") or "")
        calibration_gate_status = str(row.get("calibration_gate_status") or "")
        source_freshness_status = str(row.get("source_freshness_status") or "")
        feedback_status = str(row.get("feedback_status") or watch_only.get("feedback_status") or "")
        source_truth_status = str(row.get("source_truth_status") or "")
        live_prereq_status = str(row.get("live_prereq_status") or "")

        operator_bucket, queue_priority, reason_codes = _classify_operator_bucket(
            actionability_status=actionability_status,
            allocation_status=allocation_status,
            agent_review_status=agent_review_status,
            market_quality_status=market_quality_status,
            calibration_freshness_status=calibration_freshness_status,
            calibration_gate_status=calibration_gate_status,
            source_freshness_status=source_freshness_status,
            feedback_status=feedback_status,
            source_truth_status=source_truth_status,
            live_prereq_status=live_prereq_status,
        )

        allocation_reason_codes = _json_list(allocation.get("reason_codes_json"))
        for code in allocation_reason_codes:
            if code not in reason_codes:
                reason_codes.append(code)

        budget_impact = _json_object(allocation.get("budget_impact_json") or row.get("budget_impact"))
        preview_budget = _json_object(budget_impact.get("preview"))
        updated_at = _latest_timestamp(
            allocation.get("created_at"),
            row.get("signal_created_at"),
            row.get("agent_updated_at"),
            row.get("live_updated_at"),
            row.get("calibration_profile_materialized_at"),
            retrospective.get("updated_at"),
        )
        strategy_id = allocation.get("strategy_id") or retrospective.get("strategy_id")
        queue_item_id = f"queue:{market_id}:{allocation.get('allocation_decision_id') or strategy_id or 'none'}"
        rows.append(
            {
                "queue_item_id": queue_item_id,
                "market_id": market_id,
                "wallet_id": allocation.get("wallet_id"),
                "strategy_id": strategy_id,
                "location_name": row.get("location_name"),
                "question": row.get("question"),
                "best_side": row.get("best_side"),
                "ranking_score": _coerce_float(_prefer_overlay_value(allocation, row, "ranking_score")) or 0.0,
                "base_ranking_score": _coerce_float(_prefer_overlay_value(allocation, row, "base_ranking_score")),
                "expected_dollar_pnl": _coerce_float(row.get("expected_dollar_pnl")) or 0.0,
                "deployable_expected_pnl": _coerce_float(_prefer_overlay_value(allocation, row, "deployable_expected_pnl")),
                "deployable_notional": _coerce_float(_prefer_overlay_value(allocation, row, "deployable_notional")),
                "max_deployable_size": _coerce_float(_prefer_overlay_value(allocation, row, "max_deployable_size")),
                "pre_budget_deployable_size": _coerce_float(allocation.get("pre_budget_deployable_size")) if allocation.get("pre_budget_deployable_size") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_size")),
                "pre_budget_deployable_notional": _coerce_float(allocation.get("pre_budget_deployable_notional")) if allocation.get("pre_budget_deployable_notional") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_notional")),
                "pre_budget_deployable_expected_pnl": _coerce_float(allocation.get("pre_budget_deployable_expected_pnl")) if allocation.get("pre_budget_deployable_expected_pnl") is not None else _coerce_float(preview_budget.get("pre_budget_deployable_expected_pnl")),
                "preview_binding_limit_scope": allocation.get("preview_binding_limit_scope") or preview_budget.get("preview_binding_limit_scope"),
                "preview_binding_limit_key": allocation.get("preview_binding_limit_key") or preview_budget.get("preview_binding_limit_key"),
                "requested_size": _coerce_float(allocation.get("requested_size")) if allocation.get("requested_size") is not None else _coerce_float(preview_budget.get("requested_size")),
                "requested_notional": _coerce_float(allocation.get("requested_notional")) if allocation.get("requested_notional") is not None else _coerce_float(preview_budget.get("requested_notional")),
                "recommended_size": _coerce_float(_prefer_overlay_value(allocation, row, "recommended_size")),
                "allocation_status": allocation_status or None,
                "actionability_status": actionability_status or None,
                "agent_review_status": agent_review_status or None,
                "feedback_status": feedback_status or None,
                "feedback_penalty": _coerce_float(row.get("feedback_penalty") or watch_only.get("feedback_penalty")),
                "calibration_freshness_status": calibration_freshness_status or None,
                "calibration_gate_status": calibration_gate_status or None,
                "calibration_gate_reason_codes_json": json.dumps(_json_list(row.get("calibration_gate_reason_codes")), ensure_ascii=True, sort_keys=True),
                "calibration_impacted_market": bool(row.get("calibration_impacted_market")),
                "market_quality_status": market_quality_status or None,
                "source_freshness_status": source_freshness_status or None,
                "source_badge": row.get("source_badge"),
                "source_truth_status": row.get("source_truth_status"),
                "operator_bucket": operator_bucket,
                "queue_priority": queue_priority,
                "queue_reason_codes_json": json.dumps(reason_codes, ensure_ascii=True, sort_keys=True),
                "binding_limit_scope": allocation.get("binding_limit_scope") or budget_impact.get("binding_limit_scope"),
                "binding_limit_key": allocation.get("binding_limit_key") or budget_impact.get("binding_limit_key"),
                "capital_policy_id": allocation.get("capital_policy_id") or row.get("capital_policy_id"),
                "capital_policy_version": allocation.get("capital_policy_version") or row.get("capital_policy_version"),
                "capital_scaling_reason_codes_json": json.dumps(
                    _json_list(allocation.get("capital_scaling_reason_codes_json")) if allocation.get("capital_scaling_reason_codes_json") is not None else _json_list(row.get("capital_scaling_reason_codes")),
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                "regime_bucket": allocation.get("regime_bucket") or row.get("regime_bucket"),
                "capital_scarcity_penalty": _coerce_float(_prefer_overlay_value(allocation, row, "capital_scarcity_penalty")),
                "concentration_penalty": _coerce_float(_prefer_overlay_value(allocation, row, "concentration_penalty")),
                "rerank_position": int(_coerce_float(allocation.get("rerank_position")) or _coerce_float(budget_impact.get("rerank_position")) or 0) or None,
                "rerank_reason_codes_json": json.dumps(
                    _json_list(allocation.get("rerank_reason_codes_json")) if allocation.get("rerank_reason_codes_json") is not None else _json_list(budget_impact.get("rerank_reason_codes")),
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                "remaining_run_budget": _coerce_float(budget_impact.get("remaining_run_budget")),
                "allocation_decision_id": allocation.get("allocation_decision_id") or row.get("allocation_decision_id"),
                "updated_at": updated_at,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        _create_empty_action_queue_summary(con, table_row_counts=table_row_counts)
        return
    frame = frame.sort_values(
        by=["queue_priority", "ranking_score", "deployable_expected_pnl", "expected_dollar_pnl", "updated_at"],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)
    frame = ensure_primary_score_fields(frame)
    frame = annotate_frame_with_source_truth(
        frame,
        source_origin="ui_lite",
        derived=False,
        freshness_column="source_freshness_status",
    )
    con.register("action_queue_summary_df", frame)
    con.execute("CREATE OR REPLACE TABLE ui.action_queue_summary AS SELECT * FROM action_queue_summary_df")
    row = con.execute("SELECT COUNT(*) FROM ui.action_queue_summary").fetchone()
    table_row_counts["ui.action_queue_summary"] = int(row[0]) if row is not None else 0
    con.unregister("action_queue_summary_df")


def _create_empty_action_queue_summary(con, *, table_row_counts: dict[str, int]) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE ui.action_queue_summary (
            queue_item_id TEXT,
            market_id TEXT,
            wallet_id TEXT,
            strategy_id TEXT,
            location_name TEXT,
            question TEXT,
            best_side TEXT,
            ranking_score DOUBLE,
            base_ranking_score DOUBLE,
            expected_dollar_pnl DOUBLE,
            deployable_expected_pnl DOUBLE,
            deployable_notional DOUBLE,
            max_deployable_size DOUBLE,
            pre_budget_deployable_size DOUBLE,
            pre_budget_deployable_notional DOUBLE,
            pre_budget_deployable_expected_pnl DOUBLE,
            preview_binding_limit_scope TEXT,
            preview_binding_limit_key TEXT,
            requested_size DOUBLE,
            requested_notional DOUBLE,
            recommended_size DOUBLE,
            allocation_status TEXT,
            actionability_status TEXT,
            agent_review_status TEXT,
            feedback_status TEXT,
            feedback_penalty DOUBLE,
            calibration_freshness_status TEXT,
            calibration_gate_status TEXT,
            calibration_gate_reason_codes_json TEXT,
            calibration_impacted_market BOOLEAN,
            market_quality_status TEXT,
            source_freshness_status TEXT,
            source_badge TEXT,
            source_truth_status TEXT,
            operator_bucket TEXT,
            queue_priority BIGINT,
            queue_reason_codes_json TEXT,
            binding_limit_scope TEXT,
            binding_limit_key TEXT,
            capital_policy_id TEXT,
            capital_policy_version TEXT,
            capital_scaling_reason_codes_json TEXT,
            regime_bucket TEXT,
            capital_scarcity_penalty DOUBLE,
            concentration_penalty DOUBLE,
            rerank_position BIGINT,
            rerank_reason_codes_json TEXT,
            remaining_run_budget DOUBLE,
            allocation_decision_id TEXT,
            updated_at TIMESTAMP,
            primary_score_label TEXT
        )
        """
    )
    table_row_counts["ui.action_queue_summary"] = 0


def _classify_operator_bucket(
    *,
    actionability_status: str,
    allocation_status: str,
    agent_review_status: str,
    market_quality_status: str,
    calibration_freshness_status: str,
    calibration_gate_status: str,
    source_freshness_status: str,
    feedback_status: str,
    source_truth_status: str,
    live_prereq_status: str,
) -> tuple[str, int, list[str]]:
    reasons: list[str] = []
    if calibration_gate_status == "research_only":
        reasons.append("calibration_gate:research_only")
        return "research_only", 5, reasons
    if actionability_status == "no_trade" or not allocation_status:
        reasons.append("research_only")
        return "research_only", 5, reasons
    if allocation_status in {"blocked", "policy_missing"} or actionability_status == "blocked" or live_prereq_status == "attention_required":
        if allocation_status in {"blocked", "policy_missing"}:
            reasons.append(f"allocation:{allocation_status}")
        if actionability_status == "blocked":
            reasons.append("actionability:blocked")
        if live_prereq_status == "attention_required":
            reasons.append("live_prereq:attention_required")
        return "blocked", 4, reasons
    if calibration_gate_status == "review_required":
        reasons.append("calibration_gate:review_required")
        return "review_required", 3, reasons
    if actionability_status == "review_required" or agent_review_status != "passed":
        if actionability_status == "review_required":
            reasons.append("actionability:review_required")
        if agent_review_status and agent_review_status != "passed":
            reasons.append(f"agent_review:{agent_review_status}")
        return "review_required", 3, reasons
    high_risk_reasons = []
    if calibration_freshness_status and calibration_freshness_status != "fresh":
        high_risk_reasons.append(f"calibration_freshness:{calibration_freshness_status}")
    if source_freshness_status and source_freshness_status != "fresh":
        high_risk_reasons.append(f"source_freshness:{source_freshness_status}")
    if feedback_status and feedback_status not in {"healthy", "heuristic_only"}:
        high_risk_reasons.append(f"feedback_status:{feedback_status}")
    if market_quality_status and market_quality_status != "pass":
        high_risk_reasons.append(f"market_quality:{market_quality_status}")
    if source_truth_status == "degraded":
        high_risk_reasons.append("source_truth:degraded")
    if allocation_status in {"approved", "resized"} and high_risk_reasons:
        return "high_risk", 2, high_risk_reasons
    reasons.append(f"allocation:{allocation_status or 'none'}")
    return "ready_now", 1, reasons


def _load_latest_retrospective_rows(con) -> dict[str, dict[str, Any]]:
    if not _table_exists(con, "src.runtime.ranking_retrospective_rows"):
        return {}
    if _table_exists(con, "src.runtime.ranking_retrospective_runs"):
        frame = con.execute(
            """
            WITH latest_run AS (
                SELECT run_id
                FROM src.runtime.ranking_retrospective_runs
                ORDER BY window_end DESC, created_at DESC, run_id DESC
                LIMIT 1
            )
            SELECT
                row_id AS history_row_id,
                run_id,
                market_id,
                strategy_id,
                side,
                ranking_decile,
                top_k_bucket,
                evaluation_status,
                submitted_capture_ratio,
                fill_capture_ratio,
                resolution_capture_ratio,
                avg_ranking_score,
                avg_edge_bps_executable,
                avg_realized_pnl,
                avg_predicted_vs_realized_gap,
                forecast_replay_change_rate,
                top_rank_share_of_realized_pnl,
                created_at AS updated_at
            FROM src.runtime.ranking_retrospective_rows
            WHERE run_id = (SELECT run_id FROM latest_run)
            """
        ).df()
    else:
        frame = con.execute(
            """
            SELECT
                row_id AS history_row_id,
                run_id,
                market_id,
                strategy_id,
                side,
                ranking_decile,
                top_k_bucket,
                evaluation_status,
                submitted_capture_ratio,
                fill_capture_ratio,
                resolution_capture_ratio,
                avg_ranking_score,
                avg_edge_bps_executable,
                avg_realized_pnl,
                avg_predicted_vs_realized_gap,
                forecast_replay_change_rate,
                top_rank_share_of_realized_pnl,
                created_at AS updated_at
            FROM src.runtime.ranking_retrospective_rows
            """
        ).df()
    if frame.empty:
        return {}
    latest_rows: dict[str, dict[str, Any]] = {}
    sorted_frame = frame.sort_values(
        by=["updated_at", "ranking_decile", "avg_ranking_score"],
        ascending=[False, True, False],
        na_position="last",
    )
    for _, row in sorted_frame.iterrows():
        market_id = str(row.get("market_id") or "")
        latest_rows.setdefault(market_id, row.to_dict())
    return latest_rows


def _table_dict_by_key(con, table_name: str, key_column: str) -> dict[str, dict[str, Any]]:
    if not _table_exists(con, table_name):
        return {}
    frame = con.execute(f"SELECT * FROM {table_name}").df()
    if frame.empty or key_column not in frame.columns:
        return {}
    return {
        str(row[key_column]): row.to_dict()
        for _, row in frame.iterrows()
        if row.get(key_column) is not None
    }


def _table_exists(con, table_name: str) -> bool:
    parts = table_name.split(".")
    if len(parts) == 2:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            LIMIT 1
            """,
            [parts[0], parts[1]],
        ).fetchone()
    elif len(parts) == 3:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            LIMIT 1
            """,
            [parts[0], parts[1], parts[2]],
        ).fetchone()
    else:
        row = None
    return row is not None


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None or value == "":
        return []
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    return payload if isinstance(payload, list) else []


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None or value == "":
        return {}
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:  # noqa: BLE001
        pass
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return None


def _prefer_overlay_value(overlay: dict[str, Any], base: Any, key: str) -> Any:
    if isinstance(overlay, dict) and key in overlay and overlay.get(key) is not None:
        return overlay.get(key)
    if isinstance(base, dict):
        return base.get(key)
    return None


def _latest_timestamp(*values: Any) -> Any:
    candidates = [value for value in values if value not in {None, ""}]
    if not candidates:
        return None
    return max(candidates, key=lambda item: str(item))
