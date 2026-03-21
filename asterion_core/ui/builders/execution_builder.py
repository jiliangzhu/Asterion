from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from asterion_core.ui.surface_truth_shared import annotate_frame_with_source_truth, ensure_primary_score_fields


BUILDER_NAME = "execution_builder"
TABLES = (
    "ui.execution_ticket_summary",
    "ui.execution_run_summary",
    "ui.execution_exception_summary",
    "ui.predicted_vs_realized_summary",
    "ui.watch_only_vs_executed_summary",
    "ui.execution_science_summary",
    "ui.market_research_summary",
    "ui.cohort_history_summary",
)


def build_execution_tables(
    con,
    *,
    table_row_counts: dict[str, int],
    create_execution_ticket_summary: Callable[[], None],
    create_execution_run_summary: Callable[[], None],
    create_execution_exception_summary: Callable[[], None],
    create_predicted_vs_realized_summary: Callable[[], None],
    create_watch_only_vs_executed_summary: Callable[[], None],
    create_execution_science_summary: Callable[[], None],
    create_market_research_summary: Callable[[], None],
) -> None:
    create_execution_ticket_summary()
    create_execution_run_summary()
    create_execution_exception_summary()
    create_predicted_vs_realized_summary()
    create_watch_only_vs_executed_summary()
    create_execution_science_summary()
    create_market_research_summary()
    _create_cohort_history_summary(con, table_row_counts=table_row_counts)


def _create_cohort_history_summary(con, *, table_row_counts: dict[str, int]) -> None:
    if not _table_exists(con, "src.runtime.ranking_retrospective_rows"):
        _create_empty_cohort_history_summary(con, table_row_counts=table_row_counts)
        return
    frame = _load_latest_retrospective_rows(con)
    if frame.empty:
        _create_empty_cohort_history_summary(con, table_row_counts=table_row_counts)
        return

    science_frame = _load_table(con, "ui.execution_science_summary")
    market_frame = _load_table(con, "ui.market_opportunity_summary")
    science_keys: dict[tuple[str, str], dict[str, Any]] = {}
    if not science_frame.empty and {"cohort_type", "cohort_key"} <= set(science_frame.columns):
        science_keys = {
            (str(row["cohort_type"]), str(row["cohort_key"])): row.to_dict()
            for _, row in science_frame.iterrows()
        }
    market_by_market = (
        {
            str(row["market_id"]): row.to_dict()
            for _, row in market_frame.iterrows()
            if row.get("market_id") is not None
        }
        if not market_frame.empty and "market_id" in market_frame.columns
        else {}
    )

    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        market_overlay = market_by_market.get(str(row.get("market_id") or "")) or {}
        strategy_overlay = science_keys.get(("strategy", str(row.get("strategy_id") or ""))) or {}
        rows.append(
            {
                **row.to_dict(),
                "feedback_status": strategy_overlay.get("feedback_status") or market_overlay.get("feedback_status"),
                "feedback_penalty": _coerce_float(strategy_overlay.get("feedback_penalty") or market_overlay.get("feedback_penalty")),
                "cohort_prior_version": strategy_overlay.get("cohort_prior_version") or market_overlay.get("cohort_prior_version"),
                "calibration_freshness_status": market_overlay.get("calibration_freshness_status"),
                "source_badge": strategy_overlay.get("source_badge") or market_overlay.get("source_badge"),
                "source_truth_status": strategy_overlay.get("source_truth_status") or market_overlay.get("source_truth_status"),
            }
        )

    result = pd.DataFrame(rows)
    result = ensure_primary_score_fields(result)
    result = annotate_frame_with_source_truth(
        result,
        source_origin="ui_lite",
        derived=True,
    )
    con.register("cohort_history_summary_df", result)
    con.execute("CREATE OR REPLACE TABLE ui.cohort_history_summary AS SELECT * FROM cohort_history_summary_df")
    row = con.execute("SELECT COUNT(*) FROM ui.cohort_history_summary").fetchone()
    table_row_counts["ui.cohort_history_summary"] = int(row[0]) if row is not None else 0
    con.unregister("cohort_history_summary_df")


def _load_latest_retrospective_rows(con) -> pd.DataFrame:
    if _table_exists(con, "src.runtime.ranking_retrospective_runs"):
        return con.execute(
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
                window_start,
                window_end,
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
    return con.execute(
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
            window_start,
            window_end,
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


def _create_empty_cohort_history_summary(con, *, table_row_counts: dict[str, int]) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE ui.cohort_history_summary (
            history_row_id TEXT,
            run_id TEXT,
            market_id TEXT,
            strategy_id TEXT,
            side TEXT,
            ranking_decile BIGINT,
            top_k_bucket TEXT,
            evaluation_status TEXT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            submitted_capture_ratio DOUBLE,
            fill_capture_ratio DOUBLE,
            resolution_capture_ratio DOUBLE,
            avg_ranking_score DOUBLE,
            avg_edge_bps_executable DOUBLE,
            avg_realized_pnl DOUBLE,
            avg_predicted_vs_realized_gap DOUBLE,
            forecast_replay_change_rate DOUBLE,
            top_rank_share_of_realized_pnl DOUBLE,
            feedback_status TEXT,
            feedback_penalty DOUBLE,
            cohort_prior_version TEXT,
            calibration_freshness_status TEXT,
            source_badge TEXT,
            source_truth_status TEXT,
            updated_at TIMESTAMP,
            primary_score_label TEXT
        )
        """
    )
    table_row_counts["ui.cohort_history_summary"] = 0


def _load_table(con, table_name: str) -> pd.DataFrame:
    if not _table_exists(con, table_name):
        return pd.DataFrame()
    return con.execute(f"SELECT * FROM {table_name}").df()


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
