from __future__ import annotations

import pandas as pd

from ui.loaders.shared_truth_source import SurfaceLoaderContract, build_truth_source_summary, validate_surface_loader_contract


def load_execution_console_data() -> dict[str, pd.DataFrame]:
    from ui import data_access as compat

    snapshot = compat.load_ui_lite_snapshot()
    tickets = compat._sort_desc(snapshot["tables"]["execution_ticket_summary"], "latest_transition_at", "last_fill_at")
    runs = compat._sort_desc(snapshot["tables"]["execution_run_summary"], "latest_event_at")
    exceptions = compat._sort_desc(snapshot["tables"]["execution_exception_summary"], "latest_transition_at", "latest_event_at")
    live_prereq = compat._sort_desc(snapshot["tables"]["live_prereq_execution_summary"], "latest_submit_created_at", "latest_sign_attempt_created_at")
    journal = compat._sort_desc(snapshot["tables"]["paper_run_journal_summary"], "latest_event_at")
    daily_ops = compat._sort_desc(snapshot["tables"]["daily_ops_summary"], "latest_event_at")
    predicted_vs_realized = compat.annotate_frame_with_source_truth(
        compat._sort_desc(snapshot["tables"]["predicted_vs_realized_summary"], "latest_fill_at", "latest_resolution_at"),
        source_origin="ui_lite",
        derived=True,
        freshness_column="forecast_freshness",
    )
    watch_only_vs_executed = compat.annotate_frame_with_source_truth(
        compat._sort_desc(snapshot["tables"]["watch_only_vs_executed_summary"], "fill_capture_ratio", "avg_executable_edge_bps"),
        source_origin="ui_lite",
        derived=True,
    )
    execution_science = compat.annotate_frame_with_source_truth(
        compat._sort_desc(snapshot["tables"]["execution_science_summary"], "resolution_capture_ratio", "fill_capture_ratio", "submission_capture_ratio"),
        source_origin="ui_lite",
        derived=True,
    )
    market_research = compat._sort_desc(snapshot["tables"]["market_research_summary"], "resolution_capture_ratio", "avg_post_trade_error")
    calibration_health = compat._sort_desc(snapshot["tables"]["calibration_health_summary"], "sample_count", "mean_abs_residual")
    cohort_history = snapshot["tables"]["cohort_history_summary"]
    if not cohort_history.empty:
        cohort_history = cohort_history.sort_values(
            by=["updated_at", "ranking_decile", "avg_ranking_score"],
            ascending=[False, True, False],
            na_position="last",
        ).reset_index(drop=True)
    cohort_history = compat.annotate_frame_with_source_truth(
        cohort_history,
        source_origin="ui_lite",
        derived=True,
    )
    return {
        "tickets": tickets,
        "runs": runs,
        "exceptions": exceptions,
        "live_prereq": live_prereq,
        "journal": journal,
        "daily_ops": daily_ops,
        "predicted_vs_realized": predicted_vs_realized,
        "watch_only_vs_executed": watch_only_vs_executed,
        "execution_science": execution_science,
        "market_research": market_research,
        "calibration_health": calibration_health,
        "cohort_history": cohort_history,
    }


def load_execution_surface_contract() -> SurfaceLoaderContract:
    payload = load_execution_console_data()
    contract = SurfaceLoaderContract(
        surface_id="execution",
        primary_dataframe_name="execution_science",
        supporting_payload=payload,
        truth_source_summary=build_truth_source_summary(
            surface_id="execution",
            primary_table="ui.execution_science_summary",
            source="ui_lite",
            supports_source_badges=True,
        ),
    )
    validate_surface_loader_contract(contract)
    return contract
