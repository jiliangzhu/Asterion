CREATE TABLE IF NOT EXISTS runtime.ranking_retrospective_runs (
    run_id TEXT PRIMARY KEY,
    baseline_version TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    snapshot_count BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    summary_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime.ranking_retrospective_rows (
    row_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    side TEXT NOT NULL,
    ranking_decile BIGINT NOT NULL,
    top_k_bucket TEXT NOT NULL,
    evaluation_status TEXT NOT NULL,
    submitted_capture_ratio DOUBLE NOT NULL,
    fill_capture_ratio DOUBLE NOT NULL,
    resolution_capture_ratio DOUBLE NOT NULL,
    avg_ranking_score DOUBLE NOT NULL,
    avg_edge_bps_executable DOUBLE NOT NULL,
    avg_realized_pnl DOUBLE,
    avg_predicted_vs_realized_gap DOUBLE,
    forecast_replay_change_rate DOUBLE NOT NULL,
    top_rank_share_of_realized_pnl DOUBLE NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL
);
