CREATE TABLE IF NOT EXISTS runtime.execution_intelligence_runs (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    input_ticket_count BIGINT NOT NULL,
    summary_count BIGINT NOT NULL,
    materialized_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_intelligence_runs_materialized_at
ON runtime.execution_intelligence_runs(materialized_at DESC);

CREATE TABLE IF NOT EXISTS runtime.execution_intelligence_summaries (
    summary_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    quote_imbalance_score DOUBLE NOT NULL,
    top_of_book_stability DOUBLE NOT NULL,
    book_update_intensity DOUBLE NOT NULL,
    spread_regime TEXT NOT NULL,
    visible_size_shock_flag BOOLEAN NOT NULL,
    book_pressure_side TEXT NOT NULL,
    expected_capture_regime TEXT NOT NULL,
    expected_slippage_regime TEXT NOT NULL,
    execution_intelligence_score DOUBLE NOT NULL,
    reason_codes_json TEXT NOT NULL,
    source_window_start TIMESTAMP NOT NULL,
    source_window_end TIMESTAMP NOT NULL,
    materialized_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_intelligence_summaries_market_side
ON runtime.execution_intelligence_summaries(market_id, side, materialized_at DESC);
