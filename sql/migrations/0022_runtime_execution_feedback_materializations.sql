CREATE SCHEMA IF NOT EXISTS runtime;

CREATE TABLE IF NOT EXISTS runtime.execution_feedback_materializations (
    materialization_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    prior_version TEXT NOT NULL,
    status TEXT NOT NULL,
    lookback_days BIGINT NOT NULL,
    source_window_start TIMESTAMP NOT NULL,
    source_window_end TIMESTAMP NOT NULL,
    input_ticket_count BIGINT NOT NULL,
    output_prior_count BIGINT NOT NULL,
    degraded_prior_count BIGINT NOT NULL,
    materialized_at TIMESTAMP NOT NULL,
    error TEXT
);
