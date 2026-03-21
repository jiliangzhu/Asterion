CREATE TABLE IF NOT EXISTS runtime.calibration_profile_materializations (
    materialization_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL,
    lookback_days BIGINT NOT NULL,
    source_window_start TIMESTAMP NOT NULL,
    source_window_end TIMESTAMP NOT NULL,
    input_sample_count BIGINT NOT NULL,
    output_profile_count BIGINT NOT NULL,
    fresh_profile_count BIGINT NOT NULL,
    stale_profile_count BIGINT NOT NULL,
    degraded_profile_count BIGINT NOT NULL,
    materialized_at TIMESTAMP NOT NULL,
    error TEXT
);
