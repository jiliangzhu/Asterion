CREATE TABLE IF NOT EXISTS runtime.operator_surface_refresh_runs (
    refresh_run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    trigger_mode TEXT NOT NULL,
    source_db_path TEXT NOT NULL,
    ui_replica_ok BOOLEAN NOT NULL,
    ui_lite_ok BOOLEAN NOT NULL,
    truth_check_fail_count BIGINT NOT NULL,
    degraded_surface_count BIGINT NOT NULL,
    read_error_surface_count BIGINT NOT NULL,
    refreshed_at TIMESTAMP NOT NULL,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_operator_surface_refresh_runs_refreshed_at
ON runtime.operator_surface_refresh_runs(refreshed_at DESC);
