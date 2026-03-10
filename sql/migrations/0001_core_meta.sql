CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.ingest_runs (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    source TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    rows_written BIGINT,
    error_message TEXT,
    params_json TEXT
);

CREATE TABLE IF NOT EXISTS meta.watermarks (
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    market_id TEXT NOT NULL,
    cursor_name TEXT NOT NULL,
    cursor_value TEXT,
    cursor_value_ms BIGINT,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (source, endpoint, market_id, cursor_name)
);

CREATE TABLE IF NOT EXISTS meta.domain_events (
    event_id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_payload_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    request_id TEXT
);

CREATE TABLE IF NOT EXISTS meta.signature_audit_logs (
    log_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    signature_type TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    signature TEXT,
    status TEXT NOT NULL,
    requester TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    error TEXT
);
