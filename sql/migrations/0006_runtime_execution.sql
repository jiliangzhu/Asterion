CREATE SCHEMA IF NOT EXISTS runtime;

CREATE TABLE IF NOT EXISTS runtime.strategy_runs (
    run_id TEXT PRIMARY KEY,
    data_snapshot_id TEXT NOT NULL,
    universe_snapshot_id TEXT,
    asof_ts_ms BIGINT NOT NULL,
    dq_level TEXT NOT NULL,
    strategy_ids_json TEXT NOT NULL,
    decision_count INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime.trade_tickets (
    ticket_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    reference_price DECIMAL(18,8) NOT NULL,
    fair_value DECIMAL(18,8) NOT NULL,
    edge_bps INTEGER NOT NULL,
    threshold_bps INTEGER NOT NULL,
    route_action TEXT NOT NULL,
    size DECIMAL(18,8) NOT NULL,
    signal_ts_ms BIGINT NOT NULL,
    forecast_run_id TEXT NOT NULL,
    watch_snapshot_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    ticket_hash TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime.gate_decisions (
    gate_id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    allowed BOOLEAN NOT NULL,
    reason TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime.journal_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    run_id TEXT,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
