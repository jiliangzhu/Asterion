CREATE TABLE IF NOT EXISTS runtime.external_order_observations (
    observation_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    order_id TEXT,
    wallet_id TEXT NOT NULL,
    execution_context_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    observation_kind TEXT NOT NULL,
    submit_mode TEXT NOT NULL,
    canonical_order_hash TEXT NOT NULL,
    external_order_id TEXT,
    external_status TEXT NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    error TEXT,
    raw_observation_json TEXT NOT NULL
);
