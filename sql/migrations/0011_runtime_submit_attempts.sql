CREATE TABLE IF NOT EXISTS runtime.submit_attempts (
    attempt_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    order_id TEXT,
    wallet_id TEXT NOT NULL,
    execution_context_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    attempt_kind TEXT NOT NULL,
    attempt_mode TEXT NOT NULL,
    canonical_order_hash TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    submit_payload_json TEXT NOT NULL,
    signed_payload_ref TEXT,
    status TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMP NOT NULL
);
