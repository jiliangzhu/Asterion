CREATE SCHEMA IF NOT EXISTS runtime;

CREATE TABLE IF NOT EXISTS runtime.live_boundary_attestations (
    attestation_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    source_attempt_id TEXT,
    ticket_id TEXT,
    execution_context_id TEXT,
    attestation_kind TEXT NOT NULL,
    submit_mode TEXT NOT NULL,
    target_backend_kind TEXT NOT NULL,
    attestation_status TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    attestation_payload_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
