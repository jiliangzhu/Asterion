CREATE SCHEMA IF NOT EXISTS runtime;

ALTER TABLE runtime.live_boundary_attestations ADD COLUMN IF NOT EXISTS issuer TEXT;
ALTER TABLE runtime.live_boundary_attestations ADD COLUMN IF NOT EXISTS issued_at TIMESTAMP;
ALTER TABLE runtime.live_boundary_attestations ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;
ALTER TABLE runtime.live_boundary_attestations ADD COLUMN IF NOT EXISTS nonce TEXT;
ALTER TABLE runtime.live_boundary_attestations ADD COLUMN IF NOT EXISTS decision_fingerprint TEXT;
ALTER TABLE runtime.live_boundary_attestations ADD COLUMN IF NOT EXISTS attestation_mac TEXT;

CREATE TABLE IF NOT EXISTS runtime.live_boundary_attestation_uses (
    use_id TEXT PRIMARY KEY,
    attestation_id TEXT NOT NULL UNIQUE,
    request_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    target_backend_kind TEXT NOT NULL,
    submitter_endpoint_fingerprint TEXT NOT NULL,
    use_status TEXT NOT NULL,
    provider_status TEXT,
    error TEXT,
    created_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);
