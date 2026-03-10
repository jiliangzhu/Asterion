CREATE SCHEMA IF NOT EXISTS resolution;

CREATE TABLE IF NOT EXISTS resolution.uma_proposals (
    proposal_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    proposer TEXT NOT NULL,
    proposed_outcome TEXT NOT NULL,
    proposal_bond DOUBLE NOT NULL,
    dispute_bond DOUBLE,
    proposal_tx_hash TEXT NOT NULL,
    proposal_block_number BIGINT NOT NULL,
    proposal_timestamp TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    on_chain_settled_at TIMESTAMP,
    safe_redeem_after TIMESTAMP,
    human_review_required BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution.proposal_state_transitions (
    transition_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    old_status TEXT NOT NULL,
    new_status TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    recorded_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution.processed_uma_events (
    event_id TEXT PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution.block_watermarks (
    chain_id BIGINT PRIMARY KEY,
    last_processed_block BIGINT NOT NULL,
    last_finalized_block BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution.watcher_continuity_checks (
    check_id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    from_block BIGINT NOT NULL,
    to_block BIGINT NOT NULL,
    last_known_finalized_block BIGINT NOT NULL,
    status TEXT NOT NULL,
    gap_count BIGINT NOT NULL,
    details_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution.watcher_continuity_gaps (
    gap_id TEXT PRIMARY KEY,
    check_id TEXT NOT NULL,
    gap_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    block_start BIGINT NOT NULL,
    block_end BIGINT NOT NULL,
    entity_ref TEXT,
    details_json TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution.settlement_verifications (
    verification_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    proposed_outcome TEXT NOT NULL,
    expected_outcome TEXT NOT NULL,
    is_correct BOOLEAN NOT NULL,
    confidence DOUBLE NOT NULL,
    discrepancy_details TEXT,
    sources_checked TEXT,
    evidence_package TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution.proposal_evidence_links (
    proposal_id TEXT NOT NULL,
    verification_id TEXT NOT NULL,
    evidence_package_id TEXT NOT NULL,
    linked_at TIMESTAMP NOT NULL,
    PRIMARY KEY (proposal_id, verification_id)
);

CREATE TABLE IF NOT EXISTS resolution.redeem_readiness_suggestions (
    suggestion_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    on_chain_settled_at TIMESTAMP,
    safe_redeem_after TIMESTAMP,
    human_review_required BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL
);
