CREATE SCHEMA IF NOT EXISTS agent;

CREATE TABLE IF NOT EXISTS agent.invocations (
    invocation_id TEXT PRIMARY KEY,
    agent_type TEXT NOT NULL,
    agent_version TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    model_provider TEXT NOT NULL,
    model_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    latency_ms BIGINT,
    error_message TEXT,
    input_payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent.outputs (
    output_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL,
    verdict TEXT NOT NULL,
    confidence DOUBLE NOT NULL,
    summary TEXT NOT NULL,
    findings_json TEXT NOT NULL,
    structured_output_json TEXT NOT NULL,
    human_review_required BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS agent.reviews (
    review_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL,
    review_status TEXT NOT NULL,
    reviewer_id TEXT NOT NULL,
    review_notes TEXT,
    review_payload_json TEXT NOT NULL,
    reviewed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS agent.evaluations (
    evaluation_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL,
    verification_method TEXT NOT NULL,
    score_json TEXT NOT NULL,
    is_verified BOOLEAN NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL
);
