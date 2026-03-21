CREATE SCHEMA IF NOT EXISTS resolution;

CREATE TABLE IF NOT EXISTS resolution.operator_review_decisions (
    review_decision_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    invocation_id TEXT NOT NULL,
    suggestion_id TEXT NOT NULL,
    decision_status TEXT NOT NULL,
    operator_action TEXT NOT NULL,
    reason TEXT,
    actor TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_resolution_operator_review_decisions_proposal
ON resolution.operator_review_decisions (proposal_id, updated_at);
