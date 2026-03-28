CREATE SCHEMA IF NOT EXISTS agent;

CREATE TABLE IF NOT EXISTS agent.operator_review_decisions (
    review_decision_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL,
    agent_type TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    decision_status TEXT NOT NULL,
    operator_action TEXT NOT NULL,
    reason TEXT,
    actor TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
