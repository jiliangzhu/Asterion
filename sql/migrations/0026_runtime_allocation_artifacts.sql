CREATE TABLE IF NOT EXISTS runtime.capital_allocation_runs (
    allocation_run_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    strategy_id TEXT,
    source_kind TEXT NOT NULL,
    requested_decision_count BIGINT NOT NULL,
    decision_count BIGINT NOT NULL,
    approved_count BIGINT NOT NULL,
    resized_count BIGINT NOT NULL,
    blocked_count BIGINT NOT NULL,
    policy_missing_count BIGINT NOT NULL,
    requested_buy_notional_total DOUBLE NOT NULL,
    recommended_buy_notional_total DOUBLE NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime.allocation_decisions (
    allocation_decision_id TEXT PRIMARY KEY,
    allocation_run_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    decision_id TEXT NOT NULL,
    watch_snapshot_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    ranking_score DOUBLE NOT NULL,
    requested_size DOUBLE NOT NULL,
    recommended_size DOUBLE NOT NULL,
    requested_notional DOUBLE NOT NULL,
    recommended_notional DOUBLE NOT NULL,
    allocation_status TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    budget_impact_json TEXT NOT NULL,
    policy_id TEXT,
    policy_version TEXT,
    source_kind TEXT NOT NULL,
    binding_limit_scope TEXT,
    binding_limit_key TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime.position_limit_checks (
    check_id TEXT PRIMARY KEY,
    allocation_decision_id TEXT NOT NULL,
    limit_id TEXT NOT NULL,
    limit_scope TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    observed_gross_notional DOUBLE NOT NULL,
    candidate_gross_notional DOUBLE NOT NULL,
    remaining_capacity DOUBLE,
    check_status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
