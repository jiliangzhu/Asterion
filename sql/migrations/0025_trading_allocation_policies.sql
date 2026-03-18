CREATE TABLE IF NOT EXISTS trading.allocation_policies (
    policy_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    strategy_id TEXT,
    status TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    max_buy_notional_per_run DOUBLE NOT NULL,
    max_buy_notional_per_ticket DOUBLE NOT NULL,
    min_recommended_size DOUBLE NOT NULL,
    size_rounding_increment DOUBLE NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trading.position_limit_policies (
    limit_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    limit_scope TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    max_gross_notional DOUBLE,
    max_position_quantity DOUBLE,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
