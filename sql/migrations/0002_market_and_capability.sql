CREATE SCHEMA IF NOT EXISTS capability;

CREATE TABLE IF NOT EXISTS capability.market_capabilities (
    token_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    tick_size DECIMAL(18,8) NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    neg_risk BOOLEAN NOT NULL,
    min_order_size DECIMAL(18,8) NOT NULL,
    tradable BOOLEAN NOT NULL,
    fees_enabled BOOLEAN NOT NULL,
    data_sources TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS capability.account_trading_capabilities (
    wallet_id TEXT PRIMARY KEY,
    wallet_type TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    allowance_targets TEXT NOT NULL,
    can_use_relayer BOOLEAN NOT NULL,
    can_trade BOOLEAN NOT NULL,
    restricted_reason TEXT,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS capability.capability_overrides (
    override_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS capability.execution_contexts (
    execution_context_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    route_action TEXT NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    tick_size DECIMAL(18,8) NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    risk_gate_result TEXT NOT NULL,
    market_capability_ref TEXT NOT NULL,
    account_capability_ref TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
