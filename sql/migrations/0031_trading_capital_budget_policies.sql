CREATE TABLE IF NOT EXISTS trading.capital_budget_policies (
    capital_policy_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    strategy_id TEXT,
    regime_bucket TEXT,
    calibration_gate_status TEXT,
    status TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    max_buy_notional_per_run DOUBLE NOT NULL,
    max_buy_notional_per_ticket DOUBLE NOT NULL,
    max_open_markets BIGINT,
    max_same_station_markets BIGINT,
    min_recommended_size DOUBLE NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS capital_policy_id TEXT;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS capital_policy_version TEXT;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS regime_bucket TEXT;
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS calibration_gate_status TEXT DEFAULT 'clear';
ALTER TABLE runtime.allocation_decisions ADD COLUMN IF NOT EXISTS capital_scaling_reason_codes_json TEXT DEFAULT '[]';
