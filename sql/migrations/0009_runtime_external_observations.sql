CREATE TABLE IF NOT EXISTS runtime.external_balance_observations (
    observation_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    observation_kind TEXT NOT NULL,
    allowance_target TEXT,
    chain_id INTEGER NOT NULL,
    block_number BIGINT,
    observed_quantity DECIMAL(38,18) NOT NULL,
    source TEXT NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    raw_observation_json TEXT NOT NULL
);
