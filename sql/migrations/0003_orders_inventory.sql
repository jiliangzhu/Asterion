CREATE SCHEMA IF NOT EXISTS trading;

CREATE TABLE IF NOT EXISTS trading.orders (
    order_id TEXT PRIMARY KEY,
    client_order_id TEXT UNIQUE NOT NULL,
    wallet_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price DECIMAL(18,8) NOT NULL,
    size DECIMAL(18,8) NOT NULL,
    route_action TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    expiration TIMESTAMP,
    fee_rate_bps INTEGER NOT NULL,
    signature_type INTEGER NOT NULL,
    funder TEXT NOT NULL,
    status TEXT NOT NULL,
    filled_size DECIMAL(18,8) NOT NULL DEFAULT 0,
    remaining_size DECIMAL(18,8) NOT NULL,
    avg_fill_price DECIMAL(18,8),
    reservation_id TEXT,
    exchange_order_id TEXT,
    created_at TIMESTAMP NOT NULL,
    submitted_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trading.fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    price DECIMAL(18,8) NOT NULL,
    size DECIMAL(18,8) NOT NULL,
    fee DECIMAL(18,8) NOT NULL,
    fee_rate_bps INTEGER NOT NULL,
    trade_id TEXT UNIQUE NOT NULL,
    exchange_order_id TEXT NOT NULL,
    filled_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trading.order_state_transitions (
    transition_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    reason TEXT,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trading.reservations (
    reservation_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    wallet_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    reserved_quantity DECIMAL(18,8) NOT NULL,
    remaining_quantity DECIMAL(18,8) NOT NULL,
    reserved_notional DECIMAL(18,8) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trading.inventory_positions (
    wallet_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    outcome TEXT,
    balance_type TEXT NOT NULL,
    quantity DECIMAL(18,8) NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (wallet_id, asset_type, token_id, market_id, outcome, balance_type)
);

CREATE TABLE IF NOT EXISTS trading.exposure_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    open_order_size DECIMAL(18,8) NOT NULL,
    reserved_notional_usdc DECIMAL(18,8) NOT NULL,
    filled_position_size DECIMAL(18,8) NOT NULL,
    settled_position_size DECIMAL(18,8) NOT NULL,
    redeemable_size DECIMAL(18,8) NOT NULL,
    captured_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trading.reconciliation_results (
    reconciliation_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    funder TEXT NOT NULL,
    signature_type INTEGER NOT NULL,
    asset_type TEXT NOT NULL,
    token_id TEXT,
    market_id TEXT,
    balance_type TEXT NOT NULL,
    local_quantity DECIMAL(18,8) NOT NULL,
    remote_quantity DECIMAL(18,8) NOT NULL,
    discrepancy DECIMAL(18,8) NOT NULL,
    status TEXT NOT NULL,
    resolution TEXT,
    created_at TIMESTAMP NOT NULL
);
