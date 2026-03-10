CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.weather_markets (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    event_id TEXT,
    slug TEXT,
    title TEXT NOT NULL,
    description TEXT,
    rules TEXT,
    status TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    closed BOOLEAN NOT NULL,
    archived BOOLEAN NOT NULL,
    accepting_orders BOOLEAN,
    enable_order_book BOOLEAN,
    tags_json TEXT NOT NULL,
    outcomes_json TEXT NOT NULL,
    token_ids_json TEXT NOT NULL,
    close_time TIMESTAMP,
    end_date TIMESTAMP,
    raw_market_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.weather_station_map (
    map_id TEXT PRIMARY KEY,
    market_id TEXT,
    location_name TEXT NOT NULL,
    location_key TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    timezone TEXT NOT NULL,
    source TEXT NOT NULL,
    authoritative_source TEXT,
    is_override BOOLEAN NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.weather_market_specs (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    location_name TEXT NOT NULL,
    station_id TEXT NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    timezone TEXT NOT NULL,
    observation_date DATE NOT NULL,
    observation_window_local TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT NOT NULL,
    bucket_min_value DOUBLE,
    bucket_max_value DOUBLE,
    authoritative_source TEXT NOT NULL,
    fallback_sources TEXT NOT NULL,
    rounding_rule TEXT NOT NULL,
    inclusive_bounds BOOLEAN NOT NULL,
    spec_version TEXT NOT NULL,
    parse_confidence DOUBLE NOT NULL,
    risk_flags_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.resolution_specs (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    location_name TEXT NOT NULL,
    station_id TEXT NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    timezone TEXT NOT NULL,
    observation_date DATE NOT NULL,
    observation_window_local TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT NOT NULL,
    authoritative_source TEXT NOT NULL,
    fallback_sources TEXT NOT NULL,
    rounding_rule TEXT NOT NULL,
    inclusive_bounds BOOLEAN NOT NULL,
    spec_version TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.weather_forecast_runs (
    run_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    source TEXT NOT NULL,
    model_run TEXT NOT NULL,
    forecast_target_time TIMESTAMP NOT NULL,
    observation_date DATE NOT NULL,
    metric TEXT NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    timezone TEXT NOT NULL,
    spec_version TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    source_trace_json TEXT NOT NULL,
    fallback_used BOOLEAN NOT NULL,
    from_cache BOOLEAN NOT NULL,
    confidence DOUBLE NOT NULL,
    forecast_payload_json TEXT NOT NULL,
    raw_payload_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.weather_forecast_replays (
    replay_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    source TEXT NOT NULL,
    model_run TEXT NOT NULL,
    forecast_target_time TIMESTAMP NOT NULL,
    spec_version TEXT NOT NULL,
    replay_key TEXT NOT NULL,
    replay_reason TEXT NOT NULL,
    original_run_id TEXT NOT NULL,
    replayed_run_id TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.weather_forecast_replay_diffs (
    diff_id TEXT PRIMARY KEY,
    replay_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    original_entity_id TEXT,
    replayed_entity_id TEXT,
    status TEXT NOT NULL,
    diff_summary_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.weather_fair_values (
    fair_value_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    fair_value DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    priced_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.weather_watch_only_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    fair_value_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reference_price DOUBLE NOT NULL,
    fair_value DOUBLE NOT NULL,
    edge_bps INTEGER NOT NULL,
    threshold_bps INTEGER NOT NULL,
    decision TEXT NOT NULL,
    side TEXT NOT NULL,
    rationale TEXT NOT NULL,
    pricing_context_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
