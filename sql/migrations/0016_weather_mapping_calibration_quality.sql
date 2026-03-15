ALTER TABLE weather.weather_station_map ADD COLUMN IF NOT EXISTS mapping_method TEXT DEFAULT 'location_default';
ALTER TABLE weather.weather_station_map ADD COLUMN IF NOT EXISTS mapping_confidence DOUBLE DEFAULT 1.0;
ALTER TABLE weather.weather_station_map ADD COLUMN IF NOT EXISTS override_reason TEXT;

CREATE TABLE IF NOT EXISTS weather.forecast_calibration_samples (
    sample_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    source TEXT NOT NULL,
    forecast_horizon_bucket TEXT NOT NULL,
    season_bucket TEXT NOT NULL,
    metric TEXT NOT NULL,
    forecast_target_time TIMESTAMP NOT NULL,
    forecast_mean DOUBLE NOT NULL,
    observed_value DOUBLE NOT NULL,
    residual DOUBLE NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS weather.source_health_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    station_id TEXT NOT NULL,
    source TEXT NOT NULL,
    latest_market_updated_at TIMESTAMP,
    latest_forecast_created_at TIMESTAMP,
    latest_snapshot_created_at TIMESTAMP,
    price_staleness_ms BIGINT NOT NULL,
    source_freshness_status TEXT NOT NULL,
    degraded_reason_codes_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);
