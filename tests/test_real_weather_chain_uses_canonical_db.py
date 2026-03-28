from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "run_real_weather_chain_smoke.py"
LOOP_PATH = ROOT / "scripts" / "run_real_weather_chain_loop.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path.name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RealWeatherChainUsesCanonicalDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.smoke = _load_module(SCRIPT_PATH, "real_weather_chain_smoke_canonical")
        cls.loop = _load_module(LOOP_PATH, "real_weather_chain_loop_canonical")

    def test_smoke_db_resolution_prefers_cli_then_env_then_default(self) -> None:
        with patch.dict(os.environ, {"ASTERION_DB_PATH": "data/from_env.duckdb"}, clear=False):
            resolved = self.smoke._resolve_db_path(None)
        self.assertEqual(resolved, Path("data/from_env.duckdb"))

        resolved = self.smoke._resolve_db_path("data/from_cli.duckdb")
        self.assertEqual(resolved, Path("data/from_cli.duckdb"))

        with patch.dict(os.environ, {}, clear=True):
            resolved = self.smoke._resolve_db_path(None)
        self.assertEqual(resolved, self.smoke.DEFAULT_CANONICAL_DB_PATH)

    def test_loop_status_command_uses_same_canonical_db(self) -> None:
        args = self.loop.parse_args.__globals__["argparse"].Namespace(
            output_dir="data/dev/real_weather_chain",
            db_path="data/canonical_runtime.duckdb",
            recent_within_days=14,
            market_limit=24,
            skip_agent=False,
        )
        cmd = self.loop.build_smoke_command(args, force_rebuild=False)
        self.assertEqual(cmd[cmd.index("--db-path") + 1], "data/canonical_runtime.duckdb")
        self.assertEqual(cmd[cmd.index("--market-limit") + 1], "24")

    def test_startup_script_pins_data_loop_to_asterion_db_path(self) -> None:
        startup = (ROOT / "start_asterion.sh").read_text(encoding="utf-8")
        self.assertIn('canonical_db_path="${ASTERION_DB_PATH:-data/asterion.duckdb}"', startup)
        self.assertIn('--db-path "$canonical_db_path"', startup)

    def test_truth_source_split_brain_detects_legacy_dev_db_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            canonical = Path(tmpdir) / "canonical.duckdb"
            legacy = temp_root / "data" / "dev" / "real_weather_chain" / "real_weather_chain.duckdb"
            canonical.parent.mkdir(parents=True, exist_ok=True)
            self.smoke.apply_schema(canonical)
            legacy.parent.mkdir(parents=True, exist_ok=True)
            self.smoke.apply_schema(legacy)
            import duckdb

            con = duckdb.connect(str(canonical))
            try:
                con.execute("INSERT INTO weather.weather_forecast_runs (run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time, observation_date, metric, latitude, longitude, timezone, spec_version, cache_key, source_trace_json, fallback_used, from_cache, confidence, forecast_payload_json, raw_payload_json, created_at) VALUES ('run_canonical', 'mkt_1', 'cond_1', 'KSEA', 'openmeteo', '2026-03-20T00:00Z', '2026-03-20 12:00:00', '2026-03-20', 'temperature_max', 47.6062, -122.3321, 'America/Los_Angeles', 'spec_v1', 'cache_1', '[]', FALSE, FALSE, 0.9, '{\"temperature_distribution\":{\"65\":1.0}}', '{}', '2026-03-20 08:00:00')")
            finally:
                con.close()

            con = duckdb.connect(str(legacy))
            try:
                con.execute("INSERT INTO weather.weather_forecast_runs (run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time, observation_date, metric, latitude, longitude, timezone, spec_version, cache_key, source_trace_json, fallback_used, from_cache, confidence, forecast_payload_json, raw_payload_json, created_at) VALUES ('run_legacy', 'mkt_1', 'cond_1', 'KSEA', 'openmeteo', '2026-03-20T00:00Z', '2026-03-20 12:00:00', '2026-03-20', 'temperature_max', 47.6062, -122.3321, 'America/Los_Angeles', 'spec_v1', 'cache_1', '[]', FALSE, FALSE, 0.9, '{\"temperature_distribution\":{\"65\":1.0}}', '{}', '2026-03-20 08:00:00')")
                con.execute("INSERT INTO weather.weather_watch_only_snapshots (snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome, reference_price, fair_value, edge_bps, threshold_bps, decision, side, rationale, pricing_context_json, created_at) VALUES ('snap_legacy', 'fv_1', 'run_legacy', 'mkt_1', 'cond_1', 'tok_yes', 'YES', 0.45, 0.55, 1000, 100, 'TAKE', 'BUY', 'edge', '{}', '2026-03-20 08:10:00')")
            finally:
                con.close()

            with patch.object(self.smoke, "ROOT", temp_root):
                self.assertTrue(self.smoke._truth_source_split_brain_status(canonical_db_path=canonical))

    def test_runtime_forecast_source_prefers_env_override_then_profile_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "canonical.duckdb"
            self.smoke.apply_schema(db_path)
            con = duckdb.connect(str(db_path))
            try:
                con.execute(
                    """
                    INSERT INTO weather.forecast_calibration_profiles_v2 (
                        profile_key, station_id, source, metric, forecast_horizon_bucket, season_bucket, regime_bucket,
                        sample_count, mean_bias, mean_abs_residual, p90_abs_residual, empirical_coverage_50,
                        empirical_coverage_80, empirical_coverage_95, regime_stability_score, residual_quantiles_json,
                        threshold_probability_profile_json, calibration_health_status, window_start, window_end, materialized_at
                    ) VALUES (
                        'prof_1', 'KNYC', 'openmeteo', 'temperature_max', '0-1', 'spring', 'mild',
                        12, 0.1, 1.2, 2.4, 0.5, 0.8, 0.95, 0.7, '{}', NULL, 'watch',
                        '2026-03-01 00:00:00', '2026-03-20 00:00:00', '2026-03-20 00:00:00'
                    )
                    """
                )
            finally:
                con.close()

            with patch.dict(os.environ, {}, clear=False):
                self.assertEqual(self.smoke._resolve_runtime_forecast_source(db_path), "openmeteo")
            with patch.dict(os.environ, {"ASTERION_REAL_CHAIN_FORECAST_SOURCE": "nws"}, clear=False):
                self.assertEqual(self.smoke._resolve_runtime_forecast_source(db_path), "nws")


if __name__ == "__main__":
    unittest.main()
