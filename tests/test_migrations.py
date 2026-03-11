from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations, discover_migration_files

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class MigrationFilesTest(unittest.TestCase):
    def test_expected_migration_files_exist_in_order(self) -> None:
        root = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        files = [path.name for path in discover_migration_files(str(root))]
        self.assertEqual(
            files,
            [
                "0001_core_meta.sql",
                "0002_market_and_capability.sql",
                "0003_orders_inventory.sql",
                "0004_weather_specs_and_forecasts.sql",
                "0005_uma_watcher.sql",
                "0006_runtime_execution.sql",
                "0007_agent_runtime.sql",
                "0008_runtime_handoff.sql",
                "0009_runtime_external_observations.sql",
                "0010_signature_audit_boundary.sql",
            ],
        )

    def test_migrations_contain_key_tables(self) -> None:
        root = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        contents = {path.name: path.read_text(encoding="utf-8") for path in discover_migration_files(str(root))}
        self.assertIn("meta.ingest_runs", contents["0001_core_meta.sql"])
        self.assertIn("capability.market_capabilities", contents["0002_market_and_capability.sql"])
        self.assertIn("trading.orders", contents["0003_orders_inventory.sql"])
        self.assertIn("weather.weather_markets", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("weather.weather_station_map", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("weather.weather_forecast_runs", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("weather.weather_forecast_replays", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("weather.weather_forecast_replay_diffs", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("weather.weather_fair_values", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("weather.weather_watch_only_snapshots", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("parse_confidence", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("source_trace_json", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("raw_payload_json", contents["0004_weather_specs_and_forecasts.sql"])
        self.assertIn("resolution.uma_proposals", contents["0005_uma_watcher.sql"])
        self.assertIn("resolution.processed_uma_events", contents["0005_uma_watcher.sql"])
        self.assertIn("resolution.block_watermarks", contents["0005_uma_watcher.sql"])
        self.assertIn("resolution.watcher_continuity_checks", contents["0005_uma_watcher.sql"])
        self.assertIn("resolution.watcher_continuity_gaps", contents["0005_uma_watcher.sql"])
        self.assertIn("resolution.redeem_readiness_suggestions", contents["0005_uma_watcher.sql"])
        self.assertIn("runtime.strategy_runs", contents["0006_runtime_execution.sql"])
        self.assertIn("runtime.trade_tickets", contents["0006_runtime_execution.sql"])
        self.assertIn("runtime.gate_decisions", contents["0006_runtime_execution.sql"])
        self.assertIn("runtime.journal_events", contents["0006_runtime_execution.sql"])
        self.assertIn("agent.invocations", contents["0007_agent_runtime.sql"])
        self.assertIn("agent.outputs", contents["0007_agent_runtime.sql"])
        self.assertIn("agent.reviews", contents["0007_agent_runtime.sql"])
        self.assertIn("agent.evaluations", contents["0007_agent_runtime.sql"])
        self.assertIn("ALTER TABLE runtime.trade_tickets ADD COLUMN IF NOT EXISTS wallet_id", contents["0008_runtime_handoff.sql"])
        self.assertIn("ALTER TABLE runtime.trade_tickets ADD COLUMN IF NOT EXISTS execution_context_id", contents["0008_runtime_handoff.sql"])
        self.assertIn("runtime.external_balance_observations", contents["0009_runtime_external_observations.sql"])
        self.assertIn("ALTER TABLE meta.signature_audit_logs ADD COLUMN IF NOT EXISTS wallet_type", contents["0010_signature_audit_boundary.sql"])
        self.assertIn("ALTER TABLE meta.signature_audit_logs ADD COLUMN IF NOT EXISTS signing_purpose", contents["0010_signature_audit_boundary.sql"])


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for migration application tests")
class ApplyMigrationsTest(unittest.TestCase):
    def test_apply_migrations_records_schema_versions(self) -> None:
        root = Path(__file__).resolve().parents[1] / "sql" / "migrations"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "writer",
                    "WRITERD": "1",
                },
                clear=False,
            ):
                applied = apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=str(root)))
            self.assertEqual(len(applied), 10)


if __name__ == "__main__":
    unittest.main()
