from __future__ import annotations

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.spec import (
    StationMapper,
    build_station_mapping_record,
    enqueue_station_mapping_upserts,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for station mapper tests")
class StationMapperTest(unittest.TestCase):
    def test_station_mapper_prefers_market_override_over_location_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")

            with patch.dict(
                os.environ,
                {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "writer",
                    "WRITERD": "1",
                },
                clear=False,
            ):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))

            queue_cfg = WriteQueueConfig(path=queue_path)
            default_mapping = build_station_mapping_record(
                location_name="New York City",
                station_id="KNYC",
                station_name="Central Park",
                latitude=40.7128,
                longitude=-74.0060,
                timezone="America/New_York",
                source="station_registry",
                authoritative_source="weather.com",
                metadata={"kind": "default"},
            )
            override_mapping = build_station_mapping_record(
                market_id="mkt_weather_1",
                location_name="New York City",
                station_id="KLGA",
                station_name="LaGuardia",
                latitude=40.7769,
                longitude=-73.8740,
                timezone="America/New_York",
                source="operator_override",
                authoritative_source="weather.com",
                is_override=True,
                metadata={"kind": "override"},
            )
            task_id = enqueue_station_mapping_upserts(queue_cfg, mappings=[default_mapping, override_mapping], run_id="run_station_map")
            self.assertIsNotNone(task_id)

            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": "weather.weather_station_map",
                },
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                reader_env = {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                }
                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        mapper = StationMapper()
                        station = mapper.resolve_from_spec_inputs(
                            con,
                            market_id="mkt_weather_1",
                            location_name="New York City",
                            authoritative_source="weather.com",
                        )
                        fallback = mapper.get_station_metadata(con, station_id="KNYC")
                    finally:
                        con.close()

            self.assertEqual(station.station_id, "KLGA")
            self.assertEqual(station.source, "operator_override")
            self.assertEqual(fallback.station_id, "KNYC")
            self.assertEqual(fallback.source, "station_registry")


if __name__ == "__main__":
    unittest.main()
