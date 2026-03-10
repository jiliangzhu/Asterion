from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .database import DuckDBConfig, connect_duckdb


@dataclass(frozen=True)
class MigrationConfig:
    db_path: str
    migrations_dir: str


def discover_migration_files(migrations_dir: str) -> list[Path]:
    root = Path(migrations_dir)
    return sorted(path for path in root.glob("*.sql") if path.is_file())


def apply_migrations(cfg: MigrationConfig) -> list[str]:
    applied: list[str] = []
    con = connect_duckdb(DuckDBConfig(db_path=cfg.db_path, ddl_path=None))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS meta")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMP NOT NULL DEFAULT now()
            )
            """
        )
        for path in discover_migration_files(cfg.migrations_dir):
            version = path.name
            row = con.execute(
                "SELECT 1 FROM meta.schema_migrations WHERE version = ? LIMIT 1",
                [version],
            ).fetchone()
            if row:
                continue
            sql = path.read_text(encoding="utf-8").strip()
            if sql:
                con.execute(sql)
            con.execute("INSERT INTO meta.schema_migrations (version) VALUES (?)", [version])
            applied.append(version)
    finally:
        con.close()
    return applied
