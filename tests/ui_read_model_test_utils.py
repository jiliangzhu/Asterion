from __future__ import annotations

from pathlib import Path

import duckdb

from asterion_core.ui.builders.catalog_builder import build_catalog_tables
from asterion_core.ui.read_model_registry import iter_read_model_catalog_records


def build_minimal_ui_read_model_db(path: Path, *, empty_tables: set[str] | None = None, skip_columns: dict[str, set[str]] | None = None) -> None:
    empty_tables = empty_tables or set()
    skip_columns = skip_columns or {}
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE SCHEMA ui")
        for record in iter_read_model_catalog_records():
            if record.table_name in {"ui.read_model_catalog", "ui.truth_source_checks"}:
                continue
            columns = []
            seen: set[str] = set()
            for name in [*record.primary_key_columns, *record.required_columns]:
                if name not in seen and name not in skip_columns.get(record.table_name, set()):
                    columns.append(name)
                    seen.add(name)
            if record.primary_score_column and record.primary_score_column not in seen and record.primary_score_column not in skip_columns.get(record.table_name, set()):
                columns.append(record.primary_score_column)
                seen.add(record.primary_score_column)
            create_columns = ", ".join(f"{column} TEXT" for column in columns) or "placeholder TEXT"
            con.execute(f"CREATE TABLE {record.table_name} ({create_columns})")
            if record.table_name not in empty_tables:
                values = ", ".join(["?"] * len(columns))
                row = [f"{record.table_name}:{column}" for column in columns]
                if columns:
                    con.execute(f"INSERT INTO {record.table_name} VALUES ({values})", row)
        build_catalog_tables(con, table_row_counts={})
    finally:
        con.close()
