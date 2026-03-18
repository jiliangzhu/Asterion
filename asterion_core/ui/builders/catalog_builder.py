from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from asterion_core.ui.read_model_registry import (
    get_read_model_catalog_record,
    iter_read_model_catalog_records,
    surface_truth_check_specs,
)


def build_catalog_tables(con, *, table_row_counts: dict[str, int]) -> None:
    updated_at = datetime.now(UTC).isoformat()
    catalog_rows = [
        {
            "table_name": record.table_name,
            "schema_version": record.schema_version,
            "builder_name": record.builder_name,
            "primary_key_columns_json": json.dumps(list(record.primary_key_columns), ensure_ascii=True, sort_keys=True),
            "primary_score_column": record.primary_score_column,
            "truth_source_description": record.truth_source_description,
            "required_columns_json": json.dumps(list(record.required_columns), ensure_ascii=True, sort_keys=True),
            "updated_at": updated_at,
        }
        for record in iter_read_model_catalog_records()
    ]
    catalog_df = pd.DataFrame(catalog_rows)
    con.execute("DROP TABLE IF EXISTS ui.read_model_catalog")
    con.execute("CREATE OR REPLACE TABLE ui.read_model_catalog AS SELECT * FROM catalog_df")
    table_row_counts["ui.read_model_catalog"] = int(len(catalog_df.index))

    check_rows = _build_truth_source_check_rows(con, checked_at=updated_at)
    checks_df = pd.DataFrame(check_rows)
    con.execute("DROP TABLE IF EXISTS ui.truth_source_checks")
    con.execute("CREATE OR REPLACE TABLE ui.truth_source_checks AS SELECT * FROM checks_df")
    table_row_counts["ui.truth_source_checks"] = int(len(checks_df.index))


def _build_truth_source_check_rows(con, *, checked_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in surface_truth_check_specs():
        for table_name in spec.table_names:
            issues: list[str] = []
            status = "ok"
            record = get_read_model_catalog_record(table_name)
            if record is None:
                issues.append(f"catalog_missing:{table_name}")
                status = "fail"
            elif not _table_exists(con, table_name):
                issues.append(f"table_missing:{table_name}")
                status = "fail"
            else:
                columns = _table_columns(con, table_name)
                missing_columns = [column for column in record.required_columns if column not in columns]
                if missing_columns:
                    issues.append(f"missing_columns:{','.join(missing_columns)}")
                    status = "fail"
                if record.primary_score_column == "ranking_score":
                    if "ranking_score" not in columns:
                        issues.append("primary_score_missing:ranking_score")
                        status = "fail"
                    if "primary_score_label" not in columns:
                        issues.append("primary_score_label_missing")
                        status = "fail"
                row_count = _table_row_count(con, table_name)
                if status == "ok" and row_count == 0:
                    issues.append("table_empty")
                    status = "warn"
            rows.append(
                {
                    "check_id": str(uuid.uuid4()),
                    "surface_id": spec.surface_id,
                    "table_name": table_name,
                    "check_status": status,
                    "issues_json": json.dumps(issues, ensure_ascii=True, sort_keys=True),
                    "checked_at": checked_at,
                }
            )
    return rows


def _table_exists(con, table_name: str) -> bool:
    schema_name, _, short_name = table_name.partition(".")
    if not short_name:
        schema_name = "main"
        short_name = schema_name
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema_name, short_name],
    ).fetchone()
    return row is not None


def _table_columns(con, table_name: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}


def _table_row_count(con, table_name: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row is not None else 0
