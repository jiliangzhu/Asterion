from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from ui.loaders.shared_io import read_ui_table, resolve_ui_lite_db_path


@dataclass(frozen=True)
class SourceBadgePolicy:
    family: tuple[str, ...] = ("canonical", "fallback", "stale", "degraded", "derived")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PrimaryScorePolicy:
    primary_score_field: str = "ranking_score"
    primary_score_label: str = "Ranking Score"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SurfaceLoaderContract:
    surface_id: str
    primary_dataframe_name: str
    supporting_payload: dict[str, Any]
    truth_source_summary: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_truth_source_summary(
    *,
    surface_id: str,
    primary_table: str,
    source: str,
    supports_source_badges: bool,
) -> dict[str, Any]:
    db_path = resolve_ui_lite_db_path()
    catalog = read_ui_table(db_path, "ui.read_model_catalog")
    checks = read_ui_table(db_path, "ui.truth_source_checks")
    schema_versions: dict[str, str] = {}
    check_rows: list[dict[str, Any]] = []
    if not catalog.empty and "table_name" in catalog.columns:
        matching = catalog[catalog["table_name"] == primary_table]
        for _, row in matching.iterrows():
            schema_versions[str(row["table_name"])] = str(row.get("schema_version") or "")
    if not checks.empty and {"surface_id", "table_name"} <= set(checks.columns):
        matching_checks = checks[(checks["surface_id"] == surface_id)]
        for _, row in matching_checks.iterrows():
            check_rows.append(
                {
                    "table_name": row.get("table_name"),
                    "check_status": row.get("check_status"),
                    "issues": _json_list(row.get("issues_json")),
                }
            )
    return {
        "primary_table": primary_table,
        "source": source,
        "primary_score_label": PrimaryScorePolicy().primary_score_label,
        "supports_source_badges": supports_source_badges,
        "schema_versions": schema_versions,
        "truth_checks": check_rows,
        "source_badge_policy": SourceBadgePolicy().as_dict(),
        "primary_score_policy": PrimaryScorePolicy().as_dict(),
    }


def validate_surface_loader_contract(contract: SurfaceLoaderContract) -> None:
    if contract.primary_dataframe_name not in contract.supporting_payload:
        raise ValueError(f"missing primary payload: {contract.primary_dataframe_name}")
    summary = contract.truth_source_summary
    required = {"primary_table", "source", "primary_score_label", "supports_source_badges", "schema_versions"}
    missing = [name for name in required if name not in summary]
    if missing:
        raise ValueError(f"missing truth source summary fields: {','.join(missing)}")


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in {None, ""}:
        return []
    try:
        payload = json.loads(str(value))
    except Exception:  # noqa: BLE001
        return []
    return payload if isinstance(payload, list) else []
