from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from asterion_core.monitoring.readiness_checker_v1 import ReadinessReport


DEFAULT_READINESS_EVIDENCE_JSON_PATH = "data/ui/asterion_readiness_evidence_p4.json"
_STALE_AFTER_SECONDS = 6 * 60 * 60


@dataclasses.dataclass(frozen=True)
class ReadinessEvidenceBundle:
    schema_version: str
    generated_at: datetime
    go_decision: str
    decision_reason: str
    capability_boundary_summary: dict[str, Any]
    capability_manifest_path: str | None
    capability_manifest_status: str | None
    dependency_statuses: dict[str, Any]
    artifact_freshness: dict[str, Any]
    latest_verification_summary: dict[str, Any]
    stale_dependencies: list[str]
    blockers: list[str]
    warnings: list[str]
    evidence_paths: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": _iso_utc(self.generated_at),
            "go_decision": self.go_decision,
            "decision_reason": self.decision_reason,
            "capability_boundary_summary": dict(self.capability_boundary_summary),
            "capability_manifest_path": self.capability_manifest_path,
            "capability_manifest_status": self.capability_manifest_status,
            "dependency_statuses": dict(self.dependency_statuses),
            "artifact_freshness": dict(self.artifact_freshness),
            "latest_verification_summary": dict(self.latest_verification_summary),
            "stale_dependencies": list(self.stale_dependencies),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "evidence_paths": dict(self.evidence_paths),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReadinessEvidenceBundle":
        return cls(
            schema_version=str(payload.get("schema_version") or "v1"),
            generated_at=_parse_datetime(str(payload["generated_at"])),
            go_decision=str(payload.get("go_decision") or "NO_GO"),
            decision_reason=str(payload.get("decision_reason") or ""),
            capability_boundary_summary=dict(payload.get("capability_boundary_summary") or {}),
            capability_manifest_path=str(payload["capability_manifest_path"]) if payload.get("capability_manifest_path") else None,
            capability_manifest_status=str(payload["capability_manifest_status"]) if payload.get("capability_manifest_status") else None,
            dependency_statuses=dict(payload.get("dependency_statuses") or {}),
            artifact_freshness=dict(payload.get("artifact_freshness") or {}),
            latest_verification_summary=dict(payload.get("latest_verification_summary") or {}),
            stale_dependencies=[str(item) for item in payload.get("stale_dependencies", [])],
            blockers=[str(item) for item in payload.get("blockers", [])],
            warnings=[str(item) for item in payload.get("warnings", [])],
            evidence_paths={str(key): str(value) for key, value in dict(payload.get("evidence_paths") or {}).items()},
        )


def build_readiness_evidence_bundle(
    report: ReadinessReport,
    *,
    readiness_report_json_path: str,
    readiness_report_markdown_path: str,
    capability_manifest_path: str | None = None,
    ui_lite_db_path: str | None = None,
    ui_lite_meta_path: str | None = None,
    ui_replica_db_path: str | None = None,
    ui_replica_meta_path: str | None = None,
    weather_smoke_report_path: str | None = None,
    weather_smoke_db_path: str | None = None,
) -> ReadinessEvidenceBundle:
    generated_at = datetime.now(UTC)
    evidence_paths = {
        "readiness_report_json": readiness_report_json_path,
        "readiness_report_markdown": readiness_report_markdown_path,
    }
    if capability_manifest_path:
        evidence_paths["capability_manifest"] = capability_manifest_path
    if ui_lite_db_path:
        evidence_paths["ui_lite_db"] = ui_lite_db_path
    if ui_lite_meta_path:
        evidence_paths["ui_lite_meta"] = ui_lite_meta_path
    if ui_replica_db_path:
        evidence_paths["ui_replica_db"] = ui_replica_db_path
    if ui_replica_meta_path:
        evidence_paths["ui_replica_meta"] = ui_replica_meta_path
    if weather_smoke_report_path:
        evidence_paths["weather_smoke_report"] = weather_smoke_report_path
    if weather_smoke_db_path:
        evidence_paths["weather_smoke_db"] = weather_smoke_db_path

    dependency_statuses: dict[str, Any] = {}
    artifact_freshness: dict[str, Any] = {}
    stale_dependencies: list[str] = []
    warnings: list[str] = []
    blockers = [f"gate:{gate.gate_name}" for gate in report.gate_results if not gate.passed]

    for name, raw_path in evidence_paths.items():
        info = _artifact_info(Path(raw_path), generated_at=generated_at)
        dependency_statuses[name] = info["dependency_status"]
        artifact_freshness[name] = info["freshness"]
        if info["dependency_status"]["status"] == "missing":
            if name in {"capability_manifest", "readiness_report_json", "readiness_report_markdown"}:
                blockers.append(f"missing:{name}")
            else:
                warnings.append(f"missing:{name}")
        elif info["dependency_status"]["status"] == "stale":
            stale_dependencies.append(name)
            warnings.append(f"stale:{name}")

    manifest_status = report.capability_manifest_status or "missing"
    if manifest_status != "valid":
        blockers.append(f"capability_manifest:{manifest_status}")

    latest_verification_summary = {
        "target": report.target.value,
        "generated_at": _iso_utc(report.generated_at),
        "gate_count": len(report.gate_results),
        "failed_gate_names": [gate.gate_name for gate in report.gate_results if not gate.passed],
        "all_passed": report.all_passed,
        "data_hash": report.data_hash,
    }

    return ReadinessEvidenceBundle(
        schema_version="v1",
        generated_at=generated_at,
        go_decision=report.go_decision,
        decision_reason=report.decision_reason,
        capability_boundary_summary=dict(report.capability_boundary_summary or {}),
        capability_manifest_path=capability_manifest_path or report.capability_manifest_path,
        capability_manifest_status=manifest_status,
        dependency_statuses=dependency_statuses,
        artifact_freshness=artifact_freshness,
        latest_verification_summary=latest_verification_summary,
        stale_dependencies=stale_dependencies,
        blockers=sorted(set(blockers)),
        warnings=sorted(set(warnings)),
        evidence_paths=evidence_paths,
    )


def write_readiness_evidence_bundle(bundle: ReadinessEvidenceBundle, *, json_path: str) -> None:
    path = Path(json_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(bundle.to_dict(), ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")


def load_readiness_evidence_bundle(path: str | Path) -> ReadinessEvidenceBundle | None:
    artifact = Path(path)
    if not artifact.exists():
        return None
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("readiness evidence payload must be an object")
    return ReadinessEvidenceBundle.from_dict(payload)


def _artifact_info(path: Path, *, generated_at: datetime) -> dict[str, Any]:
    if not path.exists():
        return {
            "dependency_status": {
                "status": "missing",
                "path": str(path),
                "updated_at": None,
            },
            "freshness": {
                "path": str(path),
                "updated_at": None,
                "age_seconds": None,
            },
        }
    stat = path.stat()
    updated_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
    age_seconds = max(0.0, (generated_at - updated_at).total_seconds())
    return {
        "dependency_status": {
            "status": "stale" if age_seconds > _STALE_AFTER_SECONDS else "ok",
            "path": str(path),
            "updated_at": _iso_utc(updated_at),
        },
        "freshness": {
            "path": str(path),
            "updated_at": _iso_utc(updated_at),
            "age_seconds": age_seconds,
        },
    }


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
