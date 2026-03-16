from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb

from asterion_core.contracts import build_submitter_boundary_attestation
from asterion_core.execution import RealClobSubmitterBackend, SubmitMode, SubmitterServiceShell
from asterion_core.monitoring import (
    ReadinessGateResult,
    ReadinessReport,
    ReadinessTarget,
    build_controlled_live_capability_manifest,
    write_controlled_live_capability_manifest,
)
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import run_weather_submitter_smoke_job
from tests.test_p2_closeout import _apply_schema
from tests.test_submitter_shell import _signed_attempt


class _AcceptedClient:
    def post_json(self, url: str, *, payload: dict[str, object]) -> dict[str, object]:
        return {"status": "accepted", "external_order_id": "ext_live_1", "echo_request_id": payload["request_id"]}


class _RejectedClient:
    def post_json(self, url: str, *, payload: dict[str, object]) -> dict[str, object]:
        return {"status": "rejected", "error": "provider_rejected"}


class _BrokenClient:
    def post_json(self, url: str, *, payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("network_down")


class _ExplodingClient:
    def post_json(self, url: str, *, payload: dict[str, object]) -> dict[str, object]:
        raise AssertionError("provider client should not be called")


class RealClobSubmitterBackendTest(unittest.TestCase):
    def test_real_backend_accepts_live_submit(self) -> None:
        backend = RealClobSubmitterBackend(api_base_url="https://submit.invalid/orders", client=_AcceptedClient())
        request = _build_live_submit_request()
        result = backend.submit(request, boundary_attestation=_approved_attestation(backend, request))
        self.assertEqual(result.status, "accepted")
        self.assertEqual(result.external_order_id, "ext_live_1")
        self.assertEqual(result.submit_payload_json["backend_kind"], "real_clob_submit")

    def test_real_backend_rejects_provider_error_without_raising(self) -> None:
        backend = RealClobSubmitterBackend(api_base_url="https://submit.invalid/orders", client=_BrokenClient())
        request = _build_live_submit_request()
        result = backend.submit(request, boundary_attestation=_approved_attestation(backend, request))
        self.assertEqual(result.status, "rejected")
        self.assertIn("submitter_provider_error:", result.error or "")

    def test_real_backend_requires_live_submit_mode(self) -> None:
        backend = RealClobSubmitterBackend(api_base_url="https://submit.invalid/orders", client=_AcceptedClient())
        request = _build_live_submit_request(submit_mode=SubmitMode.SHADOW_SUBMIT)
        result = backend.submit(request)
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.error, "real_submitter_requires_submit_mode_live_submit")

    def test_real_backend_rejects_missing_attestation(self) -> None:
        backend = RealClobSubmitterBackend(api_base_url="https://submit.invalid/orders", client=_AcceptedClient())
        request = _build_live_submit_request()
        result = backend.submit(request)
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.error, "submitter_boundary_attestation_missing")

    def test_real_backend_rejects_non_approved_attestation(self) -> None:
        backend = RealClobSubmitterBackend(api_base_url="https://submit.invalid/orders", client=_AcceptedClient())
        request = _build_live_submit_request()
        result = backend.submit(
            request,
            boundary_attestation=build_submitter_boundary_attestation(
                request_id=request.request_id,
                wallet_id=request.wallet_id,
                submit_mode=request.submit_mode.value,
                target_backend_kind="real_clob_submit",
                submitter_endpoint_fingerprint=backend.endpoint_fingerprint(),
                manifest_payload=None,
                readiness_report_payload=None,
                reason_codes=["manifest_missing"],
                created_at=request.timestamp,
            ),
        )
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.error, "submitter_boundary_attestation_not_approved")

    def test_real_backend_rejects_endpoint_fingerprint_mismatch(self) -> None:
        backend = RealClobSubmitterBackend(api_base_url="https://submit.invalid/orders", client=_AcceptedClient())
        request = _build_live_submit_request()
        result = backend.submit(
            request,
            boundary_attestation=build_submitter_boundary_attestation(
                request_id=request.request_id,
                wallet_id=request.wallet_id,
                submit_mode=request.submit_mode.value,
                target_backend_kind="real_clob_submit",
                submitter_endpoint_fingerprint="wrong",
                manifest_payload={"manifest_status": "valid"},
                readiness_report_payload={"go_decision": "GO"},
                reason_codes=[],
                created_at=request.timestamp,
            ),
        )
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.error, "submitter_endpoint_fingerprint_mismatch")


class LiveSubmitterSmokeHandlerTest(unittest.TestCase):
    def test_live_submit_without_manifest_is_rejected_and_attested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_submitter_env(tmpdir)
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_WRITERD_ALLOWED_TABLES": "runtime.journal_events,runtime.submit_attempts,runtime.external_order_observations,runtime.live_boundary_attestations",
                    },
                    clear=False,
                ):
                    result = run_weather_submitter_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        submitter_service=SubmitterServiceShell(
                            RealClobSubmitterBackend(
                                api_base_url="https://submit.invalid/orders",
                                client=_AcceptedClient(),
                            )
                        ),
                        controlled_live_capability_manifest_path=str(Path(tmpdir) / "missing_manifest.json"),
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        params_json={
                            "attempt_ids": ["sign_1"],
                            "requester": "operator",
                            "submit_mode": "live_submit",
                            "approval_token": "live-token",
                        },
                        run_id="run_live_submit_blocked",
                        observed_at=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
                    )
                    while process_one(queue_path=env["queue_cfg"].path, db_path=env["db_path"], ddl_path=None, apply_schema=False):
                        pass
            finally:
                env["con"].close()
            self.assertEqual(result.metadata["rejected_count"], 1)
            con = connect_duckdb(DuckDBConfig(db_path=env["db_path"], ddl_path=None))
            try:
                row = con.execute(
                    "SELECT attestation_status, reason_codes_json FROM runtime.live_boundary_attestations"
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(row[0], "blocked")
            self.assertIn("manifest_missing", row[1])

    def test_live_submit_persists_submit_attempts_external_observations_and_attestation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_submitter_env(tmpdir, manifest_submitter_backend_kind="real_clob_submit")
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "true",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        "ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1": "0xabc",
                        "ASTERION_WRITERD_ALLOWED_TABLES": "runtime.journal_events,runtime.submit_attempts,runtime.external_order_observations,runtime.live_boundary_attestations",
                    },
                    clear=False,
                ):
                    result = run_weather_submitter_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        submitter_service=SubmitterServiceShell(
                            RealClobSubmitterBackend(
                                api_base_url="https://submit.invalid/orders",
                                client=_AcceptedClient(),
                            )
                        ),
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        params_json={
                            "attempt_ids": ["sign_1"],
                            "requester": "operator",
                            "submit_mode": "live_submit",
                            "approval_token": "live-token",
                        },
                        run_id="run_live_submit_ok",
                        observed_at=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
                    )
                    while process_one(queue_path=env["queue_cfg"].path, db_path=env["db_path"], ddl_path=None, apply_schema=False):
                        pass
            finally:
                env["con"].close()
            self.assertEqual(result.metadata["submit_mode"], "live_submit")
            self.assertEqual(result.metadata["accepted_count"], 1)
            con = connect_duckdb(DuckDBConfig(db_path=env["db_path"], ddl_path=None))
            try:
                attempt_row = con.execute(
                    "SELECT attempt_mode, status FROM runtime.submit_attempts WHERE attempt_kind = 'submit_order'"
                ).fetchone()
                observation_row = con.execute(
                    "SELECT observation_kind, external_status FROM runtime.external_order_observations"
                ).fetchone()
                attestation_row = con.execute(
                    "SELECT attestation_status FROM runtime.live_boundary_attestations"
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(attempt_row, ("live_submit", "accepted"))
            self.assertEqual(observation_row, ("live_submit_ack", "accepted"))
            self.assertEqual(attestation_row, ("approved",))

    def test_live_submit_not_armed_is_rejected_before_provider_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_submitter_env(tmpdir, manifest_submitter_backend_kind="real_clob_submit")
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "false",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        "ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1": "0xabc",
                        "ASTERION_WRITERD_ALLOWED_TABLES": "runtime.journal_events,runtime.submit_attempts,runtime.external_order_observations,runtime.live_boundary_attestations",
                    },
                    clear=False,
                ):
                    result = run_weather_submitter_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        submitter_service=SubmitterServiceShell(
                            RealClobSubmitterBackend(
                                api_base_url="https://submit.invalid/orders",
                                client=_ExplodingClient(),
                            )
                        ),
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        params_json={
                            "attempt_ids": ["sign_1"],
                            "requester": "operator",
                            "submit_mode": "live_submit",
                            "approval_token": "live-token",
                        },
                        run_id="run_live_submit_not_armed",
                        observed_at=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
                    )
                    while process_one(queue_path=env["queue_cfg"].path, db_path=env["db_path"], ddl_path=None, apply_schema=False):
                        pass
            finally:
                env["con"].close()
            self.assertEqual(result.metadata["rejected_count"], 1)
            con = connect_duckdb(DuckDBConfig(db_path=env["db_path"], ddl_path=None))
            try:
                attempt = con.execute(
                    "SELECT status, error FROM runtime.submit_attempts WHERE attempt_kind = 'submit_order'"
                ).fetchone()
                attestation = con.execute(
                    "SELECT attestation_status, reason_codes_json FROM runtime.live_boundary_attestations"
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(attempt, ("rejected", "live_submit_not_armed"))
            self.assertEqual(attestation[0], "blocked")
            self.assertIn("live_submit_not_armed", attestation[1])


def _build_live_submit_request(*, submit_mode: SubmitMode = SubmitMode.LIVE_SUBMIT):
    from asterion_core.execution import build_submit_order_request_from_sign_attempt

    return build_submit_order_request_from_sign_attempt(
        _signed_attempt(),
        requester="operator",
        request_id="subreq_live_1",
        timestamp=datetime(2026, 3, 15, 10, 0, tzinfo=UTC),
        submit_mode=submit_mode,
    )


def _approved_attestation(backend: RealClobSubmitterBackend, request):
    return build_submitter_boundary_attestation(
        request_id=request.request_id,
        wallet_id=request.wallet_id,
        submit_mode=request.submit_mode.value,
        target_backend_kind=backend.backend_kind(),
        submitter_endpoint_fingerprint=backend.endpoint_fingerprint(),
        manifest_payload={"manifest_status": "valid"},
        readiness_report_payload={"go_decision": "GO"},
        reason_codes=[],
        created_at=request.timestamp,
    )


def _seed_submitter_env(tmpdir: str, *, manifest_submitter_backend_kind: str = "shadow_stub") -> dict[str, object]:
    db_path = str(Path(tmpdir) / "asterion.duckdb")
    ui_lite_db_path = str(Path(tmpdir) / "ui_lite.duckdb")
    queue_path = str(Path(tmpdir) / "write_queue.sqlite")
    readiness_path = str(Path(tmpdir) / "readiness.json")
    manifest_path = str(Path(tmpdir) / "manifest.json")
    policy_path = Path(tmpdir) / "controlled_live_smoke.json"
    _apply_schema(db_path)
    con = duckdb.connect(db_path)
    con.execute(
        """
        INSERT INTO runtime.submit_attempts (
            attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id, exchange,
            attempt_kind, attempt_mode, canonical_order_hash, payload_hash, submit_payload_json,
            signed_payload_ref, status, error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "sign_1",
            "sigreq_1",
            "tt_1",
            "ordr_1",
            "wallet_weather_1",
            "ectx_1",
            "polymarket_clob",
            "sign_order",
            "sign_only",
            "coh_1",
            "phash_sign_1",
            json.dumps(_signed_attempt().submit_payload_json),
            "sign_1",
            "signed",
            None,
            datetime(2026, 3, 15, 9, 55),
        ],
    )
    lite = duckdb.connect(ui_lite_db_path)
    lite.execute("CREATE SCHEMA IF NOT EXISTS ui")
    lite.execute(
        """
        CREATE TABLE ui.live_prereq_wallet_summary (
            wallet_id TEXT,
            wallet_readiness_status TEXT
        )
        """
    )
    lite.execute("INSERT INTO ui.live_prereq_wallet_summary VALUES ('wallet_weather_1', 'ready')")
    lite.close()

    report = ReadinessReport(
        target=ReadinessTarget.P4_LIVE_PREREQUISITES,
        generated_at=datetime(2026, 3, 15, 9, 50, tzinfo=UTC),
        all_passed=True,
        go_decision="GO",
        decision_reason="ready for controlled live rollout decision",
        data_hash="hash_live_1",
        gate_results=[ReadinessGateResult(gate_name="submitter_shadow_path", passed=True, checks={}, violations=[], warnings=[], metadata={})],
        capability_boundary_summary={
            "manual_only": True,
            "default_off": True,
            "approve_usdc_only": True,
            "shadow_submitter_only": manifest_submitter_backend_kind == "shadow_stub",
            "constrained_real_submit_enabled": manifest_submitter_backend_kind == "real_clob_submit",
            "manifest_status": "valid",
        },
        capability_manifest_path=manifest_path,
        capability_manifest_status="valid",
    )
    Path(readiness_path).write_text(json.dumps(report.to_dict(), ensure_ascii=True), encoding="utf-8")
    policy_path.write_text(
        json.dumps(
            {
                "chain_id": 137,
                "wallets": [
                    {
                        "wallet_id": "wallet_weather_1",
                        "allowed_tx_kinds": ["approve_usdc"],
                        "allowed_spenders": ["0x2222222222222222222222222222222222222222"],
                        "max_approve_amount": "100",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    with patch.dict(
        os.environ,
        {
            "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "true",
            "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
            "ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1": "0xabc",
        },
        clear=False,
    ):
        manifest = build_controlled_live_capability_manifest(
            policy_path=policy_path,
            signer_backend_kind="env_private_key_tx",
            chain_tx_backend_kind="real_broadcast",
            submitter_backend_kind=manifest_submitter_backend_kind,
        )
    write_controlled_live_capability_manifest(manifest, path=manifest_path)
    return {
        "con": con,
        "db_path": db_path,
        "ui_lite_db_path": ui_lite_db_path,
        "queue_cfg": WriteQueueConfig(path=queue_path),
        "readiness_path": readiness_path,
        "manifest_path": manifest_path,
    }


if __name__ == "__main__":
    unittest.main()
