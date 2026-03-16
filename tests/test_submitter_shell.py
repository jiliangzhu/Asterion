from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import SubmitterBoundaryInputs
from asterion_core.execution import (
    DisabledSubmitterBackend,
    ShadowSubmitterBackend,
    ShadowFillMode,
    SubmitMode,
    SubmitterServiceShell,
    build_external_fill_observations,
    build_external_order_observation,
    build_submit_attempt_from_signed_payload,
    build_submit_order_request_from_sign_attempt,
    external_fill_observation_to_row,
    external_order_observation_to_row,
)
from asterion_core.monitoring import build_live_side_effect_guard
from asterion_core.signer import SubmitAttemptRecord
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one


def _signed_attempt() -> SubmitAttemptRecord:
    return SubmitAttemptRecord(
        attempt_id="satt_sign_1",
        request_id="sigreq_1",
        ticket_id="tt_1",
        order_id="ordr_1",
        wallet_id="wallet_weather_1",
        execution_context_id="ectx_1",
        exchange="polymarket_clob",
        attempt_kind="sign_order",
        attempt_mode="sign_only",
        canonical_order_hash="coh_1",
        payload_hash="phash_sign_1",
        submit_payload_json={
            "exchange": "polymarket_clob",
            "attempt_kind": "sign_order",
            "attempt_mode": "sign_only",
            "backend_kind": "official_stub",
            "signed": True,
            "error": None,
            "request_id": "sigreq_1",
            "ticket_id": "tt_1",
            "execution_context_id": "ectx_1",
            "canonical_order_hash": "coh_1",
            "order": {
                "market_id": "mkt_weather_1",
                "token_id": "tok_yes",
                "outcome": "YES",
                "side": "buy",
                "price": "0.63",
                "size": "10",
                "fee_rate_bps": 30,
            },
            "signature": "stubsig_1",
        },
        signed_payload_ref="satt_sign_1",
        status="signed",
        error=None,
        created_at=datetime(2026, 3, 12, 10, 0),
    )


class SubmitterShellUnitTest(unittest.TestCase):
    class _ExplodingBackend:
        def backend_kind(self) -> str:
            return "real_clob_submit"

        def endpoint_fingerprint(self) -> str | None:
            return "submitter-fingerprint"

        def submit(self, request, *, boundary_attestation=None):  # noqa: ANN001
            raise AssertionError("backend should not be called")

    class _RecordingBackend:
        def __init__(self) -> None:
            self.called = False
            self.last_attestation = None

        def backend_kind(self) -> str:
            return "real_clob_submit"

        def endpoint_fingerprint(self) -> str | None:
            return "submitter-fingerprint"

        def submit(self, request, *, boundary_attestation=None):  # noqa: ANN001
            self.called = True
            self.last_attestation = boundary_attestation
            return ShadowSubmitterBackend().submit(request)

    def test_build_submit_order_request_from_sign_attempt(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_1",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.DRY_RUN,
        )
        self.assertEqual(request.source_attempt_id, "satt_sign_1")
        self.assertEqual(request.submit_mode, SubmitMode.DRY_RUN)
        self.assertEqual(request.wallet_id, "wallet_weather_1")

    def test_submitter_rejects_shadow_fill_mode_for_dry_run(self) -> None:
        with self.assertRaises(ValueError):
            build_submit_order_request_from_sign_attempt(
                _signed_attempt(),
                requester="operator",
                request_id="subreq_1",
                timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
                submit_mode=SubmitMode.DRY_RUN,
                shadow_fill_mode=ShadowFillMode.PARTIAL,
            )

    def test_submitter_rejects_non_sign_only_attempt(self) -> None:
        invalid = _signed_attempt()
        invalid = SubmitAttemptRecord(**{**invalid.__dict__, "attempt_mode": "shadow_submit"})
        with self.assertRaises(ValueError):
            build_submit_order_request_from_sign_attempt(
                invalid,
                requester="operator",
                request_id="subreq_1",
                timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
                submit_mode=SubmitMode.DRY_RUN,
            )

    def test_disabled_backend_rejects_submit(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_1",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.SHADOW_SUBMIT,
        )
        result = DisabledSubmitterBackend().submit(request)
        self.assertEqual(result.status, "rejected")
        self.assertEqual(result.error, "submitter_backend_disabled")

    def test_shadow_submitter_accepts_stub_submit(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_1",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.SHADOW_SUBMIT,
        )
        result = ShadowSubmitterBackend().submit(request)
        self.assertEqual(result.status, "accepted")
        self.assertIsNotNone(result.external_order_id)

    def test_shadow_submitter_rejects_marked_payload(self) -> None:
        attempt = _signed_attempt()
        payload = dict(attempt.submit_payload_json)
        payload["shadow_reject"] = True
        attempt = SubmitAttemptRecord(**{**attempt.__dict__, "submit_payload_json": payload})
        request = build_submit_order_request_from_sign_attempt(
            attempt,
            requester="operator",
            request_id="subreq_1",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.SHADOW_SUBMIT,
        )
        result = ShadowSubmitterBackend().submit(request)
        self.assertEqual(result.status, "rejected")

    def test_dry_run_generates_preview_and_observation(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_1",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.DRY_RUN,
        )
        shell = SubmitterServiceShell(ShadowSubmitterBackend())
        with patch("asterion_core.execution.live_submitter_v1.enqueue_journal_event_upserts", return_value="task_journal"):
            invocation = shell.submit_order(request, queue_cfg=WriteQueueConfig(path=":memory:"), run_id="run_submit")
        self.assertEqual(invocation.response.status, "previewed")
        attempt = build_submit_attempt_from_signed_payload(request, invocation.response)
        observation = build_external_order_observation(attempt, observed_at=invocation.response.completed_at)
        self.assertEqual(attempt.attempt_kind, "submit_order")
        self.assertEqual(attempt.attempt_mode, "dry_run")
        self.assertEqual(observation.external_status, "preview")
        self.assertEqual(external_order_observation_to_row(observation)[0], observation.observation_id)

    def test_shadow_submit_generates_external_fill_observation(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_1",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.SHADOW_SUBMIT,
            shadow_fill_mode=ShadowFillMode.PARTIAL,
        )
        shell = SubmitterServiceShell(ShadowSubmitterBackend())
        with patch("asterion_core.execution.live_submitter_v1.enqueue_journal_event_upserts", return_value="task_journal"):
            invocation = shell.submit_order(request, queue_cfg=WriteQueueConfig(path=":memory:"), run_id="run_submit")
        attempt = build_submit_attempt_from_signed_payload(request, invocation.response)
        observations = build_external_fill_observations(attempt, observed_at=invocation.response.completed_at)
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].external_status, "partial_filled")
        self.assertEqual(external_fill_observation_to_row(observations[0])[0], observations[0].observation_id)

    def test_submitter_shell_has_no_arbitrary_submit_api(self) -> None:
        self.assertFalse(hasattr(SubmitterServiceShell(), "submit_message"))

    def test_live_submit_without_guard_is_rejected_before_backend(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_live_1",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.LIVE_SUBMIT,
        )
        shell = SubmitterServiceShell(self._ExplodingBackend())
        with (
            patch("asterion_core.execution.live_submitter_v1.enqueue_journal_event_upserts", return_value="task_journal"),
            patch(
                "asterion_core.execution.live_submitter_v1.enqueue_live_boundary_attestation_upserts",
                return_value="task_attestation",
            ),
        ):
            invocation = shell.submit_order(
                request,
                queue_cfg=WriteQueueConfig(path=":memory:"),
                run_id="run_submit_live",
            )
        self.assertEqual(invocation.response.status, "rejected")
        self.assertEqual(invocation.response.error, "boundary_inputs_missing")

    def test_live_submit_with_not_armed_guard_is_rejected_before_backend(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_live_2",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.LIVE_SUBMIT,
        )
        shell = SubmitterServiceShell(self._ExplodingBackend())
        with (
            patch("asterion_core.execution.live_submitter_v1.enqueue_journal_event_upserts", return_value="task_journal"),
            patch(
                "asterion_core.execution.live_submitter_v1.enqueue_live_boundary_attestation_upserts",
                return_value="task_attestation",
            ),
        ):
            invocation = shell.submit_order(
                request,
                live_guard=build_live_side_effect_guard(mode="live_submit", armed=False),
                boundary_inputs=_live_boundary_inputs(request_id=request.request_id),
                queue_cfg=WriteQueueConfig(path=":memory:"),
                run_id="run_submit_live",
            )
        self.assertEqual(invocation.response.status, "rejected")
        self.assertEqual(invocation.response.error, "live_submit_not_armed")

    def test_live_submit_without_boundary_inputs_is_rejected_before_backend(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_live_3",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.LIVE_SUBMIT,
        )
        shell = SubmitterServiceShell(self._ExplodingBackend())
        with (
            patch("asterion_core.execution.live_submitter_v1.enqueue_journal_event_upserts", return_value="task_journal"),
            patch(
                "asterion_core.execution.live_submitter_v1.enqueue_live_boundary_attestation_upserts",
                return_value="task_attestation",
            ),
        ):
            invocation = shell.submit_order(
                request,
                live_guard=build_live_side_effect_guard(mode="live_submit", armed=True),
                queue_cfg=WriteQueueConfig(path=":memory:"),
                run_id="run_submit_live",
            )
        self.assertEqual(invocation.response.status, "rejected")
        self.assertEqual(invocation.response.error, "boundary_inputs_missing")

    def test_live_submit_with_approved_attestation_calls_backend(self) -> None:
        request = build_submit_order_request_from_sign_attempt(
            _signed_attempt(),
            requester="operator",
            request_id="subreq_live_4",
            timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            submit_mode=SubmitMode.LIVE_SUBMIT,
        )
        backend = self._RecordingBackend()
        shell = SubmitterServiceShell(backend)
        with (
            patch("asterion_core.execution.live_submitter_v1.enqueue_journal_event_upserts", return_value="task_journal"),
            patch(
                "asterion_core.execution.live_submitter_v1.enqueue_live_boundary_attestation_upserts",
                return_value="task_attestation",
            ),
        ):
            invocation = shell.submit_order(
                request,
                live_guard=build_live_side_effect_guard(mode="live_submit", armed=True),
                boundary_inputs=_live_boundary_inputs(request_id=request.request_id),
                queue_cfg=WriteQueueConfig(path=":memory:"),
                run_id="run_submit_live",
            )
        self.assertEqual(invocation.response.status, "accepted")
        self.assertTrue(backend.called)
        self.assertIsNotNone(backend.last_attestation)
        self.assertEqual(backend.last_attestation.attestation_status, "approved")


class SubmitterShellDuckDBTest(unittest.TestCase):
    def test_submitter_shell_persists_only_journal_from_shell(self) -> None:
        root = Path(__file__).resolve().parents[1]
        migrations_dir = root / "sql" / "migrations"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "writer",
                    "WRITERD": "1",
                },
                clear=False,
            ):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=str(migrations_dir)))

            queue_cfg = WriteQueueConfig(path=queue_path)
            request = build_submit_order_request_from_sign_attempt(
                _signed_attempt(),
                requester="operator",
                request_id="subreq_1",
                timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
                submit_mode=SubmitMode.DRY_RUN,
            )
            shell = SubmitterServiceShell(ShadowSubmitterBackend())
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": "runtime.journal_events,runtime.submit_attempts,runtime.external_order_observations,runtime.live_boundary_attestations",
                },
                clear=False,
            ):
                shell.submit_order(request, queue_cfg=queue_cfg, run_id="run_submit")
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass
            con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
            try:
                self.assertEqual(
                    con.execute("SELECT COUNT(*) FROM runtime.journal_events WHERE entity_type = 'submit_request'").fetchone()[0],
                    2,
                )
                self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.submit_attempts").fetchone()[0], 0)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.external_order_observations").fetchone()[0], 0)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.live_boundary_attestations").fetchone()[0], 0)
            finally:
                con.close()

    def test_live_submit_persists_boundary_attestation(self) -> None:
        root = Path(__file__).resolve().parents[1]
        migrations_dir = root / "sql" / "migrations"
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "writer",
                    "WRITERD": "1",
                },
                clear=False,
            ):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=str(migrations_dir)))

            queue_cfg = WriteQueueConfig(path=queue_path)
            request = build_submit_order_request_from_sign_attempt(
                _signed_attempt(),
                requester="operator",
                request_id="subreq_live_duckdb_1",
                timestamp=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
                submit_mode=SubmitMode.LIVE_SUBMIT,
            )
            shell = SubmitterServiceShell(ShadowSubmitterBackend())
            with patch.dict(
                "os.environ",
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": "runtime.journal_events,runtime.live_boundary_attestations",
                },
                clear=False,
            ):
                shell.submit_order(
                    request,
                    live_guard=build_live_side_effect_guard(mode="live_submit", armed=True),
                    boundary_inputs=_live_boundary_inputs(
                        request_id=request.request_id,
                        submitter_backend_kind="shadow_stub",
                    ),
                    queue_cfg=queue_cfg,
                    run_id="run_submit_live",
                )
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass
            con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
            try:
                row = con.execute(
                    "SELECT attestation_status, reason_codes_json FROM runtime.live_boundary_attestations"
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(row[0], "blocked")
            self.assertIn("submitter_backend_not_real_clob_submit", row[1])


def _live_boundary_inputs(
    *,
    request_id: str,
    submitter_backend_kind: str = "real_clob_submit",
    wallet_id: str = "wallet_weather_1",
) -> SubmitterBoundaryInputs:
    return SubmitterBoundaryInputs(
        request_id=request_id,
        wallet_id=wallet_id,
        source_attempt_id="satt_sign_1",
        ticket_id="tt_1",
        execution_context_id="ectx_1",
        submit_mode="live_submit",
        submitter_backend_kind=submitter_backend_kind,
        signer_backend_kind="env_private_key_tx",
        chain_tx_backend_kind="real_broadcast",
        submitter_endpoint_fingerprint="submitter-fingerprint",
        manifest_payload={
            "manifest_status": "valid",
            "controlled_live_mode": "manual_only",
            "allowed_wallet_ids": [wallet_id],
        },
        manifest_path="data/meta/controlled_live_capability_manifest.json",
        readiness_report_payload={"go_decision": "GO"},
        wallet_readiness_status="ready",
        approval_token_matches=True,
        armed=True,
        evaluated_at=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
    )


if __name__ == "__main__":
    unittest.main()
