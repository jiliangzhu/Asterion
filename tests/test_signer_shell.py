from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from asterion_core.contracts import AccountTradingCapability
from asterion_core.signer import (
    DisabledSignerBackend,
    SignerRequest,
    SignerServiceShell,
    SigningPurpose,
    SignatureAuditStatus,
    build_signing_context_from_account_capability,
    hash_signer_payload,
    signature_audit_log_to_row,
)
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one


def _account_capability() -> AccountTradingCapability:
    return AccountTradingCapability(
        wallet_id="wallet_weather_1",
        wallet_type="eoa",
        signature_type=1,
        funder="0x1111111111111111111111111111111111111111",
        allowance_targets=["0x2222222222222222222222222222222222222222"],
        can_use_relayer=True,
        can_trade=True,
        restricted_reason=None,
    )


class SignerShellUnitTest(unittest.TestCase):
    def test_build_signing_context_defaults_signer_address_to_funder(self) -> None:
        context = build_signing_context_from_account_capability(
            _account_capability(),
            signing_purpose=SigningPurpose.ORDER,
            chain_id=137,
            token_id="tok_yes",
            fee_rate_bps=30,
        )
        self.assertEqual(context.wallet_type.value, "eoa")
        self.assertEqual(context.signer_address, "0x1111111111111111111111111111111111111111")
        self.assertEqual(context.token_id, "tok_yes")
        self.assertEqual(context.fee_rate_bps, 30)

    def test_order_context_requires_token_id_and_fee_rate(self) -> None:
        with self.assertRaises(ValueError):
            build_signing_context_from_account_capability(
                _account_capability(),
                signing_purpose=SigningPurpose.ORDER,
                chain_id=137,
            )

    def test_hash_signer_payload_is_stable(self) -> None:
        payload = {"kind": "signer_smoke", "order_id": "ordr_test"}
        self.assertEqual(hash_signer_payload(payload), hash_signer_payload(dict(payload)))

    def test_signer_request_rejects_empty_payload(self) -> None:
        context = build_signing_context_from_account_capability(
            _account_capability(),
            signing_purpose=SigningPurpose.TRANSACTION,
            chain_id=137,
        )
        with self.assertRaises(ValueError):
            SignerRequest(
                request_id="req_1",
                requester="operator",
                timestamp=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
                context=context,
                payload={},
            )

    def test_disabled_backend_always_rejects(self) -> None:
        context = build_signing_context_from_account_capability(
            _account_capability(),
            signing_purpose=SigningPurpose.ORDER,
            chain_id=137,
            token_id="tok_yes",
            fee_rate_bps=30,
        )
        request = SignerRequest(
            request_id="req_1",
            requester="operator",
            timestamp=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
            context=context,
            payload={"kind": "signer_smoke"},
        )
        response = DisabledSignerBackend().sign_order(request)
        self.assertEqual(response.status, SignatureAuditStatus.REJECTED.value)
        self.assertEqual(response.error, "signer_backend_disabled")
        self.assertIsNone(response.signature)

    def test_signature_audit_log_to_row_maps_extended_columns(self) -> None:
        row = signature_audit_log_to_row(
            SimpleNamespace(
                log_id="siglog_1",
                request_id="req_1",
                signature_type=1,
                payload_hash="phash_1",
                signature=None,
                status="rejected",
                requester="operator",
                timestamp=datetime(2026, 3, 11, 10, 0),
                error="signer_backend_disabled",
                wallet_type="eoa",
                signer_address="0x1111111111111111111111111111111111111111",
                funder="0x1111111111111111111111111111111111111111",
                api_key_ref=None,
                chain_id=137,
                token_id="tok_yes",
                fee_rate_bps=30,
                signing_purpose="order",
                created_at=datetime(2026, 3, 11, 10, 0),
            )
        )
        self.assertEqual(row[0], "siglog_1")
        self.assertEqual(row[2], 1)
        self.assertEqual(row[14], "tok_yes")
        self.assertEqual(row[15], 30)
        self.assertEqual(row[16], "order")

    def test_shell_does_not_expose_arbitrary_sign_api(self) -> None:
        self.assertFalse(hasattr(SignerServiceShell(), "sign_message"))


class SignerShellDuckDBTest(unittest.TestCase):
    def test_sign_order_persists_signature_audit_logs_and_journal(self) -> None:
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
            signer_service = SignerServiceShell(DisabledSignerBackend())
            context = build_signing_context_from_account_capability(
                _account_capability(),
                signing_purpose=SigningPurpose.ORDER,
                chain_id=137,
                token_id="tok_yes",
                fee_rate_bps=30,
            )
            request = SignerRequest(
                request_id="req_signer_1",
                requester="operator",
                timestamp=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
                context=context,
                payload={"kind": "signer_smoke", "order_id": "ordr_test"},
            )
            result = signer_service.sign_order(request, queue_cfg=queue_cfg, run_id="run_signer_1")
            self.assertEqual(result.response.status, "rejected")
            self.assertEqual(len(result.task_ids), 4)

            with patch.dict(
                "os.environ",
                {
                    "ASTERION_WRITERD_ALLOWED_TABLES": "meta.signature_audit_logs,runtime.journal_events",
                },
                clear=False,
            ):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass

            con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
            try:
                audit_row = con.execute(
                    """
                    SELECT
                        request_id,
                        payload_hash,
                        status,
                        wallet_type,
                        signer_address,
                        funder,
                        chain_id,
                        token_id,
                        fee_rate_bps,
                        signing_purpose
                    FROM meta.signature_audit_logs
                    """
                ).fetchone()
                self.assertEqual(audit_row[0], "req_signer_1")
                self.assertEqual(audit_row[1], result.payload_hash)
                self.assertEqual(audit_row[2], "rejected")
                self.assertEqual(audit_row[3], "eoa")
                self.assertEqual(audit_row[4], "0x1111111111111111111111111111111111111111")
                self.assertEqual(audit_row[5], "0x1111111111111111111111111111111111111111")
                self.assertEqual(audit_row[6], 137)
                self.assertEqual(audit_row[7], "tok_yes")
                self.assertEqual(audit_row[8], 30)
                self.assertEqual(audit_row[9], "order")

                journal_rows = con.execute(
                    """
                    SELECT event_type, entity_type, entity_id
                    FROM runtime.journal_events
                    ORDER BY event_type
                    """
                ).fetchall()
                self.assertEqual(
                    journal_rows,
                    [
                        ("signer.rejected", "signature_request", "req_signer_1"),
                        ("signer.requested", "signature_request", "req_signer_1"),
                    ],
                )
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
