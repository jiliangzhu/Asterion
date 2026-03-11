from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from asterion_core.contracts import (
    AccountTradingCapability,
    MarketCapability,
    RouteAction,
)
from asterion_core.execution import build_execution_context
from asterion_core.execution.order_router_v1 import RoutedCanonicalOrder
from asterion_core.signer import (
    DeterministicOfficialOrderSigningBackend,
    DisabledSignerBackend,
    SignOrderRequest,
    SignerRequest,
    SignerServiceShell,
    SigningPurpose,
    SignatureAuditStatus,
    build_sign_order_request_from_routed_order,
    build_signing_context_from_account_capability,
    build_submit_attempt_record,
    hash_signer_payload,
    signature_audit_log_to_row,
    submit_attempt_record_to_row,
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


def _market_capability() -> MarketCapability:
    return MarketCapability(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id="tok_yes",
        outcome="YES",
        tick_size=Decimal("0.01"),
        fee_rate_bps=30,
        neg_risk=False,
        min_order_size=Decimal("1"),
        tradable=True,
        fees_enabled=True,
        data_sources=["gamma", "clob_public"],
        updated_at=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
    )


def _routed_order() -> RoutedCanonicalOrder:
    return RoutedCanonicalOrder(
        ticket_id="tt_1",
        request_id="req_ticket_1",
        wallet_id="wallet_weather_1",
        execution_context_id="ectx_1",
        market_id="mkt_weather_1",
        token_id="tok_yes",
        outcome="YES",
        side="buy",
        price=Decimal("0.63"),
        size=Decimal("10"),
        route_action=RouteAction.FAK,
        time_in_force=_market_time_in_force(),
        expiration=None,
        fee_rate_bps=30,
        signature_type=1,
        funder="0x1111111111111111111111111111111111111111",
        post_only=False,
        canonical_order_hash="coh_1",
        router_reason="route_action_normalized",
    )


def _market_time_in_force():
    from asterion_core.contracts import TimeInForce

    return TimeInForce.FAK


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

    def test_build_sign_order_request_from_routed_order(self) -> None:
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        request = build_sign_order_request_from_routed_order(
            _routed_order(),
            execution_context,
            requester="operator",
            request_id="req_sign_1",
            timestamp=datetime(2026, 3, 11, 10, 5, tzinfo=timezone.utc),
        )
        self.assertIsInstance(request, SignOrderRequest)
        self.assertEqual(request.ticket_id, "tt_1")
        self.assertEqual(request.execution_context_id, "ectx_1")
        self.assertEqual(request.canonical_order.token_id, "tok_yes")
        self.assertEqual(request.context.signer_address, _account_capability().funder)

    def test_disabled_backend_rejects_order_signing(self) -> None:
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        request = build_sign_order_request_from_routed_order(
            _routed_order(),
            execution_context,
            requester="operator",
            request_id="req_sign_1",
            timestamp=datetime(2026, 3, 11, 10, 5, tzinfo=timezone.utc),
        )
        response = DisabledSignerBackend().sign_order(request)
        self.assertEqual(response.status, "rejected")
        self.assertEqual(response.error, "signer_backend_disabled")
        self.assertIsNone(response.signature)

    def test_official_stub_generates_deterministic_payload(self) -> None:
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        request = build_sign_order_request_from_routed_order(
            _routed_order(),
            execution_context,
            requester="operator",
            request_id="req_sign_1",
            timestamp=datetime(2026, 3, 11, 10, 5, tzinfo=timezone.utc),
        )
        backend = DeterministicOfficialOrderSigningBackend()
        left = backend.sign_order(request)
        right = backend.sign_order(request)
        self.assertEqual(left.status, "signed")
        self.assertEqual(left.payload_hash, right.payload_hash)
        self.assertEqual(left.submit_payload_json, right.submit_payload_json)
        self.assertIn("order", left.submit_payload_json)
        self.assertEqual(left.submit_payload_json["backend_kind"], "official_stub")

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

    def test_build_submit_attempt_record_maps_sign_only_ledger(self) -> None:
        execution_context = build_execution_context(
            market_capability=_market_capability(),
            account_capability=_account_capability(),
            route_action=RouteAction.FAK,
        )
        request = build_sign_order_request_from_routed_order(
            _routed_order(),
            execution_context,
            requester="operator",
            request_id="req_sign_1",
            timestamp=datetime(2026, 3, 11, 10, 5, tzinfo=timezone.utc),
        )
        result = DeterministicOfficialOrderSigningBackend().sign_order(request)
        record = build_submit_attempt_record(request, result, wallet_id="wallet_weather_1")
        row = submit_attempt_record_to_row(record)
        self.assertEqual(record.attempt_kind, "sign_order")
        self.assertEqual(record.attempt_mode, "sign_only")
        self.assertEqual(record.status, "signed")
        self.assertEqual(row[1], "req_sign_1")
        self.assertEqual(row[4], "wallet_weather_1")

    def test_shell_does_not_expose_arbitrary_sign_api(self) -> None:
        self.assertFalse(hasattr(SignerServiceShell(), "sign_message"))


class SignerShellDuckDBTest(unittest.TestCase):
    def test_sign_order_persists_signature_audit_logs_and_journal_but_not_submit_attempts(self) -> None:
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
            signer_service = SignerServiceShell(DeterministicOfficialOrderSigningBackend())
            execution_context = build_execution_context(
                market_capability=_market_capability(),
                account_capability=_account_capability(),
                route_action=RouteAction.FAK,
            )
            request = build_sign_order_request_from_routed_order(
                _routed_order(),
                execution_context,
                requester="operator",
                request_id="req_signer_1",
                timestamp=datetime(2026, 3, 11, 10, 0, tzinfo=timezone.utc),
            )
            result = signer_service.sign_order(request, queue_cfg=queue_cfg, run_id="run_signer_1")
            self.assertEqual(result.response.status, "signed")

            with patch.dict(
                "os.environ",
                {
                    "ASTERION_WRITERD_ALLOWED_TABLES": "meta.signature_audit_logs,runtime.journal_events,runtime.submit_attempts",
                },
                clear=False,
            ):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass

            con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
            try:
                audit_row = con.execute(
                    """
                    SELECT request_id, payload_hash, status, wallet_type, signer_address, token_id, fee_rate_bps, signing_purpose
                    FROM meta.signature_audit_logs
                    """
                ).fetchone()
                self.assertEqual(audit_row[0], "req_signer_1")
                self.assertEqual(audit_row[1], result.payload_hash)
                self.assertEqual(audit_row[2], "succeeded")
                self.assertEqual(audit_row[3], "eoa")
                self.assertEqual(audit_row[4], "0x1111111111111111111111111111111111111111")
                self.assertEqual(audit_row[5], "tok_yes")
                self.assertEqual(audit_row[6], 30)
                self.assertEqual(audit_row[7], "order")

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
                        ("signer.requested", "signature_request", "req_signer_1"),
                        ("signer.succeeded", "signature_request", "req_signer_1"),
                    ],
                )
                self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.submit_attempts").fetchone()[0], 0)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
