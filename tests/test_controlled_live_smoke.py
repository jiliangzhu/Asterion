from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import duckdb
from web3 import Account

from asterion_core.blockchain import (
    ChainTxServiceShell,
    GasEstimate,
    NonceSelection,
    RealBroadcastBackend,
    controlled_live_wallet_secret_env_var,
)
from asterion_core.monitoring import (
    ReadinessReport,
    ReadinessTarget,
    build_controlled_live_capability_manifest,
    write_controlled_live_capability_manifest,
)
from asterion_core.signer import EnvPrivateKeyTransactionSignerBackend, SignerServiceShell
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import run_weather_controlled_live_smoke_job
from tests.test_p2_closeout import _apply_schema


class _Reader:
    def select_nonce(self, funder: str) -> NonceSelection:
        return NonceSelection(nonce=7)

    def estimate_approve_usdc_gas(self) -> GasEstimate:
        return GasEstimate(
            gas_limit=120000,
            max_fee_per_gas=100,
            max_priority_fee_per_gas=10,
        )


class ControlledLiveSmokeTest(unittest.TestCase):
    def test_not_armed_is_blocked_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_env(tmpdir, wallet_status="ready", readiness_go=True)
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "false",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        env["wallet_secret_env_var"]: env["private_key"],
                    },
                    clear=False,
                ):
                    result = run_weather_controlled_live_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        signer_service=SignerServiceShell(EnvPrivateKeyTransactionSignerBackend()),
                        chain_tx_service=ChainTxServiceShell(RealBroadcastBackend(chain_id=137, rpc_urls=["http://rpc.invalid"])),
                        chain_registry_path=env["chain_registry_path"],
                        controlled_live_smoke_policy_path=env["policy_path"],
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        chain_tx_reader=_Reader(),
                        params_json=_request_params(),
                        run_id="run_controlled_live_1",
                        observed_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
                    )
            finally:
                env["con"].close()
            self.assertEqual(result.metadata["status"], "blocked")
            self.assertEqual(result.metadata["reason"], "controlled_live_smoke_not_armed")
            _drain_queue(env["queue_cfg"], env["db_path"], "runtime.journal_events")
            con = connect_duckdb(DuckDBConfig(db_path=env["db_path"], ddl_path=None))
            try:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM meta.signature_audit_logs").fetchone()[0], 0)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.chain_tx_attempts").fetchone()[0], 0)
                self.assertEqual(
                    con.execute(
                        "SELECT event_type FROM runtime.journal_events ORDER BY created_at, event_id"
                    ).fetchall(),
                    [
                        ("controlled_live_smoke.requested",),
                        ("controlled_live_smoke.blocked",),
                    ],
                )
            finally:
                con.close()

    def test_readiness_not_go_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_env(tmpdir, wallet_status="ready", readiness_go=False)
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "true",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        env["wallet_secret_env_var"]: env["private_key"],
                    },
                    clear=False,
                ):
                    result = run_weather_controlled_live_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        signer_service=SignerServiceShell(EnvPrivateKeyTransactionSignerBackend()),
                        chain_tx_service=ChainTxServiceShell(RealBroadcastBackend(chain_id=137, rpc_urls=["http://rpc.invalid"])),
                        chain_registry_path=env["chain_registry_path"],
                        controlled_live_smoke_policy_path=env["policy_path"],
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        chain_tx_reader=_Reader(),
                        params_json=_request_params(),
                        run_id="run_controlled_live_2",
                        observed_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
                    )
            finally:
                env["con"].close()
            self.assertEqual(result.metadata["status"], "blocked")
            self.assertEqual(result.metadata["reason"], "p4_live_prereq_not_go")

    def test_wallet_not_ready_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_env(tmpdir, wallet_status="allowance_action_required", readiness_go=True)
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "true",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        env["wallet_secret_env_var"]: env["private_key"],
                    },
                    clear=False,
                ):
                    result = run_weather_controlled_live_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        signer_service=SignerServiceShell(EnvPrivateKeyTransactionSignerBackend()),
                        chain_tx_service=ChainTxServiceShell(RealBroadcastBackend(chain_id=137, rpc_urls=["http://rpc.invalid"])),
                        chain_registry_path=env["chain_registry_path"],
                        controlled_live_smoke_policy_path=env["policy_path"],
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        chain_tx_reader=_Reader(),
                        params_json=_request_params(),
                        run_id="run_controlled_live_3",
                        observed_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
                    )
            finally:
                env["con"].close()
            self.assertEqual(result.metadata["status"], "blocked")
            self.assertEqual(result.metadata["reason"], "wallet_not_ready")

    def test_allowlist_and_cap_failures_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_env(tmpdir, wallet_status="ready", readiness_go=True)
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "true",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        env["wallet_secret_env_var"]: env["private_key"],
                    },
                    clear=False,
                ):
                    bad_spender = run_weather_controlled_live_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        signer_service=SignerServiceShell(EnvPrivateKeyTransactionSignerBackend()),
                        chain_tx_service=ChainTxServiceShell(RealBroadcastBackend(chain_id=137, rpc_urls=["http://rpc.invalid"])),
                        chain_registry_path=env["chain_registry_path"],
                        controlled_live_smoke_policy_path=env["policy_path"],
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        chain_tx_reader=_Reader(),
                        params_json=_request_params(spender="0x3333333333333333333333333333333333333333"),
                        run_id="run_controlled_live_4",
                        observed_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
                    )
                    over_cap = run_weather_controlled_live_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        signer_service=SignerServiceShell(EnvPrivateKeyTransactionSignerBackend()),
                        chain_tx_service=ChainTxServiceShell(RealBroadcastBackend(chain_id=137, rpc_urls=["http://rpc.invalid"])),
                        chain_registry_path=env["chain_registry_path"],
                        controlled_live_smoke_policy_path=env["policy_path"],
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        chain_tx_reader=_Reader(),
                        params_json=_request_params(amount="250"),
                        run_id="run_controlled_live_5",
                        observed_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
                    )
            finally:
                env["con"].close()
            self.assertEqual(bad_spender.metadata["reason"], "spender_not_allowlisted")
            self.assertEqual(over_cap.metadata["reason"], "amount_cap_exceeded")

    def test_controlled_live_approve_broadcasts_and_does_not_persist_raw_signed_tx(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_env(tmpdir, wallet_status="ready", readiness_go=True)
            signer_service = SignerServiceShell(EnvPrivateKeyTransactionSignerBackend())
            chain_tx_service = ChainTxServiceShell(RealBroadcastBackend(chain_id=137, rpc_urls=["http://rpc.invalid"]))
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "true",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        env["wallet_secret_env_var"]: env["private_key"],
                    },
                    clear=False,
                ), patch(
                    "asterion_core.blockchain.chain_tx_v1.RealBroadcastBackend._broadcast_raw_transaction",
                    return_value="0xabc123",
                ):
                    result = run_weather_controlled_live_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        signer_service=signer_service,
                        chain_tx_service=chain_tx_service,
                        chain_registry_path=env["chain_registry_path"],
                        controlled_live_smoke_policy_path=env["policy_path"],
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        chain_tx_reader=_Reader(),
                        params_json=_request_params(),
                        run_id="run_controlled_live_6",
                        observed_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
                    )
            finally:
                env["con"].close()
            self.assertEqual(result.metadata["status"], "broadcasted")
            self.assertEqual(result.metadata["tx_hash"], "0xabc123")
            _drain_queue(
                env["queue_cfg"],
                env["db_path"],
                "meta.signature_audit_logs,runtime.journal_events,runtime.chain_tx_attempts",
            )
            con = connect_duckdb(DuckDBConfig(db_path=env["db_path"], ddl_path=None))
            try:
                attempt = con.execute(
                    """
                    SELECT tx_mode, status, tx_hash, tx_payload_json
                    FROM runtime.chain_tx_attempts
                    """
                ).fetchone()
                self.assertEqual(attempt[0], "controlled_live")
                self.assertEqual(attempt[1], "broadcasted")
                self.assertEqual(attempt[2], "0xabc123")
                self.assertNotIn("raw_transaction_hex", attempt[3])
                self.assertNotIn("private_key_env_var", attempt[3])
                self.assertEqual(con.execute("SELECT COUNT(*) FROM meta.signature_audit_logs").fetchone()[0], 1)
                events = {
                    row[0]
                    for row in con.execute(
                        "SELECT event_type FROM runtime.journal_events"
                    ).fetchall()
                }
                self.assertEqual(
                    events,
                    {
                        "controlled_live_smoke.requested",
                        "signer.requested",
                        "signer.succeeded",
                        "chain_tx.requested",
                        "chain_tx.broadcasted",
                        "controlled_live_smoke.broadcasted",
                    },
                )
            finally:
                con.close()

    def test_writerd_denies_non_allowlisted_controlled_live_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _seed_env(tmpdir, wallet_status="ready", readiness_go=True)
            signer_service = SignerServiceShell(EnvPrivateKeyTransactionSignerBackend())
            chain_tx_service = ChainTxServiceShell(RealBroadcastBackend(chain_id=137, rpc_urls=["http://rpc.invalid"]))
            try:
                with patch.dict(
                    os.environ,
                    {
                        "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "true",
                        "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
                        env["wallet_secret_env_var"]: env["private_key"],
                    },
                    clear=False,
                ), patch(
                    "asterion_core.blockchain.chain_tx_v1.RealBroadcastBackend._broadcast_raw_transaction",
                    return_value="0xabc123",
                ):
                    run_weather_controlled_live_smoke_job(
                        env["con"],
                        env["queue_cfg"],
                        signer_service=signer_service,
                        chain_tx_service=chain_tx_service,
                        chain_registry_path=env["chain_registry_path"],
                        controlled_live_smoke_policy_path=env["policy_path"],
                        controlled_live_capability_manifest_path=env["manifest_path"],
                        readiness_report_json_path=env["readiness_path"],
                        ui_lite_db_path=env["ui_lite_db_path"],
                        chain_tx_reader=_Reader(),
                        params_json=_request_params(),
                        run_id="run_controlled_live_allowlist",
                        observed_at=datetime(2026, 3, 12, 12, 5, tzinfo=UTC),
                    )
            finally:
                env["con"].close()

            _drain_queue(
                env["queue_cfg"],
                env["db_path"],
                "runtime.journal_events",
            )
            con = connect_duckdb(DuckDBConfig(db_path=env["db_path"], ddl_path=None))
            try:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM meta.signature_audit_logs").fetchone()[0], 1)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM runtime.chain_tx_attempts").fetchone()[0], 0)
                self.assertGreater(con.execute("SELECT COUNT(*) FROM runtime.journal_events").fetchone()[0], 0)
            finally:
                con.close()


def _request_params(*, spender: str = "0x2222222222222222222222222222222222222222", amount: str = "25") -> dict[str, str]:
    return {
        "wallet_id": "wallet_weather_1",
        "requester": "operator",
        "approval_id": "clive_approve_20260312_01",
        "approval_reason": "controlled live approve smoke",
        "approval_token": "live-token",
        "tx_kind": "approve_usdc",
        "spender": spender,
        "amount": amount,
    }


def _seed_env(tmpdir: str, *, wallet_status: str, readiness_go: bool) -> dict[str, object]:
    db_path = str(Path(tmpdir) / "asterion.duckdb")
    queue_path = str(Path(tmpdir) / "write_queue.sqlite")
    ui_lite_db_path = str(Path(tmpdir) / "ui_lite.duckdb")
    readiness_path = str(Path(tmpdir) / "asterion_readiness_p4.json")
    policy_path = str(Path(tmpdir) / "controlled_live_smoke.json")
    chain_registry_path = str(Path(__file__).resolve().parents[1] / "config" / "chain_registry.polygon.json")
    _apply_schema(db_path)
    account = Account.from_key(_private_key())
    con = duckdb.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO capability.account_trading_capabilities (
                wallet_id, wallet_type, signature_type, funder, allowance_targets,
                can_use_relayer, can_trade, restricted_reason, updated_at
            ) VALUES (?, 'eoa', 1, ?, ?, TRUE, TRUE, NULL, ?)
            """,
            [
                "wallet_weather_1",
                account.address,
                json.dumps(["0x2222222222222222222222222222222222222222"]),
                datetime(2026, 3, 12, 10, 0),
            ],
        )
        con.execute(
            """
            INSERT INTO runtime.external_balance_observations (
                observation_id, wallet_id, funder, signature_type, asset_type, token_id, market_id, outcome,
                observation_kind, allowance_target, chain_id, block_number, observed_quantity, source, observed_at,
                raw_observation_json
            ) VALUES
            ('ebal_native', 'wallet_weather_1', ?, 1, 'native_gas', NULL, NULL, NULL, 'wallet_balance', NULL, 137, 100, 1.0, 'shadow_stub', ?, '{}'),
            ('ebal_usdc', 'wallet_weather_1', ?, 1, 'usdc_e', 'usdc_e', NULL, NULL, 'wallet_balance', NULL, 137, 100, 100.0, 'shadow_stub', ?, '{}'),
            ('ebal_allow', 'wallet_weather_1', ?, 1, 'usdc_e', 'usdc_e', NULL, NULL, 'token_allowance', '0x2222222222222222222222222222222222222222', 137, 100, 100.0, 'shadow_stub', ?, '{}')
            """,
            [
                account.address,
                datetime(2026, 3, 12, 10, 20),
                account.address,
                datetime(2026, 3, 12, 10, 20),
                account.address,
                datetime(2026, 3, 12, 10, 20),
            ],
        )
    finally:
        con.close()
    ui_con = duckdb.connect(ui_lite_db_path)
    try:
        ui_con.execute("CREATE SCHEMA IF NOT EXISTS ui")
        ui_con.execute(
            """
            CREATE TABLE ui.live_prereq_wallet_summary AS
            SELECT
                'wallet_weather_1' AS wallet_id,
                ? AS funder,
                1 AS signature_type,
                TRUE AS can_trade,
                NULL::TEXT AS restricted_reason,
                TRUE AS can_use_relayer,
                1 AS configured_allowance_target_count,
                1.0 AS latest_native_gas_quantity,
                TIMESTAMP '2026-03-12 10:20:00' AS latest_native_gas_observed_at,
                100.0 AS latest_usdc_balance_quantity,
                TIMESTAMP '2026-03-12 10:20:00' AS latest_usdc_balance_observed_at,
                1 AS observed_allowance_target_count,
                1 AS approved_allowance_target_count,
                TIMESTAMP '2026-03-12 10:20:00' AS latest_allowance_observed_at,
                'succeeded' AS latest_signer_status,
                NULL::TEXT AS latest_signer_error,
                TIMESTAMP '2026-03-12 10:00:00' AS latest_signer_created_at,
                'ctx_1' AS latest_chain_tx_attempt_id,
                'approve_usdc' AS latest_chain_tx_kind,
                'shadow_broadcast' AS latest_chain_tx_mode,
                'accepted' AS latest_chain_tx_status,
                NULL::TEXT AS latest_chain_tx_error,
                TIMESTAMP '2026-03-12 10:15:00' AS latest_chain_tx_created_at,
                ? AS wallet_readiness_status,
                ? AS wallet_readiness_blockers_json,
                CAST(? <> 'ready' AS BOOLEAN) AS attention_required
            """,
            [
                account.address,
                wallet_status,
                json.dumps([] if wallet_status == "ready" else [wallet_status]),
                wallet_status,
            ],
        )
    finally:
        ui_con.close()
    report = ReadinessReport(
        target=ReadinessTarget.P4_LIVE_PREREQUISITES,
        generated_at=datetime(2026, 3, 12, 12, 0, tzinfo=UTC),
        all_passed=readiness_go,
        go_decision="GO" if readiness_go else "NO-GO",
        decision_reason=(
            "all readiness gates passed; ready for controlled live rollout decision"
            if readiness_go
            else "failed gates: signer_path_health; not ready for controlled live rollout decision"
        ),
        data_hash="hash_p4",
        gate_results=[],
    )
    Path(readiness_path).write_text(json.dumps(report.to_dict()), encoding="utf-8")
    wallet_secret_env_var = controlled_live_wallet_secret_env_var("wallet_weather_1")
    Path(policy_path).write_text(
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
    manifest_path = str(Path(tmpdir) / "controlled_live_capability_manifest.json")
    with patch.dict(
        os.environ,
        {
            "ASTERION_CONTROLLED_LIVE_SECRET_ARMED": "false",
            "ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN": "live-token",
            wallet_secret_env_var: _private_key(),
        },
        clear=False,
    ):
        manifest = build_controlled_live_capability_manifest(
            policy_path=policy_path,
            signer_backend_kind="env_private_key_tx",
            chain_tx_backend_kind="real_broadcast",
            submitter_backend_kind="shadow_stub",
        )
        write_controlled_live_capability_manifest(manifest, path=manifest_path)
    return {
        "db_path": db_path,
        "queue_cfg": WriteQueueConfig(path=queue_path),
        "ui_lite_db_path": ui_lite_db_path,
        "readiness_path": readiness_path,
        "policy_path": policy_path,
        "manifest_path": manifest_path,
        "chain_registry_path": chain_registry_path,
        "wallet_secret_env_var": wallet_secret_env_var,
        "private_key": _private_key(),
        "con": connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None)),
    }


def _drain_queue(queue_cfg: WriteQueueConfig, db_path: str, allowed_tables: str) -> None:
    with patch.dict(
        os.environ,
        {"ASTERION_WRITERD_ALLOWED_TABLES": allowed_tables},
        clear=False,
    ):
        while process_one(queue_path=queue_cfg.path, db_path=db_path, ddl_path=None, apply_schema=False):
            pass


def _private_key() -> str:
    return "0x59c6995e998f97a5a0044966f094538e9dc9e86dae88c7a8412c4f2f8e8ddc5f"


if __name__ == "__main__":
    unittest.main()
