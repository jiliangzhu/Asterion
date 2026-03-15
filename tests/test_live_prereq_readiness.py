from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from asterion_core.monitoring import (
    ReadinessConfig,
    build_controlled_live_capability_manifest,
    evaluate_p4_live_prereq_readiness,
    write_controlled_live_capability_manifest,
)
from asterion_core.storage.write_queue import WriteQueueConfig, enqueue_task, init_queue, mark_task_failed
from tests.test_p2_closeout import _apply_schema


class LivePrereqReadinessTest(unittest.TestCase):
    def test_evaluate_p4_live_prereq_readiness_returns_go(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _seed_p4_readiness_environment(tmpdir)
            report = evaluate_p4_live_prereq_readiness(config)
            self.assertEqual(report.target.value, "p4_live_prerequisites")
            self.assertEqual(report.go_decision, "GO")
            self.assertEqual(
                report.decision_reason,
                "all readiness gates passed; ready for controlled live rollout decision",
            )
            self.assertEqual(report.capability_manifest_status, "valid")
            self.assertTrue((report.capability_boundary_summary or {}).get("manual_only"))

    def test_signer_path_health_fail(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            config = _seed_p4_readiness_environment(tmpdir)
            con = duckdb.connect(config.db_path)
            try:
                con.execute("UPDATE meta.signature_audit_logs SET status = 'rejected'")
            finally:
                con.close()
            report = evaluate_p4_live_prereq_readiness(config)
            gate = _gate(report, "signer_path_health")
            self.assertFalse(gate.passed)

    def test_submitter_shadow_path_fail(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            config = _seed_p4_readiness_environment(tmpdir, execution_status="submit_rejected")
            con = duckdb.connect(config.db_path)
            try:
                con.execute("DELETE FROM runtime.submit_attempts WHERE attempt_kind = 'submit_order'")
            finally:
                con.close()
            report = evaluate_p4_live_prereq_readiness(config)
            gate = _gate(report, "submitter_shadow_path")
            self.assertFalse(gate.passed)

    def test_wallet_state_and_allowance_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config = _seed_p4_readiness_environment(tmpdir, wallet_status="allowance_action_required")
            report = evaluate_p4_live_prereq_readiness(config)
            gate = _gate(report, "wallet_state_and_allowance")
            self.assertFalse(gate.passed)

    def test_external_execution_alignment_fail(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            config = _seed_p4_readiness_environment(tmpdir, execution_status="external_mismatch", external_status="external_fill_mismatch")
            con = duckdb.connect(config.db_path)
            try:
                con.execute(
                    "UPDATE trading.reconciliation_results SET status = 'external_fill_mismatch', resolution = 'external_fill_quantity_diff' WHERE reconciliation_scope = 'external_execution'"
                )
            finally:
                con.close()
            report = evaluate_p4_live_prereq_readiness(config)
            gate = _gate(report, "external_execution_alignment")
            self.assertFalse(gate.passed)

    def test_ops_queue_and_chain_tx_fail(self) -> None:
        import duckdb

        with tempfile.TemporaryDirectory() as tmpdir:
            config = _seed_p4_readiness_environment(tmpdir)
            con = duckdb.connect(config.db_path)
            try:
                con.execute("UPDATE runtime.chain_tx_attempts SET status = 'rejected'")
            finally:
                con.close()
            queue_cfg = WriteQueueConfig(path=config.write_queue_path)
            init_queue(queue_cfg)
            dead = enqueue_task(queue_cfg, task_type="UPSERT_ROWS_V1", payload={"table": "x"}, max_attempts=1)
            mark_task_failed(queue_cfg, task_id=dead, error_message="boom")
            report = evaluate_p4_live_prereq_readiness(config)
            gate = _gate(report, "ops_queue_and_chain_tx")
            self.assertFalse(gate.passed)


def _seed_p4_readiness_environment(
    tmpdir: str,
    *,
    wallet_status: str = "ready",
    signer_status: str = "succeeded",
    execution_status: str = "shadow_aligned",
    external_status: str = "ok",
    can_trade: bool = True,
) -> ReadinessConfig:
    import duckdb

    db_path = str(Path(tmpdir) / "asterion.duckdb")
    lite_db_path = str(Path(tmpdir) / "ui_lite.duckdb")
    queue_path = str(Path(tmpdir) / "write_queue.sqlite")
    policy_path = str(Path(tmpdir) / "controlled_live_smoke.json")
    capability_manifest_path = str(Path(tmpdir) / "controlled_live_capability_manifest.json")
    _apply_schema(db_path)
    con = duckdb.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO capability.account_trading_capabilities (
                wallet_id, wallet_type, signature_type, funder, allowance_targets,
                can_use_relayer, can_trade, restricted_reason, updated_at
            ) VALUES (
                'wallet_weather_1', 'eoa', 1, '0xfunder', ?, TRUE, ?, NULL, ?
            )
            """,
            [json.dumps(["0xrelayer"]), can_trade, datetime(2026, 3, 12, 10, 0)],
        )
        con.execute(
            """
            INSERT INTO meta.signature_audit_logs (
                log_id, request_id, signature_type, payload_hash, signature, status, requester, timestamp, error,
                wallet_type, signer_address, funder, api_key_ref, chain_id, token_id, fee_rate_bps, signing_purpose, created_at
            ) VALUES (
                'sig_1', 'sigreq_1', '1', 'ph_sig', 'sig', ?, 'operator', ?, NULL,
                'eoa', '0xfunder', '0xfunder', NULL, 137, 'tok_yes', 30, 'order', ?
            )
            """,
            [signer_status, datetime(2026, 3, 12, 10, 0), datetime(2026, 3, 12, 10, 0)],
        )
        con.execute(
            """
            INSERT INTO runtime.submit_attempts (
                attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id, exchange,
                attempt_kind, attempt_mode, canonical_order_hash, payload_hash, submit_payload_json,
                signed_payload_ref, status, error, created_at
            ) VALUES
            ('sign_1', 'req_sign_1', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
             'sign_order', 'sign_only', 'coh_1', 'ph_1', '{}', 'sign_1', 'signed', NULL, ?),
            ('submit_1', 'req_submit_1', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
             'submit_order', 'shadow_submit', 'coh_1', 'ph_2', '{}', 'sign_1', 'accepted', NULL, ?)
            """,
            [datetime(2026, 3, 12, 10, 5), datetime(2026, 3, 12, 10, 10)],
        )
        con.execute(
            """
            INSERT INTO runtime.chain_tx_attempts (
                attempt_id, request_id, wallet_id, tx_kind, tx_mode, chain_id, funder, token_id, allowance_target,
                nonce, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, payload_hash, tx_payload_json,
                signed_payload_ref, tx_hash, status, error, created_at
            ) VALUES (
                'ctx_1', 'ctxreq_1', 'wallet_weather_1', 'approve_usdc', 'shadow_broadcast', 137, '0xfunder',
                'usdc_e', '0xrelayer', 1, 120000, 100, 2, 'ph_ctx', '{}', 'sig_1', '0xtx', 'accepted', NULL, ?
            )
            """,
            [datetime(2026, 3, 12, 10, 15)],
        )
        con.execute(
            """
            INSERT INTO runtime.external_balance_observations (
                observation_id, wallet_id, funder, signature_type, asset_type, token_id, market_id, outcome,
                observation_kind, allowance_target, chain_id, block_number, observed_quantity, source, observed_at,
                raw_observation_json
            ) VALUES
            ('ebal_native', 'wallet_weather_1', '0xfunder', 1, 'native_gas', NULL, NULL, NULL, 'wallet_balance', NULL, 137, 100, 1.000000000000000000, 'shadow_stub', ?, '{}'),
            ('ebal_usdc', 'wallet_weather_1', '0xfunder', 1, 'usdc_e', 'usdc_e', NULL, NULL, 'wallet_balance', NULL, 137, 100, 100.000000000000000000, 'shadow_stub', ?, '{}'),
            ('ebal_allow', 'wallet_weather_1', '0xfunder', 1, 'usdc_e', 'usdc_e', NULL, NULL, 'token_allowance', '0xrelayer', 137, 100, 100.000000000000000000, 'shadow_stub', ?, '{}')
            """,
            [datetime(2026, 3, 12, 10, 20), datetime(2026, 3, 12, 10, 20), datetime(2026, 3, 12, 10, 20)],
        )
        con.execute(
            """
            INSERT INTO runtime.external_order_observations (
                observation_id, attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                exchange, observation_kind, submit_mode, canonical_order_hash, external_order_id, external_status,
                observed_at, error, raw_observation_json
            ) VALUES (
                'eord_1', 'submit_1', 'req_submit_1', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
                'shadow_submit_ack', 'shadow_submit', 'coh_1', 'ext_1', 'accepted', ?, NULL, '{}'
            )
            """,
            [datetime(2026, 3, 12, 10, 11)],
        )
        con.execute(
            """
            INSERT INTO runtime.external_fill_observations (
                observation_id, attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id, exchange,
                observation_kind, external_order_id, external_trade_id, market_id, token_id, outcome, side, price, size,
                fee, fee_rate_bps, external_status, observed_at, error, raw_observation_json
            ) VALUES (
                'efill_1', 'submit_1', 'req_submit_1', 'tt_1', 'ordr_1', 'wallet_weather_1', 'ectx_1', 'polymarket_clob',
                'shadow_fill_full', 'ext_1', 'trade_1', 'mkt_weather_1', 'tok_yes', 'YES', 'buy', 0.60, 10.00, 0.18, 30,
                'filled', ?, NULL, '{}'
            )
            """,
            [datetime(2026, 3, 12, 10, 12)],
        )
        con.execute(
            """
            INSERT INTO trading.reconciliation_results (
                reconciliation_id, wallet_id, funder, signature_type, asset_type, token_id, market_id, balance_type,
                local_quantity, remote_quantity, discrepancy, status, resolution, created_at, order_id, ticket_id,
                execution_context_id, reconciliation_scope, source_system, local_state, remote_state,
                external_order_observation_id, external_fill_observation_id, external_balance_observation_id
            ) VALUES (
                'recon_ext_1', 'wallet_weather_1', '0xfunder', 1, 'execution', 'tok_yes', 'mkt_weather_1', 'position',
                10.0, 10.0, 0.0, ?, 'external_execution_match', ?, 'ordr_1', 'tt_1', 'ectx_1',
                'external_execution', 'shadow_submit', 'filled', 'accepted', 'eord_1', 'efill_agg', 'ebal_usdc'
            )
            """,
            [external_status, datetime(2026, 3, 12, 10, 25)],
        )
    finally:
        con.close()
    _create_live_prereq_lite_db(
        lite_db_path=lite_db_path,
        wallet_status=wallet_status,
        signer_status=signer_status,
        execution_status=execution_status,
        external_status=external_status,
        can_trade=can_trade,
    )
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
    os.environ["ASTERION_CONTROLLED_LIVE_SECRET_ARMED"] = "false"
    os.environ["ASTERION_CONTROLLED_LIVE_SECRET_APPROVAL_TOKEN"] = "live-token"
    os.environ["ASTERION_CONTROLLED_LIVE_SECRET_PK_WALLET_WEATHER_1"] = "0xabc"
    manifest = build_controlled_live_capability_manifest(
        policy_path=policy_path,
        signer_backend_kind="env_private_key_tx",
        chain_tx_backend_kind="real_broadcast",
        submitter_backend_kind="shadow_stub",
    )
    write_controlled_live_capability_manifest(manifest, path=capability_manifest_path)
    return ReadinessConfig(
        db_path=db_path,
        ui_lite_db_path=lite_db_path,
        write_queue_path=queue_path,
        controlled_live_smoke_policy_path=policy_path,
        controlled_live_capability_manifest_path=capability_manifest_path,
        signer_backend_kind="env_private_key_tx",
        submitter_backend_kind="shadow_stub",
        chain_tx_backend_kind="real_broadcast",
    )


def _create_live_prereq_lite_db(
    *,
    lite_db_path: str,
    wallet_status: str,
    signer_status: str,
    execution_status: str,
    external_status: str,
    can_trade: bool,
) -> None:
    import duckdb

    con = duckdb.connect(lite_db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS ui")
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.live_prereq_wallet_summary AS
            SELECT
                'wallet_weather_1' AS wallet_id,
                '0xfunder' AS funder,
                1 AS signature_type,
                CAST(? AS BOOLEAN) AS can_trade,
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
                ? AS latest_signer_status,
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
                CAST(? <> 'ready' OR ? = 'rejected' AS BOOLEAN) AS attention_required
            """,
            [can_trade, signer_status, wallet_status, json.dumps([] if wallet_status == "ready" else [wallet_status]), wallet_status, signer_status],
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.live_prereq_execution_summary AS
            SELECT
                'srun_1' AS run_id,
                'tt_1' AS ticket_id,
                'req_sign_1' AS request_id,
                'wallet_weather_1' AS wallet_id,
                'weather_primary' AS strategy_id,
                'v1' AS strategy_version,
                'mkt_weather_1' AS market_id,
                'ordr_1' AS order_id,
                'ectx_1' AS execution_context_id,
                'sign_1' AS latest_sign_attempt_id,
                ? AS latest_sign_attempt_status,
                TIMESTAMP '2026-03-12 10:05:00' AS latest_sign_attempt_created_at,
                'submit_1' AS latest_submit_attempt_id,
                'shadow_submit' AS latest_submit_mode,
                'accepted' AS latest_submit_status,
                TIMESTAMP '2026-03-12 10:10:00' AS latest_submit_created_at,
                'accepted' AS external_order_status,
                TIMESTAMP '2026-03-12 10:11:00' AS external_order_observed_at,
                1 AS external_fill_count,
                10.0 AS external_filled_size,
                TIMESTAMP '2026-03-12 10:12:00' AS external_last_fill_at,
                ? AS external_reconciliation_status,
                0.0 AS external_reconciliation_discrepancy,
                ? AS live_prereq_execution_status,
                CAST(? IN ('sign_rejected', 'submit_rejected', 'external_unverified', 'external_mismatch') AS BOOLEAN) AS live_prereq_attention_required
            """,
            [signer_status, external_status, execution_status, execution_status],
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE ui.execution_exception_summary AS
            SELECT
                'tt_1' AS ticket_id,
                'srun_1' AS run_id,
                'mkt_weather_1' AS market_id,
                'weather_primary' AS strategy_id,
                'filled' AS execution_result,
                NULL::TEXT AS gate_reason,
                'ok' AS reconciliation_status,
                0.0 AS reconciliation_discrepancy,
                'filled' AS latest_transition_to_status,
                'paper_fill' AS latest_transition_reason,
                'submitter.accepted' AS latest_journal_event_type,
                FALSE AS operator_attention_required,
                ? AS latest_sign_attempt_status,
                'accepted' AS latest_submit_status,
                'shadow_submit' AS latest_submit_mode,
                'accepted' AS external_order_status,
                ? AS external_reconciliation_status,
                ? AS live_prereq_execution_status,
                CAST(? IN ('sign_rejected', 'submit_rejected', 'external_unverified', 'external_mismatch') AS BOOLEAN) AS live_prereq_attention_required
            WHERE ? IN ('sign_rejected', 'submit_rejected', 'external_unverified', 'external_mismatch')
            """,
            [signer_status, external_status, execution_status, execution_status, execution_status],
        )
    finally:
        con.close()


def _gate(report, gate_name: str):
    return next(item for item in report.gate_results if item.gate_name == gate_name)


if __name__ == "__main__":
    unittest.main()
