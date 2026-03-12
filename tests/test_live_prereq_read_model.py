from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from asterion_core.ui import build_ui_lite_db_once
from dagster_asterion.handlers import run_weather_paper_execution_job


def _setup_base_paper_execution(*, db_path: str, queue_path: str) -> tuple[str, str, str, str]:
    migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
    writer_env = {
        "ASTERION_STRICT_SINGLE_WRITER": "1",
        "ASTERION_DB_ROLE": "writer",
        "WRITERD": "1",
    }
    with patch.dict("os.environ", writer_env, clear=False):
        apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
        con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
        try:
            con.execute(
                "INSERT INTO capability.market_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "tok_yes",
                    "mkt_weather_1",
                    "cond_weather_1",
                    "YES",
                    Decimal("0.01"),
                    30,
                    False,
                    Decimal("1"),
                    True,
                    True,
                    '["gamma","clob_public"]',
                    datetime(2026, 3, 10, 10, 0),
                ],
            )
            con.execute(
                "INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "wallet_weather_1",
                    "eoa",
                    1,
                    "0xfunder",
                    '["0xrelayer"]',
                    True,
                    True,
                    None,
                    datetime(2026, 3, 10, 10, 0),
                ],
            )
            con.execute(
                "INSERT INTO weather.weather_watch_only_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "snap_yes",
                    "fv_yes",
                    "frun_weather_1",
                    "mkt_weather_1",
                    "cond_weather_1",
                    "tok_yes",
                    "YES",
                    0.55,
                    0.63,
                    800,
                    500,
                    "TAKE",
                    "BUY",
                    "ui live prereq",
                    '{"signal_ts_ms":1710000000000}',
                    datetime(2026, 3, 10, 10, 0),
                ],
            )
            con.execute(
                "INSERT INTO trading.inventory_positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    "wallet_weather_1",
                    "usdc_e",
                    "usdc_e",
                    "cash",
                    "cash",
                    "available",
                    Decimal("100"),
                    "0xfunder",
                    1,
                    datetime(2026, 3, 10, 10, 0),
                ],
            )
        finally:
            con.close()
    reader_env = {
        "ASTERION_STRICT_SINGLE_WRITER": "1",
        "ASTERION_DB_ROLE": "reader",
        "WRITERD": "0",
    }
    queue_cfg = WriteQueueConfig(path=queue_path)
    with patch.dict("os.environ", reader_env, clear=False):
        con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
        try:
            run_weather_paper_execution_job(
                con,
                queue_cfg,
                params_json={
                    "wallet_id": "wallet_weather_1",
                    "strategy_registrations": [
                        {
                            "strategy_id": "weather_primary",
                            "strategy_version": "v1",
                            "priority": 1,
                            "route_action": "FAK",
                            "size": "10",
                            "min_edge_bps": 500,
                        }
                    ],
                    "snapshot_ids": ["snap_yes"],
                },
                observed_at=datetime(2026, 3, 10, 10, 6, tzinfo=UTC),
            )
        finally:
            con.close()
    allow_tables = ",".join(
        [
            "runtime.strategy_runs",
            "runtime.trade_tickets",
            "runtime.gate_decisions",
            "trading.orders",
            "trading.reservations",
            "trading.fills",
            "trading.inventory_positions",
            "trading.order_state_transitions",
            "trading.exposure_snapshots",
            "trading.reconciliation_results",
            "capability.execution_contexts",
            "runtime.journal_events",
        ]
    )
    with patch.dict("os.environ", {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow_tables}, clear=False):
        while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
            pass

    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
    try:
        ticket_id, run_id, request_id, execution_context_id = con.execute(
            "SELECT ticket_id, run_id, request_id, execution_context_id FROM runtime.trade_tickets"
        ).fetchone()
        order_id = con.execute("SELECT order_id FROM trading.orders").fetchone()[0]
    finally:
        con.close()
    return str(ticket_id), str(run_id), str(request_id), str(order_id), str(execution_context_id)


class LivePrereqReadModelDuckDBTest(unittest.TestCase):
    def test_live_prereq_read_model_surfaces_shadow_aligned_execution_and_ready_wallet(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            report_json = str(Path(tmpdir) / "readiness.json")
            ticket_id, run_id, request_id, order_id, execution_context_id = _setup_base_paper_execution(
                db_path=db_path,
                queue_path=queue_path,
            )
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                    """
                    INSERT INTO runtime.submit_attempts (
                        attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                        exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                        submit_payload_json, signed_payload_ref, status, error, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "satt_sign_1",
                        "sigreq_1",
                        ticket_id,
                        order_id,
                        "wallet_weather_1",
                        execution_context_id,
                        "polymarket_clob",
                        "sign_order",
                        "sign_only",
                        "coh_1",
                        "phash_sign_1",
                        '{"signed":true}',
                        "satt_sign_1",
                        "signed",
                        None,
                        datetime(2026, 3, 12, 10, 0),
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.submit_attempts (
                        attempt_id, request_id, ticket_id, order_id, wallet_id, execution_context_id,
                        exchange, attempt_kind, attempt_mode, canonical_order_hash, payload_hash,
                        submit_payload_json, signed_payload_ref, status, error, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "satt_submit_1",
                        "subreq_1",
                        ticket_id,
                        order_id,
                        "wallet_weather_1",
                        execution_context_id,
                        "polymarket_clob",
                        "submit_order",
                        "shadow_submit",
                        "coh_1",
                        "phash_submit_1",
                        '{"status":"accepted","shadow_fill_mode":"full"}',
                        "satt_sign_1",
                        "accepted",
                        None,
                        datetime(2026, 3, 12, 10, 1),
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.external_order_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "eordobs_1",
                        "satt_submit_1",
                        "subreq_1",
                        ticket_id,
                        order_id,
                        "wallet_weather_1",
                        execution_context_id,
                        "polymarket_clob",
                        "shadow_submit_ack",
                        "shadow_submit",
                        "coh_1",
                        "extord_1",
                        "accepted",
                        datetime(2026, 3, 12, 10, 2),
                        None,
                        '{"status":"accepted"}',
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.external_fill_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "efillobs_1",
                        "satt_submit_1",
                        "subreq_1",
                        ticket_id,
                        order_id,
                        "wallet_weather_1",
                        execution_context_id,
                        "polymarket_clob",
                        "shadow_fill_full",
                        "extord_1",
                        "exttrade_1",
                        "mkt_weather_1",
                        "tok_yes",
                        "YES",
                        "buy",
                        Decimal("0.63"),
                        Decimal("10.00000000"),
                        Decimal("0.01890000"),
                        30,
                        "filled",
                        datetime(2026, 3, 12, 10, 3),
                        None,
                        '{"status":"filled"}',
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO meta.signature_audit_logs (
                        log_id, request_id, signature_type, payload_hash, signature, status, requester,
                        timestamp, error, wallet_type, signer_address, funder, api_key_ref, chain_id,
                        token_id, fee_rate_bps, signing_purpose, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "siglog_1",
                        "sigreq_1",
                        1,
                        "phash_sign_1",
                        "stubsig_1",
                        "succeeded",
                        "operator",
                        datetime(2026, 3, 12, 10, 0),
                        None,
                        "eoa",
                        "0xfunder",
                        "0xfunder",
                        None,
                        137,
                        "tok_yes",
                        30,
                        "order",
                        datetime(2026, 3, 12, 10, 0),
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.chain_tx_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "ctxatt_1",
                        "ctxreq_1",
                        "wallet_weather_1",
                        "approve_usdc",
                        "dry_run",
                        137,
                        "0xfunder",
                        "usdc_e",
                        "0xrelayer",
                        12,
                        120000,
                        100,
                        10,
                        "phash_ctx_1",
                        '{"status":"previewed"}',
                        None,
                        None,
                        "previewed",
                        None,
                        datetime(2026, 3, 12, 10, 4),
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.external_balance_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "ebal_native_1",
                        "wallet_weather_1",
                        "0xfunder",
                        1,
                        "native_gas",
                        None,
                        None,
                        None,
                        "wallet_balance",
                        None,
                        137,
                        123,
                        Decimal("1.250000000000000000"),
                        "polygon_rpc",
                        datetime(2026, 3, 12, 10, 5),
                        '{"kind":"wallet_balance"}',
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.external_balance_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "ebal_usdc_1",
                        "wallet_weather_1",
                        "0xfunder",
                        1,
                        "usdc_e",
                        "usdc_e",
                        None,
                        None,
                        "wallet_balance",
                        None,
                        137,
                        123,
                        Decimal("100.000000000000000000"),
                        "polygon_rpc",
                        datetime(2026, 3, 12, 10, 5),
                        '{"kind":"wallet_balance"}',
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.external_balance_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "ebal_allow_1",
                        "wallet_weather_1",
                        "0xfunder",
                        1,
                        "usdc_e",
                        "usdc_e",
                        None,
                        None,
                        "token_allowance",
                        "0xrelayer",
                        137,
                        123,
                        Decimal("100.000000000000000000"),
                        "polygon_rpc",
                        datetime(2026, 3, 12, 10, 5),
                        '{"kind":"token_allowance"}',
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO trading.reconciliation_results (
                        reconciliation_id, wallet_id, funder, signature_type, order_id, ticket_id, execution_context_id, asset_type, token_id, market_id,
                        balance_type, local_quantity, remote_quantity, discrepancy, status, resolution, created_at,
                        reconciliation_scope, source_system, local_state, remote_state,
                        external_order_observation_id, external_fill_observation_id, external_balance_observation_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "recon_ext_1",
                        "wallet_weather_1",
                        "0xfunder",
                        1,
                        order_id,
                        ticket_id,
                        execution_context_id,
                        "external_execution",
                        "tok_yes",
                        "mkt_weather_1",
                        "settled",
                        Decimal("10.00000000"),
                        Decimal("10.00000000"),
                        Decimal("0E-8"),
                        "ok",
                        "external_execution_match",
                        datetime(2026, 3, 12, 10, 6),
                        "external_execution",
                        "polymarket_clob",
                        "filled",
                        "accepted",
                        "eordobs_1",
                        "efillagg_1",
                        "ebal_usdc_1",
                    ],
                    )
                finally:
                    con.close()

            built = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_db, readiness_report_json_path=report_json)
            self.assertTrue(built.ok, built.error)
            con = connect_duckdb(DuckDBConfig(db_path=lite_db, ddl_path=None))
            try:
                ticket_row = con.execute(
                    """
                    SELECT
                        reconciliation_status,
                        external_reconciliation_status,
                        latest_sign_attempt_status,
                        latest_submit_status,
                        external_order_status,
                        external_fill_count,
                        external_filled_size,
                        live_prereq_execution_status,
                        live_prereq_attention_required
                    FROM ui.execution_ticket_summary
                    """
                ).fetchone()
                execution_row = con.execute(
                    """
                    SELECT
                        ticket_id,
                        latest_submit_mode,
                        external_reconciliation_status,
                        live_prereq_execution_status,
                        live_prereq_attention_required
                    FROM ui.live_prereq_execution_summary
                    """
                ).fetchone()
                wallet_row = con.execute(
                    """
                    SELECT
                        wallet_id,
                        latest_signer_status,
                        latest_chain_tx_kind,
                        wallet_readiness_status,
                        attention_required
                    FROM ui.live_prereq_wallet_summary
                    """
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(
                ticket_row,
                ("ok", "ok", "signed", "accepted", "accepted", 1, Decimal("10.00000000"), "shadow_aligned", False),
            )
            self.assertEqual(execution_row, (ticket_id, "shadow_submit", "ok", "shadow_aligned", False))
            self.assertEqual(wallet_row, ("wallet_weather_1", "succeeded", "approve_usdc", "ready", False))

    def test_live_prereq_exception_summary_surfaces_external_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            ticket_id, run_id, request_id, order_id, execution_context_id = _setup_base_paper_execution(
                db_path=db_path,
                queue_path=queue_path,
            )
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                    """
                    INSERT INTO runtime.submit_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "satt_sign_2",
                        "sigreq_2",
                        ticket_id,
                        order_id,
                        "wallet_weather_1",
                        execution_context_id,
                        "polymarket_clob",
                        "sign_order",
                        "sign_only",
                        "coh_2",
                        "phash_sign_2",
                        '{"signed":true}',
                        "satt_sign_2",
                        "signed",
                        None,
                        datetime(2026, 3, 12, 10, 0),
                    ],
                    )
                    con.execute(
                    """
                    INSERT INTO runtime.submit_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        "satt_submit_2",
                        "subreq_2",
                        ticket_id,
                        order_id,
                        "wallet_weather_1",
                        execution_context_id,
                        "polymarket_clob",
                        "submit_order",
                        "shadow_submit",
                        "coh_2",
                        "phash_submit_2",
                        '{"status":"accepted","shadow_fill_mode":"partial"}',
                        "satt_sign_2",
                        "accepted",
                        None,
                        datetime(2026, 3, 12, 10, 1),
                    ],
                    )
                    con.execute(
                    "INSERT INTO runtime.external_order_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        "eordobs_2",
                        "satt_submit_2",
                        "subreq_2",
                        ticket_id,
                        order_id,
                        "wallet_weather_1",
                        execution_context_id,
                        "polymarket_clob",
                        "shadow_submit_ack",
                        "shadow_submit",
                        "coh_2",
                        "extord_2",
                        "accepted",
                        datetime(2026, 3, 12, 10, 2),
                        None,
                        '{"status":"accepted"}',
                    ],
                    )
                    con.execute(
                    "INSERT INTO trading.reconciliation_results (reconciliation_id, wallet_id, funder, signature_type, order_id, ticket_id, execution_context_id, asset_type, token_id, market_id, balance_type, local_quantity, remote_quantity, discrepancy, status, resolution, created_at, reconciliation_scope, source_system, local_state, remote_state, external_order_observation_id, external_fill_observation_id, external_balance_observation_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        "recon_ext_2",
                        "wallet_weather_1",
                        "0xfunder",
                        1,
                        order_id,
                        ticket_id,
                        execution_context_id,
                        "external_execution",
                        "tok_yes",
                        "mkt_weather_1",
                        "settled",
                        Decimal("10.00000000"),
                        Decimal("5.00000000"),
                        Decimal("5.00000000"),
                        "external_fill_mismatch",
                        "external_fill_quantity_diff",
                        datetime(2026, 3, 12, 10, 6),
                        "external_execution",
                        "polymarket_clob",
                        "filled",
                        "accepted",
                        "eordobs_2",
                        "efillagg_2",
                        None,
                    ],
                    )
                finally:
                    con.close()
            built = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_db)
            self.assertTrue(built.ok, built.error)
            con = connect_duckdb(DuckDBConfig(db_path=lite_db, ddl_path=None))
            try:
                exception_row = con.execute(
                    """
                    SELECT
                        external_reconciliation_status,
                        live_prereq_execution_status,
                        live_prereq_attention_required
                    FROM ui.execution_exception_summary
                    """
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(exception_row, ("external_fill_mismatch", "external_mismatch", True))

    def test_live_prereq_wallet_summary_classifies_blocked_and_allowance_action_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            lite_db = str(Path(tmpdir) / "ui_lite.duckdb")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")
            writer_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "writer",
                "WRITERD": "1",
            }
            with patch.dict("os.environ", writer_env, clear=False):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    con.execute(
                    "INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        "wallet_blocked",
                        "eoa",
                        1,
                        "0xblocked",
                        '["0xrelayer"]',
                        True,
                        False,
                        "blocked",
                        datetime(2026, 3, 12, 10, 0),
                    ],
                    )
                    con.execute(
                    "INSERT INTO capability.account_trading_capabilities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        "wallet_allowance",
                        "eoa",
                        1,
                        "0xallowance",
                        '["0xrelayer"]',
                        True,
                        True,
                        None,
                        datetime(2026, 3, 12, 10, 0),
                    ],
                    )
                    con.execute(
                    "INSERT INTO runtime.external_balance_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        "bal_native_allowance",
                        "wallet_allowance",
                        "0xallowance",
                        1,
                        "native_gas",
                        None,
                        None,
                        None,
                        "wallet_balance",
                        None,
                        137,
                        1,
                        Decimal("1.000000000000000000"),
                        "polygon_rpc",
                        datetime(2026, 3, 12, 10, 0),
                        '{"kind":"wallet_balance"}',
                    ],
                    )
                    con.execute(
                    "INSERT INTO runtime.external_balance_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        "bal_usdc_allowance",
                        "wallet_allowance",
                        "0xallowance",
                        1,
                        "usdc_e",
                        "usdc_e",
                        None,
                        None,
                        "wallet_balance",
                        None,
                        137,
                        1,
                        Decimal("5.000000000000000000"),
                        "polygon_rpc",
                        datetime(2026, 3, 12, 10, 0),
                        '{"kind":"wallet_balance"}',
                    ],
                    )
                    con.execute(
                    "INSERT INTO runtime.external_balance_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        "bal_allowance_zero",
                        "wallet_allowance",
                        "0xallowance",
                        1,
                        "usdc_e",
                        "usdc_e",
                        None,
                        None,
                        "token_allowance",
                        "0xrelayer",
                        137,
                        1,
                        Decimal("0E-18"),
                        "polygon_rpc",
                        datetime(2026, 3, 12, 10, 0),
                        '{"kind":"token_allowance"}',
                    ],
                    )
                finally:
                    con.close()
            built = build_ui_lite_db_once(src_db_path=db_path, dst_db_path=lite_db)
            self.assertTrue(built.ok, built.error)
            con = connect_duckdb(DuckDBConfig(db_path=lite_db, ddl_path=None))
            try:
                rows = con.execute(
                    """
                    SELECT wallet_id, wallet_readiness_status, attention_required
                    FROM ui.live_prereq_wallet_summary
                    ORDER BY wallet_id
                    """
                ).fetchall()
            finally:
                con.close()
            self.assertEqual(
                rows,
                [
                    ("wallet_allowance", "allowance_action_required", True),
                    ("wallet_blocked", "capability_blocked", True),
                ],
            )


if __name__ == "__main__":
    unittest.main()
