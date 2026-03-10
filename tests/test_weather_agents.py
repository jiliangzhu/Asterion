from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from agents.common import AgentInvocationStatus, AgentVerdict, FakeAgentClient
from agents.weather import (
    DataQaAgentRequest,
    ResolutionAgentRequest,
    Rule2SpecAgentRequest,
    run_data_qa_agent_review,
    run_resolution_agent_review,
    run_rule2spec_agent_review,
)
from asterion_core.contracts import (
    EvidencePackageLinkRecord,
    ForecastReplayDiffRecord,
    ForecastReplayRecord,
    ForecastRunRecord,
    ProposalStatus,
    RedeemDecision,
    RedeemReadinessRecord,
    Rule2SpecDraft,
    SettlementVerificationRecord,
    StationMetadata,
    UMAProposal,
    WatchOnlySnapshotRecord,
    WatcherContinuityCheck,
    WeatherFairValueRecord,
    WeatherMarket,
    WeatherMarketSpecRecord,
)
from asterion_core.storage.database import DuckDBConfig, connect_duckdb
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from dagster_asterion.handlers import (
    run_weather_data_qa_review_job,
    run_weather_resolution_review_job,
    run_weather_rule2spec_review_job,
)


HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


def _weather_market() -> WeatherMarket:
    return WeatherMarket(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        event_id="evt_weather_1",
        slug="nyc-high-temp-mar-8",
        title="Will the high temperature in New York City on March 8, 2026 be 50-59°F?",
        description="Template weather market",
        rules="Resolve using weather.com official station high temperature.",
        status="active",
        active=True,
        closed=False,
        archived=False,
        accepting_orders=True,
        enable_order_book=True,
        tags=["Weather", "Temperature"],
        outcomes=["Yes", "No"],
        token_ids=["tok_yes", "tok_no"],
        close_time=datetime(2026, 3, 8, 23, 59, 59),
        end_date=datetime(2026, 3, 8, 23, 59, 59),
        raw_market={"id": "mkt_weather_1"},
    )


def _draft() -> Rule2SpecDraft:
    return Rule2SpecDraft(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        location_name="New York City",
        observation_date=date(2026, 3, 8),
        observation_window_local="daily_max",
        metric="temperature_max",
        unit="fahrenheit",
        bucket_min_value=50.0,
        bucket_max_value=59.0,
        authoritative_source="weather.com",
        fallback_sources=["nws", "openmeteo"],
        rounding_rule="identity",
        inclusive_bounds=True,
        parse_confidence=0.95,
        risk_flags=[],
    )


def _station_metadata() -> StationMetadata:
    return StationMetadata(
        station_id="KNYC",
        location_name="New York City",
        latitude=40.7128,
        longitude=-74.006,
        timezone="America/New_York",
        source="nws",
    )


def _weather_spec() -> WeatherMarketSpecRecord:
    return WeatherMarketSpecRecord(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        location_name="New York City",
        station_id="KNYC",
        latitude=40.7128,
        longitude=-74.006,
        timezone="America/New_York",
        observation_date=date(2026, 3, 8),
        observation_window_local="daily_max",
        metric="temperature_max",
        unit="fahrenheit",
        bucket_min_value=50.0,
        bucket_max_value=59.0,
        authoritative_source="weather.com",
        fallback_sources=["nws", "openmeteo"],
        rounding_rule="identity",
        inclusive_bounds=True,
        spec_version="spec_123",
        parse_confidence=0.95,
        risk_flags=[],
    )


def _forecast_run(run_id: str, *, fallback_used: bool = False, confidence: float = 0.95) -> ForecastRunRecord:
    return ForecastRunRecord(
        run_id=run_id,
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        observation_date=date(2026, 3, 8),
        metric="temperature_max",
        latitude=40.7128,
        longitude=-74.006,
        timezone="America/New_York",
        spec_version="spec_123",
        cache_key="mkt_weather_1|KNYC|spec_123|openmeteo|2026-03-07T12:00Z|2026-03-07T12:00:00+00:00",
        source_trace=["openmeteo"] if not fallback_used else ["openmeteo", "nws"],
        fallback_used=fallback_used,
        from_cache=False,
        confidence=confidence,
        forecast_payload={"temperature_distribution": {55: 1.0}},
        raw_payload={"daily": {"temperature_2m_max": [55.0]}},
    )


def _replay() -> ForecastReplayRecord:
    return ForecastReplayRecord(
        replay_id="freplay_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=UTC),
        spec_version="spec_123",
        replay_key="mkt_weather_1|KNYC|spec_123|openmeteo|2026-03-07T12:00Z|2026-03-07T12:00:00+00:00",
        replay_reason="unit_test",
        original_run_id="frun_original",
        replayed_run_id="frun_replayed",
        created_at=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
    )


def _diff(status: str = "MATCH") -> ForecastReplayDiffRecord:
    return ForecastReplayDiffRecord(
        diff_id=f"fdiff_{status.lower()}",
        replay_id="freplay_1",
        entity_type="forecast_run",
        entity_key="forecast_run",
        original_entity_id="frun_original",
        replayed_entity_id="frun_replayed",
        status=status,
        diff_summary_json={"changed_fields": [] if status == "MATCH" else ["temperature_distribution"]},
        created_at=datetime(2026, 3, 10, 0, 0, tzinfo=UTC),
    )


def _fair_value(run_id: str) -> WeatherFairValueRecord:
    return WeatherFairValueRecord(
        fair_value_id=f"fv_{run_id}",
        run_id=run_id,
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id="tok_yes",
        outcome="YES",
        fair_value=0.7,
        confidence=0.95,
    )


def _snapshot(run_id: str) -> WatchOnlySnapshotRecord:
    return WatchOnlySnapshotRecord(
        snapshot_id=f"snap_{run_id}",
        fair_value_id=f"fv_{run_id}",
        run_id=run_id,
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id="tok_yes",
        outcome="YES",
        reference_price=0.5,
        fair_value=0.7,
        edge_bps=2000,
        threshold_bps=100,
        decision="TAKE",
        side="BUY",
        rationale="unit",
        pricing_context={"threshold_bps": 100},
    )


def _proposal() -> UMAProposal:
    return UMAProposal(
        proposal_id="prop_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        proposer="0xabc",
        proposed_outcome="YES",
        proposal_bond=100.0,
        dispute_bond=None,
        proposal_tx_hash="0xhash",
        proposal_block_number=100,
        proposal_timestamp=datetime(2026, 3, 8, 12, 0),
        status=ProposalStatus.SETTLED,
        on_chain_settled_at=datetime(2026, 3, 9, 1, 0),
        safe_redeem_after=datetime(2026, 3, 10, 1, 0),
        human_review_required=False,
    )


def _verification() -> SettlementVerificationRecord:
    return SettlementVerificationRecord(
        verification_id="verify_1",
        proposal_id="prop_1",
        market_id="mkt_weather_1",
        proposed_outcome="YES",
        expected_outcome="YES",
        is_correct=True,
        confidence=0.95,
        discrepancy_details=None,
        sources_checked=["weather.com"],
        evidence_package_id="evidence_1",
        created_at=datetime(2026, 3, 10, 0, 0),
    )


def _evidence_link() -> EvidencePackageLinkRecord:
    return EvidencePackageLinkRecord(
        proposal_id="prop_1",
        verification_id="verify_1",
        evidence_package_id="evidence_1",
        linked_at=datetime(2026, 3, 10, 0, 0),
    )


def _redeem_readiness() -> RedeemReadinessRecord:
    return RedeemReadinessRecord(
        suggestion_id="redeem_1",
        proposal_id="prop_1",
        decision=RedeemDecision.READY_FOR_REDEEM,
        reason="ready",
        on_chain_settled_at=datetime(2026, 3, 9, 1, 0),
        safe_redeem_after=datetime(2026, 3, 10, 1, 0),
        human_review_required=False,
        created_at=datetime(2026, 3, 10, 0, 0),
    )


def _continuity_check(status: str = "OK") -> WatcherContinuityCheck:
    return WatcherContinuityCheck(
        check_id=f"wcck_{status.lower()}",
        chain_id=137,
        from_block=100,
        to_block=110,
        last_known_finalized_block=110,
        status=status,
        gap_count=0 if status == "OK" else 1,
        details_json={},
        created_at=datetime(2026, 3, 10, 0, 0),
    )


class Rule2SpecAgentTest(unittest.TestCase):
    def test_rule2spec_agent_outputs_station_first_patch(self) -> None:
        request = Rule2SpecAgentRequest(
            market=_weather_market(),
            draft=_draft(),
            current_spec=_weather_spec(),
            station_metadata=_station_metadata(),
            station_override_summary={"has_override": True},
        )
        client = FakeAgentClient(
            responses={
                "rule2spec": {
                    "verdict": "pass",
                    "confidence": 0.92,
                    "summary": "station-first spec looks good",
                    "risk_flags": [],
                    "suggested_patch_json": {"station_id": "KNYC", "authoritative_source": "weather.com"},
                    "findings": [],
                    "human_review_required": False,
                }
            }
        )
        artifacts = run_rule2spec_agent_review(client, request)
        assert artifacts.output is not None
        self.assertEqual(artifacts.invocation.status, AgentInvocationStatus.SUCCESS)
        self.assertEqual(artifacts.output.verdict, AgentVerdict.PASS)
        self.assertNotIn("city", artifacts.output.structured_output_json["suggested_patch_json"])

    def test_rule2spec_agent_parse_error_rejects_city_first_patch(self) -> None:
        request = Rule2SpecAgentRequest(
            market=_weather_market(),
            draft=_draft(),
            current_spec=_weather_spec(),
            station_metadata=_station_metadata(),
            station_override_summary={"has_override": True},
        )
        client = FakeAgentClient(
            responses={
                "rule2spec": {
                    "verdict": "review",
                    "confidence": 0.4,
                    "summary": "bad patch",
                    "risk_flags": ["missing_station_mapping"],
                    "suggested_patch_json": {"city": "New York City"},
                    "findings": [],
                }
            }
        )
        artifacts = run_rule2spec_agent_review(client, request)
        self.assertEqual(artifacts.invocation.status, AgentInvocationStatus.PARSE_ERROR)
        self.assertIsNone(artifacts.output)


class DataQaAgentTest(unittest.TestCase):
    def test_data_qa_agent_low_risk_for_match(self) -> None:
        request = DataQaAgentRequest(
            spec=_weather_spec(),
            replay=_replay(),
            diffs=[_diff("MATCH")],
            original_run=_forecast_run("frun_original"),
            replayed_run=_forecast_run("frun_replayed"),
            replay_fair_values=[_fair_value("frun_replayed")],
            replay_watch_only_snapshots=[_snapshot("frun_replayed")],
        )
        client = FakeAgentClient(
            responses={
                "data_qa": {
                    "verdict": "pass",
                    "confidence": 0.91,
                    "summary": "replay matches original provenance",
                    "station_match_score": 1.0,
                    "timezone_ok": True,
                    "unit_ok": True,
                    "pricing_provenance_ok": True,
                    "fallback_risk": "low",
                    "findings": [],
                    "recommended_actions": [],
                    "human_review_required": False,
                }
            }
        )
        artifacts = run_data_qa_agent_review(client, request)
        assert artifacts.output is not None
        self.assertEqual(artifacts.output.verdict, AgentVerdict.PASS)
        self.assertFalse(artifacts.output.human_review_required)

    def test_data_qa_agent_flags_fallback_and_diff(self) -> None:
        request = DataQaAgentRequest(
            spec=_weather_spec(),
            replay=_replay(),
            diffs=[_diff("DIFFERENT")],
            original_run=_forecast_run("frun_original"),
            replayed_run=_forecast_run("frun_replayed", fallback_used=True),
            replay_fair_values=[_fair_value("frun_replayed")],
            replay_watch_only_snapshots=[_snapshot("frun_replayed")],
        )
        client = FakeAgentClient(
            responses={
                "data_qa": {
                    "verdict": "review",
                    "confidence": 0.55,
                    "summary": "fallback and critical diff need review",
                    "station_match_score": 0.8,
                    "timezone_ok": True,
                    "unit_ok": True,
                    "pricing_provenance_ok": False,
                    "fallback_risk": "high",
                    "findings": [
                        {
                            "finding_code": "critical_diff",
                            "severity": "warn",
                            "summary": "forecast distribution changed",
                        }
                    ],
                    "recommended_actions": ["manual_replay_review"],
                }
            }
        )
        artifacts = run_data_qa_agent_review(client, request)
        assert artifacts.output is not None
        self.assertTrue(artifacts.output.human_review_required)
        self.assertEqual(artifacts.output.structured_output_json["fallback_risk"], "high")


class ResolutionAgentTest(unittest.TestCase):
    def test_resolution_agent_low_risk_when_records_align(self) -> None:
        request = ResolutionAgentRequest(
            proposal=_proposal(),
            verification=_verification(),
            evidence_link=_evidence_link(),
            redeem_readiness=_redeem_readiness(),
            continuity_check=_continuity_check("OK"),
        )
        client = FakeAgentClient(
            responses={
                "resolution": {
                    "verdict": "pass",
                    "confidence": 0.9,
                    "summary": "verification and redeem readiness align",
                    "settlement_risk_score": 0.1,
                    "recommended_operator_action": "ready_for_redeem_review",
                    "findings": [],
                    "human_review_required": False,
                }
            }
        )
        artifacts = run_resolution_agent_review(client, request)
        assert artifacts.output is not None
        self.assertEqual(artifacts.output.verdict, AgentVerdict.PASS)
        self.assertFalse(artifacts.output.human_review_required)

    def test_resolution_agent_flags_manual_review_on_conflict(self) -> None:
        request = ResolutionAgentRequest(
            proposal=_proposal(),
            verification=SettlementVerificationRecord(**{**_verification().__dict__, "is_correct": False}),
            evidence_link=_evidence_link(),
            redeem_readiness=_redeem_readiness(),
            continuity_check=_continuity_check("GAP_DETECTED"),
        )
        client = FakeAgentClient(
            responses={
                "resolution": {
                    "verdict": "review",
                    "confidence": 0.5,
                    "summary": "continuity and verification conflict",
                    "settlement_risk_score": 0.8,
                    "recommended_operator_action": "hold_redeem",
                    "findings": [
                        {
                            "finding_code": "continuity_gap",
                            "severity": "error",
                            "summary": "continuity gap detected",
                        }
                    ],
                }
            }
        )
        artifacts = run_resolution_agent_review(client, request)
        assert artifacts.output is not None
        self.assertTrue(artifacts.output.human_review_required)
        self.assertEqual(artifacts.output.structured_output_json["recommended_operator_action"], "hold_redeem")


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for weather agent integration tests")
class WeatherAgentDuckDBIntegrationTest(unittest.TestCase):
    def test_agent_jobs_persist_to_agent_schema_without_touching_canonical_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "asterion.duckdb")
            queue_path = str(Path(tmpdir) / "write_queue.sqlite")
            migrations_dir = str(Path(__file__).resolve().parents[1] / "sql" / "migrations")

            with patch.dict(
                os.environ,
                {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "writer",
                    "WRITERD": "1",
                },
                clear=False,
            ):
                apply_migrations(MigrationConfig(db_path=db_path, migrations_dir=migrations_dir))

            _insert_agent_test_state(db_path)
            queue_cfg = WriteQueueConfig(path=queue_path)
            client = FakeAgentClient(
                responses={
                    "rule2spec": {
                        "verdict": "pass",
                        "confidence": 0.9,
                        "summary": "spec parse looks valid",
                        "risk_flags": [],
                        "suggested_patch_json": {"station_id": "KNYC"},
                        "findings": [],
                        "human_review_required": False,
                    },
                    "data_qa": {
                        "verdict": "pass",
                        "confidence": 0.9,
                        "summary": "replay provenance is consistent",
                        "station_match_score": 1.0,
                        "timezone_ok": True,
                        "unit_ok": True,
                        "pricing_provenance_ok": True,
                        "fallback_risk": "low",
                        "findings": [],
                        "recommended_actions": [],
                        "human_review_required": False,
                    },
                    "resolution": {
                        "verdict": "review",
                        "confidence": 0.6,
                        "summary": "operator should confirm redeem readiness",
                        "settlement_risk_score": 0.4,
                        "recommended_operator_action": "ready_for_redeem_review",
                        "findings": [],
                        "human_review_required": True,
                    },
                }
            )

            reader_env = {
                "ASTERION_STRICT_SINGLE_WRITER": "1",
                "ASTERION_DB_ROLE": "reader",
                "WRITERD": "0",
            }
            with patch.dict(os.environ, reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    canonical_before = _canonical_table_counts(con)
                    result_rule2spec = run_weather_rule2spec_review_job(con, queue_cfg, client=client, market_ids=["mkt_weather_1"])
                    result_data_qa = run_weather_data_qa_review_job(con, queue_cfg, client=client, replay_ids=["freplay_1"])
                    result_resolution = run_weather_resolution_review_job(con, queue_cfg, client=client, proposal_ids=["prop_1"])
                finally:
                    con.close()

            self.assertGreaterEqual(len(result_rule2spec.task_ids), 4)
            self.assertGreaterEqual(len(result_data_qa.task_ids), 4)
            self.assertGreaterEqual(len(result_resolution.task_ids), 4)

            allowed_tables = ",".join(["agent.invocations", "agent.outputs", "agent.reviews", "agent.evaluations"])
            with patch.dict(
                os.environ,
                {
                    "ASTERION_DB_PATH": db_path,
                    "ASTERION_WRITERD_ALLOWED_TABLES": allowed_tables,
                },
                clear=False,
            ):
                while process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False):
                    pass

            with patch.dict(os.environ, reader_env, clear=False):
                con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                try:
                    canonical_after = _canonical_table_counts(con)
                    agent_counts = {
                        "invocations": con.execute("SELECT COUNT(*) FROM agent.invocations").fetchone()[0],
                        "outputs": con.execute("SELECT COUNT(*) FROM agent.outputs").fetchone()[0],
                        "reviews": con.execute("SELECT COUNT(*) FROM agent.reviews").fetchone()[0],
                        "evaluations": con.execute("SELECT COUNT(*) FROM agent.evaluations").fetchone()[0],
                    }
                finally:
                    con.close()

            self.assertEqual(agent_counts, {"invocations": 3, "outputs": 3, "reviews": 3, "evaluations": 3})
            self.assertEqual(canonical_before, canonical_after)


def _insert_agent_test_state(db_path: str) -> None:
    writer_env = {
        "ASTERION_STRICT_SINGLE_WRITER": "1",
        "ASTERION_DB_ROLE": "writer",
        "WRITERD": "1",
    }
    with patch.dict(os.environ, writer_env, clear=False):
        con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
        try:
            con.execute(
                """
                INSERT INTO weather.weather_markets (
                    market_id, condition_id, event_id, slug, title, description, rules, status, active, closed,
                    archived, accepting_orders, enable_order_book, tags_json, outcomes_json, token_ids_json,
                    close_time, end_date, raw_market_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "mkt_weather_1",
                    "cond_weather_1",
                    "evt_weather_1",
                    "nyc-high-temp-mar-8",
                    "Will the high temperature in New York City on March 8, 2026 be 50-59°F?",
                    "Template weather market",
                    "Resolve using weather.com official station high temperature.",
                    "active",
                    True,
                    False,
                    False,
                    True,
                    True,
                    json.dumps(["Weather", "Temperature"]),
                    json.dumps(["Yes", "No"]),
                    json.dumps(["tok_yes", "tok_no"]),
                    "2026-03-08 23:59:59",
                    "2026-03-08 23:59:59",
                    json.dumps({"id": "mkt_weather_1"}),
                    "2026-03-01 00:00:00",
                    "2026-03-01 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO weather.weather_station_map (
                    map_id, market_id, location_name, location_key, station_id, station_name, latitude, longitude,
                    timezone, source, authoritative_source, is_override, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "stmap_1",
                    "mkt_weather_1",
                    "New York City",
                    "new york city",
                    "KNYC",
                    "New York City",
                    40.7128,
                    -74.006,
                    "America/New_York",
                    "nws",
                    "weather.com",
                    True,
                    json.dumps({"note": "override"}),
                    "2026-03-01 00:00:00",
                    "2026-03-01 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO weather.weather_market_specs (
                    market_id, condition_id, location_name, station_id, latitude, longitude, timezone,
                    observation_date, observation_window_local, metric, unit, bucket_min_value, bucket_max_value,
                    authoritative_source, fallback_sources, rounding_rule, inclusive_bounds, spec_version,
                    parse_confidence, risk_flags_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "mkt_weather_1",
                    "cond_weather_1",
                    "New York City",
                    "KNYC",
                    40.7128,
                    -74.006,
                    "America/New_York",
                    "2026-03-08",
                    "daily_max",
                    "temperature_max",
                    "fahrenheit",
                    50.0,
                    59.0,
                    "weather.com",
                    json.dumps(["nws", "openmeteo"]),
                    "identity",
                    True,
                    "spec_123",
                    0.95,
                    json.dumps([]),
                    "2026-03-01 00:00:00",
                    "2026-03-01 00:00:00",
                ],
            )
            for run_id, fallback_used, created_at in [
                ("frun_original", False, "2026-03-07 12:00:00"),
                ("frun_replayed", False, "2026-03-10 00:00:00"),
            ]:
                con.execute(
                    """
                    INSERT INTO weather.weather_forecast_runs (
                        run_id, market_id, condition_id, station_id, source, model_run, forecast_target_time,
                        observation_date, metric, latitude, longitude, timezone, spec_version, cache_key,
                        source_trace_json, fallback_used, from_cache, confidence, forecast_payload_json,
                        raw_payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        "mkt_weather_1",
                        "cond_weather_1",
                        "KNYC",
                        "openmeteo",
                        "2026-03-07T12:00Z",
                        "2026-03-07 12:00:00",
                        "2026-03-08",
                        "temperature_max",
                        40.7128,
                        -74.006,
                        "America/New_York",
                        "spec_123",
                        "mkt_weather_1|KNYC|spec_123|openmeteo|2026-03-07T12:00Z|2026-03-07T12:00:00+00:00",
                        json.dumps(["openmeteo"]),
                        fallback_used,
                        False,
                        0.95,
                        json.dumps({"temperature_distribution": {"55": 1.0}}),
                        json.dumps({"daily": {"temperature_2m_max": [55.0]}}),
                        created_at,
                    ],
                )
            con.execute(
                """
                INSERT INTO weather.weather_forecast_replays (
                    replay_id, market_id, condition_id, station_id, source, model_run, forecast_target_time,
                    spec_version, replay_key, replay_reason, original_run_id, replayed_run_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "freplay_1",
                    "mkt_weather_1",
                    "cond_weather_1",
                    "KNYC",
                    "openmeteo",
                    "2026-03-07T12:00Z",
                    "2026-03-07 12:00:00",
                    "spec_123",
                    "mkt_weather_1|KNYC|spec_123|openmeteo|2026-03-07T12:00Z|2026-03-07T12:00:00+00:00",
                    "integration_test",
                    "frun_original",
                    "frun_replayed",
                    "2026-03-10 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO weather.weather_forecast_replay_diffs (
                    diff_id, replay_id, entity_type, entity_key, original_entity_id, replayed_entity_id, status,
                    diff_summary_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "fdiff_1",
                    "freplay_1",
                    "forecast_run",
                    "forecast_run",
                    "frun_original",
                    "frun_replayed",
                    "MATCH",
                    json.dumps({"changed_fields": []}),
                    "2026-03-10 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO weather.weather_fair_values (
                    fair_value_id, run_id, market_id, condition_id, token_id, outcome, fair_value, confidence, priced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "fv_replayed",
                    "frun_replayed",
                    "mkt_weather_1",
                    "cond_weather_1",
                    "tok_yes",
                    "YES",
                    0.7,
                    0.95,
                    "2026-03-10 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO weather.weather_watch_only_snapshots (
                    snapshot_id, fair_value_id, run_id, market_id, condition_id, token_id, outcome, reference_price,
                    fair_value, edge_bps, threshold_bps, decision, side, rationale, pricing_context_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "snap_replayed",
                    "fv_replayed",
                    "frun_replayed",
                    "mkt_weather_1",
                    "cond_weather_1",
                    "tok_yes",
                    "YES",
                    0.5,
                    0.7,
                    2000,
                    100,
                    "TAKE",
                    "BUY",
                    "integration",
                    json.dumps({"threshold_bps": 100}),
                    "2026-03-10 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO resolution.uma_proposals (
                    proposal_id, market_id, condition_id, proposer, proposed_outcome, proposal_bond, dispute_bond,
                    proposal_tx_hash, proposal_block_number, proposal_timestamp, status, on_chain_settled_at,
                    safe_redeem_after, human_review_required, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "prop_1",
                    "mkt_weather_1",
                    "cond_weather_1",
                    "0xabc",
                    "YES",
                    100.0,
                    None,
                    "0xhash",
                    100,
                    "2026-03-08 12:00:00",
                    "settled",
                    "2026-03-09 01:00:00",
                    "2026-03-10 01:00:00",
                    False,
                    "2026-03-08 12:00:00",
                    "2026-03-09 01:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO resolution.watcher_continuity_checks (
                    check_id, chain_id, from_block, to_block, last_known_finalized_block, status, gap_count, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "wcck_ok",
                    137,
                    100,
                    110,
                    110,
                    "OK",
                    0,
                    json.dumps({}),
                    "2026-03-10 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO resolution.settlement_verifications (
                    verification_id, proposal_id, market_id, proposed_outcome, expected_outcome, is_correct,
                    confidence, discrepancy_details, sources_checked, evidence_package, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "verify_1",
                    "prop_1",
                    "mkt_weather_1",
                    "YES",
                    "YES",
                    True,
                    0.95,
                    None,
                    json.dumps(["weather.com"]),
                    json.dumps({"evidence_package_id": "evidence_1"}),
                    "2026-03-10 00:00:00",
                ],
            )
            con.execute(
                """
                INSERT INTO resolution.proposal_evidence_links (
                    proposal_id, verification_id, evidence_package_id, linked_at
                ) VALUES (?, ?, ?, ?)
                """,
                ["prop_1", "verify_1", "evidence_1", "2026-03-10 00:00:00"],
            )
            con.execute(
                """
                INSERT INTO resolution.redeem_readiness_suggestions (
                    suggestion_id, proposal_id, decision, reason, on_chain_settled_at, safe_redeem_after,
                    human_review_required, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "redeem_1",
                    "prop_1",
                    "ready_for_redeem",
                    "ready",
                    "2026-03-09 01:00:00",
                    "2026-03-10 01:00:00",
                    False,
                    "2026-03-10 00:00:00",
                ],
            )
        finally:
            con.close()


def _canonical_table_counts(con) -> dict[str, int]:
    tables = [
        "weather.weather_markets",
        "weather.weather_market_specs",
        "weather.weather_forecast_runs",
        "weather.weather_forecast_replays",
        "weather.weather_forecast_replay_diffs",
        "weather.weather_fair_values",
        "weather.weather_watch_only_snapshots",
        "resolution.uma_proposals",
        "resolution.settlement_verifications",
        "resolution.proposal_evidence_links",
        "resolution.redeem_readiness_suggestions",
    ]
    return {table: int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}


if __name__ == "__main__":
    unittest.main()
