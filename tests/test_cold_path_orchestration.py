from __future__ import annotations

from contextlib import ExitStack
import importlib.util
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from agents.common import (
    AgentExecutionArtifacts,
    AgentInvocationStatus,
    AgentType,
    AgentVerdict,
    build_agent_evaluation_record,
    build_agent_invocation_record,
    build_agent_output_record,
    build_agent_review_record,
)
from asterion_core.contracts import (
    AccountTradingCapability,
    ForecastReplayResult,
    ForecastRunRecord,
    ForecastReplayRequest,
    ForecastResolutionContract,
    MarketCapability,
    ProposalStatus,
    ResolutionSpec,
    RouteAction,
    StrategyRun,
    TradeTicket,
    UMAProposal,
    WatchOnlySnapshotRecord,
    WeatherFairValueRecord,
    WeatherMarketSpecRecord,
    stable_object_id,
)
from asterion_core.storage.write_queue import WriteQueueConfig
from dagster_asterion import (
    DAGSTER_AVAILABLE,
    AsterionColdPathSettings,
    build_definitions,
    build_weather_cold_path_job_map,
    list_weather_cold_path_schedules,
)
from dagster_asterion.handlers import (
    SettlementVerificationInput,
    run_weather_paper_execution_job,
    run_weather_data_qa_review_job,
    run_weather_forecast_refresh,
    run_weather_forecast_replay_job,
    run_weather_resolution_review_job,
    run_weather_resolution_reconciliation,
    run_weather_rule2spec_review_job,
    run_weather_spec_sync,
    run_weather_watcher_backfill_job,
)
from dagster_asterion.resources import (
    DuckDBResource,
    ForecastRuntimeResource,
    WatcherRpcPoolResource,
    WriteQueueResource,
    build_dagster_resource_defs,
)
from dagster_asterion.schedules import build_schedule_definitions, list_enabled_schedule_keys
from domains.weather.forecast import ForecastDistribution, InMemoryForecastCache


HAS_DAGSTER = importlib.util.find_spec("dagster") is not None


def _settings() -> AsterionColdPathSettings:
    return AsterionColdPathSettings(
        db_path="data/asterion.duckdb",
        ddl_path=None,
        write_queue_path="data/meta/write_queue.sqlite",
        forecast_primary_source="openmeteo",
        forecast_fallback_sources=["nws", "openmeteo"],
        watcher_chain_id=137,
        watcher_rpc_urls=[],
    )


def _spec_record() -> WeatherMarketSpecRecord:
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
        parse_confidence=0.9,
        risk_flags=[],
    )


def _resolution_spec() -> ResolutionSpec:
    spec = _spec_record()
    return ResolutionSpec(
        market_id=spec.market_id,
        condition_id=spec.condition_id,
        location_name=spec.location_name,
        station_id=spec.station_id,
        latitude=spec.latitude,
        longitude=spec.longitude,
        timezone=spec.timezone,
        observation_date=spec.observation_date,
        observation_window_local=spec.observation_window_local,
        metric=spec.metric,
        unit=spec.unit,
        authoritative_source=spec.authoritative_source,
        fallback_sources=list(spec.fallback_sources),
        rounding_rule=spec.rounding_rule,
        inclusive_bounds=spec.inclusive_bounds,
        spec_version=spec.spec_version,
    )


def _forecast_distribution() -> ForecastDistribution:
    return ForecastDistribution(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
        observation_date=date(2026, 3, 8),
        metric="temperature_max",
        latitude=40.7128,
        longitude=-74.006,
        timezone="America/New_York",
        spec_version="spec_123",
        temperature_distribution={55: 1.0},
        source_trace=["openmeteo"],
        raw_payload={"source": "stub"},
        from_cache=False,
        fallback_used=False,
        cache_key="fck_test",
    )


def _forecast_run() -> ForecastRunRecord:
    return ForecastRunRecord(
        run_id="frun_test",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
        observation_date=date(2026, 3, 8),
        metric="temperature_max",
        latitude=40.7128,
        longitude=-74.006,
        timezone="America/New_York",
        spec_version="spec_123",
        cache_key="fck_test",
        source_trace=["openmeteo"],
        fallback_used=False,
        from_cache=False,
        confidence=1.0,
        forecast_payload={"temperature_distribution": {55: 1.0}},
        raw_payload={"source": "stub"},
    )


def _fair_value() -> WeatherFairValueRecord:
    return WeatherFairValueRecord(
        fair_value_id="fval_yes",
        run_id="frun_test",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        token_id="tok_yes",
        outcome="YES",
        fair_value=0.7,
        confidence=1.0,
    )


def _snapshot() -> WatchOnlySnapshotRecord:
    fair_value = _fair_value()
    return WatchOnlySnapshotRecord(
        snapshot_id="snap_yes",
        fair_value_id=fair_value.fair_value_id,
        run_id=fair_value.run_id,
        market_id=fair_value.market_id,
        condition_id=fair_value.condition_id,
        token_id=fair_value.token_id,
        outcome=fair_value.outcome,
        reference_price=0.5,
        fair_value=0.7,
        edge_bps=2000,
        threshold_bps=100,
        decision="TAKE",
        side="BUY",
        rationale="stub",
        pricing_context={"threshold_bps": 100},
    )


def _forecast_replay_result() -> ForecastReplayResult:
    request = ForecastReplayRequest(
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        station_id="KNYC",
        source="openmeteo",
        model_run="2026-03-07T12:00Z",
        forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
        spec_version="spec_123",
        replay_reason="test",
    )
    return ForecastReplayResult(
        replay_id="freplay_test",
        request=request,
        forecast_run=_forecast_run(),
        fair_values=[_fair_value()],
        watch_only_snapshots=[_snapshot()],
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


class ColdPathJobMapTest(unittest.TestCase):
    def test_job_map_contains_agent_manual_jobs(self) -> None:
        jobs = build_weather_cold_path_job_map()
        self.assertEqual(set(jobs), {
            "weather_spec_sync",
            "weather_forecast_refresh",
            "weather_forecast_replay",
            "weather_paper_execution",
            "weather_watcher_backfill",
            "weather_resolution_reconciliation",
            "weather_rule2spec_review",
            "weather_data_qa_review",
            "weather_resolution_review",
        })
        self.assertEqual(jobs["weather_forecast_refresh"].upstream_jobs, ["weather_spec_sync"])
        self.assertEqual(jobs["weather_watcher_backfill"].mode, "scheduled")
        self.assertEqual(jobs["weather_paper_execution"].mode, "manual")
        self.assertEqual(jobs["weather_paper_execution"].upstream_jobs, ["weather_forecast_replay"])
        self.assertEqual(jobs["weather_rule2spec_review"].mode, "manual")
        self.assertEqual(jobs["weather_data_qa_review"].upstream_jobs, ["weather_forecast_replay"])
        self.assertEqual(jobs["weather_resolution_review"].upstream_jobs, ["weather_resolution_reconciliation"])
        self.assertIn("resolution.watcher_continuity_checks", jobs["weather_watcher_backfill"].output_tables)

    def test_schedule_specs_keep_replay_manual_by_default(self) -> None:
        schedules = {item.schedule_key: item for item in list_weather_cold_path_schedules()}
        self.assertFalse(schedules["weather_forecast_replay_manual"].enabled_by_default)
        self.assertEqual(list_enabled_schedule_keys(), [
            "weather_spec_sync_daily",
            "weather_forecast_refresh_hourly",
            "weather_watcher_backfill_bihourly",
            "weather_resolution_reconciliation_bihourly",
        ])


class ColdPathResourcesTest(unittest.TestCase):
    def test_runtime_resources_build_without_dagster(self) -> None:
        settings = _settings()
        duckdb_resource = DuckDBResource(settings=settings)
        write_queue_resource = WriteQueueResource(settings=settings)
        forecast_runtime = ForecastRuntimeResource(settings=settings)
        watcher_rpc = WatcherRpcPoolResource(settings=settings)

        self.assertEqual(duckdb_resource.get_config().db_path, settings.db_path)
        self.assertEqual(write_queue_resource.get_config().path, settings.write_queue_path)
        self.assertIsInstance(forecast_runtime.build_cache(), InMemoryForecastCache)
        with self.assertRaises(ValueError):
            watcher_rpc.build_rpc_pool()

    def test_build_dagster_resource_defs_is_safe_without_optional_dep(self) -> None:
        resource_defs = build_dagster_resource_defs(_settings())
        self.assertIn("duckdb", resource_defs)
        if HAS_DAGSTER:
            self.assertNotIsInstance(resource_defs["duckdb"], DuckDBResource)
        else:
            self.assertIsInstance(resource_defs["duckdb"], DuckDBResource)


class ColdPathDefinitionsTest(unittest.TestCase):
    def test_top_level_import_and_definitions_smoke(self) -> None:
        defs = build_definitions(_settings())
        if HAS_DAGSTER:
            self.assertIsNotNone(defs)
        else:
            self.assertIsNone(defs)

    def test_schedule_definitions_build_only_when_dagster_available(self) -> None:
        schedules = build_schedule_definitions(job_definitions={})
        if HAS_DAGSTER:
            self.assertEqual(schedules, [])
        else:
            self.assertEqual(schedules, [])


class ColdPathHandlersSmokeTest(unittest.TestCase):
    def test_weather_paper_execution_rejects_invalid_selectors(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        with self.assertRaises(ValueError):
            run_weather_paper_execution_job(
                object(),
                queue_cfg,
                params_json={
                    "wallet_id": "wallet_weather_1",
                    "strategy_registrations": [],
                    "snapshot_ids": ["snap_yes"],
                },
            )
        with self.assertRaises(ValueError):
            run_weather_paper_execution_job(
                object(),
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
                        }
                    ],
                    "snapshot_ids": ["snap_yes"],
                    "market_ids": ["mkt_weather_1"],
                },
            )
        with self.assertRaises(ValueError):
            run_weather_paper_execution_job(
                object(),
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
                        }
                    ],
                    "market_ids": ["mkt_weather_1"],
                },
            )

    def test_weather_paper_execution_routes_to_runtime_and_capability_persistence(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        account_capability = AccountTradingCapability(
            wallet_id="wallet_weather_1",
            wallet_type="eoa",
            signature_type=1,
            funder="0xfunder",
            allowance_targets=["0xrelayer"],
            can_use_relayer=True,
            can_trade=True,
            restricted_reason=None,
        )
        market_capability = MarketCapability(
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
            data_sources=["gamma"],
            updated_at=datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc),
        )
        strategy_run = StrategyRun(
            run_id="srun_1",
            data_snapshot_id="dsnap_1",
            universe_snapshot_id="usnap_1",
            asof_ts_ms=1710000000000,
            dq_level="PASS",
            strategy_ids=["weather_primary"],
            decision_count=2,
            created_at=datetime(2026, 3, 10, 10, 1, tzinfo=timezone.utc),
        )
        tickets = [
            TradeTicket(
                ticket_id="tt_1",
                run_id="srun_1",
                strategy_id="weather_primary",
                strategy_version="v1",
                market_id="mkt_weather_1",
                token_id="tok_yes",
                outcome="YES",
                side="buy",
                reference_price=Decimal("0.55"),
                fair_value=Decimal("0.63"),
                edge_bps=800,
                threshold_bps=500,
                route_action=RouteAction.FAK,
                size=Decimal("10"),
                signal_ts_ms=1710000000000,
                forecast_run_id="frun_weather_1",
                watch_snapshot_id="snap_yes",
                request_id="req_1",
                ticket_hash="thash_1",
                provenance_json={"watch_snapshot_id": "snap_yes"},
                created_at=datetime(2026, 3, 10, 10, 1, tzinfo=timezone.utc),
            ),
            TradeTicket(
                ticket_id="tt_2",
                run_id="srun_1",
                strategy_id="weather_primary",
                strategy_version="v1",
                market_id="mkt_weather_1",
                token_id="tok_yes",
                outcome="YES",
                side="buy",
                reference_price=Decimal("0.56"),
                fair_value=Decimal("0.64"),
                edge_bps=900,
                threshold_bps=500,
                route_action=RouteAction.FAK,
                size=Decimal("10"),
                signal_ts_ms=1710000000001,
                forecast_run_id="frun_weather_1",
                watch_snapshot_id="snap_yes_2",
                request_id="req_2",
                ticket_hash="thash_2",
                provenance_json={"watch_snapshot_id": "snap_yes_2"},
                created_at=datetime(2026, 3, 10, 10, 1, tzinfo=timezone.utc),
            ),
        ]
        fake_record = type("ExecutionContextRecordStub", (), {"execution_context_id": "ectx_1", "execution_context": object()})()
        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "dagster_asterion.handlers.load_selected_watch_only_snapshots",
                    return_value=(
                        [_snapshot(), _snapshot()],
                        {"snap_yes": 1710000000000},
                        {"snap_yes": datetime(2026, 3, 10, 10, 0, tzinfo=timezone.utc)},
                    ),
                )
            )
            stack.enter_context(patch("dagster_asterion.handlers.load_account_trading_capability", return_value=account_capability))
            stack.enter_context(patch("dagster_asterion.handlers.load_market_capability", return_value=market_capability))
            stack.enter_context(patch("dagster_asterion.handlers.run_strategy_engine", return_value=(strategy_run, [object(), object()])))
            stack.enter_context(patch("dagster_asterion.handlers.build_trade_ticket", side_effect=tickets))
            build_context = stack.enter_context(patch("dagster_asterion.handlers.build_execution_context", return_value=object()))
            build_record = stack.enter_context(patch("dagster_asterion.handlers.build_execution_context_record", return_value=fake_record))
            stack.enter_context(
                patch(
                    "dagster_asterion.handlers.evaluate_execution_gate",
                    return_value=type(
                        "GateDecisionStub",
                        (),
                        {"gate_id": "gate_1", "allowed": True, "reason": "allowed", "reason_codes": [], "metrics_json": {}},
                    )(),
                )
            )
            stack.enter_context(patch("dagster_asterion.handlers.route_trade_ticket", return_value=object()))
            stack.enter_context(patch("dagster_asterion.handlers.build_paper_order", return_value=type("OrderStub", (), {"order_id": "ordr_1"})()))
            stack.enter_context(
                patch("dagster_asterion.handlers.build_order_state_transition", return_value=type("TransitionStub", (), {"transition_id": "otrans_1"})())
            )
            stack.enter_context(patch("dagster_asterion.handlers.paper_order_journal_payload", return_value={"order_id": "ordr_1", "status": "posted"}))
            stack.enter_context(patch("dagster_asterion.handlers.paper_order_journal_payload_with_status", return_value={"order_id": "ordr_1", "status": "created"}))
            stack.enter_context(patch("dagster_asterion.handlers.gate_rejection_journal_payload", return_value={"reason": "blocked"}))
            stack.enter_context(
                patch("dagster_asterion.handlers.canonical_order_router_payload", return_value={"route_action": "fak", "router_reason": "route_action_normalized"})
            )
            stack.enter_context(patch("dagster_asterion.handlers.canonical_order_router_hash", return_value="coh_1"))
            stack.enter_context(patch("dagster_asterion.handlers.build_signal_order_intent_from_handoff", return_value=object()))
            stack.enter_context(patch("dagster_asterion.handlers.canonical_order_handoff_payload", return_value={"route_action": "fak", "post_only": False}))
            stack.enter_context(patch("dagster_asterion.handlers.enqueue_strategy_run_upserts", return_value="task_strategy"))
            stack.enter_context(patch("dagster_asterion.handlers.enqueue_trade_ticket_upserts", return_value="task_ticket"))
            stack.enter_context(patch("dagster_asterion.handlers.enqueue_gate_decision_upserts", return_value="task_gate"))
            stack.enter_context(patch("dagster_asterion.handlers.enqueue_order_upserts", return_value="task_order"))
            stack.enter_context(patch("dagster_asterion.handlers.enqueue_order_state_transition_upserts", return_value="task_transition"))
            enqueue_contexts = stack.enter_context(patch("dagster_asterion.handlers.enqueue_execution_context_upserts", return_value="task_context"))
            stack.enter_context(patch("dagster_asterion.handlers.enqueue_journal_event_upserts", return_value="task_journal"))
            result = run_weather_paper_execution_job(
                object(),
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
                        }
                    ],
                    "snapshot_ids": ["snap_yes", "snap_yes"],
                },
            )
        self.assertEqual(
            result.task_ids,
            ["task_strategy", "task_ticket", "task_gate", "task_order", "task_transition", "task_context", "task_journal"],
        )
        self.assertEqual(result.metadata["ticket_count"], 2)
        self.assertEqual(result.metadata["ticket_ids"], ["tt_1", "tt_2"])
        self.assertEqual(result.metadata["gate_count"], 2)
        self.assertEqual(result.metadata["allowed_order_count"], 2)
        self.assertEqual(result.metadata["order_ids"], ["ordr_1", "ordr_1"])
        self.assertEqual(result.metadata["rejected_ticket_ids"], [])
        self.assertEqual(result.metadata["execution_context_count"], 1)
        self.assertEqual(result.metadata["ticket_execution_context_ids"], {"tt_1": "ectx_1", "tt_2": "ectx_1"})
        self.assertEqual(build_context.call_count, 2)
        build_record.assert_called()
        enqueue_contexts.assert_called_once()

    def test_weather_spec_sync_routes_to_rule2spec_and_station_mapper(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        market = type("Market", (), {"market_id": "m1"})()
        draft = object()
        record = _spec_record()
        with (
            patch("dagster_asterion.handlers.load_weather_markets_for_rule2spec", return_value=[market]) as load_markets,
            patch("dagster_asterion.handlers.parse_rule2spec_draft", return_value=draft) as parse_draft,
            patch("dagster_asterion.handlers.build_weather_market_spec_record_via_station_mapper", return_value=record) as build_record,
            patch("dagster_asterion.handlers.enqueue_weather_market_spec_upserts", return_value="task_specs") as enqueue_specs,
        ):
            result = run_weather_spec_sync(object(), queue_cfg, mapper=object())
        load_markets.assert_called_once()
        parse_draft.assert_called_once_with(market)
        build_record.assert_called_once()
        enqueue_specs.assert_called_once()
        self.assertEqual(result.task_ids, ["task_specs"])

    def test_weather_forecast_refresh_routes_to_forecast_persistence(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")

        class _StubService:
            def get_forecast(self, *args, **kwargs):
                return _forecast_distribution()

        with (
            patch("dagster_asterion.handlers._load_weather_market_specs", return_value=[_spec_record()]) as load_specs,
            patch("dagster_asterion.handlers.build_forecast_run_record", return_value=_forecast_run()) as build_record,
            patch("dagster_asterion.handlers.enqueue_forecast_run_upserts", return_value="task_forecast") as enqueue_runs,
        ):
            result = run_weather_forecast_refresh(
                object(),
                queue_cfg,
                forecast_service=_StubService(),
                source="openmeteo",
                model_run="2026-03-07T12:00Z",
                forecast_target_time=datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
            )
        load_specs.assert_called_once()
        build_record.assert_called_once()
        enqueue_runs.assert_called_once()
        self.assertEqual(result.task_ids, ["task_forecast"])

    def test_weather_forecast_replay_routes_to_replay_pipeline(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        replay_result = _forecast_replay_result()
        with (
            patch("dagster_asterion.handlers.run_forecast_replay", return_value=replay_result) as run_replay,
            patch("dagster_asterion.handlers.load_replay_inputs", return_value=(_spec_record(), _forecast_run(), object())),
            patch("dagster_asterion.handlers.load_original_pricing_outputs", return_value=([_fair_value()], [_snapshot()])),
            patch("dagster_asterion.handlers.build_forecast_replay_record", return_value=type("ReplayRecord", (), {"replay_id": "freplay_test"})()),
            patch("dagster_asterion.handlers.build_forecast_replay_diff_records", return_value=[]),
            patch("dagster_asterion.handlers.enqueue_forecast_run_upserts", return_value="task_runs"),
            patch("dagster_asterion.handlers.enqueue_fair_value_upserts", return_value="task_fair"),
            patch("dagster_asterion.handlers.enqueue_watch_only_snapshot_upserts", return_value="task_snap"),
            patch("dagster_asterion.handlers.enqueue_forecast_replay_upserts", return_value="task_replay"),
            patch("dagster_asterion.handlers.enqueue_forecast_replay_diff_upserts", return_value="task_diff"),
        ):
            result = run_weather_forecast_replay_job(
                object(),
                queue_cfg,
                adapter_router=object(),
                cache=object(),
                replay_requests=[
                    {
                        "market_id": "mkt_weather_1",
                        "station_id": "KNYC",
                        "source": "openmeteo",
                        "model_run": "2026-03-07T12:00Z",
                        "forecast_target_time": datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc),
                        "spec_version": "spec_123",
                        "replay_reason": "test",
                    }
                ],
            )
        run_replay.assert_called_once()
        self.assertEqual(result.task_ids, ["task_runs", "task_fair", "task_snap", "task_replay", "task_diff"])

    def test_weather_watcher_backfill_routes_to_backfill_pipeline(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        fake_result = type(
            "WatcherResult",
            (),
            {
                "run_id": "req_test",
                "processed_events_written": 2,
                "continuity_check_id": "wcck_test",
                "finalized_block": 110,
                "rpc_trace": {"events": {}},
            },
        )()
        with (
            patch("dagster_asterion.handlers.run_watcher_backfill", return_value=fake_result) as run_backfill,
            patch("dagster_asterion.handlers.persist_watcher_backfill", return_value=["task_backfill"]) as persist,
        ):
            result = run_weather_watcher_backfill_job(
                object(),
                queue_cfg,
                rpc_pool=object(),
                chain_id=137,
                replay_reason="test",
            )
        run_backfill.assert_called_once()
        persist.assert_called_once()
        self.assertEqual(result.task_ids, ["task_backfill"])

    def test_weather_resolution_reconciliation_routes_to_verification_pipeline(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        with (
            patch("dagster_asterion.handlers._load_uma_proposals_for_reconciliation", return_value=[_proposal()]),
            patch("dagster_asterion.handlers.enqueue_settlement_verification_upserts", return_value="task_verify") as enqueue_verify,
            patch("dagster_asterion.handlers.enqueue_evidence_link_upserts", return_value="task_link") as enqueue_link,
            patch("dagster_asterion.handlers.enqueue_redeem_readiness_upserts", return_value="task_redeem") as enqueue_redeem,
        ):
            result = run_weather_resolution_reconciliation(
                object(),
                queue_cfg,
                verification_inputs=[
                    SettlementVerificationInput(
                        proposal_id="prop_1",
                        expected_outcome="YES",
                        confidence=0.95,
                        sources_checked=["nws"],
                        evidence_payload={"observed_value": 55},
                    )
                ],
            )
        enqueue_verify.assert_called_once()
        enqueue_link.assert_called_once()
        enqueue_redeem.assert_called_once()
        self.assertEqual(result.task_ids, ["task_verify", "task_link", "task_redeem"])

    def test_weather_rule2spec_review_routes_to_agent_pipeline(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        artifacts = [_agent_artifacts("weather_market", "mkt_weather_1")]
        with (
            patch("dagster_asterion.handlers.load_rule2spec_agent_requests", return_value=[object()]) as load_requests,
            patch("dagster_asterion.handlers.run_rule2spec_agent_review", return_value=artifacts[0]) as run_agent,
            patch("dagster_asterion.handlers.enqueue_agent_artifact_upserts", return_value=["task_agent"]) as enqueue_agent,
        ):
            result = run_weather_rule2spec_review_job(
                object(),
                queue_cfg,
                client=object(),
            )
        load_requests.assert_called_once()
        run_agent.assert_called_once()
        enqueue_agent.assert_called_once()
        self.assertEqual(result.task_ids, ["task_agent"])

    def test_weather_data_qa_review_routes_to_agent_pipeline(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        artifacts = [_agent_artifacts("forecast_replay", "freplay_test")]
        with (
            patch("dagster_asterion.handlers.load_data_qa_agent_requests", return_value=[object()]) as load_requests,
            patch("dagster_asterion.handlers.run_data_qa_agent_review", return_value=artifacts[0]) as run_agent,
            patch("dagster_asterion.handlers.enqueue_agent_artifact_upserts", return_value=["task_agent"]) as enqueue_agent,
        ):
            result = run_weather_data_qa_review_job(
                object(),
                queue_cfg,
                client=object(),
            )
        load_requests.assert_called_once()
        run_agent.assert_called_once()
        enqueue_agent.assert_called_once()
        self.assertEqual(result.task_ids, ["task_agent"])

    def test_weather_resolution_review_routes_to_agent_pipeline(self) -> None:
        queue_cfg = WriteQueueConfig(path=":memory:")
        artifacts = [_agent_artifacts("uma_proposal", "prop_1")]
        with (
            patch("dagster_asterion.handlers.load_resolution_agent_requests", return_value=[object()]) as load_requests,
            patch("dagster_asterion.handlers.run_resolution_agent_review", return_value=artifacts[0]) as run_agent,
            patch("dagster_asterion.handlers.enqueue_agent_artifact_upserts", return_value=["task_agent"]) as enqueue_agent,
        ):
            result = run_weather_resolution_review_job(
                object(),
                queue_cfg,
                client=object(),
            )
        load_requests.assert_called_once()
        run_agent.assert_called_once()
        enqueue_agent.assert_called_once()
        self.assertEqual(result.task_ids, ["task_agent"])


def _agent_artifacts(subject_type: str, subject_id: str) -> AgentExecutionArtifacts:
    invocation = build_agent_invocation_record(
        agent_type=AgentType.RULE2SPEC,
        agent_version="v1",
        prompt_version="p1",
        subject_type=subject_type,
        subject_id=subject_id,
        input_payload_json={"subject_id": subject_id},
        model_provider="fake",
        model_name="fake",
        status=AgentInvocationStatus.SUCCESS,
        started_at=datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 3, 10, 0, 0, 1, tzinfo=timezone.utc),
        latency_ms=1000,
    )
    output = build_agent_output_record(
        invocation_id=invocation.invocation_id,
        verdict=AgentVerdict.PASS,
        confidence=0.9,
        summary="ok",
        findings=[],
        structured_output_json={"verdict": "pass"},
        human_review_required=False,
        created_at=datetime(2026, 3, 10, 0, 0, 1, tzinfo=timezone.utc),
    )
    review = build_agent_review_record(
        invocation_id=invocation.invocation_id,
        human_review_required=False,
        review_payload_json={},
        reviewed_at=datetime(2026, 3, 10, 0, 0, 1, tzinfo=timezone.utc),
    )
    evaluation = build_agent_evaluation_record(
        invocation_id=invocation.invocation_id,
        confidence=0.9,
        human_review_required=False,
        created_at=datetime(2026, 3, 10, 0, 0, 1, tzinfo=timezone.utc),
    )
    return AgentExecutionArtifacts(invocation=invocation, output=output, review=review, evaluation=evaluation)


if __name__ == "__main__":
    unittest.main()
