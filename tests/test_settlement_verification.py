from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from asterion_core.contracts import ProposalStatus, RedeemDecision, RedeemScheduleInput
from asterion_core.storage.db_migrate import MigrationConfig, apply_migrations
from asterion_core.storage.write_queue import WriteQueueConfig
from asterion_core.storage.writerd import process_one
from domains.weather.resolution import (
    RedeemScheduler,
    build_evidence_package_link,
    build_redeem_readiness_record,
    build_settlement_verification,
    enqueue_evidence_link_upserts,
    enqueue_redeem_readiness_upserts,
    enqueue_settlement_verification_upserts,
    enqueue_uma_replay_writes,
    load_uma_proposals,
    replay_uma_events,
)

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


def _proposal_created():
    from domains.weather.resolution import UMAEvent

    return UMAEvent(
        tx_hash="0xaaa",
        log_index=1,
        block_number=100,
        event_type="proposal_created",
        proposal_id="prop_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        proposer="0xproposer",
        proposed_outcome="YES",
        proposal_bond=100.0,
        dispute_bond=None,
        proposal_timestamp=datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc),
        on_chain_settled_at=None,
        safe_redeem_after=None,
        human_review_required=False,
    )


def _proposal_settled():
    from domains.weather.resolution import UMAEvent

    return UMAEvent(
        tx_hash="0xbbb",
        log_index=2,
        block_number=110,
        event_type="proposal_settled",
        proposal_id="prop_1",
        market_id="mkt_weather_1",
        condition_id="cond_weather_1",
        proposer=None,
        proposed_outcome=None,
        proposal_bond=None,
        dispute_bond=None,
        proposal_timestamp=None,
        on_chain_settled_at=datetime(2026, 3, 9, 1, 0, tzinfo=timezone.utc),
        safe_redeem_after=datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc),
        human_review_required=True,
    )


class SettlementVerificationUnitTest(unittest.TestCase):
    def test_redeem_scheduler_outputs_expected_decisions(self) -> None:
        scheduler = RedeemScheduler()
        now = datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc)

        not_ready = scheduler.decide(
            schedule_input=RedeemScheduleInput(
                proposal_status=ProposalStatus.PROPOSED,
                on_chain_settled_at=None,
                safe_redeem_after=None,
                human_review_required=False,
            ),
            now=now,
        )
        blocked = scheduler.decide(
            schedule_input=RedeemScheduleInput(
                proposal_status=ProposalStatus.SETTLED,
                on_chain_settled_at=now,
                safe_redeem_after=now,
                human_review_required=True,
            ),
            now=now,
        )
        ready = scheduler.decide(
            schedule_input=RedeemScheduleInput(
                proposal_status=ProposalStatus.SETTLED,
                on_chain_settled_at=now,
                safe_redeem_after=datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc),
                human_review_required=False,
            ),
            now=now,
        )

        self.assertEqual(not_ready.decision, RedeemDecision.NOT_REDEEMABLE)
        self.assertEqual(blocked.decision, RedeemDecision.BLOCKED_PENDING_REVIEW)
        self.assertEqual(ready.decision, RedeemDecision.READY_FOR_REDEEM)


@unittest.skipUnless(HAS_DUCKDB, "duckdb is required for settlement verification tests")
class SettlementVerificationDuckDBTest(unittest.TestCase):
    def test_settled_proposal_links_verification_and_redeem_suggestion(self) -> None:
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

            proposals, transitions, _ = replay_uma_events(events=[_proposal_created(), _proposal_settled()])
            queue_cfg = WriteQueueConfig(path=queue_path)
            enqueue_uma_replay_writes(
                queue_cfg,
                chain_id=137,
                proposals=proposals,
                transitions=transitions,
                processed_events=[_proposal_created(), _proposal_settled()],
                last_processed_block=110,
                last_finalized_block=110,
                run_id="run_uma_replay",
            )

            allow = ",".join(
                [
                    "resolution.uma_proposals",
                    "resolution.proposal_state_transitions",
                    "resolution.processed_uma_events",
                    "resolution.block_watermarks",
                    "resolution.settlement_verifications",
                    "resolution.proposal_evidence_links",
                    "resolution.redeem_readiness_suggestions",
                ]
            )
            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow},
                clear=False,
            ):
                for _ in range(4):
                    self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

                from asterion_core.storage.database import DuckDBConfig, connect_duckdb

                reader_env = {
                    "ASTERION_STRICT_SINGLE_WRITER": "1",
                    "ASTERION_DB_ROLE": "reader",
                    "WRITERD": "0",
                }
                with patch.dict(os.environ, reader_env, clear=False):
                    con = connect_duckdb(DuckDBConfig(db_path=db_path, ddl_path=None))
                    try:
                        proposal = load_uma_proposals(con)["prop_1"]
                    finally:
                        con.close()

            verification = build_settlement_verification(
                proposal=proposal,
                expected_outcome="YES",
                confidence=0.93,
                sources_checked=["nws", "openmeteo"],
                evidence_payload={"observed_value": 55},
            )
            evidence_link = build_evidence_package_link(verification)
            redeem = build_redeem_readiness_record(
                proposal,
                scheduler=RedeemScheduler(),
                now=datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc),
            )

            enqueue_settlement_verification_upserts(queue_cfg, verifications=[verification], run_id="run_verify")
            enqueue_evidence_link_upserts(queue_cfg, links=[evidence_link], run_id="run_verify")
            enqueue_redeem_readiness_upserts(queue_cfg, suggestions=[redeem], run_id="run_verify")

            with patch.dict(
                os.environ,
                {"ASTERION_DB_PATH": db_path, "ASTERION_WRITERD_ALLOWED_TABLES": allow},
                clear=False,
            ):
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))
                self.assertTrue(process_one(queue_path=queue_path, db_path=db_path, ddl_path=None, apply_schema=False))

            import duckdb

            con = duckdb.connect(db_path, read_only=True)
            try:
                verification_row = con.execute(
                    """
                    SELECT expected_outcome, is_correct, confidence, sources_checked, evidence_package
                    FROM resolution.settlement_verifications
                    WHERE proposal_id = 'prop_1'
                    """
                ).fetchone()
                link_row = con.execute(
                    """
                    SELECT verification_id, evidence_package_id
                    FROM resolution.proposal_evidence_links
                    WHERE proposal_id = 'prop_1'
                    """
                ).fetchone()
                redeem_row = con.execute(
                    """
                    SELECT decision, reason, human_review_required
                    FROM resolution.redeem_readiness_suggestions
                    WHERE proposal_id = 'prop_1'
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertEqual(verification_row[0], "YES")
            self.assertTrue(verification_row[1])
            self.assertEqual(verification_row[2], 0.93)
            self.assertEqual(json.loads(verification_row[3]), ["nws", "openmeteo"])
            self.assertEqual(json.loads(verification_row[4])["evidence_package_id"], verification.evidence_package_id)
            self.assertEqual(link_row[0], verification.verification_id)
            self.assertEqual(link_row[1], verification.evidence_package_id)
            self.assertEqual(redeem_row[0], RedeemDecision.BLOCKED_PENDING_REVIEW.value)
            self.assertTrue(redeem_row[2])


if __name__ == "__main__":
    unittest.main()
