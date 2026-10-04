import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def stable_id(source_id, record_id):
    return "off-" + hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]


class OfflineMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.actor = Actor("admin", "admin")
        self.station_id = stable_id("offline", "station")
        self.service.merge_offline(
            self.actor,
            [{"entity_kind": "station", "source_id": "offline", "record_id": "station",
              "name": "OSN-01", "region": "East"}],
        )

    def tearDown(self):
        self.tmp.cleanup()

    def merge(self, *records):
        return self.service.merge_offline(self.actor, list(records))

    def result_of(self, batch, index=0):
        return batch["results"][index]

    def incident(self, source_id, record_id, **overrides):
        data = {"entity_kind": "incident", "source_id": source_id, "record_id": record_id,
                "station_id": self.station_id, "kind": "loss", "severity": "high", "summary": "x"}
        data.update(overrides)
        return data

    def test_merge_creates_entities_and_marks_queue_done(self):
        batch = self.merge(
            {"entity_kind": "station", "source_id": "s", "record_id": "1", "name": "OSN-02", "region": "East"},
        )
        self.assertEqual(batch["summary"]["merged"], 1)
        self.assertEqual(batch["summary"]["failed"], 0)
        entity = self.repo.get_entity(stable_id("s", "1"))
        self.assertIsNotNone(entity)
        self.assertEqual(entity["data"]["name"], "OSN-02")
        self.assertEqual(self.repo.list_queue(status="pending"), [])

    def test_unchanged_merge_counts_as_merged(self):
        self.merge(self.incident("fm", "1", kind="outage", severity="low", summary="A"))
        # Re-merge identical fields -> no change, but successfully processed.
        batch = self.merge(self.incident("fm", "1", kind="outage", severity="low", summary="A"))
        self.assertEqual(batch["summary"]["merged"], 1)
        self.assertEqual(batch["summary"]["failed"], 0)

    def test_late_telemetry_revision_pending_copy(self):
        st_id = stable_id("off", "st")
        as_id = stable_id("off", "as")
        self.merge(
            {"entity_kind": "station", "source_id": "off", "record_id": "st", "name": "S", "region": "R"},
            {"entity_kind": "asset", "source_id": "off", "record_id": "as", "station_id": st_id,
             "asset_type": "sensor", "serial_no": "X", "last_seen": "2026-10-01T00:00:00Z"},
        )
        # Center telemetry at revision 5.
        self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel", "asset_id": as_id,
             "metric": "pressure", "value": 1, "observed_at": "2026-10-01T00:00:00Z", "revision": 5},
        )
        # Late arrival with lower revision -> pending copy, not an overwrite.
        batch = self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel2", "asset_id": as_id,
             "metric": "pressure", "value": 0.8, "observed_at": "2026-09-30T00:00:00Z", "revision": 3},
        )
        result = self.result_of(batch)
        self.assertEqual(result["result"]["status"], "pending")
        self.assertEqual(result["result"]["copy"]["reason"], "late_revision")
        # Center telemetry is untouched.
        self.assertEqual(self.repo.get_entity(stable_id("off", "tel"))["data"]["revision"], 5)

    def test_merge_order_incident_before_action(self):
        # The action references the incident; the incident must merge first.
        inc_id = stable_id("ord", "inc")
        batch = self.merge(
            {"entity_kind": "recovery_action", "source_id": "ord", "record_id": "act",
             "incident_id": inc_id, "action_type": "remote_restart", "dedupe_key": "ord-1"},
            self.incident("ord", "inc", summary="order"),
        )
        self.assertEqual(batch["summary"]["merged"], 2)
        self.assertIsNotNone(self.repo.get_entity(inc_id))
        self.assertIsNotNone(self.repo.get_entity(stable_id("ord", "act")))

    def test_field_merge_applies_incoming_only_change(self):
        fm_id = stable_id("fm", "1")
        self.merge(self.incident("fm", "1", kind="outage", severity="low", summary="A"))
        batch = self.merge(self.incident("fm", "1", kind="outage", severity="high", summary="A"))
        self.assertEqual(self.result_of(batch)["result"]["status"], "merged")
        entity = self.repo.get_entity(fm_id)
        self.assertEqual(entity["data"]["severity"], "high")
        self.assertEqual(entity["data"]["summary"], "A")

    def test_field_merge_conflict_pending_copy(self):
        fm_id = stable_id("fm", "1")
        self.merge(self.incident("fm", "1", kind="outage", severity="low", summary="A"))
        # Center changes summary.
        entity = self.repo.get_entity(fm_id)
        self.repo.update_entity(fm_id, entity["version"], entity["status"],
                                dict(entity["data"], summary="B"))
        # Offline also changed summary -> field conflict.
        batch = self.merge(self.incident("fm", "1", kind="outage", severity="low", summary="C"))
        result = self.result_of(batch)
        self.assertEqual(result["result"]["status"], "pending")
        self.assertEqual(result["result"]["copy"]["reason"], "field_conflict")
        self.assertEqual(result["result"]["copy"]["payload"]["_conflict_fields"], ["summary"])
        # The center value is untouched.
        self.assertEqual(self.repo.get_entity(fm_id)["data"]["summary"], "B")

    def test_telemetry_two_version_blocks_resolve_then_select(self):
        st_id = stable_id("off", "st")
        as_id = stable_id("off", "as")
        self.merge(
            {"entity_kind": "station", "source_id": "off", "record_id": "st", "name": "S", "region": "R"},
            {"entity_kind": "asset", "source_id": "off", "record_id": "as", "station_id": st_id,
             "asset_type": "sensor", "serial_no": "X", "last_seen": "2026-10-01T00:00:00Z"},
        )
        tel_id = stable_id("off", "tel")
        self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel", "asset_id": as_id,
             "metric": "pressure", "value": 1, "observed_at": "2026-10-01T00:00:00Z", "revision": 1},
        )
        self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel", "asset_id": as_id,
             "metric": "pressure", "value": 1.5, "observed_at": "2026-10-01T00:00:00Z", "revision": 1},
        )
        inc_id = stable_id("off", "inc")
        self.merge(self.incident("off", "inc", asset_id=as_id))
        for action in ("diagnose", "plan_recovery", "start_recovery"):
            self.service.transition(self.actor, inc_id, action)
        # Two versions pending -> resolve blocked.
        with self.assertRaises(ConflictError):
            self.service.transition(self.actor, inc_id, "resolve", {"summary": "done"})
        # Select the incoming version.
        copy = self.repo.list_pending_copies(status="pending")[0]
        self.service.apply_pending_copy(self.actor, copy["id"])
        resolved = self.service.transition(self.actor, inc_id, "resolve", {"summary": "done"})
        self.assertEqual(resolved["status"], "resolved")
        self.assertEqual(resolved["data"]["closing_basis"]["telemetry"]["pressure"], 1)

    def test_duplicate_action_first_writer_wins(self):
        inc_id = stable_id("off", "inc")
        self.merge(self.incident("off", "inc"))
        first = self.merge(
            {"entity_kind": "recovery_action", "source_id": "s1", "record_id": "r1",
             "incident_id": inc_id, "action_type": "remote_restart", "dedupe_key": "k"},
        )
        self.assertEqual(self.result_of(first)["result"]["status"], "merged")
        second = self.merge(
            {"entity_kind": "recovery_action", "source_id": "s2", "record_id": "r2",
             "incident_id": inc_id, "action_type": "remote_restart", "dedupe_key": "k"},
        )
        result = self.result_of(second)
        self.assertEqual(result["result"]["status"], "pending")
        self.assertEqual(result["result"]["copy"]["reason"], "duplicate")

    def test_duplicate_gap_pending_copy(self):
        inc_id = stable_id("off", "inc")
        self.merge(self.incident("off", "inc"))
        first = self.merge(
            {"entity_kind": "gap", "source_id": "g1", "record_id": "r1", "incident_id": inc_id,
             "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-01T00:30:00Z"},
        )
        self.assertEqual(self.result_of(first)["result"]["status"], "merged")
        second = self.merge(
            {"entity_kind": "gap", "source_id": "g2", "record_id": "r2", "incident_id": inc_id,
             "start_at": "2026-10-01T01:00:00Z", "end_at": "2026-10-01T01:30:00Z"},
        )
        result = self.result_of(second)
        self.assertEqual(result["result"]["status"], "pending")
        self.assertEqual(result["result"]["copy"]["reason"], "duplicate")

    def test_new_telemetry_invalidates_closing_basis(self):
        st_id = stable_id("off", "st")
        as_id = stable_id("off", "as")
        self.merge(
            {"entity_kind": "station", "source_id": "off", "record_id": "st", "name": "S", "region": "R"},
            {"entity_kind": "asset", "source_id": "off", "record_id": "as", "station_id": st_id,
             "asset_type": "sensor", "serial_no": "X", "last_seen": "2026-10-01T00:00:00Z"},
        )
        self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel", "asset_id": as_id,
             "metric": "pressure", "value": 1, "observed_at": "2026-10-01T00:00:00Z", "revision": 1},
        )
        inc_id = stable_id("off", "inc")
        self.merge(self.incident("off", "inc", asset_id=as_id))
        for action in ("diagnose", "plan_recovery", "start_recovery"):
            self.service.transition(self.actor, inc_id, action)
        resolved = self.service.transition(self.actor, inc_id, "resolve", {"summary": "done"})
        self.assertEqual(resolved["status"], "resolved")
        # New telemetry arrives -> basis invalidated, incident reopened.
        self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel2", "asset_id": as_id,
             "metric": "pressure", "value": 2, "observed_at": "2026-10-02T00:00:00Z", "revision": 2},
        )
        incident = self.service.get(inc_id)
        self.assertEqual(incident["status"], "open")
        self.assertTrue(incident["data"]["closing_basis"]["stale"])

    def test_write_failure_retried_on_restart(self):
        call = {"count": 0}
        original = self.repo.create_entity

        def flaky(*args, **kwargs):
            call["count"] += 1
            if call["count"] == 1:
                raise Exception("database is locked")
            return original(*args, **kwargs)

        with mock.patch.object(self.repo, "create_entity", side_effect=flaky):
            batch = self.merge(
                {"entity_kind": "station", "source_id": "retry", "record_id": "s1",
                 "name": "Retry", "region": "West"},
            )
        self.assertEqual(batch["summary"]["failed"], 1)
        self.assertEqual(len(self.repo.list_queue(status="pending")), 1)
        # Restart: a new service resumes the queue.
        restarted = DomainService(self.repo, RuleEngine())
        restarted.recover_pending()
        self.assertEqual(self.repo.list_queue(status="pending"), [])
        self.assertIsNotNone(self.repo.get_entity(stable_id("retry", "s1")))

    def test_reconcile_reports_pending_and_stale(self):
        st_id = stable_id("off", "st")
        as_id = stable_id("off", "as")
        self.merge(
            {"entity_kind": "station", "source_id": "off", "record_id": "st", "name": "S", "region": "R"},
            {"entity_kind": "asset", "source_id": "off", "record_id": "as", "station_id": st_id,
             "asset_type": "sensor", "serial_no": "X", "last_seen": "2026-10-01T00:00:00Z"},
        )
        self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel", "asset_id": as_id,
             "metric": "pressure", "value": 1, "observed_at": "2026-10-01T00:00:00Z", "revision": 1},
        )
        self.merge(
            {"entity_kind": "telemetry", "source_id": "off", "record_id": "tel", "asset_id": as_id,
             "metric": "pressure", "value": 1.5, "observed_at": "2026-10-01T00:00:00Z", "revision": 1},
        )
        report = self.service.reconcile()
        self.assertFalse(report["ok"])
        self.assertEqual(len(report["copies_pending"]), 1)
        self.assertEqual(report["copies_pending"][0]["reason"], "two_version")
        # After selecting, reconcile is clean.
        copy = report["copies_pending"][0]
        self.service.apply_pending_copy(self.actor, copy["id"])
        self.assertTrue(self.service.reconcile()["ok"])


if __name__ == "__main__":
    unittest.main()
