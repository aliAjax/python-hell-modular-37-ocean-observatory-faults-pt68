import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService
from src.sync import SyncService


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "sync.db"
        self.repo = SQLiteRepository(self.db_path)
        self.rules = RuleEngine()
        self.svc = DomainService(self.repo, self.rules)
        self.sync = SyncService(self.repo, self.rules)
        self.admin = Actor("admin", "admin")
        self.field = Actor("field-1", "field")
        station = self.svc.create(self.admin, "station", {"name": "OSN-1", "region": "East"})
        self.asset = self.svc.create(self.admin, "asset", {
            "station_id": station["id"], "asset_type": "sensor",
            "serial_no": "S-1", "last_seen": "2026-10-01T00:00:00Z"})
        self.base = {"asset_id": self.asset["id"], "kind": "link_loss",
                     "severity": "medium", "summary": "common base"}

    def tearDown(self):
        self.tmp.cleanup()

    def _divergent_incident(self):
        """中心与站点各自修改同一事件，回网合并后 summary 字段留两版。"""
        inc = self.svc.create(self.admin, "incident", dict(self.base))
        center_data = dict(inc["data"])
        center_data["summary"] = "center: swapped SFP"
        center_data["root_cause"] = "fiber cut"
        self.repo.update_entity(inc["id"], None, "diagnosing", center_data)
        record = {"kind": "incident", "stable_id": "INC-1",
                  "data": {**self.base, "severity": "high",
                           "summary": "station: vessel needed", "field_note": "call boat"},
                  "status": "recovering", "base": dict(self.base)}
        result = self.sync.apply_records(self.field, "STN-1", [record])
        return inc["id"], result["applied"][0]

    def test_offline_records_register_then_merge_before_reconcile(self):
        # 断网先登记：只入持久队列，不产生实体
        records = [{"kind": "incident", "stable_id": "INC-9",
                    "data": {**self.base, "severity": "low", "summary": "queued"},
                    "status": "open"}]
        enq = self.sync.enqueue_records(self.field, "STN-1", records)
        self.assertEqual(enq["queued"], 1)
        self.assertEqual([e["stable_id"] for e in self.sync.list_outbox(self.field, "STN-1")],
                         ["INC-9"])
        # 回网：先合并再对账，一次调用返回对账报告
        report = self.sync.reconnect(self.field, "STN-1")
        self.assertIn("balanced", report)
        self.assertEqual(report["applied"], 1)
        self.assertEqual(self.sync.list_outbox(self.field, "STN-1", "done")[0]["status"], "done")

    def test_field_level_three_way_merge_keeps_two_versions(self):
        inc_id, result = self._divergent_incident()
        self.assertTrue(result["divergent"])
        self.assertEqual(result["divergent_fields"], ["summary"])
        # severity 双方都改但可自动取高；仅站点改的字段被采纳；仅中心改的保留
        inc = self.repo.get_entity(inc_id)
        self.assertEqual(inc["data"]["severity"], "high")
        self.assertEqual(inc["data"]["root_cause"], "fiber cut")
        self.assertEqual(inc["data"]["field_note"], "call boat")
        # 测量/处置两版各存一分支
        versions = {(v["branch"], v["data"]["summary"])
                    for v in self.sync.list_versions("INC-1")}
        self.assertIn(("station", "station: vessel needed"), versions)
        self.assertIn(("center", "center: swapped SFP"), versions)

    def test_incident_cannot_close_before_version_selected(self):
        inc_id, _ = self._divergent_incident()
        self.repo.update_entity(inc_id, None, "recovering",
                                self.repo.get_entity(inc_id)["data"])
        with self.assertRaises(ConflictError):
            self.svc.transition(self.admin, inc_id, "resolve", {"summary": "x"})
        versions = self.sync.list_versions("INC-1")
        station_v = next(v for v in versions if v["branch"] == "station")
        updated = self.sync.select_version(self.admin, "INC-1", station_v["id"])
        self.assertNotIn("divergent", updated["data"])

    def test_new_telemetry_invalidates_resolution_and_reopens(self):
        inc_id, _ = self._divergent_incident()
        versions = self.sync.list_versions("INC-1")
        self.sync.select_version(
            self.admin, "INC-1", next(v for v in versions if v["branch"] == "station")["id"])
        self.svc.transition(self.admin, inc_id, "resolve", {"summary": "restored"})
        self.assertEqual(self.repo.get_entity(inc_id)["status"], "resolved")
        result = self.sync.apply_records(self.admin, "STN-1", [{
            "kind": "telemetry", "stable_id": "TEL-NEW",
            "data": {"asset_id": self.asset["id"], "metric": "p", "value": 99,
                     "observed_at": "2026-10-01T03:00:00Z", "revision": 9},
            "status": "current"}])
        self.assertEqual(result["applied"][0]["reopened_incidents"],
                         [{"incident_id": inc_id, "from": "resolved"}])
        self.assertEqual(self.repo.get_entity(inc_id)["status"], "open")

    def test_concurrent_disposition_first_writer_wins(self):
        inc_id, _ = self._divergent_incident()
        barrier = threading.Barrier(2)
        outcomes = {}

        def submit(uid):
            barrier.wait()
            try:
                act = self.svc.create(Actor(uid, "operator"), "recovery_action", {
                    "incident_id": inc_id, "action_type": "switch_backup",
                    "dedupe_key": "key-" + uid, "disposition_key": "disp-X"})
                outcomes[uid] = ("won", act["id"])
            except ConflictError:
                outcomes[uid] = ("lost",)

        threads = [threading.Thread(target=submit, args=(u,)) for u in ("op-1", "op-2")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual({v[0] for v in outcomes.values()}, {"won", "lost"})
        claim = self.repo.find_disposition_claim(inc_id, "disp-X")
        self.assertIn(claim["actor_id"], ("op-1", "op-2"))

    def test_late_recovery_action_and_gap_go_to_pending_copies(self):
        inc_id, _ = self._divergent_incident()
        self.svc.create(self.admin, "recovery_action", {
            "incident_id": inc_id, "action_type": "remote_restart",
            "dedupe_key": "center-key"})
        action_result = self.sync.apply_records(self.field, "STN-1", [{
            "kind": "recovery_action", "stable_id": "ACT-LATE",
            "data": {"incident_id": inc_id, "action_type": "remote_restart",
                     "dedupe_key": "center-key"}, "status": "proposed"}])
        self.assertEqual(action_result["pending"][0]["stable_id"], "ACT-LATE")

        self.svc.create(self.admin, "gap", {"incident_id": inc_id,
                                            "start_at": "2026-10-01T01:00:00Z",
                                            "end_at": "2026-10-01T02:00:00Z"})
        gap_result = self.sync.apply_records(self.field, "STN-1", [{
            "kind": "gap", "stable_id": "GAP-LATE",
            "data": {"incident_id": inc_id, "start_at": "2026-10-01T01:30:00Z",
                     "end_at": "2026-10-01T02:30:00Z"}, "status": "open"}])
        self.assertEqual(gap_result["pending"][0]["stable_id"], "GAP-LATE")
        self.assertEqual(len(self.sync.list_pending()), 2)
        # 待处理副本可驳回
        copy = self.sync.list_pending()[0]
        decision = self.sync.resolve_pending(self.admin, copy["id"], "reject")
        self.assertEqual(decision["decision"], "rejected")

    def test_stale_telemetry_revision_does_not_overwrite(self):
        self.svc.create(self.admin, "telemetry", {
            "asset_id": self.asset["id"], "metric": "p", "value": 10,
            "observed_at": "2026-10-01T00:00:00Z", "revision": 5})
        result = self.sync.apply_records(self.field, "STN-1", [{
            "kind": "telemetry", "stable_id": "TEL-OLD",
            "data": {"asset_id": self.asset["id"], "metric": "p", "value": 1,
                     "observed_at": "2026-09-30T00:00:00Z", "revision": 2},
            "status": "current"}])
        self.assertEqual(result["skipped"][0]["reason"], "stale_revision")

    def test_failed_write_stays_queued_and_restart_resumes(self):
        self.sync.enqueue_records(self.field, "STN-1", [{
            "kind": "incident", "stable_id": "INC-Q",
            "data": {**self.base, "severity": "low", "summary": "queued"},
            "status": "open"}])
        original = self.repo.create_entity
        state = {"calls": 0}

        def flaky(*args, **kwargs):
            state["calls"] += 1
            if state["calls"] == 1:
                raise sqlite3.OperationalError("simulated disk I/O error")
            return original(*args, **kwargs)

        self.repo.create_entity = flaky
        first = self.sync.drain_outbox(self.field, "STN-1")
        self.repo.create_entity = original
        self.assertEqual(first[0]["outcome"], "retry")
        # 记录仍在队列
        self.assertEqual(self.sync.list_outbox(self.field, "STN-1", "queued")[0]["stable_id"],
                         "INC-Q")
        # 重启：inflight 重置，新仓储实例接着做完
        fresh_repo = SQLiteRepository(self.db_path)
        fresh_sync = SyncService(fresh_repo, self.rules)
        second = fresh_sync.drain_outbox(self.field, "STN-1")
        self.assertEqual(second[0]["outcome"], "created")

    def test_viewer_cannot_enqueue(self):
        with self.assertRaises(PermissionDenied):
            self.sync.enqueue_records(Actor("v", "viewer"), "STN-1", [{
                "kind": "incident", "stable_id": "X",
                "data": {**self.base}, "status": "open"}])


if __name__ == "__main__":
    unittest.main()
