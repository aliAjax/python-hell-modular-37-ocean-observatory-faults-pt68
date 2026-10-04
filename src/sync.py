"""近海站网断网—回网同步服务。

顺序固定：断网先登记（sync_outbox 持久队列）；回网后先合并故障事件、
遥测和恢复动作；合并排空后再与中心对账（reconciliation_runs）。

冲突策略：
- 按稳定编号（stable_id）逐字段三向合并（见 src/merge.py）；
- 遥测测量值与处置动作的两版分别存入 entity_versions，选定前事件不可关闭；
- 中心已有恢复动作或数据缺口时，后到提交转 pending_copies 留待处理；
- 新遥测一到，基于旧状态的关闭/解决依据失效，事件自动重开；
- 同一事件同一处置的并发提交由 disposition_claims 唯一约束仲裁，先入库者胜。
"""

from uuid import uuid4

from .domain import Actor, ConflictError, PermissionDenied, ValidationError
from .merge import merge_status, three_way_merge
from .rules import RuleEngine

# 合并阶段允许的角色：现场、操作员与工程师可回传，管理员可对账
SYNC_ROLES = ("admin", "operator", "engineer", "field")
SYNC_KINDS = ("incident", "telemetry", "recovery_action", "gap")

CREATE_REQUIRED = {
    "incident": ("kind", "severity", "summary"),
    "telemetry": ("asset_id", "metric", "value", "observed_at", "revision"),
    "recovery_action": ("incident_id", "action_type", "dedupe_key"),
    "gap": ("incident_id", "start_at", "end_at"),
}
REFERENCE_FIELDS = ("station_id", "asset_id", "link_id", "incident_id")
TERMINAL_STATUSES = {
    "recovery_action": ("succeeded", "failed", "cancelled"),
    "gap": ("filled", "accepted", "closed"),
}
# 确定性错误：重试不会改变结果，隔离出队列以免阻塞后续步骤
DETERMINISTIC_ERRORS = (ValidationError, PermissionDenied)


class SyncService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.system_actor = Actor("sync", "operator")

    # ----- 工具 -----
    def _require_sync_role(self, actor):
        if actor.role not in SYNC_ROLES:
            raise PermissionDenied("role %s is not allowed to sync" % actor.role)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def _resolve_ref(self, value):
        """站点侧稳定编号引用解析为中心实体编号。"""
        if value in (None, ""):
            return value
        stable = self.repository.get_stable(str(value))
        return stable["entity_id"] if stable else value

    def _resolve_data_refs(self, data):
        resolved = dict(data)
        for field in REFERENCE_FIELDS:
            if field in resolved:
                resolved[field] = self._resolve_ref(resolved.get(field))
        return resolved

    def _record_actor(self, actor, record):
        actor_id = record.get("actor_id")
        return Actor(actor_id, actor.role) if actor_id else actor

    # ----- 断网先登记：仅持久化，不做合并 -----
    def enqueue_records(self, actor, station_id, records):
        self._require_sync_role(actor)
        station_id = str(station_id or "").strip()
        if not station_id:
            raise ValidationError("station_id is required")
        if not isinstance(records, list) or not records:
            raise ValidationError("records must be a non-empty list")
        queued = []
        for record in records:
            self._validate_envelope(record)
            queued.append(
                self.repository.enqueue_record(
                    station_id, record["kind"], record["stable_id"], record
                )
            )
        return {"station_id": station_id, "queued_ids": queued, "queued": len(queued)}

    def _validate_envelope(self, record):
        if not isinstance(record, dict):
            raise ValidationError("each record must be an object")
        kind = record.get("kind")
        stable_id = str(record.get("stable_id", "")).strip()
        data = record.get("data")
        if kind not in SYNC_KINDS:
            raise ValidationError("unsupported sync kind: " + str(kind))
        if not stable_id:
            raise ValidationError("stable_id is required")
        if not isinstance(data, dict):
            raise ValidationError("record data must be an object")
        for field in CREATE_REQUIRED[kind]:
            if data.get(field) in (None, "", [], {}):
                raise ValidationError("missing required field: " + field)

    # ----- 回网：先合并队列，再对账 -----
    def reconnect(self, actor, station_id):
        self._require_sync_role(actor)
        station_id = str(station_id or "").strip()
        if not station_id:
            raise ValidationError("station_id is required")
        # 重启场景：上次卡在 inflight 的步骤退回队列接着做
        self.repository.reset_inflight()
        applied = []
        pending = []
        skipped = []
        failed = []
        while True:
            item = self.repository.claim_next_queued(station_id)
            if not item:
                break
            try:
                result = self._apply_one(actor, station_id, item["payload"])
            except DETERMINISTIC_ERRORS as exc:
                self.repository.mark_outbox_dead(item["id"], exc)
                failed.append({"outbox_id": item["id"], "stable_id": item["stable_id"],
                               "kind": item["kind"], "error": str(exc), "retry": False})
                continue
            except Exception as exc:
                # 写入失败：未入库步骤留在队列重试
                self.repository.mark_outbox_failed(item["id"], exc)
                failed.append({"outbox_id": item["id"], "stable_id": item["stable_id"],
                               "kind": item["kind"], "error": str(exc), "retry": True})
                break
            self.repository.mark_outbox_done(item["id"])
            bucket = {"created": applied, "merged": applied,
                      "pending": pending, "stale": skipped, "skipped": skipped}
            bucket.get(result["outcome"], applied).append(result)
        report = self.reconcile(actor, station_id, save=True,
                                applied=applied, pending=pending, skipped=skipped, failed=failed)
        return report

    def drain_outbox(self, actor, station_id):
        """只排空队列不对账，供分步调用与测试使用。"""
        self._require_sync_role(actor)
        self.repository.reset_inflight()
        results = []
        while True:
            item = self.repository.claim_next_queued(station_id)
            if not item:
                break
            try:
                result = self._apply_one(actor, station_id, item["payload"])
            except DETERMINISTIC_ERRORS as exc:
                self.repository.mark_outbox_dead(item["id"], exc)
                results.append({"outcome": "dead", "outbox_id": item["id"], "error": str(exc)})
                continue
            except Exception as exc:
                self.repository.mark_outbox_failed(item["id"], exc)
                results.append({"outcome": "retry", "outbox_id": item["id"], "error": str(exc)})
                break
            self.repository.mark_outbox_done(item["id"])
            result["outbox_id"] = item["id"]
            results.append(result)
        return results

    # ----- 直接提交一个批次（在线或回传文件）：合并但不对账 -----
    def apply_records(self, actor, station_id, records):
        self._require_sync_role(actor)
        station_id = str(station_id or "").strip()
        if not station_id:
            raise ValidationError("station_id is required")
        if not isinstance(records, list) or not records:
            raise ValidationError("records must be a non-empty list")
        applied, pending, skipped = [], [], []
        for record in records:
            self._validate_envelope(record)
            result = self._apply_one(actor, station_id, record)
            if result["outcome"] == "pending":
                pending.append(result)
            elif result["outcome"] in ("stale", "skipped"):
                skipped.append(result)
            else:
                applied.append(result)
        return {
            "station_id": station_id,
            "applied": applied,
            "pending": pending,
            "skipped": skipped,
        }

    # ----- 单条记录合并 -----
    def _apply_one(self, actor, station_id, record):
        kind = record["kind"]
        stable_id = record["stable_id"]
        station_data = self._resolve_data_refs(record["data"])
        station_status = record.get("status")
        explicit_base = isinstance(record.get("base"), dict)
        station_base = self._resolve_data_refs(
            dict(record["base"]) if explicit_base else dict(record.get("data") or {}))
        record_actor = self._record_actor(actor, record)

        existing = self._find_center_twin(kind, stable_id, station_data)
        if existing == "pending":
            copy = self._to_pending(station_id, record, "conflicting center record")
            return {"outcome": "pending", "stable_id": stable_id, "kind": kind,
                    "reason": copy["reason"], "pending_copy": copy}
        if existing is None:
            return self._create_from_record(
                actor, record_actor, station_id, kind, stable_id,
                station_data, station_status, record
            )

        # 遥测旧修订号：后到但更旧，不覆盖现行值
        if kind == "telemetry":
            local_revision = int(existing["data"].get("revision", 0))
            incoming_revision = int(station_data.get("revision", 0))
            if incoming_revision < local_revision:
                self._to_pending(station_id, record,
                                 "stale revision %s < %s" % (incoming_revision, local_revision))
                return {"outcome": "stale", "stable_id": stable_id, "kind": kind,
                        "entity_id": existing["id"], "reason": "stale_revision"}

        stable = self.repository.get_stable(stable_id)
        if stable is None:
            # 中心先独立创建、站点后到：有断网前共同快照则用之，
            # 否则以中心现状为基线（站点字段视为单方面推进）
            if explicit_base:
                adopt_base = station_base
            else:
                adopt_base = dict(existing["data"])
            self.repository.upsert_stable(
                stable_id, existing["id"], kind, "center",
                adopt_base, existing["status"], "adopted")
            base_data = dict(adopt_base)
            base_status = existing["status"]
        else:
            base_data = dict(stable["base_data"])
            base_status = stable["base_status"]

        merged_data, conflicts, auto_take = three_way_merge(
            base_data, existing["data"], station_data)
        target_status = station_status or existing["status"]
        next_status, status_conflict = merge_status(
            kind, base_status, existing["status"], target_status, self.rules)

        divergence = dict(conflicts)
        if status_conflict:
            divergence["status"] = {"center": existing["status"], "station": target_status}
        self._snapshot_versions(stable_id, kind, existing, station_data, station_status, divergence)

        # 以本次合并结论为准：清除上一轮遗留的分歧标记
        merged_data.pop("divergent", None)
        merged_data.pop("divergent_fields", None)
        if divergence:
            merged_data["divergent"] = True
            merged_data["divergent_fields"] = sorted(divergence.keys())
        merged_data["merged"] = True

        updated = self.repository.update_entity(existing["id"], None, next_status, merged_data)
        self.repository.append_audit(
            existing["id"], record_actor.user_id, record_actor.role,
            "sync_merge", existing["status"], next_status,
            {"station_id": station_id, "stable_id": stable_id,
             "conflicts": divergence, "auto_take": auto_take},
        )
        self.repository.upsert_stable(
            stable_id, existing["id"], kind,
            (stable["origin"] if stable else "center"),
            merged_data, next_status, "divergent" if divergence else "merged")

        # 新遥测一到，旧的关闭依据失效并重算
        telemetry_bumped = kind == "telemetry" and int(station_data.get("revision", 0)) > int(
            base_data.get("revision", 0))
        reopened = self._invalidate_closures(
            updated, previous_revision=base_data.get("revision", 0)) \
            if telemetry_bumped else []

        return {
            "outcome": "merged",
            "stable_id": stable_id,
            "kind": kind,
            "entity_id": existing["id"],
            "version": updated["version"],
            "divergent": bool(divergence),
            "divergent_fields": sorted(divergence.keys()),
            "auto_take": auto_take,
            "reopened_incidents": reopened,
        }

    def _find_center_twin(self, kind, stable_id, data):
        """按稳定编号或业务唯一特征寻找中心现有对象。

        返回 None 表示无对应对象；"pending" 表示中心已有互斥对象，
        后到的提交应留待处理副本。
        """
        stable = self.repository.get_stable(stable_id)
        if stable:
            return self.repository.get_entity(stable["entity_id"])
        if kind == "incident":
            for incident in self._lookup("incident", "*", None) or []:
                if incident["status"] in ("resolved", "closed"):
                    continue
                if incident["data"].get("asset_id") == data.get("asset_id") and \
                        incident["data"].get("kind") == data.get("kind"):
                    # 同一资产同一故障类型的活动事件：中心版与站点版逐字段合并
                    return incident
            return None
        if kind == "recovery_action":
            for action in self._lookup("recovery_action", "*", None) or []:
                if action["data"].get("dedupe_key") == data.get("dedupe_key") and \
                        action["status"] not in TERMINAL_STATUSES["recovery_action"]:
                    return "pending"
            return None
        if kind == "gap":
            for gap in self._lookup("gap", "*", None) or []:
                if gap["data"].get("incident_id") != data.get("incident_id"):
                    continue
                if gap["status"] in TERMINAL_STATUSES["gap"]:
                    continue
                if self._windows_overlap(
                        gap["data"].get("start_at"), gap["data"].get("end_at"),
                        data.get("start_at"), data.get("end_at")):
                    return "pending"
            return None
        if kind == "telemetry":
            twins = [
                t for t in self._lookup("telemetry", "*", None) or []
                if t["data"].get("asset_id") == data.get("asset_id")
                and t["data"].get("metric") == data.get("metric")
            ]
            return max(twins, key=lambda t: int(t["data"].get("revision", 0))) if twins else None
        return None

    @staticmethod
    def _windows_overlap(start_a, end_a, start_b, end_b):
        if not all((start_a, end_a, start_b, end_b)):
            return False
        return str(start_a) <= str(end_b) and str(start_b) <= str(end_a)

    def _create_from_record(self, actor, record_actor, station_id, kind,
                            stable_id, data, status, record):
        data = dict(data)
        if kind == "incident":
            self._check_references(data, ("station_id", "asset_id", "link_id"))
        elif kind == "telemetry":
            self._check_references(data, ("asset_id",))
            if any(int(t["data"].get("revision", 0)) >= int(data["revision"])
                   for t in self._lookup("telemetry", "asset_id", data["asset_id"]) or []
                   if t["data"].get("metric") == data["metric"]):
                self._to_pending(station_id, record, "telemetry revision already present")
                return {"outcome": "stale", "stable_id": stable_id, "kind": kind,
                        "reason": "stale_revision"}
        elif kind == "recovery_action":
            incident = self.repository.get_entity(data.get("incident_id"))
            if not incident or incident["status"] in ("resolved", "closed"):
                raise ValidationError("recovery action requires an active incident")
            # 同一事件同一处置：先入库的一方生效，后到者留待处理副本
            disposition_key = data.get("disposition_key") or data.get("action_type")
            entity_id = str(uuid4())
            won = self.repository.claim_disposition(
                data["incident_id"], disposition_key, entity_id, record_actor.user_id)
            if not won:
                self._to_pending(station_id, record,
                                 "disposition %s already claimed" % disposition_key)
                return {"outcome": "pending", "stable_id": stable_id, "kind": kind,
                        "reason": "disposition_already_claimed"}
            data["disposition_key"] = disposition_key
        elif kind == "gap":
            incident = self.repository.get_entity(data.get("incident_id"))
            if not incident:
                raise ValidationError("data gap requires incident")

        initial = self.rules.initial_status(kind)
        if status and self.rules.can_reach(kind, initial, status):
            initial = status
        entity_id = entity_id if kind == "recovery_action" else str(uuid4())
        entity = self.repository.create_entity(
            entity_id, kind, initial, data, record_actor.user_id)
        self.repository.append_audit(
            entity_id, record_actor.user_id, record_actor.role,
            "sync_create", None, initial,
            {"station_id": station_id, "stable_id": stable_id})
        # 中心首入库即该稳定编号的现行版本，共同基线就是落库内容
        self.repository.upsert_stable(
            stable_id, entity_id, kind, "station",
            dict(entity["data"]), initial, "created")
        self.repository.add_entity_version(
            stable_id, "station", kind, entity["version"], initial, dict(data))

        # 新遥测一到，旧关闭依据失效：新建遥测无历史基线，无条件重算
        reopened = []
        if kind == "telemetry":
            reopened = self._invalidate_closures(entity, previous_revision=None)
        return {"outcome": "created", "stable_id": stable_id, "kind": kind,
                "entity_id": entity_id, "version": entity["version"],
                "reopened_incidents": reopened}

    def _check_references(self, data, fields):
        for field in fields:
            value = data.get(field)
            if value and not self.repository.get_entity(value):
                raise ValidationError("unresolved %s: %s" % (field, value))

    def _snapshot_versions(self, stable_id, kind, existing, station_data,
                           station_status, divergence):
        self.repository.add_entity_version(
            stable_id, "center", kind, existing["version"],
            existing["status"], dict(existing["data"]))
        if divergence:
            # 冲突字段才保留站点分支，供逐字段选定
            self.repository.add_entity_version(
                stable_id, "station", kind, existing["version"] + 1,
                station_status, dict(station_data))

    def _to_pending(self, station_id, record, reason):
        return self.repository.add_pending_copy(
            station_id, record["stable_id"], record["kind"], record, reason)

    # ----- 新遥测到达：旧关闭依据失效，事件重开并重算 -----
    def _invalidate_closures(self, telemetry, previous_revision="unset"):
        """新遥测到达使解决/关闭依据失效。

        previous_revision 为 None 表示全新测量序列（首次入库），无条件重算；
        合并路径传入共同基线中的修订号，只有修订号真正变大才使依据失效。
        """
        asset_id = telemetry["data"].get("asset_id")
        new_revision = int(telemetry["data"].get("revision", 0))
        bumped = previous_revision is None or new_revision > int(previous_revision or 0)
        if not bumped:
            return []
        reopened = []
        for incident in self._lookup("incident", "*", None) or []:
            if incident["data"].get("asset_id") != asset_id:
                continue
            if incident["status"] not in ("resolved", "closed"):
                continue
            previous_status = incident["status"]
            data = dict(incident["data"])
            data["reopened_reason"] = "new_telemetry_invalidated_closure"
            data["reopened_by_telemetry"] = telemetry["id"]
            updated = self.repository.update_entity(incident["id"], None, "open", data)
            self.repository.append_audit(
                incident["id"], self.system_actor.user_id, self.system_actor.role,
                "auto_reopen", previous_status, "open",
                {"telemetry_id": telemetry["id"],
                 "revision": telemetry["data"].get("revision")})
            reopened.append({"incident_id": incident["id"], "from": previous_status})
        return reopened

    # ----- 版本选定：测量和处置两版择一后才能关闭事件 -----
    def list_versions(self, stable_id):
        if not self.repository.get_stable(stable_id):
            raise ConflictError("unknown stable_id: " + str(stable_id))
        return self.repository.list_entity_versions(stable_id)

    def select_version(self, actor, stable_id, version_id):
        self._require_sync_role(actor)
        stable = self.repository.get_stable(stable_id)
        if not stable:
            raise ConflictError("unknown stable_id: " + str(stable_id))
        versions = self.repository.list_entity_versions(stable_id)
        chosen = next((v for v in versions if v["id"] == int(version_id)), None)
        if not chosen:
            raise ValidationError("version %s not found for stable_id" % version_id)
        entity = self.repository.get_entity(stable["entity_id"])
        if not entity:
            raise ConflictError("center entity missing for stable_id")

        if chosen["branch"] == "station":
            data = dict(chosen["data"])
        else:
            data = dict(entity["data"])
            # 选定中心版：用保存的中心快照覆盖冲突字段
            for field in entity["data"].get("divergent_fields", []):
                if field == "status":
                    continue
                if field in chosen["data"]:
                    data[field] = chosen["data"][field]
        status = entity["status"]
        if "status" in entity["data"].get("divergent_fields", []):
            candidate = chosen.get("status")
            if candidate and self.rules.can_reach(
                    stable["kind"], status, candidate) and chosen["branch"] == "station":
                status = candidate
        data.pop("divergent", None)
        data.pop("divergent_fields", None)
        updated = self.repository.update_entity(entity["id"], None, status, data)
        self.repository.mark_version_selected(chosen["id"])
        self.repository.upsert_stable(
            stable_id, entity["id"], stable["kind"], stable["origin"],
            data, status, "selected")
        self.repository.append_audit(
            entity["id"], actor.user_id, actor.role, "select_version",
            entity["status"], status,
            {"stable_id": stable_id, "version_id": chosen["id"], "branch": chosen["branch"]})
        return updated

    # ----- 待处理副本：人工处置后到提交 -----
    def list_pending(self, status="pending"):
        return self.repository.list_pending_copies(status)

    def resolve_pending(self, actor, copy_id, decision):
        self._require_sync_role(actor)
        copy = self.repository.get_pending_copy(copy_id)
        if not copy:
            raise ConflictError("pending copy not found: " + str(copy_id))
        if copy["status"] != "pending":
            raise ConflictError("pending copy already %s" % copy["status"])
        if decision == "reject":
            self.repository.resolve_pending_copy(copy_id, "rejected")
            return {"id": copy_id, "decision": "rejected"}
        if decision != "apply":
            raise ValidationError("decision must be apply or reject")
        record = copy["payload"]
        result = self._apply_one(actor, copy["station_id"], record)
        if result["outcome"] == "pending":
            raise ConflictError("record still conflicts: " + str(result.get("reason")))
        self.repository.resolve_pending_copy(copy_id, "applied")
        result["id"] = copy_id
        result["decision"] = "applied"
        return result

    # ----- 对账：合并排空后比较中心与站点 -----
    def reconcile(self, actor=None, station_id=None, save=False,
                  applied=None, pending=None, skipped=None, failed=None):
        if actor is not None:
            self._require_sync_role(actor)
        applied, pending = applied or [], pending or []
        skipped, failed = skipped or [], failed or []

        divergence_rows = []
        for entity in self.repository.list_entities():
            if entity["data"].get("divergent"):
                stable_match = self._stable_for_entity(entity["id"])
                divergence_rows.append({
                    "entity_id": entity["id"],
                    "kind": entity["kind"],
                    "stable_id": stable_match,
                    "fields": entity["data"].get("divergent_fields", []),
                })

        queue = self.repository.list_outbox()
        queue_counts = {"queued": 0, "inflight": 0, "done": 0, "dead": 0}
        for item in queue:
            if station_id and item["station_id"] != station_id:
                continue
            queue_counts[item["status"]] = queue_counts.get(item["status"], 0) + 1

        pending_copies = self.repository.list_pending_copies("pending")
        if station_id:
            pending_copies = [c for c in pending_copies if c["station_id"] == station_id]

        matched = set()
        for result in applied:
            if result.get("entity_id"):
                matched.add(result["entity_id"])
        missing_on_center = []
        if station_id:
            for item in queue:
                if item["station_id"] != station_id or item["status"] != "done":
                    continue
                stable = self.repository.get_stable(item["stable_id"])
                if stable and not self.repository.get_entity(stable["entity_id"]):
                    missing_on_center.append(item["stable_id"])

        report = {
            "station_id": station_id,
            "applied": len(applied),
            "pending_copies": len(pending_copies),
            "skipped": len(skipped),
            "failed": failed,
            "unresolved_divergences": divergence_rows,
            "queue": queue_counts,
            "missing_on_center": missing_on_center,
            "balanced": (
                not divergence_rows
                and not pending_copies
                and queue_counts.get("queued", 0) == 0
                and queue_counts.get("inflight", 0) == 0
                and queue_counts.get("dead", 0) == 0
                and not missing_on_center
            ),
        }
        if save and station_id:
            report["run_id"] = self.repository.save_reconciliation(station_id, report)
        return report

    def _stable_for_entity(self, entity_id):
        for stable in self.repository.list_stables():
            if stable["entity_id"] == entity_id:
                return stable["stable_id"]
        return None

    def list_reconciliations(self, actor, station_id=None):
        self._require_sync_role(actor)
        return self.repository.list_reconciliations(station_id)

    def list_outbox(self, actor, station_id=None, status=None):
        self._require_sync_role(actor)
        items = self.repository.list_outbox(status)
        if station_id:
            items = [item for item in items if item["station_id"] == station_id]
        return items
