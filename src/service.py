import hashlib
from uuid import uuid4

from .audit import AuditTrail
from .domain import Actor, ConflictError, NotFoundError, ValidationError
from .repository import utcnow
from .rules import RuleEngine


class DomainService:
    # Merge order on reconnect: infrastructure first, then incidents, then
    # telemetry, then recovery actions, finally missions and data gaps.
    MERGE_PRIORITY = {
        "station": 0,
        "asset": 1,
        "link": 2,
        "incident": 3,
        "telemetry": 4,
        "recovery_action": 5,
        "mission": 6,
        "gap": 7,
    }

    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        kind = self.rules.normalize_kind(kind)
        if kind == "pending_copy":
            copies = self.repository.list_pending_copies()
            wrapped = [
                {
                    "id": copy["id"],
                    "kind": "pending_copy",
                    "status": copy["status"],
                    "data": {
                        "reason": copy["reason"],
                        "incident_id": copy["incident_id"],
                        "asset_id": copy["asset_id"],
                        "entity_id": copy["entity_id"],
                        "stable_id": copy["stable_id"],
                        "payload": copy["payload"],
                    },
                }
                for copy in copies
            ]
            if field == "*":
                return wrapped
            return [copy for copy in wrapped if copy["data"].get(field) == value]
        return self.repository.find_entities(kind, field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if kind == "telemetry":
            self._after_upsert(actor, kind, entity)
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        if entity["kind"] == "telemetry" and action == "revise":
            self._after_upsert(actor, "telemetry", updated)
        return updated

    # --- offline merge pipeline ---------------------------------------------

    def merge_offline(self, actor, records):
        """Register offline records and merge them on reconnect.

        Records are first persisted to a retry queue so that a write failure
        never loses un-merged steps; processing then runs in dependency order.
        Each record carries an ``entity_kind`` (station/asset/incident/...),
        a stable ``source_id``/``record_id`` identity, and the entity fields.
        """
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        batch_id = str(uuid4())
        entries = []
        for seq, raw in enumerate(records):
            if not isinstance(raw, dict):
                raise ValidationError("each offline record must be an object")
            kind = raw.get("entity_kind")
            if not kind:
                raise ValidationError("entity_kind is required for each offline record")
            kind = self.rules.normalize_kind(kind)
            source_id = str(raw.get("source_id", "")).strip()
            record_id = str(raw.get("record_id", "")).strip()
            if not source_id or not record_id:
                raise ValidationError("source_id and record_id are required")
            entries.append((seq, kind, raw, actor.user_id, actor.role))
        self.repository.enqueue_offline(batch_id, entries)
        results = self._process_batch(batch_id)
        return {"batch_id": batch_id, "results": results, "summary": self._summarize(results)}

    def _process_batch(self, batch_id):
        entries = self.repository.list_queue(batch_id=batch_id, status="pending")
        entries.sort(key=lambda entry: (self.MERGE_PRIORITY.get(entry["kind"], 99), entry["seq"]))
        return [self._process_queue_entry(entry) for entry in entries]

    def _process_queue_entry(self, entry):
        actor = Actor(entry["actor_id"], entry["actor_role"])
        record = entry["payload"]
        try:
            kind = self.rules.normalize_kind(record["entity_kind"])
            source_id = str(record["source_id"])
            record_id = str(record["record_id"])
            stable_id = "off-" + hashlib.sha256(
                (source_id + "\0" + record_id).encode("utf-8")
            ).hexdigest()[:32]
            data = {
                key: value
                for key, value in record.items()
                if key not in ("entity_kind", "source_id", "record_id")
            }
            result = self._process_record(actor, kind, stable_id, data)
            self.repository.mark_queue_done(entry["id"])
            return {"queue_id": entry["id"], "status": "done", "result": result}
        except Exception as exc:
            self.repository.mark_queue_failed(entry["id"], str(exc))
            return {"queue_id": entry["id"], "status": "pending", "error": str(exc)}

    def _process_record(self, actor, kind, stable_id, data):
        existing = self.repository.get_entity(stable_id)
        if existing:
            return self._merge_existing(actor, kind, stable_id, existing, data)
        duplicate = self._duplicate_reason(kind, data)
        if duplicate:
            # First writer wins: the center already has this action or gap.
            return self._pending_copy(
                kind, stable_id, None, data, duplicate, actor,
                incident_id=data.get("incident_id"), asset_id=data.get("asset_id"),
            )
        try:
            self.rules.validate_create(actor, kind, data, self._lookup)
        except ConflictError as exc:
            reason = self._conflict_reason(kind, exc)
            return self._pending_copy(
                kind, stable_id, None, data, reason, actor,
                incident_id=data.get("incident_id"), asset_id=data.get("asset_id"), error=str(exc),
            )
        create_data = dict(data)
        create_data["_base"] = dict(data)
        try:
            entity = self.repository.create_entity(
                stable_id, kind, self.rules.initial_status(kind, create_data), create_data, actor.user_id
            )
        except ConflictError as exc:
            # First writer wins: a concurrent insert already took the stable id.
            return self._pending_copy(
                kind, stable_id, None, data, "concurrent", actor,
                incident_id=data.get("incident_id"), asset_id=data.get("asset_id"), error=str(exc),
            )
        self.audit.record(stable_id, actor, "merge_create", None, entity["status"], {"kind": kind})
        self._after_upsert(actor, kind, entity)
        return {"status": "merged", "entity": entity}

    def _merge_existing(self, actor, kind, stable_id, existing, data):
        if kind in ("telemetry", "recovery_action"):
            # Measurement and handling keep two versions until one is selected.
            return self._pending_copy(
                kind, stable_id, existing["id"], data, "two_version", actor,
                incident_id=data.get("incident_id"), asset_id=data.get("asset_id"),
            )
        if kind == "gap":
            # Center already has a data gap for this incident: keep the late copy.
            return self._pending_copy(
                kind, stable_id, existing["id"], data, "duplicate", actor,
                incident_id=data.get("incident_id"),
            )
        return self._field_merge(actor, kind, stable_id, existing, data)

    def _field_merge(self, actor, kind, stable_id, existing, data):
        """Three-way merge per field using the offline base snapshot."""
        current = existing["data"]
        base = current.get("_base")
        merged = dict(current)
        conflicts = []
        changed = []
        for key, incoming in data.items():
            if key.startswith("_"):
                continue
            if key not in current:
                merged[key] = incoming
                changed.append(key)
                continue
            if current[key] == incoming:
                continue
            if base is not None and key in base:
                if current[key] == base[key]:
                    merged[key] = incoming
                    changed.append(key)
                elif incoming == base[key]:
                    # Only the center side changed; keep it.
                    pass
                else:
                    conflicts.append(key)
            else:
                merged[key] = incoming
                changed.append(key)
        if conflicts:
            return self._pending_copy(
                kind, stable_id, existing["id"], data, "field_conflict", actor,
                incident_id=data.get("incident_id"), asset_id=data.get("asset_id"),
                conflict_fields=conflicts,
            )
        if not changed:
            return {"status": "unchanged", "entity": existing}
        updated = self.repository.update_entity(
            existing["id"], existing["version"], existing["status"], merged
        )
        self.audit.record(
            existing["id"], actor, "merge_field", existing["status"], existing["status"],
            {"changed": changed},
        )
        self._after_upsert(actor, kind, updated)
        return {"status": "merged", "entity": updated}

    def _conflict_reason(self, kind, exc):
        message = str(exc)
        if kind == "telemetry" and "revision" in message:
            return "late_revision"
        if kind == "recovery_action" and "dedupe" in message:
            return "duplicate"
        if kind == "gap":
            return "duplicate"
        return "concurrent"

    def _duplicate_reason(self, kind, data):
        """A center-side action or gap already covers the same ground."""
        if kind == "recovery_action":
            key = data.get("dedupe_key")
            for action in self.repository.list_entities("recovery_action"):
                if action["data"].get("dedupe_key") == key and action["status"] not in ("succeeded", "failed", "cancelled"):
                    return "duplicate"
        if kind == "gap":
            incident_id = data.get("incident_id")
            for gap in self.repository.list_entities("gap"):
                if gap["data"].get("incident_id") == incident_id and gap["status"] not in ("filled", "accepted", "closed"):
                    return "duplicate"
        return None

    def _pending_copy(self, kind, stable_id, entity_id, payload, reason, actor,
                      incident_id=None, asset_id=None, conflict_fields=None, error=None):
        copy_id = "copy-" + uuid4().hex
        data = dict(payload)
        if conflict_fields:
            data["_conflict_fields"] = list(conflict_fields)
        if error:
            data["_error"] = str(error)
        copy = self.repository.create_pending_copy(
            copy_id, kind, stable_id, entity_id, data, reason, actor.user_id,
            incident_id=incident_id, asset_id=asset_id,
        )
        return {"status": "pending", "copy": copy}

    def _after_upsert(self, actor, kind, entity):
        if kind == "telemetry":
            self._invalidate_closing_basis(actor, entity["data"].get("asset_id"))

    def _invalidate_closing_basis(self, actor, asset_id):
        """New telemetry invalidates the closing basis of resolved incidents."""
        if not asset_id:
            return
        for incident in self.repository.find_entities("incident", "asset_id", asset_id):
            basis = incident["data"].get("closing_basis")
            if not basis or basis.get("stale"):
                continue
            stale = False
            for telemetry in self.repository.find_entities("telemetry", "asset_id", asset_id):
                metric = telemetry["data"].get("metric")
                revision = int(telemetry["data"].get("revision", 0))
                if revision > int(basis.get("telemetry", {}).get(metric, 0)):
                    stale = True
                    break
            if not stale:
                continue
            data = dict(incident["data"])
            data["closing_basis"] = dict(basis, stale=True, stale_since=utcnow())
            if incident["status"] == "resolved":
                self.repository.update_entity(incident["id"], incident["version"], "open", data)
                self.audit.record(
                    incident["id"], actor, "basis_invalidated", "resolved", "open",
                    {"reason": "new telemetry"},
                )
            else:
                self.repository.update_entity(incident["id"], incident["version"], incident["status"], data)
                self.audit.record(
                    incident["id"], actor, "basis_invalidated", incident["status"], incident["status"],
                    {"reason": "new telemetry"},
                )

    # --- pending copies: selection and discard ------------------------------

    def apply_pending_copy(self, actor, copy_id):
        copy = self.repository.get_pending_copy(copy_id)
        if not copy:
            raise NotFoundError("pending copy not found: " + copy_id)
        if copy["status"] != "pending":
            raise ValidationError("copy already resolved: " + copy["status"])
        payload = {key: value for key, value in copy["payload"].items() if not key.startswith("_")}
        kind = copy["kind"]
        if copy["reason"] == "two_version":
            entity_id = copy["entity_id"] or copy["stable_id"]
            entity = self.repository.get_entity(entity_id)
            if not entity:
                entity = self.repository.create_entity(
                    entity_id, kind, self.rules.initial_status(kind, payload), payload, actor.user_id
                )
            else:
                merged = dict(entity["data"])
                merged.update(payload)
                entity = self.repository.update_entity(entity_id, entity["version"], entity["status"], merged)
            self.audit.record(
                entity_id, actor, "version_selected", entity["status"], entity["status"],
                {"copy_id": copy_id, "reason": copy["reason"]},
            )
            self._after_upsert(actor, kind, entity)
        else:
            new_id = str(uuid4())
            entity = self.repository.create_entity(
                new_id, kind, self.rules.initial_status(kind, payload), payload, actor.user_id
            )
            self.audit.record(
                new_id, actor, "copy_applied", None, entity["status"],
                {"copy_id": copy_id, "reason": copy["reason"]},
            )
        self.repository.resolve_pending_copy(copy_id, "applied", "applied", actor.user_id)
        return entity

    def discard_pending_copy(self, actor, copy_id):
        copy = self.repository.get_pending_copy(copy_id)
        if not copy:
            raise NotFoundError("pending copy not found: " + copy_id)
        if copy["status"] != "pending":
            raise ValidationError("copy already resolved: " + copy["status"])
        return self.repository.resolve_pending_copy(copy_id, "discarded", "discarded", actor.user_id)

    # --- retry and reconciliation --------------------------------------------

    def recover_pending(self, actor=None):
        """Resume queued steps after a restart."""
        actor = actor or Actor("system", "admin")
        entries = self.repository.next_pending_queue(limit=500)
        return [self._process_queue_entry(entry) for entry in entries]

    def reconcile(self, actor=None):
        queue_pending = self.repository.list_queue(status="pending")
        copies_pending = self.repository.list_pending_copies(status="pending")
        stale_bases = [
            incident
            for incident in self.repository.list_entities("incident")
            if incident["data"].get("closing_basis")
            and incident["data"]["closing_basis"].get("stale")
        ]
        return {
            "ok": not queue_pending and not copies_pending and not stale_bases,
            "queue_pending": queue_pending,
            "copies_pending": copies_pending,
            "stale_bases": stale_bases,
        }

    def _summarize(self, results):
        merged = sum(
            1 for result in results
            if result["status"] == "done" and result["result"]["status"] in ("merged", "unchanged")
        )
        pending = sum(
            1 for result in results
            if result["status"] == "done" and result["result"]["status"] == "pending"
        )
        failed = sum(1 for result in results if result["status"] == "pending")
        return {"merged": merged, "pending": pending, "failed": failed, "total": len(results)}

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
