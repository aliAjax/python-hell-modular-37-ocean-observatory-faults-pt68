import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS stable_index (
                    stable_id TEXT PRIMARY KEY,
                    entity_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    base_data TEXT NOT NULL,
                    base_status TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS entity_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stable_id TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    entity_version INTEGER NOT NULL,
                    status TEXT,
                    data TEXT NOT NULL,
                    selected INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(stable_id, branch)
                );
                CREATE TABLE IF NOT EXISTS pending_copies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT NOT NULL,
                    stable_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    handled_at TEXT
                );
                CREATE TABLE IF NOT EXISTS disposition_claims (
                    incident_id TEXT NOT NULL,
                    disposition_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(incident_id, disposition_key)
                );
                CREATE TABLE IF NOT EXISTS sync_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    stable_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_status
                    ON sync_outbox(status, id);
                CREATE TABLE IF NOT EXISTS reconciliation_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT NOT NULL,
                    report TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)
            # 重启后未完成的入队步骤回到 queued，等待接着重试
            connection.execute(
                "UPDATE sync_outbox SET status = 'queued' WHERE status = 'inflight'"
            )

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        entities = self.list_entities(kind=kind)
        if field == "*":
            return entities
        return [
            entity
            for entity in entities
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

    # ----- 稳定编号 / 版本分支 -----
    def get_stable(self, stable_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM stable_index WHERE stable_id = ?", (stable_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "stable_id": row["stable_id"],
            "entity_id": row["entity_id"],
            "kind": row["kind"],
            "origin": row["origin"],
            "base_data": json.loads(row["base_data"]),
            "base_status": row["base_status"],
            "state": row["state"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_stables(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT stable_id, entity_id, kind, state, updated_at FROM stable_index ORDER BY stable_id"
            ).fetchall()
        return [
            {
                "stable_id": row["stable_id"],
                "entity_id": row["entity_id"],
                "kind": row["kind"],
                "state": row["state"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def upsert_stable(self, stable_id, entity_id, kind, origin, base_data, base_status, state):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO stable_index(stable_id, entity_id, kind, origin, base_data, base_status, state, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(stable_id) DO UPDATE SET entity_id = excluded.entity_id, "
                "base_data = excluded.base_data, base_status = excluded.base_status, "
                "state = excluded.state, updated_at = excluded.updated_at",
                (
                    stable_id,
                    entity_id,
                    kind,
                    origin,
                    json.dumps(base_data, ensure_ascii=False, sort_keys=True),
                    base_status,
                    state,
                    now,
                    now,
                ),
            )

    def add_entity_version(self, stable_id, branch, kind, entity_version, status, data):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entity_versions(stable_id, branch, kind, entity_version, status, data, selected, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 0, ?) "
                "ON CONFLICT(stable_id, branch) DO UPDATE SET entity_version = excluded.entity_version, "
                "status = excluded.status, data = excluded.data",
                (
                    stable_id,
                    branch,
                    kind,
                    entity_version,
                    status,
                    json.dumps(data, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )

    def list_entity_versions(self, stable_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entity_versions WHERE stable_id = ? ORDER BY id", (stable_id,)
            ).fetchall()
        return [
            {
                "id": row["id"],
                "stable_id": row["stable_id"],
                "branch": row["branch"],
                "kind": row["kind"],
                "entity_version": row["entity_version"],
                "status": row["status"],
                "data": json.loads(row["data"]),
                "selected": bool(row["selected"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def mark_version_selected(self, version_id):
        with self._connect() as connection:
            cur = connection.execute(
                "UPDATE entity_versions SET selected = 1 WHERE id = ?", (version_id,)
            )
            if cur.rowcount == 0:
                raise NotFoundError("entity version not found: " + str(version_id))

    # ----- 待处理副本 -----
    def add_pending_copy(self, station_id, stable_id, kind, payload, reason):
        now = utcnow()
        with self._connect() as connection:
            cur = connection.execute(
                "INSERT INTO pending_copies(station_id, stable_id, kind, payload, reason, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (
                    station_id,
                    stable_id,
                    kind,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    reason,
                    now,
                ),
            )
            copy_id = cur.lastrowid
        return self.get_pending_copy(copy_id)

    def get_pending_copy(self, copy_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM pending_copies WHERE id = ?", (copy_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "station_id": row["station_id"],
            "stable_id": row["stable_id"],
            "kind": row["kind"],
            "payload": json.loads(row["payload"]),
            "reason": row["reason"],
            "status": row["status"],
            "created_at": row["created_at"],
            "handled_at": row["handled_at"],
        }

    def list_pending_copies(self, status=None):
        with self._connect() as connection:
            if status:
                rows = connection.execute(
                    "SELECT * FROM pending_copies WHERE status = ? ORDER BY id", (status,)
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM pending_copies ORDER BY id"
                ).fetchall()
        return [
            {
                "id": row["id"],
                "station_id": row["station_id"],
                "stable_id": row["stable_id"],
                "kind": row["kind"],
                "payload": json.loads(row["payload"]),
                "reason": row["reason"],
                "status": row["status"],
                "created_at": row["created_at"],
                "handled_at": row["handled_at"],
            }
            for row in rows
        ]

    def resolve_pending_copy(self, copy_id, status):
        with self._connect() as connection:
            cur = connection.execute(
                "UPDATE pending_copies SET status = ?, handled_at = ? WHERE id = ?",
                (status, utcnow(), copy_id),
            )
            if cur.rowcount == 0:
                raise NotFoundError("pending copy not found: " + str(copy_id))

    # ----- 处置先入库先得 -----
    def claim_disposition(self, incident_id, disposition_key, entity_id, actor_id):
        """原子抢占处置权；已被他方占有时返回 False（先入库者胜）。"""
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO disposition_claims(incident_id, disposition_key, entity_id, actor_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (incident_id, disposition_key, entity_id, actor_id, utcnow()),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def find_disposition_claim(self, incident_id, disposition_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM disposition_claims WHERE incident_id = ? AND disposition_key = ?",
                (incident_id, disposition_key),
            ).fetchone()
        if not row:
            return None
        return {
            "incident_id": row["incident_id"],
            "disposition_key": row["disposition_key"],
            "entity_id": row["entity_id"],
            "actor_id": row["actor_id"],
            "created_at": row["created_at"],
        }

    # ----- 断网登记队列 / 重连续传 -----
    def enqueue_record(self, station_id, kind, stable_id, payload):
        now = utcnow()
        with self._connect() as connection:
            cur = connection.execute(
                "INSERT INTO sync_outbox(station_id, kind, stable_id, payload, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'queued', ?, ?)",
                (
                    station_id,
                    kind,
                    stable_id,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            return cur.lastrowid

    def reset_inflight(self):
        """重启后把未完成的 inflight 步骤退回队列，接着重试。"""
        with self._connect() as connection:
            connection.execute(
                "UPDATE sync_outbox SET status = 'queued', updated_at = ? WHERE status = 'inflight'",
                (utcnow(),),
            )

    def claim_next_queued(self, station_id=None):
        """取出最早一条排队记录并原子置为 inflight；队列为空返回 None。"""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if station_id:
                row = connection.execute(
                    "SELECT * FROM sync_outbox WHERE status = 'queued' AND station_id = ? "
                    "ORDER BY id LIMIT 1",
                    (station_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM sync_outbox WHERE status = 'queued' ORDER BY id LIMIT 1"
                ).fetchone()
            if not row:
                connection.commit()
                return None
            connection.execute(
                "UPDATE sync_outbox SET status = 'inflight', attempts = attempts + 1, updated_at = ? WHERE id = ?",
                (utcnow(), row["id"]),
            )
            connection.commit()
            item = {
                "id": row["id"],
                "station_id": row["station_id"],
                "kind": row["kind"],
                "stable_id": row["stable_id"],
                "payload": json.loads(row["payload"]),
                "attempts": int(row["attempts"]) + 1,
            }
            return item
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def mark_outbox_done(self, item_id):
        with self._connect() as connection:
            connection.execute(
                "UPDATE sync_outbox SET status = 'done', last_error = NULL, updated_at = ? WHERE id = ?",
                (utcnow(), item_id),
            )

    def mark_outbox_failed(self, item_id, error):
        """写入失败：记录留在队列，稍后重试。"""
        with self._connect() as connection:
            connection.execute(
                "UPDATE sync_outbox SET status = 'queued', last_error = ?, updated_at = ? WHERE id = ?",
                (str(error), utcnow(), item_id),
            )

    def mark_outbox_dead(self, item_id, error):
        """确定性的数据错误不会因重试改变，隔离到 dead 以免阻塞队列。"""
        with self._connect() as connection:
            connection.execute(
                "UPDATE sync_outbox SET status = 'dead', last_error = ?, updated_at = ? WHERE id = ?",
                (str(error), utcnow(), item_id),
            )

    def list_outbox(self, status=None):
        with self._connect() as connection:
            if status:
                rows = connection.execute(
                    "SELECT * FROM sync_outbox WHERE status = ? ORDER BY id", (status,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM sync_outbox ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "station_id": row["station_id"],
                "kind": row["kind"],
                "stable_id": row["stable_id"],
                "payload": json.loads(row["payload"]),
                "status": row["status"],
                "attempts": row["attempts"],
                "last_error": row["last_error"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def save_reconciliation(self, station_id, report):
        now = utcnow()
        with self._connect() as connection:
            cur = connection.execute(
                "INSERT INTO reconciliation_runs(station_id, report, created_at) VALUES (?, ?, ?)",
                (station_id, json.dumps(report, ensure_ascii=False, sort_keys=True), now),
            )
            run_id = cur.lastrowid
        return run_id

    def list_reconciliations(self, station_id=None):
        with self._connect() as connection:
            if station_id:
                rows = connection.execute(
                    "SELECT * FROM reconciliation_runs WHERE station_id = ? ORDER BY id",
                    (station_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM reconciliation_runs ORDER BY id"
                ).fetchall()
        return [
            {
                "id": row["id"],
                "station_id": row["station_id"],
                "report": json.loads(row["report"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]
