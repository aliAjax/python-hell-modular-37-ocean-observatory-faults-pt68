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
                CREATE TABLE IF NOT EXISTS offline_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_queue_status
                    ON offline_queue(status, id);
                CREATE INDEX IF NOT EXISTS idx_queue_batch
                    ON offline_queue(batch_id, seq);
                CREATE TABLE IF NOT EXISTS pending_copies (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    stable_id TEXT NOT NULL,
                    entity_id TEXT,
                    incident_id TEXT,
                    asset_id TEXT,
                    payload TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT,
                    resolved_by TEXT,
                    resolution TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_pending_status
                    ON pending_copies(status);
            """)

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
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                    (entity_id, kind, status, payload, actor_id, now, now),
                )
        except sqlite3.IntegrityError:
            raise ConflictError("entity already exists: " + entity_id)
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

    # --- offline retry queue -------------------------------------------------

    def enqueue_offline(self, batch_id, entries):
        """Persist offline records before any write so a restart can resume.

        entries: iterable of (seq, kind, record_dict, actor_id, actor_role).
        """
        now = utcnow()
        with self._connect() as connection:
            connection.executemany(
                "INSERT INTO offline_queue(batch_id, seq, kind, payload, actor_id, actor_role, "
                "status, attempts, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)",
                [
                    (
                        batch_id,
                        int(seq),
                        kind,
                        json.dumps(record, ensure_ascii=False, sort_keys=True),
                        actor_id,
                        actor_role,
                        now,
                        now,
                    )
                    for seq, kind, record, actor_id, actor_role in entries
                ],
            )

    @staticmethod
    def _queue_from_row(row):
        return {
            "id": int(row["id"]),
            "batch_id": row["batch_id"],
            "seq": int(row["seq"]),
            "kind": row["kind"],
            "payload": json.loads(row["payload"]),
            "actor_id": row["actor_id"],
            "actor_role": row["actor_role"],
            "status": row["status"],
            "attempts": int(row["attempts"]),
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_queue(self, batch_id=None, status=None):
        clauses = []
        params = []
        if batch_id:
            clauses.append("batch_id = ?")
            params.append(batch_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        order = " ORDER BY batch_id, seq" if batch_id else " ORDER BY id"
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM offline_queue" + where + order, params
            ).fetchall()
        return [self._queue_from_row(row) for row in rows]

    def next_pending_queue(self, limit=100):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM offline_queue WHERE status = 'pending' ORDER BY id LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [self._queue_from_row(row) for row in rows]

    def mark_queue_done(self, queue_id):
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_queue SET status = 'done', updated_at = ? WHERE id = ?",
                (utcnow(), int(queue_id)),
            )

    def mark_queue_failed(self, queue_id, error):
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_queue SET attempts = attempts + 1, last_error = ?, updated_at = ? "
                "WHERE id = ?",
                (str(error)[:500], utcnow(), int(queue_id)),
            )

    # --- pending copies (待处理副本) ----------------------------------------

    def create_pending_copy(self, copy_id, kind, stable_id, entity_id, payload, reason,
                            actor_id, incident_id=None, asset_id=None):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO pending_copies(id, kind, stable_id, entity_id, incident_id, asset_id, "
                "payload, reason, status, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (
                    copy_id,
                    kind,
                    stable_id,
                    entity_id,
                    incident_id,
                    asset_id,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    reason,
                    actor_id,
                    utcnow(),
                ),
            )
        return self.get_pending_copy(copy_id)

    @staticmethod
    def _copy_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "stable_id": row["stable_id"],
            "entity_id": row["entity_id"],
            "incident_id": row["incident_id"],
            "asset_id": row["asset_id"],
            "payload": json.loads(row["payload"]),
            "reason": row["reason"],
            "status": row["status"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
            "resolved_by": row["resolved_by"],
            "resolution": row["resolution"],
        }

    def get_pending_copy(self, copy_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM pending_copies WHERE id = ?", (copy_id,)
            ).fetchone()
        return self._copy_from_row(row) if row else None

    def list_pending_copies(self, status=None):
        clauses = []
        params = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM pending_copies" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._copy_from_row(row) for row in rows]

    def resolve_pending_copy(self, copy_id, status, resolution, actor_id):
        with self._connect() as connection:
            connection.execute(
                "UPDATE pending_copies SET status = ?, resolution = ?, resolved_by = ?, resolved_at = ? "
                "WHERE id = ?",
                (status, resolution, actor_id, utcnow(), copy_id),
            )
        return self.get_pending_copy(copy_id)

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
