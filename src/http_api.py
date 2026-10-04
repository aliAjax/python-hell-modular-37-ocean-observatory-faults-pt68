import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .domain import (
    Actor,
    ConflictError,
    DomainError,
    InvalidTransition,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)


def _json_bytes(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def create_handler(service, rules, static_dir, sync_service=None):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ModularPython/1.0"

        def log_message(self, format, *args):
            return

        def _send(self, status, payload):
            body = _json_bytes(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, status, body):
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _actor(self):
            return Actor.from_headers(self.headers)

        def _body(self):
            length = int(self.headers.get("Content-Length", "0") or 0)
            if not length:
                return {}
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise ValidationError("request body must be valid JSON")
            if not isinstance(value, dict):
                raise ValidationError("request body must be a JSON object")
            return value

        def _fail(self, exc):
            if isinstance(exc, PermissionDenied):
                status = 403
            elif isinstance(exc, NotFoundError):
                status = 404
            elif isinstance(exc, (ConflictError, InvalidTransition)):
                status = 409
            elif isinstance(exc, ValidationError):
                status = 400
            elif isinstance(exc, DomainError):
                status = 400
            else:
                status = 500
            self._send(status, {"error": str(exc), "type": type(exc).__name__})

        def do_GET(self):
            try:
                parsed = urlparse(self.path)
                parts = [part for part in parsed.path.split("/") if part]
                if parsed.path == "/health":
                    return self._send(200, service.health())
                if parsed.path == "/":
                    index = os.path.join(static_dir, "index.html")
                    with open(index, "r", encoding="utf-8") as handle:
                        return self._send_html(200, handle.read())
                if parts == ["api", "audit"]:
                    return self._send(200, {"items": service.audit_log()})
                if len(parts) == 5 and parts[:3] == ["api", "sync", "stations"] \
                        and parts[4] == "outbox":
                    query = parse_qs(parsed.query)
                    status = query.get("status", [None])[0]
                    return self._send(
                        200,
                        {"items": sync_service.list_outbox(self._actor(), parts[3], status)},
                    )
                if len(parts) == 5 and parts[:3] == ["api", "sync", "stations"] \
                        and parts[4] == "reconciliations":
                    return self._send(
                        200,
                        {"items": sync_service.list_reconciliations(self._actor(), parts[3])},
                    )
                if len(parts) == 3 and parts[:2] == ["api", "sync"] and parts[2] == "pending":
                    query = parse_qs(parsed.query)
                    status = query.get("status", ["pending"])[0]
                    return self._send(200, {"items": sync_service.list_pending(status)})
                if len(parts) == 4 and parts[:2] == ["api", "sync"] and parts[2] == "versions":
                    return self._send(200, {"items": sync_service.list_versions(parts[3])})
                if len(parts) == 3 and parts[:2] == ["api", "entities"]:
                    return self._send(200, service.get(parts[2]))
                if len(parts) >= 2 and parts[0] == "api" and parts[1] not in ("entities", "sync"):
                    if len(parts) == 3:
                        return self._send(200, service.get(parts[2]))
                    query = parse_qs(parsed.query)
                    status = query.get("status", [None])[0]
                    return self._send(200, {"items": service.list(parts[1], status=status)})
                raise NotFoundError("not found")
            except Exception as exc:
                self._fail(exc)

        def do_POST(self):
            try:
                parsed = urlparse(self.path)
                parts = [part for part in parsed.path.split("/") if part]
                actor = self._actor()
                # 断网登记：回网前只入持久队列
                if len(parts) == 5 and parts[:3] == ["api", "sync", "stations"] \
                        and parts[4] == "queue":
                    body = self._body()
                    return self._send(
                        202,
                        sync_service.enqueue_records(actor, parts[3], body.get("records", [])),
                    )
                # 回网：先排空队列合并事件/遥测/恢复动作，再与中心对账
                if len(parts) == 5 and parts[:3] == ["api", "sync", "stations"] \
                        and parts[4] == "reconnect":
                    return self._send(200, sync_service.reconnect(actor, parts[3]))
                # 直接提交一个回传批次：只合并不对账
                if parts == ["api", "sync", "batches"]:
                    body = self._body()
                    return self._send(
                        200,
                        sync_service.apply_records(
                            actor, body.get("station_id"), body.get("records", [])),
                    )
                # 触发对账（合并已排空时单独调用）
                if len(parts) == 5 and parts[:3] == ["api", "sync", "stations"] \
                        and parts[4] == "reconcile":
                    return self._send(200, sync_service.reconcile(actor, parts[3], save=True))
                # 待处理副本：apply / reject
                if len(parts) == 4 and parts[:2] == ["api", "sync"] and parts[2] == "pending":
                    body = self._body()
                    decision = body.get("decision")
                    return self._send(
                        200, sync_service.resolve_pending(actor, parts[3], decision))
                # 两版测量/处置择一
                if len(parts) == 5 and parts[:2] == ["api", "sync"] and parts[2] == "versions":
                    return self._send(
                        200, sync_service.select_version(actor, parts[3], parts[4]))
                if parts == ["api", "offline-records"]:
                    body = self._body()
                    return self._send(
                        200,
                        sync_service.apply_records(actor, body.get("station_id"),
                                                   body.get("records", [])),
                    )
                if len(parts) == 3 and parts[:2] == ["api", "entities"]:
                    body = self._body()
                    action = body.pop("action", None)
                    if not action:
                        raise ValidationError("action is required")
                    data = body.pop("data", body)
                    expected = body.pop("expected_version", None)
                    return self._send(200, service.transition(actor, parts[2], action, data, expected))
                if len(parts) == 4 and parts[0] == "api" and parts[3] == "actions":
                    body = self._body()
                    action = body.pop("action", None)
                    if not action:
                        raise ValidationError("action is required")
                    return self._send(
                        200,
                        service.transition(
                            actor,
                            parts[2],
                            action,
                            body.pop("data", body),
                            body.pop("expected_version", None),
                        ),
                    )
                if len(parts) == 5 and parts[0] == "api" and parts[4] == "actions":
                    return self._send(200, service.transition(actor, parts[2], parts[3], self._body(), None))
                if len(parts) == 2 and parts[0] == "api":
                    body = self._body()
                    idem = self.headers.get("Idempotency-Key")
                    return self._send(201, service.create(actor, parts[1], body, idem))
                raise NotFoundError("not found")
            except Exception as exc:
                self._fail(exc)

    return Handler


def create_server(host, port, service, rules, static_dir, sync_service=None):
    handler = create_handler(service, rules, static_dir, sync_service)
    return ThreadingHTTPServer((host, int(port)), handler)
