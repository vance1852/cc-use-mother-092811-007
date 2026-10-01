"""提供不依赖第三方框架的 HTTP/JSON 边界。"""

from __future__ import annotations

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import DomainError, ValidationError
from .orchestration import OrchestrationService
from .service import DomainService
from .storage import Database


def route(services, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到领域服务。"""

    if isinstance(services, DomainService):
        services = {"domain": services, "orch": OrchestrationService(services.database,
                                                                     services.clock)}
    service: DomainService = services["domain"]
    orch: OrchestrationService = services["orch"]
    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    query = parse_qs(parsed.query)
    actor_id = headers.get("X-Actor-Id", "")

    def q(name: str, default: str = "") -> str:
        return query.get(name, [default])[0]

    def call(action, **kwargs):
        result = action(**kwargs)
        return result

    try:
        if method == "GET" and parsed.path == "/health":
            valid, count = service.verify_audit()
            return 200, {"status": "ok", "audit_valid": valid, "audit_events": count}

        # ------------------------------------------------------------ 基础登记
        if method == "POST" and parsed.path == "/organizations":
            receipt = service.register_organization(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/actors":
            receipt = service.register_actor(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/sites":
            receipt = service.register_site(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "POST" and parsed.path == "/domain-records":
            receipt = service.record_domain_data(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        if method == "GET" and parsed.path == "/domain-records":
            site_id = q("site_id")
            if not site_id:
                raise ValidationError("site_id 不能为空")
            return 200, {"items": [item.__dict__
                                   for item in service.list_domain_data(site_id, q("category") or None)]}
        if method == "GET" and parsed.path == "/audit-events":
            return 200, {"items": service.audit_events(int(q("after_sequence", "0")))}

        # ------------------------------------------------------------ 版本化资料
        if method == "POST" and parsed.path == "/references":
            receipt = orch.publish_reference(
                actor_id=actor_id, request_id=body["request_id"],
                entity_type=body["entity_type"], entity_id=body["entity_id"],
                payload=body["payload"], valid_from=body.get("valid_from"))
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/references/([^/]+)/([^/]+)", parsed.path)
        if method == "GET" and match:
            return 200, orch.get_reference(match.group(1), match.group(2), q("at") or None)

        # ------------------------------------------------------------ 货物
        if method == "POST" and parsed.path == "/shipments":
            receipt = orch.register_shipment(actor_id=actor_id, **body)
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/shipments/([^/]+)/handover", parsed.path)
        if method == "POST" and match:
            receipt = orch.mark_handover(actor_id=actor_id, cargo_id=match.group(1),
                                         request_id=body["request_id"],
                                         note=body.get("note", ""))
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/shipments/([^/]+)/delivered", parsed.path)
        if method == "POST" and match:
            receipt = orch.mark_delivered(actor_id=actor_id, cargo_id=match.group(1),
                                          request_id=body["request_id"])
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/shipments/([^/]+)/timeline", parsed.path)
        if method == "GET" and match:
            return 200, orch.cargo_timeline(match.group(1))

        # ------------------------------------------------------------ 中断与恢复
        if method == "POST" and parsed.path == "/disruptions/detect":
            return 200, orch.detect_disruption(actor_id=actor_id,
                                               advisory_id=body["advisory_id"])
        if method == "GET" and parsed.path == "/disruptions":
            return 200, {"items": orch.list_impacts(q("advisory_id"))}
        if method == "POST" and parsed.path == "/recoveries":
            receipt = orch.record_recovery(actor_id=actor_id,
                                           request_id=body["request_id"],
                                           advisory_id=body["advisory_id"])
            return 200 if receipt.replayed else 201, receipt.__dict__

        # ------------------------------------------------------------ 候选路径
        if method == "GET" and parsed.path == "/candidates":
            return 200, orch.evaluate_candidates(actor_id=actor_id,
                                                 cargo_id=q("cargo_id"))

        # ------------------------------------------------------------ 方案生命周期
        if method == "POST" and parsed.path == "/plans":
            receipt = orch.create_plan(
                actor_id=actor_id, request_id=body["request_id"],
                cargo_id=body["cargo_id"], advisory_id=body.get("advisory_id"),
                candidate_index=int(body.get("candidate_index", 0)))
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/plans/([^/]+)", parsed.path)
        if method == "GET" and match:
            return 200, orch.get_plan(match.group(1))
        match = re.fullmatch(r"/plans/([^/]+)/lock", parsed.path)
        if method == "POST" and match:
            receipt = orch.lock_plan(actor_id=actor_id, request_id=body["request_id"],
                                     plan_id=match.group(1))
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/plans/([^/]+)/approvals", parsed.path)
        if method == "POST" and match:
            receipt = orch.approve_plan(actor_id=actor_id, request_id=body["request_id"],
                                        plan_id=match.group(1), key=body["key"],
                                        note=body.get("note", ""))
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/plans/([^/]+)/commit", parsed.path)
        if method == "POST" and match:
            receipt = orch.commit_plan(actor_id=actor_id, request_id=body["request_id"],
                                       plan_id=match.group(1))
            return 200 if receipt.replayed else 201, receipt.__dict__
        match = re.fullmatch(r"/plans/([^/]+)/cancel", parsed.path)
        if method == "POST" and match:
            receipt = orch.cancel_plan(actor_id=actor_id, request_id=body["request_id"],
                                       plan_id=match.group(1),
                                       reason=body.get("reason", "人工撤销"))
            return 200 if receipt.replayed else 201, receipt.__dict__

        # ------------------------------------------------------------ 视图与复原
        if method == "GET" and parsed.path == "/ops":
            return 200, orch.ops_view(actor_id=actor_id, cargo_id=q("cargo_id") or None,
                                      at=q("at") or None)
        if method == "GET" and parsed.path == "/finance":
            return 200, orch.finance_view(actor_id=actor_id, cargo_id=q("cargo_id") or None,
                                          at=q("at") or None)
        if method == "GET" and parsed.path == "/reconstruct":
            at = q("at")
            if not at:
                raise ValidationError("at 不能为空")
            return 200, orch.reconstruct(at, cargo_id=q("cargo_id") or None)

        return 404, {"error": "route_not_found", "message": "接口不存在"}
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, KeyError) as exc:
        return 400, {"error": "invalid_request", "message": f"缺少或非法字段：{exc}"}
    except ValueError as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    services: dict[str, Any]

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.services, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动本地 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动国际通道韧性编排服务")
    parser.add_argument("--database", default="service.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    database = Database(args.database)
    domain = DomainService(database)
    services = {"domain": domain, "orch": OrchestrationService(database, domain.clock)}
    Handler.services = services
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
