"""国际通道韧性编排服务。

在基础服务的事务、幂等与审计边界上，组织线路分段、口岸日历、承运能力、
货物限制、客户时限、合同费用与风险通告七类版本化资料；在中断时生成可解释
候选路径与受影响清单，并以 saga 方式锁定全部分段、完成必要审批后切换。

关键不变量：

* 候选方案只有在 ``selected`` 且必需审批齐备时才能执行切换；
* 任一分段锁定失败，已锁定能力立即释放，承诺费仍然记账；
* 同一货物同时只有一个生效方案，竞争方案在切换时作废；
* 货物一旦交接，迟到的恢复消息或任何新方案都不能再改路线。
"""

from __future__ import annotations

import json
import re
import uuid
from typing import Any, Callable

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .models import Actor
from .planning import (
    NetworkContext,
    enumerate_candidates,
    evaluate_path,
    explain_reroute_baseline,
    format_ts,
    parse_ts,
    required_approval_roles,
)
from .storage import Database

REF_KINDS = frozenset({
    "segment",            # 线路分段
    "border_calendar",    # 口岸日历
    "capacity",           # 承运能力
    "cargo_restriction",  # 货物限制
    "contract_rate",      # 合同费用
})

# 各类资料的业务主键字段。
_ENTITY_KEY = {
    "segment": "segment_id",
    "border_calendar": "border_crossing_id",
    "capacity": "capacity_id",
    "cargo_restriction": "restriction_id",
    "contract_rate": "rate_id",
}

# 能力在容量核算中视为仍然占用的锁定状态。
_OCCUPYING_HOLD_STATES = ("committed", "held")

_MODES = frozenset({"rail", "sea", "road", "air"})
_NOTICE_SEVERITIES = frozenset({"info", "warning", "critical"})
_HANDOVER_EVENT = "handed_over"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")


class CorridorOrchestrator:
    """实现版本资料、中断评估、方案切换与视图查询。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ 基础

    def _now(self) -> str:
        return self.clock.now().isoformat().replace("+00:00", "Z")

    def _actor(self, connection, actor_id: str) -> Actor:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        actor = Actor(row["actor_id"], row["display_name"], row["role"],
                      row["organization_id"], bool(row["active"]))
        if not actor.active:
            raise PermissionDenied("操作者已停用")
        return actor

    @staticmethod
    def _require(actor: Actor, *roles: str) -> None:
        if actor.role not in roles:
            raise PermissionDenied(f"当前角色 {actor.role} 不能执行该动作")

    def _identifier(self, value: Any, field: str) -> str:
        value = str(value).strip()
        if not _IDENTIFIER.fullmatch(value):
            raise ValidationError(f"{field} 格式无效")
        return value

    @staticmethod
    def _ts(value: Any, field: str) -> str:
        try:
            return format_ts(parse_ts(str(value)))
        except (ValueError, TypeError) as exc:
            raise ValidationError(f"{field} 必须是 ISO 时间") from exc

    @staticmethod
    def _number(value: Any, field: str, *, minimum: float = 0.0) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(f"{field} 必须是数字")
        if value < minimum:
            raise ValidationError(f"{field} 不能小于 {minimum}")
        return float(value)

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any], create: Callable[[], tuple[str, str, dict[str, Any]]]):
        request_id = self._identifier(request_id, "request_id")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return {**json.loads(row["response_json"]), "request_id": request_id,
                    "resource_type": row["resource_type"], "resource_id": row["resource_id"],
                    "replayed": True}
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,"
            "resource_id,response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id,
             canonical_json(response), self._now()),
        )
        return {**response, "request_id": request_id, "resource_type": resource_type,
                "resource_id": resource_id, "replayed": False}

    def _decision(self, connection, *, kind: str, detail: dict[str, Any], actor_id: str,
                  shipment_id: str | None = None, plan_id: str | None = None,
                  notice_id: str | None = None) -> None:
        connection.execute(
            "INSERT INTO corridor_decisions(decision_id,shipment_id,plan_id,notice_id,kind,"
            "detail_json,actor_id,at) VALUES(?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, shipment_id, plan_id, notice_id, kind,
             canonical_json(detail), actor_id, self._now()),
        )

    def _set_plan_status(self, connection, plan_id: str, status: str, note: str) -> None:
        connection.execute(
            "UPDATE corridor_plans SET status=? WHERE plan_id=?", (status, plan_id)
        )
        connection.execute(
            "INSERT INTO corridor_plan_status_history(history_id,plan_id,status,note,at) "
            "VALUES(?,?,?,?,?)",
            (uuid.uuid4().hex, plan_id, status, note, self._now()),
        )

    # ------------------------------------------------------------ 版本化资料

    def _validate_reference(self, kind: str, data: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(data, dict) or not data:
            raise ValidationError("data 必须是非空对象")
        if kind == "segment":
            for field in ("segment_id", "from_node", "to_node", "mode"):
                if not str(data.get(field, "")).strip():
                    raise ValidationError(f"segment.{field} 不能为空")
            if data["mode"] not in _MODES:
                raise ValidationError("segment.mode 不被支持")
            data["transit_hours"] = self._number(data.get("transit_hours", 0), "transit_hours")
        elif kind == "border_calendar":
            key = "border_crossing_id"
            if not str(data.get(key, "")).strip():
                raise ValidationError(f"border_calendar.{key} 不能为空")
            intervals = data.get("closed_intervals", [])
            if not isinstance(intervals, list):
                raise ValidationError("closed_intervals 必须是数组")
            normalized = []
            for interval in intervals:
                start = self._ts(interval.get("from"), "closed_intervals.from")
                end = self._ts(interval.get("to"), "closed_intervals.to")
                if parse_ts(end) <= parse_ts(start):
                    raise ValidationError("口岸关闭窗口结束时间必须晚于开始时间")
                normalized.append({"from": start, "to": end,
                                   "reason": str(interval.get("reason", ""))[:200]})
            data["closed_intervals"] = normalized
        elif kind == "capacity":
            for field in ("capacity_id", "segment_id", "carrier_id", "valid_from", "valid_to", "unit"):
                if not str(data.get(field, "")).strip():
                    raise ValidationError(f"capacity.{field} 不能为空")
            data["valid_from"] = self._ts(data["valid_from"], "capacity.valid_from")
            data["valid_to"] = self._ts(data["valid_to"], "capacity.valid_to")
            if parse_ts(data["valid_to"]) <= parse_ts(data["valid_from"]):
                raise ValidationError("承运能力失效时间必须晚于生效时间")
            data["capacity_units"] = self._number(data.get("capacity_units", 0), "capacity_units")
            data["unit_cost"] = self._number(data.get("unit_cost", 0), "unit_cost")
            data["commitment_fee"] = self._number(data.get("commitment_fee", 0), "commitment_fee")
            data.setdefault("currency", "USD")
        elif kind == "cargo_restriction":
            for field in ("restriction_id",):
                if not str(data.get(field, "")).strip():
                    raise ValidationError(f"cargo_restriction.{field} 不能为空")
            if not isinstance(data.get("scope", {}), dict):
                raise ValidationError("cargo_restriction.scope 必须是对象")
            classes = data.get("forbidden_classes", [])
            cargo_types = data.get("forbidden_cargo_types", [])
            if not isinstance(classes, list) or not isinstance(cargo_types, list):
                raise ValidationError("禁运清单必须是数组")
            if not classes and not cargo_types:
                raise ValidationError("货物限制至少要声明一类禁止危险品或货种")
        elif kind == "contract_rate":
            for field in ("rate_id", "segment_id", "carrier_id", "valid_from", "valid_to"):
                if not str(data.get(field, "")).strip():
                    raise ValidationError(f"contract_rate.{field} 不能为空")
            data["valid_from"] = self._ts(data["valid_from"], "contract_rate.valid_from")
            data["valid_to"] = self._ts(data["valid_to"], "contract_rate.valid_to")
            if parse_ts(data["valid_to"]) <= parse_ts(data["valid_from"]):
                raise ValidationError("合同费用失效时间必须晚于生效时间")
            data["unit_cost"] = self._number(data.get("unit_cost", 0), "unit_cost")
            data["fixed_cost"] = self._number(data.get("fixed_cost", 0), "fixed_cost")
            data["minimum_charge"] = self._number(data.get("minimum_charge", 0), "minimum_charge")
            data.setdefault("currency", "USD")
        return data

    def publish_reference(self, *, request_id: str, actor_id: str, kind: str,
                          data: dict[str, Any]) -> dict[str, Any]:
        """发布一版线路、口岸日历、能力、限制或合同费用。"""

        if kind not in REF_KINDS:
            raise ValidationError("资料类别不被支持")
        data = self._validate_reference(kind, data)
        entity_id = self._identifier(data[_ENTITY_KEY[kind]], f"{kind}.{_ENTITY_KEY[kind]}")
        payload = {"actor_id": actor_id, "kind": kind, "entity_id": entity_id, "data": data}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            if kind == "contract_rate":
                self._require(actor, "admin", "finance")
            elif kind == "cargo_restriction":
                self._require(actor, "admin", "operator", "safety")
            else:
                self._require(actor, "admin", "operator")

            def create() -> tuple[str, str, dict[str, Any]]:
                row = connection.execute(
                    "SELECT COALESCE(MAX(version),0) AS version FROM corridor_ref_versions "
                    "WHERE kind=? AND entity_id=?", (kind, entity_id)
                ).fetchone()
                version = row["version"] + 1
                published_at = self._now()
                connection.execute(
                    "INSERT INTO corridor_ref_versions(kind,entity_id,version,payload_json,"
                    "payload_hash,published_at,published_by) VALUES(?,?,?,?,?,?,?)",
                    (kind, entity_id, version, canonical_json(data), digest(data),
                     published_at, actor_id),
                )
                append_event(connection, actor_id=actor_id, action=f"corridor.reference_published.{kind}",
                             resource_type="corridor_reference", resource_id=f"{kind}/{entity_id}/v{version}",
                             detail={"kind": kind, "entity_id": entity_id, "version": version,
                                     "payload_hash": digest(data)}, occurred_at=published_at)
                self._decision(connection, kind="reference.published",
                               detail={"kind": kind, "entity_id": entity_id, "version": version},
                               actor_id=actor_id)
                return "corridor_reference", f"{kind}/{entity_id}/v{version}", {
                    "kind": kind, "entity_id": entity_id, "version": version}

            return self._idempotent(connection, request_id=request_id,
                                    action=f"publish_reference:{kind}", payload=payload, create=create)

    # -------------------------------------------------------------- 风险通告

    def publish_notice(self, *, request_id: str, actor_id: str, notice_id: str, title: str,
                       severity: str, detail: str, impact: dict[str, Any]) -> dict[str, Any]:
        """发布一版风险通告（限流、关闭、承运暂停等）。"""

        title = str(title).strip()
        if not title:
            raise ValidationError("title 不能为空")
        if severity not in _NOTICE_SEVERITIES:
            raise ValidationError("severity 不被支持")
        if not isinstance(impact, dict):
            raise ValidationError("impact 必须是对象")
        notice_id = self._identifier(notice_id, "notice_id")
        impact = {key: list(value) for key, value in impact.items() if isinstance(value, list)}
        payload = {"actor_id": actor_id, "notice_id": notice_id, "title": title,
                   "severity": severity, "detail": detail, "impact": impact}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator", "safety")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                connection.execute(
                    "INSERT INTO corridor_notices(notice_id,title,scope_json,severity,detail,"
                    "status,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (notice_id, title, canonical_json(impact), severity, str(detail)[:1000],
                     "active", actor_id, now),
                )
                append_event(connection, actor_id=actor_id, action="corridor.notice_published",
                             resource_type="corridor_notice", resource_id=notice_id,
                             detail={"title": title, "severity": severity, "impact": impact},
                             occurred_at=now)
                self._decision(connection, kind="notice.published",
                               detail={"title": title, "severity": severity, "impact": impact},
                               actor_id=actor_id, notice_id=notice_id)
                return "corridor_notice", notice_id, {"notice_id": notice_id, "status": "active"}

            return self._idempotent(connection, request_id=request_id,
                                    action="publish_notice", payload=payload, create=create)

    def recover_notice(self, *, request_id: str, actor_id: str, notice_id: str,
                       note: str = "") -> dict[str, Any]:
        """登记通道恢复。恢复只影响后续规划，绝不回拉已交接或已切换的货物。"""

        payload = {"actor_id": actor_id, "notice_id": notice_id, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator", "safety")

            def create() -> tuple[str, str, dict[str, Any]]:
                existing = connection.execute(
                    "SELECT * FROM corridor_notices WHERE notice_id=?", (notice_id,)
                ).fetchone()
                if existing is None:
                    raise NotFoundError("风险通告不存在")
                if existing["status"] != "active":
                    raise ConflictError("风险通告已恢复，重复的恢复消息被忽略")
                now = self._now()
                connection.execute(
                    "UPDATE corridor_notices SET status='recovered',recovered_at=?,recovered_by=? "
                    "WHERE notice_id=? AND status='active'", (now, actor_id, notice_id)
                )
                handed = [row["shipment_id"] for row in connection.execute(
                    "SELECT DISTINCT cp.shipment_id FROM corridor_checkpoints cp "
                    "WHERE cp.event=? ORDER BY cp.shipment_id", (_HANDOVER_EVENT,))]
                append_event(connection, actor_id=actor_id, action="corridor.notice_recovered",
                             resource_type="corridor_notice", resource_id=notice_id,
                             detail={"note": note, "handed_over_shipments_unchanged": handed},
                             occurred_at=now)
                self._decision(connection, kind="notice.recovered",
                               detail={"note": note,
                                       "handed_over_shipments_unchanged": handed,
                                       "policy": "恢复消息不会改变任何货物的生效路线"},
                               actor_id=actor_id, notice_id=notice_id)
                return "corridor_notice", notice_id, {
                    "notice_id": notice_id, "status": "recovered",
                    "handed_over_shipments_unchanged": handed}

            return self._idempotent(connection, request_id=request_id,
                                    action="recover_notice", payload=payload, create=create)

    # ---------------------------------------------------------------- 货物

    def register_shipment(self, *, request_id: str, actor_id: str, shipment_id: str,
                          cargo_type: str, units: float, origin_node: str, destination_node: str,
                          customer_ref: str, must_arrive_by: str, weight_kg: float = 0.0,
                          hazmat_class: str | None = None) -> dict[str, Any]:
        shipment_id = self._identifier(shipment_id, "shipment_id")
        for field_name, value in (("cargo_type", cargo_type),
                                  ("origin_node", origin_node), ("destination_node", destination_node),
                                  ("customer_ref", customer_ref)):
            if not str(value).strip():
                raise ValidationError(f"{field_name} 不能为空")
        units = self._number(units, "units", minimum=0.0001)
        weight_kg = self._number(weight_kg, "weight_kg")
        deadline = self._ts(must_arrive_by, "must_arrive_by")
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "cargo_type": cargo_type,
                   "units": units, "origin_node": origin_node, "destination_node": destination_node,
                   "customer_ref": customer_ref, "must_arrive_by": deadline,
                   "weight_kg": weight_kg, "hazmat_class": hazmat_class}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                try:
                    connection.execute(
                        "INSERT INTO corridor_shipments(shipment_id,cargo_type,hazmat_class,weight_kg,"
                        "units,origin_node,destination_node,customer_ref,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (shipment_id, cargo_type, hazmat_class, weight_kg, units,
                         origin_node, destination_node, customer_ref, actor_id, now),
                    )
                except Exception as exc:
                    raise ConflictError("货物编号已经存在") from exc
                connection.execute(
                    "INSERT INTO corridor_deadlines(shipment_id,version,must_arrive_by,published_at,published_by)"
                    " VALUES(?,1,?,?,?)",
                    (shipment_id, deadline, now, actor_id),
                )
                append_event(connection, actor_id=actor_id, action="corridor.shipment_registered",
                             resource_type="corridor_shipment", resource_id=shipment_id,
                             detail={"cargo_type": cargo_type, "origin_node": origin_node,
                                     "destination_node": destination_node,
                                     "must_arrive_by": deadline}, occurred_at=now)
                self._decision(connection, kind="shipment.registered",
                               detail={"customer_ref": customer_ref, "must_arrive_by": deadline},
                               actor_id=actor_id, shipment_id=shipment_id)
                return "corridor_shipment", shipment_id, {"shipment_id": shipment_id, "deadline_version": 1}

            return self._idempotent(connection, request_id=request_id,
                                    action="register_shipment", payload=payload, create=create)

    def revise_deadline(self, *, request_id: str, actor_id: str, shipment_id: str,
                        must_arrive_by: str) -> dict[str, Any]:
        """发布客户交付窗的新版本，历史版本保留。"""

        shipment_id = self._identifier(shipment_id, "shipment_id")
        deadline = self._ts(must_arrive_by, "must_arrive_by")
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "must_arrive_by": deadline}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            if connection.execute("SELECT 1 FROM corridor_shipments WHERE shipment_id=?",
                                  (shipment_id,)).fetchone() is None:
                raise NotFoundError("货物不存在")

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                version = connection.execute(
                    "SELECT COALESCE(MAX(version),0) AS version FROM corridor_deadlines WHERE shipment_id=?",
                    (shipment_id,),
                ).fetchone()["version"] + 1
                connection.execute(
                    "INSERT INTO corridor_deadlines(shipment_id,version,must_arrive_by,published_at,published_by)"
                    " VALUES(?,?,?,?,?)",
                    (shipment_id, version, deadline, now, actor_id),
                )
                append_event(connection, actor_id=actor_id, action="corridor.deadline_revised",
                             resource_type="corridor_shipment", resource_id=shipment_id,
                             detail={"version": version, "must_arrive_by": deadline}, occurred_at=now)
                self._decision(connection, kind="deadline.revised",
                               detail={"version": version, "must_arrive_by": deadline},
                               actor_id=actor_id, shipment_id=shipment_id)
                return "corridor_deadline", f"{shipment_id}/v{version}", {
                    "shipment_id": shipment_id, "version": version}

            return self._idempotent(connection, request_id=request_id,
                                    action="revise_deadline", payload=payload, create=create)

    # ------------------------------------------------------------- 快照读取

    def _version_rows(self, connection, kind: str, at: str) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT v.* FROM corridor_ref_versions v JOIN ("
            "SELECT entity_id, MAX(version) AS version FROM corridor_ref_versions "
            "WHERE kind=? AND published_at<=? GROUP BY entity_id) m "
            "ON v.kind=? AND v.entity_id=m.entity_id AND v.version=m.version ORDER BY v.entity_id",
            (kind, at, kind),
        ).fetchall()
        return [{"entity_id": row["entity_id"], "version": row["version"],
                 "published_at": row["published_at"], "published_by": row["published_by"],
                 "payload": json.loads(row["payload_json"])}
                for row in rows]

    def _active_notice(self, connection, notice_id: str, at: str) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT * FROM corridor_notices WHERE notice_id=? AND created_at<=? "
            "ORDER BY created_at DESC LIMIT 1", (notice_id, at)
        ).fetchone()
        if row is None:
            return None
        recovered_at = row["recovered_at"]
        if row["status"] != "active" or (recovered_at and recovered_at <= at):
            return None
        return {"notice_id": row["notice_id"], "title": row["title"], "severity": row["severity"],
                "detail": row["detail"], "status": "active",
                "impact": json.loads(row["scope_json"])}

    def _shipment(self, connection, shipment_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM corridor_shipments WHERE shipment_id=?",
                                 (shipment_id,)).fetchone()
        if row is None:
            raise NotFoundError("货物不存在")
        return dict(row)

    def _deadline_at(self, connection, shipment_id: str, at: str) -> str | None:
        row = connection.execute(
            "SELECT must_arrive_by FROM corridor_deadlines WHERE shipment_id=? AND published_at<=? "
            "ORDER BY version DESC LIMIT 1", (shipment_id, at)
        ).fetchone()
        return row["must_arrive_by"] if row else None

    def _capacity(self, connection, capacity_id: str, *, strict: bool = True) -> dict[str, Any]:
        """读取能力当前已发布的最新版本；清算费用时允许缺失或停用。"""

        row = connection.execute(
            "SELECT payload_json FROM corridor_ref_versions WHERE kind='capacity' AND entity_id=? "
            "ORDER BY version DESC LIMIT 1", (capacity_id,),
        ).fetchone()
        if row is None:
            if strict:
                raise NotFoundError(f"承运能力 {capacity_id} 不存在")
            return {"capacity_id": capacity_id, "currency": "USD"}
        payload = json.loads(row["payload_json"])
        if strict and payload.get("status", "active") != "active":
            raise ConflictError(f"承运能力 {capacity_id} 已停用")
        return payload

    def _build_context(self, connection, *, shipment_id: str, at: str,
                       notice_id: str | None, exclude_own_plan: str | None = None) -> NetworkContext:
        shipment = self._shipment(connection, shipment_id)
        segment_rows = self._version_rows(connection, "segment", at)
        segments: dict[str, dict[str, Any]] = {}
        for row in segment_rows:
            payload = row["payload"]
            if payload.get("status", "active") == "active":
                segments[payload["segment_id"]] = payload
        capacities = [row["payload"] for row in self._version_rows(connection, "capacity", at)
                      if row["payload"].get("status", "active") == "active"
                      and row["payload"]["segment_id"] in segments]
        rates = [row["payload"] for row in self._version_rows(connection, "contract_rate", at)]
        restrictions = [row["payload"] for row in self._version_rows(connection, "cargo_restriction", at)
                        if row["payload"].get("status", "active") == "active"]
        calendars = {row["entity_id"]: row["payload"]
                     for row in self._version_rows(connection, "border_calendar", at)}
        notice = self._active_notice(connection, notice_id, at) if notice_id else None
        usage: dict[str, float] = {}
        query = ("SELECT capacity_id, SUM(units) AS units, plan_id FROM corridor_holds "
                 "WHERE state IN (?,?) GROUP BY capacity_id, plan_id")
        for hold in connection.execute(query, _OCCUPYING_HOLD_STATES):
            if exclude_own_plan and hold["plan_id"] == exclude_own_plan:
                continue
            usage[hold["capacity_id"]] = usage.get(hold["capacity_id"], 0.0) + float(hold["units"])
        return NetworkContext(
            at=at, shipment=shipment, deadline=self._deadline_at(connection, shipment_id, at),
            segments=segments, capacities=capacities, rates=rates, calendars=calendars,
            restrictions=restrictions, notice=notice, usage=usage)

    # ---------------------------------------------------------------- 基线

    def set_baseline(self, *, request_id: str, actor_id: str, shipment_id: str,
                     path: list[str], depart_at: str) -> dict[str, Any]:
        """登记中断前的既定路线与既订能力（基线）。"""

        if not isinstance(path, list) or not path:
            raise ValidationError("path 必须是非空分段编号数组")
        depart_at = self._ts(depart_at, "depart_at")
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "path": path,
                   "depart_at": depart_at}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            shipment = self._shipment(connection, shipment_id)
            if connection.execute("SELECT 1 FROM corridor_shipment_routes WHERE shipment_id=?",
                                  (shipment_id,)).fetchone():
                raise ConflictError("基线路线已经存在")
            ctx = self._build_context(connection, shipment_id=shipment_id, at=depart_at,
                                      notice_id=None)
            for segment_id in path:
                if segment_id not in ctx.segments:
                    raise ValidationError(f"分段 {segment_id} 在该时点不存在或不可用")
            evaluation = evaluate_path(ctx, path, {"border_crossings": set(), "segments": set(),
                                                   "nodes": set(), "carriers": set()})
            if not evaluation["feasible"]:
                raise ValidationError("基线路线不可行：" + "; ".join(
                    v["message"] for v in evaluation["violations"]))

            def create() -> tuple[str, str, dict[str, Any]]:
                now = self._now()
                plan_id = uuid.uuid4().hex
                explanation = {"violations": [], "reroute_basis": None,
                               "baseline_cost": evaluation["total_cost"]}
                connection.execute(
                    "INSERT INTO corridor_plans(plan_id,evaluation_id,shipment_id,trigger_notice_id,"
                    "path_json,departures_json,eta,total_cost,currency,feasible,explanation_json,"
                    "required_approvals_json,status,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (plan_id, None, shipment_id, None, canonical_json(path),
                     canonical_json(evaluation["legs"]), evaluation["eta"],
                     evaluation["total_cost"], evaluation["currency"], 1,
                     canonical_json(explanation), canonical_json([]), "baseline", actor_id, now),
                )
                self._set_plan_status(connection, plan_id, "baseline", "基线登记")
                for leg in evaluation["legs"]:
                    option = leg["option"]
                    connection.execute(
                        "INSERT INTO corridor_holds(hold_id,plan_id,shipment_id,capacity_id,"
                        "segment_id,units,state,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, plan_id, shipment_id, option["capacity_id"],
                         leg["segment_id"], float(shipment["units"]), "committed", now),
                    )
                connection.execute(
                    "INSERT INTO corridor_shipment_routes(shipment_id,baseline_plan_id,"
                    "effective_plan_id,epoch) VALUES(?,?,?,1)",
                    (shipment_id, plan_id, plan_id),
                )
                append_event(connection, actor_id=actor_id, action="corridor.baseline_set",
                             resource_type="corridor_plan", resource_id=plan_id,
                             detail={"shipment_id": shipment_id, "path": path, "eta": evaluation["eta"],
                                     "total_cost": evaluation["total_cost"]}, occurred_at=now)
                self._decision(connection, kind="baseline.set", detail={"path": path},
                               actor_id=actor_id, shipment_id=shipment_id, plan_id=plan_id)
                return "corridor_plan", plan_id, {"plan_id": plan_id, "status": "baseline",
                                                  "eta": evaluation["eta"]}

            return self._idempotent(connection, request_id=request_id,
                                    action="set_baseline", payload=payload, create=create)

    # ------------------------------------------------------------ 中断评估

    def _load_plan(self, connection, plan_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM corridor_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFoundError("方案不存在")
        return dict(row)

    def _route_row(self, connection, shipment_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM corridor_shipment_routes WHERE shipment_id=?",
                                 (shipment_id,)).fetchone()
        if row is None:
            raise NotFoundError("货物尚未登记基线路线")
        return dict(row)

    def _is_handed_over(self, connection, shipment_id: str, at: str | None = None) -> bool:
        if at is None:
            row = connection.execute(
                "SELECT 1 FROM corridor_checkpoints WHERE shipment_id=? AND event=? LIMIT 1",
                (shipment_id, _HANDOVER_EVENT),
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT 1 FROM corridor_checkpoints WHERE shipment_id=? AND event=? AND at<=? LIMIT 1",
                (shipment_id, _HANDOVER_EVENT, at),
            ).fetchone()
        return row is not None

    def evaluate_disruption(self, *, actor_id: str, notice_id: str,
                            shipment_id: str | None = None) -> dict[str, Any]:
        """针对一条活跃通告，计算受影响货物与可解释候选路径（只读分析，不落业务锁）。"""

        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            at = self._now()
            notice = self._active_notice(connection, notice_id, at)
            if notice is None:
                raise NotFoundError("风险通告不存在或已恢复")
            if shipment_id:
                shipment_rows = [self._shipment(connection, shipment_id)]
            else:
                shipment_rows = [dict(row) for row in
                                 connection.execute("SELECT * FROM corridor_shipments ORDER BY shipment_id")]
            run_id = uuid.uuid4().hex
            results: list[dict[str, Any]] = []
            for shipment in shipment_rows:
                sid = shipment["shipment_id"]
                evaluation_id = uuid.uuid4().hex
                try:
                    route = self._route_row(connection, sid)
                except NotFoundError:
                    continue
                current_plan = self._load_plan(connection, route["effective_plan_id"])
                current_legs = json.loads(current_plan["departures_json"])
                basis = explain_reroute_baseline(current_legs, notice)
                ctx = self._build_context(
                    connection, shipment_id=sid, at=at, notice_id=notice_id,
                    exclude_own_plan=route["effective_plan_id"])
                baseline_summary = {
                    "path": json.loads(current_plan["path_json"]),
                    "legs": current_legs,
                    "total_cost": float(current_plan["total_cost"]),
                    "eta": current_plan["eta"],
                    "currency": current_plan["currency"],
                }
                candidates = enumerate_candidates(ctx)
                notice_impact = {"border_crossings": set(notice["impact"].get("border_crossings", [])),
                                 "segments": set(notice["impact"].get("segments", [])),
                                 "nodes": set(notice["impact"].get("nodes", [])),
                                 "carriers": set(notice["impact"].get("carriers", []))}
                affected = bool(basis["blocked_on_baseline"]) or not evaluate_path(
                    ctx, baseline_summary["path"], notice_impact)["feasible"]
                plan_summaries: list[dict[str, Any]] = []
                for candidate in candidates:
                    plan_id = uuid.uuid4().hex
                    approvals = (required_approval_roles(candidate, baseline_summary, shipment)
                                 if candidate["feasible"] else [])
                    explanation = {**candidate,
                                   "reroute_basis": basis,
                                   "baseline_comparison": {
                                       "path": baseline_summary["path"],
                                       "total_cost": baseline_summary["total_cost"],
                                       "eta": baseline_summary["eta"]}}
                    connection.execute(
                        "INSERT INTO corridor_plans(plan_id,evaluation_id,shipment_id,trigger_notice_id,"
                        "path_json,departures_json,eta,total_cost,currency,feasible,explanation_json,"
                        "required_approvals_json,status,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (plan_id, evaluation_id, sid, notice_id,
                         canonical_json(candidate["path"]), canonical_json(candidate["legs"]),
                         candidate["eta"], candidate["total_cost"], candidate["currency"],
                         1 if candidate["feasible"] else 0, canonical_json(explanation),
                         canonical_json(approvals), "candidate", actor_id, at),
                    )
                    self._set_plan_status(connection, plan_id, "candidate", "中断评估生成候选")
                    plan_summaries.append({
                        "plan_id": plan_id, "feasible": candidate["feasible"],
                        "path": candidate["path"], "eta": candidate["eta"],
                        "total_cost": candidate["total_cost"], "currency": candidate["currency"],
                        "violations": [v["message"] for v in candidate["violations"]],
                        "required_approvals": approvals})
                connection.execute(
                    "INSERT INTO corridor_evaluations(evaluation_id,notice_id,shipment_id,basis_json,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?)",
                    (evaluation_id, notice_id, sid, canonical_json(basis), actor_id, at),
                )
                results.append({"shipment_id": sid, "evaluation_id": evaluation_id,
                                "affected": affected,
                                "current_plan_id": current_plan["plan_id"],
                                "current_eta": current_plan["eta"],
                                "reroute_basis": basis, "candidates": plan_summaries})
            append_event(connection, actor_id=actor_id, action="corridor.disruption_evaluated",
                         resource_type="corridor_evaluation", resource_id=run_id,
                         detail={"notice_id": notice_id,
                                 "affected": [r["shipment_id"] for r in results if r["affected"]],
                                 "shipments": len(results)}, occurred_at=at)
            self._decision(connection, kind="disruption.evaluated",
                           detail={"run_id": run_id, "notice_id": notice_id,
                                   "affected": [r["shipment_id"] for r in results if r["affected"]]},
                           actor_id=actor_id, notice_id=notice_id)
            return {"run_id": run_id, "notice_id": notice_id, "at": at,
                    "shipments": results}

    # ------------------------------------------------------------- 选择方案

    def select_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                    reason: str = "") -> dict[str, Any]:
        """从竞争候选中选定一个；同一货物同时只能有一个待执行方案。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")

            def create() -> tuple[str, str, dict[str, Any]]:
                fresh = self._load_plan(connection, plan_id)
                if fresh["status"] != "candidate":
                    raise ConflictError(f"方案状态为 {fresh['status']}，不能选择")
                if not fresh["feasible"]:
                    raise ValidationError("该候选不可行，不能选择")
                shipment_id = fresh["shipment_id"]
                if self._is_handed_over(connection, shipment_id):
                    raise ConflictError("货物已完成交接，迟到的改道请求被拒绝")
                active = connection.execute(
                    "SELECT plan_id FROM corridor_plans WHERE shipment_id=? "
                    "AND status IN ('selected','switching')", (shipment_id,),
                ).fetchone()
                if active is not None:
                    raise ConflictError(
                        f"货物已有待执行方案 {active['plan_id']}，竞争方案不能同时生效")
                self._set_plan_status(connection, plan_id, "selected",
                                      reason or "运营选定候选方案")
                append_event(connection, actor_id=actor_id, action="corridor.plan_selected",
                             resource_type="corridor_plan", resource_id=plan_id,
                             detail={"shipment_id": shipment_id, "reason": reason},
                             occurred_at=self._now())
                self._decision(connection, kind="plan.selected", detail={"reason": reason},
                               actor_id=actor_id, shipment_id=shipment_id, plan_id=plan_id,
                               notice_id=fresh["trigger_notice_id"])
                return "corridor_plan", plan_id, {"plan_id": plan_id, "status": "selected"}

            return self._idempotent(connection, request_id=request_id,
                                    action="select_plan", payload=payload, create=create)

    # ---------------------------------------------------------------- 审批

    def approve_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                     note: str = "") -> dict[str, Any]:
        """登记一个必需审批；审批角色由改道内容推导。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                plan = self._load_plan(connection, plan_id)
                if plan["status"] not in ("selected", "switching"):
                    raise ConflictError("只有已选定待切换的方案才能审批")
                required = json.loads(plan["required_approvals_json"])
                if actor.role not in required:
                    raise PermissionDenied(
                        f"该方案需要的审批角色为 {required}，当前角色 {actor.role} 不在其中")
                if connection.execute(
                        "SELECT 1 FROM corridor_plan_approvals WHERE plan_id=? AND role=?",
                        (plan_id, actor.role)).fetchone() is not None:
                    return "corridor_approval", f"{plan_id}/{actor.role}", {
                        "plan_id": plan_id, "role": actor.role, "already_granted": True}
                now = self._now()
                connection.execute(
                    "INSERT INTO corridor_plan_approvals(plan_id,role,actor_id,note,at) "
                    "VALUES(?,?,?,?,?)", (plan_id, actor.role, actor_id, note[:500], now),
                )
                append_event(connection, actor_id=actor_id, action="corridor.plan_approved",
                             resource_type="corridor_plan", resource_id=plan_id,
                             detail={"role": actor.role, "note": note}, occurred_at=now)
                self._decision(connection, kind="plan.approved",
                               detail={"role": actor.role, "note": note},
                               actor_id=actor_id, shipment_id=plan["shipment_id"], plan_id=plan_id)
                return "corridor_approval", f"{plan_id}/{actor.role}", {
                    "plan_id": plan_id, "role": actor.role}

            return self._idempotent(connection, request_id=request_id,
                                    action="approve_plan", payload=payload, create=create)

    # ---------------------------------------------------------------- 切换

    def _lock_one_leg(self, *, plan: dict[str, Any], leg: dict[str, Any],
                      old_plan_id: str) -> None:
        """在独立事务里锁定单个分段；BEGIN IMMEDIATE 串行并发写入。"""

        shipment_id = plan["shipment_id"]
        option = leg["option"]
        capacity_id = option["capacity_id"]
        with self.database.transaction(immediate=True) as connection:
            used = 0.0
            rows = connection.execute(
                "SELECT plan_id, SUM(units) AS units FROM corridor_holds "
                "WHERE capacity_id=? AND state IN (?,?) GROUP BY plan_id",
                (capacity_id,) + _OCCUPYING_HOLD_STATES,
            ).fetchall()
            for row in rows:
                # 本货物即将被替换的旧方案占用量在切换中释放，不构成竞争。
                if row["plan_id"] == old_plan_id:
                    continue
                used += float(row["units"])
            capacity = self._capacity(connection, capacity_id)
            total_units = float(capacity["capacity_units"])
            need = float(self._shipment(connection, shipment_id)["units"])
            if total_units - used + 1e-9 < need:
                raise ConflictError(
                    f"分段 {leg['segment_id']} 能力 {capacity_id} 余量不足：可用 "
                    f"{round(total_units - used, 4)}，需要 {need}")
            connection.execute(
                "INSERT INTO corridor_holds(hold_id,plan_id,shipment_id,capacity_id,segment_id,"
                "units,state,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, plan["plan_id"], shipment_id, capacity_id, leg["segment_id"],
                 need, "held", self._now()),
            )

    def _release_attempt(self, *, plan: dict[str, Any]) -> list[str]:
        """切换失败：释放本次已锁定但尚未使用的能力，承诺费保留为已发生费用。"""

        fees: list[str] = []
        with self.database.transaction(immediate=True) as connection:
            now = self._now()
            holds = connection.execute(
                "SELECT * FROM corridor_holds WHERE plan_id=? AND state='held'",
                (plan["plan_id"],),
            ).fetchall()
            for hold in holds:
                capacity = self._capacity(connection, hold["capacity_id"], strict=False)
                fee = float(capacity.get("commitment_fee", 0) or 0)
                connection.execute(
                    "UPDATE corridor_holds SET state='released', released_at=? WHERE hold_id=?",
                    (now, hold["hold_id"]),
                )
                connection.execute(
                    "INSERT INTO corridor_fees(fee_id,plan_id,shipment_id,segment_id,fee_type,"
                    "amount,currency,note,at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, plan["plan_id"], plan["shipment_id"], hold["segment_id"],
                     "commitment_forfeited", fee, capacity.get("currency", "USD"),
                     f"分段锁定失败，释放能力 {hold['capacity_id']}，承诺费不退", now),
                )
                fees.append(hold["capacity_id"])
            self._set_plan_status(connection, plan["plan_id"], "selected", "锁定失败后回到已选定")
        return fees

    def execute_switch(self, *, request_id: str, actor_id: str, plan_id: str) -> dict[str, Any]:
        """锁定全部分段并在审批齐备后原子切换。

        成功结果按 ``request_id`` 幂等；锁定阶段失败不写回执，允许同一请求号重试。
        """

        request_id = self._identifier(request_id, "request_id")
        payload = {"actor_id": actor_id, "plan_id": plan_id}
        payload_hash = digest(payload)
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            receipt = connection.execute(
                "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
            ).fetchone()
            if receipt is not None:
                if receipt["action"] != "execute_switch" or receipt["payload_hash"] != payload_hash:
                    raise ConflictError("request_id 已被不同内容使用")
                return {**json.loads(receipt["response_json"]), "replayed": True}
            plan = self._load_plan(connection, plan_id)
            shipment_id = plan["shipment_id"]
            if self._is_handed_over(connection, shipment_id):
                raise ConflictError("货物已完成交接，迟到的改道请求不能再改变路线")
            if plan["status"] == "switching":
                # 上次尝试在逐段锁定过程中中断：先按部分失败规则释放残留锁定并保留费用
                stale_holds = connection.execute(
                    "SELECT * FROM corridor_holds WHERE plan_id=? AND state='held'",
                    (plan_id,)).fetchall()
                if stale_holds:
                    for hold in stale_holds:
                        capacity = self._capacity(connection, hold["capacity_id"], strict=False)
                        connection.execute(
                            "UPDATE corridor_holds SET state='released', released_at=? WHERE hold_id=?",
                            (self._now(), hold["hold_id"]))
                        connection.execute(
                            "INSERT INTO corridor_fees(fee_id,plan_id,shipment_id,segment_id,fee_type,"
                            "amount,currency,note,at) VALUES(?,?,?,?,?,?,?,?,?)",
                            (uuid.uuid4().hex, plan_id, shipment_id, hold["segment_id"],
                             "commitment_forfeited",
                             float(capacity.get("commitment_fee", 0) or 0),
                             capacity.get("currency", "USD"),
                             f"崩溃恢复：释放残留锁定 {hold['capacity_id']}，承诺费保留",
                             self._now()))
                self._set_plan_status(connection, plan_id, "selected", "崩溃恢复后回到已选定")
                plan = self._load_plan(connection, plan_id)
            if plan["status"] != "selected":
                raise ConflictError(f"方案状态为 {plan['status']}，必须先选定再切换")
            required = json.loads(plan["required_approvals_json"])
            granted = {row["role"] for row in connection.execute(
                "SELECT role FROM corridor_plan_approvals WHERE plan_id=?", (plan_id,))}
            missing = [role for role in required if role not in granted]
            if missing:
                raise PermissionDenied(f"缺少必要审批：{missing}")
            legs = json.loads(plan["departures_json"])
            route = self._route_row(connection, shipment_id)
            old_plan_id = route["effective_plan_id"]
            self._set_plan_status(connection, plan_id, "switching", "开始逐段锁定能力")

        released_capacities: list[str] = []
        failed_leg: str | None = None
        for leg in legs:
            try:
                self._lock_one_leg(plan=plan, leg=leg, old_plan_id=old_plan_id)
            except ConflictError as exc:
                failed_leg = f"{leg['segment_id']}: {exc}"
                break
        if failed_leg is not None:
            released_capacities = self._release_attempt(plan=plan)
            with self.database.transaction(immediate=True) as connection:
                self._decision(connection, kind="switch.lock_failed",
                               detail={"failed_leg": failed_leg,
                                       "released_capacities": released_capacities,
                                       "fees_retained": True},
                               actor_id=actor_id, shipment_id=plan["shipment_id"], plan_id=plan_id,
                               notice_id=plan["trigger_notice_id"])
                append_event(connection, actor_id=actor_id, action="corridor.switch_failed",
                             resource_type="corridor_plan", resource_id=plan_id,
                             detail={"failed_leg": failed_leg,
                                     "released_capacities": released_capacities},
                             occurred_at=self._now())
            raise ConflictError(f"能力锁定失败（{failed_leg}）；已释放 {len(released_capacities)} "
                                f"个未使用锁定，承诺费保留在费用台账中")

        # 全部锁定成功：原子提交切换。
        with self.database.transaction(immediate=True) as connection:
            now = self._now()
            connection.execute(
                "UPDATE corridor_holds SET state='committed' WHERE plan_id=? AND state='held'",
                (plan_id,),
            )
            new_hold_rows = connection.execute(
                "SELECT capacity_id FROM corridor_holds WHERE plan_id=? AND state='committed'",
                (plan_id,),
            ).fetchall()
            new_capacities = {row["capacity_id"] for row in new_hold_rows}
            old_holds = connection.execute(
                "SELECT * FROM corridor_holds WHERE plan_id=? AND state IN (?,?)",
                (old_plan_id,) + _OCCUPYING_HOLD_STATES,
            ).fetchall()
            sunk_total = 0.0
            currency = plan["currency"]
            for hold in old_holds:
                if hold["capacity_id"] in new_capacities:
                    connection.execute(
                        "UPDATE corridor_holds SET state='replaced_relocked', released_at=? WHERE hold_id=?",
                        (now, hold["hold_id"]),
                    )
                    continue
                connection.execute(
                    "UPDATE corridor_holds SET state='released_after_switch', released_at=? WHERE hold_id=?",
                    (now, hold["hold_id"]),
                )
                capacity = self._capacity(connection, hold["capacity_id"], strict=False)
                sunk = float(capacity.get("commitment_fee", 0) or 0)
                if sunk > 0:
                    sunk_total += sunk
                    connection.execute(
                        "INSERT INTO corridor_fees(fee_id,plan_id,shipment_id,segment_id,fee_type,"
                        "amount,currency,note,at) VALUES(?,?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, plan_id, shipment_id, hold["segment_id"],
                         "sunk_commitment", sunk, capacity.get("currency", currency),
                         f"切换后旧路线能力 {hold['capacity_id']} 未使用，承诺费沉没", now),
                    )
            connection.execute(
                "UPDATE corridor_plans SET status='superseded' WHERE plan_id=?", (old_plan_id,),
            )
            self._set_plan_status(connection, old_plan_id, "superseded", "被新方案替代")
            # 同一货物的其他竞争候选一律作废，保证只有一个方案生效。
            connection.execute(
                "UPDATE corridor_plans SET status='obsolete' WHERE shipment_id=? AND plan_id!=? "
                "AND status IN ('candidate','selected','switching')",
                (shipment_id, plan_id),
            )
            obsolete = connection.execute(
                "SELECT changes() AS c",
            ).fetchone()["c"]
            connection.execute(
                "UPDATE corridor_plans SET status='effective' WHERE plan_id=?", (plan_id,),
            )
            self._set_plan_status(connection, plan_id, "effective", "全部分段锁定且审批齐备，切换生效")
            connection.execute(
                "UPDATE corridor_shipment_routes SET effective_plan_id=?, epoch=epoch+1, switched_at=? "
                "WHERE shipment_id=?", (plan_id, now, shipment_id),
            )
            append_event(connection, actor_id=actor_id, action="corridor.switched",
                         resource_type="corridor_plan", resource_id=plan_id,
                         detail={"shipment_id": shipment_id, "old_plan_id": old_plan_id,
                                 "obsolete_competing_plans": obsolete, "sunk_commitment": sunk_total,
                                 "eta": plan["eta"], "total_cost": plan["total_cost"]},
                         occurred_at=now)
            self._decision(connection, kind="switch.executed",
                           detail={"old_plan_id": old_plan_id, "obsolete_competing_plans": obsolete,
                                   "sunk_commitment": sunk_total, "path": json.loads(
                                       plan["path_json"]), "eta": plan["eta"]},
                           actor_id=actor_id, shipment_id=shipment_id, plan_id=plan_id,
                           notice_id=plan["trigger_notice_id"])
            response = {"plan_id": plan_id, "shipment_id": shipment_id, "status": "effective",
                        "old_plan_id": old_plan_id, "released_capacities_on_failure": [],
                        "sunk_commitment": round(sunk_total, 2), "eta": plan["eta"],
                        "total_cost": plan["total_cost"], "currency": plan["currency"], "at": now}
            connection.execute(
                "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,"
                "resource_id,response_json,created_at) VALUES(?,?,?,?,?,?,?)",
                (request_id, "execute_switch", payload_hash, "corridor_plan", plan_id,
                 canonical_json(response), now),
            )
            return {**response, "request_id": request_id,
                    "resource_type": "corridor_plan", "replayed": False}

    # ---------------------------------------------------------------- 交接

    def record_checkpoint(self, *, request_id: str, actor_id: str, shipment_id: str,
                          node_id: str, event: str, at: str | None = None) -> dict[str, Any]:
        """登记运输节点事件；``handed_over`` 后路线即冻结。"""

        if event not in ("departed", "arrived_at_node", "handed_over"):
            raise ValidationError("event 不被支持")
        if not str(node_id).strip():
            raise ValidationError("node_id 不能为空")
        shipment_id = self._identifier(shipment_id, "shipment_id")
        when = self._ts(at or self._now(), "at")
        payload = {"actor_id": actor_id, "shipment_id": shipment_id, "node_id": node_id,
                   "event": event, "at": when}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")

            def create() -> tuple[str, str, dict[str, Any]]:
                shipment = self._shipment(connection, shipment_id)
                if event == _HANDOVER_EVENT and node_id != shipment["destination_node"]:
                    raise ValidationError("交接只能在目的地节点完成")
                if event == _HANDOVER_EVENT and self._is_handed_over(connection, shipment_id):
                    raise ConflictError("货物已经交接，不能重复交接")
                checkpoint_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO corridor_checkpoints(checkpoint_id,shipment_id,node_id,event,at,recorded_by)"
                    " VALUES(?,?,?,?,?,?)",
                    (checkpoint_id, shipment_id, node_id, event, when, actor_id),
                )
                append_event(connection, actor_id=actor_id, action=f"corridor.checkpoint.{event}",
                             resource_type="corridor_shipment", resource_id=shipment_id,
                             detail={"node_id": node_id, "at": when}, occurred_at=when)
                self._decision(connection, kind=f"checkpoint.{event}",
                               detail={"node_id": node_id}, actor_id=actor_id,
                               shipment_id=shipment_id)
                return "corridor_checkpoint", checkpoint_id, {
                    "checkpoint_id": checkpoint_id, "event": event}

            return self._idempotent(connection, request_id=request_id,
                                    action="record_checkpoint", payload=payload, create=create)

    # ---------------------------------------------------------------- 视图

    def _status_at(self, connection, plan_id: str, at: str | None) -> str:
        """返回方案在某时点的状态；不传时点时返回当前状态。"""

        if at is None:
            return self._load_plan(connection, plan_id)["status"]
        row = connection.execute(
            "SELECT status FROM corridor_plan_status_history WHERE plan_id=? AND at<=? "
            "ORDER BY at DESC, rowid DESC LIMIT 1", (plan_id, at),
        ).fetchone()
        return row["status"] if row else "candidate"

    @staticmethod
    def _plan_payload(plan_row: dict[str, Any], status_override: str | None = None) -> dict[str, Any]:
        return {
            "plan_id": plan_row["plan_id"], "shipment_id": plan_row["shipment_id"],
            "status": status_override or plan_row["status"], "feasible": bool(plan_row["feasible"]),
            "trigger_notice_id": plan_row["trigger_notice_id"],
            "evaluation_id": plan_row["evaluation_id"],
            "path": json.loads(plan_row["path_json"]),
            "legs": json.loads(plan_row["departures_json"]),
            "eta": plan_row["eta"], "total_cost": plan_row["total_cost"],
            "currency": plan_row["currency"],
            "required_approvals": json.loads(plan_row["required_approvals_json"]),
            "explanation": json.loads(plan_row["explanation_json"]),
            "created_at": plan_row["created_at"],
        }

    def _effective_plan_at(self, connection, shipment_id: str, at: str | None) -> dict[str, Any] | None:
        route = connection.execute("SELECT * FROM corridor_shipment_routes WHERE shipment_id=?",
                                   (shipment_id,)).fetchone()
        if route is None:
            return None
        if at is None:
            return self._plan_payload(self._load_plan(connection, route["effective_plan_id"]))
        row = connection.execute(
            "SELECT plan_id FROM corridor_plan_status_history WHERE status='effective' AND at<=? "
            "AND plan_id IN (SELECT plan_id FROM corridor_plans WHERE shipment_id=?) "
            "ORDER BY at DESC, rowid DESC LIMIT 1", (at, shipment_id),
        ).fetchone()
        plan_id = row["plan_id"] if row else route["baseline_plan_id"]
        plan_row = self._load_plan(connection, plan_id)
        if plan_row["created_at"] > at:
            return None
        return self._plan_payload(plan_row, self._status_at(connection, plan_id, at))

    def ops_view(self, *, actor_id: str, shipment_id: str | None = None,
                 as_of: str | None = None) -> dict[str, Any]:
        """运营视图：当前责任方、预计到达、改道依据。"""

        at = self._ts(as_of, "as_of") if as_of else self._now()
        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator", "reviewer", "safety", "auditor")
            rows = [self._shipment(connection, shipment_id)] if shipment_id else [
                dict(row) for row in connection.execute(
                    "SELECT * FROM corridor_shipments ORDER BY shipment_id")]
            items: list[dict[str, Any]] = []
            for shipment in rows:
                sid = shipment["shipment_id"]
                plan = self._effective_plan_at(connection, sid, at)
                if plan is None:
                    continue
                checkpoint_row = connection.execute(
                    "SELECT * FROM corridor_checkpoints WHERE shipment_id=? AND at<=? "
                    "ORDER BY at DESC, checkpoint_id DESC LIMIT 1", (sid, at),
                ).fetchone()
                handed = self._is_handed_over(connection, sid, at)
                responsible = None
                active_leg = None
                if not handed:
                    for leg in plan["legs"]:
                        if leg["depart_at"] <= at < leg["arrive_at"]:
                            active_leg = leg
                            break
                    if active_leg is not None:
                        responsible = {"party_type": "carrier",
                                       "carrier_id": active_leg["option"]["carrier_id"],
                                       "carrier_name": active_leg["option"]["carrier_name"],
                                       "segment_id": active_leg["segment_id"]}
                    elif plan["legs"] and at < plan["legs"][0]["depart_at"]:
                        first = plan["legs"][0]["option"]
                        responsible = {"party_type": "carrier_pending_departure",
                                       "carrier_id": first["carrier_id"],
                                       "carrier_name": first["carrier_name"],
                                       "segment_id": plan["legs"][0]["segment_id"]}
                    elif plan["legs"]:
                        last = plan["legs"][-1]["option"]
                        responsible = {"party_type": "carrier_awaiting_handover",
                                       "carrier_id": last["carrier_id"],
                                       "carrier_name": last["carrier_name"],
                                       "segment_id": plan["legs"][-1]["segment_id"]}
                items.append({
                    "shipment_id": sid, "customer_ref": shipment["customer_ref"],
                    "plan_id": plan["plan_id"], "plan_status": plan["status"],
                    "path": plan["path"], "eta": plan["eta"],
                    "must_arrive_by": self._deadline_at(connection, sid, at),
                    "on_time": (self._deadline_at(connection, sid, at) is None
                                or plan["eta"] <= self._deadline_at(connection, sid, at)),
                    "responsible_party": responsible,
                    "active_segment": active_leg["segment_id"] if active_leg else None,
                    "last_checkpoint": ({"node_id": checkpoint_row["node_id"],
                                         "event": checkpoint_row["event"], "at": checkpoint_row["at"]}
                                        if checkpoint_row else None),
                    "handed_over": handed,
                    "reroute_basis": plan["explanation"].get("reroute_basis"),
                    "trigger_notice_id": plan["trigger_notice_id"] if plan["status"] != "baseline" else None,
                })
            return {"view": "operations", "as_of": at, "items": items}

    def finance_view(self, *, actor_id: str, shipment_id: str | None = None,
                     as_of: str | None = None) -> dict[str, Any]:
        """财务视图：基线与当前方案的费用差异、沉没与罚没费用。"""

        at = self._ts(as_of, "as_of") if as_of else self._now()
        with self.database.transaction() as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "finance", "reviewer", "auditor")
            rows = [self._shipment(connection, shipment_id)] if shipment_id else [
                dict(row) for row in connection.execute(
                    "SELECT * FROM corridor_shipments ORDER BY shipment_id")]
            items: list[dict[str, Any]] = []
            for shipment in rows:
                sid = shipment["shipment_id"]
                route = connection.execute("SELECT * FROM corridor_shipment_routes WHERE shipment_id=?",
                                           (sid,)).fetchone()
                if route is None:
                    continue
                baseline = self._plan_payload(self._load_plan(connection, route["baseline_plan_id"]))
                if baseline["created_at"] > at:
                    continue
                current = self._effective_plan_at(connection, sid, at) or baseline
                fee_rows = connection.execute(
                    "SELECT * FROM corridor_fees WHERE shipment_id=? AND at<=? ORDER BY at", (sid, at)
                ).fetchall()
                fees = [{"fee_id": row["fee_id"], "plan_id": row["plan_id"],
                         "segment_id": row["segment_id"], "fee_type": row["fee_type"],
                         "amount": row["amount"], "currency": row["currency"],
                         "note": row["note"], "at": row["at"]} for row in fee_rows]
                sunk = sum(row["amount"] for row in fee_rows)
                delta = round(float(current["total_cost"]) - float(baseline["total_cost"]), 2)
                base_cost = float(baseline["total_cost"])
                items.append({
                    "shipment_id": sid, "customer_ref": shipment["customer_ref"],
                    "currency": current["currency"],
                    "baseline_plan_id": baseline["plan_id"],
                    "effective_plan_id": current["plan_id"],
                    "baseline_cost": baseline["total_cost"],
                    "effective_cost": current["total_cost"],
                    "cost_delta": delta,
                    "cost_delta_ratio": round(delta / base_cost, 4) if base_cost else None,
                    "incurred_fees_total": round(sunk, 2),
                    "incurred_fees": fees,
                    "switched": current["plan_id"] != baseline["plan_id"],
                })
            return {"view": "finance", "as_of": at, "items": items}

    def list_plans(self, *, actor_id: str, shipment_id: str | None = None,
                   evaluation_id: str | None = None) -> dict[str, Any]:
        query = "SELECT * FROM corridor_plans WHERE 1=1"
        parameters: list[Any] = []
        if shipment_id:
            query += " AND shipment_id=?"
            parameters.append(shipment_id)
        if evaluation_id:
            query += " AND evaluation_id=?"
            parameters.append(evaluation_id)
        query += " ORDER BY created_at, plan_id"
        with self.database.transaction() as connection:
            self._actor(connection, actor_id)
            rows = connection.execute(query, parameters).fetchall()
            return {"items": [self._plan_payload(dict(row)) for row in rows]}

    def history_snapshot(self, *, actor_id: str, at: str) -> dict[str, Any]:
        """复原任一时点：当时有效的资料版本、通告、货物、方案状态与决定记录。"""

        at = self._ts(at, "at")
        with self.database.transaction() as connection:
            self._actor(connection, actor_id)

            def versions(kind: str) -> list[dict[str, Any]]:
                return [{"entity_id": row["entity_id"], "version": row["version"],
                         "published_at": row["published_at"], "published_by": row["published_by"],
                         "payload": row["payload"]}
                        for row in self._version_rows(connection, kind, at)]

            notices = []
            for row in connection.execute("SELECT * FROM corridor_notices WHERE created_at<=? ORDER BY created_at",
                                          (at,)):
                recovered = bool(row["recovered_at"] and row["recovered_at"] <= at)
                notices.append({"notice_id": row["notice_id"], "title": row["title"],
                                "severity": row["severity"],
                                "active": not recovered,
                                "impact": json.loads(row["scope_json"]),
                                "created_at": row["created_at"],
                                "recovered_at": None if not recovered else row["recovered_at"]})
            shipments = []
            for row in connection.execute("SELECT * FROM corridor_shipments WHERE created_at<=? ORDER BY shipment_id",
                                          (at,)):
                sid = row["shipment_id"]
                plan = self._effective_plan_at(connection, sid, at)
                holds = []
                for hold in connection.execute(
                        "SELECT * FROM corridor_holds WHERE shipment_id=? AND created_at<=? ORDER BY created_at",
                        (sid, at)):
                    active_hold = hold["released_at"] is None or hold["released_at"] > at
                    holds.append({"hold_id": hold["hold_id"], "plan_id": hold["plan_id"],
                                  "capacity_id": hold["capacity_id"], "segment_id": hold["segment_id"],
                                  "units": hold["units"],
                                  "state": hold["state"] if active_hold else "released",
                                  "active_at_point": bool(active_hold)})
                shipments.append({
                    "shipment_id": sid, "cargo_type": row["cargo_type"],
                    "hazmat_class": row["hazmat_class"], "units": row["units"],
                    "origin_node": row["origin_node"], "destination_node": row["destination_node"],
                    "customer_ref": row["customer_ref"],
                    "must_arrive_by": self._deadline_at(connection, sid, at),
                    "effective_plan": ({"plan_id": plan["plan_id"], "status": plan["status"],
                                        "path": plan["path"], "eta": plan["eta"],
                                        "total_cost": plan["total_cost"]} if plan else None),
                    "holds_at_point": holds,
                    "checkpoints": [{"node_id": c["node_id"], "event": c["event"], "at": c["at"]}
                                    for c in connection.execute(
                                        "SELECT * FROM corridor_checkpoints WHERE shipment_id=? AND at<=? "
                                        "ORDER BY at", (sid, at))],
                })
            decisions = [{"decision_id": row["decision_id"], "shipment_id": row["shipment_id"],
                          "plan_id": row["plan_id"], "notice_id": row["notice_id"],
                          "kind": row["kind"], "detail": json.loads(row["detail_json"]),
                          "actor_id": row["actor_id"], "at": row["at"]}
                         for row in connection.execute(
                             "SELECT * FROM corridor_decisions WHERE at<=? ORDER BY at", (at,))]
            plans = []
            for row in connection.execute("SELECT * FROM corridor_plans WHERE created_at<=? ORDER BY created_at",
                                          (at,)):
                status_row = connection.execute(
                    "SELECT status,at FROM corridor_plan_status_history WHERE plan_id=? AND at<=? "
                    "ORDER BY at DESC, rowid DESC LIMIT 1", (row["plan_id"], at)).fetchone()
                approvals = [{"role": a["role"], "actor_id": a["actor_id"],
                              "note": a["note"], "at": a["at"]}
                             for a in connection.execute(
                                 "SELECT * FROM corridor_plan_approvals WHERE plan_id=? AND at<=? "
                                 "ORDER BY at", (row["plan_id"], at))]
                plans.append({"plan_id": row["plan_id"], "shipment_id": row["shipment_id"],
                              "status": status_row["status"] if status_row else row["status"],
                              "trigger_notice_id": row["trigger_notice_id"],
                              "feasible": bool(row["feasible"]),
                              "required_approvals": json.loads(row["required_approvals_json"]),
                              "approvals": approvals,
                              "path": json.loads(row["path_json"]),
                              "eta": row["eta"], "total_cost": row["total_cost"],
                              "created_at": row["created_at"]})
            return {"as_of": at,
                    "references": {kind: versions(kind) for kind in sorted(REF_KINDS)},
                    "notices": notices, "shipments": shipments, "plans": plans,
                    "decisions": decisions}
