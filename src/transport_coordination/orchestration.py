"""国际通道韧性编排服务。

在基础登记服务之上实现：版本化通道资料发布、在途货物登记、中断影响识别、
可解释候选路径生成、全分段锁定与审批 Saga、部分失败补偿、费用台账、
交接保护与迟到恢复处理，以及运营/财务双视图和任一时点复原。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from .audit import append_event, canonical_json, digest
from .base import ServiceBase
from .errors import (ApprovalRequired, ConflictError, LockFailed, NotFoundError,
                     ValidationError)
from .planner import plan_candidates
from .reference import ENTITY_TYPES, REQUIRED_FIELDS, active_version, build_snapshot

#: 方案的终态；终态方案释放每件货物“唯一生效方案”的唯一索引。
TERMINAL_STATUSES = frozenset({"committed", "failed", "cancelled", "expired"})
#: 已完成物理交接、任何迟到消息都不能再拉回的货物状态。
HANDED_STATES = frozenset({"handed_over", "delivered"})
#: 各审批键允许的角色。
APPROVER_ROLES = {
    "operations_duty": ("admin", "operator"),
    "customer_window": ("admin", "reviewer"),
    "border_authority": ("admin", "reviewer"),
    "finance": ("admin", "finance"),
}


class OrchestrationService(ServiceBase):
    """国际通道中断韧性编排的领域服务。"""

    # ------------------------------------------------------------------ 资料发布

    def publish_reference(self, *, request_id: str, actor_id: str, entity_type: str,
                          entity_id: str, payload: dict[str, Any],
                          valid_from: str | None = None):
        """发布一类通道资料的新版本，旧版本在新版本生效时闭合。"""

        if entity_type not in ENTITY_TYPES:
            raise ValidationError("entity_type 不受编排服务治理")
        payload_out = dict(payload)
        payload = {"actor_id": actor_id, "entity_type": entity_type,
                   "entity_id": entity_id, "payload": payload_out,
                   "valid_from": valid_from}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            allowed = ("admin", "operator")
            if entity_type == "contract_rate":
                allowed = ("admin", "operator", "finance")
            self._require(actor, *allowed)
            entity_id = self._identifier(entity_id, "entity_id")
            if not isinstance(payload_out, dict) or not payload_out:
                raise ValidationError("payload 必须是非空对象")
            for field in REQUIRED_FIELDS[entity_type]:
                if field not in payload_out:
                    raise ValidationError(f"{entity_type} 缺少必填字段 {field}")
            valid_from = valid_from or self._now()
            self._timestamp(valid_from, "valid_from")
            latest = connection.execute(
                "SELECT * FROM ref_versions WHERE entity_type=? AND entity_id=? "
                "ORDER BY version DESC LIMIT 1", (entity_type, entity_id)).fetchone()
            if latest is not None:
                if valid_from < latest["valid_from"]:
                    raise ValidationError("新版本生效时间不能早于当前版本")
                if latest["payload_json"] == canonical_json(payload_out):
                    from .models import WriteReceipt
                    return WriteReceipt(request_id, entity_type, entity_id, True)
                if latest["superseded_at"] is None:
                    connection.execute(
                        "UPDATE ref_versions SET superseded_at=? "
                        "WHERE entity_type=? AND entity_id=? AND version=?",
                        (valid_from, entity_type, entity_id, latest["version"]))
            version = (latest["version"] + 1) if latest else 1

            def create() -> tuple[str, str, dict[str, Any]]:
                connection.execute(
                    "INSERT INTO ref_versions(entity_type,entity_id,version,payload_json,"
                    "valid_from,superseded_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (entity_type, entity_id, version, canonical_json(payload_out),
                     valid_from, None, actor_id, self._now()))
                append_event(connection, actor_id=actor_id, action="reference.published",
                             resource_type=entity_type, resource_id=entity_id,
                             detail={"version": version, "valid_from": valid_from,
                                     "payload_hash": digest(payload_out)},
                             occurred_at=self._now())
                return entity_type, entity_id, {"entity_id": entity_id, "version": version}

            return self._idempotent(connection, request_id=request_id,
                                    action="publish_reference", payload=payload, create=create)

    def get_reference(self, entity_type: str, entity_id: str, at: str | None = None):
        """读取某实体当前或指定时点的生效版本。"""

        result = active_version(self.database.connection, entity_type, entity_id,
                                at or self._now())
        if result is None:
            raise NotFoundError("该时点没有生效版本")
        return {"entity_type": entity_type, "entity_id": entity_id, **result}

    # ------------------------------------------------------------------ 货物登记

    def register_shipment(self, *, request_id: str, actor_id: str, cargo_id: str,
                          customer_id: str, origin: str, destination: str,
                          tags: list[str], weight: float, legs: list[dict[str, Any]]):
        """登记货物及其原始多分段预订，立即占用对应班期能力。"""

        payload = {"actor_id": actor_id, "cargo_id": cargo_id, "customer_id": customer_id,
                   "origin": origin, "destination": destination, "tags": tags,
                   "weight": weight, "legs": legs}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            request_id, replay = self._replay_receipt(
                connection, request_id, "register_shipment", payload)
            if replay:
                return replay
            cargo_id = self._identifier(cargo_id, "cargo_id")
            customer_id = self._identifier(customer_id, "customer_id")
            origin = self._identifier(origin, "origin")
            destination = self._identifier(destination, "destination")
            if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
                raise ValidationError("tags 必须是字符串数组")
            if not isinstance(weight, (int, float)) or weight <= 0:
                raise ValidationError("weight 必须是正数")
            legs = self._validated_legs(connection, legs, origin, destination,
                                        {"cargo_id": cargo_id, "origin": origin,
                                         "destination": destination, "tags": tags,
                                         "weight": float(weight)})
            now = self._now()

            def create() -> tuple[str, str, dict[str, Any]]:
                if connection.execute("SELECT 1 FROM shipments WHERE cargo_id=?",
                                      (cargo_id,)).fetchone():
                    raise ConflictError("货物编号已经存在")
                first_carrier = legs[0]["carrier_name"]
                eta = legs[-1]["arrives_at"]
                connection.execute(
                    "INSERT INTO shipments(cargo_id,customer_id,origin,destination,"
                    "cargo_tags_json,weight,state,responsible_party,eta,active_plan_id,"
                    "created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (cargo_id, customer_id, origin, destination, canonical_json(tags),
                     float(weight), "booked", first_carrier, eta, None, actor_id, now, now))
                for leg in legs:
                    leg_id = uuid.uuid4().hex
                    leg_status = leg.get("status", "booked")
                    # 已实际发运的航段能力视为已消费，不能再被释放或重复预订。
                    hold_status = "consumed" if leg_status in ("departed", "in_transit") else "booked"
                    connection.execute(
                        "INSERT INTO shipment_legs(leg_id,cargo_id,generation,seq,segment_id,"
                        "capacity_id,carrier_id,carrier_name,departs_at,arrives_at,booked_cost,"
                        "currency,source_plan_id,status,created_at,released_at) "
                        "VALUES(?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (leg_id, cargo_id, leg["seq"], leg["segment_id"], leg["capacity_id"],
                         leg["carrier_id"], leg["carrier_name"], leg["departs_at"],
                         leg["arrives_at"], leg["booked_cost"], leg["currency"],
                         None, leg_status, now, None))
                    connection.execute(
                        "INSERT INTO capacity_holds(hold_id,cargo_id,plan_id,capacity_id,"
                        "segment_id,slots,kind,status,departs_at,locked_at,consumed_at,"
                        "released_at,release_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, cargo_id, None, leg["capacity_id"],
                         leg["segment_id"], 1, "booking", hold_status,
                         leg["departs_at"], now, now if hold_status == "consumed" else None,
                         None, None))
                    connection.execute(
                        "INSERT INTO cost_entries(entry_id,cargo_id,plan_id,category,amount,"
                        "currency,reason,occurred_at,created_by) VALUES(?,?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, cargo_id, None, "booking", leg["booked_cost"],
                         leg["currency"], f"原始预订 {leg['segment_id']}", now, actor_id))
                self._journal(connection, cargo_id, "shipment.registered",
                              {"state": "booked", "responsible_party": first_carrier,
                               "eta": eta, "customer_id": customer_id}, now)
                append_event(connection, actor_id=actor_id, action="shipment.registered",
                             resource_type="shipment", resource_id=cargo_id,
                             detail={"legs": len(legs), "eta": eta}, occurred_at=now)
                return "shipment", cargo_id, {"cargo_id": cargo_id, "eta": eta}

            return self._idempotent(connection, request_id=request_id,
                                    action="register_shipment", payload=payload, create=create)

    def _validated_legs(self, connection, legs: list[dict[str, Any]], origin: str,
                        destination: str, cargo: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(legs, list) or not legs:
            raise ValidationError("legs 必须是非空数组")
        snapshot = build_snapshot(connection, self._now())
        normalized: list[dict[str, Any]] = []
        previous_node = origin
        previous_arrival = ""
        for index, raw in enumerate(legs, start=1):
            segment_id = self._identifier(raw["segment_id"], f"legs[{index}].segment_id")
            segment = snapshot["segments"].get(segment_id)
            if segment is None:
                raise ValidationError(f"分段 {segment_id} 没有生效版本")
            if segment["payload"]["origin"] != previous_node:
                raise ValidationError(f"分段 {segment_id} 与前段不衔接")
            previous_node = segment["payload"]["destination"]
            capacity_id = raw.get("capacity_id") or segment["payload"].get("capacity_id")
            capacity = snapshot["capacities"].get(capacity_id)
            if capacity is None:
                raise ValidationError(f"分段 {segment_id} 没有生效的承运能力 {capacity_id}")
            departs_at = raw["departs_at"]
            arrives_at = raw["arrives_at"]
            self._timestamp(departs_at, "departs_at")
            self._timestamp(arrives_at, "arrives_at")
            if not (departs_at < arrives_at):
                raise ValidationError("到达时间必须晚于发车时间")
            if previous_arrival and departs_at < previous_arrival:
                raise ValidationError(f"分段 {segment_id} 发车早于前段到达")
            previous_arrival = arrives_at
            departure = next((item for item in capacity["payload"]["departures"]
                              if item["departs_at"] == departs_at
                              and item["arrives_at"] == arrives_at), None)
            if departure is None:
                raise ValidationError(f"能力 {capacity_id} 在 {departs_at} 无对应班期")
            occupied = self._occupied(connection, capacity_id, departs_at)
            if occupied + 1 > departure["slots"]:
                raise ConflictError(f"能力 {capacity_id}@{departs_at} 余位不足")
            for restriction in snapshot["restrictions"]:
                from .planner import _restriction_matches  # 局部复用规划器规则
                if not _restriction_matches(restriction, cargo):
                    continue
                forbidden = segment_id in restriction["payload"].get("forbidden_segments", ()) \
                    or segment["payload"]["mode"] in restriction["payload"].get("forbidden_modes", ())
                if forbidden:
                    raise ValidationError(f"货物限制 {restriction['entity_id']} 禁止该预订")
            rate = next((item["payload"] for item in snapshot["rates"]
                         if item["payload"]["carrier_id"] == capacity["payload"]["carrier_id"]), {})
            cost = raw.get("booked_cost")
            if cost is None:
                cost = rate.get("segment_rates", {}).get(segment_id, departure.get("base_cost", 0))
            status = raw.get("status", "booked")
            if status not in ("booked", "departed", "in_transit"):
                raise ValidationError("leg 状态只能是 booked/departed/in_transit")
            normalized.append({
                "seq": index, "segment_id": segment_id,
                "capacity_id": capacity_id,
                "carrier_id": capacity["payload"]["carrier_id"],
                "carrier_name": capacity["payload"]["carrier_name"],
                "departs_at": departs_at, "arrives_at": arrives_at,
                "booked_cost": round(float(cost), 2),
                "currency": rate.get("currency", raw.get("currency", "CNY")),
                "status": status,
            })
        if previous_node != destination:
            raise ValidationError("行程终点必须与货物目的地一致")
        return normalized

    # ------------------------------------------------------------------ 中断检测

    def detect_disruption(self, *, actor_id: str, advisory_id: str) -> dict[str, Any]:
        """依据生效风险通告扫描在途货物，生成受影响清单（重复调用幂等）。"""

        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator", "reviewer")
            now = self._now()
            version = active_version(connection, "advisory", advisory_id, now)
            if version is None:
                raise NotFoundError("风险通告不存在或未生效")
            payload = version["payload"]
            affected = payload.get("affected", {})
            window = payload.get("window", {})
            impacted: list[dict[str, Any]] = []
            cargo_rows = connection.execute(
                "SELECT * FROM shipments WHERE state IN ('booked','switched')").fetchall()
            for cargo_row in cargo_rows:
                legs = self._current_legs(connection, cargo_row["cargo_id"])
                for leg in legs:
                    hit = leg["segment_id"] in affected.get("segments", []) or \
                        leg["capacity_id"] in affected.get("capacities", [])
                    in_window = window.get("from", "") <= leg["departs_at"] <= window.get("to", "")
                    if not (hit and in_window):
                        continue
                    impact_id = uuid.uuid4().hex
                    cursor = connection.execute(
                        "INSERT OR IGNORE INTO disruption_impacts(impact_id,advisory_id,cargo_id,"
                        "segment_id,capacity_id,departs_at,state_at_impact,handed,"
                        "recovery_action,detected_at) VALUES(?,?,?,?,?,?,?,0,NULL,?)",
                        (impact_id, advisory_id, cargo_row["cargo_id"], leg["segment_id"],
                         leg["capacity_id"], leg["departs_at"], cargo_row["state"], now))
                    if cursor.rowcount:
                        impacted.append({"cargo_id": cargo_row["cargo_id"],
                                         "segment_id": leg["segment_id"],
                                         "capacity_id": leg["capacity_id"],
                                         "departs_at": leg["departs_at"]})
                        self._journal(connection, cargo_row["cargo_id"], "disruption.detected",
                                      {"advisory_id": advisory_id,
                                       "segment_id": leg["segment_id"]}, now)
            if impacted:
                append_event(connection, actor_id=actor_id, action="disruption.detected",
                             resource_type="advisory", resource_id=advisory_id,
                             detail={"impacted": impacted, "version": version["version"]},
                             occurred_at=now)
            return {"advisory_id": advisory_id, "version": version["version"],
                    "impacted": impacted, "total": len(impacted)}

    def list_impacts(self, advisory_id: str) -> list[dict[str, Any]]:
        rows = self.database.connection.execute(
            "SELECT * FROM disruption_impacts WHERE advisory_id=? ORDER BY detected_at, cargo_id",
            (advisory_id,)).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------ 方案生成

    def create_plan(self, *, request_id: str, actor_id: str, cargo_id: str,
                    advisory_id: str | None = None, candidate_index: int = 0):
        """基于当前快照为货物生成可解释改道方案（此时尚未锁定任何能力）。"""

        payload = {"actor_id": actor_id, "cargo_id": cargo_id,
                   "advisory_id": advisory_id, "candidate_index": candidate_index}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            request_id, replay = self._replay_receipt(
                connection, request_id, "create_plan", payload)
            if replay:
                return replay
            cargo_id = self._identifier(cargo_id, "cargo_id")
            cargo_row = self._cargo(connection, cargo_id)
            now = self._now()
            if cargo_row["state"] in HANDED_STATES:
                raise ConflictError("货物已完成交接，不能再生成改道方案")
            active = connection.execute(
                "SELECT plan_id FROM plans WHERE cargo_id=? AND terminal=0",
                (cargo_id,)).fetchone()
            if active:
                raise ConflictError("同一货物只能有一个生效中的竞争方案")
            if advisory_id is not None and active_version(
                    connection, "advisory", advisory_id, now) is None:
                raise NotFoundError("风险通告不存在")
            snapshot = build_snapshot(connection, now)
            result = plan_candidates(snapshot, self._cargo_view(cargo_row),
                                     self._holds(connection),
                                     self._current_legs(connection, cargo_id))
            if not result["feasible"]:
                raise ConflictError("没有同时满足交付窗与通行限制的候选路径")
            if candidate_index < 0 or candidate_index >= len(result["feasible"]):
                raise ValidationError("candidate_index 超出可行方案范围")
            candidate = result["feasible"][candidate_index]

            def create() -> tuple[str, str, dict[str, Any]]:
                plan_id = uuid.uuid4().hex
                data_versions = self._data_versions(snapshot)
                cost = {"new_total": candidate["total_cost"],
                        "original_total": result["cost"]["original_total"],
                        "sunk_total": result["cost"]["sunk_total"],
                        "replaceable_total": result["cost"]["replaceable_total"],
                        "delta": round(candidate["total_cost"]
                                       - result["cost"]["replaceable_total"], 2),
                        "expected_release_penalty": result["cost"]["expected_release_penalty"],
                        "currency": candidate["currency"]}
                basis = {"advisory_id": advisory_id,
                         "snapshot_at": now,
                         "ready_at": result["ready_at"],
                         "due_at": result["due_at"],
                         "data_versions": data_versions,
                         "current_assessment": result["current_assessment"],
                         "required_documents": candidate["required_documents"],
                         "candidate_count": len(result["feasible"]),
                         "rejected_count": len(result["rejected"]),
                         "explain": candidate["explain"],
                         "rejected": [{"path": item["path"],
                                       "violations": item["violations"]}
                                      for item in result["rejected"]]}
                connection.execute(
                    "INSERT INTO plans(plan_id,cargo_id,status,terminal,candidate_json,"
                    "basis_json,cost_json,required_approvals_json,failure_json,created_by,"
                    "created_at,updated_at,committed_at) VALUES(?,?,'created',0,?,?,?,?,NULL,?,?,?,NULL)",
                    (plan_id, cargo_id, canonical_json(candidate["legs"]),
                     canonical_json(basis), canonical_json(cost),
                     canonical_json(candidate["required_approvals"]),
                     actor_id, now, now))
                connection.execute(
                    "INSERT INTO plan_history(plan_id,status,detail_json,occurred_at) "
                    "VALUES(?,?,?,?)",
                    (plan_id, "created", canonical_json({"by": actor_id}), now))
                append_event(connection, actor_id=actor_id, action="plan.created",
                             resource_type="plan", resource_id=plan_id,
                             detail={"cargo_id": cargo_id, "advisory_id": advisory_id,
                                     "required_approvals": candidate["required_approvals"],
                                     "arrives_at": candidate["arrives_at"],
                                     "delta": cost["delta"]}, occurred_at=now)
                return "plan", plan_id, {"plan_id": plan_id, "status": "created"}

            return self._idempotent(connection, request_id=request_id,
                                    action="create_plan", payload=payload, create=create)

    def lock_plan(self, *, request_id: str, actor_id: str, plan_id: str):
        """锁定方案的全部分段能力；任一分段失败则释放已锁能力并使方案失败。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id}
        pending_failure: Exception | None = None
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            request_id, replay = self._replay_receipt(
                connection, request_id, "lock_plan", payload)
            if replay:
                return replay
            plan = self._plan(connection, plan_id)
            now = self._now()
            if plan["status"] != "created":
                raise ConflictError(f"方案处于 {plan['status']}，不能重复锁定")
            legs = json.loads(plan["candidate_json"])
            snapshot = build_snapshot(connection, now)
            acquired: list[str] = []
            reasons: list[str] = []
            for leg in legs:
                departure = self._find_departure(snapshot, leg["capacity_id"],
                                                 leg["departs_at"])
                if departure is None:
                    reasons.append(f"能力 {leg['capacity_id']}@{leg['departs_at']} 已无班期")
                    break
                occupied = self._occupied(connection, leg["capacity_id"], leg["departs_at"])
                if occupied + 1 > departure["slots"]:
                    reasons.append(f"能力 {leg['capacity_id']}@{leg['departs_at']} 余位不足"
                                   f"（占用 {occupied}/{departure['slots']}）")
                    break
                hold_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO capacity_holds(hold_id,cargo_id,plan_id,capacity_id,segment_id,"
                    "slots,kind,status,departs_at,locked_at,consumed_at,released_at,"
                    "release_reason) VALUES(?,?,?,?,?,1,'plan_lock','locked',?,?,NULL,NULL,NULL)",
                    (hold_id, plan["cargo_id"], plan_id, leg["capacity_id"], leg["segment_id"],
                     leg["departs_at"], now))
                acquired.append(hold_id)
            if reasons:
                # 补偿必须随事务提交：释放已锁能力并把方案置为终态 failed。
                self._release_holds(connection, acquired, "锁定失败回滚", now)
                self._terminate_plan(connection, plan, "failed",
                                     {"stage": "lock", "reasons": reasons}, actor_id, now)
                pending_failure = LockFailed("全部分段锁定未完成：" + "；".join(reasons))
            else:
                def create() -> tuple[str, str, dict[str, Any]]:
                    connection.execute("UPDATE plans SET status='locked',updated_at=? WHERE plan_id=?",
                                       (now, plan_id))
                    connection.execute(
                        "INSERT INTO plan_history(plan_id,status,detail_json,occurred_at) "
                        "VALUES(?,?,?,?)",
                        (plan_id, "locked",
                         canonical_json({"holds": len(acquired), "by": actor_id}), now))
                    self._journal(connection, plan["cargo_id"], "plan.locked",
                                  {"plan_id": plan_id, "holds": len(acquired)}, now)
                    append_event(connection, actor_id=actor_id, action="plan.locked",
                                 resource_type="plan", resource_id=plan_id,
                                 detail={"holds": len(acquired)}, occurred_at=now)
                    return "plan", plan_id, {"plan_id": plan_id, "status": "locked"}

                return self._idempotent(connection, request_id=request_id,
                                        action="lock_plan", payload=payload, create=create)
        # 补偿已提交，再向调用方报告失败。
        raise pending_failure

    def approve_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                     key: str, note: str = ""):
        """登记一个必要审批；审批角色由审批键决定。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id, "key": key, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            request_id, replay = self._replay_receipt(
                connection, request_id, "approve_plan", payload)
            if replay:
                return replay
            plan = self._plan(connection, plan_id)
            required = json.loads(plan["required_approvals_json"])
            if key not in required:
                raise ValidationError("该方案不需要此审批")
            if key not in APPROVER_ROLES:
                raise ValidationError("未知审批键")
            self._require(actor, *APPROVER_ROLES[key])
            if plan["status"] != "locked":
                raise ConflictError("请先锁定全部分段再提交审批")
            now = self._now()
            existing = connection.execute(
                "SELECT 1 FROM plan_approvals WHERE plan_id=? AND approval_key=?",
                (plan_id, key)).fetchone()

            def create() -> tuple[str, str, dict[str, Any]]:
                if existing:
                    return "approval", f"{plan_id}:{key}", {"plan_id": plan_id, "key": key}
                connection.execute(
                    "INSERT INTO plan_approvals(plan_id,approval_key,actor_id,decided_at,note) "
                    "VALUES(?,?,?,?,?)", (plan_id, key, actor_id, now, note))
                connection.execute(
                    "INSERT INTO plan_history(plan_id,status,detail_json,occurred_at) "
                    "VALUES(?,?,?,?)",
                    (plan_id, "locked",
                     canonical_json({"approval": key, "by": actor_id, "note": note}), now))
                append_event(connection, actor_id=actor_id, action="plan.approved",
                             resource_type="plan", resource_id=plan_id,
                             detail={"approval": key}, occurred_at=now)
                return "approval", f"{plan_id}:{key}", {"plan_id": plan_id, "key": key}

            return self._idempotent(connection, request_id=request_id,
                                    action="approve_plan", payload=payload, create=create)

    def commit_plan(self, *, request_id: str, actor_id: str, plan_id: str):
        """审批齐备后切换：释放未使用旧能力（保留费用并计违约金），启用新行程。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id}
        pending_failure: Exception | None = None
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            request_id, replay = self._replay_receipt(
                connection, request_id, "commit_plan", payload)
            if replay:
                return replay
            plan = self._plan(connection, plan_id)
            now = self._now()
            if plan["status"] != "locked":
                raise ConflictError(f"方案处于 {plan['status']}，不能切换")
            cargo_row = self._cargo(connection, plan["cargo_id"])
            plan_holds = connection.execute(
                "SELECT * FROM capacity_holds WHERE plan_id=? AND status='locked'",
                (plan_id,)).fetchall()
            legs = json.loads(plan["candidate_json"])
            if len(plan_holds) != len(legs):
                self._abort_plan(connection, plan, actor, now,
                                 "方案锁定不完整，拒绝切换", {"stage": "precommit"})
                pending_failure = LockFailed("方案锁定不完整")

            # 交接保护：货物已物理交接则方案作废，只释放未使用的锁定能力。
            elif cargo_row["state"] in HANDED_STATES:
                self._release_holds(connection, [row["hold_id"] for row in plan_holds],
                                    "货物已交接，方案能力释放", now)
                self._terminate_plan(connection, plan, "failed",
                                     {"stage": "commit", "reason": "货物已交接，不能拉回旧路线"},
                                     actor_id, now)
                pending_failure = ConflictError("货物已完成交接，迟到的切换被拒绝")

            if pending_failure is None:
                # 按最新版本复核通行限制与口岸状态；新出现的阻断失败闭合，不产生新费用。
                snapshot = build_snapshot(connection, now)
                cargo_view = self._cargo_view(cargo_row)
                blockers: list[str] = []
                for leg in legs:
                    segment = snapshot["segments"].get(leg["segment_id"])
                    if segment is None:
                        blockers.append(f"分段 {leg['segment_id']} 已无生效版本")
                        continue
                    from .planner import _departure_blockers
                    blockers.extend(_departure_blockers(
                        {"capacity_id": leg["capacity_id"], "slots": 10**9,
                         "departs_at": leg["departs_at"], "arrives_at": leg["arrives_at"]},
                        segment, cargo_view, snapshot, 0,
                        json.loads(plan["basis_json"]).get("ready_at", ""), "0"))
                if blockers:
                    self._release_holds(connection, [row["hold_id"] for row in plan_holds],
                                        "复核发现新通行限制，能力释放", now)
                    self._terminate_plan(connection, plan, "failed",
                                         {"stage": "precommit_recheck", "reasons": blockers},
                                         actor_id, now)
                    pending_failure = LockFailed("切换前复核未通过：" + "；".join(blockers))

            if pending_failure is None:
                # 复核通过后才检查审批闸门：只有锁定全部分段且审批齐备才允许切换。
                required = json.loads(plan["required_approvals_json"])
                granted = {row["approval_key"] for row in connection.execute(
                    "SELECT approval_key FROM plan_approvals WHERE plan_id=?", (plan_id,))}
                missing = [key for key in required if key not in granted]
                if missing:
                    raise ApprovalRequired(f"尚缺审批：{', '.join(missing)}")

            def create() -> tuple[str, str, dict[str, Any]]:
                old_legs = self._current_legs(connection, plan["cargo_id"])
                generation = max(row["generation"] for row in connection.execute(
                    "SELECT generation FROM shipment_legs WHERE cargo_id=?",
                    (plan["cargo_id"],)).fetchall())
                fees = self._release_previous_booking(connection, cargo_row, old_legs, plan,
                                                      snapshot, actor_id, now)
                hold_by_departure = {(row["capacity_id"], row["departs_at"]): row["hold_id"]
                                     for row in plan_holds}
                # 已实际走行的前缀分段结转到新一代行程，能力与费用均保持不变。
                for old_leg in old_legs:
                    if old_leg["status"] in ("departed", "in_transit"):
                        connection.execute(
                            "INSERT INTO shipment_legs(leg_id,cargo_id,generation,seq,segment_id,"
                            "capacity_id,carrier_id,carrier_name,departs_at,arrives_at,booked_cost,"
                            "currency,source_plan_id,status,created_at,released_at) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
                            (uuid.uuid4().hex, plan["cargo_id"], generation + 1, old_leg["seq"],
                             old_leg["segment_id"], old_leg["capacity_id"], old_leg["carrier_id"],
                             old_leg["carrier_name"], old_leg["departs_at"], old_leg["arrives_at"],
                             old_leg["booked_cost"], old_leg["currency"], plan_id,
                             old_leg["status"], now))
                for leg in legs:
                    connection.execute(
                        "INSERT INTO shipment_legs(leg_id,cargo_id,generation,seq,segment_id,"
                        "capacity_id,carrier_id,carrier_name,departs_at,arrives_at,booked_cost,"
                        "currency,source_plan_id,status,created_at,released_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'booked',?,NULL)",
                        (uuid.uuid4().hex, plan["cargo_id"], generation + 1, leg["seq"],
                         leg["segment_id"], leg["capacity_id"], leg["carrier_id"],
                         leg["carrier_name"], leg["departs_at"], leg["arrives_at"],
                         leg["booked_cost"], leg["currency"], plan_id, now))
                    connection.execute(
                        "UPDATE capacity_holds SET status='consumed',consumed_at=? WHERE hold_id=?",
                        (now, hold_by_departure[(leg["capacity_id"], leg["departs_at"])]))
                    connection.execute(
                        "INSERT INTO cost_entries(entry_id,cargo_id,plan_id,category,amount,"
                        "currency,reason,occurred_at,created_by) VALUES(?,?,?,?,?,?,?,?,?)",
                        (uuid.uuid4().hex, plan["cargo_id"], plan_id, "new_booking",
                         leg["booked_cost"], leg["currency"],
                         f"改道新订 {leg['segment_id']}", now, actor_id))
                connection.execute(
                    "UPDATE shipments SET state='switched',responsible_party=?,eta=?,"
                    "active_plan_id=?,updated_at=? WHERE cargo_id=?",
                    (legs[0]["carrier_name"], legs[-1]["arrives_at"], plan_id, now,
                     plan["cargo_id"]))
                cost = json.loads(plan["cost_json"])
                cost["incurred_fees"] = fees
                connection.execute(
                    "UPDATE plans SET status='committed',terminal=1,cost_json=?,"
                    "committed_at=?,updated_at=? WHERE plan_id=?",
                    (canonical_json(cost), now, now, plan_id))
                connection.execute(
                    "INSERT INTO plan_history(plan_id,status,detail_json,occurred_at) "
                    "VALUES(?,?,?,?)",
                    (plan_id, "committed", canonical_json({"by": actor_id, "fees": fees}), now))
                self._journal(connection, plan["cargo_id"], "plan.committed",
                              {"plan_id": plan_id, "state": "switched",
                               "responsible_party": legs[0]["carrier_name"],
                               "eta": legs[-1]["arrives_at"], "fees": fees}, now)
                append_event(connection, actor_id=actor_id, action="plan.committed",
                             resource_type="plan", resource_id=plan_id,
                             detail={"cargo_id": plan["cargo_id"], "fees": fees,
                                     "eta": legs[-1]["arrives_at"]}, occurred_at=now)
                return "plan", plan_id, {"plan_id": plan_id, "status": "committed"}

            if pending_failure is not None:
                # 补偿已随本事务提交，退出事务后再报告失败。
                pass
            else:
                return self._idempotent(connection, request_id=request_id,
                                        action="commit_plan", payload=payload, create=create)
        raise pending_failure

    def cancel_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                    reason: str = "人工撤销"):
        """撤销未切换方案，释放尚未使用的锁定能力，不计费用。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id, "reason": reason}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            request_id, replay = self._replay_receipt(
                connection, request_id, "cancel_plan", payload)
            if replay:
                return replay
            plan = self._plan(connection, plan_id)
            now = self._now()
            if plan["status"] not in ("created", "locked"):
                raise ConflictError("只有未切换的方案可以撤销")

            def create() -> tuple[str, str, dict[str, Any]]:
                if plan["status"] == "locked":
                    rows = connection.execute(
                        "SELECT hold_id FROM capacity_holds WHERE plan_id=? AND status='locked'",
                        (plan_id,)).fetchall()
                    self._release_holds(connection, [row["hold_id"] for row in rows], reason, now)
                self._terminate_plan(connection, plan, "cancelled",
                                     {"reason": reason, "by": actor_id}, actor_id, now)
                return "plan", plan_id, {"plan_id": plan_id, "status": "cancelled"}

            return self._idempotent(connection, request_id=request_id,
                                    action="cancel_plan", payload=payload, create=create)

    # ------------------------------------------------- 交接与迟到的恢复消息

    def mark_handover(self, *, request_id: str, actor_id: str, cargo_id: str, note: str = ""):
        """记录货物已向当前责任方完成物理交接，此后拒绝任何拉回。"""

        payload = {"actor_id": actor_id, "cargo_id": cargo_id, "note": note}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            request_id, replay = self._replay_receipt(
                connection, request_id, "mark_handover", payload)
            if replay:
                return replay
            cargo = self._cargo(connection, cargo_id)
            now = self._now()
            if cargo["state"] in HANDED_STATES:
                from .models import WriteReceipt
                return WriteReceipt(request_id, "shipment", cargo_id, True)
            if cargo["state"] not in ("booked", "switched"):
                raise ConflictError(f"货物状态 {cargo['state']} 不能交接")

            def create() -> tuple[str, str, dict[str, Any]]:
                connection.execute(
                    "UPDATE shipments SET state='handed_over',updated_at=? WHERE cargo_id=?",
                    (now, cargo_id))
                connection.execute(
                    "UPDATE disruption_impacts SET handed=1 WHERE cargo_id=?", (cargo_id,))
                self._journal(connection, cargo_id, "shipment.handed_over",
                              {"state": "handed_over",
                               "responsible_party": cargo["responsible_party"],
                               "eta": cargo["eta"], "note": note}, now)
                append_event(connection, actor_id=actor_id, action="shipment.handed_over",
                             resource_type="shipment", resource_id=cargo_id,
                             detail={"note": note}, occurred_at=now)
                return "shipment", cargo_id, {"cargo_id": cargo_id, "state": "handed_over"}

            return self._idempotent(connection, request_id=request_id,
                                    action="mark_handover", payload=payload, create=create)

    def mark_delivered(self, *, request_id: str, actor_id: str, cargo_id: str):
        """记录货物完成交付。"""

        payload = {"actor_id": actor_id, "cargo_id": cargo_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator")
            request_id, replay = self._replay_receipt(
                connection, request_id, "mark_delivered", payload)
            if replay:
                return replay
            cargo = self._cargo(connection, cargo_id)
            now = self._now()
            if cargo["state"] == "delivered":
                from .models import WriteReceipt
                return WriteReceipt(request_id, "shipment", cargo_id, True)
            if cargo["state"] != "handed_over":
                raise ConflictError("货物须先完成交接才能交付")

            def create() -> tuple[str, str, dict[str, Any]]:
                connection.execute(
                    "UPDATE shipments SET state='delivered',updated_at=? WHERE cargo_id=?",
                    (now, cargo_id))
                self._journal(connection, cargo_id, "shipment.delivered",
                              {"state": "delivered"}, now)
                append_event(connection, actor_id=actor_id, action="shipment.delivered",
                             resource_type="shipment", resource_id=cargo_id,
                             detail={}, occurred_at=now)
                return "shipment", cargo_id, {"cargo_id": cargo_id, "state": "delivered"}

            return self._idempotent(connection, request_id=request_id,
                                    action="mark_delivered", payload=payload, create=create)

    def record_recovery(self, *, request_id: str, actor_id: str, advisory_id: str):
        """登记中断解除消息；已交接货物不拉回，未切换方案自动作废并释放能力。"""

        payload = {"actor_id": actor_id, "advisory_id": advisory_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "operator", "reviewer")
            request_id, replay = self._replay_receipt(
                connection, request_id, "record_recovery", payload)
            if replay:
                return replay
            now = self._now()
            if active_version(connection, "advisory", advisory_id, now) is None:
                raise NotFoundError("风险通告不存在")
            impacts = connection.execute(
                "SELECT * FROM disruption_impacts WHERE advisory_id=?",
                (advisory_id,)).fetchall()
            if not impacts:
                raise NotFoundError("没有该通告对应的受影响清单")
            actions: dict[str, int] = {}
            detail_per_cargo: list[dict[str, Any]] = []
            for impact in impacts:
                cargo = self._cargo(connection, impact["cargo_id"])
                if cargo["state"] in HANDED_STATES:
                    action = "ignored_handed_over"
                else:
                    active_plan = connection.execute(
                        "SELECT * FROM plans WHERE cargo_id=? AND terminal=0",
                        (impact["cargo_id"],)).fetchone()
                    if active_plan:
                        holds = connection.execute(
                            "SELECT hold_id FROM capacity_holds WHERE plan_id=? AND status='locked'",
                            (active_plan["plan_id"],)).fetchall()
                        self._release_holds(connection, [row["hold_id"] for row in holds],
                                            "恢复消息先于切换到达，释放能力", now)
                        self._terminate_plan(connection, active_plan, "cancelled",
                                             {"reason": "中断解除，方案自动作废"},
                                             actor_id, now)
                        action = "plan_cancelled"
                    elif cargo["state"] == "switched":
                        action = "kept_new_route"
                    else:
                        action = "resolved_no_change"
                actions[action] = actions.get(action, 0) + 1
                detail_per_cargo.append({"cargo_id": impact["cargo_id"], "action": action})
                connection.execute(
                    "UPDATE disruption_impacts SET recovery_action=? WHERE impact_id=?",
                    (action, impact["impact_id"]))
                self._journal(connection, impact["cargo_id"], "recovery.recorded",
                              {"advisory_id": advisory_id, "action": action}, now)

            def create() -> tuple[str, str, dict[str, Any]]:
                append_event(connection, actor_id=actor_id, action="recovery.recorded",
                             resource_type="advisory", resource_id=advisory_id,
                             detail={"actions": actions, "items": detail_per_cargo},
                             occurred_at=now)
                return "advisory", advisory_id, {"advisory_id": advisory_id, "actions": actions}

            return self._idempotent(connection, request_id=request_id,
                                    action="record_recovery", payload=payload, create=create)

    # ------------------------------------------------------------------ 视图

    def evaluate_candidates(self, *, actor_id: str, cargo_id: str) -> dict[str, Any]:
        """只读：返回当前时点的可解释候选路径，不落地方案。"""

        connection = self.database.connection
        actor = self._actor(connection, actor_id)
        self._require(actor, "admin", "operator", "reviewer", "finance")
        cargo_row = self._cargo(connection, cargo_id)
        now = self._now()
        snapshot = build_snapshot(connection, now)
        result = plan_candidates(snapshot, self._cargo_view(cargo_row),
                                 self._holds(connection),
                                 self._current_legs(connection, cargo_id))
        return {"cargo_id": cargo_id, "at": now,
                "feasible_count": len(result["feasible"]),
                "feasible": result["feasible"], "rejected": result["rejected"],
                "current_assessment": result["current_assessment"],
                "cost": result["cost"]}

    def ops_view(self, *, actor_id: str, cargo_id: str | None = None,
                 at: str | None = None) -> Any:
        """运营视图：当前责任方、ETA、状态、改道依据与受影响情况。"""

        actor = self._actor(self.database.connection, actor_id)
        self._require(actor, "admin", "operator", "reviewer")
        if at is not None:
            return self.reconstruct(at, cargo_id=cargo_id)
        connection = self.database.connection
        rows = self._shipment_rows(connection, cargo_id)
        items = []
        for row in rows:
            plan = None
            if row["active_plan_id"]:
                plan = self.get_plan(row["active_plan_id"])
            items.append({
                "cargo_id": row["cargo_id"], "customer_id": row["customer_id"],
                "state": row["state"], "origin": row["origin"],
                "destination": row["destination"],
                "responsible_party": row["responsible_party"], "eta": row["eta"],
                "active_plan_id": row["active_plan_id"],
                "reroute_basis": (plan["basis"] if plan else None),
                "current_legs": [dict(leg) for leg in
                                 self._current_legs(connection, row["cargo_id"])],
                "open_plan": (self._brief_open_plan(connection, row["cargo_id"])
                              if row["state"] not in HANDED_STATES else None),
            })
        return {"at": self._now(), "items": items}

    def finance_view(self, *, actor_id: str, cargo_id: str | None = None,
                     at: str | None = None) -> dict[str, Any]:
        """财务视图：只追加费用台账、费用差异与已发生违约金。"""

        actor = self._actor(self.database.connection, actor_id)
        self._require(actor, "admin", "finance")
        connection = self.database.connection
        query = "SELECT * FROM cost_entries"
        parameters: list[Any] = []
        if at is not None or cargo_id is not None:
            clauses = []
            if at is not None:
                clauses.append("occurred_at<=?")
                parameters.append(at)
            if cargo_id is not None:
                clauses.append("cargo_id=?")
                parameters.append(cargo_id)
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY occurred_at, entry_id"
        entries = [dict(row) for row in connection.execute(query, parameters)]
        totals: dict[str, dict[str, float]] = {}
        for entry in entries:
            bucket = totals.setdefault(entry["cargo_id"], {})
            bucket[entry["category"]] = round(
                bucket.get(entry["category"], 0) + entry["amount"], 2)
        plan_query = ("SELECT plan_id,cargo_id,cost_json,committed_at FROM plans "
                      "WHERE status='committed'")
        plan_parameters: list[Any] = []
        if at is not None:
            plan_query += " AND committed_at<=?"
            plan_parameters.append(at)
        if cargo_id is not None:
            plan_query += " AND cargo_id=?"
            plan_parameters.append(cargo_id)
        differences = []
        for row in connection.execute(plan_query, plan_parameters):
            cost = json.loads(row["cost_json"])
            differences.append({"plan_id": row["plan_id"], "cargo_id": row["cargo_id"],
                                "original_total": cost["original_total"],
                                "new_total": cost["new_total"], "delta": cost["delta"],
                                "incurred_fees": cost.get("incurred_fees", []),
                                "committed_at": row["committed_at"]})
        return {"at": at or self._now(), "ledger": entries, "totals": totals,
                "reroute_differences": differences}

    def cargo_timeline(self, cargo_id: str) -> dict[str, Any]:
        """返回货物的完整状态流水与方案轨迹。"""

        cargo = self._cargo(self.database.connection, cargo_id)
        journal = [dict(row) for row in self.database.connection.execute(
            "SELECT sequence,event_id,event_type,detail_json,occurred_at "
            "FROM shipment_journal WHERE cargo_id=? ORDER BY sequence", (cargo_id,))]
        for event in journal:
            event["detail"] = json.loads(event.pop("detail_json"))
        plans = [dict(row) for row in self.database.connection.execute(
            "SELECT plan_id,status,terminal,created_at,committed_at FROM plans "
            "WHERE cargo_id=? ORDER BY created_at", (cargo_id,))]
        return {"cargo_id": cargo_id, "current_state": cargo["state"],
                "journal": journal, "plans": plans}

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        """读取方案详情，含候选路径、改道依据、费用与审批进度。"""

        connection = self.database.connection
        plan = self._plan(connection, plan_id)
        approvals = [dict(row) for row in connection.execute(
            "SELECT * FROM plan_approvals WHERE plan_id=? ORDER BY approval_key", (plan_id,))]
        holds = [dict(row) for row in connection.execute(
            "SELECT hold_id,capacity_id,segment_id,slots,kind,status,departs_at,locked_at,"
            "released_at,release_reason FROM capacity_holds WHERE plan_id=? ORDER BY locked_at",
            (plan_id,))]
        history = [dict(row) for row in connection.execute(
            "SELECT sequence,status,detail_json,occurred_at FROM plan_history "
            "WHERE plan_id=? ORDER BY sequence", (plan_id,))]
        for event in history:
            event["detail"] = json.loads(event.pop("detail_json"))
        required = json.loads(plan["required_approvals_json"])
        granted = {item["approval_key"] for item in approvals}
        return {"plan_id": plan_id, "cargo_id": plan["cargo_id"], "status": plan["status"],
                "terminal": bool(plan["terminal"]),
                "candidate": json.loads(plan["candidate_json"]),
                "basis": json.loads(plan["basis_json"]),
                "cost": json.loads(plan["cost_json"]),
                "required_approvals": required,
                "approvals": approvals,
                "missing_approvals": [key for key in required if key not in granted],
                "holds": holds, "history": history,
                "failure": json.loads(plan["failure_json"]) if plan["failure_json"] else None,
                "created_at": plan["created_at"], "committed_at": plan["committed_at"]}

    def reconstruct(self, at: str, cargo_id: str | None = None) -> dict[str, Any]:
        """从任一历史时点复原当时可用的通道版本、货物状态与在途方案。"""

        self._timestamp(at, "at")
        connection = self.database.connection
        snapshot = build_snapshot(connection, at)
        reference = {
            "segments": {key: value["version"] for key, value in snapshot["segments"].items()},
            "borders": {key: value["version"] for key, value in snapshot["borders"].items()},
            "capacities": {key: value["version"] for key, value in snapshot["capacities"].items()},
            "restrictions": [{"entity_id": item["entity_id"], "version": item["version"]}
                             for item in snapshot["restrictions"]],
            "deadlines": {key: value["version"] for key, value in snapshot["deadlines"].items()},
            "rates": [{"entity_id": item["entity_id"], "version": item["version"]}
                      for item in snapshot["rates"]],
            "active_advisories": [{"entity_id": item["entity_id"], "version": item["version"]}
                                  for item in snapshot["advisories"]],
        }
        shipments = []
        rows = self._shipment_rows(connection, cargo_id)
        for row in rows:
            if row["created_at"] > at:
                continue
            state = {"state": "booked", "responsible_party": row["responsible_party"],
                     "eta": row["eta"], "active_plan_id": None}
            events = connection.execute(
                "SELECT event_type,detail_json FROM shipment_journal "
                "WHERE cargo_id=? AND occurred_at<=? ORDER BY sequence", (row["cargo_id"], at))
            for event in events:
                detail = json.loads(event["detail_json"])
                for field in ("state", "responsible_party", "eta", "active_plan_id"):
                    if field in detail:
                        state[field] = detail[field]
            legs = [dict(leg) for leg in connection.execute(
                "SELECT * FROM shipment_legs WHERE cargo_id=? AND created_at<=? "
                "AND (released_at IS NULL OR released_at>?) ORDER BY seq",
                (row["cargo_id"], at, at))]
            generations = {leg["generation"] for leg in legs}
            if generations:
                top = max(generations)
                legs = [leg for leg in legs if leg["generation"] == top]
            plans = []
            for plan_row in connection.execute(
                    "SELECT * FROM plans WHERE cargo_id=? AND created_at<=? ORDER BY created_at",
                    (row["cargo_id"], at)):
                status_row = connection.execute(
                    "SELECT status FROM plan_history WHERE plan_id=? AND occurred_at<=? "
                    "ORDER BY sequence DESC LIMIT 1", (plan_row["plan_id"], at)).fetchone()
                status = status_row["status"] if status_row else "created"
                plans.append({"plan_id": plan_row["plan_id"], "status": status,
                              "terminal": status in TERMINAL_STATUSES})
            shipments.append({"cargo_id": row["cargo_id"], "customer_id": row["customer_id"],
                              "origin": row["origin"], "destination": row["destination"],
                              "state": state["state"],
                              "responsible_party": state["responsible_party"],
                              "eta": state["eta"], "active_plan_id": state["active_plan_id"],
                              "legs": legs, "plans": plans})
        impacts = [dict(row) for row in connection.execute(
            "SELECT impact_id,advisory_id,cargo_id,segment_id,capacity_id,departs_at,"
            "state_at_impact,handed,detected_at FROM disruption_impacts "
            "WHERE detected_at<=? ORDER BY detected_at", (at,))]
        if cargo_id is not None:
            impacts = [item for item in impacts if item["cargo_id"] == cargo_id]
        for item in impacts:
            recovered = connection.execute(
                "SELECT 1 FROM shipment_journal WHERE cargo_id=? AND event_type='recovery.recorded' "
                "AND occurred_at<=? LIMIT 1", (item["cargo_id"], at)).fetchone()
            item["recovered"] = bool(recovered)
        return {"at": at, "reference": reference, "shipments": shipments, "impacts": impacts}

    # ------------------------------------------------------------------ 内部助手

    def _release_previous_booking(self, connection, cargo_row, old_legs, plan,
                                  snapshot, actor_id, now) -> list[dict[str, Any]]:
        """结转旧预订：已用能力保留、未用能力归还；费用一律保留，另按合同计违约/改签费。"""

        fees: list[dict[str, Any]] = []
        rates = {item["payload"]["carrier_id"]: item["payload"]
                 for item in snapshot["rates"]}
        rebooking_charged: set[str] = set()
        for leg in old_legs:
            rate = rates.get(leg["carrier_id"], {})
            if leg["status"] in ("departed", "in_transit"):
                # 已实际走行：能力继续占用，费用保留，只闭合旧版行程（新代中已结转）。
                connection.execute(
                    "UPDATE shipment_legs SET released_at=? WHERE leg_id=?", (now, leg["leg_id"]))
                continue
            # 未使用分段：归还班期能力，但原始预订费用留在台账。
            connection.execute(
                "UPDATE capacity_holds SET status='released',released_at=?,"
                "release_reason=? WHERE cargo_id=? AND capacity_id=? AND departs_at=? "
                "AND kind='booking' AND status='booked'",
                (now, "改道释放未使用能力", cargo_row["cargo_id"],
                 leg["capacity_id"], leg["departs_at"]))
            connection.execute(
                "UPDATE shipment_legs SET released_at=? WHERE leg_id=?", (now, leg["leg_id"]))
            penalty_rate = rate.get("cancel_penalty_rate", 0)
            penalty = round(leg["booked_cost"] * penalty_rate, 2)
            if penalty:
                fees.append({"category": "cancellation_penalty", "segment_id": leg["segment_id"],
                             "amount": penalty, "currency": leg["currency"]})
                connection.execute(
                    "INSERT INTO cost_entries(entry_id,cargo_id,plan_id,category,amount,"
                    "currency,reason,occurred_at,created_by) VALUES(?,?,?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, cargo_row["cargo_id"], plan["plan_id"],
                     "cancellation_penalty", penalty, leg["currency"],
                     f"未使用分段 {leg['segment_id']} 合同违约费用", now, actor_id))
            fee_amount = rate.get("rebooking_fee", 0)
            if fee_amount and leg["carrier_id"] not in rebooking_charged:
                rebooking_charged.add(leg["carrier_id"])
                fees.append({"category": "rebooking_fee", "segment_id": None,
                             "amount": round(float(fee_amount), 2),
                             "currency": rate.get("currency", leg["currency"])})
                connection.execute(
                    "INSERT INTO cost_entries(entry_id,cargo_id,plan_id,category,amount,"
                    "currency,reason,occurred_at,created_by) VALUES(?,?,?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, cargo_row["cargo_id"], plan["plan_id"],
                     "rebooking_fee", round(float(fee_amount), 2),
                     rate.get("currency", leg["currency"]),
                     f"合同改签费（{leg['carrier_name']}）", now, actor_id))
        return fees

    def _abort_plan(self, connection, plan, actor, now, message, failure) -> None:
        holds = connection.execute(
            "SELECT hold_id FROM capacity_holds WHERE plan_id=? AND status='locked'",
            (plan["plan_id"],)).fetchall()
        self._release_holds(connection, [row["hold_id"] for row in holds], message, now)
        self._terminate_plan(connection, plan, "failed", failure, actor["actor_id"], now)

    def _terminate_plan(self, connection, plan, status: str, detail: dict[str, Any],
                        actor_id: str, now: str) -> None:
        connection.execute(
            "UPDATE plans SET status=?,terminal=1,failure_json=?,updated_at=? WHERE plan_id=?",
            (status, canonical_json(detail) if status == "failed" else plan["failure_json"],
             now, plan["plan_id"]))
        connection.execute(
            "INSERT INTO plan_history(plan_id,status,detail_json,occurred_at) VALUES(?,?,?,?)",
            (plan["plan_id"], status, canonical_json(detail), now))
        self._journal(connection, plan["cargo_id"], f"plan.{status}",
                      {"plan_id": plan["plan_id"], **detail}, now)
        append_event(connection, actor_id=actor_id, action=f"plan.{status}",
                     resource_type="plan", resource_id=plan["plan_id"],
                     detail=detail, occurred_at=now)

    @staticmethod
    def _release_holds(connection, hold_ids: list[str], reason: str, now: str) -> None:
        for hold_id in hold_ids:
            connection.execute(
                "UPDATE capacity_holds SET status='released',released_at=?,release_reason=? "
                "WHERE hold_id=? AND status='locked'", (now, reason, hold_id))

    @staticmethod
    def _journal(connection, cargo_id: str, event_type: str, detail: dict[str, Any],
                 now: str) -> None:
        connection.execute(
            "INSERT INTO shipment_journal(event_id,cargo_id,event_type,detail_json,occurred_at) "
            "VALUES(?,?,?,?,?)",
            (uuid.uuid4().hex, cargo_id, event_type, canonical_json(detail), now))

    @staticmethod
    def _data_versions(snapshot) -> dict[str, Any]:
        return {
            "segments": {key: value["version"] for key, value in snapshot["segments"].items()},
            "borders": {key: value["version"] for key, value in snapshot["borders"].items()},
            "capacities": {key: value["version"] for key, value in snapshot["capacities"].items()},
            "restrictions": {item["entity_id"]: item["version"]
                             for item in snapshot["restrictions"]},
            "deadlines": {key: value["version"] for key, value in snapshot["deadlines"].items()},
            "rates": {item["entity_id"]: item["version"] for item in snapshot["rates"]},
            "advisories": {item["entity_id"]: item["version"] for item in snapshot["advisories"]},
        }

    @staticmethod
    def _find_departure(snapshot, capacity_id: str, departs_at: str):
        capacity = snapshot["capacities"].get(capacity_id)
        if capacity is None:
            return None
        return next((item for item in capacity["payload"]["departures"]
                     if item["departs_at"] == departs_at), None)

    @staticmethod
    def _occupied(connection, capacity_id: str, departs_at: str) -> int:
        row = connection.execute(
            "SELECT COALESCE(SUM(slots),0) AS n FROM capacity_holds "
            "WHERE capacity_id=? AND departs_at=? AND status IN ('booked','locked','consumed')",
            (capacity_id, departs_at)).fetchone()
        return row["n"]

    @staticmethod
    def _holds(connection) -> list[dict[str, Any]]:
        return [dict(row) for row in connection.execute(
            "SELECT cargo_id,capacity_id,departs_at,slots,status FROM capacity_holds "
            "WHERE status IN ('booked','locked','consumed')")]

    @staticmethod
    def _current_legs(connection, cargo_id: str):
        rows = connection.execute(
            "SELECT * FROM shipment_legs WHERE cargo_id=? AND released_at IS NULL "
            "ORDER BY seq", (cargo_id,)).fetchall()
        if rows:
            generation = max(row["generation"] for row in rows)
            rows = [row for row in rows if row["generation"] == generation]
        return rows

    @staticmethod
    def _cargo(connection, cargo_id: str):
        row = connection.execute("SELECT * FROM shipments WHERE cargo_id=?",
                                 (cargo_id,)).fetchone()
        if row is None:
            raise NotFoundError("货物不存在")
        return row

    @staticmethod
    def _cargo_view(row) -> dict[str, Any]:
        return {"cargo_id": row["cargo_id"], "origin": row["origin"],
                "destination": row["destination"],
                "tags": json.loads(row["cargo_tags_json"]), "weight": row["weight"]}

    @staticmethod
    def _plan(connection, plan_id: str):
        row = connection.execute("SELECT * FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFoundError("方案不存在")
        return row

    @staticmethod
    def _shipment_rows(connection, cargo_id: str | None):
        if cargo_id:
            return connection.execute(
                "SELECT * FROM shipments WHERE cargo_id=? ORDER BY cargo_id",
                (cargo_id,)).fetchall()
        return connection.execute("SELECT * FROM shipments ORDER BY cargo_id").fetchall()

    @staticmethod
    def _brief_open_plan(connection, cargo_id: str):
        row = connection.execute(
            "SELECT plan_id,status,required_approvals_json FROM plans "
            "WHERE cargo_id=? AND terminal=0", (cargo_id,)).fetchone()
        if row is None:
            return None
        return {"plan_id": row["plan_id"], "status": row["status"],
                "required_approvals": json.loads(row["required_approvals_json"])}

    @staticmethod
    def _timestamp(value: str, field: str) -> str:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise ValidationError(f"{field} 必须是 ISO-8601 时间") from exc
        return value
