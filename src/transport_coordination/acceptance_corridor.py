"""国际通道韧性编排的离线端到端验收。

在临时 SQLite 库中构建"铁路基线 + 海运备选 + 公路接驳"网络，制造口岸
限流，验证：可解释候选、受影响清单、审批门禁、部分失败补偿、
竞争方案唯一、交接冻结与任一时点复原。成功时输出一行 status=ok 的 JSON。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .corridor import CorridorOrchestrator
from .errors import ConflictError, PermissionDenied
from .service import DomainService
from .storage import Database


def _bootstrap(database, clock):
    domain = DomainService(database, clock)
    oc = CorridorOrchestrator(database, clock)
    domain.register_organization(request_id="org1", actor_id="bootstrap",
                                 organization_id="org", name="国际物流平台")
    domain.register_actor(request_id="ra1", actor_id="bootstrap", new_actor_id="admin",
                          display_name="管理员", role="admin", organization_id="org")
    for rid, aid, role, name in [
            ("ro1", "op", "operator", "运营"), ("rf1", "fi", "finance", "财务"),
            ("rs1", "sa", "safety", "安全")]:
        domain.register_actor(request_id=rid, actor_id="admin", new_actor_id=aid,
                              display_name=name, role=role, organization_id="org")
    return domain, oc


def _publish_network(oc):
    for i, (sid, a, b, mode, hours, bc) in enumerate([
            ("g1", "a", "x", "rail", 10, "bc1"), ("g2", "x", "b", "rail", 10, None),
            ("h1", "a", "p", "sea", 30, None), ("h2", "p", "b", "road", 5, None)]):
        oc.publish_reference(request_id=f"sg{i}", actor_id="op", kind="segment", data={
            "segment_id": sid, "from_node": a, "to_node": b, "mode": mode,
            "transit_hours": hours, **({"border_crossing_id": bc} if bc else {})})
    for i, (cid, seg, carrier, units, fee) in enumerate([
            ("cg1", "g1", "carrier_rail", 10, 100), ("cg2", "g2", "carrier_rail2", 10, 100),
            ("ch1", "h1", "carrier_sea", 10, 100), ("ch2", "h2", "carrier_road", 1, 70)]):
        oc.publish_reference(request_id=f"cp{i}", actor_id="op", kind="capacity", data={
            "capacity_id": cid, "segment_id": seg, "carrier_id": carrier,
            "carrier_name": carrier.upper(), "valid_from": "2026-09-01T00:00Z",
            "valid_to": "2026-12-31T00:00Z", "unit": "FEU", "capacity_units": units,
            "unit_cost": 50, "commitment_fee": fee, "currency": "USD"})
    for i, (rid, seg, carrier, cost) in enumerate([
            ("rg1", "g1", "carrier_rail", 100), ("rg2", "g2", "carrier_rail2", 100),
            ("rh1", "h1", "carrier_sea", 400), ("rh2", "h2", "carrier_road", 100)]):
        oc.publish_reference(request_id=f"rp{i}", actor_id="fi", kind="contract_rate", data={
            "rate_id": rid, "segment_id": seg, "carrier_id": carrier,
            "valid_from": "2026-09-01T00:00Z", "valid_to": "2026-12-31T00:00Z",
            "unit_cost": cost, "fixed_cost": 0, "minimum_charge": 0, "currency": "USD"})
    oc.publish_reference(request_id="rr1", actor_id="sa", kind="cargo_restriction", data={
        "restriction_id": "no_class3_road", "scope": {"mode": "road"},
        "forbidden_classes": ["class3"], "forbidden_cargo_types": [],
        "note": "公路禁运三类危险品", "status": "active"})


def run() -> dict[str, object]:
    with tempfile.TemporaryDirectory() as directory:
        clock = FixedClock(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc))
        database = Database(Path(directory) / "corridor_acceptance.sqlite3")
        domain, oc = _bootstrap(database, clock)
        _publish_network(oc)

        oc.register_shipment(request_id="sh1", actor_id="op", shipment_id="s1",
                             cargo_type="general", units=1, origin_node="a",
                             destination_node="b", customer_ref="CUST-1",
                             must_arrive_by="2026-10-20T00:00Z")
        oc.set_baseline(request_id="bl1", actor_id="op", shipment_id="s1",
                        path=["g1", "g2"], depart_at="2026-10-02T00:00Z")

        clock._value = datetime(2026, 10, 3, tzinfo=timezone.utc)
        oc.publish_notice(request_id="n1", actor_id="op", notice_id="notice1",
                          title="口岸临时限流", severity="critical", detail="bc1 关闭",
                          impact={"border_crossings": ["bc1"]})
        evaluation = oc.evaluate_disruption(actor_id="op", notice_id="notice1")
        entry = evaluation["shipments"][0]
        assert entry["affected"]
        sea = next(c for c in entry["candidates"] if c["path"] == ["h1", "h2"])
        assert sea["feasible"] and set(sea["required_approvals"]) == {"admin", "finance"}
        rail = next(c for c in entry["candidates"] if c["path"] == ["g1", "g2"])
        assert not rail["feasible"]

        oc.select_plan(request_id="sel1", actor_id="op", plan_id=sea["plan_id"], reason="海运备选")
        blocked = False
        try:
            oc.execute_switch(request_id="sw1", actor_id="op", plan_id=sea["plan_id"])
        except PermissionDenied:
            blocked = True
        assert blocked
        oc.approve_plan(request_id="ap1", actor_id="admin", plan_id=sea["plan_id"])
        oc.approve_plan(request_id="ap2", actor_id="fi", plan_id=sea["plan_id"])

        # 占满公路接驳能力，制造部分失败：已锁海运能力释放，承诺费保留。
        oc.register_shipment(request_id="sh2", actor_id="op", shipment_id="s2",
                             cargo_type="bulk", units=1, origin_node="a",
                             destination_node="b", customer_ref="CUST-2",
                             must_arrive_by="2026-11-20T00:00Z")
        oc.set_baseline(request_id="bl2", actor_id="op", shipment_id="s2",
                        path=["h1", "h2"], depart_at="2026-10-02T00:00Z")
        partial_failed = False
        try:
            oc.execute_switch(request_id="sw2", actor_id="op", plan_id=sea["plan_id"])
        except ConflictError:
            partial_failed = True
        assert partial_failed
        fee = database.connection.execute(
            "SELECT amount FROM corridor_fees WHERE plan_id=? AND fee_type='commitment_forfeited'",
            (sea["plan_id"],)).fetchone()
        assert fee is not None and fee["amount"] == 100

        # 释放后重试：放宽公路能力新版本，切换成功。
        oc.publish_reference(request_id="cp_h2_v2", actor_id="op", kind="capacity", data={
            "capacity_id": "ch2", "segment_id": "h2", "carrier_id": "carrier_road",
            "carrier_name": "CARRIER_ROAD", "valid_from": "2026-09-01T00:00Z",
            "valid_to": "2026-12-31T00:00Z", "unit": "FEU", "capacity_units": 5,
            "unit_cost": 50, "commitment_fee": 70, "currency": "USD"})
        result = oc.execute_switch(request_id="sw3", actor_id="op", plan_id=sea["plan_id"])
        assert result["status"] == "effective"
        assert oc.execute_switch(request_id="sw3", actor_id="op",
                                 plan_id=sea["plan_id"])["replayed"]
        effective_count = database.connection.execute(
            "SELECT COUNT(*) AS c FROM corridor_plans WHERE shipment_id='s1' AND status='effective'"
        ).fetchone()["c"]
        assert effective_count == 1

        # 交接冻结：恢复消息不回拉。
        clock._value = datetime(2026, 10, 15, tzinfo=timezone.utc)
        oc.record_checkpoint(request_id="ckp1", actor_id="op", shipment_id="s1",
                             node_id="b", event="handed_over")
        recovered = oc.recover_notice(request_id="rc1", actor_id="op", notice_id="notice1")
        assert "s1" in recovered["handed_over_shipments_unchanged"]

        # 任一时点复原。
        before = oc.history_snapshot(actor_id="op", at="2026-10-02T12:00:00Z")
        assert before["shipments"][0]["effective_plan"]["status"] == "baseline"
        assert before["notices"] == []
        finance = oc.finance_view(actor_id="fi", shipment_id="s1")["items"][0]
        assert finance["cost_delta"] == 300
        audit_valid, audit_events = domain.verify_audit()
        database.close()
        return {"status": "ok", "affected": entry["affected"],
                "candidate_paths": [c["path"] for c in entry["candidates"]],
                "partial_failure_retained_fee": 100,
                "switched": result["status"] == "effective",
                "cost_delta": finance["cost_delta"],
                "handed_over_unchanged": recovered["handed_over_shipments_unchanged"],
                "history_baseline_restored": True,
                "audit_valid": audit_valid, "audit_events": audit_events}


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" and result["audit_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
