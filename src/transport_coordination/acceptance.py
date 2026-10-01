"""运行基础服务与国际通道韧性编排的离线端到端验收。"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .orchestration import OrchestrationService
from .service import DomainService
from .storage import Database


def run() -> dict[str, object]:
    """执行登记链与中断改道全流程并返回结果。"""

    with tempfile.TemporaryDirectory() as directory:
        database = Database(Path(directory) / "acceptance.sqlite3")
        clock = FixedClock(datetime(2026, 9, 25, 9, 0, tzinfo=timezone.utc))
        service = DomainService(database, clock)
        orch = OrchestrationService(database, clock)

        # ---- 基础登记链（保留原有验收语义）
        service.register_organization(request_id="req-org", actor_id="bootstrap",
                                      organization_id="org-001", name="示范运营机构")
        service.register_actor(request_id="req-admin", actor_id="bootstrap",
                               new_actor_id="admin-001", display_name="系统管理员",
                               role="admin", organization_id="org-001")
        service.register_actor(request_id="req-operator", actor_id="admin-001",
                               new_actor_id="operator-001", display_name="运营负责人",
                               role="operator", organization_id="org-001")
        service.register_actor(request_id="req-reviewer", actor_id="admin-001",
                               new_actor_id="reviewer-001", display_name="客服审核",
                               role="reviewer", organization_id="org-001")
        service.register_actor(request_id="req-finance", actor_id="admin-001",
                               new_actor_id="finance-001", display_name="财务",
                               role="finance", organization_id="org-001")
        service.register_site(request_id="req-site", actor_id="operator-001",
                              site_id="site-001", organization_id="org-001",
                              name="一号交通节点", timezone_name="Asia/Shanghai")
        first = service.record_domain_data(request_id="req-data", actor_id="operator-001",
                                           site_id="site-001", category="operator_profile",
                                           external_key="record-001",
                                           data={"name": "基础资料", "enabled": True})
        replay = service.record_domain_data(request_id="req-data", actor_id="operator-001",
                                            site_id="site-001", category="operator_profile",
                                            external_key="record-001",
                                            data={"name": "基础资料", "enabled": True})

        # ---- 通道资料
        segments = {
            "rail-cn-xi": ("CN", "XI", "rail", None),
            "rail-xi-eu": ("XI", "EU", "rail", "border-ala"),
            "rail-eu-de": ("EU", "DE", "rail", None),
            "truck-xi-port": ("XI", "PORT", "truck", None),
            "sea-port-eu": ("PORT", "EU", "sea", "border-sea"),
            "truck-eu-de": ("EU", "DE", "truck", None),
        }
        for sid, (a, b, mode, border) in segments.items():
            orch.publish_reference(request_id=f"seg-{sid}", actor_id="operator-001",
                                   entity_type="segment", entity_id=sid,
                                   valid_from="2026-09-01T00:00:00Z",
                                   payload={"origin": a, "destination": b, "mode": mode,
                                            "border_id": border, "capacity_id": f"cap-{sid}"})
        departures = {
            "cap-rail-cn-xi": ("CRE", "班列", "rail-cn-xi",
                               [("2026-09-22T08:00:00Z", "2026-09-22T20:00:00Z", 20)]),
            "cap-rail-xi-eu": ("CRE", "班列", "rail-xi-eu",
                               [("2026-09-23T08:00:00Z", "2026-10-01T08:00:00Z", 20)]),
            "cap-rail-eu-de": ("CRE", "班列", "rail-eu-de",
                               [("2026-10-01T12:00:00Z", "2026-10-02T08:00:00Z", 20)]),
            "cap-truck-xi-port": ("SHT", "集卡", "truck-xi-port",
                                  [("2026-09-25T06:00:00Z", "2026-09-26T06:00:00Z", 10)]),
            "cap-sea-port-eu": ("MSC", "海运", "sea-port-eu",
                                [("2026-09-27T00:00:00Z", "2026-10-05T12:00:00Z", 5)]),
            "cap-truck-eu-de": ("SHT", "接驳", "truck-eu-de",
                                [("2026-10-06T00:00:00Z", "2026-10-06T18:00:00Z", 10)]),
        }
        for cid, (carrier, name, seg, deps) in departures.items():
            orch.publish_reference(request_id=f"cap-{cid}", actor_id="operator-001",
                                   entity_type="capacity", entity_id=cid,
                                   valid_from="2026-09-01T00:00:00Z",
                                   payload={"segment_id": seg, "carrier_id": carrier,
                                            "carrier_name": name,
                                            "departures": [{"departs_at": x, "arrives_at": y,
                                                            "slots": n} for x, y, n in deps]})
        orch.publish_reference(request_id="border-ala", actor_id="operator-001",
                               entity_type="border_calendar", entity_id="border-ala",
                               valid_from="2026-09-01T00:00:00Z",
                               payload={"closures": [
                                   {"from": "2026-09-23T00:00:00Z",
                                    "to": "2026-09-26T23:59:59Z",
                                    "reason": "口岸临时限流", "segment_id": "rail-xi-eu"}]})
        orch.publish_reference(request_id="border-sea", actor_id="operator-001",
                               entity_type="border_calendar", entity_id="border-sea",
                               valid_from="2026-09-01T00:00:00Z",
                               payload={"closures": []})
        orch.publish_reference(request_id="rest-li", actor_id="operator-001",
                               entity_type="cargo_restriction", entity_id="rest-li",
                               valid_from="2026-09-01T00:00:00Z",
                               payload={"scope": "cargo_tag", "target_id": "lithium",
                                        "forbidden_modes": ["sea"], "forbidden_segments": [],
                                        "document_required": "MSDS"})
        for cargo_id in ("cargo-001", "cargo-002"):
            orch.publish_reference(request_id=f"deadline-{cargo_id}", actor_id="operator-001",
                                   entity_type="customer_deadline", entity_id=cargo_id,
                                   valid_from="2026-09-01T00:00:00Z",
                                   payload={"ready_at": "2026-09-18T00:00:00Z",
                                            "due_at": "2026-10-10T23:59:59Z"})
        rates = {"CRE": (0.5, 800, {"rail-cn-xi": 3000, "rail-xi-eu": 12000,
                                    "rail-eu-de": 2500}),
                 "SHT": (0.3, 300, {"truck-xi-port": 2000, "truck-eu-de": 3000}),
                 "MSC": (0.8, 1500, {"sea-port-eu": 18000})}
        for carrier, (penalty, rebook, seg_rates) in rates.items():
            orch.publish_reference(request_id=f"rate-{carrier}", actor_id="finance-001",
                                   entity_type="contract_rate", entity_id=f"rate-{carrier}",
                                   valid_from="2026-09-01T00:00:00Z",
                                   payload={"carrier_id": carrier,
                                            "cancel_penalty_rate": penalty,
                                            "rebooking_fee": rebook, "currency": "CNY",
                                            "segment_rates": seg_rates})
        orch.publish_reference(request_id="advisory", actor_id="operator-001",
                               entity_type="advisory", entity_id="adv-ala",
                               valid_from="2026-09-23T00:05:00Z",
                               payload={"level": "severe",
                                        "affected": {"segments": ["rail-xi-eu"],
                                                     "capacities": []},
                                        "window": {"from": "2026-09-23T00:00:00Z",
                                                   "to": "2026-09-26T23:59:59Z"},
                                        "summary": "阿拉山口口岸临时限流"})

        # ---- 货物登记（10:00）
        clock._value = datetime(2026, 9, 25, 10, 0, tzinfo=timezone.utc)
        original_legs = [
            {"segment_id": "rail-cn-xi", "departs_at": "2026-09-22T08:00:00Z",
             "arrives_at": "2026-09-22T20:00:00Z", "status": "departed"},
            {"segment_id": "rail-xi-eu", "departs_at": "2026-09-23T08:00:00Z",
             "arrives_at": "2026-10-01T08:00:00Z"},
            {"segment_id": "rail-eu-de", "departs_at": "2026-10-01T12:00:00Z",
             "arrives_at": "2026-10-02T08:00:00Z"},
        ]
        orch.register_shipment(request_id="ship-1", actor_id="operator-001",
                               cargo_id="cargo-001", customer_id="cust-a", origin="CN",
                               destination="DE", tags=["general"], weight=12.5,
                               legs=original_legs)
        orch.register_shipment(request_id="ship-2", actor_id="operator-001",
                               cargo_id="cargo-002", customer_id="cust-b", origin="CN",
                               destination="DE", tags=["lithium"], weight=2.0,
                               legs=original_legs)

        # ---- 中断检测（11:00）
        clock._value = datetime(2026, 9, 25, 11, 0, tzinfo=timezone.utc)
        detected = orch.detect_disruption(actor_id="operator-001", advisory_id="adv-ala")

        # ---- 方案生成：普货可海运，锂电池无可行路径
        clock._value = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)
        candidates = orch.evaluate_candidates(actor_id="operator-001", cargo_id="cargo-001")
        lithium = orch.evaluate_candidates(actor_id="operator-001", cargo_id="cargo-002")
        feasible_modes = sorted({leg["mode"] for leg in candidates["feasible"][0]["legs"]})
        orch.create_plan(request_id="plan-1", actor_id="operator-001",
                         cargo_id="cargo-001", advisory_id="adv-ala")
        plan_id = database.connection.execute(
            "SELECT plan_id FROM plans WHERE cargo_id='cargo-001' AND terminal=0"
        ).fetchone()["plan_id"]

        # ---- 锁定与审批
        orch.lock_plan(request_id="lock-1", actor_id="operator-001", plan_id=plan_id)
        orch.approve_plan(request_id="ap-ops", actor_id="operator-001", plan_id=plan_id,
                          key="operations_duty")
        orch.approve_plan(request_id="ap-rev", actor_id="reviewer-001", plan_id=plan_id,
                          key="customer_window", note="客户接受改期")
        orch.approve_plan(request_id="ap-fin", actor_id="finance-001", plan_id=plan_id,
                          key="finance")
        orch.commit_plan(request_id="commit-1", actor_id="operator-001", plan_id=plan_id)

        # ---- 交接后迟到恢复消息不拉回
        orch.mark_handover(request_id="hand-1", actor_id="operator-001",
                           cargo_id="cargo-001")
        orch.record_recovery(request_id="recovery-1", actor_id="operator-001",
                             advisory_id="adv-ala")
        ops = orch.ops_view(actor_id="operator-001", cargo_id="cargo-001")
        finance = orch.finance_view(actor_id="finance-001", cargo_id="cargo-001")
        past = orch.reconstruct("2026-09-25T10:30:00Z", cargo_id="cargo-001")

        valid, event_count = service.verify_audit()
        records = service.list_domain_data("site-001")
        result = {
            "status": "ok",
            "records": len(records),
            "audit_events": event_count,
            "audit_valid": valid,
            "first_replayed": first.replayed,
            "second_replayed": replay.replayed,
            "impacted": detected["total"],
            "reroute_modes": feasible_modes,
            "lithium_feasible": bool(lithium["feasible"]),
            "switched_state": ops["items"][0]["state"],
            "switched_eta": ops["items"][0]["eta"],
            "cost_delta": finance["reroute_differences"][0]["delta"],
            "past_state": past["shipments"][0]["state"],
        }
        database.close()
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    expectations = {
        "status": "ok", "audit_valid": True,
        "first_replayed": False, "second_replayed": True,
        "impacted": 2, "lithium_feasible": False,
        "switched_state": "handed_over",
        "switched_eta": "2026-10-06T18:00:00Z", "cost_delta": 8500.0,
        "past_state": "booked",
    }
    return 0 if all(result[key] == value for key, value in expectations.items()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
