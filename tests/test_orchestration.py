"""国际通道韧性编排服务的集成测试。"""

import unittest
from datetime import datetime, timezone

from transport_coordination.clock import FixedClock
from transport_coordination.errors import (ApprovalRequired, ConflictError,
                                           LockFailed, NotFoundError,
                                           PermissionDenied)
from transport_coordination.orchestration import OrchestrationService
from transport_coordination.service import DomainService
from transport_coordination.storage import Database


class OrchestrationTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = FixedClock(datetime(2026, 9, 25, 9, 0, tzinfo=timezone.utc))
        self.domain = DomainService(self.database, self.clock)
        self.orch = OrchestrationService(self.database, self.clock)
        self._bootstrap()
        self._publish_network()
        self.clock._value = datetime(2026, 9, 25, 10, 0, tzinfo=timezone.utc)
        self._register_cargo("c1", ["general"])
        self._register_cargo("c2", ["lithium"])

    def tearDown(self):
        self.database.close()

    # ------------------------------------------------------------------ 装配

    def _bootstrap(self):
        d = self.domain
        d.register_organization(request_id="org", actor_id="bootstrap",
                                organization_id="o1", name="平台")
        d.register_actor(request_id="a-admin", actor_id="bootstrap", new_actor_id="admin",
                         display_name="管理员", role="admin", organization_id="o1")
        for key, name, role in (("ops", "运营", "operator"), ("rev", "审核", "reviewer"),
                                ("fin", "财务", "finance"), ("au", "审计", "auditor")):
            d.register_actor(request_id=f"a-{key}", actor_id="admin", new_actor_id=key,
                             display_name=name, role=role, organization_id="o1")

    def _publish_network(self, sea_slots=5):
        o = self.orch
        segments = {
            "rail-cn-xi": ("CN", "XI", "rail", None),
            "rail-xi-eu": ("XI", "EU", "rail", "border-ala"),
            "rail-eu-de": ("EU", "DE", "rail", None),
            "truck-xi-port": ("XI", "PORT", "truck", None),
            "sea-port-eu": ("PORT", "EU", "sea", "border-sea"),
            "truck-eu-de": ("EU", "DE", "truck", None),
        }
        for sid, (a, b, mode, border) in segments.items():
            o.publish_reference(request_id=f"s-{sid}", actor_id="ops", entity_type="segment",
                                entity_id=sid, valid_from="2026-09-01T00:00:00Z",
                                payload={"origin": a, "destination": b, "mode": mode,
                                         "border_id": border, "capacity_id": f"cap-{sid}"})
        caps = {
            "cap-rail-cn-xi": ("CRE", "班列", "rail-cn-xi",
                               [("2026-09-22T08:00:00Z", "2026-09-22T20:00:00Z", 20)]),
            "cap-rail-xi-eu": ("CRE", "班列", "rail-xi-eu",
                               [("2026-09-23T08:00:00Z", "2026-10-01T08:00:00Z", 20)]),
            "cap-rail-eu-de": ("CRE", "班列", "rail-eu-de",
                               [("2026-10-01T12:00:00Z", "2026-10-02T08:00:00Z", 20)]),
            "cap-truck-xi-port": ("SHT", "集卡", "truck-xi-port",
                                  [("2026-09-25T06:00:00Z", "2026-09-26T06:00:00Z", 10)]),
            "cap-sea-port-eu": ("MSC", "海运", "sea-port-eu",
                                [("2026-09-27T00:00:00Z", "2026-10-05T12:00:00Z", sea_slots)]),
            "cap-truck-eu-de": ("SHT", "接驳", "truck-eu-de",
                                [("2026-10-06T00:00:00Z", "2026-10-06T18:00:00Z", 10)]),
        }
        for cid, (carrier, name, seg, deps) in caps.items():
            o.publish_reference(request_id=f"c-{cid}", actor_id="ops", entity_type="capacity",
                                entity_id=cid, valid_from="2026-09-01T00:00:00Z",
                                payload={"segment_id": seg, "carrier_id": carrier,
                                         "carrier_name": name,
                                         "departures": [{"departs_at": x, "arrives_at": y,
                                                         "slots": n} for x, y, n in deps]})
        o.publish_reference(request_id="b-ala", actor_id="ops", entity_type="border_calendar",
                            entity_id="border-ala", valid_from="2026-09-01T00:00:00Z",
                            payload={"closures": [
                                {"from": "2026-09-23T00:00:00Z",
                                 "to": "2026-09-26T23:59:59Z",
                                 "reason": "临时限流", "segment_id": "rail-xi-eu"}]})
        o.publish_reference(request_id="b-sea", actor_id="ops", entity_type="border_calendar",
                            entity_id="border-sea", valid_from="2026-09-01T00:00:00Z",
                            payload={"closures": []})
        o.publish_reference(request_id="x-li", actor_id="ops", entity_type="cargo_restriction",
                            entity_id="rest-li", valid_from="2026-09-01T00:00:00Z",
                            payload={"scope": "cargo_tag", "target_id": "lithium",
                                     "forbidden_modes": ["sea"], "forbidden_segments": [],
                                     "document_required": "MSDS"})
        for cargo_id in ("c1", "c2"):
            o.publish_reference(request_id=f"d-{cargo_id}", actor_id="ops",
                                entity_type="customer_deadline", entity_id=cargo_id,
                                valid_from="2026-09-01T00:00:00Z",
                                payload={"ready_at": "2026-09-18T00:00:00Z",
                                         "due_at": "2026-10-10T23:59:59Z"})
        rates = {
            "CRE": (0.5, 800, {"rail-cn-xi": 3000, "rail-xi-eu": 12000, "rail-eu-de": 2500}),
            "SHT": (0.3, 300, {"truck-xi-port": 2000, "truck-eu-de": 3000}),
            "MSC": (0.8, 1500, {"sea-port-eu": 18000}),
        }
        for carrier, (penalty, rebook, seg_rates) in rates.items():
            o.publish_reference(request_id=f"r-{carrier}", actor_id="fin",
                                entity_type="contract_rate", entity_id=f"rate-{carrier}",
                                valid_from="2026-09-01T00:00:00Z",
                                payload={"carrier_id": carrier, "cancel_penalty_rate": penalty,
                                         "rebooking_fee": rebook, "currency": "CNY",
                                         "segment_rates": seg_rates})
        o.publish_reference(request_id="adv-1", actor_id="ops", entity_type="advisory",
                            entity_id="adv-ala", valid_from="2026-09-23T00:05:00Z",
                            payload={"level": "severe",
                                     "affected": {"segments": ["rail-xi-eu"], "capacities": []},
                                     "window": {"from": "2026-09-23T00:00:00Z",
                                                "to": "2026-09-26T23:59:59Z"},
                                     "summary": "口岸临时限流"})

    def _register_cargo(self, cargo_id, tags, weight=12.5):
        self.orch.register_shipment(
            request_id=f"reg-{cargo_id}", actor_id="ops", cargo_id=cargo_id,
            customer_id="cust", origin="CN", destination="DE", tags=tags, weight=weight,
            legs=[
                {"segment_id": "rail-cn-xi", "departs_at": "2026-09-22T08:00:00Z",
                 "arrives_at": "2026-09-22T20:00:00Z", "status": "departed"},
                {"segment_id": "rail-xi-eu", "departs_at": "2026-09-23T08:00:00Z",
                 "arrives_at": "2026-10-01T08:00:00Z"},
                {"segment_id": "rail-eu-de", "departs_at": "2026-10-01T12:00:00Z",
                 "arrives_at": "2026-10-02T08:00:00Z"},
            ])

    def _open_plan_id(self, cargo_id="c1", request_id="plan-c1"):
        self.orch.create_plan(request_id=request_id, actor_id="ops", cargo_id=cargo_id,
                              advisory_id="adv-ala")
        return self.database.connection.execute(
            "SELECT plan_id FROM plans WHERE cargo_id=? AND terminal=0",
            (cargo_id,)).fetchone()["plan_id"]

    def _full_commit(self, plan_id, prefix="p"):
        self.orch.lock_plan(request_id=f"{prefix}-lock", actor_id="ops", plan_id=plan_id)
        self.orch.approve_plan(request_id=f"{prefix}-a1", actor_id="ops", plan_id=plan_id,
                               key="operations_duty")
        self.orch.approve_plan(request_id=f"{prefix}-a2", actor_id="rev", plan_id=plan_id,
                               key="customer_window")
        self.orch.approve_plan(request_id=f"{prefix}-a3", actor_id="fin", plan_id=plan_id,
                               key="finance")
        self.orch.commit_plan(request_id=f"{prefix}-commit", actor_id="ops", plan_id=plan_id)

    # ------------------------------------------------------------------ 用例

    def test_reference_versions_are_historical(self):
        self.orch.publish_reference(
            request_id="b-ala-v2", actor_id="ops", entity_type="border_calendar",
            entity_id="border-ala",
            payload={"closures": [{"from": "2026-10-01T00:00:00Z",
                                   "to": "2026-10-02T00:00:00Z",
                                   "reason": "节后检查", "segment_id": "rail-xi-eu"}]},
            valid_from="2026-09-30T00:00:00Z")
        current = self.orch.get_reference("border_calendar", "border-ala",
                                          at="2026-10-01T12:00:00Z")
        old = self.orch.get_reference("border_calendar", "border-ala",
                                      at="2026-09-25T00:00:00Z")
        self.assertEqual(2, current["version"])
        self.assertEqual(1, old["version"])
        self.assertIn("临时限流", old["payload"]["closures"][0]["reason"])

    def test_disruption_detects_each_affected_cargo_once(self):
        first = self.orch.detect_disruption(actor_id="ops", advisory_id="adv-ala")
        second = self.orch.detect_disruption(actor_id="ops", advisory_id="adv-ala")
        self.assertEqual(2, first["total"])
        self.assertEqual(0, second["total"])
        impacts = self.orch.list_impacts("adv-ala")
        self.assertEqual({"c1", "c2"}, {row["cargo_id"] for row in impacts})

    def test_planner_explains_feasible_and_rejected(self):
        result = self.orch.evaluate_candidates(actor_id="ops", cargo_id="c1")
        self.assertTrue(result["feasible"])
        self.assertTrue(result["current_assessment"]["blocked"])
        modes = {leg["mode"] for leg in result["feasible"][0]["legs"]}
        self.assertEqual({"truck", "sea"}, modes)
        self.assertIn(23000.0, [c["new_total"] for c in result["feasible"]])
        lithium = self.orch.evaluate_candidates(actor_id="ops", cargo_id="c2")
        self.assertFalse(lithium["feasible"])
        reasons = " ".join(reason for item in lithium["rejected"]
                           for reason in item["violations"])
        self.assertIn("无可用班期", reasons)

    def test_only_one_active_competing_plan(self):
        self._open_plan_id()
        with self.assertRaises(ConflictError):
            self._open_plan_id(request_id="plan-c1-alt")

    def test_cannot_approve_or_commit_before_lock(self):
        plan_id = self._open_plan_id()
        with self.assertRaises(ConflictError):
            self.orch.approve_plan(request_id="x", actor_id="ops", plan_id=plan_id,
                                   key="operations_duty")
        with self.assertRaises(ConflictError):
            self.orch.commit_plan(request_id="y", actor_id="ops", plan_id=plan_id)

    def test_partial_lock_failure_releases_everything(self):
        # 用新版本把海运营位收紧到 1：c1 先提出方案（尚有余位），
        # 随后 c3 抢先锁定唯一舱位，c1 锁定必须在海运段失败并整体回滚。
        self.orch.publish_reference(
            request_id="cap-sea-tight", actor_id="ops", entity_type="capacity",
            entity_id="cap-sea-port-eu",
            payload={"segment_id": "sea-port-eu", "carrier_id": "MSC", "carrier_name": "海运",
                     "departures": [{"departs_at": "2026-09-27T00:00:00Z",
                                     "arrives_at": "2026-10-05T12:00:00Z", "slots": 1}]},
            valid_from="2026-09-25T09:30:00Z")
        plan_id = self._open_plan_id(request_id="plan-c1")
        self._register_cargo("c3", ["general"])
        c3 = self._open_plan_id("c3", "plan-c3")
        self.orch.lock_plan(request_id="lock-c3", actor_id="ops", plan_id=c3)

        with self.assertRaises(LockFailed):
            self.orch.lock_plan(request_id="lock-c1", actor_id="ops", plan_id=plan_id)
        locked = self.database.connection.execute(
            "SELECT COUNT(*) n FROM capacity_holds WHERE plan_id=? AND status='locked'",
            (plan_id,)).fetchone()["n"]
        self.assertEqual(0, locked)
        failed = self.orch.get_plan(plan_id)
        self.assertEqual("failed", failed["status"])
        self.assertTrue(failed["terminal"])
        self.assertIn("余位不足", "；".join(failed["failure"]["reasons"]))

        # c3 撤销释放后，c1 可以重新提出方案并锁定成功。
        self.orch.cancel_plan(request_id="cancel-c3", actor_id="ops", plan_id=c3)
        again = self._open_plan_id(request_id="plan-c1-retry")
        self.orch.lock_plan(request_id="lock-c1-retry", actor_id="ops", plan_id=again)

    def test_approval_gate_and_role_boundaries(self):
        plan_id = self._open_plan_id()
        self.orch.lock_plan(request_id="lock", actor_id="ops", plan_id=plan_id)
        with self.assertRaises(PermissionDenied):
            self.orch.approve_plan(request_id="bad-fin", actor_id="ops", plan_id=plan_id,
                                   key="finance")
        self.orch.approve_plan(request_id="a1", actor_id="ops", plan_id=plan_id,
                               key="operations_duty")
        self.orch.approve_plan(request_id="a2", actor_id="rev", plan_id=plan_id,
                               key="customer_window")
        with self.assertRaises(ApprovalRequired):
            self.orch.commit_plan(request_id="early", actor_id="ops", plan_id=plan_id)
        self.orch.approve_plan(request_id="a3", actor_id="fin", plan_id=plan_id,
                               key="finance")
        self.orch.commit_plan(request_id="commit", actor_id="ops", plan_id=plan_id)
        self.assertEqual("committed", self.orch.get_plan(plan_id)["status"])

    def test_commit_releases_unused_capacity_but_keeps_all_costs(self):
        plan_id = self._open_plan_id()
        self._full_commit(plan_id)
        states = {row["status"]: row["n"] for row in self.database.connection.execute(
            "SELECT status, COUNT(*) n FROM capacity_holds WHERE cargo_id='c1' GROUP BY status")}
        # 原班列：第一段 consumed，二三段 released；海运方案三段 consumed。
        self.assertEqual(4, states["consumed"])
        self.assertEqual(2, states["released"])
        ledger = self.orch.finance_view(actor_id="fin", cargo_id="c1")
        categories = {entry["category"] for entry in ledger["ledger"]}
        self.assertEqual({"booking", "new_booking", "cancellation_penalty", "rebooking_fee"},
                         categories)
        # 原始预订费用一条不少。
        booking_total = sum(e["amount"] for e in ledger["ledger"]
                            if e["category"] == "booking")
        self.assertEqual(17500.0, booking_total)
        diff = ledger["reroute_differences"][0]
        self.assertEqual(8500.0, diff["delta"])
        ops = self.orch.ops_view(actor_id="ops", cargo_id="c1")
        item = ops["items"][0]
        self.assertEqual("switched", item["state"])
        self.assertEqual("集卡", item["responsible_party"])
        self.assertEqual("2026-10-06T18:00:00Z", item["eta"])
        self.assertEqual("adv-ala", item["reroute_basis"]["advisory_id"])

    def test_late_recovery_does_not_pull_handed_cargo_back(self):
        self.orch.detect_disruption(actor_id="ops", advisory_id="adv-ala")
        plan_id = self._open_plan_id()
        self._full_commit(plan_id)
        self.orch.mark_handover(request_id="hand", actor_id="ops", cargo_id="c1")
        self.orch.record_recovery(request_id="rec", actor_id="ops", advisory_id="adv-ala")
        item = self.orch.ops_view(actor_id="ops", cargo_id="c1")["items"][0]
        self.assertEqual("handed_over", item["state"])
        self.assertEqual(plan_id, item["active_plan_id"])
        impact = next(row for row in self.orch.list_impacts("adv-ala")
                      if row["cargo_id"] == "c1")
        self.assertEqual("ignored_handed_over", impact["recovery_action"])

    def test_recovery_before_commit_cancels_plan_and_frees_capacity(self):
        self.orch.detect_disruption(actor_id="ops", advisory_id="adv-ala")
        plan_id = self._open_plan_id()
        self.orch.lock_plan(request_id="lock", actor_id="ops", plan_id=plan_id)
        self.orch.record_recovery(request_id="rec-early", actor_id="ops",
                                  advisory_id="adv-ala")
        self.assertEqual("cancelled", self.orch.get_plan(plan_id)["status"])
        locked = self.database.connection.execute(
            "SELECT COUNT(*) n FROM capacity_holds WHERE plan_id=? AND status='locked'",
            (plan_id,)).fetchone()["n"]
        self.assertEqual(0, locked)
        item = self.orch.ops_view(actor_id="ops", cargo_id="c1")["items"][0]
        self.assertIsNone(item["open_plan"])

    def test_recheck_blocks_commit_when_new_restriction_appears(self):
        plan_id = self._open_plan_id()
        self.orch.lock_plan(request_id="lock", actor_id="ops", plan_id=plan_id)
        # 锁定之后海上突发恶劣天气，覆盖已锁定船期。
        self.clock._value = datetime(2026, 9, 25, 16, 0, tzinfo=timezone.utc)
        self.orch.publish_reference(
            request_id="adv-storm", actor_id="ops", entity_type="advisory",
            entity_id="adv-storm",
            payload={"level": "severe",
                     "affected": {"segments": ["sea-port-eu"], "capacities": []},
                     "window": {"from": "2026-09-26T00:00:00Z",
                                "to": "2026-09-30T23:59:59Z"},
                     "summary": "海上大风停航"},
            valid_from="2026-09-25T15:00:00Z")
        self.orch.approve_plan(request_id="a1", actor_id="ops", plan_id=plan_id,
                               key="operations_duty")
        with self.assertRaises(LockFailed):
            self.orch.commit_plan(request_id="commit", actor_id="ops", plan_id=plan_id)
        detail = self.orch.get_plan(plan_id)
        self.assertEqual("failed", detail["status"])
        self.assertEqual("precommit_recheck", detail["failure"]["stage"])
        locked = self.database.connection.execute(
            "SELECT COUNT(*) n FROM capacity_holds WHERE plan_id=? AND status='locked'",
            (plan_id,)).fetchone()["n"]
        self.assertEqual(0, locked)
        # 失败闭合不产生任何新费用。
        fees = self.database.connection.execute(
            "SELECT COUNT(*) n FROM cost_entries WHERE plan_id=?", (plan_id,)).fetchone()["n"]
        self.assertEqual(0, fees)

    def test_idempotent_retry_after_state_advanced(self):
        plan_id = self._open_plan_id()
        first = self.orch.lock_plan(request_id="lock-once", actor_id="ops", plan_id=plan_id)
        second = self.orch.lock_plan(request_id="lock-once", actor_id="ops", plan_id=plan_id)
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)
        # 提交后用同一 request_id 再提交，也返回原始回执而不是状态冲突。
        self.orch.approve_plan(request_id="a1", actor_id="ops", plan_id=plan_id,
                               key="operations_duty")
        self.orch.approve_plan(request_id="a2", actor_id="rev", plan_id=plan_id,
                               key="customer_window")
        self.orch.approve_plan(request_id="a3", actor_id="fin", plan_id=plan_id,
                               key="finance")
        commit = self.orch.commit_plan(request_id="commit-once", actor_id="ops",
                                       plan_id=plan_id)
        retry = self.orch.commit_plan(request_id="commit-once", actor_id="ops",
                                      plan_id=plan_id)
        self.assertFalse(commit.replayed)
        self.assertTrue(retry.replayed)

    def test_reconstruct_historical_points(self):
        before = self.orch.reconstruct("2026-09-25T09:30:00Z", cargo_id="c1")
        self.assertEqual([], before["shipments"])
        registered = self.orch.reconstruct("2026-09-25T10:30:00Z", cargo_id="c1")
        self.assertEqual("booked", registered["shipments"][0]["state"])
        self.assertEqual(3, len(registered["shipments"][0]["legs"]))

        self.clock._value = datetime(2026, 9, 25, 11, 0, tzinfo=timezone.utc)
        self.orch.detect_disruption(actor_id="ops", advisory_id="adv-ala")
        impacted = self.orch.reconstruct("2026-09-25T11:30:00Z", cargo_id="c1")
        self.assertTrue(impacted["impacts"])

        plan_id = self._open_plan_id()
        self._full_commit(plan_id)
        self.orch.mark_handover(request_id="hand", actor_id="ops", cargo_id="c1")
        later = self.orch.reconstruct("2026-10-07T00:00:00Z", cargo_id="c1")
        shipment = later["shipments"][0]
        self.assertEqual("handed_over", shipment["state"])
        self.assertEqual(["rail-cn-xi", "truck-xi-port", "sea-port-eu", "truck-eu-de"],
                         [leg["segment_id"] for leg in shipment["legs"]])
        committed = next(p for p in shipment["plans"] if p["plan_id"] == plan_id)
        self.assertEqual("committed", committed["status"])
        # 方案刚创建尚未锁定的那一刻也可复原。
        self.clock._value = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)

    def test_views_enforce_separation_of_duties(self):
        with self.assertRaises(PermissionDenied):
            self.orch.ops_view(actor_id="fin")
        with self.assertRaises(PermissionDenied):
            self.orch.finance_view(actor_id="ops")
        with self.assertRaises(PermissionDenied):
            self.orch.ops_view(actor_id="au")
        ops = self.orch.ops_view(actor_id="rev")
        self.assertEqual(2, len(ops["items"]))
        finance = self.orch.finance_view(actor_id="admin")
        self.assertEqual(6, len(finance["ledger"]))

    def test_finance_cannot_publish_operational_reference(self):
        with self.assertRaises(PermissionDenied):
            self.orch.publish_reference(
                request_id="x", actor_id="fin", entity_type="segment",
                entity_id="seg-x", payload={"origin": "A", "destination": "B", "mode": "rail"})

    def test_handover_required_before_delivery(self):
        with self.assertRaises(ConflictError):
            self.orch.mark_delivered(request_id="d", actor_id="ops", cargo_id="c1")
        self.orch.mark_handover(request_id="h", actor_id="ops", cargo_id="c1")
        self.orch.mark_delivered(request_id="d", actor_id="ops", cargo_id="c1")
        self.assertEqual("delivered",
                         self.orch.ops_view(actor_id="ops", cargo_id="c1")
                         ["items"][0]["state"])

    def test_unknown_advisory_detection_raises_not_found(self):
        with self.assertRaises(NotFoundError):
            self.orch.detect_disruption(actor_id="ops", advisory_id="nope")


if __name__ == "__main__":
    unittest.main()
