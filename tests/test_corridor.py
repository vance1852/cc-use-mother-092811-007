import unittest
from datetime import datetime, timezone

from transport_coordination.clock import FixedClock
from transport_coordination.corridor import CorridorOrchestrator
from transport_coordination.errors import ConflictError, PermissionDenied, ValidationError
from transport_coordination.service import DomainService
from transport_coordination.storage import Database


class CorridorTestBase(unittest.TestCase):
    """搭建组织、四类角色与最小两通道网络。"""

    def setUp(self):
        self.database = Database()
        self.clock = FixedClock(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc))
        self.domain = DomainService(self.database, self.clock)
        self.oc = CorridorOrchestrator(self.database, self.clock)
        self.domain.register_organization(request_id="org1", actor_id="bootstrap",
                                          organization_id="org", name="平台")
        self.domain.register_actor(request_id="ra", actor_id="bootstrap", new_actor_id="admin",
                                   display_name="管理员", role="admin", organization_id="org")
        for rid, aid, role, name in [
                ("rop", "op", "operator", "运营"),
                ("rfi", "fi", "finance", "财务"),
                ("rsa", "sa", "safety", "安全"),
                ("rau", "au", "auditor", "审计")]:
            self.domain.register_actor(request_id=rid, actor_id="admin", new_actor_id=aid,
                                       display_name=name, role=role, organization_id="org")
        self._network()

    def tearDown(self):
        self.database.close()

    def tick(self, value):
        self.clock._value = datetime.fromisoformat(value.replace("Z", "+00:00"))

    def ref(self, kind, data, rid, actor="op"):
        return self.oc.publish_reference(request_id=rid, actor_id=actor, kind=kind, data=data)

    def _segments(self):
        specs = [
            ("g1", "a", "x", "rail", 10, "bc1"),
            ("g2", "x", "b", "rail", 10, None),
            ("h1", "a", "p", "sea", 30, None),
            ("h2", "p", "b", "road", 5, None),
        ]
        for i, (sid, frm, to, mode, hours, bc) in enumerate(specs):
            self.ref("segment", {"segment_id": sid, "from_node": frm, "to_node": to,
                                 "mode": mode, "transit_hours": hours,
                                 **({"border_crossing_id": bc} if bc else {})}, f"sg{i}")

    def _capacities(self):
        specs = [
            ("cap_g1", "g1", "carrier_rail", 10, 50, 100),
            ("cap_g2", "g2", "carrier_rail2", 10, 50, 100),
            ("cap_h1", "h1", "carrier_sea", 10, 400, 100),
            ("cap_h2", "h2", "carrier_road", 1, 100, 70),
        ]
        for i, (cid, seg, carrier, units, cost, fee) in enumerate(specs):
            self.ref("capacity", {"capacity_id": cid, "segment_id": seg, "carrier_id": carrier,
                                  "carrier_name": carrier.upper(),
                                  "valid_from": "2026-09-01T00:00Z",
                                  "valid_to": "2026-12-31T00:00Z", "unit": "FEU",
                                  "capacity_units": units, "unit_cost": cost,
                                  "commitment_fee": fee, "currency": "USD"}, f"cp{i}")

    def _rates(self):
        specs = [
            ("rate_g1", "g1", "carrier_rail", 100),
            ("rate_g2", "g2", "carrier_rail2", 100),
            ("rate_h1", "h1", "carrier_sea", 400),
            ("rate_h2", "h2", "carrier_road", 100),
        ]
        for i, (rid, seg, carrier, cost) in enumerate(specs):
            self.ref("contract_rate", {"rate_id": rid, "segment_id": seg, "carrier_id": carrier,
                                       "valid_from": "2026-09-01T00:00Z",
                                       "valid_to": "2026-12-31T00:00Z", "unit_cost": cost,
                                       "fixed_cost": 0, "minimum_charge": 0,
                                       "currency": "USD"}, f"rt{i}", actor="fi")

    def _network(self):
        self._segments()
        self._capacities()
        self._rates()

    def shipment(self, sid="s1", hazmat=None, deadline="2026-10-20T00:00Z", rid="sh1", units=1):
        return self.oc.register_shipment(
            request_id=rid, actor_id="op", shipment_id=sid,
            cargo_type="general", units=units, origin_node="a", destination_node="b",
            customer_ref=f"C-{sid}", must_arrive_by=deadline, hazmat_class=hazmat)

    def baseline(self, sid="s1", rid="bl1"):
        return self.oc.set_baseline(request_id=rid, actor_id="op", shipment_id=sid,
                                    path=["g1", "g2"], depart_at="2026-10-02T00:00Z")

    def notice(self, impact=None, nid="n1", rid="n1r"):
        return self.oc.publish_notice(
            request_id=rid, actor_id="op", notice_id=nid, title="口岸中断",
            severity="critical", detail="限流",
            impact=impact or {"border_crossings": ["bc1"]})

    def evaluate(self, sid=None):
        return self.oc.evaluate_disruption(actor_id="op", notice_id="n1", shipment_id=sid)

    def feasible_candidates(self, evaluation, sid="s1"):
        entry = next(s for s in evaluation["shipments"] if s["shipment_id"] == sid)
        return [c for c in entry["candidates"] if c["feasible"]]


class VersionedReferenceTest(CorridorTestBase):
    def test_reference_versions_and_history_visibility(self):
        first = self.ref("capacity", {"capacity_id": "cap_g1", "segment_id": "g1",
                                      "carrier_id": "carrier_rail",
                                      "valid_from": "2026-09-01T00:00Z",
                                      "valid_to": "2026-12-31T00:00Z", "unit": "FEU",
                                      "capacity_units": 5, "unit_cost": 50,
                                      "commitment_fee": 0, "currency": "USD"}, "v1")
        self.assertEqual(first["version"], 2)  # setUp 已发布 v1
        self.tick("2026-10-02T00:00:00Z")
        self.ref("capacity", {"capacity_id": "cap_g1", "segment_id": "g1",
                              "carrier_id": "carrier_rail",
                              "valid_from": "2026-09-01T00:00Z",
                              "valid_to": "2026-12-31T00:00Z", "unit": "FEU",
                              "capacity_units": 50, "unit_cost": 50,
                              "commitment_fee": 0, "currency": "USD"}, "v2")
        snap = self.oc.history_snapshot(actor_id="op", at="2026-10-01T12:00:00Z")
        cap = next(r for r in snap["references"]["capacity"] if r["entity_id"] == "cap_g1")
        self.assertEqual(cap["version"], 2)
        self.assertEqual(cap["payload"]["capacity_units"], 5)

    def test_contract_rate_requires_finance(self):
        with self.assertRaises(PermissionDenied):
            self.ref("contract_rate", {"rate_id": "rx", "segment_id": "g1",
                                       "carrier_id": "carrier_rail",
                                       "valid_from": "2026-09-01T00:00Z",
                                       "valid_to": "2026-12-31T00:00Z", "unit_cost": 1}, "rx1")

    def test_invalid_reference_rejected(self):
        with self.assertRaises(ValidationError):
            self.ref("segment", {"segment_id": "bad", "from_node": "a", "to_node": "b",
                                 "mode": "teleport", "transit_hours": 1}, "bad")
        with self.assertRaises(ValidationError):
            self.ref("border_calendar", {"border_crossing_id": "bc1", "closed_intervals": [
                {"from": "2026-10-05T00:00Z", "to": "2026-10-04T00:00Z"}]}, "bad2")


class EvaluationTest(CorridorTestBase):
    def test_disruption_lists_affected_and_explains_candidates(self):
        self.shipment()
        self.baseline()
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        ev = self.evaluate()
        entry = ev["shipments"][0]
        self.assertTrue(entry["affected"])
        self.assertEqual(entry["reroute_basis"]["blocked_on_baseline"][0]["point"], "bc1")
        sea = next(c for c in entry["candidates"] if c["path"] == ["h1", "h2"])
        self.assertTrue(sea["feasible"])
        rail = next(c for c in entry["candidates"] if c["path"] == ["g1", "g2"])
        self.assertFalse(rail["feasible"])
        self.assertIn("被风险通告中断", rail["violations"][0])

    def test_cargo_restriction_blocks_road(self):
        self.ref("cargo_restriction", {"restriction_id": "no_road_haz", "scope": {"mode": "road"},
                                       "forbidden_classes": ["class3"],
                                       "forbidden_cargo_types": [],
                                       "note": "公路禁运", "status": "active"}, "rx", actor="sa")
        self.shipment(hazmat="class3")
        self.baseline()
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        self.assertEqual(self.feasible_candidates(self.evaluate()), [])

    def test_capacity_shortfall_is_explained(self):
        # 第二票货物占满公路运力（1 FEU），第一票无法走海运接驳
        self.shipment("s1", rid="sh1")
        self.shipment("s2", deadline="2026-11-20T00:00Z", rid="sh2")
        self.baseline("s1", "bl1")
        self.oc.set_baseline(request_id="bl2", actor_id="op", shipment_id="s2",
                             path=["h1", "h2"], depart_at="2026-10-02T00:00Z")
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        ev = self.evaluate("s1")
        sea = next(c for c in ev["shipments"][0]["candidates"] if c["path"] == ["h1", "h2"])
        self.assertFalse(sea["feasible"])
        self.assertTrue(any("余量不足" in v for v in sea["violations"]))

    def test_deadline_miss_marks_infeasible(self):
        # 交付窗在中断评估时刻（10-03 00:00）后仅 1 小时，任何路径都无法准时
        self.shipment(deadline="2026-10-03T01:00:00Z")
        self.baseline()
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        self.assertEqual(self.feasible_candidates(self.evaluate()), [])

    def test_unaffected_shipment_excluded(self):
        self.shipment("s1", rid="sh1")
        self.shipment("s2", deadline="2026-11-20T00:00Z", rid="sh2")
        self.baseline("s1", "bl1")
        self.oc.set_baseline(request_id="bl2", actor_id="op", shipment_id="s2",
                             path=["h1", "h2"], depart_at="2026-10-02T00:00Z")
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        affected = {s["shipment_id"] for s in self.evaluate()["shipments"] if s["affected"]}
        self.assertEqual(affected, {"s1"})

    def test_border_calendar_wait_counts_toward_deadline(self):
        # 例行口岸日历关闭 5 天：铁路路径须等待，叠加交付窗后违约
        self.ref("border_calendar", {"border_crossing_id": "bc1", "closed_intervals": [
            {"from": "2026-10-03T00:00:00Z", "to": "2026-10-08T00:00:00Z",
             "reason": "例行检修"}]}, "cal1")
        self.shipment(deadline="2026-10-05T00:00:00Z")
        self.baseline()
        self.tick("2026-10-03T00:00:00Z")
        # 通告不直接命中任何点，仅口岸日历影响班期
        self.notice(impact={})
        ev = self.evaluate()
        rail = next(c for c in ev["shipments"][0]["candidates"] if c["path"] == ["g1", "g2"])
        self.assertFalse(rail["feasible"])
        self.assertTrue(any("交付窗" in v for v in rail["violations"]))
        g1 = next(leg for leg in next(
            p for p in self.oc.list_plans(actor_id="op", shipment_id="s1")["items"]
            if p["plan_id"] == rail["plan_id"])["legs"] if leg["segment_id"] == "g1")
        self.assertEqual(g1["wait_hours"], 120.0)

    def test_capacity_window_outside_validity_blocks_path(self):
        # 先用有效班期设定基线，再发布已失效的能力新版本
        self.shipment()
        self.baseline()
        self.tick("2026-10-03T00:00:00Z")
        for i, (cid, seg, carrier) in enumerate([
                ("cap_g1", "g1", "carrier_rail"), ("cap_g2", "g2", "carrier_rail2")]):
            self.ref("capacity", {"capacity_id": cid, "segment_id": seg, "carrier_id": carrier,
                                  "carrier_name": carrier,
                                  "valid_from": "2026-09-01T00:00Z",
                                  "valid_to": "2026-09-15T00:00Z", "unit": "FEU",
                                  "capacity_units": 10, "unit_cost": 50,
                                  "commitment_fee": 0, "currency": "USD"}, f"cw{i}")
        self.notice(impact={})
        rail = next(c for c in self.evaluate()["shipments"][0]["candidates"]
                    if c["path"] == ["g1", "g2"])
        self.assertFalse(rail["feasible"])
        self.assertTrue(any("班期不覆盖" in v for v in rail["violations"]))


class SwitchSagaTest(CorridorTestBase):
    def _prepare_sea_plan(self):
        self.shipment()
        self.baseline()
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        ev = self.evaluate()
        sea = self.feasible_candidates(ev)[0]
        self.assertEqual(sea["path"], ["h1", "h2"])
        return sea

    def test_switch_requires_approvals_then_succeeds(self):
        sea = self._prepare_sea_plan()
        self.oc.select_plan(request_id="sel", actor_id="op", plan_id=sea["plan_id"])
        with self.assertRaises(PermissionDenied):
            self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])
        # 海运成本高于铁路，需要 admin + finance
        self.oc.approve_plan(request_id="a1", actor_id="admin", plan_id=sea["plan_id"])
        with self.assertRaises(PermissionDenied):
            self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a2", actor_id="fi", plan_id=sea["plan_id"])
        result = self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])
        self.assertEqual(result["status"], "effective")
        route = self.database.connection.execute(
            "SELECT * FROM corridor_shipment_routes WHERE shipment_id='s1'").fetchone()
        self.assertEqual(route["effective_plan_id"], sea["plan_id"])
        self.assertEqual(route["epoch"], 2)
        # 旧基线能力被释放，新能力 committed
        old = {r["state"] for r in self.database.connection.execute(
            "SELECT state FROM corridor_holds WHERE plan_id=(SELECT baseline_plan_id "
            "FROM corridor_shipment_routes WHERE shipment_id='s1')")}
        self.assertTrue(old <= {"released_after_switch", "replaced_relocked"})
        self.assertTrue(result["sunk_commitment"] > 0)

    def test_switch_is_idempotent(self):
        sea = self._prepare_sea_plan()
        self.oc.select_plan(request_id="sel", actor_id="op", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a1", actor_id="admin", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a2", actor_id="fi", plan_id=sea["plan_id"])
        first = self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])
        replay = self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])
        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])

    def test_only_one_selected_plan_per_shipment(self):
        sea = self._prepare_sea_plan()
        self.oc.select_plan(request_id="sel1", actor_id="op", plan_id=sea["plan_id"])
        ev2 = self.evaluate()
        other = self.feasible_candidates(ev2)[0]
        with self.assertRaises(ConflictError):
            self.oc.select_plan(request_id="sel2", actor_id="op", plan_id=other["plan_id"])
        # 切换成功后，其余候选作废，全货物只有一个生效方案
        self.oc.approve_plan(request_id="a1", actor_id="admin", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a2", actor_id="fi", plan_id=sea["plan_id"])
        self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])
        statuses = [r["status"] for r in self.database.connection.execute(
            "SELECT status FROM corridor_plans WHERE shipment_id='s1'")]
        self.assertEqual(statuses.count("effective"), 1)
        self.assertIn("obsolete", statuses)

    def test_partial_failure_releases_holds_but_keeps_fee(self):
        sea = self._prepare_sea_plan()
        self.oc.select_plan(request_id="sel", actor_id="op", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a1", actor_id="admin", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a2", actor_id="fi", plan_id=sea["plan_id"])
        # 占满公路运力（h2 仅 1 FEU）
        self.shipment("s2", deadline="2026-11-20T00:00Z", rid="sh2")
        self.oc.set_baseline(request_id="bl2", actor_id="op", shipment_id="s2",
                             path=["h1", "h2"], depart_at="2026-10-02T00:00Z")
        with self.assertRaises(ConflictError):
            self.oc.execute_switch(request_id="swfail", actor_id="op", plan_id=sea["plan_id"])
        holds = self.database.connection.execute(
            "SELECT capacity_id,state FROM corridor_holds WHERE plan_id=?",
            (sea["plan_id"],)).fetchall()
        self.assertTrue(all(r["state"] == "released" for r in holds))
        self.assertIn("cap_h1", [r["capacity_id"] for r in holds])
        fee = self.database.connection.execute(
            "SELECT amount,fee_type FROM corridor_fees WHERE plan_id=?",
            (sea["plan_id"],)).fetchone()
        self.assertEqual(fee["fee_type"], "commitment_forfeited")
        self.assertEqual(fee["amount"], 100)
        # 能力释放回池：h1 不被失败方案占用
        occupied = self.database.connection.execute(
            "SELECT COALESCE(SUM(units),0) AS u FROM corridor_holds "
            "WHERE capacity_id='cap_h1' AND state IN ('committed','held')").fetchone()["u"]
        self.assertEqual(occupied, 1)  # 仅 s2 的基线占用
        # 方案回到 selected，审批保留，可重试
        self.assertEqual(self.database.connection.execute(
            "SELECT status FROM corridor_plans WHERE plan_id=?",
            (sea["plan_id"],)).fetchone()["status"], "selected")
        approvals = self.database.connection.execute(
            "SELECT COUNT(*) AS c FROM corridor_plan_approvals WHERE plan_id=?",
            (sea["plan_id"],)).fetchone()["c"]
        self.assertEqual(approvals, 2)

    def test_handed_over_shipment_cannot_be_pulled_back(self):
        sea = self._prepare_sea_plan()
        self.oc.select_plan(request_id="sel", actor_id="op", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a1", actor_id="admin", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a2", actor_id="fi", plan_id=sea["plan_id"])
        self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])
        self.tick("2026-10-15T00:00:00Z")
        self.oc.record_checkpoint(request_id="cp", actor_id="op", shipment_id="s1",
                                  node_id="b", event="handed_over")
        # 迟到的恢复消息不回拉
        result = self.oc.recover_notice(request_id="rc", actor_id="op", notice_id="n1")
        self.assertIn("s1", result["handed_over_shipments_unchanged"])
        # 对历史候选的迟到切换被拒绝
        obsolete = next(p["plan_id"] for p in self.oc.list_plans(actor_id="op", shipment_id="s1")["items"]
                        if p["status"] == "obsolete")
        with self.assertRaises(ConflictError):
            self.oc.execute_switch(request_id="late", actor_id="op", plan_id=obsolete)


class ViewAndHistoryTest(CorridorTestBase):
    def test_ops_and_finance_views(self):
        self.shipment()
        self.baseline()
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        sea = self.feasible_candidates(self.evaluate())[0]
        self.oc.select_plan(request_id="sel", actor_id="op", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a1", actor_id="admin", plan_id=sea["plan_id"])
        self.oc.approve_plan(request_id="a2", actor_id="fi", plan_id=sea["plan_id"])
        self.oc.execute_switch(request_id="sw", actor_id="op", plan_id=sea["plan_id"])

        ops = self.oc.ops_view(actor_id="op", shipment_id="s1", as_of="2026-10-04T00:00:00Z")
        item = ops["items"][0]
        self.assertEqual(item["active_segment"], "h1")
        self.assertEqual(item["responsible_party"]["carrier_id"], "carrier_sea")
        self.assertEqual(item["reroute_basis"]["blocked_on_baseline"][0]["point"], "bc1")
        finance = self.oc.finance_view(actor_id="fi", shipment_id="s1")
        f = finance["items"][0]
        self.assertEqual(f["baseline_cost"], 200)
        self.assertEqual(f["effective_cost"], 500)
        self.assertEqual(f["cost_delta"], 300)
        self.assertTrue(f["switched"])
        self.assertTrue(f["incurred_fees_total"] > 0)

    def test_finance_view_forbidden_for_operator(self):
        self.shipment()
        self.baseline()
        with self.assertRaises(PermissionDenied):
            self.oc.finance_view(actor_id="op")

    def test_history_snapshot_reconstructs_point_in_time(self):
        self.shipment()
        self.baseline()
        before = self.oc.history_snapshot(actor_id="au", at="2026-10-02T12:00:00Z")
        plan = before["shipments"][0]["effective_plan"]
        self.assertEqual(plan["status"], "baseline")
        self.assertEqual(before["notices"], [])
        self.tick("2026-10-03T00:00:00Z")
        self.notice()
        self.evaluate()
        during = self.oc.history_snapshot(actor_id="au", at="2026-10-03T01:00:00Z")
        self.assertTrue(during["notices"][0]["active"])
        kinds = {d["kind"] for d in during["decisions"]}
        self.assertIn("disruption.evaluated", kinds)
        self.oc.recover_notice(request_id="rc", actor_id="op", notice_id="n1")
        after = self.oc.history_snapshot(actor_id="au", at=self.clock.now().isoformat().replace(
            "+00:00", "Z"))
        self.assertFalse(after["notices"][0]["active"])


if __name__ == "__main__":
    unittest.main()
