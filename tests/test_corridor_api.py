import unittest
from datetime import datetime, timezone

from transport_coordination.api import route
from transport_coordination.clock import FixedClock
from transport_coordination.service import DomainService
from transport_coordination.storage import Database


class CorridorApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = FixedClock(datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc))
        self.service = DomainService(self.database, self.clock)
        status, _ = route(self.service, "POST", "/organizations",
                          {"request_id": "org1", "organization_id": "org", "name": "平台"},
                          {"X-Actor-Id": "bootstrap"})
        self.assertEqual(201, status)
        route(self.service, "POST", "/actors",
              {"request_id": "ra1", "new_actor_id": "admin", "display_name": "管理员",
               "role": "admin", "organization_id": "org"}, {"X-Actor-Id": "bootstrap"})
        for rid, aid, role, name in [
                ("ro1", "op", "operator", "运营"), ("rf1", "fi", "finance", "财务")]:
            route(self.service, "POST", "/actors",
                  {"request_id": rid, "new_actor_id": aid, "display_name": name,
                   "role": role, "organization_id": "org"}, {"X-Actor-Id": "admin"})
        self._network()

    def tearDown(self):
        self.database.close()

    def post(self, path, body, actor="op"):
        return route(self.service, "POST", path, body, {"X-Actor-Id": actor})

    def get(self, path, actor="op"):
        return route(self.service, "GET", path, None, {"X-Actor-Id": actor})

    def _network(self):
        segments = [
            ("g1", "a", "x", "rail", 10, "bc1"), ("g2", "x", "b", "rail", 10, None),
            ("h1", "a", "p", "sea", 30, None), ("h2", "p", "b", "road", 5, None)]
        for i, (sid, a, b, mode, hours, bc) in enumerate(segments):
            status, _ = self.post("/corridor/references", {"request_id": f"sg{i}", "kind": "segment",
                                  "data": {"segment_id": sid, "from_node": a, "to_node": b,
                                           "mode": mode, "transit_hours": hours,
                                           **({"border_crossing_id": bc} if bc else {})}})
            self.assertEqual(201, status)
        for i, (cid, seg, carrier) in enumerate([
                ("cg1", "g1", "carrier_rail"), ("cg2", "g2", "carrier_rail2"),
                ("ch1", "h1", "carrier_sea"), ("ch2", "h2", "carrier_road")]):
            status, _ = self.post("/corridor/references", {"request_id": f"cp{i}", "kind": "capacity",
                                  "data": {"capacity_id": cid, "segment_id": seg,
                                           "carrier_id": carrier, "carrier_name": carrier,
                                           "valid_from": "2026-09-01T00:00Z",
                                           "valid_to": "2026-12-31T00:00Z", "unit": "FEU",
                                           "capacity_units": 10, "unit_cost": 50,
                                           "commitment_fee": 0, "currency": "USD"}})
            self.assertEqual(201, status)
        for i, (rid, seg, carrier, cost) in enumerate([
                ("rg1", "g1", "carrier_rail", 100), ("rg2", "g2", "carrier_rail2", 100),
                ("rh1", "h1", "carrier_sea", 400), ("rh2", "h2", "carrier_road", 100)]):
            status, _ = self.post("/corridor/references", {"request_id": f"rp{i}",
                                  "kind": "contract_rate", "data": {
                                      "rate_id": rid, "segment_id": seg, "carrier_id": carrier,
                                      "valid_from": "2026-09-01T00:00Z",
                                      "valid_to": "2026-12-31T00:00Z", "unit_cost": cost,
                                      "fixed_cost": 0, "minimum_charge": 0,
                                      "currency": "USD"}}, actor="fi")
            self.assertEqual(201, status)

    def test_full_reroute_flow_over_http(self):
        self.clock._value = datetime(2026, 10, 2, 0, 0, tzinfo=timezone.utc)
        status, _ = self.post("/corridor/shipments",
                              {"request_id": "sh1", "shipment_id": "s1", "cargo_type": "general",
                               "units": 1, "origin_node": "a", "destination_node": "b",
                               "customer_ref": "C1", "must_arrive_by": "2026-10-20T00:00Z"})
        self.assertEqual(201, status)
        status, _ = self.post("/corridor/baselines",
                              {"request_id": "bl1", "shipment_id": "s1",
                               "path": ["g1", "g2"], "depart_at": "2026-10-02T00:00Z"})
        self.assertEqual(201, status)
        self.clock._value = datetime(2026, 10, 3, 0, 0, tzinfo=timezone.utc)
        status, _ = self.post("/corridor/notices",
                              {"request_id": "n1", "notice_id": "notice1", "title": "口岸限流",
                               "severity": "critical", "detail": "bc1 关闭",
                               "impact": {"border_crossings": ["bc1"]}})
        self.assertEqual(201, status)
        status, body = self.post("/corridor/evaluations", {"notice_id": "notice1"})
        self.assertEqual(200, status)
        self.assertTrue(body["shipments"][0]["affected"])
        sea = next(c for c in body["shipments"][0]["candidates"] if c["path"] == ["h1", "h2"])
        self.assertTrue(sea["feasible"])
        self.assertEqual(set(sea["required_approvals"]), {"admin", "finance"})

        status, _ = self.post("/corridor/plans/select",
                              {"request_id": "sel1", "plan_id": sea["plan_id"], "reason": "海运"})
        self.assertEqual(201, status)
        status, body = self.post("/corridor/plans/switch",
                                 {"request_id": "sw1", "plan_id": sea["plan_id"]})
        self.assertEqual(403, status)  # 缺审批
        self.post("/corridor/plans/approve",
                  {"request_id": "ap1", "plan_id": sea["plan_id"], "note": "ok"}, actor="admin")
        self.post("/corridor/plans/approve",
                  {"request_id": "ap2", "plan_id": sea["plan_id"], "note": "ok"}, actor="fi")
        status, body = self.post("/corridor/plans/switch",
                                 {"request_id": "sw2", "plan_id": sea["plan_id"]})
        self.assertEqual(200, status)
        self.assertEqual("effective", body["status"])

        status, ops = self.get("/corridor/ops-view?shipment_id=s1")
        self.assertEqual(200, status)
        self.assertEqual("operations", ops["view"])
        self.assertEqual(ops["items"][0]["plan_status"], "effective")
        self.assertEqual(ops["items"][0]["responsible_party"]["carrier_id"], "carrier_sea")
        self.assertEqual(ops["items"][0]["reroute_basis"]["blocked_on_baseline"][0]["point"], "bc1")

        status, body = self.get("/corridor/finance-view", actor="op")
        self.assertEqual(403, status)  # 运营看不到财务视图
        status, finance = self.get("/corridor/finance-view?shipment_id=s1", actor="fi")
        self.assertEqual(200, status)
        self.assertEqual(300, finance["items"][0]["cost_delta"])

        status, history = self.get("/corridor/history?at=2026-10-02T12:00:00Z", actor="fi")
        self.assertEqual(200, status)
        self.assertEqual([], history["notices"])
        self.assertEqual("baseline",
                         history["shipments"][0]["effective_plan"]["status"])

    def test_unknown_corridor_route_returns_404(self):
        status, body = route(self.service, "GET", "/corridor/nope", None,
                             {"X-Actor-Id": "op"})
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", body["error"])

    def test_history_requires_at(self):
        status, body = self.get("/corridor/history")
        self.assertEqual(400, status)
        self.assertEqual("validation_error", body["error"])


if __name__ == "__main__":
    unittest.main()
