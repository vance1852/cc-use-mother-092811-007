import unittest

from transport_coordination.api import route
from transport_coordination.orchestration import OrchestrationService
from transport_coordination.service import DomainService
from transport_coordination.storage import Database


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.domain = DomainService(self.database)
        self.orch = OrchestrationService(self.database, self.domain.clock)
        self.services = {"domain": self.domain, "orch": self.orch}

    def tearDown(self):
        self.database.close()

    def _call(self, method, path, body=None, actor=""):
        return route(self.services, method, path, body or {},
                     {"X-Actor-Id": actor})

    def _bootstrap(self):
        self._call("POST", "/organizations",
                   {"request_id": "org", "organization_id": "o1", "name": "平台"}, "bootstrap")
        self._call("POST", "/actors",
                   {"request_id": "a1", "new_actor_id": "admin", "display_name": "管理员",
                    "role": "admin", "organization_id": "o1"}, "bootstrap")
        for key, role in (("ops", "operator"), ("rev", "reviewer"), ("fin", "finance")):
            self._call("POST", "/actors",
                       {"request_id": f"a-{key}", "new_actor_id": key, "display_name": key,
                        "role": role, "organization_id": "o1"}, "admin")

    def test_health_is_available_without_actor(self):
        status, payload = self._call("GET", "/health")
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_unknown_route_returns_404(self):
        status, payload = self._call("GET", "/missing")
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_invalid_json_shape_returns_400(self):
        status, payload = self._call("POST", "/organizations", {"request_id": "x"}, "bootstrap")
        self.assertEqual(400, status)
        self.assertEqual("invalid_request", payload["error"])

    def test_reference_and_views_role_boundaries(self):
        self._bootstrap()
        status, _ = self._call("POST", "/references",
                               {"request_id": "r1", "entity_type": "segment",
                                "entity_id": "seg-1",
                                "payload": {"origin": "A", "destination": "B", "mode": "rail"}},
                               "ops")
        self.assertEqual(201, status)

        status, payload = self._call("POST", "/references",
                                     {"request_id": "r2", "entity_type": "segment",
                                      "entity_id": "seg-2",
                                      "payload": {"origin": "A", "destination": "C",
                                                  "mode": "rail"}}, "fin")
        self.assertEqual(403, status)
        self.assertEqual("permission_denied", payload["error"])

        status, payload = self._call("GET", "/references/segment/seg-1")
        self.assertEqual(200, status)
        self.assertEqual(1, payload["version"])
        status, _ = self._call("GET", "/references/segment/missing")
        self.assertEqual(404, status)

        # 运营不能看财务视图，财务不能看运营视图。
        status, _ = self._call("GET", "/finance", actor="ops")
        self.assertEqual(403, status)
        status, _ = self._call("GET", "/ops", actor="fin")
        self.assertEqual(403, status)
        status, _ = self._call("GET", "/ops", actor="rev")
        self.assertEqual(200, status)

    def test_full_reroute_flow_over_http(self):
        self._bootstrap()
        network = [
            ("segment", "rail-cn-xi", {"origin": "CN", "destination": "XI", "mode": "rail",
                                       "capacity_id": "cap-rail-cn-xi"}),
            ("segment", "rail-xi-eu", {"origin": "XI", "destination": "EU", "mode": "rail",
                                       "border_id": "border-ala",
                                       "capacity_id": "cap-rail-xi-eu"}),
            ("segment", "rail-eu-de", {"origin": "EU", "destination": "DE", "mode": "rail",
                                       "capacity_id": "cap-rail-eu-de"}),
            ("segment", "truck-xi-port", {"origin": "XI", "destination": "PORT", "mode": "truck",
                                          "capacity_id": "cap-truck-xi-port"}),
            ("segment", "sea-port-eu", {"origin": "PORT", "destination": "EU", "mode": "sea",
                                        "border_id": "border-sea",
                                        "capacity_id": "cap-sea-port-eu"}),
            ("segment", "truck-eu-de", {"origin": "EU", "destination": "DE", "mode": "truck",
                                        "capacity_id": "cap-truck-eu-de"}),
            ("border_calendar", "border-ala", {"closures": [
                {"from": "2026-09-23T00:00:00Z", "to": "2026-09-26T23:59:59Z",
                 "reason": "临时限流", "segment_id": "rail-xi-eu"}]}),
            ("border_calendar", "border-sea", {"closures": []}),
            ("cargo_restriction", "rest-li",
             {"scope": "cargo_tag", "target_id": "lithium", "forbidden_modes": ["sea"],
              "forbidden_segments": []}),
            ("customer_deadline", "c1",
             {"ready_at": "2026-09-18T00:00:00Z", "due_at": "2026-10-10T23:59:59Z"}),
            ("contract_rate", "rate-CRE",
             {"carrier_id": "CRE", "cancel_penalty_rate": 0.5, "rebooking_fee": 800,
              "currency": "CNY", "segment_rates": {"rail-cn-xi": 3000, "rail-xi-eu": 12000,
                                                   "rail-eu-de": 2500}}),
            ("contract_rate", "rate-SHT",
             {"carrier_id": "SHT", "cancel_penalty_rate": 0.3, "rebooking_fee": 300,
              "currency": "CNY", "segment_rates": {"truck-xi-port": 2000,
                                                   "truck-eu-de": 3000}}),
            ("contract_rate", "rate-MSC",
             {"carrier_id": "MSC", "cancel_penalty_rate": 0.8, "rebooking_fee": 1500,
              "currency": "CNY", "segment_rates": {"sea-port-eu": 18000}}),
            ("advisory", "adv-1",
             {"level": "severe", "affected": {"segments": ["rail-xi-eu"], "capacities": []},
              "window": {"from": "2026-09-23T00:00:00Z", "to": "2026-09-26T23:59:59Z"},
              "summary": "口岸限流"}),
        ]
        for index, (kind, entity_id, payload) in enumerate(network):
            actor = "fin" if kind == "contract_rate" else "ops"
            status, _ = self._call("POST", "/references",
                                   {"request_id": f"ref-{index}", "entity_type": kind,
                                    "entity_id": entity_id, "payload": payload,
                                    "valid_from": "2026-09-01T00:00:00Z"}, actor)
            self.assertEqual(201, status, payload)
        capacities = [
            ("cap-rail-cn-xi", "CRE", "班列", "rail-cn-xi",
             ("2026-09-22T08:00:00Z", "2026-09-22T20:00:00Z")),
            ("cap-rail-xi-eu", "CRE", "班列", "rail-xi-eu",
             ("2026-09-23T08:00:00Z", "2026-10-01T08:00:00Z")),
            ("cap-rail-eu-de", "CRE", "班列", "rail-eu-de",
             ("2026-10-01T12:00:00Z", "2026-10-02T08:00:00Z")),
            ("cap-truck-xi-port", "SHT", "集卡", "truck-xi-port",
             ("2026-09-25T06:00:00Z", "2026-09-26T06:00:00Z")),
            ("cap-sea-port-eu", "MSC", "海运", "sea-port-eu",
             ("2026-09-27T00:00:00Z", "2026-10-05T12:00:00Z")),
            ("cap-truck-eu-de", "SHT", "接驳", "truck-eu-de",
             ("2026-10-06T00:00:00Z", "2026-10-06T18:00:00Z")),
        ]
        for index, (cid, carrier, name, seg, (dep, arr)) in enumerate(capacities):
            status, _ = self._call("POST", "/references",
                                   {"request_id": f"cap-{index}", "entity_type": "capacity",
                                    "entity_id": cid, "valid_from": "2026-09-01T00:00:00Z",
                                    "payload": {"segment_id": seg, "carrier_id": carrier,
                                                "carrier_name": name,
                                                "departures": [{"departs_at": dep,
                                                                "arrives_at": arr,
                                                                "slots": 10}]}}, "ops")
            self.assertEqual(201, status)

        status, _ = self._call("POST", "/shipments",
                               {"request_id": "ship-c1", "cargo_id": "c1", "customer_id": "cust",
                                "origin": "CN", "destination": "DE", "tags": ["general"],
                                "weight": 10,
                                "legs": [
                                    {"segment_id": "rail-cn-xi",
                                     "departs_at": "2026-09-22T08:00:00Z",
                                     "arrives_at": "2026-09-22T20:00:00Z",
                                     "status": "departed"},
                                    {"segment_id": "rail-xi-eu",
                                     "departs_at": "2026-09-23T08:00:00Z",
                                     "arrives_at": "2026-10-01T08:00:00Z"},
                                    {"segment_id": "rail-eu-de",
                                     "departs_at": "2026-10-01T12:00:00Z",
                                     "arrives_at": "2026-10-02T08:00:00Z"},
                                ]}, "ops")
        self.assertEqual(201, status)

        status, payload = self._call("POST", "/disruptions/detect",
                                     {"advisory_id": "adv-1"}, "ops")
        self.assertEqual(200, status)
        self.assertEqual(1, payload["total"])

        status, payload = self._call("GET", "/candidates?cargo_id=c1", actor="ops")
        self.assertEqual(200, status)
        self.assertTrue(payload["feasible"])

        status, payload = self._call("POST", "/plans",
                                     {"request_id": "plan-1", "cargo_id": "c1",
                                      "advisory_id": "adv-1"}, "ops")
        self.assertEqual(201, status)
        plan_id = payload["resource_id"]

        status, _ = self._call("POST", f"/plans/{plan_id}/lock",
                               {"request_id": "lock-1"}, "ops")
        self.assertEqual(201, status)
        status, payload = self._call("POST", f"/plans/{plan_id}/commit",
                                     {"request_id": "commit-early"}, "ops")
        self.assertEqual(409, status)
        self.assertEqual("approval_required", payload["error"])

        for request_id, key, actor in (("ap1", "operations_duty", "ops"),
                                       ("ap2", "customer_window", "rev"),
                                       ("ap3", "finance", "fin")):
            status, _ = self._call("POST", f"/plans/{plan_id}/approvals",
                                   {"request_id": request_id, "key": key}, actor)
            self.assertEqual(201, status)
        status, payload = self._call("POST", f"/plans/{plan_id}/commit",
                                     {"request_id": "commit-1"}, "ops")
        self.assertEqual(201, status)

        status, payload = self._call("GET", "/ops?cargo_id=c1", actor="ops")
        self.assertEqual(200, status)
        item = payload["items"][0]
        self.assertEqual("switched", item["state"])
        self.assertEqual("2026-10-06T18:00:00Z", item["eta"])
        self.assertEqual("adv-1", item["reroute_basis"]["advisory_id"])

        status, payload = self._call("GET", "/finance?cargo_id=c1", actor="fin")
        self.assertEqual(200, status)
        self.assertEqual(8500.0, payload["reroute_differences"][0]["delta"])

        status, payload = self._call("GET",
                                     "/reconstruct?at=2026-09-22T12:00:00Z&cargo_id=c1")
        self.assertEqual(200, status)
        self.assertEqual([], payload["shipments"])


if __name__ == "__main__":
    unittest.main()
