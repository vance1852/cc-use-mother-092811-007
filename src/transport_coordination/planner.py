"""国际通道候选路径规划器（纯函数，无副作用）。

规划器只读取某一时点的通道快照与当前占用，枚举起讫点之间的分段组合，
对每个可用班次解释“为什么选/为什么不选”，输出可解释的可行方案与被否决
方案，并给出费用差异、客户时限结论与必要审批。所有时间均为 UTC 文本，
按字典序即可比较。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

#: 占用能力的持有状态：未释放的预订、锁定与已消费均计入占用。
OCCUPYING_STATUSES = frozenset({"booked", "locked", "consumed"})


def _parse_ts(value: str | None) -> str:
    return value or ""


def _restriction_matches(restriction: dict[str, Any], cargo: dict[str, Any]) -> bool:
    payload = restriction["payload"]
    target = payload.get("target_id", "*")
    if target == "*":
        return True
    scope = payload.get("scope", "cargo_tag")
    tags = set(cargo.get("tags", ()))
    if scope == "cargo_tag":
        return target in tags
    if scope == "hazard_class":
        return f"hazmat:{target}" in tags or target in tags
    return False


def _departure_blockers(departure: dict[str, Any], segment: dict[str, Any],
                        cargo: dict[str, Any], snapshot: dict[str, Any],
                        occupied: int, ready_at: str,
                        not_before: str) -> list[str]:
    """返回某班次对某货物不可用的全部原因，空列表表示可用。"""

    blockers: list[str] = []
    departs = departure["departs_at"]
    if departs < not_before:
        blockers.append(f"发车 {departs} 早于衔接到达 {not_before}")
    if ready_at and departs < ready_at:
        blockers.append(f"发车 {departs} 早于货物就绪 {ready_at}")
    if departure.get("slot_weight") and cargo.get("weight", 0) > departure["slot_weight"]:
        blockers.append(f"单件重量 {cargo['weight']} 超过班次载重 {departure['slot_weight']}")
    if occupied + 1 > departure["slots"]:
        blockers.append(f"能力已占满（占用 {occupied}/{departure['slots']}）")

    segment_id = segment["entity_id"]
    border_id = segment["payload"].get("border_id")
    if border_id and border_id in snapshot["borders"]:
        for closure in snapshot["borders"][border_id]["payload"].get("closures", []):
            applies = closure.get("segment_id", segment_id) in (segment_id, "*")
            if applies and closure["from"] <= departs <= closure["to"]:
                blockers.append(
                    f"口岸 {border_id} 在 {closure['from']}~{closure['to']} 关闭"
                    f"（{closure.get('reason', '未说明原因')}）")

    for advisory in snapshot["advisories"]:
        affected = advisory["payload"].get("affected", {})
        window = advisory["payload"].get("window", {})
        hit = departure["capacity_id"] in affected.get("capacities", ()) or \
            segment_id in affected.get("segments", ())
        if hit and window.get("from", "") <= departs <= window.get("to", ""):
            blockers.append(f"风险通告 {advisory['entity_id']}（{advisory['payload'].get('summary', '')}）"
                            f"覆盖该班期")

    for restriction in snapshot["restrictions"]:
        if not _restriction_matches(restriction, cargo):
            continue
        payload = restriction["payload"]
        if segment_id in payload.get("forbidden_segments", ()):
            blockers.append(f"货物限制 {restriction['entity_id']} 禁止通行该分段")
        if segment["payload"].get("mode") in payload.get("forbidden_modes", ()):
            blockers.append(f"货物限制 {restriction['entity_id']} 禁止 {segment['payload']['mode']} 运输")
    return blockers


def _enumerate_paths(snapshot: dict[str, Any], origin: str, destination: str) -> list[list[str]]:
    """在分段图上枚举简单路径，返回分段编号序列。"""

    outgoing: dict[str, list[str]] = defaultdict(list)
    for segment_id, segment in snapshot["segments"].items():
        outgoing[segment["payload"]["origin"]].append(segment_id)

    paths: list[list[str]] = []

    def walk(node: str, chain: list[str], visited_segments: set[str]) -> None:
        if node == destination:
            paths.append(list(chain))
            return
        for segment_id in outgoing.get(node, ()):
            if segment_id in visited_segments:
                continue
            segment = snapshot["segments"][segment_id]
            walk(segment["payload"]["destination"], chain + [segment_id],
                 visited_segments | {segment_id})

    walk(origin, [], set())
    return paths


def plan_candidates(snapshot: dict[str, Any], cargo: dict[str, Any],
                    holds: list[dict[str, Any]], current_legs: list[dict[str, Any]]) -> dict[str, Any]:
    """生成候选路径评估结果。

    ``cargo`` 至少包含 cargo_id/origin/destination/tags/weight；时限从快照中
    的 customer_deadline（以货物编号为键）读取。``holds`` 为当前能力占用行，
    ``current_legs`` 为货物当前一代行程。
    """

    deadline = snapshot["deadlines"].get(cargo["cargo_id"])
    ready_at = deadline["payload"].get("ready_at", "") if deadline else ""
    due_at = deadline["payload"].get("due_at", "") if deadline else ""

    occupancy: dict[tuple[str, str], int] = defaultdict(int)
    for hold in holds:
        if hold["status"] in OCCUPYING_STATUSES and hold["cargo_id"] != cargo["cargo_id"]:
            occupancy[(hold["capacity_id"], hold.get("departs_at", ""))] += hold.get("slots", 1)

    rate_by_carrier = {rate["payload"]["carrier_id"]: rate["payload"]
                       for rate in snapshot["rates"]}

    original_legs = [leg for leg in current_legs if not leg["released_at"]]
    original_modes = {snapshot["segments"][leg["segment_id"]]["payload"]["mode"]
                      for leg in original_legs if leg["segment_id"] in snapshot["segments"]}
    original_total = sum(leg["booked_cost"] for leg in original_legs)
    original_eta = original_legs[-1]["arrives_at"] if original_legs else ""
    currency = original_legs[0]["currency"] if original_legs else "CNY"

    # 改道从货物当前物理位置起算：已发运分段不再重复预订，
    # 其能力继续计占用、其费用留在台账中。
    reroute_origin = cargo["origin"]
    reroute_not_before = ready_at
    for leg in sorted(original_legs, key=lambda item: item["seq"]):
        if leg["status"] in ("departed", "in_transit"):
            segment = snapshot["segments"].get(leg["segment_id"])
            if segment is not None:
                reroute_origin = segment["payload"]["destination"]
            if leg["arrives_at"] > reroute_not_before:
                reroute_not_before = leg["arrives_at"]
        else:
            break

    candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    prefix_legs = [{
        "seq": leg["seq"], "segment_id": leg["segment_id"],
        "mode": snapshot["segments"][leg["segment_id"]]["payload"]["mode"],
        "capacity_id": leg["capacity_id"], "carrier_id": leg["carrier_id"],
        "carrier_name": leg["carrier_name"], "departs_at": leg["departs_at"],
        "arrives_at": leg["arrives_at"], "booked_cost": leg["booked_cost"],
        "currency": leg["currency"], "already_traversed": True,
    } for leg in sorted(original_legs, key=lambda item: item["seq"])
        if leg["status"] in ("departed", "in_transit")
        and leg["segment_id"] in snapshot["segments"]]
    prefix_seq = max((leg["seq"] for leg in prefix_legs), default=0)
    replaceable_total = sum(leg["booked_cost"] for leg in original_legs
                            if leg["status"] not in ("departed", "in_transit"))
    prefix_modes = {leg["mode"] for leg in prefix_legs}

    for path in _enumerate_paths(snapshot, reroute_origin, cargo["destination"]):
        legs: list[dict[str, Any]] = []
        explain: list[str] = []
        violations: list[str] = []
        documents: list[str] = []
        not_before = reroute_not_before
        feasible_path = True

        for offset, segment_id in enumerate(path, start=1):
            segment = snapshot["segments"].get(segment_id)
            if segment is None:
                feasible_path = False
                violations.append(f"分段 {segment_id} 在该时点无生效版本")
                break
            mode = segment["payload"]["mode"]
            capacity = snapshot["capacities"].get(
                segment["payload"].get("capacity_id", segment_id))
            available: list[dict[str, Any]] = []
            skipped: list[str] = []
            if capacity is not None:
                for departure in capacity["payload"].get("departures", []):
                    dep = {**departure, "capacity_id": capacity["entity_id"],
                           "segment_id": segment_id,
                           "carrier_id": capacity["payload"]["carrier_id"],
                           "carrier_name": capacity["payload"]["carrier_name"]}
                    blockers = _departure_blockers(
                        dep, segment, cargo, snapshot,
                        occupancy[(dep["capacity_id"], dep["departs_at"])],
                        ready_at, not_before)
                    if blockers:
                        skipped.append(f"跳过班次 {dep['capacity_id']}@{dep['departs_at']}："
                                       + "；".join(blockers))
                    else:
                        available.append(dep)
            if not available:
                feasible_path = False
                violations.append(f"分段 {segment_id}（{mode}）无可用班期")
                explain.extend(skipped)
                break
            chosen = available[0]
            rate = rate_by_carrier.get(chosen["carrier_id"], {})
            base_cost = rate.get("segment_rates", {}).get(segment_id)
            if base_cost is None:
                base_cost = chosen.get("base_cost", 0)
            legs.append({
                "seq": prefix_seq + offset,
                "segment_id": segment_id,
                "mode": mode,
                "capacity_id": chosen["capacity_id"],
                "carrier_id": chosen["carrier_id"],
                "carrier_name": chosen["carrier_name"],
                "departs_at": chosen["departs_at"],
                "arrives_at": chosen["arrives_at"],
                "booked_cost": float(base_cost),
                "currency": rate.get("currency", currency),
                "already_traversed": False,
            })
            explain.append(f"分段 {prefix_seq + offset} {segment_id}（{mode}）：选择 "
                           f"{chosen['capacity_id']} {chosen['carrier_name']}，"
                           f"{chosen['departs_at']} 发车 / {chosen['arrives_at']} 到达，"
                           f"费用 {base_cost}")
            explain.extend(skipped[:5])
            not_before = chosen["arrives_at"]

        documents: list[str] = []
        for restriction in snapshot["restrictions"]:
            if _restriction_matches(restriction, cargo) and \
                    restriction["payload"].get("document_required"):
                doc = restriction["payload"]["document_required"]
                if doc not in documents:
                    documents.append(doc)

        if not feasible_path:
            rejected.append({
                "path": path,
                "violations": violations,
                "explain": explain,
            })
            continue

        arrives_at = legs[-1]["arrives_at"]
        new_booking_total = sum(leg["booked_cost"] for leg in legs)
        full_legs = prefix_legs + legs
        modes = {leg["mode"] for leg in full_legs}

        if due_at and arrives_at > due_at:
            violations.append(f"到达 {arrives_at} 晚于客户交付窗 {due_at}")
        if ready_at and legs[0]["departs_at"] < ready_at:
            violations.append(f"首发 {legs[0]['departs_at']} 早于货物就绪 {ready_at}")

        approvals = ["operations_duty"]
        if modes != original_modes:
            approvals.append("customer_window")
        if original_eta and arrives_at > original_eta:
            if "customer_window" not in approvals:
                approvals.append("customer_window")
        if new_booking_total > replaceable_total:
            approvals.append("finance")
        touches_advisory = any(
            (leg["segment_id"] in advisory["payload"].get("affected", {}).get("segments", ())
             or leg["capacity_id"] in advisory["payload"].get("affected", {}).get("capacities", ()))
            for leg in legs for advisory in snapshot["advisories"])
        if touches_advisory or documents:
            approvals.append("border_authority")

        explain.append(f"已完成分段费用（不可退）{sum(l['booked_cost'] for l in prefix_legs)}；"
                       f"待替换原订费用 {replaceable_total}；新订费用 {new_booking_total}，"
                       f"净差额 {new_booking_total - replaceable_total:+.2f}")
        explain.append("满足客户交付窗" if not violations else "；".join(violations))
        if documents:
            explain.append(f"需随附单证：{', '.join(documents)}")

        candidate = {
            "legs": legs,
            "prefix_legs": prefix_legs,
            "arrives_at": arrives_at,
            "new_total": new_booking_total,
            "total_cost": new_booking_total,
            "currency": legs[0]["currency"],
            "meets_deadline": not violations,
            "violations": violations,
            "required_documents": documents,
            "required_approvals": approvals,
            "explain": explain,
        }
        if violations:
            rejected.append({"path": path, "violations": violations,
                             "candidate": candidate, "explain": explain})
        else:
            candidates.append(candidate)

    candidates.sort(key=lambda item: (item["arrives_at"], item["new_total"]))
    for index, candidate in enumerate(candidates, start=1):
        candidate["candidate_id"] = f"C{index}"
    sunk_total = sum(leg["booked_cost"] for leg in original_legs
                     if leg["status"] in ("departed", "in_transit"))
    # 释放未使用预订时，按合同取消费率预计的违约金。
    expected_penalty = 0.0
    for leg in original_legs:
        if leg["status"] not in ("departed", "in_transit"):
            rate = rate_by_carrier.get(leg["carrier_id"], {})
            expected_penalty += leg["booked_cost"] * rate.get("cancel_penalty_rate", 0)
    return {
        "ready_at": ready_at,
        "due_at": due_at,
        "feasible": candidates,
        "rejected": rejected,
        "current_assessment": assess_current(snapshot, cargo, original_legs,
                                             occupancy, ready_at),
        "cost": {"original_total": original_total, "sunk_total": sunk_total,
                 "replaceable_total": replaceable_total,
                 "expected_release_penalty": round(expected_penalty, 2),
                 "currency": currency},
    }


def assess_current(snapshot: dict[str, Any], cargo: dict[str, Any],
                   original_legs: list[dict[str, Any]],
                   occupancy: dict[tuple[str, str], int] | None = None,
                   ready_at: str = "") -> dict[str, Any]:
    """评估当前行程在最新快照下是否仍可通行。"""

    findings: list[dict[str, Any]] = []
    for leg in original_legs:
        segment = snapshot["segments"].get(leg["segment_id"])
        if segment is None:
            findings.append({"seq": leg["seq"], "segment_id": leg["segment_id"],
                             "blocked": True, "reasons": ["分段已无生效版本"]})
            continue
        departure = {"capacity_id": leg["capacity_id"], "slots": 10**9,
                     "departs_at": leg["departs_at"], "arrives_at": leg["arrives_at"]}
        blockers = _departure_blockers(
            departure, segment, cargo, snapshot, 0, ready_at or "0", "0")
        if blockers:
            findings.append({"seq": leg["seq"], "segment_id": leg["segment_id"],
                             "capacity_id": leg["capacity_id"],
                             "blocked": True, "reasons": blockers})
    return {
        "eta": original_legs[-1]["arrives_at"] if original_legs else None,
        "blocked": any(item.get("blocked") for item in findings),
        "findings": findings,
    }
