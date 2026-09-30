"""国际通道候选路径枚举与可行性校验（纯函数）。

编排器在调用前把某一时点的版本化资料整理成 ``NetworkContext``，
本模块只依据给定快照计算候选路径，不触碰数据库与时钟，便于离线推理与测试。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# 路径枚举的保护性上限，避免图异常时组合爆炸。
_MAX_DEPTH = 10
_MAX_PATHS = 200
_TOP_K = 6

# 费用相对基线上涨超过该比例需要财务审批。
COST_INCREASE_APPROVAL = 0.10


def parse_ts(value: str) -> datetime:
    """解析统一的 UTC ISO 字符串。"""

    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def format_ts(value: datetime) -> str:
    """格式化为统一的 UTC ISO 字符串。"""

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def hours_later(value: str, hours: float) -> str:
    return format_ts(parse_ts(value) + timedelta(hours=hours))


@dataclass(frozen=True)
class NetworkContext:
    """一次规划所依赖的全部时点数据。"""

    at: str
    shipment: dict[str, Any]
    deadline: str | None
    segments: dict[str, dict[str, Any]]
    capacities: list[dict[str, Any]]
    rates: list[dict[str, Any]]
    calendars: dict[str, dict[str, Any]]
    restrictions: list[dict[str, Any]]
    notice: dict[str, Any] | None
    usage: dict[str, float]
    baseline_path: list[str] | None = None


def _intervals_closed_at(calendar: dict[str, Any] | None, moment: datetime) -> dict[str, Any] | None:
    """返回时刻 ``moment`` 正在生效的口岸关闭窗口。"""

    if not calendar:
        return None
    for interval in calendar.get("closed_intervals", []):
        start = parse_ts(interval["from"])
        end = parse_ts(interval["to"])
        if start <= moment < end:
            return interval
    return None


def _scope_matches(scope: dict[str, Any], *, mode: str, crossing_id: str | None,
                   segment_id: str, carrier_id: str) -> bool:
    """货物限制的作用域是否覆盖当前运输单元。空作用域视为全局。"""

    if not scope:
        return True
    if scope.get("mode") and scope["mode"] != mode:
        return False
    if scope.get("border_crossing_id") and scope["border_crossing_id"] != crossing_id:
        return False
    if scope.get("segment_id") and scope["segment_id"] != segment_id:
        return False
    if scope.get("carrier_id") and scope["carrier_id"] != carrier_id:
        return False
    return True


def _restriction_violations(ctx: NetworkContext, segment: dict[str, Any], carrier_id: str) -> list[dict[str, str]]:
    violations: list[dict[str, str]] = []
    hazmat = ctx.shipment.get("hazmat_class")
    cargo_type = ctx.shipment.get("cargo_type")
    for rule in ctx.restrictions:
        scope = rule.get("scope", {})
        if not _scope_matches(scope, mode=segment["mode"],
                              crossing_id=segment.get("border_crossing_id"),
                              segment_id=segment["segment_id"], carrier_id=carrier_id):
            continue
        forbidden_classes = set(rule.get("forbidden_classes", []))
        forbidden_types = set(rule.get("forbidden_cargo_types", []))
        if (hazmat and hazmat in forbidden_classes) or cargo_type in forbidden_types:
            violations.append({
                "code": "cargo_restricted",
                "segment_id": segment["segment_id"],
                "carrier_id": carrier_id,
                "restriction_id": rule["restriction_id"],
                "blocking": True,
                "message": f"货物不满足限制 {rule['restriction_id']}：{rule.get('note', '禁止通行')}",
            })
    return violations


def _rate_for(rates: list[dict[str, Any]], segment_id: str, carrier_id: str,
              moment: datetime) -> dict[str, Any] | None:
    candidates = [
        rate for rate in rates
        if rate["segment_id"] == segment_id and rate.get("carrier_id") == carrier_id
        and parse_ts(rate["valid_from"]) <= moment < parse_ts(rate["valid_to"])
    ]
    return min(candidates, key=lambda r: r.get("unit_cost", 0) * r.get("unit_count", 1), default=None)


def _cost_of(rate: dict[str, Any] | None, capacity: dict[str, Any], units: float) -> tuple[float, str | None]:
    if rate is None:
        return round(float(capacity.get("unit_cost", 0)) * units, 2), None
    raw = rate.get("unit_cost", 0) * units + rate.get("fixed_cost", 0)
    return round(max(raw, rate.get("minimum_charge", 0)), 2), rate["rate_id"]


def _notice_impact(ctx: NetworkContext) -> dict[str, set[str]]:
    impact = {"border_crossings": set(), "segments": set(), "nodes": set(), "carriers": set()}
    if ctx.notice and ctx.notice.get("status") == "active":
        raw = ctx.notice.get("impact", {})
        impact["border_crossings"] = set(raw.get("border_crossings", []))
        impact["segments"] = set(raw.get("segments", []))
        impact["nodes"] = set(raw.get("nodes", []))
        impact["carriers"] = set(raw.get("carriers", []))
    return impact


def _evaluate_leg(ctx: NetworkContext, segment: dict[str, Any], depart_at: str,
                  impact: dict[str, set[str]]) -> dict[str, Any]:
    """评估单个分段：等待窗口、班期、限制、余量与计价。"""

    depart_dt = parse_ts(depart_at)
    crossing_id = segment.get("border_crossing_id")
    violations: list[dict[str, Any]] = []
    wait_hours = 0.0

    closed = _intervals_closed_at(ctx.calendars.get(crossing_id), depart_dt)
    if closed:
        reopen = parse_ts(closed["to"])
        wait_hours = max(0.0, (reopen - depart_dt).total_seconds() / 3600.0)
    if crossing_id and crossing_id in impact["border_crossings"]:
        violations.append({"code": "notice_border_closed", "segment_id": segment["segment_id"],
                           "border_crossing_id": crossing_id, "blocking": True,
                           "message": f"口岸 {crossing_id} 被风险通告中断"})
    if segment["segment_id"] in impact["segments"]:
        violations.append({"code": "notice_segment_blocked", "segment_id": segment["segment_id"],
                           "blocking": True, "message": f"分段 {segment['segment_id']} 被风险通告中断"})
    if segment["from_node"] in impact["nodes"] or segment["to_node"] in impact["nodes"]:
        violations.append({"code": "notice_node_blocked", "segment_id": segment["segment_id"],
                           "blocking": True, "message": "分段端点被风险通告中断"})

    options: list[dict[str, Any]] = []
    units = float(ctx.shipment["units"])
    for capacity in ctx.capacities:
        if capacity["segment_id"] != segment["segment_id"]:
            continue
        window_start = parse_ts(capacity["valid_from"])
        window_end = parse_ts(capacity["valid_to"])
        carrier_violations = list(violations)
        if not window_start <= depart_dt < window_end:
            carrier_violations.append({"code": "capacity_window", "segment_id": segment["segment_id"],
                                       "capacity_id": capacity["capacity_id"], "blocking": True,
                                       "message": "班期不覆盖分段出发时刻"})
        if capacity["carrier_id"] in impact["carriers"]:
            carrier_violations.append({"code": "notice_carrier_suspended", "segment_id": segment["segment_id"],
                                       "capacity_id": capacity["capacity_id"], "blocking": True,
                                       "message": f"承运方 {capacity['carrier_id']} 被风险通告暂停"})
        carrier_violations.extend(_restriction_violations(ctx, segment, capacity["carrier_id"]))
        used = float(ctx.usage.get(capacity["capacity_id"], 0.0))
        free_units = round(float(capacity["capacity_units"]) - used, 4)
        shortfall = free_units + 1e-9 < units
        if shortfall:
            carrier_violations.append({"code": "capacity_shortfall", "segment_id": segment["segment_id"],
                                       "capacity_id": capacity["capacity_id"], "blocking": True,
                                       "message": f"可用 {free_units} {capacity.get('unit', '单位')}，"
                                                  f"需要 {units}，余量不足"})
        rate = _rate_for(ctx.rates, segment["segment_id"], capacity["carrier_id"], depart_dt)
        cost, rate_id = _cost_of(rate, capacity, units)
        options.append({
            "capacity_id": capacity["capacity_id"],
            "carrier_id": capacity["carrier_id"],
            "carrier_name": capacity.get("carrier_name", capacity["carrier_id"]),
            "rate_id": rate_id,
            "pricing": "contract" if rate else "spot",
            "cost": cost,
            "commitment_fee": float(capacity.get("commitment_fee", 0) or 0),
            "currency": (rate or capacity).get("currency", "USD"),
            "free_units": free_units,
            "feasible": not carrier_violations,
            "violations": carrier_violations,
        })

    feasible_options = sorted((o for o in options if o["feasible"]), key=lambda o: o["cost"])
    if feasible_options:
        chosen = feasible_options[0]
        leg_violations = []
    else:
        chosen = min(options, key=lambda o: o["cost"], default=None)
        leg_violations = chosen["violations"] if chosen else [
            {"code": "no_capacity", "segment_id": segment["segment_id"], "blocking": True,
             "message": "该分段没有任何承运能力"}]

    arrive_at = hours_later(depart_at, wait_hours + float(segment["transit_hours"]))
    return {
        "segment_id": segment["segment_id"],
        "mode": segment["mode"],
        "from_node": segment["from_node"],
        "to_node": segment["to_node"],
        "border_crossing_id": crossing_id,
        "depart_at": depart_at,
        "wait_hours": round(wait_hours, 2),
        "arrive_at": arrive_at,
        "option": chosen,
        "all_options": [{"capacity_id": o["capacity_id"], "carrier_id": o["carrier_id"],
                         "free_units": o["free_units"], "feasible": o["feasible"], "cost": o["cost"],
                         "reason": (next((v["message"] for v in o["violations"]), None)
                                    if not o["feasible"] else None)}
                        for o in options],
        "violations": leg_violations,
    }


def evaluate_path(ctx: NetworkContext, path: list[str], impact: dict[str, set[str]]) -> dict[str, Any]:
    legs: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    total_cost = 0.0
    currency = "USD"
    cursor = ctx.at
    for segment_id in path:
        segment = ctx.segments[segment_id]
        leg = _evaluate_leg(ctx, segment, cursor, impact)
        legs.append(leg)
        violations.extend(leg["violations"])
        if leg["option"] is not None:
            total_cost += float(leg["option"]["cost"])
            currency = leg["option"]["currency"]
        cursor = leg["arrive_at"]

    eta = cursor
    if ctx.deadline and parse_ts(eta) > parse_ts(ctx.deadline):
        violations.append({"code": "deadline_missed", "blocking": True, "eta": eta,
                           "must_arrive_by": ctx.deadline,
                           "message": f"预计到达 {eta} 晚于客户交付窗 {ctx.deadline}"})

    blocking = {v["code"] for v in violations if v.get("blocking")}
    return {
        "path": path,
        "legs": [{k: leg[k] for k in ("segment_id", "mode", "from_node", "to_node",
                                      "border_crossing_id", "depart_at", "arrive_at",
                                      "wait_hours", "option", "all_options")} for leg in legs],
        "eta": eta,
        "total_cost": round(total_cost, 2),
        "currency": currency,
        "feasible": not blocking,
        "violations": violations,
    }


def enumerate_candidates(ctx: NetworkContext) -> list[dict[str, Any]]:
    """枚举起讫节点间的简单路径并给出可行性解释，按可行性与费用返回前 K 条。"""

    adjacency: dict[str, list[str]] = {}
    for segment_id, segment in ctx.segments.items():
        adjacency.setdefault(segment["from_node"], []).append(segment_id)

    origin = ctx.shipment["origin_node"]
    destination = ctx.shipment["destination_node"]
    impact = _notice_impact(ctx)
    results: list[dict[str, Any]] = []
    seen_paths: set[tuple[str, ...]] = set()
    paths_seen = 0

    def walk(node: str, path: list[str], visited: set[str]) -> None:
        nonlocal paths_seen
        if paths_seen >= _MAX_PATHS:
            return
        if node == destination:
            key = tuple(path)
            if key not in seen_paths:
                seen_paths.add(key)
                paths_seen += 1
                results.append(evaluate_path(ctx, path, impact))
            return
        if len(path) >= _MAX_DEPTH:
            return
        for segment_id in sorted(adjacency.get(node, []), key=lambda s: ctx.segments[s]["transit_hours"]):
            next_node = ctx.segments[segment_id]["to_node"]
            if next_node in visited:
                continue
            visited.add(next_node)
            walk(next_node, path + [segment_id], visited)
            visited.remove(next_node)

    walk(origin, [], {origin})
    results.sort(key=lambda c: (not c["feasible"], c["total_cost"], parse_ts(c["eta"])))
    return results[:_TOP_K]


def explain_reroute_baseline(baseline_legs: list[dict[str, Any]] | None,
                             notice: dict[str, Any] | None) -> dict[str, Any]:
    """说明基线为何被中断命中，作为候选路径的改道依据。"""

    blocked: list[dict[str, str]] = []
    if baseline_legs and notice and notice.get("status") == "active":
        impact = notice.get("impact", {})
        crossings = set(impact.get("border_crossings", []))
        segments_hit = set(impact.get("segments", []))
        nodes = set(impact.get("nodes", []))
        carriers = set(impact.get("carriers", []))
        for leg in baseline_legs:
            if leg.get("border_crossing_id") in crossings:
                blocked.append({"point": leg["border_crossing_id"], "reason": "口岸中断",
                                "segment_id": leg["segment_id"]})
            if leg["segment_id"] in segments_hit:
                blocked.append({"point": leg["segment_id"], "reason": "分段中断"})
            if leg["from_node"] in nodes or leg["to_node"] in nodes:
                blocked.append({"point": f"{leg['from_node']}->{leg['to_node']}", "reason": "节点中断"})
            option = leg.get("option") or {}
            if option.get("carrier_id") in carriers:
                blocked.append({"point": option["carrier_id"], "reason": "承运方暂停"})
    return {
        "trigger_notice_id": notice["notice_id"] if notice else None,
        "trigger_title": notice.get("title") if notice else None,
        "blocked_on_baseline": blocked,
    }


def required_approval_roles(candidate: dict[str, Any], baseline: dict[str, Any] | None,
                            shipment: dict[str, Any]) -> list[str]:
    """依据改道内容推导必须完成的审批角色。"""

    roles: list[str] = []
    if baseline is not None:
        baseline_carriers = {leg["option"]["carrier_id"] for leg in baseline.get("legs", [])
                             if leg.get("option")}
        candidate_carriers = {leg["option"]["carrier_id"] for leg in candidate.get("legs", [])
                              if leg.get("option")}
        if candidate_carriers != baseline_carriers:
            roles.append("admin")
        base_cost = float(baseline.get("total_cost", 0) or 0)
        if base_cost > 0 and (candidate["total_cost"] - base_cost) / base_cost > COST_INCREASE_APPROVAL:
            roles.append("finance")
        base_crossings = {leg.get("border_crossing_id") for leg in baseline.get("legs", [])}
        candidate_crossings = {leg.get("border_crossing_id") for leg in candidate.get("legs", [])}
        if shipment.get("hazmat_class") and base_crossings != candidate_crossings:
            roles.append("safety")
    return roles
