"""版本化参考数据的实体类型与只读快照查询。

所有通道资料（线路分段、口岸日历、承运能力、货物限制、客户时限、
合同费用、风险通告）都以不可变版本存储；本模块只负责在给定历史时点
复原当时生效的版本，不包含任何写入逻辑。
"""

from __future__ import annotations

import json
from typing import Any

#: 受编排服务治理的七类参考数据。
ENTITY_TYPES = frozenset({
    "segment",            # 线路分段
    "border_calendar",    # 口岸日历
    "capacity",           # 承运能力/班期
    "cargo_restriction",  # 货物通行限制
    "customer_deadline",  # 客户交付时限
    "contract_rate",      # 合同费用
    "advisory",           # 风险通告
})

#: 每类资料必须提供的字段，用于登记时的最小校验。
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "segment": ("origin", "destination", "mode"),
    "border_calendar": ("closures",),
    "capacity": ("segment_id", "carrier_id", "carrier_name", "departures"),
    "cargo_restriction": ("scope", "target_id"),
    "customer_deadline": ("ready_at", "due_at"),
    "contract_rate": ("carrier_id", "cancel_penalty_rate", "rebooking_fee", "currency"),
    "advisory": ("level", "affected", "window", "summary"),
}


def active_version(connection, entity_type: str, entity_id: str, at: str) -> dict[str, Any] | None:
    """返回某实体在指定时点生效的版本行，不存在则为 None。"""

    row = connection.execute(
        "SELECT * FROM ref_versions WHERE entity_type=? AND entity_id=? AND valid_from<=? "
        "AND (superseded_at IS NULL OR superseded_at>?) "
        "ORDER BY version DESC LIMIT 1",
        (entity_type, entity_id, at, at),
    ).fetchone()
    if row is None:
        return None
    return {"version": row["version"], "payload": json.loads(row["payload_json"]),
            "valid_from": row["valid_from"], "superseded_at": row["superseded_at"]}


def build_snapshot(connection, at: str) -> dict[str, Any]:
    """构建某一时点的全量通道视图，供规划器与复原接口使用。"""

    snapshot: dict[str, Any] = {
        "at": at,
        "segments": {},
        "borders": {},
        "capacities": {},
        "restrictions": [],
        "deadlines": {},
        "rates": [],
        "advisories": [],
    }
    rows = connection.execute(
        "SELECT * FROM ref_versions WHERE valid_from<=? "
        "AND (superseded_at IS NULL OR superseded_at>?)",
        (at, at),
    ).fetchall()
    for row in rows:
        entry = {"entity_id": row["entity_id"], "version": row["version"],
                 "payload": json.loads(row["payload_json"])}
        kind = row["entity_type"]
        if kind == "segment":
            snapshot["segments"][row["entity_id"]] = entry
        elif kind == "border_calendar":
            snapshot["borders"][row["entity_id"]] = entry
        elif kind == "capacity":
            snapshot["capacities"][row["entity_id"]] = entry
        elif kind == "cargo_restriction":
            snapshot["restrictions"].append(entry)
        elif kind == "customer_deadline":
            snapshot["deadlines"][row["entity_id"]] = entry
        elif kind == "contract_rate":
            snapshot["rates"].append(entry)
        elif kind == "advisory":
            # 通告是否进入快照取决于版本生效区间（发布生命周期）；
            # 事件窗口对某班期是否适用由规划器逐班期判断。
            snapshot["advisories"].append(entry)
    return snapshot
