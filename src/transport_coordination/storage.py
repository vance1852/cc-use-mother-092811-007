"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
-- 国际通道韧性编排：线路分段、口岸日历、承运能力、货物限制、客户时限、
-- 合同费用与风险通告统一按“实体 + 不可变版本”组织，superseded_at 闭合旧版本生效区间。
CREATE TABLE IF NOT EXISTS ref_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    superseded_at TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (entity_type, entity_id, version)
);
CREATE INDEX IF NOT EXISTS idx_ref_versions_type ON ref_versions(entity_type);
-- 在途货物及其状态机：booked -> switched -> handed_over -> delivered。
CREATE TABLE IF NOT EXISTS shipments (
    cargo_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    cargo_tags_json TEXT NOT NULL,
    weight REAL NOT NULL,
    state TEXT NOT NULL,
    responsible_party TEXT,
    eta TEXT,
    active_plan_id TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- 货物每一代行程（原始预订或某方案提交后生成），旧行 released_at 闭合以支持时点复原。
CREATE TABLE IF NOT EXISTS shipment_legs (
    leg_id TEXT PRIMARY KEY,
    cargo_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    segment_id TEXT NOT NULL,
    capacity_id TEXT NOT NULL,
    carrier_id TEXT NOT NULL,
    carrier_name TEXT NOT NULL,
    departs_at TEXT NOT NULL,
    arrives_at TEXT NOT NULL,
    booked_cost REAL NOT NULL,
    currency TEXT NOT NULL,
    source_plan_id TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_shipment_legs_cargo ON shipment_legs(cargo_id);
-- 货物状态流水：任意时点的状态、责任方、ETA 从该流水复原。
CREATE TABLE IF NOT EXISTS shipment_journal (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    cargo_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shipment_journal_cargo ON shipment_journal(cargo_id, sequence);
-- 改道方案；terminal=0 的方案每件货物至多一个（部分唯一索引保证竞争方案互斥）。
CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    cargo_id TEXT NOT NULL,
    status TEXT NOT NULL,
    terminal INTEGER NOT NULL CHECK(terminal IN (0, 1)),
    candidate_json TEXT NOT NULL,
    basis_json TEXT NOT NULL,
    cost_json TEXT NOT NULL,
    required_approvals_json TEXT NOT NULL,
    failure_json TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    committed_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_plan ON plans(cargo_id) WHERE terminal = 0;
CREATE TABLE IF NOT EXISTS plan_approvals (
    plan_id TEXT NOT NULL,
    approval_key TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    note TEXT,
    PRIMARY KEY (plan_id, approval_key)
);
CREATE TABLE IF NOT EXISTS plan_history (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_plan_history_plan ON plan_history(plan_id, sequence);
-- 能力占用：原始预订与方案锁定统一建模，consumed 表示随方案提交转为实际使用。
CREATE TABLE IF NOT EXISTS capacity_holds (
    hold_id TEXT PRIMARY KEY,
    cargo_id TEXT NOT NULL,
    plan_id TEXT,
    capacity_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    slots INTEGER NOT NULL CHECK(slots >= 1),
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    departs_at TEXT NOT NULL,
    locked_at TEXT NOT NULL,
    consumed_at TEXT,
    released_at TEXT,
    release_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_holds_capacity ON capacity_holds(capacity_id, status);
CREATE INDEX IF NOT EXISTS idx_holds_cargo ON capacity_holds(cargo_id);
-- 只追加费用台账：任何补偿动作都不删除或改写已发生费用。
CREATE TABLE IF NOT EXISTS cost_entries (
    entry_id TEXT PRIMARY KEY,
    cargo_id TEXT NOT NULL,
    plan_id TEXT,
    category TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cost_cargo ON cost_entries(cargo_id);
-- 中断影响清单：recovery_action 记录恢复消息到达后的处理（含已交接保护）。
CREATE TABLE IF NOT EXISTS disruption_impacts (
    impact_id TEXT PRIMARY KEY,
    advisory_id TEXT NOT NULL,
    cargo_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    capacity_id TEXT,
    departs_at TEXT,
    state_at_impact TEXT NOT NULL,
    handed INTEGER NOT NULL,
    recovery_action TEXT,
    detected_at TEXT NOT NULL,
    UNIQUE(advisory_id, cargo_id, segment_id)
);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
