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
CREATE TABLE IF NOT EXISTS corridor_ref_versions (
    kind TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    published_at TEXT NOT NULL,
    published_by TEXT NOT NULL,
    PRIMARY KEY(kind, entity_id, version)
);
CREATE TABLE IF NOT EXISTS corridor_shipments (
    shipment_id TEXT PRIMARY KEY,
    cargo_type TEXT NOT NULL,
    hazmat_class TEXT,
    weight_kg REAL NOT NULL,
    units REAL NOT NULL,
    origin_node TEXT NOT NULL,
    destination_node TEXT NOT NULL,
    customer_ref TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corridor_deadlines (
    shipment_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    must_arrive_by TEXT NOT NULL,
    published_at TEXT NOT NULL,
    published_by TEXT NOT NULL,
    PRIMARY KEY(shipment_id, version)
);
CREATE TABLE IF NOT EXISTS corridor_notices (
    notice_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    severity TEXT NOT NULL,
    detail TEXT NOT NULL,
    status TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    recovered_at TEXT,
    recovered_by TEXT
);
CREATE TABLE IF NOT EXISTS corridor_evaluations (
    evaluation_id TEXT PRIMARY KEY,
    notice_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    basis_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corridor_plans (
    plan_id TEXT PRIMARY KEY,
    evaluation_id TEXT,
    shipment_id TEXT NOT NULL,
    trigger_notice_id TEXT,
    path_json TEXT NOT NULL,
    departures_json TEXT NOT NULL,
    eta TEXT,
    total_cost REAL,
    currency TEXT NOT NULL,
    feasible INTEGER NOT NULL,
    explanation_json TEXT NOT NULL,
    required_approvals_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corridor_plan_status_history (
    history_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    status TEXT NOT NULL,
    note TEXT NOT NULL,
    at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corridor_holds (
    hold_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    capacity_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    units REAL NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT
);
CREATE TABLE IF NOT EXISTS corridor_plan_approvals (
    plan_id TEXT NOT NULL,
    role TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    note TEXT NOT NULL,
    at TEXT NOT NULL,
    PRIMARY KEY(plan_id, role)
);
CREATE TABLE IF NOT EXISTS corridor_shipment_routes (
    shipment_id TEXT PRIMARY KEY,
    baseline_plan_id TEXT NOT NULL,
    effective_plan_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    switched_at TEXT
);
CREATE TABLE IF NOT EXISTS corridor_checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    shipment_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    event TEXT NOT NULL,
    at TEXT NOT NULL,
    recorded_by TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corridor_fees (
    fee_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    shipment_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    fee_type TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    note TEXT NOT NULL,
    at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS corridor_decisions (
    decision_id TEXT PRIMARY KEY,
    shipment_id TEXT,
    plan_id TEXT,
    notice_id TEXT,
    kind TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    at TEXT NOT NULL
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
