"""领域服务共享的鉴权、校验、时钟与幂等助手。"""

from __future__ import annotations

import re
from typing import Any, Callable

from .audit import canonical_json, digest
from .clock import Clock, SystemClock
from .access import load_actor, require_roles
from .errors import ConflictError, ValidationError
from .models import Actor, WriteReceipt
from .storage import Database

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")


class ServiceBase:
    """所有领域服务的公共基类。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.database = database
        self.clock = clock or SystemClock()

    def _now(self) -> str:
        return self.clock.now().isoformat().replace("+00:00", "Z")

    def _identifier(self, value: str, field: str) -> str:
        value = str(value).strip()
        if not IDENTIFIER.fullmatch(value):
            raise ValidationError(f"{field} 格式无效")
        return value

    def _text(self, value: str, field: str, limit: int = 200) -> str:
        value = str(value).strip()
        if not value or len(value) > limit:
            raise ValidationError(f"{field} 不能为空且不能超过 {limit} 个字符")
        return value

    def _actor(self, connection, actor_id: str) -> Actor:
        return load_actor(connection, actor_id)

    def _require(self, actor: Actor, *roles: str) -> None:
        require_roles(actor, *roles)

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any],
                    create: Callable[[], tuple[str, str, dict[str, Any]]]) -> WriteReceipt:
        """在事务内执行带幂等回执的写入。"""

        request_id = self._identifier(request_id, "request_id")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return WriteReceipt(request_id, row["resource_type"], row["resource_id"], True)
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,"
            "resource_id,response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id,
             canonical_json(response), self._now()),
        )
        return WriteReceipt(request_id, resource_type, resource_id, False)

    def _replay_receipt(self, connection, request_id: str, action: str,
                        payload: dict[str, Any]):
        """若请求编号已处理过，返回原回执；否则返回规范化后的 request_id 与 None。

        状态机类操作必须在状态校验之前调用，保证客户端超时重试时得到同一结果，
        而不是被“方案已锁定/已交接”之类的状态错误拒绝。
        """

        request_id = self._identifier(request_id, "request_id")
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row is None:
            return request_id, None
        if row["action"] != action or row["payload_hash"] != digest(payload):
            raise ConflictError("request_id 已被不同内容使用")
        return request_id, WriteReceipt(request_id, row["resource_type"],
                                        row["resource_id"], True)
