"""操作者鉴权与角色校验的共享助手。"""

from __future__ import annotations

from .errors import NotFoundError, PermissionDenied
from .models import Actor


def load_actor(connection, actor_id: str) -> Actor:
    """读取操作者并在不存在或停用时拒绝。"""

    row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
    if row is None:
        raise NotFoundError("操作者不存在")
    actor = Actor(row["actor_id"], row["display_name"], row["role"],
                  row["organization_id"], bool(row["active"]))
    if not actor.active:
        raise PermissionDenied("操作者已停用")
    return actor


def require_roles(actor: Actor, *roles: str) -> None:
    """要求操作者属于给定角色之一。"""

    if actor.role not in roles:
        raise PermissionDenied("当前角色不能执行该动作")
