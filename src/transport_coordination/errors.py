"""领域服务使用的业务异常。"""


class DomainError(Exception):
    """所有可预期业务异常的基类。"""

    code = "domain_error"
    status = 400


class ValidationError(DomainError):
    """输入字段不符合业务约束。"""

    code = "validation_error"


class NotFoundError(DomainError):
    """请求引用的业务对象不存在。"""

    code = "not_found"
    status = 404


class PermissionDenied(DomainError):
    """操作者没有执行当前动作的权限。"""

    code = "permission_denied"
    status = 403


class ConflictError(DomainError):
    """请求编号或业务唯一键与既有内容冲突。"""

    code = "conflict"
    status = 409


class LockFailed(DomainError):
    """全分段锁定未能完成，调用方应放弃该方案。"""

    code = "lock_failed"
    status = 409


class ApprovalRequired(DomainError):
    """方案尚缺少必要审批，不能切换。"""

    code = "approval_required"
    status = 409
