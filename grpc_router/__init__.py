"""Python gRPC 路由决策核心。

负责在上游 deadline 约束下做出发送 / 重试 / hedge / 立即失败的决策，
保证下游预算只减不增，取消传播到所有已发送调用。
"""

from .router import (
    Action,
    Attempt,
    CallContext,
    ConnState,
    Decision,
    RouteResult,
    Router,
    ScriptedBackend,
    Status,
)

__all__ = [
    "Action",
    "Attempt",
    "CallContext",
    "ConnState",
    "Decision",
    "RouteResult",
    "Router",
    "ScriptedBackend",
    "Status",
]
