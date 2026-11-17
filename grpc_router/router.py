"""gRPC 路由决策核心。

输入原始 deadline、剩余预算、方法幂等性、连接状态和重试历史，
决定发送、重试、hedge 或立即失败。核心不变量：

1. 下游预算只能减少，不能延长：每次尝试的 downstream_deadline 都不会
   超过上游原始 deadline。
2. 取消必须传播到所有已发送调用：客户端取消、deadline 到期或首个成功
   响应到达时，所有在途尝试都会被标记取消。
3. 非幂等方法一旦发送，绝不重试、绝不 hedge，避免重复执行。
"""

from __future__ import annotations

import enum
import random
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple


class Action(enum.Enum):
    SEND = "send"
    RETRY = "retry"
    HEDGE = "hedge"
    FAIL = "fail"


class Status(enum.Enum):
    OK = "OK"
    UNAVAILABLE = "UNAVAILABLE"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    CANCELLED = "CANCELLED"


class ConnState(enum.Enum):
    READY = "ready"
    CONNECTING = "connecting"
    TRANSIENT_FAILURE = "transient_failure"
    SHUTDOWN = "shutdown"


@dataclass
class Decision:
    action: Action
    connection: Optional[str] = None
    downstream_deadline: Optional[float] = None
    delay: float = 0.0
    reason: str = ""
    fail_status: Optional[Status] = None


@dataclass
class Attempt:
    index: int
    action: Action
    connection: str
    started_at: float
    downstream_deadline: float
    outcome: Optional[Status] = None
    finished_at: Optional[float] = None
    reason: str = ""
    cancelled: bool = False


@dataclass
class CallContext:
    method: str
    idempotent: bool
    deadline: float  # 绝对时间（相对时钟，从 0 开始）
    cancel_at: Optional[float] = None  # 客户端取消时刻，None 表示不取消


@dataclass
class RouteResult:
    status: Status
    reason: str
    attempts: List[Attempt] = field(default_factory=list)
    response_from: Optional[int] = None


class Router:
    """路由决策器。所有时间使用同一单调时钟（秒）。"""

    def __init__(
        self,
        max_attempts: int = 4,
        retry_budget: int = 2,
        base_backoff: float = 0.05,
        max_backoff: float = 1.0,
        jitter: float = 0.2,
        hedge_delay: float = 0.05,
        min_budget: float = 0.005,
        rng: Optional[random.Random] = None,
    ):
        self.max_attempts = max_attempts
        self.retry_budget = retry_budget  # 首次尝试之外允许的重试/hedge 总次数
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.jitter = jitter
        self.hedge_delay = hedge_delay
        self.min_budget = min_budget
        self.rng = rng if rng is not None else random.Random(0)

    @staticmethod
    def effective_deadline(now: float, original_deadline: float, remaining_budget: float) -> float:
        """下游 deadline 钳制：取原始 deadline 与剩余预算的较小者，只减不增。"""
        return min(original_deadline, now + remaining_budget)

    def backoff(self, retry_number: int) -> float:
        """指数退避 + 对称抖动，retry_number 从 1 开始。"""
        base = min(self.max_backoff, self.base_backoff * (2 ** (retry_number - 1)))
        spread = self.jitter * base
        return base + self.rng.uniform(-spread, spread)

    def decide(
        self,
        now: float,
        ctx: CallContext,
        connections: Dict[str, ConnState],
        attempts: List[Attempt],
        exclude: frozenset = frozenset(),
        force_hedge: bool = False,
    ) -> Decision:
        remaining = ctx.deadline - now
        if remaining <= self.min_budget:
            return Decision(
                Action.FAIL,
                reason="剩余预算不足，立即失败",
                fail_status=Status.DEADLINE_EXCEEDED,
            )

        ready = [
            name
            for name, state in connections.items()
            if state is ConnState.READY and name not in exclude
        ]
        if not ready:
            return Decision(
                Action.FAIL,
                reason="无 READY 连接",
                fail_status=Status.UNAVAILABLE,
            )

        if attempts and not ctx.idempotent:
            return Decision(
                Action.FAIL,
                reason="非幂等方法已发送，禁止重试/hedge",
                fail_status=attempts[-1].outcome or Status.UNAVAILABLE,
            )

        if len(attempts) >= self.max_attempts:
            return Decision(
                Action.FAIL,
                reason="达到最大尝试次数",
                fail_status=attempts[-1].outcome or Status.UNAVAILABLE,
            )

        if attempts and len(attempts) > self.retry_budget:
            return Decision(
                Action.FAIL,
                reason="重试预算耗尽",
                fail_status=attempts[-1].outcome or Status.UNAVAILABLE,
            )

        failed_conns = {
            a.connection for a in attempts if a.outcome not in (None, Status.OK)
        }
        candidates = [c for c in ready if c not in failed_conns] or ready
        connection = candidates[0]

        if force_hedge:
            action, delay = Action.HEDGE, 0.0
        elif not attempts:
            action, delay = Action.SEND, 0.0
        else:
            action = Action.RETRY
            delay = self.backoff(len(attempts))

        if now + delay > ctx.deadline - self.min_budget:
            return Decision(
                Action.FAIL,
                reason="退避等待会超出 deadline，立即失败",
                fail_status=Status.DEADLINE_EXCEEDED,
            )

        downstream_deadline = self.effective_deadline(
            now + delay, ctx.deadline, ctx.deadline - now - delay
        )
        return Decision(
            action,
            connection=connection,
            downstream_deadline=downstream_deadline,
            delay=delay,
            reason="ok",
        )

    # ------------------------------------------------------------------
    # 离散事件执行器：驱动尝试、hedge 定时器、deadline 与取消传播。
    # ------------------------------------------------------------------

    def run(
        self,
        ctx: CallContext,
        backend: "ScriptedBackend",
        connections: Dict[str, ConnState],
    ) -> RouteResult:
        now = 0.0
        attempts: List[Attempt] = []
        inflight: Dict[int, Tuple[float, Status]] = {}
        scheduled: Optional[Tuple[float, Decision]] = None

        def cancel_all(reason: str) -> None:
            for idx in list(inflight):
                att = attempts[idx]
                att.cancelled = True
                att.outcome = Status.CANCELLED
                att.finished_at = now
                att.reason = reason
                del inflight[idx]

        while True:
            if not inflight and scheduled is None:
                decision = self.decide(now, ctx, connections, attempts)
                if decision.action is Action.FAIL:
                    return RouteResult(
                        status=decision.fail_status or Status.UNAVAILABLE,
                        reason=decision.reason,
                        attempts=attempts,
                    )
                scheduled = (now + decision.delay, decision)

            events: List[Tuple[float, int, tuple]] = []
            if scheduled is not None:
                send_at, send_decision = scheduled
                events.append((send_at, 0, ("send", send_decision)))
            for idx, (fin, st) in inflight.items():
                events.append((fin, 1, ("complete", idx)))
            if self._can_hedge(ctx, connections, attempts, inflight):
                hedge_at = min(attempts[i].started_at for i in inflight) + self.hedge_delay
                events.append((hedge_at, 2, ("hedge", None)))
            if ctx.cancel_at is not None:
                events.append((ctx.cancel_at, 3, ("cancel", None)))

            t, _, (kind, payload) = min(events)

            if t >= ctx.deadline:
                now = ctx.deadline
                cancel_all("上游 deadline 到期，取消传播到所有在途调用")
                return RouteResult(
                    status=Status.DEADLINE_EXCEEDED,
                    reason="deadline 耗尽",
                    attempts=attempts,
                )

            now = t

            if kind == "cancel":
                cancel_all("客户端取消，传播到所有在途调用")
                return RouteResult(
                    status=Status.CANCELLED,
                    reason="客户端取消",
                    attempts=attempts,
                )

            if kind == "send":
                scheduled = None
                self._start(ctx, backend, payload, now, attempts, inflight)
            elif kind == "hedge":
                used = {attempts[i].connection for i in inflight}
                decision = self.decide(
                    now, ctx, connections, attempts,
                    exclude=frozenset(used), force_hedge=True,
                )
                if decision.action is not Action.FAIL:
                    self._start(ctx, backend, decision, now, attempts, inflight)
            else:  # complete
                idx = payload
                fin, st = inflight.pop(idx)
                att = attempts[idx]
                att.outcome = st
                att.finished_at = fin
                if st is Status.OK:
                    att.reason = "成功"
                    # 同一时刻完成的其他成功响应：重复响应，丢弃
                    for other_idx, (ofin, ost) in list(inflight.items()):
                        if ofin <= now and ost is Status.OK:
                            dup = attempts[other_idx]
                            dup.outcome = Status.OK
                            dup.finished_at = ofin
                            dup.reason = "重复响应，已丢弃"
                            del inflight[other_idx]
                    cancel_all("已有成功响应，取消其余在途调用")
                    return RouteResult(
                        status=Status.OK,
                        reason=f"第 {idx} 次尝试成功",
                        attempts=attempts,
                        response_from=idx,
                    )
                att.reason = f"后端返回 {st.value}"

    def _can_hedge(
        self,
        ctx: CallContext,
        connections: Dict[str, ConnState],
        attempts: List[Attempt],
        inflight: Dict[int, Tuple[float, Status]],
    ) -> bool:
        if not inflight or not ctx.idempotent:
            return False
        if len(attempts) >= self.max_attempts or len(attempts) > self.retry_budget:
            return False
        used = {attempts[i].connection for i in inflight}
        return any(
            state is ConnState.READY and name not in used
            for name, state in connections.items()
        )

    def _start(
        self,
        ctx: CallContext,
        backend: "ScriptedBackend",
        decision: Decision,
        now: float,
        attempts: List[Attempt],
        inflight: Dict[int, Tuple[float, Status]],
    ) -> None:
        idx = len(attempts)
        att = Attempt(
            index=idx,
            action=decision.action,
            connection=decision.connection,
            started_at=now,
            downstream_deadline=decision.downstream_deadline,
        )
        attempts.append(att)
        latency, status = backend.start(decision.connection, idx, decision.downstream_deadline)
        finish = now + latency
        if finish > decision.downstream_deadline:
            finish, status = decision.downstream_deadline, Status.DEADLINE_EXCEEDED
        inflight[idx] = (finish, status)


class ScriptedBackend:
    """可编排的模拟后端：按连接返回 (latency, status) 序列，并统计执行次数。"""

    def __init__(self, default_latency: float = 0.01):
        self.default_latency = default_latency
        self.scripts: Dict[str, Deque[Tuple[float, Status]]] = {}
        self.executions: Counter = Counter()

    def script(self, connection: str, outcomes) -> None:
        self.scripts[connection] = deque(outcomes)

    def start(self, connection: str, attempt_index: int, deadline: float):
        self.executions[connection] += 1
        queue = self.scripts.get(connection)
        if queue:
            return queue.popleft()
        return (self.default_latency, Status.OK)
