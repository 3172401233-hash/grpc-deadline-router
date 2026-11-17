import random
import unittest

from grpc_router import (
    Action,
    CallContext,
    ConnState,
    Router,
    ScriptedBackend,
    Status,
)


def ctx(idempotent=True, deadline=5.0, cancel_at=None, method="/svc/Method"):
    return CallContext(
        method=method, idempotent=idempotent, deadline=deadline, cancel_at=cancel_at
    )


class DeadlineTest(unittest.TestCase):
    def test_deadline_nearly_exhausted_fails_fast(self):
        router = Router(min_budget=0.005)
        backend = ScriptedBackend()
        result = router.run(
            ctx(deadline=0.003), backend, {"a": ConnState.READY}
        )
        self.assertEqual(result.status, Status.DEADLINE_EXCEEDED)
        self.assertEqual(result.attempts, [])
        self.assertEqual(sum(backend.executions.values()), 0)
        self.assertIn("预算不足", result.reason)

    def test_downstream_budget_never_extends(self):
        # 剩余预算比原始 deadline 更紧：取较小者
        self.assertEqual(Router.effective_deadline(1.0, 5.0, 2.0), 3.0)
        # 剩余预算更宽：钳制到原始 deadline，绝不延长
        self.assertEqual(Router.effective_deadline(1.0, 5.0, 10.0), 5.0)

        router = Router(jitter=0.0)
        backend = ScriptedBackend()
        backend.script("a", [(0.01, Status.UNAVAILABLE)])
        result = router.run(ctx(deadline=1.0), backend, {"a": ConnState.READY})
        self.assertEqual(result.status, Status.OK)
        self.assertEqual(len(result.attempts), 2)
        for attempt in result.attempts:
            self.assertLessEqual(attempt.downstream_deadline, 1.0)

    def test_deadline_expires_midflight_cancels_attempt(self):
        router = Router()
        backend = ScriptedBackend()
        backend.script("a", [(0.5, Status.OK)])
        result = router.run(ctx(deadline=0.1), backend, {"a": ConnState.READY})
        self.assertEqual(result.status, Status.DEADLINE_EXCEEDED)
        self.assertEqual(len(result.attempts), 1)
        self.assertTrue(result.attempts[0].cancelled)
        self.assertEqual(result.attempts[0].outcome, Status.CANCELLED)
        self.assertIn("取消传播", result.attempts[0].reason)

    def test_backoff_would_pass_deadline_fails_fast(self):
        router = Router(base_backoff=0.05, jitter=0.0, min_budget=0.005)
        backend = ScriptedBackend()
        backend.script("a", [(0.01, Status.UNAVAILABLE)])
        result = router.run(ctx(deadline=0.06), backend, {"a": ConnState.READY})
        self.assertEqual(result.status, Status.DEADLINE_EXCEEDED)
        self.assertEqual(len(result.attempts), 1)
        self.assertIn("退避", result.reason)


class IdempotencyTest(unittest.TestCase):
    def test_idempotent_method_retries_and_succeeds(self):
        router = Router(jitter=0.0)
        backend = ScriptedBackend()
        backend.script("a", [(0.01, Status.UNAVAILABLE), (0.01, Status.OK)])
        result = router.run(ctx(idempotent=True), backend, {"a": ConnState.READY})
        self.assertEqual(result.status, Status.OK)
        self.assertEqual([a.action for a in result.attempts], [Action.SEND, Action.RETRY])
        self.assertEqual(backend.executions["a"], 2)

    def test_non_idempotent_method_never_retried(self):
        router = Router(jitter=0.0)
        backend = ScriptedBackend()
        backend.script("a", [(0.01, Status.UNAVAILABLE), (0.01, Status.OK)])
        result = router.run(ctx(idempotent=False), backend, {"a": ConnState.READY})
        self.assertEqual(result.status, Status.UNAVAILABLE)
        self.assertEqual(len(result.attempts), 1)
        self.assertEqual(backend.executions["a"], 1)
        self.assertIn("非幂等", result.reason)

    def test_non_idempotent_method_never_hedged(self):
        router = Router(hedge_delay=0.01)
        backend = ScriptedBackend()
        backend.script("a", [(0.05, Status.OK)])
        backend.script("b", [(0.01, Status.OK)])
        result = router.run(
            ctx(idempotent=False),
            backend,
            {"a": ConnState.READY, "b": ConnState.READY},
        )
        self.assertEqual(result.status, Status.OK)
        self.assertEqual(len(result.attempts), 1)
        self.assertEqual(backend.executions["b"], 0)


class RetryBackoffTest(unittest.TestCase):
    def test_jittered_backoff_bounded_and_deterministic(self):
        router = Router(base_backoff=0.1, jitter=0.2, rng=random.Random(42))
        delays = [router.backoff(1) for _ in range(200)]
        for d in delays:
            self.assertGreaterEqual(d, 0.08)
            self.assertLessEqual(d, 0.12)
        self.assertGreater(len(set(delays)), 1)

        again = Router(base_backoff=0.1, jitter=0.2, rng=random.Random(42))
        self.assertEqual(delays[0], again.backoff(1))

    def test_backoff_capped_by_max_backoff(self):
        router = Router(base_backoff=0.1, max_backoff=0.15, jitter=0.2,
                        rng=random.Random(1))
        for _ in range(100):
            self.assertLessEqual(router.backoff(5), 0.15 * 1.2 + 1e-9)

    def test_retry_budget_exhausted(self):
        router = Router(retry_budget=2, jitter=0.0)
        backend = ScriptedBackend()
        backend.script("a", [(0.01, Status.UNAVAILABLE)] * 3 + [(0.01, Status.OK)])
        result = router.run(ctx(), backend, {"a": ConnState.READY})
        self.assertEqual(result.status, Status.UNAVAILABLE)
        self.assertEqual(len(result.attempts), 3)  # 1 次首发 + 2 次重试
        self.assertEqual(backend.executions["a"], 3)
        self.assertIn("重试预算耗尽", result.reason)


class ConnectionTest(unittest.TestCase):
    def test_no_ready_connection_fails(self):
        router = Router()
        backend = ScriptedBackend()
        result = router.run(
            ctx(),
            backend,
            {"a": ConnState.TRANSIENT_FAILURE, "b": ConnState.SHUTDOWN},
        )
        self.assertEqual(result.status, Status.UNAVAILABLE)
        self.assertEqual(result.attempts, [])
        self.assertEqual(sum(backend.executions.values()), 0)

    def test_retry_switches_to_healthy_connection(self):
        router = Router(jitter=0.0)
        backend = ScriptedBackend()
        backend.script("a", [(0.01, Status.UNAVAILABLE)])
        backend.script("b", [(0.01, Status.OK)])
        result = router.run(
            ctx(),
            backend,
            {"a": ConnState.READY, "b": ConnState.READY},
        )
        self.assertEqual(result.status, Status.OK)
        self.assertEqual([a.connection for a in result.attempts], ["a", "b"])

    def test_only_failed_connection_ready_uses_it(self):
        router = Router()
        backend = ScriptedBackend()
        result = router.run(
            ctx(),
            backend,
            {"a": ConnState.CONNECTING, "b": ConnState.READY},
        )
        self.assertEqual(result.status, Status.OK)
        self.assertEqual(result.attempts[0].connection, "b")


class HedgeTest(unittest.TestCase):
    def test_hedge_first_response_wins_and_cancels_other(self):
        router = Router(hedge_delay=0.05)
        backend = ScriptedBackend()
        backend.script("a", [(0.20, Status.OK)])
        backend.script("b", [(0.05, Status.OK)])
        result = router.run(
            ctx(),
            backend,
            {"a": ConnState.READY, "b": ConnState.READY},
        )
        self.assertEqual(result.status, Status.OK)
        self.assertEqual(len(result.attempts), 2)
        self.assertEqual(result.attempts[1].action, Action.HEDGE)
        self.assertEqual(result.response_from, 1)
        loser = result.attempts[0]
        self.assertTrue(loser.cancelled)
        self.assertEqual(loser.outcome, Status.CANCELLED)
        self.assertIn("取消", loser.reason)

    def test_duplicate_response_discarded(self):
        # 两个 hedge 尝试同一时刻完成：先处理者获胜，后者为重复响应
        router = Router(hedge_delay=0.05)
        backend = ScriptedBackend()
        backend.script("a", [(0.20, Status.OK)])
        backend.script("b", [(0.15, Status.OK)])  # 0.05 + 0.15 == 0.20
        result = router.run(
            ctx(),
            backend,
            {"a": ConnState.READY, "b": ConnState.READY},
        )
        self.assertEqual(result.status, Status.OK)
        self.assertEqual(result.response_from, 0)
        dup = result.attempts[1]
        self.assertEqual(dup.outcome, Status.OK)
        self.assertFalse(dup.cancelled)
        self.assertIn("重复响应", dup.reason)

    def test_client_cancel_propagates_to_all_inflight(self):
        router = Router(hedge_delay=0.05)
        backend = ScriptedBackend()
        backend.script("a", [(0.5, Status.OK)])
        backend.script("b", [(0.5, Status.OK)])
        result = router.run(
            ctx(cancel_at=0.08),
            backend,
            {"a": ConnState.READY, "b": ConnState.READY},
        )
        self.assertEqual(result.status, Status.CANCELLED)
        self.assertEqual(len(result.attempts), 2)
        for attempt in result.attempts:
            self.assertTrue(attempt.cancelled)
            self.assertEqual(attempt.outcome, Status.CANCELLED)
            self.assertIn("客户端取消", attempt.reason)


if __name__ == "__main__":
    unittest.main()
