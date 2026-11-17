# grpc-deadline-router
Python gRPC 请求路由库，处理 deadline、幂等重试、连接失败和取消传播

## 核心不变量

- **下游预算只减不增**：每次尝试的 `downstream_deadline = min(原始 deadline, now + 剩余预算)`，
  重试 / hedge 绝不会把上游 deadline 重置成更晚的值。
- **取消传播**：客户端取消、deadline 到期或首个成功响应到达时，
  所有在途尝试都会被取消（`Attempt.cancelled = True`）。
- **非幂等保护**：非幂等方法一旦发送，绝不重试、绝不 hedge，避免重复执行。
- **重复响应**：hedge 场景下首个成功响应获胜，同时到达的其余成功响应被丢弃。

## 路由决策

`Router.decide(now, ctx, connections, attempts)` 输入原始 deadline、剩余预算、
方法幂等性、连接状态和重试历史，输出四种决策之一：

| 决策 | 条件 |
| --- | --- |
| `SEND` | 首次尝试，存在 READY 连接且预算充足 |
| `RETRY` | 幂等方法、上次失败、重试预算未耗尽，带指数退避 + 抖动 |
| `HEDGE` | 幂等方法、在途尝试超过 `hedge_delay` 未响应、存在空闲 READY 连接 |
| `FAIL` | 预算不足 / 无 READY 连接 / 非幂等已发送 / 重试预算耗尽 / 退避会超出 deadline |

## 重试预算（retry budget）

`retry_budget` 是**首次尝试之外**允许的重试与 hedge 总次数，默认 2：

- 每次重试和每次 hedge 都消耗 1 点预算，二者共享同一额度；
  允许的总尝试次数为 `1 + retry_budget`，同时受 `max_attempts` 上限约束。
- 预算耗尽后立即失败，返回最后一次尝试的错误，不再发起新调用，
  防止重试风暴把后端打挂。
- 重试间隔为指数退避加对称抖动：
  `delay = min(max_backoff, base_backoff * 2^(n-1)) * (1 ± jitter)`，
  抖动避免多客户端同步重试；若退避等待会超出 deadline，则直接失败不再等待。
- 非幂等方法不消耗预算——首次失败后直接终止，保证不会重复执行。

## 测试

```bash
python -m unittest
```

覆盖：deadline 快耗尽立即失败、下游预算钳制、幂等 / 非幂等方法、
抖动退避、连接切换、hedge 竞争、重复响应丢弃、取消传播。
每个 `Attempt` 记录连接、下游 deadline、结果与终止原因，
`RouteResult` 汇总最终状态与获胜尝试。
