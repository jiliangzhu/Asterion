# P1 P2 AlphaDesk Remaining Migration Checklist

**版本**: v1.2  
**更新日期**: 2026-03-10  
**目标**: 明确 `P0` 之后仍需参考 AlphaDesk 的剩余模块，并给出 `P1/P2` 的迁移顺序、退出条件和新建 Asterion 独立 Git 仓库的判断门槛。

---

## 1. 当前结论

截至 `P2-21` 关闭时：

- Asterion 运行代码已经不直接 import AlphaDesk
- AlphaDesk 台账中所有 `direct_reuse` / `keep_shell_rewrite_content` 项都已完成迁移或改判
- `readiness_checker_v1`、`ui_lite_db`、`dagster_asterion/resources.py`、`dagster_asterion/schedules.py` 已完成收口

因此：

- **当前已经可以宣告“AlphaDesk 可复用代码都已迁完”**
- **当前可以以 Asterion 作为唯一维护仓库继续开发**
- **当前可以新建并长期维护独立的 Asterion Git 仓库**

---

## 2. AlphaDesk Exit Gate

以下条件已经满足，因此 AlphaDesk Exit Gate 现判定为 `EXIT_READY`：

1. [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md) 中所有 `direct_reuse` 和 `keep_shell_rewrite_content` 项都已变成 `ported`，或者被正式改判为 `do_not_port`
2. `ws_subscribe.py` / `ws_agg_v3.py` 的去留已经明确，并在 Asterion 内有最终落点或被正式裁撤
3. Asterion 代码仓内不再需要通过阅读 AlphaDesk 源码来完成 `watch-only / replay / execution foundation / monitoring / ui replica` 的实现
4. Asterion 代码树内不存在任何对 AlphaDesk 代码路径、包名、环境变量的运行时依赖
5. README、Project Plan、Roadmap、Migration Ledger 对“已迁完/未迁完”的口径一致

当前结论：

- 后续开发不再需要通过阅读 AlphaDesk 源码完成实现
- AlphaDesk 可以降级为历史参考仓库

在此前阶段未满足 Exit Gate 时，AlphaDesk 曾被保留为：

- 只读参考仓库
- 迁移源代码仓库
- 语义比对和回归参考

---

## 3. P1 迁移清单

`P1` 目标是让 Weather MVP 的 `watch-only / replay / cold path` 不再需要回头看 AlphaDesk。

### 3.1 P1 必迁模块

| Source Module | Target Path | Classification | P1 Priority | Why |
| --- | --- | --- | --- | --- |
| `alphadesk/ws_subscribe.py` | `asterion_core/ws/ws_subscribe.py` | direct_reuse | P1-blocker | watch-only 若要接实时盘口/事件流，需要先把 WS 订阅底座迁入 |
| `alphadesk/ws_agg_v3.py` | `asterion_core/ws/ws_agg_v3.py` | direct_reuse | P1-blocker | replay/live monitor 都需要统一的 WS 聚合壳 |
| `alphadesk/strategies/base.py` | `asterion_core/runtime/strategy_base.py` | direct_reuse | P1-high | 为 watch-only strategy/runtime 提供统一壳 |
| `alphadesk/watch_only_gate_v3.py` | `asterion_core/execution/watch_only_gate_v3.py` | direct_reuse | P1-high | watch-only 决策管线的最小门控壳 |
| `alphadesk/health_monitor_v1.py` | `asterion_core/monitoring/health_monitor_v1.py` | direct_reuse | P1-high | queue/ws/degrade 健康采集是 watch-only 运行所需 |
| `alphadesk/ui_db_replica.py` | `asterion_core/ui/ui_db_replica.py` | direct_reuse | P1-medium | operator 读路径和只读副本能力 |

### 3.2 P1 可并行但不阻塞 watch-only 开工

| Source Module | Target Path | Classification | Why |
| --- | --- | --- | --- |
| `dagster_alphadesk/resources.py` | `dagster_asterion/resources.py` | direct_reuse | 冷路径编排资源层很薄，可尽早迁但不阻塞 P1 主链路 |
| `dagster_alphadesk/schedules.py` | `dagster_asterion/schedules.py` | direct_reuse | 同上 |

### 3.3 P1 验收标准

满足以下条件后，可认为 `P1` 阶段已基本摆脱 AlphaDesk 的 watch-only 代码依赖：

1. `ws_subscribe/ws_agg_v3/strategy_base/watch_only_gate_v3/health_monitor_v1/ui_db_replica` 已迁入 Asterion
2. 对应 module notes 已补齐
3. Asterion 能独立完成：
   - WS 订阅
   - WS 聚合
   - watch-only gate
   - 健康采集
   - UI replica 基础读路径

### 3.4 P1 关闭时的实际状态

截至 `2026-03-09`：

- `ws_subscribe.py` 已 `ported`
- `ws_agg_v3.py` 已 `ported`
- `strategy_base.py` 已 `ported`
- `watch_only_gate_v3.py` 已 `ported`
- `health_monitor_v1.py` 已 `ported`
- `ui_db_replica.py` 已 `ported`

结论（`P1` closeout 当时）：

- `P1` 所需的 AlphaDesk 直接复用模块已经全部迁入
- Weather `watch-only / replay / cold path` 的基础开发不再需要把 AlphaDesk 当作必须参考仓库
- 当时 AlphaDesk Exit Gate 仍未满足；该约束已在 `P2-21` closeout 后解除

---

## 4. P2 迁移清单

`P2` 目标是让 Asterion 的 execution foundation、journal、risk、readiness 不再需要依赖 AlphaDesk 源码做对照开发。

### 4.1 P2 必迁模块（历史清单，现均已完成）

| Source Module | Target Path | Classification | P2 Priority | Why |
| --- | --- | --- | --- | --- |
| `alphadesk/strategy_engine_v3.py` | `asterion_core/runtime/strategy_engine_v3.py` | keep_shell_rewrite_content | P2-blocker | 运行时调度壳、稳定排序、run_id 规则需要 Asterion 自有实现 |
| `alphadesk/trade_ticket_v1.py` | `asterion_core/execution/trade_ticket_v1.py` | keep_shell_rewrite_content | P2-blocker | 已完成；票据/provenance/hash 机制已从 AlphaDesk 脱钩 |
| `alphadesk/signal_to_order_v1.py` | `asterion_core/execution/signal_to_order_v1.py` | keep_shell_rewrite_content | P2-blocker | 已完成；ticket -> canonical order handoff 已收口到 Asterion 自有实现 |
| `alphadesk/execution_gate_v1.py` | `asterion_core/execution/execution_gate_v1.py` | keep_shell_rewrite_content | P2-blocker | 进入 paper/live 前必须有自有 gate 流水线 |
| `alphadesk/portfolio_v3.py` | `asterion_core/risk/portfolio_v3.py` | keep_shell_rewrite_content | P2-blocker | inventory/reservation/exposure gate 需落到 Asterion 语义 |
| `alphadesk/journal_v3.py` | `asterion_core/journal/journal_v3.py` | keep_shell_rewrite_content | P2-blocker | order/fill/reservation journal 不能继续靠 AlphaDesk 参考实现 |
| `alphadesk/readiness_checker_v1.py` | `asterion_core/monitoring/readiness_checker_v1.py` | keep_shell_rewrite_content | P2-high | paper/live 前的 readiness gate 要切到 Asterion phases |
| `alphadesk/ui_lite_db.py` | `asterion_core/ui/ui_lite_db.py` | keep_shell_rewrite_content | P2-medium | operator / report 读模型需要 Asterion 自有输出 contract |

### 4.2 P2 验收标准

满足以下条件后，可认为 Asterion 已不再需要以 AlphaDesk 作为“执行路径代码参考仓库”：

1. Runtime Skeleton 和 Ops/UI 剩余项已迁入或明确 `do_not_port`
2. Strategy runtime、ticket、gate、risk、journal、readiness 都已有 Asterion 自有实现
3. Asterion 实施文档不再要求“阅读 AlphaDesk 某模块源码后再开发”

---

## 5. ws_subscribe / ws_agg_v3 的统一口径

统一定义如下：

- `ws_subscribe.py` 和 `ws_agg_v3.py` 仍属于 **可复用代码**
- 但它们 **未在 P0 完成迁入**
- 它们现在应被视为 **P1-blocker**
- 直到迁入前，Asterion 仍需要把 AlphaDesk 作为 WS 层的只读参考仓库

因此：

- 不能再在任何文档中把它们写成“已经随 Wave A 迁完”
- 也不能在迁移台账里遗漏它们

---

## 6. 新建 Asterion 独立 Git 仓库的建议时点

建议时点已经到达：`P2` 收尾后即可执行。

推荐操作顺序：

1. 复查 [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md) 中所有可复用项的状态
2. 以 [P2_Closeout_Checklist.md](./P2_Closeout_Checklist.md) 为准确认 Exit Gate
3. 创建并切换到新的 Asterion 独立 Git 仓库
4. 后续只维护 Asterion 仓库

这样做的原因：

- 避免在仍需频繁对照 AlphaDesk 源码时，过早切仓导致迁移追踪混乱
- 避免文档口径和实际代码迁移进度不一致
- 避免后续还要反复把 AlphaDesk 里的剩余模块手工同步进新仓

---

## 7. 当前行动建议

当前行动建议：

1. 以 Asterion 为唯一开发仓库继续推进 `P3`
2. 保留本清单和迁移台账作为历史审计记录
3. 不再新增 AlphaDesk 迁移项
