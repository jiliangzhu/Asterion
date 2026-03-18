# Asterion P0 Implementation Plan

**版本**: v1.0
**更新日期**: 2026-03-09
**阶段**: `P0`
**目标**: 把已经冻结的核心语义和设计文档落成可开发的工程底座，为 `P1 watch-only`、`P2 replay/cold path` 提供稳定起点。

---

## 1. P0 定位

`P0` 不是业务功能阶段，而是“工程底座落地”阶段。
本阶段只做四类事情：

1. 建立 Asterion 的代码骨架和包边界
2. 迁入 AlphaDesk 可复用的基础设施
3. 落地 canonical contracts 与数据库 schema
4. 建立后续阶段可以复用的测试、迁移、文档治理基础

`P0` 完成后，项目应达到：

- 可以开始写 `watch-only` 代码，而不是继续讨论基础接口
- `RouteAction / ExecutionContext / Reservation / ResolutionSpec / UMAProposal` 等核心对象已冻结到代码与 schema
- AlphaDesk Wave A 基础模块已迁入或已明确 module note，不再停留在口头复用

---

## 2. P0 Source Of Truth

本阶段实施以以下文档为准：

- [Asterion_Project_Plan.md](../../../../00-overview/Asterion_Project_Plan.md)
- [DEVELOPMENT_ROADMAP.md](../../../../00-overview/DEVELOPMENT_ROADMAP.md)
- [Documentation_Index.md](../../../../00-overview/Documentation_Index.md)
- [AlphaDesk_Migration_Ledger.md](../migration-ledger/AlphaDesk_Migration_Ledger.md)
- [P0_Closeout_Checklist.md](../checklists/P0_Closeout_Checklist.md)
- [OMS_Design.md](../../../../30-trading/OMS_Design.md)
- [Market_Capability_Registry_Design.md](../../../../30-trading/Market_Capability_Registry_Design.md)
- [Signer_Service_Design.md](../../../../30-trading/Signer_Service_Design.md)
- [Forecast_Ensemble_Design.md](../../../../40-weather/Forecast_Ensemble_Design.md)
- [UMA_Watcher_Design.md](../../../../40-weather/UMA_Watcher_Design.md)

如果实施中发现冲突，优先处理顺序：

1. 对应 subsystem 设计文档
2. `Asterion_Project_Plan.md`
3. 本实施文档

本实施文档负责“顺序、交付物、目录、验收动作”，不负责重新定义业务契约。

---

## 3. P0 退出条件

`P0` 完成必须同时满足：

1. AlphaDesk Wave A 模块已形成迁移清单和 module notes
2. Asterion 包结构已经创建，后续阶段不再反复改根目录布局
3. 共享 contracts 已有统一代码落点
4. 首版 migrations 能创建 `weather / capability / oms / inventory / uma` 相关表
5. 单写者、write queue、writerd、determinism 等基础能力已能在 Asterion 中被导入和 smoke test
6. README 和 `docs/` 归档规则已经稳定，可支撑后续持续加文档

---

## 4. P0 不做什么

`P0` 不做以下事情：

- 不做 live signing
- 不做真实下单
- 不做完整 watch-only 机会生成
- 不做 Operator UI 页面
- 不做 dispute 自动化
- 不做策略 alpha 逻辑
- 不迁 AlphaDesk 的旧 `opportunities_v1/v2/v3`、旧 `exec_plan_v3`、旧 crypto capital engine

---

## 5. P0 目标代码布局

`P0` 结束时，代码树至少应落到下面这个层次：

```text
asterion_core/
  contracts/
    execution.py
    inventory.py
    weather.py
    ids.py
  clients/
    data_api.py
    gamma.py
  ingest/
    bronze.py
  storage/
    database.py
    db_migrate.py
    write_queue.py
    writerd.py
    write_guard_audit.py
    os_queue.py
    determinism.py
  runtime/
    strategy_base.py
  execution/
  risk/
  journal/
  monitoring/
  ui/

domains/
  markets/
  trading/
    oms/
  weather/
    scout/
    spec/
    forecast/
    pricing/
    resolution/

sql/
  migrations/
```

说明：

- `P0` 只要求创建稳定落点，不要求所有模块立即写满
- `runtime/execution/risk/journal/monitoring/ui` 在 `P0` 可以先只有包和基础占位，不强求功能完成
- `contracts/` 是 `P0` 的关键目录，后续不得在各模块内部重复定义同名核心对象

---

## 6. P0 五个工作流

### 6.1 工作流 A: 文档治理与 AlphaDesk 迁移清单

目标：

- 把“复用 AlphaDesk”变成显式清单，而不是口头约定

必须产出：

- AlphaDesk -> Asterion 迁移清单
- 每个 Wave A 模块一份 module note
- 文档目录和 source-of-truth 规则

执行动作：

1. 为 `bronze / data_api / gamma / database / db_migrate / write_queue / writerd / write_guard_audit / os_queue / determinism` 建立 module note
2. 每份 module note 至少写清楚：
   - 来源文件
   - 迁入目标路径
   - 直接保留的类和函数
   - 删除的 AlphaDesk 假设
   - 新接入的 Asterion contract
   - smoke test 点
3. 后续所有实施文档统一进入 `docs/10-implementation/`

### 6.2 工作流 B: AlphaDesk Wave A 基础模块迁移

目标：

- 先迁最稳定、最通用的基础设施

迁移顺序建议：

1. `determinism.py`
2. `write_guard_audit.py`
3. `write_queue.py`
4. `os_queue.py`
5. `database.py`
6. `db_migrate.py`
7. `writerd.py`
8. `bronze.py`
9. `clients/data_api.py`
10. `clients/gamma.py`

这样排的原因：

- 前 7 个先把“单写者、可审计、可迁移”基础打稳
- `bronze` 和 `clients` 再落，后续 P1 才能安全接入数据

迁移适配要求：

- `ALPHADESK_*` 环境变量统一改为 `ASTERION_*`
- `gold/silver/meta` 等旧 schema allow-list 重新对齐 Asterion
- 写队列 task payload 只允许引用 Asterion 的表名和字段
- 日志、运行模式、错误消息从 AlphaDesk 语义改为 Asterion 语义

### 6.3 工作流 C: Shared Contracts 落库

目标：

- 把冻结的设计契约变成唯一代码定义

建议按 4 个文件拆：

#### `asterion_core/contracts/execution.py`

包含：

- `RouteAction`
- `CanonicalOrderContract`
- `MarketCapability`
- `AccountTradingCapability`
- `ExecutionContext`

#### `asterion_core/contracts/inventory.py`

包含：

- `Order`
- `Fill`
- `Reservation`
- `InventoryPosition`
- `ExposureSnapshot`

#### `asterion_core/contracts/weather.py`

包含：

- `ResolutionSpec`
- `ForecastRequest`
- `ForecastResolutionContract`
- `UMAProposal`
- `ProposalStateTransition`

#### `asterion_core/contracts/ids.py`

包含：

- `request_id`
- `client_order_id`
- `reservation_id`
- `proposal_id`
- `event_id`
- cache key helper

要求：

- 所有字段命名与设计文档一致
- 不允许模块内重新定义同名枚举或 dataclass
- 序列化格式在 `P0` 就要稳定

### 6.4 工作流 D: 首版数据库 migration

目标：

- 让 schema 从文档进入实际数据库定义

建议 migration 顺序：

1. `0001_core_meta.sql`
   - run log
   - block watermark
   - generic request / audit metadata

2. `0002_market_and_capability.sql`
   - market capability
   - account trading capability

3. `0003_orders_inventory.sql`
   - orders
   - fills
   - reservations
   - inventory_positions
   - exposure_snapshots

4. `0004_weather_specs_and_forecasts.sql`
   - weather market specs
   - resolution specs
   - forecast runs

5. `0005_uma_watcher.sql`
   - uma proposals
   - proposal state transitions
   - processed uma events
   - proposal evidence links

P0 约束：

- migration 必须幂等
- 所有核心表都要有稳定主键和时间字段
- `old_status/new_status`、`route_action`、`fee_rate_bps`、`signature_type`、`funder` 等冻结字段必须体现在 schema 中

### 6.5 工作流 E: 测试与 smoke check

目标：

- 确保 P0 不是“文档已写、代码未稳”的假完成

最低测试集合：

1. contract serialization tests
2. enum freeze tests
3. migration apply tests
4. `write_queue` enqueue / claim / retry smoke tests
5. `writerd` 单 writer smoke tests
6. `determinism` stable hash golden tests
7. `database.py` reader guard / writer guard tests

---

## 7. 详细任务清单

### P0-01 文档与目录治理

输出：

- `docs/00-overview/Documentation_Index.md`
- `docs/10-implementation/versions/v1.0/phase-plans/P0_Implementation_Plan.md`

完成条件：

- 根目录只剩 `README.md`
- 文档分类规则固定

### P0-02 AlphaDesk 迁移台账

输出：

- Wave A 模块清单
- module note 模板和首批 notes

完成条件：

- 每个 Wave A 模块都有明确目标路径和适配说明

### P0-03 创建 Asterion 包骨架

输出：

- `asterion_core/*`
- `domains/*`
- `sql/migrations/*`

完成条件：

- 目录已存在
- import path 稳定

### P0-04 落地 shared contracts

输出：

- `contracts/execution.py`
- `contracts/inventory.py`
- `contracts/weather.py`
- `contracts/ids.py`

完成条件：

- 核心对象只有一个定义位置

### P0-05 落地 ID / key 规范

输出：

- request / order / reservation / proposal / event / cache key 规则

完成条件：

- contract tests 覆盖 key 生成与序列化

### P0-06 迁入 determinism

输出：

- `asterion_core/storage/determinism.py`

完成条件：

- hash 输出稳定
- golden tests 通过

### P0-07 迁入 write guard audit

输出：

- `asterion_core/storage/write_guard_audit.py`

完成条件：

- reader/write guard 被拦截时有审计落点

### P0-08 迁入 write queue + os queue

输出：

- `write_queue.py`
- `os_queue.py`

完成条件：

- enqueue / claim / retry / archive 闭环可跑

### P0-09 迁入 database 和 db_migrate

输出：

- `database.py`
- `db_migrate.py`

完成条件：

- 单写者模式能配置
- migration 入口可调用

### P0-10 迁入 writerd

输出：

- `writerd.py`

完成条件：

- allow-list 改成 Asterion
- batch / fallback 逻辑可 smoke

### P0-11 迁入 bronze

输出：

- `bronze.py`

完成条件：

- minute rolling + finalize 行为正确

### P0-12 迁入 Gamma / Data API clients

输出：

- `clients/gamma.py`
- `clients/data_api.py`

完成条件：

- market discovery 所需最小接口可被调用

### P0-13 首版 migrations

输出：

- `0001` 到 `0005` migration

完成条件：

- 空库 apply 成功
- 关键表全部存在

### P0-14 基础测试框架

输出：

- contracts / storage / migration smoke tests
- `tests/README.md`
- `pyproject.toml`

完成条件：

- P0 核心模块可回归
- `duckdb` 被写入正式项目依赖
- 测试入口固定为 `python3 -m unittest discover -s tests -v`

### P0-15 P0 关闭审查

输出：

- [P0_Closeout_Checklist.md](../checklists/P0_Closeout_Checklist.md)
- P1 开工前阻塞项清单

完成条件：

- `P0` 退出条件全部满足

---

## 8. 推荐实施顺序

### 第 1 批: 治理先行

1. `P0-01`
2. `P0-02`
3. `P0-03`
4. `P0-04`
5. `P0-05`

### 第 2 批: 存储与写路径底座

1. `P0-06`
2. `P0-07`
3. `P0-08`
4. `P0-09`
5. `P0-10`

### 第 3 批: 数据接入底座

1. `P0-11`
2. `P0-12`

### 第 4 批: schema 与测试收口

1. `P0-13`
2. `P0-14`
3. `P0-15`

---

## 9. 实施中的文档产物要求

P0 期间新增的实施文档，统一放在 `docs/10-implementation/`，建议至少包括：

- `P0_Implementation_Plan.md`
- `module-notes/AlphaDesk_bronze_Module_Note.md`
- `module-notes/AlphaDesk_database_Module_Note.md`
- `module-notes/AlphaDesk_write_queue_Module_Note.md`
- `module-notes/AlphaDesk_writerd_Module_Note.md`

要求：

- 每份 module note 不超过 1 页
- 只写“保留什么 / 改什么 / 测什么”
- 不重复粘贴源代码

---

## 10. P0 完成后的下一步

`P0` 完成后，立刻进入：

1. Weather market discovery
2. Rule2Spec -> ResolutionSpec
3. Station mapper onboarding path
4. forecast cache + forecast run persistence
5. UMA watcher finalized watermark + replay

也就是开始 `P1 watch-only`，而不是继续扩写基础文档。
