# Operator Console Truth Source Design

**版本**: v1.2
**更新日期**: 2026-03-17
**状态**: historical accepted supporting design (`Post-P4 Phase 10` / `Post-P4 Phase 11` / `Post-P4 Phase 14` / `Post-P4 Phase 15`)
**对应阶段**: `Post-P4 Phase 10`, `Post-P4 Phase 11`, `Post-P4 Phase 14`, `Post-P4 Phase 15`

---

## 1. 背景与问题

`Phase 9` 已经完成了一轮 wording cleanup，但深度审计指出 operator surface 仍有 4 类隐患：

1. sidebar truth-source 仍偏硬编码
2. fallback / degraded 数据在行级别不够显式
3. score 主次关系还不够硬，容易把 diagnostics 当主信号
4. analysis docs 和 implementation docs 的角色边界，对新进入仓库的人仍可能不够清晰

本设计的目标，是把 operator console 的 truth-source 从“文案一致”继续推进到“数据来源、边界、主分数、降级态都可机器化校验”。

---

## 2. 当前代码事实

当前相关落点：

- `ui/app.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/pages/agents.py`
- `ui/pages/system.py`
- `ui/data_access.py`
- `asterion_core/ui/ui_lite_db.py`

当前事实判断：

- 页面边界文案已经基本一致
- `ranking_score` 已经是 runtime 主排序信号
- 但 sidebar boundary summary 仍主要来自 app-level static copy
- degraded / fallback / stale 的显式 badge 还不统一
- analysis docs 仍缺统一 banner baseline

---

## 3. 锁定决策

### 3.1 Surface Truth Descriptor

固定新增：

- `SurfaceTruthDescriptor`

固定用途：

- 给每个 UI surface 声明：
  - 主要数据源
  - fallback 数据源
  - primary score
  - boundary wording baseline

### 3.2 Boundary Sidebar Summary

sidebar 不再长期依赖硬编码 copy，固定改成从 readiness / capability 读模型生成：

- `BoundarySidebarSummary`

固定字段：

- `system_positioning`
- `current_phase_status`
- `capability_boundary`
- `live_negations`
- `truth_source_doc`

### 3.3 Opportunity Row Source Badge

Markets / Execution / Home 所有核心表行固定支持：

- `source_badge`
- `source_truth_status`
- `is_degraded_source`

固定 badge family：

- `canonical`
- `fallback`
- `stale`
- `degraded`
- `derived`

### 3.4 Primary Score Descriptor

固定全 UI 只承认一个 primary score：

- `ranking_score`

其它分数固定下沉为 diagnostics：

- `expected_value_score`
- `expected_pnl_score`
- `ops_readiness_score`
- `opportunity_score` 只作兼容 alias，最终应退场

### 3.5 Analysis Doc Banner Baseline

所有 `docs/analysis/*.md` 固定加统一 banner：

- 这是 analysis input
- 不是 implementation truth-source
- 当前 active implementation entry 是 `V2_Implementation_Plan.md`

---

## 4. 接口 / Contract / Schema

### 4.1 SurfaceTruthDescriptor

建议新增：

```python
@dataclass(frozen=True)
class SurfaceTruthDescriptor:
    surface_id: str
    primary_table: str
    fallback_sources: list[str]
    primary_score: str
    boundary_copy_key: str
    supports_source_badges: bool
```

### 4.2 BoundarySidebarSummary

建议新增：

```python
@dataclass(frozen=True)
class BoundarySidebarSummary:
    system_positioning: str
    current_phase_status: str
    capability_boundary: str
    live_negations: list[str]
    truth_source_doc: str
```

### 4.3 OpportunityRowSourceBadge

建议新增：

```python
@dataclass(frozen=True)
class OpportunityRowSourceBadge:
    source_badge: str
    source_truth_status: str
    is_degraded_source: bool
    reason_codes: list[str]
```

### 4.4 UI Read Model Extensions

`Post-P4 Phase 11` 和 `Post-P4 Phase 15` 固定允许扩展 `ui.*` projections：

- `source_badge`
- `source_truth_status`
- `primary_score_label`
- `is_degraded_source`
- `surface_truth_descriptor_json`

不允许：

- 新造第二套排序字段
- UI 自己改写 capability boundary 语义

---

## 5. 数据流

```text
readiness evidence + capability boundary
-> BoundarySidebarSummary
-> app sidebar + home/system surface

ui.* read models
-> source truth evaluator
-> row-level source badges
-> markets/execution/home tables

analysis docs policy
-> banner injector / template baseline
-> docs/analysis/*.md
```

---

## 6. 失败模式与边界

固定失败模式：

- readiness evidence missing
- capability boundary stale
- row fallback data missing
- source badge inconsistent with underlying loader
- doc banner missing

固定处理：

- UI 显式显示 degraded truth-source
- 不 silent fallback 到 hardcoded optimistic wording
- 缺 truth-source descriptor 时页面仍可渲染，但必须显示 degraded marker

固定边界：

- 本设计不改变 execution / ranking / submitter 行为
- 只改变 operator surface 和 truth-source generation path

---

## 7. 测试策略

至少补：

- `tests.test_operator_truth_source`
- `tests.test_ui_source_badges`
- `tests.test_sidebar_boundary_summary`
- `tests.test_analysis_doc_banner_policy`
- `tests.test_ui_primary_score_baseline`

最小 acceptance：

- `.venv/bin/python3 -m unittest tests.test_operator_truth_source -v`
- `.venv/bin/python3 -m unittest tests.test_ui_source_badges -v`
- `.venv/bin/python3 -m unittest tests.test_analysis_doc_banner_policy -v`

---

## 8. 文档同步要求

实现本设计时必须同步：

- `README.md`
- `Documentation_Index.md`
- `Implementation_Index.md`
- `Asterion_Project_Plan.md`
- `DEVELOPMENT_ROADMAP.md`
- `Post_P4_Remediation_Implementation_Plan.md`

---

## 9. Deferred / Non-Goals

本设计明确不做：

- 大 UI IA 重构
- 多租户 operator console
- marketing site copy rewrite
- independent BI dashboard product
