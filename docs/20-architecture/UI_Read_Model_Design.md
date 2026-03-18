# UI Read Model Design

**版本**: v1.1  
**更新日期**: 2026-03-17  
**状态**: historical accepted supporting design (`Post-P4 Phase 15`)  
**对应阶段**: `Post-P4 Phase 15: UI Read-Model and Truth-Source Refactor`

---

## 1. 背景与问题

当前 UI read path 已经能支撑 operator console，但深度审计指出两个长期维护风险：

1. `asterion_core/ui/ui_lite_db.py` 已经变成 projection super-file
2. `ui/data_access.py` 已经变成 page loader super-file

这两处带来的问题：

- read model schema 变更难追踪
- page loader truth-source 分散
- JSON-heavy contract 容易漂移
- 新人难以判断“哪个 surface 读哪个 projection”

`Post-P4 Phase 15` 的目标，是在不新建第二套 UI truth-source 的前提下，把现有 read model 架构拆回可维护形态。

---

## 2. 当前代码事实

当前相关落点：

- `asterion_core/ui/ui_lite_db.py`
- `ui/data_access.py`
- `ui/pages/*`

当前事实判断：

- UI 仍然建立在 single lite DB projection 上，这条主线是正确的
- 问题不在“有没有统一 read model”，而在“统一得过于集中，难演化”

---

## 3. 锁定决策

### 3.1 拆分 ui_lite_db.py

固定拆分为按主题组织的 projection builders，例如：

- `asterion_core/ui/builders/readiness_builder.py`
- `asterion_core/ui/builders/opportunity_builder.py`
- `asterion_core/ui/builders/execution_science_builder.py`
- `asterion_core/ui/builders/agent_review_builder.py`
- `asterion_core/ui/builders/catalog_builder.py`

固定保留一个 orchestrator：

- `asterion_core/ui/ui_lite_db.py`

它只负责：

- attach snapshot
- call builders
- validate required tables
- 写 meta

### 3.2 拆分 ui/data_access.py

固定按 surface 拆成 loader modules，例如：

- `ui/loaders/home_loader.py`
- `ui/loaders/markets_loader.py`
- `ui/loaders/execution_loader.py`
- `ui/loaders/system_loader.py`
- `ui/loaders/agents_loader.py`
- `ui/loaders/shared_truth_source.py`

固定保留一个兼容 facade：

- `ui/data_access.py`

它只负责 backward-compatible exports。

### 3.3 Versioned Read-Model Schemas

固定新增两张 UI lite internal tables：

- `ui.read_model_catalog`
- `ui.truth_source_checks`

### 3.4 Golden Tests and Truth-Source Checker

固定引入：

- golden snapshot tests
- truth-source checker

目标不是 snapshot everything，而是固定核心 schema、主列、primary score label、source badge 语义。

---

## 4. 接口 / Contract / Schema

### 4.1 ReadModelCatalogRecord

建议新增：

```python
@dataclass(frozen=True)
class ReadModelCatalogRecord:
    table_name: str
    schema_version: str
    builder_name: str
    primary_key_columns: list[str]
    primary_score_column: str | None
    truth_source_description: str
    updated_at: datetime
```

### 4.2 TruthSourceCheckRecord

建议新增：

```python
@dataclass(frozen=True)
class TruthSourceCheckRecord:
    check_id: str
    surface_id: str
    table_name: str
    check_status: str
    issues_json: list[str]
    checked_at: datetime
```

### 4.3 SurfaceLoaderContract

建议新增统一 loader output contract：

```python
@dataclass(frozen=True)
class SurfaceLoaderContract:
    surface_id: str
    primary_dataframe_name: str
    supporting_payload: dict[str, Any]
    truth_source_summary: dict[str, Any]
```

### 4.4 Internal UI Tables

固定新增：

- `ui.read_model_catalog`
  - `table_name`
  - `schema_version`
  - `builder_name`
  - `primary_key_columns_json`
  - `primary_score_column`
  - `truth_source_description`
  - `updated_at`
- `ui.truth_source_checks`
  - `check_id`
  - `surface_id`
  - `table_name`
  - `check_status`
  - `issues_json`
  - `checked_at`

固定边界：

- 这两张表只服务 UI 自检
- 不替代 canonical runtime / trading facts

---

## 5. 数据流

```text
src replica snapshot
-> ui_lite orchestrator
-> thematic builders
-> ui.* tables
-> catalog builder
-> ui.read_model_catalog
-> truth-source checker
-> ui.truth_source_checks
-> surface loaders
-> Streamlit pages
```

---

## 6. 失败模式与边界

固定失败模式：

- builder output missing required column
- page loader reads stale contract
- schema version mismatch
- fallback loader silently reimplements table logic

固定处理：

- truth-source checker fail
- golden tests fail
- page renders degraded state rather than silently drifting

固定边界：

- 本设计不新增新的 UI truth-source
- 不允许 page 层重新计算 canonical ranking / boundary logic

---

## 7. 测试策略

至少补：

- `tests.test_ui_read_model_catalog`
- `tests.test_truth_source_checks`
- `tests.test_ui_loader_contracts`
- `tests.test_ui_golden_surfaces`
- `tests.test_ui_lite_builder_registry`

最小 acceptance：

- `.venv/bin/python3 -m unittest tests.test_ui_read_model_catalog -v`
- `.venv/bin/python3 -m unittest tests.test_ui_loader_contracts -v`
- `.venv/bin/python3 -m unittest tests.test_ui_golden_surfaces -v`

---

## 8. 文档同步要求

实现本设计时必须同步：

- `Post_P4_Remediation_Implementation_Plan.md`
- `Implementation_Index.md`
- `Documentation_Index.md`
- `Operator_Console_Truth_Source_Design.md`

如果 surface loader import path 变化，还要同步：

- `README.md`
- relevant page/module notes

---

## 9. Deferred / Non-Goals

本设计明确不做：

- front-end framework migration
- replacing DuckDB UI lite path
- multi-tenant API backend for UI
- new page IA
