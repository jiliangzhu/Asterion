# Execution Economics Design

**版本**: v1.0
**更新日期**: 2026-03-17
**状态**: historical accepted supporting design (`Post-P4 Phase 12` / `Post-P4 Phase 14`)
**对应阶段**: `Post-P4 Phase 12: Execution Economics and Ranking v2`, `Post-P4 Phase 14: Execution Feedback Loop and Cohort Priors`

---

## 1. 背景与问题

当前系统已经有：

- `edge_bps_model`
- `edge_bps_executable`
- penalty-aware `ranking_score`
- execution science / capture / miss / distortion read models

但 execution economics 仍偏 heuristic：

- fill probability 仍是轻量 proxy
- slippage / depth / capture 还没有 cohort priors
- `ranking_score` 还不是明确的 dollar-EV / risk-aware score
- execution science 还停留在“解释层”，没有反哺 ranking

`Post-P4 Phase 12` 和 `Post-P4 Phase 14` 的目标，是把 execution economics 从 deterministic heuristic 升级成可被研究、可被 nightly 反馈、可被 UI 和 runtime 共同消费的单一排序 contract。

---

## 2. 当前代码事实

当前相关代码落点：

- `domains/weather/opportunity/service.py`
  - 负责 `OpportunityAssessment`
- `asterion_core/contracts/opportunity.py`
  - 已有 `ranking_score` 和 penalty fields
- `asterion_core/runtime/strategy_engine_v3.py`
  - runtime 已按 `ranking_score` 排序
- `asterion_core/ui/ui_lite_db.py`
  - 已有 execution science / watch-only vs executed / market research

当前事实判断：

- 排序 contract 已经从 raw edge 提升到 penalty-aware
- `Post-P4 Phase 12` 已把 `ranking_score` 升级成 unit-opportunity EV / capture / risk / capital-efficiency 语义
- `weather.weather_execution_priors` 已作为独立 serving table 落地，当前采用 manual refresh cadence
- `why_ranked_json` 已进入 assessment / pricing context，并由 Home / Markets 直接消费

---

## 3. 锁定决策

### 3.1 引入 Execution Priors v1

固定新增 serving table：

- `weather.weather_execution_priors`

固定用途：

- 为 `OpportunityAssessment` 提供 capture-aware / slippage-aware / fill-aware priors
- 不替代 canonical execution ledger
- 不直接承载 order-level state

### 3.2 Ranking Score v2

固定把 `ranking_score` 升级成 `ranking_score_v2` 语义，但字段名继续保持 `ranking_score`，避免平行 contract。

固定含义：

- `ranking_score` = dollar-EV aware + capture aware + risk aware 的主排序信号
- UI / runtime / paper candidate 继续只消费一个主排序字段

### 3.3 Ops Readiness Score Downshift

固定把 `ops_readiness_score` 从主加分项降级为：

- gate input
- 或 deterministic tie-breaker

固定不允许：

- `ops_readiness_score` 单独把一个低 EV 机会推到高 EV 机会前面

### 3.4 Why-Ranked Decomposition

固定输出：

- `why_ranked_json`
- `expected_dollar_pnl`
- `capture_probability`
- `risk_penalty`
- `capital_efficiency`
- `execution_prior_key`

UI 只解释同一套 runtime score，不重算第二套排序。

---

## 4. 接口 / Contract / Schema

### 4.1 ExecutionPriorKey

建议新增：

```python
@dataclass(frozen=True)
class ExecutionPriorKey:
    market_id: str | None
    strategy_id: str | None
    wallet_id: str | None
    side: str | None
    horizon_bucket: str | None
    liquidity_bucket: str | None
```

### 4.2 ExecutionPriorSummary

建议新增：

```python
@dataclass(frozen=True)
class ExecutionPriorSummary:
    prior_key: ExecutionPriorKey
    sample_count: int
    submit_ack_rate: float
    fill_rate: float
    resolution_rate: float
    partial_fill_rate: float
    cancel_rate: float
    adverse_fill_slippage_bps_p50: float | None
    adverse_fill_slippage_bps_p90: float | None
    avg_realized_pnl: float | None
    avg_post_trade_error: float | None
    prior_quality_status: str
```

### 4.3 RankingScoreV2Decomposition

建议新增：

```python
@dataclass(frozen=True)
class RankingScoreV2Decomposition:
    expected_dollar_pnl: float
    capture_probability: float
    risk_penalty: float
    capital_efficiency: float
    ops_tie_breaker: float
    ranking_score: float
    why_ranked_json: dict[str, Any]
```

### 4.4 Storage Contract

固定新增：

- `weather.weather_execution_priors`

建议列：

- `prior_key TEXT PRIMARY KEY`
- `market_id TEXT`
- `strategy_id TEXT`
- `wallet_id TEXT`
- `side TEXT`
- `horizon_bucket TEXT`
- `liquidity_bucket TEXT`
- `sample_count BIGINT NOT NULL`
- `submit_ack_rate DOUBLE NOT NULL`
- `fill_rate DOUBLE NOT NULL`
- `resolution_rate DOUBLE NOT NULL`
- `partial_fill_rate DOUBLE NOT NULL`
- `cancel_rate DOUBLE NOT NULL`
- `adverse_fill_slippage_bps_p50 DOUBLE`
- `adverse_fill_slippage_bps_p90 DOUBLE`
- `avg_realized_pnl DOUBLE`
- `avg_post_trade_error DOUBLE`
- `prior_quality_status TEXT NOT NULL`
- `source_window_start TIMESTAMP NOT NULL`
- `source_window_end TIMESTAMP NOT NULL`
- `materialized_at TIMESTAMP NOT NULL`

固定扩展但不新增列：

- `assessment_context_json`
- `pricing_context_json`
- `trade_tickets.provenance_json`

其中固定新增 keys：

- `ranking_score_v2`
- `expected_dollar_pnl`
- `capture_probability`
- `risk_penalty`
- `capital_efficiency`
- `why_ranked_json`
- `execution_prior_key`

---

## 5. 数据流

Post-P4 Phase 12 ranking path:

```text
forecast + pricing + quality context
-> opportunity assessment
-> load execution priors
-> ranking decomposition
-> watch snapshot pricing_context_json
-> strategy_engine_v3 sort
-> UI / paper candidate consume same ranking_score
```

Post-P4 Phase 14 feedback path:

```text
runtime.trade_tickets
+ trading.fills
+ resolution.settlement_verifications
+ ui.execution_science_summary
-> nightly cohort prior materialization
-> weather.weather_execution_priors
-> next-day opportunity ranking
```

---

## 6. 失败模式与边界

固定失败模式：

- cohort sample too small
- prior quality degraded
- slippage prior stale
- capture prior missing

固定处理：

- fallback 到 deterministic heuristic
- 同时显式写入 `why_ranked_json`
- 降权，不 silent fail

固定边界：

- 本设计不直接改写 canonical order / fill / exposure
- priors 只作为 ranking input，不作为 execution gate 单独真相源
- 不让 UI 发明独立排序逻辑

---

## 7. 测试策略

`Post-P4 Phase 12` 至少补：

- `tests.test_execution_priors_materialization`
- `tests.test_ranking_score_v2`
- `tests.test_opportunity_service_ranking_v2`
- `tests.test_ui_why_ranked`

`Post-P4 Phase 14` 至少补：

- `tests.test_execution_feedback_loop`
- `tests.test_cohort_prior_backfill`
- `tests.test_execution_science_to_priors`

最小 acceptance：

- `.venv/bin/python3 -m unittest tests.test_ranking_score_v2 -v`
- `.venv/bin/python3 -m unittest tests.test_execution_priors_materialization -v`
- `.venv/bin/python3 -m unittest tests.test_execution_feedback_loop -v`

---

## 8. 文档同步要求

实现本设计时必须同步：

- `Post_P4_Remediation_Implementation_Plan.md`
- `README.md`
- `Implementation_Index.md`
- `Documentation_Index.md`
- `Operator_Console_Truth_Source_Design.md`

如果 `why-ranked` UI surface 落地，还需要同步 `Markets` / `Home` 的 operator copy。

---

## 9. Deferred / Non-Goals

本设计明确不做：

- black-box ML ranking
- online learning
- autonomous capital optimizer
- multi-asset portfolio optimization
- 第二套 paper-only ranking contract
