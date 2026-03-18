# AlphaDesk readiness_checker_v1 Module Note

**Source**: `AlphaDesk/alphadesk/readiness_checker_v1.py`
**Target**: `asterion_core/monitoring/readiness_checker_v1.py`
**Classification**: `keep_shell_rewrite_content`
**Status**: `ported`

## 保留什么

- checker/report 的外层组织方式
- `to_dict() / from_dict() / to_markdown()` 报告壳
- data-hash 与落盘输出模式

## 改什么

- 彻底移除 AlphaDesk 的 `M1-M5` 里程碑语义
- readiness target 固定为 `p3_paper_execution`
- gate 重写为：
  - `cold_path_determinism`
  - `execution_foundation`
  - `agent_review_surface`
  - `operator_surface`
- operator surface 改读 `UI replica meta + UI lite DB`

## 不保留什么

- profitability / latency / burn-in 等旧 live milestone 指标
- AlphaDesk `gold.*` 表依赖
- 在 readiness checker 内执行 replay / backfill / 下单 / agent 的行为

## 接入的 Asterion Contracts

- `weather.* / resolution.* / runtime.* / trading.* / agent.*` 的 P3 门禁检查
- `dagster_asterion/job_map.py` 的 manual review job 完整性检查
- `UI replica -> UI lite -> readiness report` operator surface 闭环

## Smoke Test

- 缺失 required table 时返回 `NO-GO`
- 最新 continuity 为 `RPC_INCOMPLETE` 时返回 `NO-GO`
- UI replica 成功但 UI lite 缺失时 `operator_surface` fail
- 全量条件满足时返回 `GO`
