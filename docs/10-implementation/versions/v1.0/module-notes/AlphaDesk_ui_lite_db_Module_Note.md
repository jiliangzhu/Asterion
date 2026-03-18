# AlphaDesk ui_lite_db Module Note

**Source**: `AlphaDesk/alphadesk/ui_lite_db.py`
**Target**: `asterion_core/ui/ui_lite_db.py`
**Classification**: `keep_shell_rewrite_content`
**Status**: `ported`

## 保留什么

- Lite DB build 的外层壳
- `tmp -> validate -> replace` 的原子替换路径
- meta 文件写入和周期 loop 壳
- source snapshot 后再构建 Lite DB 的安全模式

## 改什么

- Lite DB 固定为独立文件：`data/ui/asterion_ui_lite.duckdb`
- 只读输入固定为 `UI replica`
- 输出 contract 改为 `ui.*` 5 张 summary tables：
  - `ui.market_watch_summary`
  - `ui.proposal_resolution_summary`
  - `ui.execution_ticket_summary`
  - `ui.agent_review_summary`
  - `ui.phase_readiness_summary`

## 不保留什么

- AlphaDesk 的 `gold.*` / `silver.*` UI contract
- `gold.serving_opportunities`、`gold.exec_plans_v1`、`gold.journal_trades_v1` 等旧表验证
- 任何平行 canonical ledger

## 接入的 Asterion Contracts

- `weather.*` watch-only / replay provenance
- `resolution.*` watcher / settlement / redeem suggestion 读面
- `runtime.* + trading.*` execution foundation 读面
- `agent.*` review surface
- `readiness report -> ui.phase_readiness_summary` 物化链路

## Smoke Test

- 从 `UI replica` 构建独立 Lite DB
- 5 张 `ui.*` summary tables 全部存在且可读
- 不依赖 AlphaDesk `gold.*`
- build 失败不覆盖旧 Lite DB
