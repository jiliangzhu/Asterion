# P4 Real Weather Chain Smoke Runbook

**状态**: archived accepted historical runbook
**阶段**: `P4`  
**边界**: 这是 `市场发现 -> 规则解析 -> 预测 -> 定价 -> 机会发现` 的真实数据 smoke，不是 paper execution，也不是 controlled live。

---

> Historical accepted record only.
> 该 runbook 保留为 `P4` real weather chain smoke 的历史 operator 记录，不再作为当前 active runbook 入口。

## 1. 目标

本 runbook 定义一条可复跑的 Weather 主链 smoke：

```text
Gamma market discovery
-> weather tag filter
-> template recognition
-> WeatherMarket
-> WeatherMarketSpec
-> forecast refresh
-> fair values
-> watch-only snapshots
```

本 smoke 的目标是验证：

- 真实 Weather 市场发现语义仍可被 canonical discovery path 接住
- 规则解析能落成 `weather.weather_market_specs`
- 真实 forecast adapters 能生成 `weather.weather_forecast_runs`
- 定价与机会发现能落成 `weather.weather_fair_values` / `weather.weather_watch_only_snapshots`

当前固定语义：

- `weather.weather_fair_values` 保存 `model fair value`
- `weather.weather_watch_only_snapshots.fair_value` 保存 `execution-adjusted fair value`
- `weather.weather_watch_only_snapshots.edge_bps` 保存 `executable edge`
- `fees / slippage / liquidity penalty / model edge / ranking` 进入 `pricing_context_json`
- Phase 3 之后，`pricing_context_json` 还会携带：
  - `mapping_confidence`
  - `source_freshness_status`
  - `price_staleness_ms`
  - `market_quality_status`
  - `market_quality_reason_codes`

明确不覆盖：

- `weather_paper_execution`
- signer / submitter / chain tx
- controlled live

---

## 2. 运行边界

默认实现采用：

- 真实 `Gamma` 开盘天气市场
- 真实 `NWS` / `OpenMeteo` forecast adapter

固定筛选规则：

- `active = true`
- `closed = false`
- `archived = false`
- 优先 `acceptingOrders = true`
- `close_time / end_date` 必须在未来，并按 `14 -> 30 -> 60 -> 90` 天窗口自适应扩大

说明：

- 默认模式不再使用历史已关闭市场
- primary discovery path 为 `Gamma events weather feed`
- 若 API 没命中可运行市场，会自动回退到 `polymarket.com/markets/weather` 只读 discovery
- 当前机器对 `gamma-api.polymarket.com` 的 Python TLS 访问不稳定时，脚本会通过 `curl` 获取 JSON，避免 `httpx` 的 TLS EOF 问题
- 只有在你显式传入 `--use-frozen-market` 时，才会回退到冻结的历史真实 Gamma 样本

---

## 3. 前置条件

建议在仓库根目录执行：

```bash
cd /Users/jayzhu/web3/Asterion
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
```

默认 smoke 会启用 weather agents，并把产物写入 `agent.*`：

- `rule2spec_agent`：必跑
- `data_qa_agent`：仅当 smoke 链路内存在可构造的 canonical 输入时运行
- `resolution_agent`：若当前链路没有 resolution proposal 输入，则明确记为 `not_run`

若需要 agent 生效：

- 在根目录 `.env` 中配置兼容接口 key，例如：
  - `ASTERION_OPENAI_COMPATIBLE_API_KEY`
  - `ASTERION_OPENAI_COMPATIBLE_MODEL`
- 若使用自定义兼容网关，还需配置：
  - `ASTERION_OPENAI_COMPATIBLE_BASE_URL`

说明：

- 当前 weather smoke 的 agent runtime 走 `openai_compatible` provider
- 默认会把 `rule2spec` 的 work rows 写入 `agent.invocations / outputs / reviews / evaluations`
- 单个 market 的 agent failure 不应拖垮整条 weather smoke；报告和 UI 会明确显示对应 market 的 agent status

若未配置或上游模型服务不稳定，可使用 `--skip-agent` 进入 debug/fallback 模式；主链仍可跑通，但 report 和 UI 会明确显示 agent 被跳过。

---

## 4. Canonical 执行命令

### 默认执行（agent-on）

```bash
cd /Users/jayzhu/web3/Asterion
source .venv/bin/activate
.venv/bin/python scripts/run_real_weather_chain_smoke.py --force-rebuild
```

说明：

- 默认会批量处理所有当前开盘、近期且能完成站点映射的天气市场
- 默认会跑 `rule2spec_agent`
- 结果会在 `real_weather_chain_report.json.market_discovery.selected_markets[*]` 中附带每个 market 的 agent status / verdict / summary

### 显式回退到历史冻结样本

```bash
cd /Users/jayzhu/web3/Asterion
source .venv/bin/activate
.venv/bin/python scripts/run_real_weather_chain_smoke.py --force-rebuild --use-frozen-market
```

### 跳过 agent 校验（仅 debug/fallback）

```bash
cd /Users/jayzhu/web3/Asterion
source .venv/bin/activate
.venv/bin/python scripts/run_real_weather_chain_smoke.py --force-rebuild --skip-agent
```

### 自定义输出目录

```bash
cd /Users/jayzhu/web3/Asterion
source .venv/bin/activate
.venv/bin/python scripts/run_real_weather_chain_smoke.py \
  --force-rebuild \
  --skip-agent \
  --output-dir data/dev/real_weather_chain_alt
```

---

## 5. 输出产物

默认输出目录：

- `data/dev/real_weather_chain/`

关键产物：

- `real_weather_chain.duckdb`
- `real_weather_chain_report.json`
- `weather.source_health_snapshots`

当前 report 固定包含：

- `market_discovery`
- `rule_parse`
- `forecast_service`
- `pricing_engine`
- `opportunity_discovery`
- `db_counts`
- `agent_summary`
- `artifacts`

当前运行状态补充：

- `chain_status` 允许出现 `ok / degraded / initializing / transport_error / no_open_recent_markets`
- `degraded` 表示部分市场的 forecast 或 agent 失败，但整体市场发现与机会链路仍可读
- `initializing` 表示 loop 正在刷新新一轮报告；UI 会优先回退到 smoke DuckDB 中的最新市场视图，而不是直接显示空白

`market_discovery` 中新增：

- `market_source`
- `selected_horizon_days`
- `close_time`
- `selected_market_count`
- `selected_markets[*].rule2spec_*`
- `selected_markets[*].data_qa_*`
- `selected_markets[*].resolution_*`

UI 侧的当前约定：

- 市场页会展示所有 selected open markets 的详细分析，而不是只展示首条
- agent 页会展示每个 agent 实际做了什么、产出了什么、是否需要人工复核
- 市场页会显示 Phase 3 的质量字段：
  - `mapping_confidence`
  - `source_freshness_status`
  - `price_staleness_ms`
  - `market_quality_status`

说明：

- `weather.forecast_calibration_samples` 不在本 smoke 中直接产生
- calibration sample 的 canonical 生产路径在 `weather_resolution_reconciliation`，当 verification evidence 含 `observed_value` 时，会基于该市场最新 forecast run 生成 residual 样本

---

## 6. 成功判定

成功时应满足：

- `chain_status = "ok"`
- `weather.weather_markets >= 1`
- `weather.weather_market_specs >= 1`
- `weather.weather_forecast_runs >= 1`
- `weather.weather_fair_values >= 2`
- `weather.weather_watch_only_snapshots >= 2`
- `selected_market_count >= 1`

若当前命中多市场：

- `selected_market_count > 1`
- `selected_markets` 会覆盖多个城市
- 市场页和首页都应展示多城市覆盖情况

单个 market 的 canonical 信号期望仍然是：

- `YES` outcome:
  - fair value 约为 `1.0`
  - market price 接近 `0`
  - `edge_bps = 10000`
  - `decision = TAKE`
  - `side = BUY`
- `NO` outcome:
  - fair value 约为 `0.0`
  - market price 接近 `1`
  - `edge_bps = -10000`
  - `decision = TAKE`
  - `side = SELL`

---

## 7. 排查路径

### 7.1 市场发现失败

先查：

- `weather.weather_markets`
- `real_weather_chain_report.json.market_discovery`

说明：

- 默认 smoke 只抓取开盘且最近的真实市场
- 优先 `Gamma events weather feed`
- 若 API 漏掉官网市场，会自动回退到官网天气页只读 discovery
- 若这里失败，先确认当前 Gamma 是否确实存在满足条件的天气市场
- 若只是想验证 deterministic 全链路，可显式使用 `--use-frozen-market`
- 若当前没有满足条件的开盘天气市场，脚本会明确报错，不会再静默回退到历史关盘样本

### 7.2 规则解析失败

先查：

- `weather.weather_market_specs`
- `real_weather_chain_report.json.rule_parse`

重点确认：

- threshold 模板仍能识别 `60°F or higher`
- station override 是否写入 `KNYC`

### 7.3 预测服务失败

先查：

- `weather.weather_forecast_runs`
- `source_requested`
- `source_used`
- `source_trace`

说明：

- 当前 smoke 以 `weather.com` 作为请求语义
- 实际运行路径可 fallback 到 `NWS` 或 `OpenMeteo`

### 7.4 定价或机会发现失败

先查：

- `weather.weather_fair_values`
- `weather.weather_watch_only_snapshots`
- report 中的 `market_prices` / `fair_values` / `signals`

---

## 8. 验证命令

脚本与 UI 相关回归建议至少运行：

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_real_weather_chain_smoke.py' -v
.venv/bin/python -m unittest discover -s tests -p 'test_ui_data_access.py' -v
.venv/bin/python -m unittest discover -s tests -p 'test_weather_agents.py' -v
git diff --check
```

若改动了 smoke script 或文档导航，还应实际重跑：

```bash
.venv/bin/python scripts/run_real_weather_chain_smoke.py --force-rebuild
```

---

## 9. Human-In-The-Loop 边界

当前 smoke 可以用于：

- 验证 Weather 研究主链
- 验证 discovery/spec/forecast/pricing 的 canonical 闭环
- 生成可人工检查的 `watch_only` 信号

当前 smoke 不代表：

- 自动进入 `weather_paper_execution`
- 自动触发真实 submit
- 自动触发 controlled live

它的定位是：

- `P4` 下真实数据 ingress 与 Weather 研究主链的最小可复跑验证入口
