# P4 Real Weather Chain Smoke Runbook

**状态**: active  
**阶段**: `P4`  
**边界**: 这是 `市场发现 -> 规则解析 -> 预测 -> 定价 -> 机会发现` 的真实数据 smoke，不是 paper execution，也不是 controlled live。

---

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

- 在根目录 `.env` 中配置 `ALIBABA_API_KEY`
- 配置 `QWEN_MODEL`

若未配置或网络不稳定，可使用 `--skip-agent` 进入 debug/fallback 模式；主链仍可跑通，但 report 和 UI 会明确显示 agent 被跳过。

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

当前 report 固定包含：

- `market_discovery`
- `rule_parse`
- `forecast_service`
- `pricing_engine`
- `opportunity_discovery`
- `db_counts`
- `agent_summary`
- `artifacts`

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
