# Asterion Deep Audit and v2.0 Planning Blueprint

## 1. Executive Summary

这次审计的核心结论是：**当前 Asterion 已经不是“概念性 trading bot”，而是一套真实存在、边界清晰、以 operator 为中心的 constrained execution trading platform foundation。**

但它距离用户真正关心的目标——**长期稳定盈利 + 可规模化运营 + 高置信决策**——还差最后一段最难的路。这段路的主矛盾已经不再是“有没有基础设施”，而是：

1. **机会排序是否已经足够真钱化**
2. **execution capture / slippage / fill / cancellation 的经验模型是否足够强**
3. **是否有 capital-aware sizing / portfolio allocation，而不是只做 unit-opportunity 排序**
4. **calibration / threshold probability / execution feedback 是否已经形成持续自我强化的盈利闭环**
5. **operator 是否能更快、更稳、更少误判地处理更多机会**

我的总体判断如下。

### 直接结论

- **当前系统有哪些已经扎实成立的能力？**
  - canonical contracts、order routing、execution context、OMS / inventory / reconciliation 基础已经扎实成立。
  - controlled live boundary v2 已经真正落到 shell + backend，不再只是 handler 约定。
  - signer 已经改成 **按 `wallet_id` 推导 secret env scope**，且 transaction signing 不再接受 caller 注入 env var。
  - chain-tx 的 controlled live 路径已经是 **approve_usdc only + wallet/spender/amount allowlist + readiness + arming + scrubbed payload**。
  - weather opportunity 链已经不是“README 幻觉”：`forecast -> pricing -> calibration v2 -> execution priors -> feedback suppression -> ranking_score -> runtime / UI consumption` 已真实接线。
  - UI truth-source / source badge / read-model catalog / truth_source_checks 已经是实装能力。

- **当前系统最薄弱的安全点在哪里？**
  - 不是 UI env，也不是 signer payload 注入；这些大问题已经被修掉了。
  - 当前最薄弱的一环是：**submitter boundary 仍然是同进程 / 同信任域 / shared-secret 的 attestation 模型，而不是独立 control-plane / KMS / capability service 级别的边界**。
  - 对当前 manual-only / constrained live 范围来说它已经足够谨慎；对未来扩大真钱边界来说还不够。

- **当前系统最薄弱的赚钱链路在哪里？**
  - 不是 forecast plumbing，也不是 UI 有没有表。
  - 真正最薄弱的是：**execution economics 仍偏 heuristic，ranking 仍不是 capital-sized / portfolio-aware / marginal capacity-aware 的真钱排序。**

- **当前系统最强的赚钱链路在哪里？**
  - 在 weather temperature threshold 这类市场中，**station mapping 好、source fresh、calibration profile hit、threshold probability quality healthy、execution prior sample 足够、market quality 过关** 的样本，当前系统已经具备“挑出值得 operator 出手机会”的真实能力。

- **当前 ranking / calibration / execution feedback 是否已经足以支撑“稳定赚钱”？**
  - **还不够。**
  - 但必须明确：它们已经从“概念层”进入“主链层”。今天的问题不是“没接进去”，而是“接进去了但还不够经济化、资本化、经验化”。

- **当前是否具备“规模化赚钱”的基础？**
  - **具备部分基础，但还不具备完整基础。**
  - 现在更接近“高质量研究级 operator-assisted profit loop”，而不是“可规模化资本部署平台”。
  - 最大卡点不是 CPU 或 DuckDB，而是：
    1. 没有 capital allocator
    2. execution prior 特征空间还太粗
    3. calibration profile refresh 还没 fully operationalized
    4. operator workflow 还缺 size / capacity / action queue
    5. coverage 仍以 weather 为主、机会集本身有限

- **当前 operator console 是否真的帮助 operator 更好赚钱？**
  - **已经开始真正帮助，而不是单纯工程可视化。**
  - `Home` / `Markets` / `Execution` 三页已经明显进入 operator decision support 范畴。
  - 但它还没达到“让 operator 快速、稳健、低失误地扩大处理规模”的水平；缺的不是页面数量，而是 **recommended action / recommended size / cohort history / source freshness / capital constraint**。

- **当前系统有没有“看起来完成了，但实际仍不够可信”的部分？**
  - 有，且很重要：
    1. `ranking_score` 已进入主排序，但 `expected_dollar_pnl` 仍更接近 **unit opportunity EV**，不是实际 trade-sized PnL。
    2. calibration v2 已接线，但 materialization job 仍是 manual，不是 default scheduled。
    3. UI truth-source 机制存在，但 **phase/version/status constants 仍旧漂移**。
    4. read-model catalog / validation 已存在，但 `ui.daily_review_input` 当前 builder 与 registry 已经发生真实漂移，且会导致 UI lite build 在特定执行场景中失败。

### 总体判断

**Asterion 当前适合被定义为：**

> 一套已经具备真实边界、真实 execution audit、真实 operator console、真实 ranking/calibration/feedback 主链的 constrained execution trading system foundation。

**它还不适合被定义为：**

> 一套已经证明可长期稳定、可规模化、高置信赚钱的 production trading stack。

v2.0 不应该推翻重写；应该在现有基础上做 **盈利闭环强化、capital/risk 层补齐、truth-source 收口、读模型与交付稳定性提升**。

---

## 2. Audit Method and Scope

### 2.1 审计方法

本次结论以 **当前仓库真实代码 + 当前文档 + 当前测试行为** 为准，不以历史 analysis 文档结论代替现状。

我做了三层审查：

1. **入口文档与定位审查**
   - `README.md`
   - `AGENTS.md`
   - `docs/00-overview/*`
   - `docs/10-implementation/*`
   - 以及你指定的 trading / calibration / truth-source / UI architecture 设计文档

2. **关键代码审查**
   - execution / live boundary / signer / chain tx / execution gate / order router / portfolio
   - opportunity / pricing / execution priors / feedback / ranking
   - forecast / calibration / adapters / replay
   - readiness / monitoring / Dagster handlers / startup script
   - ui read models / truth source / pages / data access

3. **定向测试复核**
   - 我实际跑了多组你点名的核心测试，用来区分“已经被兜住的事实”和“还只是设计意图”。

### 2.2 实际复核的测试结果

#### 已通过的关键测试组

- `tests/test_p4_closeout.py` + `tests/test_p4_plan_docs.py`：**9 passed**
- `tests/test_live_submitter_backend.py` + `tests/test_submitter_boundary_attestation.py` + `tests/test_signer_shell.py`：**36 passed**
- `tests/test_execution_priors_materialization.py` + `tests/test_ranking_score_v2.py` + `tests/test_execution_feedback_loop.py` + `tests/test_forecast_calibration.py` + `tests/test_calibration_profile_v2.py` + `tests/test_threshold_probability_profile.py` + `tests/test_weather_pricing.py`：**25 passed**
- `tests/test_ui_runtime_env.py` + `tests/test_controlled_live_capability_manifest.py` + `tests/test_live_prereq_readiness.py` + `tests/test_controlled_live_smoke.py`：**22 passed**
- `tests/test_operator_truth_source.py`：**2 passed**
- `tests/test_ui_source_badges.py`：**2 passed**
- `tests/test_ui_read_model_catalog.py`：**2 passed**
- `tests/test_truth_source_checks.py`：**3 passed**
- `tests/test_ui_pages.py`：**1 passed**
- `tests/test_execution_foundation.py` 去掉两个 UI-lite 失败用例后：**22 passed，2 deselected，3 subtests passed**

#### 当前真实失败的关键测试

`tests/test_execution_foundation.py` 中有 **2 个 DuckDB/UI-lite contract 用例失败**：

- `test_ui_execution_exception_summary_surfaces_reconciliation_mismatches`
- `test_ui_execution_ticket_summary_surfaces_reconciliation_status`

失败原因非常具体：

- `build_ui_lite_db_once(...)` 会因为 `ui.daily_review_input` 缺少 registry 要求的 `item_id` 列而失败。
- 这不是测试写错，而是当前代码里 `ui.read_model_registry` 与 `ui_lite_db` 的 `daily_review_input` builder 已经发生真实 contract drift。

这件事非常重要，因为它说明：

- truth-source / read-model contract 机制是存在的，而且严格；
- 但当前 repo 确实已经出现了一处 **implementation 与 registry 不再同步** 的回归。

### 2.3 审计范围限制

我没有把历史 PnL、真实 exchange fills、真实 operator logs 当作当前仓库可直接证明的事实。因此：

- 我可以判断 **系统是否具备赚钱基础**；
- 但不会把“代码里有 ranking / feedback / calibration”误写成“已经证明稳定盈利”。

---

## 3. What Is Already Strong

这一节非常重要。Asterion 当前仓库里，已经有一批设计不该被轻易推翻，反而应该被保留并继续加强。

### 3.1 Canonical contracts / execution foundation 已经扎实

`asterion_core/contracts/*`、`order_router_v1.py`、`strategy_engine_v3.py`、`portfolio_v3.py`、`execution_gate_v1.py` 共同形成了一条稳定的 canonical path：

- watch-only snapshot
- strategy decision
- trade ticket
- execution context
- canonical order
- reservation / order / fill / reconciliation

这条链不是伪架构；`tests/test_execution_foundation.py` 的大部分核心用例都通过了，说明以下能力已经被兜住：

- trade ticket hash 稳定
- execution context 与 capability 绑定稳定
- canonical routing 校验 tick / min size / context match
- OMS transition 约束存在
- portfolio reservation / fill / release 语义存在
- gate 与 inventory / watch-only / threshold 约束是实装的

**结论**：execution foundation 是当前 repo 最值得保留的骨架之一。

### 3.2 Controlled live boundary v2 已经是真实能力，不再是文档承诺

当前代码里，controlled live 路径已经明显比历史 assessment 中描述的状态更强：

- `live_boundary.py` 已经有 `submitter_live_boundary_v2`
- attestation 带 `issuer` / `issued_at` / `expires_at` / `nonce` / `decision_fingerprint` / `attestation_mac`
- `live_submitter_v1.py` backend 会校验：
  - approved attestation
  - request/wallet/mode/backend/fingerprint match
  - issuer / attestation kind
  - expiry
  - decision fingerprint
  - HMAC MAC
  - persisted attestation row
  - single-use claim

`tests/test_live_submitter_backend.py` 和 `tests/test_submitter_boundary_attestation.py` 也证明了这些边界不只是“看起来有函数”，而是实测会拦截：

- missing attestation
- non-approved attestation
- endpoint mismatch
- attestation not persisted
- attestation reuse
- secret missing
- readiness not GO
- wallet not ready
- approval token mismatch

**结论**：当前 constrained real submitter 已经可以被视为“对 manual-only constrained live 足够谨慎”的设计。

### 3.3 Signer 边界修复到位

这是本次 repo 里最值得明确肯定的一处修复。

当前 `signer_service_v1.py`：

- order signing 不允许使用 env private key backend
- transaction signing **拒绝 payload supplied `private_key_env_var`**
- 只按 `wallet_id -> controlled_live_wallet_secret_env_var(wallet_id)` 推导 secret env scope
- 还会检查导出的地址必须等于交易 `from`

`tests/test_signer_shell.py` 也明确通过了：

- `env_private_key_tx_only_supports_transaction_signing`
- `env_private_key_tx_rejects_payload_supplied_env_var`
- `shell_does_not_expose_arbitrary_sign_api`

**结论**：历史上“caller 可影响 signer secret 选择”的大风险，现在已经不是当前事实。

### 3.4 Chain-tx controlled live 范围设计正确而克制

`chain_tx_v1.py` 的 controlled live 现在故意非常窄，这不是缺点，而是优点：

- 只允许 `approve_usdc`
- 必须过 `validate_live_side_effect_guard(expected_mode="controlled_live")`
- spender 必须同时存在于 wallet capability allowlist 和 chain registry
- amount 有 cap / allowlist 约束
- wallet readiness 要过
- raw signed tx / raw transaction / private key env name 都会被 scrub

`tests/test_controlled_live_smoke.py` 也已经覆盖了：

- not armed block
- readiness not GO block
- wallet not ready block
- allowlist / cap 失败 block
- controlled live approve broadcast 成功且 **不会持久化 raw signed tx**
- writerd deny non-allowlisted controlled live tables

**结论**：如果 v2.0 只考虑当前真钱边界，**不要急于扩大 tx kind**。现在的狭窄边界本身就是安全资产。

### 3.5 Ranking / execution priors / feedback 已真正进入主链

当前 weather opportunity 链最大的积极变化是：盈利相关逻辑不再只是辅助分析，而是进入了主排序。

`domains/weather/opportunity/service.py` 当前已经做了这些事：

- 计算 executable edge
- 引入 calibration v2 multiplier
- 引入 bias quality / threshold probability quality / regime stability
- 引入 market quality / mapping confidence / freshness penalty
- 读取 `ExecutionPriorSummary`
- 计算 `capture_probability`
- 计算 `risk_penalty`
- 计算 `capital_efficiency`
- 输出 `ranking_score`
- 再乘以 execution feedback penalty，形成 final ranking
- 写入 `why_ranked_json`

`strategy_engine_v3.py` 也已经把 `pricing_context["ranking_score"]` 作为真正排序依据。

`tests/test_execution_foundation.py`、`tests/test_ranking_score_v2.py`、`tests/test_execution_feedback_loop.py` 明确证明：

- runtime 会优先使用 `ranking_score`
- penalty-aware ranking 能压过 raw edge
- feedback penalty 会真实压低最终 ranking
- ops tie-breaker 不会压过 materially better EV

**结论**：历史上“ranking/calibration/feedback 只是旁路解释”的说法，已经不再适用于当前代码。

### 3.6 Calibration v2 和 threshold probability quality 已经是主链输入

当前 calibration 也不是旁路了。

`forecast/calibration.py` + `forecast/adapters.py` + `pricing/engine.py` 当前已经形成：

- calibration profile v2
- regime bucket
- bias correction
- corrected stddev
- threshold probability bucket quality
- distribution summary v2
- pricing context 透传到 fair value / watch-only / opportunity assessment

`tests/test_calibration_profile_v2.py`、`tests/test_threshold_probability_profile.py`、`tests/test_weather_pricing.py` 证明：

- calibration profile rows 能 materialize
- threshold probability profile 能被选中
- pricing context 里会带 threshold probability quality
- 低质量 calibration 会进入 ranking penalty reasons

**结论**：当前 prediction quality 的问题已经从“没接进去”转成“接进去了但还不够强”。

### 3.7 UI truth-source 基础已经比很多交易台都成熟

当前 UI 不是纯 Streamlit 拼图，而是已经有这些机制：

- `read_model_registry`
- `truth_source_checks`
- `source_badge`
- `primary_score_descriptor`
- `boundary_sidebar_truth`
- UI runtime banned-env / public bind boundary

通过的测试包括：

- `test_operator_truth_source`
- `test_ui_source_badges`
- `test_ui_read_model_catalog`
- `test_truth_source_checks`
- `test_ui_runtime_env`

**结论**：Asterion 当前的 UI 基础设计方向是对的，问题主要在 truth-source 最后一公里和读模型回归，不在总思路。

---

## 4. Critical Findings

先把最重要的判断压缩成一句话：

> Asterion 当前最大的短板，不是“没有基础设施”，而是“盈利经济学还不够真钱化、资本化、经验化”，其次是“truth-source 与 read-model contract 已出现局部漂移”。

### 当前最重要的 8 个结论

1. **controlled live boundary 已从“危险”进入“谨慎可用”**，但仍不应被误判为 production-grade multi-service trust boundary。
2. **signer 秘钥边界的大问题已经修掉**；不要再沿用旧 assessment 的结论。
3. **ranking/calibration/execution feedback 已经真实进入主排序**；旧结论里“只是旁路”已过时。
4. **当前最影响赚钱的不是模型 plumbing，而是 execution economics + capital allocation。**
5. **`expected_dollar_pnl` 命名已经落后于真实经济语义**；当前更像 unit-opportunity EV，不是 sized trade PnL。
6. **`weather_forecast_calibration_profiles_v2_refresh` 仍是 manual job**，这对高置信盈利是明显 operational gap。
7. **UI phase/version/truth-source 仍有漂移**：README/AGENTS 已进入 `v2.0 planning`，但 `surface_truth_shared.py`、`ui/app.py`、`home.py`、`start_asterion.sh` 仍在输出 post-P4 remediation active / closeout pending 等旧状态。
8. **`ui.daily_review_input` 当前存在真实 contract drift**，并已经被 `test_execution_foundation.py` 暴露成失败用例。

---

## 5. Security / Boundary Findings

### 5.1 Finding: Constrained live boundary 现在已足够安全用于当前 scope，但仍不是最终形态

- **优先级**：P1
- **类型**：Security / Boundary / Live Integrity
- **性质**：历史问题已大幅修复后的残余上限
- **受影响文件**：
  - `asterion_core/contracts/live_boundary.py`
  - `asterion_core/execution/live_submitter_v1.py`
  - `dagster_asterion/handlers.py`
  - `asterion_core/monitoring/capability_manifest_v1.py`
  - `tests/test_live_submitter_backend.py`
  - `tests/test_submitter_boundary_attestation.py`

#### 当前代码事实

- attestation 已升级到 v2，包含 TTL、nonce、decision fingerprint、MAC。
- backend 会校验 attestation persisted、MAC valid、not expired、single-use。
- shell 会在 live submit 前执行 guard，并在 guard 不成立时强制构造 blocked attestation。
- manifest / readiness / approval token / wallet readiness / backend kind 等都进入边界输入。

#### 当前测试事实

- submitter backend 测试覆盖 attestation missing / non-approved / endpoint mismatch / not persisted / reuse。
- boundary attestation 测试覆盖 manifest invalid、approval token mismatch、readiness not GO、wallet not ready、secret missing 等。

#### 当前文档事实

- README / AGENTS / P4 / post-P4 文档都把当前定位限定在 `operator console + constrained execution infra`，不宣称 unattended live。
- Controlled Live Boundary Design 文档已经把旧问题归档为历史 supporting design，并指出当前 scope 仍非 public production hardening。

#### 我的推断

当前 boundary 对**内部可信部署 + manual-only + constrained live**已经是一个合理的“强约束实现”。但它仍然有一个上限：

- attestation secret 在同一信任域中
- persisted attestation 也在同一 DB 中
- shell / backend / DB / env 仍是同进程或同部署边界内的信任组合

这意味着它不是“多服务相互独立背书”的 boundary。

#### 风险或缺口描述

如果未来你要扩大 live surface，例如：

- 不再只做 manual smoke / constrained live
- 扩大真钱 submit 的频次和范围
- 引入更多内部 worker / CLI / automation
- 让更多服务能接近 signer / submitter

那么当前同信任域模型会成为边界上限。

#### 为什么重要

当前不是立即会爆的风险，但它决定了：

- 现在是否可以安全做小范围 live：**可以**
- 是否已经适合更大范围真钱生产：**还不行**

#### 对“稳定、规模化、高置信赚钱”的影响

- 对“当前 manual-only 赚钱试验”影响不大
- 对“后续扩大 live capture”影响很大

#### 推荐修复方向

**v2.0 不需要立刻推翻这套边界，但要明确做两层策略：**

1. **短期保留现状**：继续 manual-only / constrained live，不扩大 tx kind，不扩大自动 side effect。
2. **中长期升级**：如果要扩大真钱边界，再引入：
   - signer secret resolver abstraction（env -> KMS / Vault）
   - attestation issuer abstraction（control-plane service or signing daemon）
   - session / user / operator identity audit
   - dual-control / approval chain（如果 live scope 扩大）

#### 需要改哪些模块

- 短期：无需大改 core boundary 逻辑
- 中长期：
  - `signer_service_v1.py`
  - `live_submitter_v1.py`
  - `live_boundary.py`
  - deployment / secret management 层

#### 需要哪些测试

- env resolver vs KMS resolver contract tests
- attestation issuer boundary tests
- multi-process / replay / revocation tests

#### 是否需要 migration

- 短期不需要
- 中长期大概率需要（如果引入 issuer/session/approval chain）

#### 是否需要文档同步

需要：要把“当前已经安全 enough for constrained live，但还不是扩张后的终态”写清楚。

#### 推荐实施顺序

**Phase 3 以后再做**，除非 v2.0 明确扩大真钱边界。

---

### 5.2 Finding: Signer secret boundary 的大风险已经被修复，应该保留而不是重写

- **优先级**：P2
- **类型**：Security
- **性质**：历史问题已修复
- **受影响文件**：
  - `asterion_core/signer/signer_service_v1.py`
  - `asterion_core/blockchain/chain_tx_v1.py`
  - `tests/test_signer_shell.py`
  - `tests/test_controlled_live_smoke.py`

#### 当前代码事实

- signer transaction path 直接拒绝 payload supplied `private_key_env_var`
- secret env key 只由 `wallet_id` 推导
- order signing 仍被禁止用 env private key backend
- raw tx 在持久化链路被 scrub

#### 当前测试事实

- `test_env_private_key_tx_rejects_payload_supplied_env_var`
- `test_env_private_key_tx_only_supports_transaction_signing`
- `test_controlled_live_approve_broadcasts_and_does_not_persist_raw_signed_tx`

都通过了。

#### 当前文档事实

README 也已经同步说明：controlled-live tx signer 固定按 `wallet_id` 推导 secret scope。

#### 我的推断

这是一个“应继续收紧，但不应再误判为现存漏洞”的点。

#### 风险或缺口描述

当前剩余风险不是 caller-env 注入，而是：**secret 仍是 env-based secret，而不是 external secret manager**。

#### 为什么重要

这是将来扩大真钱边界时必须升级的点，但不是今天最影响赚钱或最影响当前安全的点。

#### 对“稳定、规模化、高置信赚钱”的影响

主要影响 future live expansion，不是当前利润主瓶颈。

#### 推荐修复方向

- 保留 wallet_id -> secret scope 的 contract
- v2.0 后半段引入 secret resolver abstraction，而不是直接重写 signer shell

#### 推荐实施顺序

**不要抢前排优先级**。保留现状，作为未来 live expansion 的 prerequisite。

---

### 5.3 Finding: UI runtime boundary 已经 default-safe，但 UI truth-source 仍在传递过时状态

- **优先级**：P1
- **类型**：Security / UX / Truth-Source
- **性质**：新发现问题
- **受影响文件**：
  - `ui/runtime_env.py`
  - `ui/app.py`
  - `ui/surface_truth.py`
  - `asterion_core/ui/surface_truth_shared.py`
  - `ui/pages/home.py`
  - `start_asterion.sh`
  - `tests/test_ui_runtime_env.py`
  - `tests/test_operator_truth_source.py`

#### 当前代码事实

- `ui/runtime_env.py` 已经有 allowlist / banned-env detection / public bind opt-in。
- `ui/app.py` 启动时会先检查 `load_ui_runtime_boundary_status()`，blocked 就拒绝渲染。
- `start_asterion.sh` 也已默认 `127.0.0.1`，public bind 需要显式 opt-in。
- 但 `surface_truth_shared.py` 里仍固定：
  - `CURRENT_PHASE_STATUS = "post-P4 remediation active / closeout pending objective verification"`
  - `TRUTH_SOURCE_DOC = "docs/10-implementation/phase-plans/Post_P4_Remediation_Implementation_Plan.md"`
- `ui/app.py` 顶部 header 仍显示：
  - `Post-P4 remediation active`
  - `Closeout pending objective verification`
  - 版本 `Asterion v1.2`
- `README.md` 与 `AGENTS.md` 的当前状态已经是：
  - `P4 accepted; post-P4 remediation accepted; v2.0 planning`
  - README 版本是 `v1.5`
- `start_asterion.sh` 的 `print_boundary_summary()` 还在输出 `remediation in progress`。
- `ui/pages/home.py` 也还在 caption 中复述旧 phase 状态。

#### 当前测试事实

- `test_ui_runtime_env.py` 证明 UI runtime boundary 是真的。
- `test_operator_truth_source.py` 也通过了，但它通过的同时说明：**测试本身把旧 phase status constant 锁死了。**

#### 当前文档事实

- README / AGENTS / V2 placeholder 都表明当前仓库已经切到 `v2.0 planning`。
- Post-P4 remediation plan 当前应视为 historical accepted record，不应继续被 UI 当作“当前 truth-source doc”。

#### 我的推断

当前 UI 在“安全边界”层面比历史版本强很多；但在“系统当前状态”表达上已经落后于 repo 现实。

#### 风险或缺口描述

这是一个高价值 truth-source 问题：

- operator 会被旧 phase/status 误导
- reviewer 会觉得 repo 还停在 remediation closeout
- UI 自己在强调一个已经不是主入口的 historical doc

#### 为什么重要

因为 Asterion 现在已经不是“只有技术债”的系统，而是一个需要靠 operator 做真钱判断的系统。**状态表述错了，就会伤害决策质量和开发优先级。**

#### 对“稳定、规模化、高置信赚钱”的影响

- 不直接影响撮合
- 但会直接影响：
  - operator 对当前可信度的判断
  - roadmap 排序
  - 团队对什么已经完成、什么仍是 current risk 的理解

#### 推荐修复方向

1. 新建一个 **单一 truth-source**，不要继续把 phase/status 分散在 README、UI constants、shell script 里各写一份。
2. 建议新增一个 checked-in 轻量配置，例如：
   - `config/system_status.json` 或 `config/system_status.py`
3. 至少统一这些字段：
   - current_phase_status
   - system_positioning
   - public claims / live negations
   - displayed version
   - truth_source_doc
4. `ui/app.py`、`surface_truth_shared.py`、`start_asterion.sh`、相关 tests 都从同一处读取。
5. UI 的 `truth_source_doc` 不应再指向 historical remediation plan；建议改成：
   - 一个专门的 `Current_System_Status.md`；或
   - `V2_Implementation_Plan.md` 在扩充后作为当前入口。

#### 需要改哪些模块

- `asterion_core/ui/surface_truth_shared.py`
- `ui/app.py`
- `ui/pages/home.py`
- `start_asterion.sh`
- 文档入口文件与 tests

#### 需要哪些测试

- phase status consistency test
- version consistency test
- UI header / sidebar truth-source acceptance test
- docs vs constants sync test

#### 是否需要 migration

不需要。

#### 是否需要文档同步

需要，而且优先级高。

#### 推荐实施顺序

**Immediate Fixes 第一批就做。**

---

### 5.4 Finding: 当前不应扩大 controlled live 的能力范围

- **优先级**：P1
- **类型**：Security / Ops Strategy
- **性质**：不是 bug，而是策略建议
- **受影响文件**：
  - `asterion_core/blockchain/chain_tx_v1.py`
  - `dagster_asterion/job_map.py`
  - `docs/30-trading/Controlled_Live_Boundary_Design.md`

#### 当前代码事实

- controlled live chain tx 当前只允许 `approve_usdc`
- submitter live path 有 attestation v2
- 当前系统仍明确不是 unattended / unrestricted live

#### 当前测试事实

- controlled live smoke tests 已经把 approve path、not armed、readiness not GO、wallet not ready、allowlist failure 都兜住了。

#### 我的推断

在当前阶段扩大以下事情，ROI 都不高且会提升风险：

- 更多 tx kinds
- 更广泛 chain interactions
- automated order management / cancel-replace
- unattended live

#### 为什么重要

因为 v2.0 的主任务不是“扩大 side-effect 能力”，而是“把现有机会链变得更会赚钱”。

#### 推荐修复方向

保持 current live boundary scope，不扩张；把主要资源投到 **profit engine / allocator / calibration freshness / operator throughput**。

---

## 6. Profitability / Alpha Findings

这一节是本报告最重要的部分。

### 6.1 当前 Asterion 最有可能靠什么赚钱？

当前最可能的盈利来源，不是“通用 prediction market alpha”，而是更具体的链条：

> **weather threshold markets**（尤其是温度桶类市场）中，利用 station-first mapping、corrected forecast distribution、threshold probability quality、market quality filtering、execution priors、feedback suppression，把“看起来有 edge”的机会进一步压缩成“更有机会被 capture 且风险更低”的机会，再由 operator 做最终执行判断。

也就是说，当前最真实的 alpha 不在“大模型 agent”，也不在 UI，而在：

1. **天气分布 -> 阈值概率**
2. **阈值概率 -> fair value**
3. **fair value -> executable edge**
4. **execution priors / feedback -> ranking suppression**
5. **operator 在较少高质量样本上人工出手**

### 6.2 当前最接近真实 alpha 的能力是什么？

**最接近真实 alpha 的能力，是“经过 calibration v2 + threshold probability quality + execution priors + feedback suppression 的 ranking v2”**。

不是 raw edge_bps，不是 model fair value 本身，也不是单纯 agent review。

### 6.3 当前哪些能力已经足以支撑“赚钱基础”？

我认为已经足以支撑“赚钱基础”的能力有 5 类：

1. **机会定价链真实存在**：forecast / pricing / watch-only / opportunity assessment 已接线。
2. **排序链真实存在**：runtime 真实按 `ranking_score` 排序。
3. **处罚链真实存在**：calibration / market quality / freshness / mapping / feedback 都会压低排名。
4. **执行审计链真实存在**：submitter / signer / chain tx / external reconciliation / execution science 已经存在。
5. **operator surface 真实存在**：Home / Markets / Execution 已经能帮助筛选机会与复盘 capture / miss / distortion。

### 6.4 当前最阻碍稳定赚钱的 5 个问题

#### 1) execution economics 仍偏 heuristic

当前 `service.py` 中的：

- `_slippage_bps`
- `_fill_probability`
- `_depth_proxy`
- `_ops_tie_breaker`

虽然会被 priors 修正，但 baseline 仍较粗。

#### 2) ranking 仍不是 trade-sized / portfolio-sized 真实利润排序

当前 `ranking_score` 已经是 unit-opportunity EV / capture / risk / capital-efficiency 语义，但它还不是：

- recommended size aware
- bankroll aware
- position limit aware
- concentration aware
- portfolio marginal utility aware

#### 3) calibration v2 有了，但仍依赖 normal distribution correction

当前 adapter 仍是 point forecast -> normal distribution -> bias/stddev correction。
对 threshold markets 来说这已经比没有 calibration 强很多，但它还不是真正的 ensemble-native probability engine。

#### 4) execution feedback 有了，但 cohort 维度仍偏粗

当前 execution priors / feedback 主要按：

- market
- strategy
- wallet
- side
- horizon bucket
- liquidity bucket

这对天气类“短生命、单市场样本稀疏”的场景还不够细，也不够 transferable。

#### 5) operator 仍缺 size / capacity / action queue

现在 operator 看得到“哪些机会好”，但看不到：

- 建议做多大
- 当前是否占用太多资本
- 这个机会是否只是研究级，而不是应推进执行

### 6.5 当前最阻碍规模化赚钱的 5 个问题

1. **没有 capital allocator / portfolio budget layer**
2. **execution priors 维度不够适合天气这种 ephemeral market 结构**
3. **calibration profile refresh 仍是 manual，freshness 不可运营化**
4. **UI / read-model / handlers 大文件化，会拖慢迭代与稳定交付**
5. **operator throughput 仍靠人工阅读多个 panel，不是 action queue + recommended size workflow**

### 6.6 当前最阻碍高置信赚钱的 5 个问题

1. **`expected_dollar_pnl` 字段名比真实语义更强，容易让人高估经济解释力**
2. **sparse prior 时仍会回退到较粗 heuristic**
3. **calibration v2 的 freshness 没有自动 materialization / freshness SLO**
4. **feedback penalty 的权重与 aggregation 仍是 hand-tuned，不是 replay-validated**
5. **没有“ranking 改动必须在 replay/economic acceptance 上过关”的 delivery gate**

### 6.7 当前更大的问题到底是什么？

如果必须排序，我的判断是：

1. **ranking 不够真钱化 / 不够 capital-aware**
2. **execution capture 模型不够强**
3. **execution feedback 闭环还不够细**
4. **calibration freshness / tail quality 还不够强**
5. **market coverage 与 operator workflow 共同限制规模**
6. **prediction quality 本身不是当前第一瓶颈**

也就是说，当前更大的问题不是“预测完全不行”，而是：

> **预测已经够支撑一部分机会识别，但从机会识别到真实可持续赚钱之间，execution economics、capital allocation、feedback closure 还不够成熟。**

---

### 6.8 Finding: Ranking v2 已真实存在，但还不够“真钱排序”

- **优先级**：P1
- **类型**：Trading / Profitability
- **性质**：新发现问题
- **受影响文件**：
  - `domains/weather/opportunity/service.py`
  - `asterion_core/contracts/opportunity.py`
  - `asterion_core/runtime/strategy_engine_v3.py`
  - `tests/test_ranking_score_v2.py`
  - `tests/test_execution_foundation.py`

#### 当前代码事实

`_ranking_score_v2_decomposition(...)` 当前会计算：

- `capture_probability = fill_probability * submit_ack_rate * fill_rate * resolution_rate`
- `expected_dollar_pnl = gross_unit_edge * capture_probability`
- `risk_penalty`
- `capital_efficiency = depth_proxy / unit_capital_cost`
- `ranking_score = max(expected_dollar_pnl - risk_penalty, 0) * capital_efficiency + ops_tie_breaker`

runtime 确实使用 `pricing_context["ranking_score"]` 排序。

#### 当前测试事实

- `test_prior_backed_ranking_populates_v2_fields`
- `test_ops_tie_breaker_does_not_override_materially_better_ev`
- `test_feedback_suppression_applies_after_ranking_v2`
- `test_strategy_engine_prefers_higher_ranking_score_over_absolute_edge`

这些测试都通过，说明 ranking v2 不是假接线。

#### 当前文档事实

README 和 Execution Economics Design 文档都已将 `ranking_score` 定义为 unit-opportunity EV / capture / risk / capital-efficiency 语义。

#### 我的推断

当前 ranking v2 是一个**明显优于过去的中间态**：

- 它比 raw edge 强很多
- 但它还不是最终真钱排序

#### 风险或缺口描述

主要缺口有两个：

1. `expected_dollar_pnl` 名称过强，真实上更接近 **unit opportunity EV**。
2. 排序没有 sizing / bankroll / portfolio concentration / capacity 上限。

#### 为什么重要

如果要稳定赚钱，排序必须回答：

- 不只是“哪个机会好”
- 而是“哪个机会值得分配资本、分多少、为什么”

#### 对“稳定、规模化、高置信赚钱”的影响

这是 **当前最直接的利润瓶颈之一**。

#### 推荐修复方向

v2.0 不要废掉 `ranking_score` 字段，而是升级语义：

1. 保持 `ranking_score` 作为主排序字段，避免 contract 平行化。
2. 新增真实经济字段：
   - `expected_unit_pnl_quote`
   - `recommended_size`
   - `expected_trade_pnl_quote`
   - `marginal_capital_efficiency`
   - `position_limit_status`
   - `sizing_model_version`
3. strategy engine 不直接自己“想大小”，而是接一个 allocator / sizing result。
4. UI 不再把 `expected_dollar_pnl` 当成 operator 看到的第一利润字段。

#### 需要改哪些模块

- `domains/weather/opportunity/service.py`
- `asterion_core/contracts/opportunity.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/data_access.py`

#### 需要哪些测试

- ranking + sizing integration tests
- sized expected pnl correctness tests
- bankroll / position limit tests
- UI label truthfulness tests

#### 是否需要 migration

需要，至少涉及 read model / snapshot / possibly runtime tables 的新增字段。

#### 是否需要文档同步

需要。

#### 推荐实施顺序

**Phase 1 核心工作之一。**

---

### 6.9 Finding: Execution priors / feedback 已经形成闭环雏形，但 feature space 还不够适合 weather 市场

- **优先级**：P1
- **类型**：Trading / Scale
- **性质**：新发现问题
- **受影响文件**：
  - `domains/weather/opportunity/execution_priors.py`
  - `domains/weather/opportunity/execution_feedback.py`
  - `domains/weather/opportunity/service.py`
  - `dagster_asterion/job_map.py`
  - `tests/test_execution_priors_materialization.py`
  - `tests/test_execution_feedback_loop.py`

#### 当前代码事实

当前 execution prior key 主要由这些维度构成：

- `market_id`
- `strategy_id`
- `wallet_id`
- `side`
- `horizon_bucket`
- `liquidity_bucket`

feedback aggregation 使用 market / strategy / wallet 三层，默认权重 `0.50 / 0.30 / 0.20`。

#### 当前测试事实

- priors materialization 已测试通过
- feedback penalty suppression 已测试通过
- scope breakdown 也有覆盖

#### 当前文档事实

Execution Economics Design 文档已经把 feedback path 视为 accepted supporting design。

#### 我的推断

这套机制当前已经比“没有经验反馈”强很多，但对 weather 市场有一个天然问题：

- **market_id 太短生命**
- 很多 daily weather market 是一次性 market
- 单市场 cohort 很容易稀疏

因此，如果 execution prior 仍大量围绕 `market_id`，它的可迁移性和 sample efficiency 都会受限。

#### 风险或缺口描述

当前 execution prior 更适合“重复 market id 的 venue / instrument”，不完全适合“按日期生成的新 market”。

#### 为什么重要

如果 priors 不能跨 market template / station / regime 迁移，你就很难形成稳定的 execution learning。

#### 对“稳定、规模化、高置信赚钱”的影响

这是 **规模化赚钱** 的核心阻碍之一。

#### 推荐修复方向

v2.0 建议把 execution prior / feedback 维度升级成更适合 weather 的结构：

1. 在 key 中新增或引入 shrinkage 的 cohort 维度：
   - `station_id`
   - `location_family`
   - `market_template`
   - `time_to_close_bucket`
   - `price_bucket`
   - `edge_bucket`
   - `regime_bucket`
2. 反馈聚合从固定 market/strategy/wallet 改为：
   - template / station / strategy / wallet / global baseline 的层级 shrinkage
3. 把 prior freshness 带到 serving summary：
   - `materialized_at`
   - `source_window_end`
   - `sample_sufficiency_status`

#### 需要改哪些模块

- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/service.py`
- `dagster_asterion/handlers.py`
- `job_map.py`

#### 需要哪些测试

- new key derivation tests
- hierarchical aggregation tests
- sparse cohort fallback tests
- replay uplift tests

#### 是否需要 migration

需要。建议新增 `weather.weather_execution_priors_v2`，而不是直接在旧表上暴力堆字段。

#### 是否需要文档同步

需要。

#### 推荐实施顺序

**Phase 1 后半段 / Phase 2 前半段。**

---

### 6.10 Finding: Calibration v2 已接主链，但 materialization 还没有进入真正可运营状态

- **优先级**：P1
- **类型**：Trading / Ops / Observability
- **性质**：新发现问题
- **受影响文件**：
  - `domains/weather/forecast/calibration.py`
  - `domains/weather/forecast/adapters.py`
  - `domains/weather/pricing/engine.py`
  - `dagster_asterion/job_map.py`
  - `dagster_asterion/handlers.py`
  - `tests/test_calibration_profile_v2.py`
  - `tests/test_threshold_probability_profile.py`
  - `tests/test_weather_pricing.py`

#### 当前代码事实

- calibration v2 已存在真实 serving table：`weather.forecast_calibration_profiles_v2`
- adapter 会做 bias correction / corrected stddev / threshold probability quality
- pricing context 会透传 calibration v2 信息
- 但 `job_map.py` 中 `weather_forecast_calibration_profiles_v2_refresh` 当前仍是 **manual job**，不是 enabled-by-default scheduled job。
- 同时 execution priors 有 `runtime.execution_feedback_materializations` 这类 status 记录，而 calibration v2 目前没有对等的 materialization status / freshness surface。

#### 当前测试事实

- calibration profile v2 materialization 测试通过
- threshold probability profile 选择测试通过
- pricing 测试也验证了 calibration quality 会进入 ranking penalties

#### 当前文档事实

README 已经如实写 calibration v2 已进入主链；V2 设计文档则强调它仍是当前 accepted supporting design，而不是未来终态。

#### 我的推断

当前 calibration v2 在“算法接线”上已经成立，但在“运维可持续性”上还没闭合。

#### 风险或缺口描述

如果 calibration profile refresh 仍然依赖手动触发：

- profile 容易变旧
- operator 看不到 profile age
- 机会排序会在“看起来用了 calibration v2”的同时，偷偷使用 stale calibration

#### 为什么重要

这会直接伤害高置信赚钱，因为 threshold probability 类市场对 calibration freshness 很敏感。

#### 对“稳定、规模化、高置信赚钱”的影响

这是 **高置信赚钱** 的关键缺口之一。

#### 推荐修复方向

1. 把 `weather_forecast_calibration_profiles_v2_refresh` 加入 schedule，建议与 resolution reconciliation 后联动。
2. 新增 calibration materialization status table，例如：
   - `runtime.forecast_calibration_materializations`
3. 在 UI / pricing context / why-ranked 中显示：
   - calibration profile materialized_at
   - source window end
   - freshness status
4. 中期再做更高阶升级：
   - ensemble-native / quantile-native distribution
   - 非 normal tail handling

#### 需要改哪些模块

- `dagster_asterion/job_map.py`
- `dagster_asterion/handlers.py`
- calibration persistence / UI summary modules
- `ui/pages/markets.py`
- `ui/pages/execution.py`

#### 需要哪些测试

- calibration refresh schedule tests
- materialization status tests
- freshness indicator tests
- stale profile penalty tests

#### 是否需要 migration

需要。

#### 是否需要文档同步

需要。

#### 推荐实施顺序

**Immediate / Phase 1 之间就可以做，ROI 很高。**

---

## 7. Scale / Throughput Findings

### 7.1 当前是否有“规模化赚钱”的基础？

**有一部分基础，但还不是完整基础。**

必须区分两种“规模”：

1. **工程吞吐规模**：能不能跑更多表、更多 job、更多页面
2. **盈利规模**：能不能在更多机会里稳定分配资本并维持 capture quality

当前 Asterion 在第一种规模上还行，在第二种规模上还不够。

### 7.2 当前最大的 scale blocker 是什么？

如果只能选一个，我会选：

> **没有 capital allocation / portfolio budgeting layer。**

因为只要没有这层：

- 再多机会也只能人工粗选
- 再好的 ranking 也无法转化成“应该下多大”
- 再多 market coverage 也会变成 operator overload

### 7.3 Finding: 当前系统的规模瓶颈首先不是算力，而是资本分配与 workflow

- **优先级**：P1
- **类型**：Scale / Trading / UX
- **性质**：新发现问题
- **受影响文件**：
  - `asterion_core/risk/portfolio_v3.py`
  - `asterion_core/execution/execution_gate_v1.py`
  - `asterion_core/runtime/strategy_engine_v3.py`
  - `ui/pages/home.py`
  - `ui/pages/markets.py`

#### 当前代码事实

- `portfolio_v3.py` 已有 inventory / reservation / fill / release 语义
- `execution_gate_v1.py` 已有 watch-only / market / inventory / economic threshold gate
- `strategy_engine_v3.py` 主要是排序与生成 decisions
- 但没有看到：
  - bankroll budget
  - exposure concentration cap
  - market family / location correlation cap
  - recommended size decision
  - capital reservation by opportunity priority

#### 当前测试事实

execution foundation 测试覆盖了 inventory semantics 和 gate semantics，但没有覆盖：

- portfolio budget allocation
- recommended size
- correlated exposure limits
- multi-opportunity capital scheduling

#### 当前文档事实

Execution Economics Design 文档也明确写了当前还不是 autonomous capital optimizer / multi-asset portfolio optimization。

#### 我的推断

这说明当前 repo 对自己的 scope 是诚实的：它还没有 allocator。

#### 风险或缺口描述

没有 allocator，规模增长时会出现：

- 好机会之间抢资本
- operator 无法快速比较 marginal value
- 高 ranking 机会被过度或不足执行

#### 为什么重要

这是从“能赚一点”到“能规模化赚钱”的核心缺口。

#### 对“稳定、规模化、高置信赚钱”的影响

**规模化赚钱的第一阻碍。**

#### 推荐修复方向

新增一个薄的 allocator 层，不要把它塞回 strategy engine：

- `risk/allocator_v1.py` 或 `portfolio_allocator_v4.py`
- 输入：机会 assessment + inventory + wallet budget + concentration rules
- 输出：
  - `recommended_size`
  - `expected_trade_pnl`
  - `budget_status`
  - `allocation_reason`

#### 推荐实施顺序

**Phase 1 核心工作。**

---

### 7.4 Finding: UI lite / handlers 当前足够支撑 weather-only，但会成为下一阶段 delivery 瓶颈

- **优先级**：P2
- **类型**：Scale / Architecture / Delivery
- **性质**：新发现问题
- **受影响文件**：
  - `asterion_core/ui/ui_lite_db.py`（3190 行）
  - `ui/data_access.py`（1788 行）
  - `dagster_asterion/handlers.py`（3249 行）
  - `job_map.py`

#### 当前代码事实

- UI lite build 基本是 full snapshot attach + create-or-replace tables 模式
- handlers 文件已经非常大
- data_access 与 read-model builder 都在承担多种职责

#### 当前测试事实

很多 truth-source / UI tests 通过，说明现在还可控；但 `ui.daily_review_input` contract drift 已经说明大文件化开始带来同步风险。

#### 当前文档事实

UI Read Model Design 文档已经明确把“大文件 / truth-source 分散”当作需要解决的长期维护风险。

#### 我的推断

这不是“今天的主要利润问题”，但如果不处理，会在 v2.0 中期显著拖慢交付速度。

#### 风险或缺口描述

- 新增一个 read model / page 逻辑，很容易在 builder、registry、loader、page 四处同步出错
- handler 演进会更难 review

#### 推荐修复方向

- Phase 2 再拆，不要现在大重构
- 先按 builder / loader / handler family 做物理拆分
- 保留现有 contract 和 table 名，不做语义大搬家

---

## 8. UX / Operator Workflow Findings

### 8.1 当前 UI 已经不像纯工程控制台，但还不是完整的 operator decision workstation

这点需要非常客观地说。

### 哪些 surface 现在最有价值？

按“对赚钱的直接帮助”排序，我会排成：

1. **Markets**：当前最有价值
2. **Home**：第二高价值
3. **Execution**：对复盘和闭环有很高价值
4. **System**：对安全与 readiness 有价值，但不是直接 alpha surface
5. **Agents**：对 exception review 有价值，但直接 ROI 最低

### 8.2 当前 operator 是否能快速分辨高质量机会 / 低质量机会 / research-only 机会？

**已经比旧状态好很多，但还不够快。**

当前 `Markets` / `Home` 已经有：

- `actionability_status`
- `source_badge`
- `market_quality_status`
- `feedback_status`
- `capture_probability`
- `why_ranked_json`

但 operator 还缺三件最关键的信息：

1. **推荐动作**：review / trade now / blocked / research only
2. **推荐 size**：值得投入多少资本
3. **机会容量与约束**：高 ranking 是否只是“小而美”的单位机会

### 8.3 Finding: 当前 UI 最大的问题不是“太工程”，而是缺少 capital / action semantics

- **优先级**：P1
- **类型**：UX / Trading
- **性质**：新发现问题
- **受影响文件**：
  - `ui/pages/home.py`
  - `ui/pages/markets.py`
  - `ui/pages/execution.py`
  - `ui/data_access.py`
  - `domains/weather/opportunity/service.py`

#### 当前代码事实

- `Markets` 与 `Home` 已经显示 `ranking_score`、`expected_dollar_pnl`、`capture_probability`、`feedback_status` 等。
- `why_ranked_json` 已经能把 mode / capture / ev / risk / feedback 展出来。
- 但没有：
  - `recommended_size`
  - `expected_trade_pnl`
  - `capital_limit_status`
  - `action_bucket`
  - `time_to_close_risk`

#### 当前测试事实

UI pages 与 truth-source tests 当前主要覆盖：

- primary score
- source badges
- truth-source descriptor
- basic page rendering

没有覆盖 operator capital-action semantics，因为当前代码里确实还没有这层。

#### 当前文档事实

Operator Console Truth Source Design 已经把“主分数 / source badge / degraded state”作为 accepted design；但它没有解决 allocator 缺失问题。

#### 我的推断

当前 operator UI 已经能帮助发现机会；v2.0 的任务是让它帮助**更快做对的资本决策**。

#### 风险或缺口描述

没有 capital/action semantics 时，operator 很容易：

- 误把 unit EV 当成 full trade EV
- 在很多看起来都不错的机会里不知道先做哪个、做多大

#### 推荐修复方向

新增 `Opportunity Decision Queue`：

- action bucket：`trade_now / review / blocked / research_only`
- recommended size
- expected trade pnl
- action confidence
- dominant blocker / dominant reason
- cohort history summary

#### 推荐实施顺序

**跟 allocator 一起做，Phase 1/2。**

---

### 8.4 Finding: Agents / System 页不应继续抢 roadmap 资源

- **优先级**：P3
- **类型**：UX / Roadmap
- **性质**：策略建议

#### 当前代码事实

- `Agents` 页已经明确声明自己不在 execution path 中
- `System` 页主要承载 readiness / evidence / diagnostics

#### 我的推断

这两个页面当前已经“够用”。

#### 推荐方向

- 不要在 v2.0 前期把很多资源花在这两页的视觉优化上
- 真正高 ROI 的是 `Home` / `Markets` / `Execution`

---

## 9. Architecture / Maintainability Findings

### 9.1 哪些模块应该继续保留和加强？

这些模块，我建议明确列入“保留并增强，而不是推翻”的清单：

- `asterion_core/contracts/*`
- `execution/order_router_v1.py`
- `execution/execution_gate_v1.py`
- `risk/portfolio_v3.py` 的 inventory foundation
- `runtime/strategy_engine_v3.py` 的 thin orchestration 角色
- `domains/weather/opportunity/*` 的 priors/feedback framework
- `domains/weather/forecast/calibration.py` 的 calibration v2 scaffold
- `asterion_core/ui/read_model_registry.py`
- `truth_source_checks`
- readiness / capability manifest / UI runtime boundary

### 9.2 哪些模块如果继续堆功能会出问题？

#### 1) `ui_lite_db.py`

已经 3000+ 行，而且 builder + validation + materialization 混在一起。

#### 2) `ui/data_access.py`

已经接近 1800 行，承担了 page loader、fallback、truth-source、overview 组装等多职责。

#### 3) `dagster_asterion/handlers.py`

已经 3200+ 行，跨 discovery / pricing / execution / calibration / readiness / controlled live 等多个域。

### 9.3 Finding: `ui.daily_review_input` 的 contract drift 是一个已经发生的架构信号

- **优先级**：P1
- **类型**：Architecture / Testing / Truth-Source
- **性质**：新发现问题
- **受影响文件**：
  - `asterion_core/ui/read_model_registry.py`
  - `asterion_core/ui/ui_lite_db.py`
  - `tests/test_execution_foundation.py`

#### 当前代码事实

- registry 要求 `ui.daily_review_input` 的主键列是 `item_id`
- builder 当前没有生成 `item_id`
- `validate_ui_lite_db()` 会严格检查 required columns

#### 当前测试事实

`test_execution_foundation.py` 中两个 UI-lite 构建用例因此失败。

#### 当前文档事实

README 仍把 `ui.daily_review_input` 当作 future `Daily Review Agent` 的输入面之一。

#### 我的推断

这说明：

- 当前 read-model registry 的理念是对的
- 但 builder / registry / tests 已经不再完全同步

#### 风险或缺口描述

这类 drift 如果继续出现，会伤害：

- UI build 稳定性
- truth-source credibility
- v2 delivery speed

#### 为什么重要

这是一个已经造成真实测试失败的缺口，不是理论问题。

#### 对“稳定、规模化、高置信赚钱”的影响

它不直接影响 alpha，但会直接影响 operator surface 的可靠性和交付节奏。

#### 推荐修复方向

立即修：

- 给 `ui.daily_review_input` 增加 `item_id`
- 建议用 stable object id，例如基于 `run_id + ticket_id + request_id`
- 加一条专门 regression test：`daily_review_input contains item_id and validates`

#### 推荐实施顺序

**Immediate Fixes 第一项。**

---

### 9.4 Finding: `strategy_engine_v3.py` 当前反而不应该大改

- **优先级**：P3
- **类型**：Architecture
- **性质**：积极判断

#### 当前代码事实

`strategy_engine_v3.py` 现在主要做：

- filter hold/no_trade
- filter min_edge
- sort by priority / ranking_score / edge / time
- 生成 decisions

#### 我的推断

这正是一个 orchestrator 应有的复杂度。问题不在 strategy engine，而在它前面的 economics 和它后面的 allocator。

#### 推荐方向

- 保持 `strategy_engine_v3.py` 薄
- 不要把 capital allocator、complex risk logic 再塞回它

---

## 10. Observability / Testing / Delivery Findings

### 10.1 当前测试已经兜住了什么？

当前最值得肯定的是：测试已经不只是 unit correctness，而是开始兜真实系统语义。

#### 已被强兜住的事实

- live submitter boundary v2
- signer env var injection 禁止
- controlled live approve-only boundary
- ranking v2 / feedback suppression
- calibration v2 / threshold probability profile
- truth-source checks / source badges / catalog
- page basic rendering
- P4 / post-P4 historical status表达

### 10.2 当前测试还缺什么？

#### 1) 经济接受度测试还不够强

目前有 ranking / feedback unit tests，但缺：

- replay uplift acceptance
- historical cohort profit attribution regression
- ranking change must not reduce realized capture on benchmark windows

#### 2) UI truth-source 现在反而锁住了旧 phase 状态

`test_operator_truth_source.py` 的存在是好事，但它当前在帮 repo 锁住旧 status constant。

#### 3) build_ui_lite_db end-to-end 契约测试还不够系统化

虽然当前已经有一个失败暴露问题，但建议把它单独升级成更明确的 CI gate。

### 10.3 Finding: 当前 delivery drift 最危险的地方，不在交易逻辑，而在“状态表达 + read-model contract”

- **优先级**：P1
- **类型**：Testing / Delivery / Truth-Source
- **性质**：新发现问题
- **受影响文件**：
  - `surface_truth_shared.py`
  - `ui/app.py`
  - `start_asterion.sh`
  - `read_model_registry.py`
  - `ui_lite_db.py`
  - 文档入口文件

#### 当前代码事实

- UI runtime boundary 与 source truth framework 很强
- 但 phase/version/status 文案仍旧漂移
- read-model contract 已经出现一次真实回归

#### 当前测试事实

- truth-source tests 通过，但锁住旧 phase status
- execution foundation 暴露 read-model regression

#### 当前文档事实

- README / AGENTS / v2 placeholder 已更新
- Post_P4_Remediation 仍是 historical record

#### 我的推断

如果继续开发 v2.0，而不先收紧这些“表达层 / contract 层”的 drift，后面每个 workstream 都会越来越容易出现“代码已经到了，UI/测试/文档还停在前一个阶段”。

#### 推荐修复方向

- 统一 current system status truth-source
- 给关键 read models 增加 stronger catalog/build acceptance
- 加 replay/economic acceptance，阻止“逻辑接了但赚钱能力退化”

---

## 11. Top Risks Ranked by Priority

> 当前没有看到必须立刻中止开发的 **P0 catastrophic issue**。当前 highest priority 是一组 **P1 问题**。

| 优先级 | 风险 | 类型 | 为什么重要 |
|---|---|---|---|
| P1 | ranking 还不是 capital-sized / portfolio-aware 真钱排序 | Trading | 这是当前最直接的利润瓶颈 |
| P1 | execution economics 仍偏 heuristic，尤其在 sparse prior 时 | Trading | 决定 capture 质量和 realized PnL |
| P1 | calibration v2 refresh 仍 manual，freshness 不可运营化 | Trading/Ops | 直接影响高置信定价 |
| P1 | UI / truth-source 仍输出旧 phase/version/status | UX/Truth-Source | 伤害 operator 决策与 roadmap 判断 |
| P1 | `ui.daily_review_input` contract drift 导致 UI-lite build failure | Testing/Architecture | 已形成真实回归 |
| P1 | 当前没有 allocator / size recommendation / capital budget | Scale/Trading | 限制规模化赚钱 |
| P2 | live boundary 仍是同信任域 shared-secret 模型 | Security | 扩大真钱边界前必须升级 |
| P2 | execution prior feature space 不够适合天气类 ephemeral market | Trading/Scale | 限制经验迁移与 sample efficiency |
| P2 | read-model / handler 模块过大 | Architecture/Delivery | 会拖慢 v2 迭代稳定性 |
| P3 | Agents / System 页还可优化，但不是前排 ROI | UX | 不应抢占核心资源 |

---

## 12. What Asterion v2.0 Should Optimize For

如果 v2.0 的目标是“稳定、规模化、高置信赚钱”，那么优化目标应该按下面顺序排：

1. **真实可分配资本后的期望收益质量**
2. **execution capture 与 realized distortion 可学习、可压制**
3. **calibration / threshold probability quality 的稳定 freshness**
4. **operator 在有限时间内处理更多高质量机会的能力**
5. **在不扩大危险 live scope 的前提下，维持边界与运维可靠性**

不应该把 v2.0 的首要目标设成：

- 新域扩张
- UI 大换皮
- agent autonomy
- unrestricted live
- 大规模重写

---

## 13. Asterion v2.0 Core Principles

### Principle 1: Preserve the working skeleton

不要推翻：contracts、canonical order、execution context、read-model registry、truth-source checks、controlled live boundary 的总体结构。

### Principle 2: Make ranking economically honest

v2.0 必须把“看起来有 edge”进一步升级成“值得投入资本的机会”。

### Principle 3: Make feedback operational, not decorative

execution feedback / calibration freshness 不能只存在于表里；必须成为排序、告警、operator decision 的一部分。

### Principle 4: Improve operator throughput, not just UI richness

最重要的不是更多 panel，而是更少误判、更快 triage、更明确 action。

### Principle 5: Keep live boundary narrow until economics justify expansion

在 profit loop 没成熟之前，不要扩大 uncontrolled live scope。

### Principle 6: Use new tables and modules surgically

v2.0 应当是 **extend + split + tighten**，不是 parallel stack rewrite。

---

## 14. Asterion v2.0 Recommended Workstreams

我建议把 v2.0 切成 6 条主 workstreams。

### WS1. Profit Engine v2 — Ranking, Execution Economics, Capture Modeling

**目标**：把当前 ranking 从“真实接线但仍偏 heuristic 的 unit-opportunity score”升级成“更接近可赚钱资本排序”的系统。

**关键改动模块**：

- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `asterion_core/contracts/opportunity.py`
- `asterion_core/runtime/strategy_engine_v3.py`

**建议新增/扩展内容**：

- 扩展 `ExecutionPriorKey` / serving features：
  - `station_id` / `market_template`
  - `time_to_close_bucket`
  - `price_bucket`
  - `edge_bucket`
  - `regime_bucket`
- 扩展 `OpportunityAssessment`：
  - `recommended_size`
  - `expected_trade_pnl_quote`
  - `expected_unit_pnl_quote`
  - `position_limit_status`
  - `execution_model_version`
- 新增 `weather.weather_execution_priors_v2`
- 新增 `runtime.execution_model_materializations`
- 新增 replay comparison pipeline

**验收标准**：

- ranking v3 在 replay windows 上优于当前 ranking v2 baseline
- top-ranked opportunities 的 capture / realized pnl 相关性提升
- operator UI 可直接展示 sized trade economics

### WS2. Portfolio & Capital Allocator

**目标**：补上当前最大的 scale/profitability 缺口：capital-aware sizing。

**关键改动模块**：

- `asterion_core/risk/portfolio_v3.py`（保留 inventory 语义）
- 新增 `asterion_core/risk/allocator_v1.py` 或 `portfolio_allocator_v4.py`
- `asterion_core/execution/execution_gate_v1.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- UI `Home` / `Markets`

**建议新增/扩展内容**：

- 新 contracts：
  - `PortfolioBudget`
  - `SizingDecision`
  - `PositionLimitDecision`
- 新 tables：
  - `risk.portfolio_budgets`
  - `risk.position_limits`
  - `runtime.sizing_decisions`
- gate / router 接收 sizing result，而不是内生 size judgement

**验收标准**：

- 每个 actionable opportunity 都有 recommended size
- 超预算 / 超 concentration 情况下会被明确降级或 block
- UI 不再只显示 unit ranking，而能显示 actual allocation recommendation

### WS3. Forecast Calibration & Probability Quality Operations

**目标**：把 calibration v2 从“已接主链”升级到“真正可长期运营”。

**关键改动模块**：

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/pricing/engine.py`
- `dagster_asterion/job_map.py`
- `dagster_asterion/handlers.py`
- execution / UI freshness surfaces

**建议新增/扩展内容**：

- schedule `weather_forecast_calibration_profiles_v2_refresh`
- 新增 calibration materialization status table
- pricing context 增加 calibration freshness fields
- 中期支持 quantile/ensemble-native inputs

**验收标准**：

- calibration profile refresh 不再依赖手工触发
- stale profile 会在 UI / readiness surface 被显式标出
- threshold quality / bias quality / regime quality 的 freshness 可见

### WS4. Operator Surface v2

**目标**：让 operator 更快区分 `trade_now / review / blocked / research_only`，并且理解大小与风险。

**关键改动模块**：

- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/data_access.py`
- `ui/surface_truth.py`
- `asterion_core/ui/ui_lite_db.py`

**建议新增/扩展内容**：

- 新 read models：
  - `ui.opportunity_decision_queue`
  - `ui.opportunity_cohort_history`
  - `ui.capital_allocation_summary`
  - `ui.materialization_freshness_summary`
- 首页只保留：
  - readiness
  - largest blocker
  - top decision queue
  - execution capture summary
- Markets 页加入：
  - recommended size
  - expected trade pnl
  - cohort history
  - calibration / prior freshness
- Execution 页加入：
  - model drift / distortion attribution
  - worst cohort queue

**验收标准**：

- operator 能在一个页面上快速看见：做不做、为什么、做多大、主要风险是什么
- `source_badge` / `truth_source_status` / phase status 完全一致且不漂移

### WS5. Read Model / Delivery Hardening

**目标**：降低 v2.0 开发过程中的 truth-source drift 与 build fragility。

**关键改动模块**：

- `asterion_core/ui/read_model_registry.py`
- `asterion_core/ui/ui_lite_db.py`
- `ui/data_access.py`
- `dagster_asterion/handlers.py`
- UI / docs tests

**建议新增/扩展内容**：

- 立即修 `ui.daily_review_input.item_id`
- 将 `ui_lite_db.py` 按 builder family 拆分
- 将 `data_access.py` 按 page loader 拆分
- 将 `handlers.py` 按 weather/execution/readiness family 拆分
- 新增 build_ui_lite_db E2E acceptance
- 新增 phase/status consistency tests

**验收标准**：

- 当前 2 个 execution foundation 失败用例修复
- UI lite build 在 CI 中稳定通过
- phase/version/status 只从单一 source 派生

### WS6. Boundary & Deployment Hardening (Conditional)

**目标**：仅在 future live scope 要扩大时推进。

**关键改动模块**：

- `signer_service_v1.py`
- `live_submitter_v1.py`
- `live_boundary.py`
- auth / deployment / secret management

**建议新增/扩展内容**：

- secret resolver abstraction
- KMS/Vault signer option
- attestation issuer abstraction
- operator identity / approval chain
- stronger UI auth / OIDC

**验收标准**：

- 只有在 live scope 计划扩大时才作为 blocker workstream 启动

---

## 15. Asterion v2.0 Phase Breakdown

### Phase 0 — Immediate Fixes

**目标**：先修掉当前真实回归和 truth-source 漂移，保证 v2.0 规划建立在可信现状之上。

**交付物**：

1. 修复 `ui.daily_review_input.item_id`
2. 修复 UI / shell / truth-source shared constants 的 phase/version/status 漂移
3. 把 `truth_source_doc` 从 historical remediation record 切到新的当前状态入口
4. 给 calibration profile materialization 增加 freshness visibility

**不做项**：

- 不做 allocator
- 不做新市场域
- 不做 live scope 扩张

**关键测试**：

- execution foundation 两个失败用例转绿
- new phase/version consistency tests
- UI sidebar/header truth-source acceptance tests

**验收标准**：

- 当前 repo 不再自相矛盾地宣称自己仍在 remediation active
- UI lite build validation 稳定通过

### Phase 1 — Profit Loop Hardening

**目标**：最大化短期 ROI，把当前机会链变得更会赚钱。

**交付物**：

1. execution priors v2 特征升级
2. ranking v3（但字段名仍叫 `ranking_score`）
3. allocator v1 + recommended size
4. expected trade pnl / unit pnl 语义拆分
5. calibration refresh schedule + status surface

**关键测试**：

- replay uplift tests
- sizing / budget tests
- stale calibration / sparse prior penalties

**验收标准**：

- top-ranked opportunities 的 replay quality 比当前基线更好
- operator 可以看到 recommended size 与 expected trade pnl

### Phase 2 — Operator Workflow & Delivery Hardening

**目标**：在不扩大风险边界的前提下提高 operator throughput 与交付稳定性。

**交付物**：

1. decision queue read model
2. cohort history / materialization freshness surfaces
3. 拆分 `ui_lite_db.py` / `ui.data_access.py` / `handlers.py`
4. economic acceptance 在 CI 中落地

**关键测试**：

- read-model golden tests
- page acceptance tests
- truth-source drift tests

**验收标准**：

- operator 可以更快 triage 更多机会
- v2 新功能不会持续引入 truth-source drift

### Phase 3 — Scale Within Weather

**目标**：在 weather 域内部扩大覆盖与样本效率，而不是盲目跨域。

**交付物**：

1. 更强的 station/template/regime priors
2. 更细的 market family coverage
3. replay-based alpha attribution by cohort
4. readiness / freshness / drift observability

**关键测试**：

- cohort generalization tests
- materialization freshness tests
- larger historical replay benchmarks

**验收标准**：

- 样本稀疏问题减轻
- scale 增长主要受 operator 预算和市场本身约束，而不是系统表达能力约束

### Optional Phase 4 — Boundary Upgrade for Larger Live Surface

**目标**：只有在要扩大真钱范围时才启动。

**交付物**：

- KMS/Vault signer
- stronger attestation issuer model
- richer auth / approval

**验收标准**：

- 未来扩大 live scope 时有独立安全依据

---

## 16. Detailed Implementation Plan

### 16.1 Workstream WS1 — Profit Engine v2

#### 目标

把 Asterion 从“有 ranking 的研究系统”升级成“有资本分配语义的盈利排序系统”。

#### 建议模块改动

- `domains/weather/opportunity/service.py`
  - 引入 `execution_model_v2`
  - 让 ranking consumption 接 size / budget aware estimates
- `contracts/opportunity.py`
  - 扩展 assessment 字段
- `execution_priors.py`
  - 新 key 维度
  - materialization status / freshness metadata
- `execution_feedback.py`
  - scope aggregation 从固定权重升级到 sample-aware shrinkage

#### 建议新增表

- `weather.weather_execution_priors_v2`
- `runtime.execution_model_materializations`
- 可选：`runtime.ranking_replay_evaluations`

#### 建议新增作业

- `weather_execution_priors_v2_refresh`（nightly）
- `weather_ranking_replay_eval`（manual or scheduled in CI environment）

#### 关键测试

- `test_ranking_score_v3_capital_aware.py`
- `test_execution_priors_v2_loader.py`
- `test_ranking_replay_eval.py`

#### 验收标准

- replay 上 top decile / top queue 的 capture / realized pnl 指标优于 v2 baseline
- sparse prior fallback 不会虚高排名

### 16.2 Workstream WS2 — Portfolio & Capital Allocator

#### 目标

让 operator 看到的不是“哪个好”，而是“哪个值得分配多少资本”。

#### 建议模块改动

- `portfolio_v3.py` 保持 inventory foundation
- 新增 allocator 模块
- `execution_gate_v1.py` 接收 sizing decision 结果
- `strategy_engine_v3.py` 仍保持 thin orchestration

#### 建议新增 contracts

- `SizingDecision`
- `PortfolioBudget`
- `PositionLimitDecision`

#### 建议新增表

- `risk.portfolio_budgets`
- `risk.position_limits`
- `runtime.sizing_decisions`

#### 关键测试

- `test_portfolio_allocator_v1.py`
- `test_position_limit_enforcement.py`
- `test_strategy_engine_with_sizing.py`

#### 验收标准

- 每个 actionable opportunity 有明确 recommended size
- 排序与 size 一起构成 action decision

### 16.3 Workstream WS3 — Calibration Ops v2

#### 目标

把 calibration v2 从“研究可用”变成“持续可用”。

#### 建议模块改动

- `job_map.py` 把 calibration v2 refresh 加到 schedule map
- `handlers.py` 写 materialization status
- `calibration.py` 输出 profile freshness metadata
- `pricing/engine.py` 将 freshness 写入 pricing context

#### 建议新增表

- `runtime.forecast_calibration_materializations`
- 可选：`ui.calibration_materialization_summary`

#### 关键测试

- `test_calibration_refresh_schedule.py`
- `test_calibration_materialization_status.py`
- `test_stale_calibration_surface.py`

#### 验收标准

- operator 能看到 calibration freshness
- stale calibration 会触发 degraded / warn surface

### 16.4 Workstream WS4 — Operator Surface v2

#### 目标

把当前 console 变成更强的 decision workstation。

#### 建议模块改动

- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/data_access.py`
- `ui/surface_truth.py`
- `surface_truth_shared.py`

#### 建议新增 read models

- `ui.opportunity_decision_queue`
- `ui.opportunity_cohort_history`
- `ui.capital_allocation_summary`
- `ui.materialization_freshness_summary`

#### 关键测试

- `test_operator_decision_queue.py`
- `test_home_shows_recommended_size.py`
- `test_markets_shows_cohort_history.py`
- `test_phase_status_consistency.py`

#### 验收标准

- `Home` / `Markets` 只用一个 truth-source 表达当前 phase
- operator 可以从 UI 直接分辨：做不做、做多大、主要风险是什么

### 16.5 Workstream WS5 — Delivery Hardening

#### 目标

让 v2.0 开发不会被 truth-source drift 和 read-model 回归拖垮。

#### 建议模块改动

- 拆分 `ui_lite_db.py`
- 拆分 `ui/data_access.py`
- 拆分 `handlers.py`
- 修 `daily_review_input.item_id`

#### 建议新增测试

- `test_ui_lite_db_contract_e2e.py`
- `test_read_model_registry_sync.py`
- `test_current_status_string_sync.py`

#### 验收标准

- 新增 read model 不再轻易破坏 build validation
- docs / UI / tests 的 current status 一致

### 16.6 Workstream WS6 — Boundary & Deployment Hardening

#### 目标

为将来可能扩大的真钱边界预留升级路径。

#### 建议模块改动

- signer secret resolver abstraction
- submitter attestation issuer abstraction
- stronger auth

#### 验收标准

- 只有当 live scope 计划扩大时才推进，不抢 WS1/WS2 的优先级

---

## 17. Testing and Acceptance Strategy

v2.0 最需要的，不是更多 unit test，而是**更贴近赚钱主链的 acceptance**。

### 17.1 必须新增的测试层次

#### A. Economic Replay Acceptance

任何 ranking / execution prior / calibration 变更，都应该跑 replay 并产出：

- baseline vs candidate 的 top-queue comparison
- capture uplift
- realized pnl proxy uplift
- distortion reduction

#### B. Capital Allocation Acceptance

新增 allocator 后，必须验证：

- 高 ranking 但高 concentration 的机会不会过分拿到 size
- 低容量机会不会因为 unit score 高而过分排前

#### C. Truth-Source Acceptance

必须阻止以下回归再次发生：

- UI phase/status/version 与 README/AGENTS 不一致
- read-model registry 与 builder 不一致
- `truth_source_doc` 指向 historical file 但 UI 把它当 current state

#### D. Boundary Acceptance

保持现有 submitter / signer / controlled live 测试，并在未来 boundary 升级时继续扩展。

### 17.2 推荐的 CI Gate

建议把 v2.0 关键 CI 分成 5 组：

1. `boundary_and_signer`
2. `ranking_calibration_feedback`
3. `ui_truth_source`
4. `ui_lite_contract`
5. `economic_replay_acceptance`

其中第 5 组应该成为任何 ranking v3 / priors v2 / calibration ops 改动的强 gate。

---

## 18. Top 10 Highest-ROI Improvements

1. **修复 `ui.daily_review_input.item_id` contract drift**
2. **统一 current phase/version/status 的单一 truth-source**
3. **将 calibration v2 refresh 改为 scheduled，并显式显示 freshness**
4. **引入 allocator v1：recommended size / budget / limits**
5. **把 `expected_dollar_pnl` 语义拆成 unit pnl 与 sized trade pnl**
6. **将 execution prior key 扩展到 station/template/time-to-close/price/edge/regime**
7. **引入 replay-based economic acceptance，阻止 ranking 改动无约束上线**
8. **把 Markets / Home 升级为 decision queue + cohort history surface**
9. **拆分 `ui_lite_db.py` / `ui.data_access.py` / `handlers.py`，降低 delivery risk**
10. **仅在计划扩大真钱边界时，再做 KMS / attestation issuer / stronger auth**

---

## 19. What Not To Prioritize Yet

以下事情不是不重要，而是**短期不应抢优先级**：

1. **unattended live / unrestricted live**
2. **更多 tx kinds / 更复杂链上 side effects**
3. **多资产 / 多领域 prediction market 扩张**
4. **把 agent 拉进 execution path**
5. **大规模 UI 视觉翻新**
6. **微服务重写 / 全栈重构**
7. **RL ranking / autonomous capital optimizer 这类“表面高级”的项目**
8. **多租户 / public operator console**

这些事情里，很多以后都可以做；但当前 ROI 远低于：

- execution economics
- sizing / allocator
- calibration freshness
- truth-source hardening
- economic acceptance

---

## 20. Appendix: Files Reviewed

### 文档入口

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Asterion_Project_Plan.md`
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/phase-plans/V2_Implementation_Plan.md`
- `docs/10-implementation/phase-plans/Post_P4_Remediation_Implementation_Plan.md`

### 设计文档

- `docs/30-trading/Controlled_Live_Boundary_Design.md`
- `docs/30-trading/Execution_Economics_Design.md`
- `docs/40-weather/Forecast_Calibration_v2_Design.md`
- `docs/50-operations/Operator_Console_Truth_Source_Design.md`
- `docs/20-architecture/UI_Read_Model_Design.md`

### 参考但未当成当前事实的 analysis 文档

- `docs/analysis/01_Current_Code_Reassessment.md`
- `docs/analysis/02_Current_Deep_Audit_and_Improvement_Plan.md`
- `docs/analysis/11_Project_Full_Assessment.md`
- `docs/analysis/13_UI_Redesign_Assessment.md`

### 核心代码

#### execution / boundary / constrained live

- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/contracts/live_boundary.py`
- `asterion_core/execution/execution_gate_v1.py`
- `asterion_core/execution/order_router_v1.py`
- `asterion_core/risk/portfolio_v3.py`

#### opportunity / ranking / pricing / alpha chain

- `asterion_core/contracts/opportunity.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/pricing/engine.py`

#### forecast / calibration / probability quality

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/forecast/service.py`
- `domains/weather/forecast/replay.py`

#### monitoring / readiness / orchestration

- `asterion_core/monitoring/health_monitor_v1.py`
- `asterion_core/monitoring/readiness_checker_v1.py`
- `asterion_core/monitoring/capability_manifest_v1.py`
- `dagster_asterion/handlers.py`
- `dagster_asterion/job_map.py`
- `start_asterion.sh`

#### UI / operator surface / truth-source

- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/ui/read_model_registry.py`
- `asterion_core/ui/surface_truth_shared.py`
- `ui/data_access.py`
- `ui/surface_truth.py`
- `ui/app.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/pages/system.py`
- `ui/pages/agents.py`
- `ui/runtime_env.py`

### 重点复核并运行的测试

- `tests/test_p4_closeout.py`
- `tests/test_p4_plan_docs.py`
- `tests/test_execution_foundation.py`
- `tests/test_live_submitter_backend.py`
- `tests/test_submitter_boundary_attestation.py`
- `tests/test_signer_shell.py`
- `tests/test_execution_priors_materialization.py`
- `tests/test_ranking_score_v2.py`
- `tests/test_execution_feedback_loop.py`
- `tests/test_forecast_calibration.py`
- `tests/test_calibration_profile_v2.py`
- `tests/test_threshold_probability_profile.py`
- `tests/test_weather_pricing.py`
- `tests/test_ui_data_access.py`（选定子集）
- `tests/test_ui_pages.py`
- `tests/test_operator_truth_source.py`
- `tests/test_ui_source_badges.py`
- `tests/test_ui_read_model_catalog.py`
- `tests/test_truth_source_checks.py`
- `tests/test_ui_runtime_env.py`
- `tests/test_controlled_live_capability_manifest.py`
- `tests/test_live_prereq_readiness.py`
- `tests/test_controlled_live_smoke.py`

---

## Final Recommendation

如果 Asterion 要认真进入 `v2.0`，最正确的路线不是“重写成全自动 production stack”，而是：

1. **先把当前真实回归和 truth-source 漂移修平**
2. **立刻把 execution economics / sizing / calibration freshness 做强**
3. **把 operator surface 升级成真正的 capital decision queue**
4. **在 weather 域内先做规模化与经验迁移**
5. **只有在 profit loop 变强之后，才考虑扩大真钱边界与部署硬化**

用一句最实在的话收尾：

> **Asterion 现在最需要的不是更多“能不能交易”的能力，而是更强的“哪些机会值得投入多少资本、并且为什么”的能力。**
