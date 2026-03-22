# Asterion Current Deep Audit and Improvement Plan

> Analysis input only.
> Not implementation truth-source.
> Active implementation entry: `docs/10-implementation/versions/v2.0/phase-plans/V2_Implementation_Plan.md`

**状态**: current analysis input (`2026-03-17`)

这份报告基于**当前真实代码**与我实际复核/跑过的定向测试来写，不是复述 README，也不是沿用旧 assessment。
我重点看了你指定的入口文件，并实际跑了 submitter / boundary attestation、controlled-live chain-tx、UI env、ranking penalty、predicted-vs-realized、watch-only-vs-executed、execution science、wording baseline 等测试组。

---

## 1. Executive Summary

Asterion 当前已经不是“只有概念没有实现”的状态了。它已经具备一条**真实存在的 constrained live 基础链**：

- `real_clob_submit` 已经是真实 backend，不再只是 shadow 占位。
- `SubmitterServiceShell` 已经做 live guard，并把 submitter boundary attestation 持久化。
- `ChainTxServiceShell` 的 controlled-live 路径已经做到了 `approve_usdc only`、wallet/spender/amount allowlist、arming、approval token、wallet readiness、payload 脱敏。
- UI 的标准启动路径不再默认继承 full `.env`。
- `ranking_score` 确实进入了 runtime 主排序，不只是 UI 注释。
- `predicted_vs_realized`、`watch_only_vs_executed`、`execution_science` 都是真正的读模型，不再是 README 幻影。

但结论也很明确：

**安全上**，当前 constrained real submitter 已经达到了“适合 manual-only / internal controlled live”的谨慎水平，**还没有达到高保证、不可绕过的 canonical gate 水平**。最薄弱的一环不是 chain-tx，而是：

1. **submitter attestation 的信任模型仍是进程内逻辑 attestation，不是强闭环 capability proof**
2. **signer 仍信任 caller 提供的 `private_key_env_var` 名称**

**UX 上**，控制台方向是对的，已经明显从“工程控制台”转向“operator decision surface”。但它还存在两个会真实影响操作判断的问题：

1. **分数语义混杂**：`opportunity_score` / `ranking_score` / `expected_value_score` / `expected_pnl_score` / `confidence_score` 混在一起，主次不够清晰。
2. **truth-source 仍有漂移**：sidebar 的 boundary 还是硬编码，Home 里还有 “executed-only” 的旧措辞，degraded/fallback source 虽有告警，但还不够行级显式。

**交易能力上**，当前系统已经具备“挑出一部分值得人工出手的机会”的能力，但**还不具备“可稳定、规模化、高置信赚钱”的能力**。
最强链条是：

`forecast -> fair value -> executable edge -> calibration/freshness/mapping/market-quality penalty -> ranking_score -> operator review -> post-trade analytics`

最弱链条是：

`execution economics empirical model -> realized feedback loop back into ranking`

换句话说，当前瓶颈**不是 plumbing**，而是：

- execution model 还太 heuristic
- calibration/uncertainty 还太粗
- execution science 还只是 descriptive，不是 feedback loop
- ranking 还是 pseudo-score，不是资本/风险/美元 EV 意义上的 production ranking

一句话判断：

**Asterion 现在是一个“研究级、运营员驱动、受约束 live 基础设施 + 初版盈利引擎”，不是一个“高质量闭环盈利机器”。**

---

## 2. What Is Already Strong

下面这些点，我认为应该明确算作“已经做对了”，而不是继续沿用旧批评。

- **旧问题：real submitter 不存在。当前事实：已修复。**
  `asterion_core/execution/live_submitter_v1.py` 里已经有 `RealClobSubmitterBackend`；`tests/test_live_submitter_backend.py` 也实际覆盖了 live submit 行为。

- **旧问题：submitter 边界只在 handler。当前事实：已明显改善。**
  `SubmitterServiceShell.submit_order()` 在 `live_submit` 分支里已经做 `validate_live_side_effect_guard(...)`，并持久化 `runtime.live_boundary_attestations`。这比旧版本强很多。

- **旧问题：UI 继承 full env。当前事实：标准启动路径已基本修复。**
  `start_asterion.sh:59-79` 会先清掉 `ASTERION_* / QWEN_* / OPENAI_API_KEY / ALIBABA_API_KEY`，再通过 `ui/runtime_env.py` 只注入 allowlisted key。`tests/test_ui_runtime_env.py` 明确验证了 approval token / live secret 不会进 UI env。

- **旧问题：env private key 会被拿去做 order signing。当前事实：已修复。**
  `EnvPrivateKeyTransactionSignerBackend.sign_order()` 明确返回 `env_private_key_tx_order_signing_disabled`。

- **旧问题：chain-tx 会把 raw tx / key ref 落库。当前事实：已修复。**
  `chain_tx_v1.py:781-795` 会 scrub `raw_transaction_hex` / `private_key_env_var`；`tests/test_controlled_live_smoke.py` 明确验证这些字段不会进入 `runtime.chain_tx_attempts`。

- **旧问题：ranking/calibration 没进主链。当前事实：已修复到“真实接线”的程度。**
  `domains/weather/opportunity/service.py:101-118` 把 calibration/freshness/mapping/market-quality multiplier 合进 `uncertainty_multiplier`，`strategy_engine_v3.py:281-286` 也确实优先用 `ranking_score`。`tests/test_execution_foundation.py:279-319` 证明 runtime 排序会服从 `ranking_score`。

- **旧问题：post-trade analytics 只是文档。当前事实：已真实落地。**
  `ui_lite_db.py` 已经有 `predicted_vs_realized_summary`、`watch_only_vs_executed_summary`、`execution_science_summary`。
  更关键的是：`tests/test_predicted_vs_realized_summary.py` 证明它**已经不是 executed-only / fill-only**，现在会纳入 gate rejected、sign rejected、submit rejected、working/unfilled 等生命周期。

复杂但合理的设计，我也想明确说一句：

- **`ui_lite_db` 读模型层是正确方向。**
  问题不在“要不要有 projection/read model”，而在于现在文件太大、truth-source 还没彻底收口。

---

## 3. Security / Boundary Findings

### 先给直接答案

- **constrained real submitter 现在是否足够安全？**
  对于**manual-only、内部可信环境下的 constrained live**，已经足够谨慎；对于**高保证 canonical gate**，还不够。

- **attestation 是否足够强？**
  **不够。**它足够做 audit，不足够做 tamper-resistant boundary proof。

- **是否还存在绕过 canonical gate 的风险？**
  **存在 residual risk。**主要是内部代码路径/直接 backend 调用层面的绕过，不是普通 UI 用户一键绕过。

- **UI / web secret minimization 是否真正彻底？**
  **标准启动路径下基本到位，但系统整体还不是绝对彻底。**

- **当前 live boundary 最薄弱的一环是什么？**
  **submitter attestation trust model，其次是 signer 对 `private_key_env_var` 的信任。**

---

### 3.1 `real_clob_submit` 的 attestation 仍然不是强闭环 capability proof

**优先级**：P1
**类型**：Security
**性质**：旧问题残留，但已比旧版本明显收口
**受影响文件**：
`asterion_core/contracts/live_boundary.py`
`asterion_core/execution/live_submitter_v1.py`
`dagster_asterion/handlers.py`
`sql/migrations/0017_runtime_live_boundary_attestations.sql`
`tests/test_live_submitter_backend.py`
`tests/test_submitter_boundary_attestation.py`

**代码事实**：

- `evaluate_submitter_boundary()` 会检查 manifest status、manual_only、wallet allowlist、submitter/signer/chain-tx backend kind、armed、approval token match、wallet readiness、readiness GO 等条件（`live_boundary.py:118-181`）。
- `SubmitterServiceShell.submit_order()` 在 `live_submit` 分支里会先跑 `validate_live_side_effect_guard(...)`，再构建并落库 attestation（`live_submitter_v1.py:467-510`）。
- `RealClobSubmitterBackend.submit()` 会拒绝没有 attestation、未 approved、request/wallet/mode/backend/fingerprint 不匹配的请求（`live_submitter_v1.py:329-372`）。
- 但 attestation 本身只是 `build_submitter_boundary_attestation(...)` 构造出来的 dataclass；**没有签名、没有 TTL、没有 nonce、没有 consumed-once 语义**（`live_boundary.py:66-115`）。
- `evaluate_submitter_boundary()` 对 fingerprint 的检查只是“非空”，不是“和真实 endpoint 精确匹配”；精确匹配放在 backend 内（`live_boundary.py:150-151` vs `live_submitter_v1.py:365-372`）。

**测试事实**：

- `tests/test_submitter_boundary_attestation.py` 证明 boundary 条件判断本身是存在且覆盖了主要 blocker 的。
- `tests/test_live_submitter_backend.py:51-54, 294-305` 直接构造 `_approved_attestation(...)` 并传给 backend，backend 会接受。
  这个测试本意是测 backend，但它也同时说明：**backend 信任的是 caller 提供的 approved attestation，而不是 backend 自己重算 boundary。**

**文档事实**：

- README / AGENTS / Phase 9 wording 已经明确系统是 `operator console + constrained execution infra`，不是 unattended live。这个定位是对的。
- 但这些文档没有把“attestation 目前是 audit-strength，不是 cryptographic-strength”说清楚。

**我的推断**：

- canonical handler + shell 路径目前已经相当谨慎；
- 但如果未来多一个 job、脚本、内部服务直接 new 一个 backend，再手工构造 approved attestation，这条边界不是天然封死的。

**风险或问题描述**：

当前 boundary 是“**约定式强约束**”，不是“**能力对象级别不可伪造约束**”。

**为什么重要**：

这是 live side-effect 最核心的 trust boundary。
一旦系统继续演进，多一个内部调度路径，就会把“现在靠规范和调用顺序成立的安全”变成“以后可被内部误用/错用/旁路”的安全。

**如果不修会导致什么**：

- submitter 的 canonical gate 仍有内部绕过面；
- audit 表看起来是“有 attestation 才 live submit”，但 attestation 本身不是强来源证明；
- 将来做 service 化或多作业编排时，边界很容易变脆。

**具体修复方案**：

1. 把 attestation 升级成**可验证、短时效、一次性**的 boundary token。
   至少加入：
   - `issuer`
   - `expires_at`
   - `nonce`
   - `decision_fingerprint`
   - `attestation_mac` 或签名

2. `decision_fingerprint` 必须覆盖这些字段：
   - request_id / wallet_id / source_attempt_id / ticket_id / execution_context_id
   - submit_mode
   - submitter_backend_kind / signer_backend_kind / chain_tx_backend_kind
   - submitter_endpoint_fingerprint
   - manifest_hash
   - readiness_hash
   - wallet_readiness_status
   - approval_token_matches
   - armed

3. backend 不再接受“任意 approved dataclass”，而是只接受：
   - shell 用 runtime-only secret/HMAC mint 出来的 attestation；或
   - 更进一步，接受一个 `CapabilityScopedLiveSubmitRequest`，backend 内部只验证 capability，不直接相信 caller 自填 attestation。

4. 在 attestation 层就做**精确 fingerprint 检查**，不要只检查非空。

5. 增加 attestation consume 记录，避免重复使用同一 boundary token。

**需要改哪些模块**：

- `asterion_core/contracts/live_boundary.py`
- `asterion_core/execution/live_submitter_v1.py`
- `dagster_asterion/handlers.py`
- migration for `runtime.live_boundary_attestations`

**需要加哪些测试**：

- forged approved attestation 被 backend 拒绝
- expired attestation 被拒绝
- nonce reused 被拒绝
- fingerprint wrong / manifest hash wrong / readiness hash wrong 被拒绝
- non-shell minted attestation 被拒绝

**是否需要文档同步**：需要
**是否需要 migration**：需要
**推荐实施顺序**：1

---

### 3.2 signer 仍信任 caller 提供的 `private_key_env_var`

**优先级**：P1
**类型**：Security
**性质**：新发现的核心残余风险
**受影响文件**：
`asterion_core/signer/signer_service_v1.py`
`dagster_asterion/handlers.py`
`asterion_core/blockchain/chain_tx_v1.py`

**代码事实**：

- `EnvPrivateKeyTransactionSignerBackend.sign_transaction()` 直接从 `request.payload["private_key_env_var"]` 取 env var 名称，再 `os.getenv(...)` 读私钥（`signer_service_v1.py:356-421`）。
- 它会校验私钥导出的地址是否等于 `from`（`379-388`），这是好的。
- 但它**不验证这个 env var 名称是否和 wallet policy / wallet_id / manifest 相匹配**。
- 当前 canonical handler 确实是安全地通过 `controlled_live_wallet_secret_env_var(wallet_id)` 来推导 env var 名称（`handlers.py:1148-1206`），不是用户输入直传。

**测试事实**：

- `tests/test_signer_shell.py` 证明 `env_private_key_tx` 不会被用于 order signing，这是加分项。
- 但我没有看到一个 regression test 去证明：**payload 里塞一个别的 env var 名称时，backend 会拒绝**。当前代码也确实不会拒绝。

**文档事实**：

- 文档整体把 controlled-live 私钥边界描述为 wallet allowlisted / secret env segregated，这在 canonical handler 上是成立的。
- 但 signer backend 自身没有把这个边界内生化。

**我的推断**：

- 这和 submitter attestation 问题一样，本质上是“canonical path 很谨慎，但 backend 自己还在相信 caller”。

**风险或问题描述**：

只要内部某段代码能调用 signer backend，并构造一个匹配地址的 `private_key_env_var`，它就可能把 signer 指到不该用的 secret。

**为什么重要**：

这是 secret boundary，不是普通逻辑 bug。

**如果不修会导致什么**：

- wallet 与 secret 绑定关系靠调用方 discipline 维持；
- 系统一旦多入口，secret 使用边界会变脆。

**具体修复方案**：

1. `sign_transaction()` 不再接受 payload 提供的 `private_key_env_var`。
2. 在 signer request/context 里显式带 `wallet_id`。
3. signer backend 内部通过 `controlled_live_wallet_secret_env_var(wallet_id)` 自己推导 env var。
4. 可选地再对照 manifest / wallet policy 做二次校验。
5. scrub 层继续保留，确保任何情况下都不把 env var 名称或 raw tx 回写。

**需要改哪些模块**：

- `asterion_core/signer/signer_service_v1.py`
- `dagster_asterion/handlers.py`
- 可能少量改 `chain_tx_v1.py` / signer request context builder

**需要加哪些测试**：

- payload 自带 `private_key_env_var` 时 backend 忽略或拒绝
- wallet_id -> env var 推导正确
- wallet_id 与 from 地址不匹配时拒绝
- handler 不再把 `private_key_env_var` 注入 payload

**是否需要文档同步**：需要
**是否需要 migration**：不需要
**推荐实施顺序**：2

---

### 3.3 UI secret minimization 已大体修好，但仍是“内部可信部署级别”，不是公开暴露级别

**优先级**：P2
**类型**：Security
**性质**：旧问题大体修复后的残余风险
**受影响文件**：
`start_asterion.sh`
`ui/runtime_env.py`
`ui/auth.py`
`ui/pages/agents.py`
`ui/data_access.py`
`ui/app.py`

**代码事实**：

- `start_asterion.sh:59-79` 会清掉敏感 env，再注入 allowlisted UI env。
- `ui/runtime_env.py:12-24, 51-61` 只允许极少数 exact keys/prefix 进入 UI env。
- `ui/auth.py:19-60` 是 default-deny，这一点是对的。
- 但 auth 仍然是**简单 SHA-256 hash 比对 + session_state**，无 rate limiting、无 lockout、无 session TTL、无 2FA（`ui/auth.py:25-31, 40-59`）。
- `start_asterion.sh:183` 让 Streamlit 监听 `0.0.0.0`。
- `ui/pages/agents.py:118-127` 仍然展示 `QWEN_API_KEY` / `ALIBABA_API_KEY` 是否配置。
- `ui/data_access.py:1264-1286` 仍然会探测 `ASTERION_OPENAI_COMPATIBLE_API_KEY` / `ALIBABA_API_KEY` / `QWEN_API_KEY` 的存在性。

**测试事实**：

- `tests/test_ui_runtime_env.py` 很有价值，明确证明 approval token / controlled live secret 不会进入标准 UI env。
- 但没有 operator-facing test 去阻止 agents page 继续做 secret-adjacent probing。

**文档事实**：

- README / AGENTS 把 UI 描述为 minimal read-only runtime env，这在标准启动路径上基本成立。
- 但“最小化 env”不等于“公开网络安全”。

**我的推断**：

- 如果 UI 只在内网/VPN/跳板机后面，这个水平是能接受的。
- 如果有人把它直接暴露出去，这套 auth 不够。

**风险或问题描述**：

- 标准 path 很安全；
- 直接 `streamlit run ui/app.py` 跳过 wrapper 时，env scrub 不再自动成立；
- Agents 页仍然在做 secret-adjacent runtime introspection。

**为什么重要**：

这是 operator console，不该给运维/交易员传递“secret presence”这种低价值高敏感的信息。

**如果不修会导致什么**：

- 误把内部工具当作对公网可暴露工具；
- secret minimization 的理念在 UI 细节层继续被侵蚀。

**具体修复方案**：

1. UI 进程启动时增加**敏感 env 泄露检测**。
   一旦发现 `ASTERION_CONTROLLED_LIVE_SECRET_*` / `*_API_KEY` 这类 banned key 出现在 UI 进程环境中，直接显示 fatal banner 并拒绝渲染主控制台。

2. 删除 Agents 页 “Runtime Configuration” 中所有 key-presence 信息。
   只保留：
   - provider/model 名义配置
   - agent pipeline health
   - queue / failure buckets
   - review age / latest exception

3. 把 `0.0.0.0` 变成显式 opt-in。默认只监听 localhost，除非设置 `ASTERION_UI_LISTEN_PUBLIC=true`。

4. 中期把 UI auth 升级为反向代理/OIDC/RBAC；当前内置 auth 只保留作开发/内网 fallback。

**需要改哪些模块**：

- `start_asterion.sh`
- `ui/app.py`
- `ui/auth.py`
- `ui/pages/agents.py`
- `ui/data_access.py`

**需要加哪些测试**：

- banned env present -> UI refuses
- agents page 不再出现 key presence
- public bind requires explicit opt-in

**是否需要文档同步**：需要
**是否需要 migration**：不需要
**推荐实施顺序**：3

---

## 4. UX / Operator Console Findings

### 4.1 当前 UI 最大的 operator 风险，不是“太丑”，而是“主次信息还不够硬”

**优先级**：P1
**类型**：UX
**性质**：新发现问题
**受影响文件**：
`ui/app.py`
`ui/pages/home.py`
`ui/pages/markets.py`
`ui/pages/execution.py`
`ui/data_access.py`

**代码事实**：

- `Markets` 页同时展示 `opportunity_score`、`ranking_score`、`expected_value_score`、`expected_pnl_score`、`confidence_score`、`liquidity_proxy`（`markets.py:191-236, 314-357`）。
- `Home` 页的 top metric 叫 `Top Opportunity Score`，但底层会优先取 `ranking_score`，否则 fallback 到 `opportunity_score`（`data_access.py:1663`）。
- `Markets` 在 `opportunities` 为空时会 fallback 到 `market_rows` 构表（`markets.py:52-89`）；这意味着 operator 看到的表，有时是 canonical opportunity rows，有时是 fallback/derived rows。
- `load_operator_surface_status()` 已经能识别 `degraded_source`（`data_access.py:1371-1482`），这很好。
- 但 `ui/app.py` sidebar 里的 boundary 文案仍然是**硬编码**（`app.py:251-257`），不是 manifest/readiness truth-source。

**测试事实**：

- `tests/test_phase9_wording.py` 证明 UI wording 基线是被维护的。
- 但没有测试去保证：
  - sidebar 显示的是**当前真实 boundary**；
  - fallback rows 在 UI 中会被足够显式地区分；
  - operator 不会把 `opportunity_score` 和 `ranking_score` 混读。

**文档事实**：

- 文档明确说当前阶段是 truth-source cleanup / operator console。
- 现在最主要的 truth-source 漂移，不在 README，而在 UI 细节。

**我的推断**：

现在页面“看起来像一个决策台”，但还没有做到“看错了也不容易误判”。

**风险或问题描述**：

- operator 容易把 component score 当成最终排序依据；
- 容易忽略 degraded source 与 canonical source 的区别；
- 容易把 sidebar 的“constrained real submit”理解成“当前环境真实启用”。

**为什么重要**：

这是直接影响手工交易/审查判断的 UX 问题，不是 cosmetic。

**如果不修会导致什么**：

- 会把“研究辅助分数”误当“执行优先级”；
- 会把“配置能力”误当“当前启用状态”。

**具体修复方案**：

1. 明确单一主分数：
   全 UI 只把 `ranking_score` 作为主排序分数。
   其它分数统一改名为“components / diagnostics”。

2. 在 Top Opportunity 与 Opportunity Table 加一个固定的“Why Ranked Here”分解框：
   - executable edge
   - fill probability
   - uncertainty multiplier
   - market quality status
   - ops gate / ops bonus
   - source type

3. 所有 fallback/degraded row 必须加**行级 source badge**，不是只在页头 warning。
   例如：`canonical_ui_lite` / `derived_from_smoke_report` / `runtime_db_fallback`

4. sidebar 从 `load_readiness_evidence_bundle()` / `load_readiness_summary()` 动态渲染：
   - manual_only
   - default_off
   - approve_usdc_only
   - constrained_real_submit_enabled
   - manifest_status
   - readiness GO/NO-GO
   不再写死。

**需要改哪些模块**：

- `ui/app.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/data_access.py`

**需要加哪些测试**：

- dynamic sidebar truth-source test
- degraded source badge acceptance test
- primary score label consistency test

**是否需要文档同步**：需要
**是否需要 migration**：不需要
**推荐实施顺序**：4

---

### 4.2 Agents / System 两页仍然有明显“工程控制台味道”

**优先级**：P2
**类型**：UX
**性质**：新发现问题
**受影响文件**：
`ui/pages/agents.py`
`ui/pages/system.py`

**代码事实**：

- `Agents` 页虽然主叙事已经是 exception review，但底部仍有 `Runtime Configuration`，并展示 key presence（`agents.py:114-127`）。
- `System` 页对 operator 有价值的部分其实是：
  - readiness decision
  - blockers/warnings
  - capability boundary
  - dependency freshness
  但页面里仍有大量 path/component/dataframe，偏工程调试（`system.py` 全页）。

**测试事实**：

- wording baseline 只保证它们不再叫 `Agent Workbench`，并强调 exception review/readiness evidence。
- 没有测试去约束“这些页面是否围绕 operator decision，而不是 file-path console”。

**文档事实**：

- 当前定位就是 operator console，不是 engineering admin pane。

**我的推断**：

页面方向是对的，但信息密度仍偏“工程师自己看着舒服”，不够“操作员做决定舒服”。

**风险或问题描述**：

- Agents 页把 operator 视线从 review queue 拉到 runtime config；
- System 页让 operator 在太多“存在性/路径性”信息里找 decision signal。

**为什么重要**：

高质量 operator surface 的核心不是信息更多，而是**错误更少、认知成本更低**。

**如果不修会导致什么**：

- 决策速度慢；
- 误把工程调试信息当作操作证据；
- 真正的 blockers 不够突出。

**具体修复方案**：

- `Agents`：
  - 保留 human review queue、latest exceptions、agent by type
  - 删掉 runtime config
  - 增加 review age、affected market count、failure bucket、latest unresolved exception

- `System`：
  - 顶部只保留三类 operator 问题：
    1. can I trust current market ranking?
    2. can I safely run controlled live approve?
    3. what is the single largest blocker?
  - 其它 file-path / component tables 下沉到 “Engineering Diagnostics” 折叠区

**需要改哪些模块**：

- `ui/pages/agents.py`
- `ui/pages/system.py`
- 少量 `ui/data_access.py`

**需要加哪些测试**：

- agents page no secret-adjacent runtime config test
- system page operator-first summary test

**是否需要文档同步**：需要
**是否需要 migration**：不需要
**推荐实施顺序**：5

---

## 5. Trading / Profitability Findings

### 5.1 直接回答：当前系统“赚钱能力”到底如何？

**代码无法证明它已经稳定赚钱。**
我没有在这批当前入口代码与测试里看到可作为“持续净盈利已验证”的客观证据。
所以我不会说“它已经能稳定赚钱”。

但从结构能力上看，我会这样判断：

- **已经足以支撑赚钱的部分**
  在 `source fresh + mapping confidence 高 + market_quality=pass + order book 可用 + edge 足够大 + operator 选择性执行` 的样本上，系统已经具备比较像样的**人工筛选盈利能力**。

- **最强的盈利链条**
  `forecast/fair value -> executable edge -> uncertainty penalties -> ranking_score -> operator review -> post-trade analysis`

- **最弱的盈利链条**
  `execution economics empirical fit -> realized feedback back into ranking`

- **最影响赚钱的缺口**
  不是 UI，也不是 shell plumbing。
  真正最影响赚钱的是：
  1. fill/slippage/depth/liquidity 仍是 heuristic
  2. calibration/uncertainty 仍然粗
  3. execution science 还没回灌主排序
  4. ranking 不是 dollar EV / capital-aware ranking

### 5.2 ranking / calibration / executable edge / execution science 是否足以支撑高质量盈利？

我的结论是：

- **ranking**：已经是“真实接线”，但还不是“高质量经济排序”
- **calibration**：已经进入主链，但还只是 bucketed residual penalty
- **executable edge**：方向正确，但数字质量不足
- **execution science**：已经存在，而且比旧评估说的更强，但仍是 descriptive first pass

所以整体答案是：

**足以支撑“研究级、运营员驱动的 constrained trading”，不足以支撑“高质量、可扩张的盈利系统”。**

### 5.3 market quality / source freshness / mapping confidence 的降权是否合理？

**方向上合理，数值上仍然粗。**

`service.py:263-329` 的阈值设计是有交易常识的：

- mapping confidence `<0.35` 直接 blocked，`<0.75` review
- staleness `>=15min` review，`>=1h` blocked
- spread `>=100bps` review，`>=200bps` blocked
- depth proxy 很低时 review

这比“完全不惩罚质量问题”强得多。
但这些阈值和 multiplier 目前还是人为设定，不是从 capture / fill / pnl distortion 数据里学出来的。

---

### 5.4 当前最该优先增强的 5 个盈利能力动作

1. **把 fill/slippage/depth/liquidity 从 heuristic 改成 empirical execution priors**
2. **把 ranking 从 pseudo-score 改成 dollar EV / capital-efficiency / risk-aware ranking**
3. **把 execution science 的 cohort capture/miss/distortion 回灌主排序**
4. **把 calibration 从 coarse Gaussian sigma 升级到 bias-corrected / conformal uncertainty**
5. **把 operator surface 做成“减少 alpha leakage”的界面，而不是“展示更多数据”的界面**

---

### 5.5 Execution economics 仍是当前最大的盈利瓶颈

**优先级**：P1
**类型**：Trading
**性质**：新发现问题
**受影响文件**：
`domains/weather/opportunity/service.py`
`asterion_core/runtime/strategy_engine_v3.py`
`ui/pages/markets.py`
`tests/test_phase7_ranking_penalty.py`
`tests/test_execution_foundation.py`

**代码事实**：

- `slippage_bps` 目前是固定 40/80（`service.py:390-395`）
- `liquidity_penalty_bps` 目前是固定 25/60/999999（`398-403`）
- `fill_probability` 目前是固定 0.25 / 0.50 / 0.75 / 0.60（`406-415`）
- `depth_proxy` 目前是固定 0.85 / 0.55 / 0.25（`418-423`）
- `ops_readiness_score` 目前是固定 20 / 10 / 0，并且直接加进 `ranking_score`（`426-431`，`118`）

**测试事实**：

- `tests/test_phase7_ranking_penalty.py` 证明 penalty 确实会压低 `ranking_score`。
- `tests/test_execution_foundation.py` 证明 runtime 确实按 `ranking_score` 排。
  这说明“接线是真的”；但没有测试能证明“数值是对的”。

**文档事实**：

- 文档把 executable edge / expected PnL ranking 视为 accepted capability。这个说法作为“机制存在”是成立的。
- 但文档没有充分强调当前 execution economics 仍是 heuristic baseline。

**我的推断**：

现在最大的 ranking distortion，不是代码 bug，而是**经济模型太粗**。

**风险或问题描述**：

- 好机会可能因为 fill/slippage 假设不准被压下去；
- 坏机会可能因为 ops bonus 或过乐观 fill probability 被抬上来。

**为什么重要**：

这正是“会不会赚钱”的核心，不是次要优化项。

**如果不修会导致什么**：

- alpha leakage 持续存在；
- operator 看到的是“接起来的研究链”，不是“高质量可赚钱链”。

**具体修复方案**：

1. 做一层 `execution_priors_v1`：
   - 维度先从简单开始：market / station / source / best_side / price bucket / spread bucket / freshness bucket
   - 输出：
     - empirical submission probability
     - empirical fill probability
     - empirical adverse slippage
     - empirical cancel/working timeout rate

2. `ranking_score` 改成：
   - 主体：`expected_dollar_pnl_after_costs`
   - 乘子：`capture_probability * uncertainty_multiplier`
   - 减项：`risk_penalty`
   - `ops_readiness_score` 从“主加分项”改为 gate / tiebreaker

3. 把 current `expected_value_score / expected_pnl_score` 改成显式语义：
   - `expected_edge_bps_after_costs`
   - `expected_fill_value`
   - `expected_pnl_quote`
   - `ranking_score_v2`

**需要改哪些模块**：

- `domains/weather/opportunity/service.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/ui/ui_lite_db.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`

**需要加哪些测试**：

- ranking v2 deterministic tests
- historical capture correlation regression
- ops bonus no longer outranks materially better EV case
- market/source cohort prior application tests

**是否需要文档同步**：需要
**是否需要 migration**：大概率需要（新读模型字段/新 summary 列）
**推荐实施顺序**：6

---

### 5.6 calibration 已经进入主链，但 uncertainty model 仍然过粗

**优先级**：P1
**类型**：Trading
**性质**：旧问题部分修复后的残余能力缺口
**受影响文件**：
`domains/weather/forecast/calibration.py`
`domains/weather/forecast/adapters.py`
`domains/weather/opportunity/service.py`
`tests/test_phase7_ranking_penalty.py`

**代码事实**：

- calibration lookup 已按 `station/source/horizon bucket/season bucket/metric` 组织（`calibration.py:33-50`）。
- confidence multiplier 分成 `lookup_missing / insufficient_samples / limited_samples / healthy / watch / degraded`（`125-194`）。
- adapter 仍然是“point forecast + Gaussian distribution”，fallback sigma 还是 3.0 / 4.5 / 6.0 F（`adapters.py:13-17, 20-45, 48-55`）。
- `OpenMeteoAdapter` / `NWSAdapter` 都是把 point value 包成 Gaussian（`119-175`）。

**测试事实**：

- calibration tier 的 deterministic behavior 被测试覆盖了。
- 但没有看到：
  - station-level bias correction uplift test
  - interval coverage test
  - threshold-market calibration quality test

**文档事实**：

- “calibration-driven sigma lookup” 已落地，这个说法成立。
- 但它离“高质量概率模型”还有明显距离。

**我的推断**：

当前 calibration 更像**风险折扣器**，还不是**高质量概率定价器**。

**风险或问题描述**：

- model fair value 仍可能在关键温度阈值附近系统性偏差；
- Gaussian 假设对 tail / regime shift 可能过于理想化。

**为什么重要**：

天气市场的 alpha 很大一部分来自**阈值附近概率质量**，不是简单的点预测均值。

**如果不修会导致什么**：

- ranking 会对不确定性处理不够细；
- “看起来 edge 很大”的机会里，会混入很多模型边界不稳的样本。

**具体修复方案**：

1. 增加 bias correction：
   - per station/source/horizon/season 的 mean residual correction
2. 增加 conformal 或 quantile-based uncertainty：
   - 不再只输出单一 sigma
3. 对 threshold-sensitive 市场增加本地概率质量特征：
   - 例如阈值两侧质量密度、历史 crossing error
4. 将 calibration 健康度拆成：
   - bias quality
   - variance quality
   - sample sufficiency
   - regime stability

**需要改哪些模块**：

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- 机会评估相关接线模块

**需要加哪些测试**：

- coverage / calibration reliability tests
- bias correction regression tests
- threshold-sensitive forecast quality tests

**是否需要文档同步**：需要
**是否需要 migration**：很可能需要（扩展 calibration stats 存储）
**推荐实施顺序**：7

---

### 5.7 execution science 已经真实存在，但还没有形成盈利闭环

**优先级**：P1
**类型**：Trading
**性质**：旧问题已明显改善，但闭环仍未完成
**受影响文件**：
`asterion_core/ui/ui_lite_db.py`
`ui/data_access.py`
`ui/pages/execution.py`
`tests/test_predicted_vs_realized_summary.py`
`tests/test_post_trade_analytics.py`
`tests/test_execution_science_summary.py`

**代码事实**：

- `ui.predicted_vs_realized_summary` 已经不是 executed-only；它会纳入 gate/sign/submit rejected 和 working/unfilled 生命周期。
- `ui.watch_only_vs_executed_summary` 已经有 submission/fill/resolution capture ratio。
- `ui.execution_science_summary` 已按 market/strategy/wallet cohort 做聚合。
- 但 `strategy_engine_v3.py` / `service.py` 并没有直接使用这些 cohort analytics 去调 ranking 参数。

**测试事实**：

- 上述 3 组 analytics 都有真实测试，不是摆设。
- 但没有“analytics 反哺 ranking”的测试，因为现在代码里确实还没形成这一步。

**文档事实**：

- Post_P4 计划里对 richer analytics 的设想是对的，但当前很多表述还停在“分析层”，不是“反馈层”。

**我的推断**：

当前 execution science 更像**observer**，不是**controller**。

**风险或问题描述**：

系统已经知道自己在哪些 cohort 上 submit fail / fill fail / distortion 高，但主排序不会自动降低这些 cohort 的优先级。

**为什么重要**：

不把真实执行反馈接回主排序，系统就只能“看见错误”，不能“减少错误”。

**如果不修会导致什么**：

- 同类坏机会重复上榜；
- analytics 变成“好看的复盘”，不是“赚钱能力提升器”。

**具体修复方案**：

1. 新建 `execution_priors_by_cohort` 物化层：
   - cohort key: market / station / source / wallet / strategy / price bucket / freshness bucket
   - fields:
     - submission_capture_prior
     - fill_capture_prior
     - resolution_capture_prior
     - adverse_slippage_prior
     - dominant_miss_reason
     - dominant_distortion_reason

2. 在 `build_weather_opportunity_assessment()` 里读取这些 priors：
   - 调整 fill probability
   - 调整 liquidity/depth penalty
   - 对高 distortion cohort 额外降权

3. 在 UI 上展示“该机会的 cohort history”而不是只展示聚合榜单。

**需要改哪些模块**：

- `asterion_core/ui/ui_lite_db.py`
- `domains/weather/opportunity/service.py`
- `ui/data_access.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`

**需要加哪些测试**：

- cohort prior materialization tests
- ranking consumes cohort priors regression tests
- historical bad cohort gets downweighted tests

**是否需要文档同步**：需要
**是否需要 migration**：需要（新 summary/projection）
**推荐实施顺序**：8

---

## 6. Architecture / Maintainability Findings

### 6.1 读模型架构方向正确，但 `ui_lite_db.py` 与 `ui/data_access.py` 已经开始成为维护热点

**优先级**：P2
**类型**：Architecture
**性质**：新发现问题
**受影响文件**：
`asterion_core/ui/ui_lite_db.py`
`ui/data_access.py`

**代码事实**：

- `ui_lite_db.py` 约 **2979 行**
- `ui/data_access.py` 约 **1697 行**
- 两个文件都已经是多职责：
  - projection 构建
  - fallback source 选择
  - page-specific shaping
  - operator metrics
  - source status
  - detail row synthesis

**测试事实**：

- 这些模块现在还能被测试覆盖住，说明设计还没失控。
- 但测试主要是 projection correctness，不是长期演化成本控制。

**文档事实**：

- 当前阶段强调 truth-source cleanup。超大聚合文件会让 truth-source 更容易漂移。

**我的推断**：

这不是“现在坏了”，而是“再继续加功能会明显变坏”。

**风险或问题描述**：

- page 需求一多，read-model 与 fallback shaping 会越来越难收口；
- 任何一个字段名或 JSON 结构变化，都容易在多个页面上悄悄飘。

**为什么重要**：

这是未来 3-6 个月最容易积累维护债的地方。

**如果不修会导致什么**：

- UI truth-source 漂移变频繁；
- 新 analytics 接线成本升高；
- operator surface 会越来越像 patchwork。

**具体修复方案**：

1. `ui_lite_db.py` 按 projection 拆分：
   - `predicted_vs_realized_projection.py`
   - `watch_only_vs_executed_projection.py`
   - `execution_science_projection.py`
   - `market_research_projection.py`
   - `readiness_projection.py`

2. `ui/data_access.py` 按页面拆分：
   - `home_loader.py`
   - `markets_loader.py`
   - `execution_loader.py`
   - `agents_loader.py`
   - `system_loader.py`
   - `source_status.py`

3. 对重要 read-model row 定义 versioned schema / typed adapters。

**需要改哪些模块**：

- 上述两个大文件及其引用

**需要加哪些测试**：

- projection schema contract tests
- page loader golden tests
- fallback source adapter tests

**是否需要文档同步**：需要
**是否需要 migration**：通常不需要
**推荐实施顺序**：9

---

### 6.2 功能基本完成，但表达方式仍不稳：硬编码 copy 与 JSON-heavy contract 仍在制造漂移

**优先级**：P2
**类型**：Architecture
**性质**：旧问题收尾不彻底
**受影响文件**：
`ui/app.py`
`ui/pages/home.py`
`ui/pages/execution.py`
`docs/...`
若干 JSON context 字段使用点

**代码事实**：

- sidebar boundary 是硬编码。
- `Home` 仍有 “executed-only predicted-vs-realized rows” 文案（`home.py:104-110`）。
- `Execution` / `Markets` 里仍有一些 “executed evidence” 口径，而底层 lifecycle 已更宽。
- `assessment_context_json` / `pricing_context` / diagnostic details 的 JSON 字段很多，类型约束偏软。

**测试事实**：

- Phase 9 wording baseline 很有帮助，但还没覆盖这些更细的语义漂移点。

**文档事实**：

- 文档总叙事已经更新到位，但页面/脚本/历史分析文档还没全部同步。

**我的推断**：

Asterion 当前不是设计错，而是“表达系统”还没完全跟上“实现系统”。

**风险或问题描述**：

实现已经比文案新，但 operator 看到的文案还在说旧话。

**为什么重要**：

对于 operator console，误导性的正确措辞，比一个普通 bug 更危险。

**如果不修会导致什么**：

- 人会比代码更落后；
- 旧理解会继续影响操作习惯和 roadmap 判断。

**具体修复方案**：

- 所有 global boundary 文案改成动态 truth-source
- 所有 analytics 文案统一改成 lifecycle-aware
- 为关键 JSON context 引入 typed view model，不直接在页面里散读 JSON

**需要改哪些模块**：

- `ui/app.py`
- `ui/pages/home.py`
- `ui/pages/execution.py`
- `ui/pages/markets.py`
- 文档入口文件

**需要加哪些测试**：

- forbid stale wording tests
- JSON view model contract tests

**是否需要文档同步**：需要
**是否需要 migration**：不需要
**推荐实施顺序**：10

---

## 7. Test / Truth-Source Findings

### 7.1 当前 tests 比很多文档更接近真相，但 truth-source 还没完全收口

**优先级**：P2
**类型**：Testing
**性质**：新发现问题
**受影响文件**：
`tests/test_phase9_wording.py`
`tests/test_predicted_vs_realized_summary.py`
`docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md`
`docs/analysis/01_Current_Code_Reassessment.md`
`ui/pages/home.py`
`start_asterion.sh`

**代码事实**：

- 当前实现里 `predicted_vs_realized` 已经包含未成交/拒绝/working 等 lifecycle。
- submitter shell 已经有 live guard + attestation 持久化。
- start script boundary 文案仍然写 `remediation in progress`（`start_asterion.sh:20-27`）。

**测试事实**：

- `tests/test_predicted_vs_realized_summary.py` 明确证明 `predicted_vs_realized` 不是 executed-only。
- `tests/test_phase9_wording.py` 已经对大量关键措辞做了 baseline 管理。
- 但它**没有覆盖**：
  - Home 页那句 executed-only 旧文案
  - Post_P4 计划里 “executed-only predicted_vs_realized” 的旧表述
  - sidebar 是否动态来自 truth-source

**文档事实**：

- `Post_P4_Remediation_Implementation_Plan.md:54,63` 仍写着 executed-only `predicted_vs_realized`，这已经落后于真实代码与测试。
- `docs/analysis/01_Current_Code_Reassessment.md` 里关于 “submitter 边界主要在 handler” 与 “predicted_vs_realized fill-only/executed-only” 的旧判断，已经不是当前事实，应视为历史快照中的过时段落，不应再拿来当现状依据。
- README / AGENTS / Phase 9 主入口文档整体上反而比较接近当前现实。

**我的推断**：

目前最可信的 truth-source 顺序是：

`代码 + 通过的 targeted tests > README/AGENTS/入口 docs > 历史 analysis 文档`

**风险或问题描述**：

文档与实现之间的漂移已经从“宏观状态描述”缩小到“语义层/细节层”，但仍然存在。

**为什么重要**：

Asterion 现在的主要风险不再是“没实现”，而是“别人以为它还是旧实现”。

**如果不修会导致什么**：

- roadmap 会被历史结论拖慢或拖偏；
- operator 和 reviewer 会继续被旧语义影响。

**具体修复方案**：

1. 立刻改：
   - `home.py` 的 executed-only 旧文案
   - `Post_P4_Remediation_Implementation_Plan.md` 中 executed-only 相关描述
   - `start_asterion.sh` 的 boundary/status copy

2. 给历史 analysis 文档加醒目 banner：
   `historical assessment — not current truth-source`

3. 补 test：
   - forbid `executed-only predicted-vs-realized` in Home/docs
   - require dynamic boundary sidebar
   - require degraded-source badges

**需要改哪些模块**：

- 文档入口文件
- `ui/pages/home.py`
- `ui/app.py`
- `start_asterion.sh`
- wording / truth-source tests

**需要加哪些测试**：

- `test_phase9_wording.py` 扩展
- operator-facing acceptance tests
- truth-source consistency tests

**是否需要文档同步**：需要
**是否需要 migration**：不需要
**推荐实施顺序**：11

---

## 8. Top Risks Ranked by Priority

1. **P1 / Security**：submitter attestation 仍是 caller-trusted audit artifact，不是强闭环 capability token
2. **P1 / Security**：signer 仍信任 payload 提供的 `private_key_env_var`
3. **P1 / Trading**：execution economics 仍是 heuristic，已成为当前最大盈利瓶颈
4. **P1 / Trading**：execution science 还没反馈进 ranking，闭环未成
5. **P1 / UX+Trading**：UI 分数语义与 source truth-source 不够硬，可能误导 operator
6. **P2 / Security**：UI 适合内网，不适合直接公网暴露；Agents 页仍有 secret-adjacent exposure
7. **P2 / Architecture**：`ui_lite_db.py` / `ui.data_access.py` 已成维护热点
8. **P2 / Testing**：docs / home wording / historical analysis 仍有漂移
9. **P2 / Trading**：calibration 已接线但仍过粗，阈值型市场概率质量不足
10. **P3 / Architecture**：payload / diagnostic JSON 还偏重，typed schema 不足

---

## 9. Detailed Improvement Roadmap

### 9.1 Immediate Fixes

**目标**
把 live boundary 从“规范上谨慎”收口到“机制上更难绕过”，同时清理最明显的 truth-source 与 operator 误导点。

**交付物**

- submitter attestation v2
  - signed/HMAC attestation
  - expiry + nonce + consume-once
  - exact endpoint fingerprint included in signed decision
- signer v2
  - backend 内部基于 `wallet_id` 推导 secret env var
  - 不再接受 payload 注入的 `private_key_env_var`
- UI truth-source cleanup
  - sidebar 改为动态 boundary summary
  - Home 删除 executed-only 旧文案
  - Agents 删除 key-presence runtime config
- start script wording cleanup
- 历史 analysis 文档加 banner

**不做项**

- 不做 unattended live
- 不做新交易域
- 不做复杂 ML 升级

**关键改动模块**

- `asterion_core/contracts/live_boundary.py`
- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/signer/signer_service_v1.py`
- `dagster_asterion/handlers.py`
- `ui/app.py`
- `ui/pages/home.py`
- `ui/pages/agents.py`
- `start_asterion.sh`
- `tests/test_phase9_wording.py`

**关键测试**

- forged/expired/reused attestation rejected
- signer ignores/rejects payload env var
- sidebar reflects real manifest/readiness state
- Home/docs no longer contain executed-only drift

**验收标准**

- 不能再通过手工构造 approved attestation 直接驱动 `real_clob_submit`
- signer 不再依赖 caller 指定 secret env 名称
- UI 不再显示任何 API key presence
- 文档/代码/tests 对 `predicted_vs_realized` 语义一致

---

### 9.2 Near-Term Plan

**目标**
把 ranking 从“可用研究排序”升级为“更接近真实盈利排序”。

**交付物**

- `execution_priors_v1`
  - submission/fill/resolution capture priors
  - adverse slippage priors
  - cohort miss/distortion priors
- ranking v2
  - `expected_dollar_pnl_after_costs`
  - `capture_probability`
  - `risk_penalty`
  - `capital_efficiency`
- UI operator decomposition
  - why-ranked breakdown
  - row-level source badges
  - degraded-source clearly de-emphasized

**不做项**

- 不做全自动执行
- 不做复杂 portfolio optimizer

**关键改动模块**

- `domains/weather/opportunity/service.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/ui/ui_lite_db.py`
- `ui/data_access.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`

**关键测试**

- ranking v2 deterministic regression
- historical replay / capture correlation uplift
- degraded source badge acceptance tests

**验收标准**

- `ranking_score_v2` 和 realized capture / realized pnl 的相关性优于当前版本
- operator 能明确看出当前 top-ranked 机会为什么排第一
- fallback/degraded source 不再和 canonical rows 被同等视觉对待

---

### 9.3 Mid-Term Plan

**目标**
把 calibration 与 execution science 真正做成盈利闭环。

**交付物**

- calibration v2
  - bias correction
  - conformal/quantile uncertainty
  - threshold-sensitive probability features
- execution feedback loop
  - cohort priors nightly materialization
  - direct feed into opportunity assessment
- UI/model decomposition refactor
  - 拆分 `ui_lite_db.py`
  - 拆分 `ui.data_access.py`
  - versioned read-model schemas

**不做项**

- 不扩新资产类别
- 不做过早的分布式服务化

**关键改动模块**

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/opportunity/service.py`
- `asterion_core/ui/ui_lite_db.py`
- `ui/data_access.py`

**关键测试**

- calibration coverage / reliability tests
- bias correction regression tests
- cohort prior integration tests
- schema contract / golden tests

**验收标准**

- calibration 指标明显提升
- ranking 对“高 miss / 高 distortion cohort”有自动抑制能力
- UI read model 结构更清晰，truth-source 更容易维护

---

### 9.4 Optional Longer-Term Bets

**目标**
把当前 internal-grade constrained system 升级成更高保证、更可扩展的 live control plane。

**交付物**

- 反向代理 / OIDC / RBAC
- KMS / Vault / HSM 替代 env-private-key
- service-to-service attested control plane
- richer forecast ensemble / regime model
- 更强的 capital allocation / risk budget layer

**不做项**

- 不建议在当前闭环未成前追求“更多页面”“更多花哨 agent”

**关键改动模块**

- auth / deployment / secret management / model stack / execution orchestration

**关键测试**

- security integration tests
- auth access tests
- end-to-end dry-run / controlled-live acceptance

**验收标准**

- live boundary 不再主要依赖进程内 discipline
- UI 可以安全放在更正式的运维环境中
- 交易系统能在更多场景下稳定做出一致判断

---

## 10. Appendix: Files Reviewed

### 核心入口与文档

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/versions/v2.0/Asterion_Project_Plan.md`
- `docs/00-overview/versions/v2.0/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/versions/v1.0-remediation/phase-plans/Post_P4_Remediation_Implementation_Plan.md`

### 重点代码

- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/blockchain/chain_tx_v1.py`
- `asterion_core/contracts/live_boundary.py`
- `asterion_core/contracts/opportunity.py`
- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/ui/ui_lite_db.py`
- `domains/weather/opportunity/service.py`
- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `ui/data_access.py`
- `ui/app.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/pages/agents.py`
- `ui/pages/system.py`
- `start_asterion.sh`

### 额外支撑实现

- `asterion_core/signer/signer_service_v1.py`
- `asterion_core/live_side_effect_guard_v1.py`
- `asterion_core/monitoring/capability_manifest_v1.py`
- `asterion_core/monitoring/readiness_checker_v1.py`
- `dagster_asterion/handlers.py`
- `dagster_asterion/resources.py`
- `sql/migrations/0017_runtime_live_boundary_attestations.sql`
- `ui/auth.py`
- `ui/runtime_env.py`

### 重点参考并实际复核/执行的测试

- `tests/test_execution_foundation.py`
- `tests/test_live_submitter_backend.py`
- `tests/test_submitter_boundary_attestation.py`
- `tests/test_phase7_ranking_penalty.py`
- `tests/test_predicted_vs_realized_summary.py`
- `tests/test_post_trade_analytics.py`
- `tests/test_execution_science_summary.py`
- `tests/test_phase9_wording.py`
- 以及：
  - `tests/test_signer_shell.py`
  - `tests/test_chain_tx_scaffold.py`
  - `tests/test_controlled_live_capability_manifest.py`
  - `tests/test_ui_runtime_env.py`
  - `tests/test_live_prereq_readiness.py`
  - `tests/test_controlled_live_smoke.py`

---

最终结论可以压缩成一句：

**Asterion 现在已经是“有真实骨架的 constrained live trading system”，但它的下一阶段重点不该再是堆页面或堆文案，而是：把 submitter/signer boundary 做成真正难绕过的机制，并把 execution economics + calibration + execution science 变成真正会提升盈利质量的闭环。**
