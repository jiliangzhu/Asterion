# Asterion 当前代码重审报告（Current Code Reassessment）

> Analysis input only.
> Not implementation truth-source.
> Active planning entry: `docs/10-implementation/phase-plans/V2_Implementation_Plan.md`

**状态**: current analysis input (`2026-03-17`)

评审对象：`/mnt/data/Asterion_unzipped/Asterion` 当前代码快照  
评审方式：重新阅读代码、migrations、tests、runbooks、active implementation docs；不把旧报告当作当前事实。  
执行验证：本次额外执行了 65 个与 remediation / controlled-live / UI / post-trade analytics 直接相关的 targeted tests，均通过，包括：

- `tests.test_ui_auth`
- `tests.test_ui_runtime_env`
- `tests.test_controlled_live_capability_manifest`
- `tests.test_live_submitter_backend`
- `tests.test_live_prereq_readiness`
- `tests.test_readiness_evidence_bundle`
- `tests.test_weather_opportunity_service`
- `tests.test_market_quality_screen`
- `tests.test_predicted_vs_realized_summary`
- `tests.test_post_trade_analytics`
- `tests.test_p4_closeout`
- `tests.test_p4_plan_docs`
- `tests.test_ui_pages`
- `tests.test_ui_phase4_console`
- `tests.test_ui_data_access`

---

## 1. Executive Summary

这次重审后的核心结论与旧报告相比有明显变化：**Asterion 的 post-P4 remediation 不是“还在 PPT 阶段”，而是已经把很多之前缺失的关键骨架真正补上了。**

基于当前代码事实，我认为以下判断成立：

1. **旧报告里一批高优先级问题已经被真实修复或明显收口。** 典型包括：UI default-deny auth、UI 最小环境注入、post-P4 remediation 状态口径、capability manifest、readiness evidence bundle、canonical `OpportunityAssessment`、constrained `real_clob_submit` backend、以及一版可用的 predicted-vs-realized / watch-only-vs-executed / calibration health read model。
2. **当前架构比旧状态稳健得多。** `contracts -> runtime/trading -> ui_lite -> streamlit` 的主干更清楚；`ui_lite` 的原子构建与 required table 校验也明显提高了 operator surface 的稳定性。
3. **但系统仍然没有到“更高质量 constrained live”或“可持续赚钱”的程度。** 现在最危险的点不再是“完全没有边界”，而是：边界已经存在，但还有几个关键缺口；经济模型也已经成型，但还有直接影响排名和资本损失的缺陷。
4. **当前最值得担心的 P0/P1，不是 README 夸口，而是交易逻辑与 live boundary 的具体缺口。** 最严重的是 SELL 侧 executable edge 计算方向错误；其次是 live submit 路径没有和 chain-tx 路径一样强制 `ASTERION_CONTROLLED_LIVE_SECRET_ARMED=true`。
5. **系统现在适合的定位，仍然更像“operator console + weather market research / constrained execution infra”，而不是 unattended live trading system。**

### 直接回答本次 6 个特别问题

**1. 当前 `real_clob_submit` constrained backend 是否设计合理？是否还存在绕过风险？**  
合理性比旧版本高很多：它已经是 canonical backend，能落 `runtime.submit_attempts` / `runtime.external_order_observations`，并且 Dagster handler 已加上 manifest / manual-only / readiness GO / approval token / wallet allowlist / wallet readiness 等边界检查（`dagster_asterion/handlers.py:753-783`，`tests/test_live_submitter_backend.py:68-152`）。但它仍有绕过风险：**边界主要在 handler，不在 `SubmitterServiceShell` / `RealClobSubmitterBackend` 内部**（`asterion_core/execution/live_submitter_v1.py:310-386`）。任何绕过 canonical handler 的直接调用，都会失去这些约束。

**2. 当前 UI/web secret minimization 是否真正完成？还有没有残余暴露面？**  
**大部分已经完成，但不是绝对完成。** 当前标准启动路径会先清空 `ASTERION_* / QWEN_* / OPENAI_API_KEY / ALIBABA_API_KEY`，再只注入 allowlisted UI env（`start_asterion.sh:59-79`，`ui/runtime_env.py:12-24, 51-61`，`tests/test_ui_runtime_env.py:13-60`）。这说明“UI 默认继承 full `.env`”这一旧问题已经被实质修复。残余风险主要有两个：
- 如果有人绕开 `start_asterion.sh` 直接以 full env 启动 UI，最小化策略就失效。
- UI 仍然允许读取 `ASTERION_DB_PATH` 并在某些 fallback 路径直接读 canonical runtime DB（`ui/runtime_env.py:12-18`，`ui/data_access.py:1037-1081`）。这不是 secret 泄露，但仍是边界耦合。

**3. 当前 `predicted_vs_realized` 和 `watch-only vs executed` 是否足以支持真实研究闭环？**  
**足以算“第一版真实闭环”，不足以算“高质量交易研究闭环”。** `ui.predicted_vs_realized_summary`、`ui.watch_only_vs_executed_summary`、`ui.market_research_summary`、`ui.calibration_health_summary` 已经真实落地（`asterion_core/ui/ui_lite_db.py:41-45, 1563-1961`，`tests/test_predicted_vs_realized_summary.py:135-181`，`tests/test_post_trade_analytics.py:118-143`）。但现在的闭环还是 **fill-only / binary / resolved-biased**：未成交、撤单、超时、部分成交、submit 但未 fill 的情况都没有被正确纳入 capture / miss / execution science。

**4. 当前系统离“更高质量的 constrained live”还差什么？**  
差五件关键事：
- live submit 也要强制 `ARMED=true`，不能只在 chain-tx smoke 路径做；
- submitter 内部要内生化 boundary attestation，而不是只依赖 handler；
- `real_clob_submit` 需要真实 lifecycle runbook/tests（ack / external order id / fill / reconciliation / fault injection）；
- capability manifest/evidence 需要从“文件存在 + env presence”升级到“runtime attestation + semantic freshness”；
- 交易排序和 post-trade analytics 必须从启发式升级到 execution-aware 研究闭环。

**5. 如果我是 CTO，下一阶段最该做的 10 件事是什么？**  
见第 9.5 节。结论先说：优先顺序不是再堆 UI 页面，而是先修 live boundary 缺口、修 SELL-side edge bug、把 ranking 与 calibration 真正做对，再做 real submit lifecycle 验证。

**6. 如果我是交易负责人，下一阶段最该做的 5 个赚钱能力增强动作是什么？**  
见第 9.6 节。结论先说：最值钱的动作依次是：修 SELL-side executable edge、把 ranking 变成 expected executable PnL × confidence × calibration quality、补真实 fill/slippage 模型、把低质量市场连续惩罚纳入排序、做 predicted vs realized cohort attribution。

---

## 2. What Was Actually Fixed Since The Previous Assessment

### 2.1 旧报告核心问题逐条复核

| 旧报告问题主题 | 当前状态 | 代码 / 测试证据 | 当前结论 |
|---|---|---|---|
| P4 / closeout 被过度宣称 | **已修复** | `README.md:5` 改成 `post-P4 remediation active`；`ui/app.py:186-195` 明确 `Closeout pending objective verification`；`tests/test_p4_plan_docs.py:67-80` 强制禁止 `P4 closed` | 旧结论不应再作为当前事实 |
| closeout / readiness 测试治理坏掉 | **已修复** | `tests/__init__.py` 存在；`tests/test_p4_closeout.py`、`tests/test_p4_plan_docs.py`、`tests/test_live_prereq_readiness.py` 可运行并通过 | 旧的“关键 closeout tests 坏掉”结论已过时 |
| UI 默认认证过弱 | **已修复** | `ui/auth.py:19-60` 未配置即 default-deny；`ui/app.py:234-236` 未认证直接 `st.stop()`；`tests/test_ui_auth.py` 通过 | 旧风险已显著下降 |
| UI / web 会继承 full `.env` | **大体修复，残余风险仍在** | `start_asterion.sh:59-79` 先清 env 再只注入 allowlist；`ui/runtime_env.py:12-24, 51-61` allowlist；`tests/test_ui_runtime_env.py:13-60` 验证 secret 不进入 UI | “默认暴露 full env”已修复；“绕开启动脚本或读 canonical DB fallback”仍是残余风险 |
| opportunity 主要是 UI heuristic，缺少 canonical contract | **已修复** | `asterion_core/contracts/opportunity.py:37-93`；`domains/weather/opportunity/service.py:21-182`；`asterion_core/ui/ui_lite_db.py:2148-2223` 使用 canonical assessment | 这类旧批评已不再成立 |
| 缺少 capability manifest / readiness evidence | **已修复** | `asterion_core/monitoring/capability_manifest_v1.py`；`readiness_evidence_v1.py`；`tests/test_controlled_live_capability_manifest.py`、`tests/test_readiness_evidence_bundle.py` | 新边界证据层是真实落地，不再只是文档 |
| 没有 real submitter | **已修复** | `asterion_core/execution/live_submitter_v1.py:238-307`；`dagster_asterion/resources.py:281-282`；`tests/test_live_submitter_backend.py:44-152` | `real_clob_submit` 已真实存在 |
| calibration 没进入主链 | **已修复，但统计治理不足** | `domains/weather/forecast/adapters.py:57-69, 101-103, 132-133` 调用 provider；`scripts/run_real_weather_chain_smoke.py:344-351` 注入 `DuckDBForecastStdDevProvider` | “完全没接入主链”已过时，但“接入质量还弱”仍成立 |
| 没有 post-trade analytics 闭环 | **部分修复** | `asterion_core/ui/ui_lite_db.py:1563-1961`；`tests/test_predicted_vs_realized_summary.py`、`tests/test_post_trade_analytics.py` | 已有一版真实闭环，但还不是高质量 execution science |
| UI 不是 decision-centric | **大幅修复** | `ui/pages/home.py`、`markets.py`、`execution.py`、`system.py` 全部围绕 readiness / opportunities / execution reality / evidence 设计；`tests/test_ui_pages.py`、`tests/test_ui_phase4_console.py` 通过 | 旧判断需要更新：当前 UI 已明显转向 operator decision |
| `start_asterion.sh` 硬编码路径 / `--paper` 名称漂移 | **已修复到可接受水平** | `start_asterion.sh:14-18` 基于脚本目录发现路径；`start_asterion.sh:91, 245-312` 已改为 `--inspect-paper` | 旧问题已过时，但部署形态仍偏开发态 |

### 2.2 当前真实新增且有价值的工程能力

**代码事实**

- `OpportunityAssessment` / `MarketQualityAssessment` 已成为 canonical 机会评估契约（`asterion_core/contracts/opportunity.py`）。
- `ReadinessEvidenceBundle` 已把 readiness 从单一 GO/NO-GO 升级为 evidence artifact（`asterion_core/monitoring/readiness_evidence_v1.py:16-151`）。
- `real_clob_submit` constrained backend 已经具备 canonical request/response / journal / observation 路径（`asterion_core/execution/live_submitter_v1.py:238-307, 310-386`）。
- calibration persistence 与 source health 已经有 migration 支撑（`sql/migrations/0016_weather_mapping_calibration_quality.sql:1-32`）。
- `ui_lite` 不只是 read replica；它已经有 market opportunity / readiness evidence / predicted-vs-realized / calibration health 等 read model（`asterion_core/ui/ui_lite_db.py:26-45`）。
- `build_ui_lite_db_once(...)` 采用 snapshot + temp db + validate + atomic swap，失败时不会覆盖旧 lite DB（`asterion_core/ui/ui_lite_db.py:87-165`）。

**测试事实**

- readiness / capability manifest / submitter / UI auth / UI runtime env / weather opportunity service / post-trade analytics 都有针对性测试，并且在本次 targeted run 中通过。

**结论**

Asterion 当前已经不再是“文档多、代码少”的状态，而是拥有了相当明确的 infra skeleton。旧报告里把它描述成“更像 readiness/status console 而非真正有骨架的系统”这一点，已经不准确了。

---

## 3. What Still Remains Unfixed

### 3.1 旧问题中仍成立、或只修了一半的部分

| 问题主题 | 当前状态 | 当前判断 |
|---|---|---|
| controlled-live 边界过于依赖 env / 文件 / 推荐调用路径 | **部分修复** | 比旧状态强很多，但 live submit 仍主要依赖 handler gating，仍不是系统级强边界 |
| capability manifest 作为 machine truth-source | **部分成立** | 作为 capability inventory 合理；作为“当前 runtime boundary 完整真相源”不够 |
| UI secret / boundary 暴露面 | **部分修复** | 默认启动路径已最小化；直接启动 UI、fallback 读 canonical DB、env-based agent config 仍是残余问题 |
| replay / idempotency / external path 真实性证明不足 | **仍成立** | `real_clob_submit` 已落地，但 readiness 仍未证明真实外部 submit 生命周期 |
| 远程 API / provider 单点脆弱性 | **仍成立** | forecast / submitter / external execution 依赖外部服务，且目前主要靠 graceful failure，而不是多路径冗余 |
| 策略经济学弱于基础设施工程 | **仍成立** | 这仍然是最核心的 business / PnL 短板 |
| writerd / start script 偏开发态 | **部分成立** | allow-list、single-writer 约束更清楚了，但仍是单机本地风格 |

### 3.2 新的、比旧报告更具体的剩余问题

这次重审中真正需要 CTO / trading lead 立刻关注的，不是“旧问题还在不在”，而是以下几个 **当前代码层面真实存在** 的缺陷：

1. **SELL 侧 executable edge 方向错误**（P0，新的代码级发现）
2. **live submit 没有像 chain-tx 一样强制 `ARMED=true`**（P1，旧问题残留的更具体落点）
3. **submitter boundary 仍然不是内生化约束**（P1，旧问题残留）
4. **calibration 已接入主链，但统计治理仍弱，且未进入 ranking**（P1，部分修复后残留）
5. **predicted-vs-realized / watch-only-vs-executed 仍不足以做真实 execution science**（P1，部分修复后残留）
6. **UI 的 agent / runtime boundary 文案与实际影响路径存在漂移**（P2，新发现）
7. **active remediation doc 仍有内部矛盾**（P2，当前文档漂移）

---

## 4. Security / Boundary Review

### 4.1 Issue: Live submit path misses explicit `ARMED=true` enforcement

- **风险等级**：P1
- **问题类型**：旧问题残留 + 新定位出的具体缺口
- **受影响文件**：
  - `dagster_asterion/handlers.py`
  - `asterion_core/monitoring/capability_manifest_v1.py`
  - `tests/test_controlled_live_capability_manifest.py`
  - `docs/10-implementation/runbooks/P4_Controlled_Live_Smoke_Runbook.md`

**代码事实**

- live submit 路径的 guard 在 `dagster_asterion/handlers.py:753-783`；这里检查 manifest、manual_only、real submitter、real_broadcast、approval token、wallet readiness、readiness GO，但**没有**检查 `ASTERION_CONTROLLED_LIVE_SECRET_ARMED == true`。
- 与之对照，chain-tx controlled live smoke 路径在 `dagster_asterion/handlers.py:1114-1116` 明确检查 `is_armed`，否则返回 `controlled_live_smoke_not_armed`。
- capability manifest 只要求 `_ARMED` 环境变量“存在且非空”，不会验证它是否为 `true`（`asterion_core/monitoring/capability_manifest_v1.py:30, 65-69`）。

**测试事实**

- `tests/test_controlled_live_capability_manifest.py:14-33` 和 `:73-94` 明确把 `ASTERION_CONTROLLED_LIVE_SECRET_ARMED="false"` 视为 `manifest_status = valid`。
- `tests/test_live_submitter_backend.py:99-152` 只覆盖 `ARMED=true` 的成功路径，没有覆盖 `ARMED=false` 的 live submit 负测试。
- chain-tx 路径则有明确的 arm-bit negative test：`tests/test_controlled_live_smoke.py:49-96`。

**文档事实**

- runbook 把 `ASTERION_CONTROLLED_LIVE_SECRET_ARMED=true` 写成 controlled-live smoke 的前置条件（`docs/10-implementation/runbooks/P4_Controlled_Live_Smoke_Runbook.md:57-67`）。

**推断**

当前 live submit 的安全语义与 chain-tx smoke 语义不一致。对 operator 和 reviewer 来说，这会造成“manifest valid / readiness GO / approval token matched 就等于 armed”的误判。

**为什么重要**

这不是“随手补个 if”的小问题。它直接影响 **real submit 路径是否真正仍然处于 default-off + armed-gate 模式**。现在的代码语义更像“只要 manifest 和 approval token 在，就可以 live submit”。

**修复方案**

1. 在 `run_weather_submitter_smoke_job(...)` 的 `LIVE_SUBMIT` 分支里，加入与 chain-tx 路径一致的 `is_armed` 检查。  
2. 在 capability manifest 中增加 `armed_state`（布尔值）与 `armed_env_present`（是否存在）两个字段，避免把 presence 和 state 混为一谈。  
3. 增加负测试：
   - `ARMED=false` 时 live submit 必须 `blocked`
   - manifest `valid` 但 `armed_state=false` 时 UI/System 页面必须显式 warning  
4. 同步 README / runbook / system page 的 boundary 说明。

**建议实施顺序**

- 立即做（P0/P1 交界，建议按 P0 执行）

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：需要  
- docs sync：需要

---

### 4.2 Issue: Submitter boundary is still handler-centric, not intrinsic

- **风险等级**：P1
- **问题类型**：旧问题残留
- **受影响文件**：
  - `asterion_core/execution/live_submitter_v1.py`
  - `dagster_asterion/handlers.py`
  - `dagster_asterion/resources.py`

**代码事实**

- `SubmitterServiceShell.submit_order(...)` 只负责 journal + delegate，并不检查 readiness、allowlist、approval token、manifest hash 或 wallet readiness（`asterion_core/execution/live_submitter_v1.py:314-386`）。
- `RealClobSubmitterBackend.submit(...)` 只检查 `submit_mode == LIVE_SUBMIT`，然后调用 provider（`asterion_core/execution/live_submitter_v1.py:246-307`）。
- 真正的 constrained boundary 主要放在 `run_weather_submitter_smoke_job(...)` 的 Dagster handler 内（`dagster_asterion/handlers.py:753-783`）。

**测试事实**

- `tests/test_live_submitter_backend.py:68-152` 验证的是 canonical handler 路径；没有测试“直接调用 shell/backend 也必须被 boundary 拦住”的语义，因为当前代码本身并不具备这个性质。

**推断**

如果未来出现新的 manual script、CLI、agent worker 或内部模块直接拿到 `SubmitterServiceShell` / `RealClobSubmitterBackend`，就可能绕过 handler 级别的 constrained boundary。

**为什么重要**

这类问题在系统变复杂后极易复发：**第一版只有一个入口，所以看起来安全；第二版多了一个入口，旧假设就失效。** 当前 submitter 的安全边界还没有“内生化”。

**修复方案**

建议增加一个 `LiveSubmitAuthorization` / `BoundaryAttestation` 对象，作为 `submit_order(...)` 的必填入参之一；至少包含：

- manifest hash
- readiness report hash / generated_at
- approval token digest
- requester
- wallet_id
- TTL / issued_at
- `armed_state`
- allowlisted wallet assertion

`SubmitterServiceShell` 在 `LIVE_SUBMIT` 时必须验证这个 attestation；没有 attestation 或 attestation 失效时直接拒绝。

**更优替代方案**

把 real submit 做成独立进程 / service，UI / orchestration 只能 RPC 调它，service 自身验证 boundary token，而不是同进程 import backend。

**建议实施顺序**

- P1

**是否需要 migration / tests / docs sync**

- migration：可选；如果要落审计表，建议新增 `runtime.live_boundary_attestations`  
- tests：必须  
- docs sync：必须

---

### 4.3 Issue: Capability manifest is useful, but not sufficient as a machine truth-source

- **风险等级**：P1
- **问题类型**：旧问题残留（语义升级版）
- **受影响文件**：
  - `asterion_core/monitoring/capability_manifest_v1.py`
  - `asterion_core/monitoring/readiness_evidence_v1.py`
  - `ui/pages/system.py`
  - `README.md`

**代码事实**

- manifest v1 记录 backend kinds、allowed wallets/tx kinds/spenders、max approve caps、required env vars、submitter capability（`asterion_core/monitoring/capability_manifest_v1.py:17-88`）。这本身是好的。
- 但它没有记录：
  - policy hash
  - submitter endpoint / API base URL 指纹
  - signer address / wallet-funder attestation
  - 当前 arm state（只有 env presence）
  - 生成 manifest 时的 semantic dependency hash
- `ReadinessEvidenceBundle` 的 freshness 也是基于文件 mtime，而不是内容/数据水位/环境 attestation（`asterion_core/monitoring/readiness_evidence_v1.py:170-198`）。
- System 页面展示 boundary 时也只展示 summary 字段，不展示实际 armed state / policy hash / attested identities（`ui/pages/system.py:103-125`）。

**测试事实**

- `tests/test_controlled_live_capability_manifest.py` 覆盖了 valid / invalid / blocked 与 submitter capability，但没有覆盖“manifest 能否证明 runtime current state”这件事；因为 v1 本身就不提供这个能力。

**文档事实**

- README 把 `data/meta/controlled_live_capability_manifest.json` 描述为 capability boundary 的 truth-source（`README.md:183-189`）。

**推断**

更准确的说法应该是：**manifest v1 是 capability inventory / config boundary snapshot，不是 full runtime attestation。** 如果把它当成更强的真相源，会高估它的证明能力。

**为什么重要**

这会影响 rollout review 的质量：审查人容易把“文件存在且 valid”误当成“当前 runtime 真的处于同样边界之内”。

**修复方案**

升级到 `controlled_live_capability_manifest.v2`：

- 新增 `policy_hash`
- 新增 `armed_state`
- 新增 `signer_identity` / `wallet_funder_map`
- 新增 `submitter_endpoint_fingerprint`
- 新增 `generated_from`（readiness data hash / env snapshot hash）
- UI System 页面展示这些字段，不再只展示 `manual_only/default_off/...`

**建议实施顺序**

- P1

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：需要  
- docs sync：需要

---

### 4.4 Issue: UI/web secret minimization is materially improved, but not fully isolated

- **风险等级**：P2
- **问题类型**：旧问题部分修复后的残余风险
- **受影响文件**：
  - `ui/runtime_env.py`
  - `start_asterion.sh`
  - `ui/data_access.py`
  - `ui/pages/agents.py`

**代码事实**

- UI env allowlist 只保留少量 exact keys + prefixes（`ui/runtime_env.py:12-24`）。
- `start_asterion.sh` 在启动 UI 前会先清除 `ASTERION_* / QWEN_* / OPENAI_API_KEY / ALIBABA_API_KEY`，再重新导出 allowlisted UI env（`start_asterion.sh:59-79`）。
- 但是 UI 仍可读取 `ASTERION_DB_PATH`，并在 fallback 场景直接读 runtime DB / smoke DB（`ui/runtime_env.py:15-18`，`ui/data_access.py:1037-1081`）。
- Agents 页面还会直接显示 `QWEN_API_KEY` / `ALIBABA_API_KEY` 是否存在（`ui/pages/agents.py:117-124`）。

**测试事实**

- `tests/test_ui_runtime_env.py:13-60` 证明 secret key 与 controlled-live token 不会被 allowlist 带进 UI env。这个修复是真实的。

**推断**

Asterion 已经实现了“标准路径 secret minimization”，但还没有实现“结构性 UI/runtime 隔离”。尤其是：

- UI 直接启动时没有强制 minimal-env invariant；
- UI 仍然能够对 canonical runtime DB 形成 read coupling；
- Agents 页面基于进程 env 显示 runtime config，这在 minimal env 下可能变成误导性的“未配置”。

**修复方案**

1. UI 启动时主动检测并拒绝含有非 allowlisted secret env 的进程。  
2. Agents 页面不再直接看 `QWEN_API_KEY` / `ALIBABA_API_KEY`；改成读取 runtime side 生成的 canonical health artifact。  
3. 中期收紧为“UI 只读 `ui_lite` + readiness/evidence artifacts，不再 fallback 读 canonical runtime DB”。

**建议实施顺序**

- P2

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：需要  
- docs sync：建议有

---

### 4.5 Issue: Live submit provider response can be accepted without a stable external order id

- **风险等级**：P2
- **问题类型**：新发现问题
- **受影响文件**：
  - `asterion_core/execution/live_submitter_v1.py`

**代码事实**

- `_normalize_real_submit_provider_response(...)` 在 `status in {accepted, ok, success, submitted}` 时会直接返回 `accepted`，即使 `external_order_id` 为空（`asterion_core/execution/live_submitter_v1.py:730-743`）。

**推断**

如果 provider 返回 `accepted` 但没有外部订单 ID，系统仍会把本次 submit 记为 accepted。这会削弱之后的 reconciliation / fill polling / operator auditability。

**为什么重要**

对于 real submit，`accepted` 但没有可追踪的 external identifier，几乎等于“口头说已提交”。这不利于高质量 live boundary。

**修复方案**

- live submit 场景下：没有 `external_order_id` 的 accepted response 一律降级为 `accepted_untracked` 或直接 `rejected_invalid_provider_response`。  
- 增加 provider 契约测试与 runbook。  
- UI Execution 页面应显式显示 `external_order_id` 是否缺失。

**建议实施顺序**

- P2

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：需要  
- docs sync：建议有

---

### 4.6 Issue: Readiness evidence freshness is file-mtime based, not semantic freshness

- **风险等级**：P2
- **问题类型**：新发现问题
- **受影响文件**：
  - `asterion_core/monitoring/readiness_evidence_v1.py`
  - `ui/pages/system.py`

**代码事实**

- `_artifact_info(...)` 只根据文件是否存在、mtime 和 6 小时阈值判断 `ok/stale/missing`（`asterion_core/monitoring/readiness_evidence_v1.py:170-198`）。

**推断**

这意味着“一个刚被 touch 过但内容过时的文件”在 evidence 里也会显示 fresh。对 operator 来说，这不足以支撑更强的 rollout decision。

**修复方案**

- readiness / evidence 升级为 semantic freshness：
  - readiness report 内嵌 evaluated_at
  - ui_lite meta 读 `last_success_ts_ms`
  - manifest 读 `generated_at + policy_hash`
  - weather smoke 读 report timestamp 与 runtime watermarks
- System 页面同时展示 `file freshness` 和 `semantic freshness`。

**建议实施顺序**

- P2

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：需要  
- docs sync：建议有

---

## 5. Architecture Review

### 5.1 当前 architecture 是否比之前更稳健？

**结论：是，明显更稳健。**

相比旧状态，当前架构最显著的进步有四点：

1. **canonical 契约更明确。** `OpportunityAssessment`、`MarketQualityAssessment`、`ReadinessEvidenceBundle`、capability manifest 让 UI 与 orchestration 不再只能依赖 ad-hoc 结构。
2. **持久化分层总体仍然合理。** `trading.*`、`runtime.*`、`meta.*`、`agent.*`、`ui.*` 的分层没有出现根本性塌陷；相反，`ui.*` 现在更像真正的 read model，而不只是随手拼表。
3. **weather 域模型更完整。** `weather.forecast_calibration_samples`、`weather.source_health_snapshots`、`mapping_confidence/mapping_method` 让 forecast / source health / mapping 的不确定性开始进入 canonical surfaces（`sql/migrations/0016_weather_mapping_calibration_quality.sql:1-32`）。
4. **operator UI 的主叙事已经明显改善。** Home / Markets / Execution / System 的组织方式，比旧版更像决策系统，而不是 status dump。

### 5.2 当前设计得好的部分

#### A. `ui_lite` 的构建方式是正确的

- `build_ui_lite_db_once(...)` 使用 snapshot + temp db + validate + atomic swap（`asterion_core/ui/ui_lite_db.py:87-165`）。
- `_REQUIRED_UI_TABLES` 把 operator console 所依赖的主表列成显式契约（`asterion_core/ui/ui_lite_db.py:26-45`）。

这套设计对 operator console 非常重要：**它避免了“半构建状态的 UI”误导人**。这是当前仓库里最成熟的基础设施设计之一。

#### B. opportunity contract 下沉到 domain 层是正确的

`domains/weather/opportunity/service.py` 已经不再只是 UI heuristic helper，而是 canonical weather opportunity assessment 的 domain service。这个方向比旧版“到处算 edge 和 score”的状态稳健得多。

#### C. readiness / evidence / capability 的分层方向正确

现在至少可以区分：

- readiness report：gate judgement
- capability manifest：边界能力快照
- evidence bundle：交付物 / freshness / blockers / warnings 汇总

虽然这三者还需要进一步收口，但方向是对的。

### 5.3 当前仍然过度复杂或维护风险高的部分

#### Issue: live boundary 逻辑仍然散落在多个层

- `dagster_asterion/handlers.py` 里有主要 guard
- `capability_manifest_v1.py` 定义边界快照
- runbook / README / System page 又各自表达一次
- `SubmitterServiceShell` / backend 本体却不 enforce

这说明 **boundary semantics 还没有形成单一执行入口**。短期能跑，长期容易出现“文档这么说、UI 那么说、handler 做一套、backend 什么都不做”的边界漂移。

#### Issue: opportunity 表达仍有平行字段和轻度语义重复

- `ranking_score` 与 `opportunity_score` 在 UI summary 中被双轨保留（`asterion_core/ui/ui_lite_db.py:2210-2214`）
- `confidence_score` / `confidence_proxy` 也是并行表达（`asterion_core/ui/ui_lite_db.py:2205-2206`）

这不算大 bug，但说明当前仓库仍保留一些“兼容旧 UI / 旧视图”的平行表达。长期应收敛到一套 canonical naming。

#### Issue: `ui/data_access.py` 承担了太多职责

当前它同时负责：

- data loading
- fallback orchestration
- surface health judgement
- overview metric synthesis
- market chain intelligence merge

这在 remediation 阶段是能接受的，但长期看会让 UI 读层逐渐变成“第二套业务逻辑引擎”。

### 5.4 哪些边界最值得继续重构？

1. **live boundary**：必须从“handler 规则集合”重构成“submitter intrinsic boundary + attestation”。
2. **opportunity economics**：要从当前 heuristic score 进化成真正的 execution-aware ranking model。
3. **execution research loop**：需要从 fill-only projection 升级到完整订单生命周期研究层。
4. **UI data access**：应逐步减少 runtime DB fallback，收敛为 `ui_lite + artifacts first`。

### 5.5 当前文档漂移

当前最明确的一处漂移是：

- `Post_P4_Remediation_Implementation_Plan.md:56-67, 94-99` 明确把 `real_clob_submit` 写成已落地；
- 但同一文档 `:207-210` 的 “明确不做” 仍写着“`不实现 real live submitter`”。

这属于**当前仍未收口的 active-doc drift**。它不影响代码真实性，但会影响 reviewer 对计划语义的理解。

---

## 6. Trading / Profitability Review

### 6.1 总判断

当前 trading 能力比旧评估时更真实，但**仍然是 infrastructure 成熟度高于 alpha / execution science 成熟度**。

- market discovery / rule2spec / station mapping / forecast / pricing / watch-only / live-prereq / external reconciliation 的主链条已经真实存在。
- calibration 也已经不是“写在建议里”，而是进入了 forecast adapters。
- 但当前最影响赚钱能力的部分——**executable edge、ranking、fill/slippage 模型、post-trade attribution**——仍然不够强，甚至有一处会直接扭曲 SELL 机会排序的 bug。

### 6.2 Issue: SELL-side executable edge is directionally wrong

- **风险等级**：P0
- **问题类型**：新发现问题
- **受影响文件**：
  - `domains/weather/opportunity/service.py`
  - `asterion_core/ui/ui_lite_db.py`
  - `ui/pages/markets.py`
  - 所有依赖 `edge_bps_executable` / `ranking_score` 的 downstream surface

**代码事实**

- 当前实现无论 BUY 还是 SELL，都执行：
  - `execution_adjusted_fair_value = model_fair_value - costs`
  - `edge_bps_executable = execution_adjusted_fair_value - reference_price`  
  （`domains/weather/opportunity/service.py:80-85`）

这对 BUY 是合理的；对 SELL 则方向错了。SELL 机会本质上要求 fair value **低于** market price；加入成本后，应该让可执行 edge **变小**，而不是变得更负更极端。

**评审时执行事实**

直接调用当前代码：

- `reference_price = 0.70`
- `model_fair_value = 0.50`
- `fees_bps = 30`

返回：

- `edge_bps_model = -2000`
- `edge_bps_executable = -2095`
- `best_side = SELL`

即成本被错误地用于“放大 SELL 绝对 edge”。

**测试事实**

- `tests/test_weather_opportunity_service.py:9-30` 只覆盖 BUY 侧正边；没有 SELL 对称性测试。

**为什么重要**

这是当前最直接影响赚钱能力的 bug。它会：

- 高估 SELL 机会
- 扭曲 `ranking_score`
- 误导 Markets / Home / Execution 页面
- 让 post-trade attribution 与机会排序发生系统性偏差

**修复方案**

把 executable edge 改成 side-aware：

- BUY：`execution_adjusted_fair_value = model_fair_value - costs`
- SELL：`execution_adjusted_fair_value = model_fair_value + costs`

更稳妥的做法是直接用 side-aware executable price：

- BUY edge = `fair_value - (reference_price + entry_cost)`
- SELL edge = `(reference_price - exit_cost) - fair_value`

再统一转换成 signed edge / absolute edge / ranking features。

**必须增加的测试**

- BUY / SELL 对称性测试
- 相同绝对 mispricing 下，加入成本后 BUY/SELL 的 absolute executable edge 都应下降
- UI lite summary 对 SELL 市场的 regression tests

**建议实施顺序**

- **P0，立即修**

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：必须  
- docs sync：建议有（Markets 页说明）

---

### 6.3 Issue: Ranking is still heuristic-heavy and confidence-blind

- **风险等级**：P1
- **问题类型**：旧问题残留 + 当前代码明确化
- **受影响文件**：
  - `domains/weather/opportunity/service.py`
  - `asterion_core/contracts/opportunity.py`
  - `asterion_core/ui/ui_lite_db.py`
  - `ui/data_access.py`

**代码事实**

- `confidence_score` 虽然进入了 contract 和 assessment context（`asterion_core/contracts/opportunity.py:53`，`domains/weather/opportunity/service.py:45, 112`），但 **没有**进入 `expected_value_score`、`expected_pnl_score` 或 `ranking_score` 的计算（`domains/weather/opportunity/service.py:89-91`）。
- `expected_value_score = abs(edge_bps_executable) * fill_probability`；`expected_pnl_score = expected_value_score * depth_proxy`；`ranking_score = expected_pnl_score + ops_readiness_score`（`domains/weather/opportunity/service.py:89-91`）。
- `ops_readiness_score` 居然给 `not_started` 一个正向 +10（`domains/weather/opportunity/service.py:348-353`）。

**推断**

当前 `expected_pnl_score` 这个名字其实容易误导，因为它不是钱包规模意义上的 expected PnL，而更像“边 × fill × depth 的启发式强度分数”。再加上 `not_started` 也加分，会使一些没经过 live-prereq 验证的市场在排序上被抬高。

**为什么重要**

这会造成三个问题：

1. 低置信度市场和高置信度市场在 ranking 上差别不够大；
2. calibration 不足 / source stale / mapping low-confidence 的市场，只有 actionability bucket 惩罚，没有强的连续排序惩罚；
3. operator 容易把 `ranking_score` 理解成更接近盈利的指标，但当前它仍然偏 heuristic。

**修复方案**

建议把 ranking 拆成四个明确组件：

- `expected_executable_pnl_score`
- `confidence_penalty`
- `input_quality_penalty`（mapping / freshness / calibration quality / market quality）
- `ops_execution_readiness_score`

并明确：

- `not_started` 不应加分；最多是 0，甚至在 live-related ranking 中应小幅扣分  
- `confidence_score`、`calibration_health_status`、`mapping_confidence` 应进入连续惩罚项  
- `ranking_score` 只保留一条 canonical 公式，不再与 `opportunity_score` 双轨并行

**建议实施顺序**

- P1

**是否需要 migration / tests / docs sync**

- migration：不需要（可先在 read model / JSON context 中推进）  
- tests：必须  
- docs sync：建议有

---

### 6.4 Issue: Calibration is wired into the main path, but statistical governance is still weak

- **风险等级**：P1
- **问题类型**：部分修复后的残余风险
- **受影响文件**：
  - `domains/weather/forecast/calibration.py`
  - `domains/weather/forecast/adapters.py`
  - `scripts/run_real_weather_chain_smoke.py`
  - `asterion_core/ui/ui_lite_db.py`

**代码事实**

- forecast adapters 已通过 `_resolve_std_dev(...)` 使用 calibration provider（`domains/weather/forecast/adapters.py:57-69, 101-103, 132-133`）。
- smoke 链也真实注入了 `DuckDBForecastStdDevProvider`（`scripts/run_real_weather_chain_smoke.py:344-351`）。
- provider 查询 exact bucket：`station_id + source + horizon_bucket + season_bucket + metric`（`domains/weather/forecast/calibration.py:137-157`）。
- 只要 `sample_count > 0`，它就会返回 `STDDEV_SAMP(residual)`；如果 stddev 为 0 但 `avg_abs_residual > 0`，也会直接返回（`domains/weather/forecast/calibration.py:164-173`）。
- UI calibration health 却把 `<5` 样本定义为 `insufficient_samples`（`asterion_core/ui/ui_lite_db.py:1883-1961`）。

**推断**

当前系统出现了一个不一致：**forecast 主链可能在用统计上很弱的 calibration sigma，而 UI 侧却会把同样的数据标成 insufficient**。此外，当前 exact-bucket lookup 没有 station -> region/source/horizon 的层级回退策略，覆盖率会比较脆弱。

**为什么重要**

这会让 operator 以为“calibration 已经进入主链，所以 forecast 更可靠了”，但实际上：

- calibration coverage 可能稀疏
- sample size 可能太少
- ranking 还没把 calibration quality 真正纳进去

**修复方案**

1. provider 增加最低样本数阈值（例如 <5 返回 `None`）  
2. 增加 hierarchical fallback：
   - station+source+horizon+season+metric  
   - source+horizon+season+metric  
   - source+horizon+metric  
   - source+metric global fallback  
3. 在 pricing context 中显式记录：
   - calibration bucket key
   - sample count
   - sigma source（calibrated / fallback_static / fallback_hierarchical）  
4. 在 opportunity ranking 中对 `insufficient_samples` / `fallback_static` 进行惩罚。

**建议实施顺序**

- P1

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：必须  
- docs sync：建议有

---

### 6.5 Issue: `predicted_vs_realized` / `watch-only vs executed` is still too coarse for real execution science

- **风险等级**：P1
- **问题类型**：部分修复后的残余风险
- **受影响文件**：
  - `asterion_core/ui/ui_lite_db.py`
  - `ui/pages/execution.py`
  - `ui/pages/markets.py`

**代码事实**

- `ui.predicted_vs_realized_summary` 的 base query 是 inner join `fill_agg`（`asterion_core/ui/ui_lite_db.py:1628-1630`），因此它实际上是 **fill-only**。submit 但未 fill、被 cancel、挂单超时的 ticket 不会出现。
- `ui.watch_only_vs_executed_summary` 把 `opportunity_count` 写死为 1，并把 `execution_capture_ratio` 做成 `0/1` 二元值（`asterion_core/ui/ui_lite_db.py:1761-1783`）。
- `ui.market_research_summary` 只有 `resolved_trade_count > 0` 才认为 `executed_evidence_status = executed`（`asterion_core/ui/ui_lite_db.py:1854-1855`）。

**测试事实**

- `tests/test_post_trade_analytics.py:118-143` 验证的是最简单的“一市场有一个 resolved trade -> capture=1.0 / executed”。这说明当前实现是有意的，但也证明了它还只是第一版。

**为什么重要**

这会误导三个核心研究问题：

1. **capture ratio**：现在更像“是否有 fill”，不是“是否执行了机会”；
2. **watch-only vs executed**：submitted-but-unfilled 会被归到 watch-only 一侧；
3. **executed evidence**：filled 但未 resolution 的市场，在 market research 上仍可能显示成 watch-only。

**修复方案**

建议新增一个更完整的 execution research projection，例如：

- `ui.order_lifecycle_research_summary`
- `ui.market_capture_summary_v2`

至少纳入这些状态：

- watch_only
- proposed
- ticket_created
- signed
- submitted_ack
- working_unfilled
- partially_filled
- filled_unresolved
- resolved
- cancelled / expired
- rejected

并把 capture 定义拆成：

- `submission_capture_ratio`
- `fill_capture_ratio`
- `resolution_capture_ratio`

**建议实施顺序**

- P1

**是否需要 migration / tests / docs sync**

- migration：不一定；先做 `ui.*` projection 即可  
- tests：必须  
- docs sync：建议有

---

### 6.6 当前 weather strategy 链还缺什么？

**当前链条的优点**

- market discovery、rule2spec、station mapping、forecast adapters、pricing、watch-only、live-prereq、external reconciliation 已有真实主链；
- market quality / source freshness / mapping confidence 已进入 assessment context；
- agent 现在更偏 review / exception，而不是直接参与 execution 主链。

**当前最明显的 alpha leakage / ranking distortion / execution distortion**

1. SELL-side edge bug 会直接扭曲一部分 market ranking。  
2. confidence / calibration quality 还没有进入 ranking 主体。  
3. fill probability / depth proxy / slippage 仍是固定 heuristic，而不是来自历史 fill 数据。  
4. market quality 仍主要是 threshold bucket，而不是 continuous penalty。  
5. predicted-vs-realized 还不足以告诉你“高 edge 没赚到钱，到底是 forecast 错、定价错、还是 execution 差”。

**当前系统是否已具备真正赚钱潜力？**

- **研究潜力：有。**  
- **infra 价值：有，而且不低。**  
- **稳定赚钱能力：尚未被证明。**

现在的系统已经足以支持“半自动 weather market 研究 + operator decision support”，但离“我愿意把更大真钱托付给它”还有明显距离。

---

## 7. UI / Operator Console Review

### 7.1 当前 UI 是否真的围绕 operator decision 设计？

**结论：比旧版明显更对，但还没有真正围绕“盈利归因”做到位。**

**代码事实**

- Home 页面直接把 `Readiness Decision / Top Opportunities / Largest Blocker / Predicted vs Realized Snapshot / Edge Capture / Degraded Inputs` 放在首页（`ui/pages/home.py:34-188`）。
- Markets 页面以 `Opportunity Terminal` 为主叙事，重点看 `Executable Edge / Ranking Score / Mapping Confidence / Source Freshness / Market Quality / Execution Reality`（`ui/pages/markets.py:114-415`）。
- Execution 页面先展示 executed-only PVR，再展示 live-prereq / tickets / watch-only-vs-executed / calibration health（`ui/pages/execution.py:31-255`）。
- System 页面把 evidence bundle 放在中心位置（`ui/pages/system.py:70-193`）。

**测试事实**

- `tests/test_ui_pages.py`、`tests/test_ui_phase4_console.py` 覆盖了 Home / Markets / Execution / Agents / System 的主要渲染路径。

**当前仍然不够清晰的地方**

1. **Top Opportunity / Top Opportunity Score 仍然建立在 heuristic ranking 上。** operator 很容易把这个分数当成更接近 PnL 的指标。  
2. **Execution 页面展示的 capture ratio 仍然是过于粗糙的二元 metric。**  
3. **Markets 页面会在 degraded source 情况下继续展示 actionability / ranking，虽然有 warning，但还不够“防误导”。**

### 7.2 Issue: Agents page message drifts from actual ranking/actionability behavior

- **风险等级**：P2
- **问题类型**：新发现问题
- **受影响文件**：
  - `ui/pages/agents.py`
  - `domains/weather/opportunity/service.py`

**代码事实**

- Agents 页面写明：“不参与主排序，也不参与 readiness 判定”（`ui/pages/agents.py:36-38`）；下方 runtime boundary 也写“不会进入机会主排序或 readiness gate”（`ui/pages/agents.py:111-115`）。
- 但实际 `agent_review_status` 会影响 `fill_probability`、`actionability_status` 和最终 `ranking_score`（`domains/weather/opportunity/service.py:57-61, 89-100, 328-337, 370-371`）。

**推断**

更准确的表述应该是：**agent 不进入 readiness gate，也不直接下单；但 agent review status 仍会影响 opportunity actionability 和 ranking。** 当前页面文案把这个影响说没了。

**修复方案**

- 改页面文案，明确：agent 只做 review/exception/human queue，不直接执行；但 `agent_review_status` 会作为 risk / fill-probability input 影响 opportunity assessment。  
- 如果产品意图是不让 agent 影响主排序，那就应该把 `agent_review_status` 从 ranking 公式移除，只保留 actionability gate。

**建议实施顺序**

- P2

**是否需要 migration / tests / docs sync**

- migration：不需要  
- tests：建议有  
- docs sync：必须

---

### 7.3 Issue: Agents page runtime config can be inaccurate under minimal UI env

- **风险等级**：P2
- **问题类型**：新发现问题
- **受影响文件**：
  - `ui/pages/agents.py`
  - `ui/runtime_env.py`
  - `start_asterion.sh`

**代码事实**

- Agents 页会直接显示 `QWEN_API_KEY` / `ALIBABA_API_KEY` 是否存在（`ui/pages/agents.py:117-124`）。
- 但 UI runtime env allowlist 不会注入这些 API key（`ui/runtime_env.py:12-24`，`tests/test_ui_runtime_env.py:38-40, 58-60`）。

**推断**

如果 UI 按推荐路径启动，Agents 页面很可能会显示“未配置 API key”，即使 runtime side 的 real weather chain loop 其实是配置好的。也就是说，这个 expander 目前更像“UI 进程 env 状态”，不是“runtime agent configuration 状态”。

**修复方案**

- 删除页面中对 API key presence 的直接展示。  
- 改为展示 runtime side 生成的 agent health artifact，例如：最近成功调用时间、最近失败时间、最近 provider/model、最近 invocation count。  
- UI 只展示“观测到的运行状态”，不展示进程级 env。

**建议实施顺序**

- P2

---

### 7.4 Issue: Fallback surfaces are now labeled, but still too easy to over-trust

- **风险等级**：P2
- **问题类型**：旧问题部分修复后的残余风险
- **受影响文件**：
  - `ui/data_access.py`
  - `ui/pages/home.py`
  - `ui/pages/markets.py`

**代码事实**

- `load_market_opportunity_data()` 会按 `ui_lite -> smoke_report -> weather_smoke_db` 回退（`ui/data_access.py:1037-1053`）。
- `load_operator_surface_status()` 会把 `smoke_report / weather_smoke_db` 标成 `degraded_source`（`ui/data_access.py:1418-1425`）。
- Markets / Home 页面也会展示 degraded warnings（`ui/pages/markets.py:117-123`，`ui/pages/home.py:37-44`）。

**结论**

这比旧版已经好很多；但当前 UI 仍然在 degraded source 下继续给出 `Top Opportunity / Actionable Markets / Ranking Score`。对经验不足的 operator 来说，这仍可能构成“看起来可以操作”的暗示。

**修复方案**

- degraded source 时，把 `actionability_status=actionable` 统一降级为 `review_required_degraded_source`；  
- Home / Markets 显式显示“当前排序仅供研究，不应用于 live decision”；  
- 或者至少在 degraded source 下，不显示 `Top Opportunity Score` 主 metric。

**建议实施顺序**

- P2

---

## 8. Top Remaining Risks

| 风险 | 等级 | 类型 | 受影响核心区域 | 当前判断 |
|---|---|---|---|---|
| SELL-side executable edge 方向错误 | **P0** | 新发现问题 | `domains/weather/opportunity/service.py` | 当前最直接的资本损失风险 |
| live submit 缺少 `ARMED=true` 强制检查 | **P1** | 旧问题残留 | `dagster_asterion/handlers.py` | 当前 constrained live 语义不一致 |
| submitter boundary 仍是 handler-centric | **P1** | 旧问题残留 | `live_submitter_v1.py` / `handlers.py` | 新入口很容易绕过当前边界 |
| capability manifest 被高估为 full truth-source | **P1** | 旧问题残留 | `capability_manifest_v1.py` / `readiness_evidence_v1.py` | 现在更像 capability snapshot，不是 runtime attestation |
| calibration 主链已接入，但统计治理弱且未进入 ranking | **P1** | 部分修复残留 | forecast / opportunity / ui | 会影响 fair value 与排序可靠性 |
| predicted-vs-realized / watch-only 指标过粗 | **P1** | 部分修复残留 | `ui_lite_db.py` / execution UI | 现在不足以做高质量 execution science |
| readiness GO 仍未证明 real submit lifecycle | **P1** | 旧问题残留 | readiness checker / runbook | readiness 证明的是 shadow path + approve smoke，而不是完整 real submit |
| degraded source 下 UI 仍易被过度信任 | **P2** | 残余风险 | `ui/data_access.py` / pages | 有 warning，但仍会显示 actionable/score |
| agent 文案与实际影响路径漂移 | **P2** | 新发现问题 | `ui/pages/agents.py` / opportunity service | 影响 operator 对 agent role 的理解 |
| deployment / writerd 仍偏单机开发态 | **P2** | 旧问题部分成立 | `start_asterion.sh` / `writerd.py` | 现在可用，但不应被误解为 production deployment |
| provider ack 不要求 external_order_id | **P2** | 新发现问题 | `live_submitter_v1.py` | 会削弱 reconciliation 和 audit |
| active remediation 文档仍有内部矛盾 | **P2** | 当前文档漂移 | `Post_P4_Remediation_Implementation_Plan.md` | 容易误导 reviewer |

---

## 9. Prioritized Implementation Plan

### 9.1 P0（立即做，1-3 天）

#### P0-1 修 SELL-side executable edge bug

- **改动模块**：`domains/weather/opportunity/service.py`、`asterion_core/ui/ui_lite_db.py`
- **动作**：把 executable edge 改为 side-aware；重新生成 `ui.market_opportunity_summary`
- **新增测试**：
  - SELL symmetry tests
  - ranking regression tests
  - Markets UI regression for SELL opportunities
- **是否需要 migration**：否
- **为什么是 P0**：这是直接扭曲盈利判断的资本风险

#### P0-2 给 live submit 增加 `ARMED=true` gate

- **改动模块**：`dagster_asterion/handlers.py`、`asterion_core/monitoring/capability_manifest_v1.py`、`ui/pages/system.py`
- **动作**：
  - live submit handler 加 arm-bit check
  - manifest 区分 `armed_env_present` 与 `armed_state`
  - System 页面显示 armed state
- **新增测试**：`tests/test_live_submitter_backend.py` 增加 `ARMED=false` blocked case
- **是否需要 migration**：否
- **为什么是 P0/P1 之间但建议按 P0 执行**：这是 constrained live 边界语义的一致性问题

### 9.2 P1（1-2 周）

#### P1-1 把 live boundary 内生化到 submitter service

- 增加 `BoundaryAttestation` 对象，`LIVE_SUBMIT` 必须携带并在 `SubmitterServiceShell` 内验证
- 如果可能，新建 `runtime.live_boundary_attestations` 审计表
- 给 provider 请求打上 attestation hash / request provenance

#### P1-2 Ranking v2：把 confidence / calibration / quality 真正纳入公式

- 新增 canonical ranking components：
  - `expected_executable_pnl_score`
  - `confidence_penalty`
  - `input_quality_penalty`
  - `ops_execution_readiness_score`
- 删除 `not_started` 的正向 readiness 加分
- 收敛 `ranking_score` / `opportunity_score` 双轨表达

#### P1-3 Calibration governance v2

- provider 增加 minimum sample count
- 实现 hierarchical fallback
- pricing context 记录 `sigma_source` / `calibration_sample_count`
- low coverage 时自动降权 / 触发 review_required

#### P1-4 Execution research loop v2

- 新增订单生命周期研究 projection
- 区分 `submitted / working / filled / resolved / cancelled / rejected`
- 拆分 `submission_capture_ratio` / `fill_capture_ratio` / `resolution_capture_ratio`
- Market / Execution 页面显示 miss reason distribution，而不是简单 0/1 capture

#### P1-5 Real submit lifecycle 验证与 runbook

- 为 `real_clob_submit` 新增 canonical smoke / reconciliation runbook
- 增加 fault injection tests：
  - provider timeout
  - accepted without external_order_id
  - duplicate ack
  - fill mismatch
  - partial fill / unresolved order

### 9.3 P2（2-4 周）

#### P2-1 UI fallback hardening

- degraded source 时降级 actionability
- Home 不再突出展示 `Top Opportunity Score`
- Markets 增加 “research-only under degraded source” 标记

#### P2-2 Agent boundary 清理

- 修正文案：agent 不进入 readiness gate，但会影响 review / actionability / fill probability
- 删除基于 UI env 的 API key presence 展示
- 改用 runtime artifact 展示 agent health

#### P2-3 Capability manifest / evidence v2

- policy hash
- signer identity
- submitter endpoint fingerprint
- semantic freshness
- dependency hashes

#### P2-4 `ui/data_access.py` 解耦

- 把 surface health judgement、overview synthesis、fallback orchestration 分拆成更小模块
- 长期目标：UI 尽量只读 `ui_lite + artifacts`

### 9.4 P3（后续）

#### P3-1 部署与运维硬化

- 把 `start_asterion.sh` 从开发入口升级成 service-managed deployment（systemd / container / supervisor）
- signer / submitter / writerd 进一步进程隔离
- 考虑 KMS / HSM / remote signer

#### P3-2 Queue / writer service 升级

- 保留 SQLite 作为当前单机方案没有问题，但若要扩大 live usage，需要规划 durable queue / writer service 边界

### 9.5 如果我是 CTO，下一阶段最该做的 10 件事

1. 修 SELL-side executable edge bug。  
2. 给 live submit 路径加 `ARMED=true` gate，并补负测试。  
3. 把 submitter boundary 从 handler 规则升级成 intrinsic attestation。  
4. 设计 manifest/evidence v2，加入 policy hash、armed state、signer identity、semantic freshness。  
5. 重构 ranking，使其纳入 confidence、calibration quality、mapping confidence、market quality。  
6. 取消 `not_started` 正向 readiness 加分。  
7. 做 execution research loop v2，覆盖 submitted / no-fill / partial / cancelled / resolved 全生命周期。  
8. 为 `real_clob_submit` 建真正的 lifecycle smoke / runbook / fault injection suite。  
9. 收口 UI fallback 和 Agents 页面文案 / runtime config 误导问题。  
10. 逐步把 signer / submitter / writerd 与 UI / orchestration 进程隔离，准备更高质量 constrained live。  

### 9.6 如果我是交易负责人，下一阶段最该做的 5 个赚钱能力增强动作

1. **修 SELL-side cost model**，否则所有 SELL 市场的 edge/ranking 都不可信。  
2. **把 ranking 改成 expected executable PnL × confidence × calibration quality**，而不是当前 heuristic score。  
3. **用真实历史订单/成交数据学习 fill probability / slippage / depth proxy**，取代固定常数。  
4. **把 source freshness / mapping confidence / calibration coverage 变成连续惩罚，而不是只做 bucket actionability**。  
5. **按 cohort 归因 predicted vs realized**：按 station/source/horizon/mapping confidence/liquidity/agent status 看真实赚钱能力，找出 alpha leakage 真正来源。  

---

## 10. Appendix: Key Code Areas Reviewed

### 10.1 顶层文档与 active implementation docs

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Asterion_Project_Plan.md`
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`
- `docs/10-implementation/phase-plans/Post_P4_Remediation_Implementation_Plan.md`
- `docs/10-implementation/runbooks/P4_Controlled_Live_Smoke_Runbook.md`
- `docs/analysis/11_Project_Full_Assessment.md`

### 10.2 核心代码与脚本

- `asterion_core/execution/live_submitter_v1.py`
- `asterion_core/monitoring/capability_manifest_v1.py`
- `asterion_core/monitoring/readiness_checker_v1.py`
- `asterion_core/monitoring/readiness_evidence_v1.py`
- `asterion_core/contracts/opportunity.py`
- `domains/weather/opportunity/service.py`
- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `asterion_core/ui/ui_lite_db.py`
- `ui/data_access.py`
- `ui/runtime_env.py`
- `ui/auth.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/pages/agents.py`
- `ui/pages/system.py`
- `asterion_core/storage/writerd.py`
- `dagster_asterion/handlers.py`
- `dagster_asterion/resources.py`
- `scripts/run_real_weather_chain_smoke.py`
- `start_asterion.sh`

### 10.3 Migrations

- `sql/migrations/0016_weather_mapping_calibration_quality.sql`
- 并结合 `0009` 到 `0015` 的 runtime / submit / chain-tx / external reconciliation migrations 复核当前执行骨架

### 10.4 重点 tests（本次实际执行并通过）

- `tests.test_ui_auth`
- `tests.test_ui_runtime_env`
- `tests.test_controlled_live_capability_manifest`
- `tests.test_live_submitter_backend`
- `tests.test_live_prereq_readiness`
- `tests.test_readiness_evidence_bundle`
- `tests.test_weather_opportunity_service`
- `tests.test_market_quality_screen`
- `tests.test_predicted_vs_realized_summary`
- `tests.test_post_trade_analytics`
- `tests.test_p4_closeout`
- `tests.test_p4_plan_docs`
- `tests.test_ui_pages`
- `tests.test_ui_phase4_console`
- `tests.test_ui_data_access`

---

## 最终结论（供管理层快速阅读）

- **当前 Asterion 架构总体是合理的，而且比旧评估时显著更稳健。**
- **旧报告里关于 UI auth、UI env 暴露、状态过度宣称、缺少 canonical opportunity contract、缺少 capability manifest / evidence / real submitter 的批评，现在多数都已经不再成立。**
- **当前最危险的不是“系统没有边界”，而是边界已经成形，但还有关键缺口：live submit arm-bit 不一致、submitter 边界未内生化、capability manifest 被高估。**
- **当前离“更高质量 constrained live”最远的，不是再多做一个页面，而是：真正把 execution-aware ranking、calibration governance、order lifecycle research loop 做对。**
- **当前离“可持续赚钱”最远的短板，仍然是交易经济模型，而不是基础设施工程。**
- **如果只允许做一件事，我会先修 SELL-side executable edge bug；如果允许做两件事，第二件是把 live submit 的 arm-bit 与 boundary attestation 做实。**
