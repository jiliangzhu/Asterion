# Asterion Deep Audit and v2.0 Planning Blueprint (Revised)

## 1. Revision Summary

这份修订版不是从零重写，而是基于现有 `Asterion_Deep_Audit_and_v2_Planning_Blueprint.md`，重新核对当前仓库 HEAD 的入口文档、关键代码和代表性测试之后，做出的严格修订。

本次修订重点处理了两类问题：

1. **repo / docs / UI truth-source 状态需要按当前 HEAD 重写**。原报告里一些关于“仓库已经切到 v2.0 planning”的表述方向是对的，但不够完整，也没有把“入口文档已切换、UI/shared constants/startup surface 仍旧停留在旧状态”的 split-brain 写透。
2. **原报告对 allocator / budget / position limit 的持久化落点给得过快**。直接提出 `risk.portfolio_budgets` / `risk.position_limits`，和当前仓库的 persistence discipline 不完全一致，容易让 v2.0 过早发散出一条新的 schema 体系。

这次重新核对后的总判断是：

- **原报告的大方向仍然成立**：当前系统的主矛盾不是“有没有基础设施”，而是 **ranking 的真钱化程度、capital allocation、calibration freshness / ops、execution feedback closure、operator throughput**。
- **原报告的一些安全结论需要收敛修正**：当前 HEAD 的 constrained live boundary 比原报告写得更强，submitter attestation 和 signer secret boundary 都已经明显前进，不应再把它们当成 v2.0 的首要重构对象。
- **当前最需要优先修的，不是 live boundary 再扩写，而是 delivery / truth-source / read-model contract 漂移**。最典型、最有代表性的当前真实问题，就是 `ui.daily_review_input` 的 registry-builder contract 失配，已经能直接打断 UI-lite build，并实打实导致两个执行基础测试失败。
- **v2.0 仍应围绕赚钱能力展开**，但要更克制：
  - 强化 execution economics
  - 补 allocator / sizing
  - 把 calibration ops 做成可靠的冷路径运营能力
  - 提升 operator workflow throughput
  - 修 read-model / truth-source / delivery risk
  - **而不是**先去扩大 live boundary，或者轻率引入新的 `risk.*` schema

本次我重新核对了你指定的关键文件，并定向重跑了代表性测试。当前我实际观察到的测试结果是：

- 代表性测试集 **120 passed, 2 failed, 3 subtests passed**
- 当前真实失败点集中在：
  - `tests/test_execution_foundation.py::test_ui_execution_ticket_summary_surfaces_reconciliation_status`
  - `tests/test_execution_foundation.py::test_ui_execution_exception_summary_surfaces_reconciliation_mismatches`
- 两个失败都不是 execution ledger 主链坏掉，而是 **`ui.daily_review_input` 缺少 `item_id`**，导致 UI-lite build 失败，进而拖累 execution summary 相关测试。

这意味着：**当前 HEAD 最需要优先解决的不是“系统不会跑”，而是“系统已经能跑，但交付面、truth-source 和 read-model contract 还不够稳”。**

---

## 2. What From the Original Report Still Holds

下面这些原报告中的核心判断，重新核对后我认为仍然成立，而且仍然应该保留为 v2.0 的主线判断。

### 2.1 主矛盾已经不是“有没有基础设施”

**原报告结论**  
当前系统的主矛盾不是缺 execution plumbing，而是：

- ranking 的经济化程度
- capital allocation / sizing
- calibration freshness
- execution feedback closure
- operator throughput

**当前代码事实**  
当前 HEAD 已经具备：

- `ranking_score` 的统一主排序接线
- execution priors materialization
- execution feedback penalty 回写 `ranking_score`
- calibration v2 / threshold probability quality 主链接线
- constrained live submitter / signer / chain-tx 的真实边界

`domains/weather/opportunity/service.py` 已经不是“edge + confidence 的轻量打分器”，而是把：

- executable edge
- execution priors
- calibration / threshold probability / regime quality
- feedback penalty

都接进了最终 `ranking_score`。

**当前测试事实**  
- `tests/test_ranking_score_v2.py` 通过，证明 ranking v2 确实存在且接入 `why_ranked_json`
- `tests/test_execution_feedback_loop.py` 通过，证明 feedback penalty 会压低最终 ranking
- `tests/test_calibration_profile_v2.py` 和 `tests/test_threshold_probability_profile.py` 通过，证明 calibration v2 和 threshold probability profile 已经实装

**当前文档事实**  
README、AGENTS、入口 overview 文档都把当前阶段定义成 `v2.0 planning`，且 README 已明确写出 execution economics、feedback-backed ranking、calibration v2、truth-source hardening 都已落地。

**修正后的结论**  
原报告这一条仍然成立，甚至比原报告更明确：**当前 Asterion 的主矛盾已经是盈利链条质量，而不是基础设施存在性。**

**对 v2.0 规划的影响**  
v2.0 不应再把大量篇幅浪费在“证明 plumbing 存在”上，而应直接把主要 workstreams 对准：

- execution economics
- allocator / sizing
- calibration ops
- operator workflow throughput
- read-model / delivery hardening

---

### 2.2 `ui.daily_review_input` contract drift 仍然是真实问题

**原报告结论**  
原报告指出 UI-lite / read-model contract 存在真实漂移，尤其是：

- `ui.daily_review_input` 缺少 `item_id`
- `tests/test_execution_foundation.py` 中有两个失败与之相关

**当前代码事实**  
`asterion_core/ui/read_model_registry.py` 当前明确把 `ui.daily_review_input` 注册为：

- `primary_key_columns=("item_id",)`
- `required_columns=("item_id",)`

但 `asterion_core/ui/ui_lite_db.py` 构建 `ui.daily_review_input` 时，只选出了：

- `run_id`
- `ticket_id`
- `request_id`
- `wallet_id`
- `strategy_id`
- `market_id`
- `execution_result`
- `reconciliation_status`
- `summary_json`

并**没有生成 `item_id`**。

**当前测试事实**  
本次重跑代表性测试时，当前真实失败的两个用例就是：

- `test_ui_execution_ticket_summary_surfaces_reconciliation_status`
- `test_ui_execution_exception_summary_surfaces_reconciliation_mismatches`

失败根因是 `build_ui_lite_db_once(...)` 报错：

- `ui lite table ui.daily_review_input missing required columns: item_id`

**当前文档事实**  
README 对 `ui.read_model_catalog` / `ui.truth_source_checks` 的描述是积极的，这个方向本身没问题；问题在于 **catalog 和 checks 存在，并不等于 builder contract 已经完全收口**。

**修正后的结论**  
原报告这一条 **完全仍然成立**，而且应该上调其优先级：这是当前 HEAD 最明确、最可重现、最影响交付稳定性的真实缺口之一。

**对 v2.0 规划的影响**  
这件事不应放到“后面顺手修”。它应该进入 **Phase 0 / immediate fix**，因为它直接影响：

- UI-lite build 稳定性
- execution summary read-model 可信度
- truth-source / read-model contract 的说服力

---

### 2.3 calibration refresh 仍然是 manual job，而不是 default scheduled

**原报告结论**  
原报告认为 calibration v2 已接主链，但 refresh 仍未 fully operationalized，还是 manual job。

**当前代码事实**  
`dagster_asterion/job_map.py` 当前定义：

- `weather_execution_priors_refresh`：`mode="scheduled"`，默认 schedule 为 nightly，且 enabled by default
- `weather_forecast_calibration_profiles_v2_refresh`：`mode="manual"`，`default_schedule_key=None`

**当前测试事实**  
`tests/test_cold_path_orchestration.py` 通过，并锁定了：

- `weather_execution_priors_refresh` 已有默认 schedule
- calibration profiles v2 refresh 已进入 job map，但没有默认 schedule

**当前文档事实**  
README 已经描述 calibration v2 已进入主链，但没有宣称 calibration profiles refresh 已是 default scheduled。这个文档状态与当前代码基本一致。

**修正后的结论**  
原报告这一条 **仍然成立**。当前 calibration 的问题不是“没有 v2 profile”，而是 **materialization / freshness / 运营调度还没有默认自动化**。

**对 v2.0 规划的影响**  
calibration v2 workstream 不应再作为“建模概念”推进，而应作为 **ops + freshness + quality control** workstream 推进。

---

### 2.4 UI truth-source / phase / version drift 仍然真实存在

**原报告结论**  
原报告认为 UI truth-source 机制虽然已存在，但 phase/version/status 仍有漂移。

**当前代码事实**  
当前 HEAD 中，入口 docs 已切到 `v2.0 planning`，但 UI 和 shared constants 仍存在明显旧状态：

- `asterion_core/ui/surface_truth_shared.py`
  - `TRUTH_SOURCE_DOC = "docs/10-implementation/phase-plans/Post_P4_Remediation_Implementation_Plan.md"`
  - `CURRENT_PHASE_STATUS = "post-P4 remediation active / closeout pending objective verification"`
- `ui/app.py`
  - header badge 仍写 `Post-P4 remediation active`
  - subcopy 仍写 `closeout pending objective verification`
  - sidebar caption 仍写 `Asterion v1.2`
- `ui/pages/home.py`
  - caption 仍写 `post-P4 remediation active / closeout pending objective verification`
- `start_asterion.sh`
  - `print_boundary_summary()` 仍输出 `remediation in progress`

**当前测试事实**  
- `tests/test_phase9_wording.py`、`tests/test_p4_plan_docs.py` 会锁入口文档的 v2 planning 状态
- 但 `tests/test_operator_truth_source.py` 只是验证 loader 返回的值与 `CURRENT_PHASE_STATUS` / `TRUTH_SOURCE_DOC` 常量一致；它**不会**发现这些常量本身已落后于 docs

**当前文档事实**  
README、AGENTS、Documentation_Index、Project Plan、Roadmap、Implementation Index 都已经切到 `P4 accepted; post-P4 remediation accepted; v2.0 planning`。

**修正后的结论**  
原报告这一条 **完全仍然成立**，而且这次可以更明确地写成：

> 当前不是 docs 还没切，而是 docs 已经切了；真正没切的是 UI shared constants、startup copy 和部分 operator surface 文案。

**对 v2.0 规划的影响**  
truth-source cleanup 仍然是 v2.0 的第一批工作，而且它的对象应该更精确：

- 不是“重写 docs 叙事”
- 而是“让 UI / startup / shared constants / tests 与入口 docs 对齐”

---

### 2.5 v2.0 重点仍应放在 execution economics / allocator / calibration ops / operator workflow / read-model hardening

**原报告结论**  
v2.0 的重点应放在：

- execution economics
- allocator / sizing
- calibration ops
- operator workflow
- read-model / delivery hardening

而不是扩大 live boundary。

**当前代码事实**  
当前 HEAD 已经有：

- feedback-backed ranking
- calibration v2
- execution priors scheduled materialization
- truth-source checks / read-model catalog
- stronger controlled-live boundary

也就是说，当前最缺的确实不是“再造一个 live submitter”，而是：

- 让 `ranking_score` 更接近真实资本部署优先级
- 让机会从 unit-opportunity 升级成 size-aware decision
- 让 calibration freshness 运营化
- 让 operator 更快处理更多高质量机会

**当前测试事实**  
代表性测试证明：

- 安全边界主链多数已通过
- economics 主链已经接线
- calibration v2 已接线
- 当前最显性的失败不在 live boundary，而在 read-model contract

**当前文档事实**  
V2 入口文档当前仍只是 planning placeholder，并未锁死具体实现路线；因此现在更需要的是 **谨慎、贴近 HEAD 的 v2 blueprint**。

**修正后的结论**  
原报告这一条 **仍然是最值得保留的总方向**。

**对 v2.0 规划的影响**  
v2.0 的前几批 workstreams 应继续聚焦在赚钱能力与交付稳定性，而不是扩大真钱边界。

---

## 3. What Needs Correction

下面这些点，原报告的方向不一定错，但已经需要按当前 HEAD 做严格改写。

### 3.1 关于“仓库已经切到 v2.0 planning”的表述需要更精确

**原报告结论**  
原报告大体上已经把仓库状态写成 `v2.0 planning`，但没有完整区分：

- 哪些入口 docs 已切换
- 哪些 supporting design docs 只是 historical accepted references
- 哪些 UI/shared constants/startup surface 仍停留在旧状态
- 哪些 tests 还会把旧状态锁住，或者根本没锁住 drift

**当前代码事实**  
UI / startup / shared truth constants 仍有旧状态表达，见上节。

**当前测试事实**  
- docs tests 正确地锁住了 `v2.0 planning`
- operator truth-source test 没有真正检测 UI vs docs drift

**当前文档事实**  
- 入口 docs 已切到 `v2.0 planning`
- `Post_P4_Remediation_Implementation_Plan.md` 已归档为 historical accepted remediation record
- `V2_Implementation_Plan.md` 明确只是 planning placeholder，不是完整 implementation contract
- 边界 / execution economics / calibration / operator truth / UI read model design docs 都是 supporting design，且已被标记为 historical accepted references，不再是 active implementation truth-source

**修正后的结论**  
对当前 HEAD 更准确的写法应该是：

> 当前仓库的“文档入口层”已经切换到 `v2.0 planning`；`P4` 和 post-P4 remediation 已被归档为 historical accepted records。当前真正落后的不是入口 docs，而是 UI/shared constants/startup surfaces 以及对应的 phase-status test discipline。

**对 v2.0 规划的影响**  
truth-source 修订应聚焦在 UI / startup / shared constants / tests，不应再把“重写入口 docs 状态”当成主要任务。

---

### 3.2 原报告把 submitter attestation 写得过弱，需要修正

**原报告结论**  
原报告把 submitter attestation 描述为偏“进程内 / caller-trusted 的 audit artifact”，认为它还不是强闭环 capability proof。

**当前代码事实**  
当前 HEAD 已经有完整的 attestation v2：

- `asterion_core/contracts/live_boundary.py`
  - attestation kind v2
  - issuer
  - `issued_at` / `expires_at`
  - `nonce`
  - `decision_fingerprint`
  - `attestation_mac`
  - TTL = 300 秒
- `asterion_core/execution/live_submitter_v1.py`
  - backend 会校验 attestation kind / issuer / request / wallet / mode / backend / fingerprint
  - 校验是否过期
  - 校验 decision fingerprint
  - 用 `ASTERION_CONTROLLED_LIVE_SECRET_ATTESTATION_MAC_KEY` 校验 HMAC
  - 校验 attestation 是否已写入 DB
  - 校验 persisted attestation 是否匹配
  - 校验 `runtime.live_boundary_attestation_uses`，实现 claim-once / reuse rejection

**当前测试事实**  
- `tests/test_live_submitter_backend.py` 通过，并覆盖 attestation reuse rejection
- `tests/test_submitter_boundary_attestation.py` 通过，并覆盖 v2 attestation / MAC 相关行为

**当前文档事实**  
README 已经把 controlled-live signer / tx / boundary 的收口状态写成已落地能力。

**修正后的结论**  
原报告这一段需要收敛：

> 当前 HEAD 的 submitter boundary 已经不是“弱 audit artifact”级别，而是一个同信任域内、带 TTL / nonce / fingerprint / HMAC / persistence / consume-once 的 boundary token 方案。

它仍然**不是**独立 control-plane / KMS / capability service 级别的零信任边界，但对于当前 `operator console + constrained execution infra` 的 manual-only 范围，已经比原报告描述的更强。

**对 v2.0 规划的影响**  
- 需要**下调**“submitter boundary redesign”优先级
- v2.0 不应把时间优先花在再造一套 live submitter 架构
- 更合理的做法是：保持边界收紧、不扩 scope，把精力转向赚钱链和 delivery hardening

---

### 3.3 原报告关于 signer `private_key_env_var` 的结论已经过时

**原报告结论**  
原报告认为 signer 仍信任 caller 提供的 `private_key_env_var`，这是一个残余 secret boundary 风险。

**当前代码事实**  
当前 `asterion_core/signer/signer_service_v1.py` 已经：

- 若 payload 里出现 `private_key_env_var`，直接返回 `controlled_live_private_key_env_var_forbidden`
- 真正的 secret scope 是通过 `wallet_id` 调用 `controlled_live_wallet_secret_env_var(wallet_id)` 推导

**当前测试事实**  
`tests/test_signer_shell.py` 当前通过，并覆盖了 caller 提供 `private_key_env_var` 会被拒绝的路径。

**当前文档事实**  
README 也已明确写出 controlled-live tx signer 只按 `wallet_id` 推导 secret scope。

**修正后的结论**  
原报告这一点已经过时，应明确删除或改写为：

> signer payload env-var injection 问题已经修掉，不再是当前 v2.0 的主风险。

**对 v2.0 规划的影响**  
- 不要再把 signer secret boundary 当成 v2.0 的主要重构对象
- 更重要的是保持这条边界不被后续新功能回退，并继续由测试锁住

---

### 3.4 原报告对 execution feedback closure 的描述需要更新

**原报告结论**  
原报告把 execution science / feedback 更多写成“已有描述性 analytics，但尚未闭环回灌 ranking”。

**当前代码事实**  
当前 `domains/weather/opportunity/service.py` 已经做到了：

- `ranking_v2` decomposition
- `pre_feedback_ranking_score`
- 从 `ExecutionPriorSummary.feedback_prior` 读取 `feedback_penalty`
- 最终 `ranking_score = pre_feedback_ranking_score * (1 - feedback_penalty)`
- `why_ranked_json` 明确包含 `feedback_penalty`、`feedback_status`、`feedback_scope_breakdown`

当前 `domains/weather/opportunity/execution_feedback.py` 也已经不是单纯报表，而是定义了：

- feedback penalty 计算
- feedback status
- cohort prior 聚合
- miss / distortion buckets

**当前测试事实**  
`tests/test_execution_feedback_loop.py` 当前通过，能证明 feedback penalty 会真实压低 ranking。

**当前文档事实**  
README 已明确写 execution economics 已进入 feedback-backed ranking 阶段。

**修正后的结论**  
原报告这一段需要改写为：

> execution feedback 已经进入主排序，不应再被表述成“还没接进去”；当前更准确的问题是：feedback 已接入，但 cohort 特征空间、经验模型颗粒度和 capital-aware 转化还不够强。

**对 v2.0 规划的影响**  
v2.0 的重点应该从“把 feedback 接进来”改成：

- 扩 execution priors / feedback 的特征空间
- 提高 cohort 经验模型颗粒度
- 让 ranking 更 capital-aware
- 把 operator surface 做成能直接消费这些 feedback 信号

---

### 3.5 原报告里的 `risk.*` schema 建议需要严格收回

**原报告结论**  
原报告建议为 allocator / budget / position limit 引入：

- `risk.portfolio_budgets`
- `risk.position_limits`

**当前代码事实**  
当前仓库 migration 和 persistence discipline 非常清晰：

- `trading.*`：canonical trading ledger / inventory / exposure / reconciliation facts
- `runtime.*`：run-time decision / attempt / materialization / audit artifacts
- `weather.*`：domain serving / feature materialization
- `ui.*`：read models
- 当前并不存在 `risk.*` schema

同时，`asterion_core/risk/portfolio_v3.py` 虽然模块名带 `risk`，但其逻辑依赖的 canonical persistence truth-source 仍然是：

- `trading.inventory_positions`
- `trading.reservations`
- `trading.fills`
- `trading.exposure_snapshots`

也就是说，当前 repo 已经把“风险 / 库存 / 预约 / 持仓”事实放在 `trading.*` 里，而不是独立 schema。

**当前测试事实**  
代表性 execution foundation / reconciliation / portfolio 相关测试都建立在现有 `trading.*` + `runtime.*` 纪律之上，没有任何测试暗示需要一个新 `risk.*` schema。

**当前文档事实**  
入口 docs 和现有 implementation truth-source 也没有为 `risk.*` 打开 canonical persistence 入口。

**修正后的结论**  
原报告这一条需要明确修正为：

> v2.0 **不应默认引入新的 `risk.*` schema**。

更合理、也更克制的持久化策略是：

1. **allocator / sizing 的每次运行结果、limit check、budget usage 决策**，优先落在 `runtime.*`，因为它们本质上是 decision artifacts，而不是 canonical ledger facts。  
   例如可考虑：
   - `runtime.allocation_decisions`
   - `runtime.position_limit_checks`
   - `runtime.capital_allocation_runs`

2. **如果未来确实形成长期稳定、operator-managed 的 canonical capital policy**，再考虑将其落入 `trading.*`，例如：
   - `trading.allocation_policies`
   - `trading.position_limit_policies`

3. 只有当 Asterion 将来真的演化出一个独立、跨域、具有自己生命周期和 truth-source 的风险子系统时，才有理由讨论新 `risk.*` schema。当前 HEAD 还没有到这一步。

**对 v2.0 规划的影响**  
- allocator 仍然应该做，但 persistence plan 必须更克制
- v2.0 应避免一上来引入 `risk.*`，以免打破当前清晰的 canonical persistence discipline
- allocator 应该被设计成：
  - 读取 `ranking_score`
  - 读取 `execution_feedback`
  - 读取 `portfolio_v3` 对接的 `trading.*` inventory truth
  - 输出 runtime 级 sizing / budget / limit decisions

---

## 4. Updated Code Facts

下面是本次复核后、对 v2.0 规划最重要的当前代码事实。

### 4.1 入口 docs 已经切换到 `v2.0 planning`

当前 HEAD 的入口文档层已经统一切换到：

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Asterion_Project_Plan.md`
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`

这些文件当前都把仓库阶段表达为：

- `P4 accepted; post-P4 remediation accepted; v2.0 planning`

并把：

- `docs/10-implementation/phase-plans/V2_Implementation_Plan.md`

作为 **active planning entry**。

### 4.2 `V2_Implementation_Plan.md` 目前还是 planning placeholder

当前 `docs/10-implementation/phase-plans/V2_Implementation_Plan.md` 明确写的是：

- `planned / not yet active implementation contract`
- 只作为 `v2.0` 的 planning placeholder
- 不承载完整的 implementation contract

这意味着：

- repo 已经切到 v2.0 入口
- 但 **v2.0 还没有正式进入细化 implementation contract 阶段**

### 4.3 `Post_P4_Remediation_Implementation_Plan.md` 已归档为历史记录

当前 `Post_P4_Remediation_Implementation_Plan.md` 明确写为：

- `archived accepted historical remediation record`
- 当前不再视为 active implementation entry

这比原报告更清楚：**post-P4 remediation 不是“仍在进行中”，而是已 accepted 的历史 remediation 记录。**

### 4.4 UI / startup / shared truth surfaces 仍停留在旧状态

当前 HEAD 仍存在明确 phase-status drift：

- `asterion_core/ui/surface_truth_shared.py`
  - `TRUTH_SOURCE_DOC` 仍指向 `Post_P4_Remediation_Implementation_Plan.md`
  - `CURRENT_PHASE_STATUS` 仍写 `post-P4 remediation active / closeout pending objective verification`
- `ui/app.py`
  - 页面 header 仍写 `Post-P4 remediation active`
  - 当前副文案仍解释为 `closeout pending objective verification`
  - sidebar 仍写 `Asterion v1.2`
- `ui/pages/home.py`
  - 首页 caption 仍写 post-P4 remediation active
- `start_asterion.sh`
  - `print_boundary_summary()` 仍输出 `remediation in progress`

### 4.5 constrained live boundary 已经明显强于旧版判断

当前 HEAD 的 submitter / signer 边界是：

- submitter attestation v2：TTL + nonce + HMAC + fingerprint + persisted record + claimed use
- signer：不接受 caller 提供 `private_key_env_var`，只按 `wallet_id` 推导 secret scope
- start script：UI 默认只监听 localhost，public bind 需要显式 opt-in

当前最准确的边界评价不是“危险”，而是：

> 对 manual-only constrained live 已经相当克制和谨慎；当前主要问题不是边界设计不存在，而是不要在 v2.0 里过早扩大边界 scope。

### 4.6 ranking v2 / execution feedback / calibration v2 都已经是真实主链能力

当前 `domains/weather/opportunity/service.py` 已经做到了：

- executable edge
- calibration / freshness / mapping / market quality / bias / threshold probability / regime stability multiplier
- execution prior summary 接入
- ranking v2 decomposition
- feedback penalty 回写 final `ranking_score`
- `why_ranked_json` 暴露主排序解释

当前 `domains/weather/opportunity/execution_feedback.py` 已经提供：

- feedback penalty
- feedback status
- cohort prior 聚合
- miss / distortion 分类

当前 `domains/weather/forecast/calibration.py` 已经提供：

- calibration profile v2
- threshold probability profile
- corrected mean / std dev summary
- regime / bias / threshold quality summary

### 4.7 当前 orchestration 的主要短板不是 job map 不存在，而是 calibration v2 refresh 还没默认 schedule

当前 `dagster_asterion/job_map.py` 已经具备：

- execution priors nightly refresh
- 其它 weather cold-path jobs 的默认调度

但 calibration profiles v2 refresh 仍是 manual。这个是一个 **ops / freshness bottleneck**，不是 orchestrator 缺位。

### 4.8 `ui.read_model_catalog` / `ui.truth_source_checks` 已落地，但还没有兜住所有 builder drift

当前 HEAD 已有：

- `asterion_core/ui/read_model_registry.py`
- `ui.read_model_catalog`
- `ui.truth_source_checks`

这是好基础。

但 `ui.daily_review_input` 的例子说明：

- catalog / checks / registry 的存在不等于所有 builder 都已经对齐
- 当前 delivery risk 已从“没有机制”转向“机制存在但 contract 仍可漂移”

---

## 5. Updated Test Facts

### 5.1 本次定向重跑的测试事实

本次我定向重跑了你指定的一组代表性测试，结果如下：

- `tests/test_execution_foundation.py`
- `tests/test_operator_truth_source.py`
- `tests/test_ui_read_model_catalog.py`
- `tests/test_truth_source_checks.py`
- `tests/test_ranking_score_v2.py`
- `tests/test_execution_feedback_loop.py`
- `tests/test_calibration_profile_v2.py`
- `tests/test_threshold_probability_profile.py`
- `tests/test_cold_path_orchestration.py`
- 以及补充复核：
  - `tests/test_live_submitter_backend.py`
  - `tests/test_submitter_boundary_attestation.py`
  - `tests/test_signer_shell.py`
  - `tests/test_p4_plan_docs.py`
  - `tests/test_p4_closeout.py`
  - `tests/test_execution_priors_materialization.py`
  - `tests/test_phase9_wording.py`

当前观察到的总结果是：

- **120 passed**
- **2 failed**
- **3 subtests passed**

### 5.2 当前真实失败点比原报告更集中

当前真实失败点只集中在：

- `ui.daily_review_input` builder / registry contract drift

这说明：

- 当前 HEAD 的很多基础能力已经不是“普遍性不稳定”
- 真正危险的是 **局部但关键的 delivery / read-model 漂移**

### 5.3 docs tests 已经把入口状态锁到 `v2.0 planning`

当前通过的 docs / wording 相关测试表明：

- README / roadmap / plan docs 已经把状态切到 `P4 accepted; post-P4 remediation accepted; v2.0 planning`
- `Post_P4_Remediation_Implementation_Plan.md` 已被当作历史 accepted record

### 5.4 operator truth-source test 目前是“自洽”，不是“与 docs 对齐”

`tests/test_operator_truth_source.py` 当前验证的是：

- UI loader 返回的 truth-source 值等于 shared constant

它没有验证：

- shared constant 是否等于当前入口 docs 的真实 phase status
- startup copy / app header / home caption 是否与 docs 一致

这意味着：

- 当前 operator truth-source test **并没有真正兜住 phase drift**
- 它更像是“shared constant consistency test”，不是“repo truth-source alignment test”

### 5.5 ranking v2 / feedback loop / calibration v2 都已经被测试证明存在

这点非常重要。当前通过的测试说明：

- ranking v2 不是 aspirational
- execution feedback penalty 不是 aspirational
- calibration profile v2 和 threshold probability profile 不是 aspirational
- orchestration 也不是 aspirational

因此 v2.0 规划应该建立在“这些都已经存在”的基础上，而不是把它们当成 v2 greenfield。

---

## 6. Updated Documentation Facts

### 6.1 已经切到 `v2.0 planning` 的入口 docs

当前 HEAD 中，以下文档已经明确切到 `v2.0 planning`：

- `README.md`
- `AGENTS.md`
- `docs/00-overview/Documentation_Index.md`
- `docs/00-overview/Asterion_Project_Plan.md`
- `docs/00-overview/DEVELOPMENT_ROADMAP.md`
- `docs/10-implementation/Implementation_Index.md`

这些文件共同表达的当前 repo 状态是：

- `P4 accepted; post-P4 remediation accepted; v2.0 planning`

### 6.2 已经归档成历史 accepted record 的文档

当前 HEAD 已经把以下内容归档成 historical accepted record / historical accepted reference：

- `P0` 到 `P4` phase plans
- `Post_P4_Remediation_Implementation_Plan.md`
- supporting designs，例如：
  - `Controlled_Live_Boundary_Design.md`
  - `Execution_Economics_Design.md`
  - `Forecast_Calibration_v2_Design.md`
  - `Operator_Console_Truth_Source_Design.md`
  - `UI_Read_Model_Design.md`

这些文档仍然有价值，但不应再被当成 active implementation truth-source。

### 6.3 `V2_Implementation_Plan.md` 目前还不是完整 implementation contract

当前 HEAD 中，v2 文档是：

- 当前 active planning entry
- 但尚未扩展为完整 implementation contract

这意味着本次修订版蓝图的价值，就是给后续把它落成真正 v2 实施计划提供更准确的基础。

### 6.4 仍停留在旧状态表达的 surfaces

当前仍明显停留在旧状态表达的地方包括：

- `asterion_core/ui/surface_truth_shared.py`
- `ui/app.py`
- `ui/pages/home.py`
- `start_asterion.sh`

这些地方需要在 v2 的第一批修正中被收口。

### 6.5 仍把旧状态锁死或没有锁住 drift 的 tests

严格说，当前并不是“tests 把旧状态锁死了”，而是：

- docs tests 已正确锁住 `v2.0 planning`
- operator truth-source tests **没有**真正兜住 docs vs UI drift

因此这部分更准确的说法是：

> 当前 tests 对 repo truth-source 的覆盖是不对称的：入口 docs 被锁得比较好，UI truth-source alignment 还没被真正锁住。

---

## 7. Corrected Critical Findings

下面是基于当前 HEAD 的修订版关键发现。为了方便直接指导 v2.0 开发，我尽量按统一格式写。

### 7.1 Phase Status Split-Brain：入口 docs 已切 v2，UI / startup / shared constants 仍停在旧阶段

- **优先级**：P1
- **类型**：Testing / UX / Delivery
- **性质**：历史遗留问题，当前仍真实存在
- **受影响文件**：
  - `README.md`
  - `AGENTS.md`
  - `docs/00-overview/*`
  - `docs/10-implementation/Implementation_Index.md`
  - `docs/10-implementation/phase-plans/V2_Implementation_Plan.md`
  - `asterion_core/ui/surface_truth_shared.py`
  - `ui/app.py`
  - `ui/pages/home.py`
  - `start_asterion.sh`
  - `tests/test_operator_truth_source.py`

**原报告结论**  
原报告认为 phase / version / truth-source 仍有漂移，这点是对的，但没有把当前 HEAD 的 split 写得足够精确。

**当前代码事实**  
- 入口 docs 已切 `v2.0 planning`
- UI shared constants / app / home / startup copy 仍是 `post-P4 remediation active`

**当前测试事实**  
- docs tests 锁住了 v2 status
- operator truth-source test 只是自洽，不会发现 drift

**当前文档事实**  
`V2_Implementation_Plan.md` 只是 planning placeholder；`Post_P4_Remediation_Implementation_Plan.md` 已归档。

**我的推断**  
这不是语义小瑕疵，而是当前 repo 最典型的 truth-source split-brain。它会影响：

- operator 对当前阶段的理解
- reviewer 对项目状态的理解
- 后续 v2 文档扩写时的统一性

**风险或缺口描述**  
系统对外（docs）已经说“v2.0 planning”；系统对内（UI/startup/shared constants）还在说“post-P4 remediation active”。

**为什么重要**  
这会持续制造评审误差、operator 误读和 implementation drift。

**对稳定、规模化、高置信赚钱的影响**  
间接但真实：当系统状态表达不稳定时，优先级会被旧阶段叙事拖偏，影响 v2 的交付节奏。

**推荐修复方向**  
1. 统一 phase/status single source
2. 更新 `surface_truth_shared.py`
3. 更新 `ui/app.py` header / sidebar / subcopy
4. 更新 `ui/pages/home.py` caption
5. 更新 `start_asterion.sh` boundary summary copy
6. 新增 cross-surface truth-source alignment tests

**需要改哪些模块**  
- `asterion_core/ui/surface_truth_shared.py`
- `ui/app.py`
- `ui/pages/home.py`
- `start_asterion.sh`
- `tests/test_operator_truth_source.py`
- 新增 docs-vs-ui alignment test

**需要哪些测试**  
- docs status == shared status == app header == home caption == startup copy
- `TRUTH_SOURCE_DOC` 指向当前 active planning entry，而不是 historical remediation doc

**是否需要 migration**  
不需要。

**是否需要文档同步**  
需要。

**推荐实施顺序**  
1

---

### 7.2 `ui.daily_review_input` registry-builder drift 目前是当前 HEAD 最明确的 read-model contract defect

- **优先级**：P1
- **类型**：Architecture / Testing / Delivery
- **性质**：历史遗留问题，当前仍真实存在
- **受影响文件**：
  - `asterion_core/ui/read_model_registry.py`
  - `asterion_core/ui/ui_lite_db.py`
  - `tests/test_execution_foundation.py`

**原报告结论**  
原报告指出 `ui.daily_review_input` 缺 `item_id` 并导致两个 execution foundation 测试失败。

**当前代码事实**  
registry 要求 `item_id`，builder 没产出 `item_id`。

**当前测试事实**  
当前两处真实失败都由此引起。

**当前文档事实**  
文档层强调 read-model catalog / truth_source_checks 已落地；这并不与当前 defect 矛盾，反而说明“机制有了，但 contract 还会飘”。

**我的推断**  
这类问题如果不优先修，会持续削弱对 UI-lite build 的信任，进而削弱 execution science / operator surface 的可信度。

**风险或缺口描述**  
一个 read-model table 的主键 contract 和 builder 漂移，会以非常隐蔽的方式打断更高层 surface 测试。

**为什么重要**  
这已经是当前 HEAD 的真实红灯，不是潜在风险。

**对稳定、规模化、高置信赚钱的影响**  
直接影响 operator surface 交付稳定性，进而影响复盘和决策支持。

**推荐修复方向**  
优先做最小正确修复：

- 在 `ui.daily_review_input` builder 中稳定生成 `item_id`
- 推荐用 deterministic id，例如基于 `run_id + ticket_id + request_id` 的 `stable_object_id`
- 然后补回整条 execution summary acceptance coverage

**需要改哪些模块**  
- `asterion_core/ui/ui_lite_db.py`
- 如有必要，少量调整 `read_model_registry.py`

**需要哪些测试**  
- 当前两条失败用例转绿
- 新增 `ui.daily_review_input` schema contract test
- 新增 UI-lite full build acceptance test

**是否需要 migration**  
不需要（若只是在 build 时生成列）。

**是否需要文档同步**  
可选，但建议在 v2 implementation notes 里记录。

**推荐实施顺序**  
2

---

### 7.3 Constrained live boundary 当前比原报告描述的更强，不应再作为 v2 首要重构目标

- **优先级**：P2
- **类型**：Security / Ops
- **性质**：原报告需要收敛修正
- **受影响文件**：
  - `asterion_core/contracts/live_boundary.py`
  - `asterion_core/execution/live_submitter_v1.py`
  - `tests/test_live_submitter_backend.py`
  - `tests/test_submitter_boundary_attestation.py`

**原报告结论**  
原报告把 submitter attestation 看得偏弱，认为仍主要是同进程 caller-trusted audit artifact。

**当前代码事实**  
当前 HEAD 已是 attestation v2：

- TTL / nonce / fingerprint / issuer / HMAC
- persisted attestation 校验
- claim-once reuse rejection

**当前测试事实**  
相关测试通过，覆盖 v2 attestation 和 reuse rejection。

**当前文档事实**  
README 已把 controlled-live boundary 的这些点写成已成立能力。

**我的推断**  
对当前 manual-only 范围，这个边界已经足够克制。继续在 v2 初期大动 live boundary，ROI 会低于修 economics / allocator / calibration ops / operator workflow。

**风险或缺口描述**  
残余风险主要在：

- 它仍是同信任域 shared-secret model
- 它不是零信任 control plane

但这不等于当前阶段必须重做。

**为什么重要**  
如果不修正这个判断，v2 优先级会被安全焦虑拉偏。

**对稳定、规模化、高置信赚钱的影响**  
影响主要是“不要误投资源”。在当前阶段，live boundary 再升级的边际收益低于盈利链和交付稳定性改进。

**推荐修复方向**  
- 保持现有 boundary scope 不扩大
- 保持测试锁住当前能力
- 只做少量 deployment hardening / observability 强化
- 暂不把 boundary redesign 放进 v2 前排 workstreams

**需要改哪些模块**  
短期不需要大改核心模块；主要是文档、runbook、deployment defaults。

**需要哪些测试**  
保持现有 tests，补少量 deployment / startup truth alignment tests 即可。

**是否需要 migration**  
不需要。

**是否需要文档同步**  
需要，在 v2 blueprint 中明确“保持边界收紧，不扩 live scope”。

**推荐实施顺序**  
后置

---

### 7.4 Signer secret boundary 已收口，v2 不应再围绕它做主工作流

- **优先级**：P3
- **类型**：Security
- **性质**：原报告需要修正
- **受影响文件**：
  - `asterion_core/signer/signer_service_v1.py`
  - `tests/test_signer_shell.py`

**原报告结论**  
原报告认为 signer 仍信任 caller 提供 env var。

**当前代码事实**  
当前 code 已禁止 caller 注入 `private_key_env_var`，只按 `wallet_id` 推导 secret scope。

**当前测试事实**  
对应测试通过。

**当前文档事实**  
README 已同步这一变化。

**修正后的结论**  
这是一个**已修复问题**，应从 v2 主风险列表中移除。

**对 v2.0 规划的影响**  
保持回归测试即可，不需要围绕它单独开 workstream。

---

### 7.5 ranking v2 与 execution feedback 已经进入主排序，但还不够资本化 / 容量化 / 经验化

- **优先级**：P1
- **类型**：Trading
- **性质**：原报告方向正确，但需要更新基线
- **受影响文件**：
  - `domains/weather/opportunity/service.py`
  - `domains/weather/opportunity/execution_feedback.py`
  - `domains/weather/opportunity/execution_priors.py`
  - `tests/test_ranking_score_v2.py`
  - `tests/test_execution_feedback_loop.py`

**原报告结论**  
原报告认为 feedback 还没真正闭环进入主排序。

**当前代码事实**  
当前 final `ranking_score` 已经是：

- ranking v2 decomposition
- × uncertainty multiplier
- × (1 - feedback penalty)

并且 `why_ranked_json` 已暴露足够多的解释字段。

**当前测试事实**  
相关测试通过，说明这条链是实的。

**当前文档事实**  
README 也已明确写 feedback-backed ranking 已进入主链。

**我的推断**  
当前真正的缺口不是“feedback 没接进来”，而是：

- execution priors 的特征空间还太粗
- capture / slippage / distortion 仍然不足以代表真实容量成本
- `ranking_score` 仍然更像 unit-opportunity score，而不是 capital allocation score

**风险或缺口描述**  
系统已经能惩罚坏 cohort，但还不能很好地回答：

- 该投多少 size
- 哪个机会的边际资本效率更高
- 当前 wallet / market / cohort 的真实容量上限在哪里

**为什么重要**  
这是 v2.0 最核心的赚钱问题。

**对稳定、规模化、高置信赚钱的影响**  
直接影响规模化盈利能力。没有 capital-aware ranking，就很难从“能挑机会”升级成“能部署资本”。

**推荐修复方向**  
- 保留 ranking v2 主链，不重写
- 扩 execution priors / feedback 特征空间
- 引入 allocator / sizing 层
- 让 `ranking_score` 与 `recommended_size` 联动，而不是把 unit score 误当 portfolio priority

**需要改哪些模块**  
- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_feedback.py`
- 新的 allocator 模块
- `portfolio_v3` 对接层
- UI 首页 / Markets / Execution

**需要哪些测试**  
- replay / ranking correlation tests
- sizing monotonicity tests
- budget exhaustion / concentration limit tests
- operator action queue acceptance tests

**是否需要 migration**  
需要，但应优先落在 `runtime.*`，不是新建 `risk.*`。

**是否需要文档同步**  
需要。

**推荐实施顺序**  
3

---

### 7.6 Calibration v2 已经够强到值得保留，但 calibration ops 仍是 v2 的重要短板

- **优先级**：P1
- **类型**：Trading / Ops
- **性质**：原报告仍成立，但需要更精准
- **受影响文件**：
  - `domains/weather/forecast/calibration.py`
  - `domains/weather/forecast/adapters.py`
  - `domains/weather/forecast/service.py`
  - `dagster_asterion/job_map.py`
  - `tests/test_calibration_profile_v2.py`
  - `tests/test_threshold_probability_profile.py`
  - `tests/test_cold_path_orchestration.py`

**原报告结论**  
原报告认为 calibration 已接主链，但 freshness / ops 仍不足。

**当前代码事实**  
- calibration profile v2 已存在
- threshold probability profile 已存在
- corrected mean / std dev 已进入 distribution summary v2
- 但 profile refresh 仍是 manual

**当前测试事实**  
相关功能测试通过；orchestration tests 也证明 calibration v2 refresh 当前未默认 schedule。

**当前文档事实**  
入口 docs 与当前代码一致：calibration v2 已落地，但并未宣称它已 fully automated。

**我的推断**  
当前 calibration 的瓶颈已经不是数学表达，而是：

- freshness
- materialization cadence
- failure visibility
- operator 是否能看见 profile stale / sparse / degraded

**风险或缺口描述**  
一个好的 calibration system，如果 refresh 仍然靠 manual job，就很难支撑持续运营。

**为什么重要**  
这会直接限制高置信赚钱能力。

**对稳定、规模化、高置信赚钱的影响**  
直接影响模型信任边界和机会质量；尤其在 weather threshold 市场中，calibration freshness 非常关键。

**推荐修复方向**  
- 把 calibration v2 从“建模能力”推进到“运营能力”
- 为 refresh 增加 schedule / observability / stale surface
- 在 UI 中显式显示 calibration freshness / profile quality / sample sufficiency

**需要改哪些模块**  
- `dagster_asterion/job_map.py`
- `dagster_asterion/handlers.py`
- `domains/weather/forecast/calibration.py`
- `ui/data_access.py`
- `ui/pages/markets.py`
- `ui/pages/system.py`

**需要哪些测试**  
- scheduled orchestration tests
- stale calibration surface tests
- calibration materialization status tests

**是否需要 migration**  
需要，建议使用 `runtime.*` materialization status table，而不是新 schema。

**是否需要文档同步**  
需要。

**推荐实施顺序**  
4

---

### 7.7 Allocator / sizing 是 v2 的核心缺口，但持久化设计必须遵守现有 `trading.* / runtime.*` discipline

- **优先级**：P1
- **类型**：Trading / Architecture / Scale
- **性质**：原报告方向正确，但 schema 建议需要修订
- **受影响文件**：
  - `asterion_core/risk/portfolio_v3.py`
  - `asterion_core/runtime/strategy_engine_v3.py`
  - `asterion_core/execution/execution_gate_v1.py`
  - `domains/weather/opportunity/service.py`
  - `ui/pages/home.py`
  - `ui/pages/markets.py`

**原报告结论**  
原报告正确地指出 allocator / sizing 是 v2 的重要能力，但直接建议新增 `risk.portfolio_budgets` / `risk.position_limits`。

**当前代码事实**  
- `portfolio_v3` 依赖 `trading.inventory_positions` / `trading.reservations`
- 当前 canonical persistence 并没有 `risk.*`
- runtime 中已经有大量 decision / materialization artifacts 表，说明 allocator decision 更适合先落在 `runtime.*`

**当前测试事实**  
当前还没有 allocator 主链测试，这恰恰说明它是 v2 的新增重点；但也说明没有必要为了它先建立一套全新 schema 世界。

**当前文档事实**  
当前入口 docs 并没有为新 `risk.*` schema 建立 canonical position。

**我的推断**  
allocator 需要做，但应该按下面这条纪律推进：

- canonical trading facts 继续留在 `trading.*`
- allocator 的每次运行决策、预算分配、limit check 先留在 `runtime.*`
- 只有当 operator-managed policy 稳定下来，才考虑把 policy 落到 `trading.*`
- 默认**不新建 `risk.*`**

**风险或缺口描述**  
如果现在就开 `risk.*`，很容易造出一套平行 truth-source：

- 一套在 `trading.*`
- 一套在 `risk.*`

最终会增加 reconciliation 和 delivery drift。

**为什么重要**  
这关系到 v2 的 persistence discipline 是否还能保持简洁稳定。

**对稳定、规模化、高置信赚钱的影响**  
allocator 是规模化赚钱必须补齐的能力；但错误的 schema 设计会把它变成技术债放大器。

**推荐修复方向**  
1. 先做 allocator 逻辑与 runtime decision persistence
2. 先不要新建 `risk.*`
3. 推荐的落点：
   - `runtime.allocation_decisions`
   - `runtime.position_limit_checks`
   - `runtime.capital_allocation_runs`
4. 如果未来 policy 固化，再考虑：
   - `trading.allocation_policies`
   - `trading.position_limit_policies`

**需要改哪些模块**  
- 新增 allocator module（可放 `asterion_core/risk/allocator_v1.py` 或 `allocation_v1.py`，模块名不等于 schema）
- `portfolio_v3`
- `strategy_engine_v3`
- `execution_gate_v1`
- UI 首页 / Markets

**需要哪些测试**  
- budget exhaustion
- concentration cap
- inventory-aware sizing
- portfolio_v3 integration
- runtime allocation decision persistence
- recommended size UI acceptance tests

**是否需要 migration**  
需要，但优先在 `runtime.*`，而不是 `risk.*`。

**是否需要文档同步**  
需要，且要明确“不新增 risk schema 作为默认答案”。

**推荐实施顺序**  
5

---

### 7.8 当前 scale blocker 更偏 operator throughput / capital discipline，而不是 cold-path orchestration 本身

- **优先级**：P2
- **类型**：Scale / UX / Trading
- **性质**：原报告需要更聚焦地改写
- **受影响文件**：
  - `dagster_asterion/job_map.py`
  - `ui/pages/home.py`
  - `ui/pages/markets.py`
  - `ui/pages/execution.py`
  - `ui/data_access.py`

**原报告结论**  
原报告认为系统离规模化赚钱还有距离，这个判断是对的。

**当前代码事实**  
- cold-path orchestration 其实已经比较完整
- execution priors 已有 nightly job
- readiness / monitoring / UI read-model 也都有基础
- 缺的是 recommended action / size / capacity-aware queue

**当前测试事实**  
`tests/test_cold_path_orchestration.py` 通过，证明当前 job map 并不是一片空白。

**当前文档事实**  
入口 docs 也没有把 scale 问题描述成“基础 job 还没建”。

**我的推断**  
当前 scale blocker 不是“dagster 不够用”，而是：

- 机会排序还没变成资本部署排序
- operator 还没有 size-aware action queue
- calibration ops 还不够自动化

**修正后的结论**  
scale workstream 应聚焦在 **throughput of good decisions**，不是简单扩大 job 数量或 domain 数量。

**对 v2.0 规划的影响**  
不应把“扩 coverage / 扩 live boundary / 扩 domain”放到 v2 最前面。

---

## 8. Corrected v2.0 Priorities

基于上面的修订，v2.0 的优先级建议应做如下调整。

### 8.1 应上调优先级的事项

#### P1. Truth-source and read-model contract hardening

原因：

- 当前 HEAD 有真实失败
- 影响 UI-lite build 与 operator surface 稳定性
- 是后续所有 v2 workstreams 的基础

包括：

- 修 `ui.daily_review_input.item_id`
- 统一 docs / UI / startup / shared constants phase status
- 补 cross-surface truth-source tests

#### P1. Execution economics refinement

原因：

- 当前主排序已经接线，但还不够 capital-aware / capacity-aware
- 这是直接决定是否更稳定赚钱的主问题

包括：

- 扩 execution priors feature space
- 让 ranking 更接近 marginal capital efficiency
- 更好地利用 feedback scope breakdown

#### P1. Allocator / sizing / capital discipline

原因：

- 当前 `ranking_score` 还是 unit-opportunity priority
- 缺少 recommended size / budget usage / concentration control

包括：

- allocator logic
- runtime allocation persistence
- UI recommended size / action queue

#### P1. Calibration ops and freshness

原因：

- calibration v2 已有，但 refresh 还不是默认 schedule
- 不做这件事，高置信赚钱能力始终会受限

包括：

- schedule / monitoring / stale surface
- calibration materialization status

#### P1. Operator throughput

原因：

- 当前 operator surface 已经有帮助，但还没有“快速扩大处理规模”的动作组织形式
- 这是规模化赚钱的关键瓶颈之一

包括：

- action queue
- cohort history
- recommended size
- quality / risk / source flags

### 8.2 应下调优先级的事项

#### P2/P3. Submitter / signer boundary major redesign

原因：

- 当前 HEAD 已比原报告更强
- 对当前 constrained live 范围已足够谨慎
- ROI 不如 economics / allocator / calibration ops / operator workflow

#### P3. 新建 `risk.*` schema

原因：

- 当前 persistence discipline 已经清晰
- allocator 并不天然要求新 schema
- 容易造成平行 truth-source

#### P3. 扩大 live boundary / 扩大自动化执行 scope

原因：

- 当前系统定位仍然是 `operator console + constrained execution infra`
- 赚钱能力主问题还不在“自动化不够”，而在“资本部署和机会质量闭环不够”

---

## 9. Revised Workstreams

下面给出更贴近当前 HEAD、也更克制的 v2.0 workstreams。

### WS0. Truth-Source and Delivery Hardening

**目标**  
收口当前最明确的 repo / UI / startup / read-model 漂移，建立 v2.0 的稳定交付基线。

**关键改动模块**

- `asterion_core/ui/surface_truth_shared.py`
- `ui/app.py`
- `ui/pages/home.py`
- `start_asterion.sh`
- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/ui/read_model_registry.py`
- `tests/test_operator_truth_source.py`
- `tests/test_execution_foundation.py`

**建议新增或扩展内容**

- 统一 `CURRENT_PHASE_STATUS` 与 `TRUTH_SOURCE_DOC`
- `ui.daily_review_input.item_id` deterministic build
- 新增 cross-surface truth alignment tests
- 新增 full UI-lite build acceptance test

**不做项**

- 不做架构重写
- 不做 UI 大改版

**验收标准**

- 代表性测试集全绿
- docs / app / home / startup phase status 完全一致
- UI-lite build 不再因 registry-builder drift 失败

---

### WS1. Execution Economics and Ranking Refinement

**目标**  
在当前 ranking v2 基础上，把 unit-opportunity ranking 进一步推进为更接近真实资本部署价值的排序。

**关键改动模块**

- `domains/weather/opportunity/service.py`
- `domains/weather/opportunity/execution_feedback.py`
- `domains/weather/opportunity/execution_priors.py`
- `domains/weather/pricing/engine.py`
- `asterion_core/runtime/strategy_engine_v3.py`

**建议新增或扩展内容**

- 扩 execution priors 特征空间
  - source freshness bucket
  - spread / depth / liquidity bucket
  - price bucket / edge bucket
  - maybe threshold-regime-aware cohorts
- 更明确地区分：
  - unit EV
  - capture-adjusted EV
  - marginal capital efficiency
- 继续把 `why_ranked_json` 做成稳定 contract

**不做项**

- 不重写 ranking 主链
- 不把当前 v2 当成全新 economics 系统重建

**验收标准**

- replay / retrospective windows 上，top-ranked opportunity 的 capture / realized outcome 相关性优于当前 baseline
- `ops_tie_breaker` 不会盖过显著更优的经济机会
- `why_ranked_json` 稳定且可被 UI / tests 消费

---

### WS2. Allocation, Sizing, and Capital Discipline

**目标**  
补上从“机会排序”到“资本部署建议”的关键桥梁。

**关键改动模块**

- `asterion_core/risk/portfolio_v3.py`
- 新 allocator module（例如 `asterion_core/risk/allocator_v1.py`）
- `asterion_core/runtime/strategy_engine_v3.py`
- `asterion_core/execution/execution_gate_v1.py`
- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/data_access.py`

**建议新增或扩展内容**

优先使用现有 persistence discipline，不引入新 `risk.*` schema：

- `runtime.capital_allocation_runs`
- `runtime.allocation_decisions`
- `runtime.position_limit_checks`

如果未来 policy 稳定，再评估：

- `trading.allocation_policies`
- `trading.position_limit_policies`

**allocator 与现有主链的关系**

- `ranking_score`：回答“哪个机会更值得优先处理”
- `execution_feedback`：回答“这个机会所在 cohort 的经验质量如何”
- `portfolio_v3`：回答“当前 inventory / reservation / exposure 真相是什么”
- allocator：回答“在当前资本与持仓约束下，这个机会该不该做、做多大”

**不做项**

- 不引入平行 `risk.*` ledger
- 不做过早复杂的多资产风险系统

**验收标准**

- 每个 actionable opportunity 都能得到 `recommended_size`
- 超预算 / 超 concentration / inventory constrained 时会明确降级或 block
- UI 可展示 recommended size 和 budget impact

---

### WS3. Calibration Ops and Forecast Quality Operations

**目标**  
把 calibration v2 从“已接主链的模型能力”升级为“可长期运营的冷路径能力”。

**关键改动模块**

- `domains/weather/forecast/calibration.py`
- `domains/weather/forecast/adapters.py`
- `domains/weather/forecast/service.py`
- `dagster_asterion/job_map.py`
- `dagster_asterion/handlers.py`
- `ui/pages/markets.py`
- `ui/pages/system.py`

**建议新增或扩展内容**

- 为 calibration profile v2 refresh 增加默认 schedule（在安全/观测前提下）
- 增加 `runtime.calibration_profile_materializations` 或等价 runtime materialization status 表
- 在 operator surface 显示：
  - latest materialized at
  - sample sufficiency
  - stale / degraded flags

**不做项**

- 不急着引入更重的 model family 扩张
- 不把 calibration v2 workstream 变成纯研究工作

**验收标准**

- calibration profile refresh 不再是纯人工触发的黑箱流程
- stale / missing / sparse profile 在 UI 上可见
- calibration freshness 成为 operator 可消费信息，而不是埋在 source context 里

---

### WS4. Operator Workflow and Throughput

**目标**  
把当前 operator console 从“已经有帮助”推进到“能支持更高吞吐量、更低误判的赚钱工作流”。

**关键改动模块**

- `ui/pages/home.py`
- `ui/pages/markets.py`
- `ui/pages/execution.py`
- `ui/data_access.py`
- `asterion_core/ui/ui_lite_db.py`

**建议新增或扩展内容**

- recommended action queue
- recommended size / budget impact
- cohort history summary
- calibration freshness / feedback status / source badge 的更显式组合展示
- operator-facing surface 分层：
  - actionable
  - review required
  - research only

**不做项**

- 不做更多页面数量
- 不做偏工程内部的配置展示

**验收标准**

- operator 可以快速区分：
  - 高质量机会
  - 高风险机会
  - research-only 机会
- 首页与 Markets 页不再只是 score table，而是更接近 action surface

---

### WS5. Read-Model and Delivery Hardening

**目标**  
降低当前 UI-lite / loader / builder 的 delivery risk，为后续 v2 演进提供更稳定的测试与 read-model contract。

**关键改动模块**

- `asterion_core/ui/ui_lite_db.py`
- `asterion_core/ui/read_model_registry.py`
- `ui/data_access.py`
- `ui/surface_truth.py`

**建议新增或扩展内容**

- 拆分过大的 builder / loader 文件
- 为关键 read models 建立更显式的 schema contract tests
- 为关键 operator surfaces 建立 golden / acceptance tests

**不做项**

- 不做过早 service 化
- 不做大规模 UI 架构重写

**验收标准**

- 新增 read-model 时不容易再引入静默 contract drift
- UI surface contract 有明确测试锁定
- builder / loader 改动的 delivery risk 降低

---

## 10. Revised Phase Breakdown

### Phase 0 — Stabilize Current HEAD

**目标**  
先把当前 HEAD 的显式漂移和真实失败修干净，建立 v2 开发基线。

**主要交付物**

- `ui.daily_review_input.item_id` 修复
- phase status / truth-source 对齐
- app / home / startup copy 与 docs 对齐
- operator truth-source tests 从“自洽”升级为“对齐 docs”

**不做项**

- 不碰 live boundary 设计
- 不扩 domain
- 不做复杂 allocator

**关键测试**

- 当前 2 个 failing tests 转绿
- docs/UI/startup truth alignment tests
- UI-lite full build acceptance test

**验收标准**

- 代表性测试集全绿
- 当前 repo 不再存在 phase split-brain

### Phase 1 — Strengthen Profit Engine

**目标**  
基于现有 ranking v2 / feedback loop，提升 economics 的真实度。

**主要交付物**

- richer execution priors
- ranking economics refinement
- replay-based comparison harness

**不做项**

- 不重写 ranking 主链
- 不做过早 portfolio optimizer

**关键测试**

- ranking replay regression
- feedback prior refinement tests
- ranking explanation contract tests

**验收标准**

- retrospective windows 上 ranking 与 capture / realized outcomes 的相关性提升

### Phase 2 — Add Capital Discipline

**目标**  
把 ranking 结果变成资金部署建议。

**主要交付物**

- allocator v1
- runtime allocation tables
- recommended size / position limit checks
- UI action queue 初版

**不做项**

- 不引入 `risk.*` schema
- 不做复杂多域 risk engine

**关键测试**

- budget / concentration / inventory-aware sizing tests
- allocator-runtime persistence tests
- UI recommended size acceptance tests

**验收标准**

- 每个 actionable opportunity 都有 size-aware recommendation
- portfolio constraints 真正进入 operator decision surface

### Phase 3 — Operationalize Calibration

**目标**  
把 calibration v2 从“模型能力”推进到“日常运营能力”。

**主要交付物**

- calibration refresh scheduling
- calibration materialization status
- stale / sparse / degraded calibration surface

**不做项**

- 不抢做更重的新模型体系

**关键测试**

- scheduled job tests
- stale calibration UI tests
- materialization status tests

**验收标准**

- calibration freshness 进入日常运营闭环

### Phase 4 — Increase Operator Throughput and Delivery Stability

**目标**  
让 operator 能在更稳定的 read-model / UI surface 上处理更多机会。

**主要交付物**

- richer action queue
- cohort history surfaces
- read-model / loader 拆分收口
-更多 acceptance tests

**不做项**

- 不扩 unrestricted / unattended live
- 不过早增加新 domain complexity

**关键测试**

- operator workflow acceptance tests
- read-model contract tests
- UI surface regression tests

**验收标准**

- operator 处理机会的效率与一致性提升
- UI / read-model delivery drift 降低

---

## 11. Revision Notes / Delta vs Original Report

下面列出这次修订相对于原报告的关键 delta。

### 11.1 保留的高价值洞见

以下洞见仍然成立，并被本修订版保留：

1. 当前主矛盾不是缺 execution infra，而是缺更强的 profit loop quality。
2. execution economics / allocator / calibration ops / operator throughput 是 v2 的主线。
3. UI truth-source 和 read-model contract drift 仍然是很重要的 delivery 风险。
4. 不应把 v2.0 做成 live boundary scope expansion 项目。

### 11.2 明确修正的结论

以下是这次修订中最重要的修正：

1. **repo status 写法更精确了**  
   不是“仓库大概切到 v2 了”，而是：
   - 入口 docs 已切到 `v2.0 planning`
   - `P4` / post-P4 remediation 已归档为 historical accepted records
   - 真正落后的，是 UI/startup/shared constants 和部分 tests discipline

2. **submitter attestation 的评价收敛了**  
   不再把它写成弱 audit artifact；当前 HEAD 已经是 attestation v2 with TTL / nonce / HMAC / persisted use。它仍不是零信任 control plane，但已明显强于原报告描述。

3. **signer secret boundary 问题被移出主风险列表**  
   caller 注入 `private_key_env_var` 已被禁止，这个问题已经修掉。

4. **execution feedback 已被承认为主排序一部分**  
   不再写成“还没真正接进来”，而是写成“已经接进来，但还不够经济化 / 资本化 / 经验化”。

5. **`risk.*` schema 建议被收回并替换为更克制的 persistence plan**  
   这是本修订版最重要的架构修正之一：
   - allocator 仍然要做
   - 但默认落点应是 `runtime.*` decision artifacts + 未来必要时的 `trading.*` policy tables
   - 不默认开新 `risk.*`

### 11.3 对 v2.0 规划的净影响

修订后的 v2.0 blueprint 更贴近当前 HEAD，也更适合直接指导开发：

- **更准确**：不会再重复已经修掉的 signer / boundary 问题
- **更克制**：不会过早发散出新 schema
- **更聚焦赚钱能力**：重点仍落在 economics / allocator / calibration ops / operator throughput
- **更适合作为实现入口**：可以直接拿来扩成真正的 `V2_Implementation_Plan.md`

---

## Appendix: Files Rechecked and Tests Rerun

### 重新核对的文档

- `Asterion/README.md`
- `Asterion/AGENTS.md`
- `Asterion/docs/00-overview/Documentation_Index.md`
- `Asterion/docs/00-overview/Asterion_Project_Plan.md`
- `Asterion/docs/00-overview/DEVELOPMENT_ROADMAP.md`
- `Asterion/docs/10-implementation/Implementation_Index.md`
- `Asterion/docs/10-implementation/phase-plans/V2_Implementation_Plan.md`
- `Asterion/docs/10-implementation/phase-plans/Post_P4_Remediation_Implementation_Plan.md`

### 重点复核的代码

- `Asterion/asterion_core/ui/surface_truth_shared.py`
- `Asterion/ui/app.py`
- `Asterion/ui/pages/home.py`
- `Asterion/start_asterion.sh`
- `Asterion/asterion_core/ui/ui_lite_db.py`
- `Asterion/asterion_core/ui/read_model_registry.py`
- `Asterion/dagster_asterion/job_map.py`
- `Asterion/dagster_asterion/handlers.py`
- `Asterion/domains/weather/opportunity/service.py`
- `Asterion/domains/weather/opportunity/execution_feedback.py`
- `Asterion/domains/weather/forecast/calibration.py`
- 补充复核：
  - `Asterion/asterion_core/contracts/live_boundary.py`
  - `Asterion/asterion_core/execution/live_submitter_v1.py`
  - `Asterion/asterion_core/signer/signer_service_v1.py`
  - `Asterion/asterion_core/risk/portfolio_v3.py`

### 重点复核并重跑的测试

- `Asterion/tests/test_execution_foundation.py`
- `Asterion/tests/test_operator_truth_source.py`
- `Asterion/tests/test_ui_read_model_catalog.py`
- `Asterion/tests/test_truth_source_checks.py`
- `Asterion/tests/test_ranking_score_v2.py`
- `Asterion/tests/test_execution_feedback_loop.py`
- `Asterion/tests/test_calibration_profile_v2.py`
- `Asterion/tests/test_threshold_probability_profile.py`
- `Asterion/tests/test_cold_path_orchestration.py`
- 补充：
  - `Asterion/tests/test_live_submitter_backend.py`
  - `Asterion/tests/test_submitter_boundary_attestation.py`
  - `Asterion/tests/test_signer_shell.py`
  - `Asterion/tests/test_p4_plan_docs.py`
  - `Asterion/tests/test_p4_closeout.py`
  - `Asterion/tests/test_execution_priors_materialization.py`
  - `Asterion/tests/test_phase9_wording.py`

### 当前最重要的一句话总结

> 修订后的结论不是推翻原报告，而是把它收紧到更贴近当前 HEAD 的形态：Asterion 现在已经具备一条真实存在的 constrained trading foundation，v2.0 不该再花主要精力证明基础设施存在，而应在不破坏现有 persistence discipline 的前提下，优先把 truth-source / read-model contract 修稳，把 execution economics、allocator、calibration ops 和 operator throughput 做成更接近“稳定、规模化、高置信赚钱”的主线能力。
