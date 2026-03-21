# Asterion Agent 价值分析报告

**分析日期**: 2026-03-19
**分析范围**: 项目中全部 3 个 agent（Rule2Spec、Data QA、Resolution）及其公共基础设施
**分析方法**: 代码级逐行验证，追踪每个 agent 的输入来源、输出去向、实际消费方

---

## 1. 结论先行

**三个 agent 当前的实际价值远低于它们的代码体量和架构复杂度。**

| 维度 | 数据 |
|------|------|
| Agent 相关代码总量 | ~3,700 行（3 个 agent + common infra + persistence + tests） |
| Agent 输出的实际消费方 | 仅 UI 展示 + readiness checker 表存在性检查 |
| Agent 输出对执行路径的影响 | **零** |
| Agent 输出对排序/定价的影响 | **零** |
| Agent 建议被自动应用的场景 | **零** |

核心问题：**agent 产出的 verdict、suggested_patch、risk_flags 没有任何代码路径消费它们来改变系统行为。** 它们是纯粹的"写入后展示"模式。

---

## 2. 逐个 Agent 分析

### 2.1 Rule2Spec Agent（522 行）

**它做什么**: 接收确定性解析器 `parse_rule2spec_draft()` 的输出，发送给 LLM，让 LLM 判断解析结果是否正确，返回 verdict + suggested_patch。

**关键问题：LLM 审查的对象本身就是确定性的。**

代码验证：

```
domains/weather/spec/rule2spec.py  →  parse_rule2spec_draft()  →  Rule2SpecDraft
```

这个确定性解析器已经：
- 用正则从 market rules 中提取温度阈值、日期、指标
- 通过 StationMapper 解析 station_id
- 生成 parse_confidence 和 risk_flags

然后 Rule2Spec Agent 做的事情是：
1. 把 draft + current_spec + station_metadata 序列化为 JSON
2. 发送给 LLM，让 LLM 返回 verdict
3. 把 LLM 返回的 suggested_patch_json 写入 `agent.outputs`

**但 suggested_patch_json 从未被任何代码消费。**

验证：
```bash
grep -rn "suggested_patch_json" domains/ asterion_core/execution/ asterion_core/strategy/
# 结果：空
```

`suggested_patch_json` 只存在于：
- `agents/weather/rule2spec_agent.py`（生产）
- `agents/common/persistence.py`（写入 DB）
- `ui/pages/agents.py`（展示）
- `tests/test_weather_agents.py`（测试）

**没有任何代码读取 agent.outputs 中的 patch 并应用到 weather_market_specs。**

**价值判断**: 如果 operator 从不看 agent UI 页面，这个 agent 的运行等于零效果。即使 operator 看了，也需要手动把 suggested_patch 复制到某个地方执行——但项目中没有这个"执行"入口。

---

### 2.2 Data QA Agent（459 行）

**它做什么**: 接收 forecast replay 的 diff 结果，发送给 LLM，让 LLM 判断数据质量。

**关键问题：它审查的维度（station_match、timezone_ok、unit_ok）完全可以用确定性代码检查。**

代码验证：

Agent 让 LLM 判断的字段：
- `station_match_score`: 0.0-1.0 — 但 station 匹配是确定性的（StationMapper 已经做了）
- `timezone_ok`: boolean — timezone 是 spec 中的固定字段，匹配与否是确定性的
- `unit_ok`: boolean — 同上
- `pricing_provenance_ok`: boolean — fair value 是否存在，是确定性查询
- `fallback_risk`: low/medium/high — fallback 是否使用，是确定性事实

**这些判断不需要 LLM。** 一个 50 行的确定性函数可以完成同样的工作，而且结果 100% 可复现。

LLM 唯一可能增加价值的地方是 `summary`（自然语言解释）和 `findings`（结构化发现）。但这些输出同样没有被任何执行路径消费。

**价值判断**: 这个 agent 用 LLM 做了确定性代码就能做的事，且输出无人消费。是三个 agent 中 ROI 最低的。

---

### 2.3 Resolution Agent（487 行）

**它做什么**: 接收 UMA proposal 的 settlement verification 结果，发送给 LLM，让 LLM 评估结算风险并建议 operator 行动。

**这是三个 agent 中唯一可能有真实价值的。**

原因：
- settlement verification 涉及多个数据源的交叉验证（expected vs proposed outcome）
- watcher continuity 检查有 gap 时，判断是否影响结算需要上下文理解
- `recommended_operator_action` 的 5 个选项（observe / manual_review / consider_dispute / hold_redeem / ready_for_redeem_review）确实需要综合判断

**但同样的问题：输出没有被消费。**

`recommended_operator_action` 写入 `agent.outputs` 后，没有任何代码：
- 根据 `consider_dispute` 触发 dispute 流程
- 根据 `hold_redeem` 阻止 redeem
- 根据 `ready_for_redeem_review` 推进 redeem 审批

它只是在 UI 上展示一行文字。

**价值判断**: 方向正确，但当前是"有枪无弹"——agent 给出了建议，但系统没有执行建议的机制。

---

## 3. 公共基础设施分析

### 3.1 Agent Client（499 行）

支持 Anthropic、OpenAI-compatible（Qwen/GLM/Healwrap）、Fake 三种 provider。包含：
- response_format 协商（400 时自动降级）
- 502/503/504 重试
- curl fallback（针对 Dashscope/Healwrap）

**这是整个 agent 体系中工程质量最高的部分。** 但它服务的上层 agent 价值有限，导致这些精心设计的容错机制没有发挥应有的作用。

### 3.2 Runtime Records（316 行）

完整的 invocation → output → review → evaluation 四表模型。设计合理：
- 确定性 invocation_id（基于 input hash）
- force_rerun 支持
- 自动 review/evaluation 生成

**问题**: review 表的 `review_status` 只有 APPROVED / REJECTED / NEEDS_FOLLOWUP，但没有任何代码路径让 operator 实际执行 review（UI 只展示，没有 approve/reject 按钮的 action handler）。

### 3.3 Persistence（223 行）

通过 write queue 写入 `agent.*` schema。隔离设计正确——agent 不写 canonical 表。

**但这个隔离也意味着 agent 输出永远无法影响系统行为，除非有人手动搬运。**

---

## 4. Readiness Checker 中的 Agent Gate

```python
# readiness_checker_v1.py:76-87
_AGENT_SURFACE_TABLES = [
    "agent.invocations",
    "agent.outputs",
    "agent.reviews",
    "agent.evaluations",
]
_REQUIRED_AGENT_JOBS = {
    "weather_rule2spec_review",
    "weather_data_qa_review",
    "weather_resolution_review",
}
```

Readiness checker 的 `agent_review_surface` gate 只检查：
1. 这 4 张表是否存在
2. 这 3 个 job 是否在 job_map 中注册

**它不检查 agent 是否实际运行过、verdict 是否为 pass、是否有未处理的 review。** 这意味着即使 agent 从未运行，只要表和 job 定义存在，readiness 就能通过。

---

## 5. 成本分析

### 5.1 LLM 调用成本

每次 agent 运行：
- Rule2Spec: ~2,000 token input + ~500 token output（per market）
- Data QA: ~3,000 token input + ~500 token output（per replay）
- Resolution: ~2,500 token input + ~500 token output（per proposal）

按 GLM-5 定价（约 $0.001/1K tokens），单次调用成本极低。但问题不是成本，而是 **这些调用产生的输出没有被利用**。

### 5.2 维护成本

3,700 行 agent 代码 + 1,199 行测试 = ~4,900 行需要维护的代码。这占项目总代码量（43,865 行）的 ~11%。

每次修改 contract（如 `Rule2SpecDraft` 字段变更）、修改 spec 解析逻辑、修改 settlement 流程，都需要同步更新对应 agent 的 input payload 构建和 output 解析。

---

## 6. 具体建议

### 方案 A：最小化（推荐）

**保留 Resolution Agent，删除 Rule2Spec Agent 和 Data QA Agent。**

理由：
- Resolution Agent 是唯一涉及真实资金风险（dispute/redeem）的 agent
- Rule2Spec 的工作可以用确定性 validation 替代
- Data QA 的工作可以用确定性 assertion 替代

具体操作：

**1) 用确定性函数替代 Rule2Spec Agent（新增 ~60 行，删除 ~522 行）**

```python
# domains/weather/spec/rule2spec_validation.py

from asterion_core.contracts import Rule2SpecDraft, StationMetadata, WeatherMarketSpecRecord


def validate_rule2spec_draft(
    draft: Rule2SpecDraft,
    current_spec: WeatherMarketSpecRecord | None,
    station_metadata: StationMetadata | None,
) -> dict:
    """确定性验证 Rule2Spec draft，替代 LLM agent"""
    risk_flags = list(draft.risk_flags)
    violations = []

    # station 验证
    if station_metadata is None:
        risk_flags.append("missing_station_mapping")
        violations.append("no station metadata for location")
    elif draft.location_name and station_metadata.location_name != draft.location_name:
        risk_flags.append("location_name_mismatch")

    # spec 一致性验证
    if current_spec is not None:
        if draft.metric != current_spec.metric:
            violations.append(f"metric changed: {current_spec.metric} -> {draft.metric}")
        if draft.unit != current_spec.unit:
            violations.append(f"unit changed: {current_spec.unit} -> {draft.unit}")
        if draft.authoritative_source != current_spec.authoritative_source:
            violations.append(f"source changed: {current_spec.authoritative_source} -> {draft.authoritative_source}")

    # parse confidence 验证
    if draft.parse_confidence < 0.8:
        risk_flags.append("low_parse_confidence")

    verdict = "pass"
    if violations or "missing_station_mapping" in risk_flags:
        verdict = "review"
    if draft.parse_confidence < 0.5:
        verdict = "block"

    return {
        "verdict": verdict,
        "risk_flags": risk_flags,
        "violations": violations,
        "human_review_required": verdict != "pass",
    }
```

**2) 用确定性函数替代 Data QA Agent（新增 ~40 行，删除 ~459 行）**

```python
# domains/weather/forecast/replay_validation.py

def validate_replay_quality(
    replay_diffs: list[dict],
    fallback_used: bool,
) -> dict:
    """确定性验证 replay 数据质量，替代 LLM agent"""
    critical_diffs = [d for d in replay_diffs if d.get("status") == "DIFFERENT"]
    missing_diffs = [d for d in replay_diffs if d.get("status") == "MISSING"]

    fallback_risk = "low"
    if fallback_used:
        fallback_risk = "medium"
    if fallback_used and critical_diffs:
        fallback_risk = "high"

    verdict = "pass"
    if critical_diffs or missing_diffs:
        verdict = "review"
    if len(critical_diffs) > 3:
        verdict = "block"

    return {
        "verdict": verdict,
        "critical_diff_count": len(critical_diffs),
        "missing_diff_count": len(missing_diffs),
        "fallback_risk": fallback_risk,
        "human_review_required": verdict != "pass",
    }
```

**3) 为 Resolution Agent 增加 action handler（新增 ~30 行）**

让 Resolution Agent 的输出真正有用：

```python
# 在 ui/pages/agents.py 中增加 resolution action handler

def _render_resolution_action(row):
    action = row.get("recommended_operator_action", "observe")
    if action == "consider_dispute":
        st.warning(f"Proposal {row['subject_id']}: Agent 建议考虑 dispute")
        if st.button(f"标记为 dispute_review", key=f"dispute_{row['subject_id']}"):
            # 写入 agent.reviews 更新 review_status
            pass
    elif action == "hold_redeem":
        st.error(f"Proposal {row['subject_id']}: Agent 建议暂停 redeem")
```

### 方案 B：保留但闭环

如果希望保留全部 3 个 agent，则必须让它们的输出产生实际效果：

**1) Rule2Spec Agent → 自动应用 patch**

在 `dagster_asterion/handlers.py` 的 `run_weather_rule2spec_review_job()` 后增加：

```python
# 如果 verdict=pass 且 confidence >= 0.9，自动应用 suggested_patch
if output.verdict == AgentVerdict.PASS and output.confidence >= 0.9:
    apply_spec_patch(con, market_id, output.suggested_patch_json)
```

需要新增 `apply_spec_patch()` 函数，写入 `weather.weather_market_specs`。

**2) Data QA Agent → 影响 forecast freshness**

在 `calibration.py` 中：

```python
# 如果最近的 data_qa verdict=block，降低 calibration confidence
if latest_data_qa_verdict == "block":
    corrected_std_dev *= 1.5  # 增加不确定性
```

**3) Resolution Agent → 影响 redeem gate**

在 redeem 流程中增加 agent verdict 检查：

```python
# 如果 resolution agent verdict != pass，阻止自动 redeem
if latest_resolution_verdict != "pass":
    return "redeem_blocked_by_agent_review"
```

---

## 7. 总结

| Agent | 当前价值 | 代码量 | 建议 |
|-------|----------|--------|------|
| Rule2Spec | 极低 — 用 LLM 做确定性验证，输出无人消费 | 522 行 | 替换为确定性函数 |
| Data QA | 极低 — 所有判断维度都是确定性的，输出无人消费 | 459 行 | 替换为确定性函数 |
| Resolution | 中等 — 方向正确但输出未闭环 | 487 行 | 保留并增加 action handler |
| Common Infra | 工程质量高但服务对象价值有限 | ~2,200 行 | 保留（Resolution Agent 仍需要） |

**一句话总结**: 这三个 agent 的架构设计是专业的（隔离、审计、幂等），但它们解决的问题要么不需要 LLM（Rule2Spec、Data QA），要么需要 LLM 但输出没有闭环（Resolution）。建议用 ~100 行确定性代码替代前两个 agent，集中精力让 Resolution Agent 的输出真正影响系统行为。
