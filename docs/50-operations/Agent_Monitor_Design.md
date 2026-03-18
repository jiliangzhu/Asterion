# Agent Monitor 详细设计

**模块**: `asterion_core/agents/monitor/`
**版本**: v1.0
**创建日期**: 2026-03-07

---

## 1. 模块概述

### 1.1 职责

Agent Monitor 是 Asterion 的 AI Agent 监控和评估系统，负责：

1. **性能追踪** - 监控所有 Agent 的执行性能和准确率
2. **质量评估** - 评估 Agent 输出质量和可靠性
3. **人工反馈** - 收集和整合人工反馈用于持续改进
4. **异常检测** - 识别 Agent 行为异常和性能退化
5. **成本监控** - 追踪 API 调用成本和 token 使用量

### 1.2 核心原则

**Agent 在执行路径之外**:
- Agent 只做分析和建议，不直接触发交易
- 所有 Agent 输出都需要人工审核或系统验证
- Agent 失败不应影响核心交易流程

**可观测性优先**:
- 所有 Agent 调用都记录完整上下文
- 输入、输出、耗时、成本全部可追溯
- 支持回放和调试

**持续改进**:
- 收集人工反馈用于 prompt 优化
- 追踪准确率变化趋势
- A/B 测试不同 prompt 版本

### 1.3 监控的 Agent

Weather MVP 阶段监控 4 个 Agent：

1. **Rule2Spec Agent** - 市场规则解析
2. **Data QA Agent** - 数据质量检查
3. **Resolution Sentinel Agent** - 结算验证
4. **Daily Review Agent** - 每日复盘

---

## 2. 核心组件设计

### 2.1 Agent Evaluator（Agent 评估器）

**文件**: `asterion_core/agents/monitor/agent_evaluator.py`

#### 2.1.1 数据结构

```python
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum

class AgentType(Enum):
    """Agent 类型"""
    RULE2SPEC = "rule2spec"
    DATA_QA = "data_qa"
    RESOLUTION_SENTINEL = "resolution_sentinel"
    DAILY_REVIEW = "daily_review"

class AgentStatus(Enum):
    """Agent 执行状态"""
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"

@dataclass
class AgentInvocation:
    """Agent 调用记录"""
    invocation_id: str
    agent_type: AgentType
    agent_version: str

    # 输入输出
    input_data: Dict[str, Any]
    output_data: Optional[Dict[str, Any]]

    # 执行信息
    status: AgentStatus
    start_time: datetime
    end_time: Optional[datetime]
    duration_ms: Optional[int]

    # API 信息
    model_name: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    api_cost_usd: float

    # 错误信息
    error_message: Optional[str]
    error_traceback: Optional[str]

@dataclass
class AgentEvaluation:
    """Agent 评估结果"""
    invocation_id: str
    agent_type: AgentType

    # 质量评分
    accuracy_score: Optional[float]  # 0-1
    confidence_score: float  # Agent 自评置信度
    human_rating: Optional[int]  # 1-5 人工评分

    # 性能指标
    latency_ms: int
    cost_usd: float

    # 验证结果
    is_verified: bool
    verification_method: str  # "human" / "automated" / "ground_truth"
    verification_notes: Optional[str]

    created_at: datetime
```

#### 2.1.2 核心实现

```python
class AgentEvaluator:
    """Agent 评估器"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def evaluate_rule2spec(
        self,
        invocation: AgentInvocation,
        ground_truth: Optional[Dict[str, Any]] = None
    ) -> AgentEvaluation:
        """评估 Rule2Spec Agent"""

        if not invocation.output_data:
            return AgentEvaluation(
                invocation_id=invocation.invocation_id,
                agent_type=invocation.agent_type,
                accuracy_score=0.0,
                confidence_score=0.0,
                human_rating=None,
                latency_ms=invocation.duration_ms or 0,
                cost_usd=invocation.api_cost_usd,
                is_verified=False,
                verification_method="none",
                verification_notes="No output",
                created_at=datetime.now(),
            )

        # 提取 Agent 输出
        output = invocation.output_data
        confidence = output.get("confidence", 0.5)

        # 如果有 ground truth，计算准确率
        accuracy = None
        if ground_truth:
            accuracy = self._calc_rule2spec_accuracy(output, ground_truth)

        return AgentEvaluation(
            invocation_id=invocation.invocation_id,
            agent_type=invocation.agent_type,
            accuracy_score=accuracy,
            confidence_score=confidence,
            human_rating=None,
            latency_ms=invocation.duration_ms or 0,
            cost_usd=invocation.api_cost_usd,
            is_verified=ground_truth is not None,
            verification_method="ground_truth" if ground_truth else "none",
            verification_notes=None,
            created_at=datetime.now(),
        )

    def _calc_rule2spec_accuracy(
        self,
        output: Dict[str, Any],
        ground_truth: Dict[str, Any]
    ) -> float:
        """计算 Rule2Spec 准确率"""

        # 关键字段匹配
        key_fields = ["city", "date", "metric", "threshold_low", "threshold_high"]
        matches = 0

        for field in key_fields:
            if output.get(field) == ground_truth.get(field):
                matches += 1

        return matches / len(key_fields)
```


    def evaluate_data_qa(
        self,
        invocation: AgentInvocation,
        actual_issues: Optional[list] = None
    ) -> AgentEvaluation:
        """评估 Data QA Agent"""

        if not invocation.output_data:
            return self._create_failed_evaluation(invocation)

        output = invocation.output_data
        confidence = output.get("confidence", 0.5)
        detected_issues = output.get("issues", [])

        # 如果有实际问题列表，计算准确率
        accuracy = None
        if actual_issues is not None:
            accuracy = self._calc_detection_accuracy(detected_issues, actual_issues)

        return AgentEvaluation(
            invocation_id=invocation.invocation_id,
            agent_type=invocation.agent_type,
            accuracy_score=accuracy,
            confidence_score=confidence,
            human_rating=None,
            latency_ms=invocation.duration_ms or 0,
            cost_usd=invocation.api_cost_usd,
            is_verified=actual_issues is not None,
            verification_method="ground_truth" if actual_issues else "none",
            verification_notes=None,
            created_at=datetime.now(),
        )

    def evaluate_resolution_sentinel(
        self,
        invocation: AgentInvocation,
        actual_outcome: Optional[str] = None
    ) -> AgentEvaluation:
        """评估 Resolution Sentinel Agent"""

        if not invocation.output_data:
            return self._create_failed_evaluation(invocation)

        output = invocation.output_data
        confidence = output.get("confidence", 0.5)
        predicted_outcome = output.get("outcome")

        # 如果有实际结果，计算准确率
        accuracy = None
        if actual_outcome and predicted_outcome:
            accuracy = 1.0 if predicted_outcome == actual_outcome else 0.0

        return AgentEvaluation(
            invocation_id=invocation.invocation_id,
            agent_type=invocation.agent_type,
            accuracy_score=accuracy,
            confidence_score=confidence,
            human_rating=None,
            latency_ms=invocation.duration_ms or 0,
            cost_usd=invocation.api_cost_usd,
            is_verified=actual_outcome is not None,
            verification_method="ground_truth" if actual_outcome else "none",
            verification_notes=None,
            created_at=datetime.now(),
        )

    def _calc_detection_accuracy(
        self,
        detected: list,
        actual: list
    ) -> float:
        """计算检测准确率（F1 score）"""

        if not actual and not detected:
            return 1.0

        if not actual or not detected:
            return 0.0

        detected_set = set(detected)
        actual_set = set(actual)

        true_positives = len(detected_set & actual_set)
        false_positives = len(detected_set - actual_set)
        false_negatives = len(actual_set - detected_set)

        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
        recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0

        if precision + recall == 0:
            return 0.0

        f1 = 2 * (precision * recall) / (precision + recall)
        return f1

    def _create_failed_evaluation(self, invocation: AgentInvocation) -> AgentEvaluation:
        """创建失败的评估结果"""
        return AgentEvaluation(
            invocation_id=invocation.invocation_id,
            agent_type=invocation.agent_type,
            accuracy_score=0.0,
            confidence_score=0.0,
            human_rating=None,
            latency_ms=invocation.duration_ms or 0,
            cost_usd=invocation.api_cost_usd,
            is_verified=False,
            verification_method="none",
            verification_notes="Agent failed",
            created_at=datetime.now(),
        )

---

### 2.2 Agent Monitor（Agent 监控器）

**文件**: `asterion_core/agents/monitor/agent_monitor.py`

#### 2.2.1 数据结构

```python
@dataclass
class AgentMetrics:
    """Agent 性能指标"""
    agent_type: AgentType
    time_window: str  # "1h" / "24h" / "7d" / "30d"

    # 调用统计
    total_invocations: int
    success_count: int
    failure_count: int
    timeout_count: int
    success_rate: float

    # 性能指标
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float

    # 质量指标
    avg_accuracy: Optional[float]
    avg_confidence: float
    avg_human_rating: Optional[float]

    # 成本指标
    total_cost_usd: float
    avg_cost_per_call_usd: float
    total_tokens: int

    # 趋势
    accuracy_trend: str  # "improving" / "stable" / "degrading"
    latency_trend: str

    calculated_at: datetime

@dataclass
class AgentAlert:
    """Agent 告警"""
    alert_id: str
    agent_type: AgentType
    severity: str  # "info" / "warning" / "critical"
    alert_type: str  # "high_latency" / "low_accuracy" / "high_failure_rate" / "cost_spike"
    message: str
    metric_value: float
    threshold_value: float
    created_at: datetime
```


#### 2.2.2 核心实现

```python
class AgentMonitor:
    """Agent 监控器"""

    def __init__(self, db_path: str, evaluator: AgentEvaluator):
        self.db_path = db_path
        self.evaluator = evaluator
        self.alert_thresholds = {
            "latency_p95_ms": 5000,
            "failure_rate": 0.1,
            "accuracy_min": 0.8,
            "cost_spike_multiplier": 3.0,
        }

    def record_invocation(self, invocation: AgentInvocation) -> None:
        """记录 Agent 调用"""
        # 保存到数据库
        self._save_invocation(invocation)

        # 触发评估
        evaluation = self._evaluate_invocation(invocation)
        self._save_evaluation(evaluation)

        # 检查告警
        alerts = self._check_alerts(invocation.agent_type)
        for alert in alerts:
            self._send_alert(alert)

    def get_metrics(
        self,
        agent_type: AgentType,
        time_window: str = "24h"
    ) -> AgentMetrics:
        """获取 Agent 性能指标"""

        # 从数据库查询统计数据
        stats = self._query_stats(agent_type, time_window)

        return AgentMetrics(
            agent_type=agent_type,
            time_window=time_window,
            total_invocations=stats["total"],
            success_count=stats["success"],
            failure_count=stats["failure"],
            timeout_count=stats["timeout"],
            success_rate=stats["success"] / stats["total"] if stats["total"] > 0 else 0,
            avg_latency_ms=stats["avg_latency"],
            p50_latency_ms=stats["p50_latency"],
            p95_latency_ms=stats["p95_latency"],
            p99_latency_ms=stats["p99_latency"],
            avg_accuracy=stats["avg_accuracy"],
            avg_confidence=stats["avg_confidence"],
            avg_human_rating=stats["avg_human_rating"],
            total_cost_usd=stats["total_cost"],
            avg_cost_per_call_usd=stats["total_cost"] / stats["total"] if stats["total"] > 0 else 0,
            total_tokens=stats["total_tokens"],
            accuracy_trend=self._calc_trend(agent_type, "accuracy", time_window),
            latency_trend=self._calc_trend(agent_type, "latency", time_window),
            calculated_at=datetime.now(),
        )

    def _check_alerts(self, agent_type: AgentType) -> list[AgentAlert]:
        """检查告警条件"""
        alerts = []
        metrics = self.get_metrics(agent_type, "1h")

        # 高延迟告警
        if metrics.p95_latency_ms > self.alert_thresholds["latency_p95_ms"]:
            alerts.append(AgentAlert(
                alert_id=f"{agent_type.value}_high_latency_{int(datetime.now().timestamp())}",
                agent_type=agent_type,
                severity="warning",
                alert_type="high_latency",
                message=f"P95 latency {metrics.p95_latency_ms}ms exceeds threshold",
                metric_value=metrics.p95_latency_ms,
                threshold_value=self.alert_thresholds["latency_p95_ms"],
                created_at=datetime.now(),
            ))

        # 高失败率告警
        if metrics.success_rate < (1 - self.alert_thresholds["failure_rate"]):
            alerts.append(AgentAlert(
                alert_id=f"{agent_type.value}_high_failure_{int(datetime.now().timestamp())}",
                agent_type=agent_type,
                severity="critical",
                alert_type="high_failure_rate",
                message=f"Success rate {metrics.success_rate:.2%} below threshold",
                metric_value=metrics.success_rate,
                threshold_value=1 - self.alert_thresholds["failure_rate"],
                created_at=datetime.now(),
            ))

        # 低准确率告警
        if metrics.avg_accuracy and metrics.avg_accuracy < self.alert_thresholds["accuracy_min"]:
            alerts.append(AgentAlert(
                alert_id=f"{agent_type.value}_low_accuracy_{int(datetime.now().timestamp())}",
                agent_type=agent_type,
                severity="warning",
                alert_type="low_accuracy",
                message=f"Accuracy {metrics.avg_accuracy:.2%} below threshold",
                metric_value=metrics.avg_accuracy,
                threshold_value=self.alert_thresholds["accuracy_min"],
                created_at=datetime.now(),
            ))

        return alerts

    def _calc_trend(self, agent_type: AgentType, metric: str, time_window: str) -> str:
        """计算指标趋势"""
        # 比较当前窗口和上一个窗口
        current = self._query_metric_avg(agent_type, metric, time_window)
        previous = self._query_metric_avg(agent_type, metric, time_window, offset=1)

        if previous == 0:
            return "stable"

        change_pct = (current - previous) / previous

        if metric == "accuracy":
            if change_pct > 0.05:
                return "improving"
            elif change_pct < -0.05:
                return "degrading"
        elif metric == "latency":
            if change_pct > 0.2:
                return "degrading"
            elif change_pct < -0.2:
                return "improving"

        return "stable"

---

### 2.3 Human Feedback System（人工反馈系统）

**文件**: `asterion_core/agents/monitor/human_feedback.py`

#### 2.3.1 数据结构

```python
@dataclass
class HumanFeedback:
    """人工反馈"""
    feedback_id: str
    invocation_id: str
    agent_type: AgentType

    # 评分
    rating: int  # 1-5
    accuracy_correct: Optional[bool]

    # 反馈内容
    feedback_text: Optional[str]
    issues_found: list[str]
    suggestions: list[str]

    # 标注数据
    corrected_output: Optional[Dict[str, Any]]

    # 元数据
    reviewer_id: str
    review_time_seconds: int
    created_at: datetime

@dataclass
class FeedbackSummary:
    """反馈汇总"""
    agent_type: AgentType
    time_window: str

    total_feedbacks: int
    avg_rating: float
    rating_distribution: Dict[int, int]

    common_issues: list[tuple[str, int]]  # [(issue, count), ...]
    common_suggestions: list[tuple[str, int]]

    accuracy_rate: float  # 人工验证的准确率

    calculated_at: datetime
```


#### 2.3.2 核心实现

```python
class HumanFeedbackCollector:
    """人工反馈收集器"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def submit_feedback(self, feedback: HumanFeedback) -> None:
        """提交人工反馈"""
        self._save_feedback(feedback)

        # 更新 Agent 评估记录
        self._update_evaluation_with_feedback(feedback)

        # 如果有修正的输出，保存为训练数据
        if feedback.corrected_output:
            self._save_training_example(feedback)

    def get_feedback_summary(
        self,
        agent_type: AgentType,
        time_window: str = "7d"
    ) -> FeedbackSummary:
        """获取反馈汇总"""

        feedbacks = self._query_feedbacks(agent_type, time_window)

        if not feedbacks:
            return FeedbackSummary(
                agent_type=agent_type,
                time_window=time_window,
                total_feedbacks=0,
                avg_rating=0.0,
                rating_distribution={},
                common_issues=[],
                common_suggestions=[],
                accuracy_rate=0.0,
                calculated_at=datetime.now(),
            )

        # 计算评分分布
        rating_dist = {}
        for fb in feedbacks:
            rating_dist[fb.rating] = rating_dist.get(fb.rating, 0) + 1

        # 统计常见问题
        issue_counts = {}
        for fb in feedbacks:
            for issue in fb.issues_found:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

        common_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        # 统计常见建议
        suggestion_counts = {}
        for fb in feedbacks:
            for suggestion in fb.suggestions:
                suggestion_counts[suggestion] = suggestion_counts.get(suggestion, 0) + 1

        common_suggestions = sorted(suggestion_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        # 计算准确率
        accuracy_feedbacks = [fb for fb in feedbacks if fb.accuracy_correct is not None]
        accuracy_rate = sum(1 for fb in accuracy_feedbacks if fb.accuracy_correct) / len(accuracy_feedbacks) if accuracy_feedbacks else 0.0

        return FeedbackSummary(
            agent_type=agent_type,
            time_window=time_window,
            total_feedbacks=len(feedbacks),
            avg_rating=sum(fb.rating for fb in feedbacks) / len(feedbacks),
            rating_distribution=rating_dist,
            common_issues=common_issues,
            common_suggestions=common_suggestions,
            accuracy_rate=accuracy_rate,
            calculated_at=datetime.now(),
        )

    def get_training_examples(
        self,
        agent_type: AgentType,
        min_rating: int = 4,
        limit: int = 100
    ) -> list[Dict[str, Any]]:
        """获取高质量训练样本"""

        # 查询高评分且有修正输出的反馈
        examples = self._query_training_examples(agent_type, min_rating, limit)

        return examples

---

## 3. 数据库设计

### 3.1 Agent 调用记录表

```sql
CREATE TABLE agent_invocations (
    invocation_id TEXT PRIMARY KEY,
    agent_type TEXT NOT NULL,
    agent_version TEXT NOT NULL,

    -- 输入输出
    input_data JSON NOT NULL,
    output_data JSON,

    -- 执行信息
    status TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_ms INTEGER,

    -- API 信息
    model_name TEXT NOT NULL,
    prompt_tokens INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL,
    api_cost_usd REAL NOT NULL,

    -- 错误信息
    error_message TEXT,
    error_traceback TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_agent_type_time (agent_type, created_at),
    INDEX idx_status (status)
);
```

### 3.2 Agent 评估表

```sql
CREATE TABLE agent_evaluations (
    evaluation_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL,
    agent_type TEXT NOT NULL,

    -- 质量评分
    accuracy_score REAL,
    confidence_score REAL NOT NULL,
    human_rating INTEGER,

    -- 性能指标
    latency_ms INTEGER NOT NULL,
    cost_usd REAL NOT NULL,

    -- 验证结果
    is_verified BOOLEAN NOT NULL,
    verification_method TEXT NOT NULL,
    verification_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (invocation_id) REFERENCES agent_invocations(invocation_id),
    INDEX idx_agent_type_time (agent_type, created_at)
);
```

### 3.3 人工反馈表

```sql
CREATE TABLE human_feedbacks (
    feedback_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL,
    agent_type TEXT NOT NULL,

    -- 评分
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    accuracy_correct BOOLEAN,

    -- 反馈内容
    feedback_text TEXT,
    issues_found JSON,
    suggestions JSON,

    -- 标注数据
    corrected_output JSON,

    -- 元数据
    reviewer_id TEXT NOT NULL,
    review_time_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (invocation_id) REFERENCES agent_invocations(invocation_id),
    INDEX idx_agent_type_time (agent_type, created_at),
    INDEX idx_rating (rating)
);
```


### 3.4 Agent 告警表

```sql
CREATE TABLE agent_alerts (
    alert_id TEXT PRIMARY KEY,
    agent_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    message TEXT NOT NULL,
    metric_value REAL NOT NULL,
    threshold_value REAL NOT NULL,

    -- 处理状态
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolved_by TEXT,
    resolution_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_agent_type_severity (agent_type, severity),
    INDEX idx_unresolved (is_resolved, created_at)
);
```

### 3.5 训练样本表

```sql
CREATE TABLE agent_training_examples (
    example_id TEXT PRIMARY KEY,
    agent_type TEXT NOT NULL,
    feedback_id TEXT NOT NULL,

    input_data JSON NOT NULL,
    expected_output JSON NOT NULL,

    quality_score REAL NOT NULL,
    is_validated BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (feedback_id) REFERENCES human_feedbacks(feedback_id),
    INDEX idx_agent_type_quality (agent_type, quality_score DESC)
);
```

---

## 4. 使用示例

### 4.1 记录 Agent 调用

```python
from asterion_core.agents.monitor import AgentMonitor, AgentEvaluator
from asterion_core.agents.monitor.agent_evaluator import AgentInvocation, AgentType, AgentStatus
from datetime import datetime
import uuid

# 初始化
evaluator = AgentEvaluator(db_path="asterion.db")
monitor = AgentMonitor(db_path="asterion.db", evaluator=evaluator)

# 调用 Rule2Spec Agent
start_time = datetime.now()
try:
    # 调用 Agent
    output = rule2spec_agent.parse(market_rules)

    # 记录成功调用
    invocation = AgentInvocation(
        invocation_id=str(uuid.uuid4()),
        agent_type=AgentType.RULE2SPEC,
        agent_version="v1.0",
        input_data={"rules": market_rules},
        output_data=output,
        status=AgentStatus.SUCCESS,
        start_time=start_time,
        end_time=datetime.now(),
        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000),
        model_name="claude-opus-4-6",
        prompt_tokens=1500,
        completion_tokens=500,
        total_tokens=2000,
        api_cost_usd=0.03,
        error_message=None,
        error_traceback=None,
    )

    monitor.record_invocation(invocation)

except Exception as e:
    # 记录失败调用
    invocation = AgentInvocation(
        invocation_id=str(uuid.uuid4()),
        agent_type=AgentType.RULE2SPEC,
        agent_version="v1.0",
        input_data={"rules": market_rules},
        output_data=None,
        status=AgentStatus.FAILURE,
        start_time=start_time,
        end_time=datetime.now(),
        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000),
        model_name="claude-opus-4-6",
        prompt_tokens=1500,
        completion_tokens=0,
        total_tokens=1500,
        api_cost_usd=0.015,
        error_message=str(e),
        error_traceback=traceback.format_exc(),
    )

    monitor.record_invocation(invocation)
```

### 4.2 查询性能指标

```python
# 获取 24 小时性能指标
metrics = monitor.get_metrics(AgentType.RULE2SPEC, time_window="24h")

print(f"Total invocations: {metrics.total_invocations}")
print(f"Success rate: {metrics.success_rate:.2%}")
print(f"Avg latency: {metrics.avg_latency_ms}ms")
print(f"P95 latency: {metrics.p95_latency_ms}ms")
print(f"Avg accuracy: {metrics.avg_accuracy:.2%}" if metrics.avg_accuracy else "N/A")
print(f"Total cost: ${metrics.total_cost_usd:.2f}")
print(f"Accuracy trend: {metrics.accuracy_trend}")
```

### 4.3 提交人工反馈

```python
from asterion_core.agents.monitor.human_feedback import HumanFeedback, HumanFeedbackCollector

collector = HumanFeedbackCollector(db_path="asterion.db")

# 人工审核 Agent 输出
feedback = HumanFeedback(
    feedback_id=str(uuid.uuid4()),
    invocation_id="inv_123",
    agent_type=AgentType.RULE2SPEC,
    rating=4,
    accuracy_correct=True,
    feedback_text="Good parsing but missed timezone info",
    issues_found=["missing_timezone"],
    suggestions=["Add explicit timezone extraction"],
    corrected_output={
        "city": "New York",
        "date": "2026-03-15",
        "metric": "high_temp",
        "threshold_low": 70,
        "threshold_high": 75,
        "timezone": "America/New_York",  # 修正
    },
    reviewer_id="operator_001",
    review_time_seconds=45,
    created_at=datetime.now(),
)

collector.submit_feedback(feedback)
```


### 4.4 获取反馈汇总

```python
# 获取 7 天反馈汇总
summary = collector.get_feedback_summary(AgentType.RULE2SPEC, time_window="7d")

print(f"Total feedbacks: {summary.total_feedbacks}")
print(f"Avg rating: {summary.avg_rating:.1f}/5")
print(f"Accuracy rate: {summary.accuracy_rate:.2%}")
print(f"\nCommon issues:")
for issue, count in summary.common_issues:
    print(f"  - {issue}: {count}")
print(f"\nCommon suggestions:")
for suggestion, count in summary.common_suggestions:
    print(f"  - {suggestion}: {count}")
```

### 4.5 获取训练样本

```python
# 获取高质量训练样本用于 prompt 优化
examples = collector.get_training_examples(
    agent_type=AgentType.RULE2SPEC,
    min_rating=4,
    limit=50
)

print(f"Found {len(examples)} high-quality examples")
for ex in examples[:3]:
    print(f"\nInput: {ex['input_data']}")
    print(f"Expected output: {ex['expected_output']}")
```

---

## 5. Operator UI 集成

### 5.1 Agent 监控面板

**文件**: `ui/pages/agent_monitor.py`

```python
import streamlit as st
from asterion_core.agents.monitor import AgentMonitor, AgentEvaluator
from asterion_core.agents.monitor.agent_evaluator import AgentType

st.title("Agent Monitor Dashboard")

# 选择 Agent 类型
agent_type = st.selectbox(
    "Agent Type",
    options=[t.value for t in AgentType],
)

# 选择时间窗口
time_window = st.selectbox(
    "Time Window",
    options=["1h", "24h", "7d", "30d"],
    index=1,
)

# 获取指标
monitor = AgentMonitor(db_path="asterion.db", evaluator=AgentEvaluator(db_path="asterion.db"))
metrics = monitor.get_metrics(AgentType(agent_type), time_window)

# 显示关键指标
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Success Rate", f"{metrics.success_rate:.1%}")
with col2:
    st.metric("Avg Latency", f"{metrics.avg_latency_ms:.0f}ms")
with col3:
    st.metric("Avg Accuracy", f"{metrics.avg_accuracy:.1%}" if metrics.avg_accuracy else "N/A")
with col4:
    st.metric("Total Cost", f"${metrics.total_cost_usd:.2f}")

# 显示趋势
st.subheader("Trends")
col1, col2 = st.columns(2)
with col1:
    st.metric("Accuracy Trend", metrics.accuracy_trend)
with col2:
    st.metric("Latency Trend", metrics.latency_trend)

# 显示详细统计
st.subheader("Detailed Statistics")
st.write(f"Total invocations: {metrics.total_invocations}")
st.write(f"Success: {metrics.success_count}, Failure: {metrics.failure_count}, Timeout: {metrics.timeout_count}")
st.write(f"P50 latency: {metrics.p50_latency_ms}ms")
st.write(f"P95 latency: {metrics.p95_latency_ms}ms")
st.write(f"P99 latency: {metrics.p99_latency_ms}ms")
```

### 5.2 人工反馈界面

**文件**: `ui/pages/agent_feedback.py`

```python
import streamlit as st
from asterion_core.agents.monitor.human_feedback import HumanFeedback, HumanFeedbackCollector
import uuid
from datetime import datetime

st.title("Agent Feedback")

# 获取待审核的 Agent 调用
collector = HumanFeedbackCollector(db_path="asterion.db")
pending_invocations = collector.get_pending_reviews(limit=10)

if not pending_invocations:
    st.info("No pending reviews")
    st.stop()

# 选择一个调用
invocation = st.selectbox(
    "Select invocation to review",
    options=pending_invocations,
    format_func=lambda x: f"{x['agent_type']} - {x['invocation_id'][:8]}",
)

# 显示输入输出
st.subheader("Input")
st.json(invocation["input_data"])

st.subheader("Output")
st.json(invocation["output_data"])

# 反馈表单
with st.form("feedback_form"):
    rating = st.slider("Rating", 1, 5, 3)
    accuracy_correct = st.radio("Is the output accurate?", ["Yes", "No", "Unsure"])
    feedback_text = st.text_area("Feedback")
    issues = st.multiselect(
        "Issues found",
        options=["missing_field", "wrong_value", "format_error", "logic_error", "other"],
    )
    suggestions = st.text_area("Suggestions (one per line)")

    submitted = st.form_submit_button("Submit Feedback")

    if submitted:
        feedback = HumanFeedback(
            feedback_id=str(uuid.uuid4()),
            invocation_id=invocation["invocation_id"],
            agent_type=AgentType(invocation["agent_type"]),
            rating=rating,
            accuracy_correct=accuracy_correct == "Yes" if accuracy_correct != "Unsure" else None,
            feedback_text=feedback_text if feedback_text else None,
            issues_found=issues,
            suggestions=suggestions.split("\n") if suggestions else [],
            corrected_output=None,
            reviewer_id=st.session_state.get("user_id", "operator"),
            review_time_seconds=0,
            created_at=datetime.now(),
        )

        collector.submit_feedback(feedback)
        st.success("Feedback submitted!")
        st.rerun()
```


---

## 6. 监控指标和告警

### 6.1 关键监控指标

#### 6.1.1 性能指标

| 指标 | 目标值 | 告警阈值 |
|------|--------|----------|
| P95 延迟 | < 3s | > 5s |
| P99 延迟 | < 5s | > 10s |
| 成功率 | > 95% | < 90% |
| 超时率 | < 1% | > 5% |

#### 6.1.2 质量指标

| Agent | 准确率目标 | 告警阈值 |
|-------|-----------|----------|
| Rule2Spec | > 90% | < 80% |
| Data QA | > 85% | < 75% |
| Resolution Sentinel | > 95% | < 90% |
| Daily Review | N/A | N/A |

#### 6.1.3 成本指标

| 指标 | 目标值 | 告警阈值 |
|------|--------|----------|
| 单次调用成本 | < $0.05 | > $0.10 |
| 日总成本 | < $10 | > $20 |
| Token 使用量 | < 100K/day | > 200K/day |

### 6.2 告警规则

```python
ALERT_RULES = {
    "high_latency": {
        "metric": "p95_latency_ms",
        "threshold": 5000,
        "severity": "warning",
        "window": "1h",
    },
    "high_failure_rate": {
        "metric": "failure_rate",
        "threshold": 0.1,
        "severity": "critical",
        "window": "1h",
    },
    "low_accuracy": {
        "metric": "avg_accuracy",
        "threshold": 0.8,
        "severity": "warning",
        "window": "24h",
    },
    "cost_spike": {
        "metric": "cost_per_hour",
        "threshold_multiplier": 3.0,  # 3x baseline
        "severity": "warning",
        "window": "1h",
    },
    "timeout_spike": {
        "metric": "timeout_rate",
        "threshold": 0.05,
        "severity": "critical",
        "window": "1h",
    },
}
```

### 6.3 告警通知

```python
class AlertNotifier:
    """告警通知器"""

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url

    def send_alert(self, alert: AgentAlert) -> None:
        """发送告警"""

        # 记录到日志
        logger.warning(f"Agent alert: {alert.message}")

        # 发送到 Slack/Discord
        if self.webhook_url:
            self._send_webhook(alert)

        # 保存到数据库
        self._save_alert(alert)

    def _send_webhook(self, alert: AgentAlert) -> None:
        """发送 webhook 通知"""
        payload = {
            "text": f"🚨 Agent Alert: {alert.agent_type.value}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{alert.severity.upper()}*: {alert.message}"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Type:*\n{alert.alert_type}"},
                        {"type": "mrkdwn", "text": f"*Value:*\n{alert.metric_value:.2f}"},
                        {"type": "mrkdwn", "text": f"*Threshold:*\n{alert.threshold_value:.2f}"},
                    ]
                }
            ]
        }

        requests.post(self.webhook_url, json=payload)
```

---

## 7. 持续改进机制

### 7.1 Prompt 优化流程

```python
class PromptOptimizer:
    """Prompt 优化器"""

    def __init__(self, feedback_collector: HumanFeedbackCollector):
        self.feedback_collector = feedback_collector

    def analyze_feedback(self, agent_type: AgentType) -> Dict[str, Any]:
        """分析反馈，识别改进机会"""

        summary = self.feedback_collector.get_feedback_summary(agent_type, "30d")

        # 识别常见问题
        common_issues = summary.common_issues[:3]

        # 获取低评分样本
        low_rating_examples = self._get_low_rating_examples(agent_type, max_rating=2)

        # 获取高评分样本
        high_rating_examples = self._get_high_rating_examples(agent_type, min_rating=4)

        return {
            "common_issues": common_issues,
            "low_rating_count": len(low_rating_examples),
            "high_rating_count": len(high_rating_examples),
            "improvement_suggestions": self._generate_suggestions(common_issues),
        }

    def _generate_suggestions(self, issues: list) -> list[str]:
        """根据常见问题生成改进建议"""
        suggestions = []

        for issue, count in issues:
            if issue == "missing_field":
                suggestions.append("Add explicit field extraction instructions to prompt")
            elif issue == "wrong_value":
                suggestions.append("Add validation examples to prompt")
            elif issue == "format_error":
                suggestions.append("Add output format schema to prompt")

        return suggestions
```

### 7.2 A/B 测试框架

```python
class AgentABTest:
    """Agent A/B 测试"""

    def __init__(self, agent_type: AgentType, variant_a: str, variant_b: str):
        self.agent_type = agent_type
        self.variant_a = variant_a
        self.variant_b = variant_b
        self.traffic_split = 0.5  # 50/50 split

    def select_variant(self, invocation_id: str) -> str:
        """选择测试变体"""
        # 使用 invocation_id 的哈希值决定分组
        hash_val = int(hashlib.md5(invocation_id.encode()).hexdigest(), 16)
        return self.variant_a if hash_val % 2 == 0 else self.variant_b

    def get_test_results(self, min_samples: int = 100) -> Dict[str, Any]:
        """获取测试结果"""

        results_a = self._get_variant_metrics(self.variant_a)
        results_b = self._get_variant_metrics(self.variant_b)

        if results_a["sample_count"] < min_samples or results_b["sample_count"] < min_samples:
            return {"status": "insufficient_data"}

        # 比较关键指标
        comparison = {
            "accuracy": {
                "variant_a": results_a["avg_accuracy"],
                "variant_b": results_b["avg_accuracy"],
                "winner": "a" if results_a["avg_accuracy"] > results_b["avg_accuracy"] else "b",
            },
            "latency": {
                "variant_a": results_a["avg_latency"],
                "variant_b": results_b["avg_latency"],
                "winner": "a" if results_a["avg_latency"] < results_b["avg_latency"] else "b",
            },
            "cost": {
                "variant_a": results_a["avg_cost"],
                "variant_b": results_b["avg_cost"],
                "winner": "a" if results_a["avg_cost"] < results_b["avg_cost"] else "b",
            },
        }

        return {
            "status": "complete",
            "comparison": comparison,
            "recommendation": self._make_recommendation(comparison),
        }
```


---

## 8. 测试策略

### 8.1 单元测试

```python
import pytest
from asterion_core.agents.monitor import AgentEvaluator
from asterion_core.agents.monitor.agent_evaluator import AgentInvocation, AgentType, AgentStatus
from datetime import datetime
from decimal import Decimal

def test_rule2spec_accuracy_calculation():
    """测试 Rule2Spec 准确率计算"""
    evaluator = AgentEvaluator(db_path=":memory:")

    invocation = AgentInvocation(
        invocation_id="test_001",
        agent_type=AgentType.RULE2SPEC,
        agent_version="v1.0",
        input_data={"rules": "test rules"},
        output_data={
            "city": "New York",
            "date": "2026-03-15",
            "metric": "high_temp",
            "threshold_low": 70,
            "threshold_high": 75,
        },
        status=AgentStatus.SUCCESS,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_ms=1000,
        model_name="claude-opus-4-6",
        prompt_tokens=1000,
        completion_tokens=500,
        total_tokens=1500,
        api_cost_usd=0.02,
        error_message=None,
        error_traceback=None,
    )

    ground_truth = {
        "city": "New York",
        "date": "2026-03-15",
        "metric": "high_temp",
        "threshold_low": 70,
        "threshold_high": 75,
    }

    evaluation = evaluator.evaluate_rule2spec(invocation, ground_truth)

    assert evaluation.accuracy_score == 1.0
    assert evaluation.is_verified is True

def test_data_qa_detection_accuracy():
    """测试 Data QA 检测准确率"""
    evaluator = AgentEvaluator(db_path=":memory:")

    invocation = AgentInvocation(
        invocation_id="test_002",
        agent_type=AgentType.DATA_QA,
        agent_version="v1.0",
        input_data={"data": "test data"},
        output_data={
            "issues": ["missing_data", "outlier"],
            "confidence": 0.9,
        },
        status=AgentStatus.SUCCESS,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_ms=800,
        model_name="claude-opus-4-6",
        prompt_tokens=800,
        completion_tokens=200,
        total_tokens=1000,
        api_cost_usd=0.015,
        error_message=None,
        error_traceback=None,
    )

    actual_issues = ["missing_data", "outlier", "format_error"]

    evaluation = evaluator.evaluate_data_qa(invocation, actual_issues)

    # F1 score: TP=2, FP=0, FN=1
    # Precision=1.0, Recall=0.67, F1=0.8
    assert evaluation.accuracy_score == pytest.approx(0.8, abs=0.01)
```

### 8.2 集成测试

```python
def test_monitor_workflow():
    """测试完整监控流程"""
    evaluator = AgentEvaluator(db_path=":memory:")
    monitor = AgentMonitor(db_path=":memory:", evaluator=evaluator)

    # 记录多次调用
    for i in range(10):
        invocation = create_test_invocation(f"test_{i}")
        monitor.record_invocation(invocation)

    # 获取指标
    metrics = monitor.get_metrics(AgentType.RULE2SPEC, "1h")

    assert metrics.total_invocations == 10
    assert metrics.success_rate > 0
    assert metrics.avg_latency_ms > 0

def test_feedback_collection():
    """测试反馈收集"""
    collector = HumanFeedbackCollector(db_path=":memory:")

    feedback = HumanFeedback(
        feedback_id="fb_001",
        invocation_id="inv_001",
        agent_type=AgentType.RULE2SPEC,
        rating=4,
        accuracy_correct=True,
        feedback_text="Good output",
        issues_found=[],
        suggestions=["Add timezone"],
        corrected_output=None,
        reviewer_id="test_reviewer",
        review_time_seconds=30,
        created_at=datetime.now(),
    )

    collector.submit_feedback(feedback)

    summary = collector.get_feedback_summary(AgentType.RULE2SPEC, "7d")
    assert summary.total_feedbacks == 1
    assert summary.avg_rating == 4.0
```

### 8.3 性能测试

```python
def test_monitor_performance():
    """测试监控性能"""
    import time

    evaluator = AgentEvaluator(db_path=":memory:")
    monitor = AgentMonitor(db_path=":memory:", evaluator=evaluator)

    # 记录 1000 次调用
    start = time.time()
    for i in range(1000):
        invocation = create_test_invocation(f"perf_{i}")
        monitor.record_invocation(invocation)
    duration = time.time() - start

    # 应该在 10 秒内完成
    assert duration < 10.0

    # 查询性能
    start = time.time()
    metrics = monitor.get_metrics(AgentType.RULE2SPEC, "1h")
    query_duration = time.time() - start

    # 查询应该在 100ms 内完成
    assert query_duration < 0.1
```

---

## 9. 部署和运维

### 9.1 部署架构

```
┌─────────────────────────────────────────────────────────┐
│                    Asterion Platform                     │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────┐         ┌──────────────┐              │
│  │   Agents     │────────▶│Agent Monitor │              │
│  │  (4 types)   │         │              │              │
│  └──────────────┘         └──────┬───────┘              │
│                                   │                       │
│                          ┌────────▼────────┐             │
│                          │   DuckDB        │             │
│                          │  (metrics DB)   │             │
│                          └─────────────────┘             │
│                                                           │
│  ┌──────────────┐         ┌──────────────┐              │
│  │ Operator UI  │────────▶│Alert Notifier│              │
│  │ (Streamlit)  │         │  (Webhook)   │              │
│  └──────────────┘         └──────────────┘              │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

### 9.2 配置管理

```python
# config/agent_monitor.yaml
agent_monitor:
  database:
    path: "data/asterion.db"
    backup_enabled: true
    backup_interval_hours: 24

  alert_thresholds:
    latency_p95_ms: 5000
    failure_rate: 0.1
    accuracy_min: 0.8
    cost_spike_multiplier: 3.0

  alert_notifier:
    webhook_url: "${SLACK_WEBHOOK_URL}"
    enabled: true

  feedback:
    auto_request_on_low_confidence: true
    confidence_threshold: 0.7

  ab_testing:
    enabled: false
    traffic_split: 0.5
```

### 9.3 监控数据保留策略

```python
RETENTION_POLICY = {
    "agent_invocations": {
        "raw_data": "90d",  # 保留 90 天原始数据
        "aggregated": "1y",  # 保留 1 年聚合数据
    },
    "agent_evaluations": {
        "raw_data": "90d",
        "aggregated": "1y",
    },
    "human_feedbacks": {
        "raw_data": "永久",  # 人工反馈永久保留
    },
    "agent_alerts": {
        "resolved": "30d",  # 已解决告警保留 30 天
        "unresolved": "永久",
    },
}
```


### 9.4 数据清理任务

```python
class DataRetentionManager:
    """数据保留管理器"""

    def __init__(self, db_path: str, retention_policy: Dict[str, Any]):
        self.db_path = db_path
        self.retention_policy = retention_policy

    def cleanup_old_data(self) -> Dict[str, int]:
        """清理过期数据"""
        results = {}

        # 清理过期的 invocations
        deleted = self._cleanup_invocations()
        results["invocations_deleted"] = deleted

        # 清理过期的 evaluations
        deleted = self._cleanup_evaluations()
        results["evaluations_deleted"] = deleted

        # 清理已解决的告警
        deleted = self._cleanup_resolved_alerts()
        results["alerts_deleted"] = deleted

        return results

    def _cleanup_invocations(self) -> int:
        """清理过期的调用记录"""
        retention_days = int(self.retention_policy["agent_invocations"]["raw_data"].rstrip("d"))

        query = f"""
        DELETE FROM agent_invocations
        WHERE created_at < datetime('now', '-{retention_days} days')
        """

        # 执行删除
        return self._execute_delete(query)
```

---

## 10. 关键指标和 SLA

### 10.1 系统性能 SLA

| 指标 | SLA | 测量方法 |
|------|-----|----------|
| 监控记录延迟 | < 100ms | 从 Agent 完成到记录入库的时间 |
| 指标查询延迟 | < 500ms | 查询 24h 指标的响应时间 |
| 告警触发延迟 | < 1min | 从异常发生到告警发送的时间 |
| 数据库写入吞吐 | > 100 TPS | 每秒可记录的调用数 |

### 10.2 监控覆盖率

| Agent | 监控覆盖率 | 人工审核率 |
|-------|-----------|-----------|
| Rule2Spec | 100% | 10% (抽样) |
| Data QA | 100% | 5% (抽样) |
| Resolution Sentinel | 100% | 100% (关键) |
| Daily Review | 100% | 20% (抽样) |

### 10.3 质量目标

| 指标 | MVP 目标 | 长期目标 |
|------|---------|----------|
| Rule2Spec 准确率 | > 85% | > 95% |
| Data QA F1 Score | > 80% | > 90% |
| Resolution Sentinel 准确率 | > 90% | > 98% |
| 人工反馈响应时间 | < 24h | < 4h |

---

## 11. 最佳实践

### 11.1 Agent 设计原则

1. **输出结构化** - 所有 Agent 输出必须是结构化 JSON，便于解析和评估
2. **置信度评分** - Agent 必须输出置信度评分，用于触发人工审核
3. **可解释性** - Agent 应输出推理过程，便于调试和改进
4. **幂等性** - 相同输入应产生相同输出，便于回测和验证

### 11.2 监控最佳实践

1. **实时监控** - 所有 Agent 调用实时记录，不要批量延迟记录
2. **异常捕获** - 捕获所有异常并记录完整堆栈，便于调试
3. **成本追踪** - 精确追踪每次调用的 token 和成本
4. **趋势分析** - 定期分析趋势，及早发现性能退化

### 11.3 反馈收集最佳实践

1. **低置信度优先** - 优先审核低置信度的输出
2. **关键路径全覆盖** - Resolution Sentinel 等关键 Agent 100% 审核
3. **快速反馈循环** - 反馈应在 24 小时内处理并用于改进
4. **标注质量控制** - 定期抽查人工标注质量

---

## 12. 故障排查指南

### 12.1 常见问题

#### 问题 1: Agent 准确率突然下降

**可能原因**:
- Prompt 版本变更
- 输入数据质量下降
- API 模型更新
- 市场规则变化

**排查步骤**:
1. 检查最近的 prompt 变更
2. 对比低准确率样本和高准确率样本
3. 检查输入数据质量指标
4. 查看人工反馈中的常见问题

#### 问题 2: Agent 延迟增加

**可能原因**:
- API 限流
- 输入数据量增加
- 网络问题
- 数据库性能问题

**排查步骤**:
1. 检查 API 响应时间
2. 检查输入 token 数量趋势
3. 检查网络延迟
4. 检查数据库查询性能

#### 问题 3: 成本突然增加

**可能原因**:
- 调用频率增加
- 输入/输出 token 增加
- 使用了更贵的模型

**排查步骤**:
1. 检查调用频率趋势
2. 检查平均 token 使用量
3. 检查模型版本
4. 查看成本异常的具体调用

### 12.2 调试工具

```python
class AgentDebugger:
    """Agent 调试工具"""

    def replay_invocation(self, invocation_id: str) -> Dict[str, Any]:
        """重放 Agent 调用"""
        invocation = self._load_invocation(invocation_id)

        # 使用相同输入重新调用 Agent
        result = self._call_agent(
            agent_type=invocation.agent_type,
            input_data=invocation.input_data,
        )

        return {
            "original_output": invocation.output_data,
            "replay_output": result,
            "is_consistent": invocation.output_data == result,
        }

    def compare_versions(
        self,
        invocation_id: str,
        version_a: str,
        version_b: str
    ) -> Dict[str, Any]:
        """比较不同版本的输出"""
        invocation = self._load_invocation(invocation_id)

        output_a = self._call_agent_version(
            agent_type=invocation.agent_type,
            version=version_a,
            input_data=invocation.input_data,
        )

        output_b = self._call_agent_version(
            agent_type=invocation.agent_type,
            version=version_b,
            input_data=invocation.input_data,
        )

        return {
            "version_a": output_a,
            "version_b": output_b,
            "diff": self._compute_diff(output_a, output_b),
        }
```


---

## 13. 与其他模块的集成

### 13.1 与 Operator UI 集成

Agent Monitor 为 Operator UI 提供以下接口：

```python
# ui/components/agent_status_widget.py
def render_agent_status():
    """渲染 Agent 状态组件"""
    monitor = get_agent_monitor()

    for agent_type in AgentType:
        metrics = monitor.get_metrics(agent_type, "1h")

        status = "🟢" if metrics.success_rate > 0.95 else "🟡" if metrics.success_rate > 0.9 else "🔴"

        st.metric(
            label=f"{status} {agent_type.value}",
            value=f"{metrics.success_rate:.1%}",
            delta=f"{metrics.avg_latency_ms:.0f}ms"
        )
```

### 13.2 与 Risk Management 集成

当 Agent 性能退化时，触发风控机制：

```python
class AgentRiskIntegration:
    """Agent 风控集成"""

    def check_agent_health(self, agent_type: AgentType) -> bool:
        """检查 Agent 健康状态"""
        metrics = self.monitor.get_metrics(agent_type, "1h")

        # 如果准确率过低，禁用自动执行
        if metrics.avg_accuracy and metrics.avg_accuracy < 0.7:
            self.risk_manager.disable_auto_execution(
                reason=f"{agent_type.value} accuracy too low: {metrics.avg_accuracy:.2%}"
            )
            return False

        return True
```

### 13.3 与 Dagster 集成

定期运行监控任务：

```python
from dagster import asset, AssetExecutionContext

@asset(group_name="monitoring")
def agent_metrics_hourly(context: AssetExecutionContext):
    """每小时计算 Agent 指标"""
    monitor = get_agent_monitor()

    for agent_type in AgentType:
        metrics = monitor.get_metrics(agent_type, "1h")
        context.log.info(f"{agent_type.value}: {metrics.success_rate:.2%} success rate")

@asset(group_name="monitoring")
def cleanup_old_data_daily(context: AssetExecutionContext):
    """每天清理过期数据"""
    manager = DataRetentionManager(db_path="asterion.db", retention_policy=RETENTION_POLICY)
    results = manager.cleanup_old_data()
    context.log.info(f"Cleaned up: {results}")
```

---

## 14. 路线图

### Phase 1: MVP（当前）
- ✅ 基础监控框架
- ✅ 4 个 Agent 的监控
- ✅ 人工反馈收集
- ✅ 基础告警机制
- ⏳ Operator UI 集成

### Phase 2: 增强（Q2 2026）
- ⏳ A/B 测试框架
- ⏳ 自动 prompt 优化
- ⏳ 高级异常检测
- ⏳ 成本优化建议

### Phase 3: 智能化（Q3 2026）
- ⏳ 自动化反馈标注
- ⏳ 预测性告警
- ⏳ Agent 性能预测
- ⏳ 自适应阈值调整

---

## 15. 附录

### 15.1 术语表

| 术语 | 定义 |
|------|------|
| Invocation | Agent 的一次调用 |
| Evaluation | 对 Agent 输出的评估 |
| Accuracy | Agent 输出的准确率 |
| Confidence | Agent 自评的置信度 |
| Human Rating | 人工评分（1-5） |
| F1 Score | 检测任务的综合指标 |
| P95 Latency | 95 分位延迟 |
| Cost Spike | 成本异常增长 |

### 15.2 参考资料

- [Anthropic API Documentation](https://docs.anthropic.com/)
- [Agent Evaluation Best Practices](https://www.anthropic.com/research)
- [Prompt Engineering Guide](https://www.promptingguide.ai/)

### 15.3 FAQ

**Q: 为什么不使用 LangSmith 或 LangFuse？**

A: 我们需要完全控制监控逻辑和数据存储，且需要与 Asterion 的风控系统深度集成。自建监控系统更灵活。

**Q: 人工反馈的成本如何控制？**

A: 通过置信度阈值和抽样策略控制。低置信度输出优先审核，高置信度输出抽样审核。

**Q: 如何处理 Agent 输出不一致的问题？**

A: 记录所有调用的完整上下文，支持重放和调试。如果发现不一致，可以通过 A/B 测试验证不同 prompt 版本。

**Q: 监控数据会占用多少存储空间？**

A: 预计每天 1000 次调用，每条记录约 10KB，每天约 10MB。90 天保留期约 900MB，可接受。

---

## 16. 总结

Agent Monitor 是 Asterion 平台的关键组件，确保所有 AI Agent 的性能、质量和成本可控。

**核心价值**:
1. **可观测性** - 所有 Agent 行为完全可追溯
2. **质量保证** - 通过人工反馈持续改进
3. **风险控制** - 及时发现和处理异常
4. **成本优化** - 精确追踪和优化 API 成本

**设计亮点**:
1. **轻量级** - 最小化对 Agent 调用的性能影响
2. **可扩展** - 支持新 Agent 类型的快速接入
3. **自动化** - 自动评估、告警、清理
4. **人机协同** - 人工反馈与自动监控结合

**下一步**:
1. 实现基础监控框架
2. 集成到 Operator UI
3. 收集真实数据验证指标
4. 根据反馈迭代优化

---

## 17. P2-11 增强总结

### 17.1 新增指标

✅ **schema_valid_rate（Schema 有效率）**
- 验证 Agent 输出是否符合预期 schema
- 检测结构化输出的完整性
- 及早发现格式错误

**实现**:
```python
class SchemaValidator:
    """Schema 验证器"""

    def validate(self, output: Dict, expected_schema: Dict) -> bool:
        """验证输出是否符合 schema"""
        try:
            # 使用 jsonschema 验证
            from jsonschema import validate, ValidationError
            validate(instance=output, schema=expected_schema)
            return True
        except ValidationError:
            return False

class AgentEvaluator:
    """Agent 评估器（增强版）"""

    def evaluate(self, invocation: AgentInvocation) -> AgentEvaluation:
        """评估 Agent 调用（新增 schema_valid_rate）"""

        # ... 原有评估逻辑 ...

        # 新增：Schema 验证
        schema_valid = self.schema_validator.validate(
            invocation.output_data,
            self._get_expected_schema(invocation.agent_type)
        )

        return AgentEvaluation(
            # ... 原有字段 ...
            schema_valid=schema_valid,
        )

    def calculate_metrics(self, agent_type: AgentType, time_window: str) -> Dict:
        """计算指标（新增 schema_valid_rate）"""

        evaluations = self._query_evaluations(agent_type, time_window)

        schema_valid_count = sum(1 for e in evaluations if e.schema_valid)
        schema_valid_rate = schema_valid_count / len(evaluations) if evaluations else 0

        return {
            # ... 原有指标 ...
            'schema_valid_rate': schema_valid_rate,
        }
```

✅ **calibration（校准度）**
- 评估 Agent 置信度的准确性
- 检测过度自信或过度保守
- 优化置信度阈值

**实现**:
```python
class CalibrationAnalyzer:
    """校准度分析器"""

    def calculate_calibration(
        self,
        predictions: List[Tuple[float, bool]]  # [(confidence, is_correct), ...]
    ) -> float:
        """计算校准度（Expected Calibration Error）"""

        # 将置信度分桶
        bins = np.linspace(0, 1, 11)  # [0, 0.1, 0.2, ..., 1.0]
        bin_indices = np.digitize([p[0] for p in predictions], bins) - 1

        ece = 0.0
        for i in range(len(bins) - 1):
            # 获取该桶内的预测
            bin_predictions = [p for j, p in enumerate(predictions) if bin_indices[j] == i]

            if not bin_predictions:
                continue

            # 计算平均置信度和准确率
            avg_confidence = np.mean([p[0] for p in bin_predictions])
            accuracy = np.mean([p[1] for p in bin_predictions])

            # 计算该桶的误差
            bin_error = abs(avg_confidence - accuracy)
            bin_weight = len(bin_predictions) / len(predictions)

            ece += bin_weight * bin_error

        return ece

class AgentEvaluator:
    """Agent 评估器（增强版）"""

    def calculate_metrics(self, agent_type: AgentType, time_window: str) -> Dict:
        """计算指标（新增 calibration）"""

        evaluations = self._query_evaluations(agent_type, time_window)

        # 提取置信度和准确性
        predictions = [
            (e.confidence_score, e.accuracy_correct)
            for e in evaluations
            if e.confidence_score is not None and e.accuracy_correct is not None
        ]

        calibration = self.calibration_analyzer.calculate_calibration(predictions)

        return {
            # ... 原有指标 ...
            'calibration': calibration,  # 越接近 0 越好
        }
```

✅ **prompt_version_drift（Prompt 版本漂移）**
- 检测 prompt 版本变更的影响
- 追踪不同版本的性能差异
- 支持 A/B 测试

**实现**:
```python
class PromptVersionTracker:
    """Prompt 版本追踪器"""

    def detect_drift(
        self,
        agent_type: AgentType,
        current_version: str,
        baseline_version: str,
        time_window: str = "7d"
    ) -> Dict:
        """检测 prompt 版本漂移"""

        # 1. 获取当前版本的指标
        current_metrics = self._get_metrics(agent_type, current_version, time_window)

        # 2. 获取基线版本的指标
        baseline_metrics = self._get_metrics(agent_type, baseline_version, time_window)

        # 3. 计算差异
        drift = {
            'accuracy_drift': current_metrics['accuracy'] - baseline_metrics['accuracy'],
            'latency_drift': current_metrics['avg_latency_ms'] - baseline_metrics['avg_latency_ms'],
            'cost_drift': current_metrics['avg_cost_usd'] - baseline_metrics['avg_cost_usd'],
        }

        # 4. 判断是否显著
        drift['is_significant'] = abs(drift['accuracy_drift']) > 0.05  # 5% 阈值

        return drift

class AgentMonitor:
    """Agent 监控器（增强版）"""

    def check_prompt_version_drift(self):
        """检查 prompt 版本漂移"""

        for agent_type in AgentType:
            # 获取当前版本和基线版本
            current_version = self._get_current_version(agent_type)
            baseline_version = self._get_baseline_version(agent_type)

            if current_version == baseline_version:
                continue

            # 检测漂移
            drift = self.prompt_version_tracker.detect_drift(
                agent_type, current_version, baseline_version
            )

            if drift['is_significant']:
                # 发送告警
                self._alert_prompt_drift(agent_type, drift)
```

✅ **cost_per_accepted_output（每个被接受输出的成本）**
- 计算实际有效输出的成本
- 排除被拒绝的输出
- 优化成本效益

**实现**:
```python
class CostAnalyzer:
    """成本分析器"""

    def calculate_cost_per_accepted_output(
        self,
        agent_type: AgentType,
        time_window: str = "7d"
    ) -> float:
        """计算每个被接受输出的成本"""

        evaluations = self._query_evaluations(agent_type, time_window)

        # 1. 总成本
        total_cost = sum(e.api_cost_usd for e in evaluations)

        # 2. 被接受的输出数量
        accepted_count = sum(
            1 for e in evaluations
            if e.human_feedback and e.human_feedback.rating >= 4
        )

        # 3. 计算平均成本
        if accepted_count == 0:
            return float('inf')

        return total_cost / accepted_count

class AgentEvaluator:
    """Agent 评估器（增强版）"""

    def calculate_metrics(self, agent_type: AgentType, time_window: str) -> Dict:
        """计算指标（新增 cost_per_accepted_output）"""

        cost_per_accepted = self.cost_analyzer.calculate_cost_per_accepted_output(
            agent_type, time_window
        )

        return {
            # ... 原有指标 ...
            'cost_per_accepted_output': cost_per_accepted,
        }
```

### 17.2 Golden Set（黄金测试集）

✅ **建立 golden set（100-300 条）**
- 人工标注的高质量测试数据
- 覆盖各种边界情况
- 用于持续评估

**实现**:
```python
@dataclass
class GoldenExample:
    """黄金测试样例"""
    example_id: str
    agent_type: AgentType
    input_data: Dict
    expected_output: Dict
    difficulty: str  # 'easy', 'medium', 'hard'
    tags: List[str]
    created_at: datetime

class GoldenSetManager:
    """黄金测试集管理器"""

    def __init__(self):
        self.golden_set: List[GoldenExample] = []

    def add_example(self, example: GoldenExample):
        """添加测试样例"""
        self.golden_set.append(example)
        self._save_to_db(example)

    def evaluate_on_golden_set(
        self,
        agent_type: AgentType,
        agent_version: str
    ) -> Dict:
        """在黄金测试集上评估"""

        # 1. 获取该 Agent 类型的测试集
        examples = [e for e in self.golden_set if e.agent_type == agent_type]

        # 2. 运行 Agent
        results = []
        for example in examples:
            output = self._run_agent(agent_type, agent_version, example.input_data)
            is_correct = self._compare_output(output, example.expected_output)
            results.append(is_correct)

        # 3. 计算指标
        accuracy = sum(results) / len(results) if results else 0

        # 4. 按难度分组
        easy_examples = [e for e in examples if e.difficulty == 'easy']
        medium_examples = [e for e in examples if e.difficulty == 'medium']
        hard_examples = [e for e in examples if e.difficulty == 'hard']

        return {
            'overall_accuracy': accuracy,
            'easy_accuracy': self._calc_accuracy(easy_examples, results),
            'medium_accuracy': self._calc_accuracy(medium_examples, results),
            'hard_accuracy': self._calc_accuracy(hard_examples, results),
            'total_examples': len(examples),
        }
```

### 17.3 模型分层

✅ **模型分层策略**
- Rule2Spec/Resolution 用 Sonnet（高准确率）
- Daily Review 用 Haiku/Batch（低成本）
- 根据任务复杂度选择模型

**实现**:
```python
class ModelSelector:
    """模型选择器"""

    MODEL_TIERS = {
        'high_accuracy': 'claude-sonnet-4-6',
        'balanced': 'claude-sonnet-4-6',
        'low_cost': 'claude-haiku-4-5',
    }

    def select_model(self, agent_type: AgentType, task_complexity: str) -> str:
        """选择合适的模型"""

        if agent_type in [AgentType.RULE2SPEC, AgentType.RESOLUTION_SENTINEL]:
            # 高准确率任务
            return self.MODEL_TIERS['high_accuracy']

        elif agent_type == AgentType.DAILY_REVIEW:
            # 低成本任务
            return self.MODEL_TIERS['low_cost']

        else:
            # 平衡任务
            return self.MODEL_TIERS['balanced']

class AgentRunner:
    """Agent 运行器（增强版）"""

    def __init__(self, model_selector: ModelSelector):
        self.model_selector = model_selector

    async def run_agent(
        self,
        agent_type: AgentType,
        input_data: Dict,
        task_complexity: str = 'medium'
    ) -> Dict:
        """运行 Agent（使用模型分层）"""

        # 1. 选择模型
        model = self.model_selector.select_model(agent_type, task_complexity)

        # 2. 调用 API
        response = await self._call_api(model, input_data)

        return response
```

✅ **Prompt caching**
- 缓存常用的 system prompt
- 减少 token 使用量
- 降低 API 成本

**实现**:
```python
class PromptCacheManager:
    """Prompt 缓存管理器"""

    def __init__(self):
        self.cache = {}

    def get_cached_prompt(self, agent_type: AgentType) -> Optional[str]:
        """获取缓存的 prompt"""
        return self.cache.get(agent_type)

    def set_cached_prompt(self, agent_type: AgentType, prompt: str):
        """设置缓存的 prompt"""
        self.cache[agent_type] = prompt

class AgentRunner:
    """Agent 运行器（增强版）"""

    async def run_agent(
        self,
        agent_type: AgentType,
        input_data: Dict
    ) -> Dict:
        """运行 Agent（使用 prompt caching）"""

        # 1. 获取缓存的 system prompt
        system_prompt = self.prompt_cache.get_cached_prompt(agent_type)

        if system_prompt is None:
            system_prompt = self._load_system_prompt(agent_type)
            self.prompt_cache.set_cached_prompt(agent_type, system_prompt)

        # 2. 构造请求（使用 prompt caching）
        messages = [
            {
                "role": "system",
                "content": system_prompt,
                "cache_control": {"type": "ephemeral"}  # 启用缓存
            },
            {
                "role": "user",
                "content": json.dumps(input_data)
            }
        ]

        # 3. 调用 API
        response = await self._call_api(messages)

        return response
```

### 17.4 架构改进

- **更全面的指标**: schema_valid_rate, calibration, prompt_version_drift, cost_per_accepted_output
- **更可靠的评估**: Golden Set 持续评估
- **更优化的成本**: 模型分层 + Prompt caching
- **更智能的选择**: 根据任务复杂度选择模型

### 17.5 成本优化效果

**预期成本降低**:
- 模型分层: 30-50% 成本降低
- Prompt caching: 20-30% token 减少
- 总体: 40-60% 成本优化

---

**文档版本**: v2.0
**创建日期**: 2026-03-07
**最后更新**: 2026-03-08 (P2-11 增强)
**作者**: Jay Zhu

