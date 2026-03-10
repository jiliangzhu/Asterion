# UMA Watcher 详细设计

**模块**: `domains/weather/resolution/uma_watcher.py`  
**版本**: v2.1  
**更新日期**: 2026-03-08  
**状态**: Interface Freeze Candidate

---

## 1. 模块概述

### 1.1 冻结目标

本设计冻结 UMA Watcher 的三层边界：

1. 链上权威状态
2. 本地调度建议
3. replay / backfill / persistence

### 1.2 核心原则

- proposal 状态唯一来源是链上
- wall clock 只能用于调度建议
- wall clock 不能定义 proposal 的权威状态
- finalized block watermark 是 replay / persistence 的核心锚点

---

## 2. 权威状态来源

proposal 状态唯一来源：

- `on-chain events`
- `on-chain reads`
- `finalized block watermark`
- `replay / backfill`

说明：

- `on-chain events` 提供状态转移事实
- `on-chain reads` 用于补齐当前状态、参数与确认信息
- `finalized block watermark` 用于限制处理范围，避免 reorg 污染
- `replay / backfill` 用于恢复 watcher 本地状态，不产生新的权威定义

禁止项：

- 靠本地时间判断 proposal 已确认
- 用 wall clock 覆盖链上 `settled`
- `settled` 后继续靠 challenge / liveness 推导最终状态

---

## 3. Proposal State Machine

### 3.1 状态定义

```python
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

class ProposalStatus(Enum):
    PENDING = "pending"
    PROPOSED = "proposed"
    DISPUTED = "disputed"
    SETTLED = "settled"
    REDEEMED = "redeemed"

@dataclass
class UMAProposal:
    proposal_id: str
    market_id: str
    condition_id: str
    proposer: str
    proposed_outcome: str
    proposal_bond: float
    dispute_bond: Optional[float]
    proposal_tx_hash: str
    proposal_block_number: int
    proposal_timestamp: datetime
    status: ProposalStatus
    on_chain_settled_at: Optional[datetime]
    safe_redeem_after: Optional[datetime]
    human_review_required: bool
```

### 3.2 状态解释

- `PENDING`: 本地已知市场，但尚无链上 proposal 事件
- `PROPOSED`: 已收到 proposal 事件，链上尚未 dispute / settled
- `DISPUTED`: 已收到 dispute 事件
- `SETTLED`: 已由链上事件/读取确认 settled
- `REDEEMED`: 已由链上事件确认 redeem

权威性说明：

- `challenge_period_seconds`、`liveness_seconds` 可作为上下文参数保留
- 但它们不是 `SETTLED` 的定义条件

---

## 4. 状态转移记录

### 4.1 Transition Contract

```python
@dataclass
class StateTransition:
    proposal_id: str
    old_status: ProposalStatus
    new_status: ProposalStatus
    block_number: int
    tx_hash: str
    event_type: str
    recorded_at: datetime
```

### 4.2 正确写法

必须：

1. 先保存 `old_status`
2. 再写入 `new_status`
3. transition log 记录 `old_status -> new_status`

```python
async def apply_transition(self, proposal: UMAProposal, new_status: ProposalStatus, event):
    old_status = proposal.status
    proposal.status = new_status

    transition = StateTransition(
        proposal_id=proposal.proposal_id,
        old_status=old_status,
        new_status=new_status,
        block_number=event["block_number"],
        tx_hash=event["tx_hash"],
        event_type=event["event_type"],
        recorded_at=event["recorded_at"],
    )

    await self._save_proposal(proposal)
    await self._save_transition(transition)
```

禁止项：

- 先更新状态再把 `from_status` 读出来
- 用当前内存状态回填历史 `from_status`

---

## 5. Watcher Processing Model

### 5.1 Processing Pipeline

```text
load last_finalized_block
-> replay persisted state
-> backfill finalized block range
-> poll new finalized blocks
-> apply idempotent events
-> refresh proposal projections
-> emit scheduling suggestions
```

### 5.2 主循环

```python
class UMAMonitor:
    async def poll(self) -> None:
        finalized_block = await self.rpc.get_finalized_block()
        from_block = self.watermark.last_finalized_block + 1
        to_block = finalized_block

        if from_block > to_block:
            return

        events = await self._load_events(from_block, to_block)
        for event in events:
            if await self._is_duplicate_event(event):
                continue
            await self._apply_event(event)

        self.watermark.last_finalized_block = to_block
        await self._save_watermark()
```

说明：

- watcher 只处理 finalized block 范围
- 本地 wall clock 只决定“什么时候再轮询”
- 不决定 proposal 最终状态

---

## 6. On-Chain Reads 与 Projection

Watcher 需要两类链上数据：

1. 事件流：proposal / dispute / settled / redeemed
2. 当前读取：proposal 是否 settled、结算时间、是否可 redeem

```python
@dataclass
class OnChainProposalRead:
    proposal_id: str
    status: ProposalStatus
    on_chain_settled_at: Optional[datetime]
    redeemable: bool
    challenge_period_seconds: Optional[int]
    liveness_seconds: Optional[int]
```

说明：

- `challenge_period_seconds` / `liveness_seconds` 用于解释上下文和生成建议
- `status` / `on_chain_settled_at` 才是权威状态投影输入

---

## 7. RedeemScheduler

### 7.1 输入输出模型

输入：

- `proposal_status`
- `on_chain_settled_at`
- `safe_redeem_after`
- `human_review_required`

输出：

- `WAIT`
- `READY_FOR_REDEEM`
- `BLOCKED_PENDING_REVIEW`
- `NOT_REDEEMABLE`

### 7.2 数据结构

```python
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

class RedeemDecision(Enum):
    WAIT = "wait"
    READY_FOR_REDEEM = "ready_for_redeem"
    BLOCKED_PENDING_REVIEW = "blocked_pending_review"
    NOT_REDEEMABLE = "not_redeemable"

@dataclass
class RedeemScheduleInput:
    proposal_status: ProposalStatus
    on_chain_settled_at: Optional[datetime]
    safe_redeem_after: Optional[datetime]
    human_review_required: bool

@dataclass
class RedeemScheduleOutput:
    decision: RedeemDecision
    reason: str
```

### 7.3 核心逻辑

```python
class RedeemScheduler:
    def decide(self, schedule_input: RedeemScheduleInput, now: datetime) -> RedeemScheduleOutput:
        if schedule_input.proposal_status not in {
            ProposalStatus.SETTLED,
            ProposalStatus.REDEEMED,
        }:
            return RedeemScheduleOutput(
                decision=RedeemDecision.NOT_REDEEMABLE,
                reason="proposal not settled on chain",
            )

        if schedule_input.proposal_status == ProposalStatus.REDEEMED:
            return RedeemScheduleOutput(
                decision=RedeemDecision.NOT_REDEEMABLE,
                reason="already redeemed",
            )

        if schedule_input.human_review_required:
            return RedeemScheduleOutput(
                decision=RedeemDecision.BLOCKED_PENDING_REVIEW,
                reason="human review required",
            )

        if schedule_input.safe_redeem_after and now < schedule_input.safe_redeem_after:
            return RedeemScheduleOutput(
                decision=RedeemDecision.WAIT,
                reason="waiting until safe redeem time",
            )

        return RedeemScheduleOutput(
            decision=RedeemDecision.READY_FOR_REDEEM,
            reason="settled on chain and safe redeem window reached",
        )
```

说明：

- `safe_redeem_after` 是本地调度建议字段
- `safe_redeem_after` 不是链上最终状态字段
- `SETTLED` 后不再用 challenge / liveness 推导权威状态

---

## 8. Settlement Verifier Linkage

Watcher 只负责触发和链接 verifier，不把 verifier 结果当成链上状态替代品。

```python
@dataclass
class EvidencePackageLink:
    proposal_id: str
    verification_id: str
    evidence_package_id: str
    linked_at: datetime
```

用途：

- 把链上 proposal 与 Settlement Verifier 证据包关联
- 支持 replay、审计、operator review

---

## 9. Watcher Persistence and Replay

### 9.1 必须持久化的对象

- `last_finalized_block`
- proposal projection
- state transitions
- processed event ids
- evidence package linkage

### 9.2 关键机制

- `last_finalized_block`: restart 时从该水位恢复
- `event idempotency`: 使用 `(tx_hash, log_index)` 或等价 event id 去重
- `restart replay`: 重启后先 replay 本地 projection，再补 finalized block 缺口
- `multi-RPC fallback`: 主 RPC 失败时切到备用 RPC，但不改变 finalized-watermark 语义
- `evidence package linkage`: verifier 证据包必须可追到 proposal

### 9.3 数据结构

```python
@dataclass
class BlockWatermark:
    chain_id: int
    last_processed_block: int
    last_finalized_block: int
    updated_at: datetime

@dataclass
class ProcessedEvent:
    event_id: str
    tx_hash: str
    log_index: int
    block_number: int
    processed_at: datetime
```

---

## 10. 数据库设计

### 10.1 UMA Proposals

```sql
CREATE TABLE uma_proposals (
    proposal_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    proposer TEXT NOT NULL,
    proposed_outcome TEXT NOT NULL,
    proposal_bond REAL NOT NULL,
    dispute_bond REAL,
    proposal_tx_hash TEXT NOT NULL,
    proposal_block_number INTEGER NOT NULL,
    proposal_timestamp TIMESTAMP NOT NULL,
    status TEXT NOT NULL,
    on_chain_settled_at TIMESTAMP,
    safe_redeem_after TIMESTAMP,
    human_review_required BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL
);
```

### 10.2 Proposal State Transitions

```sql
CREATE TABLE proposal_state_transitions (
    transition_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    old_status TEXT NOT NULL,
    new_status TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    FOREIGN KEY (proposal_id) REFERENCES uma_proposals(proposal_id)
);
```

### 10.3 Processed Events

```sql
CREATE TABLE processed_uma_events (
    event_id TEXT PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    processed_at TIMESTAMP NOT NULL
);
```

### 10.4 Block Watermarks

```sql
CREATE TABLE block_watermarks (
    chain_id INTEGER PRIMARY KEY,
    last_processed_block INTEGER NOT NULL,
    last_finalized_block INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

### 10.5 Evidence Package Linkage

```sql
CREATE TABLE proposal_evidence_links (
    proposal_id TEXT NOT NULL,
    verification_id TEXT NOT NULL,
    evidence_package_id TEXT NOT NULL,
    linked_at TIMESTAMP NOT NULL,
    PRIMARY KEY (proposal_id, verification_id)
);
```

---

## 11. MVP 结论

已冻结的内容：

- proposal 权威状态来源
- transition log 写法
- finalized-block 驱动的 replay / backfill
- RedeemScheduler 的最小输入输出模型

Phase 1 可以开始：

- watch-only UMA monitor
- restart replay
- cold-path backfill
- evidence linkage persistence

仍需 human-in-the-loop：

- dispute 最终决策
- `human_review_required=True` 时的 redeem 放行
- 异常链上状态与证据包冲突时的最终裁定
