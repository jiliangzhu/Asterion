from __future__ import annotations

from typing import Iterable


_VALUE_MAPS: dict[str, dict[str, str]] = {
    "priority_band": {
        "critical": "最高优先级",
        "high": "高优先级",
        "medium": "中优先级",
        "low": "低优先级",
    },
    "triage_status": {
        "review": "待人工复核",
        "defer": "暂缓处理",
        "block": "阻断",
        "blocked": "阻断",
        "accepted": "已接受",
        "ignored": "已忽略",
        "deferred": "已延后",
        "agent_timeout": "分诊超时",
        "agent_parse_error": "分诊解析失败",
        "agent_failed": "分诊失败",
        "no_triage": "暂无分诊",
    },
    "recommended_operator_action": {
        "manual_review": "人工复核",
        "defer": "暂缓处理",
        "ignore": "忽略",
        "take_review": "进入人工复核",
        "hold_redeem": "暂缓赎回",
        "consider_dispute": "考虑争议",
        "observe": "继续观察",
    },
    "confidence_band": {
        "high": "高置信度",
        "medium": "中置信度",
        "low": "低置信度",
    },
    "advisory_gate_status": {
        "enabled": "已启用",
        "experimental": "实验态",
    },
    "agent_status": {
        "success": "成功",
        "ok": "正常",
        "failed": "失败",
        "failure": "失败",
        "timeout": "超时",
        "parse_error": "解析失败",
        "not_run": "未运行",
        "idle": "空闲",
        "idle_no_subjects": "当前无对象",
        "awaiting_uma_proposal": "等待链上提案",
        "awaiting_observation": "等待权威观测值",
    },
    "evaluation_method": {
        "replay_backtest": "回放评估",
        "operator_outcome_proxy": "人工结果代理",
    },
    "operator_review_status": {
        "accepted": "已接受",
        "ignored": "已忽略",
        "deferred": "已延后",
        "pending_operator_review": "待人工处理",
    },
    "agent_running_status": {
        "ok": "运行正常",
        "failed": "运行失败",
        "idle": "空闲",
        "idle_no_subjects": "当前无对象",
    },
    "agent_value_status": {
        "useful": "已有真实价值",
        "fallback_only": "仅保守降级",
        "not_running": "尚无有效产出",
    },
    "settlement_feedback_status": {
        "open": "尚未开始",
        "waiting_for_resolution": "等待真实结算",
        "closed": "已完成回写",
    },
}

_FIELD_GROUPS: dict[str, str] = {
    "priority_band": "priority_band",
    "triage_priority_band": "priority_band",
    "triage_status": "triage_status",
    "effective_triage_status": "triage_status",
    "latest_triage_status": "triage_status",
    "recommended_operator_action": "recommended_operator_action",
    "triage_recommended_operator_action": "recommended_operator_action",
    "latest_operator_action": "recommended_operator_action",
    "confidence_band": "confidence_band",
    "advisory_gate_status": "advisory_gate_status",
    "triage_advisory_gate_status": "advisory_gate_status",
    "latest_agent_status": "agent_status",
    "triage_latest_run_status": "agent_status",
    "triage_runtime_status": "agent_status",
    "resolution_runtime_status": "agent_status",
    "resolution_latest_run_status": "agent_status",
    "resolution_reconciliation_status": "agent_status",
    "latest_evaluation_method": "evaluation_method",
    "triage_latest_evaluation_method": "evaluation_method",
    "latest_operator_review_status": "operator_review_status",
    "triage_latest_operator_review_status": "operator_review_status",
    "profitability_settlement_feedback_closure_status": "settlement_feedback_status",
    "profitability_agent_running_status": "agent_running_status",
    "profitability_agent_value_status": "agent_value_status",
}

_REASON_CODE_MAP = {
    "provider_unauthorized": "外部分诊服务鉴权失败",
    "provider_forbidden": "外部分诊服务拒绝访问",
    "provider_rate_limited": "外部分诊服务触发限流",
    "provider_unavailable": "外部分诊服务当前不可用",
    "surface_degraded": "输入读面处于降级状态",
    "delivery_degraded": "输入交付状态已降级",
    "book_unstable": "盘口稳定性不足",
    "operator_review_required": "需要人工复核",
    "execution_intelligence_weak": "执行情报偏弱",
    "book_stability_low": "盘口稳定度偏低",
    "size_shock": "下单尺寸冲击偏高",
    "unstable_book": "盘口波动较大",
    "triage:none": "暂无分诊原因",
    "execution_risk:none": "暂无执行风险",
    "triage_gate:enabled": "分诊建议已启用",
}


def localize_triage_value(field: str, value: object) -> object:
    if value is None:
        return value
    if isinstance(value, str) and value == "":
        return value
    if field in {"latest_evaluation_verified"}:
        return "已验证" if bool(value) else "未验证"
    if field in {"profitability_agents_have_useful_output"}:
        return "是" if bool(value) else "否"
    group = _FIELD_GROUPS.get(field)
    if group is None:
        return value
    return _VALUE_MAPS.get(group, {}).get(str(value), value)


def localize_reason_codes(values: Iterable[object], *, empty_label: str) -> list[str]:
    items = [str(item) for item in values if str(item).strip()]
    if not items:
        items = [empty_label]
    localized: list[str] = []
    for item in items:
        if item.startswith("calibration:"):
            status = item.split(":", 1)[1]
            localized.append(
                {
                    "clear": "校准状态: clear",
                    "research_only": "校准状态: research_only",
                    "review_required": "校准状态: review_required",
                }.get(status, f"校准状态: {status}")
            )
            continue
        localized.append(_REASON_CODE_MAP.get(item, item))
    return localized


def localize_triage_frame(frame):
    localized = frame.copy()
    for column in localized.columns:
        localized[column] = localized[column].apply(lambda value, field=column: localize_triage_value(field, value))
    return localized
