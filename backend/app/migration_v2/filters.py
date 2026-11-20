"""事件过滤：只消费格式适配产出的 mirror-json-v2 转换记录。

两种过滤配置（升级前后各一）：
- after_image_only_v1：升级期沿用的现有规则，只看后镜像——after 可解析且
  after.status 命中即放行，不要求 before 完整；
- full_mirror_integrity_v2：新版规则，要求前后镜像均 PARSED 且 after.status 命中，
  镜像完整性与下游字段需求冲突时拦截，进入待重放范围。

每条决策都持久化判定所用字段（evidence_fields）与镜像版本/状态（mirror_versions）。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Callable

from app.migration_v2.models import (
    SCHEMA_VERSION_V2,
    ConversionRecord,
    FilterDecision,
    MirrorState,
)

POLICY_AFTER_ONLY = "after_image_only_v1"
POLICY_FULL_INTEGRITY = "full_mirror_integrity_v2"

# 业务放行条件：目标本轮订单字段只接受 PAID 状态的变更
REQUIRED_AFTER_STATUS = "PAID"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _after_only_rule(record: ConversionRecord) -> tuple[bool, list[str]]:
    """现有规则：只看后镜像。"""
    reasons: list[str] = []
    after = record.after
    if after.state != MirrorState.PARSED:
        reasons.append(f"AFTER_NOT_PARSED:{after.state}:{after.missing_reason}")
        return False, reasons
    status = (after.value or {}).get("status")
    if status != REQUIRED_AFTER_STATUS:
        reasons.append(f"AFTER_STATUS_MISMATCH:{status!r}!={REQUIRED_AFTER_STATUS}")
        return False, reasons
    # before 坏文本不影响放行，但必须在决策证据里显式记录其状态与缺失原因
    if record.before.state != MirrorState.PARSED:
        reasons.append(
            f"BEFORE_IGNORED_BY_V1_RULE:{record.before.state}:"
            f"{record.before.missing_reason}"
        )
    return True, reasons


def _full_integrity_rule(record: ConversionRecord) -> tuple[bool, list[str]]:
    """新版规则：前后镜像完整性 + 后镜像业务条件。"""
    reasons: list[str] = []
    ok = True
    if record.before.state != MirrorState.PARSED:
        ok = False
        reasons.append(
            f"BEFORE_INCOMPLETE:{record.before.state}:"
            f"{record.before.missing_reason}:FULL_MIRROR_INTEGRITY_REQUIRED"
        )
    if record.after.state != MirrorState.PARSED:
        ok = False
        reasons.append(f"AFTER_INCOMPLETE:{record.after.state}:{record.after.missing_reason}")
    if record.after.state == MirrorState.PARSED:
        status = (record.after.value or {}).get("status")
        if status != REQUIRED_AFTER_STATUS:
            ok = False
            reasons.append(f"AFTER_STATUS_MISMATCH:{status!r}!={REQUIRED_AFTER_STATUS}")
    return ok, reasons


# 策略注册表：版本号独立演进，决策记录里固化当时使用的策略版本
POLICIES: dict[str, dict[str, Any]] = {
    POLICY_AFTER_ONLY: {
        "name": POLICY_AFTER_ONLY,
        "version": "filter-1.0",
        "description": "升级期现有规则：仅依据 after 镜像与 after.status 判定，before 不参与拦截。",
        "rule": _after_only_rule,
    },
    POLICY_FULL_INTEGRITY: {
        "name": POLICY_FULL_INTEGRITY,
        "version": "filter-2.0",
        "description": "新版规则：before/after 必须均为 PARSED，且 after.status=PAID。",
        "rule": _full_integrity_rule,
    },
}


def evaluate(record: ConversionRecord, policy_name: str) -> FilterDecision:
    """对一条转换记录做出过滤决策（纯函数，不落盘）。"""
    if policy_name not in POLICIES:
        raise KeyError(f"未知过滤配置：{policy_name}，可选 {sorted(POLICIES)}")
    policy = POLICIES[policy_name]
    rule: Callable[[ConversionRecord], tuple[bool, list[str]]] = policy["rule"]
    passed, reasons = rule(record)

    after_status = (
        (record.after.value or {}).get("status")
        if record.after.state == MirrorState.PARSED
        else None
    )
    evidence_fields = {
        # 判定实际读取的字段：路径 + 观测值 + 槽位状态，避免只留结论不留证据
        "after.state": record.after.state,
        "after.$.status": after_status,
        "after.missing_reason": record.after.missing_reason,
        "before.state": record.before.state,
        "before.missing_reason": record.before.missing_reason,
        "operation": record.operation,
        "required_after_status": REQUIRED_AFTER_STATUS,
    }
    mirror_versions = {
        "schema_version": record.schema_version,
        "before.slot_state": record.before.state,
        "before.observed_type": record.before.observed_type,
        "after.slot_state": record.after.state,
        "after.observed_type": record.after.observed_type,
        "policy_version": policy["version"],
    }

    decision_id = "fd-" + hashlib.sha256(
        f"{record.record_id}|{policy_name}|{policy['version']}".encode("utf-8")
    ).hexdigest()[:16]

    return FilterDecision(
        decision_id=decision_id,
        record_id=record.record_id,
        coordinate_key=record.coordinate_key,
        policy_name=policy_name,
        policy_version=policy["version"],
        result="DELIVERED" if passed else "HELD_FOR_REPLAY",
        evidence_fields=evidence_fields,
        mirror_versions=mirror_versions,
        reasons=reasons,
        decided_at=_now_iso(),
    )
