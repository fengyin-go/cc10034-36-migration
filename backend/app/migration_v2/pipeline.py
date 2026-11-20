"""迁移管道编排：摄入旧信封 → 转换记录 → 过滤决策 → 交付确认/待重放。

关键不变量：
1. 正常事件的前后镜像与 schema_version 随转换记录一并持久化；
2. PARTIAL 证据必须带 MISSING_REASONS 中定义的缺失原因（由 MirrorSlot 构造保证）；
3. 过滤放行的坐标若已在 deliveries 中确认，重复转换只追加审计、不再交付；
4. 被拦截的坐标进入待重放范围，新版策略部署后由 replay_pending 重新判定。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.migration_v2.adapter import convert_envelope
from app.migration_v2.filters import POLICIES, evaluate
from app.migration_v2.ledger import MigrationLedger
from app.migration_v2.models import ConversionRecord


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ingest_envelope(
    *,
    ledger: MigrationLedger,
    envelope: dict[str, Any],
    origin: str,
    policy_name: str,
) -> dict[str, Any]:
    """处理单个旧信封，返回本次摄入的结构化摘要。"""
    if policy_name not in POLICIES:
        raise KeyError(f"未知过滤配置：{policy_name}")

    record = convert_envelope(envelope, origin=origin)
    repeated_conversion = ledger.has_conversion(record.record_id)
    # 转换记录每次都留痕（正常事件前后镜像和版本一并持久化）
    ledger.append_conversion(record)

    decision = evaluate(record, policy_name)
    already_delivered = ledger.is_delivered(record.coordinate_key)
    ledger.append_decision(decision)

    delivery_status = "NOT_ELIGIBLE"
    if decision.result == "DELIVERED":
        if already_delivered:
            # 重复转换：源坐标已确认交付，抑制再次投递
            delivery_status = "SUPPRESSED_DUPLICATE"
        else:
            ledger.append_delivery({
                "coordinate_key": record.coordinate_key,
                "record_id": record.record_id,
                "source_coordinate": record.source_coordinate,
                "policy_name": decision.policy_name,
                "policy_version": decision.policy_version,
                "schema_version": record.schema_version,
                "delivered_at": _now_iso(),
            })
            delivery_status = "DELIVERED"
    else:
        delivery_status = "HELD_FOR_REPLAY"

    return {
        "record_id": record.record_id,
        "coordinate_key": record.coordinate_key,
        "evidence_completeness": record.evidence_completeness,
        "decision_result": decision.result,
        "delivery_status": delivery_status,
        "repeated_conversion": repeated_conversion,
        "reasons": list(decision.reasons),
        "record": record.to_dict(),
        "decision": decision.to_dict(),
    }


def ingest_batch(
    *,
    ledger: MigrationLedger,
    envelopes: list[dict[str, Any]],
    origin: str,
    policy_name: str,
) -> dict[str, Any]:
    """批量摄入并汇总实际投递与待重放范围。"""
    results = [
        ingest_envelope(
            ledger=ledger, envelope=envelope, origin=origin, policy_name=policy_name
        )
        for envelope in envelopes
    ]
    return {
        "policy_name": policy_name,
        "policy_version": POLICIES[policy_name]["version"],
        "total": len(results),
        "delivered": [r["coordinate_key"] for r in results if r["delivery_status"] == "DELIVERED"],
        "suppressed_duplicates": [
            r["coordinate_key"] for r in results if r["delivery_status"] == "SUPPRESSED_DUPLICATE"
        ],
        "held_for_replay": [
            r["coordinate_key"] for r in results if r["delivery_status"] == "HELD_FOR_REPLAY"
        ],
        "results": results,
    }


def _latest_decisions(ledger: MigrationLedger) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    for decision in ledger.load_decisions():
        latest[decision.record_id] = decision
    return latest


def replay_scope(ledger: MigrationLedger) -> list[ConversionRecord]:
    """待重放范围：最新决策为 HELD 且坐标未确认交付的转换记录。

    已被任一策略确认交付的坐标不进入操作待办，重复转换的交付抑制由
    deliveries 坐标表单独保证。
    """
    latest = _latest_decisions(ledger)
    delivered = ledger.delivered_coordinates()
    held: list[ConversionRecord] = []
    for record in ledger.load_conversions():
        decision = latest.get(record.record_id)
        if decision is None:
            continue
        if decision.result == "HELD_FOR_REPLAY" and record.coordinate_key not in delivered:
            held.append(record)
    # 同一记录可能被重复转换多次，按 record_id 去重，保留最新一份
    unique: dict[str, ConversionRecord] = {}
    for record in held:
        unique[record.record_id] = record
    return list(unique.values())


def _stored_records(ledger: MigrationLedger) -> list[ConversionRecord]:
    """全部存量转换记录，按 record_id 去重保留最新一份。"""
    unique: dict[str, ConversionRecord] = {}
    for record in ledger.load_conversions():
        unique[record.record_id] = record
    return list(unique.values())


def replay_pending(
    *,
    ledger: MigrationLedger,
    policy_name: str,
    scope: str = "held",
) -> dict[str, Any]:
    """新版部署后回放存量：用当前策略重新判定并尝试交付。

    scope="held"：只回放当前待重放范围（最新决策 HELD 且未交付）；
    scope="all"：升级场景，用新策略重判全部存量转换记录——
      - 新策略放行且未交付      → DELIVERED_ON_REPLAY
      - 新策略放行但坐标已确认  → SUPPRESSED_DUPLICATE（不重复交付）
      - 新策略拦截且从未交付    → STILL_HELD（留在待重放范围）
      - 新策略拦截但旧策略交付过 → NEWLY_HELD_UNDER_POLICY（进入完整性整改清单，
        不重复交付，需从源端补齐镜像后重新摄入）
    """
    if policy_name not in POLICIES:
        raise KeyError(f"未知过滤配置：{policy_name}")
    if scope not in {"held", "all"}:
        raise ValueError("scope 仅支持 'held' 或 'all'")
    pending = replay_scope(ledger) if scope == "held" else _stored_records(ledger)
    delivered_set = ledger.delivered_coordinates()
    replayed: list[dict[str, Any]] = []
    for record in pending:
        decision = evaluate(record, policy_name)
        ledger.append_decision(decision)
        was_delivered = record.coordinate_key in delivered_set
        if decision.result == "DELIVERED":
            if was_delivered:
                outcome = "SUPPRESSED_DUPLICATE"
            else:
                ledger.append_delivery({
                    "coordinate_key": record.coordinate_key,
                    "record_id": record.record_id,
                    "source_coordinate": record.source_coordinate,
                    "policy_name": decision.policy_name,
                    "policy_version": decision.policy_version,
                    "schema_version": record.schema_version,
                    "delivered_at": _now_iso(),
                    "replayed": True,
                })
                delivered_set.add(record.coordinate_key)
                outcome = "DELIVERED_ON_REPLAY"
        else:
            outcome = "NEWLY_HELD_UNDER_POLICY" if was_delivered else "STILL_HELD"
        replayed.append({
            "record_id": record.record_id,
            "coordinate_key": record.coordinate_key,
            "outcome": outcome,
            "previously_delivered": was_delivered,
            "reasons": list(decision.reasons),
        })
    return {
        "policy_name": policy_name,
        "policy_version": POLICIES[policy_name]["version"],
        "scope": scope,
        "replayed_count": len(replayed),
        "delivered_on_replay": [r["coordinate_key"] for r in replayed if r["outcome"] == "DELIVERED_ON_REPLAY"],
        "suppressed_duplicates": [r["coordinate_key"] for r in replayed if r["outcome"] == "SUPPRESSED_DUPLICATE"],
        "still_held": [r["coordinate_key"] for r in replayed if r["outcome"] == "STILL_HELD"],
        "newly_held_under_policy": [r["coordinate_key"] for r in replayed if r["outcome"] == "NEWLY_HELD_UNDER_POLICY"],
        "details": replayed,
    }
