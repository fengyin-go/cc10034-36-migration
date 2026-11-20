"""格式适配：Debezium 1.9.7.Final 旧信封 → mirror-json-v2 转换记录。

旧信封关键特征（1.9.7.Final 时代的 MySQL connector 输出）：
- payload.before：可能是 JSON 字符串（历史落库形态），也可能已经是对象；
- payload.after：对象；
- payload.source：库表/GTID/binlog 位点等源坐标。

与旧实现 format_adapter._from_debezium 的关键差异：JSON 解析失败时
本模块保留原文、标记 UNPARSEABLE 并登记缺失原因，绝不写成 {} 或 null 冒充空镜像。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.migration_v2.models import (
    SCHEMA_VERSION_V2,
    SUPPORTED_SOURCE_MARKERS,
    ConversionRecord,
    EvidenceCompleteness,
    MirrorSlot,
    MirrorState,
    build_coordinate_key,
    build_record_id,
)


class LegacyEnvelopeError(ValueError):
    """旧信封不符合 1.9.7.Final 适配前提（缺标记、结构损坏等）。"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_marker(envelope: dict[str, Any]) -> str:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise LegacyEnvelopeError("旧信封缺少 payload 对象，无法适配")
    source = payload.get("source")
    if not isinstance(source, dict):
        raise LegacyEnvelopeError("旧信封缺少 payload.source 源坐标块")
    marker = source.get("connector_version") or envelope.get("schema_marker")
    if marker not in SUPPORTED_SOURCE_MARKERS:
        raise LegacyEnvelopeError(
            f"仅适配标记 {sorted(SUPPORTED_SOURCE_MARKERS)} 的旧信封，实际标记={marker!r}"
        )
    return str(marker)


def _slot_from_mirror(raw: Any, *, field_name: str) -> tuple[MirrorSlot, str | None]:
    """把 payload.before/after 的原始字段归一成 MirrorSlot。

    返回 (槽位, 解析错误描述)。错误描述进入转换记录 parse_errors，
    槽位自身通过 state + missing_reason 表达缺失语义。
    """
    if raw is None:
        reason = (
            "BEFORE_FIELD_ABSENT" if field_name == "before" else "AFTER_FIELD_ABSENT"
        )
        return (
            MirrorSlot(
                state=MirrorState.ABSENT,
                observed_type="null" if raw is None else "missing",
                value=None,
                missing_reason=reason,
            ),
            None,
        )

    if isinstance(raw, str):
        text = raw
        if not text.strip():
            reason = (
                "BEFORE_FIELD_ABSENT" if field_name == "before" else "AFTER_FIELD_ABSENT"
            )
            return (
                MirrorSlot(
                    state=MirrorState.ABSENT,
                    observed_type="json-string",
                    raw_text=text,
                    missing_reason=reason,
                ),
                None,
            )
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            # 红线：坏文本原样留痕，绝不替换成 {} 或 null 镜像
            reason = (
                "BEFORE_JSON_UNPARSEABLE"
                if field_name == "before"
                else "MIRROR_NOT_OBJECT"
            )
            slot = MirrorSlot(
                state=MirrorState.UNPARSEABLE,
                observed_type="json-string",
                value=None,
                raw_text=text,
                missing_reason=reason,
            )
            return slot, f"payload.{field_name} JSON 解析失败：{exc.msg}（原文已留痕）"
        if not isinstance(parsed, dict):
            slot = MirrorSlot(
                state=MirrorState.UNPARSEABLE,
                observed_type="non-object",
                value=None,
                raw_text=text,
                missing_reason="MIRROR_NOT_OBJECT",
            )
            return slot, f"payload.{field_name} 解析结果不是 JSON 对象"
        return MirrorSlot(
            state=MirrorState.PARSED,
            observed_type="json-string",
            value=parsed,
            raw_text=text,
        ), None

    if isinstance(raw, dict):
        return MirrorSlot(
            state=MirrorState.PARSED,
            observed_type="object",
            value=dict(raw),
        ), None

    return (
        MirrorSlot(
            state=MirrorState.UNPARSEABLE,
            observed_type=type(raw).__name__,
            value=None,
            missing_reason="MIRROR_NOT_OBJECT",
        ),
        f"payload.{field_name} 类型 {type(raw).__name__} 无法作为行镜像",
    )


def _source_coordinate(payload: dict[str, Any], operation: str) -> dict[str, Any]:
    src = payload["source"]
    coordinate = {
        "database": src.get("db"),
        "table": src.get("table"),
        "operation": operation,
        "binlog_file": src.get("file"),
        "binlog_position": src.get("pos"),
        "gtid": src.get("gtid"),
        "row_key": None,
    }
    # 行级坐标优先取 after 主键，after 不可用时退回 before 原文无法解析的情形，
    # 由调用方在拿到槽位后补齐（见 convert_envelope）。
    after = payload.get("after")
    if isinstance(after, dict) and "id" in after:
        coordinate["row_key"] = {"id": after["id"]}
    return coordinate


# Debezium envelope 的 op 单字母码 → 统一操作名
OP_CODE_MAP = {
    "c": "INSERT",   # create
    "u": "UPDATE",   # update
    "d": "DELETE",   # delete
    "r": "INSERT",   # snapshot read，按初始插入处理
}


def convert_envelope(envelope: dict[str, Any], *, origin: str) -> ConversionRecord:
    """把单个 1.9.7.Final 旧信封转换为 mirror-json-v2 转换记录。

    纯函数：不写盘、不发事件，只产出明确带版本与缺失原因的转换记录。
    """
    marker = _extract_marker(envelope)
    payload: dict[str, Any] = envelope["payload"]
    raw_op = str(payload.get("op", "")).lower()
    operation = OP_CODE_MAP.get(raw_op, raw_op.upper())
    if operation not in {"INSERT", "UPDATE", "DELETE"}:
        raise LegacyEnvelopeError(f"不支持的操作类型 op={payload.get('op')!r}")

    before_slot, before_err = _slot_from_mirror(
        payload.get("before"), field_name="before"
    )
    after_slot, after_err = _slot_from_mirror(
        payload.get("after"), field_name="after"
    )

    coordinate = _source_coordinate(payload, operation)
    if coordinate["row_key"] is None:
        # after 不可用时，若 before 可解析则用 before 主键；仍不可解析则记录为未知，
        # 位点坐标（file/pos/gtid）仍然唯一定位该事件，不会丢失可重放性。
        if before_slot.state == MirrorState.PARSED and "id" in (before_slot.value or {}):
            coordinate["row_key"] = {"id": before_slot.value["id"]}
        else:
            coordinate["row_key"] = None

    coordinate_key = build_coordinate_key(coordinate)
    parse_errors = [err for err in (before_err, after_err) if err]

    completeness = (
        EvidenceCompleteness.COMPLETE
        if before_slot.state == MirrorState.PARSED
        and after_slot.state == MirrorState.PARSED
        else EvidenceCompleteness.PARTIAL
    )

    fingerprint = "|".join(
        [
            marker,
            operation,
            before_slot.state,
            after_slot.state,
            before_slot.raw_text or "",
            json.dumps(after_slot.value, sort_keys=True, ensure_ascii=False)
            if after_slot.value is not None
            else "",
        ]
    )
    record = ConversionRecord(
        record_id=build_record_id(coordinate_key, fingerprint),
        schema_version=SCHEMA_VERSION_V2,
        source_marker=marker,
        origin=origin,
        operation=operation,
        source_coordinate=coordinate,
        coordinate_key=coordinate_key,
        before=before_slot,
        after=after_slot,
        evidence_completeness=completeness,
        parse_errors=parse_errors,
        converted_at=_utc_now_iso(),
    )
    return record
