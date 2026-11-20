"""mirror-json-v2 领域模型：转换记录、镜像槽位、缺失原因目录。"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any

# 统一 JSON 镜像结构的明确第二版标识
SCHEMA_VERSION_V2 = "mirror-json-v2"
# 本适配器接受的旧信封标记（Debezium connector 版本）
SUPPORTED_SOURCE_MARKERS = frozenset({"1.9.7.Final"})


class MirrorState:
    """镜像槽位状态。UNPARSEABLE/ABSENT 时 value 必须为 None。"""

    PARSED = "PARSED"                # 成功解析为 JSON 对象
    UNPARSEABLE = "UNPARSEABLE"      # 原文存在但无法解析，禁止降级为 {} / null
    ABSENT = "ABSENT"                # 字段缺失或显式为 null


# 明确定义的缺失原因目录。任何 PARTIAL 证据都必须引用这里的 code。
MISSING_REASONS: dict[str, dict[str, str]] = {
    "BEFORE_JSON_UNPARSEABLE": {
        "applies_to": "before",
        "meaning": "payload.before 为字符串但不是合法 JSON，原文保留在 raw_text，"
                   "镜像值置空（null）且状态为 UNPARSEABLE，不得用空对象替代。",
    },
    "BEFORE_FIELD_ABSENT": {
        "applies_to": "before",
        "meaning": "payload.before 字段缺失或显式为 null，镜像槽位状态为 ABSENT。",
    },
    "AFTER_FIELD_ABSENT": {
        "applies_to": "after",
        "meaning": "payload.after 字段缺失或显式为 null，镜像槽位状态为 ABSENT。",
    },
    "MIRROR_NOT_OBJECT": {
        "applies_to": "before|after",
        "meaning": "镜像字段可解析但顶层不是 JSON 对象，无法作为行镜像使用。",
    },
    "FULL_MIRROR_INTEGRITY_REQUIRED": {
        "applies_to": "decision",
        "meaning": "full_mirror_integrity_v2 策略要求前后镜像均为 PARSED，"
                   "任一槽位非 PARSED 即拦截并进入待重放范围。",
    },
}


class EvidenceCompleteness:
    COMPLETE = "COMPLETE"  # 前后镜像均 PARSED
    PARTIAL = "PARTIAL"    # 至少一个槽位 UNPARSEABLE/ABSENT，带缺失原因


@dataclass
class MirrorSlot:
    """单个镜像（before/after）的证据槽位。"""

    state: str
    observed_type: str                       # json-string / object / null / missing / non-object
    value: dict[str, Any] | None = None      # 仅 state == PARSED 时非 None
    raw_text: str | None = None              # 字符串型镜像的原文，留痕审计
    missing_reason: str | None = None        # 指向 MISSING_REASONS 的 code

    def __post_init__(self) -> None:
        if self.state != MirrorState.PARSED and self.value is not None:
            raise ValueError(
                f"镜像槽位不变量被破坏：state={self.state} 不允许携带 value，"
                "坏文本不得伪装成可消费镜像"
            )
        if self.state != MirrorState.PARSED and self.missing_reason is None:
            raise ValueError("非 PARSED 槽位必须给出明确定义的 missing_reason")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MirrorSlot":
        return cls(**data)


@dataclass
class ConversionRecord:
    """镜像版本转换记录：旧信封（1.9.7.Final）→ mirror-json-v2。"""

    record_id: str
    schema_version: str
    source_marker: str
    origin: str                              # 来源文件/导入批次标识
    operation: str
    source_coordinate: dict[str, Any]        # 源坐标，去重与交付确认的唯一依据
    coordinate_key: str
    before: MirrorSlot
    after: MirrorSlot
    evidence_completeness: str
    parse_errors: list[str] = field(default_factory=list)
    converted_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "schema_version": self.schema_version,
            "source_marker": self.source_marker,
            "origin": self.origin,
            "operation": self.operation,
            "source_coordinate": dict(self.source_coordinate),
            "coordinate_key": self.coordinate_key,
            "before": self.before.to_dict(),
            "after": self.after.to_dict(),
            "evidence_completeness": self.evidence_completeness,
            "parse_errors": list(self.parse_errors),
            "converted_at": self.converted_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConversionRecord":
        return cls(
            record_id=data["record_id"],
            schema_version=data["schema_version"],
            source_marker=data["source_marker"],
            origin=data["origin"],
            operation=data["operation"],
            source_coordinate=dict(data["source_coordinate"]),
            coordinate_key=data["coordinate_key"],
            before=MirrorSlot.from_dict(data["before"]),
            after=MirrorSlot.from_dict(data["after"]),
            evidence_completeness=data["evidence_completeness"],
            parse_errors=list(data.get("parse_errors", [])),
            converted_at=data.get("converted_at", ""),
        )


@dataclass
class FilterDecision:
    """事件过滤决策：保留每项判定所用字段值与镜像版本。"""

    decision_id: str
    record_id: str
    coordinate_key: str
    policy_name: str
    policy_version: str
    result: str                              # DELIVERED / HELD_FOR_REPLAY
    evidence_fields: dict[str, Any]          # 判定实际读取的字段与值
    mirror_versions: dict[str, str]          # before/after 槽位版本与状态
    reasons: list[str] = field(default_factory=list)
    decided_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FilterDecision":
        return cls(**data)


def build_coordinate_key(source_coordinate: dict[str, Any]) -> str:
    """源坐标归一化哈希：同一 binlog 位点的同一行变更永远得到同一 key。"""
    canonical = json.dumps(source_coordinate, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_record_id(coordinate_key: str, fingerprint: str) -> str:
    digest = hashlib.sha256(
        f"{coordinate_key}|{fingerprint}".encode("utf-8")
    ).hexdigest()[:16]
    return f"cv2-{digest}"
