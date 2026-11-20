"""Debezium 旧版事件信封 → 统一 JSON 镜像结构（mirror-json-v2）迁移包。

职责边界：
- adapter：把标记 1.9.7.Final 的旧信封转换为明确的第二版镜像转换记录；
- filters：事件过滤以转换记录为唯一输入，判定证据（字段+版本）随决策持久化；
- ledger/pipeline：转换记录、过滤决策、交付确认三类账本分盘持久化，
  重复转换不得再次交付已确认的源坐标。

设计红线：无法解析的镜像文本只能以 UNPARSEABLE + 缺失原因 + 原文留痕，
禁止回退成空对象 {} 或 null 冒充“空镜像”。
"""
from app.migration_v2.adapter import LegacyEnvelopeError, convert_envelope
from app.migration_v2.filters import POLICIES, evaluate
from app.migration_v2.ledger import MigrationLedger
from app.migration_v2.pipeline import ingest_envelope, replay_pending, replay_scope
from app.migration_v2.models import (
    MISSING_REASONS,
    SCHEMA_VERSION_V2,
    SUPPORTED_SOURCE_MARKERS,
    ConversionRecord,
    FilterDecision,
    MirrorSlot,
    MirrorState,
)

__all__ = [
    "LegacyEnvelopeError",
    "convert_envelope",
    "POLICIES",
    "evaluate",
    "MigrationLedger",
    "ingest_envelope",
    "replay_pending",
    "replay_scope",
    "MISSING_REASONS",
    "SCHEMA_VERSION_V2",
    "SUPPORTED_SOURCE_MARKERS",
    "ConversionRecord",
    "FilterDecision",
    "MirrorSlot",
    "MirrorState",
]
