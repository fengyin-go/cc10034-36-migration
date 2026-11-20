"""迁移账本：转换记录 / 过滤决策 / 交付确认三类 JSONL 分盘持久化。

所有状态落盘，进程重启后可重建。deliveries.jsonl 只记录“实际已交付”的源
坐标；重复转换命中已确认坐标时由 pipeline 层抑制再次交付。
"""
from __future__ import annotations

import json
import os
from typing import Any

from app.migration_v2.models import ConversionRecord, FilterDecision

CONVERSIONS_FILE = "conversions.jsonl"
DECISIONS_FILE = "filter_decisions.jsonl"
DELIVERIES_FILE = "deliveries.jsonl"


def _append_jsonl(path: str, obj: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # 单行原子追加：每行一条 JSON，崩溃最多损坏最后一行且可被 load 跳过
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


class MigrationLedger:
    """文件系统账本。每个 state_dir 对应一条独立部署/策略链路。"""

    def __init__(self, state_dir: str) -> None:
        self.state_dir = state_dir
        os.makedirs(state_dir, exist_ok=True)
        self.conversions_path = os.path.join(state_dir, CONVERSIONS_FILE)
        self.decisions_path = os.path.join(state_dir, DECISIONS_FILE)
        self.deliveries_path = os.path.join(state_dir, DELIVERIES_FILE)

    # ---- 转换记录 ----
    def append_conversion(self, record: ConversionRecord) -> None:
        _append_jsonl(self.conversions_path, record.to_dict())

    def load_conversions(self) -> list[ConversionRecord]:
        return [ConversionRecord.from_dict(row) for row in _read_jsonl(self.conversions_path)]

    def has_conversion(self, record_id: str) -> bool:
        return any(item.record_id == record_id for item in self.load_conversions())

    # ---- 过滤决策 ----
    def append_decision(self, decision: FilterDecision) -> None:
        _append_jsonl(self.decisions_path, decision.to_dict())

    def load_decisions(self) -> list[FilterDecision]:
        return [FilterDecision.from_dict(row) for row in _read_jsonl(self.decisions_path)]

    # ---- 交付确认 ----
    def append_delivery(self, entry: dict[str, Any]) -> None:
        _append_jsonl(self.deliveries_path, entry)

    def load_deliveries(self) -> list[dict[str, Any]]:
        return _read_jsonl(self.deliveries_path)

    def delivered_coordinates(self) -> set[str]:
        """已确认交付的源坐标集合——重复转换不得再交付的依据。"""
        return {row["coordinate_key"] for row in self.load_deliveries()}

    def is_delivered(self, coordinate_key: str) -> bool:
        return coordinate_key in self.delivered_coordinates()

    # ---- 通用导出 ----
    def export(self) -> dict[str, Any]:
        return {
            "state_dir": self.state_dir,
            "conversions": [r.to_dict() for r in self.load_conversions()],
            "decisions": [d.to_dict() for d in self.load_decisions()],
            "deliveries": self.load_deliveries(),
        }
