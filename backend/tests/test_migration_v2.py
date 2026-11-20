"""mirror-json-v2 迁移全链路测试。

以 `python3 -I -m unittest discover -s tests` 方式运行（-I 隔离环境，
夹具/产物目录与代码目录分离，仅依赖标准库）。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

# -I 隔离模式下 cwd 不在 sys.path，显式加入 backend 根目录
_BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)

from app.migration_v2.adapter import LegacyEnvelopeError, convert_envelope
from app.migration_v2.filters import (
    POLICY_AFTER_ONLY,
    POLICY_FULL_INTEGRITY,
    evaluate,
)
from app.migration_v2.ledger import MigrationLedger
from app.migration_v2.models import (
    MISSING_REASONS,
    SCHEMA_VERSION_V2,
    MirrorState,
)
from app.migration_v2.pipeline import (
    ingest_envelope,
    ingest_batch,
    replay_pending,
    replay_scope,
)

FIXTURE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "migration_data",
    "fixtures",
    "legacy_batch.json",
)


def _envelope(before, after, *, status=None, pos=1, marker="1.9.7.Final", op="u"):
    if status is not None and isinstance(after, dict):
        after = dict(after)
        after["status"] = status
    return {
        "schema_marker": marker,
        "payload": {
            "before": before,
            "after": after,
            "op": op,
            "source": {
                "connector": "mysql",
                "connector_version": marker,
                "db": "sales",
                "table": "orders",
                "file": "mysql-bin.000042",
                "pos": pos,
                "gtid": f"gtid-{pos}",
                "ts_ms": 1763630400000,
            },
        },
    }


class AdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        with open(FIXTURE_PATH, "r", encoding="utf-8") as fh:
            self.fixtures = json.load(fh)

    def test_bad_before_text_is_not_coerced_to_empty_or_null(self):
        """坏 before 文本：UNPARSEABLE + 原因 + 原文留痕，value 必须为 None。"""
        record = convert_envelope(self.fixtures[0], origin="legacy-env/legacy_batch.json")
        self.assertEqual(record.schema_version, SCHEMA_VERSION_V2)
        self.assertEqual(record.source_marker, "1.9.7.Final")
        self.assertEqual(record.before.state, MirrorState.UNPARSEABLE)
        self.assertIsNone(record.before.value)
        self.assertEqual(record.before.raw_text, "{oops")
        self.assertEqual(record.before.missing_reason, "BEFORE_JSON_UNPARSEABLE")
        self.assertIn("BEFORE_JSON_UNPARSEABLE", MISSING_REASONS)
        self.assertTrue(record.parse_errors)
        # after 完整且 PAID
        self.assertEqual(record.after.state, MirrorState.PARSED)
        self.assertEqual(record.after.value["status"], "PAID")
        self.assertEqual(record.evidence_completeness, "PARTIAL")

    def test_complete_record_persists_both_mirrors_and_version(self):
        """双侧完整记录：前后镜像 PARSED，COMPLETE，源坐标含行键。"""
        record = convert_envelope(self.fixtures[1], origin="legacy-env/legacy_batch.json")
        self.assertEqual(record.before.state, MirrorState.PARSED)
        self.assertEqual(record.before.value["status"], "CREATED")
        self.assertEqual(record.after.value["status"], "PAID")
        self.assertEqual(record.evidence_completeness, "COMPLETE")
        self.assertEqual(record.source_coordinate["row_key"], {"id": 1002})
        self.assertEqual(record.source_coordinate["binlog_position"], 88501)

    def test_string_before_that_is_valid_json_is_parsed(self):
        envelope = _envelope('{"id": 9, "status": "CREATED"}', {"id": 9, "status": "PAID"}, pos=9)
        record = convert_envelope(envelope, origin="test")
        self.assertEqual(record.before.state, MirrorState.PARSED)
        self.assertEqual(record.before.value["id"], 9)
        self.assertEqual(record.before.observed_type, "json-string")

    def test_unsupported_marker_rejected(self):
        envelope = _envelope(None, {"id": 1}, marker="2.4.0.Final", pos=2)
        with self.assertRaises(LegacyEnvelopeError):
            convert_envelope(envelope, origin="test")

    def test_missing_payload_rejected(self):
        with self.assertRaises(LegacyEnvelopeError):
            convert_envelope({"schema_marker": "1.9.7.Final"}, origin="test")

    def test_non_parsed_slot_requires_reason_invariant(self):
        from app.migration_v2.models import MirrorSlot

        with self.assertRaises(ValueError):
            MirrorSlot(state=MirrorState.UNPARSEABLE, observed_type="json-string", value={})
        with self.assertRaises(ValueError):
            MirrorSlot(state=MirrorState.ABSENT, observed_type="null")

    def test_absent_before_uses_defined_reason(self):
        envelope = _envelope(None, {"id": 3, "status": "PAID"}, pos=3)
        record = convert_envelope(envelope, origin="test")
        self.assertEqual(record.before.state, MirrorState.ABSENT)
        self.assertEqual(record.before.missing_reason, "BEFORE_FIELD_ABSENT")
        self.assertIsNone(record.before.value)


class FilterTests(unittest.TestCase):
    def setUp(self) -> None:
        with open(FIXTURE_PATH, "r", encoding="utf-8") as fh:
            self.fixtures = json.load(fh)
        self.bad = convert_envelope(self.fixtures[0], origin="f")
        self.good = convert_envelope(self.fixtures[1], origin="f")

    def test_v1_after_only_passes_bad_before_paid_after(self):
        decision = evaluate(self.bad, POLICY_AFTER_ONLY)
        self.assertEqual(decision.result, "DELIVERED")
        self.assertEqual(decision.evidence_fields["after.state"], MirrorState.PARSED)
        self.assertEqual(decision.evidence_fields["after.$.status"], "PAID")
        self.assertEqual(decision.evidence_fields["before.state"], MirrorState.UNPARSEABLE)
        self.assertEqual(
            decision.evidence_fields["before.missing_reason"], "BEFORE_JSON_UNPARSEABLE"
        )
        self.assertEqual(decision.mirror_versions["schema_version"], SCHEMA_VERSION_V2)
        self.assertTrue(
            any("BEFORE_IGNORED_BY_V1_RULE" in r for r in decision.reasons)
        )

    def test_v2_full_integrity_holds_bad_before(self):
        decision = evaluate(self.bad, POLICY_FULL_INTEGRITY)
        self.assertEqual(decision.result, "HELD_FOR_REPLAY")
        self.assertTrue(
            any("FULL_MIRROR_INTEGRITY_REQUIRED" in r for r in decision.reasons)
        )
        self.assertEqual(decision.evidence_fields["after.$.status"], "PAID")

    def test_v2_passes_complete_record(self):
        self.assertEqual(evaluate(self.good, POLICY_FULL_INTEGRITY).result, "DELIVERED")
        self.assertEqual(evaluate(self.good, POLICY_AFTER_ONLY).result, "DELIVERED")

    def test_non_paid_after_blocked_under_both_policies(self):
        envelope = _envelope(
            {"id": 5, "status": "CREATED"}, {"id": 5, "status": "CANCELLED"}, pos=5
        )
        record = convert_envelope(envelope, origin="t")
        d1 = evaluate(record, POLICY_AFTER_ONLY)
        d2 = evaluate(record, POLICY_FULL_INTEGRITY)
        self.assertEqual(d1.result, "HELD_FOR_REPLAY")
        self.assertEqual(d2.result, "HELD_FOR_REPLAY")


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        with open(FIXTURE_PATH, "r", encoding="utf-8") as fh:
            self.fixtures = json.load(fh)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def ledger(self, name: str) -> MigrationLedger:
        return MigrationLedger(os.path.join(self.tmp.name, name))

    def test_legacy_v1_delivers_both_and_empty_replay_scope(self):
        ledger = self.ledger("legacy")
        summary = ingest_batch(
            ledger=ledger,
            envelopes=self.fixtures,
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_AFTER_ONLY,
        )
        self.assertEqual(len(summary["delivered"]), 2)
        self.assertEqual(summary["held_for_replay"], [])
        self.assertEqual(replay_scope(ledger), [])
        # 正常事件前后镜像和版本一并持久化
        for record in ledger.load_conversions():
            self.assertEqual(record.schema_version, SCHEMA_VERSION_V2)
            self.assertIsNotNone(record.before)
            self.assertIsNotNone(record.after)
        # 决策证据字段与版本齐全
        for decision in ledger.load_decisions():
            self.assertIn("after.$.status", decision.evidence_fields)
            self.assertIn("schema_version", decision.mirror_versions)
            self.assertTrue(decision.policy_version)

    def test_new_v2_delivers_only_complete_one_and_holds_bad(self):
        ledger = self.ledger("new")
        summary = ingest_batch(
            ledger=ledger,
            envelopes=self.fixtures,
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_FULL_INTEGRITY,
        )
        self.assertEqual(len(summary["delivered"]), 1)
        self.assertEqual(len(summary["held_for_replay"]), 1)
        scope = replay_scope(ledger)
        self.assertEqual(len(scope), 1)
        held = scope[0]
        self.assertEqual(held.before.state, MirrorState.UNPARSEABLE)
        self.assertEqual(held.before.raw_text, "{oops")
        self.assertEqual(held.after.value["status"], "PAID")

    def test_upgrade_replay_keeps_duplicate_suppression(self):
        """S1 已交付坐标在升级新策略后不得重复交付；坏记录被新策略识别为完整性缺口。"""
        ledger = self.ledger("upgrade")
        ingest_batch(
            ledger=ledger,
            envelopes=self.fixtures,
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_AFTER_ONLY,
        )
        delivered_after_v1 = set(ledger.delivered_coordinates())
        self.assertEqual(len(delivered_after_v1), 2)
        # v1 下全部放行，操作待办（待重放范围）为空
        self.assertEqual(replay_scope(ledger), [])

        # 新策略重判全部存量：两条都已交付过，故不会重复交付；
        # 完整记录被抑制，坏记录进入新策略完整性整改清单
        replay = replay_pending(
            ledger=ledger, policy_name=POLICY_FULL_INTEGRITY, scope="all"
        )
        self.assertEqual(replay["delivered_on_replay"], [])
        self.assertEqual(len(replay["suppressed_duplicates"]), 1)
        self.assertEqual(len(replay["newly_held_under_policy"]), 1)
        self.assertEqual(ledger.delivered_coordinates(), delivered_after_v1)

        # 同一批存量再摄入一次：v2 下两条都不应产生新交付
        reingest = ingest_batch(
            ledger=ledger,
            envelopes=self.fixtures,
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_FULL_INTEGRITY,
        )
        self.assertEqual(reingest["delivered"], [])
        self.assertEqual(len(reingest["suppressed_duplicates"]), 1)
        self.assertEqual(len(reingest["held_for_replay"]), 1)
        self.assertEqual(ledger.delivered_coordinates(), delivered_after_v1)

    def test_repeated_conversion_never_redelivers_confirmed_coordinate(self):
        ledger = self.ledger("idem")
        first = ingest_envelope(
            ledger=ledger,
            envelope=self.fixtures[1],
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_FULL_INTEGRITY,
        )
        second = ingest_envelope(
            ledger=ledger,
            envelope=self.fixtures[1],
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_FULL_INTEGRITY,
        )
        self.assertEqual(first["delivery_status"], "DELIVERED")
        self.assertTrue(second["repeated_conversion"])
        self.assertEqual(second["delivery_status"], "SUPPRESSED_DUPLICATE")
        self.assertEqual(len(ledger.load_deliveries()), 1)
        # 转换记录仍每次留痕
        self.assertEqual(len(ledger.load_conversions()), 2)

    def test_held_scope_is_stable_across_repeated_replays(self):
        ledger = self.ledger("held")
        ingest_batch(
            ledger=ledger,
            envelopes=[self.fixtures[0]],
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_FULL_INTEGRITY,
        )
        first_scope = {r.coordinate_key for r in replay_scope(ledger)}
        replay_pending(ledger=ledger, policy_name=POLICY_FULL_INTEGRITY)
        second_scope = {r.coordinate_key for r in replay_scope(ledger)}
        self.assertEqual(first_scope, second_scope)
        self.assertEqual(len(ledger.load_deliveries()), 0)
        # held 范围重放：坏 before 无法修复，结论保持 STILL_HELD
        again = replay_pending(ledger=ledger, policy_name=POLICY_FULL_INTEGRITY, scope="held")
        self.assertEqual(len(again["still_held"]), 1)
        self.assertEqual(again["delivered_on_replay"], [])

    def test_ledger_survives_reopen(self):
        state_dir = os.path.join(self.tmp.name, "persist")
        ledger = MigrationLedger(state_dir)
        ingest_batch(
            ledger=ledger,
            envelopes=self.fixtures,
            origin="legacy-env/legacy_batch.json",
            policy_name=POLICY_FULL_INTEGRITY,
        )
        reopened = MigrationLedger(state_dir)
        self.assertEqual(len(reopened.load_conversions()), 2)
        self.assertEqual(len(reopened.load_decisions()), 2)
        self.assertEqual(len(reopened.delivered_coordinates()), 1)
        self.assertEqual(len(replay_scope(reopened)), 1)


if __name__ == "__main__":
    unittest.main()
