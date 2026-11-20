"""端到端升级演练：同一批存量记录在两种过滤配置与新旧部署下的投递/回放验证。

场景：
  S1 旧版部署 + after_image_only_v1（现有只看后镜像规则）
  S2 新版部署 + full_mirror_integrity_v2（镜像完整性规则）
  S3 升级序列：S1 账本上部署新策略回放，验证已确认坐标不重复交付
  S4 新版重跑同一批两次，验证重复转换抑制与待重放范围稳定

所有产物写入 migration_data/runs/<时间戳>/，夹具路径由参数传入，
脚本不以任何数据目录为工作目录，配合 `python3 -I` 防止 planted 模块注入。
"""
from __future__ import annotations

import copy
import json
import os
import sys
from datetime import datetime, timezone

_BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)

from app.migration_v2.filters import (  # noqa: E402
    POLICY_AFTER_ONLY,
    POLICY_FULL_INTEGRITY,
)
from app.migration_v2.ledger import MigrationLedger  # noqa: E402
from app.migration_v2.models import MISSING_REASONS, MirrorState  # noqa: E402
from app.migration_v2.pipeline import (  # noqa: E402
    ingest_batch,
    replay_pending,
    replay_scope,
)

WORKSPACE = os.path.dirname(_BACKEND_ROOT)
DEFAULT_FIXTURE = os.path.join(WORKSPACE, "migration_data", "fixtures", "legacy_batch.json")
RUNS_DIR = os.path.join(WORKSPACE, "migration_data", "runs")


def _load_fixture(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_json(path: str, obj: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=True)


def _coordinate_label(record_dict: dict) -> str:
    coord = record_dict["source_coordinate"]
    row_key = coord.get("row_key") or {}
    return f"{coord['database']}.{coord['table']} id={row_key.get('id')} {coord['binlog_file']}:{coord['binlog_position']}"


def _summarize_ledger(ledger: MigrationLedger) -> dict:
    conversions = ledger.load_conversions()
    decisions = ledger.load_decisions()
    deliveries = ledger.load_deliveries()
    return {
        "conversions_persisted": len(conversions),
        "decisions_persisted": len(decisions),
        "deliveries_confirmed": len(deliveries),
        "delivered_coordinates": sorted(d["coordinate_key"] for d in deliveries),
        "replay_scope_coordinates": sorted(r.coordinate_key for r in replay_scope(ledger)),
        "conversion_evidence": [
            {
                "record_id": r.record_id,
                "coordinate": _coordinate_label(r.to_dict()),
                "schema_version": r.schema_version,
                "before_state": r.before.state,
                "before_missing_reason": r.before.missing_reason,
                "before_raw_text_preserved": r.before.raw_text,
                "after_state": r.after.state,
                "after_status": (r.after.value or {}).get("status")
                if r.after.state == MirrorState.PARSED
                else None,
                "evidence_completeness": r.evidence_completeness,
            }
            for r in conversions
        ],
    }


def main() -> int:
    fixture_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FIXTURE
    envelopes = _load_fixture(fixture_path)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = os.path.join(RUNS_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)

    _write_json(os.path.join(run_dir, "missing_reason_catalog.json"), MISSING_REASONS)
    report: list[dict] = []

    # ---------- S1：旧版部署，现有只看后镜像规则 ----------
    s1_dir = os.path.join(run_dir, "s1_legacy_after_only")
    s1_ledger = MigrationLedger(s1_dir)
    s1 = ingest_batch(
        ledger=s1_ledger,
        envelopes=copy.deepcopy(envelopes),
        origin="legacy-env/legacy_batch.json",
        policy_name=POLICY_AFTER_ONLY,
    )
    report.append({
        "scenario": "S1 旧版部署 + after_image_only_v1",
        "batch_summary": {k: v for k, v in s1.items() if k != "results"},
        "ledger": _summarize_ledger(s1_ledger),
    })
    _write_json(os.path.join(s1_dir, "ingest_summary.json"), s1)

    # ---------- S2：新版部署，完整镜像规则（独立新账本） ----------
    s2_dir = os.path.join(run_dir, "s2_new_full_integrity")
    s2_ledger = MigrationLedger(s2_dir)
    s2 = ingest_batch(
        ledger=s2_ledger,
        envelopes=copy.deepcopy(envelopes),
        origin="legacy-env/legacy_batch.json",
        policy_name=POLICY_FULL_INTEGRITY,
    )
    report.append({
        "scenario": "S2 新版部署 + full_mirror_integrity_v2",
        "batch_summary": {k: v for k, v in s2.items() if k != "results"},
        "ledger": _summarize_ledger(s2_ledger),
    })
    _write_json(os.path.join(s2_dir, "ingest_summary.json"), s2)

    # ---------- S3：升级序列——在 S1 账本上部署新策略并回放 ----------
    s3 = replay_pending(
        ledger=s1_ledger, policy_name=POLICY_FULL_INTEGRITY, scope="all"
    )
    # 升级后新到的同一批存量再走一遍新管道（验证已交付坐标被抑制）
    s3_reingest = ingest_batch(
        ledger=s1_ledger,
        envelopes=copy.deepcopy(envelopes),
        origin="legacy-env/legacy_batch.json",
        policy_name=POLICY_FULL_INTEGRITY,
    )
    report.append({
        "scenario": "S3 S1 账本升级新策略后的回放与重复摄入",
        "replay": s3,
        "reingest_summary": {k: v for k, v in s3_reingest.items() if k != "results"},
        "ledger": _summarize_ledger(s1_ledger),
    })
    _write_json(os.path.join(s1_dir, "upgrade_replay.json"), s3)
    _write_json(os.path.join(s1_dir, "upgrade_reingest_summary.json"), s3_reingest)

    # ---------- S4：新版部署重放同一批存量两次（幂等性） ----------
    s4_dir = os.path.join(run_dir, "s4_new_idempotency")
    s4_ledger = MigrationLedger(s4_dir)
    first = ingest_batch(
        ledger=s4_ledger,
        envelopes=copy.deepcopy(envelopes),
        origin="legacy-env/legacy_batch.json",
        policy_name=POLICY_FULL_INTEGRITY,
    )
    second = ingest_batch(
        ledger=s4_ledger,
        envelopes=copy.deepcopy(envelopes),
        origin="legacy-env/legacy_batch.json",
        policy_name=POLICY_FULL_INTEGRITY,
    )
    second_replay = replay_pending(
        ledger=s4_ledger, policy_name=POLICY_FULL_INTEGRITY, scope="held"
    )
    report.append({
        "scenario": "S4 新版部署重复转换 + 待重放幂等",
        "first_run": {k: v for k, v in first.items() if k != "results"},
        "second_run": {k: v for k, v in second.items() if k != "results"},
        "replay_of_held": second_replay,
        "ledger": _summarize_ledger(s4_ledger),
    })
    _write_json(os.path.join(s4_dir, "first_ingest.json"), first)
    _write_json(os.path.join(s4_dir, "second_ingest.json"), second)
    _write_json(os.path.join(s4_dir, "replay_held.json"), second_replay)

    _write_json(os.path.join(run_dir, "report.json"), report)

    print(f"运行目录: {run_dir}")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
