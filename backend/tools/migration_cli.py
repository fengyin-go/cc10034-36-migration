"""mirror-json-v2 迁移命令行：ingest / replay / show。

用法（始终以 -I 隔离模式运行，避免从不可信数据目录加载模块）：
  python3 -I tools/migration_cli.py ingest --fixture <path> --state <dir> --policy <name> --origin <name>
  python3 -I tools/migration_cli.py replay --state <dir> --policy <name>
  python3 -I tools/migration_cli.py show --state <dir>
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# -I 模式下 sys.path 不含 cwd，显式把 backend 根目录加入模块搜索路径
_BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)

from app.migration_v2.filters import POLICIES  # noqa: E402
from app.migration_v2.ledger import MigrationLedger  # noqa: E402
from app.migration_v2.models import MISSING_REASONS  # noqa: E402
from app.migration_v2.pipeline import ingest_batch, replay_pending, replay_scope  # noqa: E402


def _load_fixture(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise SystemExit(f"夹具文件必须是事件数组：{path}")
    return data


def _print(obj: object) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Debezium 1.9.7.Final → mirror-json-v2 迁移工具")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="导入一批旧信封并执行适配+过滤")
    p_ingest.add_argument("--fixture", required=True)
    p_ingest.add_argument("--state", required=True)
    p_ingest.add_argument("--policy", required=True, choices=sorted(POLICIES))
    p_ingest.add_argument("--origin", required=True)

    p_replay = sub.add_parser("replay", help="按当前策略回放待重放范围")
    p_replay.add_argument("--state", required=True)
    p_replay.add_argument("--policy", required=True, choices=sorted(POLICIES))
    p_replay.add_argument(
        "--scope",
        choices=["held", "all"],
        default="held",
        help="held=只回放待重放范围；all=新策略重判全部存量（升级场景）",
    )

    p_show = sub.add_parser("show", help="查看账本与待重放范围")
    p_show.add_argument("--state", required=True)

    p_reasons = sub.add_parser("reasons", help="输出缺失原因目录")

    args = parser.parse_args(argv)

    if args.command == "reasons":
        _print(MISSING_REASONS)
        return 0

    ledger = MigrationLedger(args.state)

    if args.command == "ingest":
        envelopes = _load_fixture(args.fixture)
        summary = ingest_batch(
            ledger=ledger,
            envelopes=envelopes,
            origin=args.origin,
            policy_name=args.policy,
        )
        _print(summary)
        return 0

    if args.command == "replay":
        _print(replay_pending(
            ledger=ledger, policy_name=args.policy, scope=args.scope
        ))
        return 0

    if args.command == "show":
        snapshot = ledger.export()
        snapshot["replay_scope"] = [r.to_dict() for r in replay_scope(ledger)]
        _print(snapshot)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
