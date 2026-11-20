# Debezium 1.9.7.Final → mirror-json-v2 镜像迁移

把标记 `1.9.7.Final` 的旧版 Debezium 信封迁移到统一的第二版 JSON 镜像结构，
覆盖「格式适配 → 事件过滤 → 交付确认 → 升级回放」全链路。

## 目录

```text
backend/app/migration_v2/
├── models.py    # mirror-json-v2 结构、MirrorSlot 不变量、缺失原因目录、源坐标哈希
├── adapter.py   # 旧信封解析（before 字符串/对象、after 对象、Debezium op 码）
├── filters.py   # 两种过滤配置，决策证据保留判定字段与版本
├── ledger.py    # conversions / filter_decisions / deliveries 三类 JSONL 账本
└── pipeline.py  # 摄入、重复抑制、待重放范围、升级回放
backend/tools/   # migration_cli.py、run_migration_demo.py（与数据目录隔离，python3 -I 运行）
backend/tests/   # test_migration_v2.py（17 项，纯标准库）
migration_data/
├── fixtures/legacy_batch.json   # 不可信导入数据（独立目录）
└── runs/<时间戳>/               # 每次演练的账本与报告
```

## 核心红线

1. **坏文本不得降级为空对象或 null 镜像。** `payload.before="{oops"` 解析失败时，
   槽位为 `state=UNPARSEABLE, value=null, missing_reason=BEFORE_JSON_UNPARSEABLE,
   raw_text="{oops"`；`MirrorSlot.__post_init__` 强制「非 PARSED 不允许带 value、
   必须带缺失原因」不变量。对照旧实现 `services/format_adapter.py:101` 静默写成
   `{"before": {}, "after": {}}` 的行为，本包明确禁止。
2. **正常事件前后镜像和版本一并持久化。** 每条转换记录都含
   `schema_version=mirror-json-v2`、source_marker、before/after 槽位与源坐标。
3. **过滤只消费转换记录。** 每条决策持久化 `evidence_fields`（实际读取的字段路径、
   观测值、槽位状态）和 `mirror_versions`（镜像版本 + 策略版本）。
4. **重复转换不得再交付已确认坐标。** `deliveries.jsonl` 是交付坐标的唯一事实源；
   重复转换仍追加转换/决策留痕，但交付状态为 `SUPPRESSED_DUPLICATE`。

## 两种过滤配置

| 策略 | 版本 | 规则 | 坏 before + after.status=PAID |
| --- | --- | --- | --- |
| `after_image_only_v1` | filter-1.0 | 只看后镜像与 `after.status=PAID`，before 不参与拦截（证据仍记录） | DELIVERED |
| `full_mirror_integrity_v2` | filter-2.0 | before/after 均须 PARSED 且 `after.status=PAID` | HELD_FOR_REPLAY |

## 回放语义

- `replay_scope`：最新决策 HELD 且坐标未确认交付的记录（操作待办）。
- `replay_pending(scope="held")`：只重放待办，坏镜像无法修复则保持 STILL_HELD。
- `replay_pending(scope="all")`：升级场景，用新策略重判全部存量——
  已交付坐标只产生 `SUPPRESSED_DUPLICATE`（完整记录）或
  `NEWLY_HELD_UNDER_POLICY`（旧规则放过但新规则不满足的完整性缺口，需源端补齐镜像），
  绝不会重复投递。

## 运行

```bash
cd backend
python3 -I -m unittest discover -s tests          # 17 项测试
python3 -I tools/run_migration_demo.py            # 四场景升级演练，产物在 migration_data/runs/
python3 -I tools/migration_cli.py reasons         # 缺失原因目录
python3 -I tools/migration_cli.py ingest --fixture <path> --state <dir> \
    --policy after_image_only_v1 --origin legacy-env
python3 -I tools/migration_cli.py replay --state <dir> \
    --policy full_mirror_integrity_v2 --scope all
python3 -I tools/migration_cli.py show --state <dir>
```

## 验证结论（夹具：一笔 before="{oops" 的 UPDATE + 一笔双侧完整对照）

- **S1 旧版 v1**：两条均投递（含目标本轮需要的 PAID 订单字段），待重放为空。
- **S2 新版 v2（独立部署）**：完整记录投递；坏 before 记录挂起待重放，
  订单字段在该部署本轮不交付。
- **S3 同账本升级**：新策略全量重判，已交付坐标零重复投递；坏记录进入
  完整性整改清单；同批再摄入时完整记录被 SUPPRESSED_DUPLICATE。
- **S4 幂等**：重复转换不产生新交付，待重放范围跨重放保持稳定。
