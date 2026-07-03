# Ingest Layer: ResultDealer + EventBus

Medallion 架构编排层：事件总线驱动，ResultDealer 根据返回结果决定下一步。

## 核心设计

Pipeline 不是 Bronze/Silver 的 wrapper。Pipeline 的职责是**事件流转**和**串行约束**，
不是逐个给 Bronze/Silver 调用包一层函数。

三个核心组件：

1. **EventBus**：状态机 + 事件排队 + 串行保证
2. **ResultDealer**：看结果 → 决定拉起什么下游
3. **Pipeline**：薄层接线，把上面两个粘在一起

```
                    Pipeline
                    ┌──────────────────────────────────────┐
                    │                                      │
trigger ──emit──→   │  EventBus ──handler──→ Bronze.execute │
                    │                 │                    │
                    │                 ↓ IngestResult[]     │
                    │             ResultDealer             │
                    │           ┌───┤┌───┐┌────────┐      │
                    │           │   ││   ││        │      │
                    │           ↓   ↓↓   ↓↓        ↓      │
                    │     silver retry check emit_complete │
                    │                 │                    │
                    │                 ↓ BuildResult[]      │
                    │             ResultDealer             │
                    │                 │                    │
                    │                 ↓                    │
                    │           emit_complete → idle       │
                    └──────────────────────────────────────┘
```

## 为什么不用硬编码映射

之前的做法：

```python
# ❌ 每个 action 写一个 handler，逻辑几乎一样
def _on_daily_close(self, event):
    results = list(self._bronze.execute(IngestOrder(action="fast_close")))

def _on_daily_evening(self, event):
    results = list(self._bronze.execute(IngestOrder(action="light_daily")))

# ❌ 硬编码 action → silver targets
ACTION_SILVER_MAP = {
    "light_daily": ["daily_kline", "adj_factor"],
    "night_full": ["stock_map", "daily_kline", ...],
}
```

问题：
- 三个定时 handler 逻辑几乎相同，只是 action 名不同
- action → silver 的映射是预设的，新增数据源要改映射表
- Pipeline 大部分代码在包装 Bronze/Silver 调用，事件机制本身只有几十行

## ResultDealer

**核心思想**：看 Bronze/Silver 返回了什么，决定拉起什么。

### Bronze ResultDealer

Bronze 返回 `IngestResult(source, table, status, rows, errors)`。
ResultDealer 检查每个 result 的 `table` 字段，映射到 Silver target：

```
IngestResult.table                     →  Silver target
─────────────────────────────────────────────────────────
bronze.raw_tdx_kline_daily_bfq         →  daily_kline
bronze.raw_tencent_kline_day_bfq       →  daily_kline
bronze.raw_tdx_kline_m1_*_bfq          →  minute_kline
bronze.raw_tencent_minute              →  minute_kline
bronze.raw_tdx_xdxr                    →  adj_factor
bronze.raw_tdx_finance                 →  finance + stock_map
bronze.raw_tdx_stocks                  →  stock_map
bronze.raw_tdx_f10                     →  f10_doc
bronze.raw_tdx_quotes                  →  （无 Silver 消费）
bronze.raw_tencent_quotes              →  （无 Silver 消费）
bronze.raw_tencent_brief               →  （无 Silver 消费）
bronze.raw_tdx_block                   →  （无 Silver 消费）
bronze.raw_tdx_index_daily_bfq         →  （暂无 Silver 消费）
```

同时检查 `errors`，提取 failed codes 决定是否 retry。

```python
class ResultDealer:
    # table 前缀 → Silver target 映射
    TABLE_TARGET_MAP = {
        "raw_tdx_kline_daily_bfq":   "daily_kline",
        "raw_tencent_kline_day_bfq": "daily_kline",
        "raw_tdx_kline_m1":          "minute_kline",   # 前缀匹配，含 _2026_bfq 等
        "raw_tencent_minute":        "minute_kline",
        "raw_tdx_xdxr":             "adj_factor",
        "raw_tdx_finance":          "finance",
        "raw_tdx_stocks":           "stock_map",
        "raw_tdx_f10":              "f10_doc",
    }

    def deal_bronze(self, results: list[IngestResult]) -> Deal:
        """看 Bronze 结果，决定下一步。"""
        targets = set()
        for r in results:
            if r.status in ("ok", "partial"):
                target = self._match_table(r.table)
                if target:
                    targets.add(target)

        failed_codes = self._extract_failed_codes(results)

        return Deal(
            silver_targets=sorted(targets),
            failed_codes=failed_codes,
        )
```

### Silver ResultDealer

Silver 返回 `BuildResult(target, status, rows_read, rows_written, rows_rejected, errors)`。

Silver 构建完之后，需要决定四件事：

```
Silver ResultDealer 的决策逻辑：

1. Silver 自身失败？
   ├─ status = "partial" / "fail"
   │  → 从 errors 提取 failed_codes
   │  → 这些 codes 的 Bronze 数据可能有质量问题
   │  → 需要 Bronze 重新拉 + Silver repair
   │
   └─ status = "ok"
      → 无需处理

2. Bronze 之前有 failed_codes？
   ├─ 有 → 执行 Bronze retry(codes)
   │       retry 成功的 codes → Silver repair(succeeded_codes)
   │       retry 仍失败的 → 放弃，下次调度自动补
   │
   └─ 无 → 跳过

3. 需要完整性检查？
   ├─ daily_kline 参与了构建 → check_completeness()
   │   ├─ 有缺失 → backfill + repair（最多 2 轮）
   │   └─ 无缺失 → 通过
   │
   └─ daily_kline 没参与 → 跳过

4. 以上都不需要 → emit_complete → idle
```

**注意**：retry 在 Silver 之前执行，确保 Silver 拿到尽可能完整的数据。

```python
    def deal_silver(self, silver_results: list[BuildResult]) -> SilverDeal:
        """看 Silver 结果，决定下一步。"""

        # Silver 自身失败
        silver_failed = []
        for r in silver_results:
            if r.status in ("partial", "fail"):
                codes = self._extract_codes_from_errors(r.errors)
                silver_failed.extend(codes)

        # 是否需要完整性检查
        need_check = any(r.target == "daily_kline" for r in silver_results)

        # 需要 Silver repair 的 targets
        repair_targets = [r.target for r in silver_results
                          if r.status == "partial"]

        return SilverDeal(
            silver_failed_codes=sorted(set(silver_failed)),
            need_completeness_check=need_check,
            repair_targets=repair_targets,
        )
```

### Deal 数据结构

```python
@dataclass
class BronzeDeal:
    """Bronze ResultDealer 的决策。"""
    silver_targets: list[str] = field(default_factory=list)
    failed_codes: list[str] = field(default_factory=list)


@dataclass
class SilverDeal:
    """Silver ResultDealer 的决策。"""
    silver_failed_codes: list[str] = field(default_factory=list)  # Silver 自身失败的 codes
    repair_targets: list[str] = field(default_factory=list)       # 需要 repair 的 Silver target
    need_completeness_check: bool = False                         # 是否需要完整性检查
```

## 通用事件流

有了 ResultDealer，Pipeline 的流程变成**通用**的，不再因 action 不同而写不同 handler：

```
schedule.* 触发
  → Bronze.execute(order)       # order 来自事件 data
  → results
  → ResultDealer.deal_bronze(results) → BronzeDeal

  → [if failed_codes] Bronze retry(failed_codes)
     → 最多 N 轮，每轮只重试上一轮仍失败的
     → N 轮后仍失败的 → 搁置（记录日志，不再尝试）
     → retry 成功的 codes → Silver repair

  → [if silver_targets] Silver.execute(target, mode="incremental") for each
  → silver_results

  → [if daily_kline 参与构建] check_completeness()
     → [if gap] backfill + repair（最多 2 轮）

  → emit_complete → idle
```

**一个通用流程覆盖 fast_close / light_daily / night_full**。差别只在于：
- fast_close 返回的 results 里 table 是 quotes/brief → ResultDealer 知道不需要拉 Silver
- light_daily 返回 daily kline → ResultDealer 知道要拉 daily_kline + adj_factor
- night_full 返回全套 → ResultDealer 知道要拉 6 个 target

## EventBus 状态机

```
          ┌──────┐
          │ idle │ ← emit_complete
          └──┬───┘
             │ 事件触发
             ▼
     ┌───────────────┐
     │    running     │
     │               │
     │  子状态:       │
     │  .bronze.tdx   ← TDX 正在拉数据
     │  .bronze.tx    ← 腾讯正在拉数据
     │  .silver       ← Silver 正在构建
     │  .check        ← 完整性检查中
     └───────────────┘
```

### 调度规则

1. **硬约束**：前一个事件未结束 → 不能触发下一个（API 限频必须串行）
2. **软约束**：时间到了就可以触发

```
15:05  schedule.daily_close 触发
       → Bronze fast_close → ResultDealer → 无 Silver → emit_complete → idle

18:00  schedule.daily_evening 触发
       → 检查：idle? ✓ → 触发
       → Bronze light_daily → ResultDealer → Silver → retry → emit_complete → idle

20:00  schedule.daily_night 触发
       → 检查：idle?
         → daily_evening 还在跑 → 等待，结束后再触发
         → idle ✓ → 触发
       → Bronze night_full → ResultDealer → Silver → retry → check → backfill → ... → idle
```

### 事件排队

```
idle:   事件来了 → 立即触发 → running
running: 事件来了 → 加入队列
        当前事件 emit_complete → idle → 检查队列 → 有就触发下一个

队列优先级: schedule > completeness
```

## 每日管线流程图

### Flow 1: daily_close — 15:05

```
cron 15:05 → emit("schedule.trigger", action="fast_close")
  │
  ├─ Bronze fast_close → IngestResult(s)
  │   ├─ TDX quotes          ⚠ NET
  │   └─ Tencent quotes+brief ⚠ NET (并行)
  │
  ├─ ResultDealer.deal_bronze(results) → BronzeDeal
  │   └─ tables: [quotes, brief] → silver_targets: [] → 无 Silver
  │   └─ errors: [] → failed_codes: [] → 无 retry
  │
  └─ SilverDeal 全部不需要 → emit_complete → idle
```

### Flow 2: daily_evening — 18:00

```
cron 18:00 → emit("schedule.trigger", action="light_daily")
  │
  ├─ Bronze light_daily → IngestResult(s)
  │   ├─ stocks(如需)     ⚠ NET
  │   ├─ TDX daily        ⚠ NET
  │   └─ Tencent daily    ⚠ NET (并行)
  │
  ├─ ResultDealer.deal_bronze(results) → BronzeDeal
  │   └─ tables: [kline_daily, kline_day] → silver_targets: [daily_kline, adj_factor]
  │   └─ errors: [...] → failed_codes: [...]
  │
  ├─ Bronze retry(failed_codes) ⚠ NET（最多 2 轮）
  │   └─ retry 成功 → Silver repair(succeeded_codes, targets)
  │   └─ 仍失败的 → 搁置
  │
  ├─ Silver daily_kline (incremental)   ⚠ DB
  ├─ Silver adj_factor (incremental)    ⚠ DB
  │
  └─ emit_complete → idle
```

### Flow 3: daily_night — 20:00

```
cron 20:00 → emit("schedule.trigger", action="night_full")
  │
  ├─ Bronze night_full（含 auto_heal + dedup）→ IngestResult(s)
  │   ├─ TDX: daily/1m/xdxr/index/block/finance  ⚠ NET
  │   └─ Tencent: daily/minute                    ⚠ NET (并行)
  │
  ├─ ResultDealer.deal_bronze(results) → BronzeDeal
  │   └─ tables: [kline_daily, kline_m1_*, xdxr, finance, stocks, ...]
  │      → silver_targets: [stock_map, daily_kline, adj_factor, minute_kline, finance, f10_doc]
  │   └─ errors: [...] → failed_codes: [...]
  │
  ├─ Bronze retry(failed_codes) ⚠ NET（最多 2 轮）
  │   └─ retry 成功 → Silver repair(succeeded_codes, targets)
  │   └─ 仍失败的 → 搁置
  │
  ├─ Silver 6 target incremental     ⚠ DB
  │
  ├─ check_completeness()            ⚠ DB
  │   └─ [if gap] emit("completeness.gap", missing_codes)
  │       ├─ Bronze backfill(missing)  ⚠ NET
  │       ├─ Silver repair(missing)    ⚠ DB
  │       └─ re-check（最多 2 轮）
  │
  └─ emit_complete → idle
```

## 错误处理

### Bronze 层（已实现）

`_loop_fetch` 内建 early_stop：
- 连续 5 次失败 **或** 失败率达 10% → 停止后续请求
- **已拉数据先入库**，不丢
- 剩余未尝试的 codes 写入 errors：`"600519: skipped (early_stop)"`
- 返回 `IngestResult(status="partial")`

### ResultDealer 层

```
BronzeDeal:
  ├─ failed_codes 为空     → 直接进 Silver
  ├─ failed_codes 少量     → retry（最多 N 轮）→ 成功的 repair → 搁置仍失败的
  └─ failed_codes 大量     → 同上（底层可能还在 early_stop，retry 也会很快失败）

SilverDeal:
  ├─ silver_failed_codes   → 记录，下次调度补
  ├─ need_completeness_check → check → 有 gap 则 backfill + repair
  └─ 都不需要               → emit_complete → idle

搁置策略：
  retry 最多 N 轮（默认 2），仍失败的 codes 搁置：
  - 记录到日志（WARNING 级别）
  - 记录到 ingest.step（failed_codes 字段）
  - 不再尝试，等下次调度自动补
```

### 各环错误影响

| 失败环 | 后果 | 恢复方式 |
|--------|------|---------|
| ⚠ NET TDX TCP 超时 | 该 code 跳过，IngestResult partial | 下次 daily_night auto_heal |
| ⚠ NET Tencent HTTP 限频 | 同上 | 同上 |
| ⚠ NET 连续失败 early_stop | 已拉数据入库，剩余 skipped | 下次调度自动补 |
| ⚠ DB 写入失败 | 该股票数据缺失 | 修复 DB 后重跑 |
| ⚠ DISK F10 文件写入失败 | 有元数据无文件 | 重跑 f10 |

## 文件结构

```
ingest/
├── __init__.py       # 公开导出
├── event.py          # EventBus + PipelineEvent + BusState + 排队
├── dealer.py         # ResultDealer：看结果决定下一步
├── pipeline.py       # Pipeline = EventBus + 通用流程（薄层接线）
├── store.py          # DB 持久化（ingest.plan / ingest.step）
└── scheduler.py      # 定时调度，emit 定时事件
```

## 实施步骤

1. `dealer.py` — ResultDealer + Deal + TABLE_TARGET_MAP
2. `store.py` — DB 持久化拆出
3. `pipeline.py` — 通用流程重写：一个 handler 覆盖所有 schedule trigger
4. `scheduler.py` — cron emit 定时事件 + 串行约束检查
5. 测试：手动 trigger 验证 ResultDealer 决策
6. 测试：定时调度 + 串行约束
