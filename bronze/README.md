# Bronze — Bronze 层摄入引擎

> 依赖 `fetch/` + `db/`。编排层逐条发令，Bronze 层自闭合执行。

## 架构

```
编排层 (crontab / daily_pipeline.py)
  │  大领导：只发 IngestOrder，不管细节
  │  bronze.execute(IngestOrder(action="light_daily"))
  │
  ▼
指令层 (instruction.py: Bronze)
  │  小领导：看 action 分派 → 开线程调度 → 返回 IngestResult
  │  light_daily / fast_close / night_full / first_init / refresh_v2 → 套餐
  │  retry / backfill / f10 + codes → 根据代码格式自动路由源
  │
  ▼
执行层 (TDXBronze / TencentBronze)
  │  员工：自闭合执行，fetch → write → 返回 IngestResult
  │
  ▼
获取层 (fetch/tdx.py, fetch/tencent.py)
    纯接口调用，不接触数据库
```

## 指令协议

```python
from bronze.instruction import IngestOrder

@dataclass
class IngestOrder:
    action: str              # "light_daily" | "fast_close" | "night_full" | "first_init" | "refresh_v2" | "retry" | "backfill" | "f10"
    codes: list[str] = []    # retry/backfill/f10 用，格式自动路由源
```

## 指令层入口

唯一入口 `execute(order)`：

```python
from bronze import Bronze, IngestOrder

bronze = Bronze()

# 套餐
results = list(bronze.execute(IngestOrder(action="light_daily")))

# 补拉：收齐结果后按表定位失败代码
for r in results:
    if r.status == "partial" and "kline" in r.table:
        failed_codes = [e.split(":")[0] for e in r.errors]
        if failed_codes:
            bronze.execute(IngestOrder(action="retry", codes=failed_codes))
```

### `light_daily`
stocks + 双源不复权日线。stocks 今日已更新则跳过。主线程先确保 stocks 就绪，再并行拉双源日线。

### `fast_close`
Tencent quotes/brief + TDX quotes，全批量秒级。

### `night_full`
深夜全量档 + 自动补拉 + 去重。主线程先确保 stocks 就绪，再并行拉取：
- TDX 线程：日线 + 1m分钟线 + 指数日线 + xdxr + 板块 + 财务
- 腾讯线程：日线 + 分时

主流程结束后自动执行 `_auto_heal`：调用 `diagnose()` 检查数据完整性，对缺口表补拉缺失 codes，最后 `dedup()` 去除重复行。

### `first_init`
初次部署：主线程先确保 stocks 就绪，再并行拉取：
- TDX：日线 offset=800 + 1m 全量回填（get_kline_full，DROP 重建）
- 腾讯：日线 count=800

### `refresh_v2`
串行执行：先刷新股票列表，再拉财务数据。stocks 是 finance 的前置依赖。

### `retry`
根据 codes 格式自动路由源（sh600519→tencent, 600519→tdx），尝试 get_kline + get_quotes。

### `backfill`
从 IPO 日至今全量拉取指定股票日线（追加写入 + 去重）。调 `get_kline_full(force=False)`，表中已有数据不删除，新数据按主键去重写入。

### `f10`
F10 资料详情，调 `get_f10(symbols=codes)`。DB-aware 去重：同 code+hash 不重复写入。

### `status()` → DataFrame

返回所有 Bronze 表名及列结构（DuckDB SHOW ALL TABLES）。

### `diagnose()` → DataFrame

扫描今日数据完整性，返回各表健康报告：

| 列 | 含义 |
|----|------|
| `table` | 表名 |
| `source` | 数据源 (tdx/tencent) |
| `expected` | 预期股票数 |
| `present` | 今日已有股票数 |
| `missing` | 缺口数 |
| `status` | ok / gap / missing |
| `missing_codes` | 缺失代码列表 |

逐只类型的表（kline/xdxr/finance/minute）检查 code 覆盖率：从 `raw_tdx_stocks` 取 A 股列表为预期集合，从目标表取今日 DISTINCT code 为实际集合，差集即为缺失。全量类型的表（stocks/block/index）只检查今日是否有数据。

### `dedup()`

对逐只类型表去重：按业务 key 分组（如 kline 按 `stock_code + datetime`，xdxr 按 `code + year/month/day/category`），保留每组 `MAX(ingested_at)` 的行，删除其余重复。因 light_daily 和 night_full 同日运行会产生重复增量数据，dedup 保证表内无冗余。

去重规则：

| 表 | 去重 key |
|----|---------|
| `raw_tdx_kline_daily_bfq` | stock_code, datetime |
| `raw_tdx_kline_m1_bfq` | stock_code, datetime |
| `raw_tdx_xdxr` | code, year, month, day, category |
| `raw_tdx_finance` | code |
| `raw_tencent_kline_day_bfq` | stock_code, date |

---

## 指令清单

### TDXBronze

| 指令 | 遍历 | fetch 调用 | 写入表 |
|------|------|-----------|--------|
| `get_stocks()` | 无 | `tdx.stocks()` 1 次 | `bronze.raw_tdx_stocks` (DROP+INSERT) |
| `get_kline(freq, symbols?, start=0, offset=1)` | 全市场逐只 | `tdx.bars(code, FREQ[freq], start, offset)` | `bronze.raw_tdx_kline_{freq}_bfq` |
| `get_kline_full(freq, symbols?)` | 全市场逐只 | `tdx.bars(code, FREQ[freq], start=0, offset=800)` 分页循环 | `bronze.raw_tdx_kline_{freq}_bfq` (DROP+INSERT) |
| `get_index_kline(freq, symbols?)` | 指数列表 | `tdx.index_bars(code, FREQ[freq], ...)` | `bronze.raw_tdx_index_{freq}_bfq` |
| `get_quotes(symbols?)` | 无（内置分批 100 只/批） | `tdx.quotes(chunk)` | `bronze.raw_tdx_quotes` |
| `get_xdxr(symbols?)` | 全市场逐只 | `tdx.xdxr(code)` | `bronze.raw_tdx_xdxr` |
| `get_finance(symbols?)` | 全市场逐只 | `tdx.finance(code)` | `bronze.raw_tdx_finance` |
| `get_block()` | 无 | `tdx.block()` 1 次 | `bronze.raw_tdx_block` (DROP+INSERT) |
| `get_f10(name?, symbols?)` | 全市场逐只 | `tdx.f10(code, name)` | `bronze.raw_tdx_f10` (仅存指针) |

`freq`: `"daily"/"1m"/"5m"/"15m"/"30m"/"60m"/"weekly"/"monthly"/"quarterly"/"yearly"`

> **`get_xdxr`** 返回的 DataFrame 包含 `code` 列（插入在最前），标识每条除权除息记录所属股票。自含循环实现，不使用 `_loop_fetch`。

> **`get_kline` vs `get_kline_full`**：`get_kline` 默认 `offset=1`（增量，仅最新 1 根）；`get_kline_full` 分页循环拉全量历史（DROP 重建表），用于初次部署或全量回填。

> **TDX 分钟线历史深度限制**：实测 TDX 免费服务器分钟线仅保留近 5-6 个月数据（约 24000 条）。如需更长分钟线历史（如 10 年），需切换 baostock（从 1999 年起）或只做日线级别回测。`get_kline_full` 会拉取服务器端的全部可用数据，超出边界返回空。

> **`get_f10` 与其它方法不同**：F10 返回的是非结构化文本，不适合直接存入数据库。`name` 默认为空（全量拉取所有 16 个类别），内容合并写入 `data/bronze/f10/{code}_{YYYYMMDD}.txt`（文件名带拉取日期），DB 表仅存储元数据指针（`code, file_path, file_size, file_hash`）。`file_hash` 用于去重：启动时从 DB 加载已有 code+hash 集合，内容未变则跳过文件写入和入库。

### TencentBronze

| 指令 | 遍历 | fetch 调用 | 写入表 |
|------|------|-----------|--------|
| `get_kline(ktype, fq, symbols?, count=1)` | 全市场逐只 | `tencent.bars(code, ktype, fq, count)` | `bronze.raw_tencent_kline_{ktype}_{fq}` |
| `get_quotes(symbols?)` | 无（内置分批 800 只/批） | `tencent.quotes(codes)` | `bronze.raw_tencent_quotes` |
| `get_brief(symbols?)` | 无（内置分批 800 只/批） | `tencent.brief(codes)` | `bronze.raw_tencent_brief` |
| `get_minute(symbols?)` | 全市场逐只 | `tencent.minute(code)` | `bronze.raw_tencent_minute` |
| `get_money_flow(symbols?)` | 全市场逐只 | `tencent.money_flow(code)` | `bronze.raw_tencent_money_flow` |
| `get_order_book(symbols?)` | 全市场逐只 | `tencent.order_book(code)` | `bronze.raw_tencent_order_book` |

`ktype`: `"day"/"week"/"month"`, `fq`: `"qfq"/"hfq"/""(bfq)"`

> `get_kline` 的 `count` 参数控制单次拉取条数，默认 1（增量），最大约 800。腾讯 API 返回的 `extra` 列（分红信息）在 count=1 和 count>1 时列数可能不一致，已统一处理为始终包含 `extra` 列。

> `get_brief` 于 2026-06-20 改造为批量模式（与 `get_quotes` 一致），800 只/批，延迟 150ms。`tencent.brief()` 同步支持 `str` 和 `list[str]` 输入。

---

## 共享基类 BaseBronze

| 方法 | 职责 |
|------|------|
| `_load_stocks(source="tdx")` | 从 `bronze.raw_{source}_stocks` 读 A 股代码，过滤 `market='sz' AND (code LIKE '00%' OR '30%')` 或 `market='sh' AND (code LIKE '60%' OR '68%')`。当前约 5210 只 |
| `_load_indexes()` | 返回指数代码：`market='sz' AND code LIKE '399%'` 或 `market='sh' AND code LIKE '000%'`。约 569 只 |
| `_rate_limit()` | `time.sleep(delay)`，子类声明 `rate_limit_delay` |
| `_add_metadata(df, batch_id, data_type)` | 加 4 列元数据：`ingested_at, source_system, source_api, batch_id` |
| `_batch_id(data_type)` | `{source}_{type}_{YYYYMMDD_HHMMSS}` |
| `_write(df, table, pk=None)` | `db.create_table` → pk 去重 → `db.execute("INSERT ...")` |
| `_progress(n, total, avg_ms)` | 进度日志字符串 |

> `_load_stocks` 于 2026-06-20 修复：原 SQL 只按 `code LIKE '00%'` 过滤，导致上证 `sh000001` 等 200 只指数被误纳入。现加入 `market` 条件，正确过滤。

---

## IngestResult

```python
@dataclass
class IngestResult:
    source: str          # "tdx" / "tencent"
    table: str           # 目标 Bronze 表
    status: str          # "ok" / "partial" / "fail" / "empty"
    rows: int            # 写入行数
    total: int           # 尝试股票数
    failed: int          # 失败股票数
    batch_id: str        # 批次号
    elapsed_ms: int      # 总耗时
    errors: list[str]    # 失败的代码及原因
```

---

## 调度策略

每日管线三档**串行**运行（前一个完成后再起下一个，禁止并发，避免同时向 TDX/Tencent 服务器请求触发限频。DB 层已有写优先读者写者锁，并发写表不会冲突）：

### 1. 收盘快照（约 15:05）— `fast_close`

**目标：抢时效**。批量请求，秒级完成，供下游做当日快照、截面因子等时效计算。

| 方法 | 批量能力 | 预估耗时 |
|------|---------|---------|
| Tencent `get_quotes()` | 800 只/批 × 7 批 | ~1s |
| Tencent `get_brief()` | 同上 | ~1s |
| TDX `get_quotes()` | 100 只/批 × 52 批 | ~10s |

### 2. 日线增量（约 18:00）— `light_daily`

**目标：保日线**。stocks + 双源日线增量，确保当日 K 线入库。

### 3. 深夜全量（约 20:00+）— `night_full`

**目标：保完整**。全市场逐只遍历 + 自动补拉 + 去重。

| 方法 | 调度频率 | 原因 |
|------|---------|------|
| TDX `get_stocks()` | 每日 | 新股/退市/更名 |
| TDX `get_kline("daily")` + Tencent `get_kline("day","")` | 每日 | 日线增量 |
| TDX `get_kline("1m")` | 每日 | 分钟线增量 |
| TDX `get_index_kline("daily")` | 每日 | 指数日线 |
| Tencent `get_minute()` | 每日 | 当日分时（交易时段内有效） |
| TDX `get_xdxr()` | 每周 | 除权除息变更频率低 |
| TDX `get_block()` | 每周 | 板块成分变更频率低 |
| TDX `get_finance()` | 财报季后 | 财务数据按季更新 |

> `light_daily` 和 `night_full` 同日运行会产生重复增量数据，`night_full` 末尾自动执行 `dedup()` 去除。

> `get_kline(freq)` 只拉当天最新 1 根（`offset=1`），用于每日增量。全量历史回填用 `get_kline_full()` 单独处理，不纳入每日调度。

### 补拉策略

`night_full` 主流程结束后自动执行 `_auto_heal`：
1. 调用 `diagnose()` 扫描今日数据完整性
2. 逐只类型表：按缺失 codes 补拉（自动处理 TDX/Tencent 代码格式转换）
3. 全量类型表：缺则重跑整表
4. 补拉完成后 `dedup()` 去重

手动补拉可通过 `retry` action + codes 实现。

| 数据类型 | 容忍度 | 策略 |
|----------|--------|------|
| 日线、股票列表 | 零容忍 | 失败必补 |
| 分钟线 | 低容忍 | 失败率 > 1% 时补拉 |
| 资金流向、分时 | 可容忍少量 | 仅记录日志，不自动补 |
| 除权除息、板块、财务 | 低容忍 | 失败必补 |

---

## 已知缺口

| 事项 | 说明 |
|------|------|
| TDX F10 (`get_f10`) | 已实现。非结构化文本 → 文件系统，DB 存指针 |
| TDX 历史分时 (`minutes`) | `fetch/tdx.py` 已有，支持按 YYYYMMDD 查询。因与 bars 分钟线重叠且列少，暂不封装 |
| TDX 分笔成交 (`transaction`/`transactions`) | 数据量极大，按需拉取，不纳入每日调度 |
| `_loop_fetch` 代码重复 | TDXBronze 和 TencentBronze 各有一份，可提入 BaseBronze |
| diagnose/dedup 时效性 | 基于 `ingested_at = today` 判断，仅在当日数据拉取后有意义。未拉数据前跑 diagnose 会误报全缺。night_full 内置 auto_heal 是先拉后诊断，不存在此问题 |
| 过夜风险 | night_full 正常 2 小时内跑完，不会跨日。若极端情况跨日，后半段数据 ingested_at 为次日，当日 diagnose 会漏检。建议确保 night_full 当天完成 |
| 指定日期补拉 | TDX API 为 start+offset 模式，不支持按日期范围查询。补指定历史日期需计算交易日偏移量（需交易日历），暂不支持 |

---

## 编排层示例

```python
from bronze import Bronze, IngestOrder

bronze = Bronze()

# 每日调度（建议 crontab 依次执行）
# 15:05 收盘快照
results = list(bronze.execute(IngestOrder(action="fast_close")))
# 18:00 日线增量
results = list(bronze.execute(IngestOrder(action="light_daily")))
# 20:00+ 深夜全量（含自动补拉 + 去重）
results = list(bronze.execute(IngestOrder(action="night_full")))

# 初次部署
results = list(bronze.execute(IngestOrder(action="first_init")))

# 刷新基础数据
results = list(bronze.execute(IngestOrder(action="refresh_v2")))

# 检查今日数据完整性
print(bronze.diagnose())

# 手动去重
bronze.dedup()

# 补拉指定股票全量历史（从 IPO 日至今，追加去重）
list(bronze.execute(IngestOrder(action="backfill", codes=["000010", "000007"])))

# F10 资料详情（同 code+hash 去重）
list(bronze.execute(IngestOrder(action="f10", codes=["000010", "000007"])))

# 查看表结构
print(bronze.status())

# 手动发指令
from bronze import TDXBronze, TencentBronze

tdx = TDXBronze()
tc  = TencentBronze()

tdx.get_kline("daily")
tdx.get_xdxr()
tc.get_money_flow()
```
