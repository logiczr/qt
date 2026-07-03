# Silver 层

Medallion 架构第二层：从 Bronze 原始数据构建清洗后的分析就绪表。

## 核心协议

```
BuildOrder (编排层 → Silver)     BuildResult (Silver → 编排层)
┌─────────────────────┐         ┌─────────────────────┐
│ target: str         │         │ target: str         │
│ mode: full|incr|rep │  ──→    │ status: ok|partial  │
│ date: str           │         │ rows_read/written   │
└─────────────────────┘         │ errors / warnings   │
                                └─────────────────────┘
```

## 已实现 Builder

### stock_map

股票主表，A 股代码+名称+市场+IPO日期。

| 列 | 类型 | 说明 |
|---|---|---|
| stock_code_id | SMALLINT | 自增序号 PK |
| stock_code | VARCHAR | 股票代码，如 '000001' |
| stock_name | VARCHAR | 股票名称 |
| market | VARCHAR | 'sh' / 'sz' |
| ipo_date | DATE | 上市日期（来自 finance 表） |
| delist_date | DATE | 退市日期（暂空） |

Bronze 来源：raw_tdx_stocks + raw_tdx_finance，按 code 合并。

### daily_kline

日线行情，TDX 优先 + 腾讯补缺。

| 列 | 类型 | 说明 |
|---|---|---|
| stock_code | VARCHAR | 股票代码 |
| trade_date | TIMESTAMP | 交易日期 (PK) |
| open | DOUBLE | 开盘价 |
| high | DOUBLE | 最高价 |
| low | DOUBLE | 最低价 |
| close | DOUBLE | 收盘价 |
| pre_close | DOUBLE | 前收盘价 (groupby shift) |
| volume | BIGINT | 成交量（股） |
| amount | DOUBLE | 成交额（TDX有值，腾讯NaN） |
| change_pct | DOUBLE | 涨跌幅 % |

主键：`(stock_code, trade_date)`，增量写入用 `ON CONFLICT DO NOTHING` 去重。

Bronze 来源：
- 主源：raw_tdx_kline_daily_bfq
- 备源：raw_tencent_kline_day_bfq（volume ×100 手→股，amount 全空）

清洗规则：
- volume 垃圾极小值(≈0)归零，转 BIGINT
- 质量门：close > 0 且 high >= low
- pre_close = groupby(stock_code).close.shift(1)
- change_pct = (close - pre_close) / pre_close * 100

### adj_factor

复权因子表，仅维护事件→因子映射，不负责复权计算。

| 列 | 类型 | 说明 |
|---|---|---|
| stock_code | VARCHAR | 股票代码 PK |
| trade_date | DATE | 除权除息日 PK |
| fenhong | DOUBLE | 每股分红（元，已从每10股转换） |
| peigu_price | DOUBLE | 配股价（元） |
| songzhuangu | DOUBLE | 每股送转股（已从每10股转换） |
| peigu | DOUBLE | 每股配股（已从每10股转换） |
| single_factor | DOUBLE | 单次复权因子 |

主键：`(stock_code, trade_date)`

Bronze 来源：raw_tdx_xdxr（仅 category=1 除权除息）

清洗规则：
- 每10股→每股（÷10）
- 同日多事件合并（分红/送转/配股叠加）
- single_factor = (pre_close - fenhong + peigu_price × peigu) / (pre_close × (1 + songzhuangu + peigu))
- pre_close 从 daily_kline 读取，分批处理（500只/批）

### minute_kline

分钟线行情，按年分表 `silver.minute_kline_{year}`。

| 列 | 类型 | 说明 |
|---|---|---|
| stock_code | VARCHAR | 股票代码 PK |
| datetime | TIMESTAMP | 分钟时间戳 PK |
| open | DOUBLE | 开盘价 |
| high | DOUBLE | 最高价 |
| low | DOUBLE | 最低价 |
| close | DOUBLE | 收盘价 |
| vol | DOUBLE | 成交量（原始值，不做清洗） |
| amount | DOUBLE | 成交额 |

主键：`(stock_code, datetime)`

Bronze 来源：raw_tdx_kline_m1_{year}_bfq（年份路由：从哪年读写入哪年）

清洗规则：
- 删冗余列：volume(=vol)、year/month/day/hour/minute、元数据列
- datetime VARCHAR → TIMESTAMP
- 质量门：close > 0 且 high >= low
- 分批处理：100只/批（分钟线数据量大，控制内存峰值 ~900MB）

增量逻辑：
- 读 Silver 每只股票 MAX(datetime)，从 Bronze 读比之更新的数据
- Silver 中没有的新股，Bronze 数据全部保留
- Silver 表不存在时 fallback 到 full

完整性校验：`check_completeness()` — 基于代理交易日历检查缺失日期 + offset

### finance

财务数据，每只股票最新一期截面。

| 列 | 类型 | 说明 |
|---|---|---|
| stock_code | VARCHAR | 股票代码 PK |
| report_date | DATE | 报告期 PK |
| ipo_date | DATE | 上市日期 |
| liutongguben | DOUBLE | 流通股本 |
| zongguben | DOUBLE | 总股本 |
| guojiagu | DOUBLE | 国家股 |
| faqirenfarengu | DOUBLE | 发起人法人股 |
| farengu | DOUBLE | 法人股 |
| bgu/hgu/zhigonggu | DOUBLE | B股/H股/职工股 |
| zongzichan | DOUBLE | 总资产 |
| liudongzichan | DOUBLE | 流动资产 |
| gudingzichan | DOUBLE | 固定资产 |
| wuxingzichan | DOUBLE | 无形资产 |
| gudongrenshu | DOUBLE | 股东人数 |
| liudongfuzhai | DOUBLE | 流动负债 |
| changqifuzhai | DOUBLE | 长期负债 |
| zibengongjijin | DOUBLE | 资本公积金 |
| jingzichan | DOUBLE | 净资产 |
| zhuyingshouru | DOUBLE | 主营收入 |
| zhuyinglirun | DOUBLE | 主营利润 |
| yingshouzhangkuan | DOUBLE | 应收账款 |
| yingyelirun | DOUBLE | 营业利润 |
| touzishouyu | DOUBLE | 投资收益 |
| jingyingxianjinliu | DOUBLE | 经营现金流 |
| zongxianjinliu | DOUBLE | 总现金流 |
| cunhuo | DOUBLE | 存货 |
| lirunzonghe | DOUBLE | 利润总额 |
| shuihoulirun | DOUBLE | 税后利润 |
| jinglirun | DOUBLE | 净利润 |
| weifenpeilirun | DOUBLE | 未分配利润 |
| meigujingzichan | DOUBLE | 每股净资产 |

主键：`(stock_code, report_date)`

Bronze 来源：raw_tdx_finance

清洗规则：
- 去重：同一 (code, updated_date) 保留最后一条
- 日期转换：updated_date/ipo_date 从 YYYYMMDD 整数 → DATE
- 重命名：code → stock_code, updated_date → report_date
- 删无用列：market/province/industry/baoliu2/元数据列
- incremental = full（TDX finance 每次全量刷新）

### f10_doc

F10 文档，Bronze txt → Silver HTML 转换。与 Bronze 层存储模式一致：DB 存文件路径指针，内容落盘。

| 列 | 类型 | 说明 |
|---|---|---|
| stock_code | VARCHAR | 股票代码 PK |
| trade_date | DATE | 更新日期 PK（从文件名提取） |
| file_path | VARCHAR | HTML 文件路径 |
| file_size | BIGINT | 文件大小(bytes) |
| file_hash | VARCHAR | HTML 文件 SHA256 前16位 |
| source_hash | VARCHAR | Bronze 源 txt 的 file_hash（去重依据） |
| ingested_at | TIMESTAMP | 构建时间 |
| source_system | VARCHAR | 'tdx' |

主键：`(stock_code, trade_date)`

Bronze 来源：raw_tdx_f10（元数据指针表，指向 data/bronze/f10/ 下的 txt 文件）

转换逻辑：
- 读取 Bronze txt → 解析制表符表格 + 文本段落 → 渲染 HTML
- 16 个板块按 `=== xxx ===` 切分 → `<section>`，子板块按 `【N.标题】` → `<h2>`
- box-drawing 字符表格（┌─┬─┐）→ `<table>`，跨行自动合并
- dash 分隔表格（─────）→ `<pre class="dash-table">` 保留原始对齐
- 公告分隔块（───┬───）→ `<div class="announcement">`，正文在 ┴ 后收集
- URL 自动转为超链接 `<a href target="_blank">`
- HTML 文件落盘 data/silver/f10/{code}_{YYYYMMDD}.html

增量逻辑：
- 比较 source_hash 与 Bronze 源 hash，源文件未变则跳过

## 用法

```python
from db import configure, get_db
from silver import Silver, BuildOrder

configure('./Data.duckdb')

sv = Silver()

# 全量重建
for r in sv.execute(BuildOrder(target="daily_kline", mode="full")):
    print(r)

# 增量
for r in sv.execute(BuildOrder(target="all", mode="incremental")):
    print(r)

# 补数据
for r in sv.execute(BuildOrder(target="finance", mode="repair", codes=["000001"])):
    print(r)
```

## 文件结构

```
silver/
├── base.py          # BuildOrder / BuildResult / QualityIssue / BaseBuilder
├── silver.py        # Silver 调度中枢，路由 target → Builder
├── stock_map.py     # StockMapBuilder
├── daily_kline.py   # DailyKlineBuilder
├── adj_factor.py    # AdjFactorBuilder
├── minute_kline.py  # MinuteKlineBuilder（按年分表）
├── finance.py       # FinanceBuilder
├── f10_doc.py       # F10DocBuilder（TXT → HTML）
└── __init__.py      # 公开导出
```

## Builder 汇总

| # | target | 说明 | Bronze 来源 | 模式 |
|---|--------|------|------------|------|
| 1 | stock_map | 股票主表 | raw_tdx_stocks + raw_tdx_finance | full/repair |
| 2 | daily_kline | 日线行情 | raw_tdx_kline_daily_bfq + raw_tencent_kline_day_bfq | full/incr/repair |
| 3 | adj_factor | 复权因子 | raw_tdx_xdxr | full/incr/repair |
| 4 | minute_kline | 分钟线（按年分表） | raw_tdx_kline_m1_{year}_bfq | full/incr/repair |
| 5 | finance | 财务数据 | raw_tdx_finance | full(=incr)/repair |
| 6 | f10_doc | F10 文档 HTML | raw_tdx_f10（文件指针） | full/incr/repair |

## 待实现

| # | target | 说明 | 依赖 | 优先级 |
|---|--------|------|------|--------|
| 7 | qfq_daily | 前复权日线 | daily_kline + adj_factor | P0 |
| 8 | hfq_daily | 后复权日线 | daily_kline + adj_factor | P0 |
| 9 | daily_basic | 每日基本面截面（PE/PB/市值等） | baostock（待接入） | P1 |

### 不在范围

| 表 | 原因 |
|----|------|
| money_flow | 腾讯 API 已不返回资金流向，废弃 |
| suspend | 暂无稳定数据源 |
| trade_cal | 暂无数据源，可后续补 |

## 完整性检查

### check_completeness

依据 IPO 日期 + 代理交易日历（600000/000001 的交易日并集）检查日线数据是否完整。

```python
from silver.daily_kline import DailyKlineBuilder

b = DailyKlineBuilder()

# 全市场检查
report = b.check_completeness()

# 指定股票
report = b.check_completeness(codes=["600000", "000001"])
```

返回 DataFrame：`stock_code, ipo_date, expected_days, actual_days, missing_count, missing_dates`

已知局限：
- 暂无独立交易日历，以参考股交易日为代理
- 无法区分"数据缺失"与"停牌"，停牌日也会被标记为缺失

### Ingest 层补数据实践

完整性检查返回的缺失个股，应触发自动补拉流程：

```
check_completeness(缺失个股)
    ↓ 返回 codes=["000010", "000007", ...]
    ↓
Bronze 层：backfill 补拉这些个股（从上市第一日至今，追加去重写入）
    ↓ 数据入库
    ↓
Silver 层：repair 清洗这些个股并写入（ON CONFLICT DO NOTHING）
    ↓
再次 check_completeness(codes=[...]) 验证
```

各环节已全部就绪：

| 环节 | 调用方式 |
|------|---------|
| 完整性检查 | `DailyKlineBuilder().check_completeness(codes=[...])` |
| Bronze 补拉 | `IngestOrder(action="backfill", codes=[...])` |
| Silver 清洗 | `BuildOrder(target="daily_kline", mode="repair", codes=[...])` |
| 验证 | 再次调用 `check_completeness(codes=[...])` |

### Best Practice：每日管线编排

```python
from db import configure
from bronze import Bronze, IngestOrder
from silver import Silver, BuildOrder
from silver.daily_kline import DailyKlineBuilder

configure('./Data.duckdb')

# ---- 1. Bronze 层：数据摄入 ----

bronze = Bronze()

# 收盘快照（15:05）
list(bronze.execute(IngestOrder(action="fast_close")))

# 日线增量（18:00）
list(bronze.execute(IngestOrder(action="light_daily")))

# 深夜全量（20:00+，含 auto_heal + dedup）
list(bronze.execute(IngestOrder(action="night_full")))

# ---- 2. Silver 层：清洗构建 ----

sv = Silver()

# 增量清洗当日数据
for r in sv.execute(BuildOrder(target="all", mode="incremental")):
    print(r)

# ---- 3. 完整性检查 + 自动补数据 ----

b = DailyKlineBuilder()
report = b.check_completeness()

# 筛出有缺失的股票（排除无 ipo_date 无法审查的）
missing_codes = report[report["missing_count"].notna()]["stock_code"].tolist()

if missing_codes:
    print(f"发现 {len(missing_codes)} 只股票日线缺失，开始补数据...")

    # 3a. Bronze 补拉
    list(bronze.execute(IngestOrder(action="backfill", codes=missing_codes)))

    # 3b. Silver repair
    for r in sv.execute(BuildOrder(
        target="daily_kline", mode="repair", codes=missing_codes
    )):
        print(r)

    # 3c. 验证
    report2 = b.check_completeness(codes=missing_codes)
    still_missing = report2[report2["missing_count"].notna()]
    if still_missing.empty:
        print("补数据完成，所有股票日线完整")
    else:
        print(f"仍有 {len(still_missing)} 只股票缺失（可能是停牌）")
```

### 事件驱动架构（设计草案）

当前编排是过程式的（先 A 再 B 再 C），耦合在调用方。长期应往事件驱动演化：任务完成即发事件，下游订阅自动触发。

#### 事件定义

```python
@dataclass
class PipelineEvent:
    topic: str           # 事件主题
    source: str          # 发射方：bronze / silver / gold
    target: str          # 相关表：daily_kline / stock_map / ...
    data: dict           # 事件负载
```

#### 事件流

```
Bronze IngestResult
  │  topic: "bronze.ingest.complete"
  │  data: {action, table, status, rows, missing_codes}
  │
  ├─→ Silver Builder 自动触发清洗
  │     订阅：topic="bronze.ingest.complete" → BuildOrder(mode="incremental")
  │
  ├─→ Silver BuildResult 发射事件
  │     topic: "silver.build.complete"
  │     data: {target, status, rows_written, issues}
  │
  │     ├─→ 完整性检查自动触发
  │     │     订阅：topic="silver.build.complete" AND target="daily_kline"
  │     │     → check_completeness() → 有缺失则发射 "silver.completeness.gap"
  │     │
  │     ├─→ Bronze backfill 自动触发
  │     │     订阅：topic="silver.completeness.gap"
  │     │     → IngestOrder(action="backfill", codes=...)
  │     │
  │     ├─→ Silver repair 自动触发
  │     │     订阅：topic="bronze.backfill.complete" AND codes in event
  │     │     → BuildOrder(mode="repair", codes=...)
  │     │
  │     └─→ Gold 层订阅（未来）
  │           订阅：topic="silver.build.complete" AND target="daily_kline"
  │           → Gold 层因子计算、策略回测等
  │
  └─→ Bronze auto_heal 订阅
        订阅：topic="bronze.ingest.complete" AND status="partial"
        → IngestOrder(action="retry", codes=failed_codes)
```

#### 实施路径

1. **当前**：过程式编排，手动串行调用
2. **下一步**：实现 EventBus + 关键事件定义，Bronze/Silver 层内 emit
3. **远期**：Gold 层订阅 Silver 事件，策略/回测模块按需接入
