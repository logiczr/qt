# fetch — 数据源获取模块

> **`[实时]`** = 盘中才有/才有意义的数据。非交易时段 TDX 返回 `None`，腾讯财经返回仅含头行的空壳（如 `v_s······`），均不可用。

## 模块概览

| 模块 | 数据源 | 协议 | 编码 | 限制 |
|------|--------|------|------|------|
| `tdx` | 通达信行情服务器 | TCP (端口 7709) | — | K线单次 ≤800 条，分笔 ≤2000 条 |
| `tencent` | 腾讯财经 HTTP API | HTTP GET | GBK (快照) / JSON (K线/分时) | 建议间隔 ≥100ms，高频可能封 IP |

---

## tdx — 通达信

基于 [mootdx](https://github.com/bopjiang/mootdx) 库，直连通达信行情服务器。

```python
from fetch.tdx import quotes, bars, stocks, k, ...
```

### 实时行情

#### `quotes(symbols, market="std") -> DataFrame | None` `[实时]`

实时行情快照 (5 档买卖盘)，46 列。非交易时段返回 `None`。

| 参数 | 类型 | 说明 |
|------|------|------|
| `symbols` | `str \| list[str]` | 股票代码，如 `'000001'` 或 `['000001', '600519']` |

返回字段：`market` / `code` / `price` / `last_close` / `open` / `high` / `low` / `volume` / `amount` / `bid1~bid5` / `ask1~ask5` 等。

---

### K 线

#### `bars(symbol, frequency, start=0, offset=800, market="std") -> DataFrame | None`

按起始位置获取 K 线，单次最多 800 条。

| 参数 | 类型 | 说明 |
|------|------|------|
| `symbol` | `str` | 股票代码，如 `'000001'` |
| `frequency` | `int` | K线周期，推荐用 `FREQ` 字典取值 |
| `start` | `int` | 起始位置，`0` = 最新 |
| `offset` | `int` | 条数，最大 800 |

返回 13 列：`open` / `close` / `high` / `low` / `vol` / `amount` / `datetime` 等，含 `stock_code` 列。

```python
from fetch.tdx import bars, FREQ
df = bars("000001", FREQ["day"], start=0, offset=10)  # 最近 10 根日 K
```

#### `bars_all(symbol, frequency, max_start=25600, market="std") -> DataFrame | None`

分页拉取全量K线，自动循环 800 条/次，间隔 0.5s。`max_start=25600` ≈ 10 年日线。

#### `bars_batch(symbols, frequency=FREQ["day"], start=0, offset=1, market="std") -> DataFrame | None`

批量获取多只股票 K 线。每只间隔 0.2s。

| 参数 | 类型 | 说明 |
|------|------|------|
| `symbols` | `DataFrame` | 含 `code` 列的股票列表，传入 `stocks()` 返回值即可 |
| `offset` | `int` | 每只取几条，默认 1 (当天) |

#### `k(symbol, begin, end, market="std") -> DataFrame | None`

按日期范围获取日 K 线（不复权），`begin` / `end` 为 `'YYYY-MM-DD'`。仅返回已入库的历史数据，无法取当天。

#### `index_bars(symbol, frequency, start=0, offset=800, market="std") -> DataFrame | None`

指数 K 线，15 列，比个股多 `up_count` / `down_count`。

---

### 分时 & 分笔

#### `minute(symbol, market="std") -> DataFrame | None` `[实时]`

当日分时数据，3 列 (`price` / `vol` / `volume`)。无显式时间列，行索引隐含分钟顺序。

#### `minutes(symbol, date, market="std") -> DataFrame | None`

历史分时数据，`date` 格式 `'YYYYMMDD'`。

#### `transaction(symbol, start=0, offset=2000, market="std") -> DataFrame | None` `[实时]`

当日分笔成交。**交易时段外返回空。**

#### `transactions(symbol, start=0, offset=2000, date="", market="std") -> DataFrame | None`

历史分笔成交，5 列 (`time` / `price` / `vol` / `buyorsell` / `volume`)。`date` 格式 `'YYYYMMDD'`。最多 2000 条。

---

### 基础信息

#### `stocks(market="std") -> DataFrame | None`

沪深两市股票列表。返回 `code` / `name` / `pre_close` / `market`（`'sz'` / `'sh'`）。

#### `xdxr(symbol, market="std") -> DataFrame | None`

除权除息数据，16 列。`category` 字段：1=除权除息，2=送配股上市，5=股本变化。

#### `finance(symbol, market="std") -> DataFrame | None`

在线财务数据，shape (1, 37)，仅最新一期汇总截面。

#### `f10c(symbol, market="std") -> list[dict] | None`

F10 资料目录，返回 `list[dict]`。

#### `f10(symbol, name="", market="std") -> DataFrame | str | None`

F10 资料详情（如股东、高管等信息），返回 `DataFrame` 或原始文本。

#### `block(tofile="block.dat", market="std") -> DataFrame | None`

板块-成分股映射，4 列 (`blockname` / `block_type` / `code_index` / `code`)，约 38000 行。

---

### 周期常量 `FREQ`

```python
from fetch.tdx import FREQ

FREQ["1m"]       # 8
FREQ["5m"]       # 0
FREQ["15m"]      # 1
FREQ["30m"]      # 2
FREQ["60m"]      # 3
FREQ["day"]      # 4
FREQ["week"]     # 5
FREQ["month"]    # 6
FREQ["quarter"]  # 10
FREQ["year"]     # 11
```

---

## tencent — 腾讯财经

基于 HTTP GET 协议。K线/分时返回 JSON，快照类返回 GBK 编码 `~` 分隔文本。

**代码格式**：`sh` 前缀 = 上海，`sz` 前缀 = 深圳。如 `sh600519`、`sz000001`。

```python
from fetch.tencent import quotes, bars, minute, money_flow, order_book, brief
```

### 实时行情

#### `quotes(codes, delay=0.15) -> DataFrame | None` `[实时]`

实时行情快照 (5 档买卖盘 + PE/PB/市值)，87 列。非交易时段返回仅含头行的空壳（如 `v_sh600519="1~···`）。

| 参数 | 类型 | 说明 |
|------|------|------|
| `codes` | `str \| list[str]` | 代码，如 `'sh600519'` 或 `['sh600519', 'sz000001']` |
| `delay` | `float` | 批次间隔 (秒)，默认 150ms |

每批最多 10 只，超过自动分批。返回原始 `~` 分隔值，不做类型转换。

---

### K 线

#### `bars(code, ktype="day", fq="qfq", start="", end="", count=250) -> DataFrame | None`

K 线数据 (日/周/月)。

| 参数 | 类型 | 说明 |
|------|------|------|
| `code` | `str` | 股票代码，如 `'sh600519'` |
| `ktype` | `str` | `'day'` / `'week'` / `'month'` |
| `fq` | `str` | `'qfq'`=前复权 / `'hfq'`=后复权 / `''`=不复权 |
| `start` | `str` | 开始日期 `'YYYY-MM-DD'`，留空按 count 取 |
| `end` | `str` | 结束日期 `'YYYY-MM-DD'`，留空到最新 |
| `count` | `int` | 返回条数 |

返回 `date` / `open` / `close` / `high` / `low` / `volume` / `amount` / `stock_code`。若某行为除权除息日，额外含 `extra` 列（原始 JSON 对象）。

---

### 分时数据

#### `minute(code) -> DataFrame | None` `[实时]`

当日分时数据，5 列 (`time` / `price` / `volume` / `amount` / `stock_code`)。`volume` 和 `amount` 为累计值。

---

### 资金 & 盘口

#### `money_flow(code) -> DataFrame | None` `[实时]`

资金流向，15 列。返回字段：`main_in` / `main_out` / `main_net` / `retail_in` / `retail_out` 等。

注意：部分股票可能不支持，返回 `None`。

#### `order_book(code) -> DataFrame | None` `[实时]`

盘口分析 (大小单统计)，5 列。返回 `buy_big` / `buy_small` / `sell_big` / `sell_small`。

---

### 轻量信息

#### `brief(code) -> DataFrame | None` `[实时]`

简要快照，仅约 12 字段。比完整 `quotes()` 轻量，适合列表/概览场景。非交易时段行为同 `quotes`。
