# CODEBUDDY.md — 项目上下文

## 项目概述

A股量化数据仓库，Medallion 架构（Bronze → Silver → Gold），DuckDB 单文件存储。

## 目录结构

```
qt/
├── bronze/          # Bronze 层摄入引擎
│   ├── instruction.py   # 调度中枢，6 action + diagnose/dedup/auto_heal
│   ├── tdx.py           # TDX 执行层（日线/分钟/xdxr/finance/F10/板块/指数）
│   ├── tencent.py       # 腾讯执行层（日线/分钟）
│   └── base.py          # 基类，IngestResult/IngestOrder 协议
├── silver/          # Silver 层清洗引擎
│   ├── stock_map.py     # 股票代码映射表（stock_code_id → stock_code/name/market/ipo_date）
│   ├── daily_kline.py   # 日线清洗（TDX 主 + 腾讯补缺）
│   ├── adj_factor.py    # 复权因子（从 xdxr + pre_close 计算 single_factor）
│   ├── minute_kline.py  # 分钟线清洗（按年分表 minute_kline_{year}）
│   ├── finance.py       # 财务数据清洗（33 个财务字段）
│   └── f10_doc.py       # F10 HTML 文档索引
├── serve/           # Serve 层 — 历史数据读取服务（端口 8000）
│   ├── queries.py       # 查询逻辑（纯函数：参数 → DataFrame），读 Silver 表
│   └── api.py           # FastAPI REST 传输层，调 queries.py
├── realtime/        # Realtime 层 — 盘中实时行情服务（端口 8888）
│   ├── feed.py          # 行情逻辑（轮询 + 内存 DataFrame + 订阅回调）
│   └── api.py           # FastAPI REST + WebSocket 传输层
├── fetch/           # 纯接口层，不接触数据库
│   ├── tdx.py           # mootdx TCP 封装
│   └── tencent.py       # 腾讯 HTTP 封装
├── db/              # DuckDB 读写管理器
│   └── manager.py       # 写优先 + 读并发 + 写排队 + 单例
├── heatmap.py       # 行业热力图服务（端口 9999）
├── test/            # 测试脚本
└── Data.duckdb      # 数据库文件（schemas: bronze/silver/gold）
```

## Bronze 层调度协议

```python
from bronze.instruction import IngestOrder, Bronze

brz = Bronze()
# 套餐式
result = brz.execute(IngestOrder(action="fast_close"))
result = brz.execute(IngestOrder(action="daily"))      # 内含 auto_heal
result = brz.execute(IngestOrder(action="weekly"))      # xdxr + block + finance

# 单只补拉（按表精确补拉）
result = brz.execute(IngestOrder(action="retry", codes=["600000", "sz000001"]))
result = brz.execute(IngestOrder(action="retry.kline_daily", codes=["600000"]))
result = brz.execute(IngestOrder(action="retry.xdxr", codes=["600000"]))

# 全量回填
result = brz.execute(IngestOrder(action="backfill_daily", codes=["600000"]))

# 运维
report = brz.diagnose()   # 返回 DataFrame 健康报告
brz.dedup()                # 去重
```

action 列表：
- 日频：`fast_close` / `daily` / `retry` / `retry.*`
- 周频：`weekly`（xdxr + block + finance）
- 一次性：`first_init` / `backfill_daily` / `f10` / `refresh_v2`

## 调度顺序（串行，不可并发）

```
fast_close → daily
```

两个日频任务必须串行，原因是 API 限频。weekly 独立运行，不与日频冲突。

## 关键设计决策

- **执行层自闭合**：TDXBronze/TencentBronze 的每个 get_* 方法独立完成 fetch → write → 返回 IngestResult，调用方不需要关心写入逻辑
- **线程 + Queue 并行**：daily/weekly/first_init 等套餐内开线程并行拉多源，Queue 收集结果
- **stocks 列表先行**：所有需要股票列表的 action 必须在主线程 yield get_stocks() 后再开并行线程，避免竞态
- **xdxr 不用 _loop_fetch**：自含循环，便于插入 code 列；其他逐只方法仍用 _loop_fetch
- **F10 去重**：DB-aware，先加载已有 code+hash，循环内跳过
- **diagnose 基于 ingested_at=today**：未拉数据前跑会误报全缺
- **retry 按表路由**：action 格式 `retry.kline_daily` / `retry.xdxr` 等，空则只拉日线+quotes

## 已知限制

- diagnose 只能诊断当天数据（基于 ingested_at 日期），无法指定历史日期
- 指定日期补拉需交易日历计算偏移，暂不支持
- TDX 分钟线历史深度仅 5-6 个月
- xdxr/finance 每次全量重插（效率问题，非 bug）
- 过夜运行时 ingested_at 跨日会导致 diagnose 漏检

## DB 层

写优先读写锁（db/manager.py）：_pending_writes 计数器 + Event 握手，无后台线程，全程 Event 阻塞等待。

## Serve 层

历史数据读取服务，数仓的**读端封装**，与 fetch 层（写端）对称。

```
外部应用 → serve/queries.py（本机 import）→ DuckDB Silver 表
外部应用 → serve/api.py（HTTP 8000）→ queries.py → DuckDB
```

### queries.py — 7 个查询函数

| 函数 | 读的表 | 说明 |
|---|---|---|
| `get_stock_list(date)` | silver.stock_map | 股票列表，date 过滤 IPO/退市 |
| `get_stock_info(code)` | silver.stock_map | 单只股票信息，code 支持 sh/sz 前缀 |
| `get_daily_kline(code, start, end)` | silver.daily_kline | 单只日线，当前仅 bfq |
| `get_daily_kline_batch(codes, start, end)` | silver.daily_kline | 批量日线 |
| `get_minute_kline(code, date)` | silver.minute_kline_{year} | 分钟线，year 自动推导 |
| `get_adj_factor(code, start, end)` | silver.adj_factor | 复权因子原始数据 |
| `get_finance(code)` | silver.finance | 财务数据 |
| `get_f10(code)` | silver.f10_doc + HTML 文件 | 最新 F10 HTML 内容 |
| `get_tables(schema)` | 全部 schema | 列出数据库表，可按 schema 过滤 |
| `get_table_schema(table_name)` | 指定表 | 查询单表列名、类型、主键 |

### api.py — REST 路由

```
GET /api/stock_list?date=2024-01-01
GET /api/stock_info?code=600519
GET /api/daily_kline?code=600519&start=2025-01-01&end=2025-06-30
GET /api/daily_kline/batch?codes=600519,000001&start=...&end=...
GET /api/minute_kline?code=600519&date=2025-06-03
GET /api/adj_factor?code=600519&start=...&end=...
GET /api/finance?code=600519
GET /api/f10?code=600519
GET /api/tables?schema=silver
GET /api/table_schema?table=silver.daily_kline
```

鉴权：`QUANTDB_API_KEYS` 环境变量（逗号分隔），未配置则不鉴权。与 realtime 共用。

### 设计决策

- **逻辑 + 传输分离**：queries.py 纯逻辑，api.py 纯传输，改逻辑不影响 API，加传输不改逻辑
- **基于 Silver 表**：当前直接读 Silver，Gold 层完成后只需改 queries.py 的 SQL
- **不做复权**：第一版 `fq` 参数仅支持 "bfq"，复权计算后续加
- **code 兼容**：queries.py 内部统一纯数字，api.py 入口兼容 sh/sz 前缀

## Realtime 层

盘中实时行情服务，绕过数据库，直连 fetch 层。

```
外部应用 → realtime/feed.py（本机 import）→ fetch.tencent.quotes() → 腾讯 API
外部应用 → realtime/api.py（HTTP 8888 + WS）→ feed.py → 腾讯 API
```

### feed.py 核心设计

- **内存常驻 DataFrame**：后台线程 0.3s 轮询全市场，拉到新数据替换 `_df`
- **REST 查询**：客户端 GET 时直接读内存 `_df`，零延迟
- **WebSocket 推送**：每轮轮询后触发订阅者回调，通过 asyncio.Queue 桥接线程→asyncio
- **单例模式**：全局一个 Feed 实例，一个轮询线程

### 关键实现细节

- `_to_plain()`：腾讯返回 code 为纯数字 "000001"，输入兼容 "sz000001"
- `ws_max_size=None`：全市场 5200+ 股票 × 87 列约 1.7MB，需关闭 uvicorn 默认 1MB 限制
- `_is_market_open()`：交易时段 9:25-11:35 / 12:55-15:05，宽 5 分钟余量

## Heatmap 服务

独立热力图页面（端口 9999），后端合并行业分类 + 实时行情推给前端。

- 行业分类：baostock 拉取后缓存为 `data/industry.csv`
- 行情数据：订阅 Feed 回调，`_build_heatmap_data()` 合并为 `{行业: [{name, pct}, ...]}` 结构
- WebSocket：heatmap 自开 `/ws/heatmap` 端点，不依赖 realtime 8888
- 布局：CSS columns masonry，5 档硬分大小（8%+/6-8%/4-6%/2-4%/0-2%）

## 编码风格

- 中文注释为主，变量/函数英文
- log 用 `_log = logging.getLogger(__name__)`
- 不修改共享工具函数（如 _loop_fetch）来满足单个调用方需求，改在调用方自行实现
