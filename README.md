# A股量化数据仓库

Medallion 架构（Bronze → Silver → Gold），DuckDB 单文件存储，FastAPI 服务层。

---

## 技术栈

| 层 | 技术 | 用途 |
|---|---|---|
| 存储 | DuckDB | 单文件列式数据库，Bronze/Silver/Gold 三层 schema |
| 摄入 | mootdx + requests | TDX TCP 行情源 + 腾讯 HTTP 行情源 |
| 清洗 | pandas + numpy | 数据清洗、类型转换、复权因子计算 |
| 行业 | baostock | 行业分类数据源，CSV 缓存 |
| 服务 | FastAPI + uvicorn | REST API + WebSocket，三个独立服务 |
| 调度 | APScheduler | Cron 定时触发 Pipeline |
| 语言 | Python 3.10+ | 全栈 |

---

## 项目结构

```
qt/
├── bronze/              # Bronze 层摄入引擎
│   ├── instruction.py   # 调度中枢，6 action + diagnose/dedup/auto_heal
│   ├── tdx.py           # TDX 执行层（日线/分钟/xdxr/finance/F10/板块/指数）
│   ├── tencent.py       # 腾讯执行层（日线/分钟）
│   └── base.py          # 基类，IngestResult/IngestOrder 协议
├── silver/              # Silver 层清洗引擎
│   ├── stock_map.py     # 股票代码映射表
│   ├── daily_kline.py   # 日线清洗（TDX 主 + 腾讯补缺）
│   ├── adj_factor.py    # 复权因子（从 xdxr + pre_close 计算 single_factor）
│   ├── minute_kline.py  # 分钟线清洗（按年分表 minute_kline_{year}）
│   ├── finance.py       # 财务数据清洗（33 个财务字段）
│   └── f10_doc.py       # F10 HTML 文档索引
├── serve/               # Serve 层 — 历史数据读取服务（端口 8000）
│   ├── queries.py       # 查询逻辑（纯函数：参数 → DataFrame），读 Silver 表
│   └── api.py           # FastAPI REST 传输层
├── realtime/            # Realtime 层 — 盘中实时行情服务（端口 8888）
│   ├── feed.py          # 行情逻辑（轮询 + 内存 DataFrame + 订阅回调）
│   └── api.py           # FastAPI REST + WebSocket 传输层
├── ingest/              # Ingest 编排层（Pipeline + Dealer）
│   ├── pipeline.py      # 执行器 + 定时调度 + DB 持久化
│   └── dealer.py        # 决策器：看结果决定下一步
├── fetch/               # 纯接口层，不接触数据库
│   ├── tdx.py           # mootdx TCP 封装
│   └── tencent.py       # 腾讯 HTTP 封装
├── db/                  # DuckDB 读写管理器
│   ├── manager.py       # 写优先 + 读并发 + 写排队 + 单例
│   └── schema.sql       # 三层 schema 初始化
├── heatmap.py           # 行业热力图服务（端口 9999）
├── log.py               # 全局日志配置（控制台 + 文件轮转）
├── main.py              # 主入口（一键启动全部服务）
├── setup.py             # 首次部署脚本
├── Data.duckdb          # 数据库文件
├── data/                # 数据目录（F10 文本/HTML/行业缓存）
├── logs/                # 日志文件（每天轮转，保留 30 天）
└── requirements.txt     # Python 依赖
```

---

## 数据流向

```
外部数据源                    数仓内部                         外部应用
───────────                ──────────                      ─────────

通达信 TCP  ──→ fetch/tdx ──→ Bronze ──→ Silver ──→ serve/queries.py ──→ 本机 import
                                      │                  └→ serve/api.py ──→ HTTP 8000
腾讯 HTTP   ──→ fetch/tencent ──→     │
                                      └→ Gold（未实现）

腾讯 HTTP   ──→ fetch/tencent ──→ realtime/feed.py ──→ realtime/api.py ──→ HTTP 8888 + WS
                                    （绕过数据库，内存常驻）
```

---

## Silver 表结构

### silver.stock_map — 股票代码映射

| 列 | 类型 | 主键 | 说明 |
|---|---|---|---|
| stock_code_id | SMALLINT | PK | 数字 ID |
| stock_code | VARCHAR | UNIQUE | 纯数字代码，如 "600519" |
| stock_name | VARCHAR | | 股票名称 |
| market | VARCHAR | | "sh" / "sz" |
| ipo_date | DATE | | 上市日期 |
| delist_date | DATE | | 退市日期（当前均为 NULL） |

### silver.daily_kline — 日线行情

| 列 | 类型 | 主键 | 说明 |
|---|---|---|---|
| stock_code | VARCHAR | PK | 股票代码 |
| trade_date | TIMESTAMP | PK | 交易日期 |
| open/high/low/close | DOUBLE | | OHLC |
| pre_close | DOUBLE | | 前收盘价 |
| volume | BIGINT | | 成交量（股） |
| amount | DOUBLE | | 成交额 |
| change_pct | DOUBLE | | 涨跌幅（%） |

### silver.adj_factor — 复权因子

| 列 | 类型 | 主键 | 说明 |
|---|---|---|---|
| stock_code | VARCHAR | PK | 股票代码 |
| trade_date | TIMESTAMP | PK | 除权日 |
| fenhong | DOUBLE | | 每股分红（元） |
| peigu_price | DOUBLE | | 配股价（元） |
| songzhuangu | DOUBLE | | 每股送转股 |
| peigu | DOUBLE | | 每股配股 |
| single_factor | DOUBLE | | 单次复权因子 |

### silver.minute_kline_{year} — 分钟线（按年分表）

| 列 | 类型 | 主键 | 说明 |
|---|---|---|---|
| stock_code | VARCHAR | PK | 股票代码 |
| datetime | TIMESTAMP | PK | 分钟时间戳 |
| open/high/low/close | DOUBLE | | OHLC |
| vol | DOUBLE | | 成交量 |
| amount | DOUBLE | | 成交额 |

### silver.finance — 财务数据

| 列 | 类型 | 主键 | 说明 |
|---|---|---|---|
| stock_code | VARCHAR | PK | 股票代码 |
| report_date | DATE | PK | 报告期 |
| ipo_date | DATE | | 上市日期 |
| liutongguben ~ meigujingzichan | DOUBLE | | 33 个财务字段 |

### silver.f10_doc — F10 文档索引

| 列 | 类型 | 主键 | 说明 |
|---|---|---|---|
| stock_code | VARCHAR | PK | 股票代码 |
| trade_date | VARCHAR | PK | 更新日期 |
| file_path | VARCHAR | | HTML 文件路径 |
| file_size | BIGINT | | 文件大小（bytes） |
| file_hash | VARCHAR | | SHA256 前 16 位 |
| source_hash | VARCHAR | | 源文件 hash（去重依据） |

---

## Pipeline 调用链流程

每次触发一个 action，Pipeline 循环执行 Dealer 决策直到完成：

```
trigger(action) → Dealer.start()
     │
     ▼
┌─ Bronze 拉取 ─┐
│  fetch → write │
│  返回 results  │
└───────┬───────┘
        │
        ▼
   Dealer 看结果
        │
        ├── 有失败？ → retry（最多 2 轮，超限则 shelved）
        │                  │
        │                  ▼
        │              retry 成功 → Silver repair（补修失败股票）
        │              retry 仍失败 → 放入 shelved，不再重试
        │
        ├── 有新数据？ → Silver incremental（增量构建）
        │                  │
        │                  ▼
        │              daily_kline 参与了？ → completeness check
        │                                        │
        │                                        ├── 有缺数据 → backfill → Silver repair → 再查一轮
        │                                        └── 无缺数据 → done
        │
        └── 无新数据无失败 → done
```

每一步执行结果写入 `ingest.plan` + `ingest.step` 表，可通过 `GET /api/ingest/plans` 和 `GET /api/ingest/steps` 查询。

---

## 定时调度

| 时间 | Action | 说明 |
|---|---|---|
| 每日 15:05 | `fast_close` | 盘后快照（TDX quotes） |
| 每日 18:00 | `daily` | 日线 + 分钟线 + auto_heal |
| 每周六 10:00 | `weekly` | xdxr + 板块 + 财务 |

`fast_close` 和 `daily` 串行执行（API 限频），`weekly` 独立运行。

### 手动触发

```python
from bronze import Bronze, IngestOrder
brz = Bronze()

# 套餐式
brz.execute(IngestOrder(action="fast_close"))
brz.execute(IngestOrder(action="daily"))
brz.execute(IngestOrder(action="weekly"))

# 单只补拉
brz.execute(IngestOrder(action="retry", codes=["600000", "sz000001"]))
brz.execute(IngestOrder(action="retry.kline_daily", codes=["600000"]))

# 全量回填
brz.execute(IngestOrder(action="backfill_daily", codes=["600000"]))
```

### HTTP 触发全量初始化

```bash
curl -X POST http://127.0.0.1:8000/api/init/hard
```

后台执行：全量日线 + 分钟线拉取 → Silver 全量构建。通过 `/api/ingest/plans` 跟踪进度。

---

## 快速部署

### 1. 安装依赖

```bash
cd qt
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 首次部署

```bash
python setup.py
```

自动完成：创建目录 → 初始化 DB → Bronze 拉取 → Silver 构建 → 启动 serve(8000) + realtime(8888)。

### 3. 日常运行

```bash
python main.py              # 全部：serve + realtime + scheduler
python main.py serve        # 只启动历史数据服务
python main.py realtime     # 只启动实时行情
python main.py scheduler    # 只启动定时调度
```

---

## Docker 部署

```bash
# 构建
docker build -t qt .

# 运行
docker run -d --name qt \
  -p 8000:8000 \
  -p 8888:8888 \
  -v /path/to/data:/app/data \
  -v /path/to/logs:/app/logs \
  qt

# 首次部署（容器内执行）
docker exec qt python setup.py
```

数据目录 `/app/data` 包含 DuckDB 文件和 F10 文档，必须挂载到宿主机持久化。

---

## API 一览

### Serve（端口 8000）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/stock_list?date=2024-01-01` | 股票列表 |
| GET | `/api/stock_info?code=600519` | 单只股票信息 |
| GET | `/api/daily_kline?code=600519&start=...&end=...` | 日线 |
| GET | `/api/daily_kline/batch?codes=600519,000001&start=...&end=...` | 批量日线 |
| GET | `/api/minute_kline?code=600519&date=2025-06-03` | 分钟线 |
| GET | `/api/adj_factor?code=600519&start=...&end=...` | 复权因子 |
| GET | `/api/finance?code=600519` | 财务数据 |
| GET | `/api/f10?code=600519` | F10 文档（最新 HTML） |
| GET | `/api/tables?schema=silver` | 列出数据库表 |
| GET | `/api/table_schema?table=silver.daily_kline` | 查询单表结构 |
| GET | `/api/ingest/plans?limit=50` | ingest 操作记录 |
| GET | `/api/ingest/steps?plan_id=xxx` | ingest 步骤详情 |
| POST | `/api/init/hard` | 全量初始化（后台运行） |

### Realtime（端口 8888）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/quotes?codes=sh600519,sz000001` | 实时行情快照 |
| GET | `/api/quotes/all` | 全市场快照 |
| WS | `/ws/quotes?codes=sh600519` | WebSocket 实时推送 |

### 鉴权

环境变量 `QUANTDB_API_KEYS`（逗号分隔），请求头 `X-API-Key`。未配置则不鉴权。

---

## 日志

- 控制台 + 文件双输出
- 文件路径：`logs/qt.log`
- 每天轮转，保留 30 天
- 初始化：`from log import setup; setup()`（main.py 已集成）

---

## 已知缺陷

- **ingested_at 跨日问题**：过夜运行时 `ingested_at` 日期跨日，导致 `diagnose()` 漏检。diagnose 基于 `ingested_at = today` 检查，凌晨后日期变化会导致当天数据被误判为"缺失"
- **diagnose 只能诊断当天**：无法指定历史日期，隔天验证无效
- **TDX 分钟线历史深度仅 5-6 个月**：更早的分钟数据无法获取
- **xdxr/finance 每次全量重插**：效率问题，非 bug
- **不做复权**：serve 层 `get_daily_kline` 当前仅支持不复权（bfq），前/后复权后续加
- **serve 查缺不触发 ingest**：serve 层只读，查到缺数据不会通知 ingest 补拉，需手动触发
