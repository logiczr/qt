# Serve 层 — 历史数据读取服务

数仓的读端封装，外部应用不直接写 SQL、不直接访问 DuckDB。

## 架构

```
逻辑层 + 传输层分离

外部应用 ──本机 import──→ queries.py ──→ DuckDB Silver 表
外部应用 ──HTTP 8000──→ api.py ──→ queries.py ──→ DuckDB
```

- `queries.py`：纯函数，参数 → DataFrame，不含 HTTP 逻辑
- `api.py`：FastAPI 薄壳，参数解析 + 鉴权 + 返回 JSON，不含查询逻辑

改查询只改 queries.py，API 自动同步；加传输方式不改 queries.py。

## 本机调用

```python
from db import configure
configure(db_path="Data.duckdb")

from serve.queries import get_daily_kline, get_stock_list

# 股票列表
df = get_stock_list(date="2025-01-01")

# 日线
df = get_daily_kline("600519", start="2025-01-01", end="2025-06-30")

# code 兼容 sh/sz 前缀
df = get_daily_kline("sh600519", start="2025-01-01", end="2025-06-30")
```

## HTTP API

启动：`python -m serve.api`，默认端口 8000。

### 路由

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/stock_list?date=2024-01-01` | 股票列表，date 可选 |
| GET | `/api/stock_info?code=600519` | 单只股票信息 |
| GET | `/api/daily_kline?code=600519&start=2025-01-01&end=2025-06-30` | 单只日线 |
| GET | `/api/daily_kline/batch?codes=600519,000001&start=...&end=...` | 批量日线 |
| GET | `/api/minute_kline?code=600519&date=2025-06-03` | 分钟线 |
| GET | `/api/adj_factor?code=600519&start=2025-01-01&end=2025-06-30` | 复权因子 |
| GET | `/api/finance?code=600519` | 财务数据 |
| GET | `/api/f10?code=600519` | F10 文档（最新 HTML） |
| GET | `/api/tables?schema=silver` | 列出数据库表，schema 可选 |
| GET | `/api/table_schema?table=silver.daily_kline` | 查询单表结构 |
| GET | `/api/ingest/plans?limit=50` | ingest 操作记录 |
| GET | `/api/ingest/steps?plan_id=xxx` | ingest 步骤详情 |
| POST | `/api/init/hard` | 全量初始化（后台运行） |

### 鉴权

环境变量 `QUANTDB_API_KEYS`（逗号分隔），未配置则不鉴权。

```bash
# 不鉴权
python -m serve.api

# 鉴权
QUANTDB_API_KEYS=key1,key2 python -m serve.api
```

请求时带 Header：`X-API-Key: key1`

### 示例

```bash
# 茅台日线
curl "http://127.0.0.1:8000/api/daily_kline?code=600519&start=2025-06-01&end=2025-06-30"

# 股票列表
curl "http://127.0.0.1:8000/api/stock_list?date=2025-01-01"

# 批量日线
curl "http://127.0.0.1:8000/api/daily_kline/batch?codes=600519,000001&start=2025-06-01&end=2025-06-30"

# 带鉴权
curl -H "X-API-Key: mykey" "http://127.0.0.1:8000/api/finance?code=600519"
```

## 数据源

当前直接读 Silver 表，Gold 层完成后只需改 queries.py 的 SQL，api.py 不动。

| 查询函数 | Silver 表 |
|---|---|
| get_stock_list / get_stock_info | silver.stock_map |
| get_daily_kline / get_daily_kline_batch | silver.daily_kline |
| get_minute_kline | silver.minute_kline_{year} |
| get_adj_factor | silver.adj_factor |
| get_finance | silver.finance |
| get_f10 | silver.f10_doc + HTML 文件 |
| get_tables | 全部 schema | 列出数据库表，可按 schema 过滤 |
| get_table_schema | 指定表 | 查询单表列名、类型、主键 |

## 当前限制

- **不做复权**：`get_daily_kline` 的 `fq` 参数当前仅支持 `"bfq"`，复权后续加
- **分钟线依赖年表**：`minute_kline_{year}` 表不存在时返回空结果
- **SQL 拼接**：查询条件通过 f-string 拼接，仅限内部调用，不对外暴露原始 SQL
