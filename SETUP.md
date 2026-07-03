# 首次部署指南

从零开始部署 A 股量化数据仓库，让系统跑起来。

---

## 1. 环境准备

### Python 版本

Python 3.10+

### 创建虚拟环境 & 安装依赖

```bash
cd qt
python -m venv venv
source venv/bin/activate
pip install duckdb pandas numpy mootdx requests apscheduler \
    fastapi uvicorn baostock tqdm
```

核心依赖：

| 包 | 用途 |
|---|---|
| duckdb | 单文件列式数据库 |
| pandas | 数据处理 |
| numpy | 数值计算（Silver 层） |
| mootdx | 通达信 TCP 行情源 |
| requests | 腾讯 HTTP 行情源 |
| apscheduler | 定时调度 |
| fastapi | serve/realtime/heatmap API 服务 |
| uvicorn | ASGI 服务器 |
| baostock | 行业分类（heatmap 用） |
| tqdm | 进度条 |

### 目录结构

部署前需确保以下目录存在：

```bash
mkdir -p data/bronze/f10 data/silver/f10 data/industry
```

- `data/bronze/f10/` — F10 原始文本（TDX）
- `data/silver/f10/` — F10 渲染 HTML
- `data/industry/` — 行业分类 CSV 缓存（heatmap 用）

数据库文件 `Data.duckdb` 在初始化时自动创建。

---

## 2. 初始化数据库

```python
from db import configure, get_db

configure("Data.duckdb")
db = get_db()
db.init_database()  # 创建 bronze/silver/gold 三个 schema
```

这会执行 `db/schema.sql`，创建三个空 schema。表在数据首次写入时按 DataFrame 列类型自动创建，无需预定义 DDL。

ingest schema（`ingest.plan` / `ingest.step`）在 `Pipeline.__init__()` 时自动创建。

---

## 3. 首次数据拉取（Bronze 层）

首次部署必须按顺序串行执行，因为后续步骤依赖前置数据。

### 3.1 股票列表 + 日线 + 分钟线

```python
from bronze import Bronze, IngestOrder

brz = Bronze()
list(brz.execute(IngestOrder(action="first_init")))
```

`first_init` 做的事：
1. 拉取 stocks（股票列表）
2. 拉取 daily kline（TDX offset=800 + Tencent 全量翻页）
3. 拉取 1m kline（TDX get_kline_full）

但 first_init **缺少** xdxr/finance/block/F10，这些需要单独补。

### 3.2 除权除息 + 板块 + 财务

```python
list(brz.execute(IngestOrder(action="weekly")))
```

`weekly` 做的事：
1. 拉取 stocks（再次确认最新）
2. 拉取 xdxr（除权除息）
3. 拉取 block（板块）
4. 拉取 finance（财务）

### 3.3 F10 资料（可选，耗时长）

```python
list(brz.execute(IngestOrder(action="f10")))
# 默认拉全市场
```

F10 原始文件写入 `data/bronze/f10/{code}_{date}.txt`，DB 只存元数据指针。已去重：同 code+hash 不重复写入。

### 3.4 验证 Bronze 数据

```python
report = brz.diagnose()
print(report)
```

diagnose 基于当日 `ingested_at` 检查各表 code 覆盖率。首次部署当天跑才有意义。

---

## 4. 首次构建 Silver 层

Bronze 数据就绪后，按依赖顺序构建 Silver：

```python
from silver import Silver, BuildOrder

sv = Silver()

# 1. stock_map（依赖 stocks + finance）
list(sv.execute(BuildOrder(target="stock_map", mode="full")))

# 2. adj_factor（依赖 xdxr）
list(sv.execute(BuildOrder(target="adj_factor", mode="full")))

# 3. daily_kline（依赖 BFQ 日线 + adj_factor + stock_map 的 ipo_date）
list(sv.execute(BuildOrder(target="daily_kline", mode="full")))

# 4. minute_kline（依赖 1m BFQ + adj_factor）
list(sv.execute(BuildOrder(target="minute_kline", mode="full")))

# 5. finance（依赖 raw_tdx_finance）
list(sv.execute(BuildOrder(target="finance", mode="full")))

# 6. f10_doc（依赖 raw_tdx_f10 文本 → 渲染 HTML）
list(sv.execute(BuildOrder(target="f10_doc", mode="full")))
```

**顺序不能乱**：daily_kline 依赖 adj_factor 和 stock_map，minute_kline 依赖 adj_factor。

---

## 5. 启动服务

Silver 层构建完成后，可以启动以下服务：

### 5.1 历史数据服务（端口 8000）

```bash
python -m serve.api
```

验证：

```bash
curl "http://127.0.0.1:8000/api/stock_list?date=2025-01-01" | head
curl "http://127.0.0.1:8000/api/daily_kline?code=600519&start=2025-01-01&end=2025-06-30"
curl "http://127.0.0.1:8000/api/tables?schema=silver"
curl "http://127.0.0.1:8000/api/ingest/plans"
```

### 5.2 实时行情服务（端口 8888）

```bash
python -m realtime.api
```

验证：

```bash
# REST（需盘中）
curl "http://127.0.0.1:8000/api/quotes?codes=sh600519,sz000001"

# WebSocket
python test_ws_client.py
```

盘中自动轮询（9:25-11:35 / 12:55-15:05），盘前盘后自动暂停。

### 5.3 行业热力图（端口 9999）

```bash
python heatmap.py
```

浏览器访问 `http://127.0.0.1:9999`。首次启动会从 baostock 拉取行业分类并缓存到 `data/industry/industry.csv`，后续直接读缓存。

热力图同时启动 Feed 轮询，盘中自动推送数据。

### 鉴权（可选）

三个服务共用 `QUANTDB_API_KEYS` 环境变量：

```bash
QUANTDB_API_KEYS=key1,key2 python -m serve.api
```

请求时带 Header：`X-API-Key: key1`。未配置则不鉴权。

---

## 6. 启动日常调度

首次部署完成后，启动 Pipeline 进入自动调度：

```python
from ingest import Pipeline

pipe = Pipeline()
pipe.start_scheduler()

# 每日 15:05 → fast_close
# 每日 18:00 → daily（含 auto_heal）
# 每周六 10:00 → weekly（xdxr + block + finance + stock_map 更新）
```

Pipeline 会通过 Dealer 自动处理：Bronze → 失败重试 → Silver 构建 → 完整性检查 → 补缺。

---

## 7. 完整首次部署脚本

```python
"""首次部署 — 从零到可用"""
import os

# 目录
os.makedirs("data/bronze/f10", exist_ok=True)
os.makedirs("data/silver/f10", exist_ok=True)
os.makedirs("data/industry", exist_ok=True)

# DB
from db import configure, get_db
configure("Data.duckdb")
get_db().init_database()

# Bronze
from bronze import Bronze, IngestOrder
brz = Bronze()

print(">>> stocks + daily + 1m")
list(brz.execute(IngestOrder(action="first_init")))

print(">>> xdxr + block + finance")
list(brz.execute(IngestOrder(action="weekly")))

# F10 可选，很慢
# print(">>> F10")
# list(brz.execute(IngestOrder(action="f10")))

# Silver
from silver import Silver, BuildOrder
sv = Silver()

for target in ["stock_map", "adj_factor", "daily_kline", "minute_kline", "finance"]:
    print(f">>> {target}")
    list(sv.execute(BuildOrder(target=target, mode="full")))

# 启动服务（另开终端）
# python -m serve.api         # 端口 8000
# python -m realtime.api      # 端口 8888
# python heatmap.py           # 端口 9999

# 日常调度
from ingest import Pipeline
pipe = Pipeline()
pipe.start_scheduler()
```

---

## 注意事项

- **TDX 分钟线历史深度仅 5-6 个月**，首次部署前的更早分钟数据无法获取
- **xdxr/finance 每次全量重插**，首次部署不影响，日常运行效率偏低
- **diagnose 基于当日 ingested_at**，隔天跑会误报，需在拉取数据当天验证
- **first_init 的 daily 只拉 offset=800**（约 3 年），如需更早日线可单独跑 backfill_daily
- **F10 全量拉取很慢**（5000+ 只），建议放在最后或空闲时跑
- **三个服务端口**：serve 8000、realtime 8888、heatmap 9999，可按需启动
- **实时行情仅盘中有效**，盘前盘后 Feed 自动暂停轮询
