# Serve 层 + 实时服务设计

## 全局架构

```
                        ┌─────────────────────────────────────┐
                        │            外部应用                   │
                        │   （本机 / 内网用户 / 策略引擎）        │
                        └──────┬──────────────┬───────────────┘
                               │              │
                    历史数据查询 │              │ 实时行情
                               ▼              ▼
                        ┌──────────┐   ┌──────────┐
                        │  serve/  │   │ realtime/ │
                        │ 读取服务  │   │ 实时服务   │
                        └────┬─────┘   └────┬─────┘
                             │              │
                     读 DuckDB          读 fetch 层
                             │              │
                        ┌────▼─────┐   ┌────▼─────┐
                        │  Gold /  │   │  fetch/  │
                        │  Silver  │   │ tdx 腾讯  │
                        └──────────┘   └──────────┘
```

两条数据通路：

| 通路 | 数据来源 | 延迟 | 用途 |
|---|---|---|---|
| **serve 层** | DuckDB（Gold/Silver） | 分钟级 | 历史回测、报表分析 |
| **realtime 层** | fetch 层直连数据源 | 秒级 | 盘中监控、实时策略 |

两条通路共享同一个架构模式：**逻辑层 + 传输层分离**。

---

## 一、Serve 层 — 历史数据读取

### 定位

数仓的**读端封装**，与 fetch 层（写端封装）对称：

```
外部数据源 → fetch 层（封装拉取）→ Bronze → Silver → Gold → serve 层（封装读取）→ 外部应用
```

外部应用不直接写 SQL、不直接访问 DuckDB、不感知表结构变化。

### 为什么需要这一层

现在项目里取数据是 `get_db().execute("SELECT ...")` 裸查，问题：

- SQL 散落各处，表结构一改全崩
- 没有过滤/分页/参数校验
- 调用方需要理解 Bronze/Silver/Gold 表结构
- 无法提供内网访问

Serve 层封装后，调用方只需要知道业务语义（"拿日线"），不需要知道表名和 SQL。

### 为什么不用 DuckDB httpserver 扩展

DuckDB 社区有 httpserver 扩展可以直接暴露 SQL 接口：

```
INSTALL httpserver FROM community;
LOAD httpserver;
SELECT httpserve_start('0.0.0.0', 9999, 'user:pass');
```

但：

- **裸 SQL 暴露**：别人可以 DROP TABLE、DELETE、改数据
- **无语义化接口**：调用方需要知道表名、字段名、JOIN 关系
- **表结构变化全崩**：所有调用方的 SQL 都硬编码了表结构

用 FastAPI 包一层，只暴露定义好的接口，安全可控。

### 目录结构

```
serve/
├── __init__.py
├── queries.py      # 查询逻辑（纯函数：参数 → DataFrame）
└── api.py          # HTTP 传输层（FastAPI，调 queries.py，返回 JSON）
```

### queries.py

```python
"""Serve 层查询逻辑 — 历史数据读取的唯一入口。

本机调用直接 import，内网调用通过 api.py 走 HTTP。
所有查询逻辑只写在这里，api.py 不写任何查询。
"""
import pandas as pd


def get_daily_kline(
    code: str,
    start: str,
    end: str,
    fq: str = "bfq",
) -> pd.DataFrame:
    """单只股票日线行情。

    Args:
        code: 股票代码，如 "600519"
        start: 起始日期 "YYYY-MM-DD"
        end: 结束日期 "YYYY-MM-DD"
        fq: 复权方式 "bfq" / "qfq" / "hfq"

    Returns: DataFrame，列含 stock_code/date/open/high/low/close/vol/amount 等
    """

def get_daily_kline_batch(
    codes: list[str],
    start: str,
    end: str,
    fq: str = "bfq",
) -> pd.DataFrame:
    """批量股票日线行情。参数同 get_daily_kline，codes 为列表。"""

def get_minute_kline(
    code: str,
    date: str,
) -> pd.DataFrame:
    """单只股票分钟线。

    Args:
        code: 股票代码
        date: 日期 "YYYY-MM-DD"

    Returns: DataFrame，列含 stock_code/datetime/open/high/low/close/vol/amount
    """

def get_adj_factor(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """复权因子。

    Args:
        code: 股票代码
        start: 起始日期
        end: 结束日期

    Returns: DataFrame，列含 stock_code/date/adj_factor
    """

def get_stock_list(
    date: str | None = None,
) -> pd.DataFrame:
    """某日有效股票列表。

    Args:
        date: 日期，None 则返回最新

    Returns: DataFrame，列含 stock_code/name/ipo_date/market/industry
    """

def get_stock_info(
    code: str,
) -> dict:
    """单只股票基本信息（IPO日期、行业、市场等）。"""

def get_finance(
    code: str,
    report_type: str = "summary",
) -> pd.DataFrame:
    """财务数据。

    Args:
        code: 股票代码
        report_type: "summary" 汇总截面 / "income" 利润表 / "balance" 资产负债表

    Returns: DataFrame
    """
```

### api.py

```python
"""Serve 层 HTTP 传输 — FastAPI REST。

只做：参数解析、鉴权、调 queries.py、返回 JSON。
不写任何查询逻辑。
"""
from fastapi import FastAPI, Depends, Query

app = FastAPI(title="QuantDB Serve", version="0.1")


# ---- 鉴权 ----

async def verify_api_key(x_api_key: str = Header(...)):
    """API Key 鉴权中间件。请求头带 X-API-Key，与配置比对。"""


# ---- 路由 ----

@app.get("/api/daily_kline")
async def daily_kline(
    code: str,
    start: str,
    end: str,
    fq: str = "bfq",
):
    """单只股票日线。参数转调 queries.get_daily_kline()。"""

@app.get("/api/daily_kline/batch")
async def daily_kline_batch(
    codes: str,          # 逗号分隔，如 "600519,000001"
    start: str,
    end: str,
    fq: str = "bfq",
):
    """批量股票日线。"""

@app.get("/api/minute_kline")
async def minute_kline(code: str, date: str):
    """分钟线。"""

@app.get("/api/adj_factor")
async def adj_factor(code: str, start: str, end: str):
    """复权因子。"""

@app.get("/api/stock_list")
async def stock_list(date: str | None = None):
    """股票列表。"""

@app.get("/api/stock_info")
async def stock_info(code: str):
    """股票基本信息。"""

@app.get("/api/finance")
async def finance(code: str, type: str = "summary"):
    """财务数据。"""


# ---- 启动 ----

def start(host: str = "0.0.0.0", port: int = 8000):
    """uvicorn 启动入口。"""
    import uvicorn
    uvicorn.run(app, host=host, port=port)
```

### 对标 Databricks

| Databricks | 本项目 | 说明 |
|---|---|---|
| Gold 层视图/模型 | `serve/queries.py` | 语义化查询逻辑 |
| SQL Warehouse (JDBC/ODBC) | `serve/api.py` (FastAPI) | 网络传输层 |
| Unity Catalog 权限控制 | FastAPI 鉴权中间件 | 访问控制 |

Databricks 把 Gold 表推到 operational DB 给业务系统读，不在 Lakehouse 上直接查。
本项目同理：serve 层是唯一对外出口，不暴露 DuckDB 本身。

### 实现前提

1. Gold 层表设计（backtest_ready / factor_snapshot / stock_pool 等）
2. Gold 层 Builder 实现（Silver → Gold 转换）
3. 再基于 Gold 表实现 serve/queries.py
4. 最后加 serve/api.py 暴露 HTTP 接口

---

## 二、Realtime 层 — 盘中实时行情

### 定位

盘中实时数据服务，**绕过三层架构和数据库**，直接从 fetch 层获取最新行情推送给外部。

与 serve 层的区别：

| | serve 层 | realtime 层 |
|---|---|---|
| 数据来源 | DuckDB（已入库） | fetch 层（直连数据源） |
| 延迟 | 分钟级（入库后才能查） | 秒级（盘中实时） |
| 数据范围 | 历史全量 | 当日快照 |
| 写入 DB | 不写 | 不写 |

### 现有基础

fetch 层已有实时接口：

- `fetch.tencent.quotes(codes)` — HTTP 实时行情，全市场 0.3 秒一轮
- `fetch.tdx.quotes(symbols)` — TCP 实时行情快照

这两个都是**请求-响应**模式：调用一次返回一次，不会自动推送。

### 核心设计

```
┌──────────────────────────────────────────────────┐
│                   Feed 实例                       │
│                                                   │
│   后台线程：while True:                            │
│       df = fetch.tencent.quotes(codes=全市场)      │
│       self._df = df        ← 内存常驻 DataFrame   │
│       触发 WebSocket 订阅者回调                     │
│       sleep(0.3)                                  │
│                                                   │
│   REST 查询者：不管，等他们自己来拉 self._df         │
└──────────────────────────────────────────────────┘
```

- **内存常驻一个全市场 DataFrame** — 轮询拉到新数据直接替换
- **REST 不用管** — 客户端自己定时 GET，返回内存中 _df 的切片
- **WebSocket 订阅** — 每轮轮询完成后触发订阅者回调，推送变化数据

### 目录结构

```
realtime/
├── __init__.py
├── feed.py         # 行情逻辑（轮询 fetch 层，维护内存快照，订阅回调）
└── api.py          # 传输层（REST 查询 + WebSocket 推送）
```

### feed.py

```python
"""Realtime 层行情逻辑 — 盘中实时数据的唯一入口。

本机调用直接 import，内网调用通过 api.py 走 HTTP/WebSocket。
所有行情逻辑只写在这里，api.py 不写任何逻辑。

核心设计：
- 内存常驻一个全市场 DataFrame，轮询拉到新数据直接替换
- 一个轮询循环，所有消费者读同一份 DataFrame
- WebSocket 订阅者：每轮轮询完成后触发回调推送
- REST 查询者：不管，等他们自己来拉内存中的 DataFrame
"""
import threading
import time
import pandas as pd
from typing import Callable


class Feed:
    """行情引擎：轮询数据源，维护内存快照，分发更新。

    单例模式。全局只一个 Feed 实例，一个轮询循环。
    """

    def __init__(self, interval: float = 0.3):
        """
        Args:
            interval: 轮询间隔（秒），默认 0.3 秒
        """
        self._interval = interval
        self._df: pd.DataFrame | None = None       # 内存中全市场快照
        self._lock = threading.Lock()                # 替换 DataFrame 时的锁
        self._running = False
        self._thread: threading.Thread | None = None
        self._subscribers: dict[str, Callable] = {}  # subscription_id → callback

    def start(self):
        """启动轮询。盘中自动运行，盘前盘后自动暂停。
        后台线程循环：
            if market_open:
                df = fetch.tencent.quotes(codes=全市场代码)
                with lock: self._df = df
                for sub in subscribers: sub.callback(df.filter(sub.codes))
            sleep(interval)
        REST 不用管，等客户端自己来拉 self._df。
        """

    def stop(self):
        """停止轮询。"""

    @property
    def is_running(self) -> bool:
        """轮询是否在运行。"""

    # ---- 读取（读内存中的 _df） ----

    def get_snapshot(self, codes: list[str] | None = None) -> pd.DataFrame:
        """获取当前快照。直接返回内存中 _df 的切片，零延迟。

        Args:
            codes: 股票代码列表，None 则返回全市场

        Returns: DataFrame，每行一只股票
        """

    def get_one(self, code: str) -> pd.Series | None:
        """获取单只股票快照。无数据返回 None。"""

    # ---- 订阅（WebSocket 用） ----

    def subscribe(self, codes: list[str], callback: Callable[[pd.DataFrame], None]) -> str:
        """订阅股票更新。每轮轮询完成后触发 callback，传入筛选后的 DataFrame。

        Args:
            codes: 订阅的股票代码列表
            callback: 回调函数，接收筛选后的 DataFrame

        Returns: subscription_id，用于取消订阅
        """

    def unsubscribe(self, subscription_id: str):
        """取消订阅。"""

    # ---- 内部 ----

    def _poll_loop(self):
        """轮询主循环（后台线程执行）：
        while running:
            if market_open:
                df = fetch.tencent.quotes(codes=全市场)
                with lock: self._df = df
                for sub in subscribers: sub.callback(df.filter(sub.codes))
            sleep(interval)
        """

    def _is_market_open(self) -> bool:
        """判断当前是否在交易时段（9:30-11:30, 13:00-15:00）。"""


# ---- 全局单例 ----

_feed: Feed | None = None


def get_feed() -> Feed:
    """获取全局 Feed 单例。"""

def start_feed(interval: float = 0.3):
    """启动全局 Feed。"""

def stop_feed():
    """停止全局 Feed。"""


# ---- 便捷函数（本机直接用） ----

def get_snapshot(codes: list[str] | None = None) -> pd.DataFrame:
    """获取当前快照，直接调 get_feed().get_snapshot()。"""

def subscribe(codes: list[str], callback: Callable) -> str:
    """订阅更新，直接调 get_feed().subscribe()。"""

def unsubscribe(subscription_id: str):
    """取消订阅，直接调 get_feed().unsubscribe()。"""
```

### api.py

```python
"""Realtime 层 HTTP 传输 — FastAPI REST + WebSocket。

只做：参数解析、鉴权、调 feed.py、返回 JSON / 推送。
不写任何行情逻辑。
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Query, Header

app = FastAPI(title="QuantDB Realtime", version="0.1")


# ---- 鉴权 ----

async def verify_api_key(x_api_key: str = Header(...)):
    """API Key 鉴权中间件。"""


# ---- REST ----

@app.get("/api/quotes")
async def quotes(codes: str = Query(...)):
    """查询当前快照（客户端轮询模式）。

    直接返回内存中 _df 的切片，零延迟。

    Args:
        codes: 逗号分隔的股票代码，如 "600519,000001"

    Returns: JSON，每只股票一个对象
    """

@app.get("/api/quotes/all")
async def quotes_all():
    """全市场快照。"""


# ---- WebSocket（可选升级） ----

@app.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket, codes: str = ""):
    """WebSocket 推送。

    客户端连接时传 codes 参数订阅股票，
    Feed 每轮轮询完成后触发回调 → 推送更新给客户端。

    流程：
    1. 客户端连接 ws://host:8888/ws/quotes?codes=600519,000001
    2. 服务端 accept → 调 feed.subscribe(codes, callback)
    3. callback 触发时 → websocket.send_json(df)
    4. 客户端断开 → feed.unsubscribe()
    """


# ---- 启动 ----

def start(host: str = "0.0.0.0", port: int = 8888):
    """uvicorn 启动入口。"""
    import uvicorn
    uvicorn.run(app, host=host, port=port)
```

### 本机调用示例

```python
from realtime.feed import get_snapshot, subscribe

# 拿一次快照
df = get_snapshot(["600519", "000001"])

# 盘中持续订阅（回调模式）
def on_update(df):
    print(df[["code", "price", "vol"]])

sub_id = subscribe(["600519"], on_update)
# ... 之后
unsubscribe(sub_id)
```

### 内网调用示例

```
# REST — 客户端自己定时轮询（如每3秒）
GET /api/quotes?codes=600519,000001

# WebSocket（可选升级）— 服务端每轮轮询后主动推送
ws://192.168.x.x:8888/ws/quotes?codes=600519,000001
```

### WebSocket vs REST

```
REST（请求-响应）：客户端问 → 服务端答 → 结束。下次再问再答。
WebSocket（持续推送）：客户端订阅 → 连接保持 → 服务端有变化就推 → 一直推到收盘。
```

盘中5000只股票价格每秒在变：
- REST：10个用户每秒各请求一次 = 10次查询/秒（但服务端只读内存，不重复拉数据）
- WebSocket：1个轮询循环 + 推给10个订阅者 = 1次拉取/秒

**建议**：先用 REST，客户端3秒轮询一次延迟也可接受。等跑起来发现不够再升级 WebSocket。

### 鉴权

- REST：API Key header（`X-API-Key: xxx`）
- WebSocket：连接时带 token 参数
- feed.py / queries.py 本机调用不需要鉴权

---

## 三、共享模式：逻辑层 + 传输层分离

```
┌─────────────────┐    ┌─────────────────┐
│  serve/          │    │  realtime/       │
│  queries.py     │    │  feed.py         │
│  (查询逻辑)      │    │  (行情逻辑)       │
│  输入: 业务参数   │    │  输入: 股票代码    │
│  输出: DataFrame │    │  输出: DataFrame  │
└───────┬─────────┘    └───────┬─────────┘
        │                      │
  本机直接 import         本机直接 import
        │                      │
        ▼                      ▼
┌─────────────────┐    ┌─────────────────┐
│  serve/api.py   │    │  realtime/api.py │
│  (FastAPI)      │    │  (FastAPI)       │
│  REST → JSON    │    │  REST + WS       │
│  鉴权中间件      │    │  鉴权中间件       │
└─────────────────┘    └─────────────────┘
```

原则：

1. **逻辑只写一份** — queries.py / feed.py 是唯一真相源
2. **传输层是薄壳** — api.py 只做参数解析、鉴权、格式转换，不写业务逻辑
3. **改逻辑不影响传输** — 改查询只改 queries.py，API 自动同步
4. **改传输不影响逻辑** — 加 WebSocket 不改 feed.py

两层模块对照：

| | Serve 层 | Realtime 层 |
|---|---|---|
| 逻辑层 | `queries.py` — 纯函数，查 DuckDB | `feed.py` — Feed 类，轮询 + 内存 DataFrame |
| 传输层 | `api.py` — FastAPI REST | `api.py` — FastAPI REST + WebSocket |
| 本机入口 | `from serve.queries import get_daily_kline` | `from realtime.feed import get_snapshot` |
| 内网入口 | `GET /api/daily_kline?code=600519` | `GET /api/quotes?codes=600519` |
| 鉴权 | api.py 的 `verify_api_key` | api.py 的 `verify_api_key` |

---

## 四、依赖

| 包 | 用途 |
|---|---|
| fastapi | HTTP 框架（serve + realtime 共用） |
| uvicorn | ASGI 服务器 |
| websockets | WebSocket 支持（fastapi 自带，可选启用） |

均不在当前 requirements 中，实现时添加。

---

## 五、实现路线

按依赖关系排序：

1. **Gold 层** — 定义表结构 + Builder（serve 层的前提）
2. **serve/queries.py** — 基于 Gold 表写查询逻辑
3. **realtime/feed.py** — 轮询循环 + 内存 DataFrame + 订阅回调
4. **serve/api.py** — FastAPI REST，包 queries.py
5. **realtime/api.py** — FastAPI REST（先），WebSocket（后），包 feed.py
6. **鉴权** — API Key 中间件

5 和 6 可以并行开发。realtime 层不依赖 Gold 层，可以提前做。
