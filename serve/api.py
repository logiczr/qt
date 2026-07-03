"""Serve 层 HTTP 传输 — FastAPI REST。

只做：参数解析、鉴权、调 queries.py、返回 JSON。
不写任何查询逻辑。
"""

import json
import logging
import os
import threading

import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse

from serve.queries import (
    get_adj_factor,
    get_daily_kline,
    get_daily_kline_batch,
    get_f10,
    get_finance,
    get_ingest_plans,
    get_ingest_steps,
    get_minute_kline,
    get_stock_info,
    get_stock_list,
    get_table_schema,
    get_tables,
)

_log = logging.getLogger(__name__)

app = FastAPI(title="QuantDB Serve", version="0.1")


# ---- 鉴权 ----

_API_KEYS: set[str] | None = None


def _get_api_keys() -> set[str]:
    global _API_KEYS
    if _API_KEYS is None:
        raw = os.getenv("QUANTDB_API_KEYS", "")
        _API_KEYS = set(k.strip() for k in raw.split(",") if k.strip())
        if _API_KEYS:
            _log.info("API key auth enabled, %d keys", len(_API_KEYS))
        else:
            _log.info("API key auth disabled (no QUANTDB_API_KEYS set)")
    return _API_KEYS


async def verify_api_key(x_api_key: str = Header(default="")):
    """API Key 鉴权中间件。未配置 QUANTDB_API_KEYS 则不鉴权。"""
    keys = _get_api_keys()
    if not keys:
        return
    if x_api_key not in keys:
        raise HTTPException(status_code=401, detail="invalid API key")


# ---- 工具 ----

def _df_to_json(df):
    """DataFrame → JSON list。"""
    if df is None or df.empty:
        return []
    return json.loads(df.to_json(orient="records"))


# ---- 路由 ----

@app.get("/api/stock_list")
async def api_stock_list(
    date: str | None = Query(default=None, description="日期 YYYY-MM-DD，空则全部"),
    _auth: str = Header(default=""),
):
    """股票列表。"""
    await verify_api_key(_auth)
    df = get_stock_list(date=date)
    return _df_to_json(df)


@app.get("/api/stock_info")
async def api_stock_info(
    code: str = Query(..., description="股票代码，如 600519 或 sh600519"),
    _auth: str = Header(default=""),
):
    """单只股票基本信息。"""
    await verify_api_key(_auth)
    info = get_stock_info(code)
    if info is None:
        raise HTTPException(status_code=404, detail="stock not found")
    # Series 的 to_dict 可能含 numpy 类型，走 JSON 序列化
    return json.loads(pd.io.json.dumps(info, default=str))


@app.get("/api/daily_kline")
async def api_daily_kline(
    code: str = Query(..., description="股票代码"),
    start: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end: str = Query(..., description="结束日期 YYYY-MM-DD"),
    fq: str = Query(default="bfq", description="复权方式，当前仅 bfq"),
    _auth: str = Header(default=""),
):
    """单只股票日线。"""
    await verify_api_key(_auth)
    df = get_daily_kline(code=code, start=start, end=end, fq=fq)
    return _df_to_json(df)


@app.get("/api/daily_kline/batch")
async def api_daily_kline_batch(
    codes: str = Query(..., description="逗号分隔的股票代码，如 600519,000001"),
    start: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end: str = Query(..., description="结束日期 YYYY-MM-DD"),
    fq: str = Query(default="bfq", description="复权方式，当前仅 bfq"),
    _auth: str = Header(default=""),
):
    """批量股票日线。"""
    await verify_api_key(_auth)
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    df = get_daily_kline_batch(codes=code_list, start=start, end=end, fq=fq)
    return _df_to_json(df)


@app.get("/api/minute_kline")
async def api_minute_kline(
    code: str = Query(..., description="股票代码"),
    date: str = Query(..., description="日期 YYYY-MM-DD"),
    _auth: str = Header(default=""),
):
    """分钟线。"""
    await verify_api_key(_auth)
    df = get_minute_kline(code=code, date=date)
    return _df_to_json(df)


@app.get("/api/adj_factor")
async def api_adj_factor(
    code: str = Query(..., description="股票代码"),
    start: str = Query(..., description="起始日期 YYYY-MM-DD"),
    end: str = Query(..., description="结束日期 YYYY-MM-DD"),
    _auth: str = Header(default=""),
):
    """复权因子。"""
    await verify_api_key(_auth)
    df = get_adj_factor(code=code, start=start, end=end)
    return _df_to_json(df)


@app.get("/api/finance")
async def api_finance(
    code: str = Query(..., description="股票代码"),
    _auth: str = Header(default=""),
):
    """财务数据。"""
    await verify_api_key(_auth)
    df = get_finance(code=code)
    return _df_to_json(df)


@app.get("/api/f10", response_class=HTMLResponse)
async def api_f10(
    code: str = Query(..., description="股票代码"),
    _auth: str = Header(default=""),
):
    """F10 文档（最新 HTML）。"""
    await verify_api_key(_auth)
    html = get_f10(code=code)
    if html is None:
        raise HTTPException(status_code=404, detail="F10 not found")
    return html


@app.get("/api/tables")
async def api_tables(
    schema: str | None = Query(default=None, description="schema 名，如 bronze/silver/gold，空则全部"),
    _auth: str = Header(default=""),
):
    """列出数据库表。"""
    await verify_api_key(_auth)
    df = get_tables(schema=schema)
    return _df_to_json(df)


@app.get("/api/table_schema")
async def api_table_schema(
    table: str = Query(..., description="完整表名，如 silver.daily_kline"),
    _auth: str = Header(default=""),
):
    """查询单表结构（列名、类型、主键等）。"""
    await verify_api_key(_auth)
    try:
        df = get_table_schema(table_name=table)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    return _df_to_json(df)


@app.get("/api/ingest/plans")
async def api_ingest_plans(
    limit: int = Query(default=50, description="返回条数"),
    _auth: str = Header(default=""),
):
    """查询 ingest 操作记录。"""
    await verify_api_key(_auth)
    df = get_ingest_plans(limit=limit)
    return _df_to_json(df)


@app.get("/api/ingest/steps")
async def api_ingest_steps(
    plan_id: str = Query(..., description="操作 ID"),
    _auth: str = Header(default=""),
):
    """查询某次 ingest 操作的步骤详情。"""
    await verify_api_key(_auth)
    df = get_ingest_steps(plan_id=plan_id)
    return _df_to_json(df)


# ---- Hard Init ----

_hard_init_running = False


def _run_hard_init():
    """后台线程：全量拉取 + 构建。"""
    global _hard_init_running
    try:
        _log.info("hard init: 开始全量拉取")

        # Bronze: backfill_daily 全市场
        from bronze import Bronze, IngestOrder
        brz = Bronze()

        # 先确保股票列表就绪
        _log.info("hard init: 拉取股票列表")
        for r in brz.execute(IngestOrder(action="weekly")):
            _log.info("hard init bronze: %s %s %d rows", r.action, r.status, r.rows)

        # 日线 full（TDX get_kline_full + Tencent get_kline_full）
        _log.info("hard init: 全量日线 backfill")
        for r in brz.execute(IngestOrder(action="backfill_daily", codes=[])):
            _log.info("hard init backfill: %s %s %d rows", r.action, r.status, r.rows)

        # 分钟线 full（TDX 1m get_kline_full）
        _log.info("hard init: 全量分钟线")
        for r in brz.execute(IngestOrder(action="first_init")):
            _log.info("hard init 1m: %s %s %d rows", r.action, r.status, r.rows)

        # Silver: full 构建
        _log.info("hard init: Silver 全量构建")
        from silver import Silver, BuildOrder
        sv = Silver()
        for target in ["stock_map", "adj_factor", "daily_kline", "minute_kline", "finance"]:
            for r in sv.execute(BuildOrder(target=target, mode="full")):
                _log.info("hard init silver %s: %s %d rows", target, r.status, r.rows)

        _log.info("hard init: 完成")
    except Exception:
        _log.exception("hard init: 失败")
    finally:
        _hard_init_running = False


@app.post("/api/init/hard")
async def api_hard_init(_auth: str = Header(default="")):
    """全量初始化：后台拉取所有日线/分钟线历史数据并构建 Silver 层。

    非阻塞，立即返回。通过 /api/ingest/plans 查看进度。
    同一时间只能跑一个。
    """
    await verify_api_key(_auth)
    global _hard_init_running
    if _hard_init_running:
        return {"status": "already_running", "message": "hard init 正在运行中"}
    _hard_init_running = True
    threading.Thread(target=_run_hard_init, daemon=True).start()
    return {"status": "started", "message": "hard init 已启动，查看 /api/ingest/plans 跟踪进度"}


# ---- 启动 ----

def start(host: str = "0.0.0.0", port: int = 8000):
    """启动 serve API 服务。"""
    import uvicorn
    from db import configure
    configure(db_path="Data.duckdb")
    _log.info("serve starting on %s:%d", host, port)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start()
