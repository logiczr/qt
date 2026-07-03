"""Serve 层 — 历史数据读取服务。

本机调用: from serve.queries import get_daily_kline
内网调用: GET /api/daily_kline?code=600519&start=...&end=...
"""

from serve.queries import (
    get_stock_list,
    get_stock_info,
    get_daily_kline,
    get_daily_kline_batch,
    get_minute_kline,
    get_adj_factor,
    get_finance,
    get_f10,
    get_tables,
    get_table_schema,
    get_ingest_plans,
    get_ingest_steps,
)

__all__ = [
    "get_stock_list",
    "get_stock_info",
    "get_daily_kline",
    "get_daily_kline_batch",
    "get_minute_kline",
    "get_adj_factor",
    "get_finance",
    "get_f10",
    "get_tables",
    "get_table_schema",
    "get_ingest_plans",
    "get_ingest_steps",
]
