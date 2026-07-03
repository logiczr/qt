"""Serve 层查询逻辑 — 历史数据读取的唯一入口。

本机调用直接 import，内网调用通过 api.py 走 HTTP。
所有查询逻辑只写在这里，api.py 不写任何查询。
"""

import logging
import os

import pandas as pd

_log = logging.getLogger(__name__)


def _db():
    """获取 DB 单例。延迟导入避免循环依赖。"""
    from db import get_db
    return get_db()


def _to_plain(code: str) -> str:
    """代码格式统一为纯数字：sh600519/sz000001 → 600519/000001。"""
    if not code:
        return code
    s = str(code)
    if (s.startswith("sh") or s.startswith("sz")) and len(s) == 8:
        return s[2:]
    return s


# ---- 股票列表 ----

def get_stock_list(date: str | None = None) -> pd.DataFrame:
    """某日有效股票列表。

    Args:
        date: 日期 "YYYY-MM-DD"，None 则返回全部

    Returns: DataFrame，列含 stock_code_id/stock_code/stock_name/market/ipo_date
    """
    sql = "SELECT * FROM silver.stock_map"
    if date:
        sql += f" WHERE ipo_date <= '{date}'"
        # delist_date 为 NULL 或 > date
        sql += f" AND (delist_date IS NULL OR delist_date > '{date}')"
    sql += " ORDER BY stock_code"
    return _db().execute(sql, mode="read")


def get_stock_info(code: str) -> dict | None:
    """单只股票基本信息。

    Args:
        code: 股票代码，如 "600519" 或 "sh600519"

    Returns: dict 或 None
    """
    c = _to_plain(code)
    df = _db().execute(
        f"SELECT * FROM silver.stock_map WHERE stock_code = '{c}'",
        mode="read",
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


# ---- 日线 ----

def get_daily_kline(
    code: str,
    start: str,
    end: str,
    fq: str = "bfq",
) -> pd.DataFrame:
    """单只股票日线行情（不复权）。

    Args:
        code: 股票代码，如 "600519" 或 "sh600519"
        start: 起始日期 "YYYY-MM-DD"
        end: 结束日期 "YYYY-MM-DD"
        fq: 复权方式，当前仅支持 "bfq"

    Returns: DataFrame
    """
    c = _to_plain(code)
    df = _db().execute(
        f"SELECT * FROM silver.daily_kline "
        f"WHERE stock_code = '{c}' "
        f"AND trade_date >= '{start}' AND trade_date <= '{end}' "
        f"ORDER BY trade_date",
        mode="read",
    )
    return df


def get_daily_kline_batch(
    codes: list[str],
    start: str,
    end: str,
    fq: str = "bfq",
) -> pd.DataFrame:
    """批量股票日线行情。

    Args:
        codes: 股票代码列表
        start: 起始日期 "YYYY-MM-DD"
        end: 结束日期 "YYYY-MM-DD"
        fq: 复权方式，当前仅支持 "bfq"

    Returns: DataFrame
    """
    plain_codes = [_to_plain(c) for c in codes]
    in_list = ", ".join(f"'{c}'" for c in plain_codes)
    df = _db().execute(
        f"SELECT * FROM silver.daily_kline "
        f"WHERE stock_code IN ({in_list}) "
        f"AND trade_date >= '{start}' AND trade_date <= '{end}' "
        f"ORDER BY stock_code, trade_date",
        mode="read",
    )
    return df


# ---- 分钟线 ----

def get_minute_kline(
    code: str,
    date: str,
) -> pd.DataFrame:
    """单只股票分钟线。

    Args:
        code: 股票代码
        date: 日期 "YYYY-MM-DD"

    Returns: DataFrame
    """
    c = _to_plain(code)
    year = date[:4]
    table = f"silver.minute_kline_{year}"

    # 检查表是否存在
    tables = _db().list_tables()
    if table not in tables["name"].values:
        _log.warning("minute kline table not found: %s", table)
        return pd.DataFrame()

    return _db().execute(
        f"SELECT * FROM {table} "
        f"WHERE stock_code = '{c}' "
        f"AND datetime::DATE = '{date}' "
        f"ORDER BY datetime",
        mode="read",
    )


# ---- 复权因子 ----

def get_adj_factor(
    code: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """复权因子原始数据。

    Args:
        code: 股票代码
        start: 起始日期 "YYYY-MM-DD"
        end: 结束日期 "YYYY-MM-DD"

    Returns: DataFrame，列含 stock_code/trade_date/fenhong/peigu_price/songzhuangu/peigu/single_factor
    """
    c = _to_plain(code)
    return _db().execute(
        f"SELECT * FROM silver.adj_factor "
        f"WHERE stock_code = '{c}' "
        f"AND trade_date >= '{start}' AND trade_date <= '{end}' "
        f"ORDER BY trade_date",
        mode="read",
    )


# ---- 财务数据 ----

def get_finance(code: str) -> pd.DataFrame:
    """财务数据。

    Args:
        code: 股票代码

    Returns: DataFrame，列含 report_date 及 33 个财务字段
    """
    c = _to_plain(code)
    return _db().execute(
        f"SELECT * FROM silver.finance "
        f"WHERE stock_code = '{c}' "
        f"ORDER BY report_date DESC",
        mode="read",
    )


# ---- F10 文档 ----

def get_f10(code: str) -> str | None:
    """获取个股最新 F10 HTML 内容。

    Args:
        code: 股票代码

    Returns: HTML 字符串，无数据返回 None
    """
    c = _to_plain(code)
    df = _db().execute(
        f"SELECT file_path FROM silver.f10_doc "
        f"WHERE stock_code = '{c}' "
        f"ORDER BY trade_date DESC LIMIT 1",
        mode="read",
    )
    if df.empty:
        return None
    file_path = df.iloc[0]["file_path"]
    if not os.path.isfile(file_path):
        _log.warning("f10 file not found: %s", file_path)
        return None
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


# ---- Schema 查询 ----

def get_tables(schema: str | None = None) -> pd.DataFrame:
    """列出数据库表。

    Args:
        schema: 过滤指定 schema（如 "bronze"/"silver"/"gold"），None 则全部

    Returns: DataFrame，列含 schema/name/column_names/column_types
    """
    df = _db().list_tables()
    if schema:
        df = df[df["schema"] == schema]
    return df[["schema", "name", "column_names", "column_types"]]


def get_table_schema(table_name: str) -> pd.DataFrame:
    """查询单表结构。

    Args:
        table_name: 完整表名，如 "silver.daily_kline"

    Returns: DataFrame，列含 column_name/column_type/null/key/default/extra
    """
    return _db().execute(f"DESCRIBE {table_name}", mode="read")


# ---- Ingest 操作记录 ----

def get_ingest_plans(limit: int = 50) -> pd.DataFrame:
    """查询 ingest 操作记录（plan 级别）。

    Args:
        limit: 返回条数，默认 50

    Returns: DataFrame，列含 plan_id/action/status/started_at/finished_at
    """
    return _db().execute(
        f"SELECT plan_id, action, status, started_at, finished_at "
        f"FROM ingest.plan ORDER BY started_at DESC LIMIT {limit}",
        mode="read",
    )


def get_ingest_steps(plan_id: str) -> pd.DataFrame:
    """查询某次 ingest 操作的步骤详情。

    Args:
        plan_id: 操作 ID

    Returns: DataFrame，列含 step_name/seq/status/elapsed_ms/rows/failed/failed_codes
    """
    return _db().execute(
        f"SELECT step_name, seq, status, elapsed_ms, rows, failed, failed_codes "
        f"FROM ingest.step WHERE plan_id = '{plan_id}' ORDER BY seq",
        mode="read",
    )
