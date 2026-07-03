"""
腾讯财经 (Tencent Finance) 数据源，基于 HTTP GET 协议。
返回原始 DataFrame，不做列改名/类型转换。
编码: 快照类接口 GBK (~~分隔)，K线/分时 JSON。
限制: 建议请求间隔 >=100ms，高频调用可能封IP。
"""

import json
import logging
import time
import datetime
from typing import Optional

import pandas as pd
import requests

_logger = logging.getLogger(__name__)

# ---- 常量 ----------------------------------------------------------------

# 实时快照 87 个字段名 (按索引位置)
_SNAPSHOT_COLS = [
    "version", "name", "code", "current_price", "last_close",
    "open", "volume", "outer_disc", "inner_disc",
    "bid1_price", "bid1_volume", "bid2_price", "bid2_volume",
    "bid3_price", "bid3_volume", "bid4_price", "bid4_volume",
    "bid5_price", "bid5_volume",
    "ask1_price", "ask1_volume", "ask2_price", "ask2_volume",
    "ask3_price", "ask3_volume", "ask4_price", "ask4_volume",
    "ask5_price", "ask5_volume",
    "latest_trade", "update_time", "change", "change_pct",
    "high", "low", "price_vol_amt", "volume_hand", "amount_wan",
    "turnover_rate", "pe_ttm", "unknown_40", "high_dup", "low_dup",
    "amplitude", "circ_market_cap", "total_market_cap", "pb",
    "limit_up", "limit_down", "volume_ratio", "bid_ask_diff",
    "avg_price", "pe_dynamic", "pe_static",
    "unknown_54", "unknown_55", "unknown_56","amount_wan_hp",
    "unknown_58", "unknown_59", "unknown_60",
    "stock_type", "speed_pct", "change_pct_5min", "change_pct_ytd",
    "unknown_65", "unknown_66", "unknown_67", 
    "unknown_68", "unknown_69","dividend_yield_ttm",
    "unknown_71","circ_shares", "total_shares",
    "unknown_74", "unknown_75", "unknown_76", "unknown_77",
    "unknown_78", "unknown_79", "unknown_80", "unknown_81",
    "currency", "listing_status", "event_mark",
    "unknown_85", "unknown_86","tail"
]


# ---- 实时行情 ------------------------------------------------------------

def quotes(codes, delay: float = 0.15) -> Optional[pd.DataFrame]:
    """实时行情快照 (5档买卖盘 + PE/PB/市值)，87 列。

    Args:
        codes: str 或 list[str]，如 'sh600519' 或 ['sh600519','sz000001']，最多支持900支股票。
        delay: 单次请求间隔(秒)，默认 150ms。腾讯每次仅支持有限只股票，数量多时分批请求。

    返回字段: version/name/code/current_price/last_close/open/high/low/
        volume/turnover_rate/amount_wan/pe_ttm/pb/
        bid1~bid5_price/ask1~ask5_price/
        circ_market_cap/total_market_cap/dividend_yield_ttm/stock_type 等。
    """

    if isinstance(codes, str):
        codes = [codes]
    #_logger.info("quotes codes=%s", codes)

    frames: list[pd.DataFrame] = []
    batch_size = 900
    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        url = "http://qt.gtimg.cn/q=" + ",".join(batch)
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            r.encoding = "gbk"
        except requests.RequestException:
            _logger.warning("quotes batch %d-%d: 请求失败", i, i + len(batch))
            continue

        rows = [line.strip().split("~") for line in r.text.strip().splitlines()]
        if not rows:
            _logger.warning("quotes batch %d-%d: 返回空", i, i + len(batch))
            continue
        df = pd.DataFrame(rows, columns=_SNAPSHOT_COLS)
        frames.append(df)
        if i + batch_size < len(codes):
            time.sleep(delay)
    if not frames:
        _logger.warning("quotes: 全部批次返回空")
        return None
    result = pd.concat(frames, ignore_index=True)
    _logger.info("quotes: %d 行", len(result))
    return result


# ---- K 线 ----------------------------------------------------------------

def bars(
    code: str,
    ktype: str = "day",
    fq: str = "qfq",
    start: str = "",
    end: str = "",
    count: int = 250,
) -> Optional[pd.DataFrame]:
    """K 线数据 (日/周/月)。

    Args:
        code: 股票代码，如 'sh600519'。
        ktype: K线周期 'day'/'week'/'month'。
        fq: 复权方式 'qfq'=前复权 / 'hfq'=后复权 / ''=不复权。
        start: 开始日期 'YYYY-MM-DD'，留空按 count 取。
        end: 结束日期 'YYYY-MM-DD'，留空到最新。
        count: 条数。
    """

    param = f"{code},{ktype},{start},{end},{count},{fq}"
    param = param.rstrip(",")
    _logger.info("bars code=%s ktype=%s fq=%s start=%s end=%s count=%d",
                 code, ktype, fq, start, end, count)
    url = (f"https://ifzq.gtimg.cn/appstock/app/kline/kline?param={param}"
           if fq == "" else
           f"https://ifzq.gtimg.cn/appstock/app/fqkline/get?param={param}")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        _logger.warning("bars %s: 请求失败 %s", code, e)
        return None

    try:
        data = r.json()
    except json.JSONDecodeError:
        _logger.warning("bars %s: JSON 解析失败", code)
        return None

    if not isinstance(data, dict) or data.get("code") != 0 or "data" not in data:
        _logger.warning("bars %s: 返回异常 code=%s", code, data.get("code") if isinstance(data, dict) else "N/A")
        return None

    stock_data = data["data"].get(code, None)
    if stock_data is None:
        _logger.warning("bars %s: data.%s 缺失", code, code)
        return None

    # 查找 K 线数据 key (如 qfqday / day / hfqweek 等)
    kline_key = f"{fq}{ktype}" if fq else ktype
    kline_list = stock_data.get(kline_key)
    if kline_list is None:
        # fallback: 尝试匹配任意 key
        for k in stock_data:
            if k.endswith(ktype):
                kline_list = stock_data[k]
                break
    if kline_list is None:
        _logger.warning("bars %s: 无 K 线数据 (key=%s)", code, kline_key)
        return None

    # K线字段: [日期, 开盘, 收盘, 最高, 最低, 成交量, ?分红对象]
    rows = []
    for item in kline_list:
        row = {
            "date": str(item[0]),
            "open": float(item[1]),
            "close": float(item[2]),
            "high": float(item[3]),
            "low": float(item[4]),
            "volume": float(item[5]),
            "amount": float(item[6]) if len(item) >= 7 and not isinstance(item[6], dict) else None,
            "extra": None,
        }
        if len(item) >= 7 and isinstance(item[6], dict):
            row["amount"] = None
            row["extra"] = item[6]
        elif len(item) >= 7 and isinstance(item[6], (str, int, float)):
            try:
                row["amount"] = float(item[6])
            except (ValueError, TypeError):
                row["amount"] = None
        if len(item) >= 8 and isinstance(item[7], dict):
            row["extra"] = item[7]
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        _logger.warning("bars %s: 无数据行", code)
        return None
    df["stock_code"] = code
    _logger.info("bars %s: %d 行", code, len(df))
    return df


# ---- 分时数据 ------------------------------------------------------------

def minute(code: str) -> Optional[pd.DataFrame]:
    """当日分时数据，5 列 (time/price/volume/amount/stock_code)。

    volume 和 amount 为累计值。

    Args:
        code: 股票代码，如 'sh600519'。
    """

    _logger.info("minute code=%s", code)
    try:
        r = requests.get("https://ifzq.gtimg.cn/appstock/app/minute/query",
                         params={"code": code}, timeout=10)
        r.raise_for_status()
    except requests.RequestException:
        _logger.warning("minute %s: 请求失败", code)
        return None

    try:
        data = r.json()
    except json.JSONDecodeError:
        _logger.warning("minute %s: JSON 解析失败", code)
        return None

    if data.get("code") != 0 or "data" not in data:
        _logger.warning("minute %s: 返回异常", code)
        return None

    stock_data = data["data"].get(code, None)
    if stock_data is None or "data" not in stock_data:
        _logger.warning("minute %s: 无分时数据", code)
        return None

    # data.data 是分钟数据列表，每条: space-separated "time price volume amount"
    minutes_raw = stock_data["data"]
    if not minutes_raw:
        _logger.warning("minute %s: 分时列表为空", code)
        return None

    rows = []
    for item in minutes_raw:
        parts = item.split(" ")
        if len(parts) >= 4:
            rows.append({
                "time": parts[0],
                "price": float(parts[1]),
                "volume": float(parts[2]),
                "amount": float(parts[3]),
            })

    if not rows:
        _logger.warning("minute %s: 解析后无数据", code)
        return None
    df = pd.DataFrame(rows)
    df["stock_code"] = code
    _logger.info("minute %s: %d 行", code, len(df))
    return df


# ---- 资金流向 ------------------------------------------------------------

def money_flow(code: str) -> Optional[pd.DataFrame]:
    """资金流向，15 列。

    Args:
        code: 股票代码，如 'sz000858'。注意: 部分股票可能返回空。

    返回字段: code/main_in/main_out/main_net/main_ratio/
        retail_in/retail_out/retail_net/retail_ratio/
        total_flow/name/date 等。
    """

    _logger.info("money_flow code=%s", code)
    try:
        r = requests.get(f"http://qt.gtimg.cn/q=ff_{code}", timeout=10)
        r.raise_for_status()
        r.encoding = "gbk"
    except requests.RequestException:
        _logger.warning("money_flow %s: 请求失败", code)
        return None

    rows = [line.strip().split("~") for line in r.text.strip().splitlines()]
    if not rows:
        _logger.warning("money_flow %s: 返回空 (可能不支持该股票)", code)
        return None
    df = pd.DataFrame(rows)
    _logger.info("money_flow %s: %d 行", code, len(df))
    return df


# ---- 盘口分析 ------------------------------------------------------------

def order_book(code: str) -> Optional[pd.DataFrame]:
    """盘口分析 (大小单统计)，5 列。

    Args:
        code: 股票代码，如 'sh600519'。

    返回字段: buy_big/buy_small/sell_big/sell_small。
    """

    _logger.info("order_book code=%s", code)
    try:
        r = requests.get(f"http://qt.gtimg.cn/q=s_pk{code}", timeout=10)
        r.raise_for_status()
        r.encoding = "gbk"
    except requests.RequestException:
        _logger.warning("order_book %s: 请求失败", code)
        return None

    rows = [line.strip().split("~") for line in r.text.strip().splitlines() if line.strip()]
    if not rows:
        _logger.warning("order_book %s: 返回空", code)
        return None
    df = pd.DataFrame(rows)
    _logger.info("order_book %s: %d 行", code, len(df))
    return df


# ---- 简要信息 ------------------------------------------------------------

def brief(codes, delay: float = 0.15) -> Optional[pd.DataFrame]:
    """简要信息 (轻量快照)，12 列。

    相比完整快照仅 12 字段，适合列表/概览场景。

    Args:
        codes: str 或 list[str]，如 'sh600519' 或 ['sh600519','sz000001']。
        delay: 单次请求间隔(秒)，默认 150ms。

    返回字段: version/name/code/current_price/change/change_pct/
        volume/amount_wan/total_market_cap/stock_type。
    """

    if isinstance(codes, str):
        codes = [codes]
    _logger.info("brief codes=%s", codes)

    frames: list[pd.DataFrame] = []
    batch_size = 600
    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        url = "http://qt.gtimg.cn/q=" + ",".join(f"s_{c}" for c in batch)
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            r.encoding = "gbk"
        except requests.RequestException:
            _logger.warning("brief batch %d-%d: 请求失败", i, i + len(batch))
            continue

        rows = [line.strip().split("~") for line in r.text.strip().splitlines() if line.strip()]
        if not rows:
            _logger.warning("brief batch %d-%d: 返回空", i, i + len(batch))
            continue
        df = pd.DataFrame(rows)
        frames.append(df)
        if i + batch_size < len(codes):
            time.sleep(delay)
    if not frames:
        _logger.warning("brief: 全部批次返回空")
        return None
    result = pd.concat(frames, ignore_index=True)
    _logger.info("brief: %d 行", len(result))
    return result
