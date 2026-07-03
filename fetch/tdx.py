"""
通达信 (TDX) 数据源，基于 mootdx 库。
通过 TCP 协议直连通达信行情服务器，返回原始 DataFrame。
连接端口: 7709 (标准市场)，无需认证。
限制: 单次 K 线最多 800 条，分笔成交最多 2000 条。
"""

import logging
import time
from typing import Optional
import pandas as pd
from mootdx.quotes import Quotes

_logger = logging.getLogger(__name__)

# ---- 常量 ----------------------------------------------------------------

FREQ: dict[str, int] = {
    "1m": 8,
    "5m": 0,
    "15m": 1,
    "30m": 2,
    "60m": 3,
    "day": 4,
    "week": 5,
    "month": 6,
    "quarter": 10,
    "year": 11,
}

# ---- 实时行情 ------------------------------------------------------------

def quotes(symbols, market: str = "std") -> Optional[pd.DataFrame]:
    """实时行情快照 (5档买卖盘)，46 列。

    Args:
        symbols: str 或 list[str]，如 '000001' 或 ['000001','600519']。

    返回字段: market/code/active1/price/last_close/open/high/low/
        servertime/vol/cur_vol/amount/s_vol/b_vol/
        bid1~bid5/ask1~ask5/bid_vol1~bid_vol5/ask_vol1~ask_vol5/
        volume/reversed_bytes* 等。
    """

    _logger.info("quotes symbols=%s", symbols)
    df = Quotes.factory(market=market).quotes(symbols)
    if df is None:
        _logger.warning("quotes: 返回空")
        return None
    _logger.info("quotes: %d 行", len(df))
    return df

# ---- K 线 (bars 系列) ---------------------------------------------------

def bars(
    symbol: str,
    frequency: int,
    start: int = 0,
    offset: int = 800,
    market: str = "std",
) -> Optional[pd.DataFrame]:
    """K 线数据 (按起始位置和条数)，13 列，单次最多 800 条。

    Args:
        symbol: 股票代码，如 '000001'。
        frequency: K线周期，推荐用 FREQ 字典按名称取值。
        start: 起始位置，0=最新。
        offset: 条数，最大 800。

    返回: open/close/high/low/vol/amount/year/month/day/hour/
        minute/datetime/volume。datetime 作 index。
        含 stock_code 列 (bars 本身不返回，这里补齐)。
    """

    _logger.info("bars symbol=%s freq=%d start=%d offset=%d", symbol, frequency, start, offset)
    df = Quotes.factory(market=market).bars(symbol, frequency, start, offset)
    if df is None:
        _logger.warning("bars %s: 返回空", symbol)
        return None
    if "stock_code" not in df.columns:
        df["stock_code"] = symbol
    _logger.info("bars %s: %d 行", symbol, len(df))
    return df

def bars_all(
    symbol: str,
    frequency: int,
    max_start: int = 25600,
    market: str = "std",
) -> Optional[pd.DataFrame]:
    """分页拉取全量 K 线，自动循环 800 条/次，间隔 0.5s 防断连。

    Args:
        max_start: 最大起始位置，总条数，25600 ≈ 10 年日线。
    """

    _logger.info("bars_all symbol=%s freq=%d max_start=%d", symbol, frequency, max_start)
    client = Quotes.factory(market=market)
    frames: list[pd.DataFrame] = []
    start = 0
    while start <= max_start:
        df = client.bars(symbol, frequency, start, 800)
        print(df)
        if df is None:
            _logger.warning("bars_all %s start=%d: 返回空", symbol, start)
            break
        if "stock_code" not in df.columns:
            df["stock_code"] = symbol
        frames.append(df)
        if len(df) < 800:
            break
        start += 800
        time.sleep(0.2)
    _logger.info("bars_all %s: %d 行", symbol, sum(len(f) for f in frames) if frames else 0)
    return pd.concat(frames, ignore_index=True) if frames else None

# ---- K 线 (按日期范围) --------------------------------------------------

def k(
    symbol: str,
    begin: str,
    end: str,
    market: str = "std",
) -> Optional[pd.DataFrame]:
    """按日期范围获取日 K 线 (不复权)，10 列。

    Args:
        begin: 开始日期 'YYYY-MM-DD'。
        end: 结束日期 'YYYY-MM-DD'。

    返回: open/close/high/low/vol/amount/date/code/volume/factor。
        date 作 index。

    注意: 仅返回已入库的历史数据，无法取当天数据。
    如需最新 K 线，用 bars() (start=0)。
    """

    _logger.info("k symbol=%s begin=%s end=%s", symbol, begin, end)
    df = Quotes.factory(market=market).k(symbol, begin, end)
    if df is None:
        _logger.warning("k %s: 返回空", symbol)
        return None
    _logger.info("k %s: %d 行", symbol, len(df))
    return df

# ---- 指数 K 线 -----------------------------------------------------------

def index_bars(
    symbol: str,
    frequency: int,
    start: int = 0,
    offset: int = 800,
    market: str = "std",
) -> Optional[pd.DataFrame]:
    """指数 K 线，15 列。在个股 K 线基础上多 up_count/down_count。"""

    _logger.info("index_bars symbol=%s freq=%d", symbol, frequency)
    df = Quotes.factory(market=market).index_bars(symbol, frequency, start, offset)
    if df is None:
        _logger.warning("index_bars %s: 返回空", symbol)
        return None
    _logger.info("index_bars %s: %d 行", symbol, len(df))
    return df

# ---- 分时数据 ------------------------------------------------------------

def minute(symbol: str, market: str = "std") -> Optional[pd.DataFrame]:
    """当日分时数据，3 列 (price/vol/volume)。无显式时间列，
    行索引 0~119/240 隐含分钟顺序，需结合行情时段推算。"""

    _logger.info("minute symbol=%s", symbol)
    df = Quotes.factory(market=market).minute(symbol)
    if df is None:
        _logger.warning("minute %s: 返回空", symbol)
        return None
    _logger.info("minute %s: %d 行", symbol, len(df))
    return df

def minutes(symbol: str, date: str, market: str = "std") -> Optional[pd.DataFrame]:
    """历史分时数据，3 列，同 minute()。date: 'YYYYMMDD'。"""

    _logger.info("minutes symbol=%s date=%s", symbol, date)
    df = Quotes.factory(market=market).minutes(symbol, date)
    if df is None:
        _logger.warning("minutes %s %s: 返回空", symbol, date)
        return None
    _logger.info("minutes %s %s: %d 行", symbol, date, len(df))
    return df

# ---- 分笔成交 ------------------------------------------------------------

def transaction(
    symbol: str,
    start: int = 0,
    offset: int = 2000,
    market: str = "std",
) -> Optional[pd.DataFrame]:
    """当日分笔成交。交易时段外返回空。"""

    _logger.info("transaction symbol=%s", symbol)
    df = Quotes.factory(market=market).transaction(symbol, start, offset)
    if df is None:
        _logger.warning("transaction %s: 返回空", symbol)
        return None
    _logger.info("transaction %s: %d 行", symbol, len(df))
    return df

def transactions(
    symbol: str,
    start: int = 0,
    offset: int = 2000,
    date: str = "",
    market: str = "std",
) -> Optional[pd.DataFrame]:
    """历史分笔成交，5 列 (time/price/vol/buyorsell/volume)。
    buyorsell: 0=卖, 1=买, 2=中性。date: 'YYYYMMDD'。最多 2000 条。"""

    _logger.info("transactions symbol=%s date=%s", symbol, date)
    df = Quotes.factory(market=market).transactions(symbol, start, offset, date)
    if df is None:
        _logger.warning("transactions %s %s: 返回空", symbol, date)
        return None
    _logger.info("transactions %s %s: %d 行", symbol, date, len(df))
    return df

# ---- 股票列表 ------------------------------------------------------------

def stocks(market: str = "std") -> Optional[pd.DataFrame]:
    """沪深两市股票列表。

    返回: code/volunit/decimal_point/name/pre_close/market
        market: 'sz'=深圳, 'sh'=上海。
    """
    _logger.info("stocks")
    client = Quotes.factory(market=market)
    market_map = {0: "sz", 1: "sh"}
    frames = []
    for mkt in (0, 1):
        df = client.stocks(mkt)
        if df is not None:
            df["market"] = market_map[mkt]
            frames.append(df)
        else:
            _logger.warning("stocks market=%d: 返回空", mkt)
    _logger.info("stocks: %d 行 (sz=%d sh=%d)",
                 sum(len(f) for f in frames),
                 len(frames[0]) if len(frames) > 0 else 0,
                 len(frames[1]) if len(frames) > 1 else 0)
    return pd.concat(frames, ignore_index=True) if frames else None

# ---- 除权除息 ------------------------------------------------------------

def xdxr(symbol: str, market: str = "std") -> Optional[pd.DataFrame]:
    """除权除息，16 列。category: 1=除权除息, 2=送配股上市, 5=股本变化。"""

    _logger.info("xdxr symbol=%s", symbol)
    df = Quotes.factory(market=market).xdxr(symbol)
    if df is None:
        _logger.warning("xdxr %s: 返回空", symbol)
        return None
    _logger.info("xdxr %s: %d 行", symbol, len(df))
    return df

# ---- 财务数据 (在线) -----------------------------------------------------

def finance(symbol: str, market: str = "std") -> Optional[pd.DataFrame]:
    """在线财务数据，shape (1, 37)，仅最新一期汇总截面，非历史序列。"""

    _logger.info("finance symbol=%s", symbol)
    df = Quotes.factory(market=market).finance(symbol)
    if df is None:
        _logger.warning("finance %s: 返回空", symbol)
        return None
    _logger.info("finance %s: %d 列", symbol, df.shape[1])
    return df

# ---- F10 公司信息 --------------------------------------------------------

def f10c(symbol: str, market: str = "std") -> Optional[list[dict]]:
    """F10 资料目录。返回 list[dict]，key/value 均为字符串。"""

    _logger.info("f10c symbol=%s", symbol)
    result = Quotes.factory(market=market).F10C(symbol)
    if result is None:
        _logger.warning("f10c %s: 返回空", symbol)
        return None
    _logger.info("f10c %s: %d 条", symbol, len(result))
    return result

def f10(symbol: str, name: str = "", market: str = "std"):
    """
    获取 F10 资料详情
    
    Args:
        symbol (str): 证券代码
        name (str): 资料类别名称，默认为空字符串
        market (str): 市场标识，默认为 "std"
    
    Returns:
        DataFrame | str | None: 返回 F10 资料详情，内容均为字符串类型；若结果为空则返回 None
    """
    """F10 资料详情。返回 DataFrame 或原始文本，均为字符串类型。"""

    _logger.info("f10 symbol=%s name=%s", symbol, name)
    result = Quotes.factory(market=market).F10(symbol, name)
    if result is None or (hasattr(result, "empty") and result.empty):
        _logger.warning("f10 %s %s: 返回空", symbol, name)
        return None
    _logger.info("f10 %s %s: 成功", symbol, name)
    return result

# ---- 板块信息 ------------------------------------------------------------

def block(tofile: str = "block.dat", market: str = "std") -> Optional[pd.DataFrame]:
    """板块-成分股，4 列 (blockname/block_type/code_index/code)，
    约 38379 行，扁平展开格式。"""

    _logger.info("block tofile=%s", tofile)
    df = Quotes.factory(market=market).block(tofile)
    if df is None:
        _logger.warning("block: 返回空")
        return None
    _logger.info("block: %d 行", len(df))
    return df

def bars_batch(
    symbols: pd.DataFrame,
    frequency: int = FREQ["day"],
    start: int = 0,
    offset: int = 1,
    market: str = "std",
) -> Optional[pd.DataFrame]:
    """批量获取多只股票的 K 线数据，默认取最新 1 根 (当天)。

    Args:
        symbols: 股票代码列表，如 ['000001','600519']。
        frequency: K线周期，默认日线。
        start: 起始位置，0=最新。
        offset: 条数，默认 1 (当天)。
    """
    _logger.info("bars_batch n=%d freq=%d start=%d offset=%d",
                 len(symbols) if symbols is not None else 0, frequency, start, offset)
    frames = []
    client = Quotes.factory(market=market)
    if symbols is None or symbols.empty:
        symbols = stocks()
    total = len(symbols)
    for i, symbol in enumerate(symbols['code']):
        df = client.bars(symbol, frequency, start, offset)
        if df is not None:
            df["stock_code"] = symbol
            frames.append(df)
        else:
            _logger.warning("bars_batch %s (%d/%d): 返回空", symbol, i + 1, total)
        time.sleep(0.2)
    _logger.info("bars_batch: 成功 %d/%d", len(frames), total)
    return pd.concat(frames, ignore_index=True) if frames else None

