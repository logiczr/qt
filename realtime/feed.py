"""Realtime 层行情逻辑 — 盘中实时数据的唯一入口。

本机调用直接 import，内网调用通过 api.py 走 HTTP/WebSocket。
所有行情逻辑只写在这里，api.py 不写任何逻辑。

核心设计：
- 内存常驻一个全市场 DataFrame，轮询拉到新数据直接替换
- 一个轮询循环，所有消费者读同一份 DataFrame
- WebSocket 订阅者：每轮轮询完成后触发回调推送
- REST 查询者：不管，等他们自己来拉内存中的 DataFrame
"""

import logging
import threading
import time
from datetime import datetime, time as dtime
from typing import Callable
from uuid import uuid4

import pandas as pd

_log = logging.getLogger(__name__)


# ---- 工具函数 ----

def to_tencent_code(code: str) -> str:
    """纯数字代码转腾讯格式：'000001' → 'sz000001', '600519' → 'sh600519'"""
    if len(code) != 6 or not code.isdigit():
        return code
    prefix = code[:2]
    if prefix in ("00", "30"):
        return f"sz{code}"
    if prefix in ("60", "68"):
        return f"sh{code}"
    return code


def _load_codes_from_db() -> list[str]:
    """从 DuckDB 读 A 股列表，转腾讯格式。"""
    try:
        from db import get_db
        db = get_db()
        r = db.execute(
            "SELECT code FROM bronze.raw_tdx_stocks WHERE "
            "(market = 'sz' AND (code LIKE '00%' OR code LIKE '30%')) OR "
            "(market = 'sh' AND (code LIKE '60%' OR code LIKE '68%'))",
            mode="read",
        )
        codes = r["code"].astype(str).tolist()
        if codes:
            return [to_tencent_code(c) for c in codes]
    except Exception:
        _log.debug("load codes from DB failed, will try fetch")
    return []


def _load_codes_from_fetch() -> list[str]:
    """实时拉 TDX 股票列表，过滤 A 股，转腾讯格式。"""
    try:
        from fetch import tdx
        df = tdx.stocks()
        if df is not None and not df.empty:
            mask = (
                (df["market"] == "sz") & (df["code"].str.startswith(("00", "30"))) |
                (df["market"] == "sh") & (df["code"].str.startswith(("60", "68")))
            )
            codes = df.loc[mask, "code"].astype(str).tolist()
            return [to_tencent_code(c) for c in codes]
    except Exception:
        _log.warning("load codes from fetch failed")
    return []


def _load_codes() -> list[str]:
    """获取 A 股代码列表（腾讯格式）。DB 优先，回退到实时拉取。"""
    codes = _load_codes_from_db()
    if codes:
        _log.info("loaded %d codes from DB", len(codes))
        return codes
    codes = _load_codes_from_fetch()
    if codes:
        _log.info("loaded %d codes from fetch", len(codes))
        return codes
    _log.error("cannot load stock codes from any source")
    return []


# ---- Feed 类 ----

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
        self._df: pd.DataFrame | None = None
        self._lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None
        self._subscribers: dict[str, dict] = {}  # {id: {codes: set, callback: fn}}
        self._codes: list[str] = []
        self._fail_count = 0

    def start(self):
        """启动轮询。盘中自动运行，盘前盘后自动暂停。"""
        if self._running:
            return
        self._codes = _load_codes()
        if not self._codes:
            _log.error("no stock codes, feed not started")
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        _log.info("feed started, %d codes, interval=%.1fs", len(self._codes), self._interval)

    def stop(self):
        """停止轮询。"""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        _log.info("feed stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    # ---- 读取 ----

    def get_snapshot(self, codes: list[str] | None = None) -> pd.DataFrame:
        """获取当前快照。直接返回内存中 _df 的切片，零延迟。

        codes 支持两种格式：sz000001 或 000001，内部统一转纯数字匹配。
        """
        with self._lock:
            if self._df is None:
                return pd.DataFrame()
            if codes is None:
                return self._df.copy()
            plain = [self._to_plain(c) for c in codes]
            return self._df[self._df["code"].isin(plain)].copy()

    def get_one(self, code: str) -> pd.Series | None:
        """获取单只股票快照。无数据返回 None。"""
        with self._lock:
            if self._df is None:
                return None
            rows = self._df[self._df["code"] == self._to_plain(code)]
            return rows.iloc[0] if not rows.empty else None

    # ---- 订阅 ----

    def subscribe(self, codes: list[str], callback: Callable[[pd.DataFrame], None]) -> str:
        """订阅股票更新。每轮轮询完成后触发 callback，传入筛选后的 DataFrame。

        codes 支持两种格式：sz000001 或 000001，内部统一转纯数字匹配。

        Returns: subscription_id，用于取消订阅
        """
        sub_id = uuid4().hex[:8]
        self._subscribers[sub_id] = {
            "codes": set(self._to_plain(c) for c in codes) if codes else None,
            "callback": callback,
        }
        _log.info("subscribed %s, %s", sub_id,
                  f"{len(codes)} codes" if codes else "all")
        return sub_id

    def unsubscribe(self, subscription_id: str):
        """取消订阅。"""
        self._subscribers.pop(subscription_id, None)

    # ---- 内部 ----

    @staticmethod
    def _to_plain(code: str) -> str:
        """腾讯格式转纯数字：'sz000001' → '000001'，'000001' 不变。"""
        if len(code) == 8 and code[:2] in ("sh", "sz"):
            return code[2:]
        return code

    def _poll_loop(self):
        """轮询主循环（后台线程）。"""
        from fetch import tencent

        while self._running:
            if not self._is_market_open():
                time.sleep(60)
                continue

            try:
                df = tencent.quotes(codes=self._codes)
                if df is not None:
                    with self._lock:
                        self._df = df
                    self._fail_count = 0
                    self._notify_subscribers(df)
                else:
                    self._fail_count += 1
                    if self._fail_count <= 3:
                        _log.warning("poll returned None, fail_count=%d", self._fail_count)
                    elif self._fail_count == 10:
                        _log.error("poll failed %d times consecutively", self._fail_count)
            except Exception:
                self._fail_count += 1
                _log.exception("poll failed, fail_count=%d", self._fail_count)

            time.sleep(self._interval)

    def _notify_subscribers(self, df: pd.DataFrame):
        """通知所有订阅者。codes 为 None 时推全量。"""
        for sub in list(self._subscribers.values()):
            try:
                if sub["codes"] is None:
                    sub["callback"](df)
                else:
                    filtered = df[df["code"].isin(sub["codes"])]
                    if not filtered.empty:
                        sub["callback"](filtered)
            except Exception:
                _log.exception("subscriber callback failed")

    @staticmethod
    def _is_market_open() -> bool:
        """判断当前是否在交易时段。比实际时段宽5分钟，留启动余量。"""
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        t = now.time()
        res = (dtime(9, 25) <= t <= dtime(11, 35)) or \
               (dtime(12, 55) <= t <= dtime(15, 5))
        return res


# ---- 全局单例 ----

_feed: Feed | None = None


def get_feed() -> Feed:
    """获取全局 Feed 单例。"""
    global _feed
    if _feed is None:
        _feed = Feed()
    return _feed


def start_feed(interval: float = 0.3):
    """启动全局 Feed。"""
    feed = get_feed()
    feed._interval = interval
    feed.start()


def stop_feed():
    """停止全局 Feed。"""
    get_feed().stop()


# ---- 便捷函数 ----

def get_snapshot(codes: list[str] | None = None) -> pd.DataFrame:
    """获取当前快照。"""
    return get_feed().get_snapshot(codes)


def subscribe(codes: list[str], callback: Callable) -> str:
    """订阅更新。"""
    return get_feed().subscribe(codes, callback)


def unsubscribe(subscription_id: str):
    """取消订阅。"""
    get_feed().unsubscribe(subscription_id)
