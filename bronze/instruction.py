"""Bronze 层指令中枢。内建全部数据源，通过 IngestOrder 协议接收编排层指令。"""

import queue
import logging
import threading
from dataclasses import dataclass, field
import pandas as pd
from db import get_db
from bronze.tdx import TDXBronze, _FREQ_SUFFIX
from bronze.tencent import TencentBronze

_log = logging.getLogger(__name__)


@dataclass
class IngestOrder:
    """编排层 → 指令层协议。

    action: "fast_close" | "daily" | "weekly" | "retry" | "retry.*" | "backfill_daily" | "f10" | "refresh_v2" | "first_init"
    codes:  retry/backfill/f10 时用，根据格式自动路由（sh600519→tencent, 600519→tdx）
    """
    action: str = ""  # 同上
    codes: list[str] = field(default_factory=list)


class Bronze:
    """Bronze 层调度中枢。

    用法:
        from bronze import Bronze, IngestOrder
        bronze = Bronze()
        for r in bronze.execute(IngestOrder(action="light_daily")):
            ...
    """

    def __init__(self):
        self._tdx = TDXBronze()
        self._tencent = TencentBronze()

    # ---- 对外唯一入口 ----------------------------------------------------

    def execute(self, order: IngestOrder):
        if order.action == "fast_close":
            yield from self._fast_close()
        elif order.action == "daily":
            yield from self._daily()
        elif order.action == "weekly":
            yield from self._weekly()
        elif order.action.startswith("retry"):
            table = order.action.split(".", 1)[1] if "." in order.action else ""
            yield from self._retry(order.codes, table)
        elif order.action == "backfill_daily":
            yield from self._backfill_daily(order.codes)
        elif order.action == "f10":
            yield from self._f10(order.codes)
        elif order.action == "refresh_v2":
            yield from self._refresh_v2()
        elif order.action == "first_init":
            yield from self._first_init()
        else:
            _log.warning("unknown action: %s", order.action)

    def status(self) -> pd.DataFrame:
        return get_db().list_tables()

    def dedup(self):
        """去重：对逐只类型表，保留每组业务 key 下最新 ingested_at 的行，删除重复。"""
        db = get_db()
        # (表名, 去重 key 列)
        rules = [
            ("bronze.raw_tdx_kline_daily_bfq", ["stock_code", "datetime"]),
            (f"bronze.raw_tdx_kline_{_FREQ_SUFFIX['1m']}_bfq", ["stock_code", "datetime"]),
            ("bronze.raw_tdx_xdxr", ["code", "year", "month", "day", "category"]),
            ("bronze.raw_tdx_finance", ["code"]),
            ("bronze.raw_tencent_kline_day_bfq", ["stock_code", "date"]),
        ]
        for table, key_cols in rules:
            key_expr = ", ".join(key_cols)
            try:
                db.execute(
                    f"DELETE FROM {table} WHERE (ingested_at, {key_expr}) NOT IN ("
                    f"  SELECT MAX(ingested_at), {key_expr} FROM {table} GROUP BY {key_expr}"
                    f")",
                    mode="write",
                )
            except Exception as e:
                _log.warning("dedup %s failed: %s", table, e)

    def diagnose(self) -> pd.DataFrame:
        """扫描今日数据完整性，返回各表健康报告。

        逐只类型的表（kline/xdxr/finance/minute）会检查 code 覆盖率，
        全量类型的表（stocks/block）只检查今日是否有数据。
        """
        db = get_db()
        today = pd.Timestamp.today().strftime("%Y-%m-%d")

        # 获取预期股票列表（仅 A 股，与 _load_stocks 一致）
        try:
            stocks = db.execute(
                "SELECT code FROM bronze.raw_tdx_stocks WHERE "
                "(market = 'sz' AND (code LIKE '00%' OR code LIKE '30%')) OR "
                "(market = 'sh' AND (code LIKE '60%' OR code LIKE '68%'))",
                mode="read",
            )
            expected_tdx = set(stocks["code"].astype(str).tolist())
        except Exception:
            expected_tdx = set()
        expected_tencent = set(
            TencentBronze._to_tencent_code(c) for c in expected_tdx
        )

        # 定义检查规则: (表名, code列名, 预期code集合, 对应数据源)
        per_stock_checks = [
            ("bronze.raw_tdx_kline_daily_bfq", "stock_code", expected_tdx, "tdx"),
            (f"bronze.raw_tdx_kline_{_FREQ_SUFFIX['1m']}_bfq", "stock_code", expected_tdx, "tdx"),
            ("bronze.raw_tdx_xdxr", "code", expected_tdx, "tdx"),
            ("bronze.raw_tdx_finance", "code", expected_tdx, "tdx"),
            ("bronze.raw_tencent_kline_day_bfq", "stock_code", expected_tencent, "tencent"),
            ("bronze.raw_tencent_minute", "stock_code", expected_tencent, "tencent"),
        ]
        bulk_checks = [
            ("bronze.raw_tdx_stocks", "tdx"),
            ("bronze.raw_tdx_block", "tdx"),
            ("bronze.raw_tdx_index_daily_bfq", "tdx"),
        ]

        rows = []
        for table, code_col, expected, source in per_stock_checks:
            try:
                r = db.execute(
                    f"SELECT DISTINCT CAST({code_col} AS VARCHAR) AS code "
                    f"FROM {table} WHERE CAST(ingested_at AS DATE) = '{today}'",
                    mode="read",
                )
                present = set(r["code"].tolist())
                missing = expected - present
                rows.append({
                    "table": table,
                    "source": source,
                    "expected": len(expected),
                    "present": len(present),
                    "missing": len(missing),
                    "status": "ok" if not missing else "gap",
                    "missing_codes": sorted(missing),
                })
            except Exception:
                rows.append({
                    "table": table,
                    "source": source,
                    "expected": len(expected),
                    "present": 0,
                    "missing": len(expected),
                    "status": "missing",
                    "missing_codes": sorted(expected),
                })

        for table, source in bulk_checks:
            try:
                r = db.execute(
                    f"SELECT COUNT(*) AS cnt FROM {table} "
                    f"WHERE CAST(ingested_at AS DATE) = '{today}'",
                    mode="read",
                )
                cnt = int(r["cnt"].values[0])
                rows.append({
                    "table": table,
                    "source": source,
                    "expected": 1,
                    "present": 1 if cnt > 0 else 0,
                    "missing": 0 if cnt > 0 else 1,
                    "status": "ok" if cnt > 0 else "missing",
                    "missing_codes": [],
                })
            except Exception:
                rows.append({
                    "table": table,
                    "source": source,
                    "expected": 1,
                    "present": 0,
                    "missing": 1,
                    "status": "missing",
                    "missing_codes": [],
                })

        return pd.DataFrame(rows)

    # ---- 套餐 -----------------------------------------------------------

    def _fast_close(self):
        q = queue.Queue()

        def _tdx():
            q.put(self._tdx.get_quotes())

        def _tc():
            q.put(self._tencent.get_quotes())
            q.put(self._tencent.get_brief())

        t1 = threading.Thread(target=_tdx)
        t2 = threading.Thread(target=_tc)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        while not q.empty():
            yield q.get()

    def _daily(self):
        """每日行情：stocks + 日线 + 分钟线 + 指数 + auto_heal + dedup。"""
        q = queue.Queue()

        # 先在主线程确保股票列表就绪
        if not self._stocks_fresh():
            yield self._tdx.get_stocks()

        def _tdx():
            q.put(self._tdx.get_kline(freq="daily"))
            q.put(self._tdx.get_kline(freq="1m", offset=240))
            q.put(self._tdx.get_index_kline(freq="daily"))

        def _tc():
            q.put(self._tencent.get_kline(ktype="day", fq=""))
            q.put(self._tencent.get_minute())

        t1 = threading.Thread(target=_tdx)
        t2 = threading.Thread(target=_tc)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        while not q.empty():
            yield q.get()

        # 自动补拉缺口
        yield from self._auto_heal()

    def _weekly(self):
        """周频更新：stocks + xdxr + 板块 + 财务。stock_map 在此更新。"""
        q = queue.Queue()

        # 先在主线程拉 stocks（stock_map 依赖）
        yield self._tdx.get_stocks()

        def _tdx():
            q.put(self._tdx.get_xdxr())
            q.put(self._tdx.get_block())
            q.put(self._tdx.get_finance())

        t1 = threading.Thread(target=_tdx)
        t1.start()
        t1.join()

        while not q.empty():
            yield q.get()

    def _refresh_v2(self):
        """刷新股票列表 + 财务数据。"""
        yield self._tdx.get_stocks()
        yield self._tdx.get_finance()

    def _first_init(self):
        """初次部署：拉取日线和分钟线历史数据。

        日线：TDX offset=800 + Tencent get_kline_full（翻页回填）
        分钟线：TDX 1m 全量（get_kline_full）
        """
        q = queue.Queue()

        # 先在主线程确保股票列表就绪
        if not self._stocks_fresh():
            yield self._tdx.get_stocks()

        def _tdx():
            q.put(self._tdx.get_kline(freq="daily", offset=800))
            q.put(self._tdx.get_kline_full(freq="1m"))

        def _tc():
            q.put(self._tencent.get_kline_full(ktype="day", fq=""))

        t1 = threading.Thread(target=_tdx)
        t2 = threading.Thread(target=_tc)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        while not q.empty():
            yield q.get()

    def _retry(self, codes: list[str], table: str = ""):
        """按 table 补拉指定 codes。

        action 格式: "retry" → 只拉日线 + quotes（向后兼容）
                     "retry.kline_daily" → 只拉日线
                     "retry.kline_m1" → 只拉1m
                     "retry.xdxr" → 只拉 xdxr
                     "retry.finance" → 只拉 finance
                     "retry.f10" → 只拉 f10
                     "retry.quotes" → 只拉 quotes
                     "retry.minute" → 只拉腾讯分钟线
        """
        sh = [c for c in codes if c.startswith("sh") or c.startswith("sz")]
        tx = [c for c in codes if c.isdigit()]

        if not table:
            # 向后兼容：日线 + quotes
            if sh:
                yield self._tencent.get_kline(ktype="day", fq="", symbols=sh)
                yield self._tencent.get_quotes(symbols=sh)
            if tx:
                yield self._tdx.get_kline(freq="daily", symbols=tx)
                yield self._tdx.get_quotes(symbols=tx)
            return

        # 按 table 短名精确补拉
        if table == "kline_daily":
            if tx:
                yield self._tdx.get_kline(freq="daily", symbols=tx)
            if sh:
                yield self._tencent.get_kline(ktype="day", fq="", symbols=sh)
        elif table == "kline_m1":
            if tx:
                yield self._tdx.get_kline(freq="1m", offset=240, symbols=tx)
            if sh:
                yield self._tencent.get_minute(symbols=sh)
        elif table == "xdxr":
            if tx:
                yield self._tdx.get_xdxr(symbols=tx)
        elif table == "finance":
            if tx:
                yield self._tdx.get_finance(symbols=tx)
        elif table == "f10":
            if tx:
                yield self._tdx.get_f10(symbols=tx)
        elif table == "quotes":
            if tx:
                yield self._tdx.get_quotes(symbols=tx)
            if sh:
                yield self._tencent.get_quotes(symbols=sh)
        elif table == "minute":
            if sh:
                yield self._tencent.get_minute(symbols=sh)
        else:
            _log.warning("retry: unknown table %s, fallback to daily+quotes", table)
            if sh:
                yield self._tencent.get_kline(ktype="day", fq="", symbols=sh)
            if tx:
                yield self._tdx.get_kline(freq="daily", symbols=tx)

    def _backfill_daily(self, codes: list[str]):
        """从 IPO 日至今全量拉取指定股票日线数据（追加写入 + 去重）。"""
        sh = [c for c in codes if c.startswith("sh") or c.startswith("sz")]
        tx = [c for c in codes if c.isdigit()]

        if tx:
            yield self._tdx.get_kline_full(freq="daily", symbols=tx, force=False)
        if sh:
            yield self._tencent.get_kline_full(ktype="day", fq="", symbols=sh, force=False)

    def _f10(self, codes: list[str]):
        """F10 资料详情（DB-aware 去重：同 code+hash 不重复写入）。"""
        tx = [c for c in codes if c.isdigit()]
        if tx:
            yield self._tdx.get_f10(symbols=tx)

    # ---- 内部 -----------------------------------------------------------

    def _auto_heal(self):
        """根据 diagnose 结果自动补拉缺口。"""
        report = self.diagnose()
        gap_rows = report[report["status"] != "ok"]

        if gap_rows.empty:
            _log.info("auto_heal: all tables healthy, no gaps")
            return

        for _, row in gap_rows.iterrows():
            table = row["table"]
            source = row["source"]
            missing_codes = row["missing_codes"]

            _log.info("auto_heal: %s missing %d codes", table, len(missing_codes))

            # 全量类型的表直接重跑
            if not missing_codes:
                if "stocks" in table:
                    yield self._tdx.get_stocks()
                elif "block" in table:
                    yield self._tdx.get_block()
                elif "index" in table:
                    yield self._tdx.get_index_kline(freq="daily")
                continue

            # 逐只类型：按缺失 codes 补拉
            # TDX codes 是纯数字，Tencent codes 带 sh/sz 前缀
            if source == "tdx":
                tdx_codes = missing_codes
                tc_codes = [TencentBronze._to_tencent_code(c) for c in tdx_codes]
            else:
                tc_codes = missing_codes
                tdx_codes = [c[2:] for c in tc_codes if len(c) > 2 and c[:2] in ("sh", "sz")]

            if "kline_daily" in table and source == "tdx":
                yield self._tdx.get_kline(freq="daily", symbols=tdx_codes)
            elif "kline_m1" in table:
                yield self._tdx.get_kline(freq="1m", offset=240, symbols=tdx_codes)
            elif "xdxr" in table:
                yield self._tdx.get_xdxr(symbols=tdx_codes)
            elif "finance" in table:
                yield self._tdx.get_finance(symbols=tdx_codes)
            elif "kline_day" in table and source == "tencent":
                yield self._tencent.get_kline(ktype="day", fq="", symbols=tc_codes)
            elif "minute" in table:
                yield self._tencent.get_minute(symbols=tc_codes)

        # 补拉完去重
        self.dedup()

    def _stocks_fresh(self) -> bool:
        try:
            db = get_db()
            r = db.execute(
                "SELECT CAST(MAX(ingested_at) AS DATE) AS last_date "
                "FROM bronze.raw_tdx_stocks",
                mode="read",
            )
            return r["last_date"].values[0] == pd.Timestamp.today().date()
        except Exception:
            return False
