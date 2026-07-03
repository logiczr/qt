"""Dealer：Ingest 层决策器。

看结果决定下一步，不执行任何操作。
Pipeline 执行 Dealer 返回的 Decision，把结果喂回来，Dealer 再决策。
循环直到 Dealer 返回 done。

映射表在此维护：
- Bronze table → Silver target
- Bronze table → retry action
- retry table → Silver repair targets
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from bronze.base import IngestResult
from silver.base import BuildResult

_log = logging.getLogger(__name__)


# =====================================================================
# Decision — Dealer 的输出
# =====================================================================

@dataclass
class Decision:
    """Dealer 返回的指令，Pipeline 据此执行。"""

    type: str   # "bronze" / "retry" / "silver" / "check" / "backfill" / "done"

    # bronze
    action: str = ""
    codes: list[str] = field(default_factory=list)

    # retry
    table_key: str = ""       # "kline_daily" / "xdxr" / ...
    round: int = 1

    # silver
    target: str = ""           # "daily_kline" / "adj_factor" / ...
    mode: str = "incremental"  # "incremental" / "repair"

    # check / backfill 无额外字段


# =====================================================================
# 映射表
# =====================================================================

# Bronze table 短名 → Silver target
_TABLE_TARGET_EXACT: dict[str, str] = {
    "raw_tdx_kline_daily_bfq":   "daily_kline",
    "raw_tencent_kline_day_bfq": "daily_kline",
    "raw_tencent_minute":        "minute_kline",
    "raw_tdx_xdxr":             "adj_factor",
    "raw_tdx_finance":          "finance",
    "raw_tdx_stocks":           "stock_map",
    "raw_tencent_stocks":       "stock_map",
    "raw_tdx_f10":              "f10_doc",
}

_TABLE_TARGET_PREFIX: list[tuple[str, str]] = [
    ("raw_tdx_kline_m1_", "minute_kline"),
]

_NO_SILVER_TABLES = {
    "raw_tdx_quotes", "raw_tencent_quotes", "raw_tencent_brief",
    "raw_tdx_block", "raw_tdx_index_daily_bfq",
    "raw_tencent_money_flow", "raw_tencent_order_book",
}

# Bronze table 短名 → retry action 后缀
_TABLE_RETRY_MAP: dict[str, str] = {
    "raw_tdx_kline_daily_bfq":   "kline_daily",
    "raw_tencent_kline_day_bfq": "kline_daily",
    "raw_tdx_kline_m1":          "kline_m1",
    "raw_tencent_minute":        "kline_m1",
    "raw_tdx_xdxr":             "xdxr",
    "raw_tdx_finance":          "finance",
    "raw_tdx_f10":              "f10",
    "raw_tdx_quotes":           "quotes",
    "raw_tencent_quotes":       "quotes",
}

# retry table_key → 需要修的 Silver targets
_RETRY_REPAIR_MAP: dict[str, list[str]] = {
    "kline_daily": ["daily_kline"],
    "kline_m1": ["minute_kline"],
    "xdxr": ["adj_factor"],
    "finance": ["finance", "stock_map"],
    "f10": ["f10_doc"],
}

MAX_RETRY_ROUNDS = 2


# =====================================================================
# Dealer
# =====================================================================

class Dealer:
    """决策器：看结果决定下一步。

    Pipeline 循环调用:
        decision = dealer.start(action, codes)
        while decision.type != "done":
            results = execute(decision)
            decision = dealer.next(results)
    """

    def __init__(self):
        self._queue: deque = deque()     # 待执行的工作
        self._plan_id: str = ""
        self._shelved: list[str] = []     # 放弃的 codes
        self._has_daily_kline: bool = False  # daily_kline 是否参与构建

    # ===================================================================
    # 入口
    # ===================================================================

    def start(self, action: str, codes: list[str] | None = None) -> tuple[Decision, str]:
        """开始流程。返回 (第一步 Decision, plan_id)。"""
        self._queue.clear()
        self._shelved = []
        self._has_daily_kline = False
        self._plan_id = _plan_id(action)

        # 第一步：跑 Bronze
        self._queue.append(_Work("bronze", action=action, codes=codes or []))
        return self._next_decision(), self._plan_id

    def next(self, results: list) -> Decision:
        """处理执行结果，返回下一步 Decision。"""
        work = self._last_work

        if work.type == "bronze":
            self._after_bronze(results)
        elif work.type == "retry":
            self._after_retry(results)
        elif work.type == "silver":
            self._after_silver(results)
        elif work.type == "check":
            self._after_check(results)
        elif work.type == "backfill":
            self._after_backfill(results)

        return self._next_decision()

    @property
    def shelved_codes(self) -> list[str]:
        """被搁置的 codes（retry 耗尽仍失败的）。"""
        return self._shelved

    @property
    def has_daily_kline(self) -> bool:
        return self._has_daily_kline

    # ===================================================================
    # 各阶段结果处理
    # ===================================================================

    def _after_bronze(self, results: list[IngestResult]):
        """Bronze 完成 → 排入 retry 和 Silver。"""
        # 映射 Silver targets
        targets = set()
        for r in results:
            if r.status in ("ok", "partial") and r.rows > 0:
                t = self._match_table(r.table)
                if t:
                    targets.add(t)

        # 按表分组 failed codes
        failed = self._extract_failed_by_table(results)

        # 排入 retry
        for table_key, codes in failed.items():
            self._queue.append(_Work("retry", table_key=table_key, codes=codes, round=1))

        # 排入 Silver
        for target in sorted(targets):
            self._queue.append(_Work("silver", target=target, mode="incremental"))

    def _after_retry(self, results: list[IngestResult]):
        """retry 完成 → 排入 repair 或再试一轮。"""
        work = self._last_work
        still_failed = _extract_failed_codes(results)
        succeeded = [c for c in work.codes if c not in still_failed]

        # 成功的 → Silver repair
        if succeeded:
            for target in _RETRY_REPAIR_MAP.get(work.table_key, []):
                self._queue.appendleft(
                    _Work("silver", target=target, mode="repair", codes=succeeded))

        # 还失败的 → 再试或搁置
        if still_failed:
            if work.round < MAX_RETRY_ROUNDS:
                self._queue.appendleft(
                    _Work("retry", table_key=work.table_key,
                          codes=still_failed, round=work.round + 1))
            else:
                self._shelved.extend(still_failed)
                _log.warning("shelved %d codes after %d retries: %s",
                             len(still_failed), work.round, still_failed[:5])

    def _after_silver(self, results: list[BuildResult]):
        """Silver 完成 → 检查是否需要 completeness check。"""
        if any(r.target == "daily_kline" for r in results):
            self._has_daily_kline = True

        # 最后一个 silver 完成后，检查 completeness
        if not any(w.type == "silver" for w in self._queue):
            if self._has_daily_kline:
                self._queue.append(_Work("check", round=1))

    def _after_check(self, results):
        """completeness check 完成 → 排入 backfill 或完成。"""
        work = self._last_work
        # results 是 check_completeness() 返回的 DataFrame
        missing = results[results["missing_count"].notna() & (results["missing_count"] > 0)]
        if missing.empty:
            _log.info("completeness OK")
            return

        codes = missing["stock_code"].tolist()
        _log.warning("completeness gap: %d stocks", len(codes))
        self._queue.append(_Work("backfill", codes=codes, round=work.round))

    def _after_backfill(self, results):
        """backfill 完成 → 排入 repair + 再查一轮。"""
        work = self._last_work
        if work.codes:
            self._queue.append(
                _Work("silver", target="daily_kline", mode="repair", codes=work.codes))

        if work.round < 2:
            self._queue.append(_Work("check", round=work.round + 1))
        else:
            _log.warning("completeness check max rounds reached")

    # ===================================================================
    # 内部
    # ===================================================================

    def _next_decision(self) -> Decision:
        """从队列取出下一个工作，转为 Decision。"""
        if not self._queue:
            return Decision(type="done")

        work = self._queue.popleft()
        self._last_work = work

        if work.type == "bronze":
            return Decision(type="bronze", action=work.action, codes=work.codes)
        elif work.type == "retry":
            return Decision(type="retry", table_key=work.table_key,
                            codes=work.codes, round=work.round)
        elif work.type == "silver":
            return Decision(type="silver", target=work.target,
                            mode=work.mode, codes=work.codes)
        elif work.type == "check":
            return Decision(type="check")
        elif work.type == "backfill":
            return Decision(type="backfill", codes=work.codes)
        else:
            return Decision(type="done")

    # ---- 映射 ----

    @staticmethod
    def _match_table(full_table: str) -> str | None:
        short = full_table.split(".")[-1] if "." in full_table else full_table
        if short in _NO_SILVER_TABLES:
            return None
        if short in _TABLE_TARGET_EXACT:
            return _TABLE_TARGET_EXACT[short]
        for prefix, target in _TABLE_TARGET_PREFIX:
            if short.startswith(prefix):
                return target
        return None

    def _extract_failed_by_table(self, results: list[IngestResult]) -> dict[str, list[str]]:
        """按 table 分组提取 failed codes → retry action。"""
        raw: dict[str, set[str]] = {}
        for r in results:
            if r.status in ("partial", "fail") and r.errors:
                short = r.table.split(".")[-1] if "." in r.table else r.table
                codes = set()
                for e in r.errors:
                    if ":" in e:
                        code = e.split(":")[0].strip()
                        if code and (code.isdigit() or code.startswith("sh") or code.startswith("sz")):
                            codes.add(code)
                if codes:
                    raw.setdefault(short, set()).update(codes)

        result: dict[str, list[str]] = {}
        for short, codes in raw.items():
            retry_key = self._table_to_retry(short)
            if retry_key:
                result.setdefault(retry_key, set()).update(codes)

        return {k: sorted(v) for k, v in result.items()}

    @staticmethod
    def _table_to_retry(short_table: str) -> str | None:
        if short_table in _TABLE_RETRY_MAP:
            return _TABLE_RETRY_MAP[short_table]
        for prefix, retry_key in _TABLE_RETRY_MAP.items():
            if short_table.startswith(prefix):
                return retry_key
        return None


# =====================================================================
# 内部工作项
# =====================================================================

@dataclass
class _Work:
    """Dealer 内部工作队列项。"""
    type: str  # "bronze" / "retry" / "silver" / "check" / "backfill"
    action: str = ""
    codes: list[str] = field(default_factory=list)
    table_key: str = ""
    target: str = ""
    mode: str = "incremental"
    round: int = 1


# =====================================================================
# 工具函数
# =====================================================================

def _plan_id(action: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{action}_{now}"


def _extract_failed_codes(results: list[IngestResult]) -> list[str]:
    """从 IngestResult.errors 提取失败 codes。"""
    codes: set[str] = set()
    for r in results:
        for e in getattr(r, "errors", []):
            if ":" in e:
                code = e.split(":")[0].strip()
                if code and (code.isdigit() or code.startswith("sh") or code.startswith("sz")):
                    codes.add(code)
    return sorted(codes)
