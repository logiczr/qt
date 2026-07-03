"""腾讯财经数据源 Bronze 层 — 全市场自闭合摄入。"""

import logging
import time
import pandas as pd
from requests import RequestException
from fetch import tencent
from bronze.base import BaseBronze, IngestResult
from db import get_db

_log = logging.getLogger(__name__)


class TencentBronze(BaseBronze):
    source_name = "tencent"
    rate_limit_delay = 0.3

    # ---- K 线 ----------------------------------------------------------

    def get_kline(self, ktype: str, fq: str,
                  symbols: list[str] | None = None,
                  count: int = 1) -> IngestResult:
        """全市场 K 线增量。

        Args:
            ktype: 'day'/'week'/'month'
            fq: 'qfq'/'hfq'/''(bfq)
            symbols: 代码列表（tencent 格式: 'sh600519'），默认自动从 TDX 股票列表转换
            count: 单次拉取条数，默认 1（增量）。TCP 包长度限制，实测最大约 800。
        """
        fq_suffix = fq if fq else "bfq"
        data_type = f"kline_{ktype}_{fq_suffix}"
        table = f"bronze.raw_tencent_{data_type}"
        batch_id = self._batch_id(data_type)
        codes = symbols or self._load_codes()

        return self._loop_fetch(
            codes, data_type, table, batch_id,
            lambda code: tencent.bars(code, ktype=ktype, fq=fq, count=count),
            "bars",
            pk=["stock_code", "date"],
        )

    def get_kline_full(self, ktype: str = "day", fq: str = "",
                       symbols: list[str] | None = None,
                       force: bool = True) -> IngestResult:
        """全市场 K 线历史回填，逐只翻页至触底，批量落盘防 OOM。

        逻辑：
          1. 首轮 count=800 拉取最新数据
          2. 用本轮最早日期作为下轮 end，继续往前翻
          3. 写入前按 (stock_code, date) 去重：表中已有该日期的行则跳过
          4. 某只股票某轮拉回的日期全部已存在 → 触底，该股停止
          5. 每 500 只股票落盘一次，避免全量累积撑爆内存

        无需交易日历：腾讯 API 只返回交易日，非交易日天然不存在。

        Args:
            ktype: 'day'/'week'/'month'
            fq: 'qfq'/'hfq'/''(bfq)
            symbols: 代码列表（tencent 格式），默认全市场
            force: True=DROP 表重建；False=表已存在时跳过并返回 skipped
        """
        fq_suffix = fq if fq else "bfq"
        data_type = f"kline_{ktype}_{fq_suffix}"
        table = f"bronze.raw_tencent_{data_type}"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()

        # force=False 时：表存在则追加写入（去重），不存在则正常建表
        # force=True 时：DROP 表重建
        if force:
            db = get_db()
            db.drop_table(table)

        codes = symbols or self._load_codes()
        if not codes:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["no codes"])

        db = get_db()
        if force:
            db.drop_table(table)

        # 加载已有数据的日期集合，用于去重
        existing_dates = self._load_existing_dates(table)

        # 落盘函数（函数内闭包）
        total_rows = 0

        def _flush(frames: list[pd.DataFrame]) -> None:
            nonlocal total_rows
            if not frames:
                return
            big_df = pd.concat(frames, ignore_index=True)
            # extra 列可能是 dict/struct，转成字符串避免 DuckDB 类型不匹配
            if "extra" in big_df.columns:
                big_df["extra"] = big_df["extra"].apply(
                    lambda x: str(x) if x is not None and not isinstance(x, str) else x
                )
            big_df = self._add_metadata(big_df, batch_id, data_type)
            written = self._write(big_df, table)
            total_rows += written

        frames = []
        errors = []
        total = len(codes)
        times = []
        batch_size = 500

        for i, code in enumerate(codes):
            self._rate_limit()
            t1 = time.perf_counter()
            code_new = []
            end = ""

            while True:
                try:
                    df = tencent.bars(code, ktype=ktype, fq=fq, end=end, count=800)
                except Exception as e:
                    errors.append(f"{code}: {e}")
                    _log.warning("kline_full %s fail: %s", code, e)
                    break

                if df is None or df.empty:
                    break

                # 去重：只保留表中不存在的日期
                df["date"] = df["date"].astype(str)
                mask = ~df.apply(
                    lambda r: (code, r["date"]) in existing_dates, axis=1
                )
                new_rows = df[mask]

                if new_rows.empty:
                    # 本轮全是已有日期 → 触底
                    break

                code_new.append(new_rows)
                # 记录新日期到集合，防止后续轮次重复写入
                for _, r in new_rows.iterrows():
                    existing_dates.add((code, r["date"]))

                # 下一轮 end = 本轮最早日期
                end = df["date"].iloc[0]
                self._rate_limit()

            if code_new:
                frames.append(pd.concat(code_new, ignore_index=True))
            times.append(time.perf_counter() - t1)

            if (i + 1) % 100 == 0:
                avg_ms = sum(times) / len(times) * 1000 if times else 0
                _log.info(self._progress(i + 1, total, avg_ms))

            # 批量落盘
            if len(frames) >= batch_size:
                _flush(frames)
                frames = []

        # 剩余数据落盘
        _flush(frames)

        if total_rows == 0:
            return IngestResult(
                source=self.source_name, table=table, status="ok",
                total=total, failed=len(errors), batch_id=batch_id,
                errors=errors,
                elapsed_ms=int((time.perf_counter() - t0) * 1000),
            )

        return IngestResult(
            source=self.source_name, table=table,
            status="ok" if not errors else "partial",
            rows=total_rows, total=total, failed=len(errors),
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
            errors=errors,
        )

    # ---- 实时快照 -------------------------------------------------------

    def get_quotes(self, symbols: list[str] | None = None) -> IngestResult:
        """实时行情快照。内部 800 只/批，单次调用即可。

        Args:
            symbols: 代码列表，默认全市场
        """
        data_type = "quotes"
        table = f"bronze.raw_tencent_quotes"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()
        codes = symbols or self._load_codes()

        if not codes:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["no codes"])

        try:
            df = tencent.quotes(codes)
        except RequestException as e:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=[str(e)])

        if df is None:
            return IngestResult(source=self.source_name, table=table,
                                status="empty", batch_id=batch_id,
                                total=len(codes))

        df = self._add_metadata(df, batch_id, data_type)
        self._write(df, table)

        return IngestResult(
            source=self.source_name, table=table, status="ok",
            rows=len(df), total=len(codes), failed=0,
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
        )

    # ---- 分时 -----------------------------------------------------------

    def get_minute(self, symbols: list[str] | None = None) -> IngestResult:
        """全市场分时数据。"""
        data_type = "minute"
        table = f"bronze.raw_tencent_minute"
        batch_id = self._batch_id(data_type)
        codes = symbols or self._load_codes()

        return self._loop_fetch(
            codes, data_type, table, batch_id,
            lambda code: tencent.minute(code),
            "minute",
            pk=["stock_code", "date"],
        )

    # ---- 资金 / 盘口 / 简要 ---------------------------------------------

    def get_money_flow(self, symbols: list[str] | None = None) -> IngestResult:
        data_type = "money_flow"
        table = f"bronze.raw_tencent_money_flow"
        batch_id = self._batch_id(data_type)
        codes = symbols or self._load_codes()

        return self._loop_fetch(
            codes, data_type, table, batch_id,
            lambda code: tencent.money_flow(code),
            "money_flow",
        )

    def get_order_book(self, symbols: list[str] | None = None) -> IngestResult:
        data_type = "order_book"
        table = f"bronze.raw_tencent_order_book"
        batch_id = self._batch_id(data_type)
        codes = symbols or self._load_codes()

        return self._loop_fetch(
            codes, data_type, table, batch_id,
            lambda code: tencent.order_book(code),
            "order_book",
        )

    def get_brief(self, symbols: list[str] | None = None) -> IngestResult:
        """简要信息（轻量快照）。内部 800 只/批，单次调用即可。

        Args:
            symbols: 代码列表，默认全市场
        """
        data_type = "brief"
        table = f"bronze.raw_tencent_brief"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()
        codes = symbols or self._load_codes()

        if not codes:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["no codes"])

        try:
            df = tencent.brief(codes)
        except RequestException as e:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=[str(e)])

        if df is None:
            return IngestResult(source=self.source_name, table=table,
                                status="empty", batch_id=batch_id,
                                total=len(codes))

        df = self._add_metadata(df, batch_id, data_type)
        self._write(df, table)

        return IngestResult(
            source=self.source_name, table=table, status="ok",
            rows=len(df), total=len(codes), failed=0,
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
        )

    # ---- 内部 ----------------------------------------------------------

    def _load_existing_dates(self, table: str) -> set[tuple[str, str]]:
        """加载表中已有的 (stock_code, date) 集合，用于去重。"""
        existing = set()
        try:
            df = get_db().execute(
                f"SELECT stock_code, date FROM {table}", mode="read"
            )
            for _, row in df.iterrows():
                existing.add((str(row["stock_code"]), str(row["date"])))
        except Exception:
            pass  # 表不存在，首次运行
        return existing

    def _load_codes(self) -> list[str]:
        """从基类获取 A 股列表，转为腾讯格式。"""
        return [self._to_tencent_code(c) for c in self._load_stocks()]

    @staticmethod
    def _to_tencent_code(code: str) -> str:
        """'000001' → 'sz000001', '600519' → 'sh600519'"""
        if len(code) != 6:
            return code
        prefix = code[:2]
        if prefix in ("00", "30"):
            return f"sz{code}"
        if prefix in ("60", "68"):
            return f"sh{code}"
        return code
    from typing import Callable
    def _loop_fetch(self, codes: list[str], data_type: str, table: str,
                    batch_id: str, fetch_fn: Callable, action_name: str,
                    pk: list[str] | None = None,
                    early_stop_consecutive: int = 5,
                    early_stop_ratio: float = 0.1) -> IngestResult:
        """全市场遍历: for code → fetch → concat → write。

        Args:
            pk: 主键列名列表，传入则写入时去重（从 DataFrame 去除表中已有行）
            early_stop_consecutive: 连续失败 N 次后提前终止
            early_stop_ratio: 失败率达此比例后提前终止
        """
        t0 = time.perf_counter()
        frames = []
        errors = []
        total = len(codes)
        times = []
        consecutive_fails = 0
        early_stopped = False

        for i, code in enumerate(codes):
            self._rate_limit()

            t1 = time.perf_counter()
            try:
                df = fetch_fn(code)
                consecutive_fails = 0  # 成功则重置连续失败计数
            except (RequestException, ConnectionError, TimeoutError) as e:
                errors.append(f"{code}: {e}")
                consecutive_fails += 1
                _log.warning("%s %s fail (%d consecutive): %s",
                             action_name, code, consecutive_fails, e)
                if (i + 1) % 100 == 0:
                    avg = sum(times) / len(times) * 1000 if times else 0
                    _log.info(self._progress(i + 1, total, avg))

                fail_ratio = len(errors) / (i + 1)
                if (consecutive_fails >= early_stop_consecutive or
                        fail_ratio >= early_stop_ratio):
                    _log.warning("early_stop: consecutive=%d ratio=%.0f%% "
                                 "at %d/%d, saving %d frames",
                                 consecutive_fails, fail_ratio * 100,
                                 i + 1, total, len(frames))
                    remaining = codes[i + 1:]
                    for rc in remaining:
                        errors.append(f"{rc}: skipped (early_stop)")
                    early_stopped = True
                    break
                continue
            except Exception as e:
                errors.append(f"{code}: {e}")
                consecutive_fails += 1
                _log.warning("%s %s unexpected (%d consecutive): %s",
                             action_name, code, consecutive_fails, e)
                if (i + 1) % 100 == 0:
                    avg = sum(times) / len(times) * 1000 if times else 0
                    _log.info(self._progress(i + 1, total, avg))

                fail_ratio = len(errors) / (i + 1)
                if (consecutive_fails >= early_stop_consecutive or
                        fail_ratio >= early_stop_ratio):
                    _log.warning("early_stop: consecutive=%d ratio=%.0f%% "
                                 "at %d/%d, saving %d frames",
                                 consecutive_fails, fail_ratio * 100,
                                 i + 1, total, len(frames))
                    remaining = codes[i + 1:]
                    for rc in remaining:
                        errors.append(f"{rc}: skipped (early_stop)")
                    early_stopped = True
                    break
                continue

            times.append(time.perf_counter() - t1)

            if df is not None and not df.empty:
                frames.append(df)
            elif df is None:
                errors.append(f"{code}: returned None")
                consecutive_fails += 1

            if (i + 1) % 100 == 0:
                avg_ms = sum(times) / len(times) * 1000
                _log.info(self._progress(i + 1, total, avg_ms))

        # 写入已累积的数据（不管是否 early_stop，已拉数据先入库）
        written = 0
        if frames:
            big_df = pd.concat(frames, ignore_index=True)
            big_df = self._add_metadata(big_df, batch_id, data_type)
            written = self._write(big_df, table, pk=pk)

        if not frames and not early_stopped:
            return IngestResult(
                source=self.source_name, table=table, status="fail",
                total=total, failed=total, batch_id=batch_id,
                errors=errors,
                elapsed_ms=int((time.perf_counter() - t0) * 1000),
            )

        status = "partial" if errors else "ok"
        return IngestResult(
            source=self.source_name, table=table,
            status=status,
            rows=written, total=total, failed=len(errors),
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
            errors=errors,
        )
