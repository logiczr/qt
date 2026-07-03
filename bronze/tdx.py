"""TDX 数据源 Bronze 层 — 全市场自闭合摄入。"""

import logging
import time
import pandas as pd
from fetch import tdx
from db import get_db
from bronze.base import BaseBronze, IngestResult

_log = logging.getLogger(__name__)

# freq 参数 → FREQ 字典 key
_FREQ_KEY = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "60m": "60m",
    "daily": "day", "day": "day",
    "weekly": "week", "week": "week",
    "monthly": "month", "month": "month",
    "quarterly": "quarter", "quarter": "quarter",
    "yearly": "year", "year": "year",
}
# freq 参数 → 表名后缀
# freq 参数 → 表名后缀（1m 按年分表，取系统年份）
_CURRENT_YEAR = str(pd.Timestamp.now().year)
_FREQ_SUFFIX = {
    "1m": f"m1_{_CURRENT_YEAR}", "5m": "m5", "15m": "m15", "30m": "m30", "60m": "m60",
    "daily": "daily", "day": "daily",
    "weekly": "weekly", "week": "weekly",
    "monthly": "monthly", "month": "monthly",
    "quarterly": "quarterly", "quarter": "quarterly",
    "yearly": "yearly", "year": "yearly",
}


class TDXBronze(BaseBronze):
    source_name = "tdx"
    rate_limit_delay = 0.2

    # ---- 股票列表 ------------------------------------------------------

    def get_stocks(self) -> IngestResult:
        """全量覆盖股票列表。"""
        data_type = "stocks"
        table = f"bronze.raw_tdx_stocks"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()

        df = tdx.stocks()
        if df is None:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["tdx.stocks() returned None"])

        df = self._add_metadata(df, batch_id, data_type)
        # 全量覆盖：删旧表重写
        db = get_db()
        db.drop_table(table)
        self._write(df, table)

        return IngestResult(
            source=self.source_name, table=table, status="ok",
            rows=len(df), total=1, failed=0,
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
        )

    # ---- K 线 ----------------------------------------------------------

    def get_kline(self, freq: str, symbols: list[str] | None = None,
                  start: int = 0, offset: int = 1) -> IngestResult:
        """K 线（默认只拉最新 1 根，增量用）。

        Args:
            freq: 'daily'/'1m'/'5m'/'15m'/'30m'/'60m'/'weekly'/'monthly'
            symbols: 代码列表，默认全市场
            start: 起始位置，0=最新
            offset: 条数，默认 1
        """
        data_type = f"kline_{_FREQ_SUFFIX[freq]}_bfq"
        table = f"bronze.raw_tdx_{data_type}"
        batch_id = self._batch_id(data_type)

        codes = symbols or self._load_stocks()
        if not codes:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["no stocks in bronze.raw_tdx_stocks"])

        return self._loop_fetch(
            codes, data_type, table, batch_id,
            lambda code: tdx.bars(symbol=code, frequency=tdx.FREQ[_FREQ_KEY[freq]],
                                  start=start, offset=offset),
            "bars",
            pk=["stock_code", "datetime"],
        )

    def get_kline_full(self, freq: str, symbols: list[str] | None = None,
                       force: bool = True) -> IngestResult:
        """全市场历史 K 线全量回填（循环分页 800 条/次，批量落盘防 OOM）。

        1m 分钟线按年分表（如 raw_tdx_kline_m1_2026_bfq），其余频率单表。
        批量写：每 500 只股票落盘一次，避免全量累积撑爆内存。

        Args:
            freq: 'daily'/'1m'/'5m'/'15m'/'30m'/'60m'/'weekly'/'monthly'
            symbols: 代码列表，默认全市场
            force: True=DROP 表重建；False=表已存在时跳过并返回 skipped
        """
        if freq == "1m":
            return self._get_kline_full_m1(symbols, force=force)

        data_type = f"kline_{_FREQ_SUFFIX[freq]}_bfq"
        table = f"bronze.raw_tdx_{data_type}"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()

        # force=False 时：表存在则追加写入（去重），不存在则正常建表
        # force=True 时：DROP 表重建
        if force:
            db = get_db()
            db.drop_table(table)

        codes = symbols or self._load_stocks()
        if not codes:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["no stocks in bronze.raw_tdx_stocks"])

        # 落盘函数（函数内闭包）
        total_rows = 0
        is_first_batch = True

        def _flush(frames: list[pd.DataFrame]) -> None:
            nonlocal total_rows, is_first_batch
            if not frames:
                return
            big_df = pd.concat(frames, ignore_index=True)
            big_df = self._add_metadata(big_df, batch_id, data_type)
            big_df.sort_values("datetime", inplace=True)
            pk = ["stock_code", "datetime"] if not force else None
            written = self._write(big_df, table, pk=pk)
            total_rows += written
            is_first_batch = False

        frames = []
        errors = []
        total = len(codes)
        times = []
        batch_size = 500  # 每 500 只股票落盘一次

        for i, code in enumerate(codes):
            self._rate_limit()
            t1 = time.perf_counter()
            code_frames = []
            start = 0
            while True:
                try:
                    df = tdx.bars(
                        symbol=code,
                        frequency=tdx.FREQ[_FREQ_KEY[freq]],
                        start=start,
                        offset=800,
                    )
                except Exception as e:
                    errors.append(f"{code}: {e}")
                    _log.warning("bars_full %s fail: %s", code, e)
                    break

                if df is None or df.empty:
                    break
                code_frames.append(df)
                if len(df) < 800:
                    break
                start += len(df)

            if code_frames:
                frames.append(pd.concat(code_frames, ignore_index=True))
            times.append(time.perf_counter() - t1)

            if (i + 1) % 100 == 0:
                avg_ms = sum(times) / len(times) * 1000
                _log.info(self._progress(i + 1, total, avg_ms))

            # 批量落盘
            if len(frames) >= batch_size:
                _flush(frames)
                frames = []

        # 剩余数据落盘
        _flush(frames)

        if total_rows == 0:
            return IngestResult(
                source=self.source_name, table=table, status="fail",
                total=total, failed=total, batch_id=batch_id,
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

    def _get_kline_full_m1(self, symbols: list[str] | None = None,
                            force: bool = True) -> IngestResult:
        """1m 分钟线全量回填，按年分表，批量落盘防 OOM。

        拉取每只股票全量分钟线，按 datetime 中的年份过滤，
        只保留当前年数据写入 raw_tdx_kline_m1_{year}_bfq。
        批量写：每 500 只股票落盘一次，避免全量累积撑爆内存。

        Args:
            symbols: 代码列表，默认全市场
            force: True=DROP 表重建；False=表已存在时跳过并返回 skipped
        """
        year = _CURRENT_YEAR
        data_type = f"kline_m1_{year}_bfq"
        table = f"bronze.raw_tdx_{data_type}"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()

        # force=False 时检查表是否已存在
        if not force:
            db = get_db()
            existing = db.execute(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema || '.' || table_name = '{table}'",
                mode="read",
            )
            if not existing.empty:
                return IngestResult(
                    source=self.source_name, table=table,
                    status="skipped", batch_id=batch_id,
                    errors=[f"{table} already exists, not init (force=False)"],
                )

        codes = symbols or self._load_stocks()
        if not codes:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["no stocks in bronze.raw_tdx_stocks"])

        db = get_db()
        if force:
            db.drop_table(table)

        # 落盘函数（函数内闭包）
        total_rows = 0

        def _flush(frames: list[pd.DataFrame]) -> None:
            nonlocal total_rows
            if not frames:
                return
            big_df = pd.concat(frames, ignore_index=True)
            big_df = self._add_metadata(big_df, batch_id, data_type)
            big_df.sort_values("datetime", inplace=True)
            self._write(big_df, table)
            total_rows += len(big_df)

        frames = []
        errors = []
        total = len(codes)
        times = []
        batch_size = 500

        for i, code in enumerate(codes):
            self._rate_limit()
            t1 = time.perf_counter()
            code_frames = []
            start = 0
            while True:
                try:
                    df = tdx.bars(
                        symbol=code,
                        frequency=tdx.FREQ[_FREQ_KEY["1m"]],
                        start=start,
                        offset=800,
                    )
                except Exception as e:
                    errors.append(f"{code}: {e}")
                    _log.warning("m1_full %s fail: %s", code, e)
                    break

                if df is None or df.empty:
                    break
                code_frames.append(df)
                if len(df) < 800:
                    break
                start += len(df)

            if code_frames:
                code_df = pd.concat(code_frames, ignore_index=True)
                # 按年份过滤，只保留当前年数据
                code_df["datetime"] = code_df["datetime"].astype(str)
                code_year = code_df[code_df["datetime"].str.startswith(year)]
                if not code_year.empty:
                    frames.append(code_year)
            times.append(time.perf_counter() - t1)

            if (i + 1) % 100 == 0:
                avg_ms = sum(times) / len(times) * 1000
                _log.info(self._progress(i + 1, total, avg_ms))

            # 批量落盘
            if len(frames) >= batch_size:
                _flush(frames)
                frames = []

        # 剩余数据落盘
        _flush(frames)

        if total_rows == 0:
            return IngestResult(
                source=self.source_name, table=table, status="fail",
                total=total, failed=total, batch_id=batch_id,
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

    def get_index_kline(self, freq: str, symbols: list[str] | None = None) -> IngestResult:
        """指数 K 线。

        Args:
            freq: 同 get_kline
            symbols: 指数代码列表，默认从 bronze.raw_tdx_stocks 读取指数代码
        """
        codes = symbols or self._load_indexes()
        data_type = f"index_{_FREQ_SUFFIX[freq]}_bfq"
        table = f"bronze.raw_tdx_{data_type}"
        batch_id = self._batch_id(data_type)

        return self._loop_fetch(
            codes, data_type, table, batch_id,
            lambda code: tdx.index_bars(code, tdx.FREQ[_FREQ_KEY[freq]], start=0, offset=1),
            "index_bars",
        )

    # ---- 基础信息 -------------------------------------------------------

    def get_xdxr(self, symbols: list[str] | None = None) -> IngestResult:
        """全市场除权除息。"""
        data_type = "xdxr"
        table = "bronze.raw_tdx_xdxr"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()

        codes = symbols or self._load_stocks()
        if not codes:
            return IngestResult(source=self.source_name, table=table,
                                status="fail", batch_id=batch_id,
                                errors=["no stocks in bronze.raw_tdx_stocks"])

        frames = []
        errors = []
        total = len(codes)
        times = []

        for i, code in enumerate(codes):
            self._rate_limit()
            t1 = time.perf_counter()
            try:
                df = tdx.xdxr(code)
            except Exception as e:
                errors.append(f"{code}: {e}")
                _log.warning("xdxr %s fail: %s", code, e)
                if (i + 1) % 100 == 0:
                    avg = sum(times) / len(times) * 1000 if times else 0
                    _log.info(self._progress(i + 1, total, avg))
                continue

            times.append(time.perf_counter() - t1)

            if df is not None and not df.empty:
                df = df.copy()
                df.insert(0, "code", code)
                frames.append(df)
            elif df is None:
                errors.append(f"{code}: returned None")

            if (i + 1) % 100 == 0:
                avg_ms = sum(times) / len(times) * 1000
                _log.info(self._progress(i + 1, total, avg_ms))

        if not frames:
            return IngestResult(
                source=self.source_name, table=table, status="fail",
                total=total, failed=total, batch_id=batch_id,
                errors=errors,
                elapsed_ms=int((time.perf_counter() - t0) * 1000),
            )

        big_df = pd.concat(frames, ignore_index=True)
        big_df = self._add_metadata(big_df, batch_id, data_type)
        self._write(big_df, table)

        return IngestResult(
            source=self.source_name, table=table,
            status="ok" if not errors else "partial",
            rows=len(big_df), total=total, failed=len(errors),
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
            errors=errors,
        )

    def get_finance(self, symbols: list[str] | None = None) -> IngestResult:
        """全市场在线财务（最新一期截面）。
           finance表中应该以 code 和 updated_date 作为主键，传入时 如果有相同的则不写入 
           如果 updated_date或者code 不同则更新数据
        """
        data_type = "finance"
        table = f"bronze.raw_tdx_finance"
        batch_id = self._batch_id(data_type)
        codes = symbols or self._load_stocks()

        return self._loop_fetch(
            codes, data_type, table, batch_id,
            lambda code: tdx.finance(code),
            "finance",
        )

    def get_block(self) -> IngestResult:
        """全量覆盖板块-成分股。"""
        data_type = "block"
        table = f"bronze.raw_tdx_block"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()

        df = tdx.block()
        if df.empty:
            return IngestResult(source=self.source_name, table=table,
                                status="empty", batch_id=batch_id,
                                total=len(df))

        df = self._add_metadata(df, batch_id, data_type)
        db = get_db()
        db.drop_table(table)
        self._write(df, table)

        return IngestResult(
            source=self.source_name, table=table, status="ok",
            rows=len(df), total=1, failed=0,
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
        )

    def get_quotes(self, symbols: list[str] | None = None) -> IngestResult:
        """全市场实时行情快照（46 列含五档盘口）。

        Args:
            symbols: 代码列表，默认全市场
        """
        data_type = "quotes"
        table = f"bronze.raw_tdx_quotes"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()
        codes = symbols or self._load_stocks()

        #if not codes:
        #    return IngestResult(source=self.source_name, table=table,
        #                        status="fail", batch_id=batch_id,
        #                        errors=["no codes"])

        frames = []
        for i in range(0, len(codes), 100):
            chunk = codes[i:i + 100]
            df = tdx.quotes(chunk)
            if not df.empty:
                frames.append(df)
            self._rate_limit()
        if not frames:
            return IngestResult(source=self.source_name, table=table,
                                status="empty", batch_id=batch_id,
                                total=len(codes))

        df = pd.concat(frames, ignore_index=True)

        df = self._add_metadata(df, batch_id, data_type)
        self._write(df, table)

        return IngestResult(
            source=self.source_name, table=table, status="ok",
            rows=len(df), total=len(codes), failed=0,
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
        )

    # ---- F10 公司资料 ---------------------------------------------------

    def get_f10(self, name: str = "", symbols: list[str] | None = None) -> IngestResult:
        """全市场 F10 资料详情。

        原始文本落文件系统 (data/bronze/f10/{code}_{YYYYMMDD}.txt)，
        文件名带拉取日期，便于追溯每天的快照版本。
        Bronze 表仅存元数据指针 (code, file_path, file_size, file_hash)。
        解析结构化字段是 Silver 层的职责。

        同内容跳过: file_hash 相同时不重复写文件和入库。

        Args:
            name: 资料类别名称，默认空=全量拉取所有类别。指定时仅拉该类。
            symbols: 代码列表，默认全市场
        """
        import os
        import hashlib
        from datetime import date

        fetch_date = date.today().strftime("%Y%m%d")
        data_type = "f10"
        table = "bronze.raw_tdx_f10"
        batch_id = self._batch_id(data_type)
        t0 = time.perf_counter()
        codes = symbols or self._load_stocks()

        file_dir = "data/bronze/f10"
        os.makedirs(file_dir, exist_ok=True)

        rows = []
        errors = []
        times = []
        skipped = 0

        # 从 DB 加载已有 code+hash，用于去重
        existing_hashes = set()
        try:
            db = get_db()
            existing_df = db.execute(
                "SELECT code, file_hash FROM bronze.raw_tdx_f10", mode="read"
            )
            existing_hashes = set(zip(existing_df["code"], existing_df["file_hash"]))
        except Exception:
            pass  # 表不存在，首次运行

        for i, code in enumerate(codes):
            self._rate_limit()
            t1 = time.perf_counter()
            try:
                result = tdx.f10(code, name)
            except Exception as e:
                errors.append(f"{code}: {e}")
                _log.warning("f10 %s fail: %s", code, e)
                if (i + 1) % 100 == 0:
                    avg = sum(times) / len(times) * 1000 if times else 0
                    _log.info(self._progress(i + 1, len(codes), avg))
                continue

            times.append(time.perf_counter() - t1)

            if result is None:
                errors.append(f"{code}: returned None")
                continue

            if isinstance(result, dict):
                # name="" 时返回所有类别的 dict
                parts = []
                for cat_name, cat_content in result.items():
                    if isinstance(cat_content, str) and cat_content.strip():
                        parts.append(f"=== {cat_name} ===\n{cat_content}")
                content = "\n\n".join(parts) if parts else ""
            elif isinstance(result, str):
                content = result
            elif isinstance(result, pd.DataFrame):
                content = result.to_csv(index=False)
            else:
                errors.append(f"{code}: unexpected type {type(result).__name__}")
                continue

            if not content.strip():
                errors.append(f"{code}: empty content")
                continue

            file_path = f"{file_dir}/{code}_{fetch_date}.txt"
            content_bytes = content.encode("utf-8")
            file_hash = hashlib.sha256(content_bytes).hexdigest()[:16]

            # 去重：DB 已有同 code+hash 则跳过（内容未变）
            if (code, file_hash) in existing_hashes:
                skipped += 1
                continue

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)

            rows.append({
                "code": code,
                "file_path": file_path,
                "file_size": len(content_bytes),
                "file_hash": file_hash,
            })
            existing_hashes.add((code, file_hash))

            if (i + 1) % 100 == 0:
                avg_ms = sum(times) / len(times) * 1000
                _log.info(self._progress(i + 1, len(codes), avg_ms))

        if not rows:
            if skipped > 0:
                return IngestResult(
                    source=self.source_name, table=table, status="ok",
                    total=len(codes), failed=len(errors), batch_id=batch_id,
                    errors=errors,
                    elapsed_ms=int((time.perf_counter() - t0) * 1000),
                )
            return IngestResult(
                source=self.source_name, table=table, status="fail",
                total=len(codes), failed=len(codes), batch_id=batch_id,
                errors=errors,
                elapsed_ms=int((time.perf_counter() - t0) * 1000),
            )

        df = pd.DataFrame(rows)
        df = self._add_metadata(df, batch_id, data_type)
        self._write(df, table)

        return IngestResult(
            source=self.source_name, table=table,
            status="ok" if not errors else "partial",
            rows=len(df), total=len(codes), failed=len(errors) + skipped,
            batch_id=batch_id,
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
            errors=errors,
        )

    def _loop_fetch(self, codes: list[str], data_type: str, table: str,
                    batch_id: str, fetch_fn, action_name: str,
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
            except Exception as e:
                errors.append(f"{code}: {e}")
                consecutive_fails += 1
                _log.warning("%s %s fail (%d consecutive): %s",
                             action_name, code, consecutive_fails, e)
                if (i + 1) % 100 == 0:
                    _log.info(self._progress(i + 1, total, sum(times) / len(times) * 1000 if times else 0))

                # 早期停止检查：连续失败 or 失败率
                fail_ratio = len(errors) / (i + 1)
                if (consecutive_fails >= early_stop_consecutive or
                        fail_ratio >= early_stop_ratio):
                    _log.warning("early_stop: consecutive=%d ratio=%.0f%% "
                                 "at %d/%d, saving %d frames",
                                 consecutive_fails, fail_ratio * 100,
                                 i + 1, total, len(frames))
                    # 剩余未尝试的 codes 写入 errors
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
