"""silver.minute_kline 分钟线清洗构建器。

Bronze 来源：
  - bronze.raw_tdx_kline_m1_{year}_bfq（按年分表）

清洗逻辑：
  - 删冗余列：volume(=vol)、year/month/day/hour/minute
  - 删元数据列：ingested_at/source_system/source_api/batch_id
  - datetime VARCHAR → TIMESTAMP
  - 质量门：close > 0, high >= low

Silver 目标（按年分表）：
  silver.minute_kline_{year}

  stock_code     VARCHAR
  datetime       TIMESTAMP   PK
  open           DOUBLE
  high           DOUBLE
  low            DOUBLE
  close          DOUBLE
  vol            DOUBLE
  amount         DOUBLE

年份路由：从 bronze.raw_tdx_kline_m1_{year}_bfq 读 → 写 silver.minute_kline_{year}
自动检测 Bronze 层存在哪些年份的表。
"""

import logging
import time
import pandas as pd
import numpy as np
from db import get_db
from silver.base import BaseBuilder, BuildOrder, BuildResult, QualityIssue

_log = logging.getLogger(__name__)

_BATCH_SIZE = 100


class MinuteKlineBuilder(BaseBuilder):
    target = "minute_kline"

    def build(self, order: BuildOrder) -> BuildResult:
        t0 = time.perf_counter()
        db = get_db()
        self._ensure_silver_schema()

        try:
            if order.mode == "full":
                rows_read, rows_written, issues = self._build_full(db)
            elif order.mode == "incremental":
                rows_read, rows_written, issues = self._build_incremental(db)
            elif order.mode == "repair":
                if not order.codes:
                    return BuildResult(
                        target=self.target, status="skipped",
                        errors=["repair mode requires codes"],
                        elapsed_ms=int((time.perf_counter() - t0) * 1000),
                    )
                rows_read, rows_written, issues = self._build_repair(db, order.codes)
            else:
                return BuildResult(
                    target=self.target, status="skipped",
                    errors=[f"unsupported mode: {order.mode}"],
                    elapsed_ms=int((time.perf_counter() - t0) * 1000),
                )
        except Exception as e:
            _log.error("minute_kline build failed: %s", e)
            return BuildResult(
                target=self.target, status="fail",
                errors=[str(e)],
                elapsed_ms=int((time.perf_counter() - t0) * 1000),
            )

        warnings = [i.message for i in issues if i.level == "warning"]
        errors = [i.message for i in issues if i.level == "error"]
        status = "ok" if not errors else ("partial" if rows_written > 0 else "fail")

        return BuildResult(
            target=self.target,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=len(errors),
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
            errors=errors,
            warnings=warnings,
        )

    # ---- 年份检测 ----------------------------------------------------------

    @staticmethod
    def _detect_years(db) -> list[str]:
        """检测 Bronze 层存在哪些年份的分钟线表，返回年份列表。"""
        df = db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'bronze' "
            "AND table_name LIKE 'raw_tdx_kline_m1_%_bfq'",
            mode="read",
        )
        if df.empty:
            return []
        years = []
        for name in df["table_name"].astype(str):
            # raw_tdx_kline_m1_2026_bfq → 2026
            parts = name.split("_")
            # m1 后面就是年份
            try:
                idx = parts.index("m1")
                year = parts[idx + 1]
                if year.isdigit() and len(year) == 4:
                    years.append(year)
            except (ValueError, IndexError):
                continue
        return sorted(years)

    # ---- 数据读取与清洗 ----------------------------------------------------

    def _read_bronze(self, db, year: str, codes: list[str] | None = None,
                     date_filter: str = "") -> pd.DataFrame:
        """读取 Bronze 分钟线，标准化列名。

        Args:
            year: 年份，如 "2026"
            codes: 股票代码列表，为空则全读
            date_filter: 日期过滤，格式 'YYYY-MM-DD'
        """
        table = f"bronze.raw_tdx_kline_m1_{year}_bfq"
        conds = []
        if date_filter:
            conds.append(f"datetime LIKE '{date_filter}%'")
        if codes:
            in_list = ",".join(f"'{c}'" for c in codes)
            conds.append(f"stock_code IN ({in_list})")
        where = f" WHERE {' AND '.join(conds)}" if conds else ""

        df = db.execute(
            f"SELECT stock_code, datetime, open, high, low, close, vol, amount "
            f"FROM {table}{where}",
            mode="read",
        )
        return df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗：类型修正 + 质量门。"""
        # datetime → TIMESTAMP
        df["datetime"] = pd.to_datetime(df["datetime"])

        # vol 保持原始数据
        df["vol"] = pd.to_numeric(df["vol"], errors="coerce")

        # amount
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

        # OHLC
        for col in ("open", "close", "high", "low"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 质量门：close > 0, high >= low
        mask = (df["close"].notna()) & (df["close"] > 0) & (df["high"] >= df["low"])
        rejected = len(df) - mask.sum()
        df = df[mask].copy()

        df.sort_values(["stock_code", "datetime"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df._rejected = rejected
        return df

    # ---- full 模式 ---------------------------------------------------------

    def _build_full(self, db) -> tuple[int, int, list[QualityIssue]]:
        """全量重建。逐年、逐批处理。"""
        years = self._detect_years(db)
        if not years:
            _log.warning("minute_kline: no Bronze year tables found")
            return 0, 0, []

        total_read = 0
        total_written = 0
        total_rejected = 0
        all_issues = []

        for year in years:
            _log.info("minute_kline full: processing year %s", year)
            silver_table = f"silver.minute_kline_{year}"

            # 获取该年所有股票代码
            bronze_table = f"bronze.raw_tdx_kline_m1_{year}_bfq"
            codes = db.execute(
                f"SELECT DISTINCT stock_code FROM {bronze_table}",
                mode="read",
            )["stock_code"].astype(str).tolist()

            if not codes:
                continue

            # 删旧表
            db.drop_table(silver_table)

            n_batches = (len(codes) + _BATCH_SIZE - 1) // _BATCH_SIZE
            is_first = True

            for i in range(0, len(codes), _BATCH_SIZE):
                batch = codes[i : i + _BATCH_SIZE]
                raw = self._read_bronze(db, year, codes=batch)
                rows_read = len(raw)
                total_read += rows_read

                if rows_read == 0:
                    continue

                cleaned = self._clean(raw)
                rejected = getattr(cleaned, "_rejected", 0)
                total_rejected += rejected

                if cleaned.empty:
                    continue

                out = cleaned[[
                    "stock_code", "datetime",
                    "open", "high", "low", "close", "vol", "amount",
                ]].copy()

                if is_first:
                    db.create_table(silver_table, out)
                    db.execute(
                        f"ALTER TABLE {silver_table} "
                        "ADD PRIMARY KEY (stock_code, datetime)",
                        mode="write",
                    )
                    is_first = False

                db.execute(
                    f"INSERT INTO {silver_table} SELECT * FROM _tmp "
                    "ON CONFLICT (stock_code, datetime) DO NOTHING",
                    mode="write", df=out,
                )
                total_written += len(out)

                batch_idx = i // _BATCH_SIZE + 1
                _log.info("minute_kline full %s batch %d/%d: codes=%d read=%d written=%d",
                          year, batch_idx, n_batches, len(batch), rows_read, len(out))

            issues = self._check_quality(db, year, total_rejected)
            all_issues.extend(issues)

        _log.info("minute_kline full: total read=%d written=%d rejected=%d issues=%d",
                  total_read, total_written, total_rejected, len(all_issues))
        return total_read, total_written, all_issues

    # ---- incremental 模式 --------------------------------------------------

    def _build_incremental(self, db) -> tuple[int, int, list[QualityIssue]]:
        """增量：找 Bronze 中比 Silver 更新的数据。

        逻辑：
          1. Silver 表不存在 → fallback 单年 full
          2. 读 Silver 每只股票的 MAX(datetime)
          3. 从 Bronze 读比 MAX(datetime) 更新的数据
          4. 清洗后 ON CONFLICT DO NOTHING 插入
          5. Bronze 中有但 Silver 没有的股票（新股），也一起处理
        """
        years = self._detect_years(db)
        if not years:
            _log.warning("minute_kline: no Bronze year tables found")
            return 0, 0, []

        total_read = 0
        total_written = 0
        all_issues = []

        for year in years:
            silver_table = f"silver.minute_kline_{year}"

            # 检查 Silver 表是否存在
            try:
                db.execute(f"SELECT 1 FROM {silver_table} LIMIT 1", mode="read")
            except Exception:
                _log.info("minute_kline %s not exists, fallback to full for this year", year)
                r, w, iss = self._build_full_year(db, year)
                total_read += r
                total_written += w
                all_issues.extend(iss)
                continue

            # 获取 Silver 中每只股票的最大 datetime
            existing_max = db.execute(
                f"SELECT stock_code, MAX(datetime) AS max_dt "
                f"FROM {silver_table} GROUP BY stock_code",
                mode="read",
            )
            max_dict = dict(zip(
                existing_max["stock_code"].astype(str),
                pd.to_datetime(existing_max["max_dt"]),
            ))

            # 获取 Bronze 所有股票代码
            bronze_table = f"bronze.raw_tdx_kline_m1_{year}_bfq"
            bronze_codes = db.execute(
                f"SELECT DISTINCT stock_code FROM {bronze_table}",
                mode="read",
            )["stock_code"].astype(str).tolist()

            if not bronze_codes:
                continue

            # 分批处理
            n_batches = (len(bronze_codes) + _BATCH_SIZE - 1) // _BATCH_SIZE

            for i in range(0, len(bronze_codes), _BATCH_SIZE):
                batch = bronze_codes[i : i + _BATCH_SIZE]

                # 读 Bronze 全量数据（该批次股票）
                raw = self._read_bronze(db, year, codes=batch)
                if raw.empty:
                    continue

                raw["datetime"] = pd.to_datetime(raw["datetime"])

                # 过滤：只保留比 Silver 中更新的数据
                frames = []
                for code in batch:
                    code_df = raw[raw["stock_code"] == code]
                    if code_df.empty:
                        continue
                    if code in max_dict:
                        code_df = code_df[code_df["datetime"] > max_dict[code]]
                    # Silver 中没有的股票，code_df 全部保留
                    if not code_df.empty:
                        frames.append(code_df)

                if not frames:
                    continue

                new_df = pd.concat(frames, ignore_index=True)
                total_read += len(new_df)

                # 清洗（datetime 已经转过了，_clean 会再转一次，幂等）
                cleaned = self._clean(new_df)
                if cleaned.empty:
                    continue

                out = cleaned[[
                    "stock_code", "datetime",
                    "open", "high", "low", "close", "vol", "amount",
                ]].copy()

                db.execute(
                    f"INSERT INTO {silver_table} SELECT * FROM _tmp "
                    "ON CONFLICT (stock_code, datetime) DO NOTHING",
                    mode="write", df=out,
                )
                total_written += len(out)

                batch_idx = i // _BATCH_SIZE + 1
                _log.info("minute_kline incremental %s batch %d/%d: written=%d",
                          year, batch_idx, n_batches, len(out))

        issues = self._check_quality(db, years[-1] if years else "", 0)
        all_issues.extend(issues)

        _log.info("minute_kline incremental: total read=%d written=%d",
                  total_read, total_written)
        return total_read, total_written, all_issues

    # ---- repair 模式 -------------------------------------------------------

    def _build_repair(self, db, codes: list[str]) -> tuple[int, int, list[QualityIssue]]:
        """补数据：指定股票重新清洗写入，ON CONFLICT DO UPDATE。"""
        years = self._detect_years(db)
        if not years:
            _log.warning("minute_kline: no Bronze year tables found")
            return 0, 0, []

        total_read = 0
        total_written = 0
        all_issues = []

        for year in years:
            silver_table = f"silver.minute_kline_{year}"

            # 检查 Silver 表是否存在
            try:
                db.execute(f"SELECT 1 FROM {silver_table} LIMIT 1", mode="read")
            except Exception:
                _log.info("minute_kline %s not exists, fallback to full for this year", year)
                r, w, iss = self._build_full_year(db, year, codes=codes)
                total_read += r
                total_written += w
                all_issues.extend(iss)
                continue

            raw = self._read_bronze(db, year, codes=codes)
            rows_read = len(raw)
            total_read += rows_read

            if rows_read == 0:
                continue

            cleaned = self._clean(raw)
            rejected = getattr(cleaned, "_rejected", 0)

            if cleaned.empty:
                continue

            out = cleaned[[
                "stock_code", "datetime",
                "open", "high", "low", "close", "vol", "amount",
            ]].copy()

            # 先删旧数据再插入
            in_list = ",".join(f"'{c}'" for c in codes)
            db.execute(
                f"DELETE FROM {silver_table} WHERE stock_code IN ({in_list})",
                mode="write",
            )
            db.execute(
                f"INSERT INTO {silver_table} SELECT * FROM _tmp "
                "ON CONFLICT (stock_code, datetime) DO NOTHING",
                mode="write", df=out,
            )
            total_written += len(out)

        issues = self._check_quality(db, years[-1] if years else "", 0)
        all_issues.extend(issues)

        _log.info("minute_kline repair: codes=%s read=%d written=%d",
                  codes, total_read, total_written)
        return total_read, total_written, all_issues

    # ---- 单年 full（供 incremental/repair fallback）-------------------------

    def _build_full_year(self, db, year: str,
                         codes: list[str] | None = None) -> tuple[int, int, list[QualityIssue]]:
        """单年全量重建。"""
        silver_table = f"silver.minute_kline_{year}"
        bronze_table = f"bronze.raw_tdx_kline_m1_{year}_bfq"

        # 获取股票代码
        if codes:
            all_codes = codes
        else:
            all_codes = db.execute(
                f"SELECT DISTINCT stock_code FROM {bronze_table}",
                mode="read",
            )["stock_code"].astype(str).tolist()

        if not all_codes:
            return 0, 0, []

        db.drop_table(silver_table)

        total_read = 0
        total_written = 0
        total_rejected = 0
        is_first = True

        for i in range(0, len(all_codes), _BATCH_SIZE):
            batch = all_codes[i : i + _BATCH_SIZE]
            raw = self._read_bronze(db, year, codes=batch)
            rows_read = len(raw)
            total_read += rows_read

            if rows_read == 0:
                continue

            cleaned = self._clean(raw)
            rejected = getattr(cleaned, "_rejected", 0)
            total_rejected += rejected

            if cleaned.empty:
                continue

            out = cleaned[[
                "stock_code", "datetime",
                "open", "high", "low", "close", "vol", "amount",
            ]].copy()

            if is_first:
                db.create_table(silver_table, out)
                db.execute(
                    f"ALTER TABLE {silver_table} "
                    "ADD PRIMARY KEY (stock_code, datetime)",
                    mode="write",
                )
                is_first = False

            db.execute(
                f"INSERT INTO {silver_table} SELECT * FROM _tmp "
                "ON CONFLICT (stock_code, datetime) DO NOTHING",
                mode="write", df=out,
            )
            total_written += len(out)

        issues = self._check_quality(db, year, total_rejected)
        return total_read, total_written, issues

    # ---- 质量检查 ----------------------------------------------------------

    def _check_quality(self, db, year: str, rejected: int) -> list[QualityIssue]:
        issues = []

        if rejected > 0:
            issues.append(QualityIssue(
                level="warning", table=f"silver.minute_kline_{year}",
                stock_code="", trade_date="", column="",
                message=f"质量门拒绝 {rejected} 行（close<=0 或 high<low）",
                raw_value=str(rejected),
            ))

        if not year:
            return issues

        silver_table = f"silver.minute_kline_{year}"
        try:
            # 检查重复
            dup = db.execute(
                f"SELECT stock_code, datetime, COUNT(*) AS cnt "
                f"FROM {silver_table} "
                f"GROUP BY stock_code, datetime HAVING cnt > 1 "
                f"LIMIT 10",
                mode="read",
            )
            if not dup.empty:
                issues.append(QualityIssue(
                    level="error", table=silver_table,
                    stock_code="", trade_date="", column="",
                    message=f"存在 {len(dup)} 组重复 (stock_code, datetime)",
                    raw_value=str(len(dup)),
                ))
        except Exception as e:
            _log.warning("quality check failed for %s: %s", silver_table, e)

        return issues

    # ---- 完整性校验 ----------------------------------------------------------

    def check_completeness(self, codes: list[str] | None = None) -> pd.DataFrame:
        """本年度分钟线完整性校验。

        逻辑：
          1. 以数据中参考股票（600000）的交易日作为代理交易日历
          2. 对每只股票，找出交易日历中有但该股票没有的日期
          3. 同时检查每个交易日内的 bar 数量（正常应为 240）
          4. 缺失日期附带 offset：距离该股票最新数据的交易日天数

        Args:
            codes: 股票代码列表，为空则检查全市场

        Returns:
            DataFrame，列：stock_code, missing_date, offset, daily_bars(如有)
        """
        db = get_db()
        years = self._detect_years(db)
        if not years:
            _log.warning("minute_kline: no Silver tables found")
            return pd.DataFrame()

        results = []

        for year in years:
            silver_table = f"silver.minute_kline_{year}"

            try:
                db.execute(f"SELECT 1 FROM {silver_table} LIMIT 1", mode="read")
            except Exception:
                _log.warning("%s not exists, skip completeness check", silver_table)
                continue

            # 1. 代理交易日历：取 600000 的交易日
            cal = db.execute(
                f"SELECT DISTINCT CAST(datetime AS DATE) AS trade_date "
                f"FROM {silver_table} WHERE stock_code = '600000' "
                f"ORDER BY trade_date",
                mode="read",
            )
            if cal.empty:
                _log.warning("600000 无数据，无法生成代理交易日历")
                continue
            cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date
            cal_dates = sorted(cal["trade_date"].tolist())
            latest_date = cal_dates[-1]

            # 2. 获取待检查的股票列表
            stock_where = ""
            if codes:
                in_list = ",".join(f"'{c}'" for c in codes)
                stock_where = f" WHERE stock_code IN ({in_list})"

            stock_dates = db.execute(
                f"SELECT stock_code, CAST(datetime AS DATE) AS trade_date, "
                f"COUNT(*) AS bars "
                f"FROM {silver_table}{stock_where} "
                f"GROUP BY stock_code, CAST(datetime AS DATE)",
                mode="read",
            )
            stock_dates["trade_date"] = pd.to_datetime(stock_dates["trade_date"]).dt.date

            # 每只股票的交易日集合
            stock_calendar = {}
            stock_bars = {}
            for (code, date), grp in stock_dates.groupby(["stock_code", "trade_date"]):
                code_str = str(code)
                if code_str not in stock_calendar:
                    stock_calendar[code_str] = set()
                    stock_bars[code_str] = {}
                stock_calendar[code_str].add(date)
                stock_bars[code_str][date] = grp["bars"].iloc[0]

            # 每只股票的最新日期
            stock_max_date = {}
            for code, dates in stock_calendar.items():
                stock_max_date[code] = max(dates)

            # 3. 找缺失
            check_codes = codes if codes else list(stock_calendar.keys())
            for code in check_codes:
                code_str = str(code)
                actual = stock_calendar.get(code_str, set())
                max_dt = stock_max_date.get(code_str, latest_date)

                # 只检查到该股票最晚日期或日历最晚日期（取较小）
                check_end = min(max_dt, latest_date)
                expected = {d for d in cal_dates if d <= check_end}

                missing = sorted(expected - actual)
                for m_date in missing:
                    # offset: 距离最新数据的交易日天数
                    offset = sum(1 for d in cal_dates if m_date < d <= max_dt)
                    results.append({
                        "stock_code": code_str,
                        "missing_date": m_date,
                        "offset": offset,
                        "daily_bars": None,
                    })

                # 4. 检查 bar 数量不足的日期（正常 240）
                for d in sorted(actual):
                    bars = stock_bars.get(code_str, {}).get(d, 0)
                    if bars < 240:
                        # 只标记明显不足的（<200），排除收盘半日等情况
                        offset = sum(1 for dd in cal_dates if d < dd <= max_dt)
                        results.append({
                            "stock_code": code_str,
                            "missing_date": d,
                            "offset": offset,
                            "daily_bars": bars,
                        })

        if not results:
            _log.info("分钟线完整性校验：所有数据完整")
            return pd.DataFrame(columns=["stock_code", "missing_date", "offset", "daily_bars"])

        out = pd.DataFrame(results)
        _log.info("分钟线完整性校验：%d 条缺失/异常记录", len(out))
        return out
