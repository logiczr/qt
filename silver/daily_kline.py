"""silver.daily_kline 清洗构建器。

Bronze 来源：
  - 主源：bronze.raw_tdx_kline_daily_bfq（不复权）
  - 备源：bronze.raw_tencent_kline_daily_bfq（不复权）

清洗逻辑：
  P0 直接映射：stock_code, trade_date, open, high, low, close, volume, amount
  P1 简单计算：pre_close（前一日 close 平移）、change_pct

多源择优：TDX > 腾讯，同日同股取 TDX。

Silver 目标：
  stock_code     VARCHAR
  trade_date     DATE
  open           DOUBLE
  high           DOUBLE
  low            DOUBLE
  close          DOUBLE
  pre_close      DOUBLE
  volume         BIGINT
  amount         DOUBLE
  change_pct     DOUBLE
"""

import logging
import time
import pandas as pd
import numpy as np
from db import get_db
from silver.base import BaseBuilder, BuildOrder, BuildResult, QualityIssue

_log = logging.getLogger(__name__)


class DailyKlineBuilder(BaseBuilder):
    target = "daily_kline"

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
            _log.error("daily_kline build failed: %s", e)
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

    # ---- 数据读取与清洗 ----------------------------------------------------

    def _read_tdx(self, db, date_filter: str = "", codes: list[str] | None = None) -> pd.DataFrame:
        """读取 TDX 日线，标准化列名。

        Args:
            date_filter: 非空时只读该日期，格式 'YYYY-MM-DD'
            codes: 非空时只读这些股票代码
        """
        conds = []
        if date_filter:
            conds.append(f"datetime LIKE '{date_filter}%'")
        if codes:
            in_list = ",".join(f"'{c}'" for c in codes)
            conds.append(f"stock_code IN ({in_list})")
        where = f" WHERE {' AND '.join(conds)}" if conds else ""
        df = db.execute(
            f"SELECT stock_code, datetime, open, close, high, low, volume, amount "
            f"FROM bronze.raw_tdx_kline_daily_bfq{where}",
            mode="read",
        )
        df["source"] = "tdx"
        df["trade_date"] = pd.to_datetime(
            df["datetime"].astype(str).str[:10]
        ).dt.date
        return df

    def _read_tencent(self, db, date_filter: str = "", codes: list[str] | None = None) -> pd.DataFrame:
        """读取腾讯日线，标准化列名。

        注意：腾讯 volume 单位为手，需 ×100 转股；
        amount 列全为空，不读取。
        表不存在时返回空 DataFrame，不影响 TDX 主源构建。

        Args:
            date_filter: 非空时只读该日期，格式 'YYYY-MM-DD'
            codes: 非空时只读这些股票代码（不带 sh/sz 前缀）
        """
        # 腾讯表可能不存在（未拉取），跳过
        try:
            db.execute("SELECT 1 FROM bronze.raw_tencent_kline_daily_bfq LIMIT 1", mode="read")
        except Exception:
            _log.warning("bronze.raw_tencent_kline_daily_bfq 不存在，跳过腾讯源")
            return pd.DataFrame()
        conds = []
        if date_filter:
            conds.append(f"date = '{date_filter}'")
        if codes:
            # 腾讯表 stock_code 带 sh/sz 前缀，需要还原
            prefixed = []
            for c in codes:
                if c.startswith(("6", "9")):
                    prefixed.append(f"'sh{c}'")
                else:
                    prefixed.append(f"'sz{c}'")
            in_list = ",".join(prefixed)
            conds.append(f"stock_code IN ({in_list})")
        where = f" WHERE {' AND '.join(conds)}" if conds else ""
        df = db.execute(
            f"SELECT stock_code, date, open, close, high, low, volume "
            f"FROM bronze.raw_tencent_kline_daily_bfq{where}",
            mode="read",
        )
        df["source"] = "tencent"
        # 腾讯 stock_code 带 sh/sz 前缀，去掉
        df["stock_code"] = df["stock_code"].astype(str).str.replace(
            r"^(sh|sz)", "", regex=True
        )
        df["trade_date"] = pd.to_datetime(df["date"].astype(str)).dt.date
        # 手 → 股
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100
        # amount 腾讯不提供，填 NaN
        df["amount"] = np.nan
        return df

    def _merge_sources(self, tdx_df: pd.DataFrame, tc_df: pd.DataFrame) -> pd.DataFrame:
        """多源合并，TDX 优先。同日同股取 TDX，腾讯补缺。"""
        if tc_df.empty:
            return tdx_df
        if tdx_df.empty:
            return tc_df

        # 找出腾讯中有但 TDX 中没有的 (stock_code, trade_date)
        tdx_keys = set(zip(tdx_df["stock_code"].astype(str), tdx_df["trade_date"].astype(str)))
        tc_df["key"] = list(zip(tc_df["stock_code"].astype(str), tc_df["trade_date"].astype(str)))
        tc_only = tc_df[~tc_df["key"].isin(tdx_keys)].drop(columns=["key"])

        if tc_only.empty:
            return tdx_df

        # 统一列顺序后合并
        cols = ["stock_code", "trade_date", "open", "close", "high", "low", "volume", "amount", "source"]
        return pd.concat([tdx_df[cols], tc_only[cols]], ignore_index=True)

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗：类型修正 + 质量门 + 计算 pre_close/change_pct。"""
        # volume 清洗：垃圾极小值(≈0)归零
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        df.loc[df["volume"] < 1, "volume"] = 0
        df["volume"] = df["volume"].astype(np.int64)

        # amount 清洗：NaN 表示数据源未提供，保留 NaN
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

        # OHLC 清洗
        for col in ("open", "close", "high", "low"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 质量门：close > 0, high >= low
        mask = (df["close"].notna()) & (df["close"] > 0) & (df["high"] >= df["low"])
        rejected = len(df) - mask.sum()
        df = df[mask].copy()

        # 按 (stock_code, trade_date) 排序，计算 pre_close
        df.sort_values(["stock_code", "trade_date"], inplace=True)
        df["pre_close"] = df.groupby("stock_code")["close"].shift(1)

        # change_pct
        df["change_pct"] = np.where(
            df["pre_close"].notna() & (df["pre_close"] > 0),
            (df["close"] - df["pre_close"]) / df["pre_close"] * 100,
            np.nan,
        )

        df.reset_index(drop=True, inplace=True)

        # change_pct
        df._rejected = rejected  # 附带拒绝行数
        return df

    # ---- full 模式 ---------------------------------------------------------

    def _build_full(self, db) -> tuple[int, int, list[QualityIssue]]:
        """全量重建。按股票分批处理，避免 OOM。"""
        # 1. 获取所有需要处理的股票代码
        tdx_codes = db.execute(
            "SELECT DISTINCT stock_code FROM bronze.raw_tdx_kline_daily_bfq",
            mode="read",
        )["stock_code"].astype(str).tolist()
        # 腾讯表可能不存在
        try:
            tc_codes = db.execute(
                "SELECT DISTINCT stock_code FROM bronze.raw_tencent_kline_daily_bfq",
                mode="read",
            )["stock_code"].astype(str).tolist()
        except Exception:
            _log.warning("bronze.raw_tencent_kline_daily_bfq 不存在，仅使用 TDX 源")
            tc_codes = []
        # 腾讯 code 去 sh/sz 前缀后合并
        tc_clean = [c.replace("sh", "").replace("sz", "") for c in tc_codes]
        all_codes = sorted(set(tdx_codes + tc_clean))

        # 2. 删旧表
        db.drop_table("silver.daily_kline")

        total_read = 0
        total_written = 0
        total_rejected = 0
        batch_size = 500
        is_first = True
        n_batches = (len(all_codes) + batch_size - 1) // batch_size

        # 3. 分批处理
        for i in range(0, len(all_codes), batch_size):
            batch = all_codes[i : i + batch_size]
            tdx_df = self._read_tdx(db, codes=batch)
            tc_df = self._read_tencent(db, codes=batch)
            rows_read = len(tdx_df) + len(tc_df)
            total_read += rows_read

            if rows_read == 0:
                continue

            merged = self._merge_sources(tdx_df, tc_df)
            cleaned = self._clean(merged)
            rejected = getattr(cleaned, "_rejected", 0)
            total_rejected += rejected

            if cleaned.empty:
                continue

            out = cleaned[[
                "stock_code", "trade_date",
                "open", "high", "low", "close", "pre_close",
                "volume", "amount", "change_pct",
            ]].copy()
            out["trade_date"] = pd.to_datetime(out["trade_date"])

            if is_first:
                db.create_table("silver.daily_kline", out)
                db.execute(
                    "ALTER TABLE silver.daily_kline "
                    "ADD PRIMARY KEY (stock_code, trade_date)",
                    mode="write",
                )
                is_first = False

            db.execute(
                "INSERT INTO silver.daily_kline SELECT * FROM _tmp "
                "ON CONFLICT (stock_code, trade_date) DO NOTHING",
                mode="write", df=out,
            )
            total_written += len(out)

            batch_idx = i // batch_size + 1
            _log.info("daily_kline full batch %d/%d: codes=%d read=%d written=%d",
                      batch_idx, n_batches, len(batch), rows_read, len(out))

        issues = self._check_quality(db, total_rejected)
        _log.info("daily_kline full: total read=%d written=%d rejected=%d issues=%d",
                  total_read, total_written, total_rejected, len(issues))
        return total_read, total_written, issues

    # ---- incremental 模式 --------------------------------------------------

    def _build_incremental(self, db) -> tuple[int, int, list[QualityIssue]]:
        """增量：只处理当日新数据。"""
        try:
            db.execute("SELECT 1 FROM silver.daily_kline LIMIT 1", mode="read")
        except Exception:
            _log.info("daily_kline not exists, fallback to full")
            return self._build_full(db)

        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        tdx_df = self._read_tdx(db, date_filter=today)
        tc_df = self._read_tencent(db, date_filter=today)
        rows_read = len(tdx_df) + len(tc_df)

        if rows_read == 0:
            return 0, 0, []

        merged = self._merge_sources(tdx_df, tc_df)
        cleaned = self._clean(merged)

        if cleaned.empty:
            _log.info("daily_kline incremental: no new data after cleaning")
            return rows_read, 0, []

        out = cleaned[[
            "stock_code", "trade_date",
            "open", "high", "low", "close", "pre_close",
            "volume", "amount", "change_pct",
        ]].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"])

        db.execute(
            "INSERT INTO silver.daily_kline SELECT * FROM _tmp "
            "ON CONFLICT (stock_code, trade_date) DO NOTHING",
            mode="write", df=out,
        )

        rows_written = len(out)
        issues = self._check_quality(db, 0)

        _log.info("daily_kline incremental: read=%d written=%d issues=%d",
                  rows_read, rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- repair 模式 -------------------------------------------------------

    def _build_repair(self, db, codes: list[str]) -> tuple[int, int, list[QualityIssue]]:
        """补数据：指定股票从 IPO 日期至今清洗写入。"""
        # 检查目标表是否存在
        try:
            db.execute("SELECT 1 FROM silver.daily_kline LIMIT 1", mode="read")
        except Exception:
            _log.info("daily_kline not exists, fallback to full")
            return self._build_full(db)

        tdx_df = self._read_tdx(db, codes=codes)
        tc_df = self._read_tencent(db, codes=codes)
        rows_read = len(tdx_df) + len(tc_df)

        if rows_read == 0:
            _log.info("daily_kline repair: no Bronze data for %s", codes)
            return 0, 0, []

        merged = self._merge_sources(tdx_df, tc_df)
        cleaned = self._clean(merged)
        rejected = getattr(cleaned, "_rejected", 0)

        if cleaned.empty:
            return rows_read, 0, []

        out = cleaned[[
            "stock_code", "trade_date",
            "open", "high", "low", "close", "pre_close",
            "volume", "amount", "change_pct",
        ]].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"])

        db.execute(
            "INSERT INTO silver.daily_kline SELECT * FROM _tmp "
            "ON CONFLICT (stock_code, trade_date) DO NOTHING",
            mode="write", df=out,
        )

        rows_written = len(out)
        issues = self._check_quality(db, rejected)

        _log.info("daily_kline repair: codes=%s read=%d written=%d issues=%d",
                  codes, rows_read, rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- 质量检查 ----------------------------------------------------------

    def _check_quality(self, db, rejected: int) -> list[QualityIssue]:
        issues = []

        if rejected > 0:
            issues.append(QualityIssue(
                level="warning", table="silver.daily_kline",
                stock_code="", trade_date="", column="",
                message=f"质量门拒绝 {rejected} 行（close<=0 或 high<low）",
                raw_value=str(rejected),
            ))

        # pre_close 为空（首日无前值，正常）
        try:
            cnt = db.execute(
                "SELECT count(*) AS c FROM silver.daily_kline WHERE pre_close IS NULL",
                mode="read",
            ).iloc[0, 0]
            total = db.execute(
                "SELECT count(*) AS c FROM silver.daily_kline", mode="read"
            ).iloc[0, 0]
            if cnt > 0:
                issues.append(QualityIssue(
                    level="info", table="silver.daily_kline",
                    stock_code="", trade_date="", column="pre_close",
                    message=f"pre_close 为空 {cnt}/{total} 行（首日或增量无前值）",
                    raw_value=f"{cnt}/{total}",
                ))
        except Exception as e:
            _log.warning("quality check pre_close failed: %s", e)

        return issues

    # ---- 完整性审查 --------------------------------------------------------

    def check_completeness(self, codes: list[str] | None = None) -> pd.DataFrame:
        """完整性审查：依据 IPO 日期检查日线数据是否完整。

        Args:
            codes: 股票代码列表，格式同 stock_map（如 ["600000", "000001"]）。
                   为空则检查全市场。

        逻辑：
          1. 从 silver.stock_map 获取 ipo_date
          2. 从 silver.daily_kline 提取所有交易日作为代理交易日历
          3. SQL LEFT JOIN 差集：预期有但实际没有的 (stock_code, trade_date)
          4. 返回缺失报告

        已知局限：
          - 暂无独立交易日历，以 daily_kline 已有交易日为代理
            若某日全市场无数据则不会被检测为缺失
          - 无法区分"数据缺失"与"停牌"，停牌日也会被标记为缺失
          - 无 ipo_date 的股票无法审查，单独列出

        Returns:
            DataFrame，列：stock_code, ipo_date, expected_days,
            actual_days, missing_count, missing_dates
            无 ipo_date 的股票会出现在末尾，ipo_date 列为 NaN
        """
        db = get_db()

        # 检查前置条件
        try:
            db.execute("SELECT 1 FROM silver.daily_kline LIMIT 1", mode="read")
        except Exception:
            _log.warning("daily_kline 表不存在，无法审查")
            return pd.DataFrame()

        try:
            db.execute("SELECT 1 FROM silver.stock_map LIMIT 1", mode="read")
        except Exception:
            _log.warning("stock_map 表不存在，无法审查")
            return pd.DataFrame()

        # 构建股票过滤条件
        stock_where = ""
        if codes:
            in_list = ",".join(f"'{c}'" for c in codes)
            stock_where = f" AND s.stock_code IN ({in_list})"

        # 1. SQL 差集：找出所有缺失的 (stock_code, trade_date)
        #    以参考股票（600000 + 000001）的交易日作为代理交易日历
        #    这两只大盘蓝筹几乎全勤，交易日覆盖最完整
        missing_df = db.execute(f"""
            WITH cal AS (
                SELECT DISTINCT trade_date FROM silver.daily_kline
                WHERE stock_code IN ('600000', '000001')
            ),
            expected AS (
                SELECT s.stock_code, c.trade_date
                FROM silver.stock_map s
                JOIN cal c ON c.trade_date >= s.ipo_date
                WHERE s.ipo_date IS NOT NULL{stock_where}
            )
            SELECT e.stock_code, e.trade_date AS missing_date
            FROM expected e
            LEFT JOIN silver.daily_kline k
                ON e.stock_code = k.stock_code AND e.trade_date = k.trade_date
            WHERE k.stock_code IS NULL
            ORDER BY e.stock_code, e.trade_date
        """, mode="read")

        results = []
        actual_dict: dict[str, int] = {}

        # 2. 按股票汇总
        if not missing_df.empty:
            missing_df["missing_date"] = pd.to_datetime(
                missing_df["missing_date"]
            ).dt.date

            # ipo_date 映射
            ipo_where = ""
            if codes:
                in_list = ",".join(f"'{c}'" for c in codes)
                ipo_where = f" AND stock_code IN ({in_list})"
            ipo_map = db.execute(
                "SELECT stock_code, CAST(ipo_date AS DATE) AS ipo_date "
                f"FROM silver.stock_map WHERE ipo_date IS NOT NULL{ipo_where}",
                mode="read",
            )
            ipo_map["ipo_date"] = pd.to_datetime(ipo_map["ipo_date"]).dt.date
            ipo_dict = dict(zip(ipo_map["stock_code"].astype(str), ipo_map["ipo_date"]))

            # 每只股票实际天数
            kline_where = ""
            if codes:
                in_list = ",".join(f"'{c}'" for c in codes)
                kline_where = f" WHERE stock_code IN ({in_list})"
            actual_counts = db.execute(
                f"SELECT stock_code, COUNT(*) AS actual_days "
                f"FROM silver.daily_kline{kline_where} GROUP BY stock_code",
                mode="read",
            )
            actual_dict = dict(
                zip(actual_counts["stock_code"].astype(str), actual_counts["actual_days"])
            )

            for code, grp in missing_df.groupby("stock_code"):
                code_str = str(code)
                dates = sorted(grp["missing_date"].tolist())
                actual = actual_dict.get(code_str, 0)
                expected = actual + len(dates)
                results.append({
                    "stock_code": code_str,
                    "ipo_date": ipo_dict.get(code_str),
                    "expected_days": expected,
                    "actual_days": actual,
                    "missing_count": len(dates),
                    "missing_dates": dates,
                })

        # 3. 无 ipo_date 的股票：无法审查，单独标记
        no_ipo_where = "WHERE ipo_date IS NULL"
        if codes:
            in_list = ",".join(f"'{c}'" for c in codes)
            no_ipo_where += f" AND stock_code IN ({in_list})"
        no_ipo = db.execute(
            f"SELECT stock_code FROM silver.stock_map {no_ipo_where} "
            "AND stock_code IN (SELECT DISTINCT stock_code FROM silver.daily_kline)",
            mode="read",
        )
        if not no_ipo.empty:
            # 补查 actual_dict（若上面没查过）
            if not actual_dict:
                kline_where = ""
                if codes:
                    in_list = ",".join(f"'{c}'" for c in codes)
                    kline_where = f" WHERE stock_code IN ({in_list})"
                actual_counts = db.execute(
                    f"SELECT stock_code, COUNT(*) AS actual_days "
                    f"FROM silver.daily_kline{kline_where} GROUP BY stock_code",
                    mode="read",
                )
                actual_dict = dict(
                    zip(actual_counts["stock_code"].astype(str), actual_counts["actual_days"])
                )
            for code in no_ipo["stock_code"].astype(str):
                results.append({
                    "stock_code": code,
                    "ipo_date": None,
                    "expected_days": None,
                    "actual_days": actual_dict.get(code, 0),
                    "missing_count": None,
                    "missing_dates": "无 ipo_date，无法审查",
                })

        if not results:
            _log.info("完整性审查：所有有 ipo_date 的股票日线数据完整")
            return pd.DataFrame(columns=[
                "stock_code", "ipo_date", "expected_days",
                "actual_days", "missing_count", "missing_dates",
            ])

        out = pd.DataFrame(results)
        if not missing_df.empty:
            _log.info("完整性审查：%d 只股票存在缺失，共 %d 条缺失记录",
                      len(out[out["missing_count"].notna()]), missing_df.shape[0])
        return out
