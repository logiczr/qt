"""silver.adj_factor 复权因子构建器。

Bronze 来源：
  - bronze.raw_tdx_xdxr（除权除息事件）

清洗逻辑：
  1. 筛选 category=1（除权除息事件）
  2. year/month/day → trade_date，和 daily_kline 对齐
  3. code → stock_code
  4. 原始 per-10-share 转为 per-share（÷10）
  5. 同日多事件合并（sum fenhong/songzhuangu/peigu）
  6. 按股票分批关联 daily_kline.pre_close 计算单次除权因子

复权因子计算：
  除权参考价 = (前收盘 - 每股分红 + 配股价×每股配股) / (1 + 每股送转 + 每股配股)
  单次因子 = 除权参考价 / 前收盘
  前复权因子(d) = ∏ single_factor(e)  for all events e where e.trade_date > d
  后复权因子(d) = 1 / ∏ single_factor(e)  for all events e where e.trade_date <= d

Silver 目标：
  stock_code     VARCHAR    股票代码
  trade_date     DATE       除权除息日
  fenhong        DOUBLE     每股分红(元)
  peigu_price    DOUBLE     配股价(元)
  songzhuangu    DOUBLE     每股送转
  peigu          DOUBLE     每股配股
  single_factor  DOUBLE     单次除权因子
"""

import logging
import time
import pandas as pd
import numpy as np
from db import get_db
from silver.base import BaseBuilder, BuildOrder, BuildResult, QualityIssue

_log = logging.getLogger(__name__)

# 分批计算 single_factor 时，每批股票数
_BATCH_SIZE = 500


class AdjFactorBuilder(BaseBuilder):
    target = "adj_factor"

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
            _log.error("adj_factor build failed: %s", e)
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

    def _read_xdxr(self, db, codes: list[str] | None = None) -> pd.DataFrame:
        """读取 xdxr category=1，标准化列名和单位。"""
        conds = ["category = 1"]
        if codes:
            in_list = ",".join(f"'{c}'" for c in codes)
            conds.append(f"code IN ({in_list})")
        where = " WHERE " + " AND ".join(conds)

        df = db.execute(
            f"SELECT code, year, month, day, fenhong, peigujia, songzhuangu, peigu "
            f"FROM bronze.raw_tdx_xdxr{where}",
            mode="read",
        )
        if df.empty:
            return df

        # year/month/day → trade_date
        df["trade_date"] = pd.to_datetime(
            df["year"].astype(str) + "-" +
            df["month"].astype(str).str.zfill(2) + "-" +
            df["day"].astype(str).str.zfill(2)
        ).dt.date

        # code → stock_code
        df.rename(columns={"code": "stock_code"}, inplace=True)

        # per-10-share → per-share（÷10）
        df["fenhong"] = pd.to_numeric(df["fenhong"], errors="coerce").fillna(0) / 10
        df["peigu_price"] = pd.to_numeric(df["peigujia"], errors="coerce").fillna(0)
        df["songzhuangu"] = pd.to_numeric(df["songzhuangu"], errors="coerce").fillna(0) / 10
        df["peigu"] = pd.to_numeric(df["peigu"], errors="coerce").fillna(0) / 10

        # 同日多事件合并（极端情况，同一只股票同日多条 category=1）
        df = df.groupby(["stock_code", "trade_date"], as_index=False).agg({
            "fenhong": "sum",
            "peigu_price": "first",
            "songzhuangu": "sum",
            "peigu": "sum",
        })

        return df

    def _compute_single_factor(self, df: pd.DataFrame, db) -> pd.DataFrame:
        """分批关联 daily_kline.pre_close 计算单次除权因子。

        按股票分批（每批 _BATCH_SIZE 只），控制 daily_kline 读取规模。
        """
        if df.empty:
            df["single_factor"] = pd.Series(dtype="float64")
            return df

        all_codes = sorted(df["stock_code"].unique())
        results = []

        for i in range(0, len(all_codes), _BATCH_SIZE):
            batch_codes = all_codes[i:i + _BATCH_SIZE]
            batch_df = df[df["stock_code"].isin(batch_codes)].copy()

            in_list = ",".join(f"'{c}'" for c in batch_codes)
            pre_close_df = db.execute(
                f"SELECT stock_code, trade_date, pre_close "
                f"FROM silver.daily_kline WHERE stock_code IN ({in_list})",
                mode="read",
            )
            pre_close_df["trade_date"] = pd.to_datetime(pre_close_df["trade_date"]).dt.date

            batch_df = batch_df.merge(
                pre_close_df[["stock_code", "trade_date", "pre_close"]],
                on=["stock_code", "trade_date"],
                how="left",
            )

            # single_factor = 除权参考价 / 前收盘
            valid = batch_df["pre_close"].notna() & (batch_df["pre_close"] > 0)
            denominator = 1 + batch_df["songzhuangu"] + batch_df["peigu"]
            safe_denom = denominator.replace(0, np.nan)

            batch_df.loc[valid, "single_factor"] = (
                (batch_df.loc[valid, "pre_close"] - batch_df.loc[valid, "fenhong"] +
                 batch_df.loc[valid, "peigu_price"] * batch_df.loc[valid, "peigu"]) /
                (batch_df.loc[valid, "pre_close"] * safe_denom[valid])
            )

            batch_df.drop(columns=["pre_close"], inplace=True)
            results.append(batch_df)

            batch_idx = i // _BATCH_SIZE + 1
            n_batches = (len(all_codes) + _BATCH_SIZE - 1) // _BATCH_SIZE
            _log.info("adj_factor compute batch %d/%d: codes=%d events=%d",
                      batch_idx, n_batches, len(batch_codes), len(batch_df))

        return pd.concat(results, ignore_index=True)

    # ---- full 模式 ---------------------------------------------------------

    def _build_full(self, db) -> tuple[int, int, list[QualityIssue]]:
        """全量重建。"""
        # 前置检查：daily_kline 必须存在
        try:
            db.execute("SELECT 1 FROM silver.daily_kline LIMIT 1", mode="read")
        except Exception:
            raise RuntimeError("silver.daily_kline 不存在，请先构建 daily_kline")

        df = self._read_xdxr(db)
        rows_read = len(df)
        if df.empty:
            _log.warning("adj_factor full: 无 category=1 数据")
            return 0, 0, []

        df = self._compute_single_factor(df, db)

        # 写入
        db.drop_table("silver.adj_factor")
        out = df[["stock_code", "trade_date", "fenhong", "peigu_price",
                   "songzhuangu", "peigu", "single_factor"]].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"])

        db.create_table("silver.adj_factor", out)
        db.execute(
            "ALTER TABLE silver.adj_factor ADD PRIMARY KEY (stock_code, trade_date)",
            mode="write",
        )
        db.execute(
            "INSERT INTO silver.adj_factor SELECT * FROM _tmp",
            mode="write", df=out,
        )

        rows_written = len(out)
        issues = self._check_quality(db)

        _log.info("adj_factor full: read=%d written=%d issues=%d",
                  rows_read, rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- incremental 模式 --------------------------------------------------

    def _build_incremental(self, db) -> tuple[int, int, list[QualityIssue]]:
        """增量：只插入新增除权事件。"""
        try:
            db.execute("SELECT 1 FROM silver.daily_kline LIMIT 1", mode="read")
        except Exception:
            raise RuntimeError("silver.daily_kline 不存在，请先构建 daily_kline")

        try:
            db.execute("SELECT 1 FROM silver.adj_factor LIMIT 1", mode="read")
        except Exception:
            _log.info("adj_factor not exists, fallback to full")
            return self._build_full(db)

        df = self._read_xdxr(db)
        rows_read = len(df)
        if df.empty:
            return 0, 0, []

        # 筛出新增事件
        existing = db.execute(
            "SELECT stock_code, trade_date FROM silver.adj_factor", mode="read"
        )
        existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
        existing_keys = set(zip(existing["stock_code"].astype(str), existing["trade_date"].astype(str)))

        df["key"] = list(zip(df["stock_code"].astype(str), df["trade_date"].astype(str)))
        new_df = df[~df["key"].isin(existing_keys)].drop(columns=["key"]).copy()

        if new_df.empty:
            _log.info("adj_factor incremental: no new events")
            return rows_read, 0, []

        new_df = self._compute_single_factor(new_df, db)

        out = new_df[["stock_code", "trade_date", "fenhong", "peigu_price",
                       "songzhuangu", "peigu", "single_factor"]].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"])

        db.execute(
            "INSERT INTO silver.adj_factor SELECT * FROM _tmp "
            "ON CONFLICT (stock_code, trade_date) DO NOTHING",
            mode="write", df=out,
        )

        rows_written = len(out)
        issues = self._check_quality(db)

        _log.info("adj_factor incremental: read=%d new=%d written=%d issues=%d",
                  rows_read, len(new_df), rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- repair 模式 -------------------------------------------------------

    def _build_repair(self, db, codes: list[str]) -> tuple[int, int, list[QualityIssue]]:
        """补数据：指定股票重算复权因子。"""
        try:
            db.execute("SELECT 1 FROM silver.daily_kline LIMIT 1", mode="read")
        except Exception:
            raise RuntimeError("silver.daily_kline 不存在，请先构建 daily_kline")

        try:
            db.execute("SELECT 1 FROM silver.adj_factor LIMIT 1", mode="read")
        except Exception:
            _log.info("adj_factor not exists, fallback to full")
            return self._build_full(db)

        df = self._read_xdxr(db, codes=codes)
        rows_read = len(df)
        if df.empty:
            _log.info("adj_factor repair: no xdxr data for %s", codes)
            return 0, 0, []

        df = self._compute_single_factor(df, db)

        out = df[["stock_code", "trade_date", "fenhong", "peigu_price",
                   "songzhuangu", "peigu", "single_factor"]].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"])

        db.execute(
            "INSERT INTO silver.adj_factor SELECT * FROM _tmp "
            "ON CONFLICT (stock_code, trade_date) DO UPDATE "
            "SET fenhong=EXCLUDED.fenhong, peigu_price=EXCLUDED.peigu_price, "
            "songzhuangu=EXCLUDED.songzhuangu, peigu=EXCLUDED.peigu, "
            "single_factor=EXCLUDED.single_factor",
            mode="write", df=out,
        )

        rows_written = len(out)
        issues = self._check_quality(db)

        _log.info("adj_factor repair: codes=%s read=%d written=%d issues=%d",
                  codes, rows_read, rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- 质量检查 ----------------------------------------------------------

    def _check_quality(self, db) -> list[QualityIssue]:
        issues = []

        # single_factor 为空（无法关联 daily_kline）
        try:
            null_count = db.execute(
                "SELECT count(*) AS c FROM silver.adj_factor WHERE single_factor IS NULL",
                mode="read",
            ).iloc[0, 0]
            if null_count > 0:
                issues.append(QualityIssue(
                    level="warning", table="silver.adj_factor",
                    stock_code="", trade_date="", column="single_factor",
                    message=f"{null_count} 条除权事件无 single_factor（daily_kline 缺对应日）",
                    raw_value=str(null_count),
                ))
        except Exception as e:
            _log.warning("quality check single_factor failed: %s", e)

        # single_factor 异常值（<=0 或 >=1.5）
        try:
            abnormal = db.execute(
                "SELECT stock_code, trade_date, single_factor "
                "FROM silver.adj_factor "
                "WHERE single_factor IS NOT NULL AND (single_factor <= 0 OR single_factor > 1.5)",
                mode="read",
            )
            for _, row in abnormal.iterrows():
                issues.append(QualityIssue(
                    level="warning", table="silver.adj_factor",
                    stock_code=row["stock_code"],
                    trade_date=str(row["trade_date"]),
                    column="single_factor",
                    message=f"single_factor 异常: {row['single_factor']:.6f}",
                    raw_value=str(row["single_factor"]),
                ))
        except Exception as e:
            _log.warning("quality check abnormal factor failed: %s", e)

        return issues
