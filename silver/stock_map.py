"""silver.stock_map 清洗构建器。

Bronze 来源：
  - bronze.raw_tdx_finance（code / ipo_date）— ipo_date 是核心字段
  - bronze.raw_tdx_stocks（code / name / market）— 补 name 和 market

思路：分别 read 两张表到 DataFrame，按 code 合并，写回 silver。
stock_map 就几千行，不用担心性能。

Silver 目标：
  stock_code_id  SMALLINT PK
  stock_code     VARCHAR NOT NULL UNIQUE
  stock_name     VARCHAR
  market         VARCHAR  "sh" / "sz"
  ipo_date       DATE
  delist_date    DATE
"""

import logging
import time
import pandas as pd
from db import get_db
from silver.base import BaseBuilder, BuildOrder, BuildResult, QualityIssue

_log = logging.getLogger(__name__)

# A 股 code 前缀
_A_SHARE_PREFIX = ("00", "30", "60", "68")


def _is_a_share(code: str, market: str) -> bool:
    """A 股判定：code 前缀 + 交易所必须匹配。"""
    if market == "sz":
        return code.startswith(("00", "30"))
    if market == "sh":
        return code.startswith(("60", "68"))
    return False


class StockMapBuilder(BaseBuilder):
    target = "stock_map"

    def build(self, order: BuildOrder) -> BuildResult:
        t0 = time.perf_counter()
        db = get_db()
        self._ensure_silver_schema()

        try:
            if order.mode == "full":
                rows_read, rows_written, issues = self._build_full(db)
            elif order.mode == "incremental":
                rows_read, rows_written, issues = self._build_incremental(db)
            else:
                return BuildResult(
                    target=self.target, status="skipped",
                    errors=[f"unsupported mode: {order.mode}"],
                    elapsed_ms=int((time.perf_counter() - t0) * 1000),
                )
        except Exception as e:
            _log.error("stock_map build failed: %s", e)
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

    # ---- 数据读取与合并 ----------------------------------------------------

    def _read_and_merge(self, db) -> pd.DataFrame:
        """从两张 Bronze 表读取，按 code 合并，返回 A 股 stock_map DataFrame。"""
        # 1. 读 finance → ipo_date
        finance = db.execute(
            "SELECT code, ipo_date FROM bronze.raw_tdx_finance",
            mode="read",
        )
        finance["code"] = finance["code"].astype(str).str.strip()
        # ipo_date BIGINT → DATE
        finance["ipo_date"] = finance["ipo_date"].apply(self._int_to_date)

        # 2. 读 stocks → name, market
        stocks = db.execute(
            "SELECT code, name, market FROM bronze.raw_tdx_stocks",
            mode="read",
        )
        stocks["code"] = stocks["code"].astype(str).str.strip()

        # 3. 按 code 合并：stocks 为主（有 name 和 market），finance 补 ipo_date
        df = stocks.merge(finance[["code", "ipo_date"]], on="code", how="left")

        # 4. 只保留 A 股
        df = df[df.apply(lambda r: _is_a_share(r["code"], r["market"]), axis=1)].copy()
        df.sort_values("code", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 5. 加 stock_code_id 和 delist_date
        df.insert(0, "stock_code_id", range(1, len(df) + 1))
        df["delist_date"] = None
        # 按 silver 列顺序排列
        df = df[["stock_code_id", "code", "name", "market", "ipo_date", "delist_date"]]
        df.rename(columns={"code": "stock_code", "name": "stock_name"}, inplace=True)

        return df

    @staticmethod
    def _int_to_date(val) -> pd.Timestamp | None:
        """BIGINT 19991110 → date 1999-11-10。"""
        if val is None or pd.isna(val) or val <= 0:
            return None
        s = str(int(val))
        try:
            return pd.Timestamp(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
        except (ValueError, IndexError):
            return None

    # ---- full 模式 ---------------------------------------------------------

    def _build_full(self, db) -> tuple[int, int, list[QualityIssue]]:
        """全量重建。"""
        df = self._read_and_merge(db)
        rows_read = len(df)

        db.drop_table("silver.stock_map")
        db.create_table("silver.stock_map", df)
        db.execute(
            "INSERT INTO silver.stock_map SELECT * FROM _tmp",
            mode="write", df=df,
        )

        rows_written = len(df)
        issues = self._check_quality(db)

        _log.info("stock_map full: read=%d written=%d issues=%d",
                  rows_read, rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- incremental 模式 --------------------------------------------------

    def _build_incremental(self, db) -> tuple[int, int, list[QualityIssue]]:
        """增量：只插入新增股票。"""
        df = self._read_and_merge(db)
        rows_read = len(df)

        # 检查目标表是否存在
        try:
            existing = db.execute(
                "SELECT stock_code FROM silver.stock_map", mode="read"
            )
            existing_codes = set(existing["stock_code"].astype(str))
        except Exception:
            _log.info("stock_map not exists, fallback to full")
            return self._build_full(db)

        # 筛出新增
        new_df = df[~df["stock_code"].isin(existing_codes)].copy()
        if new_df.empty:
            _log.info("stock_map incremental: no new stocks")
            return 0, 0, []

        # 重新编号 stock_code_id 从已有最大值开始
        max_id = db.execute(
            "SELECT COALESCE(MAX(stock_code_id), 0) FROM silver.stock_map",
            mode="read",
        ).iloc[0, 0]
        new_df["stock_code_id"] = range(max_id + 1, max_id + 1 + len(new_df))

        db.execute(
            "INSERT INTO silver.stock_map SELECT * FROM _tmp",
            mode="write", df=new_df,
        )

        rows_written = len(new_df)
        issues = self._check_quality(db)

        _log.info("stock_map incremental: new=%d issues=%d",
                  rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- 质量检查 ----------------------------------------------------------

    def _check_quality(self, db) -> list[QualityIssue]:
        """缺什么报什么。"""
        issues = []

        # ipo_date 为空
        try:
            missing = db.execute(
                "SELECT stock_code FROM silver.stock_map WHERE ipo_date IS NULL",
                mode="read",
            )
            for _, row in missing.iterrows():
                issues.append(QualityIssue(
                    level="warning",
                    table="silver.stock_map",
                    stock_code=row["stock_code"],
                    trade_date="",
                    column="ipo_date",
                    message=f"stock_code={row['stock_code']} ipo_date 为空，finance 数据缺失",
                    raw_value="NULL",
                ))
        except Exception as e:
            _log.warning("quality check ipo_date failed: %s", e)

        # stock_name 为空
        try:
            missing_name = db.execute(
                "SELECT stock_code FROM silver.stock_map WHERE stock_name IS NULL OR stock_name = ''",
                mode="read",
            )
            for _, row in missing_name.iterrows():
                issues.append(QualityIssue(
                    level="warning",
                    table="silver.stock_map",
                    stock_code=row["stock_code"],
                    trade_date="",
                    column="stock_name",
                    message=f"stock_code={row['stock_code']} stock_name 为空",
                    raw_value="NULL",
                ))
        except Exception as e:
            _log.warning("quality check stock_name failed: %s", e)

        # 覆盖率汇总
        try:
            total = db.execute(
                "SELECT count(*) AS c FROM silver.stock_map", mode="read"
            ).iloc[0, 0]
            missing_cnt = db.execute(
                "SELECT count(*) AS c FROM silver.stock_map WHERE ipo_date IS NULL",
                mode="read",
            ).iloc[0, 0]
            if missing_cnt > 0:
                pct = missing_cnt * 100 / total if total else 0
                issues.append(QualityIssue(
                    level="error",
                    table="silver.stock_map",
                    stock_code="",
                    trade_date="",
                    column="ipo_date",
                    message=f"finance 覆盖率不足: {total - missing_cnt}/{total} ({100-pct:.1f}%)，{missing_cnt} 只股票缺 ipo_date",
                    raw_value=f"{missing_cnt}/{total}",
                ))
        except Exception as e:
            _log.warning("quality check coverage failed: %s", e)

        return issues
