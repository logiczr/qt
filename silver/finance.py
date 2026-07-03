"""silver.finance 财务数据清洗构建器。

Bronze 来源：
  - bronze.raw_tdx_finance

清洗逻辑：
  - 去重：同一 (code, updated_date) 只保留最新一条
  - 重命名：code → stock_code
  - 日期转换：updated_date / ipo_date 从 YYYYMMDD 整数 → DATE
  - 删元数据列：ingested_at / source_system / source_api / batch_id
  - 删无用列：market / province / industry / baoliu2

Silver 目标：
  stock_code       VARCHAR   PK
  report_date      DATE      PK  (原 updated_date)
  ipo_date         DATE
  liutongguben     DOUBLE
  zongguben        DOUBLE
  guojiagu         DOUBLE
  faqirenfarengu   DOUBLE
  farengu          DOUBLE
  bgu              DOUBLE
  hgu              DOUBLE
  zhigonggu        DOUBLE
  zongzichan       DOUBLE
  liudongzichan    DOUBLE
  gudingzichan     DOUBLE
  wuxingzichan     DOUBLE
  gudongrenshu     DOUBLE
  liudongfuzhai    DOUBLE
  changqifuzhai    DOUBLE
  zibengongjijin   DOUBLE
  jingzichan       DOUBLE
  zhuyingshouru     DOUBLE
  zhuyinglirun     DOUBLE
  yingshouzhangkuan DOUBLE
  yingyelirun      DOUBLE
  touzishouyu      DOUBLE
  jingyingxianjinliu DOUBLE
  zongxianjinliu   DOUBLE
  cunhuo           DOUBLE
  lirunzonghe      DOUBLE
  shuihoulirun     DOUBLE
  jinglirun        DOUBLE
  weifenpeilirun   DOUBLE
  meigujingzichan  DOUBLE
"""

import logging
import time
import pandas as pd
import numpy as np
from db import get_db
from silver.base import BaseBuilder, BuildOrder, BuildResult, QualityIssue

_log = logging.getLogger(__name__)

# 保留的业务列（从 Bronze 读取的列）
_KEEP_COLS = [
    "code", "updated_date", "ipo_date",
    "liutongguben", "zongguben", "guojiagu", "faqirenfarengu", "farengu",
    "bgu", "hgu", "zhigonggu",
    "zongzichan", "liudongzichan", "gudingzichan", "wuxingzichan",
    "gudongrenshu",
    "liudongfuzhai", "changqifuzhai", "zibengongjijin", "jingzichan",
    "zhuyingshouru", "zhuyinglirun", "yingshouzhangkuan", "yingyelirun",
    "touzishouyu", "jingyingxianjinliu", "zongxianjinliu", "cunhuo",
    "lirunzonghe", "shuihoulirun", "jinglirun", "weifenpeilirun",
    "meigujingzichan",
]


class FinanceBuilder(BaseBuilder):
    target = "finance"

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
            _log.error("finance build failed: %s", e)
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

    def _read_bronze(self, db, codes: list[str] | None = None) -> pd.DataFrame:
        """读取 Bronze finance 表。"""
        where = ""
        if codes:
            in_list = ",".join(f"'{c}'" for c in codes)
            where = f" WHERE code IN ({in_list})"

        cols = ", ".join(_KEEP_COLS)
        df = db.execute(
            f"SELECT {cols} FROM bronze.raw_tdx_finance{where}",
            mode="read",
        )
        return df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗：去重 + 日期转换 + 重命名。"""
        # 数值类型修正
        num_cols = [c for c in _KEEP_COLS if c not in ("code", "updated_date", "ipo_date")]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 去重：同一 (code, updated_date) 保留最后一条
        df.drop_duplicates(subset=["code", "updated_date"], keep="last", inplace=True)

        # 日期转换：YYYYMMDD 整数 → DATE
        df["updated_date"] = pd.to_datetime(
            df["updated_date"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.date
        df["ipo_date"] = pd.to_datetime(
            df["ipo_date"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.date

        # 过滤 report_date 无效的行
        df = df[df["updated_date"].notna()].copy()

        # 重命名
        df.rename(columns={"code": "stock_code", "updated_date": "report_date"}, inplace=True)

        # 排序
        df.sort_values(["stock_code", "report_date"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    # ---- full 模式 ---------------------------------------------------------

    def _build_full(self, db) -> tuple[int, int, list[QualityIssue]]:
        """全量重建。"""
        raw = self._read_bronze(db)
        rows_read = len(raw)
        if rows_read == 0:
            return 0, 0, []

        cleaned = self._clean(raw)
        rows_written = len(cleaned)

        db.drop_table("silver.finance")
        db.create_table("silver.finance", cleaned)
        db.execute(
            "ALTER TABLE silver.finance "
            "ADD PRIMARY KEY (stock_code, report_date)",
            mode="write",
        )
        db.execute(
            "INSERT INTO silver.finance SELECT * FROM _tmp",
            mode="write", df=cleaned,
        )

        issues = self._check_quality(db)
        _log.info("finance full: read=%d written=%d issues=%d",
                  rows_read, rows_written, len(issues))
        return rows_read, rows_written, issues

    # ---- incremental 模式 --------------------------------------------------

    def _build_incremental(self, db) -> tuple[int, int, list[QualityIssue]]:
        """增量：finance 每次全量刷新，等同 full。"""
        return self._build_full(db)
        return rows_read, rows_written, issues

    # ---- repair 模式 -------------------------------------------------------

    def _build_repair(self, db, codes: list[str]) -> tuple[int, int, list[QualityIssue]]:
        """补数据：指定股票重新清洗写入。"""
        try:
            db.execute("SELECT 1 FROM silver.finance LIMIT 1", mode="read")
        except Exception:
            _log.info("finance not exists, fallback to full")
            return self._build_full(db)

        raw = self._read_bronze(db, codes=codes)
        rows_read = len(raw)
        if rows_read == 0:
            return 0, 0, []

        cleaned = self._clean(raw)

        # 删旧数据
        in_list = ",".join(f"'{c}'" for c in codes)
        db.execute(
            f"DELETE FROM silver.finance WHERE stock_code IN ({in_list})",
            mode="write",
        )

        db.execute(
            "INSERT INTO silver.finance SELECT * FROM _tmp "
            "ON CONFLICT (stock_code, report_date) DO NOTHING",
            mode="write", df=cleaned,
        )

        rows_written = len(cleaned)
        issues = self._check_quality(db)
        _log.info("finance repair: codes=%s read=%d written=%d",
                  codes, rows_read, rows_written)
        return rows_read, rows_written, issues

    # ---- 质量检查 ----------------------------------------------------------

    def _check_quality(self, db) -> list[QualityIssue]:
        issues = []

        try:
            # 检查 report_date 为空
            null_date = db.execute(
                "SELECT COUNT(*) AS c FROM silver.finance WHERE report_date IS NULL",
                mode="read",
            ).iloc[0, 0]
            if null_date > 0:
                issues.append(QualityIssue(
                    level="warning", table="silver.finance",
                    stock_code="", trade_date="", column="report_date",
                    message=f"report_date 为空 {null_date} 条",
                    raw_value=str(null_date),
                ))

            # 检查重复
            dup = db.execute(
                "SELECT stock_code, report_date, COUNT(*) AS cnt "
                "FROM silver.finance GROUP BY stock_code, report_date "
                "HAVING cnt > 1 LIMIT 10",
                mode="read",
            )
            if not dup.empty:
                issues.append(QualityIssue(
                    level="error", table="silver.finance",
                    stock_code="", trade_date="", column="",
                    message=f"存在 {len(dup)} 组重复 (stock_code, report_date)",
                    raw_value=str(len(dup)),
                ))
        except Exception as e:
            _log.warning("quality check failed: %s", e)

        return issues
