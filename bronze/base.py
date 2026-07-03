"""Bronze 层核心类型与基类。"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import time
import pandas as pd
from db import get_db

_log = logging.getLogger(__name__)


@dataclass
class IngestResult:
    source: str                # "tdx" / "tencent"
    table: str                 # "bronze.raw_tdx_kline_daily_bfq"
    status: str                # "ok" / "partial" / "fail"
    rows: int = 0              # 写入行数
    total: int = 0             # 尝试股票数
    failed: int = 0            # 失败股票数
    batch_id: str = ""         # 批次号
    elapsed_ms: int = 0        # 总耗时
    errors: list[str] = field(default_factory=list)


class BaseBronze:

    source_name: str           # 子类声明: "tdx" / "tencent"
    rate_limit_delay: float    

    # ---- 共享工具 -----------------------------------------------------

    def _load_stocks(self, source: str = "tdx") -> list[str]:
        """返回 A 股股票代码。

        Args:
            source: 股票表所属数据源，默认 tdx
        """
        table = f"bronze.raw_{source}_stocks"
        try:
            if source == "tdx":
                df = get_db().execute(
                    f"SELECT code FROM {table} WHERE "
                    "(market = 'sz' AND (code LIKE '00%' OR code LIKE '30%')) OR "
                    "(market = 'sh' AND (code LIKE '60%' OR code LIKE '68%'))",
                    mode="read",
                )
            else:
                df = get_db().execute(
                    f"SELECT code FROM {table}", mode="read"
                )
            return df["code"].astype(str).tolist()
        except Exception:
            _log.warning("_load_stocks: %s 不存在或无数据", table)
            return []

    def _load_indexes(self) -> list[str]:
        """返回指数代码。
        
        SZ: 399xxx
        SH: 000xxx (上证系列)
        """
        table = f"bronze.raw_{self.source_name}_stocks"
        try:
            df = get_db().execute(
                f"SELECT code FROM {table} WHERE "
                "(code LIKE '399%' AND market = 'sz') OR "
                "(code LIKE '000%' AND market = 'sh')",
                mode="read",
            )
            return df["code"].astype(str).tolist()
        except Exception:
            _log.warning("_load_indexes: %s 不存在或无数据", table)
            return []

    def _rate_limit(self) -> None:
        """频率控制。"""
        time.sleep(self.rate_limit_delay)

    def _add_metadata(self, df: pd.DataFrame, batch_id: str,
                      data_type: str) -> pd.DataFrame:
        """给 DataFrame 加 4 列元数据，返回新 df。"""
        df = df.copy()
        df["ingested_at"] = pd.Timestamp.now()
        df["source_system"] = self.source_name
        df["source_api"] = data_type
        df["batch_id"] = batch_id
        return df

    def _batch_id(self, data_type: str) -> str:
        """生成批次号: {source}_{type}_{YYYYMMDD_HHMMSS}"""
        now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"{self.source_name}_{data_type}_{now}"

    def _write(self, df: pd.DataFrame, table: str,
               pk: list[str] | None = None) -> int:
        """建表（如需）+ INSERT 写入 Bronze。

        Args:
            df: 待写入数据
            table: 目标表名
            pk: 主键列名列表。非空时，从 df 中去除表中已有的行再写入（去重）；
                为空则直接 INSERT（可能产生重复行）。

        Returns:
            实际写入行数
        """
        db = get_db()
        db.create_table(table, df)

        if pk:
            # 查出表中已有的 pk 值，从 df 中去除
            in_list = ",".join(f"'{v}'" for v in df[pk[0]].dropna().unique())
            if in_list:
                try:
                    existing = db.execute(
                        f"SELECT {', '.join(pk)} FROM {table} "
                        f"WHERE {pk[0]} IN ({in_list})",
                        mode="read",
                    )
                    if not existing.empty:
                        existing_set = set(existing.itertuples(index=False, name=None))
                        mask = ~df[pk].apply(tuple, axis=1).isin(existing_set)
                        df = df[mask]
                except Exception:
                    _log.warning("_write dedup query failed, fallback to direct insert")
            if df.empty:
                _log.info("_write: all rows already exist in %s, skip", table)
                return 0

        db.execute(f"INSERT INTO {table} SELECT * FROM _tmp", mode="write", df=df)
        return len(df)

    def _progress(self, n: int, total: int, avg_ms: float) -> str:
        """进度日志字符串。"""
        pct = n * 100 / total if total else 0
        eta = (total - n) * avg_ms / 1000
        return f"{self.source_name}: {n}/{total} ({pct:.0f}%)  eta={eta:.0f}s"
