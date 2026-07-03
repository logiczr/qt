"""Silver 层核心类型与基类。"""

import logging
import time
from dataclasses import dataclass, field
from db import get_db

_log = logging.getLogger(__name__)


@dataclass
class BuildOrder:
    """编排层 → Silver 层协议。"""

    target: str  # 目标表名，如 "daily_kline" / "stock_map" / "all"
    mode: str = "incremental"  # "full" | "incremental" | "repair"
    date: str = ""  # 增量模式指定日期，空则取当天
    codes: list[str] = field(default_factory=list)  # repair 模式指定股票代码


@dataclass
class BuildResult:
    """Silver 层 → 编排层结果。"""

    target: str  # "stock_map"
    status: str  # "ok" / "partial" / "fail" / "skipped"
    rows_read: int = 0  # 从 Bronze 读取行数
    rows_written: int = 0  # 写入 Silver 行数
    rows_rejected: int = 0  # 质量门拒绝行数
    elapsed_ms: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class QualityIssue:
    """单条质量问题。"""

    level: str  # "warning" / "error"
    table: str  # "silver.stock_map"
    stock_code: str  # "600000"
    trade_date: str  # "2026-06-20" (stock_map 无日期时可空)
    column: str  # "list_date"
    message: str  # "list_date 为空"
    raw_value: str  # "NULL"


class BaseBuilder:
    """Silver 层清洗基类。"""

    target: str  # 子类声明，如 "stock_map"

    def build(self, order: BuildOrder) -> BuildResult:
        raise NotImplementedError

    def _ensure_silver_schema(self) -> None:
        """确保 silver schema 存在。"""
        get_db().execute("CREATE SCHEMA IF NOT EXISTS silver", mode="write")
