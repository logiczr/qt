"""Silver 层调度中枢。内建全部 Builder，通过 BuildOrder 协议接收编排层指令。"""

import logging
from silver.base import BuildOrder, BuildResult, BaseBuilder
from silver.stock_map import StockMapBuilder
from silver.daily_kline import DailyKlineBuilder
from silver.adj_factor import AdjFactorBuilder
from silver.minute_kline import MinuteKlineBuilder
from silver.finance import FinanceBuilder
from silver.f10_doc import F10DocBuilder

_log = logging.getLogger(__name__)

# target → Builder 类映射
_BUILDERS: dict[str, type[BaseBuilder]] = {
    "stock_map": StockMapBuilder,
    "daily_kline": DailyKlineBuilder,
    "adj_factor": AdjFactorBuilder,
    "minute_kline": MinuteKlineBuilder,
    "finance": FinanceBuilder,
    "f10_doc": F10DocBuilder,
}


class Silver:
    """Silver 层调度中枢。

    用法:
        from silver import Silver, BuildOrder
        sv = Silver()
        for r in sv.execute(BuildOrder(target="stock_map", mode="full")):
            ...
    """

    def __init__(self):
        self._builders: dict[str, BaseBuilder] = {
            name: cls() for name, cls in _BUILDERS.items()
        }

    def execute(self, order: BuildOrder):
        """执行构建，yield BuildResult。"""
        if order.target == "all":
            for name in _BUILDERS:
                builder = self._builders[name]
                _log.info("Silver build: %s mode=%s", name, order.mode)
                yield builder.build(order)
        elif order.target in self._builders:
            builder = self._builders[order.target]
            _log.info("Silver build: %s mode=%s", order.target, order.mode)
            yield builder.build(order)
        else:
            _log.warning("unknown target: %s", order.target)
            yield BuildResult(
                target=order.target, status="skipped",
                errors=[f"unknown target: {order.target}"],
            )
