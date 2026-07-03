"""Ingest 编排层：Dealer 决策 + Pipeline 执行。"""

from ingest.dealer import Dealer, Decision
from ingest.pipeline import Pipeline

__all__ = ["Pipeline", "Dealer", "Decision"]
