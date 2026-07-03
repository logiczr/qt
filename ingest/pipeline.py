"""Ingest 编排层：Pipeline 执行 Dealer 决策。

Pipeline 的职责：
1. 执行 Dealer 返回的 Decision（调 Bronze/Silver）
2. 串行约束（fast_close 和 daily 不能同时跑）
3. DB 持久化（ingest.plan / ingest.step）
4. 每日自动化调度
"""

import json
import logging
import threading
from collections import deque
from datetime import time

from bronze import Bronze, IngestOrder
from silver import Silver, BuildOrder
from silver.daily_kline import DailyKlineBuilder
from db import get_db
from ingest.dealer import Dealer, Decision

_log = logging.getLogger(__name__)


class Pipeline:
    """执行器 + 串行约束 + 调度。

    循环：Dealer 决策 → Pipeline 执行 → 结果喂回 Dealer → 再决策 → ... → done
    """

    # 日频调度：时间 → action
    DAILY_SCHEDULE = {
        time(15, 5): "fast_close",
        time(18, 0): "daily",
    }

    # 周频调度：cron → action
    WEEKLY_SCHEDULE = {
        "0 10 * * 6": "weekly",
    }

    def __init__(self):
        self.dealer = Dealer()
        self._bronze = Bronze()
        self._silver = Silver()
        self._dk = DailyKlineBuilder()

        # 串行约束
        self._lock = threading.Lock()
        self._queue: deque[tuple[str, list[str]]] = deque()  # (action, codes)
        self._running = False

        # DB
        self._ensure_tables()

    # ===================================================================
    # 触发
    # ===================================================================

    def trigger(self, action: str, codes: list[str] | None = None):
        """触发一次管线。如果正在跑则排队。"""
        if self._running:
            _log.info("pipeline running, queuing %s", action)
            self._queue.append((action, codes or []))
            return
        self._run(action, codes or [])

    # ===================================================================
    # 执行循环
    # ===================================================================

    def _run(self, action: str, codes: list[str]):
        """执行完整流程：Dealer 决策 → 执行 → 再决策 → ... → done。"""
        self._running = True
        self._lock.acquire()

        decision, plan_id = self.dealer.start(action, codes)
        self._save_plan(plan_id, action)
        _log.info("===== %s start =====", action)

        while decision.type != "done":
            results = self._execute(decision, plan_id)
            decision = self.dealer.next(results)

        # 完成
        shelved = self.dealer.shelved_codes
        if shelved:
            _log.warning("shelved %d codes: %s", len(shelved), shelved[:10])

        self._finish_plan(plan_id)
        _log.info("===== %s done =====", action)

        self._lock.release()
        self._running = False

        # 检查排队
        if self._queue:
            next_action, next_codes = self._queue.popleft()
            _log.info("dequeue next: %s", next_action)
            self._run(next_action, next_codes)

    def _execute(self, decision: Decision, plan_id: str) -> list:
        """根据 Decision 执行，返回结果。"""
        if decision.type == "bronze":
            results = list(self._bronze.execute(
                IngestOrder(action=decision.action, codes=decision.codes)))
            self._save_step(plan_id, f"bronze.{decision.action}", results)
            _log.info("bronze.%s done, %d results", decision.action, len(results))
            return results

        elif decision.type == "retry":
            action = f"retry.{decision.table_key}"
            results = list(self._bronze.execute(
                IngestOrder(action=action, codes=decision.codes)))
            from ingest.dealer import _extract_failed_codes
            still = _extract_failed_codes(results)
            self._save_step(plan_id, f"bronze.retry.{decision.table_key}.r{decision.round}",
                            results, still)
            _log.info("retry.%s r%d: %d→%d failed",
                      decision.table_key, decision.round,
                      len(decision.codes), len(still))
            return results

        elif decision.type == "silver":
            results = list(self._silver.execute(
                BuildOrder(target=decision.target, mode=decision.mode,
                           codes=decision.codes)))
            step_name = f"silver.{decision.target}"
            if decision.mode == "repair":
                step_name += ".repair"
            self._save_step(plan_id, step_name, results)
            _log.info("silver.%s (%s) done, %d results",
                      decision.target, decision.mode, len(results))
            return results

        elif decision.type == "check":
            try:
                report = self._dk.check_completeness()
            except Exception:
                _log.exception("check_completeness failed")
                return []  # 空结果 → Dealer 当作无缺失
            return report

        elif decision.type == "backfill":
            results = list(self._bronze.execute(
                IngestOrder(action="backfill_daily", codes=decision.codes)))
            self._save_step(plan_id, "bronze.backfill", results)
            _log.info("backfill %d codes", len(decision.codes))
            return results

        return []

    # ===================================================================
    # 调度
    # ===================================================================

    def start_scheduler(self):
        """启动定时调度（APScheduler）。"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:
            _log.error("apscheduler not installed, scheduler disabled")
            return

        self._sched = BackgroundScheduler(timezone="Asia/Shanghai")

        for t, action in self.DAILY_SCHEDULE.items():
            self._sched.add_job(
                self.trigger,
                CronTrigger(hour=t.hour, minute=t.minute),
                kwargs={"action": action},
                id=f"schedule_{action}",
            )
            _log.info("scheduled: %s at %s", action, t)

        for cron_expr, action in self.WEEKLY_SCHEDULE.items():
            parts = cron_expr.split()
            self._sched.add_job(
                self.trigger,
                CronTrigger(minute=parts[0], hour=parts[1],
                            day=parts[2], month=parts[3],
                            day_of_week=parts[4]),
                kwargs={"action": action},
                id=f"schedule_{action}",
            )
            _log.info("scheduled: %s cron %s", action, cron_expr)

        self._sched.start()
        _log.info("scheduler started")

    def stop_scheduler(self):
        if hasattr(self, "_sched"):
            self._sched.shutdown(wait=False)

    # ===================================================================
    # DB 持久化
    # ===================================================================

    def _ensure_tables(self):
        db = get_db()
        db.execute("CREATE SCHEMA IF NOT EXISTS ingest", mode="write")
        db.execute("""
            CREATE TABLE IF NOT EXISTS ingest.plan (
                plan_id    VARCHAR PRIMARY KEY,
                action     VARCHAR,
                status     VARCHAR DEFAULT 'running',
                started_at TIMESTAMP DEFAULT current_timestamp,
                finished_at TIMESTAMP
            )
        """, mode="write")
        db.execute("""
            CREATE TABLE IF NOT EXISTS ingest.step (
                plan_id      VARCHAR,
                step_name    VARCHAR,
                seq          INTEGER,
                status       VARCHAR DEFAULT 'pending',
                rows         INTEGER DEFAULT 0,
                failed       INTEGER DEFAULT 0,
                failed_codes VARCHAR DEFAULT '[]',
                errors_json  VARCHAR DEFAULT '[]',
                PRIMARY KEY (plan_id, step_name)
            )
        """, mode="write")

    def _save_plan(self, plan_id: str, action: str):
        db = get_db()
        db.execute(
            "INSERT OR REPLACE INTO ingest.plan (plan_id, action, status, started_at) "
            "VALUES (?, ?, 'running', current_timestamp)",
            mode="write", params=[plan_id, action])

    def _finish_plan(self, plan_id: str):
        db = get_db()
        db.execute(
            "UPDATE ingest.plan SET status = 'ok', finished_at = current_timestamp "
            "WHERE plan_id = ?", mode="write", params=[plan_id])

    def _save_step(self, plan_id: str, step_name: str,
                   results: list, failed_codes: list[str] | None = None):
        failed_codes = failed_codes or []
        db = get_db()
        rows = sum(getattr(r, "rows", 0) or getattr(r, "rows_written", 0) for r in results)
        failed = sum(getattr(r, "failed", 0) for r in results)
        errors: list[str] = []
        for r in results:
            errors.extend(getattr(r, "errors", []))
        status = "ok" if not failed and not failed_codes else "partial"

        seq = 0
        try:
            existing = db.execute(
                "SELECT COUNT(*) AS cnt FROM ingest.step WHERE plan_id = ?",
                mode="read", params=[plan_id])
            seq = int(existing["cnt"].values[0])
        except Exception:
            pass

        db.execute(
            "INSERT OR REPLACE INTO ingest.step "
            "(plan_id, step_name, seq, status, rows, failed, failed_codes, errors_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            mode="write",
            params=[plan_id, step_name, seq, status, rows, failed,
                    json.dumps(failed_codes[:100]),
                    json.dumps(errors[:50])])
