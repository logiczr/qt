"""A股量化数据仓库 — 主入口。

启动所有服务：
    python main.py

仅启动部分服务：
    python main.py serve        # 只启动历史数据服务 8000
    python main.py realtime    # 只启动实时行情 8888
    python main.py scheduler   # 只启动日常调度
"""

import sys
import threading

from log import setup
setup()

import logging
import uvicorn


def run_serve():
    """启动 serve（端口 8000）。"""
    from db import configure
    configure(db_path="Data.duckdb")
    from serve.api import app
    uvicorn.run(app, host="0.0.0.0", port=8000)


def run_realtime():
    """启动 realtime（端口 8888）。"""
    from realtime.feed import start_feed
    start_feed()
    from realtime.api import app
    uvicorn.run(app, host="0.0.0.0", port=8888, ws_max_size=None)


def run_scheduler():
    """启动日常调度。"""
    from ingest import Pipeline
    pipe = Pipeline()
    pipe.start_scheduler()


def main():
    args = sys.argv[1:]

    if not args:
        # 默认：全部启动
        print("启动全部服务: serve(8000) + realtime(8888) + scheduler")
        print()

        # scheduler 后台线程
        threading.Thread(target=run_scheduler, daemon=True).start()
        print("  ✓ scheduler 已启动")

        # realtime 后台线程
        threading.Thread(target=run_realtime, daemon=True).start()
        print("  ✓ realtime 已启动 → http://127.0.0.1:8888")

        # serve 主线程（阻塞）
        print("  ● serve 启动中   → http://127.0.0.1:8000")
        print()
        run_serve()

    elif "serve" in args:
        run_serve()
    elif "realtime" in args:
        run_realtime()
    elif "scheduler" in args:
        run_scheduler()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
