"""首次部署 — 从零到可用。

执行顺序：目录 → DB → Bronze → Silver → 验证 → 启动全部服务

用法:
    source venv/bin/activate
    python setup.py
"""

import os
import threading


def step(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def is_initialized():
    """检测是否已完成首次初始化 — 检查 Silver stock_map 是否有数据。"""
    try:
        from db import configure, get_db
        db_path = os.environ.get("QT_DB_PATH", "Data.duckdb")
        configure(db_path)
        db = get_db()
        tables = db.list_tables()
        silver_tables = tables[tables["schema"] == "silver"]
        if silver_tables.empty:
            return False
        # stock_map 是 Silver 第一张表，有数据就算初始化过
        count = db.execute("SELECT count(*) AS c FROM silver.stock_map", mode="read").iloc[0, 0]
        return count > 0
    except Exception:
        return False


def main():
    # ---- 0. 检测 ----
    if is_initialized():
        print("已检测到初始化数据，跳过 setup")
        return

    # ---- 1. 目录 ----
    step("1. 创建目录")
    for d in ["data/bronze/f10", "data/silver/f10", "data/industry"]:
        os.makedirs(d, exist_ok=True)
        print(f"  {d}/ OK")

    # ---- 2. 数据库 ----
    step("2. 初始化数据库")
    from db import configure, get_db
    configure("Data.duckdb")
    db = get_db()
    db.init_database()
    print("  bronze/silver/gold schema 已创建")

    # ---- 3. Bronze 层 ----
    from bronze import Bronze, IngestOrder
    brz = Bronze()

    step("3.1 拉取股票列表 + 日线 + 分钟线 (first_init)")
    for result in brz.execute(IngestOrder(action="first_init")):
        print(f"  {result.action}: {result.status}, {result.rows} rows"
              + (f", {result.failed} failed" if result.failed else ""))

    step("3.2 拉取除权除息 + 板块 + 财务 (weekly)")
    for result in brz.execute(IngestOrder(action="weekly")):
        print(f"  {result.action}: {result.status}, {result.rows} rows"
              + (f", {result.failed} failed" if result.failed else ""))

    # F10 可选，很慢
    # step("3.3 拉取 F10 (可选，很慢)")
    # for result in brz.execute(IngestOrder(action="f10")):
    #     print(f"  {result.action}: {result.status}, {result.rows} rows")

    # ---- 4. Silver 层 ----
    from silver import Silver, BuildOrder
    sv = Silver()

    silver_targets = ["stock_map", "adj_factor", "daily_kline", "minute_kline", "finance"]
    for target in silver_targets:
        step(f"4. 构建 Silver: {target}")
        for result in sv.execute(BuildOrder(target=target, mode="full")):
            print(f"  {result.target}: {result.status}, {result.rows} rows")

    # ---- 5. 验证 ----
    step("5. 验证数据")
    tables = db.list_tables()
    silver_tables = tables[tables["schema"] == "silver"]
    for _, row in silver_tables.iterrows():
        count = db.execute(
            f"SELECT count(*) AS c FROM {row['schema']}.{row['name']}",
            mode="read",
        ).iloc[0, 0]
        print(f"  {row['schema']}.{row['name']}: {count} rows")

    # ---- 6. 启动全部服务 ----
    step("6. 启动服务")

    import uvicorn

    # serve: 8000（主线程跑，阻塞）
    # realtime: 8888（子线程）

    def run_realtime():
        from realtime.api import app as realtime_app
        from realtime.feed import start_feed
        start_feed()
        uvicorn.run(realtime_app, host="0.0.0.0", port=8888, ws_max_size=None, log_level="warning")

    threading.Thread(target=run_realtime, daemon=True).start()

    print("  serve     → http://127.0.0.1:8000  (历史数据)")
    print("  realtime  → http://127.0.0.1:8888  (实时行情)")
    print()

    # serve 跑在主线程
    from serve.api import app as serve_app
    uvicorn.run(serve_app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
