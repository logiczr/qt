"""
DuckDB 读写管理器 — 写优先 + 读并发 + 写排队 + Event 握手 + 单例

三个 Event：
  resume_accept  — 所有排队写完成 → 读池：可以接新读了
  reads_idle     — 读池 → 写：所有活跃读已完成

写排队机制：
  _pending_writes 计数器，写进入时 +1（0→1 闭锁），写完 -1（1→0 放行）。
  多写按 _write_lock FIFO 执行，全部写完后 resume_accept 才 set。

无后台线程，无 while True。全程 Event 阻塞等待。
"""

import os
import threading
import duckdb
import pandas as pd


class DuckDBManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path: str = "", max_readers: int = 5):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    obj = super().__new__(cls)
                    obj._init_once(db_path, max_readers)
                    cls._instance = obj
        return cls._instance

    def _init_once(self, db_path: str, max_readers: int):
        if not db_path:
            raise ValueError("db_path required for first initialization")
        self._db_path = db_path
        self._readers = max_readers

        # 读并发管理
        self._active_readers = 0
        self._reader_lock = threading.Lock()

        # 读写握手 Event
        self.resume_accept = threading.Event()
        self.resume_accept.set()

        self.reads_idle = threading.Event()
        self.reads_idle.set()

        # 写互斥 + 排队计数器
        self._write_lock = threading.Lock()
        self._pending_writes = 0
        self._pw_lock = threading.Lock()

    # ---- 运行时读写 ------------------------------------------------

    def execute(self, sql: str, mode: str, params=None, df: pd.DataFrame = None) -> pd.DataFrame | None:
        """读/写统一入口。

        mode="read"  → 返回 DataFrame
        mode="write" → 无返回值，失败抛异常。传入 df 则先 register 再 execute。
        """
        if mode == "read":
            return self._read(sql, params)
        return self._write(sql, params, df)

    def create_table(self, table_name: str, df: pd.DataFrame) -> None:
        """根据 DataFrame 列结构创建表（不含数据）。"""
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM _tmp LIMIT 0"
        self._enter_write()
        try:
            with self._write_lock:
                self.reads_idle.wait()
                con = duckdb.connect(self._db_path)
                try:
                    con.register("_tmp", df)
                    con.execute(sql)
                finally:
                    con.close()
        finally:
            self._exit_write()

    def drop_table(self, table_name: str) -> None:
        """删除表（不存在则忽略）。"""
        self._write(f"DROP TABLE IF EXISTS {table_name}")

    # ---- DDL / Schema ----------------------------------------------

    def init_database(self) -> None:
        """首次部署：执行包内自带的 schema.sql，建立三层 schema。"""
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
        self.run_sql_file(schema_path)

    def list_tables(self) -> pd.DataFrame:
        """返回当前数据库中所有表（database, schema, table_name）。"""
        return self._read("SHOW ALL TABLES")

    def run_sql_file(self, filepath: str) -> None:
        """执行单个 .sql 文件。"""
        with open(filepath, encoding="utf-8") as f:
            sql = f.read()
        self._execute_ddl(sql)

    # ---- 写排队 ----------------------------------------------------

    def _enter_write(self) -> None:
        """写入口：第一个写闭锁 resume_accept，所有写递增计数。"""
        with self._pw_lock:
            if self._pending_writes == 0:
                self.resume_accept.clear()
            self._pending_writes += 1

    def _exit_write(self) -> None:
        """写出口：最后一个写放行 resume_accept。"""
        with self._pw_lock:
            self._pending_writes -= 1
            if self._pending_writes == 0:
                self.resume_accept.set()

    # ---- internals --------------------------------------------------

    def _read(self, sql: str, params=None) -> pd.DataFrame:
        self.resume_accept.wait()

        with self._reader_lock:
            self._active_readers += 1
            self.reads_idle.clear()

        con = duckdb.connect(self._db_path)
        try:
            return con.execute(sql, params).fetchdf()
        finally:
            con.close()
            with self._reader_lock:
                self._active_readers -= 1
                if self._active_readers == 0:
                    self.reads_idle.set()

    def _write(self, sql: str, params=None, df: pd.DataFrame = None) -> None:
        self._enter_write()
        try:
            with self._write_lock:
                self.reads_idle.wait()

                con = duckdb.connect(self._db_path)
                try:
                    if df is not None:
                        con.register("_tmp", df)
                    con.execute(sql, params)
                finally:
                    con.close()
        finally:
            self._exit_write()

    def _execute_ddl(self, sql: str) -> None:
        self._enter_write()
        try:
            with self._write_lock:
                self.reads_idle.wait()
                con = duckdb.connect(self._db_path)
                try:
                    for statement in sql.split(";"):
                        statement = statement.strip()
                        if statement:
                            con.execute(statement)
                finally:
                    con.close()
        finally:
            self._exit_write()


# ---- 全局单例 --------------------------------------------------------

_db: DuckDBManager | None = None


def configure(db_path: str, max_readers: int = 5) -> DuckDBManager:
    """启动时调用一次，初始化全局数据库管理器。"""
    global _db
    _db = DuckDBManager(db_path, max_readers)
    return _db


def get_db() -> DuckDBManager:
    """获取全局单例。未 configure 时抛异常。"""
    if _db is None:
        raise RuntimeError("db_manager not configured, call configure() first")
    return _db
