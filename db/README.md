# db — DuckDB 读写管理器

## 快速开始

```python
from db import configure, get_db

configure("data/a_stock.db")
db = get_db()

# 首次部署：建三层 schema（bronze/silver/gold）
db.init_database()

# 读写
df = db.execute("SELECT * FROM gold.backtest_ready WHERE trade_date = '2026-06-16'", mode="read")
db.execute("INSERT INTO bronze.raw_tdx_stocks SELECT * FROM _tmp", mode="write")
```

## API

### `execute(sql, mode, params=None)`

统一读写入口。

| mode | 返回 | 说明 |
|------|------|------|
| `"read"` | `pd.DataFrame` | 多读并发，MVCC 快照 |
| `"write"` | `None` | 写互斥，写优先，失败抛异常 |

### `init_database()`

执行包内自带的 `schema.sql`，创建 bronze/silver/gold 三层 schema。无参数，不建表——表由摄入脚本按 API 实际返回动态创建。

### `list_tables() -> pd.DataFrame`

返回 DuckDB 原生 `SHOW ALL TABLES` 的结果。列：`database` / `schema` / `name` / `column_names` / `column_types` / `temporary`。

### `run_sql_file(filepath)`

执行外部 `.sql` 文件，走写锁。

## 读写调度

### 核心机制：读者-写者问题的写优先变体

本质上是操作系统经典 Readers-Writers Problem 的写优先实现。用 `threading.Event` + `Lock` 替代信号量，无忙等。

#### 八样东西，四组

| 组 | 成员 | 类型 | 用途 |
|----|------|------|------|
| **读并发** | `_active_readers` | `int` | 当前活跃读数量 |
| | `_reader_lock` | `Lock` | 保护 `_active_readers` 计数，**不是读互斥锁** |
| **读→写信号** | `reads_idle` | `Event` | 读池空闲信号。初始 `set()`（无读活跃），读进入 → `clear()`（忙碌），最后一个读退出 → `set()`（空闲） |
| **写互斥** | `_write_lock` | `Lock` | 写之间串行执行，同一时刻只有一个写在跑 |
| **写→读闸门** | `resume_accept` | `Event` | 写举旗，读站住。初始 `set()`（读可进入） |
| | `_pending_writes` | `int` | 排队写计数 |
| | `_pw_lock` | `Lock` | 保护 `_pending_writes` 计数器 |

### 读执行路径

```
读到达
  │
  resume_accept.wait()        ← 写的旗号。有写排队就阻塞在这里
  │
  _reader_lock.acquire()     ← 保护计数器，不是保护读之间
  _active_readers += 1
  reads_idle.clear()          ← 闲逛 → 忙
  _reader_lock.release()
  │
  duckdb.connect → 执行读 → close
  │
  _reader_lock.acquire()
  _active_readers -= 1
  if _active_readers == 0:
      reads_idle.set()        ← 最后一个读退出，设回闲逛
  _reader_lock.release()
```

### 写执行路径

```
写到达
  │
  _enter_write():             ← 尚未拿 _write_lock，先登记
    _pw_lock.acquire()
    if _pending_writes == 0:
        resume_accept.clear() ← 第一个写 → 闭锁（旗子举起）
    _pending_writes += 1
    _pw_lock.release()
  │
  _write_lock.acquire()       ← 争写锁（多个写竞争，不保证先来后到）
  reads_idle.wait()           ← 等读池清空
  │
  duckdb.connect → 执行写 → close
  │
  _write_lock.release()
  │
  _exit_write():              ← 锁已释放，才注销
    _pw_lock.acquire()
    _pending_writes -= 1
    if _pending_writes == 0:
        resume_accept.set()   ← 最后一个写 → 放行（旗子落下）
    _pw_lock.release()
```

### 写优先怎么保证

关键：W1 只要登记 `_enter_write`，不管有没有抢到 `_write_lock`，`resume_accept` 都被清。新读全堵在外面。等到所有写跑完，最后一个 `_exit_write` 才把旗子落下。

所以即便 W1 还没开始实际执行，门已经关了，读饥饿不会发生：现有读总会跑完 → 写拿到锁 → 写完 → 开门。

### 时序示例

```
读 R1 ════════════════
读 R2   ════════════════════
读 R3     ══════════
                      写 W1 到达
                      │ _enter_write: pending 0→1 → clear resume_accept
                      │ 排队在 _write_lock 上
                      │
                      读 R_new 到达
                      │ resume_accept.wait() ← 阻塞
                      │
                                 R1 完
                                      R3 完
读 R_new（阻塞…）                         R2 完 → reads_idle.set()
                      │
                      W1 拿锁 → reads_idle.wait()（秒过）→ 执行
                      W2 拿锁 → reads_idle.wait()（秒过）→ 执行
                      │
                      _exit_write: pending 1→0 → set resume_accept
                      │
读 R_new ──────────────────────────────────────── 通过 → 执行
```

### 写之间的顺序

`threading.Lock()` 不保证 FIFO，由 OS 调度决定谁先抢到锁。写之间无依赖关系，都是独立 INSERT/DDL，先写后写不影响最终结果。

## 表结构

### 三层 Schema

一个 DuckDB 文件内，按 schema 隔离：

| Schema | 职责 | 写入方式 |
|--------|------|---------|
| `bronze` | API 原始数据，多源共存 | 仅 INSERT |
| `silver` | 清洗、去重、标准化 | 从 bronze 构建 |
| `gold` | 回测就绪、因子快照 | 从 silver 构建 |

`schema.sql` 仅负责 `CREATE SCHEMA IF NOT EXISTS`，不预先建表。表由摄入脚本根据 API 实际返回动态 `CREATE TABLE`。

### Bronze 表命名规则

| 数据类型 | 规则 | 示例 |
|---------|------|------|
| K 线 | `raw_{源}_kline_{频率}_{复权}` | `raw_tdx_kline_daily_bfq` |
| 非 K 线 | `raw_{源}_{接口名}` | `raw_tdx_stocks` |

**频率**：`m1` / `m5` / `m15` / `m30` / `m60` / `daily` / `weekly` / `monthly`  
**复权**：`bfq`（不复权）/ `qfq`（前复权）/ `hfq`（后复权）

不同频率分表是因为数据量级悬殊（日线全市场/年 ~24 万行 vs 1 分钟线 ~3 亿行），混表导致无意义全表扫。

### TDX — `fetch/tdx.py`

| fetch 函数 | 关键参数 | 列结构 | Bronze 表 |
|-----------|---------|--------|-----------|
| `bars()` | `frequency`=m1~year | 14 列（open/close/high/low/vol/amount/year/month/day/hour/minute/datetime/volume，fetcher 补 `stock_code`），全频率一致 | 按频率分表：`raw_tdx_kline_m1_bfq` ~ `raw_tdx_kline_yearly_bfq`（10 张） |
| `k()` | `begin`/`end` 日期范围 | **10 列**（含 `code`/`factor`，不同于 bars） | `raw_tdx_kline_date_bfq` |
| `index_bars()` | 同 bars | **15 列**（多个股 OHLCV + `up_count`/`down_count`） | 按频率分表 |
| `quotes()` | 代码列表 | 46 列，5 档盘口 | `raw_tdx_quotes` |
| `minute()` / `minutes()` | `date` 有无 | 同 3 列（price/vol/volume） | `raw_tdx_minute` |
| `transaction()` / `transactions()` | `date` 有无 | 同 5 列（time/price/vol/buyorsell/volume） | `raw_tdx_transaction` |
| `stocks()` | 无 | 6 列（code/name/market 等） | `raw_tdx_stocks` |
| `xdxr()` | 单只代码 | 16 列除权除息 | `raw_tdx_xdxr` |
| `finance()` | 单只代码 | 37 列，仅最新一期 | `raw_tdx_finance` |
| `f10()` / `f10c()` | `name` 选类别 | DataFrame 或 list[dict] | `raw_tdx_f10` |
| `block()` | 无 | 4 列板块-成分 | `raw_tdx_block` |

> `minute`/`minutes` 写入同一张表，`transaction`/`transactions` 同理——只是参数有无 `date` 的区别，返回结构一致。
> `bars_batch()` 是 `bars()` 的批量封装，数据落入对应频率表中，不单独建表。

### 腾讯财经 — `fetch/tencent.py`

| fetch 函数 | 关键参数 | 列结构 | Bronze 表 |
|-----------|---------|--------|-----------|
| `bars()` | `ktype`=day/week/month, `fq`=qfq/hfq/''(bfq) | 7~8 列 OHLCV，全组合一致 | 按 ktype+fq 分表：`raw_tencent_kline_daily_qfq` 等（9 张） |
| `quotes()` | 代码列表 | 87 列（快照 + PE/PB/市值） | `raw_tencent_quotes` |
| `minute()` | 单只代码 | 5 列（time/price/volume/amount/stock_code） | `raw_tencent_minute` |
| `money_flow()` | 单只代码 | 15 列资金流向 | `raw_tencent_money_flow` |
| `order_book()` | 单只代码 | 5 列买/卖大/小单 | `raw_tencent_order_book` |
| `brief()` | 单只代码 | 12 列轻量快照 | `raw_tencent_brief` |

### 表创建时机

摄入脚本首次写入时，根据 DataFrame 的实际 `dtypes` 动态生成 `CREATE TABLE` 语句。不在 `schema.sql` 中硬编码——API 返回字段可能变化，硬编码的 DDL 与 DataFrame 列不匹配会导致写入失败。

## 全局单例

```python
configure(db_path, max_readers=5)  # 必须最先调用
db = get_db()                      # 任意位置获取同一个实例
```
