# daily_kline Full Build OOM 解决方案

## 问题

`_build_full` 一次性 `SELECT * FROM bronze.raw_tdx_kline_daily_bfq`（1600万行）读到 Python DataFrame，内存溢出被 kill（exit 137）。

腾讯表仅 9277 行，无压力。问题仅在 TDX 全量读取。

## 现有代码流程（会 OOM）

```
_read_tdx(全量) → _read_tencent(全量) → _merge_sources → _clean → 一次性写入
```

内存峰值 = TDX 1600万行 DataFrame + 合并后 DataFrame + 清洗后 DataFrame ≈ 数 GB。

## 方案：按股票分块读 + 流式写

### 核心思路

不做全量读入，改为**按股票代码分批**：每批 N 只股票，读 → 清洗 → 写入 → 释放，循环直到处理完所有股票。

### 改动范围

仅改 `_build_full`，`_read_tdx` / `_read_tencent` / `_merge_sources` / `_clean` 不动（它们仍支持按 code 过滤调用）。

### 改动后的流程

```
1. DROP TABLE + CREATE TABLE（空表，用第一只股票的 schema）
2. ADD PRIMARY KEY (stock_code, trade_date)
3. 获取 Bronze 表中所有 DISTINCT stock_code 列表
4. 分批（每批 500 只）：
   a. _read_tdx(db, codes=batch)     ← 新增 codes 参数
   b. _read_tencent(db, codes=batch) ← 新增 codes 参数
   c. _merge_sources → _clean
   d. INSERT INTO silver.daily_kline ... ON CONFLICT DO NOTHING
   e. 释放 DataFrame
5. 质量检查
```

### 具体改动

#### 1. `_read_tdx` 增加 `codes` 参数

```python
def _read_tdx(self, db, date_filter: str = "", codes: list[str] | None = None) -> pd.DataFrame:
    conds = []
    if date_filter:
        conds.append(f"datetime LIKE '{date_filter}%'")
    if codes:
        in_list = ",".join(f"'{c}'" for c in codes)
        conds.append(f"stock_code IN ({in_list})")
    where = f" WHERE {' AND '.join(conds)}" if conds else ""

    df = db.execute(
        f"SELECT stock_code, datetime, open, close, high, low, volume, amount "
        f"FROM bronze.raw_tdx_kline_daily_bfq{where}",
        mode="read",
    )
    # ... 后续不变
```

#### 2. `_read_tencent` 同理增加 `codes` 参数

#### 3. `_build_full` 改为分块

```python
def _build_full(self, db) -> tuple[int, int, list[QualityIssue]]:
    # 1. 获取 Bronze 中所有股票代码
    tdx_codes = db.execute(
        "SELECT DISTINCT stock_code FROM bronze.raw_tdx_kline_daily_bfq",
        mode="read",
    )["stock_code"].astype(str).tolist()
    tc_codes = db.execute(
        "SELECT DISTINCT stock_code FROM bronze.raw_tencent_kline_day_bfq",
        mode="read",
    )["stock_code"].astype(str).tolist()
    # 腾讯 code 去 sh/sz 前缀
    tc_codes = [c.replace("sh", "").replace("sz", "") for c in tc_codes]
    all_codes = sorted(set(tdx_codes + tc_codes))

    # 2. 建空表（用第一批数据的 schema）
    db.drop_table("silver.daily_kline")

    total_read = 0
    total_written = 0
    total_rejected = 0
    batch_size = 500
    is_first = True

    for i in range(0, len(all_codes), batch_size):
        batch = all_codes[i : i + batch_size]
        tdx_df = self._read_tdx(db, codes=batch)
        tc_df = self._read_tencent(db, codes=batch)
        rows_read = len(tdx_df) + len(tc_df)
        total_read += rows_read

        if rows_read == 0:
            continue

        merged = self._merge_sources(tdx_df, tc_df)
        cleaned = self._clean(merged)
        rejected = getattr(cleaned, "_rejected", 0)
        total_rejected += rejected

        if cleaned.empty:
            continue

        out = cleaned[[
            "stock_code", "trade_date",
            "open", "high", "low", "close", "pre_close",
            "volume", "amount", "change_pct",
        ]].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"])

        if is_first:
            db.create_table("silver.daily_kline", out)
            db.execute(
                "ALTER TABLE silver.daily_kline ADD PRIMARY KEY (stock_code, trade_date)",
                mode="write",
            )
            is_first = False

        db.execute(
            "INSERT INTO silver.daily_kline SELECT * FROM _tmp "
            "ON CONFLICT (stock_code, trade_date) DO NOTHING",
            mode="write", df=out,
        )
        total_written += len(out)

        _log.info("daily_kline full batch %d/%d: codes=%d read=%d written=%d",
                  i // batch_size + 1, (len(all_codes) + batch_size - 1) // batch_size,
                  len(batch), rows_read, len(out))

    issues = self._check_quality(db, total_rejected)
    _log.info("daily_kline full: total read=%d written=%d rejected=%d issues=%d",
              total_read, total_written, total_rejected, len(issues))
    return total_read, total_written, issues
```

### 内存估算

- 每批 500 只股票，每只约 8000 天 × 10 列 ≈ 4M 行
- DataFrame 约 4M × 10 × 8B ≈ 320MB
- 峰值（含 merge/clean 中间态）约 1GB 以内
- 批次间 DataFrame 释放，不会累积

### 与 Bronze 层 _flush 模式的区别

| | Bronze _flush | Silver 分块 |
|---|---|---|
| 数据来源 | 逐只 API 拉取 | 已在 DuckDB 的 Bronze 表中 |
| 分块单位 | 每 500 只股票刷盘 | 每 500 只股票读+洗+写 |
| 写入方式 | _write（CREATE + INSERT） | INSERT ON CONFLICT DO NOTHING |
| 首批建表 | 首批 CREATE TABLE | 首批 CREATE TABLE + ADD PK |

### 风险点

1. **pre_close 计算**：`_clean` 中 `groupby("stock_code")["close"].shift(1)` 要求每只股票的行连续。分批处理时，每批内的股票数据完整，pre_close 计算不受影响。但一只股票如果跨批（不会，因为按 stock_code 分批），则会有问题。
2. **_merge_sources**：按 stock_code 分批后，每批内 TDX 和腾讯数据自然按 stock_code 隔离，merge 逻辑不受影响。
3. **首批 schema**：create_table 依赖第一批数据非空。如果第一批全空，需要兜底处理。

### 不改动的部分

- `_build_incremental`：只读当天数据，无 OOM 风险
- `_clean` / `_merge_sources`：逻辑不变，只是输入从全量变分批
- `check_completeness`：纯 SQL，不涉及 Python 端大 DataFrame
