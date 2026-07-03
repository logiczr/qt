"""A股行业热力图 — 前端页面 + 数据服务。

启动方式: python heatmap.py
访问: http://127.0.0.1:9999

数据源:
- 行业分类: baostock（启动时一次性加载，缓存为 CSV）
- 个股行情: realtime Feed 内存快照
- 后端合并后按行业分组推给前端，前端不做任何 code 匹配
"""

import json
import logging
import os

import pandas as pd
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

# ---- 行业分类数据 ----

_industry_map: dict[str, dict] = {}  # code → {name, industry}
_CSV_PATH = os.path.join(os.path.dirname(__file__), "data", "industry.csv")


def _load_industry():
    """加载行业分类。优先读 CSV 缓存，没有则从 baostock 拉取后存 CSV。"""
    global _industry_map

    if os.path.exists(_CSV_PATH):
        df = pd.read_csv(_CSV_PATH, dtype=str)
        for _, row in df.iterrows():
            code = row.get("code", "")
            name = row.get("name", "")
            ind_name = row.get("industry", "")
            if code and ind_name:
                _industry_map[code] = {"name": name, "industry": ind_name}
        _log.info("industry loaded from CSV: %d stocks, %d industries",
                  len(_industry_map), len(set(v["industry"] for v in _industry_map.values())))
        return

    # 从 baostock 拉取
    import baostock as bs
    lg = bs.login()
    rs = bs.query_stock_industry()
    rows = []
    while (rs.error_code == "0") and rs.next():
        rows.append(rs.get_row_data())
    df = pd.DataFrame(rows, columns=rs.fields)
    bs.logout()

    records = []
    for _, row in df.iterrows():
        raw_code = row["code"].replace(".", "")
        plain = raw_code[2:] if len(raw_code) == 8 else raw_code
        ind = row.get("industry", "")
        ind_name = ind[3:] if len(ind) > 3 and ind[:3].isalnum() and not ind[:3].isalpha() else ind
        if ind_name:
            _industry_map[plain] = {"name": row["code_name"], "industry": ind_name}
            records.append({"code": plain, "name": row["code_name"], "industry": ind_name})

    # 存 CSV
    os.makedirs(os.path.dirname(_CSV_PATH), exist_ok=True)
    pd.DataFrame(records).to_csv(_CSV_PATH, index=False, encoding="utf-8")
    _log.info("industry loaded from baostock & saved to CSV: %d stocks, %d industries",
              len(_industry_map), len(set(v["industry"] for v in _industry_map.values())))


def _build_heatmap_data() -> dict:
    """后端合并行业+行情，返回 {行业: [{code, name, pct}, ...]} 结构。"""
    from realtime.feed import get_feed
    df = get_feed().get_snapshot()
    if df is None or df.empty:
        return {}

    # 确保code是字符串
    df["code"] = df["code"].astype(str)

    groups: dict[str, list] = {}
    for _, row in df.iterrows():
        code = str(row.get("code", ""))
        if not code:
            continue
        info = _industry_map.get(code)
        ind = info["industry"] if info else "其他"
        name = info["name"] if info else str(row.get("name", code))
        pct = row.get("change_pct", "")
        try:
            pct = float(pct)
        except (ValueError, TypeError):
            pct = 0.0

        if ind not in groups:
            groups[ind] = []
        groups[ind].append({"code": code, "name": name, "pct": round(pct, 2)})

    return groups


# ---- HTML 前端 ----

HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>A股行业热力图</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #1a1a2e; color: #eee; font-family: "Microsoft YaHei", sans-serif; }
#header { background: #16213e; padding: 8px 16px; display: flex; align-items: center; justify-content: space-between; }
#header h1 { font-size: 18px; }
#status { font-size: 12px; color: #888; }
.legend { display: flex; align-items: center; gap: 6px; font-size: 11px; }
.legend-bar { width: 160px; height: 10px; border-radius: 3px; background: linear-gradient(to right, #00c853, #4caf50, #555, #ef5350, #ff1744); }
#map { width: 100%; padding: 8px; columns: 4; column-gap: 4px; }
.industry { border: 1px solid rgba(255,255,255,0.15); display: flex; flex-direction: column; break-inside: avoid; margin-bottom: 4px; }
.ind-label { font-size: 12px; font-weight: bold; padding: 3px 6px; color: #fff; text-shadow: 0 1px 3px rgba(0,0,0,0.8); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex-shrink: 0; }
.ind-stocks { flex: 1; display: flex; flex-wrap: wrap; align-content: flex-start; padding: 2px; gap: 1px; }
.stock { display: flex; flex-direction: column; align-items: center; justify-content: center; overflow: hidden; cursor: pointer; border: 1px solid rgba(0,0,0,0.3); flex-shrink: 0; }
.stock-name { font-size: 9px; line-height: 1.1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 95%; text-shadow: 0 1px 2px rgba(0,0,0,0.6); }
.stock-pct { font-size: 10px; font-weight: bold; line-height: 1.2; text-shadow: 0 1px 2px rgba(0,0,0,0.6); }
</style>
</head>
<body>
<div id="header">
  <h1>A股行业热力图</h1>
  <div class="legend"><span>-10%</span><div class="legend-bar"></div><span>+10%</span></div>
  <div id="status">连接中...</div>
</div>
<div id="map"></div>
<script>
const HM_HOST = "{{HM_HOST}}";
const WS_URL = "ws://" + HM_HOST + "/ws/heatmap";

function pctColor(pct) {
    if (pct === undefined || pct === null || isNaN(pct)) return "#555";
    const v = Math.max(-10, Math.min(10, pct));
    if (v > 0) {
        const t = v / 10;
        return `rgb(${Math.round(140+115*t)},${Math.round(50-20*t)},${Math.round(50-20*t)})`;
    } else if (v < 0) {
        const t = -v / 10;
        return `rgb(${Math.round(50-20*t)},${Math.round(130+70*t)},${Math.round(50+20*t)})`;
    }
    return "#555";
}

function pctTier(pct) {
    const a = Math.abs(pct || 0);
    if (a >= 8) return 5;
    if (a >= 6) return 4;
    if (a >= 4) return 3;
    if (a >= 2) return 2;
    return 1;
}

function render(data) {
    const container = document.getElementById("map");
    container.innerHTML = "";

    const industries = Object.entries(data).map(([name, stocks]) => {
        const avgPct = stocks.reduce((s, st) => s + st.pct, 0) / stocks.length;
        return { name, stocks, avgPct, tier: pctTier(avgPct) };
    });
    industries.sort((a, b) => b.tier - a.tier || Math.abs(b.avgPct) - Math.abs(a.avgPct));

    const tierW = { 5: 140, 4: 110, 3: 85, 2: 65, 1: 50 };
    const tierH = { 5: 34, 4: 28, 3: 24, 2: 22, 1: 20 };

    for (const item of industries) {
        const div = document.createElement("div");
        div.className = "industry";
        div.style.background = pctColor(item.avgPct);

            const label = document.createElement("div");
            label.className = "ind-label";
            label.textContent = item.name + " " + (item.avgPct >= 0 ? "+" : "") + item.avgPct.toFixed(2) + "%";
            div.appendChild(label);

            const stocksBox = document.createElement("div");
            stocksBox.className = "ind-stocks";

            const sorted = [...item.stocks].sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct));
            for (const st of sorted) {
                const t = pctTier(st.pct);
                const block = document.createElement("div");
                block.className = "stock";
                block.style.width = tierW[t] + "px";
                block.style.height = tierH[t] + "px";
                block.style.background = pctColor(st.pct);
                block.title = st.name + " " + st.pct.toFixed(2) + "%";

                const nameEl = document.createElement("div");
                nameEl.className = "stock-name";
                nameEl.textContent = st.name;
                block.appendChild(nameEl);

                const pctEl = document.createElement("div");
                pctEl.className = "stock-pct";
                pctEl.textContent = (st.pct >= 0 ? "+" : "") + st.pct.toFixed(2) + "%";
                block.appendChild(pctEl);

                stocksBox.appendChild(block);
            }

            div.appendChild(stocksBox);
        container.appendChild(div);
    }

    const totalStocks = industries.reduce((s, d) => s + d.stocks.length, 0);
    document.getElementById("status").textContent =
        industries.length + " 个行业 | " + totalStocks + " 只股票 | 实时更新中";
}

function connectWS() {
    document.getElementById("status").textContent = "连接中...";
    const ws = new WebSocket(WS_URL);
    ws.onopen = () => { document.getElementById("status").textContent = "已连接，等待数据..."; };
    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data && data.type === "ping") return;
        if (typeof data !== "object" || Array.isArray(data)) return;
        render(data);
    };
    ws.onclose = () => {
        document.getElementById("status").textContent = "断线重连中...";
        setTimeout(connectWS, 3000);
    };
    ws.onerror = () => {};
}

connectWS();
window.addEventListener("resize", () => {});
</script>
</body>
</html>"""


# ---- FastAPI ----

app = FastAPI(title="Heatmap", version="0.1")


@app.get("/", response_class=HTMLResponse)
async def index():
    """热力图页面。"""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = "127.0.0.1"

    hm_host = f"{local_ip}:9999"
    html = HTML.replace("{{HM_HOST}}", hm_host)
    return html


@app.websocket("/ws/heatmap")
async def ws_heatmap(websocket: WebSocket):
    """WebSocket 推送行业分组数据。

    后端合并行业分类+行情快照，推 {行业: [{code, name, pct}, ...]} 结构。
    前端只管渲染，不做任何 code 匹配。
    """
    import asyncio
    from realtime.feed import get_feed

    await websocket.accept()
    _log.info("heatmap ws client connected")

    queue: asyncio.Queue = asyncio.Queue()

    def on_update(df: pd.DataFrame):
        """Feed 回调：后端合并行业+行情，推分组数据。"""
        try:
            data = _build_heatmap_data()
            if data:
                queue.put_nowait(data)
        except Exception:
            _log.exception("heatmap callback failed")

    feed = get_feed()
    sub_id = feed.subscribe([], on_update)

    try:
        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=30)
                await websocket.send_json(data)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except Exception:
        _log.info("heatmap ws client disconnected")
    finally:
        feed.unsubscribe(sub_id)


# ---- 启动 ----

if __name__ == "__main__":
    print("加载行业分类数据...")
    _load_industry()
    print(f"行业分类: {len(_industry_map)} 只股票, {len(set(v['industry'] for v in _industry_map.values()))} 个行业")
    # 启动 Feed 轮询
    from realtime.feed import start_feed
    start_feed()
    print("启动热力图服务: http://127.0.0.1:9999")
    uvicorn.run(app, host="0.0.0.0", port=9999, ws_max_size=None)
