"""Realtime 层 HTTP 传输 — FastAPI REST + WebSocket。

只做：参数解析、鉴权、调 feed.py、返回 JSON / 推送。
不写任何行情逻辑。
"""

import asyncio
import json
import logging
import os

from fastapi import FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect

from realtime.feed import get_feed

_log = logging.getLogger(__name__)

app = FastAPI(title="QuantDB Realtime", version="0.1")


# ---- 鉴权 ----

_API_KEYS: set[str] | None = None


def _get_api_keys() -> set[str]:
    global _API_KEYS
    if _API_KEYS is None:
        raw = os.getenv("QUANTDB_API_KEYS", "")
        _API_KEYS = set(k.strip() for k in raw.split(",") if k.strip())
        if _API_KEYS:
            _log.info("API key auth enabled, %d keys", len(_API_KEYS))
        else:
            _log.info("API key auth disabled (no QUANTDB_API_KEYS set)")
    return _API_KEYS


async def verify_api_key(x_api_key: str = Header(default="")):
    """API Key 鉴权中间件。未配置 QUANTDB_API_KEYS 则不鉴权。"""
    keys = _get_api_keys()
    if not keys:
        return  # 无配置，不鉴权
    if x_api_key not in keys:
        raise HTTPException(status_code=401, detail="invalid API key")


# ---- REST ----

@app.get("/api/quotes")
async def quotes(
    codes: str = Query(..., description="逗号分隔的股票代码，如 sh600519,sz000001"),
    _auth=Header(default=""),
):
    """查询当前快照（客户端轮询模式）。直接返回内存中 _df 的切片。"""
    await verify_api_key(_auth)
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    df = get_feed().get_snapshot(code_list)
    return json.loads(df.to_json(orient="records"))


@app.get("/api/quotes/all")
async def quotes_all(_auth: str = Header(default="")):
    """全市场快照。"""
    await verify_api_key(_auth)
    df = get_feed().get_snapshot()
    return json.loads(df.to_json(orient="records"))


# ---- WebSocket ----

@app.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket, codes: str = ""):
    """WebSocket 推送。

    客户端连接时传 codes 参数订阅股票，
    Feed 每轮轮询完成后触发回调 → 推送更新给客户端。

    用法: ws://host:8888/ws/quotes?codes=sh600519,sz000001
    """
    await websocket.accept()
    code_list = [c.strip() for c in codes.split(",") if c.strip()]

    # asyncio Queue 用于线程 → asyncio 事件循环桥接
    queue: asyncio.Queue = asyncio.Queue()

    def on_update(df):
        """Feed 回调（后台线程执行），把数据放进 asyncio Queue。"""
        try:
            data = json.loads(df.to_json(orient="records"))
            # put_nowait 是线程安全的（asyncio.Queue 内部有锁）
            queue.put_nowait(data)
        except Exception:
            _log.exception("ws callback push to queue failed")

    sub_id = get_feed().subscribe(code_list, on_update)
    _log.info("ws client connected, sub_id=%s, %d codes", sub_id, len(code_list))

    try:
        while True:
            # 30秒超时：发送 ping 保活
            try:
                data = await asyncio.wait_for(queue.get(), timeout=30)
                await websocket.send_json(data)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        _log.info("ws client disconnected, sub_id=%s", sub_id)
    except Exception:
        _log.exception("ws error, sub_id=%s", sub_id)
    finally:
        get_feed().unsubscribe(sub_id)


# ---- 启动 ----

def start(host: str = "0.0.0.0", port: int = 8888, interval: float = 0.3):
    """启动 Feed 轮询 + uvicorn HTTP 服务。

    Args:
        host: 监听地址
        port: 监听端口
        interval: Feed 轮询间隔（秒）
    """
    from realtime.feed import start_feed
    start_feed(interval=interval)
    logging.basicConfig(level=logging.INFO)
    import uvicorn
    uvicorn.run(app, host=host, port=port, ws_max_size=None)
