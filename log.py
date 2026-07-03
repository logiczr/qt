"""全局日志配置。

日志同时输出到控制台和文件：
- 控制台：实时查看
- 文件：logs/qt.log，每天轮转，保留 30 天

使用方式（在项目入口调用一次）:
    from log import setup
    setup()

各模块正常用:
    import logging
    _log = logging.getLogger(__name__)
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler

_LOG_DIR = os.path.dirname(__file__)
_FMT = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

_setup_done = False


def setup():
    """初始化日志：控制台 + 文件轮转。全局只调一次。"""
    global _setup_done
    if _setup_done:
        return
    _setup_done = True

    os.makedirs(_LOG_DIR, exist_ok=True)

    # 控制台
    console = logging.StreamHandler()
    console.setFormatter(_FMT)

    # 文件：每天轮转，保留 30 天
    file_handler = TimedRotatingFileHandler(
        os.path.join(_LOG_DIR, "qt.log"),
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(_FMT)

    logging.basicConfig(level=logging.INFO, handlers=[console, file_handler])
