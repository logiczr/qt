FROM python:3.10-slim

WORKDIR /app

# 系统依赖（mootdx 需要的 C 库）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 项目代码
COPY . .

# 数据和日志目录
RUN mkdir -p data/bronze/f10 data/silver/f10 data/industry logs

# 数据库挂载点（volume 覆盖）
VOLUME /app/data
VOLUME /app/logs

# 端口：serve 8000, realtime 8888
EXPOSE 8000 8888

# 首次部署自动初始化（setup检测已有数据则跳过），然后启动服务
CMD ["sh", "-c", "python setup.py && python main.py"]
