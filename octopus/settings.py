#!/usr/bin/env python
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# 默认日志位置
DEFAULT_LOG = "{}/logs/octopus.log".format(BASE_DIR)

# 发送数据的中间件
SEND_MIDDLEWARES = {
    "octopus.middlewares.send_kafka.SendKafkaMiddleware": 200,
}

# 需要移除监控的采集程序
REMOVE_INACTIVE_COLLECTORS = []
ALLOWED_INACTIVITY_TIME = 60 * 3  # 3分钟
