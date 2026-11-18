#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Author: bboysoul

import requests
import json
import sys
import os

# 拨测接口：POST（GET 会返回 405），probe_id / token 放在 query 里
# 凭据不落库：在 Zabbix 脚本媒体类型的执行环境里通过环境变量提供
api_url = os.environ.get("PROBE_API_URL", "http://127.0.0.1:8084/probe")
probe_id = os.environ.get("PROBE_ID", "")
token = os.environ.get("PROBE_TOKEN", "")

# Zabbix 脚本媒体类型超时默认 30s，这里留足余量
timeout = 10


def msg(text):
    params = {
        "probe_id": probe_id,
        "token": token,
    }
    # 告警内容放到 body 里，接口当前忽略 body，仅作留档/兼容后续扩展
    json_text = {
        "message": text,
        "subject": sys.argv[2] if len(sys.argv) > 2 else "",
    }
    resp = requests.post(api_url, params=params, json=json_text, timeout=timeout)
    body = resp.text
    print("HTTP %s: %s" % (resp.status_code, body))

    # 返回 0 表示发送成功，非 0 会在 Zabbix 里把该次告警标记为失败并触发重试
    try:
        data = json.loads(body)
    except ValueError:
        return 1
    if resp.status_code != 200 or data.get("status") != 200:
        return 1
    return 0


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("usage: %s <message> [subject]" % os.path.basename(sys.argv[0]))
        sys.exit(2)
    text = sys.argv[1]
    sys.exit(msg(text))
