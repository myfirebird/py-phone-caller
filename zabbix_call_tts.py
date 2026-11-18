#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Author: bboysoul

"""Zabbix 电话告警：接通后由 Asterisk 用 TTS 播出告警内容。

调用链（均有源码依据）：
    POST http://<webhook>/call?group_id=..&token=..   caller_prometheus_webhook/routes.py:430
      -> AsteriskCallClient.place_call(...)           caller_prometheus_webhook/clients.py:42
      -> POST http://<asterisk_caller>/place_call?phone=..&message=..
      -> ARI 外呼 + caller_register 记录 message
      -> 接通(StasisStart) 后 generate_audio -> play  asterisk_ws_monitor.py:219
即 ``message`` 就是对方接通后听到的语音，所以脚本只做一件事：把 Zabbix 的
告警整理成适合朗读的中文，再交给接口。

Zabbix 脚本媒体类型参数按顺序传入（多余的可以不配）：
    zabbix_call_tts.py "{ALERT.MESSAGE}" "{ALERT.SUBJECT}" "{HOST.NAME}" "{EVENT.SEVERITY}" "{EVENT.DATE} {EVENT.TIME}"

常用开关：
    --dry-run                 只打印整理后的播报文本，不拨号（调话术用）
    --lang z                  指定 TTS 语言（webhook 模式作为 ?lang= 传，直呼模式作为 lang 参数）
    --direct <号码>            跳过 webhook，直呼指定号码

优先级（服务端）：?lang= > 值班组配置的 lang > settings.toml 的 webhook_tts_lang > generate_audio 默认
"""

import json
import os
import re
import sys

import requests

# webhook 模式（推荐）：按值班组广播，带 180s 去重
# 凭据不落库：在 Zabbix 脚本媒体类型的执行环境里通过环境变量提供
API_URL = os.environ.get("CALL_API_URL", "http://127.0.0.1:8084/call")
TOKEN = os.environ.get("CALL_API_TOKEN", "")
GROUP_ID = os.environ.get("CALL_GROUP_ID", "")

# 直呼模式：asterisk_caller 的 /place_call，支持 lang（TTS 语言）
DIRECT_URL = os.environ.get("CALL_DIRECT_URL", "http://127.0.0.1:8081/place_call")
DIRECT_LANG = os.environ.get("CALL_LANG", "")

TIMEOUT = int(os.environ.get("CALL_API_TIMEOUT", "10"))
MAX_SPEECH_CHARS = 240

SEVERITY_CN = {
    "disaster": "灾难",
    "high": "严重",
    "average": "一般",
    "warning": "警告",
    "information": "信息",
    "info": "信息",
    "not classified": "未分类",
    "0": "未分类",
    "1": "信息",
    "2": "警告",
    "3": "一般",
    "4": "严重",
    "5": "灾难",
    "灾难": "灾难",
    "严重": "严重",
    "一般": "一般",
    "警告": "警告",
    "信息": "信息",
    "未分类": "未分类",
}

# Zabbix 默认消息里的机器字段行：朗读时丢弃（主机/级别由参数单独播报）
_BOILERPLATE_PREFIXES = (
    "problem started",
    "event started",
    "problem name:",  # 只保留值
    "host:",
    "hostname:",
    "severity:",
    "event id:",
    "event name:",
    "event time:",
    "original problem id",
    "operational data",
    "trigger:",
    "故障开始",
    "事件开始",
    "故障名称:",  # 只保留值
    "问题名称:",  # 只保留值
    "主机:",
    "主机：",
    "严重性:",
    "严重性：",
    "告警级别",
    "事件id",
    "事件 id",
    "事件名称",
    "原始问题id",
    "原始问题 id",
    "运维数据",
)

_NAME_PREFIXES = ("problem name:", "故障名称:", "问题名称:")

_SUBJECT_PREFIX_RE = re.compile(
    r"^(problem|resolved|recovery|firing|故障|告警|恢复|已恢复)\s*[:：]\s*", re.IGNORECASE
)

# TTS 读不好的符号 -> 中文读法
_DATE_RE = re.compile(r"\b(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})\b")
_TIME_RE = re.compile(r"\b(\d{1,2}):(\d{2})(?::(\d{2}))?\b")

_SPEECH_RULES = (
    (re.compile(r"https?://\S+"), " 链接 "),
    (re.compile(r"(\d+(?:\.\d+)?)\s*%"), r"百分之\1"),
    (re.compile(r">="), "大于等于 "),
    (re.compile(r"<="), "小于等于 "),
    (re.compile(r"!="), "不等于 "),
    (re.compile(r"=="), "等于 "),
    (re.compile(r">"), "大于 "),
    (re.compile(r"<"), "小于 "),
    (re.compile(r"%"), "百分之"),
    (re.compile(r"[*_`#|\\]+"), " "),
    (re.compile(r"[ \t]+"), " "),
)


def _is_resolved(subject: str, severity: str) -> bool:
    """判断是否为恢复通知（Zabbix 恢复消息或 severity=Resolved）。"""
    head = (subject or "").strip().lower()
    level = (severity or "").strip().lower()
    if level in ("resolved", "recovery", "ok", "恢复", "已恢复"):
        return True
    return head.startswith(("resolved", "recovery", "ok:")) or "恢复" in head[:8]


def _trim_line(line: str, drop_boilerplate: bool) -> str:
    """丢弃 Zabbix 机器字段行，只保留人类可读内容。"""
    lower = line.lower()
    for prefix in _NAME_PREFIXES:
        if lower.startswith(prefix):
            return line[len(prefix):].strip()
    if drop_boilerplate:
        for prefix in _BOILERPLATE_PREFIXES:
            if lower.startswith(prefix):
                return ""
    return line


def _clean(text: str, drop_boilerplate: bool = False) -> str:
    """把告警原文整理成可以朗读的中文。"""
    if not text:
        return ""
    lines = []
    for raw in str(text).splitlines():
        line = raw.strip()
        if not line:
            continue
        line = _trim_line(line, drop_boilerplate)
        if line:
            lines.append(line)
    body = "。".join(lines)
    body = _DATE_RE.sub(r"\1年\2月\3日", body)
    body = _TIME_RE.sub(lambda m: f"{m.group(1)}点{m.group(2)}分" + (f"{m.group(3)}秒" if m.group(3) else ""), body)
    for pattern, replacement in _SPEECH_RULES:
        body = pattern.sub(replacement, body)
    body = re.sub(r"[，,、;；]{2,}", "，", body)
    body = re.sub(r"[。.]{2,}", "。", body)
    return body.strip(" 。，,.;；:：-")


def _limit(text: str, limit: int = MAX_SPEECH_CHARS) -> str:
    """限制播报长度（TTS 与通话时长都别太长），尽量断在句号处。"""
    if len(text) <= limit:
        return text
    cut = text[:limit]
    for sep in ("。", "，", ",", ";"):
        index = cut.rfind(sep)
        if index > limit // 2:
            return cut[:index] + "。"
    return cut + "。"


def compose_speech(
    message: str = "",
    subject: str = "",
    host: str = "",
    severity: str = "",
    event_time: str = "",
) -> str:
    """把 Zabbix 告警字段拼成一段自解释的中文播报稿。"""
    level = SEVERITY_CN.get((severity or "").strip().lower(), "")
    resolved = _is_resolved(subject, severity)

    if resolved:
        parts = ["告警恢复。"]
    elif level:
        parts = [f"{level}告警。"]
    else:
        parts = ["值班告警。"]

    subject_text = _clean(_SUBJECT_PREFIX_RE.sub("", (subject or "").strip()))
    if subject_text:
        parts.append(subject_text + "。")

    if host:
        parts.append(f"主机 {_clean(host)}。")

    body = _clean(message, drop_boilerplate=bool(host or severity))
    if body:
        parts.append(body + "。")

    if event_time:
        parts.append(f"时间 {_clean(event_time)}。")

    parts.append("本语音由 Zabbix 监控自动播报。")
    return _limit("".join(parts))


def _parse_args(argv):
    """解析参数：前 5 个位置参数按 Zabbix 媒体类型顺序映射，另支持开关。"""
    opts = {"dry_run": False, "raw": False, "direct": "", "lang": DIRECT_LANG, "positional": []}
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg in ("-n", "--dry-run"):
            opts["dry_run"] = True
        elif arg in ("--raw", "--verbatim"):
            opts["raw"] = True
        elif arg in ("--direct", "--phone"):
            index += 1
            opts["direct"] = argv[index] if index < len(argv) else ""
        elif arg in ("--lang", "--language"):
            index += 1
            opts["lang"] = argv[index] if index < len(argv) else ""
        else:
            opts["positional"].append(arg)
        index += 1
    return opts


def _post(url, params, payload=None):
    """发请求并打印结果，返回 0 成功 / 1 失败。"""
    try:
        resp = requests.post(url, params=params, json=payload, timeout=TIMEOUT)
    except requests.RequestException as err:
        print(f"请求失败: {url}: {err}")
        return 1
    print(f"HTTP {resp.status_code}: {resp.text}")
    if resp.status_code != 200:
        return 1
    try:
        data = json.loads(resp.text)
    except ValueError:
        return 0
    # /call 返回 {"status": "200"}，/place_call 返回 {"status": 200}
    return 0 if str(data.get("status")) == "200" else 1


def main(argv):
    opts = _parse_args(argv)
    positional = opts["positional"]
    message = positional[0] if len(positional) > 0 else ""
    subject = positional[1] if len(positional) > 1 else ""
    host = positional[2] if len(positional) > 2 else ""
    severity = positional[3] if len(positional) > 3 else ""
    event_time = positional[4] if len(positional) > 4 else ""

    if not any(value.strip() for value in (message, subject, host, severity, event_time)):
        print("没有可播报的内容：至少传入 {ALERT.MESSAGE} 或 {ALERT.SUBJECT}")
        return 2

    if opts["raw"]:
        speech = "。".join(value.strip("。") for value in positional if value.strip())
        if speech and not speech.endswith("。"):
            speech += "。"
    else:
        speech = compose_speech(message, subject, host, severity, event_time)
    print(f"播报内容({len(speech)}字): {speech}")
    if opts["dry_run"]:
        return 0

    if opts["direct"]:
        params = {"phone": opts["direct"], "message": speech}
        if opts["lang"]:
            params["lang"] = opts["lang"]
        return _post(DIRECT_URL, params)

    query = {"group_id": GROUP_ID, "token": TOKEN}
    if opts["lang"]:
        query["lang"] = opts["lang"]
    return _post(API_URL, query, {"message": speech})


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
