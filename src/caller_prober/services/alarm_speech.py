"""
Chinese speech text for duty alarms.

A duty alarm is delivered by phone, so the spoken text must be self-contained
and audible: it spells the probed number digit by digit, names the business
type, explains the failure in plain Chinese, reports how long the call was
connected and how the call ended. Raw machine codes (``CC_EARLY_HANGUP``) are
never spoken - they stay in the webhook JSON and in the logs.
"""

from __future__ import annotations

from caller_prober.constants import ALARM_ACK_DIGIT

_CN_DIGITS = "零一二三四五六七八九"

_FAILURE_SPEECH: dict[str, str] = {
    "NO_ANSWER": "对方无应答",
    "NOT_ANSWERED": "呼叫未接通",
    "SIP_BUSY": "线路忙",
    "SIP_CONGESTION": "网络拥塞",
    "SIP_CALL_REJECTED": "呼叫被拒绝",
    "SIP_CLIENT_SERVER_ERROR": "对端返回错误",
    "SIP_TEMPORARILY_UNAVAILABLE": "线路暂时不可用",
    "SIP_SERVICE_UNAVAILABLE": "线路服务不可用",
    "SIP_NOT_FOUND": "号码不存在",
    "SIP_RESOLUTION_FAILURE": "号码解析失败",
    "ARI_ERROR": "拨号接口异常",
    "ARI_PROTOCOL_ERROR": "拨号接口协议错误",
    "CALL_REJECTED": "呼叫被拒绝",
    "PROBE_TIMEOUT": "拨测超时, 未接通",
    "CC_NO_STASIS": "接通后未进入拨测应用",
    "CC_EARLY_HANGUP": "接通后三秒内早挂机",
    "CC_ONE_WAY_AUDIO": "单通, 没有语音媒体",
    "CC_DTMF_CHANNEL_DEAD": "按键后通道已中断",
    "CC_NO_MEDIA": "没有收到语音媒体",
}

_PROBE_TYPE_SPEECH = {
    "callcenter": "呼叫中心客服号码",
    "direct": "普通直线号码",
}

_HANGUP_CAUSE_SPEECH = {
    16: "正常挂断",
    17: "用户忙",
    18: "无人接听",
    19: "振铃超时无人接听",
    21: "呼叫被拒绝",
    34: "线路不可用",
    38: "网络不可用",
    41: "网络临时故障",
    42: "网络临时故障",
    44: "无人接听",
    102: "号码无法解析",
    111: "协议错误",
    127: "号码不存在",
    503: "服务不可用",
}


def _int_to_words(value: int) -> str:
    """Converts a non-negative integer (< 10000) to Chinese words."""
    if value < 10:
        return _CN_DIGITS[value]
    if value < 20:
        return "十" + (_CN_DIGITS[value % 10] if value % 10 else "")
    if value < 100:
        return _CN_DIGITS[value // 10] + "十" + (_CN_DIGITS[value % 10] if value % 10 else "")
    for unit, name in ((1000, "千"), (100, "百")):
        if value >= unit:
            head = _CN_DIGITS[value // unit] + name
            rest = value % unit
            if rest == 0:
                return head
            if rest < unit // 10:
                return f"{head}零{_int_to_words(rest)}"
            return head + _int_to_words(rest)
    return str(value)


def speak_digits(value: str | int) -> str:
    """
    Reads a phone number / digit string digit by digit in Chinese.

    Digits are grouped in fours with a space so the listened text is easy to
    follow; the Chinese numeral characters already force digit-by-digit
    synthesis instead of the number being read as a quantity.
    """
    digits = [char for char in str(value) if char.isdigit()]
    if not digits:
        return str(value)
    words = [_CN_DIGITS[int(char)] for char in digits]
    groups: list[str] = []
    index = 0
    while index < len(words):
        remaining = len(words) - index
        # Never leave a dangling single digit ("九五一零 零").
        take = 5 if remaining == 5 else min(4, remaining)
        groups.append("".join(words[index : index + take]))
        index += take
    return " ".join(groups)


def speak_seconds(seconds: float | None) -> str:
    """Reads a connected duration as Chinese words, e.g. ``二点五秒``."""
    value = max(0.0, float(seconds or 0.0))
    whole, tenth = divmod(round(value * 10), 10)
    if tenth:
        return f"{_int_to_words(whole)}点{_CN_DIGITS[tenth]}秒"
    return f"{_int_to_words(whole)}秒"


def build_alarm_speech(
    probe_name: str,
    phone: str,
    probe_type: str | None,
    failure_code: str | None,
    hangup_cause: int | None = None,
    connected_seconds: float | None = 0.0,
    ack_digit: str | None = None,
) -> str:
    """
    Builds the Chinese alarm text spoken to the duty roster.

    Args:
        probe_name: Probe target name as configured.
        phone: Probed destination number.
        probe_type: ``callcenter`` or ``direct``.
        failure_code: Machine failure code extracted by the evaluator.
        hangup_cause: Asterisk hangup cause code, when observed.
        connected_seconds: Connected duration of the failed probe call.
        ack_digit: ACK digit the listener must press (defaults to config).

    Returns:
        str: Self-contained Chinese sentence, safe for TTS and SMS.
    """
    reason = _FAILURE_SPEECH.get(str(failure_code or "").upper(), "未知故障")
    type_text = _PROBE_TYPE_SPEECH.get(str(probe_type or "").lower(), "拨测号码")
    parts = [
        "值班告警。",
        f"拨测号码 {speak_digits(phone)}, {type_text} {probe_name}, 呼叫失败。",
        f"原因: {reason}。",
        f"接通时长 {speak_seconds(connected_seconds)}。",
    ]
    if hangup_cause is not None:
        cause_text = _HANGUP_CAUSE_SPEECH.get(int(hangup_cause))
        if cause_text:
            parts.append(f"挂断原因: {cause_text}。")
    parts.append(f"请按 {speak_digits(ack_digit or ALARM_ACK_DIGIT)} 确认接警。")
    return "".join(parts)
