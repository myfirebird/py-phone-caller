"""
Celery tasks for scheduled probing and duty-group alarm routing.

* ``scan_probes`` - Beat-driven scan of the probe list. Each probe whose cron
  schedule fires in the current minute is dispatched to ``probe_queue``.
* ``run_probe`` - Executes one probe call through ari_prober, evaluates the
  outcome with services.probe_evaluator and, on failure, triggers
  ``dispatch_duty_alarm`` with the extracted failure code.
* ``dispatch_duty_alarm`` - Dual-path alarm: duty-group webhook POST first,
  then sequential voice calls to every roster member until one presses the
  ACK digit.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import suppress
from datetime import UTC, datetime

from py_phone_caller_utils.tasks.celery_task import app

from caller_prober.ari_prober import AlarmCaller, run_probe_call
from caller_prober.config import DutyGroup, load_probe_config
from caller_prober.constants import (
    ALARM_FALLBACK_MEDIA,
    ALARM_TTS_LANG,
    LOG_FORMATTER,
    LOG_LEVEL,
    PROBE_QUEUE_NAME,
    PROBE_SCAN_LOCK_KEY,
    PROBE_SCAN_LOCK_TTL,
    PROBE_TIMEZONE,
)
from caller_prober.exceptions import ProbeConfigError
from caller_prober.records import append_record
from caller_prober.services.alarm_speech import build_alarm_speech
from caller_prober.services.probe_evaluator import evaluate_probe

# Give the prober its own handler instead of calling basicConfig(force=True):
# forcing the root logger replaced Celery's logging in the worker, which then
# dropped every INFO line (scan dispatch, probe verdicts, duty alarms) that
# operators rely on to verify scheduling.
logger = logging.getLogger(__name__)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(LOG_FORMATTER))
    logger.addHandler(_handler)
    logger.setLevel(LOG_LEVEL)
logger.propagate = False


_WORKER_LOOP: asyncio.AbstractEventLoop | None = None


def _run_async(coro):
    """
    Runs a coroutine to completion on a per-process event loop.

    The loop is created once and kept: async DB/HTTP clients (piccolo, aiohttp)
    bind their connection pools to the loop that created them, so closing the
    loop after every call left the next call awaiting futures on a dead loop -
    the worker task then hung forever and the pool stopped executing anything.
    """
    global _WORKER_LOOP
    if _WORKER_LOOP is None or _WORKER_LOOP.is_closed():
        _WORKER_LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(_WORKER_LOOP)
    return _WORKER_LOOP.run_until_complete(coro)


def _local_now() -> datetime:
    """Returns the current time in the probe schedule timezone."""
    from zoneinfo import ZoneInfo

    return datetime.now(UTC).astimezone(ZoneInfo(PROBE_TIMEZONE))


def _probe_due(schedule: str, now: datetime) -> bool:
    """
    True when the 5-field cron expression fires for ``now``.

    ``now`` must already carry the schedule timezone: cron fields are wall-clock
    values (a 10:00 schedule means 10:00 local, not 10:00 UTC).
    """
    from croniter import croniter

    return croniter.match(schedule, now)


@app.task(name="caller_prober.tasks.probe_tasks.scan_probes")
def scan_probes():
    """
    Beat task: dispatch due probes to probe_queue (Redis lock deduplicated).
    """
    import redis as redis_mod
    from py_phone_caller_utils.config import settings

    queue_url = settings.get("QUEUE", {}).get("QUEUE_URL", "redis://redis.lan:6379/7")
    now = _local_now()
    try:
        r = redis_mod.from_url(queue_url, socket_connect_timeout=2)
        if not r.set(PROBE_SCAN_LOCK_KEY, "1", nx=True, ex=PROBE_SCAN_LOCK_TTL):
            return "skipped_locked"
    except Exception as err:  # noqa: BLE001 - lock is best-effort
        logger.debug("Probe scan lock skipped: %s", err)

    try:
        config = load_probe_config()
    except ProbeConfigError as err:
        logger.error("Probe scan aborted, bad config: %s", err)
        return f"config_error_{err}"

    dispatched = 0
    for probe in config.probes:
        if not probe.enabled or not probe.schedule:
            continue
        try:
            due = _probe_due(probe.schedule, now)
        except (ValueError, KeyError) as err:
            logger.error("Invalid cron '%s' for probe %s: %s", probe.schedule, probe.name, err)
            continue
        if due:
            run_probe.apply_async(
                kwargs={
                    "phone": probe.phone,
                    "probe_name": probe.name,
                    "probe_type": probe.probe_type,
                    "timeout": probe.timeout,
                    "max_call_seconds": probe.max_call_seconds,
                    "dtmf_preset": probe.dtmf_digits,
                    "duty_group": probe.duty_group,
                    "source": "schedule",
                },
                queue=PROBE_QUEUE_NAME,
            )
            dispatched += 1
    logger.info(
        "Probe scan dispatched %d/%d probes at %s (%s)",
        dispatched,
        len(config.probes),
        now.strftime("%Y-%m-%d %H:%M:%S"),
        PROBE_TIMEZONE,
    )
    return f"dispatched_{dispatched}"


@app.task(
    name="caller_prober.tasks.probe_tasks.run_probe",
    bind=True,
    max_retries=1,
    default_retry_delay=10,
)
def run_probe(
    self,
    phone,
    probe_name,
    probe_type,
    timeout,
    dtmf_preset,
    duty_group,
    source="schedule",
    max_call_seconds=None,
):
    """
    Executes one probe call and evaluates the verdict.

    Returns a JSON string with start/end timestamps, hangup cause, verdict and
    failure code; on failure it fans out to dispatch_duty_alarm. Every run is
    appended to the probe execution history.
    """
    started_at = time.time()
    task_id = getattr(self.request, "id", None)
    try:
        outcome = _run_async(
            run_probe_call(phone, probe_type, timeout, dtmf_preset, max_call_seconds)
        )
        result = evaluate_probe(outcome, dtmf_preset)
    except Exception as err:
        logger.exception("Probe %s crashed", probe_name)
        finished_at = time.time()
        record = {
            "event": "probe_completed",
            "probe_name": probe_name,
            "phone": phone,
            "probe_type": probe_type,
            "source": source,
            "task_id": task_id,
            "started_at": datetime.fromtimestamp(started_at, UTC).isoformat(),
            "finished_at": datetime.fromtimestamp(finished_at, UTC).isoformat(),
            "duration_seconds": round(finished_at - started_at, 3),
            "answered": False,
            "connected_seconds": 0.0,
            "hangup_cause": None,
            "media_frames": 0,
            "dtmf_sent": False,
            "hung_up_by_probe": False,
            "recording": None,
            "verdict": "FAIL",
            "failure_code": "PROBE_RUNTIME_ERROR",
            "error": str(err)[:400],
            "duty_group": duty_group,
        }
        append_record(record)
        return json.dumps(record)

    finished_at = time.time()

    log_record = {
        "event": "probe_completed",
        "probe_name": probe_name,
        "phone": phone,
        "probe_type": probe_type,
        "source": source,
        "task_id": task_id,
        "started_at": datetime.fromtimestamp(started_at, UTC).isoformat(),
        "finished_at": datetime.fromtimestamp(finished_at, UTC).isoformat(),
        "duration_seconds": round(finished_at - started_at, 3),
        "answered": outcome.answered,
        "connected_seconds": round(outcome.connected_seconds, 3),
        "hangup_cause": outcome.hangup_cause,
        "hangup_cause_txt": outcome.hangup_cause_txt,
        "media_frames": outcome.media_frames_observed,
        "dtmf_sent": outcome.dtmf_digits_sent,
        "hung_up_by_probe": outcome.hung_up_by_probe,
        "recording": outcome.recording_name,
        "verdict": result.verdict,
        "failure_code": result.failure_code,
        "duty_group": duty_group,
    }
    append_record(log_record)
    logger.info("PROBE %s", json.dumps(log_record, ensure_ascii=False))

    if not result.ok and result.failure_code:
        dispatch_duty_alarm.apply_async(
            kwargs={
                "probe_name": probe_name,
                "phone": phone,
                "probe_type": probe_type,
                "failure_code": result.failure_code,
                "hangup_cause": outcome.hangup_cause,
                "connected_seconds": round(outcome.connected_seconds, 3),
                "duty_group": duty_group,
                "probe_task_id": task_id,
            },
            queue=PROBE_QUEUE_NAME,
        )
    return json.dumps(log_record)


# Webhook endpoint suffixes this platform serves; the suffix decides which
# channels the receiver fires. Aligning it with notification_type keeps the
# stored URL from silently overriding the configured mode.
_WEBHOOK_MODE_ROUTES = {
    "call": "call",
    "sms": "sms",
    "call_and_sms": "call_and_sms",
    "sms_before_call": "sms_before_call",
}
_KNOWN_WEBHOOK_SUFFIXES = {
    "call",
    "call_only",
    "sms",
    "sms_only",
    "sms_before_call",
    "call_and_sms",
    "alert",
    "webhook",
}


def _aligned_webhook_url(group) -> str:
    """
    Returns the group webhook URL matching its notification type.

    The receiver decides the channel mix from the URL suffix (/call, /sms,
    /call_and_sms, /sms_before_call). A stored URL that drifts from
    ``notification_type`` made "call only" groups still request SMS, so the
    suffix is rewritten for every known platform endpoint. Custom third-party
    URLs are left untouched.

    Args:
        group: Duty group with ``webhook_url`` and ``notification_type``.

    Returns:
        str: URL to POST the alarm summary to.
    """
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    url = str(group.webhook_url or "")
    mode = str(getattr(group, "notification_type", "") or "call_and_sms").strip().lower()
    route = _WEBHOOK_MODE_ROUTES.get(mode)
    if not route or not url:
        return url
    parts = urlsplit(url)
    path = parts.path.rstrip("/")
    head, _, suffix = path.rpartition("/")
    if suffix not in _KNOWN_WEBHOOK_SUFFIXES:
        return url
    new_path = f"{head}/{route}" if head else f"/{route}"

    # Prefer the immutable group id over the (renameable) group name.
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    group_id = str(getattr(group, "id", "") or "")
    if group_id:
        query.pop("group", None)
        query = {"group_id": group_id, **{k: v for k, v in query.items() if k != "group_id"}}
    return urlunsplit(
        (parts.scheme, parts.netloc, new_path, urlencode(query), parts.fragment)
    )


def _persist_alarm_calls(group, alarm_text, details, failure_code, probe_name, probe_phone):
    """
    Writes every alarm call attempt into the Managed Calls table.

    The prober dials the duty roster through ARI directly, so it records the
    calls itself; otherwise the alert trail never shows up next to the other
    hosted calls in the UI.
    """

    async def _insert(detail: dict) -> None:
        from datetime import datetime

        from py_phone_caller_utils.py_phone_caller_db.py_phone_caller_piccolo_app.tables import (
            Calls,
        )

        def _naive(value: str | None, fallback: datetime | None = None) -> datetime:
            if not value:
                return fallback or datetime.min  # noqa: DTZ901 - DB stores naive UTC
            return datetime.fromisoformat(value).replace(tzinfo=None)

        started = _naive(detail.get("started_at"))
        answered = detail.get("answered_at")
        acked = detail.get("acked_at")
        channel = str(detail.get("channel_id") or "")
        await Calls.insert(
            Calls(
                phone=str(detail.get("phone") or ""),
                message=alarm_text,
                asterisk_chan=channel,
                msg_chk_sum=f"alarm:{probe_name}:{failure_code}"[:64],
                call_chk_sum=str(probe_phone)[:64],
                unique_chk_sum=(channel or detail.get("started_at") or "")[:64],
                times_to_dial=1,
                dialed_times=1,
                seconds_to_forget=int(group.escalation_timeout),
                first_dial=started,
                last_dial=started,
                heard_at=_naive(answered) if answered else started,
                acknowledge_at=_naive(acked) if acked else datetime.min,  # noqa: DTZ901
                cycle_done=True,
                oncall=True,
                backup_callee=False,
                lang=ALARM_TTS_LANG,
            )
        )

    for detail in details:
        with suppress(Exception):
            _run_async(_insert(detail))


@app.task(name="caller_prober.tasks.probe_tasks.dispatch_duty_alarm")
def dispatch_duty_alarm(
    probe_name,
    phone,
    probe_type,
    failure_code,
    hangup_cause,
    connected_seconds,
    duty_group,
    probe_task_id=None,
):
    """
    Dual-path duty alarm for one failed probe.

    1. POST the JSON alarm summary to the group's webhook (strict timeout).
    2. Sequentially call each roster member with TTS playback; pressing the ACK
       digit stops escalation, timeout/reject immediately escalates onward.
    """
    try:
        config = load_probe_config()
        group: DutyGroup = config.duty_groups[duty_group]
    except (ProbeConfigError, KeyError) as err:
        logger.error("Duty alarm aborted, unknown group '%s': %s", duty_group, err)
        return f"unknown_group_{duty_group}"

    summary = {
        "event": "probe_alarm",
        "probe_name": probe_name,
        "phone": phone,
        "probe_type": probe_type,
        "failure_code": failure_code,
        "hangup_cause": hangup_cause,
        "connected_seconds": connected_seconds,
        "duty_group": duty_group,
        "triggered_at": datetime.now(UTC).isoformat(),
    }
    dispatch_log = {"webhook_ok": False, "acked_by": None, "called": []}

    caller = AlarmCaller()
    notif_type = str(getattr(group, "notification_type", "call_and_sms") or "").strip().lower()
    webhook_url = _aligned_webhook_url(group)
    webhook_ok = _run_async(caller.post_webhook(webhook_url, summary))
    dispatch_log["webhook_ok"] = webhook_ok
    logger.info(
        "DUTY_ALARM webhook group=%s status=%s probe=%s code=%s url=%s",
        duty_group,
        "delivered" if webhook_ok else "failed",
        probe_name,
        failure_code,
        webhook_url,
    )

    alarm_text = build_alarm_speech(
        probe_name=probe_name,
        phone=phone,
        probe_type=probe_type,
        failure_code=failure_code,
        hangup_cause=hangup_cause,
        connected_seconds=connected_seconds,
    )
    logger.info(
        "DUTY_ALARM speech group=%s probe=%s phone=%s text=%s",
        duty_group,
        probe_name,
        phone,
        alarm_text,
    )
    tts_media = _run_async(caller.generate_tts_media(alarm_text))
    if not tts_media:
        logger.warning(
            "DUTY_ALARM TTS unavailable for probe=%s; playing preset local alert %s",
            probe_name,
            ALARM_FALLBACK_MEDIA,
        )
    sends_sms = notif_type in ("sms", "call_and_sms", "sms_before_call")
    places_calls = notif_type != "sms"
    logger.info(
        "DUTY_ALARM channels group=%s notification_type=%s sms=%s calls=%s",
        duty_group,
        notif_type,
        "enabled" if sends_sms else "skipped",
        "enabled" if places_calls else "skipped",
    )
    if sends_sms:
        from py_phone_caller_utils.tasks.celery_task import dispatch_async_sms

        for member in group.members:
            if member.phone:
                with suppress(Exception):
                    dispatch_async_sms.apply_async(
                        args=[member.phone, alarm_text],
                        queue="telephony.p2",
                    )

    if places_calls:
        for member in group.members:
            dispatch_log["called"].append(member.phone)
            logger.info(
                "DUTY_ALARM calling member=%s phone=%s group=%s", member.name, member.phone, duty_group
            )
            acked = _run_async(
                caller.call_member_for_ack(
                    member.phone, alarm_text, tts_media, group.escalation_timeout
                )
            )
            if acked:
                dispatch_log["acked_by"] = member.name
                logger.info(
                    "DUTY_ALARM ACK received from member=%s phone=%s group=%s - escalation stopped",
                    member.name,
                    member.phone,
                    duty_group,
                )
                break
            logger.warning(
                "DUTY_ALARM member=%s phone=%s did not ACK - escalating to next member",
                member.name,
                member.phone,
            )

    details = list(getattr(caller, "call_details", []))
    dispatch_log["calls"] = details
    append_record(
        {
            "event": "duty_alarm",
            "probe_task_id": probe_task_id,
            "probe_name": probe_name,
            "phone": phone,
            "probe_type": probe_type,
            "failure_code": failure_code,
            "duty_group": duty_group,
            "webhook_ok": webhook_ok,
            "acked_by": dispatch_log["acked_by"],
            "calls": details,
            "triggered_at": summary["triggered_at"],
            "finished_at": datetime.now(UTC).isoformat(),
        }
    )
    _persist_alarm_calls(group, alarm_text, details, failure_code, probe_name, phone)
    logger.info("DUTY_ALARM dispatch trail: %s", json.dumps(dispatch_log))
    return json.dumps(dispatch_log)
