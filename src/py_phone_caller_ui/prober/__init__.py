"""
Prober blueprint for the py-phone-caller UI.

Provides a web console for the caller_prober engine: CRUD on duty groups and
probe targets stored in probe_config.yml, an immediate probe-run trigger that
dispatches run_probe to the Celery probe_queue, and a duty-group webhook
connectivity test.
"""

import logging
from dataclasses import replace
from datetime import UTC, datetime
from math import ceil

import pytz
from flask import Blueprint, jsonify, render_template, request
from flask_login import login_required
from py_phone_caller_utils.config import settings
from py_phone_caller_utils.tasks.celery_task import app as celery_app

from caller_prober.ari_prober import AlarmCaller
from caller_prober.config import (
    DEFAULT_MAX_CALL_SECONDS,
    DutyGroup,
    DutyMember,
    ProbeConfig,
    ProbeTarget,
    load_probe_config,
    parse_enabled,
    save_probe_config,
)
from caller_prober.exceptions import ProbeConfigError

logger = logging.getLogger(__name__)

prober_blueprint = Blueprint(
    "prober_blueprint",
    __name__,
    template_folder="templates/prober",
)

MAX_MEMBERS = 32
MAX_PROBES = 256
# Pagination mirrors the Managed Calls page (same sizes, same default).
RECORDS_PAGE_SIZES = (5, 10, 25, 50, 100)
RECORDS_PAGE_SIZE = 50
LOCAL_TZ = pytz.timezone(settings.scheduled_calls.local_timezone)


def _local_time(iso_value: str | None) -> str:
    """
    Renders a stored UTC ISO timestamp in the platform's local timezone.

    Args:
        iso_value: ISO-8601 timestamp from an execution record (UTC).

    Returns:
        str: ``YYYY-MM-DD HH:MM:SS`` in the configured timezone, or ``—``.
    """
    if not iso_value:
        return "—"
    try:
        moment = datetime.fromisoformat(str(iso_value))
    except ValueError:
        return str(iso_value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _filter_history(history, search_query: str) -> list[dict[str, object]]:
    """
    Filters probe executions by a free-text query.

    Matches target name, phone, type, source, verdict, failure code and the
    duty numbers called for the failure - the same "type anything" behaviour as
    the Managed Calls search box.

    Args:
        history: Probe runs (newest first) with alarm calls attached.
        search_query: Raw query string (case-insensitive substring match).

    Returns:
        list: Matching runs, or all of them when the query is empty.
    """
    if not search_query:
        return list(history)
    needle = search_query.casefold()
    matches = []
    for row in history:
        alarm_phones = " ".join(
            str(call.get("phone") or "") for call in row.get("alarm_calls") or []
        )
        haystack = " ".join(
            str(value or "")
            for value in (
                row.get("probe_name"),
                row.get("phone"),
                row.get("probe_type"),
                row.get("source"),
                row.get("verdict"),
                row.get("failure_code"),
                row.get("duty_group"),
                alarm_phones,
            )
        ).casefold()
        if needle in haystack:
            matches.append(row)
    return matches


def _records_page_params() -> tuple[int, int]:
    """Returns the validated (page, per_page) pair from the query string."""
    per_page = request.args.get("per_page", default=RECORDS_PAGE_SIZE, type=int)
    if per_page not in RECORDS_PAGE_SIZES:
        per_page = RECORDS_PAGE_SIZE
    page = request.args.get("page", default=1, type=int) or 1
    return max(1, page), per_page


@prober_blueprint.route("/prober")
@login_required
async def prober():
    """
    Renders the probe configuration console.

    Returns:
        flask.Response: Rendered prober page with the current duty groups,
        probe targets, and the resolved config file path.
    """
    active_tab = request.args.get("tab", "groups").strip().lower()
    if active_tab not in ("groups", "probes"):
        active_tab = "groups"
    error = None
    config = None
    config_path = ""
    groups_json: dict[str, dict[str, object]] = {}
    try:
        config = load_probe_config()
        from caller_prober.config import _probe_config_path

        config_path = str(_probe_config_path())
        groups_json = {
            name: {
                "name": name,
                "members": [{"name": m.name, "phone": m.phone} for m in group.members],
                "escalation_timeout": group.escalation_timeout,
                "webhook_url": group.webhook_url,
                "notification_type": getattr(group, "notification_type", "call_and_sms"),
                "token": getattr(group, "token", ""),
                "id": getattr(group, "id", ""),
                "lang": getattr(group, "lang", ""),
            }
            for name, group in config.duty_groups.items()
        }
    except ProbeConfigError as err:
        error = str(err)
    return render_template(
        "prober.html",
        config=config,
        config_path=config_path,
        groups_json=groups_json,
        active_tab=active_tab,
        error=error,
    )


@prober_blueprint.route("/prober/records")
@login_required
async def prober_records():
    """
    Renders the probe execution history as its own page.

    Returns:
        flask.Response: Rendered records page, newest executions first, each
        row carrying the duty numbers that were called for a failure.
    """
    from caller_prober.config import _probe_config_path
    from caller_prober.records import read_probe_history, records_path

    error = None
    records: list[dict[str, object]] = []
    config_path = ""
    records_file = ""
    search_query = request.args.get("search", "").strip()
    page, per_page = _records_page_params()
    total = 0
    pages = 1
    try:
        config_path = str(_probe_config_path())
        records_file = str(records_path())
        history = _filter_history(read_probe_history(limit=0), search_query)
        total = len(history)
        pages = max(1, ceil(total / per_page))
        page = min(page, pages)
        start = (page - 1) * per_page
        records = history[start : start + per_page]
        for row in records:
            row["local_time"] = _local_time(
                row.get("finished_at") or row.get("started_at")
            )
    except ProbeConfigError as err:
        error = str(err)
    return render_template(
        "records.html",
        records=records,
        config_path=config_path,
        records_file=records_file,
        error=error,
        search_query=search_query,
        page=page,
        total_pages=pages,
        total=total,
        per_page=per_page,
        page_sizes=RECORDS_PAGE_SIZES,
        local_tz=str(settings.scheduled_calls.local_timezone),
    )


def _group_members_from_payload(payload) -> list[DutyMember]:
    """Parses the members array of a duty-group payload."""
    members_raw = payload.get("members") or []
    members = []
    for m in members_raw[:MAX_MEMBERS]:
        name = str(m.get("name", "")).strip()
        phone = str(m.get("phone", "")).strip()
        if name and phone:
            members.append(DutyMember(name=name, phone=phone))
    if not members:
        raise ProbeConfigError("A duty group needs at least one member")
    return members


def _mutate(load, save):
    """
    Loads the current config, applies the mutation callback, and saves.

    Returns:
        tuple: (ok: bool, detail: str).
    """
    try:
        config = load()
        detail = save(config)
        save_probe_config(config)
        return True, detail or "saved"
    except ProbeConfigError as err:
        return False, str(err)
    except OSError as err:
        return False, f"Cannot write probe config: {err}"


@prober_blueprint.route("/prober/api/groups/save", methods=["POST"])
@login_required
async def api_save_group():
    """
    Adds or updates a duty group (keyed by group name).

    Returns:
        flask.Response: JSON {status, message}.
    """
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()
    if not name:
        return jsonify({"status": 400, "message": "Group name is required"}), 400
    webhook = str(payload.get("webhook_url", "")).strip()
    if not webhook.startswith(("http://", "https://")):
        return jsonify({"status": 400, "message": "A valid webhook URL is required"}), 400
    try:
        escalation = float(payload.get("escalation_timeout", 60))
    except (TypeError, ValueError):
        return jsonify({"status": 400, "message": "escalation_timeout must be a number"}), 400
    if escalation <= 0:
        return jsonify({"status": 400, "message": "escalation_timeout must be positive"}), 400

    notif_type = str(payload.get("notification_type") or "call_and_sms").strip()
    if notif_type not in ("call", "sms", "call_and_sms", "sms_before_call"):
        notif_type = "call_and_sms"
    token = str(payload.get("token") or "").strip()
    group_id = str(payload.get("id") or "").strip()
    lang = str(payload.get("lang") or "").strip()
    if not token:
        import secrets

        token = secrets.token_urlsafe(16)

    def apply(config: ProbeConfig) -> str:
        try:
            members = _group_members_from_payload(payload)
        except ProbeConfigError as err:
            raise ProbeConfigError(str(err)) from err
        from uuid import uuid4

        existing = config.duty_groups.get(name)
        if existing is None and group_id:
            existing = next(
                (g for g in config.duty_groups.values() if g.id == group_id), None
            )
        stable_id = (existing.id if existing else "") or group_id or uuid4().hex
        groups = dict(config.duty_groups)
        groups[name] = DutyGroup(
            name=name,
            members=tuple(members),
            escalation_timeout=escalation,
            webhook_url=webhook,
            notification_type=notif_type,
            token=token,
            id=stable_id,
            lang=lang,
        )
        object.__setattr__(config, "duty_groups", groups)
        return f"Group '{name}' saved with {len(members)} member(s)"
    ok, detail = _mutate(load_probe_config, apply)
    status = 200 if ok else 400
    return jsonify({"status": status, "message": detail}), status


@prober_blueprint.route("/prober/api/groups/delete", methods=["POST"])
@login_required
async def api_delete_group():
    """
    Deletes a duty group. Refuses when probes still bind to it.

    Returns:
        flask.Response: JSON {status, message}.
    """
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()

    def apply(config: ProbeConfig) -> str:
        if name not in config.duty_groups:
            raise ProbeConfigError(f"Unknown group '{name}'")
        bound = [p.name for p in config.probes if p.duty_group == name]
        if bound:
            raise ProbeConfigError(
                f"Group '{name}' is bound to probes: {', '.join(bound)}. Remove them first."
            )
        groups = dict(config.duty_groups)
        del groups[name]
        object.__setattr__(config, "duty_groups", groups)
        return f"Group '{name}' deleted"

    ok, detail = _mutate(load_probe_config, apply)
    status = 200 if ok else 400
    return jsonify({"status": status, "message": detail}), status

@prober_blueprint.route("/prober/api/groups/regen_token", methods=["POST"])
@login_required
async def api_regen_group_token():
    """Regenerates a secure random token for a duty group."""
    import secrets

    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()
    if not name:
        return jsonify({"status": 400, "message": "Group name is required"}), 400

    new_token = secrets.token_urlsafe(16)

    def apply(config: ProbeConfig) -> str:
        if name not in config.duty_groups:
            raise ProbeConfigError(f"Unknown group '{name}'")
        g = config.duty_groups[name]
        groups = dict(config.duty_groups)
        groups[name] = DutyGroup(
            name=g.name,
            members=g.members,
            escalation_timeout=g.escalation_timeout,
            webhook_url=g.webhook_url,
            notification_type=g.notification_type,
            token=new_token,
            id=g.id,
            lang=getattr(g, "lang", ""),
        )
        object.__setattr__(config, "duty_groups", groups)
        return f"Token for group '{name}' regenerated"

    ok, detail = _mutate(load_probe_config, apply)
    status = 200 if ok else 400
    return jsonify({"status": status, "token": new_token if ok else "", "message": detail}), status


@prober_blueprint.route("/prober/api/token/generate", methods=["GET", "POST"])
@login_required
async def api_generate_token():
    """Generates a fresh secure random token."""
    import secrets

    return jsonify({"status": 200, "token": secrets.token_urlsafe(16)})


@prober_blueprint.route("/prober/api/probes/save", methods=["POST"])
@login_required
async def api_save_probe():
    """
    Adds or updates a probe target (keyed by probe name).

    Returns:
        flask.Response: JSON {status, message}.
    """
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()
    phone = str(payload.get("phone", "")).strip()
    probe_type = str(payload.get("type", "direct")).strip()
    schedule = str(payload.get("schedule", "")).strip()
    duty_group = str(payload.get("duty_group", "")).strip()
    if not name or not phone:
        return jsonify({"status": 400, "message": "Name and phone are required"}), 400
    if probe_type not in ("callcenter", "direct"):
        return jsonify({"status": 400, "message": "type must be callcenter or direct"}), 400
    if not duty_group:
        return jsonify({"status": 400, "message": "duty_group is required"}), 400
    if schedule:
        try:
            from croniter import croniter

            croniter(schedule)
        except (ValueError, KeyError) as err:
            return jsonify({"status": 400, "message": f"Invalid cron schedule: {err}"}), 400
    try:
        timeout = float(payload.get("timeout", 60))
    except (TypeError, ValueError):
        return jsonify({"status": 400, "message": "timeout must be a number"}), 400
    if timeout <= 0:
        return jsonify({"status": 400, "message": "timeout must be positive"}), 400
    try:
        max_call = float(payload.get("max_call_seconds", DEFAULT_MAX_CALL_SECONDS))
    except (TypeError, ValueError):
        return jsonify({"status": 400, "message": "max_call_seconds must be a number"}), 400
    if max_call <= 0:
        return jsonify({"status": 400, "message": "max_call_seconds must be positive"}), 400
    dtmf = str(payload.get("dtmf_digits", "")).strip() or None
    token = str(payload.get("token") or "").strip()
    probe_id = str(payload.get("id") or "").strip()

    def apply(config: ProbeConfig) -> str:
        if duty_group not in config.duty_groups:
            raise ProbeConfigError(f"Unknown duty group '{duty_group}'")
        import secrets
        from uuid import uuid4

        existing = next((p for p in config.probes if p.name == name), None)
        if existing is None and probe_id:
            existing = next((p for p in config.probes if p.id == probe_id), None)
        probe_token = token or (existing.token if existing else "") or secrets.token_urlsafe(16)
        stable_id = (existing.id if existing else "") or probe_id or uuid4().hex
        new_probe = ProbeTarget(
            phone=phone,
            name=name,
            probe_type=probe_type,
            schedule=schedule,
            timeout=timeout,
            dtmf_digits=dtmf,
            duty_group=duty_group,
            max_call_seconds=max_call,
            token=probe_token,
            id=stable_id,
            enabled=parse_enabled(
                payload.get("enabled"), existing.enabled if existing else True
            ),
        )
        probes = [p for p in config.probes if p.name != name]
        if len(probes) + 1 > MAX_PROBES:
            raise ProbeConfigError(f"At most {MAX_PROBES} probes are supported")
        probes.append(new_probe)
        object.__setattr__(config, "probes", tuple(probes))
        return f"Probe '{name}' saved"

    ok, detail = _mutate(load_probe_config, apply)
    status = 200 if ok else 400
    return jsonify({"status": status, "message": detail}), status


@prober_blueprint.route("/prober/api/probes/delete", methods=["POST"])
@login_required
async def api_delete_probe():
    """
    Deletes a probe target by name.

    Returns:
        flask.Response: JSON {status, message}.
    """
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()

    def apply(config: ProbeConfig) -> str:
        probes = [p for p in config.probes if p.name != name]
        if len(probes) == len(config.probes):
            raise ProbeConfigError(f"Unknown probe '{name}'")
        object.__setattr__(config, "probes", tuple(probes))
        return f"Probe '{name}' deleted"

    ok, detail = _mutate(load_probe_config, apply)
    status = 200 if ok else 400
    return jsonify({"status": status, "message": detail}), status


@prober_blueprint.route("/prober/api/probes/toggle", methods=["POST"])
@login_required
async def api_toggle_probe():
    """
    Starts or stops a probe's cron schedule.

    Stopping only disables the Beat-driven scan for this probe; manual runs
    (console "run now" and the trigger webhook) keep working. Omit ``enabled``
    in the payload to flip the current state.

    Returns:
        flask.Response: JSON {status, message, enabled}.
    """
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()
    if not name:
        return jsonify({"status": 400, "message": "Probe name is required"}), 400
    state: dict[str, bool | None] = {"enabled": None}

    def apply(config: ProbeConfig) -> str:
        target = next((p for p in config.probes if p.name == name), None)
        if target is None:
            raise ProbeConfigError(f"Unknown probe '{name}'")
        enabled = parse_enabled(payload.get("enabled"), not target.enabled)
        probes = tuple(
            replace(p, enabled=enabled) if p.name == name else p for p in config.probes
        )
        object.__setattr__(config, "probes", probes)
        state["enabled"] = enabled
        return f"Probe '{name}' {'started' if enabled else 'stopped'}"

    ok, detail = _mutate(load_probe_config, apply)
    status = 200 if ok else 400
    return (
        jsonify({"status": status, "message": detail, "enabled": state["enabled"]}),
        status,
    )


@prober_blueprint.route("/prober/api/probes/run", methods=["POST"])
@login_required
async def api_run_probe():
    """
    Dispatches an immediate probe run to the Celery probe_queue.

    Returns:
        flask.Response: JSON {status, message, task_id}.
    """
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()
    try:
        config = load_probe_config()
    except ProbeConfigError as err:
        return jsonify({"status": 400, "message": str(err)}), 400
    probe = next((p for p in config.probes if p.name == name), None)
    if probe is None:
        return jsonify({"status": 404, "message": f"Unknown probe '{name}'"}), 404
    try:
        result = celery_app.send_task(
            "caller_prober.tasks.probe_tasks.run_probe",
            kwargs={
                "phone": probe.phone,
                "probe_name": probe.name,
                "probe_type": probe.probe_type,
                "timeout": probe.timeout,
                "max_call_seconds": probe.max_call_seconds,
                "dtmf_preset": probe.dtmf_digits,
                "duty_group": probe.duty_group,
                "source": "manual",
            },
            queue="probe_queue",
        )
    except Exception as err:  # noqa: BLE001 - broker outage must not 500 the UI
        logger.error("Dispatching probe run for '%s' failed: %s", name, err)
        return jsonify({"status": 502, "message": f"Broker unreachable: {err}"}), 502
    logger.info("Probe '%s' dispatched to probe_queue as %s", name, result.id)
    return jsonify({"status": 200, "message": f"Probe '{name}' dispatched", "task_id": result.id})


@prober_blueprint.route("/prober/api/probes/records", methods=["GET"])
@login_required
async def api_probe_records():
    """
    Returns the execution history for probe runs.

    Returns:
        flask.Response: JSON array of execution records, newest first.
    """
    from caller_prober.records import read_probe_history

    search_query = request.args.get("search", "").strip()
    page, per_page = _records_page_params()
    history = _filter_history(read_probe_history(limit=0), search_query)
    total = len(history)
    pages = max(1, ceil(total / per_page))
    page = min(page, pages)
    start = (page - 1) * per_page
    rows = history[start : start + per_page]
    for row in rows:
        row["local_time"] = _local_time(row.get("finished_at") or row.get("started_at"))
    return jsonify(
        {
            "status": 200,
            "records": rows,
            "count": len(rows),
            "total": total,
            "page": page,
            "pages": pages,
            "per_page": per_page,
            "timezone": str(settings.scheduled_calls.local_timezone),
        }
    )



@prober_blueprint.route("/prober/records/recording/<recording_name>")
@login_required
async def api_probe_recording(recording_name):
    """
    Streams a probe call recording fetched from Asterisk ARI.

    Only recordings referenced by an execution record are served, so the
    endpoint cannot be used to pull arbitrary Asterisk recordings.

    Args:
        recording_name: Recording identifier stored in the probe record.

    Returns:
        flask.Response: audio/wav bytes, or a JSON error status.
    """
    import base64

    import aiohttp
    from flask import Response

    from caller_prober.constants import ASTERISK_PASS, ASTERISK_URL, ASTERISK_USER
    from caller_prober.records import read_probe_history

    known = {
        str(row.get("recording"))
        for row in read_probe_history(limit=0)
        if row.get("recording")
    }
    if recording_name not in known:
        return jsonify({"status": 404, "message": "Unknown recording"}), 404

    token = base64.b64encode(f"{ASTERISK_USER}:{ASTERISK_PASS}".encode()).decode()
    url = f"{ASTERISK_URL}/ari/recordings/stored/{recording_name}/file"
    try:
        async with aiohttp.ClientSession() as session, session.get(
            url,
            headers={"Authorization": f"Basic {token}"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status >= 300:
                return jsonify(
                    {"status": 404, "message": f"Recording unavailable ({resp.status})"}
                ), 404
            payload = await resp.read()
    except Exception as err:  # noqa: BLE001 - ARI outage must not 500 the page
        logger.error("Recording %s fetch failed: %s", recording_name, err)
        return jsonify({"status": 502, "message": f"Recording fetch failed: {err}"}), 502

    return Response(payload, mimetype="audio/wav", headers={"Cache-Control": "private, max-age=3600"})


@prober_blueprint.route("/prober/api/test_webhook", methods=["POST"])
@login_required
async def api_test_webhook():
    """
    Sends a test JSON payload to a duty group webhook URL.

    Returns:
        flask.Response: JSON {status, message, http_status}.
    """
    payload = request.get_json(silent=True) or {}
    webhook = str(payload.get("webhook_url", "")).strip()
    group = str(payload.get("group", "ui-test")).strip()
    if not webhook.startswith(("http://", "https://")):
        return jsonify({"status": 400, "message": "A valid webhook URL is required"}), 400

    from datetime import UTC, datetime

    body = {
        "event": "probe_alarm_test",
        "duty_group": group,
        "triggered_at": datetime.now(UTC).isoformat(),
        "message": f"Webhook connectivity test for duty group '{group}' from py-phone-caller UI",
        "msgtype": "text",
        "msg_type": "text",
        "text": {"content": f"【py-phone-caller】值班组 [{group}] Webhook 连通性测试成功"},
        "content": {"text": f"【py-phone-caller】值班组 [{group}] Webhook 连通性测试成功"},
        "alerts": [
            {
                "status": "firing",
                "labels": {"alertname": "WebhookTest", "duty_group": group},
                "annotations": {
                    "description": f"【py-phone-caller】值班组 [{group}] Webhook 连通性测试成功",
                    "summary": f"Webhook test for {group}",
                },
            }
        ],
    }
    caller = AlarmCaller()
    try:
        ok, status_code, detail = await caller.test_webhook(webhook, body)
    except Exception as err:  # noqa: BLE001
        logger.error("Webhook test for group '%s' failed: %s", group, err)
        return jsonify({"status": 502, "message": f"连接目标失败: {err}"}), 502
    if ok:
        return jsonify({"status": 200, "message": f"Webhook 测试成功 (HTTP {status_code})"})
    return jsonify({"status": 502, "message": f"Webhook 响应异常: {detail}"}), 502
