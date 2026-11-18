"""
HTTP request handlers and routing for caller_prometheus_webhook.

Defines REST webhook endpoints compatible with Prometheus Alertmanager:
call-only, SMS-only, SMS-before-call, and call-and-SMS.
"""

import base64
import hmac
import json
import logging
import sys
from collections.abc import Mapping
from contextlib import suppress

from aiohttp import web
from py_phone_caller_utils.auth import get_oidc_client
from py_phone_caller_utils.config import settings
from py_phone_caller_utils.web import create_service_catalog, setup_swagger_routes

from caller_prometheus_webhook.constants import (
    PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_AND_SMS,
    PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY,
    PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_BEFORE_CALL,
    PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_ONLY,
    PROMETHEUS_WEBHOOK_RECEIVERS,
)
from caller_prometheus_webhook.schemas import NotificationMode
from caller_prometheus_webhook.services import AlertNotificationService

logger = logging.getLogger(__name__)


async def parse_webhook_payload(request: web.Request) -> dict:
    """
    Parses an inbound alert body regardless of the sender's encoding.

    Zabbix and other monitoring tools post JSON, form data or plain text; all
    three are accepted so the alert text always reaches the roster.

    Args:
        request: Incoming aiohttp request.

    Returns:
        dict: Payload mapping; plain text bodies become ``{"message": text}``.
    """
    raw = await request.text()
    text = (raw or "").strip()
    if not text:
        return {}
    content_type = (request.content_type or "").lower()
    if "json" in content_type or content_type in ("", "text/plain"):
        with suppress(ValueError):
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
    if "form" in content_type:
        with suppress(Exception):
            form = await request.post()
            return {key: str(value) for key, value in form.items()}
    return {"message": text}


def _resolve_service(request: web.Request) -> AlertNotificationService:
    """Resolves AlertNotificationService instance from app state or module factory."""
    factory = request.app.get("alert_service_factory")
    if factory:
        return factory()
    mod = sys.modules.get("src.caller_prometheus_webhook.caller_prometheus_webhook") or sys.modules.get(
        "caller_prometheus_webhook.caller_prometheus_webhook"
    )
    if mod and hasattr(mod, "_get_active_service"):\
        return mod._get_active_service()
    return request.app.get("alert_service")


async def response_for_alert_manager() -> web.Response:
    """Returns standardized JSON response for Alertmanager webhooks."""
    return web.json_response({"status": "200"})


def resolve_tts_lang(
    query: Mapping[str, str],
    target_group: object | None,
    webhook_cfg: Mapping[str, object] | None,
) -> str | None:
    """
    Picks the TTS voice language for an inbound alert webhook.

    Order: explicit ``?lang=``/``?language=`` query parameter, then the duty
    group's configured default, then the service-wide ``webhook_tts_lang``.

    Args:
        query: Request query parameters.
        target_group: Resolved duty group, if the request targeted one.
        webhook_cfg: ``caller_prometheus_webhook`` settings mapping.

    Returns:
        Language code, or None so the generate_audio service keeps its default voice.
    """
    lang = (query.get("lang") or query.get("language") or "").strip()
    if not lang and target_group is not None:
        lang = str(getattr(target_group, "lang", "") or "").strip()
    if not lang:
        lang = str((webhook_cfg or {}).get("webhook_tts_lang") or "").strip()
    return lang or None


async def handle_webhook_request(request: web.Request, action_mode: str) -> web.Response:
    """
    Common handler for processing Prometheus Alertmanager webhooks.

    Validates optional Machine-to-Machine (M2M) Bearer tokens when auth is required,
    delegates incoming payloads to the notification service (or legacy patched handler),
    and enforces deduplication windows.

    Args:
        request: Incoming aiohttp Request.
        action_mode: Mode string (call_only, sms_only, sms_before_call, call_and_sms).

    Returns:
        JSON response with HTTP 200.
    """
    webhook_cfg = settings.get("caller_prometheus_webhook", {})
    configured_token = str(
        webhook_cfg.get("webhook_auth_token")
        or webhook_cfg.get("webhook_token")
        or ""
    ).strip()
    require_auth = bool(
        webhook_cfg.get("require_auth", False)
        or getattr(getattr(settings, "auth", None), "m2m_required", False)
    )

    authenticated = False
    client_identity = None

    auth_header = request.headers.get("Authorization", "").strip()
    header_token = request.headers.get("X-Webhook-Token") or request.headers.get("X-API-Key")
    query_token = request.query.get("token") or request.query.get("api_key")

    candidate_tokens: list[str] = []
    if auth_header.startswith("Bearer "):
        candidate_tokens.append(auth_header.split(" ", 1)[1].strip())
    elif auth_header.startswith("Basic "):
        with suppress(Exception):
            raw_b64 = auth_header.split(" ", 1)[1].strip()
            decoded = base64.b64decode(raw_b64).decode("utf-8")
            if ":" in decoded:
                u, p = decoded.split(":", 1)
                candidate_tokens.extend([p, u])
            else:
                candidate_tokens.append(decoded)

    if header_token:
        candidate_tokens.append(header_token.strip())
    if query_token:
        candidate_tokens.append(query_token.strip())

    # 1. Match configured secret token with constant-time comparison
    if configured_token:
        for tok in candidate_tokens:
            if hmac.compare_digest(tok, configured_token):
                authenticated = True
                client_identity = "token-client"
                break
    # 2. Match duty group token if a specific group is targeted. group_id is
    # the stable key (survives renames); ?group=<name> stays supported.
    target_group_name = request.query.get("group", "").strip()
    target_group_id = (request.query.get("group_id") or request.query.get("id") or "").strip()
    target_group = None
    if target_group_id or target_group_name:
        with suppress(Exception):
            from caller_prober.config import load_probe_config

            cfg = load_probe_config()
            if target_group_id:
                target_group = next(
                    (g for g in cfg.duty_groups.values() if g.id == target_group_id), None
                )
            if target_group is None and target_group_name:
                target_group = cfg.duty_groups.get(target_group_name)
    if target_group is not None:
        target_group_name = target_group.name

    if not authenticated and target_group and target_group.token:
        for tok in candidate_tokens:
            if hmac.compare_digest(tok, target_group.token):
                authenticated = True
                client_identity = f"duty-group:{target_group_name}"
                break
        if not authenticated:
            require_auth = True

    # 3. Match OIDC Bearer token if not already authenticated
    if not authenticated and auth_header.startswith("Bearer "):
        bearer_val = auth_header.split(" ", 1)[1].strip()
        with suppress(Exception):
            oidc = get_oidc_client()
            claims = oidc.verify_token(bearer_val, required_role="alert-operator" if require_auth else None)
            if claims:
                authenticated = True
                client_identity = claims.get("azp") or claims.get("client_id") or claims.get("sub", "m2m-client")

    if authenticated:
        logger.info("Accepted alert webhook from authenticated client '%s'", client_identity)
    elif require_auth:
        return web.json_response(
            {
                "error": "Unauthorized",
                "message": "Invalid or missing webhook authentication credentials. Provide Authorization: Bearer <token>, X-Webhook-Token header, or ?token= query parameter.",
            },
            status=401,
        )

    mod = sys.modules.get("src.caller_prometheus_webhook.caller_prometheus_webhook") or sys.modules.get(
        "caller_prometheus_webhook.caller_prometheus_webhook"
    )
    receivers = getattr(mod, "PROMETHEUS_WEBHOOK_RECEIVERS", PROMETHEUS_WEBHOOK_RECEIVERS)
    if target_group:
        group_phones = [m.phone for m in target_group.members if m.phone]
        if group_phones:
            receivers = group_phones
        mode_map = {
            "call": NotificationMode.CALL_ONLY.value,
            "sms": NotificationMode.SMS_ONLY.value,
            "call_and_sms": NotificationMode.CALL_AND_SMS.value,
            "sms_before_call": NotificationMode.SMS_BEFORE_CALL.value,
        }
        if target_group.notification_type in mode_map and request.path in ("/", "/alert", "/webhook"):
            action_mode = mode_map[target_group.notification_type]

    # TTS voice language: explicit ?lang= wins, then the duty group's default,
    # then the service-wide setting. Empty means "keep generate_audio default".
    tts_lang = resolve_tts_lang(request.query, target_group, webhook_cfg)

    if mod and hasattr(mod, "data_from_alert_manager"):
        await mod.data_from_alert_manager(
            request, action_mode, receivers, lang=tts_lang
        )
    else:
        payload = await parse_webhook_payload(request)
        svc = _resolve_service(request)
        dedup_window = int(settings.get("DEDUPLICATION_WINDOW_SECONDS", 180))
        await svc.handle_alert_payload(
            payload_data=payload,
            action_mode=action_mode,
            receivers=receivers,
            dedup_window=dedup_window,
            lang=tts_lang,
        )

    return await response_for_alert_manager()


async def call_only(request: web.Request) -> web.Response:
    """Handles POST requests to dispatch phone calls for firing alerts."""
    return await handle_webhook_request(request, NotificationMode.CALL_ONLY.value)


async def sms_only(request: web.Request) -> web.Response:
    """Handles POST requests to dispatch SMS text for firing alerts."""
    return await handle_webhook_request(request, NotificationMode.SMS_ONLY.value)


async def sms_before_call(request: web.Request) -> web.Response:
    """Handles POST requests to dispatch SMS followed by call after delay."""
    return await handle_webhook_request(request, NotificationMode.SMS_BEFORE_CALL.value)


async def call_and_sms(request: web.Request) -> web.Response:
    """Handles POST requests to dispatch simultaneous phone call and SMS."""
    return await handle_webhook_request(request, NotificationMode.CALL_AND_SMS.value)


async def probe_trigger(request: web.Request) -> web.Response:
    """
    Triggers one probe run on demand (Zabbix / monitoring systems).

    Authenticates with the probe's own token (shown in its edit dialog) or the
    service token, then enqueues ``run_probe`` on ``probe_queue`` so the regular
    verdict + duty-alarm pipeline runs unchanged.

    Returns:
        web.Response: JSON {status, probe, task_id} or an error status.
    """
    import hmac

    webhook_cfg = settings.get("caller_prometheus_webhook", {})
    service_token = str(
        webhook_cfg.get("webhook_auth_token") or webhook_cfg.get("webhook_token") or ""
    ).strip()
    query_token = request.query.get("token") or request.query.get("api_key")
    header_token = request.headers.get("X-Webhook-Token") or request.headers.get("X-API-Key")
    auth_header = request.headers.get("Authorization", "").strip()
    bearer = auth_header.split(" ", 1)[1].strip() if auth_header.startswith("Bearer ") else ""
    candidates = [
        tok.strip()
        for tok in (query_token, header_token, bearer)
        if tok and str(tok).strip()
    ]

    probe_name = (request.query.get("probe") or request.query.get("probe_name") or "").strip()
    probe_id = (request.query.get("probe_id") or request.query.get("id") or "").strip()

    try:
        from caller_prober.config import load_probe_config

        config = load_probe_config()
    except Exception as err:  # noqa: BLE001 - config errors must not 500
        return web.json_response(
            {"status": 500, "message": f"Probe config unavailable: {err}"}, status=500
        )

    def _matches(candidate: str, expected: str) -> bool:
        return bool(candidate) and bool(expected) and hmac.compare_digest(candidate, expected)

    def _token_ok(probe) -> bool:
        return any(
            _matches(candidate, getattr(probe, "token", "")) for candidate in candidates
        ) or any(_matches(candidate, service_token) for candidate in candidates)

    def _unauthorized() -> web.Response:
        return web.json_response(
            {
                "status": 401,
                "message": "Invalid or missing probe token (pass ?token=, X-Webhook-Token or Authorization: Bearer).",
            },
            status=401,
        )

    # 1) Immutable id: survives renames and token rotation of other probes.
    if probe_id:
        probe = next((p for p in config.probes if p.id == probe_id), None)
        if probe is None:
            return web.json_response(
                {"status": 404, "message": f"Unknown probe id '{probe_id}'"}, status=404
            )
        if not _token_ok(probe):
            return _unauthorized()
    else:
        by_name = (
            next((p for p in config.probes if p.name == probe_name), None)
            if probe_name
            else None
        )
        by_token = [
            p
            for p in config.probes
            if any(_matches(candidate, getattr(p, "token", "")) for candidate in candidates)
        ]

        if by_name is not None:
            probe = by_name
            if not _token_ok(probe):
                return _unauthorized()
        elif len(by_token) == 1:
            # Renamed probe (or no name supplied): the token identifies it.
            probe = by_token[0]
            if probe_name:
                logger.warning(
                    "Probe trigger used stale name '%s'; resolved by token to '%s'",
                    probe_name,
                    probe.name,
                )
        elif by_token:
            return web.json_response(
                {"status": 409, "message": "Token matches several probes; pass ?probe_id=<id>."},
                status=409,
            )
        elif not probe_name:
            return web.json_response(
                {
                    "status": 400,
                    "message": "Missing 'probe_id' query parameter (or a valid token)",
                },
                status=400,
            )
        else:
            return web.json_response(
                {"status": 404, "message": f"Unknown probe '{probe_name}'"}, status=404
            )

    try:
        from py_phone_caller_utils.tasks.celery_task import app as celery_app

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
                "source": "webhook",
            },
            queue="probe_queue",
        )
    except Exception as err:  # noqa: BLE001 - broker outage
        logger.error("Probe trigger for '%s' failed: %s", probe_name, err)
        return web.json_response(
            {"status": 502, "message": f"Broker unreachable: {err}"}, status=502
        )

    logger.info(
        "Probe '%s' (id=%s) triggered by webhook as %s",
        probe.name,
        probe.id,
        result.id,
    )
    return web.json_response(
        {
            "status": 200,
            "probe": probe.name,
            "probe_id": probe.id,
            "phone": probe.phone,
            "task_id": result.id,
        }
    )


def setup_routes(app: web.Application) -> None:
    """
    Configures and mounts all REST routes onto the aiohttp application.

    Args:
        app: The aiohttp Application instance.
    """
    async def root_catalog(request: web.Request) -> web.Response:
        catalog = create_service_catalog(
            service_name="caller_prometheus_webhook",
            description="Prometheus Alertmanager webhook receiver for py-phone-caller",
            version="1.0.0",
            docs_url="/docs",
            openapi_spec="/docs/swagger.json",
            endpoints={
                f"POST /{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}": "Dispatches phone calls for firing Prometheus alerts",
                f"POST /{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_ONLY}": "Dispatches SMS for firing Prometheus alerts",
                f"POST /{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_BEFORE_CALL}": "Sends SMS first, then calls after configurable delay",
                f"POST /{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_AND_SMS}": "Dispatches simultaneous call and SMS",
                "GET /live": "Liveness health check",
                "GET /ready": "Readiness probe",
                "GET /metrics": "Prometheus telemetry metrics",
            },
        )
        return web.json_response(catalog)

    app.router.add_route("GET", "/", root_catalog)
    app.router.add_route(
        "POST", f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}", call_only
    )
    app.router.add_route("POST", f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_ONLY}", sms_only)
    app.router.add_route(
        "POST",
        f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_BEFORE_CALL}",
        sms_before_call,
    )
    app.router.add_route(
        "POST",
        f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_AND_SMS}",
        call_and_sms,
    )
    app.router.add_route("POST", "/call", call_only)
    app.router.add_route("POST", "/probe", probe_trigger)
    app.router.add_route("POST", "/probe_trigger", probe_trigger)
    app.router.add_route("POST", "/sms", sms_only)
    app.router.add_route("POST", "/alert", call_and_sms)
    app.router.add_route("POST", "/webhook", call_and_sms)

    setup_swagger_routes(
        app=app,
        service_name="caller_prometheus_webhook",
        description="Prometheus Alertmanager Webhook Ingestion Service",
        paths={
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}": {
                "post": {
                    "summary": "Process alert for phone call dispatch",
                    "description": "Receives standard Prometheus Alertmanager JSON payload and queues phone calls to configured receivers.",
                    "responses": {"200": {"description": "Webhook received"}},
                }
            },
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_ONLY}": {
                "post": {
                    "summary": "Process alert for SMS dispatch",
                    "responses": {"200": {"description": "Webhook received"}},
                }
            },
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_BEFORE_CALL}": {
                "post": {
                    "summary": "Process alert with SMS followed by call",
                    "responses": {"200": {"description": "Webhook received"}},
                }
            },
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_AND_SMS}": {
                "post": {
                    "summary": "Process alert with concurrent call and SMS",
                    "responses": {"200": {"description": "Webhook received"}},
                }
            },
        },
    )
