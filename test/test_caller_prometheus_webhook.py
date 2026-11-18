import asyncio
import sys
import time
from unittest.mock import AsyncMock, patch

import pytest

from src.caller_prometheus_webhook import (
    AlertNotificationService,
    DeduplicationService,
    NotificationMode,
    PrometheusAlert,
    WebhookPayload,
    init_app,
)
from src.caller_prometheus_webhook.caller_prometheus_webhook import (
    send_message_to_caller_sms,
    start_the_asterisk_call,
    the_alert_description,
)


@pytest.fixture(autouse=True)
def _no_live_probe_dispatch(monkeypatch):
    """Never enqueue real probe runs on the live broker during tests."""
    import sys

    sent = []

    class _FakeCelery:
        @staticmethod
        def send_task(name, kwargs=None, queue=None):
            sent.append((name, kwargs, queue))
            return type("R", (), {"id": "test-task"})

    stub = type("M", (), {"app": _FakeCelery()})()
    monkeypatch.setitem(sys.modules, "py_phone_caller_utils.tasks.celery_task", stub)
    return sent


@pytest.fixture(autouse=True)
def reset_dedup(monkeypatch):
    from src.caller_prometheus_webhook import caller_prometheus_webhook
    caller_prometheus_webhook._LOCAL_DEDUP_CACHE.clear()
    # Mock redis check in unit tests to use fresh memory
    async def mock_is_duplicate_alert(message: str, ttl_seconds: int = 180) -> bool:
        msg_hash = caller_prometheus_webhook.hashlib.sha256(message.strip().encode("utf-8")).hexdigest()[:16]
        now = time.time()
        last_seen = caller_prometheus_webhook._LOCAL_DEDUP_CACHE.get(msg_hash)
        if last_seen and (now - last_seen) < ttl_seconds:
            return True
        caller_prometheus_webhook._LOCAL_DEDUP_CACHE[msg_hash] = now
        return False
    monkeypatch.setattr(caller_prometheus_webhook, "is_duplicate_alert", mock_is_duplicate_alert)

from src.caller_prometheus_webhook.constants import (
    PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_AND_SMS,
    PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY,
    PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_BEFORE_CALL,
    PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_ONLY,
)


@pytest.fixture
async def cli(aiohttp_client):
    app = await init_app()
    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_call_only_endpoint(cli):
    payload = {
        "alerts": [{"status": "firing", "annotations": {"description": "Test Alert Description"}}]
    }
    with patch(
        "src.caller_prometheus_webhook.caller_prometheus_webhook.producer"
    ) as mock_producer:
        resp = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}", json=payload
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "200"
        mock_producer.assert_called_once()


@pytest.mark.asyncio
async def test_sms_only_endpoint(cli):
    payload = {
        "alerts": [{"status": "firing", "annotations": {"description": "SMS Critical Alert"}}]
    }
    with patch(
        "src.caller_prometheus_webhook.caller_prometheus_webhook.producer"
    ) as mock_producer:
        resp = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_ONLY}", json=payload
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "200"
        mock_producer.assert_called_once()


@pytest.mark.asyncio
async def test_sms_before_call_endpoint(cli):
    payload = {
        "alerts": [{"status": "firing", "annotations": {"description": "SMS Before Call Alert"}}]
    }
    with patch(
        "src.caller_prometheus_webhook.caller_prometheus_webhook.producer"
    ) as mock_producer:
        resp = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_SMS_BEFORE_CALL}", json=payload
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "200"
        mock_producer.assert_called_once()


@pytest.mark.asyncio
async def test_call_and_sms_endpoint(cli):
    payload = {
        "alerts": [{"status": "firing", "annotations": {"description": "Combined Alert"}}]
    }
    with patch(
        "src.caller_prometheus_webhook.caller_prometheus_webhook.producer"
    ) as mock_producer:
        resp = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_AND_SMS}", json=payload
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "200"
        mock_producer.assert_called_once()


@pytest.mark.asyncio
async def test_the_alert_description_empty():
    desc = await the_alert_description({"alerts": []})
    assert desc == []

    desc_resolved = await the_alert_description(
        {"alerts": [{"status": "resolved", "annotations": {"description": "Resolved Alert"}}]}
    )
    assert desc_resolved == []


@pytest.mark.asyncio
async def test_start_the_asterisk_call_and_sms_network():
    with patch("aiohttp.ClientSession.post") as mock_post:
        await start_the_asterisk_call("+393340000000", "Test Call")
        assert mock_post.called

    with patch("aiohttp.ClientSession.post") as mock_post:
        await send_message_to_caller_sms("+393340000000", "Test SMS")
        assert mock_post.called


@pytest.mark.asyncio
async def test_alert_deduplication(cli):
    payload = {
        "alerts": [{"status": "firing", "annotations": {"description": "Deduplication Test Alert"}}]
    }
    with patch(
        "src.caller_prometheus_webhook.caller_prometheus_webhook.producer"
    ) as mock_producer:
        # First post should trigger producer
        resp1 = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}", json=payload
        )
        assert resp1.status == 200
        assert mock_producer.call_count == 1

        # Second post within sliding window should be suppressed by deduplicator
        resp2 = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}", json=payload
        )
        assert resp2.status == 200
        # Call count remains 1 because duplicate alert was discarded
        assert mock_producer.call_count == 1


def test_schemas_dataclasses():
    alert = PrometheusAlert.from_dict({
        "status": "firing",
        "annotations": {"description": "High memory"},
        "labels": {"severity": "critical"},
    })
    assert alert.is_firing is True
    assert alert.description == "High memory"

    payload = WebhookPayload.from_dict({
        "receiver": "webhook_receiver",
        "alerts": [
            {"status": "resolved", "annotations": {"description": "Old issue"}},
            {"status": "firing", "annotations": {"description": "Current issue"}},
        ],
    })
    assert payload.firing_descriptions == ["Current issue"]
    assert payload.receiver == "webhook_receiver"


@pytest.mark.asyncio
async def test_deduplication_service_in_memory():
    dedup = DeduplicationService(ttl_seconds=1, use_redis=False)
    is_dup1 = await dedup.is_duplicate("Message 1")
    assert is_dup1 is False
    is_dup2 = await dedup.is_duplicate("Message 1")
    assert is_dup2 is True

    # After TTL
    await asyncio.sleep(1.1)
    is_dup3 = await dedup.is_duplicate("Message 1")
    assert is_dup3 is False


@pytest.mark.asyncio
async def test_alert_notification_service_strategy():
    mock_call = AsyncMock()
    mock_call.place_call.return_value = True
    mock_sms = AsyncMock()
    mock_sms.send_sms.return_value = True

    svc = AlertNotificationService(call_client=mock_call, sms_client=mock_sms)
    await svc.execute_action("123", "msg", NotificationMode.CALL_ONLY.value)
    mock_call.place_call.assert_called_once_with("123", "msg", lang=None)

    await svc.execute_action("456", "msg2", NotificationMode.SMS_ONLY.value)
    mock_sms.send_sms.assert_called_once_with("456", "msg2")

    await svc.execute_action("789", "msg3", NotificationMode.CALL_AND_SMS.value)
    assert mock_sms.send_sms.call_count == 2
    assert mock_call.place_call.call_count == 2

    await svc.execute_action("999", "msg4", NotificationMode.CALL_ONLY.value, lang="z")
    mock_call.place_call.assert_called_with("999", "msg4", lang="z")

@pytest.mark.asyncio
async def test_webhook_with_valid_bearer_token(cli):
    import base64
    import json
    payload = {
        "alerts": [{"status": "firing", "annotations": {"description": "M2M Authenticated Alert"}}]
    }
    jwt_claims = {
        "sub": "service-account-m2m",
        "azp": "py-phone-caller-m2m",
        "iss": "https://py-phone-caller.link/realms/py-phone-caller",
        "exp": 9999999999,
        "realm_access": {"roles": ["alert-operator"]},
    }
    b64_payload = base64.urlsafe_b64encode(json.dumps(jwt_claims).encode()).decode().rstrip("=")
    token = f"header.{b64_payload}.signature"
    headers = {"Authorization": f"Bearer {token}"}

    with patch(
        "src.caller_prometheus_webhook.caller_prometheus_webhook.producer"
    ) as mock_producer:
        resp = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}", json=payload, headers=headers
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "200"
        mock_producer.assert_called_once()


@pytest.mark.asyncio
async def test_webhook_required_auth_behavior(cli, monkeypatch):
    import base64
    import json

    from py_phone_caller_utils.config import settings

    # Simulate require_auth = True
    monkeypatch.setattr(settings, "caller_prometheus_webhook", {
        **settings.get("caller_prometheus_webhook", {}),
        "require_auth": True,
    })

    payload = {
        "alerts": [{"status": "firing", "annotations": {"description": "Auth Enforced Alert"}}]
    }

    # 1. Missing Authorization header -> 401 Unauthorized
    resp_no_auth = await cli.post(f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}", json=payload)
    assert resp_no_auth.status == 401

    # 2. Expired token -> 401 Unauthorized
    expired_claims = {
        "sub": "service-account-m2m",
        "azp": "py-phone-caller-m2m",
        "exp": 1000,
        "realm_access": {"roles": ["alert-operator"]},
    }
    b64_expired = base64.urlsafe_b64encode(json.dumps(expired_claims).encode()).decode().rstrip("=")
    expired_token = f"h.{b64_expired}.s"
    resp_expired = await cli.post(
        f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}",
        json=payload,
        headers={"Authorization": f"Bearer {expired_token}"},
    )
    assert resp_expired.status == 401

    # 3. Valid token with alert-operator role -> 200 OK
    valid_claims = {
        "sub": "service-account-m2m",
        "azp": "py-phone-caller-m2m",
        "exp": 9999999999,
        "realm_access": {"roles": ["alert-operator"]},
    }
    b64_valid = base64.urlsafe_b64encode(json.dumps(valid_claims).encode()).decode().rstrip("=")
    valid_token = f"h.{b64_valid}.s"
    with patch(
        "src.caller_prometheus_webhook.caller_prometheus_webhook.producer"
    ) as mock_producer:
        resp_valid = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}",
            json=payload,
            headers={"Authorization": f"Bearer {valid_token}"},
        )
        assert resp_valid.status == 200
        mock_producer.assert_called_once()

@pytest.mark.asyncio
async def test_alert_coalescer_single_and_multi():
    from src.caller_prometheus_webhook.coalescing import AlertCoalescer

    coalescer = AlertCoalescer(max_samples_in_tts=2)

    # Empty
    empty_inc = coalescer.coalesce_descriptions([])
    assert empty_inc.alert_count == 0

    # Single
    single_inc = coalescer.coalesce_descriptions(["Host down"])
    assert single_inc.alert_count == 1
    assert single_inc.summary_message == "Host down"

    # Multiple (Alert Storm)
    multi_inc = coalescer.coalesce_descriptions(["Host down", "Disk full", "High memory", "DB connection lost"])
    assert multi_inc.alert_count == 4
    assert "Alert Storm: 4 firing alerts detected." in multi_inc.summary_message
    assert "Host down. Disk full." in multi_inc.summary_message
    assert "And 2 additional alerts." in multi_inc.summary_message
    assert "[ALERT STORM - 4 ALERTS]" in multi_inc.detailed_sms


@pytest.mark.asyncio
async def test_coalescing_buffer_flushing():
    import asyncio

    from src.caller_prometheus_webhook.coalescing import CoalescingBuffer

    flushed_incidents = []

    async def mock_callback(incident, receivers, mode):
        flushed_incidents.append((incident, receivers, mode))

    buffer = CoalescingBuffer(flush_callback=mock_callback, window_seconds=0.1, max_batch_size=3)
    assert not buffer.is_active

    await buffer.add_alerts(["Alert 1", "Alert 2"], ["+1234"], "call_only")
    assert buffer.is_active

    # Wait for timer to flush
    await asyncio.sleep(0.15)
    assert not buffer.is_active
    assert len(flushed_incidents) == 1
    inc, rcvs, _mode = flushed_incidents[0]
    assert inc.alert_count == 2
    assert "+1234" in rcvs


@pytest.mark.asyncio
async def test_handle_alert_payload_with_coalescing_enabled(monkeypatch):
    from py_phone_caller_utils.config import settings

    from src.caller_prometheus_webhook.services import AlertNotificationService

    settings.set("caller_prometheus_webhook.coalescing_enabled", True)
    try:
        mock_call = AsyncMock()
        mock_call.place_call.return_value = True
        mock_sms = AsyncMock()
        mock_sms.send_sms.return_value = True

        svc = AlertNotificationService(call_client=mock_call, sms_client=mock_sms)

        payload = {
            "alerts": [
                {"status": "firing", "annotations": {"description": "CPU Spike"}},
                {"status": "firing", "annotations": {"description": "RAM Full"}},
                {"status": "firing", "annotations": {"description": "Disk Full"}},
            ]
        }

        with patch.object(svc, "process_queue", new_callable=AsyncMock) as mock_queue:
            dispatched = await svc.handle_alert_payload(payload, "call_only", ["+1234"])
            assert dispatched is True
            mock_queue.assert_called_once()
            queued_msg = mock_queue.call_args[0][0]
            assert "Alert Storm: 3 firing alerts detected" in queued_msg
    finally:
        settings.set("caller_prometheus_webhook.coalescing_enabled", False)

@pytest.mark.asyncio
async def test_webhook_static_token_auth_bearer(cli):
    from py_phone_caller_utils.config import settings

    settings.set("caller_prometheus_webhook.webhook_auth_token", "secret-test-token-12345")
    settings.set("caller_prometheus_webhook.require_auth", True)
    try:
        payload = {"alerts": [{"status": "firing", "annotations": {"description": "Token Alert"}}]}

        # 1. Without token -> 401
        resp_no_token = await cli.post(f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}", json=payload)
        assert resp_no_token.status == 401

        # 2. Wrong token -> 401
        resp_wrong = await cli.post(
            f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}",
            json=payload,
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp_wrong.status == 401

        # 3. Correct Bearer token -> 200
        with patch("src.caller_prometheus_webhook.caller_prometheus_webhook.producer"):
            resp_ok = await cli.post(
                f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}",
                json=payload,
                headers={"Authorization": "Bearer secret-test-token-12345"},
            )
            assert resp_ok.status == 200
    finally:
        settings.set("caller_prometheus_webhook.webhook_auth_token", "")
        settings.set("caller_prometheus_webhook.require_auth", False)


@pytest.mark.asyncio
async def test_webhook_static_token_auth_query_and_headers(cli):
    import base64

    from py_phone_caller_utils.config import settings

    settings.set("caller_prometheus_webhook.webhook_auth_token", "secret-test-token-12345")
    settings.set("caller_prometheus_webhook.require_auth", True)
    try:
        payload = {"alerts": [{"status": "firing", "annotations": {"description": "Query Token Alert"}}]}

        # Query parameter ?token=...
        with patch("src.caller_prometheus_webhook.caller_prometheus_webhook.producer"):
            resp_query = await cli.post(
                f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}?token=secret-test-token-12345",
                json=payload,
            )
            assert resp_query.status == 200

        # X-Webhook-Token header
        with patch("src.caller_prometheus_webhook.caller_prometheus_webhook.producer"):
            resp_header = await cli.post(
                f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}",
                json=payload,
                headers={"X-Webhook-Token": "secret-test-token-12345"},
            )
            assert resp_header.status == 200

        # Basic Auth header
        basic_val = base64.b64encode(b"admin:secret-test-token-12345").decode("utf-8")
        with patch("src.caller_prometheus_webhook.caller_prometheus_webhook.producer"):
            resp_basic = await cli.post(
                f"/{PROMETHEUS_WEBHOOK_APP_ROUTE_CALL_ONLY}",
                json=payload,
                headers={"Authorization": f"Basic {basic_val}"},
            )
            assert resp_basic.status == 200
    finally:
        settings.set("caller_prometheus_webhook.webhook_auth_token", "")
        settings.set("caller_prometheus_webhook.require_auth", False)


@pytest.mark.asyncio
async def test_webhook_alert_route_alias(cli):
    payload = {"alerts": [{"status": "firing", "annotations": {"description": "Alert Route Alert"}}]}
    with patch("src.caller_prometheus_webhook.caller_prometheus_webhook.producer") as mock_producer:
        resp = await cli.post("/alert", json=payload)
        assert resp.status == 200
        mock_producer.assert_called_once()


@pytest.mark.asyncio
async def test_webhook_call_and_sms_aliases(cli):
    with patch("src.caller_prometheus_webhook.caller_prometheus_webhook.producer") as mock_producer:
        resp_call = await cli.post("/call", json={"alerts": [{"status": "firing", "annotations": {"description": "Call Alias 1"}}]})
        assert resp_call.status == 200
        resp_sms = await cli.post("/sms", json={"alerts": [{"status": "firing", "annotations": {"description": "Sms Alias 2"}}]})
        assert resp_sms.status == 200
        resp_webhook = await cli.post("/webhook", json={"alerts": [{"status": "firing", "annotations": {"description": "Webhook Alias 3"}}]})
        assert resp_webhook.status == 200
        assert mock_producer.call_count == 3



class TestZabbixPayloads:
    """The duty-group webhook must accept alert text from Zabbix too."""

    def test_zabbix_message_field(self):
        from src.caller_prometheus_webhook.schemas import extract_alert_messages

        payload = {
            "subject": "PROBLEM: CPU load",
            "message": "Host srv-01 CPU load above 90% for 5m",
            "event_id": "42",
            "severity": "High",
        }
        assert extract_alert_messages(payload) == [
            "Host srv-01 CPU load above 90% for 5m"
        ]

    def test_zabbix_subject_body_description_variants(self):
        from src.caller_prometheus_webhook.schemas import extract_alert_messages

        assert extract_alert_messages({"body": "disk full"}) == ["disk full"]
        assert extract_alert_messages({"subject": "alert subject"}) == ["alert subject"]
        assert extract_alert_messages({"description": "desc"}) == ["desc"]
        assert extract_alert_messages({"Message": "capitalised"}) == ["capitalised"]

    def test_alertmanager_payload_still_wins(self):
        from src.caller_prometheus_webhook.schemas import extract_alert_messages

        payload = {
            "message": "ignored",
            "alerts": [
                {
                    "status": "firing",
                    "annotations": {"description": "node down"},
                    "labels": {"alertname": "NodeDown"},
                }
            ],
        }
        assert extract_alert_messages(payload) == ["node down"]

    def test_resolved_alerts_are_not_dispatched(self):
        from src.caller_prometheus_webhook.schemas import extract_alert_messages

        payload = {"alerts": [{"status": "resolved", "annotations": {"description": "back"}}]}
        assert extract_alert_messages(payload) == []

    def test_empty_payload_yields_nothing(self):
        from src.caller_prometheus_webhook.schemas import extract_alert_messages

        assert extract_alert_messages({}) == []
        assert extract_alert_messages({"unrelated": 1}) == []

    async def test_body_parser_accepts_text_json_and_forms(self):
        from urllib.parse import parse_qsl

        from src.caller_prometheus_webhook.routes import parse_webhook_payload

        class FakeRequest:
            def __init__(self, body, content_type):
                self._body = body
                self.content_type = content_type

            async def text(self):
                return self._body

            async def post(self):
                return dict(parse_qsl(self._body))

        json_body = await parse_webhook_payload(
            FakeRequest('{"message": "json alert", "subject": "PROBLEM"}', "application/json")
        )
        assert json_body["message"] == "json alert"

        plain = await parse_webhook_payload(
            FakeRequest("Zabbix: disk full on srv-01", "text/plain")
        )
        assert plain == {"message": "Zabbix: disk full on srv-01"}

        form = await parse_webhook_payload(
            FakeRequest(
                "message=form+alert&subject=PROBLEM",
                "application/x-www-form-urlencoded",
            )
        )
        assert form["message"] == "form alert"
        assert form["subject"] == "PROBLEM"

        assert await parse_webhook_payload(FakeRequest("   ", "text/plain")) == {}

    async def test_zabbix_text_reaches_the_dispatch_queue(self, cli, monkeypatch):
        """A plain-text alert body must reach the dispatcher as the message."""
        import src.caller_prometheus_webhook.caller_prometheus_webhook as handler
        from src.caller_prometheus_webhook.routes import parse_webhook_payload

        captured = []

        async def fake_data_from_alert_manager(request, caller_func, custom_receivers=None, lang=None):
            payload = await parse_webhook_payload(request)
            captured.append(payload)

        monkeypatch.setattr(
            handler, "data_from_alert_manager", fake_data_from_alert_manager, raising=False
        )
        monkeypatch.setattr(
            "src.caller_prometheus_webhook.routes.settings",
            type("S", (), {"get": staticmethod(lambda *a, **k: {})})(),
        )

        response = await cli.post("/call", data="Zabbix: disk full on srv-01")

        assert response.status == 200
        assert captured == [{"message": "Zabbix: disk full on srv-01"}]


def _probe_cfg(probes):
    from caller_prober.config import DutyGroup, DutyMember, ProbeConfig

    return ProbeConfig(
        duty_groups={
            "ops": DutyGroup(
                name="ops",
                members=(DutyMember("A", "100"),),
                escalation_timeout=30,
                webhook_url="https://hooks.example.com/ops",
                token="grp-token",
            )
        },
        probes=tuple(probes),
    )


def _probe(name, token, phone="200", probe_id=""):
    from caller_prober.config import ProbeTarget

    return ProbeTarget(
        phone=phone,
        name=name,
        probe_type="direct",
        schedule="0 9 * * *",
        timeout=30.0,
        dtmf_digits=None,
        duty_group="ops",
        token=token,
        id=probe_id or f"id-{name}-{token}",
    )


class TestProbeTriggerWebhook:
    """Zabbix triggers a probe run through /probe with the probe's token."""

    async def test_trigger_enqueues_probe_run(self, cli, monkeypatch):
        from caller_prober.config import (
            DutyGroup,
            DutyMember,
            ProbeConfig,
            ProbeTarget,
        )

        probe = ProbeTarget(
            phone="200",
            name="hotline",
            probe_type="callcenter",
            schedule="0 9 * * *",
            timeout=45.0,
            dtmf_digits="1",
            duty_group="ops",
            max_call_seconds=15.0,
            token="probe-token-abc",
        )
        cfg = ProbeConfig(
            duty_groups={
                "ops": DutyGroup(
                    name="ops",
                    members=(DutyMember("A", "100"),),
                    escalation_timeout=30,
                    webhook_url="https://hooks.example.com/ops",
                    token="grp",
                )
            },
            probes=(probe,),
        )
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        sent = []

        class FakeCelery:
            def send_task(self, name, kwargs=None, queue=None):
                sent.append((name, kwargs, queue))

                class R:
                    id = "task-123"

                return R()

        monkeypatch.setitem(
            __import__("sys").modules,
            "py_phone_caller_utils.tasks.celery_task",
            type("M", (), {"app": FakeCelery()})(),
        )

        response = await cli.post("/probe?probe=hotline&token=probe-token-abc")

        assert response.status == 200
        body = await response.json()
        assert body["task_id"] == "task-123"
        assert body["phone"] == "200"
        assert sent and sent[0][0] == "caller_prober.tasks.probe_tasks.run_probe"
        assert sent[0][1]["probe_name"] == "hotline"
        assert sent[0][1]["max_call_seconds"] == 15.0
        assert sent[0][2] == "probe_queue"

    async def test_trigger_rejects_bad_token(self, cli, monkeypatch):
        from caller_prober.config import DutyGroup, DutyMember, ProbeConfig, ProbeTarget

        probe = ProbeTarget(
            phone="200",
            name="hotline",
            probe_type="direct",
            schedule="0 9 * * *",
            timeout=30.0,
            dtmf_digits=None,
            duty_group="ops",
            token="probe-token-abc",
        )
        cfg = ProbeConfig(
            duty_groups={
                "ops": DutyGroup(
                    name="ops",
                    members=(DutyMember("A", "100"),),
                    escalation_timeout=30,
                    webhook_url="https://hooks.example.com/ops",
                    token="grp",
                )
            },
            probes=(probe,),
        )
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response = await cli.post("/probe?probe=hotline&token=wrong")

        assert response.status == 401

    async def test_trigger_unknown_probe(self, cli, monkeypatch):
        from caller_prober.config import DutyGroup, DutyMember, ProbeConfig

        cfg = ProbeConfig(
            duty_groups={
                "ops": DutyGroup(
                    name="ops",
                    members=(DutyMember("A", "100"),),
                    escalation_timeout=30,
                    webhook_url="https://hooks.example.com/ops",
                    token="grp",
                )
            },
            probes=(),
        )
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response = await cli.post("/probe?probe=missing&token=x")

        assert response.status == 404


class TestProbeTriggerRename:
    """Renaming a probe must not break an existing trigger URL."""

    async def test_stale_name_resolves_by_token(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("New Name", "tok-new")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)
        monkeypatch.setitem(
            __import__("sys").modules,
            "py_phone_caller_utils.tasks.celery_task",
            type(
                "M",
                (),
                {
                    "app": type(
                        "C",
                        (),
                        {"send_task": staticmethod(lambda *a, **k: type("R", (), {"id": "t-1"})())},
                    )()
                },
            )(),
        )

        response = await cli.post("/probe?probe=Old%20Name&token=tok-new")

        assert response.status == 200
        body = await response.json()
        assert body["probe"] == "New Name"

    async def test_token_alone_is_enough(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("hotline", "tok-h")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)
        monkeypatch.setitem(
            __import__("sys").modules,
            "py_phone_caller_utils.tasks.celery_task",
            type(
                "M",
                (),
                {
                    "app": type(
                        "C",
                        (),
                        {"send_task": staticmethod(lambda *a, **k: type("R", (), {"id": "t-2"})())},
                    )()
                },
            )(),
        )

        response = await cli.post("/probe?token=tok-h")

        assert response.status == 200
        assert (await response.json())["probe"] == "hotline"

    async def test_named_probe_cannot_use_another_probes_token(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("A", "tok-a"), _probe("B", "tok-b", phone="300")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response = await cli.post("/probe?probe=A&token=tok-b")

        assert response.status == 401

    async def test_named_probe_without_token_is_rejected(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("A", "tok-a")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response = await cli.post("/probe?probe=A")

        assert response.status == 401

    async def test_shared_token_requires_the_name(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("A", "dup"), _probe("B", "dup", phone="300")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        ambiguous = await cli.post("/probe?token=dup")
        explicit = await cli.post("/probe?probe=A&token=dup")

        assert ambiguous.status == 409
        assert explicit.status == 200


class TestProbeTriggerStableId:
    """probe_id addresses the probe even after a rename and a token rotation."""

    async def test_id_resolves_after_rename_and_new_token(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("Renamed Task", "rotated-token", probe_id="stable-id-1")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response = await cli.post("/probe?probe_id=stable-id-1&token=rotated-token")

        assert response.status == 200
        body = await response.json()
        assert body["probe"] == "Renamed Task"
        assert body["probe_id"] == "stable-id-1"

    async def test_id_with_old_token_is_rejected(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("Renamed Task", "rotated-token", probe_id="stable-id-1")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response = await cli.post("/probe?probe_id=stable-id-1&token=old-token")

        assert response.status == 401

    async def test_unknown_id(self, cli, monkeypatch):
        cfg = _probe_cfg([_probe("A", "tok-a", probe_id="stable-id-1")])
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response = await cli.post("/probe?probe_id=nope&token=tok-a")

        assert response.status == 404


class TestDutyGroupWebhookId:
    """The receiver resolves ?group_id= so renames cannot break the URL."""

    async def _post(self, cli, monkeypatch, url):
        import src.caller_prometheus_webhook.caller_prometheus_webhook as handler

        captured = []

        async def fake_data_from_alert_manager(request, caller_func, custom_receivers=None, lang=None):
            captured.append((caller_func, list(custom_receivers or [])))

        monkeypatch.setattr(
            handler, "data_from_alert_manager", fake_data_from_alert_manager, raising=False
        )
        response = await cli.post(url, json={"message": "alert"})
        return response, captured

    async def test_group_id_resolves_after_rename(self, cli, monkeypatch):
        from caller_prober.config import DutyGroup, DutyMember, ProbeConfig

        cfg = ProbeConfig(
            duty_groups={
                "renamed-group": DutyGroup(
                    name="renamed-group",
                    members=(DutyMember("A", "111"),),
                    escalation_timeout=30,
                    webhook_url="https://hooks.example.com/x",
                    notification_type="call",
                    token="grp-token",
                    id="stable-group-id",
                )
            },
            probes=(),
        )
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response, captured = await self._post(
            cli, monkeypatch, "/call?group_id=stable-group-id&token=grp-token"
        )

        assert response.status == 200
        assert captured and captured[0][1] == ["111"]

    async def test_group_id_with_wrong_token_is_rejected(self, cli, monkeypatch):
        from caller_prober.config import DutyGroup, DutyMember, ProbeConfig

        cfg = ProbeConfig(
            duty_groups={
                "g": DutyGroup(
                    name="g",
                    members=(DutyMember("A", "111"),),
                    escalation_timeout=30,
                    webhook_url="https://hooks.example.com/x",
                    notification_type="call",
                    token="grp-token",
                    id="stable-group-id",
                )
            },
            probes=(),
        )
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response, captured = await self._post(
            cli, monkeypatch, "/call?group_id=stable-group-id&token=wrong"
        )

        assert response.status == 401
        assert captured == []

    async def test_legacy_group_name_still_works(self, cli, monkeypatch):
        from caller_prober.config import DutyGroup, DutyMember, ProbeConfig

        cfg = ProbeConfig(
            duty_groups={
                "ops": DutyGroup(
                    name="ops",
                    members=(DutyMember("A", "222"),),
                    escalation_timeout=30,
                    webhook_url="https://hooks.example.com/x",
                    notification_type="call",
                    token="grp-token",
                    id="stable-group-id",
                )
            },
            probes=(),
        )
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

        response, captured = await self._post(cli, monkeypatch, "/call?group=ops&token=grp-token")

        assert response.status == 200
        assert captured and captured[0][1] == ["222"]


class TestEndpointAliases:
    """Both the canonical and short webhook paths must stay registered."""

    async def test_all_alias_paths_are_live(self, cli, monkeypatch):
        import src.caller_prometheus_webhook.caller_prometheus_webhook as handler

        dispatched = []

        async def fake_data_from_alert_manager(request, caller_func, custom_receivers=None, lang=None):
            dispatched.append(caller_func)

        monkeypatch.setattr(
            handler, "data_from_alert_manager", fake_data_from_alert_manager, raising=False
        )

        for path in (
            "/call_only",
            "/call",
            "/sms_only",
            "/sms",
            "/sms_before_call",
            "/call_and_sms",
            "/alert",
            "/webhook",
        ):
            response = await cli.post(path, json={})
            assert response.status == 200, path

        # every alias reached the dispatcher once, with its own mode
        assert len(dispatched) == 8

    async def test_aliases_map_to_the_same_mode(self, cli, monkeypatch):
        import src.caller_prometheus_webhook.caller_prometheus_webhook as handler

        modes = []

        async def fake_data_from_alert_manager(request, caller_func, custom_receivers=None, lang=None):
            modes.append((request.path, caller_func))

        monkeypatch.setattr(
            handler, "data_from_alert_manager", fake_data_from_alert_manager, raising=False
        )

        await cli.post("/call_only", json={"message": "x", "test": 1})
        await cli.post("/call", json={"message": "x", "test": 1})
        await cli.post("/sms_only", json={"message": "x", "test": 1})
        await cli.post("/sms", json={"message": "x", "test": 1})

        by_path = dict(modes)
        assert by_path["/call_only"] == by_path["/call"] == "call_only"
        assert by_path["/sms_only"] == by_path["/sms"] == "sms_only"


class TestTtsLanguageResolution:
    """TTS voice language: ?lang= > duty group lang > webhook_tts_lang setting."""

    def _group(self, lang=""):
        from caller_prober.config import DutyGroup, DutyMember

        return DutyGroup(
            name="ops",
            members=(DutyMember("A", "111"),),
            escalation_timeout=30,
            webhook_url="https://hooks.example.com/x",
            notification_type="call",
            token="grp-token",
            id="stable-group-id",
            lang=lang,
        )

    async def _dispatch_lang(self, cli, monkeypatch, url, *, group_lang="", setting_lang=""):
        import src.caller_prometheus_webhook.caller_prometheus_webhook as handler
        from caller_prober.config import ProbeConfig
        from py_phone_caller_utils.config import settings

        cfg = ProbeConfig(duty_groups={"ops": self._group(group_lang)}, probes=())
        monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)
        # Dynaconf 的 __setattr__ 会写进 _store（monkeypatch 撤销时还会删掉键），
        # 所以换掉 routes 模块引用的 settings 对象，而不是给它设属性。
        webhook_cfg = {
            **settings.get("caller_prometheus_webhook", {}),
            "webhook_tts_lang": setting_lang,
        }
        base_settings = settings

        class _FakeSettings:
            def get(self, key, default=None, *args, **kwargs):
                if key == "caller_prometheus_webhook":
                    return webhook_cfg
                return base_settings.get(key, default, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(base_settings, name)

        for module_name in (
            "src.caller_prometheus_webhook.routes",
            "caller_prometheus_webhook.routes",
        ):
            routes_module = sys.modules.get(module_name)
            if routes_module is not None:
                monkeypatch.setattr(routes_module, "settings", _FakeSettings())

        captured = []

        async def fake_data_from_alert_manager(
            request, caller_func, custom_receivers=None, lang=None
        ):
            captured.append(lang)

        monkeypatch.setattr(
            handler, "data_from_alert_manager", fake_data_from_alert_manager, raising=False
        )

        response = await cli.post(url, json={"message": "alert"})
        assert response.status == 200
        assert len(captured) == 1
        return captured[0]

    async def test_group_lang_is_used(self, cli, monkeypatch):
        lang = await self._dispatch_lang(
            cli,
            monkeypatch,
            "/call?group_id=stable-group-id&token=grp-token",
            group_lang="z",
        )
        assert lang == "z"

    async def test_query_lang_overrides_group_lang(self, cli, monkeypatch):
        lang = await self._dispatch_lang(
            cli,
            monkeypatch,
            "/call?group_id=stable-group-id&token=grp-token&lang=en",
            group_lang="z",
        )
        assert lang == "en"

    async def test_setting_lang_is_the_last_resort(self, cli, monkeypatch):
        lang = await self._dispatch_lang(
            cli,
            monkeypatch,
            "/call?group_id=stable-group-id&token=grp-token",
            group_lang="",
            setting_lang="z",
        )
        assert lang == "z"

    async def test_no_lang_keeps_the_audio_service_default(self, cli, monkeypatch):
        lang = await self._dispatch_lang(
            cli,
            monkeypatch,
            "/call?group_id=stable-group-id&token=grp-token",
            group_lang="",
            setting_lang="",
        )
        assert lang is None


@pytest.mark.asyncio
async def test_place_call_forwards_lang_to_the_caller_service(monkeypatch):
    from unittest.mock import MagicMock

    import src.caller_prometheus_webhook.clients as clients_mod

    class _Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

    session = MagicMock()
    session.post.return_value = _Response()
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(clients_mod, "ClientSession", MagicMock(return_value=session_cm))

    client = clients_mod.AsteriskCallClient(base_url="http://asterisk:8081")

    assert await client.place_call("123", "hello", lang="z") is True
    assert session.post.call_args.kwargs["params"] == {
        "phone": "123",
        "message": "hello",
        "lang": "z",
    }

    assert await client.place_call("123", "hello") is True
    assert session.post.call_args.kwargs["params"] == {"phone": "123", "message": "hello"}


@pytest.mark.asyncio
async def test_group_lang_reaches_asterisk_place_call(cli, monkeypatch):
    """The legacy dispatch path must forward the group's TTS lang to Asterisk."""
    import src.caller_prometheus_webhook.caller_prometheus_webhook as handler
    from caller_prober.config import DutyGroup, DutyMember, ProbeConfig

    cfg = ProbeConfig(
        duty_groups={
            "ops": DutyGroup(
                name="ops",
                members=(DutyMember("A", "111"),),
                escalation_timeout=30,
                webhook_url="https://hooks.example.com/x",
                notification_type="call",
                token="grp-token",
                id="stable-group-id",
                lang="z",
            )
        },
        probes=(),
    )
    monkeypatch.setattr("caller_prober.config.load_probe_config", lambda *a, **k: cfg)

    recorded = []

    async def fake_place_call(self, phone, message, lang=None):
        recorded.append((phone, message, lang))
        return True

    # The handler imports the client class itself, so patch the binding it holds.
    monkeypatch.setattr(handler.AsteriskCallClient, "place_call", fake_place_call)

    response = await cli.post(
        "/call?group_id=stable-group-id&token=grp-token",
        json={"message": "unique group lang alert"},
    )

    assert response.status == 200
    assert recorded == [("111", "unique group lang alert", "z")]
