"""
Tests for the prober console blueprint of the py_phone_caller UI: page render,
duty-group/probe CRUD persisted to probe_config.yml, and the immediate
probe-run dispatch.
"""

import textwrap
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from py_phone_caller_ui.app import app

PROBE_YML = textwrap.dedent(
    """
    duty_groups:
      ops:
        members:
          - name: Alice
            phone: "100"
        escalation_timeout: 30
        webhook_url: "https://hooks.example.com/ops"

    probes:
      - phone: "95100"
        name: hotline
        type: callcenter
        schedule: "0 9 * * *"
        timeout: 60
        dtmf_digits: "1"
        duty_group: ops
    """
)


@pytest.fixture
def probe_config(tmp_path, monkeypatch):
    """Writes a valid probe config and points the loader at it."""
    path = tmp_path / "probe_config.yml"
    path.write_text(PROBE_YML, encoding="utf-8")
    monkeypatch.setenv("CALLER_PROBE_CONFIG", str(path))
    return path


@pytest.fixture
def client():
    with app.test_client() as c:
        yield c


@contextmanager
def _login(client):
    """Authenticates the session and stubs the user loader for the request."""
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
    with patch("flask_login.utils._get_user") as mock_user:
        mock_user.return_value = MagicMock(is_authenticated=True)
        yield client


def test_prober_page_unauthenticated_redirects(client):
    response = client.get("/prober")
    assert response.status_code == 200
    assert b"sign in" in response.data.lower() or b"unauthorized" in response.data.lower()


def test_prober_page_loads(client, probe_config):
    with _login(client) as c:
        response = c.get("/prober")
    assert response.status_code == 200
    assert "值班与拨测".encode() in response.data or b"Duty &amp; Prober" in response.data
    assert b"hotline" in response.data
    assert b"ops" in response.data
    assert b"Alice" in response.data
    assert b"bi-lightning-charge" in response.data
    assert b"toggleProbe" in response.data



def test_prober_page_renders_chinese_locale(client, probe_config):
    with client.session_transaction() as sess:
        sess["_user_id"] = "1"
        sess["_fresh"] = True
        sess["locale"] = "zh"
    with patch("flask_login.utils._get_user") as mock_user:
        mock_user.return_value = MagicMock(is_authenticated=True)
        response = client.get("/prober")
    assert response.status_code == 200
    assert "值班与拨测".encode() in response.data
    assert "值班组".encode() in response.data
    assert "拨测任务".encode() in response.data
    assert "Webhook 推送地址".encode() in response.data

def test_prober_page_renders_dynamic_webhook_url(client, probe_config):
    with _login(client) as c:
        response = c.get("/prober", headers={"Host": "172.28.146.121:5000"})
    assert response.status_code == 200
    assert b"172.28.146.121:8084/call_and_sms" in response.data
    assert b"172.28.146.121:8084/call_only" in response.data

def test_save_group_round_trips_to_yml(client, probe_config):
    with _login(client) as c:
        response = c.post(
            "/prober/api/groups/save",
            json={
                "name": "net",
                "webhook_url": "https://hooks.example.com/net",
                "escalation_timeout": 45,
                "members": [
                    {"name": "Bob", "phone": "200"},
                    {"name": "Carol", "phone": "300"},
                ],
            },
        )
    assert response.status_code == 200
    data = response.get_json()
    assert "2 member" in data["message"] and "net" in data["message"]

    saved = probe_config.read_text(encoding="utf-8")
    assert "net:" in saved and "Carol" in saved and "https://hooks.example.com/net" in saved


def test_save_group_rejects_empty_roster(client, probe_config):
    with _login(client) as c:
        response = c.post(
            "/prober/api/groups/save",
            json={"name": "x", "webhook_url": "https://h.example", "members": []},
        )
    assert response.status_code == 400
    assert "member" in response.get_json()["message"]


def test_save_group_rejects_invalid_webhook(client, probe_config):
    with _login(client) as c:
        response = c.post(
            "/prober/api/groups/save",
            json={"name": "x", "webhook_url": "not-a-url", "members": [{"name": "a", "phone": "1"}]},
        )
    assert response.status_code == 400


def test_delete_group_blocked_while_probes_bind(client, probe_config):
    with _login(client) as c:
        response = c.post("/prober/api/groups/delete", json={"name": "ops"})
    assert response.status_code == 400
    assert "hotline" in response.get_json()["message"]


def test_save_probe_validates_type_and_group(client, probe_config):
    with _login(client) as c:
        bad_type = c.post(
            "/prober/api/probes/save",
            json={"name": "p1", "phone": "1", "type": "ivr", "duty_group": "ops"},
        )
        assert bad_type.status_code == 400

        bad_group = c.post(
            "/prober/api/probes/save",
            json={"name": "p1", "phone": "1", "type": "direct", "duty_group": "ghost"},
        )
        assert bad_group.status_code == 400
        assert "ghost" in bad_group.get_json()["message"]

        bad_cron = c.post(
            "/prober/api/probes/save",
            json={
                "name": "p1",
                "phone": "1",
                "type": "direct",
                "duty_group": "ops",
                "schedule": "not a cron",
            },
        )
        assert bad_cron.status_code == 400
        assert "cron" in bad_cron.get_json()["message"].lower()


def test_save_probe_round_trips_and_replaces_by_name(client, probe_config):
    with _login(client) as c:
        response = c.post(
            "/prober/api/probes/save",
            json={
                "name": "hotline",
                "phone": "95999",
                "type": "direct",
                "schedule": "30 10 * * *",
                "timeout": 30,
                "dtmf_digits": "",
                "duty_group": "ops",
            },
        )
    assert response.status_code == 200
    saved = probe_config.read_text(encoding="utf-8")
    assert "95999" in saved
    assert "95100" not in saved  # replaced, not duplicated

    from caller_prober.config import load_probe_config

    reloaded = load_probe_config(probe_config)
    assert len(reloaded.probes) == 1
    assert reloaded.probes[0].phone == "95999"
    assert reloaded.probes[0].probe_type == "direct"


def test_delete_probe_removes_entry(client, probe_config):
    with _login(client) as c:
        response = c.post("/prober/api/probes/delete", json={"name": "hotline"})
    assert response.status_code == 200
    from caller_prober.config import ProbeConfigError, load_probe_config

    # An empty probe list is rejected on load, so reload now fails loudly.
    with pytest.raises(ProbeConfigError):
        load_probe_config(probe_config)


def test_run_probe_dispatches_to_probe_queue(client, probe_config):
    with _login(client) as c, patch(
        "py_phone_caller_ui.prober.celery_app.send_task"
    ) as mock_send:
        mock_send.return_value = MagicMock(id="task-123")
        response = c.post("/prober/api/probes/run", json={"name": "hotline"})
    assert response.status_code == 200
    data = response.get_json()
    assert data["task_id"] == "task-123"
    kwargs = mock_send.call_args.kwargs["kwargs"]
    assert kwargs["phone"] == "95100"
    assert kwargs["duty_group"] == "ops"
    assert mock_send.call_args.kwargs["queue"] == "probe_queue"
    assert (
        mock_send.call_args.args[0]
        == "caller_prober.tasks.probe_tasks.run_probe"
    )


def test_run_probe_unknown_name(client, probe_config):
    with _login(client) as c:
        response = c.post("/prober/api/probes/run", json={"name": "ghost"})
    assert response.status_code == 404


def test_toggle_probe_stops_and_starts_schedule(client, probe_config):
    from caller_prober.config import load_probe_config

    with _login(client) as c:
        stopped = c.post(
            "/prober/api/probes/toggle", json={"name": "hotline", "enabled": False}
        )
    assert stopped.status_code == 200
    assert stopped.get_json()["enabled"] is False
    assert load_probe_config(probe_config).probes[0].enabled is False

    # Omitted flag flips the current state back on.
    with _login(client) as c:
        started = c.post("/prober/api/probes/toggle", json={"name": "hotline"})
    assert started.status_code == 200
    assert started.get_json()["enabled"] is True
    assert load_probe_config(probe_config).probes[0].enabled is True


def test_toggle_probe_unknown_name(client, probe_config):
    with _login(client) as c:
        response = c.post("/prober/api/probes/toggle", json={"name": "ghost"})
    assert response.status_code == 400
    assert "ghost" in response.get_json()["message"]


def test_save_probe_preserves_stopped_state(client, probe_config):
    from caller_prober.config import load_probe_config

    with _login(client) as c:
        c.post("/prober/api/probes/toggle", json={"name": "hotline", "enabled": False})
        response = c.post(
            "/prober/api/probes/save",
            json={
                "name": "hotline",
                "phone": "95999",
                "type": "direct",
                "schedule": "30 10 * * *",
                "duty_group": "ops",
            },
        )
    assert response.status_code == 200
    assert load_probe_config(probe_config).probes[0].enabled is False


def test_test_webhook_posts_summary(client, probe_config):
    with _login(client) as c, patch(
        "py_phone_caller_ui.prober.AlarmCaller"
    ) as mock_caller_cls:

            async def ok(url, payload):
                assert payload["event"] == "probe_alarm_test"
                assert payload["duty_group"] == "ops"
                return True, 200, "OK"

            mock_caller_cls.return_value.test_webhook = ok
            response = c.post(
                "/prober/api/test_webhook",
                json={"group": "ops", "webhook_url": "https://hooks.example.com/ops"},
            )
    assert response.status_code == 200
    assert "200" in response.get_json()["message"]


def test_save_group_with_notification_type_and_token(client, probe_config):
    with _login(client) as c:
        response = c.post(
            "/prober/api/groups/save",
            json={
                "name": "sms_ops",
                "webhook_url": "https://hooks.example.com/sms",
                "escalation_timeout": 30,
                "notification_type": "sms_before_call",
                "token": "tok_custom_123",
                "members": [{"name": "Dave", "phone": "400"}],
            },
        )
    assert response.status_code == 200
    from caller_prober.config import load_probe_config

    cfg = load_probe_config(probe_config)
    g = cfg.duty_groups["sms_ops"]
    assert g.notification_type == "sms_before_call"
    assert g.token == "tok_custom_123"


def test_regen_group_token(client, probe_config):
    with _login(client) as c:
        response = c.post("/prober/api/groups/regen_token", json={"name": "ops"})
    assert response.status_code == 200
    data = response.get_json()
    assert data["token"] and data["token"].startswith("tok_") or len(data["token"]) >= 16
    from caller_prober.config import load_probe_config

    cfg = load_probe_config(probe_config)
    assert cfg.duty_groups["ops"].token == data["token"]


def test_generate_token_api(client, probe_config):
    with _login(client) as c:
        response = c.get("/prober/api/token/generate")
    assert response.status_code == 200
    data = response.get_json()
    assert "token" in data and len(data["token"]) >= 16


def test_api_probe_records_returns_list(client, probe_config):
    with _login(client) as c:
        response = c.get("/prober/api/probes/records")
    assert response.status_code == 200
    data = response.get_json()
    assert "records" in data
    assert isinstance(data["records"], list)


def test_records_page_lists_alarm_calls(client, probe_config):
    with _login(client) as c:
        response = c.get("/prober/records")
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Alarm Calls" in body or "告警呼叫" in body
    assert "Probe Records" in body or "拨测记录" in body


def test_prober_page_links_records_tab(client, probe_config):
    with _login(client) as c:
        response = c.get("/prober")
    body = response.get_data(as_text=True)
    assert "/prober/records" in body


def _write_records(tmp_path, monkeypatch, n):
    import json as json_mod
    from datetime import UTC, datetime, timedelta

    from caller_prober import records as rec

    path = tmp_path / "probe_runs.jsonl"
    base = datetime(2026, 11, 7, 0, 0, tzinfo=UTC)
    rows = []
    for index in range(n):
        rows.append(
            {
                "event": "probe_completed",
                "task_id": f"t-{index}",
                "probe_name": f"probe-{index}",
                "phone": "100",
                "verdict": "OK",
                "finished_at": (base + timedelta(minutes=index)).isoformat(),
            }
        )
    path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(rec, "records_path", lambda: path)


def test_records_page_default_page_size_and_order(client, probe_config, tmp_path, monkeypatch):
    _write_records(tmp_path, monkeypatch, 60)
    with _login(client) as c:
        response = c.get("/prober/records")
    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "probe-59" in body  # newest first
    # same default page size as the Managed Calls page
    assert body.count('font-monospace">2026-11-07') == 50
    assert "probe-9" not in body  # page 1 only holds the newest 50


def test_records_page_size_five(client, probe_config, tmp_path, monkeypatch):
    _write_records(tmp_path, monkeypatch, 8)
    with _login(client) as c:
        response = c.get("/prober/records?per_page=5&page=2")
    body = response.get_data(as_text=True)
    assert "probe-2" in body
    assert "probe-3" not in body
    assert "probe-7" not in body


def test_records_page_rejects_unknown_page_size(client, probe_config, tmp_path, monkeypatch):
    _write_records(tmp_path, monkeypatch, 60)
    with _login(client) as c:
        response = c.get("/prober/records?per_page=999")
    body = response.get_data(as_text=True)
    assert body.count('font-monospace">2026-11-07') == 50


def test_records_timestamps_use_platform_timezone(client, probe_config, tmp_path, monkeypatch):
    _write_records(tmp_path, monkeypatch, 1)
    with _login(client) as c:
        response = c.get("/prober/records")
    body = response.get_data(as_text=True)
    # 2026-11-07T00:00Z rendered in Asia/Shanghai (UTC+8)
    assert "2026-11-07 08:00:00" in body


def test_records_api_paginates(client, probe_config, tmp_path, monkeypatch):
    _write_records(tmp_path, monkeypatch, 12)
    with _login(client) as c:
        response = c.get("/prober/api/probes/records?per_page=10&page=2")
    data = response.get_json()
    assert data["total"] == 12
    assert data["pages"] == 2
    assert data["per_page"] == 10
    assert len(data["records"]) == 2
    assert data["records"][0]["probe_name"] == "probe-1"
    assert data["timezone"] == "Asia/Shanghai"


TWO_PROBES_YML = textwrap.dedent(
    """
    duty_groups:
      network_ops:
        members:
          - name: Alice
            phone: "100"
        escalation_timeout: 30
        webhook_url: "https://hooks.example.com/ops"
    probes:
      - phone: "057188880001"
        name: 办公室直线
        type: direct
        schedule: "0 10 * * *"
        timeout: 30
        dtmf_digits: null
        duty_group: network_ops
      - phone: "01084607360"
        name: 客服中心主热线
        type: callcenter
        schedule: "0 9 * * *"
        timeout: 60
        dtmf_digits: "1"
        duty_group: network_ops
    """
)


@pytest.fixture
def two_probes_config(tmp_path, monkeypatch):
    """Probe config with two targets so search can be verified both ways."""
    path = tmp_path / "probe_config.yml"
    path.write_text(TWO_PROBES_YML, encoding="utf-8")
    monkeypatch.setenv("CALLER_PROBE_CONFIG", str(path))
    return path


def test_probe_task_list_has_no_search_or_auto_refresh(client, two_probes_config):
    with _login(client) as c:
        body = c.get("/prober").get_data(as_text=True)
    assert 'name="search"' not in body
    assert "autoRefreshSelect" not in body
    assert "办公室直线" in body and "客服中心主热线" in body


def test_records_page_searches_by_target_and_failure_code(client, two_probes_config, tmp_path, monkeypatch):
    import json as json_mod

    from caller_prober import records as rec

    path = tmp_path / "probe_runs.jsonl"
    rows = [
        {
            "event": "probe_completed",
            "task_id": "t-ok",
            "probe_name": "办公室直线",
            "phone": "057188880001",
            "verdict": "OK",
            "finished_at": "2026-11-07T09:10:00+00:00",
        },
        {
            "event": "probe_completed",
            "task_id": "t-fail",
            "probe_name": "客服中心主热线",
            "phone": "01084607360",
            "verdict": "FAIL",
            "failure_code": "CC_ONE_WAY_AUDIO",
            "finished_at": "2026-11-07T09:20:00+00:00",
        },
    ]
    path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(rec, "records_path", lambda: path)

    with _login(client) as c:
        by_name = c.get("/prober/records?search=办公室").get_data(as_text=True)
        by_code = c.get("/prober/records?search=CC_ONE_WAY").get_data(as_text=True)
        no_match = c.get("/prober/records?search=zzz").get_data(as_text=True)

    assert "办公室直线" in by_name and "客服中心主热线" not in by_name
    assert "客服中心主热线" in by_code and "办公室直线" not in by_code
    assert "未找到匹配的拨测记录" in no_match and "zzz" in no_match


def test_records_page_searches_alarm_phone_numbers(client, two_probes_config, tmp_path, monkeypatch):
    import json as json_mod

    from caller_prober import records as rec

    path = tmp_path / "probe_runs.jsonl"
    rows = [
        {
            "event": "probe_completed",
            "task_id": "t-1",
            "probe_name": "hotline",
            "phone": "95100",
            "verdict": "FAIL",
            "finished_at": "2026-11-07T09:20:00+00:00",
        },
        {
            "event": "duty_alarm",
            "probe_task_id": "t-1",
            "calls": [{"phone": "13800000002", "acked": True}],
        },
    ]
    path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(rec, "records_path", lambda: path)

    with _login(client) as c:
        body = c.get("/prober/records?search=13800000002").get_data(as_text=True)

    assert "hotline" in body


def test_records_page_has_search_and_auto_refresh(client, two_probes_config, tmp_path, monkeypatch):
    _write_records(tmp_path, monkeypatch, 3)
    with _login(client) as c:
        body = c.get("/prober/records").get_data(as_text=True)
    assert 'name="search"' in body
    assert "autoRefreshSelect" in body
    assert "autoRefreshCountdown" in body
    assert "prober_records_auto_refresh_seconds" in body


def test_records_search_is_kept_across_pages(client, two_probes_config, tmp_path, monkeypatch):
    import json as json_mod

    from caller_prober import records as rec

    path = tmp_path / "probe_runs.jsonl"
    rows = [
        {
            "event": "probe_completed",
            "task_id": f"t-{i}",
            "probe_name": "hotline",
            "verdict": "FAIL",
            "failure_code": "PROBE_TIMEOUT",
            "finished_at": f"2026-11-07T09:{i:02d}:00+00:00",
        }
        for i in range(12)
    ]
    path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(rec, "records_path", lambda: path)

    with _login(client) as c:
        body = c.get("/prober/records?search=PROBE_TIMEOUT&per_page=5").get_data(as_text=True)

    assert "search=PROBE_TIMEOUT" in body
    assert body.count('font-monospace">2026-11-07') == 5


def test_help_center_documents_webhook_integration(client, two_probes_config):
    with _login(client) as c:
        body = c.get("/prober").get_data(as_text=True)
    # the guide now lives in the help drawer
    assert 'id="collapseWebhook"' in body
    for endpoint in ("/call_only", "/sms_only", "/sms_before_call", "/call_and_sms"):
        assert endpoint in body
    assert "ALERT.MESSAGE" in body  # Zabbix example
    assert "curl -X POST" in body
    assert "X-Webhook-Token" in body
    assert 'data-section="webhook"' in body


def test_prober_page_links_help_center_instead_of_embedding_guide(client, two_probes_config):
    with _login(client) as c:
        body = c.get("/prober").get_data(as_text=True)
    assert "Deployed Webhook Integration Guide" not in body
    assert 'data-bs-target="#helpOffcanvas"' in body
    # per-group copy shortcut still available in the roster table
    assert "copyGroupWebhookUrl" in body


def _write_recording_record(tmp_path, monkeypatch, name):
    import json as json_mod

    from caller_prober import records as rec

    path = tmp_path / "probe_runs.jsonl"
    rows = [
        {
            "event": "probe_completed",
            "task_id": "t-rec",
            "probe_name": "hotline",
            "phone": "95100",
            "verdict": "OK",
            "recording": name,
            "finished_at": "2026-11-08T05:00:00+00:00",
        }
    ]
    path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(rec, "records_path", lambda: path)


def test_records_page_renders_audio_player(client, two_probes_config, tmp_path, monkeypatch):
    _write_recording_record(tmp_path, monkeypatch, "20261108T050000-abc123")

    with _login(client) as c:
        body = c.get("/prober/records").get_data(as_text=True)

    assert "<audio" in body
    assert "/prober/records/recording/20261108T050000-abc123" in body


def test_recording_endpoint_rejects_unknown_name(client, two_probes_config, tmp_path, monkeypatch):
    _write_recording_record(tmp_path, monkeypatch, "20261108T050000-abc123")

    with _login(client) as c:
        response = c.get("/prober/records/recording/not-in-records")

    assert response.status_code == 404


def test_recording_endpoint_streams_ari_audio(client, two_probes_config, tmp_path, monkeypatch):
    import aiohttp

    _write_recording_record(tmp_path, monkeypatch, "20261108T050000-abc123")
    payload = b"RIFF" + b"\x00" * 64

    class FakeResponse:
        status = 200

        async def read(self):
            return payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class FakeSession:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        def get(self, *a, **k):
            return FakeResponse()

    monkeypatch.setattr(aiohttp, "ClientSession", FakeSession)

    with _login(client) as c:
        response = c.get("/prober/records/recording/20261108T050000-abc123")

    assert response.status_code == 200
    assert response.mimetype == "audio/wav"
    assert response.data.startswith(b"RIFF")
