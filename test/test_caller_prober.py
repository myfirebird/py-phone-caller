"""
Unit tests for the caller_prober package: config loading, probe evaluation
verdicts, and the duty alarm dispatch flow (webhook + sequential ACK calls).
"""

import json
import textwrap
from datetime import UTC, datetime

import pytest

from caller_prober.config import load_probe_config
from caller_prober.exceptions import ProbeConfigError
from caller_prober.services.probe_evaluator import (
    ProbeOutcome,
    evaluate_probe,
    failure_code_for_hangup_cause,
)


@pytest.fixture(autouse=True)
def _no_live_sms_dispatch(monkeypatch):
    """Tests must never enqueue real SMS tasks onto the live Celery broker."""
    from py_phone_caller_utils.tasks import celery_task

    calls = []
    monkeypatch.setattr(
        celery_task.dispatch_async_sms, "apply_async", lambda *a, **k: calls.append((a, k))
    )
    return calls


VALID_CONFIG = textwrap.dedent(
    """
    duty_groups:
      ops:
        members:
          - name: Alice
            phone: "100"
          - name: Bob
            phone: "200"
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
      - phone: "5551234"
        name: trunk
        type: direct
        schedule: "0 10 * * *"
        timeout: 30
        duty_group: ops
    """
)


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    """Writes a valid probe config to disk and points the loader at it."""
    path = tmp_path / "probe_config.yml"
    path.write_text(VALID_CONFIG, encoding="utf-8")
    monkeypatch.setenv("CALLER_PROBE_CONFIG", str(path))
    return path


class TestProbeConfig:
    def test_loads_groups_and_probes(self, config_file):
        config = load_probe_config()
        assert set(config.duty_groups) == {"ops"}
        group = config.duty_groups["ops"]
        assert group.phones() == ("100", "200")
        assert group.webhook_url == "https://hooks.example.com/ops"
        assert group.escalation_timeout == 30
        assert len(config.probes) == 2
        assert config.probes[0].probe_type == "callcenter"
        assert config.probes[0].dtmf_digits == "1"
        assert config.probes[1].probe_type == "direct"

    def test_group_for_binds_probe(self, config_file):
        config = load_probe_config()
        assert config.group_for(config.probes[0]).name == "ops"

    def test_rejects_unknown_duty_group(self, tmp_path, monkeypatch):
        path = tmp_path / "probe_config.yml"
        path.write_text(
            VALID_CONFIG.replace("duty_group: ops", "duty_group: ghost"), encoding="utf-8"
        )
        monkeypatch.setenv("CALLER_PROBE_CONFIG", str(path))
        with pytest.raises(ProbeConfigError):
            load_probe_config()

    def test_rejects_invalid_type(self, tmp_path, monkeypatch):
        path = tmp_path / "probe_config.yml"
        path.write_text(
            VALID_CONFIG.replace("type: callcenter", "type: ivr-bot"), encoding="utf-8"
        )
        monkeypatch.setenv("CALLER_PROBE_CONFIG", str(path))
        with pytest.raises(ProbeConfigError):
            load_probe_config()

    def test_rejects_empty_roster(self, tmp_path, monkeypatch):
        import yaml

        path = tmp_path / "probe_config.yml"
        data = yaml.safe_load(VALID_CONFIG)
        data["duty_groups"]["ops"]["members"] = []
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        monkeypatch.setenv("CALLER_PROBE_CONFIG", str(path))
        with pytest.raises(ProbeConfigError):
            load_probe_config()


    def test_missing_file_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CALLER_PROBE_CONFIG", str(tmp_path / "nope.yml"))
        with pytest.raises(ProbeConfigError):
            load_probe_config()


class TestDirectEvaluation:
    def test_answered_is_ok(self):
        result = evaluate_probe(ProbeOutcome(probe_type="direct", answered=True), None)
        assert result.ok
        assert result.failure_code is None

    def test_ring_no_answer_is_reachable(self):
        result = evaluate_probe(
            ProbeOutcome(probe_type="direct", answered=False, hangup_cause=19), None
        )
        assert result.ok

    def test_busy_fails_with_sip_busy(self):
        result = evaluate_probe(
            ProbeOutcome(probe_type="direct", answered=False, hangup_cause=17), None
        )
        assert not result.ok
        assert result.failure_code == "SIP_BUSY"

    def test_congestion_fails(self):
        result = evaluate_probe(
            ProbeOutcome(probe_type="direct", answered=False, hangup_cause=34), None
        )
        assert result.failure_code == "SIP_CONGESTION"

    def test_not_answered_without_cause(self):
        result = evaluate_probe(ProbeOutcome(probe_type="direct"), None)
        assert result.failure_code == "NOT_ANSWERED"

    def test_originate_http_503(self):
        result = evaluate_probe(
            ProbeOutcome(probe_type="direct", http_status=503), None
        )
        assert result.failure_code == "SIP_SERVICE_UNAVAILABLE"

    def test_probe_timeout(self):
        result = evaluate_probe(
            ProbeOutcome(probe_type="direct", timed_out=True), None
        )
        assert result.failure_code == "PROBE_TIMEOUT"


class TestCallcenterEvaluation:
    def test_healthy_path(self):
        result = evaluate_probe(
            ProbeOutcome(
                probe_type="callcenter",
                answered=True,
                entered_stasis=True,
                connected_seconds=12.0,
                media_frames_observed=40,
                dtmf_digits_sent=True,
                channel_alive_after_dtmf=True,
                hangup_cause=16,
            ),
            "1",
        )
        assert result.ok

    def test_early_hangup(self):
        result = evaluate_probe(
            ProbeOutcome(
                probe_type="callcenter",
                answered=True,
                entered_stasis=True,
                connected_seconds=1.5,
                media_frames_observed=5,
            ),
            None,
        )
        assert result.failure_code == "CC_EARLY_HANGUP"
        assert result.details["early_hangup_threshold"] == 3.0

    def test_one_way_audio(self):
        result = evaluate_probe(
            ProbeOutcome(
                probe_type="callcenter",
                answered=True,
                entered_stasis=True,
                connected_seconds=20.0,
                media_frames_observed=0,
            ),
            None,
        )
        assert result.failure_code == "CC_ONE_WAY_AUDIO"

    def test_no_stasis(self):
        result = evaluate_probe(
            ProbeOutcome(
                probe_type="callcenter",
                answered=True,
                entered_stasis=False,
                connected_seconds=20.0,
                media_frames_observed=10,
            ),
            None,
        )
        assert result.failure_code == "CC_NO_STASIS"

    def test_dtmf_kills_channel(self):
        result = evaluate_probe(
            ProbeOutcome(
                probe_type="callcenter",
                answered=True,
                entered_stasis=True,
                connected_seconds=20.0,
                media_frames_observed=10,
                dtmf_digits_sent=True,
                channel_alive_after_dtmf=False,
            ),
            "1",
        )
        assert result.failure_code == "CC_DTMF_CHANNEL_DEAD"

    def test_unanswered_callcenter_maps_cause(self):
        result = evaluate_probe(
            ProbeOutcome(probe_type="callcenter", answered=False, hangup_cause=19), None
        )
        assert result.failure_code == "NO_ANSWER"

    def test_4xx_cause_family(self):
        assert failure_code_for_hangup_cause(21) == "CALL_REJECTED"
        assert failure_code_for_hangup_cause(486) == "SIP_CLIENT_SERVER_ERROR"


class TestScanProbes:
    def test_dispatches_only_due_probes(self, config_file, monkeypatch):
        # Only the callcenter probe (cron "0 9 * * *") is due at 09:00.
        from datetime import UTC
        from datetime import datetime as real_datetime

        from caller_prober.tasks import probe_tasks

        fixed_now = real_datetime(2026, 11, 6, 9, 0, tzinfo=UTC)
        monkeypatch.setattr(
            probe_tasks, "datetime", type("FakeDT", (), {"now": staticmethod(lambda *a, **k: fixed_now)})
        )
        monkeypatch.setattr(probe_tasks, "_probe_due", lambda schedule, now: schedule == "0 9 * * *")

        dispatched = []
        monkeypatch.setattr(
            probe_tasks.run_probe,
            "apply_async",
            lambda kwargs=None, queue=None: dispatched.append((kwargs, queue)),
        )

        class FakeRedis:
            def set(self, *a, **k):
                return True

        import redis as redis_mod

        monkeypatch.setattr(redis_mod, "from_url", lambda *a, **k: FakeRedis())

        result = probe_tasks.scan_probes()
        assert result == "dispatched_1"
        kwargs, queue = dispatched[0]
        assert kwargs["probe_name"] == "hotline"
        assert kwargs["probe_type"] == "callcenter"
        assert queue == "probe_queue"


    def test_stopped_probe_is_not_dispatched(self, config_file, monkeypatch):
        from dataclasses import replace

        from caller_prober.config import load_probe_config, save_probe_config
        from caller_prober.tasks import probe_tasks

        config = load_probe_config()
        probes = tuple(replace(p, enabled=False) for p in config.probes)
        save_probe_config(replace(config, probes=probes))
        monkeypatch.setattr(probe_tasks, "_probe_due", lambda schedule, now: True)

        dispatched = []
        monkeypatch.setattr(
            probe_tasks.run_probe,
            "apply_async",
            lambda kwargs=None, queue=None: dispatched.append(kwargs),
        )

        class FakeRedis:
            def set(self, *a, **k):
                return True

        import redis as redis_mod

        monkeypatch.setattr(redis_mod, "from_url", lambda *a, **k: FakeRedis())

        assert probe_tasks.scan_probes() == "dispatched_0"
        assert dispatched == []

    def test_due_probe_fires_at_schedule_minute(self):
        from datetime import datetime

        from croniter import croniter

        now = datetime(2026, 11, 6, 9, 0, tzinfo=UTC)
        assert croniter.match("0 9 * * *", now)
        assert not croniter.match("30 9 * * *", now)


class TestDispatchDutyAlarm:
    @pytest.fixture
    def alarm_stub(self, monkeypatch):
        from caller_prober.tasks import probe_tasks

        calls = []
        stub = type("Stub", (), {})()
        monkeypatch.setattr(
            probe_tasks, "AlarmCaller", lambda: stub
        )

        async def post_webhook(url, payload):
            stub.webhook_payload = payload
            return True

        async def generate_tts_media(text):
            stub.spoken_text = text
            return "sound:http://127.0.0.1:8082/audio/alarm_z.wav"

        async def call_member_for_ack(phone, message, tts_media, wait_seconds):
            calls.append((phone, tts_media, wait_seconds))
            return phone == "200"  # second member ACKs

        stub.post_webhook = post_webhook
        stub.generate_tts_media = generate_tts_media
        stub.call_member_for_ack = call_member_for_ack
        stub.member_calls = calls
        stub.call_details = [
            {"phone": "100", "answered": True, "acked": False},
            {"phone": "200", "answered": True, "acked": True},
        ]
        return stub

    def test_webhook_then_sequential_calls_until_ack(self, config_file, alarm_stub):
        from caller_prober.tasks import probe_tasks

        raw = probe_tasks.dispatch_duty_alarm(
            probe_name="hotline",
            phone="95100",
            probe_type="callcenter",
            failure_code="CC_EARLY_HANGUP",
            hangup_cause=16,
            connected_seconds=1.4,
            duty_group="ops",
        )
        trail = json.loads(raw)
        assert trail["webhook_ok"] is True
        assert trail["acked_by"] == "Bob"
        assert [c[0] for c in alarm_stub.member_calls] == ["100", "200"]
        # Webhook summary carries the extracted failure code.
        assert alarm_stub.webhook_payload["failure_code"] == "CC_EARLY_HANGUP"

    def test_dispatch_speaks_probe_number_and_result(self, config_file, alarm_stub):
        from caller_prober.tasks import probe_tasks

        probe_tasks.dispatch_duty_alarm(
            probe_name="客服热线",
            phone="95100",
            probe_type="callcenter",
            failure_code="CC_ONE_WAY_AUDIO",
            hangup_cause=16,
            connected_seconds=8.0,
            duty_group="ops",
        )
        spoken = alarm_stub.spoken_text
        assert "九五一零零" in spoken
        assert "单通, 没有语音媒体" in spoken
        assert "八秒" in spoken
        assert "CC_ONE_WAY_AUDIO" not in spoken

    def test_dispatch_uses_preset_alert_when_tts_fails(self, config_file, alarm_stub):
        from caller_prober.tasks import probe_tasks

        async def failing_tts(text):
            alarm_stub.spoken_text = text

        alarm_stub.generate_tts_media = failing_tts
        raw = probe_tasks.dispatch_duty_alarm(
            probe_name="客服热线",
            phone="95100",
            probe_type="callcenter",
            failure_code="CC_NO_STASIS",
            hangup_cause=None,
            connected_seconds=0.0,
            duty_group="ops",
        )
        trail = json.loads(raw)
        # Calls still happen and carry no TTS media, so _wait_ack falls back to
        # the locally installed preset alert (sound:prober-alarm).
        assert trail["called"] == ["100", "200"]
        assert [c[1] for c in alarm_stub.member_calls] == [None, None]



    def test_dispatch_records_alarm_trail(self, config_file, alarm_stub, monkeypatch):
        from caller_prober.tasks import probe_tasks

        async def failing_tts(text):
            alarm_stub.spoken_text = text

        alarm_stub.generate_tts_media = failing_tts
        async def noop_persist(*args, **kwargs):
            return None

        monkeypatch.setattr(probe_tasks, "_persist_alarm_calls", lambda *a, **k: None)

        probe_tasks.dispatch_duty_alarm(
            probe_name="客服热线",
            phone="95100",
            probe_type="callcenter",
            failure_code="CC_ONE_WAY_AUDIO",
            hangup_cause=16,
            connected_seconds=8.0,
            duty_group="ops",
            probe_task_id="probe-t-9",
        )

        from caller_prober import records as rec

        rows = rec.read_records(limit=0, newest_first=False)
        alarm = next(r for r in rows if r.get("event") == "duty_alarm")
        assert alarm["probe_task_id"] == "probe-t-9"
        assert alarm["failure_code"] == "CC_ONE_WAY_AUDIO"
        assert alarm["duty_group"] == "ops"

    def test_unknown_group_aborts(self, config_file):
        from caller_prober.tasks import probe_tasks

        raw = probe_tasks.dispatch_duty_alarm(
            "hotline", "95100", "callcenter", "NO_ANSWER", 19, 0.0, "ghost"
        )
        assert raw.startswith("unknown_group_")


class TestRunProbe:
    def test_failed_probe_fans_out_alarm(self, config_file, monkeypatch):
        from caller_prober.services import probe_evaluator as ev
        from caller_prober.tasks import probe_tasks

        outcome = ev.ProbeOutcome(
            probe_type="direct", answered=False, hangup_cause=17
        )


        fanned = []
        monkeypatch.setattr(
            probe_tasks.dispatch_duty_alarm,
            "apply_async",
            lambda kwargs=None, queue=None: fanned.append(kwargs),
        )

        # Force the evaluate path: run_probe calls run_probe_call then evaluate.
        async def fake_probe_call(
            phone, probe_type, timeout, dtmf_preset, max_call_seconds=None
        ):
            return outcome

        monkeypatch.setattr(probe_tasks, "run_probe_call", fake_probe_call)
        raw = probe_tasks.run_probe(
            phone="5551234",
            probe_name="trunk",
            probe_type="direct",
            timeout=30,
            dtmf_preset=None,
            duty_group="ops",
        )
        record = json.loads(raw)
        assert record["verdict"] == "FAIL"
        assert record["failure_code"] == "SIP_BUSY"
        assert fanned and fanned[0]["failure_code"] == "SIP_BUSY"
        assert fanned[0]["duty_group"] == "ops"
        assert "started_at" in record and "hangup_cause" in record


class TestTalkLimit:
    def test_default_talk_limit_from_settings(self, tmp_path, monkeypatch):
        from caller_prober import config as cfg_mod
        from caller_prober.config import _parse_probe

        monkeypatch.setattr(cfg_mod, "DEFAULT_MAX_CALL_SECONDS", 20.0)
        probe = _parse_probe(
            {
                "phone": "100",
                "name": "p",
                "type": "direct",
                "schedule": "0 3 * * *",
                "duty_group": "ops",
            }
        )
        assert probe.max_call_seconds == 20.0

    def test_per_probe_talk_limit_override(self):
        from caller_prober.config import _parse_probe

        probe = _parse_probe(
            {
                "phone": "100",
                "name": "p",
                "type": "callcenter",
                "schedule": "0 3 * * *",
                "duty_group": "ops",
                "max_call_seconds": 8,
            }
        )
        assert probe.max_call_seconds == 8.0

    def test_negative_talk_limit_rejected(self, tmp_path, monkeypatch):
        from caller_prober.config import load_probe_config

        cfg = tmp_path / "probe.yml"
        cfg.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                    max_call_seconds: 0
                """
            ),
            encoding="utf-8",
        )
        with pytest.raises(ProbeConfigError, match="max_call_seconds"):
            load_probe_config(cfg)

    def test_talk_budget_caps_at_configured_talk_time(self):
        from caller_prober import ari_prober as ap

        loop = type("L", (), {"time": lambda self: 1000.0})()
        state = ap._EventState(answered=True, answered_at=995.0)  # answered 5s ago
        assert ap._talk_budget(state, loop, 20.0, 60.0) == pytest.approx(15.0)
        # a probe timeout tighter than the talk limit wins
        assert ap._talk_budget(state, loop, 20.0, 8.0) == pytest.approx(3.0)

    def test_run_probe_passes_talk_limit_to_call(self, config_file, monkeypatch):
        from caller_prober.services import probe_evaluator as ev
        from caller_prober.tasks import probe_tasks

        seen = {}

        async def fake_probe_call(phone, probe_type, timeout, dtmf_preset, max_call_seconds=None):
            seen["max_call_seconds"] = max_call_seconds
            return ev.ProbeOutcome(probe_type="direct", answered=True)

        monkeypatch.setattr(probe_tasks, "run_probe_call", fake_probe_call)
        monkeypatch.setattr(
            probe_tasks.dispatch_duty_alarm, "apply_async", lambda **kwargs: None
        )
        probe_tasks.run_probe(
            phone="100",
            probe_name="p",
            probe_type="direct",
            timeout=30,
            dtmf_preset=None,
            duty_group="ops",
            max_call_seconds=12.0,
        )
        assert seen["max_call_seconds"] == 12.0


class TestMediaTap:
    def test_joins_bridge_with_addchannel_endpoint(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        calls = []

        async def fake_post(session, url):
            calls.append(url)
            return 204, {}

        monkeypatch.setattr(ap, "_post_json", fake_post)
        status, _ = asyncio.run(ap._bridge_add(None, "bridge-1", "chan-1"))

        assert status == 204
        assert len(calls) == 1
        assert "bridges/bridge-1/addChannel" in calls[0]

    def test_falls_back_to_legacy_add_endpoint(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        calls = []

        async def fake_post(session, url):
            calls.append(url)
            return (404, {"message": "Resource not found"}) if len(calls) == 1 else (204, {})

        monkeypatch.setattr(ap, "_post_json", fake_post)
        status, _ = asyncio.run(ap._bridge_add(None, "bridge-1", "chan-1"))

        assert status == 204
        assert "addChannel" in calls[0]
        assert calls[1].endswith("/bridges/bridge-1/add?channel=chan-1")


class TestAlarmSpeech:
    def test_phone_spoken_digit_by_digit(self):
        from caller_prober.services.alarm_speech import speak_digits

        assert speak_digits("057188880001") == "零五七一 八八八八 零零零一"
        assert speak_digits(10086) == "一零零八六"

    def test_connected_duration_in_chinese(self):
        from caller_prober.services.alarm_speech import speak_seconds

        assert speak_seconds(0) == "零秒"
        assert speak_seconds(2.5) == "二点五秒"
        assert speak_seconds(12) == "十二秒"
        assert speak_seconds(105.4) == "一百零五点四秒"

    def test_alarm_speech_carries_number_result_and_ack(self):
        from caller_prober.services.alarm_speech import build_alarm_speech

        text = build_alarm_speech(
            probe_name="客服热线",
            phone="95100",
            probe_type="callcenter",
            failure_code="CC_EARLY_HANGUP",
            hangup_cause=16,
            connected_seconds=1.4,
        )
        assert "九五一零零" in text
        assert "客服热线" in text
        assert "呼叫中心客服号码" in text
        assert "接通后三秒内早挂机" in text
        assert "一点四秒" in text
        assert "正常挂断" in text
        assert "请按 一 确认接警" in text
        # Raw machine codes must never be spoken.
        assert "CC_EARLY_HANGUP" not in text

    def test_unknown_failure_code_falls_back_to_chinese(self):
        from caller_prober.services.alarm_speech import build_alarm_speech

        text = build_alarm_speech(
            probe_name="trunk",
            phone="200",
            probe_type="direct",
            failure_code="SOMETHING_NEW",
        )
        assert "未知故障" in text
        assert "普通直线号码" in text
        assert "SOMETHING_NEW" not in text


class _FakeWs:
    """Minimal async-iterable stand-in for an ARI events websocket."""

    def __init__(self, events):
        self._events = list(events)
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._events):
            raise StopAsyncIteration
        event = self._events[self._index]
        self._index += 1
        return json.dumps(event)


class TestAlarmAckWait:
    def _run(self, coro):
        import asyncio

        return asyncio.run(coro)

    def test_announces_only_after_answer_then_stops_on_ack(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        plays = []

        async def fake_post(session, url):
            plays.append(url)
            return 201, {"id": "pb-1"}

        async def fake_get(session, url):
            return 200, {"state": "playing"}

        monkeypatch.setattr(ap, "_post_json", fake_post)
        monkeypatch.setattr(ap, "_get_json", fake_get)
        state = ap._EventState()
        caller = ap.AlarmCaller()

        async def scenario():
            loop = asyncio.get_running_loop()
            task = asyncio.create_task(
                caller._wait_ack(
                    None, state, "chan-1", "sound:http://tts/alarm.wav", 5.0, loop
                )
            )
            await asyncio.sleep(0.3)
            assert not plays  # still ringing: nothing may be announced yet
            state.answered = True
            await asyncio.sleep(0.6)
            state.last_dtmf = "1"
            return await asyncio.wait_for(task, timeout=5)

        assert self._run(scenario()) is True
        assert len(plays) == 1
        assert "channels/chan-1/play" in plays[0]
        assert "sound%3Ahttp" in plays[0]  # media URI kept intact (sound: prefix)

    def test_unanswered_alarm_never_plays(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        plays = []

        async def fake_post(session, url):
            plays.append(url)
            return 201, {"id": "pb-1"}

        async def fake_get(session, url):
            return 200, {"state": "playing"}

        monkeypatch.setattr(ap, "_post_json", fake_post)
        monkeypatch.setattr(ap, "_get_json", fake_get)
        state = ap._EventState()
        caller = ap.AlarmCaller()

        async def scenario():
            loop = asyncio.get_running_loop()
            return await caller._wait_ack(None, state, "chan-2", None, 0.6, loop)

        assert self._run(scenario()) is False
        assert not plays

    def test_answer_detected_from_channel_state(self):
        """Regression: ChannelStateChange keeps the state inside `channel`."""
        import asyncio

        from caller_prober import ari_prober as ap

        def chan(state):
            return {"id": "c1", "state": state, "caller": {"number": "1"}, "connected": {}}

        state = ap._EventState()
        events = [
            {"type": "ChannelStateChange", "channel": chan("Ring")},
            {"type": "ChannelStateChange", "channel": chan("Up")},
        ]

        asyncio.run(ap._consume_events(_FakeWs(events), state, "c1"))

        assert state.answered is True
        assert state.answered_at is not None

    def test_ring_alone_is_not_an_answer(self):
        import asyncio

        from caller_prober import ari_prober as ap

        state = ap._EventState()
        events = [
            {
                "type": "ChannelStateChange",
                "channel": {"id": "c1", "state": "Ring", "caller": {"number": "1"}},
            }
        ]

        asyncio.run(ap._consume_events(_FakeWs(events), state, "c1"))

        assert state.answered is False

    def test_snapshot_marks_answered_channel(self, monkeypatch):
        """A call answered before the consumer started is still announced."""
        import asyncio

        from caller_prober import ari_prober as ap

        async def fake_get(session, url):
            return 200, {"id": "c1", "state": "Up"}

        monkeypatch.setattr(ap, "_get_json", fake_get)
        state = ap._EventState()
        caller = ap.AlarmCaller()

        asyncio.run(caller._sync_channel_state(None, state, "c1"))

        assert state.answered is True

    def test_unplayable_tts_degrades_to_preset_alert(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        plays = []
        gets = []

        async def fake_post(session, url):
            plays.append(url)
            return 201, {"id": f"pb-{len(plays)}"}

        async def fake_get(session, url):
            gets.append(url)
            # The first (TTS) playback fails, the preset alert then plays.
            return 200, {"state": "failed" if len(plays) == 1 else "playing"}

        monkeypatch.setattr(ap, "_post_json", fake_post)
        monkeypatch.setattr(ap, "_get_json", fake_get)
        state = ap._EventState(answered=True)
        caller = ap.AlarmCaller()

        async def scenario():
            loop = asyncio.get_running_loop()
            task = asyncio.create_task(
                caller._wait_ack(
                    None, state, "chan-4", "sound:http://tts/broken.wav", 6.0, loop
                )
            )
            await asyncio.sleep(1.2)
            state.last_dtmf = "1"
            return await asyncio.wait_for(task, timeout=6)

        assert self._run(scenario()) is True
        assert len(plays) == 2
        assert "sound%3Ahttp" in plays[0]
        assert "sound%3Aprober-alarm" in plays[1]
        assert gets  # playback progress was polled, not assumed

    def test_missing_tts_plays_preset_local_alert(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap
        from caller_prober.constants import ALARM_FALLBACK_MEDIA

        assert ALARM_FALLBACK_MEDIA == "sound:prober-alarm"
        plays = []

        async def fake_post(session, url):
            plays.append(url)
            return 201, {"id": "pb-1"}

        async def fake_get(session, url):
            return 200, {"state": "playing"}

        monkeypatch.setattr(ap, "_post_json", fake_post)
        monkeypatch.setattr(ap, "_get_json", fake_get)
        state = ap._EventState(answered=True)
        caller = ap.AlarmCaller()

        async def scenario():
            loop = asyncio.get_running_loop()
            task = asyncio.create_task(caller._wait_ack(None, state, "chan-3", None, 5.0, loop))
            await asyncio.sleep(0.6)
            state.last_dtmf = "1"
            return await asyncio.wait_for(task, timeout=5)

        assert self._run(scenario()) is True
        assert len(plays) == 1
        assert "sound%3Aprober-alarm" in plays[0]


class TestRecordsOrdering:
    def test_append_keeps_chronological_file_and_newest_first_reads(self, tmp_path, monkeypatch):
        from caller_prober import records as rec

        path = tmp_path / "probe_runs.jsonl"
        monkeypatch.setattr(rec, "records_path", lambda: path)

        for minute, name in ((10, "first"), (20, "second"), (30, "third")):
            rec.append_record(
                {
                    "event": "probe_completed",
                    "task_id": name,
                    "probe_name": name,
                    "finished_at": f"2026-11-07T09:{minute}:00+00:00",
                }
            )

        stored = rec.read_records(limit=0, newest_first=False)
        assert [r["probe_name"] for r in stored] == ["first", "second", "third"]
        assert [r["probe_name"] for r in rec.read_records(limit=0)] == [
            "third",
            "second",
            "first",
        ]
        assert [r["probe_name"] for r in rec.read_probe_history(limit=0)] == [
            "third",
            "second",
            "first",
        ]

    def test_legacy_scrambled_file_reads_newest_first(self, tmp_path, monkeypatch):
        """Earlier builds wrote newest-first plus the new entry at the tail."""
        import json as json_mod

        from caller_prober import records as rec

        path = tmp_path / "probe_runs.jsonl"
        rows = [
            {"event": "probe_completed", "task_id": "c", "finished_at": "2026-11-07T09:30:00+00:00"},
            {"event": "probe_completed", "task_id": "b", "finished_at": "2026-11-07T09:20:00+00:00"},
            {"event": "probe_completed", "task_id": "a", "finished_at": "2026-11-07T09:10:00+00:00"},
            {"event": "probe_completed", "task_id": "d", "finished_at": "2026-11-07T09:40:00+00:00"},
        ]
        path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n", encoding="utf-8")
        monkeypatch.setattr(rec, "records_path", lambda: path)

        history = rec.read_probe_history(limit=0)

        assert [r["task_id"] for r in history] == ["d", "c", "b", "a"]


class TestAlarmHistory:
    def test_history_joins_alarm_calls_to_probe_run(self, tmp_path, monkeypatch):
        import json as json_mod

        from caller_prober import records as rec

        path = tmp_path / "probe_runs.jsonl"
        rows = [
            {
                "event": "probe_completed",
                "task_id": "t-1",
                "probe_name": "hotline",
                "phone": "100",
                "verdict": "FAIL",
                "failure_code": "CC_EARLY_HANGUP",
            },
            {
                "event": "duty_alarm",
                "probe_task_id": "t-1",
                "calls": [
                    {"phone": "200", "acked": False, "answered": True},
                    {"phone": "300", "acked": True, "answered": True},
                ],
                "acked_by": "Bob",
            },
        ]
        path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n", encoding="utf-8")
        monkeypatch.setattr(rec, "records_path", lambda: path)

        history = rec.read_probe_history(limit=10)

        assert len(history) == 1
        assert history[0]["task_id"] == "t-1"
        assert history[0]["alarm_phones"] == "200, 300"
        assert history[0]["alarm_acked_by"] == "Bob"
        assert [c["phone"] for c in history[0]["alarm_calls"]] == ["200", "300"]

    def test_history_without_alarm_records(self, tmp_path, monkeypatch):
        import json as json_mod

        from caller_prober import records as rec

        path = tmp_path / "probe_runs.jsonl"
        path.write_text(
            json_mod.dumps({"event": "probe_completed", "task_id": "t-2", "verdict": "OK"}) + "\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(rec, "records_path", lambda: path)

        history = rec.read_probe_history()

        assert history[0]["alarm_phones"] == ""
        assert history[0]["alarm_calls"] == []


class TestNotificationType:
    def test_webhook_url_aligned_with_notification_type(self):
        from caller_prober.config import DutyGroup, DutyMember
        from caller_prober.tasks.probe_tasks import _aligned_webhook_url

        def group(mode, url):
            return DutyGroup(
                name="ops",
                members=(DutyMember("A", "100"),),
                escalation_timeout=30,
                webhook_url=url,
                notification_type=mode,
                token="t",
            )

        base = "http://192.168.1.12:8084"
        assert _aligned_webhook_url(group("call", f"{base}/call_and_sms?group=ops&token=t")) == (
            f"{base}/call?group=ops&token=t"
        )
        assert _aligned_webhook_url(group("sms", f"{base}/call?group=ops&token=t")) == (
            f"{base}/sms?group=ops&token=t"
        )
        assert _aligned_webhook_url(group("call_and_sms", f"{base}/alert?group=ops&token=t")) == (
            f"{base}/call_and_sms?group=ops&token=t"
        )
        # custom third-party endpoints are never rewritten
        custom = "https://hooks.example.com/ops"
        assert _aligned_webhook_url(group("call", custom)) == custom

    def test_call_only_group_never_sends_sms(self, config_file, monkeypatch):
        from py_phone_caller_utils.tasks import celery_task

        from caller_prober.tasks import probe_tasks

        sms_calls = []
        monkeypatch.setattr(
            celery_task.dispatch_async_sms,
            "apply_async",
            lambda *a, **k: sms_calls.append((a, k)),
        )

        posted = []

        class Stub:
            def __init__(self):
                self.call_details = []

            async def post_webhook(self, url, payload):
                posted.append(url)
                return True

            async def generate_tts_media(self, text):
                return None

            async def call_member_for_ack(self, *args, **kwargs):
                return False

        monkeypatch.setattr(probe_tasks, "AlarmCaller", Stub)
        monkeypatch.setattr(probe_tasks, "_persist_alarm_calls", lambda *a, **k: None)

        cfg = __import__("yaml").safe_load(config_file.read_text(encoding="utf-8"))
        cfg["duty_groups"]["ops"]["notification_type"] = "call"
        # a stale /call_and_sms suffix must not resurrect SMS
        cfg["duty_groups"]["ops"]["webhook_url"] = (
            "http://127.0.0.1:8084/call_and_sms?group=ops&token=t"
        )
        config_file.write_text(__import__("yaml").safe_dump(cfg), encoding="utf-8")

        probe_tasks.dispatch_duty_alarm(
            probe_name="p",
            phone="200",
            probe_type="direct",
            failure_code="SIP_BUSY",
            hangup_cause=17,
            connected_seconds=0.0,
            duty_group="ops",
        )

        assert sms_calls == []
        assert posted and posted[0].startswith("http://127.0.0.1:8084/call?group_id=")
        assert posted[0].endswith("&token=t")

    def test_sms_only_group_skips_calls(self, config_file, monkeypatch):
        from caller_prober.tasks import probe_tasks

        called = []

        class Stub:
            def __init__(self):
                self.call_details = []

            async def post_webhook(self, url, payload):
                return True

            async def generate_tts_media(self, text):
                return None

            async def call_member_for_ack(self, phone, *args, **kwargs):
                called.append(phone)
                return False

        monkeypatch.setattr(probe_tasks, "AlarmCaller", Stub)
        monkeypatch.setattr(probe_tasks, "_persist_alarm_calls", lambda *a, **k: None)

        cfg = __import__("yaml").safe_load(config_file.read_text(encoding="utf-8"))
        cfg["duty_groups"]["ops"]["notification_type"] = "sms"
        config_file.write_text(__import__("yaml").safe_dump(cfg), encoding="utf-8")

        probe_tasks.dispatch_duty_alarm(
            probe_name="p",
            phone="200",
            probe_type="direct",
            failure_code="SIP_BUSY",
            hangup_cause=17,
            connected_seconds=0.0,
            duty_group="ops",
        )

        assert called == []


class TestProbeWebhookToken:
    def test_token_generated_and_persisted(self, tmp_path):
        from caller_prober.config import load_probe_config, save_probe_config

        cfg_path = tmp_path / "probe_config.yml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                """
            ),
            encoding="utf-8",
        )
        config = load_probe_config(cfg_path)
        token = config.probes[0].token
        assert token

        save_probe_config(config, cfg_path)
        reloaded = load_probe_config(cfg_path)
        assert reloaded.probes[0].token == token

    def test_explicit_token_is_kept(self, tmp_path):
        from caller_prober.config import load_probe_config

        cfg_path = tmp_path / "probe_config.yml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                    token: "fixed-token-123"
                """
            ),
            encoding="utf-8",
        )
        assert load_probe_config(cfg_path).probes[0].token == "fixed-token-123"


class TestProbeStableId:
    def test_id_generated_and_persisted(self, tmp_path):
        from caller_prober.config import load_probe_config, save_probe_config

        cfg_path = tmp_path / "probe_config.yml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                """
            ),
            encoding="utf-8",
        )
        first = load_probe_config(cfg_path)
        probe_id = first.probes[0].id
        assert len(probe_id) >= 16

        save_probe_config(first, cfg_path)
        again = load_probe_config(cfg_path)
        assert again.probes[0].id == probe_id

    def test_id_survives_rename_and_token_rotation(self, tmp_path):
        from caller_prober.config import (
            ProbeTarget,
            load_probe_config,
            save_probe_config,
        )

        cfg_path = tmp_path / "probe_config.yml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: old-name
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                """
            ),
            encoding="utf-8",
        )
        cfg = load_probe_config(cfg_path)
        original = cfg.probes[0]
        renamed = ProbeTarget(
            phone=original.phone,
            name="new-name",
            probe_type=original.probe_type,
            schedule=original.schedule,
            timeout=original.timeout,
            dtmf_digits=original.dtmf_digits,
            duty_group=original.duty_group,
            max_call_seconds=original.max_call_seconds,
            token="rotated-token",
            id=original.id,
        )
        object.__setattr__(cfg, "probes", (renamed,))
        save_probe_config(cfg, cfg_path)

        reloaded = load_probe_config(cfg_path).probes[0]
        assert reloaded.name == "new-name"
        assert reloaded.token == "rotated-token"
        assert reloaded.id == original.id


class TestDutyGroupStableId:
    def test_group_id_generated_and_persisted(self, tmp_path):
        from caller_prober.config import load_probe_config, save_probe_config

        cfg_path = tmp_path / "probe_config.yml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                """
            ),
            encoding="utf-8",
        )
        cfg = load_probe_config(cfg_path)
        group_id = cfg.duty_groups["ops"].id
        assert len(group_id) >= 16

        save_probe_config(cfg, cfg_path)
        assert load_probe_config(cfg_path).duty_groups["ops"].id == group_id

    def test_alarm_url_uses_group_id_and_mode(self):
        from caller_prober.config import DutyGroup, DutyMember
        from caller_prober.tasks.probe_tasks import _aligned_webhook_url

        group = DutyGroup(
            name="ops",
            members=(DutyMember("A", "100"),),
            escalation_timeout=30,
            webhook_url="http://host:8084/call_and_sms?group=ops&token=t",
            notification_type="call",
            token="t",
            id="abc123",
        )

        url = _aligned_webhook_url(group)

        assert url == "http://host:8084/call?group_id=abc123&token=t"

    def test_alarm_url_keeps_custom_endpoints(self):
        from caller_prober.config import DutyGroup, DutyMember
        from caller_prober.tasks.probe_tasks import _aligned_webhook_url

        group = DutyGroup(
            name="ops",
            members=(DutyMember("A", "100"),),
            escalation_timeout=30,
            webhook_url="https://hooks.example.com/ops?group=ops",
            notification_type="call",
            token="t",
            id="abc123",
        )

        assert _aligned_webhook_url(group) == "https://hooks.example.com/ops?group=ops"


class TestProbeRecording:
    def test_start_recording_posts_channel_record(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        calls = []

        async def fake_post(session, url):
            calls.append(url)
            return 200, {}

        monkeypatch.setattr(ap, "_post_json", fake_post)
        name = asyncio.run(ap._start_recording(None, "channels", "chan-9", 20.0))

        assert name and len(name.split("-")[0]) == 15  # YYYYmmddTHHMMSS
        assert "channels/chan-9/record" in calls[0]
        assert "format=wav" in calls[0]
        assert "direction=both" in calls[0]

    def test_start_recording_targets_bridge_without_direction(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        calls = []

        async def fake_post(session, url):
            calls.append(url)
            return 200, {}

        monkeypatch.setattr(ap, "_post_json", fake_post)
        name = asyncio.run(ap._start_recording(None, "bridges", "bridge-7", 20.0))

        assert name
        assert "bridges/bridge-7/record" in calls[0]
        assert "direction=" not in calls[0]

    def test_start_recording_disabled(self, monkeypatch):
        import asyncio

        from caller_prober import ari_prober as ap

        monkeypatch.setattr(ap, "PROBE_RECORDING_ENABLED", False)
        assert asyncio.run(ap._start_recording(None, "channels", "chan-9", 20.0)) is None

    def test_prune_deletes_only_expired_probe_recordings(self, monkeypatch):
        import asyncio
        from datetime import UTC, datetime, timedelta

        from caller_prober import ari_prober as ap

        old = (datetime.now(UTC) - timedelta(days=30)).strftime("%Y%m%dT%H%M%S")
        fresh = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        deleted = []

        async def fake_get(session, url):
            return 200, [
                {"name": f"{old}-aaaaaa"},
                {"name": f"{fresh}-bbbbbb"},
                {"name": "unrelated-recording"},
            ]

        async def fake_delete(session, url):
            deleted.append(url)
            return 204, {}

        monkeypatch.setattr(ap, "_get_json", fake_get)
        monkeypatch.setattr(ap, "_delete", fake_delete)
        monkeypatch.setattr(ap, "PROBE_RECORDING_RETENTION_DAYS", 7)

        asyncio.run(ap._prune_recordings(None))

        assert len(deleted) == 1
        assert f"{old}-aaaaaa" in deleted[0]

    def test_probe_record_carries_recording(self, config_file, monkeypatch):
        from caller_prober.services import probe_evaluator as ev
        from caller_prober.tasks import probe_tasks

        async def fake_probe_call(phone, probe_type, timeout, dtmf_preset, max_call_seconds=None):
            return ev.ProbeOutcome(
                probe_type="direct", answered=True, recording_name="20261108T130000-abc123"
            )

        monkeypatch.setattr(probe_tasks, "run_probe_call", fake_probe_call)
        monkeypatch.setattr(
            probe_tasks.dispatch_duty_alarm, "apply_async", lambda **kwargs: None
        )

        raw = probe_tasks.run_probe(
            phone="100",
            probe_name="p",
            probe_type="direct",
            timeout=30,
            dtmf_preset=None,
            duty_group="ops",
        )

        assert json.loads(raw)["recording"] == "20261108T130000-abc123"


class TestProbeScheduleTimezone:
    def test_cron_is_evaluated_in_local_timezone(self):
        from zoneinfo import ZoneInfo

        from caller_prober.tasks.probe_tasks import _probe_due

        tz = ZoneInfo("Asia/Shanghai")
        # cron fields are wall-clock values of the datetime that is passed in
        assert _probe_due("0 10 * * *", datetime(2026, 11, 8, 10, 0, tzinfo=tz)) is True
        assert _probe_due("0 10 * * *", datetime(2026, 11, 8, 2, 0, tzinfo=tz)) is False
        # and the scanner passes LOCAL time, so 10:00 UTC (=18:00 local) must
        # evaluate against 18:00, not 10:00
        local = datetime(2026, 11, 8, 10, 0, tzinfo=UTC).astimezone(tz)
        assert local.hour == 18
        assert _probe_due("0 10 * * *", local) is False

    def test_local_now_uses_configured_timezone(self, monkeypatch):
        from caller_prober.tasks import probe_tasks

        monkeypatch.setattr(probe_tasks, "PROBE_TIMEZONE", "Asia/Shanghai")
        now = probe_tasks._local_now()

        assert now.tzinfo is not None
        assert now.utcoffset().total_seconds() == 8 * 3600

    def test_scan_dispatches_at_local_schedule_minute(self, config_file, monkeypatch):
        from zoneinfo import ZoneInfo

        from caller_prober.tasks import probe_tasks


        config_file.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 10 * * *"
                    duty_group: ops
                """
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(
            probe_tasks,
            "_local_now",
            lambda: datetime(2026, 11, 8, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        dispatched = []
        monkeypatch.setattr(
            probe_tasks.run_probe, "apply_async", lambda **kw: dispatched.append(kw)
        )
        monkeypatch.setattr(
            probe_tasks,
            "PROBE_TIMEZONE",
            "Asia/Shanghai",
        )
        # bypass the redis lock
        import redis as redis_mod

        class _FakeRedis:
            def set(self, *a, **k):
                return True

        monkeypatch.setattr(redis_mod, "from_url", lambda *a, **k: _FakeRedis())

        result = probe_tasks.scan_probes()

        assert result == "dispatched_1"
        assert dispatched[0]["kwargs"]["probe_name"] == "p"


class TestDutyGroupLang:
    """The duty group carries a default TTS voice language for webhook calls."""

    def test_group_lang_round_trips_through_the_config_file(self, tmp_path):
        from caller_prober.config import load_probe_config, save_probe_config

        cfg_path = tmp_path / "probe_config.yml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                    lang: "z"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                """
            ),
            encoding="utf-8",
        )
        config = load_probe_config(cfg_path)
        assert config.duty_groups["ops"].lang == "z"

        save_probe_config(config, cfg_path)
        assert load_probe_config(cfg_path).duty_groups["ops"].lang == "z"

    def test_group_without_lang_defaults_to_the_service_setting(self, tmp_path):
        from caller_prober.config import load_probe_config

        cfg_path = tmp_path / "probe_config.yml"
        cfg_path.write_text(
            textwrap.dedent(
                """
                duty_groups:
                  ops:
                    members:
                      - name: A
                        phone: "100"
                    escalation_timeout: 30
                    webhook_url: "https://hooks.example.com/ops"
                probes:
                  - phone: "200"
                    name: p
                    type: direct
                    schedule: "0 3 * * *"
                    duty_group: ops
                """
            ),
            encoding="utf-8",
        )
        assert load_probe_config(cfg_path).duty_groups["ops"].lang == ""
