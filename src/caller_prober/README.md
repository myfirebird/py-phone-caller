# caller_prober

Scheduled number probing and duty-group alarm routing engine for
py-phone-caller.

## What it does

- **Batch probing** of plain trunk numbers (`direct`) and callcenter hotlines
  (`callcenter`) via Asterisk ARI, driven by Celery Beat + a per-probe cron
  schedule (`config/probe_config.yml`). Cron expressions are wall-clock values
  in the platform local timezone (`scheduled_calls.local_timezone`, override
  with `[caller_prober] probe_timezone`) - a `0 10 * * *` probe fires at 10:00
  local, not 10:00 UTC.
- **On-demand trigger webhook**: every probe carries an immutable `id` and its
  own `token`; the platform exposes
  `POST /probe?probe_id=<id>&token=<probe token>` (same host/port as the
  duty-group webhooks). Monitoring systems (Zabbix, …) can
  start a probe immediately; the verdict and duty alarm pipeline are identical
  to a scheduled run. `probe_id` never changes (renames are harmless) while the
  token can be rotated at will - rotating only requires updating `?token=`.
  `?probe=<name>` and token-only addressing remain supported for convenience
  (409 only when two probes share a token and no id is given). The URL is shown at the bottom of the probe edit dialog
  with a copy button and is documented in the in-app Help Center.
- **Bounded talk time**: once connected, a probe stays on the call for
  `max_call_seconds` (per probe, default `probe_max_call_seconds` = 20s) and
  then hangs the channel up itself - IVR queues never hang up, so without the
  cap every callcount probe leaked a live call. The record reports
  `hung_up_by_probe: true` and the verdict is unaffected (normal clearing).
- **Call recording**: every probe call is recorded (Asterisk ARI, `wav`, both
  directions) and the recording name is stored in the execution record, so the
  console can play it back. Channels inside the media-tap bridge cannot be
  recorded directly, so callcenter probes record the mixing bridge. Asterisk
  needs its recording spool (`/var/spool/asterisk/recording`, created by the
  asterisk role); recordings older than `probe_recording_retention_days`
  (default 7) are purged on the next probe run.
- **Callcenter-aware verdicts**: SIP 200 OK alone is not enough. A healthy
  callcenter probe must answer, enter the Stasis probe app, stay connected
  ≥ 3s, show bidirectional media (externalMedia RTP tap), and survive a DTMF
  keypress sent 2s after answer. Failures map to codes such as
  `CC_EARLY_HANGUP`, `CC_ONE_WAY_AUDIO`, `CC_DTMF_CHANNEL_DEAD`,
  `SIP_BUSY`, `NO_ANSWER`, `SIP_CONGESTION`.
- **Stable webhook addressing**: duty groups and probes both carry an immutable
  `id`. Alarm and trigger URLs use `?group_id=` / `?probe_id=`, so renaming a
  group/task never changes the URL; rotating a token only invalidates the
  `?token=` part. Legacy `?group=<name>` / `?probe=<name>` addressing still
  works for compatibility.
- **Notification type is authoritative**: `notification_type` decides the
  channels. `call` never sends SMS, `sms` never dials. The group webhook URL is
  realigned to that type at dispatch (`/call`, `/sms`, `/call_and_sms`,
  `/sms_before_call`), because the receiver picks the channel mix from the URL
  suffix - a stale `/call_and_sms` suffix would otherwise resurrect SMS for a
  "call only" group. Third-party URLs are left untouched and every dispatch
  logs `DUTY_ALARM channels ... sms=skipped|enabled calls=skipped|enabled`.
- **Dual-path duty alarms**: on failure the bound duty group receives (1) a
  JSON summary POST to its exclusive webhook and (2) sequential voice calls
  with a Chinese TTS announcement; pressing `1` (ACK) stops escalation,
  timeout/reject escalates immediately to the next member. Every attempt
  (who, when, answered, ACK) is appended as a `duty_alarm` record and also
  written into the **Managed Calls** table so the alert trail sits next to the
  rest of the hosted calls.

## Alarm announcement (Chinese TTS)

The spoken message is built by `services/alarm_speech.py` and carries the
probed number and the call result - never a raw machine code:

> 值班告警。拨测号码 零一零八 四六零七 三六零, 呼叫中心客服号码 客服中心主热线,
> 呼叫失败。原因: 接通后三秒内早挂机。接通时长 一点四秒。挂断原因: 正常挂断。
> 请按 一 确认接警。

- Phone numbers are spelled digit by digit in Chinese numerals (groups of
  four) so the listener can transcribe them.
- Failure codes and hangup causes are translated to plain Chinese
  (`CC_EARLY_HANGUP` → 接通后三秒内早挂机, `SIP_BUSY` → 线路忙, …), and the
  connected duration is spoken (`一点四秒`).
- The same text is sent as the SMS body, so voice and text stay identical.

Delivery path: `dispatch_duty_alarm` → `generate_audio` (`make_audio` with
`lang` = `alarm_tts_lang`, default `z` = Chinese) → `is_audio_ready` →
ARI `play` of `sound:http://<generate_audio>/audio/<checksum>_<lang>.wav`.

Two deployment requirements follow from that path:

1. Asterisk fetches the wav over **HTTP**, so the `generate_audio` port must
   be labelled `http_port_t` in SELinux (see the ansible role). Without it
   `asterisk_t` gets `name_connect` denied and the call plays silence.
2. `res_stasis_playback` rejects bare `http://` URIs; the `sound:` prefix is
   mandatory. The announcement starts only after the callee answers and is
   replayed until ACK, hangup, or escalation timeout.

### Alarm call sequencing (why it can go silent)

- The ARI event websocket is opened **before** the originate. A trunk that
  answers immediately fires `ChannelStateChange(Up)`/`StasisStart` within
  milliseconds; subscribing afterwards loses those events forever, which made
  the alarm wait out its whole window without announcing anything.
- `ChannelStateChange` keeps the lifecycle state in `channel.state`, not at the
  event top level - matching on the event body alone never detects an answer.
- After the originate the channel snapshot (`GET /channels/{id}`) is read so a
  call answered before the consumer started is still announced. A `Down`
  snapshot is *not* treated as a hangup (freshly originated channels report it
  before the first SIP exchange).
- Playback progress is polled through `GET /playbacks/{id}`; a rejected play
  request, a failed playback, or one that ends in under 2s (Asterisk reports
  `done` even when it cannot fetch the media) degrades the current call to
  `alarm_fallback_media` so the responder never hears silence.

## Layout

| Path | Role |
| --- | --- |
| `config.py` | `probe_config.yml` loader (duty groups, probes, talk limit) |
| `records.py` | JSONL execution history + `read_probe_history()` alarm join |
| `services/probe_evaluator.py` | Pure verdict logic (direct / callcenter) |
| `services/alarm_speech.py` | Chinese TTS/SMS announcement text builder |
| `ari_prober.py` | ARI originate, event websocket, DTMF, media tap, alarm calls |
| `tasks/probe_tasks.py` | `scan_probes` / `run_probe` / `dispatch_duty_alarm` Celery tasks |

## Tasks & queues

| Task | Queue | Purpose |
| --- | --- | --- |
| `caller_prober.tasks.probe_tasks.scan_probes` | `telephony.p2` (Beat, default every 60s) | Redis-lock deduplicated scan; dispatches due probes |
| `caller_prober.tasks.probe_tasks.run_probe` | `probe_queue` | One probe call + verdict + alarm fan-out |
| `caller_prober.tasks.probe_tasks.dispatch_duty_alarm` | `probe_queue` | Webhook + sequential ACK calls |

Run a dedicated worker: `celery -A py_phone_caller_utils.tasks.celery_task worker -Q probe_queue`.

## Configuration

- `config/probe_config.yml` (repo root; override with `CALLER_PROBE_CONFIG`)
  defines `duty_groups` (members, `escalation_timeout`, `webhook_url`,
  optional `lang`) and
  `probes` (phone, type, cron `schedule`, timeout, `dtmf_digits`, `duty_group`,
  `enabled`). `enabled` defaults to `true`; `false` stops the cron schedule
  while leaving manual runs and the trigger webhook usable.
- A duty group's optional `lang` is the default TTS voice language for
  webhook calls routed to that group (`POST /call?group_id=<id>`); it is
  overridden by an explicit `?lang=` and falls back to
  `caller_prometheus_webhook.webhook_tts_lang`, then to the generate_audio
  default. Empty means "no override". Editable in the console's group dialog.
- `[caller_prober]` in `src/config/settings.toml` tunes Stasis app names,
  timeouts (`webhook_timeout_total`, `ari_http_timeout_total`,
  `ari_ws_close_timeout`), ACK digit (`alarm_ack_digit`), the announcement
  language (`alarm_tts_lang`, default `z`), the synthesis budget
  (`alarm_tts_timeout_total`, default 90s) and the TTS fallback sound.

## Web console

The `py_phone_caller_ui` service exposes a **Prober Console** page
(`/prober`, navbar entry "Prober") that manages everything in
`probe_config.yml` at runtime:

- Duty groups: create/edit/delete (members in escalation order, webhook URL,
  escalation timeout) plus one-click webhook delivery tests.
- Probe targets: create/edit/delete with server-side validation of type,
  cron schedule, timeout, and duty-group binding.
- "Run now": dispatches `run_probe` to `probe_queue` immediately, bypassing
  the cron schedule. The button uses the same secondary/"lightning" style as
  the duty-group webhook test.
- Start/Stop: toggles `enabled` per probe. Stopped probes show a "Stopped"
  badge and are skipped by the Beat scan; manual runs and the trigger webhook
  still work.
- Two tabs: **值班与拨测** (duty groups + probe targets) and **拨测记录**
  (`/prober/records`, execution history). Each record row lists the duty
  numbers dialled for that failure, whether the member answered, who pressed
  the ACK digit, and flags runs hung up by the talk limit.

Saves round-trip through the config parser before the file is replaced
 atomically, so a broken edit can never corrupt the live worker config.

## Notes

- All webhook and ARI HTTP calls enforce strict timeouts; the event
  websockets are closed with a bounded timeout.
- TTS uses the `generate_audio` service (`make_audio` + `is_audio_ready`).
  When it is unreachable the alarm does **not** go silent: a warning is logged
  and the preset local alert `sound:prober-alarm` (`alarm_fallback_media`) is
  played instead. That file ships with the asterisk role
  (`files/sounds/prober-alarm.wav`, installed into
  `{{ asterisk_sounds_dir }}/prober-alarm.wav`): a 0.5s alert tone followed by
  a Chinese sentence telling the responder that voice synthesis is down.
  Replace it with a recording of your own (8kHz mono PCM wav) to customise the
  fallback - it is static on purpose, so it survives a `generate_audio`
  outage. (`sound:hello` must not be used: the deployment does not install the
  Asterisk core sound packages.)
- Media detection taps the far end through an ARI `externalMedia` channel
  (`encapsulation=rtp`, `transport=udp`) pointed at a local UDP socket, joined
  to a mixing bridge with the probe channel. Every received RTP packet counts
  as one media frame; zero frames while answered ⇒ `CC_ONE_WAY_AUDIO`.
  Two deployment facts this depends on:
  - the bridge join goes through `/bridges/{id}/addChannel` (the legacy
    `/add` path answers 404 and silently leaves the tap out of the bridge);
  - the `websocket` transport is **not** supported by this Asterisk build
    (HTTP 501 "encapsulation and/or transport is not supported"), which is why
    plain RTP/UDP is used. The media Stasis app still needs a websocket
    subscriber, otherwise the externalMedia channel is hung up on entry.

## Execution model & data locations

```
Celery Beat (60s, probe_scan_interval) -> scan_probes
         |  Redis lock lock:prober_scan (TTL 45s, dedupes concurrent beats)
         |  re-reads probe_config.yml on every run  <-- edits apply within 60s
         v  cron match per probe (wall clock in PROBE_TIMEZONE)
   run_probe.apply_async(queue="probe_queue")
         v  worker -> run_probe_call (ARI dial, events, media tap, DTMF,
            recording, talk-limit hangup) -> evaluate_probe -> JSONL record
         v  on failure: dispatch_duty_alarm (same queue) -> webhook + calls
```

| Data | Location |
| --- | --- |
| Probes & duty groups | `$CALLER_CONFIG_DIR/probe_config.yml` (atomic write, validated on save) |
| Execution history | same directory, `probe_runs.jsonl` (append-only, newest 2000 kept) |
| Scan lock / task queue | Redis `lock:prober_scan`, `probe_queue` |
| Alarm call trail (Managed Calls) | PostgreSQL `Calls` table (`oncall=true`) |
| Recordings | Asterisk `/var/spool/asterisk/recording/<utc>-<rand>.wav` |
| Runtime settings (interval, tz, recording, TTS) | `$CALLER_CONFIG_DIR/settings.toml` |

**Changes and restarts**: edits to `probe_config.yml` (via the console or the
file) are picked up by the next scan - no restart. Anything in `settings.toml`
is read at process import, so changing the scan interval, timezone, recording
flags or service endpoints needs a `systemctl restart` of the worker (and of
the UI for UI-side values). An invalid `probe_config.yml` makes the scan abort
without dispatching any probe; the console validates before writing.
