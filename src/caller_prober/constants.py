"""
Constants for the caller_prober package.

All values are sourced from Dynaconf settings (src/config/settings.toml) so the
prober stays deployable through the same configuration pipeline as every other
py-phone-caller service.
"""

from py_phone_caller_utils.config import settings

ASTERISK_URL = f"{settings.commons.asterisk_http_scheme}://{settings.commons.asterisk_host}:{settings.commons.asterisk_web_port}"
ASTERISK_USER = settings.commons.asterisk_user
ASTERISK_PASS = settings.commons.asterisk_pass
ASTERISK_CHAN_TYPE = settings.asterisk_call.asterisk_chan_type
ASTERISK_CONTEXT = settings.asterisk_call.asterisk_context
ASTERISK_CALLER_ID = settings.asterisk_call.asterisk_caller_id

# Prober specifics
PROBER_SETTINGS = settings.get("caller_prober", {})
PROBE_STASIS_APP = PROBER_SETTINGS.get("probe_stasis_app", "py-phone-caller-probe")
PROBE_MEDIA_APP = PROBER_SETTINGS.get("probe_media_app", "py-phone-caller-probe-media")
ALARM_STASIS_APP = PROBER_SETTINGS.get("alarm_stasis_app", "py-phone-caller-probe-alarm")
PROBE_QUEUE_NAME = PROBER_SETTINGS.get("probe_queue", "probe_queue")
# Cron expressions are evaluated in this timezone (defaults to the platform's
# local timezone). Evaluating them in UTC shifted every schedule by the offset.
PROBE_TIMEZONE = (
    PROBER_SETTINGS.get("probe_timezone")
    or getattr(settings.scheduled_calls, "local_timezone", "UTC")
)
PROBE_SCAN_INTERVAL = float(PROBER_SETTINGS.get("probe_scan_interval", 60.0))
PROBE_MEDIA_FORMAT = PROBER_SETTINGS.get("probe_media_format", "ulaw")
PROBE_SCAN_LOCK_KEY = PROBER_SETTINGS.get("probe_scan_lock_key", "lock:prober_scan")
PROBE_SCAN_LOCK_TTL = int(PROBER_SETTINGS.get("probe_scan_lock_ttl", 45))
WEBHOOK_TIMEOUT_TOTAL = float(PROBER_SETTINGS.get("webhook_timeout_total", 10.0))
ARI_HTTP_TIMEOUT_TOTAL = float(PROBER_SETTINGS.get("ari_http_timeout_total", 10.0))
ARI_WS_CLOSE_TIMEOUT = float(PROBER_SETTINGS.get("ari_ws_close_timeout", 5.0))
ALARM_ACK_DIGIT = PROBER_SETTINGS.get("alarm_ack_digit", "1")
# Language passed to the generate-audio service for the alarm announcement.
# The deployment default ("z" = Chinese) makes the paging message speakable.
ALARM_TTS_LANG = PROBER_SETTINGS.get("alarm_tts_lang", "z")
# Neural TTS synthesis is synchronous inside the generate-audio request, so it
# needs a far wider budget than a plain ARI call.
ALARM_TTS_TIMEOUT_TOTAL = float(PROBER_SETTINGS.get("alarm_tts_timeout_total", 90.0))
# Dialplan context that dials the probed destination through the SIP trunk.
# The probe engine originates Local/<number>@<context> into the Stasis app so
# the channel is ARI-controllable (DTMF, media tap, hangup cause).
PROBE_DIAL_CONTEXT = PROBER_SETTINGS.get("probe_dial_context", "py-phone-caller-probe")
# Talk-time cap applied after the answer: IVR queues and hotlines never hang
# up on their own, so the probe terminates the call itself once the target has
# been observed long enough (per-probe override: max_call_seconds).
PROBE_MAX_CALL_SECONDS = float(PROBER_SETTINGS.get("probe_max_call_seconds", 20.0))
# Call recording: every probe call is recorded (both directions) so operators
# can listen to what the far end actually played.
PROBE_RECORDING_ENABLED = bool(PROBER_SETTINGS.get("probe_recording_enabled", True))
PROBE_RECORDING_FORMAT = PROBER_SETTINGS.get("probe_recording_format", "wav")
# Recordings older than this are purged from Asterisk on the next probe run.
PROBE_RECORDING_RETENTION_DAYS = int(PROBER_SETTINGS.get("probe_recording_retention_days", 7))
PROBE_RECORDS_FILE = PROBER_SETTINGS.get("probe_records_file", "probe_runs.jsonl")
PROBE_RECORDS_LIMIT = int(PROBER_SETTINGS.get("probe_records_limit", 2000))

# Probe evaluation thresholds (spec)
CALLCENTER_EARLY_HANGUP_SECONDS = 3.0
CALLCENTER_DTMF_SEND_DELAY_SECONDS = 2.0

# Hangup cause -> failure code mapping (Asterisk cause codes)
HANGUP_CAUSE_MAP = {
    16: None,  # NORMAL_CLEARING (not a failure by itself)
    17: "SIP_BUSY",
    19: "NO_ANSWER",
    21: "CALL_REJECTED",
    34: "SIP_CONGESTION",
    38: "SIP_TEMPORARILY_UNAVAILABLE",
    41: "SIP_TEMPORARILY_UNAVAILABLE",
    42: "SIP_TEMPORARILY_UNAVAILABLE",
    44: "NO_ANSWER",
    102: "SIP_RESOLUTION_FAILURE",
    111: "ARI_PROTOCOL_ERROR",
    127: "SIP_NOT_FOUND",
    503: "SIP_SERVICE_UNAVAILABLE",
}

# HTTP-style failures observed on ARI originate (Asterisk returns 4xx/5xx bodies)
ORIGINATE_HTTP_FAILURE_MAP = {
    400: "ARI_PROTOCOL_ERROR",
    404: "SIP_NOT_FOUND",
    409: "SIP_BUSY",
    500: "SIP_SERVICE_UNAVAILABLE",
    503: "SIP_SERVICE_UNAVAILABLE",
}

# Static local alert played when generate-audio is unavailable. The asterisk
# role installs files/sounds/prober-alarm.wav next to the core sounds, so a TTS
# outage degrades to a spoken fallback instead of a silent call.
ALARM_FALLBACK_MEDIA = PROBER_SETTINGS.get("alarm_fallback_media", "sound:prober-alarm")

GENERATE_AUDIO_URL = f"{settings.generate_audio.generate_audio_http_scheme}://{settings.generate_audio.generate_audio_host}:{settings.generate_audio.generate_audio_port}"
GENERATE_AUDIO_ROUTE = settings.generate_audio.get(
    "generate_audio_app_route", "make_audio"
)
GENERATE_AUDIO_READY_ROUTE = settings.generate_audio.get(
    "generate_audio_ready_app_route", "is_audio_ready"
)
# Folder where the generate-audio service serves rendered wav files; the ARI
# player fetches the alarm announcement straight from this HTTP endpoint.
GENERATE_AUDIO_SERVING_FOLDER = settings.generate_audio.get(
    "serving_audio_folder", "audio"
)

LOG_FORMATTER = settings.logs.log_formatter
LOG_LEVEL = settings.logs.log_level
