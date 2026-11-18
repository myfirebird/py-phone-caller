"""
Async ARI operations for the caller_prober package.

Two engines live here:

* :func:`run_probe_call` - originates the probe number into the Stasis probe
  app, listens on the ARI event websocket for answer / hangup / DTMF events,
  feeds media frames through an externalMedia channel joined to the probe
  channel (transport=websocket delivers RTP as binary websocket frames), and
  sends the configured DTMF preset two seconds after answer.
* :class:`AlarmCaller` - originates duty-group alarm calls, plays TTS audio
  (generate-audio service, with an Asterisk built-in sound fallback) and waits
  for a DTMF ACK from the callee.

Every network operation is bounded by a strict timeout (constants:
ARI_HTTP_TIMEOUT_TOTAL / ARI_WS_CLOSE_TIMEOUT / WEBHOOK_TIMEOUT_TOTAL).
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from base64 import b64encode
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import aiohttp
import websockets

from caller_prober.constants import (
    ALARM_ACK_DIGIT,
    ALARM_FALLBACK_MEDIA,
    ALARM_STASIS_APP,
    ALARM_TTS_LANG,
    ALARM_TTS_TIMEOUT_TOTAL,
    ARI_HTTP_TIMEOUT_TOTAL,
    ARI_WS_CLOSE_TIMEOUT,
    ASTERISK_CALLER_ID,
    ASTERISK_CHAN_TYPE,
    ASTERISK_CONTEXT,
    ASTERISK_PASS,
    ASTERISK_URL,
    ASTERISK_USER,
    CALLCENTER_DTMF_SEND_DELAY_SECONDS,
    GENERATE_AUDIO_READY_ROUTE,
    GENERATE_AUDIO_ROUTE,
    GENERATE_AUDIO_SERVING_FOLDER,
    GENERATE_AUDIO_URL,
    PROBE_DIAL_CONTEXT,
    PROBE_MAX_CALL_SECONDS,
    PROBE_MEDIA_APP,
    PROBE_MEDIA_FORMAT,
    PROBE_RECORDING_ENABLED,
    PROBE_RECORDING_FORMAT,
    PROBE_RECORDING_RETENTION_DAYS,
    PROBE_STASIS_APP,
    WEBHOOK_TIMEOUT_TOTAL,
)
from caller_prober.exceptions import AriProbeError
from caller_prober.services.probe_evaluator import ProbeOutcome

logger = logging.getLogger(__name__)


def _auth_header() -> str:
    """Returns the HTTP Basic Authorization header value for ARI."""
    token = str(b64encode(bytearray(f"{ASTERISK_USER}:{ASTERISK_PASS}", "utf8")), "utf-8")
    return f"Basic {token}"


def _ari_url(path: str, params: dict[str, str] | None = None) -> str:
    """Builds a full ARI HTTP URL with encoded query parameters."""
    url = f"{ASTERISK_URL}/ari/{path.lstrip('/')}"
    if params:
        qs = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
        url = f"{url}?{qs}"
    return url


def _events_ws_url(app: str) -> str:
    """Builds the ARI event websocket URL for the given Stasis application."""
    base = ASTERISK_URL.split("://", 1)
    scheme = "ws" if base[0] == "http" else "wss"
    token = quote(f"{ASTERISK_USER}:{ASTERISK_PASS}", safe="")
    return f"{scheme}://{base[1]}/ari/events?app={quote(app)}&api_key={token}"

def format_asterisk_endpoint(phone: str) -> str:
    """Formats destination phone number into an ARI dialable endpoint string."""
    if "{phone}" in ASTERISK_CHAN_TYPE:
        return ASTERISK_CHAN_TYPE.replace("{phone}", phone)
    if ASTERISK_CHAN_TYPE.startswith("PJSIP") and "/" in ASTERISK_CHAN_TYPE:
        tech, trunk = ASTERISK_CHAN_TYPE.split("/", 1)
        from py_phone_caller_utils.config import settings

        sip_host = settings.get("pbx_sip_provider_host") or "172.28.17.225"
        sip_port = settings.get("pbx_sip_provider_port") or 5060
        return f"{tech}/{trunk}/sip:{phone}@{sip_host}:{sip_port}"
    return f"{ASTERISK_CHAN_TYPE}/{phone}"


async def _get_json(session: aiohttp.ClientSession, url: str) -> tuple[int, dict[str, Any]]:
    """GETs an ARI resource with a strict timeout; returns (status, body)."""
    try:
        async with session.get(
            url,
            headers={"Authorization": _auth_header()},
            timeout=aiohttp.ClientTimeout(total=ARI_HTTP_TIMEOUT_TOTAL),
        ) as resp:
            body: dict[str, Any] = {}
            with suppress(Exception):
                body = await resp.json(content_type=None)
            return resp.status, body
    except (TimeoutError, aiohttp.ClientError) as err:
        raise AriProbeError(f"ARI request failed ({url}): {err}") from err


async def _post_json(session: aiohttp.ClientSession, url: str) -> tuple[int, dict[str, Any]]:
    """POSTs to an ARI endpoint with a strict timeout; returns (status, body)."""
    try:
        async with session.post(
            url,
            headers={"Authorization": _auth_header()},
            timeout=aiohttp.ClientTimeout(total=ARI_HTTP_TIMEOUT_TOTAL),
        ) as resp:
            body: dict[str, Any] = {}
            with suppress(Exception):
                body = await resp.json(content_type=None)
            return resp.status, body
    except (TimeoutError, aiohttp.ClientError) as err:
        raise AriProbeError(f"ARI request failed ({url}): {err}") from err


@dataclass(slots=True)
class _EventState:
    """Mutable observation state shared by the event websocket consumer."""

    answered: bool = False
    stasis_started: bool = False
    app_engaged: bool = False
    answered_at: float | None = None
    hangup_cause: int | None = None
    hangup_cause_txt: str | None = None
    media_frames: int = 0
    last_dtmf: str | None = None
    playbacks_finished: int = 0
    last_playback_state: str = ""
    channel_alive: bool = True
    done: bool = False


async def _consume_events(
    ws: websockets.WebSocketClientConnection,
    state: _EventState,
    channel_id: str,
    ack_digit: str | None = None,
    destination: str = "",
) -> None:
    """
    Consumes ARI events (and binary RTP frames) until the channel tree is gone.

    A probe runs through a Local channel pair: the ``;1`` leg holds Stasis
    control while the ``;2`` leg dials the destination. Both legs describe the
    same destination number, so events are attributed by tracking every channel
    seen dialling the destination.
    """
    tracked: set[str] = {channel_id}
    dest = str(destination or "").strip()
    loop = asyncio.get_running_loop()

    def _belongs(chan_obj: dict[str, Any]) -> bool:
        cid = chan_obj.get("id")
        if cid in tracked:
            return True
        if not dest:
            return False
        numbers = {
            str((chan_obj.get("connected") or {}).get("number") or ""),
            str((chan_obj.get("caller") or {}).get("number") or ""),
        }
        if dest in numbers:
            if cid:
                tracked.add(cid)
            return True
        return False

    async for raw in ws:
        if isinstance(raw, (bytes, bytearray)):
            state.media_frames += 1
            continue
        try:
            event = json.loads(raw)
        except (ValueError, TypeError):
            continue

        etype = event.get("type")
        chan_obj = event.get("channel") or {}

        if etype == "PlaybackFinished":
            # Playback events carry a `playback` object instead of a channel.
            playback = event.get("playback") or {}
            target = str(playback.get("target_uri") or "")
            if channel_id in target or any(tracked_id in target for tracked_id in tracked):
                state.playbacks_finished += 1
                state.last_playback_state = str(playback.get("state") or "")
            continue

        if etype == "Dial":
            dialstring = str(event.get("dialstring") or "")
            peer_id = (event.get("peer") or {}).get("id")
            if peer_id and (
                peer_id in tracked or (dest and dest in dialstring) or _belongs(chan_obj)
            ):
                tracked.add(peer_id)
            continue

        if not chan_obj or not _belongs(chan_obj):
            continue

        state.app_engaged = True

        if etype == "StasisStart":
            state.stasis_started = True
            state.channel_alive = True
        elif etype in ("ChannelAnswer", "ChannelStateChange"):
            # ChannelStateChange keeps the lifecycle state inside `channel`
            # ("Ring"/"Up"/...), never at the event top level.
            chan_state = event.get("state") or chan_obj.get("state")
            if chan_state == "Up":
                if not state.answered:
                    state.answered = True
                    state.answered_at = loop.time()
                state.channel_alive = True
        elif etype == "ChannelDtmfReceived":
            state.last_dtmf = event.get("digit")
        elif etype in ("ChannelHangupRequest", "ChannelDestroyed"):
            state.channel_alive = False
            if "cause" in event:
                state.hangup_cause = event.get("cause")
                state.hangup_cause_txt = event.get("cause_txt")
            if etype == "ChannelDestroyed" and chan_obj.get("id") == channel_id:
                state.done = True
        if state.done:
            break


async def _send_dtmf(session: aiohttp.ClientSession, channel_id: str, digits: str) -> bool:
    """Sends DTMF digits to a live channel; returns True when ARI accepted."""
    url = _ari_url(
        f"channels/{channel_id}/dtmf",
        {"dtmf": digits, "duration": "100", "between": "200"},
    )
    try:
        status, _ = await _post_json(session, url)
        return status < 300
    except AriProbeError as err:
        logger.warning("DTMF send failed on channel %s: %s", channel_id, err)
        return False


async def _delete(
    session: aiohttp.ClientSession, url: str
) -> tuple[int, dict[str, Any]]:
    """DELETEs an ARI resource (hang up a channel / destroy a bridge).

    ARI requires DELETE for teardown; POSTing to the resource URL answers 404
    and leaves the channel live, which leaked every probe call.
    """
    try:
        async with session.delete(
            url,
            headers={"Authorization": _auth_header()},
            timeout=aiohttp.ClientTimeout(total=ARI_HTTP_TIMEOUT_TOTAL),
        ) as resp:
            body: dict[str, Any] = {}
            with suppress(Exception):
                body = await resp.json(content_type=None)
            return resp.status, body
    except (TimeoutError, aiohttp.ClientError) as err:
        raise AriProbeError(f"ARI request failed ({url}): {err}") from err


async def _bridge_add(
    session: aiohttp.ClientSession, bridge_id: str, channel_id: str
) -> tuple[int, dict[str, Any]]:
    """Joins a channel to a bridge.

    Asterisk >= 16 exposes ``/bridges/{id}/addChannel``; older builds use
    ``/add``. Requesting the wrong one answers 404, which silently kept the
    media tap out of the bridge and produced false single-way-audio verdicts.
    """
    status, body = await _post_json(
        session, _ari_url(f"bridges/{bridge_id}/addChannel", {"channel": channel_id})
    )
    if status == 404:
        status, body = await _post_json(
            session, _ari_url(f"bridges/{bridge_id}/add", {"channel": channel_id})
        )
    return status, body


async def _start_media_tap(
    session: aiohttp.ClientSession, channel_id: str
) -> tuple[str | None, asyncio.Task[None] | None, str | None]:
    """
    Taps the far end's audio through an externalMedia channel into a local UDP
    socket, and bridges that channel with the probe channel.

    UDP/RTP is used because this Asterisk build rejects the websocket
    transport for externalMedia (HTTP 501 "encapsulation and/or transport is
    not supported"); plain RTP needs no special build options. Each received
    packet counts as one media frame, which is exactly the evidence the
    evaluator needs to rule out single-way audio.

    Returns (bridge_id, media_task, media_channel_id); (None, None, None) on
    failure - media absence is then reported by the evaluator as ONE_WAY_AUDIO
    only when no DTMF/audio evidence exists either.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        sock.setblocking(False)
    except OSError as err:
        logger.warning("Probe media socket unavailable: %s", err)
        return None, None, None
    listen_port = sock.getsockname()[1]

    # The externalMedia channel enters PROBE_MEDIA_APP, and Stasis hangs up any
    # channel whose application has no subscriber - keep one attached. RTP
    # itself flows to the UDP socket, not to this websocket.
    try:
        media_ws = await asyncio.wait_for(
            websockets.connect(_events_ws_url(PROBE_MEDIA_APP)),
            timeout=ARI_HTTP_TIMEOUT_TOTAL,
        )
    except Exception as err:  # noqa: BLE001 - media tap is best-effort
        logger.warning("Probe media websocket unavailable: %s", err)
        with suppress(OSError):
            sock.close()
        return None, None, None

    async def _keep_app_alive() -> None:
        try:
            async for _ in media_ws:
                continue
        except websockets.exceptions.WebSocketException:
            pass

    async def _drain_rtp() -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                payload = await loop.sock_recv(sock, 4096)
                if len(payload) > RTP_HEADER_BYTES:
                    MEDIA_FRAME_COUNTER[0] += 1
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            with suppress(OSError):
                sock.close()

    async def _media_session() -> None:
        try:
            await asyncio.gather(_keep_app_alive(), _drain_rtp())
        finally:
            with suppress(Exception):
                await media_ws.close()
            with suppress(OSError):
                sock.close()

    media_task = asyncio.create_task(_media_session())

    status, bridge_body = await _post_json(session, _ari_url("bridges", {"type": "mixing"}))
    if status >= 300 or "id" not in bridge_body:
        media_task.cancel()
        logger.warning("Probe media bridge creation failed: HTTP %s", status)
        return None, None, None
    bridge_id = bridge_body["id"]

    status, media_body = await _post_json(
        session,
        _ari_url(
            "channels/externalMedia",
            {
                "app": PROBE_MEDIA_APP,
                "external_host": f"127.0.0.1:{listen_port}",
                "format": PROBE_MEDIA_FORMAT,
                "encapsulation": "rtp",
                "transport": "udp",
                "direction": "both",
            },
        ),
    )
    media_channel_id = media_body.get("id") if isinstance(media_body, dict) else None
    if status >= 300 or not media_channel_id:
        logger.warning(
            "Probe externalMedia channel creation failed: HTTP %s (%s)", status, media_body
        )
        media_task.cancel()
        with suppress(AriProbeError):
            await _delete(session, _ari_url(f"bridges/{bridge_id}"))
        return None, None, None

    ok1, body1 = await _bridge_add(session, bridge_id, channel_id)
    ok2, body2 = await _bridge_add(session, bridge_id, media_channel_id)
    if ok1 >= 300 or ok2 >= 300:
        logger.warning(
            "Probe media bridge join failed: probe=%s (%s) media=%s (%s)",
            ok1,
            body1,
            ok2,
            body2,
        )
        media_task.cancel()
        return bridge_id, None, media_channel_id

    logger.info(
        "Probe media tap active: bridge=%s media_channel=%s rtp_port=%s",
        bridge_id,
        media_channel_id,
        listen_port,
    )
    return bridge_id, media_task, media_channel_id


# RTP packets shorter than this carry no payload (12-byte header + padding).
RTP_HEADER_BYTES = 12

# Shared frame counter for the media tap (a probe run resets it before dial).
MEDIA_FRAME_COUNTER = [0]


async def _cleanup(
    session: aiohttp.ClientSession,
    channel_id: str | None,
    bridge_id: str | None,
    media_channel_id: str | None,
) -> None:
    """Hangs up leftover probe resources; every step is best-effort."""
    for path in filter(
        None,
        [
            f"channels/{channel_id}" if channel_id else None,
            f"bridges/{bridge_id}" if bridge_id else None,
            f"channels/{media_channel_id}" if media_channel_id else None,
        ],
    ):
        with suppress(AriProbeError):
            await _delete(session, _ari_url(path))


async def _start_recording(
    session: aiohttp.ClientSession,
    target: str,
    target_id: str,
    max_seconds: float,
) -> str | None:
    """
    Starts a full-duplex recording of the probe call.

    Asterisk refuses to record a channel that sits in a bridge, which is
    exactly what the media tap does, so callcenter probes record the mixing
    bridge instead of the channel.

    Args:
        session: Shared aiohttp session.
        target: ``channels`` or ``bridges``.
        target_id: Channel/bridge id.
        max_seconds: Safety cap for the recording length.

    Returns:
        str | None: Recording name (used for playback), or None on failure.
    """
    if not PROBE_RECORDING_ENABLED:
        return None
    from datetime import UTC, datetime
    from uuid import uuid4

    name = f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:6]}"
    params = {
        "name": name,
        "format": PROBE_RECORDING_FORMAT,
        "ifExists": "overwrite",
        "maxDurationSeconds": str(max(5, int(max_seconds) + 10)),
        "maxSilenceSeconds": "0",
        "beep": "false",
        "terminateOn": "none",
    }
    if target == "channels":
        params["direction"] = "both"
    status, body = await _post_json(
        session, _ari_url(f"{target}/{target_id}/record", params)
    )
    if status >= 300:
        logger.warning(
            "Probe recording could not start on %s/%s: HTTP %s (%s)",
            target,
            target_id,
            status,
            body,
        )
        return None
    logger.info("Probe recording started on %s/%s as %s", target, target_id, name)
    return name


async def _prune_recordings(session: aiohttp.ClientSession) -> None:
    """Deletes stored probe recordings older than the retention window."""
    if PROBE_RECORDING_RETENTION_DAYS <= 0:
        return
    from datetime import UTC, datetime, timedelta

    status, body = await _get_json(session, _ari_url("recordings/stored"))
    if status >= 300 or not isinstance(body, list):
        return
    cutoff = datetime.now(UTC) - timedelta(days=PROBE_RECORDING_RETENTION_DAYS)
    for item in body:
        name = str((item or {}).get("name") or "")
        stamp = name.split("-", 1)[0]
        try:
            recorded_at = datetime.strptime(stamp, "%Y%m%dT%H%M%S").replace(tzinfo=UTC)
        except ValueError:
            continue
        if recorded_at >= cutoff:
            continue
        with suppress(AriProbeError):
            await _delete(session, _ari_url(f"recordings/stored/{name}"))
        logger.info("Purged expired probe recording %s", name)


async def run_probe_call(
    phone: str,
    probe_type: str,
    timeout: float,
    dtmf_preset: str | None,
    max_call_seconds: float | None = None,
) -> ProbeOutcome:
    """
    Executes one probe call end-to-end and returns the raw observed facts.

    Args:
        phone: Destination number (E.164 or dialplan-local string).
        probe_type: "direct" or "callcenter".
        timeout: Overall probe timeout in seconds.
        dtmf_preset: DTMF digits to send 2s after answer (callcenter only).
        max_call_seconds: Talk time allowed after the answer. The probe hangs
            the channel up itself when it elapses, because IVR queues and
            hotlines never hang up on their own. Defaults to
            PROBE_MAX_CALL_SECONDS.

    Returns:
        ProbeOutcome: Facts consumed by services.probe_evaluator.
    """
    talk_cap = float(max_call_seconds or PROBE_MAX_CALL_SECONDS)
    state = _EventState()
    MEDIA_FRAME_COUNTER[0] = 0
    channel_id: str | None = None
    bridge_id: str | None = None
    media_channel_id: str | None = None
    media_task: asyncio.Task[None] | None = None
    state_dtmf = False
    alive_after_dtmf = True
    hung_up_by_probe = False
    recording_name: str | None = None
    loop = asyncio.get_running_loop()

    originate_params = {
        "endpoint": f"Local/{phone}@{PROBE_DIAL_CONTEXT}",
        "callerId": ASTERISK_CALLER_ID,
        "app": PROBE_STASIS_APP,
        "appArgs": f"probe,{phone},{probe_type}",
        "timeout": "30",
    }

    timeout_mgr = asyncio.timeout(timeout)
    try:
        async with timeout_mgr, aiohttp.ClientSession() as session:
            with suppress(AriProbeError):
                await _prune_recordings(session)
            # Subscribe before dialling: the Local leg enters Stasis within
            # milliseconds, and a channel entering Stasis with no subscriber is
            # rejected outright - the probe then times out without a call.
            events_ws = await asyncio.wait_for(
                websockets.connect(_events_ws_url(PROBE_STASIS_APP)),
                timeout=ARI_HTTP_TIMEOUT_TOTAL,
            )
            consumer: asyncio.Task[None] | None = None
            status, body = await _post_json(
                session, _ari_url("channels", originate_params)
            )
            channel_id = body.get("id") if isinstance(body, dict) else None

            if status >= 300 or not channel_id:
                # Fall back to a direct PJSIP originate (no Stasis control).
                logger.warning(
                    "Local probe originate for %s failed (HTTP %s), "
                    "retrying direct PJSIP endpoint",
                    phone,
                    status,
                )
                originate_params = {
                    "endpoint": format_asterisk_endpoint(phone),
                    "context": ASTERISK_CONTEXT,
                    "callerId": ASTERISK_CALLER_ID,
                    "app": PROBE_STASIS_APP,
                    "appArgs": f"probe,{phone},{probe_type}",
                    "timeout": "30",
                }
                status, body = await _post_json(
                    session, _ari_url("channels", originate_params)
                )
                channel_id = body.get("id") if isinstance(body, dict) else None

            if status >= 300 or not channel_id:
                logger.warning(
                    "Probe originate to %s failed: HTTP %s (%s)", phone, status, body
                )
                with suppress(Exception):
                    await events_ws.close()
                return ProbeOutcome(
                    probe_type=probe_type,
                    answered=False,
                    http_status=status,
                )

            consumer = asyncio.create_task(
                _consume_events(events_ws, state, channel_id, destination=phone)
            )
            try:
                if probe_type == "callcenter":
                    # Wait for StasisStart before tapping media.
                    await _wait_state(lambda: state.stasis_started, timeout)
                    bridge_id, media_task, media_channel_id = await _start_media_tap(
                        session, channel_id
                    )
                    if dtmf_preset:
                        await asyncio.sleep(CALLCENTER_DTMF_SEND_DELAY_SECONDS)
                        state_dtmf = await _send_dtmf(session, channel_id, dtmf_preset)
                        if state_dtmf:
                            await asyncio.sleep(1.0)  # let the far end react
                    else:
                        state_dtmf = False
                    alive_after_dtmf = state.channel_alive
                else:
                    state_dtmf = False
                    alive_after_dtmf = True
                    await _wait_state(lambda: state.done or state.answered, timeout)

                if not state.done and (state.answered or state.stasis_started):
                    # Bridged channels cannot be recorded directly: record the
                    # mixing bridge that carries the probe audio instead.
                    with suppress(AriProbeError):
                        if bridge_id:
                            recording_name = await _start_recording(
                                session, "bridges", bridge_id, talk_cap
                            )
                        else:
                            recording_name = await _start_recording(
                                session, "channels", channel_id, talk_cap
                            )
                await _wait_state(
                    lambda: state.done, _talk_budget(state, loop, talk_cap, timeout)
                )
                if not state.done and state.answered:
                    # Bounded talk time: hang up instead of leaking a live call.
                    hung_up_by_probe = True
                    logger.info(
                        "Probe to %s reached the %.1fs talk limit, hanging up",
                        phone,
                        talk_cap,
                    )
                    with suppress(AriProbeError):
                        await _delete(session, _ari_url(f"channels/{channel_id}"))
                    await _wait_state(lambda: state.done, 3.0)
            finally:
                if consumer:
                    consumer.cancel()
                with suppress(Exception):
                    await asyncio.wait_for(
                        events_ws.close(), timeout=ARI_WS_CLOSE_TIMEOUT
                    )
                if media_task:
                    media_task.cancel()
                await _cleanup(session, channel_id, bridge_id, media_channel_id)
    except TimeoutError:
        logger.info("Probe to %s hit overall timeout of %ss", phone, timeout)
        if state.answered and not state.done:
            hung_up_by_probe = True
        return _finalize(
            state,
            probe_type,
            state_dtmf,
            timed_out=True,
            hung_up_by_probe=hung_up_by_probe,
            recording_name=recording_name,
        )
    except AriProbeError as err:
        logger.error("Probe to %s failed at transport level: %s", phone, err)
        return ProbeOutcome(probe_type=probe_type, http_status=None)

    return _finalize(
        state,
        probe_type,
        state_dtmf,
        alive_after_dtmf,
        hung_up_by_probe=hung_up_by_probe,
        recording_name=recording_name,
    )


def remaining(timeout: float, state: _EventState, loop: asyncio.AbstractEventLoop) -> float:
    """Seconds left in the probe budget based on the answer timestamp."""
    if state.answered_at is None:
        return max(1.0, timeout / 2)
    left = timeout - (loop.time() - state.answered_at)
    return max(1.0, min(left, 30.0))


def _talk_budget(
    state: _EventState,
    loop: asyncio.AbstractEventLoop,
    talk_cap: float,
    timeout: float,
) -> float:
    """Seconds to wait before the probe hangs the call up itself."""
    if state.answered_at is None:
        return max(1.0, min(talk_cap, timeout / 2))
    elapsed = loop.time() - state.answered_at
    return max(1.0, min(talk_cap - elapsed, timeout - elapsed, 30.0))


async def _wait_state(pred, budget: float) -> None:
    """Polls a state predicate until true or the budget expires."""
    deadline = asyncio.get_running_loop().time() + budget
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.25)


def _finalize(
    state: _EventState,
    probe_type: str,
    dtmf_sent: bool,
    alive_after_dtmf: bool = True,
    timed_out: bool = False,
    hung_up_by_probe: bool = False,
    recording_name: str | None = None,
) -> ProbeOutcome:
    """Builds the ProbeOutcome from the observed websocket state."""
    loop = asyncio.get_running_loop()
    connected = 0.0
    if state.answered_at is not None:
        end = loop.time()
        connected = max(0.0, end - state.answered_at)
    media = MEDIA_FRAME_COUNTER[0] if state.answered else 0
    return ProbeOutcome(
        probe_type=probe_type,
        answered=state.answered,
        entered_stasis=state.stasis_started or state.app_engaged,
        connected_seconds=connected,
        hangup_cause=state.hangup_cause,
        hangup_cause_txt=state.hangup_cause_txt,
        dtmf_digits_sent=dtmf_sent,
        channel_alive_after_dtmf=alive_after_dtmf,
        media_frames_observed=media,
        timed_out=timed_out,
        hung_up_by_probe=hung_up_by_probe,
        recording_name=recording_name,
    )


class AlarmCaller:
    """
    Places duty-group alarm calls and waits for a DTMF ACK.

    One instance per alarm fan-out; sequential per-member calls are driven by
    tasks/probe_tasks.dispatch_duty_alarm. Every attempt is appended to
    ``call_details`` so the dispatch task can persist the exact alarm trail
    (who was called, when, whether they answered, who acknowledged).
    """

    def __init__(self) -> None:
        """Creates a caller with an empty per-dispatch call trail."""
        self.call_details: list[dict[str, Any]] = []

    async def test_webhook(self, url: str, payload: dict[str, Any]) -> tuple[bool, int, str]:
        """Tests webhook connectivity and returns status code and detail."""
        try:
            async with aiohttp.ClientSession() as session, session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=WEBHOOK_TIMEOUT_TOTAL),
            ) as resp:
                logger.info("Test webhook %s responded HTTP %s", url, resp.status)
                ok = resp.status < 300
                return ok, resp.status, "OK" if ok else f"HTTP {resp.status}"
        except TimeoutError:
            logger.error("Test webhook %s timed out after %ss", url, WEBHOOK_TIMEOUT_TOTAL)
            return False, 504, f"请求超时 ({WEBHOOK_TIMEOUT_TOTAL}s)"
        except aiohttp.ClientConnectorError as err:
            logger.error("Test webhook %s connection failed: %s", url, err)
            return False, 502, f"连接目标失败: {err}"
        except aiohttp.ClientError as err:
            logger.error("Test webhook %s failed: %s", url, err)
            return False, 502, f"网络请求异常: {err}"

    async def post_webhook(self, url: str, payload: dict[str, Any]) -> bool:
        """POSTs the JSON alarm summary to the duty group webhook."""
        ok, _, _ = await self.test_webhook(url, payload)
        return ok

    async def generate_tts_media(self, text: str) -> str | None:
        """
        Synthesizes the alarm text through the generate-audio service.

        The rendered wav is fetched by Asterisk over HTTP from the service's
        audio endpoint, so the returned ARI media locator is the full URL
        (``recording:`` locators only resolve inside Asterisk's own sounds
        directory and would never find a freshly generated file).

        Args:
            text: Chinese announcement text.

        Returns:
            str | None: ARI media URI (``sound:http://...``) for playback, or
            None to fall back to the built-in Asterisk sound.
        """
        from hashlib import sha256

        checksum = sha256(text.encode("utf-8")).hexdigest()[:10]
        file_name = f"{checksum}_{ALARM_TTS_LANG}.wav"
        # res_stasis_playback only understands URI schemes; a bare http URL is
        # rejected, while the sound: prefix routes it through the HTTP media
        # cache (which downloads and caches the wav).
        media_uri = (
            f"sound:{GENERATE_AUDIO_URL}/{GENERATE_AUDIO_SERVING_FOLDER}/{file_name}"
        )
        try:
            async with aiohttp.ClientSession() as session:
                params = {"message": text, "msg_chk_sum": checksum, "lang": ALARM_TTS_LANG}
                async with session.post(
                    f"{GENERATE_AUDIO_URL}/{GENERATE_AUDIO_ROUTE.lstrip('/')}",
                    json=params,
                    timeout=aiohttp.ClientTimeout(total=ALARM_TTS_TIMEOUT_TOTAL),
                ) as resp:
                    if resp.status >= 300:
                        logger.warning(
                            "generate-audio rejected alarm TTS (%s): HTTP %s",
                            checksum,
                            resp.status,
                        )
                        return None
                    body = await resp.json(content_type=None)
                if isinstance(body, dict) and body.get("status", 200) >= 300:
                    logger.warning("generate-audio failed alarm TTS for %s", checksum)
                    return None
                # Poll until the wav file is materialised (bounded retries).
                for _ in range(12):
                    async with session.get(
                        f"{GENERATE_AUDIO_URL}/{GENERATE_AUDIO_READY_ROUTE.lstrip('/')}",
                        params={"msg_chk_sum": checksum, "lang": ALARM_TTS_LANG},
                        timeout=aiohttp.ClientTimeout(total=ARI_HTTP_TIMEOUT_TOTAL),
                    ) as ready_resp:
                        if ready_resp.status >= 300:
                            return None
                        ready_body = await ready_resp.json(content_type=None)
                    if isinstance(ready_body, dict) and (
                        ready_body.get("ready") or ready_body.get("exists")
                    ):
                        logger.info("Alarm TTS ready: %s", media_uri)
                        return media_uri
                    await asyncio.sleep(2.0)
                logger.warning("Alarm TTS audio never became ready for %s", checksum)
                return None
        except (TimeoutError, aiohttp.ClientError) as err:
            logger.warning("generate-audio unavailable for alarm TTS: %s", err)
            return None

    async def call_member_for_ack(
        self,
        phone: str,
        message: str,
        tts_media: str | None,
        wait_seconds: float,
    ) -> bool:
        """
        Places one alarm call and waits for the ACK digit.

        Args:
            phone: Duty member phone number.
            message: Alarm text (already synthesized to tts_media when available).
            tts_media: ARI media locator for the TTS audio, or None for fallback.
            wait_seconds: Group escalation timeout - how long to wait for ACK.

        Returns:
            bool: True when the member pressed the ACK digit.
        """
        state = _EventState()
        channel_id: str | None = None
        loop = asyncio.get_running_loop()
        started_wall = datetime.now(UTC)
        start_loop = loop.time()
        detail: dict[str, Any] = {
            "phone": phone,
            "channel_id": None,
            "started_at": started_wall.isoformat(),
            "answered": False,
            "answered_at": None,
            "acked": False,
            "acked_at": None,
            "status": "no_answer",
        }

        def _wall(offset: float | None) -> str | None:
            """Converts a loop-clock timestamp to wall time."""
            if offset is None:
                return None
            return (started_wall + timedelta(seconds=offset - start_loop)).isoformat()

        try:
            async with asyncio.timeout(wait_seconds + 20):
                # The event websocket must exist BEFORE the originate: a trunk
                # that answers immediately fires ChannelStateChange(Up) and
                # StasisStart within milliseconds, and those events are gone
                # forever if nobody is listening yet - which made the alarm
                # wait out the whole window in silence (no announcement, no
                # fallback). Messages arriving before the consumer starts are
                # buffered by the websocket, so subscribing first loses nothing.
                events_ws = await asyncio.wait_for(
                    websockets.connect(_events_ws_url(ALARM_STASIS_APP)),
                    timeout=ARI_HTTP_TIMEOUT_TOTAL,
                )
                consumer: asyncio.Task[None] | None = None
                try:
                    async with aiohttp.ClientSession() as session:
                        status, body = await _post_json(
                            session,
                            _ari_url(
                                "channels",
                                {
                                    "endpoint": format_asterisk_endpoint(phone),
                                    "context": ASTERISK_CONTEXT,
                                    "callerId": ASTERISK_CALLER_ID,
                                    "app": ALARM_STASIS_APP,
                                    "appArgs": f"alarm,{phone}",
                                    "timeout": "30",
                                },
                            ),
                        )
                        if status >= 300:
                            logger.warning(
                                "Alarm call to %s rejected: HTTP %s", phone, status
                            )
                            return False
                        channel_id = body.get("id") if isinstance(body, dict) else None
                        if not channel_id:
                            return False

                        consumer = asyncio.create_task(
                            _consume_events(events_ws, state, channel_id, ALARM_ACK_DIGIT)
                        )
                        detail["channel_id"] = channel_id
                        await self._sync_channel_state(session, state, channel_id)

                        acked = await self._wait_ack(
                            session, state, channel_id, tts_media, wait_seconds, loop
                        )
                        detail["answered"] = state.answered or state.stasis_started
                        if state.answered_at is not None:
                            detail["answered_at"] = _wall(state.answered_at)
                        detail["acked"] = acked
                        if acked:
                            detail["acked_at"] = datetime.now(UTC).isoformat()
                        detail["status"] = (
                            "acked"
                            if acked
                            else ("answered_no_ack" if detail["answered"] else "no_answer")
                        )
                        return acked
                finally:
                    if consumer:
                        consumer.cancel()
                    with suppress(Exception):
                        await asyncio.wait_for(
                            events_ws.close(), timeout=ARI_WS_CLOSE_TIMEOUT
                        )
                    if channel_id:
                        with suppress(AriProbeError):
                            async with aiohttp.ClientSession() as session:
                                await _delete(
                                    session, _ari_url(f"channels/{channel_id}")
                                )
        except TimeoutError:
            logger.info("Alarm call to %s exceeded acknowledgement window", phone)
            return False
        except AriProbeError as err:
            logger.error("Alarm call to %s failed: %s", phone, err)
            detail["status"] = "error"
            detail["error"] = str(err)
            return False
        finally:
            self.call_details.append(detail)

    async def _sync_channel_state(
        self,
        session: aiohttp.ClientSession,
        state: _EventState,
        channel_id: str,
    ) -> None:
        """Reconciles answer state from the channel snapshot.

        Events emitted between the originate and the consumer start can be
        missed on a slow link; the snapshot makes the answer gate reliable.
        """
        try:
            status, body = await _get_json(session, _ari_url(f"channels/{channel_id}"))
        except AriProbeError as err:
            logger.debug("Alarm channel snapshot unavailable for %s: %s", channel_id, err)
            return
        if status >= 300 or not isinstance(body, dict):
            return
        chan_state = str(body.get("state") or "")
        logger.info(
            "Alarm channel %s snapshot: state=%s answered=%s",
            channel_id,
            chan_state,
            state.answered,
        )
        if chan_state == "Up" and not state.answered:
            state.answered = True
            state.answered_at = asyncio.get_running_loop().time()
        if chan_state == "Up":
            state.channel_alive = True
        # Other states (Down/Ring/...) are not terminal: a freshly originated
        # channel reports "Down" until the first SIP exchange starts, and only
        # the event stream knows when the channel is really gone.

    async def _wait_ack(
        self,
        session: aiohttp.ClientSession,
        state: _EventState,
        channel_id: str,
        tts_media: str | None,
        wait_seconds: float,
        loop: asyncio.AbstractEventLoop,
    ) -> bool:
        """
        Announces the alarm and polls the shared state for the ACK digit.

        The announcement only starts once the callee answers (media queued on a
        ringing channel is never heard) and is replayed whenever the previous
        playback completes, so a person picking up mid-sentence still hears the
        whole message before the escalation window closes. Playback progress is
        polled through the ARI playback resource - relying on websocket events
        alone left silent calls whenever an event was missed - and a rejected or
        failed announcement immediately degrades to the preset local alert.
        """
        media = tts_media or ALARM_FALLBACK_MEDIA
        fallback_media = ALARM_FALLBACK_MEDIA
        deadline = loop.time() + wait_seconds

        while loop.time() < deadline and not (state.answered or state.stasis_started):
            if state.done or not state.channel_alive:
                logger.info("Alarm callee %s hung up before answering", channel_id)
                return False
            await asyncio.sleep(0.2)
        if not (state.answered or state.stasis_started):
            logger.info("Alarm callee %s never answered", channel_id)
            return False

        announced = 0
        last_play_at = 0.0
        play_started_at = 0.0
        playback_id: str | None = None
        degraded = False
        while loop.time() < deadline:
            if state.last_dtmf == ALARM_ACK_DIGIT:
                logger.info("Alarm ACK digit received on channel %s", channel_id)
                return True
            if state.done or not state.channel_alive:
                logger.info("Alarm callee %s hung up without ACK", channel_id)
                return False
            now = loop.time()
            if playback_id is None:
                # 3s floor between attempts: a broken media URI must not spin.
                if last_play_at and now - last_play_at < 3.0:
                    await asyncio.sleep(0.3)
                    continue
                status, body = await _post_json(
                    session,
                    _ari_url(
                        f"channels/{channel_id}/play",
                        {"media": media, "lang": ALARM_TTS_LANG, "offsetms": "0"},
                    ),
                )
                last_play_at = now
                if status >= 300:
                    if not degraded and media != fallback_media:
                        degraded = True
                        media = fallback_media
                        last_play_at = 0.0
                        logger.warning(
                            "Alarm announcement rejected (HTTP %s) on channel %s; "
                            "switching to preset alert %s",
                            status,
                            channel_id,
                            fallback_media,
                        )
                        continue
                    logger.warning(
                        "Alarm playback rejected on channel %s: HTTP %s", channel_id, status
                    )
                    await asyncio.sleep(1.0)
                    continue
                announced += 1
                playback_id = (body or {}).get("id") or None
                play_started_at = now
                state.last_playback_state = ""
                logger.info(
                    "Alarm announcement %s started on channel %s (media=%s, playback=%s)",
                    announced,
                    channel_id,
                    media,
                    playback_id,
                )
            else:
                status, body = await _get_json(
                    session, _ari_url(f"playbacks/{playback_id}")
                )
                play_state = str((body or {}).get("state") or "")
                if status >= 300 or play_state in ("done", "failed"):
                    elapsed = now - play_started_at
                    failed = (
                        play_state == "failed"
                        or state.last_playback_state == "failed"
                        or (media != fallback_media and elapsed < 2.0)
                    )
                    if failed and not degraded and media != fallback_media:
                        degraded = True
                        media = fallback_media
                        last_play_at = 0.0
                        logger.warning(
                            "Alarm announcement unusable on channel %s "
                            "(state=%s, elapsed=%.1fs); switching to preset alert %s",
                            channel_id,
                            play_state or state.last_playback_state,
                            elapsed,
                            fallback_media,
                        )
                    playback_id = None
            await asyncio.sleep(0.3)
        return False
