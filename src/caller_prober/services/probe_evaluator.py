"""
Probe result evaluation for direct and callcenter numbers.

Pure, synchronous, dependency-free judgement logic: given the raw facts
collected during a probe call (SIP answer/hangup cause, Stasis entry,
connected duration, DTMF liveness, media evidence) it derives a single
verdict plus a machine-readable failure code.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from caller_prober.constants import (
    CALLCENTER_EARLY_HANGUP_SECONDS,
    HANGUP_CAUSE_MAP,
    ORIGINATE_HTTP_FAILURE_MAP,
)

# Verdicts
VERDICT_OK = "OK"
VERDICT_FAIL = "FAIL"

# Failure codes
F_NO_ANSWER = "NO_ANSWER"
F_SIP_BUSY = "SIP_BUSY"
F_SIP_CONGESTION = "SIP_CONGESTION"
F_SIP_REJECT = "SIP_CALL_REJECTED"
F_SIP_4XX_5XX = "SIP_CLIENT_SERVER_ERROR"
F_ARI_ERROR = "ARI_ERROR"
F_PROBE_TIMEOUT = "PROBE_TIMEOUT"
F_NOT_ANSWERED = "NOT_ANSWERED"
F_NO_STASIS = "CC_NO_STASIS"
F_EARLY_HANGUP = "CC_EARLY_HANGUP"
F_ONE_WAY_AUDIO = "CC_ONE_WAY_AUDIO"
F_DTMF_DEAD = "CC_DTMF_CHANNEL_DEAD"
F_NO_MEDIA = "CC_NO_MEDIA"


@dataclass(slots=True, frozen=True)
class ProbeOutcome:
    """Raw observable facts of a completed probe call."""

    probe_type: str
    answered: bool = False
    entered_stasis: bool = False
    connected_seconds: float = 0.0
    hangup_cause: int | None = None
    hangup_cause_txt: str | None = None
    dtmf_digits_sent: bool = False
    channel_alive_after_dtmf: bool = False
    media_frames_observed: int = 0
    http_status: int | None = None
    timed_out: bool = False
    hung_up_by_probe: bool = False
    recording_name: str | None = None


@dataclass(slots=True)
class ProbeResult:
    """Judgement for one probe execution."""

    verdict: str
    failure_code: str | None = None
    hangup_cause: int | None = None
    connected_seconds: float = 0.0
    media_frames_observed: int = 0
    details: dict[str, object] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """True when the probe passed."""
        return self.verdict == VERDICT_OK


def _failure_code_for_cause(cause: int) -> str:
    """Maps an Asterisk hangup cause code to a failure code."""
    mapped = HANGUP_CAUSE_MAP.get(cause)
    if mapped:
        return mapped
    if 400 <= cause < 600 or 4000 <= cause < 5000:  # 4xx/5xx family
        return F_SIP_4XX_5XX
    return F_SIP_4XX_5XX


def evaluate_probe(outcome: ProbeOutcome, dtmf_preset: str | None) -> ProbeResult:
    """
    Derives the verdict for one probe execution.

    Args:
        outcome: Raw facts observed during the call (see ProbeOutcome).
        dtmf_preset: DTMF digits configured for this probe (callcenter only).

    Returns:
        ProbeResult: Verdict + failure code + supporting details.
    """
    result = ProbeResult(
        verdict=VERDICT_FAIL,
        hangup_cause=outcome.hangup_cause,
        connected_seconds=outcome.connected_seconds,
        media_frames_observed=outcome.media_frames_observed,
    )

    # Transport-level originate failure (ARI returned 4xx/5xx to the originate)
    if outcome.http_status is not None and not outcome.answered:
        result.failure_code = ORIGINATE_HTTP_FAILURE_MAP.get(
            outcome.http_status, F_ARI_ERROR
        )
        return result

    if outcome.timed_out and not outcome.answered:
        result.failure_code = F_PROBE_TIMEOUT
        return result

    # Direct numbers: ringing or answered is enough; BUSY/NO_ANSWER/CONGESTION/4xx-5xx fail.
    if outcome.probe_type == "direct":
        if outcome.answered:
            result.verdict = VERDICT_OK
            return result
        if outcome.hangup_cause is None:
            result.failure_code = F_NOT_ANSWERED
        elif outcome.hangup_cause in (18, 19, 44):  # ring / no-answer family
            # Ringing observed but never answered counts as reachable.
            result.verdict = VERDICT_OK
            result.failure_code = F_NO_ANSWER if outcome.hangup_cause in (19, 44) else None
        else:
            result.failure_code = _failure_code_for_cause(outcome.hangup_cause)
        return result

    # Callcenter numbers: answered + Stasis + duration + media + DTMF liveness.
    if not outcome.answered:
        if outcome.hangup_cause is None:
            result.failure_code = F_NOT_ANSWERED
        elif outcome.hangup_cause == 16:
            # Normal clearing without an answer means the far end never picked up.
            result.failure_code = F_NO_ANSWER
        else:
            result.failure_code = _failure_code_for_cause(outcome.hangup_cause)
        return result

    if not outcome.entered_stasis:
        result.failure_code = F_NO_STASIS
        return result

    if outcome.connected_seconds < CALLCENTER_EARLY_HANGUP_SECONDS:
        result.failure_code = F_EARLY_HANGUP
        result.details["early_hangup_threshold"] = CALLCENTER_EARLY_HANGUP_SECONDS
        return result

    if outcome.media_frames_observed == 0:
        result.failure_code = F_ONE_WAY_AUDIO
        return result

    if dtmf_preset and outcome.dtmf_digits_sent and not outcome.channel_alive_after_dtmf:
        result.failure_code = F_DTMF_DEAD
        return result

    result.verdict = VERDICT_OK
    return result


def failure_code_for_hangup_cause(cause: int) -> str:
    """Public helper: maps an Asterisk hangup cause to a failure code."""
    return _failure_code_for_cause(cause)
