"""
Loader for the probe routing configuration (config/probe_config.yml).

The file defines duty groups (escalation rosters with webhooks) and probe
targets (numbers, schedule, DTMF presets, duty group binding). Resolution
order for the file location:

1. ``CALLER_PROBE_CONFIG`` environment variable (explicit file path)
2. ``<CONFIG_DIR>/probe_config.yml`` (the shared Dynaconf config directory)
3. ``<cwd>/config/probe_config.yml``
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from caller_prober.constants import PROBE_MAX_CALL_SECONDS
from caller_prober.exceptions import ProbeConfigError

logger = logging.getLogger(__name__)

# Talk-time default used when a probe does not override max_call_seconds.
DEFAULT_MAX_CALL_SECONDS = PROBE_MAX_CALL_SECONDS


@dataclass(slots=True, frozen=True)
class DutyMember:
    """One on-call person reachable by voice."""

    name: str
    phone: str


@dataclass(slots=True, frozen=True)
class DutyGroup:
    """On-call group bound to a probe: webhook + ordered call roster."""

    name: str
    members: tuple[DutyMember, ...]
    escalation_timeout: float
    webhook_url: str
    notification_type: str = "call_and_sms"
    token: str = ""
    # Immutable identifier used by the webhook URL: survives renames and token
    # rotation, unlike the group name/token pair.
    id: str = ""
    # Default TTS voice language for this group's webhook calls (empty = the
    # service-wide caller_prometheus_webhook.webhook_tts_lang, then the
    # generate_audio default). A ?lang= query parameter still overrides it.
    lang: str = ""

    def phones(self) -> tuple[str, ...]:
        """Returns the member phone numbers in escalation order."""
        return tuple(m.phone for m in self.members)


@dataclass(slots=True, frozen=True)
class ProbeTarget:
    """A single probed number and everything needed to dial and judge it."""

    phone: str
    name: str
    probe_type: str  # "callcenter" | "direct"
    schedule: str  # cron expression (5 fields)
    timeout: float
    dtmf_digits: str | None
    duty_group: str
    # Talk time allowed after the answer; the probe hangs up when it elapses.
    max_call_seconds: float = DEFAULT_MAX_CALL_SECONDS
    # Bearer token for the probe-trigger webhook (?token=...), so monitoring
    # systems can start this probe on demand without touching the console.
    token: str = ""
    # Immutable identifier used by the trigger URL: survives renames and token
    # rotation, unlike the name/token pair.
    id: str = ""
    # False = stopped: the Beat scan skips this probe's cron schedule. Manual
    # runs (console button / trigger webhook) still work while stopped.
    enabled: bool = True


@dataclass(slots=True, frozen=True)
class ProbeConfig:
    """Parsed top-level probe configuration."""

    duty_groups: dict[str, DutyGroup] = field(default_factory=dict)
    probes: tuple[ProbeTarget, ...] = ()

    def group_for(self, probe: ProbeTarget) -> DutyGroup:
        """Returns the duty group bound to the given probe."""
        try:
            return self.duty_groups[probe.duty_group]
        except KeyError as err:
            raise ProbeConfigError(
                f"Probe '{probe.name}' references undefined duty group '{probe.duty_group}'"
            ) from err


def _probe_config_path() -> Path:
    env_path = os.environ.get("CALLER_PROBE_CONFIG")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p
        raise ProbeConfigError(f"CALLER_PROBE_CONFIG file not found: {p}")
    from py_phone_caller_utils.config import CONFIG_DIR

    candidates = [
        CONFIG_DIR / "probe_config.yml",
        Path.cwd() / "config" / "probe_config.yml",
    ]
    for cand in candidates:
        if cand.is_file():
            return cand
    raise ProbeConfigError(
        "probe_config.yml not found (checked: "
        + ", ".join(str(c) for c in candidates)
        + ")"
    )


def _parse_group(name: str, raw: dict) -> DutyGroup:
    members = tuple(
        DutyMember(name=str(m["name"]), phone=str(m["phone"])) for m in raw.get("members", [])
    )
    if not members:
        raise ProbeConfigError(f"Duty group '{name}' has no members")
    webhook = str(raw.get("webhook_url", "")).strip()
    if not webhook:
        raise ProbeConfigError(f"Duty group '{name}' has no webhook_url")
    notif_type = str(raw.get("notification_type") or "call_and_sms").strip()
    if notif_type not in ("call", "sms", "call_and_sms", "sms_before_call"):
        notif_type = "call_and_sms"
    token = str(raw.get("token") or "").strip()
    if not token:
        import secrets
        token = secrets.token_urlsafe(16)
    group_id = str(raw.get("id") or "").strip()
    if not group_id:
        from uuid import uuid4

        group_id = uuid4().hex
    lang = str(raw.get("lang") or "").strip()
    return DutyGroup(
        name=name,
        members=members,
        escalation_timeout=float(raw.get("escalation_timeout", 60)),
        webhook_url=webhook,
        notification_type=notif_type,
        token=token,
        id=group_id,
        lang=lang,
    )


def parse_enabled(value: object, default: bool = True) -> bool:
    """
    Coerces a stored/requested probe enable flag to bool.

    Args:
        value: Raw YAML or JSON value (bool, "true"/"false", 1/0, None).
        default: Returned when ``value`` is None.

    Returns:
        bool: Whether scheduled probing is enabled.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _parse_probe(raw: dict) -> ProbeTarget:
    phone = str(raw["phone"]).strip()
    probe_type = str(raw.get("type", "direct")).strip()
    if probe_type not in ("callcenter", "direct"):
        raise ProbeConfigError(f"Probe '{raw.get('name', phone)}': invalid type '{probe_type}'")
    dtmf = raw.get("dtmf_digits")
    if dtmf is not None:
        dtmf = str(dtmf).strip() or None
    raw_max_call = raw.get("max_call_seconds")
    try:
        max_call = (
            float(raw_max_call) if raw_max_call not in (None, "") else DEFAULT_MAX_CALL_SECONDS
        )
    except (TypeError, ValueError) as err:
        raise ProbeConfigError(
            f"Probe '{raw.get('name', phone)}': max_call_seconds must be a number"
        ) from err
    if max_call <= 0:
        raise ProbeConfigError(
            f"Probe '{raw.get('name', phone)}': max_call_seconds must be positive"
        )
    token = str(raw.get("token") or "").strip()
    if not token:
        import secrets

        token = secrets.token_urlsafe(16)
    probe_id = str(raw.get("id") or "").strip()
    if not probe_id:
        from uuid import uuid4

        probe_id = uuid4().hex
    return ProbeTarget(
        phone=phone,
        name=str(raw.get("name", phone)),
        probe_type=probe_type,
        schedule=str(raw.get("schedule", "")).strip(),
        timeout=float(raw.get("timeout", 60)),
        dtmf_digits=dtmf,
        duty_group=str(raw.get("duty_group", "")).strip(),
        max_call_seconds=max_call,
        token=token,
        id=probe_id,
        enabled=parse_enabled(raw.get("enabled"), True),
    )


def load_probe_config(path: str | Path | None = None) -> ProbeConfig:
    """
    Loads and validates probe_config.yml.

    Args:
        path: Optional explicit path (tests / tooling). When omitted the
            standard resolution order documented at module level applies.

    Returns:
        ProbeConfig: Frozen, validated configuration object.

    Raises:
        ProbeConfigError: On missing file, YAML syntax errors, or schema
            violations (unknown type, empty roster, missing binding).
    """
    cfg_path = Path(path) if path else _probe_config_path()
    try:
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    except FileNotFoundError as err:
        raise ProbeConfigError(f"Probe config not found: {cfg_path}") from err
    except yaml.YAMLError as err:
        raise ProbeConfigError(f"Invalid YAML in {cfg_path}: {err}") from err
    if not isinstance(raw, dict):
        raise ProbeConfigError(f"{cfg_path} must contain a mapping at top level")

    groups_raw = raw.get("duty_groups") or {}
    if not isinstance(groups_raw, dict) or not groups_raw:
        raise ProbeConfigError("No duty_groups defined in probe config")
    groups = {str(k): _parse_group(str(k), v) for k, v in groups_raw.items()}

    probes_raw = raw.get("probes") or []
    if not isinstance(probes_raw, list) or not probes_raw:
        raise ProbeConfigError("No probes defined in probe config")
    probes = tuple(_parse_probe(p) for p in probes_raw)

    known_groups = set(groups)
    for probe in probes:
        if not probe.phone:
            raise ProbeConfigError(f"Probe '{probe.name}': phone is required")
        if probe.duty_group not in known_groups:
            raise ProbeConfigError(
                f"Probe '{probe.name}' binds unknown duty group '{probe.duty_group}'"
            )

    logger.info(
        "Loaded probe config: %d duty groups, %d probes", len(groups), len(probes)
    )
    return ProbeConfig(duty_groups=groups, probes=probes)


def save_probe_config(config: ProbeConfig, path: str | Path | None = None) -> Path:
    """
    Serialises a ProbeConfig back to probe_config.yml.

    Args:
        config: Validated configuration object (duty groups + probes).
        path: Optional explicit destination path. When omitted the file that
            _probe_config_path() resolves (env var or config dir) is rewritten
            in place.

    Returns:
        Path: The file that was written.

    Raises:
        ProbeConfigError: When validation of the serialised form fails or the
            file cannot be written.
    """
    cfg_path = Path(path) if path else _probe_config_path()
    data = {
        "duty_groups": {
            name: {
                "members": [
                    {"name": m.name, "phone": m.phone} for m in group.members
                ],
                "escalation_timeout": group.escalation_timeout,
                "webhook_url": group.webhook_url,
                "notification_type": group.notification_type,
                "token": group.token,
                "id": group.id,
                "lang": group.lang,
            }
            for name, group in config.duty_groups.items()
        },
        "probes": [
            {
                "phone": probe.phone,
                "name": probe.name,
                "type": probe.probe_type,
                "schedule": probe.schedule,
                "timeout": probe.timeout,
                "max_call_seconds": probe.max_call_seconds,
                "dtmf_digits": probe.dtmf_digits,
                "duty_group": probe.duty_group,
                "token": probe.token,
                "id": probe.id,
                "enabled": probe.enabled,
            }
            for probe in config.probes
        ],
    }
    try:
        # Round-trip through the parser so a broken save can never corrupt
        # the live file consumed by the Celery workers.
        groups = {str(k): _parse_group(str(k), v) for k, v in data["duty_groups"].items()}
        probes = tuple(_parse_probe(p) for p in data["probes"])
        known = set(groups)
        for probe in probes:
            if probe.duty_group not in known:
                raise ProbeConfigError(
                    f"Probe '{probe.name}' binds unknown duty group '{probe.duty_group}'"
                )
        tmp_path = cfg_path.with_suffix(cfg_path.suffix + ".tmp")
        tmp_path.write_text(
            yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8"
        )
        tmp_path.replace(cfg_path)
    except OSError as err:
        raise ProbeConfigError(f"Cannot write probe config {cfg_path}: {err}") from err
    logger.info("Saved probe config: %d duty groups, %d probes", len(groups), len(probes))
    return cfg_path
