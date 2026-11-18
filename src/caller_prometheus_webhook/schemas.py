"""
Data schemas and contracts for the caller_prometheus_webhook service.

Adheres to Python 3.14 standards using modern immutable slotted dataclasses
for high performance and strict type checking.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class NotificationMode(StrEnum):
    """Supported alert notification dispatch modes."""

    CALL_ONLY = "call_only"
    SMS_ONLY = "sms_only"
    SMS_BEFORE_CALL = "sms_before_call"
    CALL_AND_SMS = "call_and_sms"


@dataclass(slots=True, frozen=True)
class PrometheusAlert:
    """
    Represents an alert item from Prometheus Alertmanager.

    Attributes:
        status: Alert state (e.g. 'firing', 'resolved').
        description: Extracted human-readable description summary.
        labels: Key-value metadata labels.
        annotations: Alert annotations dictionary.
    """

    status: str
    description: str
    labels: Mapping[str, str] = field(default_factory=dict)
    annotations: Mapping[str, str] = field(default_factory=dict)

    @property
    def is_firing(self) -> bool:
        """Indicates if the alert is actively firing."""
        return self.status.lower() == "firing"

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PrometheusAlert:
        """
        Parses an alert entry from an Alertmanager payload.

        Args:
            data: Key-value dictionary representing one alert.

        Returns:
            Validated immutable PrometheusAlert instance.
        """
        status = str(data.get("status") or "")
        annotations = data.get("annotations") or {}
        labels = data.get("labels") or {}
        description = str(annotations.get("description") or "No data")

        return cls(
            status=status,
            description=description,
            labels=labels,
            annotations=annotations,
        )


@dataclass(slots=True, frozen=True)
class WebhookPayload:
    """
    Payload envelope received from Alertmanager webhook triggers.

    Attributes:
        alerts: Sequence of parsed PrometheusAlert instances.
        receiver: Target Alertmanager receiver name.
    """

    alerts: Sequence[PrometheusAlert]
    receiver: str = ""

    @property
    def firing_descriptions(self) -> list[str]:
        """Returns the description of the first firing alert if present."""
        for alert in self.alerts:
            if alert.is_firing:
                return [alert.description]
        return []

    @property
    def all_firing_descriptions(self) -> list[str]:
        """Returns the descriptions of all firing alerts in the payload."""
        return [alert.description for alert in self.alerts if alert.is_firing]

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WebhookPayload:
        """
        Constructs a WebhookPayload from raw incoming JSON.

        Args:
            data: Inbound JSON payload dictionary.

        Returns:
            Validated WebhookPayload instance.
        """
        raw_alerts = data.get("alerts") or []
        parsed_alerts = [
            PrometheusAlert.from_dict(a) for a in raw_alerts if isinstance(a, dict)
        ]
        return cls(
            alerts=parsed_alerts,
            receiver=str(data.get("receiver") or ""),
        )


# Human readable text fields used by non-Alertmanager callers (Zabbix, CI,
# generic webhook media types). First non-empty field wins.
_FLAT_TEXT_FIELDS: tuple[str, ...] = (
    "message",
    "Message",
    "description",
    "Description",
    "body",
    "content",
    "subject",
    "Subject",
    "title",
    "alert",
    "event_name",
)


def _flatten_text(value: Any) -> str | None:
    """Returns a usable string from a scalar or ``{"content": ...}`` mapping."""
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, Mapping):
        inner = value.get("content") or value.get("text") or value.get("message")
        return _flatten_text(inner)
    if isinstance(value, (int, float)):
        return str(value)
    return None


def extract_alert_messages(payload: Mapping[str, Any]) -> list[str]:
    """
    Extracts the alert text from an inbound webhook payload.

    Understands the Prometheus Alertmanager envelope (firing alerts only) and,
    as a fallback, the flat JSON bodies produced by Zabbix and other systems -
    e.g. ``{"message": "{ALERT.MESSAGE}"}``, ``{"subject": "...", "body": "..."}``
    or a bare ``{"description": "..."}``. Resolved alerts are never returned.

    Args:
        payload: Parsed JSON body of the webhook request.

    Returns:
        list: Alert messages to dispatch (empty when nothing usable is found).
    """
    if not isinstance(payload, Mapping):
        return []

    envelope = WebhookPayload.from_dict(dict(payload))
    firing = [
        alert.description
        for alert in envelope.alerts
        if alert.is_firing and alert.description and alert.description != "No data"
    ]
    if firing:
        return firing

    for field_name in _FLAT_TEXT_FIELDS:
        text = _flatten_text(payload.get(field_name))
        if text:
            return [text]
    return []


@dataclass(slots=True, frozen=True)
class DispatchItem:
    """
    Queue item representing an alert message and target phone number.

    Attributes:
        message: Alert notification prompt.
        receiver: Target telephone number.
        mode: Dispatched notification mode.
    """

    message: str
    receiver: str
    mode: NotificationMode | str
