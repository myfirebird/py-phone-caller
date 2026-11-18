"""
Execution history for probe runs.

Every scheduled or manually triggered probe execution appends one JSON record to
an append-only JSONL file so operators can inspect what happened (verdict,
failure code, SIP hangup cause, duration, media frames, DTMF result) long after
the call ended. The file is trimmed to the newest ``PROBE_RECORDS_LIMIT``
entries on append to keep it bounded.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from caller_prober.constants import PROBE_RECORDS_FILE, PROBE_RECORDS_LIMIT

logger = logging.getLogger(__name__)

# Timestamp used to order records: the moment the run finished, else started.
_EPOCH_MIN = datetime.min.replace(tzinfo=UTC)


def _sort_key(row: dict[str, Any]) -> datetime:
    """Ordering key for one record (UTC, oldest first)."""
    for field_name in ("finished_at", "started_at", "recorded_at"):
        value = row.get(field_name)
        if not value:
            continue
        try:
            moment = datetime.fromisoformat(str(value))
        except ValueError:
            continue
        return moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return _EPOCH_MIN


def records_path() -> Path:
    """Resolves the JSONL execution-record file, next to the probe config."""
    explicit = os.environ.get("CALLER_PROBE_RECORDS")
    if explicit:
        return Path(explicit)
    from caller_prober.config import _probe_config_path

    try:
        return _probe_config_path().parent / PROBE_RECORDS_FILE
    except Exception:  # noqa: BLE001 - fall back to the working directory
        return Path.cwd() / PROBE_RECORDS_FILE


def append_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Appends one execution record and trims the file to the configured limit.

    Args:
        record: Flat dictionary describing the probe execution.

    Returns:
        dict: The stored record with ``recorded_at`` filled in.
    """
    entry = dict(record)
    entry.setdefault("recorded_at", datetime.now(UTC).isoformat())
    path = records_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = read_records(limit=PROBE_RECORDS_LIMIT - 1, newest_first=False)
        rows.append(entry)
        # Keep the file strictly chronological: append-only history must never
        # depend on the order earlier versions happened to write it in.
        rows.sort(key=_sort_key)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp_path.replace(path)
    except OSError as err:
        logger.error("Cannot persist probe execution record to %s: %s", path, err)
    return entry


def read_records(limit: int = 50, newest_first: bool = True) -> list[dict[str, Any]]:
    """
    Reads stored execution records.

    Args:
        limit: Maximum number of records to return.
        newest_first: Return the newest executions first.

    Returns:
        list: Execution records (possibly empty).
    """
    path = records_path()
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except ValueError:
                    continue
                if isinstance(parsed, dict):
                    rows.append(parsed)
    except OSError as err:
        logger.error("Cannot read probe execution records from %s: %s", path, err)
        return []
    rows.sort(key=_sort_key)
    rows = rows[-limit:] if limit > 0 else rows
    return list(reversed(rows)) if newest_first else rows


def read_probe_history(limit: int = 100) -> list[dict[str, Any]]:
    """
    Returns probe runs newest first, each enriched with its alarm dispatch.

    ``dispatch_duty_alarm`` appends its own ``duty_alarm`` record carrying the
    roster calls it placed; joining on the originating probe task id lets the
    console show which duty numbers were dialled for every failure.

    Args:
        limit: Maximum number of probe runs to return.

    Returns:
        list: Probe records with ``alarm_calls`` (list) and ``alarm_phones``
        (comma separated string) attached; ``[]`` when nothing was recorded.
    """
    rows = read_records(limit=0, newest_first=False)
    alarms: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.get("event") != "duty_alarm":
            continue
        key = str(row.get("probe_task_id") or "")
        if key:
            alarms[key] = row

    history: list[dict[str, Any]] = []
    for row in sorted(rows, key=_sort_key, reverse=True):
        if row.get("event") != "probe_completed":
            continue
        entry = dict(row)
        alarm = alarms.get(str(row.get("task_id") or ""))
        calls = list((alarm or {}).get("calls") or [])
        entry["alarm_calls"] = calls
        entry["alarm_phones"] = ", ".join(
            str(call.get("phone")) for call in calls if call.get("phone")
        )
        entry["alarm_acked_by"] = (alarm or {}).get("acked_by")
        history.append(entry)
        if limit and len(history) >= limit:
            break
    return history
