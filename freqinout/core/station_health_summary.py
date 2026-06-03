from __future__ import annotations

import time
from typing import Callable, Dict, Iterable, List, Mapping, Optional

from freqinout.core.dependency_health import get_dependency_health_registry
from freqinout.core.scheduler_events import load_recent_scheduler_events


ScopeResolver = Callable[[str, Mapping[str, object]], str]


def _int_value(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _float_value(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _format_duration_ms(value: object) -> str:
    duration = _float_value(value)
    if duration <= 0:
        return ""
    if duration >= 1000:
        return f"{duration / 1000.0:.1f}s"
    return f"{duration:.0f} ms"


def _format_age(ts: object, *, now: Optional[float] = None) -> str:
    stamp = _float_value(ts)
    if stamp <= 0:
        return ""
    age = max(0.0, (time.monotonic() if now is None else now) - stamp)
    if age < 1:
        return "just now"
    if age < 60:
        return f"{age:.0f}s ago"
    if age < 3600:
        return f"{age / 60.0:.0f}m ago"
    return f"{age / 3600.0:.1f}h ago"


def _format_issue_since(ts: object, *, now: Optional[float] = None) -> str:
    age = _format_age(ts, now=now)
    return f"since {age}" if age else ""


def _format_cooldown(value: object) -> str:
    cooldown = _float_value(value)
    if cooldown <= 0:
        return ""
    if cooldown < 60:
        return f"{cooldown:.0f}s"
    return f"{cooldown / 60.0:.1f}m"


def _shorten_error(value: object) -> str:
    text = str(value or "").strip()
    if len(text) <= 140:
        return text
    return text[:137].rstrip() + "..."


def _dependency_label(key: str, owner: str = "") -> str:
    parts = [part for part in str(key or "").split(":") if part]
    if not parts:
        return str(owner or "Dependency").strip() or "Dependency"
    service = parts[0].upper().replace("-", "_")
    if service == "BACKGROUND_INGEST":
        job = _background_job_label(parts[1] if len(parts) > 1 else "")
        return f"Background ingest: {job}" if job else "Background ingest jobs"
    if service == "SCHEDULER":
        return _scheduler_health_label(parts[1] if len(parts) > 1 else "")
    labels = {
        "JS8CALL": "JS8Call API",
        "FLRIG": "FLRig API",
        "FLDIGI": "FLDigi XML-RPC",
        "COMMSTAT": "CommStat data",
        "VARAC": "VarAC data",
        "VARAC_BBS_VAULT_ALIASES": "Managed BBS Vault aliases",
        "RIGCTLD": "rigctld",
        "OBSERVER": "Observer SDR",
        "BACKGROUND_INGEST": "Background ingest",
    }
    label = labels.get(service, service.replace("_", " ").title())
    endpoint = ""
    if len(parts) >= 3 and parts[1] not in {"false", "true"}:
        endpoint_host = _display_host(parts[1])
        endpoint = f" ({endpoint_host}:{parts[2]})"
    return f"{label}{endpoint}"


def _scheduler_health_label(value: object) -> str:
    raw = str(value or "").strip().replace("-", "_")
    labels = {
        "fldigi_busy": "Scheduler hold: FLDigi RX activity",
        "js8_busy": "Scheduler hold: JS8Call busy",
        "varac_busy": "Scheduler hold: VarAC busy",
        "flrig_ptt": "Scheduler hold: FLRig PTT",
        "status_snapshot": "Scheduler status snapshot",
        "control_task": "Scheduler control task",
    }
    if not raw:
        return "Scheduler"
    return labels.get(raw.lower(), f"Scheduler: {raw.replace('_', ' ').title()}")


def _display_host(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() in {"loopback", "localhost", "::1"}:
        return "127.0.0.1"
    return text


def _canonical_endpoint_key(key: object) -> str:
    parts = [part for part in str(key or "").split(":") if part]
    if len(parts) >= 3:
        service = parts[0].strip().lower().replace("-", "_")
        if service in {"js8call", "flrig", "fldigi", "rigctld", "observer"}:
            host = _display_host(parts[1]).strip().lower()
            port = parts[2].strip()
            return f"{service}:{host}:{port}"
    return str(key or "").strip().lower()


def _background_job_label(value: object) -> str:
    raw = str(value or "").strip().replace("-", "_")
    labels = {
        "js8_links": "JS8 links",
        "js8_messages": "JS8 messages",
        "js8_spotter": "JS8Spotter",
        "commstat": "CommStat",
        "commstat_messages": "CommStat messages",
        "varac_vault": "Managed BBS",
        "varac_messages": "VarAC messages",
        "fldigi_messages": "FLDigi messages",
        "flamp_messages": "FLAMP messages",
    }
    if not raw:
        return ""
    return labels.get(raw.lower(), raw.replace("_", " ").title())


def _scope_from_metadata(metadata: Mapping[str, object]) -> str:
    for key in ("scope", "radio_name", "radio", "profile_name", "device_name"):
        text = str(metadata.get(key, "") or "").strip()
        if text:
            return text
    for key in ("radio_id", "device_profile_id", "profile_id"):
        value = metadata.get(key)
        if value not in (None, ""):
            return f"Radio {value}"
    return "Station-wide"


def _item_from_snapshot(
    key: str,
    snapshot: Mapping[str, object],
    *,
    scope_resolver: Optional[ScopeResolver] = None,
    now: Optional[float] = None,
) -> Dict[str, object]:
    metadata = snapshot.get("metadata", {})
    if not isinstance(metadata, Mapping):
        metadata = {}
    owner = str(snapshot.get("owner", "") or "").strip()
    failures = _int_value(snapshot.get("consecutive_failures"))
    slow = _int_value(snapshot.get("consecutive_slow"))
    cooldown = _float_value(snapshot.get("cooldown_remaining_sec"))
    service = str(key or "").split(":", 1)[0].strip().lower().replace("-", "_")
    backoff = cooldown > 0 or (failures >= 3 and service != "scheduler")
    last_checked_ts = _float_value(snapshot.get("last_checked_ts"))
    stale_ok = bool(
        service != "scheduler"
        and last_checked_ts > 0
        and failures <= 0
        and slow <= 0
        and (now or time.monotonic()) - last_checked_ts > 600.0
    )
    warning = stale_ok or failures > 0 or slow > 0 or bool(str(snapshot.get("last_error", "") or "").strip())
    if backoff:
        severity = "danger"
        state = "Backoff"
        action = "Backing off to keep FIO responsive"
    elif stale_ok:
        severity = "warning"
        state = "Stale"
        action = "Last OK check is stale; waiting for the next fresh check"
    elif warning:
        severity = "warning"
        state = "Warning"
        action = "Waiting for the next normal check"
    else:
        severity = "ok"
        state = "OK"
        action = "Normal"
    action_override = str(metadata.get("action", "") or "").strip()
    if action_override:
        action = action_override
    scope = ""
    if scope_resolver is not None:
        try:
            scope = str(scope_resolver(str(key or ""), metadata) or "").strip()
        except Exception:
            scope = ""
    if not scope:
        scope = _scope_from_metadata(metadata)
    return {
        "key": str(key or ""),
        "scope": scope,
        "dependency": _dependency_label(str(key or ""), owner),
        "state": state,
        "severity": severity,
        "action": action,
        "last_issue": _shorten_error(snapshot.get("last_error", "")),
        "issue_since": _format_issue_since(snapshot.get("issue_started_ts"), now=now) if warning else "",
        "cooldown": _format_cooldown(cooldown),
        "last_check": _format_age(snapshot.get("last_checked_ts"), now=now),
        "last_check_ts": _float_value(snapshot.get("last_checked_ts")),
        "last_duration": _format_duration_ms(snapshot.get("last_duration_ms")),
        "failures": failures,
        "slow": slow,
        "is_issue": severity != "ok",
        "group": "background_ingest" if str(key or "").strip().lower().startswith("background-ingest:") else "",
    }


def _collapse_background_ingest_ok_items(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
    collapsed: List[Dict[str, object]] = []
    groups: Dict[str, List[Dict[str, object]]] = {}
    for item in items:
        if item.get("group") == "background_ingest" and not item.get("is_issue"):
            groups.setdefault(str(item.get("scope", "") or "Station-wide"), []).append(item)
        else:
            collapsed.append(item)
    for scope, group_items in groups.items():
        if not group_items:
            continue
        latest = max(group_items, key=lambda item: _float_value(item.get("last_check_ts")))
        row = dict(latest)
        row["scope"] = scope
        row["dependency"] = "Background ingest jobs"
        row["action"] = f"Normal ({len(group_items)} jobs healthy)"
        row["last_issue"] = ""
        row["issue_since"] = ""
        row["cooldown"] = ""
        collapsed.append(row)
    return collapsed


def _merge_equivalent_endpoint_items(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
    severity_rank = {"danger": 0, "warning": 1, "ok": 2}
    merged: Dict[str, Dict[str, object]] = {}
    for item in items:
        key = _canonical_endpoint_key(item.get("key", ""))
        existing = merged.get(key)
        if existing is None:
            merged[key] = item
            continue
        item_rank = severity_rank.get(str(item.get("severity", "ok")), 3)
        existing_rank = severity_rank.get(str(existing.get("severity", "ok")), 3)
        if item_rank < existing_rank:
            merged[key] = item
        elif item_rank == existing_rank and _float_value(item.get("last_check_ts")) >= _float_value(
            existing.get("last_check_ts")
        ):
            merged[key] = item
    return list(merged.values())


def summarize_station_health(
    registry_snapshot: Optional[Mapping[str, object]] = None,
    *,
    include_ok: bool = True,
    include_scheduler_events: bool = False,
    scope_resolver: Optional[ScopeResolver] = None,
) -> Dict[str, object]:
    """
    Build a low-impact Station Health summary from the existing health registry.

    This intentionally does not probe external software. It only reports what
    existing FIO work has already learned about dependency responsiveness.
    """
    if registry_snapshot is None:
        try:
            registry_snapshot = get_dependency_health_registry().snapshot()
        except Exception:
            registry_snapshot = {}
    now = time.monotonic()
    items: List[Dict[str, object]] = []
    if isinstance(registry_snapshot, Mapping):
        iterable: Iterable[tuple[str, object]] = registry_snapshot.items()
    else:
        iterable = []
    for key, raw_snapshot in iterable:
        if not isinstance(raw_snapshot, Mapping):
            continue
        item = _item_from_snapshot(str(key or ""), raw_snapshot, scope_resolver=scope_resolver, now=now)
        if include_ok or item["is_issue"]:
            items.append(item)
    items = _collapse_background_ingest_ok_items(items)
    items = _merge_equivalent_endpoint_items(items)
    items.sort(
        key=lambda item: (
            {"danger": 0, "warning": 1, "ok": 2}.get(str(item.get("severity")), 3),
            str(item.get("scope", "")),
            str(item.get("dependency", "")),
        )
    )
    issue_items = [item for item in items if item.get("is_issue")]
    severity = "ok"
    if any(str(item.get("severity")) == "danger" for item in issue_items):
        severity = "danger"
    elif issue_items:
        severity = "warning"
    recent_scheduler_events = load_recent_scheduler_events(limit=25) if include_scheduler_events else []
    return {
        "severity": severity,
        "issue_count": len(issue_items),
        "items": items,
        "issue_items": issue_items,
        "recent_scheduler_events": recent_scheduler_events,
    }
