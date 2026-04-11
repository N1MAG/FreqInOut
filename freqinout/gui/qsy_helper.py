from __future__ import annotations

from typing import Dict, List, Optional
import datetime
import time

from PySide6.QtWidgets import QComboBox, QMessageBox
from freqinout.core.mode_utils import normalize_operating_group_mode


HOLD_DURATION_PRESETS: tuple[int, ...] = (30, 60, 90, 120)
DEFAULT_HOLD_DURATION_MIN = 30
HOLD_WARNING_SECONDS = 10 * 60
HOLD_CRITICAL_SECONDS = 2 * 60
_HOLD_DURATION_DEFAULT_CACHE: Dict[str, Optional[int]] = {"minutes": None}
_SCHEDULER_ENABLED_OVERRIDE: Dict[str, Optional[bool]] = {"enabled": None}


def load_operating_groups(settings) -> List[Dict]:
    """
    Load operating_groups from settings with normalized fields.
    """
    data = settings.all()
    og = data.get("operating_groups", [])
    if not isinstance(og, list):
        return []
    cleaned: List[Dict] = []
    for g in og:
        if not isinstance(g, dict):
            continue
        g = dict(g)
        try:
            g["frequency"] = f"{float(g.get('frequency', 0)):.3f}"
        except Exception:
            g["frequency"] = ""
        g["mode"] = normalize_operating_group_mode(g.get("mode", ""), g.get("band", ""))
        g["auto_tune"] = bool(g.get("auto_tune", False))
        cleaned.append(g)
    return cleaned


def snapshot_operating_groups(og_list: List[Dict]) -> str:
    """
    Deterministic snapshot of operating groups used to detect changes.
    """
    parts = []
    for g in sorted(
        og_list, key=lambda x: (str(x.get("group", "")).lower(), str(x.get("band", "")).lower())
    ):
        parts.append(
            f"{g.get('group','')}|{g.get('mode','')}|{g.get('band','')}|{g.get('frequency','')}|"
            f"{g.get('vfo','')}|{g.get('fldigi_mode','')}|{g.get('fldigi_offset','')}|"
            f"{int(bool(g.get('auto_tune', False)))}"
        )
    return ";".join(parts)


def build_qsy_options(og_list: List[Dict]) -> Dict[str, Dict]:
    """
    Build a unique frequency map keyed by frequency string. Auto-tune wins on duplicates.
    """
    meta: Dict[str, Dict] = {}
    for g in og_list:
        try:
            fval = float(g.get("frequency", 0))
        except Exception:
            continue
        key = f"{fval:.3f}"
        auto = bool(g.get("auto_tune", False))
        vfo = (g.get("vfo") or "").strip().upper()
        group = (g.get("group") or "").strip().upper()
        existing = meta.get(key)
        if existing:
            existing["auto_tune"] = existing.get("auto_tune", False) or auto
            if auto and g.get("mode"):
                existing["mode"] = g.get("mode", "")
            if auto and g.get("band"):
                existing["band"] = g.get("band", "")
            if vfo and not existing.get("vfo"):
                existing["vfo"] = vfo
            if group and not existing.get("group"):
                existing["group"] = group
        else:
            meta[key] = {
                "freq": fval,
                "mode": g.get("mode", ""),
                "band": g.get("band", ""),
                "auto_tune": auto,
                "vfo": vfo,
                "group": group,
            }
    return meta


def refresh_qsy_combo(combo: QComboBox, meta: Dict[str, Dict]) -> None:
    items = sorted(meta.items(), key=lambda kv: float(kv[0]))
    combo.blockSignals(True)
    combo.clear()
    combo.addItem("Select frequency", None)
    for key, m in items:
        combo.addItem(f"{key} MHz", m)
    combo.blockSignals(False)


def selected_qsy_meta(combo: QComboBox) -> Optional[Dict]:
    data = combo.currentData()
    return data if isinstance(data, dict) else None


def current_scheduler_freq(window) -> Optional[float]:
    try:
        sched = getattr(window, "scheduler", None)
        entry = getattr(sched, "current_schedule_entry", {}) if sched else {}
        if not entry:
            return None
        return float(entry.get("frequency"))
    except Exception:
        return None


def _shared_ptt_block_reason(scheduler) -> str:
    if scheduler is None or not hasattr(scheduler, "get_status_summary"):
        return ""
    try:
        status = scheduler.get_status_summary()
    except Exception:
        return ""
    if not isinstance(status, dict) or not bool(status.get("shared_ptt_blocked")):
        return ""
    reason = str(status.get("shared_ptt_reason") or "").strip()
    if reason:
        return reason
    group = str(status.get("shared_ptt_group") or "").strip()
    owner = str(status.get("shared_ptt_owner_name") or "").strip()
    if group and owner:
        return f"Shared PTT group {group} is in use by {owner}."
    if group:
        return f"Shared PTT group {group} is currently busy."
    return "Shared PTT interlock is active."


def _coordination_conflict_warning(scheduler, entry: Dict) -> Dict[str, object]:
    if scheduler is None or not hasattr(scheduler, "evaluate_coordination_conflict"):
        return {}
    try:
        payload = scheduler.evaluate_coordination_conflict(entry, source="QSY")
    except TypeError:
        try:
            payload = scheduler.evaluate_coordination_conflict(entry)
        except Exception:
            return {}
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def perform_qsy(window, meta: Dict) -> bool:
    try:
        scheduler = getattr(window, "scheduler", None)
    except Exception:
        scheduler = None
    if not scheduler:
        QMessageBox.warning(window, "Scheduler", "Scheduler engine is unavailable.")
        return False
    freq = meta.get("freq")
    if freq is None:
        QMessageBox.warning(window, "QSY", "Select a frequency before QSY.")
        return False
    entry = {
        "frequency": f"{float(freq):.3f}",
        "band": meta.get("band", ""),
        "mode": meta.get("mode", ""),
        "auto_tune": bool(meta.get("auto_tune", False)),
        "vfo": meta.get("vfo", ""),
        "group": (meta.get("group") or "").strip().upper(),
        "group_name": (meta.get("group") or "").strip().upper(),
    }
    block_reason = _shared_ptt_block_reason(scheduler)
    if block_reason:
        QMessageBox.warning(window, "QSY Blocked", block_reason)
        return False
    conflict = _coordination_conflict_warning(scheduler, entry)
    if bool(conflict.get("warning")):
        msg = QMessageBox(window)
        msg.setWindowTitle("RF Conflict Warning")
        msg.setText(str(conflict.get("summary") or "RF conflict detected.").strip() or "RF conflict detected.")
        detail = str(conflict.get("detail") or "").strip()
        if detail:
            msg.setInformativeText(detail)
        proceed_btn = msg.addButton("Proceed QSY", QMessageBox.AcceptRole)
        msg.addButton("Cancel", QMessageBox.RejectRole)
        msg.exec()
        if msg.clickedButton() != proceed_btn:
            return False
        try:
            scheduler.apply_manual_qsy(entry, ignore_coordination_prompt=True)
        except TypeError:
            scheduler.apply_manual_qsy(entry)
        return True
    scheduler.apply_manual_qsy(entry)
    return True


def normalize_hold_minutes(value) -> int:
    try:
        mins = int(value)
    except Exception:
        mins = DEFAULT_HOLD_DURATION_MIN
    return mins if mins in HOLD_DURATION_PRESETS else DEFAULT_HOLD_DURATION_MIN


def get_hold_duration_default(settings) -> int:
    cached = _HOLD_DURATION_DEFAULT_CACHE.get("minutes")
    if isinstance(cached, int):
        return normalize_hold_minutes(cached)
    try:
        mins = normalize_hold_minutes(settings.get("schedule_hold_minutes_default", DEFAULT_HOLD_DURATION_MIN))
    except Exception:
        mins = DEFAULT_HOLD_DURATION_MIN
    _HOLD_DURATION_DEFAULT_CACHE["minutes"] = mins
    return mins


def set_hold_duration_default(settings, minutes: int) -> int:
    mins = normalize_hold_minutes(minutes)
    _HOLD_DURATION_DEFAULT_CACHE["minutes"] = mins
    try:
        if hasattr(settings, "set"):
            settings.set("schedule_hold_minutes_default", mins)
    except Exception:
        pass
    return mins


def _top_level_hold_window(window):
    if window is None:
        return None
    try:
        root = window.window() if hasattr(window, "window") else window
    except Exception:
        root = window
    return root


def notify_hold_state_changed(window, *, force_reload: bool = False) -> None:
    root = _top_level_hold_window(window)
    if root is None:
        return
    try:
        if hasattr(root, "on_hold_state_changed"):
            root.on_hold_state_changed(force_reload=force_reload)
    except Exception:
        pass


def notify_hold_duration_default_changed(window) -> None:
    root = _top_level_hold_window(window)
    if root is None:
        return
    try:
        if hasattr(root, "on_hold_duration_default_changed"):
            root.on_hold_duration_default_changed()
    except Exception:
        pass


def refresh_hold_duration_combo(combo: QComboBox, settings) -> None:
    current = get_hold_duration_default(settings)
    combo.blockSignals(True)
    combo.clear()
    for mins in HOLD_DURATION_PRESETS:
        combo.addItem(f"{mins} min", mins)
    idx = combo.findData(current)
    combo.setCurrentIndex(idx if idx >= 0 else 0)
    combo.blockSignals(False)


def selected_hold_duration(combo: Optional[QComboBox], settings) -> int:
    if combo is None:
        return get_hold_duration_default(settings)
    data = combo.currentData()
    if data is None:
        return get_hold_duration_default(settings)
    return normalize_hold_minutes(data)


def suspend_snapshot(settings, *, allow_reload: bool = True) -> Dict[str, object]:
    dt = get_suspend_until(settings, allow_reload=allow_reload)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    active = dt is not None and now_utc < dt
    if not active:
        return {
            "active": False,
            "until": dt,
            "remaining_sec": None,
            "remaining_minutes": 0,
            "severity": "idle",
            "about_to_resume": False,
        }
    remaining_sec = max(0.0, (dt - now_utc).total_seconds())
    remaining_minutes = max(0, int((remaining_sec + 59) // 60))
    severity = "info"
    if remaining_sec <= HOLD_CRITICAL_SECONDS:
        severity = "critical"
    elif remaining_sec <= HOLD_WARNING_SECONDS:
        severity = "warning"
    return {
        "active": True,
        "until": dt,
        "remaining_sec": remaining_sec,
        "remaining_minutes": remaining_minutes,
        "severity": severity,
        "about_to_resume": severity in {"warning", "critical"},
    }


def active_hold_button_role(remaining_sec: Optional[float]) -> str:
    if remaining_sec is None:
        return "info"
    if remaining_sec <= HOLD_CRITICAL_SECONDS:
        return "danger"
    if remaining_sec <= HOLD_WARNING_SECONDS:
        return "warning"
    return "info"


def active_hold_button_text(remaining_sec: Optional[float]) -> str:
    if remaining_sec is None:
        return "Resume Schedule"
    mins = max(0, int((float(remaining_sec) + 59) // 60))
    return f"Resume ({mins}m)" if mins else "Resume Schedule"


def active_hold_status_text(remaining_sec: Optional[float]) -> str:
    if remaining_sec is None:
        return "Schedule resumes automatically."
    mins = max(0, int((float(remaining_sec) + 59) // 60))
    return f"Schedule resumes automatically in {mins} min." if mins else "Schedule resumes automatically."


def set_active_hold_duration(
    window,
    settings,
    minutes: Optional[int] = None,
    *,
    notify: bool = True,
) -> int:
    mins = normalize_hold_minutes(minutes if minutes is not None else get_hold_duration_default(settings))
    until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=mins)
    try:
        scheduler = getattr(window, "scheduler", None)
    except Exception:
        scheduler = None
    if scheduler and hasattr(scheduler, "suspend_schedule"):
        scheduler.suspend_schedule(mins)
    else:
        set_suspend_until(settings, until)
    _SUSPEND_CACHE["ts"] = until.timestamp()
    _SUSPEND_CACHE["loaded_at"] = time.time()
    if notify:
        notify_hold_state_changed(window, force_reload=False)
    return mins


def suspend_schedule_hold(window, settings, minutes: Optional[int] = None) -> int:
    mins = set_active_hold_duration(window, settings, minutes=minutes, notify=False)
    notify_hold_state_changed(window, force_reload=False)
    return mins


def resume_schedule_hold(window, settings) -> bool:
    try:
        scheduler = getattr(window, "scheduler", None)
    except Exception:
        scheduler = None
    if scheduler and hasattr(scheduler, "resume_schedule"):
        scheduler.resume_schedule()
        _SUSPEND_CACHE["ts"] = 0
        _SUSPEND_CACHE["loaded_at"] = time.time()
        notify_hold_state_changed(window, force_reload=False)
        return True
    set_suspend_until(settings, None)
    notify_hold_state_changed(window, force_reload=False)
    return False


def perform_qsy_with_hold(window, settings, meta: Dict, minutes: Optional[int] = None) -> int:
    if not perform_qsy(window, meta):
        return 0
    return suspend_schedule_hold(window, settings, minutes=minutes)


# Suspend helpers (shared across tabs)
_SUSPEND_CACHE: Dict[str, Optional[float]] = {"ts": None, "loaded_at": 0.0}


def get_suspend_until(
    settings,
    max_age_sec: int = 10,
    *,
    allow_reload: bool = True,
) -> Optional[datetime.datetime]:
    """
    Read schedule_suspend_until (UTC timestamp) from settings, with a small cache to avoid frequent reloads.
    """
    now = time.time()
    cached_ts = _SUSPEND_CACHE.get("ts")
    loaded_at = _SUSPEND_CACHE.get("loaded_at", 0.0) or 0.0
    if cached_ts is not None and (not allow_reload or (now - loaded_at) < max_age_sec):
        return (
            datetime.datetime.fromtimestamp(cached_ts, tz=datetime.timezone.utc)
            if cached_ts > 0
            else None
        )
    if not allow_reload and cached_ts is None:
        return None
    try:
        if allow_reload and hasattr(settings, "reload"):
            settings.reload()
        ts = float(settings.get("schedule_suspend_until", 0) or 0)
        _SUSPEND_CACHE["ts"] = ts
        _SUSPEND_CACHE["loaded_at"] = now
        if ts > 0:
            return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    except Exception:
        pass
    return None


def set_suspend_until(settings, dt: Optional[datetime.datetime]) -> None:
    try:
        if hasattr(settings, "set"):
            ts = dt.timestamp() if dt else 0
            settings.set("schedule_suspend_until", ts)
            _SUSPEND_CACHE["ts"] = ts
            _SUSPEND_CACHE["loaded_at"] = time.time()
    except Exception:
        pass


def suspend_active(settings) -> bool:
    dt = get_suspend_until(settings)
    return dt is not None and datetime.datetime.now(datetime.timezone.utc) < dt


def set_scheduler_enabled_override(enabled: Optional[bool]) -> None:
    _SCHEDULER_ENABLED_OVERRIDE["enabled"] = None if enabled is None else bool(enabled)


def scheduler_enabled(settings) -> bool:
    override = _SCHEDULER_ENABLED_OVERRIDE.get("enabled")
    if override is not None:
        return bool(override)
    try:
        return bool(settings.get("use_scheduler", True))
    except Exception:
        return True
