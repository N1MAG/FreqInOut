from __future__ import annotations

from typing import Dict, List, Optional
import datetime
import time

from PySide6.QtWidgets import QComboBox, QMessageBox


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
            f"{g.get('group','')}|{g.get('mode','')}|{g.get('band','')}|{g.get('frequency','')}|{int(bool(g.get('auto_tune', False)))}"
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
        existing = meta.get(key)
        if existing:
            existing["auto_tune"] = existing.get("auto_tune", False) or auto
            if auto and g.get("mode"):
                existing["mode"] = g.get("mode", "")
            if auto and g.get("band"):
                existing["band"] = g.get("band", "")
        else:
            meta[key] = {
                "freq": fval,
                "mode": g.get("mode", ""),
                "band": g.get("band", ""),
                "auto_tune": auto,
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
    }
    scheduler.apply_manual_qsy(entry)
    return True


# Suspend helpers (shared across tabs)
_SUSPEND_CACHE: Dict[str, Optional[float]] = {"ts": None, "loaded_at": 0.0}


def get_suspend_until(settings, max_age_sec: int = 10) -> Optional[datetime.datetime]:
    """
    Read schedule_suspend_until (UTC timestamp) from settings, with a small cache to avoid frequent reloads.
    """
    now = time.time()
    cached_ts = _SUSPEND_CACHE.get("ts")
    loaded_at = _SUSPEND_CACHE.get("loaded_at", 0.0) or 0.0
    if cached_ts is not None and (now - loaded_at) < max_age_sec:
        return (
            datetime.datetime.fromtimestamp(cached_ts, tz=datetime.timezone.utc)
            if cached_ts > 0
            else None
        )
    try:
        if hasattr(settings, "reload"):
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


def scheduler_enabled(settings) -> bool:
    try:
        return bool(settings.get("use_scheduler", True))
    except Exception:
        return True
