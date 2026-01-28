from __future__ import annotations

import datetime
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from PySide6.QtCore import QObject, QTimer, Signal

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.rigctl_client import FLRigClient, FrequencyCommand
from freqinout.radio_interface.js8_status import JS8ControlClient


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _parse_hhmm_to_minutes(hhmm: str) -> Optional[int]:
    """
    Parse a time string "HH:MM" (24-hour) into minutes since midnight.

    Returns:
        int minutes, or None if invalid.
    """
    s = (hhmm or "").strip()
    if not s:
        return None
    try:
        hh, mm = s.split(":")
        hh_i = int(hh)
        mm_i = int(mm)
        if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
            return hh_i * 60 + mm_i
    except Exception:
        return None
    return None


def _python_weekday_to_day_name(weekday: int) -> str:
    """
    Convert datetime.weekday() (0=Monday..6=Sunday) into a day name we
    use in config ("Sunday".. "Saturday").
    """
    # datetime.weekday(): Monday=0 .. Sunday=6
    mapping = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    return mapping.get(weekday, "Sunday")


def _prev_day_name(day_name: str) -> str:
    order = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    try:
        idx = order.index(day_name)
    except ValueError:
        return "Saturday"
    return order[(idx - 1) % 7]


# ---------------------------------------------------------------------------
# Public helper (used by planner tabs etc.)
# ---------------------------------------------------------------------------


def compute_next_change_time(
    now_utc: datetime.datetime,
    hf_entry: Optional[Dict],
    net_entry: Optional[Dict],
) -> Optional[datetime.datetime]:
    """
    Compute the *next* UTC datetime at which the active schedule should
    change frequency, given the currently-active hf_entry and net_entry.

    The algorithm:

      - Each entry has start_utc and end_utc as "HH:MM" (UTC).
      - If now_utc is before the start time, the "change" is at the
        start time.
      - If now_utc is between start_utc and end_utc, the "change"
        is end_utc (i.e. stop using that entry).
      - If now_utc is after end_utc, the "change" is None for that entry.

    We compute this candidate time for both HF and Net entries and
    return the earliest non-None.
    """
    def next_for_entry(entry: Optional[Dict]) -> Optional[datetime.datetime]:
        if not entry:
            return None
        start_str = entry.get("start_utc") or ""
        end_str = entry.get("end_utc") or ""
        start_min = _parse_hhmm_to_minutes(start_str)
        end_min = _parse_hhmm_to_minutes(end_str)
        if start_min is None or end_min is None:
            return None

        now_min = now_utc.hour * 60 + now_utc.minute

        # Handle overnight windows (start > end = crosses midnight)
        if start_min <= end_min:
            # Before start => change at today's start time
            if now_min < start_min:
                change_min = start_min
            # Between start and end => change at today's end time
            elif start_min <= now_min < end_min:
                change_min = end_min
            else:
                return None
            day_offset = 0
        else:
            # Overnight: start today, end tomorrow
            end_min_ext = end_min + 24 * 60
            now_ext = now_min if now_min >= start_min else now_min + 24 * 60

            if now_ext < start_min:
                change_min = start_min
                day_offset = 0
            elif start_min <= now_ext < end_min_ext:
                change_min = end_min_ext
                day_offset = change_min // (24 * 60)
            else:
                return None

        change_hour = (change_min % (24 * 60)) // 60
        change_minute = change_min % 60
        return now_utc.replace(
            hour=change_hour,
            minute=change_minute,
            second=0,
            microsecond=0,
        ) + datetime.timedelta(days=day_offset)

    hf_next = next_for_entry(hf_entry)
    net_next = next_for_entry(net_entry)

    if hf_next and net_next:
        return hf_next if hf_next <= net_next else net_next
    return hf_next or net_next


# ---------------------------------------------------------------------------
# SchedulerEngine
# ---------------------------------------------------------------------------


class SchedulerEngine(QObject):
    """
    Central frequency scheduler.

    Responsibilities:

      - Load hf_schedule and net_schedule from SettingsManager.
      - Given "now" in UTC, determine:
          * Active HF schedule entry (if any).
          * Active Net schedule entry (if any).
          * The next transition time.
      - Optionally, drive FLRig (via FLRigClient) or JS8Call (via
        js8net-backed wrapper) to set frequency based on 'control_via' setting.

    The engine does **not** own the event loop; it exposes a periodic
    QTimer and emits signals so tabs can reflect the current/next info.

    IMPORTANT:
    When Settings 'control_via' is set to "Manual", the engine computes
    and exposes all schedule state but does not send any FrequencyCommand
    to FLRig/JS8Call. That allows full planner/Net Control UI without
    automatic rig control.
    """

    # Emitted whenever active entry or next change time updates
    active_entry_changed = Signal(dict, str)  # (entry, source: "HF" / "NET" / "NONE")
    next_change_updated = Signal(object)      # datetime or None
    off_schedule_detected = Signal(dict)

    def __init__(
        self,
        parent: Optional[QObject] = None,
        rig: Optional[FLRigClient] = None,
        js8: Optional[JS8ControlClient] = None,
        varac: Optional[object] = None,
        poll_interval_ms: int = 5_000,
    ) -> None:
        super().__init__(parent)
        self.settings = SettingsManager()
        self.rig: Optional[FLRigClient] = rig
        self.js8: Optional[JS8ControlClient] = js8
        self.varac: Optional[object] = varac

        # We keep a small cache of the last applied entry so we don't
        # spam the rig with identical commands.
        self._last_source: Optional[str] = None
        self._last_entry_key: Optional[Tuple] = None
        self._last_freq_hz: Optional[int] = None
        self._last_band: Optional[str] = None
        self._scheduled_vfo: Optional[str] = None
        self._last_js8_sync_ts: float = 0.0
        self._desired_fldigi_mode: Optional[str] = None
        self._desired_fldigi_offset: Optional[int] = None
        self._last_fldigi_apply: Optional[Tuple[Optional[str], Optional[int]]] = None
        self._fldigi_apply_after_ts: Optional[float] = None
        self._fldigi_was_available: bool = False
        self._fldigi_apply_pending: bool = False
        self._last_resync_ts: float = 0.0
        self._off_schedule_prompt_active: bool = False
        self._last_off_schedule_key: Optional[Tuple] = None
        self._control_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="freqinout-control")
        self._control_future = None
        self._control_backoff_until: float = 0.0
        self._control_fail_count: int = 0
        self._pending_entry_key: Optional[Tuple] = None

        self.current_source: str = "NONE"
        self.current_schedule_entry: Dict = {}
        self.next_change_utc: Optional[datetime.datetime] = None

        self.timer = QTimer(self)
        self.timer.setInterval(poll_interval_ms)
        self.timer.timeout.connect(self._on_timer)

        # If a rig was provided, we can optionally sanity-check it
        # (non-fatal if unavailable).
        if self.rig is not None:
            try:
                if hasattr(rig, "is_available") and not rig.is_available():
                    log.warning("SchedulerEngine: FLRig client is not available at init.")
            except Exception as e:
                log.error("SchedulerEngine: error probing FLRig availability: %s", e)
        self._ensure_js8_offset_default()

    # ------------------------------------------------------------------
    # Paths / DB helpers
    # ------------------------------------------------------------------

    def _config_dir(self) -> Path:
        """
        Return the config directory (where freqinout.db lives).
        """
        cfg = getattr(self.settings, "config_dir", None)
        try:
            if cfg:
                return Path(cfg)
        except Exception:
            pass

        try:
            return Path(__file__).resolve().parents[2] / "config"
        except Exception:
            return Path.cwd()

    def _db_mtime(self, path: Path) -> Optional[float]:
        try:
            return path.stat().st_mtime
        except Exception:
            return None

    def _table_exists(self, conn: sqlite3.Connection, name: str) -> bool:
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            )
            return cur.fetchone() is not None
        except Exception:
            return False

    def _table_has_columns(self, conn: sqlite3.Connection, table: str, columns: List[str]) -> bool:
        """
        True if all requested columns exist on the table.
        """
        try:
            cur = conn.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in cur.fetchall()}
            return all(col in existing for col in columns)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Control mode helper
    # ------------------------------------------------------------------

    def _control_mode(self) -> str:
        """
        Determine how (or if) we should control frequency:
          - MANUAL: compute schedule only, no rig commands.
          - FLRIG: use FLRigClient.
          - JS8CALL: use JS8ControlClient via js8net.
          - NONE: requested backend unavailable.
        """
        mode = (self.settings.get("control_via", "FLRig") or "FLRig").upper()
        if mode == "MANUAL":
            return "MANUAL"
        if mode == "FLRIG":
            return "FLRIG" if self.rig is not None else "NONE"
        if mode == "JS8CALL":
            return "JS8CALL" if self.js8 is not None else "NONE"
        return "NONE"

    def _js8_offset_setting(self) -> int:
        try:
            val = int(self.settings.get("js8_offset_hz", 0) or 0)
            return val
        except Exception:
            return 0

    def _parse_freq_hz(self, freq_text: str) -> Optional[int]:
        if not freq_text:
            return None
        try:
            normalized = freq_text.replace(",", ".").replace(" ", "")
            parts = normalized.split(".")
            if len(parts) > 2:
                normalized = parts[0] + "." + "".join(parts[1:])
            freq_mhz = float(normalized)
            return int(round(freq_mhz * 1_000_000))
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Begin periodic schedule evaluation."""
        if not self.timer.isActive():
            self.timer.start()
        self._apply_js8_offset_startup()
        # Perform an immediate evaluation so UI sees something right away.
        try:
            self._evaluate(now_utc=datetime.datetime.now(datetime.timezone.utc))
        except Exception as e:
            log.error("SchedulerEngine initial evaluate failed: %s", e)

    def stop(self) -> None:
        """Stop periodic schedule evaluation."""
        if self.timer.isActive():
            self.timer.stop()

    def _ensure_js8_offset_default(self) -> None:
        try:
            val = self.settings.get("js8_offset_hz", None)
        except Exception:
            val = None
        if val not in (None, "", 0):
            return
        now_utc = datetime.datetime.utcnow()
        offset = 1900 + (now_utc.hour % 7) * 50
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("js8_offset_hz", offset)
        except Exception:
            pass

    def _apply_js8_offset_startup(self) -> None:
        if not self.js8:
            return
        try:
            offset = int(self.settings.get("js8_offset_hz", 0) or 0)
        except Exception:
            offset = 0
        if offset <= 0:
            return
        try:
            self.js8.set_offset(offset)
        except Exception as e:
            log.debug("SchedulerEngine: JS8 offset startup apply failed: %s", e)

    # ------------------------------------------------------------------
    # Suspend helpers
    # ------------------------------------------------------------------

    def _suspend_until_dt(self) -> Optional[datetime.datetime]:
        try:
            ts = float(self.settings.get("schedule_suspend_until", 0) or 0)
            if ts > 0:
                return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        except Exception:
            return None
        return None

    def _scheduling_suspended(self, now_utc: datetime.datetime) -> bool:
        dt = self._suspend_until_dt()
        return dt is not None and now_utc < dt

    def force_refresh(self) -> None:
        """
        Force re-loading schedules from settings and reevaluating
        using the current UTC time.
        """
        self._evaluate(now_utc=datetime.datetime.now(datetime.timezone.utc), force=True)

    def _maybe_resync_js8(self) -> None:
        """
        Every ~60s, ensure JS8Call dial/offset match the active schedule entry.
        """
        return

    def _enforcement_mode(self) -> str:
        try:
            mode = (self.settings.get("schedule_enforcement_mode", "Strict") or "Strict").strip()
        except Exception:
            mode = "Strict"
        legacy_map = {"Shack": "Normal", "POTA": "Loose", "EmComms": "Strict"}
        mode = legacy_map.get(mode, mode)
        return mode if mode in {"Normal", "Loose", "Strict"} else "Strict"

    def _maybe_resync_frequency(self) -> None:
        if self._enforcement_mode() != "Strict":
            return
        entry = self.current_schedule_entry or {}
        if not entry:
            return
        if self.current_source == "NET":
            return
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if self._scheduling_suspended(now_utc):
            return
        now_ts = time.time()
        if now_ts - self._last_resync_ts < 15 * 60:
            return
        freq_hz = self._parse_freq_hz((entry.get("frequency") or "").strip())
        if not freq_hz:
            return
        cur = self._current_rig_frequency()
        if cur is not None and abs(cur - freq_hz) <= 5:
            self._last_resync_ts = now_ts
            return
        self._apply_schedule_entry(entry, self.current_source, force=True, apply_js8_offset=False, apply_fldigi=False)
        self._last_resync_ts = now_ts

    def _control_can_attempt(self) -> bool:
        return time.time() >= (self._control_backoff_until or 0.0)

    def _control_backoff(self) -> float:
        base = 5.0
        max_backoff = 300.0
        return min(base * (2 ** max(0, self._control_fail_count - 1)), max_backoff)

    def _queue_control_action(
        self,
        *,
        control_mode: str,
        entry_key: Tuple,
        source: str,
        freq_hz: int,
        band: str,
        vfo: Optional[str],
        auto_tune: bool,
        js8_offset: Optional[int],
        js8_group: str,
    ) -> bool:
        if not self._control_can_attempt():
            return False
        if self._control_future is not None and not self._control_future.done():
            return False
        if self._pending_entry_key == entry_key:
            return False
        self._pending_entry_key = entry_key

        def _task() -> bool:
            ok = False
            if control_mode == "JS8CALL":
                try:
                    if self.js8:
                        if js8_offset is None:
                            current_off = self.js8.get_offset()
                            ok = self.js8.set_frequency(freq_hz, offset_hz=current_off)
                        else:
                            ok = self.js8.set_frequency(freq_hz, offset_hz=js8_offset)
                except Exception as e:
                    log.error("SchedulerEngine: error sending set_frequency to JS8Call: %s", e)
            else:
                cmd = FrequencyCommand(
                    band=band,
                    rig_hz=freq_hz,
                    fldigi_center_hz=None,
                    js8_tune_hz=None,
                    vfo=vfo,
                    js8_group=js8_group or None,
                )
                try:
                    if self.rig:
                        ok = self.rig.set_frequency(cmd)
                except Exception as e:
                    log.error("SchedulerEngine: error sending set_frequency to FLRig: %s", e)
            if ok and control_mode == "FLRIG":
                if auto_tune:
                    try:
                        if self.rig and hasattr(self.rig, "tune"):
                            self.rig.tune()
                    except Exception as e:
                        log.error("SchedulerEngine: error invoking rig.tune(): %s", e)
                if self.js8:
                    try:
                        if js8_offset is None:
                            current_off = self.js8.get_offset()
                            self.js8.set_frequency(freq_hz, offset_hz=current_off)
                        else:
                            self.js8.set_frequency(freq_hz, offset_hz=js8_offset)
                    except Exception as e:
                        log.debug("SchedulerEngine: JS8Call set_frequency (FLRig control) failed: %s", e)
            return ok

        def _on_done(fut):
            def _apply_result():
                self._pending_entry_key = None
                ok = False
                try:
                    ok = bool(fut.result())
                except Exception as e:
                    log.error("SchedulerEngine: control task failed: %s", e)
                    ok = False
                if ok:
                    self._control_fail_count = 0
                    self._control_backoff_until = 0.0
                    self._last_entry_key = entry_key
                    self._last_source = source
                    self._last_freq_hz = freq_hz
                    self._last_band = band
                else:
                    self._control_fail_count += 1
                    backoff = self._control_backoff()
                    self._control_backoff_until = time.time() + backoff
                    log.warning(
                        "SchedulerEngine: control action failed; backing off %.1fs (failures=%d)",
                        backoff,
                        self._control_fail_count,
                    )
            QTimer.singleShot(0, _apply_result)

        self._control_future = self._control_executor.submit(_task)
        self._control_future.add_done_callback(_on_done)
        return True

    def _expected_fldigi_offset(self, entry: Dict) -> Optional[int]:
        og = self._resolve_operating_group(entry)
        if not isinstance(og, dict):
            return None
        txt = (og.get("fldigi_offset") or "").strip()
        if not txt:
            return None
        try:
            return int(txt)
        except Exception:
            return None

    def _current_fldigi_offset(self) -> Optional[int]:
        if not self.rig or not hasattr(self.rig, "get_fldigi_offset"):
            return None
        try:
            return self.rig.get_fldigi_offset()
        except Exception:
            return None

    def _maybe_prompt_off_schedule(self) -> None:
        if self._enforcement_mode() != "Loose":
            return
        entry = self.current_schedule_entry or {}
        if not entry:
            return
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if self._scheduling_suspended(now_utc):
            return
        freq_hz = self._parse_freq_hz((entry.get("frequency") or "").strip())
        if not freq_hz:
            return
        cur = self._current_rig_frequency()
        off_freq = cur is not None and abs(cur - freq_hz) > 5
        js8_off = False
        try:
            desired_js8 = self._js8_offset_setting()
            current_js8 = self.js8.get_offset() if self.js8 else None
            js8_off = current_js8 is not None and desired_js8 != current_js8
        except Exception:
            js8_off = False
        fldigi_off = False
        desired_fldigi = self._expected_fldigi_offset(entry)
        if desired_fldigi is not None:
            current_fldigi = self._current_fldigi_offset()
            fldigi_off = current_fldigi is not None and desired_fldigi != current_fldigi
        if not (off_freq or js8_off or fldigi_off):
            self._off_schedule_prompt_active = False
            self._last_off_schedule_key = None
            return
        key = (entry.get("frequency"), entry.get("band"), entry.get("mode"), entry.get("group_name"))
        if self._off_schedule_prompt_active and self._last_off_schedule_key == key:
            return
        self._off_schedule_prompt_active = True
        self._last_off_schedule_key = key
        self.off_schedule_detected.emit(entry)

    def resolve_off_schedule(self, action: str) -> None:
        self._off_schedule_prompt_active = False
        if action == "suspend":
            self._suspend_for_minutes(30)
            return
        if action == "apply":
            entry = self.current_schedule_entry or {}
            if entry:
                self._apply_schedule_entry(
                    entry,
                    self.current_source,
                    force=True,
                    apply_js8_offset=True,
                    apply_fldigi=True,
                )

    def _suspend_for_minutes(self, minutes: int) -> None:
        try:
            until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
            if hasattr(self.settings, "set"):
                self.settings.set("schedule_suspend_until", until.timestamp())
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Internal evaluation
    # ------------------------------------------------------------------

    def _on_timer(self) -> None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        try:
            self._evaluate(now_utc=now_utc)
            self._maybe_resync_frequency()
            self._maybe_apply_fldigi()
            self._maybe_prompt_off_schedule()
        except Exception as e:
            log.error("SchedulerEngine timer tick failed: %s", e)

    def _load_operating_groups(self) -> List[Dict]:
        data = self.settings.all()
        og = data.get("operating_groups", [])
        return og if isinstance(og, list) else []

    def _resolve_operating_group(self, entry: Dict) -> Optional[Dict]:
        group_name = (entry.get("group_name") or entry.get("group") or "").strip().upper()
        band = (entry.get("band") or "").strip().upper()
        mode = (entry.get("mode") or "").strip()
        if not group_name:
            return None
        candidates = []
        for g in self._load_operating_groups():
            if not isinstance(g, dict):
                continue
            g_name = (g.get("group") or "").strip().upper()
            if g_name != group_name:
                continue
            g_band = (g.get("band") or "").strip().upper()
            g_mode = (g.get("mode") or "").strip()
            candidates.append((g_band, g_mode, g))
        if not candidates:
            return None
        # Prefer exact band+mode match, then band-only, then first group match
        for g_band, g_mode, g in candidates:
            if g_band == band and g_mode == mode:
                return g
        for g_band, _g_mode, g in candidates:
            if g_band == band:
                return g
        return candidates[0][2]

    def _update_desired_fldigi_settings(self, entry: Dict) -> None:
        og = self._resolve_operating_group(entry) or {}
        mode = (og.get("fldigi_mode") or "").strip() or None
        offset_txt = (og.get("fldigi_offset") or "").strip()
        offset = None
        if offset_txt:
            try:
                offset = int(offset_txt)
            except Exception:
                offset = None
        self._desired_fldigi_mode = mode
        self._desired_fldigi_offset = offset
        if mode is None and offset is None:
            self._fldigi_apply_pending = False
        elif self._enforcement_mode() == "Normal":
            self._fldigi_apply_pending = True
        if mode or offset is not None:
            # Request immediate apply if FLDigi is already available.
            self._fldigi_apply_after_ts = 0.0
        else:
            self._fldigi_apply_after_ts = None

    def _maybe_apply_fldigi(self) -> None:
        if not self.rig:
            return
        try:
            if not bool(self.settings.get("use_scheduler", True)):
                return
        except Exception:
            pass
        if self._control_mode() in ("MANUAL", "NONE"):
            return
        if self._enforcement_mode() == "Normal" and not self._fldigi_apply_pending:
            return
        if not (self._desired_fldigi_mode or self._desired_fldigi_offset is not None):
            return
        available = self.rig.is_fldigi_available()
        now_ts = time.time()
        if not available:
            self._fldigi_was_available = False
            return
        if not self._fldigi_was_available:
            self._fldigi_was_available = True
            self._fldigi_apply_after_ts = now_ts + 5
            return
        if self._fldigi_apply_after_ts is not None and now_ts < self._fldigi_apply_after_ts:
            return
        desired = (self._desired_fldigi_mode, self._desired_fldigi_offset)
        if self._last_fldigi_apply == desired and self._fldigi_apply_after_ts is None:
            return
        if self.rig.set_fldigi_mode_offset(self._desired_fldigi_mode, self._desired_fldigi_offset):
            self._last_fldigi_apply = desired
            self._fldigi_apply_after_ts = None
            self._fldigi_apply_pending = False

    def _load_daily_schedule_from_db(self) -> Optional[List[Dict]]:
        """
        Read daily / HF schedule entries from SQLite table daily_schedule_tab.
        Returns None if DB/table is absent.
        """
        db_path = self._config_dir() / "freqinout.db"
        if not db_path.exists():
            return None

        conn = sqlite3.connect(db_path)
        try:
            if not self._table_exists(conn, "daily_schedule_tab"):
                return None

            new_cols = [
                "day_utc",
                "band",
                "mode",
                "vfo",
                "frequency",
                "start_utc",
                "end_utc",
                "group_name",
                "auto_tune",
            ]
            legacy_cols = [
                "day_utc",
                "band",
                "mode",
                "vfo",
                "frequency",
                "fldigi_offset",
                "js8_offset",
                "start_utc",
                "end_utc",
                "primary_js8call_group",
                "group_name",
                "comment",
                "auto_tune",
            ]

            if self._table_has_columns(conn, "daily_schedule_tab", new_cols):
                cur = conn.execute(
                    """
                    SELECT
                        day_utc,
                        band,
                        mode,
                        vfo,
                        frequency,
                        start_utc,
                        end_utc,
                        group_name,
                        auto_tune
                    FROM daily_schedule_tab
                    """
                )
                rows: List[Dict] = []
                for (
                    day_utc,
                    band,
                    mode,
                    vfo,
                    freq,
                    start_utc,
                    end_utc,
                    group_name,
                    auto_tune,
                ) in cur.fetchall():
                    rows.append(
                        {
                            "day_utc": day_utc or "ALL",
                            "band": band or "",
                            "mode": mode or "",
                            "vfo": (vfo or "A").strip().upper() or "A",
                            "frequency": str(freq or ""),
                            "fldigi_offset": "",
                            "js8_offset": "",
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "primary_js8call_group": "",
                            "group_name": group_name or "",
                            "comment": "",
                            "auto_tune": bool(auto_tune),
                        }
                    )
                return rows

            if self._table_has_columns(conn, "daily_schedule_tab", legacy_cols):
                cur = conn.execute(
                    """
                    SELECT
                        day_utc,
                        band,
                        mode,
                        vfo,
                        frequency,
                        fldigi_offset,
                        js8_offset,
                        start_utc,
                        end_utc,
                        primary_js8call_group,
                        group_name,
                        comment,
                        auto_tune
                    FROM daily_schedule_tab
                    """
                )
                rows: List[Dict] = []
                for (
                    day_utc,
                    band,
                    mode,
                    vfo,
                    freq,
                    fldigi_offset,
                    js8_offset,
                    start_utc,
                    end_utc,
                    primary_group,
                    group_name,
                    comment,
                    auto_tune,
                ) in cur.fetchall():
                    rows.append(
                        {
                            "day_utc": day_utc or "ALL",
                            "band": band or "",
                            "mode": mode or "",
                            "vfo": (vfo or "A").strip().upper() or "A",
                            "frequency": str(freq or ""),
                            "fldigi_offset": "",
                            "js8_offset": "",
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "primary_js8call_group": "",
                            "group_name": group_name or "",
                            "comment": "",
                            "auto_tune": bool(auto_tune),
                        }
                    )
                return rows

            log.error(
                "SchedulerEngine: daily_schedule_tab schema does not match new or legacy layouts in %s",
                db_path,
            )
            return None
        except Exception as e:
            log.error("SchedulerEngine: failed to load daily schedule from DB %s: %s", db_path, e)
            return None
        finally:
            conn.close()

    def _load_net_schedule_from_db(self) -> Optional[List[Dict]]:
        """
        Read net schedule entries from SQLite. Prefers the richer net_schedule_tab
        table (includes VFO) and falls back to legacy net_schedule.
        """
        db_path = self._config_dir() / "freqinout_nets.db"
        if not db_path.exists():
            return None

        conn = sqlite3.connect(db_path)
        try:
            rows: List[Dict] = []
            if self._table_exists(conn, "net_schedule_tab"):
                try:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            month_weeks,
                            band,
                            mode,
                            vfo,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            auto_tune,
                            primary_js8call_group,
                            comment,
                            net_name,
                            group_name
                        FROM net_schedule_tab
                        """
                    )
                except Exception:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            band,
                            mode,
                            vfo,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name
                        FROM net_schedule_tab
                        """
                    )
                for row in cur.fetchall():
                    if len(row) == 16:
                        (
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            month_weeks,
                            band,
                            mode,
                            vfo,
                            freq,
                            start_utc,
                            end_utc,
                            early,
                            auto_tune,
                            primary_group,
                            comment,
                            net_name,
                            group_name,
                        ) = row
                    else:
                        (
                            day_utc,
                            band,
                            mode,
                            vfo,
                            freq,
                            start_utc,
                            end_utc,
                            early,
                            primary_group,
                            comment,
                            net_name,
                        ) = row
                        recurrence = "Weekly"
                        biweekly_offset_weeks = 0
                        month_weeks = ""
                        group_name = ""
                    rows.append(
                        {
                            "day_utc": day_utc or "",
                            "recurrence": recurrence or "Weekly",
                            "biweekly_offset_weeks": int(biweekly_offset_weeks or 0),
                            "month_weeks": month_weeks or "",
                            "band": band or "",
                            "mode": mode or "",
                            "vfo": (vfo or "A").strip().upper() or "A",
                            "frequency": str(freq or ""),
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "early_checkin": early if early is not None else 0,
                            "auto_tune": bool(auto_tune) if len(row) == 15 else False,
                            "primary_js8call_group": primary_group or "",
                            "comment": comment or "",
                            "net_name": net_name or "",
                            "group_name": group_name or "",
                        }
                    )
                return rows

            if self._table_exists(conn, "net_schedule"):
                try:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            month_weeks,
                            band,
                            mode,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            auto_tune,
                            primary_js8call_group,
                            comment,
                            net_name,
                            group_name
                        FROM net_schedule
                        """
                    )
                except Exception:
                    cur = conn.execute(
                        """
                        SELECT
                            day_utc,
                            band,
                            mode,
                            frequency,
                            start_utc,
                            end_utc,
                            early_checkin,
                            primary_js8call_group,
                            comment,
                            net_name
                        FROM net_schedule
                        """
                    )
                for row in cur.fetchall():
                    if len(row) == 15:
                        (
                            day_utc,
                            recurrence,
                            biweekly_offset_weeks,
                            month_weeks,
                            band,
                            mode,
                            freq,
                            start_utc,
                            end_utc,
                            early,
                            auto_tune,
                            primary_group,
                            comment,
                            net_name,
                            group_name,
                        ) = row
                    else:
                        (
                            day_utc,
                            band,
                            mode,
                            freq,
                            start_utc,
                            end_utc,
                            early,
                            primary_group,
                            comment,
                            net_name,
                        ) = row
                        recurrence = "Weekly"
                        biweekly_offset_weeks = 0
                        month_weeks = ""
                        group_name = ""
                    rows.append(
                        {
                            "day_utc": day_utc or "",
                            "recurrence": recurrence or "Weekly",
                            "biweekly_offset_weeks": int(biweekly_offset_weeks or 0),
                            "month_weeks": month_weeks or "",
                            "band": band or "",
                            "mode": mode or "",
                            "vfo": "A",
                            "frequency": str(freq or ""),
                            "start_utc": start_utc or "",
                            "end_utc": end_utc or "",
                            "early_checkin": early if early is not None else 0,
                            "auto_tune": bool(auto_tune) if len(row) == 14 else False,
                            "primary_js8call_group": primary_group or "",
                            "comment": comment or "",
                            "net_name": net_name or "",
                            "group_name": group_name or "",
                        }
                    )
            return rows
        except Exception as e:
            log.error("SchedulerEngine: failed to load net schedule from DB %s: %s", db_path, e)
            return None
        finally:
            conn.close()

    def _load_schedules(self, *, force: bool = False) -> Tuple[List[Dict], List[Dict]]:
        """
        Load schedules, preferring the database tables and falling back to the
        SettingsManager key/value store for backwards compatibility.
        """
        cache = getattr(self, "_schedule_cache", None)
        config_db = self._config_dir() / "freqinout.db"
        nets_db = self._config_dir() / "freqinout_nets.db"
        mtimes = (self._db_mtime(config_db), self._db_mtime(nets_db))

        if cache and not force and cache.get("mtimes") == mtimes and cache.get("data"):
            return cache["data"]  # type: ignore[return-value]

        hf_db = self._load_daily_schedule_from_db()
        net_db = self._load_net_schedule_from_db()

        data = self.settings.all()
        hf = hf_db if hf_db is not None else data.get("hf_schedule") or data.get("daily_schedule") or []
        net = net_db if net_db is not None else data.get("net_schedule") or []

        if not isinstance(hf, list):
            hf = []
        if not isinstance(net, list):
            net = []

        self._schedule_cache = {"mtimes": mtimes, "data": (hf, net)}
        return hf, net

    def _evaluate(self, now_utc: datetime.datetime, force: bool = False) -> None:
        """
        Core evaluation step: decides which entry should be active (HF
        or Net), computes the next change time, and optionally drives
        the rig.
        """
        # Pick up any setting changes (control_via, use_scheduler, offsets, etc.)
        try:
            self.settings.reload()
        except Exception as e:
            log.debug("SchedulerEngine: settings reload failed (continuing with cached data): %s", e)

        hf_sched, net_sched = self._load_schedules(force=force)

        try:
            hf_active = self._find_active_hf_entry(now_utc, hf_sched)
        except Exception as e:
            log.error("SchedulerEngine: failed to evaluate HF schedule: %s", e)
            hf_active = None
        try:
            net_active = self._find_active_net_entry(now_utc, net_sched)
        except Exception as e:
            log.error("SchedulerEngine: failed to evaluate Net schedule: %s", e)
            net_active = None

        # Decide which source "wins" if both HF and Net have entries.
        source = "NONE"
        active_entry: Optional[Dict] = None
        if net_active and hf_active:
            # If both schedules have something active, we prefer Net
            # schedule (operator-level nets tend to be higher priority).
            source = "NET"
            active_entry = net_active
        elif net_active:
            source = "NET"
            active_entry = net_active
        elif hf_active:
            source = "HF"
            active_entry = hf_active

        # Compute next change moment.
        self.next_change_utc = compute_next_change_time(now_utc, hf_active, net_active)
        self.next_change_updated.emit(self.next_change_utc)

        if not active_entry:
            # No active schedule; if we previously had something applied,
            # we keep the rig where it was (no auto "clear") but still
            # notify UI that source is NONE.
            if source != self.current_source or force:
                self.current_source = "NONE"
                self.current_schedule_entry = {}
                self.active_entry_changed.emit({}, "NONE")
            return

        # Apply to rig (if needed) and emit active_entry_changed
        self._apply_schedule_entry(active_entry, source, now_utc=now_utc, force=force)

    def apply_manual_qsy(self, entry: Dict) -> None:
        """
        Apply an immediate user-driven QSY, bypassing suspend and force-applying the change.

        Expects entry to contain at least "frequency" (MHz). Mode/band/vfo/auto_tune
        are honored when provided.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        self._apply_schedule_entry(entry, "QSY", now_utc=now, force=True, ignore_suspend=True)

    # ------------------------------------------------------------------
    # Active entry lookup
    # ------------------------------------------------------------------

    def _find_active_hf_entry(
        self,
        now_utc: datetime.datetime,
        hf_sched: List[Dict],
    ) -> Optional[Dict]:
        if not hf_sched:
            return None

        weekday_name = _python_weekday_to_day_name(now_utc.weekday())
        weekday_upper = weekday_name.upper()
        now_min = now_utc.hour * 60 + now_utc.minute

        best: Optional[Dict] = None
        best_start_min = -1

        for row in hf_sched:
            try:
                day = (row.get("day_utc") or "ALL").strip().upper()

                smin = _parse_hhmm_to_minutes(row.get("start_utc", ""))
                emin = _parse_hhmm_to_minutes(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue

                overnight = smin > emin
                prev_day = _prev_day_name(weekday_name).upper()

                active = False
                if day == "ALL" or day == weekday_upper:
                    if not overnight:
                        active = smin <= now_min < emin
                    else:
                        # Starts today, ends tomorrow
                        active = now_min >= smin or now_min < emin
                elif day == prev_day and overnight:
                    # Overnight carry from previous day's entry into today's early hours
                    active = now_min < emin
                else:
                    active = False

                if active and smin > best_start_min:
                    best_start_min = smin
                    best = row
            except Exception:
                continue

        return best

    def _find_active_net_entry(
        self,
        now_utc: datetime.datetime,
        net_sched: List[Dict],
    ) -> Optional[Dict]:
        if not net_sched:
            return None

        weekday_name = _python_weekday_to_day_name(now_utc.weekday())
        weekday_upper = weekday_name.upper()
        prev_day_name = _prev_day_name(weekday_name).upper()
        now_min = now_utc.hour * 60 + now_utc.minute

        best: Optional[Dict] = None
        best_start_min = -1

        for row in net_sched:
            try:
                day = (row.get("day_utc") or "ALL").strip().upper()
                recurrence = (row.get("recurrence") or "Weekly").strip()
                if recurrence == "Monthly":
                    recurrence = "Periodic"
                month_weeks = self._parse_month_weeks(row.get("month_weeks", ""))

                smin = _parse_hhmm_to_minutes(row.get("start_utc", ""))
                emin = _parse_hhmm_to_minutes(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue

                early = int(row.get("early_checkin", 0) or 0)
                window_start = max(0, smin - early)
                overnight = smin > emin
                prev_day = prev_day_name

                active = False
                if recurrence == "Daily":
                    day = "ALL"
                if day == "ALL":
                    day = weekday_upper
                if not self._monthly_match(now_utc, day, prev_day, recurrence, month_weeks, overnight):
                    continue
                if day == weekday_upper:
                    if not overnight:
                        active = window_start <= now_min < emin
                    else:
                        # Starts today, ends tomorrow
                        active = now_min >= window_start or now_min < emin
                elif day == prev_day and overnight:
                    # Overnight carry from previous day into early hours
                    active = now_min < emin

                if active and smin > best_start_min:
                    best_start_min = smin
                    best = row
            except Exception:
                continue

        return best

    def _parse_month_weeks(self, txt: str) -> List[int]:
        weeks: List[int] = []
        for token in (txt or "").split(","):
            token = token.strip()
            if not token:
                continue
            try:
                val = int(token)
            except Exception:
                continue
            if 1 <= val <= 5:
                weeks.append(val)
        return sorted(set(weeks))

    def _month_week_index(self, date_val: datetime.date) -> int:
        return 1 + ((date_val.day - 1) // 7)

    def _monthly_match(
        self,
        now_utc: datetime.datetime,
        day: str,
        prev_day: str,
        recurrence: str,
        month_weeks: List[int],
        overnight: bool,
    ) -> bool:
        if recurrence != "Periodic":
            return True
        if not month_weeks:
            month_weeks = [1]
        today = now_utc.date()
        if day == _python_weekday_to_day_name(now_utc.weekday()).upper():
            return self._month_week_index(today) in month_weeks
        if overnight and day == prev_day:
            return self._month_week_index(today - datetime.timedelta(days=1)) in month_weeks
        return False

    # ------------------------------------------------------------------
    # Rig status helpers
    # ------------------------------------------------------------------

    def _current_rig_frequency(self) -> Optional[int]:
        """
        Query FLRig (preferred) or JS8Call for the current frequency in Hz.
        """
        try:
            if self.rig and hasattr(self.rig, "get_vfo_frequency"):
                freq = self.rig.get_vfo_frequency()
                if freq:
                    return freq
        except Exception as e:
            log.error("SchedulerEngine: failed to read current rig frequency: %s", e)

        try:
            if self.js8 and hasattr(self.js8, "get_frequency"):
                return self.js8.get_frequency()  # type: ignore[no-any-return]
        except Exception as e:
            log.debug("SchedulerEngine: failed to read JS8Call frequency: %s", e)
        return None

    # ------------------------------------------------------------------
    # Apply entry to rig
    # ------------------------------------------------------------------

    def _apply_schedule_entry(
        self,
        entry: Dict,
        source: str,
        *,
        now_utc: Optional[datetime.datetime] = None,
        force: bool = False,
        ignore_suspend: bool = False,
        apply_js8_offset: bool = True,
        apply_fldigi: bool = True,
    ) -> None:
        """
        Apply a single schedule entry to the rig.

        We avoid re-sending the same frequency/band data unless
        something actually changed, unless 'force' is True.
        """
        # Extract fields
        band = (entry.get("band") or "").strip().upper()
        freq_text = (entry.get("frequency") or "").strip()
        js8_group = (entry.get("primary_js8call_group") or "").strip()
        comment = (entry.get("comment") or "").strip()
        og = self._resolve_operating_group(entry)
        vfo_source = og.get("vfo") if isinstance(og, dict) else None
        vfo_raw = (vfo_source or entry.get("vfo") or "A").strip().upper()
        vfo: Optional[str] = vfo_raw if vfo_raw in ("A", "B") else None
        auto_tune = bool(entry.get("auto_tune"))

        # Update internal state regardless of whether we can actually
        # command the rig. This allows UI elements (Net Control tabs,
        # countdown timers, etc.) to reflect the upcoming change even
        # when running in Manual mode.
        self.current_source = source
        self.current_schedule_entry = entry
        self._scheduled_vfo = vfo

        control_mode = self._control_mode()
        # If we're not in JS8CALL mode and have no rig backend, just update UI state.
        if control_mode != "JS8CALL" and self.rig is None:
            self.active_entry_changed.emit(entry, source)
            return

        if control_mode == "MANUAL":
            log.debug("SchedulerEngine: manual control selected; no frequency commands sent.")
            self.active_entry_changed.emit(entry, source)
            return
        if control_mode == "NONE":
            log.debug(
                "SchedulerEngine: control backend unavailable for mode=%s; not sending commands.",
                self.settings.get("control_via", "FLRig"),
            )
            self.active_entry_changed.emit(entry, source)
            return
        # Respect temporary suspend timer (QSY/Suspend button)
        if not ignore_suspend and self._scheduling_suspended(now_utc or datetime.datetime.now(datetime.timezone.utc)):
            dt = self._suspend_until_dt()
            until_txt = dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%MZ") if dt else ""
            log.debug("SchedulerEngine: scheduling suspended until %s; skipping frequency change.", until_txt)
            self.active_entry_changed.emit(entry, source)
            return

        # Scheduler master switch (from Settings tab)
        try:
            if not bool(self.settings.get("use_scheduler", True)):
                log.debug("SchedulerEngine: scheduler disabled in settings; no frequency changes sent.")
                self.active_entry_changed.emit(entry, source)
                return
        except Exception:
            pass
        # Safety: avoid changing frequency while a backend is busy transmitting.
        busy_reasons = []
        if self.rig and hasattr(self.rig, "get_ptt"):
            try:
                if self.rig.get_ptt():
                    busy_reasons.append("FLRig PTT is active")
            except Exception as e:
                log.warning("SchedulerEngine: get_ptt() failed: %s", e)

        if self.js8 and hasattr(self.js8, "is_busy"):
            try:
                if self.js8.is_busy():
                    busy_reasons.append("JS8Call is busy (RX/TX)")
            except Exception as e:
                log.warning("SchedulerEngine: js8.is_busy() failed: %s", e)

        if self.varac and hasattr(self.varac, "is_busy"):
            try:
                if self.varac.is_busy():
                    busy_reasons.append("VarAC is busy")
            except Exception as e:
                log.warning("SchedulerEngine: varac.is_busy() failed: %s", e)

        if busy_reasons:
            log.warning(
                "SchedulerEngine: skipping frequency change for %s schedule due to activity: %s",
                source,
                "; ".join(busy_reasons),
            )
            self.active_entry_changed.emit(entry, source)
            return

        log.info(
            "SchedulerEngine applying entry (%s) from %s: band=%s freq=%s vfo=%s comment=%s",
            control_mode,
            source,
            band,
            freq_text,
            vfo or "-",
            comment,
        )

        # Parse frequency text to MHz
        if not freq_text:
            log.warning("SchedulerEngine: schedule entry missing 'frequency'; skipping.")
            return
        freq_hz = self._parse_freq_hz(freq_text)
        if freq_hz is None:
            log.error("SchedulerEngine: invalid frequency text '%s'; skipping.", freq_text)
            return

        fldigi_offset_text = (og.get("fldigi_offset") or "").strip() if isinstance(og, dict) else ""
        fldigi_center = None
        js8_tune = None
        try:
            if apply_fldigi and fldigi_offset_text:
                fldigi_center = int(float(fldigi_offset_text))
        except Exception:
            fldigi_center = None

        current_freq_hz = self._current_rig_frequency()

        # Avoid redundant commands
        entry_key = (
            band,
            freq_hz,
            fldigi_center,
            js8_tune,
            vfo,
            js8_group,
        )
        already_applied = (
            self._last_entry_key == entry_key and self._last_source == source
        )
        freq_matches = (
            current_freq_hz is None or abs(current_freq_hz - freq_hz) <= 5
        )
        if not force and already_applied:
            log.debug("SchedulerEngine: schedule entry already applied; skipping re-apply.")
            self.active_entry_changed.emit(entry, source)
            return
        if apply_fldigi:
            self._update_desired_fldigi_settings(entry)
        if current_freq_hz is not None and not freq_matches:
            log.info(
                "SchedulerEngine: rig currently at %d Hz, target %d Hz; reapplying schedule.",
                current_freq_hz,
                freq_hz,
            )

        ok = False
        js8_offset = self._js8_offset_setting() if apply_js8_offset else None
        queued = self._queue_control_action(
            control_mode=control_mode,
            entry_key=entry_key,
            source=source,
            freq_hz=freq_hz,
            band=band,
            vfo=vfo,
            auto_tune=auto_tune,
            js8_offset=js8_offset,
            js8_group=js8_group,
        )
        if not queued:
            log.debug("SchedulerEngine: control action skipped (pending/backoff).")
        # Update UI state immediately regardless of control action.
        self.active_entry_changed.emit(entry, source)
