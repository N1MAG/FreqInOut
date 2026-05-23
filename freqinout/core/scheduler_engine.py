from __future__ import annotations

import datetime
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

import psutil
from PySide6.QtCore import QCoreApplication, QObject, QTimer, Signal

from freqinout.core.logger import log
from freqinout.core.mode_utils import normalize_operating_group_mode, resolve_rig_mode
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


def _parse_iso_utc_to_epoch(value: object) -> float:
    txt = str(value or "").strip()
    if not txt:
        return 0.0
    try:
        dt_obj = datetime.datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=datetime.timezone.utc)
        else:
            dt_obj = dt_obj.astimezone(datetime.timezone.utc)
        return float(dt_obj.timestamp())
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Public helper (used by planner tabs etc.)
# ---------------------------------------------------------------------------


def compute_next_change_time(
    now_utc: datetime.datetime,
    hf_entry: Optional[Dict],
    net_entry: Optional[Dict],
    sop_entry: Optional[Dict] = None,
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

    candidates = [next_for_entry(hf_entry), next_for_entry(net_entry), next_for_entry(sop_entry)]
    candidates = [c for c in candidates if isinstance(c, datetime.datetime)]
    if not candidates:
        return None
    return min(candidates)


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
    active_entry_changed = Signal(dict, str)  # (entry, source: "HF" / "NET" / "SOP" / "NONE")
    next_change_updated = Signal(object)      # datetime or None
    off_schedule_detected = Signal(dict)
    off_schedule_cleared = Signal()
    varac_wait_detected = Signal(dict)
    varac_wait_cleared = Signal()
    _scheduler_thread_call = Signal(object)

    def __init__(
        self,
        parent: Optional[QObject] = None,
        rig: Optional[FLRigClient] = None,
        js8: Optional[JS8ControlClient] = None,
        varac: Optional[object] = None,
        fldigi_log: Optional[object] = None,
        poll_interval_ms: int = 5_000,
    ) -> None:
        super().__init__(parent)
        self._assert_scheduler_thread_contract()
        self.settings = SettingsManager()
        self._scheduler_thread_call.connect(self._run_scheduler_thread_call)
        self.rig: Optional[FLRigClient] = rig
        self.js8: Optional[JS8ControlClient] = js8
        self.varac: Optional[object] = varac
        self.fldigi_log: Optional[object] = fldigi_log

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
        self._fldigi_force_apply_once: bool = False
        self._prompt_active: bool = False
        self._prompt_items: List[str] = []
        self._prompt_entry_key: Optional[Tuple] = None
        self._prompt_state = {
            "frequency": {"last_prompt_ts": 0.0},
            "mode": {"last_prompt_ts": 0.0},
            "offset": {"last_prompt_ts": 0.0},
            "fldigi_offset": {"last_prompt_ts": 0.0},
        }
        self._last_fldigi_offset_prompt_sig: Optional[Tuple[Optional[int], Optional[int]]] = None
        self._varac_wait_prompt_active: bool = False
        self._varac_wait_prompt_entry_key: Optional[Tuple] = None
        self._proc_snapshot: List[str] = []
        self._proc_snapshot_ts: float = 0.0
        self._status_poll_ttl_s: float = 0.8
        self._status_poll_retry_s: float = 4.0
        self._status_flrig_freq_hz: Optional[int] = None
        self._status_flrig_freq_ts: float = 0.0
        self._status_flrig_ptt: bool = False
        self._status_flrig_ptt_ts: float = 0.0
        self._status_flrig_retry_ts: float = 0.0
        self._status_summary_cache: Optional[Dict[str, object]] = None
        self._status_summary_cache_ts: float = 0.0
        self._status_summary_cache_ttl_s: float = 2.5
        self._fldigi_mode_cache: Optional[str] = None
        self._fldigi_mode_cache_ts: float = 0.0
        self._fldigi_offset_cache: Optional[int] = None
        self._fldigi_offset_cache_ts: float = 0.0
        self._fldigi_status_cache_ttl_s: float = 5.0
        self._control_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="freqinout-control")
        self._control_future = None
        self._control_future_token: int = 0
        self._control_future_started_at: Optional[float] = None
        self._control_timeout_s: float = 8.0
        self._control_backoff_until: float = 0.0
        self._control_fail_count: int = 0
        self._pending_entry_key: Optional[Tuple] = None
        self._force_retry_after_control: bool = False
        self._forced_retry_attempts_left: int = 0
        self._latest_intent: Optional[Dict[str, object]] = None
        self._latest_intent_ts: float = 0.0
        self._retry_scheduled: bool = False
        self._manual_qsy_active: bool = False
        self._manual_qsy_entry_key: Optional[Tuple] = None
        self._manual_net_fldigi_active: bool = False
        self._manual_net_js8_active: bool = False
        self._net_schedule_active: bool = False
        self._net_fldigi_apply_allowed_once: bool = False
        self._net_resume_apply_once: bool = False
        self._net_schedule_started_at: Optional[float] = None
        self._net_schedule_entry_key: Optional[Tuple] = None
        self._last_off_schedule_flags = {
            "frequency": False,
            "mode": False,
            "offset": False,
            "fldigi_offset": False,
        }
        self._fldigi_available_cache: Optional[bool] = None
        self._fldigi_available_ts: float = 0.0
        self._fldigi_busy_entry_key: Optional[Tuple] = None
        self._fldigi_busy_since_ts: Optional[float] = None
        self._fldigi_busy_last_reason: Optional[str] = None

        self.current_source: str = "NONE"
        self.current_schedule_entry: Dict = {}
        self.next_change_utc: Optional[datetime.datetime] = None
        self._sop_contention: bool = False
        self._sop_contention_profiles: List[str] = []
        self._sop_winner_profile: str = ""
        self._sop_winner_priority: int = 100
        self._sop_winner_reason_code: str = ""
        self._sop_winner_reason_detail: str = ""
        self._source_reason_code: str = ""
        self._source_reason_detail: str = ""
        self._next_source: str = "NONE"
        self._next_net_kind: str = ""
        self._next_transition_freq_hz: Optional[int] = None
        self._next_transition_utc: Optional[datetime.datetime] = None
        self._next_transition_note: str = ""
        self._next_source_change: bool = False
        self._last_scheduler_selection_sig: Optional[Tuple] = None
        self._shutdown_requested: bool = False

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

    def _run_scheduler_thread_call(self, callback: object) -> None:
        if not callable(callback):
            return
        try:
            callback()
        except Exception as e:
            log.debug("SchedulerEngine: queued scheduler-thread callback failed: %s", e)

    def _queue_scheduler_thread_call(self, callback: Callable[[], None]) -> None:
        self._scheduler_thread_call.emit(callback)

    def set_manual_net_active(self, kind: str, active: bool) -> None:
        """
        Track manual net sessions started from NCS tabs.

        kind: "FLDIGI" or "JS8"
        """
        key = (kind or "").strip().upper()
        if key == "FLDIGI":
            self._manual_net_fldigi_active = bool(active)
        elif key == "JS8":
            self._manual_net_js8_active = bool(active)
        if active:
            self._net_fldigi_apply_allowed_once = False
        if not active:
            try:
                self.force_refresh()
            except Exception:
                pass
            # End of net should resume schedule enforcement fully.
            self._manual_qsy_active = False
            self._manual_qsy_entry_key = None
            self._control_backoff_until = 0.0
            self._control_fail_count = 0
            self._pending_entry_key = None
            self._reset_control_if_running("end net (force resume)")
            self._force_retry_after_control = True
            self._forced_retry_attempts_left = 5
            self._net_resume_apply_once = True
            self.apply_current_entry(
                force=True,
                ignore_wait_prompt=True,
                ignore_suspend=True,
                ignore_net_suppression=True,
            )
            self._maybe_apply_fldigi()
            self._net_resume_apply_once = False
            self._schedule_forced_retry()

    def _net_corrections_suppressed(self) -> bool:
        return bool(self._net_schedule_active or self._manual_net_fldigi_active or self._manual_net_js8_active)

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
          - FLRIG: use FLRigClient when flrig process is running.
          - JS8CALL: use JS8ControlClient when js8call process is running.
          - NONE: requested backend unavailable.
        """
        mode = (self.settings.get("control_via", "FLRig") or "FLRig").upper()
        if mode == "MANUAL":
            return "MANUAL"
        if mode == "FLRIG":
            if self.rig is None:
                return "NONE"
            return "FLRIG" if self._flrig_running() else "NONE"
        if mode == "JS8CALL":
            if self.js8 is None:
                return "NONE"
            return "JS8CALL" if self._js8_running() else "NONE"
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
        self._assert_scheduler_thread_contract()
        if self._shutdown_requested:
            self._control_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="freqinout-control",
            )
        self._shutdown_requested = False
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
        self._shutdown_requested = True
        if self.timer.isActive():
            self.timer.stop()
        self._latest_intent = None
        self._latest_intent_ts = 0.0
        self._retry_scheduled = False
        self._force_retry_after_control = False
        self._forced_retry_attempts_left = 0
        self._shutdown_control_executor("stop")

    def _ensure_js8_offset_default(self) -> None:
        try:
            val = self.settings.get("js8_offset_hz", None)
        except Exception:
            val = None
        if val not in (None, "", 0):
            return
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        offset = 1900 + (now_utc.hour % 7) * 50
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("js8_offset_hz", offset)
        except Exception:
            pass

    def _assert_scheduler_thread_contract(self) -> None:
        """
        SchedulerEngine and its SettingsManager are expected to live on the Qt
        app thread. Constructing or starting the engine elsewhere can trip the
        settings thread-affinity guard and produce hard-to-diagnose behavior.
        """
        app = QCoreApplication.instance()
        if app is None:
            return
        app_thread = app.thread()
        engine_thread = self.thread()
        if app_thread is None or engine_thread is None or engine_thread == app_thread:
            return
        raise RuntimeError("SchedulerEngine must be constructed and started on the Qt application thread.")

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
                dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
                if datetime.datetime.now(datetime.timezone.utc) >= dt:
                    try:
                        if hasattr(self.settings, "set"):
                            self.settings.set("schedule_suspend_until", 0)
                    except Exception:
                        pass
                    self._manual_qsy_active = False
                    self._manual_qsy_entry_key = None
                    return None
                return dt
        except Exception:
            return None
        return None

    @staticmethod
    def _normalize_hold_minutes(value: object) -> int:
        try:
            mins = int(value)
        except Exception:
            mins = 30
        return mins if mins in {30, 60, 90, 120} else 30

    def _default_hold_minutes(self) -> int:
        try:
            return self._normalize_hold_minutes(self.settings.get("schedule_hold_minutes_default", 30))
        except Exception:
            return 30

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

    def _control_can_attempt(self) -> bool:
        return time.time() >= (self._control_backoff_until or 0.0)

    def _control_backoff(self) -> float:
        base = 5.0
        max_backoff = 300.0
        return min(base * (2 ** max(0, self._control_fail_count - 1)), max_backoff)

    def _record_latest_intent(
        self,
        entry: Dict,
        source: str,
        *,
        now_utc: Optional[datetime.datetime] = None,
        force: bool = False,
        ignore_suspend: bool = False,
        ignore_wait_prompt: bool = False,
        ignore_fldigi_busy: bool = False,
        apply_js8_offset: bool = True,
        apply_fldigi: bool = True,
    ) -> None:
        self._latest_intent = {
            "entry": dict(entry),
            "source": source,
            "now_utc": now_utc,
            "force": bool(force),
            "ignore_suspend": bool(ignore_suspend),
            "ignore_wait_prompt": bool(ignore_wait_prompt),
            "ignore_fldigi_busy": bool(ignore_fldigi_busy),
            "apply_js8_offset": bool(apply_js8_offset),
            "apply_fldigi": bool(apply_fldigi),
        }
        self._latest_intent_ts = time.time()

    def _apply_latest_intent_if_any(self) -> bool:
        intent = self._latest_intent
        if not intent:
            return False
        self._latest_intent = None
        entry = intent.get("entry") or {}
        if not isinstance(entry, dict) or not entry:
            return False
        now_utc = intent.get("now_utc")
        if not isinstance(now_utc, datetime.datetime):
            now_utc = datetime.datetime.now(datetime.timezone.utc)
        self._apply_schedule_entry(
            entry,
            intent.get("source") or self.current_source or "NONE",
            now_utc=now_utc,
            force=bool(intent.get("force")),
            ignore_suspend=bool(intent.get("ignore_suspend")),
            ignore_wait_prompt=bool(intent.get("ignore_wait_prompt")),
            ignore_fldigi_busy=bool(intent.get("ignore_fldigi_busy")),
            apply_js8_offset=bool(intent.get("apply_js8_offset")),
            apply_fldigi=bool(intent.get("apply_fldigi")),
        )
        return True

    def _reset_control_executor(self, reason: str) -> None:
        self._shutdown_control_executor(f"reset ({reason})")
        self._control_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="freqinout-control",
        )
        self._control_future = None
        self._control_future_started_at = None
        self._pending_entry_key = None
        self._control_backoff_until = 0.0
        self._control_fail_count = 0
        log.warning("SchedulerEngine: control executor reset (%s).", reason)

    def _shutdown_control_executor(self, reason: str) -> None:
        self._control_future_token += 1
        future = self._control_future
        if future is not None and not future.done():
            try:
                future.cancel()
            except Exception as e:
                log.debug("SchedulerEngine: control future cancel failed during %s: %s", reason, e)
        try:
            self._control_executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            try:
                self._control_executor.shutdown(wait=False)
            except Exception as e:
                log.debug("SchedulerEngine: control executor shutdown failed during %s: %s", reason, e)
        except Exception as e:
            log.debug("SchedulerEngine: control executor shutdown failed during %s: %s", reason, e)
        self._control_future = None
        self._control_future_started_at = None
        self._pending_entry_key = None

    def _reset_control_if_running(self, reason: str) -> None:
        """
        Force-clear any in-flight control task so an immediate user action
        (resume schedule / end net) can apply without waiting on backoff.
        """
        if self._control_future is None:
            return
        if not self._control_future.done():
            self._reset_control_executor(reason)

    def _control_future_stuck(self) -> bool:
        if self._control_future is None or self._control_future.done():
            return False
        started_at = self._control_future_started_at or 0.0
        return (time.time() - started_at) > self._control_timeout_s

    def _queue_control_action(
        self,
        *,
        control_mode: str,
        entry_key: Tuple,
        source: str,
        freq_hz: int,
        band: str,
        mode: Optional[str],
        vfo: Optional[str],
        auto_tune: bool,
        js8_offset: Optional[int],
        js8_group: str,
    ) -> bool:
        if self._shutdown_requested:
            log.debug("SchedulerEngine: control action skipped (shutdown requested).")
            return False
        if not self._control_can_attempt():
            log.debug("SchedulerEngine: control action skipped (backoff active).")
            return False
        if self._control_future is not None and not self._control_future.done():
            if self._control_future_stuck():
                self._reset_control_executor("timeout waiting for control task")
            else:
                log.debug("SchedulerEngine: control action skipped (control task running).")
                return False
        if self._pending_entry_key == entry_key:
            log.debug("SchedulerEngine: control action skipped (pending entry key).")
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
                    mode=mode,
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

        self._control_future_token += 1
        control_future_token = self._control_future_token

        def _on_done(fut):
            def _apply_result():
                if self._shutdown_requested or control_future_token != self._control_future_token:
                    return
                self._control_future = None
                self._pending_entry_key = None
                self._control_future_started_at = None
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
                if self._latest_intent:
                    self._force_retry_after_control = False
                    QTimer.singleShot(0, self._apply_latest_intent_if_any)
                elif self._force_retry_after_control:
                    self._force_retry_after_control = False
                    QTimer.singleShot(
                        0,
                        lambda: self.apply_current_entry(
                            force=True,
                            ignore_wait_prompt=True,
                            ignore_suspend=True,
                        ),
                    )
            self._queue_scheduler_thread_call(_apply_result)

        self._control_future = self._control_executor.submit(_task)
        self._control_future_started_at = time.time()
        self._control_future.add_done_callback(_on_done)
        return True

    def _expected_fldigi_offset(self, entry: Dict) -> Optional[int]:
        txt_entry = (entry.get("fldigi_offset") or "").strip()
        if txt_entry:
            try:
                return int(float(txt_entry))
            except Exception:
                pass
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

    def _expected_fldigi_mode(self, entry: Dict) -> Optional[str]:
        mode_entry = (entry.get("fldigi_mode") or "").strip()
        if mode_entry:
            return mode_entry
        og = self._resolve_operating_group(entry)
        if not isinstance(og, dict):
            return None
        mode = (og.get("fldigi_mode") or "").strip()
        return mode or None

    def _current_fldigi_offset(self) -> Optional[int]:
        if not self.rig or not hasattr(self.rig, "get_fldigi_offset"):
            return None
        now_ts = time.time()
        if now_ts - self._fldigi_offset_cache_ts < self._fldigi_status_cache_ttl_s:
            return self._fldigi_offset_cache
        try:
            offset = self.rig.get_fldigi_offset()
        except Exception:
            return None
        self._fldigi_offset_cache = offset
        self._fldigi_offset_cache_ts = now_ts
        return offset

    def _fldigi_available(self) -> bool:
        if not self.rig or not hasattr(self.rig, "is_fldigi_available"):
            return False
        now_ts = time.time()
        if now_ts - self._fldigi_available_ts < 5.0 and self._fldigi_available_cache is not None:
            return self._fldigi_available_cache
        try:
            available = bool(self.rig.is_fldigi_available())
        except Exception:
            available = False
        self._fldigi_available_cache = available
        self._fldigi_available_ts = now_ts
        return available

    def _current_fldigi_mode(self) -> Optional[str]:
        if not self.rig or not hasattr(self.rig, "get_fldigi_mode"):
            return None
        now_ts = time.time()
        if now_ts - self._fldigi_mode_cache_ts < self._fldigi_status_cache_ttl_s:
            return self._fldigi_mode_cache
        try:
            mode = self.rig.get_fldigi_mode()
        except Exception:
            return None
        normalized = mode.strip().upper() if isinstance(mode, str) else None
        self._fldigi_mode_cache = normalized
        self._fldigi_mode_cache_ts = now_ts
        return normalized

    def _enforcement_mode(self, key: str, default: str = "On Schedule Change") -> str:
        try:
            raw = (self.settings.get(key, default) or default).strip()
        except Exception:
            raw = default
        if raw not in {"On Schedule Change", "Prompt"}:
            return default
        return raw

    def _prompt_interval_minutes(self, key: str, default: int = 60) -> int:
        try:
            raw = (self.settings.get(key, "Hourly") or "Hourly").strip()
        except Exception:
            raw = "Hourly"
        mapping = {
            "Hourly": 60,
            "Every 5 minutes": 5,
            "Every 10 minutes": 10,
            "Every 15 minutes": 15,
            "Every 30 minutes": 30,
        }
        return mapping.get(raw, default)

    def _prompt_next_due_utc(
        self, now_utc: datetime.datetime, flags: Dict[str, bool]
    ) -> Optional[datetime.datetime]:
        candidates: List[datetime.datetime] = []
        prompt_specs = [
            ("frequency", "freq_enforcement_mode", "freq_prompt_interval"),
            ("mode", "fldigi_enforcement_mode", "fldigi_prompt_interval"),
            ("fldigi_offset", "fldigi_enforcement_mode", "fldigi_prompt_interval"),
            ("offset", "js8_enforcement_mode", "js8_prompt_interval"),
        ]
        for flag_key, mode_key, interval_key in prompt_specs:
            if not bool(flags.get(flag_key)):
                continue
            if self._enforcement_mode(mode_key) != "Prompt":
                continue
            interval_mins = self._prompt_interval_minutes(interval_key)
            try:
                state = self._prompt_state.get(flag_key, {})
                last_ts = float(state.get("last_prompt_ts") or 0.0)
            except Exception:
                last_ts = 0.0
            if last_ts <= 0.0:
                due_utc = now_utc
            else:
                due_utc = datetime.datetime.fromtimestamp(last_ts, tz=datetime.timezone.utc)
                due_utc = due_utc + datetime.timedelta(minutes=interval_mins)
                if due_utc < now_utc:
                    due_utc = now_utc
            candidates.append(due_utc)
        if not candidates:
            return None
        return min(candidates)

    def _auto_resume_utc(
        self,
        now_utc: datetime.datetime,
        suspended_until: Optional[datetime.datetime],
        flags: Dict[str, bool],
    ) -> Tuple[Optional[datetime.datetime], str]:
        if isinstance(suspended_until, datetime.datetime):
            if suspended_until.tzinfo is None:
                suspended_until = suspended_until.replace(tzinfo=datetime.timezone.utc)
            else:
                suspended_until = suspended_until.astimezone(datetime.timezone.utc)
            return suspended_until, "suspend"
        prompt_due = self._prompt_next_due_utc(now_utc, flags)
        if isinstance(prompt_due, datetime.datetime):
            return prompt_due, "prompt"
        next_change = self.next_change_utc
        if isinstance(next_change, datetime.datetime):
            if next_change.tzinfo is None:
                next_change = next_change.replace(tzinfo=datetime.timezone.utc)
            else:
                next_change = next_change.astimezone(datetime.timezone.utc)
            return next_change, "schedule"
        return None, "none"

    def _refresh_proc_snapshot(self) -> None:
        now_ts = time.time()
        if now_ts - self._proc_snapshot_ts < 2.0:
            return
        snap: List[str] = []
        for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
            try:
                name = (proc.info.get("name") or "").lower()
                exe_path = (proc.info.get("exe") or "")
                exe = exe_path.lower()
                exe_base = Path(exe_path).name.lower() if exe_path else ""
                cmdline_list = proc.info.get("cmdline") or []
                first_arg = (cmdline_list[0] if cmdline_list else "")
                cmd_base = Path(first_arg).name.lower() if first_arg else ""
                for token in (name, exe, exe_base, cmd_base):
                    if token:
                        snap.append(token)
            except Exception:
                continue
        self._proc_snapshot = snap
        self._proc_snapshot_ts = now_ts

    def _process_running(self, name: str) -> bool:
        target = (name or "").strip().lower()
        if not target:
            return False
        self._refresh_proc_snapshot()
        targets = {target, f"{target}.exe"}
        return any(entry in targets for entry in self._proc_snapshot)

    def _js8_running(self) -> bool:
        return self._process_running("js8call")

    def _flrig_running(self) -> bool:
        return self._process_running("flrig")

    def _varac_running(self) -> bool:
        return self._process_running("varac")

    def _js8_busy_ok(self) -> bool:
        if not self.js8:
            return True
        if not self._js8_running():
            return True
        try:
            return not self.js8.is_busy()
        except Exception:
            return True

    def _varac_status(self) -> Dict[str, object]:
        if not self.varac or not self._varac_running():
            return {"busy": False, "waiting_for_frequency": False, "reason": None}
        if hasattr(self.varac, "get_status"):
            try:
                status = self.varac.get_status()
                if isinstance(status, dict):
                    return status
            except Exception:
                return {"busy": False, "waiting_for_frequency": False, "reason": None}
        try:
            return {"busy": bool(self.varac.is_busy()), "waiting_for_frequency": False, "reason": None}
        except Exception:
            return {"busy": False, "waiting_for_frequency": False, "reason": None}

    def _varac_busy_ok(self, status: Optional[Dict[str, object]] = None) -> bool:
        status = status or self._varac_status()
        try:
            return not bool(status.get("busy"))
        except Exception:
            return True

    def _fldigi_log_status(self) -> Dict[str, object]:
        if not self.fldigi_log:
            return {"busy": False, "reason": None, "last_valid_age_s": None}
        if hasattr(self.fldigi_log, "get_status"):
            try:
                status = self.fldigi_log.get_status()
                if isinstance(status, dict):
                    return status
                if hasattr(status, "busy"):
                    return {
                        "busy": bool(getattr(status, "busy", False)),
                        "reason": getattr(status, "reason", None),
                        "last_valid_age_s": getattr(status, "last_valid_age_s", None),
                    }
            except Exception:
                return {"busy": False, "reason": None, "last_valid_age_s": None}
        try:
            return {"busy": bool(self.fldigi_log.is_busy()), "reason": None, "last_valid_age_s": None}
        except Exception:
            return {"busy": False, "reason": None, "last_valid_age_s": None}

    def _should_delay_for_fldigi(
        self,
        *,
        entry_key: Tuple,
        source: str,
        want_freq_change: bool,
        ignore_fldigi_busy: bool,
        now_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        if ignore_fldigi_busy or not want_freq_change:
            return False, None
        if self._manual_net_fldigi_active or self._manual_net_js8_active:
            return False, None
        if (source or "").upper() == "NET":
            return False, None
        if not self._fldigi_available():
            return False, None
        status = self._fldigi_log_status()
        busy = bool(status.get("busy"))
        if not busy:
            self._fldigi_busy_entry_key = None
            self._fldigi_busy_since_ts = None
            self._fldigi_busy_last_reason = None
            return False, None
        now_ts = now_ts if now_ts is not None else time.time()
        if entry_key != self._fldigi_busy_entry_key:
            self._fldigi_busy_entry_key = entry_key
            self._fldigi_busy_since_ts = now_ts
        self._fldigi_busy_last_reason = str(status.get("reason") or "") or None
        if (source or "").upper() == "HF":
            since = self._fldigi_busy_since_ts or now_ts
            if now_ts - since > 600:
                # Max 10-minute delay for HF schedule changes.
                self._fldigi_busy_entry_key = None
                self._fldigi_busy_since_ts = None
                self._fldigi_busy_last_reason = None
                return False, None
        return True, self._fldigi_busy_last_reason or "RX activity"

    def apply_current_entry(
        self,
        *,
        force: bool = False,
        ignore_wait_prompt: bool = False,
        ignore_suspend: bool = False,
        ignore_net_suppression: bool = False,
        ignore_fldigi_busy: bool = False,
        apply_js8_offset: bool = True,
        apply_fldigi: bool = True,
    ) -> None:
        entry = self.current_schedule_entry or {}
        if not entry:
            return
        source = self.current_source or "NONE"
        now = datetime.datetime.now(datetime.timezone.utc)
        self._apply_schedule_entry(
            entry,
            source,
            now_utc=now,
            force=force,
            ignore_wait_prompt=ignore_wait_prompt,
            ignore_suspend=ignore_suspend,
            ignore_net_suppression=ignore_net_suppression,
            ignore_fldigi_busy=ignore_fldigi_busy,
            apply_js8_offset=apply_js8_offset,
            apply_fldigi=apply_fldigi,
        )

    def resolve_varac_wait(self, action: str, minutes: Optional[int] = None) -> None:
        self._varac_wait_prompt_active = False
        self._varac_wait_prompt_entry_key = None
        self.varac_wait_cleared.emit()
        if action == "apply":
            self.apply_current_entry(
                force=True,
                ignore_wait_prompt=True,
                ignore_suspend=True,
                ignore_fldigi_busy=True,
            )
        elif action == "suspend":
            self._suspend_for_minutes(self._normalize_hold_minutes(minutes if minutes is not None else self._default_hold_minutes()))

    def resume_schedule(self) -> None:
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("schedule_suspend_until", 0)
        except Exception:
            pass
        self._manual_qsy_active = False
        self._manual_qsy_entry_key = None
        self._prompt_active = False
        self._prompt_items = []
        self._prompt_entry_key = None
        self._reset_prompt_timers()
        resume_skip_fldigi_apply = False
        entry = self.current_schedule_entry or {}
        if entry:
            try:
                effective_entry, _og = self._entry_with_operating_group_overrides(entry)
                band = (effective_entry.get("band") or "").strip().upper()
                freq_hz = self._parse_freq_hz((effective_entry.get("frequency") or "").strip())
                js8_group = (effective_entry.get("primary_js8call_group") or "").strip()
                vfo_raw = (effective_entry.get("vfo") or "A").strip().upper()
                vfo = vfo_raw if vfo_raw in ("A", "B") else None
                rig_mode = self._resolve_rig_mode(effective_entry)
                resume_entry_key = (
                    band,
                    freq_hz,
                    self._expected_fldigi_offset(effective_entry),
                    self._js8_offset_setting(),
                    vfo,
                    js8_group,
                    rig_mode,
                )
                # Resume should restore scheduler activity, but avoid re-forcing FLDigi
                # for the already-current entry so offset drift stays notify-only.
                same_source = (self._last_source or "") == (self.current_source or "NONE")
                resume_skip_fldigi_apply = bool(
                    same_source
                    and (
                        self._last_entry_key == resume_entry_key
                        or self._last_entry_matches_schedule_identity(effective_entry)
                    )
                )
            except Exception:
                resume_skip_fldigi_apply = False
        self._fldigi_force_apply_once = not resume_skip_fldigi_apply
        self._latest_intent = None
        self._latest_intent_ts = 0.0
        self._retry_scheduled = False
        self._control_backoff_until = 0.0
        self._control_fail_count = 0
        self._pending_entry_key = None
        self._force_retry_after_control = True
        self._forced_retry_attempts_left = 5
        self._reset_control_if_running("resume schedule (force apply)")
        if self._control_future_stuck():
            self._reset_control_executor("resume schedule (stuck control task)")
        self._net_resume_apply_once = True
        self.apply_current_entry(
            force=True,
            ignore_wait_prompt=True,
            ignore_suspend=True,
            ignore_net_suppression=True,
            ignore_fldigi_busy=True,
            apply_fldigi=not resume_skip_fldigi_apply,
        )
        if resume_skip_fldigi_apply:
            self._fldigi_apply_pending = False
        if not resume_skip_fldigi_apply:
            self._maybe_apply_fldigi()
        self._net_resume_apply_once = False
        self._schedule_forced_retry()

    def suspend_schedule(self, minutes: Optional[int] = None) -> None:
        """
        Suspend schedule-driven corrections for the requested duration.

        This is intended for user-invoked temporary holds from global UI controls.
        """
        mins = self._normalize_hold_minutes(minutes if minutes is not None else self._default_hold_minutes())
        self._prompt_active = False
        self._prompt_items = []
        self._prompt_entry_key = None
        self._varac_wait_prompt_active = False
        self._varac_wait_prompt_entry_key = None
        self._reset_prompt_timers()
        try:
            self.off_schedule_cleared.emit()
        except Exception:
            pass
        try:
            self.varac_wait_cleared.emit()
        except Exception:
            pass
        self._suspend_for_minutes(mins)

    def _schedule_forced_retry(self) -> None:
        if self._retry_scheduled:
            return
        if self._forced_retry_attempts_left <= 0:
            return
        self._retry_scheduled = True

        def _try():
            self._retry_scheduled = False
            if self._control_future_stuck():
                self._reset_control_executor("forced retry (stuck control task)")
            if self._control_future is not None and not self._control_future.done():
                self._schedule_forced_retry()
                return
            if not self._control_can_attempt():
                self._schedule_forced_retry()
                return
            if self._forced_retry_attempts_left <= 0:
                return
            self._forced_retry_attempts_left -= 1
            if self._apply_latest_intent_if_any():
                return
            self.apply_current_entry(
                force=True,
                ignore_wait_prompt=True,
                ignore_suspend=True,
            )

        delay_ms = 1000
        remaining_backoff = (self._control_backoff_until or 0.0) - time.time()
        if remaining_backoff > 0:
            delay_ms = int(min(60_000, max(1_000, remaining_backoff * 1000.0 + 100.0)))
        QTimer.singleShot(delay_ms, _try)

    @staticmethod
    def _source_net_kind(source: str, entry: Optional[Dict]) -> str:
        src = (source or "").strip().upper()
        row = entry or {}
        if src == "NET":
            if row.get("primary_js8call_group"):
                return "JS8 Net"
            return "FLDigi Net"
        if src == "SOP":
            sop_name = str(row.get("sop_profile_name") or "").strip()
            return f"SOP Layer ({sop_name})" if sop_name else "SOP Layer"
        if src == "HF":
            return "HF Schedule"
        return ""

    @staticmethod
    def _entry_transition_signature(entry: Optional[Dict]) -> Tuple[object, ...]:
        row = entry or {}
        return (
            (row.get("source_type") or "").strip(),
            int(row.get("id") or 0),
            int(row.get("sop_profile_id") or 0),
            (row.get("band") or "").strip().upper(),
            (row.get("mode") or "").strip().upper(),
            (row.get("frequency") or "").strip(),
            (row.get("start_utc") or "").strip(),
            (row.get("end_utc") or "").strip(),
        )

    def _last_entry_matches_schedule_identity(self, entry: Optional[Dict]) -> bool:
        key = self._last_entry_key
        if not isinstance(key, tuple) or len(key) < 7:
            return False
        row = entry or {}
        band = (row.get("band") or "").strip().upper()
        freq_hz = self._parse_freq_hz((row.get("frequency") or "").strip())
        js8_group = (row.get("primary_js8call_group") or "").strip()
        og = self._resolve_operating_group(row)
        vfo_source = og.get("vfo") if isinstance(og, dict) else None
        vfo_raw = (vfo_source or row.get("vfo") or "A").strip().upper()
        vfo = vfo_raw if vfo_raw in ("A", "B") else None
        rig_mode = self._resolve_rig_mode(row)
        return (
            key[0],
            key[1],
            key[4],
            key[5],
            key[6],
        ) == (
            band,
            freq_hz,
            vfo,
            js8_group,
            rig_mode,
        )

    def _derive_source_reason(
        self,
        source: str,
        entry: Optional[Dict],
        sop_meta: Optional[Dict[str, object]] = None,
    ) -> Tuple[str, str]:
        src = (source or "").strip().upper()
        if src == "NET":
            return "net_precedence", "Net schedule is active and overrides SOP/HF."
        if src == "SOP":
            meta = sop_meta or {}
            reason_code = str(meta.get("winner_reason_code") or "single_active_profile").strip()
            detail = str(meta.get("winner_reason_detail") or "").strip()
            if not detail:
                detail = "SOP layer is active and overrides HF schedule."
            return reason_code, detail
        if src == "HF":
            return "hf_fallback", "No active Net/SOP layer row; baseline HF schedule is active."
        return "none", "No active schedule row."

    def get_status_summary(self) -> Dict[str, object]:
        now_cache = time.time()
        if (
            self._status_summary_cache is not None
            and now_cache - self._status_summary_cache_ts < self._status_summary_cache_ttl_s
        ):
            return dict(self._status_summary_cache)
        try:
            use_scheduler = bool(self.settings.get("use_scheduler", True))
        except Exception:
            use_scheduler = True
        control_mode = self._control_mode()
        entry = self.current_schedule_entry or {}
        freq_hz = self._current_rig_frequency(control_mode=control_mode, status_cached=True)
        flags = (
            self._off_schedule_flags(
                entry,
                control_mode=control_mode,
                current_freq_hz=freq_hz,
            )
            if entry
            else {"frequency": False, "mode": False, "offset": False, "fldigi_offset": False}
        )
        off_schedule = any(flags.values())
        varac_status = self._varac_status()
        js8_busy = False
        if self.js8 and self._js8_running():
            try:
                js8_busy = bool(self.js8.is_busy())
            except Exception:
                js8_busy = False
        fldigi_busy = False
        fldigi_busy_reason = None
        try:
            fldigi_status = self._fldigi_log_status()
            fldigi_busy = bool(fldigi_status.get("busy"))
            fldigi_busy_reason = fldigi_status.get("reason")
        except Exception:
            fldigi_busy = False
            fldigi_busy_reason = None
        ptt_active = False
        if control_mode == "FLRIG":
            ptt_active = self._status_poll_flrig_ptt()
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        suspended_until = self._suspend_until_dt()
        auto_resume_utc, auto_resume_source = self._auto_resume_utc(now_utc, suspended_until, flags)
        freq_label = ""
        if isinstance(freq_hz, (int, float)) and freq_hz > 0:
            freq_label = f"{freq_hz / 1_000_000:.3f}"
        source = self.current_source or "NONE"
        net_kind = self._source_net_kind(source, entry)
        next_freq_hz = self._next_transition_freq_hz
        next_freq_label = ""
        next_freq_mhz = None
        if isinstance(next_freq_hz, (int, float)) and next_freq_hz > 0:
            next_freq_mhz = float(next_freq_hz) / 1_000_000.0
            next_freq_label = f"{next_freq_mhz:.3f}"
        # Reuse computed off-schedule flags to avoid duplicate FLDigi mode/offset polls
        # in status-heavy UI refresh paths (sidebar + ControlFreq).
        fldigi_mode_off = bool(flags.get("mode"))
        fldigi_offset_off = bool(flags.get("fldigi_offset"))
        summary = {
            "use_scheduler": use_scheduler,
            "control_mode": control_mode,
            "off_schedule": off_schedule,
            "off_schedule_flags": flags,
            "varac_waiting": bool(varac_status.get("waiting_for_frequency")),
            "js8_busy": js8_busy,
            "fldigi_busy": fldigi_busy,
            "fldigi_busy_reason": fldigi_busy_reason,
            "varac_busy": bool(varac_status.get("busy")),
            "ptt_active": ptt_active,
            "suspended_until": suspended_until,
            "auto_resume_utc": auto_resume_utc,
            "auto_resume_source": auto_resume_source,
            "freq_label": freq_label,
            "source": source,
            "net_kind": net_kind,
            "sop_contention": bool(self._sop_contention),
            "sop_contention_profiles": list(self._sop_contention_profiles),
            "sop_selected_profile": str(self._sop_winner_profile or ""),
            "sop_selected_priority": int(self._sop_winner_priority or 100),
            "sop_selected_reason": str(self._sop_winner_reason_code or ""),
            "sop_selected_reason_detail": str(self._sop_winner_reason_detail or ""),
            "source_reason": str(self._source_reason_code or ""),
            "source_reason_detail": str(self._source_reason_detail or ""),
            "next_source": str(self._next_source or "NONE"),
            "next_net_kind": str(self._next_net_kind or ""),
            "next_frequency_label": next_freq_label,
            "next_frequency_mhz": next_freq_mhz,
            "next_transition_utc": self._next_transition_utc,
            "next_transition_note": str(self._next_transition_note or ""),
            "next_source_change": bool(self._next_source_change),
            "fldigi_mode_off": fldigi_mode_off,
            "fldigi_offset_off": fldigi_offset_off,
        }
        self._status_summary_cache = dict(summary)
        self._status_summary_cache_ts = now_cache
        return summary

    def _off_schedule_flags(
        self,
        entry: Dict,
        *,
        check_frequency: bool = True,
        check_mode: bool = True,
        check_offset: bool = True,
        control_mode: Optional[str] = None,
        current_freq_hz: Optional[int] = None,
    ) -> Dict[str, bool]:
        flags = {"frequency": False, "mode": False, "offset": False, "fldigi_offset": False}
        if not entry:
            return flags
        active_control_mode = (control_mode or self._control_mode()).strip().upper()
        display_only_manual = active_control_mode in {"MANUAL", "NONE"}
        if check_frequency:
            freq_hz = self._parse_freq_hz((entry.get("frequency") or "").strip())
            if freq_hz:
                cur = current_freq_hz
                if cur is None and active_control_mode not in {"MANUAL", "NONE"}:
                    cur = self._current_rig_frequency(
                        control_mode=active_control_mode,
                        status_cached=True,
                    )
                if cur is not None and abs(cur - freq_hz) > 5:
                    flags["frequency"] = True
        if display_only_manual:
            return flags
        if check_offset and self._js8_running() and self.js8:
            try:
                desired_js8 = self._js8_offset_setting()
                current_js8 = self.js8.get_offset()
                if current_js8 is not None and desired_js8 != current_js8:
                    flags["offset"] = True
            except Exception:
                pass
        if check_mode and self._fldigi_available():
            desired_mode = self._expected_fldigi_mode(entry)
            desired_offset = self._expected_fldigi_offset(entry)
            if desired_mode:
                current_mode = self._current_fldigi_mode()
                if current_mode is not None and current_mode != desired_mode.strip().upper():
                    flags["mode"] = True
            if desired_offset is not None:
                current_offset = self._current_fldigi_offset()
                if current_offset is not None and desired_offset != current_offset:
                    flags["fldigi_offset"] = True
        return flags

    def _reset_prompt_timers(self, now_ts: Optional[float] = None, items: Optional[List[str]] = None) -> None:
        ts = now_ts if now_ts is not None else time.time()
        if not items:
            for state in self._prompt_state.values():
                state["last_prompt_ts"] = ts
            return
        mapping = {
            "Frequency": "frequency",
            "Mode": "mode",  # legacy compatibility
            "FLDigi Mode": "mode",
            "Offset": "offset",
            "FLDigi Offset": "fldigi_offset",
        }
        for label in items:
            key = mapping.get(label)
            if key and key in self._prompt_state:
                self._prompt_state[key]["last_prompt_ts"] = ts

    def _maybe_prompt_enforcement(self) -> None:
        try:
            if not bool(self.settings.get("use_scheduler", True)):
                self._prompt_active = False
                self._prompt_items = []
                self._last_fldigi_offset_prompt_sig = None
                return
        except Exception:
            pass
        control_mode = self._control_mode()
        if control_mode in ("MANUAL", "NONE"):
            self._prompt_active = False
            self._prompt_items = []
            self._last_fldigi_offset_prompt_sig = None
            return
        if self._net_corrections_suppressed():
            self._prompt_active = False
            self._prompt_items = []
            self._last_fldigi_offset_prompt_sig = None
            return
        entry = self.current_schedule_entry or {}
        if not entry:
            self._prompt_active = False
            self._prompt_items = []
            self._last_fldigi_offset_prompt_sig = None
            return
        entry_key = (entry.get("frequency"), entry.get("band"), entry.get("mode"), entry.get("group_name"))
        now_ts = time.time()
        if self._prompt_entry_key and entry_key != self._prompt_entry_key:
            self._prompt_active = False
            self._prompt_items = []
            self._prompt_entry_key = entry_key
            self._last_fldigi_offset_prompt_sig = None
            self._reset_prompt_timers(now_ts)
            try:
                self.off_schedule_cleared.emit()
            except Exception:
                pass
            return
        self._prompt_entry_key = entry_key
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if self._scheduling_suspended(now_utc):
            return
        freq_prompt = self._enforcement_mode("freq_enforcement_mode") == "Prompt"
        fldigi_prompt = self._enforcement_mode("fldigi_enforcement_mode") == "Prompt"
        js8_prompt = self._enforcement_mode("js8_enforcement_mode") == "Prompt"
        if not (freq_prompt or fldigi_prompt or js8_prompt):
            self._prompt_active = False
            self._prompt_items = []
            self._last_fldigi_offset_prompt_sig = None
            return
        flags = self._off_schedule_flags(
            entry,
            check_frequency=freq_prompt,
            check_mode=fldigi_prompt,
            check_offset=js8_prompt,
            control_mode=control_mode,
        )
        if not any(flags.values()):
            self._prompt_active = False
            self._prompt_items = []
            self._last_fldigi_offset_prompt_sig = None
            self._last_off_schedule_flags = {
                "frequency": False,
                "mode": False,
                "offset": False,
                "fldigi_offset": False,
            }
            return
        if self._prompt_active:
            if not bool(flags.get("fldigi_offset")):
                self._last_fldigi_offset_prompt_sig = None
            return
        fldigi_offset_prompt_sig: Optional[Tuple[Optional[int], Optional[int]]] = None
        fldigi_offset_prompt_changed = False
        if fldigi_prompt and bool(flags.get("fldigi_offset")):
            fldigi_offset_prompt_sig = (
                self._expected_fldigi_offset(entry),
                self._current_fldigi_offset(),
            )
            fldigi_offset_prompt_changed = (
                self._last_fldigi_offset_prompt_sig is not None
                and fldigi_offset_prompt_sig != self._last_fldigi_offset_prompt_sig
            )
        prev_flags = self._last_off_schedule_flags or {}
        items: List[str] = []
        if flags["frequency"] and freq_prompt:
            interval = self._prompt_interval_minutes("freq_prompt_interval")
            if (not prev_flags.get("frequency")) or (
                now_ts - self._prompt_state["frequency"]["last_prompt_ts"] >= interval * 60
            ):
                items.append("Frequency")
        if flags["fldigi_offset"] and fldigi_prompt:
            interval = self._prompt_interval_minutes("fldigi_prompt_interval")
            if fldigi_offset_prompt_changed or (not prev_flags.get("fldigi_offset")) or (
                now_ts - self._prompt_state["fldigi_offset"]["last_prompt_ts"] >= interval * 60
            ):
                items.append("FLDigi Offset")
        if flags["mode"] and fldigi_prompt:
            interval = self._prompt_interval_minutes("fldigi_prompt_interval")
            if (not prev_flags.get("mode")) or (
                now_ts - self._prompt_state["mode"]["last_prompt_ts"] >= interval * 60
            ):
                items.append("FLDigi Mode")
        if flags["offset"] and js8_prompt:
            interval = self._prompt_interval_minutes("js8_prompt_interval")
            if (not prev_flags.get("offset")) or (
                now_ts - self._prompt_state["offset"]["last_prompt_ts"] >= interval * 60
            ):
                items.append("Offset")
        if not items:
            if bool(flags.get("fldigi_offset")):
                self._last_fldigi_offset_prompt_sig = fldigi_offset_prompt_sig
            else:
                self._last_fldigi_offset_prompt_sig = None
            self._last_off_schedule_flags = dict(flags)
            return
        if any(item in {"Mode", "FLDigi Mode", "FLDigi Offset"} for item in items):
            self._fldigi_force_apply_once = False
        self._prompt_active = True
        self._prompt_items = items
        for item in items:
            if item == "Frequency":
                self._prompt_state["frequency"]["last_prompt_ts"] = now_ts
            elif item in {"Mode", "FLDigi Mode"}:
                self._prompt_state["mode"]["last_prompt_ts"] = now_ts
            elif item == "FLDigi Offset":
                self._prompt_state["fldigi_offset"]["last_prompt_ts"] = now_ts
            elif item == "Offset":
                self._prompt_state["offset"]["last_prompt_ts"] = now_ts
        self.off_schedule_detected.emit({"entry": entry, "items": items})
        self._last_fldigi_offset_prompt_sig = fldigi_offset_prompt_sig if bool(flags.get("fldigi_offset")) else None
        self._last_off_schedule_flags = dict(flags)

    def resolve_off_schedule(
        self,
        action: str,
        items: Optional[List[str]] = None,
        minutes: Optional[int] = None,
    ) -> None:
        self._prompt_active = False
        self._prompt_items = []
        fldigi_items = {"Mode", "FLDigi Mode", "FLDigi Offset"}
        if action == "suspend":
            if items and any(item in fldigi_items for item in items):
                self._fldigi_force_apply_once = False
            self._reset_prompt_timers(items=items)
            self._suspend_for_minutes(self._normalize_hold_minutes(minutes if minutes is not None else self._default_hold_minutes()))
            return
        if action == "ignore":
            if items and any(item in fldigi_items for item in items):
                self._fldigi_force_apply_once = False
            self._reset_prompt_timers(items=items)
            return
        if action != "apply":
            return
        entry = self.current_schedule_entry or {}
        if not entry:
            return
        apply_items = items or []
        if "Frequency" in apply_items:
            self._apply_schedule_entry(
                entry,
                self.current_source,
                force=True,
                apply_js8_offset=False,
                apply_fldigi=False,
            )
        if any(item in fldigi_items for item in apply_items):
            self._update_desired_fldigi_settings(entry)
            if self._desired_fldigi_mode or self._desired_fldigi_offset is not None:
                self._fldigi_apply_pending = True
                self._fldigi_force_apply_once = True
                self._maybe_apply_fldigi()
        if "Offset" in apply_items:
            try:
                desired = self._js8_offset_setting()
                if self.js8:
                    self.js8.set_offset(desired)
            except Exception:
                pass

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
            self._maybe_apply_fldigi()
            self._maybe_prompt_enforcement()
        except Exception as e:
            log.error("SchedulerEngine timer tick failed: %s", e)

    def _load_operating_groups(self) -> List[Dict]:
        data = self.settings.all()
        og = data.get("operating_groups", [])
        return og if isinstance(og, list) else []

    def _resolve_operating_group(self, entry: Dict) -> Optional[Dict]:
        group_name = (entry.get("group_name") or entry.get("group") or "").strip().upper()
        band = (entry.get("band") or "").strip().upper()
        mode = normalize_operating_group_mode(entry.get("mode") or "", band)
        freq = self._normalize_sched_frequency(entry.get("frequency"))
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
            g_mode = normalize_operating_group_mode(g.get("mode") or "", g_band)
            g_freq = self._normalize_sched_frequency(g.get("frequency"))
            candidates.append((g_band, g_mode, g_freq, g))
        if not candidates:
            return None
        # Prefer exact band+mode match, then band+frequency, then band-only,
        # then mode-only, then frequency-only, then first group match.
        for g_band, g_mode, _g_freq, g in candidates:
            if g_band == band and g_mode == mode and g_mode:
                return g
        for g_band, _g_mode, g_freq, g in candidates:
            if g_band == band and g_freq == freq and g_freq:
                return g
        for g_band, _g_mode, _g_freq, g in candidates:
            if g_band == band:
                return g
        for _g_band, g_mode, _g_freq, g in candidates:
            if g_mode == mode and g_mode:
                return g
        for _g_band, _g_mode, g_freq, g in candidates:
            if g_freq == freq and g_freq:
                return g
        return candidates[0][3]

    def _entry_with_operating_group_overrides(self, entry: Dict) -> Tuple[Dict, Optional[Dict]]:
        """
        Return an entry copy with operating-group-backed values overlaid.

        For rows that specify an Operating Group, group values are authoritative
        for runtime rig control (frequency/vfo/mode), which keeps scheduler/QSY
        behavior aligned with Settings > Operating Groups.
        """
        effective = dict(entry or {})
        og = self._resolve_operating_group(effective)
        if not isinstance(og, dict):
            return effective, None

        og_band = (og.get("band") or "").strip().upper()
        if og_band:
            effective["band"] = og_band

        mode_band = (effective.get("band") or "").strip().upper()
        og_mode = normalize_operating_group_mode(og.get("mode") or "", mode_band)
        if og_mode:
            effective["mode"] = og_mode

        og_freq = self._normalize_sched_frequency(og.get("frequency"))
        if og_freq:
            effective["frequency"] = og_freq

        og_vfo = (og.get("vfo") or "").strip().upper()
        if og_vfo in ("A", "B"):
            effective["vfo"] = og_vfo

        if not str(effective.get("fldigi_mode") or "").strip():
            fldigi_mode = str(og.get("fldigi_mode") or "").strip()
            if fldigi_mode:
                effective["fldigi_mode"] = fldigi_mode
        if not str(effective.get("fldigi_offset") or "").strip():
            fldigi_offset = str(og.get("fldigi_offset") or "").strip()
            if fldigi_offset:
                effective["fldigi_offset"] = fldigi_offset

        if "auto_tune" not in effective:
            effective["auto_tune"] = bool(og.get("auto_tune"))

        return effective, og

    def _update_desired_fldigi_settings(self, entry: Dict) -> None:
        mode = self._expected_fldigi_mode(entry)
        offset = self._expected_fldigi_offset(entry)
        prev_desired = (self._desired_fldigi_mode, self._desired_fldigi_offset)
        desired = (mode, offset)
        self._desired_fldigi_mode = mode
        self._desired_fldigi_offset = offset
        if mode is None and offset is None:
            self._fldigi_apply_pending = False
        else:
            # Only re-queue FLDigi apply when the desired tuple changed, a prior
            # apply is still pending, we have never applied this tuple, or a real
            # schedule transition/user action explicitly forced FLDigi re-apply.
            self._fldigi_apply_pending = bool(
                self._fldigi_apply_pending
                or desired != prev_desired
                or self._last_fldigi_apply != desired
                or self._fldigi_force_apply_once
            )
        if (mode or offset is not None) and self._fldigi_apply_pending:
            # Request immediate apply if FLDigi is already available.
            self._fldigi_apply_after_ts = 0.0
        elif mode is None and offset is None:
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
        entry = self.current_schedule_entry or {}
        rig_mode = self._resolve_rig_mode(entry)
        js8_tune = self._js8_offset_setting()
        same_source_entry = bool(
            (self._last_source or "") == (self.current_source or "NONE")
            and self._last_entry_matches_schedule_identity(entry)
        )
        if self._net_corrections_suppressed():
            band = (entry.get("band") or "").strip().upper()
            freq_hz = self._parse_freq_hz((entry.get("frequency") or "").strip())
            js8_group = (entry.get("primary_js8call_group") or "").strip()
            og = self._resolve_operating_group(entry)
            vfo_source = og.get("vfo") if isinstance(og, dict) else None
            vfo_raw = (vfo_source or entry.get("vfo") or "A").strip().upper()
            vfo = vfo_raw if vfo_raw in ("A", "B") else None
            entry_key = (
                band,
                freq_hz,
                self._expected_fldigi_offset(entry),
                js8_tune,
                vfo,
                js8_group,
                rig_mode,
            )
            if self._net_schedule_active and self._net_schedule_entry_key is None:
                self._net_schedule_entry_key = entry_key
            if self._manual_net_fldigi_active or self._manual_net_js8_active:
                if not self._net_resume_apply_once:
                    return
            if not self._net_fldigi_apply_allowed_once:
                if not self._net_resume_apply_once:
                    return
            if self._net_schedule_active and not self._net_resume_apply_once:
                if self._net_schedule_started_at is not None:
                    if time.time() - self._net_schedule_started_at > 12:
                        self._net_fldigi_apply_allowed_once = False
                        return
            if (self._last_entry_key == entry_key or same_source_entry) and not self._fldigi_force_apply_once:
                return
        if self._enforcement_mode("fldigi_enforcement_mode") == "Prompt":
            flags = self._off_schedule_flags(entry, check_frequency=False, check_mode=True, check_offset=False)
            fldigi_prompt_mismatch = bool(flags.get("mode") or flags.get("fldigi_offset"))
            if fldigi_prompt_mismatch:
                if self._prompt_active and any(
                    item in {"Mode", "FLDigi Mode", "FLDigi Offset"} for item in self._prompt_items
                ):
                    return
                if self._fldigi_force_apply_once:
                    band = (entry.get("band") or "").strip().upper()
                    freq_hz = self._parse_freq_hz((entry.get("frequency") or "").strip())
                    js8_group = (entry.get("primary_js8call_group") or "").strip()
                    og = self._resolve_operating_group(entry)
                    vfo_source = og.get("vfo") if isinstance(og, dict) else None
                    vfo_raw = (vfo_source or entry.get("vfo") or "A").strip().upper()
                    vfo = vfo_raw if vfo_raw in ("A", "B") else None
                    entry_key = (
                        band,
                        freq_hz,
                        self._expected_fldigi_offset(entry),
                        js8_tune,
                        vfo,
                        js8_group,
                        rig_mode,
                    )
                    if self._last_entry_key == entry_key or same_source_entry:
                        self._fldigi_force_apply_once = False
                        return
                if not self._fldigi_force_apply_once:
                    return
        if not self._fldigi_apply_pending:
            return
        if not (self._desired_fldigi_mode or self._desired_fldigi_offset is not None):
            return
        available = self._fldigi_available()
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
        status = self._fldigi_log_status()
        if bool(status.get("busy")):
            return
        desired = (self._desired_fldigi_mode, self._desired_fldigi_offset)
        if self._last_fldigi_apply == desired and self._fldigi_apply_after_ts is None:
            return
        if self.rig.set_fldigi_mode_offset(self._desired_fldigi_mode, self._desired_fldigi_offset):
            self._last_fldigi_apply = desired
            self._fldigi_mode_cache = self._desired_fldigi_mode.strip().upper() if self._desired_fldigi_mode else None
            self._fldigi_offset_cache = self._desired_fldigi_offset
            self._fldigi_mode_cache_ts = time.time()
            self._fldigi_offset_cache_ts = self._fldigi_mode_cache_ts
            self._fldigi_apply_after_ts = None
            self._fldigi_apply_pending = False
            self._fldigi_force_apply_once = False
            self._net_fldigi_apply_allowed_once = False
            self._net_resume_apply_once = False

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

            def _table_columns(name: str) -> set[str]:
                cur = conn.execute(f"PRAGMA table_info({name})")
                return {str(r[1]) for r in cur.fetchall()}

            def _append_rows(table: str, *, default_vfo: str) -> bool:
                if not self._table_exists(conn, table):
                    return False
                cols = _table_columns(table)
                required = {"day_utc", "band", "mode", "frequency", "start_utc", "end_utc", "early_checkin"}
                if not required.issubset(cols):
                    return False
                select_order = [
                    "day_utc",
                    "recurrence",
                    "biweekly_offset_weeks",
                    "month_weeks",
                    "band",
                    "mode",
                    "vfo",
                    "frequency",
                    "start_utc",
                    "end_utc",
                    "early_checkin",
                    "auto_tune",
                    "primary_js8call_group",
                    "comment",
                    "net_name",
                    "group_name",
                    "fldigi_mode",
                    "fldigi_offset",
                ]
                available = [c for c in select_order if c in cols]
                cur = conn.execute(f"SELECT {', '.join(available)} FROM {table}")
                for row in cur.fetchall():
                    row_data = dict(zip(available, row))
                    vfo_value = (row_data.get("vfo") or default_vfo or "A").strip().upper() or "A"
                    try:
                        biweekly = int(row_data.get("biweekly_offset_weeks") or 0)
                    except Exception:
                        biweekly = 0
                    rows.append(
                        {
                            "day_utc": row_data.get("day_utc") or "",
                            "recurrence": row_data.get("recurrence") or "Weekly",
                            "biweekly_offset_weeks": biweekly,
                            "month_weeks": row_data.get("month_weeks") or "",
                            "band": row_data.get("band") or "",
                            "mode": row_data.get("mode") or "",
                            "vfo": vfo_value,
                            "frequency": str(row_data.get("frequency") or ""),
                            "start_utc": row_data.get("start_utc") or "",
                            "end_utc": row_data.get("end_utc") or "",
                            "early_checkin": row_data.get("early_checkin")
                            if row_data.get("early_checkin") is not None
                            else 0,
                            "auto_tune": bool(row_data.get("auto_tune")),
                            "primary_js8call_group": row_data.get("primary_js8call_group") or "",
                            "comment": row_data.get("comment") or "",
                            "net_name": row_data.get("net_name") or "",
                            "group_name": row_data.get("group_name") or "",
                            "fldigi_mode": row_data.get("fldigi_mode") or "",
                            "fldigi_offset": row_data.get("fldigi_offset") or "",
                        }
                    )
                return True

            if _append_rows("net_schedule_tab", default_vfo="A"):
                return rows
            _append_rows("net_schedule", default_vfo="A")
            return rows
        except Exception as e:
            log.error("SchedulerEngine: failed to load net schedule from DB %s: %s", db_path, e)
            return None
        finally:
            conn.close()

    def _sop_layer_enabled(self) -> bool:
        try:
            return bool(self.settings.get("sop_schedule_layer_enabled", True))
        except Exception:
            return True

    @staticmethod
    def _normalize_condition_levels(value: object) -> str:
        raw = str(value or "").strip().upper()
        if not raw or raw == "ALL":
            return "ALL"
        vals: List[int] = []
        for token in raw.replace(";", ",").replace("|", ",").split(","):
            token = token.strip()
            if not token:
                continue
            if token == "ALL":
                return "ALL"
            try:
                lvl = int(token)
            except Exception:
                continue
            if 1 <= lvl <= 5:
                vals.append(lvl)
        if not vals:
            return "ALL"
        return ",".join(str(v) for v in sorted(set(vals)))

    def _condition_level_map(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        try:
            rows = self.settings.get("operating_groups", []) or []
        except Exception:
            rows = []
        if not isinstance(rows, list):
            return out
        for row in rows:
            if not isinstance(row, dict):
                continue
            group = str(row.get("group", "") or "").strip().upper()
            if not group:
                continue
            if not bool(row.get("use_condition_levels", False)):
                continue
            try:
                level = int(row.get("condition_level", 0) or 0)
            except Exception:
                level = 0
            if not (1 <= level <= 5):
                continue
            prev = out.get(group)
            if prev is None or level < prev:
                out[group] = level
        return out

    @classmethod
    def _condition_level_match(cls, condition_levels: str, group_level: Optional[int]) -> bool:
        normalized = cls._normalize_condition_levels(condition_levels)
        if normalized == "ALL":
            return True
        if group_level is None:
            return True
        allowed: Set[int] = set()
        for token in normalized.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                lvl = int(token)
            except Exception:
                continue
            if 1 <= lvl <= 5:
                allowed.add(lvl)
        if not allowed:
            return True
        return int(group_level) in allowed

    def _load_sop_schedule_layer_from_db(self) -> Optional[List[Dict]]:
        """
        Read active SOP schedule-layer entries from freqinout_nets.db.
        Rows are joined with sop_profiles so only active profiles are considered.
        """
        if not self._sop_layer_enabled():
            return []
        db_path = self._config_dir() / "freqinout_nets.db"
        if not db_path.exists():
            return None
        conn = sqlite3.connect(db_path)
        try:
            if not self._table_exists(conn, "sop_schedule_layer") or not self._table_exists(conn, "sop_profiles"):
                return []
            profile_cols = set()
            try:
                cur_cols = conn.execute("PRAGMA table_info(sop_profiles)")
                profile_cols = {str(r[1] or "").strip().lower() for r in cur_cols.fetchall() if len(r) > 1}
            except Exception:
                profile_cols = set()
            layer_cols = set()
            try:
                cur_cols = conn.execute("PRAGMA table_info(sop_schedule_layer)")
                layer_cols = {str(r[1] or "").strip().lower() for r in cur_cols.fetchall() if len(r) > 1}
            except Exception:
                layer_cols = set()
            priority_expr = "COALESCE(p.priority, 100)" if "priority" in profile_cols else "100"
            if "updated_utc" in profile_cols and "created_utc" in profile_cols:
                updated_expr = "COALESCE(p.updated_utc, p.created_utc, '')"
            elif "updated_utc" in profile_cols:
                updated_expr = "COALESCE(p.updated_utc, '')"
            elif "created_utc" in profile_cols:
                updated_expr = "COALESCE(p.created_utc, '')"
            else:
                updated_expr = "''"
            condition_expr = "COALESCE(l.condition_levels, 'ALL')" if "condition_levels" in layer_cols else "'ALL'"
            group_expr = (
                "COALESCE(NULLIF(TRIM(l.group_name), ''), COALESCE(p.operating_group, ''))"
                if "group_name" in layer_cols
                else "COALESCE(p.operating_group, '')"
            )
            cur = conn.execute(
                f"""
                SELECT
                    l.id,
                    l.day_utc,
                    l.recurrence,
                    l.biweekly_offset_weeks,
                    l.month_weeks,
                    {condition_expr} AS condition_levels,
                    l.band,
                    l.mode,
                    l.vfo,
                    l.frequency,
                    l.start_utc,
                    l.end_utc,
                    l.enabled,
                    l.sort_order,
                    l.profile_id,
                    p.name,
                    {group_expr} AS group_name,
                    {priority_expr} AS sop_priority,
                    {updated_expr} AS sop_profile_updated_utc
                FROM sop_schedule_layer l
                JOIN sop_profiles p ON p.id = l.profile_id
                WHERE p.active = 1
                  AND COALESCE(l.enabled, 1) = 1
                ORDER BY p.id, COALESCE(l.sort_order, 0), l.id
                """
            )
            rows: List[Dict] = []
            cond_map = self._condition_level_map()
            for (
                layer_id,
                day_utc,
                recurrence,
                biweekly_offset_weeks,
                month_weeks,
                condition_levels,
                band,
                mode,
                vfo,
                frequency,
                start_utc,
                end_utc,
                _enabled,
                _sort_order,
                profile_id,
                profile_name,
                operating_group,
                sop_priority,
                sop_profile_updated_utc,
            ) in cur.fetchall():
                freq_txt = str(frequency or "").strip()
                if not freq_txt:
                    continue
                try:
                    biweekly = int(biweekly_offset_weeks or 0)
                except Exception:
                    biweekly = 0
                try:
                    priority_num = int(sop_priority or 100)
                except Exception:
                    priority_num = 100
                sort_order = int(_sort_order or 0)
                group_name = (operating_group or "").strip().upper()
                group_level = cond_map.get(group_name)
                if not self._condition_level_match(str(condition_levels or "ALL"), group_level):
                    continue
                rows.append(
                    {
                        "id": int(layer_id or 0),
                        "day_utc": day_utc or "ALL",
                        "recurrence": recurrence or "Weekly",
                        "biweekly_offset_weeks": biweekly,
                        "month_weeks": month_weeks or "",
                        "band": (band or "").strip().upper(),
                        "mode": (mode or "").strip().upper(),
                        "vfo": (vfo or "A").strip().upper() or "A",
                        "frequency": freq_txt,
                        "start_utc": start_utc or "",
                        "end_utc": end_utc or "",
                        "early_checkin": 0,
                        "auto_tune": False,
                        "primary_js8call_group": "",
                        "comment": f"SOP Layer: {profile_name or ''}".strip(),
                        "group_name": group_name,
                        "condition_levels": self._normalize_condition_levels(condition_levels),
                        "sop_profile_id": int(profile_id or 0),
                        "sop_profile_name": profile_name or "",
                        "sop_priority": priority_num,
                        "sop_profile_updated_utc": str(sop_profile_updated_utc or ""),
                        "sort_order": sort_order,
                        "source_type": "SOP_LAYER",
                    }
                )
            return rows
        except Exception as e:
            log.error("SchedulerEngine: failed to load sop schedule layer from DB %s: %s", db_path, e)
            return None
        finally:
            conn.close()

    @staticmethod
    def _normalize_sched_frequency(value: object) -> str:
        txt = str(value or "").strip()
        if not txt:
            return ""
        try:
            return f"{float(txt):.3f}"
        except Exception:
            return txt

    @staticmethod
    def _normalize_sched_recurrence(value: object) -> str:
        raw = str(value or "Weekly").strip().upper()
        if raw == "MONTHLY":
            raw = "PERIODIC"
        if raw in {"DAILY", "PERIODIC", "BI-WEEKLY", "WEEKLY"}:
            return "Bi-Weekly" if raw == "BI-WEEKLY" else raw.title()
        return "Weekly"

    @staticmethod
    def _normalize_sched_month_weeks(value: object) -> str:
        weeks: List[int] = []
        for token in str(value or "").split(","):
            tok = token.strip()
            if not tok:
                continue
            try:
                val = int(tok)
            except Exception:
                continue
            if 1 <= val <= 5:
                weeks.append(val)
        return ",".join(str(v) for v in sorted(set(weeks)))

    def _net_row_signature(self, row: Optional[Dict]) -> str:
        data = row or {}
        group_name = str(data.get("group_name") or "").strip().upper()
        band = str(data.get("band") or "").strip().upper()
        freq = self._normalize_sched_frequency(data.get("frequency"))
        day = str(data.get("day_utc") or "ALL").strip().upper() or "ALL"
        recurrence = self._normalize_sched_recurrence(data.get("recurrence"))
        biweekly = int(data.get("biweekly_offset_weeks") or 0)
        month_weeks = self._normalize_sched_month_weeks(data.get("month_weeks"))
        start_utc = str(data.get("start_utc") or "").strip()
        end_utc = str(data.get("end_utc") or "").strip()
        net_name = str(data.get("net_name") or data.get("name") or "").strip().upper()
        return (
            f"NET|{group_name}|{band}|{freq}|{day}|{recurrence}|{biweekly}|"
            f"{month_weeks}|{start_utc}|{end_utc}|{net_name}"
        )

    def _sop_row_signature(self, row: Optional[Dict]) -> str:
        data = row or {}
        profile_id = int(data.get("sop_profile_id") or 0)
        layer_id = int(data.get("id") or 0)
        group_name = str(data.get("group_name") or "").strip().upper()
        band = str(data.get("band") or "").strip().upper()
        freq = self._normalize_sched_frequency(data.get("frequency"))
        day = str(data.get("day_utc") or "ALL").strip().upper() or "ALL"
        recurrence = self._normalize_sched_recurrence(data.get("recurrence"))
        biweekly = int(data.get("biweekly_offset_weeks") or 0)
        month_weeks = self._normalize_sched_month_weeks(data.get("month_weeks"))
        start_utc = str(data.get("start_utc") or "").strip()
        end_utc = str(data.get("end_utc") or "").strip()
        return (
            f"SOP|{profile_id}|{layer_id}|{group_name}|{band}|{freq}|{day}|"
            f"{recurrence}|{biweekly}|{month_weeks}|{start_utc}|{end_utc}"
        )

    def _load_sop_net_conflict_policies_from_db(self) -> Optional[List[Dict]]:
        db_path = self._config_dir() / "freqinout_nets.db"
        if not db_path.exists():
            return None
        conn = sqlite3.connect(db_path)
        try:
            if not self._table_exists(conn, "sop_net_conflict_policy"):
                return []
            cur = conn.execute(
                """
                SELECT
                    sop_profile_id,
                    sop_layer_id,
                    net_row_signature,
                    sop_row_signature,
                    policy,
                    window_start_utc,
                    window_end_utc,
                    active,
                    updated_utc
                FROM sop_net_conflict_policy
                WHERE COALESCE(active, 1) = 1
                ORDER BY COALESCE(updated_utc, '') DESC, id DESC
                """
            )
            out: List[Dict] = []
            for (
                sop_profile_id,
                sop_layer_id,
                net_row_signature,
                sop_row_signature,
                policy,
                window_start_utc,
                window_end_utc,
                active,
                updated_utc,
            ) in cur.fetchall():
                pol = str(policy or "").strip().upper()
                if pol not in {"SOP_PRIORITY", "NET_PRIORITY"}:
                    pol = "NET_PRIORITY"
                out.append(
                    {
                        "sop_profile_id": int(sop_profile_id or 0),
                        "sop_layer_id": int(sop_layer_id or 0),
                        "net_row_signature": str(net_row_signature or "").strip(),
                        "sop_row_signature": str(sop_row_signature or "").strip(),
                        "policy": pol,
                        "window_start_utc": str(window_start_utc or "").strip(),
                        "window_end_utc": str(window_end_utc or "").strip(),
                        "active": bool(active) if active is not None else True,
                        "updated_utc": str(updated_utc or "").strip(),
                    }
                )
            return out
        except Exception as e:
            log.error("SchedulerEngine: failed to load Net/SOP conflict policy rows: %s", e)
            return None
        finally:
            conn.close()

    def _find_net_sop_policy_override(
        self,
        now_utc: datetime.datetime,
        net_entry: Optional[Dict],
        sop_entry: Optional[Dict],
        policy_rows: List[Dict],
    ) -> Optional[Dict]:
        if not net_entry or not sop_entry or not policy_rows:
            return None
        net_sig = self._net_row_signature(net_entry)
        sop_sig = self._sop_row_signature(sop_entry)
        if not net_sig or not sop_sig:
            return None
        now_ts = float(now_utc.timestamp())
        best: Optional[Dict] = None
        best_updated = 0.0
        for row in policy_rows:
            if str(row.get("net_row_signature") or "") != net_sig:
                continue
            if str(row.get("sop_row_signature") or "") != sop_sig:
                continue
            start_ts = _parse_iso_utc_to_epoch(row.get("window_start_utc"))
            end_ts = _parse_iso_utc_to_epoch(row.get("window_end_utc"))
            if end_ts <= start_ts:
                continue
            if not (start_ts <= now_ts < end_ts):
                continue
            updated_ts = _parse_iso_utc_to_epoch(row.get("updated_utc"))
            if best is None or updated_ts >= best_updated:
                best = row
                best_updated = updated_ts
        return best

    def _select_runtime_source(
        self,
        *,
        now_utc: datetime.datetime,
        hf_entry: Optional[Dict],
        net_entry: Optional[Dict],
        sop_entry: Optional[Dict],
        policy_rows: List[Dict],
    ) -> Tuple[str, Optional[Dict], Optional[Dict]]:
        policy_override = self._find_net_sop_policy_override(now_utc, net_entry, sop_entry, policy_rows)
        if net_entry:
            if sop_entry and policy_override and str(policy_override.get("policy") or "").upper() == "SOP_PRIORITY":
                return "SOP", sop_entry, policy_override
            return "NET", net_entry, policy_override
        if sop_entry:
            return "SOP", sop_entry, None
        if hf_entry:
            return "HF", hf_entry, None
        return "NONE", None, None

    def _load_schedules(self, *, force: bool = False) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """
        Load schedules, preferring the database tables and falling back to the
        SettingsManager key/value store for backwards compatibility.
        """
        cache = getattr(self, "_schedule_cache", None)
        config_db = self._config_dir() / "freqinout.db"
        nets_db = self._config_dir() / "freqinout_nets.db"
        mtimes = (self._db_mtime(config_db), self._db_mtime(nets_db), 1 if self._sop_layer_enabled() else 0)

        if cache and not force and cache.get("mtimes") == mtimes and cache.get("data"):
            return cache["data"]  # type: ignore[return-value]

        hf_db = self._load_daily_schedule_from_db()
        net_db = self._load_net_schedule_from_db()
        sop_layer_db = self._load_sop_schedule_layer_from_db()
        policy_db = self._load_sop_net_conflict_policies_from_db()

        data = self.settings.all()
        hf = hf_db if hf_db is not None else data.get("hf_schedule") or data.get("daily_schedule") or []
        net = net_db if net_db is not None else data.get("net_schedule") or []
        sop_layer = sop_layer_db if sop_layer_db is not None else []
        policies = policy_db if policy_db is not None else []

        if not isinstance(hf, list):
            hf = []
        if not isinstance(net, list):
            net = []
        if not isinstance(sop_layer, list):
            sop_layer = []
        if not isinstance(policies, list):
            policies = []

        self._schedule_cache = {"mtimes": mtimes, "data": (hf, net, sop_layer, policies)}
        return hf, net, sop_layer, policies

    def _evaluate(self, now_utc: datetime.datetime, force: bool = False) -> None:
        """
        Core evaluation step: decides which entry should be active
        (NET, SOP layer, or HF), computes the next change time, and
        optionally drives the rig.
        """
        # Pick up any setting changes (control_via, use_scheduler, offsets, etc.)
        try:
            self.settings.reload()
        except Exception as e:
            log.debug("SchedulerEngine: settings reload failed (continuing with cached data): %s", e)

        hf_sched, net_sched, sop_sched, net_sop_policies = self._load_schedules(force=force)

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
        sop_meta: Dict[str, object] = {}
        try:
            sop_active, sop_meta = self._find_active_sop_entry(now_utc, sop_sched)
            self._sop_contention = bool(sop_meta.get("contention"))
            self._sop_contention_profiles = list(sop_meta.get("profiles") or [])
            self._sop_winner_profile = str(sop_meta.get("winner_profile") or "")
            try:
                self._sop_winner_priority = int(sop_meta.get("winner_priority") or 100)
            except Exception:
                self._sop_winner_priority = 100
            self._sop_winner_reason_code = str(sop_meta.get("winner_reason_code") or "")
            self._sop_winner_reason_detail = str(sop_meta.get("winner_reason_detail") or "")
        except Exception as e:
            log.error("SchedulerEngine: failed to evaluate SOP layer schedule: %s", e)
            sop_active = None
            self._sop_contention = False
            self._sop_contention_profiles = []
            self._sop_winner_profile = ""
            self._sop_winner_priority = 100
            self._sop_winner_reason_code = ""
            self._sop_winner_reason_detail = ""

        source, active_entry, policy_override = self._select_runtime_source(
            now_utc=now_utc,
            hf_entry=hf_active,
            net_entry=net_active,
            sop_entry=sop_active,
            policy_rows=net_sop_policies,
        )
        source_reason_code = ""
        source_reason_detail = ""
        if policy_override and net_active and sop_active:
            policy_name = str(policy_override.get("policy") or "").strip().upper()
            if policy_name == "SOP_PRIORITY" and source == "SOP":
                source_reason_code = "sop_policy_override"
                source_reason_detail = "Saved conflict policy gives SOP priority over Net for this overlap window."
            elif policy_name == "NET_PRIORITY" and source == "NET":
                source_reason_code = "net_policy_override"
                source_reason_detail = "Saved conflict policy keeps Net priority for this overlap window."
        if not source_reason_code:
            source_reason_code, source_reason_detail = self._derive_source_reason(
                source,
                active_entry,
                sop_meta if source == "SOP" else None,
            )
        self._source_reason_code = source_reason_code
        self._source_reason_detail = source_reason_detail

        # Compute next change moment.
        self.next_change_utc = compute_next_change_time(now_utc, hf_active, net_active, sop_active)
        self.next_change_updated.emit(self.next_change_utc)
        self._next_transition_utc = self.next_change_utc
        self._next_source = source
        self._next_net_kind = self._source_net_kind(source, active_entry)
        self._next_transition_freq_hz = None
        self._next_transition_note = ""
        self._next_source_change = False
        if isinstance(self.next_change_utc, datetime.datetime):
            probe_utc = self.next_change_utc + datetime.timedelta(seconds=1)
            try:
                hf_next = self._find_active_hf_entry(probe_utc, hf_sched)
                net_next = self._find_active_net_entry(probe_utc, net_sched)
                sop_next, _sop_next_meta = self._find_active_sop_entry(probe_utc, sop_sched)
                next_source, next_entry, _next_policy = self._select_runtime_source(
                    now_utc=probe_utc,
                    hf_entry=hf_next,
                    net_entry=net_next,
                    sop_entry=sop_next,
                    policy_rows=net_sop_policies,
                )
                self._next_source = next_source
                self._next_net_kind = self._source_net_kind(next_source, next_entry)
                self._next_transition_freq_hz = (
                    self._parse_freq_hz((next_entry.get("frequency") or "").strip())
                    if next_entry
                    else None
                )
                self._next_source_change = next_source != source
                cur_sig = self._entry_transition_signature(active_entry)
                next_sig = self._entry_transition_signature(next_entry)
                if self._next_source_change:
                    self._next_transition_note = f"{source} -> {next_source}"
                elif next_sig != cur_sig and next_source in {"HF", "NET", "SOP"}:
                    self._next_transition_note = f"{next_source} entry update"
            except Exception as e:
                log.debug("SchedulerEngine: next-source preview failed: %s", e)

        prev_source = self.current_source
        net_ended = prev_source == "NET" and source != "NET"
        net_started = prev_source != "NET" and source == "NET"

        if not active_entry:
            # No active schedule; if we previously had something applied,
            # we keep the rig where it was (no auto "clear") but still
            # notify UI that source is NONE.
            if source != self.current_source or force:
                self.current_source = "NONE"
                self.current_schedule_entry = {}
                self.active_entry_changed.emit({}, "NONE")
            self._net_schedule_active = False
            self._net_fldigi_apply_allowed_once = False
            self._net_schedule_started_at = None
            self._net_schedule_entry_key = None
            self._last_scheduler_selection_sig = None
            return

        # Apply to rig (if needed) and emit active_entry_changed
        if net_ended:
            # Clear control conflicts so resume can apply immediately.
            self._manual_qsy_active = False
            self._manual_qsy_entry_key = None
            self._control_backoff_until = 0.0
            self._control_fail_count = 0
            self._pending_entry_key = None
            self._reset_control_if_running("net schedule end (force resume)")
            self._force_retry_after_control = True
            self._forced_retry_attempts_left = max(self._forced_retry_attempts_left, 5)
            self._net_fldigi_apply_allowed_once = False
            self._net_schedule_started_at = None
            self._net_schedule_entry_key = None
            self._varac_wait_prompt_active = False
            self._varac_wait_prompt_entry_key = None

        self._net_schedule_active = bool(source == "NET")
        if net_started:
            self._net_schedule_started_at = time.time()
            self._net_schedule_entry_key = None
            self._net_fldigi_apply_allowed_once = True
        scheduler_transition = False
        if source in {"HF", "NET", "SOP"}:
            selection_sig = (source,) + self._entry_transition_signature(active_entry)
            scheduler_transition = selection_sig != self._last_scheduler_selection_sig
            self._last_scheduler_selection_sig = selection_sig
        else:
            self._last_scheduler_selection_sig = None
        self._apply_schedule_entry(
            active_entry,
            source,
            now_utc=now_utc,
            force=force or net_ended,
            ignore_wait_prompt=net_ended,
            ignore_net_suppression=net_ended,
            scheduler_transition=scheduler_transition,
        )

    def apply_manual_qsy(self, entry: Dict) -> None:
        """
        Apply an immediate user-driven QSY, bypassing suspend and force-applying the change.

        Expects entry to contain at least "frequency" (MHz). Mode/band/vfo/auto_tune
        are honored when provided.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        self._manual_qsy_active = True
        self._manual_qsy_entry_key = (
            (entry.get("band") or "").strip().upper(),
            self._parse_freq_hz((entry.get("frequency") or "").strip()),
            (entry.get("vfo") or "A").strip().upper(),
            (entry.get("primary_js8call_group") or "").strip(),
        )
        self._control_backoff_until = 0.0
        self._control_fail_count = 0
        self._pending_entry_key = None
        self._force_retry_after_control = True
        self._forced_retry_attempts_left = max(self._forced_retry_attempts_left, 5)
        self._apply_schedule_entry(
            entry,
            "QSY",
            now_utc=now,
            force=True,
            ignore_suspend=True,
            ignore_wait_prompt=True,
            ignore_fldigi_busy=True,
        )

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

    def _find_active_sop_entry(
        self,
        now_utc: datetime.datetime,
        sop_sched: List[Dict],
    ) -> Tuple[Optional[Dict], Dict[str, object]]:
        """
        Evaluate active SOP layer rows and resolve overlaps deterministically.

        Arbitration order:
          1) Lower `sop_priority` wins.
          2) Newer profile update timestamp wins.
          3) Later start time wins.
          4) Stable profile/sort/id tie-breakers.
        """
        empty_meta = {
            "contention": False,
            "profiles": [],
            "winner_profile": "",
            "winner_priority": 100,
            "winner_reason_code": "",
            "winner_reason_detail": "",
        }
        if not sop_sched:
            return None, dict(empty_meta)

        weekday_name = _python_weekday_to_day_name(now_utc.weekday())
        weekday_upper = weekday_name.upper()
        prev_day_name = _prev_day_name(weekday_name).upper()
        now_min = now_utc.hour * 60 + now_utc.minute
        candidates: List[Tuple[Tuple[object, ...], Dict]] = []

        for idx, row in enumerate(sop_sched):
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
                        active = now_min >= window_start or now_min < emin
                elif day == prev_day and overnight:
                    active = now_min < emin

                if not active:
                    continue

                priority = int(row.get("sop_priority") or 100)
                updated_epoch = _parse_iso_utc_to_epoch(row.get("sop_profile_updated_utc"))
                profile_id = int(row.get("sop_profile_id") or 0)
                sort_order = int(row.get("sort_order") or 0)
                layer_id = int(row.get("id") or 0)
                rank = (
                    priority,
                    -updated_epoch,
                    -int(smin),
                    profile_id,
                    sort_order,
                    layer_id,
                    idx,
                )
                candidates.append((rank, row))
            except Exception:
                continue

        if not candidates:
            return None, dict(empty_meta)

        candidates.sort(key=lambda item: item[0])
        winner_rank = candidates[0][0]
        winner = candidates[0][1]
        profile_name_map: Dict[int, str] = {}
        for _rank, row in candidates:
            pid = int(row.get("sop_profile_id") or 0)
            if pid <= 0:
                continue
            name = str(row.get("sop_profile_name") or f"SOP-{pid}").strip() or f"SOP-{pid}"
            profile_name_map[pid] = name
        contender_profiles = [profile_name_map[k] for k in sorted(profile_name_map.keys())]
        contention = len(contender_profiles) > 1
        winner_name = str(winner.get("sop_profile_name") or "").strip()
        try:
            winner_priority = int(winner.get("sop_priority") or 100)
        except Exception:
            winner_priority = 100
        winner_reason_code = "single_active_profile"
        winner_reason_detail = "Only one active SOP profile row matched this window."
        if len(candidates) > 1:
            runner_rank = candidates[1][0]
            runner = candidates[1][1]
            if winner_rank[0] != runner_rank[0]:
                winner_reason_code = "priority"
                try:
                    runner_priority = int(runner.get("sop_priority") or 100)
                except Exception:
                    runner_priority = 100
                winner_reason_detail = f"Priority {winner_priority} wins over {runner_priority}."
            elif winner_rank[1] != runner_rank[1]:
                winner_reason_code = "updated_utc"
                winner_reason_detail = "Winner profile is the most recently updated among equal priority rows."
            elif winner_rank[2] != runner_rank[2]:
                winner_reason_code = "start_time"
                winner_reason_detail = "Later start time wins among equal priority/update rows."
            else:
                winner_reason_code = "stable_tiebreak"
                winner_reason_detail = "Stable tie-break (profile/sort/id) selected the winner."
        return winner, {
            "contention": contention,
            "profiles": contender_profiles,
            "winner_profile": winner_name,
            "winner_priority": winner_priority,
            "winner_reason_code": winner_reason_code,
            "winner_reason_detail": winner_reason_detail,
        }

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

    def _resolve_rig_mode(self, entry: Dict) -> Optional[str]:
        mode_txt = (entry.get("mode") or "").strip()
        band_txt = (entry.get("band") or "").strip()
        voice_hint = (entry.get("fldigi_mode") or "").strip()
        return resolve_rig_mode(mode_txt, band_txt, voice_hint=voice_hint)

    def _status_poll_flrig_frequency(self) -> Optional[int]:
        """
        Lightweight FLRig status poll with short-lived caching/backoff.
        """
        if not self.rig or not hasattr(self.rig, "get_vfo_frequency"):
            self._status_flrig_freq_hz = None
            return None
        now_ts = time.time()
        if now_ts - self._status_flrig_freq_ts < self._status_poll_ttl_s:
            return self._status_flrig_freq_hz
        if now_ts < self._status_flrig_retry_ts:
            return self._status_flrig_freq_hz
        self._status_flrig_freq_ts = now_ts
        freq = self._current_rig_frequency(control_mode="FLRIG", status_cached=False)
        if isinstance(freq, (int, float)) and freq > 0:
            self._status_flrig_freq_hz = int(freq)
            self._status_flrig_retry_ts = 0.0
            return self._status_flrig_freq_hz
        self._status_flrig_freq_hz = None
        self._status_flrig_retry_ts = now_ts + self._status_poll_retry_s
        return None

    def _status_poll_flrig_ptt(self) -> bool:
        """
        Lightweight FLRig PTT status poll with shared retry backoff.
        """
        if not self.rig or not hasattr(self.rig, "get_ptt"):
            self._status_flrig_ptt = False
            return False
        now_ts = time.time()
        if now_ts - self._status_flrig_ptt_ts < self._status_poll_ttl_s:
            return self._status_flrig_ptt
        if now_ts < self._status_flrig_retry_ts:
            return self._status_flrig_ptt
        self._status_flrig_ptt_ts = now_ts
        try:
            self._status_flrig_ptt = bool(self.rig.get_ptt())
        except Exception:
            self._status_flrig_ptt = False
        return self._status_flrig_ptt

    def _current_rig_frequency(
        self,
        *,
        control_mode: Optional[str] = None,
        status_cached: bool = False,
    ) -> Optional[int]:
        """
        Query current control frequency in Hz.

        When control_mode is specified, query only that backend. The legacy
        fallback path (FLRig then JS8) is kept for compatibility when no mode
        hint is provided.
        """
        mode = (control_mode or "").strip().upper()
        if control_mode is not None and mode not in {"FLRIG", "JS8CALL"}:
            return None
        if mode == "FLRIG":
            if status_cached:
                return self._status_poll_flrig_frequency()
            try:
                if self.rig and hasattr(self.rig, "get_vfo_frequency"):
                    freq = self.rig.get_vfo_frequency()
                    if freq:
                        return freq
            except Exception as e:
                log.error("SchedulerEngine: failed to read current rig frequency: %s", e)
            return None
        if mode == "JS8CALL":
            try:
                if self.js8 and hasattr(self.js8, "get_frequency"):
                    return self.js8.get_frequency()  # type: ignore[no-any-return]
            except Exception as e:
                log.debug("SchedulerEngine: failed to read JS8Call frequency: %s", e)
            return None

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
        ignore_wait_prompt: bool = False,
        ignore_net_suppression: bool = False,
        ignore_fldigi_busy: bool = False,
        apply_js8_offset: bool = True,
        apply_fldigi: bool = True,
        scheduler_transition: bool = False,
    ) -> None:
        """
        Apply a single schedule entry to the rig.

        We avoid re-sending the same frequency/band data unless
        something actually changed, unless 'force' is True.
        """
        effective_entry, _og = self._entry_with_operating_group_overrides(entry)
        # Extract fields
        band = (effective_entry.get("band") or "").strip().upper()
        freq_text = (effective_entry.get("frequency") or "").strip()
        js8_group = (effective_entry.get("primary_js8call_group") or "").strip()
        comment = (effective_entry.get("comment") or "").strip()
        vfo_raw = (effective_entry.get("vfo") or "A").strip().upper()
        vfo: Optional[str] = vfo_raw if vfo_raw in ("A", "B") else None
        auto_tune = bool(effective_entry.get("auto_tune"))
        rig_mode = self._resolve_rig_mode(effective_entry)

        # Update internal state regardless of whether we can actually
        # command the rig. This allows UI elements (Net Control tabs,
        # countdown timers, etc.) to reflect the upcoming change even
        # when running in Manual mode.
        self.current_source = source
        self.current_schedule_entry = effective_entry
        self._scheduled_vfo = vfo
        if source != "QSY" and self._manual_qsy_active:
            log.debug("SchedulerEngine: manual QSY active; skipping scheduled frequency change.")
            self.active_entry_changed.emit(effective_entry, source)
            return

        control_mode = self._control_mode()
        # If we're not in JS8CALL mode and have no rig backend, just update UI state.
        if control_mode != "JS8CALL" and self.rig is None:
            self.active_entry_changed.emit(effective_entry, source)
            return

        if control_mode == "MANUAL":
            log.debug("SchedulerEngine: manual control selected; no frequency commands sent.")
            self.active_entry_changed.emit(effective_entry, source)
            return
        if control_mode == "NONE":
            log.debug(
                "SchedulerEngine: control backend unavailable for mode=%s; not sending commands.",
                self.settings.get("control_via", "FLRig"),
            )
            self.active_entry_changed.emit(effective_entry, source)
            return
        # Respect temporary suspend timer (QSY/Suspend button)
        if not ignore_suspend and self._scheduling_suspended(now_utc or datetime.datetime.now(datetime.timezone.utc)):
            dt = self._suspend_until_dt()
            until_txt = dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%MZ") if dt else ""
            log.debug("SchedulerEngine: scheduling suspended until %s; skipping frequency change.", until_txt)
            self.active_entry_changed.emit(effective_entry, source)
            return

        # Scheduler master switch (from Settings tab)
        try:
            if not bool(self.settings.get("use_scheduler", True)):
                log.debug("SchedulerEngine: scheduler disabled in settings; no frequency changes sent.")
                self.active_entry_changed.emit(effective_entry, source)
                return
        except Exception:
            pass
        # Parse frequency text early to support VarAC wait prompts.
        if not freq_text:
            log.warning("SchedulerEngine: schedule entry missing 'frequency'; skipping.")
            return
        freq_hz = self._parse_freq_hz(freq_text)
        if freq_hz is None:
            log.error("SchedulerEngine: invalid frequency text '%s'; skipping.", freq_text)
            return
        current_freq_hz = self._current_rig_frequency(control_mode=control_mode)
        freq_matches = current_freq_hz is not None and abs(current_freq_hz - freq_hz) <= 5
        want_freq_change = current_freq_hz is None or not freq_matches
        varac_status = self._varac_status()
        if self._varac_wait_prompt_active and not bool(varac_status.get("waiting_for_frequency")):
            self._varac_wait_prompt_active = False
            self._varac_wait_prompt_entry_key = None
            self.varac_wait_cleared.emit()
        prompt_key = (band, freq_hz, vfo, js8_group)
        if (
            source != "NET"
            and want_freq_change
            and bool(varac_status.get("waiting_for_frequency"))
            and not ignore_wait_prompt
        ):
            if (not self._varac_wait_prompt_active) or (self._varac_wait_prompt_entry_key != prompt_key):
                self._varac_wait_prompt_active = True
                self._varac_wait_prompt_entry_key = prompt_key
                self.varac_wait_detected.emit({"entry": effective_entry, "source": source})
            self.active_entry_changed.emit(effective_entry, source)
            return
        fldigi_delay, fldigi_reason = self._should_delay_for_fldigi(
            entry_key=prompt_key,
            source=source,
            want_freq_change=want_freq_change,
            ignore_fldigi_busy=ignore_fldigi_busy,
        )
        # Safety: avoid changing frequency while a backend is busy transmitting.
        busy_reasons = []
        if control_mode == "FLRIG" and self.rig and hasattr(self.rig, "get_ptt"):
            try:
                if self.rig.get_ptt():
                    busy_reasons.append("FLRig PTT is active")
            except Exception as e:
                log.warning("SchedulerEngine: get_ptt() failed: %s", e)

        if source != "NET" and not self._js8_busy_ok():
            busy_reasons.append("JS8Call is busy (RX/TX)")

        if source != "NET" and not self._varac_busy_ok(status=varac_status):
            varac_reason = str(varac_status.get("reason") or "").strip()
            if varac_reason:
                busy_reasons.append(f"VarAC is busy ({varac_reason})")
            else:
                busy_reasons.append("VarAC is busy")

        if fldigi_delay:
            reason = "FLDigi RX activity"
            if fldigi_reason:
                reason = f"FLDigi RX activity ({fldigi_reason})"
            busy_reasons.append(reason)

        if busy_reasons:
            log.warning(
                "SchedulerEngine: skipping frequency change for %s schedule due to activity: %s",
                source,
                "; ".join(busy_reasons),
            )
            self.active_entry_changed.emit(effective_entry, source)
            return

        log.info(
            "SchedulerEngine applying entry (%s) from %s: band=%s freq=%s vfo=%s mode=%s comment=%s",
            control_mode,
            source,
            band,
            freq_text,
            vfo or "-",
            rig_mode or "-",
            comment,
        )

        fldigi_center = self._expected_fldigi_offset(effective_entry)
        js8_tune = None
        if not apply_fldigi:
            fldigi_center = None

        # Avoid redundant commands
        entry_key = (
            band,
            freq_hz,
            fldigi_center,
            js8_tune,
            vfo,
            js8_group,
            rig_mode,
        )
        if self._net_corrections_suppressed() and not force and not ignore_net_suppression:
            if source == "NET" and self._last_entry_key != entry_key:
                self._net_fldigi_apply_allowed_once = True
            if self._manual_net_fldigi_active or self._manual_net_js8_active:
                log.debug("SchedulerEngine: net active; skipping schedule enforcement.")
                self.active_entry_changed.emit(effective_entry, source)
                return
            if self._last_entry_key == entry_key:
                log.debug("SchedulerEngine: net schedule active; skipping corrections for current entry.")
                self.active_entry_changed.emit(effective_entry, source)
                return
        if source in ("HF", "NET", "SOP") and scheduler_transition and apply_fldigi:
            # Only real scheduler row transitions should re-arm one-shot FLDigi
            # enforcement. Internal reapply key differences (resume/retry/
            # frequency-only actions) must not behave like schedule transitions.
            self._fldigi_force_apply_once = True
        if self._pending_entry_key == entry_key and not force:
            log.debug("SchedulerEngine: control action skipped (pending entry key).")
            self.active_entry_changed.emit(effective_entry, source)
            return
        already_applied = (
            self._last_entry_key == entry_key and self._last_source == source
        )
        if not force and already_applied:
            log.debug("SchedulerEngine: schedule entry already applied; skipping re-apply.")
            self.active_entry_changed.emit(effective_entry, source)
            return
        if apply_fldigi:
            self._update_desired_fldigi_settings(effective_entry)
        if current_freq_hz is not None and not freq_matches:
            log.info(
                "SchedulerEngine: rig currently at %d Hz, target %d Hz; reapplying schedule.",
                current_freq_hz,
                freq_hz,
            )

        js8_offset = self._js8_offset_setting() if apply_js8_offset else None
        queued = self._queue_control_action(
            control_mode=control_mode,
            entry_key=entry_key,
            source=source,
            freq_hz=freq_hz,
            band=band,
            mode=rig_mode,
            vfo=vfo,
            auto_tune=auto_tune,
            js8_offset=js8_offset,
            js8_group=js8_group,
        )
        if not queued:
            log.debug("SchedulerEngine: control action skipped (pending/backoff).")
            self._record_latest_intent(
                entry,
                source,
                now_utc=now_utc,
                force=force,
                ignore_suspend=ignore_suspend,
                ignore_wait_prompt=ignore_wait_prompt,
                ignore_fldigi_busy=ignore_fldigi_busy,
                apply_js8_offset=apply_js8_offset,
                apply_fldigi=apply_fldigi,
            )
            should_force_retry = bool(force or source == "QSY" or self._force_retry_after_control)
            if should_force_retry:
                self._forced_retry_attempts_left = max(self._forced_retry_attempts_left, 5)
                if self._control_future is not None and not self._control_future.done():
                    self._force_retry_after_control = True
                self._schedule_forced_retry()
        # Update UI state immediately regardless of control action.
        self.active_entry_changed.emit(effective_entry, source)
