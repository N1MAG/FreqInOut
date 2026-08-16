from __future__ import annotations

import datetime
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple

from PySide6.QtCore import QCoreApplication, QObject, QTimer, Signal

from freqinout.core.logger import log
from freqinout.core.busy_evidence_service import BusyEvidenceService
from freqinout.core.dependency_health import get_dependency_health_registry
from freqinout.core.js8_defaults import random_default_js8_offset_hz
from freqinout.core.mode_utils import normalize_operating_group_mode, resolve_rig_mode
from freqinout.core.multi_radio_store import MultiRadioStore, normalize_rf_guard_mode, settings_db_path
from freqinout.core.ptt_conflict_service import PttConflictService
from freqinout.core.radio_status_poll_coordinator import RadioStatusPollCoordinator
from freqinout.core.scheduler_manual_control_service import SchedulerManualControlService
from freqinout.core.scheduler_events import record_scheduler_event
from freqinout.core.schedule_targeting import (
    normalize_schedule_target_fields,
    schedule_row_matches_target_context,
)
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.shared_state import BusyEvidence, PttConflictEvidence, SchedulerManualControlState, SchedulerManualTarget
from freqinout.core.software_status_service import SoftwareStatusService
from freqinout.core.station_runtime_manager import DeviceSettingsProxy
from freqinout.radio_interface.rigctl_client import FrequencyCommand, RigControlClient, rig_control_client_from_settings
from freqinout.radio_interface.js8_status import JS8ControlClient, VarACStatusClient
from freqinout.radio_interface.js8_rx_hub import JS8RxHub


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _hz_to_amateur_band(freq_hz: Optional[float]) -> str:
    if not freq_hz:
        return ""
    try:
        mhz = float(freq_hz) / 1_000_000.0
    except Exception:
        return ""
    bands = [
        ("160M", 1.8, 2.0),
        ("80M", 3.5, 4.0),
        ("60M", 5.0, 5.5),
        ("40M", 7.0, 7.3),
        ("30M", 10.1, 10.15),
        ("20M", 14.0, 14.35),
        ("17M", 18.068, 18.168),
        ("15M", 21.0, 21.45),
        ("12M", 24.89, 24.99),
        ("10M", 28.0, 29.7),
        ("6M", 50.0, 54.0),
        ("2M", 144.0, 148.0),
    ]
    for name, lo, hi in bands:
        if lo <= mhz <= hi:
            return name
    return ""


@dataclass
class StationActualState:
    flrig_freq_hz: Optional[int] = None
    flrig_ptt_active: bool = False
    flrig_ptt_known: bool = False
    flrig_ptt_age_s: Optional[float] = None
    flrig_ptt_stale: bool = False
    flrig_vfo: Optional[str] = None
    js8_freq_hz: Optional[int] = None
    js8_offset_hz: Optional[int] = None
    js8_offset_age_s: Optional[float] = None
    js8_offset_stale: bool = False
    fldigi_mode: Optional[str] = None
    fldigi_offset_hz: Optional[int] = None
    actual_frequency_hz: Optional[int] = None
    actual_frequency_source: str = "unknown"
    checked_ts: float = 0.0
    stale: bool = False
    errors: Dict[str, str] = field(default_factory=dict)


@dataclass
class OffScheduleState:
    off_schedule: bool = False
    status_unknown: bool = False
    flags: Dict[str, bool] = field(
        default_factory=lambda: {
            "frequency": False,
            "mode": False,
            "offset": False,
            "fldigi_offset": False,
            "vfo": False,
        }
    )
    target_frequency_hz: Optional[int] = None
    actual_frequency_hz: Optional[int] = None
    actual_frequency_source: str = "unknown"
    target_vfo: Optional[str] = None
    actual_vfo: Optional[str] = None
    vfo_verified: bool = False
    reasons: List[str] = field(default_factory=list)


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
    coordination_conflict_detected = Signal(dict)
    coordination_conflict_cleared = Signal()
    _scheduler_thread_call = Signal(object)

    def __init__(
        self,
        parent: Optional[QObject] = None,
        rig: Optional[RigControlClient] = None,
        js8: Optional[JS8ControlClient] = None,
        varac: Optional[object] = None,
        fldigi_log: Optional[object] = None,
        station_runtime_manager: Optional[object] = None,
        poll_interval_ms: int = 5_000,
    ) -> None:
        super().__init__(parent)
        self._assert_scheduler_thread_contract()
        self.settings = SettingsManager()
        self._software_status = SoftwareStatusService(self.settings)
        self._manual_control_service = SchedulerManualControlService(MultiRadioStore(settings_db_path()))
        self._busy_evidence_service = BusyEvidenceService(MultiRadioStore(settings_db_path()))
        self._ptt_conflict_service = PttConflictService(MultiRadioStore(settings_db_path()))
        self._status_poll_coordinator = RadioStatusPollCoordinator(
            ttl_seconds=0.8,
            retry_seconds=4.0,
            time_fn=time.time,
        )
        self._scheduler_thread_call_connected = False
        self._connect_scheduler_thread_call()
        self.rig: Optional[RigControlClient] = rig
        self.js8: Optional[JS8ControlClient] = js8
        self.varac: Optional[object] = varac
        self.fldigi_log: Optional[object] = fldigi_log
        self.station_runtime_manager = station_runtime_manager
        self._runtime_scheduler_enabled_override: Optional[bool] = None
        self._runtime_timer_policy_override: Dict[str, str] = {}

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
        self._frequency_prompt_last_by_entry: Dict[Tuple, float] = {}
        self._off_schedule_prompt_suppress_until_by_key: Dict[Tuple[object, ...], float] = {}
        self._last_off_schedule_prompt_by_radio: Dict[int, Dict[str, object]] = {}
        self._prompt_state = {
            "frequency": {"last_prompt_ts": 0.0},
            "mode": {"last_prompt_ts": 0.0},
            "offset": {"last_prompt_ts": 0.0},
            "fldigi_offset": {"last_prompt_ts": 0.0},
        }
        self._last_fldigi_offset_prompt_sig: Optional[Tuple[Optional[int], Optional[int]]] = None
        self._varac_wait_prompt_active: bool = False
        self._varac_wait_prompt_entry_key: Optional[Tuple] = None
        self._coordination_prompt_active: bool = False
        self._coordination_prompt_signature: Optional[str] = None
        self._coordination_prompt_payload: Optional[Dict[str, object]] = None
        self._coordination_prompt_suppressed_signature: Optional[str] = None
        self._status_poll_ttl_s: float = 0.8
        self._status_poll_retry_s: float = 4.0
        self._status_flrig_freq_hz: Optional[int] = None
        self._status_flrig_freq_ts: float = 0.0
        self._status_flrig_ptt: bool = False
        self._status_flrig_ptt_ts: float = 0.0
        self._status_flrig_ptt_known: bool = False
        self._status_flrig_ptt_max_age_s: float = 30.0
        self._status_flrig_vfo: Optional[str] = None
        self._status_flrig_vfo_ts: float = 0.0
        self._status_flrig_retry_ts: float = 0.0
        self._status_js8_freq_hz: Optional[int] = None
        self._status_js8_offset_hz: Optional[int] = None
        self._status_js8_freq_ts: float = 0.0
        self._status_js8_offset_ts: float = 0.0
        self._status_summary_cache: Optional[Dict[str, object]] = None
        self._status_summary_cache_ts: float = 0.0
        self._status_summary_cache_ttl_s: float = 2.5
        self._status_summary_external_ts: float = 0.0
        self._status_snapshot_refresh_ts: float = 0.0
        self._status_snapshot_refresh_interval_s: float = 5.0
        self._status_snapshot_future = None
        self._status_snapshot_started_at: Optional[float] = None
        self._status_snapshot_timeout_s: float = 15.0
        self._status_snapshot_timeout_reported: bool = False
        self._last_js8_shadow_comparison: Dict[str, object] = {}
        self._last_varac_status: Dict[str, object] = {"busy": False, "waiting_for_frequency": False, "reason": None}
        self._last_varac_status_stale: bool = False
        self._last_varac_status_detail: str = ""
        self._last_js8_busy: bool = False
        self._last_js8_status_stale: bool = False
        self._last_js8_status_detail: str = ""
        self._last_ptt_active: bool = False
        self._fldigi_mode_cache: Optional[str] = None
        self._fldigi_mode_cache_ts: float = 0.0
        self._fldigi_offset_cache: Optional[int] = None
        self._fldigi_offset_cache_ts: float = 0.0
        self._fldigi_status_cache_ttl_s: float = 5.0
        self._control_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="freqinout-control")
        self._status_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="freqinout-status")
        self._control_future = None
        self._control_future_token: int = 0
        self._control_future_started_at: Optional[float] = None
        self._control_timeout_s: float = 8.0
        self._control_timeout_reported: bool = False
        self._control_backoff_until: float = 0.0
        self._control_fail_count: int = 0
        self._pending_entry_key: Optional[Tuple] = None
        self._force_retry_after_control: bool = False
        self._forced_retry_attempts_left: int = 0
        self._latest_intent: Optional[Dict[str, object]] = None
        self._latest_intents_by_radio: Dict[int, Dict[str, object]] = {}
        self._latest_intent_ts: float = 0.0
        self._retry_scheduled: bool = False
        self._manual_qsy_active: bool = False
        self._manual_qsy_entry_key: Optional[Tuple] = None
        self._manual_qsy_radio_id: Optional[int] = None
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
            "vfo": False,
        }
        self._fldigi_available_cache: Optional[bool] = None
        self._fldigi_available_ts: float = 0.0
        self._fldigi_busy_entry_key: Optional[Tuple] = None
        self._fldigi_busy_since_ts: Optional[float] = None
        self._fldigi_busy_last_reason: Optional[str] = None
        self._fldigi_busy_watchdog_s: float = 180.0
        self._fldigi_busy_check_source: str = ""
        self._fldigi_busy_check_target_hz: Optional[int] = None
        self._fldigi_busy_check_result: Optional[Dict[str, object]] = None
        self._fldigi_busy_check_in_flight: bool = False
        self._fldigi_busy_check_token: int = 0
        self._fldigi_busy_check_next_ts: float = 0.0
        self._fldigi_busy_check_interval_s: float = 5.0
        self._js8_busy_entry_key: Optional[Tuple] = None
        self._js8_busy_since_ts: Optional[float] = None
        self._varac_busy_entry_key: Optional[Tuple] = None
        self._varac_busy_since_ts: Optional[float] = None
        self._varac_wait_since_ts: Optional[float] = None
        self._external_busy_watchdog_s: float = 90.0
        self._health = get_dependency_health_registry()
        self._scheduler_event_last: Dict[Tuple[object, ...], float] = {}

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
        self._current_entry_end_utc: Optional[datetime.datetime] = None
        self._next_entry_start_utc: Optional[datetime.datetime] = None
        self._next_entry_source: str = "NONE"
        self._next_entry_freq_hz: Optional[int] = None
        self._schedule_gap_seconds: Optional[int] = None
        self._last_scheduler_selection_sig: Optional[Tuple] = None
        self._active_schedule_lane_rows_cache: Optional[Dict[str, object]] = None
        self._active_schedule_lane_rows_cache_ttl_s: float = 0.75
        self._shutdown_requested: bool = False

        self.timer = QTimer(self)
        self.timer.setInterval(poll_interval_ms)
        self._timer_connected = False
        self._connect_timer()

        # If a rig was provided, we can optionally sanity-check it
        # (non-fatal if unavailable).
        if self.rig is not None:
            try:
                if hasattr(rig, "is_available") and not rig.is_available():
                    log.warning("SchedulerEngine: rig control client is not available at init.")
            except Exception as e:
                log.error("SchedulerEngine: error probing rig control availability: %s", e)
        self._ensure_js8_offset_default()

    def _run_scheduler_thread_call(self, callback: object) -> None:
        if not callable(callback):
            return
        try:
            callback()
        except Exception as e:
            log.debug("SchedulerEngine: queued scheduler-thread callback failed: %s", e)

    def _connect_scheduler_thread_call(self) -> None:
        if self._scheduler_thread_call_connected:
            return
        try:
            self._scheduler_thread_call.connect(self._run_scheduler_thread_call)
            self._scheduler_thread_call_connected = True
        except Exception:
            self._scheduler_thread_call_connected = False

    def _disconnect_scheduler_thread_call(self) -> None:
        if not self._scheduler_thread_call_connected:
            return
        try:
            self._scheduler_thread_call.disconnect(self._run_scheduler_thread_call)
        except Exception:
            pass
        self._scheduler_thread_call_connected = False

    def _connect_timer(self) -> None:
        if self._timer_connected:
            return
        try:
            self.timer.timeout.connect(self._on_timer)
            self._timer_connected = True
        except Exception:
            self._timer_connected = False

    def _disconnect_timer(self) -> None:
        if not self._timer_connected:
            return
        try:
            self.timer.timeout.disconnect(self._on_timer)
        except Exception:
            pass
        self._timer_connected = False

    def _queue_scheduler_thread_call(self, callback: Callable[[], None]) -> None:
        self._scheduler_thread_call.emit(callback)

    def set_runtime_scheduler_enabled(self, enabled: Optional[bool]) -> None:
        self._runtime_scheduler_enabled_override = None if enabled is None else bool(enabled)

    def set_runtime_timer_policy(self, policy: Optional[Mapping[str, Any]]) -> None:
        if not isinstance(policy, Mapping):
            self._runtime_timer_policy_override = {}
            return
        allowed_modes = {"On Schedule Change", "Prompt"}
        allowed_intervals = {"Hourly", "Every 5 minutes", "Every 10 minutes", "Every 15 minutes", "Every 30 minutes"}
        values: Dict[str, str] = {}
        for key in ("freq_enforcement_mode", "fldigi_enforcement_mode", "js8_enforcement_mode"):
            value = str(policy.get(key, "") or "").strip()
            if value in allowed_modes:
                values[key] = value
        for key in ("freq_prompt_interval", "fldigi_prompt_interval", "js8_prompt_interval"):
            value = str(policy.get(key, "") or "").strip()
            if value in allowed_intervals:
                values[key] = value
        self._runtime_timer_policy_override = values

    def _scheduler_enabled(self) -> bool:
        override = self._runtime_scheduler_enabled_override
        if override is not None:
            return bool(override)
        try:
            return bool(self.settings.get("use_scheduler", True))
        except Exception:
            return True

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
        if mode == "RIGCTLD":
            if self.rig is None:
                return "NONE"
            return "RIGCTLD"
        if mode == "JS8CALL":
            if self.js8 is None:
                return "NONE"
            return "JS8CALL" if self._js8_running() else "NONE"
        return "NONE"

    def _control_mode_for_context(
        self,
        settings: Optional[object],
        *,
        rig: Optional[RigControlClient],
        js8: Optional[JS8ControlClient],
    ) -> str:
        if settings is self.settings and rig is self.rig and js8 is self.js8:
            return self._control_mode()
        getter = getattr(settings, "get", None)
        try:
            raw_mode = getter("control_via", "FLRig") if callable(getter) else self.settings.get("control_via", "FLRig")
        except Exception:
            raw_mode = self.settings.get("control_via", "FLRig")
        mode = (raw_mode or "FLRig").upper()
        if mode == "MANUAL":
            return "MANUAL"
        if mode == "FLRIG":
            if rig is None:
                return "NONE"
            return "FLRIG" if self._flrig_running() else "NONE"
        if mode == "RIGCTLD":
            return "RIGCTLD" if rig is not None else "NONE"
        if mode == "JS8CALL":
            if js8 is None:
                return "NONE"
            return "JS8CALL" if self._js8_running() else "NONE"
        return "NONE"

    def _control_context_for_entry(
        self,
        entry: Optional[Dict],
    ) -> Tuple[Optional[RigControlClient], Optional[JS8ControlClient], Optional[object], object, Optional[int]]:
        radio_id = self._entry_manual_control_radio_id(entry)
        if not radio_id:
            return self.rig, self.js8, self.varac, self.settings, None
        manager = getattr(self, "station_runtime_manager", None)
        if manager is None:
            return self._control_context_from_device_profile(radio_id)
        has_runtime_lookup = hasattr(manager, "get_runtime_for_device") or hasattr(manager, "_runtimes")
        if not has_runtime_lookup:
            return self._control_context_from_device_profile(radio_id)
        runtime = None
        try:
            if hasattr(manager, "get_runtime_for_device"):
                runtime = manager.get_runtime_for_device(int(radio_id))
            elif hasattr(manager, "_runtimes"):
                runtime = getattr(manager, "_runtimes", {}).get(int(radio_id))
        except Exception as exc:
            log.debug("SchedulerEngine: failed resolving runtime for radio %s: %s", radio_id, exc)
            runtime = None
        if runtime is None:
            log.warning(
                "SchedulerEngine: no runtime found for targeted radio %s; trying profile-backed control client.",
                radio_id,
            )
            return self._control_context_from_device_profile(radio_id)

        runtime_settings = getattr(runtime, "settings_proxy", None) or self.settings
        return (
            getattr(runtime, "rig_client", None),
            getattr(runtime, "js8_control_client", None),
            getattr(runtime, "varac_status_client", None),
            runtime_settings,
            radio_id,
        )

    def _control_context_from_device_profile(
        self,
        radio_id: int,
    ) -> Tuple[Optional[RigControlClient], Optional[JS8ControlClient], Optional[object], object, Optional[int]]:
        """
        Resolve a targeted control context directly from the configured radio.

        Targeted commands must never fall back to the singleton/global rig
        client. If the runtime manager is rebuilding, missing, or stale, this
        profile-backed path creates a fresh client for the selected radio's
        endpoint. If that cannot be done, the caller receives no client and the
        command is skipped instead of being sent to another radio.
        """
        settings_proxy: object = self.settings
        profile: Optional[Mapping[str, Any]] = None
        try:
            store = MultiRadioStore(settings_db_path())
            loaded = store.get_device_profile(int(radio_id))
            if isinstance(loaded, Mapping):
                profile = loaded
        except Exception as exc:
            log.debug("SchedulerEngine: failed loading profile for radio %s: %s", radio_id, exc)
        if not profile:
            log.warning(
                "SchedulerEngine: no configured profile found for targeted radio %s; refusing fallback control client.",
                radio_id,
            )
            return None, None, None, settings_proxy, radio_id

        settings_proxy = DeviceSettingsProxy(profile, self.settings)
        backend = str(profile.get("control_backend", "") or "").strip().lower()
        rig_client: Optional[RigControlClient] = None
        js8_client: Optional[JS8ControlClient] = None
        if backend in {"flrig", "rigctld"}:
            try:
                rig_client = rig_control_client_from_settings(settings_proxy)
            except Exception as exc:
                log.warning(
                    "SchedulerEngine: failed building %s control client for radio %s: %s",
                    backend or "rig",
                    radio_id,
                    exc,
                )
        elif backend == "js8call":
            try:
                host = str(settings_proxy.get("js8_host", "127.0.0.1") or "127.0.0.1")
                port = int(settings_proxy.get("js8_port", 2442) or 2442)
                js8_client = JS8ControlClient(host=host, port=port, settings=settings_proxy)
            except Exception as exc:
                log.warning(
                    "SchedulerEngine: failed building JS8Call control client for radio %s: %s",
                    radio_id,
                    exc,
                )
        else:
            if self._target_may_use_singleton_control_client(radio_id):
                return self.rig, self.js8, self.varac, self.settings, radio_id
            log.warning(
                "SchedulerEngine: targeted radio %s uses unsupported control backend %r; no command will be sent.",
                radio_id,
                backend or "manual",
            )
        return rig_client, js8_client, None, settings_proxy, radio_id

    def _target_may_use_singleton_control_client(self, radio_id: int) -> bool:
        """
        Compatibility path for single-runtime tests and migrated single-rig use.

        This deliberately rejects multi-active-radio configurations. When two
        radios are active, the singleton client is ambiguous and using it for a
        targeted command can key the wrong FLRig/JS8/RigCtl instance.
        """
        if self.rig is None and self.js8 is None and self.varac is None:
            return False
        try:
            store = MultiRadioStore(settings_db_path())
            active = store.list_runtime_active_device_profiles()
        except Exception as exc:
            log.debug("SchedulerEngine: could not inspect active radio count for singleton fallback: %s", exc)
            return False
        active_ids: List[int] = []
        for profile in active:
            try:
                profile_id = int(profile.get("id") or 0)
            except Exception:
                profile_id = 0
            if profile_id > 0:
                active_ids.append(profile_id)
        if not active_ids:
            return False
        if len(set(active_ids)) > 1:
            return False
        return int(active_ids[0]) == int(radio_id)

    def _cached_control_mode(self) -> str:
        mode = (self.settings.get("control_via", "FLRig") or "FLRig").upper()
        if mode == "MANUAL":
            return "MANUAL"
        if mode in {"FLRIG", "RIGCTLD"}:
            return mode if self.rig is not None else "NONE"
        if mode == "JS8CALL":
            return "JS8CALL" if self.js8 is not None else "NONE"
        return "NONE"

    def _js8_offset_setting(self) -> int:
        try:
            val = int(self.settings.get("js8_offset_hz", 0) or 0)
            return val
        except Exception:
            return 0

    def _js8_offset_authority_active(self, entry: Optional[Dict], control_mode: Optional[str] = None) -> bool:
        mode = (control_mode or self._cached_control_mode()).strip().upper()
        if mode not in {"FLRIG", "RIGCTLD", "JS8CALL"}:
            return False
        if self._js8_offset_setting() <= 0:
            return False
        if mode == "JS8CALL":
            return True
        row = entry or self.current_schedule_entry or {}
        if str(row.get("primary_js8call_group") or "").strip():
            return True
        if str(row.get("js8_offset") or row.get("js8call_offset") or "").strip():
            return True
        return bool(self.js8 and self._js8_running())

    def _primary_schedule_target_context(self) -> Tuple[Optional[int], Optional[int]]:
        manager = getattr(self, "station_runtime_manager", None)
        if manager is not None:
            try:
                runtime = manager.get_primary_runtime() if hasattr(manager, "get_primary_runtime") else None
            except Exception:
                runtime = None
            if runtime is not None:
                try:
                    profile = runtime.profile if isinstance(runtime.profile, dict) else {}
                    assignment = runtime.assignment if isinstance(runtime.assignment, dict) else {}
                    device_profile_id = int(profile.get("id", 0) or 0)
                    operating_profile_id = assignment.get("operating_profile_id")
                    return (
                        device_profile_id or None,
                        int(operating_profile_id) if operating_profile_id not in (None, "") else None,
                    )
                except Exception:
                    pass
        try:
            store = MultiRadioStore(settings_db_path())
            primary = store.get_runtime_primary_device_profile()
            if not primary:
                return None, None
            device_profile_id = int(primary.get("id", 0) or 0)
            assignment = store.get_effective_assignment_for_device(device_profile_id)
            operating_profile_id = assignment.get("operating_profile_id") if assignment else None
            return (
                device_profile_id or None,
                int(operating_profile_id) if operating_profile_id not in (None, "") else None,
            )
        except Exception:
            return None, None

    def _primary_manual_control_radio_id(self) -> Optional[int]:
        device_profile_id, _operating_profile_id = self._primary_schedule_target_context()
        try:
            return int(device_profile_id) if device_profile_id not in (None, "") else None
        except Exception:
            return None

    def _entry_manual_control_radio_id(self, entry: Optional[Dict]) -> Optional[int]:
        row = entry or {}
        for key in ("target_device_profile_id", "device_profile_id", "radio_profile_id"):
            try:
                value = int(row.get(key) or 0)
            except Exception:
                value = 0
            if value > 0:
                return value
        return self._primary_manual_control_radio_id()

    def _filter_rows_for_runtime_target(
        self,
        rows: List[Dict],
        *,
        primary_device_profile_id: Optional[int],
        primary_operating_profile_id: Optional[int],
    ) -> List[Dict]:
        filtered: List[Dict] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            row = normalize_schedule_target_fields(raw)
            if schedule_row_matches_target_context(
                row,
                device_profile_id=primary_device_profile_id,
                operating_profile_id=primary_operating_profile_id,
            ):
                filtered.append(row)
        return filtered

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
            self._status_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="freqinout-status",
            )
        self._shutdown_requested = False
        self._connect_scheduler_thread_call()
        self._connect_timer()
        self._maybe_refresh_external_status_snapshot(force=True)
        self._apply_js8_offset_startup()
        self._clear_startup_manual_qsy_states()
        # Perform an immediate evaluation so UI sees something right away.
        # In multi-radio mode, assigned plan lanes are the source of truth;
        # the legacy singleton evaluator is only a fallback when no radio
        # lanes are active.
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if not self._apply_active_schedule_lanes(now_utc=now_utc, force=True):
                self._evaluate(now_utc=now_utc)
        except Exception as e:
            log.error("SchedulerEngine initial evaluate failed: %s", e)
        if not self.timer.isActive():
            self.timer.start()

    def stop(self) -> None:
        """Stop periodic schedule evaluation."""
        self._shutdown_requested = True
        if self.timer.isActive():
            self.timer.stop()
        self._disconnect_timer()
        self._disconnect_scheduler_thread_call()
        self._latest_intent = None
        self._latest_intents_by_radio = {}
        self._latest_intent_ts = 0.0
        self._retry_scheduled = False
        self._force_retry_after_control = False
        self._forced_retry_attempts_left = 0
        self._clear_fldigi_busy_check_state()
        self._shutdown_control_executor("stop")
        self._shutdown_status_executor("stop")

    def _ensure_js8_offset_default(self) -> None:
        try:
            val = self.settings.get("js8_offset_hz", None)
        except Exception:
            val = None
        if val not in (None, "", 0):
            return
        offset = random_default_js8_offset_hz()
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
                    resumed_radio_id = self._manual_qsy_radio_id
                    self._manual_qsy_active = False
                    self._manual_qsy_entry_key = None
                    self._manual_qsy_radio_id = None
                    self._record_manual_resume_state(resumed_radio_id)
                    return None
                return dt
        except Exception:
            return None
        return None

    @staticmethod
    def _parse_manual_hold_until(value: object) -> Optional[datetime.datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            dt = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)

    def _manual_state_for_radio(self, radio_id: Optional[int]) -> Optional[SchedulerManualControlState]:
        if radio_id is None:
            return None
        try:
            return self._manual_control_service.get_state(int(radio_id))
        except Exception:
            return None

    def _clear_startup_manual_qsy_states(self) -> None:
        """Manual QSY is an in-session override; startup must follow assigned plans."""
        cleared = 0
        try:
            cleared = self._manual_control_service.clear_manual_qsy_states()
        except Exception as exc:
            log.debug("SchedulerEngine: failed to clear startup manual QSY states: %s", exc)
        self._manual_qsy_active = False
        self._manual_qsy_entry_key = None
        self._manual_qsy_radio_id = None
        if cleared:
            log.info("SchedulerEngine: cleared %s stale manual QSY state(s) at startup.", cleared)

    def _radio_suspend_until_dt(self, radio_id: Optional[int]) -> Optional[datetime.datetime]:
        state = self._manual_state_for_radio(radio_id)
        if state is None or state.state not in {"manual_hold", "manual_qsy"}:
            return None
        dt = self._parse_manual_hold_until(state.hold_until_utc)
        if dt is None:
            return None
        now = datetime.datetime.now(datetime.timezone.utc)
        if now >= dt:
            try:
                self._manual_control_service.resume(int(radio_id))
            except Exception:
                pass
            if self._manual_qsy_radio_id == radio_id:
                self._manual_qsy_active = False
                self._manual_qsy_entry_key = None
                self._manual_qsy_radio_id = None
            return None
        return dt

    def _radio_manual_suspend_active(self, radio_id: Optional[int]) -> bool:
        state = self._manual_state_for_radio(radio_id)
        return bool(state is not None and state.state == "manual_suspend")

    def _radio_manual_control_blocks_off_schedule_prompt(self, radio_id: Optional[int]) -> bool:
        state = self._manual_state_for_radio(radio_id)
        if state is not None and state.state in {"manual_hold", "manual_qsy", "manual_suspend"}:
            return True
        if radio_id is not None and self._manual_qsy_active:
            try:
                ident = int(radio_id)
            except Exception:
                ident = None
            if self._manual_qsy_radio_id in (None, ident):
                return True
        return False

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

    def _scheduling_suspended_for_radio(
        self,
        radio_id: Optional[int],
        now_utc: datetime.datetime,
    ) -> tuple[bool, Optional[datetime.datetime]]:
        if radio_id is not None and self._radio_manual_suspend_active(radio_id):
            return True, None
        dt = self._radio_suspend_until_dt(radio_id)
        if dt is None and radio_id is None:
            dt = self._suspend_until_dt()
        return (dt is not None and now_utc < dt), dt

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

    @staticmethod
    def _scheduler_health_key(name: str) -> str:
        return f"scheduler:{str(name or '').strip().lower().replace('_', '-') or 'unknown'}"

    def _record_scheduler_health_issue(self, name: str, message: str, *, cooldown_sec: float = 0.0, **metadata) -> None:
        try:
            action = str(metadata.pop("action", "") or "").strip() or message
            self._health.record_failure(
                self._scheduler_health_key(name),
                owner="SchedulerEngine",
                error=message,
                cooldown_sec=cooldown_sec,
                metadata={"scope": "Station-wide", "action": action, **{k: v for k, v in metadata.items() if v is not None}},
            )
        except Exception:
            pass

    def _update_js8_shadow_health(self, shadow: object) -> None:
        if not isinstance(shadow, dict):
            return
        differences = shadow.get("differences")
        if isinstance(differences, dict) and differences:
            labels = {
                "busy": "busy state",
                "frequency_hz": "frequency",
                "offset_hz": "offset",
            }
            names = [labels.get(str(key), str(key).replace("_", " ")) for key in differences.keys()]
            joined = ", ".join(names)
            message = (
                f"Native JS8Call diagnostic disagrees with the existing JS8 status for {joined}. "
                "FIO is still using the existing JS8 path; native JS8 remains diagnostic only."
            )
            self._record_scheduler_health_issue(
                "js8-shadow",
                message,
                action=message,
                endpoint=str(shadow.get("endpoint", "") or ""),
                mode=str(shadow.get("mode", "") or ""),
                version=str(shadow.get("version", "") or ""),
                diagnostic_only=True,
            )
            return
        self._clear_scheduler_health_issue(
            "js8-shadow",
            action="Native JS8Call diagnostic check is not reporting a mismatch.",
            endpoint=str(shadow.get("endpoint", "") or ""),
            mode=str(shadow.get("mode", "") or ""),
            version=str(shadow.get("version", "") or ""),
            diagnostic_only=True,
        )

    def _schedule_event_key(self, source: str, entry_key: object = None) -> str:
        if entry_key is not None:
            return "|".join(str(part) for part in (entry_key if isinstance(entry_key, tuple) else (entry_key,)))
        entry = self.current_schedule_entry or {}
        try:
            return "|".join(
                [
                    str(source or self.current_source or ""),
                    str(entry.get("band") or ""),
                    str(entry.get("frequency") or ""),
                    str(entry.get("vfo") or ""),
                    str(entry.get("primary_js8call_group") or ""),
                    str(entry.get("mode") or ""),
                    str(entry.get("fldigi_mode") or ""),
                    str(entry.get("fldigi_offset") or ""),
                ]
            )
        except Exception:
            return str(source or self.current_source or "")

    def _record_scheduler_event(
        self,
        event_type: str,
        code: str,
        *,
        source: str = "",
        entry: Optional[Dict] = None,
        entry_key: object = None,
        action: str = "",
        detail: str = "",
        frequency_hz: Optional[int] = None,
        band: str = "",
        mode: Optional[str] = None,
        vfo: Optional[str] = None,
        throttle_sec: float = 0.0,
        **metadata,
    ) -> None:
        source_text = str(source or self.current_source or "").strip()
        entry_obj = entry if isinstance(entry, dict) else (self.current_schedule_entry or {})
        band_text = str(band or entry_obj.get("band") or "").strip().upper()
        mode_text = str(mode if mode is not None else (self._resolve_rig_mode(entry_obj) if entry_obj else "") or "").strip()
        vfo_text = str(vfo if vfo is not None else entry_obj.get("vfo") or "").strip().upper()
        freq_hz = frequency_hz
        if freq_hz is None and entry_obj:
            try:
                freq_hz = self._parse_freq_hz((entry_obj.get("frequency") or "").strip())
            except Exception:
                freq_hz = None
        schedule_key = self._schedule_event_key(source_text, entry_key=entry_key)
        sig = (str(event_type or ""), str(code or ""), source_text, schedule_key, str(detail or ""))
        now_ts = time.time()
        if throttle_sec > 0:
            last_ts = self._scheduler_event_last.get(sig, 0.0)
            if now_ts - last_ts < float(throttle_sec):
                return
        if len(self._scheduler_event_last) > 512:
            cutoff = now_ts - 3600.0
            self._scheduler_event_last = {
                key: ts for key, ts in self._scheduler_event_last.items() if ts >= cutoff
            }
        self._scheduler_event_last[sig] = now_ts
        radio_profile_id = ""
        try:
            radio_id = self._primary_manual_control_radio_id()
            radio_profile_id = f"radio_{int(radio_id)}" if radio_id is not None else ""
        except Exception:
            radio_profile_id = ""
        clean_metadata = {k: v for k, v in metadata.items() if v is not None}
        if radio_profile_id and "radio_profile_id" not in clean_metadata:
            clean_metadata["radio_profile_id"] = radio_profile_id
        record_scheduler_event(
            event_type=event_type,
            code=code,
            source=source_text,
            action=action,
            detail=detail,
            radio_profile_id=radio_profile_id,
            frequency_hz=freq_hz,
            band=band_text,
            mode=mode_text,
            vfo=vfo_text,
            schedule_key=schedule_key,
            metadata=clean_metadata,
        )

    def _clear_scheduler_health_issue(self, name: str, **metadata) -> None:
        try:
            self._health.record_success(
                self._scheduler_health_key(name),
                owner="SchedulerEngine",
                metadata={"scope": "Station-wide", **{k: v for k, v in metadata.items() if v is not None}},
            )
        except Exception:
            pass

    def _record_latest_intent(
        self,
        entry: Dict,
        source: str,
        *,
        now_utc: Optional[datetime.datetime] = None,
        force: bool = False,
        ignore_suspend: bool = False,
        ignore_wait_prompt: bool = False,
        ignore_coordination_prompt: bool = False,
        ignore_js8_busy: bool = False,
        ignore_varac_busy: bool = False,
        ignore_fldigi_busy: bool = False,
        apply_js8_offset: bool = True,
        apply_fldigi: bool = True,
    ) -> None:
        intent = {
            "entry": dict(entry),
            "source": source,
            "now_utc": now_utc,
            "force": bool(force),
            "ignore_suspend": bool(ignore_suspend),
            "ignore_wait_prompt": bool(ignore_wait_prompt),
            "ignore_coordination_prompt": bool(ignore_coordination_prompt),
            "ignore_js8_busy": bool(ignore_js8_busy),
            "ignore_varac_busy": bool(ignore_varac_busy),
            "ignore_fldigi_busy": bool(ignore_fldigi_busy),
            "apply_js8_offset": bool(apply_js8_offset),
            "apply_fldigi": bool(apply_fldigi),
        }
        self._latest_intent = intent
        radio_id = None
        try:
            radio_id = self._entry_manual_control_radio_id(entry)
        except Exception:
            radio_id = None
        if radio_id is not None:
            try:
                if not isinstance(getattr(self, "_latest_intents_by_radio", None), dict):
                    self._latest_intents_by_radio = {}
                self._latest_intents_by_radio[int(radio_id)] = intent
            except Exception:
                pass
        self._latest_intent_ts = time.time()

    def _apply_latest_intent_if_any(self) -> bool:
        intent = None
        intents_by_radio = getattr(self, "_latest_intents_by_radio", None)
        if isinstance(intents_by_radio, dict) and intents_by_radio:
            try:
                radio_id = sorted(intents_by_radio.keys())[0]
            except Exception:
                radio_id = next(iter(intents_by_radio))
            intent = intents_by_radio.pop(radio_id, None)
        if intent is None:
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
            ignore_coordination_prompt=bool(intent.get("ignore_coordination_prompt")),
            ignore_js8_busy=bool(intent.get("ignore_js8_busy")),
            ignore_varac_busy=bool(intent.get("ignore_varac_busy")),
            ignore_fldigi_busy=bool(intent.get("ignore_fldigi_busy")),
            apply_js8_offset=bool(intent.get("apply_js8_offset")),
            apply_fldigi=bool(intent.get("apply_fldigi")),
        )
        return True

    def _reset_control_executor(self, reason: str) -> None:
        if self._control_timeout_reported:
            return
        self._control_timeout_reported = True
        log.warning("SchedulerEngine: control worker is stuck (%s); keeping the existing worker to prevent a thread leak.", reason)
        self._record_scheduler_health_issue(
            "control-task",
            f"control worker is stuck: {reason}; restart the unresponsive companion app or FIO",
            cooldown_sec=30.0,
            reason=reason,
        )

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

    def _shutdown_status_executor(self, reason: str) -> None:
        future = self._status_snapshot_future
        if future is not None and not future.done():
            try:
                future.cancel()
            except Exception as e:
                log.debug("SchedulerEngine: status future cancel failed during %s: %s", reason, e)
        try:
            self._status_executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            try:
                self._status_executor.shutdown(wait=False)
            except Exception as e:
                log.debug("SchedulerEngine: status executor shutdown failed during %s: %s", reason, e)
        except Exception as e:
            log.debug("SchedulerEngine: status executor shutdown failed during %s: %s", reason, e)
        self._status_snapshot_future = None

    def _maybe_refresh_external_status_snapshot(self, *, force: bool = False) -> None:
        if self._shutdown_requested:
            return
        now_ts = time.time()
        future = self._status_snapshot_future
        if future is not None and not future.done():
            started = float(self._status_snapshot_started_at or 0.0)
            if started and (now_ts - started) > self._status_snapshot_timeout_s:
                age = now_ts - started
                if not self._status_snapshot_timeout_reported:
                    self._status_snapshot_timeout_reported = True
                    log.warning(
                        "SchedulerEngine: status snapshot worker timed out after %.1fs; keeping the existing worker to prevent a thread leak.",
                        age,
                    )
                    self._record_scheduler_health_issue(
                        "status-snapshot",
                        f"status snapshot worker timed out after {age:.1f}s; restart the unresponsive companion app or FIO",
                        cooldown_sec=30.0,
                        age_s=round(age, 1),
                    )
                return
        future = self._status_snapshot_future
        if future is not None and not future.done():
            return
        if not force and now_ts - float(self._status_snapshot_refresh_ts or 0.0) < self._status_snapshot_refresh_interval_s:
            return
        self._status_snapshot_refresh_ts = now_ts
        try:
            hub = JS8RxHub.instance()
            self._last_js8_busy = bool(
                hub.is_active()
                and (hub.ptt_active() or (time.time() - hub.last_rx_activity_ts()) <= 12.0)
            )
        except Exception:
            pass
        rig = self.rig
        status_poll_coordinator = self._status_poll_coordinator
        current_entry = dict(self.current_schedule_entry or {})
        js8_offset_check_active = self._js8_offset_authority_active(current_entry, self._cached_control_mode())

        def _task() -> Dict[str, object]:
            settings = SettingsManager()
            try:
                control_mode = (settings.get("control_via", "FLRig") or "FLRig").upper()
                out: Dict[str, object] = {
                    "varac_status": {"busy": False, "waiting_for_frequency": False, "reason": None},
                    "rig_ptt": False,
                    "rig_ptt_known": False,
                    "rig_freq_hz": None,
                    "rig_vfo": None,
                    "js8_freq_hz": None,
                    "js8_offset_hz": None,
                    "js8_busy": self._last_js8_busy,
                    "checked_ts": time.time(),
                }
                try:
                    def _poll_varac_status() -> Dict[str, object]:
                        varac = VarACStatusClient()
                        return {
                            "varac_status": varac.get_status(include_db_transfer=True),
                            "source": "scheduler_background_varac",
                        }

                    varac_snapshot = status_poll_coordinator.get_snapshot(
                        "scheduler:primary:background_varac",
                        _poll_varac_status,
                        force=force,
                    )
                    if not varac_snapshot.errors:
                        out["varac_status"] = dict(varac_snapshot.varac_status or {})
                    out["varac_status_stale"] = bool(varac_snapshot.stale)
                    out["varac_status_detail"] = "; ".join(
                        str(value) for value in (varac_snapshot.errors or {}).values() if str(value or "").strip()
                    )
                except Exception as e:
                    log.debug("SchedulerEngine: background VarAC status failed: %s", e)
                if rig is not None:
                    try:
                        def _poll_rig_status() -> Dict[str, object]:
                            reading: Dict[str, object] = {}
                            if hasattr(rig, "get_ptt"):
                                reading["ptt_active"] = bool(rig.get_ptt())
                                reading["ptt_known"] = True
                            if hasattr(rig, "get_vfo_frequency"):
                                reading["frequency_hz"] = rig.get_vfo_frequency()
                            if hasattr(rig, "get_active_vfo"):
                                reading["vfo"] = rig.get_active_vfo()
                            reading["source"] = "scheduler_background_rig"
                            return reading

                        rig_snapshot = status_poll_coordinator.get_snapshot(
                            "scheduler:primary:background_rig",
                            _poll_rig_status,
                            force=force,
                        )
                        out["rig_ptt"] = bool(rig_snapshot.ptt_active)
                        out["rig_ptt_known"] = bool(rig_snapshot.ptt_known and not rig_snapshot.errors)
                        out["rig_freq_hz"] = rig_snapshot.frequency_hz
                        out["rig_vfo"] = rig_snapshot.vfo
                    except Exception as e:
                        log.debug("SchedulerEngine: background rig status failed: %s", e)
                if control_mode == "JS8CALL" or js8_offset_check_active:
                    try:
                        def _poll_js8_status() -> Dict[str, object]:
                            js8 = JS8ControlClient()
                            reading: Dict[str, object] = {"source": "scheduler_background_js8"}
                            if control_mode == "JS8CALL":
                                reading["js8_busy"] = bool(js8.is_busy())
                                reading["js8_frequency_hz"] = js8.get_frequency()
                                reading["js8_offset_hz"] = js8.get_offset()
                            elif js8_offset_check_active:
                                reading["js8_offset_hz"] = js8.get_offset()
                            return reading

                        js8_snapshot = status_poll_coordinator.get_snapshot(
                            "scheduler:primary:background_js8",
                            _poll_js8_status,
                            force=force,
                        )
                        legacy_readings: Dict[str, object] = {}
                        if not js8_snapshot.errors:
                            if control_mode == "JS8CALL":
                                out["js8_busy"] = bool(js8_snapshot.js8_busy)
                                out["js8_freq_hz"] = js8_snapshot.js8_frequency_hz
                                out["js8_offset_hz"] = js8_snapshot.js8_offset_hz
                                legacy_readings = {
                                    "busy": out.get("js8_busy"),
                                    "frequency_hz": out.get("js8_freq_hz"),
                                    "offset_hz": out.get("js8_offset_hz"),
                                }
                            elif js8_offset_check_active:
                                out["js8_offset_hz"] = js8_snapshot.js8_offset_hz
                                legacy_readings = {"offset_hz": out.get("js8_offset_hz")}
                        if legacy_readings:
                            shadow = self._software_status.js8_shadow_comparison_status(legacy_readings=legacy_readings)
                            out["js8_shadow_comparison"] = dict(shadow)
                        out["js8_status_stale"] = bool(js8_snapshot.stale)
                        out["js8_status_detail"] = "; ".join(
                            str(value) for value in (js8_snapshot.errors or {}).values() if str(value or "").strip()
                        )
                    except Exception as e:
                        log.debug("SchedulerEngine: background JS8Call status failed: %s", e)
                else:
                    out["js8_status_stale"] = False
                    out["js8_status_detail"] = ""
                return out
            finally:
                try:
                    settings.close()
                except Exception:
                    pass

        def _on_done(done) -> None:
            def _apply() -> None:
                if self._shutdown_requested:
                    return
                try:
                    data = done.result()
                except Exception as e:
                    log.debug("SchedulerEngine: background status snapshot failed: %s", e)
                    self._status_snapshot_started_at = None
                    self._status_snapshot_timeout_reported = False
                    self._record_scheduler_health_issue("status-snapshot", f"status snapshot failed: {e}", cooldown_sec=30.0)
                    return
                varac_status = data.get("varac_status")
                if isinstance(varac_status, dict):
                    self._last_varac_status = dict(varac_status)
                    self._last_varac_status_stale = bool(data.get("varac_status_stale"))
                    self._last_varac_status_detail = str(data.get("varac_status_detail") or "").strip()
                ptt_known = bool(data.get("rig_ptt_known", False))
                if ptt_known:
                    self._last_ptt_active = bool(data.get("rig_ptt", False))
                    self._status_flrig_ptt = self._last_ptt_active
                    self._status_flrig_ptt_known = True
                    self._status_flrig_ptt_ts = time.time()
                freq = data.get("rig_freq_hz")
                if isinstance(freq, (int, float)) and freq > 0:
                    self._status_flrig_freq_hz = int(freq)
                    self._status_flrig_freq_ts = time.time()
                vfo_val = data.get("rig_vfo")
                vfo_txt = str(vfo_val or "").strip().upper()[:1]
                if vfo_txt in {"A", "B"}:
                    self._status_flrig_vfo = vfo_txt
                    self._status_flrig_vfo_ts = time.time()
                js8_freq = data.get("js8_freq_hz")
                if isinstance(js8_freq, (int, float)) and js8_freq > 0:
                    self._status_js8_freq_hz = int(js8_freq)
                    self._status_js8_freq_ts = time.time()
                js8_offset = data.get("js8_offset_hz")
                if isinstance(js8_offset, (int, float)):
                    self._status_js8_offset_hz = int(js8_offset)
                    self._status_js8_offset_ts = time.time()
                self._last_js8_busy = bool(data.get("js8_busy", self._last_js8_busy))
                if "js8_status_stale" in data:
                    self._last_js8_status_stale = bool(data.get("js8_status_stale"))
                    self._last_js8_status_detail = str(data.get("js8_status_detail") or "").strip()
                shadow = data.get("js8_shadow_comparison")
                if isinstance(shadow, dict):
                    self._last_js8_shadow_comparison = dict(shadow)
                    self._update_js8_shadow_health(shadow)
                self._status_summary_external_ts = float(data.get("checked_ts") or time.time())
                self._status_summary_cache = None
                self._status_snapshot_started_at = None
                self._status_snapshot_timeout_reported = False
                self._clear_scheduler_health_issue("status-snapshot")

            self._queue_scheduler_thread_call(_apply)

        try:
            self._status_snapshot_future = self._status_executor.submit(_task)
            self._status_snapshot_started_at = time.time()
            self._status_snapshot_future.add_done_callback(_on_done)
        except RuntimeError:
            if not self._shutdown_requested:
                self._record_scheduler_health_issue(
                    "status-snapshot",
                    "status snapshot could not be queued; restart FIO if dependency status remains unavailable",
                    cooldown_sec=30.0,
                )

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

    def _queue_post_apply_verification(
        self,
        *,
        control_future_token: int,
        control_mode: str,
        source: str,
        freq_hz: int,
        band: str,
        mode: Optional[str],
        vfo: Optional[str],
        entry_key: Tuple,
        verify_js8_offset: bool = False,
    ) -> None:
        verify_entry = {
            "band": band,
            "frequency": f"{freq_hz / 1_000_000:.6f}",
            "mode": mode or "",
            "vfo": vfo or "",
        }
        rig = self.rig
        js8 = self.js8
        mode_key = (control_mode or "").strip().upper()
        status_poll_coordinator = self._status_poll_coordinator

        def _task() -> Dict[str, object]:
            out: Dict[str, object] = {
                "flrig_freq_hz": None,
                "flrig_ptt_active": False,
                "flrig_ptt_known": False,
                "flrig_vfo": None,
                "js8_freq_hz": None,
                "js8_offset_hz": None,
                "checked_ts": time.time(),
                "errors": {},
            }
            errors: Dict[str, str] = {}
            if rig is not None:
                try:
                    def _poll_rig_status() -> Dict[str, object]:
                        reading: Dict[str, object] = {}
                        if hasattr(rig, "get_vfo_frequency"):
                            reading["frequency_hz"] = rig.get_vfo_frequency()
                        if hasattr(rig, "get_ptt"):
                            reading["ptt_active"] = bool(rig.get_ptt())
                            reading["ptt_known"] = True
                        if hasattr(rig, "get_active_vfo"):
                            reading["vfo"] = rig.get_active_vfo()
                        reading["source"] = "scheduler_post_apply_rig"
                        return reading

                    rig_snapshot = status_poll_coordinator.get_snapshot(
                        "scheduler:primary:post_apply_rig",
                        _poll_rig_status,
                        force=True,
                    )
                    out["flrig_freq_hz"] = rig_snapshot.frequency_hz
                    out["flrig_ptt_active"] = bool(rig_snapshot.ptt_active)
                    out["flrig_ptt_known"] = bool(rig_snapshot.ptt_known and not rig_snapshot.errors)
                    out["flrig_vfo"] = rig_snapshot.vfo
                    if rig_snapshot.errors:
                        errors["rig"] = "; ".join(str(value) for value in rig_snapshot.errors.values())
                except Exception as e:
                    errors["rig"] = str(e)
            if mode_key == "JS8CALL" or verify_js8_offset or not out.get("flrig_freq_hz"):
                try:
                    if js8 is not None:
                        if hasattr(js8, "get_frequency"):
                            out["js8_freq_hz"] = js8.get_frequency()
                        if hasattr(js8, "get_offset"):
                            out["js8_offset_hz"] = js8.get_offset()
                except Exception as e:
                    errors["js8"] = str(e)
            out["errors"] = errors
            return out

        def _on_done(done) -> None:
            def _apply() -> None:
                if self._shutdown_requested or control_future_token != self._control_future_token:
                    return
                try:
                    data = done.result()
                except Exception as e:
                    log.debug("SchedulerEngine: post-apply verification worker failed: %s", e)
                    return
                now_ts = float(data.get("checked_ts") or time.time())
                flrig_freq = data.get("flrig_freq_hz")
                js8_freq = data.get("js8_freq_hz")
                js8_offset = data.get("js8_offset_hz")
                flrig_vfo = str(data.get("flrig_vfo") or "").strip().upper()[:1]
                if isinstance(flrig_freq, (int, float)) and flrig_freq > 0:
                    self._status_flrig_freq_hz = int(flrig_freq)
                    self._status_flrig_freq_ts = now_ts
                if flrig_vfo in {"A", "B"}:
                    self._status_flrig_vfo = flrig_vfo
                    self._status_flrig_vfo_ts = now_ts
                if isinstance(js8_freq, (int, float)) and js8_freq > 0:
                    self._status_js8_freq_hz = int(js8_freq)
                    self._status_js8_freq_ts = now_ts
                if isinstance(js8_offset, (int, float)):
                    self._status_js8_offset_hz = int(js8_offset)
                    self._status_js8_offset_ts = now_ts
                ptt_known = bool(data.get("flrig_ptt_known", False))
                if ptt_known:
                    self._last_ptt_active = bool(data.get("flrig_ptt_active", self._last_ptt_active))
                    self._status_flrig_ptt = self._last_ptt_active
                    self._status_flrig_ptt_known = True
                    self._status_flrig_ptt_ts = now_ts
                actual = StationActualState(
                    checked_ts=now_ts,
                    flrig_freq_hz=int(flrig_freq) if isinstance(flrig_freq, (int, float)) and flrig_freq > 0 else None,
                    flrig_ptt_active=bool(data.get("flrig_ptt_active", False)) if ptt_known else False,
                    flrig_ptt_known=ptt_known,
                    flrig_ptt_age_s=0.0 if ptt_known else None,
                    flrig_ptt_stale=not ptt_known,
                    flrig_vfo=flrig_vfo if flrig_vfo in {"A", "B"} else None,
                    js8_freq_hz=int(js8_freq) if isinstance(js8_freq, (int, float)) and js8_freq > 0 else None,
                    js8_offset_hz=int(js8_offset) if isinstance(js8_offset, (int, float)) else None,
                    errors=dict(data.get("errors") or {}),
                )
                if actual.flrig_freq_hz is not None:
                    actual.actual_frequency_hz = actual.flrig_freq_hz
                    actual.actual_frequency_source = "Rig"
                elif actual.js8_freq_hz is not None:
                    actual.actual_frequency_hz = actual.js8_freq_hz
                    actual.actual_frequency_source = "JS8Call"
                else:
                    actual.actual_frequency_source = "unknown"
                    actual.stale = True
                verify_state = self._compute_off_schedule_state(
                    verify_entry,
                    actual,
                    control_mode=control_mode,
                    check_mode=False,
                    check_offset=False,
                )
                if bool(verify_state.flags.get("frequency")):
                    self._record_scheduler_event(
                        "failed",
                        "post_apply_still_off_schedule",
                        source=source,
                        action="Schedule command succeeded but verification still appears off schedule",
                        detail="; ".join(verify_state.reasons),
                        frequency_hz=freq_hz,
                        band=band,
                        mode=mode,
                        vfo=vfo,
                        entry_key=entry_key,
                        throttle_sec=0.0,
                        actual_frequency_hz=verify_state.actual_frequency_hz,
                        actual_frequency_source=verify_state.actual_frequency_source,
                    )
                else:
                    self._record_scheduler_event(
                        "verified",
                        "post_apply_on_schedule",
                        source=source,
                        action="Schedule command verified on frequency",
                        frequency_hz=freq_hz,
                        band=band,
                        mode=mode,
                        vfo=vfo,
                        entry_key=entry_key,
                        throttle_sec=30.0,
                        actual_frequency_hz=verify_state.actual_frequency_hz,
                        actual_frequency_source=verify_state.actual_frequency_source,
                    )

            self._queue_scheduler_thread_call(_apply)

        try:
            future = self._status_executor.submit(_task)
            future.add_done_callback(_on_done)
        except RuntimeError as e:
            log.debug("SchedulerEngine: post-apply verification could not be queued: %s", e)

    def _queue_control_action(
        self,
        *,
        control_mode: str,
        rig_client: Optional[RigControlClient] = None,
        js8_client: Optional[JS8ControlClient] = None,
        allow_global_fallback: bool = True,
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
            self._record_scheduler_event(
                "skip",
                "shutdown_requested",
                source=source,
                action="Control action skipped because FIO is shutting down",
                frequency_hz=freq_hz,
                band=band,
                mode=mode,
                vfo=vfo,
                entry_key=entry_key,
                throttle_sec=15.0,
            )
            return False
        if not self._control_can_attempt():
            log.debug("SchedulerEngine: control action skipped (backoff active).")
            self._record_scheduler_event(
                "skip",
                "control_backoff",
                source=source,
                action="Control action delayed by scheduler backoff",
                detail="A previous control action failed or timed out; FIO is waiting briefly before retrying.",
                frequency_hz=freq_hz,
                band=band,
                mode=mode,
                vfo=vfo,
                entry_key=entry_key,
                throttle_sec=15.0,
                backoff_until=self._control_backoff_until,
            )
            return False
        if self._control_future is not None and not self._control_future.done():
            if self._control_future_stuck():
                self._reset_control_executor("timeout waiting for control task")
            log.debug("SchedulerEngine: control action skipped (control task running).")
            self._record_scheduler_event(
                "skip",
                "control_task_running",
                source=source,
                action="Control action waiting for prior control task",
                frequency_hz=freq_hz,
                band=band,
                mode=mode,
                vfo=vfo,
                entry_key=entry_key,
                throttle_sec=15.0,
            )
            return False
        if self._pending_entry_key == entry_key:
            log.debug("SchedulerEngine: control action skipped (pending entry key).")
            self._record_scheduler_event(
                "skip",
                "pending_entry_key",
                source=source,
                action="Control action already pending for this schedule entry",
                frequency_hz=freq_hz,
                band=band,
                mode=mode,
                vfo=vfo,
                entry_key=entry_key,
                throttle_sec=15.0,
            )
            return False
        self._pending_entry_key = entry_key
        self._record_scheduler_event(
            "apply_attempt",
            "control_action_queued",
            source=source,
            action="Queued schedule control action",
            frequency_hz=freq_hz,
            band=band,
            mode=mode,
            vfo=vfo,
            entry_key=entry_key,
            throttle_sec=0.0,
            control_mode=control_mode,
            js8_offset=js8_offset,
        )

        target_js8 = js8_client if not allow_global_fallback else (js8_client or self.js8)
        target_rig = rig_client if not allow_global_fallback else (rig_client or self.rig)
        if control_mode == "JS8CALL" and target_js8 is None:
            log.warning("SchedulerEngine: targeted JS8Call control requested but no JS8 client is available.")
            self._pending_entry_key = None
            self._record_scheduler_event(
                "skip",
                "missing_target_control_client",
                source=source,
                action="Control action skipped because the selected radio has no JS8Call control client",
                frequency_hz=freq_hz,
                band=band,
                mode=mode,
                vfo=vfo,
                entry_key=entry_key,
                throttle_sec=15.0,
                control_mode=control_mode,
            )
            return False
        if control_mode in {"FLRIG", "RIGCTLD"} and target_rig is None:
            log.warning("SchedulerEngine: targeted %s control requested but no rig client is available.", control_mode)
            self._pending_entry_key = None
            self._record_scheduler_event(
                "skip",
                "missing_target_control_client",
                source=source,
                action="Control action skipped because the selected radio has no rig control client",
                frequency_hz=freq_hz,
                band=band,
                mode=mode,
                vfo=vfo,
                entry_key=entry_key,
                throttle_sec=15.0,
                control_mode=control_mode,
            )
            return False

        def _task() -> bool:
            ok = False
            if control_mode == "JS8CALL":
                try:
                    if target_js8:
                        if js8_offset is None:
                            current_off = target_js8.get_offset()
                            ok = target_js8.set_frequency(freq_hz, offset_hz=current_off)
                        else:
                            ok = target_js8.set_frequency(freq_hz, offset_hz=js8_offset)
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
                    if target_rig:
                        ok = target_rig.set_frequency(cmd)
                except Exception as e:
                    log.error("SchedulerEngine: error sending set_frequency to FLRig: %s", e)
            if ok and control_mode in {"FLRIG", "RIGCTLD"}:
                if auto_tune and control_mode == "FLRIG":
                    try:
                        if target_rig and hasattr(target_rig, "tune"):
                            target_rig.tune()
                    except Exception as e:
                        log.error("SchedulerEngine: error invoking rig.tune(): %s", e)
                if target_js8:
                    try:
                        if js8_offset is None:
                            current_off = target_js8.get_offset()
                            target_js8.set_frequency(freq_hz, offset_hz=current_off)
                        else:
                            target_js8.set_frequency(freq_hz, offset_hz=js8_offset)
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
                self._control_timeout_reported = False
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
                    self._clear_fldigi_busy_check_state()
                    self._clear_scheduler_health_issue("control-task", source=source, frequency_hz=freq_hz)
                    self._record_scheduler_event(
                        "applied",
                        "control_action_succeeded",
                        source=source,
                        action="Schedule control action succeeded",
                        frequency_hz=freq_hz,
                        band=band,
                        mode=mode,
                        vfo=vfo,
                        entry_key=entry_key,
                        throttle_sec=0.0,
                    )
                    self._queue_post_apply_verification(
                        control_future_token=control_future_token,
                        control_mode=control_mode,
                        source=source,
                        freq_hz=freq_hz,
                        band=band,
                        mode=mode,
                        vfo=vfo,
                        entry_key=entry_key,
                        verify_js8_offset=js8_offset is not None,
                    )
                else:
                    self._control_fail_count += 1
                    backoff = self._control_backoff()
                    self._control_backoff_until = time.time() + backoff
                    log.warning(
                        "SchedulerEngine: control action failed; backing off %.1fs (failures=%d)",
                        backoff,
                        self._control_fail_count,
                    )
                    self._record_scheduler_health_issue(
                        "control-task",
                        f"control action failed; backing off {backoff:.1f}s",
                        cooldown_sec=min(backoff, 60.0),
                        source=source,
                        frequency_hz=freq_hz,
                        failures=self._control_fail_count,
                    )
                    self._record_scheduler_event(
                        "failed",
                        "control_action_failed",
                        source=source,
                        action=f"Control action failed; backing off {backoff:.1f}s",
                        detail="FIO will retry the latest scheduler intent after the control path recovers.",
                        frequency_hz=freq_hz,
                        band=band,
                        mode=mode,
                        vfo=vfo,
                        entry_key=entry_key,
                        throttle_sec=0.0,
                        failures=self._control_fail_count,
                        backoff_s=round(backoff, 1),
                    )
                intents_by_radio = getattr(self, "_latest_intents_by_radio", None)
                if self._latest_intent or (isinstance(intents_by_radio, dict) and intents_by_radio):
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
        override = self._runtime_timer_policy_override.get(key)
        if override in {"On Schedule Change", "Prompt"}:
            return override
        try:
            raw = (self.settings.get(key, default) or default).strip()
        except Exception:
            raw = default
        if raw not in {"On Schedule Change", "Prompt"}:
            return default
        return raw

    def _prompt_interval_minutes(self, key: str, default: int = 60) -> int:
        raw = self._runtime_timer_policy_override.get(key)
        if not raw:
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

    def _process_running(self, name: str) -> bool:
        target = (name or "").strip().lower()
        if not target:
            return False
        program_names = {
            "js8call": "JS8Call",
            "flrig": "FLRig",
            "varac": "VarAC",
        }
        return bool(self._software_status.program_is_running(program_names.get(target, name)))

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
            self._last_varac_status = {"busy": False, "waiting_for_frequency": False, "reason": None}
            return dict(self._last_varac_status)
        if hasattr(self.varac, "get_status"):
            try:
                status = self.varac.get_status()
                if isinstance(status, dict):
                    self._last_varac_status = dict(status)
                    self._status_summary_external_ts = time.time()
                    return status
            except Exception:
                self._last_varac_status = {"busy": False, "waiting_for_frequency": False, "reason": None}
                return dict(self._last_varac_status)
        try:
            self._last_varac_status = {"busy": bool(self.varac.is_busy()), "waiting_for_frequency": False, "reason": None}
            self._status_summary_external_ts = time.time()
            return dict(self._last_varac_status)
        except Exception:
            self._last_varac_status = {"busy": False, "waiting_for_frequency": False, "reason": None}
            return dict(self._last_varac_status)

    def _varac_busy_ok(self, status: Optional[Dict[str, object]] = None) -> bool:
        status = status or self._varac_status()
        try:
            return not bool(status.get("busy"))
        except Exception:
            return True

    def _clear_fldigi_busy_check_state(self) -> None:
        self._fldigi_busy_entry_key = None
        self._fldigi_busy_since_ts = None
        self._fldigi_busy_last_reason = None
        self._fldigi_busy_check_source = ""
        self._fldigi_busy_check_target_hz = None
        self._fldigi_busy_check_result = None
        self._fldigi_busy_check_in_flight = False
        self._fldigi_busy_check_token += 1
        self._fldigi_busy_check_next_ts = 0.0
        self._clear_fldigi_busy_evidence()
        self._clear_scheduler_health_issue("fldigi-busy")

    def _queue_fldigi_busy_check(
        self,
        *,
        entry_key: Tuple,
        source: str,
        target_frequency_hz: int,
    ) -> None:
        if self._shutdown_requested or self._fldigi_busy_check_in_flight or not self.fldigi_log:
            return
        self._fldigi_busy_check_token += 1
        check_token = self._fldigi_busy_check_token

        def _task() -> Dict[str, object]:
            started = time.time()
            try:
                status = self.fldigi_log.get_status()
                if isinstance(status, dict):
                    busy = bool(status.get("busy"))
                    reason = status.get("reason")
                    last_valid_age_s = status.get("last_valid_age_s")
                else:
                    busy = bool(getattr(status, "busy", False))
                    reason = getattr(status, "reason", None)
                    last_valid_age_s = getattr(status, "last_valid_age_s", None)
                return {
                    "busy": busy,
                    "reason": reason,
                    "last_valid_age_s": last_valid_age_s,
                    "checked_ts": time.time(),
                    "duration_ms": round((time.time() - started) * 1000.0, 1),
                    "error": None,
                }
            except Exception as e:
                return {
                    "busy": False,
                    "reason": None,
                    "last_valid_age_s": None,
                    "checked_ts": time.time(),
                    "duration_ms": round((time.time() - started) * 1000.0, 1),
                    "error": str(e),
                }

        def _on_done(done) -> None:
            def _apply() -> None:
                if check_token != self._fldigi_busy_check_token:
                    return
                self._fldigi_busy_check_in_flight = False
                if self._shutdown_requested:
                    return
                if (
                    self._fldigi_busy_entry_key != entry_key
                    or self._fldigi_busy_check_source != source
                    or self._fldigi_busy_check_target_hz != target_frequency_hz
                ):
                    return
                try:
                    result = dict(done.result())
                except Exception as e:
                    result = {
                        "busy": False,
                        "reason": None,
                        "last_valid_age_s": None,
                        "checked_ts": time.time(),
                        "duration_ms": 0.0,
                        "error": str(e),
                    }
                self._fldigi_busy_check_result = result
                self._fldigi_busy_check_next_ts = time.time() + self._fldigi_busy_check_interval_s
                self._status_summary_cache = None
                busy = bool(result.get("busy"))
                reason = str(result.get("reason") or "") or "RX activity"
                error = str(result.get("error") or "")
                if error:
                    self._record_scheduler_health_issue(
                        "fldigi-busy-check",
                        f"could not verify FLDigi receive activity; continuing schedule: {error}",
                        cooldown_sec=30.0,
                        source=source,
                        frequency_hz=target_frequency_hz,
                    )
                    self._record_scheduler_event(
                        "failed",
                        "fldigi_busy_check_failed",
                        source=source,
                        action="Could not verify FLDigi receive activity; continuing schedule",
                        detail=error,
                        frequency_hz=target_frequency_hz,
                        entry_key=entry_key,
                        throttle_sec=30.0,
                        duration_ms=result.get("duration_ms"),
                    )
                else:
                    self._clear_scheduler_health_issue("fldigi-busy-check")
                    self._record_scheduler_event(
                        "status",
                        "fldigi_busy_check_result",
                        source=source,
                        action="FLDigi receive activity check completed",
                        detail=f"FLDigi busy={busy}; reason={reason}",
                        frequency_hz=target_frequency_hz,
                        entry_key=entry_key,
                        throttle_sec=30.0,
                        busy=busy,
                        reason=reason,
                        duration_ms=result.get("duration_ms"),
                    )
                try:
                    self._evaluate(now_utc=datetime.datetime.now(datetime.timezone.utc))
                except Exception as e:
                    log.error("SchedulerEngine: FLDigi busy result reevaluation failed: %s", e)

            self._queue_scheduler_thread_call(_apply)

        try:
            self._fldigi_busy_check_in_flight = True
            future = self._status_executor.submit(_task)
            future.add_done_callback(_on_done)
            self._record_scheduler_event(
                "status",
                "fldigi_busy_check_queued",
                source=source,
                action="Checking FLDigi receive activity before changing frequency",
                frequency_hz=target_frequency_hz,
                entry_key=entry_key,
                throttle_sec=30.0,
            )
        except RuntimeError as e:
            self._fldigi_busy_check_in_flight = False
            self._fldigi_busy_check_result = {
                "busy": False,
                "reason": None,
                "last_valid_age_s": None,
                "checked_ts": time.time(),
                "duration_ms": 0.0,
                "error": str(e),
            }

    def _should_delay_for_fldigi(
        self,
        *,
        entry_key: Tuple,
        source: str,
        target_frequency_hz: int,
        want_freq_change: bool,
        ignore_fldigi_busy: bool,
        now_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        source_upper = (source or "").upper()
        if ignore_fldigi_busy or not want_freq_change or source_upper not in {"HF", "SOP"}:
            self._clear_fldigi_busy_check_state()
            self._clear_scheduler_health_issue("fldigi-busy")
            return False, None
        if self._manual_net_fldigi_active or self._manual_net_js8_active:
            self._record_scheduler_event(
                "skip",
                "manual_net_active",
                source=source,
                action="FLDigi busy hold bypassed during active manual net control",
                detail="Manual net control owns FLDigi/JS8 behavior for this net window.",
                throttle_sec=60.0,
            )
            self._clear_fldigi_busy_check_state()
            return False, None
        if not self.fldigi_log:
            self._clear_fldigi_busy_check_state()
            return False, None
        now_ts = now_ts if now_ts is not None else time.time()
        if (
            self._fldigi_busy_entry_key != entry_key
            or self._fldigi_busy_check_source != source
            or self._fldigi_busy_check_target_hz != target_frequency_hz
        ):
            self._clear_fldigi_busy_check_state()
            self._fldigi_busy_entry_key = entry_key
            self._fldigi_busy_check_source = source
            self._fldigi_busy_check_target_hz = target_frequency_hz
            self._queue_fldigi_busy_check(
                entry_key=entry_key,
                source=source,
                target_frequency_hz=target_frequency_hz,
            )
            return True, "checking FLDigi receive activity"
        status = dict(self._fldigi_busy_check_result or {})
        if not status:
            if not self._fldigi_busy_check_in_flight and now_ts >= self._fldigi_busy_check_next_ts:
                self._queue_fldigi_busy_check(
                    entry_key=entry_key,
                    source=source,
                    target_frequency_hz=target_frequency_hz,
                )
            return True, "checking FLDigi receive activity"
        checked_ts = float(status.get("checked_ts") or 0.0)
        if checked_ts <= 0.0 or now_ts - checked_ts >= self._fldigi_busy_check_interval_s:
            self._clear_fldigi_busy_evidence()
            self._fldigi_busy_check_result = None
            self._queue_fldigi_busy_check(
                entry_key=entry_key,
                source=source,
                target_frequency_hz=target_frequency_hz,
            )
            return True, "checking FLDigi receive activity"
        if status.get("error"):
            self._clear_fldigi_busy_evidence()
            return False, None
        busy = bool(status.get("busy"))
        if not busy:
            self._fldigi_busy_since_ts = None
            self._fldigi_busy_last_reason = None
            self._clear_fldigi_busy_evidence()
            self._clear_scheduler_health_issue("fldigi-busy")
            return False, None
        if self._fldigi_busy_since_ts is None:
            self._fldigi_busy_since_ts = now_ts
        self._fldigi_busy_last_reason = str(status.get("reason") or "") or None
        since = self._fldigi_busy_since_ts or now_ts
        hold_age = max(0.0, now_ts - since)
        reason = self._fldigi_busy_last_reason or "RX activity"
        if hold_age >= self._fldigi_busy_watchdog_s:
            log.warning(
                "SchedulerEngine: FLDigi busy watchdog breaking away after %.1fs hold (reason=%s).",
                hold_age,
                reason,
            )
            self._record_scheduler_health_issue(
                "fldigi-busy",
                (
                    f"watchdog break-away after {hold_age:.0f}s; possible stale/hung external app "
                    f"busy state because FLDigi still reports {reason}"
                ),
                cooldown_sec=30.0,
                reason=reason,
                hold_age_s=round(hold_age, 1),
                source=source,
                active_hold=True,
            )
            self._record_scheduler_event(
                "breakaway",
                "fldigi_busy_breakaway",
                source=source,
                action="FLDigi busy held schedule for 3 minutes; applying schedule anyway",
                detail=(
                    "Authoritative schedule break-away. FIO will proceed because a stale/hung FLDigi busy "
                    f"state should not hold the operating plan indefinitely. Reason: {reason}"
                ),
                throttle_sec=0.0,
                reason=reason,
                hold_age_s=round(hold_age, 1),
            )
            self._clear_fldigi_busy_check_state()
            return False, None
        if not self._fldigi_busy_check_in_flight and now_ts >= self._fldigi_busy_check_next_ts:
            self._queue_fldigi_busy_check(
                entry_key=entry_key,
                source=source,
                target_frequency_hz=target_frequency_hz,
            )
        self._record_scheduler_health_issue(
            "fldigi-busy",
            f"holding schedule change for FLDigi RX activity ({reason})",
            cooldown_sec=0.0,
            reason=reason,
            hold_age_s=round(hold_age, 1),
            source=source,
        )
        self._publish_fldigi_busy_evidence(reason=reason)
        self._record_scheduler_event(
            "hold",
            "fldigi_busy",
            source=source,
            action="Holding schedule change for FLDigi RX activity",
            detail=f"FIO will recheck and break away after 3 minutes if this busy state does not clear. Reason: {reason}",
            throttle_sec=30.0,
            reason=reason,
            hold_age_s=round(hold_age, 1),
        )
        return True, self._fldigi_busy_last_reason or "RX activity"

    def _should_delay_for_external_busy(
        self,
        *,
        kind: str,
        entry_key: Tuple,
        source: str,
        busy: bool,
        reason: str,
        ignore_busy: bool,
        protected_busy: bool = False,
        now_ts: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        key = (kind or "").strip().lower()
        if protected_busy:
            ignore_busy = False
        if ignore_busy or not busy:
            self._clear_external_busy_evidence(key)
            if key == "js8":
                self._js8_busy_entry_key = None
                self._js8_busy_since_ts = None
                self._clear_scheduler_health_issue("js8-busy")
            elif key == "varac":
                self._varac_busy_entry_key = None
                self._varac_busy_since_ts = None
                self._clear_scheduler_health_issue("varac-busy")
            elif key == "varac-wait":
                self._varac_wait_since_ts = None
            return False, None
        if (self._manual_net_fldigi_active or self._manual_net_js8_active) and not protected_busy:
            self._clear_external_busy_evidence(key)
            return False, None
        if (source or "").upper() == "NET" and not protected_busy:
            self._clear_external_busy_evidence(key)
            return False, None

        now_ts = now_ts if now_ts is not None else time.time()
        if key == "js8":
            health_key = "js8-busy"
            event_code = "js8_busy"
            label = "JS8Call"
            since_attr = "_js8_busy_since_ts"
            entry_attr = "_js8_busy_entry_key"
        elif key == "varac-wait":
            health_key = "varac-waiting-for-frequency"
            event_code = "varac_waiting_for_frequency"
            label = "VarAC waiting for frequency"
            since_attr = "_varac_wait_since_ts"
            entry_attr = "_varac_wait_prompt_entry_key"
        else:
            health_key = "varac-busy"
            event_code = "varac_busy"
            label = "VarAC"
            since_attr = "_varac_busy_since_ts"
            entry_attr = "_varac_busy_entry_key"

        if getattr(self, entry_attr, None) != entry_key:
            setattr(self, entry_attr, entry_key)
            setattr(self, since_attr, now_ts)
        since = getattr(self, since_attr, None) or now_ts
        hold_age = max(0.0, now_ts - since)
        detail_reason = reason or "busy"
        if protected_busy:
            self._publish_external_busy_evidence(
                kind=key,
                reason=detail_reason,
                protected_busy=True,
            )
            self._record_scheduler_health_issue(
                health_key,
                f"holding schedule change because {label} protected traffic is active ({detail_reason})",
                cooldown_sec=0.0,
                reason=detail_reason,
                hold_age_s=round(hold_age, 1),
                source=source,
                active_hold=True,
            )
            self._record_scheduler_event(
                "hold",
                f"{event_code}_protected",
                source=source,
                action=f"Holding schedule change because {label} protected traffic is active",
                detail=f"FIO will not break through active VarAC file transfer/file-wait traffic. Reason: {detail_reason}",
                throttle_sec=30.0,
                reason=detail_reason,
                hold_age_s=round(hold_age, 1),
            )
            return True, detail_reason
        if hold_age >= self._external_busy_watchdog_s:
            self._clear_external_busy_evidence(key)
            log.warning(
                "SchedulerEngine: %s busy watchdog breaking away after %.1fs hold (reason=%s).",
                label,
                hold_age,
                detail_reason,
            )
            self._record_scheduler_health_issue(
                health_key,
                f"watchdog break-away after {hold_age:.0f}s; possible stale external busy state ({detail_reason})",
                cooldown_sec=30.0,
                reason=detail_reason,
                hold_age_s=round(hold_age, 1),
                source=source,
                active_hold=True,
            )
            self._record_scheduler_event(
                "breakaway",
                f"{event_code}_breakaway",
                source=source,
                action=f"{label} held schedule for 90 seconds; applying schedule anyway",
                detail=(
                    "Authoritative schedule break-away. FIO will proceed because an external busy "
                    f"state should not hold the operating plan indefinitely. Reason: {detail_reason}"
                ),
                throttle_sec=0.0,
                reason=detail_reason,
                hold_age_s=round(hold_age, 1),
            )
            setattr(self, since_attr, None)
            return False, None

        self._publish_external_busy_evidence(
            kind=key,
            reason=detail_reason,
            protected_busy=False,
        )
        self._record_scheduler_health_issue(
            health_key,
            f"holding schedule change because {label} is busy ({detail_reason})",
            cooldown_sec=0.0,
            reason=detail_reason,
            hold_age_s=round(hold_age, 1),
            source=source,
            active_hold=True,
        )
        self._record_scheduler_event(
            "hold",
            event_code,
            source=source,
            action=f"Holding schedule change because {label} is busy",
            detail=f"FIO will break away after 90 seconds if this busy state does not clear. Reason: {detail_reason}",
            throttle_sec=30.0,
            reason=detail_reason,
            hold_age_s=round(hold_age, 1),
        )
        return True, detail_reason

    def apply_current_entry(
        self,
        *,
        force: bool = False,
        ignore_wait_prompt: bool = False,
        ignore_coordination_prompt: bool = False,
        ignore_suspend: bool = False,
        ignore_net_suppression: bool = False,
        ignore_js8_busy: bool = False,
        ignore_varac_busy: bool = False,
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
            ignore_coordination_prompt=ignore_coordination_prompt,
            ignore_suspend=ignore_suspend,
            ignore_net_suppression=ignore_net_suppression,
            ignore_js8_busy=ignore_js8_busy,
            ignore_varac_busy=ignore_varac_busy,
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

    def _clear_coordination_prompt(self) -> None:
        was_active = bool(
            self._coordination_prompt_active or self._coordination_prompt_signature or self._coordination_prompt_payload
        )
        self._coordination_prompt_active = False
        self._coordination_prompt_signature = None
        self._coordination_prompt_payload = None
        if was_active:
            try:
                self.coordination_conflict_cleared.emit()
            except Exception:
                pass

    def resolve_coordination_conflict(self, action: str, minutes: Optional[int] = None) -> None:
        signature = str(
            (self._coordination_prompt_payload or {}).get("signature") or self._coordination_prompt_signature or ""
        ).strip()
        if action in {"apply", "ignore"} and signature:
            self._coordination_prompt_suppressed_signature = signature
        elif not signature:
            self._coordination_prompt_suppressed_signature = None
        self._clear_coordination_prompt()
        if action == "apply":
            self.apply_current_entry(
                force=True,
                ignore_coordination_prompt=True,
            )
        elif action == "suspend":
            self._suspend_for_minutes(self._normalize_hold_minutes(minutes if minutes is not None else self._default_hold_minutes()))

    def resume_schedule(
        self,
        *,
        ignore_coordination_prompt: bool = False,
        target_device_profile_id: Optional[int] = None,
    ) -> bool:
        resume_radio_id = None
        try:
            resume_radio_id = int(target_device_profile_id or 0) or None
        except Exception:
            resume_radio_id = None
        source = self.current_source or "NONE"
        entry = self.current_schedule_entry or {}
        if resume_radio_id is not None:
            lane_source, lane_entry = self._active_schedule_entry_for_radio(resume_radio_id, force=True)
            if lane_entry:
                source = lane_source or source
                entry = lane_entry
        coordination_conflict = (
            self._coordination_conflict_status(entry, source="RESUME", force=True)
            if isinstance(entry, dict) and entry
            else {}
        )
        coordination_signature = self._coordination_conflict_signature(coordination_conflict)
        if bool(coordination_conflict.get("blocked")):
            self._clear_coordination_prompt()
            self._record_scheduler_event(
                "blocked",
                "rf_safety_guard_block",
                source="RESUME",
                entry=entry,
                action="Blocked resume by RF Safety Guard",
                detail=str(coordination_conflict.get("detail") or coordination_conflict.get("summary") or ""),
                throttle_sec=30.0,
                signature=coordination_signature,
                guard_mode=str(coordination_conflict.get("guard_mode") or ""),
            )
            self.active_entry_changed.emit(entry, "RESUME")
            return False
        if resume_radio_id is None:
            resume_radio_id = self._manual_qsy_radio_id
        try:
            if hasattr(self.settings, "set") and target_device_profile_id is None:
                self.settings.set("schedule_suspend_until", 0)
        except Exception:
            pass
        if resume_radio_id is None or self._manual_qsy_radio_id in (None, resume_radio_id):
            self._manual_qsy_active = False
            self._manual_qsy_entry_key = None
            self._manual_qsy_radio_id = None
        self._record_manual_resume_state(resume_radio_id)
        self._prompt_active = False
        self._prompt_items = []
        self._prompt_entry_key = None
        self._clear_coordination_prompt()
        self._reset_prompt_timers()
        self._record_scheduler_event(
            "resume",
            "resume_schedule",
            action="Resume Schedule requested; forcing active operating plan",
            detail="Resume clears holds/backoff and reapplies the active schedule, including FLDigi mode/offset.",
            throttle_sec=0.0,
        )
        self._fldigi_force_apply_once = True
        self._latest_intent = None
        self._latest_intents_by_radio = {}
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
        if target_device_profile_id is not None and entry:
            self._apply_schedule_entry(
                entry,
                source,
                now_utc=datetime.datetime.now(datetime.timezone.utc),
                force=True,
                ignore_wait_prompt=True,
                ignore_coordination_prompt=ignore_coordination_prompt,
                ignore_suspend=True,
                ignore_net_suppression=True,
                ignore_js8_busy=True,
                ignore_varac_busy=True,
                ignore_fldigi_busy=True,
                apply_fldigi=True,
            )
        else:
            self.apply_current_entry(
                force=True,
                ignore_wait_prompt=True,
                ignore_coordination_prompt=ignore_coordination_prompt,
                ignore_suspend=True,
                ignore_net_suppression=True,
                ignore_js8_busy=True,
                ignore_varac_busy=True,
                ignore_fldigi_busy=True,
                apply_fldigi=True,
            )
        self._maybe_apply_fldigi()
        self._net_resume_apply_once = False
        self._schedule_forced_retry()
        return True

    def suspend_schedule(
        self,
        minutes: Optional[int] = None,
        *,
        target_device_profile_id: Optional[int] = None,
    ) -> None:
        """
        Suspend schedule-driven corrections for the requested duration.

        This is intended for user-invoked temporary holds from global UI controls.
        """
        mins = self._normalize_hold_minutes(minutes if minutes is not None else self._default_hold_minutes())
        self._record_scheduler_event(
            "hold",
            "schedule_suspended",
            action=f"Scheduler manually suspended for {mins} minutes",
            detail="FIO will keep showing schedule state, but automatic frequency changes are paused until resumed or the hold expires.",
            throttle_sec=0.0,
            minutes=mins,
        )
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
        self._suspend_for_minutes(mins, target_device_profile_id=target_device_profile_id)

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

    def _js8_group_control_supported(self) -> bool:
        return bool(self.js8 and hasattr(self.js8, "set_group") and hasattr(self.js8, "get_group"))

    def _entry_js8_group_key(self, js8_group: object = "") -> str:
        if not self._js8_group_control_supported():
            return ""
        return str(js8_group or "").strip()

    def _frequency_tolerance_hz(self, entry: Optional[Dict] = None, control_mode: Optional[str] = None) -> int:
        try:
            configured = self.settings.get("scheduler_frequency_tolerance_hz", None)
        except Exception:
            configured = None
        try:
            if configured not in (None, ""):
                value = max(1, min(1000, int(float(configured))))
                if value > 250:
                    self._record_scheduler_health_issue(
                        "frequency-tolerance",
                        f"configured scheduler frequency tolerance is wide ({value} Hz)",
                        cooldown_sec=300.0,
                        tolerance_hz=value,
                    )
                    self._record_scheduler_event(
                        "status",
                        "frequency_tolerance_wide",
                        action="Configured scheduler frequency tolerance is wide",
                        detail=f"Scheduler frequency tolerance is set to {value} Hz. Wide values can hide schedule drift.",
                        tolerance_hz=value,
                        throttle_sec=300.0,
                    )
                return value
        except Exception:
            pass
        row = entry or {}
        mode_txt = str(row.get("mode") or row.get("fldigi_mode") or "").strip().upper()
        band_txt = str(row.get("band") or "").strip().upper()
        if mode_txt in {"SSB", "VOICE", "USB", "LSB", "AM", "FM"} or band_txt in {"SSB", "VOICE"}:
            return 50
        return 5

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
            (row.get("vfo") or "").strip().upper(),
            (row.get("fldigi_mode") or "").strip().upper(),
            str(row.get("fldigi_offset") or "").strip(),
            str(row.get("js8_offset") or row.get("js8call_offset") or "").strip(),
            (row.get("start_utc") or "").strip(),
            (row.get("end_utc") or "").strip(),
        )

    def _manual_qsy_identity(self, entry: Optional[Dict]) -> Tuple[object, ...]:
        row = entry or {}
        vfo_raw = str(row.get("vfo") or "A").strip().upper()[:1]
        vfo = vfo_raw if vfo_raw in {"A", "B"} else "A"
        return (
            str(row.get("band") or "").strip().upper(),
            self._parse_freq_hz(str(row.get("frequency") or "").strip()),
            vfo,
            self._entry_js8_group_key(row.get("primary_js8call_group") or ""),
            self._resolve_rig_mode(row),
            self._js8_offset_setting(),
            self._expected_fldigi_mode(row),
            self._expected_fldigi_offset(row),
        )

    def _manual_control_target_from_entry(
        self,
        entry: Optional[Dict],
        *,
        source_action: str,
    ) -> Optional[SchedulerManualTarget]:
        row = entry or {}
        frequency_hz = self._parse_freq_hz(str(row.get("frequency") or "").strip()) or 0
        if frequency_hz <= 0:
            return None
        vfo_raw = str(row.get("vfo") or "A").strip().upper()[:1]
        vfo = vfo_raw if vfo_raw in {"A", "B"} else "A"
        return SchedulerManualTarget(
            frequency_hz=frequency_hz,
            mode=self._resolve_rig_mode(row),
            vfo=vfo,
            offset_hz=self._js8_offset_setting(),
            source_action=source_action,
        )

    def _record_manual_qsy_state(self, entry: Optional[Dict], *, operator_source: str = "controlfreq") -> None:
        radio_id = self._entry_manual_control_radio_id(entry)
        target = self._manual_control_target_from_entry(entry, source_action="qsy")
        if radio_id is None or target is None:
            return
        try:
            self._manual_control_service.set_manual_qsy(
                radio_id,
                target,
                reason_code="operator_qsy",
                operator_source=operator_source,
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to persist manual QSY state: %s", exc)

    def _record_manual_hold_state(
        self,
        *,
        until: datetime.datetime,
        operator_source: str = "main_control_center",
        target_device_profile_id: Optional[int] = None,
    ) -> None:
        radio_id = target_device_profile_id if target_device_profile_id is not None else self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        try:
            hold_until_utc = (
                until.astimezone(datetime.timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
            self._manual_control_service.hold(
                radio_id,
                hold_until_utc=hold_until_utc,
                reason_code="operator_hold",
                operator_source=operator_source,
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to persist manual hold state: %s", exc)

    def _record_manual_resume_state(self, radio_id: Optional[int] = None) -> None:
        radio_id = radio_id if radio_id is not None else self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        try:
            self._manual_control_service.resume(radio_id)
        except Exception as exc:
            log.debug("SchedulerEngine: failed to persist manual resume state: %s", exc)

    def _record_manual_suspend_state(
        self,
        *,
        operator_source: str = "scheduler_prompt",
        target_device_profile_id: Optional[int] = None,
    ) -> None:
        radio_id = target_device_profile_id if target_device_profile_id is not None else self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        try:
            self._manual_control_service.suspend(
                radio_id,
                reason_code="operator_suspend",
                operator_source=operator_source,
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to persist manual suspend state: %s", exc)

    def _shared_ptt_busy_evidence_id(self, radio_id: int) -> str:
        return f"busy_shared_ptt_{int(radio_id)}"

    def _local_ptt_busy_evidence_id(self, radio_id: int) -> str:
        return f"busy_local_ptt_{int(radio_id)}"

    def _external_busy_evidence_id(self, kind: str, radio_id: int) -> str:
        normalized = (kind or "external").strip().lower().replace("-", "_")
        return f"busy_{normalized}_{int(radio_id)}"

    def _fldigi_busy_evidence_id(self, radio_id: int) -> str:
        return f"busy_fldigi_{int(radio_id)}"

    @staticmethod
    def _external_busy_evidence_fields(kind: str, *, protected_busy: bool = False) -> Tuple[str, str]:
        key = (kind or "").strip().lower()
        if key == "js8":
            return "js8", "js8_tx"
        if key == "varac-wait":
            return "varac", "varac_waiting_for_frequency"
        if key == "varac" and protected_busy:
            return "varac", "varac_transfer"
        if key == "varac":
            return "varac", "varac_busy"
        return "unknown", "control_backend_busy"

    def _publish_external_busy_evidence(
        self,
        *,
        kind: str,
        reason: str,
        protected_busy: bool = False,
    ) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        source_family, reason_code = self._external_busy_evidence_fields(kind, protected_busy=protected_busy)
        detail = str(reason or "busy").strip() or "busy"
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        try:
            self._busy_evidence_service.publish(
                BusyEvidence(
                    id=self._external_busy_evidence_id(kind, radio_id),
                    radio_profile_id=f"radio_{radio_id}",
                    source_family=source_family,
                    reason_code=reason_code,
                    severity="hard" if protected_busy else "soft",
                    evidence_timestamp_utc=now,
                    description=detail,
                )
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to publish external busy evidence: %s", exc)

    def _clear_external_busy_evidence(self, kind: str) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        try:
            self._busy_evidence_service.clear(self._external_busy_evidence_id(kind, radio_id))
        except Exception as exc:
            log.debug("SchedulerEngine: failed to clear external busy evidence: %s", exc)

    def _publish_fldigi_busy_evidence(self, *, reason: str) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        detail = str(reason or "RX activity").strip() or "RX activity"
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        try:
            self._busy_evidence_service.publish(
                BusyEvidence(
                    id=self._fldigi_busy_evidence_id(radio_id),
                    radio_profile_id=f"radio_{radio_id}",
                    source_family="fl",
                    reason_code="receive_decode",
                    severity="soft",
                    evidence_timestamp_utc=now,
                    description=detail,
                )
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to publish FLDigi busy evidence: %s", exc)

    def _clear_fldigi_busy_evidence(self) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        try:
            self._busy_evidence_service.clear(self._fldigi_busy_evidence_id(radio_id))
        except Exception as exc:
            log.debug("SchedulerEngine: failed to clear FLDigi busy evidence: %s", exc)

    def _publish_local_ptt_busy_evidence(self, *, source: str) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        try:
            self._busy_evidence_service.publish(
                BusyEvidence(
                    id=self._local_ptt_busy_evidence_id(radio_id),
                    radio_profile_id=f"radio_{radio_id}",
                    source_family="ptt",
                    reason_code="ptt_active",
                    severity="hard",
                    evidence_timestamp_utc=now,
                    description="Rig PTT is active.",
                )
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to publish local PTT busy evidence: %s", exc)

    def _clear_local_ptt_busy_evidence(self) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        try:
            self._busy_evidence_service.clear(self._local_ptt_busy_evidence_id(radio_id))
        except Exception as exc:
            log.debug("SchedulerEngine: failed to clear local PTT busy evidence: %s", exc)

    def _publish_shared_ptt_block_evidence(
        self,
        shared_ptt: Dict[str, object],
        *,
        source: str,
    ) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        ptt_group = str(shared_ptt.get("ptt_group", "") or "").strip()
        if not ptt_group:
            return
        owner_id = shared_ptt.get("owner_device_profile_id")
        try:
            owner_device_id = int(owner_id) if owner_id not in (None, "") else None
        except Exception:
            owner_device_id = None
        reason = str(shared_ptt.get("reason", "") or "").strip() or f"Shared PTT group {ptt_group} is active."
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        try:
            self._busy_evidence_service.publish(
                BusyEvidence(
                    id=self._shared_ptt_busy_evidence_id(radio_id),
                    radio_profile_id=f"radio_{radio_id}",
                    source_family="ptt",
                    reason_code="shared_ptt_interlock",
                    severity="hard",
                    evidence_timestamp_utc=now,
                    description=reason,
                )
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to publish shared PTT busy evidence: %s", exc)
        try:
            self._ptt_conflict_service.publish(
                PttConflictEvidence(
                    id=f"ptt_shared_{int(radio_id)}",
                    ptt_group=ptt_group,
                    requested_radio_id=f"radio_{radio_id}",
                    blocking_radio_id=f"radio_{owner_device_id}" if owner_device_id is not None else None,
                    severity="hard",
                    source="scheduler_shared_ptt",
                    created_at_utc=now,
                )
            )
        except Exception as exc:
            log.debug("SchedulerEngine: failed to publish shared PTT conflict evidence: %s", exc)

    def _clear_shared_ptt_block_evidence(self) -> None:
        radio_id = self._primary_manual_control_radio_id()
        if radio_id is None:
            return
        try:
            self._busy_evidence_service.clear(self._shared_ptt_busy_evidence_id(radio_id))
        except Exception as exc:
            log.debug("SchedulerEngine: failed to clear shared PTT busy evidence: %s", exc)
        try:
            self._ptt_conflict_service.clear(f"ptt_shared_{int(radio_id)}")
        except Exception as exc:
            log.debug("SchedulerEngine: failed to clear shared PTT conflict evidence: %s", exc)

    def _last_entry_matches_schedule_identity(self, entry: Optional[Dict]) -> bool:
        key = self._last_entry_key
        if not isinstance(key, tuple) or len(key) < 7:
            return False
        row = entry or {}
        band = (row.get("band") or "").strip().upper()
        freq_hz = self._parse_freq_hz((row.get("frequency") or "").strip())
        js8_group = self._entry_js8_group_key(row.get("primary_js8call_group") or "")
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

    def _row_start_candidates(
        self,
        row: Dict,
        *,
        now_utc: datetime.datetime,
        horizon_days: int,
        include_early: bool = False,
    ) -> List[datetime.datetime]:
        out: List[datetime.datetime] = []
        smin = _parse_hhmm_to_minutes(str(row.get("start_utc") or ""))
        emin = _parse_hhmm_to_minutes(str(row.get("end_utc") or ""))
        if smin is None or emin is None:
            return out
        start_min = smin
        if include_early:
            try:
                start_min = max(0, smin - int(row.get("early_checkin", 0) or 0))
            except Exception:
                start_min = smin
        recurrence = str(row.get("recurrence") or "Weekly").strip()
        if recurrence == "Monthly":
            recurrence = "Periodic"
        month_weeks = self._parse_month_weeks(row.get("month_weeks", ""))
        row_day = str(row.get("day_utc") or "ALL").strip().upper()
        overnight = bool(smin > emin)
        base_date = now_utc.date()
        for offset in range(0, max(1, int(horizon_days)) + 1):
            day_date = base_date + datetime.timedelta(days=offset)
            weekday_name = _python_weekday_to_day_name(day_date.weekday())
            weekday_upper = weekday_name.upper()
            prev_day = _prev_day_name(weekday_name).upper()
            effective_day = row_day
            if recurrence == "Daily":
                effective_day = "ALL"
            if effective_day not in {"ALL", weekday_upper}:
                continue
            if not self._monthly_match(
                datetime.datetime.combine(day_date, datetime.time(12, 0), tzinfo=datetime.timezone.utc),
                weekday_upper if effective_day == "ALL" else effective_day,
                prev_day,
                recurrence,
                month_weeks,
                overnight,
            ):
                continue
            candidate = datetime.datetime.combine(
                day_date,
                datetime.time(start_min // 60, start_min % 60, tzinfo=datetime.timezone.utc),
            )
            if candidate > now_utc + datetime.timedelta(seconds=1):
                out.append(candidate)
        return out

    def _find_next_schedule_start(
        self,
        *,
        now_utc: datetime.datetime,
        hf_sched: List[Dict],
        net_sched: List[Dict],
        sop_sched: List[Dict],
        policy_rows: List[Dict],
        horizon_days: int = 62,
    ) -> Tuple[Optional[datetime.datetime], str, Optional[Dict]]:
        candidates: Set[datetime.datetime] = set()
        for row in hf_sched:
            candidates.update(self._row_start_candidates(row, now_utc=now_utc, horizon_days=horizon_days))
        for row in net_sched:
            candidates.update(self._row_start_candidates(row, now_utc=now_utc, horizon_days=horizon_days, include_early=True))
        for row in sop_sched:
            candidates.update(self._row_start_candidates(row, now_utc=now_utc, horizon_days=horizon_days, include_early=True))
        for candidate in sorted(candidates):
            probe_utc = candidate + datetime.timedelta(seconds=1)
            hf_next = self._find_active_hf_entry(probe_utc, hf_sched)
            net_next = self._find_active_net_entry(probe_utc, net_sched)
            sop_next, _sop_meta = self._find_active_sop_entry(probe_utc, sop_sched)
            next_source, next_entry, _policy = self._select_runtime_source(
                now_utc=probe_utc,
                hf_entry=hf_next,
                net_entry=net_next,
                sop_entry=sop_next,
                policy_rows=policy_rows,
            )
            if next_entry:
                return candidate, next_source, next_entry
        return None, "NONE", None

    def _read_station_actual_state(
        self,
        *,
        force: bool = False,
        control_mode: Optional[str] = None,
        allow_poll: bool = True,
    ) -> StationActualState:
        now_ts = time.time()
        mode = (control_mode or self._control_mode()).strip().upper()
        state = StationActualState(
            checked_ts=now_ts,
            flrig_freq_hz=self._status_flrig_freq_hz,
            flrig_ptt_active=bool(self._last_ptt_active),
            flrig_ptt_known=bool(self._status_flrig_ptt_known),
            flrig_ptt_age_s=(now_ts - self._status_flrig_ptt_ts) if self._status_flrig_ptt_ts else None,
            flrig_vfo=self._status_flrig_vfo,
            js8_freq_hz=self._status_js8_freq_hz,
            js8_offset_hz=self._status_js8_offset_hz,
            js8_offset_age_s=(now_ts - self._status_js8_offset_ts) if self._status_js8_offset_ts else None,
            fldigi_mode=self._fldigi_mode_cache,
            fldigi_offset_hz=self._fldigi_offset_cache,
        )
        if state.js8_offset_age_s is None or state.js8_offset_age_s > 30.0:
            state.js8_offset_stale = True
        if state.flrig_ptt_age_s is None or state.flrig_ptt_age_s > self._status_flrig_ptt_max_age_s:
            state.flrig_ptt_known = False
            state.flrig_ptt_stale = True
            state.flrig_ptt_active = False

        if force:
            freq = self._status_poll_rig_frequency(
                control_mode=mode if mode in {"FLRIG", "RIGCTLD"} else "FLRIG",
                force=True,
            )
            if isinstance(freq, (int, float)) and freq > 0:
                state.flrig_freq_hz = int(freq)
            elif state.flrig_freq_hz is None:
                state.errors["rig_frequency"] = "unavailable"
            if self.rig and hasattr(self.rig, "get_ptt"):
                ptt_active = self._status_poll_rig_ptt(force=True)
                state.flrig_ptt_active = bool(ptt_active and self._status_flrig_ptt_known)
                state.flrig_ptt_known = bool(self._status_flrig_ptt_known)
                state.flrig_ptt_age_s = 0.0 if state.flrig_ptt_known else None
                state.flrig_ptt_stale = not state.flrig_ptt_known
                if not state.flrig_ptt_known:
                    state.errors["rig_ptt"] = "unavailable"
            try:
                if self.rig and hasattr(self.rig, "get_active_vfo"):
                    vfo_txt = str(self.rig.get_active_vfo() or "").strip().upper()[:1]
                    if vfo_txt in {"A", "B"}:
                        state.flrig_vfo = vfo_txt
                        self._status_flrig_vfo = vfo_txt
                        self._status_flrig_vfo_ts = now_ts
            except Exception as e:
                state.errors["rig_vfo"] = str(e)
            if mode == "JS8CALL" or self._js8_offset_authority_active(self.current_schedule_entry, mode) or state.flrig_freq_hz is None:
                try:
                    if self.js8 and self._js8_running():
                        js8_freq = self.js8.get_frequency()
                        if isinstance(js8_freq, (int, float)) and js8_freq > 0:
                            state.js8_freq_hz = int(js8_freq)
                            self._status_js8_freq_hz = int(js8_freq)
                            self._status_js8_freq_ts = now_ts
                        js8_offset = self.js8.get_offset()
                        if isinstance(js8_offset, (int, float)):
                            state.js8_offset_hz = int(js8_offset)
                            self._status_js8_offset_hz = int(js8_offset)
                            self._status_js8_offset_ts = now_ts
                            state.js8_offset_age_s = 0.0
                            state.js8_offset_stale = False
                except Exception as e:
                    state.errors["js8_frequency"] = str(e)
            try:
                state.fldigi_mode = self._current_fldigi_mode()
                state.fldigi_offset_hz = self._current_fldigi_offset()
            except Exception as e:
                state.errors["fldigi"] = str(e)
        elif allow_poll:
            if state.flrig_freq_hz is None:
                rig_mode = mode if mode in {"FLRIG", "RIGCTLD"} else "FLRIG"
                state.flrig_freq_hz = self._status_poll_rig_frequency(control_mode=rig_mode)

        if state.flrig_freq_hz is not None:
            state.actual_frequency_hz = state.flrig_freq_hz
            state.actual_frequency_source = "Rig"
        elif state.js8_freq_hz is not None:
            state.actual_frequency_hz = state.js8_freq_hz
            state.actual_frequency_source = "JS8Call"
        else:
            state.actual_frequency_hz = None
            state.actual_frequency_source = "unknown"

        freshest_ts = max(
            float(self._status_flrig_freq_ts or 0.0),
            float(self._status_js8_freq_ts or 0.0),
            float(self._status_summary_external_ts or 0.0),
        )
        state.stale = bool(freshest_ts and now_ts - freshest_ts > 30.0)
        if state.actual_frequency_source == "Rig" and self._status_flrig_freq_ts:
            if now_ts - float(self._status_flrig_freq_ts) > 30.0:
                state.actual_frequency_hz = None
                state.actual_frequency_source = "unknown"
                state.stale = True
        elif state.actual_frequency_source == "JS8Call" and self._status_js8_freq_ts:
            if now_ts - float(self._status_js8_freq_ts) > 30.0:
                state.actual_frequency_hz = None
                state.actual_frequency_source = "unknown"
                state.stale = True
        if state.actual_frequency_hz is None:
            state.stale = True
        return state

    def _compute_off_schedule_state(
        self,
        entry: Dict,
        actual: StationActualState,
        *,
        control_mode: Optional[str] = None,
        check_frequency: bool = True,
        check_mode: bool = True,
        check_offset: bool = True,
        allow_external_reads: bool = True,
    ) -> OffScheduleState:
        flags = {"frequency": False, "mode": False, "offset": False, "fldigi_offset": False, "vfo": False}
        state = OffScheduleState(flags=flags)
        if not entry:
            return state
        active_control_mode = (control_mode or self._control_mode()).strip().upper()
        display_only_manual = active_control_mode in {"MANUAL", "NONE"}
        target_freq = self._parse_freq_hz((entry.get("frequency") or "").strip())
        state.target_frequency_hz = target_freq
        state.actual_frequency_hz = actual.actual_frequency_hz
        state.actual_frequency_source = actual.actual_frequency_source
        expected_vfo_raw = str(entry.get("vfo") or "").strip().upper()[:1]
        expected_vfo = expected_vfo_raw if expected_vfo_raw in {"A", "B"} else None
        actual_vfo_raw = str(actual.flrig_vfo or "").strip().upper()[:1]
        actual_vfo = actual_vfo_raw if actual_vfo_raw in {"A", "B"} else None
        state.target_vfo = expected_vfo
        state.actual_vfo = actual_vfo
        state.vfo_verified = bool(expected_vfo and actual_vfo)

        if check_frequency and target_freq:
            cur = actual.actual_frequency_hz
            if cur is None:
                if not display_only_manual:
                    flags["frequency"] = True
                    state.status_unknown = True
                    state.reasons.append("actual frequency unavailable")
                    self._record_scheduler_health_issue(
                        "actual-frequency",
                        "cannot verify actual frequency; schedule state is unknown",
                        cooldown_sec=30.0,
                        target_frequency_hz=target_freq,
                    )
                    self._record_scheduler_event(
                        "status",
                        "actual_frequency_unknown",
                        action="Cannot verify actual frequency",
                        detail="FIO could not read the rig or JS8Call frequency, so it will not treat the station as on schedule.",
                        frequency_hz=target_freq,
                        throttle_sec=60.0,
                    )
            elif abs(cur - target_freq) > self._frequency_tolerance_hz(entry, active_control_mode):
                flags["frequency"] = True
                tolerance_hz = self._frequency_tolerance_hz(entry, active_control_mode)
                state.reasons.append(
                    f"actual {cur} Hz from {actual.actual_frequency_source}, target {target_freq} Hz (tolerance {tolerance_hz} Hz)"
                )
                self._record_scheduler_event(
                    "drift",
                    "frequency_tolerance_exceeded",
                    action="Actual frequency is outside scheduler tolerance",
                    detail=f"Actual {cur} Hz from {actual.actual_frequency_source}; target {target_freq} Hz; tolerance {tolerance_hz} Hz.",
                    frequency_hz=target_freq,
                    actual_frequency_hz=cur,
                    actual_frequency_source=actual.actual_frequency_source,
                    tolerance_hz=tolerance_hz,
                    throttle_sec=30.0,
                )
                self._clear_scheduler_health_issue("actual-frequency")
            else:
                self._clear_scheduler_health_issue("actual-frequency")

        if display_only_manual:
            if expected_vfo and actual_vfo and expected_vfo != actual_vfo:
                flags["vfo"] = True
                state.reasons.append(f"VFO {actual_vfo}, target {expected_vfo}")
            state.off_schedule = any(flags.values())
            return state

        if expected_vfo and actual_vfo and expected_vfo != actual_vfo:
            flags["vfo"] = True
            state.reasons.append(f"VFO {actual_vfo}, target {expected_vfo}")
            self._record_scheduler_event(
                "drift",
                "vfo_mismatch",
                action="Actual VFO is outside the scheduled target",
                detail=f"Actual VFO {actual_vfo}; target VFO {expected_vfo}.",
                frequency_hz=target_freq,
                actual_vfo=actual_vfo,
                scheduled_vfo=expected_vfo,
                throttle_sec=30.0,
            )
        elif expected_vfo and actual.actual_frequency_source == "Rig" and not actual_vfo:
            state.status_unknown = True
            state.reasons.append(f"VFO target {expected_vfo}, not verified")
            self._record_scheduler_event(
                "status",
                "vfo_unverified",
                action="Cannot verify active VFO",
                detail=f"FIO expected VFO {expected_vfo}, but the rig did not report active VFO.",
                frequency_hz=target_freq,
                scheduled_vfo=expected_vfo,
                throttle_sec=60.0,
            )

        if check_offset and self._js8_offset_authority_active(entry, active_control_mode):
            desired_js8 = self._js8_offset_setting()
            current_js8 = actual.js8_offset_hz
            if current_js8 is None:
                state.status_unknown = True
                state.reasons.append("JS8 offset unavailable")
            elif actual.js8_offset_stale:
                state.status_unknown = True
                state.reasons.append("JS8 offset pending fresh verification")
            elif desired_js8 != current_js8:
                flags["offset"] = True
                state.reasons.append(f"JS8 offset {current_js8} Hz, target {desired_js8} Hz")

        if check_mode and self._fldigi_available():
            desired_mode = self._expected_fldigi_mode(entry)
            desired_offset = self._expected_fldigi_offset(entry)
            if desired_mode:
                current_mode = actual.fldigi_mode
                if current_mode is None and allow_external_reads:
                    current_mode = self._current_fldigi_mode()
                if current_mode is not None and current_mode != desired_mode.strip().upper():
                    flags["mode"] = True
                    state.reasons.append(f"FLDigi mode {current_mode}, target {desired_mode.strip().upper()}")
            if desired_offset is not None:
                current_offset = actual.fldigi_offset_hz
                if current_offset is None and allow_external_reads:
                    current_offset = self._current_fldigi_offset()
                if current_offset is not None and desired_offset != current_offset:
                    flags["fldigi_offset"] = True
                    state.reasons.append(f"FLDigi offset {current_offset} Hz, target {desired_offset} Hz")

        state.off_schedule = any(flags.values())
        return state

    def get_status_summary(self, *, live: bool = False, refresh: bool = True) -> Dict[str, object]:
        now_cache = time.time()
        if (
            not live
            and
            self._status_summary_cache is not None
            and now_cache - self._status_summary_cache_ts < self._status_summary_cache_ttl_s
        ):
            return dict(self._status_summary_cache)
        use_scheduler = self._scheduler_enabled()
        control_mode = self._control_mode() if live else self._cached_control_mode()
        entry = self.current_schedule_entry or {}
        if live:
            self._maybe_refresh_external_status_snapshot(force=True)
        elif refresh:
            self._maybe_refresh_external_status_snapshot()
        actual_state = self._read_station_actual_state(force=False, control_mode=control_mode, allow_poll=False)
        off_state = (
            self._compute_off_schedule_state(
                entry,
                actual_state,
                control_mode=control_mode,
                allow_external_reads=False,
            )
            if entry
            else OffScheduleState()
        )
        flags = dict(off_state.flags)
        for key in ("frequency", "mode", "offset", "fldigi_offset", "vfo"):
            flags.setdefault(key, False)
        self._last_off_schedule_flags = dict(flags)
        off_schedule = bool(off_state.off_schedule)
        freq_hz = actual_state.actual_frequency_hz
        varac_status = dict(self._last_varac_status or {})
        js8_busy = bool(self._last_js8_busy)
        fldigi_result = dict(self._fldigi_busy_check_result or {})
        fldigi_busy = bool(self._fldigi_busy_since_ts is not None and fldigi_result.get("busy"))
        fldigi_busy_reason = self._fldigi_busy_last_reason if fldigi_busy else None
        ptt_active = bool(actual_state.flrig_ptt_known and actual_state.flrig_ptt_active)
        shared_ptt = self._shared_ptt_lock_status(force=False)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        suspended_until = self._suspend_until_dt()
        auto_resume_utc, auto_resume_source = self._auto_resume_utc(now_utc, suspended_until, flags)
        freq_label = ""
        if isinstance(freq_hz, (int, float)) and freq_hz > 0:
            freq_label = f"{freq_hz / 1_000_000:.3f}"
        source = self.current_source or "NONE"
        coordination_conflict = self._coordination_conflict_status(entry, source=source, force=False)
        net_kind = self._source_net_kind(source, entry)
        next_freq_hz = self._next_transition_freq_hz
        next_freq_label = ""
        next_freq_mhz = None
        if isinstance(next_freq_hz, (int, float)) and next_freq_hz > 0:
            next_freq_mhz = float(next_freq_hz) / 1_000_000.0
            next_freq_label = f"{next_freq_mhz:.3f}"
        next_entry_freq_mhz = None
        next_entry_freq_label = ""
        if isinstance(self._next_entry_freq_hz, (int, float)) and self._next_entry_freq_hz > 0:
            next_entry_freq_mhz = float(self._next_entry_freq_hz) / 1_000_000.0
            next_entry_freq_label = f"{next_entry_freq_mhz:.3f}"
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
            "js8_status_stale": bool(self._last_js8_status_stale),
            "js8_status_detail": str(self._last_js8_status_detail or "").strip(),
            "fldigi_busy": fldigi_busy,
            "fldigi_busy_reason": fldigi_busy_reason,
            "fldigi_busy_check_pending": bool(self._fldigi_busy_check_in_flight),
            "fldigi_busy_checked_at": fldigi_result.get("checked_ts"),
            "varac_busy": bool(varac_status.get("busy")),
            "varac_status_stale": bool(self._last_varac_status_stale),
            "varac_status_detail": str(self._last_varac_status_detail or "").strip(),
            "ptt_active": ptt_active,
            "ptt_state_known": bool(actual_state.flrig_ptt_known),
            "ptt_state_stale": bool(actual_state.flrig_ptt_stale),
            "ptt_state_age_s": actual_state.flrig_ptt_age_s,
            "shared_ptt_group": str(shared_ptt.get("ptt_group", "") or "").strip(),
            "shared_ptt_blocked": bool(shared_ptt.get("blocked")),
            "shared_ptt_owner_name": str(shared_ptt.get("owner_name", "") or "").strip(),
            "shared_ptt_reason": str(shared_ptt.get("reason", "") or "").strip(),
            "rf_conflict_warning": bool(coordination_conflict.get("warning")),
            "rf_conflict_summary": str(coordination_conflict.get("summary", "") or "").strip(),
            "rf_conflict_detail": str(coordination_conflict.get("detail", "") or "").strip(),
            "rf_conflict_signature": str(coordination_conflict.get("signature", "") or "").strip(),
            "rf_conflict_peer_name": str(coordination_conflict.get("peer_name", "") or "").strip(),
            "rf_conflict_peer_status_unknown": bool(coordination_conflict.get("peer_status_unknown")),
            "rf_conflict_peer_status_stale": bool(coordination_conflict.get("peer_status_stale")),
            "rf_conflict_peer_status_detail": str(coordination_conflict.get("peer_status_detail", "") or "").strip(),
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
            "next_transition_source": str(self._next_source or "NONE"),
            "next_net_kind": str(self._next_net_kind or ""),
            "next_frequency_label": next_freq_label,
            "next_frequency_mhz": next_freq_mhz,
            "next_transition_utc": self._next_transition_utc,
            "next_transition_note": str(self._next_transition_note or ""),
            "next_source_change": bool(self._next_source_change),
            "current_entry_end_utc": self._current_entry_end_utc,
            "next_entry_start_utc": self._next_entry_start_utc,
            "next_entry_source": str(self._next_entry_source or "NONE"),
            "next_entry_frequency_label": next_entry_freq_label,
            "next_entry_frequency_mhz": next_entry_freq_mhz,
            "schedule_gap_seconds": self._schedule_gap_seconds,
            "js8_group_control_supported": self._js8_group_control_supported(),
            "js8_group_planned_only": not self._js8_group_control_supported(),
            "fldigi_mode_off": fldigi_mode_off,
            "fldigi_offset_off": fldigi_offset_off,
            "schedule_state_unknown": bool(off_state.status_unknown),
            "actual_frequency_source": off_state.actual_frequency_source,
            "off_schedule_reasons": list(off_state.reasons),
            "scheduled_vfo": off_state.target_vfo,
            "actual_vfo": off_state.actual_vfo,
            "vfo_verified": bool(off_state.vfo_verified),
            "status_live": bool(live),
            "status_age_s": max(0.0, now_cache - float(self._status_summary_external_ts or 0.0))
            if self._status_summary_external_ts
            else None,
            "status_stale": bool(
                not live
                and self._status_summary_external_ts
                and now_cache - float(self._status_summary_external_ts or 0.0) > 30.0
            ),
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
        flags = {"frequency": False, "mode": False, "offset": False, "fldigi_offset": False, "vfo": False}
        if not entry:
            return flags
        active_control_mode = (control_mode or self._control_mode()).strip().upper()
        actual = self._read_station_actual_state(force=False, control_mode=active_control_mode, allow_poll=False)
        if current_freq_hz is not None:
            actual.actual_frequency_hz = int(current_freq_hz)
            actual.actual_frequency_source = "provided"
        state = self._compute_off_schedule_state(
            entry,
            actual,
            control_mode=active_control_mode,
            check_frequency=check_frequency,
            check_mode=check_mode,
            check_offset=check_offset,
            allow_external_reads=False,
        )
        return dict(state.flags)

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

    def _read_target_station_actual_state(
        self,
        entry: Dict,
        *,
        control_mode: Optional[str] = None,
    ) -> StationActualState:
        rig_client, js8_client, _varac_client, control_settings, target_radio_id = self._control_context_for_entry(entry)
        if target_radio_id is None:
            return self._read_station_actual_state(force=True, control_mode=control_mode, allow_poll=True)
        mode = self._control_mode_for_context(control_settings, rig=rig_client, js8=js8_client)
        if control_mode:
            requested_mode = str(control_mode or "").strip().upper()
            if requested_mode in {"FLRIG", "RIGCTLD", "JS8CALL"}:
                mode = requested_mode
        now_ts = time.time()
        state = StationActualState(checked_ts=now_ts)
        if mode in {"FLRIG", "RIGCTLD"} and rig_client is not None:
            try:
                if hasattr(rig_client, "get_vfo_frequency"):
                    freq = rig_client.get_vfo_frequency()
                    if isinstance(freq, (int, float)) and freq > 0:
                        state.flrig_freq_hz = int(freq)
                if hasattr(rig_client, "get_ptt"):
                    state.flrig_ptt_active = bool(rig_client.get_ptt())
                    state.flrig_ptt_known = True
                    state.flrig_ptt_age_s = 0.0
                    state.flrig_ptt_stale = False
                if hasattr(rig_client, "get_active_vfo"):
                    vfo_txt = str(rig_client.get_active_vfo() or "").strip().upper()[:1]
                    if vfo_txt in {"A", "B"}:
                        state.flrig_vfo = vfo_txt
            except Exception as exc:
                state.errors["rig_frequency"] = str(exc)
        if js8_client is not None and (mode == "JS8CALL" or state.flrig_freq_hz is None):
            try:
                if hasattr(js8_client, "get_frequency"):
                    freq = js8_client.get_frequency()
                    if isinstance(freq, (int, float)) and freq > 0:
                        state.js8_freq_hz = int(freq)
                if hasattr(js8_client, "get_offset"):
                    offset = js8_client.get_offset()
                    if isinstance(offset, (int, float)):
                        state.js8_offset_hz = int(offset)
                        state.js8_offset_age_s = 0.0
                        state.js8_offset_stale = False
            except Exception as exc:
                state.errors["js8_frequency"] = str(exc)
        if state.flrig_freq_hz is not None:
            state.actual_frequency_hz = state.flrig_freq_hz
            state.actual_frequency_source = "Rig"
        elif state.js8_freq_hz is not None:
            state.actual_frequency_hz = state.js8_freq_hz
            state.actual_frequency_source = "JS8Call"
        else:
            state.actual_frequency_hz = None
            state.actual_frequency_source = "unknown"
            state.stale = True
        return state

    def _fresh_off_schedule_state_for_prompt(
        self,
        entry: Dict,
        *,
        control_mode: Optional[str] = None,
        check_frequency: bool,
        check_mode: bool,
        check_offset: bool,
    ) -> OffScheduleState:
        actual = self._read_target_station_actual_state(entry, control_mode=control_mode)
        _rig_client, _js8_client, _varac_client, control_settings, _target_radio_id = self._control_context_for_entry(entry)
        effective_mode = control_mode
        if not effective_mode:
            effective_mode = self._control_mode_for_context(
                control_settings,
                rig=_rig_client,
                js8=_js8_client,
            )
        return self._compute_off_schedule_state(
            entry,
            actual,
            control_mode=effective_mode,
            check_frequency=check_frequency,
            check_mode=check_mode,
            check_offset=check_offset,
            allow_external_reads=False,
        )

    def _off_schedule_prompt_suppression_key(
        self,
        entry: Optional[Dict],
        items: Optional[List[str]],
        radio_id: Optional[int],
    ) -> Tuple[object, ...]:
        try:
            ident = int(radio_id or 0)
        except Exception:
            ident = 0
        normalized_items = tuple(sorted(str(item or "").strip() for item in (items or []) if str(item or "").strip()))
        return (ident, normalized_items, self._entry_transition_signature(entry or {}))

    def _off_schedule_prompt_suppressed(
        self,
        entry: Optional[Dict],
        items: Optional[List[str]],
        radio_id: Optional[int],
        now_ts: Optional[float] = None,
    ) -> bool:
        now = time.time() if now_ts is None else float(now_ts)
        suppressions = self._off_schedule_prompt_suppress_until_by_key
        expired = [key for key, until in suppressions.items() if float(until or 0.0) <= now]
        for key in expired:
            suppressions.pop(key, None)
        key = self._off_schedule_prompt_suppression_key(entry, items, radio_id)
        return float(suppressions.get(key) or 0.0) > now

    def _suppress_off_schedule_prompt_once(
        self,
        entry: Optional[Dict],
        items: Optional[List[str]],
        radio_id: Optional[int],
    ) -> None:
        key = self._off_schedule_prompt_suppression_key(entry, items, radio_id)
        intervals: List[int] = []
        item_set = {str(item or "").strip() for item in (items or [])}
        if "Frequency" in item_set:
            intervals.append(self._prompt_interval_minutes("freq_prompt_interval"))
        if item_set.intersection({"Mode", "FLDigi Mode", "FLDigi Offset"}):
            intervals.append(self._prompt_interval_minutes("fldigi_prompt_interval"))
        if "Offset" in item_set:
            intervals.append(self._prompt_interval_minutes("js8_prompt_interval"))
        minutes = max(intervals or [self._default_hold_minutes()])
        self._off_schedule_prompt_suppress_until_by_key[key] = time.time() + max(1, minutes) * 60

    def _record_off_schedule_prompt_context(
        self,
        entry: Optional[Dict],
        items: Optional[List[str]],
        radio_id: Optional[int],
    ) -> None:
        try:
            ident = int(radio_id or 0)
        except Exception:
            ident = 0
        if ident <= 0:
            return
        self._last_off_schedule_prompt_by_radio[ident] = {
            "entry": dict(entry or {}),
            "items": [str(item) for item in (items or [])],
        }

    def _maybe_prompt_enforcement(self) -> None:
        if not self._scheduler_enabled():
            self._prompt_active = False
            self._prompt_items = []
            self._last_fldigi_offset_prompt_sig = None
            return
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
        entry_radio_id = self._entry_manual_control_radio_id(entry)
        if self._radio_manual_control_blocks_off_schedule_prompt(entry_radio_id):
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
        entry_key = (
            entry_radio_id,
            entry.get("frequency"),
            entry.get("band"),
            entry.get("mode"),
            entry.get("group_name"),
        )
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
        if any(flags.values()):
            fresh_state = self._fresh_off_schedule_state_for_prompt(
                entry,
                check_frequency=freq_prompt,
                check_mode=fldigi_prompt,
                check_offset=js8_prompt,
            )
            flags = dict(fresh_state.flags)
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
        if self._off_schedule_prompt_suppressed(entry, items, entry_radio_id, now_ts):
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
        self._record_off_schedule_prompt_context(entry, items, entry_radio_id)
        self.off_schedule_detected.emit({"entry": entry, "items": items, "device_profile_id": entry_radio_id})
        self._last_fldigi_offset_prompt_sig = fldigi_offset_prompt_sig if bool(flags.get("fldigi_offset")) else None
        self._last_off_schedule_flags = dict(flags)

    def _hold_for_frequency_prompt(
        self,
        entry: Dict,
        source: str,
        off_state: OffScheduleState,
        *,
        want_freq_change: bool,
        ignore_wait_prompt: bool,
        frequency_hz: Optional[int],
    ) -> bool:
        if source == "QSY" or ignore_wait_prompt:
            return False
        if self._enforcement_mode("freq_enforcement_mode") != "Prompt":
            return False
        if not want_freq_change or not bool(off_state.flags.get("frequency")):
            return False
        now_ts = time.time()
        radio_id = self._entry_manual_control_radio_id(entry)
        if self._radio_manual_control_blocks_off_schedule_prompt(radio_id):
            return False
        fresh_state = self._fresh_off_schedule_state_for_prompt(
            entry,
            check_frequency=True,
            check_mode=False,
            check_offset=False,
        )
        if not bool(fresh_state.flags.get("frequency")):
            self._prompt_active = False
            self._prompt_items = []
            self._last_off_schedule_flags = dict(fresh_state.flags)
            return False
        off_state = fresh_state
        entry_key = (source, radio_id) + self._entry_transition_signature(entry)
        interval = self._prompt_interval_minutes("freq_prompt_interval")
        prompt_times = getattr(self, "_frequency_prompt_last_by_entry", None)
        if not isinstance(prompt_times, dict):
            prompt_times = {}
            self._frequency_prompt_last_by_entry = prompt_times
        last_prompt_ts = float(prompt_times.get(entry_key) or 0.0)
        prompt_due = now_ts - last_prompt_ts >= interval * 60
        same_prompt = self._prompt_active and self._prompt_entry_key == entry_key and "Frequency" in self._prompt_items
        should_emit = (not same_prompt) and prompt_due
        if self._off_schedule_prompt_suppressed(entry, ["Frequency"], radio_id, now_ts):
            self._prompt_active = False
            self._prompt_items = []
            self._last_off_schedule_flags = dict(off_state.flags)
            return False
        self._prompt_active = True
        self._prompt_items = ["Frequency"]
        self._prompt_entry_key = entry_key
        if should_emit:
            prompt_times[entry_key] = now_ts
            self._prompt_state["frequency"]["last_prompt_ts"] = now_ts
        self._last_off_schedule_flags = dict(off_state.flags)
        self._clear_coordination_prompt()
        self._record_scheduler_event(
            "hold",
            "frequency_prompt_review",
            source=source,
            entry=entry,
            action="Holding schedule frequency change for operator review",
            detail="Frequency control is set to Prompt, so FIO will not change the rig frequency until the operator approves.",
            frequency_hz=frequency_hz,
            throttle_sec=30.0,
        )
        if should_emit:
            self._record_off_schedule_prompt_context(entry, ["Frequency"], radio_id)
            self.off_schedule_detected.emit(
                {
                    "entry": entry,
                    "items": ["Frequency"],
                    "device_profile_id": radio_id,
                    "source": source,
                }
            )
        self.active_entry_changed.emit(entry, source)
        return True

    def resolve_off_schedule(
        self,
        action: str,
        items: Optional[List[str]] = None,
        minutes: Optional[int] = None,
        target_device_profile_id: Optional[int] = None,
    ) -> None:
        self._prompt_active = False
        self._prompt_items = []
        fldigi_items = {"Mode", "FLDigi Mode", "FLDigi Offset"}
        radio_id = None
        try:
            radio_id = int(target_device_profile_id or 0) or None
        except Exception:
            radio_id = None
        if action == "suspend":
            if items and any(item in fldigi_items for item in items):
                self._fldigi_force_apply_once = False
            self._reset_prompt_timers(items=items)
            if minutes is not None and int(minutes or 0) <= 0:
                self._record_manual_suspend_state(target_device_profile_id=radio_id)
            else:
                self._suspend_for_minutes(
                    self._normalize_hold_minutes(minutes if minutes is not None else self._default_hold_minutes()),
                    target_device_profile_id=radio_id,
                )
            return
        if action == "ignore":
            if items and any(item in fldigi_items for item in items):
                self._fldigi_force_apply_once = False
            prompt_context = self._last_off_schedule_prompt_by_radio.get(int(radio_id or 0), {})
            suppress_entry = prompt_context.get("entry") if isinstance(prompt_context, dict) else None
            suppress_items = items or (prompt_context.get("items") if isinstance(prompt_context, dict) else None)
            if not isinstance(suppress_entry, dict):
                suppress_entry = self.current_schedule_entry or {}
            self._suppress_off_schedule_prompt_once(suppress_entry, suppress_items, radio_id)
            self._reset_prompt_timers(items=items)
            return
        if action != "apply":
            return
        entry = self.current_schedule_entry or {}
        source = self.current_source
        if radio_id is not None:
            lane_source, lane_entry = self._active_schedule_entry_for_radio(radio_id, force=True)
            if lane_entry:
                source = lane_source or source
                entry = lane_entry
        if not entry:
            return
        apply_items = items or []
        if apply_items:
            self._reset_prompt_timers(items=apply_items)
        if "Frequency" in apply_items:
            if radio_id is not None:
                entry = dict(entry)
                entry["target_scope"] = "device_profile"
                entry["target_device_profile_id"] = radio_id
            self._apply_schedule_entry(
                entry,
                source,
                force=True,
                ignore_wait_prompt=True,
                ignore_coordination_prompt=True,
                ignore_suspend=True,
                ignore_net_suppression=True,
                ignore_js8_busy=True,
                ignore_varac_busy=True,
                ignore_fldigi_busy=True,
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

    def _suspend_for_minutes(
        self,
        minutes: int,
        *,
        target_device_profile_id: Optional[int] = None,
    ) -> None:
        try:
            until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
            if hasattr(self.settings, "set"):
                if target_device_profile_id is None:
                    self.settings.set("schedule_suspend_until", until.timestamp())
            self._record_manual_hold_state(
                until=until,
                operator_source="main_control_center",
                target_device_profile_id=target_device_profile_id,
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Internal evaluation
    # ------------------------------------------------------------------

    def _on_timer(self) -> None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        try:
            self._maybe_refresh_external_status_snapshot()
            if not self._apply_active_schedule_lanes(now_utc=now_utc):
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
        if not self._scheduler_enabled():
            return
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
            js8_group = self._entry_js8_group_key(entry.get("primary_js8call_group") or "")
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
        # Prompt mode is informational once the scheduler is authoritative.
        # The prompt UI can still explain the mismatch, but Resume Schedule
        # and active schedule transitions must bring FLDigi back to plan.
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
            target_cols = [
                "target_scope",
                "target_device_profile_id",
                "target_operating_profile_id",
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
            has_target_cols = self._table_has_columns(conn, "daily_schedule_tab", target_cols)

            if self._table_has_columns(conn, "daily_schedule_tab", new_cols):
                cur = conn.execute(
                    (
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
                            auto_tune,
                            target_scope,
                            target_device_profile_id,
                            target_operating_profile_id
                        FROM daily_schedule_tab
                        """
                        if has_target_cols
                        else """
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
                )
                rows: List[Dict] = []
                for fetched in cur.fetchall():
                    (
                        day_utc,
                        band,
                        mode,
                        vfo,
                        freq,
                        start_utc,
                        end_utc,
                        group_name,
                        auto_tune,
                        *target_meta,
                    ) = fetched
                    rows.append(
                        normalize_schedule_target_fields(
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
                                "target_scope": target_meta[0] if len(target_meta) > 0 else "station",
                                "target_device_profile_id": target_meta[1] if len(target_meta) > 1 else None,
                                "target_operating_profile_id": target_meta[2] if len(target_meta) > 2 else None,
                            }
                        )
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
                        normalize_schedule_target_fields(
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
                    "target_scope",
                    "target_device_profile_id",
                    "target_operating_profile_id",
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
                        normalize_schedule_target_fields(
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
                                "target_scope": row_data.get("target_scope") or "station",
                                "target_device_profile_id": row_data.get("target_device_profile_id"),
                                "target_operating_profile_id": row_data.get("target_operating_profile_id"),
                            }
                        )
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
        data = normalize_schedule_target_fields(row or {})
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
        target_scope = str(data.get("target_scope") or "station").strip().lower() or "station"
        target_device_profile_id = int(data.get("target_device_profile_id") or 0)
        target_operating_profile_id = int(data.get("target_operating_profile_id") or 0)
        return (
            f"NET|{group_name}|{band}|{freq}|{day}|{recurrence}|{biweekly}|"
            f"{month_weeks}|{start_utc}|{end_utc}|{net_name}|{target_scope}|"
            f"{target_device_profile_id}|{target_operating_profile_id}"
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
        primary_device_profile_id, primary_operating_profile_id = self._primary_schedule_target_context()
        cache_key = (
            self._db_mtime(config_db),
            self._db_mtime(nets_db),
            1 if self._sop_layer_enabled() else 0,
            int(primary_device_profile_id or 0),
            int(primary_operating_profile_id or 0),
        )

        if cache and not force and cache.get("cache_key") == cache_key and cache.get("data"):
            return cache["data"]  # type: ignore[return-value]

        assigned_hf, assigned_net, has_assigned_plan = self._load_assigned_frequency_plan_schedule_rows(primary_device_profile_id)
        hf_db = assigned_hf if has_assigned_plan else self._load_daily_schedule_from_db()
        net_db = assigned_net if has_assigned_plan else self._load_net_schedule_from_db()
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
        hf_rows = [normalize_schedule_target_fields(row) for row in hf if isinstance(row, dict)]
        net_rows = [normalize_schedule_target_fields(row) for row in net if isinstance(row, dict)]
        hf_filtered = self._filter_rows_for_runtime_target(
            hf_rows,
            primary_device_profile_id=primary_device_profile_id,
            primary_operating_profile_id=primary_operating_profile_id,
        )
        net_filtered = self._filter_rows_for_runtime_target(
            net_rows,
            primary_device_profile_id=primary_device_profile_id,
            primary_operating_profile_id=primary_operating_profile_id,
        )

        self._schedule_cache = {"cache_key": cache_key, "data": (hf_filtered, net_filtered, sop_layer, policies)}
        return hf_filtered, net_filtered, sop_layer, policies

    def _load_assigned_frequency_plan_schedule_rows(
        self,
        primary_device_profile_id: Optional[int],
    ) -> Tuple[List[Dict], List[Dict], bool]:
        if primary_device_profile_id in (None, ""):
            return [], [], False
        try:
            device_profile_id = int(primary_device_profile_id or 0)
        except Exception:
            return [], [], False
        if device_profile_id <= 0:
            return [], [], False
        try:
            store = MultiRadioStore(settings_db_path())
            assignment = store.get_effective_assigned_plan_for_device(device_profile_id)
        except Exception as exc:
            log.debug("SchedulerEngine: failed to load assigned frequency plan for radio %s: %s", device_profile_id, exc)
            return [], [], False
        if not assignment:
            return [], [], False
        plan = assignment.get("frequency_plan") if isinstance(assignment.get("frequency_plan"), dict) else None
        if not plan:
            try:
                plan_id = int(assignment.get("frequency_plan_id") or 0)
            except Exception:
                plan_id = 0
            if plan_id > 0:
                try:
                    plan = store.get_frequency_plan(plan_id)
                except Exception as exc:
                    log.debug("SchedulerEngine: failed to load assigned frequency plan %s: %s", plan_id, exc)
                    plan = None
        if not plan:
            return [], [], False
        try:
            refs = json.loads(str(plan.get("schedule_refs_json") or "[]"))
        except Exception as exc:
            log.debug("SchedulerEngine: assigned frequency plan has invalid schedule refs: %s", exc)
            return [], [], True
        if not isinstance(refs, list):
            return [], [], True
        hf_rows: List[Dict] = []
        net_rows: List[Dict] = []
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            row = dict(ref)
            row["target_scope"] = "device_profile"
            row["target_device_profile_id"] = device_profile_id
            row.setdefault("frequency_plan_id", plan.get("id"))
            row.setdefault("frequency_plan_name", plan.get("name") or "")
            source = str(row.get("source") or row.get("source_type") or "").strip().upper()
            source_table = str(row.get("source_table") or "").strip().lower()
            if source == "NET" or "net" in source_table:
                net_rows.append(row)
            else:
                hf_rows.append(row)
        if hf_rows or net_rows:
            log.debug(
                "SchedulerEngine: loaded assigned Frequency Plan for radio %s: %s HF row(s), %s net row(s)",
                device_profile_id,
                len(hf_rows),
                len(net_rows),
            )
        return hf_rows, net_rows, True

    def _load_active_schedule_lane_rows(self, *, force: bool = False) -> List[Dict[str, object]]:
        """
        Build schedule-row lanes for every active radio without touching any
        external radio/application status endpoint.

        This is intentionally a DB/config projection only. It gives UI, health,
        RF Guard, and future multi-radio scheduler execution one consistent
        view of each active radio's assigned plan while keeping polling bounded
        to the existing status coordinators.
        """
        cache = self._active_schedule_lane_rows_cache
        now_ts = time.monotonic()
        if (
            cache
            and not force
            and isinstance(cache.get("data"), list)
            and now_ts - float(cache.get("checked_ts") or 0.0) < self._active_schedule_lane_rows_cache_ttl_s
        ):
            return list(cache["data"])  # type: ignore[index,return-value]

        config_db = self._config_dir() / "freqinout.db"
        nets_db = self._config_dir() / "freqinout_nets.db"
        try:
            store = MultiRadioStore(settings_db_path())
            active_profiles = list(store.list_runtime_active_device_profiles())
        except Exception as exc:
            log.debug("SchedulerEngine: failed loading active radio schedule lanes: %s", exc)
            active_profiles = []

        assignment_by_device: Dict[int, Dict[str, Any]] = {}
        plan_by_device: Dict[int, Dict[str, Any]] = {}
        active_summary: List[Tuple[int, Optional[int], Optional[int], str]] = []
        for profile in active_profiles:
            try:
                device_id = int(profile.get("id", 0) or 0)
            except Exception:
                device_id = 0
            if device_id <= 0:
                continue
            try:
                operating_assignment = store.get_effective_assignment_for_device(device_id)
            except Exception:
                operating_assignment = None
            try:
                plan_assignment = store.get_effective_assigned_plan_for_device(device_id)
            except Exception:
                plan_assignment = None
            if isinstance(plan_assignment, dict):
                assignment_by_device[device_id] = dict(plan_assignment)
            operating_id = None
            if isinstance(operating_assignment, dict):
                try:
                    operating_id = int(operating_assignment.get("operating_profile_id") or 0) or None
                except Exception:
                    operating_id = None
            plan_id = None
            plan_updated = ""
            if isinstance(plan_assignment, dict):
                try:
                    plan_id = int(plan_assignment.get("frequency_plan_id") or 0) or None
                except Exception:
                    plan_id = None
                if plan_id:
                    try:
                        plan = store.get_frequency_plan(plan_id)
                    except Exception:
                        plan = None
                    if isinstance(plan, dict):
                        plan_by_device[device_id] = dict(plan)
                        plan_updated = str(plan.get("updated_utc") or "")
            active_summary.append((device_id, operating_id, plan_id, plan_updated))

        cache_key = (
            self._db_mtime(config_db),
            self._db_mtime(nets_db),
            1 if self._sop_layer_enabled() else 0,
            tuple(active_summary),
        )
        if cache and not force and cache.get("cache_key") == cache_key and isinstance(cache.get("data"), list):
            cache["checked_ts"] = now_ts
            return list(cache["data"])  # type: ignore[index,return-value]

        active_ids = [
            int(profile.get("id", 0) or 0)
            for profile in active_profiles
            if isinstance(profile, dict) and int(profile.get("id", 0) or 0) > 0
        ]
        needs_base_schedule = any(device_id not in assignment_by_device for device_id in active_ids)
        data: Dict[str, Any] = {}
        hf_db = None
        net_db = None
        if needs_base_schedule:
            data = self.settings.all()
            hf_db = self._load_daily_schedule_from_db()
            net_db = self._load_net_schedule_from_db()
        sop_layer_db = self._load_sop_schedule_layer_from_db()
        policy_db = self._load_sop_net_conflict_policies_from_db()

        hf_base = hf_db if hf_db is not None else data.get("hf_schedule") or data.get("daily_schedule") or []
        net_base = net_db if net_db is not None else data.get("net_schedule") or []
        sop_layer = sop_layer_db if sop_layer_db is not None else []
        policies = policy_db if policy_db is not None else []
        if not isinstance(hf_base, list):
            hf_base = []
        if not isinstance(net_base, list):
            net_base = []
        if not isinstance(sop_layer, list):
            sop_layer = []
        if not isinstance(policies, list):
            policies = []
        hf_base_rows = [normalize_schedule_target_fields(row) for row in hf_base if isinstance(row, dict)]
        net_base_rows = [normalize_schedule_target_fields(row) for row in net_base if isinstance(row, dict)]
        sop_rows = [normalize_schedule_target_fields(row) for row in sop_layer if isinstance(row, dict)]
        policy_rows = [dict(row) for row in policies if isinstance(row, dict)]

        lanes: List[Dict[str, object]] = []
        for profile in active_profiles:
            try:
                device_id = int(profile.get("id", 0) or 0)
            except Exception:
                device_id = 0
            if device_id <= 0:
                continue
            operating_id = None
            for summary_device_id, summary_operating_id, _plan_id, _plan_updated in active_summary:
                if summary_device_id == device_id:
                    operating_id = summary_operating_id
                    break
            assigned_hf, assigned_net, has_assigned_plan = self._load_assigned_frequency_plan_schedule_rows(device_id)
            if has_assigned_plan:
                hf_rows = [normalize_schedule_target_fields(row) for row in assigned_hf if isinstance(row, dict)]
                net_rows = [normalize_schedule_target_fields(row) for row in assigned_net if isinstance(row, dict)]
            else:
                hf_rows = self._filter_rows_for_runtime_target(
                    hf_base_rows,
                    primary_device_profile_id=device_id,
                    primary_operating_profile_id=operating_id,
                )
                net_rows = self._filter_rows_for_runtime_target(
                    net_base_rows,
                    primary_device_profile_id=device_id,
                    primary_operating_profile_id=operating_id,
                )
            lane_sop_rows = self._filter_rows_for_runtime_target(
                sop_rows,
                primary_device_profile_id=device_id,
                primary_operating_profile_id=operating_id,
            )
            plan_assignment = assignment_by_device.get(device_id, {})
            plan = plan_by_device.get(device_id, {})
            lanes.append(
                {
                    "device_profile": dict(profile),
                    "device_profile_id": device_id,
                    "device_name": str(profile.get("name") or ""),
                    "operating_profile_id": operating_id,
                    "frequency_plan_id": plan_assignment.get("frequency_plan_id"),
                    "frequency_plan_name": str(plan.get("name") or ""),
                    "has_assigned_plan": bool(has_assigned_plan),
                    "hf_rows": hf_rows,
                    "net_rows": net_rows,
                    "sop_rows": lane_sop_rows,
                    "policy_rows": policy_rows,
                }
            )

        self._active_schedule_lane_rows_cache = {"cache_key": cache_key, "checked_ts": now_ts, "data": list(lanes)}
        return lanes

    def active_schedule_lanes(
        self,
        *,
        force: bool = False,
        now_utc: Optional[datetime.datetime] = None,
    ) -> List[Dict[str, object]]:
        """
        Return the current schedule projection for every active radio.

        The projection is safe for frequent UI reads: schedule rows are cached
        by database/config mtime and active assignment identity, and each call
        only recomputes in-memory active/current/next selections.
        """
        if now_utc is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
        lanes: List[Dict[str, object]] = []
        for lane in self._load_active_schedule_lane_rows(force=force):
            hf_rows = list(lane.get("hf_rows") or [])
            net_rows = list(lane.get("net_rows") or [])
            sop_rows = list(lane.get("sop_rows") or [])
            policy_rows = list(lane.get("policy_rows") or [])
            try:
                hf_active = self._find_active_hf_entry(now_utc, hf_rows)
            except Exception as exc:
                log.debug("SchedulerEngine: failed active HF lane evaluation for %s: %s", lane.get("device_name"), exc)
                hf_active = None
            try:
                net_active = self._find_active_net_entry(now_utc, net_rows)
            except Exception as exc:
                log.debug("SchedulerEngine: failed active Net lane evaluation for %s: %s", lane.get("device_name"), exc)
                net_active = None
            try:
                sop_active, sop_meta = self._find_active_sop_entry(now_utc, sop_rows)
            except Exception as exc:
                log.debug("SchedulerEngine: failed active SOP lane evaluation for %s: %s", lane.get("device_name"), exc)
                sop_active = None
                sop_meta = {}
            source, active_entry, policy_override = self._select_runtime_source(
                now_utc=now_utc,
                hf_entry=hf_active,
                net_entry=net_active,
                sop_entry=sop_active,
                policy_rows=policy_rows,
            )
            try:
                next_start, next_source, next_entry = self._find_next_schedule_start(
                    now_utc=now_utc,
                    hf_sched=hf_rows,
                    net_sched=net_rows,
                    sop_sched=sop_rows,
                    policy_rows=policy_rows,
                )
            except Exception as exc:
                log.debug("SchedulerEngine: failed next lane evaluation for %s: %s", lane.get("device_name"), exc)
                next_start, next_source, next_entry = None, "NONE", None
            out = dict(lane)
            out.update(
                {
                    "current_source": source,
                    "current_entry": dict(active_entry) if isinstance(active_entry, dict) else {},
                    "hf_entry": dict(hf_active) if isinstance(hf_active, dict) else {},
                    "net_entry": dict(net_active) if isinstance(net_active, dict) else {},
                    "sop_entry": dict(sop_active) if isinstance(sop_active, dict) else {},
                    "sop_meta": dict(sop_meta) if isinstance(sop_meta, dict) else {},
                    "policy_override": dict(policy_override) if isinstance(policy_override, dict) else {},
                    "next_entry_start_utc": next_start,
                    "next_entry_source": next_source,
                    "next_entry": dict(next_entry) if isinstance(next_entry, dict) else {},
                    "row_counts": {
                        "hf": len(hf_rows),
                        "net": len(net_rows),
                        "sop": len(sop_rows),
                    },
                }
            )
            lanes.append(out)
        return lanes

    def _active_schedule_entry_for_radio(
        self,
        radio_id: Optional[int],
        *,
        force: bool = False,
    ) -> tuple[str, Dict[str, object]]:
        try:
            target_id = int(radio_id or 0)
        except Exception:
            target_id = 0
        if target_id <= 0:
            return "", {}
        for lane in self.active_schedule_lanes(force=force):
            if not isinstance(lane, dict):
                continue
            try:
                lane_id = int(lane.get("device_profile_id") or 0)
            except Exception:
                lane_id = 0
            if lane_id != target_id:
                continue
            entry = lane.get("current_entry")
            if not isinstance(entry, dict) or not entry:
                return str(lane.get("current_source") or "NONE"), {}
            row = dict(entry)
            row["target_scope"] = "device_profile"
            row["target_device_profile_id"] = target_id
            return str(lane.get("current_source") or "NONE"), row
        return "", {}

    def _apply_active_schedule_lanes(
        self,
        *,
        now_utc: datetime.datetime,
        force: bool = False,
    ) -> bool:
        lanes = self.active_schedule_lanes(force=force, now_utc=now_utc)
        for lane in lanes:
            if not isinstance(lane, dict):
                continue
            try:
                radio_id = int(lane.get("device_profile_id") or 0)
            except Exception:
                radio_id = 0
            if radio_id <= 0:
                continue
            source = str(lane.get("current_source") or "NONE")
            entry = lane.get("current_entry")
            if not isinstance(entry, dict) or not entry:
                continue
            suspended, _until = self._scheduling_suspended_for_radio(radio_id, now_utc)
            if suspended:
                continue
            row = dict(entry)
            row["target_scope"] = "device_profile"
            row["target_device_profile_id"] = radio_id
            self._apply_schedule_entry(
                row,
                source,
                now_utc=now_utc,
                force=force,
                scheduler_transition=force,
                ignore_wait_prompt=force,
                ignore_coordination_prompt=force,
                ignore_js8_busy=force,
                ignore_varac_busy=force,
                ignore_fldigi_busy=force,
            )
        return bool(lanes)

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
        self._current_entry_end_utc = self.next_change_utc if active_entry else None
        self._next_entry_start_utc = None
        self._next_entry_source = "NONE"
        self._next_entry_freq_hz = None
        self._schedule_gap_seconds = None
        preview_next_entry = None
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
                preview_next_entry = next_entry
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
        try:
            next_start, next_entry_source, next_entry = self._find_next_schedule_start(
                now_utc=now_utc,
                hf_sched=hf_sched,
                net_sched=net_sched,
                sop_sched=sop_sched,
                policy_rows=net_sop_policies,
            )
            if isinstance(next_start, datetime.datetime) and next_entry:
                self._next_entry_start_utc = next_start
                self._next_entry_source = next_entry_source
                self._next_entry_freq_hz = self._parse_freq_hz(
                    (next_entry.get("frequency") or "").strip()
                )
                gap_anchor = self.next_change_utc if active_entry else now_utc
                if isinstance(gap_anchor, datetime.datetime):
                    self._schedule_gap_seconds = int(
                        max(0.0, (next_start - gap_anchor).total_seconds())
                    )
                if not preview_next_entry and self._schedule_gap_seconds and self._schedule_gap_seconds > 0:
                    self._next_transition_note = (
                        f"Schedule gap; next {next_entry_source} entry starts after the gap"
                    )
        except Exception as e:
            log.debug("SchedulerEngine: next schedule start preview failed: %s", e)

        prev_source = self.current_source
        net_ended = prev_source == "NET" and source != "NET"
        net_started = prev_source != "NET" and source == "NET"

        if not active_entry:
            # No active schedule; if we previously had something applied,
            # we keep the rig where it was (no auto "clear") but still
            # notify UI that source is NONE.
            self._clear_coordination_prompt()
            self._record_scheduler_event(
                "skip",
                "no_active_schedule",
                source=source,
                action="No active schedule entry",
                detail="FIO found no HF, NET, or SOP schedule row active for this time.",
                throttle_sec=300.0,
            )
            if source != self.current_source or force:
                self.current_source = "NONE"
                self.current_schedule_entry = {}
                self.active_entry_changed.emit({}, "NONE")
            self._net_schedule_active = False
            self._net_fldigi_apply_allowed_once = False
            self._net_schedule_started_at = None
            self._net_schedule_entry_key = None
            self._last_scheduler_selection_sig = None
            self._clear_fldigi_busy_check_state()
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

    def apply_manual_qsy(self, entry: Dict, *, ignore_coordination_prompt: bool = False) -> None:
        """
        Apply an immediate user-driven QSY, bypassing suspend and force-applying the change.

        Expects entry to contain at least "frequency" (MHz). Mode/band/vfo/auto_tune
        are honored when provided.
        """
        shared_ptt = self._shared_ptt_lock_status(force=True)
        if bool(shared_ptt.get("blocked")):
            log.warning(
                "SchedulerEngine: refusing manual QSY due to shared PTT interlock: %s",
                str(shared_ptt.get("reason", "") or "").strip() or "shared PTT blocked",
            )
            return
        now = datetime.datetime.now(datetime.timezone.utc)
        self._manual_qsy_active = True
        self._manual_qsy_entry_key = self._manual_qsy_identity(entry)
        self._manual_qsy_radio_id = self._entry_manual_control_radio_id(entry)
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
            ignore_coordination_prompt=ignore_coordination_prompt,
            ignore_fldigi_busy=True,
        )
        if not self._coordination_prompt_active:
            self._record_manual_qsy_state(entry, operator_source="controlfreq")

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

    def _status_poll_rig_frequency(self, *, control_mode: Optional[str] = None, force: bool = False) -> Optional[int]:
        """
        Lightweight rig-backend status poll with short-lived caching/backoff.
        """
        if not self.rig or not hasattr(self.rig, "get_vfo_frequency"):
            self._status_flrig_freq_hz = None
            return None
        self._status_poll_coordinator.ttl_seconds = float(self._status_poll_ttl_s)
        self._status_poll_coordinator.retry_seconds = float(self._status_poll_retry_s)

        def _poll() -> Dict[str, object]:
            freq = self._current_rig_frequency(control_mode=control_mode or "FLRIG", status_cached=False)
            if not isinstance(freq, (int, float)) or int(freq) <= 0:
                raise RuntimeError("rig frequency unavailable")
            return {
                "frequency_hz": int(freq),
                "source": "scheduler_rig_frequency",
            }

        snapshot = self._status_poll_coordinator.get_snapshot(
            "scheduler:primary:rig_frequency",
            _poll,
            force=force,
        )
        self._status_flrig_freq_hz = snapshot.frequency_hz
        self._status_flrig_freq_ts = float(snapshot.generated_at or 0.0)
        self._status_flrig_retry_ts = float(snapshot.backoff_until or 0.0)
        return self._status_flrig_freq_hz

    def _status_poll_rig_ptt(self, *, force: bool = False) -> bool:
        """
        Lightweight rig-backend PTT status poll with shared retry backoff.
        """
        if not self.rig or not hasattr(self.rig, "get_ptt"):
            self._status_flrig_ptt = False
            self._status_flrig_ptt_known = False
            return False
        self._status_poll_coordinator.ttl_seconds = float(self._status_poll_ttl_s)
        self._status_poll_coordinator.retry_seconds = float(self._status_poll_retry_s)

        def _poll() -> Dict[str, object]:
            return {
                "ptt_active": bool(self.rig.get_ptt()),
                "ptt_known": True,
                "source": "scheduler_rig_ptt",
            }

        snapshot = self._status_poll_coordinator.get_snapshot(
            "scheduler:primary:rig_ptt",
            _poll,
            force=force,
        )
        if snapshot.errors or snapshot.source in {"error", "backoff"}:
            self._status_flrig_ptt = False
            self._status_flrig_ptt_known = False
        else:
            self._status_flrig_ptt = bool(snapshot.ptt_active)
            self._status_flrig_ptt_known = bool(snapshot.ptt_known)
            self._last_ptt_active = bool(snapshot.ptt_active)
        self._status_flrig_ptt_ts = float(snapshot.generated_at or 0.0)
        self._status_flrig_retry_ts = float(snapshot.backoff_until or 0.0)
        return self._status_flrig_ptt

    def get_status_poll_metrics(self) -> Dict[str, int]:
        return self._status_poll_coordinator.metrics_snapshot().as_dict()

    def _shared_ptt_lock_status(self, *, force: bool = False) -> Dict[str, object]:
        manager = getattr(self, "station_runtime_manager", None)
        if manager is None or not hasattr(manager, "shared_ptt_lock_snapshot"):
            return {
                "ptt_group": "",
                "blocked": False,
                "owner_device_profile_id": None,
                "owner_name": "",
                "owner_backend": "",
                "owner_ptt_active": False,
                "target_ptt_active": False,
                "reason": "",
            }
        try:
            snapshot = manager.shared_ptt_lock_snapshot(force=force)
        except Exception as exc:
            log.debug("SchedulerEngine: shared PTT status lookup failed: %s", exc)
            return {
                "ptt_group": "",
                "blocked": False,
                "owner_device_profile_id": None,
                "owner_name": "",
                "owner_backend": "",
                "owner_ptt_active": False,
                "target_ptt_active": False,
                "reason": "",
            }
        return {
            "ptt_group": str(getattr(snapshot, "ptt_group", "") or "").strip(),
            "blocked": bool(getattr(snapshot, "blocked", False)),
            "owner_device_profile_id": getattr(snapshot, "owner_device_profile_id", None),
            "owner_name": str(getattr(snapshot, "owner_name", "") or "").strip(),
            "owner_backend": str(getattr(snapshot, "owner_backend", "") or "").strip(),
            "owner_ptt_active": bool(getattr(snapshot, "owner_ptt_active", False)),
            "target_ptt_active": bool(getattr(snapshot, "target_ptt_active", False)),
            "reason": str(getattr(snapshot, "reason", "") or "").strip(),
        }

    def _coordination_conflict_status(
        self,
        entry: Optional[Dict],
        *,
        source: str,
        force: bool = False,
    ) -> Dict[str, object]:
        row = entry or {}
        if not row:
            return {}
        freq_hz = self._parse_freq_hz((row.get("frequency") or "").strip())
        band = (row.get("band") or "").strip().upper()
        if not band:
            band = _hz_to_amateur_band(freq_hz)
        if not band and freq_hz is None:
            return {}
        guard_status = self._antenna_supported_band_guard_status(
            row,
            source=source,
            target_band=band,
            target_frequency_hz=freq_hz,
        )
        runtime_status: Dict[str, object] = {}
        manager = getattr(self, "station_runtime_manager", None)
        if manager is None or not hasattr(manager, "evaluate_primary_rf_conflict"):
            return guard_status
        else:
            try:
                snapshot = manager.evaluate_primary_rf_conflict(
                    target_band=band,
                    target_frequency_hz=freq_hz,
                    source=source,
                    force=force,
                )
            except Exception as exc:
                log.debug("SchedulerEngine: RF conflict status lookup failed: %s", exc)
                return guard_status
            if snapshot is not None:
                runtime_status = {
                    "warning": True,
                    "summary": str(getattr(snapshot, "summary", "") or "").strip(),
                    "detail": str(getattr(snapshot, "detail", "") or "").strip(),
                    "signature": str(getattr(snapshot, "signature", "") or "").strip(),
                    "peer_device_id": getattr(snapshot, "peer_device_profile_id", None),
                    "peer_name": str(getattr(snapshot, "peer_name", "") or "").strip(),
                    "peer_band": str(getattr(snapshot, "peer_band", "") or "").strip(),
                    "peer_frequency_hz": getattr(snapshot, "peer_frequency_hz", None),
                    "target_band": str(getattr(snapshot, "target_band", "") or "").strip(),
                    "target_frequency_hz": getattr(snapshot, "target_frequency_hz", None),
                    "same_band": bool(getattr(snapshot, "same_band", False)),
                    "same_frequency": bool(getattr(snapshot, "same_frequency", False)),
                    "shared_antenna_groups": list(getattr(snapshot, "shared_antenna_groups", []) or []),
                    "shared_amplifier_groups": list(getattr(snapshot, "shared_amplifier_groups", []) or []),
                    "shared_frontend_groups": list(getattr(snapshot, "shared_frontend_groups", []) or []),
                    "shared_band_overlap_groups": list(getattr(snapshot, "shared_band_overlap_groups", []) or []),
                    "shared_advanced_frequency_groups": list(
                        getattr(snapshot, "shared_advanced_frequency_groups", []) or []
                    ),
                    "advanced_frequency_window_hz": getattr(snapshot, "advanced_frequency_window_hz", 0),
                    "frequency_delta_hz": getattr(snapshot, "frequency_delta_hz", None),
                    "guard_mode": str(getattr(snapshot, "guard_mode", "") or "prompt").strip().lower() or "prompt",
                    "blocked": bool(getattr(snapshot, "blocked", False)),
                    "peer_status_unknown": bool(getattr(snapshot, "peer_status_unknown", False)),
                    "peer_status_stale": bool(getattr(snapshot, "peer_status_stale", False)),
                    "peer_status_detail": str(getattr(snapshot, "peer_status_detail", "") or "").strip(),
                }
        return self._strictest_coordination_conflict_status(guard_status, runtime_status)

    @staticmethod
    def _coordination_guard_rank(status: Mapping[str, object]) -> int:
        if not status:
            return -1
        if bool(status.get("blocked")):
            return 3
        mode = normalize_rf_guard_mode(status.get("guard_mode", "confirm"), "confirm")
        return {"warn": 1, "confirm": 2, "block": 3}.get(mode, 2)

    @staticmethod
    def _coordination_conflict_signature(status: Mapping[str, object]) -> str:
        if not status:
            return ""
        explicit = str(status.get("signature", "") or "").strip()
        if explicit:
            return explicit
        parts = [
            str(status.get("summary") or "").strip(),
            str(status.get("detail") or "").strip(),
            str(status.get("guard_mode") or "").strip(),
            str(status.get("peer_device_id") or "").strip(),
            str(status.get("peer_name") or "").strip(),
            str(status.get("target_band") or "").strip(),
            str(status.get("target_frequency_hz") or "").strip(),
            str(status.get("advanced_frequency_window_hz") or "").strip(),
            str(status.get("frequency_delta_hz") or "").strip(),
        ]
        return "|".join(part for part in parts if part)

    @classmethod
    def _strictest_coordination_conflict_status(
        cls,
        first: Mapping[str, object],
        second: Mapping[str, object],
    ) -> Dict[str, object]:
        first_status = dict(first or {})
        second_status = dict(second or {})
        if not first_status:
            return second_status
        if not second_status:
            return first_status
        preferred, other = (
            (first_status, second_status)
            if cls._coordination_guard_rank(first_status) >= cls._coordination_guard_rank(second_status)
            else (second_status, first_status)
        )
        detail_parts = [
            str(preferred.get("detail") or preferred.get("summary") or "").strip(),
            str(other.get("detail") or other.get("summary") or "").strip(),
        ]
        combined = dict(preferred)
        combined["warning"] = True
        combined["detail"] = " ".join(part for part in detail_parts if part).strip()
        combined["signature"] = "||".join(
            part
            for part in (
                cls._coordination_conflict_signature(preferred),
                cls._coordination_conflict_signature(other),
            )
            if part
        )
        combined["blocked"] = cls._coordination_guard_rank(preferred) >= 3
        combined["peer_status_unknown"] = bool(preferred.get("peer_status_unknown")) or bool(other.get("peer_status_unknown"))
        combined["peer_status_stale"] = bool(preferred.get("peer_status_stale")) or bool(other.get("peer_status_stale"))
        peer_status_details = [
            str(preferred.get("peer_status_detail") or "").strip(),
            str(other.get("peer_status_detail") or "").strip(),
        ]
        combined["peer_status_detail"] = " ".join(
            part for idx, part in enumerate(peer_status_details) if part and part not in peer_status_details[:idx]
        ).strip()
        if not str(combined.get("peer_name", "") or "").strip():
            combined["peer_name"] = str(other.get("peer_name", "") or "").strip()
        return combined

    @staticmethod
    def _profile_supported_bands(profile: Mapping[str, object]) -> List[str]:
        raw = profile.get("antenna_supported_bands_json", "[]") if isinstance(profile, Mapping) else "[]"
        parsed: object
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = [part.strip() for part in raw.split(",") if part.strip()]
        elif isinstance(raw, (list, tuple, set)):
            parsed = list(raw)
        else:
            parsed = []
        out: List[str] = []
        items = parsed if isinstance(parsed, list) else []
        for item in items:
            token = str(item or "").strip().upper().replace(" ", "")
            if token and token not in out:
                out.append(token)
        return out

    def _primary_runtime_profile_for_guard(self) -> Dict[str, object]:
        manager = getattr(self, "station_runtime_manager", None)
        if manager is not None and hasattr(manager, "get_primary_runtime"):
            try:
                runtime = manager.get_primary_runtime()
                if runtime is not None and isinstance(getattr(runtime, "profile", None), dict):
                    return dict(runtime.profile)
            except Exception:
                pass
        try:
            store = MultiRadioStore(settings_db_path())
            profile = store.get_runtime_primary_device_profile()
            return dict(profile or {})
        except Exception:
            return {}

    def _antenna_supported_band_guard_status(
        self,
        entry: Mapping[str, object],
        *,
        source: str,
        target_band: str,
        target_frequency_hz: Optional[int],
    ) -> Dict[str, object]:
        profile = self._primary_runtime_profile_for_guard()
        if not profile:
            return {}
        supported = self._profile_supported_bands(profile)
        if not supported:
            return {}
        band = str(target_band or "").strip().upper()
        if not band:
            return {}
        if band in supported:
            return {}
        mode = normalize_rf_guard_mode(profile.get("antenna_band_guard_mode", "warn"))
        radio_name = str(profile.get("name", "") or "Radio").strip() or "Radio"
        summary = f"RF Safety Guard: {radio_name} antenna is not configured for {band}."
        detail = f"Antenna Supports These Bands: {', '.join(supported)}. Target band: {band}."
        signature = "|".join(
            [
                str(profile.get("id", "") or ""),
                "ANTENNA_BAND_SUPPORT",
                str(source or "").strip().upper(),
                band,
                str(int(target_frequency_hz) if isinstance(target_frequency_hz, (int, float)) else 0),
                mode,
            ]
        )
        return {
            "warning": True,
            "summary": summary,
            "detail": detail,
            "signature": signature,
            "peer_device_id": None,
            "peer_name": "",
            "peer_band": "",
            "peer_frequency_hz": None,
            "target_band": band,
            "target_frequency_hz": target_frequency_hz,
            "same_band": False,
            "same_frequency": False,
            "shared_antenna_groups": [],
            "shared_amplifier_groups": [],
            "shared_frontend_groups": [],
            "shared_band_overlap_groups": [],
            "guard_mode": mode,
            "blocked": mode == "block",
        }

    def evaluate_coordination_conflict(
        self,
        entry: Dict,
        *,
        source: str = "HF",
        force: bool = False,
    ) -> Dict[str, object]:
        return self._coordination_conflict_status(entry, source=source, force=force)

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
        if control_mode is not None and mode not in {"FLRIG", "RIGCTLD", "JS8CALL"}:
            return None
        if mode in {"FLRIG", "RIGCTLD"}:
            if status_cached:
                return self._status_poll_rig_frequency(control_mode=mode)
            try:
                if self.rig and hasattr(self.rig, "get_vfo_frequency"):
                    freq = self.rig.get_vfo_frequency()
                    if freq:
                        return freq
            except Exception as e:
                log.error("SchedulerEngine: failed to read current rig-backend frequency: %s", e)
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
            log.error("SchedulerEngine: failed to read current rig-backend frequency: %s", e)

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
        ignore_coordination_prompt: bool = False,
        ignore_net_suppression: bool = False,
        ignore_js8_busy: bool = False,
        ignore_varac_busy: bool = False,
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
        rig_client, js8_client, _varac_client, control_settings, target_radio_id = self._control_context_for_entry(
            effective_entry
        )
        # Extract fields
        band = (effective_entry.get("band") or "").strip().upper()
        freq_text = (effective_entry.get("frequency") or "").strip()
        js8_group = (effective_entry.get("primary_js8call_group") or "").strip()
        js8_group_key = self._entry_js8_group_key(js8_group)
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
        manual_qsy_key = self._manual_qsy_identity(effective_entry)
        if source != "QSY" and self._manual_qsy_active and not force:
            entry_radio_id = self._entry_manual_control_radio_id(effective_entry)
            if self._manual_qsy_radio_id in (None, entry_radio_id):
                self._clear_coordination_prompt()
                log.debug("SchedulerEngine: manual QSY active; skipping scheduled frequency change.")
                self._record_scheduler_event(
                    "skip",
                    "manual_qsy_active",
                    source=source,
                    entry=effective_entry,
                    action="Manual QSY holding current schedule entry",
                    detail="Resume Schedule clears manual QSY. Timed QSY clears when the timer expires.",
                    throttle_sec=30.0,
                )
                self._record_scheduler_health_issue(
                    "manual-qsy",
                    "Manual QSY is holding the current schedule entry",
                    cooldown_sec=30.0,
                    source=source,
                    frequency_hz=manual_qsy_key[1],
                )
                self.active_entry_changed.emit(effective_entry, source)
                return

        control_mode = self._control_mode_for_context(control_settings, rig=rig_client, js8=js8_client)
        # If we're not in JS8CALL mode and have no rig backend, just update UI state.
        if control_mode != "JS8CALL" and rig_client is None:
            self._clear_coordination_prompt()
            self._record_scheduler_event(
                "skip",
                "rig_backend_missing",
                source=source,
                entry=effective_entry,
                action="Schedule state updated, but no rig control backend is available",
                detail="FIO cannot send frequency commands until the selected control app is available.",
                throttle_sec=60.0,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return

        if control_mode == "MANUAL":
            self._clear_coordination_prompt()
            log.debug("SchedulerEngine: manual control selected; no frequency commands sent.")
            self._record_scheduler_event(
                "skip",
                "manual_control",
                source=source,
                entry=effective_entry,
                action="Schedule state updated; Manual control is selected",
                detail="FIO will not send frequency commands while control mode is Manual.",
                throttle_sec=60.0,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return
        if control_mode == "NONE":
            self._clear_coordination_prompt()
            control_get = getattr(control_settings, "get", None)
            try:
                control_label = control_get("control_via", "FLRig") if callable(control_get) else self.settings.get("control_via", "FLRig")
            except Exception:
                control_label = self.settings.get("control_via", "FLRig")
            log.debug(
                "SchedulerEngine: control backend unavailable for mode=%s; not sending commands.",
                control_label,
            )
            self._record_scheduler_event(
                "skip",
                "control_backend_unavailable",
                source=source,
                entry=effective_entry,
                action="Schedule state updated; selected control backend is unavailable",
                detail=f"Selected control path: {control_label}",
                throttle_sec=60.0,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return
        # Respect temporary suspend timer (QSY/Suspend button)
        suspend_now_utc = now_utc or datetime.datetime.now(datetime.timezone.utc)
        suspended_for_radio, suspended_until = self._scheduling_suspended_for_radio(target_radio_id, suspend_now_utc)
        if not ignore_suspend and suspended_for_radio:
            self._clear_coordination_prompt()
            until_txt = suspended_until.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%MZ") if suspended_until else ""
            log.debug("SchedulerEngine: scheduling suspended until %s; skipping frequency change.", until_txt)
            self._record_scheduler_event(
                "hold",
                "schedule_suspended",
                source=source,
                entry=effective_entry,
                action="Holding schedule change because scheduler is suspended",
                detail=f"Suspended until {until_txt}" if until_txt else "Scheduler suspend is active.",
                throttle_sec=30.0,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return

        # Scheduler master switch (from Settings tab)
        if not self._scheduler_enabled():
            self._clear_coordination_prompt()
            log.debug("SchedulerEngine: scheduler disabled in settings; no frequency changes sent.")
            self._record_scheduler_event(
                "skip",
                "scheduler_disabled",
                source=source,
                entry=effective_entry,
                action="Schedule state updated; scheduler automation is disabled",
                detail="Turn scheduler automation back on in Settings to allow FIO to send schedule frequency changes.",
                throttle_sec=60.0,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return
        # Parse frequency text early to support VarAC wait prompts.
        if not freq_text:
            log.warning("SchedulerEngine: schedule entry missing 'frequency'; skipping.")
            self._record_scheduler_event(
                "skip",
                "missing_frequency",
                source=source,
                entry=effective_entry,
                action="Schedule entry missing frequency",
                detail="FIO cannot apply a schedule entry without a frequency.",
                throttle_sec=60.0,
            )
            return
        freq_hz = self._parse_freq_hz(freq_text)
        if freq_hz is None:
            log.error("SchedulerEngine: invalid frequency text '%s'; skipping.", freq_text)
            self._record_scheduler_event(
                "skip",
                "invalid_frequency",
                source=source,
                entry=effective_entry,
                action="Schedule entry has an invalid frequency",
                detail=f"Frequency text was {freq_text!r}.",
                throttle_sec=60.0,
            )
            return
        if force or scheduler_transition:
            self._maybe_refresh_external_status_snapshot(force=True)
        if target_radio_id is not None:
            actual_state = self._read_target_station_actual_state(
                effective_entry,
                control_mode=control_mode,
            )
        else:
            actual_state = self._read_station_actual_state(
                force=False,
                control_mode=control_mode,
                allow_poll=False,
            )
        off_state = self._compute_off_schedule_state(
            effective_entry,
            actual_state,
            control_mode=control_mode,
        )
        current_freq_hz = off_state.actual_frequency_hz
        freq_matches = (
            current_freq_hz is not None
            and off_state.target_frequency_hz is not None
            and abs(current_freq_hz - off_state.target_frequency_hz)
            <= self._frequency_tolerance_hz(effective_entry, control_mode)
        )
        want_freq_change = bool(force or scheduler_transition or current_freq_hz is None or not freq_matches)
        self._last_off_schedule_flags = dict(off_state.flags)
        if self._hold_for_frequency_prompt(
            effective_entry,
            source,
            off_state,
            want_freq_change=want_freq_change,
            ignore_wait_prompt=ignore_wait_prompt,
            frequency_hz=freq_hz,
        ):
            return
        varac_status = dict(self._last_varac_status or {})
        if ignore_wait_prompt:
            if self._varac_wait_prompt_active:
                self._varac_wait_prompt_active = False
                self._varac_wait_prompt_entry_key = None
                self._varac_wait_since_ts = None
                self.varac_wait_cleared.emit()
        elif self._varac_wait_prompt_active and not bool(varac_status.get("waiting_for_frequency")):
            self._varac_wait_prompt_active = False
            self._varac_wait_prompt_entry_key = None
            self._varac_wait_since_ts = None
            self.varac_wait_cleared.emit()
        prompt_key = (band, freq_hz, vfo, js8_group_key)
        if (
            source != "NET"
            and want_freq_change
            and bool(varac_status.get("waiting_for_frequency"))
            and not ignore_wait_prompt
        ):
            varac_wait_delay, _varac_wait_reason = self._should_delay_for_external_busy(
                kind="varac-wait",
                entry_key=prompt_key,
                source=source,
                busy=True,
                reason="waiting for frequency confirmation",
                ignore_busy=ignore_wait_prompt,
            )
            if varac_wait_delay:
                self._clear_coordination_prompt()
                if (not self._varac_wait_prompt_active) or (self._varac_wait_prompt_entry_key != prompt_key):
                    self._varac_wait_prompt_active = True
                    self._varac_wait_prompt_entry_key = prompt_key
                    self.varac_wait_detected.emit({"entry": effective_entry, "source": source})
                self.active_entry_changed.emit(effective_entry, source)
                return
            if self._varac_wait_prompt_active:
                self._varac_wait_prompt_active = False
                self._varac_wait_prompt_entry_key = None
                self._varac_wait_since_ts = None
                self.varac_wait_cleared.emit()
        fldigi_delay, fldigi_reason = self._should_delay_for_fldigi(
            entry_key=prompt_key,
            source=source,
            target_frequency_hz=freq_hz,
            want_freq_change=want_freq_change,
            ignore_fldigi_busy=ignore_fldigi_busy,
        )
        # Safety: avoid changing frequency while a backend is busy transmitting.
        busy_reasons = []
        ptt_hold_active = False
        ptt_state_known = bool(actual_state.flrig_ptt_known and not actual_state.flrig_ptt_stale)
        if want_freq_change and ptt_state_known and actual_state.flrig_ptt_active:
            busy_reasons.append("Rig PTT is active")
            ptt_hold_active = True
            self._publish_local_ptt_busy_evidence(source=source)
            self._record_scheduler_health_issue(
                "flrig-ptt",
                "holding schedule change because rig PTT is active",
                source=source,
                frequency_hz=freq_hz,
                control_mode=control_mode,
                active_hold=True,
            )
            self._record_scheduler_event(
                "hold",
                "flrig_ptt",
                source=source,
                entry=effective_entry,
                entry_key=prompt_key,
                action="Holding schedule change because rig PTT is active",
                detail="FIO will not change frequency while the station appears to be transmitting.",
                frequency_hz=freq_hz,
                throttle_sec=15.0,
                control_mode=control_mode,
            )
        else:
            self._clear_local_ptt_busy_evidence()
        shared_ptt = self._shared_ptt_lock_status(force=bool(force))
        if want_freq_change and bool(shared_ptt.get("blocked")):
            shared_reason = str(shared_ptt.get("reason", "") or "").strip() or "Shared PTT interlock is active"
            busy_reasons.append(shared_reason)
            ptt_hold_active = True
            self._publish_shared_ptt_block_evidence(shared_ptt, source=source)
            self._record_scheduler_health_issue(
                "flrig-ptt",
                f"holding schedule change because {shared_reason}",
                source=source,
                frequency_hz=freq_hz,
                control_mode=control_mode,
                active_hold=True,
            )
            self._record_scheduler_event(
                "hold",
                "shared_ptt_interlock",
                source=source,
                entry=effective_entry,
                entry_key=prompt_key,
                action="Holding schedule change because shared PTT interlock is active",
                detail=shared_reason,
                frequency_hz=freq_hz,
                throttle_sec=15.0,
                control_mode=control_mode,
            )
        else:
            self._clear_shared_ptt_block_evidence()
        if not ptt_hold_active:
            self._clear_scheduler_health_issue("flrig-ptt")
            if self._last_ptt_active and not ptt_state_known:
                age = actual_state.flrig_ptt_age_s
                self._record_scheduler_health_issue(
                    "ptt-state",
                    "PTT state is stale or unknown; not holding schedule on stale PTT",
                    cooldown_sec=30.0,
                    source=source,
                    frequency_hz=freq_hz,
                    ptt_age_s=round(float(age), 1) if isinstance(age, (int, float)) else None,
                )
                self._record_scheduler_event(
                    "status",
                    "ptt_state_unknown",
                    source=source,
                    entry=effective_entry,
                    entry_key=prompt_key,
                    action="PTT state is stale or unknown",
                    detail="FIO will not hold the operating plan indefinitely on stale PTT state.",
                    frequency_hz=freq_hz,
                    throttle_sec=30.0,
                    ptt_age_s=round(float(age), 1) if isinstance(age, (int, float)) else None,
                )

        js8_delay, js8_reason = self._should_delay_for_external_busy(
            kind="js8",
            entry_key=prompt_key,
            source=source,
            busy=bool(want_freq_change and self._last_js8_busy),
            reason="RX/TX",
            ignore_busy=ignore_js8_busy,
        )
        if js8_delay:
            busy_reasons.append(f"JS8Call is busy ({js8_reason or 'RX/TX'})")

        varac_is_busy = bool(want_freq_change and not self._varac_busy_ok(status=varac_status))
        if varac_is_busy:
            varac_reason = str(varac_status.get("reason") or "").strip()
        else:
            varac_reason = ""
        varac_protected_transfer = bool(varac_status.get("db_transfer_active")) or varac_reason in {
            "transfer",
            "file_wait",
        }
        varac_delay, varac_delay_reason = self._should_delay_for_external_busy(
            kind="varac",
            entry_key=prompt_key,
            source=source,
            busy=varac_is_busy,
            reason=varac_reason or "busy",
            ignore_busy=bool(ignore_varac_busy and not varac_protected_transfer),
            protected_busy=varac_protected_transfer,
        )
        if varac_delay:
            if varac_delay_reason:
                busy_reasons.append(f"VarAC is busy ({varac_delay_reason})")
            else:
                busy_reasons.append("VarAC is busy")

        if bool(off_state.flags.get("frequency")):
            self._record_scheduler_event(
                "drift",
                "external_frequency_drift",
                source=source,
                entry=effective_entry,
                action="External frequency drift detected; applying active schedule",
                detail="; ".join(off_state.reasons),
                frequency_hz=freq_hz,
                throttle_sec=30.0,
                actual_frequency_hz=current_freq_hz,
                actual_frequency_source=off_state.actual_frequency_source,
            )

        if fldigi_delay:
            reason = "FLDigi RX activity"
            if fldigi_reason:
                reason = f"FLDigi RX activity ({fldigi_reason})"
            busy_reasons.append(reason)

        if busy_reasons:
            self._clear_coordination_prompt()
            log.warning(
                "SchedulerEngine: skipping frequency change for %s schedule due to activity: %s",
                source,
                "; ".join(busy_reasons),
            )
            self.active_entry_changed.emit(effective_entry, source)
            return

        coordination_conflict = self._coordination_conflict_status(effective_entry, source=source, force=bool(force))
        coordination_signature = self._coordination_conflict_signature(coordination_conflict)
        if bool(coordination_conflict.get("blocked")):
            self._clear_coordination_prompt()
            self._record_scheduler_event(
                "blocked",
                "rf_safety_guard_block",
                source=source,
                entry=effective_entry,
                action="Blocked schedule change by RF Safety Guard",
                detail=str(coordination_conflict.get("detail") or coordination_conflict.get("summary") or ""),
                frequency_hz=freq_hz,
                throttle_sec=30.0,
                signature=coordination_signature,
                guard_mode=str(coordination_conflict.get("guard_mode") or ""),
            )
            self.active_entry_changed.emit(effective_entry, source)
            return
        suppressed_signature = str(self._coordination_prompt_suppressed_signature or "").strip()
        if suppressed_signature and suppressed_signature != coordination_signature:
            self._coordination_prompt_suppressed_signature = None
            suppressed_signature = ""
        if not coordination_signature:
            self._coordination_prompt_suppressed_signature = None
        coordination_guard_mode = normalize_rf_guard_mode(coordination_conflict.get("guard_mode", "confirm"), "confirm")
        if coordination_signature and coordination_guard_mode == "warn":
            self._clear_coordination_prompt()
            self._record_scheduler_event(
                "warning",
                "rf_safety_guard_warning",
                source=source,
                entry=effective_entry,
                action="Continuing schedule change after RF Safety Guard warn-only notice",
                detail=str(coordination_conflict.get("detail") or coordination_conflict.get("summary") or ""),
                frequency_hz=freq_hz,
                throttle_sec=30.0,
                signature=coordination_signature,
                guard_mode=coordination_guard_mode,
            )
            should_prompt_coordination = False
        else:
            should_prompt_coordination = bool(
                coordination_signature
                and not ignore_coordination_prompt
                and coordination_signature != suppressed_signature
            )
        if self._coordination_prompt_active:
            active_signature = str(self._coordination_prompt_signature or "").strip()
            if (not should_prompt_coordination) or active_signature != coordination_signature:
                self._clear_coordination_prompt()
        if should_prompt_coordination:
            if (not self._coordination_prompt_active) or (self._coordination_prompt_signature != coordination_signature):
                self._coordination_prompt_active = True
                self._coordination_prompt_signature = coordination_signature
                self._coordination_prompt_payload = dict(coordination_conflict)
                self.coordination_conflict_detected.emit(dict(coordination_conflict))
            self._record_scheduler_event(
                "hold",
                "coordination_conflict",
                source=source,
                entry=effective_entry,
                action="Holding schedule change for RF Safety Guard operator review",
                detail=str(coordination_conflict.get("detail") or coordination_conflict.get("summary") or ""),
                frequency_hz=freq_hz,
                throttle_sec=30.0,
                signature=coordination_signature,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return

        log.info(
            "SchedulerEngine applying entry (%s) from %s: radio=%s band=%s freq=%s vfo=%s mode=%s comment=%s",
            control_mode,
            source,
            target_radio_id or "-",
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
            int(target_radio_id) if target_radio_id is not None else "station",
            band,
            freq_hz,
            fldigi_center,
            js8_tune,
            vfo,
            js8_group_key,
            rig_mode,
        )
        if self._net_corrections_suppressed() and not force and not ignore_net_suppression:
            if source == "NET" and self._last_entry_key != entry_key:
                self._net_fldigi_apply_allowed_once = True
            if self._manual_net_fldigi_active or self._manual_net_js8_active:
                self._clear_coordination_prompt()
                log.debug("SchedulerEngine: net active; skipping schedule enforcement.")
                self._record_scheduler_event(
                    "skip",
                    "manual_net_active",
                    source=source,
                    entry=effective_entry,
                    entry_key=entry_key,
                    action="Net schedule enforcement skipped during manual net control",
                    frequency_hz=freq_hz,
                    throttle_sec=30.0,
                )
                self.active_entry_changed.emit(effective_entry, source)
                return
            if self._last_entry_key == entry_key and not off_state.off_schedule:
                self._clear_coordination_prompt()
                log.debug("SchedulerEngine: net schedule active; skipping corrections for current entry.")
                self._record_scheduler_event(
                    "skip",
                    "net_corrections_suppressed",
                    source=source,
                    entry=effective_entry,
                    entry_key=entry_key,
                    action="Net schedule corrections suppressed for current entry",
                    detail="FIO is avoiding repeated corrections while the net schedule is active.",
                    frequency_hz=freq_hz,
                    throttle_sec=60.0,
                )
                self.active_entry_changed.emit(effective_entry, source)
                return
        if source in ("HF", "NET", "SOP") and scheduler_transition and apply_fldigi:
            # Only real scheduler row transitions should re-arm one-shot FLDigi
            # enforcement. Internal reapply key differences (resume/retry/
            # frequency-only actions) must not behave like schedule transitions.
            self._fldigi_force_apply_once = True
        if self._pending_entry_key == entry_key and not force:
            self._clear_coordination_prompt()
            log.debug("SchedulerEngine: control action skipped (pending entry key).")
            self._record_scheduler_event(
                "skip",
                "pending_entry_key",
                source=source,
                entry=effective_entry,
                entry_key=entry_key,
                action="Schedule control action already pending for this entry",
                frequency_hz=freq_hz,
                throttle_sec=15.0,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return
        already_applied = (
            self._last_entry_key == entry_key and self._last_source == source
        )
        if not force and already_applied and not off_state.off_schedule:
            self._clear_coordination_prompt()
            log.debug("SchedulerEngine: schedule entry already applied; skipping re-apply.")
            self._record_scheduler_event(
                "skip",
                "already_applied",
                source=source,
                entry=effective_entry,
                entry_key=entry_key,
                action="Schedule entry already applied",
                detail="FIO did not resend the same command because the active schedule key already matches the last successful apply.",
                frequency_hz=freq_hz,
                throttle_sec=120.0,
            )
            self.active_entry_changed.emit(effective_entry, source)
            return
        if not force and already_applied and off_state.status_unknown and not scheduler_transition:
            self._clear_coordination_prompt()
            self._record_scheduler_event(
                "correction",
                "stale_state_correction",
                source=source,
                entry=effective_entry,
                entry_key=entry_key,
                action="Actual frequency could not be verified; re-sending active schedule",
                detail="FIO will not treat the last successful command as proof that the station is still on schedule.",
                frequency_hz=freq_hz,
                throttle_sec=60.0,
            )
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
            rig_client=rig_client,
            js8_client=js8_client,
            allow_global_fallback=target_radio_id is None,
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
                ignore_coordination_prompt=ignore_coordination_prompt,
                ignore_js8_busy=ignore_js8_busy,
                ignore_varac_busy=ignore_varac_busy,
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
