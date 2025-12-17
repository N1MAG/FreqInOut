from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QObject, QTimer

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.rigctl_client import FLRigClient, FrequencyCommand


def _parse_hhmm_to_minutes(hhmm: str) -> Optional[int]:
    """
    Parse 'HH:MM' (24h) into minutes after midnight, or None if invalid.
    """
    try:
        hhmm = (hhmm or "").strip()
        h, m = [int(x) for x in hhmm.split(":")]
        if not (0 <= h < 24 and 0 <= m < 60):
            return None
        return h * 60 + m
    except Exception:
        return None


def _frequency_str_to_hz(freq_str: str) -> Optional[int]:
    """
    Convert a human-entered frequency like:
      "7.115.000" or "7.115" or "7115000"
    into an integer Hz.

    Strategy:
      - Remove all non-digits
      - Interpret result as Hz
    """
    if not freq_str:
        return None
    digits = "".join(ch for ch in freq_str if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _python_weekday_to_day_name(weekday: int) -> str:
    """
    Convert Python weekday (Monday=0) to our net/daily schedule day name,
    where the JSON uses Sunday..Saturday.
    """
    days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    idx_sun0 = (weekday + 1) % 7
    return days[idx_sun0]


def _hf_row_matches_day(row: Dict[str, Any], day_name: str) -> bool:
    """
    Daily/HF schedule day matching:

      - If 'day_utc' == "ALL" (case-insensitive) → applies every day.
      - If 'day_utc' missing/empty → treat as "ALL".
      - Otherwise must equal the given day_name exactly.

    This "ALL" logic is used **only** for the daily/HF schedule, not net_schedule.
    """
    day_val = (row.get("day_utc") or "ALL").strip()
    if day_val.upper() == "ALL":
        return True
    return day_val == day_name


# ======================================================================
#  SchedulerEngine
# ======================================================================


class SchedulerEngine(QObject):
    """
    Periodically checks the current time in UTC and applies the appropriate
    frequency schedule via FLRig.

    Priority rules:
      1. Net schedule has priority over daily/HF schedule.
      2. Within net_schedule:
         - Choose entries whose UTC day/time window matches now, including
           early_checkin minutes before start_utc.
      3. If no net matches, use hf_schedule / daily_schedule entries whose
         day_utc matches (or ALL) and start_utc <= now < end_utc.
      4. If neither matches, do nothing.

    Safety:
      - If FLRig reports PTT active, skip frequency change.
      - If JS8Call is busy (RX/TX) or VarAC is busy, skip frequency change.

    Extras:
      - VFO support:
          * Entries may include "vfo": "A" or "B".
          * This is passed through to FrequencyCommand for FLRig.
      - Auto-tune on band change:
          * If settings["tune_on_band_change"] is true and the band changes,
            SchedulerEngine calls rig.tune() after a successful set_frequency().
    """

    def __init__(
        self,
        parent: Optional[QObject] = None,
        poll_interval_ms: int = 30_000,
        rig: Optional[FLRigClient] = None,
        js8_client: Optional[object] = None,
        varac_client: Optional[object] = None,
    ):
        """
        parent: Qt parent (e.g., MainWindow)
        rig:    Optional FLRigClient (or other rig control client)
        js8_client / varac_client: optional digital-mode status clients
                                   expected to expose is_busy()
        """
        super().__init__(parent)
        self.settings = SettingsManager()

        # Rig and digital clients can be wired later
        self.rig: Optional[FLRigClient] = rig
        self.js8_client = js8_client
        self.varac_client = varac_client

        self.timer = QTimer(self)
        self.timer.setInterval(poll_interval_ms)
        self.timer.timeout.connect(self._tick)

        # Remember last applied entry
        self._last_source: Optional[str] = None   # "net" or "daily" or None
        self._last_key: Optional[str] = None      # unique key of last applied entry
        self._last_band: Optional[str] = None

        log.info("SchedulerEngine initialized (rig=%s)", type(rig).__name__ if rig else "None")

    # ------------- PUBLIC API -------------

    def start(self):
        log.info("SchedulerEngine starting (interval %d ms)", self.timer.interval())
        self.timer.start()

    def stop(self):
        log.info("SchedulerEngine stopping")
        self.timer.stop()

    def set_rig(self, rig: Optional[FLRigClient]):
        """
        Allow MainWindow or other code to wire in a rig object after construction.
        """
        self.rig = rig
        log.info("SchedulerEngine rig set to %s", type(rig).__name__ if rig else "None")

    # ------------- INTERNAL LOGIC -------------

    def _tick(self):
        """
        Periodic scheduler tick.

        - If no rig is configured yet, do nothing.
        - If rig has is_available() and returns False, do nothing.
        - Otherwise, run daily / net schedule logic.
        """
        rig = getattr(self, "rig", None)
        if rig is None:
            # Rig not wired yet; skip scheduling without crashing
            return

        # If rig exposes is_available(), respect it
        if hasattr(rig, "is_available") and not rig.is_available():
            return

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        minutes_utc = now_utc.hour * 60 + now_utc.minute

        # 1) Try net schedule (priority)
        net_entry = self._find_active_net_entry(now_utc, minutes_utc)

        if net_entry:
            self._apply_entry(net_entry, source="net")
            return

        # 2) Try daily/HF schedule
        daily_entry = self._find_active_daily_entry(now_utc, minutes_utc)

        if daily_entry:
            self._apply_entry(daily_entry, source="daily")
            return

        # 3) No schedule active
        if self._last_key is not None:
            log.info("No active schedule; leaving rig at last frequency.")
            self._last_key = None
            self._last_source = None

    # ------------- ENTRY FINDERS -------------

    def _find_active_daily_entry(
        self,
        now_utc: datetime.datetime,
        minutes_utc: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Find an active HF/daily schedule entry.

        Data source priority:
          1) "hf_schedule"
          2) "daily_schedule" (backwards compatibility)
        """
        data = self.settings.get("hf_schedule", None)
        if not isinstance(data, list):
            data = self.settings.get("daily_schedule", [])
        if not isinstance(data, list):
            return None

        day_name = _python_weekday_to_day_name(now_utc.weekday())

        candidates: List[Dict[str, Any]] = []

        for row in data:
            if not isinstance(row, dict):
                continue

            # Day match with "ALL" logic
            if not _hf_row_matches_day(row, day_name):
                continue

            start_m = _parse_hhmm_to_minutes(row.get("start_utc", ""))
            end_m = _parse_hhmm_to_minutes(row.get("end_utc", ""))
            if start_m is None or end_m is None:
                continue
            if start_m <= minutes_utc < end_m:
                candidates.append(row)

        if not candidates:
            return None

        candidates.sort(key=lambda r: _parse_hhmm_to_minutes(r.get("start_utc", "9999")) or 9999)
        return candidates[0]

    def _find_active_net_entry(
        self,
        now_utc: datetime.datetime,
        minutes_utc: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Find an active net_schedule entry for the current UTC day/time.

        No "ALL" logic here – day_utc must match the specific weekday name.
        """
        data = self.settings.get("net_schedule", [])
        if not isinstance(data, list):
            return None

        weekday = now_utc.weekday()  # Monday=0
        day_utc_name = _python_weekday_to_day_name(weekday)

        candidates: List[Dict[str, Any]] = []

        for row in data:
            if not isinstance(row, dict):
                continue

            row_day_utc = row.get("day_utc") or row.get("day")
            if row_day_utc != day_utc_name:
                continue

            start_m = _parse_hhmm_to_minutes(row.get("start_utc", ""))
            end_m = _parse_hhmm_to_minutes(row.get("end_utc", ""))
            if start_m is None or end_m is None:
                continue

            early = int(row.get("early_checkin", 0) or 0)
            window_start = max(0, start_m - early)

            if window_start <= minutes_utc < end_m:
                candidates.append(row)

        if not candidates:
            return None

        candidates.sort(key=lambda r: _parse_hhmm_to_minutes(r.get("start_utc", "9999")) or 9999)
        return candidates[0]

    # ------------- APPLY ENTRY -------------

    def _entry_key(self, entry: Dict[str, Any], source: str) -> str:
        parts = [
            source,
            str(entry.get("band", "")),
            str(entry.get("frequency", "")),
            str(entry.get("start_utc", "")),
            str(entry.get("end_utc", "")),
            str(entry.get("vfo", "")),
        ]
        return "|".join(parts)

    def _apply_entry(self, entry: Dict[str, Any], source: str):
        """
        Change rig frequency via FLRig if this entry is different from the last
        one we applied, and safety checks allow it.

        Also wires:
          - entry["vfo"] → FrequencyCommand.vfo (A/B/None)
          - tune on band change if settings["tune_on_band_change"] is True.
        """
        if self.rig is None:
            return

        key = self._entry_key(entry, source)
        if key == self._last_key:
            # Already applied this entry; nothing to do
            return

        # SAFETY CHECKS
        busy_reasons = []

        # 1) Rig PTT
        try:
            if hasattr(self.rig, "get_ptt") and self.rig.get_ptt():
                busy_reasons.append("FLRig PTT is active")
        except Exception as e:
            log.warning("SchedulerEngine: get_ptt() failed: %s", e)

        # 2) JS8Call busy?
        if self.js8_client is not None and hasattr(self.js8_client, "is_busy"):
            try:
                if self.js8_client.is_busy():
                    busy_reasons.append("JS8Call is busy (RX/TX)")
            except Exception as e:
                log.warning("SchedulerEngine: js8_client.is_busy() failed: %s", e)

        # 3) VarAC busy?
        if self.varac_client is not None and hasattr(self.varac_client, "is_busy"):
            try:
                if self.varac_client.is_busy():
                    busy_reasons.append("VarAC is busy")
            except Exception as e:
                log.warning("SchedulerEngine: varac_client.is_busy() failed: %s", e)

        if busy_reasons:
            log.warning(
                "Skipping frequency change for %s schedule due to activity: %s",
                source,
                "; ".join(busy_reasons),
            )
            return

        # Parse frequency
        freq_hz = _frequency_str_to_hz(entry.get("frequency", ""))
        if freq_hz is None:
            log.warning("Scheduler %s entry has invalid frequency: %s", source, entry.get("frequency"))
            return

        band = entry.get("band", "")
        js8_group = entry.get("group", "") or entry.get("primary_js8call_group", "")
        comment = entry.get("comment", "")
        vfo_raw = (entry.get("vfo") or "").strip().upper()
        vfo: Optional[str] = vfo_raw if vfo_raw in ("A", "B") else None

        log.info(
            "Scheduler activating %s schedule: band=%s freq=%d Hz vfo=%s group=%s comment=%s",
            source,
            band,
            freq_hz,
            vfo or "-",
            js8_group,
            comment,
        )

        # For now we don't change mode here (could be extended later).
        cmd = FrequencyCommand(
            frequency_hz=freq_hz,
            mode=None,
            vfo=vfo,
        )

        # Optional tune at band change
        try:
            tune_on_band_change = bool(self.settings.get("tune_on_band_change", False))
        except Exception:
            data = self.settings.all()
            tune_on_band_change = bool(data.get("tune_on_band_change", False))

        band_changed = bool(band) and band != self._last_band

        try:
            ok = self.rig.set_frequency(cmd)
        except Exception as e:
            log.error("FLRig set_frequency failed for schedule entry: %s", e)
            ok = False

        if ok:
            log.info("FLRig frequency change applied successfully.")
            if tune_on_band_change and band_changed and hasattr(self.rig, "tune"):
                try:
                    log.info("Band changed (%s -> %s); invoking FLRig tune().", self._last_band, band)
                    self.rig.tune()
                except Exception as e:
                    log.error("FLRig tune() failed after band change: %s", e)
            self._last_key = key
            self._last_source = source
            self._last_band = band
        else:
            log.error("FLRig frequency change failed for schedule entry.")

# ======================================================================
#  compute_next_change_time
# ======================================================================


def compute_next_change_time() -> Optional[datetime.datetime]:
    """
    Compute the next scheduled frequency-change time in UTC, looking at both:

      - net_schedule (with early_checkin)
      - hf_schedule / daily_schedule (with day_utc and 'ALL' logic)

    Returns:
      - datetime (timezone=UTC) of the next *start* window, or
      - None if no future entries are found within the next 7 days.

    This is intended for the UI "Next frequency change" button text.
    """

    settings = SettingsManager()
    now = datetime.datetime.now(datetime.timezone.utc)
    now_min = now.hour * 60 + now.minute

    # ---- Collect candidates from net_schedule ----
    net_rows = settings.get("net_schedule", [])
    if not isinstance(net_rows, list):
        net_rows = []

    candidates: List[datetime.datetime] = []

    # For the coming 7 days
    for offset in range(0, 7):
        day_dt = (now + datetime.timedelta(days=offset)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        day_name = _python_weekday_to_day_name(day_dt.weekday())

        for row in net_rows:
            if not isinstance(row, dict):
                continue
            if row.get("day_utc") != day_name:
                continue

            smin = _parse_hhmm_to_minutes(row.get("start_utc", ""))
            emin = _parse_hhmm_to_minutes(row.get("end_utc", ""))
            if smin is None or emin is None:
                continue

            early = int(row.get("early_checkin", 0) or 0)
            window_start_min = max(0, smin - early)

            candidate_dt = day_dt + datetime.timedelta(minutes=window_start_min)
            if candidate_dt <= now:
                # if this is "today" and already passed, skip; for other days offset>0 it's fine
                continue

            candidates.append(candidate_dt)

    # ---- Collect candidates from HF/daily schedule ----
    hf_rows = settings.get("hf_schedule", None)
    if not isinstance(hf_rows, list):
        hf_rows = settings.get("daily_schedule", [])
    if not isinstance(hf_rows, list):
        hf_rows = []

    for offset in range(0, 7):
        day_dt = (now + datetime.timedelta(days=offset)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        day_name = _python_weekday_to_day_name(day_dt.weekday())

        for row in hf_rows:
            if not isinstance(row, dict):
                continue
            # day_utc == "ALL" applies every day for HF
            if not _hf_row_matches_day(row, day_name):
                continue

            smin = _parse_hhmm_to_minutes(row.get("start_utc", ""))
            emin = _parse_hhmm_to_minutes(row.get("end_utc", ""))
            if smin is None or emin is None:
                continue

            candidate_dt = day_dt + datetime.timedelta(minutes=smin)
            if candidate_dt <= now:
                continue

            candidates.append(candidate_dt)

    if not candidates:
        return None

    next_dt = min(candidates)
    return next_dt
