from __future__ import annotations

import datetime
import re
import sqlite3
import time
import json
import queue
from pathlib import Path
from typing import List, Dict, Set, Optional

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QTextEdit,
    QPushButton,
    QMessageBox,
    QComboBox,
    QApplication,
    QSpinBox,
    QCompleter,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.utils.timezones import get_timezone
import psutil

# Vendored js8net (replacement for pyjs8call)
JS8NET_PATH = Path(__file__).resolve().parents[2] / "third_party" / "js8net" / "js8net-main"
if JS8NET_PATH.exists():
    import sys
    sys.path.insert(0, str(JS8NET_PATH))
try:
    import js8net  # type: ignore
except Exception:
    js8net = None


class JS8CallNetControlTab(QWidget):
    """
    JS8Call Net Control tab.

    Uses JS8Call's DIRECTED.TXT and net_schedule to manage JS8 nets:

    - Settings:
        * callsign, operator_name, operator_state
        * js8_directed_path: full path to JS8Call DIRECTED.TXT
        * js8_refresh_sec: poll interval (seconds)
    - Two panels:
        * Initial Check-ins
        * New / Late Check-ins
    - New calls:
        * Go to Initial until 'Copy Initial Check-ins' is clicked once
        * After that, only to New/Late
    - Deduplicates calls across the net.

    Buttons (in this order):
        Start Net, Copy Initial Check-ins, Copy New Check-ins,
        Merge Check-ins, Save Check-ins, End Net, QSY/Suspend (shared across tabs)

    - Copy Initial/New:
        * Parses callsigns
        * Dedup
        * Takes last 3 characters of each callsign
        * Copies space-delimited list to clipboard

    - End Net:
        * Stops polling
        * Writes log file in 'net_logs' under DIRECTED.TXT's directory
          filename: netname-ROLE-YYYYMMDD.txt
          header: net name, role, start/end UTC, band (if found)
          body: full callsigns, one per line

    - Auto-prefill Net Name:
        * Uses net_schedule and current UTC time
        * Looks for nets on current day whose (start_utc - early_checkin)
          is within next 20 minutes.
        * Prefills net name if the field is empty and no net is in progress.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()

        self._net_in_progress = False
        self._net_start_utc: str | None = None
        self._net_end_utc: str | None = None

        self._directed_path: Path | None = None
        self._last_directed_size: int = 0
        self._startup_directed_size: int = 0

        self._initial_phase = True  # true until first Copy Initial click
        self._initial_calls: Set[str] = set()
        self._late_calls: Set[str] = set()
        self._all_calls_seen: Set[str] = set()
        self._queried_msg_ids: Set[str] = set()
        self._pending_queries: List[tuple[Optional[float], str, str]] = []
        self._waiting_for_completion: bool = False
        self._current_query: tuple[str, str] | None = None
        self._js8_client = None
        self.auto_query_msg_id = bool(self.settings.get("js8_auto_query_msg_id", False))
        self.auto_query_grids = bool(self.settings.get("js8_auto_query_grids", False))
        self._last_rx_ts: float = 0.0
        self._js8_rx_timer: QTimer | None = None
        self._last_rx_ts: float = 0.0
        self._pending_grid_queries: List[tuple[Optional[float], str]] = []
        self._grid_waiting: bool = False
        self._grid_last_rx_ts: float = 0.0
        self._last_directed_size: int = 0
        self._last_all_size: int = 0
        self._last_query_tx_ts: float = 0.0
        self._app_start_ts: float = time.time()
        # Track inbound triggers to map replies to groups
        self._last_inbound_triggers: Dict[str, tuple[str, float]] = {}

        self._poll_timer: QTimer | None = None
        self._clock_timer: QTimer | None = None
        self._js8_rx_timer: QTimer | None = None

        self._build_ui()
        self._load_settings()
        self._setup_timer()
        self._update_clock_labels()
        self._setup_js8_rx_timer()
        self._update_suspend_state()
        if self._poll_timer:
            self._poll_timer.start()

    # ---------------- UI ---------------- #

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Header with clocks
        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>JS8Call Net Control</h3>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        layout.addLayout(header)

        # Role + Net Name + refresh
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("Role:"))
        self.role_combo = QComboBox()
        self.role_combo.addItems(["NCS", "ANCS"])
        top_row.addWidget(self.role_combo)

        top_row.addSpacing(20)
        top_row.addWidget(QLabel("Net Name:"))
        self.net_name_edit = QLineEdit()
        self.net_name_edit.setPlaceholderText("Type net name (auto-complete from schedule)...")
        top_row.addWidget(self.net_name_edit, stretch=1)

        top_row.addSpacing(20)
        top_row.addWidget(QLabel("Refresh (sec):"))
        self.refresh_spin = QSpinBox()
        self.refresh_spin.setRange(5, 300)
        self.refresh_spin.setValue(15)
        top_row.addWidget(self.refresh_spin)

        top_row.addStretch()
        layout.addLayout(top_row)

        # Panels for check-ins
        panels_row = QHBoxLayout()

        # Initial check-ins
        left_col = QVBoxLayout()
        left_col.addWidget(QLabel("<b>Initial Check-ins</b>"))
        self.initial_text = QTextEdit()
        left_col.addWidget(self.initial_text)
        panels_row.addLayout(left_col, stretch=1)

        # New/Late check-ins
        right_col = QVBoxLayout()
        self.new_late_label = QLabel("<b>New / Late Check-ins</b>")
        right_col.addWidget(self.new_late_label)
        self.new_late_text = QTextEdit()
        self.new_late_text.setStyleSheet("QTextEdit { border: 1px solid #cccccc; }")
        right_col.addWidget(self.new_late_text)
        panels_row.addLayout(right_col, stretch=1)

        layout.addLayout(panels_row)

        # Buttons row (in required order)
        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("Start Net")
        self.copy_initial_btn = QPushButton("Copy Initial Check-ins")
        self.copy_new_btn = QPushButton("Copy New Check-ins")
        self.merge_btn = QPushButton("Merge Check-ins")
        self.save_btn = QPushButton("Save Check-ins")
        self.end_btn = QPushButton("End Net")

        btn_row.addWidget(self.start_btn)
        btn_row.addWidget(self.copy_initial_btn)
        btn_row.addWidget(self.copy_new_btn)
        btn_row.addWidget(self.merge_btn)
        btn_row.addWidget(self.save_btn)
        btn_row.addWidget(self.end_btn)
        btn_row.addStretch()
        self.suspend_btn = QPushButton("QSY/Suspend")
        btn_row.addWidget(self.suspend_btn)

        layout.addLayout(btn_row)

        # Signals
        self.start_btn.clicked.connect(self._start_net)
        self.copy_initial_btn.clicked.connect(self._copy_initial_checkins)
        self.copy_new_btn.clicked.connect(self._copy_new_checkins)
        self.merge_btn.clicked.connect(self._merge_checkins)
        self.save_btn.clicked.connect(self._save_checkins)
        self.end_btn.clicked.connect(self._end_net)
        self.refresh_spin.valueChanged.connect(self._update_timer_interval)
        self.suspend_btn.clicked.connect(self._on_suspend_clicked)

        # Clock timer
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._update_clock_labels)
        self._clock_timer.start(1000)

    # ---------------- SETTINGS & TIMER ---------------- #

    def _load_settings(self):
        data = self.settings.all()
        self.auto_query_msg_id = bool(data.get("js8_auto_query_msg_id", False))

        # Net name autocomplete from net_schedule
        net_sched = data.get("net_schedule", [])
        net_names = sorted(
            {row.get("net_name", "") for row in net_sched if isinstance(row, dict) and row.get("net_name")}
        )
        if net_names:
            completer = QCompleter(net_names, self)
            completer.setCaseSensitivity(Qt.CaseInsensitive)
            self.net_name_edit.setCompleter(completer)

        # DIRECTED.TXT path
        directed_path = data.get("js8_directed_path", "")
        if directed_path:
            p = Path(directed_path)
            if p.exists() and p.is_file():
                self._directed_path = p
                try:
                    self._startup_directed_size = p.stat().st_size
                    self._last_directed_size = self._startup_directed_size
                except Exception:
                    self._startup_directed_size = 0
            else:
                self._directed_path = None
                log.warning("JS8CallNetControl: js8_directed_path not found: %s", directed_path)
        else:
            self._directed_path = None

        # Refresh interval
        refresh = int(data.get("js8_refresh_sec", 15) or 15)
        self.refresh_spin.setValue(refresh)

    def _save_refresh_setting(self):
        try:
            self.settings.set("js8_refresh_sec", int(self.refresh_spin.value()))
            if hasattr(self.settings, "save"):
                self.settings.save()
            elif hasattr(self.settings, "write"):
                self.settings.write()
        except AttributeError:
            # Fallback if set()/save() not implemented
            data = self.settings.all()
            data["js8_refresh_sec"] = int(self.refresh_spin.value())
            if hasattr(self.settings, "_data"):
                self.settings._data = data  # type: ignore[attr-defined]
            if hasattr(self.settings, "write"):
                self.settings.write()

    def _setup_timer(self):
        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._poll_directed_file)
        self._update_timer_interval()

    def _setup_js8_rx_timer(self):
        self._js8_rx_timer = QTimer(self)
        self._js8_rx_timer.setInterval(1000)
        self._js8_rx_timer.timeout.connect(self._poll_js8_rx_queue)
        self._js8_rx_timer.start()

    def _update_timer_interval(self):
        if self._poll_timer:
            self._poll_timer.setInterval(self.refresh_spin.value() * 1000)
        self._save_refresh_setting()

    # ---------------- CLOCK LABELS ---------------- #

    def _ui_tz_abbr(self, tz_name: str, fallback: str) -> str:
        mapping = {
            "UTC": "UTC",
            "America/New_York": "ET",
            "America/Chicago": "CT",
            "America/Denver": "MT",
            "America/Los_Angeles": "PT",
        }
        return mapping.get(tz_name, fallback)

    def _update_clock_labels(self):
        """
        UTC from system clock; local time derived via Settings timezone + get_timezone(),
        with a UI label like ET / CT / MT / PT / UTC.
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a")
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))

        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        fallback = now_local.tzname() or tz_name
        abbr = self._ui_tz_abbr(tz_name, fallback)

        local_day = now_local.strftime("%a")
        self.local_label.setText(
            now_local.strftime(f"<b>Local ({local_day}):</b> %y%m%d %H:%M:%S {abbr}")
        )
        self._update_suspend_state()

    # --------- Suspend (shared across tabs) --------- #

    def _get_suspend_until(self) -> Optional[datetime.datetime]:
        try:
            ts = float(self.settings.get("schedule_suspend_until", 0) or 0)
            if ts > 0:
                return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        except Exception:
            return None
        return None

    def _set_suspend_until(self, dt: Optional[datetime.datetime]) -> None:
        try:
            if hasattr(self.settings, "set"):
                self.settings.set("schedule_suspend_until", dt.timestamp() if dt else 0)
        except Exception:
            pass

    def _suspend_active(self) -> bool:
        dt = self._get_suspend_until()
        return dt is not None and datetime.datetime.now(datetime.timezone.utc) < dt

    def _set_suspend_button(self, active: bool):
        if active:
            self.suspend_btn.setText("Schedule Suspended for 30 Minutes")
            self.suspend_btn.setStyleSheet("QPushButton { background-color: #2196F3; color: white; }")
        else:
            self.suspend_btn.setText("QSY/Suspend")
            self.suspend_btn.setStyleSheet("QPushButton { background-color: gold; color: black; }")

    def _update_suspend_state(self):
        dt = self._get_suspend_until()
        if dt and datetime.datetime.now(datetime.timezone.utc) < dt:
            self._set_suspend_button(True)
        else:
            if dt:
                self._set_suspend_until(None)
            self._set_suspend_button(False)

    def _on_suspend_clicked(self):
        if self._suspend_active():
            self._set_suspend_until(None)
            self._set_suspend_button(False)
            QMessageBox.information(self, "Scheduling", "Scheduling resumed.")
        else:
            new_until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)
            self._set_suspend_until(new_until)
            self._set_suspend_button(True)
            QMessageBox.information(self, "Schedule Suspended", "Scheduling suspended for 30 minutes.")

    def _refresh_operator_history_views(self) -> None:
        try:
            parent = self.parent()
            if parent and hasattr(parent, "refresh_operator_history_views"):
                parent.refresh_operator_history_views()
        except Exception:
            pass

    # ---------------- START / END NET ---------------- #

    def _validate_before_start(self) -> bool:
        if not self.net_name_edit.text().strip():
            # Try auto-prefill once if empty
            self._auto_prefill_net_name()
        if not self.net_name_edit.text().strip():
            QMessageBox.warning(self, "Missing Net Name", "Enter Net Name before starting the net.")
            return False

        cs = self._my_callsign()
        if not cs:
            QMessageBox.warning(self, "Missing Callsign", "Configure your callsign in the Settings tab.")
            return False
        if not self._directed_path:
            QMessageBox.warning(
                self,
                "DIRECTED.TXT Not Configured",
                "JS8Call DIRECTED.TXT path is not configured or does not exist.\n"
                "Set it in the Settings tab.",
            )
            return False
        return True

    def _start_net(self):
        if not self._validate_before_start():
            return

        # Reset buffers
        self.initial_text.clear()
        self.new_late_text.clear()
        self._initial_calls.clear()
        self._late_calls.clear()
        self._all_calls_seen.clear()
        self._queried_msg_ids.clear()
        self._initial_phase = True

        self._net_in_progress = True
        self._net_start_utc = datetime.datetime.utcnow().isoformat(timespec="seconds")
        self._net_end_utc = None

        # Track file size so we only read new lines
        try:
            if self._directed_path:
                self._last_directed_size = self._directed_path.stat().st_size
                all_path = self._directed_path.parent / "ALL.TXT"
                self._last_all_size = all_path.stat().st_size if all_path.exists() else 0
        except Exception:
            self._last_directed_size = 0
            self._last_all_size = 0
        self._last_query_tx_ts = 0.0

        if self._poll_timer:
            self._poll_timer.start()

        log.info("JS8Call net started: %s (%s)", self.net_name_edit.text().strip(), self.role_combo.currentText())
        self._refresh_operator_history_views()

    def _end_net(self):
        if not self._net_in_progress:
            log.info("JS8Call End Net clicked but net_in_progress flag not set; writing log from current state.")

        if self._poll_timer:
            self._poll_timer.stop()

        self._net_end_utc = datetime.datetime.utcnow().isoformat(timespec="seconds")

        # Write the net log file from the current panels
        self._write_net_log_file()

        self._net_in_progress = False
        QMessageBox.information(self, "Net Ended", "JS8Call net ended and log saved.")

    # ---------------- AUTO-PREFILL NET NAME ---------------- #

    def _auto_prefill_net_name(self):
        """
        Prefill net_name_edit from net_schedule if:
          - net is NOT in progress
          - net_name_edit is currently empty
          - there's a net on current UTC day whose (start_utc - early_checkin)
            is within the next 20 minutes.
        """
        if self._net_in_progress:
            return
        if self.net_name_edit.text().strip():
            return

        data = self.settings.all()
        net_sched = data.get("net_schedule", [])
        if not isinstance(net_sched, list):
            return

        now_utc = datetime.datetime.utcnow()
        day_name = now_utc.strftime("%A")
        now_min = now_utc.hour * 60 + now_utc.minute

        best_row = None
        best_delta = 9999

        for row in net_sched:
            try:
                if row.get("day_utc") != day_name:
                    continue
                smin = self._parse_hhmm(row.get("start_utc", ""))
                if smin is None:
                    continue
                early = int(row.get("early_checkin", "0") or 0)
                s_eff = max(0, smin - early)
                delta = s_eff - now_min
                if 0 <= delta <= 20 and delta < best_delta:
                    best_row = row
                    best_delta = delta
            except Exception:
                continue

        if best_row:
            nn = (best_row.get("net_name") or "").strip()
            if nn and not self.net_name_edit.text().strip():
                self.net_name_edit.setText(nn)

    # ---------------- POLLING DIRECTED.TXT ---------------- #

    def _poll_directed_file(self):
        if not self._directed_path:
            return
        if not self._net_in_progress and not self.auto_query_msg_id:
            return

        log.info("JS8CallNetControl: polling DIRECTED/ALL (net_in_progress=%s)", self._net_in_progress)
        # First, scan ALL.TXT for recent QUERY MSG transmissions to gate auto-queries
        self._poll_all_for_query_tx()
        log.info("JS8CallNetControl: last query TX ts=%s", self._last_query_tx_ts)

        try:
            size_now = self._directed_path.stat().st_size
        except Exception as e:
            log.error("JS8CallNetControl: stat DIRECTED.TXT failed: %s", e)
            return

        if size_now < self._last_directed_size:
            # File truncated or rotated; re-read from start
            self._last_directed_size = 0

        new_calls: List[str] = []

        try:
            with self._directed_path.open("r", encoding="utf-8", errors="ignore") as f:
                if self._last_directed_size > 0:
                    f.seek(self._last_directed_size)
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if not self._line_ts_after_start(line):
                        continue
                    calls = self._extract_callsigns_from_line(line)
                    self._maybe_record_inbound_trigger(line, calls)
                    msg_ids = self._extract_message_ids(line)
                    # If multiple stations reported YES MSG <id>, query each
                    if msg_ids and calls:
                        if not self._saw_recent_query_tx():
                            log.info("JS8CallNetControl: skipping YES MSG in DIRECTED (no recent query TX)")
                        else:
                            for c in calls:
                                for mid in msg_ids:
                                    log.debug("JS8CallNetControl: queueing auto-query %s from DIRECTED line (call=%s)", mid, c)
                                    self._queue_auto_query(c, mid)
                    call_primary = calls[0] if calls else ""
                    if not call_primary:
                        continue
                    if call_primary in self._all_calls_seen:
                        continue
                    self._all_calls_seen.add(call_primary)
                    new_calls.append(call_primary)

                    # Check for completion markers to advance queue
                    self._process_message_completion(line)

                self._last_directed_size = f.tell()
        except Exception as e:
            log.error("JS8CallNetControl: failed reading DIRECTED.TXT: %s", e)
            return

        if not new_calls:
            return

        # Do not populate panels unless a net is in progress (auto-query can still run without one)
        if not self._net_in_progress:
            return

        # Append new calls to appropriate panel
        for call in new_calls:
            if self._initial_phase:
                if call not in self._initial_calls:
                    self._initial_calls.add(call)
                    self._append_line_to_text(self.initial_text, call)
            else:
                if call not in self._late_calls:
                    self._late_calls.add(call)
                    self._append_line_to_text(self.new_late_text, call)

        # Flash border on new/late panel only when timer adds new calls
        if not self._initial_phase and new_calls:
            self._flash_new_late_border()

    def _poll_all_for_query_tx(self):
        """
        Scan ALL.TXT for outgoing QUERY MSG(S) transmissions to enable auto-query from DIRECTED.
        """
        if not self._directed_path:
            return
        all_path = self._directed_path.parent / "ALL.TXT"
        if not all_path.exists():
            return
        try:
            size_now = all_path.stat().st_size
        except Exception as e:
            log.error("JS8CallNetControl: stat ALL.TXT failed: %s", e)
            return
        if size_now < self._last_all_size:
            self._last_all_size = 0
        try:
            with all_path.open("r", encoding="utf-8", errors="ignore") as f:
                if self._last_all_size > 0:
                    f.seek(self._last_all_size)
                for line in f:
                    if "Transmitting" not in line:
                        continue
                    if not self._line_ts_after_start(line):
                        continue
                    up = line.upper()
                    if "QUERY MSG" in up:
                        self._last_query_tx_ts = time.time()
                        log.info("JS8CallNetControl: detected outgoing QUERY MSG in ALL.TXT: %s", line.strip())
                    # Track outbound direct transmissions to add untrusted operators
                    self._maybe_register_outgoing_call(line)
                self._last_all_size = f.tell()
        except Exception as e:
            log.error("JS8CallNetControl: failed reading ALL.TXT: %s", e)
            return

    def _saw_recent_query_tx(self, window_sec: int = 600) -> bool:
        """
        Return True if a QUERY MSG(S) transmit was seen in ALL.TXT within the last window.
        """
        if self._last_query_tx_ts <= 0:
            return False
        return (time.time() - self._last_query_tx_ts) <= window_sec

    def _line_ts_after_start(self, line: str) -> bool:
        """
        Return True if the line begins with a timestamp after app start.
        """
        try:
            ts_str = line[:19]
            ts = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
            log.debug("JS8CallNetControl: parsed line ts=%s (app_start=%s)", ts, self._app_start_ts)
            return ts > self._app_start_ts
        except Exception:
            return False

    def _append_line_to_text(self, text_widget: QTextEdit, line: str):
        if text_widget.toPlainText().strip():
            text_widget.append(line)
        else:
            text_widget.setPlainText(line)

    def _flash_new_late_border(self):
        # subtle green border flash
        self.new_late_text.setStyleSheet("QTextEdit { border: 2px solid #4CAF50; }")
        QTimer.singleShot(
            1000,
            lambda: self.new_late_text.setStyleSheet("QTextEdit { border: 1px solid #cccccc; }"),
        )

    # ---------------- COPY / MERGE / SAVE ---------------- #

    def _copy_initial_checkins(self):
        # After first click, new calls go into New/Late
        self._initial_phase = False
        calls = self._dedupe_calls_from_text(self.initial_text.toPlainText())
        summary = self._build_short_code_summary(calls)
        QApplication.clipboard().setText(summary)
        QMessageBox.information(self, "Copied", "Initial check-in short codes copied to clipboard.")

    def _copy_new_checkins(self):
        calls = self._dedupe_calls_from_text(self.new_late_text.toPlainText())
        summary = self._build_short_code_summary(calls)
        QApplication.clipboard().setText(summary)
        QMessageBox.information(self, "Copied", "New/late check-in short codes copied to clipboard.")

    def _merge_checkins(self):
        """
        Merge New/Late into Initial panel, then clear New/Late panel.
        After this, we continue appending new timer-detected calls
        only into New/Late (per your instructions).
        """
        new_calls = self._dedupe_calls_from_text(self.new_late_text.toPlainText())
        if not new_calls:
            self.new_late_text.clear()
            self._late_calls.clear()
            return

        # Merge into initial set and panel
        for call in new_calls:
            if call not in self._initial_calls:
                self._initial_calls.add(call)
                self._append_line_to_text(self.initial_text, call)

        # Clear new/late panel and set
        self.new_late_text.clear()
        self._late_calls.clear()

        QMessageBox.information(self, "Merged", "New / Late check-ins merged into Initial list.")

    def _save_checkins(self):
        """
        No external log files are written here; file logging happens when the net ends.
        This button is kept for parity with FLDigi (and to future-proof if you want
        mid-net file saves). For now it just acknowledges the check-ins are captured.
        """
        QMessageBox.information(self, "Saved", "Check-ins are ready and will be logged when the net ends.")
        self._refresh_operator_history_views()

    # ---------------- LOG FILE WRITING ---------------- #

    def _write_net_log_file(self):
        """
        Write net log into net_logs directory under DIRECTED.TXT's directory.

        Filename: netname-ROLE-YYYYMMDD.txt
        Header:
          # Net: ...
          # Role: ...
          # Start UTC: ...
          # End UTC: ...
          # Band: ... (if found)
        Body:
          one full callsign per line.
        """
        if not self._directed_path:
            return

        net_name = self.net_name_edit.text().strip()
        role = self.role_combo.currentText().strip().upper()
        if not net_name:
            net_name = "UNKNOWN_NET"

        date_str = datetime.datetime.utcnow().strftime("%Y%m%d")

        base_dir = self._directed_path.parent
        net_logs_dir = base_dir / "net_logs"
        try:
            net_logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            log.error("JS8CallNetControl: unable to create net_logs directory: %s", e)
            return

        safe_net = "".join(c for c in net_name if c.isalnum() or c in ("_", "-", " "))
        safe_net = safe_net.replace(" ", "_") or "net"
        filename = f"{safe_net}-{role}-{date_str}.txt"
        path = net_logs_dir / filename

        # Collect all full callsigns from both panels
        all_calls = self._dedupe_calls_from_text(
            self.initial_text.toPlainText() + "\n" + self.new_late_text.toPlainText()
        )

        # Try to find band from net_schedule
        band = self._lookup_band_for_net(net_name)

        lines: List[str] = []
        lines.append(f"# Net: {net_name}")
        lines.append(f"# Role: {role}")
        if self._net_start_utc:
            lines.append(f"# Start UTC: {self._net_start_utc}")
        if self._net_end_utc:
            lines.append(f"# End UTC:   {self._net_end_utc}")
        if band:
            lines.append(f"# Band: {band}")
        lines.append("#")
        for cs in all_calls:
            lines.append(cs)

        try:
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            log.info("JS8Call net log written to %s", path)
        except Exception as e:
            log.error("JS8CallNetControl: failed to write net log file %s: %s", path, e)

    def _lookup_band_for_net(self, net_name: str) -> str:
        """
        Attempts to find a band in net_schedule matching this net_name and current UTC day/time.
        """
        if not net_name:
            return ""

        data = self.settings.all()
        net_sched = data.get("net_schedule", [])
        if not isinstance(net_sched, list):
            return ""

        now_utc = datetime.datetime.utcnow()
        current_day_name = now_utc.strftime("%A")
        now_min = now_utc.hour * 60 + now_utc.minute

        for row in net_sched:
            try:
                if (row.get("net_name", "") or "").strip().lower() != net_name.strip().lower():
                    continue
                if row.get("day_utc") != current_day_name:
                    continue
                smin = self._parse_hhmm(row.get("start_utc", ""))
                emin = self._parse_hhmm(row.get("end_utc", ""))
                if smin is None or emin is None:
                    continue
                if smin <= now_min <= emin:
                    return (row.get("band") or "").strip()
            except Exception:
                continue

        return ""

    # ---------------- PARSING & UTILS ---------------- #

    def _parse_hhmm(self, text: str) -> int | None:
        text = (text or "").strip()
        if not text:
            return None
        try:
            h, m = text.split(":")
            h = int(h)
            m = int(m)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return h * 60 + m
        except Exception:
            return None
        return None

    def _my_callsign(self) -> str:
        return (
            (self.settings.get("operator_callsign", "") or self.settings.get("callsign", "") or "")
            .strip()
            .upper()
        )

    def _extract_callsigns_from_line(self, line: str) -> List[str]:
        """
        JS8Check-in line examples:

          XY1245: ... F!103 ...
          N1MAG: some text
          A1BC:  something

        Rules:
          - If line contains 'F!103', treat first token up to ':' as callsign.
          - Else, if any token ends with ':', treat that token (without ':')
            as the remote callsign, as long as it's not our own callsign.
        """
        line = line.strip()
        if not line:
            return []

        mycall = self._my_callsign()

        # Try F!103 pattern first
        if "F!103" in line:
            first = line.split()[0]
            if ":" in first:
                first = first.split(":", 1)[0]
            return [first.upper()]

        # Otherwise, look for token ending with ':'
        hits: List[str] = []
        parts = line.split()
        for tok in parts:
            if tok.endswith(":"):
                cs = tok[:-1].upper()
                if cs and cs != mycall:
                    hits.append(cs)
        return hits

    def _extract_message_ids(self, line: str) -> List[str]:
        """
        Look for all patterns like 'YES MSG 123' in a JS8Call line and
        return numeric message IDs as strings.
        """
        return re.findall(r"\bYES\s+MSG\s+(\d+)", line, flags=re.IGNORECASE)

    def _get_js8_client(self):
        if self._js8_client:
            return self._js8_client
        if js8net is None:
            log.warning("JS8CallNetControl: js8net not available")
            return None
        # Do not spawn JS8Call; only attach if it is already running
        try:
            running = False
            for proc in psutil.process_iter(attrs=["name", "exe"]):
                try:
                    name = (proc.info.get("name") or "").lower()
                    exe = (proc.info.get("exe") or "").lower()
                    if "js8call" in name or "js8call" in exe:
                        running = True
                        break
                except Exception:
                    continue
            if not running:
                log.info("JS8CallNetControl: JS8Call not running; skipping js8net attach.")
                return None
        except Exception:
            return None
        try:
            port = int(self.settings.get("js8_port", 2442) or 2442)
        except Exception:
            port = 2442
        try:
            js8net.start_net("127.0.0.1", port)
            self._js8_client = js8net
            return js8net
        except BaseException as e:
            log.error("JS8CallNetControl: failed to start js8net: %s", e)
            return None

    def _maybe_record_inbound_trigger(self, line: str, calls: List[str]) -> None:
        """
        Track the group that caused our potential autoreply so we can tag outbound inserts.
        If message was to our callsign, store group = our callsign.
        If message was to a configured @GROUP, store that group.
        """
        if not calls:
            return
        mycall = self._my_callsign()
        upper = line.upper()
        to_me = mycall and mycall in upper
        groups_cfg = [g.strip().upper() for g in (self.settings.get("primary_js8_groups", []) or []) if g]
        hit_group = None
        for g in groups_cfg:
            if f"@{g}" in upper:
                hit_group = g
                break
        group_val = None
        if hit_group:
            group_val = hit_group
        elif to_me:
            group_val = mycall
        if not group_val:
            return
        origin = calls[0]
        self._last_inbound_triggers[origin] = (group_val, time.time())
        # prune stale entries (older than 15 min)
        now = time.time()
        stale = [k for k, (_, ts) in self._last_inbound_triggers.items() if now - ts > 900]
        for k in stale:
            self._last_inbound_triggers.pop(k, None)

    def _maybe_register_outgoing_call(self, line: str) -> None:
        """
        For any outgoing transmission to a callsign, add to operator_checkins as untrusted
        if not already present. Use group from the triggering inbound if available.
        """
        # Parse timestamp
        ts = None
        try:
            ts = datetime.datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
        except Exception:
            ts = datetime.datetime.now(datetime.timezone.utc)
        # Extract message after "JS8:"
        if "JS8:" not in line:
            return
        try:
            msg_part = line.split("JS8:", 1)[1].strip()
        except Exception:
            return
        tokens = msg_part.split()
        if not tokens:
            return
        # Skip @GROUP-only transmissions
        if tokens[0].startswith("@"):
            return
        dest_call = None
        if tokens[0].endswith(":") and len(tokens) > 1:
            dest_call = tokens[1].strip().strip(":").upper()
        else:
            dest_call = tokens[0].strip().strip(":").upper()
        if not dest_call:
            return
        # Determine group from last trigger if recent
        group_val = ""
        now = time.time()
        trig = self._last_inbound_triggers.get(dest_call)
        if trig and now - trig[1] <= 900:
            group_val = trig[0]
        elif self._my_callsign():
            group_val = self._my_callsign()
        self._maybe_insert_untrusted(dest_call, ts, group_val)

    def _maybe_insert_untrusted(self, callsign: str, last_seen: datetime.datetime, group_val: str) -> None:
        cs = (callsign or "").strip().upper()
        if not cs:
            return
        try:
            root = Path(__file__).resolve().parents[2]
            db_path = root / "config" / "freqinout_nets.db"
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_checkins (
                    callsign TEXT PRIMARY KEY,
                    name TEXT,
                    state TEXT,
                    grid TEXT,
                    group1 TEXT,
                    group2 TEXT,
                    group3 TEXT,
                    group_role TEXT,
                    first_seen_utc TEXT,
                    last_seen_utc TEXT,
                    checkin_count INTEGER,
                    groups_json TEXT,
                    trusted INTEGER
                )
                """
            )
            # check existing
            cur.execute("SELECT trusted FROM operator_checkins WHERE callsign=?", (cs,))
            row = cur.fetchone()
            ts_str = last_seen.astimezone(datetime.timezone.utc).isoformat()
            groups_json = json.dumps([group_val]) if group_val else None
            if row is None:
                cur.execute(
                    """
                    INSERT INTO operator_checkins (
                        callsign, name, state, grid, group1, group2, group3, group_role,
                        first_seen_utc, last_seen_utc, checkin_count, groups_json, trusted
                    ) VALUES (?, '', '', '', ?, '', '', '', ?, ?, 0, ?, 0)
                    """,
                    (cs, group_val, ts_str, ts_str, groups_json),
                )
            else:
                trusted = int(row[0] or 0)
                if trusted == 0:
                    cur.execute(
                        """
                        UPDATE operator_checkins
                        SET last_seen_utc=?, group1=COALESCE(NULLIF(group1,''), ?), groups_json=COALESCE(groups_json, ?)
                        WHERE callsign=?
                        """,
                        (ts_str, group_val, groups_json, cs),
                    )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("JS8CallNetControl: failed to upsert untrusted operator %s: %s", callsign, e)
    def _queue_auto_query(self, call: str, msg_id: str, snr: float | None = None) -> None:
        """
        Queue a query for MSG ID, and process one at a time when enabled.
        """
        if not self.auto_query_msg_id:
            return
        key = f"{call}:{msg_id}"
        if key in self._queried_msg_ids:
            return
        try:
            snr_val = float(snr) if snr is not None else None
        except Exception:
            snr_val = None
        self._pending_queries.append((snr_val, call, msg_id))
        log.debug("JS8CallNetControl: queued auto-query call=%s id=%s (snr=%s) pending=%d", call, msg_id, snr_val, len(self._pending_queries))
        self._maybe_process_next_query()

    def _maybe_process_next_query(self) -> None:
        if self._waiting_for_completion:
            return
        if not self._pending_queries:
            return
        # Avoid querying while RX just occurred (idle gap)
        if time.time() - self._last_rx_ts < 2.0:
            return
        client = self._get_js8_client()
        if client is None:
            return
        # Prefer weakest SNR first (more negative first), unknowns last
        self._pending_queries.sort(key=lambda t: (999 if t[0] is None else t[0]))
        snr_val, call, msg_id = self._pending_queries.pop(0)
        log.debug("JS8CallNetControl: processing auto-query call=%s id=%s (snr=%s) remaining=%d", call, msg_id, snr_val, len(self._pending_queries))
        key = f"{call}:{msg_id}"
        if key in self._queried_msg_ids:
            # skip duplicates
            self._maybe_process_next_query()
            return
        try:
            if hasattr(client, "query_message_id"):
                log.info("JS8CallNetControl: sending query_message_id(%s, %s) to JS8Call", call, msg_id)
                client.query_message_id(call, str(msg_id))  # type: ignore[attr-defined]
                self._queried_msg_ids.add(key)
                self._waiting_for_completion = True
                self._current_query = (call, msg_id)
                log.info("JS8CallNetControl: auto-queried MSG ID %s from %s via query_message_id", msg_id, call)
            elif hasattr(client, "send_message"):
                # Fallback for js8net: send explicit QUERY MSG <id>
                log.info("JS8CallNetControl: sending QUERY MSG %s to %s via send_message", msg_id, call)
                client.send_message(f"{call}: QUERY MSG {msg_id}")  # type: ignore[attr-defined]
                self._queried_msg_ids.add(key)
                self._waiting_for_completion = True
                self._current_query = (call, msg_id)
                log.info("JS8CallNetControl: auto-queried MSG ID %s from %s via send_message fallback", msg_id, call)
            else:
                log.debug("JS8CallNetControl: js8net does not support query_message_id; skipping auto-query.")
        except Exception as e:
            log.error("JS8CallNetControl: auto query failed for %s/%s: %s", call, msg_id, e)
            self._current_query = None
            self._waiting_for_completion = False
            # Try next one to avoid stall
            self._maybe_process_next_query()

    def _poll_js8_rx_queue(self) -> None:
        if js8net is None:
            return
        client = self._get_js8_client()
        if client is None or not hasattr(js8net, "rx_queue"):
            return
        try:
            while True:
                msg = js8net.rx_queue.get_nowait()
                now_ts = time.time()
                self._last_rx_ts = now_ts
                self._grid_last_rx_ts = now_ts
                try:
                    p = msg.get("params", {}) if isinstance(msg, dict) else {}
                    txt = str(p.get("TEXT") or "").upper()
                    cmd_txt = str(p.get("CMD") or "").upper()
                    extra_txt = str(p.get("EXTRA") or "").upper()
                    combined = " ".join([txt, cmd_txt, extra_txt]).strip()
                    frm = (p.get("FROM") or "").strip().upper()
                    snr_val = None
                    try:
                        snr_val = float(p.get("SNR")) if p.get("SNR") not in (None, "") else None
                    except Exception:
                        snr_val = None
                    if self.auto_query_msg_id:
                        if self._net_lockout_active():
                            log.debug("JS8CallNetControl: skipping auto-query (net lockout active)")
                        elif "YES MSG" in combined:
                            ids = re.findall(r"\b(\d+)\b", combined)
                            for mid in ids:
                                if frm:
                                    log.info("JS8CallNetControl: detected YES MSG %s from %s (snr=%s)", mid, frm, snr_val)
                                    self._queue_auto_query(frm, mid, snr=snr_val)
                    # Passive grid capture
                    grid_val = (p.get("GRID") or "").strip()
                    if grid_val and frm:
                        self._update_operator_grid(frm, grid_val, self._active_group_name())
                    else:
                        for token in txt.split():
                            if 4 <= len(token) <= 6 and token[:2].isalpha() and token[2:4].isdigit():
                                self._update_operator_grid(frm, token, self._active_group_name())
                                break
                    # Auto grid query when allowed
                    if self.auto_query_grids and not self._net_lockout_active():
                        if frm and self._operator_missing_grid(frm):
                            self._maybe_queue_grid_query(frm, snr_val, msg_params=p, text=txt)
                except Exception:
                    continue
        except queue.Empty:
            pass
        self._maybe_process_next_query()
        self._maybe_process_next_grid()

    def _process_message_completion(self, line: str) -> None:
        """
        Detect end-of-message markers before issuing next queued query.
        """
        if not self._waiting_for_completion:
            return
        if not self._is_message_complete_line(line):
            return
        self._waiting_for_completion = False
        call = self._current_query[0] if self._current_query else None
        self._current_query = None
        # After a message completes, ask that station if more messages exist
        if call:
            log.info("JS8CallNetControl: message completion detected; querying MSGS from %s", call)
            self._send_query_msgs(call)
        self._maybe_process_next_query()

    def _send_query_msgs(self, call: str) -> None:
        """
        Send QUERY MSGS to a specific station to discover additional messages.
        """
        if js8net is None:
            return
        client = self._get_js8_client()
        if client is None:
            return
        try:
            if hasattr(client, "send_message"):
                client.send_message(f"{call}: QUERY MSGS")  # type: ignore[attr-defined]
                log.info("JS8CallNetControl: queried additional messages from %s", call)
        except Exception as e:
            log.debug("JS8CallNetControl: failed sending QUERY MSGS to %s: %s", call, e)

    # ---------------- Grid helpers ---------------- #

    def _operator_missing_grid(self, callsign: str) -> bool:
        cs = (callsign or "").strip().upper()
        if not cs:
            return False
        try:
            db_path = Path(__file__).resolve().parents[2] / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return True
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT grid FROM operator_checkins WHERE callsign=?", (cs,))
            row = cur.fetchone()
            conn.close()
            if row is None:
                return True
            grid = row[0] or ""
            return grid.strip() == ""
        except Exception:
            return True

    def _update_operator_grid(self, callsign: str, grid: str, group_name: str = "") -> None:
        cs = (callsign or "").strip().upper()
        grid = (grid or "").strip().upper()
        if not cs or not grid:
            return
        try:
            db_path = Path(__file__).resolve().parents[2] / "config" / "freqinout_nets.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS operator_checkins (
                    callsign TEXT PRIMARY KEY,
                    name TEXT,
                    state TEXT,
                    grid TEXT,
                    group1 TEXT,
                    group2 TEXT,
                    group3 TEXT,
                    group_role TEXT,
                    first_seen_utc TEXT,
                    last_seen_utc TEXT,
                    last_net TEXT,
                    last_role TEXT,
                    checkin_count INTEGER DEFAULT 0,
                    groups_json TEXT,
                    trusted INTEGER DEFAULT 1
                )
                """
            )
            cur.execute("SELECT grid, group1, group2, group3, groups_json, trusted FROM operator_checkins WHERE callsign=?", (cs,))
            row = cur.fetchone()
            now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            if row is None:
                groups = [g for g in [group_name.strip()] if g]
                groups_json = json.dumps(groups) if groups else None
                cur.execute(
                    """
                    INSERT OR REPLACE INTO operator_checkins
                    (callsign, grid, group1, group2, group3, group_role, first_seen_utc, last_seen_utc, checkin_count, groups_json, trusted)
                    VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 0, ?, 1)
                    """,
                    (cs, grid, group_name or None, None, None, now_iso, now_iso, groups_json),
                )
            else:
                old_grid, g1, g2, g3, groups_json, trusted = row
                new_grid = old_grid or grid
                g_list = [g1 or "", g2 or "", g3 or ""]
                if group_name and group_name not in g_list:
                    for idx, val in enumerate(g_list):
                        if not val:
                            g_list[idx] = group_name
                            break
                try:
                    current_groups = json.loads(groups_json) if groups_json else []
                    if group_name and group_name not in current_groups:
                        current_groups.append(group_name)
                    groups_json_out = json.dumps(current_groups) if current_groups else None
                except Exception:
                    groups_json_out = groups_json
                cur.execute(
                    """
                    UPDATE operator_checkins
                    SET grid=?, group1=?, group2=?, group3=?, last_seen_utc=?, groups_json=?, trusted=COALESCE(trusted, ?)
                    WHERE callsign=?
                    """,
                    (new_grid, g_list[0] or None, g_list[1] or None, g_list[2] or None, now_iso, groups_json_out, trusted if trusted is not None else 1, cs),
                )
            conn.commit()
        except Exception as e:
            log.debug("JS8CallNetControl: failed to update operator grid for %s: %s", callsign, e)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _active_group_name(self) -> str:
        entry = self._active_schedule()
        if not entry:
            return ""
        return (entry.get("group_name") or "").strip()

    def _maybe_queue_grid_query(self, callsign: str, snr: Optional[float], msg_params: Dict, text: str) -> None:
        call = (callsign or "").strip().upper()
        if not call:
            return
        # Require active schedule and group match: a AND (b OR c)
        active = self._active_schedule()
        if not active:
            return
        sched_group = (active.get("group_name") or "").strip().upper()
        configured_groups = [g.strip().upper() for g in (self.settings.get("primary_js8_groups", []) or []) if g]
        incoming_group = ""
        for tok in text.split():
            if tok.startswith("@") and len(tok) > 1:
                incoming_group = tok[1:].upper()
                break
        if not configured_groups:
            group_ok = True
        else:
            group_ok = sched_group in configured_groups or incoming_group in configured_groups
        if not group_ok:
            return
        # Enqueue if not already queued
        for _, queued_call in self._pending_grid_queries:
            if queued_call == call:
                return
        self._pending_grid_queries.append((snr, call))

    def _maybe_process_next_grid(self) -> None:
        if not self._pending_grid_queries:
            return
        if time.time() - self._grid_last_rx_ts < 2.0:
            return
        if self._net_lockout_active():
            return
        client = self._get_js8_client()
        if client is None:
            return
        # Weakest SNR first
        self._pending_grid_queries.sort(key=lambda t: (999 if t[0] is None else t[0]))
        snr_val, call = self._pending_grid_queries.pop(0)
        try:
            if hasattr(client, "send_message"):
                client.send_message(f"{call}: GRID?")  # type: ignore[attr-defined]
                log.info("JS8CallNetControl: auto grid query to %s", call)
                self._grid_waiting = True
        except Exception as e:
            log.debug("JS8CallNetControl: failed GRID? to %s: %s", call, e)

    def _is_message_complete_line(self, line: str) -> bool:
        """
        Heuristic: treat lines containing the JS8Call end-of-message marker
        (diamond '♢') as completion markers.
        """
        txt = line.strip()
        if not txt:
            return False
        if "♢" in txt:
            return True
        return False

    # ---------------- Schedule helpers ---------------- #

    def _parse_hhmm_to_minutes(self, hhmm: str) -> Optional[int]:
        txt = (hhmm or "").strip()
        if not txt:
            return None
        try:
            hh, mm = txt.split(":")
            hh_i = int(hh)
            mm_i = int(mm)
            if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
                return hh_i * 60 + mm_i
        except Exception:
            return None
        return None

    def _load_net_rows(self) -> List[Dict]:
        data = []
        try:
            db_path = Path(__file__).resolve().parents[2] / "config" / "freqinout_nets.db"
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute(
                    "SELECT day_utc, frequency, start_utc, end_utc, early_checkin, group_name FROM net_schedule_tab"
                )
                for row in cur.fetchall():
                    data.append(
                        {
                            "day_utc": row[0] or "",
                            "frequency": row[1] or "",
                            "start_utc": row[2] or "",
                            "end_utc": row[3] or "",
                            "early_checkin": int(row[4] or 0),
                            "group_name": row[5] or "",
                        }
                    )
                conn.close()
        except Exception:
            pass
        if not data:
            try:
                raw = self.settings.get("net_schedule", [])
                if isinstance(raw, list):
                    data = [
                        {
                            "day_utc": r.get("day_utc", ""),
                            "frequency": r.get("frequency", ""),
                            "start_utc": r.get("start_utc", ""),
                            "end_utc": r.get("end_utc", ""),
                            "early_checkin": int(r.get("early_checkin", 0) or 0),
                            "group_name": r.get("group_name", ""),
                        }
                        for r in raw
                        if isinstance(r, dict)
                    ]
            except Exception:
                data = []
        return data

    def _load_daily_rows(self) -> List[Dict]:
        data = []
        try:
            db_path = Path(__file__).resolve().parents[2] / "config" / "freqinout.db"
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT day_utc, frequency, start_utc, end_utc, group_name FROM daily_schedule_tab")
                for row in cur.fetchall():
                    data.append(
                        {
                            "day_utc": row[0] or "ALL",
                            "frequency": row[1] or "",
                            "start_utc": row[2] or "",
                            "end_utc": row[3] or "",
                            "group_name": row[4] or "",
                        }
                    )
                conn.close()
        except Exception:
            pass
        if not data:
            try:
                raw = self.settings.get("daily_schedule", [])
                if isinstance(raw, list):
                    data = [
                        {
                            "day_utc": r.get("day_utc", "ALL"),
                            "frequency": r.get("frequency", ""),
                            "start_utc": r.get("start_utc", ""),
                            "end_utc": r.get("end_utc", ""),
                            "group_name": r.get("group_name", ""),
                        }
                        for r in raw
                        if isinstance(r, dict)
                    ]
            except Exception:
                data = []
        return data

    def _day_matches(self, entry_day: str, now_day: str) -> bool:
        d = (entry_day or "ALL").strip().upper()
        if d == "ALL":
            return True
        return d.upper() == now_day.upper()

    def _is_in_window(self, entry, now: datetime.datetime, allow_early: bool = False) -> bool:
        day = entry.get("day_utc", "ALL")
        start_txt = entry.get("start_utc", "")
        end_txt = entry.get("end_utc", "")
        early = int(entry.get("early_checkin", 0) or 0) if allow_early else 0
        start_m = self._parse_hhmm_to_minutes(start_txt)
        end_m = self._parse_hhmm_to_minutes(end_txt)
        if start_m is None or end_m is None:
            return False
        start_m = max(0, start_m - early)
        now_m = now.hour * 60 + now.minute
        # Overnight handling
        if start_m <= end_m:
            return self._day_matches(day, now.strftime("%A")) and start_m <= now_m <= end_m
        else:
            # window crosses midnight
            today_match = self._day_matches(day, now.strftime("%A")) and now_m >= start_m
            prev_day = (now - datetime.timedelta(days=1)).strftime("%A")
            overnight_match = self._day_matches(day, prev_day) and now_m <= end_m
            return today_match or overnight_match

    def _active_schedule(self) -> Optional[Dict]:
        now = datetime.datetime.now(datetime.timezone.utc)
        # Prefer net schedule windows (respect early)
        for row in self._load_net_rows():
            if self._is_in_window(row, now, allow_early=True):
                return row
        for row in self._load_daily_rows():
            if self._is_in_window(row, now, allow_early=False):
                return row
        return None

    def _next_net_lockout(self) -> Optional[datetime.datetime]:
        """
        Return the UTC datetime when the next net window starts (start - early).
        """
        rows = self._load_net_rows()
        if not rows:
            return None
        now = datetime.datetime.now(datetime.timezone.utc)
        now_day = now.strftime("%A")
        candidates: List[datetime.datetime] = []
        for row in rows:
            start_m = self._parse_hhmm_to_minutes(row.get("start_utc", ""))
            end_m = self._parse_hhmm_to_minutes(row.get("end_utc", ""))
            if start_m is None or end_m is None:
                continue
            early = int(row.get("early_checkin", 0) or 0)
            window_start = max(0, start_m - early)
            for day_offset in (0, 1):
                dt = now + datetime.timedelta(days=day_offset)
                if not self._day_matches(row.get("day_utc", ""), dt.strftime("%A")):
                    continue
                cand = dt.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(
                    minutes=window_start
                )
                if cand >= now:
                    candidates.append(cand)
        if not candidates:
            return None
        return min(candidates)

    def _net_lockout_active(self) -> bool:
        """
        True if within 5 minutes of a net window start (including early check-in)
        or currently inside a net window.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        for row in self._load_net_rows():
            if self._is_in_window(row, now, allow_early=True):
                return True
        nxt = self._next_net_lockout()
        if nxt is None:
            return False
        delta = (nxt - now).total_seconds() / 60.0
        return 0 <= delta <= 5

    def _dedupe_calls_from_text(self, text: str) -> List[str]:
        calls = []
        seen = set()
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            # If line looks like full JS8 text, re-parse; otherwise treat as bare callsign
            if ":" in line or " " in line:
                call = self._extract_callsign_from_line(line)
            else:
                call = line.upper()
            if not call:
                continue
            if call not in seen:
                seen.add(call)
                calls.append(call)
        return calls

    def _build_short_code_summary(self, calls: List[str]) -> str:
        """
        Take a list of full callsigns, keep only last 3 characters of each,
        and return them as a space-delimited string.
        """
        shorts: List[str] = []
        for cs in calls:
            cs = cs.strip().upper()
            if not cs:
                continue
            if len(cs) <= 3:
                shorts.append(cs)
            else:
                shorts.append(cs[-3:])
        return " ".join(shorts)

    # ---------------- Qt events ---------------- #

    def showEvent(self, event):
        """
        When the tab is shown:
         - Reload settings (in case DIRECTED.TXT path or net schedule changed)
         - Try auto-prefill net name (if not in progress and empty)
         - Update clocks
        """
        super().showEvent(event)
        try:
            self._load_settings()
            self._auto_prefill_net_name()
            self._update_clock_labels()
        except Exception as e:
            log.error("JS8CallNetControl: showEvent failed: %s", e)
