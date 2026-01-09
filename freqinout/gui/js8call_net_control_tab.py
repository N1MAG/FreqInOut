from __future__ import annotations

import datetime
import re
import sqlite3
import time
import json
import queue
import socket
from pathlib import Path
from typing import List, Dict, Set, Optional

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QMessageBox,
    QComboBox,
    QApplication,
    QSpinBox,
    QCompleter,
    QTableWidget,
    QTableWidgetItem,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.logger import log
from freqinout.utils.timezones import get_timezone
from freqinout.gui.qsy_helper import (
    load_operating_groups as qsy_load_operating_groups,
    snapshot_operating_groups as qsy_snapshot_operating_groups,
    build_qsy_options,
    refresh_qsy_combo,
    selected_qsy_meta,
    current_scheduler_freq,
    perform_qsy,
    get_suspend_until,
    set_suspend_until,
    suspend_active,
    scheduler_enabled,
)
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

AUTO_GRID_QUIET_SECS = 90  # idle time required since last RX from a station before sending GRID?
CHECKIN_FORMS = {"F!103", "F!104"}
ANNOUNCE_FORM = "F!106"  # JS8Spotter net announcement


class JS8CallNetControlTab(QWidget):
    """
    JS8Call Net Control tab.

    Uses JS8Call's DIRECTED.TXT and net_schedule to manage JS8 nets:

    - Settings:
        * callsign, operator_name, operator_state
        * js8_directed_path: full path to JS8Call DIRECTED.TXT
        * js8_refresh_sec: poll interval (seconds)
    - Single Check-Ins table with per-call metadata (mode, SNR, DT, offset, status).

    Buttons:
        Start Net, ACK Check-ins, Set Group, Set Spotter, Group Spotter,
        Single Spotter, Save Checkins, End Net, QSY/Suspend (shared across tabs)

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

        self._all_calls_seen: Set[str] = set()
        self._queried_msg_ids: Set[str] = set()
        self._pending_queries: List[tuple[Optional[float], str, str]] = []
        self._waiting_for_completion: bool = False
        self._current_query: tuple[str, str] | None = None
        self._js8_client = None
        self.auto_query_msg_id = bool(self.settings.get("js8_auto_query_msg_id", False))
        self.auto_query_grids = bool(self.settings.get("js8_auto_query_grids", False))
        self._js8_rx_timer: QTimer | None = None
        self._last_rx_ts: float = 0.0
        self._pending_grid_queries: List[tuple[Optional[float], str]] = []
        self._grid_waiting: bool = False
        self._grid_last_rx_ts: float = 0.0
        self._last_directed_size: int = 0
        self._last_all_size: int = 0
        self._last_query_tx_ts: float = 0.0
        self._app_start_ts: float = time.time()
        self._last_tx_ts: float = 0.0
        # Track inbound triggers to map replies to groups
        self._last_inbound_triggers: Dict[str, tuple[str, float]] = {}
        self._auto_inserted_callsigns: Set[str] = set()
        self._awaiting_ack_for: Optional[str] = None
        self._call_last_rx_ts: Dict[str, float] = {}
        self._auto_query_paused_by_net = False
        self._start_btn_default_style = "QPushButton { background-color: #4CAF50; color: white; }"
        self._end_btn_default_style = "QPushButton { background-color: #F44336; color: white; }"

        # Check-in table state
        self._checkins: Dict[str, Dict] = {}
        self._checkin_rows: Dict[str, int] = {}
        self._checkins_saved: Set[str] = set()
        self._group_target: str = ""
        self._spotter_form: Optional[str] = None
        self._expected_form: Optional[str] = None
        self._status_mismatch: Dict[str, bool] = {}
        self._pending_announcements: Dict[str, float] = {}  # callsign -> ts waiting for completion
        self._recent_announcements: Dict[str, float] = {}  # callsign -> last popup ts
        self._backlog_loaded: bool = False
        self._awaiting_msg_responses: Dict[tuple[str, str], float] = {}  # (call, msg_id) -> expiry ts
        self._awaiting_grid_responses: Dict[str, float] = {}  # call -> expiry ts
        self._current_query_sent_ts: float = 0.0
        self._qsy_options: Dict[str, Dict] = {}
        self._opgroups_sig: str = ""

        self._poll_timer: QTimer | None = None
        self._clock_timer: QTimer | None = None
        self._js8_rx_timer: QTimer | None = None
        self._clock_timer: QTimer | None = None

        self._build_ui()
        self._load_settings()
        self._setup_timer()
        self._setup_clock_timer()
        self._update_clock_labels()
        self._setup_js8_rx_timer()
        self._update_suspend_state()
        self._refresh_auto_query_flags()
        self._refresh_qsy_options()
        if self._poll_timer:
            self._poll_timer.start()

    def _send_js8_message(self, text: str) -> bool:
        """
        Send a one-shot TX.SEND_MESSAGE to JS8Call over the TCP API.
        """
        host = "127.0.0.1"
        try:
            port = int(self.settings.get("js8_port", 2442) or 2442)
        except Exception:
            port = 2442
        payload = json.dumps({"params": {}, "type": "TX.SEND_MESSAGE", "value": text}) + "\n"
        try:
            with socket.create_connection((host, port), timeout=3) as sock:
                sock.sendall(payload.encode("utf-8"))
            self._last_tx_ts = time.time()
            log.info("JS8CallNetControl: sent TX.SEND_MESSAGE to %s:%s text=%s", host, port, text)
            return True
        except Exception as e:
            log.error("JS8CallNetControl: failed TX.SEND_MESSAGE to %s:%s text=%s err=%s", host, port, text, e)
            return False

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

        # Group / Spotter controls
        gs_row = QHBoxLayout()
        self.set_group_btn = QPushButton("Set Group")
        self.group_edit = QLineEdit()
        self.group_edit.setPlaceholderText("@GROUP")
        gs_row.addWidget(self.set_group_btn)
        gs_row.addWidget(self.group_edit)
        gs_row.addSpacing(12)
        self.set_spotter_btn = QPushButton("Set Spotter")
        self.spotter_combo = QComboBox()
        gs_row.addWidget(self.set_spotter_btn)
        gs_row.addWidget(self.spotter_combo)
        gs_row.addStretch()
        self.qsy_combo = QComboBox()
        self.qsy_combo.currentIndexChanged.connect(self._update_qsy_button_enabled)
        gs_row.addWidget(self.qsy_combo)
        self.suspend_btn = QPushButton("QSY")
        gs_row.addWidget(self.suspend_btn)
        self.ad_hoc_btn = QPushButton("Ad Hoc Net")
        gs_row.addWidget(self.ad_hoc_btn)
        layout.addLayout(gs_row)

        # Check-ins table
        table_layout = QVBoxLayout()
        table_layout.addWidget(QLabel("<b>Check-Ins</b>"))
        self.checkin_table = QTableWidget(0, 10)
        self.checkin_table.setHorizontalHeaderLabels(
            ["CALLSIGN", "NAME", "ST", "GRID", "REGION", "MODE", "SNR", "DT ms", "OFFSET", "STATUS"]
        )
        self.checkin_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.checkin_table.setSelectionMode(QTableWidget.SingleSelection)
        self.checkin_table.horizontalHeader().setStretchLastSection(True)
        table_layout.addWidget(self.checkin_table)
        layout.addLayout(table_layout)

        # Buttons row
        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("Start Net")
        self.ack_btn = QPushButton("ACK Check-ins")
        self.group_spotter_btn = QPushButton("Spotter-Group")
        self.single_spotter_btn = QPushButton("Spotter-Callsign")
        self.save_btn = QPushButton("Save Checkins")
        self.end_btn = QPushButton("End Net")
        self.ack_btn.setEnabled(False)
        self.end_btn.setEnabled(False)
        self.start_btn.setStyleSheet(self._start_btn_default_style)
        self.end_btn.setStyleSheet(self._end_btn_default_style)

        btn_row.addWidget(self.start_btn)
        btn_row.addWidget(self.ack_btn)
        btn_row.addWidget(self.group_spotter_btn)
        btn_row.addWidget(self.single_spotter_btn)
        btn_row.addWidget(self.save_btn)
        btn_row.addWidget(self.end_btn)
        btn_row.addStretch()
        # Ad hoc button already placed in group row

        layout.addLayout(btn_row)

        # Signals
        self.start_btn.clicked.connect(self._start_net)
        self.ack_btn.clicked.connect(self._ack_checkins)
        self.group_spotter_btn.clicked.connect(self._group_spotter)
        self.single_spotter_btn.clicked.connect(self._single_spotter)
        self.save_btn.clicked.connect(self._save_checkins)
        self.end_btn.clicked.connect(self._end_net)
        self.set_group_btn.clicked.connect(self._set_group_target)
        self.set_spotter_btn.clicked.connect(self._set_spotter_form)
        self.refresh_spin.valueChanged.connect(self._update_timer_interval)
        self.suspend_btn.clicked.connect(self._on_suspend_clicked)
        self.ad_hoc_btn.clicked.connect(self._start_ad_hoc_net)

        # Clock timer
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._update_clock_labels)
        self._clock_timer.start(1000)
        self._set_net_button_styles(active=False)

    # ---------------- SETTINGS & TIMER ---------------- #

    def _refresh_auto_query_flags(self):
        try:
            self.settings.reload()
        except Exception:
            pass
        self._load_settings()

    def on_settings_saved(self):
        """
        Slot invoked when Settings tab emits settings_saved.
        """
        self._refresh_auto_query_flags()
        self._maybe_reload_operating_groups()

    def _load_settings(self):
        data = self.settings.all()
        self.auto_query_msg_id = bool(data.get("js8_auto_query_msg_id", False))
        self.auto_query_grids = bool(data.get("js8_auto_query_grids", False))

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
        if self._poll_timer:
            self._poll_timer.setInterval(refresh * 1000)

        # Spotter forms dropdown
        forms_dir = Path(data.get("js8_forms_path", "") or "")
        self.spotter_combo.clear()
        forms = []
        if forms_dir.exists() and forms_dir.is_dir():
            for fn in sorted(forms_dir.glob("MCF*.txt")):
                try:
                    num = fn.stem.replace("MCF", "").strip()
                    if num.isdigit():
                        forms.append(f"F!{num}")
                except Exception:
                    continue
        if forms:
            self.spotter_combo.addItems(forms)
            self.spotter_combo.setEnabled(True)
            self.set_spotter_btn.setEnabled(True)
            self.group_spotter_btn.setEnabled(True)
            self.single_spotter_btn.setEnabled(True)
        else:
            self.spotter_combo.addItem("No forms found")
            self.spotter_combo.setEnabled(False)
            self.set_spotter_btn.setEnabled(False)
            self.group_spotter_btn.setEnabled(False)
            self.single_spotter_btn.setEnabled(False)

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

    def _setup_clock_timer(self):
        self._clock_timer = QTimer(self)
        self._clock_timer.timeout.connect(self._tick_clock)
        self._clock_timer.start(1000)

    def _tick_clock(self):
        self._update_clock_labels()
        self._update_suspend_state()

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
            if hasattr(self.settings, "reload"):
                self.settings.reload()
        except Exception:
            pass
        return get_suspend_until(self.settings)

    def _set_suspend_until(self, dt: Optional[datetime.datetime]) -> None:
        set_suspend_until(self.settings, dt)

    def _suspend_active(self) -> bool:
        return suspend_active(self.settings)

    def _scheduler_enabled(self) -> bool:
        return scheduler_enabled(self.settings)

    def _set_suspend_button(self, active: bool, remaining_sec: Optional[float] = None):
        if active:
            mins = 0
            if remaining_sec is not None:
                mins = max(0, int((remaining_sec + 59) // 60))
            label = f"Sched. Paused: {mins} min" if mins else "Sched. Paused"
            self.suspend_btn.setText(label)
            self.suspend_btn.setStyleSheet("QPushButton { background-color: #2196F3; color: white; }")
        else:
            self.suspend_btn.setText("QSY")
            self.suspend_btn.setStyleSheet("QPushButton { background-color: gold; color: black; }")
        self._update_qsy_button_enabled()

    def _update_suspend_state(self):
        enabled = self._scheduler_enabled()
        self.suspend_btn.setEnabled(enabled)
        if not enabled:
            self._set_suspend_button(False)
            self._update_qsy_button_enabled()
            return

        dt = self._get_suspend_until()
        if dt and datetime.datetime.now(datetime.timezone.utc) < dt:
            remaining = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            self._set_suspend_button(True, remaining_sec=remaining)
        else:
            if dt:
                self._set_suspend_until(None)
            self._set_suspend_button(False)
        self._update_qsy_button_enabled()

    def _on_suspend_clicked(self):
        if self._suspend_active():
            self._set_suspend_until(None)
            self._set_suspend_button(False)
            QMessageBox.information(self, "Scheduling", "Scheduling resumed.")
        else:
            meta = self._selected_qsy_meta()
            if not meta:
                QMessageBox.warning(self, "QSY", "Select a frequency to QSY to.")
                return
            if not self._perform_qsy(meta):
                return
            new_until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=30)
            self._set_suspend_until(new_until)
            remaining = (new_until - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            self._set_suspend_button(True, remaining_sec=remaining)
            QMessageBox.information(self, "QSY Applied", "Frequency changed and scheduling paused for 30 minutes.")

    def _refresh_operator_history_views(self) -> None:
        try:
            win = self.window()
            if win and hasattr(win, "refresh_operator_history_views"):
                win.refresh_operator_history_views()
        except Exception:
            pass

    def _format_freq(self, val) -> str:
        try:
            return f"{float(val):.3f}"
        except Exception:
            return str(val) if val is not None else ""

    def _set_net_button_styles(self, active: bool):
        """
        Mirror FLDigi styling: green Start when idle, gray when active; End stays red.
        """
        if active:
            self.start_btn.setStyleSheet("QPushButton { background-color: #9E9E9E; color: white; }")
            self.end_btn.setStyleSheet(self._end_btn_default_style)
        else:
            self.start_btn.setStyleSheet(self._start_btn_default_style)
            self.end_btn.setStyleSheet(self._end_btn_default_style)

    def _maybe_reload_operating_groups(self):
        try:
            if hasattr(self.settings, "reload"):
                self.settings.reload()
        except Exception:
            pass
        og = self._load_operating_groups()
        sig = self._snapshot_operating_groups(og)
        if sig == self._opgroups_sig:
            return
        self._opgroups_sig = sig
        self._refresh_qsy_options(og)

    def _load_operating_groups(self) -> List[Dict]:
        return qsy_load_operating_groups(self.settings)

    def _snapshot_operating_groups(self, og_list: List[Dict]) -> str:
        return qsy_snapshot_operating_groups(og_list)

    def _refresh_qsy_options(self, og_list: Optional[List[Dict]] = None):
        ops = og_list if og_list is not None else self._load_operating_groups()
        self._opgroups_sig = self._snapshot_operating_groups(ops)
        self._qsy_options = build_qsy_options(ops)
        refresh_qsy_combo(self.qsy_combo, self._qsy_options)
        self._update_qsy_button_enabled()

    def _selected_qsy_meta(self) -> Optional[Dict]:
        return selected_qsy_meta(self.qsy_combo)

    def _current_scheduler_freq(self) -> Optional[float]:
        return current_scheduler_freq(self.window())

    def _update_qsy_button_enabled(self):
        if self._suspend_active():
            self.suspend_btn.setEnabled(True)
            return
        enabled = self._scheduler_enabled()
        meta = self._selected_qsy_meta()
        if not enabled or not meta:
            self.suspend_btn.setEnabled(False)
            return
        cur = self._current_scheduler_freq()
        if cur is not None and abs(cur - meta.get("freq", -1)) < 0.001:
            self.suspend_btn.setEnabled(False)
        else:
            self.suspend_btn.setEnabled(True)

    def _perform_qsy(self, meta: Dict) -> bool:
        win = self.window()
        return perform_qsy(win, meta)

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

        self._net_in_progress = True
        self._net_start_utc = datetime.datetime.utcnow().isoformat(timespec="seconds")
        self._net_end_utc = None
        self._all_calls_seen.clear()
        self._queried_msg_ids.clear()
        self._pending_queries.clear()
        self._waiting_for_completion = False
        self._current_query = None
        self._pending_grid_queries.clear()
        self._grid_waiting = False
        self._awaiting_ack_for = None
        self._call_last_rx_ts.clear()
        self._checkins.clear()
        self._checkin_rows.clear()
        self._checkins_saved.clear()
        self._clear_table()
        self._auto_query_paused_by_net = True
        if hasattr(self, "ad_hoc_btn"):
            self.ad_hoc_btn.setEnabled(False)
        self.start_btn.setEnabled(False)
        self._set_net_button_styles(active=True)
        self.end_btn.setEnabled(True)
        self.ack_btn.setEnabled(True)

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

    def _start_ad_hoc_net(self):
        """
        Generate and start an ad hoc JS8 net with a UTC-stamped name.
        """
        if self._net_in_progress:
            QMessageBox.information(self, "Net In Progress", "End the current net before starting an ad hoc net.")
            return
        current = self.net_name_edit.text().strip()
        if current:
            resp = QMessageBox.question(
                self,
                "Replace Net Name",
                "Replace the current net name with an ad hoc name?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if resp != QMessageBox.Yes:
                return
        ts = datetime.datetime.utcnow().strftime("%Y%m%d %H:%M")
        ad_hoc_name = f"JS8 Net - Ad Hoc - {ts} UTC"
        self.net_name_edit.setText(ad_hoc_name)
        self._start_net()

    def _end_net(self):
        if not self._net_in_progress:
            log.info("JS8Call End Net clicked but net_in_progress flag not set; writing log from current state.")

        if self._poll_timer:
            self._poll_timer.stop()

        self._net_end_utc = datetime.datetime.utcnow().isoformat(timespec="seconds")

        # Send net concluded to group if set
        group = self._group_target or self.group_edit.text().strip().upper()
        if group:
            if not group.startswith("@"):
                group = "@" + group
            mycall = self._my_callsign()
            if mycall:
                self._send_js8_message(f"{mycall}: {group} NET CONCLUDED")

        # Record check-ins once more at end (no duplicate increments)
        self._save_checkins(show_message=False)

        # Write the net log file from the current panels
        self._write_net_log_file()

        self._net_in_progress = False
        if hasattr(self, "ad_hoc_btn"):
            self.ad_hoc_btn.setEnabled(True)
        self._auto_query_paused_by_net = False
        self.start_btn.setEnabled(True)
        self.end_btn.setEnabled(False)
        self._set_net_button_styles(active=False)
        self.ack_btn.setEnabled(False)
        self._checkins.clear()
        self._checkin_rows.clear()
        self._checkins_saved.clear()
        self._clear_table()
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
        monitor_announcements = True
        if not self._net_in_progress and not self.auto_query_msg_id and not monitor_announcements:
            return

        log.debug("JS8CallNetControl: polling DIRECTED/ALL (net_in_progress=%s)", self._net_in_progress)
        # First, scan ALL.TXT for recent QUERY MSG transmissions to gate auto-queries
        self._poll_all_for_query_tx()
        log.debug("JS8CallNetControl: last query TX ts=%s", self._last_query_tx_ts)

        try:
            size_now = self._directed_path.stat().st_size
        except Exception as e:
            log.error("JS8CallNetControl: stat DIRECTED.TXT failed: %s", e)
            return

        if size_now < self._last_directed_size:
            # File truncated or rotated; re-read from start
            self._last_directed_size = 0

        # Drop stale pending announcements
        now_ts = time.time()
        for k, ts in list(self._pending_announcements.items()):
            if now_ts - ts > 60:
                self._pending_announcements.pop(k, None)
        # Expire pending query waits
        self._expire_pending_responses()

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
                    # Load pending backlog for seen calls if applicable
                    calls = self._extract_callsigns_from_line(line)
                    if self._net_in_progress and not self._auto_query_paused_by_net:
                        pending_items = self._backlog_fetch_pending(calls)
                        for cs_b, mid_b, kind_b in pending_items:
                            if kind_b == "MSG" and mid_b:
                                self._pending_queries.append((None, cs_b, mid_b))
                                self._backlog_touch_attempt(cs_b, mid_b, "MSG")
                            elif kind_b == "GRID":
                                self._pending_grid_queries.append((None, cs_b))
                                self._backlog_touch_attempt(cs_b, mid_b, "GRID")
                    # Net announcement detection (only when net not in progress)
                    if self._line_has_announce_form(line):
                        # If message completion marker present, notify immediately; else mark pending
                        call_primary = calls[0] if calls else ""
                        if self._is_message_complete_line(line):
                            self._maybe_notify_announcement(call_primary, line)
                        else:
                            self._pending_announcements[(call_primary or "UNKNOWN")] = time.time()
                    # If a completion marker arrives, see if we had a pending announcement for this call
                    if self._is_message_complete_line(line) and self._pending_announcements:
                        call_primary = calls[0] if calls else "UNKNOWN"
                        pending_ts = self._pending_announcements.pop(call_primary, None)
                        if pending_ts:
                            self._maybe_notify_announcement(call_primary, line)
                    self._maybe_capture_grid_report(line)
                    self._maybe_record_inbound_trigger(line, calls)
                    msg_ids = self._extract_message_ids(line)
                    # If multiple stations reported YES MSG <id>, query each (only when addressed to us)
                    mycall = self._my_callsign()
                    if msg_ids and calls:
                        dest_cs = ""
                        try:
                            msg_field = line.split("\t", 4)[4]
                            if ":" in msg_field:
                                dest_cs = msg_field.split(":", 1)[1].strip().split()[0].strip().upper()
                        except Exception:
                            dest_cs = ""
                        if not mycall:
                            log.info("JS8CallNetControl: YES MSG line but no mycall set; skipping: %s", line.strip())
                        elif dest_cs != mycall:
                            log.info(
                                "JS8CallNetControl: YES MSG line addressed to %s (not %s); skipping",
                                dest_cs or "(unknown)",
                                mycall,
                            )
                        elif not self._saw_recent_query_tx():
                            log.info(
                                "JS8CallNetControl: skipping YES MSG (no recent QUERY MSG TX): %s", line.strip()
                            )
                        else:
                            for c in calls:
                                for mid in msg_ids:
                                    log.info(
                                        "JS8CallNetControl: queueing auto-query id=%s from %s (dest=%s)", mid, c, dest_cs
                                    )
                                    self._queue_auto_query(c, mid)
                    call_primary = calls[0] if calls else ""
                    if not call_primary:
                        continue

                    # During an active net, record/update the check-in row
                    if self._net_in_progress:
                        if not self._line_has_checkin_form(line) and call_primary not in self._checkins:
                            continue
                        self._upsert_checkin(call_primary, status="NEW")

                    # Check for completion markers to advance queue
                    self._process_message_completion(line)

                self._last_directed_size = f.tell()
        except Exception as e:
            log.error("JS8CallNetControl: failed reading DIRECTED.TXT: %s", e)
            return

        # If no net in progress, skip UI updates (auto-query can still run)
        if not self._net_in_progress:
            return

    def _poll_all_for_query_tx(self):
        """
        Scan ALL.TXT for outgoing QUERY MSG(S) transmissions to enable auto-query from DIRECTED.
        """
        if self._auto_query_paused_by_net:
            return
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
                    mycall = self._my_callsign()
                    if mycall and f"{mycall}:" in up:
                        self._last_tx_ts = time.time()
                    if "QUERY MSG" in up:
                        self._last_query_tx_ts = time.time()
                        log.info("JS8CallNetControl: detected outgoing QUERY MSG in ALL.TXT: %s", line.strip())
                    # Detect outbound ACK to release pending QUERY MSGS
                    if "ACK" in up and mycall and f"{mycall}:" in up:
                        dest = ""
                        try:
                            msg_part = line.split("JS8:", 1)[1]
                            rest = msg_part.split(":", 1)[1]
                            dest = rest.strip().split()[0].strip().upper()
                        except Exception:
                            dest = ""
                        if dest and self._awaiting_ack_for and dest == self._awaiting_ack_for:
                            log.info("JS8CallNetControl: ACK sent to %s; issuing follow-up QUERY MSGS", dest)
                            self._awaiting_ack_for = None
                            self._send_query_msgs(dest)
                            self._maybe_process_next_query()
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

    # ---------------- CHECK-IN TABLE HELPERS ---------------- #

    def _clear_table(self) -> None:
        self.checkin_table.setRowCount(0)

    def _region_for_state(self, st: str) -> str:
        st = (st or "").strip().upper()
        fema = {
            "CT": "R01",
            "ME": "R01",
            "MA": "R01",
            "NH": "R01",
            "RI": "R01",
            "VT": "R01",
            "NJ": "R02",
            "NY": "R02",
            "PR": "R02",
            "VI": "R02",
            "DC": "R03",
            "DE": "R03",
            "MD": "R03",
            "PA": "R03",
            "VA": "R03",
            "WV": "R03",
            "AL": "R04",
            "FL": "R04",
            "GA": "R04",
            "KY": "R04",
            "MS": "R04",
            "NC": "R04",
            "SC": "R04",
            "TN": "R04",
            "IL": "R05",
            "IN": "R05",
            "MI": "R05",
            "MN": "R05",
            "OH": "R05",
            "WI": "R05",
            "AR": "R06",
            "LA": "R06",
            "NM": "R06",
            "OK": "R06",
            "TX": "R06",
            "IA": "R07",
            "KS": "R07",
            "MO": "R07",
            "NE": "R07",
            "CO": "R08",
            "MT": "R08",
            "ND": "R08",
            "SD": "R08",
            "UT": "R08",
            "WY": "R08",
            "AZ": "R09",
            "CA": "R09",
            "HI": "R09",
            "NV": "R09",
            "GU": "R09",
            "AS": "R09",
            "MP": "R09",
            "AK": "R10",
            "ID": "R10",
            "OR": "R10",
            "WA": "R10",
        }
        return fema.get(st, "")

    def _lookup_operator_meta(self, callsign: str) -> Dict[str, str]:
        meta = {"name": "", "state": "", "grid": "", "region": ""}
        cs = (callsign or "").strip().upper()
        if not cs:
            return meta
        try:
            root = Path(__file__).resolve().parents[2]
            db_path = root / "config" / "freqinout_nets.db"
            if not db_path.exists():
                return meta
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT name, state, grid FROM operator_checkins WHERE callsign=?",
                (cs,),
            )
            row = cur.fetchone()
            conn.close()
            if row:
                meta["name"] = row[0] or ""
                meta["state"] = (row[1] or "").upper()
                meta["grid"] = (row[2] or "").upper()
                if meta["state"]:
                    meta["region"] = self._region_for_state(meta["state"])
        except Exception:
            pass
        return meta

    def _ensure_row(self, callsign: str) -> int:
        if callsign in self._checkin_rows:
            return self._checkin_rows[callsign]
        row = self.checkin_table.rowCount()
        self.checkin_table.insertRow(row)
        self._checkin_rows[callsign] = row
        for col in range(self.checkin_table.columnCount()):
            item = QTableWidgetItem("")
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            self.checkin_table.setItem(row, col, item)
        return row

    def _update_row(self, callsign: str, data: Dict) -> None:
        row = self._ensure_row(callsign)
        cols = ["CALLSIGN", "NAME", "ST", "GRID", "REGION", "MODE", "SNR", "DT ms", "OFFSET", "STATUS"]
        values = [
            callsign,
            data.get("name", ""),
            data.get("state", ""),
            data.get("grid", ""),
            data.get("region", ""),
            data.get("mode", ""),
            "" if data.get("snr") is None else str(data.get("snr")),
            "" if data.get("dt") is None else str(data.get("dt")),
            "" if data.get("offset") is None else str(data.get("offset")),
            data.get("status", ""),
        ]
        for idx, val in enumerate(values):
            item = self.checkin_table.item(row, idx)
            if item is None:
                item = QTableWidgetItem()
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self.checkin_table.setItem(row, idx, item)
            item.setText(val)
            # Status coloring
            if cols[idx] == "STATUS":
                status_upper = val.upper()
                mismatch = self._status_mismatch.get(callsign, False)
                if status_upper == "ACKED":
                    item.setBackground(Qt.green)
                elif status_upper.startswith("F!"):
                    item.setBackground(Qt.red if mismatch else Qt.cyan)
                else:
                    item.setBackground(Qt.white)

    def _upsert_checkin(
        self,
        callsign: str,
        *,
        status: str = "NEW",
        mode: Optional[str] = None,
        snr: Optional[float] = None,
        dt_ms: Optional[float] = None,
        offset: Optional[int] = None,
        grid: str = "",
        status_mismatch: bool = False,
    ) -> None:
        cs = (callsign or "").strip().upper()
        if not cs:
            return
        base = cs.split("/", 1)[0]
        meta = self._lookup_operator_meta(base)
        if grid:
            meta["grid"] = grid
        data = self._checkins.get(base, {})
        current_status = (data.get("status") or "").upper()
        # Do not downgrade from ACKED/F!xxx to NEW
        if status:
            if current_status.startswith("F!") or current_status == "ACKED":
                status_to_use = data.get("status", status)
            else:
                status_to_use = status
        else:
            status_to_use = data.get("status", "")
        data.update(
            {
                "name": meta.get("name", ""),
                "state": meta.get("state", ""),
                "grid": meta.get("grid", ""),
                "region": meta.get("region", ""),
                "mode": mode or data.get("mode", ""),
                "snr": snr if snr is not None else data.get("snr"),
                "dt": dt_ms if dt_ms is not None else data.get("dt"),
                "offset": offset if offset is not None else data.get("offset"),
                "status": status_to_use,
            }
        )
        self._checkins[base] = data
        self._status_mismatch[base] = status_mismatch
        self._update_row(base, data)

    def _selected_callsign(self) -> Optional[str]:
        selected = self.checkin_table.selectedItems()
        if not selected:
            return None
        row = selected[0].row()
        item = self.checkin_table.item(row, 0)
        return item.text().strip().upper() if item else None

    def _set_group_target(self):
        txt = self.group_edit.text().strip().upper()
        if txt and not txt.startswith("@"):
            txt = "@" + txt.lstrip("@")
        if txt.count("@") > 1:
            txt = "@" + txt.replace("@", "")
        self._group_target = txt
        self.group_edit.setText(txt)

    def _set_spotter_form(self):
        if not self.spotter_combo.isEnabled():
            QMessageBox.warning(self, "Spotter", "No JS8Spotter forms found.")
            return
        self._spotter_form = self.spotter_combo.currentText().strip().upper()
        self._expected_form = self._spotter_form

    def _ack_checkins(self):
        if not self._net_in_progress:
            QMessageBox.information(self, "Net", "Start the net before ACK.")
            return
        new_calls = [c for c, d in self._checkins.items() if (d.get("status") or "").upper() == "NEW"]
        if not new_calls:
            QMessageBox.information(self, "ACK", "No NEW check-ins to ACK.")
            return
        short_codes = self._build_short_code_summary(new_calls)
        if not short_codes:
            QMessageBox.information(self, "ACK", "No callsigns to ACK.")
            return
        text = f"ACK {short_codes}"
        if self._send_js8_message(text):
            for cs in new_calls:
                self._upsert_checkin(cs, status="ACKED")

    def _group_spotter(self):
        if not self._net_in_progress:
            QMessageBox.information(self, "Net", "Start the net first.")
            return
        group = self._group_target or self.group_edit.text().strip().upper()
        if group and not group.startswith("@"):
            group = "@" + group
        if not group:
            QMessageBox.warning(self, "Group", "Set a group first.")
            return
        if self._spotter_form is None:
            QMessageBox.warning(self, "Spotter", "Select a spotter form first.")
            return
        mycall = self._my_callsign()
        if not mycall:
            QMessageBox.warning(self, "Callsign", "Configure your callsign in Settings.")
            return
        self._expected_form = self._spotter_form
        text = f"{mycall}: {group} E? {self._spotter_form}"
        self._send_js8_message(text)

    def _single_spotter(self):
        if not self._net_in_progress:
            QMessageBox.information(self, "Net", "Start the net first.")
            return
        if self._spotter_form is None:
            QMessageBox.warning(self, "Spotter", "Select a spotter form first.")
            return
        cs = self._selected_callsign()
        if not cs:
            QMessageBox.information(self, "Spotter", "Select one check-in row.")
            return
        mycall = self._my_callsign()
        if not mycall:
            QMessageBox.warning(self, "Callsign", "Configure your callsign in Settings.")
            return
        self._expected_form = self._spotter_form
        text = f"{mycall}: {cs} E? {self._spotter_form}"
        self._send_js8_message(text)

    def _save_checkins(self, *, show_message: bool = True):
        """
        Increment check-in counters for current check-ins (once per net).
        """
        try:
            for cs in self._checkins.keys():
                base = cs.split("/", 1)[0]
                if base in self._checkins_saved:
                    continue
                self._increment_checkin_counter(base)
                self._checkins_saved.add(base)
        except Exception as e:
            log.error("JS8CallNetControl: save checkins failed: %s", e)
        if show_message:
            QMessageBox.information(self, "Saved", "Check-ins recorded for this net.")
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

        # Collect all full callsigns from the table
        all_calls = list(self._checkins.keys())

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

    @staticmethod
    def _base_callsign(cs: str) -> str:
        import re

        cs_norm = (cs or "").strip().upper()
        if not cs_norm:
            return ""
        return re.sub(r"/(P|M|MM|QRP|SOTA|ROVER|[A-Z0-9]{1,4})$", "", cs_norm)

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
        return re.findall(r"\bYES\s+MSG(?:\s+ID)?\s+(\d+)", line, flags=re.IGNORECASE)

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
        # Require first token to be exactly our callsign + colon, then dest callsign token
        mycall = self._my_callsign()
        first = tokens[0].strip()
        if not mycall or first.upper() != (mycall + ":"):
            return
        if len(tokens) < 2:
            return
        dest_call = tokens[1].strip().strip(":").upper()
        if not dest_call:
            return
        # Only proceed if dest looks like a normal callsign (must contain a letter; avoid pure digits/macros)
        if not re.match(r"^(?=.*[A-Z])[A-Z0-9]{3,}$", dest_call):
            return
        # Determine group from last trigger if recent
        group_val = ""
        now = time.time()
        trig = self._last_inbound_triggers.get(dest_call)
        if trig and now - trig[1] <= 900:
            group_val = trig[0]
        elif self._my_callsign():
            group_val = self._my_callsign()
        # Prevent multiple inserts for the same dest during this run
        if dest_call in self._auto_inserted_callsigns:
            return
        self._auto_inserted_callsigns.add(dest_call)
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

    def _increment_checkin_counter(self, callsign: str) -> None:
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
                    last_net TEXT,
                    last_role TEXT,
                    checkin_count INTEGER DEFAULT 0,
                    groups_json TEXT,
                    trusted INTEGER DEFAULT 1
                )
                """
            )
            cur.execute("SELECT checkin_count FROM operator_checkins WHERE callsign=?", (cs,))
            row = cur.fetchone()
            if row:
                count = int(row[0] or 0) + 1
                cur.execute(
                    "UPDATE operator_checkins SET checkin_count=?, last_seen_utc=strftime('%Y-%m-%d', 'now') WHERE callsign=?",
                    (count, cs),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO operator_checkins (callsign, first_seen_utc, last_seen_utc, checkin_count, trusted)
                    VALUES (?, strftime('%Y-%m-%d','now'), strftime('%Y-%m-%d','now'), 1, 0)
                    """,
                    (cs,),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            log.error("JS8CallNetControl: failed to increment checkin count for %s: %s", cs, e)
    def _maybe_capture_grid_report(self, line: str) -> None:
        """
        Capture GRID reports in DIRECTED.TXT lines (ignore GRID? queries).
        """
        if "..." in line:
            return
        parts = line.split("\t")
        if len(parts) < 5:
            return
        if "GRID?" in line.upper():
            return
        msg = parts[4]
        if "GRID" not in msg.upper():
            return
        try:
            ts = datetime.datetime.strptime(parts[0][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
        except Exception:
            ts = datetime.datetime.now(datetime.timezone.utc)
        freq_hz = None
        try:
            freq_hz = float(parts[1]) * 1_000_000.0
        except Exception:
            freq_hz = None
        # Parse origin and tokens
        if ":" not in msg:
            return
        origin, rest = msg.split(":", 1)
        origin = origin.strip().upper()
        tokens = rest.strip().replace(",", " ").split()
        if not tokens:
            return
        # Look for GRID token
        try:
            idx = [t.upper() for t in tokens].index("GRID")
        except ValueError:
            return
        if idx + 1 >= len(tokens):
            return
        grid = tokens[idx + 1].strip().upper()
        if not grid or "?" in grid or not self._valid_grid(grid):
            return
        # Choose longest grid compared to existing later
        grp = ""
        # explicit @GROUP if present
        for t in tokens:
            if t.startswith("@"):
                grp = t.lstrip("@").upper()
                break
        groups = []
        if grp and self._is_allowed_group(grp):
            groups.append(grp)
        op_group = self._lookup_operating_group(freq_hz)
        if op_group:
            groups.append(op_group)
        # Require at least one group (explicit or via frequency)
        if not groups:
            return
        self._upsert_operator_info(origin, grid, groups, ts)

    def _lookup_operating_group(self, freq_hz: Optional[float]) -> str:
        try:
            ops = self.settings.get("operating_groups", []) or []
        except Exception:
            return ""
        if not freq_hz:
            return ""
        mhz = round(freq_hz / 1_000_000.0, 3)
        for row in ops:
            try:
                ftxt = str(row.get("frequency", "")).strip()
                if not ftxt:
                    continue
                if abs(float(ftxt) - mhz) < 0.0005:
                    grp = str(row.get("group", "")).strip()
                    if grp:
                        return grp.upper()
            except Exception:
                continue
        return ""

    def _is_allowed_group(self, grp: str) -> bool:
        g = (grp or "").strip().upper()
        if not g:
            return False
        try:
            prim = [x.strip().upper() for x in (self.settings.get("primary_js8_groups", []) or []) if x]
        except Exception:
            prim = []
        try:
            ops = [str(row.get("group", "")).strip().upper() for row in (self.settings.get("operating_groups", []) or []) if row]
        except Exception:
            ops = []
        return g in prim or g in ops

    def _valid_grid(self, grid: str) -> bool:
        import re
        # Maidenhead: 4-char (LLDD) or 6-char (LLDDLL)
        return bool(re.match(r"^[A-R]{2}[0-9]{2}([A-X]{2})?$", grid.upper()))

    def _upsert_operator_info(self, callsign: str, grid: str, groups: List[str], ts: datetime.datetime) -> None:
        cs = (callsign or "").strip().upper()
        if not cs:
            return
        ts_str = ts.astimezone(datetime.timezone.utc).isoformat()
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
            cur.execute(
                "SELECT grid, group1, group2, group3, groups_json, trusted FROM operator_checkins WHERE callsign=?",
                (cs,),
            )
            row = cur.fetchone()
            groups = [g.strip().upper() for g in groups if g]
            groups = [g for g in groups if g]
            groups_json = json.dumps(groups) if groups else None
            if row is None:
                cur.execute(
                    """
                    INSERT INTO operator_checkins (
                        callsign, name, state, grid, group1, group2, group3, group_role,
                        first_seen_utc, last_seen_utc, checkin_count, groups_json, trusted
                    ) VALUES (?, '', '', ?, ?, ?, ?, '', ?, ?, 0, ?, 0)
                    """,
                    (
                        cs,
                        grid,
                        groups[0] if len(groups) > 0 else "",
                        groups[1] if len(groups) > 1 else "",
                        groups[2] if len(groups) > 2 else "",
                        ts_str,
                        ts_str,
                        groups_json,
                    ),
                )
            else:
                existing_grid, g1, g2, g3, gj, trusted = row
                # Keep existing grid if already set; do not replace with new reports
                final_grid = existing_grid.strip().upper() if existing_grid else grid
                # merge groups into slots then json
                slots = [g1 or "", g2 or "", g3 or ""]
                slot_set = {s.strip().upper() for s in slots if s}
                merged = slot_set.copy()
                merged.update(groups)
                # fill slots first
                slots_filled = []
                for s in slots:
                    val = s.strip().upper()
                    if val:
                        slots_filled.append(val)
                for g in groups:
                    if len(slots_filled) < 3 and g not in slots_filled:
                        slots_filled.append(g)
                while len(slots_filled) < 3:
                    slots_filled.append("")
                extra = merged - set(slots_filled) if merged else set()
                extra_json = []
                if gj:
                    try:
                        prev = json.loads(gj)
                        if isinstance(prev, list):
                            extra_json.extend([str(x).upper() for x in prev])
                    except Exception:
                        pass
                for g in extra:
                    if g and g not in extra_json:
                        extra_json.append(g)
                cur.execute(
                    """
                    UPDATE operator_checkins
                    SET
                        grid=?,
                        group1=?,
                        group2=?,
                        group3=?,
                        groups_json=?,
                        last_seen_utc=?
                    WHERE callsign=?
                    """,
                    (
                        final_grid,
                        slots_filled[0],
                        slots_filled[1],
                        slots_filled[2],
                        json.dumps(extra_json) if extra_json else gj,
                        ts_str,
                        cs,
                    ),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("JS8CallNetControl: failed to upsert operator info %s: %s", callsign, e)
    def _queue_auto_query(self, call: str, msg_id: str, snr: float | None = None) -> None:
        """
        Queue a query for MSG ID, and process one at a time when enabled.
        """
        if not self.auto_query_msg_id:
            return
        if self._auto_query_paused_by_net:
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
            if self._current_query_sent_ts and (time.time() - self._current_query_sent_ts) > 15:
                log.debug("JS8CallNetControl: completion timeout; clearing wait and advancing queue")
                self._waiting_for_completion = False
                self._awaiting_ack_for = None
                self._current_query = None
                self._current_query_sent_ts = 0.0
            else:
                log.debug("JS8CallNetControl: waiting_for_completion; skipping process_next_query")
                return
        if not self._pending_queries:
            log.debug("JS8CallNetControl: no pending queries to process")
            return
        if self._auto_query_paused_by_net:
            # Persist pending to backlog so we can retry later
            for snr_val, call, msg_id in list(self._pending_queries):
                self._backlog_upsert(call, msg_id, "MSG", status="PENDING")
            self._pending_queries.clear()
            return
        # Avoid querying while RX just occurred (idle gap)
        if time.time() - self._last_rx_ts < 2.0:
            log.debug("JS8CallNetControl: RX idle gap not met; deferring auto-query")
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
        mycall = self._my_callsign() or ""
        query_text = f"{mycall}: {call} QUERY MSG {msg_id}".strip()
        log.info("JS8CallNetControl: attempting auto-query TX to %s msg_id=%s text=\"%s\"", call, msg_id, query_text)
        sent = self._send_js8_message(query_text)
        if sent:
            self._queried_msg_ids.add(key)
            self._waiting_for_completion = True
            self._current_query = (call, msg_id)
            self._current_query_sent_ts = time.time()
            # Expect a MSG reply within 120s; track pending response and backlog
            expiry = time.time() + 120
            self._awaiting_msg_responses[(call, msg_id)] = expiry
            self._backlog_upsert(call, msg_id, "MSG", status="PENDING")
            log.info("JS8CallNetControl: auto-queried MSG ID %s from %s via TX.SEND_MESSAGE", msg_id, call)
        else:
            log.error("JS8CallNetControl: auto query send failed for %s/%s", call, msg_id)
            self._backlog_upsert(call, msg_id, "MSG", status="PENDING")
            self._current_query = None
            self._waiting_for_completion = False
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
                    base_frm = self._base_callsign(frm) if frm else ""
                    if base_frm:
                        self._call_last_rx_ts[base_frm] = now_ts
                        # If awaiting MSG response for this call, mark retrieved on any MSG token
                        for (c, mid), exp in list(self._awaiting_msg_responses.items()):
                            if c == base_frm and "MSG" in combined:
                                self._mark_backlog_retrieved(c, mid, "MSG")
                                self._awaiting_msg_responses.pop((c, mid), None)
                        # If awaiting GRID response for this call and GRID present, mark retrieved
                        if base_frm in self._awaiting_grid_responses and "GRID" in combined:
                            self._mark_backlog_retrieved(base_frm, "", "GRID")
                            self._awaiting_grid_responses.pop(base_frm, None)
                        if self._net_in_progress:
                            # Extract metrics from API payload when available
                            try:
                                snr_val = float(p.get("SNR")) if p.get("SNR") not in (None, "") else None
                            except Exception:
                                snr_val = None
                            speed_val = p.get("SPEED")
                            mode_name = ""
                            if speed_val is not None:
                                try:
                                    sval = int(speed_val)
                                    mode_name = {0: "Normal", 1: "Fast", 2: "Turbo", 4: "Slow"}.get(
                                        sval, str(speed_val)
                                    )
                                except Exception:
                                    mode_name = str(speed_val)
                            try:
                                offset_val = int(p.get("OFFSET")) if p.get("OFFSET") not in (None, "") else None
                            except Exception:
                                offset_val = None
                            try:
                                dt_val = float(p.get("DT")) if p.get("DT") not in (None, "") else None
                            except Exception:
                                dt_val = None
                            self._upsert_checkin(
                                base_frm,
                                status="NEW",
                                mode=mode_name,
                                snr=snr_val,
                                dt_ms=dt_val,
                                offset=offset_val,
                                grid=(p.get("GRID") or "").strip().upper(),
                            )
                    snr_val = None
                    try:
                        snr_val = float(p.get("SNR")) if p.get("SNR") not in (None, "") else None
                    except Exception:
                        snr_val = None
                    if self.auto_query_msg_id and not self._auto_query_paused_by_net:
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
                    base_frm = self._base_callsign(frm) if frm else ""
                    if grid_val and base_frm:
                        self._update_operator_grid(base_frm, grid_val, self._active_group_name())
                    else:
                        for token in txt.split():
                            if 4 <= len(token) <= 6 and token[:2].isalpha() and token[2:4].isdigit():
                                self._update_operator_grid(base_frm or frm, token, self._active_group_name())
                                break
                    # Spotter form response handling
                    if self._net_in_progress and self._expected_form:
                        forms_found = re.findall(r"F![0-9]{3}", combined)
                        for form in forms_found:
                            if form.upper() not in CHECKIN_FORMS:
                                continue
                            if base_frm:
                                mismatch = form != self._expected_form
                                self._upsert_checkin(
                                    base_frm,
                                    status=form,
                                    status_mismatch=mismatch,
                                )
                    # Auto grid query when allowed
                    if self.auto_query_grids and not self._auto_query_paused_by_net and not self._net_lockout_active():
                        target_cs = base_frm or frm
                        if target_cs and self._operator_missing_grid(target_cs):
                            self._maybe_queue_grid_query(target_cs, snr_val, msg_params=p, text=txt)
                except Exception:
                    continue
        except queue.Empty:
            pass
        self._maybe_process_next_query()
        self._maybe_process_next_grid()

    def _line_has_checkin_form(self, line: str) -> bool:
        """
        Returns True if the line contains a JS8Spotter check-in form (F!103 or F!104).
        """
        up = line.upper()
        return any(form in up for form in CHECKIN_FORMS)

    def _line_has_announce_form(self, line: str) -> bool:
        return ANNOUNCE_FORM in line.upper()

    def _maybe_notify_announcement(self, callsign: str, line: str) -> None:
        """
        Show a popup when a net announcement (F!106) is fully received and no net is in progress.
        Debounced per callsign.
        """
        if self._net_in_progress:
            return
        call = (callsign or "").strip().upper()
        if not call:
            return
        now = time.time()
        last = self._recent_announcements.get(call, 0)
        if now - last < 300:
            return
        self._recent_announcements[call] = now
        msg_text = line
        try:
            QMessageBox.information(
                self,
                "Net Announcement Received",
                f"Net announcement (F!106) received from {call}.\n\n{msg_text}",
            )
        except Exception as e:
            log.debug("JS8CallNetControl: failed to show announcement popup: %s", e)

    # ---------------- Auto-query backlog ---------------- #

    def _backlog_db_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "config" / "freqinout_nets.db"

    def _backlog_upsert(self, callsign: str, msg_id: str, kind: str, status: str = "PENDING") -> None:
        try:
            conn = sqlite3.connect(self._backlog_db_path())
            cur = conn.cursor()
            now_ts = time.time()
            cur.execute(
                """
                DELETE FROM autoquery_backlog WHERE callsign=? AND COALESCE(msg_id,'')=COALESCE(?, '') AND kind=?
                """,
                (callsign, msg_id or "", kind),
            )
            cur.execute(
                """
                INSERT INTO autoquery_backlog (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts)
                VALUES (?, ?, ?, ?, 0, ?, ?)
                """,
                (callsign, msg_id, kind, status, now_ts, now_ts),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("JS8 autoquery backlog upsert failed: %s", e)

    def _backlog_mark(self, callsign: str, msg_id: str, kind: str, status: str) -> None:
        try:
            conn = sqlite3.connect(self._backlog_db_path())
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE autoquery_backlog
                SET status=?, last_attempt_ts=?
                WHERE callsign=? AND COALESCE(msg_id,'')=COALESCE(?, '') AND kind=?
                """,
                (status, time.time(), callsign, msg_id or "", kind),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("JS8 autoquery backlog mark failed: %s", e)

    def _backlog_fetch_pending(self, callsigns: List[str]) -> List[tuple[str, str, str]]:
        if not callsigns:
            return []
        try:
            conn = sqlite3.connect(self._backlog_db_path())
            cur = conn.cursor()
            qs = ",".join("?" for _ in callsigns)
            cur.execute(
                f"""
                SELECT callsign, msg_id, kind
                FROM autoquery_backlog
                WHERE status='PENDING' AND callsign IN ({qs})
                """,
                [c.upper() for c in callsigns],
            )
            rows = cur.fetchall()
            conn.close()
            return [(r[0] or "", r[1] or "", r[2] or "MSG") for r in rows]
        except Exception as e:
            log.debug("JS8 autoquery backlog fetch failed: %s", e)
            return []

    def _backlog_touch_attempt(self, callsign: str, msg_id: str, kind: str) -> None:
        try:
            conn = sqlite3.connect(self._backlog_db_path())
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE autoquery_backlog
                SET attempts=attempts+1, last_attempt_ts=?
                WHERE callsign=? AND COALESCE(msg_id,'')=COALESCE(?, '') AND kind=?
                """,
                (time.time(), callsign, msg_id or "", kind),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("JS8 autoquery backlog touch failed: %s", e)

    def _mark_backlog_retrieved(self, callsign: str, msg_id: str, kind: str) -> None:
        self._backlog_mark(callsign, msg_id, kind, "RETRIEVED")

    def _mark_backlog_failed(self, callsign: str, msg_id: str, kind: str) -> None:
        self._backlog_mark(callsign, msg_id, kind, "FAILED")

    def _expire_pending_responses(self) -> None:
        now = time.time()
        for key, exp in list(self._awaiting_msg_responses.items()):
            if now > exp:
                call, mid = key
                self._mark_backlog_failed(call, mid, "MSG")
                self._awaiting_msg_responses.pop(key, None)
        for call, exp in list(self._awaiting_grid_responses.items()):
            if now > exp:
                self._mark_backlog_failed(call, "", "GRID")
                self._awaiting_grid_responses.pop(call, None)

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
        self._current_query_sent_ts = 0.0
        # After a message completes, wait for our ACK to be sent before querying for more
        if call:
            self._awaiting_ack_for = call
            log.info("JS8CallNetControl: message completion detected; waiting for ACK before querying MSGS from %s", call)
        self._maybe_process_next_query()

    def _send_query_msgs(self, call: str) -> None:
        """
        Send QUERY MSGS to a specific station to discover additional messages.
        """
        mycall = self._my_callsign() or ""
        text = f"{mycall}: {call} QUERY MSGS".strip()
        sent = self._send_js8_message(text)
        if sent:
            log.info("JS8CallNetControl: queried additional messages from %s", call)
        else:
            log.error("JS8CallNetControl: failed sending QUERY MSGS to %s", call)

    # ---------------- Grid helpers ---------------- #

    def _operator_missing_grid(self, callsign: str) -> bool:
        cs = self._base_callsign(callsign)
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
        cs = self._base_callsign(callsign)
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
        if self._auto_query_paused_by_net:
            return
        # Only query when traffic is directed to us or a group (skip third-party directed traffic)
        mycall = (self._my_callsign() or "").strip().upper()
        dest = (msg_params.get("TO") or "").strip().upper()
        if dest and not dest.startswith("@") and mycall and dest != mycall:
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
        if self._auto_query_paused_by_net:
            return
        if time.time() - self._grid_last_rx_ts < 2.0:
            return
        if self._net_lockout_active():
            return
        # Defer while our own transmission recently occurred (e.g., auto-reply in progress)
        if time.time() - self._last_tx_ts < 5.0:
            return
        now_ts = time.time()
        # Weakest SNR first
        self._pending_grid_queries.sort(key=lambda t: (999 if t[0] is None else t[0]))
        # Respect per-callsign quiet window
        processed = 0
        max_attempts = len(self._pending_grid_queries)
        while self._pending_grid_queries and processed < max_attempts:
            snr_val, call = self._pending_grid_queries.pop(0)
            last_rx = self._call_last_rx_ts.get(self._base_callsign(call), 0.0)
            if last_rx and (now_ts - last_rx) < AUTO_GRID_QUIET_SECS:
                # Too recent; push to back and try later
                self._pending_grid_queries.append((snr_val, call))
                processed += 1
                continue
            break
        else:
            return
        mycall = self._my_callsign() or ""
        query_text = f"{mycall}: {call} GRID?".strip()
        log.info("JS8CallNetControl: attempting auto grid query to %s text=\"%s\"", call, query_text)
        if self._send_js8_message(query_text):
            log.info("JS8CallNetControl: auto grid query to %s", call)
            self._grid_waiting = True
            self._awaiting_grid_responses[call] = time.time() + 120
            self._backlog_upsert(call, "", "GRID", status="PENDING")
        else:
            log.error("JS8CallNetControl: failed GRID? to %s", call)
            self._backlog_upsert(call, "", "GRID", status="PENDING")

    def _is_message_complete_line(self, line: str) -> bool:
        """
        Heuristic: treat lines containing the JS8Call end-of-message marker
        (diamond 'â™¢') as completion markers.
        """
        txt = line.strip()
        if not txt:
            return False
        if "â™¢" in txt:
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
        Build minimal unique short codes from callsigns.

        Rules:
          - If a call has a suffix (e.g. K7ABC/P), strip everything after "/" and
            derive the short code from the base call.
          - Start with the last 3 characters of the base call (or fewer if shorter).
          - If duplicates collide, incrementally extend to 4, 5, ... characters
            (up to the full base) until each code is unique.
          - Preserve input order; return space-delimited codes.
        """
        bases: List[str] = []
        for cs in calls:
            base = (cs or "").strip().upper()
            if not base:
                continue
            if "/" in base:
                base = base.split("/", 1)[0]
            bases.append(base)

        # Track how many chars to use from the end for each base call
        lengths = [min(3, len(b)) if len(b) < 3 else 3 for b in bases]

        # Gradually extend colliding codes until unique or max length reached
        while True:
            codes = [b[-lengths[i]:] for i, b in enumerate(bases)]
            counts = {}
            for c in codes:
                counts[c] = counts.get(c, 0) + 1
            duplicates = {idx for idx, c in enumerate(codes) if counts[c] > 1}
            if not duplicates:
                break
            progressed = False
            for idx in duplicates:
                if lengths[idx] < len(bases[idx]):
                    lengths[idx] += 1
                    progressed = True
            if not progressed:
                # Cannot disambiguate further (very short/identical bases); exit
                break

        return " ".join(bases[i][-lengths[i]:] for i in range(len(bases)))

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
