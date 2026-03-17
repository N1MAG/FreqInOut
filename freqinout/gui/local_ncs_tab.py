from __future__ import annotations

import csv
import datetime
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, QTimer, Signal, QEvent
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QCompleter,
)

from freqinout.core.local_ops_store import (
    get_all_operators,
    get_operator,
    list_checkins,
    record_checkin,
    update_checkin_entry,
)
from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.gui.theme import resolve_theme, button_style
from freqinout.utils.timezones import get_timezone


STATUS_OPTIONS = ["GREEN", "YELLOW", "RED"]


class LocalNCSTab(QWidget):
    """
    Local NCS check-in workflow backed by the local operator roster.
    """

    local_data_updated = Signal()
    net_status_changed = Signal(str, bool)

    COL_TIME = 0
    COL_CALLSIGN = 1
    COL_NAME = 2
    COL_CITY = 3
    COL_STATE = 4
    COL_CATEGORY = 5
    COL_STATUS = 6
    COL_NOTES = 7

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self._rows: List[Dict[str, Any]] = []
        self._rows_by_id: Dict[int, Dict[str, Any]] = {}
        self._lookup_rows: List[Dict[str, Any]] = []
        self._lookup_by_callsign: Dict[str, Dict[str, Any]] = {}
        self._session_entry_ids: set[int] = set()
        self._editing_entry_id: Optional[int] = None
        self._dirty_ids: set[int] = set()
        self._binding_selection = False
        self._binding_editor = False
        self._net_in_progress = False
        self._net_session_mode = ""
        self._net_start_utc: Optional[str] = None
        self._ignore_next_lookup_return = False
        self._clock_timer: Optional[QTimer] = None
        self._autosave_timer: Optional[QTimer] = None
        self._station_context = None
        self._station_context_syncing = False
        self._session_device_profile_id: Optional[int] = None

        self._build_ui()
        self._restore_context()
        self.reload_operator_lookup()
        self._load_checkins()
        self._setup_timers()
        self.apply_theme()

    def set_station_context(self, context: object) -> None:
        if self._station_context is context:
            return
        if self._station_context is not None:
            try:
                self._station_context.snapshots_changed.disconnect(self._on_station_context_snapshots_changed)
            except Exception:
                pass
            try:
                self._station_context.selection_changed.disconnect(self._on_station_context_selection_changed)
            except Exception:
                pass
        self._station_context = context
        if self._station_context is not None:
            try:
                self._station_context.snapshots_changed.connect(self._on_station_context_snapshots_changed)
                self._station_context.selection_changed.connect(self._on_station_context_selection_changed)
            except Exception:
                pass
        self._refresh_station_context_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        header.addWidget(QLabel("<h3>Local NCS</h3>"))
        header.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        header.addWidget(self.utc_label)
        header.addWidget(self.local_label)
        layout.addLayout(header)

        scope_row = QHBoxLayout()
        self.selected_radio_label = QLabel("Run on: Station Default")
        self.selected_radio_label.setStyleSheet("font-weight: 600;")
        scope_row.addWidget(self.selected_radio_label)
        self.selected_radio_combo = QComboBox()
        self.selected_radio_combo.setMinimumWidth(220)
        self.selected_radio_combo.currentIndexChanged.connect(self._on_selected_radio_combo_changed)
        self.selected_radio_combo.setVisible(False)
        scope_row.addWidget(self.selected_radio_combo)
        scope_row.addStretch()
        layout.addLayout(scope_row)

        self.selected_radio_summary_label = QLabel("Selected radio drives local net-control actions.")
        self.selected_radio_summary_label.setWordWrap(True)
        self.selected_radio_summary_label.setStyleSheet("color: #888;")
        layout.addWidget(self.selected_radio_summary_label)

        info_row = QHBoxLayout()
        info_row.addWidget(QLabel("Role:"))
        self.role_combo = QComboBox()
        self.role_combo.addItems(["NCS"])
        self.role_combo.setEnabled(False)
        info_row.addWidget(self.role_combo)
        info_row.addSpacing(12)
        info_row.addWidget(QLabel("Net Name:"))
        self.net_name_edit = QLineEdit()
        self.net_name_edit.setPlaceholderText("Local net name (ad hoc supported)")
        info_row.addWidget(self.net_name_edit, stretch=1)
        info_row.addSpacing(12)
        info_row.addWidget(QLabel("Channels:"))
        self.channels_edit = QLineEdit()
        self.channels_edit.setPlaceholderText("Example: 146.520 simplex; GMRS RPT 462.650")
        info_row.addWidget(self.channels_edit, stretch=1)
        layout.addLayout(info_row)

        session_row = QHBoxLayout()
        self.start_net_btn = QPushButton("Start Net")
        self.join_net_btn = QPushButton("Join Net")
        self.end_net_btn = QPushButton("End Net")
        self.session_status_label = QLabel("Net Session: Not Active")
        session_row.addWidget(self.start_net_btn)
        session_row.addWidget(self.join_net_btn)
        session_row.addWidget(self.end_net_btn)
        session_row.addSpacing(10)
        session_row.addWidget(self.session_status_label, stretch=1)
        layout.addLayout(session_row)

        lookup_row = QHBoxLayout()
        lookup_row.addWidget(QLabel("Operator Lookup/Add:"))
        self.lookup_edit = QLineEdit()
        self.lookup_edit.setPlaceholderText("CALL / Name / State (or callsign only)")
        lookup_row.addWidget(self.lookup_edit, stretch=1)
        self.add_checkin_btn = QPushButton("Add Check-in")
        self.add_checkin_btn.setFocusPolicy(Qt.StrongFocus)
        lookup_row.addWidget(self.add_checkin_btn)
        self.refresh_btn = QPushButton("Refresh")
        lookup_row.addWidget(self.refresh_btn)
        self.export_btn = QPushButton("Export CSV")
        lookup_row.addWidget(self.export_btn)
        layout.addLayout(lookup_row)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Callsign, name, city, state, category, notes")
        filter_row.addWidget(self.search_edit, stretch=1)
        filter_row.addWidget(QLabel("SitRep:"))
        self.status_filter_combo = QComboBox()
        self.status_filter_combo.addItems(["All", "GREEN", "YELLOW", "RED"])
        filter_row.addWidget(self.status_filter_combo)
        layout.addLayout(filter_row)

        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(
            ["Check-in UTC", "Callsign", "Name", "City", "State", "Category", "SitRep", "Notes"]
        )
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        header_view = self.table.horizontalHeader()
        header_view.setSectionResizeMode(self.COL_TIME, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(self.COL_CALLSIGN, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(self.COL_NAME, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(self.COL_CITY, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(self.COL_STATE, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(self.COL_CATEGORY, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(self.COL_STATUS, QHeaderView.ResizeToContents)
        header_view.setSectionResizeMode(self.COL_NOTES, QHeaderView.Stretch)
        layout.addWidget(self.table, stretch=1)

        editor_row = QVBoxLayout()
        self.editor_target_label = QLabel("Selected Check-in: none")
        editor_row.addWidget(self.editor_target_label)

        editor_top = QHBoxLayout()
        editor_top.addWidget(QLabel("SitRep:"))
        self.status_combo = QComboBox()
        self.status_combo.addItems(STATUS_OPTIONS)
        self.status_combo.setToolTip("GREEN=All ok, YELLOW=Situation has risks, RED=Priority issue")
        editor_top.addWidget(self.status_combo)
        self.save_entry_btn = QPushButton("Save Entry")
        editor_top.addWidget(self.save_entry_btn)
        self.autosave_label = QLabel("Autosave: idle")
        editor_top.addWidget(self.autosave_label)
        editor_top.addStretch()
        editor_row.addLayout(editor_top)

        self.notes_edit = QTextEdit()
        self.notes_edit.setPlaceholderText(
            "Persistent notes for this check-in. Update as new local information arrives."
        )
        self.notes_edit.setMinimumHeight(90)
        editor_row.addWidget(self.notes_edit)
        layout.addLayout(editor_row)

        self.net_name_edit.textChanged.connect(self._persist_context)
        self.channels_edit.textChanged.connect(self._persist_context)
        self.net_name_edit.textChanged.connect(self._update_net_session_ui)
        self.channels_edit.textChanged.connect(self._update_net_session_ui)
        self.start_net_btn.clicked.connect(self._start_local_net)
        self.join_net_btn.clicked.connect(self._join_local_net)
        self.end_net_btn.clicked.connect(self._end_local_net)
        self.lookup_edit.returnPressed.connect(self._on_lookup_return)
        self.add_checkin_btn.clicked.connect(self._safe_add_lookup_checkin)
        self.lookup_edit.textChanged.connect(self._update_action_button_styles)
        self.refresh_btn.clicked.connect(self._load_checkins)
        self.export_btn.clicked.connect(self._export_csv)
        self.search_edit.textChanged.connect(self._apply_filters)
        self.status_filter_combo.currentIndexChanged.connect(self._apply_filters)
        self.table.itemSelectionChanged.connect(self._on_table_selection_changed)
        self.status_combo.currentTextChanged.connect(self._mark_current_dirty)
        self.notes_edit.textChanged.connect(self._mark_current_dirty)
        self.save_entry_btn.clicked.connect(lambda: self._save_current_entry(show_feedback=True))
        self.lookup_edit.installEventFilter(self)
        self.add_checkin_btn.installEventFilter(self)
        self.setTabOrder(self.lookup_edit, self.add_checkin_btn)
        self._update_net_session_ui()

    def _setup_timers(self) -> None:
        self._clock_timer = QTimer(self)
        self._clock_timer.setInterval(1000)
        self._clock_timer.timeout.connect(self._update_clock_labels)
        self._clock_timer.start()
        self._update_clock_labels()

        self._autosave_timer = QTimer(self)
        self._autosave_timer.setInterval(30000)
        self._autosave_timer.timeout.connect(self._autosave_dirty)
        self._autosave_timer.start()

    def _restore_context(self) -> None:
        try:
            net_name = (self.settings.get("local_ncs_net_name", "") or "").strip()
            channels = (self.settings.get("local_ncs_channels", "") or "").strip()
        except Exception:
            net_name = ""
            channels = ""
        self.net_name_edit.setText(net_name)
        self.channels_edit.setText(channels)

    def _persist_context(self) -> None:
        try:
            self.settings.set("local_ncs_net_name", self.net_name_edit.text().strip())
            self.settings.set("local_ncs_channels", self.channels_edit.text().strip())
        except Exception:
            pass

    def _ui_tz_abbr(self, tz_name: str, fallback: str) -> str:
        mapping = {
            "UTC": "UTC",
            "America/New_York": "ET",
            "America/Chicago": "CT",
            "America/Denver": "MT",
            "America/Los_Angeles": "PT",
        }
        return mapping.get(tz_name, fallback)

    def _update_clock_labels(self) -> None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a")
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))

        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        tz = get_timezone(tz_name)
        now_local = now_utc.astimezone(tz)
        fallback = now_local.tzname() or tz_name
        abbr = self._ui_tz_abbr(tz_name, fallback)
        local_day = now_local.strftime("%a")
        self.local_label.setText(now_local.strftime(f"<b>Local ({local_day}):</b> %y%m%d %H:%M:%S {abbr}"))

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.start_net_btn.setStyleSheet(button_style("muted", theme))
        self.join_net_btn.setStyleSheet(button_style("muted", theme))
        self.end_net_btn.setStyleSheet(button_style("muted", theme))
        self.add_checkin_btn.setStyleSheet(button_style("muted", theme))
        self.refresh_btn.setStyleSheet(button_style("primary", theme))
        self.export_btn.setStyleSheet(button_style("muted", theme))
        self.save_entry_btn.setStyleSheet(button_style("muted", theme))
        self._update_action_button_styles(theme)
        self._refresh_status_cell_colors()

    def _update_action_button_styles(self, theme: Optional[Dict[str, str]] = None, *_args) -> None:
        if not isinstance(theme, dict):
            theme = resolve_theme(self.settings)
        active = bool(self._net_in_progress)
        lookup_has_text = bool((self.lookup_edit.text() or "").strip()) if hasattr(self, "lookup_edit") else False
        dirty_entry = bool(self._dirty_ids)
        if active:
            self.start_net_btn.setStyleSheet(button_style("muted", theme))
            self.join_net_btn.setStyleSheet(button_style("muted", theme))
            self.end_net_btn.setStyleSheet(button_style("eligible_danger", theme))
            add_role = "eligible_success" if lookup_has_text else "muted"
            self.add_checkin_btn.setStyleSheet(button_style(add_role, theme))
        else:
            self.start_net_btn.setStyleSheet(button_style("eligible_success", theme))
            self.join_net_btn.setStyleSheet(button_style("eligible_info", theme))
            self.end_net_btn.setStyleSheet(button_style("muted", theme))
            self.add_checkin_btn.setStyleSheet(button_style("muted", theme))
        self.save_entry_btn.setStyleSheet(button_style("eligible_info" if dirty_entry else "muted", theme))

    def on_settings_saved(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        self.apply_theme()
        self._update_clock_labels()

    def set_tab_active(self, active: bool) -> None:
        if active:
            if self._clock_timer is not None and not self._clock_timer.isActive():
                self._clock_timer.start()
            if self._autosave_timer is not None and not self._autosave_timer.isActive():
                self._autosave_timer.start()
            self._update_clock_labels()
            return
        self._autosave_dirty()
        if self._clock_timer is not None:
            self._clock_timer.stop()
        if self._autosave_timer is not None:
            self._autosave_timer.stop()

    def shutdown(self) -> None:
        self._autosave_dirty()

    def reload_operator_lookup(self) -> None:
        try:
            rows = get_all_operators()
        except Exception as e:
            log.error("LocalNCSTab: failed to load local operators for lookup: %s", e)
            rows = []

        self._lookup_rows = rows
        self._lookup_by_callsign = {}
        completion_values: List[str] = []
        for row in rows:
            cs = str(row.get("callsign", "")).strip().upper()
            if not cs:
                continue
            self._lookup_by_callsign[cs] = row
            first_name = str(row.get("first_name", "")).strip()
            last_name = str(row.get("last_name", "")).strip()
            full_name = str(row.get("name", "")).strip() or " ".join(
                [p for p in (first_name, last_name) if p]
            ).strip()
            display = self._format_entry(
                cs,
                full_name,
                str(row.get("state", "")).strip().upper(),
            )
            completion_values.append(display)

        completer = QCompleter(completion_values, self.lookup_edit)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        completer.setFilterMode(Qt.MatchContains)
        completer.setCompletionMode(QCompleter.PopupCompletion)
        try:
            completer.activated[str].connect(self._on_lookup_completion_activated)
        except Exception:
            try:
                completer.activated.connect(self._on_lookup_completion_activated)
            except Exception:
                pass
        self.lookup_edit.setCompleter(completer)

    def _parse_lookup_line(self, line: str) -> tuple[str, str, str]:
        text = (line or "").strip()
        if not text:
            return "", "", ""
        if "/" in text and not text.startswith("#"):
            parts = [p.strip() for p in text.split("/") if p.strip()]
            if len(parts) >= 3:
                return self._normalize_callsign(parts[0].split()[0]), parts[1], parts[2].upper()
            if len(parts) == 2:
                return self._normalize_callsign(parts[0].split()[0]), parts[1], ""
            if len(parts) == 1:
                return self._normalize_callsign(parts[0].split()[0]), "", ""

        tokens = text.split()
        if len(tokens) >= 3:
            return self._normalize_callsign(tokens[0]), tokens[1], tokens[2].upper()
        if len(tokens) == 2:
            return self._normalize_callsign(tokens[0]), tokens[1], ""
        return self._normalize_callsign(tokens[0]), "", ""

    @staticmethod
    def _normalize_callsign(token: str) -> str:
        txt = (token or "").strip().upper()
        if not txt:
            return ""
        # Keep common callsign characters only.
        cleaned = re.sub(r"[^A-Z0-9/]", "", txt)
        return cleaned

    def _format_entry(self, cs: str, name: str, state: str) -> str:
        parts = [p for p in (cs.strip().upper(), name.strip(), state.strip().upper()) if p]
        return " / ".join(parts)

    def _full_name_from_row(self, row: Dict[str, Any]) -> str:
        first_name = str(row.get("first_name", "")).strip()
        last_name = str(row.get("last_name", "")).strip()
        return str(row.get("name", "")).strip() or " ".join([p for p in (first_name, last_name) if p]).strip()

    def _formatted_lookup_entry(self, row: Dict[str, Any]) -> str:
        return self._format_entry(
            str(row.get("callsign", "")).strip().upper(),
            self._full_name_from_row(row),
            str(row.get("state", "")).strip().upper(),
        )

    def _matching_lookup_rows(self, query: str) -> List[Dict[str, Any]]:
        needle = (query or "").strip().lower()
        if not needle:
            return []
        matches: List[Dict[str, Any]] = []
        for row in self._lookup_rows:
            display = self._formatted_lookup_entry(row)
            if needle in display.lower():
                matches.append(row)
        return matches

    def _apply_lookup_autofill(self) -> bool:
        text = self.lookup_edit.text().strip()
        if not text:
            return False
        matches = self._matching_lookup_rows(text)
        if not matches:
            comp = self.lookup_edit.completer()
            if comp:
                comp.setCompletionPrefix(text)
                model = comp.completionModel()
                if model and model.rowCount() == 1:
                    completion = model.index(0, 0).data()
                    if completion:
                        matches = self._matching_lookup_rows(str(completion))
        row: Optional[Dict[str, Any]] = matches[0] if len(matches) == 1 else None
        if row is None:
            cs, _, _ = self._parse_lookup_line(text)
            if cs:
                row = self._lookup_by_callsign.get(cs)
        if row is None:
            return False
        formatted = self._formatted_lookup_entry(row)
        self.lookup_edit.setText(formatted)
        self.lookup_edit.setFocus()
        self.lookup_edit.setCursorPosition(len(formatted))
        return True

    @staticmethod
    def _split_name(value: str) -> tuple[str, str]:
        txt = (value or "").strip()
        if not txt:
            return "", ""
        parts = [p for p in txt.split() if p]
        if len(parts) <= 1:
            return (parts[0] if parts else ""), ""
        return parts[0], " ".join(parts[1:])

    def _start_local_net(self) -> None:
        self._begin_local_net_session(joined=False)

    def _join_local_net(self) -> None:
        self._begin_local_net_session(joined=True)

    def _begin_local_net_session(self, *, joined: bool) -> None:
        if self._net_in_progress:
            QMessageBox.information(self, "Local Net", "A local net session is already active.")
            return

        net_name = self.net_name_edit.text().strip()
        if not net_name and not joined:
            ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d %H:%M")
            net_name = f"LOCAL - Ad Hoc - {ts} UTC"
            self.net_name_edit.setText(net_name)
        if not net_name:
            QMessageBox.warning(self, "Local Net", "Enter Net Name before joining a local net.")
            return

        self._persist_context()
        self._net_in_progress = True
        self._net_session_mode = "JOINED" if joined else "STARTED"
        self._net_start_utc = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        self._reset_session_table()
        self.lookup_edit.setFocus()
        self.net_status_changed.emit("LOCAL", True)
        self._update_net_session_ui()
        self._pin_session_radio()

    def _end_local_net(self) -> None:
        if not self._net_in_progress:
            QMessageBox.information(self, "Local Net", "No active local net session.")
            return
        resp = QMessageBox.question(
            self,
            "End Local Net",
            "End local net session now?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return
        self._autosave_dirty()
        self._net_in_progress = False
        self._net_session_mode = ""
        self._net_start_utc = None
        self._reset_session_table()
        self.net_status_changed.emit("LOCAL", False)
        self._update_net_session_ui()
        self._clear_session_radio_pin()

    def _update_net_session_ui(self) -> None:
        active = bool(self._net_in_progress)
        self.start_net_btn.setEnabled(not active)
        self.join_net_btn.setEnabled(not active)
        self.end_net_btn.setEnabled(active)
        self.lookup_edit.setEnabled(active)
        self.add_checkin_btn.setEnabled(True)
        if not active:
            self.add_checkin_btn.setToolTip("Start or Join Net to enable operator lookup entry.")
            self.session_status_label.setText("Net Session: Not Active")
            self._update_action_button_styles()
            return

        self.add_checkin_btn.setToolTip("")
        net_name = self.net_name_edit.text().strip() or "(unnamed)"
        channels = self.channels_edit.text().strip() or "ad hoc channels"
        started = (self._net_start_utc or "").replace("T", " ").replace("+00:00", "Z")
        self.session_status_label.setText(
            f"Net Session: {self._net_session_mode} | {net_name} | {channels} | {started}"
        )
        self._update_action_button_styles()

    def _pin_session_radio(self) -> None:
        selected = None
        if self._station_context is not None and hasattr(self._station_context, "selected_snapshot"):
            try:
                selected = self._station_context.selected_snapshot()
            except Exception:
                selected = None
        if selected is not None:
            try:
                self._session_device_profile_id = int(selected.device_profile_id)
            except Exception:
                self._session_device_profile_id = None
        self._refresh_station_context_ui()

    def _clear_session_radio_pin(self) -> None:
        self._session_device_profile_id = None
        self._refresh_station_context_ui()

    def _refresh_station_context_ui(self) -> None:
        if not hasattr(self, "selected_radio_combo"):
            return
        context = self._station_context
        if context is None:
            self.selected_radio_combo.setVisible(False)
            self.selected_radio_label.setText("Run on: Station Default")
            self.selected_radio_summary_label.setText("Selected radio drives local net-control actions.")
            return
        snapshots = list(getattr(context, "active_txrx_snapshots", lambda: [])())
        selected = getattr(context, "selected_snapshot", lambda: None)()
        pinned_snapshot = None
        if self._session_device_profile_id is not None:
            pinned_snapshot = next(
                (
                    snapshot
                    for snapshot in snapshots
                    if int(snapshot.device_profile_id or 0) == int(self._session_device_profile_id)
                ),
                None,
            )
        self._station_context_syncing = True
        try:
            self.selected_radio_combo.blockSignals(True)
            self.selected_radio_combo.clear()
            for snapshot in snapshots:
                self.selected_radio_combo.addItem(snapshot.name or f"Device {snapshot.device_profile_id}", int(snapshot.device_profile_id))
            active_snapshot = pinned_snapshot or selected
            if active_snapshot is not None:
                idx = self.selected_radio_combo.findData(int(active_snapshot.device_profile_id))
                if idx >= 0:
                    self.selected_radio_combo.setCurrentIndex(idx)
            self.selected_radio_combo.setVisible(len(snapshots) > 1)
            self.selected_radio_combo.setEnabled(self._session_device_profile_id is None)
        finally:
            self.selected_radio_combo.blockSignals(False)
            self._station_context_syncing = False
        active_snapshot = pinned_snapshot or selected
        if active_snapshot is None:
            self.selected_radio_label.setText("Run on: Station Default")
            self.selected_radio_summary_label.setText("Selected radio drives local net-control actions.")
            return
        name = active_snapshot.name or f"Device {active_snapshot.device_profile_id}"
        prefix = "Run on (Pinned):" if pinned_snapshot is not None else "Run on:"
        self.selected_radio_label.setText(f"{prefix} {name}")
        summary = (
            f"Backend: {active_snapshot.control_backend.upper()} | "
            f"Endpoint: {active_snapshot.endpoint_summary or 'Unavailable'} | "
            f"Schedule/Policy: {active_snapshot.assigned_operating_profile_name or 'Unassigned'}"
        )
        if pinned_snapshot is not None:
            summary = f"Active session stays pinned to {name}. {summary}"
        self.selected_radio_summary_label.setText(summary)

    def _on_selected_radio_combo_changed(self, _index: int) -> None:
        if self._station_context is None or self._station_context_syncing or self._session_device_profile_id is not None:
            return
        device_profile_id = int(self.selected_radio_combo.currentData() or 0)
        if device_profile_id <= 0:
            return
        try:
            self._station_context.set_selected_device_profile(device_profile_id)
        except Exception as exc:
            log.debug("LocalNCS: failed changing selected radio: %s", exc)

    def _on_station_context_snapshots_changed(self, _snapshots: object) -> None:
        self._refresh_station_context_ui()

    def _on_station_context_selection_changed(self, _snapshot: object) -> None:
        if self._session_device_profile_id is not None and self._station_context is not None:
            selected = getattr(self._station_context, "selected_snapshot", lambda: None)()
            if (
                selected is None
                or int(getattr(selected, "device_profile_id", 0) or 0) != int(self._session_device_profile_id)
            ):
                try:
                    self._station_context.set_selected_device_profile(int(self._session_device_profile_id))
                except Exception:
                    pass
                return
        self._refresh_station_context_ui()

    def _add_lookup_checkin(self) -> None:
        if not self._net_in_progress:
            QMessageBox.information(self, "Add Check-in", "Start or join a local net before adding check-ins.")
            return
        text = self.lookup_edit.text().strip()
        if not text:
            return
        matches = self._matching_lookup_rows(text)
        if len(matches) == 1:
            text = self._formatted_lookup_entry(matches[0])
            self.lookup_edit.setText(text)
        elif len(matches) > 1:
            cs_guess, _, _ = self._parse_lookup_line(text)
            if cs_guess and cs_guess in self._lookup_by_callsign:
                text = self._formatted_lookup_entry(self._lookup_by_callsign[cs_guess])
                self.lookup_edit.setText(text)
            else:
                QMessageBox.information(
                    self,
                    "Add Check-in",
                    "Multiple operators match this lookup. Press Enter to refine selection, then add.",
                )
                return
        cs, name, state = self._parse_lookup_line(text)
        if not cs:
            QMessageBox.warning(self, "Add Check-in", "Enter at least a callsign.")
            return

        known = get_operator(cs) or self._lookup_by_callsign.get(cs) or {}
        known_first = str(known.get("first_name", "")).strip()
        known_last = str(known.get("last_name", "")).strip()
        known_name = str(known.get("name", "")).strip()
        name_out = name.strip() or known_name or " ".join([p for p in (known_first, known_last) if p]).strip()
        first_name_out = known_first
        last_name_out = known_last
        if name_out and not first_name_out and not last_name_out:
            first_name_out, last_name_out = self._split_name(name_out)
        state_out = state.strip().upper() or str(known.get("state", "")).strip().upper()
        city_out = str(known.get("city", "")).strip()
        category_out = str(known.get("category", "")).strip()
        status_out = str(known.get("sitrep_status", "GREEN")).strip().upper() or "GREEN"
        notes_out = str(known.get("notes", "")).strip()

        net_name = self.net_name_edit.text().strip()
        channels = self.channels_edit.text().strip()

        entry_id = record_checkin(
            callsign=cs,
            net_name=net_name,
            channels=channels,
            first_name=first_name_out,
            last_name=last_name_out,
            name=name_out,
            city=city_out,
            state=state_out,
            category=category_out,
            sitrep_status=status_out,
            notes=notes_out,
        )
        if not entry_id:
            QMessageBox.warning(self, "Add Check-in", f"Unable to add check-in for {cs}.")
            return
        self._session_entry_ids.add(int(entry_id))
        self.lookup_edit.clear()
        self._load_checkins(select_id=int(entry_id))
        self.local_data_updated.emit()

    def _safe_add_lookup_checkin(self) -> None:
        try:
            self._add_lookup_checkin()
        except Exception as e:
            log.exception("LocalNCSTab: add-checkin action failed")
            QMessageBox.warning(self, "Add Check-in", f"Unable to add check-in.\n{e}")

    def _on_lookup_completion_activated(self, text: object) -> None:
        try:
            if text:
                self.lookup_edit.setText(str(text).strip())
            # Completion activation can be followed by returnPressed; suppress duplicate handling.
            self._ignore_next_lookup_return = True
            self._apply_lookup_autofill()
        except Exception as e:
            log.exception("LocalNCSTab: completion activation failed")
            QMessageBox.warning(self, "Add Check-in", f"Unable to use selected operator.\n{e}")

    def _on_lookup_return(self) -> None:
        try:
            # If completion handler already consumed this Enter, skip duplicate handling.
            if self._ignore_next_lookup_return:
                self._ignore_next_lookup_return = False
                return
            if self._apply_lookup_autofill():
                return
            text = self.lookup_edit.text().strip()
            cs, name, state = self._parse_lookup_line(text)
            if cs and name and state:
                self.lookup_edit.setText(self._format_entry(cs, name, state))
        except Exception as e:
            log.exception("LocalNCSTab: lookup return handler failed")
            QMessageBox.warning(self, "Add Check-in", f"Unable to use selected operator.\n{e}")

    def eventFilter(self, obj, event):
        try:
            if obj is self.lookup_edit and event.type() == QEvent.ShortcutOverride:
                if event.key() in (Qt.Key_Return, Qt.Key_Enter):
                    event.accept()
                    return True
                if event.key() == Qt.Key_Tab:
                    self._on_lookup_return()
                    QTimer.singleShot(0, self.add_checkin_btn.setFocus)
                    event.accept()
                    return True
            if obj is self.lookup_edit and event.type() == QEvent.KeyPress:
                if event.key() == Qt.Key_Tab:
                    self.add_checkin_btn.setFocus()
                    event.accept()
                    return True
            if obj is self.add_checkin_btn and event.type() == QEvent.KeyPress:
                if event.key() in (Qt.Key_Return, Qt.Key_Enter, Qt.Key_Space):
                    self._safe_add_lookup_checkin()
                    event.accept()
                    return True
        except Exception as e:
            log.exception("LocalNCSTab: eventFilter failure: %s", e)
            return True
        return super().eventFilter(obj, event)

    def _load_checkins(self, select_id: Optional[int] = None) -> None:
        try:
            rows = list_checkins(limit=2000)
        except Exception as e:
            log.error("LocalNCSTab: failed to load check-ins: %s", e)
            rows = []
        if self._session_entry_ids:
            rows = [r for r in rows if int(r.get("id", 0) or 0) in self._session_entry_ids]
        elif not self._net_in_progress:
            rows = []
        self._rows = rows
        self._rows_by_id = {int(r.get("id", 0)): r for r in rows if int(r.get("id", 0) or 0) > 0}
        self._apply_filters(select_id=select_id)

    def _reset_session_table(self) -> None:
        self._session_entry_ids.clear()
        self._rows = []
        self._rows_by_id = {}
        self._editing_entry_id = None
        self._dirty_ids.clear()
        self._apply_filters(select_id=None)

    def _row_matches(self, row: Dict[str, Any], query: str, status_filter: str) -> bool:
        if status_filter and status_filter.upper() != "ALL":
            if str(row.get("sitrep_status", "")).strip().upper() != status_filter.upper():
                return False
        if not query:
            return True
        hay = " ".join(
            [
                str(row.get("checkin_utc", "")),
                str(row.get("callsign", "")),
                str(row.get("first_name", "")),
                str(row.get("last_name", "")),
                str(row.get("name", "")),
                str(row.get("city", "")),
                str(row.get("state", "")),
                str(row.get("category", "")),
                str(row.get("sitrep_status", "")),
                str(row.get("notes", "")),
            ]
        ).upper()
        return query.upper() in hay

    def _apply_filters(self, select_id: Optional[int] = None) -> None:
        query = self.search_edit.text().strip()
        status_filter = self.status_filter_combo.currentText().strip()
        rows = [r for r in self._rows if self._row_matches(r, query, status_filter)]
        target_id = select_id or self._editing_entry_id
        self._populate_table(rows, target_id=target_id)

    def _populate_table(self, rows: List[Dict[str, Any]], *, target_id: Optional[int]) -> None:
        self._binding_selection = True
        try:
            self.table.setRowCount(0)
            for row in rows:
                r = self.table.rowCount()
                self.table.insertRow(r)
                entry_id = int(row.get("id", 0) or 0)
                notes_full = str(row.get("notes", ""))
                notes_preview = notes_full if len(notes_full) <= 120 else (notes_full[:117] + "...")
                values = [
                    str(row.get("checkin_utc", "")),
                    str(row.get("callsign", "")).upper(),
                    (
                        str(row.get("name", "")).strip()
                        or " ".join(
                            [
                                p
                                for p in (
                                    str(row.get("first_name", "")).strip(),
                                    str(row.get("last_name", "")).strip(),
                                )
                                if p
                            ]
                        ).strip()
                    ),
                    str(row.get("city", "")),
                    str(row.get("state", "")).upper(),
                    str(row.get("category", "")),
                    str(row.get("sitrep_status", "GREEN")).upper(),
                    notes_preview,
                ]
                for c, value in enumerate(values):
                    item = QTableWidgetItem(value)
                    if c == self.COL_TIME:
                        item.setData(Qt.UserRole, entry_id)
                    if c == self.COL_NOTES:
                        item.setToolTip(notes_full)
                    self.table.setItem(r, c, item)
                self._apply_status_item_style(self.table.item(r, self.COL_STATUS), values[self.COL_STATUS])

            if self.table.rowCount() == 0:
                self._set_editor_enabled(False)
                self._editing_entry_id = None
                self.editor_target_label.setText("Selected Check-in: none")
                return

            row_to_select = 0
            if target_id:
                for r in range(self.table.rowCount()):
                    it = self.table.item(r, self.COL_TIME)
                    if it and int(it.data(Qt.UserRole) or 0) == int(target_id):
                        row_to_select = r
                        break
            self.table.selectRow(row_to_select)
        finally:
            self._binding_selection = False
        self._on_table_selection_changed()

    def _selected_entry_id(self) -> Optional[int]:
        rows = self.table.selectionModel().selectedRows() if self.table.selectionModel() else []
        if not rows:
            return None
        row_idx = rows[0].row()
        item = self.table.item(row_idx, self.COL_TIME)
        if item is None:
            return None
        entry_id = int(item.data(Qt.UserRole) or 0)
        return entry_id if entry_id > 0 else None

    def _set_editor_enabled(self, enabled: bool) -> None:
        self.status_combo.setEnabled(enabled)
        self.notes_edit.setEnabled(enabled)
        self.save_entry_btn.setEnabled(enabled)

    def _on_table_selection_changed(self) -> None:
        if self._binding_selection:
            return
        self._save_current_entry(show_feedback=False)

        entry_id = self._selected_entry_id()
        if not entry_id:
            self._editing_entry_id = None
            self._set_editor_enabled(False)
            self.editor_target_label.setText("Selected Check-in: none")
            self._set_autosave_label()
            return

        row = self._rows_by_id.get(entry_id)
        if not row:
            return
        self._editing_entry_id = entry_id
        self._binding_editor = True
        try:
            self.status_combo.setCurrentText(str(row.get("sitrep_status", "GREEN")).strip().upper())
            self.notes_edit.setPlainText(str(row.get("notes", "")))
        finally:
            self._binding_editor = False
        self._set_editor_enabled(True)
        self.editor_target_label.setText(
            f"Selected Check-in: {row.get('callsign', '').upper()} @ {row.get('checkin_utc', '')}"
        )
        self._set_autosave_label()

    def _mark_current_dirty(self) -> None:
        if self._binding_editor:
            return
        if not self._editing_entry_id:
            return
        self._dirty_ids.add(int(self._editing_entry_id))
        self._set_autosave_label()
        self._update_action_button_styles()
        self._mirror_editor_to_selected_row()

    def _mirror_editor_to_selected_row(self) -> None:
        entry_id = self._editing_entry_id
        if not entry_id:
            return
        status = self.status_combo.currentText().strip().upper() or "GREEN"
        notes = self.notes_edit.toPlainText().strip()
        row_cache = self._rows_by_id.get(entry_id)
        if row_cache is not None:
            row_cache["sitrep_status"] = status
            row_cache["notes"] = notes
        for r in range(self.table.rowCount()):
            id_item = self.table.item(r, self.COL_TIME)
            if id_item is None:
                continue
            if int(id_item.data(Qt.UserRole) or 0) != entry_id:
                continue
            status_item = self.table.item(r, self.COL_STATUS)
            notes_item = self.table.item(r, self.COL_NOTES)
            if status_item is not None:
                status_item.setText(status)
                self._apply_status_item_style(status_item, status)
            if notes_item is not None:
                preview = notes if len(notes) <= 120 else (notes[:117] + "...")
                notes_item.setText(preview)
                notes_item.setToolTip(notes)
            break

    def _save_current_entry(self, *, show_feedback: bool) -> bool:
        entry_id = self._editing_entry_id
        if not entry_id:
            return True
        if int(entry_id) not in self._dirty_ids:
            if show_feedback:
                QMessageBox.information(self, "Save Entry", "No pending edits for the selected check-in.")
            return True

        status = self.status_combo.currentText().strip().upper() or "GREEN"
        notes = self.notes_edit.toPlainText().strip()
        ok = update_checkin_entry(int(entry_id), sitrep_status=status, notes=notes)
        if not ok:
            if show_feedback:
                QMessageBox.warning(self, "Save Entry", "Failed to persist check-in status/notes.")
            return False

        row_cache = self._rows_by_id.get(int(entry_id))
        if row_cache is not None:
            row_cache["sitrep_status"] = status
            row_cache["notes"] = notes
        self._dirty_ids.discard(int(entry_id))
        self._set_autosave_label(saved=True)
        self._update_action_button_styles()
        self.local_data_updated.emit()
        if show_feedback:
            QMessageBox.information(self, "Save Entry", "Check-in entry saved.")
        return True

    def _autosave_dirty(self) -> None:
        if not self._dirty_ids:
            self._set_autosave_label()
            return
        saved_count = 0
        if self._editing_entry_id and int(self._editing_entry_id) in self._dirty_ids:
            if self._save_current_entry(show_feedback=False):
                saved_count += 1
        for entry_id in list(self._dirty_ids):
            row = self._rows_by_id.get(int(entry_id))
            if not row:
                self._dirty_ids.discard(int(entry_id))
                continue
            status = str(row.get("sitrep_status", "GREEN")).strip().upper()
            notes = str(row.get("notes", "")).strip()
            if update_checkin_entry(int(entry_id), sitrep_status=status, notes=notes):
                self._dirty_ids.discard(int(entry_id))
                saved_count += 1
        self._set_autosave_label(saved=saved_count > 0)
        self._update_action_button_styles()
        if saved_count > 0:
            self.local_data_updated.emit()

    def _set_autosave_label(self, *, saved: bool = False) -> None:
        if saved:
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            self.autosave_label.setText(f"Autosave: saved {ts}")
            return
        if self._dirty_ids:
            self.autosave_label.setText(f"Autosave: pending ({len(self._dirty_ids)})")
        else:
            self.autosave_label.setText("Autosave: idle")

    def _apply_status_item_style(self, item: Optional[QTableWidgetItem], status: str) -> None:
        if item is None:
            return
        key = (status or "").strip().upper()
        if key == "RED":
            bg = QColor("#D32F2F")
            fg = QColor("#FFFFFF")
        elif key == "YELLOW":
            bg = QColor("#FBC02D")
            fg = QColor("#111111")
        else:
            bg = QColor("#43A047")
            fg = QColor("#FFFFFF")
        item.setBackground(bg)
        item.setForeground(fg)

    def _refresh_status_cell_colors(self) -> None:
        for r in range(self.table.rowCount()):
            item = self.table.item(r, self.COL_STATUS)
            if item is None:
                continue
            self._apply_status_item_style(item, item.text())

    def _export_csv(self) -> None:
        out, _ = QFileDialog.getSaveFileName(
            self,
            "Export Local NCS Check-ins",
            "local_ncs_checkins.csv",
            "CSV Files (*.csv)",
        )
        if not out:
            return
        self._autosave_dirty()
        rows = self._rows
        try:
            with Path(out).open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "id",
                        "checkin_utc",
                        "net_name",
                        "channels",
                        "callsign",
                        "name",
                        "city",
                        "state",
                        "category",
                        "sitrep_status",
                        "notes",
                    ],
                )
                writer.writeheader()
                for row in rows:
                    writer.writerow(
                        {
                            "id": row.get("id", ""),
                            "checkin_utc": row.get("checkin_utc", ""),
                            "net_name": row.get("net_name", ""),
                            "channels": row.get("channels", ""),
                            "callsign": row.get("callsign", ""),
                            "name": row.get("name", ""),
                            "city": row.get("city", ""),
                            "state": row.get("state", ""),
                            "category": row.get("category", ""),
                            "sitrep_status": row.get("sitrep_status", "GREEN"),
                            "notes": row.get("notes", ""),
                        }
                    )
            QMessageBox.information(self, "Export Local NCS Check-ins", f"Exported {len(rows)} row(s).")
        except Exception as e:
            log.error("LocalNCSTab: export failed: %s", e)
            QMessageBox.warning(self, "Export Local NCS Check-ins", f"Export failed:\n{e}")
