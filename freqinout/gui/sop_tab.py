
from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QFontMetrics, QColor
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QComboBox,
    QLineEdit,
    QPushButton,
    QCheckBox,
    QFileDialog,
    QMessageBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QGroupBox,
    QSpinBox,
    QCompleter,
)

from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sop_manager import SOPManager
from freqinout.gui.theme import resolve_theme, button_style
from freqinout.utils.timezones import get_timezone


def _contrast_text_hex(bg_hex: str) -> str:
    h = (bg_hex or "").strip().lstrip("#")
    if len(h) != 6:
        return "#111111"
    try:
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
    except Exception:
        return "#111111"
    yiq = ((r * 299) + (g * 587) + (b * 114)) / 1000
    return "#111111" if yiq >= 140 else "#FFFFFF"


class SOPTab(QWidget):
    """
    SOP reminders tab.
    Reminder-only workflow with manual completion and UTC-driven cadence.
    """

    CONTACT_RULE_OPTIONS = [
        ("none", "None"),
        ("hub_or_hub_alt", "HUB OR HUB-ALT"),
        ("ncs_or_ancs", "NCS OR ANCS"),
        ("peer", "PEER"),
        ("callsign", "CallSign"),
    ]
    ANY_ROLE_TOKEN = "__any_role__"
    INTERVAL_PRESETS = ["00:30", "01:00", "03:00", "06:00", "12:00"]
    SOFTWARES = ["JS8Call", "VarAC", "FLDigi"]
    BAND_CHOICES = ["160M", "80M", "60M", "40M", "30M", "20M", "17M", "15M", "12M", "10M", "6M"]

    COL_BAND = 0
    COL_FREQ = 1
    COL_SOFTWARE = 2
    COL_ACTION = 3
    COL_INTERVAL = 4
    COL_CONTACT = 5
    COL_CONTACT_TARGET = 6
    COL_DESC = 7
    COL_REMOVE = 8

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = SettingsManager()
        self.manager = SOPManager()
        self._profiles: List[Dict[str, Any]] = []
        self._selected_profile_id: int | None = None
        self._upcoming_rows: List[Dict[str, Any]] = []
        self._loading_ui = False
        self._operating_groups: List[Dict[str, Any]] = []
        self._hidden_actions: List[Dict[str, Any]] = []
        default_mode = (self.settings.get("display_time_mode", "LOCAL") or "LOCAL").upper()
        self._show_local = default_mode != "UTC"
        self._dirty = False

        self._build_ui()
        self._set_save_dirty(False)
        self._refresh_reference_data()
        self._reload_profiles(select_id=None)
        self.refresh_upcoming()

        self._timer = QTimer(self)
        self._timer.setInterval(30_000)
        self._timer.timeout.connect(self.refresh_upcoming)
        self._timer.start()

        self._clock_timer = QTimer(self)
        self._clock_timer.setInterval(1000)
        self._clock_timer.timeout.connect(self._update_clock_labels)
        self._clock_timer.start()
        self._update_clock_labels()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        title_row = QHBoxLayout()
        title_row.addWidget(QLabel("<h3>SOP Builder</h3>"))
        title_row.addStretch()
        self.utc_label = QLabel()
        self.local_label = QLabel()
        title_row.addWidget(self.utc_label)
        title_row.addWidget(self.local_label)
        self.time_toggle_btn = QPushButton("Showing: Local")
        self.time_toggle_btn.clicked.connect(self._toggle_time_view)
        title_row.addWidget(self.time_toggle_btn)
        root.addLayout(title_row)

        header = QHBoxLayout()
        self.profile_combo = QComboBox()
        self.profile_combo.currentIndexChanged.connect(self._on_profile_selected)
        header.addWidget(QLabel("SOP:"))
        header.addWidget(self.profile_combo, stretch=1)

        self.new_btn = QPushButton("New")
        self.save_btn = QPushButton("Save")
        self.delete_btn = QPushButton("Delete")
        self.export_btn = QPushButton("Export")
        self.import_btn = QPushButton("Import")
        self.new_btn.clicked.connect(self._new_profile)
        self.save_btn.clicked.connect(self._save_profile)
        self.delete_btn.clicked.connect(self._delete_profile)
        self.export_btn.clicked.connect(self._export_profile)
        self.import_btn.clicked.connect(self._import_profile)
        for btn in (self.new_btn, self.save_btn, self.delete_btn, self.export_btn, self.import_btn):
            header.addWidget(btn)
        root.addLayout(header)

        cfg_box = QGroupBox("SOP Configuration")
        cfg_layout = QVBoxLayout(cfg_box)

        row1 = QHBoxLayout()
        self.name_edit = QLineEdit()
        self.group_combo = QComboBox()
        self.secondary_combo = QComboBox()
        self.start_edit = QLineEdit()
        self.start_edit.setPlaceholderText("HH:MM")
        self.active_cb = QCheckBox("Active")
        row1.addWidget(QLabel("Name:"))
        row1.addWidget(self.name_edit, stretch=2)
        row1.addWidget(QLabel("Operating Group:"))
        row1.addWidget(self.group_combo, stretch=1)
        row1.addWidget(QLabel("Secondary Group:"))
        row1.addWidget(self.secondary_combo, stretch=1)
        cfg_layout.addLayout(row1)

        row2 = QHBoxLayout()
        self.start_label = QLabel("SOP Daily Start (UTC):")
        row2.addWidget(self.start_label)
        self.start_edit.setFixedWidth(100)
        row2.addWidget(self.start_edit)
        row2.addWidget(self.active_cb)
        row2.addStretch()
        cfg_layout.addLayout(row2)

        self.contact_label = QLabel("Primary Contacts: --")
        self.contact_label.setWordWrap(True)
        cfg_layout.addWidget(self.contact_label)

        rows_head = QHBoxLayout()
        rows_head.addWidget(QLabel("Action Rows (each row = one SOP reminder action)"))
        rows_head.addStretch()
        self.hidden_rows_label = QLabel("")
        rows_head.addWidget(self.hidden_rows_label)
        self.add_row_btn = QPushButton("Add Action Row")
        self.add_row_btn.clicked.connect(lambda: self._add_action_row(existing=None))
        rows_head.addWidget(self.add_row_btn)
        cfg_layout.addLayout(rows_head)

        self.actions_table = QTableWidget(0, 9)
        self.actions_table.setHorizontalHeaderLabels(
            [
                "Band",
                "Frequency",
                "Software",
                "Action",
                "Interval (HH:MM)",
                "Contact Rule",
                "Contact Target",
                "Description",
                "Remove",
            ]
        )
        self.actions_table.verticalHeader().setVisible(False)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_BAND, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_FREQ, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_SOFTWARE, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_ACTION, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_INTERVAL, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_CONTACT, QHeaderView.ResizeToContents)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_CONTACT_TARGET, QHeaderView.Fixed)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_DESC, QHeaderView.Stretch)
        self.actions_table.horizontalHeader().setSectionResizeMode(self.COL_REMOVE, QHeaderView.ResizeToContents)
        self.actions_table.setColumnWidth(self.COL_CONTACT_TARGET, 170)
        cfg_layout.addWidget(self.actions_table)
        root.addWidget(cfg_box)

        upcoming_box = QGroupBox("Upcoming SOP Actions")
        upcoming_layout = QVBoxLayout(upcoming_box)
        top = QHBoxLayout()
        self.horizon_spin = QSpinBox()
        self.horizon_spin.setRange(1, 48)
        self.horizon_spin.setValue(12)
        self.horizon_spin.valueChanged.connect(self.refresh_upcoming)
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh_upcoming)
        top.addWidget(QLabel("Show next N hours:"))
        top.addWidget(self.horizon_spin)
        top.addStretch()
        top.addWidget(self.refresh_btn)
        upcoming_layout.addLayout(top)

        self.alignment_label = QLabel("")
        self.alignment_label.setWordWrap(True)
        self.alignment_label.setVisible(False)
        upcoming_layout.addWidget(self.alignment_label)

        self.upcoming_table = QTableWidget(0, 8)
        self.upcoming_table.setHorizontalHeaderLabels(
            ["Profile", "Band/Freq", "Software", "Action", "Description", "Next", "Contact", "Status"]
        )
        self.upcoming_table.verticalHeader().setVisible(False)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.upcoming_table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeToContents)
        upcoming_layout.addWidget(self.upcoming_table)
        root.addWidget(upcoming_box)

        self.group_combo.currentIndexChanged.connect(self._on_group_changed)
        self.secondary_combo.currentIndexChanged.connect(self._on_secondary_group_changed)
        self._wire_dirty_tracking()

    def _refresh_reference_data(self) -> None:
        data = self.settings.all()
        og = data.get("operating_groups", [])
        self._operating_groups = [g for g in og if isinstance(g, dict)]

        self.group_combo.blockSignals(True)
        self.group_combo.clear()
        group_names = sorted({(g.get("group") or "").strip().upper() for g in self._operating_groups if g.get("group")})
        self.group_combo.addItem("")
        for name in group_names:
            self.group_combo.addItem(name)
        self.group_combo.blockSignals(False)

        self.secondary_combo.clear()
        self.secondary_combo.addItem("")
        for g in self.manager.load_secondary_groups():
            self.secondary_combo.addItem(g)

    def _wire_dirty_tracking(self) -> None:
        self.name_edit.textChanged.connect(self._mark_dirty)
        self.group_combo.currentIndexChanged.connect(self._mark_dirty)
        self.secondary_combo.currentIndexChanged.connect(self._mark_dirty)
        self.start_edit.textChanged.connect(self._mark_dirty)
        self.active_cb.toggled.connect(self._mark_dirty)

    def _set_save_dirty(self, dirty: bool) -> None:
        self._dirty = bool(dirty)
        try:
            theme = resolve_theme(self.settings)
            if self._dirty:
                self.save_btn.setStyleSheet(button_style("info", theme))
            else:
                self.save_btn.setStyleSheet(button_style("primary", theme))
        except Exception:
            pass

    def _mark_dirty(self, *_args) -> None:
        if self._loading_ui:
            return
        self._set_save_dirty(True)

    def _configured_softwares(self) -> List[str]:
        data = self.settings.all()
        configured: List[str] = []
        if (data.get("js8_directed_path") or "").strip() or (data.get("js8_forms_path") or "").strip():
            configured.append("JS8Call")
        msg_paths = data.get("message_paths", {}) or {}
        if (data.get("varac_path") or "").strip() or str(msg_paths.get("varac") or "").strip():
            configured.append("VarAC")
        if (
            (data.get("path_fldigi") or "").strip()
            or (data.get("fldigi_log_path") or "").strip()
            or (data.get("fldigi_checkin_dir") or "").strip()
        ):
            configured.append("FLDigi")
        return configured

    def _frequency_options_for_group(self, group: str) -> List[str]:
        return self._frequency_options_for_group_band(group, "")

    def _frequency_options_for_group_band(self, group: str, band: str) -> List[str]:
        grp = (group or "").strip().upper()
        band_uc = (band or "").strip().upper()
        values = []
        for row in self._operating_groups:
            if (row.get("group") or "").strip().upper() != grp:
                continue
            row_band = (row.get("band") or "").strip().upper()
            if band_uc and row_band != band_uc:
                continue
            try:
                values.append(f"{float(row.get('frequency', 0)):.3f}")
            except Exception:
                pass
        return sorted(set(values), key=lambda x: float(x)) if values else []

    def _band_options_for_group(self, group: str) -> List[str]:
        grp = (group or "").strip().upper()
        values = set()
        for row in self._operating_groups:
            if (row.get("group") or "").strip().upper() != grp:
                continue
            band = (row.get("band") or "").strip().upper()
            if band:
                values.add(band)
        return sorted(values, key=lambda x: (len(x), x))

    def _load_spotter_forms(self) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        forms_dir = Path(self.settings.get("js8_forms_path", "") or "")
        if not forms_dir.exists():
            return out
        for fn in sorted(forms_dir.glob("MCF*.txt")):
            try:
                num = fn.stem.replace("MCF", "").strip()
                if not num.isdigit():
                    continue
                code = f"F!{num}"
                out.append((f"js8_spotter_{code}", code))
            except Exception:
                continue
        return out

    def _action_catalog(self) -> Dict[str, List[Tuple[str, str]]]:
        catalog: Dict[str, List[Tuple[str, str]]] = {
            "JS8Call": [("js8_send_status", "Status"), ("js8_commstat", "CommStat")],
            "VarAC": [
                ("varac_send_broadcast", "Broadcast"),
                ("varac_direct_contact", "Direct Contact"),
                ("varac_send_sitrep", "SitRep"),
                ("varac_send_statrep", "StatRep"),
                ("varac_send_report", "General"),
            ],
            "FLDigi": [
                ("fldigi_send_sitrep", "SitRep"),
                ("fldigi_send_statrep", "StatRep"),
                ("fldigi_send_report", "General"),
            ],
        }
        for key, label in self._load_spotter_forms():
            catalog.setdefault("JS8Call", []).append((key, label))
        return catalog

    def _reload_profiles(self, select_id: int | None) -> None:
        self._profiles = self.manager.list_profiles()
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self.profile_combo.addItem("New SOP", None)
        for p in self._profiles:
            self.profile_combo.addItem(p.get("name", ""), int(p.get("id")))
        self.profile_combo.blockSignals(False)

        if select_id:
            for i in range(self.profile_combo.count()):
                if self.profile_combo.itemData(i) == select_id:
                    self.profile_combo.setCurrentIndex(i)
                    self._on_profile_selected(i)
                    return
        self.profile_combo.setCurrentIndex(0)
        self._new_profile()

    def _on_profile_selected(self, idx: int) -> None:
        if self._loading_ui:
            return
        profile_id = self.profile_combo.itemData(idx)
        if not profile_id:
            self._new_profile()
            return
        profile = self.manager.get_profile(int(profile_id))
        if not profile:
            self._new_profile()
            return
        self._selected_profile_id = int(profile["id"])
        self._loading_ui = True
        try:
            self.name_edit.setText(profile.get("name", ""))
            self.group_combo.setCurrentText(profile.get("operating_group", ""))
            self.secondary_combo.setCurrentText(profile.get("secondary_group", ""))
            self.start_edit.setText(self._display_start_hhmm_from_utc(profile.get("sop_start_utc", "00:00")))
            self.active_cb.setChecked(bool(profile.get("active")))
            self._populate_actions(profile.get("actions", []))
            self._refresh_contact_label()
        finally:
            self._loading_ui = False
        self._set_save_dirty(False)
        self.refresh_upcoming()

    def _new_profile(self) -> None:
        self._selected_profile_id = None
        self._loading_ui = True
        try:
            self.name_edit.setText("")
            if self.group_combo.count() > 0:
                self.group_combo.setCurrentIndex(0)
            if self.secondary_combo.count() > 0:
                self.secondary_combo.setCurrentIndex(0)
            self.start_edit.setText(self._display_start_hhmm_from_utc("00:00"))
            self.active_cb.setChecked(False)
            self._populate_actions([])
            self._refresh_contact_label()
        finally:
            self._loading_ui = False
        self._set_save_dirty(False)
        self.refresh_upcoming()

    def _on_group_changed(self) -> None:
        if self._loading_ui:
            return
        self._refresh_all_row_group_options()
        self._refresh_contact_label()

    def _on_secondary_group_changed(self) -> None:
        if self._loading_ui:
            return
        self._refresh_all_contact_target_options()
        self._refresh_contact_label()

    def _refresh_all_row_group_options(self) -> None:
        group = self.group_combo.currentText().strip().upper()
        band_opts = self._band_options_for_group(group)
        for r in range(self.actions_table.rowCount()):
            band_combo = self.actions_table.cellWidget(r, self.COL_BAND)
            freq_combo = self.actions_table.cellWidget(r, self.COL_FREQ)
            if isinstance(band_combo, QComboBox):
                current = band_combo.currentText().strip()
                band_combo.blockSignals(True)
                band_combo.clear()
                band_combo.addItem("")
                for v in band_opts:
                    band_combo.addItem(v)
                band_combo.setCurrentText(current)
                band_combo.blockSignals(False)
            if isinstance(freq_combo, QComboBox):
                selected_band = band_combo.currentText().strip() if isinstance(band_combo, QComboBox) else ""
                freq_opts = self._frequency_options_for_group_band(group, selected_band)
                current = freq_combo.currentText().strip()
                freq_combo.blockSignals(True)
                freq_combo.clear()
                freq_combo.addItem("")
                for v in freq_opts:
                    freq_combo.addItem(v)
                freq_combo.setCurrentText(current)
                freq_combo.blockSignals(False)
        self._refresh_all_contact_target_options()

    def _available_callsign_targets(self) -> List[str]:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        return self.manager.resolve_group_callsigns(group, subgroup)

    def _contact_rule_options_for_current_filter(self) -> List[Tuple[str, str]]:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        contacts = self.manager.resolve_primary_contacts(group, subgroup)
        out: List[Tuple[str, str]] = [("none", "None")]
        if contacts.get("hub"):
            out.append(("hub_or_hub_alt", "HUB OR HUB-ALT"))
        if contacts.get("ncs"):
            out.append(("ncs_or_ancs", "NCS OR ANCS"))
        if contacts.get("peer"):
            out.append(("peer", "PEER"))
        out.append(("callsign", "CallSign"))
        return out

    def _refresh_all_contact_target_options(self) -> None:
        for r in range(self.actions_table.rowCount()):
            self._refresh_contact_rule_options_for_row(r)
            self._on_contact_rule_changed(r)

    def _refresh_contact_rule_options_for_row(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        rule_combo = self.actions_table.cellWidget(row, self.COL_CONTACT)
        if not isinstance(rule_combo, QComboBox):
            return
        current = str(rule_combo.currentData() or "none").strip()
        opts = self._contact_rule_options_for_current_filter()
        rule_combo.blockSignals(True)
        rule_combo.clear()
        for code, txt in opts:
            rule_combo.addItem(txt, code)
        idx = rule_combo.findData(current)
        if idx >= 0:
            rule_combo.setCurrentIndex(idx)
        else:
            idx_none = rule_combo.findData("none")
            rule_combo.setCurrentIndex(idx_none if idx_none >= 0 else 0)
        self._fit_combo_popup(rule_combo)
        rule_combo.blockSignals(False)

    def _update_hidden_actions_label(self) -> None:
        configured = self._configured_softwares()
        if not configured:
            self.add_row_btn.setEnabled(False)
            self.hidden_rows_label.setText("No software configured in Settings. Configure JS8/VarAC/FLDigi first.")
            return
        self.add_row_btn.setEnabled(True)
        if self._hidden_actions:
            self.hidden_rows_label.setText(
                f"{len(self._hidden_actions)} row(s) hidden (software not configured). Stored and preserved."
            )
        else:
            self.hidden_rows_label.setText("")

    def _autosize_actions_table(self) -> None:
        try:
            for col in (
                self.COL_BAND,
                self.COL_FREQ,
                self.COL_SOFTWARE,
                self.COL_ACTION,
                self.COL_INTERVAL,
                self.COL_CONTACT,
                self.COL_REMOVE,
            ):
                self.actions_table.resizeColumnToContents(col)
            if self.actions_table.columnWidth(self.COL_CONTACT_TARGET) < 170:
                self.actions_table.setColumnWidth(self.COL_CONTACT_TARGET, 170)
            if self.actions_table.columnWidth(self.COL_DESC) < 260:
                self.actions_table.setColumnWidth(self.COL_DESC, 260)
        except Exception:
            pass

    def _populate_actions(self, existing: List[Dict[str, Any]]) -> None:
        self.actions_table.setRowCount(0)
        self._hidden_actions = []

        configured = set(self._configured_softwares())
        ordered = sorted(existing, key=lambda x: int(x.get("sort_order") or 0))
        for row in ordered:
            sw = (row.get("software") or "").strip()
            if sw not in configured:
                self._hidden_actions.append(dict(row))
                continue
            self._add_action_row(existing=row)

        if self.actions_table.rowCount() == 0 and configured:
            self._add_action_row(existing=None)

        self._update_hidden_actions_label()
        self._autosize_actions_table()

    def _make_band_widget(self, value: str) -> QComboBox:
        combo = QComboBox()
        combo.setEditable(True)
        combo.setMinimumContentsLength(4)
        combo.setMinimumWidth(84)
        combo.addItem("")
        for b in self._band_options_for_group(self.group_combo.currentText()):
            combo.addItem(b)
        combo.setCurrentText((value or "").strip().upper())
        self._fit_combo_popup(combo)
        return combo

    def _make_freq_widget(self, value: str) -> QComboBox:
        combo = QComboBox()
        combo.setEditable(True)
        combo.addItem("")
        for f in self._frequency_options_for_group_band(self.group_combo.currentText(), ""):
            combo.addItem(f)
        combo.setCurrentText((value or "").strip())
        self._fit_combo_popup(combo)
        return combo

    def _make_software_widget(self, value: str) -> QComboBox:
        combo = QComboBox()
        for sw in self._configured_softwares():
            combo.addItem(sw)
        if value and combo.findText(value) < 0:
            combo.addItem(value)
        if value:
            combo.setCurrentText(value)
        self._fit_combo_popup(combo)
        return combo

    def _apply_typeahead(self, combo: QComboBox) -> None:
        try:
            completer = QCompleter(combo.model(), combo)
            completer.setCaseSensitivity(Qt.CaseInsensitive)
            completer.setFilterMode(Qt.MatchContains)
            completer.setCompletionMode(QCompleter.PopupCompletion)
            combo.setCompleter(completer)
        except Exception:
            pass

    def _tz_short_name(self) -> str:
        tz_name = str(self.settings.get("timezone", "UTC") or "UTC")
        upper = tz_name.upper()
        if "UTC" in upper:
            return "UTC"
        if "NEW_YORK" in upper or "EASTERN" in upper:
            return "ET"
        if "CHICAGO" in upper or "CENTRAL" in upper:
            return "CT"
        if "DENVER" in upper or "MOUNTAIN" in upper:
            return "MT"
        if "LOS_ANGELES" in upper or "PACIFIC" in upper:
            return "PT"
        try:
            tz = get_timezone(tz_name)
            now = datetime.datetime.now(datetime.timezone.utc).astimezone(tz)
            return now.tzname() or "Local"
        except Exception:
            return "Local"

    @staticmethod
    def _format_interval_hhmm(minutes: int) -> str:
        m = max(1, int(minutes))
        return f"{m // 60:02d}:{m % 60:02d}"

    def _parse_interval_minutes(self, text: str) -> int:
        raw = (text or "").strip().lower()
        if not raw:
            raise ValueError("Interval is required.")
        if raw.endswith("m"):
            return max(1, int(float(raw[:-1].strip())))
        if raw.endswith("h"):
            return max(1, int(round(float(raw[:-1].strip()) * 60)))
        if ":" in raw:
            hh, mm = raw.split(":", 1)
            total = (int(hh.strip() or "0") * 60) + int(mm.strip() or "0")
            if total <= 0:
                raise ValueError("Interval must be greater than 00:00.")
            return total
        if raw.isdigit() and len(raw) == 4:
            total = (int(raw[:2]) * 60) + int(raw[2:])
            if total <= 0:
                raise ValueError("Interval must be greater than 00:00.")
            return total
        if raw.isdigit():
            return max(1, int(raw))
        if "." in raw:
            return max(1, int(round(float(raw) * 60)))
        raise ValueError(f"Invalid interval: {text}")

    def _refresh_action_combo_for_row(
        self,
        row: int,
        preferred_key: str | None = None,
        keep_current: bool = True,
    ) -> None:
        sw_combo = self.actions_table.cellWidget(row, self.COL_SOFTWARE)
        action_combo = self.actions_table.cellWidget(row, self.COL_ACTION)
        if not isinstance(sw_combo, QComboBox) or not isinstance(action_combo, QComboBox):
            return
        software = sw_combo.currentText().strip()
        preferred = preferred_key
        if preferred is None and keep_current:
            preferred = action_combo.currentData() if action_combo.count() else ""
        preferred = str(preferred or "").strip()
        catalog = self._action_catalog().get(software, [])
        action_combo.blockSignals(True)
        action_combo.clear()
        has_spotter = any(key.startswith("js8_spotter_") for key, _label in catalog)
        inserted_spotter_header = False
        for key, label in catalog:
            if key.startswith("js8_spotter_") and not inserted_spotter_header:
                action_combo.addItem("Spotter", "__spotter_header__")
                inserted_spotter_header = True
            action_combo.addItem(label, key)
        if has_spotter:
            model = action_combo.model()
            for idx in range(action_combo.count()):
                if action_combo.itemData(idx) == "__spotter_header__":
                    item = model.item(idx)
                    if item is not None:
                        item.setEnabled(False)
                    break
        if preferred and action_combo.findData(preferred) < 0 and keep_current:
            action_combo.addItem(preferred, preferred)
        idx = action_combo.findData(preferred)
        if idx >= 0:
            action_combo.setCurrentIndex(idx)
        elif action_combo.count() > 0:
            action_combo.setCurrentIndex(0)
        self._fit_combo_popup(action_combo)
        action_combo.blockSignals(False)

    def _add_action_row(self, existing: Dict[str, Any] | None) -> None:
        row = self.actions_table.rowCount()
        self.actions_table.insertRow(row)

        band_combo = self._make_band_widget((existing or {}).get("band", ""))
        self.actions_table.setCellWidget(row, self.COL_BAND, band_combo)

        freq_combo = self._make_freq_widget((existing or {}).get("frequency", ""))
        self.actions_table.setCellWidget(row, self.COL_FREQ, freq_combo)
        band_combo.currentIndexChanged.connect(lambda _=0, r=row: self._refresh_freq_combo_for_row(r))
        band_combo.currentTextChanged.connect(lambda _=None, r=row: self._refresh_freq_combo_for_row(r))
        self._refresh_freq_combo_for_row(row)

        sw_combo = self._make_software_widget((existing or {}).get("software", ""))
        sw_combo.setProperty("action_id", int((existing or {}).get("id") or 0))
        sw_combo.setProperty("sort_order", int((existing or {}).get("sort_order") or row))
        sw_combo.currentIndexChanged.connect(
            lambda _=0, r=row: self._refresh_action_combo_for_row(r, preferred_key=None, keep_current=False)
        )
        self.actions_table.setCellWidget(row, self.COL_SOFTWARE, sw_combo)

        action_combo = QComboBox()
        self.actions_table.setCellWidget(row, self.COL_ACTION, action_combo)
        self._refresh_action_combo_for_row(row, (existing or {}).get("action_key", ""))

        interval_combo = QComboBox()
        interval_combo.setEditable(True)
        for preset in self.INTERVAL_PRESETS:
            interval_combo.addItem(preset, preset)
        interval_minutes = int((existing or {}).get("interval_minutes") or 0)
        if interval_minutes <= 0:
            interval_minutes = int((existing or {}).get("interval_hours") or 3) * 60
        interval_txt = self._format_interval_hhmm(interval_minutes)
        if interval_combo.findText(interval_txt) < 0:
            interval_combo.addItem(interval_txt, interval_txt)
        interval_combo.setCurrentText(interval_txt)
        interval_combo.setToolTip("Examples: 00:45, 90m, 1.5h, 0130")
        if interval_combo.lineEdit() is not None:
            interval_combo.lineEdit().setPlaceholderText("type or select...")
        self._fit_combo_popup(interval_combo)
        self.actions_table.setCellWidget(row, self.COL_INTERVAL, interval_combo)

        rule_combo = QComboBox()
        for code, txt in self._contact_rule_options_for_current_filter():
            rule_combo.addItem(txt, code)
        rule = ((existing or {}).get("contact_rule") or "none").strip()
        idx_rule = rule_combo.findData(rule)
        rule_combo.setCurrentIndex(idx_rule if idx_rule >= 0 else 0)
        self._fit_combo_popup(rule_combo)
        self.actions_table.setCellWidget(row, self.COL_CONTACT, rule_combo)

        target_combo = QComboBox()
        target_combo.setEditable(True)
        target_combo.setInsertPolicy(QComboBox.NoInsert)
        target_combo.setMaxVisibleItems(12)
        self._apply_typeahead(target_combo)
        target_combo.setProperty("saved_target", ((existing or {}).get("contact_target") or "").strip().upper())
        self.actions_table.setCellWidget(row, self.COL_CONTACT_TARGET, target_combo)
        self._refresh_contact_target_options_for_row(row)
        rule_combo.currentIndexChanged.connect(lambda _=0, r=row: self._on_contact_rule_changed(r))
        self._on_contact_rule_changed(row)

        desc_edit = QLineEdit((existing or {}).get("description", ""))
        desc_edit.setPlaceholderText("Optional description for reminder meaning")
        self.actions_table.setCellWidget(row, self.COL_DESC, desc_edit)

        remove_btn = QPushButton("Remove")
        remove_btn.clicked.connect(lambda _=False, b=remove_btn: self._remove_row_for_button(b))
        self.actions_table.setCellWidget(row, self.COL_REMOVE, remove_btn)
        band_combo.currentIndexChanged.connect(self._mark_dirty)
        band_combo.currentTextChanged.connect(self._mark_dirty)
        freq_combo.currentIndexChanged.connect(self._mark_dirty)
        freq_combo.currentTextChanged.connect(self._mark_dirty)
        sw_combo.currentIndexChanged.connect(self._mark_dirty)
        action_combo.currentIndexChanged.connect(self._mark_dirty)
        interval_combo.currentIndexChanged.connect(self._mark_dirty)
        if interval_combo.lineEdit() is not None:
            interval_combo.lineEdit().textChanged.connect(self._mark_dirty)
        rule_combo.currentIndexChanged.connect(self._mark_dirty)
        target_combo.currentIndexChanged.connect(self._mark_dirty)
        target_combo.currentTextChanged.connect(self._mark_dirty)
        desc_edit.textChanged.connect(self._mark_dirty)
        self._mark_dirty()
        self._autosize_actions_table()

    def _remove_row_for_button(self, btn: QPushButton) -> None:
        for r in range(self.actions_table.rowCount()):
            if self.actions_table.cellWidget(r, self.COL_REMOVE) is btn:
                self.actions_table.removeRow(r)
                self._mark_dirty()
                self._autosize_actions_table()
                break

    def _refresh_freq_combo_for_row(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        group = self.group_combo.currentText().strip().upper()
        band_combo = self.actions_table.cellWidget(row, self.COL_BAND)
        freq_combo = self.actions_table.cellWidget(row, self.COL_FREQ)
        if not isinstance(band_combo, QComboBox) or not isinstance(freq_combo, QComboBox):
            return
        selected_band = band_combo.currentText().strip().upper()
        options = self._frequency_options_for_group_band(group, selected_band)
        current = freq_combo.currentText().strip()
        freq_combo.blockSignals(True)
        freq_combo.clear()
        freq_combo.addItem("")
        for val in options:
            freq_combo.addItem(val)
        if not current and len(options) == 1:
            freq_combo.setCurrentText(options[0])
        else:
            freq_combo.setCurrentText(current)
        self._fit_combo_popup(freq_combo)
        freq_combo.blockSignals(False)

    def _role_targets_for_rule(self, rule: str) -> List[str]:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        contacts = self.manager.resolve_primary_contacts(group, subgroup)
        if rule == "hub_or_hub_alt":
            return contacts.get("hub", []) or []
        if rule == "ncs_or_ancs":
            return contacts.get("ncs", []) or []
        if rule == "peer":
            return contacts.get("peer", []) or []
        return []

    def _refresh_contact_target_options_for_row(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        target_combo = self.actions_table.cellWidget(row, self.COL_CONTACT_TARGET)
        if not isinstance(target_combo, QComboBox):
            return
        current = target_combo.currentText().strip().upper() or str(target_combo.property("saved_target") or "").strip().upper()
        opts = self._available_callsign_targets()
        target_combo.blockSignals(True)
        target_combo.clear()
        target_combo.addItem("")
        for cs in opts:
            target_combo.addItem(cs, cs)
        if current and target_combo.findText(current) < 0:
            target_combo.addItem(current, current)
        target_combo.setCurrentText(current)
        self._fit_combo_popup(target_combo)
        target_combo.blockSignals(False)

    def _on_contact_rule_changed(self, row: int) -> None:
        if row < 0 or row >= self.actions_table.rowCount():
            return
        rule_combo = self.actions_table.cellWidget(row, self.COL_CONTACT)
        target_combo = self.actions_table.cellWidget(row, self.COL_CONTACT_TARGET)
        if not isinstance(rule_combo, QComboBox) or not isinstance(target_combo, QComboBox):
            return
        rule = str(rule_combo.currentData() or "none").strip()
        saved_target = str(target_combo.property("saved_target") or "").strip().upper()
        target_combo.blockSignals(True)
        target_combo.clear()
        target_combo.setEditable(True)
        target_combo.setEnabled(True)

        if rule in {"hub_or_hub_alt", "ncs_or_ancs"}:
            target_combo.addItem("Any (Role Match)", self.ANY_ROLE_TOKEN)
            for cs in self._role_targets_for_rule(rule):
                target_combo.addItem(cs, cs)
            chosen = saved_target if saved_target else self.ANY_ROLE_TOKEN
            idx = target_combo.findData(chosen)
            target_combo.setCurrentIndex(idx if idx >= 0 else 0)
            target_combo.setEnabled(True)
            target_combo.setEditable(False)
        elif rule == "peer":
            target_combo.addItem("", "")
            for cs in self._role_targets_for_rule("peer"):
                target_combo.addItem(cs, cs)
            if target_combo.lineEdit() is not None:
                target_combo.lineEdit().setPlaceholderText("type or select...")
            if saved_target:
                idx = target_combo.findData(saved_target)
                target_combo.setCurrentIndex(idx if idx >= 0 else 0)
            else:
                target_combo.setCurrentIndex(0)
            target_combo.setEnabled(True)
            target_combo.setEditable(True)
        elif rule == "callsign":
            target_combo.addItem("", "")
            for cs in self._available_callsign_targets():
                target_combo.addItem(cs, cs)
            if target_combo.lineEdit() is not None:
                target_combo.lineEdit().setPlaceholderText("type or select...")
            if saved_target:
                idx = target_combo.findData(saved_target)
                target_combo.setCurrentIndex(idx if idx >= 0 else 0)
            else:
                target_combo.setCurrentIndex(0)
            target_combo.setEnabled(True)
            target_combo.setEditable(True)
        else:
            target_combo.addItem("", "")
            target_combo.setCurrentIndex(0)
            target_combo.setEnabled(False)
            target_combo.setEditable(False)
        self._fit_combo_popup(target_combo)
        target_combo.blockSignals(False)
        self._autosize_actions_table()

    def _fit_combo_popup(self, combo: QComboBox) -> None:
        try:
            fm = QFontMetrics(combo.font())
            text_w = 0
            for i in range(combo.count()):
                text_w = max(text_w, fm.horizontalAdvance(combo.itemText(i)))
            popup_w = max(combo.width(), text_w + 44)
            view = combo.view()
            if view is not None:
                view.setMinimumWidth(popup_w)
        except Exception:
            pass

    def _collect_profile_payload(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        name = self.name_edit.text().strip()
        if not name:
            raise ValueError("SOP name is required.")
        group = self.group_combo.currentText().strip().upper()
        if not group:
            raise ValueError("Operating group is required.")
        start = self.start_edit.text().strip()
        if len(start) != 5 or ":" not in start:
            raise ValueError("SOP Start Time must be HH:MM.")
        start_utc = self._utc_start_hhmm_from_display(start)

        payload = {
            "id": self._selected_profile_id,
            "name": name,
            "operating_group": group,
            "secondary_group": self.secondary_combo.currentText().strip().upper(),
            "frequency": "",
            "sop_start_utc": start_utc,
            "active": self.active_cb.isChecked(),
            "window_hours": int(self.horizon_spin.value()),
        }

        actions: List[Dict[str, Any]] = []
        for r in range(self.actions_table.rowCount()):
            band_combo = self.actions_table.cellWidget(r, self.COL_BAND)
            freq_combo = self.actions_table.cellWidget(r, self.COL_FREQ)
            sw_combo = self.actions_table.cellWidget(r, self.COL_SOFTWARE)
            action_combo = self.actions_table.cellWidget(r, self.COL_ACTION)
            interval_combo = self.actions_table.cellWidget(r, self.COL_INTERVAL)
            rule_combo = self.actions_table.cellWidget(r, self.COL_CONTACT)
            target_combo = self.actions_table.cellWidget(r, self.COL_CONTACT_TARGET)
            desc_edit = self.actions_table.cellWidget(r, self.COL_DESC)

            if not isinstance(sw_combo, QComboBox) or not isinstance(action_combo, QComboBox):
                continue

            software = sw_combo.currentText().strip()
            action_key = str(action_combo.currentData() or "").strip()
            action_label = action_combo.currentText().strip()
            if not software or not action_key or action_key == "__spotter_header__":
                continue
            contact_rule = rule_combo.currentData() if isinstance(rule_combo, QComboBox) else "none"
            contact_target = ""
            if isinstance(target_combo, QComboBox):
                if str(contact_rule) in {"hub_or_hub_alt", "ncs_or_ancs"}:
                    contact_target = str(target_combo.currentData() or "").strip().upper()
                elif str(contact_rule) in {"callsign", "peer"}:
                    contact_target = str(target_combo.currentData() or target_combo.currentText() or "").strip().upper()
            if str(contact_rule) != "none" and not contact_target:
                raise ValueError(f"Contact Target is required on row {r + 1}.")
            interval_minutes = (
                self._parse_interval_minutes(interval_combo.currentText())
                if isinstance(interval_combo, QComboBox)
                else 180
            )

            actions.append(
                {
                    "id": int(sw_combo.property("action_id") or 0),
                    "band": band_combo.currentText().strip().upper() if isinstance(band_combo, QComboBox) else "",
                    "frequency": freq_combo.currentText().strip() if isinstance(freq_combo, QComboBox) else "",
                    "software": software,
                    "action_key": action_key,
                    "action_label": action_label,
                    "enabled": True,
                    "interval_minutes": interval_minutes,
                    "interval_hours": max(1, int((interval_minutes + 59) // 60)),
                    "description": desc_edit.text().strip() if isinstance(desc_edit, QLineEdit) else "",
                    "contact_rule": contact_rule,
                    "contact_target": contact_target,
                    "sort_order": int(sw_combo.property("sort_order") or r),
                }
            )

        for i, hidden in enumerate(self._hidden_actions):
            preserved = dict(hidden)
            preserved["sort_order"] = int(
                preserved.get("sort_order") if preserved.get("sort_order") is not None else len(actions) + i
            )
            actions.append(preserved)

        if not actions:
            raise ValueError("Add at least one action row.")

        return payload, actions

    def _save_profile(self) -> None:
        try:
            payload, actions = self._collect_profile_payload()
            profile_id = self.manager.save_profile(payload, actions)
            self._reload_profiles(select_id=profile_id)
            self._set_save_dirty(False)
            self.refresh_upcoming()
            QMessageBox.information(self, "SOP", "SOP saved.")
        except Exception as e:
            QMessageBox.warning(self, "SOP", str(e))

    def _delete_profile(self) -> None:
        if not self._selected_profile_id:
            return
        resp = QMessageBox.question(self, "Delete SOP", "Delete this SOP profile?")
        if resp != QMessageBox.Yes:
            return
        self.manager.delete_profile(int(self._selected_profile_id))
        self._reload_profiles(select_id=None)
        self.refresh_upcoming()

    def _export_profile(self) -> None:
        if not self._selected_profile_id:
            QMessageBox.information(self, "Export SOP", "Save the SOP first before exporting.")
            return
        try:
            payload = self.manager.export_profile_json(int(self._selected_profile_id))
            out, _ = QFileDialog.getSaveFileName(
                self,
                "Export SOP",
                f"{payload['profile'].get('name', 'sop')}.json",
                "JSON Files (*.json)",
            )
            if not out:
                return
            Path(out).write_text(self.manager.dumps_json(payload), encoding="utf-8")
            QMessageBox.information(self, "Export SOP", f"Exported SOP to:\n{out}")
        except Exception as e:
            QMessageBox.warning(self, "Export SOP", str(e))

    def _import_profile(self) -> None:
        try:
            src, _ = QFileDialog.getOpenFileName(self, "Import SOP", "", "JSON Files (*.json)")
            if not src:
                return
            payload = json.loads(Path(src).read_text(encoding="utf-8"))
            profile_id = self.manager.import_profile_json(payload)
            self._reload_profiles(select_id=profile_id)
            self.refresh_upcoming()
            QMessageBox.information(self, "Import SOP", "SOP imported.")
        except Exception as e:
            QMessageBox.warning(self, "Import SOP", str(e))

    def _refresh_contact_label(self) -> None:
        group = self.group_combo.currentText().strip().upper()
        subgroup = self.secondary_combo.currentText().strip().upper()
        contacts = self.manager.resolve_primary_contacts(group, subgroup)
        hub = contacts.get("hub", [])
        ncs = contacts.get("ncs", [])
        parts = []
        if hub:
            parts.append(f"HUB/HUB-ALT: {', '.join(hub[:6])}")
        if ncs:
            parts.append(f"NCS/ANCS: {', '.join(ncs[:6])}")
        if parts:
            self.contact_label.setText(f"Primary Contacts ({group or 'N/A'}): " + " | ".join(parts))
        else:
            self.contact_label.setText(f"Primary Contacts ({group or 'N/A'}): None")

    def _format_due(self, due_utc, tz_mode: str) -> str:
        if tz_mode == "Local":
            tz_name = self.settings.get("timezone", "UTC")
            tz = get_timezone(tz_name)
            return due_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
        return due_utc.strftime("%Y-%m-%d %H:%M")

    def _update_clock_labels(self) -> None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_day = now_utc.strftime("%a").upper()
        self.utc_label.setText(now_utc.strftime(f"<b>UTC ({utc_day}):</b> %y%m%d %H:%M:%S Z"))
        tz_name = self.settings.get("timezone", "UTC") or "UTC"
        try:
            local_dt = now_utc.astimezone(get_timezone(tz_name))
            local_day = local_dt.strftime("%a").upper()
            self.local_label.setText(local_dt.strftime(f"<b>{tz_name} ({local_day}):</b> %y%m%d %H:%M:%S"))
        except Exception:
            self.local_label.setText("<b>Local:</b> --")
        self.time_toggle_btn.setText("Showing: Local" if self._show_local else "Showing: UTC")
        tz_short = self._tz_short_name() if self._show_local else "UTC"
        self.start_label.setText(f"SOP Daily Start ({tz_short}):")

    def _toggle_time_view(self) -> None:
        prev_show_local = self._show_local
        prior_text = self.start_edit.text().strip()
        prior_utc = self._utc_start_hhmm_from_display(prior_text, show_local=prev_show_local)
        self._show_local = not self._show_local
        self.start_edit.setText(self._display_start_hhmm_from_utc(prior_utc))
        self._update_clock_labels()
        self.refresh_upcoming()

    def _display_start_hhmm_from_utc(self, utc_hhmm: str) -> str:
        text = (utc_hhmm or "00:00").strip()
        if len(text) != 5 or ":" not in text:
            return "00:00"
        if not self._show_local:
            return text
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            h, m = [int(x) for x in text.split(":")]
            dt_utc = now_utc.replace(hour=h, minute=m, second=0, microsecond=0)
            return dt_utc.astimezone(tz).strftime("%H:%M")
        except Exception:
            return text

    def _utc_start_hhmm_from_display(self, display_hhmm: str, show_local: bool | None = None) -> str:
        text = (display_hhmm or "00:00").strip()
        if len(text) != 5 or ":" not in text:
            return "00:00"
        use_local = self._show_local if show_local is None else bool(show_local)
        if not use_local:
            return text
        try:
            tz_name = self.settings.get("timezone", "UTC") or "UTC"
            tz = get_timezone(tz_name)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_local = now_utc.astimezone(tz)
            h, m = [int(x) for x in text.split(":")]
            dt_local = now_local.replace(hour=h, minute=m, second=0, microsecond=0)
            return dt_local.astimezone(datetime.timezone.utc).strftime("%H:%M")
        except Exception:
            return text

    def refresh_upcoming(self) -> None:
        try:
            horizon = int(self.horizon_spin.value())
            rows = self.manager.build_upcoming_actions(horizon_hours=horizon, only_active=True)
            configured = set(self._configured_softwares())
            if configured:
                rows = [r for r in rows if (r.get("software") or "").strip() in configured]
            else:
                rows = []
            self._upcoming_rows = rows
            self._populate_upcoming_table()
        except Exception as e:
            log.debug("SOP: refresh_upcoming failed: %s", e)

    def _populate_upcoming_table(self) -> None:
        tz_mode = "Local" if self._show_local else "UTC"
        theme = resolve_theme(self.settings)
        self.upcoming_table.setRowCount(0)
        misaligned = 0
        for row in self._upcoming_rows:
            r = self.upcoming_table.rowCount()
            self.upcoming_table.insertRow(r)
            self.upcoming_table.setItem(r, 0, QTableWidgetItem(row.get("profile_name", "")))

            band = (row.get("band") or "").strip()
            freq = (row.get("frequency") or "").strip()
            band_freq = f"{band} {freq}".strip() if (band or freq) else "--"
            band_item = QTableWidgetItem(band_freq)
            aligned = bool(row.get("aligned", True))
            if not aligned:
                warn_bg = theme.get("warning", "#C99700")
                band_item.setBackground(QColor(warn_bg))
                band_item.setForeground(QColor(_contrast_text_hex(warn_bg)))
                band_item.setToolTip("Scheduling Mismatch")
                misaligned += 1
            self.upcoming_table.setItem(r, 1, band_item)

            self.upcoming_table.setItem(r, 2, QTableWidgetItem(row.get("software", "")))
            self.upcoming_table.setItem(r, 3, QTableWidgetItem(row.get("action_label", "")))
            self.upcoming_table.setItem(r, 4, QTableWidgetItem(row.get("description", "")))
            self.upcoming_table.setItem(r, 5, QTableWidgetItem(self._format_due(row.get("next_due_utc"), tz_mode)))
            targets = row.get("contact_targets", []) or []
            if targets:
                contact_txt = " OR ".join(targets[:4])
            else:
                contact_txt = "--"
            self.upcoming_table.setItem(r, 6, QTableWidgetItem(contact_txt))

            btn = QPushButton("Complete")
            pid = int(row.get("profile_id"))
            aid = int(row.get("action_id"))
            status = (row.get("status") or "").strip()
            if status == "Due Now":
                btn.setText("Due Now")
                btn.setStyleSheet(button_style("info", theme))
            elif status == "Overdue":
                btn.setText(status)
                btn.setStyleSheet(button_style("warning", theme))
            elif status == "Completed":
                btn.setText("Completed")
                btn.setStyleSheet(button_style("success", theme))
            else:
                btn.setText(status or "Upcoming")
            btn.clicked.connect(lambda _=False, p=pid, a=aid: self._complete_action(p, a))
            self.upcoming_table.setCellWidget(r, 7, btn)

        if misaligned > 0:
            self.alignment_label.setText(
                f"Warning: {misaligned} upcoming SOP check-in reminder(s) do not align with Daily/Net schedule windows."
            )
            self.alignment_label.setVisible(True)
        else:
            self.alignment_label.setVisible(False)

    def _complete_action(self, profile_id: int, action_id: int) -> None:
        try:
            self.manager.mark_action_complete(profile_id, action_id)
            self.refresh_upcoming()
        except Exception as e:
            QMessageBox.warning(self, "SOP", f"Could not complete action: {e}")

    def on_settings_saved(self) -> None:
        try:
            self.settings.reload()
        except Exception:
            pass
        current_group = self.group_combo.currentText()
        current_start_utc = self._utc_start_hhmm_from_display(self.start_edit.text().strip())
        self._refresh_reference_data()
        self.group_combo.setCurrentText(current_group)
        self.start_edit.setText(self._display_start_hhmm_from_utc(current_start_utc))
        self._update_clock_labels()
        self._refresh_contact_label()
        self._on_profile_selected(self.profile_combo.currentIndex())
        self.refresh_upcoming()

    def on_tab_activated(self) -> None:
        self.refresh_upcoming()

    def apply_theme(self) -> None:
        try:
            theme = resolve_theme(self.settings)
            self.alignment_label.setStyleSheet(f"color: {theme.get('warning', '#B71C1C')}; font-weight: 600;")
            self.time_toggle_btn.setStyleSheet(button_style("primary", theme))
        except Exception:
            pass
