"""Primary, lazy FIO Spotter service workspace.

This surface intentionally consumes the established projection, Expect, form and
import stores.  It does not maintain a second Spotter catalog in the UI.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
import time
from typing import Any, Callable

from PySide6.QtCore import Qt, QTimer, Signal, QStringListModel
from PySide6.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QFormLayout, QGridLayout, QGroupBox,
    QCompleter, QFrame, QHBoxLayout, QHeaderView, QLabel, QLineEdit, QPushButton, QFileDialog, QMessageBox,
    QScrollArea, QSizePolicy, QSplitter, QSpinBox, QTabWidget, QTableWidget, QTableWidgetItem, QTextEdit,
    QVBoxLayout, QWidget,
)

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.fio_spotter_store import (
    MATCH_MODES, PRIORITIES, WATCH_KINDS, delete_spotter_watch,
    list_spotter_activity, list_spotter_watches, save_spotter_watch, watch_matches,
)
from freqinout.core.js8_expect_dispatcher import list_expect_dispatch_audit
from freqinout.core.js8_expect_runtime import (
    load_expect_automation_runtime_state, set_expect_automation_runtime_state,
)
from freqinout.core.js8_expect_store import (
    default_expect_db_path,
    delete_expect_allow_policy, delete_expect_entry, list_expect_allow_policies, list_expect_entries,
    list_expect_operator_access_catalog, list_expect_runtime_audit,
    save_expect_allow_policy, save_expect_entry,
)
from freqinout.core.js8_spotter_forms import (
    MAPPER_SETTINGS_KEY,
    PURPOSE_OPTIONS,
    discover_spotter_forms,
    effective_mapping_rows,
    factory_mapping_for_form,
    normalize_mapping_row,
)
from freqinout.core.js8spotter_importer import import_js8spotter_database, preview_js8spotter_import
from freqinout.core.perf_metrics import emit_span
from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.traffic_actionability import (
    TrafficActionSummary,
    build_operator_traffic_context,
    build_traffic_action_summary,
    configured_group_names,
    load_operator_traffic_context,
    message_matches_traffic_bucket,
    traffic_action_item,
)
from freqinout.core.varac_bbs_vault import list_flamp_transfer_index_statuses
from freqinout.gui.traffic_action_summary_widget import TrafficActionSummaryWidget


_MAX_ROWS = 200
_TAB_NAMES = ("Activity", "Watches", "Expect", "Forms", "Imports")


def _csv(value: object) -> list[str]:
    return [part.strip().upper() for part in str(value or "").split(",") if part.strip()]


def _text(value: object) -> str:
    return str(value or "").strip()


def _when(value: object) -> str:
    try:
        stamp = float(value or 0)
    except (TypeError, ValueError):
        return "—"
    if stamp <= 0:
        return "—"
    return dt.datetime.fromtimestamp(stamp).strftime("%Y-%m-%d %H:%M")


class _TokenListEditor(QWidget):
    """Visible, de-duplicated tokens plus a lookup/custom-value editor."""

    valuesChanged = Signal()

    def __init__(self, parent: QWidget | None = None, *, token_prefix: str = "") -> None:
        super().__init__(parent)
        self._token_prefix = str(token_prefix or "").strip().upper()
        self._values: list[str] = []
        self._completion_values: list[str] = []
        self._chip_buttons: list[QPushButton] = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        self.chip_scroll = QScrollArea(self)
        self.chip_scroll.setObjectName("fioSpotterTokenChips")
        self.chip_scroll.setWidgetResizable(False)
        self.chip_scroll.setFrameShape(QFrame.NoFrame)
        self.chip_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.chip_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.chip_body = QWidget(self.chip_scroll)
        self.chip_layout = QHBoxLayout(self.chip_body)
        self.chip_layout.setContentsMargins(0, 0, 0, 0)
        self.chip_layout.setSpacing(4)
        self.chip_layout.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.chip_scroll.setWidget(self.chip_body)
        self.chip_scroll.setFixedHeight(38)
        self.chip_scroll.setVisible(False)
        layout.addWidget(self.chip_scroll)

        entry_row = QHBoxLayout()
        entry_row.setContentsMargins(0, 0, 0, 0)
        entry_row.setSpacing(4)
        self.input = QLineEdit(self)
        self.input.setAccessibleName("Value to add")
        self.add_button = QPushButton("Add", self)
        self.add_button.setAccessibleName("Add entered value")
        self.add_button.setToolTip("Add the lookup selection or custom value. Enter does the same thing.")
        entry_row.addWidget(self.input, 1)
        entry_row.addWidget(self.add_button)
        layout.addLayout(entry_row)

        self._completion_model = QStringListModel(self)
        self._token_completer = QCompleter(self._completion_model, self)
        self._token_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._token_completer.setFilterMode(Qt.MatchContains)
        self._token_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._token_completer.setMaxVisibleItems(14)
        self.input.setCompleter(self._token_completer)
        self._token_completer.activated[str].connect(self._insert_completion)
        self.input.textEdited.connect(self._complete_token)
        self.input.returnPressed.connect(self._add_current_input)
        self.add_button.clicked.connect(self._add_current_input)

    def set_completion_values(self, values: list[str]) -> None:
        self._completion_values = sorted({self._normalize(value) for value in values if self._normalize(value)})
        self._completion_model.setStringList(self._completion_values)

    def _normalize(self, value: object) -> str:
        token = str(value or "").strip().upper()
        if not token:
            return ""
        if self._token_prefix:
            token = f"{self._token_prefix}{token.lstrip(self._token_prefix)}"
        return token

    def setPlaceholderText(self, text: str) -> None:
        self.input.setPlaceholderText(text)

    def placeholderText(self) -> str:
        return self.input.placeholderText()

    def completer(self) -> QCompleter:
        return self._token_completer

    def setFocus(self, reason=Qt.OtherFocusReason) -> None:  # type: ignore[override]
        self.input.setFocus(reason)

    def text(self) -> str:
        return ", ".join(self._values)

    def setText(self, text: object) -> None:
        values: list[str] = []
        seen: set[str] = set()
        for part in str(text or "").split(","):
            value = self._normalize(part)
            if value and value not in seen:
                values.append(value)
                seen.add(value)
        self._values = values
        self.input.clear()
        self._rebuild_chips()

    def clear(self) -> None:
        self._values = []
        self.input.clear()
        self._rebuild_chips()

    def _complete_token(self, text: str) -> None:
        token = self._normalize(text)
        if not token:
            self._token_completer.popup().hide()
            return
        self._token_completer.setCompletionPrefix(token)
        if self._token_completer.completionCount():
            self._token_completer.complete()

    def _insert_completion(self, value: str) -> None:
        self._add_values(value)
        # QLineEdit applies a clicked QCompleter value after activated handlers
        # return on some Qt/platform combinations. Clear again on the next event
        # turn so the accepted lookup cannot remain in front of the next search.
        QTimer.singleShot(0, self, self._reset_entry_input)

    def _add_current_input(self) -> None:
        self._add_values(self.input.text())

    def _add_values(self, text: object) -> None:
        changed = False
        existing = set(self._values)
        for part in str(text or "").split(","):
            value = self._normalize(part)
            if value and value not in existing:
                self._values.append(value)
                existing.add(value)
                changed = True
        self._reset_entry_input()
        if changed:
            self._rebuild_chips()
        self.input.setFocus()

    def _reset_entry_input(self) -> None:
        self.input.clear()
        self._token_completer.setCompletionPrefix("")
        self._token_completer.popup().hide()

    def _remove_value(self, value: str) -> None:
        self._values = [item for item in self._values if item != value]
        self._rebuild_chips()

    def _rebuild_chips(self) -> None:
        self.setMinimumHeight(0)
        existing = {
            str(chip.property("tokenValue") or ""): chip
            for chip in self._chip_buttons
        }
        wanted = set(self._values)
        for value, chip in existing.items():
            if value not in wanted:
                self.chip_layout.removeWidget(chip)
                chip.hide()
                chip.deleteLater()

        next_buttons: list[QPushButton] = []
        for value in self._values:
            chip = existing.get(value)
            if chip is None:
                chip = QPushButton(f"{value}  ×", self.chip_body)
                chip.setObjectName("fioSpotterTokenChip")
                chip.setProperty("tokenValue", value)
                chip.setAccessibleName(f"Remove {value}")
                chip.setToolTip(f"Remove {value}")
                chip.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
                chip.clicked.connect(lambda _checked=False, selected=value: self._remove_value(selected))
            else:
                self.chip_layout.removeWidget(chip)
            self.chip_layout.addWidget(chip)
            chip.show()
            next_buttons.append(chip)
        self._chip_buttons = next_buttons
        self.chip_layout.activate()
        chip_height = max((chip.sizeHint().height() for chip in self._chip_buttons), default=0)
        scrollbar_height = self.chip_scroll.horizontalScrollBar().sizeHint().height()
        self.chip_scroll.setFixedHeight(
            max(self.input.sizeHint().height() + 6, chip_height + scrollbar_height + 4)
        )
        body_hint = self.chip_layout.sizeHint()
        self.chip_body.setFixedSize(max(1, body_hint.width()), max(1, body_hint.height()))
        self.chip_scroll.setVisible(bool(self._values))
        self.layout().activate()
        self.setMinimumHeight(self.sizeHint().height())
        self.updateGeometry()
        self.valuesChanged.emit()


class FioSpotterTab(QWidget):
    """A bounded browser for FIO's existing Spotter facilities.

    Tables deliberately contain at most 200 rows.  A tab is populated only on
    first visit or explicit refresh, so changing selection or painting never
    opens a database or walks a forms directory.
    """

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        settings: SettingsManager | None = None,
        open_compose: Callable[[], None] | None = None,
        open_inbox: Callable[[dict[str, Any]], None] | None = None,
        open_map: Callable[[dict[str, Any]], None] | None = None,
        open_operator: Callable[[dict[str, Any]], None] | None = None,
        radio_store: object | None = None,
    ) -> None:
        super().__init__(parent)
        self.settings = settings or SettingsManager()
        self._open_compose = open_compose
        self._open_inbox = open_inbox
        self._open_map = open_map
        self._open_operator = open_operator
        self._radio_store_override = radio_store
        self._built: set[int] = set()
        self._policy_rows: list[dict[str, Any]] = []
        self._entry_rows: list[dict[str, Any]] = []
        self._activity_rows: list[dict[str, Any]] = []
        self._activity_catalog_rows: list[dict[str, Any]] = []
        self._activity_action_filter = ""
        self._activity_context = None
        self._watch_rows: list[dict[str, Any]] = []
        self._expect_access_catalog_loaded_at = 0.0
        self._compact = False
        self.setObjectName("fioSpotterTab")
        outer = QVBoxLayout(self)
        outer.setContentsMargins(12, 10, 12, 12)
        outer.setSpacing(8)
        title = QLabel("FIO Spotter")
        title.setObjectName("fioSpotterTitle")
        title.setAccessibleName("FIO Spotter service")
        outer.addWidget(title)
        why = QLabel("Monitor FIO traffic, maintain shared watches, and safely administer JS8 Expect automation.")
        why.setWordWrap(True)
        why.setObjectName("fioSpotterWhy")
        outer.addWidget(why)
        self.tabs = QTabWidget()
        self.tabs.setObjectName("fioSpotterBrowserTabs")
        self.tabs.setAccessibleName("FIO Spotter browser tabs")
        self.tabs.setUsesScrollButtons(True)
        for name in _TAB_NAMES:
            page = QWidget()
            page.setObjectName(f"fioSpotter{name}Page")
            self.tabs.addTab(page, name)
        self.tabs.currentChanged.connect(self._activate_tab)
        outer.addWidget(self.tabs, 1)
        self._activate_tab(0)

    def set_tab_active(self, active: bool) -> None:
        """Lifecycle hook used by MainWindow's lazy screen controller."""
        if active and self.tabs.currentIndex() not in self._built:
            self._activate_tab(self.tabs.currentIndex())

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        compact = self.width() <= 1000
        if compact != self._compact:
            self._compact = compact
            for splitter in self.findChildren(QSplitter):
                splitter.setOrientation(Qt.Vertical if compact else Qt.Horizontal)
            self._apply_expect_responsive_layout()
        super().resizeEvent(event)

    def _apply_expect_responsive_layout(self) -> None:
        if not hasattr(self, "expect_editor_split"):
            return
        compact = self.width() <= 1000
        orientation = Qt.Vertical if compact else Qt.Horizontal
        self.expect_editor_split.setOrientation(orientation)
        self.expect_history_split.setOrientation(orientation)
        self.expect_editor_split.setSizes([320, 620] if compact else [520, 620])
        self.expect_editor_split.widget(0).setMaximumHeight(210 if compact else 16777215)
        self.expect_history_split.setMaximumHeight(300 if compact else 190)
        self._update_expect_editor_minimum_height()

    def _load_expect_access_completions(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and self._expect_access_catalog_loaded_at and (now - self._expect_access_catalog_loaded_at) < 60.0:
            return
        try:
            catalog = list_expect_operator_access_catalog(limit=2000)
        except Exception:
            catalog = []
        callsigns = [str(row.get("callsign") or "") for row in catalog]
        groups = sorted({
            str(group or "").strip().upper()
            for row in catalog for group in (row.get("groups") or [])
            if str(group or "").strip()
        })
        addressed_groups = [f"@{group.lstrip('@')}" for group in groups]
        for widget in (self.expect_calls, self.expect_blocked, self.policy_calls, self.policy_blocked):
            widget.set_completion_values(callsigns)
        for widget in (self.expect_groups, self.policy_groups):
            widget.set_completion_values(addressed_groups)
        for widget in (self.expect_trusted_groups, self.policy_trusted_groups):
            widget.set_completion_values(groups)
        historical = sum(1 for row in catalog if row.get("historical"))
        trusted = len({str(row.get("current_callsign") or "") for row in catalog if row.get("trusted")})
        self.expect_access_catalog_state.setText(
            f"Operator History lookup: {len(callsigns)} callsigns ({historical} former); "
            f"{trusted} trusted operators; {len(groups)} groups."
            if catalog else
            "Operator History lookup is empty. Explicit callsigns and * remain available."
        )
        self._expect_access_catalog_loaded_at = now

    @staticmethod
    def _fit_spotter_combo(combo: QComboBox, *, minimum_characters: int = 10) -> None:
        """Keep compact editors flexible while making every popup legible."""
        combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLengthWithIcon)
        combo.setMinimumContentsLength(max(6, min(24, int(minimum_characters))))
        combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        metrics = combo.fontMetrics()
        widest = max(
            (metrics.horizontalAdvance(combo.itemText(index)) for index in range(combo.count())),
            default=metrics.horizontalAdvance(combo.currentText()),
        )
        try:
            combo.view().setMinimumWidth(min(max(widest + 56, 160), 640))
        except Exception:
            pass
        for index in range(combo.count()):
            combo.setItemData(index, combo.itemText(index), Qt.ToolTipRole)

    def _radio_store(self) -> object:
        if callable(getattr(self._radio_store_override, "list_device_profiles", None)):
            return self._radio_store_override
        host_store = getattr(self.window(), "multi_radio_store", None)
        if callable(getattr(host_store, "list_device_profiles", None)):
            return host_store
        db_path = getattr(self.settings, "db_path", None)
        return MultiRadioStore(Path(db_path)) if db_path else MultiRadioStore()

    def _refresh_expect_radio_choices(self) -> None:
        if not hasattr(self, "expect_radio"):
            return
        selected = _text(self.expect_radio.currentData())
        try:
            profiles = list(self._radio_store().list_device_profiles())
        except Exception as exc:
            profiles = []
            log.warning("FIO Spotter: configured radio list unavailable: %s", exc)
        js8_profiles = [
            dict(profile) for profile in profiles
            if profile.get("use_js8call") or profile.get("js8_instance_id")
        ]
        self.expect_radio.blockSignals(True)
        try:
            self.expect_radio.clear()
            self.expect_radio.addItem("All JS8 radios — reply on receiving radio", "")
            for profile in js8_profiles:
                profile_id = _text(profile.get("id"))
                if not profile_id:
                    continue
                name = _text(profile.get("name")) or f"Radio {profile_id}"
                if not bool(profile.get("enabled", True)):
                    name = f"{name} (inactive)"
                self.expect_radio.addItem(name, profile_id)
            match = self.expect_radio.findData(selected)
            self.expect_radio.setCurrentIndex(match if match >= 0 else 0)
        finally:
            self.expect_radio.blockSignals(False)
        self._fit_spotter_combo(self.expect_radio, minimum_characters=20)

    def _select_expect_radio(self, source_scope: object, source_radio_id: object) -> None:
        radio_id = _text(source_radio_id)
        if _text(source_scope).lower() != "radio" or not radio_id:
            self.expect_radio.setCurrentIndex(0)
            return
        index = self.expect_radio.findData(radio_id)
        if index < 0:
            self.expect_radio.addItem(f"Unavailable configured radio · ID {radio_id}", radio_id)
            self._fit_spotter_combo(self.expect_radio, minimum_characters=20)
            index = self.expect_radio.count() - 1
        self.expect_radio.setCurrentIndex(index)

    def _expect_radio_label(self, source_scope: object, source_radio_id: object) -> str:
        radio_id = _text(source_radio_id)
        if _text(source_scope).lower() != "radio" or not radio_id:
            return "All JS8 radios"
        index = self.expect_radio.findData(radio_id) if hasattr(self, "expect_radio") else -1
        return self.expect_radio.itemText(index) if index >= 0 else f"Radio ID {radio_id}"

    def _activate_tab(self, index: int) -> None:
        if index in self._built:
            return
        started = time.perf_counter()
        builders: tuple[Callable[[QWidget], None], ...] = (
            self._build_activity, self._build_watches, self._build_expect,
            self._build_forms, self._build_imports,
        )
        builders[index](self.tabs.widget(index))
        self._built.add(index)
        # Forms/import preview can touch an external directory/database, so
        # those scans are operator-triggered rather than tab-activation work.
        if index == 0:
            self.refresh_activity()
        elif index == 1:
            self.refresh_watches()
        elif index == 2:
            self.refresh_expect()
        elif index == 3:
            self._refresh_forms_state()
        else:
            self.refresh_imports()
        emit_span(
            "fio_spotter.tab_first_load",
            (time.perf_counter() - started) * 1000.0,
            meta={"index": index, "tab": _TAB_NAMES[index]},
            min_ms=10.0,
        )

    @staticmethod
    def _table(headers: list[str], *, name: str) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setObjectName(name)
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setVisible(False)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setAlternatingRowColors(True)
        table.setWordWrap(False)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSizeAdjustPolicy(QAbstractItemView.AdjustIgnored)
        table.horizontalHeader().setStretchLastSection(True)
        return table

    @staticmethod
    def _put(table: QTableWidget, row: int, col: int, value: object, *, data: object = None) -> None:
        item = QTableWidgetItem(_text(value) or "—")
        if data is not None:
            item.setData(Qt.UserRole, data)
        item.setToolTip(_text(value))
        table.setItem(row, col, item)

    @staticmethod
    def _page_layout(page: QWidget) -> QVBoxLayout:
        outer = QVBoxLayout(page)
        outer.setContentsMargins(0, 0, 0, 0)
        scroll = QScrollArea(page)
        scroll.setObjectName(f"{page.objectName()}Scroll")
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        # Tables own their horizontal overflow.  Keeping page-level horizontal
        # scrolling disabled prevents the whole service workspace from sliding
        # sideways at compact widths or with Large Text enabled.
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        content = QWidget(scroll)
        content.setObjectName(f"{page.objectName()}Content")
        layout = QVBoxLayout(content)
        layout.setContentsMargins(4, 8, 4, 4)
        layout.setSpacing(8)
        scroll.setWidget(content)
        outer.addWidget(scroll)
        return layout

    def _chip_row(self, labels: tuple[str, ...], callback: Callable[[], None]) -> QWidget:
        row = QWidget()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)
        for label in labels:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setObjectName("fioSpotterFilterChip")
            btn.setToolTip(f"Filter FIO Spotter results by {label}")
            btn.clicked.connect(callback)
            layout.addWidget(btn)
        layout.addStretch(1)
        return row

    # Activity -------------------------------------------------------------
    def _build_activity(self, page: QWidget) -> None:
        layout = self._page_layout(page)
        header = QHBoxLayout()
        header.addWidget(QLabel("Recent FIO traffic"))
        header.addStretch(1)
        refresh = QPushButton("Refresh")
        refresh.setAccessibleName("Refresh Spotter activity")
        refresh.clicked.connect(self.refresh_activity)
        header.addWidget(refresh)
        layout.addLayout(header)
        self.activity_chip_row = self._chip_row(("All", "JS8", "Forms"), self._on_activity_filter)
        self.activity_chips = self.activity_chip_row.findChildren(QPushButton)
        self.activity_chips[0].setChecked(True)
        layout.addWidget(self.activity_chip_row)
        self.activity_intelligence = TrafficActionSummaryWidget(self.settings)
        self.activity_intelligence.title_label.setText("Message intelligence")
        self.activity_intelligence.setToolTip(
            "Action counts are derived from the newest bounded Spotter page and your operator/group duties."
        )
        self.activity_intelligence.bucketActivated.connect(self._set_activity_action_filter)
        layout.addWidget(self.activity_intelligence)
        split = QSplitter(Qt.Horizontal)
        self.activity_table = self._table(["Age", "Source", "From", "Group", "Form", "Action", "Topic"], name="fioSpotterActivityTable")
        hdr = self.activity_table.horizontalHeader()
        for col in range(6):
            hdr.setSectionResizeMode(col, QHeaderView.Interactive)
        hdr.setSectionResizeMode(6, QHeaderView.Stretch)
        for col, width in enumerate((145, 85, 90, 90, 90, 85)):
            self.activity_table.setColumnWidth(col, width)
        detail_panel = QWidget()
        detail_layout = QVBoxLayout(detail_panel)
        detail_layout.setContentsMargins(0, 0, 0, 0)
        self.activity_detail = QTextEdit()
        self.activity_detail.setObjectName("fioSpotterActivityDetail")
        self.activity_detail.setReadOnly(True)
        self.activity_detail.setAccessibleName("Selected activity evidence")
        self.activity_detail.setPlaceholderText("Select traffic to inspect decoded evidence and routing context.")
        detail_layout.addWidget(self.activity_detail, 1)
        detail_actions = QGridLayout()
        for index, (label, callback) in enumerate(
            (
                ("Inbox", self._open_selected_activity_inbox),
                ("Map", self._open_selected_activity_map),
                ("Operator", self._open_selected_activity_operator),
                ("Reply", self._open_spotter_compose),
            )
        ):
            button = QPushButton(label)
            button.setAccessibleName(f"Open {label} for selected Spotter activity")
            button.clicked.connect(callback)
            detail_actions.addWidget(button, index // 2, index % 2)
        detail_layout.addLayout(detail_actions)
        self.activity_table.itemSelectionChanged.connect(self._show_activity_detail)
        split.addWidget(self.activity_table)
        split.addWidget(detail_panel)
        split.setSizes([700, 360])
        layout.addWidget(split, 1)

    def refresh_activity(self) -> None:
        """Explicitly refresh the bounded projection page from the station store."""
        table = getattr(self, "activity_table", None)
        if table is None:
            return
        started = time.perf_counter()
        self._activity_catalog_rows = []
        try:
            # The service defaults match projection source families emitted by
            # FIO: ``spotter`` (decoded forms), ``js8`` and legacy ``js8call``.
            # Filter chips intentionally operate on this cached bounded page;
            # toggling them must not issue a database query.
            self._activity_catalog_rows = list_spotter_activity(
                source_families=("spotter", "js8", "js8call"), limit=_MAX_ROWS
            )
        except Exception as exc:
            log.warning("FIO Spotter: Activity refresh failed: %s", exc, exc_info=True)
            self._activity_catalog_rows = []
        hf_groups, local_groups = configured_group_names(self.settings)
        callsign = self.settings.get("operator_callsign", "") or self.settings.get("callsign", "")
        try:
            self._activity_context = load_operator_traffic_context(
                default_expect_db_path(),
                callsign=callsign,
                configured_operating_groups=hf_groups,
                configured_local_groups=local_groups,
            )
        except Exception as exc:
            log.warning("FIO Spotter: operator traffic context read failed: %s", exc, exc_info=True)
            self._activity_context = build_operator_traffic_context(
                callsign=callsign,
                configured_operating_groups=hf_groups,
                configured_local_groups=local_groups,
            )
        query_elapsed = (time.perf_counter() - started) * 1000.0
        self._apply_activity_filters(query_elapsed=query_elapsed)

    def _apply_activity_filters(self, *, query_elapsed: float = 0.0) -> None:
        """Apply Activity chips to the current bounded page without I/O."""
        table = getattr(self, "activity_table", None)
        if table is None:
            return
        selected = {button.text() for button in getattr(self, "activity_chips", ()) if button.isChecked()}
        families = {"spotter", "js8", "js8call"}
        if "JS8" in selected and "Forms" not in selected:
            families = {"js8", "js8call"}
        elif "Forms" in selected and "JS8" not in selected:
            families = {"spotter"}
        source_rows = [
            row for row in self._activity_catalog_rows
            if _text(row.get("source_family")).lower() in families
        ]
        context = self._activity_context
        summary = build_traffic_action_summary(source_rows, context) if context is not None else None
        if hasattr(self, "activity_intelligence"):
            self.activity_intelligence.set_active_bucket(self._activity_action_filter)
            self.activity_intelligence.set_summary(summary or TrafficActionSummary())
        if self._activity_action_filter and context is not None:
            self._activity_rows = [
                row for row in source_rows
                if message_matches_traffic_bucket(row, context, self._activity_action_filter)
            ]
        else:
            self._activity_rows = source_rows
        render_started = time.perf_counter()
        table.setUpdatesEnabled(False)
        table.blockSignals(True)
        try:
            table.clearContents()
            table.setRowCount(len(self._activity_rows))
            for i, row in enumerate(self._activity_rows):
                age = _when(row.get("received_ts") or row.get("event_ts"))
                self._put(table, i, 0, age, data=row)
                self._put(table, i, 1, row.get("source_family"))
                self._put(table, i, 2, row.get("from_call"))
                self._put(table, i, 3, row.get("group_name") or row.get("to_call"))
                self._put(table, i, 4, row.get("form_id") or row.get("message_type"))
                self._put(table, i, 5, self._activity_action_text(row))
                self._put(table, i, 6, ", ".join(row.get("topics") or ()) or row.get("subject") or row.get("summary") or row.get("preview") or row.get("body_text"))
        finally:
            table.blockSignals(False)
            table.setUpdatesEnabled(True)
        emit_span(
            "fio_spotter.activity_refresh",
            query_elapsed + (time.perf_counter() - render_started) * 1000.0,
            meta={
                "query_ms": round(query_elapsed, 1),
                "render_ms": round((time.perf_counter() - render_started) * 1000.0, 1),
                "rows": len(self._activity_rows),
            },
            min_ms=10.0,
        )
        if not self._activity_rows:
            self.activity_detail.setPlainText("No matching traffic in the current bounded page. Refresh to read newer station traffic.")

    def _set_activity_action_filter(self, bucket: object) -> None:
        """Filter the cached Activity page by shared message intelligence."""
        self._activity_action_filter = _text(bucket).lower()
        self._apply_activity_filters()

    def _activity_action_text(self, row: dict[str, Any]) -> str:
        """Render the same operator-aware action used by the summary chips."""
        item = traffic_action_item(row, self._activity_context) if self._activity_context is not None else None
        if item is not None:
            return item.primary_bucket.title()
        action = _text(row.get("recommended_action")).replace("_", " ")
        if action:
            return action[:1].upper() + action[1:]
        if row.get("operator_attention") or row.get("actionable"):
            return "Review"
        status = _text(row.get("status"))
        severity = _text(row.get("severity"))
        return "No action" if not severity or severity.lower() == "info" else f"Monitor · {severity}"

    def _on_activity_filter(self) -> None:
        sender = self.sender()
        if isinstance(sender, QPushButton) and sender.text() == "All" and sender.isChecked():
            for button in self.activity_chips:
                if button is not sender:
                    button.setChecked(False)
        elif isinstance(sender, QPushButton) and sender.text() != "All":
            self.activity_chips[0].setChecked(False)
        if not any(button.isChecked() for button in self.activity_chips):
            self.activity_chips[0].setChecked(True)
        self._apply_activity_filters()

    def _show_activity_detail(self) -> None:
        selected = self.activity_table.selectedItems()
        if not selected:
            return
        row = selected[0].data(Qt.UserRole) or {}
        intelligence = row.get("intelligence") if isinstance(row.get("intelligence"), dict) else {}
        provenance = intelligence.get("provenance") if isinstance(intelligence.get("provenance"), dict) else {}
        map_context = intelligence.get("map") if isinstance(intelligence.get("map"), dict) else {}
        topics = ", ".join(str(topic) for topic in row.get("topics") or ()) or "None classified"
        action_item = traffic_action_item(row, self._activity_context) if self._activity_context is not None else None
        why: list[str] = []
        severity = _text(row.get("severity"))
        if severity:
            why.append(f"Severity: {severity}")
        trust = _text(provenance.get("trust"))
        freshness = _text(provenance.get("freshness"))
        if trust:
            why.append(f"Trust: {trust}")
        if freshness:
            why.append(f"Freshness: {freshness}")
        location = " / ".join(part for part in (_text(map_context.get("state")) or _text(row.get("state_code")), _text(map_context.get("grid")) or _text(row.get("grid"))) if part)
        if location:
            why.append(f"Location: {location}")
        self.activity_detail.setPlainText(
            "Assessment\n\n"
            f"{_text(row.get('summary') or row.get('subject')) or 'No shared summary is available.'}\n"
            f"Recommended action: {self._activity_action_text(row)}\n"
            f"Topics: {topics}\n"
            f"What: {action_item.what if action_item is not None else 'Monitor this traffic in context.'}\n"
            f"Why: {action_item.why if action_item is not None else ('; '.join(why) or 'No elevated condition in the shared projection.')}\n\n"
            "Source evidence\n\n"
            f"From: {_text(row.get('from_call')) or 'Unknown'}\n"
            f"Target: {_text(row.get('to_call') or row.get('group_name')) or 'Unknown'}\n"
            f"Source: {_text(row.get('source_family')) or 'Unknown'}\n\n"
            f"{_text(row.get('body_text') or row.get('preview') or row.get('subject'))}\n\n"
            "Use Inbox, Map, or Compose for the corresponding operator action."
        )

    def _selected_activity(self) -> dict[str, Any] | None:
        if not hasattr(self, "activity_table") or self.activity_table.currentRow() < 0:
            return None
        item = self.activity_table.item(self.activity_table.currentRow(), 0)
        row = item.data(Qt.UserRole) if item is not None else None
        return dict(row) if isinstance(row, dict) else None

    def _open_selected_activity_inbox(self) -> None:
        row = self._selected_activity()
        if row is not None and self._open_inbox is not None:
            self._open_inbox(row)

    def _open_selected_activity_map(self) -> None:
        row = self._selected_activity()
        if row is not None and self._open_map is not None:
            self._open_map(row)

    def _open_selected_activity_operator(self) -> None:
        row = self._selected_activity()
        if row is not None and self._open_operator is not None:
            self._open_operator(row)

    # Watches --------------------------------------------------------------
    def _build_watches(self, page: QWidget) -> None:
        layout = self._page_layout(page)
        hint = QLabel("Shared watches match callsigns, groups, topics, status, locations, and keywords in station traffic. Changes apply to the shared station watch service.")
        hint.setWordWrap(True)
        layout.addWidget(hint)
        header = QHBoxLayout()
        header.addWidget(QLabel("Watches"))
        header.addStretch(1)
        refresh = QPushButton("Refresh")
        refresh.clicked.connect(self.refresh_watches)
        header.addWidget(refresh)
        layout.addLayout(header)
        split = QSplitter(Qt.Horizontal)
        self.watches_table = self._table(["Use", "Watch", "Match", "Sources", "Priority", "Expires", "Last match", "Health"], name="fioSpotterWatchesTable")
        self.watches_table.itemSelectionChanged.connect(self._load_selected_watch)
        watch_hdr = self.watches_table.horizontalHeader()
        for col in (0, 2, 4, 5, 6, 7):
            watch_hdr.setSectionResizeMode(col, QHeaderView.Interactive)
        watch_hdr.setSectionResizeMode(1, QHeaderView.Stretch)
        for col, width in ((0, 50), (2, 150), (4, 80), (5, 130), (6, 130), (7, 90)):
            self.watches_table.setColumnWidth(col, width)
        split.addWidget(self.watches_table)
        editor = QGroupBox("Watch editor")
        editor.setAccessibleName("Spotter watch editor")
        form = QFormLayout(editor)
        self.watch_name = QLineEdit(); self.watch_name.setPlaceholderText("Watch name")
        self.watch_kind = QComboBox(); self.watch_kind.addItems(WATCH_KINDS); self.watch_kind.setCurrentText("keyword")
        self.watch_pattern = QLineEdit(); self.watch_pattern.setPlaceholderText("What to match")
        self.watch_mode = QComboBox(); self.watch_mode.addItems(MATCH_MODES)
        self.watch_priority = QComboBox(); self.watch_priority.addItems(PRIORITIES)
        for combo in (self.watch_kind, self.watch_mode, self.watch_priority):
            self._fit_spotter_combo(combo)
        self.watch_sources = QLineEdit(); self.watch_sources.setPlaceholderText("spotter, js8 (blank is all)")
        self.watch_radios = QLineEdit(); self.watch_radios.setPlaceholderText("Radio IDs, comma separated")
        self.watch_expiry_days = QSpinBox(); self.watch_expiry_days.setRange(0, 3650); self.watch_expiry_days.setSuffix(" days (0 = never)")
        self.watch_enabled = QCheckBox("Watch enabled"); self.watch_enabled.setChecked(True)
        self.watch_notes = QLineEdit(); self.watch_notes.setPlaceholderText("Optional operator note")
        for label, widget in (("Name", self.watch_name), ("Type", self.watch_kind), ("Pattern", self.watch_pattern), ("Match", self.watch_mode), ("Priority", self.watch_priority), ("Sources", self.watch_sources), ("Radios", self.watch_radios), ("Expiry", self.watch_expiry_days), ("Notes", self.watch_notes)):
            form.addRow(label, widget)
        form.addRow(self.watch_enabled)
        actions = QWidget(); action_grid = QGridLayout(actions); action_grid.setContentsMargins(0, 4, 0, 0)
        save = QPushButton("Save"); save.setAccessibleName("Save Spotter watch"); save.clicked.connect(self._save_watch)
        new = QPushButton("New"); new.clicked.connect(self._clear_watch)
        toggle = QPushButton("Enable / disable"); toggle.clicked.connect(self._toggle_watch)
        delete = QPushButton("Delete"); delete.clicked.connect(self._delete_watch)
        test = QPushButton("Test selected traffic"); test.clicked.connect(self._test_watch)
        for index, button in enumerate((save, new, toggle, delete, test)):
            action_grid.addWidget(button, index // 3, index % 3)
        action_grid.setColumnStretch(3, 1)
        form.addRow(actions)
        self.watch_status = QLabel("Select a watch or add a new one.")
        self.watch_status.setWordWrap(True); self.watch_status.setObjectName("fioSpotterWatchStatus")
        form.addRow(self.watch_status)
        split.addWidget(editor); split.setSizes([620, 460])
        layout.addWidget(split, 1)

    def refresh_watches(self) -> None:
        if not hasattr(self, "watches_table"):
            return
        try:
            self._watch_rows = list_spotter_watches(limit=_MAX_ROWS)
        except Exception as exc:
            self._watch_rows = []
            self.watch_status.setText(f"Watch service unavailable: {exc}")
        self.watches_table.setUpdatesEnabled(False)
        self.watches_table.blockSignals(True)
        try:
            self.watches_table.clearContents()
            self.watches_table.setRowCount(len(self._watch_rows))
            for i, row in enumerate(self._watch_rows):
                source_text = ", ".join(row.get("source_families") or ()) or "All sources"
                values = (
                    "●" if row.get("enabled") else "○", row.get("name"),
                    f"{row.get('watch_kind')}: {row.get('pattern')}", source_text,
                    row.get("priority"), _when(row.get("expires_ts")),
                    _when(row.get("last_match_ts")), row.get("health"),
                )
                for col, value in enumerate(values):
                    self._put(self.watches_table, i, col, value, data=row if col == 0 else None)
        finally:
            self.watches_table.blockSignals(False)
            self.watches_table.setUpdatesEnabled(True)
        if not self._watch_rows:
            self.watch_status.setText("No watches yet. Add a bounded station watch.")

    def _selected_watch(self) -> dict[str, Any] | None:
        if not hasattr(self, "watches_table") or self.watches_table.currentRow() < 0:
            return None
        item = self.watches_table.item(self.watches_table.currentRow(), 0)
        row = item.data(Qt.UserRole) if item is not None else None
        return dict(row) if isinstance(row, dict) else None

    def _load_selected_watch(self) -> None:
        row = self._selected_watch()
        if not row:
            return
        self.watch_name.setText(_text(row.get("name")))
        self.watch_kind.setCurrentIndex(max(0, self.watch_kind.findText(_text(row.get("watch_kind")))))
        self.watch_pattern.setText(_text(row.get("pattern")))
        self.watch_mode.setCurrentIndex(max(0, self.watch_mode.findText(_text(row.get("match_mode")))))
        self.watch_priority.setCurrentIndex(max(0, self.watch_priority.findText(_text(row.get("priority")))))
        self.watch_sources.setText(", ".join(row.get("source_families") or ()))
        self.watch_radios.setText(", ".join(row.get("source_radio_ids") or ()))
        expires = float(row.get("expires_ts") or 0)
        self.watch_expiry_days.setValue(max(0, int(round((expires - time.time()) / 86400))) if expires else 0)
        self.watch_enabled.setChecked(bool(row.get("enabled")))
        self.watch_notes.setText(_text(row.get("notes")))
        self.watch_status.setText(f"Selected {row.get('name')}. Matched {int(row.get('match_count') or 0)} time(s).")

    def _clear_watch(self) -> None:
        self.watches_table.clearSelection(); self.watch_name.clear(); self.watch_kind.setCurrentText("keyword"); self.watch_pattern.clear(); self.watch_mode.setCurrentIndex(0); self.watch_priority.setCurrentIndex(0); self.watch_sources.clear(); self.watch_radios.clear(); self.watch_expiry_days.setValue(0); self.watch_enabled.setChecked(True); self.watch_notes.clear(); self.watch_status.setText("New watch. Save to add it to the station service.")

    def _watch_values(self, existing: dict[str, Any] | None = None) -> dict[str, Any]:
        days = self.watch_expiry_days.value()
        return {
            "id": int((existing or {}).get("id") or 0), "name": self.watch_name.text(),
            "watch_kind": self.watch_kind.currentText(), "pattern": self.watch_pattern.text(),
            "match_mode": self.watch_mode.currentText(), "priority": self.watch_priority.currentText(),
            "source_families": [part.strip().lower() for part in self.watch_sources.text().split(",") if part.strip()],
            "source_radio_ids": _csv(self.watch_radios.text()),
            "expires_ts": time.time() + days * 86400 if days else 0,
            "enabled": self.watch_enabled.isChecked(), "notes": self.watch_notes.text(),
            "import_source": "fio-spotter",
        }

    def _save_watch(self) -> None:
        try:
            saved = save_spotter_watch(self._watch_values(self._selected_watch()))
        except Exception as exc:
            self.watch_status.setText(f"Watch not saved: {exc}")
            return
        self.refresh_watches()
        self.watch_status.setText(f"{'Added' if saved.created else 'Saved'} watch; {'enabled' if saved.enabled else 'disabled'}.")

    def _toggle_watch(self) -> None:
        current = self._selected_watch()
        if not current:
            self.watch_status.setText("Select a watch to enable or disable.")
            return
        self.watch_enabled.setChecked(not bool(current.get("enabled")))
        self._save_watch()

    def _delete_watch(self) -> None:
        current = self._selected_watch()
        if not current:
            self.watch_status.setText("Select a watch to delete.")
            return
        try:
            deleted = delete_spotter_watch(int(current.get("id") or 0))
        except Exception as exc:
            self.watch_status.setText(f"Watch not deleted: {exc}")
            return
        self._clear_watch(); self.refresh_watches()
        self.watch_status.setText("Deleted watch." if deleted else "Watch was already unavailable.")

    def _test_watch(self) -> None:
        current = self._selected_watch()
        if not current:
            current = self._watch_values()
        candidate: dict[str, Any] | None = None
        if hasattr(self, "activity_table") and self.activity_table.currentRow() >= 0:
            item = self.activity_table.item(self.activity_table.currentRow(), 0)
            candidate = item.data(Qt.UserRole) if item else None
        if not isinstance(candidate, dict):
            self.watch_status.setText("Select an Activity row, then test this watch. Testing never changes match counts.")
            return
        self.watch_status.setText("Test match: matched selected traffic." if watch_matches(current, candidate) else "Test match: no match for selected traffic.")

    # Expect ---------------------------------------------------------------
    def _build_expect(self, page: QWidget) -> None:
        layout = self._page_layout(page)
        runtime = QGroupBox("Expect runtime")
        runtime_grid = QGridLayout(runtime)
        self.expect_runtime_state = QLabel()
        self.expect_runtime_state.setObjectName("fioSpotterExpectRuntimeState")
        self.expect_runtime_state.setAccessibleName("Expect runtime enabled or paused state")
        self.expect_runtime_enabled = QCheckBox("Enable unattended auto-reply")
        self.expect_runtime_paused = QCheckBox("Pause while enabled")
        self.dynamic_flamp_enabled = QCheckBox("Enable dynamic FLAMP Q replies")
        self.expect_runtime_enabled.toggled.connect(self._save_runtime_state)
        self.expect_runtime_paused.toggled.connect(self._save_runtime_state)
        self.dynamic_flamp_enabled.toggled.connect(self._save_dynamic_flamp_state)
        runtime_grid.addWidget(self.expect_runtime_state, 0, 0, 1, 3)
        runtime_grid.addWidget(self.expect_runtime_enabled, 1, 0)
        runtime_grid.addWidget(self.expect_runtime_paused, 1, 1)
        runtime_grid.addWidget(self.dynamic_flamp_enabled, 1, 2)
        self.dynamic_flamp_state = QLabel()
        self.dynamic_flamp_state.setObjectName("fioSpotterDynamicFlampQState")
        self.dynamic_flamp_state.setWordWrap(True)
        self.dynamic_flamp_state.setAccessibleName("Dynamic FLAMP Q service status")
        runtime_grid.addWidget(self.dynamic_flamp_state, 2, 0, 1, 3)
        layout.addWidget(runtime)
        split = QSplitter(Qt.Horizontal)
        self.expect_editor_split = split
        left = QWidget(); left_layout = QVBoxLayout(left); left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.addWidget(QLabel("Expect rules"))
        self.expect_entries_table = self._table(["Use", "Key", "Access", "Auto", "Replies", "Cooldown", "Source"], name="fioSpotterExpectEntriesTable")
        self.expect_entries_table.itemSelectionChanged.connect(self._load_selected_entry)
        left_layout.addWidget(self.expect_entries_table, 1)
        right = QWidget(); right_layout = QVBoxLayout(right); right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.addWidget(QLabel("Rule and allow-policy editor"))
        form = QFormLayout()
        self.expect_key = QLineEdit(); self.expect_key.setPlaceholderText("E? token, e.g. INFO or Q")
        self.expect_reply = QLineEdit(); self.expect_reply.setPlaceholderText("Reply text")
        self.expect_policy = QComboBox(); self.expect_policy.addItem("No allow policy (use rule fields below)", 0)
        self.expect_policy.setToolTip("Optional reusable policy. Save one below, then select it here to apply it to this rule.")
        access_help = QLabel(
            "Allowed callers controls who may ask; * allows anyone. Query groups are JS8 destinations whose "
            "group-addressed E? requests may be answered. Trusted operators from groups grants caller access "
            "only to trusted Operator History records in those groups. Find or enter a value, then press Enter or Add; "
            "selected values appear above and duplicates are ignored. Blocked callers always win."
        )
        access_help.setWordWrap(True)
        access_help.setObjectName("fioSpotterExpectAccessHelp")
        right_layout.addWidget(access_help)
        self.expect_access_catalog_state = QLabel()
        self.expect_access_catalog_state.setWordWrap(True)
        self.expect_access_catalog_state.setObjectName("fioSpotterExpectAccessCatalogState")
        right_layout.addWidget(self.expect_access_catalog_state)
        self.expect_calls = _TokenListEditor(); self.expect_calls.setPlaceholderText("Callsign or *")
        self.expect_groups = _TokenListEditor(token_prefix="@"); self.expect_groups.setPlaceholderText("Find or enter a query group")
        self.expect_blocked = _TokenListEditor(); self.expect_blocked.setPlaceholderText("Find or enter a blocked callsign")
        self.expect_trusted = QCheckBox("Allow all trusted operators")
        self.expect_trusted_groups = _TokenListEditor(); self.expect_trusted_groups.setPlaceholderText("Find an operator group")
        self.expect_radio = QComboBox()
        self.expect_radio.setAccessibleName("Radios that accept this Expect query")
        self.expect_radio.setToolTip(
            "All JS8 radios is recommended. FIO replies through the radio and JS8Call service that received the query."
        )
        self.expect_radio.addItem("All JS8 radios — reply on receiving radio", "")
        self._fit_spotter_combo(self.expect_radio, minimum_characters=20)
        self.expect_max = QSpinBox(); self.expect_max.setRange(1, 99); self.expect_max.setValue(1)
        self.expect_cooldown = QSpinBox(); self.expect_cooldown.setRange(0, 86400); self.expect_cooldown.setSuffix(" sec")
        self.expect_allow_any = QCheckBox("Allow all callers (*) — use cautiously")
        self.expect_allow_any.toggled.connect(self._sync_allow_any_callers)
        self.expect_enabled = QCheckBox("Rule enabled")
        self.expect_auto = QCheckBox("Auto reply")
        self.expect_unattended = QCheckBox("Unattended auto reply")
        for label, widget in (("E? Token", self.expect_key), ("Reply", self.expect_reply), ("Allow policy", self.expect_policy), ("Allowed callers", self.expect_calls)):
            form.addRow(label, widget)
        form.addRow(self.expect_allow_any)
        form.addRow("Query groups", self.expect_groups)
        form.addRow(self.expect_trusted)
        form.addRow("Trusted operators from groups", self.expect_trusted_groups)
        form.addRow("Blocked callers", self.expect_blocked)
        for label, widget in (("Radios", self.expect_radio), ("Max replies", self.expect_max), ("Cooldown", self.expect_cooldown)):
            form.addRow(label, widget)
        form.addRow(self.expect_enabled); form.addRow(self.expect_auto); form.addRow(self.expect_unattended)
        right_layout.addLayout(form)
        actions = QGridLayout()
        save = QPushButton("Save rule"); save.clicked.connect(self._save_entry)
        remove = QPushButton("Delete rule"); remove.clicked.connect(self._delete_entry)
        new = QPushButton("New"); new.clicked.connect(self._clear_entry)
        new_q = QPushButton("New FLAMP Q rule"); new_q.clicked.connect(self._new_dynamic_q_entry)
        for index, button in enumerate((save, new, new_q, remove)):
            actions.addWidget(button, index // 2, index % 2)
        actions.setColumnStretch(2, 1)
        right_layout.addLayout(actions)
        policy_box = QGroupBox("Reusable allow policy (optional)")
        policy_form = QFormLayout(policy_box)
        self.policy_manage = QComboBox(); self.policy_manage.addItem("New policy", 0)
        self.policy_manage.currentIndexChanged.connect(self._load_policy_editor)
        self.policy_name = QLineEdit(); self.policy_name.setPlaceholderText("Policy name")
        self.policy_calls = _TokenListEditor(); self.policy_calls.setPlaceholderText("Callsign or *")
        self.policy_groups = _TokenListEditor(token_prefix="@"); self.policy_groups.setPlaceholderText("Find or enter a query group")
        self.policy_trusted = QCheckBox("Allow all trusted operators")
        self.policy_trusted_groups = _TokenListEditor(); self.policy_trusted_groups.setPlaceholderText("Find an operator group")
        self.policy_blocked = _TokenListEditor(); self.policy_blocked.setPlaceholderText("Find or enter a blocked callsign")
        self.policy_scope = QComboBox()
        self.policy_scope.addItem("All JS8 radios", "all")
        self.policy_scope.addItem("Only listed radios", "radio")
        self.policy_radios = QLineEdit(); self.policy_radios.setPlaceholderText("Radio IDs, comma separated")
        self.policy_enabled = QCheckBox("Policy enabled"); self.policy_enabled.setChecked(True)
        for combo, characters in (
            (self.expect_policy, 20),
            (self.policy_manage, 18),
            (self.policy_scope, 10),
        ):
            self._fit_spotter_combo(combo, minimum_characters=characters)
        save_policy = QPushButton("Save allow policy")
        save_policy.setAccessibleName("Save Expect allow policy")
        save_policy.clicked.connect(self._save_policy)
        policy_actions = QWidget(); policy_actions_layout = QHBoxLayout(policy_actions); policy_actions_layout.setContentsMargins(0, 0, 0, 0)
        new_policy = QPushButton("New"); new_policy.clicked.connect(self._clear_policy_editor)
        delete_policy = QPushButton("Delete"); delete_policy.clicked.connect(self._delete_policy)
        policy_actions_layout.addWidget(save_policy); policy_actions_layout.addWidget(new_policy); policy_actions_layout.addWidget(delete_policy); policy_actions_layout.addStretch(1)
        policy_form.addRow("Manage", self.policy_manage)
        policy_form.addRow("Name", self.policy_name); policy_form.addRow("Allowed callers", self.policy_calls)
        policy_form.addRow("Query groups", self.policy_groups)
        policy_form.addRow(self.policy_trusted)
        policy_form.addRow("Trusted operators from groups", self.policy_trusted_groups)
        policy_form.addRow("Blocked callers", self.policy_blocked)
        policy_form.addRow("Radio scope", self.policy_scope); policy_form.addRow("Radio IDs", self.policy_radios)
        policy_form.addRow(self.policy_enabled); policy_form.addRow(policy_actions)
        right_layout.addWidget(policy_box)
        for editor in (
            self.expect_calls,
            self.expect_groups,
            self.expect_trusted_groups,
            self.expect_blocked,
            self.policy_calls,
            self.policy_groups,
            self.policy_trusted_groups,
            self.policy_blocked,
        ):
            editor.valuesChanged.connect(self._update_expect_editor_minimum_height)
        split.addWidget(left); split.addWidget(right); split.setSizes([560, 460])
        layout.addWidget(split, 1)
        histories = QSplitter(Qt.Horizontal)
        self.expect_history_split = histories
        self.expect_requests_table = self._table(["When", "Decision", "Key", "Caller", "Reason"], name="fioSpotterExpectRequestsTable")
        self.expect_replies_table = self._table(["When", "Decision", "Reply radio", "Response"], name="fioSpotterExpectRepliesTable")
        histories.addWidget(self.expect_requests_table); histories.addWidget(self.expect_replies_table)
        histories.setMaximumHeight(190)
        layout.addWidget(histories)
        self._load_expect_access_completions()
        self._apply_expect_responsive_layout()

    def _update_expect_editor_minimum_height(self) -> None:
        if not hasattr(self, "expect_editor_split") or self.expect_editor_split.count() < 2:
            return
        editor = self.expect_editor_split.widget(1)
        editor.setMinimumHeight(0)
        if editor.layout() is not None:
            editor.layout().activate()
        editor.setMinimumHeight(editor.minimumSizeHint().height())
        editor.updateGeometry()
        children = [self.expect_editor_split.widget(index) for index in range(self.expect_editor_split.count())]
        minimums = [
            max(child.minimumHeight(), child.minimumSizeHint().height())
            for child in children
        ]
        if self.expect_editor_split.orientation() == Qt.Vertical:
            minimum_height = sum(minimums) + self.expect_editor_split.handleWidth() * max(0, len(children) - 1)
        else:
            minimum_height = max(minimums, default=0)
        self.expect_editor_split.setMinimumHeight(minimum_height)
        self.expect_editor_split.updateGeometry()

    def _save_runtime_state(self) -> None:
        if not hasattr(self, "expect_runtime_enabled"):
            return
        try:
            set_expect_automation_runtime_state(self.settings, enabled=self.expect_runtime_enabled.isChecked(), paused=self.expect_runtime_paused.isChecked())
        except Exception:
            pass
        self._refresh_runtime_state()

    def _sync_allow_any_callers(self, checked: bool) -> None:
        if not hasattr(self, "expect_calls"):
            return
        values = _csv(self.expect_calls.text())
        values = [value for value in values if value != "*"]
        if checked:
            values.insert(0, "*")
        self.expect_calls.setText(", ".join(values))

    def _refresh_runtime_state(self) -> None:
        state = load_expect_automation_runtime_state(self.settings)
        self.expect_runtime_enabled.blockSignals(True); self.expect_runtime_paused.blockSignals(True); self.dynamic_flamp_enabled.blockSignals(True)
        self.expect_runtime_enabled.setChecked(state.enabled); self.expect_runtime_paused.setChecked(state.paused)
        self.dynamic_flamp_enabled.setChecked(bool(self.settings.get("js8_expect_dynamic_flamp_enabled", False)))
        self.expect_runtime_enabled.blockSignals(False); self.expect_runtime_paused.blockSignals(False); self.dynamic_flamp_enabled.blockSignals(False)
        self.expect_runtime_state.setText(("● Running" if state.active else "○ Paused / disabled") + " — " + state.reason)
        try:
            statuses = list_flamp_transfer_index_statuses(
                db_path=Path(self.settings.config_dir) / "freqinout_nets.db",
                limit=25,
            )
        except Exception:
            statuses = []
        ready = [row for row in statuses if row.get("scan_success")]
        files = sum(int(row.get("file_count") or 0) for row in ready)
        if ready:
            newest = max(float(row.get("scanned_ts") or 0.0) for row in ready)
            scan_text = f"{len(ready)} source(s), {files} indexed transfer(s), last scan {_when(newest)}"
        elif statuses:
            scan_text = "index needs attention — " + (_text(statuses[0].get("error_text")) or "no successful source scan")
        else:
            scan_text = "waiting for the first background FLAMP source scan"
        q_rules = [row for row in self._entry_rows if _text(row.get("expect_key")).upper() == "Q"]
        enabled_q = [row for row in q_rules if row.get("enabled") and row.get("auto_reply_enabled") and row.get("unattended_auto_reply_enabled")]
        self.dynamic_flamp_state.setText(
            f"{'●' if self.dynamic_flamp_enabled.isChecked() and enabled_q else '○'} FLAMP Q — "
            f"{len(enabled_q)} approved Q rule(s); {scan_text}. Replies use the receiving JS8 source."
        )

    def _save_dynamic_flamp_state(self) -> None:
        try:
            self.settings.set("js8_expect_dynamic_flamp_enabled", self.dynamic_flamp_enabled.isChecked())
            if hasattr(self.settings, "save"):
                self.settings.save()
        except Exception:
            pass
        self._refresh_runtime_state()

    def refresh_expect(self) -> None:
        if not hasattr(self, "expect_entries_table"):
            return
        self._refresh_expect_radio_choices()
        self._load_expect_access_completions()
        self._refresh_runtime_state()
        try:
            self._policy_rows = list_expect_allow_policies()
            self._entry_rows = list_expect_entries()[:_MAX_ROWS]
        except Exception as exc:
            log.warning("FIO Spotter: Expect administration read failed: %s", exc, exc_info=True)
            self._policy_rows, self._entry_rows = [], []
            self.expect_runtime_state.setText(
                "○ Expect storage could not be read. Restart FIO to run startup database repair; details are in the log."
            )
        selected = self.expect_policy.currentData()
        selected_manage = self.policy_manage.currentData()
        self.expect_policy.blockSignals(True); self.expect_policy.clear(); self.expect_policy.addItem("No allow policy (use rule fields below)", 0)
        self.policy_manage.blockSignals(True); self.policy_manage.clear(); self.policy_manage.addItem("New policy", 0)
        for row in self._policy_rows:
            self.expect_policy.addItem(f"{'●' if row.get('enabled') else '○'} {row.get('name')}", int(row.get('id') or 0))
            self.policy_manage.addItem(f"{'●' if row.get('enabled') else '○'} {row.get('name')}", int(row.get('id') or 0))
        self.expect_policy.setCurrentIndex(max(0, self.expect_policy.findData(selected)))
        self.policy_manage.setCurrentIndex(max(0, self.policy_manage.findData(selected_manage)))
        self.expect_policy.blockSignals(False); self.policy_manage.blockSignals(False)
        self._fit_spotter_combo(self.expect_policy, minimum_characters=20)
        self._fit_spotter_combo(self.policy_manage, minimum_characters=18)
        if selected_manage and self.policy_manage.currentData() == 0:
            # A policy may have been deleted by another administration surface
            # while this editor was open.  Do not leave its stale access data
            # staged against the now-empty selector.
            self._clear_policy_editor(reset_selection=False)
        table = self.expect_entries_table
        table.setUpdatesEnabled(False)
        table.blockSignals(True)
        try:
            table.clearContents()
            table.setRowCount(len(self._entry_rows))
            for i, row in enumerate(self._entry_rows):
                values = ("●" if row.get("enabled") else "○", row.get("expect_key"), self._expect_access_summary(row), "●" if row.get("auto_reply_enabled") else "○", row.get("max_replies"), row.get("cooldown_seconds"), self._expect_radio_label(row.get("source_scope"), row.get("source_radio_id")))
                for col, value in enumerate(values): self._put(table, i, col, value, data=row if col == 0 else None)
        finally:
            table.blockSignals(False)
            table.setUpdatesEnabled(True)
        self._refresh_expect_history()

    @staticmethod
    def _expect_access_summary(row: dict[str, Any]) -> str:
        allowed = list(row.get("allowed_callsigns") or [])
        parts: list[str] = []
        if row.get("allow_any") or "*" in allowed:
            parts.append("Any caller")
        else:
            explicit = len([value for value in allowed if value != "*"])
            if explicit:
                parts.append(f"{explicit} caller{'s' if explicit != 1 else ''}")
            if row.get("allow_trusted_operators"):
                parts.append("Trusted")
            trusted_groups = len(row.get("trusted_operator_groups") or [])
            if trusted_groups:
                parts.append(f"Trusted via {trusted_groups} group{'s' if trusted_groups != 1 else ''}")
        addressed = len(row.get("allowed_groups") or [])
        if addressed:
            parts.append(f"{addressed} query group{'s' if addressed != 1 else ''}")
        policy = _text(row.get("allow_policy_name"))
        if policy:
            parts.append(policy)
        return " · ".join(parts) or "No callers"

    def _refresh_expect_history(self) -> None:
        try:
            runtime_rows = list_expect_runtime_audit(limit=_MAX_ROWS)
            dispatch_rows = list_expect_dispatch_audit(limit=_MAX_ROWS)
        except Exception:
            runtime_rows, dispatch_rows = [], []
        for table in (self.expect_requests_table, self.expect_replies_table):
            table.setUpdatesEnabled(False)
            table.blockSignals(True)
            table.clearContents()
        try:
            self.expect_requests_table.setRowCount(min(_MAX_ROWS, len(runtime_rows)))
            for i, row in enumerate(runtime_rows[:_MAX_ROWS]):
                for col, value in enumerate((_when(row.get("created_ts")), row.get("decision"), row.get("expect_key"), row.get("requesting_callsign"), row.get("reason"))): self._put(self.expect_requests_table, i, col, value)
            self.expect_replies_table.setRowCount(min(_MAX_ROWS, len(dispatch_rows)))
            for i, row in enumerate(dispatch_rows[:_MAX_ROWS]):
                for col, value in enumerate((_when(row.get("created_ts")), row.get("decision"), row.get("reply_radio_id"), row.get("transmitted_text"))): self._put(self.expect_replies_table, i, col, value)
        finally:
            for table in (self.expect_requests_table, self.expect_replies_table):
                table.blockSignals(False)
                table.setUpdatesEnabled(True)

    def _load_selected_entry(self) -> None:
        selected = self.expect_entries_table.selectedItems()
        if not selected: return
        row = selected[0].data(Qt.UserRole) or {}
        self.expect_key.setText(_text(row.get("expect_key"))); self.expect_reply.setText(_text(row.get("response_text")))
        self.expect_policy.setCurrentIndex(max(0, self.expect_policy.findData(row.get("allow_policy_id") or 0)))
        self.expect_calls.setText(", ".join(row.get("allowed_callsigns") or ())); self.expect_groups.setText(", ".join(row.get("allowed_groups") or ())); self.expect_blocked.setText(", ".join(row.get("blocked_callsigns") or ()))
        self.expect_trusted_groups.setText(", ".join(row.get("trusted_operator_groups") or ()))
        self._select_expect_radio(row.get("source_scope"), row.get("source_radio_id"))
        self.expect_max.setValue(int(row.get("max_replies") or 1)); self.expect_cooldown.setValue(int(row.get("cooldown_seconds") or 0))
        self.expect_allow_any.setChecked(bool(row.get("allow_any") or "*" in (row.get("allowed_callsigns") or ())))
        self.expect_trusted.setChecked(bool(row.get("allow_trusted_operators")))
        self.expect_enabled.setChecked(bool(row.get("enabled"))); self.expect_auto.setChecked(bool(row.get("auto_reply_enabled"))); self.expect_unattended.setChecked(bool(row.get("unattended_auto_reply_enabled")))

    def _clear_entry(self) -> None:
        self.expect_entries_table.clearSelection(); self.expect_key.clear(); self.expect_reply.clear(); self.expect_policy.setCurrentIndex(0); self.expect_calls.clear(); self.expect_groups.clear(); self.expect_trusted_groups.clear(); self.expect_blocked.clear(); self.expect_radio.setCurrentIndex(0); self.expect_max.setValue(1); self.expect_cooldown.setValue(0); self.expect_allow_any.setChecked(False); self.expect_trusted.setChecked(False); self.expect_enabled.setChecked(False); self.expect_auto.setChecked(False); self.expect_unattended.setChecked(False)

    def _new_dynamic_q_entry(self) -> None:
        self._clear_entry()
        self.expect_key.setText("Q")
        self.expect_reply.setPlaceholderText("Generated dynamically: Q <ID> YES / NO / missing blocks")
        self.expect_enabled.setChecked(True)
        self.expect_auto.setChecked(True)
        self.expect_unattended.setChecked(True)
        self.expect_radio.setCurrentIndex(0)
        self.expect_groups.setFocus()
        self.expect_runtime_state.setText(
            "Configure explicit callers or groups (or an allow policy), then Save rule. The Q ID and response are generated from the request and indexed FLAMP state."
        )
        log.info("FIO Spotter: opened new dynamic FLAMP Q rule editor")

    def _save_entry(self) -> None:
        selected = self.expect_entries_table.selectedItems(); existing = selected[0].data(Qt.UserRole) if selected else {}
        try:
            is_dynamic_q = self.expect_key.text().strip().upper() == "Q"
            allowed_calls = _csv(self.expect_calls.text())
            allow_any = self.expect_allow_any.isChecked() or "*" in allowed_calls
            if allow_any and "*" not in allowed_calls:
                allowed_calls.insert(0, "*")
            existing = dict(existing) if isinstance(existing, dict) else {}
            source_radio_id = _text(self.expect_radio.currentData())
            source_scope = "radio" if source_radio_id else "all"
            same_radio_choice = (
                source_radio_id == _text(existing.get("source_radio_id"))
                and (
                    source_scope == _text(existing.get("source_scope")).lower()
                    or (not source_radio_id and not _text(existing.get("source_radio_id")))
                )
            )
            # Instance and schedule are routing metadata, not operator-facing
            # rule fields. Preserve legacy restrictions while the radio choice
            # is unchanged; a deliberate new radio choice adopts that radio's
            # current JS8Call configuration and normal FIO schedule.
            js8_instance_id = _text(existing.get("js8_instance_id")) if same_radio_choice else ""
            auto_tx_schedule = _text(existing.get("auto_tx_schedule")) if same_radio_choice else ""
            payload = {"expect_key": self.expect_key.text(), "response_text": self.expect_reply.text(), "allow_policy_id": self.expect_policy.currentData() or None, "allowed_callsigns": allowed_calls, "allowed_groups": _csv(self.expect_groups.text()), "allow_any": allow_any, "allow_trusted_operators": self.expect_trusted.isChecked(), "trusted_operator_groups": _csv(self.expect_trusted_groups.text()), "blocked_callsigns": _csv(self.expect_blocked.text()), "max_replies": self.expect_max.value(), "cooldown_seconds": self.expect_cooldown.value(), "auto_tx_schedule": auto_tx_schedule, "enabled": self.expect_enabled.isChecked(), "auto_reply_enabled": self.expect_auto.isChecked(), "unattended_auto_reply_enabled": self.expect_unattended.isChecked(), "source_radio_id": source_radio_id, "source_scope": source_scope or ("all" if is_dynamic_q else "radio"), "js8_instance_id": js8_instance_id, "import_source": "fio-spotter"}
            saved = save_expect_entry(payload)
        except Exception as exc:
            log.warning("FIO Spotter: Expect rule save failed: %s", exc, exc_info=True)
            self.expect_runtime_state.setText(f"○ Rule not saved: {exc}")
            return
        log.info(
            "FIO Spotter: %s Expect rule id=%s key=%s policy_id=%s scope=%s",
            "created" if saved.created else "saved",
            saved.id,
            saved.expect_key,
            payload.get("allow_policy_id") or 0,
            payload.get("source_scope"),
        )
        self.refresh_expect()

    def _delete_entry(self) -> None:
        selected = self.expect_entries_table.selectedItems()
        if selected:
            try: delete_expect_entry(int((selected[0].data(Qt.UserRole) or {}).get("id") or 0))
            except Exception: pass
        self._clear_entry(); self.refresh_expect()

    def _save_policy(self) -> None:
        try:
            saved = save_expect_allow_policy({
                "id": self.policy_manage.currentData() or 0,
                "name": self.policy_name.text(),
                "allowed_callsigns": _csv(self.policy_calls.text()),
                "allowed_groups": _csv(self.policy_groups.text()),
                "allow_trusted_operators": self.policy_trusted.isChecked(),
                "trusted_operator_groups": _csv(self.policy_trusted_groups.text()),
                "blocked_callsigns": _csv(self.policy_blocked.text()),
                "enabled": self.policy_enabled.isChecked(),
                "source_scope": self.policy_scope.currentData() or "all",
                "source_radio_ids": _csv(self.policy_radios.text()),
                "import_source": "fio-spotter",
            })
        except Exception as exc:
            log.warning("FIO Spotter: allow policy save failed: %s", exc, exc_info=True)
            self.expect_runtime_state.setText(f"○ Policy not saved: {exc}")
            return
        log.info(
            "FIO Spotter: %s Expect allow policy id=%s name=%s",
            "created" if saved.created else "saved",
            saved.id,
            saved.name,
        )
        self.refresh_expect()
        policy_index = self.expect_policy.findData(saved.id)
        if policy_index >= 0:
            self.expect_policy.setCurrentIndex(policy_index)
        manage_index = self.policy_manage.findData(saved.id)
        if manage_index >= 0:
            self.policy_manage.setCurrentIndex(manage_index)
        self.expect_runtime_state.setText(
            f"● {'Created' if saved.created else 'Saved'} allow policy “{saved.name}”. "
            "It is selected for this rule; choose Save rule to attach it."
        )

    def _load_policy_editor(self) -> None:
        policy_id = int(self.policy_manage.currentData() or 0)
        row = next((item for item in self._policy_rows if int(item.get("id") or 0) == policy_id), None)
        if row is None:
            self._clear_policy_editor(reset_selection=False)
            return
        self.policy_name.setText(_text(row.get("name")))
        self.policy_calls.setText(", ".join(row.get("allowed_callsigns") or ()))
        self.policy_groups.setText(", ".join(row.get("allowed_groups") or ()))
        self.policy_trusted.setChecked(bool(row.get("allow_trusted_operators")))
        self.policy_trusted_groups.setText(", ".join(row.get("trusted_operator_groups") or ()))
        self.policy_blocked.setText(", ".join(row.get("blocked_callsigns") or ()))
        self.policy_scope.setCurrentIndex(max(0, self.policy_scope.findData(_text(row.get("source_scope")) or "all")))
        self.policy_radios.setText(", ".join(row.get("source_radio_ids") or ()))
        self.policy_enabled.setChecked(bool(row.get("enabled")))

    def _clear_policy_editor(self, *, reset_selection: bool = True) -> None:
        if reset_selection:
            self.policy_manage.setCurrentIndex(0)
        self.policy_name.clear(); self.policy_calls.clear(); self.policy_groups.clear(); self.policy_trusted.setChecked(False); self.policy_trusted_groups.clear(); self.policy_blocked.clear(); self.policy_scope.setCurrentIndex(0); self.policy_radios.clear(); self.policy_enabled.setChecked(True)

    def _delete_policy(self) -> None:
        policy_id = int(self.policy_manage.currentData() or 0)
        if policy_id <= 0:
            self.expect_runtime_state.setText("Select an allow policy to delete.")
            return
        try:
            delete_expect_allow_policy(policy_id)
        except Exception as exc:
            self.expect_runtime_state.setText(f"○ Policy not deleted: {exc}")
            return
        self._clear_policy_editor()
        self.refresh_expect()

    # Forms / imports ------------------------------------------------------
    def _build_forms(self, page: QWidget) -> None:
        layout = self._page_layout(page)
        header = QHBoxLayout(); header.addWidget(QLabel("MCF form catalog")); header.addStretch(1)
        browse = QPushButton("Browse folder"); browse.setAccessibleName("Choose MCF forms folder"); browse.clicked.connect(self._choose_forms_folder)
        refresh = QPushButton("Refresh catalog"); refresh.setAccessibleName("Refresh Spotter form catalog"); refresh.clicked.connect(self.refresh_forms)
        classify = QPushButton("Auto-classify"); classify.setAccessibleName("Auto-classify Spotter forms"); classify.clicked.connect(self._auto_classify_forms)
        save = QPushButton("Save mappings"); save.setAccessibleName("Save Spotter form mappings"); save.clicked.connect(self._save_form_mappings)
        header.addWidget(browse); header.addWidget(refresh); header.addWidget(classify); header.addWidget(save); layout.addLayout(header)
        path_row = QHBoxLayout(); path_row.addWidget(QLabel("Forms folder"))
        self.forms_path = QLineEdit(); self.forms_path.setPlaceholderText("MCF forms folder")
        use_folder = QPushButton("Use folder"); use_folder.clicked.connect(self._use_forms_folder)
        path_row.addWidget(self.forms_path, 1); path_row.addWidget(use_folder); layout.addLayout(path_row)
        self.forms_state = QLabel(); self.forms_state.setWordWrap(True); layout.addWidget(self.forms_state)
        self.forms_table = self._table(
            ["Form", "Name", "Purpose", "Inbox", "Map", "Alert", "Net", "Status"],
            name="fioSpotterFormsTable",
        )
        forms_header = self.forms_table.horizontalHeader()
        forms_header.setSectionResizeMode(0, QHeaderView.Interactive)
        forms_header.setSectionResizeMode(1, QHeaderView.Stretch)
        forms_header.setSectionResizeMode(2, QHeaderView.Interactive)
        for column in range(3, 8):
            forms_header.setSectionResizeMode(column, QHeaderView.Interactive)
        self.forms_table.setColumnWidth(0, 90)
        purpose_width = max(
            self.forms_table.fontMetrics().horizontalAdvance(option)
            for option in PURPOSE_OPTIONS
        ) + 56
        self.forms_table.setColumnWidth(2, min(max(purpose_width, 180), 340))
        for column in range(3, 8):
            self.forms_table.setColumnWidth(column, 65)
        self.forms_table.itemSelectionChanged.connect(self._preview_selected_form)
        layout.addWidget(self.forms_table, 1)
        action_row = QHBoxLayout()
        preview = QPushButton("Preview selected")
        preview.setAccessibleName("Preview selected Spotter form")
        preview.clicked.connect(self._preview_selected_form)
        compose = QPushButton("Open Compose")
        compose.setAccessibleName("Open Messages Compose for Spotter form")
        compose.clicked.connect(self._open_spotter_compose)
        action_row.addWidget(preview); action_row.addWidget(compose); action_row.addStretch(1)
        layout.addLayout(action_row)
        self.forms_preview = QTextEdit()
        self.forms_preview.setReadOnly(True)
        self.forms_preview.setMaximumHeight(180)
        self.forms_preview.setAccessibleName("Selected Spotter form preview")
        self.forms_preview.setPlaceholderText("Select a form to review its fields and mapping.")
        layout.addWidget(self.forms_preview)

    def _refresh_forms_state(self) -> None:
        if not hasattr(self, "forms_state"):
            return
        path = _text(self.settings.get("js8_forms_path", ""))
        if hasattr(self, "forms_path") and not self.forms_path.hasFocus():
            self.forms_path.setText(path)
        self.forms_state.setText(f"Forms folder: {path or 'Not configured'}. Refresh catalog scans this folder only when requested.")

    def _choose_forms_folder(self) -> None:
        selected = QFileDialog.getExistingDirectory(self, "Select MCF forms folder", self.forms_path.text().strip())
        if selected:
            self.forms_path.setText(selected)
            self._use_forms_folder()

    def _use_forms_folder(self) -> None:
        path = _text(self.forms_path.text())
        if not path:
            self.forms_state.setText("Choose a forms folder first.")
            return
        if not Path(path).expanduser().is_dir():
            self.forms_state.setText("Forms folder was not found.")
            return
        try:
            self.settings.set("js8_forms_path", path)
            if hasattr(self.settings, "save"):
                self.settings.save()
        except Exception as exc:
            self.forms_state.setText(f"Forms folder was not saved: {exc}")
            return
        self.forms_state.setText("Forms folder saved. Refresh catalog to scan it; existing form mappings are preserved.")

    def refresh_forms(self) -> None:
        if not hasattr(self, "forms_table"): return
        path = _text(self.forms_path.text() or self.settings.get("js8_forms_path", ""))
        definitions = discover_spotter_forms(path)[:_MAX_ROWS] if path else []
        paths = {form.form_code: form.path for form in definitions}
        mappings = effective_mapping_rows(self.settings, path)[:_MAX_ROWS] if path else []
        self.forms_state.setText(
            f"Forms folder: {path or 'Not configured'} — {len(mappings)} catalog entries "
            f"(bounded to {_MAX_ROWS}). Select which FIO services receive each form, then Save mappings."
        )
        self.forms_table.setUpdatesEnabled(False)
        self.forms_table.blockSignals(True)
        try:
            self.forms_table.clearContents()
            self.forms_table.setRowCount(len(mappings))
            for row_index, mapping in enumerate(mappings):
                row = dict(mapping)
                row["path"] = paths.get(_text(row.get("form_code")), "")
                self._put(self.forms_table, row_index, 0, row.get("form_code"), data=row)
                self._put(self.forms_table, row_index, 1, row.get("title"))
                purpose = QComboBox()
                purpose.setAccessibleName(f"Purpose for {_text(row.get('form_code'))}")
                purpose.addItems(list(PURPOSE_OPTIONS))
                purpose.setCurrentText(_text(row.get("purpose")))
                self._fit_spotter_combo(purpose, minimum_characters=12)
                self.forms_table.setCellWidget(row_index, 2, purpose)
                for column, key in enumerate(("messages", "map", "alert", "net", "status"), start=3):
                    item = QTableWidgetItem("")
                    item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
                    item.setCheckState(Qt.Checked if bool(row.get(key)) else Qt.Unchecked)
                    item.setToolTip(f"Use {_text(row.get('form_code'))} for {key}")
                    self.forms_table.setItem(row_index, column, item)
        finally:
            self.forms_table.blockSignals(False)
            self.forms_table.setUpdatesEnabled(True)
        if not mappings:
            self.forms_preview.setPlainText("No MCF forms are configured. Choose the folder used by FIO Spotter, then refresh the catalog.")

    def _form_mapping_rows_from_table(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for row_index in range(self.forms_table.rowCount()):
            code_item = self.forms_table.item(row_index, 0)
            source = code_item.data(Qt.UserRole) if code_item is not None else {}
            purpose = self.forms_table.cellWidget(row_index, 2)
            values: dict[str, object] = {
                "form_code": _text((source or {}).get("form_code")),
                "title": _text((source or {}).get("title")),
                "purpose": purpose.currentText() if isinstance(purpose, QComboBox) else "",
            }
            for column, key in enumerate(("messages", "map", "alert", "net", "status"), start=3):
                item = self.forms_table.item(row_index, column)
                values[key] = bool(item is not None and item.checkState() == Qt.Checked)
            rows.append(normalize_mapping_row(values))
        return rows

    def _save_form_mappings(self) -> None:
        if not hasattr(self, "forms_table") or self.forms_table.rowCount() <= 0:
            self.forms_state.setText("No form mappings are available to save.")
            return
        rows = self._form_mapping_rows_from_table()
        try:
            self.settings.set(MAPPER_SETTINGS_KEY, rows)
            if hasattr(self.settings, "save"):
                self.settings.save()
        except Exception as exc:
            self.forms_state.setText(f"Form mappings were not saved: {exc}")
            return
        self.forms_state.setText(f"Saved {len(rows)} FIO Spotter form mapping(s).")

    def _auto_classify_forms(self) -> None:
        if not hasattr(self, "forms_table"):
            return
        for row_index in range(self.forms_table.rowCount()):
            code_item = self.forms_table.item(row_index, 0)
            source = code_item.data(Qt.UserRole) if code_item is not None else {}
            mapping = factory_mapping_for_form(
                (source or {}).get("form_code"),
                (source or {}).get("title"),
            )
            purpose = self.forms_table.cellWidget(row_index, 2)
            if isinstance(purpose, QComboBox):
                purpose.setCurrentText(_text(mapping.get("purpose")))
            for column, key in enumerate(("messages", "map", "alert", "net", "status"), start=3):
                item = self.forms_table.item(row_index, column)
                if item is not None:
                    item.setCheckState(Qt.Checked if bool(mapping.get(key)) else Qt.Unchecked)
        self.forms_state.setText("Factory classifications are staged. Choose Save mappings to apply them.")

    def _selected_form_row(self) -> dict[str, object] | None:
        if not hasattr(self, "forms_table") or self.forms_table.currentRow() < 0:
            return None
        item = self.forms_table.item(self.forms_table.currentRow(), 0)
        row = item.data(Qt.UserRole) if item is not None else None
        return dict(row) if isinstance(row, dict) else None

    def _preview_selected_form(self) -> None:
        row = self._selected_form_row()
        if not row:
            return
        path = Path(_text(row.get("path")))
        try:
            body = path.read_text(encoding="utf-8", errors="replace")[:65536]
        except Exception as exc:
            body = f"Form source is unavailable: {exc}"
        mapping = self._form_mapping_rows_from_table()[self.forms_table.currentRow()]
        routes = [key.title() for key in ("messages", "map", "alert", "net", "status") if mapping.get(key)]
        self.forms_preview.setPlainText(
            f"{_text(row.get('form_code'))} — {_text(row.get('title'))}\n"
            f"Purpose: {_text(mapping.get('purpose'))} · Routes: {', '.join(routes) or 'None'}\n\n{body}"
        )

    def _open_spotter_compose(self) -> None:
        if self._open_compose is None:
            self.forms_state.setText("Open Messages > Compose and choose Spotter to use this form.")
            return
        self._open_compose()

    def _build_imports(self, page: QWidget) -> None:
        layout = self._page_layout(page)
        intro = QLabel("Preview a JS8Spotter database before importing. Preview is read-only and bounded; import is explicit and preserves provenance in the established station store.")
        intro.setWordWrap(True); layout.addWidget(intro)
        source_row = QHBoxLayout(); source_row.addWidget(QLabel("JS8Spotter database"))
        self.import_source = QLineEdit(); self.import_source.setPlaceholderText("Select a JS8Spotter SQLite database")
        choose = QPushButton("Choose"); choose.setAccessibleName("Choose JS8Spotter import database"); choose.clicked.connect(self._choose_import_source)
        source_row.addWidget(self.import_source, 1); source_row.addWidget(choose); layout.addLayout(source_row)
        actions = QHBoxLayout()
        preview = QPushButton("Preview import"); preview.setAccessibleName("Preview JS8Spotter import counts"); preview.clicked.connect(self._preview_import)
        apply = QPushButton("Import…"); apply.setAccessibleName("Import JS8Spotter database"); apply.clicked.connect(self._apply_import)
        actions.addWidget(preview); actions.addWidget(apply); actions.addStretch(1); layout.addLayout(actions)
        self.imports_state = QLabel(); self.imports_state.setWordWrap(True); layout.addWidget(self.imports_state)
        layout.addStretch(1)

    def refresh_imports(self) -> None:
        if hasattr(self, "imports_state"):
            source = _text(self.settings.get("js8spotter_import_db_path", ""))
            if hasattr(self, "import_source") and not self.import_source.hasFocus():
                self.import_source.setText(source)
            self.imports_state.setText(f"External JS8Spotter database: {source or 'Not configured'}. Choose Preview import to inspect bounded counts before changing station data.")

    def _choose_import_source(self) -> None:
        current = _text(self.import_source.text())
        start = str(Path(current).expanduser().parent) if current else ""
        filename, _ = QFileDialog.getOpenFileName(self, "Select JS8Spotter database", start, "SQLite Databases (*.db *.sqlite *.sqlite3);;All Files (*)")
        if filename:
            self.import_source.setText(filename)
            self._save_import_source(filename)

    def _save_import_source(self, source: str) -> None:
        try:
            self.settings.set("js8spotter_import_db_path", source)
            if hasattr(self.settings, "save"):
                self.settings.save()
        except Exception:
            pass

    def _preview_import(self) -> None:
        source = _text(self.import_source.text())
        if not source:
            self.imports_state.setText("Choose a JS8Spotter database first.")
            return
        self._save_import_source(source)
        try:
            preview = preview_js8spotter_import(source, limit_per_table=5000)
        except Exception as exc:
            self.imports_state.setText(f"Preview failed: {exc}")
            return
        self._import_preview = preview
        warnings = f" Warnings: {'; '.join(preview.warnings[:2])}" if preview.warnings else ""
        self.imports_state.setText(
            f"Preview: {preview.candidates} candidates — forms {preview.forms}, Expect {preview.expect}, "
            f"watches {getattr(preview, 'watches', 0)}, archive {preview.archive}; "
            f"duplicates {preview.duplicates}, skipped {preview.skipped}, conflicts {preview.conflicts}." + warnings
        )

    def _apply_import(self) -> None:
        source = _text(self.import_source.text())
        if not source:
            self.imports_state.setText("Choose and preview a JS8Spotter database first.")
            return
        preview = getattr(self, "_import_preview", None)
        if preview is None or _text(getattr(preview, "source_db", "")) != str(Path(source).expanduser()):
            self._preview_import()
            preview = getattr(self, "_import_preview", None)
        if preview is None or getattr(preview, "warnings", ()):
            self.imports_state.setText("Import was not started; resolve preview warnings first.")
            return
        answer = QMessageBox.question(self, "Import JS8Spotter database", f"Import {preview.candidates} previewed records from\n{source}?\n\nThis is an explicit station-data change.", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if answer != QMessageBox.Yes:
            self.imports_state.setText("Import cancelled.")
            return
        try:
            stats = import_js8spotter_database(source)
        except Exception as exc:
            self.imports_state.setText(f"Import failed: {exc}")
            return
        self._save_import_source(source)
        errors = f" Errors: {'; '.join(stats.errors[:2])}" if stats.errors else ""
        self.imports_state.setText(
            f"Imported: forms {stats.forms_imported}/{stats.forms_scanned}, "
            f"Expect {stats.expect_imported}/{stats.expect_scanned}, "
            f"watches {stats.watches_imported}/{stats.watches_scanned}, "
            f"archive {stats.archive_imported}/{stats.archive_scanned}." + errors
        )
