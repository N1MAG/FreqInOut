from __future__ import annotations

"""Station-owned administration surface for the shared Managed BBS catalog.

This tab owns operator-facing BBS administration. It uses bounded catalog
interfaces and may update a configured radio's BBS adapter fields, but it does
not scan folders, reconcile sources, or publish a live directory on the UI
thread. Those operations remain owned by their background services.
"""

from collections import defaultdict
from dataclasses import dataclass
import datetime as dt
import re
from pathlib import Path
from typing import Iterable, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.settings_manager import SettingsManager
from freqinout.core.message_file_scanner import is_fio_bbs_helper_file_name
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.varac_bbs_library_store import (
    BbsArtifactAdminRow,
    BbsLocationRecord,
    bbs_library_db_path_from_settings,
    list_bbs_admin_rows,
    list_bbs_artifact_location_ids,
    list_bbs_locations,
    set_bbs_artifact_locations,
    upsert_bbs_location,
)
from freqinout.core.varac_bbs_vault import hash_access_code
from freqinout.gui.theme import button_style, resolve_theme


MAX_ARTIFACT_ROWS = 200
_LOCATION_ID_ROLE = Qt.UserRole
_ARTIFACT_ID_ROLE = Qt.UserRole + 1


@dataclass(frozen=True)
class _ArtifactDisplay:
    row: BbsArtifactAdminRow
    rows: tuple[BbsArtifactAdminRow, ...]
    location_names: tuple[str, ...]
    published_location_ids: tuple[str, ...]


def _access_text(value: object) -> str:
    text = str(value or "public").strip().replace("_", " ")
    return text.title() if text else "Public"


def _retention_text(mode: object, days: object) -> str:
    normalized = str(mode or "global_default").strip().lower()
    try:
        day_count = int(days or 0)
    except (TypeError, ValueError):
        day_count = 0
    if normalized == "expire_after_days":
        return f"Expires after {day_count} day{'s' if day_count != 1 else ''}"
    if normalized == "manual":
        return "Keep until manually removed"
    return "Station default retention"


def _health_text(row: BbsArtifactAdminRow) -> str:
    state = str(row.publication_state or "").strip().lower()
    if state == "published":
        return "Published"
    if state == "source_missing":
        return "Missing source"
    if state == "deleted":
        return "Deleted source"
    if state == "retention_expired":
        return "Retention expired"
    if state == "operator_disabled":
        return "Operator disabled"
    return state.replace("_", " ").title() or "Not published"


def _byte_text(value: object) -> str:
    try:
        size = max(0, int(value or 0))
    except (TypeError, ValueError):
        return "Unknown"
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:g} {unit}" if unit == "B" else f"{size / 1:g} {unit}"
        size /= 1024
    return f"{size:g} GB"


def _modified_detail(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "modified time unknown"
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        utc_value = parsed.astimezone(dt.timezone.utc)
        local_value = parsed.astimezone()
        return f"Local {local_value.strftime('%Y-%m-%d %H:%M:%S %Z')}; UTC {utc_value.strftime('%Y-%m-%d %H:%M:%S Z')}"
    except ValueError:
        return f"modified {raw}"


class StationBbsTab(QWidget):
    """A bounded, database-backed catalog view for station Managed BBS work."""

    def __init__(self, parent: Optional[QWidget] = None, *, settings: object | None = None) -> None:
        super().__init__(parent)
        self.settings = settings if settings is not None else SettingsManager()
        self._selected_location_id = ""
        self._publishing_location_id = ""
        self._locations_by_id: dict[str, BbsLocationRecord] = {}
        self._display_rows: dict[str, _ArtifactDisplay] = {}
        self._loading = False
        self._compact_layout = False
        self._editing_location_id = ""
        self._location_editor_loading = False
        self._station_allowed_callsigns: set[str] = set()
        self._station_limit_access_enabled = False
        self._build_ui()
        self.refresh_catalog()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        title_row = QHBoxLayout()
        title = QLabel("FIO BBS")
        title.setObjectName("stationBbsTitle")
        title.setStyleSheet("font-weight: 700; font-size: 16px;")
        title.setAccessibleName("FIO BBS")
        title_row.addWidget(title)
        title_row.addStretch(1)
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setToolTip("Refresh the bounded Managed BBS catalog view.")
        self.refresh_btn.setAccessibleName("Refresh Managed BBS catalog")
        self.refresh_btn.clicked.connect(self.refresh_catalog)
        title_row.addWidget(self.refresh_btn)
        layout.addLayout(title_row)

        self.why_label = QLabel(
            "One station BBS: configure the radios that serve it, define locations and access, then publish files. "
            "FIO projects the same catalog to each enabled VarAC radio without deleting source files."
        )
        self.why_label.setObjectName("stationBbsWhy")
        self.why_label.setWordWrap(True)
        self.why_label.setAccessibleName("Managed BBS explanation")
        layout.addWidget(self.why_label)

        self.summary_label = QLabel("Loading Managed BBS catalog…")
        self.summary_label.setWordWrap(True)
        self.summary_label.setObjectName("stationBbsSummary")
        layout.addWidget(self.summary_label)

        self.service_tabs = QTabWidget(self)
        self.service_tabs.setObjectName("stationBbsServiceTabs")
        self.service_tabs.setAccessibleName("BBS service workspace")
        self.service_tabs.setUsesScrollButtons(True)
        self.service_tabs.setDocumentMode(True)
        layout.addWidget(self.service_tabs, 1)

        self.overview_page = QWidget(self.service_tabs)
        overview_layout = QVBoxLayout(self.overview_page)
        overview_layout.setContentsMargins(10, 10, 10, 10)
        overview_layout.setSpacing(10)
        overview_title = QLabel("BBS service overview")
        overview_title.setStyleSheet("font-weight: 700; font-size: 15px;")
        overview_layout.addWidget(overview_title)
        overview_copy = QLabel(
            "Follow the tabs from left to right: choose which radios serve the BBS, organize locations and access, "
            "publish operator files, then verify the visitor view and generated system helpers."
        )
        overview_copy.setWordWrap(True)
        overview_copy.setAccessibleName("BBS overview guidance")
        overview_layout.addWidget(overview_copy)
        self.overview_status_label = QLabel("Catalog status is loading…")
        self.overview_status_label.setWordWrap(True)
        self.overview_status_label.setAccessibleName("BBS overview status")
        overview_layout.addWidget(self.overview_status_label)
        overview_layout.addStretch(1)

        self.radio_service_page = QWidget(self.service_tabs)
        radio_layout = QVBoxLayout(self.radio_service_page)
        radio_layout.setContentsMargins(10, 10, 10, 10)
        radio_layout.setSpacing(8)
        radio_title = QLabel("Radio Service")
        radio_title.setStyleSheet("font-weight: 700; font-size: 15px;")
        radio_layout.addWidget(radio_title)
        radio_copy = QLabel(
            "Choose the VarAC radios that serve this BBS. Each radio has its own live folder but publishes the same "
            "station catalog. VarAC launcher, inbox, and outbox paths remain in Radio Settings."
        )
        radio_copy.setWordWrap(True)
        radio_copy.setAccessibleName("Radio Service summary")
        radio_layout.addWidget(radio_copy)
        self.radio_service_table = QTableWidget(0, 4, self.radio_service_page)
        self.radio_service_table.setObjectName("stationBbsRadioServiceTable")
        self.radio_service_table.setHorizontalHeaderLabels(["Radio", "FIO BBS", "Live folder", "Status"])
        self.radio_service_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.radio_service_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.radio_service_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.radio_service_table.verticalHeader().setVisible(False)
        self.radio_service_table.horizontalHeader().setStretchLastSection(True)
        self.radio_service_table.itemSelectionChanged.connect(self._load_selected_radio_service)
        radio_layout.addWidget(self.radio_service_table, 1)

        radio_editor = QGroupBox("Selected radio BBS service", self.radio_service_page)
        radio_editor_layout = QGridLayout(radio_editor)
        radio_editor_layout.setContentsMargins(8, 8, 8, 8)
        radio_editor_layout.setHorizontalSpacing(8)
        radio_editor_layout.setVerticalSpacing(6)
        self.radio_service_enabled_chk = QCheckBox("Enable VarAC BBS")
        self.radio_publish_enabled_chk = QCheckBox("Publish the FIO catalog")
        self.radio_announce_enabled_chk = QCheckBox("Announce BBS")
        radio_editor_layout.addWidget(self.radio_service_enabled_chk, 0, 0)
        radio_editor_layout.addWidget(self.radio_publish_enabled_chk, 0, 1)
        radio_editor_layout.addWidget(self.radio_announce_enabled_chk, 0, 2)
        radio_editor_layout.addWidget(QLabel("Live BBS folder"), 1, 0)
        self.radio_live_dir_edit = QLineEdit()
        self.radio_live_dir_edit.setPlaceholderText("VarAC live BBS folder for the selected radio")
        self.radio_live_dir_edit.setAccessibleName("Selected radio live BBS folder")
        radio_editor_layout.addWidget(self.radio_live_dir_edit, 1, 1)
        self.radio_live_dir_browse_btn = QPushButton("Browse")
        self.radio_live_dir_browse_btn.clicked.connect(self._browse_radio_live_dir)
        radio_editor_layout.addWidget(self.radio_live_dir_browse_btn, 1, 2)
        self.radio_native_paths_label = QLabel("Select a configured radio.")
        self.radio_native_paths_label.setWordWrap(True)
        self.radio_native_paths_label.setAccessibleName("Selected radio native VarAC path summary")
        radio_editor_layout.addWidget(self.radio_native_paths_label, 2, 0, 1, 3)
        self.radio_service_save_btn = QPushButton("Save Radio Service")
        self.radio_service_save_btn.clicked.connect(self._save_selected_radio_service)
        radio_editor_layout.addWidget(self.radio_service_save_btn, 3, 0)
        self.radio_settings_btn = QPushButton("Open Radio Settings")
        self.radio_settings_btn.clicked.connect(self._open_radio_settings)
        radio_editor_layout.addWidget(self.radio_settings_btn, 3, 1)
        self.radio_service_status = QLabel("Select a configured VarAC radio to review its BBS service.")
        self.radio_service_status.setWordWrap(True)
        self.radio_service_status.setAccessibleName("Radio Service status")
        radio_editor_layout.addWidget(self.radio_service_status, 4, 0, 1, 3)
        radio_layout.addWidget(radio_editor, 0)
        self._radio_profiles_by_id: dict[int, dict[str, object]] = {}
        self._radio_service_loading = False

        self.locations_page = QWidget(self.service_tabs)
        locations_layout = QVBoxLayout(self.locations_page)
        locations_layout.setContentsMargins(0, 0, 0, 0)
        self.publishing_page = QWidget(self.service_tabs)
        publishing_layout = QVBoxLayout(self.publishing_page)
        publishing_layout.setContentsMargins(0, 0, 0, 0)
        publishing_scope_row = QHBoxLayout()
        publishing_scope_row.setContentsMargins(8, 8, 8, 0)
        publishing_scope_row.addWidget(QLabel("Publish in"))
        self.publishing_location_combo = QComboBox(self.publishing_page)
        self.publishing_location_combo.setAccessibleName("Publishing location")
        self.publishing_location_combo.currentIndexChanged.connect(self._on_publishing_location_changed)
        publishing_scope_row.addWidget(self.publishing_location_combo, 1)
        self.manage_locations_btn = QPushButton("Manage Locations")
        self.manage_locations_btn.clicked.connect(
            lambda: self.service_tabs.setCurrentWidget(self.locations_page)
        )
        publishing_scope_row.addWidget(self.manage_locations_btn)
        publishing_layout.addLayout(publishing_scope_row)

        self.visitor_preview_page = QWidget(self.service_tabs)
        visitor_layout = QVBoxLayout(self.visitor_preview_page)
        visitor_layout.setContentsMargins(10, 10, 10, 10)
        visitor_layout.setSpacing(7)
        visitor_title = QLabel("Visitor Preview")
        visitor_title.setStyleSheet("font-weight: 700; font-size: 15px;")
        visitor_layout.addWidget(visitor_title)
        visitor_copy = QLabel(
            "Read-only caller-facing view. It applies enabled locations and visibility rules, but does not validate or reveal access codes."
        )
        visitor_copy.setWordWrap(True)
        visitor_copy.setAccessibleName("Visitor Preview explanation")
        visitor_layout.addWidget(visitor_copy)
        visitor_row = QHBoxLayout()
        visitor_row.setContentsMargins(0, 0, 0, 0)
        visitor_row.addWidget(QLabel("Visitor callsign"))
        self.visitor_callsign_edit = QLineEdit()
        self.visitor_callsign_edit.setPlaceholderText("Optional visitor callsign")
        self.visitor_callsign_edit.setAccessibleName("Visitor preview callsign")
        self.visitor_callsign_edit.setMaximumWidth(240)
        self.visitor_callsign_edit.textChanged.connect(self._refresh_visitor_preview)
        visitor_row.addWidget(self.visitor_callsign_edit)
        visitor_row.addStretch(1)
        visitor_layout.addLayout(visitor_row)
        self.visitor_preview_status = QLabel("Loading visitor preview…")
        self.visitor_preview_status.setWordWrap(True)
        self.visitor_preview_status.setAccessibleName("Visitor Preview status")
        visitor_layout.addWidget(self.visitor_preview_status)
        self.visitor_preview_tree = QTreeWidget(self.visitor_preview_page)
        self.visitor_preview_tree.setHeaderHidden(True)
        self.visitor_preview_tree.setAccessibleName("Visitor visible BBS locations")
        self.visitor_preview_tree.setToolTip("Read-only locations visible to the entered visitor callsign.")
        visitor_layout.addWidget(self.visitor_preview_tree, 1)
        self.visitor_artifact_table = QTableWidget(0, 4, self.visitor_preview_page)
        self.visitor_artifact_table.setHorizontalHeaderLabels(["File", "Location", "Access", "Health"])
        self.visitor_artifact_table.setAccessibleName("Visitor visible BBS artifacts")
        self.visitor_artifact_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.visitor_artifact_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.visitor_artifact_table.verticalHeader().setVisible(False)
        self.visitor_artifact_table.horizontalHeader().setStretchLastSection(True)
        visitor_layout.addWidget(self.visitor_artifact_table, 1)

        self.helpers_page = QWidget(self.service_tabs)
        helpers_layout = QVBoxLayout(self.helpers_page)
        helpers_layout.setContentsMargins(10, 10, 10, 10)
        helpers_layout.setSpacing(7)
        helpers_title = QLabel("System Helpers")
        helpers_title.setStyleSheet("font-weight: 700; font-size: 15px;")
        helpers_layout.addWidget(helpers_title)
        helpers_copy = QLabel(
            "Generated helper files are system-owned support material. They are intentionally excluded from Publishing, "
            "so changing catalog membership can never replace or delete them."
        )
        helpers_copy.setWordWrap(True)
        helpers_copy.setAccessibleName("System Helpers explanation")
        helpers_layout.addWidget(helpers_copy)
        self.helpers_status = QLabel("No generated helper files are present in the bounded catalog view.")
        self.helpers_status.setWordWrap(True)
        self.helpers_status.setAccessibleName("System Helpers status")
        helpers_layout.addWidget(self.helpers_status)
        self.helpers_table = QTableWidget(0, 6, self.helpers_page)
        self.helpers_table.setHorizontalHeaderLabels(
            ["Helper", "Purpose", "Compatibility file", "Locations", "Age", "Health"]
        )
        self.helpers_table.setAccessibleName("System generated BBS helpers")
        self.helpers_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.helpers_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.helpers_table.verticalHeader().setVisible(False)
        self.helpers_table.horizontalHeader().setStretchLastSection(True)
        helpers_layout.addWidget(self.helpers_table, 1)

        self.splitter = QSplitter(Qt.Horizontal, self.locations_page)
        self.splitter.setObjectName("stationBbsCatalogSplitter")
        self.splitter.setChildrenCollapsible(False)
        locations_layout.addWidget(self.splitter, 1)

        tree_panel = QWidget(self.splitter)
        tree_layout = QVBoxLayout(tree_panel)
        tree_layout.setContentsMargins(0, 0, 0, 0)
        tree_layout.setSpacing(5)
        tree_title_row = QHBoxLayout()
        tree_title = QLabel("Locations")
        tree_title.setStyleSheet("font-weight: 700;")
        tree_title_row.addWidget(tree_title)
        tree_title_row.addStretch(1)
        self.location_add_btn = QPushButton("Add")
        self.location_add_btn.setToolTip("Add a station-owned Managed BBS location. This never creates a folder.")
        self.location_add_btn.setAccessibleName("Add Managed BBS location")
        self.location_add_btn.clicked.connect(self._begin_new_location)
        tree_title_row.addWidget(self.location_add_btn)
        self.location_edit_btn = QToolButton(tree_panel)
        self.location_edit_btn.setText("Edit")
        self.location_edit_btn.setCheckable(True)
        self.location_edit_btn.setToolTip("Show or hide the selected location editor.")
        self.location_edit_btn.setAccessibleName("Show location editor")
        self.location_edit_btn.toggled.connect(self._set_location_editor_visible)
        tree_title_row.addWidget(self.location_edit_btn)
        tree_layout.addLayout(tree_title_row)
        self.location_tree = QTreeWidget(tree_panel)
        self.location_tree.setObjectName("stationBbsLocationTree")
        self.location_tree.setHeaderHidden(True)
        self.location_tree.setAccessibleName("Managed BBS locations")
        self.location_tree.setToolTip("Select a location to review and change its artifact memberships.")
        self.location_tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.location_tree.itemSelectionChanged.connect(self._on_location_selection_changed)
        tree_layout.addWidget(self.location_tree, 1)

        self.location_editor = QFrame(tree_panel)
        self.location_editor.setObjectName("stationBbsLocationEditor")
        self.location_editor.setVisible(False)
        editor_layout = QVBoxLayout(self.location_editor)
        editor_layout.setContentsMargins(8, 8, 8, 8)
        editor_layout.setSpacing(5)
        editor_title = QLabel("Location editor")
        editor_title.setStyleSheet("font-weight: 700;")
        editor_layout.addWidget(editor_title)
        editor_grid = QGridLayout()
        editor_grid.setContentsMargins(0, 0, 0, 0)
        editor_grid.setHorizontalSpacing(6)
        editor_grid.setVerticalSpacing(4)
        editor_grid.addWidget(QLabel("Name"), 0, 0)
        self.location_name_edit = QLineEdit()
        self.location_name_edit.setAccessibleName("Location name")
        self.location_name_edit.setPlaceholderText("Example: Logistics")
        editor_grid.addWidget(self.location_name_edit, 0, 1)
        editor_grid.addWidget(QLabel("Source folder"), 1, 0)
        self.location_source_edit = QLineEdit()
        self.location_source_edit.setAccessibleName("Location source folder")
        self.location_source_edit.setPlaceholderText("Existing folder path; FIO will not create it here")
        self.location_source_edit.setToolTip("Records a source folder path only. Saving never creates, scans, or changes that folder.")
        editor_grid.addWidget(self.location_source_edit, 1, 1)
        editor_grid.addWidget(QLabel("Parent"), 2, 0)
        self.location_parent_combo = QComboBox()
        self.location_parent_combo.setAccessibleName("Optional parent location")
        editor_grid.addWidget(self.location_parent_combo, 2, 1)
        editor_grid.addWidget(QLabel("Access"), 3, 0)
        self.location_access_combo = QComboBox()
        self.location_access_combo.setAccessibleName("Location access rule")
        self.location_access_combo.addItem("Public", "Public")
        self.location_access_combo.addItem("Allowed callsigns", "Allowed callsigns only")
        self.location_access_combo.addItem("Access code", "Access code required")
        self.location_access_combo.addItem("Allowed callsigns + code", "Allowed callsigns + access code")
        self.location_access_combo.currentIndexChanged.connect(self._update_location_access_state)
        editor_grid.addWidget(self.location_access_combo, 3, 1)
        self.location_callsigns_label = QLabel("Allowed callsigns")
        editor_grid.addWidget(self.location_callsigns_label, 4, 0)
        self.location_callsigns_edit = QLineEdit()
        self.location_callsigns_edit.setAccessibleName("Location allowed callsigns")
        self.location_callsigns_edit.setPlaceholderText("N1ABC, K2XYZ")
        editor_grid.addWidget(self.location_callsigns_edit, 4, 1)
        self.location_code_label = QLabel("Access code")
        editor_grid.addWidget(self.location_code_label, 5, 0)
        code_row = QHBoxLayout()
        code_row.setContentsMargins(0, 0, 0, 0)
        self.location_code_edit = QLineEdit()
        self.location_code_edit.setEchoMode(QLineEdit.Password)
        self.location_code_edit.setAccessibleName("New location access code")
        self.location_code_edit.setPlaceholderText("Leave blank to keep the saved code")
        code_row.addWidget(self.location_code_edit, 1)
        self.location_code_confirm_edit = QLineEdit()
        self.location_code_confirm_edit.setEchoMode(QLineEdit.Password)
        self.location_code_confirm_edit.setAccessibleName("Confirm new location access code")
        self.location_code_confirm_edit.setPlaceholderText("Confirm new code")
        code_row.addWidget(self.location_code_confirm_edit, 1)
        editor_grid.addLayout(code_row, 5, 1)
        editor_grid.addWidget(QLabel("Retention"), 6, 0)
        retention_row = QHBoxLayout()
        retention_row.setContentsMargins(0, 0, 0, 0)
        self.location_retention_combo = QComboBox()
        self.location_retention_combo.setAccessibleName("Location retention mode")
        self.location_retention_combo.addItem("Station default", "global_default")
        self.location_retention_combo.addItem("Keep until removed", "manual")
        self.location_retention_combo.addItem("Expire after days", "expire_after_days")
        self.location_retention_combo.currentIndexChanged.connect(self._update_location_retention_days_state)
        retention_row.addWidget(self.location_retention_combo, 1)
        self.location_retention_days_spin = QSpinBox()
        self.location_retention_days_spin.setRange(1, 3650)
        self.location_retention_days_spin.setValue(14)
        self.location_retention_days_spin.setSuffix(" days")
        self.location_retention_days_spin.setAccessibleName("Retention days")
        retention_row.addWidget(self.location_retention_days_spin)
        editor_grid.addLayout(retention_row, 6, 1)
        self.location_enabled_chk = QCheckBox("Enabled")
        self.location_enabled_chk.setAccessibleName("Location enabled")
        editor_grid.addWidget(self.location_enabled_chk, 7, 1)
        editor_layout.addLayout(editor_grid)
        editor_actions = QHBoxLayout()
        editor_actions.setContentsMargins(0, 0, 0, 0)
        self.location_save_btn = QPushButton("Save Location")
        self.location_save_btn.setToolTip("Save this station catalog location. No folder is created or scanned.")
        self.location_save_btn.clicked.connect(self._save_location)
        editor_actions.addWidget(self.location_save_btn)
        self.location_disable_btn = QPushButton("Disable")
        self.location_disable_btn.setToolTip("Safely disable this location without deleting its catalog history or source files.")
        self.location_disable_btn.clicked.connect(self._disable_location)
        editor_actions.addWidget(self.location_disable_btn)
        editor_actions.addStretch(1)
        editor_layout.addLayout(editor_actions)
        self.location_editor_status = QLabel("")
        self.location_editor_status.setWordWrap(True)
        self.location_editor_status.setAccessibleName("Location editor status")
        editor_layout.addWidget(self.location_editor_status)
        tree_layout.addWidget(self.location_editor, 0)
        self.splitter.addWidget(tree_panel)

        location_context_panel = QWidget(self.splitter)
        location_context_layout = QVBoxLayout(location_context_panel)
        location_context_layout.setContentsMargins(10, 10, 10, 10)
        location_context_layout.setSpacing(8)
        location_context_title = QLabel("Location policy")
        location_context_title.setStyleSheet("font-weight: 700;")
        location_context_layout.addWidget(location_context_title)
        self.location_context_label = QLabel(
            "Select a location to review its access and retention labels. Use Edit to reveal the progressive location editor."
        )
        self.location_context_label.setWordWrap(True)
        self.location_context_label.setAccessibleName("Selected location policy summary")
        location_context_layout.addWidget(self.location_context_label)
        location_context_layout.addStretch(1)
        self.splitter.addWidget(location_context_panel)

        content_panel = QWidget(self.publishing_page)
        content_layout = QVBoxLayout(content_panel)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(6)
        artifact_title_row = QHBoxLayout()
        artifact_title_row.setContentsMargins(0, 0, 0, 0)
        self.artifact_heading = QLabel("Artifacts")
        self.artifact_heading.setStyleSheet("font-weight: 700;")
        artifact_title_row.addWidget(self.artifact_heading)
        artifact_title_row.addStretch(1)
        self.detail_toggle_btn = QToolButton(content_panel)
        self.detail_toggle_btn.setText("Details")
        self.detail_toggle_btn.setCheckable(True)
        self.detail_toggle_btn.setChecked(False)
        self.detail_toggle_btn.setToolTip("Show or hide details for the selected artifact.")
        self.detail_toggle_btn.setAccessibleName("Show artifact details")
        self.detail_toggle_btn.toggled.connect(self._update_detail_visibility)
        self.detail_toggle_btn.setVisible(False)
        artifact_title_row.addWidget(self.detail_toggle_btn)
        content_layout.addLayout(artifact_title_row)
        self.artifact_table = QTableWidget(0, 6, content_panel)
        self.artifact_table.setObjectName("stationBbsArtifactTable")
        self.artifact_table.setAccessibleName("Managed BBS artifacts")
        self.artifact_table.setToolTip("Published is a managed catalog membership, not a live-folder copy.")
        self.artifact_table.setHorizontalHeaderLabels(["Published", "File", "Locations", "Age", "Retention", "Health"])
        self.artifact_table.verticalHeader().setVisible(False)
        self.artifact_table.setAlternatingRowColors(True)
        self.artifact_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.artifact_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.artifact_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.artifact_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.artifact_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        header = self.artifact_table.horizontalHeader()
        header.setStretchLastSection(True)
        self.artifact_table.setColumnWidth(0, 84)
        self.artifact_table.setColumnWidth(1, 230)
        self.artifact_table.setColumnWidth(2, 180)
        self.artifact_table.setColumnWidth(3, 64)
        self.artifact_table.setColumnWidth(4, 170)
        self.artifact_table.itemSelectionChanged.connect(self._refresh_detail_panel)
        self.artifact_table.itemChanged.connect(self._on_publication_item_changed)
        content_layout.addWidget(self.artifact_table, 1)

        self.detail_group = QGroupBox("Artifact details", content_panel)
        self.detail_group.setObjectName("stationBbsArtifactDetails")
        self.detail_group.setAccessibleName("Selected Managed BBS artifact details")
        details = QGridLayout(self.detail_group)
        details.setContentsMargins(8, 8, 8, 8)
        details.setHorizontalSpacing(10)
        details.setVerticalSpacing(4)
        self.detail_labels: dict[str, QLabel] = {}
        for index, (key, label) in enumerate(
            (
                ("origin", "Origin"),
                ("path", "Path"),
                ("size", "Size"),
                ("age", "Age / modified"),
                ("access", "Access"),
                ("retention", "Retention / expiry"),
                ("health", "Publication health"),
            )
        ):
            left = QLabel(f"{label}:")
            left.setStyleSheet("font-weight: 600;")
            value = QLabel("Select an artifact.")
            value.setWordWrap(True)
            value.setTextInteractionFlags(Qt.TextSelectableByMouse)
            value.setAccessibleName(label)
            details.addWidget(left, index, 0, Qt.AlignTop)
            details.addWidget(value, index, 1)
            self.detail_labels[key] = value
        content_layout.addWidget(self.detail_group, 0)
        publishing_layout.addWidget(content_panel, 1)
        self.splitter.setStretchFactor(0, 0)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setSizes([280, 760])

        self.service_tabs.addTab(self.overview_page, "Overview")
        self.service_tabs.addTab(self.radio_service_page, "Radio Service")
        self.service_tabs.addTab(self.locations_page, "Locations & Access")
        self.service_tabs.addTab(self.publishing_page, "Publishing")
        self.service_tabs.addTab(self.visitor_preview_page, "Visitor Preview")
        self.service_tabs.addTab(self.helpers_page, "System Helpers")
        self.apply_theme()

    def apply_theme(self) -> None:
        theme = resolve_theme(self.settings)
        self.refresh_btn.setStyleSheet(button_style("muted", theme))
        self.radio_service_save_btn.setStyleSheet(button_style("primary", theme))
        self.radio_live_dir_browse_btn.setStyleSheet(button_style("muted", theme))
        self.radio_settings_btn.setStyleSheet(button_style("muted", theme))
        self.location_add_btn.setStyleSheet(button_style("muted", theme))
        self.location_save_btn.setStyleSheet(button_style("primary", theme))
        self.location_disable_btn.setStyleSheet(button_style("warning", theme))
        self.manage_locations_btn.setStyleSheet(button_style("muted", theme))
        self.detail_group.setStyleSheet(
            f"QGroupBox {{ border: 1px solid {theme.get('border', '#d0d7de')}; border-radius: 4px; margin-top: 8px; }} "
            "QGroupBox::title { subcontrol-origin: margin; left: 8px; padding: 0 3px; }"
        )
        self.location_editor.setStyleSheet(
            f"QFrame#stationBbsLocationEditor {{ border: 1px solid {theme.get('border', '#d0d7de')}; border-radius: 4px; }}"
        )

    def _radio_store(self) -> MultiRadioStore:
        host_store = getattr(self.window(), "multi_radio_store", None)
        if callable(getattr(host_store, "list_device_profiles", None)) and callable(
            getattr(host_store, "save_device_profile", None)
        ):
            return host_store
        db_path = str(getattr(self.settings, "db_path", "") or "").strip()
        return MultiRadioStore(Path(db_path)) if db_path else MultiRadioStore()

    def _refresh_radio_services(self) -> None:
        selected_id = self._selected_radio_service_id()
        try:
            profiles = list(self._radio_store().list_device_profiles())
        except Exception as exc:
            self._radio_profiles_by_id = {}
            self.radio_service_table.setRowCount(0)
            self.radio_service_status.setText(f"Radio BBS services could not be read: {exc}")
            self._set_radio_service_editor_enabled(False)
            return
        self._radio_profiles_by_id = {
            int(profile.get("id", 0) or 0): dict(profile)
            for profile in profiles
            if int(profile.get("id", 0) or 0) > 0
        }
        self._radio_service_loading = True
        try:
            self.radio_service_table.clearContents()
            self.radio_service_table.setRowCount(len(profiles))
            selected_row = -1
            for row_index, profile in enumerate(profiles):
                profile_id = int(profile.get("id", 0) or 0)
                name = str(profile.get("name", "") or f"Radio {profile_id}")
                use_varac = bool(profile.get("use_varac", False))
                publish = bool(profile.get("varac_bbs_vault_enabled", False))
                live_dir = str(profile.get("varac_bbs_dir", "") or "").strip()
                if not use_varac:
                    service_text = "Unavailable"
                    status = "Enable VarAC in Radio Settings"
                elif not live_dir:
                    service_text = "Not configured"
                    status = "Live folder needed"
                elif publish and bool(profile.get("varac_bbs_enabled", False)):
                    service_text = "Published"
                    status = "Ready"
                elif publish:
                    service_text = "Blocked"
                    status = "Enable VarAC BBS"
                else:
                    service_text = "Paused"
                    status = "Catalog publication off"
                values = (name, service_text, live_dir or "—", status)
                for column, value in enumerate(values):
                    item = QTableWidgetItem(value)
                    item.setData(_LOCATION_ID_ROLE, profile_id)
                    item.setToolTip(value)
                    self.radio_service_table.setItem(row_index, column, item)
                if profile_id == selected_id:
                    selected_row = row_index
            if profiles:
                self.radio_service_table.selectRow(selected_row if selected_row >= 0 else 0)
        finally:
            self._radio_service_loading = False
        self._load_selected_radio_service()

    def _selected_radio_service_id(self) -> int:
        row_index = self.radio_service_table.currentRow() if hasattr(self, "radio_service_table") else -1
        item = self.radio_service_table.item(row_index, 0) if row_index >= 0 else None
        try:
            return int(item.data(_LOCATION_ID_ROLE) or 0) if item is not None else 0
        except (TypeError, ValueError):
            return 0

    def _set_radio_service_editor_enabled(self, enabled: bool) -> None:
        for widget in (
            self.radio_service_enabled_chk,
            self.radio_publish_enabled_chk,
            self.radio_announce_enabled_chk,
            self.radio_live_dir_edit,
            self.radio_live_dir_browse_btn,
            self.radio_service_save_btn,
        ):
            widget.setEnabled(bool(enabled))

    def _load_selected_radio_service(self) -> None:
        if self._radio_service_loading:
            return
        profile = self._radio_profiles_by_id.get(self._selected_radio_service_id())
        self._radio_service_loading = True
        try:
            if profile is None:
                self.radio_service_enabled_chk.setChecked(False)
                self.radio_publish_enabled_chk.setChecked(False)
                self.radio_announce_enabled_chk.setChecked(False)
                self.radio_live_dir_edit.clear()
                self.radio_native_paths_label.setText("Select a configured radio.")
                self.radio_service_status.setText("No configured radio is selected.")
                self._set_radio_service_editor_enabled(False)
                return
            use_varac = bool(profile.get("use_varac", False))
            self.radio_service_enabled_chk.setChecked(bool(profile.get("varac_bbs_enabled", False)))
            self.radio_publish_enabled_chk.setChecked(bool(profile.get("varac_bbs_vault_enabled", False)))
            self.radio_announce_enabled_chk.setChecked(bool(profile.get("varac_bbs_announce_enabled", False)))
            self.radio_live_dir_edit.setText(str(profile.get("varac_bbs_dir", "") or ""))
            install = str(profile.get("varac_install_path", "") or "Not configured")
            outbox = str(profile.get("varac_outbox_dir", "") or "Not configured")
            self.radio_native_paths_label.setText(
                f"Native VarAC paths (managed in Radio Settings) · Install: {install} · Outbox: {outbox}"
            )
            self._set_radio_service_editor_enabled(use_varac)
            self.radio_settings_btn.setEnabled(True)
            self.radio_service_status.setText(
                "Configure the live BBS service here."
                if use_varac
                else "VarAC is not enabled for this radio. Open Radio Settings first."
            )
        finally:
            self._radio_service_loading = False

    def _browse_radio_live_dir(self) -> None:
        start = self.radio_live_dir_edit.text().strip()
        selected = QFileDialog.getExistingDirectory(self, "Select VarAC Live BBS Folder", start)
        if selected:
            self.radio_live_dir_edit.setText(selected)

    def _save_selected_radio_service(self) -> None:
        profile_id = self._selected_radio_service_id()
        profile = self._radio_profiles_by_id.get(profile_id)
        if profile is None:
            self.radio_service_status.setText("Select a configured VarAC radio before saving.")
            return
        if not bool(profile.get("use_varac", False)):
            self.radio_service_status.setText("Enable VarAC for this radio in Radio Settings before configuring its BBS service.")
            return
        live_dir = self.radio_live_dir_edit.text().strip()
        if self.radio_publish_enabled_chk.isChecked() and not self.radio_service_enabled_chk.isChecked():
            self.radio_service_status.setText("Enable VarAC BBS before publishing the FIO catalog on this radio.")
            return
        if (self.radio_service_enabled_chk.isChecked() or self.radio_publish_enabled_chk.isChecked()) and not live_dir:
            self.radio_service_status.setText("Choose the selected radio's live BBS folder before enabling service or publication.")
            self.radio_live_dir_edit.setFocus(Qt.OtherFocusReason)
            return
        payload = dict(profile)
        payload.update(
            {
                "varac_bbs_dir": live_dir,
                "varac_bbs_enabled": bool(self.radio_service_enabled_chk.isChecked()),
                "varac_bbs_vault_enabled": bool(self.radio_publish_enabled_chk.isChecked()),
                "varac_bbs_announce_enabled": bool(self.radio_announce_enabled_chk.isChecked()),
            }
        )
        try:
            saved = self._radio_store().save_device_profile(payload)
        except Exception as exc:
            self.radio_service_status.setText(f"Radio BBS service was not saved: {exc}")
            return
        self._radio_profiles_by_id[profile_id] = dict(saved)
        radio_name = str(saved.get("name", "") or f"Radio {profile_id}")
        self._refresh_radio_services()
        self.radio_service_status.setText(f"Saved the BBS service for {radio_name}. No source files were copied or deleted.")

    def _open_radio_settings(self) -> None:
        host = self.window()
        opener = getattr(host, "open_settings_section", None)
        if callable(opener):
            opener(
                "radio_profiles",
                radio_id=self._selected_radio_service_id() or None,
                settings_nav_context="radios",
            )
            return
        self.radio_service_status.setText("Open Settings → Radios to configure the selected radio's native VarAC paths.")

    def _set_location_editor_visible(self, visible: bool) -> None:
        self.location_editor.setVisible(bool(visible))
        self.location_edit_btn.setText("Hide" if visible else "Edit")
        self.location_edit_btn.setAccessibleName("Hide location editor" if visible else "Show location editor")
        if not visible:
            return
        location = self._locations_by_id.get(self._selected_location_id)
        self._load_location_editor(location)

    def _begin_new_location(self) -> None:
        if not self.location_edit_btn.isChecked():
            self.location_edit_btn.setChecked(True)
        self._load_location_editor(None)
        self.location_name_edit.setFocus(Qt.OtherFocusReason)

    def _populate_parent_choices(self, current_id: str = "") -> None:
        selected_parent = self.location_parent_combo.currentData()
        self.location_parent_combo.blockSignals(True)
        try:
            self.location_parent_combo.clear()
            self.location_parent_combo.addItem("No parent", "")
            for location in sorted(self._locations_by_id.values(), key=lambda item: (item.name.lower(), item.location_id)):
                if location.location_id == current_id:
                    continue
                self.location_parent_combo.addItem(location.name, location.location_id)
            wanted = str(selected_parent or "")
            index = self.location_parent_combo.findData(wanted)
            self.location_parent_combo.setCurrentIndex(index if index >= 0 else 0)
        finally:
            self.location_parent_combo.blockSignals(False)

    def _load_location_editor(self, location: BbsLocationRecord | None) -> None:
        self._location_editor_loading = True
        try:
            self._editing_location_id = location.location_id if location is not None else ""
            self._populate_parent_choices(self._editing_location_id)
            self.location_name_edit.setText(location.name if location is not None else "")
            self.location_source_edit.setText(location.source_dir if location is not None else "")
            self.location_enabled_chk.setChecked(bool(location.enabled) if location is not None else True)
            self._set_combo_data(self.location_access_combo, location.access_rule if location is not None else "Public")
            metadata = dict(location.metadata or {}) if location is not None else {}
            allowed = metadata.get("allowed_callsigns", [])
            if isinstance(allowed, str):
                allowed_text = allowed
            else:
                allowed_text = ", ".join(str(value) for value in allowed if str(value or "").strip())
            self.location_callsigns_edit.setText(allowed_text)
            self.location_code_edit.clear()
            self.location_code_confirm_edit.clear()
            self._set_combo_data(self.location_retention_combo, location.retention_mode if location is not None else "global_default")
            if location is not None:
                self.location_retention_days_spin.setValue(max(1, int(location.retention_days or 0) or 14))
                parent_index = self.location_parent_combo.findData(location.parent_location_id)
                self.location_parent_combo.setCurrentIndex(parent_index if parent_index >= 0 else 0)
            else:
                self.location_retention_days_spin.setValue(14)
                self.location_parent_combo.setCurrentIndex(0)
            self.location_disable_btn.setEnabled(location is not None and bool(location.enabled))
            self.location_editor_status.setText(
                "Editing station-owned location. Saving records catalog policy only; it does not create or scan the source folder."
                if location is not None
                else "New station-owned location. Choose an existing source folder path if one is needed; it will not be created here."
            )
            self._update_location_retention_days_state()
            self._update_location_access_state()
        finally:
            self._location_editor_loading = False

    @staticmethod
    def _set_combo_data(combo: QComboBox, value: object) -> None:
        wanted = str(value or "").strip()
        index = combo.findData(wanted)
        if index < 0 and wanted:
            combo.addItem(_access_text(wanted), wanted)
            index = combo.count() - 1
        combo.setCurrentIndex(index if index >= 0 else 0)

    def _update_location_retention_days_state(self, *_args) -> None:
        expire = str(self.location_retention_combo.currentData() or "") == "expire_after_days"
        self.location_retention_days_spin.setEnabled(expire)
        self.location_retention_days_spin.setToolTip(
            "Number of days before this location's published mappings expire." if expire else "Used only when retention is Expire after days."
        )

    def _update_location_access_state(self, *_args) -> None:
        rule = str(self.location_access_combo.currentData() or "Public")
        callsigns = "Allowed callsigns" in rule
        code = "access code" in rule.lower()
        self.location_callsigns_label.setVisible(callsigns)
        self.location_callsigns_edit.setVisible(callsigns)
        self.location_code_label.setVisible(code)
        self.location_code_edit.setVisible(code)
        self.location_code_confirm_edit.setVisible(code)

    def _new_location_id(self, name: str) -> str:
        base = re.sub(r"[^a-z0-9]+", "-", str(name or "").strip().lower()).strip("-") or "location"
        candidate = base
        suffix = 2
        while candidate in self._locations_by_id:
            candidate = f"{base}-{suffix}"
            suffix += 1
        return candidate

    def _save_location(self) -> None:
        name = " ".join(self.location_name_edit.text().split())
        if not name:
            self.location_editor_status.setText("Location name is required before saving.")
            self.location_name_edit.setFocus(Qt.OtherFocusReason)
            return
        location_id = self._editing_location_id or self._new_location_id(name)
        existing = self._locations_by_id.get(location_id)
        parent_id = str(self.location_parent_combo.currentData() or "").strip()
        if parent_id == location_id:
            parent_id = ""
        retention_mode = str(self.location_retention_combo.currentData() or "global_default")
        retention_days = self.location_retention_days_spin.value() if retention_mode == "expire_after_days" else 0
        access_rule = str(self.location_access_combo.currentData() or "Public")
        metadata = dict(existing.metadata or {}) if existing is not None else {}
        metadata["open_rule"] = access_rule
        metadata.setdefault("visibility_rule", "Public")
        metadata.setdefault("list_in_root_menu", True)
        metadata["inherit_global_allowed_callsigns"] = False
        metadata["allowed_callsigns"] = [
            value.strip().upper()
            for value in re.split(r"[,;\s]+", self.location_callsigns_edit.text())
            if value.strip()
        ]
        code_text = self.location_code_edit.text()
        confirm_text = self.location_code_confirm_edit.text()
        needs_code = "access code" in access_rule.lower()
        has_saved_code = bool(metadata.get("access_code_hash") and metadata.get("access_code_salt"))
        if code_text or confirm_text:
            if code_text != confirm_text:
                self.location_editor_status.setText("The access code and confirmation do not match.")
                return
            metadata.update(hash_access_code(code_text))
            has_saved_code = True
        if needs_code and not has_saved_code:
            self.location_editor_status.setText("Enter and confirm an access code for this location.")
            return
        try:
            with connect_sqlite(bbs_library_db_path_from_settings(self.settings)) as conn:
                with conn:
                    upsert_bbs_location(
                        conn,
                        location_id=location_id,
                        name=name,
                        source_dir=self.location_source_edit.text().strip(),
                        enabled=bool(self.location_enabled_chk.isChecked()),
                        parent_location_id=parent_id,
                        access_rule=access_rule,
                        retention_mode=retention_mode,
                        retention_days=retention_days,
                        metadata=metadata,
                    )
        except Exception as exc:
            self.location_editor_status.setText(f"Location was not saved: {exc}")
            return
        self._selected_location_id = location_id
        if self.location_enabled_chk.isChecked():
            self._publishing_location_id = location_id
        self._editing_location_id = location_id
        self.refresh_catalog()
        state = "enabled" if self.location_enabled_chk.isChecked() else "disabled"
        self.location_editor_status.setText(f"Saved {name} ({state}). No source folders were created or scanned.")

    def _disable_location(self) -> None:
        location = self._locations_by_id.get(self._editing_location_id or self._selected_location_id)
        if location is None:
            self.location_editor_status.setText("Select a saved location before disabling it.")
            return
        try:
            with connect_sqlite(bbs_library_db_path_from_settings(self.settings)) as conn:
                with conn:
                    upsert_bbs_location(
                        conn,
                        location_id=location.location_id,
                        name=location.name,
                        source_dir=location.source_dir,
                        enabled=False,
                        parent_location_id=location.parent_location_id,
                        access_rule=location.access_rule,
                        retention_mode=location.retention_mode,
                        retention_days=location.retention_days,
                        metadata=location.metadata,
                    )
        except Exception as exc:
            self.location_editor_status.setText(f"Location was not disabled: {exc}")
            return
        self._selected_location_id = location.location_id
        self.refresh_catalog()
        self._load_location_editor(self._locations_by_id.get(location.location_id))
        self.location_editor_status.setText(
            f"Disabled {location.name}. It remains visible for review; no rows, source files, or folders were deleted."
        )

    def set_tab_active(self, active: bool) -> None:
        if active:
            self.refresh_catalog()

    def resizeEvent(self, event) -> None:  # noqa: N802 - Qt naming
        super().resizeEvent(event)
        compact = int(self.width() or 0) <= 1000
        if compact == self._compact_layout:
            return
        self._compact_layout = compact
        self.splitter.setOrientation(Qt.Vertical if compact else Qt.Horizontal)
        self.splitter.setSizes([160, 600] if compact else [320, 760])
        self.detail_toggle_btn.setVisible(compact)
        self._update_detail_visibility()

    def _update_detail_visibility(self, *_args) -> None:
        visible = not self._compact_layout or self.detail_toggle_btn.isChecked()
        self.detail_group.setVisible(visible)
        self.detail_toggle_btn.setText("Hide Details" if visible and self._compact_layout else "Details")
        self.detail_toggle_btn.setAccessibleName(
            "Hide artifact details" if visible and self._compact_layout else "Show artifact details"
        )

    def refresh_catalog(self) -> None:
        """Read at most 200 catalog rows; never touch folders or live BBS output."""

        self._refresh_radio_services()
        selected = self._selected_location_id
        try:
            with connect_sqlite(bbs_library_db_path_from_settings(self.settings)) as conn:
                locations = list_bbs_locations(conn, include_disabled=True)
                default_row = conn.execute(
                    "SELECT value FROM bbs_library_meta WHERE key='station_default_location_id' LIMIT 1"
                ).fetchone()
                permission_rows = dict(
                    conn.execute(
                        "SELECT key, value FROM bbs_library_meta WHERE key IN ('station_allowed_callsigns', 'station_limit_access_enabled')"
                    ).fetchall()
                )
        except Exception as exc:
            self._locations_by_id = {}
            self._populate_locations([])
            self._populate_artifacts([])
            self._populate_helpers([])
            self._populate_visitor_preview([])
            self.summary_label.setText(f"Managed BBS catalog is unavailable: {exc}")
            self.overview_status_label.setText(self.summary_label.text())
            return
        self._locations_by_id = {location.location_id: location for location in locations}
        self._station_allowed_callsigns = {
            value.strip().upper()
            for value in re.split(r"[,;\s]+", str(permission_rows.get("station_allowed_callsigns", "") or ""))
            if value.strip()
        }
        self._station_limit_access_enabled = str(
            permission_rows.get("station_limit_access_enabled", "0") or "0"
        ) == "1"
        if selected and selected not in self._locations_by_id:
            selected = ""
        if not selected and locations:
            configured_default = str(default_row[0] or "").strip() if default_row else ""
            selected = configured_default if configured_default in self._locations_by_id else next(
                (location.location_id for location in locations if location.enabled),
                locations[0].location_id,
            )
        self._selected_location_id = selected
        enabled_ids = {location.location_id for location in locations if location.enabled}
        if self._publishing_location_id not in enabled_ids:
            self._publishing_location_id = selected if selected in enabled_ids else next(
                (location.location_id for location in locations if location.enabled),
                "",
            )
        self._populate_locations(locations)
        self._load_artifacts()
        self._refresh_visitor_preview()

    def _refresh_visitor_preview(self, *_args) -> None:
        locations = [row for row in self._locations_by_id.values() if self._visitor_can_see_location(row)]
        self._populate_visitor_preview(locations)
        try:
            with connect_sqlite(bbs_library_db_path_from_settings(self.settings)) as conn:
                rows = list_bbs_admin_rows(conn, location_id="", limit=MAX_ARTIFACT_ROWS)
        except Exception as exc:
            self.visitor_preview_status.setText(f"Visitor preview is unavailable: {exc}")
            self.visitor_artifact_table.setRowCount(0)
            return
        visible_ids = {location.location_id for location in locations}
        visible_rows = [
            row for row in rows
            if row.published and row.location_id in visible_ids and not is_fio_bbs_helper_file_name(row.display_name or row.source_path)
        ][:MAX_ARTIFACT_ROWS]
        self.visitor_artifact_table.setRowCount(len(visible_rows))
        for index, row in enumerate(visible_rows):
            location = self._locations_by_id.get(row.location_id)
            values = (
                row.display_name or row.artifact_id,
                row.location_name or (location.name if location else "Unknown location"),
                _access_text(location.access_rule if location else "public"),
                _health_text(row),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                item.setToolTip("Read-only visitor preview")
                self.visitor_artifact_table.setItem(index, column, item)
        suffix = " (first 200)" if len(visible_rows) >= MAX_ARTIFACT_ROWS else ""
        self.visitor_preview_status.setText(
            f"{len(locations)} visible location{'s' if len(locations) != 1 else ''}; "
            f"{len(visible_rows)} published artifact{'s' if len(visible_rows) != 1 else ''}{suffix}. Read-only preview."
        )

    def _visitor_can_see_location(self, location: BbsLocationRecord) -> bool:
        if not location.enabled:
            return False
        metadata = dict(location.metadata or {})
        visibility = str(metadata.get("visibility_rule", "Public") or "Public").strip()
        if visibility == "Hidden":
            return False
        if visibility == "Public":
            return True
        callsign = self.visitor_callsign_edit.text().strip().upper()
        if not callsign:
            return False
        allowed_value = metadata.get("allowed_callsigns", [])
        if isinstance(allowed_value, str):
            allowed_text = allowed_value
        else:
            allowed_text = " ".join(str(value) for value in allowed_value)
        allowed = {
            value.strip().upper()
            for value in re.split(r"[,;\s]+", allowed_text)
            if value.strip()
        }
        if bool(metadata.get("inherit_global_allowed_callsigns", True)):
            allowed.update(self._station_allowed_callsigns)
        if self._station_limit_access_enabled and callsign not in self._station_allowed_callsigns:
            return False
        return not allowed or callsign in allowed

    def _populate_locations(self, locations: Iterable[BbsLocationRecord]) -> None:
        location_rows = list(locations)
        self.location_tree.blockSignals(True)
        try:
            self.location_tree.clear()
            root = QTreeWidgetItem(["Managed BBS Library"])
            root.setData(0, _LOCATION_ID_ROLE, "")
            root.setToolTip(0, "All catalog artifacts with an existing Managed BBS membership.")
            self.location_tree.addTopLevelItem(root)
            parents: dict[str, QTreeWidgetItem] = {"": root}
            unresolved = location_rows
            while unresolved:
                deferred: list[BbsLocationRecord] = []
                progressed = False
                for location in unresolved:
                    parent = parents.get(location.parent_location_id) or root
                    if location.parent_location_id and location.parent_location_id not in parents:
                        deferred.append(location)
                        continue
                    label = f"{location.name} — {_access_text(location.access_rule)}; {_retention_text(location.retention_mode, location.retention_days)}"
                    if not location.enabled:
                        label += "; Disabled"
                    item = QTreeWidgetItem([label])
                    item.setData(0, _LOCATION_ID_ROLE, location.location_id)
                    item.setToolTip(
                        0,
                        f"Location: {location.name}\nAccess: {_access_text(location.access_rule)}\n"
                        f"Retention: {_retention_text(location.retention_mode, location.retention_days)}\n"
                        f"Source: {location.source_dir or 'Not configured'}",
                    )
                    parent.addChild(item)
                    parents[location.location_id] = item
                    progressed = True
                if not deferred or not progressed:
                    for location in deferred:
                        parent = root
                        item = QTreeWidgetItem([f"{location.name} — {_access_text(location.access_rule)}; {_retention_text(location.retention_mode, location.retention_days)}"])
                        item.setData(0, _LOCATION_ID_ROLE, location.location_id)
                        parent.addChild(item)
                        parents[location.location_id] = item
                    break
                unresolved = deferred
            root.setExpanded(True)
            wanted = parents.get(self._selected_location_id, root)
            self.location_tree.setCurrentItem(wanted)
        finally:
            self.location_tree.blockSignals(False)
        self._sync_publishing_location_choices(location_rows)

    def _sync_publishing_location_choices(self, locations: Iterable[BbsLocationRecord]) -> None:
        rows = [location for location in locations if location.enabled]
        self.publishing_location_combo.blockSignals(True)
        try:
            self.publishing_location_combo.clear()
            for location in rows:
                self.publishing_location_combo.addItem(location.name, location.location_id)
            index = self.publishing_location_combo.findData(self._publishing_location_id)
            if index < 0 and self.publishing_location_combo.count() > 0:
                index = 0
                self._publishing_location_id = str(self.publishing_location_combo.itemData(0) or "")
            self.publishing_location_combo.setCurrentIndex(index)
        finally:
            self.publishing_location_combo.blockSignals(False)

    def _location_tree_item(self, location_id: str) -> QTreeWidgetItem | None:
        root = self.location_tree.topLevelItem(0)
        pending = [root] if root is not None else []
        while pending:
            item = pending.pop()
            if str(item.data(0, _LOCATION_ID_ROLE) or "") == location_id:
                return item
            pending.extend(item.child(index) for index in range(item.childCount()))
        return None

    def _on_publishing_location_changed(self, _index: int = -1) -> None:
        location_id = str(self.publishing_location_combo.currentData() or "")
        if not location_id or location_id == self._publishing_location_id:
            return
        self._publishing_location_id = location_id
        self._load_artifacts()

    def _populate_visitor_preview(self, locations: Iterable[BbsLocationRecord]) -> None:
        rows = list(locations)
        self.visitor_preview_tree.clear()
        root = QTreeWidgetItem(["Visitor BBS view"])
        root.setToolTip(0, "Enabled locations visible to this visitor. This preview is read-only.")
        self.visitor_preview_tree.addTopLevelItem(root)
        parents: dict[str, QTreeWidgetItem] = {"": root}
        unresolved = rows
        while unresolved:
            deferred: list[BbsLocationRecord] = []
            progressed = False
            for location in unresolved:
                parent = parents.get(location.parent_location_id) or root
                if location.parent_location_id and location.parent_location_id not in parents:
                    deferred.append(location)
                    continue
                item = QTreeWidgetItem(
                    [f"{location.name} — {_access_text(location.access_rule)}; {_retention_text(location.retention_mode, location.retention_days)}"]
                )
                item.setToolTip(0, "Read-only visitor-visible location.")
                parent.addChild(item)
                parents[location.location_id] = item
                progressed = True
            if not deferred or not progressed:
                for location in deferred:
                    root.addChild(QTreeWidgetItem([location.name]))
                break
            unresolved = deferred
        root.setExpanded(True)

    def _on_location_selection_changed(self) -> None:
        selected = self.location_tree.currentItem()
        self._selected_location_id = str(selected.data(0, _LOCATION_ID_ROLE) or "") if selected is not None else ""
        if self.location_editor.isVisible():
            self._load_location_editor(self._locations_by_id.get(self._selected_location_id))
        location = self._locations_by_id.get(self._selected_location_id)
        if location is None:
            self.location_context_label.setText(
                "Select a location to review its access and retention labels. Use Edit to reveal the progressive location editor."
            )
        else:
            status = "Disabled — it remains in the catalog and no source files were removed." if not location.enabled else "Enabled"
            self.location_context_label.setText(
                f"{location.name}: {status}. Access: {_access_text(location.access_rule)}. "
                f"Retention: {_retention_text(location.retention_mode, location.retention_days)}. "
                f"Source folder: {location.source_dir or 'Not configured'}."
            )
            if location.enabled:
                self._publishing_location_id = location.location_id
        publishing_index = self.publishing_location_combo.findData(self._publishing_location_id)
        self.publishing_location_combo.blockSignals(True)
        try:
            self.publishing_location_combo.setCurrentIndex(publishing_index)
        finally:
            self.publishing_location_combo.blockSignals(False)
        self._load_artifacts()

    def _load_artifacts(self) -> None:
        try:
            with connect_sqlite(bbs_library_db_path_from_settings(self.settings)) as conn:
                rows = list_bbs_admin_rows(
                    conn,
                    # The selected tree location scopes the membership checkbox,
                    # not the catalog read.  Keeping this catalog-wide lets an
                    # operator publish an existing artifact into a second
                    # location even before that second membership exists.
                    location_id="",
                    limit=MAX_ARTIFACT_ROWS,
                )
        except Exception as exc:
            self._populate_artifacts([])
            self._populate_helpers([])
            self.summary_label.setText(f"Managed BBS catalog could not be read: {exc}")
            self.overview_status_label.setText(self.summary_label.text())
            return
        helper_rows = [row for row in rows if is_fio_bbs_helper_file_name(row.display_name or row.source_path)]
        displays = self._display_artifacts(
            row for row in rows if not is_fio_bbs_helper_file_name(row.display_name or row.source_path)
        )
        self._populate_artifacts(displays)
        self._populate_helpers(helper_rows)
        location_name = self._locations_by_id.get(self._publishing_location_id)
        scope = location_name.name if location_name is not None else "all locations"
        self.artifact_heading.setText(f"Artifacts — publish in {scope}")
        clipped = " (first 200)" if len(rows) >= MAX_ARTIFACT_ROWS else ""
        guidance = (
            "Select a location to change Published."
            if not self._publishing_location_id
            else "Published changes only catalog membership; source files remain in place."
        )
        self.summary_label.setText(
            f"{len(self._locations_by_id)} location{'s' if len(self._locations_by_id) != 1 else ''}; "
            f"{len(displays)} artifact{'s' if len(displays) != 1 else ''}{clipped}. {guidance}"
        )
        self.overview_status_label.setText(self.summary_label.text())

    @staticmethod
    def _helper_display_name(row: BbsArtifactAdminRow) -> str:
        name = Path(row.display_name or row.source_path or row.artifact_id).name
        return name[:-4] if name.lower().endswith(".txt") else name

    @staticmethod
    def _helper_purpose(row: BbsArtifactAdminRow) -> str:
        name = Path(row.display_name or row.source_path or row.artifact_id).name.upper()
        if name.startswith("00 READ FIRST") or name.startswith("00 NOTICE"):
            return "Visitor start"
        if name.startswith("01 COMMANDS"):
            return "Command index"
        if "TYPE FLAMP" in name:
            return "FLAMP request command"
        if re.match(r"^\d{2} TYPE ", name):
            return "Location request command"
        if name.startswith("BBS_QUEUE_LIST"):
            return "Queue status"
        if name.startswith("BBS_BLOCK_LIST"):
            return "Block status"
        return "Visitor navigation"

    def _populate_helpers(self, rows: Iterable[BbsArtifactAdminRow]) -> None:
        grouped: dict[str, list[BbsArtifactAdminRow]] = defaultdict(list)
        for row in rows:
            grouped[row.artifact_id].append(row)
        helpers = list(grouped.values())[:MAX_ARTIFACT_ROWS]
        self.helpers_table.setRowCount(len(helpers))
        for index, helper_rows in enumerate(helpers):
            row = helper_rows[0]
            compatibility_name = Path(row.display_name or row.source_path or row.artifact_id).name
            locations = ", ".join(dict.fromkeys(item.location_name for item in helper_rows if item.location_name))
            values = (
                self._helper_display_name(row),
                self._helper_purpose(row),
                compatibility_name,
                locations or "System projection",
                f"{row.age_days}d",
                _health_text(row),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                item.setToolTip(
                    "System-generated helper; shown separately from operator files and not editable as catalog membership."
                )
                self.helpers_table.setItem(index, column, item)
        self.helpers_status.setText(
            f"{len(helpers)} system-generated helper{'s' if len(helpers) != 1 else ''} in the bounded catalog view."
            if helpers
            else "No generated helper files are present in the bounded catalog view. They remain excluded from Publishing."
        )

    def _display_artifacts(self, rows: Iterable[BbsArtifactAdminRow]) -> list[_ArtifactDisplay]:
        grouped: dict[str, list[BbsArtifactAdminRow]] = defaultdict(list)
        for row in rows:
            grouped[row.artifact_id].append(row)
        displays: list[_ArtifactDisplay] = []
        for artifact_id, artifact_rows in grouped.items():
            primary = artifact_rows[0]
            locations = tuple(item.location_name for item in artifact_rows)
            published_ids = tuple(item.location_id for item in artifact_rows if item.published)
            displays.append(_ArtifactDisplay(primary, tuple(artifact_rows), locations, published_ids))
        return displays[:MAX_ARTIFACT_ROWS]

    def _populate_artifacts(self, displays: Iterable[_ArtifactDisplay]) -> None:
        rendered = list(displays)
        self._loading = True
        self._display_rows = {display.row.artifact_id: display for display in rendered}
        self.artifact_table.blockSignals(True)
        try:
            self.artifact_table.clearContents()
            self.artifact_table.setRowCount(len(rendered))
            for index, display in enumerate(rendered):
                row = display.row
                published = QTableWidgetItem("")
                published.setData(_ARTIFACT_ID_ROLE, row.artifact_id)
                published.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
                selected_location = self._locations_by_id.get(self._publishing_location_id)
                can_change = bool(selected_location is not None and selected_location.enabled)
                if not can_change:
                    published.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                    published.setToolTip("Select one location before changing membership.")
                    published.setText("Choose location")
                else:
                    published.setToolTip("Check to publish this artifact in the selected managed location. Clearing it never deletes the source file.")
                    published.setCheckState(
                        Qt.Checked if self._publishing_location_id in display.published_location_ids else Qt.Unchecked
                    )
                self.artifact_table.setItem(index, 0, published)
                name = QTableWidgetItem(row.display_name or row.artifact_id)
                name.setToolTip(row.source_path)
                self.artifact_table.setItem(index, 1, name)
                locations = QTableWidgetItem(", ".join(display.location_names) or row.location_name)
                locations.setToolTip("Managed locations currently represented in this bounded view.")
                self.artifact_table.setItem(index, 2, locations)
                age = QTableWidgetItem(f"{row.age_days}d")
                age.setToolTip(_modified_detail(row.modified_utc))
                self.artifact_table.setItem(index, 3, age)
                retention = QTableWidgetItem(_retention_text(row.retention_mode, row.retention_days))
                self.artifact_table.setItem(index, 4, retention)
                health = QTableWidgetItem(_health_text(row))
                health.setToolTip(f"Source state: {row.source_state or 'unknown'}; publication state: {row.publication_state or 'unknown'}")
                self.artifact_table.setItem(index, 5, health)
        finally:
            self.artifact_table.blockSignals(False)
            self._loading = False
        if rendered:
            self.artifact_table.selectRow(0)
        else:
            self._clear_detail_panel()

    def _selected_display(self) -> _ArtifactDisplay | None:
        row_index = self.artifact_table.currentRow()
        item = self.artifact_table.item(row_index, 0) if row_index >= 0 else None
        artifact_id = str(item.data(_ARTIFACT_ID_ROLE) or "") if item is not None else ""
        return self._display_rows.get(artifact_id)

    def _clear_detail_panel(self) -> None:
        for label in self.detail_labels.values():
            label.setText("Select an artifact.")
            label.setToolTip("")

    def _refresh_detail_panel(self) -> None:
        display = self._selected_display()
        if display is None:
            self._clear_detail_panel()
            return
        row = display.row
        location = self._locations_by_id.get(self._publishing_location_id or row.location_id)
        selected_location_id = self._publishing_location_id
        selected_row = next(
            (candidate for candidate in display.rows if candidate.location_id == selected_location_id),
            None,
        )
        if selected_location_id and selected_row is None:
            health_text = "Not published in selected location"
        else:
            health_text = _health_text(selected_row or row)
        retention_row = selected_row or row
        values = {
            "origin": (row.source_kind or "Managed BBS catalog").replace("_", " ").title(),
            "path": row.source_path or "No source path recorded",
            "size": _byte_text(row.size),
            "age": f"{row.age_days}d; {_modified_detail(row.modified_utc)}",
            "access": _access_text(location.access_rule if location is not None else "public"),
            "retention": _retention_text(retention_row.retention_mode, retention_row.retention_days)
            + (f"; expires {retention_row.expires_utc}" if retention_row.expires_utc else "; no expiry recorded"),
            "health": health_text,
        }
        for key, value in values.items():
            self.detail_labels[key].setText(value)
            self.detail_labels[key].setToolTip(value)

    def _on_publication_item_changed(self, item: QTableWidgetItem) -> None:
        if self._loading or item.column() != 0 or not self._publishing_location_id:
            return
        artifact_id = str(item.data(_ARTIFACT_ID_ROLE) or "").strip()
        if not artifact_id:
            return
        try:
            with connect_sqlite(bbs_library_db_path_from_settings(self.settings)) as conn:
                with conn:
                    current = set(list_bbs_artifact_location_ids(conn, artifact_id))
                    if item.checkState() == Qt.Checked:
                        current.add(self._publishing_location_id)
                    else:
                        current.discard(self._publishing_location_id)
                    set_bbs_artifact_locations(conn, artifact_id=artifact_id, location_ids=current)
        except Exception as exc:
            self.summary_label.setText(f"Managed BBS membership was not changed: {exc}")
        self._load_artifacts()
