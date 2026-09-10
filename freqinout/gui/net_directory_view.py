"""Bounded Net Directory workspace for Tools & Resources."""

from __future__ import annotations

from dataclasses import replace
import uuid

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.resource_catalog_models import CatalogValidationError, NetDirectoryEntry, NetDirectorySession, ReadOnlyResourceError, ReferencedResourceError
from freqinout.core.resource_catalog_store import (
    MAX_RESULTS,
    STATION_MANUAL_SOURCE_KEY,
    ResourceCatalogStore,
)
from freqinout.gui.resource_picker import (
    frequency_where_text,
    populate_frequency_combo,
    populate_source_combo,
    session_when_text,
    source_display_label,
    source_display_labels,
)


def new_net_entry_key() -> str:
    return f"net_{uuid.uuid4()}"


def new_net_session_key() -> str:
    return f"session_{uuid.uuid4()}"


class NetDirectoryView(QWidget):
    """Directory identities and published sessions; never a station scheduler."""

    add_to_hf_nets_requested = Signal(object)
    open_hf_schedule_requested = Signal(object)

    def __init__(self, store: ResourceCatalogStore, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.store = store
        self._selected_entry: NetDirectoryEntry | None = None
        self._selected_session: NetDirectorySession | None = None
        self._editing_entry_key: str | None = None
        self._editing_session_key: str | None = None
        self._build_ui()
        self.refresh_results()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        title = QLabel("Net Directory")
        title.setStyleSheet("font-weight: 700; font-size: 17px;")
        layout.addWidget(title)
        why = QLabel("Directory records describe reusable nets and published net meetings. They do not activate an HF or Local schedule.")
        why.setWordWrap(True)
        layout.addWidget(why)
        filters = QHBoxLayout()
        self.search_edit = QLineEdit(self)
        self.search_edit.setPlaceholderText("Search net name, purpose, source region, or public contact")
        self.search_edit.setAccessibleName("Net directory search")
        self.search_edit.returnPressed.connect(self.refresh_results)
        filters.addWidget(self.search_edit, 1)
        self.status_filter = QComboBox(self)
        self.status_filter.addItem("Listing: Listed", True)
        self.status_filter.addItem("Retired", False)
        self.status_filter.addItem("All listings", None)
        self.status_filter.currentIndexChanged.connect(self.refresh_results)
        filters.addWidget(self.status_filter)
        refresh = QPushButton("Refresh", self)
        refresh.clicked.connect(self.refresh_results)
        filters.addWidget(refresh)
        layout.addLayout(filters)
        split = QSplitter(Qt.Horizontal, self)
        split.setChildrenCollapsible(False)
        layout.addWidget(split, 1)
        self.entry_table = QTableWidget(0, 4, split)
        self.entry_table.setHorizontalHeaderLabels(["Net", "Region", "Catalog source", "Listing"])
        self.entry_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.entry_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.entry_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.entry_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.entry_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.entry_table.itemSelectionChanged.connect(self._entry_selection_changed)
        right = QWidget(split)
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(8, 0, 0, 0)
        self.detail_label = QLabel("Select a directory entry to inspect published net meetings and station use.")
        self.detail_label.setWordWrap(True)
        self.detail_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        right_layout.addWidget(self.detail_label)
        self.entry_usage_label = QLabel("Used by: —")
        self.entry_usage_label.setWordWrap(True)
        right_layout.addWidget(self.entry_usage_label)
        entry_actions = QGridLayout()
        entry_actions.setHorizontalSpacing(6)
        entry_actions.setVerticalSpacing(6)
        self.new_entry_btn = QPushButton("New Net", right)
        self.edit_entry_btn = QPushButton("Edit", right)
        self.clone_entry_btn = QPushButton("Clone", right)
        self.retire_entry_btn = QPushButton("Retire", right)
        self.delete_entry_btn = QPushButton("Delete", right)
        for index, button in enumerate((self.new_entry_btn, self.edit_entry_btn, self.clone_entry_btn, self.retire_entry_btn, self.delete_entry_btn)):
            entry_actions.addWidget(button, index // 3, index % 3)
        for column in range(3):
            entry_actions.setColumnStretch(column, 1)
        right_layout.addLayout(entry_actions)
        self.session_table = QTableWidget(0, 4, right)
        self.session_table.setHorizontalHeaderLabels(["When", "Service", "Frequency", "Listing"])
        self.session_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.session_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.session_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.session_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.session_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.session_table.itemSelectionChanged.connect(self._session_selection_changed)
        right_layout.addWidget(self.session_table, 1)
        session_actions = QGridLayout()
        session_actions.setHorizontalSpacing(6)
        session_actions.setVerticalSpacing(6)
        self.new_session_btn = QPushButton("Add Meeting", right)
        self.edit_session_btn = QPushButton("Edit Meeting", right)
        self.clone_session_btn = QPushButton("Clone", right)
        self.retire_session_btn = QPushButton("Retire", right)
        self.delete_session_btn = QPushButton("Delete", right)
        self.add_hf_net_btn = QPushButton("Add to HF Nets", right)
        for button, action in (
            (self.new_session_btn, "Add a published net meeting"),
            (self.edit_session_btn, "Edit the selected net meeting"),
            (self.clone_session_btn, "Clone the selected net meeting"),
            (self.retire_session_btn, "Retire the selected net meeting"),
            (self.delete_session_btn, "Delete the selected net meeting"),
            (self.add_hf_net_btn, "Add selected net meetings to HF Nets"),
        ):
            button.setAccessibleName(action)
            button.setToolTip(action)
        for index, button in enumerate((self.new_session_btn, self.edit_session_btn, self.clone_session_btn, self.retire_session_btn, self.delete_session_btn, self.add_hf_net_btn)):
            session_actions.addWidget(button, index // 3, index % 3)
        for column in range(3):
            session_actions.setColumnStretch(column, 1)
        right_layout.addLayout(session_actions)
        self.entry_editor = self._build_entry_editor(right)
        self.session_editor = self._build_session_editor(right)
        right_layout.addWidget(self.entry_editor)
        right_layout.addWidget(self.session_editor)
        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        right_layout.addWidget(self.status_label)
        self.new_entry_btn.clicked.connect(self.begin_new_entry)
        self.edit_entry_btn.clicked.connect(self.begin_edit_entry)
        self.clone_entry_btn.clicked.connect(self.begin_clone_entry)
        self.retire_entry_btn.clicked.connect(self.retire_entry)
        self.delete_entry_btn.clicked.connect(self.delete_entry)
        self.new_session_btn.clicked.connect(self.begin_new_session)
        self.edit_session_btn.clicked.connect(self.begin_edit_session)
        self.clone_session_btn.clicked.connect(self.begin_clone_session)
        self.retire_session_btn.clicked.connect(self.retire_session)
        self.delete_session_btn.clicked.connect(self.delete_session)
        self.add_hf_net_btn.clicked.connect(self.request_add_to_hf_nets)
        self.entry_editor.setVisible(False)
        self.session_editor.setVisible(False)
        self._set_action_state()

    def _build_entry_editor(self, parent: QWidget) -> QFrame:
        frame = QFrame(parent)
        frame.setFrameShape(QFrame.StyledPanel)
        form = QFormLayout(frame)
        self.entry_key_edit = QLineEdit(frame)
        self.entry_source_edit = QComboBox(frame)
        self.entry_source_edit.setAccessibleName("Catalog source")
        self.entry_name_edit = QLineEdit(frame)
        self.entry_scope_edit = QLineEdit(frame)
        self.entry_description_edit = QLineEdit(frame)
        self.entry_key_edit.setVisible(False)
        for label, widget in (("Catalog source", self.entry_source_edit), ("Name", self.entry_name_edit), ("Source region", self.entry_scope_edit), ("Purpose", self.entry_description_edit)):
            form.addRow(label, widget)
        actions = QHBoxLayout()
        save = QPushButton("Save Net", frame)
        cancel = QPushButton("Cancel", frame)
        save.clicked.connect(self.save_entry)
        cancel.clicked.connect(lambda: frame.setVisible(False))
        actions.addWidget(save); actions.addWidget(cancel)
        form.addRow(actions)
        return frame

    def _build_session_editor(self, parent: QWidget) -> QFrame:
        frame = QFrame(parent)
        frame.setFrameShape(QFrame.StyledPanel)
        form = QFormLayout(frame)
        self.session_key_edit = QLineEdit(frame)
        self.session_source_edit = QComboBox(frame)
        self.session_source_edit.setAccessibleName("Catalog source")
        # Keep the canonical key as hidden internal state; the operator chooses
        # the same resource through its readable label and decimal-MHz value.
        self.session_frequency_edit = QLineEdit(frame)
        self.session_frequency_edit.setVisible(False)
        self.session_frequency_combo = QComboBox(frame)
        self.session_frequency_combo.setAccessibleName("Frequency")
        self.session_service_edit = QComboBox(frame)
        self.session_service_edit.addItems(["AMATEUR", "GMRS"])
        self.session_recurrence_edit = QLineEdit(frame)
        self.session_day_edit = QLineEdit(frame)
        self.session_start_edit = QLineEdit(frame)
        self.session_timezone_edit = QLineEdit(frame)
        self.session_duration_edit = QLineEdit(frame)
        self.session_day_edit.setPlaceholderText("Monday, Tuesday, or ALL")
        self.session_key_edit.setVisible(False)
        for label, widget in (("Catalog source", self.session_source_edit), ("Frequency", self.session_frequency_combo), ("Service", self.session_service_edit), ("Recurrence", self.session_recurrence_edit), ("Day (UTC)", self.session_day_edit), ("Local start", self.session_start_edit), ("Timezone", self.session_timezone_edit), ("Duration minutes", self.session_duration_edit)):
            form.addRow(label, widget)
        actions = QHBoxLayout()
        save = QPushButton("Save Net Meeting", frame)
        cancel = QPushButton("Cancel", frame)
        save.clicked.connect(self.save_session)
        cancel.clicked.connect(lambda: frame.setVisible(False))
        actions.addWidget(save); actions.addWidget(cancel)
        form.addRow(actions)
        return frame

    def refresh_results(self) -> None:
        rows = self.store.list_net_entries(search=self.search_edit.text(), active=self.status_filter.currentData(), limit=MAX_RESULTS)
        self.entry_table.setRowCount(len(rows))
        source_labels = source_display_labels(self.store, (row.source_key for row in rows))
        for row_index, entry in enumerate(rows):
            values = (
                entry.name,
                entry.scope or "—",
                source_labels[entry.source_key],
                self._listing_text(entry.retired, entry.active),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.UserRole, entry)
                self.entry_table.setItem(row_index, column, item)
        self.status_label.setText(f"Showing {len(rows)} bounded directory result{'s' if len(rows) != 1 else ''} (maximum {MAX_RESULTS}).")
        if rows:
            self.entry_table.selectRow(0)
        else:
            self._selected_entry = None
            self.session_table.setRowCount(0)
            self._set_action_state()

    def _entry_selection_changed(self) -> None:
        items = self.entry_table.selectedItems()
        if not items:
            return
        entry = self.entry_table.item(items[0].row(), 0).data(Qt.UserRole)
        if not isinstance(entry, NetDirectoryEntry):
            return
        self._selected_entry = entry
        usage = self.store.net_entry_usage(entry.net_entry_key)
        self.detail_label.setText("\n".join((
            f"{entry.name} · {self._listing_text(entry.retired, entry.active)}",
            f"Catalog source: {source_display_label(self.store, entry.source_key)}",
            f"Source region: {entry.scope or '—'}",
            f"Purpose: {entry.description or '—'}",
        )))
        self.entry_usage_label.setText(f"Used by: {usage.total_references}")
        self._refresh_sessions()
        self._set_action_state()

    def _refresh_sessions(self) -> None:
        if not self._selected_entry:
            self.session_table.setRowCount(0)
            return
        rows = self.store.list_sessions(net_entry_key=self._selected_entry.net_entry_key, active=None, limit=MAX_RESULTS)
        self.session_table.setRowCount(len(rows))
        frequencies = self.store.frequencies_by_keys(
            session.frequency_resource_key
            for session in rows
            if session.frequency_resource_key
        )
        for row_index, session in enumerate(rows):
            frequency = frequencies.get(session.frequency_resource_key or "")
            frequency_text = (
                f"{frequency.label} · {frequency_where_text(frequency)}"
                if frequency is not None
                else "Frequency unavailable"
            )
            values = (
                session_when_text(session),
                session.service,
                frequency_text,
                self._listing_text(session.retired, session.active),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                if column == 0:
                    item.setData(Qt.UserRole, session)
                self.session_table.setItem(row_index, column, item)
        self._selected_session = None

    def _session_selection_changed(self) -> None:
        items = self.session_table.selectedItems()
        if not items:
            return
        session = self.session_table.item(items[0].row(), 0).data(Qt.UserRole)
        if isinstance(session, NetDirectorySession):
            self._selected_session = session
            usage = self.store.session_usage(session.net_session_key)
            if usage.is_referenced:
                self.status_label.setText(f"Scheduled: this net meeting is used by {usage.total_references} HF schedule record(s). Open Schedule to review it.")
            else:
                self.status_label.setText("Not scheduled by this station.")
            self._set_action_state()

    def _selected_session_keys(self) -> tuple[str, ...]:
        keys = []
        for item in self.session_table.selectedItems():
            if item.column() != 0:
                continue
            session = item.data(Qt.UserRole)
            if isinstance(session, NetDirectorySession):
                keys.append(session.net_session_key)
        return tuple(dict.fromkeys(keys))

    def request_add_to_hf_nets(self) -> None:
        """Emit canonical session keys; HF Nets owns the destination and save."""
        keys = self._selected_session_keys()
        if not keys:
            self.status_label.setText("Select one or more published net meetings before adding to HF Nets.")
            return
        if len(keys) == 1 and self.store.session_usage(keys[0]).is_referenced:
            self.open_hf_schedule_requested.emit(keys[0])
            return
        self.add_to_hf_nets_requested.emit(keys)

    def _set_action_state(self) -> None:
        entry = self._selected_entry is not None
        session = self._selected_session is not None
        for button in (self.edit_entry_btn, self.clone_entry_btn, self.retire_entry_btn, self.delete_entry_btn, self.new_session_btn):
            button.setEnabled(entry)
        for button in (self.edit_session_btn, self.clone_session_btn, self.retire_session_btn, self.delete_session_btn):
            button.setEnabled(session)
        keys = self._selected_session_keys()
        scheduled = len(keys) == 1 and self.store.session_usage(keys[0]).is_referenced
        self.add_hf_net_btn.setText("Open Schedule" if scheduled else "Add to HF Nets")
        self.add_hf_net_btn.setEnabled(bool(keys))

    def begin_new_entry(self) -> None:
        self._editing_entry_key = None
        self.entry_key_edit.setReadOnly(False)
        for field in (self.entry_name_edit, self.entry_scope_edit, self.entry_description_edit): field.clear()
        populate_source_combo(self.entry_source_edit, self.store)
        self.entry_key_edit.setText(new_net_entry_key())
        self.entry_editor.setVisible(True)

    def begin_edit_entry(self) -> None:
        if not self._selected_entry: return
        entry = self._selected_entry; self._editing_entry_key = entry.net_entry_key
        self.entry_key_edit.setText(entry.net_entry_key); self.entry_key_edit.setReadOnly(True)
        populate_source_combo(self.entry_source_edit, self.store, entry.source_key); self.entry_name_edit.setText(entry.name)
        self.entry_scope_edit.setText(entry.scope or ""); self.entry_description_edit.setText(entry.description or "")
        self.entry_editor.setVisible(True)

    def begin_clone_entry(self) -> None:
        if not self._selected_entry: return
        self._editing_entry_key = None
        self.entry_key_edit.setText(new_net_entry_key()); self.entry_key_edit.setReadOnly(False)
        populate_source_combo(self.entry_source_edit, self.store); self.entry_name_edit.setText(f"{self._selected_entry.name} copy")
        self.entry_scope_edit.setText(self._selected_entry.scope or ""); self.entry_description_edit.setText(self._selected_entry.description or "")
        self.entry_editor.setVisible(True)

    def save_entry(self) -> None:
        prior = self._selected_entry if self._editing_entry_key else None
        try:
            if self.entry_source_edit.currentData() == STATION_MANUAL_SOURCE_KEY:
                self.store.ensure_station_source()
            entry = NetDirectoryEntry(self.entry_key_edit.text(), str(self.entry_source_edit.currentData() or ""), self.entry_name_edit.text(), description=self.entry_description_edit.text().strip() or None, scope=self.entry_scope_edit.text().strip() or None, active=prior.active if prior else True, retired=prior.retired if prior else False, replacement_net_entry_key=prior.replacement_net_entry_key if prior else None)
            self.store.update_net_entry(entry) if self._editing_entry_key else self.store.create_net_entry(entry)
        except (CatalogValidationError, ReadOnlyResourceError, ValueError) as exc:
            self.status_label.setText(f"Cannot save directory entry: {exc}"); return
        self.entry_editor.setVisible(False); self.status_label.setText("Directory entry saved."); self.refresh_results()

    def retire_entry(self) -> None:
        if not self._selected_entry: return
        try: self.store.retire_net_entry(self._selected_entry.net_entry_key)
        except (CatalogValidationError, ReadOnlyResourceError) as exc: self.status_label.setText(f"Cannot retire directory entry: {exc}"); return
        self.status_label.setText("Directory entry retired; historical net-meeting references remain."); self.refresh_results()

    def delete_entry(self) -> None:
        if not self._selected_entry: return
        usage = self.store.net_entry_usage(self._selected_entry.net_entry_key)
        if usage.is_referenced: self.status_label.setText(f"Cannot delete: used by {usage.total_references} record(s). Retire it instead."); return
        if QMessageBox.question(self, "Delete directory entry", f"Delete '{self._selected_entry.name}'? This cannot be undone.") != QMessageBox.Yes: return
        try: self.store.delete_net_entry_if_unreferenced(self._selected_entry.net_entry_key)
        except (ReferencedResourceError, ReadOnlyResourceError) as exc: self.status_label.setText(f"Cannot delete directory entry: {exc}"); return
        self._selected_entry = None; self.status_label.setText("Directory entry deleted."); self.refresh_results()

    def begin_new_session(self) -> None:
        if not self._selected_entry: return
        self._editing_session_key = None; self.session_key_edit.setReadOnly(False); self.session_key_edit.setText(new_net_session_key())
        for field in (self.session_recurrence_edit, self.session_day_edit, self.session_start_edit, self.session_timezone_edit, self.session_duration_edit): field.clear()
        populate_source_combo(self.session_source_edit, self.store); self.session_service_edit.setCurrentText("AMATEUR"); self.session_editor.setVisible(True)
        self.session_frequency_edit.clear()
        populate_frequency_combo(self.session_frequency_combo, self.store)

    def begin_edit_session(self) -> None:
        if not self._selected_session: return
        session = self._selected_session; self._editing_session_key = session.net_session_key
        self.session_key_edit.setText(session.net_session_key); self.session_key_edit.setReadOnly(True); populate_source_combo(self.session_source_edit, self.store, session.source_key)
        self.session_frequency_edit.setText(session.frequency_resource_key or "")
        populate_frequency_combo(self.session_frequency_combo, self.store, session.frequency_resource_key); self.session_service_edit.setCurrentText(session.service); self.session_recurrence_edit.setText(session.recurrence or "")
        self.session_day_edit.setText(getattr(session, "day_utc", None) or "")
        self.session_start_edit.setText(session.local_start_time or ""); self.session_timezone_edit.setText(session.timezone or ""); self.session_duration_edit.setText(str(session.duration_minutes or "")); self.session_editor.setVisible(True)

    def begin_clone_session(self) -> None:
        if not self._selected_session: return
        self.begin_edit_session(); self._editing_session_key = None; self.session_key_edit.setReadOnly(False); self.session_key_edit.setText(new_net_session_key())
        populate_source_combo(self.session_source_edit, self.store)

    def save_session(self) -> None:
        if not self._selected_entry: return
        prior = self._selected_session if self._editing_session_key else None
        try:
            duration = int(self.session_duration_edit.text()) if self.session_duration_edit.text().strip() else None
            if self.session_source_edit.currentData() == STATION_MANUAL_SOURCE_KEY:
                self.store.ensure_station_source()
            frequency_key = str(self.session_frequency_combo.currentData() or "") or None
            self.session_frequency_edit.setText(frequency_key or "")
            session = NetDirectorySession(self.session_key_edit.text(), self._selected_entry.net_entry_key, str(self.session_source_edit.currentData() or ""), self.session_service_edit.currentText(), frequency_key, recurrence=self.session_recurrence_edit.text().strip() or None, day_utc=self.session_day_edit.text().strip() or None, local_start_time=self.session_start_edit.text().strip() or None, timezone=self.session_timezone_edit.text().strip() or None, duration_minutes=duration, active=prior.active if prior else True, retired=prior.retired if prior else False, replacement_net_session_key=prior.replacement_net_session_key if prior else None)
            self.store.update_session(session) if self._editing_session_key else self.store.create_session(session)
        except (CatalogValidationError, ReadOnlyResourceError, ValueError) as exc:
            self.status_label.setText(f"Cannot save net meeting: {exc}"); return
        self.session_editor.setVisible(False); self.status_label.setText("Published net meeting saved."); self._refresh_sessions()

    def retire_session(self) -> None:
        if not self._selected_session: return
        try: self.store.retire_session(self._selected_session.net_session_key)
        except (CatalogValidationError, ReadOnlyResourceError) as exc: self.status_label.setText(f"Cannot retire net meeting: {exc}"); return
        self.status_label.setText("Published net meeting retired; existing schedules remain reviewable."); self._refresh_sessions()

    def delete_session(self) -> None:
        if not self._selected_session: return
        usage = self.store.session_usage(self._selected_session.net_session_key)
        if usage.is_referenced: self.status_label.setText(f"Cannot delete: used by {usage.total_references} record(s). Retire it instead."); return
        if QMessageBox.question(self, "Delete published net meeting", "Delete this unreferenced net meeting? This cannot be undone.") != QMessageBox.Yes: return
        try: self.store.delete_session_if_unreferenced(self._selected_session.net_session_key)
        except (ReferencedResourceError, ReadOnlyResourceError) as exc: self.status_label.setText(f"Cannot delete net meeting: {exc}"); return
        self._selected_session = None; self.status_label.setText("Published net meeting deleted."); self._refresh_sessions()

    @staticmethod
    def _listing_text(retired: bool, active: bool) -> str:
        if retired:
            return "Retired"
        return "Listed" if active else "Unlisted"


__all__ = ["NetDirectoryView", "new_net_entry_key", "new_net_session_key"]
