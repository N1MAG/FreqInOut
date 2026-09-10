"""Bounded Net Directory workspace for Tools & Resources."""

from __future__ import annotations

from dataclasses import replace
import uuid

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QFrame,
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
from freqinout.core.resource_catalog_store import MAX_RESULTS, ResourceCatalogStore
from freqinout.gui.resource_picker import session_when_text


def new_net_entry_key() -> str:
    return f"net_{uuid.uuid4()}"


def new_net_session_key() -> str:
    return f"session_{uuid.uuid4()}"


class NetDirectoryView(QWidget):
    """Directory identities and published sessions; never a station scheduler."""

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
        why = QLabel("Directory records describe reusable net identity and published sessions. They do not activate an HF or Local schedule.")
        why.setWordWrap(True)
        layout.addWidget(why)
        filters = QHBoxLayout()
        self.search_edit = QLineEdit(self)
        self.search_edit.setPlaceholderText("Search net name, purpose, scope, or public contact")
        self.search_edit.setAccessibleName("Net directory search")
        self.search_edit.returnPressed.connect(self.refresh_results)
        filters.addWidget(self.search_edit, 1)
        self.status_filter = QComboBox(self)
        self.status_filter.addItem("Status: Active", True)
        self.status_filter.addItem("Retired", False)
        self.status_filter.addItem("Any status", None)
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
        self.entry_table.setHorizontalHeaderLabels(["Net", "Scope", "Source", "Status"])
        self.entry_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.entry_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.entry_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.entry_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.entry_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.entry_table.itemSelectionChanged.connect(self._entry_selection_changed)
        right = QWidget(split)
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(8, 0, 0, 0)
        self.detail_label = QLabel("Select a directory entry to inspect published sessions and station use.")
        self.detail_label.setWordWrap(True)
        self.detail_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        right_layout.addWidget(self.detail_label)
        self.entry_usage_label = QLabel("Used by: —")
        self.entry_usage_label.setWordWrap(True)
        right_layout.addWidget(self.entry_usage_label)
        entry_actions = QHBoxLayout()
        self.new_entry_btn = QPushButton("New Net", right)
        self.edit_entry_btn = QPushButton("Edit", right)
        self.clone_entry_btn = QPushButton("Clone", right)
        self.retire_entry_btn = QPushButton("Retire", right)
        self.delete_entry_btn = QPushButton("Delete", right)
        for button in (self.new_entry_btn, self.edit_entry_btn, self.clone_entry_btn, self.retire_entry_btn, self.delete_entry_btn):
            entry_actions.addWidget(button)
        right_layout.addLayout(entry_actions)
        self.session_table = QTableWidget(0, 4, right)
        self.session_table.setHorizontalHeaderLabels(["When", "Service", "Frequency key", "Status"])
        self.session_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.session_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.session_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.session_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.session_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.session_table.itemSelectionChanged.connect(self._session_selection_changed)
        right_layout.addWidget(self.session_table, 1)
        session_actions = QHBoxLayout()
        self.new_session_btn = QPushButton("Add Session", right)
        self.edit_session_btn = QPushButton("Edit Session", right)
        self.clone_session_btn = QPushButton("Clone Session", right)
        self.retire_session_btn = QPushButton("Retire Session", right)
        self.delete_session_btn = QPushButton("Delete Session", right)
        for button in (self.new_session_btn, self.edit_session_btn, self.clone_session_btn, self.retire_session_btn, self.delete_session_btn):
            session_actions.addWidget(button)
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
        self.entry_editor.setVisible(False)
        self.session_editor.setVisible(False)
        self._set_action_state()

    def _build_entry_editor(self, parent: QWidget) -> QFrame:
        frame = QFrame(parent)
        frame.setFrameShape(QFrame.StyledPanel)
        form = QFormLayout(frame)
        self.entry_key_edit = QLineEdit(frame)
        self.entry_source_edit = QLineEdit(frame)
        self.entry_name_edit = QLineEdit(frame)
        self.entry_scope_edit = QLineEdit(frame)
        self.entry_description_edit = QLineEdit(frame)
        for label, widget in (("Net key", self.entry_key_edit), ("Source key", self.entry_source_edit), ("Name", self.entry_name_edit), ("Scope", self.entry_scope_edit), ("Purpose", self.entry_description_edit)):
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
        self.session_source_edit = QLineEdit(frame)
        self.session_frequency_edit = QLineEdit(frame)
        self.session_service_edit = QComboBox(frame)
        self.session_service_edit.addItems(["AMATEUR", "GMRS"])
        self.session_recurrence_edit = QLineEdit(frame)
        self.session_start_edit = QLineEdit(frame)
        self.session_timezone_edit = QLineEdit(frame)
        self.session_duration_edit = QLineEdit(frame)
        for label, widget in (("Session key", self.session_key_edit), ("Source key", self.session_source_edit), ("Frequency key", self.session_frequency_edit), ("Service", self.session_service_edit), ("Recurrence", self.session_recurrence_edit), ("Local start", self.session_start_edit), ("Timezone", self.session_timezone_edit), ("Duration minutes", self.session_duration_edit)):
            form.addRow(label, widget)
        actions = QHBoxLayout()
        save = QPushButton("Save Session", frame)
        cancel = QPushButton("Cancel", frame)
        save.clicked.connect(self.save_session)
        cancel.clicked.connect(lambda: frame.setVisible(False))
        actions.addWidget(save); actions.addWidget(cancel)
        form.addRow(actions)
        return frame

    def refresh_results(self) -> None:
        rows = self.store.list_net_entries(search=self.search_edit.text(), active=self.status_filter.currentData(), limit=MAX_RESULTS)
        self.entry_table.setRowCount(len(rows))
        for row_index, entry in enumerate(rows):
            values = (entry.name, entry.scope or "—", entry.source_key, "Retired" if entry.retired else "Active")
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
        self.detail_label.setText("\n".join((f"{entry.name} · {'Retired' if entry.retired else 'Active'}", f"Source: {entry.source_key}", f"Scope: {entry.scope or '—'}", f"Purpose: {entry.description or '—'}", f"Version: {entry.version_hash or '—'}")))
        self.entry_usage_label.setText(f"Used by: {usage.total_references}")
        self._refresh_sessions()
        self._set_action_state()

    def _refresh_sessions(self) -> None:
        if not self._selected_entry:
            self.session_table.setRowCount(0)
            return
        rows = self.store.list_sessions(net_entry_key=self._selected_entry.net_entry_key, active=None, limit=MAX_RESULTS)
        self.session_table.setRowCount(len(rows))
        for row_index, session in enumerate(rows):
            values = (session_when_text(session), session.service, session.frequency_resource_key or "—", "Retired" if session.retired else "Active")
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
            self.status_label.setText(f"Session used by {usage.total_references} record(s).")
            self._set_action_state()

    def _set_action_state(self) -> None:
        entry = self._selected_entry is not None
        session = self._selected_session is not None
        for button in (self.edit_entry_btn, self.clone_entry_btn, self.retire_entry_btn, self.delete_entry_btn, self.new_session_btn):
            button.setEnabled(entry)
        for button in (self.edit_session_btn, self.clone_session_btn, self.retire_session_btn, self.delete_session_btn):
            button.setEnabled(session)

    def begin_new_entry(self) -> None:
        self._editing_entry_key = None
        self.entry_key_edit.setReadOnly(False)
        for field in (self.entry_source_edit, self.entry_name_edit, self.entry_scope_edit, self.entry_description_edit): field.clear()
        self.entry_key_edit.setText(new_net_entry_key())
        self.entry_editor.setVisible(True)

    def begin_edit_entry(self) -> None:
        if not self._selected_entry: return
        entry = self._selected_entry; self._editing_entry_key = entry.net_entry_key
        self.entry_key_edit.setText(entry.net_entry_key); self.entry_key_edit.setReadOnly(True)
        self.entry_source_edit.setText(entry.source_key); self.entry_name_edit.setText(entry.name)
        self.entry_scope_edit.setText(entry.scope or ""); self.entry_description_edit.setText(entry.description or "")
        self.entry_editor.setVisible(True)

    def begin_clone_entry(self) -> None:
        if not self._selected_entry: return
        self._editing_entry_key = None
        self.entry_key_edit.setText(new_net_entry_key()); self.entry_key_edit.setReadOnly(False)
        self.entry_source_edit.setText(self._selected_entry.source_key); self.entry_name_edit.setText(f"{self._selected_entry.name} copy")
        self.entry_scope_edit.setText(self._selected_entry.scope or ""); self.entry_description_edit.setText(self._selected_entry.description or "")
        self.entry_editor.setVisible(True)

    def save_entry(self) -> None:
        prior = self._selected_entry if self._editing_entry_key else None
        try:
            entry = NetDirectoryEntry(self.entry_key_edit.text(), self.entry_source_edit.text(), self.entry_name_edit.text(), description=self.entry_description_edit.text().strip() or None, scope=self.entry_scope_edit.text().strip() or None, active=prior.active if prior else True, retired=prior.retired if prior else False, replacement_net_entry_key=prior.replacement_net_entry_key if prior else None)
            self.store.update_net_entry(entry) if self._editing_entry_key else self.store.create_net_entry(entry)
        except (CatalogValidationError, ReadOnlyResourceError, ValueError) as exc:
            self.status_label.setText(f"Cannot save directory entry: {exc}"); return
        self.entry_editor.setVisible(False); self.status_label.setText("Directory entry saved."); self.refresh_results()

    def retire_entry(self) -> None:
        if not self._selected_entry: return
        try: self.store.retire_net_entry(self._selected_entry.net_entry_key)
        except (CatalogValidationError, ReadOnlyResourceError) as exc: self.status_label.setText(f"Cannot retire directory entry: {exc}"); return
        self.status_label.setText("Directory entry retired; historical/session references remain."); self.refresh_results()

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
        for field in (self.session_source_edit, self.session_frequency_edit, self.session_recurrence_edit, self.session_start_edit, self.session_timezone_edit, self.session_duration_edit): field.clear()
        self.session_source_edit.setText(self._selected_entry.source_key); self.session_service_edit.setCurrentText("AMATEUR"); self.session_editor.setVisible(True)

    def begin_edit_session(self) -> None:
        if not self._selected_session: return
        session = self._selected_session; self._editing_session_key = session.net_session_key
        self.session_key_edit.setText(session.net_session_key); self.session_key_edit.setReadOnly(True); self.session_source_edit.setText(session.source_key)
        self.session_frequency_edit.setText(session.frequency_resource_key or ""); self.session_service_edit.setCurrentText(session.service); self.session_recurrence_edit.setText(session.recurrence or "")
        self.session_start_edit.setText(session.local_start_time or ""); self.session_timezone_edit.setText(session.timezone or ""); self.session_duration_edit.setText(str(session.duration_minutes or "")); self.session_editor.setVisible(True)

    def begin_clone_session(self) -> None:
        if not self._selected_session: return
        self.begin_edit_session(); self._editing_session_key = None; self.session_key_edit.setReadOnly(False); self.session_key_edit.setText(new_net_session_key())

    def save_session(self) -> None:
        if not self._selected_entry: return
        prior = self._selected_session if self._editing_session_key else None
        try:
            duration = int(self.session_duration_edit.text()) if self.session_duration_edit.text().strip() else None
            session = NetDirectorySession(self.session_key_edit.text(), self._selected_entry.net_entry_key, self.session_source_edit.text(), self.session_service_edit.currentText(), self.session_frequency_edit.text().strip() or None, recurrence=self.session_recurrence_edit.text().strip() or None, local_start_time=self.session_start_edit.text().strip() or None, timezone=self.session_timezone_edit.text().strip() or None, duration_minutes=duration, active=prior.active if prior else True, retired=prior.retired if prior else False, replacement_net_session_key=prior.replacement_net_session_key if prior else None)
            self.store.update_session(session) if self._editing_session_key else self.store.create_session(session)
        except (CatalogValidationError, ReadOnlyResourceError, ValueError) as exc:
            self.status_label.setText(f"Cannot save session: {exc}"); return
        self.session_editor.setVisible(False); self.status_label.setText("Published session saved."); self._refresh_sessions()

    def retire_session(self) -> None:
        if not self._selected_session: return
        try: self.store.retire_session(self._selected_session.net_session_key)
        except (CatalogValidationError, ReadOnlyResourceError) as exc: self.status_label.setText(f"Cannot retire session: {exc}"); return
        self.status_label.setText("Published session retired; existing schedules remain reviewable."); self._refresh_sessions()

    def delete_session(self) -> None:
        if not self._selected_session: return
        usage = self.store.session_usage(self._selected_session.net_session_key)
        if usage.is_referenced: self.status_label.setText(f"Cannot delete: used by {usage.total_references} record(s). Retire it instead."); return
        if QMessageBox.question(self, "Delete published session", "Delete this unreferenced session? This cannot be undone.") != QMessageBox.Yes: return
        try: self.store.delete_session_if_unreferenced(self._selected_session.net_session_key)
        except (ReferencedResourceError, ReadOnlyResourceError) as exc: self.status_label.setText(f"Cannot delete session: {exc}"); return
        self._selected_session = None; self.status_label.setText("Published session deleted."); self._refresh_sessions()


__all__ = ["NetDirectoryView", "new_net_entry_key", "new_net_session_key"]
