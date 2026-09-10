"""Lazy Tools & Resources host for the canonical LN-1 catalog."""

from __future__ import annotations

import json
from pathlib import Path

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.known_operating_groups import net_resources_db_path
from freqinout.core.navigation_intent import NavigationIntent
from freqinout.core.resource_catalog_store import MAX_RESULTS, ResourceCatalogStore
from freqinout.core.resource_catalog_transfer import (
    ImportPreview,
    apply_import_preview,
    export_selected_resources,
    preview_json_import,
)
from freqinout.gui.frequency_catalog_view import FrequencyCatalogView
from freqinout.gui.help_registry import resolve_help_host
from freqinout.gui.net_directory_view import NetDirectoryView


class ResourceImportExportView(QWidget):
    """Preview-first catalog exchange surface backed by the transfer service."""

    def __init__(self, store: ResourceCatalogStore, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.store = store
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        title = QLabel("Resource Import / Export")
        title.setStyleSheet("font-weight: 700; font-size: 17px;")
        layout.addWidget(title)
        copy = QLabel(
            "Choose an export file or preview an import before it can change the catalog. "
            "Imports never overwrite bundled read-only records and apply only after confirmation."
        )
        copy.setWordWrap(True)
        layout.addWidget(copy)
        self.summary = QLabel("")
        self.summary.setWordWrap(True)
        layout.addWidget(self.summary)
        refresh = QPushButton("Refresh Catalog Summary", self)
        refresh.clicked.connect(self.refresh_summary)
        layout.addWidget(refresh)
        exchange = QWidget(self)
        form = QFormLayout(exchange)
        self.export_frequency_keys = QLineEdit(exchange)
        self.export_frequency_keys.setPlaceholderText("Frequency keys, comma separated")
        self.export_net_entry_keys = QLineEdit(exchange)
        self.export_net_entry_keys.setPlaceholderText("Net entry keys, comma separated")
        self.import_source_key = QLineEdit(exchange)
        self.import_source_key.setText("source_imported_transfer")
        self.import_source_key.setAccessibleName("Import target source key")
        form.addRow("Export frequencies", self.export_frequency_keys)
        form.addRow("Export nets", self.export_net_entry_keys)
        form.addRow("Import target source", self.import_source_key)
        action_row = QHBoxLayout()
        self.export_btn = QPushButton("Export Selected…", exchange)
        self.preview_btn = QPushButton("Preview Import…", exchange)
        self.cancel_preview_btn = QPushButton("Cancel Preview", exchange)
        self.apply_btn = QPushButton("Apply Preview", exchange)
        for button in (self.export_btn, self.preview_btn, self.cancel_preview_btn, self.apply_btn):
            action_row.addWidget(button)
        form.addRow(action_row)
        layout.addWidget(exchange)
        self.diagnostics = QPlainTextEdit(self)
        self.diagnostics.setReadOnly(True)
        self.diagnostics.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        self.diagnostics.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.diagnostics.setPlaceholderText("Import preview diagnostics appear here. Previewing does not modify catalog data.")
        self.diagnostics.setAccessibleName("Resource transfer preview diagnostics")
        layout.addWidget(self.diagnostics, 1)
        self._preview: ImportPreview | None = None
        self.export_btn.clicked.connect(self.export_selected)
        self.preview_btn.clicked.connect(self.choose_import_and_preview)
        self.cancel_preview_btn.clicked.connect(self.cancel_preview)
        self.apply_btn.clicked.connect(self.apply_preview)
        self.cancel_preview_btn.setEnabled(False)
        self.apply_btn.setEnabled(False)
        layout.addStretch(1)
        self.refresh_summary()

    def refresh_summary(self) -> None:
        frequency_count = len(self.store.list_frequencies(active=None, limit=MAX_RESULTS))
        net_count = len(self.store.list_net_entries(active=None, limit=MAX_RESULTS))
        self.summary.setText(
            f"This bounded summary shows up to {MAX_RESULTS} frequency records and {MAX_RESULTS} directory entries: "
            f"{frequency_count} frequencies, {net_count} nets."
        )

    @staticmethod
    def _keys(value: str) -> tuple[str, ...]:
        return tuple(key.strip() for key in value.split(",") if key.strip())

    def export_selected(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Export Catalog Resources", "resource_catalog.json", "JSON files (*.json)")
        if not path:
            return
        payload = export_selected_resources(
            self.store,
            frequency_resource_keys=self._keys(self.export_frequency_keys.text()),
            net_entry_keys=self._keys(self.export_net_entry_keys.text()),
        )
        try:
            Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            self.diagnostics.setPlainText(f"Export failed: {exc}")
            return
        self.diagnostics.setPlainText(f"Exported {len(payload['frequencies'])} frequency record(s), {len(payload['net_entries'])} net(s), and {len(payload['sessions'])} net meeting(s).")

    def choose_import_and_preview(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Preview Catalog Import", "", "JSON files (*.json)")
        if not path:
            return
        try:
            payload = Path(path).read_bytes()
        except OSError as exc:
            self.diagnostics.setPlainText(f"Cannot read import file: {exc}")
            return
        self._preview = preview_json_import(self.store, payload, target_source_key=self.import_source_key.text().strip() or "source_imported_transfer")
        self._render_preview(self._preview)

    def _render_preview(self, preview: ImportPreview) -> None:
        lines = []
        for item in preview.diagnostics:
            detail = " · ".join(part for part in (item.reason, item.suggested_resolution) if part)
            lines.append(f"{item.status.upper()} {item.item_type} {item.item_key or '—'}{': ' + detail if detail else ''}")
        self.diagnostics.setPlainText("\n".join(lines) or "Preview contains no transferable items.")
        blocked = {"invalid", "duplicate", "ambiguous", "conflict"}
        self.cancel_preview_btn.setEnabled(True)
        self.apply_btn.setEnabled(preview.actionable and not any(item.status in blocked for item in preview.diagnostics))

    def cancel_preview(self) -> None:
        self._preview = None
        self.diagnostics.clear()
        self.apply_btn.setEnabled(False)
        self.cancel_preview_btn.setEnabled(False)

    def apply_preview(self) -> None:
        if not self._preview or not self.apply_btn.isEnabled():
            return
        if QMessageBox.question(self, "Apply catalog import", "Apply the reviewed catalog import now?") != QMessageBox.Yes:
            return
        try:
            result = apply_import_preview(self.store, self._preview)
        except Exception as exc:  # Store reports validation/read-only context to the operator.
            self.diagnostics.appendPlainText(f"\nApply failed: {exc}")
            return
        self.diagnostics.setPlainText("\n".join(f"{item.status.upper()} {item.item_type} {item.item_key or '—'}" for item in result))
        self._preview = None
        self.apply_btn.setEnabled(False)
        self.cancel_preview_btn.setEnabled(False)
        self.refresh_summary()


class ResourcesTab(QWidget):
    """Tools & Resources workspace with lazy child construction."""

    TAB_LABELS = ("Frequency Catalog", "Net Directory", "Import / Export")
    SECTION_INDEX = {"frequency_catalog": 0, "net_directory": 1, "import_export": 2}
    add_to_hf_nets_requested = Signal(object)
    open_hf_schedule_requested = Signal(object)
    return_requested = Signal(object)

    def __init__(self, parent: QWidget | None = None, *, store: ResourceCatalogStore | None = None, db_path: str | Path | None = None) -> None:
        super().__init__(parent)
        self.store = store or ResourceCatalogStore(Path(db_path) if db_path is not None else net_resources_db_path())
        self._pages: dict[int, QWidget] = {}
        self._navigation_intent: NavigationIntent | None = None
        self._build_ui()
        self._ensure_page(0)

    def _open_context_help(self) -> None:
        """Open task-specific guide content through the owning application shell."""
        host = resolve_help_host(self)
        if host is not None and hasattr(host, "open_context_help"):
            try:
                host.open_context_help("tab.tools-resources")
            except Exception:
                pass

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        header = QHBoxLayout()
        title = QLabel("Tools & Resources")
        title.setStyleSheet("font-weight: 700; font-size: 18px;")
        title.setAccessibleName("Tools and Resources")
        header.addWidget(title)
        header.addStretch(1)
        self.help_btn = QPushButton("Help", self)
        self.help_btn.setToolTip("Open Tools and Resources help.")
        self.help_btn.setAccessibleName("Open Tools and Resources help")
        self.help_btn.clicked.connect(self._open_context_help)
        header.addWidget(self.help_btn)
        layout.addLayout(header)
        self.help_label = QLabel("Reusable reference data; not an active schedule or radio control surface.")
        self.help_label.setWordWrap(True)
        self.help_label.setAccessibleName("Tools and Resources purpose")
        layout.addWidget(self.help_label)
        self.context_bar = QWidget(self)
        context_layout = QHBoxLayout(self.context_bar)
        context_layout.setContentsMargins(8, 4, 8, 4)
        self.context_label = QLabel("", self.context_bar)
        self.context_label.setWordWrap(True)
        context_layout.addWidget(self.context_label, 1)
        self.return_btn = QPushButton("Return", self.context_bar)
        self.return_btn.clicked.connect(self._return_to_origin)
        context_layout.addWidget(self.return_btn)
        self.context_bar.setVisible(False)
        layout.addWidget(self.context_bar)
        self.tabs = QTabWidget(self)
        self.tabs.setObjectName("toolsResourcesTabs")
        self.tabs.setDocumentMode(True)
        self.tabs.setUsesScrollButtons(True)
        self.tabs.setAccessibleName("Tools and Resources workspace")
        for title in self.TAB_LABELS:
            placeholder = QLabel("Opening bounded resource workspace…", self.tabs)
            placeholder.setWordWrap(True)
            self.tabs.addTab(placeholder, title)
        self.tabs.currentChanged.connect(self._ensure_page)
        layout.addWidget(self.tabs, 1)

    def _ensure_page(self, index: int) -> None:
        if index in self._pages or index < 0:
            return
        if index == 0:
            page: QWidget = FrequencyCatalogView(self.store, self.tabs)
        elif index == 1:
            page = NetDirectoryView(self.store, self.tabs)
            page.add_to_hf_nets_requested.connect(self.add_to_hf_nets_requested.emit)
            page.open_hf_schedule_requested.connect(self.open_hf_schedule_requested.emit)
        else:
            page = ResourceImportExportView(self.store, self.tabs)
        old = self.tabs.widget(index)
        # Register before Qt emits currentChanged while replacing the placeholder.
        self._pages[index] = page
        self.tabs.removeTab(index)
        old.deleteLater()
        self.tabs.insertTab(index, page, self.TAB_LABELS[index])
        self.tabs.setCurrentIndex(index)

    def refresh_catalog(self) -> None:
        """Refresh already-open pages only; unopened tabs remain lazy."""
        for page in self._pages.values():
            refresh = getattr(page, "refresh_results", None) or getattr(page, "refresh_summary", None)
            if callable(refresh):
                refresh()

    def open_section(self, section_key: str) -> QWidget | None:
        """Open an owned section by stable navigation key, preserving lazy tabs."""
        index = self.SECTION_INDEX.get(str(section_key or "").strip().lower())
        if index is None:
            return None
        self._ensure_page(index)
        self.tabs.setCurrentIndex(index)
        return self._pages.get(index)

    def set_navigation_intent(self, intent: NavigationIntent | None) -> None:
        """Present an explicit return path for contextual catalog work."""
        self._navigation_intent = intent
        if intent is None:
            self.context_bar.setVisible(False)
            return
        origin = "Local Nets" if intent.origin_surface == "local_nets_editor" else "previous workspace"
        self.context_label.setText(
            f"Catalog opened from {origin}. Your unsaved selections are retained."
        )
        self.return_btn.setText(f"Return to {origin}")
        self.context_bar.setVisible(True)

    def _return_to_origin(self) -> None:
        intent = self._navigation_intent
        if intent is None:
            return
        self._navigation_intent = None
        self.context_bar.setVisible(False)
        self.return_requested.emit(intent)


ToolsResourcesTab = ResourcesTab


__all__ = ["ResourceImportExportView", "ResourcesTab", "ToolsResourcesTab"]
