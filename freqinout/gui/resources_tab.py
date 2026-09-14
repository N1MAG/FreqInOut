"""Lazy Tools & Resources host for the canonical LN-1 catalog."""

from __future__ import annotations

import json
from pathlib import Path

from PySide6.QtCore import Qt, Signal, QEvent
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
    QToolButton,
    QVBoxLayout,
    QWidget,
    QScrollArea,
    QBoxLayout,
)

from freqinout.core.known_operating_groups import net_resources_db_path
from freqinout.core.navigation_intent import NavigationIntent
from freqinout.core.resource_catalog_store import MAX_RESULTS, ResourceCatalogStore
from freqinout.core.resource_catalog_transfer import (
    ExportPreview,
    ImportPreview,
    StaleExportPreviewError,
    apply_import_preview,
    confirm_export_preview,
    preview_resource_export,
    preview_json_import,
)
from freqinout.gui.frequency_catalog_view import FrequencyCatalogView
from freqinout.gui.help_registry import resolve_help_host
from freqinout.gui.net_directory_view import NetDirectoryView
from freqinout.gui.theme import (
    active_app_theme,
    button_height_for_font,
    button_style,
    control_height_for_font,
    label_style,
)


class ResourceImportExportView(QWidget):
    """Preview-first catalog exchange surface backed by the transfer service."""

    def __init__(self, store: ResourceCatalogStore, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.store = store
        outer_layout = QVBoxLayout(self)
        self.content_scroll = QScrollArea(self)
        self.content_scroll.setWidgetResizable(True)
        self.content_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.content_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        body = QWidget(self.content_scroll)
        layout = QVBoxLayout(body)
        self.content_scroll.setWidget(body)
        outer_layout.addWidget(self.content_scroll)
        layout.setContentsMargins(10, 10, 10, 10)
        self.title_label = QLabel("Resource Import / Export")
        layout.addWidget(self.title_label)
        copy = QLabel(
            "Choose an export file or preview an import before it can change the catalog. "
            "Imports never overwrite bundled read-only records and apply only after confirmation."
        )
        copy.setWordWrap(True)
        layout.addWidget(copy)
        self.summary = QLabel("")
        self.summary.setWordWrap(True)
        layout.addWidget(self.summary)
        self.refresh_summary_btn = QPushButton("Refresh Catalog Summary", self)
        self.refresh_summary_btn.clicked.connect(self.refresh_summary)
        layout.addWidget(self.refresh_summary_btn)
        self.export_selection_summary = QLabel("Select frequencies in the Frequency Catalog, then review the export.", self)
        self.export_selection_summary.setWordWrap(True)
        self.export_selection_summary.setAccessibleName("Resource export selection summary")
        layout.addWidget(self.export_selection_summary)
        export_actions = QHBoxLayout()
        self.export_actions_row = export_actions
        self.review_export_btn = QPushButton("Review export…", self)
        self.review_export_btn.setAccessibleName("Review selected resources for export")
        self.review_export_btn.setToolTip("Show selected resources and required dependencies before any file can be written.")
        self.save_reviewed_export_btn = QPushButton("Save reviewed export…", self)
        self.save_reviewed_export_btn.setAccessibleName("Save the reviewed resource export")
        self.cancel_export_preview_btn = QPushButton("Cancel export review", self)
        self.cancel_export_preview_btn.setAccessibleName("Cancel the current resource export review")
        for button in (self.review_export_btn, self.save_reviewed_export_btn, self.cancel_export_preview_btn):
            export_actions.addWidget(button)
        export_actions.addStretch(1)
        layout.addLayout(export_actions)
        self.technical_toggle = QToolButton(self)
        self.technical_toggle.setText("Technical selection entry")
        self.technical_toggle.setCheckable(True)
        self.technical_toggle.setToolTip("Enter stable resource keys only when a contextual catalog selection is not available.")
        self.technical_toggle.setAccessibleName("Show technical resource selection entry")
        layout.addWidget(self.technical_toggle)
        exchange = QWidget(self)
        form = QFormLayout(exchange)
        self.export_frequency_keys = QLineEdit(exchange)
        self.export_frequency_keys.setPlaceholderText("Frequency keys, comma separated")
        self.export_net_entry_keys = QLineEdit(exchange)
        self.export_net_entry_keys.setPlaceholderText("Net entry keys, comma separated")
        self.import_source_key = QLineEdit(self)
        self.import_source_key.setText("source_imported_transfer")
        self.import_source_key.setAccessibleName("Import target source key")
        self.import_source_key.setVisible(False)
        form.addRow("Export frequencies (technical)", self.export_frequency_keys)
        form.addRow("Export nets (technical)", self.export_net_entry_keys)
        exchange.setVisible(False)
        layout.addWidget(exchange)
        import_actions = QHBoxLayout()
        self.import_actions_row = import_actions
        self.preview_btn = QPushButton("Preview import…", self)
        self.cancel_preview_btn = QPushButton("Cancel import preview", self)
        self.apply_btn = QPushButton("Apply import preview", self)
        for button in (self.preview_btn, self.cancel_preview_btn, self.apply_btn):
            import_actions.addWidget(button)
        import_actions.addStretch(1)
        layout.addLayout(import_actions)
        self.diagnostics = QPlainTextEdit(self)
        self.diagnostics.setReadOnly(True)
        self.diagnostics.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        self.diagnostics.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.diagnostics.setPlaceholderText("Import preview diagnostics appear here. Previewing does not modify catalog data.")
        self.diagnostics.setAccessibleName("Resource transfer preview diagnostics")
        layout.addWidget(self.diagnostics, 1)
        self._preview: ImportPreview | None = None
        self._export_preview: ExportPreview | None = None
        self._export_frequency_keys: tuple[str, ...] = ()
        self._export_net_entry_keys: tuple[str, ...] = ()
        self.review_export_btn.clicked.connect(self.review_export)
        self.save_reviewed_export_btn.clicked.connect(self.save_reviewed_export)
        self.cancel_export_preview_btn.clicked.connect(self.cancel_export_preview)
        self.technical_toggle.toggled.connect(exchange.setVisible)
        self.export_frequency_keys.textChanged.connect(self._technical_selection_changed)
        self.export_net_entry_keys.textChanged.connect(self._technical_selection_changed)
        self.preview_btn.clicked.connect(self.choose_import_and_preview)
        self.cancel_preview_btn.clicked.connect(self.cancel_preview)
        self.apply_btn.clicked.connect(self.apply_preview)
        self.cancel_preview_btn.setEnabled(False)
        self.apply_btn.setEnabled(False)
        self.save_reviewed_export_btn.setEnabled(False)
        self.cancel_export_preview_btn.setEnabled(False)
        layout.addStretch(1)
        self.apply_theme()
        self.refresh_summary()

    def apply_theme(self) -> None:
        theme = active_app_theme()
        self.title_label.setStyleSheet(label_style("text", theme, weight=700))
        self.summary.setStyleSheet(label_style("muted", theme))
        self.export_selection_summary.setStyleSheet(label_style("text", theme))
        self.diagnostics.setStyleSheet(f"QPlainTextEdit {{ border: 1px solid {theme['border']}; }}")
        for button in (
            self.help_btn if hasattr(self, "help_btn") else None,
            self.return_btn if hasattr(self, "return_btn") else None,
            self.review_export_btn,
            self.refresh_summary_btn,
            self.save_reviewed_export_btn,
            self.cancel_export_preview_btn,
            self.preview_btn,
            self.cancel_preview_btn,
            self.apply_btn,
        ):
            if button is not None:
                button.setMinimumHeight(button_height_for_font(button))
        for button, role in (
            (self.refresh_summary_btn, "muted"),
            (self.review_export_btn, "primary"),
            (self.save_reviewed_export_btn, "primary"),
            (self.cancel_export_preview_btn, "muted"),
            (self.preview_btn, "primary"),
            (self.cancel_preview_btn, "muted"),
            (self.apply_btn, "primary"),
        ):
            button.setStyleSheet(button_style(role, theme))
        for control in (
            self.export_frequency_keys,
            self.export_net_entry_keys,
            self.import_source_key,
        ):
            control.setMinimumHeight(control_height_for_font(control))
        self.diagnostics.setMinimumHeight(
            max(
                control_height_for_font(self.diagnostics, vertical_padding=18, floor=48) * 3,
                self.diagnostics.fontMetrics().lineSpacing() * 4 + 20,
            )
        )
        self._apply_responsive_layout()

    def _apply_responsive_layout(self) -> None:
        compact = self.width() > 0 and self.width() < max(900, self.fontMetrics().horizontalAdvance("Resource Import / Export") * 25)
        direction = QBoxLayout.TopToBottom if compact else QBoxLayout.LeftToRight
        self.export_actions_row.setDirection(direction)
        self.import_actions_row.setDirection(direction)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._apply_responsive_layout()

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        if event.type() == QEvent.FontChange:
            self.apply_theme()

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

    def set_export_selection(
        self,
        *,
        frequency_resource_keys: tuple[str, ...] | list[str] = (),
        net_entry_keys: tuple[str, ...] | list[str] = (),
    ) -> None:
        """Receive the contextual catalog selection without exposing raw keys."""
        self._export_frequency_keys = tuple(dict.fromkeys(str(key).strip() for key in frequency_resource_keys if str(key).strip()))
        self._export_net_entry_keys = tuple(dict.fromkeys(str(key).strip() for key in net_entry_keys if str(key).strip()))
        self.export_frequency_keys.blockSignals(True)
        self.export_net_entry_keys.blockSignals(True)
        try:
            self.export_frequency_keys.setText(", ".join(self._export_frequency_keys))
            self.export_net_entry_keys.setText(", ".join(self._export_net_entry_keys))
        finally:
            self.export_frequency_keys.blockSignals(False)
            self.export_net_entry_keys.blockSignals(False)
        self.cancel_export_preview()
        self._update_export_selection_summary()

    def _technical_selection_changed(self) -> None:
        self._export_frequency_keys = self._keys(self.export_frequency_keys.text())
        self._export_net_entry_keys = self._keys(self.export_net_entry_keys.text())
        self.cancel_export_preview()
        self._update_export_selection_summary()

    def _update_export_selection_summary(self) -> None:
        count = len(self._export_frequency_keys) + len(self._export_net_entry_keys)
        if count:
            self.export_selection_summary.setText(
                f"{count} directly selected catalog record{'s' if count != 1 else ''}. Review shows dependencies before save."
            )
        else:
            self.export_selection_summary.setText("Select frequencies in the Frequency Catalog, then review the export.")
        self.review_export_btn.setEnabled(count > 0)

    def review_export(self) -> None:
        if not (self._export_frequency_keys or self._export_net_entry_keys):
            self.diagnostics.setPlainText("Select a catalog record before reviewing an export.")
            return
        try:
            self._export_preview = preview_resource_export(
                self.store,
                frequency_resource_keys=self._export_frequency_keys,
                net_entry_keys=self._export_net_entry_keys,
            )
        except (ValueError, OSError) as exc:
            self._export_preview = None
            self.diagnostics.setPlainText(f"Cannot review export: {exc}")
            return
        self._render_export_preview(self._export_preview)
        self.save_reviewed_export_btn.setEnabled(True)
        self.cancel_export_preview_btn.setEnabled(True)

    def _render_export_preview(self, preview: ExportPreview) -> None:
        lines = [
            "Export review — no file has been written.",
            f"Selected: {preview.direct_count}; dependencies: {preview.dependency_count}; total: {preview.total_count} / {preview.max_items}.",
            f"Projected payload: {preview.payload_size:,} / {preview.max_bytes:,} bytes.",
        ]
        for warning in preview.warnings:
            lines.append(f"Warning: {warning}")
        for item in preview.items:
            line = f"{item.item_type} · {item.relationship}: {item.label} · Source: {item.source_label}"
            if item.usage_summary:
                line += f" · Used by — not included in export: {item.usage_summary}"
            lines.append(line)
        self.diagnostics.setPlainText("\n".join(lines))

    def cancel_export_preview(self) -> None:
        self._export_preview = None
        self.save_reviewed_export_btn.setEnabled(False)
        self.cancel_export_preview_btn.setEnabled(False)

    def save_reviewed_export(self) -> None:
        preview = self._export_preview
        if preview is None:
            return
        if QMessageBox.question(
            self,
            "Save reviewed catalog export",
            "Save exactly the reviewed resource export? The catalog will be checked again before choosing a destination.",
        ) != QMessageBox.Yes:
            return
        try:
            payload = confirm_export_preview(self.store, preview)
        except StaleExportPreviewError as exc:
            self.cancel_export_preview()
            self.diagnostics.appendPlainText(f"\nExport review is no longer current: {exc}")
            return
        except (ValueError, OSError) as exc:
            self.diagnostics.appendPlainText(f"\nExport cannot be confirmed: {exc}")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Save Reviewed Catalog Export", "resource_catalog.json", "JSON files (*.json)")
        if not path:
            return
        try:
            Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            self.diagnostics.setPlainText(f"Export failed: {exc}")
            return
        self.diagnostics.appendPlainText(
            f"\nExported {len(payload['frequencies'])} frequency record(s), {len(payload['net_entries'])} net(s), and {len(payload['sessions'])} net meeting(s)."
        )
        self.cancel_export_preview()

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
        self.title_label = QLabel("Tools & Resources")
        self.title_label.setAccessibleName("Tools and Resources")
        header.addWidget(self.title_label)
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
        self.apply_theme()

    def apply_theme(self) -> None:
        theme = active_app_theme()
        self.title_label.setStyleSheet(label_style("text", theme, weight=700))
        self.help_label.setStyleSheet(label_style("muted", theme))
        self.context_label.setStyleSheet(label_style("info", theme))
        self.help_btn.setStyleSheet(button_style("muted", theme))
        self.return_btn.setStyleSheet(button_style("secondary", theme))
        self.help_btn.setMinimumHeight(button_height_for_font(self.help_btn))
        self.return_btn.setMinimumHeight(button_height_for_font(self.return_btn))
        for page in self._pages.values():
            apply_theme = getattr(page, "apply_theme", None)
            if callable(apply_theme):
                apply_theme()

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        if event.type() == QEvent.FontChange:
            self.apply_theme()

    def _ensure_page(self, index: int) -> None:
        if index in self._pages or index < 0:
            return
        if index == 0:
            page: QWidget = FrequencyCatalogView(self.store, self.tabs)
            page.review_export_requested.connect(self._open_frequency_export_review)
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

    def _open_frequency_export_review(self, frequency_keys: object) -> None:
        """Carry an explicit catalog selection into the shared export review."""
        keys = tuple(str(key).strip() for key in (frequency_keys or ()) if str(key).strip())
        page = self.open_section("import_export")
        if isinstance(page, ResourceImportExportView):
            page.set_export_selection(frequency_resource_keys=keys)
            page.review_export()

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
