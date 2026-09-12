"""Cache-only workspace shell for Settings software administration.

The host owns navigation, draft state, editor construction, and any explicit
validation work.  This widget intentionally only renders an immutable
``SoftwareAdministrationSnapshot`` and emits selection/routing signals.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Mapping, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QStackedWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.software_administration_model import (
    SoftwareAdministrationSnapshot,
    SoftwareFamilySummary,
)
from freqinout.gui.theme import get_theme, resolve_theme, resolve_ui_text_scale


_TASKS: dict[str, tuple[tuple[str, str], ...]] = {
    "js8call": (
        ("overview", "Overview"), ("application_profile", "Application & Profile"),
        ("api_radio", "API & Radio"), ("message_storage", "Message Storage"),
        ("ingest_forms", "Ingest & Forms"), ("launch", "Launch"),
        ("health", "Health"), ("advanced", "Advanced"),
    ),
    "fast_light": (
        ("overview", "Overview"), ("flrig_control", "FLRig Control"),
        ("fldigi_modem_logs", "FLDigi Modem & Logs"), ("flmsg", "FLMsg"),
        ("flamp_signing", "FLAmp & Signing"), ("message_folders", "Message Folders"),
        ("launch", "Launch"), ("health", "Health"), ("advanced", "Advanced"),
    ),
    "varac": (
        ("overview", "Overview"), ("application_radio", "Application & Radio"),
        ("runtime_paths", "Runtime & Paths"), ("inbox_outbox", "Inbox & Outbox"),
        ("inbound_guard", "Inbound Guard"), ("cluster", "Cluster"),
        ("launch", "Launch"), ("health", "Health"), ("advanced", "Advanced"),
    ),
    "commstat": (
        ("overview", "Overview"), ("transport_mapping", "Transport Mapping"),
        ("installation", "Installation"), ("launch", "Launch"), ("health", "Health"),
        ("advanced", "Advanced"),
    ),
    "external_spotter": (
        ("overview", "Overview"), ("installation", "Installation"), ("profile", "Profile"),
        ("forms", "Form Synchronization"), ("launch", "Launch"), ("health", "Health"),
        ("advanced", "Advanced"),
    ),
    "fio_spotter": (
        ("overview", "Overview"), ("dependencies", "Dependencies & Radio Mapping"),
        ("operational_workspace", "Open FIO Spotter"),
    ),
}


class SoftwareAdministrationWorkspace(QWidget):
    """Software-first Settings shell with no configuration or endpoint I/O."""

    family_selected = Signal(str)
    radio_selected = Signal(object)
    task_selected = Signal(str)
    assign_requested = Signal(str)
    operational_route_requested = Signal(str)
    save_all_requested = Signal()

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._snapshot = SoftwareAdministrationSnapshot(families=())
        self._family_key = ""
        self._radio_id: Optional[int] = None
        self._task_key = ""
        # The Settings host owns draft values and persistence.  The workspace
        # receives only this compact, cache-only projection so that chip
        # selection and repainting cannot inspect configuration or save data.
        self._dirty_contexts: frozenset[tuple[int, str]] = frozenset()
        self._theme: dict[str, str] = get_theme("light")
        self._text_scale = 1.0
        self._base_font = QFont(self.font())
        self._family_buttons: dict[str, QToolButton] = {}
        self._radio_buttons: dict[Optional[int], QToolButton] = {}
        self._task_buttons: dict[str, QToolButton] = {}
        # A registered editor has one clear owner: (family, optional radio,
        # task).  Editors remain in the stack when the operator changes chips,
        # so partially completed values and focus state are not discarded.
        self._registered_editors: dict[tuple[str, Optional[int], str], QWidget] = {}
        self._editor_keys_by_widget: dict[QWidget, tuple[str, Optional[int], str]] = {}
        self._legacy_editor: Optional[QWidget] = None
        self._build_ui()
        self.apply_theme(self._theme)

    def _build_ui(self) -> None:
        self.setAccessibleName("Software administration workspace")
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        self.heading_label = QLabel("Software administration")
        self.heading_label.setAccessibleName("Software administration heading")
        root.addWidget(self.heading_label)
        self.explanation_label = QLabel(
            "Choose software, then a radio and task. Selections only change the workspace; "
            "they do not save, scan, or contact a radio."
        )
        self.explanation_label.setWordWrap(True)
        root.addWidget(self.explanation_label)

        self.family_prompt_label = QLabel("1. Choose software family")
        root.addWidget(self.family_prompt_label)
        self.family_strip, self.family_layout = self._horizontal_strip("Software family choices")
        root.addWidget(self.family_strip)

        self.radio_prompt_label = QLabel("2. Choose radio context")
        root.addWidget(self.radio_prompt_label)
        self.radio_strip, self.radio_layout = self._horizontal_strip("Radio context choices")
        root.addWidget(self.radio_strip)

        self.context_banner = QLabel("Choose a software family to begin.")
        self.context_banner.setObjectName("softwareAdministrationContextBanner")
        self.context_banner.setWordWrap(True)
        self.context_banner.setAccessibleName("Selected software context")
        root.addWidget(self.context_banner)

        self.task_prompt_label = QLabel("3. Choose configuration task")
        root.addWidget(self.task_prompt_label)
        self.task_strip, self.task_layout = self._horizontal_strip("Configuration task choices")
        root.addWidget(self.task_strip)

        self.unassigned_label = QLabel()
        self.unassigned_label.setWordWrap(True)
        self.unassigned_label.setAccessibleName("Unassigned software instances")
        root.addWidget(self.unassigned_label)

        self.assign_button = QPushButton("Assign software to a radio")
        self.assign_button.setAccessibleName("Assign selected software to a radio")
        self.assign_button.setToolTip("Open assignment for the selected software family")
        self.assign_button.clicked.connect(self._emit_assign_request)
        self.save_all_button = QPushButton("Save All Changes")
        self.save_all_button.setObjectName("softwareAdministrationSaveAllButton")
        self.save_all_button.setAccessibleName("Save all unsaved software changes")
        self.save_all_button.setToolTip(
            "Save all unsaved software drafts. Review the Unsaved changes labels before continuing."
        )
        self.save_all_button.setEnabled(False)
        self.save_all_button.clicked.connect(self.save_all_requested.emit)
        action_row = QHBoxLayout()
        action_row.setContentsMargins(0, 0, 0, 0)
        action_row.setSpacing(6)
        action_row.addWidget(self.assign_button)
        action_row.addWidget(self.save_all_button)
        action_row.addStretch(1)
        root.addLayout(action_row)

        self.editor_host = QWidget()
        self.editor_host.setObjectName("softwareAdministrationEditorHost")
        self.editor_host.setAccessibleName("Software configuration editor")
        self.editor_host_layout = QVBoxLayout(self.editor_host)
        self.editor_host_layout.setContentsMargins(10, 10, 10, 10)
        self.editor_host_layout.setSpacing(0)
        self.editor_placeholder = QLabel("Choose a configuration task to display its editor here.")
        self.editor_placeholder.setWordWrap(True)
        self.editor_placeholder.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.editor_placeholder.setMargin(8)
        self.editor_placeholder.setAccessibleName("No software configuration editor selected")
        self.editor_stack = QStackedWidget()
        self.editor_stack.setAccessibleName("Software configuration editor surface")
        self.editor_stack.addWidget(self.editor_placeholder)
        self.editor_host_layout.addWidget(self.editor_stack)
        self.editor_host.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.editor_host.setMinimumHeight(96)
        root.addWidget(self.editor_host, 1)

    def set_embedded(self, embedded: bool = True) -> None:
        """Avoid repeating the Settings section title inside the workspace."""

        self.heading_label.setVisible(not bool(embedded))

    def show_family_summary(self, family: Optional[SoftwareFamilySummary]) -> None:
        """Render the non-editing ``All`` context from the cached snapshot."""

        if family is None:
            self.editor_placeholder.setText("Choose a software family to begin.")
        elif not family.assignments:
            self.editor_placeholder.setText(
                f"{family.title} is not assigned to a radio.\n\n"
                "Choose Assign software to a radio to create a clear radio-to-software mapping."
            )
        else:
            rows = []
            for assignment in family.assignments:
                instance = assignment.instance_name or "No linked instance"
                shared = " · Shared" if assignment.is_shared else ""
                rows.append(
                    f"• {assignment.radio_name} — {instance} — "
                    f"{assignment.status_text or 'Not yet verified'}{shared}"
                )
            self.editor_placeholder.setText(
                f"{family.title} across {len(family.assignments)} assigned "
                f"radio{'s' if len(family.assignments) != 1 else ''}\n\n"
                + "\n".join(rows)
                + "\n\nChoose a radio above to review or change its configuration."
            )
        if family is not None and family.unassigned_instances:
            names = ", ".join(item.instance_name for item in family.unassigned_instances[:4])
            remaining = len(family.unassigned_instances) - 4
            suffix = f", plus {remaining} more" if remaining > 0 else ""
            self.editor_placeholder.setText(
                self.editor_placeholder.text()
                + f"\n\nUnassigned instances ({len(family.unassigned_instances)}): {names}{suffix}."
            )
        self.editor_stack.setCurrentWidget(self.editor_placeholder)

    def resizeEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().resizeEvent(event)
        self._apply_compact_height(event.size().height() < 680)

    def _apply_compact_height(self, compact: bool) -> None:
        """Give the task editor priority when vertical space is constrained."""

        self.explanation_label.setVisible(not compact)
        self.family_prompt_label.setVisible(not compact)
        self.radio_prompt_label.setVisible(not compact)
        selected_radio = self._radio_id is not None
        self.task_prompt_label.setVisible(not compact and selected_radio)
        self.task_strip.setVisible(selected_radio)
        for strip in (self.family_strip, self.radio_strip, self.task_strip):
            strip.setMaximumHeight(42 if compact else 52)
        self.unassigned_label.setVisible(not compact)
        self.assign_button.setVisible(not compact or not selected_radio)
        self.save_all_button.setVisible(not compact or bool(self._dirty_contexts))
        self.editor_host.setMinimumHeight(180 if compact else 96)

    def _horizontal_strip(self, accessible_name: str) -> tuple[QScrollArea, QHBoxLayout]:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setAccessibleName(accessible_name)
        content = QWidget()
        layout = QHBoxLayout(content)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        layout.addStretch(1)
        scroll.setWidget(content)
        scroll.setMaximumHeight(52)
        return scroll, layout

    @staticmethod
    def _clear_layout(layout: QHBoxLayout | QVBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    def set_snapshot(self, snapshot: SoftwareAdministrationSnapshot) -> None:
        self._snapshot = snapshot or SoftwareAdministrationSnapshot(families=())
        available = {family.key for family in self._snapshot.families}
        family_key = self._family_key if self._family_key in available else next(
            (family.key for family in self._snapshot.families),
            "",
        )
        self._rebuild_family_buttons()
        self.select_context(family_key, self._radio_id, self._task_key)

    def select_context(
        self, family_key: str, radio_id: Optional[int] = None, task_key: Optional[str] = None
    ) -> None:
        family = self._snapshot.family(family_key)
        self._family_key = family.key if family is not None else ""
        valid_radio_ids = {item.radio_id for item in family.assignments} if family else set()
        self._radio_id = radio_id if radio_id in valid_radio_ids else None
        tasks = _TASKS.get(self._family_key, ())
        valid_task_keys = {key for key, _label in tasks}
        self._task_key = task_key if task_key in valid_task_keys else (tasks[0][0] if tasks else "")
        if self._radio_id is None and tasks:
            self._task_key = tasks[0][0]
        self._rebuild_radio_buttons(family)
        self._rebuild_task_buttons(tasks)
        self._update_context(family)
        self._sync_checked_buttons()
        self._show_registered_editor_for_context()

    def selected_family_key(self) -> str:
        return self._family_key

    def selected_radio_id(self) -> Optional[int]:
        return self._radio_id

    def selected_task_key(self) -> str:
        return self._task_key

    def set_dirty_contexts(
        self,
        contexts: Mapping[tuple[object, object], bool] | Iterable[tuple[object, object]],
    ) -> None:
        """Render the host-owned set of unsaved ``(radio_id, family_key)`` drafts.

        This accepts either a mapping whose truthy values are dirty or an
        iterable of dirty keys.  It intentionally stores no draft content and
        performs no persistence, discovery, timer, or endpoint work.  Unknown
        radio IDs remain represented in their software-family summary so a
        host refresh cannot make a pending draft appear to disappear.
        """
        entries = contexts.items() if isinstance(contexts, Mapping) else contexts
        normalized: set[tuple[int, str]] = set()
        for entry in entries:
            if isinstance(contexts, Mapping):
                key, is_dirty = entry
                if not is_dirty:
                    continue
            else:
                key = entry
            try:
                radio_id, family_key = key
                normalized.add((int(radio_id), str(family_key)))
            except (TypeError, ValueError):
                # The host may be mid-refresh.  Ignore malformed cache data
                # rather than turning a navigation-only update into a UI fault.
                continue
        self._dirty_contexts = frozenset(normalized)
        family = self._snapshot.family(self._family_key)
        self._rebuild_family_buttons()
        self._rebuild_radio_buttons(family)
        self._update_context(family)
        self._sync_checked_buttons()
        self._update_save_all_button()

    def set_dirty_state(
        self,
        contexts: Mapping[tuple[object, object], bool] | Iterable[tuple[object, object]],
    ) -> None:
        """Compatibility alias for :meth:`set_dirty_contexts`."""
        self.set_dirty_contexts(contexts)

    def dirty_contexts(self) -> frozenset[tuple[int, str]]:
        """Return the immutable dirty-key projection currently being rendered."""
        return self._dirty_contexts

    def has_dirty_drafts(self) -> bool:
        return bool(self._dirty_contexts)

    def set_editor_widget(self, widget: Optional[QWidget]) -> None:
        """Show a host-provided editor without deleting a prior editor.

        This compatibility API remains intentionally context-neutral for the
        existing Settings host.  New integrations should prefer
        :meth:`register_task_editor`, which keeps an editor bound to one
        software/radio/task context and restores it automatically after chip
        navigation.
        """
        self._legacy_editor = widget
        if widget is None:
            self.editor_stack.setCurrentWidget(self.editor_placeholder)
            return
        self._ensure_editor_in_stack(widget)
        self.editor_stack.setCurrentWidget(widget)

    def register_task_editor(
        self,
        family_key: str,
        task_key: str,
        widget: QWidget,
        *,
        radio_id: Optional[int] = None,
    ) -> None:
        """Register a durable editor or action surface for one task context.

        ``radio_id=None`` represents an ``All``-radio family surface and is
        used as the fallback when no radio-specific editor is registered.
        Registering the same widget again transfers its ownership to the new
        context; a widget can therefore never be displayed as two unrelated
        configuration surfaces.
        """
        if not family_key or not task_key:
            raise ValueError("family_key and task_key are required")
        if widget is None:
            raise ValueError("widget is required")
        key = (str(family_key), radio_id, str(task_key))
        prior_for_key = self._registered_editors.get(key)
        if prior_for_key is not None and prior_for_key is not widget:
            self._editor_keys_by_widget.pop(prior_for_key, None)
        prior_key = self._editor_keys_by_widget.get(widget)
        if prior_key is not None and prior_key != key:
            self._registered_editors.pop(prior_key, None)
        self._registered_editors[key] = widget
        self._editor_keys_by_widget[widget] = key
        if not widget.accessibleName().strip():
            widget.setAccessibleName(f"{family_key} {task_key} configuration editor")
        self._ensure_editor_in_stack(widget)
        self._show_registered_editor_for_context()

    def set_task_action_surface(
        self,
        family_key: str,
        task_key: str,
        widget: QWidget,
        *,
        radio_id: Optional[int] = None,
    ) -> None:
        """Alias for registering a task-specific non-form action surface."""
        self.register_task_editor(family_key, task_key, widget, radio_id=radio_id)

    def registered_task_editor(
        self, family_key: str, task_key: str, *, radio_id: Optional[int] = None
    ) -> Optional[QWidget]:
        """Return the exact registered editor without creating or probing anything."""
        return self._registered_editors.get((str(family_key), radio_id, str(task_key)))

    def _ensure_editor_in_stack(self, widget: QWidget) -> None:
        if self.editor_stack.indexOf(widget) < 0:
            self.editor_stack.addWidget(widget)

    def _registered_editor_for_context(self) -> Optional[QWidget]:
        exact = (self._family_key, self._radio_id, self._task_key)
        fallback = (self._family_key, None, self._task_key)
        return self._registered_editors.get(exact) or self._registered_editors.get(fallback)

    def _show_registered_editor_for_context(self) -> None:
        editor = self._registered_editor_for_context()
        if editor is not None:
            self.editor_stack.setCurrentWidget(editor)
        elif self._legacy_editor is None:
            self.editor_stack.setCurrentWidget(self.editor_placeholder)

    def apply_theme(self, settings_or_theme: Mapping[str, Any]) -> None:
        values = settings_or_theme or {}
        self._theme = dict(values) if "bg" in values and "surface" in values else resolve_theme(values)
        self._text_scale = resolve_ui_text_scale(values) if "ui_text_size" in values else 1.0
        font = QFont(self._base_font)
        if font.pointSizeF() > 0:
            font.setPointSizeF(max(8.0, font.pointSizeF() * self._text_scale))
        self.setFont(font)
        theme = self._theme
        self.setStyleSheet(
            "QWidget { background: %(bg)s; color: %(text)s; }"
            "QLabel { color: %(text)s; }"
            "QLabel#softwareAdministrationContextBanner { background: %(surface_alt)s;"
            " border: 1px solid %(accent)s; border-radius: 6px; padding: 7px; font-weight: 600; }"
            "QWidget#softwareAdministrationEditorHost { background: %(surface)s;"
            " border: 1px solid %(border)s; border-radius: 6px; }"
            "QToolButton { background: %(surface_alt)s; border: 1px solid %(border)s;"
            " border-radius: 6px; padding: 6px 9px; }"
            "QToolButton:checked { background: %(accent)s; color: white; border-color: %(accent_active)s; }"
            "QToolButton:hover { border-color: %(accent)s; }"
            "QPushButton { background: %(accent)s; color: white; border: 1px solid %(accent_active)s;"
            " border-radius: 6px; padding: 6px 10px; font-weight: 600; }"
            "QPushButton#softwareAdministrationSaveAllButton { background: %(surface_alt)s; color: %(text)s;"
            " border-color: %(accent)s; }"
            "QPushButton:disabled { background: %(surface_alt)s; color: %(text_muted)s; border-color: %(border)s; }"
            % theme
        )
        self.heading_label.setStyleSheet("font-size: 16pt; font-weight: 700;")
        self._apply_minimum_hit_heights()

    def _apply_minimum_hit_heights(self) -> None:
        height = int(32 * self._text_scale)
        for button in (*self._family_buttons.values(), *self._radio_buttons.values(), *self._task_buttons.values()):
            button.setMinimumHeight(height)
        self.assign_button.setMinimumHeight(height)
        self.save_all_button.setMinimumHeight(height)

    def _rebuild_family_buttons(self) -> None:
        self._clear_layout(self.family_layout)
        self._family_buttons = {}
        for family in self._snapshot.families:
            summary = f"{family.assigned_radio_count} radio{'s' if family.assigned_radio_count != 1 else ''}"
            if family.attention_count:
                summary += f" · {family.attention_count} needs attention"
            dirty_count = self._dirty_count_for_family(family.key)
            if dirty_count:
                summary += f" · Unsaved changes ({dirty_count})"
            dirty_hint = (
                f" {dirty_count} radio draft{'s are' if dirty_count != 1 else ' is'} unsaved."
                if dirty_count
                else ""
            )
            button = self._make_chip(
                f"{family.title} · {summary}",
                f"Select {family.title}. {family.description}{dirty_hint}",
            )
            button.clicked.connect(lambda _checked=False, key=family.key: self._choose_family(key))
            self.family_layout.addWidget(button)
            self._family_buttons[family.key] = button
        self.family_layout.addStretch(1)

    def _rebuild_radio_buttons(self, family: Optional[SoftwareFamilySummary]) -> None:
        self._clear_layout(self.radio_layout)
        self._radio_buttons = {}
        dirty_count = self._dirty_count_for_family(family.key) if family else 0
        all_text = "All"
        if dirty_count:
            all_text += f" · Unsaved changes ({dirty_count})"
        all_button = self._make_chip(
            all_text,
            "Show all radios assigned to the selected software family"
            + (f". {dirty_count} radio draft{'s are' if dirty_count != 1 else ' is'} unsaved." if dirty_count else ""),
        )
        all_button.clicked.connect(lambda: self._choose_radio(None))
        self.radio_layout.addWidget(all_button)
        self._radio_buttons[None] = all_button
        for assignment in family.assignments if family else ():
            state = assignment.status_text or "Not yet verified"
            shared = " Shared instance." if assignment.is_shared else ""
            text = f"{assignment.radio_name} — {state}" + (" · Shared" if assignment.is_shared else "")
            dirty = self._is_dirty(assignment.radio_id, family.key) if family else False
            if dirty:
                text += " · Unsaved changes"
            verification_hint = (
                " FIO has not run or received a current verification for this software. "
                "Choose the radio, then Health, to check it."
                if state == "Not yet verified"
                else ""
            )
            button = self._make_chip(
                text,
                f"Select {assignment.radio_name}. Status: {state}.{shared}"
                + verification_hint
                + (" Unsaved changes for this software and radio." if dirty else ""),
            )
            button.clicked.connect(lambda _checked=False, radio_id=assignment.radio_id: self._choose_radio(radio_id))
            self.radio_layout.addWidget(button)
            self._radio_buttons[assignment.radio_id] = button
        self.radio_layout.addStretch(1)

    def _rebuild_task_buttons(self, tasks: tuple[tuple[str, str], ...]) -> None:
        self._clear_layout(self.task_layout)
        self._task_buttons = {}
        for key, label in tasks:
            all_context = self._radio_id is None
            enabled = not all_context or key == "overview"
            tooltip = (
                f"Select {label} task"
                if enabled
                else f"Choose a radio before configuring {label}"
            )
            button = self._make_chip(label, tooltip)
            button.setEnabled(enabled)
            button.clicked.connect(lambda _checked=False, task_key=key: self._choose_task(task_key))
            self.task_layout.addWidget(button)
            self._task_buttons[key] = button
        self.task_layout.addStretch(1)

    def _make_chip(self, text: str, tooltip: str) -> QToolButton:
        button = QToolButton()
        # Qt treats a single ampersand as a mnemonic marker. These chips use
        # natural-language labels, so preserve a visible ``&``.
        button.setText(text.replace("&", "&&"))
        button.setCheckable(True)
        button.setToolButtonStyle(Qt.ToolButtonTextOnly)
        button.setAccessibleName(text)
        button.setToolTip(tooltip)
        button.setMinimumHeight(int(32 * self._text_scale))
        return button

    def _choose_family(self, key: str) -> None:
        if key == self._family_key:
            return
        self.select_context(key)
        self.family_selected.emit(self._family_key)

    def _choose_radio(self, radio_id: Optional[int]) -> None:
        if radio_id == self._radio_id:
            return
        self._radio_id = radio_id
        tasks = _TASKS.get(self._family_key, ())
        if radio_id is None and tasks:
            self._task_key = tasks[0][0]
        self._rebuild_task_buttons(tasks)
        self._update_context(self._snapshot.family(self._family_key))
        self._sync_checked_buttons()
        self._show_registered_editor_for_context()
        self.radio_selected.emit(radio_id)

    def _choose_task(self, key: str) -> None:
        if self._radio_id is None and key != "overview":
            return
        self._task_key = key
        self._sync_checked_buttons()
        self._show_registered_editor_for_context()
        if key == "operational_workspace":
            route = self._snapshot.family(self._family_key)
            self.operational_route_requested.emit(route.operational_route if route else "")
        self.task_selected.emit(key)

    def _emit_assign_request(self) -> None:
        if self._family_key:
            self.assign_requested.emit(self._family_key)

    def _update_context(self, family: Optional[SoftwareFamilySummary]) -> None:
        if family is None:
            self.context_banner.setText("Choose a software family to begin.")
            self.unassigned_label.setText("")
            self.assign_button.setEnabled(False)
            return
        self.assign_button.setEnabled(True)
        assignment = next((item for item in family.assignments if item.radio_id == self._radio_id), None)
        if assignment is None:
            dirty_count = self._dirty_count_for_family(family.key)
            suffix = (
                f" {dirty_count} radio draft{'s have' if dirty_count != 1 else ' has'} unsaved changes."
                if dirty_count
                else ""
            )
            self.context_banner.setText(f"Viewing all radios using {family.title}.{suffix}")
        else:
            instance = assignment.instance_name or "No linked instance"
            other_radios = tuple(name for name in assignment.shared_radio_names if name != assignment.radio_name)
            shared = f" Shared with: {', '.join(other_radios)}." if other_radios else ""
            dirty = self._is_dirty(assignment.radio_id, family.key)
            dirty_suffix = " Unsaved changes for this software and radio." if dirty else ""
            self.context_banner.setText(
                f"Editing {family.title} for {assignment.radio_name} — Instance: {instance}. "
                f"Status: {assignment.status_text}.{shared}{dirty_suffix}"
            )
        unassigned = family.unassigned_instances
        if unassigned:
            names = ", ".join(
                f"{item.instance_name} ({'enabled' if item.enabled else 'disabled'})"
                for item in unassigned[:4]
            )
            remaining = len(unassigned) - 4
            suffix = f", plus {remaining} more" if remaining > 0 else ""
            self.unassigned_label.setText(f"Unassigned instances ({len(unassigned)}): {names}{suffix}.")
        else:
            self.unassigned_label.setText("Unassigned instances: none.")
        self._apply_compact_height(self.height() < 680)

    def _sync_checked_buttons(self) -> None:
        for key, button in self._family_buttons.items():
            button.setChecked(key == self._family_key)
        for radio_id, button in self._radio_buttons.items():
            button.setChecked(radio_id == self._radio_id)
        for key, button in self._task_buttons.items():
            button.setChecked(key == self._task_key)

    def _is_dirty(self, radio_id: int, family_key: str) -> bool:
        return (int(radio_id), str(family_key)) in self._dirty_contexts

    def _dirty_count_for_family(self, family_key: str) -> int:
        return sum(1 for _radio_id, key in self._dirty_contexts if key == str(family_key))

    def _update_save_all_button(self) -> None:
        count = len(self._dirty_contexts)
        self.save_all_button.setEnabled(count > 0)
        label = "Save All Changes"
        if count:
            label += f" ({count})"
        self.save_all_button.setText(label)
        self.save_all_button.setAccessibleName(
            f"Save all unsaved software changes for {count} radio context{'s' if count != 1 else ''}"
        )


__all__ = ["SoftwareAdministrationWorkspace"]
