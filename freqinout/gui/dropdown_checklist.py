from __future__ import annotations

from typing import Iterable, Sequence

from PySide6.QtCore import Signal
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QCheckBox, QHBoxLayout, QLabel, QMenu, QPushButton, QVBoxLayout, QWidget, QWidgetAction


class DropdownChecklist(QPushButton):
    """Compact multi-select filter button backed by a checklist menu."""

    selectionChanged = Signal()

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self._label = str(label or "").strip()
        self._options: list[tuple[str, str]] = []
        self._selected: set[str] = set()
        self._checkboxes: dict[str, QCheckBox] = {}
        self._menu = QMenu(self)
        self.setMenu(self._menu)
        self.setMinimumWidth(132)
        self._update_text()

    def set_options(
        self,
        options: Iterable[tuple[str, str]],
        *,
        selected_values: Sequence[str] | None = None,
        select_all_when_empty: bool = True,
    ) -> None:
        self._set_options_with_sections(
            [(None, list(options))],
            selected_values=selected_values,
            select_all_when_empty=select_all_when_empty,
        )

    def set_grouped_options(
        self,
        sections: Iterable[tuple[str, Iterable[tuple[str, str]]]],
        *,
        selected_values: Sequence[str] | None = None,
        select_all_when_empty: bool = True,
    ) -> None:
        self._set_options_with_sections(
            [(str(section or "").strip() or None, list(options)) for section, options in sections],
            selected_values=selected_values,
            select_all_when_empty=select_all_when_empty,
        )

    def _set_options_with_sections(
        self,
        sections: Iterable[tuple[str | None, Iterable[tuple[str, str]]]],
        *,
        selected_values: Sequence[str] | None = None,
        select_all_when_empty: bool = True,
    ) -> None:
        seen: set[str] = set()
        normalized: list[tuple[str, str]] = []
        self._sections: list[tuple[str | None, list[str]]] = []
        for section, options in sections:
            section_values: list[str] = []
            for value, label in options:
                clean_value = str(value or "").strip()
                clean_label = str(label or clean_value).strip()
                if not clean_value or clean_value in seen:
                    continue
                seen.add(clean_value)
                normalized.append((clean_value, clean_label or clean_value))
                section_values.append(clean_value)
            if section_values:
                self._sections.append((section, section_values))
        self._options = normalized
        option_values = {value for value, _label in self._options}
        if selected_values is None:
            self._selected = set(option_values) if select_all_when_empty else set()
        else:
            self._selected = {str(value or "").strip() for value in selected_values if str(value or "").strip()}
            self._selected &= option_values
            if select_all_when_empty and not self._selected:
                self._selected = set(option_values)
        self._rebuild_menu()
        self._update_text()

    def selected_values(self) -> set[str]:
        return set(self._selected)

    def set_selected_values(self, values: Sequence[str]) -> None:
        option_values = {value for value, _label in self._options}
        next_values = {str(value or "").strip() for value in values if str(value or "").strip()} & option_values
        if next_values == self._selected:
            return
        self._selected = next_values
        for value, checkbox in self._checkboxes.items():
            checkbox.blockSignals(True)
            checkbox.setChecked(value in self._selected)
            checkbox.blockSignals(False)
        self._update_text()
        self.selectionChanged.emit()

    def all_selected(self) -> bool:
        option_values = {value for value, _label in self._options}
        return bool(option_values) and self._selected == option_values

    def _rebuild_menu(self) -> None:
        self._menu.clear()
        self._checkboxes = {}
        labels = {value: label for value, label in self._options}
        all_action = QAction("All", self._menu)
        all_action.triggered.connect(self._select_all)
        self._menu.addAction(all_action)
        none_action = QAction("None", self._menu)
        none_action.triggered.connect(self._select_none)
        self._menu.addAction(none_action)
        self._menu.addSeparator()
        sections = getattr(self, "_sections", [(None, [value for value, _label in self._options])])
        first_section = True
        for section, values in sections:
            if section:
                if not first_section:
                    self._menu.addSeparator()
                self._add_section_header(
                    section,
                    values if self._section_needs_bulk_controls(section, values) else (),
                )
            first_section = False
            for value in values:
                label = labels.get(value, value)
                self._add_checkbox_action(value, label)

    def _add_section_header(self, section: str, bulk_values: Sequence[str] = ()) -> None:
        action_widget = QWidget(self._menu)
        layout = QHBoxLayout(action_widget)
        layout.setContentsMargins(8, 5, 8, 3)
        layout.setSpacing(8)
        label = QLabel(section, action_widget)
        label.setStyleSheet("font-weight: 700;")
        layout.addWidget(label)
        layout.addStretch(1)
        values = tuple(str(value or "").strip() for value in bulk_values if str(value or "").strip())
        if values:
            all_btn = QPushButton("All", action_widget)
            all_btn.setFlat(True)
            all_btn.clicked.connect(lambda _checked=False, vals=values: self._select_section(vals))
            none_btn = QPushButton("None", action_widget)
            none_btn.setFlat(True)
            none_btn.clicked.connect(lambda _checked=False, vals=values: self._clear_section(vals))
            layout.addWidget(all_btn)
            layout.addWidget(none_btn)
        widget_action = QWidgetAction(self._menu)
        widget_action.setDefaultWidget(action_widget)
        self._menu.addAction(widget_action)

    @staticmethod
    def _section_needs_bulk_controls(section: str, values: Sequence[str]) -> bool:
        return len(values) > 1 and str(section or "").strip().lower().startswith("other")

    def _add_checkbox_action(self, value: str, label: str) -> None:
        checkbox = QCheckBox(label, self._menu)
        checkbox.setChecked(value in self._selected)
        checkbox.stateChanged.connect(lambda _state, v=value: self._on_checkbox_changed(v))
        action_widget = QWidget(self._menu)
        layout = QVBoxLayout(action_widget)
        layout.setContentsMargins(8, 2, 8, 2)
        layout.addWidget(checkbox)
        widget_action = QWidgetAction(self._menu)
        widget_action.setDefaultWidget(action_widget)
        self._menu.addAction(widget_action)
        self._checkboxes[value] = checkbox

    def _select_all(self) -> None:
        self.set_selected_values([value for value, _label in self._options])

    def _select_none(self) -> None:
        self.set_selected_values([])

    def _select_section(self, values: Sequence[str]) -> None:
        self.set_selected_values(sorted(self._selected | {str(value or "").strip() for value in values}))

    def _clear_section(self, values: Sequence[str]) -> None:
        remove = {str(value or "").strip() for value in values}
        self.set_selected_values(sorted(self._selected - remove))

    def _on_checkbox_changed(self, value: str) -> None:
        checkbox = self._checkboxes.get(value)
        if checkbox is None:
            return
        if checkbox.isChecked():
            self._selected.add(value)
        else:
            self._selected.discard(value)
        self._update_text()
        self.selectionChanged.emit()

    def _update_text(self) -> None:
        total = len(self._options)
        selected = len(self._selected)
        if total <= 0:
            value = "None"
        elif selected == total:
            value = "All"
        elif selected <= 0:
            value = "None"
        else:
            value = f"{selected} selected"
        text = f"{self._label}: {value}" if self._label else value
        self.setText(text)
        self.setToolTip(text)
