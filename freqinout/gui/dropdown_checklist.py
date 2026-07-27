from __future__ import annotations

from typing import Iterable, Sequence

from PySide6.QtCore import Signal
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QCheckBox, QMenu, QPushButton, QVBoxLayout, QWidget, QWidgetAction


class DropdownChecklist(QPushButton):
    """Compact multi-select filter button backed by a checklist menu."""

    selectionChanged = Signal()

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self._label = str(label or "Filter").strip() or "Filter"
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
        seen: set[str] = set()
        normalized: list[tuple[str, str]] = []
        for value, label in options:
            clean_value = str(value or "").strip()
            clean_label = str(label or clean_value).strip()
            if not clean_value or clean_value in seen:
                continue
            seen.add(clean_value)
            normalized.append((clean_value, clean_label or clean_value))
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
        all_action = QAction("All", self._menu)
        all_action.triggered.connect(self._select_all)
        self._menu.addAction(all_action)
        none_action = QAction("None", self._menu)
        none_action.triggered.connect(self._select_none)
        self._menu.addAction(none_action)
        self._menu.addSeparator()
        for value, label in self._options:
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
            text = f"{self._label}: None"
        elif selected == total:
            text = f"{self._label}: All"
        elif selected <= 0:
            text = f"{self._label}: None"
        else:
            text = f"{self._label}: {selected} selected"
        self.setText(text)
        self.setToolTip(text)
