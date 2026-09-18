"""GRS-7.4 real-widget acceptance coverage for the shared instance assistant.

These tests deliberately exercise the widget rather than inspecting source
strings.  Add Radio and Software Administration both use this assistant, so
the same disclosure, safety, scrolling, and responsive guarantees are tested
for every supported software family.
"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtTest import QTest
from PySide6.QtWidgets import (
    QApplication,
    QAbstractButton,
    QGroupBox,
    QLabel,
    QScrollArea,
    QWidget,
)

from freqinout.gui.software_instance_assistant import (
    SoftwareInstanceAssistant,
    VarACNativePresentation,
)
from freqinout.gui.theme import apply_app_theme, get_theme


def _app() -> QApplication:
    app = QApplication.instance()
    return app if isinstance(app, QApplication) else QApplication([])


def _assistant(family: str) -> SoftwareInstanceAssistant:
    return SoftwareInstanceAssistant(
        family,
        radios=({"id": 7, "name": "New Radio"},),
        selected_radio_id=7,
        unsaved_owner_key="guided-radio-draft-7",
        unsaved_radio_label="New Radio",
    )


def _details_buttons(assistant: QWidget) -> list[QAbstractButton]:
    buttons: list[QAbstractButton] = []
    for button in assistant.findChildren(QAbstractButton):
        text = " ".join(
            value
            for value in (
                button.text(),
                button.accessibleName(),
                button.accessibleDescription(),
                button.objectName(),
            )
            if value
        ).lower()
        if "detail" in text and ("show" in text or "technical" in text):
            buttons.append(button)
    return buttons


def _detail_panels(assistant: QWidget) -> list[QWidget]:
    """Return named technical-detail panels, excluding their disclosure button."""

    panels: list[QWidget] = []
    for widget in assistant.findChildren(QWidget):
        name = widget.objectName().lower()
        if "detail" not in name and "technical" not in name:
            continue
        if isinstance(widget, QAbstractButton):
            continue
        panels.append(widget)
    return panels


def _visible_semantics(assistant: QWidget) -> str:
    values: list[str] = []
    for widget in assistant.findChildren(QWidget):
        if not widget.isVisible():
            continue
        if isinstance(widget, QLabel):
            value = widget.text()
        elif isinstance(widget, QGroupBox):
            value = widget.title()
        elif isinstance(widget, QAbstractButton):
            value = widget.text()
        else:
            value = ""
        values.extend(
            text
            for text in (value, widget.accessibleName(), widget.accessibleDescription())
            if text
        )
    return " ".join(values).lower()


def _restore_theme(app: QApplication, prior_font: QFont) -> None:
    app.setFont(prior_font)
    apply_app_theme(app, get_theme("light"), ui_text_scale=1.0)


@pytest.mark.parametrize("family, family_words", [
    ("js8call", ("js8", "js8call")),
    ("fast_light", ("fast light", "fast_light", "fldigi")),
    ("varac", ("varac",)),
])
def test_details_are_family_scoped_accessible_and_collapsed_by_default(family, family_words) -> None:
    """Every family has one keyboard disclosure with explicit scope."""

    app = _app()
    assistant = _assistant(family)
    try:
        assistant.resize(1000, 700)
        assistant.show()
        app.processEvents()
        details = _details_buttons(assistant)
        assert details, f"{family} must expose a technical-details disclosure"
        for button in details:
            accessible = " ".join(
                (button.text(), button.accessibleName(), button.accessibleDescription())
            ).lower()
            assert any(word in accessible for word in family_words)
            assert "new radio" in accessible or "radio" in accessible
            assert button.focusPolicy() != Qt.NoFocus
            assert button.accessibleName()
            assert button.accessibleDescription()
            assert button.isCheckable()
            assert not button.isChecked(), "technical details must start collapsed"

        panels = _detail_panels(assistant)
        assert panels, "the collapsed disclosure must have a real detail panel"
        assert all(not panel.isVisible() for panel in panels)

        button = details[0]
        button.setFocus()
        QTest.keyClick(button, Qt.Key_Space)
        app.processEvents()
        assert button.isChecked()
        assert any(panel.isVisible() for panel in panels)
    finally:
        assistant.close()
        assistant.deleteLater()
        app.processEvents()


def test_safety_why_confirmation_and_unsaved_impact_stay_visible_on_every_step() -> None:
    """Persistent operator context cannot disappear when the step changes."""

    app = _app()
    assistant = _assistant("js8call")
    try:
        assistant.resize(1000, 700)
        assistant.show()
        app.processEvents()
        for step in range(len(assistant.STEP_TITLES)):
            assistant._step = step
            assistant._refresh()
            app.processEvents()
            semantics = _visible_semantics(assistant)
            assert any(token in semantics for token in ("safety", "safe", "guard")), step
            assert any(token in semantics for token in ("why", "rationale", "impact")), step
            assert any(token in semantics for token in ("confirm", "review before")), step
            assert any(token in semantics for token in ("unsaved", "nothing is saved", "pending")), step
    finally:
        assistant.close()
        assistant.deleteLater()
        app.processEvents()


@pytest.mark.parametrize("size", [(1920, 1080), (1000, 700), (900, 560)])
@pytest.mark.parametrize("scale", [1.0, 1.25])
@pytest.mark.parametrize("theme_name", ["light", "dark"])
def test_assistant_has_one_vertical_body_scroll_owner_and_reachable_footer(
    size: tuple[int, int], scale: float, theme_name: str
) -> None:
    """Responsive matrix: no nested page scroll owners or clipped footer."""

    app = _app()
    prior_font = QFont(app.font())
    apply_app_theme(app, get_theme(theme_name), ui_text_scale=scale)
    assistant = _assistant("fast_light")
    try:
        assistant.resize(*size)
        assistant.show()
        app.processEvents()

        scrolls = assistant.findChildren(QScrollArea)
        vertical_owners = [
            scroll
            for scroll in scrolls
            if scroll.verticalScrollBarPolicy() != Qt.ScrollBarAlwaysOff
        ]
        assert len(vertical_owners) == 1
        owner = vertical_owners[0]
        assert owner.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert owner.horizontalScrollBar().maximum() == 0

        for name in ("cancel_button", "back_button", "next_button"):
            button = getattr(assistant, name)
            assert button.isVisible(), f"{name} is not reachable at {size}/{scale}/{theme_name}"
            top_left = button.mapTo(assistant, button.rect().topLeft())
            bottom_right = button.mapTo(assistant, button.rect().bottomRight())
            assert top_left.y() >= 0
            assert bottom_right.x() <= assistant.width() + 1
            assert bottom_right.y() <= assistant.height() + 1

        # Only the assistant's own layout children are checked here.  Content
        # inside the body is allowed to extend in the one bounded scroll owner.
        direct_widgets = [
            child
            for child in assistant.findChildren(QWidget)
            if child.parentWidget() is assistant and child.isVisible()
        ]
        for child in direct_widgets:
            rect = child.geometry()
            assert rect.left() >= 0
            assert rect.right() <= assistant.width() + 1
    finally:
        assistant.close()
        assistant.deleteLater()
        app.processEvents()
        _restore_theme(app, prior_font)


@pytest.mark.parametrize("size", [(1920, 1080), (1000, 700), (900, 560)])
@pytest.mark.parametrize("scale", [1.0, 1.25])
@pytest.mark.parametrize("theme_name", ["light", "dark"])
def test_varac_native_card_keeps_one_scroll_owner_and_reachable_footer(
    size: tuple[int, int], scale: float, theme_name: str
) -> None:
    """The native plan card stays compact at every required VNC-4 geometry."""

    app = _app()
    prior_font = QFont(app.font())
    apply_app_theme(app, get_theme(theme_name), ui_text_scale=scale)
    assistant = SoftwareInstanceAssistant(
        "varac",
        unsaved_owner_key="guided-varac-native",
        unsaved_radio_label="New Radio",
        initial_draft={"cluster_path": "create_cluster", "cluster_instance_number": 2},
        varac_native_presentation=VarACNativePresentation(
            state="ready",
            arrangement="create_cluster",
            affected_radios=("Existing Radio", "New Radio"),
            shared_database_summary="Proposed shared database",
            member_numbers_summary="Existing 1 · New 2",
            ptt_lock_summary="On",
            email_gateway_sender_summary="New Radio",
            writer_version="13.2.7",
            writer_qualified=True,
        ),
    )
    try:
        assistant.resize(*size)
        assistant.show()
        app.processEvents()
        assert assistant.varac_native_group.isVisible()
        assert "Email gateway sender" in assistant.varac_native_summary_label.text()
        assert "gateway handler" not in assistant.varac_native_summary_label.text().lower()
        assert assistant.body_scroll.horizontalScrollBar().maximum() == 0
        assert assistant.body_scroll.verticalScrollBarPolicy() != Qt.ScrollBarAlwaysOff
        for button in (assistant.cancel_button, assistant.back_button, assistant.next_button):
            bottom_right = button.mapTo(assistant, button.rect().bottomRight())
            assert bottom_right.x() <= assistant.width() + 1
            assert bottom_right.y() <= assistant.height() + 1
    finally:
        assistant.close()
        assistant.deleteLater()
        app.processEvents()
        _restore_theme(app, prior_font)


def test_details_state_survives_resize_theme_refresh_and_async_publication() -> None:
    """A refresh must not silently collapse an operator's open details."""

    app = _app()
    prior_font = QFont(app.font())
    assistant = _assistant("js8call")
    try:
        assistant.show()
        app.processEvents()
        details = _details_buttons(assistant)
        panels = _detail_panels(assistant)
        assert details and panels
        button = details[0]
        button.click()
        app.processEvents()
        assert button.isChecked()
        assert any(panel.isVisible() for panel in panels)

        assistant.resize(900, 560)
        apply_app_theme(app, get_theme("dark"), ui_text_scale=1.25)
        assistant._refresh()
        app.processEvents()
        assert button.isChecked()
        assert any(panel.isVisible() for panel in panels)

        # Publication is intentionally delivered through the assistant's
        # existing cache-only result API, as it is by Add Radio discovery.
        assistant.set_discovery_results(
            ({
                "id": 19,
                "system_key": "new-radio-js8",
                "name": "New Radio JS8",
                "host": "127.0.0.1",
                "port": 2443,
                "candidate_classification": "usable_existing",
                "candidate_usable": True,
            },)
        )
        app.processEvents()
        assert button.isChecked()
        assert any(panel.isVisible() for panel in panels)
    finally:
        assistant.close()
        assistant.deleteLater()
        app.processEvents()
        _restore_theme(app, prior_font)
