"""Acceptance checks for the policy-first Expect/Compose workflow.

These probes intentionally keep the store and endpoint boundaries explicit.  A
selection, resize, or editor keystroke may update the already-loaded model, but
must not rediscover files, open SQLite, or contact JS8Call.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QDateTimeEdit, QDoubleSpinBox, QSpinBox, QComboBox, QLineEdit, QPushButton

from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui import message_viewer_tab as compose_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.theme import _contrast_ratio, app_stylesheet, apply_app_theme, get_theme


class _Settings:
    def __init__(self, **values):
        self.values = values
        self.config_dir = Path(".")

    def get(self, key: str, default=None):
        return self.values.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.values[key] = value

    def save(self) -> None:
        pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _patch_spotter_reads(monkeypatch, *, entries=(), policies=(), statuses=()):
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "load_operator_traffic_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: list(entries))
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: list(policies))
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: list(statuses))


def _open_expect(monkeypatch, *, entries=(), policies=(), settings=None, compose=None):
    _patch_spotter_reads(monkeypatch, entries=entries, policies=policies)
    tab = FioSpotterTab(settings=settings or _Settings(), open_compose=compose)
    tab.tabs.setCurrentIndex(2)
    _app().processEvents()
    return tab


def test_expect_runtime_and_flamp_states_are_accessible_chips(monkeypatch):
    app = _app()
    tab = _open_expect(monkeypatch)
    try:
        assert tab.expect_runtime_chip.objectName() or tab.expect_runtime_chip.property("statusChip") is not None
        assert tab.expect_runtime_chip.accessibleName() == "Toggle Expect service"
        assert tab.expect_runtime_chip.toolTip()
        assert "Expect service:" in tab.expect_runtime_chip.text()
        assert tab.dynamic_flamp_chip.accessibleName() == "FLAMP Q status"
        assert tab.dynamic_flamp_chip.toolTip()
        assert "FLAMP Q:" in tab.dynamic_flamp_chip.text()
        # The long diagnostic labels remain available as descriptions, but do
        # not occupy the normal scan path.
        assert tab.expect_runtime_state.isHidden()
        assert tab.dynamic_flamp_state.isHidden()
        app.processEvents()
    finally:
        tab.close()
        tab.deleteLater()


def test_named_policy_is_presented_and_auto_reply_requires_enabled_policy(monkeypatch):
    policy = {"id": 7, "name": "Regional access", "enabled": 1}
    entry = {"id": 4, "expect_key": "F!701C", "response_text": "F!701C OK", "allow_policy_id": 7,
             "allow_policy_name": "Regional access", "auto_reply_state": "saved-only", "manual_send_available": True}
    tab = _open_expect(monkeypatch, entries=[entry], policies=[policy])
    try:
        assert tab.expect_policy_summary.accessibleName() == "Assigned access policy summary"
        assert tab.expect_policy_summary.text() == "Policy: required for Auto reply"
        tab.expect_entries_table.selectRow(0)
        _app().processEvents()
        assert tab.expect_policy.currentData() == 7
        assert tab.expect_policy_summary.text() == "Policy: Regional access"
        assert tab.expect_delivery_mode.currentText() == "Saved only"
        tab.expect_delivery_mode.setCurrentText("Auto reply on")
        assert "may reply automatically" in tab.expect_delivery_mode.toolTip()
        tab.expect_policy.setCurrentIndex(0)
        assert "requires a named" in tab.expect_delivery_mode.toolTip()
    finally:
        tab.close()
        tab.deleteLater()


def test_new_auto_reply_save_is_blocked_until_named_policy_is_selected(monkeypatch):
    tab = _open_expect(monkeypatch)
    saves = []
    monkeypatch.setattr(spotter_ui, "save_expect_entry", lambda payload: saves.append(payload))
    try:
        tab.expect_key.setText("INFO")
        tab.expect_reply.setText("INFO READY")
        tab.expect_delivery_mode.setCurrentText("Auto reply on")
        tab._save_entry()
        assert saves == []
        assert "policy" in tab.expect_runtime_state.text().casefold() or "policy" in tab.expect_maintenance_state.text().casefold()
    finally:
        tab.close()
        tab.deleteLater()


def test_legacy_inline_access_is_explicit_and_opening_never_activates_it(monkeypatch):
    entry = {"id": 5, "expect_key": "INFO", "response_text": "INFO READY", "enabled": 1,
             "auto_reply_enabled": 1, "unattended_auto_reply_enabled": 0,
             "allowed_callsigns": ["N0CALL"], "auto_reply_state": "saved-only", "manual_send_available": True}
    tab = _open_expect(monkeypatch, entries=[entry])
    try:
        tab.expect_entries_table.selectRow(0)
        _app().processEvents()
        assert tab.expect_delivery_mode.currentText() == "Saved only"
        assert not tab.expect_legacy_access.isHidden()
        assert not tab.expect_legacy_access.isChecked()
        assert tab.expect_calls.text() == "N0CALL"
    finally:
        tab.close()
        tab.deleteLater()


def test_legacy_access_can_be_copied_to_an_unsaved_named_policy_draft(monkeypatch):
    entry = {
        "id": 6,
        "expect_key": "INFO",
        "response_text": "INFO READY",
        "allowed_callsigns": ["N0CALL"],
        "allowed_groups": ["@MR08"],
        "blocked_callsigns": ["N0BAD"],
        "auto_reply_state": "saved-only",
        "manual_send_available": True,
    }
    writes = []
    tab = _open_expect(monkeypatch, entries=[entry])
    monkeypatch.setattr(spotter_ui, "save_expect_allow_policy", lambda payload: writes.append(payload))
    try:
        tab.expect_entries_table.selectRow(0)
        tab._draft_policy_from_legacy_access()
        assert tab.tabs.currentIndex() == 3
        assert tab.policy_name.text() == "INFO access"
        assert tab.policy_calls.text() == "N0CALL"
        assert tab.policy_groups.text() == "@MR08"
        assert tab.policy_blocked.text() == "N0BAD"
        assert writes == []
        assert "no access has changed" in tab.policy_status.text()
    finally:
        tab.close()
        tab.deleteLater()


def test_expect_options_are_progressively_disclosed_and_rate_limits_remain_readable(monkeypatch):
    tab = _open_expect(monkeypatch)
    try:
        assert tab.expect_options.isCheckable()
        assert not tab.expect_options.isChecked()
        assert tab.expect_options_body.isHidden()
        tab.expect_options.setChecked(True)
        assert not tab.expect_options_body.isHidden()
        assert tab.expect_cooldown.suffix() == " sec"
    finally:
        tab.close()
        tab.deleteLater()


@pytest.mark.parametrize("theme_name", ["light", "dark"])
def test_shared_theme_covers_enabled_and_disabled_numeric_date_time_combo_controls(theme_name):
    theme = get_theme(theme_name)
    css = app_stylesheet(theme)
    selectors = ("QSpinBox", "QDoubleSpinBox", "QDateEdit", "QTimeEdit", "QDateTimeEdit", "QComboBox")
    for selector in selectors:
        assert selector in css
        assert f"{selector}:disabled" in css
    assert f"color: {theme['text']}" in css
    assert f"color: {theme['text_muted']}" in css
    assert _contrast_ratio(theme["text"], theme["surface"]) >= 4.5
    assert _contrast_ratio(theme["text_muted"], theme["surface_alt"]) >= 4.5
    app = _app()
    apply_app_theme(app, theme)
    widgets = (QSpinBox(), QDoubleSpinBox(), QDateTimeEdit(), QComboBox())
    try:
        for widget in widgets:
            widget.addItem("Ready") if isinstance(widget, QComboBox) else None
            widget.setEnabled(True)
            assert widget.palette().color(widget.palette().ColorRole.Text).isValid()
            widget.setEnabled(False)
            assert widget.palette().color(widget.palette().ColorRole.Text).isValid()
    finally:
        for widget in widgets:
            widget.deleteLater()


def test_view_sends_exact_saved_mcf_intent_and_dynamic_q_stays_in_expect(monkeypatch):
    intents = []
    rows = [
        {"id": 12, "expect_key": "F!701C", "response_text": "F!701C 100 #IMG0", "manual_send_available": True},
        {"id": 13, "expect_key": "Q", "response_text": "generated", "manual_send_available": False},
    ]
    tab = _open_expect(monkeypatch, entries=rows, compose=lambda intent: intents.append(intent))
    try:
        tab.expect_entries_table.selectRow(0)
        tab._view_selected_expect()
        assert intents == [{"mode": "spotter", "transport": "spotter", "source": "fio_spotter_expect_view",
                            "source_label": "Expect", "expect_entry_id": 12, "expect_key": "F!701C",
                            "spotter_form_code": "F!701C", "expect_view": True}]
        assert "unchanged" in tab.expect_maintenance_state.text()
        tab.expect_entries_table.selectRow(1)
        tab._view_selected_expect()
        assert len(intents) == 1
        assert "remain available in the Expect rule editor" in tab.expect_maintenance_state.text()
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_saved_source_is_copy_only_and_new_form_starts_saved_only(monkeypatch):
    _app()
    class _Label:
        def __init__(self): self.value = ""
        def setText(self, value): self.value = value

    class _Edit:
        def __init__(self): self.value = ""
        def setText(self, value): self.value = value

    class _FakeCompose:
        compose_spotter_source_combo = QComboBox()
        compose_spotter_source_hint = _Label()
        compose_js8_target_edit = _Edit()
        _compose_spotter_expect_active_id = 0
        _compose_spotter_working_response = ""
        _compose_spotter_working_response_dirty = False
        _compose_spotter_source_loading = False
        _compose_field_rows = []

        def _clear_compose_spotter_working_response(self):
            self._compose_spotter_expect_active_id = 0
            self._compose_spotter_working_response = ""
            self._compose_spotter_working_response_dirty = False

        def _compose_spotter_expect_response_parts(self, response, key):
            return MessageViewerTab._compose_spotter_expect_response_parts(response, key)

        def _compose_spotter_saved_values(self, response, key, fields):
            return MessageViewerTab._compose_spotter_saved_values(response, key, fields)

        def _compose_set_widget_value(self, *_args):
            pass

        def _update_compose_preview(self):
            pass

        def _queue_compose_spotter_scroll_reset(self):
            pass

    fake = _FakeCompose()
    fake.compose_spotter_source_combo.addItem(
        "F!701C — F!701C 100 #IMG0",
        {"kind": "saved_expect", "entry": {"id": 8, "expect_key": "F!701C", "response_text": "F!701C 100 #IMG0"}},
    )
    MessageViewerTab._on_compose_spotter_source_changed(fake)
    assert fake._compose_spotter_working_response == "F!701C 100 #IMG0"
    assert fake._compose_spotter_working_response_dirty is False
    assert fake._compose_spotter_expect_active_id == 8
    assert "working copy" in fake.compose_spotter_source_hint.value


def test_compose_back_to_expect_handler_is_wired_for_view_workflow():
    # The button is created during Compose setup and must have a concrete
    # handler so returning from View can restore the Expect row selection.
    assert callable(getattr(MessageViewerTab, "_return_to_expect", None))


def test_expect_view_is_read_only_until_operator_selects_edit_working_copy():
    _app()

    class _FakeCompose:
        _compose_intent = {"expect_view": True}
        _compose_spotter_expect_active_id = 12
        _compose_expect_view_editing = False
        _compose_field_widgets = {"TEXT": QLineEdit(), "CHOICE": QComboBox()}
        compose_edit_expect_copy_btn = QPushButton("Edit working copy")

        def _expect_view_is_read_only(self):
            return MessageViewerTab._expect_view_is_read_only(self)

    fake = _FakeCompose()
    fake._compose_field_widgets["CHOICE"].addItem("Ready")
    try:
        MessageViewerTab._apply_compose_expect_view_state(fake)
        assert fake._compose_field_widgets["TEXT"].isReadOnly()
        assert not fake._compose_field_widgets["CHOICE"].isEnabled()
        assert not fake.compose_edit_expect_copy_btn.isHidden()

        fake._compose_expect_view_editing = True
        MessageViewerTab._apply_compose_expect_view_state(fake)
        assert not fake._compose_field_widgets["TEXT"].isReadOnly()
        assert fake._compose_field_widgets["CHOICE"].isEnabled()
        assert fake.compose_edit_expect_copy_btn.isHidden()
    finally:
        for widget in fake._compose_field_widgets.values():
            widget.deleteLater()
        fake.compose_edit_expect_copy_btn.deleteLater()


def test_compose_back_to_expect_restores_entry_identity_without_saving():
    calls = []

    class _Host:
        def open_fio_spotter_expect(self, **kwargs):
            calls.append(kwargs)

    class _FakeCompose:
        _compose_intent = {"expect_entry_id": 12, "expect_key": "F!701C"}

        def window(self):
            return _Host()

    MessageViewerTab._return_to_expect(_FakeCompose())
    assert calls == [{"entry_id": 12, "expect_key": "F!701C"}]


def test_selecting_saved_source_never_calls_save_to_expect(monkeypatch):
    _app()
    saved = []
    monkeypatch.setattr(compose_ui, "save_expect_entry", lambda payload: saved.append(payload))
    # The source-change callback is the Compose read/copy path.  It must remain
    # distinct from the explicit Configure/Save to Expect action.
    class _Combo(QComboBox):
        pass

    combo = _Combo()
    combo.addItem("F!701C", {"kind": "saved_expect", "entry": {"id": 8, "expect_key": "F!701C", "response_text": "F!701C OK"}})

    class _Fake:
        compose_spotter_source_combo = combo
        compose_spotter_source_hint = type("Label", (), {"setText": lambda self, _value: None})()
        compose_js8_target_edit = type("Edit", (), {"setText": lambda self, _value: None})()
        _compose_spotter_expect_active_id = 0
        _compose_spotter_working_response = ""
        _compose_spotter_working_response_dirty = False
        _compose_spotter_source_loading = False
        _compose_field_rows = []
        _compose_spotter_expect_response_parts = staticmethod(MessageViewerTab._compose_spotter_expect_response_parts)
        _compose_spotter_saved_values = staticmethod(MessageViewerTab._compose_spotter_saved_values)
        _compose_set_widget_value = lambda self, *_args: None
        _update_compose_preview = lambda self: None
        _queue_compose_spotter_scroll_reset = lambda self: None

    fake = _Fake()
    MessageViewerTab._on_compose_spotter_source_changed(fake)
    assert saved == []


def test_expect_form_route_and_compose_route_share_spotter_intent(monkeypatch):
    intents = []
    tab = _open_expect(monkeypatch, compose=lambda intent: intents.append(intent))
    try:
        tab._new_expect_from_mcf_form()
        assert intents[-1]["mode"] == "spotter"
        assert intents[-1]["expect_create"] is True
        assert "Saved only" in tab.expect_maintenance_state.text()
    finally:
        tab.close()
        tab.deleteLater()


def test_expect_selection_resize_and_typing_are_cache_only(monkeypatch):
    entries = [{"id": 9, "expect_key": "INFO", "response_text": "INFO READY", "manual_send_available": True}]
    tab = _open_expect(monkeypatch, entries=entries)
    calls = []
    try:
        for name in ("list_expect_entries", "list_expect_allow_policies", "list_expect_operator_access_catalog"):
            monkeypatch.setattr(spotter_ui, name, lambda **_kwargs: calls.append(name) or [])
        tab.expect_entries_table.selectRow(0)
        # Typing in a selected response field is a local draft operation; it
        # must not rebuild the bounded table or discard the selected identity.
        tab.expect_key.setText("INFO2")
        tab.resize(900, 560)
        _app().processEvents()
        assert calls == []
        assert tab._selected_expect_entry_id() == 9
    finally:
        tab.close()
        tab.deleteLater()
