"""Focused Qt acceptance probes for the Messages Compose workbench.

These tests intentionally exercise the real Compose widgets while suppressing
the Messages tab's unrelated ingest/timer startup.  They are a small harness,
not a replacement for the operator screenshot matrix.  In particular, the
tests protect the responsiveness contract: a text edit must not synchronously
rediscover forms, folders, targets, or endpoint state.
"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtTest import QTest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QDialog
from pathlib import Path

from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.main_window import MainWindow


ROOT = Path(__file__).resolve().parents[1]


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _tab(monkeypatch, tmp_path) -> MessageViewerTab:
    """Build the actual tab without source discovery or background startup."""

    for name in (
        "_setup_clock_timer",
        "_setup_timer",
        "_setup_js8_timer",
        "_setup_pending_timer",
        "_setup_bbs_auto_archive_timer",
        "_initial_refresh",
        "_refresh_compose_forms",
    ):
        monkeypatch.setattr(MessageViewerTab, name, lambda self: None)
    tab = MessageViewerTab()
    tab._db_path = lambda: tmp_path / "freqinout_nets.db"
    tab._save_settings = lambda: None
    return tab


def test_compose_keystroke_does_not_synchronously_discover_sources(monkeypatch, tmp_path) -> None:
    """Typing in the JS8 body must remain local and event-loop friendly."""

    _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab._compose_mode = "js8"
        discovered: list[str] = []

        # These are deliberately the slow paths used by the old all-in-one
        # preview refresh.  A future implementation may update cached snapshots
        # in the background, but the editor event itself must not call them.
        for name in (
            "_refresh_compose_radio_targets",
            "_refresh_compose_message_folder_options",
            "_install_compose_target_completers",
            "_refresh_compose_bbs_location_targets",
            "_refresh_compose_signing_keys",
            "_compose_peer_schedule_for_callsign",
            "_compose_path_evidence_for_target",
        ):
            monkeypatch.setattr(tab, name, lambda *_args, _helper=name, **_kwargs: discovered.append(_helper))

        # Qt stores the bound callback when the widget is built.  Rewire this
        # one editor signal after replacing the slow collaborators so the
        # interaction below observes the current instance methods.
        tab.compose_js8_plain_text_edit.textChanged.disconnect()
        tab.compose_js8_plain_text_edit.textChanged.connect(tab._update_compose_preview)

        # Use the real QTextEdit signal path.  This currently exposes the
        # synchronous-discovery defect and becomes a regression guard once the
        # preview pipeline is split into local + background work.
        tab.compose_js8_plain_text_edit.setPlainText("STATUS CHECK")
        _app().processEvents()
        assert discovered == []
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_layout_refresh_is_coalesced_on_next_event_loop_turn(monkeypatch, tmp_path) -> None:
    """Several geometry requests in one signal burst produce one layout pass."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        # Drain construction-time geometry requests before counting this
        # interaction burst.  A stale singleShot callback is harmless, but it
        # must not be mistaken for a pass caused by the two calls below.
        app.processEvents()
        tab._compose_layout_refresh_pending = False
        tab._compose_layout_signature = None
        calls: list[int] = []
        original = tab._refresh_compose_layout_geometry

        def counted() -> None:
            calls.append(1)
            original()

        monkeypatch.setattr(tab, "_refresh_compose_layout_geometry", counted)
        tab._refresh_compose_layout_geometry_if_needed(force=True)
        tab._refresh_compose_layout_geometry_if_needed(force=True)
        assert tab._compose_layout_refresh_pending is True
        QTest.qWait(20)
        app.processEvents()
        assert calls == [1]
        assert tab._compose_layout_refresh_pending is False
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_resize_feedback_converges_after_one_signature_pass(monkeypatch, tmp_path) -> None:
    """Same-size native resize feedback must not manufacture layout work."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        app.processEvents()
        tab._compose_layout_refresh_pending = False
        tab._compose_layout_signature = None
        calls: list[int] = []
        monkeypatch.setattr(tab, "_refresh_compose_layout_geometry", lambda: calls.append(1))

        tab._refresh_compose_layout_geometry_if_needed()
        QTest.qWait(20)
        app.processEvents()
        assert calls == [1]

        for _ in range(8):
            tab._update_messages_responsive_layout()
        QTest.qWait(20)
        app.processEvents()
        assert calls == [1]
        assert tab._compose_layout_refresh_pending is False
        assert tab._compose_layout_refresh_running is False
    finally:
        tab.close()
        tab.deleteLater()


def test_compose_update_batch_flushes_once_and_recovers_after_failure() -> None:
    """A navigation cascade derives one preview and never strands its guard."""

    class _Draft:
        _compose_update_depth = 0
        _compose_preview_update_pending = False
        _compose_spotter_source_loading = True

        def __init__(self) -> None:
            self.previews = 0

        def _update_compose_preview(self) -> None:
            self.previews += 1

        def _apply_prefill_compose_intent(self, _intent) -> None:
            self._compose_preview_update_pending = True

    draft = _Draft()
    MessageViewerTab.prefill_compose_intent(draft, {"mode": "spotter"})
    assert draft.previews == 1
    assert draft._compose_update_depth == 0
    assert draft._compose_preview_update_pending is False
    assert draft._compose_spotter_source_loading is False

    def fail(_intent) -> None:
        draft._compose_preview_update_pending = True
        raise ValueError("malformed saved form")

    draft._apply_prefill_compose_intent = fail
    draft._compose_spotter_source_loading = True
    with pytest.raises(ValueError, match="malformed saved form"):
        MessageViewerTab.prefill_compose_intent(draft, {"mode": "spotter"})
    assert draft.previews == 1
    assert draft._compose_update_depth == 0
    assert draft._compose_preview_update_pending is False
    assert draft._compose_spotter_source_loading is False


def test_failed_compose_navigation_reports_recoverable_status(caplog) -> None:
    statuses: list[tuple[str, str]] = []

    class _Tab:
        def show_compose_from_navigation(self) -> None:
            pass

        def prefill_compose_intent(self, _intent) -> None:
            raise ValueError("bad saved payload")

        def _set_compose_status(self, text: str, *, role: str = "info") -> None:
            statuses.append((text, role))

    class _Window:
        message_viewer_tab = _Tab()
        _messages_nav_context = "compose"
        _messages_nav_filter_context = {"compose_intent": {"mode": "spotter"}}

    MainWindow._apply_messages_nav_context(_Window())

    assert statuses
    assert statuses[-1][1] == "warning"
    assert "Compose remains available" in statuses[-1][0]
    assert "failed applying Messages navigation context" in caplog.text


def test_failed_saved_spotter_source_releases_guard_and_stays_recoverable() -> None:
    statuses: list[tuple[str, str]] = []

    class _Draft:
        compose_spotter_source_combo = None
        _compose_update_depth = 0
        _compose_preview_update_pending = False
        _compose_spotter_source_loading = True

        def _clear_compose_spotter_working_response(self) -> None:
            raise ValueError("malformed saved response")

        def _set_compose_status(self, text: str, *, role: str = "info") -> None:
            statuses.append((text, role))

        def _update_compose_preview(self) -> None:
            raise AssertionError("failed transaction must not derive a preview")

    draft = _Draft()
    assert MessageViewerTab._on_compose_spotter_source_changed(draft) is False
    assert draft._compose_update_depth == 0
    assert draft._compose_preview_update_pending is False
    assert draft._compose_spotter_source_loading is False
    assert statuses and statuses[-1][1] == "warning"


def test_full_compose_workbench_respects_available_screen_geometry(monkeypatch, tmp_path) -> None:
    """A full workbench must fit the desktop instead of forcing a larger window."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab.resize(1000, 700)
        tab.show()
        app.processEvents()
        tab._open_compose_workbench_dialog()
        app.processEvents()
        dialog = tab._compose_workbench_dialog
        assert isinstance(dialog, QDialog)
        available = dialog.screen().availableGeometry()
        assert dialog.minimumWidth() <= available.width()
        assert dialog.minimumHeight() <= available.height()
        assert dialog.width() <= available.width()
        assert dialog.height() <= available.height()
    finally:
        dialog = getattr(tab, "_compose_workbench_dialog", None)
        if isinstance(dialog, QDialog):
            dialog.close()
        tab.close()
        tab.deleteLater()


def test_workbench_round_trip_preserves_typed_js8_draft(monkeypatch, tmp_path) -> None:
    """Opening and closing the workbench must not clear the current draft."""

    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        # This isolates the round-trip from discovery so the test is about
        # ownership/reparenting, not the separate preview pipeline contract.
        monkeypatch.setattr(tab, "_update_compose_preview", lambda: None)
        tab.compose_js8_target_edit.setText("@GROUP")
        tab.compose_js8_plain_text_edit.setPlainText("Meet at 1900Z")
        expected_target = tab.compose_js8_target_edit.text()
        expected_body = tab.compose_js8_plain_text_edit.toPlainText()

        tab._open_compose_workbench_dialog()
        app.processEvents()
        dialog = tab._compose_workbench_dialog
        assert isinstance(dialog, QDialog)
        dialog.close()
        QTest.qWait(20)
        app.processEvents()

        assert tab._compose_workbench_dialog is None
        assert tab.compose_js8_target_edit.text() == expected_target
        assert tab.compose_js8_plain_text_edit.toPlainText() == expected_body
        assert tab.compose_body_splitter.parent() is tab.compose_page
    finally:
        dialog = getattr(tab, "_compose_workbench_dialog", None)
        if isinstance(dialog, QDialog):
            dialog.close()
        tab.close()
        tab.deleteLater()


def test_workbench_reuses_compact_mode_selector_for_every_compose_mode(
    monkeypatch, tmp_path
) -> None:
    """Reparenting cannot restore the list viewport's old blank height."""
    app = _app()
    tab = _tab(monkeypatch, tmp_path)
    try:
        tab.show()
        tab._set_messages_mode("Compose")
        tab._open_compose_workbench_dialog()
        app.processEvents()
        dialog = tab._compose_workbench_dialog
        assert isinstance(dialog, QDialog)
        assert tab.compose_type_box.window() is dialog
        for row in range(tab.compose_mode_selector.count()):
            tab.compose_mode_selector.setCurrentRow(row)
            tab._refresh_compose_layout_geometry_if_needed(force=True)
            app.processEvents()
            selector = tab.compose_mode_selector
            assert selector.textElideMode() == Qt.ElideNone
            assert selector.verticalScrollBar().maximum() == 0
            assert tab.compose_type_box.minimumHeight() == tab.compose_type_box.maximumHeight()
            assert tab.compose_type_box.height() <= selector.height() + 3 * selector.fontMetrics().lineSpacing()
            expected = (
                Qt.Horizontal
                if tab._compose_sidebar_enabled(
                    tab._compose_mode,
                    dialog.width(),
                    in_workbench=True,
                )
                else Qt.Vertical
            )
            assert tab.compose_body_splitter.orientation() == expected
            if row in (1, 2):
                assert (
                    tab.compose_js8_target_edit.geometry().bottom()
                    < tab.compose_js8_send_as_msg_chk.geometry().top()
                )
    finally:
        dialog = getattr(tab, "_compose_workbench_dialog", None)
        if isinstance(dialog, QDialog):
            dialog.close()
        tab.close()
        tab.deleteLater()


def test_compose_source_keeps_bbs_controls_nbems_only_and_preserves_signed_paths() -> None:
    """Static guardrails for the FLMsg/FLAmp + VarAC/BBS action contract."""

    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    stage_service = (ROOT / "freqinout/core/compose_stage_service.py").read_text(encoding="utf-8")
    update = source[source.index("    def _update_compose_preview"): source.index("    def _send_compose_js8_spotter")]
    assert "bbs_selected = bool(" in update
    assert "early_nbems_mode" in update
    assert "compose_bbs_location_row_widget.setVisible(bool(bbs_selected))" in update
    assert "sign_flamp_selected" in update
    assert "FLAmp signed file verified:" in stage_service
    assert "FLAmp signing failed; no unsigned FLAmp fallback was staged." in stage_service
    assert "upsert_bbs_artifact_path" in stage_service
    assert "set_bbs_artifact_locations" in stage_service
