"""Canonical Software Administration identity controls remain review-only."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtWidgets import QApplication, QLineEdit

from freqinout.gui.software_administration_editor import SoftwareTaskEditor


_APP: QApplication | None = QApplication.instance() or QApplication([])


def test_canonical_software_identity_disables_partial_edit_save_and_discovery() -> None:
    editor = SoftwareTaskEditor()
    try:
        editor.set_context(
            family_key="js8call",
            family_title="JS8Call",
            task_key="application_profile",
            radio_id=71,
            radio_name="FT-710",
            state={"path_js8call": "/usr/bin/js8call", "js8_profile_path": "/radio/FT-710"},
        )
        editor.show()
        _APP.processEvents()
        assert editor.form_widget.isEnabled()
        assert editor.save_button.isVisible()
        assert editor.discover_button.isVisible()
        assert any(isinstance(widget, QLineEdit) for widget in editor._field_widgets.values())

        editor.set_canonical_identity_managed(True, identity_key="js8call:ft-710")
        _APP.processEvents()

        assert not editor.form_widget.isEnabled()
        assert all(not widget.isEnabled() for widget in editor._field_widgets.values())
        assert editor.save_button.isHidden()
        assert editor.discover_button.isHidden()
        assert editor.dirty_label.isHidden()
        assert editor.identity_notice_label.isVisible()
        assert "FT-710" in editor.identity_notice_label.text()
        assert "js8call:ft-710" not in editor.identity_notice_label.text()
        assert "Add software instance" in editor.identity_notice_label.text()
    finally:
        editor.deleteLater()
        _APP.processEvents()
