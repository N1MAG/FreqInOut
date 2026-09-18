"""Focused GRS-7.5 qualification for the operator-facing Add Radio route.

The route is deliberately exercised through the real dialog with an isolated
settings directory.  Discovery is replaced only at the worker boundary so the
test proves that prepared-state publication, not host filesystem state, drives
the UI sequence.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QObject, Qt, Signal
from PySide6.QtTest import QTest
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QLabel,
    QPushButton,
    QScrollArea,
)


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _app() -> QApplication:
    global _QT_APP
    app = QApplication.instance()
    if isinstance(app, QApplication):
        _QT_APP = app
    elif _QT_APP is None:
        _QT_APP = QApplication([])
    return _QT_APP


def _open_add_radio_dialog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    inspect_dialog: Callable[[QDialog], None],
) -> None:
    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(
        SettingsTab, "_refresh_running_status", lambda self, force=False: None
    )
    monkeypatch.setattr(
        SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None
    )
    tab = SettingsTab()

    def fake_exec(dialog: QDialog) -> int:
        dialog.resize(900, 560)
        dialog.show()
        _app().processEvents()
        inspect_dialog(dialog)
        return dialog.result()

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    try:
        assert tab._open_device_profile_dialog(existing=None) is None
    finally:
        tab.deleteLater()
        _app().processEvents()


def _checkbox(dialog: QDialog, text: str) -> QCheckBox:
    checkbox = next(
        (
            item
            for item in dialog.findChildren(QCheckBox)
            if item.text().strip() == text
        ),
        None,
    )
    assert checkbox is not None, text
    return checkbox


def _enter_trimode_software_step(dialog: QDialog) -> None:
    setup = dialog.findChild(QComboBox, "guidedSetupType")
    assert setup is not None
    index = setup.findData("tri_mode")
    assert index >= 0
    setup.setCurrentIndex(index)
    _app().processEvents()
    for step_id in ("model", "software"):
        button = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
        assert button is not None and button.isEnabled(), step_id
        button.click()
        _app().processEvents()


def _selected_source(dialog: QDialog, family: str) -> QComboBox:
    combo = dialog.findChild(QComboBox, f"guidedSoftwareSource_{family}")
    assert combo is not None
    return combo


def _wait_until(predicate: Callable[[], bool], *, timeout_ms: int = 5000) -> bool:
    elapsed = 0
    while elapsed < timeout_ms:
        _app().processEvents()
        if predicate():
            return True
        QTest.qWait(20)
        elapsed += 20
    return bool(predicate())


def test_trimode_intent_precedes_preparation_and_preserves_builtin_contracts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The exact operator choices appear before any technical correction UI."""

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        role = dialog.findChild(QComboBox, "guidedRadioRole")
        assert role is not None and role.currentData() == "tx_rx"
        # The route is Transceiver -> Fast Light + JS8Call + FIO Spotter +
        # CommStat + VarAC.  TriMode supplies the radio-owned JS8/VarAC lane;
        # the station-facing services remain explicit additions.
        for text in ("FLRig", "FLDigi", "FLMsg", "FLAmp", "FIO Spotter", "CommStat"):
            checkbox = _checkbox(dialog, text)
            checkbox.setChecked(True)
        _app().processEvents()

        assert _checkbox(dialog, "JS8Call").isChecked()
        assert _checkbox(dialog, "VarAC").isChecked()

        assert _selected_source(dialog, "js8call").currentData() == "create"
        assert _selected_source(dialog, "fast_light").currentData() == "create"
        assert _selected_source(dialog, "fio_spotter").currentData() == "built_in"
        assert not _selected_source(dialog, "fio_spotter").isEnabled()
        assert _selected_source(dialog, "commstat").currentData() == "station_shared"
        assert not _selected_source(dialog, "commstat").isEnabled()

        arrangement = dialog.findChild(QComboBox, "guidedVaracArrangement")
        assert arrangement is not None
        assert arrangement.currentData() == "standalone"
        assert "VarAC arrangement" in arrangement.accessibleName()

        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        assert prepare is not None
        assert prepare.text() == "Prepare selected software automatically"
        assert prepare.isVisible() and prepare.isEnabled()
        for family in ("js8call", "fast_light", "fio_spotter", "commstat", "varac"):
            details = dialog.findChild(QPushButton, f"guidedSoftwareDetails_{family}")
            assert details is not None
            assert not details.isVisible() or not details.isEnabled()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_varac_requires_explicit_arrangement_before_background_prepare(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A conditional topology never becomes a background auto-choice."""

    import freqinout.gui.settings_tab as settings_tab_module

    monkeypatch.setattr(
        settings_tab_module,
        "recommend_varac_arrangement_from_snapshots",
        lambda *_args, **_kwargs: {
            "default_path": "",
            "recommended_path": "create_cluster",
            "standalone_candidates": ({"node_id": 9, "label": "FTDX-10 VarAC"},),
            "join_choices": (),
            "needs_attention": False,
            "existing_setup_summary": (
                "Existing setup: FTDX-10 VarAC is standalone. No VarAC cluster is configured."
            ),
            "create_choice_label": (
                "Create a cluster with FTDX-10 VarAC and TriMode — Recommended"
            ),
            "why": "Choose the recommended arrangement explicitly.",
        },
    )

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        arrangement = dialog.findChild(QComboBox, "guidedVaracArrangement")
        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
        assert arrangement is not None and prepare is not None and status is not None
        assert arrangement.currentData() == ""
        assert arrangement.currentText() == "Choose VarAC arrangement…"
        prepare.click()
        _app().processEvents()
        assert "choose the VarAC arrangement" in status.text()
        assert prepare.text() == "Prepare selected software automatically"
        assert prepare.isEnabled()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_prepared_route_enables_only_prepared_review_actions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """One bounded worker publication unlocks concise family review cards."""

    import freqinout.gui.settings_tab as settings_tab_module

    class _ImmediateThread(QObject):
        """Synchronous worker shell: exercise publication without host I/O."""

        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    def publish_empty_bounded_snapshot(worker: object) -> None:
        request = getattr(worker, "request")
        worker.finished.emit(
            {
                "guided_discovery_request": request,
                "install_candidates": (),
                "fast_results": {},
                "js8_results": {},
                "varac_results": {},
                "js8_file_profiles": (),
            }
        )

    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "run",
        publish_empty_bounded_snapshot,
    )
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda _worker, _thread: None,
    )

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        for text in ("FLRig", "FLDigi", "FLMsg", "FLAmp", "FIO Spotter", "CommStat"):
            _checkbox(dialog, text).setChecked(True)
        _app().processEvents()
        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
        assert prepare is not None and status is not None
        prepare.click()
        assert _wait_until(
            lambda: prepare.isEnabled()
            and prepare.text() == "Prepare selected software automatically"
        ), status.text()
        assert status.text().startswith(("Ready —", "Needs attention —"))
        for family in ("js8call", "fast_light", "fio_spotter", "commstat", "varac"):
            state = dialog.findChild(QLabel, f"guidedSoftwarePreparedState_{family}")
            details = dialog.findChild(QPushButton, f"guidedSoftwareDetails_{family}")
            assert state is not None and details is not None
            assert state.isVisible()
            assert details.isVisible() and details.isEnabled()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


@pytest.mark.parametrize("size", [(1920, 1080), (1000, 700), (900, 560)])
def test_add_radio_fixed_navigation_has_one_body_scroll_owner_and_no_horizontal_page_scroll(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    size: tuple[int, int],
) -> None:
    """The live Add Radio shell follows the same geometry contract as the assistant."""

    def inspect(dialog: QDialog) -> None:
        dialog.resize(*size)
        _enter_trimode_software_step(dialog)
        _app().processEvents()
        scrolls = dialog.findChildren(QScrollArea)
        assert len(scrolls) == 1
        body = scrolls[0]
        assert body.objectName() == "guidedRadioSetupBodyScroll"
        assert body.verticalScrollBarPolicy() != Qt.ScrollBarAlwaysOff
        assert body.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert body.horizontalScrollBar().maximum() == 0
        footer = dialog.findChild(QDialogButtonBox, "guidedRadioSetupActionFooter")
        assert footer is not None and footer.isVisible()
        for button in (
            dialog.findChild(QPushButton, "guidedWizardBack"),
            dialog.findChild(QPushButton, "guidedWizardNext"),
            footer.button(QDialogButtonBox.Cancel),
        ):
            assert button is not None and button.isVisible()
            top_left = button.mapTo(dialog, button.rect().topLeft())
            bottom_right = button.mapTo(dialog, button.rect().bottomRight())
            assert top_left.x() >= 0 and top_left.y() >= 0
            assert bottom_right.x() <= dialog.width() + 1
            assert bottom_right.y() <= dialog.height() + 1
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)
