"""Focused GRS-7.5 qualification for the operator-facing Add Radio route.

The route is deliberately exercised through the real dialog with an isolated
settings directory.  Discovery is replaced only at the worker boundary so the
test proves that prepared-state publication, not host filesystem state, drives
the UI sequence.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
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
    QGroupBox,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QWidget,
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
) -> object:
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
    inspected_dialog: QDialog | None = None

    def fake_exec(dialog: QDialog) -> int:
        nonlocal inspected_dialog
        if inspected_dialog is not None:
            # Nested correction surfaces are deliberately cancelled unless a
            # test explicitly needs to inspect them.
            dialog.done(QDialog.Rejected)
            return QDialog.Rejected
        inspected_dialog = dialog
        dialog.resize(900, 560)
        dialog.show()
        _app().processEvents()
        inspect_dialog(dialog)
        return dialog.result()

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    try:
        return tab._open_device_profile_dialog(existing=None)
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
        assert not prepare.isVisible()
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
        assert not prepare.isVisible()
        _app().processEvents()
        assert "choose the VarAC arrangement" in status.text()
        assert prepare.text() == "Prepare selected software automatically"
        assert prepare.isEnabled()
        card = dialog.findChild(QGroupBox, "guidedSoftwareResponsibility_varac")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert card is not None
        assert card.property("guidedPreparationState") == "needs_choice"
        assert card.property("guidedCardExpanded") is True
        assert next_button is not None and not next_button.isEnabled()
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

    requests: list[object] = []

    def publish_empty_bounded_snapshot(worker: object) -> None:
        request = getattr(worker, "request")
        requests.append(request)
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
    from freqinout.gui.settings_tab import SettingsTab
    monkeypatch.setattr(
        SettingsTab,
        "_start_varac_native_job",
        lambda self, _worker, *, on_finished, on_failed: on_finished(()),
    )

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        # Coalesce this burst of capability edits into one final request.
        # VarAC's separate native worker is outside this discovery regression.
        _checkbox(dialog, "VarAC").setChecked(False)
        for text in ("FLRig", "FLDigi", "FLMsg", "FLAmp", "FIO Spotter", "CommStat"):
            _checkbox(dialog, text).setChecked(True)
        _app().processEvents()
        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
        assert prepare is not None and status is not None
        assert _wait_until(
            lambda: bool(requests)
            and status.text().startswith(("Ready —", "Needs attention —"))
        ), status.text()
        assert not prepare.isVisible()
        assert len(requests) == 1
        requested_families = {str(getattr(family, "value", family)) for family in requests[0].families}
        assert {"fast_light", "js8call"} <= requested_families
        assert status.text().startswith(("Ready —", "Needs attention —"))
        for family in ("js8call", "fast_light", "fio_spotter", "commstat", "varac"):
            state = dialog.findChild(QLabel, f"guidedSoftwarePreparedState_{family}")
            details = dialog.findChild(QPushButton, f"guidedSoftwareDetails_{family}")
            card = dialog.findChild(QGroupBox, f"guidedSoftwareResponsibility_{family}")
            assert state is not None and details is not None and card is not None
            selected = family != "varac"
            assert card.isVisible() is selected
            if selected:
                assert state.isVisible()
                assert details.isVisible() and details.isEnabled()
                assert card.property("guidedPreparationState") in {"ready", "launch_pending", "warning"}
                assert card.property("guidedCardExpanded") is False
        for family in ("js8call", "fast_light"):
            state = dialog.findChild(QLabel, f"guidedSoftwarePreparedState_{family}")
            details = dialog.findChild(QPushButton, f"guidedSoftwareDetails_{family}")
            assert state is not None and state.text().startswith("Ready to save · launch setup pending —")
            assert details is not None and details.text() == "Review Details (optional)…"
        assert "fast_light" in getattr(dialog, "_guided_software_instance_drafts", {})
        before_checks = {
            text: _checkbox(dialog, text).isChecked()
            for text in ("JS8Call", "FIO Spotter", "CommStat", "FLRig", "FLDigi", "FLMsg", "FLAmp", "VarAC")
        }
        before_drafts = dict(getattr(dialog, "_guided_software_instance_drafts", {}))
        details = dialog.findChild(QPushButton, "guidedSoftwareDetails_js8call")
        assert details is not None
        details.click()
        _app().processEvents()
        assert {
            text: _checkbox(dialog, text).isChecked() for text in before_checks
        } == before_checks
        assert getattr(dialog, "_guided_software_instance_drafts", {}) == before_drafts
        assert len(requests) == 1
        fast_source = _selected_source(dialog, "fast_light")
        fast_source.setCurrentIndex(fast_source.findData("existing"))
        _app().processEvents()
        assert "fast_light" not in getattr(dialog, "_guided_software_instance_drafts", {})
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_explicit_detected_js8_choice_reprepares_plan_and_unblocks_next(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The requested app choice is authoritative and refreshes launch readiness."""

    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.core.config_autodiscovery import AppCandidate

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    chosen_path = "/opt/JS8Call-2.2.0/js8call"
    candidates = (
        AppCandidate(
            app_id="js8call",
            display_name="JS8Call stock",
            path=chosen_path,
            source="known_path",
            confidence="high",
            exists=True,
            executable=True,
            target_type="file",
        ),
        AppCandidate(
            app_id="js8call",
            display_name="JS8Call Subspace",
            path="/opt/js8call-subspace-4.1.0.478/js8call-subspace",
            source="known_path",
            confidence="high",
            exists=True,
            executable=True,
            target_type="file",
        ),
    )
    requests: list[object] = []

    def publish_candidates(worker: object) -> None:
        request = getattr(worker, "request")
        requests.append(request)
        worker.finished.emit(
            {
                "guided_discovery_request": request,
                "install_candidates": candidates,
                "fast_results": {},
                "js8_results": {},
                "varac_results": {},
                "js8_file_profiles": (),
            }
        )

    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "run", publish_candidates)
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda *_args: None,
    )

    def inspect(dialog: QDialog) -> None:
        setup = dialog.findChild(QComboBox, "guidedSetupType")
        assert setup is not None
        setup.setCurrentIndex(setup.findData("js8_only"))
        _app().processEvents()
        for step_id in ("model", "software"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled()
            step.click()
            _app().processEvents()

        choice = dialog.findChild(QComboBox, "guidedAutoAppChoice_js8call")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert choice is not None and next_button is not None
        assert _wait_until(lambda: len(requests) == 1 and choice.count() == 3)
        assert choice.currentIndex() == 0
        assert not next_button.isEnabled()

        selected_index = choice.findData(chosen_path)
        assert selected_index > 0
        choice.setCurrentIndex(selected_index)
        assert _wait_until(lambda: len(requests) == 2)
        assert _wait_until(lambda: next_button.isEnabled())
        assert choice.currentData() == chosen_path
        drafts = getattr(dialog, "_guided_software_instance_drafts", {})
        assert drafts["js8call"]["application_path"] == chosen_path
        assert len(requests) == 2
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_preparing_next_stays_blocked_and_stale_worker_result_is_discarded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Only the current software context can publish a held discovery result."""

    import freqinout.gui.settings_tab as settings_tab_module

    class _HeldThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    workers: list[object] = []

    def hold_worker(worker: object) -> None:
        workers.append(worker)

    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "run", hold_worker)
    monkeypatch.setattr(settings_tab_module, "QThread", _HeldThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda *_args: None,
    )

    def result_for(worker: object, marker: str) -> dict[str, object]:
        request = getattr(worker, "request")
        return {
            "guided_discovery_request": request,
            "install_candidates": (),
            "fast_results": {},
            "js8_results": {},
            "varac_results": {},
            "js8_file_profiles": (),
            "test_result_marker": marker,
        }

    def inspect(dialog: QDialog) -> None:
        setup = dialog.findChild(QComboBox, "guidedSetupType")
        assert setup is not None
        custom = setup.findData("custom")
        assert custom >= 0
        setup.setCurrentIndex(custom)
        _app().processEvents()
        for step_id in ("model", "software"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled()
            step.click()
            _app().processEvents()

        js8 = _checkbox(dialog, "JS8Call")
        js8.setChecked(True)
        assert _wait_until(lambda: len(workers) == 1), "initial held discovery did not start"
        first = workers[0]
        first_request = getattr(first, "request")
        card = dialog.findChild(QGroupBox, "guidedSoftwareResponsibility_js8call")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert card is not None and card.property("guidedPreparationState") == "preparing"
        assert next_button is not None and not next_button.isEnabled()

        # Changing the source invalidates the held request but keeps the family
        # selected, so the replacement request remains observable.
        source = _selected_source(dialog, "js8call")
        existing_index = source.findData("existing")
        assert existing_index >= 0
        source.setCurrentIndex(existing_index)
        _app().processEvents()
        assert js8.isChecked()

        first.finished.emit(result_for(first, "stale"))
        assert _wait_until(lambda: len(workers) == 2), "replacement discovery did not start"
        second = workers[1]
        second_request = getattr(second, "request")
        assert second_request != first_request
        assert getattr(second_request, "generation") > getattr(first_request, "generation")
        assert js8.isChecked()
        assert card.property("guidedPreparationState") == "preparing"
        status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
        assert status is not None and not status.text().startswith(("Ready —", "Needs attention —"))
        assert getattr(dialog, "_guided_software_instance_drafts", {}) == {}

        second.finished.emit(result_for(second, "current"))
        assert _wait_until(lambda: status.text().startswith(("Ready —", "Needs attention —")))
        assert js8.isChecked()
        assert card.property("guidedPreparationState") != "preparing"
        assert card.property("guidedPreparationState") == "needs_choice"
        assert getattr(dialog, "_guided_software_instance_drafts", {}) == {}
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_first_unresolved_managed_card_is_revealed_once_per_prepared_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A blocked managed recipe receives one scroll reveal, not one per refresh."""

    import freqinout.gui.settings_tab as settings_tab_module

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    def publish_empty_snapshot(worker: object) -> None:
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

    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "run", publish_empty_snapshot)
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        settings_tab_module,
        "resolve_guided_launch_recipe",
        lambda *_args, **_kwargs: SimpleNamespace(
            qualified=False,
            recovery_action="Choose a distinct JS8Call profile path.",
        ),
    )
    monkeypatch.setattr(
        settings_tab_module,
        "recipe_draft_updates",
        lambda _resolution: {
            "launch_recipe_status": "blocked_for_safety",
            "launch_recipe": {"status": "blocked_for_safety"},
        },
    )

    reveals: list[QWidget] = []
    original_ensure_visible = QScrollArea.ensureWidgetVisible

    def record_reveal(
        scroll: QScrollArea,
        target: QWidget,
        x_margin: int = 50,
        y_margin: int = 50,
    ) -> None:
        reveals.append(target)
        original_ensure_visible(scroll, target, x_margin, y_margin)

    monkeypatch.setattr(QScrollArea, "ensureWidgetVisible", record_reveal)

    def inspect(dialog: QDialog) -> None:
        setup = dialog.findChild(QComboBox, "guidedSetupType")
        assert setup is not None
        custom = setup.findData("custom")
        assert custom >= 0
        setup.setCurrentIndex(custom)
        _app().processEvents()
        for step_id in ("model", "software"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled()
            step.click()
            _app().processEvents()
        _checkbox(dialog, "JS8Call").setChecked(True)

        card = dialog.findChild(QGroupBox, "guidedSoftwareResponsibility_js8call")
        status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
        assert card is not None and status is not None
        assert _wait_until(lambda: status.text().startswith("Needs attention —"))
        assert card.property("guidedPreparationState") == "blocked"
        assert card.property("guidedCardExpanded") is True
        _app().processEvents()
        assert reveals == [card]

        # Ordinary event processing/layout refreshes must not steal the scroll
        # position by issuing the same reveal repeatedly for one prepared plan.
        dialog.resize(880, 550)
        _app().processEvents()
        assert reveals == [card]
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_deselecting_varac_purges_its_dialog_draft_and_review_projection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """TriMode removal cannot leak a previous VarAC bundle into Review."""

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        setattr(
            dialog,
            "_guided_software_instance_drafts",
            {
                "varac": {
                    "family_key": "varac",
                    "application_path": "/managed/old/VarAC.exe",
                    "configuration_path": "/managed/old/VarAC.ini",
                    "storage_path": "/managed/old/old.db",
                    "launch_command": "old-varac",
                }
            },
        )
        varac = _checkbox(dialog, "VarAC")
        assert varac.isChecked()
        varac.setChecked(False)
        _app().processEvents()
        assert "varac" not in getattr(dialog, "_guided_software_instance_drafts", {})
        # The current family selection is also what Review reads; old paths
        # and launch data must not remain merely because a nested editor was
        # previously opened.
        for step_id in ("connection", "guard", "schedule", "review"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled(), step_id
            step.click()
            _app().processEvents()
        review = dialog.findChild(QLabel, "guidedSaveReview")
        assert review is not None
        assert "VarAC" not in review.text()
        assert "old-varac" not in review.text()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)

    # A new dialog owns a new session and begins without the first dialog's
    # retained VarAC draft.
    def inspect_new(dialog: QDialog) -> None:
        assert "varac" not in getattr(dialog, "_guided_software_instance_drafts", {})
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect_new)


def test_qualified_varac_projection_keeps_unrelated_generic_plan_blockers() -> None:
    """Filtering legacy VarAC review text must not waive JS8/Fast Light safety."""

    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    projection = source[source.index("def _current_guided_app_config_plan") : source.index("def _apply_guided_app_configuration")]
    assert 'action.app_id != "varac"' in projection
    assert "blocked=bool(plan.blocked)" in projection


def test_parent_varac_bundle_projects_connections_and_review_before_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The Settings > Radios > Add Radio parent renders the worker bundle itself."""

    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.core.varac_native_preparation import (
        VarACNativePreparationResult,
        native_draft_fingerprint,
    )
    from freqinout.gui.settings_tab import SettingsTab

    class _ImmediateThread(QObject):
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

    def immediate_native_start(self: object, worker: object, *, on_finished: Callable[[object], None], on_failed: Callable[[str], None]) -> None:
        if not isinstance(worker, settings_tab_module._VarACNativePrepareWorker):
            on_finished(())
            return
        draft = dict(worker.draft)
        presentation = {
            "state": "ready",
            "writer_qualified": True,
            "writer_version": "13.2.7",
            "generation": worker.generation,
            "application_path": "/wine/drive_c/VarAC/VarAC.exe",
            "configuration_path": "/wine/drive_c/VarAC/VarAC-Field.ini",
            "storage_path": "/cluster/VarAC.db",
            "secondary_storage_path": "/managed/field/incoming",
            "outbox_path": "/managed/field/outbox",
            "cluster_bbs_path": "/cluster/BBS",
            "cluster_bbs_archive_path": "/cluster/BBS-Archive",
            "working_directory": "/wine/drive_c/VarAC",
            "vara_runtime_path": "/managed/field/VARA",
            "vara_ini_path": "/managed/field/VARA/VARA.ini",
            "launch_command": 'wine /wine/drive_c/VarAC/VarAC.exe "C:\\VarAC\\VarAC-Field.ini"',
            "launch_argv": ("wine", "/wine/drive_c/VarAC/VarAC.exe", "C:\\VarAC\\VarAC-Field.ini"),
            "launch_environment": {"WINEPREFIX": "/wine"},
            "ports_summary": "command 8304 · KISS 8306",
        }
        on_finished(
            VarACNativePreparationResult(
                state="ready",
                presentation=presentation,
                draft_fingerprint=native_draft_fingerprint(draft),
                generation=worker.generation,
                plan=SimpleNamespace(plan_fingerprint="prepared-varac-bundle"),
            )
        )

    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "run", publish_empty_bounded_snapshot)
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "moveToThread", lambda _worker, _thread: None)
    monkeypatch.setattr(SettingsTab, "_start_varac_native_job", immediate_native_start)

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        arrangement = dialog.findChild(QComboBox, "guidedVaracArrangement")
        assert arrangement is not None
        arrangement.setCurrentIndex(arrangement.findData("create_cluster"))
        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        assert prepare is not None
        prepare.click()
        assert _wait_until(lambda: prepare.isEnabled())
        assert dialog.findChild(QLineEdit, "guidedVaracExecutable").text() == "/wine/drive_c/VarAC/VarAC.exe"  # type: ignore[union-attr]
        assert dialog.findChild(QLineEdit, "guidedVaracIni").text() == "/wine/drive_c/VarAC/VarAC-Field.ini"  # type: ignore[union-attr]
        assert dialog.findChild(QLineEdit, "guidedVaracDatabase").text() == "/cluster/VarAC.db"  # type: ignore[union-attr]
        bbs = dialog.findChild(QLineEdit, "guidedVaracBbs")
        bbs_archive = dialog.findChild(QLineEdit, "guidedVaracBbsArchive")
        assert bbs is not None and bbs.text() == "/cluster/BBS" and bbs.isReadOnly()
        assert bbs_archive is not None and bbs_archive.text() == "/cluster/BBS-Archive" and bbs_archive.isReadOnly()
        assert all(not button.isEnabled() for button in bbs.parent().findChildren(QPushButton))
        assert all(
            not button.isEnabled()
            for button in bbs_archive.parent().findChildren(QPushButton)
        )
        drafts = getattr(dialog, "_guided_software_instance_drafts")
        varac = drafts["varac"]
        assert varac["bbs_path"] == "/cluster/BBS"
        assert varac["bbs_archive_path"] == "/cluster/BBS-Archive"
        presentation = varac["varac_native_presentation"]
        assert native_draft_fingerprint(varac) == presentation["draft_fingerprint"]
        assert varac["_varac_native_apply_request"]["native_presentation"]["plan_fingerprint"] == "prepared-varac-bundle"
        review = dialog.findChild(QLabel, "guidedSaveReview")
        assert review is not None
        assert "VarAC INI: /wine/drive_c/VarAC/VarAC-Field.ini" in review.text()
        assert "Executable: wine" in review.text()
        assert "Arguments:" in review.text() and "VarAC-Field.ini" in review.text()
        assert "read/import only" not in review.text().casefold()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_zero_entry_managed_fast_js8_route_publishes_drafts_and_leaves_schedule_optional(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Qualified discovery is sufficient; Configure Details is an optional review."""

    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.core.config_autodiscovery import AppCandidate
    from freqinout.core.guided_setup import SCHEDULE_NONE

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    requests: list[object] = []

    def publish_qualified_snapshot(worker: object) -> None:
        request = getattr(worker, "request")
        requests.append(request)
        worker.finished.emit(
            {
                "guided_discovery_request": request,
                "install_candidates": (
                    AppCandidate("flrig", "FLRig", "/apps/FLRig", "test", "verified", True, True, "file"),
                    AppCandidate("fldigi", "FLDigi", "/apps/FLDigi", "test", "verified", True, True, "file"),
                    AppCandidate("flmsg", "FLMsg", "/apps/FLMsg", "test", "verified", True, True, "file"),
                    AppCandidate("flamp", "FLAmp", "/apps/FLAmp", "test", "verified", True, True, "file"),
                    # Exact version evidence permits the managed JS8 recipe;
                    # an unversioned candidate must remain Needs Attention.
                    AppCandidate("js8call", "JS8Call", "/apps/JS8Call-2.2.0", "test", "verified", True, True, "file"),
                ),
                "fast_results": {},
                "js8_results": {},
                "varac_results": {},
                "js8_file_profiles": (),
            }
        )

    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "run", publish_qualified_snapshot)
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda _worker, _thread: None,
    )

    visible_selection: dict[str, bool] = {}

    def inspect(dialog: QDialog) -> None:
        nonlocal visible_selection
        radio_name = next(
            field
            for field in dialog.findChildren(QLineEdit)
            if "radio name" in field.placeholderText().casefold()
        )
        radio_name.setText("Zero Entry Test")
        setup = dialog.findChild(QComboBox, "guidedSetupType")
        assert setup is not None
        setup.setCurrentIndex(setup.findData("fast_light"))
        _app().processEvents()
        for step_id in ("model", "software"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled()
            step.click()
            _app().processEvents()
        for text in ("JS8Call", "FIO Spotter", "CommStat"):
            _checkbox(dialog, text).setChecked(True)
        _app().processEvents()

        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        assert prepare is not None and not prepare.isVisible()
        assert _wait_until(lambda: bool(requests)), "managed preparation did not start automatically"
        assert _wait_until(
            lambda: dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus").text().startswith(("Ready", "Needs attention"))  # type: ignore[union-attr]
        ), "managed preparation did not complete"
        assert len(requests) == 1

        drafts = getattr(dialog, "_guided_software_instance_drafts", {})
        assert set(drafts) >= {"js8call", "fast_light"}
        assert drafts["js8call"]["launch_recipe_status"] == "qualified_managed"
        assert drafts["fast_light"]["launch_recipe_status"] == "qualified_managed"
        assert "fio_spotter" not in drafts
        assert "commstat" not in drafts
        assert dialog.findChild(QPushButton, "guidedSoftwareDetails_js8call").text() == "Review Details (optional)…"
        assert dialog.findChild(QPushButton, "guidedSoftwareDetails_fast_light").text() == "Review Details (optional)…"

        schedule_path = dialog.findChild(QComboBox, "guidedSchedulePathCombo")
        assert schedule_path is not None and schedule_path.currentData() == SCHEDULE_NONE
        for step_id in ("connection", "guard", "schedule", "review"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled(), step_id
            step.click()
            _app().processEvents()
        schedule_state = dialog.findChild(QLabel, "guidedSetupStepStatus_schedule")
        assert schedule_state is not None and schedule_state.text() == "Complete later"
        footer = dialog.findChild(QDialogButtonBox, "guidedRadioSetupActionFooter")
        assert footer is not None
        save = footer.button(QDialogButtonBox.Save)
        assert save is not None and save.isEnabled(), save.toolTip()
        visible_selection = {
            "use_flrig": _checkbox(dialog, "FLRig").isChecked(),
            "use_fldigi": _checkbox(dialog, "FLDigi").isChecked(),
            "use_flmsg": _checkbox(dialog, "FLMsg").isChecked(),
            "use_flamp": _checkbox(dialog, "FLAmp").isChecked(),
            "use_js8call": _checkbox(dialog, "JS8Call").isChecked(),
            "use_js8spotter": _checkbox(dialog, "FIO Spotter").isChecked(),
            "use_commstat": _checkbox(dialog, "CommStat").isChecked(),
            "use_varac": _checkbox(dialog, "VarAC").isChecked(),
        }
        save.click()
        assert _wait_until(lambda: dialog.result() == QDialog.Accepted)

    payload = _open_add_radio_dialog(monkeypatch, tmp_path, inspect)
    assert isinstance(payload, dict)
    drafts = payload["guided_software_instance_drafts"]
    assert {key: bool(payload[key]) for key in visible_selection} == visible_selection
    assert set(drafts) == {"js8call", "fast_light"}
    assert drafts["js8call"]["launch_recipe_status"] == "qualified_managed"
    assert drafts["fast_light"]["launch_recipe_status"] == "qualified_managed"
    assert "fio_spotter" not in drafts and "commstat" not in drafts
    assert "guided_frequency_plan_id" not in payload
    assert "guided_open_plan_manager_after_save" not in payload


def test_warning_recipe_permits_save_but_explicit_safety_block_does_not(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Version/launch uncertainty is not conflated with an unsafe target."""

    import freqinout.gui.settings_tab as settings_tab_module

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    def publish_unqualified_snapshot(worker: object) -> None:
        worker.finished.emit(
            {
                "guided_discovery_request": getattr(worker, "request"),
                "install_candidates": (),
                "fast_results": {},
                "js8_results": {},
                "varac_results": {},
                "js8_file_profiles": (),
            }
        )

    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "run", publish_unqualified_snapshot)
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(settings_tab_module._GuidedRadioAutofillWorker, "moveToThread", lambda *_args: None)

    def inspect(dialog: QDialog) -> None:
        name = next(
            field for field in dialog.findChildren(QLineEdit)
            if "radio name" in field.placeholderText().casefold()
        )
        name.setText("Warning policy radio")
        setup = dialog.findChild(QComboBox, "guidedSetupType")
        assert setup is not None
        setup.setCurrentIndex(setup.findData("fast_light"))
        _app().processEvents()
        for step_id in ("model", "software"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled()
            step.click()
        _checkbox(dialog, "JS8Call").setChecked(True)
        prepare = dialog.findChild(QPushButton, "guidedConfigureAutomaticallyButton")
        assert prepare is not None and not prepare.isVisible()
        assert _wait_until(
            lambda: dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus").text().startswith(("Ready", "Needs attention"))  # type: ignore[union-attr]
        )
        card = dialog.findChild(QGroupBox, "guidedSoftwareResponsibility_js8call")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert card is not None and card.property("guidedCardExpanded") is False
        assert next_button is not None and next_button.isEnabled()
        # Pending launch is an operator-visible warning, not a safety block.
        assert card.property("guidedPreparationState") in {"launch_pending", "warning"}
        for step_id in ("connection", "guard", "schedule", "review"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled(), step_id
            step.click()
        footer = dialog.findChild(QDialogButtonBox, "guidedRadioSetupActionFooter")
        assert footer is not None
        save = footer.button(QDialogButtonBox.Save)
        assert save is not None and save.isEnabled(), save.toolTip()

        drafts = getattr(dialog, "_guided_software_instance_drafts", {})
        assert "js8call" in drafts
        drafts["js8call"]["launch_recipe_status"] = "blocked_for_safety"
        drafts["js8call"]["launch_recipe"] = {
            "status": "blocked_for_safety",
            "recovery_action": "Choose a distinct profile path.",
        }
        review = dialog.findChild(QPushButton, "guidedWizardStep_review")
        assert review is not None
        review.click()
        _app().processEvents()
        assert not save.isEnabled()
        software = dialog.findChild(QPushButton, "guidedWizardStep_software")
        assert software is not None and software.isEnabled()
        software.click()
        _app().processEvents()
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        card = dialog.findChild(QGroupBox, "guidedSoftwareResponsibility_js8call")
        assert next_button is not None and not next_button.isEnabled()
        assert card is not None
        assert card.property("guidedPreparationState") == "blocked"
        assert card.property("guidedCardExpanded") is True
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_trimode_standalone_varac_skips_cluster_writer_and_keeps_topology_actionable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The safe standalone default is valid and never enters the cluster writer."""

    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.gui.settings_tab import SettingsTab

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    def publish_empty_snapshot(worker: object) -> None:
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

    cluster_writer_calls: list[object] = []

    def hold_cluster_writer(
        _self: object,
        worker: object,
        *,
        on_finished: Callable[[object], None],
        on_failed: Callable[[str], None],
    ) -> None:
        if isinstance(worker, settings_tab_module._VarACNativePrepareWorker):
            cluster_writer_calls.append(worker)
        else:
            on_finished(())

    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "run",
        publish_empty_snapshot,
    )
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda *_args: None,
    )
    monkeypatch.setattr(SettingsTab, "_start_varac_native_job", hold_cluster_writer)

    def inspect(dialog: QDialog) -> None:
        radio_name = next(
            field
            for field in dialog.findChildren(QLineEdit)
            if "radio name" in field.placeholderText().casefold()
        )
        radio_name.setText("TriMode Standalone")
        _enter_trimode_software_step(dialog)
        arrangement = dialog.findChild(QComboBox, "guidedVaracArrangement")
        card = dialog.findChild(QGroupBox, "guidedSoftwareResponsibility_varac")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        state = dialog.findChild(QLabel, "guidedSoftwarePreparedState_varac")
        assert arrangement is not None and card is not None
        assert next_button is not None and state is not None
        assert arrangement.currentData() == "standalone"
        assert _wait_until(
            lambda: card.property("guidedPreparationState") == "warning"
        ), state.text()
        assert cluster_writer_calls == []
        assert arrangement.isVisible() and arrangement.isEnabled()
        assert card.property("guidedCardExpanded") is True
        assert "standalone VarAC is selected" in state.text()
        assert _wait_until(next_button.isEnabled), next_button.toolTip()
        draft = getattr(dialog, "_guided_software_instance_drafts", {})["varac"]
        assert draft["cluster_path"] == "standalone"
        assert draft["varac_native_presentation"]["state"] == "standalone_ready"
        assert draft["varac_native_presentation"]["writer_required"] is False
        for step_id in ("connection", "guard", "schedule", "review"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled(), step_id
            step.click()
            _app().processEvents()
        footer = dialog.findChild(QDialogButtonBox, "guidedRadioSetupActionFooter")
        assert footer is not None
        save = footer.button(QDialogButtonBox.Save)
        assert save is not None and save.isEnabled(), save.toolTip()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


@pytest.mark.parametrize(
    ("selected", "expected_drafts"),
    (
        (("JS8Call",), {"js8call"}),
        (("FLRig", "FLDigi", "FLMsg", "FLAmp"), {"fast_light"}),
        (("JS8Call", "FIO Spotter", "CommStat"), {"js8call"}),
        (
            (
                "FLRig",
                "FLDigi",
                "FLMsg",
                "FLAmp",
                "JS8Call",
                "FIO Spotter",
                "CommStat",
            ),
            {"fast_light", "js8call"},
        ),
    ),
    ids=("js8-only", "fast-light-only", "js8-spotter-commstat", "trimode-without-varac"),
)
def test_guided_software_selection_matrix_allows_non_safety_warnings_to_continue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    selected: tuple[str, ...],
    expected_drafts: set[str],
) -> None:
    """Every supported non-VarAC mix prepares without a hidden combination gate.

    FIO Spotter and CommStat deliberately produce no radio-owned draft: they
    use the in-process feature and station-shared binding respectively.  They
    must therefore never turn an otherwise usable JS8Call mix into a blocked
    Add Radio step.  Empty discovery intentionally yields launch-pending
    recipes, proving those warnings are not confused with safety failures.
    """

    import freqinout.gui.settings_tab as settings_tab_module

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    def publish_empty_snapshot(worker: object) -> None:
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
        publish_empty_snapshot,
    )
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda *_args: None,
    )

    all_radio_owned = (
        "FLRig",
        "FLDigi",
        "FLMsg",
        "FLAmp",
        "JS8Call",
        "FIO Spotter",
        "CommStat",
        "VarAC",
    )

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        # Start each parameter case from an exact, visible checkbox state;
        # this catches retained defaults or implicit-service coupling.
        for label in all_radio_owned:
            _checkbox(dialog, label).setChecked(label in selected)
        _app().processEvents()

        status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert status is not None and next_button is not None
        assert _wait_until(lambda: status.text().startswith(("Ready —", "Needs attention —"))), status.text()
        assert _wait_until(next_button.isEnabled), status.text()

        drafts = getattr(dialog, "_guided_software_instance_drafts", {})
        assert expected_drafts <= set(drafts)
        assert "varac" not in drafts
        for family in expected_drafts:
            assert drafts[family]["launch_recipe_status"] in {
                "qualified_managed",
                "ready_with_warnings",
                "launch_pending",
            }
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


@pytest.mark.parametrize(
    ("native_state", "apply_requires_stopped_process", "expected_card_state", "can_continue"),
    (
        ("ready", False, "ready", True),
        ("ready", True, "warning", True),
        ("blocked", False, "blocked", False),
    ),
    ids=("ready", "ready-requires-stopped-process", "blocked-preserves-why"),
)
def test_full_trimode_varac_cluster_native_result_controls_continue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    native_state: str,
    apply_requires_stopped_process: bool,
    expected_card_state: str,
    can_continue: bool,
) -> None:
    """Native VarAC outcomes are visible and have exactly one navigation policy.

    This is the production combination: Fast Light, JS8Call, FIO Spotter,
    CommStat, and a new VarAC cluster.  The discovery worker completes first;
    the VarAC writer then completes independently.  Continue stays disabled
    during that second operation.  A qualified result (including the
    non-destructive "stop the app before final Save" warning) enables it;
    a blocked result keeps the exact native explanation visible and blocks it.
    """

    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.core.varac_native_preparation import (
        VarACNativePreparationResult,
        native_draft_fingerprint,
    )
    from freqinout.gui.settings_tab import SettingsTab
    import freqinout.core.guided_app_config_plan as guided_app_config_plan_module

    def fail_legacy_varac_scan(*_args: object, **_kwargs: object) -> object:
        raise AssertionError(
            "Add Radio must consume its native VarAC bundle; it must not run "
            "legacy synchronous VarAC filesystem discovery while rendering or navigating."
        )

    monkeypatch.setattr(
        guided_app_config_plan_module,
        "discover_varac_local_assets",
        fail_legacy_varac_scan,
    )

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    def publish_empty_snapshot(worker: object) -> None:
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

    pending_native: list[tuple[object, Callable[[object], None]]] = []

    def hold_native_prepare(
        _self: object,
        worker: object,
        *,
        on_finished: Callable[[object], None],
        on_failed: Callable[[str], None],
    ) -> None:
        if isinstance(worker, settings_tab_module._VarACNativePrepareWorker):
            pending_native.append((worker, on_finished))
        else:
            on_finished(())

    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "run",
        publish_empty_snapshot,
    )
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda *_args: None,
    )
    monkeypatch.setattr(SettingsTab, "_start_varac_native_job", hold_native_prepare)

    def native_result(worker: object) -> object:
        draft = dict(getattr(worker, "draft"))
        if native_state != "ready":
            return VarACNativePreparationResult(
                state=native_state,
                presentation={
                    "state": native_state,
                    "why": "The selected VarAC database is already owned by another active node.",
                    "writer_qualified": False,
                    "generation": getattr(worker, "generation"),
                },
                draft_fingerprint=native_draft_fingerprint(draft),
                generation=getattr(worker, "generation"),
                plan=None,
            )
        presentation = {
            "state": "ready",
            "writer_qualified": True,
            "apply_requires_stopped_process": apply_requires_stopped_process,
            "writer_version": "13.2.7",
            "generation": getattr(worker, "generation"),
            "application_path": "/wine/drive_c/VarAC/VarAC.exe",
            "configuration_path": "/wine/drive_c/VarAC/FT-710.ini",
            "storage_path": "/cluster/VarAC.db",
            "secondary_storage_path": "/managed/ft-710/incoming",
            "outbox_path": "/managed/ft-710/outbox",
            "cluster_bbs_path": "/cluster/BBS",
            "cluster_bbs_archive_path": "/cluster/BBS-Archive",
            "working_directory": "/wine/drive_c/VarAC",
            "vara_runtime_path": "/managed/ft-710/VARA",
            "vara_ini_path": "/managed/ft-710/VARA/VARA.ini",
            "launch_command": 'wine /wine/drive_c/VarAC/VarAC.exe "C:\\VarAC\\FT-710.ini"',
            "launch_argv": ("wine", "/wine/drive_c/VarAC/VarAC.exe", "C:\\VarAC\\FT-710.ini"),
            "launch_environment": {"WINEPREFIX": "/wine"},
            "ports_summary": "command 8304 · KISS 8306",
        }
        return VarACNativePreparationResult(
            state="ready",
            presentation=presentation,
            draft_fingerprint=native_draft_fingerprint(draft),
            generation=getattr(worker, "generation"),
            plan=SimpleNamespace(plan_fingerprint="trimode-varac-cluster"),
        )

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        arrangement = dialog.findChild(QComboBox, "guidedVaracArrangement")
        assert arrangement is not None
        create_cluster = arrangement.findData("create_cluster")
        assert create_cluster >= 0
        arrangement.setCurrentIndex(create_cluster)
        for label in ("FLRig", "FLDigi", "FLMsg", "FLAmp", "JS8Call", "FIO Spotter", "CommStat", "VarAC"):
            _checkbox(dialog, label).setChecked(True)
        _app().processEvents()

        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        varac_card = dialog.findChild(QGroupBox, "guidedSoftwareResponsibility_varac")
        assert next_button is not None and varac_card is not None
        assert _wait_until(lambda: bool(pending_native)), "VarAC native preparation did not start"
        # The UI must identify the in-flight qualified writer—not describe it
        # as an unexplained needs-attention condition—and Continue must wait.
        assert not next_button.isEnabled()
        assert varac_card.property("guidedPreparationState") == "preparing"
        software_step_status = dialog.findChild(QLabel, "guidedSetupStepStatus_software")
        assert software_step_status is not None
        assert software_step_status.text() != "Ready"

        worker, finish = pending_native[-1]
        finish(native_result(worker))
        expected_why = "The selected VarAC database is already owned by another active node."
        if can_continue:
            assert _wait_until(next_button.isEnabled), "qualified VarAC bundle did not enable Continue"
        else:
            assert _wait_until(
                lambda: varac_card.property("guidedPreparationState") == "blocked"
            ), "blocked VarAC result was not rendered"
            assert not next_button.isEnabled()
        assert varac_card.property("guidedPreparationState") == expected_card_state
        drafts = getattr(dialog, "_guided_software_instance_drafts", {})
        native = drafts["varac"]["varac_native_presentation"]
        assert native["state"] == native_state
        if can_continue:
            assert drafts["varac"]["configuration_path"] == "/wine/drive_c/VarAC/FT-710.ini"
            if apply_requires_stopped_process:
                state = dialog.findChild(QLabel, "guidedSoftwarePreparedState_varac")
                assert state is not None and "Close VarAC and VARA before final Save" in state.text()
        else:
            state = dialog.findChild(QLabel, "guidedSoftwarePreparedState_varac")
            top_status = dialog.findChild(QLabel, "guidedConfigureAutomaticallyStatus")
            assert state is not None and expected_why in state.text()
            assert top_status is not None and expected_why in top_status.text()
            assert arrangement.isVisible() and arrangement.isEnabled()
            assert varac_card.property("guidedCardExpanded") is True
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_deselecting_varac_clears_pending_native_gate_and_discards_late_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A removed VarAC choice cannot retain a worker gate or reappear later."""

    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.core.varac_native_preparation import (
        VarACNativePreparationResult,
        native_draft_fingerprint,
    )
    from freqinout.gui.settings_tab import SettingsTab

    class _ImmediateThread(QObject):
        started = Signal()
        finished = Signal()

        def start(self) -> None:
            self.started.emit()

        def quit(self) -> None:
            self.finished.emit()

    def publish_empty_snapshot(worker: object) -> None:
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

    held_native: list[tuple[object, Callable[[object], None]]] = []

    def hold_native_prepare(
        _self: object,
        worker: object,
        *,
        on_finished: Callable[[object], None],
        on_failed: Callable[[str], None],
    ) -> None:
        if isinstance(worker, settings_tab_module._VarACNativePrepareWorker):
            held_native.append((worker, on_finished))
        else:
            on_finished(())

    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "run",
        publish_empty_snapshot,
    )
    monkeypatch.setattr(settings_tab_module, "QThread", _ImmediateThread)
    monkeypatch.setattr(
        settings_tab_module._GuidedRadioAutofillWorker,
        "moveToThread",
        lambda *_args: None,
    )
    monkeypatch.setattr(SettingsTab, "_start_varac_native_job", hold_native_prepare)

    def stale_ready_result(worker: object) -> object:
        draft = dict(getattr(worker, "draft"))
        return VarACNativePreparationResult(
            state="ready",
            presentation={
                "state": "ready",
                "writer_qualified": True,
                "generation": getattr(worker, "generation"),
                "application_path": "/wine/drive_c/VarAC/VarAC.exe",
            },
            draft_fingerprint=native_draft_fingerprint(draft),
            generation=getattr(worker, "generation"),
            plan=SimpleNamespace(plan_fingerprint="late-removed-varac"),
        )

    def inspect(dialog: QDialog) -> None:
        _enter_trimode_software_step(dialog)
        arrangement = dialog.findChild(QComboBox, "guidedVaracArrangement")
        assert arrangement is not None
        arrangement.setCurrentIndex(arrangement.findData("create_cluster"))
        _checkbox(dialog, "JS8Call").setChecked(True)
        _checkbox(dialog, "VarAC").setChecked(True)
        _app().processEvents()

        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert next_button is not None
        assert _wait_until(lambda: bool(held_native)), "VarAC native preparation did not start"
        assert not next_button.isEnabled()

        _checkbox(dialog, "VarAC").setChecked(False)
        assert _wait_until(next_button.isEnabled), "removing VarAC did not release Continue"
        assert "varac" not in getattr(dialog, "_guided_software_instance_drafts", {})

        worker, finish = held_native[-1]
        finish(stale_ready_result(worker))
        _app().processEvents()
        assert "varac" not in getattr(dialog, "_guided_software_instance_drafts", {})
        assert next_button.isEnabled(), "late VarAC result reintroduced a navigation gate"
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)


def test_radios_mode_collapses_empty_compact_header_and_top_packs_profile_content(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The Radios workspace must not retain an empty styled header spacer."""

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)
    tab = SettingsTab()
    try:
        tab.resize(1000, 700)
        tab.show()
        assert tab.show_settings_context("radios")
        _app().processEvents()
        header = tab.findChild(QWidget, "settingsCompactHeaderBar")
        assert header is not None and not header.isVisible()
        group = tab.radio_profile_section_group
        content = tab._section_meta[group]["content"]
        header_button = tab._section_meta[group]["header_btn"]
        assert content.isVisible()
        # The natural-height profile container begins directly below its
        # section header; it does not expand an elastic blank region first.
        assert content.geometry().top() <= header_button.geometry().bottom() + 12
        assert group.sizePolicy().verticalPolicy() != QSizePolicy.Expanding
    finally:
        tab.close()
        tab.deleteLater()
        _app().processEvents()


@pytest.mark.parametrize("size", [(1920, 1080), (1000, 700), (900, 560)])
def test_add_radio_fixed_navigation_has_one_body_scroll_owner_and_no_horizontal_page_scroll(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    size: tuple[int, int],
) -> None:
    """The live Add Radio shell follows the same geometry contract as the assistant."""

    def inspect(dialog: QDialog) -> None:
        dialog.resize(*size)
        name = next(
            field for field in dialog.findChildren(QLineEdit)
            if "radio name" in field.placeholderText().casefold()
        )
        name.setText("Footer reachability radio")
        _app().processEvents()
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
        body.verticalScrollBar().setValue(body.verticalScrollBar().maximum())
        _app().processEvents()
        assert body.verticalScrollBar().value() == body.verticalScrollBar().maximum()
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
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert next_button is not None and next_button.isEnabled()
        next_button.click()
        _app().processEvents()
        back_button = dialog.findChild(QPushButton, "guidedWizardBack")
        assert back_button is not None and back_button.isEnabled()
        dialog.reject()

    _open_add_radio_dialog(monkeypatch, tmp_path, inspect)
