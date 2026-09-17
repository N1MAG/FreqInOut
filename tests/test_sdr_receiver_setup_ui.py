from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest


def _application_or_skip():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication already exists in this test process.")
    return app or QApplication([])


def _open_receiver_dialog(
    monkeypatch,
    tmp_path,
    profile: dict[str, object] | None,
    inspect,
    *,
    service_ready: bool = False,
    configure_tab=None,
) -> dict[str, object] | None:
    from PySide6.QtWidgets import QDialog

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status_compat", lambda self, force=False: None)

    def fake_exec(dialog: QDialog) -> int:
        inspect(dialog)
        return dialog.result()

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    app = _application_or_skip()
    tab = SettingsTab()
    tab.set_receiver_control_test_service_ready(service_ready)
    if configure_tab is not None:
        configure_tab(tab)
    try:
        return tab._open_device_profile_dialog(profile)
    finally:
        tab.deleteLater()
        app.processEvents()


def test_observer_guided_flow_orders_model_first_and_stages_distinct_js8(monkeypatch, tmp_path) -> None:
    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QLabel, QLineEdit, QPushButton

        setup_type = dialog.findChild(QComboBox, "guidedSetupType")
        assert setup_type is not None
        setup_type.setCurrentIndex(setup_type.findData("sdr_observer"))
        QApplication.processEvents()

        visible_steps = [
            button.objectName().removeprefix("guidedWizardStep_")
            for button in dialog.findChildren(QPushButton)
            if button.objectName().startswith("guidedWizardStep_") and not button.isHidden()
        ]
        assert visible_steps == [
            "radio",
            "model",
            "software",
            "connection",
            "guard",
            "schedule",
            "review",
        ]
        guard_step = dialog.findChild(QPushButton, "guidedWizardStep_guard")
        schedule_step = dialog.findChild(QPushButton, "guidedWizardStep_schedule")
        assert guard_step is not None and schedule_step is not None
        assert guard_step.text() == "5. Safety"
        assert schedule_step.text() == "6. Schedule"
        assert guard_step.property("guidedStepApplicable") is True
        assert schedule_step.property("guidedStepApplicable") is True
        assert "receiver guard" in guard_step.toolTip().lower()
        assert "receive schedule" in schedule_step.toolTip().lower()

        js8_choice = next(
            checkbox
            for checkbox in dialog.findChildren(QCheckBox)
            if checkbox.text() == "JS8Call"
        )
        assert js8_choice.isEnabled()
        js8_choice.setChecked(True)
        for step_id in ("model", "software", "connection"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None
            step.click()
            QApplication.processEvents()

        js8_values = {
            "guidedJs8Host": "127.0.0.1",
            "guidedJs8Port": "2448",
            "guidedJs8Application": "/opt/js8call/js8call",
            "guidedJs8Profile": str(tmp_path / "js8-rx"),
            "guidedJs8Directed": str(tmp_path / "js8-rx" / "DIRECTED.TXT"),
        }
        for object_name, value in js8_values.items():
            edit = dialog.findChild(QLineEdit, object_name)
            assert edit is not None
            edit.setText(value)
        launch = dialog.findChild(QCheckBox, "guidedObserverJs8LaunchEnabled")
        assert launch is not None and not launch.isHidden()
        launch.setChecked(True)

        # Receiver Guard and Receive Schedule are real, stable steps for an
        # observer; walk them before Review & Save instead of skipping them.
        for step_id in ("guard", "schedule", "review"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            assert step is not None and step.isEnabled()
            step.click()
            QApplication.processEvents()
        review_label = dialog.findChild(QLabel, "guidedSaveReview")
        assert review_label is not None
        assert "Operating Model: Receive-only SDR" in review_label.text()
        assert "JS8Call (receive-only; launch with FIO)" in review_label.text()
        assert "no Compose/Expect sending, QSY, PTT, or scheduler authority" in review_label.text()

    result = _open_receiver_dialog(monkeypatch, tmp_path, None, inspect)
    assert result is None


def test_transceiver_guided_flow_keeps_stable_numbering_and_selectable_model(monkeypatch, tmp_path) -> None:
    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QApplication, QComboBox, QPushButton

        setup_type = dialog.findChild(QComboBox, "guidedSetupType")
        next_button = dialog.findChild(QPushButton, "guidedWizardNext")
        assert setup_type is not None and next_button is not None
        setup_type.setCurrentIndex(setup_type.findData("js8_only"))
        QApplication.processEvents()

        steps = [
            dialog.findChild(QPushButton, f"guidedWizardStep_{step_id}")
            for step_id in ("radio", "model", "software", "connection", "guard", "schedule", "review")
        ]
        assert all(step is not None and not step.isHidden() for step in steps)
        model_step = steps[1]
        assert model_step is not None
        assert model_step.text() == "2. Operating Model"
        assert model_step.isEnabled()
        assert model_step.property("guidedStepApplicable") is True
        model_step.click()
        QApplication.processEvents()
        model_combo = dialog.findChild(QComboBox, "guidedOperatingModel")
        assert model_combo is not None
        assert model_combo.isEnabled()
        assert model_combo.count() >= 1
        assert int(model_combo.currentData() or 0) > 0
        assert next_button.text() == "Next: Software"

    _open_receiver_dialog(monkeypatch, tmp_path, None, inspect)


def test_existing_observer_custom_receive_only_model_is_preselected_on_edit(monkeypatch, tmp_path) -> None:
    selected_model = {"id": 91, "name": "Field SDR Watch", "enabled": 1, "receive_only": 1}

    def configure(tab) -> None:
        tab.operating_profiles = [selected_model]
        tab.device_assignments = [{"device_profile_id": 71, "operating_profile_id": 91}]

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QComboBox

        combo = dialog.findChild(QComboBox, "guidedOperatingModel")
        assert combo is not None
        assert combo.currentData() == 91
        assert combo.currentText() == "Field SDR Watch (receive-only)"

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {"id": 71, "name": "Existing SDR", "device_class": "observer"},
        inspect,
        configure_tab=configure,
    )


def test_existing_transceiver_operating_model_is_preselected_on_edit(monkeypatch, tmp_path) -> None:
    selected_model = {"id": 92, "name": "Portable Field Model", "enabled": 1, "receive_only": 0}

    def configure(tab) -> None:
        tab.operating_profiles = [selected_model]
        tab.device_assignments = [{"device_profile_id": 72, "operating_profile_id": 92}]

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QComboBox

        combo = dialog.findChild(QComboBox, "guidedOperatingModel")
        assert combo is not None
        assert combo.isEnabled()
        assert combo.currentData() == 92
        assert combo.currentText() == "Portable Field Model"

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {"id": 72, "name": "Existing Rig", "device_class": "tx_rx"},
        inspect,
        configure_tab=configure,
    )


def test_add_radio_save_persists_selected_model_for_nonfirst_transceiver(monkeypatch) -> None:
    from types import SimpleNamespace
    from freqinout.gui.settings_tab import SettingsTab

    assigned: list[tuple[int, int, dict[str, object]]] = []
    persisted: list[dict[str, object]] = []
    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = SimpleNamespace(
        list_device_profiles=lambda: [{"id": 1, "name": "Primary"}],
        set_device_operating_profile=lambda radio_id, model_id, **kwargs: assigned.append((radio_id, model_id, kwargs)),
    )
    tab._open_device_profile_dialog = lambda existing=None: {
        "id": None,
        "name": "Second Rig",
        "device_class": "tx_rx",
        "guided_operating_profile_id": 42,
    }
    def persist(payload, **kwargs):
        persisted.append(dict(kwargs))
        tab._last_persisted_device_profile = {**payload, "id": 2}
        return True

    tab._persist_device_profile = persist

    tab._add_device_profile()

    assert persisted == [{"defer_activation_until_assignment": True}]
    assert assigned == [(2, 42, {
        "assignment_state": "active",
        "reason": "Operating Model selected during guided radio setup.",
        "created_by": "guided_radio_setup",
    })]


def test_first_transceiver_is_activated_only_after_selected_model_assignment() -> None:
    from types import SimpleNamespace

    from freqinout.gui.settings_tab import SettingsTab

    events: list[tuple[str, int, int | None]] = []
    saved_radio = {
        "id": 1,
        "name": "First Rig",
        "device_class": "tx_rx",
        "runtime_active": 0,
        "runtime_primary": 0,
    }

    def assign(radio_id, model_id, **_kwargs):
        events.append(("assign", radio_id, model_id))

    def activate(radio_id):
        assert events == [("assign", 1, 42)]
        events.append(("activate", radio_id, None))
        return {**saved_radio, "runtime_active": 1, "runtime_primary": 1}

    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = SimpleNamespace(
        list_device_profiles=lambda: [],
        set_device_operating_profile=assign,
        set_runtime_primary_device_profile=activate,
    )
    tab._open_device_profile_dialog = lambda existing=None: {
        "id": None,
        "name": "First Rig",
        "device_class": "tx_rx",
        "guided_operating_profile_id": 42,
    }

    def persist(payload, **kwargs):
        assert kwargs == {"defer_activation_until_assignment": True}
        tab._last_persisted_device_profile = dict(saved_radio)
        return True

    tab._persist_device_profile = persist
    tab._refresh_runtime_projection_ui = lambda **_kwargs: None
    tab._refresh_multi_radio_tables = lambda: None

    tab._add_device_profile()

    assert events == [("assign", 1, 42), ("activate", 1, None)]


def test_failed_new_observer_model_assignment_remains_inactive_and_nonprimary(monkeypatch) -> None:
    from freqinout.gui import settings_tab
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(settings_tab.QMessageBox, "warning", lambda *args, **kwargs: None)
    radio = {"id": 2, "device_class": "observer", "runtime_active": 0, "runtime_primary": 0}

    class _Store:
        def ensure_receive_only_operating_profile(self):
            return {"id": 73}

        def set_device_operating_profile(self, _radio_id, _model_id, **_kwargs):
            raise ValueError("assignment unavailable")

    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = _Store()
    tab._refresh_multi_radio_tables = lambda: None
    tab._emit_device_profiles_changed = lambda: None

    assert tab._finalize_guided_observer_profile(radio) is False
    assert radio["runtime_active"] == 0
    assert radio["runtime_primary"] == 0


def test_unsaved_receiver_can_test_and_keep_verified_evidence_in_one_session(monkeypatch, tmp_path) -> None:
    emitted: list[dict[str, object]] = []

    def configure(tab) -> None:
        def complete(payload) -> None:
            snapshot = dict(payload)
            emitted.append(snapshot)
            tab.receiver_control_test_completed.emit(
                {
                    "profile_id": 0,
                    "qualification_request_id": snapshot["qualification_request_id"],
                    "verification_state": "verified",
                    "detail": "Frequency tuning and restoration were verified.",
                    "verification": {
                        "schema_version": 1,
                        "adapter": snapshot["sdr_adapter"],
                        "host": snapshot["sdr_host"],
                        "port": int(snapshot["sdr_port"]),
                        "target": snapshot["sdr_target"],
                        "tested_at_utc": "2026-09-16T18:00:00+00:00",
                        "tune_readback_verified": True,
                        "restore_readback_verified": True,
                    },
                }
            )

        tab.receiver_control_test_requested.connect(complete)

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QLabel, QPushButton

        setup_type = dialog.findChild(QComboBox, "guidedSetupType")
        adapter = dialog.findChild(QComboBox, "guidedReceiverAdapter")
        connection_step = dialog.findChild(QPushButton, "guidedWizardStep_connection")
        button = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        assert setup_type is not None
        setup_type.setCurrentIndex(setup_type.findData("sdr_observer"))
        assert adapter is not None
        adapter.setCurrentIndex(adapter.findData("sdrpp_rigctl"))
        QApplication.processEvents()
        for step_name in ("model", "software"):
            step = dialog.findChild(QPushButton, f"guidedWizardStep_{step_name}")
            assert step is not None and step.isEnabled()
            step.click()
            QApplication.processEvents()
        assert connection_step is not None and connection_step.isEnabled()
        connection_step.click()
        QApplication.processEvents()
        assert button is not None and not button.isHidden() and button.isEnabled()
        assert enabled is not None and not enabled.isEnabled()
        button.click()
        QApplication.processEvents()
        QApplication.processEvents()
        assert emitted and emitted[0]["id"] == 0
        assert str(emitted[0]["qualification_request_id"])
        assert verification is not None and "FIO tuning ready" in verification.text()
        assert enabled.isEnabled()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        None,
        inspect,
        service_ready=True,
        configure_tab=configure,
    )


def test_receiver_setup_shows_adapter_choice_and_keeps_future_control_disabled(monkeypatch, tmp_path) -> None:
    """SDR-1 setup stores intent but cannot imply a live adapter exists."""

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QCheckBox, QComboBox, QLabel, QPushButton

        adapter = dialog.findChild(QComboBox, "guidedReceiverAdapter")
        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        test_control = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        assert adapter is not None
        assert verification is not None
        assert enabled is not None
        assert test_control is not None
        assert adapter.findData("manual") >= 0
        assert adapter.findData("sdrpp_rigctl") >= 0
        assert "Manual tuning" in verification.text()
        assert not enabled.isEnabled()
        assert not enabled.isChecked()
        assert not test_control.isEnabled()
        assert "asynchronous adapter service" in test_control.toolTip()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 41,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
        },
        inspect,
    )


def test_receiver_setup_exposes_receive_only_launch_stack_in_guided_flow(monkeypatch, tmp_path) -> None:
    """Observer setup offers an RX-only app launch choice before Connection."""

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QGroupBox, QLineEdit, QPushButton

        setup_type = dialog.findChild(QComboBox, "guidedSetupType")
        software_step = dialog.findChild(QPushButton, "guidedWizardStep_software")
        stack = dialog.findChild(QGroupBox, "guidedReceiverSoftwareStack")
        application = dialog.findChild(QComboBox, "guidedReceiverLaunchApplication")
        launch_path = dialog.findChild(QLineEdit, "guidedReceiverLaunchPath")
        launch_enabled = dialog.findChild(QCheckBox, "guidedReceiverLaunchEnabled")
        assert setup_type is not None and software_step is not None
        setup_type.setCurrentIndex(setup_type.findData("sdr_observer"))
        QApplication.processEvents()
        software_step.click()
        QApplication.processEvents()
        assert stack is not None and not stack.isHidden()
        assert application is not None and application.findData("SDR++") >= 0
        assert launch_path is not None and launch_enabled is not None
        application.setCurrentIndex(application.findData("SDR++"))
        QApplication.processEvents()
        assert launch_path.text().strip()
        assert launch_enabled.isEnabled()
        assert launch_enabled.isChecked()

    _open_receiver_dialog(monkeypatch, tmp_path, None, inspect)


def test_receiver_setup_rejects_stale_persisted_evidence(monkeypatch, tmp_path) -> None:
    """Legacy or mismatched evidence must never enable receiver control."""

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QCheckBox, QComboBox, QLabel, QPushButton

        adapter = dialog.findChild(QComboBox, "guidedReceiverAdapter")
        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        test_control = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        assert adapter is not None
        assert verification is not None
        assert enabled is not None
        assert test_control is not None
        assert adapter.currentData() == "sdrpp_rigctl"
        assert "Verification pending" in verification.text()
        assert "does not match" in verification.text()
        assert not enabled.isEnabled()
        assert not enabled.isChecked()
        assert not test_control.isEnabled()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 42,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
            "sdr_adapter": "sdrpp_rigctl",
            "sdr_target": "VFO A",
            "sdr_host": "127.0.0.1",
            "sdr_port": 4532,
            "sdr_control_enabled": 1,
            "sdr_verification_state": "verified",
            "sdr_verification_json": '{"tested_at":"2026-09-10","readback":true}',
        },
        inspect,
    )


def test_receiver_setup_enables_opt_in_only_for_matching_reversible_evidence(monkeypatch, tmp_path) -> None:
    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QCheckBox, QLabel, QPushButton

        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        test_control = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        assert verification is not None
        assert enabled is not None
        assert test_control is not None
        assert "FIO tuning ready" in verification.text()
        assert enabled.isEnabled()
        assert enabled.isChecked()
        assert test_control.isEnabled()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 43,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
            "sdr_adapter": "sdrpp_rigctl",
            "sdr_target": "selected-vfo",
            "sdr_host": "127.0.0.1",
            "sdr_port": 4532,
            "sdr_control_enabled": 1,
            "sdr_verification_state": "verified",
            "sdr_verification_json": (
                '{"schema_version":1,"adapter":"sdrpp_rigctl","host":"127.0.0.1","port":4532,'
                '"target":"selected-vfo","tested_at_utc":"2026-09-10T12:00:00+00:00",'
                '"tune_readback_verified":true,"restore_readback_verified":true}'
            ),
        },
        inspect,
        service_ready=True,
    )


def test_receiver_setup_control_surface_is_cache_only() -> None:
    source = (
        __import__("pathlib").Path(__file__).parents[1] / "freqinout" / "gui" / "settings_tab.py"
    ).read_text(encoding="utf-8")
    surface = source.split("def _update_receiver_manual_card", 1)[1].split("def _browse_guided_app_choice", 1)[0]
    assert "saved evidence" in surface
    assert "FIO tuning ready" in surface
    assert ".probe(" not in surface
    assert "socket" not in surface.lower()
    assert "set_receive_frequency" not in surface


def test_async_test_result_enables_opt_in_without_endpoint_io_on_ui_thread(monkeypatch, tmp_path) -> None:
    def configure(tab) -> None:
        def complete(payload) -> None:
            evidence = {
                "schema_version": 1,
                "adapter": payload["sdr_adapter"],
                "host": payload["sdr_host"],
                "port": int(payload["sdr_port"]),
                "target": payload["sdr_target"],
                "tested_at_utc": "2026-09-10T12:00:00+00:00",
                "tune_readback_verified": True,
                "restore_readback_verified": True,
            }
            tab.receiver_control_test_completed.emit(
                {
                    "profile_id": int(payload["id"]),
                    "qualification_request_id": payload["qualification_request_id"],
                    "verification_state": "verified",
                    "detail": "Frequency tuning and restoration were verified.",
                    "verification": evidence,
                }
            )

        tab.receiver_control_test_requested.connect(complete)

    def inspect(dialog) -> None:
        from PySide6.QtWidgets import QApplication, QCheckBox, QLabel, QPushButton

        button = dialog.findChild(QPushButton, "guidedReceiverTestControl")
        enabled = dialog.findChild(QCheckBox, "guidedReceiverControlEnabled")
        verification = dialog.findChild(QLabel, "guidedReceiverVerificationSummary")
        assert button is not None and button.isEnabled()
        assert enabled is not None and not enabled.isEnabled()
        button.click()
        QApplication.processEvents()
        QApplication.processEvents()
        assert enabled.isEnabled()
        assert not enabled.isChecked()
        assert verification is not None and "FIO tuning ready" in verification.text()

    _open_receiver_dialog(
        monkeypatch,
        tmp_path,
        {
            "id": 44,
            "name": "RTL-SDR receiver",
            "device_class": "observer",
            "control_backend": "manual",
            "radio_model": "RTL-SDR",
            "sdr_application": "SDR++",
            "sdr_adapter": "sdrpp_rigctl",
            "sdr_target": "selected-vfo",
            "sdr_host": "127.0.0.1",
            "sdr_port": 4532,
            "sdr_control_enabled": 0,
            "sdr_verification_state": "unverified",
        },
        inspect,
        service_ready=True,
        configure_tab=configure,
    )
