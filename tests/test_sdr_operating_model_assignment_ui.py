from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QCheckBox

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager


def _qapplication_or_skip() -> QApplication:
    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication already exists in this test process.")
    return app or QApplication([])


def _select_assignment_radio(tab, radio_id: int) -> None:
    for row in range(tab.device_assignments_table.rowCount()):
        wrapper = tab.device_assignments_table.cellWidget(row, 0)
        checkbox = wrapper.findChild(QCheckBox) if wrapper is not None else None
        if checkbox is None:
            continue
        selected_id = int(checkbox.property("device_profile_id") or 0)
        checkbox.setChecked(selected_id == int(radio_id))


def test_assignment_profile_filter_keeps_only_receive_only_models_for_observers() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    profiles = (
        {"id": 1, "name": "Station", "enabled": 1, "receive_only": 0},
        {"id": 2, "name": "Receive-only SDR", "enabled": 1, "receive_only": 1},
        {"id": 3, "name": "Disabled Receiver", "enabled": 0, "receive_only": 1},
    )

    observer_choices = SettingsTab._assignment_profiles_for_devices(
        profiles,
        ({"device_class": "observer"},),
    )
    radio_choices = SettingsTab._assignment_profiles_for_devices(
        profiles,
        ({"device_class": "tx_rx"},),
    )

    assert [row["id"] for row in observer_choices] == [2]
    assert [row["id"] for row in radio_choices] == [1, 2]


def test_observer_assignment_editor_exposes_only_receive_only_choice(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = _qapplication_or_skip()
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    observer = store.save_device_profile(
        {
            "name": "Receive Station",
            "device_class": "observer",
            "control_backend": "manual",
        }
    )
    store.save_operating_profile(
        {
            "name": "Transmit Operations",
            "enabled": 1,
            "receive_only": 0,
        }
    )
    receive_only = store.save_operating_profile(
        {
            "name": "Receive-only Test",
            "enabled": 1,
            "receive_only": 1,
            "scheduler_enabled": 0,
        }
    )

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)
    tab = SettingsTab()
    try:
        _select_assignment_radio(tab, int(observer["id"]))
        tab._assign_operating_profile_to_selected_devices()

        choice_ids = {
            int(tab.assignment_editor_plan_combo.itemData(index) or 0)
            for index in range(tab.assignment_editor_plan_combo.count())
        }
        assert int(receive_only["id"]) in choice_ids
        assert all(
            int(profile.get("receive_only", 0) or 0) == 1
            for profile in tab.operating_profiles
            if int(profile.get("id", 0) or 0) in choice_ids
        )
        assert tab.assignment_editor_plan_label.text() == "Receive-only Model:"
        assert "receive-only Operating Model" in tab.assignment_editor_summary_label.text()
        assert tab.assignment_editor_state_combo.isEnabled() is False
        assert tab.assignment_editor_state_combo.currentData() == "active"
        assert tab.assignment_editor_ends_edit.isHidden() is True
        assert "do not grant scheduler, QSY, or transmit control" in tab.assignment_editor_state_combo.toolTip()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_guided_observer_finalization_assigns_model_then_adopts_js8_then_activates() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Store:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []

        def ensure_receive_only_operating_profile(self):
            self.calls.append(("ensure_model", None))
            return {"id": 9}

        def set_device_operating_profile(self, radio_id, model_id):
            self.calls.append(("assign_model", (radio_id, model_id)))
            return {"device_profile_id": radio_id, "operating_profile_id": model_id}

        def adopt_observer_js8_instance(self, **kwargs):
            self.calls.append(("adopt_js8", kwargs))
            return {
                "radio": {
                    "id": kwargs["radio_profile_id"],
                    "name": "RTL-SDR",
                    "device_class": "observer",
                    "js8_instance_id": 31,
                }
            }

        def set_device_profile_runtime_active(self, radio_id, active):
            self.calls.append(("activate", (radio_id, active)))
            return {"id": radio_id, "runtime_active": 1, "runtime_primary": 0}

    store = Store()
    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = store
    tab._last_persisted_device_profile = None
    tab._refresh_multi_radio_tables = lambda: None
    tab._emit_device_profiles_changed = lambda: None

    assert SettingsTab._finalize_guided_observer_profile(
        tab,
        {"id": 4, "name": "RTL-SDR", "device_class": "observer"},
        js8_draft={
            "instance_name": "RTL-SDR JS8Call",
            "host": "127.0.0.1",
            "port": 2448,
            "application_path": "/opt/js8call/js8call",
            "configuration_path": "/data/rtl-sdr-js8",
            "storage_path": "/data/rtl-sdr-js8",
            "launch_at_startup": True,
        },
        activate_after_assignment=True,
    ) is True

    assert [name for name, _payload in store.calls] == [
        "ensure_model",
        "assign_model",
        "adopt_js8",
        "activate",
    ]
    adoption = dict(store.calls[2][1])
    assert adoption["replace_existing"] is False
    assert adoption["expected_current_instance_id"] is None
    assert adoption["launch_at_startup"] is True
    assert adoption["manifest_values"]["verification_state"] == "configured"
    assert adoption["manifest_values"]["resource_claims"]
    assert int(tab._last_persisted_device_profile["runtime_primary"]) == 0
