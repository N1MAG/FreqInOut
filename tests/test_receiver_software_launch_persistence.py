from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.receiver_software_stack import build_receiver_launch_items
from freqinout.gui.settings_tab import SettingsTab


class _LaunchRecorder:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[int, list[dict[str, object]], bool]] = []

    def set_radio_launch_bundle(self, radio_id: int, items, launch_enabled: bool):
        if self.fail:
            raise RuntimeError("synthetic launch-bundle failure")
        snapshot = [dict(item) for item in items]
        self.calls.append((int(radio_id), snapshot, bool(launch_enabled)))
        return {"radio_profile_id": int(radio_id), "items": snapshot, "launch_enabled": bool(launch_enabled)}


def _settings_tab_for_persistence(store: MultiRadioStore, launch) -> SettingsTab:
    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = store
    tab.launch_orchestrator = launch
    tab._last_persisted_device_profile = None
    tab._confirm_runtime_projection_override = lambda _title: True
    tab._refresh_multi_radio_tables = lambda *args, **kwargs: None
    tab._refresh_runtime_projection_ui = lambda *args, **kwargs: None
    tab._emit_device_profiles_changed = lambda *args, **kwargs: None
    tab._set_save_button_state = lambda *args, **kwargs: None
    tab._settings_dirty = False
    return tab


def _observer_values() -> dict[str, object]:
    profile = {
        "name": "RTL-SDR",
        "device_class": "observer",
        "control_backend": "manual",
        "enabled": 1,
        "runtime_active": 0,
        "runtime_primary": 0,
        "sdr_application": "SDR++",
        "sdr_adapter": "manual",
        "sdr_verification_state": "manual",
    }
    profile["receiver_launch_bundle"] = {
        "launch_enabled": True,
        "items": build_receiver_launch_items(
            profile,
            [{"application": "SDR++", "launch_path": "sdrpp", "startup": True}],
        ),
    }
    return profile


def _seed_primary_radio(store: MultiRadioStore) -> None:
    store.save_device_profile(
        {
            "name": "HF Radio",
            "device_class": "tx_rx",
            "control_backend": "manual",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 1,
        }
    )


def test_receiver_launch_bundle_is_saved_only_after_profile_has_real_id(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    store = MultiRadioStore(settings_db_path())
    _seed_primary_radio(store)
    launch = _LaunchRecorder()
    tab = _settings_tab_for_persistence(store, launch)

    assert tab._persist_device_profile(_observer_values()) is True

    saved = tab._last_persisted_device_profile or {}
    assert int(saved.get("id", 0) or 0) > 0
    assert int(saved.get("launch_enabled", 0) or 0) == 0
    assert str(saved.get("launch_path", "") or "") == ""
    assert len(launch.calls) == 1
    radio_id, items, enabled = launch.calls[0]
    assert radio_id == int(saved["id"])
    assert enabled is True
    assert [item["name"] for item in items] == ["SDR++"]


def test_receiver_launch_failure_is_visible_after_profile_save(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    store = MultiRadioStore(settings_db_path())
    _seed_primary_radio(store)
    tab = _settings_tab_for_persistence(store, _LaunchRecorder(fail=True))
    warnings: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "freqinout.gui.settings_tab.QMessageBox.warning",
        lambda _parent, title, detail: warnings.append((str(title), str(detail))),
    )

    assert tab._persist_device_profile(_observer_values()) is False

    saved = tab._last_persisted_device_profile or {}
    assert int(saved.get("id", 0) or 0) > 0
    assert warnings and warnings[-1][0] == "Saved — launch bundle retry required"
    assert "reviewed launch bundle still needs to be applied" in warnings[-1][1]
    assert "Settings → Radios → RTL-SDR → Launch Control" in warnings[-1][1]
