from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QCheckBox, QTableWidget, QTableWidgetItem

from freqinout.core.launch_bundle_store import LaunchBundleStore
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.gui.settings_tab import SettingsTab


class _BundleWriter:
    """Small SettingsTab collaborator that persists through the production store."""

    def __init__(self, db_path) -> None:
        self._store = LaunchBundleStore(db_path)

    def set_radio_launch_bundle(self, radio_id: int, items, launch_enabled: bool):
        return self._store.save_bundle(int(radio_id), bool(launch_enabled), items)

    def get_radio_launch_bundle(self, radio_id: int):
        return self._store.get_bundle(int(radio_id))


def _item(name: str, *, startup: bool, monitor: bool) -> dict[str, object]:
    return {
        "name": name,
        "instance_key": name.casefold(),
        "enabled": True,
        "startup": startup,
        "monitor_health": monitor,
        "dependencies": [],
        "readiness_policy": {},
    }


def _make_radio(store: MultiRadioStore, name: str) -> dict[str, object]:
    return store.save_device_profile(
        {
            "name": name,
            "system_key": name.casefold(),
            "device_class": "tx_rx",
            "control_backend": "manual",
            "enabled": 1,
            "runtime_active": 1,
        }
    )


def _tab(store: MultiRadioStore, selected: list[dict[str, object]]) -> SettingsTab:
    app = QApplication.instance() or QApplication([])
    tab = SettingsTab.__new__(SettingsTab)
    tab._test_app = app  # retain the application while the test tab exists
    tab.multi_radio_store = store
    tab.launch_orchestrator = _BundleWriter(store.db_path)
    tab._launch_radio_bundle_drafts = {}
    tab._launch_items_cache = []
    tab.launch_all_with_startup_chk = QCheckBox()
    tab.launch_control_table = object()
    tab._selected_settings_radio_profile = lambda: selected[0]
    tab._sync_launch_cache_from_table = lambda: None
    return tab


def test_switching_radio_focus_keeps_each_launch_table_state_in_its_own_bundle(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    store = MultiRadioStore(settings_db_path())
    alpha = _make_radio(store, "Alpha")
    bravo = _make_radio(store, "Bravo")
    bundles = LaunchBundleStore(store.db_path)
    bundles.save_bundle(int(alpha["id"]), True, [_item("FLRig", startup=True, monitor=False)])
    bundles.save_bundle(int(bravo["id"]), False, [_item("FLRig", startup=False, monitor=True)])

    selected = [alpha]
    tab = _tab(store, selected)
    tab._load_selected_launch_radio_state()
    assert tab.launch_all_with_startup_chk.isChecked() is True
    assert tab._launch_items_cache[0]["startup"] is True
    assert tab._launch_items_cache[0]["monitor_health"] is False

    # This is the Settings focus-change sequence: stash the old table before
    # the selected profile changes, then load the newly selected radio bundle.
    tab._stash_current_launch_radio_state()
    selected[0] = bravo
    tab._load_selected_launch_radio_state()

    assert tab.launch_all_with_startup_chk.isChecked() is False
    assert tab._launch_items_cache[0]["startup"] is False
    assert tab._launch_items_cache[0]["monitor_health"] is True


def test_saving_and_reloading_radio_launch_flags_remains_isolated(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    store = MultiRadioStore(settings_db_path())
    alpha = _make_radio(store, "Alpha")
    bravo = _make_radio(store, "Bravo")
    selected = [alpha]
    tab = _tab(store, selected)

    tab.launch_all_with_startup_chk.setChecked(True)
    tab._launch_items_cache = [_item("FLRig", startup=True, monitor=False)]
    tab._stash_current_launch_radio_state()

    selected[0] = bravo
    tab.launch_all_with_startup_chk.setChecked(False)
    tab._launch_items_cache = [_item("FLRig", startup=False, monitor=True)]
    tab._stash_current_launch_radio_state()

    assert tab._persist_launch_radio_bundles() is True

    reopened = LaunchBundleStore(store.db_path)
    saved_alpha = reopened.get_bundle(int(alpha["id"]))
    saved_bravo = reopened.get_bundle(int(bravo["id"]))
    assert saved_alpha["launch_enabled"] is True
    assert saved_alpha["items"][0]["startup"] is True
    assert saved_alpha["items"][0]["monitor_health"] is False
    assert saved_bravo["launch_enabled"] is False
    assert saved_bravo["items"][0]["startup"] is False
    assert saved_bravo["items"][0]["monitor_health"] is True


def test_old_radio_table_cannot_overwrite_new_radio_cache(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    store = MultiRadioStore(settings_db_path())
    alpha = _make_radio(store, "Alpha")
    bravo = _make_radio(store, "Bravo")
    selected = [bravo]
    tab = _tab(store, selected)
    tab.launch_control_table = QTableWidget(1, 3)
    tab._launch_visible_names = ["FLRig"]
    tab._launch_table_radio_id = int(alpha["id"])
    app_item = QTableWidgetItem("FLRig")
    app_item.setData(Qt.UserRole, "fast:alpha:flrig")
    tab.launch_control_table.setItem(0, 0, app_item)
    old_monitor = QTableWidgetItem()
    old_monitor.setCheckState(Qt.Unchecked)
    tab.launch_control_table.setItem(0, 1, old_monitor)
    old_startup = QTableWidgetItem()
    old_startup.setCheckState(Qt.Checked)
    tab.launch_control_table.setItem(0, 2, old_startup)
    tab._launch_items_cache = [{
        **_item("FLRig", startup=False, monitor=True),
        "instance_key": "fast:bravo:flrig",
    }]

    SettingsTab._sync_launch_cache_from_table(tab)

    assert tab._launch_items_cache[0]["startup"] is False
    assert tab._launch_items_cache[0]["monitor_health"] is True
