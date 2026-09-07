from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.mesh import (
    MeshChannel,
    MeshChannelCapabilities,
    default_mesh_db_path,
    list_mesh_channel_policies,
)


def _mesh_settings_tab(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab()
    tab.mesh_enabled_chk.setChecked(True)
    tab._set_combo_data_if_present(tab.mesh_protocol_combo, "meshcore", fallback="meshtastic")
    tab.mesh_adapter_id_edit.setText("meshcore-field")
    tab._stage_mesh_default_channels()
    app.processEvents()
    return app, tab


def test_settings_channel_admin_routes_refresh_and_incremental_results(monkeypatch, tmp_path) -> None:
    app, tab = _mesh_settings_tab(monkeypatch, tmp_path)
    refreshes: list[str] = []
    tab.mesh_channel_refresh_requested.connect(refreshes.append)

    tab.mesh_channel_admin.refresh_button.click()
    tab.on_mesh_channels_ready(
        "meshcore-field",
        (
            MeshChannel(
                adapter_id="meshcore-field",
                transport="meshcore",
                index=3,
                name="Regional Ops",
                role="private",
                channel_id="3",
                privacy="encrypted",
            ),
        ),
    )
    tab.on_mesh_channel_capabilities_ready(
        "meshcore-field",
        MeshChannelCapabilities(can_configure=True, can_remove_from_device=True, guidance=""),
    )
    app.processEvents()

    assert refreshes == ["meshcore-field"]
    assert any(
        item.startswith("Regional Ops")
        for item in [
            tab.mesh_channel_admin.channel_list.item(i).text()
            for i in range(tab.mesh_channel_admin.channel_list.count())
        ]
    )
    assert tab.mesh_channel_admin.configure_button.isEnabled()
    assert tab.mesh_channel_admin.remove_device_button.isEnabled()


def test_remove_from_fio_archives_policy_without_device_request(monkeypatch, tmp_path) -> None:
    app, tab = _mesh_settings_tab(monkeypatch, tmp_path)
    device_requests: list[tuple[str, str]] = []
    tab.mesh_channel_remove_device_requested.connect(lambda *args: device_requests.append(args))
    tab.mesh_channel_admin.channel_list.setCurrentRow(0)
    selected_text = tab.mesh_channel_admin.channel_list.currentItem().text()
    tab.mesh_channel_admin.remove_fio_button.click()
    app.processEvents()

    policies = list_mesh_channel_policies(
        default_mesh_db_path(),
        adapter_id="meshcore-field",
        transport="meshcore",
    )
    archived = next(policy for policy in policies if policy.display_name in selected_text)
    assert archived.source == "archived"
    assert archived.review_state == "ignored"
    assert archived.inbox_enabled is False
    assert device_requests == []


def test_main_window_wires_channel_admin_to_worker_without_hiding_control_bar() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    assert "worker.channels_ready.connect(self.settings_tab.on_mesh_channels_ready)" in source
    assert "worker.operation_ready.connect(self.settings_tab.on_mesh_operation_ready)" in source
    assert "self.settings_tab.mesh_channel_refresh_requested.connect(worker.refresh_channels" in source
    assert "self.station_command_bar.hide()" not in source
    assert "self._mesh_runtime_restart_pending" in source
    assert "self._disconnect_mesh_runtime()" in source


def test_policy_update_accepts_comma_delimited_group_text(monkeypatch, tmp_path) -> None:
    app, tab = _mesh_settings_tab(monkeypatch, tmp_path)
    tab.mesh_channel_admin.channel_list.setCurrentRow(0)
    channel, _policy = tab.mesh_channel_admin._selected()
    assert channel is not None

    tab._update_mesh_channel_policy(
        "meshcore-field",
        channel.channel_id or str(channel.index),
        {"mapped_groups": "MR08, magnet"},
    )
    app.processEvents()

    updated = tab._mesh_channel_policy_for(
        "meshcore-field",
        channel.channel_id or str(channel.index),
    )
    assert updated is not None
    assert updated.mapped_groups == ("MR08", "MAGNET")
