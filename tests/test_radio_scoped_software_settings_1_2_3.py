from __future__ import annotations

from pathlib import Path

from freqinout.gui.settings_tab import _coerce_json_mapping
from freqinout.core.multi_radio_store import (
    MultiRadioStore,
    _legacy_settings_projection_from_device,
    settings_db_path,
)
from freqinout.core.settings_manager import SettingsManager


def test_legacy_projection_includes_radio_scoped_software_paths() -> None:
    projected = _legacy_settings_projection_from_device(
        {
            "control_backend": "flrig",
            "use_flrig": 1,
            "use_fldigi": 1,
            "use_flmsg": 1,
            "use_flamp": 1,
            "use_js8call": 0,
            "use_js8spotter": 0,
            "use_commstat": 0,
            "use_varac": 1,
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "flrig_path": "/Applications/FLRig.app",
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7362,
            "fldigi_path": "/Applications/FLDigi.app",
            "flmsg_path": "/Applications/FLMsg.app",
            "flmsg_message_path": "/tmp/messages/flmsg",
            "flamp_path": "/Applications/FLAmp.app",
            "flamp_message_path": "/tmp/messages/flamp",
            "varac_install_path": "/Applications/VarAC",
            "varac_db_path": "/Applications/VarAC/VarAC.db",
            "varac_ini_path": "/Applications/VarAC/VarAC.ini",
            "varac_incoming_path": "/Applications/VarAC/RX Files",
            "varac_outbox_dir": "/Applications/VarAC/Outbox",
            "varac_bbs_dir": "/Applications/VarAC/BBS",
            "varac_bbs_archive_dir": "/Applications/VarAC/BBSArchive",
            "varac_bbs_auto_archive_enabled": 1,
            "varac_bbs_auto_archive_days": 21,
            "launch_cmd": "",
            "launch_enabled": 1,
        },
        {"message_paths": {}},
    )

    assert projected["path_flmsg"] == "/Applications/FLMsg.app"
    assert projected["path_flamp"] == "/Applications/FLAmp.app"
    assert projected["message_paths"]["flmsg"] == "/tmp/messages/flmsg"
    assert projected["message_paths"]["flamp"] == "/tmp/messages/flamp"
    assert projected["varac_outbox_dir"] == "/Applications/VarAC/Outbox"
    assert projected["varac_bbs_dir"] == "/Applications/VarAC/BBS"
    assert projected["varac_bbs_archive_dir"] == "/Applications/VarAC/BBSArchive"
    assert projected["varac_bbs_auto_archive_enabled"] is True
    assert projected["varac_bbs_auto_archive_days"] == 21


def test_device_profile_persists_radio_owned_software_fields(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = next(row for row in store.list_device_profiles() if int(row.get("runtime_primary", 0) or 0) == 1)

    saved = store.save_device_profile(
        {
            "id": int(primary["id"]),
            "name": str(primary.get("name", "") or "Default Radio"),
            "control_backend": str(primary.get("control_backend", "") or "flrig"),
            "runtime_active": int(primary.get("runtime_active", 0) or 0),
            "runtime_primary": 1,
            "use_flrig": 1,
            "use_fldigi": 1,
            "use_flmsg": 1,
            "use_flamp": 1,
            "use_varac": 1,
            "flmsg_path": "/apps/flmsg",
            "flmsg_message_path": "/msgs/flmsg",
            "flamp_path": "/apps/flamp",
            "flamp_message_path": "/msgs/flamp",
            "varac_install_path": "/varac",
            "varac_db_path": "/varac/VarAC.db",
            "varac_outbox_dir": "/varac/outbox",
            "varac_bbs_dir": "/varac/bbs",
            "varac_bbs_archive_dir": "/varac/archive",
            "varac_bbs_auto_archive_enabled": 1,
            "varac_bbs_auto_archive_days": 30,
        }
    )

    assert saved["flmsg_path"] == "/apps/flmsg"
    assert saved["flmsg_message_path"] == "/msgs/flmsg"
    assert saved["flamp_path"] == "/apps/flamp"
    assert saved["flamp_message_path"] == "/msgs/flamp"
    assert saved["varac_outbox_dir"] == "/varac/outbox"
    assert saved["varac_bbs_dir"] == "/varac/bbs"
    assert saved["varac_bbs_archive_dir"] == "/varac/archive"
    assert int(saved["varac_bbs_auto_archive_days"]) == 30


def test_coerce_json_mapping_accepts_stored_json_text() -> None:
    assert _coerce_json_mapping("") == {}
    assert _coerce_json_mapping("{}") == {}
    assert _coerce_json_mapping('{"active_request":"INTEL"}') == {"active_request": "INTEL"}
    assert _coerce_json_mapping("[]") == {}


def test_settings_tab_source_mentions_radio_scoped_software_view() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "Radio Software View" in source
    assert "These software pages stay in the familiar single-rig layout" in source
    assert "Launch Control and operating status still follow the Station Default compatibility projection." in source
