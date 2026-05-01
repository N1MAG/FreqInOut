from __future__ import annotations


def test_legacy_projection_respects_radio_software_flags() -> None:
    from freqinout.core.multi_radio_store import _legacy_settings_projection_from_device

    projected = _legacy_settings_projection_from_device(
        {
            "control_backend": "flrig",
            "use_flrig": 1,
            "use_fldigi": 0,
            "use_js8call": 0,
            "use_js8spotter": 0,
            "use_commstat": 0,
            "use_varac": 0,
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "flrig_path": "/Applications/FLRig.app",
            "js8_host": "127.0.0.1",
            "js8_port": 2442,
            "js8_install_path": "/Applications/JS8Call.app",
            "spotter_launch_path": "/Applications/JS8Spotter.app",
            "commstat_launch_path": "/Applications/CommStat.app",
            "varac_install_path": "/Applications/VarAC",
            "varac_db_path": "/Applications/VarAC/VarAC.db",
            "varac_ini_path": "/Applications/VarAC/VarAC.ini",
            "varac_incoming_path": "/Applications/VarAC/RX Files",
            "launch_enabled": 1,
        },
        {"message_paths": {"varac": "/old/varac"}},
    )

    assert projected["path_flrig"] == "/Applications/FLRig.app"
    assert projected["path_js8call"] == ""
    assert projected["path_js8spotter"] == ""
    assert projected["path_commstat"] == ""
    assert projected["varac_path"] == ""
    assert projected["message_paths"] == {}
