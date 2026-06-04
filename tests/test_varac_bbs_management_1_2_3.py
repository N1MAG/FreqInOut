from __future__ import annotations

from pathlib import Path

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.varac_bbs_config import (
    get_varac_ini_sync_state,
    load_varac_bbs_config,
    varac_ini_sync_state_matches,
    varac_ini_sync_state_to_json,
    write_varac_bbs_config,
)


def test_load_varac_bbs_config_reads_bbs_section(tmp_path: Path) -> None:
    ini_path = tmp_path / "VarAC.ini"
    ini_path.write_text(
        "[BBS]\n"
        "EnableBBS=ON\n"
        "BBSDirectory=C:\\\\users\\\\bill\\\\Desktop\\\\VaraFiles\\\\BBS\n"
        "LimitAccessToCallsigns=ON\n"
        "LimitAccessToCallsignsList=K7RIE, KG5RKW, W8UFO\n"
        "Announce=ON\n",
        encoding="utf-8",
    )

    cfg = load_varac_bbs_config(ini_path)

    assert cfg["enable_bbs"] is True
    assert cfg["allowed_callsigns"] == ["K7RIE", "KG5RKW", "W8UFO"]


def test_load_varac_bbs_config_handles_case_variant_keys(tmp_path: Path) -> None:
    ini_path = tmp_path / "VarAC.ini"
    ini_path.write_text(
        "[bbs]\n"
        "enablebbs=on\n"
        "bbsdirectory=C:\\mixed\\case\\bbs\n"
        "limitaccesstocallsigns=on\n"
        "limitaccesstocallsignslist=kg5rkw, w8ufo\n"
        "announce=on\n",
        encoding="utf-8",
    )

    cfg = load_varac_bbs_config(ini_path)

    assert cfg["enable_bbs"] is True
    assert cfg["bbs_directory"] == r"C:\mixed\case\bbs"
    assert cfg["limit_access"] is True
    assert cfg["allowed_callsigns"] == ["KG5RKW", "W8UFO"]
    assert cfg["announce"] is True


def test_multi_radio_store_persists_and_projects_varac_bbs_access_fields(tmp_path: Path) -> None:
    store = MultiRadioStore(db_path=tmp_path / "freqinout.db")
    saved = store.save_device_profile(
        {
            "name": "Desk Radio",
            "control_backend": "flrig",
            "runtime_primary": 1,
            "runtime_active": 1,
            "use_varac": 1,
            "varac_install_path": "/opt/varac",
            "varac_ini_path": "/opt/varac/VarAC.ini",
            "varac_bbs_dir": "/opt/varac/BBS",
            "varac_bbs_enabled": 1,
            "varac_bbs_limit_access_enabled": 1,
            "varac_bbs_allowed_callsigns": "K7RIE,KG5RKW,W8UFO",
            "varac_bbs_announce_enabled": 1,
        }
    )

    projected = store.sync_runtime_active_device_to_legacy_settings(int(saved["id"]))

    assert projected["varac_bbs_enabled"] is True
    assert projected["varac_bbs_limit_access_enabled"] is True
    assert projected["varac_bbs_allowed_callsigns"] == "K7RIE,KG5RKW,W8UFO"
    assert projected["varac_bbs_announce_enabled"] is True


def test_messages_bbs_status_text_is_runtime_projected_context(tmp_path: Path) -> None:
    text = (Path(__file__).resolve().parents[1] / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert "runtime-projected default context" in text
    assert "VarAC BBS (runtime-projected default context):" in text


def test_multi_rig_varac_bbs_write_back_preserves_other_sections_and_tracks_sync_state(tmp_path: Path) -> None:
    ini_path = tmp_path / "VarAC.ini"
    ini_path.write_text(
        "[General]\n"
        "Nickname=Desk\n"
        "\n"
        "[BBS]\n"
        "EnableBBS=OFF\n"
        "BBSDirectory=C:\\old\\bbs\n"
        "LimitAccessToCallsigns=OFF\n"
        "LimitAccessToCallsignsList=\n"
        "Announce=OFF\n"
        "\n"
        "[Other]\n"
        "Value=KeepMe\n",
        encoding="utf-8",
    )

    loaded_state = get_varac_ini_sync_state(ini_path)
    assert varac_ini_sync_state_matches(loaded_state, varac_ini_sync_state_to_json(loaded_state))

    updated_state = write_varac_bbs_config(
        ini_path,
        enable_bbs=True,
        bbs_directory=r"C:\new\bbs",
        limit_access=True,
        allowed_callsigns="k7rie, kg5rkw",
        announce=True,
        expected_sync_state=loaded_state,
    )

    assert updated_state.path == str(ini_path)
    assert load_varac_bbs_config(ini_path)["allowed_callsigns"] == ["K7RIE", "KG5RKW"]
    rewritten = ini_path.read_text(encoding="utf-8")
    assert "[General]" in rewritten
    assert "Nickname=Desk" in rewritten
    assert "[Other]" in rewritten
    assert "Value=KeepMe" in rewritten
    assert "EnableBBS=ON" in rewritten
    assert r"BBSDirectory=C:\new\bbs" in rewritten


def test_write_varac_bbs_config_updates_existing_case_variant_section(tmp_path: Path) -> None:
    ini_path = tmp_path / "VarAC.ini"
    ini_path.write_text(
        "[general]\n"
        "Nickname=Desk\n"
        "\n"
        "[bbs]\n"
        "enablebbs=off\n"
        "bbsdirectory=C:\\old\\bbs\n"
        "limitaccesstocallsigns=off\n"
        "limitaccesstocallsignslist=\n"
        "announce=off\n",
        encoding="utf-8",
    )

    loaded_state = get_varac_ini_sync_state(ini_path)
    updated_state = write_varac_bbs_config(
        ini_path,
        enable_bbs=True,
        bbs_directory=r"C:\new\bbs",
        limit_access=True,
        allowed_callsigns="k7rie, kg5rkw",
        announce=True,
        expected_sync_state=loaded_state,
    )

    assert updated_state.path == str(ini_path)
    cfg = load_varac_bbs_config(ini_path)
    assert cfg["bbs_directory"] == r"C:\new\bbs"
    assert cfg["allowed_callsigns"] == ["K7RIE", "KG5RKW"]
    rewritten = ini_path.read_text(encoding="utf-8")
    assert "[general]" in rewritten
    assert "Nickname=Desk" in rewritten
    assert "enablebbs=ON" in rewritten or "EnableBBS=ON" in rewritten
    assert r"bbsdirectory=C:\new\bbs" in rewritten or r"BBSDirectory=C:\new\bbs" in rewritten
