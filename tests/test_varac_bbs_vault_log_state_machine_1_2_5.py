from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from freqinout.core.varac_bbs_vault import (
    DEFAULT_LOCATION_ID,
    DEFAULT_LOCATION_NAME,
    hash_access_code,
    initialize_managed_root,
    load_vault_runtime_state,
    parse_vault_log_events,
    run_varac_bbs_vault,
)


class _Settings:
    def __init__(self, **values):
        self._data = dict(values)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value


def _create_varac_db(path: Path, *, command: str = "AIB") -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE qso (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guid TEXT NOT NULL,
            callsign TEXT NOT NULL,
            my_callsign TEXT NOT NULL,
            starttime DATETIME NOT NULL,
            endtime DATETIME NOT NULL
        );
        CREATE TABLE datastream (
            id INTEGER PRIMARY KEY,
            guid TEXT NOT NULL,
            datastream_entry_type_id INTEGER NOT NULL,
            qso_guid TEXT,
            callsign TEXT,
            entry TEXT,
            creation_time DATETIME NOT NULL,
            is_deleted BOOLEAN NOT NULL DEFAULT 0
        );
        """
    )
    conn.execute(
        "INSERT INTO qso(guid,callsign,my_callsign,starttime,endtime) VALUES(?,?,?,?,?)",
        ("qso-1", "W0IFM", "N1MAG", "2026-05-25 17:36:27", "2026-05-25 19:08:28"),
    )
    conn.execute(
        "INSERT INTO datastream(id,guid,datastream_entry_type_id,qso_guid,callsign,entry,creation_time,is_deleted) VALUES(?,?,?,?,?,?,?,0)",
        (1, "ds-1", 1, "qso-1", "W0IFM", command, "2026-05-25 17:46:05.0000000Z"),
    )
    conn.commit()
    conn.close()


def _base_settings(tmp_path: Path, *, flamp: bool = False) -> tuple[_Settings, Path, Path]:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    locations_dir = Path(created["locations"])
    default_dir = Path(created["default"])
    intel_dir = locations_dir / "Intel"
    aib_dir = locations_dir / "AIB"
    hubs_dir = locations_dir / "HUBS"
    test_dir = locations_dir / "TestCode"
    for directory in (intel_dir, aib_dir, hubs_dir, test_dir):
        directory.mkdir(parents=True, exist_ok=True)
    (intel_dir / "Intel_Report.txt").write_text("intel", encoding="utf-8")
    (aib_dir / "AIB_Report.txt").write_text("aib", encoding="utf-8")
    (hubs_dir / "HUBS_Report.txt").write_text("hubs", encoding="utf-8")
    (test_dir / "TEST_Report.txt").write_text("test", encoding="utf-8")
    hubs_code = hash_access_code("MRHUB")
    test_code = hash_access_code("SPILLBEANS")
    flamp_dir = tmp_path / "flamp-relay"
    flamp_dir.mkdir()
    settings = _Settings(
        operator_callsign="N1MAG",
        callsign="N1MAG",
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_path=str(varac_root),
        varac_db_path=str(varac_root / "VarAC.db"),
        varac_bbs_vault_managed_root=str(managed_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
        varac_bbs_vault_return_mode="On disconnect",
        varac_bbs_vault_failed_attempt_limit=3,
        varac_bbs_vault_failed_attempt_window_seconds=900,
        varac_bbs_vault_cooldown_seconds=1800,
        varac_bbs_vault_flamp_enabled=flamp,
        varac_bbs_vault_flamp_relay_dir=str(flamp_dir),
        varac_bbs_vault_locations_v1=[
            {
                "id": DEFAULT_LOCATION_ID,
                "name": DEFAULT_LOCATION_NAME,
                "source_dir": str(default_dir),
                "alias": "ROOT",
                "enabled": True,
                "list_in_root_menu": False,
                "visibility_rule": "Public",
                "open_rule": "Public",
            },
            {
                "id": "intel",
                "name": "Intel",
                "source_dir": str(intel_dir),
                "alias": "INTEL",
                "description": "open Intel",
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
            },
            {
                "id": "aib",
                "name": "AIB",
                "source_dir": str(aib_dir),
                "alias": "AIB",
                "description": "open AIB",
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
            },
            {
                "id": "hubs",
                "name": "HUBS",
                "source_dir": str(hubs_dir),
                "alias": "HUBS",
                "description": "open HUBS",
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Allowed callsigns + access code",
                "inherit_global_allowed_callsigns": True,
                "access_code_hash": hubs_code["access_code_hash"],
                "access_code_salt": hubs_code["access_code_salt"],
                "access_code_iterations": hubs_code["access_code_iterations"],
            },
            {
                "id": "test",
                "name": "TestCode",
                "source_dir": str(test_dir),
                "alias": "TEST",
                "description": "open TEST",
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Allowed callsigns + access code",
                "inherit_global_allowed_callsigns": True,
                "access_code_hash": test_code["access_code_hash"],
                "access_code_salt": test_code["access_code_salt"],
                "access_code_iterations": test_code["access_code_iterations"],
            },
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="",
        varac_bbs_limit_access_enabled=False,
    )
    return settings, varac_root, live_bbs


def _names(path: Path) -> set[str]:
    return {child.name for child in path.iterdir() if child.is_file()}


def _append_log(path: Path, lines: list[str]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for line in lines:
            handle.write(line.rstrip("\n") + "\n")


def test_managed_bbs_log_cursor_keeps_view_until_new_command_or_disconnect(tmp_path: Path) -> None:
    settings, varac_root, live_bbs = _base_settings(tmp_path)
    log_path = varac_root / "VarAC_traffic.log"
    _append_log(
        log_path,
        [
            "25/05/2026 17:36:27 - CONNECTED TO W0IFM (BANDWIDTH: 500 FREQUENCY: 14.115.000)",
            "25/05/2026 17:37:00 - W0IFM> <BLR>",
            "25/05/2026 17:46:05 - W0IFM> INTEL",
        ],
    )

    run_varac_bbs_vault(settings)
    assert "Intel_Report.txt" in _names(live_bbs)
    assert load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1")).current_location_id == "intel"

    run_varac_bbs_vault(settings)
    assert "Intel_Report.txt" in _names(live_bbs)

    _append_log(log_path, ["25/05/2026 17:49:26 - W0IFM> <BLR>"])
    run_varac_bbs_vault(settings)
    assert "Intel_Report.txt" in _names(live_bbs)

    _append_log(log_path, ["25/05/2026 18:04:47 - W0IFM> AIB"])
    run_varac_bbs_vault(settings)
    names = _names(live_bbs)
    assert "AIB_Report.txt" in names
    assert "Intel_Report.txt" not in names

    _append_log(log_path, ["25/05/2026 19:08:28 - DISCONNECTED FROM W0IFM"])
    run_varac_bbs_vault(settings)
    names = _names(live_bbs)
    assert any("type INTEL" in name for name in names)
    assert "AIB_Report.txt" not in names


def test_managed_bbs_ignores_varac_db_when_traffic_log_exists(tmp_path: Path) -> None:
    settings, varac_root, live_bbs = _base_settings(tmp_path)
    _create_varac_db(varac_root / "VarAC.db", command="AIB")
    (varac_root / "VarAC_traffic.log").write_text(
        "25/05/2026 17:36:27 - CONNECTED TO W0IFM (BANDWIDTH: 500 FREQUENCY: 14.115.000)\n",
        encoding="utf-8",
    )

    run_varac_bbs_vault(settings)
    names = _names(live_bbs)
    assert "AIB_Report.txt" not in names


def test_flamp_block_overlay_survives_refresh_and_recreates_live_file(tmp_path: Path) -> None:
    settings, varac_root, live_bbs = _base_settings(tmp_path, flamp=True)
    relay_dir = Path(settings.get("varac_bbs_vault_flamp_relay_dir"))
    (relay_dir / "E957_MAGNET-S2-RR.sig.b2s").write_text(
        "<PROG 1.0>{E957}\n<SIZE xx>{E957}2119 2 1024\n{E957:1}BLOCK1\n{E957:2}BLOCK2\n",
        encoding="utf-8",
    )
    log_path = varac_root / "VarAC_traffic.log"
    _append_log(
        log_path,
        [
            "25/05/2026 21:40:00 - CONNECTED TO N5TNT (BANDWIDTH: 500 FREQUENCY: 14.115.000)",
            "25/05/2026 21:40:43 - N5TNT> BLK 0,1 E957",
            "25/05/2026 21:40:51 - N5TNT> <BLR>",
        ],
    )

    run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1"))
    assert state.current_view_mode == "flamp-block-overlay"
    assert state.current_overlay_file == "BBS_E957_BLK_0_1.txt"
    assert state.current_session_callsign == "N5TNT"
    assert state.current_overlay_file in _names(live_bbs)

    (live_bbs / state.current_overlay_file).unlink()
    run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1"))
    assert state.current_view_mode == "flamp-block-overlay"
    assert state.current_overlay_file == "BBS_E957_BLK_0_1.txt"
    assert state.current_overlay_file in _names(live_bbs)

    _append_log(log_path, ["25/05/2026 21:45:14 - N5TNT> <BLR>"])
    run_varac_bbs_vault(settings)
    assert "BBS_E957_BLK_0_1.txt" in _names(live_bbs)


def test_public_visible_code_location_is_listed_but_access_is_enforced(tmp_path: Path) -> None:
    settings, varac_root, live_bbs = _base_settings(tmp_path)
    (varac_root / "VarAC_traffic.log").write_text("25/05/2026 17:37:00 - W0IFM> <BLR>\n", encoding="utf-8")

    run_varac_bbs_vault(settings)
    assert any("type TEST [CODE]" in name for name in _names(live_bbs))


def test_bracketed_case_insensitive_access_code_opens_location(tmp_path: Path) -> None:
    settings, varac_root, live_bbs = _base_settings(tmp_path)
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "25/05/2026 17:36:27 - CONNECTED TO W0IFM (BANDWIDTH: 500 FREQUENCY: 14.115.000)",
                "25/05/2026 18:24:39 - W0IFM> hubs [mrhub]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    run_varac_bbs_vault(settings)
    assert "HUBS_Report.txt" in _names(live_bbs)


def test_flamp_view_does_not_blend_with_current_location_files(tmp_path: Path) -> None:
    settings, varac_root, live_bbs = _base_settings(tmp_path, flamp=True)
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "25/05/2026 17:36:27 - CONNECTED TO W0IFM (BANDWIDTH: 500 FREQUENCY: 14.115.000)",
                "25/05/2026 17:49:01 - W0IFM> HUBS MRHUB",
                "25/05/2026 18:27:29 - W0IFM> FLAMP",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    run_varac_bbs_vault(settings)
    names = _names(live_bbs)
    assert any("LIST Q" in name for name in names)
    assert "HUBS_Report.txt" not in names


def test_production_w0ifm_log_contains_expected_managed_bbs_commands() -> None:
    real_log = Path("/Users/bill/RadioTools/Programs/VarAC_files/0525files2/VarAC_traffic.log")
    if not real_log.exists():
        pytest.skip("Production W0IFM/N1MAG VarAC traffic log fixture is not available on this machine")
    events = parse_vault_log_events(
        real_log.read_text(encoding="utf-8", errors="replace"),
        alias_map={"INTEL": "intel", "AIB": "aib", "HUBS": "hubs", "TEST": "test"},
        local_callsigns=["N1MAG"],
    )
    w0ifm = [event for event in events if event.sender == "W0IFM"]
    assert any(event.kind == "root_request" for event in w0ifm)
    assert any(event.kind == "open_alias" and event.alias == "INTEL" for event in w0ifm)
    assert any(event.kind == "open_alias" and event.alias == "AIB" for event in w0ifm)
    assert any(event.kind == "open_alias" and event.alias == "HUBS" and event.code_text == "MRHUB" for event in w0ifm)
    assert any(event.kind == "flamp_help" for event in w0ifm)
