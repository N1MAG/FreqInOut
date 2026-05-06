from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

from freqinout.core.varac_bbs_vault import (
    DEFAULT_GLOBAL_CODE_POLICY,
    DEFAULT_LOCATION_ID,
    DEFAULT_LOCATION_NAME,
    FlampRelayStore,
    VaultLocation,
    VaultRuntimeState,
    compute_default_managed_root,
    hash_access_code,
    import_live_bbs_to_default_location,
    initialize_managed_root,
    load_vault_runtime_state,
    parse_vault_log_events,
    publish_flamp_block_overlay_view,
    publish_root_view,
    reset_to_default_location,
    run_varac_bbs_vault,
    verify_access_code,
)
from freqinout.core.varac_guard import parse_varac_transfer_events
from freqinout.core.varac_log_parser import parse_varac_event_timestamp


class _Settings:
    def __init__(self, **values):
        self._data = dict(values)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value


def _create_varac_db(path: Path) -> None:
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
    conn.commit()
    conn.close()


def _insert_qso(path: Path, *, guid: str, remote: str, mine: str = "N1MAG") -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO qso (guid, callsign, my_callsign, starttime, endtime) VALUES (?, ?, ?, ?, ?)",
        (guid, remote, mine, "2026-05-02 14:09:08", "2026-05-02 14:35:29"),
    )
    conn.commit()
    conn.close()


def _insert_datastream(path: Path, rows) -> None:
    conn = sqlite3.connect(path)
    conn.executemany(
        """
        INSERT INTO datastream
            (id, guid, datastream_entry_type_id, qso_guid, callsign, entry, creation_time, is_deleted)
        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def test_hash_and_verify_access_code_round_trip() -> None:
    payload = hash_access_code("MAGNET-OPS")
    assert payload["access_code_hash"]
    assert verify_access_code(
        "MAGNET-OPS",
        access_code_hash=str(payload["access_code_hash"]),
        access_code_salt=str(payload["access_code_salt"]),
        access_code_iterations=int(payload["access_code_iterations"]),
    )
    assert not verify_access_code(
        "WRONG-CODE",
        access_code_hash=str(payload["access_code_hash"]),
        access_code_salt=str(payload["access_code_salt"]),
        access_code_iterations=int(payload["access_code_iterations"]),
    )


def test_varac_log_parser_honors_day_first_and_month_first_inputs() -> None:
    euro = parse_varac_event_timestamp("13/04/2025 04:07:31")
    assert euro is not None
    assert (euro.year, euro.month, euro.day) == (2025, 4, 13)

    month_first = parse_varac_event_timestamp("04/03/2025 12:34:56", prefer_day_first=False)
    day_first = parse_varac_event_timestamp("04/03/2025 12:34:56", prefer_day_first=True)
    assert month_first is not None and day_first is not None
    assert (month_first.month, month_first.day) == (4, 3)
    assert (day_first.month, day_first.day) == (3, 4)


def test_varac_guard_and_vault_timestamp_parsers_accept_day_first_logs() -> None:
    guard_events = parse_varac_transfer_events(
        "13/04/2025 04:07:31 - FILE SUCCESSFULLY RECEIVED: file://C:\\VarAC\\\\Report.k2s (Size: 284 Bytes)\n",
        log_path="VarAC_traffic.log",
    )
    assert guard_events
    assert guard_events[0].timestamp_utc > 0

    vault_events = parse_vault_log_events(
        "13/04/2025 04:07:31 - BBS OPEN TEST_A\n",
        log_path="VarAC_traffic.log",
    )
    assert vault_events
    assert vault_events[0].timestamp_utc > 0


def test_initialize_and_import_live_bbs(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    (live_bbs / "Report.k2s").write_text("payload", encoding="utf-8")
    managed_root = tmp_path / "FIO_BBS_Vault"

    created = initialize_managed_root(managed_root)
    assert Path(created["default"]).exists()

    copied = import_live_bbs_to_default_location(live_bbs, created["default"])
    assert copied == 1
    assert (Path(created["default"]) / "Report.k2s").exists()
    assert compute_default_managed_root(live_bbs).endswith("FIO_BBS_Vault")


def test_publish_root_view_respects_callsign_visibility(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    docs_dir = Path(created["locations"]) / "DocDrop"
    intel_dir.mkdir(parents=True)
    docs_dir.mkdir(parents=True)
    (default_dir / "Main.txt").write_text("root", encoding="utf-8")
    (intel_dir / "Intel-1.b2s").write_text("intel", encoding="utf-8")
    (docs_dir / "DocDrop-1.k2s").write_text("docs", encoding="utf-8")

    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
        VaultLocation(
            id="docdrop",
            name="DocDrop",
            source_dir=str(docs_dir),
            alias="DOCDROP",
            description="For latest DocDrop files",
            open_rule="Public",
        ),
        VaultLocation(
            id="intel",
            name="Intel",
            source_dir=str(intel_dir),
            alias="INTEL",
            description="For latest Magnet S2 reports",
            open_rule="Allowed callsigns + access code",
            visibility_rule="Allowed callsigns only",
            allowed_callsigns=("W5TTA",),
        ),
    ]

    publish_root_view(
        sender="W5TTA",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=("W5TTA",),
        limit_access_enabled=True,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=False,
    )
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("DOCDROP" in name for name in names)
    assert any("INTEL" in name for name in names)

    publish_root_view(
        sender="KX9ZZZ",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=("W5TTA",),
        limit_access_enabled=True,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=False,
    )
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("DOCDROP" in name for name in names)
    assert not any("INTEL" in name for name in names)


def test_reset_to_default_uses_configured_root_visibility_policy(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    intel_dir.mkdir(parents=True)
    (intel_dir / "Intel-1.b2s").write_text("intel", encoding="utf-8")
    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
        VaultLocation(
            id="intel",
            name="Intel",
            source_dir=str(intel_dir),
            alias="INTEL",
            description="For latest Magnet S2 reports",
            open_rule="Allowed callsigns + access code",
            visibility_rule="Public",
        ),
    ]

    reset_to_default_location(
        locations=locations,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        default_location_id=DEFAULT_LOCATION_ID,
        runtime_state=VaultRuntimeState(current_location_id="intel", current_session_callsign="W5TTA"),
        global_allowed_callsigns=(),
        limit_access_enabled=False,
        global_code_policy="Allow public locations",
    )

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("INTEL" in name for name in names)
    assert any(name.endswith(".txt") for name in names)


def test_reset_to_default_resolves_wine_bbs_paths(tmp_path: Path) -> None:
    wineprefix = tmp_path / ".wine"
    live_bbs = wineprefix / "drive_c" / "users" / "bill" / "Desktop" / "VaraFiles" / "BBS"
    live_bbs.mkdir(parents=True)
    managed_root = wineprefix / "drive_c" / "users" / "bill" / "Desktop" / "VaraFiles" / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    intel_dir.mkdir(parents=True)
    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
        VaultLocation(
            id="intel",
            name="Intel",
            source_dir=str(intel_dir),
            alias="INTEL",
            description="For latest Magnet S2 reports",
            open_rule="Public",
            visibility_rule="Public",
        ),
    ]

    with patch.dict("os.environ", {"WINEPREFIX": str(wineprefix)}):
        reset_to_default_location(
            locations=locations,
            live_bbs_dir=r"C:\users\bill\Desktop\VaraFiles\BBS",
            managed_root=str(managed_root),
            default_location_id=DEFAULT_LOCATION_ID,
            runtime_state=VaultRuntimeState(current_location_id="intel", current_session_callsign="W5TTA"),
            global_code_policy="Allow public locations",
        )

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("INTEL" in name for name in names)


def test_run_varac_bbs_vault_processes_alias_navigation_from_varac_db(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "dce9b400-546c-401b-9876-d62f03c6ca74"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "W5TTA", "INTEL BLUEBELL", "2026-05-02 14:12:49.963574Z"),
            (3, "c", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:12:55.0000000Z"),
        ],
    )

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    intel_dir.mkdir(parents=True)
    (default_dir / "RootInfo.txt").write_text("root", encoding="utf-8")
    (intel_dir / "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s").write_text("intel", encoding="utf-8")
    code_payload = hash_access_code("BLUEBELL")

    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_db_path=str(varac_db),
        varac_path=str(varac_root),
        varac_bbs_vault_managed_root=str(managed_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
        varac_bbs_vault_global_code_policy="Require for non-default locations",
        varac_bbs_vault_trigger_mode="VarAC session commands",
        varac_bbs_vault_return_mode="On disconnect",
        varac_bbs_vault_failed_attempt_limit=3,
        varac_bbs_vault_failed_attempt_window_seconds=900,
        varac_bbs_vault_cooldown_seconds=1800,
        varac_bbs_vault_idle_timeout_seconds=600,
        varac_bbs_vault_flamp_enabled=False,
        varac_bbs_vault_flamp_relay_dir="",
        varac_bbs_vault_locations_v1=[
            {
                "id": DEFAULT_LOCATION_ID,
                "name": DEFAULT_LOCATION_NAME,
                "alias": "ROOT",
                "description": "Main menu",
                "source_dir": str(default_dir),
                "enabled": True,
                "list_in_root_menu": False,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            },
            {
                "id": "intel",
                "name": "Intel",
                "alias": "INTEL",
                "description": "For latest Magnet S2 reports",
                "source_dir": str(intel_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Allowed callsigns only",
                "open_rule": "Allowed callsigns + access code",
                "inherit_global_allowed_callsigns": False,
                "allowed_callsigns": ["W5TTA"],
                "access_code_hash": str(code_payload["access_code_hash"]),
                "access_code_salt": str(code_payload["access_code_salt"]),
                "access_code_iterations": int(code_payload["access_code_iterations"]),
            },
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="W5TTA",
        varac_bbs_limit_access_enabled=True,
    )

    result = run_varac_bbs_vault(settings)

    assert result.enabled
    assert result.processed_events >= 2
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_location_id == "intel"
    assert state.current_session_callsign == "W5TTA"
    assert state.current_view_mode == "location"
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s" in names
    assert any("ROOT" in name for name in names)


def test_run_varac_bbs_vault_processes_flamp_commands(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-flamp-1"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "W5TTA", "LIST Q", "2026-05-02 14:10:21.0000000Z"),
            (3, "c", 1, qso_guid, "W5TTA", "LIST BLKS F277", "2026-05-02 14:10:24.0000000Z"),
            (4, "d", 1, qso_guid, "W5TTA", "BLK 0,1 F277", "2026-05-02 14:10:27.0000000Z"),
        ],
    )

    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    relay_path = relay_dir / "F277_MAGNET-S2-RR-260502.sig.b2s"
    relay_path.write_text(
        "<PROG 1.0>{F277}\n<SIZE xx>{F277}2119 2 1024\n{F277:1}BLOCK1\n{F277:2}BLOCK2\n",
        encoding="utf-8",
    )
    store = FlampRelayStore(relay_dir)
    assert "F277" in store.queue_index()

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    (default_dir / "RootInfo.txt").write_text("root", encoding="utf-8")

    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_db_path=str(varac_db),
        varac_path=str(varac_root),
        varac_bbs_vault_managed_root=str(managed_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
        varac_bbs_vault_global_code_policy="Allow public locations",
        varac_bbs_vault_trigger_mode="VarAC session commands",
        varac_bbs_vault_return_mode="On disconnect",
        varac_bbs_vault_failed_attempt_limit=3,
        varac_bbs_vault_failed_attempt_window_seconds=900,
        varac_bbs_vault_cooldown_seconds=1800,
        varac_bbs_vault_idle_timeout_seconds=600,
        varac_bbs_vault_flamp_enabled=True,
        varac_bbs_vault_flamp_relay_dir=str(relay_dir),
        varac_bbs_vault_locations_v1=[
            {
                "id": DEFAULT_LOCATION_ID,
                "name": DEFAULT_LOCATION_NAME,
                "alias": "ROOT",
                "description": "Main menu",
                "source_dir": str(default_dir),
                "enabled": True,
                "list_in_root_menu": False,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            }
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="W5TTA",
        varac_bbs_limit_access_enabled=True,
    )

    result = run_varac_bbs_vault(settings)
    assert result.enabled
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_view_mode == "flamp-block-overlay"
    assert state.current_overlay_file.startswith("BBS_F277_BLK_0_1")
    assert (live_bbs / state.current_overlay_file).exists()

    (live_bbs / state.current_overlay_file).unlink()
    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_view_mode in {"root", "location"}


def test_settings_tab_persists_managed_vault_configuration(monkeypatch, tmp_path: Path) -> None:
    if sys.platform == "darwin":
        import pytest

        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    default_dir = managed_root / "locations" / "default"
    default_dir.mkdir(parents=True)

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab()
    tab.varac_bbs_dir_edit.setText(str(live_bbs))
    tab.varac_bbs_vault_enabled_chk_main.setChecked(True)
    tab.varac_bbs_vault_root_edit.setText(str(managed_root))
    tab._set_varac_bbs_vault_locations(
        [
            {
                "id": DEFAULT_LOCATION_ID,
                "name": DEFAULT_LOCATION_NAME,
                "alias": "ROOT",
                "description": "Main menu",
                "source_dir": str(default_dir),
                "enabled": True,
                "list_in_root_menu": False,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            }
        ]
    )
    tab.varac_bbs_vault_global_code_policy_combo.setCurrentText("Require for non-default locations")
    tab.varac_bbs_vault_flamp_enabled_chk.setChecked(True)
    tab.varac_bbs_vault_flamp_relay_dir_edit.setText(str(tmp_path / "relay"))

    snap = tab._settings_snapshot_for_readiness()
    assert snap["varac_bbs_vault_enabled"] is True
    assert snap["varac_bbs_vault_global_code_policy"] == "Require for non-default locations"
    assert snap["varac_bbs_vault_flamp_enabled"] is True
    assert snap["varac_bbs_vault_flamp_relay_dir"] == str(tmp_path / "relay")


def test_settings_tab_autofills_vault_location_defaults(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    live_bbs = tmp_path / "bbs"
    live_bbs.mkdir()
    (live_bbs / "Logistics.txt").write_text("test", encoding="utf-8")
    managed_root = tmp_path / "managed"

    tab = SettingsTab()
    tab.varac_bbs_dir_edit.setText(str(live_bbs))
    tab.varac_bbs_vault_root_edit.setText(str(managed_root))
    tab._new_varac_bbs_vault_location()
    tab.varac_bbs_vault_location_name_edit.setText("Logistics")

    assert tab.varac_bbs_vault_description_edit.text() == "to open Logistics"
    assert tab.varac_bbs_vault_source_dir_edit.text() == str(managed_root / "locations" / "Logistics")
    assert "Live BBS likely match: Logistics.txt" in tab.varac_bbs_vault_source_hint_label.text()


def test_settings_tab_autofills_flamp_relay_dir_but_allows_override(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab()
    first_rx = tmp_path / "FLAMP" / "rx"
    second_rx = tmp_path / "ALT" / "rx"
    custom_relay = tmp_path / "manual-relay"

    tab.msg_paths_edits["flamp"].setText(str(first_rx))
    assert tab.varac_bbs_vault_flamp_relay_dir_edit.text() == str(tmp_path / "FLAMP" / "relay")

    tab.varac_bbs_vault_flamp_relay_dir_edit.setText(str(custom_relay))
    tab.msg_paths_edits["flamp"].setText(str(second_rx))
    assert tab.varac_bbs_vault_flamp_relay_dir_edit.text() == str(custom_relay)


def test_settings_tab_syncs_default_managed_root_with_bbs_dir(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    first_bbs = tmp_path / "station-a" / "BBS"
    second_bbs = tmp_path / "station-b" / "BBS"
    first_bbs.mkdir(parents=True)
    second_bbs.mkdir(parents=True)

    tab = SettingsTab()
    tab.varac_bbs_dir_edit.setText(str(first_bbs))
    first_default = str(first_bbs.parent / "FIO_BBS_Vault")
    second_default = str(second_bbs.parent / "FIO_BBS_Vault")

    assert tab.varac_bbs_vault_root_edit.text() == first_default
    assert first_default in tab.varac_bbs_vault_root_hint_label.text()

    tab.varac_bbs_dir_edit.setText(str(second_bbs))
    assert tab.varac_bbs_vault_root_edit.text() == second_default
    assert second_default in tab.varac_bbs_vault_root_hint_label.text()


def test_settings_tab_forces_managed_root_into_bbs_area(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    first_bbs = tmp_path / "station-a" / "BBS"
    second_bbs = tmp_path / "station-b" / "BBS"
    first_bbs.mkdir(parents=True)
    second_bbs.mkdir(parents=True)
    stale_root = tmp_path / "FreqInOut-single-rig" / "FIO_BBS_Vault"

    tab = SettingsTab()
    tab.varac_bbs_dir_edit.setText(str(first_bbs))
    assert tab.varac_bbs_vault_root_edit.isReadOnly()
    tab._set_varac_bbs_vault_root_text(str(stale_root))
    tab.varac_bbs_dir_edit.setText(str(second_bbs))

    expected_default = str(second_bbs.parent / "FIO_BBS_Vault")
    assert tab.varac_bbs_vault_root_edit.text() == expected_default
    assert expected_default in tab.varac_bbs_vault_root_hint_label.text()
    assert "Automatic vault location" in tab.varac_bbs_vault_root_hint_label.text()


def test_run_varac_bbs_vault_ignores_stale_managed_root_setting(tmp_path: Path) -> None:
    live_bbs = tmp_path / "VarAC_files" / "BBS"
    live_bbs.mkdir(parents=True)
    managed_root = Path(compute_default_managed_root(live_bbs))
    stale_root = tmp_path / "FreqInOut-single-rig" / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    (default_dir / "RootInfo.txt").write_text("root", encoding="utf-8")

    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_bbs_vault_managed_root=str(stale_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
        varac_bbs_vault_global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        varac_bbs_vault_trigger_mode="VarAC session commands",
        varac_bbs_vault_return_mode="On disconnect",
        varac_bbs_vault_failed_attempt_limit=3,
        varac_bbs_vault_failed_attempt_window_seconds=900,
        varac_bbs_vault_cooldown_seconds=1800,
        varac_bbs_vault_idle_timeout_seconds=600,
        varac_bbs_vault_flamp_enabled=False,
        varac_bbs_vault_flamp_relay_dir="",
        varac_bbs_vault_locations_v1=[
            {
                "id": DEFAULT_LOCATION_ID,
                "name": DEFAULT_LOCATION_NAME,
                "alias": "ROOT",
                "description": "Main menu",
                "source_dir": str(default_dir),
                "enabled": True,
                "list_in_root_menu": False,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            }
        ],
        varac_bbs_limit_access_enabled=False,
        varac_bbs_allowed_callsigns="",
        varac_db_path="",
        varac_path="",
    )

    result = run_varac_bbs_vault(settings)
    assert result.enabled
    assert managed_root.exists()
    assert not stale_root.exists()
