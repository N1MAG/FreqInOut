from __future__ import annotations

import sqlite3
import sys
import time
import os
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
    load_vault_locations,
    load_vault_runtime_state,
    normalize_location_alias,
    parse_vault_log_events,
    publish_flamp_block_overlay_view,
    publish_flamp_queue_list_view,
    publish_root_view,
    read_publish_manifest,
    reset_to_default_location,
    run_varac_bbs_vault,
    verify_access_code,
    vault_locations_to_data,
    vault_runtime_state_to_data,
)
from freqinout.core.varac_guard import parse_varac_transfer_events, run_varac_guard
from freqinout.core.varac_log_parser import parse_varac_event_timestamp, parse_varac_event_timestamp_to_epoch


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


def test_vault_locations_preserve_local_operator_access_code_text() -> None:
    payload = hash_access_code("HUBS-17")
    locations = load_vault_locations(
        [
            {
                "id": "hubs",
                "name": "HUBS",
                "source_dir": "/tmp/hubs",
                "alias": "HUBS",
                "open_rule": "Access code required",
                "access_code_hash": payload["access_code_hash"],
                "access_code_salt": payload["access_code_salt"],
                "access_code_iterations": payload["access_code_iterations"],
                "access_code_plaintext": "HUBS-17",
            }
        ]
    )

    assert locations[0].access_code_plaintext == "HUBS-17"
    data = vault_locations_to_data(locations)
    assert data[0]["access_code_plaintext"] == "HUBS-17"
    assert verify_access_code(
        "HUBS-17",
        access_code_hash=str(data[0]["access_code_hash"]),
        access_code_salt=str(data[0]["access_code_salt"]),
        access_code_iterations=int(data[0]["access_code_iterations"]),
    )


def test_varac_guard_ignores_fio_generated_helper_files(tmp_path: Path) -> None:
    varac_base = tmp_path / "VarAC"
    incoming = tmp_path / "VaraFiles" / "Incoming"
    varac_base.mkdir(parents=True)
    incoming.mkdir(parents=True)
    helper = incoming / "BBS MSG - Type INTEL to open Intel then refresh BBS.txt"
    helper.write_text("Type INTEL to open Intel then refresh BBS.\n", encoding="utf-8")
    (varac_base / "VarAC_traffic.log").write_text(
        "06/05/2026 19:02:07 - FILE SUCCESSFULLY RECEIVED FROM W8UFO "
        "NAME: BBS MSG - Type INTEL to open Intel then refresh BBS.txt\n",
        encoding="utf-8",
    )
    settings = _Settings(
        varac_guard_enabled=True,
        varac_guard_mode="Quarantine unauthorized files",
        varac_path=str(varac_base),
        message_paths={"varac": str(incoming)},
        varac_bbs_allowed_callsigns="",
        varac_bbs_dir=str(tmp_path / "VaraFiles" / "BBS"),
    )

    result = run_varac_guard(settings, retry_seconds=1)

    assert helper.exists()
    assert result.quarantined_files == 0
    assert result.skipped_events == 1
    assert not (tmp_path / "VaraFiles" / "FIO_BBS_Vault" / "quarantine" / helper.name).exists()


def test_messages_helper_filter_hides_new_instruction_files() -> None:
    from freqinout.gui.message_viewer_tab import _is_fio_bbs_helper_file_name

    assert _is_fio_bbs_helper_file_name("00 READ FIRST - type command, wait 10 sec, refresh BBS.txt")
    assert _is_fio_bbs_helper_file_name("00 NOTICE - LIST BLKS 1AD1 received; wait 10 sec, refresh again.txt")
    assert _is_fio_bbs_helper_file_name("01 COMMANDS - type one command below.txt")
    assert _is_fio_bbs_helper_file_name("21 type HUBS [CODE] - open HUBS with access code.txt")
    assert _is_fio_bbs_helper_file_name("BBS MSG - Type INTEL to open Intel then refresh BBS.txt")
    assert not _is_fio_bbs_helper_file_name("NATL-RR-260427-1500Z-AIB-sig.k2s")


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

    alias_events = parse_vault_log_events(
        "06/05/2026 19:08:32 - W8UFO> INTEL\n",
        log_path="VarAC_traffic.log",
        alias_map={"INTEL": "intel"},
    )
    assert alias_events
    assert alias_events[0].kind == "open_alias"
    assert alias_events[0].sender == "W8UFO"
    assert alias_events[0].alias == "INTEL"

    alias_refresh_events = parse_vault_log_events(
        "06/05/2026 19:08:32 - W8UFO> INTEL\n<BLR>\n",
        log_path="VarAC_traffic.log",
        alias_map={"INTEL": "intel"},
    )
    assert alias_refresh_events
    assert alias_refresh_events[0].kind == "open_alias"
    assert alias_refresh_events[0].sender == "W8UFO"
    assert alias_refresh_events[0].alias == "INTEL"
    assert alias_refresh_events[0].code_text == ""

    root_events = parse_vault_log_events(
        "06/05/2026 19:51:12 - W8UFO> ROOT\n",
        log_path="VarAC_traffic.log",
        alias_map={"INTEL": "intel"},
    )
    assert root_events
    assert root_events[0].kind == "root_return"
    assert root_events[0].sender == "W8UFO"

    flamp_events = parse_vault_log_events(
        "06/05/2026 19:51:12 - W8UFO> de W8UFO <R-13> FLAMP\n",
        log_path="VarAC_traffic.log",
        alias_map={"INTEL": "intel"},
    )
    assert flamp_events
    assert flamp_events[0].kind == "flamp_help"
    assert flamp_events[0].sender == "W8UFO"

    list_q_events = parse_vault_log_events(
        "06/05/2026 19:52:12 - W8UFO> LIST Q\n",
        log_path="VarAC_traffic.log",
    )
    assert list_q_events
    assert list_q_events[0].kind == "flamp_list_q"

    list_blocks_events = parse_vault_log_events(
        "06/05/2026 19:53:12 - W8UFO> LIST BLKS F277\n",
        log_path="VarAC_traffic.log",
    )
    assert list_blocks_events
    assert list_blocks_events[0].kind == "flamp_list_blocks"
    assert list_blocks_events[0].queue_id == "F277"

    list_blocks_refresh_events = parse_vault_log_events(
        "09/05/2026 23:57:27 - N5TNT> LIST BLKS 1AD1\n<BLR>\n",
        log_path="VarAC_traffic.log",
    )
    assert list_blocks_refresh_events
    assert list_blocks_refresh_events[0].kind == "flamp_list_blocks"
    assert list_blocks_refresh_events[0].queue_id == "1AD1"
    assert list_blocks_refresh_events[0].refresh_requested is True

    block_events = parse_vault_log_events(
        "06/05/2026 19:54:12 - W8UFO> BLK 0,8,9 F277\n",
        log_path="VarAC_traffic.log",
    )
    assert block_events
    assert block_events[0].kind == "flamp_block_request"
    assert block_events[0].queue_id == "F277"
    assert block_events[0].block_numbers == (0, 8, 9)

    exact_code_refresh_events = parse_vault_log_events(
        "06/05/2026 19:55:12 - W8UFO> HUBCODE\n<BLR>\n",
        trigger_mode="exact code only",
        log_path="VarAC_traffic.log",
    )
    assert exact_code_refresh_events
    assert exact_code_refresh_events[0].kind == "unlock"
    assert exact_code_refresh_events[0].code_text == "HUBCODE"


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


def test_publish_root_view_appends_custom_helper_text_to_filename(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
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
            description="Latest reports",
            open_rule="Public",
            visibility_rule="Public",
        ),
    ]

    publish_root_view(
        sender="",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=(),
        limit_access_enabled=False,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=False,
    )

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "00 READ FIRST - type command, wait 10 sec, refresh BBS.txt" in names
    assert "01 COMMANDS - type one command below.txt" in names
    assert "20 type INTEL - open Intel - Latest reports.txt" in names


def test_publish_root_view_uses_normalized_alias_and_removes_old_helper(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    test_dir = Path(created["locations"]) / "TEST_A"
    test_dir.mkdir(parents=True)
    stale = live_bbs / "BBS MSG - Type TESTA to open TEST_A then refresh BBS.txt"
    stale.write_text("old helper\n", encoding="utf-8")
    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
        VaultLocation(
            id="test-a",
            name="TEST_A",
            source_dir=str(test_dir),
            alias="TEST_A",
            description="custom text add-on",
            open_rule="Public",
            visibility_rule="Public",
        ),
    ]

    publish_root_view(
        sender="",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=(),
        limit_access_enabled=False,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=False,
    )

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert normalize_location_alias("TEST_A") == "TESTA"
    assert "20 type TESTA - open TEST_A - custom text add-on.txt" in names
    assert stale.name not in names


def test_publish_root_view_only_lists_helpers_available_to_caller(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    public_dir = Path(created["locations"]) / "Public"
    restricted_dir = Path(created["locations"]) / "Restricted"
    code_dir = Path(created["locations"]) / "CodeOnly"
    public_dir.mkdir(parents=True)
    restricted_dir.mkdir(parents=True)
    code_dir.mkdir(parents=True)

    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
        VaultLocation(
            id="public",
            name="Public",
            source_dir=str(public_dir),
            alias="PUBLIC",
            open_rule="Public",
            visibility_rule="Public",
        ),
        VaultLocation(
            id="restricted",
            name="Restricted",
            source_dir=str(restricted_dir),
            alias="RESTRICTED",
            open_rule="Allowed callsigns only",
            visibility_rule="Public",
            inherit_global_allowed_callsigns=False,
            allowed_callsigns=("W5TTA",),
        ),
        VaultLocation(
            id="code",
            name="CodeOnly",
            source_dir=str(code_dir),
            alias="CODE",
            open_rule="Access code required",
            visibility_rule="Public",
        ),
    ]

    publish_root_view(
        sender="",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=(),
        limit_access_enabled=False,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=False,
    )
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("PUBLIC" in name for name in names)
    assert not any("RESTRICTED" in name for name in names)
    assert not any("CODE" in name for name in names)

    publish_root_view(
        sender="W5TTA",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=(),
        limit_access_enabled=False,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=False,
    )
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("PUBLIC" in name for name in names)
    assert any("RESTRICTED" in name for name in names)
    assert "22 type CODE [CODE] - open CODE with access code.txt" in names

    publish_root_view(
        sender="KX9ZZZ",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=(),
        limit_access_enabled=False,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=False,
    )
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("PUBLIC" in name for name in names)
    assert not any("RESTRICTED" in name for name in names)
    assert "21 type CODE [CODE] - open CODE with access code.txt" in names


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


def test_reset_to_default_falls_back_to_enabled_locations_when_root_menu_empty(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
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
            enabled=True,
            list_in_root_menu=False,
            visibility_rule="Public",
            open_rule="Allowed callsigns + access code",
        ),
        VaultLocation(
            id="hidden",
            name="Hidden",
            source_dir=str(tmp_path / "hidden"),
            alias="HIDDEN",
            enabled=True,
            list_in_root_menu=False,
            visibility_rule="Hidden",
        ),
    ]

    reset_to_default_location(
        locations=locations,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        default_location_id=DEFAULT_LOCATION_ID,
        runtime_state=VaultRuntimeState(current_location_id="intel", current_session_callsign="W5TTA"),
        global_code_policy="Allow public locations",
    )

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("INTEL" in name for name in names)
    assert not any("HIDDEN" in name for name in names)


def test_reset_to_default_does_not_resurrect_deleted_filesystem_locations(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    for folder_name in ("AIB", "HUBS", "Intel", "MR08"):
        (Path(created["locations"]) / folder_name).mkdir(parents=True)
    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
    ]

    reset_to_default_location(
        locations=locations,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        default_location_id=DEFAULT_LOCATION_ID,
        runtime_state=VaultRuntimeState(current_location_id=DEFAULT_LOCATION_ID),
        global_code_policy="Allow public locations",
    )

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert not any("AIB" in name for name in names)
    assert not any("HUBS" in name for name in names)
    assert not any("INTEL" in name for name in names)
    assert not any("MR08" in name for name in names)


def test_background_reconcile_does_not_resurrect_deleted_filesystem_locations(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    for folder_name in ("AIB", "HUBS", "Intel", "MR08"):
        (Path(created["locations"]) / folder_name).mkdir(parents=True)
    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
    ]
    reset_result = reset_to_default_location(
        locations=locations,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        default_location_id=DEFAULT_LOCATION_ID,
        runtime_state=VaultRuntimeState(current_location_id=DEFAULT_LOCATION_ID),
        global_code_policy="Allow public locations",
    )
    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_bbs_vault_managed_root=str(managed_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
        varac_bbs_vault_global_code_policy="Allow public locations",
        varac_bbs_vault_trigger_mode="VarAC session commands",
        varac_bbs_vault_return_mode="On disconnect",
        varac_bbs_vault_failed_attempt_limit=3,
        varac_bbs_vault_failed_attempt_window_seconds=900,
        varac_bbs_vault_cooldown_seconds=1800,
        varac_bbs_vault_idle_timeout_seconds=600,
        varac_bbs_vault_flamp_enabled=False,
        varac_bbs_vault_flamp_relay_dir="",
        varac_bbs_vault_locations_v1=[
            {"id": DEFAULT_LOCATION_ID, "name": DEFAULT_LOCATION_NAME, "source_dir": str(default_dir), "alias": "ROOT"},
        ],
        varac_bbs_vault_runtime_state_v1=vault_runtime_state_to_data(reset_result.runtime_state),
        varac_db_path="",
    )

    run_varac_bbs_vault(settings)

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert not any("AIB" in name for name in names)
    assert not any("INTEL" in name for name in names)


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


def test_run_varac_bbs_vault_processes_varac_bbs_msg_alias_command(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "79fe8dd5-4ff7-4899-a06a-1892d3644b53"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "W5TTA", "BBS MSG INTEL", "2026-05-02 14:11:18.7762304Z"),
            (3, "c", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:12:18.7762304Z"),
        ],
    )

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    intel_dir.mkdir(parents=True)
    (intel_dir / "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s").write_text("intel", encoding="utf-8")

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
                "description": "Open Intel",
                "source_dir": str(intel_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            },
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="",
        varac_bbs_limit_access_enabled=False,
    )

    result = run_varac_bbs_vault(settings)

    assert result.enabled
    assert result.processed_events == 3
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_location_id == "intel"
    assert state.current_view_mode == "location"
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s" in names
    assert any("ROOT" in name for name in names)
    assert not any("Type INTEL" in name for name in names)


def test_run_varac_bbs_vault_uses_log_alias_when_db_only_saw_refresh(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "08709dda-8ea8-47e5-b8e4-1c7afd299866"
    _insert_qso(varac_db, guid=qso_guid, remote="W8UFO")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W8UFO", "<BLR>", "2026-05-06 19:04:49.0000000Z"),
        ],
    )
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "06/05/2026 19:04:49 - W8UFO> <BLR>",
                "06/05/2026 19:08:32 - W8UFO> INTEL",
                "06/05/2026 19:08:51 - W8UFO> <BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    intel_dir.mkdir(parents=True)
    (intel_dir / "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s").write_text("intel", encoding="utf-8")

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
                "description": "Open Intel",
                "source_dir": str(intel_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            },
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="",
        varac_bbs_limit_access_enabled=False,
    )

    result = run_varac_bbs_vault(settings)

    assert result.enabled
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_location_id == "intel"
    assert state.current_view_mode == "location"
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s" in names
    assert any("ROOT" in name for name in names)
    assert not any("Type INTEL" in name for name in names)


def test_run_varac_bbs_vault_processes_log_location_switches_and_root(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "428d19f8-4066-4a80-b2c2-fbe2983a77fa"
    _insert_qso(varac_db, guid=qso_guid, remote="W8UFO")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W8UFO", "<BLR>", "2026-05-06 19:44:18.0000000Z"),
        ],
    )

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    intel_dir.mkdir(parents=True)
    aib_dir = Path(created["locations"]) / "AIB"
    aib_dir.mkdir(parents=True)
    hubs_dir = Path(created["locations"]) / "HUBS"
    hubs_dir.mkdir(parents=True)
    (intel_dir / "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s").write_text("intel", encoding="utf-8")
    (aib_dir / "NATL-RR-260427-1500Z-AIB-sig.k2s").write_text("aib", encoding="utf-8")
    (hubs_dir / "N1MAG-20260429-OpNet-1.b2s").write_text("hubs", encoding="utf-8")
    hubs_code = hash_access_code("HUBCODE")

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
                "description": "Open Intel",
                "source_dir": str(intel_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            },
            {
                "id": "aib",
                "name": "AIB",
                "alias": "AIB",
                "description": "Open AIB",
                "source_dir": str(aib_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            },
            {
                "id": "hubs",
                "name": "HUBS",
                "alias": "HUBS",
                "description": "Open HUBS",
                "source_dir": str(hubs_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Allowed callsigns + access code",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": str(hubs_code["access_code_hash"]),
                "access_code_salt": str(hubs_code["access_code_salt"]),
                "access_code_iterations": int(hubs_code["access_code_iterations"]),
            },
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="",
        varac_bbs_limit_access_enabled=False,
    )

    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "06/05/2026 19:44:18 - W8UFO> <BLR>",
                "06/05/2026 19:45:05 - W8UFO> INTEL",
                "06/05/2026 19:45:22 - W8UFO> <BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    run_varac_bbs_vault(settings)
    assert "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s" in {p.name for p in live_bbs.iterdir() if p.is_file()}

    settings.set("varac_bbs_vault_runtime_state_v1", {**settings.get("varac_bbs_vault_runtime_state_v1"), "last_datastream_id": 1})
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "06/05/2026 19:44:18 - W8UFO> <BLR>",
                "06/05/2026 19:45:05 - W8UFO> INTEL",
                "06/05/2026 19:45:22 - W8UFO> <BLR>",
                "06/05/2026 19:46:19 - W8UFO> AIB",
                "06/05/2026 19:46:33 - W8UFO> <BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    run_varac_bbs_vault(settings)
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "NATL-RR-260427-1500Z-AIB-sig.k2s" in names
    assert "MAGNET_S2_WEEKLY_SNAPSHOT-260426.b2s" not in names

    settings.set("varac_bbs_vault_runtime_state_v1", {**settings.get("varac_bbs_vault_runtime_state_v1"), "last_datastream_id": 1})
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "06/05/2026 19:44:18 - W8UFO> <BLR>",
                "06/05/2026 19:45:05 - W8UFO> INTEL",
                "06/05/2026 19:45:22 - W8UFO> <BLR>",
                "06/05/2026 19:46:19 - W8UFO> AIB",
                "06/05/2026 19:46:33 - W8UFO> <BLR>",
                "06/05/2026 19:47:36 - W8UFO> HUBS",
                "06/05/2026 19:47:58 - W8UFO> <BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    run_varac_bbs_vault(settings)
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "00 NOTICE - HUBS requires an access code.txt" in names
    assert "10 type HUBS [CODE] - open with access code.txt" in names
    assert "NATL-RR-260427-1500Z-AIB-sig.k2s" not in names
    assert "N1MAG-20260429-OpNet-1.b2s" not in names

    settings.set("varac_bbs_vault_runtime_state_v1", {**settings.get("varac_bbs_vault_runtime_state_v1"), "last_datastream_id": 1})
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "06/05/2026 19:44:18 - W8UFO> <BLR>",
                "06/05/2026 19:45:05 - W8UFO> INTEL",
                "06/05/2026 19:45:22 - W8UFO> <BLR>",
                "06/05/2026 19:46:19 - W8UFO> AIB",
                "06/05/2026 19:46:33 - W8UFO> <BLR>",
                "06/05/2026 19:47:36 - W8UFO> HUBS",
                "06/05/2026 19:47:58 - W8UFO> <BLR>",
                "06/05/2026 19:48:12 - W8UFO> HUBS WRONGCODE",
                "06/05/2026 19:48:28 - W8UFO> <BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    run_varac_bbs_vault(settings)
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "00 NOTICE - Incorrect code for HUBS.txt" in names
    assert "10 type HUBS [CODE] - try again with access code.txt" in names
    assert "N1MAG-20260429-OpNet-1.b2s" not in names

    settings.set("varac_bbs_vault_runtime_state_v1", {**settings.get("varac_bbs_vault_runtime_state_v1"), "last_datastream_id": 1})
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "06/05/2026 19:44:18 - W8UFO> <BLR>",
                "06/05/2026 19:45:05 - W8UFO> INTEL",
                "06/05/2026 19:45:22 - W8UFO> <BLR>",
                "06/05/2026 19:46:19 - W8UFO> AIB",
                "06/05/2026 19:46:33 - W8UFO> <BLR>",
                "06/05/2026 19:47:36 - W8UFO> HUBS",
                "06/05/2026 19:47:58 - W8UFO> <BLR>",
                "06/05/2026 19:51:12 - W8UFO> ROOT",
                "06/05/2026 19:51:27 - W8UFO> <BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    run_varac_bbs_vault(settings)
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert any("type AIB" in name for name in names)
    assert any("type INTEL" in name for name in names)
    assert "NATL-RR-260427-1500Z-AIB-sig.k2s" not in names


def test_run_varac_bbs_vault_ignores_unconfigured_filesystem_alias(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "a72dc4bc-4c9b-454a-a762-01e4a7478dc5"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "W5TTA", "AIB", "2026-05-02 14:11:18.7762304Z"),
        ],
    )

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    aib_dir = Path(created["locations"]) / "AIB"
    aib_dir.mkdir(parents=True)
    (aib_dir / "NATL-RR-260427-1500Z-AIB-sig.k2s").write_text("aib", encoding="utf-8")

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
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="",
        varac_bbs_limit_access_enabled=False,
    )

    result = run_varac_bbs_vault(settings)

    assert result.enabled
    assert result.processed_events == 1
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_view_mode == "root"
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "NATL-RR-260427-1500Z-AIB-sig.k2s" not in names
    assert not any("Type AIB" in name for name in names)


def test_run_varac_bbs_vault_uses_managed_location_folder_when_config_source_is_empty(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "58903fc6-b76e-4bd7-947b-173916587a36"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "W5TTA", "INTEL", "2026-05-02 14:11:18.7762304Z"),
        ],
    )

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    intel_dir = Path(created["locations"]) / "Intel"
    intel_dir.mkdir(parents=True)
    stale_intel_dir = tmp_path / "old-empty-intel"
    stale_intel_dir.mkdir()
    (intel_dir / "MAGNET-S2-RR-260502-_U.S_Iran_Ceasefire_Stability_Degrading.sig.b2s").write_text("intel", encoding="utf-8")

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
                "description": "Open Intel",
                "source_dir": str(stale_intel_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            },
        ],
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="",
        varac_bbs_limit_access_enabled=False,
    )

    result = run_varac_bbs_vault(settings)

    assert result.enabled
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_location_id == "intel"
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "MAGNET-S2-RR-260502-_U.S_Iran_Ceasefire_Stability_Degrading.sig.b2s" in names
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


def test_publish_root_view_lists_flamp_menu_helper(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name=DEFAULT_LOCATION_NAME, source_dir=str(default_dir), alias="ROOT"),
    ]

    publish_root_view(
        sender="",
        locations=locations,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=(),
        limit_access_enabled=False,
        global_code_policy=DEFAULT_GLOBAL_CODE_POLICY,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        flamp_enabled=True,
    )

    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "00 READ FIRST - type command, wait 10 sec, refresh BBS.txt" in names
    assert "01 COMMANDS - type one command below.txt" in names
    assert "20 type FLAMP - show Flamp block fill commands.txt" in names
    assert not any("FLAMP CMDS LIST Q" in name for name in names)


def test_run_varac_bbs_vault_flamp_command_publishes_separate_helpers(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-flamp-help"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "W5TTA", "FLAMP", "2026-05-02 14:10:21.0000000Z"),
        ],
    )
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_db_path=str(varac_db),
        varac_path=str(varac_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
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
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert result.enabled
    assert state.current_view_mode == "flamp-help"
    assert "10 type ROOT - return to main menu.txt" in names
    assert "20 type LIST Q - list available Flamp files.txt" in names
    assert "21 type LIST BLKS F277 - show blocks for queue F277.txt" in names
    assert "22 type BLK 0,8,9 F277 - request blocks 0,8,9.txt" in names


def test_run_varac_bbs_vault_flamp_command_accepts_varac_prefixed_db_entry(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-flamp-prefixed"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "", "W5TTA> de W5TTA <R-13> FLAMP", "2026-05-02 14:10:21.0000000Z"),
        ],
    )
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_db_path=str(varac_db),
        varac_path=str(varac_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
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
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert result.enabled
    assert state.current_view_mode == "flamp-help"
    assert "20 type LIST Q - list available Flamp files.txt" in names


def test_run_varac_bbs_vault_flamp_queue_and_block_commands_accept_prefixed_db_entries(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-flamp-prefixed-flow"
    _insert_qso(varac_db, guid=qso_guid, remote="W5TTA")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "W5TTA", "<BLR>", "2026-05-02 14:10:18.7762304Z"),
            (2, "b", 1, qso_guid, "", "W5TTA> LIST Q", "2026-05-02 14:10:21.0000000Z"),
            (3, "c", 1, qso_guid, "", "W5TTA> LIST BLKS F277", "2026-05-02 14:10:24.0000000Z"),
            (4, "d", 1, qso_guid, "", "W5TTA> BLK 0,1 F277", "2026-05-02 14:10:27.0000000Z"),
        ],
    )

    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "F277_MAGNET-S2-RR-260502.sig.b2s").write_text(
        "<PROG 1.0>{F277}\n<SIZE xx>{F277}2119 2 1024\n{F277:1}BLOCK1\n{F277:2}BLOCK2\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
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
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert result.enabled
    assert state.current_view_mode == "flamp-block-overlay"
    assert state.current_overlay_file.startswith("BBS_F277_BLK_0_1")
    assert (live_bbs / state.current_overlay_file).exists()


def test_run_varac_bbs_vault_log_list_blocks_publishes_block_list(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    (varac_root / "VarAC_traffic.log").write_text(
        "06/05/2026 19:53:12 - W5TTA> LIST BLKS F277\n",
        encoding="utf-8",
    )
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "F277_MAGNET-S2-RR-260502.sig.b2s").write_text(
        "<PROG 1.0>{F277}\n<SIZE xx>{F277}2119 2 1024\n{F277:1}BLOCK1\n{F277:2}BLOCK2\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    (default_dir / "RootInfo.txt").write_text("root", encoding="utf-8")
    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_db_path="",
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
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    block_list = live_bbs / "BBS_BLOCK_LIST_F277.txt"
    assert result.enabled
    assert state.current_view_mode == "flamp-blocks"
    assert state.last_error == ""
    assert block_list.exists()
    assert "AVAILABLE 1,2" in block_list.read_text(encoding="utf-8")


def test_run_varac_bbs_vault_log_list_blocks_switches_queue_views(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    log_path = varac_root / "VarAC_traffic.log"
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "41D6_NATL-RR-260413-1530Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{41D6}\n<SIZE xx>{41D6}2119 2 1024\n{41D6:1}A\n{41D6:2}B\n",
        encoding="utf-8",
    )
    (relay_dir / "1AD1_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{1AD1}\n<SIZE xx>{1AD1}2119 3 1024\n{1AD1:1}A\n{1AD1:3}C\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_db_path="",
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
    )

    log_path.write_text("06/05/2026 17:07:19 - N5TNT> LIST BLKS 41D6\n", encoding="utf-8")
    run_varac_bbs_vault(settings)
    assert (live_bbs / "BBS_BLOCK_LIST_41D6.txt").exists()

    state_data = dict(settings.get("varac_bbs_vault_runtime_state_v1"))
    state_data["processed_event_keys"] = []
    settings.set("varac_bbs_vault_runtime_state_v1", state_data)
    log_path.write_text("06/05/2026 17:08:53 - N5TNT> LIST BLKS 1AD1\n", encoding="utf-8")
    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))

    second_list = live_bbs / "BBS_BLOCK_LIST_1AD1.txt"
    assert result.enabled
    assert state.current_view_mode == "flamp-blocks"
    assert not (live_bbs / "BBS_BLOCK_LIST_41D6.txt").exists()
    assert second_list.exists()
    body = second_list.read_text(encoding="utf-8")
    assert "QUEUE 1AD1" in body
    assert "AVAILABLE 1,3" in body
    assert "MISSING 2" in body


def test_run_varac_bbs_vault_log_list_blocks_handles_bare_blr_continuation(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    log_path = varac_root / "VarAC_traffic.log"
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "41D6_NATL-RR-260413-1530Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{41D6}\n<SIZE xx>{41D6}2119 2 1024\n{41D6:1}A\n{41D6:2}B\n",
        encoding="utf-8",
    )
    (relay_dir / "1AD1_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{1AD1}\n<SIZE xx>{1AD1}2119 3 1024\n{1AD1:1}A\n{1AD1:3}C\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_db_path="",
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
    )

    log_path.write_text(
        "\n".join(
            [
                "09/05/2026 23:55:55 - N5TNT> LIST BLKS 1AD1",
                "09/05/2026 23:56:03 - N5TNT> <BLR>",
                "09/05/2026 23:56:43 - N5TNT> LIST BLKS 41D6",
                "09/05/2026 23:56:51 - N5TNT> <BLR>",
                "09/05/2026 23:57:27 - N5TNT> LIST BLKS 1AD1",
                "<BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))

    assert result.enabled
    assert state.current_view_label == "FLAMP 1AD1 blocks"
    assert not (live_bbs / "BBS_BLOCK_LIST_41D6.txt").exists()
    assert (live_bbs / "BBS_BLOCK_LIST_1AD1.txt").exists()


def test_run_varac_bbs_vault_db_blr_refreshes_current_flamp_block_list(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-flamp-refresh"
    _insert_qso(varac_db, guid=qso_guid, remote="N5TNT")
    _insert_datastream(
        varac_db,
        [
            (1, "a", 1, qso_guid, "N5TNT", "LIST BLKS 1AD1", "2026-05-09 20:17:35.0000000Z"),
            (2, "b", 1, qso_guid, "N5TNT", "<BLR>", "2026-05-09 20:17:47.0000000Z"),
            (3, "c", 1, qso_guid, "N5TNT", "LIST BLKS 41D6", "2026-05-09 20:20:00.0000000Z"),
            (4, "d", 1, qso_guid, "N5TNT", "<BLR>", "2026-05-09 20:20:09.0000000Z"),
        ],
    )
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "1AD1_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{1AD1}\n<SIZE xx>{1AD1}2119 3 1024\n{1AD1:1}A\n{1AD1:3}C\n",
        encoding="utf-8",
    )
    (relay_dir / "41D6_NATL-RR-260413-1530Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{41D6}\n<SIZE xx>{41D6}2119 2 1024\n{41D6:1}A\n{41D6:2}B\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    stale_unmanaged = live_bbs / "BBS_BLOCK_LIST_1AD1.txt"
    stale_unmanaged.write_text("stale", encoding="utf-8")
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
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
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    manifest_entries = read_publish_manifest(managed_root / "runtime" / "manifests" / "current_publish_manifest.json")
    live_names = {entry.live_name for entry in manifest_entries}

    assert result.enabled
    assert state.current_view_mode in {"flamp-block-list", "flamp-blocks"}
    assert state.current_view_label == "FLAMP 41D6 blocks"
    assert not stale_unmanaged.exists()
    assert not (live_bbs / "BBS_BLOCK_LIST_1AD1.txt").exists()
    assert (live_bbs / "BBS_BLOCK_LIST_41D6.txt").exists()
    assert "BBS_BLOCK_LIST_41D6.txt" in live_names
    assert "BBS_BLOCK_LIST_1AD1.txt" not in live_names
    assert not any(name == "20 type FLAMP - show Flamp block fill commands.txt" for name in live_names)


def test_run_varac_bbs_vault_db_first_scan_starts_from_recent_tail(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-flamp-history-tail"
    _insert_qso(varac_db, guid=qso_guid, remote="N5TNT")
    filler_rows = [
        (idx, f"f{idx}", 1, qso_guid, "N5TNT", "de N5TNT <R+36>", f"2026-05-09 19:{idx % 60:02d}:00.0000000Z")
        for idx in range(1, 301)
    ]
    command_rows = [
        (301, "cmd1", 1, qso_guid, "N5TNT", "LIST BLKS 1AD1\n<BLR>", "2026-05-10 00:31:07.0000000Z"),
        (302, "out1", 1, qso_guid, "W5TTA/P", "<BL:BBS_BLOCK_LIST_1AD1.txt|2026-05-09|144\n>", "2026-05-10 00:31:20.0000000Z"),
        (303, "cmd2", 1, qso_guid, "N5TNT", "LIST BLKS 41D6\n<BLR>", "2026-05-10 00:32:35.0000000Z"),
        (304, "out2", 1, qso_guid, "W5TTA/P", "<BL:BBS_BLOCK_LIST_41D6.txt|2026-05-09|149\n>", "2026-05-10 00:32:42.0000000Z"),
    ]
    _insert_datastream(varac_db, filler_rows + command_rows)
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "1AD1_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{1AD1}\n<SIZE xx>{1AD1}2119 3 1024\n{1AD1:1}A\n{1AD1:3}C\n",
        encoding="utf-8",
    )
    (relay_dir / "41D6_NATL-RR-260413-1530Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{41D6}\n<SIZE xx>{41D6}2119 2 1024\n{41D6:1}A\n{41D6:2}B\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
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
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))

    assert result.enabled
    assert state.last_datastream_id == 304
    assert state.current_view_label == "FLAMP 41D6"
    assert not (live_bbs / "BBS_BLOCK_LIST_1AD1.txt").exists()
    assert (live_bbs / "BBS_BLOCK_LIST_41D6.txt").exists()
    assert (live_bbs / "00 NOTICE - LIST BLKS 41D6 received; wait 10 sec, refresh again.txt").exists()


def test_run_varac_bbs_vault_db_handles_missing_qso_and_slashed_zero_queue(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-missing-join"
    _insert_datastream(
        varac_db,
        [
            (674, "cmd1", 1, qso_guid, "N5TNT", "LIST BLKS 1AD1", "2026-05-10 03:40:04.0000000Z"),
            (675, "blr1", 1, qso_guid, "N5TNT", "<BLR>", "2026-05-10 03:40:39.0000000Z"),
            (676, "out1", 2, qso_guid, "W5TTA/P", "<BL:BBS_BLOCK_LIST_1AD1.txt|2026-05-09|144\n>", "2026-05-10 03:40:47.0000000Z"),
            (677, "cmd2", 1, qso_guid, "N5TNT", "LIST BLKS 41D6\n<BLR>", "2026-05-10 03:41:15.0000000Z"),
            (678, "out2", 2, qso_guid, "W5TTA/P", "<BL:BBS_BLOCK_LIST_41D6.txt|2026-05-09|149\n>", "2026-05-10 03:41:22.0000000Z"),
            (679, "cmd3", 1, qso_guid, "N5TNT", "LIST BLKS 2AØC", "2026-05-10 03:41:50.0000000Z"),
            (680, "blr3", 1, qso_guid, "N5TNT", "<BLR>", "2026-05-10 03:41:58.0000000Z"),
        ],
    )
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "1AD1_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{1AD1}\n<SIZE xx>{1AD1}2119 3 1024\n{1AD1:1}A\n{1AD1:3}C\n",
        encoding="utf-8",
    )
    (relay_dir / "41D6_NATL-RR-260413-1530Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{41D6}\n<SIZE xx>{41D6}2119 2 1024\n{41D6:1}A\n{41D6:2}B\n",
        encoding="utf-8",
    )
    (relay_dir / "2A0C_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{2A0C}\n<SIZE xx>{2A0C}2119 4 1024\n{2A0C:1}A\n{2A0C:4}D\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
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
        varac_bbs_vault_runtime_state_v1={"last_datastream_id": 673},
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))

    assert result.enabled
    assert result.processed_events >= 3
    assert state.current_session_callsign == "N5TNT"
    assert state.current_view_label in {"FLAMP 2A0C", "FLAMP 2A0C blocks"}
    assert state.last_datastream_id == 680
    assert not (live_bbs / "BBS_BLOCK_LIST_1AD1.txt").exists()
    assert not (live_bbs / "BBS_BLOCK_LIST_41D6.txt").exists()
    assert (live_bbs / "BBS_BLOCK_LIST_2A0C.txt").exists()


def test_run_varac_bbs_vault_db_new_qso_does_not_drop_first_command(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-new-session-no-join"
    _insert_datastream(
        varac_db,
        [
            (674, "cmd1", 1, qso_guid, "N5TNT", "LIST BLKS 1AD1", "2026-05-10 03:40:04.0000000Z"),
            (675, "blr1", 1, qso_guid, "N5TNT", "<BLR>", "2026-05-10 03:40:39.0000000Z"),
            (676, "out1", 2, qso_guid, "W5TTA/P", "<BL:BBS_BLOCK_LIST_1AD1.txt|2026-05-09|144\n>", "2026-05-10 03:40:47.0000000Z"),
            (677, "cmd2", 1, qso_guid, "N5TNT", "LIST BLKS 41D6\n<BLR>", "2026-05-10 03:41:15.0000000Z"),
            (678, "out2", 2, qso_guid, "W5TTA/P", "<BL:BBS_BLOCK_LIST_41D6.txt|2026-05-09|149\n>", "2026-05-10 03:41:22.0000000Z"),
        ],
    )
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "1AD1_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{1AD1}\n<SIZE xx>{1AD1}2119 3 1024\n{1AD1:1}A\n{1AD1:3}C\n",
        encoding="utf-8",
    )
    (relay_dir / "41D6_NATL-RR-260413-1530Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{41D6}\n<SIZE xx>{41D6}2119 2 1024\n{41D6:1}A\n{41D6:2}B\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
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
        varac_bbs_vault_runtime_state_v1={
            "last_datastream_id": 673,
            "current_session_callsign": "N5TNT",
            "current_session_qso_guid": "old-session",
            "current_view_mode": "flamp-blocks",
            "current_view_label": "FLAMP 2A0C blocks",
            "last_request_ts": 1,
        },
    )

    result = run_varac_bbs_vault(settings)
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))

    assert result.enabled
    assert result.processed_events >= 2
    assert state.current_session_qso_guid == qso_guid
    assert state.current_view_label in {"FLAMP 41D6", "FLAMP 41D6 blocks"}
    assert state.last_datastream_id == 678
    assert not (live_bbs / "BBS_BLOCK_LIST_1AD1.txt").exists()
    assert (live_bbs / "BBS_BLOCK_LIST_41D6.txt").exists()


def test_run_varac_bbs_vault_db_command_is_not_overwritten_by_stale_log_tail(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    varac_db = varac_root / "VarAC.db"
    _create_varac_db(varac_db)
    qso_guid = "qso-flamp-stale-log"
    _insert_qso(varac_db, guid=qso_guid, remote="N5TNT")
    _insert_datastream(
        varac_db,
        [
            (3, "c", 1, qso_guid, "N5TNT", "LIST BLKS 1AD1", "2026-05-09 21:53:34.0000000Z"),
            (4, "d", 1, qso_guid, "N5TNT", "<BLR>", "2026-05-09 21:53:42.0000000Z"),
        ],
    )
    (varac_root / "VarAC_traffic.log").write_text(
        "\n".join(
            [
                "09/05/2026 21:52:22 - N5TNT> LIST BLKS 2A0C",
                "09/05/2026 21:52:30 - N5TNT> <BLR>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    (relay_dir / "1AD1_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{1AD1}\n<SIZE xx>{1AD1}2119 3 1024\n{1AD1:1}A\n{1AD1:3}C\n",
        encoding="utf-8",
    )
    (relay_dir / "2A0C_NATL-RR-260504-1430Z-AIB-sig.k2s").write_text(
        "<PROG 1.0>{2A0C}\n<SIZE xx>{2A0C}2119 2 1024\n{2A0C:1}A\n{2A0C:2}B\n",
        encoding="utf-8",
    )
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    (live_bbs / "BBS_BLOCK_LIST_2A0C.txt").write_text("stale", encoding="utf-8")
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
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
        varac_bbs_vault_runtime_state_v1={
            "current_location_id": DEFAULT_LOCATION_ID,
            "current_session_callsign": "N5TNT",
            "current_session_qso_guid": qso_guid,
            "current_view_mode": "flamp-blocks",
            "current_view_label": "FLAMP 2A0C blocks",
            "last_request_ts": parse_varac_event_timestamp_to_epoch("09/05/2026 21:52:38", prefer_day_first=True),
            "last_datastream_id": 2,
        },
    )

    run_varac_bbs_vault(settings)

    assert not (live_bbs / "BBS_BLOCK_LIST_2A0C.txt").exists()
    assert (live_bbs / "BBS_BLOCK_LIST_1AD1.txt").exists()
    state = load_vault_runtime_state(settings.get("varac_bbs_vault_runtime_state_v1", {}))
    assert state.current_view_label == "FLAMP 1AD1 blocks"


def test_flamp_queue_listing_filters_age_and_unassigned_files(tmp_path: Path) -> None:
    relay_dir = tmp_path / "relay"
    relay_dir.mkdir()
    fresh = relay_dir / "2A0C_NATL-RR-260504-1430Z-AIB-sig.k2s"
    old = relay_dir / "837C_NATL-RR-260427-1500Z-AIB-sig.k2s"
    unassigned = relay_dir / "1A72_Unassigned"
    for path in (fresh, old, unassigned):
        path.write_text("<PROG 1.0>{ABCD}\n{ABCD:1}BLOCK\n", encoding="utf-8")
    now = time.time()
    old_ts = now - 8 * 86400
    os.utime(old, (old_ts, old_ts))
    os.utime(unassigned, (now, now))
    os.utime(fresh, (now, now))
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    initialize_managed_root(managed_root)
    store = FlampRelayStore(relay_dir)

    publish_flamp_queue_list_view(
        store,
        base_source_dir="",
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        max_age_days=7,
    )

    queue_text = (live_bbs / "BBS_QUEUE_LIST.txt").read_text(encoding="utf-8")
    names = {p.name for p in live_bbs.iterdir() if p.is_file()}
    assert "2A0C_NATL-RR-260504-1430Z-AIB-sig.k2s" in queue_text
    assert "837C_NATL-RR-260427-1500Z-AIB-sig.k2s" not in queue_text
    assert "Unassigned" not in queue_text
    assert any("LIST BLKS 2A0C" in name for name in names)
    assert not any("LIST BLKS 837C" in name for name in names)
    assert not any("1A72" in name for name in names)


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
    tab.varac_bbs_vault_flamp_listing_age_combo.setCurrentIndex(
        tab.varac_bbs_vault_flamp_listing_age_combo.findData(7)
    )

    snap = tab._settings_snapshot_for_readiness()
    assert snap["varac_bbs_vault_enabled"] is True
    assert snap["varac_bbs_vault_global_code_policy"] == "Require for non-default locations"
    assert snap["varac_bbs_vault_flamp_enabled"] is True
    assert snap["varac_bbs_vault_flamp_relay_dir"] == str(tmp_path / "relay")
    assert snap["varac_bbs_vault_flamp_listing_max_age_days"] == 7


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

    assert tab.varac_bbs_vault_description_edit.text() == ""
    assert tab.varac_bbs_vault_source_dir_edit.text() in {
        "FIO_BBS_Vault/locations/Logistics",
        str(managed_root / "locations" / "Logistics"),
    }
    full_path = tab.varac_bbs_vault_source_dir_edit.property("full_path")
    assert full_path in {None, str(managed_root / "locations" / "Logistics")}
    assert "Live BBS likely match: Logistics.txt" in tab.varac_bbs_vault_source_hint_label.text()


def test_settings_tab_location_autofill_tracks_full_path_after_first_character(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    managed_root = tmp_path / "FIO_Managed_Vault"

    tab = SettingsTab()
    tab.varac_bbs_vault_root_edit.setText(str(managed_root))
    tab._new_varac_bbs_vault_location()

    for text in ("A", "Am", "AmR", "AmRR", "AmRRO", "AmRRON"):
        tab.varac_bbs_vault_location_name_edit.setText(text)

    assert tab.varac_bbs_vault_source_dir_edit.text() == str(managed_root / "locations" / "AmRRON")


def test_settings_tab_detects_existing_vault_folder_as_readd(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    existing = tmp_path / "FIO_Managed_Vault" / "locations" / "AmRRON"
    existing.mkdir(parents=True)
    keep_file = existing / "existing.txt"
    keep_file.write_text("keep", encoding="utf-8")

    tab = SettingsTab()

    assert tab._should_offer_readd_varac_bbs_vault_location_folder("", existing) is True
    assert tab._should_offer_readd_varac_bbs_vault_location_folder("amrron", existing) is False
    assert tab._should_offer_readd_varac_bbs_vault_location_folder("", existing / "missing") is False
    assert keep_file.read_text(encoding="utf-8") == "keep"


def test_settings_tab_helper_preview_matches_pause_helper_text(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    managed_root = tmp_path / "managed"
    location_dir = managed_root / "locations" / "Intel"
    location_dir.mkdir(parents=True)

    tab = SettingsTab()
    tab.varac_bbs_vault_root_edit.setText(str(managed_root))
    tab._set_varac_bbs_vault_locations(
        [
            {
                "id": DEFAULT_LOCATION_ID,
                "name": DEFAULT_LOCATION_NAME,
                "alias": "ROOT",
                "description": "Main menu",
                "source_dir": str(managed_root / "locations" / DEFAULT_LOCATION_NAME),
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
                "description": "Latest reports",
                "source_dir": str(location_dir),
                "enabled": True,
                "list_in_root_menu": True,
                "visibility_rule": "Public",
                "open_rule": "Public",
                "inherit_global_allowed_callsigns": True,
                "allowed_callsigns": [],
                "access_code_hash": "",
                "access_code_salt": "",
                "access_code_iterations": 310000,
            },
        ]
    )
    tab.varac_bbs_vault_locations_list.setCurrentRow(1)
    tab._refresh_varac_bbs_vault_helper_preview()

    assert (
        tab.varac_bbs_vault_helper_preview_label.text()
        == "All BBS views include: 00 READ FIRST - type command, wait 10 sec, refresh BBS.txt\n"
        "20 type INTEL - open Intel - Latest reports.txt"
    )


def test_multi_radio_store_preserves_per_varac_bbs_ui_fields(tmp_path: Path) -> None:
    from freqinout.core.multi_radio_store import MultiRadioStore

    store = MultiRadioStore(tmp_path / "freqinout.db")
    saved = store.save_device_profile(
        {
            "system_key": "varac-a",
            "name": "VarAC A",
            "control_backend": "manual",
            "use_varac": True,
            "varac_bbs_vault_global_code_policy": "Allow public locations",
            "varac_bbs_vault_flamp_enabled": True,
            "varac_bbs_vault_flamp_relay_dir": str(tmp_path / "flamp" / "relay"),
            "varac_bbs_vault_flamp_listing_max_age_days": 7,
            "varac_bbs_vault_locations_v1": [
                {
                    "id": "intel",
                    "name": "Intel",
                    "alias": "INTEL",
                    "description": "Latest reports",
                    "source_dir": str(tmp_path / "Intel"),
                    "enabled": True,
                    "list_in_root_menu": True,
                    "visibility_rule": "Public",
                    "open_rule": "Public",
                }
            ],
        }
    )

    loaded = store.get_device_profile(int(saved["id"]))
    assert loaded is not None
    assert loaded["varac_bbs_vault_global_code_policy"] == "Allow public locations"
    assert loaded["varac_bbs_vault_flamp_enabled"] == 1
    assert loaded["varac_bbs_vault_flamp_relay_dir"] == str(tmp_path / "flamp" / "relay")
    assert loaded["varac_bbs_vault_flamp_listing_max_age_days"] == 7
    assert load_vault_locations(loaded["varac_bbs_vault_locations_v1"])[0].alias == "INTEL"


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
