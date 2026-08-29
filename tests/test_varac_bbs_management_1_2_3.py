from __future__ import annotations

import os
import json
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLabel, QListWidget, QPushButton

from freqinout.core.multi_radio_store import MultiRadioStore, set_multi_rig_migration_version
from freqinout.core.operator_groups import OperatorGroupFamily
from freqinout.core.varac_bbs_sources import (
    append_group_source_selection,
    group_source_selections_json,
    group_source_summary_text,
    remove_group_source_indexes,
    roster_refresh_plan,
)
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.varac_bbs_config import (
    get_varac_ini_sync_state,
    load_varac_bbs_config,
    varac_path_to_host_path,
    varac_ini_sync_state_matches,
    varac_ini_sync_state_to_json,
    write_varac_bbs_config,
)
from freqinout.gui.settings_tab import SettingsTab
from freqinout.gui.message_viewer_tab import MessageViewerTab


class _DictSettings:
    def __init__(self, data: dict):
        self._data = dict(data)

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def reload(self) -> None:
        return None


def _app():
    return QApplication.instance() or QApplication([])


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


def test_varac_path_to_host_path_resolves_windows_bbs_path_with_wineprefix(tmp_path: Path, monkeypatch) -> None:
    wineprefix = tmp_path / ".wine"
    monkeypatch.setenv("WINEPREFIX", str(wineprefix))

    resolved = varac_path_to_host_path(r"C:\users\bill\Desktop\VaraFiles\BBS")

    assert resolved == str(wineprefix / "drive_c" / "users" / "bill" / "Desktop" / "VaraFiles" / "BBS")


def test_varac_path_to_host_path_infers_wineprefix_from_ini_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("WINEPREFIX", raising=False)
    wineprefix = tmp_path / "custom-prefix"
    ini_path = wineprefix / "drive_c" / "VarAC" / "VarAC.ini"

    resolved = varac_path_to_host_path(r"C:\users\bill\Desktop\VaraFiles\BBS", ini_path=ini_path)

    assert resolved == str(wineprefix / "drive_c" / "users" / "bill" / "Desktop" / "VaraFiles" / "BBS")


def test_multi_radio_store_persists_and_projects_varac_bbs_access_fields(tmp_path: Path) -> None:
    store = MultiRadioStore(db_path=tmp_path / "freqinout.db")
    with store._connect() as conn:
        set_multi_rig_migration_version(conn)
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
            "varac_bbs_allowed_group_sources": '[{"family":"MAGNET","groups":["MR08"],"mode":"trusted_callsigns"}]',
            "varac_bbs_announce_enabled": 1,
        }
    )

    projected = store.sync_runtime_active_device_to_legacy_settings(int(saved["id"]))

    assert projected["varac_bbs_enabled"] is True
    assert projected["varac_bbs_limit_access_enabled"] is True
    assert projected["varac_bbs_allowed_callsigns"] == "K7RIE,KG5RKW,W8UFO"
    assert projected["varac_bbs_allowed_group_sources"] == (
        '[{"family":"MAGNET","groups":["MR08"],"mode":"trusted_callsigns"}]'
    )
    assert projected["varac_bbs_announce_enabled"] is True


def test_messages_bbs_status_text_is_runtime_projected_context(tmp_path: Path) -> None:
    text = (Path(__file__).resolve().parents[1] / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert "runtime-projected default context" in text
    assert "VarAC BBS (runtime-projected default context):" in text
    assert "BBS File Area" in text
    assert "VarAC incoming file" in text
    assert "VarAC outgoing file" in text
    assert "Managed BBS Library location" in text


def test_messages_bbs_sweeper_applies_live_bbs_file_to_managed_location(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    managed = tmp_path / "managed" / "intel"
    live_bbs.mkdir()
    managed.mkdir(parents=True)
    src = live_bbs / "weather alert.txt"
    src.write_text("Regional weather alert", encoding="utf-8")
    stat = src.stat()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = _DictSettings(
        {
            "varac_bbs_vault_enabled": True,
            "varac_bbs_archive_dir": str(tmp_path / "archive"),
            "varac_bbs_sweeper_rules_v1": [
                {
                    "id": "weather",
                    "name": "Weather",
                    "enabled": True,
                    "source_families": ["varac_bbs"],
                    "subject_contains": ["weather"],
                    "target_location_ids": ["intel"],
                    "copy_mode": "copy_once",
                }
            ],
            "varac_bbs_vault_locations_v1": [
                {
                    "id": "intel",
                    "name": "Intel",
                    "source_dir": str(managed),
                    "enabled": True,
                }
            ],
        }
    )

    MessageViewerTab._apply_bbs_sweeper_rules_after_file_scan(
        tab,
        {
            "bbs": [
                FileRecord(
                    path=src,
                    origin="bbs",
                    size=stat.st_size,
                    mtime=stat.st_mtime,
                )
            ],
            "flmsg": [],
            "flamp": [],
        },
    )

    copied = managed / src.name
    assert copied.exists()
    assert copied.read_text(encoding="utf-8") == "Regional weather alert"


def test_settings_bbs_add_group_family_callsigns_dedupes_allowed_list() -> None:
    _app()
    tab = SettingsTab.__new__(SettingsTab)
    tab.varac_bbs_callsigns_list = QListWidget()
    tab._varac_bbs_lookup_by_callsign = {}
    dirty = []
    tab._mark_settings_dirty = lambda: dirty.append(True)

    SettingsTab._set_varac_bbs_allowed_callsigns(tab, ["K7ETC"])
    added = SettingsTab._add_varac_bbs_allowed_callsigns(tab, ["K7ETC", "KC1VXQ", "@@@", "N0CALL/P"])

    assert added == 2
    assert SettingsTab._varac_bbs_selected_callsigns_text(tab) == "K7ETC,KC1VXQ,N0CALL"
    assert dirty == [True]


def test_settings_bbs_group_source_selection_merges_for_roster_refresh() -> None:
    tab = SettingsTab.__new__(SettingsTab)
    tab._varac_bbs_allowed_group_sources = []
    dirty = []
    tab._mark_settings_dirty = lambda: dirty.append(True)

    SettingsTab._append_varac_bbs_group_source_selection(tab, "MAGNET", ["MR08", "MR01"])
    SettingsTab._append_varac_bbs_group_source_selection(tab, "magnet", ["MR08", "MRHUB"])

    stored = json.loads(SettingsTab._varac_bbs_group_source_selections_json(tab))
    assert stored == [
        {
            "family": "MAGNET",
            "groups": ["MR01", "MR08", "MRHUB"],
            "mode": "trusted_callsigns",
        }
    ]
    assert dirty == [True, True]

    clone = SettingsTab.__new__(SettingsTab)
    SettingsTab._set_varac_bbs_group_source_selections(clone, json.dumps(stored))
    assert SettingsTab._varac_bbs_group_source_selections_json(clone) == SettingsTab._varac_bbs_group_source_selections_json(tab)


def test_varac_bbs_source_core_merges_summarizes_and_removes_sources() -> None:
    sources = append_group_source_selection([], "MAGNET", ["MR08", "MR01"])
    sources = append_group_source_selection(sources, "magnet", ["MR08", "MRHUB"])

    assert json.loads(group_source_selections_json(sources)) == [
        {
            "family": "MAGNET",
            "groups": ["MR01", "MR08", "MRHUB"],
            "mode": "trusted_callsigns",
        }
    ]
    assert group_source_summary_text(sources) == "Roster source: MAGNET / MR01, MR08, MRHUB"
    kept, removed = remove_group_source_indexes(sources, [0])
    assert removed == 1
    assert kept == []


def test_settings_bbs_group_source_summary_is_operator_readable() -> None:
    tab = SettingsTab.__new__(SettingsTab)
    tab._varac_bbs_allowed_group_sources = []
    assert SettingsTab._varac_bbs_group_source_summary_text(tab) == (
        "Roster source: none. Allowed callsigns are manual or synced from VarAC.ini."
    )

    SettingsTab._set_varac_bbs_group_source_selections(
        tab,
        [{"family": "MAGNET", "groups": ["MR08", "MRHUB"], "mode": "trusted_callsigns"}],
    )
    assert SettingsTab._varac_bbs_group_source_summary_text(tab) == "Roster source: MAGNET / MR08, MRHUB"


def test_settings_bbs_roster_refresh_button_tracks_saved_source_state() -> None:
    _app()
    tab = SettingsTab.__new__(SettingsTab)
    tab._varac_bbs_allowed_group_sources = []
    tab.varac_bbs_group_source_label = QLabel()
    tab.varac_bbs_refresh_roster_btn = QPushButton("Refresh From Roster")
    tab.varac_bbs_manage_source_btn = QPushButton("Manage Source")

    SettingsTab._refresh_varac_bbs_group_source_summary(tab)
    assert not tab.varac_bbs_refresh_roster_btn.isEnabled()
    assert not tab.varac_bbs_manage_source_btn.isEnabled()
    assert "No roster source" in tab.varac_bbs_refresh_roster_btn.toolTip()

    SettingsTab._set_varac_bbs_group_source_selections(
        tab,
        [{"family": "MAGNET", "groups": ["MR08"], "mode": "trusted_callsigns"}],
    )
    assert tab.varac_bbs_refresh_roster_btn.isEnabled()
    assert tab.varac_bbs_manage_source_btn.isEnabled()
    assert tab.varac_bbs_group_source_label.text() == "Roster source: MAGNET / MR08"


def test_settings_bbs_allowed_callsigns_actions_are_split_into_manual_and_roster_rows() -> None:
    text = (Path(__file__).resolve().parents[1] / "freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    manual_row_idx = text.index("bbs_callsigns_lookup_row = QHBoxLayout()")
    roster_row_idx = text.index("bbs_roster_actions_row = QHBoxLayout()")
    list_idx = text.index("self.varac_bbs_callsigns_list = QListWidget()")

    assert manual_row_idx < roster_row_idx < list_idx
    assert text.index("bbs_callsigns_lookup_row.addWidget(self.varac_bbs_remove_callsign_btn)") < roster_row_idx
    assert text.index("bbs_roster_actions_row.addWidget(self.varac_bbs_add_group_family_btn)") > roster_row_idx
    assert text.index("bbs_roster_actions_row.addWidget(self.varac_bbs_refresh_roster_btn)") > roster_row_idx
    assert text.index("bbs_roster_actions_row.addWidget(self.varac_bbs_manage_source_btn)") > roster_row_idx


def test_settings_bbs_remove_group_source_keeps_allowed_callsigns() -> None:
    _app()
    tab = SettingsTab.__new__(SettingsTab)
    tab.varac_bbs_callsigns_list = QListWidget()
    tab._varac_bbs_lookup_by_callsign = {}
    tab._varac_bbs_allowed_group_sources = [
        {"family": "MAGNET", "groups": ["MR08"], "mode": "trusted_callsigns"},
        {"family": "AMRRON", "groups": ["AMRRON"], "mode": "trusted_callsigns"},
    ]
    dirty = []
    tab._mark_settings_dirty = lambda: dirty.append(True)
    SettingsTab._set_varac_bbs_allowed_callsigns(tab, ["K7ETC", "KC1VXQ"])

    removed = SettingsTab._remove_varac_bbs_group_source_indexes(tab, [0])

    assert removed == 1
    assert SettingsTab._varac_bbs_selected_callsigns_text(tab) == "K7ETC,KC1VXQ"
    assert json.loads(SettingsTab._varac_bbs_group_source_selections_json(tab)) == [
        {"family": "MAGNET", "groups": ["MR08"], "mode": "trusted_callsigns"}
    ]
    assert dirty == [True]


def test_settings_bbs_roster_refresh_plan_adds_and_removes_from_saved_group_source() -> None:
    _app()
    tab = SettingsTab.__new__(SettingsTab)
    tab.varac_bbs_callsigns_list = QListWidget()
    tab._varac_bbs_lookup_by_callsign = {}
    tab._varac_bbs_allowed_group_sources = [
        {"family": "MAGNET", "groups": ["MR08"], "mode": "trusted_callsigns"}
    ]
    SettingsTab._set_varac_bbs_allowed_callsigns(tab, ["K7ETC", "OLD1"])
    families = {
        "MAGNET": OperatorGroupFamily(
            parent="MAGNET",
            members=("MAGNET", "MR08"),
            trusted_callsigns=("K7ETC", "KC1VXQ"),
            total_callsigns=3,
            trusted_callsigns_by_group=(
                ("MAGNET", ("K7ETC", "KC1VXQ")),
                ("MR08", ("K7ETC", "KC1VXQ")),
            ),
        )
    }

    plan = SettingsTab._varac_bbs_roster_refresh_plan(tab, families)

    assert plan["groups"] == ["MR08"]
    assert plan["desired"] == ["K7ETC", "KC1VXQ"]
    assert plan["added"] == ["KC1VXQ"]
    assert plan["removed"] == ["OLD1"]
    assert plan["unchanged"] == ["K7ETC"]

    core_plan = roster_refresh_plan(tab._varac_bbs_allowed_group_sources, ["K7ETC", "OLD1"], families)
    assert core_plan["added"] == ["KC1VXQ"]
    assert core_plan["removed"] == ["OLD1"]


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
    assert "LimitAccessToCallsignsList=K7RIE,KG5RKW" in rewritten


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
