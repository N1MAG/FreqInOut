from __future__ import annotations

from pathlib import Path

import pytest

from freqinout.core.fldigi_macro_parser import (
    count_detected_file_references,
    parse_macro_profile_text,
    rewrite_macro_file_reference_text,
    rewrite_macro_profile_file_reference,
    scan_macro_profile,
)
from freqinout.core.fldigi_macro_profile import (
    FldigiMacroProfileStore,
    normalize_macro_mapping_source_path,
    standard_macro_mapping_source_filename,
)
from freqinout.core.settings_manager import SettingsManager


class FakeSettings:
    def __init__(self):
        self._data = {}

    def all(self):
        return self._data

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value


FIXTURES = Path(__file__).parent / "fixtures"


def test_sample_fixture_discovers_multiple_file_backed_macros():
    result = scan_macro_profile(str(FIXTURES / "fldigi_macro_profile_sample.mdf"))

    assert result["profile_name"] == "fldigi_macro_profile_sample"
    assert len(result["detected_macros"]) == 5
    assert count_detected_file_references(result) == 3

    macro_ids = [macro["macro_id"] for macro in result["detected_macros"] if macro["detected_files"]]
    assert macro_ids == ["slot_03", "slot_04"]

    first_file_macro = result["detected_macros"][2]
    assert first_file_macro["confidence"] == "high"
    assert first_file_macro["detected_files"] == [
        "/home/tta/.fldigi/macros/MacroTxt/CheckIns_deANCS_TRAFFIC.txt"
    ]
    assert first_file_macro["review_files"] == []


def test_manual_fallback_fixture_uses_absolute_path_detection():
    result = scan_macro_profile(str(FIXTURES / "fldigi_macro_profile_manual_fallback.mdf"))

    assert len(result["detected_macros"]) == 2
    first_macro = result["detected_macros"][0]
    assert first_macro["confidence"] == "review"
    assert first_macro["detected_files"] == []
    assert first_macro["review_files"] == [
        "/opt/fldigi/macros/notes/custom_checkin.txt",
        "/opt/fldigi/macros/notes/relay_summary.txt",
    ]
    assert result["detected_macros"][1]["detected_files"] == []
    assert result["detected_macros"][1]["confidence"] == "low"


def test_mixed_structured_and_fallback_file_references_keep_review_guesses_separate():
    text = """// Macro #0
/$ 0 Mixed Reference
Send to <FILE:/opt/fldigi/macros/confirmed.txt> and maybe /opt/fldigi/macros/guess.txt
"""

    macro = parse_macro_profile_text(text).detected_macros[0].as_dict()

    assert macro["confidence"] == "high"
    assert macro["detected_files"] == ["/opt/fldigi/macros/confirmed.txt"]
    assert macro["review_files"] == ["/opt/fldigi/macros/guess.txt"]


def test_fldigi_file_macro_slash_forms_normalize_to_local_paths():
    text = """// Macro #0
/$ 0 Close
<FILE://Users/bill/Radio/nets/CheckIns_ALL.txt>
// Macro #1
/$ 1 Triple
<FILE:///Users/bill/Radio/nets/CheckIns_TFC.txt>
// Macro #2
/$ 2 Windows
<FILE:C:\\Users\\Bill\\Radio\\nets\\CheckIns_QRU.txt>
// Macro #3
/$ 3 UNC
<FILE://SERVER/share/custom.txt>
"""

    macros = parse_macro_profile_text(text).as_dict()["detected_macros"]

    assert macros[0]["detected_files"] == ["/Users/bill/Radio/nets/CheckIns_ALL.txt"]
    assert macros[1]["detected_files"] == ["/Users/bill/Radio/nets/CheckIns_TFC.txt"]
    assert macros[2]["detected_files"] == [r"C:\Users\Bill\Radio\nets\CheckIns_QRU.txt"]
    assert macros[3]["detected_files"] == ["//SERVER/share/custom.txt"]


def test_rewrite_macro_file_reference_updates_only_selected_slot_and_file():
    text = """//fldigi macro definition file extended
// Macro # 1
/$ 0 FIRST
<FILE:/home/bill/Radio/nets/CheckIns_ALL.txt>
// Macro # 2
/$ 1 SECOND
<FILE:/home/bill/Radio/nets/CheckIns_ALL.txt>
<FILE:/home/bill/Radio/nets/CheckIns_QRU.txt>
"""

    updated, replacements = rewrite_macro_file_reference_text(
        text,
        macro_id="slot_02",
        old_path="/home/bill/Radio/nets/CheckIns_ALL.txt",
        new_path="/Users/bill/Radio/nets/CheckIns_ALL.txt",
    )

    assert replacements == 1
    assert "<FILE:/home/bill/Radio/nets/CheckIns_ALL.txt>" in updated
    assert "<FILE:/Users/bill/Radio/nets/CheckIns_ALL.txt>" in updated
    assert "<FILE:/home/bill/Radio/nets/CheckIns_QRU.txt>" in updated


def test_rewrite_macro_profile_file_reference_creates_backup(tmp_path):
    profile = tmp_path / "macros.mdf"
    profile.write_text(
        """//fldigi macro definition file extended
// Macro # 1
/$ 0 CLOSE
<FILE://Users/bill/Radio/nets/CheckIns_ALL.txt>
""",
        encoding="utf-8",
    )

    result = rewrite_macro_profile_file_reference(
        str(profile),
        macro_id="slot_01",
        old_path="/Users/bill/Radio/nets/CheckIns_ALL.txt",
        new_path="/Users/bill/FIO/nets/CheckIns_ALL.txt",
    )

    assert result["ok"] is True
    assert result["replacements"] == 1
    assert Path(str(result["backup_path"])).exists()
    assert "<FILE:/Users/bill/FIO/nets/CheckIns_ALL.txt>" in profile.read_text(encoding="utf-8")
    assert "<FILE://Users/bill/Radio/nets/CheckIns_ALL.txt>" in Path(str(result["backup_path"])).read_text(encoding="utf-8")


def test_standard_macro_roster_paths_follow_configured_checkin_dir(tmp_path):
    settings = FakeSettings()
    settings.set("fldigi_checkin_dir", str(tmp_path / "nets"))

    assert normalize_macro_mapping_source_path("/home/bill/Radio/nets/main_checkins.txt", settings) == str(
        tmp_path / "nets" / "CheckIns_TFC.txt"
    )
    assert normalize_macro_mapping_source_path(r"C:\Users\Bill\Radio\nets\new-late_checkins.txt", settings) == str(
        tmp_path / "nets" / "CheckIns_LATE.txt"
    )
    assert normalize_macro_mapping_source_path("/Users/bill/Radio/nets/CheckIns_ALL.txt", settings) == str(
        tmp_path / "nets" / "CheckIns_ALL.txt"
    )
    assert normalize_macro_mapping_source_path(r"C:\Users\Bill\Radio\nets\NCS_ACK_Pending.txt", settings) == str(
        tmp_path / "nets" / "NCS_ACK_Pending.txt"
    )
    assert normalize_macro_mapping_source_path("/home/bill/Radio/nets/ANCS_Next_TFC.txt", settings) == str(
        tmp_path / "nets" / "ANCS_Next_TFC.txt"
    )


def test_standard_macro_source_warning_detects_configured_dir_mismatch(tmp_path):
    from freqinout.gui.fldigi_macro_mapping_dialog import FldigiMacroMappingDialog

    settings = FakeSettings()
    settings.set("fldigi_checkin_dir", str(tmp_path / "nets"))
    dialog = FldigiMacroMappingDialog.__new__(FldigiMacroMappingDialog)
    dialog.settings = settings

    configured = normalize_macro_mapping_source_path("/home/bill/Radio/nets/CheckIns_ALL.txt", settings)
    warning = dialog._source_path_warning("/home/bill/Radio/nets/CheckIns_ALL.txt", configured)

    assert standard_macro_mapping_source_filename("/home/bill/Radio/nets/CheckIns_ALL.txt") == "CheckIns_ALL.txt"
    assert "Macro text points to /home/bill/Radio/nets/CheckIns_ALL.txt" in warning
    assert configured in warning
    assert dialog._source_path_warning(configured, configured) == ""
    assert dialog._source_path_warning("/home/bill/Radio/nets/custom.txt", configured) == ""


def test_custom_macro_paths_are_not_rewritten(tmp_path):
    settings = FakeSettings()
    settings.set("fldigi_checkin_dir", str(tmp_path / "nets"))

    assert normalize_macro_mapping_source_path(r"C:\Tools\custom.txt", settings) == r"C:\Tools\custom.txt"
    assert normalize_macro_mapping_source_path("/Users/bill/Radio/nets/custom.txt", settings) == "/Users/bill/Radio/nets/custom.txt"


def test_profile_store_persists_mappings_by_profile_path(tmp_path):
    settings = FakeSettings()
    store = FldigiMacroProfileStore(settings)
    profile_path = str(FIXTURES / "fldigi_macro_profile_sample.mdf")

    saved = store.upsert_mappings(
        profile_path,
        [
            {
                "scope": "NCS",
                "function": "TFC",
                "custom_name": "",
                "macro_id": "slot_03",
                "macro_label": "Send FLAMP",
                "source_file": "/home/tta/.fldigi/macros/MacroTxt/CheckIns_deANCS_TRAFFIC.txt",
                "read_only": False,
                "enabled": True,
            },
            {
                "scope": "NCS",
                "function": "CUSTOM",
                "custom_name": "CUSTOM_1",
                "macro_id": "slot_04",
                "macro_label": "Ack TFC ONLY",
                "source_file": "/home/tta/.fldigi/macros/MacroTxt/CheckIns_TRAFFIC.txt",
                "read_only": False,
                "enabled": True,
            },
        ],
    )

    assert store.profile_mode(profile_path) == "mapped"
    assert store.has_enabled_mappings(saved)
    assert store.complete_mappings(saved) == saved["mappings"]
    assert "complete mappings saved" in FldigiMacroProfileStore.summary_text(saved)
    assert settings.get("fldigi_selected_macro_profile") == str(Path(profile_path).expanduser())

    reloaded = FldigiMacroProfileStore(settings).get_record(profile_path)
    assert len(reloaded["mappings"]) == 2
    assert reloaded["mappings"][1]["custom_name"] == "CUSTOM_1"


def test_profile_store_filters_incomplete_mappings_from_complete_sets():
    settings = FakeSettings()
    store = FldigiMacroProfileStore(settings)
    profile_path = str(FIXTURES / "fldigi_macro_profile_sample.mdf")
    store.upsert_mappings(
        profile_path,
        [
            {
                "scope": "NCS",
                "function": "",
                "custom_name": "",
                "macro_id": "slot_03",
                "macro_label": "Send FLAMP",
                "source_file": "/home/tta/.fldigi/macros/MacroTxt/CheckIns_deANCS_TRAFFIC.txt",
                "read_only": False,
                "enabled": True,
            },
            {
                "scope": "NCS",
                "function": "TFC",
                "custom_name": "",
                "macro_id": "slot_03",
                "macro_label": "Send FLAMP",
                "source_file": "/home/tta/.fldigi/macros/MacroTxt/CheckIns_deANCS_TRAFFIC.txt",
                "read_only": False,
                "enabled": True,
            },
        ],
    )

    reloaded = FldigiMacroProfileStore(settings).get_record(profile_path)
    assert len(reloaded["mappings"]) == 2
    assert len(store.complete_mappings(reloaded)) == 1
    assert store.profile_mode(profile_path) == "mapped"
    assert "1 complete mappings saved" in store.summary_text(reloaded)


def test_mapping_should_persist_distinguishes_pristine_discovered_and_saved_rows():
    settings = FakeSettings()
    store = FldigiMacroProfileStore(settings)

    original = {
        "scope": "",
        "function": "",
        "custom_name": "",
        "macro_id": "slot_03",
        "macro_label": "Send FLAMP",
        "source_file": "",
        "read_only": False,
        "enabled": False,
    }
    pristine = dict(original)
    edited_incomplete = dict(original, function="TFC", enabled=False)
    saved_incomplete = dict(original, function="TFC", enabled=False)

    assert store.mapping_should_persist(pristine, original, origin="discovered") is False
    assert store.mapping_should_persist(edited_incomplete, original, origin="discovered") is True
    assert store.mapping_should_persist(saved_incomplete, original, origin="saved") is True


def test_mapping_should_persist_ignores_unedited_confidence_only_discovery_state():
    settings = FakeSettings()
    store = FldigiMacroProfileStore(settings)

    original = {
        "confidence": "high",
        "scope": "",
        "function": "",
        "custom_name": "",
        "macro_id": "slot_03",
        "macro_label": "Send FLAMP",
        "source_file": "/opt/fldigi/macros/confirmed.txt",
        "read_only": False,
        "enabled": False,
    }
    discovered_snapshot = dict(original)

    assert store.mapping_should_persist(discovered_snapshot, original, origin="discovered") is False

    edited_snapshot = dict(original, scope="NCS", function="TFC", enabled=True)
    assert store.mapping_should_persist(edited_snapshot, original, origin="discovered") is True


def test_profile_store_ignores_placeholder_mappings_for_mapped_mode():
    settings = FakeSettings()
    store = FldigiMacroProfileStore(settings)
    profile_path = str(FIXTURES / "fldigi_macro_profile_sample.mdf")
    store.upsert_mappings(
        profile_path,
        [
            {
                "scope": "NCS",
                "function": "TFC",
                "custom_name": "",
                "macro_id": "",
                "macro_label": "Placeholder",
                "source_file": "",
                "read_only": False,
                "enabled": True,
            }
        ],
    )

    assert store.profile_mode(profile_path) == "legacy"
    assert store.has_enabled_mappings(store.get_record(profile_path)) is False


@pytest.mark.parametrize("existing_names, expected", [
    ([{"function": "CUSTOM", "custom_name": "CUSTOM_1"}], "CUSTOM_2"),
    ([{"function": "CUSTOM", "custom_name": "CUSTOM_2"}, {"function": "CUSTOM", "custom_name": "CUSTOM_4"}], "CUSTOM_1"),
    ([{"function": "TFC", "custom_name": ""}], "CUSTOM_1"),
])
def test_next_custom_name_uses_profile_scoped_numeric_fallback(existing_names, expected):
    assert FldigiMacroProfileStore.next_custom_name(existing_names) == expected


def test_real_fldigi_fixture_is_parsed_when_available():
    real_fixture = Path("/Users/bill/.fldigi/macros/W5TTA_MR06_macros_20260414.mdf")
    if not real_fixture.exists():
        pytest.skip("Real FLDigi fixture not available in this environment")

    result = scan_macro_profile(str(real_fixture))
    assert len(result["detected_macros"]) >= 40
    assert count_detected_file_references(result) >= 10
    assert any(len(macro["detected_files"]) > 1 for macro in result["detected_macros"])


def test_custom_mappings_appear_as_workspace_cards_and_follow_role_visibility(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    settings = SettingsManager()
    profile_path = tmp_path / "macros" / "role_custom.mdf"
    source_path = tmp_path / "macros" / "word_of_the_week.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text("// Macro #0\n/$ 0 WOTW\n", encoding="utf-8")
    source_path.write_text("Word of the week line\n", encoding="utf-8")

    FldigiMacroProfileStore(settings).upsert_mappings(
        str(profile_path),
        [
            {
                "scope": "NCS",
                "function": "CUSTOM",
                "custom_name": "Word Of The Week",
                "macro_id": "slot_11",
                "macro_label": "WOTW",
                "source_file": str(source_path),
                "read_only": False,
                "enabled": True,
            }
        ],
    )

    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()
    try:
        custom_cards = {
            bucket_id: card
            for bucket_id, card in tab._workspace_bucket_cards.items()
            if bucket_id.startswith("custom::")
        }
        assert custom_cards
        assert any(card.isVisibleTo(tab) and card.title() == "Word Of The Week" for card in custom_cards.values())
        assert any(label == "Word Of The Week" for _, label in tab._workspace_bucket_options())

        tab.role_combo.setCurrentText("Joiner")
        app.processEvents()

        assert not any(card.isVisibleTo(tab) for card in custom_cards.values())
        assert settings.get("fldigi_selected_macro_profile") == str(profile_path.expanduser().resolve())
        assert any(mapping.get("function") == "CUSTOM" for mapping in FldigiMacroProfileStore(settings).get_record(str(profile_path)).get("mappings", []))
    finally:
        tab.deleteLater()


def test_custom_workspace_cards_clear_stale_text_on_empty_or_blank_source(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.core.fldigi_macro_profile import FldigiMacroProfileStore
    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    profile_path = tmp_path / "macros" / "role_custom.mdf"
    source_path = tmp_path / "macros" / "word_of_the_week.txt"
    empty_path = tmp_path / "macros" / "empty.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text("// Macro #0\n/$ 0 WOTW\n", encoding="utf-8")
    source_path.write_text("Word of the week line\n", encoding="utf-8")
    empty_path.write_text("", encoding="utf-8")

    settings = SettingsManager()
    store = FldigiMacroProfileStore(settings)
    store.upsert_mappings(
        str(profile_path),
        [
            {
                "scope": "NCS",
                "function": "CUSTOM",
                "custom_name": "Word Of The Week",
                "macro_id": "slot_11",
                "macro_label": "WOTW",
                "source_file": str(source_path),
                "read_only": False,
                "enabled": True,
            }
        ],
    )

    tab = FldigiNetControlTab()
    tab.settings = settings
    tab._set_macro_profile_text(str(profile_path))
    tab._save_macro_profile_selection(str(profile_path), refresh_metadata=False)
    tab._refresh_custom_bucket_cards()
    try:
        custom_cards = [card for bucket_id, card in tab._workspace_bucket_cards.items() if bucket_id.startswith("custom::")]
        assert custom_cards
        custom_card = custom_cards[0]
        assert custom_card.text().strip() == "Word of the week line"

        store.upsert_mappings(
            str(profile_path),
            [
                {
                    "scope": "NCS",
                    "function": "CUSTOM",
                    "custom_name": "Word Of The Week",
                    "macro_id": "slot_11",
                    "macro_label": "WOTW",
                    "source_file": "",
                    "read_only": False,
                    "enabled": True,
                }
            ],
        )
        tab._refresh_custom_bucket_cards()
        assert custom_card.text() == ""

        store.upsert_mappings(
            str(profile_path),
            [
                {
                    "scope": "NCS",
                    "function": "CUSTOM",
                    "custom_name": "Word Of The Week",
                    "macro_id": "slot_11",
                    "macro_label": "WOTW",
                    "source_file": str(empty_path),
                    "read_only": False,
                    "enabled": True,
                }
            ],
        )
        tab._refresh_custom_bucket_cards()
        assert custom_card.text() == ""
    finally:
        tab.deleteLater()


def test_custom_workspace_cards_participate_in_bucket_state_refresh(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    profile_path = tmp_path / "macros" / "role_custom.mdf"
    source_path = tmp_path / "macros" / "word_of_the_week.txt"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text("// Macro #0\n/$ 0 WOTW\n", encoding="utf-8")
    source_path.write_text("Word of the week line\n", encoding="utf-8")

    FldigiMacroProfileStore(SettingsManager()).upsert_mappings(
        str(profile_path),
        [
            {
                "scope": "NCS",
                "function": "CUSTOM",
                "custom_name": "Word Of The Week",
                "macro_id": "slot_11",
                "macro_label": "WOTW",
                "source_file": str(source_path),
                "read_only": False,
                "enabled": True,
            }
        ],
    )

    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()
    try:
        custom_cards = [card for bucket_id, card in tab._workspace_bucket_cards.items() if bucket_id.startswith("custom::")]
        assert custom_cards

        custom_card = custom_cards[0]
        tab.qru_text.setPlainText("K2BBB / Bob / AZ\n")
        custom_card.set_text("Word of the week line\n")
        tab._update_bucket_card_states()

        assert custom_card.copy_btn.styleSheet() == tab.qru_card.copy_btn.styleSheet()
    finally:
        tab.deleteLater()


def test_reference_aliases_return_the_visible_reference_text(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()
    try:
        tab.reference_text.setPlainText("N1ABC / Alice / CO\nN2XYZ / Bob / AZ\n")
        assert tab._workspace_bucket_text("reference") == tab.reference_text.toPlainText()
        assert tab._workspace_bucket_text("ncs_reference") == tab.reference_text.toPlainText()
        assert tab._workspace_bucket_text("ancs_reference") == tab.reference_text.toPlainText()
    finally:
        tab.deleteLater()


def test_joiner_seen_locally_compare_source_uses_visible_tfc_text(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    tab = FldigiNetControlTab()
    try:
        tab.role_combo.setCurrentText("Joiner")
        tab.main_text.setPlainText("K1AAA / Alice / CO\n")
        defaults = tab._workspace_compare_defaults()
        assert defaults["source_bucket_id"] == "seen_locally"
        assert tab._workspace_bucket_text(defaults["source_bucket_id"]) == tab.main_text.toPlainText()
    finally:
        tab.deleteLater()


def test_copy_button_state_updates_use_workspace_bucket_cards(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    tab = FldigiNetControlTab()
    try:
        tab.main_text.setPlainText("K1AAA / Alice / CO\n")
        tab.late_text.setPlainText("K2BBB / Bob / AZ\n")
        tab._update_copy_buttons_state()
        assert hasattr(tab.tfc_card, "copy_btn")
        assert hasattr(tab.late_card, "copy_btn")
    finally:
        tab.deleteLater()


def test_ensure_checkin_files_creates_only_current_default_names(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    tab = FldigiNetControlTab()
    try:
        checkin_dir = tmp_path / "macro_files"
        tab.settings.set("fldigi_checkin_dir", str(checkin_dir))
        tab._ensure_checkin_files()

        created_names = {path.name for path in checkin_dir.iterdir() if path.is_file()}
        assert {
            "CheckIns_TFC.txt",
            "CheckIns_QRU.txt",
            "CheckIns_LATE.txt",
            "CheckIns_ALL.txt",
            "NCS_CheckIns_TFC.txt",
            "NCS_CheckIns_QRU.txt",
            "NCS_CheckIns_LATE.txt",
            "NCS_CheckIns_ALL.txt",
            "NCS_ACK_Pending.txt",
            "NCS_Next_TFC.txt",
            "NCS_CheckIns_Relays.txt",
            "ANCS_CheckIns_TFC.txt",
            "ANCS_CheckIns_QRU.txt",
            "ANCS_CheckIns_LATE.txt",
            "ANCS_CheckIns_ALL.txt",
            "ANCS_ACK_Pending.txt",
            "ANCS_Next_TFC.txt",
            "ANCS_CheckIns_Relays.txt",
        } == created_names
        assert not {
            "main_checkins.txt",
            "qru_checkins.txt",
            "new-late_checkins.txt",
            "all_checkins.txt",
            "ACK_Pending.txt",
            "Next_TFC.txt",
        } & created_names
    finally:
        tab.deleteLater()


def test_qru_checkins_are_saved_and_imported_on_end_net(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication, QMessageBox

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    import freqinout.gui.fldigi_net_control_tab as fldigi_tab_module
    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(fldigi_tab_module, "upsert_checkins", lambda entries: captured_entries.extend(entries))

    captured_entries = []
    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()
    try:
        tab.main_text.setPlainText("K1AAA / Alice / CO\n")
        tab.qru_text.setPlainText("K2BBB / Bob / AZ\n")
        tab.late_text.setPlainText("K3CCC / Carol / NM\n")

        main_path, qru_path, late_path = tab._ensure_checkin_files()
        created_names = {path.name for path in Path(main_path).parent.iterdir() if path.is_file()}
        assert {"CheckIns_TFC.txt", "CheckIns_QRU.txt", "CheckIns_LATE.txt", "CheckIns_ALL.txt"} <= created_names
        assert not {"main_checkins.txt", "qru_checkins.txt", "new-late_checkins.txt", "all_checkins.txt"} & created_names
        tab._save_checkins()

        assert Path(qru_path).read_text(encoding="utf-8") == tab.qru_text.toPlainText()
        assert Path(main_path).read_text(encoding="utf-8") == tab.main_text.toPlainText()
        assert Path(late_path).read_text(encoding="utf-8") == tab.late_text.toPlainText()

        monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
        monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
        monkeypatch.setattr(tab, "_bump_operator_history", lambda entries: None)
        tab._net_in_progress = True
        tab._end_net()

        assert {entry["callsign"] for entry in captured_entries} == {"K1AAA", "K2BBB"}
    finally:
        tab.deleteLater()


def test_roster_category_change_rewrites_macro_checkin_files(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()
    try:
        checkin_dir = tmp_path / "macro_files"
        tab.settings.set("fldigi_checkin_dir", str(checkin_dir))
        tab._net_in_progress = True

        row = tab._roster_append_row("K1ABC", "Alice", "CO", "1RR", "TFC")
        main_path, qru_path, late_path = tab._checkin_file_paths()
        assert "K1ABC / Alice / CO / 1RR" in Path(main_path).read_text(encoding="utf-8")
        assert Path(qru_path).read_text(encoding="utf-8") == ""
        assert Path(late_path).read_text(encoding="utf-8") == ""

        category_combo = tab._roster_row_widget(row)
        assert category_combo is not None
        category_combo.setCurrentText("QRU")
        app.processEvents()

        assert "K1ABC" not in Path(main_path).read_text(encoding="utf-8")
        assert "K1ABC / Alice / CO / 1RR" in Path(qru_path).read_text(encoding="utf-8")
        assert "K1ABC / Alice / CO / 1RR" in Path(tab._all_checkins_file_path()).read_text(encoding="utf-8")
    finally:
        tab.deleteLater()


def test_macro_details_show_file_locations_and_expanded_info_style(monkeypatch, tmp_path):
    from PySide6.QtWidgets import QApplication, QScrollArea

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    profile_path = tmp_path / "macros" / "mapped.mdf"
    source_path = tmp_path / "macros" / "traffic.txt"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text("// Macro #0\n/$ 0 TRAFFIC\n", encoding="utf-8")
    source_path.write_text("K1ABC / Alice / CO\n", encoding="utf-8")

    FldigiMacroProfileStore(SettingsManager()).upsert_mappings(
        str(profile_path),
        [
            {
                "scope": "NCS",
                "function": "TFC",
                "custom_name": "",
                "macro_id": "slot_00",
                "macro_label": "TRAFFIC",
                "source_file": str(source_path),
                "read_only": False,
                "enabled": True,
            }
        ],
    )

    tab = FldigiNetControlTab()
    tab.show()
    app.processEvents()
    try:
        tab.settings.set("fldigi_checkin_dir", str(tmp_path / "checkins"))
        tab._set_macro_profile_text(str(profile_path))
        tab._save_macro_profile_selection(str(profile_path), refresh_metadata=False)
        tab._set_setup_details_expanded(True)
        app.processEvents()

        assert tab.copy_roster_summary_btn.text() == "All Check-ins"
        assert isinstance(tab._ncs_scroll_area, QScrollArea)
        assert tab.setup_details_frame.isVisibleTo(tab)
        assert tab.macro_profile_details_btn.styleSheet()
        assert tab.macro_profile_details_btn.text() == "Macro: Mapped - mapped"
        assert not tab.macro_setup_controls.isVisibleTo(tab)
        locations = tab.macro_mapping_locations_label.text()
        assert "CheckIns_TFC.txt" in locations
        assert "CheckIns_ALL.txt" in locations
        assert "NCS_ACK_Pending.txt" in locations
        assert "ANCS_Next_TFC.txt" in locations
        assert str(source_path) in locations

        tab._set_setup_details_expanded(False)
        assert tab.macro_profile_details_btn.text() == "Macro: Mapped - mapped"
        assert tab.macro_profile_details_btn.styleSheet()
    finally:
        tab.deleteLater()
