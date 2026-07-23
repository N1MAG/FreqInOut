from __future__ import annotations

from pathlib import Path

from freqinout.core.shared_state import ActionFeedbackService
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


def test_settings_tab_source_keeps_radio_context_labels_in_sync() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "self.device_assignments_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.js8_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.fast_light_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.varac_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.custom_tools_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.launch_control_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "def _refresh_radio_context_labels(self) -> None:" in source
    assert "self._refresh_software_scope_labels()" in source
    assert "self._refresh_device_assignment_scope_label()" in source
    focus_block = source[
        source.index("def _set_settings_radio_focus") : source.index("def _refresh_radio_settings_nav_label")
    ]
    assert "self._refresh_radio_context_labels()" in focus_block


def test_settings_action_feedback_helper_publishes_settings_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None

    tab._publish_settings_action_feedback(
        status="succeeded",
        summary="Saved settings for DX10.",
        radio_profile_id="7",
        target_label="DX10",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "succeeded"
    assert events[0].summary == "Saved settings for DX10."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "settings"
    assert tab._last_action_feedback_event == events[0]


def test_settings_save_success_uses_feedback_instead_of_success_popup() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    save_block = source[source.index("def _save_settings(") : source.index("def _on_theme_changed")]

    assert "Settings saved." in save_block
    assert "_publish_settings_action_feedback(" in save_block
    assert 'QMessageBox.information(self, "Settings", "Settings saved.")' not in save_block


def test_settings_prompt_interval_validation_uses_blocked_feedback() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    save_start = source.index("def _save_settings(")
    save_block = source[save_start : source.index("data[\"freq_enforcement_mode\"]", save_start)]

    assert "Save blocked: select" in save_block
    assert "_block_settings_action(" in save_block
    old_prompt_modal = 'QMessageBox.warning(self, "Settings", f"Please select: {\', \'.join(missing)}.")'
    assert old_prompt_modal not in save_block


def test_settings_blocked_feedback_helper_publishes_blocked_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: (None, "Settings")

    tab._block_settings_action(
        "Save blocked: select Frequency Prompt Interval.",
        "Choose a prompt interval before saving.",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "blocked"
    assert events[0].action_type == "save"
    assert events[0].summary == "Save blocked: select Frequency Prompt Interval."
    assert events[0].detail == "Choose a prompt interval before saving."
    assert events[0].source_surface == "settings"


def test_settings_launch_control_feedback_helper_publishes_history_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._publish_launch_control_feedback(
        status="in_progress",
        summary="Launch sequence started.",
        detail="FreqInOut is starting the selected configured applications.",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "launch_control"
    assert events[0].status == "in_progress"
    assert events[0].summary == "Launch sequence started."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "settings"


def test_settings_autofill_feedback_helper_publishes_configure_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._publish_autofill_feedback(
        status="partial",
        summary="Auto-fill updated JS8Call: Filled 1 field(s). Not found: 1.",
        detail="JS8Call DIRECTED.TXT: filled /tmp/DIRECTED.TXT",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "configure_automatically"
    assert events[0].status == "partial"
    assert events[0].summary.startswith("Auto-fill updated JS8Call")
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "settings"


def test_settings_autofill_feedback_status_and_labels_are_clear() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._autofill_section_label("fast_light") == "Fast Light"
    assert SettingsTab._autofill_section_label("js8") == "JS8Call"
    assert SettingsTab._autofill_section_label("varac") == "VarAC"
    assert (
        SettingsTab._autofill_feedback_status(filled_count=1, preserved_count=0, missing_count=0)
        == "succeeded"
    )
    assert (
        SettingsTab._autofill_feedback_status(filled_count=1, preserved_count=0, missing_count=1)
        == "partial"
    )
    assert (
        SettingsTab._autofill_feedback_status(filled_count=0, preserved_count=0, missing_count=1)
        == "blocked"
    )
    assert SettingsTab._autofill_health_key("js8") == "js8call"
    assert SettingsTab._autofill_health_key("fast_light") == "fast_light"
    assert (
        SettingsTab._autofill_readiness_summary("JS8Call", "JS8Call DIRECTED.TXT path missing; Forms path missing")
        == "Auto-fill needs review in JS8Call: JS8Call DIRECTED.TXT path missing."
    )
    assert SettingsTab._autofill_visible_review_text("No auto-fill changes were available.", []) == (
        "No auto-fill changes were available."
    )
    review_text = SettingsTab._autofill_visible_review_text(
        "Filled 1 field(s). Preserved 1 existing field(s). Not found: 1.",
        [
            "JS8Call install folder: filled /Applications/JS8Call.app (verified) - Found macOS app bundle",
            "DIRECTED.TXT: kept existing value; suggested /tmp/DIRECTED.TXT (high) - Found profile file",
            "JS8 Forms path: not found - No forms directory found",
            "JS8Spotter launch path: not found - No installed app found",
            "CommStat launch path: not found - No installed app found",
        ],
    )

    assert review_text.startswith("Filled 1 field(s). Preserved 1 existing field(s). Not found: 1.")
    assert "Review suggestions:" in review_text
    assert "- JS8Call install folder: filled /Applications/JS8Call.app" in review_text
    assert "- DIRECTED.TXT: kept existing value; suggested /tmp/DIRECTED.TXT" in review_text
    assert "- 1 more item" in review_text
    assert "CommStat launch path: not found" not in review_text
    assert "- 2 more items" in SettingsTab._autofill_visible_review_text(
        "Filled 1 field(s).",
        ["One", "Two", "Three"],
        max_lines=1,
    )
    suggestions_text = SettingsTab._autofill_preserved_suggestions_text(
        "JS8Call",
        [
            {
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ],
    )
    assert suggestions_text.startswith(
        "JS8Call preserved 1 existing field(s) with suggested replacement value(s):"
    )
    assert "- DIRECTED.TXT: keep /manual/DIRECTED.TXT; suggested /detected/DIRECTED.TXT (high)" in suggestions_text
    assert SettingsTab._autofill_preserved_copy_summary("JS8Call", 1) == (
        "Copied 1 preserved JS8Call Auto-Fill suggestion."
    )
    assert SettingsTab._autofill_preserved_copy_summary("Fast Light", 2) == (
        "Copied 2 preserved Fast Light Auto-Fill suggestions."
    )
    assert SettingsTab._autofill_dismiss_summary("JS8Call", 1) == (
        "Dismissed 1 preserved JS8Call Auto-Fill suggestion."
    )
    assert SettingsTab._autofill_dismiss_summary("Fast Light", 2) == (
        "Dismissed 2 preserved Fast Light Auto-Fill suggestions."
    )


def test_settings_autofill_readiness_feedback_publishes_only_when_warned() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab._build_section_health_snapshot = lambda: {
        "js8call": {"state": "warn", "detail": "JS8Call DIRECTED.TXT path missing; Forms path missing"}
    }

    tab._publish_autofill_readiness_feedback("js8")

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "configure_automatically"
    assert events[0].status == "partial"
    assert events[0].summary == "Auto-fill needs review in JS8Call: JS8Call DIRECTED.TXT path missing."
    assert "Forms path missing" in events[0].detail
    assert events[0].radio_profile_id == "7"


def test_settings_autofill_readiness_feedback_skips_when_section_is_ok() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab._build_section_health_snapshot = lambda: {
        "js8call": {"state": "ok", "detail": ""}
    }

    tab._publish_autofill_readiness_feedback("js8")

    assert service.recent(scope="settings") == []


def test_settings_save_guardrail_feedback_publishes_partial_warning_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab._current_multi_rig_guardrail_messages = lambda: (
        "Duplicate JS8Call API endpoint 127.0.0.1:2442 on active radios: DX10, Portable.",
        "Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362 on active radios: DX10, Portable.",
    )

    tab._publish_save_guardrail_feedback()

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "save_guardrails"
    assert events[0].status == "partial"
    assert events[0].summary == "Settings saved, but 2 multi-rig guardrail warnings need review."
    assert "Duplicate JS8Call API endpoint" in events[0].detail
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"


def test_settings_save_guardrail_feedback_skips_when_no_warnings() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._current_multi_rig_guardrail_messages = lambda: ()

    tab._publish_save_guardrail_feedback()

    assert service.recent(scope="settings") == []
    assert SettingsTab._save_guardrail_feedback_summary(1) == (
        "Settings saved, but 1 multi-rig guardrail warning needs review."
    )


def test_settings_save_guardrail_collection_failure_does_not_publish(monkeypatch) -> None:
    from freqinout.gui import settings_tab as settings_tab_mod
    from freqinout.gui.settings_tab import SettingsTab

    class Store:
        db_path = "/tmp/fio-missing-settings.db"

    def fail_connect(_path):
        raise OSError("database unavailable")

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab.multi_radio_store = Store()
    monkeypatch.setattr(settings_tab_mod.sqlite3, "connect", fail_connect)

    assert tab._current_multi_rig_guardrail_messages() == ()
    tab._publish_save_guardrail_feedback()

    assert service.recent(scope="settings") == []


def test_settings_guardrail_readiness_status_text_is_compact() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._guardrail_readiness_status_text(()) == ""
    assert SettingsTab._guardrail_readiness_status_text(("Duplicate JS8Call API endpoint 127.0.0.1:2442.",)) == (
        "Multi-rig guardrails: 1 persisted warning needs review.\n"
        "- Duplicate JS8Call API endpoint 127.0.0.1:2442."
    )
    text = SettingsTab._guardrail_readiness_status_text(
        (
            "Duplicate JS8Call API endpoint.",
            "Duplicate FLDigi XML-RPC endpoint.",
            "Duplicate VarAC path.",
            "Duplicate FLRig endpoint.",
        )
    )

    assert text.startswith("Multi-rig guardrails: 4 persisted warnings need review.")
    assert "- Duplicate JS8Call API endpoint." in text
    assert "- Duplicate VarAC path." in text
    assert "- 1 more warning(s)" in text
    assert "Duplicate FLRig endpoint" not in text


def test_settings_guardrail_warnings_are_visible_in_readiness_card_source() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail(") : source.index("def _set_guidance_card_state")
    ]

    assert "self.device_profile_guardrail_status_label = QLabel(\"\")" in source
    assert "deviceProfileGuardrailStatus" in source
    assert "guardrail_warnings = self._current_multi_rig_guardrail_messages()" in readiness_block
    assert "has_guardrail_warnings = self._set_device_profile_guardrail_status(guardrail_warnings)" in readiness_block
    assert "self._set_device_profile_guardrail_status(())" in readiness_block
    assert '"warning" if has_guardrail_warnings else "success"' in readiness_block


def test_settings_guardrail_review_affordance_is_non_disruptive() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    copy_block = source[
        source.index("def _copy_device_profile_guardrail_warnings(") : source.index("def _launch_sequence_feedback_status")
    ]

    assert 'self.copy_guardrail_summary_btn = QPushButton("Copy Guardrails")' in source
    assert "self.copy_guardrail_summary_btn.setVisible(False)" in source
    assert "self.copy_guardrail_summary_btn.clicked.connect(self._copy_device_profile_guardrail_warnings)" in source
    assert "button.setVisible(bool(text))" in source
    assert "button.setEnabled(bool(text))" in source
    assert 'action_type="copy_guardrails"' in copy_block
    assert "QMessageBox" not in copy_block
    assert SettingsTab._guardrail_copy_summary(1) == "Copied 1 multi-rig guardrail warning."
    assert SettingsTab._guardrail_copy_summary(2) == "Copied 2 multi-rig guardrail warnings."
    assert "No multi-rig guardrail warnings to copy." in source


def test_settings_copy_guardrail_warnings_copies_text_and_publishes_feedback(monkeypatch) -> None:
    from freqinout.gui import settings_tab as settings_tab_mod
    from freqinout.gui.settings_tab import SettingsTab

    class Clipboard:
        def __init__(self) -> None:
            self.text = ""

        def setText(self, text: str) -> None:
            self.text = text

    clipboard = Clipboard()
    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._last_device_profile_guardrail_warnings = (
        "Duplicate JS8Call API endpoint 127.0.0.1:2442.",
        "Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362.",
    )
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    monkeypatch.setattr(settings_tab_mod.QApplication, "clipboard", lambda: clipboard)

    tab._copy_device_profile_guardrail_warnings()

    assert clipboard.text == (
        "Duplicate JS8Call API endpoint 127.0.0.1:2442.\n"
        "Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362."
    )
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "copy_guardrails"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Copied 2 multi-rig guardrail warnings."
    assert events[0].detail == clipboard.text
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"


def test_settings_contextual_autofill_publishes_scan_and_result_feedback() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    attempt_block = source[source.index("def _attempt_scoped_autofill") : source.index("def _refresh_contextual_autofill_buttons")]
    apply_block = source[source.index("def _apply_autofill_results") : source.index("def _set_autofill_status")]

    assert "_publish_autofill_feedback(" in attempt_block
    assert 'summary=f"Auto-fill scanning {section_label}."' in attempt_block
    assert "_publish_autofill_feedback(" in apply_block
    assert "_autofill_feedback_status(" in apply_block
    assert "_autofill_visible_review_text(summary, detail_lines)" in apply_block
    assert "full_status = self._autofill_visible_review_text(summary, detail_lines, max_lines=len(detail_lines))" in apply_block
    assert "self._set_autofill_status(section, compact_status, detail, full_text=full_status)" in apply_block
    assert "preserved_suggestions.append(" in apply_block
    assert "self._set_autofill_preserved_suggestions(section, preserved_suggestions)" in apply_block
    assert "_publish_autofill_readiness_feedback(section)" in apply_block
    assert 'action_type="configure_automatically"' in source
    assert "QMessageBox" not in attempt_block
    assert "QMessageBox" not in apply_block


def test_settings_autofill_full_review_toggle_is_in_panel() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    setter_block = source[source.index("def _set_autofill_status") : source.index("def _autofill_target_edit")]

    assert 'self.js8_autofill_review_toggle_btn = QPushButton("Show Full Review")' in source
    assert 'self.fast_light_autofill_review_toggle_btn = QPushButton("Show Full Review")' in source
    assert 'self.varac_autofill_review_toggle_btn = QPushButton("Show Full Review")' in source
    assert "self.js8_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)" in source
    assert "self.fast_light_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)" in source
    assert "self.varac_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)" in source
    assert 'self.js8_autofill_review_toggle_btn.clicked.connect(lambda _checked=False: self._toggle_autofill_review("js8"))' in source
    assert 'self.fast_light_autofill_review_toggle_btn.clicked.connect(' in source
    assert 'lambda _checked=False: self._toggle_autofill_review("fast_light")' in source
    assert 'lambda _checked=False: self._toggle_autofill_review("varac")' in source
    assert "self._autofill_compact_status_texts[section] = compact_text" in setter_block
    assert "self._autofill_full_status_texts[section] = expanded_text" in setter_block
    assert 'button.setText("Show Full Review")' in setter_block
    assert 'button.setText("Show Less" if expanded else "Show Full Review")' in setter_block
    assert "QMessageBox" not in setter_block


def test_settings_autofill_preserved_suggestions_copy_is_non_disruptive(monkeypatch) -> None:
    from freqinout.gui import settings_tab as settings_tab_mod
    from freqinout.gui.settings_tab import SettingsTab

    class Clipboard:
        def __init__(self) -> None:
            self.text = ""

        def setText(self, text: str) -> None:
            self.text = text

    clipboard = Clipboard()
    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    monkeypatch.setattr(settings_tab_mod.QApplication, "clipboard", lambda: clipboard)

    tab._copy_autofill_preserved_suggestions("js8")

    assert "JS8Call preserved 1 existing field(s)" in clipboard.text
    assert "/manual/DIRECTED.TXT" in clipboard.text
    assert "/detected/DIRECTED.TXT" in clipboard.text
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "copy_autofill_suggestions"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Copied 1 preserved JS8Call Auto-Fill suggestion."
    assert events[0].detail == clipboard.text
    assert events[0].radio_profile_id == "7"


def test_settings_autofill_dismiss_suggestions_clears_cache_without_dirtying() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_dismiss_buttons = {}
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._dismiss_autofill_preserved_suggestions("js8")

    assert dirty_calls == []
    assert tab._autofill_preserved_suggestions["js8"] == []
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "dismiss_autofill_suggestions"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Dismissed 1 preserved JS8Call Auto-Fill suggestion."
    assert "DIRECTED.TXT: keep /manual/DIRECTED.TXT; suggested /detected/DIRECTED.TXT" in events[0].detail
    assert events[0].radio_profile_id == "7"


def test_settings_autofill_replace_suggestions_updates_cached_fields() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Edit:
        def __init__(self, value: str) -> None:
            self.value = value

        def text(self) -> str:
            return self.value

        def setText(self, value: str) -> None:
            self.value = value

    service = ActionFeedbackService()
    edit = Edit("/manual/DIRECTED.TXT")
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_target_edit = lambda key: edit if key == "js8_directed_path" else None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._refresh_section_titles = lambda: None
    tab._refresh_section_nav_health = lambda: None
    tab._refresh_contextual_autofill_buttons = lambda: None
    tab._publish_autofill_readiness_feedback = lambda _section: None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestions("js8")

    assert edit.text() == "/detected/DIRECTED.TXT"
    assert dirty_calls == ["dirty"]
    assert tab._autofill_preserved_suggestions["js8"] == []
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "replace_autofill_suggestions"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Replaced 1 JS8Call Auto-Fill suggestion."
    assert "DIRECTED.TXT: replaced /manual/DIRECTED.TXT with /detected/DIRECTED.TXT" in events[0].detail
    assert events[0].radio_profile_id == "7"


def test_settings_autofill_replace_suggestions_keeps_skipped_items_and_reports_partial() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "missing_field",
                "label": "Missing Field",
                "current": "/manual",
                "suggested": "/detected",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_target_edit = lambda _key: None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestions("js8")

    assert dirty_calls == []
    assert len(tab._autofill_preserved_suggestions["js8"]) == 1
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "partial"
    assert events[0].summary == "Replaced 0 JS8Call Auto-Fill suggestions; skipped 1 suggestion."
    assert "Missing Field: no editable target found" in events[0].detail


def test_settings_autofill_replace_suggestions_does_not_dirty_already_matching_field() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Edit:
        def __init__(self, value: str) -> None:
            self.value = value

        def text(self) -> str:
            return self.value

        def setText(self, value: str) -> None:
            self.value = value

    service = ActionFeedbackService()
    edit = Edit("/detected/DIRECTED.TXT")
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_target_edit = lambda key: edit if key == "js8_directed_path" else None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestions("js8")

    assert dirty_calls == []
    assert tab._autofill_preserved_suggestions["js8"] == []
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "succeeded"
    assert events[0].summary == "Replaced 0 JS8Call Auto-Fill suggestions."
    assert "DIRECTED.TXT: already matched /detected/DIRECTED.TXT" in events[0].detail


def test_settings_autofill_replace_suggestions_is_wired_without_modal() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    replace_block = source[
        source.index("def _replace_autofill_preserved_suggestions")
        : source.index("def _autofill_target_edit")
    ]
    dismiss_block = source[
        source.index("def _dismiss_autofill_preserved_suggestions")
        : source.index("def _autofill_replace_summary")
    ]

    assert 'self.js8_autofill_replace_btn = QPushButton("Replace Suggested")' in source
    assert 'self.fast_light_autofill_replace_btn = QPushButton("Replace Suggested")' in source
    assert 'self.varac_autofill_replace_btn = QPushButton("Replace Suggested")' in source
    assert 'self.js8_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")' in source
    assert 'self.fast_light_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")' in source
    assert 'self.varac_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")' in source
    assert 'lambda _checked=False: self._replace_autofill_preserved_suggestions("js8")' in source
    assert 'lambda _checked=False: self._replace_autofill_preserved_suggestions("fast_light")' in source
    assert 'lambda _checked=False: self._replace_autofill_preserved_suggestions("varac")' in source
    assert 'lambda _checked=False: self._dismiss_autofill_preserved_suggestions("js8")' in source
    assert 'lambda _checked=False: self._dismiss_autofill_preserved_suggestions("fast_light")' in source
    assert 'lambda _checked=False: self._dismiss_autofill_preserved_suggestions("varac")' in source
    assert 'action_type="replace_autofill_suggestions"' in replace_block
    assert 'action_type="dismiss_autofill_suggestions"' in dismiss_block
    assert "_mark_settings_dirty()" in replace_block
    assert "QMessageBox" not in replace_block
    assert "QMessageBox" not in dismiss_block


def test_settings_save_success_runs_persisted_guardrail_feedback_after_save_event() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    save_block = source[source.index("def _save_settings(") : source.index("def _on_theme_changed")]
    show_message_block = save_block[save_block.index("if show_message:") : save_block.index("# Persist operator grid")]

    assert "_publish_settings_action_feedback(" in show_message_block
    assert "_publish_save_guardrail_feedback()" in show_message_block
    assert show_message_block.index("_publish_settings_action_feedback(") < show_message_block.index(
        "_publish_save_guardrail_feedback()"
    )
    assert 'action_type="save_guardrails"' in source
    assert "multi_rig_guardrail_warnings(conn)" in source


def test_settings_launch_sequence_feedback_summary_classifies_results() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert (
        SettingsTab._launch_sequence_feedback_status(
            launched=2,
            already_running=1,
            failed=0,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "succeeded"
    )
    assert (
        SettingsTab._launch_sequence_feedback_status(
            launched=1,
            already_running=0,
            failed=1,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "partial"
    )
    assert (
        SettingsTab._launch_sequence_feedback_status(
            launched=0,
            already_running=0,
            failed=1,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "failed"
    )
    assert (
        SettingsTab._launch_sequence_feedback_summary(
            launched=2,
            already_running=1,
            failed=0,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "Launch complete: launched 2, already running 1, failed 0, timeout 0, blocked 0."
    )
    detail = SettingsTab._launch_sequence_feedback_detail(
        launched=2,
        already_running=1,
        failed=0,
        timeout=0,
        blocked_self=0,
        cancelled=True,
    )
    assert "Launched: 2" in detail
    assert "Already running: 1" in detail
    assert "Cancelled: Yes" in detail


def test_settings_launch_control_manual_run_uses_feedback_instead_of_info_popups() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    launch_block = source[source.index("def _launch_configured_now") : source.index("def _stop_launch_sequence")]
    finish_block = source[source.index("def _on_launch_sequence_finished") : source.index("# ---------- TIME / TIMEZONE")]

    assert "_publish_launch_control_feedback(" in launch_block
    assert 'QMessageBox.information(self, "Launch Control"' not in launch_block
    assert "_launch_sequence_feedback_status(" in finish_block
    assert "_launch_sequence_feedback_summary(" in finish_block
    assert "_launch_sequence_feedback_detail(" in finish_block
    assert 'QMessageBox.information(\n                self,\n                "Launch Summary"' not in finish_block


def test_settings_human_join_formats_missing_prompt_intervals() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._human_join([]) == ""
    assert SettingsTab._human_join(["Frequency Prompt Interval"]) == "Frequency Prompt Interval"
    assert (
        SettingsTab._human_join(["Frequency Prompt Interval", "JS8 Prompt Interval"])
        == "Frequency Prompt Interval and JS8 Prompt Interval"
    )
    assert (
        SettingsTab._human_join(
            ["Frequency Prompt Interval", "FLDigi Prompt Interval", "JS8 Prompt Interval"]
        )
        == "Frequency Prompt Interval, FLDigi Prompt Interval, and JS8 Prompt Interval"
    )


def test_settings_dirty_tracking_resets_stale_feedback_label() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    dirty_block = source[source.index("def _mark_settings_dirty") : source.index("def _on_sop_export_text_changed")]

    assert '_set_settings_action_feedback_status("in_progress", "Unsaved settings changes.")' in dirty_block


def test_main_window_wires_shared_action_feedback_service_to_settings() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "from freqinout.core.shared_state import ActionFeedbackEvent, ActionFeedbackService" in source
    assert "self.action_feedback_service = ActionFeedbackService()" in source
    assert "SettingsTab(self, action_feedback_service=self.action_feedback_service)" in source


def test_main_window_has_settings_feedback_banner_subscriber() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "from freqinout.core.shared_state import ActionFeedbackEvent, ActionFeedbackService" in source
    assert "self.action_feedback_banner = QFrame(right_container)" in source
    assert 'self.action_feedback_banner.setAccessibleName("Action feedback")' in source
    assert "self.action_feedback_label = QLabel(\"\")" in source
    assert 'self.action_feedback_label.setAccessibleName("Action feedback message")' in source
    assert "self._action_feedback_unsubscribe = self.action_feedback_service.subscribe(self._on_action_feedback_event)" in source
    assert "def _on_action_feedback_event(self, event: ActionFeedbackEvent) -> None:" in source
    assert "event.scope" in source
    assert '!= "settings"' in source
    assert "self.action_feedback_banner.setVisible(True)" in source
    assert "def _hide_action_feedback_banner(self) -> None:" in source
    assert 'getattr(self, "_action_feedback_unsubscribe", None)' in source
    assert 'self.action_feedback_history_btn.setText("History")' in source
    assert "self.action_feedback_history_btn.clicked.connect(self._show_recent_actions_dialog)" in source
    assert 'self.action_feedback_history_btn.setAccessibleName("Recent Settings actions")' in source
    assert "def _show_recent_actions_dialog(self) -> None:" in source
    assert 'self.action_feedback_service.recent(scope="settings")[:20]' in source
    assert 'existing.show()' in source
    assert 'dialog.setAccessibleName("Recent Settings actions")' in source
    assert 'line.setAccessibleName(line.text())' in source


def test_main_window_action_feedback_banner_status_timing_is_modest() -> None:
    from freqinout.gui.main_window import MainWindow

    assert MainWindow._action_feedback_banner_role("succeeded") == "success"
    assert MainWindow._action_feedback_banner_role("blocked") == "warning"
    assert MainWindow._action_feedback_banner_role("in_progress") == "info"
    assert MainWindow._action_feedback_display_ms("succeeded") == 6000
    assert MainWindow._action_feedback_display_ms("blocked") == 12000
    assert MainWindow._action_feedback_display_ms("in_progress") == 7000


def test_main_window_recent_action_line_formats_settings_events() -> None:
    from freqinout.core.shared_state import ActionFeedbackEvent
    from freqinout.gui.main_window import MainWindow

    event = ActionFeedbackEvent(
        id="feedback_1",
        timestamp_utc="2026-07-22T12:34:56Z",
        scope="settings",
        action_type="save",
        status="succeeded",
        summary="Saved settings for DX10.",
        radio_profile_id="7",
        target_label="DX10",
        detail="Saved from Settings.",
        source_surface="settings",
    )

    assert MainWindow._recent_action_line(event) == "SUCCEEDED | 12:34:56 | DX10: Saved settings for DX10."
