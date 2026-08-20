from pathlib import Path

from freqinout.core.js8_spotter_forms import parse_spotter_form_fields


ROOT = Path(__file__).resolve().parents[1]


def test_spotter_mcf_fields_parse_questions_and_options() -> None:
    fields = parse_spotter_form_fields(
        """
        Amateur Operator Information|F!100
        # Comment rows are ignored.

        ? GMT Offset:
        @1 -12:00
        @C +00:00

        ? Free text note:
        """
    )

    assert [field.label for field in fields] == ["GMT Offset", "Free text note"]
    assert fields[0].key == "GMT_OFFSET"
    assert fields[0].options == (("1", "-12:00"), ("C", "+00:00"))
    assert fields[1].options == ()


def test_message_compose_exposes_spotter_as_guarded_form_family() -> None:
    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert '"JS8Spotter Forms"' in source
    assert "compose_mode_tabs" in source
    assert '"FLMsg / FLAmp"' in source
    assert '"JS8Spotter"' in source
    assert "_on_compose_mode_tab_changed" in source
    assert 'self._compose_mode = "spotter"' in source
    assert "compose_nbems_dest_row_widget.setVisible(not spotter_mode)" in source
    assert "compose_send_js8_btn.setVisible(spotter_mode)" in source
    assert '"spotter_form"' in source
    assert "FIO sends only after JS8Call target-state preflight passes" in source
    assert "Send via JS8Call" in source
    assert "Save to Expect" in source
    assert "send_js8_message_guarded" in source
    assert "save_expect_entry" in source
    assert "Joseph D. Lyman, KF7MIX" not in source
    credits = (ROOT / "CREDITS.md").read_text(encoding="utf-8")
    assert "Joseph D. Lyman, KF7MIX" in credits
    assert "_spotter_msg_auth_state_map" in source
    assert "list_msg_auth_key_rows(enabled_only=True)" in source
    assert "MsgAuth: Valid" in source
    assert "Unsigned Spotter traffic stays visually quiet" in source
    assert 'return {"status": "valid", "detail": detail, "trusted": True}' in source
    assert 'if state == "unsigned":' in source
    assert 'return {}' in source
    assert "Sign MsgAuth" in source
    assert "sign_js8_text" in source
    assert "encode_short_datecode" in source
    assert "Select a matching MsgAuth key" in source
    assert "_compose_spotter_message_text(sign_for_target=False)" in source
    assert '"msg_auth_sign_enabled": self._compose_js8_msg_auth_selected()' in source
    assert '"msg_auth_sign_callsign": self._compose_operator_callsign()' in source
    assert '"msg_auth_include_datecode"' in source
    assert '"msg_auth_datecode": stored_datecode' in source
    assert "JS8ApiClientRegistry.get" in source
    assert "JS8ApiClient(" not in source


def test_js8_ncs_send_uses_shared_guarded_api_client() -> None:
    source = (ROOT / "freqinout/gui/js8call_net_control_tab.py").read_text(encoding="utf-8")

    assert "send_js8_message_guarded" in source
    assert "JS8ApiClientRegistry.get" in source
    assert "socket.create_connection" not in source
    assert "TX.SEND_MESSAGE" in source


def test_js8_workflow_modules_do_not_create_ad_hoc_api_clients_or_raw_send_sockets() -> None:
    workflow_paths = [
        ROOT / "freqinout/gui/message_viewer_tab.py",
        ROOT / "freqinout/gui/js8call_net_control_tab.py",
        ROOT / "freqinout/core/js8_expect_runtime.py",
    ]
    for path in workflow_paths:
        source = path.read_text(encoding="utf-8")
        assert "socket.create_connection" not in source, str(path)
        assert "JS8ApiClient(" not in source, str(path)

    expect_source = (ROOT / "freqinout/core/js8_expect_runtime.py").read_text(encoding="utf-8")
    assert "JS8ApiClientRegistry.get" in expect_source


def test_message_auth_settings_explain_msgauth_scope_and_credit() -> None:
    settings_source = (ROOT / "freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    help_source = (ROOT / "freqinout/gui/help_registry.py").read_text(encoding="utf-8")

    assert "JS8 Text Auth (MsgAuth)" in settings_source
    assert "Import Trusted Keys" in settings_source
    assert "Bulk Paste" in settings_source
    assert "Import Pasted Keys" in settings_source
    assert "Trusted Group Key (Any Sender)" in settings_source
    assert "Trusted Sender Key" in settings_source
    assert "My Signing Key" in settings_source
    assert "Any Sender in Group" in settings_source
    assert "Specific Sender Callsign" in settings_source
    assert "https://kf7mix.com/" not in settings_source
    credits = (ROOT / "CREDITS.md").read_text(encoding="utf-8")
    assert "https://kf7mix.com/" in credits
    assert "Save Key" in settings_source
    assert "Delete Selected" in settings_source
    assert "My Signing Keys" in settings_source
    assert "Generate My Signing Keys" in settings_source
    assert "Generate Keys" in settings_source
    assert "Show Keys" in settings_source
    assert "Enable Show Keys to reveal key values." in settings_source
    assert "setSectionResizeMode(5, QHeaderView.Stretch)" in settings_source
    assert "generate_msg_auth_secret_key" in settings_source
    assert "_generate_js8_msg_auth_keys" in settings_source
    assert "_next_msg_auth_generate_label" in settings_source
    assert "_existing_msg_auth_signing_labels" in settings_source
    assert "_import_js8_msg_auth_verification_key" in settings_source
    assert "_bulk_import_js8_msg_auth_verification_keys" in settings_source

    assert "_parse_js8_msg_auth_bulk_row" in settings_source
    assert "one active key plus optional rotation keys" in settings_source
    assert "js8_msg_auth_keys_table" in settings_source
    assert "Expect Allow Policies" in settings_source
    assert "Save Policy" in settings_source
    assert "js8_expect_policies_table" in settings_source
    assert "Expect Entries" in settings_source
    assert "Save Entry" in settings_source
    assert "js8_expect_entries_table" in settings_source
    assert "Expect Change History" in settings_source
    assert "Expect Request History" in settings_source
    assert "list_expect_management_audit" in settings_source
    assert "list_expect_runtime_audit" in settings_source
    assert "js8_expect_audit_table" in settings_source
    assert "js8_expect_requests_table" in settings_source
    assert "_expect_source_display" in settings_source
    assert "_expect_source_detail" in settings_source
    assert "update_expect_entry_controls" in settings_source
    assert "JS8Spotter DB" in settings_source
    assert "Import" in settings_source
    assert "import_js8spotter_database" in settings_source
    assert "js8spotter_import_db_path" in settings_source
    assert "Imported Spotter Watch Review" in settings_source
    assert "Imported Spotter Activity Review" in settings_source
    assert "load_js8spotter_archive_records" in settings_source
    assert "_refresh_js8spotter_watch_review" in settings_source
    assert "_refresh_js8spotter_activity_review" in settings_source
    assert 'table_names=("activity", "grid", "signal", "csstatrep")' in settings_source
    assert "JS8 MsgAuth keys scoped by group/callsign" in help_source


def test_message_inbox_uses_adaptive_spotter_sitrep_views() -> None:
    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    row_source = (ROOT / "freqinout/core/message_row_presentation.py").read_text(encoding="utf-8")

    assert "set_display_profile" in source
    assert '"field_report"' in source
    assert "_message_display_profile_for_type" in source
    assert 'text == "Spotter"' in row_source
    assert 'text == "SitRep" or text.startswith("SitRep/")' in row_source
    assert 're.match(r"^F![0-9]{3}[A-Z]?$", text)' in row_source
    assert '"MCF", "Status", "From", "To", "State / Grid", "Age"' in row_source
    assert "parse_spotter_bracket_fields" in row_source
    assert "_field_report_area" in source
    assert "_field_report_status" in source
    assert "_relative_age" in source


def test_message_inbox_uses_shared_message_intelligence_for_topics_and_summaries() -> None:
    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    intel_source = (ROOT / "freqinout/core/message_intelligence.py").read_text(encoding="utf-8")
    filter_source = (ROOT / "freqinout/core/message_inbox_filters.py").read_text(encoding="utf-8")
    row_source = (ROOT / "freqinout/core/message_row_presentation.py").read_text(encoding="utf-8")

    assert "from freqinout.core.message_intelligence import (" in source
    assert "MessageIntelligence" in source
    assert "analyze_commstat_fields" in source
    assert "analyze_spotter_text(" in source
    assert "analyze_form_text(" in source
    assert "analyze_commstat_fields(" in source
    assert "from freqinout.core.message_row_presentation import (" in source
    assert "spotter_message_row_presentation" in source
    assert "commstat_message_row_presentation" in source
    assert "topics=tuple(intelligence.topics)" in row_source
    assert "actionable=bool(intelligence.actionable)" in row_source
    assert 'preferred_order = ["FLMSG/FLAMP", "Spotter", "CommStat", "JS8Call", "VarAC", "SitRep"]' in source
    assert "InboxFilterCriteria" in source
    assert "row_matches_inbox_criteria as _core_row_matches_inbox_criteria" in source
    assert "class InboxFilterCriteria" in filter_source
    assert "def row_matches_inbox_criteria" in filter_source
    assert 'status_sel == "Action Needed"' in filter_source
    assert 'getattr(row, "actionable", False)' in filter_source
    assert "TOPIC_TAXONOMY" in intel_source
    assert '"General Intel"' in intel_source
    assert '"Infrastructure"' in intel_source


def test_message_inbox_search_sits_above_messages_table_with_useful_hints() -> None:
    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    search_idx = source.index('self.rcv_search.setPlaceholderText("Search messages:')
    table_idx = source.index("messages_layout.addWidget(self.messages_table)")
    left_grid = source.split("placements = [", 1)[1].split("]", 1)[0]

    assert search_idx > table_idx
    assert "messages_layout.insertLayout(1, search_row)" in source
    assert "callsign, group, MCF/F! code, topic, state/grid, keyword" in source
    assert "self.rcv_search" not in left_grid


def test_message_inbox_delegates_visible_ingest_to_shared_ingestor() -> None:
    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    js8_body = source.split("def _ingest_js8_messages", 1)[1].split("def _spotter_offset_key", 1)[0]
    spotter_body = source.split("def _ingest_spotter_from_directed", 1)[1].split("def _enqueue_next_msg_id", 1)[0]

    assert "from freqinout.core.message_ingest import MessageIngestor" in source
    assert "MessageIngestor(self.settings).ingest_js8_messages()" in js8_body
    assert "MessageIngestor(self.settings).ingest_spotter_from_directed()" in spotter_body
    assert "sqlite3.connect(inbox_path)" not in js8_body
    assert "directed_path.open" not in spotter_body


def test_background_spotter_ingest_preserves_source_identity() -> None:
    source = (ROOT / "freqinout/core/background_ingest.py").read_text(encoding="utf-8")
    body = source.split("def _run_multi_radio_spotter_ingest", 1)[1].split("def _run_js8_link_indexer", 1)[0]

    assert "directed_source_id" in body
    assert "source_key=directed_source_id" in body
    assert 'f"spotter_directed_offset_{directed_source_id}"' in body
    assert 'f"spotter_directed_offset_radio_{radio_id}"' in body


def test_global_search_indexes_imported_js8spotter_history() -> None:
    main_source = (ROOT / "freqinout/gui/main_window.py").read_text(encoding="utf-8")
    archive_source = (ROOT / "freqinout/core/js8spotter_archive.py").read_text(encoding="utf-8")

    assert "load_js8spotter_archive_records" in main_source
    assert "Spotter History" in main_source
    assert "spotter_archive_detail" in main_source
    assert "Archived Payload" in main_source
    assert "js8spotter spotter history traffic map operator callsign grid" in main_source
    assert "js8spotter spotter history messages expect alert profile search" in main_source
    assert "DEFAULT_SEARCH_ARCHIVE_TABLES" in archive_source
    assert '"setting"' not in archive_source.split("DEFAULT_SEARCH_ARCHIVE_TABLES", 1)[1].split(")", 1)[0]


def test_spotter_inbox_source_detail_is_structured() -> None:
    source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert "_spotter_source_lines" in source
    assert "SOURCE:" in source
    assert "Radio:" in source
    assert "JS8Call:" in source
    assert "list_expect_runtime_audit" in source
    assert "_spotter_expect_state_for_message" in source
    assert "Expect:" in source
    assert "expect_decision" in source
    assert "expect_detail" in source


def test_live_js8_receive_surfaces_feed_spotter_ingest() -> None:
    control_source = (ROOT / "freqinout/gui/js8call_net_control_tab.py").read_text(encoding="utf-8")
    map_source = (ROOT / "freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "ingest_spotter_from_js8_events" in control_source
    assert "JS8CallNetControl: Spotter live ingest failed" in control_source
    assert "ingest_spotter_from_js8_events" in map_source
    assert "StationsMap: Spotter live ingest failed" in map_source


def test_background_js8_link_ingest_uses_core_indexer_not_map_tab() -> None:
    background_source = (ROOT / "freqinout/core/background_ingest.py").read_text(encoding="utf-8")
    map_source = (ROOT / "freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer" in background_source
    assert "from freqinout.gui.stations_map_tab import JS8LogLinkIndexer" not in background_source
    assert "from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer" in map_source


def test_background_messages_job_runs_bounded_observation_backfill() -> None:
    background_source = (ROOT / "freqinout/core/background_ingest.py").read_text(encoding="utf-8")

    assert "from freqinout.core.observation_backfill import backfill_observations" in background_source
    assert "self._run_observation_backfill(worker_settings)" in background_source
    assert "observation_backfill_batch_limit" in background_source
    assert "min(500, limit)" in background_source


def test_message_file_scan_logic_lives_in_core_not_message_tab() -> None:
    message_source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    scanner_source = (ROOT / "freqinout/core/message_file_scanner.py").read_text(encoding="utf-8")
    worker_body = message_source.split("class _FileScanWorker", 1)[1].split("class _BbsAutoArchiveWorker", 1)[0]

    assert "from freqinout.core.message_file_scanner import" in message_source
    assert "MessageFileScanner(" in worker_body
    assert "def _full_scan_recursive" not in message_source
    assert "def _scan_changed_recursive" not in message_source
    assert "os.scandir(" not in worker_body
    assert "class MessageFileScanner" in scanner_source
    assert "def _full_scan_recursive" in scanner_source
    assert "os.scandir(" in scanner_source


def test_message_file_scan_projects_flmsg_flamp_observations_with_bounded_helper() -> None:
    message_source = (ROOT / "freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.core.observation_backfill import project_message_file_observations" in message_source
    assert "self._project_message_files_to_observations(records)" in message_source
    assert "observation_file_projection_batch_limit" in message_source
    assert "min(250, limit)" in message_source
