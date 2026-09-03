import datetime
import json
import os
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMessageBox

from freqinout.core.message_intelligence import (
    MessageIntelligence,
    analyze_commstat_fields,
    analyze_form_text,
    analyze_spotter_text,
    collect_topic_evidence,
    normalize_topic_terms,
)
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_file_metadata import (
    FILE_METADATA_PARSER_VERSION,
    cached_message_file_row_summary,
    delete_file_cache_entries,
    ensure_message_file_metadata_table,
    file_metadata_key,
    form_report_timestamp_from_summary,
    has_stale_message_file_metadata,
    load_existing_message_file_metadata_records,
    normalize_cached_message_file_metadata,
    remove_file_record_from_groups,
)
from freqinout.core.message_file_presentation import (
    cached_file_message_row_candidate,
    file_message_row_candidate,
    file_message_row_presentation,
    file_message_search_text,
    form_file_table_title,
    form_message_type_label,
    parsed_file_message_row_candidate,
)
from freqinout.core.message_row_presentation import (
    commstat_message_row_presentation,
    field_report_area_label,
    field_report_form_label,
    field_report_status_label,
    js8_message_row_presentation,
    message_display_profile_for_focus_type,
    message_display_profile_headers,
    message_row_search_text,
    observation_message_row_presentation,
    relative_age_label,
    spotter_mcf_display_label,
    spotter_message_row_presentation,
    varac_message_row_presentation,
)
from freqinout.core.observation_projection import Observation
from freqinout.core.message_inbox_filters import (
    InboxFilterCriteria,
    active_inbox_scope_summary,
    is_message_group_candidate,
    message_group_candidate_set,
    message_group_option_sections,
    message_group_rebuild_selection,
    message_group_source_map,
    message_group_value,
    message_source_aliases,
    message_source_options,
    message_source_value,
    normalize_message_group_filter_value,
    primary_message_group_values,
    row_matches_age_filter,
    row_matches_inbox_criteria,
    row_matches_inbox_focus,
    row_matches_search_query,
    row_matches_source_filter,
    row_matches_status_filter,
    row_matches_type_filter,
    row_matches_workspace_scope,
    row_search_text,
)
from freqinout.core.message_form_metadata import (
    extract_form_metadata_from_text,
    parse_custom_form_fields_text,
)
from freqinout.core.message_delete_audit import (
    ensure_message_delete_audit_table,
    load_message_delete_audit_rows,
    record_message_delete_audit,
    safe_audit_text,
)
from freqinout.core.message_delete_policy import (
    bulk_delete_completion_text,
    bulk_delete_confirmation_text,
    collect_deletable_message_rows,
    commstat_delete_execution_result,
    delete_audit_action_for_row,
    delete_effect_tooltip,
    MessageDeleteExecutionResult,
    delete_success_result,
    failed_source_delete_result,
    message_delete_capability,
    message_delete_result_detail,
    message_row_summary_line as core_message_row_summary_line,
    missing_identity_delete_result,
    single_delete_confirmation_text,
    single_delete_failure_warning,
    summarize_delete_effects,
    summarize_delete_sources,
)
from freqinout.core.message_row_identity import (
    filter_rows_excluding_identities,
    message_payload_identity,
    message_row_identity,
    message_row_identity_set,
)
from freqinout.core.message_source_delete import (
    delete_js8_inbox_row,
    delete_js8_local_rows,
    delete_sitrep_store_row,
    delete_spotter_store_row,
    delete_varac_local_projection,
    sitrep_message_key,
    soft_delete_varac_source_row,
)
from freqinout.core.commstat_artifacts import (
    ensure_commstat_artifact_deletion_tables,
    ensure_commstat_artifact_tables,
    tombstone_commstat_artifact,
)
from freqinout.core.commstat_config import CommStatGroupState
from freqinout.gui.message_viewer_tab import (
    CommStatArtifact,
    ComposeRadioTarget,
    JS8Message,
    MessageTableModel,
    MessageViewerTab,
    SitrepMessage,
    SpotterMessage,
    UnifiedMessage,
    VarACMessage,
    _RowsBuildWorker,
    _js8_relay_route_display,
    _spotter_mcf_display_label,
)


def _commstat_artifact(**overrides) -> CommStatArtifact:
    base = {
        "artifact_id": 1,
        "artifact_key": "commstat:test",
        "artifact_kind": "MESSAGE",
        "subtype": "COMMSTAT_FWD",
        "event_ts": 10.0,
        "event_ts_utc": "2026-08-11T12:00:00Z",
        "from_call": "K7ETC",
        "target": "@MR08",
        "report_group": "MR08",
        "grid": "",
        "state_code": "",
        "scope": "",
        "transport_mode": "js8",
        "transport_label": "JS8",
        "reach_mode": "rf_observed",
        "reach_label": "Limited Reach (RF only)",
        "status_label": "INFO",
        "alert_color": "",
        "title": "CommStat | Water update",
        "body_text": "Water update",
        "remarks_text": "",
        "source_family_label": "CommStat",
        "source_first": "COMMSTAT3",
        "source_last": "COMMSTAT3",
        "source_count": 1,
        "sources_json": '["COMMSTAT3"]',
        "source_refs_json": '["messages:42"]',
        "external_ids_json": "[]",
        "payload_json": "{}",
        "updated_ts": 10.0,
    }
    base.update(overrides)
    return CommStatArtifact(**base)


def test_spotter_mcf_extracts_operator_routing_area_topics_and_actionable_summary() -> None:
    info = analyze_spotter_text(
        "F!701 RUEHN7R TO[@MAGNET] FR[W0IFM] ST[MO] CC[USA] GR[EM48EQ] "
        "NA[FORM POSTED FOR WILDFIRE EVACUATION UPDATE] DA[260429-1839Z] #D2NT",
        form_name="MCF701 Field Report",
    )

    assert info.form_name == "MCF701 Field Report"
    assert info.from_call == "W0IFM"
    assert info.to_call == "@MAGNET"
    assert info.state == "MO"
    assert info.grid == "EM48EQ"
    assert info.groups == ("@MAGNET",)
    assert "Fire" in info.topics
    assert any(item.startswith("body:wildfire") for item in info.topic_evidence["Fire"])
    assert "General Intel" not in info.topics
    assert info.actionable is True
    assert info.operator_attention is True
    assert info.routing_candidate is True
    assert "location:grid" in info.routing_reasons
    assert info.summary == "MCF701 Field Report | W0IFM -> @MAGNET | FORM POSTED FOR WILDFIRE EVACUATION UPDATE | 260429-1839z"


def test_flmsg_form_extracts_common_fields_and_human_first_summary() -> None:
    text = """
MAGNET General Use Form - v1.1.1
Date/Time/Msg ID
260729-0354z
To
MR08
From
K7ETC
Subject
Widemouth 2 Fire
Message
UT - Widemouth 2 Fire - DM38ST - evacuation posture updated due to wildfire.
"""

    info = analyze_form_text(
        text,
        form_name="MAGNET General Use Form",
        source_type="flmsg",
        fields={
            "from": "K7ETC",
            "to": "MR08",
            "subject": "Widemouth 2 Fire",
            "date_summary": "260729-0354z",
            "grid": "DM38ST",
        },
    )

    assert info.from_call == "K7ETC"
    assert info.to_call == "MR08"
    assert info.subject == "Widemouth 2 Fire"
    assert info.date_summary == "260729-0354z"
    assert info.grid == "DM38ST"
    assert "Fire" in info.topics
    assert any(item.startswith("subject:fire") for item in info.topic_evidence["Fire"])
    assert info.actionable is True
    assert info.operator_attention is True
    assert info.routing_candidate is True
    assert info.summary == "MAGNET General Use Form | K7ETC -> MR08 | Widemouth 2 Fire | 260729-0354z"


def test_rendered_form_labels_extract_route_subject_and_multiline_body() -> None:
    text = """
MAGNET General Use Form - v1.1.1
Date/Time/Msg ID
260803-0402z
To
MR08
From
K7ETC
Msg Precedence
Routine
Region
MR08
Subject
Widemouth 2 Fire
Message
UT - Widemouth 2 Fire - DM38ST - evacuation posture updated.

Residents south of 300 South should monitor alerts and prepare to leave.
"""

    info = analyze_form_text(text, source_type="flmsg", path="K7ETC-20260803-040212Z-57.k2s")

    assert info.from_call == "K7ETC"
    assert info.to_call == "MR08"
    assert info.subject == "Widemouth 2 Fire"
    assert info.date_summary == "260803-0402z"
    assert info.state == "UT"
    assert info.grid == "DM38ST"
    assert info.metadata["precedence"] == "Routine"
    assert info.metadata["region"] == "MR08"
    assert "Residents south of 300 South" in info.body
    assert "Fire" in info.topics
    assert info.summary.startswith("MAGNET General Use Form - v1.1.1 | K7ETC -> MR08 | Widemouth 2 Fire")


def test_flmsg_report_date_extracts_compact_date_variants_for_age() -> None:
    long_date = analyze_form_text(
        "\n".join(
            [
                "MAGNET General Use Form",
                "Date/Msg ID",
                "20260729035455Z",
                "To",
                "MR08",
                "From",
                "K7ETC",
                "Subject",
                "Widemouth 2 Fire",
                "Message",
                "Wildfire update.",
            ]
        ),
        source_type="flmsg",
    )
    short_date = analyze_form_text(
        "\n".join(
            [
                "MAGNET General Use Form",
                "DTG",
                "2607290354",
                "To",
                "MR08",
                "From",
                "K7ETC",
                "Subject",
                "Widemouth 2 Fire",
                "Message",
                "Wildfire update.",
            ]
        ),
        source_type="flmsg",
    )

    assert long_date.date_summary == "20260729-0354z"
    assert short_date.date_summary == "260729-0354z"
    assert form_report_timestamp_from_summary(long_date.date_summary) == datetime.datetime(
        2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc
    ).timestamp()
    assert form_report_timestamp_from_summary(short_date.date_summary) == datetime.datetime(
        2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc
    ).timestamp()


def test_flmsg_unlabeled_magnet_general_fallback_keeps_date_out_of_to_field(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260731-000127Z-54.k2s"
    text = "\n".join(
        [
            "<customform>",
            "CUSTOM_FORM,magnet_general_V1.1.1.html",
            "L01,260730-2351z",
            "L02,MR08",
            "L03,K7ETC",
            "L04,Routine",
            "L05,MR08",
            "L06,Widemouth 2 Fire",
            "L07,UT - Widemouth 2 Fire - DM38ST - UPDATE 30JUL2026 2350Z.",
        ]
    )

    meta = extract_form_metadata_from_text(
        text,
        path,
        template_title_for_form=lambda _form: "",
        template_labels_for_form=lambda _form: [],
    )
    info = analyze_form_text(text, source_type="flmsg", path=path, fields=meta)

    assert meta["date_summary"] == "260730-2351z"
    assert meta["to"] == "MR08"
    assert meta["from"] == "K7ETC"
    assert meta["subject"] == "Widemouth 2 Fire"
    assert info.to_call == "MR08"
    assert info.from_call == "K7ETC"
    assert info.subject == "Widemouth 2 Fire"
    assert "DM38ST" in info.body
    assert form_report_timestamp_from_summary(info.date_summary) == datetime.datetime(
        2026, 7, 30, 23, 51, tzinfo=datetime.timezone.utc
    ).timestamp()


def test_message_viewer_uses_radio_scoped_nbems_paths_for_flmsg_forms(tmp_path) -> None:
    nbems_root = tmp_path / "NBEMS.files"
    flmsg_dir = nbems_root / "ICS" / "messages"
    custom_dir = nbems_root / "CUSTOM"
    flmsg_dir.mkdir(parents=True)
    custom_dir.mkdir(parents=True)
    template = custom_dir / "magnet_general.html"
    template.write_text(
        "<html><body><h1>MAGNET General Use Form</h1>"
        "<input name=\"L01\" title=\"To\"><input name=\"L02\" title=\"From\">"
        "<textarea name=\"L03\" title=\"Message\"></textarea></body></html>",
        encoding="utf-8",
    )

    tab = MessageViewerTab.__new__(MessageViewerTab)

    class Settings:
        def get(self, key, default=None):
            values = {"message_paths": {}, "nbems_custom_forms_path": ""}
            return values.get(key, default)

    class Store:
        def list_device_profiles(self):
            return [
                {
                    "enabled": True,
                    "flmsg_message_path": str(flmsg_dir),
                    "flamp_message_path": "",
                    "varac_incoming_path": "",
                }
            ]

    tab.settings = Settings()
    tab._multi_radio_store = Store()
    tab.watch_dirs = []

    effective = MessageViewerTab._effective_watch_dirs(tab)
    assert {"origin": "flmsg", "path": str(flmsg_dir)} in effective
    assert MessageViewerTab._resolve_custom_form_template_path(tab, "magnet_general.html") == template

    fallback_html = MessageViewerTab._render_custom_form_fields(
        MessageViewerTab._parse_custom_form_fields(
            "CUSTOM_FORM,missing.html\nL01,MR08\nL02,K7ETC\nL03,Wildfire update"
        ),
        [],
        "missing.html",
    )
    assert "MR08" in fallback_html
    assert "K7ETC" in fallback_html
    assert "Wildfire update" in fallback_html


def test_message_table_activation_opens_flmsg_file_row(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("CUSTOM_FORM,missing.html\nL01,MR08\nL02,K7ETC\nL03,Wildfire update", encoding="utf-8")
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)
    row = UnifiedMessage(
        msg_type="FLMSG",
        status="NEW",
        from_call="K7ETC",
        to_call="MR08",
        rcv_ts=rec.mtime,
        rcv_display="now",
        title="Wildfire update",
        origin="flmsg",
        payload=rec,
    )
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._messages_model = MessageTableModel([row])
    opened: list[UnifiedMessage] = []
    tab._on_view_message = lambda selected: opened.append(selected)

    MessageViewerTab._on_message_table_activated(tab, tab._messages_model.index(0, 6))

    assert opened == [row]


def test_file_record_view_failure_shows_operator_error_not_unselected(tmp_path) -> None:
    msg_path = tmp_path / "broken.k2s"
    msg_path.write_text("CUSTOM_FORM,missing.html\nL01,MR08", encoding="utf-8")
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)
    row = UnifiedMessage(
        msg_type="FLMSG",
        status="NEW",
        from_call="",
        to_call="",
        rcv_ts=rec.mtime,
        rcv_display="now",
        title="broken",
        origin="flmsg",
        payload=rec,
    )

    class Settings:
        def get(self, _key, default=None):
            return default

    class Label:
        text = ""

        def setText(self, value):
            self.text = value

    class Viewer:
        text = ""
        rich = False

        def setAcceptRichText(self, value):
            self.rich = bool(value)

        def setPlainText(self, value):
            self.text = value

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = Settings()
    tab.info_label = Label()
    tab.viewer = Viewer()
    tab._set_open_external_path = lambda *_args, **_kwargs: None
    tab._load_content = lambda _rec: (_ for _ in ()).throw(RuntimeError("render failed"))
    tab._set_read_state = lambda *_args, **_kwargs: None

    MessageViewerTab._on_view_message(tab, row)

    assert "could not load" in tab.info_label.text
    assert "render failed" in tab.viewer.text
    assert "No message selected" not in tab.info_label.text


def test_message_viewer_file_record_intelligence_uses_tab_metadata_helper(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260729-141819Z-50.k2s"
    msg_path.write_text(
        "CUSTOM_FORM,missing.html\n"
        "L01,MR08\n"
        "L02,K7ETC\n"
        "L03,Widemouth 2 Fire\n"
        "L04,UT - DM38ST - wildfire update",
        encoding="utf-8",
    )
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)

    class Settings:
        def get(self, key, default=None):
            values = {"message_paths": {}, "nbems_custom_forms_path": ""}
            return values.get(key, default)

    class Store:
        def list_device_profiles(self):
            return []

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = Settings()
    tab._multi_radio_store = Store()

    info = MessageViewerTab._file_record_intelligence(tab, rec, msg_path.read_text(encoding="utf-8"))

    assert info.from_call == "K7ETC"
    assert info.to_call == "MR08"
    assert info.subject == "Widemouth 2 Fire"
    assert info.body == "UT - DM38ST - wildfire update"
    assert info.grid == "DM38ST"
    assert "Fire" in info.topics
    assert info.operator_attention is True


def test_message_viewer_load_content_displays_flmsg_transport_form(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260731-000127Z-54.k2s"
    msg_path.write_text(
        "CUSTOM_FORM,missing.html\n"
        "L01,MR08\n"
        "L02,K7ETC\n"
        "L03,Widemouth 2 Fire\n"
        "L04,UT - DM38ST - wildfire update",
        encoding="utf-8",
    )
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)

    class Settings:
        def get(self, key, default=None):
            values = {"message_paths": {}, "nbems_custom_forms_path": ""}
            return values.get(key, default)

    class Store:
        def list_device_profiles(self):
            return []

    class Label:
        text = ""

        def setText(self, value):
            self.text = value

    class Viewer:
        text = ""
        html = ""
        rich = False

        def setAcceptRichText(self, value):
            self.rich = bool(value)

        def setHtml(self, value):
            self.html = value

        def setPlainText(self, value):
            self.text = value

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = Settings()
    tab._multi_radio_store = Store()
    tab._file_view_cache = {}
    tab._cache_max_view_entries = 20
    tab._read_state_map = {}
    tab._sender_cache = {}
    tab._open_external_path = None
    tab.info_label = Label()
    tab.viewer = Viewer()

    MessageViewerTab._load_content(tab, rec)

    rendered = tab.viewer.html or tab.viewer.text
    assert "Widemouth 2 Fire" in rendered
    assert "Widemouth 2 Fire" in tab.info_label.text
    assert "Key Fields" in rendered
    assert ">To<" in rendered
    assert ">From<" in rendered
    assert ">Subject<" in rendered
    assert ">Message<" in rendered
    assert "fio-message-fields" in rendered
    assert "<td class='fio-message-key'>From:</td><td class='fio-message-value'>K7ETC</td>" in rendered
    assert "FromK7ETC" not in rendered
    assert ">L01<" not in rendered
    assert "fio-message-summary" in rendered
    assert "could not load" not in tab.info_label.text


def test_message_viewer_transport_form_keeps_signature_out_of_title_label(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260731-000127Z-54.k2s"
    msg_path.write_text(
        "CUSTOM_FORM,missing.html\n"
        "L01,MR08\n"
        "L02,K7ETC\n"
        "L03,Widemouth 2 Fire\n"
        "L04,UT - DM38ST - wildfire update",
        encoding="utf-8",
    )
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)

    class Settings:
        def get(self, key, default=None):
            values = {"message_paths": {}, "nbems_custom_forms_path": ""}
            return values.get(key, default)

    class Store:
        def list_device_profiles(self):
            return []

    class Label:
        text = ""

        def setText(self, value):
            self.text = value

    class Viewer:
        text = ""
        html = ""

        def setAcceptRichText(self, _value):
            pass

        def setHtml(self, value):
            self.html = value

        def setPlainText(self, value):
            self.text = value

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = Settings()
    tab._multi_radio_store = Store()
    tab._file_view_cache = {}
    tab._cache_max_view_entries = 20
    tab._read_state_map = {}
    tab._sender_cache = {}
    tab._open_external_path = None
    tab.info_label = Label()
    tab.viewer = Viewer()
    tab._signature_detail_for_record = lambda _rec: "Signature: Invalid"

    MessageViewerTab._load_content(tab, rec)

    rendered = tab.viewer.html or tab.viewer.text
    assert tab.info_label.text
    assert "Signature:" not in tab.info_label.text
    assert "Signature / Hash" in rendered
    assert "Signature: Invalid" in rendered

    MessageViewerTab._load_content(tab, rec)

    assert "Signature:" not in tab.info_label.text


def test_rendered_form_inline_labels_and_body_location_feed_intelligence() -> None:
    text = """
MAGNET General Use Form - v1.1.1
Date/Time/Msg ID: 260810-1405z
To: MAGNET
From: K7ETC
Msg Precedence: Priority
Region: MR08
Subject: Infrastructure update
Message: UT - DM38ST - water plant generator is offline and fuel delivery is delayed.
"""

    info = analyze_form_text(text, source_type="flamp", path="K7ETC-20260810-140500Z-57.k2s")

    assert info.from_call == "K7ETC"
    assert info.to_call == "MAGNET"
    assert info.date_summary == "260810-1405z"
    assert info.state == "UT"
    assert info.grid == "DM38ST"
    assert info.metadata["precedence"] == "Priority"
    assert info.metadata["region"] == "MR08"
    assert {"Infrastructure", "Water", "Power", "Fuel"}.issubset(set(info.topics))
    assert info.routing_candidate is True
    assert info.summary == "MAGNET General Use Form - v1.1.1 | K7ETC -> MAGNET | Infrastructure update | 260810-1405z"


def test_filename_and_body_terms_support_bbs_rule_routing_concepts() -> None:
    info = analyze_form_text(
        "Infrastructure report: water plant generator power outage.",
        path="MAGNET-S2-RR-260502-_U.S_Iran_Ceasefire_Stability_Degrading.sig.b2s",
    )

    assert {"General Intel", "Infrastructure", "Power", "Water"}.issubset(set(info.topics))
    assert "@MAGNET" in info.groups
    assert "@S2" in info.groups
    assert info.actionable is True


def test_topic_terms_are_canonical_and_do_not_overmatch_short_codes() -> None:
    assert "General Intel" in normalize_topic_terms("S2 regional intelligence snapshot")
    assert "General Intel" not in normalize_topic_terms("pass2 checksum only")


def test_topic_evidence_ignores_no_report_structured_fields() -> None:
    evidence = collect_topic_evidence({"status": {"food": "not_reported", "power": "green"}})

    assert "Food" not in evidence
    assert "Power" in evidence


def test_topic_evidence_preserves_real_power_status_values() -> None:
    evidence = collect_topic_evidence({"status": {"power": "Grid down"}})

    assert "Power" in evidence


def test_topic_evidence_ignores_no_report_status_lines() -> None:
    evidence = collect_topic_evidence(
        {
            "commstat": "\n".join(
                [
                    "Status Fields",
                    "Overall: Not Reported",
                    "Power: Not Reported",
                    "Food: Not Reported",
                    "Water: Not Reported",
                ]
            )
        }
    )

    assert "Food" not in evidence
    assert "Power" not in evidence
    assert "Water" not in evidence

    evidence = collect_topic_evidence({"commstat": "Food: Limited pantry supply\nPower: Not Reported"})
    assert "Food" in evidence
    assert "Power" not in evidence


def test_commstat_alert_flattens_to_actionable_operator_summary() -> None:
    info = analyze_commstat_fields(
        artifact_kind="ALERT",
        title="Storm Surge Warning",
        body="Move to elevated shelter locations immediately.",
        from_call="W8UFO",
        target="@MAGNET",
        state="FL",
        grid="EL98",
        status="RED",
        alert_color="RED",
        source_family="CommStat",
        event_utc="2026-08-10 12:34:56",
    )

    assert info.form_name == "CommStat Alert"
    assert info.from_call == "W8UFO"
    assert info.to_call == "MAGNET"
    assert info.state == "FL"
    assert info.grid == "EL98"
    assert {"Weather", "Shelter", "General Intel"}.issubset(set(info.topics))
    assert info.actionable is True
    assert info.summary == "CommStat Alert | W8UFO -> MAGNET | Storm Surge Warning | 2026-08-10 12:34:56"


def test_commstat_statrep_terms_enrich_future_routing_topics() -> None:
    info = analyze_commstat_fields(
        artifact_kind="STATREP",
        body="County power outage, water plant generator offline, radio repeater degraded.",
        from_call="K7ETC",
        report_group="MR08",
        status="YELLOW",
    )

    assert info.form_name == "CommStat StatRep"
    assert info.to_call == "MR08"
    assert "MR08" in info.groups
    assert {"Power", "Water", "Comms", "Infrastructure", "General Intel"}.issubset(set(info.topics))
    assert "body:power" in info.topic_evidence["Power"]
    assert info.actionable is True
    assert info.operator_attention is True
    assert info.routing_candidate is True


def test_commstat_general_message_is_flat_not_nested() -> None:
    info = analyze_commstat_fields(
        artifact_kind="MESSAGE",
        title="Regional advisory",
        body="Station staffing update for internet relay coverage.",
        from_call="N1MAG",
        target="@MAGNET",
    )

    assert info.form_name == "CommStat Message"
    assert info.to_call == "MAGNET"
    assert info.summary.startswith("CommStat Message | N1MAG -> MAGNET | Regional advisory")
    assert "Comms" in info.topics
    assert info.operator_attention is True
    assert info.routing_candidate is True
    assert info.metadata["kind"] == "CommStat Message"


def test_inbox_attention_is_separate_from_routeable_scope() -> None:
    info = analyze_form_text("Weather briefing only. No location or group included.", source_type="flmsg")

    assert "Weather" in info.topics
    assert info.operator_attention is True
    assert info.actionable is True
    assert info.routing_candidate is False
    assert "topics:Weather" in info.routing_reasons


def test_message_row_builder_uses_shared_intelligence_for_flmsg_scan_and_search(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260803-040212Z-57.k2s"
    path.write_text(
        """
MAGNET General Use Form - v1.1.1
Date/Time/Msg ID
260803-0402z
To
MR08
From
K7ETC
Subject
Widemouth 2 Fire
Message
UT - Widemouth 2 Fire - DM38ST - evacuation posture updated.
""",
        encoding="utf-8",
    )
    stat = path.stat()
    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={"flmsg": [FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)]},
        file_metadata_map={},
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=1,
    )
    emitted = []
    worker.finished.connect(lambda payload: emitted.append(payload))

    worker.run()

    row = emitted[0]["rows"][0]
    assert row.msg_type == "FLMSG"
    assert row.from_call == "K7ETC"
    assert row.to_call == "MR08"
    assert row.title == "Widemouth 2 Fire"
    assert "Fire" in row.topics
    assert row.actionable is True
    assert "magnet general use form" in row.search_text
    assert "dm38st" in row.search_text
    assert "ut" in row.search_text
    assert "fire" in row.search_text


def test_form_file_table_title_prefers_blankform_subject() -> None:
    info = analyze_form_text(
        "From\nKJ4RMO\nSubject\nOp net2 81\nMessage\nGenerator fuel and comms status.",
        source_type="flmsg",
        fields={"form_title": "<blankform>", "from": "KJ4RMO", "subject": "Op net2 81"},
    )

    assert form_file_table_title(info, "KJ4RMO-20260730-151758Z-48.k2s") == "OpNet2 81"


def test_form_report_timestamp_decodes_date_msg_id_as_utc() -> None:
    ts = form_report_timestamp_from_summary("260729-0354z")
    assert ts == datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp()
    ts_long = form_report_timestamp_from_summary("20260729-035455Z")
    assert ts_long == datetime.datetime(2026, 7, 29, 3, 54, 55, tzinfo=datetime.timezone.utc).timestamp()
    ts_spaced = form_report_timestamp_from_summary("20260729 0354")
    assert ts_spaced == datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp()
    ts_compact = form_report_timestamp_from_summary("260729035455")
    assert ts_compact == datetime.datetime(2026, 7, 29, 3, 54, 55, tzinfo=datetime.timezone.utc).timestamp()
    assert form_report_timestamp_from_summary("260729-0354z") == ts
    assert form_report_timestamp_from_summary("not a date") == 0.0


def test_file_message_row_presentation_is_shared_for_parsed_file_rows(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260729-141819Z-50.k2s"
    path.write_text("body", encoding="utf-8")
    stat = path.stat()
    rec = FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    intelligence = analyze_form_text(
        "\n".join(
            [
                "MAGNET General Use Form - v1.1.1",
                "From",
                "K7ETC",
                "To",
                "MR08",
                "Subject",
                "Widemouth 2 Fire",
                "Date/Msg ID",
                "260729-0354z",
                "State",
                "UT",
                "Grid",
                "DM38ST",
                "Message",
                "Wildfire update affecting water, food, comms, and roads.",
            ]
        ),
        form_name="MAGNET General Use Form - v1.1.1",
        source_type="flmsg",
        path=path,
        fields={
            "form_title": "MAGNET General Use Form - v1.1.1",
            "from": "K7ETC",
            "to": "MR08",
            "subject": "Widemouth 2 Fire",
            "date_summary": "260729-0354z",
        },
    )

    row = file_message_row_presentation(
        rec,
        "flmsg",
        is_image=False,
        intelligence=intelligence,
        form_meta={"form_title": "MAGNET General Use Form - v1.1.1", "date_summary": "260729-0354z"},
        fallback_from="N0CALL",
    )

    assert row.msg_type == "FLMSG"
    assert row.display_type == "General"
    assert row.from_call == "K7ETC"
    assert row.to_call == "MR08"
    assert row.title == "Widemouth 2 Fire"
    assert row.report_ts == datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp()
    assert row.rcv_ts == row.report_ts
    assert row.age_ts_source == "report"
    assert "Fire" in row.topics
    assert row.actionable is True
    assert "Widemouth 2 Fire" in row.search_detail
    assert "DM38ST" in row.search_detail


def test_file_message_row_candidate_builders_share_cached_and_parsed_semantics(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260729-141819Z-50.k2s"
    path.write_text("body", encoding="utf-8")
    stat = path.stat()
    rec = FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    report_ts = datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp()

    cached = cached_file_message_row_candidate(
        rec,
        {
            "msg_type": "FLMSG",
            "display_type": "General",
            "status": "NEW",
            "from_call": "K7ETC",
            "to_call": "MR08",
            "title": "Widemouth 2 Fire",
            "report_ts": report_ts,
            "age_ts_source": "report",
            "topics_json": '["Fire"]',
            "actionable": 1,
            "search_text": "widemouth wildfire",
        },
        origin="flmsg",
        status="READ",
    )

    assert cached is not None
    assert cached.used_cache is True
    assert cached.status == "READ"
    assert cached.rcv_ts == report_ts
    assert cached.report_ts == report_ts
    assert cached.age_ts_source == "report"
    assert cached.display_type == "General"
    assert cached.topics == ("Fire",)
    assert cached.actionable is True
    assert "wildfire" in file_message_search_text(
        cached.msg_type,
        cached.status,
        cached.from_call,
        cached.to_call,
        "7d",
        cached.search_detail,
    )

    intelligence = analyze_form_text(
        "\n".join(
            [
                "MAGNET General Use Form",
                "Date/Msg ID",
                "260729-0354z",
                "To",
                "MR08",
                "From",
                "K7ETC",
                "Subject",
                "Widemouth 2 Fire",
                "Message",
                "Wildfire update.",
            ]
        ),
        form_name="MAGNET General Use Form",
        source_type="flmsg",
    )
    parsed = parsed_file_message_row_candidate(
        rec,
        "flmsg",
        status="NEW",
        is_image=False,
        intelligence=intelligence,
        form_meta={},
        fallback_from="N0CALL",
    )

    assert parsed.used_cache is False
    assert parsed.status == "NEW"
    assert parsed.from_call == "K7ETC"
    assert parsed.to_call == "MR08"
    assert parsed.title == "Widemouth 2 Fire"
    assert parsed.report_ts == report_ts
    assert parsed.rcv_ts == report_ts
    assert parsed.age_ts_source == "report"
    assert parsed.display_type == "General"
    assert "Fire" in parsed.topics


def test_file_message_row_candidate_uses_cache_before_lazy_parsing(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260729-141819Z-50.k2s"
    path.write_text("body", encoding="utf-8")
    stat = path.stat()
    rec = FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    calls: list[str] = []

    cached = file_message_row_candidate(
        rec,
        "flmsg",
        status="READ",
        is_image=False,
        is_transport_form=True,
        cached_meta={
            "msg_type": "FLMSG",
            "from_call": "K7ETC",
            "to_call": "MR08",
            "title": "Cached Widemouth",
            "report_ts": 1780000000,
            "age_ts_source": "report",
            "search_text": "cached widemouth",
        },
        form_meta_loader=lambda: calls.append("form") or {},
        fallback_from_loader=lambda: calls.append("sender") or "N0CALL",
    )

    assert cached.used_cache is True
    assert cached.title == "Cached Widemouth"
    assert calls == []

    parsed = file_message_row_candidate(
        rec,
        "flmsg",
        status="NEW",
        is_image=False,
        is_transport_form=True,
        cached_meta={},
        form_meta_loader=lambda: calls.append("form") or {
            "_raw_head": "\n".join(
                [
                    "MAGNET General Use Form",
                    "Date/Msg ID",
                    "260729-0354z",
                    "To",
                    "MR08",
                    "From",
                    "K7ETC",
                    "Subject",
                    "Parsed Widemouth",
                ]
            ),
            "form_title": "MAGNET General Use Form",
        },
        fallback_from_loader=lambda: calls.append("sender") or "N0CALL",
    )

    assert parsed.used_cache is False
    assert parsed.title == "Parsed Widemouth"
    assert calls == ["form", "sender"]


def test_file_message_row_candidate_ignores_incomplete_cache_for_lazy_parsing(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260729-141819Z-50.k2s"
    path.write_text("body", encoding="utf-8")
    stat = path.stat()
    rec = FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)

    parsed = file_message_row_candidate(
        rec,
        "flmsg",
        status="READ",
        is_image=False,
        is_transport_form=True,
        cached_meta={"status": "READ", "topics_json": "[]"},
        form_meta_loader=lambda: {
            "_raw_head": "\n".join(
                [
                    "MAGNET General Use Form",
                    "Date/Msg ID",
                    "260729-0354z",
                    "To",
                    "MR08",
                    "From",
                    "K7ETC",
                    "Subject",
                    "Recovered From Parse",
                ]
            ),
            "form_title": "MAGNET General Use Form",
        },
    )

    assert parsed.used_cache is False
    assert parsed.title == "Recovered From Parse"
    assert parsed.display_type == "General"


def test_transport_row_presenters_keep_operator_labels_and_search_context() -> None:
    js8 = js8_message_row_presentation(
        SimpleNamespace(
            msg_type="MSG",
            state="UNREAD",
            from_call="n1mag",
            to_call="@MAGNET",
            utc_ts=123.0,
            decoded_text="plain JS8 traffic",
            raw_text="",
        )
    )
    assert js8.msg_type == "JS8 MSG"
    assert js8.status == "NEW"
    assert js8.from_call == "N1MAG"
    assert js8.to_call == "MAGNET"
    assert "js8 traffic" in message_row_search_text(
        js8.msg_type, js8.status, js8.from_call, js8.to_call, "now", js8.search_detail
    )

    spotter = spotter_message_row_presentation(
        SimpleNamespace(
            msg_type="F!307",
            state="UNREAD",
            from_call="K7ETC",
            to_call="@MR08",
            utc_ts=456.0,
            raw_text="F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire Update] DA[260729-0354z]",
            decoded_text="",
        ),
        form_title_lookup={"307": "Wildfire | F!307"},
    )
    assert spotter.title.startswith("Wildfire")
    assert spotter.to_call == "MR08"
    assert "Fire" in spotter.topics
    assert "DM38ST" in spotter.search_detail

    varac = varac_message_row_presentation(
        SimpleNamespace(
            msg_type="VMAIL",
            read_status=0,
            from_call="w1abc",
            to_call="@MAGNET",
            ts=789.0,
            subject="Supply note",
            body="fallback body",
        )
    )
    assert varac.msg_type == "VarAC"
    assert varac.status == "NEW"
    assert varac.title == "VMAIL: Supply note"
    assert varac.to_call == "MAGNET"


def test_cached_file_message_search_ignores_topic_tags_without_content(tmp_path) -> None:
    path = tmp_path / "MCF103_GYQV.txt"
    path.write_text("MCF103 (#GYQV)\n", encoding="utf-8")
    rec = FileRecord(path=path, origin="flmsg", size=path.stat().st_size, mtime=path.stat().st_mtime)
    metadata = {
        "source_family": "flmsg",
        "msg_type": "FLMSG",
        "display_type": "MCF103",
        "status": "NEW",
        "from_call": "AL1Q",
        "to_call": "W3BFO",
        "title": "MCF103 (#GYQV)",
        "topics_json": '["Fire"]',
        "search_text": "",
    }

    weak = cached_file_message_row_candidate(rec, metadata, origin="flmsg", status="NEW")
    assert weak is not None
    weak_row = SimpleNamespace(
        msg_type=weak.msg_type,
        status=weak.status,
        origin="flmsg",
        payload=None,
        rcv_ts=weak.rcv_ts,
        rcv_display="now",
        from_call=weak.from_call,
        to_call=weak.to_call,
        title=weak.title,
        search_text=file_message_search_text(
            weak.msg_type,
            weak.status,
            weak.from_call,
            weak.to_call,
            "now",
            weak.search_detail,
        ),
        actionable=weak.actionable,
        topics=weak.topics,
    )
    assert row_matches_search_query(weak_row, "fire") is False

    strong_metadata = dict(metadata, title="Widemouth 2 Fire", topics_json='["Fire"]')
    strong = cached_file_message_row_candidate(rec, strong_metadata, origin="flmsg", status="NEW")
    assert strong is not None
    strong_row = SimpleNamespace(
        msg_type=strong.msg_type,
        status=strong.status,
        origin="flmsg",
        payload=None,
        rcv_ts=strong.rcv_ts,
        rcv_display="now",
        from_call=strong.from_call,
        to_call=strong.to_call,
        title=strong.title,
        search_text=file_message_search_text(
            strong.msg_type,
            strong.status,
            strong.from_call,
            strong.to_call,
            "now",
            strong.search_detail,
        ),
        actionable=strong.actionable,
        topics=strong.topics,
    )
    assert row_matches_search_query(strong_row, "fire") is True


def test_commstat_row_presenter_flattens_category_without_losing_intelligence() -> None:
    row = commstat_message_row_presentation(
        SimpleNamespace(
            artifact_kind="sitrep",
            title="My Location",
            body_text="GREEN all clear with water and comms available",
            from_call="W1NEM",
            target="@MAGNET",
            report_group="MAGNET",
            state_code="MO",
            grid="EM48EQ",
            scope="",
            status_label="INFO",
            alert_color="GREEN",
            subtype="",
            remarks_text="",
            transport_label="Internet",
            reach_label="Internet only",
            source_family_label="CommStat",
            event_ts=1000.0,
            event_ts_utc="2026-08-03T20:51:12Z",
        )
    )

    assert row.origin == "commstat"
    assert row.status == "INFO"
    assert row.from_call == "W1NEM"
    assert row.to_call == "MAGNET"
    assert row.actionable is True
    assert {"Water", "Comms"}.issubset(set(row.topics))
    assert "EM48EQ" in row.search_detail
    assert "Internet only" in row.search_detail


def test_commstat_field_intelligence_infers_other_location_state_from_body() -> None:
    info = analyze_commstat_fields(
        artifact_kind="STATREP",
        title="CommStat StatRep | COUNTY | YELLOW",
        body="Reno-Sparks NV Evacuation Center||4590 S Virginia St Reno NV 89502",
        from_call="KD9DSS",
        target="@COMMSTAT",
        grid="DM09CL",
        scope="COUNTY",
        status="YELLOW",
    )

    assert info.state == "NV"
    assert info.grid == "DM09CL"
    assert info.metadata["state_confidence"] == "remarks"


def test_commstat_field_intelligence_resolves_regional_state_from_body_over_stale_state() -> None:
    info = analyze_commstat_fields(
        artifact_kind="STATREP",
        title="CommStat StatRep | REGION | YELLOW",
        body="Extreme Heat Warning for most of southern & central AZ still in place until August 29 by NWS Phoenix AZ",
        from_call="K6NLX",
        target="@MAGNET",
        state="IN",
        grid="DM43FJ",
        scope="REGION",
        status="YELLOW",
    )

    assert info.state == "AZ"
    assert info.grid == "DM43FJ"
    assert "Weather" in info.topics
    assert info.metadata["state_confidence"] == "remarks"


def test_form_metadata_parser_is_core_and_handles_missing_templates(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260731-000127Z-54.k2s"
    raw = (
        "CUSTOM_FORM,missing.html\n"
        "L01,MR08\n"
        "L02,K7ETC\n"
        "L03,Widemouth 2 Fire\n"
        "L04,2607290354\n"
        "L06,UT - DM38ST - wildfire update"
    )

    assert parse_custom_form_fields_text(raw)["L03"] == "Widemouth 2 Fire"

    meta = extract_form_metadata_from_text(
        raw,
        path,
        template_title_for_form=lambda _name: "",
        template_labels_for_form=lambda _name: [],
    )

    assert meta["from"] == "K7ETC"
    assert meta["to"] == "MR08"
    assert meta["subject"] == "Widemouth 2 Fire"
    assert meta["date_summary"] == "260729-0354z"
    assert meta["grid"] == "DM38ST"
    assert meta["title"] == "Widemouth 2 Fire - 260729-0354z"
    assert meta["_raw_head"] == raw


def test_message_row_builder_uses_missing_template_subject_for_title(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260731-000127Z-54.k2s"
    path.write_text(
        "CUSTOM_FORM,missing.html\n"
        "L01,MR08\n"
        "L02,K7ETC\n"
        "L03,Widemouth 2 Fire\n"
        "L04,UT - DM38ST - wildfire update",
        encoding="utf-8",
    )
    stat = path.stat()
    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={"flmsg": [FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)]},
        file_metadata_map={},
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=1,
    )
    emitted = []
    worker.finished.connect(lambda payload: emitted.append(payload))

    worker.run()

    row = emitted[0]["rows"][0]
    assert row.title == "Widemouth 2 Fire"
    assert row.from_call == "K7ETC"
    assert row.to_call == "MR08"
    assert "dm38st" in row.search_text


def test_message_row_builder_uses_form_date_for_flmsg_age_not_file_mtime(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260731-000127Z-54.k2s"
    path.write_text(
        "CUSTOM_FORM,missing.html\n"
        "L01,MR08\n"
        "L02,K7ETC\n"
        "L03,Widemouth 2 Fire\n"
        "L04,260729-0354z\n"
        "L05,UT - DM38ST - wildfire update",
        encoding="utf-8",
    )
    file_ts = datetime.datetime(2026, 8, 3, 17, 33, tzinfo=datetime.timezone.utc).timestamp()
    os.utime(path, (file_ts, file_ts))
    stat = path.stat()
    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={"flmsg": [FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)]},
        file_metadata_map={},
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=1,
    )
    emitted = []
    worker.finished.connect(lambda payload: emitted.append(payload))

    worker.run()

    row = emitted[0]["rows"][0]
    assert row.rcv_ts == datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp()
    assert row.rcv_ts != stat.st_mtime


def test_message_row_builder_uses_cached_file_metadata_without_parsing_source(tmp_path) -> None:
    path = tmp_path / "K7ETC-20260803-040212Z-57.k2s"
    path.write_text("this body should not be needed when metadata identity matches", encoding="utf-8")
    stat = path.stat()
    rec = FileRecord(path=path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={"flmsg": [rec]},
        file_metadata_map={
            ("flmsg", str(path), float(stat.st_mtime), int(stat.st_size)): {
                "msg_type": "FLMSG",
                "display_type": "General",
                "status": "NEW",
                "from_call": "K7ETC",
                "to_call": "MR08",
                "title": "Widemouth 2 Fire",
                "rcv_display": "1 min",
                "report_ts": datetime.datetime(2026, 8, 3, 4, 2, 12, tzinfo=datetime.timezone.utc).timestamp(),
                "age_ts_source": "report",
                "topics_json": '["Fire","Travel/Roads"]',
                "actionable": 1,
                "search_text": "",
            }
        },
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=1,
    )
    emitted = []
    worker.finished.connect(lambda payload: emitted.append(payload))
    worker._extract_form_file_metadata = lambda _rec: (_ for _ in ()).throw(AssertionError("metadata fast path parsed source"))

    worker.run()

    row = emitted[0]["rows"][0]
    assert row.msg_type == "FLMSG"
    assert row.display_type == "General"
    assert row.from_call == "K7ETC"
    assert row.to_call == "MR08"
    assert row.title == "Widemouth 2 Fire"
    assert row.rcv_ts == datetime.datetime(2026, 8, 3, 4, 2, 12, tzinfo=datetime.timezone.utc).timestamp()
    assert row.rcv_ts != stat.st_mtime
    assert row.report_ts == row.rcv_ts
    assert row.age_ts_source == "report"
    assert row.topics == ("Fire", "Travel/Roads")
    assert row.actionable is True
    assert "widemouth" in row.search_text
    assert "fire" in row.search_text
    assert "travel/roads" in row.search_text
    assert emitted[0]["file_metadata_hits"] == 1
    assert emitted[0]["file_parse_count"] == 0


def test_inbox_focus_filters_operator_oriented_message_sets() -> None:
    spotter = UnifiedMessage("F!304", "NEW", "K7ETC", "MR08", 1.0, "", "Fire", "spotter", object())
    raw_spotter = UnifiedMessage("F!304", "NEW", "K7ETC", "MR08", 1.0, "", "Fire", "js8", object())
    flmsg = UnifiedMessage("FLMSG", "READ", "K7ETC", "MR08", 1.0, "", "Widemouth", "flmsg", object())
    commstat = UnifiedMessage("CommStat SitRep", "INFO", "K7ETC", "MR08", 1.0, "", "Power", "commstat", object())
    js8 = UnifiedMessage("JS8 MSG", "READ", "K7ETC", "MR08", 1.0, "", "Directed message", "js8", object())
    meshcore = UnifiedMessage("Mesh", "NEW", "K7MESH", "Public", 1.0, "", "Local traffic", "meshcore", object())
    meshtastic = UnifiedMessage("Mesh", "NEW", "K7MES2", "Public", 1.0, "", "Local traffic", "meshtastic", object())
    varac = UnifiedMessage("VarAC", "READ", "K7ETC", "MR08", 1.0, "", "VMAIL", "varac", object())
    bbs = UnifiedMessage("BBS", "READ", "K7ETC", "MR08", 1.0, "", "Bulletin", "bbs", object())
    bbs_archive = UnifiedMessage("BBS", "READ", "K7ETC", "MR08", 1.0, "", "Old Bulletin", "bbs_archive", object())

    assert row_matches_inbox_focus(spotter, "spotter") is True
    assert row_matches_inbox_focus(raw_spotter, "spotter") is True
    assert row_matches_inbox_focus(flmsg, "spotter") is False
    assert row_matches_type_filter(raw_spotter, "Spotter") is False

    assert row_matches_inbox_focus(flmsg, "forms") is True
    assert row_matches_inbox_focus(spotter, "forms") is False

    assert row_matches_inbox_focus(commstat, "commstat") is True
    assert row_matches_inbox_focus(flmsg, "commstat") is False

    assert row_matches_inbox_focus(js8, "js8call") is True
    assert row_matches_inbox_focus(spotter, "js8call") is False

    assert row_matches_inbox_focus(meshcore, "mesh") is True
    assert row_matches_inbox_focus(meshtastic, "mesh") is True
    assert row_matches_inbox_focus(js8, "mesh") is False

    assert row_matches_inbox_focus(varac, "varac") is True
    assert row_matches_inbox_focus(flmsg, "varac") is False

    assert row_matches_inbox_focus(bbs, "bbs") is True
    assert row_matches_inbox_focus(bbs_archive, "bbs") is True
    assert row_matches_inbox_focus(varac, "bbs") is False


def test_mesh_observation_presentation_routes_to_mesh_inbox_focus() -> None:
    observation = Observation(
        observation_id="meshcore:meshcore-field:private-1",
        source_family="meshcore",
        source_ref="mesh:meshcore:local:private-1",
        source_app="MeshCore",
        received_utc="2026-08-31T12:34:56+00:00",
        from_call="K7MESH",
        to_target="County Ops",
        groups=("COUNTY",),
        observed_topics=("Water", "Comms"),
        operator_attention=True,
        status="NEW",
        summary="county ops water outage",
        provenance={
            "channel_policy": {
                "channel_name": "County Ops",
                "channel_privacy": "encrypted",
                "key_status": "Joined",
            },
            "routing": {"route_type": "mesh", "hop_count": 2},
            "surfaces": ["inbox", "ops_center", "map", "topic_scan"],
        },
    )

    presentation = observation_message_row_presentation(observation)
    row = UnifiedMessage(
        presentation.msg_type,
        presentation.status,
        presentation.from_call,
        presentation.to_call,
        presentation.rcv_ts,
        "2026-08-31 12:34:56",
        presentation.title,
        presentation.origin,
        observation,
        topics=presentation.topics,
        actionable=presentation.actionable,
        display_type=presentation.display_type,
    )

    assert row.msg_type == "MeshCore"
    assert row.to_call == "County Ops"
    assert row.topics == ("Water", "Comms")
    assert row_matches_inbox_focus(row, "mesh") is True
    assert row_matches_age_filter(row, 0, now_ts=presentation.rcv_ts + 3600) is True
    assert row_matches_inbox_focus(row, "commstat") is False


def test_mesh_observation_presentation_uses_policy_channel_name_for_channel_target() -> None:
    observation = Observation(
        observation_id="meshcore:meshcore-field:public-1",
        source_family="meshcore",
        source_ref="mesh:meshcore:local:public-1",
        source_app="MeshCore",
        received_utc="2026-08-31T12:34:56+00:00",
        from_call="K7MESH",
        to_target="channel",
        groups=("PUBLIC",),
        observed_topics=("Social",),
        status="INFO",
        summary="hello",
        provenance={
            "channel_policy": {
                "channel_name": "Public",
                "channel_privacy": "public",
                "key_status": "Not needed",
            },
            "surfaces": ["inbox", "ops_center", "map", "topic_scan"],
        },
    )

    presentation = observation_message_row_presentation(observation)

    assert presentation.to_call == "Public"


def test_mesh_node_observation_is_not_message_inbox_traffic() -> None:
    observation = Observation(
        observation_id="meshcore:mesh-node:meshcore:local:node-1",
        source_family="meshcore",
        source_ref="mesh-node:meshcore:local:node-1",
        source_app="MeshCore",
        received_utc="2026-08-31T12:34:56+00:00",
        from_call="NODE1",
        observed_topics=("Comms",),
        status="seen",
        subject="Mesh node: NODE1",
        summary="Mesh node: NODE1",
        provenance={
            "routing": {"route_type": "direct", "direct_receive": True},
            "surfaces": ["map", "ops_center"],
        },
    )
    presentation = observation_message_row_presentation(observation)
    row = UnifiedMessage(
        presentation.msg_type,
        presentation.status,
        presentation.from_call,
        presentation.to_call,
        presentation.rcv_ts,
        "2026-08-31 12:34:56",
        presentation.title,
        presentation.origin,
        observation,
        topics=presentation.topics,
        actionable=presentation.actionable,
        display_type=presentation.display_type,
    )

    assert row_matches_inbox_focus(row, "mesh") is False


def test_inbox_focus_selects_operator_readable_table_profiles() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)

    tab._inbox_focus = "spotter"
    assert MessageViewerTab._message_display_profile_for_current_view(tab, "MSG Type...") == "field_report"

    tab._inbox_focus = "commstat"
    assert MessageViewerTab._message_display_profile_for_current_view(tab, "MSG Type...") == "intel_report"

    tab._inbox_focus = "forms"
    assert MessageViewerTab._message_display_profile_for_current_view(tab, "MSG Type...") == "form_message"

    tab._inbox_focus = "all"
    assert MessageViewerTab._message_display_profile_for_current_view(tab, "Spotter") == "field_report"
    assert MessageViewerTab._message_display_profile_for_current_view(tab, "FLMSG/FLAMP") == "form_message"
    assert message_display_profile_for_focus_type("spotter", "MSG Type...") == "field_report"
    assert message_display_profile_for_focus_type("forms", "MSG Type...") == "form_message"
    assert message_display_profile_headers("intel_report")[1] == (
        "",
        "Kind",
        "Status",
        "From",
        "To",
        "State / Grid",
        "Age",
        "",
    )


def test_spotter_mcf_column_uses_short_name_and_code() -> None:
    assert _spotter_mcf_display_label("F!307", "MCF307 Wildfire Status Report | K7ETC -> MR08 | DM38") == "Wildfire | F!307"
    assert _spotter_mcf_display_label("F!304", "MCF304 Individual Situation Report") == "Individual | F!304"
    assert _spotter_mcf_display_label("F!701", "K7ETC -> MR08") == "F!701"
    assert spotter_mcf_display_label("307", "MCF307 Wildfire Status Report") == "Wildfire | F!307"


def test_field_report_display_helpers_are_core_structural_helpers() -> None:
    spotter = SpotterMessage(
        spotter_id=1,
        from_call="K7ETC",
        to_call="@MR08",
        msg_type="F!307",
        utc_str="",
        utc_ts=1.0,
        raw_text="F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire]",
        decoded_text="",
        state="UNREAD",
    )
    row = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 1.0, "", "MCF307 Wildfire Status Report", "spotter", spotter)

    assert field_report_form_label(row) == "Wildfire | F!307"
    assert field_report_status_label(row) == "NEW"
    assert field_report_area_label(row) == "UT / DM38ST"
    assert relative_age_label(100.0, now_ts=100.0) == "now"
    assert relative_age_label(0.0, now_ts=65.0) == "1 min"


def test_active_inbox_scope_summary_is_core_operator_text() -> None:
    assert active_inbox_scope_summary() == "current view"
    summary = active_inbox_scope_summary(
        focus="spotter",
        focus_labels={"spotter": "Spotter"},
        groups={"MR08", "MAGNET", "MR09", "MR10"},
        sources={"spotter", "varac", "js8", "commstat"},
        age_label="Older than 2 weeks",
        search_query="wildfire",
        type_sel="MSG Type...",
        status_sel="Action Needed",
        from_sel="K7ETC",
        to_sel="MR08",
    )

    assert "Focus Spotter" in summary
    assert "Groups MAGNET, MR08, MR09 +1" in summary
    assert "Sources CommStat, FIOSpotter, JS8Call +1" in summary
    assert "Older than 2 weeks" in summary
    assert 'Search "wildfire"' in summary
    assert "Status Action Needed" in summary
    assert "From K7ETC" in summary
    assert "To MR08" in summary


def test_message_group_filter_uses_route_groups_without_treating_direct_calls_as_groups() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = type(
        "Settings",
        (),
        {
            "get": lambda _self, key, default=None: {
                "operating_groups": [{"group": "MAGNET"}],
                "local_net_profiles": [{"group": "LOCAL ARES"}],
            }.get(key, default)
        },
    )()
    direct = UnifiedMessage("JS8 MSG", "READ", "K7ETC", "W1NEM", 1.0, "", "Direct", "js8", object())
    hf_group = UnifiedMessage("FLMSG", "READ", "K7ETC", "MAGNET", 1.0, "", "Net", "flmsg", object())
    regional = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 1.0, "", "Fire", "spotter", object())
    relay = UnifiedMessage("SitRep", "INFO", "N1MAG", "K7RIE>", 1.0, "", "F!304 GREEN", "sitrep", object())

    assert MessageViewerTab._message_group_value(tab, direct) == "unassigned"
    assert MessageViewerTab._message_group_value(tab, hf_group) == "MAGNET"
    assert MessageViewerTab._message_group_value(tab, regional) == "MR08"
    assert MessageViewerTab._message_group_value(tab, relay) == "unassigned"
    assert MessageViewerTab._message_group_options(tab, []) == [("unassigned", "Unassigned")]


def test_message_group_value_core_extracts_operating_groups_without_callsign_noise() -> None:
    direct = UnifiedMessage("JS8 MSG", "READ", "K7ETC", "W1NEM", 1.0, "", "Direct", "js8", object())
    hf_group = UnifiedMessage("FLMSG", "READ", "K7ETC", "MAGNET", 1.0, "", "Net", "flmsg", object())
    regional = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 1.0, "", "Fire", "spotter", object())
    relay = UnifiedMessage("SitRep", "INFO", "N1MAG", "K7RIE>", 1.0, "", "F!304 GREEN", "sitrep", object())
    payload_group = UnifiedMessage(
        "CommStat",
        "INFO",
        "K7ETC",
        "W9BVM",
        1.0,
        "",
        "CommStat",
        "commstat",
        type("Payload", (), {"report_group": "MAGNET *", "to_call": "@W9BVM"})(),
    )

    assert message_group_value(direct, configured_groups={"MAGNET"}) == "unassigned"
    assert message_group_value(hf_group, configured_groups={"MAGNET"}) == "MAGNET"
    assert message_group_value(regional, configured_groups=set()) == "MR08"
    assert message_group_value(relay, configured_groups=set()) == "unassigned"
    assert message_group_value(payload_group, configured_groups={"MAGNET"}) == "MAGNET"
    assert message_group_value(
        UnifiedMessage("JS8 MSG", "INFO", "N1MAG", "MR08>", 1.0, "", "Relay", "js8", object()),
        configured_groups=set(),
    ) == "unassigned"


def test_message_workspace_scope_core_matches_selected_source_and_group() -> None:
    flmsg = UnifiedMessage("FLMSG", "READ", "K7ETC", "MAGNET", 1.0, "", "Net", "flmsg", object())
    spotter = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 1.0, "", "Fire", "spotter", object())
    direct_js8 = UnifiedMessage("JS8 MSG", "READ", "K7ETC", "W1NEM", 1.0, "", "Direct", "js8", object())

    assert row_matches_workspace_scope(flmsg, selected_sources={"flmsg"}, selected_groups={"MAGNET"})
    assert not row_matches_workspace_scope(flmsg, selected_sources={"spotter"}, selected_groups={"MAGNET"})
    assert row_matches_workspace_scope(spotter, selected_groups={"MR08"})
    assert not row_matches_workspace_scope(direct_js8, selected_groups={"W1NEM"}, configured_groups={"MAGNET"})


def test_message_group_filter_core_normalizes_groups_without_promoting_callsigns() -> None:
    assert normalize_message_group_filter_value(" MAGNET * ") == "MAGNET"
    assert normalize_message_group_filter_value("@MR08") == "MR08"
    assert is_message_group_candidate("MR08")
    assert is_message_group_candidate("MAGNET")
    assert not is_message_group_candidate("W0IFM")
    assert is_message_group_candidate("W0IFM", configured_groups={"W0IFM"})
    assert not is_message_group_candidate("K7RIE>")
    assert message_group_candidate_set(["MAGNET *", "W0IFM", "@MR08", ""]) == {"MAGNET", "MR08"}


def test_message_group_option_sections_prioritize_configured_then_active_groups() -> None:
    source_map = message_group_source_map(
        [("MR08", "spotter"), ("ANYNET", "js8"), ("MAGNET", "flmsg"), ("W0IFM", "commstat"), ("K7RIE>", "js8")],
        family_map={"MAGNET": {"MAGNET", "MR08", "MRHUB"}},
    )

    assert source_map["MR08"] == {"spotter"}
    assert source_map["MAGNET"] == {"flmsg", "spotter"}
    assert "W0IFM" not in source_map
    assert "K7RIE" not in source_map
    assert primary_message_group_values(
        source_map,
        fio_configured_groups={"MAGNET"},
        commstat_active_groups=set(),
        commstat_configured_groups=set(),
    ) == {"MAGNET"}

    sections = message_group_option_sections(
        {
            "AMRRON": {"commstat"},
            "MAGNET": {"flmsg"},
            "MR08": {"spotter"},
            "ANYNET": {"js8"},
        },
        fio_configured_groups={"MAGNET"},
        commstat_active_groups={"AMRRON"},
        commstat_configured_groups={"AMRRON", "MR08"},
        show_all_groups=False,
    )

    assert sections == [
        ("Configured Groups", [("MAGNET", "MAGNET")]),
        ("CommStat Active Groups", [("AMRRON", "AMRRON")]),
    ]

    expanded = message_group_option_sections(
        {
            "AMRRON": {"commstat"},
            "MAGNET": {"flmsg"},
            "MR08": {"spotter"},
            "ANYNET": {"js8"},
        },
        fio_configured_groups={"MAGNET"},
        commstat_active_groups={"AMRRON"},
        commstat_configured_groups={"AMRRON", "MR08"},
        show_all_groups=True,
    )

    assert expanded == [
        ("Configured Groups", [("MAGNET", "MAGNET")]),
        ("CommStat Active Groups", [("AMRRON", "AMRRON")]),
        ("Other CommStat Groups", [("MR08", "MR08")]),
        ("Other Discovered Groups", [("ANYNET", "ANYNET")]),
    ]


def test_message_group_rebuild_selection_defaults_to_primary_when_showing_all_groups() -> None:
    source_map = {
        "AMRRON": {"commstat"},
        "MAGNET": {"flmsg"},
        "MR08": {"spotter"},
        "ANYNET": {"js8"},
    }

    selected, select_all = message_group_rebuild_selection(
        source_map,
        current_selected={"AMRRON", "MAGNET"},
        current_all_selected=True,
        fio_configured_groups={"MAGNET"},
        commstat_active_groups={"AMRRON"},
        commstat_configured_groups={"AMRRON", "MR08"},
        show_all_groups=True,
    )

    assert selected == ["AMRRON", "MAGNET"]
    assert select_all is False


def test_message_group_rebuild_selection_preserves_explicit_none_in_show_all_groups() -> None:
    selected, select_all = message_group_rebuild_selection(
        {"MAGNET": {"flmsg"}, "MR08": {"spotter"}},
        current_selected=set(),
        current_all_selected=False,
        fio_configured_groups={"MAGNET"},
        commstat_active_groups=set(),
        commstat_configured_groups=set(),
        show_all_groups=True,
    )

    assert selected == []
    assert select_all is False


def test_message_group_rebuild_selection_does_not_autoselect_other_discovered_groups() -> None:
    selected, select_all = message_group_rebuild_selection(
        {"MAGNET": {"flmsg"}, "MR08": {"spotter"}, "ANYNET": {"js8"}},
        current_selected={"MAGNET", "MR08", "ANYNET"},
        current_all_selected=True,
        fio_configured_groups={"MAGNET"},
        commstat_active_groups=set(),
        commstat_configured_groups=set(),
        show_all_groups=True,
        prefer_primary=True,
    )

    assert selected == ["MAGNET"]
    assert select_all is False


def test_message_group_filter_prioritizes_configured_and_collapses_starred_variants() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = type(
        "Settings",
        (),
        {
            "get": lambda _self, key, default=None: {
                "operating_groups": [{"group": "MAGNET"}, {"group_name": "S2 UNDERGROUND"}],
                "local_net_profiles": [{"name": "LOCAL ARES"}],
            }.get(key, default)
        },
    )()
    tab.show_all_message_groups_chk = type("ShowAll", (), {"isChecked": lambda _self: True})()
    tab._commstat_group_state = lambda: CommStatGroupState(
        configured_groups=frozenset({"MAGNET", "MR08"}),
        active_groups=frozenset({"MR08"}),
        unchecked_groups=frozenset(),
        show_other_groups=True,
    )
    commstat_magnet = _commstat_artifact(report_group="MAGNET *", target="@MAGNET", artifact_key="commstat:magnet")
    commstat_mr08 = _commstat_artifact(report_group="MR08", target="@MR08", artifact_key="commstat:mr08")
    js8_other = UnifiedMessage("JS8 MSG", "READ", "K7ETC", "@ANYNET", 1.0, "", "Ping", "js8", object())
    rows = [
        UnifiedMessage("CommStat", "INFO", "K7ETC", "MAGNET", 1.0, "", "Magnet", "commstat", commstat_magnet),
        UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 1.0, "", "MR08", "commstat", commstat_mr08),
        js8_other,
    ]

    options = MessageViewerTab._message_group_options(tab, rows)

    assert [value for value, _label in options[:3]] == ["MAGNET", "MR08", "ANYNET"]
    assert ("MAGNET", "MAGNET") in options
    assert ("MAGNET *", "MAGNET *") not in options
    assert ("MR08", "MR08") in options


def test_message_group_filter_mirrors_commstat_configured_groups_until_show_all_enabled() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = type(
        "Settings",
        (),
        {
            "get": lambda _self, key, default=None: {
                "operating_groups": [{"group": "MAGNET"}],
                "local_net_profiles": [],
            }.get(key, default)
        },
    )()
    tab._commstat_group_state = lambda: CommStatGroupState(
        configured_groups=frozenset({"AMRRON", "MAGNET", "W9BVM"}),
        active_groups=frozenset({"AMRRON", "W0IFM"}),
        unchecked_groups=frozenset({"MAGNET"}),
        show_other_groups=False,
    )
    active_toggle = type("ShowAll", (), {"isChecked": lambda _self: False})()
    show_all_toggle = type("ShowAll", (), {"isChecked": lambda _self: True})()
    commstat_other = _commstat_artifact(report_group="MR08", target="@MR08", artifact_key="commstat:mr08")
    commstat_callsign = _commstat_artifact(report_group="W0IFM", target="W0IFM", artifact_key="commstat:w0ifm")
    commstat_at_callsign = _commstat_artifact(report_group="", target="@W9BVM", artifact_key="commstat:w9bvm")
    rows = [
        UnifiedMessage("CommStat", "INFO", "K7ETC", "AMRRON", 1.0, "", "AMRRON", "commstat", _commstat_artifact(report_group="AMRRON")),
        UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 1.0, "", "MR08", "commstat", commstat_other),
        UnifiedMessage("CommStat", "INFO", "K7ETC", "W0IFM", 1.0, "", "Direct", "commstat", commstat_callsign),
        UnifiedMessage("CommStat", "INFO", "K7ETC", "W9BVM", 1.0, "", "Direct", "commstat", commstat_at_callsign),
    ]

    tab.show_all_message_groups_chk = active_toggle
    focused_options = MessageViewerTab._message_group_options(tab, rows)
    assert [value for value, _label in focused_options] == ["AMRRON"]
    assert ("MR08", "MR08") not in focused_options

    tab.show_all_message_groups_chk = show_all_toggle
    expanded_options = MessageViewerTab._message_group_options(tab, rows)
    assert ("MR08", "MR08") in expanded_options
    assert not any(value == "W0IFM" for value, _label in expanded_options)
    assert not any(value == "W9BVM" for value, _label in expanded_options)


def test_message_group_options_promote_roster_parent_for_child_region_traffic() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = type(
        "Settings",
        (),
        {
            "get": lambda _self, key, default=None: {
                "operating_groups": [{"group": "MAGNET"}],
                "local_net_profiles": [],
            }.get(key, default)
        },
    )()
    tab.show_all_message_groups_chk = type("ShowAll", (), {"isChecked": lambda _self: False})()
    tab._commstat_group_state = lambda: CommStatGroupState(
        configured_groups=frozenset(),
        active_groups=frozenset(),
        unchecked_groups=frozenset(),
        show_other_groups=False,
    )
    tab._operator_group_family_map = lambda: {"MAGNET": {"MAGNET", "MR08", "MRHUB"}}
    row = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 1.0, "", "Wildfire", "spotter", object())

    options = MessageViewerTab._message_group_options(tab, [row])

    assert options == [("MAGNET", "MAGNET")]


def test_message_group_filter_parent_selection_matches_roster_child_region() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._selected_message_sources = lambda: None
    tab._selected_message_groups = lambda: {"MAGNET"}
    tab._operator_group_family_map = lambda: {"MAGNET": {"MAGNET", "MR08", "MRHUB"}}
    row = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 1.0, "", "Wildfire", "spotter", object())

    assert MessageViewerTab._row_matches_workspace_filters(tab, row) is True


def test_message_search_includes_decoded_payload_metadata() -> None:
    payload = {
        "groups": ["KC7VQR", "MAGNET"],
        "topics": ["Food", "Power"],
        "summary": "Food report from KI6QDB",
        "from_call": "KI6QDB",
        "route": "KC7VQR -> Food -> from KI6QDB",
    }
    row = UnifiedMessage(
        "F!104",
        "NEW",
        "KI6QDB",
        "MAGNET",
        1.0,
        "",
        "Food Reports",
        "spotter",
        payload,
    )

    assert row_matches_search_query(row, "Food KC7VQR")
    assert row_matches_search_query(row, "KI6QDB Power")
    assert row_matches_search_query(row, "@MAGNET")


def test_message_search_ignores_no_report_structured_fields() -> None:
    payload = {
        "status": {"food": "not_reported", "power": "green"},
        "summary": "Routine status update",
    }
    row = UnifiedMessage("F!304", "NEW", "K7ABC", "MAGNET", 1.0, "", "Status Update", "spotter", payload)

    assert not row_matches_search_query(row, "food")
    assert row_matches_search_query(row, "power")


def test_message_search_matches_real_power_status_value() -> None:
    payload = {"status": {"power": "Grid down"}, "summary": "Routine status update"}
    row = UnifiedMessage("F!304", "NEW", "K7ABC", "MAGNET", 1.0, "", "Status Update", "spotter", payload)

    assert row_matches_search_query(row, "power")


def test_message_search_ignores_no_report_status_lines() -> None:
    payload = {
        "status_fields": "\n".join(
            [
                "Status Fields",
                "Overall: Not Reported",
                "Power: Not Reported",
                "Food: Not Reported",
                "Water: Not Reported",
            ]
        ),
        "summary": "Routine status update",
    }
    row = UnifiedMessage("SitRep", "INFO", "K7ABC", "MAGNET", 1.0, "", "CommStat", "commstat", payload)

    assert not row_matches_search_query(row, "food")
    assert not row_matches_search_query(row, "power")

    payload["status_fields"] = "Food: Limited supply\nPower: Not Reported"
    row = UnifiedMessage("SitRep", "INFO", "K7ABC", "MAGNET", 1.0, "", "CommStat", "commstat", payload)
    assert row_matches_search_query(row, "food")
    assert not row_matches_search_query(row, "power")


def test_spotter_form_discovery_prefers_selected_radio_profile_path(tmp_path) -> None:
    global_forms = tmp_path / "global" / "forms"
    radio_forms = tmp_path / "radio-a" / "forms"
    global_forms.mkdir(parents=True)
    radio_forms.mkdir(parents=True)
    (global_forms / "MCF307.txt").write_text("Global Wildfire Form\n?Global", encoding="utf-8")
    (radio_forms / "MCF104.txt").write_text("Radio Status Form\n?Radio", encoding="utf-8")

    class Settings:
        def get(self, key, default=None):
            values = {"js8_forms_path": str(global_forms)}
            return values.get(key, default)

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = Settings()
    tab.forms_path = str(global_forms)
    tab._compose_mode = "spotter"
    tab._form_cache = {}
    tab._form_title_cache = {}
    tab._cache_max_form_entries = 20
    tab._cache_max_form_title_entries = 20
    target = ComposeRadioTarget(
        radio_id=1,
        label="FIO-A",
        profile={"id": 1, "name": "FIO-A", "js8_forms_path": str(radio_forms)},
        capabilities=("JS8Call",),
    )
    tab._selected_compose_radio_target = lambda: target

    entries = MessageViewerTab._compose_family_entries(tab)
    spotter_entry = next(entry for entry in entries if entry["kind"] == "spotter")

    assert [form.form_code for form in spotter_entry["forms"]] == ["F!104"]
    assert MessageViewerTab._load_form_title(tab, "104") == "Radio Status Form"
    assert MessageViewerTab._load_form_title(tab, "307") == ""


def test_message_group_expansion_default_selection_excludes_other_discovered_groups() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = type(
        "Settings",
        (),
        {
            "get": lambda _self, key, default=None: {
                "operating_groups": [{"group": "MAGNET"}],
                "local_net_profiles": [],
            }.get(key, default)
        },
    )()
    tab._commstat_group_state = lambda: CommStatGroupState(
        configured_groups=frozenset({"AMRRON"}),
        active_groups=frozenset({"AMRRON"}),
        unchecked_groups=frozenset(),
        show_other_groups=False,
    )

    class Toggle:
        def __init__(self) -> None:
            self.checked = True

        def isChecked(self) -> bool:
            return self.checked

        def setChecked(self, checked: bool) -> None:
            self.checked = bool(checked)

        def blockSignals(self, _blocked: bool) -> None:
            return None

    tab.show_all_message_groups_chk = Toggle()
    tab._update_show_all_message_groups_style = lambda: None
    rows = [
        UnifiedMessage("CommStat", "INFO", "K7ETC", "AMRRON", 1.0, "", "AMRRON", "commstat", _commstat_artifact(report_group="AMRRON")),
        UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 1.0, "", "MR08", "commstat", _commstat_artifact(report_group="MR08")),
    ]

    assert MessageViewerTab._primary_message_group_values(tab, rows) == {"AMRRON"}


def test_message_group_options_follow_current_message_focus() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = type(
        "Settings",
        (),
        {
            "get": lambda _self, key, default=None: {
                "operating_groups": [{"group": "MAGNET"}, {"group": "S2 UNDERGROUND"}],
                "local_net_profiles": [],
            }.get(key, default)
        },
    )()
    tab._commstat_group_state = lambda: CommStatGroupState(
        configured_groups=frozenset(),
        active_groups=frozenset(),
        unchecked_groups=frozenset(),
        show_other_groups=False,
    )
    tab.show_all_message_groups_chk = type("ShowAll", (), {"isChecked": lambda _self: False})()
    tab._inbox_focus = "forms"
    tab.type_filter = type("Combo", (), {"currentText": lambda _self: "MSG Type..."})()
    tab.status_filter = type("Combo", (), {"currentText": lambda _self: "Status..."})()
    tab.from_filter = type("Combo", (), {"currentText": lambda _self: ""})()
    tab.to_filter = type("Combo", (), {"currentText": lambda _self: ""})()
    tab.received_filter = type("Combo", (), {"currentData": lambda _self: 0})()
    tab.rcv_search = type("Search", (), {"text": lambda _self: ""})()
    tab._excluded_msg_types = set()

    flmsg = UnifiedMessage("FLMSG", "NEW", "K7ETC", "MAGNET", 1.0, "", "General", "flmsg", object())
    js8 = UnifiedMessage("JS8 MSG", "NEW", "K7ETC", "S2 UNDERGROUND", 1.0, "", "Ping", "js8", object())
    rows = [flmsg, js8]

    scoped_rows = MessageViewerTab._rows_for_group_filter_options(tab, rows)
    scoped_options = MessageViewerTab._message_group_options(tab, scoped_rows)

    assert scoped_rows == [flmsg]
    assert ("MAGNET", "MAGNET") in scoped_options
    assert not any(value == "S2 UNDERGROUND" for value, _label in scoped_options)


def test_message_source_filter_always_includes_connected_app_sources() -> None:
    assert {"js8", "varac"} <= {
        value for value, _label in message_source_options([])
    }
    row = UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 1.0, "", "Power", "commstat", object())
    assert message_source_value(row) == "commstat"
    assert ("commstat", "CommStat") in message_source_options([row])


def test_commstat_source_filter_matches_projected_commstat_sitreps() -> None:
    payload = SimpleNamespace(
        source_family="sitrep",
        source_label="SitRep",
        message_type="COMMSTAT",
        source_family_label="CommStat",
    )
    row = UnifiedMessage("COMMSTAT", "INFO", "K7ETC", "MR08", 1.0, "", "Power update", "sitrep", payload)

    assert "commstat" in message_source_aliases(row)
    assert row_matches_source_filter(row, {"commstat"}) is True
    assert row_matches_workspace_scope(row, selected_sources={"commstat"}) is True
    assert row_matches_inbox_focus(row, "commstat") is True


def test_message_filter_recovery_selects_all_when_stale_scope_hides_loaded_rows() -> None:
    class FakeDropdown:
        def __init__(self, options: list[tuple[str, str]]) -> None:
            self._options = options
            self.selected: list[str] = []

        def blockSignals(self, _blocked: bool) -> None:
            pass

        def set_selected_values(self, values: list[str]) -> None:
            self.selected = list(values)

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.source_filter = FakeDropdown([("commstat", "CommStat"), ("js8", "JS8Call")])
    tab.operating_group_filter = FakeDropdown([("MR08", "MR08"), ("MAGNET", "MAGNET")])
    tab._configured_message_group_names = lambda: {"MR08", "MAGNET"}
    row = UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 1.0, "", "Power", "commstat", SimpleNamespace())

    recovered = MessageViewerTab._recover_empty_message_filter_state(
        tab,
        [row],
        InboxFilterCriteria(focus="all"),
        {"missing-source"},
        {"MISSING-GROUP"},
    )

    assert recovered is True
    assert tab.source_filter.selected == ["commstat", "js8"]
    assert tab.operating_group_filter.selected == ["MR08", "MAGNET"]


def test_inbox_focus_aligns_source_filter_to_commstat() -> None:
    class FakeSourceFilter:
        def __init__(self) -> None:
            self._options = [("js8", "JS8Call"), ("spotter", "FIOSpotter"), ("commstat", "CommStat")]
            self.selected: list[str] = ["spotter"]

        def blockSignals(self, _blocked: bool) -> None:
            pass

        def set_selected_values(self, values: list[str]) -> None:
            self.selected = list(values)

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.source_filter = FakeSourceFilter()

    MessageViewerTab._sync_source_filter_for_inbox_focus(tab, "commstat")

    assert tab.source_filter.selected == ["commstat"]


def test_js8_relay_route_display_is_not_group_or_destination_noise() -> None:
    assert _js8_relay_route_display("K7RIE>", "N1MAG: K7RIE>KC7WOK status") == "KC7WOK via K7RIE"
    assert _js8_relay_route_display("K7RIE>", "F!304 GREEN") == "via K7RIE"


def test_form_message_profile_puts_message_first_and_uses_form_family_type() -> None:
    model = MessageTableModel(
        [
            UnifiedMessage(
                "FLMSG",
                "NEW",
                "K7ETC",
                "MR08",
                1.0,
                "",
                "Widemouth 2 Fire",
                "flmsg",
                object(),
                display_type="General",
            )
        ]
    )
    model.set_display_profile("form_message", "Received")

    assert model.headerData(1, Qt.Horizontal) == "Message"
    assert model.headerData(2, Qt.Horizontal) == "Type"
    assert model.data(model.index(0, 1), Qt.DisplayRole) == "Widemouth 2 Fire"
    assert model.data(model.index(0, 2), Qt.DisplayRole) == "General"


def test_form_message_type_label_normalizes_common_nbems_form_families() -> None:
    assert form_message_type_label("MAGNET General Use Form - v1.1.1", "FLMSG") == "General"
    assert form_message_type_label("<blankform>", "FLMSG") == "Blank"
    assert form_message_type_label("ICS 213 General Message", "FLMSG") == "ICS 213"
    assert form_message_type_label("Individual Situation Report", "FLMSG") == "SitRep"
    assert form_message_type_label("Hospital Status Report", "FLMSG") == "StatRep"


def test_received_age_filter_supports_recent_and_cleanup_windows() -> None:
    now_ts = 1_800_000_000.0
    recent = UnifiedMessage("FLMSG", "READ", "K7ETC", "MR08", now_ts - 3600, "", "Recent", "flmsg", object())
    old = UnifiedMessage("FLMSG", "READ", "K7ETC", "MR08", now_ts - (45 * 24 * 60 * 60), "", "Old", "flmsg", object())
    undated_mesh = UnifiedMessage("Mesh", "NEW", "K7MESH", "Public", 0.0, "", "Mesh chatter", "meshcore", object())

    assert row_matches_age_filter(undated_mesh, 0, now_ts=now_ts) is True
    assert row_matches_inbox_criteria(
        undated_mesh,
        InboxFilterCriteria(focus="mesh", age_filter_seconds=0, now_ts=now_ts),
    ) is True
    assert row_matches_age_filter(recent, 24 * 60 * 60, now_ts=now_ts) is True
    assert row_matches_age_filter(old, 24 * 60 * 60, now_ts=now_ts) is False
    assert row_matches_age_filter(old, -14 * 24 * 60 * 60, now_ts=now_ts) is True
    assert row_matches_age_filter(recent, -14 * 24 * 60 * 60, now_ts=now_ts) is False


def test_inbox_status_and_search_filters_are_core_row_logic() -> None:
    flagged_payload = type("Payload", (), {"flag_state": 1})()
    flagged = UnifiedMessage("F!307", "READ", "K7ETC", "MR08", 1.0, "1h", "Wildfire", "spotter", flagged_payload)
    actionable = UnifiedMessage("FLMSG", "READ", "K7ETC", "MR08", 1.0, "1h", "Water Update", "flmsg", object(), actionable=True)
    read = UnifiedMessage("JS8 MSG", "READ", "K7ETC", "MR08", 1.0, "1h", "Plain", "js8", object())
    searchable = UnifiedMessage("FLMSG", "NEW", "K7ETC", "MR08", 1.0, "1h", "Widemouth 2 Fire", "flmsg", object())

    assert row_matches_status_filter(flagged, "Action Needed") is True
    assert row_matches_status_filter(actionable, "Action Needed") is True
    assert row_matches_status_filter(read, "Action Needed") is False
    assert row_matches_status_filter(searchable, "NEW") is True
    assert row_matches_status_filter(searchable, "READ") is False
    assert "widemouth" in row_search_text(searchable)
    assert row_matches_search_query(searchable, "MR08") is True
    assert row_matches_search_query(searchable, "widemouth") is True
    assert row_matches_search_query(searchable, "K7ETC Fire MR08") is True
    assert row_matches_search_query(searchable, "not-present") is False


def test_map_concern_context_filters_green_messages_but_keeps_caution() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._map_context_filter = {"concern_only": True}
    green_payload = SimpleNamespace(status_label="GREEN", alert_color="green", overall_status="Functioning")
    yellow_payload = SimpleNamespace(status_label="YELLOW", alert_color="yellow", overall_status="Caution")

    green = UnifiedMessage("CommStat", "INFO", "K7ETC", "MAGNET", 1.0, "", "GREEN", "commstat", green_payload)
    yellow = UnifiedMessage("CommStat", "INFO", "K7ETC", "MAGNET", 1.0, "", "YELLOW", "commstat", yellow_payload)

    assert MessageViewerTab._row_matches_map_context_filter(tab, yellow) is True
    assert MessageViewerTab._row_matches_map_context_filter(tab, green) is False


def test_map_context_filters_messages_by_structured_state_and_fema_region() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._map_context_filter = {
        "concern_only": True,
        "state_filter": "",
        "fema_region_filter": "R05",
    }
    in_region_payload = _commstat_artifact(state_code="IN", status_label="RED", alert_color="red")
    out_region_payload = _commstat_artifact(state_code="NV", status_label="RED", alert_color="red")
    in_region = UnifiedMessage("CommStat", "INFO", "KD9DSS", "MAGNET", 1.0, "", "IN RED", "commstat", in_region_payload)
    out_region = UnifiedMessage("CommStat", "INFO", "KG6MTM", "MAGNET", 1.0, "", "NV RED", "commstat", out_region_payload)

    assert MessageViewerTab._row_matches_map_context_filter(tab, in_region) is True
    assert MessageViewerTab._row_matches_map_context_filter(tab, out_region) is False

    tab._map_context_filter = {
        "concern_only": True,
        "state_filter": "NV",
        "fema_region_filter": "",
    }

    assert MessageViewerTab._row_matches_map_context_filter(tab, in_region) is False
    assert MessageViewerTab._row_matches_map_context_filter(tab, out_region) is True


def test_inbox_filter_criteria_combines_common_row_filters() -> None:
    now_ts = 1_800_000_000.0
    keep = UnifiedMessage("FLMSG", "NEW", "K7ETC", "MR08", now_ts - 600, "", "Widemouth 2 Fire", "flmsg", object())
    hidden = UnifiedMessage("JS8 MSG", "NEW", "K7ETC", "MR08", now_ts - 600, "", "Ping", "js8", object())
    wrong_sender = UnifiedMessage("FLMSG", "NEW", "N0CALL", "MR08", now_ts - 600, "", "Widemouth", "flmsg", object())
    old = UnifiedMessage("FLMSG", "NEW", "K7ETC", "MR08", now_ts - 90_000, "", "Widemouth", "flmsg", object())
    criteria = InboxFilterCriteria(
        focus="forms",
        type_sel="MSG Type...",
        status_sel="NEW",
        from_sel="K7ETC",
        to_sel="MR08",
        age_filter_seconds=3600,
        search_query="fire",
        excluded_types=frozenset({"JS8Call"}),
        now_ts=now_ts,
    )

    assert row_matches_inbox_criteria(keep, criteria) is True
    assert row_matches_inbox_criteria(hidden, criteria) is False
    assert row_matches_inbox_criteria(wrong_sender, criteria) is False
    assert row_matches_inbox_criteria(old, criteria) is False


def test_deleted_message_rows_are_suppressed_from_current_view() -> None:
    payload = SpotterMessage(
        spotter_id=44,
        from_call="K7ETC",
        to_call="@MR08",
        msg_type="F!304",
        utc_str="2026-08-11 12:00:00",
        utc_ts=1.0,
        raw_text="F!304 TO[@MR08] FR[K7ETC] NA[Fire]",
        decoded_text="",
        state="UNREAD",
    )
    deleted = UnifiedMessage("F!304", "NEW", "K7ETC", "MR08", 1.0, "", "Fire", "spotter", payload)
    kept = UnifiedMessage("FLMSG", "READ", "N1MAG", "MAGNET", 2.0, "", "OpNet", "flmsg", object())
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._locally_deleted_row_keys = set()
    tab._message_rows = [deleted, kept]

    class Model:
        def __init__(self):
            self._rows = [deleted, kept]

        def rows(self):
            return list(self._rows)

    rendered = []
    tab._messages_model = Model()
    tab._render_messages_table = lambda rows: rendered.append(rows)

    MessageViewerTab._remember_locally_deleted_row(tab, deleted)
    MessageViewerTab._remove_deleted_rows_from_current_view(tab, [deleted])

    assert tab._message_rows == [kept]
    assert rendered[-1] == [kept]


def test_message_row_builder_strips_js8_group_marker_for_spotter_display() -> None:
    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[
            SpotterMessage(
                spotter_id=1,
                from_call="K7ETC",
                to_call="@MR08",
                msg_type="F!307",
                utc_str="2026-08-10 12:00:00",
                utc_ts=1786363200.0,
                raw_text="F!307 TO[@MR08] FR[K7ETC] GR[DM38ST] NA[Fire status] #D2NT",
                decoded_text="",
                state="UNREAD",
            )
        ],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={},
        file_metadata_map={},
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={"307": "MCF307 Wildfire Status Report"},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=1,
    )
    emitted = []
    worker.finished.connect(lambda payload: emitted.append(payload))

    worker.run()

    row = emitted[0]["rows"][0]
    assert row.to_call == "MR08"
    assert "K7ETC -> MR08" in row.title
    assert "@MR08" not in row.title


def test_selected_messages_summary_is_operator_readable_without_raw_payload() -> None:
    text = MessageViewerTab._selected_messages_summary_text(
        [
            UnifiedMessage(
                msg_type="FLMSG",
                status="NEW",
                from_call="K7ETC",
                to_call="MR08",
                rcv_ts=0.0,
                rcv_display="",
                title="MAGNET General Use Form | K7ETC -> MR08 | Widemouth 2 Fire | 260803-0402z",
                origin="flmsg",
                payload=object(),
                topics=("Fire", "Travel/Roads"),
                actionable=True,
            )
        ]
    )

    assert text.startswith("Selected Messages Summary")
    assert "Messages: 1" in text
    assert "FLMSG | NEW" in text
    assert "K7ETC -> MR08" in text
    assert "Widemouth 2 Fire" in text
    assert "Topics: Fire, Travel/Roads" in text
    assert "raw" not in text.lower()


def test_bulk_delete_confirmation_explains_source_specific_effects(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("CUSTOM_FORM,missing.html\nL01,MR08", encoding="utf-8")
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)
    spotter = SpotterMessage(
        spotter_id=44,
        from_call="K7ETC",
        to_call="@MR08",
        msg_type="F!307",
        utc_str="2026-08-11 12:00:00",
        utc_ts=1.0,
        raw_text="F!307 TO[@MR08] FR[K7ETC] NA[Wildfire]",
        decoded_text="",
        state="UNREAD",
    )
    text = MessageViewerTab._bulk_delete_confirmation_text(
        [
            UnifiedMessage("FLMSG", "NEW", "K7ETC", "MR08", 1.0, "", "Widemouth 2 Fire", "flmsg", rec),
            UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 2.0, "", "Wildfire", "spotter", spotter),
        ]
    )

    assert "Selected: 2 message" in text
    assert "FLMSG: 1" in text
    assert "JS8Spotter: 1" in text
    assert "Move files to Recycle Bin" in text
    assert "Delete from FIO message store" in text
    assert "Widemouth 2 Fire" in text
    assert "Wildfire" in text
    assert "raw" not in text.lower()


def test_delete_policy_explains_commstat_source_backed_fallback() -> None:
    msg = _commstat_artifact()
    cap = message_delete_capability(msg, origin="commstat", msg_type="CommStat")

    assert cap.source_label == "CommStat"
    assert cap.effect_label == "Delete source row when safe; otherwise hide from FIO Messages"
    assert cap.audit_action == "CommStat source delete or FIO hide"


def test_delete_policy_centralizes_commstat_result_details() -> None:
    msg = _commstat_artifact()

    assert message_delete_result_detail(msg, "deleted_source") == "CommStat source row deleted and FIO projection removed"
    assert (
        message_delete_result_detail(msg, "deleted_projection")
        == "CommStat source row was already absent; stale FIO projection removed"
    )
    assert (
        message_delete_result_detail(msg, "hidden")
        == "CommStat artifact hidden from FIO Messages; source row was not safely identifiable"
    )
    assert message_delete_result_detail(msg, "skipped") == "CommStat artifact has no stable identity"


def test_delete_policy_centralizes_execution_result_decisions(tmp_path) -> None:
    msg_path = tmp_path / "message.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(
        path=msg_path,
        origin="flmsg",
        size=stat.st_size,
        mtime=stat.st_mtime,
        source_id="source-flmsg-a",
        source_label="FIO-A FLMSG",
    )

    success = delete_success_result("hidden", hidden=True)
    missing = missing_identity_delete_result()
    failed = failed_source_delete_result(rec)
    hidden = commstat_delete_execution_result("hidden")
    skipped = commstat_delete_execution_result("skipped")
    source_failed = commstat_delete_execution_result("failed")

    assert success == MessageDeleteExecutionResult("deleted", "hidden", deleted_row=True, hidden=True)
    assert missing == MessageDeleteExecutionResult("skipped", "skipped")
    assert failed.result == "failed"
    assert failed.warning == "Failed to move file to the Recycle Bin."
    assert hidden == MessageDeleteExecutionResult("deleted", "hidden", deleted_row=True, hidden=True)
    assert skipped.result == "skipped"
    assert "stable message identity" in skipped.warning
    assert source_failed.result == "failed"
    assert "message database was busy" in source_failed.warning
    assert single_delete_failure_warning(rec, source_failed, fallback="Fallback") == source_failed.warning
    assert single_delete_failure_warning(rec, MessageDeleteExecutionResult("failed", "failed"), fallback="Fallback") == "Fallback"


def test_delete_policy_centralizes_file_result_details(tmp_path) -> None:
    msg_path = tmp_path / "message.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(
        path=msg_path,
        origin="flmsg",
        size=stat.st_size,
        mtime=stat.st_mtime,
        source_id="source-flmsg-a",
        source_label="FIO-A FLMSG",
    )

    assert message_delete_result_detail(rec, "deleted") == "file moved to Recycle Bin"
    assert message_delete_result_detail(rec, "skipped") == "file no longer exists"
    assert message_delete_result_detail(rec, "failed") == "file not moved to Recycle Bin"


def test_message_source_delete_core_removes_fio_owned_rows(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE spotter_traffic (id INTEGER PRIMARY KEY, state TEXT)")
    conn.execute("INSERT INTO spotter_traffic (id, state) VALUES (7, 'NEW')")
    conn.execute("CREATE TABLE sitrep_events (id INTEGER PRIMARY KEY, report_key TEXT)")
    conn.execute("INSERT INTO sitrep_events (id, report_key) VALUES (9, 'rpt-9')")
    conn.execute("CREATE TABLE js8_messages (id INTEGER PRIMARY KEY, state TEXT)")
    conn.execute("CREATE TABLE js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT)")
    conn.execute("INSERT INTO js8_messages (id, state) VALUES (11, 'NEW')")
    conn.execute("INSERT INTO js8_inbox_state (id, state) VALUES (11, 'NEW')")
    conn.execute("CREATE TABLE varac_messages (source TEXT, id INTEGER, state TEXT)")
    conn.execute("INSERT INTO varac_messages (source, id, state) VALUES ('FIO-A', 13, 'NEW')")
    conn.commit()
    conn.close()

    sitrep = type("SitrepMessage", (), {"event_id": 9, "report_key": ""})()

    assert delete_spotter_store_row(db_path, 7) is True
    assert delete_sitrep_store_row(db_path, sitrep) is True
    delete_js8_local_rows(db_path, 11)
    delete_varac_local_projection(db_path, source="FIO-A", msg_id=13)

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM spotter_traffic").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM sitrep_events").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM js8_messages").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM js8_inbox_state").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM varac_messages").fetchone()[0] == 0
    conn.close()


def test_message_viewer_js8_read_state_is_source_scoped(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    source_key = "js8:fio-a"
    native_id = 1
    local_id = MessageIngestor._js8_local_row_id(native_id, source_key)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE js8_messages (
            id INTEGER PRIMARY KEY,
            state TEXT,
            read_ts REAL,
            source_key TEXT,
            source_id INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO js8_messages (id, state, read_ts, source_key, source_id) VALUES (?, 'NEW', 0.0, ?, ?)",
        (local_id, source_key, native_id),
    )
    conn.commit()
    conn.close()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._local_js8_db = lambda: db_path

    MessageViewerTab._persist_js8_read(
        tab,
        local_id,
        123.0,
        456.0,
        False,
        source_id=native_id,
        source_key=source_key,
    )

    conn = sqlite3.connect(db_path)
    try:
        state_row = conn.execute(
            "SELECT id, source_key, source_id, state, read_ts FROM js8_inbox_state"
        ).fetchone()
        message_row = conn.execute("SELECT state, read_ts FROM js8_messages WHERE id=?", (local_id,)).fetchone()
    finally:
        conn.close()

    assert state_row == (MessageViewerTab._js8_state_row_id(native_id, source_key), source_key, native_id, "READ", 456.0)
    assert message_row == ("READ", 456.0)


def test_message_viewer_bulk_js8_read_state_is_source_scoped(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_a = JS8Message(
        msg_id=MessageIngestor._js8_local_row_id(1, "js8:fio-a"),
        from_call="K1AAA",
        to_call="@MAGNET",
        msg_type="MSG",
        utc_str="2026-08-08 12:34:56",
        utc_ts=111.0,
        raw_text="A",
        decoded_text="A",
        state="NEW",
        source_key="js8:fio-a",
        source_id=1,
    )
    msg_b = JS8Message(
        msg_id=MessageIngestor._js8_local_row_id(1, "js8:fio-b"),
        from_call="K1BBB",
        to_call="@MAGNET",
        msg_type="MSG",
        utc_str="2026-08-08 12:35:56",
        utc_ts=222.0,
        raw_text="B",
        decoded_text="B",
        state="NEW",
        source_key="js8:fio-b",
        source_id=1,
    )
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE js8_messages (
            id INTEGER PRIMARY KEY,
            state TEXT,
            read_ts REAL,
            source_key TEXT,
            source_id INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO js8_messages (id, state, read_ts, source_key, source_id) VALUES (?, 'NEW', 0.0, ?, ?)",
        [
            (msg_a.msg_id, msg_a.source_key, msg_a.source_id),
            (msg_b.msg_id, msg_b.source_key, msg_b.source_id),
        ],
    )
    conn.commit()
    conn.close()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._local_js8_db = lambda: db_path
    tab.settings = {"js8_inbox_mark_retrieved_sync": False}

    MessageViewerTab._mark_js8_rows_read_bulk(tab, [msg_a, msg_b], 789.0)

    conn = sqlite3.connect(db_path)
    try:
        state_rows = conn.execute(
            "SELECT source_key, source_id, state, read_ts FROM js8_inbox_state ORDER BY source_key"
        ).fetchall()
        message_rows = conn.execute("SELECT source_key, source_id, state, read_ts FROM js8_messages ORDER BY source_key").fetchall()
    finally:
        conn.close()

    assert state_rows == [
        ("js8:fio-a", 1, "READ", 789.0),
        ("js8:fio-b", 1, "READ", 789.0),
    ]
    assert message_rows == [
        ("js8:fio-a", 1, "READ", 789.0),
        ("js8:fio-b", 1, "READ", 789.0),
    ]
    assert msg_a.state == "READ"
    assert msg_b.state == "READ"


def test_message_viewer_pending_js8_query_uses_source_endpoint(monkeypatch) -> None:
    import freqinout.gui.message_viewer_tab as viewer_module

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {"js8_host": "127.0.0.1", "js8_port": 2442}

    class FakeStore:
        def list_profiles(self, enabled_only=False):
            return [
                {"id": 7, "name": "FIO-A", "js8_instance_id": "fio-a", "js8_host": "127.0.0.1", "js8_port": 2442},
                {"id": 8, "name": "FIO-B", "js8_instance_id": "fio-b", "js8_host": "127.0.0.1", "js8_port": 2444},
            ]

    monkeypatch.setattr(viewer_module, "MultiRadioStore", lambda: FakeStore())

    endpoint, label = MessageViewerTab._pending_js8_endpoint(
        tab,
        {"source_radio_id": "8", "js8_instance_id": "fio-b", "source_key": "js8:fio-b"},
    )

    assert endpoint.host == "127.0.0.1"
    assert endpoint.port == 2444
    assert label == "FIO-B"


def test_message_viewer_pending_js8_query_prefers_runtime_source_context(monkeypatch) -> None:
    import freqinout.gui.message_viewer_tab as viewer_module

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {"js8_host": "127.0.0.1", "js8_port": 2442}

    def fail_store():
        raise AssertionError("profile fallback should not be used when source context resolves")

    monkeypatch.setattr(viewer_module, "MultiRadioStore", fail_store)
    monkeypatch.setattr(
        viewer_module,
        "resolve_js8_endpoint_context",
        lambda settings, source_context=None: {
            "host": "127.0.0.1",
            "port": "2448",
            "label": "FIO-C JS8Call",
        },
    )

    endpoint, label = MessageViewerTab._pending_js8_endpoint(
        tab,
        {"source_key": "app_js8call_fio_c", "source_radio_id": "9", "js8_instance_id": "fio-c"},
    )

    assert endpoint.host == "127.0.0.1"
    assert endpoint.port == 2448
    assert label == "FIO-C JS8Call"


def test_message_viewer_pending_js8_send_uses_source_endpoint(monkeypatch) -> None:
    import freqinout.gui.message_viewer_tab as viewer_module

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {"js8_host": "127.0.0.1", "js8_port": 2442}
    captured = {}

    class FakeStore:
        def list_profiles(self, enabled_only=False):
            return [
                {"id": 7, "name": "FIO-A", "js8_instance_id": "fio-a", "js8_host": "127.0.0.1", "js8_port": 2442},
                {"id": 8, "name": "FIO-B", "js8_instance_id": "FIO-B", "js8_host": "127.0.0.1", "js8_port": 2444},
            ]

    class FakeClient:
        pass

    def fake_get(endpoint, **kwargs):
        captured["endpoint"] = endpoint
        captured["kwargs"] = kwargs
        return FakeClient()

    def fake_send(client, text, **kwargs):
        captured["text"] = text
        captured["send_kwargs"] = kwargs
        return SimpleNamespace(sent=True, detail="ok")

    monkeypatch.setattr(viewer_module, "MultiRadioStore", lambda: FakeStore())
    monkeypatch.setattr(viewer_module.JS8ApiClientRegistry, "get", staticmethod(fake_get))
    monkeypatch.setattr(viewer_module, "send_js8_message_guarded", fake_send)

    ok = MessageViewerTab._send_js8_message(
        tab,
        "N1MAG: K1BBB QUERY MSG 42",
        source_context={"source_radio_id": "8", "js8_instance_id": "fio-b", "source_key": "js8:fio-b"},
    )

    assert ok is True
    assert captured["endpoint"].host == "127.0.0.1"
    assert captured["endpoint"].port == 2444
    assert captured["text"] == "N1MAG: K1BBB QUERY MSG 42"


def test_message_viewer_pending_status_and_delete_are_source_scoped(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab._backlog_db_path = lambda: db_path

    MessageViewerTab._ensure_backlog_table(tab)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        """
        INSERT INTO autoquery_backlog
            (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts,
             source_key, source_radio_id, js8_instance_id, source_path)
        VALUES ('K1ABC', '42', 'MSG', 'PENDING', 0, 1.0, 1.0, ?, ?, ?, ?)
        """,
        [
            ("js8:fio-a", "7", "fio-a", "/tmp/fio-a/inbox.db"),
            ("js8:fio-b", "8", "fio-b", "/tmp/fio-b/inbox.db"),
        ],
    )
    conn.commit()
    conn.close()

    MessageViewerTab._pending_set_status(tab, "K1ABC", "42", "WAITING", "js8:fio-b")
    MessageViewerTab._pending_delete(tab, "K1ABC", "42", "js8:fio-a")

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT source_key, status FROM autoquery_backlog ORDER BY source_key"
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("js8:fio-b", "WAITING")]


def test_message_viewer_pending_mark_retrieved_sync_uses_source_inbox(tmp_path) -> None:
    inbox_a = tmp_path / "fio-a-inbox.db"
    inbox_b = tmp_path / "fio-b-inbox.db"
    for path, from_call, row_id in ((inbox_a, "K1AAA", 42), (inbox_b, "K1ABC", 42)):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, blob TEXT, type TEXT)")
        conn.execute(
            "INSERT INTO inbox_v1 (id, blob, type) VALUES (?, ?, 'UNREAD')",
                (
                    row_id,
                    json.dumps(
                        {"type": "UNREAD", "params": {"FROM": from_call, "_ID": "42", "TEXT": "payload"}},
                        separators=(",", ":"),
                    ),
                ),
            )
        conn.commit()
        conn.close()

    db_path = tmp_path / "freqinout_nets.db"
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {"js8_inbox_mark_retrieved_sync": True}
    tab._backlog_db_path = lambda: db_path
    tab._update_pending_table = lambda: None

    MessageViewerTab._ensure_backlog_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO autoquery_backlog
            (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts,
             source_key, source_radio_id, js8_instance_id, source_path)
        VALUES ('K1ABC', '42', 'MSG', 'PENDING', 0, 1.0, 1.0, 'js8:fio-b', '8', 'fio-b', ?)
        """,
        (str(inbox_b),),
    )
    conn.commit()
    conn.close()

    MessageViewerTab._on_pending_mark_retrieved_row(
        tab,
        {
            "callsign": "K1ABC",
            "msg_id": "42",
            "source_key": "js8:fio-b",
            "source_path": str(inbox_b),
        },
    )

    conn_a = sqlite3.connect(inbox_a)
    conn_b = sqlite3.connect(inbox_b)
    try:
        row_a = conn_a.execute("SELECT blob, type FROM inbox_v1 WHERE id=42").fetchone()
        row_b = conn_b.execute("SELECT blob, type FROM inbox_v1 WHERE id=42").fetchone()
    finally:
        conn_a.close()
        conn_b.close()

    assert row_a[1] == "UNREAD"
    assert json.loads(row_a[0])["type"] == "UNREAD"
    assert row_b[1] == "READ"
    assert json.loads(row_b[0])["type"] == "READ"


def test_message_viewer_varac_read_and_flag_are_source_scoped(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE varac_messages (
            ingest_source_key TEXT,
            source TEXT,
            id INTEGER,
            read_status INTEGER,
            flag_state INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO varac_messages (ingest_source_key, source, id, read_status, flag_state) VALUES ('varac-a', 'vmail', 1, 0, 0)"
    )
    conn.execute(
        "INSERT INTO varac_messages (ingest_source_key, source, id, read_status, flag_state) VALUES ('varac-b', 'vmail', 1, 0, 0)"
    )
    conn.commit()
    conn.close()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    msg = VarACMessage(
        msg_id=1,
        guid="guid-a",
        source="vmail",
        msg_type="VMAIL",
        from_call="K1AAA",
        to_call="K2BBB",
        subject="Scoped",
        body="",
        ts=1.0,
        band="20M",
        freq_hz=None,
        snr=None,
        read_status=0,
        folder="Inbox",
        vmail_guid="guid-a",
        source_key="varac-a",
    )

    MessageViewerTab._set_varac_flag(tab, msg, 1)
    MessageViewerTab._persist_varac_read(tab, "vmail", 1, "varac-a")

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT ingest_source_key, read_status, flag_state FROM varac_messages ORDER BY ingest_source_key"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("varac-a", 1, 1), ("varac-b", 0, 0)]


def test_message_viewer_resolves_varac_source_db_from_sync_status(tmp_path) -> None:
    local_db = tmp_path / "freqinout_nets.db"
    source_db = tmp_path / "VarAC-A.db"
    fallback_db = tmp_path / "VarAC-Fallback.db"
    source_db.write_text("", encoding="utf-8")
    fallback_db.write_text("", encoding="utf-8")
    conn = sqlite3.connect(local_db)
    try:
        conn.execute(
            """
            CREATE TABLE varac_sync_status (
                run_started_ts REAL,
                run_finished_ts REAL,
                varac_db_path TEXT,
                ingest_source_key TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO varac_sync_status (run_started_ts, run_finished_ts, varac_db_path, ingest_source_key) VALUES (1, 2, ?, 'varac-a')",
            (str(source_db),),
        )
        conn.commit()
    finally:
        conn.close()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: local_db
    tab.settings = SimpleNamespace(get=lambda key, default=None: str(fallback_db) if key == "varac_db_path" else default)
    msg = _varac_message(source="vmail", source_key="varac-a")

    resolved = MessageViewerTab._resolve_varac_db_path_for_message(tab, msg)

    assert resolved == source_db


def test_message_viewer_bulk_varac_read_falls_back_for_legacy_table(tmp_path) -> None:
    local_db = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(local_db)
    try:
        conn.execute("CREATE TABLE varac_messages (source TEXT, id INTEGER, read_status INTEGER)")
        conn.execute("INSERT INTO varac_messages (source, id, read_status) VALUES ('vmail', 1, 0)")
        conn.commit()
    finally:
        conn.close()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: local_db
    msg = _varac_message(source="vmail", msg_id=1, source_key="legacy")

    MessageViewerTab._mark_varac_rows_read_bulk(tab, [msg])

    conn = sqlite3.connect(local_db)
    try:
        assert conn.execute("SELECT read_status FROM varac_messages WHERE source='vmail' AND id=1").fetchone()[0] == 1
    finally:
        conn.close()


def test_message_source_delete_core_removes_js8call_inbox_variants(tmp_path) -> None:
    inbox_v1 = tmp_path / "inbox_v1.db"
    conn = sqlite3.connect(inbox_v1)
    conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, text TEXT)")
    conn.execute("INSERT INTO inbox_v1 (id, text) VALUES (21, 'message')")
    conn.commit()
    conn.close()

    legacy = tmp_path / "inbox_legacy.db"
    conn = sqlite3.connect(legacy)
    conn.execute("CREATE TABLE inbox (text TEXT)")
    conn.execute("INSERT INTO inbox (rowid, text) VALUES (31, 'message')")
    conn.commit()
    conn.close()

    assert delete_js8_inbox_row(inbox_v1, 21) is True
    assert delete_js8_inbox_row(legacy, 31) is True

    conn = sqlite3.connect(inbox_v1)
    assert conn.execute("SELECT COUNT(*) FROM inbox_v1").fetchone()[0] == 0
    conn.close()
    conn = sqlite3.connect(legacy)
    assert conn.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0
    conn.close()


def test_message_source_delete_core_soft_deletes_varac_source_rows(tmp_path) -> None:
    varac_db = tmp_path / "VarAC.db"
    conn = sqlite3.connect(varac_db)
    conn.execute("CREATE TABLE vmail (id INTEGER PRIMARY KEY, guid TEXT, is_deleted INTEGER DEFAULT 0)")
    conn.execute("CREATE TABLE vmail_attachment (vmail_guid TEXT, is_deleted INTEGER DEFAULT 0)")
    conn.execute("INSERT INTO vmail (id, guid, is_deleted) VALUES (42, 'guid-42', 0)")
    conn.execute("INSERT INTO vmail_attachment (vmail_guid, is_deleted) VALUES ('guid-42', 0)")
    conn.execute("CREATE TABLE qso (id INTEGER PRIMARY KEY, is_deleted INTEGER DEFAULT 0)")
    conn.execute("INSERT INTO qso (id, is_deleted) VALUES (43, 0)")
    conn.commit()
    conn.close()

    assert soft_delete_varac_source_row(varac_db, source_table="vmail", msg_id=42, vmail_guid="guid-42") is True
    assert soft_delete_varac_source_row(varac_db, source_table="qso", msg_id=43) is True
    assert soft_delete_varac_source_row(varac_db, source_table="not_allowed", msg_id=44) is False

    conn = sqlite3.connect(varac_db)
    assert conn.execute("SELECT is_deleted FROM vmail WHERE id=42").fetchone()[0] == 1
    assert conn.execute("SELECT is_deleted FROM vmail_attachment WHERE vmail_guid='guid-42'").fetchone()[0] == 1
    assert conn.execute("SELECT is_deleted FROM qso WHERE id=43").fetchone()[0] == 1
    conn.close()


def test_sitrep_message_key_core_supports_id_and_report_key() -> None:
    by_id = type("SitrepMessage", (), {"event_id": 12, "report_key": "ignored"})()
    by_report = type("SitrepMessage", (), {"event_id": 0, "report_key": "RPT-12"})()

    assert sitrep_message_key(by_id) == ("sitrep", 12)
    assert sitrep_message_key(by_report) == ("sitrep", "rpt-12")
    assert sitrep_message_key(object()) is None


def test_bulk_delete_confirmation_uses_commstat_source_backed_effect() -> None:
    msg = _commstat_artifact(title="CommStat | Water update")
    text = MessageViewerTab._bulk_delete_confirmation_text(
        [
            UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 2.0, "", "Water update", "commstat", msg),
        ]
    )

    assert "CommStat: 1" in text
    assert "Delete source row when safe; otherwise hide from FIO Messages" in text
    assert "Water update" in text


def test_bulk_delete_completion_summarizes_source_hidden_and_stale_results() -> None:
    text = MessageViewerTab._bulk_delete_completion_text(
        deleted=4,
        skipped=1,
        failed=2,
        source_summary="CommStat: 4",
        detail_counts={"deleted_source": 2, "deleted_projection": 1, "hidden": 1},
    )

    assert "Completed delete for 4 message" in text
    assert "CommStat: 4" in text
    assert "Deleted from source: 2" in text
    assert "Cleaned stale FIO rows: 1" in text
    assert "Hidden in FIO: 1" in text
    assert "Skipped: 1" in text
    assert "Failed: 2" in text


def test_single_delete_confirmation_uses_shared_policy_for_file(tmp_path) -> None:
    msg_path = tmp_path / "message.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    row = UnifiedMessage("FLMSG", "NEW", "K7ETC", "MR08", 1.0, "", "Widemouth 2 Fire", "flmsg", rec)

    text = MessageViewerTab._single_delete_confirmation_text(row, "Delete this file-backed message?")

    assert text.startswith("Delete this file-backed message?")
    assert "Source: FLMSG" in text
    assert "Delete action: Move files to Recycle Bin" in text
    assert "K7ETC -> MR08" in text
    assert "Widemouth 2 Fire" in text


def test_single_delete_confirmation_uses_shared_policy_for_commstat() -> None:
    msg = _commstat_artifact(title="CommStat | Water update")
    row = UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 2.0, "", "Water update", "commstat", msg)

    text = MessageViewerTab._single_delete_confirmation_text(row, "Delete CommStat item?")

    assert "Source: CommStat" in text
    assert "Delete action: Delete source row when safe; otherwise hide from FIO Messages" in text
    assert "Water update" in text


def test_collect_deletable_rows_uses_policy_not_only_row_key(tmp_path) -> None:
    msg_path = tmp_path / "message.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    supported = UnifiedMessage("FLMSG", "NEW", "", "", 1.0, "", "File", "flmsg", rec)
    unsupported = UnifiedMessage("Mystery", "NEW", "", "", 1.0, "", "Mystery", "mystery", object())

    rows = MessageViewerTab._collect_deletable_rows([supported, unsupported])

    assert rows == [supported]
    assert collect_deletable_message_rows([supported, unsupported]) == [supported]


def test_select_visible_requires_scoped_filter_before_bulk_selection() -> None:
    selected: list[UnifiedMessage] = []
    spotter = SpotterMessage(
        spotter_id=45,
        from_call="K7ETC",
        to_call="@MR08",
        msg_type="F!307",
        utc_str="2026-08-11 12:00:00",
        utc_ts=2.0,
        raw_text="F!307 TO[@MR08] FR[K7ETC] NA[Wildfire]",
        decoded_text="",
        state="UNREAD",
    )
    row = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 2.0, "", "Wildfire", "spotter", spotter)

    class Model:
        def rows(self):
            return [row]

        def set_selected_for_rows(self, rows, value):
            if value:
                selected.extend(rows)

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._messages_model = Model()
    tab._is_filter_active = lambda: True
    tab._update_bulk_delete_buttons = lambda: None

    MessageViewerTab._select_visible_messages(tab)

    assert selected == [row]


def test_bulk_selection_bar_summarizes_selected_rows(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("CUSTOM_FORM,missing.html\nL01,MR08", encoding="utf-8")
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)
    row = UnifiedMessage("FLMSG", "NEW", "K7ETC", "MR08", 1.0, "", "Widemouth 2 Fire", "flmsg", rec)
    model = MessageTableModel([row])
    model.set_selected_for_rows([row], True)

    class Settings:
        def get(self, _key, default=None):
            return default

    class Widget:
        def __init__(self):
            self.visible = False
            self.enabled = False
            self.text = ""
            self.tooltip = ""
            self.style = ""

        def setVisible(self, value):
            self.visible = bool(value)

        def setEnabled(self, value):
            self.enabled = bool(value)

        def setText(self, value):
            self.text = value

        def setToolTip(self, value):
            self.tooltip = value

        def setStyleSheet(self, value):
            self.style = value

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = Settings()
    tab._messages_model = model
    tab.delete_selected_btn = Widget()
    tab.export_selected_btn = Widget()
    tab.bulk_selection_bar = Widget()
    tab.bulk_selection_label = Widget()
    tab.bulk_mark_read_btn = Widget()
    tab.bulk_delete_btn = Widget()
    tab.bulk_clear_btn = Widget()
    tab._refresh_more_actions_menu = lambda: None
    tab._sync_select_all_checkbox = lambda: None
    tab._update_mark_all_read_style = lambda: None

    MessageViewerTab._update_bulk_delete_buttons(tab)

    assert tab.bulk_selection_bar.visible is True
    assert tab.delete_selected_btn.visible is False
    assert tab.delete_selected_btn.enabled is False
    assert tab.bulk_selection_label.text == "1 selected | FLMSG: 1"
    assert "Move files to Recycle Bin" in tab.bulk_selection_label.tooltip
    assert "FLMSG: 1" in tab.bulk_delete_btn.tooltip
    assert tab.bulk_mark_read_btn.enabled is True
    assert tab.bulk_mark_read_btn.text == "Mark Read (1)"
    assert tab.bulk_delete_btn.enabled is True
    assert tab.bulk_clear_btn.enabled is True


def test_header_select_all_only_selects_deletable_filtered_rows(tmp_path) -> None:
    msg_path = tmp_path / "message.k2s"
    msg_path.write_text("message", encoding="utf-8")
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)
    supported = UnifiedMessage("FLMSG", "NEW", "", "", 1.0, "", "File", "flmsg", rec)
    unsupported = UnifiedMessage("Mystery", "NEW", "", "", 1.0, "", "Mystery", "mystery", object())

    class Model:
        def __init__(self):
            self.calls = []

        def rows(self):
            return [supported, unsupported]

        def set_selected_for_rows(self, rows, selected):
            self.calls.append((list(rows), bool(selected)))

    model = Model()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._messages_model = model
    tab._is_filter_active = lambda: True
    tab._update_bulk_delete_buttons = lambda: None

    MessageViewerTab._on_header_checkbox_toggled(tab, Qt.Checked.value)

    assert model.calls == [([supported], True)]


def test_header_uncheck_clears_all_visible_rows(tmp_path) -> None:
    msg_path = tmp_path / "message.k2s"
    msg_path.write_text("message", encoding="utf-8")
    rec = FileRecord(path=msg_path, origin="flmsg", size=msg_path.stat().st_size, mtime=msg_path.stat().st_mtime)
    supported = UnifiedMessage("FLMSG", "NEW", "", "", 1.0, "", "File", "flmsg", rec)
    unsupported = UnifiedMessage("Mystery", "NEW", "", "", 1.0, "", "Mystery", "mystery", object())

    class Model:
        def __init__(self):
            self.calls = []

        def rows(self):
            return [supported, unsupported]

        def set_selected_for_rows(self, rows, selected):
            self.calls.append((list(rows), bool(selected)))

    model = Model()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._messages_model = model
    tab._is_filter_active = lambda: True
    tab._update_bulk_delete_buttons = lambda: None

    MessageViewerTab._on_header_checkbox_toggled(tab, Qt.Unchecked.value)

    assert model.calls == [([supported, unsupported], False)]


def test_select_visible_reports_active_scope_to_operator() -> None:
    selected: list[UnifiedMessage] = []
    spotter = SpotterMessage(
        spotter_id=45,
        from_call="K7ETC",
        to_call="@MR08",
        msg_type="F!307",
        utc_str="2026-08-11 12:00:00",
        utc_ts=2.0,
        raw_text="F!307 TO[@MR08] FR[K7ETC] NA[Wildfire]",
        decoded_text="",
        state="UNREAD",
    )
    row = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 2.0, "", "Wildfire", "spotter", spotter)

    class Model:
        def rows(self):
            return [row]

        def set_selected_for_rows(self, rows, value):
            if value:
                selected.extend(rows)

    class Label:
        text = ""

        def setText(self, value):
            self.text = value

    class Combo:
        def __init__(self, text="", data=0):
            self._text = text
            self._data = data

        def currentText(self):
            return self._text

        def currentData(self):
            return self._data

        def text(self):
            return self._text

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._messages_model = Model()
    tab._is_filter_active = lambda: True
    tab._update_bulk_delete_buttons = lambda: None
    tab._inbox_focus = "spotter"
    tab._message_group_filter_active = lambda: True
    tab._selected_message_groups = lambda: {"MR08"}
    tab._selected_message_sources = lambda: {"spotter"}
    tab.received_filter = Combo("Older than 2 weeks", -14 * 24 * 60 * 60)
    tab.rcv_search = Combo("wildfire", 0)
    tab.type_filter = Combo("MSG Type...", 0)
    tab.status_filter = Combo("Status...", 0)
    tab.from_filter = Combo("", 0)
    tab.to_filter = Combo("", 0)
    tab.message_check_status_label = Label()

    MessageViewerTab._select_visible_messages(tab)

    assert selected == [row]
    assert "Selected 1 visible message" in tab.message_check_status_label.text
    assert "Focus Spotter" in tab.message_check_status_label.text
    assert "Groups MR08" in tab.message_check_status_label.text
    assert "Older than 2 weeks" in tab.message_check_status_label.text
    assert 'Search "wildfire"' in tab.message_check_status_label.text


def test_cleanup_age_window_sets_age_filter_then_selects_visible() -> None:
    calls: list[str] = []

    class Combo:
        def __init__(self):
            self.current = None

        def findData(self, value):
            return 3 if value == -14 * 24 * 60 * 60 else -1

        def setCurrentIndex(self, index):
            self.current = index

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.received_filter = Combo()
    tab._unfreeze_table = lambda: calls.append("unfreeze")
    tab._apply_message_filters = lambda: calls.append("apply")
    tab._select_visible_messages = lambda: calls.append("select")

    MessageViewerTab._select_cleanup_age_window(tab, -14 * 24 * 60 * 60)

    assert tab.received_filter.current == 3
    assert calls == ["unfreeze", "apply", "select"]


def test_mark_selected_read_updates_only_selected_unread_rows() -> None:
    unread = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 2.0, "", "Wildfire", "spotter", object())
    read = UnifiedMessage("FLMSG", "READ", "N1MAG", "MAGNET", 1.0, "", "OpNet", "flmsg", object())
    cleared = []
    refreshed = []

    class Model:
        def selected_rows(self):
            return [unread, read]

        def clear_selection(self):
            cleared.append(True)

    class Label:
        text = ""

        def setText(self, value):
            self.text = value

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._messages_model = Model()
    tab.message_check_status_label = Label()
    tab._mark_rows_read_bulk = lambda rows: len(rows)
    tab._apply_message_filters_preserve_scroll = lambda: refreshed.append(True)

    MessageViewerTab._mark_selected_read(tab)

    assert cleared == [True]
    assert refreshed == [True]
    assert "Marked 1 selected" in tab.message_check_status_label.text


def test_delete_audit_records_safe_message_summary(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    spotter = SpotterMessage(
        spotter_id=45,
        from_call="K7ETC",
        to_call="@MR08",
        msg_type="F!307",
        utc_str="2026-08-11 12:00:00",
        utc_ts=2.0,
        raw_text="F!307 TO[@MR08] FR[K7ETC] NA[Wildfire] BODY[very long raw payload should not be stored]",
        decoded_text="Decoded text should not be stored in audit",
        state="UNREAD",
    )
    row = UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 2.0, "", "Wildfire", "spotter", spotter)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path

    MessageViewerTab._record_message_delete_audit(
        tab,
        row,
        result="deleted",
        detail="Spotter row deleted from FIO store",
        batch_id="test-batch",
    )

    conn = sqlite3.connect(db_path)
    stored = conn.execute(
        "SELECT batch_id, source, action, result, row_key, from_call, to_call, title, detail FROM message_delete_audit"
    ).fetchone()
    conn.close()

    assert stored[0] == "test-batch"
    assert stored[1] == "JS8Spotter"
    assert stored[2] == "Delete from FIO message store"
    assert stored[3] == "deleted"
    assert '"spotter",45' in stored[4]
    assert stored[5] == "K7ETC"
    assert stored[6] == "MR08"
    assert stored[7] == "Wildfire"
    assert "Spotter row deleted" in stored[8]
    assert "very long raw payload" not in " ".join(str(part) for part in stored)


def test_message_delete_policy_composes_bulk_and_single_operator_text() -> None:
    spotter = SpotterMessage(
        spotter_id=45,
        from_call="K7ETC",
        to_call="@MR08",
        msg_type="F!307",
        utc_str="2026-08-11 12:00:00",
        utc_ts=2.0,
        raw_text="F!307 TO[@MR08] FR[K7ETC] NA[Wildfire]",
        decoded_text="Decoded text should not be stored in audit",
        state="UNREAD",
    )
    varac = VarACMessage(
        msg_id=12,
        guid="abc",
        source="varac-a",
        msg_type="VMAIL",
        from_call="N1MAG",
        to_call="@MAGNET",
        subject="Supply note",
        body="",
        ts=3.0,
        band="40M",
        freq_hz=None,
        snr=None,
        read_status=0,
        folder="Inbox",
        vmail_guid="",
    )
    rows = [
        UnifiedMessage("F!307", "NEW", "K7ETC", "MR08", 2.0, "", "Wildfire", "spotter", spotter, topics=("Fire",)),
        UnifiedMessage("VarAC", "NEW", "N1MAG", "MAGNET", 3.0, "", "Supply note", "varac", varac),
    ]

    assert summarize_delete_sources(rows) == "JS8Spotter: 1, VarAC: 1"
    assert delete_audit_action_for_row(rows[0]) == "Delete from FIO message store"
    assert delete_audit_action_for_row(rows[1]) == "Mark deleted in VarAC"
    effects = summarize_delete_effects(rows)
    assert "Delete from FIO message store: 1" in effects
    assert "Mark deleted in VarAC: 1" in effects
    assert "Sources: JS8Spotter: 1, VarAC: 1" in delete_effect_tooltip(rows)
    assert "Topics: Fire" in core_message_row_summary_line(rows[0])

    bulk_text = bulk_delete_confirmation_text(rows, "Delete selected messages?")
    assert "Selected: 2 message(s)" in bulk_text
    assert "First selected messages:" in bulk_text
    assert "Delete action:" in bulk_text
    assert "Decoded text should not be stored" not in bulk_text

    single_text = single_delete_confirmation_text(rows[0])
    assert "Source: JS8Spotter" in single_text
    assert "Delete action: Delete from FIO message store" in single_text

    completion = bulk_delete_completion_text(
        deleted=2,
        skipped=1,
        failed=0,
        source_summary="JS8Spotter: 1, VarAC: 1",
        detail_counts={"deleted": 1, "hidden": 1},
    )
    assert "Completed delete for 2 message(s)." in completion
    assert "Hidden in FIO: 1" in completion
    assert "Skipped: 1" in completion


def test_message_delete_audit_core_stores_bounded_safe_rows(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    record_message_delete_audit(
        db_path,
        batch_id="batch-1",
        source="JS8Spotter",
        action="Delete from FIO message store",
        result="DELETED",
        row_key="spotter:45",
        from_call="K7ETC",
        to_call="@MR08",
        title="Wildfire update",
        detail="x" * 400,
        audit_ts=10.0,
    )
    record_message_delete_audit(
        db_path,
        batch_id="batch-2",
        source="FLMSG",
        action="Move files to Recycle Bin",
        result="failed",
        row_key="file:one",
        from_call="N1MAG",
        to_call="MAGNET",
        title="Older",
        detail="ok",
        audit_ts=20.0,
    )

    rows = load_message_delete_audit_rows(db_path, limit=1)

    assert len(rows) == 1
    assert rows[0]["batch_id"] == "batch-2"
    assert rows[0]["result"] == "failed"
    assert rows[0]["detail"] == "ok"

    all_rows = load_message_delete_audit_rows(db_path, limit=10)
    assert all_rows[1]["result"] == "deleted"
    assert all_rows[1]["detail"].endswith("...")
    assert len(all_rows[1]["detail"]) == 240


def test_message_delete_audit_core_creates_table_without_gui(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    ensure_message_delete_audit_table(conn)
    conn.commit()
    stored = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='message_delete_audit'"
    ).fetchone()
    conn.close()

    assert stored == ("message_delete_audit",)
    assert safe_audit_text("  a   b  ") == "a b"


def test_refresh_js8_messages_loads_cached_projection_before_background_request(monkeypatch) -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    calls: list[str] = []
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._last_js8_ingest_ts = 0.0
    tab._js8_ingest_interval_sec = 30.0
    tab._js8_display_snapshot_fp = None
    tab._message_rows = []
    tab.settings = {}
    tab._js8_display_fingerprint = lambda: (("db", len(calls)),)
    tab._load_structured_message_projections = lambda **_kwargs: calls.append("load")
    tab._request_background_ingest = lambda *_args, **_kwargs: calls.append("background") or True
    tab._populate_messages_table = lambda **_kwargs: calls.append("populate")
    monkeypatch.setattr(time, "time", lambda: 100.0)

    MessageViewerTab._refresh_js8_messages(tab, force=False, rebuild=False)

    assert calls[:2] == ["load", "background"]
    assert "populate" not in calls


def test_refresh_js8_messages_fallback_ingest_reloads_projection_after_local_ingest(monkeypatch) -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    calls: list[str] = []
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._last_js8_ingest_ts = 0.0
    tab._js8_ingest_interval_sec = 30.0
    tab._js8_display_snapshot_fp = None
    tab._message_rows = []
    tab.settings = {}
    tab._js8_display_fingerprint = lambda: (("db", len(calls)),)
    tab._load_structured_message_projections = lambda **_kwargs: calls.append("load")
    tab._request_background_ingest = lambda *_args, **_kwargs: False
    tab._ingest_js8_runtime_messages = lambda: calls.append("ingest")
    tab._populate_messages_table = lambda **_kwargs: calls.append("populate")
    monkeypatch.setattr(time, "time", lambda: 100.0)

    MessageViewerTab._refresh_js8_messages(tab, force=False, rebuild=False)

    assert calls[:3] == ["load", "ingest", "load"]
    assert "populate" not in calls


def test_projection_primary_populate_does_not_start_legacy_rows_build_on_normal_activation(monkeypatch) -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    calls: list[str] = []
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab._has_active_view = True
    tab._app_active = True
    tab._freeze_messages_table = False
    tab._deferred_refresh = True
    tab._projection_primary_enabled = True
    tab._load_projected_messages_into_table = lambda: calls.append("projected") or True
    tab._start_rows_build = lambda **_kwargs: calls.append("legacy_build")

    MessageViewerTab._populate_messages_table(tab, force=False)

    assert calls == ["projected"]
    assert tab._deferred_refresh is False


def test_projection_primary_forced_populate_can_rebuild_legacy_rows_for_repair(monkeypatch) -> None:
    from freqinout.gui.message_viewer_tab import MessageViewerTab

    calls: list[str] = []
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab._has_active_view = True
    tab._app_active = True
    tab._freeze_messages_table = False
    tab._deferred_refresh = True
    tab._projection_primary_enabled = True
    tab._load_projected_messages_into_table = lambda: calls.append("projected") or True
    tab._start_rows_build = lambda **kwargs: calls.append(f"legacy_build:{bool(kwargs.get('force'))}")

    MessageViewerTab._populate_messages_table(tab, force=True)

    assert calls == ["projected", "legacy_build:True"]
    assert tab._deferred_refresh is False


def test_bulk_delete_audits_skipped_unsupported_rows(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    row = UnifiedMessage("Mystery", "NEW", "K7ETC", "MR08", 2.0, "", "Unsupported", "mystery", object())

    class Model:
        def clear_selection(self):
            pass

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._messages_model = Model()
    tab._unfreeze_table = lambda: None
    tab._remove_deleted_rows_from_current_view = lambda _rows: None
    tab._populate_messages_table = lambda force=False: None
    monkeypatch.setattr(QMessageBox, "information", lambda *_args, **_kwargs: None)

    MessageViewerTab._bulk_delete_rows(tab, [row])

    conn = sqlite3.connect(db_path)
    stored = conn.execute("SELECT source, result, detail FROM message_delete_audit").fetchone()
    conn.close()

    assert stored == ("Mystery", "skipped", "unsupported message payload")


def test_bulk_delete_completion_uses_commstat_projection_cleanup_detail(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg = _commstat_artifact(title="CommStat | Water update")
    row = UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 2.0, "", "Water update", "commstat", msg)
    captured: dict[str, str] = {}

    class Model:
        def clear_selection(self):
            pass

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._messages_model = Model()
    tab._unfreeze_table = lambda: None
    tab._remove_deleted_rows_from_current_view = lambda _rows: None
    tab._populate_messages_table = lambda force=False: None
    tab._remember_locally_deleted_row = lambda _row: None
    tab._execute_message_delete = lambda _row: MessageDeleteExecutionResult("deleted", "deleted_projection", deleted_row=True)
    monkeypatch.setattr(QMessageBox, "information", lambda _parent, _title, text: captured.setdefault("text", text))

    MessageViewerTab._bulk_delete_rows(tab, [row])

    assert "Cleaned stale FIO rows: 1" in captured["text"]
    assert "Hidden/deleted" not in captured["text"]
    conn = sqlite3.connect(db_path)
    stored = conn.execute("SELECT result, detail FROM message_delete_audit").fetchone()
    conn.close()
    assert stored == ("deleted", "CommStat source row was already absent; stale FIO projection removed")


def test_message_maintenance_loads_recent_delete_audit_rows(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    MessageViewerTab._ensure_message_delete_audit_table(tab)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        """
        INSERT INTO message_delete_audit
            (audit_ts, batch_id, source, action, result, row_key, from_call, to_call, title, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (10.0, "a", "FLMSG", "Move files to Recycle Bin", "deleted", "1", "K7ETC", "MR08", "Old", "ok"),
            (20.0, "b", "JS8Spotter", "Delete from FIO message store", "failed", "2", "N1MAG", "MAGNET", "New", "busy"),
        ],
    )
    conn.commit()
    conn.close()

    rows = MessageViewerTab._load_message_delete_audit_rows(tab, limit=1)

    assert len(rows) == 1
    assert rows[0]["batch_id"] == "b"
    assert rows[0]["source"] == "JS8Spotter"
    assert rows[0]["title"] == "New"


def test_message_maintenance_loads_hidden_commstat_rows(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    ensure_commstat_artifact_deletion_tables(conn)
    tombstone_commstat_artifact(
        conn,
        artifact_key="commstat:one",
        artifact_kind="SITREP",
        from_call="K7ETC",
        target="@MR08",
        title="Water update",
        event_ts=12.0,
        reason="message_viewer_delete",
    )
    conn.commit()
    conn.close()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path

    rows = MessageViewerTab._load_hidden_commstat_rows(tab, limit=10)

    assert len(rows) == 1
    assert rows[0]["artifact_key"] == "commstat:one"
    assert rows[0]["from_call"] == "K7ETC"
    assert rows[0]["target"] == "@MR08"
    assert rows[0]["title"] == "Water update"


def test_commstat_delete_removes_native_source_row_when_provenance_is_known(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    source_db = tmp_path / "commstat.db"
    source_conn = sqlite3.connect(source_db)
    source_conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, msg_id TEXT, message TEXT)")
    source_conn.execute("INSERT INTO messages (id, msg_id, message) VALUES (42, 'M42', 'Water update')")
    source_conn.commit()
    source_conn.close()

    conn = sqlite3.connect(db_path)
    ensure_commstat_artifact_tables(conn)
    conn.execute(
        """
        INSERT INTO commstat_artifacts (
            artifact_key, artifact_kind, subtype, event_ts, event_ts_utc, from_call, target,
            report_group, grid, state_code, scope, transport_mode, status_label, alert_color,
            title, body_text, remarks_text, source_first, source_last, sources_json,
            source_count, source_refs_json, external_ids_json, payload_json, inserted_ts, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "commstat:test",
            "MESSAGE",
            "COMMSTAT_FWD",
            10.0,
            "2026-08-11T12:00:00Z",
            "K7ETC",
            "@MR08",
            "MR08",
            "",
            "",
            "",
            "js8",
            "INFO",
            "",
            "CommStat | Water update",
            "Water update",
            "",
            "COMMSTAT3",
            "COMMSTAT3",
            '["COMMSTAT3"]',
            1,
            '["messages:42"]',
            "[]",
            "{}",
            10.0,
            10.0,
        ),
    )
    conn.execute(
        """
        CREATE TABLE sitrep_source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            source_id INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO sitrep_source_events (source, source_table, source_db_path, source_id)
        VALUES ('COMMSTAT3', 'messages', ?, 42)
        """,
        (str(source_db),),
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    msg = _commstat_artifact()

    result = MessageViewerTab._delete_commstat_row(tab, msg)

    assert result == "deleted_source"
    source_conn = sqlite3.connect(source_db)
    assert source_conn.execute("SELECT COUNT(*) FROM messages WHERE id=42").fetchone()[0] == 0
    source_conn.close()
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM commstat_artifacts WHERE artifact_key='commstat:test'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM sitrep_source_events WHERE source_table='messages' AND source_id=42").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM commstat_artifact_deletions").fetchone()[0] == 0
    conn.close()


def test_commstat_delete_removes_stale_projection_when_source_row_is_already_absent(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    source_db = tmp_path / "commstat.db"
    source_conn = sqlite3.connect(source_db)
    source_conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, msg_id TEXT, message TEXT)")
    source_conn.commit()
    source_conn.close()

    conn = sqlite3.connect(db_path)
    ensure_commstat_artifact_tables(conn)
    conn.execute(
        """
        INSERT INTO commstat_artifacts (
            artifact_key, artifact_kind, subtype, event_ts, event_ts_utc, from_call, target,
            report_group, grid, state_code, scope, transport_mode, status_label, alert_color,
            title, body_text, remarks_text, source_first, source_last, sources_json,
            source_count, source_refs_json, external_ids_json, payload_json, inserted_ts, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "commstat:test",
            "MESSAGE",
            "COMMSTAT_FWD",
            10.0,
            "2026-08-11T12:00:00Z",
            "K7ETC",
            "@MR08",
            "MR08",
            "",
            "",
            "",
            "js8",
            "INFO",
            "",
            "CommStat | Water update",
            "Water update",
            "",
            "COMMSTAT3",
            "COMMSTAT3",
            '["COMMSTAT3"]',
            1,
            '["messages:42"]',
            "[]",
            "{}",
            10.0,
            10.0,
        ),
    )
    conn.execute(
        """
        CREATE TABLE sitrep_source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            source_id INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO sitrep_source_events (source, source_table, source_db_path, source_id)
        VALUES ('COMMSTAT3', 'messages', ?, 42)
        """,
        (str(source_db),),
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    msg = _commstat_artifact()

    result = MessageViewerTab._delete_commstat_row(tab, msg)

    assert result == "deleted_projection"
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM commstat_artifacts WHERE artifact_key='commstat:test'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM sitrep_source_events WHERE source_table='messages' AND source_id=42").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM commstat_artifact_deletions").fetchone()[0] == 0
    conn.close()


def test_commstat_delete_does_not_partially_delete_mixed_source_refs(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    source_db = tmp_path / "commstat.db"
    source_conn = sqlite3.connect(source_db)
    source_conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, msg_id TEXT, message TEXT)")
    source_conn.execute("INSERT INTO messages (id, msg_id, message) VALUES (42, 'M42', 'Water update')")
    source_conn.commit()
    source_conn.close()

    conn = sqlite3.connect(db_path)
    ensure_commstat_artifact_tables(conn)
    conn.execute(
        """
        INSERT INTO commstat_artifacts (
            artifact_key, artifact_kind, subtype, event_ts, event_ts_utc, from_call, target,
            report_group, grid, state_code, scope, transport_mode, status_label, alert_color,
            title, body_text, remarks_text, source_first, source_last, sources_json,
            source_count, source_refs_json, external_ids_json, payload_json, inserted_ts, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "commstat:test",
            "MESSAGE",
            "COMMSTAT_FWD",
            10.0,
            "2026-08-11T12:00:00Z",
            "K7ETC",
            "@MR08",
            "MR08",
            "",
            "",
            "",
            "js8",
            "INFO",
            "",
            "CommStat | Water update",
            "Water update",
            "",
            "COMMSTAT3",
            "COMMSTAT3",
            '["COMMSTAT3"]',
            1,
            '["messages:42", "messages:43"]',
            "[]",
            "{}",
            10.0,
            10.0,
        ),
    )
    conn.execute(
        """
        CREATE TABLE sitrep_source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            source_id INTEGER NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO sitrep_source_events (source, source_table, source_db_path, source_id)
        VALUES ('COMMSTAT3', 'messages', ?, ?)
        """,
        [(str(source_db), 42), (str(source_db), 43)],
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    msg = _commstat_artifact(source_refs_json='["messages:42", "messages:43"]')

    result = MessageViewerTab._delete_commstat_row(tab, msg)

    assert result == "failed"
    source_conn = sqlite3.connect(source_db)
    assert source_conn.execute("SELECT COUNT(*) FROM messages WHERE id=42").fetchone()[0] == 1
    source_conn.close()
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM commstat_artifacts WHERE artifact_key='commstat:test'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM commstat_artifact_deletions").fetchone()[0] == 0
    conn.close()


def test_commstat_delete_falls_back_to_hidden_when_source_path_is_missing(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    ensure_commstat_artifact_tables(conn)
    conn.execute(
        """
        INSERT INTO commstat_artifacts (
            artifact_key, artifact_kind, subtype, event_ts, event_ts_utc, from_call, target,
            report_group, grid, state_code, scope, transport_mode, status_label, alert_color,
            title, body_text, remarks_text, source_first, source_last, sources_json,
            source_count, source_refs_json, external_ids_json, payload_json, inserted_ts, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "commstat:test",
            "MESSAGE",
            "COMMSTAT_FWD",
            10.0,
            "2026-08-11T12:00:00Z",
            "K7ETC",
            "@MR08",
            "MR08",
            "",
            "",
            "",
            "js8",
            "INFO",
            "",
            "CommStat | Water update",
            "Water update",
            "",
            "COMMSTAT3",
            "COMMSTAT3",
            '["COMMSTAT3"]',
            1,
            '["messages:42"]',
            "[]",
            "{}",
            10.0,
            10.0,
        ),
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    msg = _commstat_artifact()

    result = MessageViewerTab._delete_commstat_row(tab, msg)

    assert result == "hidden"
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT COUNT(*) FROM commstat_artifacts WHERE artifact_key='commstat:test'").fetchone()[0] == 0
    hidden = conn.execute(
        "SELECT artifact_key, reason FROM commstat_artifact_deletions WHERE artifact_key='commstat:test'"
    ).fetchone()
    conn.close()
    assert hidden == ("commstat:test", "message_viewer_delete")


def test_message_file_metadata_cache_persists_display_ready_file_rows(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("MAGNET General Use Form\nTo\nMR08\nFrom\nK7ETC\nSubject\nWidemouth 2 Fire\n", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(
        path=msg_path,
        origin="flmsg",
        size=stat.st_size,
        mtime=stat.st_mtime,
        source_id="source-flmsg-a",
        source_label="FIO-A FLMSG",
    )
    row = UnifiedMessage(
        "General",
        "NEW",
        "K7ETC",
        "MR08",
        datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp(),
        "13 days",
        "Widemouth 2 Fire",
        "flmsg",
        rec,
        topics=("Fire", "Travel/Roads"),
        actionable=True,
        search_text="K7ETC MR08 Widemouth 2 Fire wildfire road closure",
        report_ts=datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp(),
        age_ts_source="report",
    )

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO message_scan_cache(origin, path, mtime, size) VALUES (?, ?, ?, ?)",
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size)),
    )
    conn.commit()
    conn.close()

    MessageViewerTab._save_message_file_metadata_from_rows(tab, [row])

    conn = sqlite3.connect(db_path)
    stored = conn.execute(
        """
        SELECT origin, path, msg_type, status, from_call, to_call, title, rcv_display, report_ts,
               age_ts_source, topics_json, actionable, search_text, source_id, source_label
        FROM message_file_metadata
        """
    ).fetchone()
    conn.close()

    assert stored[0] == "flmsg"
    assert stored[1] == str(msg_path)
    assert stored[2] == "General"
    assert stored[3] == "NEW"
    assert stored[4] == "K7ETC"
    assert stored[5] == "MR08"
    assert stored[6] == "Widemouth 2 Fire"
    assert stored[7] == "13 days"
    assert stored[8] == datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp()
    assert stored[9] == "report"
    assert stored[10] == '["Fire","Travel/Roads"]'
    assert stored[11] == 1
    assert "wildfire" in stored[12]
    assert stored[13] == "source-flmsg-a"
    assert stored[14] == "FIO-A FLMSG"


def test_message_file_metadata_save_projects_file_report_to_observations(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text(
        """
MAGNET General Use Form - v1.1.1
Date/Time/Msg ID
260729-0354z
To
MR08
From
K7ETC
Subject
Widemouth 2 Fire
Message
UT - Widemouth 2 Fire - DM38ST - wildfire evacuation posture updated.
""",
        encoding="utf-8",
    )
    stat = msg_path.stat()
    rec = FileRecord(
        path=msg_path,
        origin="flmsg",
        size=stat.st_size,
        mtime=stat.st_mtime,
        source_id="source-flmsg-a",
        source_label="FIO-A FLMSG",
    )
    row = UnifiedMessage(
        "General",
        "NEW",
        "K7ETC",
        "MR08",
        datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp(),
        "20 days",
        "Widemouth 2 Fire",
        "flmsg",
        rec,
        topics=("Fire",),
        actionable=True,
        search_text="K7ETC MR08 Widemouth 2 Fire wildfire DM38ST",
        report_ts=datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp(),
        age_ts_source="report",
    )

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    tab.settings = {}
    tab.files = {"flmsg": [rec], "flamp": []}
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO message_scan_cache(origin, path, mtime, size) VALUES (?, ?, ?, ?)",
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size)),
    )
    conn.commit()
    conn.close()

    MessageViewerTab._save_message_file_metadata_from_rows(tab, [row])

    conn = sqlite3.connect(db_path)
    stored = conn.execute(
        """
        SELECT source_family, from_call, to_target, subject, observed_topics_json, summary
        FROM observation_projection
        WHERE source_ref=?
        """,
        (f"file:{msg_path}",),
    ).fetchone()
    conn.close()

    assert stored is not None
    assert stored[0] == "flmsg"
    assert stored[1] == "K7ETC"
    assert stored[2] == "MR08"
    assert stored[3] == "Widemouth 2 Fire"
    assert "Fire" in stored[4]
    assert "Widemouth 2 Fire" in stored[5]


def test_message_file_metadata_table_adds_source_and_report_indexes(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_file_metadata_table(conn)
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(message_file_metadata)").fetchall()}
    finally:
        conn.close()

    assert "idx_message_file_metadata_source" in indexes
    assert "idx_message_file_metadata_report_ts" in indexes


def test_message_file_metadata_table_migrates_old_cache_columns(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE message_file_metadata (
                origin TEXT NOT NULL,
                path TEXT NOT NULL,
                mtime REAL NOT NULL,
                size INTEGER NOT NULL,
                PRIMARY KEY (origin, path, mtime, size)
            )
            """
        )
        ensure_message_file_metadata_table(conn)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(message_file_metadata)").fetchall()}
    finally:
        conn.close()

    assert {
        "source_id",
        "source_label",
        "source_family",
        "msg_type",
        "display_type",
        "status",
        "from_call",
        "to_call",
        "title",
        "rcv_display",
        "report_ts",
        "age_ts_source",
        "topics_json",
        "actionable",
        "search_text",
        "parser_version",
        "indexed_ts",
    }.issubset(columns)


def test_cached_message_file_metadata_normalizes_timestamps_topics_and_flags(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("body", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    report_ts = datetime.datetime(2026, 8, 3, 4, 2, 12, tzinfo=datetime.timezone.utc).timestamp()

    cached = normalize_cached_message_file_metadata(
        {
            "source_family": "flmsg",
            "msg_type": "FLMSG",
            "display_type": "General",
            "status": "NEW",
            "from_call": "K7ETC",
            "to_call": "MR08",
            "title": "Widemouth 2 Fire",
            "rcv_display": "8d",
            "report_ts": report_ts,
            "age_ts_source": "REPORT",
            "topics_json": '["Fire","","Travel/Roads"]',
            "actionable": 1,
            "search_text": "wildfire",
        }
    )

    assert cached is not None
    assert cached.msg_type == "FLMSG"
    assert cached.display_type == "General"
    assert cached.from_call == "K7ETC"
    assert cached.to_call == "MR08"
    assert cached.report_ts == report_ts
    assert cached.age_ts_source == "report"
    assert cached.age_timestamp_for(rec) == report_ts
    assert cached.topics == ("Fire", "Travel/Roads")
    assert cached.actionable is True
    assert cached.search_text == "wildfire"

    received = normalize_cached_message_file_metadata({"topics_json": "not json", "actionable": 0})
    assert received is not None
    assert received.report_ts == 0.0
    assert received.age_ts_source == "received"
    assert received.age_timestamp_for(rec) == stat.st_mtime
    assert received.topics == ()
    assert received.actionable is False


def test_cached_message_file_row_summary_applies_display_fallbacks(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-LONG.k2s"
    msg_path.write_text("body", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    long_title = "  Widemouth   2 Fire with a very long operational update title that needs table trimming  "
    report_ts = datetime.datetime(2026, 8, 3, 4, 2, 12, tzinfo=datetime.timezone.utc).timestamp()

    summary = cached_message_file_row_summary(
        rec,
        {
            "from_call": "K7ETC",
            "to_call": "MR08",
            "title": long_title,
            "report_ts": report_ts,
            "topics_json": '["Fire"]',
            "actionable": 1,
        },
        fallback_origin="flmsg",
    )

    assert summary is not None
    assert summary.msg_type == "FLMSG"
    assert summary.from_call == "K7ETC"
    assert summary.to_call == "MR08"
    assert summary.rcv_ts == report_ts
    assert summary.report_ts == report_ts
    assert summary.age_ts_source == "report"
    assert summary.topics == ("Fire",)
    assert summary.actionable is True
    assert summary.title == "Widemouth 2 Fire with a very long operational update titl..."

    fallback = cached_message_file_row_summary(
        rec,
        {"topics_json": "[]"},
        fallback_origin="bbs",
        fallback_title="  BBS   Notice  ",
    )
    assert fallback is not None
    assert fallback.msg_type == "BBS"
    assert fallback.title == "BBS Notice"
    assert fallback.rcv_ts == stat.st_mtime
    assert fallback.report_ts == 0.0
    assert fallback.age_ts_source == "received"


def test_message_file_metadata_does_not_store_file_mtime_as_report_timestamp(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-NODATE.k2s"
    msg_path.write_text("MAGNET General Use Form\nTo\nMR08\nFrom\nK7ETC\nSubject\nNo Date\n", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    row = UnifiedMessage(
        "General",
        "NEW",
        "K7ETC",
        "MR08",
        stat.st_mtime,
        "received",
        "No Date",
        "flmsg",
        rec,
        search_text="K7ETC MR08 No Date",
        report_ts=0.0,
        age_ts_source="received",
    )
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO message_scan_cache(origin, path, mtime, size) VALUES (?, ?, ?, ?)",
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size)),
    )
    conn.commit()
    conn.close()

    MessageViewerTab._save_message_file_metadata_from_rows(tab, [row])

    loaded = MessageViewerTab._load_message_file_metadata_map(tab, {"flmsg": [rec]})
    cached = loaded[("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size))]
    assert cached["report_ts"] == 0.0
    assert cached["age_ts_source"] == "received"

    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={"flmsg": [rec]},
        file_metadata_map=loaded,
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        generation=1,
        force=True,
    )

    emitted = []
    worker.finished.connect(lambda payload: emitted.append(payload))
    worker.run()
    rows = emitted[0]["rows"]
    assert len(rows) == 1
    assert rows[0].rcv_ts == stat.st_mtime
    assert rows[0].report_ts == 0.0
    assert rows[0].age_ts_source == "received"


def test_message_file_metadata_cache_loads_only_current_file_identity(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("current", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        """
        INSERT INTO message_file_metadata (
            origin, path, mtime, size, source_family, msg_type, display_type, status, from_call,
            to_call, title, rcv_display, topics_json, actionable, search_text, parser_version, indexed_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "flmsg",
                str(msg_path),
                float(stat.st_mtime),
                int(stat.st_size),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Widemouth 2 Fire",
                "1 min",
                '["Fire"]',
                1,
                "wildfire",
                FILE_METADATA_PARSER_VERSION,
                1.0,
            ),
            (
                "flmsg",
                str(msg_path),
                float(stat.st_mtime) - 10.0,
                int(stat.st_size),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Stale Title",
                "1 min",
                "[]",
                0,
                "stale",
                FILE_METADATA_PARSER_VERSION - 1,
                1.0,
            ),
        ],
    )
    conn.commit()
    conn.close()

    loaded = MessageViewerTab._load_message_file_metadata_map(tab, {"flmsg": [rec]})

    assert set(loaded) == {("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size))}
    assert loaded[("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size))]["title"] == "Widemouth 2 Fire"
    assert loaded[("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size))]["display_type"] == "General"


def test_existing_message_file_metadata_records_load_existing_files_only(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    current_path = tmp_path / "KJ4RMO-20260408-Op_net4-83.b2s"
    stale_path = tmp_path / "stale.b2s"
    missing_path = tmp_path / "missing.b2s"
    current_path.write_text("current", encoding="utf-8")
    stale_path.write_text("stale changed", encoding="utf-8")
    current_stat = current_path.stat()
    stale_stat = stale_path.stat()
    with sqlite3.connect(db_path) as conn:
        ensure_message_file_metadata_table(conn)
        conn.executemany(
            """
            INSERT INTO message_file_metadata (
                origin, path, mtime, size, source_family, msg_type, display_type, status, from_call,
                to_call, title, rcv_display, topics_json, actionable, search_text, parser_version, indexed_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "flmsg",
                    str(current_path),
                    float(current_stat.st_mtime),
                    int(current_stat.st_size),
                    "flmsg",
                    "FLMSG",
                    "Blank",
                    "READ",
                    "KJ4RMO",
                    "",
                    "OpNet4 83",
                    "2026-08-04 15:31:00",
                    '["Food"]',
                    1,
                    "op net4 kj4rmo food",
                    FILE_METADATA_PARSER_VERSION,
                    1.0,
                ),
                (
                    "flmsg",
                    str(stale_path),
                    float(stale_stat.st_mtime) - 10.0,
                    int(stale_stat.st_size),
                    "flmsg",
                    "FLMSG",
                    "Blank",
                    "READ",
                    "STALE",
                    "",
                    "Stale",
                    "",
                    "[]",
                    0,
                    "stale",
                    FILE_METADATA_PARSER_VERSION,
                    1.0,
                ),
                (
                    "flmsg",
                    str(missing_path),
                    1.0,
                    1,
                    "flmsg",
                    "FLMSG",
                    "Blank",
                    "READ",
                    "MISSING",
                    "",
                    "Missing",
                    "",
                    "[]",
                    0,
                    "missing",
                    FILE_METADATA_PARSER_VERSION,
                    1.0,
                ),
            ],
        )

    records, metadata = load_existing_message_file_metadata_records(db_path)

    assert [rec.path for rec in records["flmsg"]] == [current_path]
    key = ("flmsg", str(current_path), float(current_stat.st_mtime), int(current_stat.st_size))
    assert set(metadata) == {key}
    assert metadata[key]["from_call"] == "KJ4RMO"


def test_messages_rows_include_existing_cached_files_outside_current_scan(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    cached_path = tmp_path / "KJ4RMO-20260408-Op_net4-83.b2s"
    cached_path.write_text("cached", encoding="utf-8")
    stat = cached_path.stat()
    with sqlite3.connect(db_path) as conn:
        ensure_message_file_metadata_table(conn)
        conn.execute(
            """
            INSERT INTO message_file_metadata (
                origin, path, mtime, size, source_family, msg_type, display_type, status, from_call,
                to_call, title, rcv_display, topics_json, actionable, search_text, parser_version, indexed_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "flmsg",
                str(cached_path),
                float(stat.st_mtime),
                int(stat.st_size),
                "flmsg",
                "FLMSG",
                "Blank",
                "READ",
                "KJ4RMO",
                "",
                "OpNet4 83",
                "2026-08-04 15:31:00",
                '["Food"]',
                1,
                "op net4 kj4rmo food",
                FILE_METADATA_PARSER_VERSION,
                1.0,
            ),
        )

    class Settings:
        def get(self, key, default=None):
            return default

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = Settings()
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab.files = {"varac": [], "flmsg": [], "flamp": [], "bbs": []}
    tab._file_metadata_map = {}
    tab._db_path = lambda: db_path
    def ensure_cache_table():
        with sqlite3.connect(db_path) as conn:
            ensure_message_file_metadata_table(conn)

    tab._ensure_file_scan_cache_table = ensure_cache_table
    tab._get_read_state = lambda rec: "READ"
    tab._is_image_file = lambda path: False
    tab._is_transport_form_ext = lambda suffix: True
    tab._extract_form_file_metadata = lambda rec: {}
    tab._extract_sender_from_file = lambda rec: ""
    tab._format_rcv_display = lambda ts, raw=None: "2026-08-04 15:31:00"
    tab._is_auth_verifiable_file = lambda rec: False
    tab._is_truthy = staticmethod(lambda value, default=False: bool(default if value is None else value))

    rows = MessageViewerTab._build_message_rows(tab)

    assert [row.from_call for row in rows] == ["KJ4RMO"]
    assert row_matches_search_query(rows[0], "KJ4RMO")
    assert row_matches_search_query(rows[0], "food")


def test_stale_message_file_metadata_check_is_limited_to_current_files(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    current_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    other_path = tmp_path / "old.k2s"
    current_path.write_text("current", encoding="utf-8")
    other_path.write_text("other", encoding="utf-8")
    current_stat = current_path.stat()
    other_stat = other_path.stat()
    rec = FileRecord(path=current_path, origin="flmsg", size=current_stat.st_size, mtime=current_stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        """
        INSERT INTO message_file_metadata (
            origin, path, mtime, size, source_family, msg_type, display_type, status, from_call,
            to_call, title, rcv_display, report_ts, topics_json, actionable, search_text, parser_version, indexed_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "flmsg",
                str(current_path),
                float(current_stat.st_mtime),
                int(current_stat.st_size),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Current",
                "now",
                0.0,
                "[]",
                0,
                "current",
                FILE_METADATA_PARSER_VERSION,
                1.0,
            ),
            (
                "flmsg",
                str(other_path),
                float(other_stat.st_mtime),
                int(other_stat.st_size),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Old unrelated",
                "old",
                0.0,
                "[]",
                0,
                "old",
                FILE_METADATA_PARSER_VERSION - 1,
                1.0,
            ),
        ],
    )
    conn.commit()
    conn.close()

    assert has_stale_message_file_metadata(db_path, {"flmsg": [rec]}) is False

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        UPDATE message_file_metadata
        SET parser_version=?
        WHERE origin=? AND path=? AND mtime=? AND size=?
        """,
        (
            FILE_METADATA_PARSER_VERSION - 1,
            "flmsg",
            str(current_path),
            float(current_stat.st_mtime),
            int(current_stat.st_size),
        ),
    )
    conn.commit()
    conn.close()

    assert has_stale_message_file_metadata(db_path, {"flmsg": [rec]}) is True


def test_message_file_metadata_refresh_rebuilds_old_cache_with_form_report_age(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text(
        "CUSTOM_FORM,missing.html\n"
        "L01,MR08\n"
        "L02,K7ETC\n"
        "L03,Widemouth 2 Fire\n"
        "L04,260729-0354z\n"
        "L06,UT - DM38ST - wildfire update",
        encoding="utf-8",
    )
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    report_ts = datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO message_scan_cache(origin, path, mtime, size) VALUES (?, ?, ?, ?)",
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size)),
    )
    conn.execute(
        """
        INSERT INTO message_file_metadata (
            origin, path, mtime, size, source_family, msg_type, display_type, status, from_call,
            to_call, title, rcv_display, report_ts, topics_json, actionable, search_text, parser_version, indexed_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "flmsg",
            str(msg_path),
            float(stat.st_mtime),
            int(stat.st_size),
            "flmsg",
            "FLMSG",
            "General",
            "NEW",
            "K7ETC",
            "MR08",
            "Stale Mtime Row",
            "mtime",
            0.0,
            "[]",
            0,
            "stale",
            FILE_METADATA_PARSER_VERSION - 1,
            1.0,
        ),
    )
    conn.commit()
    conn.close()

    assert MessageViewerTab._load_message_file_metadata_map(tab, {"flmsg": [rec]}) == {}

    worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={"flmsg": [rec]},
        file_metadata_map={},
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=1,
    )
    emitted = []
    worker.finished.connect(lambda payload: emitted.append(payload))
    worker.run()
    row = emitted[0]["rows"][0]
    assert row.rcv_ts == report_ts
    assert row.rcv_ts != stat.st_mtime
    assert row.report_ts == report_ts
    assert row.age_ts_source == "report"

    MessageViewerTab._save_message_file_metadata_from_rows(tab, [row])
    loaded = MessageViewerTab._load_message_file_metadata_map(tab, {"flmsg": [rec]})
    cached = loaded[("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size))]
    assert cached["title"] == "Widemouth 2 Fire"
    assert cached["report_ts"] == report_ts
    assert cached["age_ts_source"] == "report"

    cached_worker = _RowsBuildWorker(
        js8_messages=[],
        spotter_messages=[],
        varac_messages=[],
        sitrep_messages=[],
        commstat_messages=[],
        files={"flmsg": [rec]},
        file_metadata_map=loaded,
        read_state_map={},
        signature_state_map={},
        spotter_auth_state_map={},
        spotter_expect_state_map={},
        sender_cache_seed={},
        form_titles={},
        custom_forms_path="",
        message_form_codes=None,
        alert_form_codes=None,
        show_local_time=False,
        tz_name="UTC",
        sitrep_dedupe_enabled=False,
        sitrep_show_raw_duplicates=False,
        force=False,
        generation=2,
    )
    cached_emitted = []
    cached_worker.finished.connect(lambda payload: cached_emitted.append(payload))
    cached_worker._extract_form_file_metadata = lambda _rec: (_ for _ in ()).throw(
        AssertionError("fresh metadata fast path should not parse source")
    )
    cached_worker.run()

    cached_row = cached_emitted[0]["rows"][0]
    assert cached_row.rcv_ts == report_ts
    assert cached_row.report_ts == report_ts
    assert cached_row.age_ts_source == "report"
    assert cached_emitted[0]["file_metadata_hits"] == 1
    assert cached_emitted[0]["file_parse_count"] == 0


def test_message_intelligence_header_shows_report_date_separate_from_received() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {"message_viewer": {"time_mode": "UTC"}}
    tab._current_time_mode = lambda: "UTC"
    info = MessageIntelligence(
        source_type="flmsg",
        form_name="MAGNET General Use Form",
        from_call="K7ETC",
        to_call="MR08",
        subject="Widemouth 2 Fire",
        date_summary="260729-0354z",
        summary="Widemouth 2 Fire",
    )

    lines = MessageViewerTab._message_intelligence_header(
        tab,
        info,
        timestamp="2026-08-03 17:33",
        report_timestamp=datetime.datetime(2026, 7, 29, 3, 54, tzinfo=datetime.timezone.utc).timestamp(),
        status="NEW",
        source="FLMSG",
    )
    text = "\n".join(lines)

    assert "  Date/Msg ID: 260729-0354z" in text
    assert "  Report Date: 2026-07-29 03:54:00" in text
    assert "  Received: 2026-08-03 17:33" in text


def test_sitrep_detail_hides_internal_report_key() -> None:
    class Label:
        text = ""

        def setText(self, value):
            self.text = value

    class Viewer:
        text = ""

        def setAcceptRichText(self, _value):
            pass

        def setPlainText(self, value):
            self.text = value

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab._current_time_mode = lambda: "UTC"
    tab._format_rcv_display = lambda *_args: "2026-08-24 12:00"
    tab.info_label = Label()
    tab.viewer = Viewer()
    msg = SitrepMessage(
        event_id=1,
        report_key="8f2d4b6a7c",
        event_ts=1.0,
        event_ts_utc="2026-08-24T12:00:00Z",
        from_call="K7ABC",
        target="@MAGNET",
        report_group="MAGNET",
        grid="DM38ST",
        state_code="UT",
        state_confidence="high",
        geo_confidence="grid",
        scope="My Location",
        subtype="COMMSTAT",
        subtype_label="CommStat",
        transport_mode="js8",
        transport_label="JS8/RF",
        remarks_text="",
        brevity_code="",
        brevity_summary="",
        source_family_label="CommStat",
        overall_status="not_reported",
        power="grid_down",
        water="not_reported",
        medical="not_reported",
        communications="not_reported",
        internet="not_reported",
        travel="not_reported",
        food="not_reported",
        fuel="not_reported",
        crime="not_reported",
        civil_unrest="not_reported",
        political="not_reported",
        source_first="CommStat",
        source_last="CommStat",
        source_count=1,
        sources_json="[]",
        source_refs_json="[]",
        raw_payload_json="{}",
        updated_ts=1.0,
    )

    MessageViewerTab._load_sitrep_content(tab, msg)

    assert "Report: 8f2d4b6a7c" not in tab.viewer.text
    assert "Radio: 8f2d4b6a7c" not in tab.viewer.text
    assert "Power:" in tab.viewer.text


def test_sitrep_search_ignores_not_reported_status_dataclass_fields() -> None:
    msg = SitrepMessage(
        event_id=1,
        report_key="8f2d4b6a7c",
        event_ts=1.0,
        event_ts_utc="2026-08-24T12:00:00Z",
        from_call="KG5RKW",
        target="@MAGNET",
        report_group="MAGNET",
        grid="EL06VD",
        state_code="",
        state_confidence="",
        geo_confidence="",
        scope="My Location",
        subtype="COMMSTAT",
        subtype_label="CommStat",
        transport_mode="",
        transport_label="Unknown",
        remarks_text="",
        brevity_code="",
        brevity_summary="",
        source_family_label="JS8Spotter",
        overall_status="not_reported",
        power="not_reported",
        water="not_reported",
        medical="not_reported",
        communications="not_reported",
        internet="not_reported",
        travel="not_reported",
        food="not_reported",
        fuel="not_reported",
        crime="not_reported",
        civil_unrest="not_reported",
        political="not_reported",
        source_first="JS8SPOTTER",
        source_last="JS8SPOTTER",
        source_count=1,
        sources_json="[]",
        source_refs_json="[]",
        raw_payload_json="{}",
        updated_ts=1.0,
    )
    row = UnifiedMessage(
        "SitRep",
        "INFO",
        "KG5RKW",
        "MAGNET",
        1.0,
        "19 days",
        "CommStat | My Location | NOT_REPORTED",
        "sitrep",
        msg,
        search_text="SitRep INFO KG5RKW MAGNET 19 days CommStat My Location JS8Spotter Unknown",
    )

    assert not row_matches_search_query(row, "food")
    assert not row_matches_search_query(row, "power")

    msg.power = "grid_down"
    assert row_matches_search_query(row, "power")


def test_message_file_metadata_cache_prunes_rows_not_in_current_scan(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    kept_path = tmp_path / "kept.k2s"
    stale_path = tmp_path / "stale.k2s"
    kept_path.write_text("kept", encoding="utf-8")
    stale_path.write_text("stale", encoding="utf-8")
    kept = kept_path.stat()
    stale = stale_path.stat()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO message_scan_cache(origin, path, mtime, size) VALUES (?, ?, ?, ?)",
        ("flmsg", str(kept_path), float(kept.st_mtime), int(kept.st_size)),
    )
    conn.executemany(
        """
        INSERT INTO message_file_metadata (
            origin, path, mtime, size, source_family, msg_type, status, title, indexed_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("flmsg", str(kept_path), float(kept.st_mtime), int(kept.st_size), "flmsg", "FLMSG", "NEW", "Kept", 1.0),
            ("flmsg", str(stale_path), float(stale.st_mtime), int(stale.st_size), "flmsg", "FLMSG", "NEW", "Stale", 1.0),
        ],
    )
    conn.commit()
    conn.close()

    row = UnifiedMessage(
        "FLMSG",
        "READ",
        "K7ETC",
        "MR08",
        kept.st_mtime,
        "1 min",
        "Kept Updated",
        "flmsg",
        FileRecord(path=kept_path, origin="flmsg", size=kept.st_size, mtime=kept.st_mtime),
    )
    MessageViewerTab._save_message_file_metadata_from_rows(tab, [row])

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT path, title FROM message_file_metadata ORDER BY path").fetchall()
    conn.close()

    assert rows == [(str(kept_path), "Kept Updated")]


def test_remove_file_record_clears_file_caches_and_read_state(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    tab.files = {"flmsg": [rec], "flamp": [], "bbs": [], "varac": []}
    tab._read_state_map = {MessageViewerTab._read_state_key("flmsg", rec): ("READ", 1.0, 0)}
    tab.current_record = None
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    MessageViewerTab._ensure_read_state_table(tab)
    MessageViewerTab._ensure_signature_cache_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS observation_projection (
            observation_id TEXT PRIMARY KEY,
            source_family TEXT,
            source_ref TEXT,
            source_radio_id INTEGER,
            source_app TEXT,
            received_utc TEXT,
            event_utc TEXT,
            from_call TEXT,
            to_target TEXT,
            groups_json TEXT,
            observed_topics_json TEXT,
            operator_attention INTEGER,
            status TEXT,
            urgency TEXT,
            subject TEXT,
            summary TEXT,
            state TEXT,
            grid TEXT,
            lat REAL,
            lon REAL,
            location_confidence TEXT,
            auth_state TEXT,
            trusted_state TEXT,
            confirmed_state TEXT,
            exercise_flag INTEGER,
            route_eligible INTEGER,
            publish_authorized INTEGER,
            provenance_json TEXT,
            projected_utc TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS observation_projection_topics (
            observation_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            PRIMARY KEY (observation_id, topic)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO observation_projection (
            observation_id, source_family, source_ref, received_utc, event_utc, from_call,
            observed_topics_json, subject, summary, projected_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "flmsg:file:" + str(msg_path),
            "flmsg",
            "file:" + str(msg_path),
            "2026-08-03T00:00:00+00:00",
            "2026-08-03T00:00:00+00:00",
            "K7ETC",
            '["Fire"]',
            "Widemouth 2 Fire",
            "Widemouth 2 Fire",
            "2026-08-24T00:00:00+00:00",
        ),
    )
    conn.execute(
        "INSERT INTO observation_projection_topics(observation_id, topic) VALUES (?, ?)",
        ("flmsg:file:" + str(msg_path), "Fire"),
    )
    conn.execute(
        "INSERT INTO message_scan_cache(origin, path, mtime, size) VALUES (?, ?, ?, ?)",
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size)),
    )
    conn.execute(
        """
        INSERT INTO message_file_metadata (
            origin, path, mtime, size, source_family, msg_type, status, title, indexed_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size), "flmsg", "FLMSG", "READ", "Fire", 1.0),
    )
    conn.execute(
        """
        INSERT INTO message_signature_cache (
            origin, path, mtime, size, status, verified_ts
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size), "unsigned", 1.0),
    )
    conn.execute(
        """
        INSERT INTO message_read_state (
            origin, path, mtime, size, status, read_ts, flag_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size), "READ", 1.0, 0),
    )
    conn.commit()
    conn.close()

    MessageViewerTab._remove_file_record(tab, rec)

    assert tab.files["flmsg"] == []
    assert tab._read_state_map == {}
    conn = sqlite3.connect(db_path)
    for table in ("message_scan_cache", "message_file_metadata", "message_signature_cache", "message_read_state"):
        assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM observation_projection").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM observation_projection_topics").fetchone()[0] == 0
    conn.close()


def test_delete_file_cache_entries_clears_read_state_table_too(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    MessageViewerTab._ensure_read_state_table(tab)
    MessageViewerTab._ensure_signature_cache_table(tab)
    conn = sqlite3.connect(db_path)
    for table in ("message_scan_cache", "message_file_metadata", "message_signature_cache"):
        cols = "origin, path, mtime, size"
        vals = "?, ?, ?, ?"
        extra_cols = ""
        extra_vals = ""
        if table == "message_file_metadata":
            extra_cols = ", source_family, msg_type, status, title, indexed_ts"
            extra_vals = ", ?, ?, ?, ?, ?"
        elif table == "message_signature_cache":
            extra_cols = ", status, verified_ts"
            extra_vals = ", ?, ?"
        params = ["flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size)]
        if table == "message_file_metadata":
            params.extend(["flmsg", "FLMSG", "READ", "Fire", 1.0])
        elif table == "message_signature_cache":
            params.extend(["unsigned", 1.0])
        conn.execute(
            f"INSERT INTO {table} ({cols}{extra_cols}) VALUES ({vals}{extra_vals})",
            tuple(params),
        )
    conn.execute(
        """
        INSERT INTO message_read_state (origin, path, mtime, size, status, read_ts, flag_state)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size), "READ", 1.0, 0),
    )
    conn.commit()
    conn.close()

    delete_file_cache_entries(db_path, rec)

    conn = sqlite3.connect(db_path)
    for table in ("message_scan_cache", "message_file_metadata", "message_signature_cache", "message_read_state"):
        assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    conn.close()


def test_file_message_row_key_uses_normalized_file_identity(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    upper = FileRecord(path=msg_path, origin="FLMSG", size=stat.st_size, mtime=stat.st_mtime)
    lower = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    upper_row = UnifiedMessage("FLMSG", "READ", "", "", stat.st_mtime, "", "Fire", "flmsg", upper)
    lower_row = UnifiedMessage("FLMSG", "READ", "", "", stat.st_mtime, "", "Fire", "flmsg", lower)

    assert MessageTableModel._row_key(upper_row) == MessageTableModel._row_key(lower_row)


def test_message_row_identity_core_handles_supported_payloads(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="FLMSG", size=stat.st_size, mtime=stat.st_mtime)

    JS8Message = type("JS8Message", (), {})
    SpotterMessage = type("SpotterMessage", (), {})
    VarACMessage = type("VarACMessage", (), {})
    SitrepMessage = type("SitrepMessage", (), {})
    CommStatArtifact = type("CommStatArtifact", (), {})
    js8 = JS8Message()
    js8.msg_id = 12
    spotter = SpotterMessage()
    spotter.spotter_id = 34
    varac = VarACMessage()
    varac.source = "FIO-A"
    varac.msg_id = 56
    sitrep = SitrepMessage()
    sitrep.event_id = 78
    commstat = CommStatArtifact()
    commstat.artifact_key = "CommStat:ABC"

    assert message_payload_identity(js8) == ("js8", 12)
    assert message_payload_identity(spotter) == ("spotter", 34)
    assert message_payload_identity(varac) == ("varac", "FIO-A", 56)
    assert message_payload_identity(rec) == ("file", "flmsg", str(msg_path), float(stat.st_mtime), int(stat.st_size))
    assert message_payload_identity(sitrep) == ("sitrep", 78)
    assert message_payload_identity(commstat) == ("commstat", "commstat:abc")
    assert message_row_identity(UnifiedMessage("FLMSG", "READ", "", "", stat.st_mtime, "", "Fire", "flmsg", rec)) == (
        "file",
        "flmsg",
        str(msg_path),
        float(stat.st_mtime),
        int(stat.st_size),
    )


def test_message_row_identity_core_filters_rows_by_identity(tmp_path) -> None:
    first_path = tmp_path / "first.k2s"
    second_path = tmp_path / "second.k2s"
    first_path.write_text("one", encoding="utf-8")
    second_path.write_text("two", encoding="utf-8")
    first_stat = first_path.stat()
    second_stat = second_path.stat()
    first = UnifiedMessage(
        "FLMSG",
        "READ",
        "",
        "",
        first_stat.st_mtime,
        "",
        "One",
        "flmsg",
        FileRecord(path=first_path, origin="FLMSG", size=first_stat.st_size, mtime=first_stat.st_mtime),
    )
    second = UnifiedMessage(
        "FLMSG",
        "READ",
        "",
        "",
        second_stat.st_mtime,
        "",
        "Two",
        "flmsg",
        FileRecord(path=second_path, origin="flmsg", size=second_stat.st_size, mtime=second_stat.st_mtime),
    )
    unsupported = UnifiedMessage("UNKNOWN", "READ", "", "", 0.0, "", "Unknown", "unknown", object())

    keys = message_row_identity_set([first, unsupported])

    assert keys == {message_row_identity(first)}
    assert filter_rows_excluding_identities([first, second, unsupported], keys) == [second, unsupported]


def test_remove_file_record_normalizes_origin_and_uses_full_file_identity(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="FLMSG", size=stat.st_size, mtime=stat.st_mtime)
    kept = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size + 1, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    tab._effective_watch_dirs = lambda: [{"origin": "flmsg", "path": str(tmp_path)}]
    tab.files = {"flmsg": [rec, kept], "flamp": [], "bbs": [], "varac": []}
    tab._read_state_map = {MessageViewerTab._read_state_key("flmsg", rec): ("READ", 1.0, 0)}
    tab.current_record = None
    MessageViewerTab._ensure_file_scan_cache_table(tab)
    MessageViewerTab._ensure_read_state_table(tab)

    MessageViewerTab._remove_file_record(tab, rec)

    assert tab.files["flmsg"] == [kept]
    assert tab._read_state_map == {}


def test_load_read_state_map_normalizes_file_origin(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path
    MessageViewerTab._ensure_read_state_table(tab)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO message_read_state (origin, path, mtime, size, status, read_ts, flag_state)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("FLMSG", str(msg_path), float(stat.st_mtime), int(stat.st_size), "READ", 1.0, 0),
    )
    conn.commit()
    conn.close()

    loaded = MessageViewerTab._load_read_state_map(tab)

    assert loaded == {file_metadata_key(rec): ("READ", 1.0, 0)}


def test_remove_file_record_from_groups_uses_normalized_full_identity(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="FLMSG", size=stat.st_size, mtime=stat.st_mtime)
    kept_same_path_other_size = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size + 1, mtime=stat.st_mtime)
    other_origin = FileRecord(path=msg_path, origin="flamp", size=stat.st_size, mtime=stat.st_mtime)

    files = remove_file_record_from_groups(
        {
            "flmsg": [rec, kept_same_path_other_size],
            "flamp": [other_origin],
        },
        rec,
    )

    assert files["flmsg"] == [kept_same_path_other_size]
    assert files["flamp"] == [other_origin]


def test_file_scan_finished_unchanged_scan_does_not_rebuild_rows(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    records = {
        "flmsg": [FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)],
        "flamp": [],
        "bbs": [],
        "varac": [],
    }
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab._db_path = lambda: db_path
    tab._is_shutting_down = False
    tab._refresh_files_inflight = True
    tab._file_scan_start_ts = time.time()
    tab._files_snapshot_fp = MessageViewerTab._files_records_fingerprint(records)
    tab.files = records
    tab._scan_cache_loaded = True
    calls: dict[str, int] = {"meta": 0, "save": 0, "senders": 0, "project": 0, "varac": 0, "populate": 0, "sig": 0}
    tab._save_file_scan_cache_meta_only = lambda **_kwargs: calls.__setitem__("meta", calls["meta"] + 1)
    tab._save_file_scan_cache = lambda *_args, **_kwargs: calls.__setitem__("save", calls["save"] + 1)
    tab._update_fldigi_senders = lambda *_args, **_kwargs: calls.__setitem__("senders", calls["senders"] + 1)
    tab._project_message_files_to_observations = lambda *_args, **_kwargs: calls.__setitem__("project", calls["project"] + 1)
    tab._start_native_file_projection_write = lambda *_args, **_kwargs: None
    tab._refresh_varac_messages = lambda *_args, **_kwargs: calls.__setitem__("varac", calls["varac"] + 1)
    tab._populate_messages_table = lambda *_args, **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)
    tab._start_signature_verification = lambda *_args, **_kwargs: calls.__setitem__("sig", calls["sig"] + 1)

    MessageViewerTab._on_file_scan_finished(
        tab,
        {"records": records, "dir_mtimes": {str(tmp_path): msg_path.parent.stat().st_mtime}, "mode": "incremental"},
        False,
    )

    assert calls == {"meta": 1, "save": 0, "senders": 0, "project": 0, "varac": 0, "populate": 0, "sig": 1}
    assert tab._refresh_files_inflight is False


def test_file_scan_cache_preserves_source_identity_across_restart(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    source_id = "ingest-flmsg-fio-a"
    source_label = "FIO-A FLMSG"
    records = {
        "flmsg": [
            FileRecord(
                path=msg_path,
                origin="flmsg",
                size=stat.st_size,
                mtime=stat.st_mtime,
                source_id=source_id,
                source_label=source_label,
            )
        ],
        "flamp": [],
        "bbs": [],
        "varac": [],
    }

    writer = MessageViewerTab.__new__(MessageViewerTab)
    writer._db_path = lambda: db_path
    writer.watch_dirs = [
        {
            "origin": "flmsg",
            "path": str(tmp_path),
            "source_id": source_id,
            "source_label": source_label,
        }
    ]
    writer._multi_radio_message_path_entries = lambda: []
    writer.settings = {}
    MessageViewerTab._save_file_scan_cache(
        writer,
        records,
        dir_mtimes={str(tmp_path): tmp_path.stat().st_mtime},
    )

    reader = MessageViewerTab.__new__(MessageViewerTab)
    reader._db_path = lambda: db_path
    reader.watch_dirs = list(writer.watch_dirs)
    reader._multi_radio_message_path_entries = lambda: []
    reader.settings = {}
    reader.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    reader._scan_dir_mtime_cache = {}
    reader._scan_cache_saved_ts = 0.0
    reader._files_snapshot_fp = None

    assert MessageViewerTab._load_file_scan_cache(reader) is True
    loaded = reader.files["flmsg"][0]
    assert loaded.source_id == source_id
    assert loaded.source_label == source_label


def test_file_scan_cache_signature_changes_when_source_identity_changes(tmp_path) -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    left = [{"origin": "flmsg", "path": str(tmp_path), "source_id": "source-a", "source_label": "FIO-A FLMSG"}]
    right = [{"origin": "flmsg", "path": str(tmp_path), "source_id": "source-b", "source_label": "FIO-B FLMSG"}]

    assert MessageViewerTab._watch_dirs_signature(tab, left) != MessageViewerTab._watch_dirs_signature(tab, right)


def test_file_scan_finished_changed_scan_rebuilds_rows_and_cache(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    old_path = tmp_path / "old.k2s"
    new_path = tmp_path / "new.k2s"
    old_path.write_text("old", encoding="utf-8")
    new_path.write_text("new", encoding="utf-8")
    old_stat = old_path.stat()
    new_stat = new_path.stat()
    previous = {
        "flmsg": [FileRecord(path=old_path, origin="flmsg", size=old_stat.st_size, mtime=old_stat.st_mtime)],
        "flamp": [],
        "bbs": [],
        "varac": [],
    }
    records = {
        "flmsg": [FileRecord(path=new_path, origin="flmsg", size=new_stat.st_size, mtime=new_stat.st_mtime)],
        "flamp": [],
        "bbs": [],
        "varac": [],
    }
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab._db_path = lambda: db_path
    tab._is_shutting_down = False
    tab._refresh_files_inflight = True
    tab._file_scan_start_ts = time.time()
    tab._files_snapshot_fp = MessageViewerTab._files_records_fingerprint(previous)
    tab.files = previous
    tab._scan_cache_loaded = True
    calls: dict[str, int] = {"meta": 0, "save": 0, "senders": 0, "project": 0, "varac": 0, "populate": 0, "sig": 0}
    tab._save_file_scan_cache_meta_only = lambda **_kwargs: calls.__setitem__("meta", calls["meta"] + 1)
    tab._save_file_scan_cache = lambda *_args, **_kwargs: calls.__setitem__("save", calls["save"] + 1)
    tab._update_fldigi_senders = lambda *_args, **_kwargs: calls.__setitem__("senders", calls["senders"] + 1)
    tab._read_state_map = {}
    tab._load_read_state_map = lambda: {"loaded": ("READ", 1.0, 0)}
    tab._project_message_files_to_observations = lambda *_args, **_kwargs: calls.__setitem__("project", calls["project"] + 1)
    tab._start_native_file_projection_write = lambda *_args, **_kwargs: None
    tab._refresh_varac_messages = lambda *_args, **_kwargs: calls.__setitem__("varac", calls["varac"] + 1)
    tab._populate_messages_table = lambda *_args, **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)
    tab._start_signature_verification = lambda *_args, **_kwargs: calls.__setitem__("sig", calls["sig"] + 1)

    MessageViewerTab._on_file_scan_finished(
        tab,
        {"records": records, "dir_mtimes": {str(tmp_path): new_path.parent.stat().st_mtime}, "mode": "incremental"},
        False,
    )

    assert calls == {"meta": 0, "save": 1, "senders": 1, "project": 1, "varac": 1, "populate": 1, "sig": 1}
    assert tab.files == records
    assert tab._read_state_map == {"loaded": ("READ", 1.0, 0)}
    assert tab._files_snapshot_fp == MessageViewerTab._files_records_fingerprint(records)


def test_visible_message_check_skips_rebuild_when_sources_are_unchanged(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.files = {
        "flmsg": [FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)],
        "flamp": [],
        "bbs": [],
        "varac": [],
    }
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._visible_message_check_inflight = False
    tab._message_check_status_text = ""
    tab._last_visible_message_check_ts = 0.0
    tab._last_activation_refresh_ts = 0.0
    tab._has_active_view = True
    tab._messages_mode = "Inbox"
    tab._visible_check_interval_sec = 30
    calls: dict[str, int] = {"populate": 0, "pending": 0}
    tab._update_message_check_status = lambda: None
    tab._refresh_js8_messages = lambda **_kwargs: None
    tab._refresh_varac_messages = lambda **_kwargs: None
    tab._load_message_sources_from_local = lambda **_kwargs: None
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)
    tab._refresh_pending_backlog = lambda: calls.__setitem__("pending", calls["pending"] + 1)

    MessageViewerTab._on_visible_message_check_timer(tab)

    assert calls == {"populate": 0, "pending": 1}
    assert tab._message_check_status_text == "No new messages"


def test_visible_message_check_rebuilds_when_sources_change() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._visible_message_check_inflight = False
    tab._message_check_status_text = ""
    tab._last_visible_message_check_ts = 0.0
    tab._last_activation_refresh_ts = 0.0
    tab._has_active_view = True
    tab._messages_mode = "Inbox"
    tab._visible_check_interval_sec = 30
    calls: dict[str, int] = {"populate": 0, "pending": 0}
    tab._update_message_check_status = lambda: None
    tab._refresh_js8_messages = lambda **_kwargs: None
    tab._refresh_varac_messages = lambda **_kwargs: None

    def load_sources(**_kwargs):
        tab.commstat_messages = [_commstat_artifact(artifact_id=99, artifact_key="commstat:new")]

    tab._load_message_sources_from_local = load_sources
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)
    tab._refresh_pending_backlog = lambda: calls.__setitem__("pending", calls["pending"] + 1)

    MessageViewerTab._on_visible_message_check_timer(tab)

    assert calls == {"populate": 1, "pending": 1}
    assert tab._message_check_status_text == "1 new message"


def test_structured_projection_uses_checkpoints_even_when_refresh_is_forced() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    calls: dict[str, object] = {}
    tab._load_js8_from_local = lambda **_kwargs: None
    tab._load_spotter_from_db = lambda **_kwargs: None
    tab._load_sitrep_from_local = lambda **_kwargs: None
    tab._load_commstat_from_local = lambda **_kwargs: None
    tab._load_mesh_observations_from_store = lambda: None
    tab._start_native_message_projection_write = lambda **kwargs: calls.setdefault("native_force", kwargs.get("force"))
    tab._populate_messages_table = lambda **_kwargs: calls.setdefault("populate", True)

    MessageViewerTab._load_structured_message_projections(tab, force=True, rebuild=False)

    assert calls == {"native_force": False}


def test_message_sources_fingerprint_includes_js8_and_spotter_source_identity() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab.js8_messages = [
        JS8Message(
            msg_id=1,
            from_call="K7ETC",
            to_call="@MR08",
            msg_type="MSG",
            utc_str="2026-08-08 12:34:56",
            utc_ts=111.0,
            raw_text="same",
            decoded_text="same",
            state="NEW",
            source_key="js8:fio-a",
            source_id=44,
            source_radio_id="FIO-A",
            js8_instance_id="JS8-A",
            source_path="/tmp/a/inbox.db",
        )
    ]
    tab.spotter_messages = [
        SpotterMessage(
            spotter_id=2,
            from_call="K7ETC",
            to_call="@MR08",
            msg_type="F!307",
            utc_str="2026-08-08 12:34:56",
            utc_ts=111.0,
            raw_text="same",
            decoded_text="same",
            state="NEW",
            source_radio_id="FIO-A",
            js8_instance_id="JS8-A",
        )
    ]
    first = MessageViewerTab._message_sources_fingerprint(tab)

    tab.js8_messages[0].source_key = "js8:fio-b"
    second = MessageViewerTab._message_sources_fingerprint(tab)
    tab.js8_messages[0].source_key = "js8:fio-a"
    tab.js8_messages[0].source_id = 45
    third = MessageViewerTab._message_sources_fingerprint(tab)
    tab.js8_messages[0].source_id = 44
    tab.spotter_messages[0].source_radio_id = "FIO-B"
    fourth = MessageViewerTab._message_sources_fingerprint(tab)

    assert second != first
    assert third != first
    assert fourth != first


def test_refresh_now_skips_rebuild_when_sources_are_unchanged(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.files = {"flmsg": [rec], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("FLMSG", "READ", "K7ETC", "MR08", stat.st_mtime, "", "Fire", "flmsg", rec)]
    tab._message_check_status_text = ""
    tab._last_visible_message_check_ts = 0.0
    tab._last_activation_refresh_ts = 0.0
    tab._has_active_view = True
    tab._messages_mode = "Inbox"
    tab._visible_check_interval_sec = 30
    calls: dict[str, int] = {"files": 0, "populate": 0}
    tab._unfreeze_table = lambda: None
    tab._update_message_check_status = lambda: None
    tab._set_loading = lambda *_args, **_kwargs: None
    tab._refresh_files = lambda **_kwargs: calls.__setitem__("files", calls["files"] + 1)
    tab._refresh_js8_messages = lambda **_kwargs: None
    tab._refresh_varac_messages = lambda **_kwargs: None
    tab._has_stale_message_file_metadata = lambda: False
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    MessageViewerTab._on_refresh_now(tab)

    assert calls == {"files": 1, "populate": 0}
    assert tab._message_check_status_text == "No new messages"


def test_refresh_now_rebuilds_when_file_metadata_is_stale(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.files = {"flmsg": [rec], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("FLMSG", "READ", "K7ETC", "MR08", stat.st_mtime, "", "Fire", "flmsg", rec)]
    tab._message_check_status_text = ""
    tab._last_visible_message_check_ts = 0.0
    tab._last_activation_refresh_ts = 0.0
    tab._has_active_view = True
    tab._messages_mode = "Inbox"
    tab._visible_check_interval_sec = 30
    calls: dict[str, int] = {"populate": 0}
    tab._unfreeze_table = lambda: None
    tab._update_message_check_status = lambda: None
    tab._set_loading = lambda *_args, **_kwargs: None
    tab._refresh_files = lambda **_kwargs: None
    tab._refresh_js8_messages = lambda **_kwargs: None
    tab._refresh_varac_messages = lambda **_kwargs: None
    tab._has_stale_message_file_metadata = lambda: True
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    MessageViewerTab._on_refresh_now(tab)

    assert calls == {"populate": 1}


def test_refresh_now_rebuilds_when_sources_change() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 1.0, "", "Old", "commstat", object())]
    tab._message_check_status_text = ""
    tab._last_visible_message_check_ts = 0.0
    tab._last_activation_refresh_ts = 0.0
    tab._has_active_view = True
    tab._messages_mode = "Inbox"
    tab._visible_check_interval_sec = 30
    calls: dict[str, int] = {"populate": 0}
    tab._unfreeze_table = lambda: None
    tab._update_message_check_status = lambda: None
    tab._set_loading = lambda *_args, **_kwargs: None
    tab._refresh_files = lambda **_kwargs: None
    tab._refresh_js8_messages = lambda **_kwargs: None

    def refresh_varac(**_kwargs):
        tab.commstat_messages = [_commstat_artifact(artifact_id=99, artifact_key="commstat:new")]

    tab._refresh_varac_messages = refresh_varac
    tab._has_stale_message_file_metadata = lambda: False
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    MessageViewerTab._on_refresh_now(tab)

    assert calls == {"populate": 1}
    assert tab._message_check_status_text == "1 new message"


def test_activation_refresh_skips_rebuild_when_sources_are_unchanged(tmp_path) -> None:
    msg_path = tmp_path / "K7ETC-20260803-FIRE.k2s"
    msg_path.write_text("message", encoding="utf-8")
    stat = msg_path.stat()
    rec = FileRecord(path=msg_path, origin="flmsg", size=stat.st_size, mtime=stat.st_mtime)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab.files = {"flmsg": [rec], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("FLMSG", "READ", "K7ETC", "MR08", stat.st_mtime, "", "Fire", "flmsg", rec)]
    tab._last_file_refresh_ts = time.time()
    tab._file_refresh_interval_sec = 60.0
    tab._activation_refresh_pending = True
    calls: dict[str, int] = {"files": 0, "populate": 0, "pending": 0}
    tab._load_paths_lists = lambda: None
    tab._refresh_files = lambda **_kwargs: calls.__setitem__("files", calls["files"] + 1)
    tab._refresh_js8_messages = lambda **_kwargs: None
    tab._refresh_varac_messages = lambda **_kwargs: None
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)
    tab._refresh_pending_backlog = lambda: calls.__setitem__("pending", calls["pending"] + 1)
    tab._set_loading = lambda *_args, **_kwargs: None

    MessageViewerTab._run_activation_refresh(tab, force=False)

    assert calls == {"files": 0, "populate": 0, "pending": 1}
    assert tab._activation_refresh_pending is False


def test_activation_refresh_rebuilds_when_sources_change() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 1.0, "", "Old", "commstat", object())]
    tab._last_file_refresh_ts = time.time()
    tab._file_refresh_interval_sec = 60.0
    tab._activation_refresh_pending = True
    calls: dict[str, int] = {"populate": 0, "pending": 0}
    tab._load_paths_lists = lambda: None
    tab._refresh_files = lambda **_kwargs: None
    tab._refresh_js8_messages = lambda **_kwargs: None

    def refresh_varac(**_kwargs):
        tab.commstat_messages = [_commstat_artifact(artifact_id=99, artifact_key="commstat:new")]

    tab._refresh_varac_messages = refresh_varac
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)
    tab._refresh_pending_backlog = lambda: calls.__setitem__("pending", calls["pending"] + 1)
    tab._set_loading = lambda *_args, **_kwargs: None

    MessageViewerTab._run_activation_refresh(tab, force=False)

    assert calls == {"populate": 1, "pending": 1}
    assert tab._activation_refresh_pending is False


def _varac_message(**overrides) -> VarACMessage:
    base = {
        "msg_id": 1,
        "guid": "vmail-1",
        "source": "varac",
        "msg_type": "VMail",
        "from_call": "K7ETC",
        "to_call": "MR08",
        "subject": "Field report",
        "body": "",
        "ts": 10.0,
        "band": "20m",
        "freq_hz": None,
        "snr": None,
        "read_status": 0,
        "folder": "Inbox",
        "vmail_guid": "vmail-1",
        "flag_state": 0,
        "has_attachment": 0,
    }
    base.update(overrides)
    return VarACMessage(**base)


def test_refresh_varac_messages_skips_rebuild_when_sources_are_unchanged() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._last_varac_ingest_ts = time.time()
    tab._varac_ingest_interval_sec = 60.0
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = [_varac_message()]
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("VarAC", "NEW", "K7ETC", "MR08", 10.0, "", "Field report", "varac", tab.varac_messages[0])]
    calls = {"populate": 0}
    tab._load_varac_from_local = lambda **_kwargs: None
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    MessageViewerTab._refresh_varac_messages(tab, force=False, rebuild=True)

    assert calls == {"populate": 0}


def test_refresh_varac_messages_rebuilds_when_sources_change() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab._last_varac_ingest_ts = time.time()
    tab._varac_ingest_interval_sec = 60.0
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("VarAC", "NEW", "K7ETC", "MR08", 10.0, "", "Old", "varac", object())]
    calls = {"populate": 0}

    def load_varac(**_kwargs):
        tab.varac_messages = [_varac_message(subject="Field report updated")]

    tab._load_varac_from_local = load_varac
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    MessageViewerTab._refresh_varac_messages(tab, force=False, rebuild=True)

    assert calls == {"populate": 1}


def test_refresh_sitrep_messages_skips_rebuild_when_sources_are_unchanged() -> None:
    msg = _commstat_artifact(artifact_id=1, artifact_key="commstat:1")
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = [msg]
    tab._message_rows = [UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 10.0, "", "Field report", "commstat", msg)]
    calls = {"populate": 0}
    tab._load_sitrep_from_local = lambda **_kwargs: None
    tab._load_commstat_from_local = lambda **_kwargs: None
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    MessageViewerTab._refresh_sitrep_messages(tab, force=False, rebuild=True)

    assert calls == {"populate": 0}


def test_refresh_sitrep_messages_rebuilds_when_sources_change() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_shutting_down = False
    tab.files = {"flmsg": [], "flamp": [], "bbs": [], "varac": []}
    tab.js8_messages = []
    tab.spotter_messages = []
    tab.varac_messages = []
    tab.sitrep_messages = []
    tab.commstat_messages = []
    tab._message_rows = [UnifiedMessage("CommStat", "INFO", "K7ETC", "MR08", 10.0, "", "Old", "commstat", object())]
    calls = {"populate": 0}
    tab._load_sitrep_from_local = lambda **_kwargs: None

    def load_commstat(**_kwargs):
        tab.commstat_messages = [_commstat_artifact(artifact_id=2, artifact_key="commstat:2")]

    tab._load_commstat_from_local = load_commstat
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    MessageViewerTab._refresh_sitrep_messages(tab, force=False, rebuild=True)

    assert calls == {"populate": 1}


def test_local_projection_fingerprint_tracks_commstat_deletions(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE commstat_artifacts (
            id INTEGER PRIMARY KEY,
            updated_ts REAL,
            event_ts REAL,
            status_label TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE commstat_artifact_deletions (
            id INTEGER PRIMARY KEY,
            artifact_key TEXT,
            deleted_ts REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO commstat_artifacts (id, updated_ts, event_ts, status_label) VALUES (1, 10, 10, 'GREEN')"
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path

    before = MessageViewerTab._local_projection_fingerprint(
        tab, ("commstat_artifacts", "commstat_artifact_deletions")
    )

    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO commstat_artifact_deletions (artifact_key, deleted_ts) VALUES ('commstat:1', 20)"
    )
    conn.commit()
    conn.close()

    after = MessageViewerTab._local_projection_fingerprint(
        tab, ("commstat_artifacts", "commstat_artifact_deletions")
    )

    assert before != after


def test_local_projection_fingerprint_tracks_varac_read_status(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE varac_messages (
            id INTEGER PRIMARY KEY,
            ts REAL,
            read_status INTEGER,
            flag_state INTEGER DEFAULT 0,
            is_deleted INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        "INSERT INTO varac_messages (id, ts, read_status, flag_state, is_deleted) VALUES (1, 10, 0, 0, 0)"
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path

    before = MessageViewerTab._local_projection_fingerprint(tab, ("varac_messages",))

    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE varac_messages SET read_status=1 WHERE id=1")
    conn.commit()
    conn.close()

    after = MessageViewerTab._local_projection_fingerprint(tab, ("varac_messages",))

    assert before != after


def test_local_projection_fingerprint_tracks_text_state_changes(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE js8_messages (
            id INTEGER PRIMARY KEY,
            utc_ts REAL,
            state TEXT,
            read_ts REAL,
            flag_state INTEGER DEFAULT 0
        )
        """
    )
    conn.executemany(
        "INSERT INTO js8_messages (id, utc_ts, state, read_ts, flag_state) VALUES (?, ?, ?, ?, ?)",
        [
            (1, 10, "NEW", 0, 0),
            (2, 11, "READ", 1, 0),
        ],
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path

    before = MessageViewerTab._local_projection_fingerprint(tab, ("js8_messages",))

    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE js8_messages SET state='READ' WHERE id=1")
    conn.commit()
    conn.close()

    after = MessageViewerTab._local_projection_fingerprint(tab, ("js8_messages",))

    assert before != after


def test_local_projection_fingerprint_tracks_source_identity_changes(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE js8_messages (
            id INTEGER PRIMARY KEY,
            utc_ts REAL,
            state TEXT,
            source_key TEXT,
            source_id INTEGER,
            source_radio_id TEXT,
            js8_instance_id TEXT,
            source_path TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO js8_messages
            (id, utc_ts, state, source_key, source_id, source_radio_id, js8_instance_id, source_path)
        VALUES
            (1, 10, 'NEW', 'js8:fio-a', 44, 'FIO-A', 'JS8-A', '/tmp/a/inbox.db')
        """
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path

    before = MessageViewerTab._local_projection_fingerprint(tab, ("js8_messages",))

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        UPDATE js8_messages
           SET source_key='js8:fio-b',
               source_radio_id='FIO-B',
               js8_instance_id='JS8-B',
               source_path='/tmp/b/inbox.db'
         WHERE id=1
        """
    )
    conn.commit()
    conn.close()

    after = MessageViewerTab._local_projection_fingerprint(tab, ("js8_messages",))

    assert before != after


def test_local_projection_fingerprint_tracks_text_source_id_changes(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE file_messages (
            id INTEGER PRIMARY KEY,
            source_id TEXT,
            state TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO file_messages (id, source_id, state) VALUES (1, 'flmsg:fio-a', 'NEW')"
    )
    conn.commit()
    conn.close()

    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._db_path = lambda: db_path

    before = MessageViewerTab._local_projection_fingerprint(tab, ("file_messages",))

    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE file_messages SET source_id='flmsg:fio-b' WHERE id=1")
    conn.commit()
    conn.close()

    after = MessageViewerTab._local_projection_fingerprint(tab, ("file_messages",))

    assert before != after


def test_sqlite_identifier_rejects_non_table_names() -> None:
    assert MessageViewerTab._sqlite_identifier("commstat_artifacts") == '"commstat_artifacts"'

    try:
        MessageViewerTab._sqlite_identifier("commstat_artifacts; DROP TABLE x")
    except ValueError:
        pass
    else:
        raise AssertionError("invalid SQLite identifiers must be rejected")


def test_local_projection_loaders_skip_when_snapshot_is_unchanged(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "fio.db"
    sqlite3.connect(db_path).close()
    fingerprint = (("projection", "1", "1"),)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab.settings = {}
    tab._db_path = lambda: db_path
    tab._local_js8_db = lambda: db_path
    tab._local_projection_fingerprint = lambda _tables, db_path=None: fingerprint
    tab._js8_local_snapshot_fp = fingerprint
    tab._varac_local_snapshot_fp = fingerprint
    tab._spotter_local_snapshot_fp = fingerprint
    tab._sitrep_local_snapshot_fp = fingerprint
    tab._commstat_local_snapshot_fp = fingerprint
    tab.js8_messages = [object()]
    tab.varac_messages = [object()]
    tab.spotter_messages = [object()]
    tab.sitrep_messages = [object()]
    tab.commstat_messages = [object()]
    tab._ensure_local_js8_tables = lambda: None
    tab._ensure_spotter_table = lambda: None
    calls = {"populate": 0}
    tab._populate_messages_table = lambda **_kwargs: calls.__setitem__("populate", calls["populate"] + 1)

    def fail_connect(*_args, **_kwargs):
        raise AssertionError("unchanged local projection should not open the DB for a full load")

    monkeypatch.setattr("freqinout.gui.message_viewer_tab.sqlite3.connect", fail_connect)

    MessageViewerTab._load_js8_from_local(tab, force=False, rebuild=True)
    MessageViewerTab._load_varac_from_local(tab, force=False, rebuild=True)
    MessageViewerTab._load_spotter_from_db(tab, force=False, rebuild=True)
    MessageViewerTab._load_sitrep_from_local(tab, force=False, rebuild=True)
    MessageViewerTab._load_commstat_from_local(tab, force=False, rebuild=True)

    assert calls == {"populate": 0}


def test_js8_local_schema_ensure_runs_once_per_db_path(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "fio.db"
    sqlite3.connect(db_path).close()
    fingerprint = (("js8_messages", "1", "1"),)
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._local_js8_db = lambda: db_path
    tab._local_projection_fingerprint = lambda _tables, db_path=None: fingerprint
    tab._js8_local_snapshot_fp = fingerprint
    tab._local_js8_schema_ready_path = ""
    tab.js8_messages = [object()]
    tab._populate_messages_table = lambda **_kwargs: None
    calls = {"ensure": 0}
    tab._ensure_local_js8_tables = lambda: calls.__setitem__("ensure", calls["ensure"] + 1)

    def fail_connect(*_args, **_kwargs):
        raise AssertionError("unchanged JS8 projection should not open the DB for a full load")

    monkeypatch.setattr("freqinout.gui.message_viewer_tab.sqlite3.connect", fail_connect)

    MessageViewerTab._load_js8_from_local(tab, force=False, rebuild=True)
    MessageViewerTab._load_js8_from_local(tab, force=False, rebuild=True)

    assert calls == {"ensure": 1}


def test_message_map_context_uses_current_row_group_and_topic() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    row = UnifiedMessage(
        msg_type="SitRep",
        status="NEW",
        from_call="K7ETC",
        to_call="@MR08",
        rcv_ts=0.0,
        rcv_display="",
        title="Wildfire",
        origin="spotter",
        payload=object(),
        topics=("Fire", "Comms"),
    )
    tab._current_message_table_row = lambda: row
    tab._message_group_filter_active = lambda: False
    tab._selected_message_groups = lambda: set()

    assert MessageViewerTab._message_map_context(tab) == {
        "group_filter": "MR08",
        "topic_filter": "Fire",
    }


def test_message_map_context_falls_back_to_single_group_filter() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._current_message_table_row = lambda: None
    tab._message_group_filter_active = lambda: True
    tab._selected_message_groups = lambda: {"@MAGNET"}

    assert MessageViewerTab._message_map_context(tab) == {
        "group_filter": "MAGNET",
        "topic_filter": "",
    }


def test_message_view_hf_reports_map_action_routes_to_map_with_filters(monkeypatch) -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    opened: list[dict[str, str]] = []
    row = UnifiedMessage(
        msg_type="Spotter",
        status="NEW",
        from_call="K7ETC",
        to_call="@MR08",
        rcv_ts=0.0,
        rcv_display="",
        title="Wildfire",
        origin="spotter",
        payload=object(),
        topics=("Fire", "Comms"),
    )
    tab._current_message_table_row = lambda: row
    tab._message_group_filter_active = lambda: False
    tab._selected_message_groups = lambda: set()
    host = SimpleNamespace(open_spotter_map=lambda **kwargs: opened.append(kwargs))
    monkeypatch.setattr(MessageViewerTab, "window", lambda _self: host)

    MessageViewerTab._request_spotter_map_view(tab)

    assert opened == [{"group_filter": "MR08", "topic_filter": "Fire"}]


def test_commstat_message_detail_uses_reported_for_and_reported_by_labels() -> None:
    source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    block = source[source.index("def _load_commstat_content") : source.index("def _mark_varac_read")]

    assert '("Reported By", msg.from_call)' in block
    assert '("Reported For", reported_for or msg.scope)' in block
    assert '("Report Scope", msg.scope)' in block
    assert '("State", msg.state_code)' not in block
    assert '("Grid", msg.grid)' not in block
