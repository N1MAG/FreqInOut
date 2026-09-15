from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication
import pytest

from freqinout.core.compose_guidance import (
    ComposeLastHeard,
    ComposePathEvidence,
    ComposePeerSchedule,
    ComposeRadioOption,
    recommend_compose_send_path,
)
from freqinout.core.js8_expect_store import (
    ExpectEntryExistsError,
    list_expect_entries,
    save_expect_entry,
)
from freqinout.gui import message_viewer_tab as viewer_module
from freqinout.radio_interface.js8_api_client import JS8ApiEndpoint


def test_peer_schedule_drives_compose_send_recommendation() -> None:
    rec = recommend_compose_send_path(
        [ComposeRadioOption(1, "FIO-A"), ComposeRadioOption(2, "FIO-B")],
        peer_schedule=ComposePeerSchedule(
            callsign="KC7WOK",
            band="40M",
            frequency_mhz=7.078,
            mode="USB",
            minutes_to_end=21,
        ),
        last_heard=ComposeLastHeard(radio_id=2, band="20M", source="JS8Call", age_label="8m ago"),
        selected_radio_id=1,
    )

    assert rec.radio_id == 1
    assert rec.frequency_mhz == 7.078
    assert rec.band == "40M"
    assert rec.mode == "USB"
    assert rec.confidence == "high"
    assert rec.tune_available is True


def test_last_heard_radio_is_used_when_no_peer_schedule() -> None:
    rec = recommend_compose_send_path(
        [ComposeRadioOption(1, "FIO-A"), ComposeRadioOption(2, "FIO-B")],
        last_heard=ComposeLastHeard(radio_id=2, band="20M", source="JS8Spotter", age_label="4m ago"),
        selected_radio_id=1,
    )

    assert rec.radio_id == 2
    assert rec.radio_label == "FIO-B"
    assert rec.band == "20M"
    assert rec.confidence == "medium"
    assert rec.tune_available is False


def test_direct_path_evidence_beats_last_heard_hint() -> None:
    rec = recommend_compose_send_path(
        [ComposeRadioOption(1, "FIO-A")],
        path_evidence=ComposePathEvidence(kind="direct", band="40M", source="JS8Call", age_label="12m ago"),
        last_heard=ComposeLastHeard(radio_id=1, band="20M", source="JS8Spotter", age_label="4m ago"),
        selected_radio_id=1,
    )

    assert rec.radio_id == 1
    assert rec.band == "40M"
    assert rec.path_kind == "direct"
    assert rec.confidence == "high"


def test_direct_path_evidence_selects_radio_that_saw_contact() -> None:
    rec = recommend_compose_send_path(
        [ComposeRadioOption(1, "FIO-A"), ComposeRadioOption(2, "FIO-B")],
        path_evidence=ComposePathEvidence(kind="direct", radio_id=2, band="20M", source="JS8Call"),
        selected_radio_id=1,
    )

    assert rec.radio_id == 2
    assert rec.radio_label == "FIO-B"
    assert rec.path_kind == "direct"


def test_relay_path_evidence_is_operator_visible() -> None:
    rec = recommend_compose_send_path(
        [ComposeRadioOption(1, "FIO-A")],
        path_evidence=ComposePathEvidence(kind="relay", relay="N7CWR", band="20M", source="JS8Call"),
        selected_radio_id=1,
    )

    assert rec.path_kind == "relay"
    assert rec.relay == "N7CWR"
    assert "N7CWR" in rec.reason


def test_selected_radio_fallback_when_no_route_evidence() -> None:
    rec = recommend_compose_send_path(
        [ComposeRadioOption(1, "FIO-A"), ComposeRadioOption(2, "FIO-B")],
        selected_radio_id=2,
    )

    assert rec.radio_id == 2
    assert rec.radio_label == "FIO-B"
    assert rec.confidence == "low"


def test_map_to_compose_uses_intent_handoff() -> None:
    source = open("freqinout/gui/stations_map_tab.py", encoding="utf-8").read()

    assert "prefill_compose_intent" in source
    assert '"recipient_callsign": callsign' in source


def test_compose_send_checks_peer_schedule_guidance_before_transmit() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    send_start = source.index("    def _send_compose_js8_spotter")
    send_end = source.index("    def _save_compose_js8_expect", send_start)
    send_block = source[send_start:send_end]
    assert "_compose_confirm_peer_schedule_before_send" in send_block
    assert "_start_compose_js8_send_worker" in send_block
    assert "_compose_send_inflight" in send_block

    confirm_start = source.index("    def _compose_confirm_peer_schedule_before_send")
    confirm_end = source.index("    def prefill_compose_intent", confirm_start)
    confirm_block = source[confirm_start:confirm_end]
    assert "Tune Now" in confirm_block
    assert "Send Anyway" in confirm_block


def test_compose_guidance_rail_uses_short_visible_text_and_tooltip_detail() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "self.compose_guidance_label.setWordWrap(False)" in source
    assert "self.compose_guidance_label.setMaximumHeight(single_line_label_height(self.compose_guidance_label))" in source
    assert "def _compose_send_guidance_summary" in source
    assert 'return f"Using {radio_label}"' in source
    assert 'return f"Use {radio_label}: last heard on {band}"' in source
    assert 'return f"Tune {radio_label}: {recommendation.frequency_mhz:.3f} MHz{band_suffix}"' in source
    assert "def _compose_send_guidance_tooltip" in source
    assert "self.compose_guidance_label.setToolTip(tooltip)" in source
    assert "self.compose_guidance_row_widget.setToolTip(tooltip)" in source


def test_compose_visible_radio_status_uses_short_name() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "def _compose_radio_target_short_label" in source
    send_start = source.index("    def _send_compose_js8_spotter")
    send_end = source.index("    def _save_compose_js8_expect", send_start)
    send_block = source[send_start:send_end]
    assert '"radio_label": radio_short_label' in send_block
    assert 'f"Queued {label} message via {radio_label}: {command}"' in source
    assert "via {radio_target.label}" not in source

    stage_start = source.index("    def _stage_compose_files")
    stage_end = source.index("    @staticmethod", stage_start)
    stage_block = source[stage_start:stage_end]
    assert "_compose_stage_request_snapshot" in stage_block
    assert "_ComposeStageWorker" in stage_block
    assert '"Staging compose files…"' in stage_block
    assert "write_text" not in stage_block
    assert "clearsign_file" not in stage_block
    assert '"Draft retained. Reset it when you are ready for the next message."' in source

    # Managed BBS choices are station locations in Slice 2, not duplicated
    # radio-prefixed targets. Radio-specific compose/send status above still
    # uses the compact radio label.
    assert '"id": f"location:{location.location_id}"' in source
    assert '"label": location.name' in source


def test_compose_blocks_manual_self_send() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    send_start = source.index("    def _send_compose_js8_spotter")
    send_end = source.index("    def _save_compose_js8_expect", send_start)
    send_block = source[send_start:send_end]
    assert "FIO will not send a message to your own callsign" in send_block
    assert "_compose_base_callsign(typed_target)" in send_block


def test_fast_light_compose_preview_explains_delimiter_and_blank_suffix() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "def _compose_fastlight_delimiter_guidance" in source
    assert "Standard blank form uses .b2s" in source
    assert "Fast Light Format" in source
    assert "if not (js8_mode or spotter_mode or commstat_mode):" in source


def test_plain_js8_compose_is_first_class_guarded_send_mode() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert '"JS8Call"' in source
    assert 'self._compose_mode = "js8"' in source
    assert "def _compose_plain_js8_command" in source
    assert "Directed Message" in source
    assert "FIO will not send a message to your own callsign" in source
    worker_start = source.index("class _ComposeJs8SendWorker")
    worker_end = source.index("class _ComposeCatalogDiscoveryWorker", worker_start)
    worker_block = source[worker_start:worker_end]
    assert "send_js8_message_guarded(" in worker_block
    assert "clear_selected_target=True" in worker_block
    send_start = source.index("    def _send_compose_js8_spotter")
    send_end = source.index("    def _save_compose_js8_expect", send_start)
    send_block = source[send_start:send_end]
    assert "send_js8_message_guarded(" not in send_block
    assert "_start_compose_js8_send_worker" in send_block


def test_compose_js8_worker_emits_guarded_result_without_gui_send_call(monkeypatch) -> None:
    """The worker owns guarded API work and returns a queued result payload."""

    app = QApplication.instance() or QApplication([])
    endpoint = JS8ApiEndpoint("127.0.0.1", 2442)
    calls = []
    expected = SimpleNamespace(sent=True, detail="queued")

    monkeypatch.setattr(
        viewer_module.JS8ApiClientRegistry,
        "get",
        lambda endpoint, **kwargs: calls.append(("client", endpoint, kwargs)) or object(),
    )
    monkeypatch.setattr(
        viewer_module,
        "send_js8_message_guarded",
        lambda client, command, **kwargs: calls.append(("guarded", client, command, kwargs)) or expected,
    )
    worker = viewer_module._ComposeJs8SendWorker(
        endpoint=endpoint,
        command="GROUP CHECK",
        generation=4,
        allow_uncertain_target_state=False,
    )
    payloads = []
    worker.finished.connect(payloads.append)
    worker.run()
    app.processEvents()

    assert calls[0][0] == "client"
    assert calls[1][0] == "guarded"
    assert calls[1][2] == "GROUP CHECK"
    assert calls[1][3]["clear_selected_target"] is True
    assert payloads[0]["generation"] == 4
    assert payloads[0]["result"] is expected


def test_compose_mode_rows_are_wrapped_for_clean_visibility() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "self.compose_form_row_widget = QWidget()" in source
    assert "self.compose_header_row_widget = QWidget()" in source
    assert "self.compose_form_row_widget.setVisible(nbems_mode or spotter_mode)" in source
    assert "self.compose_header_row_widget.setVisible(nbems_mode)" in source
    assert "self.compose_setup_box = setup_box" in source
    assert "self.compose_js8_plain_row_widget.setMinimumHeight(120)" in source
    assert "self.compose_js8_plain_text_edit.setMaximumHeight(js8_text_h)" in source
    assert "self.compose_commstat_row_widget.setMinimumHeight(240)" in source
    assert "self.compose_rf_fields_stack = QStackedWidget()" in source
    assert "self.compose_js8_plain_scroll = QScrollArea()" in source
    assert "self.compose_commstat_scroll = QScrollArea()" in source
    assert "self.compose_rf_fields_stack.addWidget(self.compose_js8_plain_scroll)" in source
    assert "self.compose_rf_fields_stack.addWidget(self.compose_commstat_scroll)" in source
    assert "self._set_compose_fixed_width(self.compose_radio_combo, floor=160, ceiling=260)" in source
    assert "target_h = max(86, target_h)" in source
    assert "def _open_compose_workbench_dialog" in source
    assert 'self.compose_workbench_btn = QPushButton("Open Full Compose Workbench")' in source
    assert 'self.compose_inline_reset_btn = QPushButton("Reset")' in source
    assert 'reset_btn = QPushButton("Reset Draft")' in source
    assert "setup_scroll.setMaximumHeight(16777215)" in source
    assert "setup_scroll.setMinimumHeight(min(target_h, max(120, viewport_height // 3)))" in source
    assert "self.compose_operating_group_combo = QComboBox()" in source
    assert "def _refresh_compose_operating_group_options" in source
    assert "self.compose_js8_auth_row_widget = QWidget()" in source
    assert "js8_auth_row.addWidget(self.compose_js8_auth_key_combo, 1)" in source
    assert "js8_plain_layout.addWidget(self.compose_js8_plain_kind_chip_container, 0, 1)" in source
    assert "labels={\"Directed Message\": \"Directed\"}" in source
    assert "row = idx // 2" in source
    assert "col = (idx % 2) * 2" in source
    assert "def _refresh_compose_layout_geometry_if_needed" in source
    assert "self._compose_layout_signature = signature" in source
    assert "self._compose_layout_refresh_pending = True" in source
    assert "QTimer.singleShot(0, self._run_pending_compose_layout_geometry_refresh)" in source
    assert "def _run_pending_compose_layout_geometry_refresh" in source


def test_compose_reset_clears_only_active_mode_but_preserves_radio_choice() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()
    reset_block = source[
        source.index("def _reset_compose_draft")
        : source.index("def _open_compose_source_folder")
    ]

    assert "compose_radio_combo" not in reset_block
    assert "mode = str(getattr(self, \"_compose_mode\", \"nbems\") or \"nbems\")" in reset_block
    assert "self._compose_mode_drafts.pop(mode, None)" in reset_block
    assert "self._compose_form_draft_mode_keys.pop(mode, set())" in reset_block
    assert "if mode == \"nbems\":" in reset_block
    assert "elif mode == \"js8\":" in reset_block
    assert "elif mode == \"spotter\":" in reset_block
    assert "elif mode == \"commstat_rf\":" in reset_block
    assert "self._compose_form_draft_values.clear()" not in reset_block


def test_compose_form_fields_use_dense_short_field_grid() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "layout = QGridLayout(container)" in source
    assert "is_long_field =" in source
    assert "layout.addWidget(field_wrap, grid_row, 0, 1, 2)" in source
    assert "layout.addWidget(field_wrap, grid_row, grid_col)" in source
    assert "splitter.setStretchFactor(0, 5)" in source
    assert "splitter.setStretchFactor(1, 2)" in source


def test_compose_rf_modes_use_vertical_panels_and_target_completion() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "viewport_width < (920 if in_workbench else int(self._responsive_compact_width))" in source
    assert "self.compose_body_splitter = body_splitter" in source
    assert "def _compose_sidebar_enabled" in source
    assert "desired_body = Qt.Horizontal if compose_sidebar else Qt.Vertical" in source
    assert "desired = Qt.Vertical" in source
    assert "def _compose_target_completion_values" in source
    assert "def _install_compose_target_completers" in source
    assert 'for widget_name in ("compose_js8_target_edit", "compose_commstat_target_edit")' in source
    assert "QCompleter(values, widget)" in source
    assert "return sorted(values)" in source
    assert 'setPlaceholderText("GROUP or CALLSIGN")' in source
    assert "known_groups: set[str] = set()" in source
    assert "def _compose_rf_target_text" in source


def test_compose_workbench_is_screen_bounded_and_resize_safe() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "class _ResponsiveComposeWorkbenchDialog(QDialog):" in source
    assert "QTimer.singleShot(0, self._on_compose_resize)" in source
    assert "def _compose_layout_viewport" in source
    assert "def _compose_workbench_available_geometry" in source
    assert "dialog = _ResponsiveComposeWorkbenchDialog(self, refresh_for_workbench_resize)" in source
    assert "dialog.setMaximumSize(usable_width, usable_height)" in source
    assert "dialog.resize(min(1280, usable_width), min(820, usable_height))" in source
    assert "root.insertWidget(index, widget" in source


def test_compose_mode_bodies_use_internal_scroll_before_fixed_height() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "self.compose_commstat_scroll.setMinimumHeight(0)" in source
    assert "self.compose_field_scroll.setMinimumHeight(0)" in source
    assert "self.compose_field_box.setMinimumHeight(180)" in source
    assert "self.compose_field_box.setMinimumHeight(460 if brevity_enabled else 340)" not in source


def test_compose_payload_preview_keeps_discovery_out_of_keystroke_path() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()
    preview = source[source.index("    def _update_compose_preview"): source.index("    def _send_compose_js8_spotter")]

    for helper in (
        "_refresh_compose_radio_targets",
        "_refresh_compose_message_folder_options",
        "_install_compose_target_completers",
        "_refresh_compose_bbs_location_targets",
        "_refresh_compose_signing_keys",
        "_refresh_compose_js8_auth_keys",
        "_compose_refresh_send_guidance",
    ):
        assert helper not in preview
    # Preview is a hot keystroke path: it may format cached state, but it must
    # not plan destinations (which probes the filesystem) or touch DB/socket
    # APIs. Those operations belong to explicit workers/actions.
    assert "_compose_destination_plans" not in preview
    assert "Path(" not in preview
    assert "sqlite3.connect" not in preview
    assert "send_js8_message_guarded" not in preview
    assert "def _refresh_compose_setup_discovery" in source
    assert "def _on_compose_rf_target_changed" in source
    assert "QTimer.singleShot(180, self._run_pending_compose_target_discovery)" in source


def test_compose_expect_save_is_create_only_and_preserves_existing_policy(tmp_path) -> None:
    db_path = tmp_path / "expect.sqlite"
    entry = {
        "expect_key": "F!103",
        "response_text": "@MAGNET F!103 ORIGINAL",
        "source_radio_id": "",
        "source_scope": "all",
        "js8_instance_id": "",
        "enabled": False,
        "auto_reply_enabled": False,
        "unattended_auto_reply_enabled": False,
        "import_source": "fio-compose-js8spotter",
        "create_only": True,
    }

    first = save_expect_entry(entry, db_path=db_path)
    assert first.created is True

    with pytest.raises(ExpectEntryExistsError):
        save_expect_entry({**entry, "response_text": "REPLACEMENT", "enabled": True}, db_path=db_path)

    rows = list_expect_entries(db_path=db_path, enabled_only=False, expect_key="F!103")
    assert len(rows) == 1
    assert rows[0]["response_text"] == "@MAGNET F!103 ORIGINAL"
    assert rows[0]["enabled"] == 0


def test_nbems_compose_uses_sidebar_and_popout_body_splitter() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "body_splitter = QSplitter(Qt.Vertical)" in source
    assert "self.compose_setup_scroll = QScrollArea()" in source
    assert "self.compose_setup_scroll.setWidget(setup_box)" in source
    assert "body_splitter.addWidget(self.compose_setup_scroll)" in source
    assert "body_splitter.addWidget(splitter)" in source
    assert 'for widget_name in ("compose_type_box", "compose_body_splitter", "compose_output_box")' in source
    assert "setup_box.setMinimumWidth(sidebar_w)" in source
    assert "setup_scroll.setMaximumWidth(sidebar_w)" in source
    assert "setup_box.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Maximum)" in source
    assert "self.compose_splitter.setOrientation(Qt.Vertical)" in source
    assert "desired_body = Qt.Horizontal if compose_sidebar else Qt.Vertical" in source
    assert "self.compose_body_splitter.orientation() != desired_body" in source
    assert "self.compose_body_splitter.setOrientation(desired_body)" in source
    assert 'mode not in {"nbems", "js8", "spotter", "commstat_rf"}' in source
    assert 'if mode == "spotter":' in source
    assert 'elif mode == "commstat_rf":' in source


def test_rf_compose_modes_do_not_render_nbems_form_preview() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "if js8_mode:" in source
    assert "elif commstat_mode:" in source
    assert "RF Payload Preview" in source
    assert "preview_body_html = preview_html" in source
    assert 'self.compose_field_box.setTitle("JS8 Message")' in source
    assert 'self.compose_field_box.setTitle("CommStat StatRep")' in source
    assert 'spotter_selected = spotter_mode and self._compose_template_kind == "spotter"' in source
    assert "self.compose_rf_fields_stack.setCurrentWidget(self.compose_js8_plain_scroll)" in source
    assert "self.compose_rf_fields_stack.setCurrentWidget(self.compose_commstat_scroll)" in source
    assert "self.compose_field_scroll.setVisible(not (js8_mode or commstat_mode))" in source
    assert "self._refresh_compose_layout_geometry_if_needed()" in source


def test_js8_compose_text_box_stays_short_and_label_top_aligned() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()
    block = source[
        source.index("self.compose_js8_plain_row_widget = QWidget()")
        : source.index("self.compose_commstat_row_widget = QWidget()")
    ]

    assert "self.compose_js8_plain_row_widget.setMinimumHeight(120)" in block
    assert "self.compose_js8_plain_row_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)" in block
    assert "self.compose_js8_plain_text_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)" in block
    assert "js8_text_h = max(" in block
    assert "self.compose_js8_plain_text_edit.setMaximumHeight(js8_text_h)" in block


def test_compose_commstat_catalogs_are_cached_for_preview_performance() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "_compose_commstat_brevity_options_cache_key" in source
    assert "_compose_commstat_brevity_options_cache" in source
    assert "_compose_commstat_brevity_catalogs_cache_key" in source
    assert "_compose_commstat_brevity_catalogs_cache" in source
    assert "cache_key = tuple(str(path) for path in self._compose_commstat_brevity_catalog_dirs())" in source
    assert "return list(self._compose_commstat_brevity_options_cache)" in source
    assert "return list(getattr(self, \"_compose_commstat_brevity_catalogs_cache\", []))" in source


def test_operating_group_form_family_drives_fast_light_compose_defaults() -> None:
    message_source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()
    settings_source = open("freqinout/gui/settings_tab.py", encoding="utf-8").read()

    assert "resolve_fastlight_form_family" in message_source
    assert "def _compose_fastlight_preferred_form_family" in message_source
    assert '"fastlight_form_family"' in settings_source
    assert "Preferred Forms" in settings_source


def test_compose_form_drafts_survive_mode_switch_rebuilds() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "self._compose_form_draft_values: Dict[str, Dict[str, str]] = {}" in source
    assert "def _store_compose_form_draft(self) -> None:" in source
    assert "self._compose_form_draft_values[form_key] = self._compose_field_values()" in source
    assert "self._store_compose_form_draft()" in source
    assert "dict(self._compose_form_draft_values.get(form_identity, {}))" in source
    assert "self._compose_form_draft_mode_keys.setdefault(mode, set()).add(form_key)" in source
    assert "def _store_compose_mode_draft" in source
    assert "def _restore_compose_mode_draft" in source
