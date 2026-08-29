from freqinout.core.compose_guidance import (
    ComposeLastHeard,
    ComposePathEvidence,
    ComposePeerSchedule,
    ComposeRadioOption,
    recommend_compose_send_path,
)


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
    assert "send_js8_message_guarded" in send_block

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
    assert "Sent {label} message via {radio_short_label}" in send_block
    assert "via {radio_target.label}" not in send_block

    stage_start = source.index("    def _stage_compose_files")
    stage_end = source.index("    @staticmethod", stage_start)
    stage_block = source[stage_start:stage_end]
    assert "for {radio_short_label}" in stage_block
    assert "for {radio_target.label}" not in stage_block

    assert '"label": f"{radio_short_label}: {label}"' in source
    assert '"radio_label": radio_short_label' in source
    assert '"full_radio_label": radio_target.label' in source


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
    assert "send_js8_message_guarded(client, command" in source
    assert "clear_selected_target=True" in source


def test_compose_mode_rows_are_wrapped_for_clean_visibility() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert "self.compose_form_row_widget = QWidget()" in source
    assert "self.compose_header_row_widget = QWidget()" in source
    assert "self.compose_form_row_widget.setVisible(nbems_mode or spotter_mode)" in source
    assert "self.compose_header_row_widget.setVisible(nbems_mode)" in source
    assert "self.compose_setup_box = setup_box" in source
    assert "self.compose_js8_plain_row_widget.setMinimumHeight(170)" in source
    assert "self.compose_js8_plain_text_edit.setMinimumHeight(132)" in source
    assert "self.compose_commstat_row_widget.setMinimumHeight(240)" in source
    assert "self.compose_rf_fields_stack = QStackedWidget()" in source
    assert "self.compose_js8_plain_scroll = QScrollArea()" in source
    assert "self.compose_commstat_scroll = QScrollArea()" in source
    assert "self.compose_rf_fields_stack.addWidget(self.compose_js8_plain_scroll)" in source
    assert "self.compose_rf_fields_stack.addWidget(self.compose_commstat_scroll)" in source
    assert "self._set_compose_fixed_width(self.compose_radio_combo, floor=160, ceiling=260)" in source
    assert "Visibility wins over compactness" in source
    assert "if target_h > cap_h:" in source
    assert "def _open_compose_workbench_dialog" in source
    assert 'self.compose_workbench_btn = QPushButton("Open Full Compose Workbench")' in source
    assert "setup_scroll.setMaximumHeight(cap_h)" in source
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

    assert "compact = False if in_workbench else self._messages_responsive_mode_for_width" in source
    assert "self.compose_body_splitter = body_splitter" in source
    assert 'compose_sidebar = mode in {"nbems", "spotter", "commstat_rf"} and not compact' in source
    assert "desired_body = Qt.Horizontal if compose_sidebar else Qt.Vertical" in source
    assert "desired = Qt.Vertical if (compact or compose_sidebar) else Qt.Horizontal" in source
    assert "def _compose_target_completion_values" in source
    assert "def _install_compose_target_completers" in source
    assert 'for widget_name in ("compose_js8_target_edit", "compose_commstat_target_edit")' in source
    assert "QCompleter(values, widget)" in source
    assert "return sorted(values)" in source
    assert 'setPlaceholderText("GROUP or CALLSIGN")' in source
    assert "known_groups: set[str] = set()" in source
    assert "def _compose_rf_target_text" in source


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
    assert "setup_box.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)" in source
    assert "self.compose_splitter.setOrientation(Qt.Vertical if (compact or compose_sidebar) else Qt.Horizontal)" in source
    assert "self.compose_body_splitter.setOrientation(Qt.Horizontal if compose_sidebar else Qt.Vertical)" in source
    assert 'compose_mode in {"nbems", "spotter", "commstat_rf"}' in source
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
    assert "self._compose_form_draft_values.clear()" in source
