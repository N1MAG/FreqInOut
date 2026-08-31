from __future__ import annotations

import datetime as dt
from pathlib import Path

from freqinout.core.nbems_compose import (
    ComposeFieldOption,
    build_compose_filename,
    build_signed_filename,
    compose_message_relative_path,
    discover_compose_message_folders,
    parse_compose_template_fields,
    plan_compose_destinations,
    resolve_fastlight_form_family,
    resolve_fastlight_filename_policy,
    resolve_compose_message_folder,
    resolve_flamp_transmit_dir,
    sanitize_report_name,
    serialize_custom_form_message,
    serialize_standard_blank_message,
    suggest_field_value,
)


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_report_name_sanitization_removes_spaces_and_hyphens() -> None:
    assert sanitize_report_name("ROAD-CLOSURE") == "ROADCLOSURE"
    assert sanitize_report_name("ROAD CLOSURE") == "ROADCLOSURE"
    assert sanitize_report_name("Road Closure 24") == "RoadClosure24"


def test_compose_filename_respects_extension_and_compact_report_name() -> None:
    when = dt.datetime(2026, 4, 23, 13, 25, tzinfo=dt.timezone.utc)
    assert build_compose_filename("W8UFO", "TN", "RR", when, "ROAD-CLOSURE", extension=".b2s") == (
        "W8UFO-TN-RR-20260423-1325z-ROADCLOSURE.b2s"
    )
    assert build_compose_filename("W8UFO", "TN", "RR", when, "Weekly Snapshot", extension=".k2s") == (
        "W8UFO-TN-RR-20260423-1325z-WeeklySnapshot.k2s"
    )


def test_signed_filename_uses_sig_suffix_before_payload_extension() -> None:
    assert build_signed_filename("W8UFO-TN-RR-20260423-1325z-Report.k2s") == (
        "W8UFO-TN-RR-20260423-1325z-Report-sig.k2s"
    )
    assert build_signed_filename("W8UFO-TN-RR-20260423-1325z-Report.b2s") == (
        "W8UFO-TN-RR-20260423-1325z-Report-sig.b2s"
    )


def test_fastlight_filename_policy_uses_magnet_underscore_defaults() -> None:
    when = dt.datetime(2026, 4, 23, 13, 25, tzinfo=dt.timezone.utc)
    policy = resolve_fastlight_filename_policy([], "MAGNET")

    name = build_compose_filename(
        "N1MAG",
        "CO",
        "RR",
        when,
        "Road Closure",
        extension=".k2s",
        filename_policy=policy,
        operating_group="MAGNET",
    )

    assert name == "N1MAG_CO_RR_20260423-1325z_RoadClosure.k2s"
    assert build_signed_filename(name, filename_policy=policy, operating_group="MAGNET") == (
        "N1MAG_CO_RR_20260423-1325z_RoadClosure.sig.k2s"
    )


def test_fastlight_filename_policy_uses_amrron_hyphen_defaults() -> None:
    when = dt.datetime(2026, 4, 23, 13, 25, tzinfo=dt.timezone.utc)
    policy = resolve_fastlight_filename_policy([], "AMRRON")

    name = build_compose_filename(
        "W8UFO",
        "TN",
        "RR",
        when,
        "Weekly Snapshot",
        extension=".b2s",
        filename_policy=policy,
        operating_group="AMRRON",
    )

    assert name == "W8UFO-TN-RR-20260423-1325z-WeeklySnapshot.b2s"
    assert build_signed_filename(name, filename_policy=policy, operating_group="AMRRON") == (
        "W8UFO-TN-RR-20260423-1325z-WeeklySnapshot-sig.b2s"
    )


def test_fastlight_form_family_resolves_from_operating_group() -> None:
    groups = [
        {"group": "MAGNET", "fastlight_form_family": "ICS"},
        {"group": "AMRRON", "fastlight_form_family": "group_default"},
    ]

    assert resolve_fastlight_form_family(groups, "MAGNET") == "ICS"
    assert resolve_fastlight_form_family(groups, "@MAGNET") == "ICS"
    assert resolve_fastlight_form_family(groups, "AMRRON") == ""
    assert resolve_fastlight_form_family(groups, "UNKNOWN") == ""


def test_fastlight_filename_policy_uses_saved_group_overrides_for_destinations(tmp_path: Path) -> None:
    when = dt.datetime(2026, 4, 23, 13, 25, tzinfo=dt.timezone.utc)
    policy = resolve_fastlight_filename_policy(
        [
            {
                "group": "FIELD",
                "fastlight_filename_delimiter": "underscore",
                "fastlight_signed_suffix": "dot_sig",
            }
        ],
        "FIELD",
    )
    base_name = build_compose_filename(
        "K7FIO",
        "UT",
        "PP",
        when,
        "Ops Check",
        extension=".k2s",
        filename_policy=policy,
        operating_group="FIELD",
    )
    flamp_dir = tmp_path / "flamp-tx"
    flamp_dir.mkdir()

    plans = plan_compose_destinations(
        base_name,
        send_target="FLAmp",
        varac_target="None",
        flamp_dir=str(flamp_dir),
        sign_flamp_copy=True,
        filename_policy=policy,
        operating_group="FIELD",
    )

    assert base_name == "K7FIO_UT_PP_20260423-1325z_OpsCheck.k2s"
    assert plans[0].path.endswith("K7FIO_UT_PP_20260423-1325z_OpsCheck.sig.k2s")


def test_plan_compose_destinations_adds_varac_outbox_and_bbs_targets(tmp_path: Path) -> None:
    flmsg_dir = tmp_path / "flmsg"
    flamp_dir = tmp_path / "flamp"
    varac_dir = tmp_path / "outbox"
    bbs_dir = tmp_path / "bbs"
    for path in (flmsg_dir, flamp_dir, varac_dir, bbs_dir):
        path.mkdir()

    plans = plan_compose_destinations(
        "W8UFO-TN-RR-20260423-1325z-ROADCLOSURE.k2s",
        send_target="Both",
        varac_target="Both",
        flmsg_dir=str(flmsg_dir),
        flamp_dir=str(flamp_dir),
        varac_outbox_dir=str(varac_dir),
        varac_bbs_dir=str(bbs_dir),
        sign_flamp_copy=True,
    )

    ready = {plan.key: plan for plan in plans if plan.ready}
    assert ready["flmsg"].path.endswith(".k2s")
    assert ready["flamp"].path.endswith("-sig.k2s")
    assert ready["varac_outbox"].path.endswith(".k2s")
    assert ready["varac_bbs"].path.endswith(".k2s")


def test_flamp_compose_uses_transmit_sibling_for_rx_path(tmp_path: Path) -> None:
    flamp_root = tmp_path / "FLAMP"
    rx_dir = flamp_root / "rx"
    tx_dir = flamp_root / "tx"
    tx_dir.mkdir(parents=True)
    rx_dir.mkdir()

    assert resolve_flamp_transmit_dir(str(rx_dir)) == str(tx_dir)
    assert resolve_flamp_transmit_dir(str(flamp_root)) == str(tx_dir)

    plans = plan_compose_destinations(
        "W8UFO-TN-RR-20260423-1325z-ROADCLOSURE.k2s",
        send_target="FLAmp",
        varac_target="None",
        flamp_dir=resolve_flamp_transmit_dir(str(rx_dir)),
        sign_flamp_copy=True,
    )

    ready = {plan.key: plan for plan in plans if plan.ready}
    assert ready["flamp"].directory == str(tx_dir)
    assert ready["flamp"].path.endswith("-sig.k2s")


def test_compose_message_folder_options_are_limited_to_two_levels(tmp_path: Path) -> None:
    root = tmp_path / "messages"
    (root / "mine" / "Intel" / "Deep").mkdir(parents=True)
    (root / "Region" / "Summary").mkdir(parents=True)
    (root / ".hidden").mkdir()

    options = discover_compose_message_folders(root)

    assert [option.label for option in options] == [
        "Messages",
        "mine",
        "mine/Intel",
        "Region",
        "Region/Summary",
    ]
    assert resolve_compose_message_folder(root, "mine/Intel") == root / "mine" / "Intel"
    assert resolve_compose_message_folder(root, "../outside") is None
    assert resolve_compose_message_folder(root, "mine/Intel/Deep") is None
    assert compose_message_relative_path(root, root / "Region" / "Summary") == "Region/Summary"


def test_standard_blank_serialization_uses_blankform_and_custom_uses_customform() -> None:
    when = dt.datetime(2026, 4, 23, 13, 25, tzinfo=dt.timezone.utc)
    blank = serialize_standard_blank_message(
        callsign="W8UFO",
        created_utc=when,
        subject="Road Closure",
        message="County road blocked.",
        to_name="@NET",
        precedence="RR",
        dtg="260423-1325z",
    )
    custom = serialize_custom_form_message(
        "magnet_general_V1.1.0.html",
        [("L01", "260423-1325z"), ("L02", "MAGNET"), ("L03", "W8UFO")],
        callsign="W8UFO",
        created_utc=when,
    )

    assert "<blankform>" in blank
    assert ":mg:" in blank
    assert "<customform>" in custom
    assert "CUSTOM_FORM,magnet_general_V1.1.0.html" in custom


def test_custom_form_serialization_counts_complete_mg_payload_and_keeps_field_order() -> None:
    when = dt.datetime(2026, 5, 12, 19, 26, 48, tzinfo=dt.timezone.utc)
    fields = [
        ("L01", "260512-1925z"),
        ("L02", "MR08"),
        ("L03", "N1MAG"),
        ("L06", "test"),
        ("L07", "this is a test message"),
        ("L04", "R"),
        ("L05", "08"),
        ("L08", ""),
    ]

    custom = serialize_custom_form_message(
        "magnet_general_V1.1.0.html",
        fields,
        callsign="N1MAG",
        created_utc=when,
        flmsg_version="4.0.24.02",
    )

    expected_payload = (
        "CUSTOM_FORM,magnet_general_V1.1.0.html\n"
        "L01,260512-1925z\n"
        "L02,MR08\n"
        "L03,N1MAG\n"
        "L06,test\n"
        "L07,this is a test message\n"
        "L04,R\n"
        "L05,08\n"
        "L08,\n"
    )
    assert "<flmsg>4.0.24.02" in custom
    assert f":mg:{len(expected_payload.encode('utf-8'))} {expected_payload}" in custom
    assert custom.index("L06,test") < custom.index("L04,R")


def test_parse_compose_template_fields_keeps_suffix_keys_and_html_metadata() -> None:
    template = """
    <html>
      <head>
        <title>AmRRON Status Report 5.1</title>
        <meta name="Menu_Item" content="AmRRON_Statrep_V5.1">
      </head>
      <body>
        <form>
          <td><b>1a. To: </b><em>(Recipient)</em><br><input name="L01a" type="TEXT" list="toOptions"></td>
          <datalist id="toOptions"><option value="AMRRON"></option><option value="@NET"></option></datalist>
          <td><b>1b. From:</b><em>(Sender)</em><br><input name="L01b" type="TEXT"></td>
          <td><b>2. Size and Scope:</b><br>
            <select name="L02" id="L02">
              <option value="" selected="selected">My Location</option>
              <option value="R">My Region</option>
            </select>
          </td>
          <td><b>3. DTG:</b><em>(YYMMDD-HHMMz)</em><br><input name="L03" type="TEXT"></td>
          <td><b>18. Brief remarks:</b><em>(Details)</em><br><textarea name="L18" rows="8"></textarea></td>
        </form>
      </body>
    </html>
    """

    fields = parse_compose_template_fields(template)
    by_key = {field.key: field for field in fields}

    assert by_key["L01A"].label == "To"
    assert by_key["L01A"].description == "Recipient"
    assert by_key["L01A"].field_type == "select"
    assert by_key["L01A"].allow_custom is True
    assert [option.value for option in by_key["L01A"].options] == ["AMRRON", "@NET"]
    assert by_key["L02"].field_type == "select"
    assert by_key["L02"].options[0].selected is True
    assert by_key["L18"].field_type == "textarea"
    assert by_key["L18"].rows == 8


def test_parse_compose_template_fields_extracts_labels_from_legacy_table_forms() -> None:
    template = """
    <html><body><form>
      <td><span style="background:#e0e0e0"><font face="Arial">
        <b><font size="3">1. To: </font></b><em><font size="2">(Recipient)</font></em><br>
        <input name="L01" type="TEXT" id="L01" size="25">
      </font></span></td>
      <td><span style="background:#e0e0e0"><font face="Arial">
        <b><font size="3">4. DTG: </font></b><em><font size="2">(YYMMDD-HHMMZ)</font></em><br>
        <input name="L04" type="TEXT" id="L04" size="25">
      </font></span></td>
      <td><span style="background:#e0e0e0"><font face="Arial"><strong><font size="3">[5]
        Size</font></strong> <font size="2"><em>(Platoon? Battalion? #Vehicles #Persons)</em></font><br>
        <input name="L05" type="TEXT" id="L05" size="99">
      </font></span></td>
    </form></body></html>
    """

    fields = {field.key: field for field in parse_compose_template_fields(template)}
    assert fields["L01"].label == "To"
    assert fields["L01"].description == "Recipient"
    assert fields["L04"].label == "DTG"
    assert fields["L04"].description == "YYMMDD-HHMMZ"
    assert fields["L05"].label == "Size"
    assert "Platoon?" in fields["L05"].description


def test_suggest_field_value_uses_form_context_and_skips_expiration_defaults() -> None:
    precedence_options = (
        ComposeFieldOption(value="R", label="Routine"),
        ComposeFieldOption(value="P", label="Priority"),
    )

    assert suggest_field_value(
        "L01",
        "To",
        description="Recipient",
        template_title="MAGNET Situation Report",
        callsign="W8UFO",
    ) == "MAGNET"
    assert suggest_field_value(
        "L02",
        "From",
        description="Sender",
        callsign="W8UFO",
    ) == "W8UFO"
    assert suggest_field_value(
        "L03",
        "Message Precedence",
        options=precedence_options,
        priority_code="PP",
    ) == "P"
    assert suggest_field_value(
        "L04",
        "State (ST)",
        state="TN",
    ) == "TN"
    assert suggest_field_value(
        "L07",
        "DTG",
        zulu_timestamp="260423-1325z",
    ) == "260423-1325z"
    assert suggest_field_value(
        "L08",
        "Expiration",
        zulu_timestamp="260423-1325z",
    ) == ""


def test_messages_source_contains_compose_mode_and_varac_copy_controls() -> None:
    text = _read("freqinout/gui/message_viewer_tab.py")
    shell = _read("freqinout/gui/main_window.py")
    assert '("Inbox", "Messages")' in shell
    assert '("Compose", "Messages")' in shell
    assert 'self._messages_nav_button_indices["inbox"] = btn_idx' in shell
    assert 'self._messages_nav_button_indices["compose"] = btn_idx' in shell
    assert "def open_messages_section(" in shell
    assert "mode: str = \"inbox\"" in shell
    assert "def show_compose_from_navigation(self) -> None:" in text
    assert 'self.inbox_controls_panel.setObjectName("messagesInboxControlPanel")' in text
    assert 'self.inbox_controls_scroll.setObjectName("messagesInboxControlScroll")' in text
    assert 'self._make_combo_searchable(self.type_filter, "Message Type")' in text
    assert 'QPushButton("Open Form Folder")' in text
    assert "class ComposeRadioTarget" in text
    assert 'radio_row.addWidget(QLabel("Compose For"))' in text
    assert 'self.compose_refresh_radios_btn.clicked.connect(self._refresh_compose_radios_clicked)' in text
    assert 'self.settings.set("messages_compose_radio_id", int(target.radio_id))' in text
    assert "No radio profile has FLMsg, FLAmp, or VarAC message destinations configured." in text
    assert "def _compose_bbs_targets_for_radio(self, target: Optional[ComposeRadioTarget])" in text
    assert 'radio_row.addWidget(QLabel("Radio"))' in text
    assert 'location_row.addWidget(QLabel("BBS Location"))' in text
    assert 'row2.addWidget(QLabel("Report Title"))' in text
    assert 'self.compose_varac_target_combo.addItems(["None", "Outbox", "BBS", "Both"])' in text
    assert 'varac_outbox_dir=self._compose_varac_outbox_dir(radio_target)' in text
    assert 'def _compose_varac_outbox_dir(self, target: Optional[ComposeRadioTarget] = None) -> str:' in text
    assert 'self.compose_family_combo.addItem("Standard Blank Form (.b2s)"' not in text
    assert 'self.compose_form_combo.addItem("Standard Blank Form (.b2s)", {"kind": "standard"})' in text
    assert "self._configure_compose_combo_width(self.compose_priority_combo, floor=96)" in text
    assert "self._configure_compose_combo_width(self.compose_send_target_combo, floor=118)" in text
    assert "self._configure_compose_combo_width(self.compose_varac_target_combo, floor=118)" in text
    assert "def _configure_compose_combo_width(self, combo: QComboBox, *, floor: int = 110) -> None:" in text
    assert "desc_widget = QLabel(field.description)" in text
    assert "field_layout = QVBoxLayout(field_wrap)" in text
    assert "self._refresh_compose_smart_defaults()" in text
    assert "self._compose_active_form_key = form_identity" in text
    assert "dict(self._compose_form_draft_values.get(form_identity, {}))" in text
    assert 'parse_compose_template_fields(template_text)' in text
    assert "field_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)" in text
    assert "widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)" in text
    assert "FLAmp signed file verified:" in text
    assert "FLAmp signing failed; no unsigned FLAmp fallback was staged." in text
    assert "FLAmp signing failed; staged unsigned file instead" not in text
    assert "self._compose_software_status.program_is_running(app_name)" in text
    assert "No second instance was opened." in text
