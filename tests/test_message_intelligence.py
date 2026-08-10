from freqinout.core.message_intelligence import (
    analyze_commstat_fields,
    analyze_form_text,
    analyze_spotter_text,
    normalize_topic_terms,
)
from freqinout.core.message_file_scanner import FileRecord
from freqinout.gui.message_viewer_tab import MessageViewerTab, SpotterMessage, UnifiedMessage, _RowsBuildWorker


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
    assert row.title == "MAGNET General Use Form - v1.1.1 | K7ETC -> MR08 | Widemouth 2 Fire | 260803-0402z"
    assert "Fire" in row.topics
    assert row.actionable is True
    assert "dm38st" in row.search_text
    assert "ut" in row.search_text
    assert "fire" in row.search_text


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
