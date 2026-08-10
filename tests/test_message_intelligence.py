from freqinout.core.message_intelligence import (
    analyze_commstat_fields,
    analyze_form_text,
    analyze_spotter_text,
    normalize_topic_terms,
)


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
    assert "General Intel" not in info.topics
    assert info.actionable is True
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
    assert info.actionable is True
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
    assert "Residents south of 300 South" in info.body
    assert "Fire" in info.topics
    assert info.summary.startswith("MAGNET General Use Form - v1.1.1 | K7ETC -> MR08 | Widemouth 2 Fire")


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
    assert info.to_call == "@MAGNET"
    assert info.state == "FL"
    assert info.grid == "EL98"
    assert {"Weather", "Shelter", "General Intel"}.issubset(set(info.topics))
    assert info.actionable is True
    assert info.summary == "CommStat Alert | W8UFO -> @MAGNET | Storm Surge Warning | 2026-08-10 12:34:56"


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
    assert "@MR08" in info.groups
    assert {"Power", "Water", "Comms", "Infrastructure", "General Intel"}.issubset(set(info.topics))
    assert info.actionable is True


def test_commstat_general_message_is_flat_not_nested() -> None:
    info = analyze_commstat_fields(
        artifact_kind="MESSAGE",
        title="Regional advisory",
        body="Station staffing update for internet relay coverage.",
        from_call="N1MAG",
        target="@MAGNET",
    )

    assert info.form_name == "CommStat Message"
    assert info.summary.startswith("CommStat Message | N1MAG -> @MAGNET | Regional advisory")
    assert "Comms" in info.topics
    assert info.metadata["kind"] == "CommStat Message"
