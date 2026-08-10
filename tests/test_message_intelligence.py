from freqinout.core.message_intelligence import analyze_form_text, analyze_spotter_text, normalize_topic_terms


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
