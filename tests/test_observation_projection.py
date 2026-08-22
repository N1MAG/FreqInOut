import json

from freqinout.core.message_intelligence import analyze_commstat_fields, analyze_form_text, analyze_spotter_text
from freqinout.core.observation_projection import (
    explain_bbs_eligibility,
    explain_map_eligibility,
    observation_from_local_report,
    observation_from_message_intelligence,
    observation_from_rf_pin,
)


def test_message_observation_keeps_intelligence_advisory_not_authorized() -> None:
    info = analyze_spotter_text(
        "F!701 TO[@MAGNET] FR[W0IFM] ST[MO] GR[EM48EQ] NA[Wildfire evacuation update] DA[260429-1839Z]",
        form_name="MCF701 Field Report",
    )

    obs = observation_from_message_intelligence(
        info,
        source_ref="js8spotter:42",
        source_family="spotter",
        source_radio_id=8,
        source_app="FIO-A",
        received_utc="2026-08-10T12:00:00+00:00",
        auth_state="valid",
        trusted_state="trusted",
    )

    assert obs.observation_id == "spotter:js8spotter:42"
    assert obs.from_call == "W0IFM"
    assert obs.to_target == "@MAGNET"
    assert obs.groups == ("@MAGNET",)
    assert obs.grid == "EM48EQ"
    assert obs.location_confidence == "grid"
    assert "Fire" in obs.observed_topics
    assert obs.operator_attention is True
    assert obs.route_eligible is False
    assert obs.publish_authorized is False
    assert json.loads(obs.provenance_json)["routing_candidate"] is True


def test_map_eligibility_requires_enabled_layer_and_mappable_location() -> None:
    info = analyze_form_text("Weather briefing only. No location or group included.", source_type="flmsg")
    obs = observation_from_message_intelligence(info, source_ref="flmsg:file.k2s")

    disabled = explain_map_eligibility(obs, layer_enabled=False)
    assert disabled.allowed is False
    assert "layer disabled" in disabled.reasons

    enabled = explain_map_eligibility(obs, layer_enabled=True)
    assert enabled.allowed is False
    assert "no mappable location" in enabled.reasons


def test_local_report_projection_is_inert_until_confirmed_or_explicitly_allowed() -> None:
    obs = observation_from_local_report(
        {
            "id": 17,
            "created_utc": "2026-08-10T15:00:00+00:00",
            "source_kind": "voice",
            "source_channel": "VHF Net",
            "callsign": "K0PRA",
            "city": "Parker",
            "state": "CO",
            "grid": "DM79",
            "status": "YELLOW",
            "topics_json": json.dumps(["Power"]),
            "subject": "Generator available",
            "confirmed_state": "UNCONFIRMED",
            "exercise_flag": False,
        }
    )

    assert obs.source_family == "local_report"
    assert obs.from_call == "K0PRA"
    assert obs.grid == "DM79"
    assert obs.confirmed_state == "UNCONFIRMED"
    assert obs.route_eligible is False
    assert obs.publish_authorized is False

    blocked = explain_map_eligibility(obs, layer_enabled=True)
    assert blocked.allowed is False
    assert "local report is unconfirmed" in blocked.reasons

    allowed = explain_map_eligibility(obs, layer_enabled=True, allow_unconfirmed_local=True)
    assert allowed.allowed is True


def test_exercise_report_is_excluded_from_operational_map_and_bbs() -> None:
    obs = observation_from_local_report(
        {
            "id": 18,
            "created_utc": "2026-08-10T15:05:00+00:00",
            "callsign": "K0PRA",
            "state": "CO",
            "grid": "DM79",
            "status": "GREEN",
            "topics": ("Comms",),
            "confirmed_state": "CONFIRMED",
            "exercise_flag": True,
        }
    )

    map_status = explain_map_eligibility(obs, layer_enabled=True)
    assert map_status.allowed is False
    assert "exercise/test report excluded from operational layer" in map_status.reasons

    exercise_map = explain_map_eligibility(obs, layer_enabled=True, exercise_layer=True)
    assert exercise_map.allowed is True

    bbs_status = explain_bbs_eligibility(
        obs,
        rule_enabled=True,
        dry_run_reviewed=True,
        destination_scope="LOCAL/COMMS",
    )
    assert bbs_status.allowed is False
    assert "exercise/test requires exercise destination" in bbs_status.reasons


def test_bbs_eligibility_requires_explicit_rule_preview_and_destination() -> None:
    info = analyze_spotter_text("F!307 TO[@MR08] FR[K7ETC] GR[DM38ST] NA[Fire status] #D2NT")
    obs = observation_from_message_intelligence(info, source_ref="forms:25")

    assert explain_bbs_eligibility(obs).allowed is False
    assert "no enabled rule" in explain_bbs_eligibility(obs).reasons

    allowed = explain_bbs_eligibility(
        obs,
        rule_enabled=True,
        dry_run_reviewed=True,
        destination_scope="HF/MR08/FIRE",
    )
    assert allowed.allowed is True
    assert "explicit rule authorized" in allowed.reasons


def test_commstat_observation_keeps_group_targets_without_js8_marker() -> None:
    info = analyze_commstat_fields(
        artifact_kind="MESSAGE",
        title="Regional advisory",
        from_call="N1MAG",
        target="@MAGNET",
    )

    obs = observation_from_message_intelligence(
        info,
        source_ref="commstat:1",
        source_family="commstat",
    )

    assert info.to_call == "MAGNET"
    assert obs.to_target == "MAGNET"
    assert obs.groups == ("MAGNET",)


def test_rf_pin_projection_is_mappable_but_not_routing_authorized() -> None:
    obs = observation_from_rf_pin(
        {
            "pin_id": "manual:ridge-repeater",
            "label": "Ridge repeater",
            "callsign": "K0PRA",
            "group": "MAGNET",
            "topics": ["Comms", "Infrastructure"],
            "grid": "DM79",
            "status": "WATCH",
            "created_by": "N1MAG",
        }
    )

    assert obs.observation_id == "rf_pin:manual:ridge-repeater"
    assert obs.source_family == "rf_pin"
    assert obs.from_call == "K0PRA"
    assert obs.to_target == "@MAGNET"
    assert obs.groups == ("@MAGNET",)
    assert obs.subject == "Ridge repeater"
    assert obs.location_confidence == "grid"
    assert obs.route_eligible is False
    assert obs.publish_authorized is False

    map_status = explain_map_eligibility(obs, layer_enabled=True)
    assert map_status.allowed is True
    assert "location:grid" in map_status.reasons
    assert json.loads(obs.provenance_json)["source_type"] == "rf_pin"
