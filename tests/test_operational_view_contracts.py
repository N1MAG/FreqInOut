from freqinout.core.view_contracts import (
    DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS,
    compose_intent_from_map_context,
    compose_intent_from_mapping,
    map_context_from_mapping,
    rf_readiness_from_mapping,
    schedule_window_from_mapping,
    setup_checklist_item_from_mapping,
    station_command_radio_from_mapping,
)


def test_compose_intent_normalizes_source_specific_modes() -> None:
    intent = compose_intent_from_mapping(
        {
            "transport": "CommStat",
            "target": "@w5tta",
            "topic_filter": "Water",
            "grid_filter": "dm79",
            "recency_seconds": "3600",
        }
    )

    assert intent.mode == "commstat_rf"
    assert intent.transport == "commstat_rf"
    assert intent.recipient_callsign == "W5TTA"
    assert intent.topic == "Water"
    assert intent.grid == "DM79"
    assert intent.age_filter_seconds == 3600


def test_map_context_preserves_operational_filters_for_handoffs() -> None:
    context = map_context_from_mapping(
        {
            "source_family": "FIOSpotter",
            "group": "@MAGNET",
            "topic": "Wildfire",
            "callsign": "@KI6QDB",
            "state": "co",
            "grid": "dm79",
            "concern_only": "yes",
        }
    )

    assert context.source_family == "spotter"
    assert context.group_filter == "MAGNET"
    assert context.topic_filter == "Wildfire"
    assert context.query_filter == "KI6QDB"
    assert context.state_filter == "CO"
    assert context.grid_filter == "DM79"
    assert context.concern_only is True
    assert context.has_focus is True


def test_compose_intent_from_map_context_uses_source_appropriate_reply_mode() -> None:
    context = map_context_from_mapping(
        {
            "source_family": "commstat_rf",
            "callsign": "aa0dy",
            "topic": "Comms",
            "age_filter_seconds": 0,
        }
    )
    intent = compose_intent_from_map_context(context)

    assert intent.mode == "commstat_rf"
    assert intent.recipient_callsign == "AA0DY"
    assert intent.body == "RE Comms: "
    assert intent.age_filter_seconds == DEFAULT_TRAFFIC_CONTEXT_RETENTION_SECONDS


def test_station_command_radio_uses_short_name_as_card_title() -> None:
    radio = station_command_radio_from_mapping(
        {
            "name": "FIO-B - Kenwood TS-2000 - FLMsg, FLAmp, VarAC, JS8Call",
            "short_name": "FIO-B",
            "group": "@AMRRON",
            "band": "80m",
            "actions": ["qsy", "hold"],
        }
    )

    assert radio.card_title == "FIO-B"
    assert radio.group == "AMRRON"
    assert radio.band == "80M"
    assert radio.actions == ("qsy", "hold")


def test_station_command_radio_collapses_legacy_long_label_to_short_title() -> None:
    radio = station_command_radio_from_mapping(
        {
            "name": "FIO-B - Kenwood TS-2000 - FLMsg, FLAmp, VarAC, JS8Call",
        }
    )

    assert radio.card_title == "FIO-B"


def test_schedule_window_projects_to_navigation_context() -> None:
    window = schedule_window_from_mapping(
        {
            "group": "@MAGNET",
            "net_name": "Magnet Main",
            "band": "80m",
            "frequency": "3.585",
            "when": "23:00",
            "grid": "dm79",
        }
    )

    assert window.headline == "23:00 | MAGNET | 80M"
    assert window.as_context_kwargs()["group_filter"] == "MAGNET"
    assert window.as_context_kwargs()["grid_filter"] == "DM79"


def test_rf_readiness_compact_label_falls_back_to_operational_recommendation() -> None:
    readiness = rf_readiness_from_mapping(
        {
            "band": "20m",
            "frequency": "14.110",
            "watch_band": "40m",
            "watch_frequency": "7.110",
            "details_available": "yes",
        }
    )

    assert readiness.compact_label == "Use 20M 14.110; watch 40M 7.110."
    assert readiness.details_available is True


def test_setup_checklist_item_marks_required_blockers() -> None:
    item = setup_checklist_item_from_mapping(
        {
            "key": "spotter_mcf_dir",
            "label": "Spotter MCF folder",
            "status": "missing",
            "required": True,
            "source_family": "FIOSpotter",
            "screen": "Settings",
        }
    )

    assert item.source_family == "spotter"
    assert item.complete is False
    assert item.blocks_operations is True
