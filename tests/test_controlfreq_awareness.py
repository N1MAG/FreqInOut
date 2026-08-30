from __future__ import annotations

from freqinout.core.controlfreq_awareness import (
    AwarenessPin,
    HIGH_VALUE_TOPICS,
    build_awareness_snapshot,
    reply_compose_mode_for_source,
)
from freqinout.core.observation_projection import Observation


def _obs(
    ident: str,
    *,
    source_family: str = "js8call",
    from_call: str = "N1ABC",
    to_target: str = "MAGNET",
    groups: tuple[str, ...] = (),
    topics: tuple[str, ...] = (),
    subject: str = "",
    summary: str = "",
    status: str = "",
    urgency: str = "",
    operator_attention: bool = False,
    received_utc: str = "2026-08-29T12:00:00Z",
    state: str = "",
    grid: str = "",
) -> Observation:
    return Observation(
        observation_id=ident,
        source_family=source_family,
        source_ref=ident,
        received_utc=received_utc,
        from_call=from_call,
        to_target=to_target,
        groups=groups,
        observed_topics=topics,
        operator_attention=operator_attention,
        status=status,
        urgency=urgency,
        subject=subject,
        summary=summary,
        state=state,
        grid=grid,
    )


def test_awareness_high_value_topics_are_initial_product_set() -> None:
    assert HIGH_VALUE_TOPICS == (
        "wildfire",
        "power",
        "water",
        "medical",
        "comms",
        "weather",
        "security",
        "logistics",
        "general intel",
    )


def test_awareness_global_attention_queue_ranks_direct_and_alerts() -> None:
    snapshot = build_awareness_snapshot(
        [
            _obs("routine", source_family="spotter", topics=("General Intel",), received_utc="2026-08-29T11:40:00Z"),
            _obs(
                "alert",
                source_family="condition_alert",
                to_target="AMRRON",
                subject="Condition alert",
                operator_attention=True,
                received_utc="2026-08-29T11:45:00Z",
            ),
            _obs(
                "direct",
                source_family="js8call",
                from_call="N1XYZ",
                to_target="N1MAG",
                subject="Need relay to AMRRON 40M",
                received_utc="2026-08-29T11:50:00Z",
            ),
        ],
        local_callsign="N1MAG",
        active_groups=("MAGNET",),
        generated_at_utc="2026-08-29T12:00:00Z",
    )

    assert [item.id for item in snapshot.attention_items] == ["direct", "alert", "routine"]
    assert snapshot.attention_items[0].subject == "Need relay to AMRRON 40M"
    assert snapshot.attention_items[0].age_seconds == 600
    assert snapshot.recommended_actions[0].kind == "reply"


def test_awareness_reply_defaults_to_source_family() -> None:
    assert reply_compose_mode_for_source("js8call") == "js8"
    assert reply_compose_mode_for_source("FIOSpotter") == "spotter"
    assert reply_compose_mode_for_source("commstat") == "commstat_rf"
    assert reply_compose_mode_for_source("flmsg") == "nbems"

    snapshot = build_awareness_snapshot(
        [_obs("spotter", source_family="spotter", topics=("Water",), operator_attention=True)]
    )

    item = snapshot.attention_items[0]
    assert item.reply_compose_mode == "spotter"
    assert any(action.kind == "reply" and action.context["compose_mode"] == "spotter" for action in item.actions)


def test_awareness_pins_boost_matching_items_and_stay_visible_without_matches() -> None:
    snapshot = build_awareness_snapshot(
        [
            _obs("routine", source_family="js8call", topics=("General Intel",)),
            _obs("fire", source_family="spotter", topics=("Wildfire",), received_utc="2026-08-29T11:55:00Z"),
        ],
        pins=[
            AwarenessPin("topic", "Wildfire"),
            AwarenessPin("callsign", "N0PIN"),
        ],
        generated_at_utc="2026-08-29T12:00:00Z",
    )

    assert snapshot.attention_items[0].id == "fire"
    assert snapshot.attention_items[0].pinned is True
    assert [(pin.pin_type, pin.value, pin.matched_count) for pin in snapshot.pins] == [
        ("topic", "Wildfire", 1),
        ("callsign", "N0PIN", 0),
    ]
    assert any(action.kind == "messages" for action in snapshot.recommended_actions)


def test_awareness_topic_rollups_include_counts_sources_and_geography() -> None:
    snapshot = build_awareness_snapshot(
        [
            _obs("fire1", source_family="spotter", from_call="N1AAA", topics=("Wildfire",), state="CO"),
            _obs("fire2", source_family="commstat", from_call="N1BBB", topics=("Wildfire",), grid="DM79"),
            _obs("water1", source_family="local_report", from_call="N1AAA", topics=("Water",), state="WY"),
        ]
    )

    wildfire = next(rollup for rollup in snapshot.topic_rollups if rollup.topic == "Wildfire")
    assert wildfire.count == 2
    assert wildfire.source_count == 2
    assert wildfire.callsign_count == 2
    assert wildfire.geography_hint == "CO, DM79"
    assert wildfire.severity == "important"


def test_awareness_more_traffic_strip_keeps_routine_overflow_visible() -> None:
    observations = [
        _obs(f"item-{idx}", from_call=f"N1A{idx}", subject=f"Routine {idx}", received_utc=f"2026-08-29T11:{50 + idx:02d}:00Z")
        for idx in range(5)
    ]

    snapshot = build_awareness_snapshot(
        observations,
        visible_attention_limit=3,
        generated_at_utc="2026-08-29T12:00:00Z",
    )

    assert len(snapshot.attention_items) == 3
    assert len(snapshot.more_traffic) == 2
    assert [item.id for item in snapshot.more_traffic] == ["item-1", "item-0"]
