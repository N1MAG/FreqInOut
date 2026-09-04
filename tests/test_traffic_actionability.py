from __future__ import annotations

from dataclasses import replace

from freqinout.core.message_summary import MessageActionValidity, MessageSummary
from freqinout.core.traffic_actionability import (
    build_operator_traffic_context,
    build_traffic_action_summary,
    message_matches_traffic_bucket,
)


def _message(**overrides) -> MessageSummary:
    base = MessageSummary(
        source_family="js8",
        source_label="JS8Call",
        stable_id="js8:1",
        received_ts=100.0,
        event_ts=100.0,
        from_call="K7ETC",
        to_target="MR08",
        group="MR08",
        subject="Power status request",
        summary="Please confirm and distribute this power outage report.",
        form_type="F!103",
        status="NEW",
        severity="important",
        topics=("Power",),
        actions=MessageActionValidity(can_read=True, can_reply=True),
    )
    return replace(base, **overrides)


def test_context_uses_only_explicit_groups_and_own_operator_row() -> None:
    context = build_operator_traffic_context(
        callsign="N1MAG",
        configured_operating_groups=("MAGNET",),
        configured_local_groups=("LOCAL-EOC",),
        operator_rows=(
            {
                "callsign": "N1MAG",
                "group1": "MR08",
                "group2": "",
                "group3": "",
                "groups_json": '["AMRRON"]',
                "group_role": "ALT-HUB",
            },
            {
                "callsign": "K7OTHER",
                "group1": "UNRELATED",
                "group_role": "HUB",
            },
        ),
    )

    assert context.groups == ("AMRRON", "LOCAL-EOC", "MAGNET", "MR08")
    assert context.role_for_group("MR08") == "HUB-ALT"
    assert context.role_for_group("AMRRON") == "HUB-ALT"
    assert context.role_for_group("MAGNET") == "MEMBER"
    assert "UNRELATED" not in context.groups


def test_hub_alt_ncs_roles_share_distribution_duty() -> None:
    for role in ("HUB", "HUB-ALT", "ALT-HUB", "NCS", "ANCS"):
        context = build_operator_traffic_context(
            callsign="N1MAG",
            operator_rows=(
                {
                    "callsign": "N1MAG",
                    "group1": "MR08",
                    "group_role": role,
                },
            ),
        )
        summary = build_traffic_action_summary((_message(),), context)
        assert summary.count("relay") == 1


def test_peer_is_reporter_not_distributor() -> None:
    context = build_operator_traffic_context(
        callsign="N1MAG",
        operator_rows=(
            {
                "callsign": "N1MAG",
                "group1": "MR08",
                "group_role": "PEER",
            },
        ),
    )
    summary = build_traffic_action_summary(
        (_message(subject="Power outage", summary="Power outage confirmed."),),
        context,
    )
    assert summary.count("relay") == 0
    assert summary.count("review") == 1


def test_direct_event_is_reply_and_relay_for_distribution_role() -> None:
    context = build_operator_traffic_context(
        callsign="N1MAG",
        operator_rows=(
            {
                "callsign": "N1MAG",
                "group1": "MR08",
                "group_role": "HUB",
            },
        ),
    )
    message = _message(to_target="N1MAG", group="")
    summary = build_traffic_action_summary((message,), context)

    assert summary.count("reply") == 1
    assert summary.count("relay") == 1
    assert summary.lead is not None
    assert summary.lead.primary_bucket == "reply"
    assert "N1MAG" in summary.lead.why
    relay_what, relay_why = summary.lead.guidance_for("relay")
    assert relay_what.startswith("Distribute Power report")
    assert "HUB duty for MR08" in relay_why


def test_social_direct_message_sorts_after_event_traffic() -> None:
    context = build_operator_traffic_context(callsign="N1MAG")
    social = _message(
        stable_id="social",
        to_target="N1MAG",
        group="",
        subject="Good morning",
        summary="Good morning from the net.",
        form_type="MSG",
        severity="watch",
        topics=(),
        event_ts=200.0,
        received_ts=200.0,
    )
    event = _message(
        stable_id="event",
        to_target="N1MAG",
        group="",
        event_ts=100.0,
    )
    summary = build_traffic_action_summary((social, event), context)

    assert summary.items[0].stable_id == "event"
    assert summary.count("social") == 1
    assert message_matches_traffic_bucket(social, context, "social")


def test_read_state_does_not_complete_operational_action() -> None:
    context = build_operator_traffic_context(callsign="N1MAG")
    read_event = _message(to_target="N1MAG", group="", status="READ")

    summary = build_traffic_action_summary((read_event,), context)

    assert summary.count("reply") == 1


def test_unassociated_group_is_not_inferred_from_related_name() -> None:
    context = build_operator_traffic_context(
        callsign="N1MAG",
        configured_operating_groups=("MAGNET",),
    )
    child_named_message = _message(to_target="MR08", group="MR08")

    assert build_traffic_action_summary((child_named_message,), context).items == ()
