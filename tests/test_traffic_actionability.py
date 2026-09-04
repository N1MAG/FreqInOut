from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from freqinout.core.message_summary import MessageActionValidity, MessageSummary
from freqinout.core.traffic_actionability import (
    build_operator_traffic_context,
    build_traffic_group_volumes,
    build_traffic_action_summary,
    filter_traffic_messages,
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


def test_green_status_report_is_volume_not_reply_work() -> None:
    context = build_operator_traffic_context(
        callsign="N1MAG",
        operator_rows=({"callsign": "N1MAG", "group1": "MR08", "group_role": "HUB"},),
    )
    green_report = {
        "message_id": "k7etc-green-f701c",
        "source_family": "js8call",
        "from_call": "K7ETC",
        "to_call": "MR08",
        "group_name": "MR08",
        "subject": "F!701C Situation Report",
        "summary": (
            "Current Operational Status (QTH) *Operations steady, no significant issues "
            "or noteworthy activity - Green"
        ),
        "severity": "important",
        "actionable": True,
    }

    summary = build_traffic_action_summary((green_report,), context)

    assert summary.count("reply") == 0
    assert summary.count("relay") == 0
    assert summary.count("review") == 0


def test_green_report_with_explicit_question_can_request_reply() -> None:
    context = build_operator_traffic_context(
        callsign="N1MAG",
        configured_operating_groups=("MR08",),
    )
    green_request = _message(
        summary="Operations steady and Green. Please confirm receipt?",
        severity="routine",
    )

    assert build_traffic_action_summary((green_request,), context).count("reply") == 1


def test_traffic_age_scope_and_group_trend_are_shared() -> None:
    now = 200_000.0
    rows = [
        {
            "message_id": f"current-{index}",
            "source_family": "js8",
            "group_name": "MR08",
            "received_ts": now - (index * 60),
            "read_state": "new" if index < 3 else "read",
        }
        for index in range(6)
    ]
    rows.append(
        {
            "message_id": "previous",
            "source_family": "js8call",
            "group_name": "MR08",
            "received_ts": now - 90_000,
            "read_state": "read",
        }
    )
    rows.append(
        {
            "message_id": "other-group",
            "source_family": "js8call",
            "group_name": "MAGNET",
            "received_ts": now - 60,
            "read_state": "new",
        }
    )

    scoped = filter_traffic_messages(
        rows,
        age_seconds=86_400,
        now_ts=now,
        source_family="js8call",
        group_filter="MR08",
    )
    volumes = build_traffic_group_volumes(
        rows,
        age_seconds=86_400,
        now_ts=now,
        source_family="js8call",
        group_filter="MR08",
    )

    assert len(scoped) == 6
    assert len(volumes) == 1
    assert volumes[0].current_count == 6
    assert volumes[0].previous_count == 1
    assert volumes[0].unread_count == 3
    assert volumes[0].trend == "Spike ↑"
    assert volumes[0].sources == (("JS8Call", 6),)


def test_operator_groups_sort_before_unassociated_volume_spikes() -> None:
    now = 200_000.0
    rows = [
        {
            "message_id": "my-group",
            "source_family": "commstat",
            "group_name": "MR08",
            "received_ts": now - 60,
            "read_state": "new",
        },
        *(
            {
                "message_id": f"other-{index}",
                "source_family": "js8call",
                "group_name": "GHOSTNET",
                "received_ts": now - (index * 30),
                "read_state": "new",
            }
            for index in range(8)
        ),
    ]

    volumes = build_traffic_group_volumes(
        rows,
        age_seconds=3600,
        now_ts=now,
        operator_groups=("MR08", "MAGNET"),
    )

    assert [volume.group for volume in volumes] == ["MR08", "GHOSTNET"]
    assert volumes[0].is_operator_group is True
    assert volumes[1].is_operator_group is False


def test_projected_wrapper_and_canonical_row_classify_identically() -> None:
    context = build_operator_traffic_context(
        callsign="N1MAG",
        operator_rows=({"callsign": "N1MAG", "group1": "MR08", "group_role": "PEER"},),
    )
    canonical = {
        "message_id": "canonical-1",
        "source_family": "commstat",
        "from_call": "K7ETC",
        "to_call": "MR08",
        "group_name": "MR08",
        "summary": "Power outage confirmed in the region.",
        "severity": "important",
        "topics_json": '["Power"]',
        "actionable": True,
        "received_ts": 500.0,
    }
    wrapper = SimpleNamespace(
        to_call="MR08",
        status="NEW",
        actionable=True,
        rcv_ts=500.0,
        topics=("Power",),
        payload=SimpleNamespace(
            message_id="canonical-1",
            source_family="commstat",
            from_call="K7ETC",
            to_call="MR08",
            group="MR08",
            summary="Power outage confirmed in the region.",
            severity="important",
        ),
        summary=_message(
            stable_id="canonical-1",
            source_family="commstat",
            group="MR08",
            to_target="MR08",
            summary="Presentation fallback should not override canonical payload.",
            severity="routine",
            topics=(),
        ),
    )

    raw_summary = build_traffic_action_summary((canonical,), context)
    wrapped_summary = build_traffic_action_summary((wrapper,), context)

    assert raw_summary.count("review") == 1
    assert wrapped_summary.count("review") == raw_summary.count("review")
