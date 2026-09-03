from __future__ import annotations

from types import SimpleNamespace

from freqinout.core.message_inbox_filters import row_matches_inbox_focus
from freqinout.core.message_intel_filters import (
    build_intel_filter_rollup,
    focus_source_values,
    message_status_bucket,
    message_topics,
    normalize_intel_topic,
    row_matches_intel_filters,
)


def _row(status: str, topics=(), origin: str = "commstat", **payload):
    return SimpleNamespace(
        status=status,
        topics=tuple(topics),
        origin=origin,
        msg_type="CommStat" if origin == "commstat" else origin,
        title="",
        payload=SimpleNamespace(**payload),
        actionable=False,
    )


def test_intel_status_bucket_prefers_explicit_report_color() -> None:
    assert message_status_bucket(_row("INFO", alert_color="RED")) == "red"
    assert message_status_bucket(_row("INFO", overall_status="YELLOW")) == "yellow"
    assert message_status_bucket(_row("INFO", status_label="GREEN")) == "green"
    assert message_status_bucket(_row("NEW")) == "info"


def test_intel_rollup_counts_active_topics_by_worst_status() -> None:
    rows = [
        _row("INFO", ("Power", "Comms"), alert_color="YELLOW"),
        _row("INFO", ("Power",), alert_color="GREEN"),
        _row("INFO", ("Medical",), alert_color="RED"),
        _row("READ", (), origin="js8"),
    ]

    rollup = build_intel_filter_rollup(rows, active_status="yellow", active_topic="Power")

    assert [(chip.value, chip.count, chip.active) for chip in rollup.status_chips] == [
        ("red", 1, False),
        ("yellow", 1, True),
        ("green", 1, False),
        ("info", 1, False),
    ]
    assert rollup.topic_chips[0].value == "Medical"
    assert rollup.topic_chips[0].status_bucket == "red"
    power = next(chip for chip in rollup.topic_chips if chip.value == "Power")
    assert power.count == 2
    assert power.status_bucket == "yellow"
    assert power.active is True


def test_intel_filters_match_exact_topic_and_status_bucket() -> None:
    row = _row("INFO", ("Power", "Comms"), alert_color="YELLOW")

    assert row_matches_intel_filters(row, status_bucket="yellow", topic="Power") is True
    assert row_matches_intel_filters(row, status_bucket="red", topic="Power") is False
    assert row_matches_intel_filters(row, status_bucket="yellow", topic="Fuel") is False


def test_travel_topic_aliases_collapse_to_travel_roads() -> None:
    assert normalize_intel_topic("Travel") == "Travel/Roads"
    assert message_topics(_row("INFO", ("Travel", "Travel/Roads"))) == ("Travel/Roads",)


def test_focus_source_values_are_operator_domain_refinements() -> None:
    assert focus_source_values("mesh") == ("mesh", "meshcore", "meshtastic")
    assert focus_source_values("js8call") == ("js8", "commstat", "spotter")


def test_js8_focus_can_include_commstat_and_spotter_sources() -> None:
    assert row_matches_inbox_focus(_row("INFO", origin="js8"), "js8call") is True
    assert row_matches_inbox_focus(_row("INFO", origin="commstat"), "js8call") is True
    assert row_matches_inbox_focus(_row("INFO", origin="spotter"), "js8call") is True
    assert row_matches_inbox_focus(_row("INFO", origin="mesh"), "js8call") is False
