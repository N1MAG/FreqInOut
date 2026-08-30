from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path

from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.message_summary import (
    DEFAULT_TRAFFIC_RETENTION_DAYS,
    default_visible_message_summaries,
    filter_message_summaries,
    message_source_label,
    message_summaries_from_rows,
    message_summary_source_counts,
    message_summary_from_row,
    normalize_message_source_family,
)


@dataclass
class _Payload:
    msg_id: int = 1
    source_key: str = "js8:fio-a"
    source_radio_id: str = "FIO-A"
    js8_instance_id: str = "JS8Call 3.x"
    grid: str = ""
    state_code: str = ""
    alert_color: str = ""
    flag_state: int = 0
    subject: str = ""
    body: str = ""


@dataclass
class _Row:
    msg_type: str = "MSG"
    status: str = "NEW"
    from_call: str = "k1abc"
    to_call: str = "@MAGNET"
    rcv_ts: float = 0.0
    rcv_display: str = "2026-08-30 12:00:00"
    title: str = "Need relay to county"
    origin: str = "js8call"
    payload: object = None
    search_text: str = "need relay county"
    topics: tuple[str, ...] = ("Comms",)
    actionable: bool = False
    auth_state: str = ""
    auth_detail: str = ""
    auth_trusted: bool = False
    expect_decision: str = ""
    expect_detail: str = ""


def _ts(days_ago: int = 0) -> float:
    now = datetime.datetime(2026, 8, 30, 12, 0, tzinfo=datetime.timezone.utc)
    return (now - datetime.timedelta(days=days_ago)).timestamp()


def test_message_source_family_normalization_uses_operator_labels() -> None:
    assert normalize_message_source_family("JS8Call") == "js8"
    assert normalize_message_source_family("JS8Spotter") == "spotter"
    assert message_source_label("spotter") == "FIOSpotter"
    assert message_source_label("commstat_rf") == "CommStat RF"


def test_js8_message_summary_preserves_source_and_action_contract() -> None:
    payload = _Payload(grid="DM79QJ", subject="Relay")
    row = _Row(payload=payload, rcv_ts=_ts(), auth_state="valid", auth_trusted=True)

    summary = message_summary_from_row(row, now_ts=_ts())

    assert summary.source_family == "js8"
    assert summary.source_label == "JS8Call"
    assert summary.from_call == "K1ABC"
    assert summary.to_target == "MAGNET"
    assert summary.group == "MAGNET"
    assert summary.provenance is not None
    assert summary.provenance.adapter_label == "JS8Call 3.x"
    assert summary.provenance.radio_short_name == "FIO-A"
    assert summary.provenance.trust_label == "Trusted"
    assert summary.map_hint is not None
    assert summary.map_hint.grid == "DM79QJ"
    assert summary.actions.can_read is True
    assert summary.actions.can_reply is True
    assert summary.actions.can_map is True


def test_seven_day_retention_hides_routine_old_traffic_by_default() -> None:
    payload = _Payload()
    row = _Row(payload=payload, rcv_ts=_ts(DEFAULT_TRAFFIC_RETENTION_DAYS + 1), status="READ")

    summary = message_summary_from_row(row, now_ts=_ts())

    assert summary.severity == "routine"
    assert summary.visible_by_default is False


def test_old_urgent_traffic_remains_visible_as_event_storyline_candidate() -> None:
    payload = _Payload(alert_color="red")
    row = _Row(payload=payload, rcv_ts=_ts(30), status="READ")

    summary = message_summary_from_row(row, now_ts=_ts())

    assert summary.severity == "urgent"
    assert summary.visible_by_default is True


def test_file_record_summary_exposes_native_open_but_not_reply_without_context(tmp_path: Path) -> None:
    path = tmp_path / "N1MAG-CO-RR-260830-1200Z-Report.k2s"
    path.write_text("FORM", encoding="utf-8")
    rec = FileRecord(path=path, origin="flmsg", size=4, mtime=_ts())
    row = _Row(
        msg_type="FLMSG",
        status="NEW",
        from_call="N1MAG",
        to_call="AMRRON",
        rcv_ts=_ts(),
        title="Situation Report",
        origin="flmsg",
        payload=rec,
        topics=("General Intel",),
    )

    summary = message_summary_from_row(row, now_ts=_ts())

    assert summary.source_family == "flmsg"
    assert summary.actions.can_open_native is True
    assert summary.actions.can_reply is False
    assert summary.actions.can_delete is True
    assert summary.actions.can_map is False
    assert summary.actions.disabled_reason == "No known state, grid, or coordinates."


def test_message_summary_helpers_filter_without_widget_state() -> None:
    now_ts = _ts()
    rows = [
        _Row(
            origin="js8",
            payload=_Payload(source_key="js8:fio-a", grid="DM79QJ"),
            rcv_ts=now_ts,
            topics=("Comms",),
        ),
        _Row(
            origin="commstat",
            payload=_Payload(source_key="commstat:fio-b", state_code="CO", alert_color="yellow"),
            rcv_ts=_ts(2),
            topics=("Water",),
            title="Water status yellow",
            actionable=True,
        ),
        _Row(
            origin="varac",
            payload=_Payload(source_key="varac:fio-a"),
            rcv_ts=_ts(30),
            topics=("General Intel",),
            status="READ",
        ),
    ]

    summaries = message_summaries_from_rows(rows, now_ts=now_ts)

    assert message_summary_source_counts(summaries) == {
        "CommStat RF": 1,
        "JS8Call": 1,
        "VarAC": 1,
    }
    assert [summary.source_family for summary in default_visible_message_summaries(summaries)] == [
        "js8",
        "commstat",
    ]
    assert [summary.source_family for summary in filter_message_summaries(summaries, topic="Water")] == [
        "commstat"
    ]
    assert [summary.source_family for summary in filter_message_summaries(summaries, grid="DM79")] == [
        "js8"
    ]
    assert filter_message_summaries(summaries, source_family="varac") == ()
    assert [summary.source_family for summary in filter_message_summaries(summaries, source_family="varac", include_hidden=True)] == [
        "varac"
    ]
