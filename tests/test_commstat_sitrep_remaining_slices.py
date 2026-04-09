from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core import sitrep_fusion
from freqinout.core.sitrep_fusion import _ensure_tables as ensure_fusion_tables
from freqinout.core.sitrep_ingest import _ensure_local_tables, _insert_source_event
from freqinout.gui.message_viewer_tab import MessageViewerTab, UnifiedMessage


class DummySettings:
    def __init__(self, values: dict[str, object] | None = None):
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)


def _prepare_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        _ensure_local_tables(conn)
        ensure_fusion_tables(conn)
        conn.commit()
    finally:
        conn.close()


def _insert_commstat_row(
    conn: sqlite3.Connection,
    *,
    source: str,
    source_table: str,
    source_id: int,
    from_call: str,
    target: str,
    report_group: str,
    grid: str,
    transport_mode: str,
    event_ts: float,
    event_ts_utc: str,
    sr_id: str,
    status: str = "321311111331",
    subtype: str = "COMMSTAT_12",
    state_code: str = "OR",
    state_confidence: str = "grid4_remarks",
    geo_confidence: str = "grid4_state",
) -> None:
    _insert_source_event(
        conn,
        source=source,
        source_table=source_table,
        source_db_path="traffic.db3",
        source_id=source_id,
        subtype=subtype,
        from_call=from_call,
        target=target,
        report_group=report_group,
        grid=grid,
        scope="My County",
        transport_mode=transport_mode,
        remarks_text="NTR OR 4BBGUB",
        brevity_code="4BBGUB",
        brevity_summary="4BBGUB: Bridge Failure | Pending | Grid Down | Unrest | Backup Power",
        state_code=state_code,
        state_confidence=state_confidence,
        geo_confidence=geo_confidence,
        status_payload={"status": status, "scope": "My County"},
        raw_payload={"sr_id": sr_id, "message": "demo"},
        event_ts=event_ts,
        event_ts_utc=event_ts_utc,
    )


def test_fusion_merges_transport_syncs_operator_and_builds_state_rollup(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    _prepare_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        _insert_commstat_row(
            conn,
            source="COMMSTAT3",
            source_table="statrep",
            source_id=1,
            from_call="N0DDK",
            target="@MAGNET",
            report_group="@MAGNET",
            grid="EM83",
            transport_mode="js8",
            event_ts=1_710_000_000.0,
            event_ts_utc="2026-04-09 10:00:00",
            sr_id="SR-1",
        )
        _insert_commstat_row(
            conn,
            source="COMMSTAT3",
            source_table="messages",
            source_id=2,
            from_call="N0DDK",
            target="@MAGNET",
            report_group="@MAGNET",
            grid="EM83",
            transport_mode="internet",
            event_ts=1_710_000_000.0,
            event_ts_utc="2026-04-09 10:00:00",
            sr_id="SR-1",
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(sitrep_fusion, "_local_db_path", lambda: db_path)
    monkeypatch.setattr(sitrep_fusion, "_LAST_RUN_MONO", 0.0)
    stats = sitrep_fusion.fuse_sitreps(DummySettings({"sitrep_unified_fusion_enabled": True}), max_rows=100)

    assert stats["events_upserted"] >= 1
    assert stats["latest_updated"] == 1
    assert stats["operators_synced"] >= 1
    assert stats["state_rollups_updated"] >= 2

    conn = sqlite3.connect(db_path)
    try:
        count_row = conn.execute("SELECT COUNT(*) FROM sitrep_events WHERE from_call='N0DDK'").fetchone()
        assert count_row == (1,)
        event_row = conn.execute(
            """
            SELECT transport_mode, report_group, state_code, remarks_text, brevity_summary, sources_json
            FROM sitrep_events
            WHERE from_call='N0DDK'
            """
        ).fetchone()
        assert event_row is not None
        assert event_row[0] == "js8+internet"
        assert event_row[1] == "@MAGNET"
        assert event_row[2] == "OR"
        assert "4BBGUB" in event_row[4]

        latest_row = conn.execute(
            """
            SELECT latest_report_group, latest_transport_mode, latest_state_code, source_summary_json
            FROM sitrep_latest_by_callsign
            WHERE callsign='N0DDK'
            """
        ).fetchone()
        assert latest_row == ("@MAGNET", "js8+internet", "OR", json.dumps({"COMMSTAT": "red"}, separators=(",", ":"), ensure_ascii=True))

        operator_row = conn.execute(
            """
            SELECT trusted, state, grid, group1, groups_json, checkin_count
            FROM operator_checkins
            WHERE callsign='N0DDK'
            """
        ).fetchone()
        assert operator_row is not None
        assert operator_row[0] == 0
        assert operator_row[1] == "OR"
        assert operator_row[2] == "EM83"
        assert operator_row[3] == "@MAGNET"
        assert json.loads(operator_row[4]) == ["@MAGNET"]
        assert operator_row[5] == 0

        roll_all = conn.execute(
            """
            SELECT callsign_count, red_count, mixed_transport_count
            FROM sitrep_state_rollup
            WHERE report_group='__ALL__' AND state_code='OR'
            """
        ).fetchone()
        assert roll_all == (1, 1, 1)
        roll_group = conn.execute(
            """
            SELECT callsign_count, red_count, mixed_transport_count
            FROM sitrep_state_rollup
            WHERE report_group='@MAGNET' AND state_code='OR'
            """
        ).fetchone()
        assert roll_group == (1, 1, 1)
    finally:
        conn.close()


def test_fusion_prefers_richer_commstat_subtype_when_timestamps_tie(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    _prepare_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        _insert_source_event(
            conn,
            source="JS8SPOTTER",
            source_table="forms",
            source_db_path="js8spotter.db",
            source_id=1,
            subtype="SPOTTER_304",
            from_call="W1ABC",
            target="@MAGNET",
            grid="FN31",
            scope="My Location",
            status_payload={"responses": "11111111"},
            raw_payload={"msg_id": "SPOT-1"},
            event_ts=1_710_000_100.0,
            event_ts_utc="2026-04-09 10:01:40",
        )
        _insert_commstat_row(
            conn,
            source="COMMSTAT3",
            source_table="statrep",
            source_id=2,
            from_call="W1ABC",
            target="@MAGNET",
            report_group="@MAGNET",
            grid="FN31",
            transport_mode="js8",
            event_ts=1_710_000_100.0,
            event_ts_utc="2026-04-09 10:01:40",
            sr_id="SR-2",
            status="111111111111",
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(sitrep_fusion, "_local_db_path", lambda: db_path)
    monkeypatch.setattr(sitrep_fusion, "_LAST_RUN_MONO", 0.0)
    sitrep_fusion.fuse_sitreps(DummySettings({"sitrep_unified_fusion_enabled": True}), max_rows=100)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT latest_subtype, latest_transport_mode, source_summary_json
            FROM sitrep_latest_by_callsign
            WHERE callsign='W1ABC'
            """
        ).fetchone()
        assert row is not None
        assert row[0] == "COMMSTAT_12"
        assert row[1] == "js8"
        assert json.loads(row[2]) == {"COMMSTAT": "green", "JS8SPOTTER": "green"}
    finally:
        conn.close()


def test_message_viewer_loads_commstat_sitrep_with_family_transport_and_filter_labels(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    _prepare_db(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO sitrep_events (
                report_key, event_ts, event_ts_utc, from_call, target, report_group, grid, state_code, state_confidence, geo_confidence,
                scope, overall_status, power, water, medical, communications, internet, travel, food, fuel, crime, civil_unrest, political,
                subtype, transport_mode, remarks_text, brevity_code, brevity_summary,
                source_first, source_last, sources_json, source_count, source_refs_json, raw_payload_json, inserted_ts, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "rk-1",
                1_710_000_200.0,
                "2026-04-09 10:03:20",
                "K7ABC",
                "@MAGNET",
                "@MAGNET",
                "EM83",
                "OR",
                "grid4_remarks",
                "grid4_state",
                "My County",
                "yellow",
                "green",
                "green",
                "green",
                "yellow",
                "green",
                "green",
                "green",
                "green",
                "green",
                "green",
                "green",
                "COMMSTAT_12",
                "js8+internet",
                "NTR OR 4BBGUB",
                "4BBGUB",
                "4BBGUB: Bridge Failure | Pending | Grid Down | Unrest | Backup Power",
                "COMMSTAT3",
                "COMMSTAT3",
                json.dumps(["COMMSTAT3", "COMMSTAT23"], separators=(",", ":"), ensure_ascii=True),
                2,
                json.dumps(["statrep:1", "messages:2"], separators=(",", ":"), ensure_ascii=True),
                json.dumps({"sr_id": "SR-3"}, separators=(",", ":"), ensure_ascii=True),
                1_710_000_200.0,
                1_710_000_200.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    app = QApplication.instance() or QApplication([])
    monkeypatch.setattr(MessageViewerTab, "_initial_refresh", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_js8_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_pending_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_setup_bbs_auto_archive_timer", lambda self: None)
    monkeypatch.setattr(MessageViewerTab, "_db_path", lambda self: db_path)

    tab = MessageViewerTab()
    tab._load_sitrep_from_local(rebuild=False)

    assert len(tab.sitrep_messages) == 1
    msg = tab.sitrep_messages[0]
    assert msg.subtype_label == "COMMSTAT"
    assert msg.source_family_label == "CommStat"
    assert msg.transport_label == "JS8 + Internet"
    assert msg.report_group == "@MAGNET"

    row = UnifiedMessage(
        msg_type="SitRep",
        status="INFO",
        from_call=msg.from_call,
        to_call=msg.target,
        rcv_ts=msg.event_ts,
        rcv_display="",
        title=msg.subtype_label,
        origin="sitrep",
        payload=msg,
    )
    assert tab._message_source_identity(row) == "CommStat"
    assert tab._row_matches_type_filter(row, "SitRep/COMMSTAT") is True
    assert tab._row_matches_type_filter(row, "SitRep/COMMSTAT FWD") is False

    tab.deleteLater()
    app.processEvents()
