from __future__ import annotations

import datetime
import json
import sqlite3
from pathlib import Path

from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.message_projection_store import (
    ensure_message_projection_schema,
    list_projected_messages,
)
from freqinout.core.message_source_projectors import project_native_message_sources
from freqinout.core.settings_manager import SettingsManager


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_js8(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE js8_messages (
            id INTEGER PRIMARY KEY,
            from_call TEXT,
            to_call TEXT,
            msg_type TEXT,
            utc_str TEXT,
            utc_ts REAL,
            raw_text TEXT,
            decoded_text TEXT,
            state TEXT,
            read_ts REAL,
            flag_state INTEGER DEFAULT 0,
            source_key TEXT,
            source_id INTEGER,
            source_radio_id TEXT,
            js8_instance_id TEXT,
            source_path TEXT
        )
        """
    )


def test_projection_hides_js8_protocol_noise_but_keeps_source_evidence(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = _connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        _ensure_js8(conn)
        rows = [
            (1, "K1NOISE", "@MAGNET", "SNR? SNR?"),
            (2, "K2NOISE", "@MAGNET", "QUERY MSGS QUERY MSGS"),
            (
                3,
                "K3HUMAN",
                "@MAGNET",
                "GOOD AFTERNOON N1MAG GOOD AFTERNOON N1MAG",
            ),
        ]
        for source_id, sender, target, payload in rows:
            conn.execute(
                """
                INSERT INTO js8_messages
                    (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text,
                     decoded_text, state, read_ts, source_key, source_id,
                     source_radio_id, js8_instance_id, source_path)
                VALUES (?, ?, ?, 'MSG', '2026-09-08 10:00:00', ?, ?, ?,
                        'UNREAD', 0, 'inbox:fio-a', ?, 'radio-a', 'fio-a', ?)
                """,
                (
                    source_id,
                    sender,
                    target,
                    1799316000 + source_id,
                    payload,
                    payload,
                    source_id,
                    str(tmp_path / "DIRECTED.TXT"),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    assert project_native_message_sources(db_path, sources=("js8",), force=True)["js8"] == 3

    visible = list_projected_messages(db_path, source_family="js8", limit=20)
    assert [row["from_call"] for row in visible] == ["K3HUMAN"]
    human = visible[0]
    assert human["inbox_visible"] == 1
    assert human["body_preview"] == "GOOD AFTERNOON N1MAG"
    assert human["summary"].count("GOOD AFTERNOON N1MAG") == 1

    suppressed = list_projected_messages(
        db_path,
        source_family="js8",
        include_suppressed=True,
        limit=20,
    )
    assert {row["from_call"] for row in suppressed} == {"K1NOISE", "K2NOISE", "K3HUMAN"}
    assert {row["inbox_visible"] for row in suppressed} == {0, 1}

    conn = _connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 3
        active_focus_rows = conn.execute(
            "SELECT COUNT(*) FROM ops_focus_message_entities WHERE deleted=0"
        ).fetchone()[0]
        assert active_focus_rows > 0
        assert conn.execute(
            """
            SELECT COUNT(*)
              FROM ops_focus_message_entities b
              JOIN message_projection p ON p.message_id=b.message_id
             WHERE b.deleted=0 AND p.inbox_visible=0
            """
        ).fetchone()[0] == 0
    finally:
        conn.close()

    assert project_native_message_sources(db_path, sources=("js8",), force=False)["js8"] == 0
    assert len(list_projected_messages(db_path, source_family="js8", limit=20)) == 1
    conn = _connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_external_refs").fetchone()[0] == 3
    finally:
        conn.close()


def _settings(monkeypatch, tmp_path: Path) -> SettingsManager:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = SettingsManager()
    settings.set("operator_callsign", "N1MAG")
    settings.set("operating_groups", [{"group": "MAGNET"}])
    settings.save()
    return settings


def _inbox_row(row_id: int, target: str, payload: str) -> tuple[int, str, str, str]:
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return (
        row_id,
        json.dumps(
            {
                "params": {
                    "FROM": f"K{row_id}TEST",
                    "TO": target,
                    "TEXT": payload,
                    "UTC": now,
                }
            }
        ),
        "RX.DIRECTED",
        payload,
    )


def test_inbox_ingest_advances_checkpoint_over_noise_and_keeps_relevant_traffic(
    monkeypatch, tmp_path: Path
) -> None:
    settings = _settings(monkeypatch, tmp_path)
    inbox_path = tmp_path / "js8.db"
    conn = sqlite3.connect(inbox_path)
    try:
        conn.execute("CREATE TABLE inbox (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
        conn.executemany(
            "INSERT INTO inbox(id, json, type, value) VALUES (?, ?, ?, ?)",
            (
                _inbox_row(1, "@MAGNET", "SNR?"),
                _inbox_row(2, "@OTHER", "NOT FOR THIS STATION"),
                _inbox_row(3, "N1MAG", "ACK ACK"),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    ingestor = MessageIngestor(settings)
    ingestor.ingest_js8_messages(
        inbox_path=inbox_path,
        source_key="inbox:fio-a",
        source_radio_id="radio-a",
        js8_instance_id="fio-a",
    )
    db_path = settings.config_dir / "freqinout_nets.db"
    conn = _connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM js8_messages").fetchone()[0] == 0
        assert conn.execute(
            "SELECT last_source_id FROM js8_ingest_checkpoint WHERE source_key='inbox:fio-a'"
        ).fetchone()[0] == 3
    finally:
        conn.close()

    conn = sqlite3.connect(inbox_path)
    try:
        conn.executemany(
            "INSERT INTO inbox(id, json, type, value) VALUES (?, ?, ?, ?)",
            (
                _inbox_row(4, "@MAGNET", "NEED WATER"),
                _inbox_row(5, "N1MAG", "GOOD AFTERNOON N1MAG"),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    ingestor.ingest_js8_messages(
        inbox_path=inbox_path,
        source_key="inbox:fio-a",
        source_radio_id="radio-a",
        js8_instance_id="fio-a",
    )
    conn = _connect(db_path)
    try:
        rows = [
            tuple(row)
            for row in conn.execute(
                "SELECT from_call, to_call, raw_text FROM js8_messages ORDER BY source_id"
            ).fetchall()
        ]
        assert rows == [
            ("K4TEST", "@MAGNET", "NEED WATER"),
            ("K5TEST", "N1MAG", "GOOD AFTERNOON N1MAG"),
        ]
        assert conn.execute(
            "SELECT last_source_id FROM js8_ingest_checkpoint WHERE source_key='inbox:fio-a'"
        ).fetchone()[0] == 5
    finally:
        conn.close()
    settings.close()
