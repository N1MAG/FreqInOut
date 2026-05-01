from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _nets_db(tmp_path: Path) -> Path:
    return tmp_path / "profile" / "config" / "freqinout_nets.db"


def test_peer_schedule_schema_adds_manual_provenance_columns(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    nets_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(nets_db)
    try:
        conn.execute(
            """
            CREATE TABLE peer_hf_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_callsign TEXT NOT NULL,
                day_utc TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                meta_json TEXT,
                imported_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    db_initializer.ensure_nets_tables()

    conn = sqlite3.connect(nets_db)
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(peer_hf_schedule)").fetchall()}
    finally:
        conn.close()

    assert "source_type" in cols
    assert "updated_at" in cols


def test_db_admin_upgrades_existing_peer_schedule_schema(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    nets_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(nets_db)
    try:
        conn.execute(
            """
            CREATE TABLE peer_hf_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_callsign TEXT NOT NULL,
                day_utc TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                band TEXT NOT NULL,
                mode TEXT NOT NULL,
                frequency TEXT NOT NULL,
                meta_json TEXT,
                imported_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    tools_dir = Path(__file__).resolve().parents[1] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))

    import db_admin

    db_admin = importlib.reload(db_admin)
    db_admin.ensure_tables(["peer_hf_schedule"])

    conn = sqlite3.connect(nets_db)
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(peer_hf_schedule)").fetchall()}
    finally:
        conn.close()

    assert "source_type" in cols
    assert "updated_at" in cols


def test_effective_view_uses_manual_rows_and_suppresses_inferred(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    db_initializer.ensure_nets_tables()

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        conn.execute(
            """
            INSERT INTO peer_hf_schedule
                (owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency, source_type, updated_at)
            VALUES ('W1AAA', 'Monday', '18:00', '18:30', '40M', 'JS8', '7.078', 'MANUAL', '2026-04-29T00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO peer_hf_schedule_inferred
                (owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency, confidence)
            VALUES ('W1AAA', 'Monday', '18:00', '18:30', '40M', 'JS8', '7.078', 0.85)
            """
        )
        conn.execute(
            """
            INSERT INTO peer_hf_schedule_inferred
                (owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency, confidence)
            VALUES ('W2BBB', 'Tuesday', '19:00', '19:30', '80M', 'JS8', '3.578', 0.75)
            """
        )
        conn.commit()

        rows = conn.execute(
            """
            SELECT owner_callsign, source_type
            FROM peer_hf_schedule_effective
            ORDER BY owner_callsign
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("W1AAA", "MANUAL"), ("W2BBB", "INFERRED")]


def test_upsert_operator_metadata_is_conservative(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.checkins_db as checkins_db

    checkins_db = importlib.reload(checkins_db)
    checkins_db.upsert_checkins(
        [
            {
                "callsign": "W1AAA",
                "name": "Alice",
                "state": "CO",
                "group1": "MAGNET",
                "trusted": 1,
                "last_seen_utc": "2026-04-20T12:00:00",
            }
        ]
    )
    checkins_db.upsert_operator_metadata(
        [
            {
                "callsign": "W1AAA",
                "name": "",
                "state": "",
                "groups_json": json.dumps(["AMRRON", "MAGNET"]),
                "last_seen_utc": "2026-04-29T09:15:00",
                "trusted": 0,
            }
        ]
    )

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        row = conn.execute(
            """
            SELECT name, state, group1, group2, checkin_count, trusted, last_seen_utc
            FROM operator_checkins
            WHERE callsign='W1AAA'
            """
        ).fetchone()
    finally:
        conn.close()

    assert row == ("Alice", "CO", "AMRRON", "MAGNET", 1, 1, "2026-04-29T09:15:00")


def test_save_explicit_peer_schedule_rows_clears_inferred_and_upserts_operator(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.db_initializer as db_initializer
    import freqinout.gui.peer_sched_tab as peer_sched_tab

    db_initializer = importlib.reload(db_initializer)
    peer_sched_tab = importlib.reload(peer_sched_tab)
    db_initializer.ensure_nets_tables()

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        conn.execute(
            """
            INSERT INTO peer_hf_schedule_inferred
                (owner_callsign, day_utc, start_utc, end_utc, band, mode, frequency, confidence)
            VALUES ('W3CCC', 'Wednesday', '20:00', '20:30', '40M', 'JS8', '7.078', 0.9)
            """
        )
        conn.commit()
    finally:
        conn.close()

    inserted = peer_sched_tab.save_explicit_peer_schedule_rows(
        nets_db,
        "W3CCC",
        [
            {
                "day_utc": "Wednesday",
                "start_utc": "20:00",
                "end_utc": "20:30",
                "band": "40M",
                "mode": "JS8",
                "frequency": "7.078",
                "notes": "Manual",
            }
        ],
        source_type="MANUAL",
        replace_callsign=True,
        operator_profile={
            "name": "Carol",
            "state": "UT",
            "groups_json": json.dumps(["MAGNET"]),
        },
    )

    conn = sqlite3.connect(nets_db)
    try:
        explicit = conn.execute(
            "SELECT owner_callsign, source_type FROM peer_hf_schedule WHERE owner_callsign='W3CCC'"
        ).fetchall()
        inferred = conn.execute(
            "SELECT COUNT(*) FROM peer_hf_schedule_inferred WHERE owner_callsign='W3CCC'"
        ).fetchone()[0]
        operator = conn.execute(
            "SELECT name, state, group1, trusted FROM operator_checkins WHERE callsign='W3CCC'"
        ).fetchone()
    finally:
        conn.close()

    assert inserted == 1
    assert explicit == [("W3CCC", "MANUAL")]
    assert inferred == 0
    assert operator == ("Carol", "UT", "MAGNET", 0)
