from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import time
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.gui.controlfreq_tab import ControlFreqTab


def _app():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication([])


def _write_settings_db(
    cfg_root: Path,
    *,
    operating_groups: list[dict[str, object]],
    daily_rows: list[dict[str, object]] | None = None,
) -> None:
    db_path = cfg_root / "config" / "freqinout.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_schedule_tab (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_utc TEXT,
            band TEXT,
            mode TEXT,
            vfo TEXT,
            frequency TEXT,
            start_utc TEXT,
            end_utc TEXT,
            group_name TEXT,
            auto_tune INTEGER DEFAULT 0
        )
        """
    )
    cur.execute(
        "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
        ("operating_groups", json.dumps(operating_groups)),
    )
    if daily_rows:
        cur.executemany(
            """
            INSERT INTO daily_schedule_tab(day_utc, band, mode, vfo, frequency, start_utc, end_utc, group_name, auto_tune)
            VALUES(:day_utc, :band, :mode, :vfo, :frequency, :start_utc, :end_utc, :group_name, :auto_tune)
            """,
            daily_rows,
        )
    conn.commit()
    conn.close()


def _write_nets_db(
    cfg_root: Path,
    *,
    js8_links: list[tuple[float, str, str, float, str, float]],
    operator_rows: list[tuple[str, str, str, str, str]] | None = None,
) -> None:
    db_path = cfg_root / "config" / "freqinout_nets.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_links (
            ts REAL,
            origin TEXT,
            destination TEXT,
            snr REAL,
            band TEXT,
            freq_hz REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_messages (
            id INTEGER PRIMARY KEY,
            from_call TEXT,
            utc_ts REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spotter_traffic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_call TEXT,
            utc_ts REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_messages (
            id TEXT PRIMARY KEY,
            from_call TEXT,
            ts REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fldigi_checkins (
            callsign TEXT PRIMARY KEY,
            last_seen_ts REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS operator_checkins (
            callsign TEXT PRIMARY KEY,
            group1 TEXT,
            group2 TEXT,
            group3 TEXT,
            groups_json TEXT
        )
        """
    )
    cur.executemany(
        "INSERT INTO js8_links(ts, origin, destination, snr, band, freq_hz) VALUES(?, ?, ?, ?, ?, ?)",
        js8_links,
    )
    if operator_rows:
        cur.executemany(
            """
            INSERT INTO operator_checkins(callsign, group1, group2, group3, groups_json)
            VALUES(?, ?, ?, ?, ?)
            """,
            operator_rows,
        )
    conn.commit()
    conn.close()


def _activity_rows(tab: ControlFreqTab) -> list[list[str]]:
    rows: list[list[str]] = []
    for r in range(tab.activity_table.rowCount()):
        rows.append(
            [
                tab.activity_table.item(r, c).text() if tab.activity_table.item(r, c) else ""
                for c in range(tab.activity_table.columnCount())
            ]
        )
    return rows


def test_activity_window_uses_recent_traffic_without_schedule_start_narrowing(monkeypatch, tmp_path):
    _app()
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    now_utc = dt.datetime.now(dt.timezone.utc)
    recent_start = (now_utc - dt.timedelta(minutes=30)).strftime("%H:%M")
    recent_end = (now_utc + dt.timedelta(minutes=30)).strftime("%H:%M")
    _write_settings_db(
        cfg_root,
        operating_groups=[
            {"group": "MAGNET", "band": "20M", "frequency": "14.115"},
            {"group": "MAGNET", "band": "40M", "frequency": "7.115"},
            {"group": "MAGNET", "band": "80M", "frequency": "3.585"},
        ],
        daily_rows=[
            {
                "day_utc": "ALL",
                "band": "80M",
                "mode": "Digi",
                "vfo": "A",
                "frequency": "3.585",
                "start_utc": recent_start,
                "end_utc": recent_end,
                "group_name": "MAGNET",
                "auto_tune": 0,
            }
        ],
    )
    now_ts = time.time()
    _write_nets_db(
        cfg_root,
        js8_links=[
            (now_ts - 600, "@MAGNET", "W6ZYC", -3.0, "20M", 14_115_000.0),
            (now_ts - 540, "N1MAG", "W6ZYC", -2.0, "20M", 14_115_000.0),
            (now_ts - 480, "KG5RKW", "N1MAG", -1.0, "20M", 14_115_000.0),
        ],
    )

    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)
    tab = ControlFreqTab()
    try:
        idx = tab.activity_window_combo.findData(360)
        tab.activity_window_combo.setCurrentIndex(idx)
        tab._refresh_activity()
        rows = _activity_rows(tab)
    finally:
        tab.deleteLater()

    assert rows == [["MAGNET", "20M/40M… 14.115, 7.115…", "3", "3"]]


def test_activity_refresh_reuses_cache_when_inputs_do_not_change(monkeypatch, tmp_path):
    _app()
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    _write_settings_db(
        cfg_root,
        operating_groups=[{"group": "MAGNET", "band": "20M", "frequency": "14.115"}],
    )
    now_ts = time.time()
    _write_nets_db(
        cfg_root,
        js8_links=[(now_ts - 300, "N1MAG", "W6ZYC", -1.0, "20M", 14_115_000.0)],
    )

    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)
    tab = ControlFreqTab()
    try:
        idx = tab.activity_window_combo.findData(360)
        tab.activity_window_combo.setCurrentIndex(idx)
        tab._refresh_activity()
        monkeypatch.setattr(
            tab,
            "_compute_activity_rows",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("activity cache should have been reused")),
        )
        tab._refresh_activity()
        rows = _activity_rows(tab)
    finally:
        tab.deleteLater()

    assert rows == [["MAGNET", "20M 14.115", "2", "1"]]


def test_db_initializer_adds_controlfreq_support_indexes_for_existing_tables(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    cfg_dir = cfg_root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)

    settings_db = cfg_dir / "freqinout.db"
    conn = sqlite3.connect(settings_db)
    conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    conn.close()

    nets_db = cfg_dir / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    conn.execute("CREATE TABLE IF NOT EXISTS js8_messages (id INTEGER PRIMARY KEY, from_call TEXT, utc_ts REAL)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS spotter_traffic (id INTEGER PRIMARY KEY AUTOINCREMENT, from_call TEXT, utc_ts REAL)"
    )
    conn.execute("CREATE TABLE IF NOT EXISTS fldigi_checkins (callsign TEXT PRIMARY KEY, last_seen_ts REAL)")
    conn.commit()
    conn.close()

    from freqinout.core.db_initializer import ensure_all_tables

    ensure_all_tables()

    conn = sqlite3.connect(nets_db)
    try:
        js8_indexes = {row[1] for row in conn.execute("PRAGMA index_list('js8_messages')").fetchall()}
        spotter_indexes = {row[1] for row in conn.execute("PRAGMA index_list('spotter_traffic')").fetchall()}
        fldigi_indexes = {row[1] for row in conn.execute("PRAGMA index_list('fldigi_checkins')").fetchall()}
    finally:
        conn.close()

    assert "idx_js8_messages_utc_ts" in js8_indexes
    assert "idx_spotter_traffic_utc_ts" in spotter_indexes
    assert "idx_fldigi_checkins_last_seen_ts" in fldigi_indexes
