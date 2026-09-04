from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.gui.controlfreq_tab import (
    ControlFreqTab,
    PeerRendezvousDelegate,
    TrafficVolumeBarDelegate,
    TRAFFIC_CHART_CURRENT_ROLE,
    TRAFFIC_CHART_PREVIOUS_ROLE,
)
from freqinout.core.controlfreq_awareness import AttentionItem, build_radio_source_lanes
from freqinout.core.observation_projection import Observation, observation_from_rf_pin
from freqinout.core.observation_store import upsert_observation
from freqinout.core.traffic_actionability import TrafficGroupVolume
from freqinout.core.ops_focus import OpsFocus, OpsFocusSnapshot, OpsHistoricalSummary


def _app():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication([])


def test_controlfreq_focus_banner_separates_current_scope_and_last_known(monkeypatch, tmp_path):
    _app()
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)
    tab = ControlFreqTab()
    try:
        focus = OpsFocus("callsign", "operator-1", "K1NEW", "K1OLD", "operator_history", "operator-1")
        tab._on_filters_changed = lambda *_args: None
        tab._retarget_propagation_for_focus = lambda *_args: None
        tab._apply_ops_focus(focus)
        tab._focus_snapshot_request_id = 7
        snapshot = OpsFocusSnapshot(
            focus=focus,
            generated_at=1_725_600_000.0,
            current_count=0,
            current_scope_summary="No traffic received in selected 24 hours",
            historical_summary=OpsHistoricalSummary(
                entity_kind="callsign",
                entity_id="operator-1",
                latest_received_at=1_722_576_000.0,
                latest_status_at_receipt="green",
                latest_source="js8call",
                latest_group="MR08",
                latest_summary="Operations normal",
            ),
            aliases=("K1NEW", "K1OLD"),
        )
        tab._on_focus_snapshot_ready(7, snapshot, "")

        assert tab.focus_banner.isHidden() is False
        assert "formerly K1OLD" in tab.focus_title_label.text()
        assert "No traffic received in selected 24 hours" in tab.focus_current_label.text()
        assert "Last known" in tab.focus_history_label.text()
        assert "reported green" in tab.focus_history_label.text()

        tab._clear_ops_focus(refresh=False)
        assert tab.focus_banner.isHidden()
        assert tab._active_ops_focus is None
    finally:
        tab.deleteLater()


def test_controlfreq_dark_semantic_panel_colors_are_readable() -> None:
    class FakeSettings:
        def get(self, key: str, default=None):
            if key == "ui_theme":
                return "dark"
            return default

    tab = ControlFreqTab.__new__(ControlFreqTab)
    tab.settings = FakeSettings()
    tab._theme_cache = None

    assert tab._semantic_panel_colors("warning") == ("#3A3015", "#FFF0BE", "#A06F18")
    assert tab._semantic_panel_colors("success") == ("#173822", "#E8F6EA", "#39874D")
    assert tab._semantic_panel_colors("secondary") == ("#16263A", "#E8F1FF", "#2E4A68")


def test_controlfreq_dark_chart_uses_contrasting_warning_palette() -> None:
    from PySide6.QtGui import QColor
    from PySide6.QtWidgets import QTableWidget

    _app()

    class FakeSettings:
        def get(self, key: str, default=None):
            return "dark" if key == "ui_theme" else default

    tab = ControlFreqTab.__new__(ControlFreqTab)
    tab.settings = FakeSettings()
    tab._theme_cache = None
    tab.traffic_group_table = QTableWidget(1, 3)
    tab.traffic_group_bar_delegate = TrafficVolumeBarDelegate(tab.traffic_group_table)

    ControlFreqTab._refresh_traffic_group_chart_theme(tab)

    assert QColor(tab.traffic_group_bar_delegate._theme["warning"]).name().upper() == "#D1A000"
    assert QColor(tab.traffic_group_bar_delegate._theme["text"]).name().upper() == "#E7EBF0"


def test_peer_rendezvous_delegate_accepts_dark_theme_palette() -> None:
    from PySide6.QtGui import QColor
    from PySide6.QtWidgets import QTableWidget

    _app()
    table = QTableWidget(1, 1)
    delegate = PeerRendezvousDelegate(table)
    delegate.apply_theme(
        {
            "surface_alt": "#202632",
            "border": "#38414D",
            "text_muted": "#AAB4C0",
            "accent": "#4EA7E5",
            "text": "#E7EBF0",
        }
    )

    assert QColor(delegate._theme["surface_alt"]).name().upper() == "#202632"
    assert QColor(delegate._theme["accent"]).name().upper() == "#4EA7E5"
    assert QColor(delegate._theme["text"]).name().upper() == "#E7EBF0"


def test_controlfreq_traffic_group_source_summary_is_compact() -> None:
    assert ControlFreqTab._traffic_source_summary(
        (("CommStat", 8), ("JS8Call", 4), ("SitRep", 2), ("Spotter", 1))
    ) == "CommStat 8 · JS8Call 4 · SitRep 2 · +1"
    assert ControlFreqTab._traffic_source_summary(()) == "Unknown"


def test_controlfreq_traffic_group_detail_can_collapse_and_persists() -> None:
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QLabel, QTableWidget, QToolButton

    _app()

    class FakeSettings:
        def __init__(self) -> None:
            self.saved: list[tuple[str, object]] = []

        def set(self, key: str, value: object) -> None:
            self.saved.append((key, value))

    tab = ControlFreqTab.__new__(ControlFreqTab)
    tab.settings = FakeSettings()
    tab._responsive_layout_mode = "wide"
    tab.traffic_group_title = QToolButton()
    tab.traffic_group_title.setCheckable(True)
    tab.traffic_group_table = QTableWidget(0, 3)
    tab.traffic_group_hint = QLabel("Trend compares the prior equal window")

    ControlFreqTab._toggle_traffic_group_detail(tab, False)

    assert tab.traffic_group_table.isHidden()
    assert tab.traffic_group_hint.isHidden()
    assert tab.traffic_group_title.arrowType() == Qt.RightArrow
    assert tab.settings.saved == [("controlfreq_traffic_group_expanded", False)]


def test_controlfreq_traffic_group_header_keeps_increasing_aggregate() -> None:
    from PySide6.QtWidgets import QTableWidget, QToolButton

    _app()

    class FakeSettings:
        def get(self, _key: str, default=None):
            return default

    tab = ControlFreqTab.__new__(ControlFreqTab)
    tab.settings = FakeSettings()
    tab._theme_cache = None
    tab._responsive_layout_mode = "wide"
    tab.traffic_group_title = QToolButton()
    tab.traffic_group_table = QTableWidget(0, 3)
    tab.traffic_group_table.setHorizontalHeaderLabels(
        ["Group", "Volume comparison", "Details"]
    )

    ControlFreqTab._render_traffic_group_volumes(
        tab,
        (
            TrafficGroupVolume(
                group="MR08",
                sources=(("CommStat", 8), ("JS8Call", 2)),
                is_operator_group=True,
                unread_count=3,
                current_count=10,
                previous_count=2,
                trend="Spike ↑",
            ),
            TrafficGroupVolume(
                group="MAGNET",
                sources=(("JS8Call", 4),),
                unread_count=1,
                current_count=4,
                previous_count=3,
                trend="Rising ↑",
            ),
            TrafficGroupVolume(
                group="AMRRON",
                sources=(("Spotter", 2),),
                unread_count=0,
                current_count=2,
                previous_count=2,
                trend="Steady →",
            ),
        ),
    )

    assert tab.traffic_group_title.text() == (
        "Traffic by group · 16 total / 4 new · 2 increasing"
    )
    assert tab.traffic_group_table.horizontalHeaderItem(1).text() == "Volume comparison"
    assert tab.traffic_group_table.item(0, 1).text() == "10 current · 2 prior"
    assert tab.traffic_group_table.item(0, 1).data(TRAFFIC_CHART_CURRENT_ROLE) == 10
    assert tab.traffic_group_table.item(0, 1).data(TRAFFIC_CHART_PREVIOUS_ROLE) == 2
    assert tab.traffic_group_table.item(0, 2).text() == (
        "Spike ↑ · 3 new · CommStat 8 · JS8Call 2 · latest —"
    )
    assert "My group" not in tab.traffic_group_table.item(0, 2).text()


def test_controlfreq_chart_activation_drills_group_from_any_chart_cell(monkeypatch) -> None:
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QTableWidget, QTableWidgetItem

    _app()
    tab = ControlFreqTab.__new__(ControlFreqTab)
    tab.traffic_group_table = QTableWidget(1, 3)
    tab.traffic_source_combo = SimpleNamespace(currentData=lambda: "commstat")
    tab._traffic_age_seconds = lambda: 6 * 60 * 60
    opened: list[tuple[str, dict[str, object]]] = []
    host = SimpleNamespace(
        open_messages_section=lambda section, **kwargs: opened.append((section, kwargs))
    )
    monkeypatch.setattr(ControlFreqTab, "window", lambda _self: host)
    bar_item = QTableWidgetItem("10 current · 2 prior")
    bar_item.setData(Qt.UserRole, "MR08")
    tab.traffic_group_table.setItem(0, 1, bar_item)

    ControlFreqTab._open_traffic_group_row(tab, bar_item)

    assert opened == [
        (
            "inbox",
            {
                "group_filter": "MR08",
                "age_filter_seconds": 6 * 60 * 60,
                "source_family": "commstat",
            },
        )
    ]


def test_controlfreq_sources_distinguish_commstat_from_sitrep_aggregate(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE js8_messages (from_call TEXT, state TEXT);
        CREATE TABLE spotter_traffic (from_call TEXT, state TEXT);
        CREATE TABLE varac_messages (from_call TEXT, read_status INTEGER);
        CREATE TABLE operator_checkins (
            callsign TEXT, group1 TEXT, group2 TEXT, group3 TEXT, groups_json TEXT
        );
        CREATE TABLE message_projection (
            from_call TEXT, read_state TEXT, status TEXT, group_name TEXT,
            source_family TEXT, deleted INTEGER, archived INTEGER
        );
        CREATE TABLE sitrep_latest_by_callsign (
            callsign TEXT, effective_status TEXT, latest_report_group TEXT,
            source_summary_json TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO operator_checkins VALUES (?, ?, ?, ?, ?)",
        ("N1MAG", "MR08", "", "", "[]"),
    )
    conn.execute(
        "INSERT INTO message_projection VALUES (?, ?, ?, ?, ?, 0, 0)",
        ("K7ETC", "new", "YELLOW", "MR08", "commstat"),
    )
    conn.execute(
        "INSERT INTO sitrep_latest_by_callsign VALUES (?, ?, ?, ?)",
        ("K7ETC", "yellow", "MR08", '{"CommStat": 1}'),
    )
    conn.commit()
    conn.close()

    tab = ControlFreqTab.__new__(ControlFreqTab)
    rows = ControlFreqTab._collect_inbox_rows(
        tab,
        "",
        db_path=db_path,
        group_filter="",
        local_operator_call="N1MAG",
    )

    by_label = {row[0]: row for row in rows}
    assert by_label["CommStat"][1] == "1"
    assert by_label["SitRep Summary"][1] == "1"
    assert by_label["SitRep Summary"][2].startswith("Aggregated station status")


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


def test_activity_panel_summarizes_condition_alert_observations(monkeypatch, tmp_path):
    _app()
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    _write_settings_db(
        cfg_root,
        operating_groups=[{"group": "MAGNET", "band": "20M", "frequency": "14.115"}],
    )
    _write_nets_db(cfg_root, js8_links=[])
    upsert_observation(
        cfg_root / "config" / "freqinout_nets.db",
        Observation(
            observation_id="condition_alert:magcon:test",
            source_family="condition_alert",
            source_ref="spotter:1",
            received_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            event_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            from_call="N1MAG",
            to_target="@MAGNET",
            groups=("MAGNET",),
            observed_topics=("General Intel", "Comms"),
            operator_attention=True,
            status="MAGCON 3",
            subject="MAGCON: Level 3",
            summary="MAGCON level changed",
        ),
    )

    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)

    class _FakeSOPManager:
        def list_profiles(self):
            return [{"id": 7, "name": "MagNet Alert SOP"}]

        def get_profile(self, profile_id):
            assert profile_id == 7
            return {
                "id": 7,
                "name": "MagNet Alert SOP",
                "schedule_layer": [
                    {
                        "group_name": "MAGNET",
                        "condition_levels": "3",
                        "band": "40M",
                        "frequency": "7.115",
                    }
                ],
            }

    tab = ControlFreqTab()
    try:
        tab._sop_manager = _FakeSOPManager()
        tab._refresh_activity()
        headline = tab.operational_activity_label.text()
        topics = tab.operational_topics_label.text()
    finally:
        tab.deleteLater()

    assert "Condition Alert: MAGCON 3" in headline
    assert "N1MAG -> MAGNET" in headline
    assert "SOP: Review MAGNET L3: MagNet Alert SOP" in headline
    assert "Comms" in topics


def test_activity_panel_summarizes_high_attention_topics(monkeypatch, tmp_path):
    _app()
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    _write_settings_db(
        cfg_root,
        operating_groups=[{"group": "MR08", "band": "40M", "frequency": "7.115"}],
    )
    _write_nets_db(cfg_root, js8_links=[])
    upsert_observation(
        cfg_root / "config" / "freqinout_nets.db",
        Observation(
            observation_id="spotter:fire:test",
            source_family="spotter",
            source_ref="spotter:2",
            received_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            event_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            from_call="K7ETC",
            to_target="@MR08",
            groups=("MR08",),
            observed_topics=("Fire", "Logistics"),
            operator_attention=True,
            status="INFO",
            subject="Widemouth 2 Fire",
            summary="Evacuation posture updated",
        ),
    )

    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)
    tab = ControlFreqTab()
    try:
        tab._pending_group_filter = "MR08"
        tab._load_group_combo()
        tab._refresh_activity()
        headline = tab.operational_activity_label.text()
        topics = tab.operational_topics_label.text()
        context = dict(tab._operational_activity_context)
        messages_enabled = tab.operational_messages_btn.isEnabled()
        map_enabled = tab.operational_map_btn.isEnabled()
    finally:
        tab.deleteLater()

    assert "Recent Traffic: 1 recent | 1 need attention" in headline
    assert "Widemouth 2 Fire" in headline
    assert "K7ETC -> MR08" in headline
    assert "Fire" in topics
    assert "Logistics" in topics
    assert context["group_filter"] == "MR08"
    assert context["topic_filter"] == "Fire"
    assert messages_enabled is True
    assert map_enabled is True


def test_activity_panel_summarizes_rf_pin_observations(monkeypatch, tmp_path):
    _app()
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    _write_settings_db(
        cfg_root,
        operating_groups=[{"group": "MAGNET", "band": "40M", "frequency": "7.115"}],
    )
    _write_nets_db(cfg_root, js8_links=[])
    upsert_observation(
        cfg_root / "config" / "freqinout_nets.db",
        observation_from_rf_pin(
            {
                "pin_id": "manual:relay-check",
                "label": "Relay check",
                "target": "MAGNET",
                "groups": ("MAGNET",),
                "topics": ("Comms",),
                "grid": "DM79",
                "status": "PIN",
                "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
        ),
    )

    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)
    tab = ControlFreqTab()
    try:
        tab._pending_group_filter = "MAGNET"
        tab._load_group_combo()
        tab._refresh_activity()
        headline = tab.operational_activity_label.text()
        topics = tab.operational_topics_label.text()
    finally:
        tab.deleteLater()

    assert "Recent Traffic: 1 recent | 1 need attention" in headline
    assert "RF Pin: Relay check" in headline
    assert "MAGNET" in headline
    assert "Comms" in topics


def test_controlfreq_activity_context_navigation_hooks_are_present():
    controlfreq_source = Path("freqinout/gui/controlfreq_tab.py").read_text()
    main_window_source = Path("freqinout/gui/main_window.py").read_text()
    message_viewer_source = Path("freqinout/gui/message_viewer_tab.py").read_text()
    local_reports_source = Path("freqinout/gui/local_report_history_tab.py").read_text()

    assert "_open_operational_activity_messages" in controlfreq_source
    assert "_open_operational_activity_map" in controlfreq_source
    assert "_open_operational_activity_compose" in controlfreq_source
    assert "compose_intent=intent" in controlfreq_source
    assert "_pin_selected_awareness_focus" in controlfreq_source
    assert "_clear_awareness_pins" in controlfreq_source
    assert "controlfreqAwarenessNowNext" in controlfreq_source
    assert "controlfreqPropagationSummary" in controlfreq_source
    assert "def _sync_inbox_summary_visibility" in controlfreq_source
    assert 'saved_preset == "Schedule"' in controlfreq_source
    assert "controlfreq_operations_dashboard_seen" in controlfreq_source
    assert "query_observations" in controlfreq_source
    assert "build_awareness_snapshot" in controlfreq_source
    assert "source_contract_for" in controlfreq_source
    assert "contract.actions.enabled_names()" in controlfreq_source
    assert "self.awareness_table.itemSelectionChanged.connect(self._sync_operational_action_buttons)" in controlfreq_source
    assert "def _sync_operational_action_buttons" in controlfreq_source
    assert "contract.actions.reply or contract.actions.compose" in controlfreq_source
    assert "host.open_local_reports(" in controlfreq_source
    assert "map_context = map_context_from_mapping(context)" in controlfreq_source
    assert "map_context.as_map_kwargs()" in controlfreq_source
    assert "map_context.as_messages_kwargs()" in controlfreq_source
    assert '"group_filter": self.group_filter' in Path("freqinout/core/view_contracts.py").read_text()
    assert "def open_local_reports(self, callsign: str = \"\", *, topic_filter: str = \"\", query: str = \"\")" in main_window_source
    assert "def open_local_reports_map(" in main_window_source
    assert "grid_filter: str = \"\"" in main_window_source
    assert "_messages_nav_filter_context" in main_window_source
    assert "prefill_compose_intent(intent)" in main_window_source
    assert "def show_inbox_with_context" in message_viewer_source
    assert "def show_context(self, *, callsign: str = \"\", topic: str = \"\", query: str = \"\")" in local_reports_source


def test_controlfreq_rendered_inbox_action_routes_to_messages_inbox(monkeypatch) -> None:
    tab = ControlFreqTab.__new__(ControlFreqTab)
    opened: list[tuple[str, dict[str, object]]] = []
    host = SimpleNamespace(open_messages_section=lambda section, **kwargs: opened.append((section, kwargs)))
    tab._operational_activity_context = {
        "source_family": "commstat",
        "group_filter": "MAGNET",
        "topic_filter": "Comms",
        "search_query": "KI6QDB",
        "grid_filter": "DM12MR",
    }
    monkeypatch.setattr(ControlFreqTab, "window", lambda _self: host)

    ControlFreqTab._open_operational_activity_messages(tab)

    assert opened == [
        (
            "inbox",
            {
                "group_filter": "MAGNET",
                "topic_filter": "Comms",
                "query_filter": "KI6QDB",
                "source_family": "commstat",
                "state_filter": "",
                "grid_filter": "DM12MR",
                "fema_region_filter": "",
                "age_filter_seconds": 7 * 24 * 60 * 60,
                "concern_only": False,
            },
        )
    ]


def test_controlfreq_reply_action_routes_only_to_compose(monkeypatch) -> None:
    tab = ControlFreqTab.__new__(ControlFreqTab)
    opened: list[tuple[str, dict[str, object]]] = []
    host = SimpleNamespace(open_messages_section=lambda section, **kwargs: opened.append((section, kwargs)))
    tab._operational_activity_context = {
        "source_family": "commstat",
        "callsign": "KI6QDB",
        "topic_filter": "Comms",
    }
    monkeypatch.setattr(ControlFreqTab, "window", lambda _self: host)

    ControlFreqTab._open_operational_activity_compose(tab)

    assert len(opened) == 1
    section, kwargs = opened[0]
    assert section == "compose"
    assert kwargs["compose_intent"]["mode"] == "commstat_rf"
    assert kwargs["compose_intent"]["recipient_callsign"] == "KI6QDB"
    assert kwargs["compose_intent"]["body"] == "RE Comms: "


def test_controlfreq_map_action_routes_by_source_family(monkeypatch) -> None:
    tab = ControlFreqTab.__new__(ControlFreqTab)
    opened: list[tuple[str, dict[str, object]]] = []
    host = SimpleNamespace(
        open_spotter_map=lambda **kwargs: opened.append(("spotter_map", kwargs)),
        open_local_reports_map=lambda **kwargs: opened.append(("local_reports_map", kwargs)),
    )
    monkeypatch.setattr(ControlFreqTab, "window", lambda _self: host)

    tab._operational_activity_context = {
        "source_family": "spotter",
        "topic_filter": "Fire",
        "group_filter": "MR08",
        "grid_filter": "DM12MR",
    }
    ControlFreqTab._open_operational_activity_map(tab)
    tab._operational_activity_context = {
        "source_family": "local_report",
        "topic_filter": "Power",
        "group_filter": "LOCAL",
        "grid_filter": "DM79QJ",
    }
    ControlFreqTab._open_operational_activity_map(tab)

    assert opened == [
        (
            "spotter_map",
            {
                "group_filter": "MR08",
                "topic_filter": "Fire",
                "query_filter": "",
                "state_filter": "",
                "grid_filter": "DM12MR",
            },
        ),
        (
            "local_reports_map",
            {
                "group_filter": "LOCAL",
                "topic_filter": "Power",
                "query_filter": "",
                "state_filter": "",
                "grid_filter": "DM79QJ",
            },
        ),
    ]


def test_controlfreq_peer_finder_actions_route_to_compose_and_map(monkeypatch) -> None:
    tab = ControlFreqTab.__new__(ControlFreqTab)
    opened_messages: list[tuple[str, dict[str, object]]] = []
    opened_maps: list[dict[str, object]] = []
    host = SimpleNamespace(
        open_messages_section=lambda section, **kwargs: opened_messages.append((section, kwargs)),
        open_spotter_map=lambda **kwargs: opened_maps.append(kwargs),
    )
    tab._peer_finder_contexts = [
        {
            "callsign": "KI6QDB",
            "group_filter": "MAGNET",
            "source_family": "js8",
            "compose_mode": "js8call",
            "search_query": "KI6QDB",
        }
    ]
    monkeypatch.setattr(ControlFreqTab, "window", lambda _self: host)

    ControlFreqTab._open_peer_finder_compose(tab, 0)
    ControlFreqTab._open_peer_finder_map(tab, 0)

    assert opened_messages == [
        (
            "compose",
            {
                "compose_intent": {
                    "mode": "js8",
                    "transport": "js8",
                    "target": "KI6QDB",
                    "recipient_callsign": "KI6QDB",
                    "group": "MAGNET",
                    "source": "context",
                    "source_family": "js8",
                    "age_filter_seconds": 7 * 24 * 60 * 60,
                    "target_callsign": "KI6QDB",
                }
            },
        )
    ]
    assert opened_maps == [
        {
            "group_filter": "MAGNET",
            "topic_filter": "",
            "query_filter": "KI6QDB",
            "state_filter": "",
            "grid_filter": "",
        }
    ]


def test_controlfreq_global_activity_button_language_matches_destination() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert 'self.operational_messages_btn = QPushButton("Inbox")' in source
    assert 'self.operational_messages_btn = QPushButton("Msgs")' not in source
    assert "use Inbox, Reply, or Map from matching traffic" in source


def test_controlfreq_builds_source_lanes_for_active_radios() -> None:
    attention = [
        AttentionItem(
            id="1",
            source_family="commstat",
            source_ref="FIO-B report",
            callsign="KI6QDB",
            subject="CommStat StatRep",
            topics=("Comms",),
        )
    ]

    lanes = build_radio_source_lanes(
        [
            {"id": 1, "name": "FIO-A", "runtime_primary": 1},
            {"id": 2, "name": "FIO-B", "runtime_primary": 0},
        ],
        current_label="MAGNET 40M 7.115 MHz",
        next_label="MAGNET 80M 23:00",
        attention_items=attention,
    )

    assert [lane.short_name for lane in lanes] == ["FIO-A", "FIO-B"]
    assert lanes[0].now == "MAGNET 40M 7.115 MHz"
    assert lanes[0].next == "MAGNET 80M 23:00"
    assert lanes[1].now == ""
    assert lanes[1].attention_count == 1
    assert "KI6QDB" in lanes[1].attention_summary


def test_controlfreq_builds_data_source_lane_for_unassigned_traffic() -> None:
    lanes = build_radio_source_lanes(
        [{"id": 1, "name": "FIO-A", "runtime_primary": 1}],
        current_label="MAGNET 40M 7.115 MHz",
        next_label="MAGNET 80M 23:00",
        attention_items=[
            AttentionItem(
                id="aprs-1",
                source_family="aprs",
                source_ref="object FIRE-1",
                callsign="W0ABC",
                subject="Wildfire object update",
                topics=("Wildfire",),
            )
        ],
    )

    assert [lane.short_name for lane in lanes] == ["FIO-A", "APRS"]
    assert lanes[1].source_kind == "aprs"
    assert lanes[1].now == "traffic"
    assert lanes[1].attention_count == 1


def test_controlfreq_operational_awareness_uses_source_lanes() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    spec = Path("docs/internal/controlfreq_operational_awareness_center_spec.md").read_text(encoding="utf-8")

    assert "self.source_lanes_table = QTableWidget(0, 4)" in source
    assert 'self.source_lanes_table.setHorizontalHeaderLabels(["Source", "Now", "Next", "Attention"])' in source
    assert "self.source_lanes_table.itemSelectionChanged.connect(self._set_source_lane_focus_from_selection)" in source
    assert "build_radio_source_lanes(" in source
    assert 'self.traffic_source_combo.addItem("Traffic Source: All", "")' in source
    assert "source_families=self._traffic_source_query_families(" in source
    assert "def _traffic_source_query_families" in source
    assert 'self._source_family_filter = ""' in source
    assert "def _set_source_lane_focus_from_selection" in source
    assert "Focused source:" in source
    assert "Sources: {', '.join(sources[:4])}" in source
    assert "Traffic Source Filtering And Telemetry Boundary" in spec
    assert "ControlFreq must not collapse multi-source operations into a single" in spec


def test_controlfreq_sparse_views_size_around_rows_and_collapse_details():
    controlfreq_source = Path("freqinout/gui/controlfreq_tab.py").read_text()

    assert "_sync_propagation_box_height" in controlfreq_source
    assert 'self.intersection_box = QGroupBox("Peer Schedule Finder")' in controlfreq_source
    assert "self.intersection_window_combo = QComboBox()" in controlfreq_source
    assert 'self.intersection_label = QLabel("Overlap Window")' in controlfreq_source
    assert 'self.peer_chart_table.setHorizontalHeaderLabels(["Operator", "Rendezvous", "Actions"])' in controlfreq_source
    assert "self.peer_callsign_filter = QLineEdit()" in controlfreq_source
    assert 'self.peer_group_filter.addItem("All groups", "")' in controlfreq_source
    assert 'self.peer_region_filter.addItem("All regions", "")' in controlfreq_source
    assert 'self.peer_role_filter.addItem("All roles", "")' in controlfreq_source
    assert 'menu.addAction("Message"' in controlfreq_source
    assert 'menu.addAction("Show on Map"' in controlfreq_source
    assert 'menu.addAction("Pin in Operational Awareness"' in controlfreq_source
    assert 'self.intersection_window_combo.addItem("30m", 30)' in controlfreq_source
    assert 'self.intersection_window_combo.addItem("6h", 360)' in controlfreq_source
    assert "self.intersection_window_combo.currentIndexChanged.connect(self._refresh_intersections)" in controlfreq_source
    assert 'intersection_combo = getattr(self, "intersection_window_combo", self.activity_window_combo)' in controlfreq_source
    assert "horizon_minutes = int(intersection_combo.currentData() or 120)" in controlfreq_source
    assert "_content_fit_group_height(self.intersection_box, floor=96)" in controlfreq_source
    assert "_content_fit_group_height(self.schedule_box, floor=120)" in controlfreq_source
    assert "group_box.setMinimumHeight(height)" in controlfreq_source
    assert "group_box.updateGeometry()" in controlfreq_source
    assert "self._fit_table_height_to_rows(table, min_rows=1, max_rows=6, empty_rows=1)" in controlfreq_source
    assert "self._fit_table_height_to_rows(self.schedule_table, min_rows=0, max_rows=8, empty_rows=1)" in controlfreq_source
    assert "self._fit_table_height_to_rows(self.prop_table, min_rows=0, max_rows=6, empty_rows=0)" in controlfreq_source
    assert "box.setMaximumHeight(min(height, 460 if details_visible else 230))" in controlfreq_source
    assert "def _set_schedule_splitter_content_sizes" in controlfreq_source
    assert "self._set_schedule_splitter_content_sizes()" in controlfreq_source


def test_controlfreq_dashboard_uses_distinct_visual_grammars_and_details_disclosures() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert "self.source_lane_cards_container = QWidget()" in source
    assert "def _render_source_lane_cards" in source
    assert "self.peer_chart_table = QTableWidget(0, 3)" in source
    assert "self.peer_rendezvous_delegate = PeerRendezvousDelegate" in source
    assert "self.schedule_timeline_container = QWidget()" in source
    assert "def _render_schedule_timeline" in source
    assert "self.prop_band_ladder_container = QWidget()" in source
    assert "def _render_prop_band_ladder" in source
    assert "self.awareness_table.setVisible(False)" in source
    assert "self.activity_table.setVisible(False)" in source
    assert "def _set_awareness_details_visible" in source
    assert "self.peer_finder_table" not in source
    assert "self.intersection_table" not in source
    assert "self.schedule_table.setVisible(False)" in source


def test_peer_finder_consolidates_multiple_bands_into_one_operator_row() -> None:
    tab = SimpleNamespace(
        _show_local=False,
        _load_my_schedule_entries=lambda: [
            {"freq": 14.115, "group": "MAGNET", "band": "20M", "segments": ()},
            {"freq": 7.115, "group": "MAGNET", "band": "40M", "segments": ()},
        ],
        _load_operator_peer_meta=lambda: {
            "K1ABC": {"groups": {"MAGNET", "MR08"}, "role": "HUB", "region": "R8"}
        },
        _load_operator_group_map=lambda: {"K1ABC": {"MAGNET", "MR08"}},
        _peer_schedule_rows=lambda: [
            {"owner_callsign": "K1ABC", "day_utc": "ALL", "start_utc": "00:00", "end_utc": "23:59", "frequency": 14.115},
            {"owner_callsign": "K1ABC", "day_utc": "ALL", "start_utc": "00:00", "end_utc": "23:59", "frequency": 7.115},
        ],
        _parse_time_minutes=lambda value: 0 if value == "00:00" else 1439,
        _parse_frequency_mhz=lambda value: float(value),
        _expand_week_segments=lambda *_args: ((0, 1),),
        _next_horizon_overlaps=lambda *_args, now_week_min, horizon_minutes: ((now_week_min, now_week_min + 30),),
        _format_peer_overlap_when=lambda *_args, **_kwargs: "Now",
        _format_group_band_freq_label=lambda entry: f"{entry['group']} {entry['band']} {entry['freq']:.3f} MHz",
    )

    rows = ControlFreqTab._compute_peer_finder_rows(tab, "", "", horizon_minutes=120)

    assert len(rows) == 1
    assert rows[0]["peer"] == "K1ABC"
    assert rows[0]["role"] == "HUB"
    assert rows[0]["region"] == "R8"
    assert [window["net_band"] for window in rows[0]["windows"]] == [
        "MAGNET 20M 14.115 MHz",
        "MAGNET 40M 7.115 MHz",
    ]


def test_peer_finder_filters_large_roster_before_rendering() -> None:
    peer_rows = [
        {"owner_callsign": f"K{i:03d}AA", "day_utc": "ALL", "start_utc": "00:00", "end_utc": "23:59", "frequency": 7.115}
        for i in range(150)
    ]
    meta = {
        f"K{i:03d}AA": {
            "groups": {"MAGNET", "MR08" if i % 2 == 0 else "MR09"},
            "role": "HUB" if i % 3 == 0 else "PEER",
            "region": "R8" if i < 75 else "R9",
        }
        for i in range(150)
    }
    tab = SimpleNamespace(
        _show_local=False,
        _load_my_schedule_entries=lambda: [
            {"freq": 7.115, "group": "MAGNET", "band": "40M", "segments": ()}
        ],
        _load_operator_peer_meta=lambda: meta,
        _load_operator_group_map=lambda: {callsign: set(row["groups"]) for callsign, row in meta.items()},
        _peer_schedule_rows=lambda: peer_rows,
        _parse_time_minutes=lambda value: 0 if value == "00:00" else 1439,
        _parse_frequency_mhz=lambda value: float(value),
        _expand_week_segments=lambda *_args: ((0, 1),),
        _next_horizon_overlaps=lambda *_args, now_week_min, horizon_minutes: ((now_week_min, now_week_min + 30),),
        _format_peer_overlap_when=lambda *_args, **_kwargs: "Now",
        _format_group_band_freq_label=lambda entry: f"{entry['group']} {entry['band']} {entry['freq']:.3f} MHz",
    )

    rows = ControlFreqTab._compute_peer_finder_rows(
        tab,
        "",
        "",
        horizon_minutes=120,
        peer_group="MR08",
        peer_region="R8",
        peer_role="HUB",
        operator_meta=meta,
    )

    assert rows
    assert len(rows) < 150
    assert all("MR08" in row["groups"] and row["region"] == "R8" and row["role"] == "HUB" for row in rows)


def test_peer_chart_keeps_large_roster_in_bounded_stable_viewport(monkeypatch, tmp_path) -> None:
    from PySide6.QtWidgets import QAbstractItemView

    _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)
    tab = ControlFreqTab()
    rows = [
        {
            "peer": f"K{i:03d}AA",
            "groups": ("MAGNET", "MR08"),
            "role": "HUB" if i % 3 == 0 else "PEER",
            "region": "R8",
            "windows": (
                {
                    "when": "Now" if i % 2 == 0 else "17:00",
                    "net_band": "MAGNET 40M 7.115 MHz",
                    "start_offset_minutes": 0 if i % 2 == 0 else 30,
                    "end_offset_minutes": 60,
                },
            ),
            "context": {"callsign": f"K{i:03d}AA"},
        }
        for i in range(150)
    ]
    try:
        tab._refresh_peer_finder_rows(rows, horizon_minutes=120)
        first_height = tab.peer_chart_table.maximumHeight()
        tab._refresh_peer_finder_rows(tuple(reversed(rows)), horizon_minutes=120)

        assert tab.peer_chart_table.rowCount() == 150
        assert len(tab._peer_finder_contexts) == 150
        assert tab.peer_chart_table.maximumHeight() == first_height
        assert first_height < 400
        assert tab.peer_chart_table.verticalScrollMode() == QAbstractItemView.ScrollPerItem
        assert tab.peer_chart_table.cellWidget(0, 2) is None
        assert tab.peer_chart_table.item(0, 0).text().startswith("K149AA")
    finally:
        tab.deleteLater()


def test_controlfreq_focus_search_is_explicit_bounded_and_accessible() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert "self._focus_autocomplete_timer.setInterval(125)" in source
    assert "self._focus_completer.setMaxVisibleItems(14)" in source
    assert 'heading = QStandardItem(f"{kind.title()} suggestions")' in source
    assert "Qt.AccessibleDescriptionRole" in source
    assert "OrderedDict" in source
    assert "limit=64" in source
    assert "limit=32" in source
    assert '"controlfreq.focus_autocomplete"' in source
    assert '"controlfreq.focus_snapshot_build"' in source
    assert '"controlfreq.focus_stale_result_drop"' in source
    assert "self.focus_more_btn.setVisible(compact)" in source


def test_controlfreq_and_shared_splitters_use_visible_handles() -> None:
    controlfreq_source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    theme_source = Path("freqinout/gui/theme.py").read_text(encoding="utf-8")
    message_source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    map_source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    ncs_source = Path("freqinout/gui/fldigi_net_control_tab.py").read_text(encoding="utf-8")
    layout_spec = Path("docs/internal/ui_layout_standards.md").read_text(encoding="utf-8")

    assert "def style_splitter_handles" in theme_source
    assert "Drag this divider to resize the panels." not in theme_source
    assert "QSplitter::handle:hover" in theme_source
    assert "style_splitter_handles(self.top_splitter" in controlfreq_source
    assert "style_splitter_handles(self.left_splitter" in controlfreq_source
    assert "style_splitter_handles(self.right_splitter" in controlfreq_source
    assert "style_splitter_handles(splitter, resolve_theme(self.settings))" in message_source
    assert "style_splitter_handles(body_splitter, resolve_theme(self.settings))" in message_source
    assert "resolve_theme(self._dark)" not in message_source
    assert "style_splitter_handles(self._map_canvas_splitter" in map_source
    assert "resolve_theme(self._dark)" not in map_source
    assert "style_splitter_handles(self.roster_compare_splitter, resolve_theme(self.settings))" in ncs_source
    assert "resolve_theme(self._dark)" not in ncs_source
    assert "persistent tooltips that can cover operational content" in layout_spec
