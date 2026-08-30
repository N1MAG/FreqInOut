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

from freqinout.gui.controlfreq_tab import ControlFreqTab
from freqinout.core.controlfreq_awareness import AttentionItem, build_radio_source_lanes
from freqinout.core.observation_projection import Observation, observation_from_rf_pin
from freqinout.core.observation_store import upsert_observation


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

    assert "Operational Activity: 1 high-value" in headline
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

    assert "Operational Activity: 1 high-value" in headline
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
    assert lanes[1].now == "monitoring"
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
    assert "build_radio_source_lanes(" in source
    assert "Sources: {', '.join(sources[:4])}" in source
    assert "ControlFreq must not collapse multi-source operations into a single" in spec


def test_controlfreq_sparse_views_size_around_rows_and_collapse_details():
    controlfreq_source = Path("freqinout/gui/controlfreq_tab.py").read_text()

    assert "_sync_propagation_box_height" in controlfreq_source
    assert "self.intersection_window_combo = QComboBox()" in controlfreq_source
    assert 'self.intersection_label = QLabel("Intersection Window")' in controlfreq_source
    assert 'self.intersection_window_combo.addItem("30m", 30)' in controlfreq_source
    assert 'self.intersection_window_combo.addItem("6h", 360)' in controlfreq_source
    assert "self.intersection_window_combo.currentIndexChanged.connect(self._refresh_intersections)" in controlfreq_source
    assert 'intersection_combo = getattr(self, "intersection_window_combo", self.activity_window_combo)' in controlfreq_source
    assert "horizon_minutes = int(intersection_combo.currentData() or 120)" in controlfreq_source
    assert "_content_fit_group_height(self.intersection_box, floor=96)" in controlfreq_source
    assert "_content_fit_group_height(self.schedule_box, floor=120)" in controlfreq_source
    assert "self._fit_table_height_to_rows(self.intersection_table, min_rows=0, max_rows=2, empty_rows=1)" in controlfreq_source
    assert "self._fit_table_height_to_rows(self.schedule_table, min_rows=0, max_rows=4, empty_rows=1)" in controlfreq_source
    assert "self._fit_table_height_to_rows(self.prop_table, min_rows=0, max_rows=6, empty_rows=0)" in controlfreq_source
    assert "box.setMaximumHeight(min(height, 420 if details_visible else 150))" in controlfreq_source
    assert "def _set_schedule_splitter_content_sizes" in controlfreq_source
    assert "self._set_schedule_splitter_content_sizes()" in controlfreq_source


def test_controlfreq_and_shared_splitters_use_visible_handles() -> None:
    controlfreq_source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    theme_source = Path("freqinout/gui/theme.py").read_text(encoding="utf-8")
    message_source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    map_source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    ncs_source = Path("freqinout/gui/fldigi_net_control_tab.py").read_text(encoding="utf-8")
    layout_spec = Path("docs/internal/ui_layout_standards.md").read_text(encoding="utf-8")

    assert "def style_splitter_handles" in theme_source
    assert "Drag this divider to resize the panels." in theme_source
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
    assert "Resizable split panels must advertise that they are resizable." in layout_spec
