from __future__ import annotations

import importlib
import datetime
import json
import os
import sqlite3
import sys
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sop_manager import SOPManager


def test_db_initializer_and_schema_include_schedule_target_columns(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.core.db_initializer as db_initializer

    db_initializer = importlib.reload(db_initializer)
    db_initializer.ensure_all_tables()

    nets_db = cfg_root / "config" / "freqinout_nets.db"
    conn = sqlite3.connect(nets_db)
    try:
        daily_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(daily_schedule_tab)").fetchall()}
        net_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(net_schedule_tab)").fetchall()}
        legacy_net_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(net_schedule)").fetchall()}
    finally:
        conn.close()

    for cols in (daily_cols, net_cols, legacy_net_cols):
        assert "target_scope" in cols
        assert "target_device_profile_id" in cols
        assert "target_operating_profile_id" in cols

    tools_dir = Path(__file__).resolve().parents[1] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    import db_schema

    db_schema = importlib.reload(db_schema)
    assert "target_scope" in db_schema.SETTINGS_TABLES["daily_schedule_tab"].ddl
    assert "target_scope" in db_schema.NETS_TABLES["net_schedule_tab"].ddl
    assert "target_scope" in db_schema.NETS_TABLES["net_schedule"].ddl


def test_scheduler_filters_schedule_rows_by_primary_target_scope(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    store.save_device_profile(
        {
            "name": "Primary Rig",
            "control_backend": "flrig",
            "runtime_active": 1,
            "runtime_primary": 1,
        }
    )
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None
    primary_id = int(primary["id"])
    default_assignment = store.get_effective_assignment_for_device(primary_id)
    assert default_assignment is not None
    default_operating_id = int(default_assignment["operating_profile_id"])

    field_ops = store.save_operating_profile(
        {
            "name": "Field Ops",
            "scheduler_enabled": True,
            "use_map": True,
            "use_messages": True,
            "use_background_ingest": True,
            "use_launch_control": True,
            "use_net_control_tabs": True,
        }
    )
    alternate = store.save_device_profile(
        {
            "name": "Field Rig",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.1",
            "flrig_port": 12346,
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7363,
            "js8_host": "127.0.0.1",
            "js8_port": 2443,
        }
    )
    alternate_id = int(alternate["id"])
    store.set_device_profile_runtime_active(alternate_id, True)
    store.set_device_operating_profile(alternate_id, int(field_ops["id"]), assignment_state="active", reason="Field role")

    hf_rows = [
        {"group_name": "STATION", "day_utc": "ALL", "band": "40M", "mode": "Digi", "frequency": "7.078", "start_utc": "00:00", "end_utc": "23:59", "target_scope": "station"},
        {"group_name": "PRIMARY", "day_utc": "ALL", "band": "20M", "mode": "Digi", "frequency": "14.078", "start_utc": "00:00", "end_utc": "23:59", "target_scope": "device_profile", "target_device_profile_id": primary_id},
        {"group_name": "ALT", "day_utc": "ALL", "band": "80M", "mode": "Digi", "frequency": "3.578", "start_utc": "00:00", "end_utc": "23:59", "target_scope": "device_profile", "target_device_profile_id": alternate_id},
        {"group_name": "DEFAULT_OP", "day_utc": "ALL", "band": "17M", "mode": "Digi", "frequency": "18.104", "start_utc": "00:00", "end_utc": "23:59", "target_scope": "operating_profile", "target_operating_profile_id": default_operating_id},
        {"group_name": "FIELD_OP", "day_utc": "ALL", "band": "15M", "mode": "Digi", "frequency": "21.078", "start_utc": "00:00", "end_utc": "23:59", "target_scope": "operating_profile", "target_operating_profile_id": int(field_ops["id"])},
    ]
    net_rows = [
        {"net_name": "Station Net", "group_name": "STATION", "day_utc": "ALL", "recurrence": "Daily", "band": "40M", "mode": "Digi", "frequency": "7.110", "start_utc": "01:00", "end_utc": "02:00", "early_checkin": "0", "target_scope": "station"},
        {"net_name": "Primary Net", "group_name": "PRIMARY", "day_utc": "ALL", "recurrence": "Daily", "band": "20M", "mode": "Digi", "frequency": "14.110", "start_utc": "01:00", "end_utc": "02:00", "early_checkin": "0", "target_scope": "device_profile", "target_device_profile_id": primary_id},
        {"net_name": "Alt Net", "group_name": "ALT", "day_utc": "ALL", "recurrence": "Daily", "band": "80M", "mode": "Digi", "frequency": "3.590", "start_utc": "01:00", "end_utc": "02:00", "early_checkin": "0", "target_scope": "device_profile", "target_device_profile_id": alternate_id},
        {"net_name": "Default Op Net", "group_name": "DEFAULT_OP", "day_utc": "ALL", "recurrence": "Daily", "band": "17M", "mode": "Digi", "frequency": "18.120", "start_utc": "01:00", "end_utc": "02:00", "early_checkin": "0", "target_scope": "operating_profile", "target_operating_profile_id": default_operating_id},
        {"net_name": "Field Op Net", "group_name": "FIELD_OP", "day_utc": "ALL", "recurrence": "Daily", "band": "15M", "mode": "Digi", "frequency": "21.120", "start_utc": "01:00", "end_utc": "02:00", "early_checkin": "0", "target_scope": "operating_profile", "target_operating_profile_id": int(field_ops["id"])},
    ]

    engine = SchedulerEngine()
    monkeypatch.setattr(engine, "_load_daily_schedule_from_db", lambda: list(hf_rows))
    monkeypatch.setattr(engine, "_load_net_schedule_from_db", lambda: list(net_rows))
    monkeypatch.setattr(engine, "_load_sop_schedule_layer_from_db", lambda: [])
    monkeypatch.setattr(engine, "_load_sop_net_conflict_policies_from_db", lambda: [])

    hf_filtered, net_filtered, _sop, _policies = engine._load_schedules(force=True)
    assert {row["group_name"] for row in hf_filtered} == {"STATION", "PRIMARY", "DEFAULT_OP"}
    assert {row["group_name"] for row in net_filtered} == {"STATION", "PRIMARY", "DEFAULT_OP"}

    store.set_runtime_primary_device_profile(alternate_id)
    hf_filtered, net_filtered, _sop, _policies = engine._load_schedules(force=True)
    assert {row["group_name"] for row in hf_filtered} == {"STATION", "ALT", "FIELD_OP"}
    assert {row["group_name"] for row in net_filtered} == {"STATION", "ALT", "FIELD_OP"}


def test_scheduler_prefers_assigned_frequency_plan_for_primary_radio(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio_a = store.save_device_profile(
        {
            "name": "FIO-A",
            "control_backend": "flrig",
            "runtime_active": 1,
            "runtime_primary": 1,
        }
    )
    radio_b = store.save_device_profile(
        {
            "name": "FIO-B",
            "control_backend": "flrig",
        }
    )
    store.set_device_profile_runtime_active(int(radio_a["id"]), True)
    store.set_device_profile_runtime_active(int(radio_b["id"]), True)
    store.set_runtime_primary_device_profile(int(radio_a["id"]))
    amrron_plan = store.save_frequency_plan(
        {
            "name": "AmRRON Plan",
            "category": "normal",
            "status": "saved",
            "schedule_refs_json": json.dumps(
                [
                    {
                        "source": "HF",
                        "source_table": "daily_schedule_tab",
                        "day_utc": "ALL",
                        "group_name": "AMRRON",
                        "band": "40M",
                        "mode": "Digi",
                        "frequency": "7.110",
                        "start_utc": "00:00",
                        "end_utc": "23:59",
                    },
                    {
                        "source": "NET",
                        "source_table": "net_schedule_tab",
                        "day_utc": "Monday",
                        "group_name": "AMRRON",
                        "net_name": "AmRRON Net",
                        "band": "20M",
                        "mode": "Digi",
                        "frequency": "14.110",
                        "start_utc": "07:00",
                        "end_utc": "08:00",
                    },
                ]
            ),
        }
    )
    store.set_assigned_plan(int(radio_b["id"]), int(amrron_plan["id"]))
    store.set_runtime_primary_device_profile(int(radio_b["id"]))

    engine = SchedulerEngine()
    try:
        monkeypatch.setattr(
            engine,
            "_load_daily_schedule_from_db",
            lambda: [
                {
                    "day_utc": "ALL",
                    "group_name": "MAGNET",
                    "band": "40M",
                    "mode": "Digi",
                    "frequency": "7.115",
                    "start_utc": "00:00",
                    "end_utc": "23:59",
                    "target_scope": "station",
                }
            ],
        )
        monkeypatch.setattr(engine, "_load_net_schedule_from_db", lambda: [])
        monkeypatch.setattr(engine, "_load_sop_schedule_layer_from_db", lambda: [])
        monkeypatch.setattr(engine, "_load_sop_net_conflict_policies_from_db", lambda: [])

        hf_filtered, net_filtered, _sop, _policies = engine._load_schedules(force=True)

        assert {row["group_name"] for row in hf_filtered} == {"AMRRON"}
        assert {row["frequency"] for row in hf_filtered} == {"7.110"}
        assert {row["net_name"] for row in net_filtered} == {"AmRRON Net"}
        assert all(int(row["target_device_profile_id"]) == int(radio_b["id"]) for row in hf_filtered + net_filtered)
    finally:
        engine.stop()


def test_scheduler_projects_assigned_schedule_lanes_for_all_active_radios(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio_a = store.save_device_profile(
        {
            "name": "FIO-A",
            "control_backend": "flrig",
            "runtime_active": 1,
            "runtime_primary": 1,
        }
    )
    radio_b = store.save_device_profile(
        {
            "name": "FIO-B",
            "control_backend": "flrig",
        }
    )
    store.set_device_profile_runtime_active(int(radio_a["id"]), True)
    store.set_device_profile_runtime_active(int(radio_b["id"]), True)
    store.set_runtime_primary_device_profile(int(radio_a["id"]))
    magnet_plan = store.save_frequency_plan(
        {
            "name": "Magnet Main Plan",
            "category": "normal",
            "status": "saved",
            "schedule_refs_json": json.dumps(
                [
                    {
                        "source": "HF",
                        "source_table": "daily_schedule_tab",
                        "day_utc": "ALL",
                        "group_name": "MAGNET",
                        "band": "40M",
                        "mode": "Digi",
                        "frequency": "7.115",
                        "start_utc": "00:00",
                        "end_utc": "23:59",
                    }
                ]
            ),
        }
    )
    amrron_plan = store.save_frequency_plan(
        {
            "name": "AmRRON Plan",
            "category": "normal",
            "status": "saved",
            "schedule_refs_json": json.dumps(
                [
                    {
                        "source": "HF",
                        "source_table": "daily_schedule_tab",
                        "day_utc": "ALL",
                        "group_name": "AMRRON",
                        "band": "20M",
                        "mode": "Digi",
                        "frequency": "14.110",
                        "start_utc": "00:00",
                        "end_utc": "23:59",
                    }
                ]
            ),
        }
    )
    store.set_assigned_plan(int(radio_a["id"]), int(magnet_plan["id"]))
    store.set_assigned_plan(int(radio_b["id"]), int(amrron_plan["id"]))

    engine = SchedulerEngine()
    try:
        monkeypatch.setattr(
            engine,
            "_load_daily_schedule_from_db",
            lambda: [
                {
                    "day_utc": "ALL",
                    "group_name": "STATION-FALLBACK",
                    "band": "80M",
                    "mode": "Digi",
                    "frequency": "3.585",
                    "start_utc": "00:00",
                    "end_utc": "23:59",
                    "target_scope": "station",
                }
            ],
        )
        monkeypatch.setattr(engine, "_load_net_schedule_from_db", lambda: [])
        monkeypatch.setattr(engine, "_load_sop_schedule_layer_from_db", lambda: [])
        monkeypatch.setattr(engine, "_load_sop_net_conflict_policies_from_db", lambda: [])

        lanes = engine.active_schedule_lanes(
            force=True,
            now_utc=datetime.datetime(2026, 8, 10, 12, 0, tzinfo=datetime.timezone.utc),
        )

        by_name = {str(lane["device_name"]): lane for lane in lanes}
        assert set(by_name) == {"FIO-A", "FIO-B"}
        assert store.get_runtime_primary_device_profile()["name"] == "FIO-A"
        assert by_name["FIO-A"]["frequency_plan_name"] == "Magnet Main Plan"
        assert by_name["FIO-B"]["frequency_plan_name"] == "AmRRON Plan"
        assert by_name["FIO-A"]["current_entry"]["group_name"] == "MAGNET"
        assert by_name["FIO-B"]["current_entry"]["group_name"] == "AMRRON"
        assert by_name["FIO-B"]["current_entry"]["band"] == "20M"
        assert all(lane["current_source"] == "HF" for lane in lanes)
        assert engine.get_status_poll_metrics()["polls_started"] == 0
    finally:
        engine.stop()


def test_scheduler_active_schedule_lanes_filter_sop_rows_by_radio(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio_a = store.save_device_profile({"name": "FIO-A", "control_backend": "flrig"})
    radio_b = store.save_device_profile({"name": "FIO-B", "control_backend": "flrig"})
    store.set_device_profile_runtime_active(int(radio_a["id"]), True)
    store.set_device_profile_runtime_active(int(radio_b["id"]), True)
    store.set_runtime_primary_device_profile(int(radio_a["id"]))
    magnet_plan = store.save_frequency_plan(
        {
            "name": "Magnet Main Plan",
            "category": "normal",
            "status": "saved",
            "schedule_refs_json": json.dumps(
                [
                    {
                        "source": "HF",
                        "source_table": "daily_schedule_tab",
                        "day_utc": "ALL",
                        "group_name": "MAGNET",
                        "band": "40M",
                        "mode": "Digi",
                        "frequency": "7.115",
                        "start_utc": "00:00",
                        "end_utc": "23:59",
                    }
                ]
            ),
        }
    )
    amrron_plan = store.save_frequency_plan(
        {
            "name": "AmRRON Plan",
            "category": "normal",
            "status": "saved",
            "schedule_refs_json": json.dumps(
                [
                    {
                        "source": "HF",
                        "source_table": "daily_schedule_tab",
                        "day_utc": "ALL",
                        "group_name": "AMRRON",
                        "band": "20M",
                        "mode": "Digi",
                        "frequency": "14.110",
                        "start_utc": "00:00",
                        "end_utc": "23:59",
                    }
                ]
            ),
        }
    )
    store.set_assigned_plan(int(radio_a["id"]), int(magnet_plan["id"]))
    store.set_assigned_plan(int(radio_b["id"]), int(amrron_plan["id"]))

    engine = SchedulerEngine()
    try:
        monkeypatch.setattr(engine, "_load_daily_schedule_from_db", lambda: [])
        monkeypatch.setattr(engine, "_load_net_schedule_from_db", lambda: [])
        monkeypatch.setattr(
            engine,
            "_load_sop_schedule_layer_from_db",
            lambda: [
                {
                    "day_utc": "ALL",
                    "recurrence": "Daily",
                    "group_name": "MAGNET-SOP",
                    "band": "80M",
                    "mode": "Digi",
                    "frequency": "3.585",
                    "start_utc": "00:00",
                    "end_utc": "23:59",
                    "target_scope": "device_profile",
                    "target_device_profile_id": int(radio_a["id"]),
                    "sop_priority": 1,
                }
            ],
        )
        monkeypatch.setattr(engine, "_load_sop_net_conflict_policies_from_db", lambda: [])

        lanes = engine.active_schedule_lanes(
            force=True,
            now_utc=datetime.datetime(2026, 8, 10, 12, 0, tzinfo=datetime.timezone.utc),
        )

        by_name = {str(lane["device_name"]): lane for lane in lanes}
        assert by_name["FIO-A"]["current_source"] == "SOP"
        assert by_name["FIO-A"]["current_entry"]["group_name"] == "MAGNET-SOP"
        assert by_name["FIO-B"]["current_source"] == "HF"
        assert by_name["FIO-B"]["current_entry"]["group_name"] == "AMRRON"
        assert by_name["FIO-B"]["sop_rows"] == []
        assert engine.get_status_poll_metrics()["polls_started"] == 0
    finally:
        engine.stop()


def test_scheduler_active_schedule_lanes_cache_rows_without_extra_status_polling(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "FIO-A",
            "control_backend": "flrig",
        }
    )
    store.set_device_profile_runtime_active(int(radio["id"]), True)
    store.set_runtime_primary_device_profile(int(radio["id"]))
    plan = store.save_frequency_plan(
        {
            "name": "Magnet Main Plan",
            "category": "normal",
            "status": "saved",
            "schedule_refs_json": json.dumps(
                [
                    {
                        "source": "HF",
                        "source_table": "daily_schedule_tab",
                        "day_utc": "ALL",
                        "group_name": "MAGNET",
                        "band": "40M",
                        "mode": "Digi",
                        "frequency": "7.115",
                        "start_utc": "00:00",
                        "end_utc": "23:59",
                    }
                ]
            ),
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    engine = SchedulerEngine()
    load_count = {"daily": 0}

    def load_daily():
        load_count["daily"] += 1
        return []

    try:
        monkeypatch.setattr(engine, "_load_daily_schedule_from_db", load_daily)
        monkeypatch.setattr(engine, "_load_net_schedule_from_db", lambda: [])
        monkeypatch.setattr(engine, "_load_sop_schedule_layer_from_db", lambda: [])
        monkeypatch.setattr(engine, "_load_sop_net_conflict_policies_from_db", lambda: [])

        now = datetime.datetime(2026, 8, 10, 12, 0, tzinfo=datetime.timezone.utc)
        first = engine.active_schedule_lanes(force=True, now_utc=now)
        second = engine.active_schedule_lanes(now_utc=now + datetime.timedelta(minutes=1))

        assert first[0]["current_entry"]["group_name"] == "MAGNET"
        assert second[0]["current_entry"]["group_name"] == "MAGNET"
        assert load_count["daily"] == 0
        assert engine.get_status_poll_metrics()["polls_started"] == 0
    finally:
        engine.stop()


def test_daily_schedule_db_roundtrip_preserves_target_scope(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.gui.daily_schedule_tab as daily_mod

    daily_mod = importlib.reload(daily_mod)
    SettingsManager()
    dummy = daily_mod.DailyScheduleTab.__new__(daily_mod.DailyScheduleTab)
    dummy.settings = SettingsManager()

    rows = [
        {
            "day_utc": "Monday",
            "group_name": "OPS-A",
            "mode": "Digi",
            "band": "40M",
            "frequency": "7.078",
            "start_utc": "01:00",
            "end_utc": "02:00",
            "auto_tune": False,
            "target_scope": "device_profile",
            "target_device_profile_id": 42,
        }
    ]

    daily_mod.DailyScheduleTab._save_schedule_to_db(dummy, rows)
    loaded = daily_mod.DailyScheduleTab._load_schedule_from_db(dummy)

    assert len(loaded) == 1
    assert loaded[0]["target_scope"] == "device_profile"
    assert int(loaded[0]["target_device_profile_id"]) == 42
    assert loaded[0]["target_operating_profile_id"] is None


def test_net_schedule_tab_collects_target_scope_from_row_widgets(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    operating = store.save_operating_profile({"name": "Field Ops"})

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_bootstrap_net_resources", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_load_resources_from_db", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resource_set_combo", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resources_table", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_schedule_net_sop_conflict_refresh", lambda self, **kwargs: None)

    tab = net_mod.NetScheduleTab()
    try:
        tab.table.setRowCount(0)
        tab._add_row(
            {
                "day_utc": "Monday",
                "recurrence": "Weekly",
                "group_name": "OPS-A",
                "mode": "Digi",
                "band": "40M",
                "frequency": "7.110",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "early_checkin": "5",
                "net_name": "Night Net",
                "target_scope": "operating_profile",
                "target_operating_profile_id": int(operating["id"]),
            }
        )
        rows = tab._collect_rows()
        assert len(rows) == 1
        assert rows[0]["target_scope"] == "operating_profile"
        assert int(rows[0]["target_operating_profile_id"]) == int(operating["id"])
        assert rows[0]["target_device_profile_id"] is None
    finally:
        tab.deleteLater()
        app.processEvents()


def test_net_schedule_save_auto_creates_manual_resource_for_new_manual_net(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    SettingsManager()
    dummy = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    dummy.settings = SettingsManager()

    rows = [
        {
            "day_utc": "Monday",
            "recurrence": "Weekly",
            "group_name": "OPS-A",
            "mode": "Digi",
            "band": "40M",
            "frequency": "7.110",
            "start_utc": "01:00",
            "end_utc": "02:00",
            "early_checkin": "5",
            "net_name": "Night Net",
            "fldigi_mode": "USB",
            "fldigi_offset": "1500",
            "target_scope": "station",
        }
    ]

    dummy._save_to_db(rows)

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        resources = conn.execute(
            """
            SELECT id, resource_set, source_type, source_ref, day_utc, net_name, frequency
              FROM net_resources
            """
        ).fetchall()
        schedule = conn.execute("SELECT resource_id FROM net_schedule_tab").fetchall()
    finally:
        conn.close()

    assert len(resources) == 1
    resource_id, resource_set, source_type, source_ref, day, net_name, frequency = resources[0]
    assert resource_set == "Custom"
    assert source_type == "manual"
    assert source_ref == "auto_from_schedule"
    assert day == "Monday"
    assert net_name == "Night Net"
    assert frequency == "7.110"
    assert schedule == [(resource_id,)]


def test_net_schedule_save_links_existing_resource_without_duplicate(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    SettingsManager()
    dummy = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    dummy.settings = SettingsManager()

    rows = [
        {
            "day_utc": "Tuesday",
            "recurrence": "Weekly",
            "group_name": "OPS-B",
            "mode": "Digi",
            "band": "80M",
            "frequency": "3.590",
            "start_utc": "03:00",
            "end_utc": "04:00",
            "early_checkin": "0",
            "net_name": "Early Net",
            "fldigi_mode": "",
            "fldigi_offset": "",
            "target_scope": "station",
        }
    ]

    dummy._save_to_db(rows)
    dummy._save_to_db(rows)

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        resource_count = conn.execute("SELECT COUNT(*) FROM net_resources").fetchone()[0]
        resource_id = conn.execute("SELECT id FROM net_resources").fetchone()[0]
        schedule = conn.execute("SELECT resource_id FROM net_schedule_tab").fetchall()
    finally:
        conn.close()

    assert resource_count == 1
    assert schedule == [(resource_id,)]


def test_net_schedule_save_unlinks_edited_resource_row_without_updating_master(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    SettingsManager()
    dummy = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    dummy.settings = SettingsManager()

    original = {
        "day_utc": "Wednesday",
        "recurrence": "Weekly",
        "group_name": "OPS-C",
        "mode": "Digi",
        "band": "40M",
        "frequency": "7.115",
        "start_utc": "05:00",
        "end_utc": "06:00",
        "early_checkin": "0",
        "net_name": "Resource Net",
        "fldigi_mode": "",
        "fldigi_offset": "",
        "target_scope": "station",
    }

    dummy._save_to_db([original])

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        resource_id = conn.execute("SELECT id FROM net_resources").fetchone()[0]
    finally:
        conn.close()

    edited = dict(original)
    edited["_resource_id"] = resource_id
    edited["_resource_set"] = "Custom"
    edited["net_name"] = "Local Override Net"
    edited["start_utc"] = "05:30"

    dummy._save_to_db([edited])

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        resource_count = conn.execute("SELECT COUNT(*) FROM net_resources").fetchone()[0]
        master = conn.execute("SELECT net_name, start_utc FROM net_resources WHERE id = ?", (resource_id,)).fetchone()
        schedule = conn.execute("SELECT net_name, start_utc, resource_id FROM net_schedule_tab").fetchall()
    finally:
        conn.close()

    assert resource_count == 1
    assert master == ("Resource Net", "05:00")
    assert schedule == [("Local Override Net", "05:30", None)]


def test_net_row_signature_includes_target_scope_metadata() -> None:
    base_row = {
        "day_utc": "Monday",
        "recurrence": "Weekly",
        "group_name": "OPS-A",
        "band": "40M",
        "mode": "Digi",
        "frequency": "7.110",
        "start_utc": "01:00",
        "end_utc": "02:00",
        "net_name": "Night Net",
        "target_scope": "station",
    }
    station_sig = SOPManager._net_row_signature(dict(base_row))
    device_sig = SOPManager._net_row_signature(
        {
            **base_row,
            "target_scope": "device_profile",
            "target_device_profile_id": 7,
        }
    )

    assert station_sig != device_sig

    parsed = SOPManager._parse_net_row_signature(device_sig)
    assert parsed["target_scope"] == "device_profile"
    assert parsed["target_device_profile_id"] == "7"
