from __future__ import annotations

import datetime as dt
import importlib
import json
import os
import sqlite3

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QComboBox, QMessageBox
from PySide6.QtCore import Qt

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.schedule_projection import ProjectionCell
from freqinout.core.schedule_projection import build_blended_schedule_projection
from freqinout.core.schedule_source_sets import (
    HF_DAILY_SOURCE_CATEGORY,
    HF_DAILY_SOURCE_SETS_KEY,
    HF_NET_SOURCE_CATEGORY,
    HF_NET_SOURCE_SETS_KEY,
    LIVE_SOURCE_SET_ID,
    SELECTED_HF_DAILY_SOURCE_SET_KEY,
    SELECTED_HF_NET_SOURCE_SET_KEY,
    assigned_plan_rf_guard_impacts_for_source_update,
    delete_source_schedule,
    save_source_schedule,
    save_source_set,
    source_schedule_dependency_ref,
)
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sop_manager import SOPManager


def test_blended_schedule_projection_layers_hf_net_and_sop_with_net_default_priority() -> None:
    projection = build_blended_schedule_projection(
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "04:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
            }
        ],
        [
            {
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "80M",
                "frequency": "3.590",
                "group_name": "OPS",
                "net_name": "Night Net",
            }
        ],
        [
            {
                "day_utc": "Monday",
                "start_utc": "02:30",
                "end_utc": "03:30",
                "band": "20M",
                "frequency": "14.078",
                "group_name": "OPS",
                "profile_name": "HF SOP",
            }
        ],
        [],
        week_start_utc=dt.date(2026, 8, 2),
    )

    refs = projection.schedule_refs()
    compact = [(row["source"], row["start_utc"], row["end_utc"], row["band"]) for row in refs]

    assert compact == [
        ("HF", "01:00", "02:00", "40M"),
        ("NET", "02:00", "03:00", "80M"),
        ("SOP", "03:00", "03:30", "20M"),
        ("HF", "03:30", "04:00", "40M"),
    ]
    assert projection.source_refs() == ["hf_daily", "hf_nets", "sop"]
    assert "80M:3.590" in projection.frequency_refs()
    assert all(str(row.get("source_key") or "").startswith(str(row["source"])) for row in refs)


def test_blended_projection_uses_shared_net_and_sop_policy_signatures() -> None:
    net_row = {
        "day_utc": "Monday",
        "start_utc": "02:00",
        "end_utc": "03:00",
        "band": "80M",
        "frequency": "3.59",
        "group_name": "ops",
        "net_name": "Night Net",
        "target_scope": "operating_profile",
        "target_operating_profile_id": 42,
    }
    sop_row = {
        "sop_profile_id": 7,
        "sop_layer_id": 11,
        "day_utc": "Monday",
        "start_utc": "02:30",
        "end_utc": "03:30",
        "band": "20M",
        "frequency": "14.078",
        "group_name": "ops",
    }

    projection = build_blended_schedule_projection(
        [],
        [net_row],
        [sop_row],
        [],
        week_start_utc=dt.date(2026, 8, 2),
    )

    net_segment = next(segment for segment in projection.cells[26].net_segments if segment.net_name == "Night Net")
    sop_segment = next(segment for segment in projection.cells[26].sop_segments if segment.band == "20M")
    assert net_segment.row_signature == SOPManager._net_row_signature(dict(net_row))
    assert sop_segment.row_signature == SOPManager._sop_row_signature(dict(sop_row))


def test_blended_projection_honors_sop_priority_only_inside_dated_policy_window() -> None:
    net_row = {
        "day_utc": "Monday",
        "start_utc": "02:00",
        "end_utc": "03:00",
        "band": "80M",
        "frequency": "3.590",
        "group_name": "OPS",
        "net_name": "Night Net",
    }
    sop_row = {
        "sop_profile_id": 7,
        "sop_layer_id": 11,
        "day_utc": "Monday",
        "start_utc": "02:30",
        "end_utc": "03:30",
        "band": "20M",
        "frequency": "14.078",
        "group_name": "OPS",
    }
    net_signature = SOPManager._net_row_signature(dict(net_row))
    sop_signature = SOPManager._sop_row_signature(dict(sop_row))

    projection = build_blended_schedule_projection(
        [],
        [net_row],
        [sop_row],
        [
            {
                "policy": "SOP_PRIORITY",
                "start_utc": "2026-08-03T02:30:00+00:00",
                "end_utc": "2026-08-03T03:00:00+00:00",
                "net_row_signature": net_signature,
                "sop_row_signature": sop_signature,
            }
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )
    compact = [(row["source"], row["start_utc"], row["end_utc"], row["band"]) for row in projection.schedule_refs()]
    assert compact == [
        ("NET", "02:00", "02:30", "80M"),
        ("SOP", "02:30", "03:30", "20M"),
    ]

    stale_policy_projection = build_blended_schedule_projection(
        [],
        [net_row],
        [sop_row],
        [
            {
                "policy": "SOP_PRIORITY",
                "start_utc": "2026-08-10T02:30:00+00:00",
                "end_utc": "2026-08-10T03:00:00+00:00",
                "net_row_signature": net_signature,
                "sop_row_signature": sop_signature,
            }
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )
    stale_compact = [
        (row["source"], row["start_utc"], row["end_utc"], row["band"])
        for row in stale_policy_projection.schedule_refs()
    ]
    assert stale_compact == [
        ("NET", "02:00", "03:00", "80M"),
        ("SOP", "03:00", "03:30", "20M"),
    ]


def test_frequency_plan_structured_schedule_refs_round_trip_and_infer_guard_band(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "Twenty Meter Only",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "block",
        }
    )
    plan = store.save_frequency_plan(
        {
            "name": "Frequency Only Forty",
            "schedule_refs": [
                {
                    "source": "NET",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "frequency": "7.078",
                    "net_name": "Forty Meter Net",
                }
            ],
            "frequency_refs": ["7.078"],
        }
    )

    loaded_refs = json.loads(str(plan["schedule_refs_json"]))
    assert loaded_refs[0]["source"] == "NET"
    assert loaded_refs[0]["frequency"] == "7.078"

    with pytest.raises(ValueError, match="antenna support does not include 40M"):
        store.set_assigned_plan(int(radio["id"]), int(plan["id"]))


def test_frequency_only_structured_schedule_refs_drive_band_overlap_guard(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    left = store.save_device_profile(
        {
            "name": "Left",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    right = store.save_device_profile(
        {
            "name": "Right",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 0,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "band_overlap_guard_group": "North Mast",
            "band_overlap_guard_mode": "block",
        }
    )
    left_plan = store.save_frequency_plan(
        {
            "name": "Left Forty",
            "schedule_refs": [{"day_utc": "Monday", "start_utc": "02:00", "end_utc": "03:00", "frequency": "7.078"}],
        }
    )
    right_plan = store.save_frequency_plan(
        {
            "name": "Right Forty",
            "schedule_refs": [{"day_utc": "Monday", "start_utc": "02:30", "end_utc": "03:30", "frequency": "7.110"}],
        }
    )

    store.set_assigned_plan(int(left["id"]), int(left_plan["id"]))
    with pytest.raises(ValueError, match="Prevent Band Overlap group NORTH MAST"):
        store.set_assigned_plan(int(right["id"]), int(right_plan["id"]))


def test_freqplanner_save_plan_persists_blended_projection(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()

    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                }
            ],
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "80M",
                    "frequency": "3.590",
                    "group_name": "OPS",
                    "net_name": "Night Net",
                }
            ],
            [],
            [],
        ),
    )
    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    monkeypatch.setattr(tab, "_prompt_for_name", lambda *args, **kwargs: ("Blended Watch", True))
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)

    tab._on_save_plan_clicked()
    app.processEvents()

    plans = tab.plan_context_service.store.list_frequency_plans()
    saved = next(row for row in plans if row["name"] == "Blended Watch")
    refs = json.loads(str(saved["schedule_refs_json"]))

    assert [(row["source"], row["band"]) for row in refs] == [("HF", "40M"), ("NET", "80M")]
    assert all(row.get("source_key") for row in refs)
    assert json.loads(str(saved["source_refs_json"])) == ["hf_daily", "hf_nets"]
    assert "Saved Frequency Plan 'Blended Watch'" in tab.frequency_plan_action_hint_label.text()
    assert tab.assign_plan_btn.isEnabled()

    tab._on_assign_plan_clicked()
    assert "Settings > Radio Profiles > Schedule Assignment" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_save_plan_requires_confirmation_when_rf_guard_preflight_skipped(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()

    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                }
            ],
            [],
            [],
            [],
        ),
    )
    monkeypatch.setattr(
        tab,
        "_rf_guard_preflight_for_plan",
        lambda _payload: {
            "state": "off",
            "rf_guard_validation": "not_enforced",
            "messages": ["RF Guard preflight skipped because no radio context is selected."],
        },
    )
    monkeypatch.setattr(tab, "_prompt_for_name", lambda *args, **kwargs: ("No Radio Context", True))
    prompts = []

    def _capture_question(_parent, title, text, *args, **kwargs):
        prompts.append((str(title), str(text)))
        return QMessageBox.Save

    monkeypatch.setattr(planner_mod.QMessageBox, "question", _capture_question)

    tab._on_save_plan_clicked()
    app.processEvents()

    assert [title for title, _text in prompts] == ["RF Guard Preflight Skipped"]
    assert "assignment-specific RF Safety Guard checks" in prompts[0][1]
    assert "RF Guard preflight skipped; assignment checks still required." in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_save_sop_schedule_plan_includes_net_resources(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "COUNTY"}, {"group": "AUX"}])

    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [],
            [],
            [
                {
                    "sop_profile_id": 7,
                    "sop_layer_id": 11,
                    "profile_name": "County SOP",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "COUNTY",
                    "action_label": "Call NCS",
                }
            ],
            [],
        ),
    )
    monkeypatch.setattr(
        tab,
        "_load_net_resources_from_db",
        lambda: [
            {
                "id": 314,
                "resource_id": 314,
                "source_key": "NET_RESOURCE:314",
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "AUX",
                "net_name": "Aux Resource Net",
            }
        ],
    )
    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    monkeypatch.setattr(tab, "_prompt_for_name", lambda *args, **kwargs: ("County Operational Day", True))
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)

    tab._on_save_sop_plan_clicked()
    app.processEvents()

    plans = tab.plan_context_service.store.list_frequency_plans()
    saved = next(row for row in plans if row["name"] == "County Operational Day")
    refs = json.loads(str(saved["schedule_refs_json"]))
    assert saved["category"] == "sop_schedule"
    assert {row["source"] for row in refs} == {"SOP", "NET_RESOURCE"}
    assert any(row.get("resource_id") == 314 for row in refs)
    assert json.loads(str(saved["source_refs_json"])) == ["net_resources", "sop"]
    assert "Saved SOP Schedule Plan 'County Operational Day'" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_rf_guard_preflight_validates_sop_plan_radio_lanes_independently(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    store = tab.plan_context_service.store
    radio_20m = store.save_device_profile(
        {
            "name": "Twenty Meter Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "block",
        }
    )
    radio_40m = store.save_device_profile(
        {
            "name": "Forty Meter Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "antenna_band_guard_mode": "block",
        }
    )
    payload = {
        "name": "Two Radio SOP Day",
        "category": "sop_schedule",
        "schedule_refs": [
            {
                "source": "SOP",
                "lane_key": f"radio:{int(radio_20m['id'])}",
                "radio_id": int(radio_20m["id"]),
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "20M",
                "frequency": "14.078",
            },
            {
                "source": "NET",
                "lane_key": f"radio:{int(radio_40m['id'])}",
                "radio_id": int(radio_40m["id"]),
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.110",
            },
        ],
    }

    validation = tab._rf_guard_preflight_for_plan(payload)
    app.processEvents()

    assert validation["state"] == "ok"
    assert validation["rf_guard_validation"] == "enforced"
    assert validation["radio_lane_ids"] == [int(radio_20m["id"]), int(radio_40m["id"])]


def test_freqplanner_rf_guard_preflight_blocks_sibling_radio_lane_band_conflict(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    store = tab.plan_context_service.store
    left_radio = store.save_device_profile(
        {
            "name": "Left Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "band_overlap_guard_group": "Field",
            "band_overlap_guard_mode": "block",
        }
    )
    right_radio = store.save_device_profile(
        {
            "name": "Right Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "band_overlap_guard_group": "Field",
            "band_overlap_guard_mode": "warn",
        }
    )
    payload = {
        "name": "Sibling Conflict SOP Day",
        "category": "sop_schedule",
        "schedule_refs": [
            {
                "source": "NET",
                "lane_key": f"radio:{int(left_radio['id'])}",
                "target_device_profile_id": int(left_radio["id"]),
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.078",
            },
            {
                "source": "SOP",
                "lane_key": f"radio:{int(right_radio['id'])}",
                "target_device_profile_id": int(right_radio["id"]),
                "day_utc": "Monday",
                "start_utc": "02:30",
                "end_utc": "03:30",
                "band": "40M",
                "frequency": "7.110",
            },
        ],
    }

    validation = tab._rf_guard_preflight_for_plan(payload)
    app.processEvents()

    assert validation["state"] == "blocked"
    assert any("Prevent Band Overlap group FIELD" in message for message in validation["blocked"])


def test_freqplanner_rf_guard_preflight_blocks_sibling_radio_lane_overnight_conflict(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    store = tab.plan_context_service.store
    left_radio = store.save_device_profile(
        {
            "name": "Night Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "band_overlap_guard_group": "Field",
            "band_overlap_guard_mode": "block",
        }
    )
    right_radio = store.save_device_profile(
        {
            "name": "Morning Radio",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "band_overlap_guard_group": "Field",
            "band_overlap_guard_mode": "warn",
        }
    )
    payload = {
        "name": "Overnight Conflict SOP Day",
        "category": "sop_schedule",
        "schedule_refs": [
            {
                "source": "NET",
                "lane_key": f"radio:{int(left_radio['id'])}",
                "target_device_profile_id": int(left_radio["id"]),
                "day_utc": "Monday",
                "start_utc": "23:30",
                "end_utc": "01:00",
                "band": "40M",
                "frequency": "7.078",
            },
            {
                "source": "SOP",
                "lane_key": f"radio:{int(right_radio['id'])}",
                "target_device_profile_id": int(right_radio["id"]),
                "day_utc": "Tuesday",
                "start_utc": "00:15",
                "end_utc": "00:45",
                "band": "40M",
                "frequency": "7.110",
            },
        ],
    }

    validation = tab._rf_guard_preflight_for_plan(payload)
    app.processEvents()

    assert validation["state"] == "blocked"
    assert any("Prevent Band Overlap group FIELD" in message for message in validation["blocked"])


def test_freqplanner_sop_lanes_view_renders_operational_lanes(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "OPS"}, {"group": "COUNTY"}])
    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [],
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "80M",
                    "frequency": "3.590",
                    "group_name": "OPS",
                    "net_name": "Night Net",
                }
            ],
            [
                {
                    "sop_profile_id": 7,
                    "profile_name": "County SOP",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "COUNTY",
                    "action_label": "Call NCS",
                }
            ],
            [],
        ),
    )
    monkeypatch.setattr(tab, "_load_net_resources_from_db", lambda: [])
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("operational"))
    tab.operational_day_combo.setCurrentIndex(tab.operational_day_combo.findData("Monday"))
    tab.rebuild_table()
    app.processEvents()

    headers = [tab.table.horizontalHeaderItem(col).text() for col in range(tab.table.columnCount())]
    assert headers[0] == "UTC Hour"
    assert headers[1].startswith("Local Time (")
    assert "OPS" in headers
    assert "County SOP" in headers
    ops_col = headers.index("OPS")
    sop_col = headers.index("County SOP")

    assert tab.table.item(2, ops_col).text() == "Night Net 80M 3.590"
    assert tab.table.item(2, sop_col).text() == "Call NCS 20M 14.078"
    tab._on_schedule_cell_clicked(2, sop_col)
    assert "Call NCS" in tab.cell_inspector_label.text()


def test_freqplanner_sop_lanes_filters_to_configured_operating_groups(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "OPS"}])
    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [],
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "80M",
                    "frequency": "3.590",
                    "group_name": "OPS",
                    "net_name": "Ops Net",
                },
                {
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.110",
                    "group_name": "UNCONFIGURED",
                    "net_name": "Hidden Net",
                },
            ],
            [],
            [],
        ),
    )
    monkeypatch.setattr(tab, "_load_net_resources_from_db", lambda: [])
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("operational"))
    tab.operational_day_combo.setCurrentIndex(tab.operational_day_combo.findData("Monday"))
    tab.rebuild_table()
    app.processEvents()

    headers = [tab.table.horizontalHeaderItem(col).text() for col in range(tab.table.columnCount())]

    assert "OPS" in headers
    assert "UNCONFIGURED" not in headers


def test_freqplanner_named_source_sets_drive_blended_projection(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    monkeypatch.setattr(
        tab,
        "_load_live_schedules",
        lambda: (
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "LIVE",
                }
            ],
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "20M",
                    "frequency": "14.110",
                    "group_name": "LIVE",
                    "net_name": "Live Net",
                }
            ],
            [],
            [],
        ),
    )
    daily = save_source_set(
        tab.settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Exercise Daily",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
            }
        ],
    )
    net = save_source_set(
        tab.settings,
        HF_NET_SOURCE_SETS_KEY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "County Nets",
        [
            {
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "80M",
                "frequency": "3.590",
                "group_name": "OPS",
                "net_name": "County Net",
            }
        ],
    )
    tab._refresh_source_set_controls()
    tab.hf_daily_source_combo.setCurrentIndex(tab.hf_daily_source_combo.findData(daily["id"]))
    tab.hf_net_source_combo.setCurrentIndex(tab.hf_net_source_combo.findData(net["id"]))

    projection = tab._build_blended_projection()
    app.processEvents()

    refs = projection.schedule_refs()
    assert [(row["source"], row["band"], row.get("net_name", "")) for row in refs] == [
        ("HF", "40M", ""),
        ("NET", "80M", "County Net"),
    ]
    assert tab._source_selection_summary() == "HF Daily: Exercise Daily; HF Nets: County Nets"


def test_hf_daily_tab_saves_named_freqplanner_source(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.daily_schedule_tab as daily_mod

    daily_mod = importlib.reload(daily_mod)
    messages = []
    monkeypatch.setattr(daily_mod.QMessageBox, "information", lambda *args: messages.append(args))
    monkeypatch.setattr(daily_mod.QMessageBox, "warning", lambda *args: messages.append(args))
    monkeypatch.setattr(daily_mod.QMessageBox, "critical", lambda *args: messages.append(args))
    tab = daily_mod.DailyScheduleTab.__new__(daily_mod.DailyScheduleTab)
    tab.settings = settings
    tab._source_rows_for_freqplanner_snapshot = lambda: [
        {
            "day_utc": "Monday",
            "start_utc": "01:00",
            "end_utc": "02:00",
            "band": "40M",
            "frequency": "7.078",
            "group_name": "OPS",
        }
    ]
    tab._prompt_for_freqplanner_source_name = lambda *args, **kwargs: ("Button Daily", True)
    refreshed = []
    tab._refresh_freq_planner = lambda: refreshed.append(True)

    daily_mod.DailyScheduleTab._on_save_freqplanner_source_clicked(tab)
    app.processEvents()

    saved_plans = [
        row
        for row in MultiRadioStore().list_frequency_plans()
        if str(row.get("category") or "") == "hf_daily_schedule"
    ]
    assert saved_plans[0]["name"] == "Button Daily"
    refs = json.loads(saved_plans[0]["schedule_refs_json"])
    assert refs[0]["frequency"] == "7.078"
    assert settings.get(SELECTED_HF_DAILY_SOURCE_SET_KEY) == f"plan:{saved_plans[0]['id']}"
    assert refreshed == [True]
    assert any("Select it in FreqPlanner Overview" in str(args[-1]) for args in messages)


def test_hf_net_tab_saves_named_freqplanner_source(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    messages = []
    monkeypatch.setattr(net_mod.QMessageBox, "information", lambda *args: messages.append(args))
    monkeypatch.setattr(net_mod.QMessageBox, "warning", lambda *args: messages.append(args))
    monkeypatch.setattr(net_mod.QMessageBox, "critical", lambda *args: messages.append(args))
    tab = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    tab.settings = settings
    tab._source_rows_for_freqplanner_snapshot = lambda: [
        {
            "day_utc": "Monday",
            "start_utc": "02:00",
            "end_utc": "03:00",
            "band": "80M",
            "frequency": "3.590",
            "group_name": "OPS",
            "net_name": "Ops Net",
        }
    ]
    tab._prompt_for_freqplanner_source_name = lambda *args, **kwargs: ("Button Nets", True)
    refreshed = []
    tab._refresh_freq_planner = lambda: refreshed.append(True)

    net_mod.NetScheduleTab._on_save_freqplanner_source_clicked(tab)
    app.processEvents()

    saved_plans = [
        row
        for row in MultiRadioStore().list_frequency_plans()
        if str(row.get("category") or "") == "hf_net_schedule"
    ]
    assert saved_plans[0]["name"] == "Button Nets"
    refs = json.loads(saved_plans[0]["schedule_refs_json"])
    assert refs[0]["net_name"] == "Ops Net"
    assert settings.get(SELECTED_HF_NET_SOURCE_SET_KEY) == f"plan:{saved_plans[0]['id']}"
    assert refreshed == [True]
    assert any("Select it in FreqPlanner Overview" in str(args[-1]) for args in messages)


def test_freqplanner_db_named_source_schedules_drive_projection(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    daily = save_source_schedule(
        settings,
        "hf_daily_schedule",
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "DB Daily",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
            }
        ],
    )
    net = save_source_schedule(
        settings,
        "hf_net_schedule",
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "DB Nets",
        [
            {
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "80M",
                "frequency": "3.590",
                "group_name": "OPS",
                "net_name": "Ops Net",
            }
        ],
    )
    tab = planner_mod.FreqPlannerTab()
    monkeypatch.setattr(tab, "_load_live_schedules", lambda: ([], [], [], []))
    tab._refresh_source_set_controls()

    assert tab.hf_daily_source_combo.findData(daily["id"]) >= 0
    assert tab.hf_net_source_combo.findData(net["id"]) >= 0
    projection = tab._build_blended_projection()
    app.processEvents()

    assert [(row["source"], row["band"], row.get("net_name", "")) for row in projection.schedule_refs()] == [
        ("HF", "40M", ""),
        ("NET", "80M", "Ops Net"),
    ]

    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    monkeypatch.setattr(tab, "_prompt_for_name", lambda *args, **kwargs: ("Assigned Blend", True))
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)

    tab._on_save_plan_clicked()
    app.processEvents()

    saved = next(row for row in tab.plan_context_service.store.list_frequency_plans() if row["name"] == "Assigned Blend")
    source_refs = json.loads(str(saved["source_refs_json"]))
    assert "hf_daily" in source_refs
    assert "hf_nets" in source_refs
    assert source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"]) in source_refs
    assert source_schedule_dependency_ref(HF_NET_SOURCE_CATEGORY, net["id"]) in source_refs


def test_saved_source_update_reports_rf_guard_impacts_for_assigned_master_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "Twenty Meter Only",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "block",
        }
    )
    daily = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Safe Daily",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "20M",
                "frequency": "14.078",
                "group_name": "OPS",
            }
        ],
    )
    source_refs = ["hf_daily", source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"])]
    plan = store.save_frequency_plan(
        {
            "name": "Assigned Master",
            "source_refs": source_refs,
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "OPS",
                }
            ],
            "frequency_refs": ["20M:14.078"],
            "group_refs": ["OPS"],
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    impacts = assigned_plan_rf_guard_impacts_for_source_update(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        daily["id"],
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
            }
        ],
    )

    assert len(impacts) == 1
    assert impacts[0]["plan"]["name"] == "Assigned Master"
    assert impacts[0]["device"]["name"] == "Twenty Meter Only"
    assert impacts[0]["validation"]["state"] == "blocked"
    assert "antenna support does not include 40M" in " ".join(impacts[0]["validation"]["messages"])


def test_saved_net_update_reports_rf_guard_impacts_for_assigned_master_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "Net Guard Radio",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["20M"],
            "antenna_band_guard_mode": "block",
        }
    )
    net = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Safe Nets",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "20M",
                "frequency": "14.078",
                "group_name": "OPS",
                "net_name": "Safe Net",
            }
        ],
    )
    plan = store.save_frequency_plan(
        {
            "name": "Assigned Net Master",
            "source_refs": ["hf_nets", source_schedule_dependency_ref(HF_NET_SOURCE_CATEGORY, net["id"])],
            "schedule_refs": [
                {
                    "source": "NET",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "OPS",
                    "net_name": "Safe Net",
                }
            ],
            "frequency_refs": ["20M:14.078"],
            "group_refs": ["OPS"],
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    impacts = assigned_plan_rf_guard_impacts_for_source_update(
        settings,
        HF_NET_SOURCE_CATEGORY,
        net["id"],
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
                "net_name": "Unsafe Net",
            }
        ],
    )

    assert len(impacts) == 1
    assert impacts[0]["plan"]["name"] == "Assigned Net Master"
    assert impacts[0]["device"]["name"] == "Net Guard Radio"
    assert impacts[0]["validation"]["state"] == "blocked"
    assert "antenna support does not include 40M" in " ".join(impacts[0]["validation"]["messages"])


def test_freqplanner_edit_bridge_loads_selected_daily_source_schedule(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    saved = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "DB Daily",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
            }
        ],
    )
    tab = planner_mod.FreqPlannerTab()
    loaded_rows = []
    refreshes = []

    class TargetDailyTab:
        def __init__(self) -> None:
            self.schedule_source_combo = QComboBox()

        def _refresh_freqplanner_source_combo(self) -> None:
            refreshes.append(True)
            self.schedule_source_combo.clear()
            self.schedule_source_combo.addItem("Live Current", LIVE_SOURCE_SET_ID)
            self.schedule_source_combo.addItem("DB Daily", saved["id"])

        def _confirm_discard_unsaved_source_load(self) -> bool:
            return True

        def _load_source_rows_into_table(self, rows) -> None:
            loaded_rows.extend(rows)

    target = TargetDailyTab()
    result = tab._load_selected_source_schedule_in_tab(
        target,
        selected_key=SELECTED_HF_DAILY_SOURCE_SET_KEY,
        sets_key=HF_DAILY_SOURCE_SETS_KEY,
        category=HF_DAILY_SOURCE_CATEGORY,
        live_loader_name="_load_schedule",
    )
    app.processEvents()

    assert result is True
    assert refreshes == [True]
    assert target.schedule_source_combo.currentData() == saved["id"]
    assert loaded_rows[0]["frequency"] == "7.078"


def test_freqplanner_edit_bridge_cancels_before_focusing_stale_source(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    saved = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "DB Nets",
        [
            {
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "80M",
                "frequency": "3.590",
                "group_name": "OPS",
                "net_name": "Ops Net",
            }
        ],
    )
    tab = planner_mod.FreqPlannerTab()
    loaded_rows = []

    class TargetNetTab:
        def __init__(self) -> None:
            self.schedule_source_combo = QComboBox()

        def _refresh_freqplanner_source_combo(self) -> None:
            self.schedule_source_combo.clear()
            self.schedule_source_combo.addItem("Live Current", LIVE_SOURCE_SET_ID)
            self.schedule_source_combo.addItem("DB Nets", saved["id"])

        def _confirm_discard_unsaved_source_load(self) -> bool:
            return False

        def _load_source_rows_into_table(self, rows) -> None:
            loaded_rows.extend(rows)

    target = TargetNetTab()
    result = tab._load_selected_source_schedule_in_tab(
        target,
        selected_key=SELECTED_HF_NET_SOURCE_SET_KEY,
        sets_key=HF_NET_SOURCE_SETS_KEY,
        category=HF_NET_SOURCE_CATEGORY,
        live_loader_name="_load",
    )
    app.processEvents()

    assert result is None
    assert target.schedule_source_combo.currentData() == saved["id"]
    assert loaded_rows == []


def test_named_source_set_ids_remain_unique_for_duplicate_names(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()

    first = save_source_set(
        settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Exercise Daily",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M"}],
    )
    second = save_source_set(
        settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Exercise Daily",
        [{"day_utc": "Monday", "start_utc": "02:00", "end_utc": "03:00", "band": "80M"}],
    )

    saved_sets = settings.get(HF_DAILY_SOURCE_SETS_KEY)
    assert first["id"] != second["id"]
    assert len({row["id"] for row in saved_sets}) == 2
    assert settings.get(SELECTED_HF_DAILY_SOURCE_SET_KEY) == second["id"]


def test_source_schedule_delete_refuses_non_source_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    normal_plan = MultiRadioStore().save_frequency_plan(
        {
            "name": "Assignable Plan",
            "category": "normal",
            "schedule_refs": [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00"}],
        }
    )

    with pytest.raises(ValueError, match="source schedule"):
        delete_source_schedule(
            settings,
            HF_DAILY_SOURCE_SETS_KEY,
            SELECTED_HF_DAILY_SOURCE_SET_KEY,
            f"plan:{normal_plan['id']}",
        )

    assert MultiRadioStore().get_frequency_plan(int(normal_plan["id"])) is not None


def test_daily_and_net_named_source_loads_confirm_before_discarding_dirty_edits(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()

    import freqinout.gui.daily_schedule_tab as daily_mod
    import freqinout.gui.net_schedule_tab as net_mod

    daily_mod = importlib.reload(daily_mod)
    net_mod = importlib.reload(net_mod)

    daily_tab = daily_mod.DailyScheduleTab.__new__(daily_mod.DailyScheduleTab)
    daily_tab._dirty = True
    net_tab = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    net_tab._dirty = True
    prompts = []

    def _cancel_question(_parent, title, text, *args, **kwargs):
        prompts.append((str(title), str(text)))
        return QMessageBox.Cancel

    monkeypatch.setattr(daily_mod.QMessageBox, "question", _cancel_question)
    monkeypatch.setattr(net_mod.QMessageBox, "question", _cancel_question)

    assert daily_mod.DailyScheduleTab._confirm_discard_unsaved_source_load(daily_tab) is False
    assert net_mod.NetScheduleTab._confirm_discard_unsaved_source_load(net_tab) is False
    assert len(prompts) == 2
    assert all("Unsaved edits" in text for _title, text in prompts)


def test_freqplanner_sop_lanes_view_renders_selected_saved_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "COUNTY"}])
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Saved SOP Day",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "lane_key": "sop:7",
                    "lane_label": "Saved County SOP",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "COUNTY",
                    "action_label": "Saved Call",
                }
            ],
        }
    )
    monkeypatch.setattr(tab, "_load_schedules", lambda: ([], [], [], []))
    monkeypatch.setattr(tab, "_load_net_resources_from_db", lambda: [])
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("operational"))
    tab.operational_day_combo.setCurrentIndex(tab.operational_day_combo.findData("Monday"))
    tab.rebuild_table()
    app.processEvents()

    headers = [tab.table.horizontalHeaderItem(col).text() for col in range(tab.table.columnCount())]
    sop_col = headers.index("Saved County SOP")

    assert tab.table.item(2, sop_col).text() == "Saved Call 20M 14.078"
    tab._on_schedule_cell_clicked(2, sop_col)
    assert tab.edit_sop_plan_entry_btn.isEnabled()
    assert "saved plan 'Saved SOP Day'" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_updates_selected_sop_plan_entry_locally(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "COUNTY"}])
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Editable SOP Day",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "lane_key": "sop:7",
                    "lane_label": "County SOP",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "COUNTY",
                    "action_label": "Call NCS",
                }
            ],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    projection = tab._build_selected_sop_plan_projection(dt.date(2026, 8, 2))
    assert projection is not None
    entry = projection.cell_for("sop:7", "Monday", 2).entries[0]

    payload = tab._updated_sop_plan_payload_for_entry(
        saved,
        entry,
        {
            "day_utc": "Monday",
            "start_utc": "02:30",
            "end_utc": "03:30",
            "band": "40M",
            "frequency": "7.110",
            "mode": "USB",
            "group_name": "COUNTY",
            "net_name": "",
            "action_label": "Monitor Peer",
            "lane_label": "County SOP",
        },
    )
    app.processEvents()

    refs = payload["schedule_refs"]
    assert payload["id"] == saved["id"]
    assert refs[0]["action_label"] == "Monitor Peer"
    assert refs[0]["band"] == "40M"
    assert payload["frequency_refs"] == ["40M:7.110"]


def test_freqplanner_group_plan_local_edit_updates_lane_identity(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "OPS"}, {"group": "COUNTY"}])
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Editable Net Day",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "NET",
                    "lane_key": "group:OPS",
                    "lane_label": "OPS",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.110",
                    "group_name": "OPS",
                    "net_name": "Ops Net",
                }
            ],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    projection = tab._build_selected_sop_plan_projection(dt.date(2026, 8, 2))
    entry = projection.cell_for("group:OPS", "Monday", 2).entries[0]

    payload = tab._updated_sop_plan_payload_for_entry(
        saved,
        entry,
        {
            "day_utc": "Monday",
            "start_utc": "02:00",
            "end_utc": "03:00",
            "band": "40M",
            "frequency": "7.110",
            "mode": "",
            "group_name": "COUNTY",
            "net_name": "County Net",
            "action_label": "",
            "lane_label": "OPS",
        },
    )
    app.processEvents()

    ref = payload["schedule_refs"][0]
    assert ref["lane_key"] == "group:COUNTY"
    assert ref["lane_label"] == "COUNTY"
    assert payload["group_refs"] == ["COUNTY"]


def test_freqplanner_plan_local_edit_rejects_unconfigured_group(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "OPS"}])
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Editable Net Day",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "NET",
                    "lane_key": "group:OPS",
                    "lane_label": "OPS",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.110",
                    "group_name": "OPS",
                    "net_name": "Ops Net",
                }
            ],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    projection = tab._build_selected_sop_plan_projection(dt.date(2026, 8, 2))
    entry = projection.cell_for("group:OPS", "Monday", 2).entries[0]

    payload = tab._updated_sop_plan_payload_for_entry(
        saved,
        entry,
        {
            "day_utc": "Monday",
            "start_utc": "02:00",
            "end_utc": "03:00",
            "band": "40M",
            "frequency": "7.110",
            "mode": "",
            "group_name": "UNCONFIGURED",
            "net_name": "County Net",
            "action_label": "",
            "lane_label": "UNCONFIGURED",
        },
    )
    app.processEvents()

    assert payload is None
    assert "configured Operating Group" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_plan_local_resource_edit_can_update_master_net_resource(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config_dir / "freqinout_nets.db")
    conn.execute(
        """
        CREATE TABLE net_resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_set TEXT,
            source_type TEXT,
            source_ref TEXT,
            readonly INTEGER,
            day_utc TEXT,
            recurrence TEXT,
            biweekly_offset_weeks INTEGER,
            month_weeks TEXT,
            group_name TEXT,
            band TEXT,
            mode TEXT,
            frequency TEXT,
            start_utc TEXT,
            end_utc TEXT,
            early_checkin INTEGER,
            primary_js8call_group TEXT,
            coverage TEXT,
            comment TEXT,
            net_name TEXT,
            fldigi_mode TEXT,
            fldigi_offset TEXT,
            updated_utc TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO net_resources (
            resource_set, source_type, source_ref, readonly, day_utc, recurrence,
            biweekly_offset_weeks, month_weeks, group_name, band, mode, frequency,
            start_utc, end_utc, early_checkin, primary_js8call_group, coverage,
            comment, net_name, fldigi_mode, fldigi_offset, updated_utc
        )
        VALUES ('Custom', 'manual', 'manual', 1, 'Monday', 'Weekly', 0, '',
                'OPS', '40M', 'USB', '7.078', '02:00', '03:00', 0,
                'OPS', '', '', 'Ops Net', '', '', '')
        """
    )
    resource_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.commit()
    conn.close()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "OPS"}, {"group": "COUNTY"}])
    ref = {
        "source": "NET_RESOURCE",
        "resource_id": resource_id,
        "lane_key": "group:OPS",
        "lane_label": "OPS",
        "day_utc": "Monday",
        "start_utc": "02:00",
        "end_utc": "03:00",
        "band": "40M",
        "frequency": "7.078",
        "group_name": "OPS",
        "net_name": "Ops Net",
    }
    updates = {
        "day_utc": "Tuesday",
        "start_utc": "04:00",
        "end_utc": "05:00",
        "band": "80M",
        "frequency": "3.590",
        "mode": "USB",
        "group_name": "COUNTY",
        "net_name": "County Net",
        "action_label": "",
        "lane_label": "COUNTY",
    }

    assert tab._update_master_net_resource_from_plan_ref(resource_id, ref, updates)
    app.processEvents()

    conn = sqlite3.connect(config_dir / "freqinout_nets.db")
    row = conn.execute(
        """
        SELECT source_type, source_ref, day_utc, group_name, band, frequency,
               start_utc, end_utc, primary_js8call_group, net_name
          FROM net_resources
         WHERE id=?
        """,
        (resource_id,),
    ).fetchone()
    conn.close()

    assert row == (
        "manual",
        "updated_from_sop_schedule_plan",
        "Tuesday",
        "COUNTY",
        "80M",
        "3.590",
        "04:00",
        "05:00",
        "COUNTY",
        "County Net",
    )


def test_freqplanner_resource_update_workflow_confirmation_matches_update_both(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "OPS"}])
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Resource Workflow SOP Day",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "NET_RESOURCE",
                    "resource_id": 77,
                    "lane_key": "group:OPS",
                    "lane_label": "OPS",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                    "net_name": "Ops Net",
                }
            ],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    projection = tab._build_selected_sop_plan_projection(dt.date(2026, 8, 2))
    entry = projection.cell_for("group:OPS", "Monday", 2).entries[0]
    monkeypatch.setattr(tab, "_choose_operational_entry_for_edit", lambda: entry)
    monkeypatch.setattr(
        tab,
        "_edit_plan_entry_dialog",
        lambda _entry: {
            "day_utc": "Monday",
            "start_utc": "02:00",
            "end_utc": "03:00",
            "band": "40M",
            "frequency": "7.110",
            "mode": "",
            "group_name": "OPS",
            "net_name": "Ops Net",
            "action_label": "",
            "lane_label": "OPS",
        },
    )
    monkeypatch.setattr(tab, "_preflight_master_net_resource_update", lambda _rid: None)
    monkeypatch.setattr(tab, "_update_master_net_resource_from_plan_ref", lambda *_args: True)
    monkeypatch.setattr(tab, "_save_plan_payload_with_guard", lambda payload, **_kwargs: {"id": payload["id"], "name": payload["name"]})
    tab.rebuild_table = lambda: None
    prompts = []

    def _question(_parent, title, text, *args, **kwargs):
        prompts.append((str(title), str(text)))
        if str(title) == "Update Master Net Resource?":
            return QMessageBox.Yes
        return QMessageBox.Save

    monkeypatch.setattr(planner_mod.QMessageBox, "question", _question)

    tab._on_edit_sop_plan_entry_clicked()
    app.processEvents()

    assert prompts[0][0] == "Update Master Net Resource?"
    assert prompts[1][0] == "Save Plan-Local Edit"
    assert "update Net Resource #77" in prompts[1][1]
    assert "master Net Resources will not be changed" not in prompts[1][1]


def test_freqplanner_resource_update_preflight_blocks_plan_save_when_resource_missing(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.settings.set("operating_groups", [{"group": "OPS"}])
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Missing Resource SOP Day",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "NET_RESOURCE",
                    "resource_id": 404,
                    "lane_key": "group:OPS",
                    "lane_label": "OPS",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                    "net_name": "Ops Net",
                }
            ],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    projection = tab._build_selected_sop_plan_projection(dt.date(2026, 8, 2))
    entry = projection.cell_for("group:OPS", "Monday", 2).entries[0]
    monkeypatch.setattr(tab, "_choose_operational_entry_for_edit", lambda: entry)
    monkeypatch.setattr(
        tab,
        "_edit_plan_entry_dialog",
        lambda _entry: {
            "day_utc": "Monday",
            "start_utc": "02:00",
            "end_utc": "03:00",
            "band": "80M",
            "frequency": "3.590",
            "mode": "",
            "group_name": "OPS",
            "net_name": "Ops Net",
            "action_label": "",
            "lane_label": "OPS",
        },
    )
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)
    warnings = []
    monkeypatch.setattr(planner_mod.QMessageBox, "warning", lambda _parent, title, text: warnings.append((str(title), str(text))))
    save_calls = []
    monkeypatch.setattr(tab, "_save_plan_payload_with_guard", lambda *args, **kwargs: save_calls.append(args) or {"id": 1})

    tab._on_edit_sop_plan_entry_clicked()
    app.processEvents()

    assert not save_calls
    assert warnings and warnings[0][0] == "Master Net Resource Unavailable"
    assert "no SOP Schedule Plan edit was saved" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_db_loaded_projection_preserves_source_row_identity(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(config_dir / "freqinout.db")
    conn.execute(
        """
        CREATE TABLE daily_schedule_tab (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_utc TEXT,
            band TEXT,
            mode TEXT,
            vfo TEXT,
            frequency TEXT,
            start_utc TEXT,
            end_utc TEXT,
            group_name TEXT,
            auto_tune INTEGER
        )
        """
    )
    daily_id = conn.execute(
        """
        INSERT INTO daily_schedule_tab(day_utc, band, mode, vfo, frequency, start_utc, end_utc, group_name, auto_tune)
        VALUES ('Monday', '40M', 'JS8', 'A', '7.078', '01:00', '03:00', 'OPS', 0)
        """
    ).lastrowid
    conn.commit()
    conn.close()

    conn = sqlite3.connect(config_dir / "freqinout_nets.db")
    conn.execute(
        """
        CREATE TABLE net_schedule_tab (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_utc TEXT,
            recurrence TEXT,
            biweekly_offset_weeks INTEGER,
            month_weeks TEXT,
            band TEXT,
            mode TEXT,
            vfo TEXT,
            frequency TEXT,
            start_utc TEXT,
            end_utc TEXT,
            early_checkin INTEGER,
            primary_js8call_group TEXT,
            comment TEXT,
            net_name TEXT,
            group_name TEXT,
            resource_id INTEGER,
            target_scope TEXT,
            target_device_profile_id INTEGER,
            target_operating_profile_id INTEGER
        )
        """
    )
    net_id = conn.execute(
        """
        INSERT INTO net_schedule_tab(
            day_utc, recurrence, biweekly_offset_weeks, month_weeks, band, mode, vfo, frequency,
            start_utc, end_utc, early_checkin, primary_js8call_group, comment, net_name, group_name,
            resource_id, target_scope, target_device_profile_id, target_operating_profile_id
        )
        VALUES ('Monday', 'Weekly', 0, '', '80M', 'JS8', 'A', '3.590',
                '02:00', '03:00', 0, 'OPS', '', 'Night Net', 'OPS',
                314, 'operating_profile', NULL, 42)
        """
    ).lastrowid
    conn.commit()
    conn.close()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    projection = tab._build_blended_projection()
    app.processEvents()

    refs = projection.schedule_refs()
    daily_ref = next(row for row in refs if row["source"] == "HF")
    net_ref = next(row for row in refs if row["source"] == "NET")
    assert daily_ref["source_table"] == "daily_schedule_tab"
    assert daily_ref["source_row_id"] == daily_id
    assert daily_ref["source_key"] == f"HF:{daily_id}"
    assert net_ref["source_table"] == "net_schedule_tab"
    assert net_ref["source_row_id"] == net_id
    assert net_ref["resource_id"] == 314
    assert net_ref["target_scope"] == "operating_profile"
    assert net_ref["target_operating_profile_id"] == 42


def test_freqplanner_grid_uses_projection_cells_and_inspector(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab._show_local = False
    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                }
            ],
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "80M",
                    "frequency": "3.590",
                    "group_name": "OPS",
                    "net_name": "Night Net",
                }
            ],
            [],
            [],
        ),
    )

    tab.rebuild_table()
    app.processEvents()

    monday_col = planner_mod.DAY_NAMES.index("Monday") + tab.COL_DAY_OFFSET
    item = tab.table.item(2, monday_col)
    assert item is not None
    assert item.text() == "Night Net"
    cell = item.data(Qt.UserRole)
    assert isinstance(cell, ProjectionCell)
    assert cell.effective_source == "NET"
    assert cell.net_segments[0].net_name == "Night Net"

    tab._on_schedule_cell_clicked(2, monday_col)
    inspector = tab.cell_inspector_label.text()
    assert tab.edit_hf_daily_btn.isEnabled()
    assert tab.edit_hf_net_btn.isEnabled()
    assert not tab.open_sop_builder_btn.isEnabled()
    assert "Effective: NET Night Net" in inspector
    assert "HF Daily: HF 01:00-03:00 OPS 40M 7.078" in inspector
    assert "HF Nets: NET 02:00-03:00 Night Net 80M 3.590" in inspector

    tab._on_schedule_cell_clicked(0, tab.COL_UTC)
    assert not tab.edit_hf_daily_btn.isEnabled()
    assert not tab.edit_hf_net_btn.isEnabled()
    assert not tab.open_sop_builder_btn.isEnabled()


def test_freqplanner_prompts_for_source_jump_when_cell_has_multiple_matching_source_rows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab._show_local = False
    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                },
                {
                    "day_utc": "Monday",
                    "start_utc": "01:30",
                    "end_utc": "02:30",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                },
            ],
            [],
            [],
            [],
        ),
    )

    tab.rebuild_table()
    app.processEvents()

    monday_col = planner_mod.DAY_NAMES.index("Monday") + tab.COL_DAY_OFFSET
    tab._on_schedule_cell_clicked(2, monday_col)

    assert len(tab._selected_projection_cell.hf_segments) == 2
    assert tab.edit_hf_daily_btn.isEnabled()
    assert "Multiple HF Daily source rows match this cell" in tab.edit_hf_daily_btn.toolTip()

    prompts = []

    def _choose_item(_parent, title, text, labels, current, editable):
        prompts.append((str(title), str(text), list(labels), current, editable))
        return labels[1], True

    monkeypatch.setattr(planner_mod.QInputDialog, "getItem", _choose_item)
    monkeypatch.setattr(tab, "_navigate_to_tab", lambda _label: None)

    tab._on_edit_hf_daily_clicked()

    assert prompts
    assert prompts[0][0] == "Choose HF Daily Source"
    assert "Multiple source rows match this cell" in prompts[0][1]
    assert "01:30-02:30" in prompts[0][2][1]


def test_freqplanner_sop_action_prefers_exact_source_focus(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    projection = build_blended_schedule_projection(
        [],
        [],
        [
            {
                "sop_profile_id": 7,
                "sop_layer_id": 11,
                "source_row_id": 11,
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "20M",
                "frequency": "14.078",
                "group_name": "OPS",
                "profile_name": "HF SOP",
            }
        ],
        [],
        week_start_utc=dt.date(2026, 8, 2),
    )
    cell = next(cell for cell in projection.cells if cell.day_utc == "Monday" and cell.hour_utc == 2)
    tab._set_projection_inspector(cell)

    class _StubSopTab:
        def __init__(self):
            self.focused = []
            self.selected = []

        def focus_source_segment(self, segment):
            self.focused.append(segment.raw.get("sop_layer_id"))
            return True

        def select_profile(self, profile_id):
            self.selected.append(profile_id)
            return True

    class _StubWindow:
        def __init__(self):
            self.sop_tab = _StubSopTab()

    stub_window = _StubWindow()
    monkeypatch.setattr(tab, "window", lambda: stub_window)
    monkeypatch.setattr(tab, "_navigate_to_tab", lambda _label: None)

    tab._on_open_sop_builder_clicked()
    app.processEvents()

    assert stub_window.sop_tab.focused == [11]
    assert stub_window.sop_tab.selected == []
    assert "source row" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_sop_action_falls_back_to_profile_when_exact_layer_focus_unavailable(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    projection = build_blended_schedule_projection(
        [],
        [],
        [
            {
                "sop_profile_id": 7,
                "sop_layer_id": 11,
                "source_row_id": 11,
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "20M",
                "frequency": "14.078",
                "group_name": "OPS",
                "profile_name": "HF SOP",
            }
        ],
        [],
        week_start_utc=dt.date(2026, 8, 2),
    )
    cell = next(cell for cell in projection.cells if cell.day_utc == "Monday" and cell.hour_utc == 2)
    tab._set_projection_inspector(cell)

    class _StubSopTab:
        def __init__(self):
            self.focused = []
            self.selected = []

        def focus_source_segment(self, segment):
            self.focused.append(segment.raw.get("sop_layer_id"))
            return False

        def select_profile(self, profile_id):
            self.selected.append(profile_id)
            return True

    class _StubWindow:
        def __init__(self):
            self.sop_tab = _StubSopTab()

    stub_window = _StubWindow()
    monkeypatch.setattr(tab, "window", lambda: stub_window)
    monkeypatch.setattr(tab, "_navigate_to_tab", lambda _label: None)

    tab._on_open_sop_builder_clicked()
    app.processEvents()

    assert stub_window.sop_tab.focused == [11]
    assert stub_window.sop_tab.selected == [7]
    assert "source profile" in tab.frequency_plan_action_hint_label.text()
    assert "source row" not in tab.frequency_plan_action_hint_label.text()


def test_source_tabs_focus_projection_segments_by_source_identity(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.daily_schedule_tab as daily_mod
    import freqinout.gui.net_schedule_tab as net_mod

    daily_mod = importlib.reload(daily_mod)
    net_mod = importlib.reload(net_mod)
    daily_tab = daily_mod.DailyScheduleTab()
    net_tab = net_mod.NetScheduleTab()

    daily_tab.table.setRowCount(0)
    daily_tab._append_entry_row(
        {
            "source_row_id": 17,
            "source_key": "HF:17",
            "day_utc": "Monday",
            "group_name": "OPS",
            "mode": "JS8",
            "band": "40M",
            "frequency": "7.078",
            "start_utc": "01:00",
            "end_utc": "02:00",
        }
    )
    net_tab.table.setRowCount(0)
    net_tab._add_row(
        {
            "source_row_id": 23,
            "source_key": "NET:23",
            "_resource_id": 314,
            "day_utc": "Monday",
            "recurrence": "Weekly",
            "group_name": "OPS",
            "mode": "JS8",
            "band": "80M",
            "frequency": "3.590",
            "start_utc": "02:00",
            "end_utc": "03:00",
            "net_name": "Night Net",
        }
    )
    app.processEvents()

    daily_projection = build_blended_schedule_projection(
        [
            {
                "source_row_id": 17,
                "source_key": "HF:17",
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
            }
        ],
        [],
        [],
        [],
        week_start_utc=dt.date(2026, 8, 2),
    )
    net_projection = build_blended_schedule_projection(
        [],
        [
            {
                "source_row_id": 23,
                "source_key": "NET:23",
                "resource_id": 314,
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "80M",
                "frequency": "3.590",
                "group_name": "OPS",
                "net_name": "Night Net",
            }
        ],
        [],
        [],
        week_start_utc=dt.date(2026, 8, 2),
    )

    assert daily_tab.focus_source_segment(daily_projection.effective_segments[0])
    assert daily_tab.table.currentRow() == 0
    assert net_tab.focus_source_segment(net_projection.effective_segments[0])
    assert net_tab.table.currentRow() == 0


def test_source_tabs_refresh_source_identity_after_save(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.daily_schedule_tab as daily_mod
    import freqinout.gui.net_schedule_tab as net_mod

    daily_mod = importlib.reload(daily_mod)
    net_mod = importlib.reload(net_mod)
    monkeypatch.setattr(daily_mod.QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_mod.QMessageBox, "warning", lambda *args, **kwargs: None)
    monkeypatch.setattr(net_mod.QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(net_mod.QMessageBox, "warning", lambda *args, **kwargs: None)

    daily_tab = daily_mod.DailyScheduleTab()
    daily_tab.operating_groups = [{"group": "OPS", "band": "40M", "mode": "JS8", "freq": "7.078"}]
    daily_tab.table.setRowCount(0)
    daily_tab._append_entry_row(
        {
            "source_row_id": 99,
            "source_key": "HF:99",
            "day_utc": "Monday",
            "group_name": "OPS",
            "mode": "JS8",
            "band": "40M",
            "frequency": "7.078",
            "start_utc": "01:00",
            "end_utc": "02:00",
        }
    )
    daily_group = daily_tab.table.cellWidget(0, daily_tab.COL_GROUP)
    daily_band = daily_tab.table.cellWidget(0, daily_tab.COL_BAND)
    daily_mode = daily_tab.table.cellWidget(0, daily_tab.COL_MODE)
    for combo, value in ((daily_group, "OPS"), (daily_band, "40M"), (daily_mode, "JS8")):
        if combo.findText(value) < 0:
            combo.addItem(value)
        combo.setCurrentText(value)
    daily_tab.table.item(0, daily_tab.COL_FREQ).setText("7.078")
    daily_tab.table.item(0, daily_tab.COL_START).setText("01:00")
    daily_tab.table.item(0, daily_tab.COL_END).setText("02:00")

    net_tab = net_mod.NetScheduleTab()
    net_tab.operating_groups = [{"group": "OPS", "band": "80M", "mode": "Digi", "freq": "3.590"}]
    net_tab.table.setRowCount(0)
    net_tab._add_row(
        {
            "source_row_id": 88,
            "source_key": "NET:88",
            "day_utc": "Monday",
            "recurrence": "Weekly",
            "group_name": "OPS",
            "mode": "Digi",
            "band": "80M",
            "frequency": "3.590",
            "start_utc": "02:00",
            "end_utc": "03:00",
            "net_name": "Night Net",
        }
    )
    net_group = net_tab.table.cellWidget(0, net_tab.COL_GROUP)
    net_band = net_tab.table.cellWidget(0, net_tab.COL_BAND)
    net_mode = net_tab.table.cellWidget(0, net_tab.COL_MODE)
    for combo, value in ((net_group, "OPS"), (net_band, "80M"), (net_mode, "Digi")):
        if combo.findText(value) < 0:
            combo.addItem(value)
        combo.setCurrentText(value)
    net_tab.table.item(0, net_tab.COL_FREQ).setText("3.590")
    net_tab.table.item(0, net_tab.COL_START).setText("02:00")
    net_tab.table.item(0, net_tab.COL_END).setText("03:00")

    daily_tab._save_schedule()
    net_tab._save()
    app.processEvents()

    daily_select = daily_tab.table.cellWidget(0, daily_tab.COL_SELECT)
    net_select = net_tab.table.cellWidget(0, net_tab.COL_SELECT)
    daily_row_id = int(daily_select.property("source_row_id") or 0)
    net_row_id = int(net_select.property("source_row_id") or 0)
    assert daily_row_id > 0
    assert daily_row_id != 99
    assert daily_select.property("source_key") == f"HF:{daily_row_id}"
    assert net_row_id > 0
    assert net_row_id != 88
    assert net_select.property("source_key") == f"NET:{net_row_id}"
