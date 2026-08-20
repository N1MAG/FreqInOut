from __future__ import annotations

import datetime as dt
import inspect
import importlib
import json
import os
import sqlite3

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QAbstractItemView, QComboBox, QMessageBox
from PySide6.QtCore import Qt

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.schedule_projection import ProjectionCell
from freqinout.core.schedule_projection import build_blended_schedule_projection
from freqinout.core.operational_projection import OperationalEntry, OperationalLane
from freqinout.core.schedule_source_sets import (
    HF_DAILY_SOURCE_CATEGORY,
    HF_DAILY_SOURCE_SETS_KEY,
    HF_NET_SOURCE_CATEGORY,
    HF_NET_SOURCE_SETS_KEY,
    LIVE_SOURCE_SET_ID,
    NO_NET_SOURCE_SET_ID,
    NO_NET_SOURCE_SET_LABEL,
    SELECTED_HF_DAILY_SOURCE_SET_KEY,
    SELECTED_HF_NET_SOURCE_SET_KEY,
    assigned_plan_rf_guard_impacts_for_sop_update,
    assigned_plan_rf_guard_impacts_for_source_update,
    delete_source_schedule,
    refresh_source_backed_frequency_plans,
    rename_source_schedule,
    reproject_frequency_plans_for_source_update,
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
                    "sop_profile_id": 7,
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
    assert projection.source_refs() == ["hf_daily", "hf_nets", "sop", "sop_schedule_layer:7"]
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
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

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
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)
    tab.frequency_plan_combo.setEditText("Blended Watch")

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
    assert tab.assign_plan_btn.text() == "Assign with RF Guard"
    assert "not assigned to a radio yet" in tab.assign_plan_btn.toolTip()

    tab._on_assign_plan_clicked()
    assert "Settings > Assign Schedule" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_save_plan_requires_confirmation_when_rf_guard_preflight_skipped(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

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
    prompts = []

    def _capture_question(_parent, title, text, *args, **kwargs):
        prompts.append((str(title), str(text)))
        return QMessageBox.Save

    monkeypatch.setattr(planner_mod.QMessageBox, "question", _capture_question)
    tab.frequency_plan_combo.setEditText("No Radio Context")

    tab._on_save_plan_clicked()
    app.processEvents()

    assert [title for title, _text in prompts] == ["RF Guard Preflight Skipped"]
    assert "assignment-specific RF Safety Guard checks" in prompts[0][1]
    assert "RF Guard preflight skipped; assignment checks still required." in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_save_plan_updates_existing_editable_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    existing = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Old Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(existing["id"])))
    tab.frequency_plan_combo.setEditText("Updated Plan")
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
    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})

    tab._on_save_plan_clicked()
    app.processEvents()

    plans = tab.plan_context_service.store.list_frequency_plans()
    assert len([row for row in plans if row["name"] in {"Old Plan", "Updated Plan"}]) == 1
    saved = next(row for row in plans if int(row["id"]) == int(existing["id"]))
    assert saved["name"] == "Updated Plan"
    assert "Saved Frequency Plan 'Updated Plan'" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_rename_plan_does_not_change_schedule_refs(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Old Name",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                }
            ],
        }
    )
    original_refs = str(saved["schedule_refs_json"])
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    tab.frequency_plan_combo.setEditText("Magnet Plan")

    tab._on_rename_plan_clicked()
    app.processEvents()

    updated = tab.plan_context_service.store.get_frequency_plan(int(saved["id"]))
    assert updated["name"] == "Magnet Plan"
    assert str(updated["schedule_refs_json"]) == original_refs
    assert tab.frequency_plan_combo.currentText() == "Magnet Plan"
    assert "Schedule windows were not changed" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_selecting_saved_plan_loads_saved_source_layers(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    daily = save_source_set(
        tab.settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Saved Daily",
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
        "Saved Nets",
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
    )
    sop_plan = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Saved SOP",
            "status": "saved",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "day_utc": "Monday",
                    "start_utc": "03:00",
                    "end_utc": "04:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "OPS",
                    "profile_name": "Saved SOP",
                    "action_label": "Check in with NCS",
                }
            ],
        }
    )
    tab.settings.set(SELECTED_HF_DAILY_SOURCE_SET_KEY, LIVE_SOURCE_SET_ID)
    tab.settings.set(SELECTED_HF_NET_SOURCE_SET_KEY, LIVE_SOURCE_SET_ID)
    tab.settings.set("freqplanner_selected_sop_schedule_plan_id", LIVE_SOURCE_SET_ID)
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Layered Plan",
            "status": "saved",
            "category": "normal",
            "source_refs": [
                source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"]),
                source_schedule_dependency_ref(HF_NET_SOURCE_CATEGORY, net["id"]),
                f"sop_schedule_plan:{int(sop_plan['id'])}",
            ],
            "schedule_refs": [],
        }
    )

    tab._refresh_source_set_controls()
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    app.processEvents()

    assert tab.hf_daily_source_combo.currentData() == daily["id"]
    assert tab.hf_net_source_combo.currentData() == net["id"]
    assert tab.sop_plan_source_combo.currentData() == str(int(sop_plan["id"]))
    assert tab.settings.get(SELECTED_HF_DAILY_SOURCE_SET_KEY) == daily["id"]
    assert tab.settings.get(SELECTED_HF_NET_SOURCE_SET_KEY) == net["id"]
    assert tab.settings.get("freqplanner_selected_sop_schedule_plan_id") == str(int(sop_plan["id"]))
    projection = tab._build_blended_projection()
    assert [(row.source, row.band) for row in projection.effective_segments] == [("HF", "40M"), ("NET", "80M"), ("SOP", "20M")]
    assert not tab._frequency_plan_layers_dirty


def test_freqplanner_no_nets_choice_supplies_empty_net_layer(monkeypatch, tmp_path) -> None:
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
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                }
            ],
            [
                {
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "80M",
                    "frequency": "3.590",
                    "group_name": "OPS",
                    "net_name": "Unexpected Net",
                }
            ],
            [],
            [],
        ),
    )
    tab._refresh_source_set_controls()
    no_nets_idx = tab.hf_net_source_combo.findData(NO_NET_SOURCE_SET_ID)
    assert no_nets_idx >= 0
    assert tab.hf_net_source_combo.itemText(no_nets_idx) == NO_NET_SOURCE_SET_LABEL

    tab.hf_net_source_combo.setCurrentIndex(no_nets_idx)
    app.processEvents()

    projection = tab._build_blended_projection()
    payload, _count, _kind = tab._current_plan_payload_from_projection()
    assert [(row.source, row.band) for row in projection.effective_segments] == [("HF", "40M")]
    assert f"{HF_NET_SOURCE_CATEGORY}:{NO_NET_SOURCE_SET_ID}" in payload["source_refs"]
    assert "HF Nets: No Nets" in tab._source_selection_summary()


def test_freqplanner_saved_no_nets_plan_reloads_no_nets_selection(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    daily = save_source_set(
        tab.settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Saved Daily",
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
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Daily Only Plan",
            "status": "saved",
            "category": "normal",
            "source_refs": [
                source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"]),
                source_schedule_dependency_ref(HF_NET_SOURCE_CATEGORY, NO_NET_SOURCE_SET_ID),
            ],
            "schedule_refs": [],
        }
    )

    tab._refresh_source_set_controls()
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    app.processEvents()

    assert tab.hf_daily_source_combo.currentData() == daily["id"]
    assert tab.hf_net_source_combo.currentData() == NO_NET_SOURCE_SET_ID
    assert tab.settings.get(SELECTED_HF_NET_SOURCE_SET_KEY) == NO_NET_SOURCE_SET_ID


def test_freqplanner_layer_change_marks_selected_plan_modified_until_save(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    daily_one = save_source_set(
        tab.settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Daily One",
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
    daily_two = save_source_set(
        tab.settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Daily Two",
        [
            {
                "day_utc": "Monday",
                "start_utc": "03:00",
                "end_utc": "04:00",
                "band": "80M",
                "frequency": "3.590",
                "group_name": "OPS",
            }
        ],
    )
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Layer Change Plan",
            "status": "saved",
            "category": "normal",
            "source_refs": [source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily_one["id"])],
            "schedule_refs": [],
        }
    )
    tab._refresh_source_set_controls()
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    app.processEvents()
    assert not tab._frequency_plan_layers_dirty

    tab.hf_daily_source_combo.setCurrentIndex(tab.hf_daily_source_combo.findData(daily_two["id"]))
    app.processEvents()

    assert tab._frequency_plan_layers_dirty
    assert tab.plan_mode_label.text() == "Modified"
    assert tab.save_plan_btn.text() == "Update Plan"
    assert "Layer selections changed" in tab.save_plan_btn.toolTip()
    assert "Layer selection changed for 'Layer Change Plan'" in tab.frequency_plan_action_hint_label.text()
    assert "Update Plan saves the visible Daily, Nets, and SOP layers" in tab.frequency_plan_action_hint_label.text()

    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    tab._on_save_plan_clicked()
    app.processEvents()

    updated = tab.plan_context_service.store.get_frequency_plan(int(saved["id"]))
    assert source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily_two["id"]) in json.loads(str(updated["source_refs_json"]))
    assert not tab._frequency_plan_layers_dirty
    assert tab.plan_mode_label.text() == "Saved"
    assert tab.save_plan_btn.text() == "Save Plan"
    assert "Layer selections changed" not in tab.save_plan_btn.toolTip()


def test_freqplanner_saves_selected_sop_plan_layer_ref(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    sop_plan = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Storm SOP",
            "status": "saved",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                    "profile_name": "Storm SOP",
                }
            ],
        }
    )
    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    tab._refresh_source_set_controls()
    tab.sop_plan_source_combo.setCurrentIndex(tab.sop_plan_source_combo.findData(str(int(sop_plan["id"]))))
    tab.frequency_plan_combo.setEditText("Storm Watch Plan")

    tab._on_save_plan_clicked()
    app.processEvents()

    saved = next(row for row in tab.plan_context_service.store.list_frequency_plans() if row["name"] == "Storm Watch Plan")
    source_refs = json.loads(str(saved["source_refs_json"]))
    assert f"sop_schedule_plan:{int(sop_plan['id'])}" in source_refs
    assert "sop" in source_refs
    assert "SOP: Storm SOP" in tab.plan_layers_label.text()


def test_freqplanner_sop_plan_action_is_contextual(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    normal_plan = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Normal Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [],
        }
    )
    sop_plan = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Storm SOP",
            "status": "saved",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                    "profile_name": "Storm SOP",
                }
            ],
        }
    )
    monkeypatch.setattr(tab, "_has_sop_schedule_rows", lambda: True)
    tab._refresh_source_set_controls()
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(normal_plan["id"])))
    app.processEvents()

    assert tab.save_sop_plan_btn.isHidden() is True

    tab.sop_plan_source_combo.setCurrentIndex(tab.sop_plan_source_combo.findData(str(int(sop_plan["id"]))))
    app.processEvents()

    assert tab.save_sop_plan_btn.isHidden() is False
    assert tab.save_sop_plan_btn.text() == "Save SOP Plan"

    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(sop_plan["id"])))
    app.processEvents()

    assert tab.plan_mode_label.text() == "SOP Plan"
    assert tab.save_plan_btn.text() == "Create Frequency Plan"
    assert tab.save_sop_plan_btn.text() == "Update SOP Plan"


def test_freqplanner_refreshes_renamed_daily_layer_in_plan_manager(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    daily = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Old Daily",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "frequency": "7.078", "group_name": "OPS"}],
    )
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Layered Plan",
            "status": "saved",
            "category": "normal",
            "source_refs": [source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"])],
            "schedule_refs": [],
        }
    )
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))

    rename_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        daily["id"],
        "MagNet Daily",
    )
    tab.rebuild_table()
    app.processEvents()

    assert tab.hf_daily_source_combo.currentData() == daily["id"]
    assert tab.hf_daily_source_combo.currentText() == "MagNet Daily"


def test_freqplanner_activation_refreshes_renamed_daily_layer_without_pending_rebuild(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    daily = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Old Daily",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "frequency": "7.078", "group_name": "OPS"}],
    )
    tab = planner_mod.FreqPlannerTab()
    tab._pending_rebuild = False
    assert tab.hf_daily_source_combo.currentText() == "Old Daily"

    rename_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        daily["id"],
        "MagNet Daily",
    )
    tab.on_tab_activated()
    app.processEvents()

    assert not tab._pending_rebuild
    assert tab.hf_daily_source_combo.currentData() == daily["id"]
    assert tab.hf_daily_source_combo.currentText() == "MagNet Daily"


def test_freqplanner_schedule_source_change_refreshes_component_names(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    daily = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Old Daily",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "frequency": "7.078", "group_name": "OPS"}],
    )
    net = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Old Nets",
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
    tab._pending_rebuild = False

    rename_source_schedule(settings, HF_DAILY_SOURCE_CATEGORY, SELECTED_HF_DAILY_SOURCE_SET_KEY, daily["id"], "MagNet Daily")
    rename_source_schedule(settings, HF_NET_SOURCE_CATEGORY, SELECTED_HF_NET_SOURCE_SET_KEY, net["id"], "MagNet Nets")
    tab.on_schedule_sources_changed()
    app.processEvents()

    assert tab._pending_rebuild is True
    assert tab.hf_daily_source_combo.findText("MagNet Daily") >= 0
    assert tab.hf_net_source_combo.findText("MagNet Nets") >= 0
    assert tab.hf_daily_source_combo.currentData() == daily["id"]
    assert tab.hf_net_source_combo.currentData() == net["id"]


def test_freqplanner_new_plan_state_survives_rebuild(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Existing Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [],
        }
    )
    tab._refresh_plan_workspace_header()

    tab._on_new_plan_clicked()
    app.processEvents()

    assert tab.frequency_plan_combo.currentIndex() == -1
    assert tab.frequency_plan_combo.currentText() == ""
    assert tab._selected_frequency_plan_row() is None
    assert tab.plan_mode_label.text() == "New"
    assert tab.save_plan_btn.text() == "Create Plan"
    assert not tab.assign_plan_btn.isEnabled()
    assert not tab.delete_plan_btn.isEnabled()
    assert tab.selected_window_title_label.text() == "New Frequency Plan"


def test_freqplanner_selected_plan_summary_is_operator_facing(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Field Plan",
            "status": "saved",
            "category": "normal",
            "source_refs": ["hf_daily:plan:101", "hf_net:plan:202"],
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                }
            ],
        }
    )

    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    app.processEvents()

    summary = tab.frequency_plan_summary_label.text()
    assert "Field Plan" in summary
    assert "Change Daily, Nets, or SOP layers" in summary
    assert "source ref" not in summary
    assert "schedule ref" not in summary


def test_freqplanner_effective_windows_are_sortable_and_hint_is_operator_facing(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("effective"))
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

    tab.rebuild_table()
    app.processEvents()

    assert tab.table.isSortingEnabled()
    hint = tab.frequency_plan_action_hint_label.text()
    assert "Sort by day, time, layer, group, band, or purpose" in hint
    assert "saved window(s) from" not in hint


def test_freqplanner_plan_layer_summary_tracks_selected_layers_and_windows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    daily = save_source_set(
        tab.settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Field Daily",
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
    )
    net = save_source_set(
        tab.settings,
        HF_NET_SOURCE_SETS_KEY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Field Nets",
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
    )
    tab.settings.set(SELECTED_HF_DAILY_SOURCE_SET_KEY, daily["id"])
    tab.settings.set(SELECTED_HF_NET_SOURCE_SET_KEY, net["id"])

    tab._refresh_source_set_controls()
    tab.rebuild_table()
    app.processEvents()

    text = tab.plan_layers_label.text()
    assert "Daily: Field Daily" in text
    assert "Nets: Field Nets" in text
    assert "SOP: Not included" in text
    assert "View: Effective Windows" in text
    assert "Windows:" in text


def test_freqplanner_delete_plan_removes_unassigned_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Delete Me",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Yes)

    tab._on_delete_plan_clicked()
    app.processEvents()

    assert all(int(row["id"]) != int(saved["id"]) for row in tab.plan_context_service.store.list_frequency_plans())
    assert "Deleted Frequency Plan 'Delete Me'." in tab.frequency_plan_action_hint_label.text()


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
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)
    tab.frequency_plan_combo.setEditText("County Operational Day")

    tab._on_save_sop_plan_clicked()
    app.processEvents()

    plans = tab.plan_context_service.store.list_frequency_plans()
    saved = next(row for row in plans if row["name"] == "County Operational Day")
    refs = json.loads(str(saved["schedule_refs_json"]))
    assert saved["category"] == "sop_schedule"
    assert {row["source"] for row in refs} == {"SOP", "NET_RESOURCE"}
    assert any(row.get("resource_id") == 314 for row in refs)
    assert json.loads(str(saved["source_refs_json"])) == ["net_resources", "sop", "sop_schedule_layer:7"]
    assert "Saved SOP Schedule Plan 'County Operational Day'" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_save_sop_plan_does_not_overwrite_selected_normal_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    normal_plan = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Normal Field Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                }
            ],
        }
    )
    monkeypatch.setattr(
        tab,
        "_build_operational_projection",
        lambda: planner_mod.OperationalDayProjection(
            dt.date(2026, 8, 9),
            (
                OperationalLane(
                    lane_key="group:OPS",
                    lane_label="OPS",
                    group_name="OPS",
                    entries=(
                        OperationalEntry(
                            source="SOP",
                            lane_key="group:OPS",
                            lane_label="OPS",
                            day_utc="Monday",
                            start_utc="02:00",
                            end_utc="03:00",
                            band="20M",
                            frequency="14.078",
                            mode="Digi",
                            group_name="OPS",
                            action_label="Monitor NCS",
                        ),
                    ),
                ),
            ),
            (),
            {"SOP": 1},
        ),
    )
    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(normal_plan["id"])))
    app.processEvents()

    tab._on_save_sop_plan_clicked()
    app.processEvents()

    unchanged = tab.plan_context_service.store.get_frequency_plan(int(normal_plan["id"]))
    assert unchanged["category"] == "normal"
    assert unchanged["name"] == "Normal Field Plan"
    sop_plans = [
        row
        for row in tab.plan_context_service.store.list_frequency_plans()
        if str(row.get("category") or "") == "sop_schedule"
    ]
    assert len(sop_plans) == 1
    assert sop_plans[0]["id"] != normal_plan["id"]
    assert str(sop_plans[0]["name"]).startswith("SOP Schedule Plan ")


def test_freqplanner_save_normal_plan_does_not_overwrite_selected_sop_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    sop_plan = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "County SOP Day",
            "status": "saved",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "COUNTY",
                    "action_label": "Monitor NCS",
                }
            ],
        }
    )
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
    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(sop_plan["id"])))
    app.processEvents()

    tab._on_save_plan_clicked()
    app.processEvents()

    unchanged = tab.plan_context_service.store.get_frequency_plan(int(sop_plan["id"]))
    assert unchanged["category"] == "sop_schedule"
    assert unchanged["name"] == "County SOP Day"
    normal_plans = [
        row
        for row in tab.plan_context_service.store.list_frequency_plans()
        if str(row.get("category") or "") == "normal"
    ]
    assert len(normal_plans) == 1
    assert normal_plans[0]["id"] != sop_plan["id"]
    assert str(normal_plans[0]["name"]).startswith("HF Daily Plan ")


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
    summary = tab._rf_guard_validation_summary_text(
        validation,
        schedule_count=2,
        plan_kind="SOP Schedule Plan",
        plan_payload=payload,
    )
    assert "Radio lanes reviewed: 2 radio lanes: Twenty Meter Radio, Forty Meter Radio." in summary


def test_freqplanner_review_rf_guard_reports_blocked_issues(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    assert tab.review_rf_guard_btn.text() == "Review RF Guard"
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
            "state": "blocked",
            "rf_guard_validation": "enforced",
            "blocked": ["FIO-A and FIO-B would both be assigned on 40M."],
            "messages": ["FIO-A and FIO-B would both be assigned on 40M."],
        },
    )

    tab._on_review_rf_guard_clicked()
    app.processEvents()

    text = tab.frequency_plan_action_hint_label.text()
    assert "RF Guard blocked this Frequency Plan" in text
    assert "Resolution Checklist - Blocked:" in text
    assert "1. Issue: FIO-A and FIO-B would both be assigned on 40M." in text
    assert "Impact: Two transmit-capable radios may operate in the same protected band/window." in text
    assert "Next: Open Settings > Radios and separate the assignments" in text
    assert tab.resolve_rf_guard_btn.isEnabled()
    assert tab.resolve_rf_guard_btn.text() == "Resolve RF Guard"
    assert not tab.rf_guard_review_card.isHidden()
    assert tab.rf_guard_review_table.rowCount() == 1
    assert tab.rf_guard_review_table.item(0, 0).text() == "Blocked"
    assert "both be assigned on 40M" in tab.rf_guard_review_table.item(0, 1).text()
    assert "Two transmit-capable radios" in tab.rf_guard_review_table.item(0, 2).text()

    tab.rf_guard_review_table.selectRow(0)
    app.processEvents()
    selected_text = tab.frequency_plan_action_hint_label.text()
    assert selected_text.startswith("Blocked: FIO-A and FIO-B would both be assigned on 40M.")
    assert "Double-click the issue or use Resolve RF Guard." in selected_text


def test_freqplanner_selecting_assigned_plan_surfaces_rf_guard_card(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    store = tab.plan_context_service.store
    radio = store.save_device_profile(
        {
            "name": "FIO-A",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "antenna_band_guard_mode": "warn",
        }
    )
    plan = store.save_frequency_plan(
        {
            "name": "Assigned Conflict Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "20M",
                    "frequency": "14.115",
                    "group_name": "MAGNET",
                }
            ],
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))
    tab._refresh_plan_workspace_header()

    idx = tab.frequency_plan_combo.findData(int(plan["id"]))
    assert idx >= 0
    tab.frequency_plan_combo.setCurrentIndex(idx)
    app.processEvents()

    assert not tab.rf_guard_review_card.isHidden()
    assert tab.rf_guard_review_table.rowCount() == 1
    assert tab.rf_guard_review_table.item(0, 0).text() == "Warning"
    assert "antenna support does not include 20M" in tab.rf_guard_review_table.item(0, 1).text()
    assert tab.resolve_rf_guard_btn.isEnabled()


def test_freqplanner_review_rf_guard_reports_warning_resolution_checklist(monkeypatch, tmp_path) -> None:
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
                    "band": "20M",
                    "frequency": "14.078",
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
            "state": "warning",
            "rf_guard_validation": "enforced",
            "warnings": ["FIO-A antenna support does not include 20M."],
            "messages": ["FIO-A antenna support does not include 20M."],
        },
    )

    tab._on_review_rf_guard_clicked()
    app.processEvents()

    text = tab.frequency_plan_action_hint_label.text()
    assert "RF Guard found warning(s) for this Frequency Plan" in text
    assert "Resolution Checklist - Warnings:" in text
    assert "1. Issue: FIO-A antenna support does not include 20M." in text
    assert "Impact: The selected radio may not be safe or useful on the planned band." in text
    assert "Next: Open Settings > Radios and adjust the radio antenna bands" in text
    assert tab.resolve_rf_guard_btn.isEnabled()
    assert not tab.rf_guard_review_card.isHidden()
    assert tab.rf_guard_review_table.rowCount() == 1
    assert tab.rf_guard_review_table.item(0, 0).text() == "Warning"
    assert "antenna support does not include 20M" in tab.rf_guard_review_table.item(0, 1).text()


def test_freqplanner_review_rf_guard_reports_assignment_checks_when_no_radio_context(monkeypatch, tmp_path) -> None:
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
            "state": "not_enforced",
            "rf_guard_validation": "not_enforced",
            "messages": ["RF Guard preflight skipped because no radio context is selected."],
        },
    )

    tab._on_review_rf_guard_clicked()
    app.processEvents()

    text = tab.frequency_plan_action_hint_label.text()
    assert "Assignment checks are still required in Settings" in text
    assert "RF Guard preflight skipped because no radio context is selected." in text
    assert not tab.resolve_rf_guard_btn.isEnabled()
    assert tab.rf_guard_review_card.isHidden()


def test_freqplanner_resolve_rf_guard_opens_schedule_assignment(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()

    class WindowStub:
        def __init__(self) -> None:
            self.calls = []
            self.settings_tab = None

        def open_settings_section(self, section, **kwargs):
            self.calls.append((section, kwargs))

    window = WindowStub()
    monkeypatch.setattr(tab, "window", lambda: window)
    tab.frequency_plan_combo.setEditText("Field Day Plan")

    tab._set_rf_guard_resolution_available(True)
    tab._on_resolve_rf_guard_clicked()
    app.processEvents()

    assert window.calls == [("schedule_assignments", {"settings_nav_context": "radios"})]
    assert "Field Day Plan" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_assign_plan_opens_settings_with_assignment_language(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Clean Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))

    class WindowStub:
        def __init__(self) -> None:
            self.calls = []
            self.editor_calls = []
            self.settings_tab = self

        def open_settings_section(self, section, **kwargs):
            self.calls.append((section, kwargs))

        def open_schedule_assignment_editor(self, **kwargs):
            self.editor_calls.append(kwargs)
            return True

    window = WindowStub()
    monkeypatch.setattr(tab, "window", lambda: window)

    tab._on_assign_plan_clicked()
    app.processEvents()

    assert window.calls == [("schedule_assignments", {"settings_nav_context": "radios"})]
    assert window.editor_calls == [{"plan_id": int(saved["id"]), "device_profile_id": 0}]
    assert "Choose the radio and save with RF Guard." in tab.frequency_plan_action_hint_label.text()
    assert "resolve RF Guard issues" not in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_guided_radio_handoff_assigns_target_radio(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Portable JS8 Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [],
        }
    )
    tab.begin_guided_radio_plan_handoff({"id": 42, "name": "Portable SDR"})
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))

    class WindowStub:
        def __init__(self) -> None:
            self.calls = []
            self.editor_calls = []
            self.settings_tab = self

        def open_settings_section(self, section, **kwargs):
            self.calls.append((section, kwargs))

        def open_schedule_assignment_editor(self, **kwargs):
            self.editor_calls.append(kwargs)
            return True

    window = WindowStub()
    monkeypatch.setattr(tab, "window", lambda: window)

    tab._on_assign_plan_clicked()
    app.processEvents()

    assert window.calls == [("schedule_assignments", {"settings_nav_context": "radios"})]
    assert window.editor_calls == [{"plan_id": int(saved["id"]), "device_profile_id": 42}]
    assert "Portable JS8 Plan" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_selecting_assigned_plan_switches_command_radio(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    store = tab.plan_context_service.store
    radio = store.save_device_profile(
        {
            "name": "FIO-B (HF)",
            "enabled": 1,
            "runtime_active": 1,
            "runtime_primary": 0,
            "device_class": "tx_rx",
            "control_backend": "manual",
        }
    )
    plan = store.save_frequency_plan(
        {
            "name": "Assigned Daily Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Monday",
                    "start_utc": "16:00",
                    "end_utc": "17:00",
                    "group_name": "MAGNET",
                    "band": "40M",
                    "frequency": "7.115",
                }
            ],
            "frequency_refs": ["40M:7.115"],
            "group_refs": ["MAGNET"],
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    class WindowStub:
        def __init__(self) -> None:
            self.activated = []

        def _activate_station_command_radio(self, radio_id):
            self.activated.append(int(radio_id))
            return True

    window = WindowStub()
    monkeypatch.setattr(tab, "window", lambda: window)
    tab._refresh_plan_workspace_header()
    idx = tab.frequency_plan_combo.findData(int(plan["id"]))
    assert idx >= 0

    tab.frequency_plan_combo.setCurrentIndex(idx)
    tab._on_frequency_plan_selected()
    app.processEvents()

    assert window.activated == [int(radio["id"])]
    assert "command bar switched to that radio" in tab.frequency_plan_action_hint_label.text()
    assert tab.assign_plan_btn.text() == "Assigned in Settings"
    assert "assigned to FIO-B (HF)" in tab.assign_plan_btn.toolTip()


def test_freqplanner_build_sop_layer_opens_sop_builder_without_selected_cell(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()

    class WindowStub:
        def __init__(self) -> None:
            self.calls = []
            self._screens = [("SOP", object())]

        def _set_screen(self, index):
            self.calls.append(self._screens[int(index)][0])

    window = WindowStub()
    monkeypatch.setattr(tab, "window", lambda: window)

    tab._on_build_sop_layer_clicked()
    app.processEvents()

    assert window.calls == ["SOP"]
    assert "condition-based what-to-do layers" in tab.frequency_plan_action_hint_label.text()


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
    assert tab._source_selection_summary() == "HF Daily: Exercise Daily; HF Nets: County Nets; SOP: Active SOP Builder layers"


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
    tab.schedule_source_combo = QComboBox()
    tab.schedule_source_combo.setEditable(True)
    tab.schedule_source_combo.addItem("Active Daily Schedule", LIVE_SOURCE_SET_ID)
    tab.schedule_source_combo.setEditText("Button Daily")
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
    assert any("Select it in Plan Manager" in str(args[-1]) for args in messages)


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
    tab.schedule_source_combo = QComboBox()
    tab.schedule_source_combo.setEditable(True)
    tab.schedule_source_combo.addItem("Active Net Schedule", LIVE_SOURCE_SET_ID)
    tab.schedule_source_combo.setEditText("Button Nets")
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
    assert any("Select it in Plan Manager" in str(args[-1]) for args in messages)


def test_hf_daily_tab_renames_existing_schedule_from_editable_combo(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.daily_schedule_tab as daily_mod

    daily_mod = importlib.reload(daily_mod)
    messages = []
    monkeypatch.setattr(daily_mod.QMessageBox, "information", lambda *args: messages.append(args))
    saved = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Old Daily",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "group_name": "OPS"}],
    )
    tab = daily_mod.DailyScheduleTab.__new__(daily_mod.DailyScheduleTab)
    tab.settings = settings
    tab.schedule_source_combo = QComboBox()
    tab.schedule_source_combo.setEditable(True)
    tab.schedule_source_combo.addItem("Old Daily", saved["id"])
    tab.schedule_source_combo.setCurrentIndex(0)
    tab._editing_freqplanner_source_id = saved["id"]
    tab._source_rows_for_freqplanner_snapshot = lambda: [
        {"day_utc": "Monday", "start_utc": "03:00", "end_utc": "04:00", "band": "80M", "group_name": "OPS"}
    ]
    tab._confirm_rf_guard_source_update = lambda *args, **kwargs: True
    tab._refresh_freqplanner_source_combo = lambda: None
    tab._refresh_schedule_resources = lambda **kwargs: None
    tab._refresh_freq_planner = lambda: None
    tab.schedule_source_combo.setEditText("Renamed Daily")

    daily_mod.DailyScheduleTab._on_save_freqplanner_source_clicked(tab)
    app.processEvents()

    plans = [
        row
        for row in MultiRadioStore().list_frequency_plans()
        if str(row.get("category") or "") == "hf_daily_schedule"
    ]
    assert len(plans) == 1
    assert int(plans[0]["id"]) == int(saved["db_id"])
    assert plans[0]["name"] == "Renamed Daily"
    assert any("Updated 'Renamed Daily'" in str(args[-1]) for args in messages)


def test_hf_daily_rename_action_does_not_duplicate_or_update_rows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    saved = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Old Daily",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "frequency": "7.078", "group_name": "OPS"}],
    )

    renamed = rename_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        saved["id"],
        "New Daily",
    )

    plans = [
        row
        for row in MultiRadioStore().list_frequency_plans()
        if str(row.get("category") or "") == HF_DAILY_SOURCE_CATEGORY
    ]
    assert len(plans) == 1
    assert int(plans[0]["id"]) == int(saved["db_id"])
    assert renamed["name"] == "New Daily"
    refs = json.loads(plans[0]["schedule_refs_json"])
    assert refs[0]["day_utc"] == "Monday"
    assert refs[0]["start_utc"] == "01:00"
    assert refs[0]["end_utc"] == "02:00"
    assert refs[0]["band"] == "40M"
    assert refs[0]["frequency"] == "7.078"


def test_hf_daily_new_schedule_action_detaches_saved_selection(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.daily_schedule_tab as daily_mod

    daily_mod = importlib.reload(daily_mod)
    saved = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Saved Daily",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "frequency": "7.078", "group_name": "OPS"}],
    )
    tab = daily_mod.DailyScheduleTab.__new__(daily_mod.DailyScheduleTab)
    tab.settings = settings
    tab.schedule_source_combo = QComboBox()
    tab.schedule_source_combo.setEditable(True)
    tab.schedule_source_combo.addItem("Active Daily Schedule", LIVE_SOURCE_SET_ID)
    tab.schedule_source_combo.addItem("Saved Daily", saved["id"])
    tab.schedule_source_combo.setCurrentIndex(1)
    tab._editing_freqplanner_source_id = saved["id"]
    tab._refresh_freq_planner = lambda: None

    daily_mod.DailyScheduleTab._on_new_freqplanner_source_clicked(tab)
    app.processEvents()

    assert tab._editing_freqplanner_source_id == LIVE_SOURCE_SET_ID
    assert settings.get(SELECTED_HF_DAILY_SOURCE_SET_KEY) == LIVE_SOURCE_SET_ID
    assert tab.schedule_source_combo.currentText() == ""
    assert daily_mod.DailyScheduleTab._selected_freqplanner_source_row(tab) is None


def test_hf_daily_delete_source_schedule_removes_full_saved_schedule(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    saved = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Delete Me",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "frequency": "7.078", "group_name": "OPS"}],
    )

    assert delete_source_schedule(
        settings,
        HF_DAILY_SOURCE_SETS_KEY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        saved["id"],
    )
    assert MultiRadioStore().get_frequency_plan(int(saved["db_id"])) is None
    assert settings.get(SELECTED_HF_DAILY_SOURCE_SET_KEY) == LIVE_SOURCE_SET_ID


def test_hf_daily_compact_time_entry_normalizes_like_colon_time(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    import freqinout.gui.daily_schedule_tab as daily_mod

    daily_mod = importlib.reload(daily_mod)
    tab = daily_mod.DailyScheduleTab.__new__(daily_mod.DailyScheduleTab)

    assert daily_mod.DailyScheduleTab._normalize_hhmm(tab, "1600") == "16:00"
    assert daily_mod.DailyScheduleTab._normalize_hhmm(tab, "16:00") == "16:00"
    assert daily_mod.DailyScheduleTab._normalize_hhmm(tab, "930") == "09:30"
    assert daily_mod.DailyScheduleTab._validate_time(tab, "1600") is True
    assert daily_mod.DailyScheduleTab._validate_time(tab, "2460") is False


def test_hf_net_tab_renames_existing_schedule_from_editable_combo(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    messages = []
    monkeypatch.setattr(net_mod.QMessageBox, "information", lambda *args: messages.append(args))
    saved = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Old Nets",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "group_name": "OPS",
                "net_name": "Old Net",
            }
        ],
    )
    tab = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    tab.settings = settings
    tab.schedule_source_combo = QComboBox()
    tab.schedule_source_combo.setEditable(True)
    tab.schedule_source_combo.addItem("Old Nets", saved["id"])
    tab.schedule_source_combo.setCurrentIndex(0)
    tab._editing_freqplanner_source_id = saved["id"]
    tab._source_rows_for_freqplanner_snapshot = lambda: [
        {
            "day_utc": "Monday",
            "start_utc": "03:00",
            "end_utc": "04:00",
            "band": "80M",
            "group_name": "OPS",
            "net_name": "Renamed Net",
        }
    ]
    tab._confirm_rf_guard_source_update = lambda *args, **kwargs: True
    tab._refresh_freqplanner_source_combo = lambda: None
    tab._refresh_freq_planner = lambda: None
    tab.schedule_source_combo.setEditText("Renamed Nets")

    net_mod.NetScheduleTab._on_save_freqplanner_source_clicked(tab)
    app.processEvents()

    plans = [
        row
        for row in MultiRadioStore().list_frequency_plans()
        if str(row.get("category") or "") == "hf_net_schedule"
    ]
    assert len(plans) == 1
    assert int(plans[0]["id"]) == int(saved["db_id"])
    assert plans[0]["name"] == "Renamed Nets"
    assert any("Updated 'Renamed Nets'" in str(args[-1]) for args in messages)


def test_hf_net_rename_action_does_not_duplicate_or_update_rows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    saved = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Old Nets",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "OPS",
                "net_name": "Ops Net",
            }
        ],
    )

    renamed = rename_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        saved["id"],
        "New Nets",
    )

    plans = [
        row
        for row in MultiRadioStore().list_frequency_plans()
        if str(row.get("category") or "") == HF_NET_SOURCE_CATEGORY
    ]
    assert len(plans) == 1
    assert int(plans[0]["id"]) == int(saved["db_id"])
    assert renamed["name"] == "New Nets"
    refs = json.loads(plans[0]["schedule_refs_json"])
    assert refs[0]["day_utc"] == "Monday"
    assert refs[0]["start_utc"] == "01:00"
    assert refs[0]["end_utc"] == "02:00"
    assert refs[0]["band"] == "40M"
    assert refs[0]["net_name"] == "Ops Net"


def test_hf_net_selecting_saved_schedule_loads_rows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    saved = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Load Nets",
        [
            {
                "day_utc": "Tuesday",
                "start_utc": "03:00",
                "end_utc": "04:00",
                "band": "80M",
                "frequency": "3.590",
                "group_name": "OPS",
                "net_name": "Loaded Net",
            }
        ],
    )
    loaded = []
    tab = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    tab.settings = settings
    tab.schedule_source_combo = QComboBox()
    tab.schedule_source_combo.addItem("Active Net Schedule", LIVE_SOURCE_SET_ID)
    tab.schedule_source_combo.addItem("Load Nets", saved["id"])
    tab._editing_freqplanner_source_id = LIVE_SOURCE_SET_ID
    tab._dirty = False
    tab._update_source_action_state = lambda: None
    tab._load_source_rows_into_table = lambda rows: loaded.extend(rows)
    tab._load = lambda: loaded.append({"loaded": "active"})
    tab._refresh_freq_planner = lambda: None

    tab.schedule_source_combo.setCurrentIndex(1)
    net_mod.NetScheduleTab._on_freqplanner_source_selected(tab)
    app.processEvents()

    assert settings.get(SELECTED_HF_NET_SOURCE_SET_KEY) == saved["id"]
    assert loaded and loaded[0]["net_name"] == "Loaded Net"


def test_hf_net_new_schedule_action_detaches_saved_selection(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    saved = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Saved Nets",
        [{"day_utc": "Monday", "start_utc": "01:00", "end_utc": "02:00", "band": "40M", "frequency": "7.110", "group_name": "OPS", "net_name": "Ops Net"}],
    )
    tab = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    tab.settings = settings
    tab.schedule_source_combo = QComboBox()
    tab.schedule_source_combo.setEditable(True)
    tab.schedule_source_combo.addItem("Active Net Schedule", LIVE_SOURCE_SET_ID)
    tab.schedule_source_combo.addItem("Saved Nets", saved["id"])
    tab.schedule_source_combo.setCurrentIndex(1)
    tab._editing_freqplanner_source_id = saved["id"]
    tab._refresh_freq_planner = lambda: None

    net_mod.NetScheduleTab._on_new_freqplanner_source_clicked(tab)
    app.processEvents()

    assert tab._editing_freqplanner_source_id == LIVE_SOURCE_SET_ID
    assert settings.get(SELECTED_HF_NET_SOURCE_SET_KEY) == LIVE_SOURCE_SET_ID
    assert tab.schedule_source_combo.currentText() == ""
    assert net_mod.NetScheduleTab._selected_freqplanner_source_row(tab) is None


def test_hf_net_save_selected_as_resources_does_not_remove_schedule_rows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    messages = []
    monkeypatch.setattr(net_mod.QMessageBox, "information", lambda *args: messages.append(args))
    monkeypatch.setattr(net_mod.QMessageBox, "critical", lambda *args: messages.append(args))
    tab = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    tab.settings = settings
    tab.resource_set_combo = QComboBox()
    tab.resource_set_combo.addItem("Training", "Training")
    tab.resource_set_combo.setCurrentIndex(0)
    tab.table = type(
        "DummyTable",
        (),
        {
            "remove_calls": [],
            "rowCount": lambda self: 1,
            "cellWidget": lambda self, *_args: None,
            "removeRow": lambda self, row: self.remove_calls.append(row),
        },
    )()
    row = {
        "day_utc": "Monday",
        "recurrence": "Weekly",
        "biweekly_offset_weeks": 0,
        "month_weeks": "",
        "group_name": "OPS",
        "band": "80M",
        "mode": "JS8",
        "frequency": "3.590",
        "start_utc": "02:00",
        "end_utc": "03:00",
        "early_checkin": 0,
        "primary_js8call_group": "OPS",
        "coverage": "",
        "comment": "",
        "net_name": "Ops Net",
        "fldigi_mode": "",
        "fldigi_offset": "",
    }
    tab._checked_schedule_row_indexes = lambda: [0]
    tab._collect_rows_by_ui_index = lambda: {0: row}
    tab._load_resources_from_db = lambda: None
    tab._refresh_resource_set_combo = lambda: None
    tab._refresh_resources_table = lambda: None
    tab._update_delete_button_state = lambda: None

    net_mod.NetScheduleTab._save_selected_schedule_rows_as_resources(tab)
    app.processEvents()

    assert tab.table.remove_calls == []
    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        saved = conn.execute(
            "SELECT resource_set, source_ref, group_name, net_name FROM net_resources"
        ).fetchone()
    finally:
        conn.close()
    assert saved == ("Training", "saved_from_schedule", "OPS", "Ops Net")
    assert any("HF Net schedule was not changed" in str(args[-1]) for args in messages)


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
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)
    tab.frequency_plan_combo.setEditText("Assigned Blend")

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


def test_saved_daily_update_reprojects_dependent_frequency_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    daily = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "AmRRON Daily",
        [
            {
                "day_utc": "Sunday",
                "start_utc": "00:00",
                "end_utc": "12:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "AMRRON",
            }
        ],
    )
    plan = store.save_frequency_plan(
        {
            "name": "AmRRON Plan",
            "source_refs": ["hf_daily", source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"])],
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Sunday",
                    "start_utc": "00:00",
                    "end_utc": "12:00",
                    "band": "40M",
                    "frequency": "7.110",
                    "group_name": "AMRRON",
                }
            ],
            "frequency_refs": ["40M:7.110"],
            "group_refs": ["AMRRON"],
        }
    )

    updated_rows = [
        {
            "day_utc": "Sunday",
            "start_utc": "00:00",
            "end_utc": "12:00",
            "band": "20M",
            "frequency": "14.110",
            "group_name": "AMRRON",
        }
    ]
    save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "AmRRON Daily",
        updated_rows,
        existing_plan_id=int(daily["db_id"]),
    )

    refreshed = reproject_frequency_plans_for_source_update(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        daily["id"],
        updated_rows,
    )

    assert [row["name"] for row in refreshed] == ["AmRRON Plan"]
    saved_plan = store.get_frequency_plan(int(plan["id"]))
    refs = json.loads(str(saved_plan["schedule_refs_json"]))
    assert [(row["group_name"], row["band"], row["frequency"]) for row in refs] == [("AMRRON", "20M", "14.110")]
    assert json.loads(str(saved_plan["frequency_refs_json"])) == ["20M:14.110"]
    updated_utc = str(saved_plan["updated_utc"])
    assert (
        reproject_frequency_plans_for_source_update(
            settings,
            HF_DAILY_SOURCE_CATEGORY,
            daily["id"],
            updated_rows,
        )
        == []
    )
    assert str(store.get_frequency_plan(int(plan["id"]))["updated_utc"]) == updated_utc


def test_refresh_source_backed_frequency_plans_repairs_stale_saved_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    daily = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Current Daily",
        [
            {
                "day_utc": "Sunday",
                "start_utc": "00:00",
                "end_utc": "12:00",
                "band": "20M",
                "frequency": "14.110",
                "group_name": "AMRRON",
            }
        ],
    )
    plan = store.save_frequency_plan(
        {
            "name": "Stale Assigned Plan",
            "source_refs": ["hf_daily", source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"])],
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Sunday",
                    "start_utc": "00:00",
                    "end_utc": "12:00",
                    "band": "40M",
                    "frequency": "7.110",
                    "group_name": "AMRRON",
                }
            ],
            "frequency_refs": ["40M:7.110"],
            "group_refs": ["AMRRON"],
        }
    )

    refreshed = refresh_source_backed_frequency_plans(settings)

    assert [row["name"] for row in refreshed] == ["Stale Assigned Plan"]
    saved_plan = store.get_frequency_plan(int(plan["id"]))
    refs = json.loads(str(saved_plan["schedule_refs_json"]))
    assert [(row["band"], row["frequency"]) for row in refs] == [("20M", "14.110")]

    updated_utc = str(saved_plan["updated_utc"])
    assert refresh_source_backed_frequency_plans(settings) == []
    assert str(store.get_frequency_plan(int(plan["id"]))["updated_utc"]) == updated_utc


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


def test_saved_sop_layer_update_reports_rf_guard_impacts_for_assigned_master_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "SOP Guard Radio",
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
            "name": "Assigned SOP Master",
            "source_refs": ["sop", "sop_schedule_layer:7"],
            "schedule_refs": [
                {
                    "source": "SOP",
                    "sop_profile_id": 7,
                    "sop_layer_id": 11,
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "COUNTY",
                }
            ],
            "frequency_refs": ["20M:14.078"],
            "group_refs": ["COUNTY"],
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    impacts = assigned_plan_rf_guard_impacts_for_sop_update(
        7,
        [
            {
                "sop_profile_id": 7,
                "sop_layer_id": 11,
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "COUNTY",
            }
        ],
    )

    assert len(impacts) == 1
    assert impacts[0]["plan"]["name"] == "Assigned SOP Master"
    assert impacts[0]["device"]["name"] == "SOP Guard Radio"
    assert impacts[0]["validation"]["state"] == "blocked"
    assert "antenna support does not include 40M" in " ".join(impacts[0]["validation"]["messages"])


def test_saved_sop_layer_removal_reprojects_baseline_before_rf_guard_check(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    settings = SettingsManager()
    settings.set(
        HF_DAILY_SOURCE_SETS_KEY,
        [
            {
                "id": "county-daily",
                "name": "County Daily",
                "rows": [
                    {
                        "day_utc": "ALL",
                        "start_utc": "00:00",
                        "end_utc": "01:00",
                        "band": "40M",
                        "frequency": "7.078",
                        "group_name": "COUNTY",
                    }
                ],
            }
        ],
    )
    settings.save()

    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "SOP Removal Guard Radio",
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
            "name": "Assigned SOP Removal Master",
            "source_refs": ["hf_daily_schedule:county-daily", "sop", "sop_schedule_layer:7"],
            "schedule_refs": [
                {
                    "source": "SOP",
                    "sop_profile_id": 7,
                    "day_utc": "ALL",
                    "start_utc": "00:00",
                    "end_utc": "01:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "COUNTY",
                }
            ],
            "frequency_refs": ["20M:14.078"],
            "group_refs": ["COUNTY"],
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    impacts = assigned_plan_rf_guard_impacts_for_sop_update(7, [])

    assert len(impacts) == 1
    assert impacts[0]["plan"]["name"] == "Assigned SOP Removal Master"
    assert impacts[0]["device"]["name"] == "SOP Removal Guard Radio"
    assert impacts[0]["validation"]["state"] == "blocked"
    assert "antenna support does not include 40M" in " ".join(impacts[0]["validation"]["messages"])


def test_sop_builder_update_confirmation_blocks_assigned_master_plan_rf_guard_conflict(
    monkeypatch, tmp_path
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "SOP UI Guard Radio",
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
            "name": "Assigned SOP UI Master",
            "source_refs": ["sop", "sop_schedule_layer:7"],
            "schedule_refs": [
                {
                    "source": "SOP",
                    "sop_profile_id": 7,
                    "day_utc": "ALL",
                    "start_utc": "00:00",
                    "end_utc": "01:00",
                    "band": "20M",
                    "frequency": "14.078",
                    "group_name": "MAGNET",
                }
            ],
            "frequency_refs": ["20M:14.078"],
            "group_refs": ["MAGNET"],
        }
    )
    store.set_assigned_plan(int(radio["id"]), int(plan["id"]))

    sop_mod = importlib.import_module("freqinout.gui.sop_tab")
    tab = sop_mod.SOPTab.__new__(sop_mod.SOPTab)
    tab.manager = SOPManager()

    warnings: list[tuple[str, str]] = []
    monkeypatch.setattr(
        sop_mod.QMessageBox,
        "warning",
        lambda _parent, title, text: warnings.append((str(title), str(text))),
    )

    allowed = tab._confirm_rf_guard_sop_update(
        7,
        "County SOP",
        [
            {
                "source": "SOP",
                "sop_profile_id": 7,
                "day_utc": "ALL",
                "start_utc": "00:00",
                "end_utc": "01:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "MAGNET",
            }
        ],
    )

    assert allowed is False
    assert warnings
    assert warnings[0][0] == "RF Guard Blocked Update"
    assert "SOP UI Guard Radio / Assigned SOP UI Master" in warnings[0][1]


def test_sop_builder_save_runs_rf_guard_confirmation_before_profile_save() -> None:
    sop_mod = importlib.import_module("freqinout.gui.sop_tab")
    source = inspect.getsource(sop_mod.SOPTab._save_profile)

    assert "_confirm_rf_guard_sop_update" in source
    assert source.index("_confirm_rf_guard_sop_update") < source.index("manager.save_profile")


def test_sop_builder_clear_runs_rf_guard_confirmation_before_profile_save() -> None:
    sop_mod = importlib.import_module("freqinout.gui.sop_tab")
    source = inspect.getsource(sop_mod.SOPTab._delete_profile)

    assert "_confirm_rf_guard_sop_update" in source
    assert source.index("_confirm_rf_guard_sop_update") < source.index("manager.save_profile")


def test_blended_projection_source_refs_dedup_sop_dependency_refs() -> None:
    projection = build_blended_schedule_projection(
        [],
        [],
        [
            {
                "sop_profile_id": 7,
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "20M",
                "frequency": "14.078",
            },
            {
                "sop_profile_id": 7,
                "day_utc": "Monday",
                "start_utc": "03:00",
                "end_utc": "04:00",
                "band": "40M",
                "frequency": "7.078",
            },
        ],
        [],
        week_start_utc=dt.date(2026, 8, 2),
    )

    assert projection.source_refs() == ["sop", "sop_schedule_layer:7"]


def test_hf_daily_update_blocks_before_saving_when_assigned_plan_rf_guard_conflicts(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    radio = store.save_device_profile(
        {
            "name": "Daily Guard Radio",
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
    master = store.save_frequency_plan(
        {
            "name": "Assigned Daily Master",
            "source_refs": ["hf_daily", source_schedule_dependency_ref(HF_DAILY_SOURCE_CATEGORY, daily["id"])],
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
    store.set_assigned_plan(int(radio["id"]), int(master["id"]))

    import freqinout.gui.daily_schedule_tab as daily_mod

    daily_mod = importlib.reload(daily_mod)
    warnings: list[tuple] = []
    monkeypatch.setattr(daily_mod.QMessageBox, "warning", lambda *args: warnings.append(args))
    monkeypatch.setattr(daily_mod.QMessageBox, "information", lambda *args: warnings.append(args))
    monkeypatch.setattr(daily_mod.QMessageBox, "critical", lambda *args: warnings.append(args))
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
    tab._selected_freqplanner_source_row = lambda: {"id": daily["id"], "db_id": daily["db_id"], "name": daily["name"]}

    daily_mod.DailyScheduleTab._on_save_freqplanner_source_clicked(tab)
    app.processEvents()

    saved = MultiRadioStore().get_frequency_plan(int(daily["db_id"]))
    refs = json.loads(str(saved["schedule_refs_json"]))
    assert refs[0]["band"] == "20M"
    assert warnings
    assert warnings[0][1] == "RF Guard Blocked Update"
    assert "Daily Guard Radio / Assigned Daily Master" in str(warnings[0][2])


def test_hf_net_update_blocks_before_saving_when_assigned_plan_rf_guard_conflicts(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
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
    master = store.save_frequency_plan(
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
    store.set_assigned_plan(int(radio["id"]), int(master["id"]))

    import freqinout.gui.net_schedule_tab as net_mod

    net_mod = importlib.reload(net_mod)
    warnings: list[tuple] = []
    monkeypatch.setattr(net_mod.QMessageBox, "warning", lambda *args: warnings.append(args))
    monkeypatch.setattr(net_mod.QMessageBox, "information", lambda *args: warnings.append(args))
    monkeypatch.setattr(net_mod.QMessageBox, "critical", lambda *args: warnings.append(args))
    tab = net_mod.NetScheduleTab.__new__(net_mod.NetScheduleTab)
    tab.settings = settings
    tab._source_rows_for_freqplanner_snapshot = lambda: [
        {
            "day_utc": "Monday",
            "start_utc": "01:00",
            "end_utc": "02:00",
            "band": "40M",
            "frequency": "7.078",
            "group_name": "OPS",
            "net_name": "Unsafe Net",
        }
    ]
    tab._selected_freqplanner_source_row = lambda: {"id": net["id"], "db_id": net["db_id"], "name": net["name"]}

    net_mod.NetScheduleTab._on_save_freqplanner_source_clicked(tab)
    app.processEvents()

    saved = MultiRadioStore().get_frequency_plan(int(net["db_id"]))
    refs = json.loads(str(saved["schedule_refs_json"]))
    assert refs[0]["band"] == "20M"
    assert warnings
    assert warnings[0][1] == "RF Guard Blocked Update"
    assert "Net Guard Radio / Assigned Net Master" in str(warnings[0][2])


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
            self.schedule_source_combo.addItem("Active Daily Schedule", LIVE_SOURCE_SET_ID)
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
            self.schedule_source_combo.addItem("Active Net Schedule", LIVE_SOURCE_SET_ID)
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
    radio = tab.plan_context_service.store.save_device_profile(
        {
            "name": "County HF",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
        }
    )
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Saved SOP Day",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "lane_key": f"radio:{int(radio['id'])}",
                    "lane_label": "County HF",
                    "radio_id": int(radio["id"]),
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
    sop_col = headers.index("County HF")

    assert tab.table.item(2, sop_col).text() == "Saved Call 20M 14.078"
    tab._on_schedule_cell_clicked(2, sop_col)
    assert tab.edit_sop_plan_entry_btn.isEnabled()
    assert "saved plan 'Saved SOP Day'" in tab.frequency_plan_action_hint_label.text()
    assert "Radios: 1 radio lane: County HF" in tab.plan_layers_label.text()

    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("effective"))
    app.processEvents()
    assert not tab.edit_sop_plan_entry_btn.isEnabled()
    assert "SOP Lanes" in tab.edit_sop_plan_entry_btn.toolTip()


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


def test_freqplanner_effective_windows_is_default_and_drives_inspector(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab._show_local = False
    assert tab.planner_view_combo.currentData() == "effective"
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

    assert [tab.table.horizontalHeaderItem(col).text() for col in range(tab.table.columnCount())] == [
        "Day (UTC)",
        "Group / Net / SOP",
        "Band",
        "Time (UTC)",
        "Mode",
        "Layer",
        "What It Means",
    ]
    assert tab.table.rowCount() == 2
    assert tab.table.item(0, 1).text() == "OPS"
    assert tab.table.item(0, 2).text() == "40M"
    assert tab.table.item(0, 5).text() == "Daily"
    assert tab.table.item(1, 1).text() == "Night Net"
    assert tab.table.item(1, 5).text() == "Net"
    assert tab.table.item(1, 6).text() == "Net overrides daily window"
    cell = tab.table.item(1, 1).data(Qt.UserRole)
    assert isinstance(cell, planner_mod.EffectiveWindowCell)
    assert cell.segment.source == "NET"
    assert cell.net_segments[0].net_name == "Night Net"

    tab._on_schedule_cell_clicked(1, 1)
    inspector = tab.cell_inspector_label.text()
    assert tab.table.selectionBehavior() == QAbstractItemView.SelectRows
    assert tab.table.selectedItems()
    assert {item.row() for item in tab.table.selectedItems()} == {1}
    assert tab.edit_hf_daily_btn.isEnabled()
    assert tab.edit_hf_net_btn.isEnabled()
    assert not tab.open_sop_builder_btn.isEnabled()
    assert "Effective: Net Night Net" in inspector
    assert "HF Daily: HF 01:00-03:00 OPS 40M 7.078" in inspector
    assert "HF Nets: NET 02:00-03:00 Night Net 80M 3.590" in inspector

    tab._toggle_time_view()
    app.processEvents()
    assert tab.table.horizontalHeaderItem(0).text() == "Day (Local)"
    assert tab.table.horizontalHeaderItem(3).text() == "Time (Local)"
    assert tab.table.item(0, 0).text() != "Monday"

    tab._toggle_band_view()
    app.processEvents()
    assert tab.table.horizontalHeaderItem(2).text() == "Freq"


def test_freqplanner_inline_editor_updates_saved_plan_only(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    saved = tab.plan_context_service.store.save_frequency_plan(
        {
            "name": "Inline Plan",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Monday",
                    "start_utc": "01:00",
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                    "mode": "Digi",
                    "source_key": "HF:test-inline",
                }
            ],
        }
    )
    tab._refresh_plan_workspace_header()
    tab.frequency_plan_combo.setCurrentIndex(tab.frequency_plan_combo.findData(int(saved["id"])))
    monkeypatch.setattr(tab, "_rf_guard_preflight_for_plan", lambda _payload: {"state": "ok", "messages": []})
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)
    segment = planner_mod.ScheduleSegment(
        source="HF",
        day_utc="Monday",
        start_minute=60,
        end_minute=120,
        band="40M",
        frequency="7.078",
        group_name="OPS",
        mode="Digi",
        raw={"source_key": "HF:test-inline"},
    )
    tab._set_projection_inspector(planner_mod.EffectiveWindowCell(segment=segment, hf_segments=(segment,)))
    assert "Plan Only changes this saved Frequency Plan" in tab.inline_editor_impact_label.text()
    assert "Source update is unavailable" in tab.inline_editor_impact_label.text()
    tab.inline_band_edit.setText("20M")
    tab.inline_frequency_edit.setText("14.078")
    tab.inline_start_edit.setText("03:00")
    tab.inline_end_edit.setText("04:00")

    tab._on_inline_update_plan_clicked()
    app.processEvents()

    updated = tab.plan_context_service.store.get_frequency_plan(int(saved["id"]))
    refs = json.loads(str(updated["schedule_refs_json"]))
    assert refs[0]["band"] == "20M"
    assert refs[0]["frequency"] == "14.078"
    assert refs[0]["start_utc"] == "03:00"
    assert "Updated plan-only window" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_inline_editor_updates_single_saved_hf_daily_source(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    saved_source = save_source_schedule(
        settings,
        HF_DAILY_SOURCE_CATEGORY,
        SELECTED_HF_DAILY_SOURCE_SET_KEY,
        "Daily Inline",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
                "mode": "Digi",
            }
        ],
    )
    tab = planner_mod.FreqPlannerTab()
    tab._refresh_source_set_controls()
    monkeypatch.setattr(planner_mod, "assigned_plan_rf_guard_impacts_for_source_update", lambda *args, **kwargs: [])
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)
    segment = planner_mod.ScheduleSegment(
        source="HF",
        day_utc="Monday",
        start_minute=60,
        end_minute=120,
        band="40M",
        frequency="7.078",
        group_name="OPS",
        mode="Digi",
        raw={"start_utc": "01:00", "end_utc": "02:00"},
    )
    tab._set_projection_inspector(planner_mod.EffectiveWindowCell(segment=segment, hf_segments=(segment,)))
    assert tab.inline_update_hf_daily_btn.isEnabled()
    assert tab.inline_update_hf_daily_btn.text() == "Update HF Daily Source"
    assert "Update HF Daily Source changes the named schedule" in tab.inline_editor_impact_label.text()
    assert "RF Guard reviews the impact" in tab.inline_editor_impact_label.text()
    tab.inline_band_edit.setText("80M")
    tab.inline_frequency_edit.setText("3.578")

    tab._on_inline_update_hf_daily_clicked()
    app.processEvents()

    source = planner_mod.source_set_row_by_id_for_category(
        settings,
        HF_DAILY_SOURCE_SETS_KEY,
        HF_DAILY_SOURCE_CATEGORY,
        saved_source["id"],
    )
    assert source is not None
    assert source["rows"][0]["band"] == "80M"
    assert source["rows"][0]["frequency"] == "3.578"
    assert "Updated HF Daily source" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_inline_editor_updates_single_saved_hf_net_source(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    settings = SettingsManager()
    settings.set("operating_groups", [{"group": "OPS", "band": "40M", "frequency": "7.078"}])

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    saved_source = save_source_schedule(
        settings,
        HF_NET_SOURCE_CATEGORY,
        SELECTED_HF_NET_SOURCE_SET_KEY,
        "Net Inline",
        [
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "02:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
                "net_name": "Morning Net",
                "mode": "Digi",
            }
        ],
    )
    tab = planner_mod.FreqPlannerTab()
    tab._refresh_source_set_controls()
    monkeypatch.setattr(planner_mod, "assigned_plan_rf_guard_impacts_for_source_update", lambda *args, **kwargs: [])
    monkeypatch.setattr(planner_mod.QMessageBox, "question", lambda *args, **kwargs: QMessageBox.Save)
    segment = planner_mod.ScheduleSegment(
        source="NET",
        day_utc="Monday",
        start_minute=60,
        end_minute=120,
        band="40M",
        frequency="7.078",
        group_name="OPS",
        net_name="Morning Net",
        mode="Digi",
        raw={"start_utc": "01:00", "end_utc": "02:00"},
    )
    tab._set_projection_inspector(planner_mod.EffectiveWindowCell(segment=segment, net_segments=(segment,)))
    assert tab.inline_update_hf_daily_btn.isEnabled()
    assert tab.inline_update_hf_daily_btn.text() == "Update HF Net Source"
    assert "Update HF Net Source changes the named schedule" in tab.inline_editor_impact_label.text()
    assert "RF Guard reviews the impact" in tab.inline_editor_impact_label.text()
    tab.inline_band_edit.setText("80M")
    tab.inline_frequency_edit.setText("3.590")

    tab._on_inline_update_hf_daily_clicked()
    app.processEvents()

    source = planner_mod.source_set_row_by_id_for_category(
        settings,
        HF_NET_SOURCE_SETS_KEY,
        HF_NET_SOURCE_CATEGORY,
        saved_source["id"],
    )
    assert source is not None
    assert source["rows"][0]["band"] == "80M"
    assert source["rows"][0]["frequency"] == "3.590"
    assert source["rows"][0]["net_name"] == "Morning Net"
    assert "Updated HF Net source" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_effective_row_click_uses_row_cell_from_any_column(monkeypatch, tmp_path) -> None:
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
                    "end_utc": "02:00",
                    "band": "40M",
                    "frequency": "7.078",
                    "group_name": "OPS",
                    "mode": "Digi",
                }
            ],
            [],
            [],
            [],
        ),
    )

    tab.rebuild_table()
    app.processEvents()

    tab._on_schedule_cell_clicked(0, 6)
    assert tab._inline_edit_segment is not None
    assert tab.inline_group_edit.text() == "OPS"
    assert not tab.inline_editor_card.isHidden()
    assert {item.row() for item in tab.table.selectedItems()} == {0}


def test_freqplanner_pattern_summary_groups_daily_and_net_windows(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab._show_local = False
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("patterns"))
    monkeypatch.setattr(
        tab,
        "_load_schedules",
        lambda: (
            [
                {
                    "day_utc": "ALL",
                    "start_utc": "10:00",
                    "end_utc": "15:00",
                    "band": "20M",
                    "frequency": "14.115",
                    "group_name": "MAGNET",
                    "mode": "Digi",
                }
            ],
            [
                {
                    "day_utc": "Thursday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "band": "40M",
                    "frequency": "7.115",
                    "group_name": "MAGNET",
                    "net_name": "MR08",
                    "mode": "Digi",
                }
            ],
            [],
            [],
        ),
    )

    tab.rebuild_table()
    app.processEvents()

    assert [tab.table.horizontalHeaderItem(col).text() for col in range(tab.table.columnCount())] == [
        "Group / Net / SOP",
        "Band",
        "Pattern",
        "Time (UTC)",
        "Mode",
        "Layer",
        "What It Means",
    ]
    rows = [
        [tab.table.item(row, col).text() for col in range(tab.table.columnCount())]
        for row in range(tab.table.rowCount())
    ]
    assert ["MAGNET", "20M", "Daily", "10:00-15:00 UTC", "Digi", "Daily", "Daily baseline"] in rows
    assert ["MR08", "40M", "Thu", "02:00-03:00 UTC", "Digi", "Net", "Net overrides daily window"] in rows
    assert tab.table.isSortingEnabled()

    net_row = next(index for index, row in enumerate(rows) if row[0] == "MR08")
    tab._on_schedule_cell_clicked(net_row, 6)
    assert tab._inline_edit_segment is not None
    assert tab.inline_group_edit.text() == "MAGNET"
    assert tab.inline_band_edit.text() == "40M"
    assert "MR08" in tab.selected_window_title_label.text()
    assert {item.row() for item in tab.table.selectedItems()} == {net_row}


def test_freqplanner_grid_uses_projection_cells_and_inspector(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab._show_local = False
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("blended"))
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
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("blended"))
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


def test_freqplanner_radio_windows_view_renders_saved_plan_radio_lanes(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab._show_local = False
    store = tab.plan_context_service.store
    radio_20m = store.save_device_profile(
        {
            "name": "County HF",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["20M"],
            "band_overlap_guard_group": "Field",
            "band_overlap_guard_mode": "block",
        }
    )
    radio_40m = store.save_device_profile(
        {
            "name": "Net Control HF",
            "enabled": 1,
            "device_class": "observer",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
        }
    )
    saved = store.save_frequency_plan(
        {
            "name": "County Multi-Radio SOP",
            "status": "saved",
            "category": "sop_schedule",
            "schedule_refs": [
                {
                    "source": "SOP",
                    "lane_key": f"radio:{int(radio_20m['id'])}",
                    "target_device_profile_id": int(radio_20m["id"]),
                    "day_utc": "Monday",
                    "start_utc": "02:00",
                    "end_utc": "03:00",
                    "group_name": "COUNTY",
                    "profile_name": "County SOP",
                    "action_label": "Monitor county status",
                    "band": "20M",
                    "frequency": "14.115",
                    "mode": "JS8",
                },
                {
                    "source": "NET",
                    "lane_key": f"radio:{int(radio_40m['id'])}",
                    "target_device_profile_id": int(radio_40m["id"]),
                    "day_utc": "Monday",
                    "start_utc": "02:30",
                    "end_utc": "03:30",
                    "group_name": "MAGNET",
                    "net_name": "MR08 Net",
                    "band": "40M",
                    "frequency": "7.115",
                    "mode": "JS8",
                },
            ],
        }
    )
    tab._refresh_plan_workspace_header()
    idx = tab.frequency_plan_combo.findData(int(saved["id"]))
    assert idx >= 0
    tab.frequency_plan_combo.setCurrentIndex(idx)
    view_idx = tab.planner_view_combo.findData("radio")
    assert view_idx >= 0
    tab.planner_view_combo.setCurrentIndex(view_idx)
    tab.rebuild_table()
    app.processEvents()

    headers = [tab.table.horizontalHeaderItem(col).text() for col in range(tab.table.columnCount())]
    assert headers[:5] == ["Window", "Day (UTC)", "Time (UTC)", "Radio", "Layer"]
    rows = [
        [tab.table.item(row, col).text() for col in range(tab.table.columnCount())]
        for row in range(tab.table.rowCount())
    ]
    assert rows[0][:6] == ["Monday 02:00-03:00 UTC", "Monday", "02:00-03:00 UTC", "County HF", "SOP", "Monitor county status"]
    assert rows[1][:6] == ["Monday 02:30-03:30 UTC", "Monday", "02:30-03:30 UTC", "Net Control HF", "Net", "MR08 Net"]
    assert "TX/RX; 20M; overlap Field Block" in rows[0][-1]
    assert "RX-only; 40M" in rows[1][-1]
    assert "overlaps Net Control HF" in rows[0][-1]
    assert "overlaps County HF" in rows[1][-1]
    assert "Windows: 2" in tab.plan_layers_label.text()
    assert "Radios: 2 radio lanes: County HF, Net Control HF" in tab.plan_layers_label.text()
    assert not tab.radio_window_radio_combo.isHidden()
    assert tab.radio_window_radio_combo.findData(int(radio_20m["id"])) >= 0
    tab.radio_window_radio_combo.setCurrentIndex(tab.radio_window_radio_combo.findData(int(radio_20m["id"])))
    tab.rebuild_table()
    app.processEvents()
    filtered_rows = [
        [tab.table.item(row, col).text() for col in range(tab.table.columnCount())]
        for row in range(tab.table.rowCount())
    ]
    assert [row[3] for row in filtered_rows] == ["County HF"]
    assert "County HF:" in tab.frequency_plan_action_hint_label.text()


def test_freqplanner_radio_windows_view_infers_lanes_from_assigned_generic_plan(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    app = QApplication.instance() or QApplication([])
    SettingsManager()

    import freqinout.gui.freq_planner_tab as planner_mod

    planner_mod = importlib.reload(planner_mod)
    tab = planner_mod.FreqPlannerTab()
    tab._show_local = False
    store = tab.plan_context_service.store
    left = store.save_device_profile(
        {
            "name": "FIO-A",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "band_overlap_guard_group": "Field",
            "band_overlap_guard_mode": "warn",
        }
    )
    right = store.save_device_profile(
        {
            "name": "FIO-B",
            "enabled": 1,
            "device_class": "tx_rx",
            "control_backend": "manual",
            "antenna_supported_bands": ["40M"],
            "band_overlap_guard_group": "Field",
            "band_overlap_guard_mode": "warn",
        }
    )
    plan = store.save_frequency_plan(
        {
            "name": "Magnet Daily",
            "status": "saved",
            "category": "normal",
            "schedule_refs": [
                {
                    "source": "HF",
                    "day_utc": "Tuesday",
                    "start_utc": "06:00",
                    "end_utc": "10:00",
                    "group_name": "MAGNET",
                    "band": "40M",
                    "frequency": "7.115",
                    "mode": "JS8",
                }
            ],
            "frequency_refs": ["40M:7.115"],
        }
    )
    store.set_assigned_plan(int(left["id"]), int(plan["id"]))
    store.set_assigned_plan(int(right["id"]), int(plan["id"]))
    tab._refresh_plan_workspace_header()
    idx = tab.frequency_plan_combo.findData(int(plan["id"]))
    assert idx >= 0
    tab.frequency_plan_combo.setCurrentIndex(idx)
    tab.planner_view_combo.setCurrentIndex(tab.planner_view_combo.findData("radio"))
    tab.rebuild_table()
    app.processEvents()

    rows = [
        [tab.table.item(row, col).text() for col in range(tab.table.columnCount())]
        for row in range(tab.table.rowCount())
    ]
    assert [row[3] for row in rows] == ["FIO-A", "FIO-B"]
    assert all(row[:3] == ["Tuesday 06:00-10:00 UTC", "Tuesday", "06:00-10:00 UTC"] for row in rows)
    assert all(row[5] == "MAGNET" for row in rows)
    assert "overlaps FIO-B on 40M" in rows[0][-1]
    assert "overlaps FIO-A on 40M" in rows[1][-1]
    assert "2 radios" in tab.frequency_plan_action_hint_label.text()
    assert "1 overlap window to review" in tab.frequency_plan_action_hint_label.text()
    assert "Windows: 2" in tab.plan_layers_label.text()
