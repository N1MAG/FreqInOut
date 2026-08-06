from __future__ import annotations

import datetime as dt
import json

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.operational_projection import (
    OPERATIONAL_PLAN_CATEGORY,
    build_operational_day_projection,
    build_operational_day_projection_from_refs,
)


def test_operational_projection_builds_side_by_side_group_sop_and_resource_lanes() -> None:
    projection = build_operational_day_projection(
        hf_rows=[
            {
                "day_utc": "Monday",
                "start_utc": "01:00",
                "end_utc": "04:00",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "OPS",
            }
        ],
        net_rows=[
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
        sop_rows=[
            {
                "sop_profile_id": 7,
                "sop_layer_id": 11,
                "profile_name": "County SOP",
                "day_utc": "Monday",
                "start_utc": "02:30",
                "end_utc": "03:30",
                "band": "20M",
                "frequency": "14.078",
                "group_name": "COUNTY",
                "action_label": "Call NCS",
            }
        ],
        net_resource_rows=[
            {
                "id": 314,
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "AUX",
                "net_name": "Aux Resource Net",
            }
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )

    lanes = {lane.lane_key: lane for lane in projection.lanes}
    assert set(lanes) == {"group:OPS", "sop:7", "group:AUX"}
    assert projection.source_refs() == ["hf_daily", "hf_nets", "net_resources", "sop"]
    assert "40M:7.110" in projection.frequency_refs()
    assert set(projection.group_refs()) == {"OPS", "COUNTY", "AUX"}

    aux_cell = next(cell for cell in projection.cells if cell.lane_key == "group:AUX" and cell.day_utc == "Monday" and cell.hour_utc == 2)
    assert aux_cell.display_label == "Aux Resource Net 40M 7.110"
    assert aux_cell.entries[0].source == "NET_RESOURCE"
    assert aux_cell.entries[0].to_schedule_ref()["resource_id"] == 314
    assert projection.cell_for("group:AUX", "Monday", 2) == aux_cell
    assert projection.lane_day_summary("group:AUX", "Monday") == ["- 02:00 Aux Resource Net 40M 7.110"]


def test_operational_projection_marks_contention_inside_a_lane() -> None:
    projection = build_operational_day_projection(
        hf_rows=[],
        net_rows=[],
        sop_rows=[
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
            },
            {
                "sop_profile_id": 7,
                "profile_name": "County SOP",
                "day_utc": "Monday",
                "start_utc": "02:30",
                "end_utc": "03:30",
                "band": "40M",
                "frequency": "7.078",
                "group_name": "COUNTY",
                "action_label": "Monitor Peer",
            },
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )

    cell = next(cell for cell in projection.cells if cell.lane_key == "sop:7" and cell.day_utc == "Monday" and cell.hour_utc == 2)
    assert cell.has_contention
    assert [entry.action_label for entry in cell.entries] == ["Call NCS", "Monitor Peer"]
    assert projection.lane_day_summary("sop:7", "Monday")[0].startswith("! 02:00")


def test_operational_projection_preserves_sop_when_net_wins_blended_layer() -> None:
    projection = build_operational_day_projection(
        hf_rows=[],
        net_rows=[
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
        sop_rows=[
            {
                "sop_profile_id": 7,
                "profile_name": "County SOP",
                "day_utc": "Monday",
                "start_utc": "02:15",
                "end_utc": "02:45",
                "band": "20M",
                "frequency": "14.078",
                "group_name": "COUNTY",
                "action_label": "Call NCS",
            }
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )

    refs = projection.schedule_refs()

    assert [row["source"] for row in refs] == ["NET", "SOP"]
    assert projection.source_counts["NET"] == 1
    assert projection.source_counts["SOP"] == 1
    assert projection.cell_for("sop:7", "Monday", 2).display_label == "Call NCS 20M 14.078"


def test_operational_projection_filters_biweekly_rows_by_selected_week() -> None:
    projection = build_operational_day_projection(
        hf_rows=[],
        net_rows=[],
        sop_rows=[],
        net_resource_rows=[
            {
                "id": 314,
                "day_utc": "Monday",
                "recurrence": "Bi-Weekly",
                "biweekly_offset_weeks": 1,
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "AUX",
                "net_name": "Off Week Resource",
            }
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )

    assert projection.schedule_refs() == []
    assert projection.source_counts["NET_RESOURCE"] == 0


def test_operational_projection_dedupes_net_resources_already_in_hf_nets() -> None:
    projection = build_operational_day_projection(
        hf_rows=[],
        net_rows=[
            {
                "resource_id": 314,
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "AUX",
                "net_name": "Aux Resource Net",
            }
        ],
        sop_rows=[],
        net_resource_rows=[
            {
                "id": 314,
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "AUX",
                "net_name": "Aux Resource Net",
            }
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )

    refs = projection.schedule_refs()

    assert [row["source"] for row in refs] == ["NET"]
    assert projection.source_counts["NET_RESOURCE"] == 0


def test_operational_projection_uses_target_device_profile_for_radio_lanes() -> None:
    projection = build_operational_day_projection(
        hf_rows=[],
        net_rows=[
            {
                "target_device_profile_id": 42,
                "day_utc": "Monday",
                "start_utc": "02:00",
                "end_utc": "03:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "AUX",
                "net_name": "Targeted Net",
            }
        ],
        sop_rows=[],
        week_start_utc=dt.date(2026, 8, 2),
    )

    lane = projection.lanes[0]
    ref = projection.schedule_refs()[0]

    assert lane.lane_key == "radio:42"
    assert lane.radio_id == 42
    assert ref["radio_id"] == 42
    assert ref["target_device_profile_id"] == 42


def test_operational_projection_payload_saves_as_sop_schedule_plan(tmp_path) -> None:
    projection = build_operational_day_projection(
        hf_rows=[],
        net_rows=[],
        sop_rows=[
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
        week_start_utc=dt.date(2026, 8, 2),
    )
    payload = projection.to_frequency_plan_payload("County Operational Day")

    store = MultiRadioStore(tmp_path / "settings.db")
    saved = store.save_frequency_plan(payload)

    assert saved["category"] == OPERATIONAL_PLAN_CATEGORY
    refs = json.loads(str(saved["schedule_refs_json"]))
    assert refs[0]["source"] == "SOP"
    assert refs[0]["lane_key"] == "sop:7"
    assert refs[0]["action_label"] == "Call NCS"
    assert json.loads(str(saved["source_refs_json"])) == ["sop"]


def test_operational_projection_from_saved_refs_preserves_plan_ref_index() -> None:
    projection = build_operational_day_projection_from_refs(
        [
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
        week_start_utc=dt.date(2026, 8, 2),
    )

    entry = projection.cell_for("sop:7", "Monday", 2).entries[0]

    assert entry.raw["plan_ref_index"] == 0
    assert projection.schedule_refs()[0]["action_label"] == "Call NCS"


def test_operational_projection_from_saved_refs_renders_overnight_next_day_cell() -> None:
    projection = build_operational_day_projection_from_refs(
        [
            {
                "source": "SOP",
                "lane_key": "sop:7",
                "lane_label": "County SOP",
                "day_utc": "Monday",
                "start_utc": "23:30",
                "end_utc": "01:00",
                "band": "40M",
                "frequency": "7.110",
                "group_name": "COUNTY",
                "action_label": "Overnight Watch",
            }
        ],
        week_start_utc=dt.date(2026, 8, 2),
    )

    monday_cell = projection.cell_for("sop:7", "Monday", 23)
    tuesday_cell = projection.cell_for("sop:7", "Tuesday", 0)

    assert monday_cell.entries[0].action_label == "Overnight Watch"
    assert tuesday_cell.entries[0].action_label == "Overnight Watch"
