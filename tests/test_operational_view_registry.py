from __future__ import annotations

from freqinout.core.operational_view_registry import (
    CONTROLFREQ_VIEW_KEYS,
    MANDATORY_VIEW_GATES,
    all_operational_views,
    controlfreq_preset_names,
    controlfreq_view_labels,
    controlfreq_view_presets,
    operational_view_for,
    operational_views_for_source,
    operational_views_for_tab,
    validate_view_sources,
)


def test_controlfreq_view_registry_defines_all_selectable_cards() -> None:
    labels = dict(controlfreq_view_labels())
    presets = controlfreq_view_presets()

    assert tuple(labels) == CONTROLFREQ_VIEW_KEYS
    assert labels == {
        "activity": "Activity",
        "intersections": "Intersections",
        "schedule": "Schedule",
        "propagation": "Propagation",
    }
    assert set(presets["Operations"]) == set(CONTROLFREQ_VIEW_KEYS)
    assert controlfreq_preset_names() == ("Operations", "All", "Traffic", "Schedule", "Propagation", "Custom")
    assert controlfreq_preset_names(include_custom=False) == ("Operations", "All", "Traffic", "Schedule", "Propagation")
    assert presets["Traffic"]["activity"] is True
    assert presets["Propagation"]["propagation"] is True
    assert presets["Propagation"]["activity"] is False


def test_all_registered_views_declare_required_gates_and_limits() -> None:
    views = all_operational_views()

    assert {view.key for view in views} >= {
        "activity",
        "traffic_inbox",
        "compose_workbench",
        "map_context",
        "station_command",
    }
    for view in views:
        assert set(view.required_gates) == set(MANDATORY_VIEW_GATES)
        assert view.label
        assert view.template
        assert view.allowed_source_families
        assert view.max_default_rows >= 0


def test_future_sources_have_expected_selectable_views() -> None:
    meshcore_views = {view.key for view in operational_views_for_source("MeshCore")}
    aprs_views = {view.key for view in operational_views_for_source("APRS")}

    assert {"activity", "traffic_inbox", "compose_workbench", "map_context"}.issubset(meshcore_views)
    assert {"activity", "traffic_inbox", "map_context"}.issubset(aprs_views)
    assert "compose_workbench" not in aprs_views


def test_controlfreq_registered_sources_pass_declared_gates() -> None:
    for key in CONTROLFREQ_VIEW_KEYS:
        reports = validate_view_sources(key)
        assert reports
        assert all(report.passed for report in reports), (key, reports)


def test_registry_rejects_source_not_allowed_for_view() -> None:
    reports = validate_view_sources("compose_workbench", ("aprs",))

    assert len(reports) == 1
    assert reports[0].source_family == "aprs"
    assert reports[0].failures == ("source_not_allowed",)


def test_views_for_tab_can_return_selectable_subset() -> None:
    controlfreq = operational_views_for_tab("ControlFreq", selectable_only=True)
    messages = {view.key for view in operational_views_for_tab("Messages")}

    assert tuple(view.key for view in controlfreq) == CONTROLFREQ_VIEW_KEYS
    assert {"traffic_inbox", "compose_workbench"}.issubset(messages)
    assert operational_view_for("map-context").key == "map_context"
