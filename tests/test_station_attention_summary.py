"""Focused contracts for the Station Control Bar attention summary."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from PySide6.QtWidgets import QApplication, QPushButton, QWidget


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _window_for_rail():
    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window._station_command_attention_role_for_snapshot = lambda item: (
        ("warning", "Watch") if getattr(item, "needs_attention", False) else ("success", "Clear")
    )
    window._station_command_snapshot_needs_operator_attention = (
        lambda item: bool(getattr(item, "needs_attention", False))
    )
    window._station_command_focus_score = lambda item, selected: int(item.device_profile_id) + (
        100 if int(item.device_profile_id) == int(selected or 0) else 0
    )
    window._station_command_snapshot_id = lambda item: int(item.device_profile_id)
    window._station_command_snapshot_name = lambda item: str(item.name)
    window._station_command_now_text_for_summary = lambda item, _selected: str(item.now)
    window._station_command_source_chip_tooltip = lambda item, _selected: str(item.name)
    window._station_command_saved_mesh_control_items = lambda: []
    window._collect_condition_levels = lambda: []
    window._on_station_command_summary_radio_clicked = lambda _ident: None
    return window


@pytest.mark.parametrize("theme_name", ["light", "dark"])
@pytest.mark.parametrize("density, expected", [("roomy", "ATTN: 1"), ("compact", "ATTN: 1"), ("condensed", "! 1")])
def test_attention_chip_uses_readable_wide_label_and_compact_label(theme_name, density, expected) -> None:
    _app()
    from freqinout.gui.main_window import MainWindow
    from freqinout.gui.theme import get_theme

    window = _window_for_rail()
    parent = QWidget()
    choices = [SimpleNamespace(device_profile_id=1, name="FIO-A", now="AMRRON 20M", needs_attention=True)]
    rail = MainWindow._build_adaptive_station_awareness_rail(
        window, parent, choices, selected_id=0, density=density, theme=get_theme(theme_name)
    )
    chips = rail.findChildren(QPushButton, "stationCommandAttentionChip")
    assert [button.text() for button in chips] == [expected]
    assert chips[0].accessibleName() == "1 radio or source needs attention"
    parent.deleteLater()


def test_attention_count_is_affected_radios_not_issue_count_and_includes_three_radio_case() -> None:
    _app()
    from freqinout.gui.main_window import MainWindow
    from freqinout.gui.theme import get_theme

    window = _window_for_rail()
    parent = QWidget()
    choices = [
        SimpleNamespace(device_profile_id=1, name="FIO-A", now="AMRRON 20M", needs_attention=True, issues=("offline", "off schedule")),
        SimpleNamespace(device_profile_id=1, name="FIO-A duplicate", now="AMRRON 20M", needs_attention=True, issues=("duplicate",)),
        SimpleNamespace(device_profile_id=2, name="FIO-B", now="MAGNET 40M", needs_attention=True, issues=("RF Guard",)),
        SimpleNamespace(device_profile_id=3, name="FIO-C", now="Local 2M", needs_attention=True, issues=("PTT", "busy", "error")),
    ]
    rail = MainWindow._build_adaptive_station_awareness_rail(
        window, parent, choices, selected_id=0, density="roomy", theme=get_theme("light")
    )
    chip = rail.findChild(QPushButton, "stationCommandAttentionChip")
    assert chip is not None
    assert chip.text() == "ATTN: 3"
    assert "3 radios or sources" in chip.accessibleName()
    parent.deleteLater()


def test_mesh_chip_does_not_inflate_radio_attention_count() -> None:
    _app()
    from freqinout.gui.main_window import MainWindow
    from freqinout.gui.theme import get_theme

    window = _window_for_rail()
    window._station_command_saved_mesh_control_items = lambda: [
        SimpleNamespace(label="MeshCore", tooltip="Connected mesh", role="success")
    ]
    window._show_mesh_source_menu = lambda *_args: None
    parent = QWidget()
    choices = [
        SimpleNamespace(device_profile_id=1, name="FIO-A", now="AMRRON 20M", needs_attention=True),
        SimpleNamespace(device_profile_id=2, name="FIO-B", now="MAGNET 40M", needs_attention=True),
    ]

    rail = MainWindow._build_adaptive_station_awareness_rail(
        window, parent, choices, selected_id=1, density="compact", theme=get_theme("light")
    )

    assert rail.findChild(QPushButton, "stationCommandAttentionChip").text() == "ATTN: 2"
    assert any(button.text().startswith("MeshCore") for button in rail.findChildren(QPushButton))
    parent.deleteLater()


def test_attention_entries_are_one_row_per_radio_with_cached_reason() -> None:
    from freqinout.gui.main_window import MainWindow

    window = _window_for_rail()
    window._station_command_off_schedule_by_radio = {}
    window._station_command_lane_cache_data = {
        2: {"assignment_validation_status_json": '{"state":"blocked"}'}
    }
    choices = [
        SimpleNamespace(device_profile_id=1, name="FIO-A", now="AMRRON 20M", needs_attention=True,
                        service_states={"FLRig": {"state": "warn", "tooltip": "Receiver unavailable"}}),
        SimpleNamespace(device_profile_id=1, name="FIO-A", now="AMRRON 20M", needs_attention=True,
                        service_states={"FLRig": {"state": "error", "tooltip": "Duplicate must not appear"}}),
        SimpleNamespace(device_profile_id=2, name="FIO-B", now="MAGNET 40M", needs_attention=True,
                        service_states={}),
    ]
    entries = MainWindow._station_command_attention_summary_entries(window, choices, 1)
    assert [(row[1], row[2], row[3]) for row in entries] == [
        (1, "FIO-A", "Receiver unavailable"),
        (2, "FIO-B", "RF Guard"),
    ]


def test_attention_menu_rows_have_review_and_station_health_actions(monkeypatch) -> None:
    _app()
    from freqinout.gui.main_window import MainWindow

    window = _window_for_rail()
    window.settings = {}
    window._station_command_off_schedule_by_radio = {}
    reviewed: list[int] = []
    window._open_station_health_detail = lambda device_profile_id=0, **_kwargs: reviewed.append(device_profile_id)
    parent = QWidget()
    choices = [SimpleNamespace(
        device_profile_id=1,
        name="FIO-A",
        now="AMRRON 20M",
        needs_attention=True,
        service_states={"FLRig": {"state": "warn", "tooltip": "Receiver unavailable"}},
    )]
    window._show_station_command_attention_menu(choices=choices, selected_id=1, anchor=parent)
    menu = window._station_command_attention_menu
    labels = [action.text() for action in menu.actions()]
    assert any(label.startswith("Review FIO-A") and "Receiver unavailable" in label for label in labels)
    assert "Open Station Health" in labels
    next(action for action in menu.actions() if action.text().startswith("Review FIO-A")).trigger()
    assert reviewed == [1]
    menu.deleteLater()
    parent.deleteLater()


def test_attention_menu_caps_rows_and_routes_overflow_to_station_health() -> None:
    _app()
    from freqinout.gui.main_window import MainWindow

    window = _window_for_rail()
    window.settings = {}
    window._station_command_off_schedule_by_radio = {}
    window._open_station_health_detail = lambda **_kwargs: None
    parent = QWidget()
    choices = [
        SimpleNamespace(
            device_profile_id=ident,
            name=f"FIO-{ident}",
            overall_state="warn",
            service_states={"FLRig": {"state": "warn", "tooltip": "FLRig unavailable"}},
        )
        for ident in range(1, 5)
    ]

    MainWindow._show_station_command_attention_menu(
        window,
        choices=choices,
        selected_id=1,
        anchor=parent,
    )

    labels = [action.text() for action in window._station_command_attention_menu.actions()]
    assert sum(label.startswith("Review FIO-") for label in labels) == 3
    assert "+1 more — open Station Health" in labels
    window._station_command_attention_menu.close()
    window._station_command_attention_menu.deleteLater()
    parent.deleteLater()


def test_attention_summary_open_does_not_start_endpoint_or_database_io() -> None:
    _app()
    from freqinout.gui.main_window import MainWindow

    def forbidden(*_args, **_kwargs):
        raise AssertionError("attention disclosure must not perform classification or I/O")

    window = _window_for_rail()
    window.settings = SimpleNamespace(get=forbidden)
    window._station_command_off_schedule_by_radio = {}
    window._station_command_lane_cache_data = {}
    window._station_command_snapshot_needs_operator_attention = forbidden
    window._station_command_attention_role_for_snapshot = forbidden
    window._station_command_focus_score = forbidden
    window._station_command_health_summary_for_profile = forbidden
    window.multi_radio_store = SimpleNamespace(list_device_profiles=forbidden)
    window.dependency_status_service = SimpleNamespace(software_status_snapshot=forbidden)
    window._open_station_health_detail = lambda **_kwargs: None
    parent = QWidget()
    choices = [SimpleNamespace(
        device_profile_id=1,
        name="FIO-A",
        runtime_active=True,
        runtime_primary=True,
        overall_state="warn",
        service_states={"FLRig": {"state": "warn", "tooltip": "FLRig unavailable"}},
    )]

    MainWindow._show_station_command_attention_menu(
        window,
        choices=choices,
        selected_id=1,
        anchor=parent,
    )

    assert window._station_command_attention_menu.isVisible()
    window._station_command_attention_menu.close()
    window._station_command_attention_menu.deleteLater()
    parent.deleteLater()
