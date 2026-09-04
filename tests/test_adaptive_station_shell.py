from __future__ import annotations

from pathlib import Path
from types import MethodType, SimpleNamespace

from PySide6.QtWidgets import QApplication, QLabel, QPushButton, QToolButton, QWidget


def test_adaptive_shell_is_the_production_view_and_reuses_command_paths() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "self._adaptive_station_shell_enabled = True" in source
    assert "self._refresh_adaptive_station_command_shell(visible_choices, selected_id)" in source
    assert 'controls.setObjectName("stationCommandControlsToggle")' in source
    assert "primary_context_text(radio_name, now)" in source
    assert "if ident > 0 and ident == int(selected_id or 0):" in source
    assert "The selected radio owns the primary context row below" in source
    assert "self._build_adaptive_station_controls_tray(" in source
    assert 'tray.setObjectName("stationCommandControlsTray")' in source
    assert 'drawer_label = QLabel("RADIO CONTROLS' not in source
    assert "self._station_command_for_radio_qsy(radio_id, combo, callback, duration_combo)()" in source
    assert "self._on_station_command_timed_suspend_clicked(radio_id)" in source
    assert "self._on_station_command_resume_clicked(radio_id)" in source


def test_adaptive_shell_moves_persistent_awareness_out_of_navigation() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert 'rail.setAccessibleName("Radio, condition and time awareness")' in source
    assert 'condition.setObjectName("stationCommandConditionChip")' in source
    assert 'clock.setObjectName("stationCommandClock")' in source
    assert "self.ledge_clock_widget.setVisible(False)" in source
    assert "self.condition_level_container.setVisible(False)" in source
    assert "window_width < 1080" in source
    assert "window_width >= 1180" in source
    assert "QTimer.singleShot(0, self._reflow_adaptive_station_shell)" in source
    assert 'density == "condensed"' in source


def test_next_minutes_prefers_a_changed_group_or_band() -> None:
    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.scheduler = None
    window._station_command_snapshot_id = MethodType(lambda _self, _snapshot: 7, window)
    window._station_command_assigned_plan_refs_for_radio = MethodType(
        lambda _self, _ident: [
            {"group": "MAGNET", "band": "40M", "delta": 4},
            {"group": "MAGNET", "band": "20M", "delta": 19},
            {"group": "AMRRON", "band": "20M", "delta": 33},
        ],
        window,
    )
    window._station_command_assigned_plan_group_band = MethodType(
        lambda _self, _snapshot: ("MAGNET", "40M"), window
    )
    window._station_command_ref_start_delta_minutes = MethodType(
        lambda _self, ref, _now: int(ref["delta"]), window
    )

    assert MainWindow._station_command_next_minutes(window, SimpleNamespace()) == 19


def test_compact_navigation_keeps_a_restore_control(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])
    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.nav_widget = QWidget()
    window.nav_collapse_btn = QToolButton(window.nav_widget)
    window.logo_label = QLabel(window.nav_widget)
    window.ledge_clock_widget = QWidget(window.nav_widget)
    window.nav_scroll = QWidget(window.nav_widget)
    window.status_dock_widget = QWidget(window.nav_widget)
    window.condition_level_container = QWidget(window.status_dock_widget)
    window.nav_compact_widget = QWidget(window.nav_widget)
    window.nav_buttons = []

    MainWindow._set_main_navigation_collapsed(window, True)
    app.processEvents()

    assert window.nav_widget.minimumWidth() == 74
    assert window.nav_widget.maximumWidth() == 74
    assert window.nav_compact_widget.isHidden() is False
    assert window.nav_collapse_btn.text() == "≫"
    assert window.nav_collapse_btn.accessibleName() == "Expand navigation"

    window.nav_widget.deleteLater()
    app.processEvents()


def test_compact_navigation_mirrors_master_groups_and_uses_owned_icons() -> None:
    from freqinout.gui.main_window import MainWindow

    specs = MainWindow._compact_navigation_specs()

    assert [(label, target) for label, _accessible, target, _icon in specs] == [
        ("Ops", "ControlFreq"),
        ("Map", "Map"),
        ("Messages", "Messages"),
        ("Net Ctrl", "NCS"),
        ("Operators", "Operators"),
        ("Plans", "Plan Builder"),
        ("Station", "Station"),
        ("Settings", "Settings"),
        ("Help", "Help"),
    ]
    icon_root = Path("assets/icons/navigation")
    assert all((icon_root / icon).is_file() for _label, _accessible, _target, icon in specs)

    full_specs = [
        ("Inbox", "Messages"),
        ("Compose", "Messages"),
        ("FLDigi / SSB", "NCS-FLDigi/SSB"),
        ("JS8Call", "NCS-JS8"),
        ("VHF/UHF", "NCS-Local"),
    ]
    assert [label for label, screen in full_specs if MainWindow._nav_group_for_label(label, screen) == "Messages"] == [
        "Inbox",
        "Compose",
    ]
    assert [label for label, screen in full_specs if MainWindow._nav_group_for_label(label, screen) == "NCS"] == [
        "FLDigi / SSB",
        "JS8Call",
        "VHF/UHF",
    ]

    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    assert "Qt.ToolButtonTextUnderIcon" in source
    assert "QStyle.SP_DialogOpenButton" not in source


def test_awareness_rail_omits_selected_radio_but_keeps_alternates(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])
    from freqinout.gui.main_window import MainWindow
    from freqinout.gui.theme import get_theme

    window = MainWindow.__new__(MainWindow)
    window._station_command_snapshot_needs_operator_attention = lambda _snapshot: False
    window._station_command_saved_mesh_control_items = lambda: []
    window._station_command_attention_role_for_snapshot = lambda _snapshot: ("success", "Clear")
    window._station_command_now_text_for_summary = lambda snapshot, _selected: snapshot.now
    window._station_command_source_chip_tooltip = lambda snapshot, _selected: snapshot.name
    window._collect_condition_levels = lambda: []
    window._on_station_command_summary_radio_clicked = lambda _ident: None
    parent = QWidget()
    radios = [
        SimpleNamespace(device_profile_id=1, name="FIO-A", now="AMRRON 20M"),
        SimpleNamespace(device_profile_id=2, name="FIO-B", now="MAGNET 40M"),
    ]

    rail = MainWindow._build_adaptive_station_awareness_rail(
        window,
        parent,
        radios,
        selected_id=1,
        density="compact",
        theme=get_theme("light"),
    )
    chips = rail.findChildren(QPushButton, "stationCommandSourceChip")

    assert [chip.text() for chip in chips] == ["● FIO-B"]
    assert "FIO-A" not in [chip.text() for chip in chips]

    parent.deleteLater()
    app.processEvents()


def test_qsy_options_exclude_the_radios_current_frequency() -> None:
    from freqinout.gui.main_window import MainWindow

    options = {
        "AMRRON|20M|14.110000": {"group": "AMRRON", "band": "20M", "freq": 14.110},
        "AMRRON|40M|7.110000": {"group": "AMRRON", "band": "40M", "freq": 7.110},
        "MAGNET|20M|14.115000": {"group": "MAGNET", "band": "20M", "freq": 14.115},
    }

    from_label = SimpleNamespace(current_frequency_label="14.110 MHz", current_frequency_hz=0)
    filtered = MainWindow._station_command_alternate_qsy_options(options, from_label)
    assert list(filtered) == ["AMRRON|40M|7.110000", "MAGNET|20M|14.115000"]

    from_hz = SimpleNamespace(current_frequency_label="", current_frequency_hz=14_115_000)
    filtered = MainWindow._station_command_alternate_qsy_options(options, from_hz)
    assert list(filtered) == ["AMRRON|20M|14.110000", "AMRRON|40M|7.110000"]


def test_qsy_options_are_unchanged_when_current_frequency_is_unavailable() -> None:
    from freqinout.gui.main_window import MainWindow

    options = {"AMRRON|40M|7.110000": {"group": "AMRRON", "band": "40M", "freq": 7.110}}
    assert MainWindow._station_command_alternate_qsy_options(options, SimpleNamespace()) == options
