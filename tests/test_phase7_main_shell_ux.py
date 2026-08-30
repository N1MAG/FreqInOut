from __future__ import annotations

import datetime
import json
from pathlib import Path
from types import MethodType, SimpleNamespace

from PySide6.QtCore import QObject, Qt
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QTableWidgetItem,
    QToolButton,
    QVBoxLayout,
    QWidget,
)


def test_phase7_main_window_has_global_ledge_clock() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "self.ledge_clock_widget = QFrame(self.nav_widget)" in source
    assert 'self.ledge_clock_widget.setObjectName("mainLedgeClock")' in source
    assert 'self.ledge_local_time_label.setObjectName("ledgeLocalTime")' in source
    assert 'self.ledge_utc_time_label.setObjectName("ledgeUtcTime")' in source
    assert "self._ledge_clock_timer = QTimer(self)" in source
    assert "self._ledge_clock_timer.timeout.connect(self._update_ledge_clock)" in source
    assert "def _update_ledge_clock(self) -> None:" in source
    assert "get_timezone(tz_name)" in source


def test_phase7_navigation_groups_station_health_and_schedule_editors() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert source.index('("ControlFreq", "ControlFreq")') < source.index('("Map", "Map")')
    assert source.index('("Map", "Map")') < source.index('("Control Center", "Station Overview")')
    assert '("Control Center", "Station Overview")' in source
    assert '("Health Details", "Station Health")' in source
    assert '("Plan Manager", "FreqPlanner")' in source
    assert '("SOP Builder", "SOP")' in source
    assert source.index('("Plan Manager", "FreqPlanner")') < source.index('("SOP Builder", "SOP")')
    assert source.index('("SOP Builder", "SOP")') < source.index('("Inbox", "Messages")')
    assert '("HF Daily", "HF Schedule")' in source
    assert '("HF Nets", "Net Schedule")' in source
    assert '("HF Peer Scheds", "Peer Schedules")' in source
    assert '("Inbox", "Messages")' in source
    assert '("Compose", "Messages")' in source
    assert '("Main", "Settings")' in source
    assert '("Radios", "Settings")' in source
    assert 'self._nav_group_order: list[str] = ["Station", "FreqPlanner", "Messages", "NCS", "Operators", "Settings"]' in source
    assert '"Messages": False' in source
    assert '"Station": False' in source
    assert '"FreqPlanner": False' in source
    assert "self._suppress_initial_nav_group_auto_expand = True" in source
    assert 'if screen in {"Station Overview", "Station Health"}:' in source
    assert 'return "Station"' in source
    assert 'if screen in {"FreqPlanner", "SOP", "HF Schedule", "Net Schedule", "Peer Schedules"}:' in source
    assert 'return "FreqPlanner"' in source
    assert 'if screen == "Messages":' in source
    assert 'return "Messages"' in source
    assert 'if screen == "Settings":' in source
    assert 'return "Settings"' in source
    assert 'self._settings_nav_context = "main"' in source
    assert "self._settings_nav_button_indices: dict[str, int] = {}" in source
    assert "self._messages_nav_button_indices: dict[str, int] = {}" in source
    assert 'btn.clicked.connect(lambda _=False: self.open_settings_section("operator_info", settings_nav_context="main"))' in source
    assert 'btn.clicked.connect(lambda _=False: self.open_settings_section("radio_profiles", settings_nav_context="radios"))' in source
    assert 'self._settings_nav_button_indices["main"] = btn_idx' in source
    assert 'self._settings_nav_button_indices["radios"] = btn_idx' in source
    assert 'self._messages_nav_button_indices["inbox"] = btn_idx' in source
    assert 'self._messages_nav_button_indices["compose"] = btn_idx' in source
    assert 'if label == "Settings":' in source
    assert 'nav_idx = self._settings_nav_button_indices.get(' in source
    assert '("Settings", "Settings")' not in source
    assert '("Station Health", "Station Health")' not in source
    assert '"Schedules": False' not in source
    assert "def _expand_nav_group_for_screen" in source
    assert "self._expand_nav_group_for_screen(label)" in source
    assert "if changed:\n            self._persist_nav_group_states()" not in source
    assert '"Station": False' in source
    assert "self._suppress_initial_nav_group_auto_expand = True" in source
    assert 'if key == "Station" and not expanded:' in source
    assert "self._station_health_alert_counts()" in source
    assert "Expand Station or open Health Details." in source


def test_phase7_primary_nav_groups_start_collapsed() -> None:
    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def get(self, key: str, default=None):
            if key == "main_nav_group_states":
                return {
                    "Station": False,
                    "FreqPlanner": False,
                    "Schedules": False,
                    "NCS": True,
                    "Operators": True,
                }
            return default

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()

    states = MainWindow._load_nav_group_states(window)

    assert states["Station"] is False
    assert states["FreqPlanner"] is False
    assert states["Messages"] is False
    assert states["NCS"] is True
    assert states["Operators"] is True


def test_main_messages_navigation_routes_to_requested_surface(monkeypatch) -> None:
    from freqinout.gui import main_window as main_window_module
    from freqinout.gui.main_window import MainWindow

    callbacks: list[object] = []
    monkeypatch.setattr(
        main_window_module.QTimer,
        "singleShot",
        lambda _delay, callback: callbacks.append(callback),
    )

    class FakeMessagesTab:
        def __init__(self) -> None:
            self.inbox_contexts: list[dict[str, object]] = []
            self.inbox_plain_count = 0
            self.compose_count = 0
            self.compose_intents: list[dict[str, object]] = []

        def show_inbox_with_context(self, **context: object) -> None:
            self.inbox_contexts.append(dict(context))

        def show_inbox_from_navigation(self) -> None:
            self.inbox_plain_count += 1

        def show_compose_from_navigation(self) -> None:
            self.compose_count += 1

        def prefill_compose_intent(self, intent: dict[str, object]) -> None:
            self.compose_intents.append(dict(intent))

    tab = FakeMessagesTab()
    window = MainWindow.__new__(MainWindow)
    window._screen_index_by_label = {"Messages": 7}
    window._messages_nav_context = "inbox"
    window._messages_nav_filter_context = {}
    window.message_viewer_tab = tab
    selected_screens: list[int] = []
    window._set_screen = selected_screens.append

    MainWindow.open_messages_section(
        window,
        "inbox",
        group_filter="MAGNET",
        topic_filter="Comms",
        grid_filter="DM79QJ",
    )
    assert selected_screens == [7]
    assert len(callbacks) == 1
    callbacks.pop()()
    assert tab.inbox_contexts
    assert tab.inbox_contexts[-1]["group_filter"] == "MAGNET"
    assert tab.inbox_contexts[-1]["topic_filter"] == "Comms"
    assert tab.inbox_contexts[-1]["grid_filter"] == "DM79QJ"
    assert tab.compose_count == 0

    MainWindow.open_messages_section(
        window,
        "compose",
        compose_intent={"target_callsign": "KI6QDB", "topic": "Comms"},
    )
    assert selected_screens == [7, 7]
    assert len(callbacks) == 1
    callbacks.pop()()
    assert tab.compose_count == 1
    assert tab.compose_intents[-1]["target_callsign"] == "KI6QDB"


def test_main_map_routes_focus_expected_report_surfaces(monkeypatch) -> None:
    from freqinout.gui import main_window as main_window_module
    from freqinout.gui.main_window import MainWindow

    callbacks: list[object] = []
    monkeypatch.setattr(
        main_window_module.QTimer,
        "singleShot",
        lambda _delay, callback: callbacks.append(callback),
    )

    class FakeMapTab:
        def __init__(self) -> None:
            self.hf_report_contexts: list[dict[str, object]] = []
            self.local_report_contexts: list[dict[str, object]] = []

        def focus_hf_reports(self, **context: object) -> None:
            self.hf_report_contexts.append(dict(context))

        def focus_local_reports(self, **context: object) -> None:
            self.local_report_contexts.append(dict(context))

    tab = FakeMapTab()
    window = MainWindow.__new__(MainWindow)
    window._screen_index_by_label = {"Map": 4}
    window.stations_map_tab = tab
    window._screen_is_runtime_suppressed = lambda _label: False
    window._sync_map_filters_from_tab = lambda: None
    selected_screens: list[int] = []
    window._set_screen = selected_screens.append

    MainWindow.open_spotter_map(
        window,
        group_filter="MAGNET",
        topic_filter="Fire",
        query_filter="KI6QDB",
    )
    assert selected_screens == [4]
    while callbacks:
        callbacks.pop(0)()
    assert tab.hf_report_contexts[-1]["group_filter"] == "MAGNET"
    assert tab.hf_report_contexts[-1]["topic_filter"] == "Fire"
    assert tab.hf_report_contexts[-1]["query_filter"] == "KI6QDB"
    assert not tab.local_report_contexts

    MainWindow.open_local_reports_map(
        window,
        group_filter="AMRRON",
        topic_filter="Comms",
        state_filter="CO",
    )
    assert selected_screens == [4, 4]
    while callbacks:
        callbacks.pop(0)()
    assert tab.local_report_contexts[-1]["group_filter"] == "AMRRON"
    assert tab.local_report_contexts[-1]["topic_filter"] == "Comms"
    assert tab.local_report_contexts[-1]["state_filter"] == "CO"


def test_phase7_controlfreq_setup_review_sits_above_title() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    review_widget = source.index("self.readiness_review_widget = QWidget()")
    review_add = source.index("root.addWidget(self.readiness_review_widget)")
    root_header = source.index("root.addLayout(header)")
    filter_add = source.index("root.addLayout(filter_row)")

    assert review_widget < review_add < root_header < filter_add


def test_phase7_station_workspace_decisions_are_specified() -> None:
    spec = Path(
        "/Users/bill/RadioCode/WORK/MultiRig/"
        "FIO_MultiRig_Phase7_Main_Shell_Station_Workspace_UX_Spec_2026-07-26.md"
    ).read_text(encoding="utf-8")

    assert "Station Control And Station Health Consolidate Into One Station Workspace" in spec
    assert "Start With A Read-Only Station Control Center" in spec
    assert "FreqPlanner Owns Schedule Planning" in spec
    assert "Slice 7J - Plan Manager And SOP Planning Navigation" in spec
    assert "Move `SOP Builder` into the FreqPlanner navigation group." in spec
    assert "`What to Do When There`: selected SOP layer(s), condition levels, roles, tiers, and action guidance." in spec
    assert "Default presentation is `Effective Windows`" in spec
    assert "The week/grid view remains available as `Week Grid`" in spec
    assert "`SOP Lanes` remains available" in spec
    assert "Global Local/UTC Clock Lives In The Main Ledge" in spec
    assert "Settings Layout Cleanup Is Part Of Phase 7" in spec
    assert "Tables Need Stable Headers" in spec


def test_phase7_high_use_tabs_hide_duplicate_live_clocks() -> None:
    sources = {
        "settings": Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8"),
        "freqplanner": Path("freqinout/gui/freq_planner_tab.py").read_text(encoding="utf-8"),
        "controlfreq": Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8"),
        "messages": Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8"),
        "hf_daily": Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8"),
        "hf_nets": Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8"),
        "sop": Path("freqinout/gui/sop_tab.py").read_text(encoding="utf-8"),
        "fldigi_ncs": Path("freqinout/gui/fldigi_net_control_tab.py").read_text(encoding="utf-8"),
        "js8_ncs": Path("freqinout/gui/js8call_net_control_tab.py").read_text(encoding="utf-8"),
        "local_ncs": Path("freqinout/gui/local_ncs_tab.py").read_text(encoding="utf-8"),
    }

    for key in ("settings", "freqplanner", "messages", "hf_daily", "hf_nets", "sop", "fldigi_ncs", "js8_ncs", "local_ncs"):
        assert "self.utc_label.setVisible(False)" in sources[key]
        assert "self.local_label.setVisible(False)" in sources[key]

    assert "self.current_time_label.setVisible(False)" in sources["controlfreq"]


def test_phase7_table_time_toggles_use_times_wording() -> None:
    source_paths = [
        "freqinout/gui/freq_planner_tab.py",
        "freqinout/gui/controlfreq_tab.py",
        "freqinout/gui/message_viewer_tab.py",
        "freqinout/gui/daily_schedule_tab.py",
        "freqinout/gui/net_schedule_tab.py",
        "freqinout/gui/peer_sched_tab.py",
        "freqinout/gui/sop_tab.py",
    ]
    combined = "\n".join(Path(path).read_text(encoding="utf-8") for path in source_paths)

    assert "Times: Local" in combined
    assert "Times: UTC" in combined
    assert "Showing: Local" not in combined
    assert "Showing: UTC" not in combined


def test_phase7_hf_nets_uses_compact_default_with_view_edit_details() -> None:
    source = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")

    assert "COMPACT_VISIBLE_COLUMNS = frozenset(" in source
    assert 'self.view_edit_btn = QPushButton("View/Edit")' in source


def test_phase7_schedule_tabs_use_operator_schedule_language() -> None:
    freqplanner = Path("freqinout/gui/freq_planner_tab.py").read_text(encoding="utf-8")
    daily = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")
    nets = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")

    assert "Active Daily Schedule" in freqplanner
    assert "Active Net Schedule" in freqplanner
    assert "Select Live Current" not in freqplanner
    assert "Review blended schedule before saving" not in freqplanner

    assert "Active Daily Schedule" in daily
    assert "Active Net Schedule" in nets
    assert 'QPushButton("Save / Update Schedule")' in daily
    assert 'QPushButton("Save Schedule")' in nets
    assert 'QPushButton("Delete Schedule")' in daily
    assert 'QPushButton("Delete Schedule")' in nets
    source = nets
    assert "self.view_edit_btn.setCheckable(True)" in source
    assert "self.view_edit_btn.toggled.connect(self._apply_compact_schedule_view)" in source
    assert "def _apply_compact_schedule_view" in source
    assert "self.table.setColumnHidden(col, not show_all and col not in self.COMPACT_VISIBLE_COLUMNS)" in source
    assert "Show all editable fields for the selected net schedule rows." in source
    assert "Hide advanced schedule fields for normal net scanning." in source
    assert "self._net_action_layout = QGridLayout()" in source
    assert "self._net_resource_filter_layout = QGridLayout()" in source
    assert "(self.time_toggle_btn, 0, 0)" in source
    assert "def _update_net_responsive_layout(self) -> None:" in source
    assert 'self.net_schedule_scroll_area.setObjectName("netScheduleScrollArea")' in source
    assert "self.net_schedule_scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "def _apply_net_compact_table_sizing(self, *, compact: bool) -> None:" in source


def test_phase7_hf_nets_schedule_name_is_inline_editable() -> None:
    source = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index('self.schedule_source_label = QLabel("Net Schedule:")') : source.index("# Net resources section")]
    action_block = source[source.index('self.move_to_resources_btn = QPushButton(') : source.index("# Net resources section")]
    save_block = source[source.index("def _on_save_freqplanner_source_clicked") : source.index("def _confirm_rf_guard_source_update")]

    assert "self.schedule_source_combo.setEditable(True)" in build_block
    assert "Name or select a net schedule" in build_block
    assert 'self.save_btn = QPushButton("Save Schedule")' in build_block
    assert 'self.delete_source_btn = QPushButton("Delete Schedule")' in build_block
    assert 'self.move_to_resources_btn = QPushButton("Save Selected to Library")' in action_block
    assert "self.move_to_resources_btn.clicked.connect(self._save_selected_schedule_rows_as_resources)" in source
    assert "self._current_freqplanner_source_name()" in save_block
    assert "_prompt_for_freqplanner_source_name" not in save_block
    assert 'return "HF Net Schedule"' in source
    assert "source schedule" not in save_block
    assert "source_ref=\"saved_from_schedule\"" in source
    assert "The HF Net schedule was not changed." in source
    assert "self.table.removeRow(r)" not in source[source.index("def _save_selected_schedule_rows_as_resources") : source.index("def _resource_import_key")]
    assert "Net Row Library</h3>" in source
    assert "Save Schedule when ready." in source
    assert 'QAction("Add Selected Rows", self)' in source
    assert 'QAction("Add Filtered Rows", self)' in source
    assert 'self.edit_resource_btn = QPushButton("Edit Library Row")' in source
    assert 'self.delete_resource_btn = QPushButton("Delete Library Rows")' in source


def test_phase7_hf_nets_resources_use_age_not_raw_updated_timestamp() -> None:
    source = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")
    table_block = source[source.index("self.resources_table.setHorizontalHeaderLabels") : source.index("self.resources_table.setEditTriggers")]
    populate_block = source[source.index("def _refresh_resources_table") : source.index("def _update_resource_action_state")]

    assert '"Age"' in table_block
    assert '"Updated (UTC)"' not in table_block
    assert "self._format_age_label(row.get(\"updated_utc\"))" in populate_block
    assert "item.setToolTip(raw_updated if raw_updated else" in populate_block


def test_phase7_hf_nets_view_edit_toggles_advanced_columns(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import net_schedule_tab as net_mod

    monkeypatch.setattr(net_mod.NetScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_bootstrap_net_resources", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_load_resources_from_db", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resource_set_combo", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resources_table", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_schedule_net_sop_conflict_refresh", lambda self, **_kwargs: None)

    tab = net_mod.NetScheduleTab()
    try:
        assert tab.view_edit_btn.text() == "View/Edit"
        assert tab.view_edit_btn.isCheckable() is True
        assert tab.view_edit_btn.isChecked() is False

        assert tab.table.isColumnHidden(tab.COL_RECURRENCE) is True
        assert tab.table.isColumnHidden(tab.COL_MONTH_WEEKS) is True
        assert tab.table.isColumnHidden(tab.COL_FLDIGI_MODE) is True
        assert tab.table.isColumnHidden(tab.COL_AUTOTUNE) is True
        assert tab.table.isColumnHidden(tab.COL_NETNAME) is False
        assert tab.table.isColumnHidden(tab.COL_START) is False
        assert tab.table.isColumnHidden(tab.COL_END) is False
        assert tab.table.isColumnHidden(tab.COL_FREQ) is False
        assert tab.table.isColumnHidden(tab.COL_TARGET) is True

        tab.view_edit_btn.setChecked(True)
        app.processEvents()

        assert all(not tab.table.isColumnHidden(col) for col in range(tab.table.columnCount()))
        assert "Hide advanced" in tab.view_edit_btn.toolTip()

        tab.view_edit_btn.setChecked(False)
        app.processEvents()

        assert tab.table.isColumnHidden(tab.COL_RECURRENCE) is True
        assert tab.table.isColumnHidden(tab.COL_NETNAME) is False
        assert tab.table.isColumnHidden(tab.COL_TARGET) is True
        assert "Show all editable" in tab.view_edit_btn.toolTip()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_nets_action_rows_reflow_at_compact_width(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import net_schedule_tab as net_mod

    monkeypatch.setattr(net_mod.NetScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_bootstrap_net_resources", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_load_resources_from_db", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resource_set_combo", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resources_table", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_schedule_net_sop_conflict_refresh", lambda self, **_kwargs: None)

    tab = net_mod.NetScheduleTab()
    try:
        tab.resize(1000, 800)
        tab._update_net_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "compact"
        assert tab._net_action_layout.itemAtPosition(0, 2).widget() is tab.schedule_source_combo
        assert tab._net_action_layout.itemAtPosition(0, 4).widget() is tab.new_source_btn
        assert tab._net_action_layout.itemAtPosition(0, 5).widget() is tab.save_btn
        assert tab._net_action_layout.itemAtPosition(0, 6).widget() is tab.rename_source_btn
        assert tab._net_action_layout.itemAtPosition(0, 7).widget() is tab.delete_source_btn
        assert tab._net_action_layout.itemAtPosition(1, 3).widget() is tab.move_to_resources_btn
        assert tab._net_resource_filter_layout.itemAtPosition(1, 0).widget() is tab.add_to_schedule_btn
        assert tab.net_schedule_scroll_area.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.table.horizontalHeader().sectionResizeMode(tab.COL_GROUP) == QHeaderView.Stretch
        assert tab.table.horizontalHeader().sectionResizeMode(tab.COL_NETNAME) == QHeaderView.Stretch
        assert tab.table.horizontalHeader().sectionResizeMode(tab.COL_FREQ) == QHeaderView.ResizeToContents
        assert tab.new_source_btn.text() == "New Schedule"
        assert tab.rename_source_btn.text() == "Rename Schedule"
        assert tab.move_to_resources_btn.text() == "Save Selected to Library"
        assert tab.add_to_schedule_default_action.text() in {"Add Selected Rows", "Add Filtered Rows"}
        assert tab.add_selected_resource_action.text() == "Add Selected Rows"
        assert tab.add_filtered_resource_action.text() == "Add Filtered Rows"
        assert tab.manage_resources_default_action.text() == "Import/Export"
        assert tab.edit_resource_btn.text() == "Edit Library Row"
        assert tab.delete_resource_btn.text() == "Delete Library Rows"
        assert not tab.save_source_btn.isVisible()
        assert tab.delete_source_btn.text() == "Delete Schedule"
        assert tab.save_btn.text() == "Save Schedule"

        tab.resize(1400, 900)
        tab._update_net_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "wide"
        assert tab._net_action_layout.itemAtPosition(0, 2).widget() is tab.schedule_source_combo
        assert tab._net_action_layout.itemAtPosition(0, 5).widget() is tab.new_source_btn
        assert tab._net_action_layout.itemAtPosition(0, 6).widget() is tab.save_btn
        assert tab._net_action_layout.itemAtPosition(0, 7).widget() is tab.rename_source_btn
        assert tab._net_action_layout.itemAtPosition(0, 8).widget() is tab.delete_source_btn
        assert tab._net_action_layout.itemAtPosition(1, 3).widget() is tab.move_to_resources_btn
        assert tab._net_resource_filter_layout.itemAtPosition(0, 4).widget() is tab.add_to_schedule_btn
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_nets_time_toggle_preserves_canonical_utc_without_dirtying(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui import net_schedule_tab as net_mod

    settings = SettingsManager()
    settings.set("timezone", "America/Denver")
    settings.set("display_time_mode", "LOCAL")
    settings.set(
        "net_schedule",
        [
            {
                "day_utc": "Wednesday",
                "recurrence": "Weekly",
                "group_name": "MAGNET",
                "mode": "Digi",
                "band": "20M",
                "frequency": "14.115",
                "start_utc": "20:00",
                "end_utc": "21:00",
                "early_checkin": "0",
                "net_name": "UTC Safety Net",
                "auto_tune": False,
            }
        ],
    )

    monkeypatch.setattr(net_mod.NetScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_bootstrap_net_resources", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_load_resources_from_db", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resource_set_combo", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resources_table", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_schedule_net_sop_conflict_refresh", lambda self, **_kwargs: None)

    tab = net_mod.NetScheduleTab()
    try:
        assert tab._show_local is True
        assert tab._get_combo_value(0, tab.COL_DAY) == "Wednesday"
        assert tab._get_combo_value(0, tab.COL_START) == "14:00"
        assert tab._get_combo_value(0, tab.COL_END) == "15:00"
        assert tab._collect_rows()[0]["day_utc"] == "Wednesday"
        assert tab._collect_rows()[0]["start_utc"] == "20:00"
        assert tab._collect_rows()[0]["end_utc"] == "21:00"
        assert tab._dirty is False

        tab._toggle_time_view()
        app.processEvents()

        assert tab._show_local is False
        assert tab._get_combo_value(0, tab.COL_DAY) == "Wednesday"
        assert tab._get_combo_value(0, tab.COL_START) == "20:00"
        assert tab._get_combo_value(0, tab.COL_END) == "21:00"
        assert tab._raw_rows[0]["day_utc"] == "Wednesday"
        assert tab._raw_rows[0]["start_utc"] == "20:00"
        assert tab._raw_rows[0]["end_utc"] == "21:00"
        assert tab._collect_rows()[0]["day_utc"] == "Wednesday"
        assert tab._collect_rows()[0]["start_utc"] == "20:00"
        assert tab._collect_rows()[0]["end_utc"] == "21:00"
        assert tab._dirty is False

        tab._toggle_time_view()
        app.processEvents()

        assert tab._show_local is True
        assert tab._get_combo_value(0, tab.COL_DAY) == "Wednesday"
        assert tab._get_combo_value(0, tab.COL_START) == "14:00"
        assert tab._get_combo_value(0, tab.COL_END) == "15:00"
        assert tab._raw_rows[0]["day_utc"] == "Wednesday"
        assert tab._raw_rows[0]["start_utc"] == "20:00"
        assert tab._raw_rows[0]["end_utc"] == "21:00"
        assert tab._collect_rows()[0]["day_utc"] == "Wednesday"
        assert tab._collect_rows()[0]["start_utc"] == "20:00"
        assert tab._collect_rows()[0]["end_utc"] == "21:00"
        assert tab._dirty is False
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_daily_uses_compact_default_with_view_edit_details() -> None:
    source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")

    assert "COMPACT_VISIBLE_COLUMNS = frozenset(" in source
    assert 'self.view_edit_btn = QPushButton("View/Edit")' in source
    assert "self.view_edit_btn.setCheckable(True)" in source
    assert "self.view_edit_btn.toggled.connect(self._apply_compact_schedule_view)" in source
    assert "def _apply_compact_schedule_view" in source
    assert "self.table.setColumnHidden(col, not show_all and col not in self.COMPACT_VISIBLE_COLUMNS)" in source
    assert "Show all editable fields for the active HF schedule rows." in source
    assert "Hide advanced HF schedule fields for normal scanning." in source
    assert "self._daily_action_layout = QGridLayout()" in source
    assert "self._daily_resource_filter_layout = QGridLayout()" in source
    assert "(self.time_toggle_btn, 0, 0)" in source
    assert "def _update_daily_responsive_layout(self) -> None:" in source
    assert 'self.daily_schedule_scroll_area.setObjectName("dailyScheduleScrollArea")' in source
    assert "self.daily_schedule_scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "def _apply_daily_compact_table_sizing(self, *, compact: bool) -> None:" in source


def test_phase7_hf_daily_schedule_name_is_inline_editable() -> None:
    source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index('self.schedule_source_label = QLabel("HF Daily Schedule:")') : source.index("resources_header = QHBoxLayout()")]
    save_block = source[source.index("def _on_save_freqplanner_source_clicked") : source.index("def _confirm_rf_guard_source_update")]

    assert "self.schedule_source_combo.setEditable(True)" in build_block
    assert "Name or select a daily schedule" in build_block
    assert "self._current_freqplanner_source_name()" in save_block
    assert "_prompt_for_freqplanner_source_name" not in save_block
    assert 'return "HF Daily Schedule"' in source


def test_phase7_hf_daily_library_uses_age_not_raw_updated_timestamp() -> None:
    source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")
    table_block = source[source.index("self.resources_table.setHorizontalHeaderLabels") : source.index("self.resources_table.setEditTriggers")]
    populate_block = source[source.index("def _populate_schedule_resources_table") : source.index("def _update_schedule_resources_empty_state")]

    assert '"Schedule"' in table_block
    assert '"Age"' in table_block
    assert '"Updated (UTC)"' not in table_block
    assert "self._format_age_label(row.get(\"updated_utc\"))" in populate_block
    assert "item.setToolTip(raw_updated if raw_updated else" in populate_block


def test_phase7_hf_daily_view_edit_toggles_advanced_columns(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import daily_schedule_tab as daily_mod

    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_sop_panel_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_qsy_options", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_load_schedule", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_sop_profiles_panel", lambda self, **_kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_populate_schedule_resources_table", lambda self: None)

    tab = daily_mod.DailyScheduleTab()
    try:
        assert tab.view_edit_btn.text() == "View/Edit"
        assert tab.view_edit_btn.isCheckable() is True
        assert tab.view_edit_btn.isChecked() is False

        assert tab.table.isColumnHidden(tab.COL_SOURCE) is True
        assert tab.table.isColumnHidden(tab.COL_AUTOTUNE) is True
        assert tab.table.isColumnHidden(tab.COL_TARGET_SCOPE) is True
        assert tab.table.isColumnHidden(tab.COL_GROUP) is False
        assert tab.table.isColumnHidden(tab.COL_START) is False
        assert tab.table.isColumnHidden(tab.COL_END) is False
        assert tab.table.isColumnHidden(tab.COL_FREQ) is False
        assert tab.table.isColumnHidden(tab.COL_TARGET) is True

        tab.view_edit_btn.setChecked(True)
        app.processEvents()

        assert all(not tab.table.isColumnHidden(col) for col in range(tab.table.columnCount()))
        assert "Hide advanced" in tab.view_edit_btn.toolTip()

        tab.view_edit_btn.setChecked(False)
        app.processEvents()

        assert tab.table.isColumnHidden(tab.COL_SOURCE) is True
        assert tab.table.isColumnHidden(tab.COL_GROUP) is False
        assert tab.table.isColumnHidden(tab.COL_TARGET) is True
        assert "Show all editable" in tab.view_edit_btn.toolTip()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_daily_action_rows_reflow_at_compact_width(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import daily_schedule_tab as daily_mod

    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_sop_panel_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_qsy_options", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_load_schedule", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_sop_profiles_panel", lambda self, **_kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_populate_schedule_resources_table", lambda self: None)

    tab = daily_mod.DailyScheduleTab()
    try:
        tab.resize(1000, 800)
        tab._update_daily_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "compact"
        assert tab._daily_action_layout.itemAtPosition(0, 2).widget() is tab.schedule_source_combo
        assert tab._daily_action_layout.itemAtPosition(0, 4).widget() is tab.new_source_btn
        assert tab._daily_action_layout.itemAtPosition(0, 5).widget() is tab.rename_source_btn
        assert tab._daily_action_layout.itemAtPosition(0, 6).widget() is tab.save_source_btn
        assert tab._daily_action_layout.itemAtPosition(1, 3).widget() is tab.resources_resolve_btn
        assert tab._daily_resource_filter_layout.itemAtPosition(1, 0).widget() is tab.add_to_schedule_btn
        assert tab.schedule_source_label.text() == "HF Daily Schedule:"
        assert tab.new_source_btn.text() == "New Schedule"
        assert tab.rename_source_btn.text() == "Rename Schedule"
        assert tab.save_source_btn.text() == "Save / Update Schedule"
        assert tab.delete_source_btn.text() == "Delete Schedule"
        assert tab.save_btn.text() == "Assign with RF Guard"
        assert tab.resources_set_label.text() == "Library:"
        assert tab.resources_set_combo.itemText(0) == "All schedules"
        assert tab.resources_group_filter.placeholderText() == "Search schedule, group, band, time..."
        assert tab.add_to_schedule_default_action.text() == "Add Selected Rows"
        assert tab.add_selected_resource_action.text() == "Add Selected Rows"
        assert tab.add_filtered_resource_action.text() == "Add Filtered Rows"
        assert tab.move_to_resources_btn.isHidden() is True
        assert tab.resources_delete_btn.isHidden() is True
        assert tab.daily_schedule_scroll_area.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.table.horizontalHeader().sectionResizeMode(tab.COL_GROUP) == QHeaderView.Stretch
        assert tab.table.horizontalHeader().sectionResizeMode(tab.COL_FREQ) == QHeaderView.ResizeToContents

        tab.resize(1400, 900)
        tab._update_daily_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "wide"
        assert tab._daily_action_layout.itemAtPosition(0, 2).widget() is tab.schedule_source_combo
        assert tab._daily_action_layout.itemAtPosition(0, 5).widget() is tab.new_source_btn
        assert tab._daily_action_layout.itemAtPosition(0, 6).widget() is tab.rename_source_btn
        assert tab._daily_action_layout.itemAtPosition(0, 7).widget() is tab.save_source_btn
        assert tab._daily_action_layout.itemAtPosition(1, 3).widget() is tab.resources_resolve_btn
        assert tab._daily_resource_filter_layout.itemAtPosition(0, 4).widget() is tab.add_to_schedule_btn
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_daily_time_toggle_preserves_canonical_utc_without_dirtying(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import daily_schedule_tab as daily_mod

    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_sop_panel_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_qsy_options", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_load_schedule", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_sop_profiles_panel", lambda self, **_kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_populate_schedule_resources_table", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_populate_schedule_issues_table", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_update_suspend_state", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_highlight_time_conflicts", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_update_resource_action_state", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_sop_overlay_rows_in_table", lambda self, *args, **kwargs: None)

    tab = daily_mod.DailyScheduleTab()
    try:
        tab.settings.set("timezone", "America/Denver")
        tab.settings.set("display_time_mode", "LOCAL")
        tab._show_local = True
        tab.operating_groups = [
            {"group": "MAGNET", "band": "20M", "mode": "Digi", "frequency": "14.115"}
        ]
        tab._raw_schedule = [
            {
                "day_utc": "Wednesday",
                "group_name": "MAGNET",
                "mode": "Digi",
                "band": "20M",
                "frequency": "14.115",
                "start_utc": "20:00",
                "end_utc": "21:00",
                "auto_tune": False,
            }
        ]
        tab._saved_rows_signature = tab._hf_rows_signature(tab._raw_schedule)
        tab._rebuild_from_raw()
        tab._set_dirty(False)

        assert tab._get_combo_value(0, tab.COL_DAY) == "Wednesday"
        assert tab._get_text_value(0, tab.COL_START) == "14:00"
        assert tab._get_text_value(0, tab.COL_END) == "15:00"
        assert tab._collect_current_hf_rows_utc()[0]["day_utc"] == "Wednesday"
        assert tab._collect_current_hf_rows_utc()[0]["start_utc"] == "20:00"
        assert tab._collect_current_hf_rows_utc()[0]["end_utc"] == "21:00"
        assert tab._dirty is False

        tab._toggle_time_view()
        app.processEvents()

        assert tab._show_local is False
        assert tab._get_combo_value(0, tab.COL_DAY) == "Wednesday"
        assert tab._get_text_value(0, tab.COL_START) == "20:00"
        assert tab._get_text_value(0, tab.COL_END) == "21:00"
        assert tab._raw_schedule[0]["day_utc"] == "Wednesday"
        assert tab._raw_schedule[0]["start_utc"] == "20:00"
        assert tab._raw_schedule[0]["end_utc"] == "21:00"
        assert tab._collect_current_hf_rows_utc()[0]["day_utc"] == "Wednesday"
        assert tab._collect_current_hf_rows_utc()[0]["start_utc"] == "20:00"
        assert tab._collect_current_hf_rows_utc()[0]["end_utc"] == "21:00"
        assert tab._dirty is False

        tab._toggle_time_view()
        app.processEvents()

        assert tab._show_local is True
        assert tab._get_combo_value(0, tab.COL_DAY) == "Wednesday"
        assert tab._get_text_value(0, tab.COL_START) == "14:00"
        assert tab._get_text_value(0, tab.COL_END) == "15:00"
        assert tab._raw_schedule[0]["day_utc"] == "Wednesday"
        assert tab._raw_schedule[0]["start_utc"] == "20:00"
        assert tab._raw_schedule[0]["end_utc"] == "21:00"
        assert tab._collect_current_hf_rows_utc()[0]["day_utc"] == "Wednesday"
        assert tab._collect_current_hf_rows_utc()[0]["start_utc"] == "20:00"
        assert tab._collect_current_hf_rows_utc()[0]["end_utc"] == "21:00"
        assert tab._dirty is False
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_schedule_time_conversion_handles_local_day_boundaries(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import daily_schedule_tab as daily_mod
    from freqinout.gui import net_schedule_tab as net_mod

    monkeypatch.setattr(net_mod.NetScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_bootstrap_net_resources", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_load_resources_from_db", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resource_set_combo", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resources_table", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_schedule_net_sop_conflict_refresh", lambda self, **_kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_sop_panel_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_qsy_options", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_load_schedule", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_sop_profiles_panel", lambda self, **_kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_populate_schedule_resources_table", lambda self: None)

    net_tab = net_mod.NetScheduleTab()
    daily_tab = daily_mod.DailyScheduleTab()
    try:
        for tab in (net_tab, daily_tab):
            tab.settings.set("timezone", "America/Denver")
            assert tab._convert_day_time("Wednesday", "02:00", to_local=True) == ("Tuesday", "20:00")
            assert tab._convert_day_time("Tuesday", "20:00", to_local=False) == ("Wednesday", "02:00")
    finally:
        net_tab.deleteLater()
        daily_tab.deleteLater()
        app.processEvents()


def test_phase7_hf_nets_dirty_invalid_time_toggle_preserves_visible_edits(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import net_schedule_tab as net_mod

    monkeypatch.setattr(net_mod.NetScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_bootstrap_net_resources", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_load_resources_from_db", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resource_set_combo", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_refresh_resources_table", lambda self: None)
    monkeypatch.setattr(net_mod.NetScheduleTab, "_schedule_net_sop_conflict_refresh", lambda self, **_kwargs: None)

    tab = net_mod.NetScheduleTab()
    try:
        tab._add_row(
            {
                "day_utc": "Wednesday",
                "group_name": "MAGNET",
                "mode": "Digi",
                "band": "20M",
                "start_utc": "bad",
                "end_utc": "21:00",
                "net_name": "Partial Net",
            }
        )
        tab._set_dirty(True)
        before_show_local = tab._show_local
        before_rows = tab.table.rowCount()
        before_start = tab._get_combo_value(0, tab.COL_START)

        tab._toggle_time_view()
        app.processEvents()

        assert tab._show_local is before_show_local
        assert tab.table.rowCount() == before_rows
        assert tab._get_combo_value(0, tab.COL_START) == before_start
        assert tab._dirty is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_daily_dirty_partial_time_toggle_preserves_visible_edits(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import daily_schedule_tab as daily_mod

    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_sop_panel_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_qsy_options", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_load_schedule", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_sop_profiles_panel", lambda self, **_kwargs: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_populate_schedule_resources_table", lambda self: None)

    tab = daily_mod.DailyScheduleTab()
    try:
        tab._add_row()
        tab._set_dirty(True)
        before_show_local = tab._show_local
        before_rows = tab.table.rowCount()
        before_start = tab._get_text_value(0, tab.COL_START)

        tab._toggle_time_view()
        app.processEvents()

        assert tab._show_local is before_show_local
        assert tab.table.rowCount() == before_rows
        assert tab._get_text_value(0, tab.COL_START) == before_start
        assert tab._dirty is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_daily_resources_use_empty_state_instead_of_blank_table(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import daily_schedule_tab as daily_mod

    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_setup_sop_panel_timer", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_qsy_options", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_load_schedule", lambda self: None)
    monkeypatch.setattr(daily_mod.DailyScheduleTab, "_refresh_sop_profiles_panel", lambda self, **_kwargs: None)

    tab = daily_mod.DailyScheduleTab()
    try:
        tab._schedule_resource_rows = []
        tab._schedule_resource_token = ("empty",)
        tab._schedule_resource_view_token = None
        tab._populate_schedule_resources_table()
        app.processEvents()

        assert tab.resources_empty_label.isHidden() is False
        assert "No saved HF Daily schedules yet" in tab.resources_empty_label.text()
        assert tab.resources_table.isHidden() is True

        tab._schedule_resource_rows = [
            {
                "resource_set": "Custom",
                "day_utc": "ALL",
                "group_name": "MAGNET",
                "mode": "Digi",
                "band": "20M",
                "frequency": "14.115",
                "start_utc": "",
                "end_utc": "",
                "source": "manual",
            }
        ]
        tab._schedule_resource_token = ("one-row",)
        tab._schedule_resource_view_token = None
        tab._populate_schedule_resources_table()
        app.processEvents()

        assert tab.resources_empty_label.isHidden() is True
        assert tab.resources_table.isHidden() is False
        assert tab.resources_table.rowCount() == 1
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_hf_daily_hides_manual_copy_to_library_action() -> None:
    source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")

    assert 'self.move_to_resources_btn = QPushButton("Copy Selected to Library")' in source
    assert "self.move_to_resources_btn.setVisible(False)" in source
    assert "(self.move_to_resources_btn," not in source[source.index("def _arrange_daily_action_rows") : source.index("def _apply_schedule_table_height_hints")]
    assert "self.resources_delete_btn.setVisible(False)" in source


def test_phase7_hf_daily_assignment_action_points_to_rf_guard_flow() -> None:
    source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")

    assert 'self.header_title_label = QLabel("<h3>HF Frequency Schedule</h3>")' in source
    assert "def _update_header_title" in source
    assert "Radio shown for context only" in source
    assert 'self.save_btn = QPushButton("Assign with RF Guard")' in source
    assert "self.save_btn.clicked.connect(self._on_assign_with_rf_guard_clicked)" in source
    assert "self.save_btn.clicked.connect(self._save_schedule)" not in source
    assert "Save the blended Frequency Plan and assign that plan to radio(s)" in source


def test_phase7_hf_daily_library_includes_saved_schedule_rows() -> None:
    source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")
    loader = source[source.index("def _load_schedule_resource_rows") : source.index("def _load_manual_schedule_resource_rows")]

    assert "def _load_saved_schedule_resource_rows" in source
    assert "saved_schedule_rows = self._load_saved_schedule_resource_rows()" in loader
    assert "_load_manual_schedule_resource_rows()" not in loader
    assert "_load_sop_schedule_resource_rows(" not in loader
    assert "_load_sop_gap_resource_rows(" not in loader
    assert 'source_key": f"saved:{schedule_id}:{idx}"' in source
    assert '"source": "saved_schedule"' in source


def test_phase7_hf_peers_uses_view_edit_selected_row_wording() -> None:
    source = Path("freqinout/gui/peer_sched_tab.py").read_text(encoding="utf-8")

    assert 'self.edit_btn = QPushButton("View/Edit Selected Row")' in source
    assert 'self.edit_btn.setToolTip("View or edit the selected explicit row.")' in source
    assert 'self.edit_btn = QPushButton("Edit Selected Row")' not in source


def test_phase7_hf_peers_keeps_times_in_filter_row_not_title_header() -> None:
    source = Path("freqinout/gui/peer_sched_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("# Table")]
    header_block = build_block[build_block.index("header = QHBoxLayout()") : build_block.index("self.schedule_source_label")]
    action_block = build_block[build_block.index("self.schedule_source_label") : build_block.index("self.cleanup_label")]
    filter_block = build_block[build_block.index("self.callsign_filter_label") :]

    assert "header.addWidget(self.help_btn)" in header_block
    assert "header.addWidget(self.refresh_btn)" not in header_block
    assert "header.addWidget(self.tz_toggle_btn)" not in header_block
    assert "self._peer_action_layout = QGridLayout()" in action_block
    assert "(self.refresh_btn, 0, 3)" in source
    assert "self._peer_filter_layout = QGridLayout()" in filter_block
    assert "(self.tz_toggle_btn, 0, 9)" in source
    assert "(self.tz_toggle_btn, 1, 6)" in source
    assert "def _update_peer_responsive_layout(self) -> None:" in source
    assert 'self.peer_schedule_scroll_area.setObjectName("peerScheduleScrollArea")' in source
    assert "self.peer_schedule_scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "def _apply_peer_table_sizing(self, *, compact: bool) -> None:" in source


def test_phase7_hf_peers_action_rows_reflow_at_compact_width(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.peer_sched_tab import PeerSchedTab

    monkeypatch.setattr(PeerSchedTab, "_load_operator_meta", lambda self: None)
    monkeypatch.setattr(PeerSchedTab, "_load_data", lambda self: None)

    tab = PeerSchedTab()
    try:
        tab.resize(1000, 800)
        tab._update_peer_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "compact"
        assert tab._peer_action_layout.itemAtPosition(1, 0).widget() is tab.selected_row_label
        assert tab._peer_filter_layout.itemAtPosition(1, 0).widget() is tab.search_filter_label
        assert tab._peer_filter_layout.itemAtPosition(1, 6).widget() is tab.tz_toggle_btn
        assert tab.peer_schedule_scroll_area.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.table.horizontalHeader().sectionResizeMode(0) == QHeaderView.Stretch
        assert tab.table.horizontalHeader().sectionResizeMode(tab._overlap_col) == QHeaderView.Stretch
        assert tab.table.horizontalHeader().sectionResizeMode(5) == QHeaderView.ResizeToContents

        tab.resize(1400, 900)
        tab._update_peer_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "wide"
        assert tab._peer_action_layout.itemAtPosition(0, 4).widget() is tab.selected_row_label
        assert tab._peer_filter_layout.itemAtPosition(0, 6).widget() is tab.search_filter_label
        assert tab._peer_filter_layout.itemAtPosition(0, 9).widget() is tab.tz_toggle_btn
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_local_ncs_uses_empty_state_instead_of_blank_table(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.local_ncs_tab import LocalNCSTab

    monkeypatch.setattr(LocalNCSTab, "_restore_context", lambda self: None)
    monkeypatch.setattr(LocalNCSTab, "reload_operator_lookup", lambda self: None)
    monkeypatch.setattr(LocalNCSTab, "_load_checkins", lambda self: None)
    monkeypatch.setattr(LocalNCSTab, "_setup_timers", lambda self: None)

    tab = LocalNCSTab()
    try:
        tab._rows = []
        tab._populate_table([], target_id=None)
        app.processEvents()

        assert tab.empty_table_label.isHidden() is False
        assert "No local check-ins yet" in tab.empty_table_label.text()
        assert tab.table.isHidden() is True

        tab._rows = [
            {
                "id": 1,
                "checkin_utc": "2026-07-27T18:00:00Z",
                "callsign": "N1MAG",
                "name": "Bill",
                "city": "Denver",
                "state": "CO",
                "category": "Operator",
                "sitrep_status": "GREEN",
                "notes": "",
            }
        ]
        tab._populate_table(tab._rows, target_id=None)
        app.processEvents()

        assert tab.empty_table_label.isHidden() is True
        assert tab.table.isHidden() is False
        assert tab.table.rowCount() == 1

        tab.status_filter_combo.setCurrentText("RED")
        tab._apply_filters()
        app.processEvents()

        assert tab.empty_table_label.isHidden() is False
        assert "No local check-ins match the current filters" in tab.empty_table_label.text()
        assert tab.table.isHidden() is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_js8_ncs_uses_empty_state_instead_of_blank_checkins_table(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.js8call_net_control_tab import JS8CallNetControlTab

    monkeypatch.setattr(JS8CallNetControlTab, "_load_settings", lambda self: None)
    monkeypatch.setattr(JS8CallNetControlTab, "_setup_timer", lambda self: None)
    monkeypatch.setattr(JS8CallNetControlTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(JS8CallNetControlTab, "_setup_js8_rx_timer", lambda self: None)
    monkeypatch.setattr(JS8CallNetControlTab, "_update_suspend_state", lambda self: None)
    monkeypatch.setattr(JS8CallNetControlTab, "_refresh_auto_query_flags", lambda self: None)
    monkeypatch.setattr(JS8CallNetControlTab, "_refresh_qsy_options", lambda self: None)
    monkeypatch.setattr(
        JS8CallNetControlTab,
        "_lookup_operator_meta",
        lambda self, callsign: {"name": "Bill", "state": "CO", "grid": "DM79", "region": "R08"},
    )

    tab = JS8CallNetControlTab()
    try:
        assert tab.checkin_empty_label.isHidden() is False
        assert tab.checkin_table.isHidden() is True

        tab._checkins = {}
        tab._rebuild_checkin_table()
        app.processEvents()

        assert tab.checkin_empty_label.isHidden() is False
        assert "No JS8 check-ins yet" in tab.checkin_empty_label.text()
        assert tab.checkin_table.isHidden() is True

        tab._upsert_checkin("N1MAG", status="NEW", mode="JS8", snr=-8, offset=1200)
        app.processEvents()

        assert tab.checkin_empty_label.isHidden() is True
        assert tab.checkin_table.isHidden() is False
        assert tab.checkin_table.rowCount() == 1

        tab._checkins.clear()
        tab._checkin_rows.clear()
        tab._clear_table()
        app.processEvents()

        assert tab.checkin_empty_label.isHidden() is False
        assert tab.checkin_table.isHidden() is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_fldigi_ncs_uses_empty_state_instead_of_blank_roster(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab

    monkeypatch.setattr(FldigiNetControlTab, "_load_known_operators", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_apply_theme", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_setup_timers", lambda self: None)
    monkeypatch.setattr(FldigiNetControlTab, "_refresh_qsy_options", lambda self, *args, **kwargs: None)

    tab = FldigiNetControlTab()
    try:
        assert tab.roster_empty_label.isHidden() is False
        assert "No roster entries yet" in tab.roster_empty_label.text()
        assert tab.roster_table.isHidden() is True

        tab._roster_append_row("N1MAG", "Bill", "CO", "1RR", "TFC", "Local")
        app.processEvents()

        assert tab.roster_empty_label.isHidden() is True
        assert tab.roster_table.isHidden() is False
        assert tab.roster_table.rowCount() == 1

        tab._roster_clear()
        app.processEvents()

        assert tab.roster_empty_label.isHidden() is False
        assert tab.roster_table.isHidden() is True

        tab._roster_rebuild_rows([])
        app.processEvents()

        assert tab.roster_empty_label.isHidden() is False
        assert tab.roster_table.isHidden() is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_station_overview_uses_empty_state_for_empty_control_center(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.core.station_runtime_manager import DeviceRuntimeSnapshot
    from freqinout.gui.station_overview_tab import StationOverviewTab

    tab = StationOverviewTab()
    try:
        tab._refresh_control_center_table([])
        app.processEvents()

        assert tab.control_center_empty_label.isHidden() is False
        assert "No active station runtimes" in tab.control_center_empty_label.text()
        assert tab.control_center_table.isHidden() is True

        snapshot = DeviceRuntimeSnapshot(
            device_profile_id=1,
            name="icom",
            device_class="radio",
            control_backend="flrig",
            deployment_mode="local",
            runtime_active=True,
            runtime_primary=True,
            scheduler_owner=True,
            endpoint_summary="FLRig",
            ptt_group="",
            assigned_operating_profile_id=1,
            assigned_operating_profile_name="All Features",
            assignment_state="assigned",
            scheduler_enabled=True,
            use_messages=True,
            use_map=True,
            use_background_ingest=True,
            use_launch_control=False,
            use_net_control_tabs=True,
            control_ready=True,
            overall_state="ok",
            status_summary="Ready",
            warning_text="",
            ptt_active=False,
            shared_ptt_blocked=False,
            shared_ptt_owner_device_id=None,
            shared_ptt_owner_name="",
            shared_ptt_status_text="",
            observer_follow_source_device_id=None,
            observer_follow_source_name="",
            observer_follow_summary="",
            varac_cluster_name="",
            varac_cluster_id="",
            varac_instance_number=None,
            varac_gateway_handler=False,
            varac_gateway_handler_name="",
            varac_cluster_summary="",
            current_frequency_hz=14115000,
            current_frequency_label="14.115 MHz",
            current_band="20M",
            antenna_group="",
            frontend_group="",
            amplifier_group="",
            swap_role="",
            swap_summary="",
            service_states={"FLRig": {"state": "ok", "tooltip": "FLRig ready"}},
        )
        tab._refresh_control_center_table([snapshot])
        app.processEvents()

        assert tab.control_center_empty_label.isHidden() is True
        assert tab.control_center_table.isHidden() is False
        assert tab.control_center_table.rowCount() == 1
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_station_health_uses_empty_states_for_blank_tables(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    try:
        tab._last_summary = {
            "issue_count": 0,
            "severity": "ok",
            "items": [],
            "recent_scheduler_events": [],
        }
        tab._render_table()
        tab._render_scheduler_events()
        app.processEvents()

        assert tab.health_empty_label.isHidden() is False
        assert tab.table.isHidden() is True
        assert tab.scheduler_empty_label.isHidden() is False
        assert tab.scheduler_table.isHidden() is True

        tab._last_summary = {
            "issue_count": 1,
            "severity": "warning",
            "items": [
                {
                    "scope": "icom",
                    "dependency": "FLRig",
                    "state": "warn",
                    "action": "Backing off",
                    "last_issue": "timeout",
                    "issue_since": "21:00Z",
                    "cooldown": "30s",
                    "last_check": "21:01Z",
                    "last_duration": "2.1s",
                    "severity": "warning",
                }
            ],
            "recent_scheduler_events": [
                {
                    "ts_utc": "2026-07-27T21:00:00Z",
                    "code": "applied",
                    "source": "icom",
                    "action": "QSY",
                    "detail": "Applied scheduled frequency",
                    "frequency_hz": 14115000,
                    "band": "20M",
                    "_station_health_kind": "latest_success",
                }
            ],
        }
        tab._render_table()
        tab._render_scheduler_events()
        app.processEvents()

        assert tab.health_empty_label.isHidden() is True
        assert tab.table.isHidden() is False
        assert tab.table.rowCount() == 1
        assert tab.scheduler_empty_label.isHidden() is True
        assert tab.scheduler_table.isHidden() is False
        assert tab.scheduler_table.rowCount() == 1
    finally:
        tab._refresh_timer.stop()
        tab.deleteLater()
        app.processEvents()


def test_phase7_station_health_keeps_refresh_out_of_title_header() -> None:
    source = Path("freqinout/gui/station_health_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("def set_scope_resolver")]
    header_block = build_block[build_block.index("header = QHBoxLayout()") : build_block.index("action_row = QHBoxLayout()")]
    action_block = build_block[build_block.index("action_row = QHBoxLayout()") :]

    assert "header.addWidget(self.help_btn" in header_block
    assert "header.addWidget(self.refresh_btn)" not in header_block
    assert "action_row.addWidget(self.refresh_btn" in action_block


def test_phase7_station_health_accepts_runtime_observability_provider() -> None:
    source = Path("freqinout/gui/station_health_tab.py").read_text(encoding="utf-8")
    main_source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "set_runtime_item_provider" in source
    assert "extra_items=runtime_items" in source
    assert "job_status_snapshot()" in main_source
    assert "get_status_poll_metrics()" in main_source
    assert "JS8ApiClientRegistry.status_dicts()" in main_source
    assert "runtime_observability_items(" in main_source


def test_phase7_map_hides_legacy_visible_plan_context_prose() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("self._map_support_card = QFrame")]

    assert "self.plan_context_label = PlanContextLabel(" in build_block
    assert "self.plan_context_label.setVisible(False)" in build_block
    assert "layout.addWidget(self.plan_context_label)" in build_block


def test_phase7_logs_use_header_and_inline_filter_row_search() -> None:
    source = Path("freqinout/gui/log_viewer.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("def _update_font")]
    header_block = build_block[build_block.index("header = QHBoxLayout()") : build_block.index("self.refresh_btn")]
    filter_block = build_block[build_block.index("self.refresh_btn") :]
    search_block = source[source.index("def _search") :]

    assert 'title = QLabel("Logs / Diagnostics")' in header_block
    assert "header.addWidget(self.refresh_btn" not in header_block
    assert "self._log_filter_layout = QGridLayout()" in filter_block
    assert "(self.refresh_btn, 0, 0)" in source
    assert "(self.search_input, 1, 1, 1, 3)" in source
    assert "def _update_log_responsive_layout(self) -> None:" in source
    assert "self.search_input = QLineEdit()" in filter_block
    assert "self.search_input.returnPressed.connect(self._search)" in filter_block
    assert "QInputDialog.getText" not in search_block
    assert "QMessageBox.information" not in search_block


def test_phase7_logs_filter_row_reflows_at_compact_width(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.log_viewer import LogViewerTab

    tab = LogViewerTab()
    try:
        tab.timer.stop()
        tab.resize(1000, 800)
        tab._update_log_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "compact"
        assert tab._log_filter_layout.itemAtPosition(1, 0).widget() is tab.search_label
        assert tab._log_filter_layout.itemAtPosition(2, 0).widget() is tab.font_group

        tab.resize(1400, 900)
        tab._update_log_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "wide"
        assert tab._log_filter_layout.itemAtPosition(0, 3).widget() is tab.search_label
        assert tab._log_filter_layout.itemAtPosition(0, 6).widget() is tab.font_group
    finally:
        tab.timer.stop()
        tab.deleteLater()
        app.processEvents()


def test_phase7_freqplanner_moves_times_into_plan_workspace_and_hides_context() -> None:
    source = Path("freqinout/gui/freq_planner_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("self.frequency_plan_summary_label")]
    header_block = build_block[build_block.index("header = QHBoxLayout()") : build_block.index("layout.addLayout(header)")]
    plan_block = build_block[build_block.index("plan_workspace = QVBoxLayout()") :]

    assert "header.addWidget(self.time_toggle_btn)" not in header_block
    assert "self.plan_context_label.setVisible(False)" in build_block
    assert "view_workspace.addWidget(self.time_toggle_btn)" in plan_block
    assert "plan_select_row.addWidget(self.save_plan_btn)" in plan_block
    assert "source_workspace.addWidget(self.save_sop_plan_btn)" in plan_block
    assert "source_workspace.addWidget(self.build_sop_layer_btn)" in plan_block
    assert "view_workspace.addWidget(self.review_rf_guard_btn)" in plan_block
    assert "view_workspace.addWidget(self.assign_plan_btn)" in plan_block


def test_phase7_controlfreq_uses_filter_row_and_hides_context() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("header = QHBoxLayout()") : source.index("controlfreq_context_text = (")]
    header_block = build_block[build_block.index("header = QHBoxLayout()") : build_block.index("filter_row = QHBoxLayout()")]
    filter_block = build_block[build_block.index("filter_row = QHBoxLayout()") :]

    assert "header.addWidget(self.help_btn)" in header_block
    assert "header.addWidget(self.search_edit" not in header_block
    assert "header.addWidget(self.time_toggle_btn" not in header_block
    assert "filter_row.addWidget(self.search_edit" in filter_block
    assert "filter_row.addWidget(self.group_combo)" in filter_block
    assert "filter_row.addWidget(self.refresh_btn)" in filter_block
    assert "filter_row.addWidget(self.clear_filters_btn)" in filter_block
    assert "filter_row.addWidget(self.time_toggle_btn)" in filter_block
    assert "self.plan_context_label.setVisible(False)" in source


def test_phase7_controlfreq_has_responsive_card_layout_breakpoint() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert 'self.controlfreq_scroll.setObjectName("controlfreqScrollArea")' in source
    assert "self.controlfreq_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "self._responsive_layout_mode = \"wide\"" in source
    assert "self._responsive_compact_width = 1200" in source
    assert "self.top_overview_row = QHBoxLayout()" in source
    assert "def _controlfreq_responsive_mode_for_width" in source
    assert "def _update_responsive_layout(self) -> None:" in source
    assert "self.top_overview_row.setDirection(QBoxLayout.TopToBottom if compact else QBoxLayout.LeftToRight)" in source
    assert "self.top_splitter.setOrientation(Qt.Vertical if compact else Qt.Horizontal)" in source
    assert "freq_h = max(170, int(self.freq_ctrl_box.sizeHint().height()))" in source
    assert "(self.freq_ctrl_box, freq_h)" in source
    assert "if getattr(self, \"_responsive_layout_mode\", \"wide\") == \"wide\":" in source
    assert "if getattr(self, \"_responsive_layout_mode\", \"wide\") != \"wide\":" in source
    assert "QTimer.singleShot(0, self._update_responsive_layout)" in source


def test_phase7_controlfreq_restores_wide_splitter_sizes_after_compact(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QHeaderView

    from freqinout.gui import controlfreq_tab as controlfreq_mod

    monkeypatch.setattr(controlfreq_mod.ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)

    tab = controlfreq_mod.ControlFreqTab()
    try:
        tab.resize(1400, 900)
        tab.show()
        app.processEvents()

        tab.top_splitter.resize(1000, 300)
        tab._saved_top_sizes = [200, 800]

        tab.resize(1000, 800)
        tab._update_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "compact"
        assert tab.top_splitter.orientation() == Qt.Vertical

        tab.resize(1400, 900)
        tab._update_responsive_layout()
        app.processEvents()

        wide_sizes = tab.top_splitter.sizes()
        assert tab._responsive_layout_mode == "wide"
        assert tab.top_splitter.orientation() == Qt.Horizontal
        assert len(wide_sizes) == 2
        assert wide_sizes[1] > wide_sizes[0] * 2
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_controlfreq_compact_mode_scrolls_without_clipping_frequency_card(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from PySide6.QtCore import Qt

    from freqinout.gui import controlfreq_tab as controlfreq_mod

    monkeypatch.setattr(controlfreq_mod.ControlFreqTab, "_refresh_all", lambda self, *args, **kwargs: None)

    tab = controlfreq_mod.ControlFreqTab()
    try:
        tab.resize(1000, 650)
        tab._update_responsive_layout()
        app.processEvents()

        assert tab.controlfreq_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.controlfreq_scroll.widget() is tab.controlfreq_content
        assert tab._responsive_layout_mode == "compact"
        assert tab.top_overview_row.direction() == controlfreq_mod.QBoxLayout.TopToBottom
        assert tab.freq_ctrl_box.isVisible() is False
        assert tab.intersection_box.minimumHeight() >= 96
        assert tab.schedule_box.minimumHeight() >= 120
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_sop_moves_times_into_management_rows_and_hides_context() -> None:
    source = Path("freqinout/gui/sop_tab.py").read_text(encoding="utf-8")

    assert source.count("title_row.addWidget(self.time_toggle_btn)") == 0
    assert source.count("header.addWidget(self.time_toggle_btn)") == 2
    assert source.count("self.plan_context_label.setVisible(False)") >= 2
    assert "Operating Plan Inputs:" in source


def test_phase7_legacy_sop_accessibility_guard_tolerates_missing_versions_button(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.sop_tab import _LegacySOPTab

    tab = _LegacySOPTab()
    try:
        assert not hasattr(tab, "versions_btn")
        tab._apply_accessibility_width_guards()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_station_command_bar_is_global_context_not_command_execution() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    theme_source = Path("freqinout/gui/theme.py").read_text(encoding="utf-8")

    assert 'self.station_command_bar.setObjectName("stationCommandBar")' in source
    assert 'self.station_command_radio_label.setObjectName("stationCommandRadioLabel")' in source
    assert 'self.station_command_radio_combo.setObjectName("stationCommandRadioSelector")' in source
    assert 'self.station_command_now_caption = QLabel("Now")' in source
    assert 'self.station_command_now_label = ElidedLabel("Now: unavailable", self.station_command_bar)' in source
    assert 'self.station_command_action_label = QLabel("Action")' in source
    assert 'self.station_command_radio_separator.setFrameShape(QFrame.VLine)' in source
    assert 'self.station_command_now_separator.setFrameShape(QFrame.VLine)' in source
    assert 'self.station_command_qsy_btn.setObjectName("stationCommandQsy")' in source
    assert 'self.station_command_hold_btn.setObjectName("stationCommandHold")' in source
    assert 'self.station_command_suspend_btn.setObjectName("stationCommandSuspend")' in source
    assert 'self.station_command_resume_btn.setObjectName("stationCommandResume")' in source
    assert 'self.station_command_duration_combo.setObjectName("stationCommandDuration")' in source
    assert 'self.station_command_radio_summary_label = QLabel("Radios")' in source
    assert 'self.station_command_radio_summary_scroll = QScrollArea(self.station_command_bar)' in source
    assert "self.station_command_radio_summary_scroll.setWidgetResizable(False)" in source
    assert "self.station_command_radio_summary_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in source
    assert "self.station_command_radio_summary_widget.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)" in source
    assert "self.station_command_radio_summary_scroll.setWidget(self.station_command_radio_summary_widget)" in source
    assert 'self.station_command_radio_admin_btn = QPushButton("All Radios")' in source
    assert 'self.station_command_radio_admin_panel = QWidget(self.station_command_bar)' in source
    assert "def _refresh_station_command_radio_summary(self, choices: list[object], selected_id: int) -> None:" in source
    assert "def _refresh_station_command_radio_admin(self, choices: list[object], selected_id: int) -> None:" in source
    assert "self._refresh_station_command_radio_summary(choices, selected_id)" in source
    assert "self._refresh_station_command_radio_admin(choices, selected_id)" in source
    assert "refresh_hold_duration_combo(self.station_command_duration_combo, self.settings, self._active_runtime_profile)" in source
    assert "Duration for QSY Suspend." in source
    assert "self.station_command_layout = QGridLayout(self.station_command_bar)" in source
    assert "def _station_command_layout_mode_for_width(self, width: int) -> str:" in source
    assert "def _apply_station_command_bar_layout(self, *, force: bool = False) -> None:" in source
    assert 'return "compact" if int(width) < 1100 else "wide"' in source
    assert "layout.addWidget(self.station_command_now_caption, 0, 3)" in source
    assert "layout.addWidget(self.station_command_now_label, 0, 4)" in source
    assert "layout.addWidget(self.station_command_action_label, 0, 7)" in source
    assert "layout.addWidget(self.station_command_freq_combo, 0, 8, 1, 2)" in source
    assert "layout.addWidget(self.station_command_qsy_btn, 0, 10)" in source
    assert "layout.addWidget(self.station_command_suspend_btn, 0, 12)" in source
    assert "layout.addWidget(self.station_command_duration_combo, 1, 8, 1, 2)" in source
    assert "layout.addWidget(self.station_command_hold_btn, 1, 10)" in source
    assert "layout.addWidget(self.station_command_resume_btn, 1, 12)" in source
    assert "layout.addWidget(self.station_command_health_label, 1, 0)" in source
    assert "layout.addWidget(self.station_command_health_widget, 1, 1)" in source
    assert "layout.addWidget(self.station_command_next_label, 1, 3, 1, 7)" in source
    assert "layout.addWidget(self.station_command_radio_summary_label, 2, 0)" in source
    assert "layout.addWidget(self.station_command_radio_prev_btn, 2, 1)" in source
    assert "layout.addWidget(self.station_command_radio_summary_scroll, 2, 2, 1, 9)" in source
    assert "layout.addWidget(self.station_command_radio_next_btn, 2, 11)" in source
    assert "layout.addWidget(self.station_command_radio_admin_btn, 2, 12)" in source
    assert "layout.addWidget(self.station_command_radio_admin_panel, 3, 0, 1, 13)" in source
    assert "self.station_command_now_label.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)" in source
    assert "self.station_command_state_label.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)" in source
    assert 'self.station_command_health_label = QLabel("Health:")' in source
    assert 'label_text="Healthy"' in source
    assert 'summary_label = "Unhealthy" if summary_state == "error" else "Needs Review"' in source
    assert "self._apply_station_command_bar_layout(force=True)" in source
    assert "right_layout.addWidget(self.station_command_bar, 0)" in source
    assert "right_layout.addWidget(self.stack, stretch=1)" in source
    assert "right_layout.setSpacing(10)" in source
    assert "border-bottom: 2px solid" in source
    assert "station_control_surface" in source
    assert "station_control_border" in source
    assert "station_control_text" in source
    assert "station_control_muted" in source
    assert "QFrame#stationCommandBar QLabel" in source
    assert "QLabel#stationCommandNow" in source
    assert "background: transparent; color:" in source
    assert '"station_control_surface": "#D7EAF8"' in theme_source
    assert '"station_control_surface": "#12324A"' in theme_source
    assert "self._status_timer.timeout.connect(self._refresh_station_overview)" in source
    assert "self._refresh_station_command_bar(force=False)" in source
    assert "self.station_command_qsy_btn.setEnabled(can_qsy)" in source
    assert '"warning" if manual_qsy_active or scheduler_suspended_manual or timed_hold_active else "muted"' in source
    assert 'self.station_command_state_label.setText("Manual QSY")' in source
    assert "self.station_command_freq_combo = QComboBox" in source
    assert "def _on_station_command_qsy_now_clicked" in source
    assert "perform_qsy(self, meta)" in source
    assert "Command target:" in source
    assert "SUPPORTED_RUNTIME_CONTROL_BACKENDS" in source
    assert "def _station_command_configured_profiles" in source
    assert "def _station_command_is_controllable_profile" in source
    assert "S2/GHOSTNET" in source
    assert "No configured radios" in source
    assert 'device_class == "observer"' in source
    assert "self.station_command_qsy_btn.clicked.connect(self._on_station_command_qsy_now_clicked)" in source
    assert "self.station_command_hold_btn.clicked.connect(self._on_station_command_qsy_hold_clicked)" in source
    assert "self.station_command_suspend_btn.clicked.connect(self._on_station_command_pause_clicked)" in source
    assert "self.station_command_resume_btn.clicked.connect(self._on_station_command_resume_clicked)" in source
    assert "def _station_command_for_radio(" not in source
    assert "self._station_command_set_scheduler_suspended_manual(True, target_device_profile_id=target_id)" in source
    assert 'self.station_command_state_label.setText("Scheduler Suspended")' in source
    assert "def _on_station_command_health_clicked" in source
    assert "self.station_command_health_widget.setCursor(Qt.PointingHandCursor)" in source
    assert "self.station_command_health_widget.mousePressEvent = (" in source
    assert "self._on_station_command_health_clicked(event, anchor=widget)" in source


def test_main_window_compacts_rf_guard_unknown_peer_reason() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "def _rf_guard_status_reason" in source
    assert 'return f"RF Guard: verify {peer_name}"' in source
    assert 'return f"RF Guard: {peer_name}"' in source
    assert "rf_conflict_peer_status_unknown" in source
    assert "rf_conflict_peer_status_stale" in source


def test_main_window_surfaces_stale_companion_status_compactly() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "scheduler_companion_status = scheduler.get_status_summary(live=False, refresh=False)" in source
    assert "scheduler_companion_status=scheduler_companion_status" in source
    assert "js8_status_stale = bool(status.get(\"js8_status_stale\"))" in source
    assert "varac_status_stale = bool(status.get(\"varac_status_stale\"))" in source
    assert 'reasons.append("Verify JS8Call")' in source
    assert 'reasons.append("Verify VarAC")' in source


def test_phase7_station_command_bar_uses_planned_compact_layout(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.station_command_bar = QFrame()
    window.station_command_layout = QGridLayout(window.station_command_bar)
    window._station_command_layout_mode = ""
    window.station_command_radio_label = QLabel("Radio")
    window.station_command_radio_combo = QComboBox()
    window.station_command_radio_separator = QFrame()
    window.station_command_now_caption = QLabel("Now")
    window.station_command_now_label = QLabel("Now: unavailable")
    window.station_command_state_label = QLabel("State: On Schedule")
    window.station_command_now_separator = QFrame()
    window.station_command_action_label = QLabel("Action")
    window.station_command_freq_combo = QComboBox()
    window.station_command_next_label = QLabel("Next: No assigned plan")
    window.station_command_health_label = QLabel("Health:")
    window.station_command_health_widget = QWidget()
    window.station_command_health_layout = QHBoxLayout(window.station_command_health_widget)
    window.station_command_duration_combo = QComboBox()
    window.station_command_qsy_btn = QPushButton("QSY Now")
    window.station_command_hold_btn = QPushButton("QSY Suspend")
    window.station_command_suspend_btn = QPushButton("Suspend Scheduler")
    window.station_command_resume_btn = QPushButton("Resume Schedule")
    window.station_command_radio_summary_label = QLabel("Radios")
    window.station_command_radio_summary_scroll = QScrollArea()
    window.station_command_radio_summary_widget = QWidget()
    window.station_command_radio_summary_scroll.setWidget(window.station_command_radio_summary_widget)
    window.station_command_radio_summary_layout = QHBoxLayout(window.station_command_radio_summary_widget)
    window.station_command_radio_prev_btn = QPushButton("Prev")
    window.station_command_radio_next_btn = QPushButton("Next")
    window.station_command_radio_admin_btn = QPushButton("All Radios")
    window.station_command_radio_admin_panel = QWidget()
    window.station_command_radio_admin_layout = QVBoxLayout(window.station_command_radio_admin_panel)

    try:
        window.station_command_bar.resize(900, 120)
        MainWindow._apply_station_command_bar_layout(window, force=True)
        app.processEvents()

        assert window._station_command_layout_mode == "compact"
        assert window.station_command_layout.itemAtPosition(0, 2).widget() is window.station_command_now_caption
        assert window.station_command_layout.itemAtPosition(0, 3).widget() is window.station_command_now_label
        assert window.station_command_layout.itemAtPosition(0, 4).widget() is window.station_command_state_label
        assert window.station_command_layout.itemAtPosition(1, 0).widget() is window.station_command_action_label
        assert window.station_command_layout.itemAtPosition(1, 1).widget() is window.station_command_freq_combo
        assert window.station_command_layout.itemAtPosition(1, 3).widget() is window.station_command_qsy_btn
        assert window.station_command_layout.itemAtPosition(2, 1).widget() is window.station_command_duration_combo
        assert window.station_command_layout.itemAtPosition(1, 4).widget() is window.station_command_suspend_btn
        assert window.station_command_layout.itemAtPosition(2, 3).widget() is window.station_command_hold_btn
        assert window.station_command_layout.itemAtPosition(2, 4).widget() is window.station_command_resume_btn
        assert window.station_command_layout.itemAtPosition(3, 0).widget() is window.station_command_health_label
        assert window.station_command_layout.itemAtPosition(3, 1).widget() is window.station_command_health_widget
        assert window.station_command_layout.itemAtPosition(3, 2).widget() is window.station_command_next_label
        assert window.station_command_layout.itemAtPosition(4, 0).widget() is window.station_command_radio_summary_label
        assert window.station_command_layout.itemAtPosition(4, 1).widget() is window.station_command_radio_prev_btn
        assert window.station_command_layout.itemAtPosition(4, 2).widget() is window.station_command_radio_summary_scroll
        assert window.station_command_layout.itemAtPosition(4, 4).widget() is window.station_command_radio_next_btn
        assert window.station_command_layout.itemAtPosition(4, 5).widget() is window.station_command_radio_admin_btn
        assert window.station_command_layout.itemAtPosition(5, 0).widget() is window.station_command_radio_admin_panel

        window.station_command_bar.resize(1300, 120)
        MainWindow._apply_station_command_bar_layout(window)
        app.processEvents()

        assert window._station_command_layout_mode == "wide"
        assert window.station_command_layout.itemAtPosition(0, 2).widget() is window.station_command_radio_separator
        assert window.station_command_layout.itemAtPosition(0, 3).widget() is window.station_command_now_caption
        assert window.station_command_layout.itemAtPosition(0, 4).widget() is window.station_command_now_label
        assert window.station_command_layout.itemAtPosition(0, 5).widget() is window.station_command_state_label
        assert window.station_command_layout.itemAtPosition(0, 6).widget() is window.station_command_now_separator
        assert window.station_command_layout.itemAtPosition(0, 7).widget() is window.station_command_action_label
        assert window.station_command_layout.itemAtPosition(0, 8).widget() is window.station_command_freq_combo
        assert window.station_command_layout.itemAtPosition(0, 10).widget() is window.station_command_qsy_btn
        assert window.station_command_layout.itemAtPosition(0, 12).widget() is window.station_command_suspend_btn
        assert window.station_command_layout.itemAtPosition(1, 8).widget() is window.station_command_duration_combo
        assert window.station_command_layout.itemAtPosition(1, 10).widget() is window.station_command_hold_btn
        assert window.station_command_layout.itemAtPosition(1, 12).widget() is window.station_command_resume_btn
        assert window.station_command_layout.itemAtPosition(1, 0).widget() is window.station_command_health_label
        assert window.station_command_layout.itemAtPosition(1, 1).widget() is window.station_command_health_widget
        assert window.station_command_layout.itemAtPosition(1, 3).widget() is window.station_command_next_label
        assert window.station_command_layout.itemAtPosition(2, 0).widget() is window.station_command_radio_summary_label
        assert window.station_command_layout.itemAtPosition(2, 1).widget() is window.station_command_radio_prev_btn
        assert window.station_command_layout.itemAtPosition(2, 2).widget() is window.station_command_radio_summary_scroll
        assert window.station_command_layout.itemAtPosition(2, 11).widget() is window.station_command_radio_next_btn
        assert window.station_command_layout.itemAtPosition(2, 12).widget() is window.station_command_radio_admin_btn
        assert window.station_command_layout.itemAtPosition(3, 0).widget() is window.station_command_radio_admin_panel

        window._station_command_multi_mode_active = True
        MainWindow._apply_station_command_bar_layout(window, force=True)
        app.processEvents()

        assert window.station_command_layout.itemAtPosition(0, 0).widget() is window.station_command_radio_summary_scroll
        assert window.station_command_radio_summary_scroll.isHidden() is False
        assert window.station_command_radio_combo.isVisible() is False
        assert window.station_command_radio_prev_btn.isVisible() is False
        assert window.station_command_radio_next_btn.isVisible() is False
    finally:
        window.station_command_bar.deleteLater()
        app.processEvents()


def test_phase7_station_command_health_collapses_all_green(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.settings = SettingsManager()
    window.station_command_health_widget = QWidget()
    window.station_command_health_layout = QHBoxLayout(window.station_command_health_widget)
    window.station_command_health_label = QLabel("Health:")
    window.station_command_health_leds = {}
    window.station_command_health_text_labels = {}
    window.dependency_status_service = SimpleNamespace(
        software_status_snapshot=lambda: {
            "FLRig": {"state": "ok", "tooltip": "FLRig OK"},
            "FLDigi": {"state": "ok", "tooltip": "FLDigi OK"},
        }
    )
    monkeypatch.setattr(
        MainWindow,
        "_station_command_health_items",
        lambda self, profile: [("FLRig", "FLRig"), ("FLDigi", "FLDigi")],
    )

    try:
        MainWindow._refresh_station_command_health(window, {"id": 1}, 1)
        app.processEvents()

        assert set(window.station_command_health_text_labels) == {"__summary__"}
        assert window.station_command_health_text_labels["__summary__"].text() == "Healthy"
    finally:
        window.station_command_health_widget.deleteLater()
        app.processEvents()


def test_phase7_station_command_health_snapshot_idle_configured_app_is_not_green(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeStore:
        def list_device_profiles(self):
            return [
                {
                    "id": 8,
                    "name": "FIO-A",
                    "enabled": 1,
                    "runtime_active": 1,
                    "control_backend": "flrig",
                    "use_flrig": 1,
                    "use_js8call": 1,
                }
            ]

    window = MainWindow.__new__(MainWindow)
    window.settings = SimpleNamespace(all=lambda: {})
    window.multi_radio_store = FakeStore()
    window._station_command_off_schedule_by_radio = {}
    window._station_command_assignment_rf_guard_issues = lambda _profile: []
    window.dependency_status_service = SimpleNamespace(
        software_status_snapshot=lambda: (_ for _ in ()).throw(AssertionError("global status should not be used"))
    )
    snapshot = SimpleNamespace(
        device_profile_id=8,
        name="FIO-A",
        control_backend="flrig",
        control_ready=False,
        status_summary="FLRig control unavailable",
        service_states={
            "FLRig": {"state": "idle", "tooltip": "FLRig is not running.", "running": False},
            "JS8Call_API": {"state": "ok", "tooltip": "JS8 API reachable", "running": True},
        },
    )

    summary = MainWindow._station_command_health_summary_for_profile(window, snapshot)

    assert summary["state"] == "warn"
    assert summary["label"] == "Needs Review"
    assert ("FLRig", "FLRig", "warn", "FLRig is not running.") in summary["issues"]

    app.processEvents()


def test_phase7_station_command_health_monitor_flag_filters_unchecked_apps(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.settings = SimpleNamespace(
        get=lambda key, default=None: [
            {"name": "FLMsg", "enabled": False, "startup": False},
            {"name": "JS8Call", "enabled": True, "startup": False},
        ]
        if key == "launch_control_items"
        else default
    )

    items = MainWindow._station_command_health_monitored_items(
        window,
        [("FLMsg", "FLMsg"), ("JS8Call_API", "JS8"), ("FLRig", "FLRig")],
    )

    assert items == [("JS8Call_API", "JS8"), ("FLRig", "FLRig")]


def test_phase7_station_command_health_shows_only_unhealthy_components(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.settings = SettingsManager()
    window.station_command_health_widget = QWidget()
    window.station_command_health_layout = QHBoxLayout(window.station_command_health_widget)
    window.station_command_health_label = QLabel("Health:")
    window.station_command_health_leds = {}
    window.station_command_health_text_labels = {}
    window.dependency_status_service = SimpleNamespace(
        software_status_snapshot=lambda: {
            "FLRig": {"state": "ok", "tooltip": "FLRig OK"},
            "FLDigi": {"state": "warn", "tooltip": "FLDigi not reachable"},
            "JS8Call_API": {"state": "error", "tooltip": "JS8 TCP failed"},
        }
    )
    monkeypatch.setattr(
        MainWindow,
        "_station_command_health_items",
        lambda self, profile: [
            ("FLRig", "FLRig"),
            ("FLDigi", "FLDigi"),
            ("JS8Call_API", "JS8"),
        ],
    )

    try:
        MainWindow._refresh_station_command_health(window, {"id": 1}, 1)
        app.processEvents()

        labels = {key: label.text() for key, label in window.station_command_health_text_labels.items()}
        assert labels == {
            "__summary__": "Unhealthy",
        }
    finally:
        window.station_command_health_widget.deleteLater()
        app.processEvents()


def test_phase7_station_command_health_summary_opens_quick_menu() -> None:
    from freqinout.gui.main_window import MainWindow

    class FakeEvent:
        def __init__(self):
            self.accepted = False

        def accept(self):
            self.accepted = True

    window = MainWindow.__new__(MainWindow)
    window._station_command_selected_profile_id = 42
    shown = []
    window._show_station_command_health_menu = lambda **kwargs: shown.append(kwargs)

    event = FakeEvent()
    MainWindow._on_station_command_health_clicked(window, event)

    assert shown == [{"device_profile_id": 42, "anchor": None}]
    assert event.accepted is True


def test_phase7_sidebar_schedule_actions_removed_from_ledge() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "self.scheduler_status_container.setVisible(False)" in source
    assert "status_layout.addLayout(hold_row)" not in source
    assert "status_layout.addWidget(self.suspend_schedule_btn" not in source
    assert "status_layout.addWidget(self.resume_schedule_btn" not in source
    assert "def _hide_sidebar_schedule_controls(self) -> None:" in source
    assert "self._hide_sidebar_schedule_controls()" in source
    assert 'QPushButton("Suspend", self.scheduler_status_container)' in source
    assert 'QPushButton("Resume Schedule", self.scheduler_status_container)' in source


def test_phase7_controlfreq_body_operating_status_is_retired() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert 'self.status_group = QGroupBox("Operating Status")' in source
    assert "updated_row.addWidget(self.status_group)" not in source
    assert "the global Station Command Bar owns status visibility" in source
    assert "self.status_group.setVisible(True)" not in source


def test_phase7_controlfreq_setup_banner_only_auto_shows_required_setup() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    update_block = source[
        source.index("def _update_readiness_review_banner")
        : source.index("def on_condition_levels_changed")
    ]

    assert 'getattr(report, "required_count", 0)' in update_block
    assert update_block.index('getattr(report, "required_count", 0)') < update_block.index(
        "should_show_startup_review("
    )
    assert "self.readiness_review_widget.setVisible(False)" in update_block


def test_phase7_dropdown_checklist_summarizes_multi_select(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.dropdown_checklist import DropdownChecklist

    widget = DropdownChecklist("Sources")
    try:
        widget.set_options([("js8", "JS8Call"), ("varac", "VarAC"), ("spotter", "JS8Spotter")])

        assert widget.text() == "Sources: All"
        assert widget.all_selected() is True

        widget.set_selected_values(["js8", "spotter"])

        assert widget.text() == "Sources: 2 selected"
        assert widget.selected_values() == {"js8", "spotter"}
        assert widget.all_selected() is False

        widget.set_selected_values([])

        assert widget.text() == "Sources: None"
        assert widget.selected_values() == set()
    finally:
        widget.deleteLater()
        app.processEvents()


def test_phase7_dropdown_checklist_supports_grouped_sections(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.dropdown_checklist import DropdownChecklist

    widget = DropdownChecklist("Operating Group")
    try:
        widget.set_grouped_options(
            [
                ("Configured Groups", [("MAGNET", "MAGNET (Configured)")]),
                ("Other CommStat Groups", [("MR08", "MR08 (Other CommStat)"), ("MR09", "MR09 (Other CommStat)")]),
            ]
        )

        header_widgets = [
            action.defaultWidget()
            for action in widget.menu().actions()
            if hasattr(action, "defaultWidget") and action.defaultWidget() is not None
        ]
        assert any("Configured Groups" in child.text() for w in header_widgets for child in w.findChildren(QLabel))
        assert any("Other CommStat Groups" in child.text() for w in header_widgets for child in w.findChildren(QLabel))
        other_buttons = [
            child
            for w in header_widgets
            for child in w.findChildren(QPushButton)
            if child.text() in {"All", "None"}
        ]
        assert {btn.text() for btn in other_buttons} == {"All", "None"}
        clear_other = next(btn for btn in other_buttons if btn.text() == "None")
        assert widget.text() == "Operating Group: All"
        assert widget.selected_values() == {"MAGNET", "MR08", "MR09"}

        clear_other.click()

        assert widget.selected_values() == {"MAGNET"}
        assert widget.text() == "Operating Group: 1 selected"
    finally:
        widget.deleteLater()
        app.processEvents()


def test_phase7_messages_workspace_filters_are_below_title_without_context_sentence() -> None:
    source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert 'self.inbox_controls_panel.setObjectName("messagesInboxControlPanel")' in source
    assert 'self.inbox_controls_scroll.setObjectName("messagesInboxControlScroll")' in source
    assert 'inbox_body.setObjectName("messagesInboxBody")' in source
    assert "MESSAGE_INBOX_BODY_MIN_WIDTH = 900" in source
    assert "inbox_body.setMinimumWidth(MESSAGE_INBOX_BODY_MIN_WIDTH)" in source
    assert "inbox_body.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Preferred)" in source
    assert 'self.inbox_body_scroll.setObjectName("messagesInboxBodyScroll")' in source
    assert "self.inbox_body_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in source
    assert "self.inbox_body_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "self.messages_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in source
    assert "self.inbox_controls_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in source
    assert "self.inbox_controls_panel.setMinimumHeight(max(420, int(layout.sizeHint().height())))" in source
    assert 'self.operating_group_filter = DropdownChecklist("")' in source
    assert 'self.source_filter = DropdownChecklist("Source")' in source
    assert 'self._make_combo_searchable(self.type_filter, "Message Type")' in source
    assert 'self.operating_group_filter.setObjectName("messageOperatingGroupFilter")' in source
    assert 'self.source_filter.setObjectName("messageSourceFilter")' in source
    assert 'self.message_funnel_widget.setObjectName("messageInboxFunnelBar")' in source
    assert "MESSAGE_INBOX_FUNNEL_MIN_WIDTH = 0" in source
    assert "self.message_funnel_widget.setMinimumWidth(MESSAGE_INBOX_FUNNEL_MIN_WIDTH)" in source
    assert "self.message_funnel_widget.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Fixed)" in source
    assert "funnel_layout.addWidget(self.operating_group_filter, 2)" in source
    assert "funnel_layout.addWidget(self.source_filter, 2)" in source
    assert "self.received_filter.setFixedWidth(150)" in source
    assert "self.advanced_filters_btn.setFixedWidth(125)" in source
    assert "funnel_layout.addWidget(self.received_filter)" in source
    assert "funnel_layout.addWidget(self.advanced_filters_btn)" in source
    assert 'self.map_context_filter_label.setObjectName("messageMapContextFilterLabel")' in source
    assert "self._inbox_actions_layout = QGridLayout()" in source
    assert "(self.inbox_actions_heading, 0, 0, 1, 2)" in source
    assert '(self.inbox_actions_heading = QLabel("Inbox Tools")' not in source
    assert 'self.inbox_actions_heading = QLabel("Inbox Tools")' in source
    assert 'self.more_actions_btn = QPushButton("More Actions")' in source
    assert 'self.more_actions_menu.addAction("Select Matching Rows")' not in source
    assert 'self.more_actions_menu.addMenu("Select Older Than")' not in source
    assert 'self.more_actions_menu.addAction("Clear Selection")' not in source
    assert 'self.more_actions_menu.addAction("Delete Selected")' not in source
    assert "(self.refresh_btn, 1, 0)" in source
    assert "(self.message_check_combo, 1, 1)" in source
    assert "(self.type_filter, 4, 1)" in source
    assert "(self.operating_group_filter, 12, 0, 1, 2)" not in source
    assert "(self.source_filter, 13, 0, 1, 2)" not in source
    assert "(self.advanced_filters_btn, 6, 0, 1, 2)" not in source
    assert "MESSAGE_INBOX_FOCUS_MIN_WIDTH = 680" in source
    assert "self.inbox_focus_widget.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Fixed)" in source
    assert "self.inbox_focus_widget.setVisible(not compose_active)" in source
    assert "self.message_funnel_widget.setVisible(not compose_active)" in source
    assert "self.messages_header.setVisible(False)" in source
    assert 'fallback_text="Messages uses the current radio and Frequency Plan context' not in source
    assert "self.plan_context_label.setVisible(False)" in source
    assert "def _row_matches_workspace_filters" in source
    assert "def _message_source_options" in source
    assert "def _message_group_options" in source
    assert "def _update_messages_responsive_layout(self) -> None:" in source
    assert "self.compose_splitter.setOrientation(Qt.Vertical if (compact or compose_sidebar) else Qt.Horizontal)" in source


def test_phase7_ui_layout_standard_requires_minimized_scrollable_controls() -> None:
    spec = Path("docs/internal/ui_layout_standards.md").read_text(encoding="utf-8")

    assert "Minimized Window Usability" in spec
    assert "Action and filter panels use vertical scrolling" in spec
    assert "Buttons, combo boxes, text fields, and labels keep a readable minimum height" in spec
    assert "900x560" in spec


def test_phase7_hf_schedule_tabs_hide_context_sentence_and_pull_times_into_actions() -> None:
    daily_source = Path("freqinout/gui/daily_schedule_tab.py").read_text(encoding="utf-8")
    nets_source = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")

    assert "HF Schedule uses the current radio and Frequency Plan context" not in daily_source
    assert "Net Schedules uses the current radio and Frequency Plan context" not in nets_source
    assert "self.plan_context_label.setVisible(False)" in daily_source
    assert "self.plan_context_label.setVisible(False)" in nets_source
    assert "self._daily_action_layout = QGridLayout()" in daily_source
    assert "(self.time_toggle_btn, 0, 0)" in daily_source
    assert "self._net_action_layout = QGridLayout()" in nets_source
    assert "(self.time_toggle_btn, 0, 0)" in nets_source
    assert "layout.setSpacing(10)" in daily_source
    assert "layout.setSpacing(10)" in nets_source


def test_phase7_messages_workspace_filters_source_and_group(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import message_viewer_tab as msg_mod

    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_js8_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_pending_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_bbs_auto_archive_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_initial_refresh", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_refresh_compose_forms", lambda self: None)

    tab = msg_mod.MessageViewerTab()
    try:
        assert tab.inbox_controls_panel.objectName() == "messagesInboxControlPanel"
        assert tab.inbox_controls_scroll.objectName() == "messagesInboxControlScroll"
        assert tab.inbox_body.objectName() == "messagesInboxBody"
        assert tab.inbox_body.minimumWidth() >= 900
        assert tab.inbox_body_scroll.objectName() == "messagesInboxBodyScroll"
        assert tab.inbox_body_scroll.widget() is tab.inbox_body
        assert tab.inbox_body_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.inbox_body_scroll.verticalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.message_funnel_widget.objectName() == "messageInboxFunnelBar"
        assert tab.inbox_controls_scroll.widget() is tab.inbox_controls_panel
        assert tab.inbox_controls_scroll.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.inbox_controls_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.messages_table.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.inbox_body.minimumWidth() >= 900
        assert tab.inbox_body.sizePolicy().horizontalPolicy() == QSizePolicy.MinimumExpanding
        assert tab.message_funnel_widget.minimumWidth() >= 0
        assert tab.message_funnel_widget.sizePolicy().horizontalPolicy() == QSizePolicy.MinimumExpanding
        assert tab.inbox_focus_widget.minimumWidth() >= 680
        assert tab.inbox_focus_widget.sizePolicy().horizontalPolicy() == QSizePolicy.MinimumExpanding
        assert tab.map_context_filter_label.objectName() == "messageMapContextFilterLabel"
        assert tab.map_context_filter_label.isVisible() is False
        assert tab.compose_open_source_btn.text() == "Open Form Folder"
        assert tab.type_filter.isEditable()
        assert tab.messages_header.isVisible() is False
        assert tab.show_all_message_groups_chk.isCheckable()
        assert tab.show_all_message_groups_chk.text() == "Configured"

        tab.show_compose_from_navigation()
        app.processEvents()
        assert tab.messages_mode_stack.currentWidget() is tab.compose_page

        tab.show_inbox_from_navigation()
        app.processEvents()
        assert tab.messages_mode_stack.currentWidget() is tab.inbox_page

        rows = [
            msg_mod.UnifiedMessage("JS8 MSG", "NEW", "A", "B", 3, "3", "JS8", "js8", SimpleNamespace()),
            msg_mod.UnifiedMessage(
                "SitRep",
                "INFO",
                "C",
                "D",
                2,
                "2",
                "SitRep",
                "sitrep",
                SimpleNamespace(report_group="HF NETS"),
            ),
            msg_mod.UnifiedMessage("VarAC", "READ", "E", "F", 1, "1", "VarAC", "varac", SimpleNamespace()),
        ]
        tab._message_rows = rows
        tab._refresh_message_filters(rows)

        assert tab.source_filter.text() == "Source: All"
        assert tab.operating_group_filter.text() == "All"
        assert tab._is_filter_active() is False
        assert tab._filters_active() is False
        tab.show_all_message_groups_chk.setChecked(True)
        app.processEvents()
        assert tab.show_all_message_groups_chk.text() == "All Groups"
        source_values = {value for value, _label in tab._message_source_options([])}
        assert {"js8", "varac"} <= source_values
        assert tab._row_matches_workspace_filters(rows[0]) is True

        tab.source_filter.set_selected_values(["sitrep"])

        assert tab._row_matches_workspace_filters(rows[0]) is False
        assert tab._row_matches_workspace_filters(rows[1]) is True

        tab.source_filter.set_selected_values([])

        assert tab._row_matches_workspace_filters(rows[1]) is False

        tab.source_filter.set_selected_values(["sitrep"])
        tab.operating_group_filter.set_selected_values(["HF NETS"])

        assert tab._is_filter_active() is True
        assert tab._filters_active() is True
        assert tab._row_matches_workspace_filters(rows[1]) is True
        assert tab._row_matches_workspace_filters(rows[2]) is False

        tab._clear_filters()

        assert tab.source_filter.all_selected() is True
        assert tab.operating_group_filter.all_selected() is True
        assert tab.show_all_message_groups_chk.text() == "Configured"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_messages_group_filter_prioritizes_configured_groups_without_callsigns(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import message_viewer_tab as msg_mod

    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_js8_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_pending_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_bbs_auto_archive_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_initial_refresh", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_refresh_compose_forms", lambda self: None)

    tab = msg_mod.MessageViewerTab()
    try:
        monkeypatch.setattr(tab, "_configured_message_group_names", lambda: {"MAGNET", "S2 UNDERGROUND"})
        monkeypatch.setattr(
            tab,
            "_commstat_group_state",
            lambda: SimpleNamespace(
                active_groups=["MR08"],
                configured_groups=["MR08", "MR09", "W0IFM", "W9BVM"],
            ),
        )
        rows = [
            msg_mod.UnifiedMessage("SitRep", "INFO", "K7ETC", "MAGNET", 5, "5", "CommStat", "commstat", SimpleNamespace()),
            msg_mod.UnifiedMessage("SitRep", "INFO", "K7ETC", "MR08", 4, "4", "CommStat", "commstat", SimpleNamespace()),
            msg_mod.UnifiedMessage("SitRep", "INFO", "K7ETC", "MR09", 3, "3", "CommStat", "commstat", SimpleNamespace()),
            msg_mod.UnifiedMessage("SitRep", "INFO", "K7ETC", "W0IFM", 2, "2", "CommStat", "commstat", SimpleNamespace()),
            msg_mod.UnifiedMessage("SitRep", "INFO", "K7ETC", "W9BVM>", 1, "1", "CommStat", "commstat", SimpleNamespace()),
        ]
        tab._message_rows = rows
        tab._inbox_focus = "commstat"
        tab._refresh_workspace_filter_options(rows)

        focused_sections = tab._message_group_option_sections(rows)
        assert focused_sections[0][0] == "Configured Groups"
        assert focused_sections[0][1] == [("MAGNET", "MAGNET")]
        assert focused_sections[1] == ("CommStat Active Groups", [("MR08", "MR08")])
        focused_options = {value for _section, options in focused_sections for value, _label in options}
        assert "S2 UNDERGROUND" not in focused_options
        assert "MR09" not in focused_options
        assert "W0IFM" not in focused_options
        assert "W9BVM" not in focused_options

        tab.show_all_message_groups_chk.setChecked(True)
        app.processEvents()

        expanded_sections = tab._message_group_option_sections(rows)
        expanded_options = {value for _section, options in expanded_sections for value, _label in options}
        assert "MR09" in expanded_options
        assert "W0IFM" not in expanded_options
        assert "W9BVM" not in expanded_options
        assert tab.operating_group_filter.selected_values() == {"MAGNET", "MR08"}
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_messages_filter_row_and_compose_splitter_reflow(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    app = QApplication.instance() or QApplication([])

    from PySide6.QtCore import Qt

    from freqinout.gui import message_viewer_tab as msg_mod

    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_clock_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_js8_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_pending_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_setup_bbs_auto_archive_timer", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_initial_refresh", lambda self: None)
    monkeypatch.setattr(msg_mod.MessageViewerTab, "_refresh_compose_forms", lambda self: None)

    tab = msg_mod.MessageViewerTab()
    try:
        tab.resize(1000, 800)
        tab._update_messages_responsive_layout()
        app.processEvents()

        compact_layout = tab._inbox_actions_layout
        assert tab._responsive_layout_mode == "compact"
        assert compact_layout.itemAtPosition(0, 0).widget() is tab.inbox_actions_heading
        assert tab.inbox_actions_heading.text() == "Inbox Tools"
        assert tab.more_actions_btn.text() == "More Actions"
        assert compact_layout.itemAtPosition(1, 0).widget() is tab.refresh_btn
        assert compact_layout.itemAtPosition(1, 1).widget() is tab.message_check_combo
        assert compact_layout.itemAtPosition(4, 1).widget() is tab.type_filter
        assert compact_layout.itemAtPosition(10, 0).widget() is tab.inbox_bbs_heading
        assert compact_layout.itemAtPosition(13, 0).widget() is tab.inbox_bbs_summary_label
        assert tab.mark_all_read_btn.parentWidget() is None
        assert tab.advanced_filters_btn.parentWidget() is tab.message_funnel_widget
        assert tab.operating_group_filter.parentWidget() is tab.message_funnel_widget
        assert tab.source_filter.parentWidget() is tab.message_funnel_widget
        assert tab.compose_splitter.orientation() == Qt.Vertical
        tab._set_message_table_display_profile("field_report")
        header = tab.messages_table.horizontalHeader()
        assert header.sectionResizeMode(1) == QHeaderView.Stretch
        assert header.sectionResizeMode(6) == QHeaderView.Interactive
        assert tab.messages_table.columnWidth(1) >= 280
        assert tab.messages_table.columnWidth(5) <= 120
        assert tab.messages_table.columnWidth(6) <= 90
        assert tab.messages_table.minimumWidth() >= 930

        tab.resize(900, 560)
        tab.show()
        tab._update_messages_responsive_layout()
        app.processEvents()
        assert tab.inbox_controls_panel.minimumHeight() >= 420
        assert tab.inbox_controls_scroll.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.inbox_body_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.inbox_body.minimumWidth() >= 900
        assert tab.inbox_body.sizePolicy().horizontalPolicy() == QSizePolicy.MinimumExpanding
        assert tab.message_funnel_widget.minimumWidth() >= 0
        assert tab.message_funnel_widget.sizePolicy().horizontalPolicy() == QSizePolicy.MinimumExpanding
        assert tab.inbox_focus_widget.minimumWidth() >= 680
        assert tab.inbox_focus_widget.sizePolicy().horizontalPolicy() == QSizePolicy.MinimumExpanding
        assert tab.operating_group_filter.minimumWidth() >= 160
        assert tab.source_filter.minimumWidth() >= 160
        assert tab.show_all_message_groups_chk.minimumWidth() >= 96
        assert tab.advanced_filters_btn.minimumWidth() >= 125
        assert tab._inbox_focus_buttons["forms"].minimumWidth() >= 120
        assert tab._inbox_focus_buttons["commstat"].minimumWidth() >= 100
        assert tab.refresh_btn.minimumHeight() >= 24
        assert tab.type_filter.minimumHeight() >= 24

        tab.resize(1400, 900)
        tab._update_messages_responsive_layout()
        app.processEvents()

        wide_layout = tab._inbox_actions_layout
        assert tab._responsive_layout_mode == "wide"
        assert wide_layout.itemAtPosition(0, 0).widget() is tab.inbox_actions_heading
        assert wide_layout.itemAtPosition(1, 0).widget() is tab.refresh_btn
        assert wide_layout.itemAtPosition(1, 1).widget() is tab.message_check_combo
        assert wide_layout.itemAtPosition(4, 1).widget() is tab.type_filter
        assert wide_layout.itemAtPosition(10, 0).widget() is tab.inbox_bbs_heading
        assert wide_layout.itemAtPosition(13, 0).widget() is tab.inbox_bbs_summary_label
        tab._set_message_table_display_profile("form_message")
        assert header.sectionResizeMode(1) == QHeaderView.Stretch
        assert header.sectionResizeMode(6) == QHeaderView.Interactive
        assert tab.messages_table.columnWidth(1) >= 420
        assert tab.messages_table.columnWidth(6) <= 90
        assert tab.messages_table.minimumWidth() >= 960
        tab._set_message_table_display_profile("intel_report")
        assert header.sectionResizeMode(1) == QHeaderView.Stretch
        assert tab.messages_table.columnWidth(1) >= 180
        assert tab.messages_table.columnWidth(5) <= 120
        assert tab.compose_splitter.orientation() == Qt.Vertical
        assert tab.compose_body_splitter.orientation() == Qt.Horizontal
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_station_command_bar_refresh_selects_primary_radio(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "MAGNET", "band": "40M", "mode": "Digi", "frequency": "7.115"},
                    {"group": "MAGNET", "band": "20M", "mode": "Digi", "frequency": "14.115"},
                ]
            }

        def get(self, _key, default=None):
            return default

        def set(self, _key, _value):
            return None

        def reload(self):
            return None

    snapshots = [
        SimpleNamespace(
            device_profile_id=1,
            name="DX10",
            device_class="tx_rx",
            runtime_primary=False,
            current_frequency_label="7.115 MHz",
            current_band="40M",
            scheduler_enabled=True,
            status_summary="On Schedule",
            overall_state="ok",
            ptt_active=False,
            shared_ptt_blocked=False,
            assigned_operating_profile_name="All Features",
            observer_follow_summary="",
        ),
        SimpleNamespace(
            device_profile_id=2,
            name="icom",
            device_class="tx_rx",
            runtime_primary=True,
            current_frequency_label="14.115 MHz",
            current_band="20M",
            scheduler_enabled=True,
            status_summary="Manual Hold",
            overall_state="warn",
            ptt_active=False,
            shared_ptt_blocked=False,
            assigned_operating_profile_name="Net Plan",
            observer_follow_summary="",
        ),
    ]

    class FakeManager:
        def get_runtime_snapshots(self, *, force: bool = False):
            return list(snapshots)

    class FakeStore:
        activated_ids: list[int] = []

        def list_device_profiles(self):
            return [
                {
                    "id": 1,
                    "name": "DX10",
                    "device_class": "tx_rx",
                    "control_backend": "flrig",
                    "runtime_active": 1,
                    "runtime_primary": 0,
                    "assigned_operating_profile_name": "All Features",
                },
                {
                    "id": 2,
                    "name": "icom",
                    "device_class": "tx_rx",
                    "control_backend": "rigctld",
                    "runtime_active": 1,
                    "runtime_primary": 1,
                    "assigned_operating_profile_name": "Net Plan",
                },
                {
                    "id": 4,
                    "name": "Spare Rig",
                    "device_class": "tx_rx",
                    "control_backend": "js8call",
                    "runtime_active": "0",
                    "runtime_primary": 0,
                    "assigned_operating_profile_name": "Backup Plan",
                },
            ]

        def list_runtime_active_device_profiles(self):
            return [profile for profile in self.list_device_profiles() if int(profile.get("runtime_active", 0) or 0) == 1]

        def get_device_profile(self, device_profile_id: int):
            return next(
                (profile for profile in self.list_device_profiles() if int(profile.get("id", 0) or 0) == int(device_profile_id)),
                None,
            )

        def set_runtime_primary_device_profile(self, device_profile_id: int):
            self.activated_ids.append(int(device_profile_id))
            return {
                "id": int(device_profile_id),
                "name": f"Radio {int(device_profile_id)}",
                "control_backend": "flrig",
            }

        def list_effective_assigned_plans(self):
            return [{"device_profile_id": 2, "frequency_plan_id": 20}]

        def list_frequency_plans(self):
            return [{"id": 20, "name": "Net Plan", "schedule_refs_json": "[]"}]

    window = MainWindow.__new__(MainWindow)
    window.station_runtime_manager = FakeManager()
    window.multi_radio_store = FakeStore()
    window._station_command_selected_profile_id = None
    window._station_command_bar_loading = False
    window.station_command_radio_combo = QComboBox()
    window.station_command_now_label = QLabel()
    window.station_command_state_label = QLabel()
    window.station_command_freq_combo = QComboBox()
    window.station_command_next_label = QLabel()
    window.station_command_duration_combo = QComboBox()
    window.station_command_qsy_btn = QPushButton("QSY Now")
    window.station_command_hold_btn = QPushButton("QSY Suspend")
    window.station_command_suspend_btn = QPushButton("Suspend Scheduler")
    window.station_command_resume_btn = QPushButton("Resume Schedule")
    window.station_command_health_label = QLabel("Health:")
    window.station_command_health_widget = QWidget()
    window.station_command_health_layout = QHBoxLayout(window.station_command_health_widget)
    window.station_command_health_leds = {}
    window.station_command_health_text_labels = {}
    window.station_command_radio_summary_widget = QWidget()
    window.station_command_radio_summary_layout = QHBoxLayout(window.station_command_radio_summary_widget)
    window.settings = FakeSettings()
    window.dependency_status_service = SimpleNamespace(software_status_snapshot=lambda: {})
    window.scheduler = SimpleNamespace(current_schedule_entry={"frequency": "14.115", "group": "MAGNET", "band": "20M"})
    window._runtime_client_signature = None
    window._active_runtime_profile = {}
    window._runtime_profile_signature = None
    window._suppressed_screen_labels = set()
    window._refresh_plan_context_labels = lambda *_args, **_kwargs: None
    window._refresh_station_overview = lambda *_args, **_kwargs: None

    MainWindow._refresh_station_command_bar(window, force=True)

    assert [window.station_command_radio_combo.itemText(idx) for idx in range(window.station_command_radio_combo.count())] == [
        "DX10 (HF)",
        "icom (HF)",
    ]
    assert window.station_command_radio_combo.currentData() == 2
    assert window.station_command_now_label.text() == "MAGNET 20M"
    assert window.station_command_now_label.toolTip() == "MAGNET 20M: 14.115.000 20M"
    assert window.station_command_state_label.text() == "Manual Hold"
    assert window.station_command_next_label.text() == "Next: Net Plan"
    summary_tiles = window.station_command_radio_summary_widget.findChildren(QFrame, "stationCommandRadioTile")
    assert len(summary_tiles) == 2
    summary_buttons = window.station_command_radio_summary_widget.findChildren(QPushButton, "stationCommandRadioTileName")
    assert [button.text() for button in summary_buttons] == ["DX10", "icom"]
    assert [button.isChecked() for button in summary_buttons] == [False, True]
    assert window.station_command_qsy_btn.isEnabled() is True
    assert window.station_command_freq_combo.currentText() == "MAGNET 20M"
    assert window.station_command_freq_combo.itemData(
        window.station_command_freq_combo.currentIndex(),
        Qt.ToolTipRole,
    ) == "MAGNET 20M: 14.115.000 Digi"
    assert "Command target: icom" in window.station_command_qsy_btn.toolTip()

    window.station_command_radio_combo.setCurrentIndex(0)
    MainWindow._on_station_command_radio_changed(window, 0)

    assert window._station_command_selected_profile_id == 1
    assert window.multi_radio_store.activated_ids == []
    assert window.station_command_now_label.text() == "MAGNET 40M"

    for widget in (
        window.station_command_radio_combo,
        window.station_command_now_label,
        window.station_command_state_label,
        window.station_command_freq_combo,
        window.station_command_next_label,
        window.station_command_duration_combo,
        window.station_command_qsy_btn,
        window.station_command_hold_btn,
        window.station_command_suspend_btn,
        window.station_command_resume_btn,
        window.station_command_health_widget,
        window.station_command_health_label,
        window.station_command_radio_summary_widget,
    ):
        widget.deleteLater()
        app.processEvents()


def test_phase7_station_command_all_radio_admin_opens_assignment_for_radio(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {"operating_groups": []}

        def get(self, _key, default=None):
            return default

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window._station_command_radio_admin_expanded = True
    window.station_command_radio_admin_panel = QWidget()
    window.station_command_radio_admin_layout = QVBoxLayout(window.station_command_radio_admin_panel)
    window.station_command_radio_admin_btn = QPushButton("All Radios")
    opened: list[tuple] = []
    assigned: list[tuple] = []
    window.open_settings_section = lambda *args, **kwargs: opened.append((args, kwargs))
    window.settings_tab = SimpleNamespace(
        open_schedule_assignment_editor=lambda **kwargs: assigned.append(tuple(sorted(kwargs.items())))
    )
    window._open_station_health_detail = lambda **_kwargs: None
    window._activate_station_command_radio = lambda _ident: True
    window._refresh_station_command_bar = lambda *args, **kwargs: None

    snapshots = [
        SimpleNamespace(
            device_profile_id=7,
            name="FIO-B",
            current_frequency_label="7.115 MHz",
            current_band="40M",
            current_group="MAGNET",
            status_summary="On Schedule",
            assigned_frequency_plan_name="Magnet Main Plan",
            scheduler_enabled=True,
            runtime_active=True,
            ptt_active=False,
            shared_ptt_blocked=False,
        )
    ]

    try:
        MainWindow._refresh_station_command_radio_admin(window, snapshots, 0)
        rows = window.station_command_radio_admin_panel.findChildren(QFrame, "stationCommandRadioAdminRow")
        assert len(rows) == 1
        buttons = rows[0].findChildren(QPushButton)
        assert [button.text() for button in buttons] == ["Select", "Assign Plan", "Health"]

        buttons[1].click()
        app.processEvents()

        assert opened == [(("schedule_assignments",), {"radio_id": 7, "settings_nav_context": "radios"})]
        assert assigned == [((("device_profile_id", 7),))]
    finally:
        window.station_command_radio_admin_panel.deleteLater()
        window.station_command_radio_admin_btn.deleteLater()
        app.processEvents()


def test_phase7_station_command_radio_selector_uses_hero_treatment() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert 'self.station_command_radio_combo.setObjectName("stationCommandRadioSelector")' in source
    assert "QComboBox#stationCommandRadioSelector {" in source
    assert "font-size: 20px;" in source
    assert "font-weight: 800;" in source
    assert "setMaximumWidth(340)" in source


def test_phase7_runtime_banner_suppresses_routine_launch_control_context() -> None:
    from freqinout.gui.main_window import MainWindow

    profile = {"name": "FIO-A", "control_backend": "flrig", "deployment_mode": "standard"}
    policy = {
        "operating_profile_name": "Default Operating Profile",
        "assignment_state": "active",
        "scheduler_enabled": True,
        "use_map": True,
        "use_messages": True,
        "use_net_control_tabs": True,
        "use_background_ingest": True,
        "use_launch_control": False,
    }

    assert MainWindow._runtime_banner_text(profile, policy) == ""

    policy["use_messages"] = False
    assert "Messages hidden" in MainWindow._runtime_banner_text(profile, policy)


def test_phase7_controlfreq_search_exposes_app_wide_quick_find() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")

    assert 'self.search_edit.setPlaceholderText("Search FIO... radios, schedules, messages, settings")' in source
    assert "self.search_edit.returnPressed.connect(self._show_app_search_results)" in source
    assert 'self.app_search_btn = QPushButton("Search FIO")' in source
    assert "root.show_quick_search_results(query, self.search_edit)" in source


def test_phase7_main_window_quick_search_indexes_core_app_objects(monkeypatch) -> None:
    from freqinout.gui.main_window import MainWindow

    class FakeStore:
        def list_device_profiles(self):
            return [
                {
                    "id": 7,
                    "name": "FIO-A",
                    "device_class": "tx_rx",
                    "control_backend": "flrig",
                    "assigned_operating_profile_name": "Default Plan",
                }
            ]

        def list_frequency_plans(self):
            return [
                {
                    "name": "MAGNET Daily Blend",
                    "category": "normal",
                    "source_refs_json": "[]",
                    "group_refs_json": '["MAGNET"]',
                    "frequency_refs_json": '["40M:7.115"]',
                }
            ]

    class FakeSettings:
        def all(self):
            return {"operator_callsign": "N0CALL", "operating_groups": []}

        def get(self, key, default=None):
            if key == "operating_groups":
                return [{"group": "MAGNET", "band": "40M", "mode": "Digi", "frequency": "7.115"}]
            return default

    window = MainWindow.__new__(MainWindow)
    window._quick_search_cache = (0.0, [])
    window._nav_specs = [("ControlFreq", "ControlFreq"), ("Compose", "Messages"), ("Radios", "Settings")]
    window._screen_is_runtime_suppressed = lambda _screen: False
    window.multi_radio_store = FakeStore()
    window.settings = FakeSettings()

    radio_results = MainWindow.quick_search(window, "FIO-A")
    schedule_results = MainWindow.quick_search(window, "MAGNET")
    compose_results = MainWindow.quick_search(window, "compose")

    assert radio_results[0]["category"] == "Radio"
    assert radio_results[0]["radio_id"] == 7
    assert any(row["title"] == "MAGNET Daily Blend" for row in schedule_results)
    assert compose_results[0]["action"] == "messages"
    assert compose_results[0]["message_mode"] == "compose"


def test_phase7_station_command_bar_handles_no_configured_radio(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {"operating_groups": []}

        def get(self, _key, default=None):
            return default

        def set(self, _key, _value):
            return None

        def reload(self):
            return None

    class FakeManager:
        def get_runtime_snapshots(self, *, force: bool = False):
            return []

    class FakeStore:
        def list_device_profiles(self):
            return []

    window = MainWindow.__new__(MainWindow)
    window.station_runtime_manager = FakeManager()
    window.multi_radio_store = FakeStore()
    window._station_command_selected_profile_id = 88
    window._station_command_bar_loading = False
    window.station_command_radio_combo = QComboBox()
    window.station_command_now_label = QLabel()
    window.station_command_state_label = QLabel()
    window.station_command_freq_combo = QComboBox()
    window.station_command_next_label = QLabel()
    window.station_command_duration_combo = QComboBox()
    window.station_command_qsy_btn = QPushButton("QSY Now")
    window.station_command_hold_btn = QPushButton("QSY Suspend")
    window.station_command_suspend_btn = QPushButton("Suspend Scheduler")
    window.station_command_resume_btn = QPushButton("Resume Schedule")
    window.station_command_health_label = QLabel("Health:")
    window.station_command_health_widget = QWidget()
    window.station_command_health_layout = QHBoxLayout(window.station_command_health_widget)
    window.station_command_health_leds = {}
    window.station_command_health_text_labels = {}
    window.settings = FakeSettings()
    window.dependency_status_service = SimpleNamespace(software_status_snapshot=lambda: {})

    MainWindow._refresh_station_command_bar(window, force=True)

    assert window._station_command_selected_profile_id is None
    assert window.station_command_radio_combo.currentText() == "No configured radios"
    assert window.station_command_now_label.text() == "Now: unavailable"
    assert window.station_command_state_label.text() == "No configured radio"
    assert window.station_command_next_label.text() == "Next: none"
    assert window.station_command_qsy_btn.isEnabled() is False
    assert window.station_command_qsy_btn.toolTip() == "No configured radio is available for station commands."

    for widget in (
        window.station_command_radio_combo,
        window.station_command_now_label,
        window.station_command_state_label,
        window.station_command_freq_combo,
        window.station_command_next_label,
        window.station_command_duration_combo,
        window.station_command_qsy_btn,
        window.station_command_hold_btn,
        window.station_command_suspend_btn,
        window.station_command_resume_btn,
        window.station_command_health_widget,
        window.station_command_health_label,
    ):
        widget.deleteLater()
    app.processEvents()


def test_phase7_station_command_manual_qsy_persists_target_until_runtime_catches_up(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "MAGNET", "band": "40M", "mode": "Digi", "frequency": "7.115"},
                    {"group": "S2 UNDERGROUND", "band": "20M", "mode": "Digi", "frequency": "14.115"},
                ]
            }

    class FakeScheduler:
        current_source = "QSY"
        current_schedule_entry = {
            "target_device_profile_id": 1,
            "frequency": "14.115",
            "group": "S2 UNDERGROUND",
            "band": "20M",
            "mode": "Digi",
        }

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.scheduler = FakeScheduler()
    window._station_command_selected_profile_id = 1
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    snapshot = SimpleNamespace(device_profile_id=1, current_frequency_label="7.115 MHz", current_band="40M")

    assert MainWindow._station_command_now_text(window, snapshot) == "S2/GHOSTNET 20M"
    assert (
        MainWindow._station_command_now_tooltip(window, snapshot)
        == "QSY target: S2/GHOSTNET 20M; radio reports: 7.115.000 40M"
    )
    assert MainWindow._station_command_scheduler_manual_qsy_active(window) is True

    app.processEvents()


def test_phase7_station_command_qsy_target_refresh_preserves_operator_selection(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "MAGNET", "band": "40M", "mode": "Digi", "frequency": "7.115"},
                    {"group": "S2 UNDERGROUND", "band": "20M", "mode": "Digi", "frequency": "14.115"},
                ]
            }

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.scheduler = SimpleNamespace(current_schedule_entry={"frequency": "7.115", "group": "MAGNET", "band": "40M"})
    window.station_command_freq_combo = QComboBox()
    window.station_command_freq_combo.addItem("S2/GHOSTNET 20M", {"freq": 14.115, "group": "S2 UNDERGROUND", "band": "20M"})
    window.station_command_freq_combo.setCurrentIndex(0)
    selected = SimpleNamespace(current_frequency_label="7.115 MHz", current_band="40M")

    assert MainWindow._refresh_station_command_frequency_combo(window, selected) is True

    assert window.station_command_freq_combo.currentText() == "S2/GHOSTNET 20M"
    assert window.station_command_freq_combo.currentData()["group"] == "S2 UNDERGROUND"

    window.station_command_freq_combo.deleteLater()
    app.processEvents()


def test_phase7_station_command_now_ignores_other_radio_scheduler_entry(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "AMRRON", "band": "40M", "mode": "Digi", "frequency": "7.110"},
                    {"group": "MAGNET", "band": "40M", "mode": "Digi", "frequency": "7.115"},
                ]
            }

    class FakeScheduler:
        current_source = "HF"
        current_schedule_entry = {
            "target_device_profile_id": 2,
            "frequency": "7.110",
            "group": "AMRRON",
            "band": "40M",
            "mode": "Digi",
        }

        def _primary_manual_control_radio_id(self):
            return 2

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.scheduler = FakeScheduler()
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_selected_profile_id = 1
    snapshot = SimpleNamespace(
        device_profile_id=1,
        current_frequency_label="7.115 MHz",
        current_band="40M",
        current_group="MAGNET",
    )

    assert MainWindow._station_command_now_text(window, snapshot) == "MAGNET 40M"

    app.processEvents()


def test_phase7_station_command_selected_radio_uses_its_snapshot_before_global_scheduler(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "AMRRON", "band": "40M", "mode": "Digi", "frequency": "7.110"},
                    {"group": "MAGNET", "band": "40M", "mode": "Digi", "frequency": "7.115"},
                ]
            }

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.scheduler = SimpleNamespace(
        current_source="HF",
        current_schedule_entry={
            "target_device_profile_id": 1,
            "frequency": "7.115",
            "group": "MAGNET",
            "band": "40M",
            "mode": "Digi",
        },
    )
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_selected_profile_id = 2
    snapshot = SimpleNamespace(
        device_profile_id=2,
        current_frequency_label="7.110 MHz",
        current_band="40M",
        current_group="AMRRON",
    )

    assert MainWindow._station_command_now_text(window, snapshot) == "AMRRON 40M"

    app.processEvents()


def test_phase7_station_command_qsy_state_does_not_bleed_across_radios(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.settings = SimpleNamespace(all=lambda: {"operating_groups": []})
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_selected_profile_id = 2
    window.scheduler = SimpleNamespace(
        current_source="QSY",
        current_schedule_entry={
            "target_device_profile_id": 1,
            "frequency": "7.115",
            "group": "MAGNET",
            "band": "40M",
            "mode": "Digi",
        },
        _manual_qsy_active=True,
    )
    snapshot = SimpleNamespace(
        device_profile_id=2,
        current_frequency_label="7.110 MHz",
        current_band="40M",
        current_group="AMRRON",
    )

    assert MainWindow._station_command_scheduler_manual_qsy_active(window) is False
    assert MainWindow._station_command_now_text(window, snapshot) == "AMRRON 40M"

    app.processEvents()


def test_phase7_station_command_scheduler_suspend_state_does_not_bleed_across_radios(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window._station_command_scheduler_suspended_manual = True
    window._station_command_scheduler_suspended_manual_profile_id = 2
    window._station_command_selected_profile_id = 2
    class FakeScheduler:
        def __init__(self) -> None:
            self._runtime_scheduler_enabled_override = False

        def set_runtime_scheduler_enabled(self, enabled: bool) -> None:
            self._runtime_scheduler_enabled_override = bool(enabled)

    window.scheduler = FakeScheduler()

    assert MainWindow._station_command_scheduler_suspended_manually_for_radio(window, 2) is True
    assert MainWindow._station_command_scheduler_suspended_manually_for_radio(window, 1) is False

    MainWindow._station_command_set_scheduler_suspended_manual(window, False)

    assert MainWindow._station_command_scheduler_suspended_manually_for_radio(window, 2) is False
    assert window._station_command_scheduler_suspended_manual_profile_id == 0

    app.processEvents()


def test_phase7_station_command_multi_radio_tiles_use_operator_command_layout() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    theme_source = Path("freqinout/gui/theme.py").read_text(encoding="utf-8")
    tile_block = source[
        source.index("def _refresh_station_command_radio_tiles")
        : source.index("def _clear_station_command_admin_layout")
    ]
    refresh_block = source[
        source.index("def _refresh_station_command_bar")
        : source.index("def _on_station_command_radio_changed")
    ]

    assert 'health_btn = QPushButton("Health", tile)' in tile_block
    assert 'health_btn.setObjectName("stationCommandRadioTileHealth")' in tile_block
    assert 'freq_combo = QComboBox(tile)' in tile_block
    assert 'freq_combo.setObjectName("stationCommandRadioTileFrequency")' in tile_block
    assert "card_width = self._station_command_radio_card_width(len(choices))" in tile_block
    assert "tile.setMinimumWidth(card_width)" in tile_block
    assert "tile.setMaximumWidth(card_width)" in tile_block
    assert "tile.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)" in tile_block
    assert "compact_card = card_width < 360" in tile_block
    assert "timer_btn = QToolButton(tile)" in tile_block
    assert 'timer_btn.setObjectName("stationCommandRadioTileTimedSuspend")' in tile_block
    assert '"Hold" if compact_card else timed_qsy_text(timed_qsy_active=timed_qsy_active)' in tile_block
    assert "scheduler_actions = scheduler_action_state(" in tile_block
    assert "action_state = qsy_action_state(" in tile_block
    assert "card_snapshot: object = snapshot" in tile_block
    assert "preferred_key = self._station_command_preferred_qsy_key(card_snapshot)" in tile_block
    assert "not preferred_key and combo.property(\"stationCommandSelectionDirty\")" in tile_block
    assert "action_state.qsy_enabled" in tile_block
    assert "def _update_station_command_radio_tile_hold_controls" in source
    assert "self._station_command_radio_tile_controls = tile_controls" in tile_block
    summary_block = source[
        source.index("def _refresh_station_command_radio_summary")
        : source.index("def _refresh_station_command_radio_tiles")
    ]
    assert "hold_countdown" not in summary_block
    assert 'source_surface", "") or "").strip().lower() == "station_command_bar"' in source
    wrapper_block = source[
        source.index("def _station_command_for_radio")
        : source.index("def _station_command_set_qsy_combo_to_meta")
    ]
    assert "_refresh_station_command_bar(force=True)" not in wrapper_block
    hold_block = source[
        source.index("def _dispatch_hold_snapshot")
        : source.index("def on_hold_state_changed")
    ]
    assert "if signature_changed:" in hold_block
    assert "elif was_active:" in hold_block
    assert "self._update_station_command_radio_tile_hold_controls(snapshot)" in hold_block
    assert "cards_active = isinstance(card_controls, Mapping) and bool(card_controls)" in hold_block
    assert "if cards_active and not force:" in hold_block
    assert "self._apply_active_hold_status_panel(snapshot)" in hold_block
    assert "self._update_station_command_hold_button_labels(snapshot)" in hold_block
    assert "statusBar().showMessage" in source[
        source.index("def _publish_station_command_feedback")
        : source.index("def _on_station_command_hold_duration_changed")
    ]
    assert "timer_btn.setPopupMode(QToolButton.MenuButtonPopup)" in tile_block
    assert "duration_menu = QMenu(timer_btn)" in tile_block
    assert 'manual_qsy_action = QAction("Indefinite", duration_menu)' in tile_block
    assert "suspend_btn = QToolButton(tile)" in tile_block
    assert 'suspend_btn.setObjectName("stationCommandRadioTileSchedulerSuspend")' in tile_block
    assert 'manual_suspend_action = QAction("Indefinite", suspend_menu)' in tile_block
    assert 'assign_btn = QPushButton("Change Plan", tile)' in tile_block
    assert 'assign_btn.setText("Plan")' in tile_block
    assert 'now_font.setBold(True)' in tile_block
    assert 'now_font.setPointSize(max(now_font.pointSize(), 13))' in tile_block
    assert 'tile_layout.addWidget(freq_combo, 1, 0, 1, 4)' in tile_block
    assert 'tile_layout.addWidget(freq_combo, 1, 0, 1, 3)' in tile_block
    assert 'tile_layout.addWidget(state_label, 0, 2)' not in tile_block
    assert 'tile_layout.addWidget(name_btn, 0, 0, 1, 3)' in tile_block
    assert 'tile_layout.addWidget(next_label, 3, 0, 1, 4)' in tile_block
    assert 'tile_layout.addWidget(resume_btn, 4, 0, 1, 2)' in tile_block
    assert 'tile_layout.addWidget(assign_btn, 4, 2, 1, 2)' in tile_block
    assert "scheduler_suspended_manual = self._station_command_scheduler_suspended_manually_for_radio(ident)" in tile_block
    assert "page_choices = visible_choices" in source
    assert "self._change_station_command_radio_page(1)" in source
    assert "station_command_radio_summary_scroll" in source[source.index("def _station_command_radio_card_width") : source.index("def _station_command_radio_page_slice")]
    assert "viewport_width or scroll_width or bar_width" in source[source.index("def _station_command_radio_card_width") : source.index("def _station_command_radio_page_slice")]
    card_width_block = source[source.index("def _station_command_radio_card_width") : source.index("def _station_command_radio_page_slice")]
    assert "min(480, available // count)" in card_width_block
    assert "parent.setMaximumWidth(16777215)" in source
    assert "scroll.horizontalScrollBar().setValue(0)" in source
    assert "return total" in source[source.index("def _station_command_radio_cards_per_page") : source.index("def _station_command_radio_page_slice")]
    assert "btn.setVisible(False)" in refresh_block
    assert "self.station_command_radio_summary_scroll.setFixedHeight(188 if card_mode else 42)" in refresh_block
    assert "widget.setVisible(not card_mode)" in refresh_block
    assert "widget.setVisible(False)" in refresh_block
    assert 'tile.setProperty("selected", "true" if selected else "false")' in tile_block
    assert "QFrame#stationCommandRadioTile[selected=\\\"true\\\"]" in source
    assert "QComboBox#stationCommandRadioTileFrequency" in source
    assert "dropdown-chevron.svg" in source
    assert "QToolButton#stationCommandRadioTileTimedSuspend::menu-indicator" in source
    assert "station_control_tile_surface" in source
    assert "station_control_tile_selected_surface" in source
    assert "station_control_tile_selected_border" in source
    assert "station_control_tile_surface" in theme_source
    assert "station_control_tile_selected_surface" in theme_source


def test_phase7_station_command_health_uses_non_modal_quick_menu() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    health_block = source[
        source.index("def _station_command_health_summary_for_profile")
        : source.index("def _add_station_command_health_item")
    ]
    click_block = source[
        source.index("def _on_station_command_health_clicked")
        : source.index("def _quick_search_blob")
    ]
    tile_block = source[
        source.index("def _refresh_station_command_radio_tiles")
        : source.index("def _clear_station_command_admin_layout")
    ]

    assert "def _show_station_command_health_menu" in health_block
    assert "menu = QMenu(anchor_widget)" in health_block
    assert 'menu.setObjectName("stationCommandHealthMenu")' in health_block
    assert "QMenu#stationCommandHealthMenu" in health_block
    assert 'open_action = QAction("Open Health Details", menu)' in health_block
    assert "menu.popup(anchor_widget.mapToGlobal(anchor_widget.rect().bottomLeft()))" in health_block
    assert "self._show_station_command_health_menu(device_profile_id=ident, anchor=anchor)" in click_block
    assert "self._open_station_health_detail(device_profile_id=ident)" not in click_block
    assert "self._show_station_command_health_menu(" in tile_block


def test_phase7_station_command_selected_radio_uses_assigned_plan_before_other_radio_scheduler(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [{"device_profile_id": 2, "frequency_plan_id": 20}]

        def list_frequency_plans(self):
            return [
                {
                    "id": 20,
                    "name": "AmRRON Plan",
                    "schedule_refs_json": '[{"day_utc":"ALL","start_utc":"00:00","end_utc":"23:59","group":"AMRRON","band":"40M"}]',
                }
            ]

    window = MainWindow.__new__(MainWindow)
    window.settings = SimpleNamespace(all=lambda: {"operating_groups": []})
    window.multi_radio_store = FakeStore()
    window.scheduler = SimpleNamespace(
        current_source="HF",
        current_schedule_entry={
            "target_device_profile_id": 1,
            "frequency": "7.115",
            "group": "MAGNET",
            "band": "40M",
            "mode": "Digi",
        },
    )
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_selected_profile_id = 2
    snapshot = SimpleNamespace(
        device_profile_id=2,
        current_frequency_label="",
        current_band="",
        current_group="",
    )

    assert MainWindow._station_command_now_text(window, snapshot) == "AMRRON 40M"

    app.processEvents()


def test_phase7_station_command_ignores_legacy_string_plan_refs(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [{"device_profile_id": 2, "frequency_plan_id": 20}]

        def list_frequency_plans(self):
            return [
                {
                    "id": 20,
                    "name": "Mixed Legacy Plan",
                    "schedule_refs_json": (
                        '["hf:legacy-string",'
                        '{"day_utc":"ALL","start_utc":"00:00","end_utc":"23:59",'
                        '"group":"AMRRON","band":"20M","frequency":"14.110"}]'
                    ),
                    "frequency_refs_json": '["20M:14.110"]',
                }
            ]

    window = MainWindow.__new__(MainWindow)
    window.settings = SimpleNamespace(all=lambda: {"operating_groups": []})
    window.multi_radio_store = FakeStore()
    window.scheduler = SimpleNamespace(current_source="HF", current_schedule_entry={})
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_selected_profile_id = 2
    snapshot = SimpleNamespace(device_profile_id=2, current_frequency_label="", current_band="", current_group="")

    assert MainWindow._station_command_ref_active_now("hf:legacy-string", datetime.datetime.now(datetime.timezone.utc)) is False
    assert MainWindow._station_command_now_text(window, snapshot) == "AMRRON 20M"
    assert MainWindow._station_command_preferred_qsy_key(window, snapshot) == "14.110000"

    app.processEvents()


def test_phase7_station_command_next_uses_selected_radio_assigned_plan(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    now = datetime.datetime.now(datetime.timezone.utc)
    next_minute = (now.hour * 60 + now.minute + 1) % 1440
    next_start = f"{next_minute // 60:02d}:{next_minute % 60:02d}"

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [
                {"device_profile_id": 1, "frequency_plan_id": 10},
                {"device_profile_id": 2, "frequency_plan_id": 20},
            ]

        def list_frequency_plans(self):
            return [
                {
                    "id": 10,
                    "name": "MagNet Plan",
                    "schedule_refs_json": '[{"day_utc":"ALL","start_utc":"00:00","end_utc":"23:59","group":"MAGNET","band":"40M"}]',
                },
                {
                    "id": 20,
                    "name": "AmRRON Plan",
                    "schedule_refs_json": (
                        '[{"day_utc":"ALL","start_utc":"00:00","end_utc":"23:59","group":"AMRRON","band":"40M"},'
                        f'{{"day_utc":"ALL","start_utc":"{next_start}","end_utc":"23:59","group":"AMRRON","band":"20M"}}]'
                    ),
                },
            ]

    window = MainWindow.__new__(MainWindow)
    window.multi_radio_store = FakeStore()
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    snapshot = SimpleNamespace(
        device_profile_id=2,
        assigned_operating_profile_name="AmRRON Plan",
        scheduler_enabled=True,
    )

    assert MainWindow._station_command_next_text(window, snapshot) == "AMRRON 20M"

    app.processEvents()


def test_phase7_station_command_next_does_not_use_other_radio_assigned_plan(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [{"device_profile_id": 1, "frequency_plan_id": 10}]

        def list_frequency_plans(self):
            return [
                {
                    "id": 10,
                    "name": "MagNet Plan",
                    "schedule_refs_json": '[{"day_utc":"ALL","start_utc":"00:00","end_utc":"23:59","group":"MAGNET","band":"40M"}]',
                }
            ]

    window = MainWindow.__new__(MainWindow)
    window.multi_radio_store = FakeStore()
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    snapshot = SimpleNamespace(
        device_profile_id=2,
        assigned_operating_profile_name="AmRRON Plan",
        scheduler_enabled=True,
    )

    assert MainWindow._station_command_next_text(window, snapshot) == "No assigned plan"

    app.processEvents()


def test_phase7_station_command_plan_label_uses_frequency_plan_assignment(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [{"device_profile_id": 2, "frequency_plan_id": 20}]

        def list_frequency_plans(self):
            return [{"id": 20, "name": "AmRRON Main Plan", "schedule_refs_json": "[]"}]

    window = MainWindow.__new__(MainWindow)
    window.multi_radio_store = FakeStore()
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    snapshot = SimpleNamespace(
        device_profile_id=2,
        assigned_operating_profile_name="Default Operating Profile",
        operating_profile_name="Default Operating Profile",
    )

    assert MainWindow._station_command_plan_name_for_snapshot(window, snapshot) == "AmRRON Main Plan"
    assert MainWindow._station_command_display_plan_name("AmRRON Main Plan") == "AmRRON Main"

    app.processEvents()


def test_phase7_station_command_card_qsy_options_are_assigned_plan_scoped(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "MAGNET", "band": "40M", "freq": "7.115", "mode": "Digi"},
                    {"group": "AMRRON", "band": "20M", "freq": "14.110", "mode": "Digi"},
                    {"group": "AMRRON", "band": "40M", "freq": "7.110", "mode": "Digi"},
                ]
            }

        def get(self, _key, default=None):
            return default

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [{"device_profile_id": 2, "frequency_plan_id": 20}]

        def list_frequency_plans(self):
            return [
                {
                    "id": 20,
                    "name": "AmRRON Main Plan",
                    "schedule_refs_json": (
                        '[{"group":"AMRRON","band":"20M","frequency":"14.110"},'
                        '{"group":"AMRRON","band":"40M","frequency":"7.110"}]'
                    ),
                    "frequency_refs_json": "[]",
                }
            ]

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.multi_radio_store = FakeStore()
    window.scheduler = SimpleNamespace(current_schedule_entry={})
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    snapshot = SimpleNamespace(device_profile_id=2, current_frequency_label="", current_band="", current_group="")

    combo = QComboBox()
    try:
        assert MainWindow._station_command_populate_card_frequency_combo(window, combo, snapshot) is True
        labels = [combo.itemText(index) for index in range(combo.count())]
        assert labels == ["AMRRON 20M", "AMRRON 40M"]
        assert all("MAGNET" not in label for label in labels)
    finally:
        combo.deleteLater()

    app.processEvents()


def test_phase7_station_command_bar_uses_card_for_single_active_radio(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    monkeypatch.setattr(main_mod, "suspend_snapshot", lambda *_args, **_kwargs: {"active": False})

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "AMRRON", "band": "20M", "freq": "14.110", "mode": "Digi"},
                    {"group": "AMRRON", "band": "40M", "freq": "7.110", "mode": "Digi"},
                ]
            }

        def get(self, _key, default=None):
            return default

    class FakeStore:
        def list_runtime_active_device_profiles(self):
            return [
                {
                    "id": 2,
                    "name": "FIO-B",
                    "device_class": "tx_rx",
                    "control_backend": "flrig",
                    "runtime_active": 1,
                    "runtime_primary": 0,
                    "current_frequency_label": "",
                }
            ]

        def list_device_profiles(self):
            return self.list_runtime_active_device_profiles()

        def list_effective_assigned_plans(self):
            return [{"device_profile_id": 2, "frequency_plan_id": 20}]

        def list_frequency_plans(self):
            return [
                {
                    "id": 20,
                    "name": "AmRRON Plan",
                    "schedule_refs_json": (
                        '[{"group":"AMRRON","band":"20M","frequency":"14.110","day":"ALL","start":"00:00","end":"23:59"},'
                        '{"group":"AMRRON","band":"40M","frequency":"7.110","day":"ALL","start":"23:59","end":"00:00"}]'
                    ),
                    "frequency_refs_json": "[]",
                }
            ]

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.multi_radio_store = FakeStore()
    window.station_runtime_manager = SimpleNamespace(get_runtime_snapshots=lambda force=False: [])
    window.scheduler = SimpleNamespace(current_schedule_entry={}, current_source="")
    window.dependency_status_service = SimpleNamespace(software_status_snapshot=lambda: {})
    window.action_feedback_service = None
    window._station_command_selected_profile_id = None
    window._station_command_bar_loading = False
    window._station_command_multi_mode_active = False
    window._station_command_radio_page = 0
    window._station_command_radio_summary_signature = None
    window._station_command_radio_tile_controls = {}
    window._station_command_card_qsy_pending_keys = {}
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    window._station_command_lane_cache_data = None
    window._station_command_lane_cache_expires = 0.0
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_scheduler_suspended_manual = False
    window._station_command_scheduler_suspended_manual_profile_id = 0
    window._station_command_timed_suspend_profile_id = 0
    window._active_runtime_profile = None
    window.station_command_bar = QFrame()
    window.station_command_bar.resize(1200, 220)
    window.station_command_radio_combo = QComboBox()
    window.station_command_radio_label = QLabel("Radio")
    window.station_command_radio_separator = QFrame()
    window.station_command_now_caption = QLabel("Now")
    window.station_command_now_label = QLabel()
    window.station_command_state_label = QLabel()
    window.station_command_now_separator = QFrame()
    window.station_command_action_label = QLabel("Action")
    window.station_command_freq_combo = QComboBox()
    window.station_command_next_label = QLabel()
    window.station_command_health_label = QLabel("Health:")
    window.station_command_health_widget = QWidget()
    window.station_command_health_layout = QHBoxLayout(window.station_command_health_widget)
    window.station_command_health_leds = {}
    window.station_command_health_text_labels = {}
    window.station_command_duration_combo = QComboBox()
    window.station_command_qsy_btn = QPushButton("QSY Now")
    window.station_command_hold_btn = QPushButton("QSY Suspend")
    window.station_command_suspend_btn = QPushButton("Suspend Scheduler")
    window.station_command_resume_btn = QPushButton("Resume Schedule")
    window.station_command_radio_summary_label = QLabel("Radios")
    window.station_command_radio_summary_scroll = QScrollArea()
    window.station_command_radio_summary_widget = QWidget()
    window.station_command_radio_summary_scroll.setWidget(window.station_command_radio_summary_widget)
    window.station_command_radio_summary_layout = QHBoxLayout(window.station_command_radio_summary_widget)
    window.station_command_radio_prev_btn = QPushButton("Prev")
    window.station_command_radio_next_btn = QPushButton("Next")
    window.station_command_radio_admin_btn = QPushButton("All Radios")
    window.station_command_radio_admin_panel = QWidget()
    window.station_command_radio_admin_layout = QVBoxLayout(window.station_command_radio_admin_panel)

    try:
        MainWindow._refresh_station_command_bar(window, force=True)
        app.processEvents()

        assert window._station_command_multi_mode_active is True
        assert window.station_command_radio_combo.isVisible() is False
        assert window.station_command_now_label.isVisible() is False
        assert window.station_command_radio_admin_btn.isVisible() is False
        tiles = window.station_command_radio_summary_widget.findChildren(QFrame, "stationCommandRadioTile")
        assert len(tiles) == 1
        combo = tiles[0].findChild(QComboBox, "stationCommandRadioTileFrequency")
        assert combo is not None
        assert [combo.itemText(index) for index in range(combo.count())] == ["AMRRON 20M", "AMRRON 40M"]
        assert combo.currentText() == "AMRRON 20M"
    finally:
        for widget in (
            window.station_command_bar,
            window.station_command_radio_combo,
            window.station_command_radio_label,
            window.station_command_radio_separator,
            window.station_command_now_caption,
            window.station_command_now_label,
            window.station_command_state_label,
            window.station_command_now_separator,
            window.station_command_action_label,
            window.station_command_freq_combo,
            window.station_command_next_label,
            window.station_command_health_label,
            window.station_command_health_widget,
            window.station_command_duration_combo,
            window.station_command_qsy_btn,
            window.station_command_hold_btn,
            window.station_command_suspend_btn,
            window.station_command_resume_btn,
            window.station_command_radio_summary_label,
            window.station_command_radio_summary_scroll,
            window.station_command_radio_prev_btn,
            window.station_command_radio_next_btn,
            window.station_command_radio_admin_btn,
            window.station_command_radio_admin_panel,
        ):
            widget.deleteLater()
        app.processEvents()


def test_phase7_station_command_active_lanes_override_stale_global_scheduler_state(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeScheduler:
        current_source = "HF"
        current_schedule_entry = {
            "target_device_profile_id": 1,
            "frequency": "7.115",
            "group": "MAGNET",
            "band": "40M",
        }

        def active_schedule_lanes(self, *, force: bool = False):
            return [
                {
                    "device_profile_id": 1,
                    "frequency_plan_name": "Magnet Main Plan",
                    "current_entry": {"group": "MAGNET", "band": "40M", "frequency": "7.115"},
                    "next_entry": {"group": "MAGNET", "band": "80M", "frequency": "3.585"},
                    "hf_rows": [
                        {"group": "MAGNET", "band": "40M", "frequency": "7.115"},
                        {"group": "MAGNET", "band": "80M", "frequency": "3.585"},
                    ],
                },
                {
                    "device_profile_id": 2,
                    "frequency_plan_name": "AmRRON Plan",
                    "current_entry": {"group": "AMRRON", "band": "20M", "frequency": "14.110"},
                    "next_entry": {"group": "AMRRON", "band": "80M", "frequency": "3.588"},
                    "hf_rows": [
                        {"group": "AMRRON", "band": "20M", "frequency": "14.110"},
                        {"group": "AMRRON", "band": "80M", "frequency": "3.588"},
                    ],
                },
            ]

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "MAGNET", "band": "40M", "freq": "7.115", "mode": "Digi"},
                    {"group": "MAGNET", "band": "80M", "freq": "3.585", "mode": "Digi"},
                    {"group": "AMRRON", "band": "20M", "freq": "14.110", "mode": "Digi"},
                    {"group": "AMRRON", "band": "80M", "freq": "3.588", "mode": "Digi"},
                ]
            }

        def get(self, _key, default=None):
            return default

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.scheduler = FakeScheduler()
    window.multi_radio_store = SimpleNamespace(
        list_effective_assigned_plans=lambda: [],
        list_frequency_plans=lambda: [],
    )
    window._station_command_lane_cache_data = None
    window._station_command_lane_cache_expires = 0.0
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    snapshot = SimpleNamespace(device_profile_id=2, current_frequency_label="", current_band="", current_group="")

    combo = QComboBox()
    try:
        assert MainWindow._station_command_now_text_for_summary(window, snapshot, selected_id=1) == "AMRRON 20M"
        assert MainWindow._station_command_next_text(window, snapshot) == "AMRRON 80M"
        assert MainWindow._station_command_plan_name_for_snapshot(window, snapshot) == "AmRRON Plan"
        assert MainWindow._station_command_populate_card_frequency_combo(window, combo, snapshot) is True
        assert [combo.itemText(index) for index in range(combo.count())] == ["AMRRON 20M", "AMRRON 80M"]
    finally:
        combo.deleteLater()

    app.processEvents()


def test_phase7_station_command_health_marks_off_schedule_as_health_category(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window._station_command_off_schedule_by_radio = {
        2: {"items": ["Frequency", "VFO"], "entry": {"target_device_profile_id": 2}}
    }
    window.dependency_status_service = SimpleNamespace(software_status_snapshot=lambda: {})
    window._station_command_health_items = lambda _profile: []

    summary = MainWindow._station_command_health_summary_for_profile(
        window,
        {"id": 2, "name": "FIO-B"},
    )

    assert summary["label"] == "Off Schedule"
    assert summary["state"] == "warn"
    assert summary["issues"][0][1] == "Off Schedule"
    assert "Frequency" in summary["tooltip"]

    app.processEvents()


def test_phase7_station_command_health_marks_assignment_rf_guard_warning(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeStore:
        def get_effective_assigned_plan_for_device(self, device_id: int) -> dict[str, str]:
            assert device_id == 3
            return {
                "validation_status_json": json.dumps(
                    {
                        "state": "warning",
                        "warnings": [
                            "Antenna and schedule mismatch: FIO-C antenna support does not include 80M for Field Plan."
                        ],
                    }
                )
            }

    window = MainWindow.__new__(MainWindow)
    window._station_command_off_schedule_by_radio = {}
    window.dependency_status_service = SimpleNamespace(software_status_snapshot=lambda: {})
    window._station_command_health_items = lambda _profile: []
    window.multi_radio_store = FakeStore()

    summary = MainWindow._station_command_health_summary_for_profile(
        window,
        {"id": 3, "name": "FIO-C"},
    )

    assert summary["label"] == "RF Guard"
    assert summary["state"] == "warn"
    assert summary["issues"][0][1] == "RF Guard"
    assert "does not include 80M" in summary["tooltip"]

    app.processEvents()


def test_phase7_off_schedule_prompt_names_and_resolves_target_radio(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    calls = []
    prompt_text = {}

    class FakeMessageBox(QObject):
        AcceptRole = 0
        RejectRole = 1
        DestructiveRole = 2

        def __init__(self, _parent=None):
            super().__init__()
            self._clicked = None

        def close(self):
            return None

        def setWindowTitle(self, value):
            prompt_text["title"] = value

        def setText(self, value):
            prompt_text["text"] = value

        def addButton(self, label, _role):
            button = object()
            if self._clicked is None:
                self._clicked = button
                prompt_text["apply_label"] = label
            return button

        def exec(self):
            return 0

        def clickedButton(self):
            return self._clicked

    monkeypatch.setattr(main_mod, "QMessageBox", FakeMessageBox)

    window = MainWindow.__new__(MainWindow)
    window._shutting_down = False
    window.settings = SimpleNamespace()
    window.scheduler = SimpleNamespace(resolve_off_schedule=lambda *args, **kwargs: calls.append((args, kwargs)))
    window._station_command_radio_choices = lambda: [
        SimpleNamespace(device_profile_id=1, name="FIO-A"),
        SimpleNamespace(device_profile_id=2, name="FIO-B"),
    ]
    window._station_command_off_schedule_by_radio = {}
    window._station_command_radio_summary_signature = None
    window._refresh_station_command_bar = lambda *args, **kwargs: None
    window._build_prompt_hold_duration_combo = lambda _parent: QComboBox()
    window._attach_prompt_hold_duration_row = lambda *_args, **_kwargs: None
    window._sync_hold_duration_combos = lambda: None
    window.on_hold_state_changed = lambda *args, **kwargs: None

    MainWindow._on_off_schedule_detected(
        window,
        {
            "device_profile_id": 2,
            "items": ["Frequency"],
            "entry": {"target_scope": "device_profile", "target_device_profile_id": 2},
        },
    )

    assert prompt_text["title"] == "FIO-B Off Schedule"
    assert prompt_text["text"] == "Frequency is off schedule."
    assert prompt_text["apply_label"] == "Resume Schedule"
    assert calls == [(("apply",), {"items": ["Frequency"], "target_device_profile_id": 2})]

    app.processEvents()


def test_phase7_station_command_tiles_arm_first_card_and_keep_each_plan_scoped(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    monkeypatch.setattr(main_mod, "suspend_snapshot", lambda *_args, **_kwargs: {"active": False})

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "MAGNET", "band": "20M", "freq": "14.115", "mode": "Digi"},
                    {"group": "MAGNET", "band": "40M", "freq": "7.115", "mode": "Digi"},
                    {"group": "AMRRON", "band": "20M", "freq": "14.110", "mode": "Digi"},
                    {"group": "AMRRON", "band": "40M", "freq": "7.110", "mode": "Digi"},
                ]
            }

        def get(self, _key, default=None):
            return default

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [
                {"device_profile_id": 1, "frequency_plan_id": 10},
                {"device_profile_id": 2, "frequency_plan_id": 20},
            ]

        def list_frequency_plans(self):
            return [
                {
                    "id": 10,
                    "name": "Magnet Main Plan",
                    "schedule_refs_json": (
                        '[{"group":"MAGNET","band":"40M","frequency":"7.115"},'
                        '{"group":"MAGNET","band":"20M","frequency":"14.115"}]'
                    ),
                    "frequency_refs_json": "[]",
                },
                {
                    "id": 20,
                    "name": "AmRRON Plan",
                    "schedule_refs_json": (
                        '[{"group":"AMRRON","band":"20M","frequency":"14.110","day":"ALL","start":"00:00","end":"23:59"},'
                        '{"group":"AMRRON","band":"40M","frequency":"7.110","day":"ALL","start":"23:59","end":"00:00"}]'
                    ),
                    "frequency_refs_json": "[]",
                },
            ]

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.multi_radio_store = FakeStore()
    window.scheduler = SimpleNamespace(current_source="", current_schedule_entry={})
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._station_command_scheduler_suspended_manual = False
    window._station_command_scheduler_suspended_manual_profile_id = 0
    window._station_command_timed_suspend_profile_id = 0
    window._station_command_selected_profile_id = 1
    window._station_command_card_qsy_pending_keys = {}
    window._station_command_health_summary_for_profile = lambda _snapshot: {"state": "ok", "label": "Ready", "tooltip": ""}
    window._show_station_command_health_menu = lambda **_kwargs: None
    window._open_schedule_assignment_for_radio = lambda *_args, **_kwargs: None
    window._on_station_command_summary_radio_clicked = lambda *_args, **_kwargs: None
    window.station_command_bar = QFrame()
    window.station_command_bar.resize(1600, 220)
    window.station_command_radio_summary_widget = QWidget()
    window.station_command_radio_summary_layout = QHBoxLayout(window.station_command_radio_summary_widget)

    snapshots = [
        SimpleNamespace(
            device_profile_id=1,
            name="FIO-A",
            current_frequency_label="7.115 MHz",
            current_group="MAGNET",
            current_band="40M",
            runtime_active=True,
        ),
        SimpleNamespace(
            device_profile_id=2,
            name="FIO-B",
            current_frequency_label="7.115 MHz",
            current_group="MAGNET",
            current_band="40M",
            runtime_active=True,
        ),
    ]

    try:
        MainWindow._refresh_station_command_radio_tiles(window, snapshots, selected_id=1)
        app.processEvents()

        tiles = window.station_command_radio_summary_widget.findChildren(QFrame, "stationCommandRadioTile")
        assert len(tiles) == 2

        first_combo = tiles[0].findChild(QComboBox, "stationCommandRadioTileFrequency")
        second_combo = tiles[1].findChild(QComboBox, "stationCommandRadioTileFrequency")
        assert first_combo is not None
        assert second_combo is not None

        first_labels = [first_combo.itemText(index) for index in range(first_combo.count())]
        second_labels = [second_combo.itemText(index) for index in range(second_combo.count())]
        assert first_labels == ["MAGNET 20M", "MAGNET 40M"]
        assert second_labels == ["AMRRON 20M", "AMRRON 40M"]
        assert MainWindow._station_command_now_text_for_summary(window, snapshots[1], selected_id=1) == "AMRRON 20M"

        first_qsy = next(btn for btn in tiles[0].findChildren(QPushButton) if btn.text() == "QSY")
        first_timed = tiles[0].findChild(QToolButton, "stationCommandRadioTileTimedSuspend")
        assert first_timed is not None
        assert first_qsy.isEnabled() is False
        assert first_timed.isEnabled() is False

        first_combo.setCurrentIndex(0)
        app.processEvents()

        assert first_combo.currentText() == "MAGNET 20M"
        assert first_qsy.isEnabled() is True
        assert first_timed.isEnabled() is True
        assert window._station_command_card_qsy_pending_keys[1] == "14.115000"
    finally:
        window.station_command_radio_summary_widget.deleteLater()
        window.station_command_bar.deleteLater()
        app.processEvents()


def test_phase7_station_command_cards_do_not_inherit_manual_state_or_unscoped_scheduler_entry(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    qsy_calls = []
    monkeypatch.setattr(main_mod, "suspend_snapshot", lambda *_args, **_kwargs: {"active": False})
    monkeypatch.setattr(main_mod, "perform_qsy", lambda _window, meta: qsy_calls.append(dict(meta)) or True)

    class FakeSettings:
        def all(self):
            return {
                "operating_groups": [
                    {"group": "MAGNET", "band": "20M", "freq": "14.115", "mode": "Digi"},
                    {"group": "MAGNET", "band": "40M", "freq": "7.115", "mode": "Digi"},
                    {"group": "AMRRON", "band": "20M", "freq": "14.110", "mode": "Digi"},
                    {"group": "AMRRON", "band": "40M", "freq": "7.110", "mode": "Digi"},
                ]
            }

        def get(self, _key, default=None):
            return default

    class FakeStore:
        def list_effective_assigned_plans(self):
            return [
                {"device_profile_id": 1, "frequency_plan_id": 10},
                {"device_profile_id": 2, "frequency_plan_id": 20},
            ]

        def list_frequency_plans(self):
            return [
                {
                    "id": 10,
                    "name": "Magnet Main Plan",
                    "schedule_refs_json": (
                        '[{"group":"MAGNET","band":"20M","frequency":"14.115","day":"ALL","start":"00:00","end":"23:59"},'
                        '{"group":"MAGNET","band":"40M","frequency":"7.115","day":"ALL","start":"23:59","end":"00:00"}]'
                    ),
                    "frequency_refs_json": "[]",
                },
                {
                    "id": 20,
                    "name": "AmRRON Plan",
                    "schedule_refs_json": (
                        '[{"group":"AMRRON","band":"20M","frequency":"14.110","day":"ALL","start":"00:00","end":"23:59"},'
                        '{"group":"AMRRON","band":"40M","frequency":"7.110","day":"ALL","start":"23:59","end":"00:00"}]'
                    ),
                    "frequency_refs_json": "[]",
                },
            ]

    window = MainWindow.__new__(MainWindow)
    window.settings = FakeSettings()
    window.multi_radio_store = FakeStore()
    stale_lanes = [
        {
            "device_profile_id": 2,
            "frequency_plan_name": "Magnet Main Plan",
            "current_entry": {"frequency": "14.115", "group": "MAGNET", "band": "20M"},
            "next_entry": {"frequency": "7.115", "group": "MAGNET", "band": "40M"},
            "hf_rows": [
                {"frequency": "14.115", "group": "MAGNET", "band": "20M"},
                {"frequency": "7.115", "group": "MAGNET", "band": "40M"},
            ],
            "net_rows": [],
            "sop_rows": [],
        }
    ]
    window.scheduler = SimpleNamespace(
        current_source="QSY",
        current_schedule_entry={"frequency": "14.115", "group": "MAGNET", "band": "20M"},
        _manual_qsy_active=True,
        active_schedule_lanes=lambda force=False: stale_lanes,
    )
    window._station_command_plan_cache_data = None
    window._station_command_plan_cache_expires = 0.0
    window._station_command_manual_qsy_meta = {"freq": 14.115, "group": "MAGNET", "band": "20M", "target_device_profile_id": 1}
    window._station_command_manual_qsy_profile_id = 1
    window._station_command_scheduler_suspended_manual = False
    window._station_command_scheduler_suspended_manual_profile_id = 0
    window._station_command_timed_suspend_profile_id = 0
    window._station_command_selected_profile_id = 1
    window._station_command_card_qsy_pending_keys = {}
    window._active_runtime_profile = {"id": 1}
    window.action_feedback_service = None
    window._station_command_health_summary_for_profile = lambda _snapshot: {"state": "ok", "label": "Ready", "tooltip": ""}
    window._show_station_command_health_menu = lambda **_kwargs: None
    window._open_schedule_assignment_for_radio = lambda *_args, **_kwargs: None
    window._on_station_command_summary_radio_clicked = lambda *_args, **_kwargs: None
    window._refresh_station_command_bar = lambda *args, **kwargs: None
    window.station_command_bar = QFrame()
    window.station_command_bar.resize(1600, 220)
    window.station_command_radio_summary_widget = QWidget()
    window.station_command_radio_summary_layout = QHBoxLayout(window.station_command_radio_summary_widget)

    snapshots = [
        SimpleNamespace(device_profile_id=1, name="FIO-A", current_frequency_label="14.115 MHz", runtime_active=True),
        SimpleNamespace(device_profile_id=2, name="FIO-B", current_frequency_label="14.115 MHz", runtime_active=True),
    ]

    try:
        MainWindow._refresh_station_command_radio_tiles(window, snapshots, selected_id=1)
        app.processEvents()

        tiles = window.station_command_radio_summary_widget.findChildren(QFrame, "stationCommandRadioTile")
        assert len(tiles) == 2
        assert all(tile.maximumWidth() <= 480 for tile in tiles)
        assert not any(button.text() == "Manual QSY" for tile in tiles for button in tile.findChildren(QPushButton))

        second_combo = tiles[1].findChild(QComboBox, "stationCommandRadioTileFrequency")
        assert second_combo is not None
        assert second_combo.currentText() == "AMRRON 20M"
        assert [second_combo.itemText(index) for index in range(second_combo.count())] == ["AMRRON 20M", "AMRRON 40M"]
        assert MainWindow._station_command_preferred_qsy_key(window, snapshots[1]) == "14.110000"
        assert MainWindow._station_command_now_text_for_summary(window, snapshots[1], selected_id=1) == "AMRRON 20M"
        assert MainWindow._station_command_next_text(window, snapshots[1]) == "AMRRON 40M"

        second_qsy = next(btn for btn in tiles[1].findChildren(QPushButton) if btn.text() == "QSY")
        second_timed = tiles[1].findChild(QToolButton, "stationCommandRadioTileTimedSuspend")
        assert second_timed is not None
        assert second_qsy.isEnabled() is False
        assert second_timed.isEnabled() is False

        second_combo.setCurrentIndex(1)
        app.processEvents()

        assert second_combo.currentText() == "AMRRON 40M"
        assert second_qsy.isEnabled() is True
        assert second_timed.isEnabled() is True

        second_qsy.click()

        assert len(qsy_calls) == 1
        assert qsy_calls[0]["target_device_profile_id"] == 2
        assert qsy_calls[0]["group"] == "AMRRON"
        assert qsy_calls[0]["band"] == "40M"
        assert qsy_calls[0]["freq"] == 7.110
    finally:
        window.station_command_radio_summary_widget.deleteLater()
        window.station_command_bar.deleteLater()
        app.processEvents()


def test_phase7_station_command_qsy_now_sets_manual_target_until_resume(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    qsy_calls = []
    monkeypatch.setattr(main_mod, "perform_qsy", lambda _window, meta: qsy_calls.append(dict(meta)) or True)
    monkeypatch.setattr(main_mod, "resume_schedule_hold", lambda _window, _settings: True)

    window = MainWindow.__new__(MainWindow)
    window._station_command_selected_profile_id = 1
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window.station_command_freq_combo = QComboBox()
    window.station_command_freq_combo.addItem(
        "S2/GHOSTNET 20M",
        {"freq": 14.115, "group": "S2 UNDERGROUND", "band": "20M", "mode": "Digi"},
    )
    window.station_command_radio_combo = QComboBox()
    window.station_command_radio_combo.addItem("FIO-A (HF)", 1)
    window.action_feedback_service = None
    window.settings = SimpleNamespace()
    window._refresh_station_command_bar = lambda *args, **kwargs: None
    snapshot = SimpleNamespace(current_frequency_label="7.115 MHz", current_band="40M")

    MainWindow._on_station_command_qsy_now_clicked(window)

    assert qsy_calls == [
        {
            "freq": 14.115,
            "group": "S2 UNDERGROUND",
            "band": "20M",
            "mode": "Digi",
            "target_device_profile_id": 1,
        }
    ]
    assert MainWindow._station_command_scheduler_manual_qsy_active(window) is True
    assert MainWindow._station_command_now_text(window, snapshot) == "S2/GHOSTNET 20M"

    MainWindow._on_station_command_resume_clicked(window)

    assert MainWindow._station_command_manual_qsy_meta_for_selected(window) is None

    window.station_command_freq_combo.deleteLater()
    window.station_command_radio_combo.deleteLater()
    app.processEvents()


def test_phase7_station_command_timed_suspend_clears_manual_qsy_marker(monkeypatch) -> None:
    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    suspend_calls = []
    monkeypatch.setattr(
        main_mod,
        "suspend_schedule_hold",
        lambda _window, _settings, **kwargs: suspend_calls.append(dict(kwargs)) or 30,
    )

    window = MainWindow.__new__(MainWindow)
    window.settings = SimpleNamespace()
    window.action_feedback_service = None
    window._station_command_selected_profile_id = 2
    window._station_command_manual_qsy_meta = {"freq": 14.110, "target_device_profile_id": 2}
    window._station_command_manual_qsy_profile_id = 2
    window._station_command_timed_suspend_profile_id = 0
    window._selected_station_command_hold_minutes = lambda: 30
    window._refresh_station_command_controls_after_state_change = lambda *args, **kwargs: None

    MainWindow._on_station_command_timed_suspend_clicked(window)

    assert suspend_calls == [
        {
            "minutes": 30,
            "warn_rf_conflict": True,
            "target_device_profile_id": 2,
        }
    ]
    assert window._station_command_manual_qsy_meta is None
    assert window._station_command_manual_qsy_profile_id is None
    assert window._station_command_timed_suspend_profile_id == 2


def test_phase7_station_command_card_indefinite_suspend_is_radio_scoped(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    override_values = []
    monkeypatch.setattr(main_mod, "set_scheduler_enabled_override", lambda value: override_values.append(value))

    class FakeManualControlService:
        def __init__(self) -> None:
            self.suspended = []

        def suspend(self, radio_id, **kwargs):
            self.suspended.append((radio_id, kwargs))

        def resume(self, radio_id):
            raise AssertionError("suspend should not resume")

    class FakeScheduler:
        def __init__(self) -> None:
            self._manual_control_service = FakeManualControlService()
            self.enabled_values = []

        def set_runtime_scheduler_enabled(self, enabled):
            self.enabled_values.append(enabled)

    feedback = []
    window = MainWindow.__new__(MainWindow)
    window.scheduler = FakeScheduler()
    window.settings = SimpleNamespace()
    window._station_command_selected_profile_id = 1
    window._station_command_scheduler_suspended_manual = False
    window._station_command_scheduler_suspended_manual_profile_id = 0
    window._station_command_timed_suspend_profile_id = 0
    window._publish_station_command_feedback = lambda **payload: feedback.append(payload)
    window._refresh_station_command_controls_after_state_change = lambda *args, **kwargs: None

    MainWindow._on_station_command_pause_clicked(window, 2)

    assert window.scheduler._manual_control_service.suspended == [
        (
            2,
            {
                "reason_code": "operator_suspend",
                "operator_source": "main_control_center",
            },
        )
    ]
    assert window.scheduler.enabled_values == []
    assert override_values == [None]
    assert window._station_command_scheduler_suspended_manual_profile_id == 2
    assert feedback[-1]["summary"] == "Scheduler suspended."

    app.processEvents()


def test_phase7_station_command_manual_state_is_radio_scoped(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.core.shared_state import SchedulerManualControlState, SchedulerManualTarget
    from freqinout.gui.main_window import MainWindow

    class FakeManualControlService:
        def get_state(self, radio_id):
            if int(radio_id) == 8:
                return SchedulerManualControlState(
                    radio_profile_id="radio_8",
                    state="manual_hold",
                    manual_target=SchedulerManualTarget(frequency_hz=14_115_000, source_action="qsy"),
                    hold_until_utc=(
                        datetime.datetime.now(datetime.timezone.utc)
                        + datetime.timedelta(minutes=30)
                    )
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z"),
                )
            if int(radio_id) == 9:
                return SchedulerManualControlState(radio_profile_id="radio_9", state="on_schedule")
            return SchedulerManualControlState(radio_profile_id=f"radio_{radio_id}", state="on_schedule")

    window = MainWindow.__new__(MainWindow)
    window.scheduler = SimpleNamespace(
        _manual_control_service=FakeManualControlService(),
        current_source="QSY",
        _manual_qsy_active=True,
        current_schedule_entry={"target_device_profile_id": 8},
    )
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None

    assert MainWindow._station_command_scheduler_manual_qsy_active_for_radio(window, 8) is True
    assert MainWindow._station_command_scheduler_manual_qsy_active_for_radio(window, 9) is False
    assert MainWindow._station_command_timed_suspend_active_for_radio(window, 8) is False

    app.processEvents()


def test_phase7_station_command_manual_service_suppresses_legacy_cross_radio_state(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.core.shared_state import SchedulerManualControlState
    from freqinout.gui.main_window import MainWindow

    class FakeManualControlService:
        def get_state(self, radio_id):
            if int(radio_id) == 8:
                return SchedulerManualControlState(radio_profile_id="radio_8", state="manual_qsy")
            return None

    window = MainWindow.__new__(MainWindow)
    window.scheduler = SimpleNamespace(
        _manual_control_service=FakeManualControlService(),
        current_source="QSY",
        _manual_qsy_active=True,
        current_schedule_entry={"target_device_profile_id": 8},
    )
    window._station_command_manual_qsy_meta = {"target_device_profile_id": 8, "frequency": "14.115"}
    window._station_command_manual_qsy_profile_id = 8
    window._station_command_scheduler_suspended_manual = True
    window._station_command_scheduler_suspended_manual_profile_id = 8
    window._station_command_selected_profile_id = 9

    assert MainWindow._station_command_scheduler_manual_qsy_active_for_radio(window, 8) is True
    assert MainWindow._station_command_scheduler_manual_qsy_active_for_radio(window, 9) is False
    assert MainWindow._station_command_scheduler_suspended_manually_for_radio(window, 9) is False

    app.processEvents()


def test_phase7_station_command_card_qsy_uses_card_radio_target(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    qsy_calls = []
    monkeypatch.setattr(main_mod, "perform_qsy", lambda _window, meta: qsy_calls.append(dict(meta)) or True)

    window = MainWindow.__new__(MainWindow)
    window._station_command_selected_profile_id = 1
    window._station_command_manual_qsy_meta = None
    window._station_command_manual_qsy_profile_id = None
    window._active_runtime_profile = {"id": 1}
    window.settings = SimpleNamespace()
    window.action_feedback_service = None
    window.station_command_radio_combo = QComboBox()
    window.station_command_radio_combo.addItem("FIO-A", 1)
    window.station_command_freq_combo = QComboBox()
    window.station_command_freq_combo.addItem("MAGNET 40M", {"freq": 7.115, "group": "MAGNET", "band": "40M"})
    window._activate_station_command_radio = lambda ident: setattr(window, "_active_runtime_profile", {"id": ident}) or True
    window._refresh_station_command_bar = lambda *args, **kwargs: None

    card_combo = QComboBox()
    card_combo.addItem("AMRRON 20M", {"freq": 14.110, "group": "AMRRON", "band": "20M", "mode": "Digi"})
    try:
        handler = MainWindow._station_command_for_radio_qsy(
            window,
            2,
            card_combo,
            window._on_station_command_qsy_now_clicked,
        )
        handler()

        assert qsy_calls == [
            {
                "freq": 14.110,
                "group": "AMRRON",
                "band": "20M",
                "mode": "Digi",
                "target_device_profile_id": 2,
            }
        ]
        assert window._station_command_selected_profile_id == 1
        assert window.station_command_freq_combo.currentText() == "MAGNET 40M"
    finally:
        card_combo.deleteLater()
        window.station_command_radio_combo.deleteLater()
        window.station_command_freq_combo.deleteLater()

    app.processEvents()


def test_phase7_station_command_suspend_buttons_show_countdown(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    window = MainWindow.__new__(MainWindow)
    window.station_command_hold_btn = QPushButton("QSY Suspend")
    window.station_command_suspend_btn = QPushButton("Suspend Scheduler")
    window._station_command_qsy_suspend_base_text = "QSY Suspend"
    window._station_command_suspend_base_text = "Suspend Scheduler"

    try:
        later = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=29 * 60 + 10)
        MainWindow._update_station_command_hold_button_labels(
            window,
            {"active": True, "remaining_sec": 29 * 60 + 10, "until": later},
        )

        assert window.station_command_hold_btn.text() == "Suspended 30m"
        assert window.station_command_suspend_btn.text() == "Suspend Scheduler"
        assert "Scheduler resumes at" in window.station_command_hold_btn.toolTip()

        soon = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=9 * 60 + 42)
        MainWindow._update_station_command_hold_button_labels(
            window,
            {"active": True, "remaining_sec": 9 * 60 + 42, "until": soon},
        )

        assert window.station_command_hold_btn.text() == "Suspended 09:42"
        assert window.station_command_suspend_btn.text() == "Suspend Scheduler"

        MainWindow._update_station_command_hold_button_labels(window, {"active": False})

        assert window.station_command_hold_btn.text() == "QSY Suspend"
        assert window.station_command_suspend_btn.text() == "Suspend Scheduler"
    finally:
        window.station_command_hold_btn.deleteLater()
        window.station_command_suspend_btn.deleteLater()
        app.processEvents()


def test_phase7_station_command_suspend_scheduler_is_indefinite_manual_control(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui import main_window as main_mod
    from freqinout.gui.main_window import MainWindow

    feedback = []
    override_values = []
    cleared_until = []

    monkeypatch.setattr(main_mod, "suspend_schedule_hold", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("timed hold not expected")))
    monkeypatch.setattr(main_mod, "set_scheduler_enabled_override", lambda value: override_values.append(value))
    monkeypatch.setattr(main_mod, "set_suspend_until", lambda _settings, value: cleared_until.append(value))

    class FakeScheduler:
        def __init__(self):
            self.enabled_values = []

        def set_runtime_scheduler_enabled(self, enabled):
            self.enabled_values.append(enabled)

    window = MainWindow.__new__(MainWindow)
    window.scheduler = FakeScheduler()
    window.settings = SimpleNamespace()
    window._station_command_scheduler_suspended_manual = False
    window._publish_station_command_feedback = lambda **payload: feedback.append(payload)
    window._refresh_station_command_bar = lambda *args, **kwargs: None

    MainWindow._on_station_command_pause_clicked(window)

    assert window._station_command_scheduler_suspended_manual is True
    assert window.scheduler.enabled_values == [False]
    assert override_values == [False]
    assert cleared_until == [None]
    assert feedback[-1]["summary"] == "Scheduler suspended."
    assert "until Resume Schedule" in feedback[-1]["detail"]

    app.processEvents()


def test_phase7_station_command_bar_excludes_observer_from_qsy_targets(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    class FakeSettings:
        def all(self):
            return {"operating_groups": []}

        def get(self, _key, default=None):
            return default

        def set(self, _key, _value):
            return None

        def reload(self):
            return None

    snapshot = SimpleNamespace(
        device_profile_id=3,
        name="KiwiSDR",
        device_class="observer",
        runtime_primary=True,
        current_frequency_label="14.230 MHz",
        current_band="20M",
        scheduler_enabled=False,
        status_summary="",
        overall_state="ok",
        ptt_active=False,
        shared_ptt_blocked=False,
        assigned_operating_profile_name="",
        observer_follow_summary="Following icom",
    )

    class FakeManager:
        def get_runtime_snapshots(self, *, force: bool = False):
            return [snapshot]

    class FakeStore:
        def list_device_profiles(self):
            return [
                {
                    "id": 3,
                    "name": "KiwiSDR",
                    "device_class": "observer",
                    "control_backend": "manual",
                    "runtime_active": 1,
                    "runtime_primary": 0,
                }
            ]

    window = MainWindow.__new__(MainWindow)
    window.station_runtime_manager = FakeManager()
    window.multi_radio_store = FakeStore()
    window._station_command_selected_profile_id = None
    window._station_command_bar_loading = False
    window.station_command_radio_combo = QComboBox()
    window.station_command_now_label = QLabel()
    window.station_command_state_label = QLabel()
    window.station_command_freq_combo = QComboBox()
    window.station_command_next_label = QLabel()
    window.station_command_duration_combo = QComboBox()
    window.station_command_qsy_btn = QPushButton("QSY Now")
    window.station_command_hold_btn = QPushButton("QSY Suspend")
    window.station_command_suspend_btn = QPushButton("Suspend Scheduler")
    window.station_command_resume_btn = QPushButton("Resume Schedule")
    window.station_command_health_label = QLabel("Health:")
    window.station_command_health_widget = QWidget()
    window.station_command_health_layout = QHBoxLayout(window.station_command_health_widget)
    window.station_command_health_leds = {}
    window.station_command_health_text_labels = {}
    window.settings = FakeSettings()
    window.dependency_status_service = SimpleNamespace(software_status_snapshot=lambda: {})

    MainWindow._refresh_station_command_bar(window, force=True)

    assert window.station_command_radio_combo.currentText() == "No configured radios"
    assert window.station_command_state_label.text() == "No configured radio"
    assert window.station_command_qsy_btn.isEnabled() is False

    for widget in (
        window.station_command_radio_combo,
        window.station_command_now_label,
        window.station_command_state_label,
        window.station_command_freq_combo,
        window.station_command_next_label,
        window.station_command_duration_combo,
        window.station_command_qsy_btn,
        window.station_command_hold_btn,
        window.station_command_suspend_btn,
        window.station_command_resume_btn,
        window.station_command_health_widget,
        window.station_command_health_label,
    ):
        widget.deleteLater()
    app.processEvents()


def test_phase7_collapsed_station_group_shows_health_alert(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow
    from freqinout.gui.theme import get_theme

    window = MainWindow.__new__(MainWindow)
    header = QPushButton("Station")
    window._nav_group_order = ["Station"]
    window._nav_group_headers = {"Station": header}
    window._nav_group_states = {"Station": False}
    window._ncs_net_active = {}
    window._station_health_alert_summary = {"issue_count": 2, "severity": "danger"}
    window._nav_button_alignment_style = MainWindow._nav_button_alignment_style
    window._sync_nav_group_header_font = MethodType(MainWindow._sync_nav_group_header_font, window)
    window._set_nav_group_header_visual_state = MethodType(MainWindow._set_nav_group_header_visual_state, window)
    window._station_health_alert_counts = MethodType(MainWindow._station_health_alert_counts, window)

    MainWindow._update_nav_group_header_styles(window, get_theme("light"))

    assert "Station Health: 2 responsiveness issues" in header.toolTip()
    assert "#C62828" in header.styleSheet()

    header.deleteLater()
    app.processEvents()


def test_phase7_station_control_center_health_details_affordance(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.station_overview_tab import StationOverviewTab

    tab = StationOverviewTab()
    tab._control_center_snapshots = [SimpleNamespace(device_profile_id=42, name="Portable SDR")]
    captured: list[tuple[int, str]] = []
    tab.health_details_requested.connect(lambda profile_id, name: captured.append((profile_id, name)))

    tab._request_health_details_for_row(0)

    assert captured == [(42, "Portable SDR")]

    tab.deleteLater()
    app.processEvents()


def test_phase7_station_health_focus_scope_selects_matching_row(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.table.setRowCount(2)
    tab.table.setItem(0, 0, QTableWidgetItem("Station-wide"))
    tab.table.setItem(1, 0, QTableWidgetItem("Portable SDR"))
    tab._pending_focus_scope = "Portable SDR"

    tab._apply_pending_focus_scope()

    assert tab.table.currentRow() == 1
    assert tab._pending_focus_scope == ""
    assert tab._pending_focus_radio_id is None

    tab.table.selectRow(0)
    tab._apply_pending_focus_scope()

    assert tab.table.currentRow() == 0

    tab.deleteLater()
    app.processEvents()


def test_phase7_station_control_center_wires_health_detail_navigation() -> None:
    overview_source = Path("freqinout/gui/station_overview_tab.py").read_text(encoding="utf-8")
    health_source = Path("freqinout/gui/station_health_tab.py").read_text(encoding="utf-8")
    main_source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "CONTROL_CENTER_HEALTH_COLUMN = 4" in overview_source
    assert "health_details_requested = Signal(int, str)" in overview_source
    assert "self.control_center_table.cellClicked.connect(self._on_control_center_cell_clicked)" in overview_source
    assert "self.control_center_table.cellDoubleClicked.connect(self._on_control_center_cell_clicked)" in overview_source
    assert "if int(column) == self.CONTROL_CENTER_HEALTH_COLUMN:" in overview_source
    assert "Open Health Details for this radio or SDR." in overview_source
    assert "def focus_scope(" in health_source
    assert "def _apply_pending_focus_scope(self) -> None:" in health_source
    assert "def _clear_pending_focus_scope(self) -> None:" in health_source
    assert "self.station_overview_tab.health_details_requested.connect(self._open_station_health_detail)" in main_source
    assert 'idx = self._screen_index_by_label.get("Station Health", -1)' in main_source


def test_phase7_settings_sections_use_bounded_fit_content_layouts() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "op_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
    assert "logging_section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
    assert "self.operating_profiles_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
    assert "self.operating_profiles_section_group = operating_group" in source
    assert 'self._make_collapsible_group(\n            "Operating Models",' in source
    assert "fit_content_in_stack=True" in source
    assert "self.sections_stack.currentChanged.connect(lambda _idx: self._sync_current_section_scroll_size())" in source
    assert "self.sections_scroll.setAlignment(Qt.AlignLeft | Qt.AlignTop)" in source
    assert "self.sections_stack.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)" in source
    assert 'self.settings_section_nav_title = QLabel("Settings")' in source
    assert 'self.global_settings_toggle_btn.setText("Main Settings")' in source
    assert 'self.radio_settings_toggle_btn.setText("Radio Settings")' in source
    assert "self.global_settings_toggle_btn.setVisible(False)" in source
    assert "self.radio_settings_toggle_btn.setVisible(False)" in source


def test_settings_configuration_assistant_spec_tracks_next_ia_work() -> None:
    spec = Path("docs/internal/settings_configuration_assistant_spec.md").read_text(encoding="utf-8")
    settings_source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "Condition Alerts" in spec
    assert "Operating Models" in spec
    assert "JS8Call, Spotter, And CommStat Setup" in spec
    assert "Fast Light Profile Creation" in spec
    assert "Guided FreqPlanner" in spec
    assert "VarAC BBS" in spec
    assert "VarAC Cluster node configuration guidance" in spec
    assert "single-instance VarAC and normal BBS monitoring do not require" in spec
    assert "Manage file purge/retention by BBS location" in spec
    assert "Treat BBS as a message entity under Messages" in spec
    assert "Treat BBS file management as FIO-owned" in spec
    assert "adds a BBS focus in Messages" in spec
    assert "preview of the managed BBS structure" in spec
    assert "configurable sweeper from VarAC BBS Inbox and FLMsg/FLAmp inputs" in spec
    assert "multiple managed BBS locations" in spec
    assert "radio-scoped Settings review surface" in spec
    assert "varacBbsSettingsTabs" in settings_source
    assert 'bbs_tabs.addTab(paths_tab, "Radio Paths")' in settings_source
    assert 'bbs_tabs.addTab(bbs_settings_tab, "Radio Live BBS")' in settings_source
    assert 'bbs_tabs.addTab(vault_tab, "Shared Library")' in settings_source
    assert 'bbs_tabs.addTab(preview_tab, "Visitor Preview")' in settings_source
    assert 'bbs_tabs.addTab(sweeper_tab, "Shared Sweeper")' in settings_source
    assert 'bbs_tabs.addTab(vguard_tab, "Access Guard")' in settings_source
    assert "shared Managed BBS Library as the source of truth" in settings_source
    assert "Radio-specific live BBS folder" in settings_source
    assert "BBS file management lives in Messages -> BBS" in settings_source
    assert "BBS Visitor Preview" in settings_source
    assert "BBS Sweeper Rules" in settings_source
    assert "varac_bbs_sweeper_rules_v1" in settings_source
    assert "remove\n  FLMsg/FLAmp content from BBS sync" in spec
    assert "self._add_settings_section(operating_group, scope=\"global\")" in settings_source
    assert "self.device_assignments_table = QTableWidget(0, 7)" in settings_source
    assert "self.schedule_assignments_table = QTableWidget(0, 7)" in settings_source
    assert 'self._settings_nav_context = "main" if scope == "global" else "radios"' in settings_source
    assert 'desired_scope = "radio" if context in {"radio", "radios"} else "global"' in settings_source
    assert 'self.settings_compact_header.setVisible(False)' in settings_source
    assert "self.settings_section_nav_scroll.setObjectName(\"settingsSectionNavScroll\")" in settings_source
    assert "self.settings_section_nav_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in settings_source
    assert "self.settings_section_nav_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in settings_source
    assert "sections_row.addWidget(self.settings_section_nav_scroll, 0)" in settings_source
    assert "def _refresh_settings_nav_scroll_size(self) -> None:" in settings_source
    assert "panel.setMinimumHeight(max(420, int(layout.sizeHint().height())))" in settings_source
    assert 'page_title_label = QLabel(title)' in settings_source
    assert "Single-instance VarAC and normal BBS monitoring do not require cluster mode." in settings_source
    assert "distinct paths, ports, and folders unless sharing is intentional" in settings_source
    assert "header_btn.setVisible(False)" in settings_source
    assert "group.setMaximumHeight(target_height)" in settings_source
    assert "if stacked_mode:\n                    group.setMaximumHeight(16777215)" not in settings_source
    assert 'self._refresh_fit_content_section_height(getattr(self, "operating_profiles_section_group", None))' in settings_source
    assert "self.trusted_hash_table.setMaximumHeight(240)" in settings_source
    assert "self.gpg_keys_table.setMaximumHeight(300)" in settings_source
    assert "gpg_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in settings_source


def test_phase7_settings_hf_groups_use_compact_detail_panel_and_precise_frequencies() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert '"Freq (MHz.kHz.Hz)"' in source
    assert 'freq_edit.setPlaceholderText("e.g., 7.115.000")' in source
    assert 'freq_edit.setPlaceholderText("e.g., 14.115.000")' in source
    assert "def _parse_freq_mhz(val) -> float | None:" in source
    assert 'return f"{mhz}.{khz:03d}.{hz:03d}"' in source
    assert "def _format_freq_storage(self, val) -> str:" in source
    assert 'return f"{freq:.6f}"' in source
    assert "self.op_groups_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "self.op_group_detail_card = QGroupBox(\"Selected Group\")" in source
    assert "self.op_group_list = QListWidget()" in source
    assert "self.op_group_list.setObjectName(\"hfOperatingGroupList\")" in source
    assert "self.op_group_list.setFlow(QListWidget.LeftToRight)" in source
    assert "self.op_group_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in source
    assert "self.op_group_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in source
    assert "op_group_list_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)" in source
    assert "def _refresh_op_group_selector_style(self) -> None:" in source
    assert "QListWidget#hfOperatingGroupList::item:selected" in source
    assert "theme.get('accent', '#2E6F9E')" in source
    assert "def _current_operating_group_name(self) -> str:" in source
    assert "def _refresh_operating_group_config_table(self) -> None:" in source
    assert "def _refresh_op_group_detail_panel(self) -> None:" in source
    assert "for hidden_col in (1, 5, 6, 7, 8, 9):" in source
    assert 'self.edit_group_btn.setVisible(False)' in source
    assert 'self.delete_group_btn = QPushButton("Delete Configuration")' in source
    assert "self.add_group_btn.clicked.connect(self._add_operating_group_inline)" in source
    assert "self.op_group_name_edit = QLineEdit()" in source
    assert "self.op_group_save_btn.clicked.connect(self._save_operating_group_editor)" in source
    assert "def _save_operating_group_editor(self) -> None:" in source
    assert 'op_group_detail_layout.addWidget(self.op_group_detail_summary_label, 0, 0, 1, 4)' in source
    assert 'op_group_detail_layout.addWidget(self.op_group_condition_levels_chk, 4, 3)' in source
    assert "for g in list(self.operating_groups or []):" in source
    assert "self.known_op_group_combo = QComboBox()" in source
    assert 'self.known_op_group_combo.setObjectName("knownOperatingGroupCombo")' in source
    assert 'self.enable_known_group_btn = QPushButton("Enable Group")' in source
    assert 'self.view_known_group_freqs_btn = QPushButton("View Frequencies")' in source
    assert 'self.disable_group_btn = QPushButton("Disable Group")' not in source
    assert "self.enable_known_group_btn.clicked.connect(self._toggle_known_operating_group)" in source
    assert 'self.wefax_station_combo = QComboBox()' in source
    assert 'self.wefax_station_combo.setObjectName("wefaxStationOverrideCombo")' in source
    assert 'self.wefax_station_combo.setVisible(False)' in source
    assert 'show_wefax_override = group.startswith("FLDIGI WEFAX")' in source
    assert "def _on_wefax_station_override_changed(self, _idx: int) -> None:" in source
    assert 'self.local_net_group_combo = QComboBox()' in source
    assert 'self.local_net_group_combo.setObjectName("localCommsGroupCombo")' in source
    assert 'self.local_net_group_list = QListWidget()' in source
    assert 'self.local_net_group_list.setObjectName("localCommsGroupList")' in source
    assert 'self.local_net_group_action_btn = QPushButton("Enable Group")' in source
    assert 'self.local_net_notes_edit = QPlainTextEdit()' in source
    assert "def _toggle_local_net_group(self) -> None:" in source
    assert "def _save_local_net_editor(self) -> None:" in source
    assert "def _refresh_local_net_group_selector_style(self) -> None:" in source
    assert "def _load_known_operating_group_catalog(self) -> None:" in source
    assert "def _enable_known_operating_group(self) -> None:" in source
    assert "def _toggle_known_operating_group(self) -> None:" in source
    assert "def _select_known_operating_group(self, group: object) -> None:" in source
    assert "def _disable_selected_operating_group(self) -> None:" in source
    assert 'button.setText("Disable Group" if group_enabled else "Enable Group")' in source
    assert "load_known_operating_group_catalog()" in source

    qsy_source = Path("freqinout/gui/qsy_helper.py").read_text(encoding="utf-8")
    assert "def parse_frequency_mhz(value) -> Optional[float]:" in qsy_source
    assert 'g["frequency"] = f"{freq:.6f}"' in qsy_source
    assert 'key = f"{fval:.6f}"' in qsy_source

    known_source = Path("freqinout/core/known_operating_groups.py").read_text(encoding="utf-8")
    assert "def build_known_operating_group_catalog" in known_source
    assert "def load_known_operating_group_catalog" in known_source
    assert 'REMOVED_KNOWN_GROUPS = {"AHRN", "RATPACK"}' in known_source
    assert '"group": "JS8CALL STANDARD"' in known_source
    assert '"group": "FT8 STANDARD"' in known_source
    assert '"group": "WSPR STANDARD"' in known_source
    assert '"group": f"FLDIGI WEFAX {station[\'call\']}"' in known_source
    assert '"fldigi_mode": "WEFAX576"' in known_source
    assert "station_grid6" in known_source
    assert "station_call_override" in known_source
    assert "FROM net_resources" in known_source
    assert "sitrepnets-*.json" in known_source


def test_phase7_known_operating_group_catalog_groups_net_resources() -> None:
    from freqinout.core.known_operating_groups import build_known_operating_group_catalog

    catalog = build_known_operating_group_catalog(
        [
            {
                "group_name": "MAGNET",
                "band": "20M",
                "mode": "Digi",
                "frequency": "14.115",
                "resource_set": "Winter",
                "net_name": "MR01",
                "fldigi_mode": "Cont-4/250",
                "fldigi_offset": "900",
            },
            {
                "group_name": "MAGNET",
                "band": "40M",
                "mode": "Digi",
                "frequency": "7.115",
                "resource_set": "Winter",
                "net_name": "MR06",
            },
            {
                "group_name": "MAGNET",
                "band": "40M",
                "mode": "Digi",
                "frequency": "7.115",
                "resource_set": "Winter",
                "net_name": "MR06",
            },
            {
                "group_name": "AHRN",
                "band": "20M",
                "mode": "Digi",
                "frequency": "14.115",
                "resource_set": "Winter",
                "net_name": "AHRN",
            },
        ]
    )

    groups = {entry["group"] for entry in catalog}
    assert "MAGNET" in groups
    assert "AHRN" not in groups
    assert {"JS8CALL STANDARD", "FT8 STANDARD", "WSPR STANDARD", "FLDIGI WEFAX NMC"} <= groups
    magnet = next(entry for entry in catalog if entry["group"] == "MAGNET")
    assert len(magnet["configs"]) == 2
    assert magnet["resource_sets"] == ["Winter"]
    assert magnet["net_names"] == ["MR01", "MR06"]
    assert magnet["configs"][0]["fldigi_mode"] == "Cont-4/250"

    colorado_catalog = build_known_operating_group_catalog([], station_state="CO", timezone_name="America/Denver")
    colorado_wefax = next(entry for entry in colorado_catalog if str(entry["group"]).startswith("FLDIGI WEFAX"))
    assert colorado_wefax["group"] == "FLDIGI WEFAX NMC"
    assert colorado_wefax["configs"][0]["mode"] == "SSB"
    assert colorado_wefax["configs"][0]["fldigi_mode"] == "WEFAX576"
    assert colorado_wefax["configs"][0]["frequency"] == "4.344100"

    new_york_catalog = build_known_operating_group_catalog([], station_state="NY", timezone_name="America/New_York")
    new_york_wefax = next(entry for entry in new_york_catalog if str(entry["group"]).startswith("FLDIGI WEFAX"))
    assert new_york_wefax["group"] == "FLDIGI WEFAX NMF"
    assert new_york_wefax["configs"][0]["frequency"] == "4.233100"

    override_catalog = build_known_operating_group_catalog(
        [],
        station_state="CO",
        timezone_name="America/Denver",
        wefax_station_override="NMG",
    )
    override_wefax = next(entry for entry in override_catalog if str(entry["group"]).startswith("FLDIGI WEFAX"))
    assert override_wefax["group"] == "FLDIGI WEFAX NMG"
    assert "selected by station override" in override_wefax["source_note"]
