from __future__ import annotations

from pathlib import Path
from types import MethodType, SimpleNamespace

from PySide6.QtWidgets import QApplication, QComboBox, QLabel, QPushButton, QTableWidgetItem


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

    assert '("Control Center", "Station Overview")' in source
    assert '("Health Details", "Station Health")' in source
    assert '("Overview", "FreqPlanner")' in source
    assert '("HF Daily", "HF Schedule")' in source
    assert '("HF Nets", "Net Schedule")' in source
    assert '("HF Peers", "Peer Schedules")' in source
    assert 'self._nav_group_order: list[str] = ["Station", "FreqPlanner", "NCS", "Operators"]' in source
    assert 'defaults = {"Station": True, "FreqPlanner": True, "NCS": False, "Operators": False}' in source
    assert 'defaults["Station"] = True' in source
    assert 'defaults["FreqPlanner"] = True' in source
    assert 'if screen in {"Station Overview", "Station Health"}:' in source
    assert 'return "Station"' in source
    assert 'if screen in {"FreqPlanner", "HF Schedule", "Net Schedule", "Peer Schedules"}:' in source
    assert 'return "FreqPlanner"' in source
    assert '("Station Health", "Station Health")' not in source
    assert '"Schedules": False' not in source
    assert "def _expand_nav_group_for_screen" in source
    assert "self._expand_nav_group_for_screen(label)" in source
    assert "if changed:\n            self._persist_nav_group_states()" not in source
    assert 'if key == "Station" and not expanded:' in source
    assert "self._station_health_alert_counts()" in source
    assert "Expand Station or open Health Details." in source


def test_phase7_primary_nav_groups_recover_from_persisted_collapsed_state() -> None:
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

    assert states["Station"] is True
    assert states["FreqPlanner"] is True
    assert states["NCS"] is True
    assert states["Operators"] is True


def test_phase7_station_workspace_decisions_are_specified() -> None:
    spec = Path(
        "/Users/bill/RadioCode/WORK/MultiRig/"
        "FIO_MultiRig_Phase7_Main_Shell_Station_Workspace_UX_Spec_2026-07-26.md"
    ).read_text(encoding="utf-8")

    assert "Station Control And Station Health Consolidate Into One Station Workspace" in spec
    assert "Start With A Read-Only Station Control Center" in spec
    assert "FreqPlanner Owns Schedule Planning" in spec
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
        assert tab.table.isColumnHidden(tab.COL_TARGET) is False

        tab.view_edit_btn.setChecked(True)
        app.processEvents()

        assert all(not tab.table.isColumnHidden(col) for col in range(tab.table.columnCount()))
        assert "Hide advanced" in tab.view_edit_btn.toolTip()

        tab.view_edit_btn.setChecked(False)
        app.processEvents()

        assert tab.table.isColumnHidden(tab.COL_RECURRENCE) is True
        assert tab.table.isColumnHidden(tab.COL_NETNAME) is False
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
        assert tab._net_action_layout.itemAtPosition(1, 0).widget() is tab.move_to_resources_btn
        assert tab._net_resource_filter_layout.itemAtPosition(1, 0).widget() is tab.add_to_schedule_btn

        tab.resize(1400, 900)
        tab._update_net_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "wide"
        assert tab._net_action_layout.itemAtPosition(0, 4).widget() is tab.move_to_resources_btn
        assert tab._net_resource_filter_layout.itemAtPosition(0, 4).widget() is tab.add_to_schedule_btn
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
        assert tab.table.isColumnHidden(tab.COL_TARGET) is False

        tab.view_edit_btn.setChecked(True)
        app.processEvents()

        assert all(not tab.table.isColumnHidden(col) for col in range(tab.table.columnCount()))
        assert "Hide advanced" in tab.view_edit_btn.toolTip()

        tab.view_edit_btn.setChecked(False)
        app.processEvents()

        assert tab.table.isColumnHidden(tab.COL_SOURCE) is True
        assert tab.table.isColumnHidden(tab.COL_GROUP) is False
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
        assert tab._daily_action_layout.itemAtPosition(1, 0).widget() is tab.move_to_resources_btn
        assert tab._daily_resource_filter_layout.itemAtPosition(1, 0).widget() is tab.add_to_schedule_btn

        tab.resize(1400, 900)
        tab._update_daily_responsive_layout()
        app.processEvents()

        assert tab._responsive_layout_mode == "wide"
        assert tab._daily_action_layout.itemAtPosition(0, 4).widget() is tab.move_to_resources_btn
        assert tab._daily_resource_filter_layout.itemAtPosition(0, 4).widget() is tab.add_to_schedule_btn
    finally:
        tab.deleteLater()
        app.processEvents()


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


def test_phase7_station_health_keeps_refresh_out_of_title_header() -> None:
    source = Path("freqinout/gui/station_health_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("def set_scope_resolver")]
    header_block = build_block[build_block.index("header = QHBoxLayout()") : build_block.index("action_row = QHBoxLayout()")]
    action_block = build_block[build_block.index("action_row = QHBoxLayout()") :]

    assert "header.addWidget(self.help_btn" in header_block
    assert "header.addWidget(self.refresh_btn)" not in header_block
    assert "action_row.addWidget(self.refresh_btn" in action_block


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
    plan_block = build_block[build_block.index("plan_workspace = QHBoxLayout()") :]

    assert "header.addWidget(self.time_toggle_btn)" not in header_block
    assert "self.plan_context_label.setVisible(False)" in build_block
    assert "plan_workspace.addWidget(self.time_toggle_btn)" in plan_block


def test_phase7_controlfreq_uses_filter_row_and_hides_context() -> None:
    source = Path("freqinout/gui/controlfreq_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("header = QHBoxLayout()") : source.index("self.readiness_review_widget = QWidget()")]
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
    assert "self.plan_context_label.setVisible(False)" in filter_block


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
        assert tab.freq_ctrl_box.minimumHeight() >= 170
        assert tab.freq_ctrl_box.maximumHeight() == 16777215
        assert tab.intersection_box.minimumHeight() >= 130
        assert tab.schedule_box.minimumHeight() >= 140
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
    assert 'self.station_command_qsy_btn.setObjectName("stationCommandQsy")' in source
    assert 'self.station_command_hold_btn.setObjectName("stationCommandHold")' in source
    assert 'self.station_command_suspend_btn.setObjectName("stationCommandSuspend")' in source
    assert 'self.station_command_resume_btn.setObjectName("stationCommandResume")' in source
    assert "command_layout = QGridLayout(self.station_command_bar)" in source
    assert "command_layout.addWidget(self.station_command_next_label, 1, 0, 1, 5)" in source
    assert "command_layout.addWidget(self.station_command_qsy_btn, 1, 5)" in source
    assert "self.station_command_now_label.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)" in source
    assert "self.station_command_state_label.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)" in source
    assert "right_layout.addWidget(self.station_command_bar, 0)" in source
    assert "right_layout.addWidget(self.stack, stretch=1)" in source
    assert "right_layout.setSpacing(10)" in source
    assert "border-bottom: 2px solid" in source
    assert "station_control_surface" in source
    assert "station_control_border" in source
    assert "station_control_text" in source
    assert "station_control_muted" in source
    assert "QFrame#stationCommandBar QLabel" in source
    assert "background: transparent; color:" in source
    assert '"station_control_surface": "#D7EAF8"' in theme_source
    assert '"station_control_surface": "#12324A"' in theme_source
    assert "self._status_timer.timeout.connect(self._refresh_station_overview)" in source
    assert "self._refresh_station_command_bar(force=False)" in source
    assert "btn.setEnabled(False)" in source
    assert "Command target:" in source
    assert "SUPPORTED_RUNTIME_CONTROL_BACKENDS" in source
    assert "def _station_command_configured_profiles" in source
    assert "def _station_command_is_controllable_profile" in source
    assert "No configured radios" in source
    assert 'device_class == "observer"' in source
    assert "self.station_command_qsy_btn.clicked.connect" not in source
    assert "self.station_command_hold_btn.clicked.connect" not in source
    assert "self.station_command_suspend_btn.clicked.connect" not in source
    assert "self.station_command_resume_btn.clicked.connect" not in source


def test_phase7_sidebar_schedule_actions_removed_from_ledge() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "status_layout.addLayout(hold_row)" not in source
    assert "status_layout.addWidget(self.suspend_schedule_btn" not in source
    assert "status_layout.addWidget(self.resume_schedule_btn" not in source
    assert "def _hide_sidebar_schedule_controls(self) -> None:" in source
    assert "self._hide_sidebar_schedule_controls()" in source
    assert 'QPushButton("Suspend", self.scheduler_status_container)' in source
    assert 'QPushButton("Resume Schedule", self.scheduler_status_container)' in source


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


def test_phase7_messages_workspace_filters_are_below_title_without_context_sentence() -> None:
    source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert 'self.operating_group_filter = DropdownChecklist("Operating Group")' in source
    assert 'self.source_filter = DropdownChecklist("Sources")' in source
    assert 'self.operating_group_filter.setObjectName("messageOperatingGroupFilter")' in source
    assert 'self.source_filter.setObjectName("messageSourceFilter")' in source
    assert "self._inbox_actions_layout = QGridLayout()" in source
    assert "(self.time_toggle_btn, 0, 2)" in source
    assert "(self.operating_group_filter, 0, 3)" in source
    assert "(self.source_filter, 0, 4)" in source
    assert 'fallback_text="Messages uses the current radio and Frequency Plan context' not in source
    assert "self.plan_context_label.setVisible(False)" in source
    assert "def _row_matches_workspace_filters" in source
    assert "def _message_source_options" in source
    assert "def _message_group_options" in source
    assert "def _update_messages_responsive_layout(self) -> None:" in source
    assert "self.compose_splitter.setOrientation(Qt.Vertical if compact else Qt.Horizontal)" in source


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

        assert tab.source_filter.text() == "Sources: All"
        assert tab.operating_group_filter.text() == "Operating Group: All"
        assert tab._row_matches_workspace_filters(rows[0]) is True

        tab.source_filter.set_selected_values(["sitrep"])

        assert tab._row_matches_workspace_filters(rows[0]) is False
        assert tab._row_matches_workspace_filters(rows[1]) is True

        tab.source_filter.set_selected_values([])

        assert tab._row_matches_workspace_filters(rows[1]) is False

        tab.source_filter.set_selected_values(["sitrep"])
        tab.operating_group_filter.set_selected_values(["HF NETS"])

        assert tab._row_matches_workspace_filters(rows[1]) is True
        assert tab._row_matches_workspace_filters(rows[2]) is False

        tab._clear_filters()

        assert tab.source_filter.all_selected() is True
        assert tab.operating_group_filter.all_selected() is True
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
        assert compact_layout.itemAtPosition(0, 3).widget() is tab.operating_group_filter
        assert compact_layout.itemAtPosition(1, 0).widget() is tab.inbox_check_label
        assert compact_layout.itemAtPosition(2, 0).widget() is tab.inbox_bbs_label
        assert tab.compose_splitter.orientation() == Qt.Vertical

        tab.resize(1400, 900)
        tab._update_messages_responsive_layout()
        app.processEvents()

        wide_layout = tab._inbox_actions_layout
        assert tab._responsive_layout_mode == "wide"
        assert wide_layout.itemAtPosition(0, 5).widget() is tab.inbox_check_label
        assert wide_layout.itemAtPosition(0, 10).widget() is tab.inbox_bbs_label
        assert tab.compose_splitter.orientation() == Qt.Horizontal
    finally:
        tab.deleteLater()
        app.processEvents()


def test_phase7_station_command_bar_refresh_selects_primary_radio(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

    snapshots = [
        SimpleNamespace(
            device_profile_id=1,
            name="DX10",
            device_class="tx_rx",
            runtime_primary=False,
            current_frequency_label="7.078 MHz",
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
            current_frequency_label="14.078 MHz",
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

    window = MainWindow.__new__(MainWindow)
    window.station_runtime_manager = FakeManager()
    window.multi_radio_store = FakeStore()
    window._station_command_selected_profile_id = None
    window._station_command_bar_loading = False
    window.station_command_radio_combo = QComboBox()
    window.station_command_now_label = QLabel()
    window.station_command_state_label = QLabel()
    window.station_command_next_label = QLabel()
    window.station_command_qsy_btn = QPushButton("QSY...")
    window.station_command_hold_btn = QPushButton("Hold")
    window.station_command_suspend_btn = QPushButton("Suspend")
    window.station_command_resume_btn = QPushButton("Resume")

    MainWindow._refresh_station_command_bar(window, force=True)

    assert [window.station_command_radio_combo.itemText(idx) for idx in range(window.station_command_radio_combo.count())] == [
        "DX10 (HF)",
        "icom (HF)",
        "Spare Rig (HF)",
    ]
    assert window.station_command_radio_combo.currentData() == 2
    assert window.station_command_now_label.text() == "Now: 14.078 MHz 20M"
    assert window.station_command_state_label.text() == "State: Manual Hold"
    assert window.station_command_next_label.text() == "Next: Plan: Net Plan"
    assert window.station_command_qsy_btn.isEnabled() is False
    assert "Command target: icom" in window.station_command_qsy_btn.toolTip()

    window.station_command_radio_combo.setCurrentIndex(0)
    MainWindow._on_station_command_radio_changed(window, 0)

    assert window._station_command_selected_profile_id == 1
    assert window.station_command_now_label.text() == "Now: 7.078 MHz 40M"

    window.station_command_radio_combo.setCurrentIndex(2)
    MainWindow._on_station_command_radio_changed(window, 2)

    assert window._station_command_selected_profile_id == 4
    assert window.station_command_now_label.text() == "Now: unavailable"
    assert window.station_command_state_label.text() == "State: Configured inactive"
    assert window.station_command_next_label.text() == "Next: Plan: Backup Plan"

    for widget in (
        window.station_command_radio_combo,
        window.station_command_now_label,
        window.station_command_state_label,
        window.station_command_next_label,
        window.station_command_qsy_btn,
        window.station_command_hold_btn,
        window.station_command_suspend_btn,
        window.station_command_resume_btn,
    ):
        widget.deleteLater()
    app.processEvents()


def test_phase7_station_command_bar_handles_no_configured_radio(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

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
    window.station_command_next_label = QLabel()
    window.station_command_qsy_btn = QPushButton("QSY...")
    window.station_command_hold_btn = QPushButton("Hold")
    window.station_command_suspend_btn = QPushButton("Suspend")
    window.station_command_resume_btn = QPushButton("Resume")

    MainWindow._refresh_station_command_bar(window, force=True)

    assert window._station_command_selected_profile_id is None
    assert window.station_command_radio_combo.currentText() == "No configured radios"
    assert window.station_command_now_label.text() == "Now: unavailable"
    assert window.station_command_state_label.text() == "State: no configured radio"
    assert window.station_command_next_label.text() == "Next: none"
    assert window.station_command_qsy_btn.isEnabled() is False
    assert window.station_command_qsy_btn.toolTip() == "No configured radio is available for station commands."

    for widget in (
        window.station_command_radio_combo,
        window.station_command_now_label,
        window.station_command_state_label,
        window.station_command_next_label,
        window.station_command_qsy_btn,
        window.station_command_hold_btn,
        window.station_command_suspend_btn,
        window.station_command_resume_btn,
    ):
        widget.deleteLater()
    app.processEvents()


def test_phase7_station_command_bar_excludes_observer_from_qsy_targets(monkeypatch) -> None:
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.main_window import MainWindow

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
    window.station_command_next_label = QLabel()
    window.station_command_qsy_btn = QPushButton("QSY...")
    window.station_command_hold_btn = QPushButton("Hold")
    window.station_command_suspend_btn = QPushButton("Suspend")
    window.station_command_resume_btn = QPushButton("Resume")

    MainWindow._refresh_station_command_bar(window, force=True)

    assert window.station_command_radio_combo.currentText() == "No configured radios"
    assert window.station_command_state_label.text() == "State: no configured radio"
    assert window.station_command_qsy_btn.isEnabled() is False

    for widget in (
        window.station_command_radio_combo,
        window.station_command_now_label,
        window.station_command_state_label,
        window.station_command_next_label,
        window.station_command_qsy_btn,
        window.station_command_hold_btn,
        window.station_command_suspend_btn,
        window.station_command_resume_btn,
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
    assert 'self._make_collapsible_group(\n            "Frequency Plans",' in source
    assert "fit_content_in_stack=True" in source
    assert "self.sections_stack.currentChanged.connect(lambda _idx: self._sync_current_section_scroll_size())" in source
    assert "if stacked_mode:\n                    group.setMaximumHeight(16777215)" in source
    assert 'self._refresh_fit_content_section_height(getattr(self, "operating_profiles_section_group", None))' in source
    assert "self.trusted_hash_table.setMaximumHeight(240)" in source
    assert "self.gpg_keys_table.setMaximumHeight(300)" in source
    assert "gpg_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in source
