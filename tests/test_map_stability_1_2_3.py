from __future__ import annotations

from pathlib import Path
import sqlite3
from types import SimpleNamespace

from freqinout.core.message_intelligence import TOPIC_TAXONOMY, analyze_spotter_text
from freqinout.core.observation_projection import (
    Observation,
    observation_from_local_report,
    observation_from_message_intelligence,
    observation_from_rf_pin,
)
from freqinout.core.observation_store import upsert_observation
from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer
from freqinout.core.settings_manager import SettingsManager
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.stations_map_tab import StationPoint, StationsMapTab


def _bare_tab() -> StationsMapTab:
    tab = StationsMapTab.__new__(StationsMapTab)
    tab._is_shutting_down = False
    tab._map_link_status_detail = "Links hidden."
    tab._map_visible = True
    tab._map_initialized = False
    tab._map_dirty = False
    tab._map_page_loading = False
    tab._render_requested_during_load = False
    tab._render_pending = False
    tab._last_map_render_ts = 0.0
    tab._map_load_ok = False
    tab._ingest_started = False
    tab._deferred_initial_ingest_pending = False
    tab._pending_map_payload = None
    tab._last_map_payload_sig = None
    tab._last_js8_load_ts = 0.0
    tab._last_exit_ts = 0.0
    tab._js8_rx_hub = None
    tab._now_reachable_enabled = False
    tab._observation_focus_enabled = False
    tab._observation_focus_mode = ""
    tab._paths_previous_observation_focus = None
    tab._paths_focus_station = ""
    tab._map_last_link_source_rows = 0
    tab._map_last_link_missing_position_rows = 0
    tab._map_last_link_all_time_count = 0
    tab._map_last_error = ""
    tab.selected_band = ""
    tab._query_cache_ttl_sec = 3.0
    tab._query_cache = {}
    tab._map_query_cache = {}
    tab.show_station_markers = True
    tab.show_link_paths = False
    tab.show_rf_pins = False
    tab.link_mode = "off"
    tab.link_value = ""
    tab.relay_target = ""
    tab._map_stack = None
    tab._map_loading_label = None
    tab._map_selected_paths_btn = None
    tab.settings = {}
    tab.web = object()
    return tab


class _FakePage:
    def __init__(self) -> None:
        self.scripts: list[str] = []

    def runJavaScript(self, script: str) -> None:
        self.scripts.append(script)


class _FakeWeb:
    def __init__(self) -> None:
        self.page_obj = _FakePage()

    def page(self) -> _FakePage:
        return self.page_obj


class _FakeLabel:
    def __init__(self) -> None:
        self.text = ""
        self.tooltip = ""

    def setText(self, value: str) -> None:
        self.text = value

    def setToolTip(self, value: str) -> None:
        self.tooltip = value


class _FakeLineEdit:
    def __init__(self, text: str = "") -> None:
        self._text = text

    def setText(self, value: str) -> None:
        self._text = str(value or "")

    def text(self) -> str:
        return self._text


class _FakeCard:
    def __init__(self) -> None:
        self.style = ""

    def setStyleSheet(self, value: str) -> None:
        self.style = value


class _FakeCombo:
    def __init__(self, items: list[tuple[str, object]]) -> None:
        self.items = items
        self.index = 0
        self.blocked = False
        self.edit_text = ""

    def blockSignals(self, value: bool) -> None:
        self.blocked = bool(value)

    def count(self) -> int:
        return len(self.items)

    def itemData(self, idx: int) -> object:
        return self.items[idx][1]

    def itemText(self, idx: int) -> str:
        return self.items[idx][0]

    def currentData(self) -> object:
        return self.items[self.index][1]

    def currentText(self) -> str:
        return self.items[self.index][0]

    def setCurrentIndex(self, idx: int) -> None:
        self.index = idx

    def findText(self, text: str) -> int:
        for idx, item in enumerate(self.items):
            if item[0] == text:
                return idx
        return -1

    def findData(self, value: object) -> int:
        for idx, item in enumerate(self.items):
            if item[1] == value:
                return idx
        return -1

    def addItem(self, text: str, data: object) -> None:
        self.items.append((text, data))

    def isEditable(self) -> bool:
        return True

    def setEditText(self, text: str) -> None:
        self.edit_text = str(text or "")


class _FakeCheck:
    def __init__(self, checked: bool = False) -> None:
        self.checked = bool(checked)
        self.blocked = False

    def blockSignals(self, value: bool) -> None:
        self.blocked = bool(value)

    def setChecked(self, value: bool) -> None:
        self.checked = bool(value)


class _FakeButton:
    def __init__(self) -> None:
        self.enabled: bool | None = None

    def setEnabled(self, value: bool) -> None:
        self.enabled = bool(value)


def test_map_auto_ingest_uses_background_controller_before_local_ingest() -> None:
    tab = _bare_tab()
    tab._app_active = True
    calls: list[str] = []
    tab._request_background_ingest = lambda *kinds: calls.append(f"background:{','.join(kinds)}") or True
    tab._ingest_js8_logs = lambda **_kwargs: calls.append("local-js8") or 0
    tab._schedule_render = lambda: calls.append("render")
    tab._emit_map_event = lambda *_args, **_kwargs: None

    StationsMapTab._auto_ingest_and_refresh(tab, initial=False)

    assert calls == ["background:js8_links,varac", "render"]


def test_map_selected_detail_actions_apply_existing_filters() -> None:
    tab = _bare_tab()
    refreshes: list[str] = []
    cleared: list[str] = []
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab.group_filter_combo = _FakeCombo([("All", ""), ("MAGNET", "MAGNET"), ("MR08", "MR08")])
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire"), ("Comms", "Comms")])
    tab._request_map_refresh = lambda **kwargs: refreshes.append(str(kwargs.get("reason") or ""))
    tab._clear_report_query_caches = lambda: cleared.append("cleared")

    StationsMapTab._handle_map_detail_action(tab, {"action": "filter_group", "group": "@MR08"})
    StationsMapTab._handle_map_detail_action(tab, {"action": "filter_topic", "topic": "Fire"})

    assert tab.group_filter_combo.index == 2
    assert tab._map_topic_filter_combo.index == 1
    assert refreshes == ["selected_detail_group", "selected_detail_topic"]
    assert cleared == ["cleared"]


def test_map_selected_station_paths_action_uses_existing_path_controls() -> None:
    tab = _bare_tab()
    refreshes: list[str] = []
    tab._map_selected_payload = {"type": "station", "title": "K7ETC"}
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("All", ("all", ""))])
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    tab.relay_target_combo = _FakeCombo([("", ""), ("K7ETC | Keith | UT | MR08", "K7ETC")])
    tab.map_links_chk = _FakeCheck(False)
    tab._refresh_relay_targets = lambda: None
    tab._update_map_mode_buttons = lambda: None
    tab._update_map_view_status_label = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    tab._request_map_refresh = lambda **kwargs: refreshes.append(str(kwargs.get("reason") or ""))

    StationsMapTab._show_paths_for_selected_station(tab)

    assert tab.show_link_paths is True
    assert tab.map_links_chk.checked is True
    assert tab.link_mode == "station"
    assert tab.link_value == "K7ETC"
    assert tab._paths_focus_station == "K7ETC"
    assert tab.relay_target == ""
    assert tab._map_path_scope_combo.currentData() == ("station", "K7ETC")
    assert tab._map_path_scope_combo.currentText() == "Selected: K7ETC"
    assert refreshes == ["selected_detail_paths"]

    StationsMapTab._show_paths_for_selected_station(tab)

    assert tab.link_mode_combo.index == 0
    assert tab._map_path_scope_combo.currentData() == ("off", "")
    assert tab.show_link_paths is False
    assert tab.map_links_chk.checked is False
    assert tab.link_mode == "off"
    assert tab.link_value == ""
    assert tab._paths_focus_station == ""
    assert refreshes == ["selected_detail_paths", "selected_detail_paths_off"]


def test_selected_station_paths_toggle_restores_previous_report_context() -> None:
    tab = _bare_tab()
    tab._map_selected_payload = {"type": "station", "title": "K7ETC"}
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "hf_reports"
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("All", ("all", ""))])
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    tab.map_links_chk = _FakeCheck(False)
    tab._refresh_relay_targets = lambda: None
    tab._update_map_mode_buttons = lambda: None
    tab._update_map_view_status_label = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    tab._request_map_refresh = lambda **_kwargs: None

    StationsMapTab._show_paths_for_selected_station(tab)
    assert tab._observation_focus_mode == "paths"

    StationsMapTab._show_paths_for_selected_station(tab)

    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "hf_reports"


def test_map_selected_center_falls_back_to_station_coordinates() -> None:
    tab = _bare_tab()
    web = _FakeWeb()
    tab.web = web
    tab._map_selected_payload = {"type": "station", "title": "K7ETC"}
    tab.stations = [StationPoint(callsign="K7ETC", grid="DM38ST", lat=38.5, lon=-112.5)]

    StationsMapTab._center_map_selected_detail(tab)

    assert web.page_obj.scripts
    assert "centerMapOn(38.500000, -112.500000, 6)" in web.page_obj.scripts[0]


def test_map_selected_detail_center_button_uses_station_coordinate_fallback() -> None:
    tab = _bare_tab()
    button = _FakeButton()
    tab._map_selected_panel = SimpleNamespace(setVisible=lambda _value: None)
    tab._map_selected_title = SimpleNamespace(setText=lambda _text: None)
    tab._map_selected_subtitle = SimpleNamespace(setText=lambda _text: None, setVisible=lambda _value: None)
    tab._map_selected_body = SimpleNamespace(setHtml=lambda _html: None)
    tab._map_selected_status_body = SimpleNamespace(setHtml=lambda _html: None)
    tab._map_selected_paths_body = SimpleNamespace(setHtml=lambda _html: None)
    tab._map_selected_messages_body = SimpleNamespace(setHtml=lambda _html: None)
    tab._map_selected_tabs = None
    tab._map_selected_center_btn = button
    tab._map_selected_paths_btn = None
    tab._map_selected_group_btn = None
    tab._map_selected_topic_btn = None
    tab._map_selected_messages_btn = None
    tab._map_selected_spotter_btn = None
    tab._map_selected_sop_btn = None
    tab._map_canvas_splitter = None
    tab.stations = [StationPoint(callsign="K7ETC", grid="DM38ST", lat=38.5, lon=-112.5)]

    StationsMapTab._show_map_selected_detail(tab, {"type": "station", "title": "K7ETC"})

    assert button.enabled is True


def test_map_path_scope_combo_controls_link_layer_without_filter_side_effects() -> None:
    tab = _bare_tab()
    refreshes: list[str] = []
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("All", ("all", ""))])
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    tab.map_links_chk = _FakeCheck(False)
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda *_args, **_kwargs: None
    tab._update_map_view_status_label = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    tab._request_map_refresh = lambda **kwargs: refreshes.append(str(kwargs.get("reason") or ""))

    tab._map_path_scope_combo.setCurrentIndex(2)
    StationsMapTab._on_map_path_scope_changed(tab, 2)

    assert tab.show_link_paths is True
    assert tab.map_links_chk.checked is True
    assert tab.link_mode == "all"
    assert tab.link_value == ""
    assert tab.link_mode_combo.currentData() == ("all", "")
    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "paths"

    tab._map_path_scope_combo.setCurrentIndex(0)
    StationsMapTab._on_map_path_scope_changed(tab, 0)

    assert tab.show_link_paths is False
    assert tab.map_links_chk.checked is False
    assert tab.link_mode == "off"
    assert tab._observation_focus_enabled is False
    assert refreshes == ["path_scope", "path_scope"]


def test_path_scope_off_restores_previous_report_context() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    tab.map_links_chk = _FakeCheck(False)
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda *_args, **_kwargs: None
    tab._update_map_view_status_label = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    tab._request_map_refresh = lambda **_kwargs: None

    tab._map_path_scope_combo.setCurrentIndex(2)
    StationsMapTab._on_map_path_scope_changed(tab, 2)
    assert tab._observation_focus_mode == "paths"

    tab._map_path_scope_combo.setCurrentIndex(0)
    StationsMapTab._on_map_path_scope_changed(tab, 0)

    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "all_reports"


def test_map_selected_detail_panel_has_operator_context_tabs() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    panel_block = source[
        source.index("def _build_map_selected_detail_panel") : source.index("def _clear_map_selected_detail")
    ]

    assert "QTabWidget" in source
    assert 'tabs.addTab(overview_body, "Overview")' in panel_block
    assert 'tabs.addTab(status_body, "Status")' in panel_block
    assert 'tabs.addTab(paths_body, "Paths")' in panel_block
    assert 'tabs.addTab(messages_body, "Messages")' in panel_block


def test_map_selected_status_explains_marker_without_raw_question_noise() -> None:
    tab = _bare_tab()
    html = StationsMapTab._map_selected_status_html(
        tab,
        {
            "type": "station",
            "title": "KC7WOK",
            "group": "MAGNET",
            "rows": [
                {"label": "Area", "value": "MT / DN28FI"},
                {"label": "SitRep", "value": "Functioning"},
                {"label": "Updated", "value": "3h"},
                {"label": "Source", "value": "fused"},
            ],
        },
        summary="Phone landline functioning?",
    )

    assert "Station Status" in html
    assert "Functioning" in html
    assert "Multiple Sources" in html
    assert "Phone landline functioning" not in html
    assert "No latest status report detail" in html


def test_map_selected_station_detail_suppresses_raw_question_prompt() -> None:
    tab = _bare_tab()
    html = StationsMapTab._map_selected_detail_html(
        tab,
        {
            "type": "station",
            "title": "KC7WOK",
            "group": "MAGNET",
            "rows": [
                {"label": "Area", "value": "MT / DN28FI"},
                {"label": "SitRep", "value": "Functioning"},
                {"label": "Marker", "value": "Green: latest status is functioning"},
                {"label": "Source", "value": "Multiple Sources"},
            ],
        },
        summary="Phone landline functioning?",
    )

    assert "Station Activity" in html
    assert "Functioning" in html
    assert "Green: latest status is functioning" in html
    assert "Phone landline functioning" not in html


def test_map_selected_paths_html_names_topology_scope() -> None:
    tab = _bare_tab()
    tab._map_selected_payload = {"type": "station", "title": "K7ETC"}
    tab.show_link_paths = True
    tab.link_mode = "station"
    tab.link_value = "K7ETC"

    html = StationsMapTab._map_selected_paths_html(tab, tab._map_selected_payload)

    assert "Path Topology" in html
    assert "K7ETC" in html
    assert "Selected K7ETC" in html
    assert "who reported hearing whom" in html


def test_map_current_link_selection_prefers_programmatic_station_focus() -> None:
    tab = _bare_tab()
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", ""))])
    tab.show_link_paths = True
    tab.link_mode = "station"
    tab.link_value = "K7ETC"

    assert StationsMapTab._current_link_selection(tab) == ("station", "K7ETC")
    assert StationsMapTab._links_active(tab)


def test_hidden_path_layer_is_not_an_active_filter_even_with_saved_scope() -> None:
    tab = _bare_tab()
    tab.show_link_paths = False
    tab.link_mode = "station"
    tab.link_value = "K7ETC"
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("K7ETC", ("station", "K7ETC"))])
    tab.link_mode_combo.setCurrentIndex(1)

    assert StationsMapTab._current_link_selection(tab) == ("off", "")
    assert StationsMapTab._links_active(tab) is False


def test_links_layer_toggle_off_clears_path_scope_and_restores_report_context() -> None:
    tab = _bare_tab()
    refreshes: list[str] = []
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "paths"
    tab._paths_previous_observation_focus = (True, "all_reports")
    tab.show_link_paths = True
    tab.link_mode = "station"
    tab.link_value = "K7ETC"
    tab._paths_focus_station = "K7ETC"
    tab.relay_target = "K7ETC"
    tab.map_links_chk = _FakeCheck(True)
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("K7ETC", ("station", "K7ETC"))])
    tab.link_mode_combo.setCurrentIndex(2)
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("K7ETC", ("station", "K7ETC"))])
    tab._map_path_scope_combo.setCurrentIndex(2)
    tab.relay_target_combo = _FakeCombo([("", ""), ("K7ETC", "K7ETC")])
    tab._save_display_preferences = lambda: None
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda *_args, **_kwargs: None
    tab._update_map_view_status_label = lambda: None
    tab._update_clear_filter_buttons_visual = lambda *_args, **_kwargs: None
    tab._request_map_refresh = lambda **kwargs: refreshes.append(str(kwargs.get("reason") or ""))

    StationsMapTab._on_map_links_changed(tab, False)

    assert tab.show_link_paths is False
    assert tab.link_mode == "off"
    assert tab.link_value == ""
    assert tab._paths_focus_station == ""
    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "all_reports"
    assert tab.link_mode_combo.index == 0
    assert tab._map_path_scope_combo.index == 0
    assert refreshes == ["toggle_link_paths"]


def test_links_layer_toggle_on_defaults_to_my_station_scope() -> None:
    tab = _bare_tab()
    refreshes: list[str] = []
    tab.show_link_paths = False
    tab.link_mode = "off"
    tab.link_value = ""
    tab.map_links_chk = _FakeCheck(False)
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    tab._save_display_preferences = lambda: None
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda *_args, **_kwargs: None
    tab._update_map_view_status_label = lambda: None
    tab._update_clear_filter_buttons_visual = lambda *_args, **_kwargs: None
    tab._request_map_refresh = lambda **kwargs: refreshes.append(str(kwargs.get("reason") or ""))

    StationsMapTab._on_map_links_changed(tab, True)

    assert tab.show_link_paths is True
    assert tab.link_mode == "my_station"
    assert tab.link_value == ""
    assert tab.link_mode_combo.currentData() == ("my_station", "")
    assert tab._map_path_scope_combo.currentData() == ("my_station", "")
    assert StationsMapTab._links_active(tab) is True
    assert refreshes == ["toggle_link_paths"]


def test_clear_map_layers_turns_off_path_focus_and_preserves_filters() -> None:
    tab = _bare_tab()
    refreshes: list[str] = []
    tab.show_link_paths = True
    tab.link_mode = "station"
    tab.link_value = "K7ETC"
    tab._paths_focus_station = "K7ETC"
    tab.relay_target = "K7ETC"
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("All", ("all", ""))])
    tab.link_mode_combo.setCurrentIndex(2)
    tab.relay_target_combo = _FakeCombo([("", ""), ("K7ETC", "K7ETC")])
    tab.map_links_chk = _FakeCheck(True)
    tab._clear_report_query_caches = lambda: None
    tab._update_sitrep_status_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda: None
    tab._update_map_view_status_label = lambda: None
    tab._update_now_reachable_summary = lambda: None
    tab._refresh_relay_targets = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    tab._request_map_refresh = lambda **kwargs: refreshes.append(str(kwargs.get("reason") or ""))

    StationsMapTab.clear_map_layers(tab)

    assert tab.show_link_paths is False
    assert tab.map_links_chk.checked is False
    assert tab.link_mode == "off"
    assert tab.link_value == ""
    assert tab.relay_target == ""
    assert tab._paths_focus_station == ""
    assert tab.link_mode_combo.index == 0
    assert refreshes == ["clear_map_layers"]


def test_clear_map_layers_preserves_topic_traffic_refinement() -> None:
    tab = _bare_tab()
    refreshes: list[str] = []
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "propagation"
    tab.prop_overlay_enabled = True
    tab.show_station_markers = True
    tab.show_link_paths = True
    tab.show_alert_reports = False
    tab.show_infrastructure_reports = False
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire")])
    tab._map_topic_filter_combo.setCurrentIndex(1)
    tab._map_search_edit = SimpleNamespace(text=lambda: "")
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", ""))])
    tab.link_mode_combo.setCurrentIndex(1)
    tab.relay_target_combo = _FakeCombo([("", "")])
    tab.map_stations_chk = _FakeCheck(True)
    tab.map_links_chk = _FakeCheck(True)
    tab.map_alerts_chk = _FakeCheck(False)
    tab.map_infrastructure_chk = _FakeCheck(False)
    tab.prop_overlay_chk = _FakeCheck(True)
    tab._clear_report_query_caches = lambda: None
    tab._update_sitrep_status_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda: None
    tab._update_map_view_status_label = lambda: None
    tab._update_now_reachable_summary = lambda: None
    tab._refresh_relay_targets = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    tab._request_map_refresh = lambda **kwargs: refreshes.append(str(kwargs.get("reason") or ""))

    StationsMapTab.clear_map_layers(tab)

    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "all_reports"
    assert tab.prop_overlay_enabled is False
    assert tab.show_link_paths is False
    assert tab.show_station_markers is False
    assert tab.show_alert_reports is True
    assert tab.show_infrastructure_reports is True
    assert tab.map_stations_chk.checked is False
    assert tab.map_alerts_chk.checked is True
    assert tab.map_infrastructure_chk.checked is True
    assert StationsMapTab._current_map_mode_key(tab) == "reports"
    assert refreshes == ["clear_map_layers"]


def test_map_topic_action_promotes_all_stations_to_all_traffic_context() -> None:
    tab = _bare_tab()
    calls: list[tuple[str, str, str]] = []
    tab.group_filter_combo = _FakeCombo([("All", ""), ("MAGNET", "MAGNET"), ("MR08", "MR08")])
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire"), ("Comms", "Comms")])
    tab._set_report_focus_mode = lambda mode, group_filter="", topic_filter="": calls.append(
        (mode, group_filter, topic_filter)
    )

    StationsMapTab._handle_map_detail_action(
        tab,
        {"action": "filter_topic", "topic": "Fire", "group": "@MR08"},
    )

    assert tab._map_topic_filter_combo.index == 1
    assert calls == [("all_reports", "MR08", "Fire")]


def test_map_selected_message_context_routes_by_source_family() -> None:
    tab = _bare_tab()

    spotter_context = StationsMapTab._map_selected_message_context(
        tab,
        {
            "type": "report",
            "source_family": "spotter",
            "group": "@MR08",
            "topic": "Fire",
        },
    )
    assert spotter_context == {
        "target": "messages",
        "group_filter": "MR08",
        "topic_filter": "Fire",
        "query_filter": "",
        "source_family": "",
    }

    station_context = StationsMapTab._map_selected_message_context(
        tab,
        {
            "type": "station",
            "title": "K7ETC",
            "source_family": "",
        },
    )
    assert station_context == {
        "target": "messages",
        "group_filter": "",
        "topic_filter": "",
        "query_filter": "K7ETC",
        "source_family": "js8call",
    }

    tab._selected_map_topic_filter = lambda: "Fire"
    station_topic_context = StationsMapTab._map_selected_message_context(
        tab,
        {
            "type": "station",
            "title": "K7ETC",
            "source_family": "",
        },
    )
    assert station_topic_context == {
        "target": "messages",
        "group_filter": "",
        "topic_filter": "Fire",
        "query_filter": "K7ETC",
        "source_family": "",
    }

    tab._selected_map_topic_filter = lambda: ""
    local_context = StationsMapTab._map_selected_message_context(
        tab,
        {
            "type": "report",
            "source_family": "local_report",
            "group": "LOCALNET",
            "topic": "Power",
            "title": "K0PRA",
            "summary": "Generator available",
            "rows": [{"label": "Reporter", "value": "K0PRA"}],
        },
    )
    assert local_context == {
        "target": "local_reports",
        "callsign": "K0PRA",
        "topic_filter": "Power",
        "query": "LOCALNET Generator available",
    }

    pin_context = StationsMapTab._map_selected_message_context(
        tab,
        {
            "type": "report",
            "source_family": "rf_pin",
            "group": "MAGNET",
            "topic": "Comms",
        },
    )
    assert pin_context == {"target": ""}


def test_map_detail_clean_text_removes_html_fragments() -> None:
    assert (
        StationsMapTab._map_detail_clean_text("Message Reports: 8<br/>K7ETC -&gt; MR08", multiline=True)
        == "Message Reports: 8\nK7ETC -> MR08"
    )
    assert (
        StationsMapTab._map_detail_clean_text(
            "Message Reports: 8&amp;lt;br/&amp;gt;Newest: 20 days ago&amp;lt;br/&amp;gt;From: K7ETC",
            multiline=True,
        )
        == "Message Reports: 8\nNewest: 20 days ago\nFrom: K7ETC"
    )
    assert StationsMapTab._map_detail_callsigns_from_text("From: K7ETC<br/>To: MR08") == ["K7ETC"]


def test_map_payload_rows_clean_html_and_report_context_stays_topic_scoped() -> None:
    tab = _bare_tab()
    tab._selected_map_topic_filter = lambda: "Fire"
    payload = {
        "type": "report",
        "source_family": "flmsg",
        "group": "MR08",
        "topic": "Comms",
        "topics": ["Comms", "Fire", "Water"],
        "title": "Message Reports: 8",
        "rows": [
            {
                "label": "Reports",
                "value": "Message Reports: 8&lt;br/&gt;Newest: 20 days ago&lt;br/&gt;From: K7ETC",
            }
        ],
    }

    rows = StationsMapTab._map_payload_rows(payload)
    assert "<br" not in rows["reports"]
    assert "Message Reports: 8" in rows["reports"]
    assert "From: K7ETC" in rows["reports"]
    assert StationsMapTab._map_selected_display_title(tab, payload, "Message Reports: 8", rows) == "K7ETC Fire Reports"
    row_html = StationsMapTab._map_detail_row_html("Reports", rows["reports"])
    assert "&lt;br" not in row_html
    assert "<br/>" not in row_html
    assert "Message Reports: 8" in row_html
    assert "From: K7ETC" in row_html

    context = StationsMapTab._map_selected_message_context(tab, payload)
    assert context == {
        "target": "messages",
        "group_filter": "MR08",
        "topic_filter": "Fire",
        "query_filter": "",
        "source_family": "",
    }


def test_map_report_cluster_call_label_drives_title_detail_and_message_filter() -> None:
    tab = _bare_tab()
    tab._selected_map_topic_filter = lambda: "Fire"
    payload = {
        "type": "report",
        "source_family": "mixed",
        "title": "Message Reports: 8",
        "call_label": "K7ETC",
        "group": "MR08",
        "topic": "Comms",
        "topics": ["Comms", "Fire", "Food"],
        "summary": (
            "Message Reports: 8<br/>Newest: 20 days ago<br/>"
            "From: K7ETC<br/>Flmsg | General | K7ETC -&gt; MR08 | 20 days ago<br/>"
            "Summary: Widemouth 2 Fire"
        ),
        "rows": [
            {"label": "Area", "value": "UT / DM38ST"},
            {"label": "Source", "value": "Fused"},
            {"label": "Reports", "value": "Message Reports: 8&lt;br/&gt;From: K7ETC"},
        ],
    }

    rows = StationsMapTab._map_payload_rows(payload)
    assert StationsMapTab._map_selected_display_title(tab, payload, "Message Reports: 8", rows) == "K7ETC Fire Reports"

    tab._map_selected_payload = payload
    assert StationsMapTab._map_selected_station_callsign(tab) == "K7ETC"

    detail_html = StationsMapTab._map_selected_detail_html(tab, payload)
    assert "K7ETC" in detail_html
    assert "Widemouth 2 Fire" in detail_html
    assert "Multiple Sources" in detail_html
    assert "&lt;br" not in detail_html
    assert "&amp;lt;br" not in detail_html
    assert "<br/>" not in detail_html

    context = StationsMapTab._map_selected_message_context(tab, payload)
    assert context == {
        "target": "messages",
        "group_filter": "MR08",
        "topic_filter": "Fire",
        "query_filter": "",
        "source_family": "",
    }


def test_map_report_detail_uses_plain_summary_and_cross_source_topic_handoff() -> None:
    tab = _bare_tab()
    tab._selected_map_topic_filter = lambda: "Fire"
    payload = {
        "type": "report",
        "source_family": "mixed",
        "title": "Fire Reports: 8",
        "route": "MR08 | Fire | from K7ETC",
        "group": "MR08",
        "topic": "Fire",
        "topics": ["Fire", "Water"],
        "callsigns": ["K7ETC"],
        "rows": [
            {"label": "Reporter", "value": "K7ETC"},
            {"label": "Source", "value": "Multiple Sources"},
            {"label": "Area", "value": "UT / DM38ST"},
        ],
    }

    detail_html = StationsMapTab._map_selected_detail_html(
        tab,
        payload,
        summary="Widemouth 2 Fire<br/>K7ETC -&gt; MR08",
    )

    assert "Widemouth 2 Fire" in detail_html
    assert "K7ETC" in detail_html
    assert "Multiple Sources" in detail_html
    assert "&lt;br" not in detail_html
    assert "&amp;gt" not in detail_html

    context = StationsMapTab._map_selected_message_context(tab, payload)
    assert context == {
        "target": "messages",
        "group_filter": "MR08",
        "topic_filter": "Fire",
        "query_filter": "",
        "source_family": "",
    }


def test_map_report_cache_signature_changes_with_refinement_filters() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab.recency_seconds = 7 * 24 * 60 * 60
    tab.group_filter_combo = _FakeCombo([("All", ""), ("MAGNET", "MAGNET")])
    tab.region_filter_combo = _FakeCombo([("All", "")])
    tab.band_combo = _FakeCombo([("All", {"type": "all"})])
    tab._map_source_filter_combo = _FakeCombo([("All Sources", "")])
    tab._map_state_filter_combo = _FakeCombo([("All States", "")])
    tab._map_status_filter_combo = _FakeCombo([("All Statuses", "")])
    tab._map_scope_filter_combo = _FakeCombo([("Stations + Traffic", "all")])
    tab._map_trust_filter_combo = _FakeCombo([("All Auth/Trust", "")])
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire")])
    tab._map_search_edit = SimpleNamespace(text=lambda: "")

    first = StationsMapTab._map_report_cache_signature(tab, "report_focus")
    tab._map_topic_filter_combo.setCurrentIndex(1)
    second = StationsMapTab._map_report_cache_signature(tab, "report_focus")
    tab.group_filter_combo.setCurrentIndex(1)
    third = StationsMapTab._map_report_cache_signature(tab, "report_focus")

    assert first["topic"] == ""
    assert second["topic"] == "Fire"
    assert third["group"] == "MAGNET"
    assert first != second
    assert second != third


def test_map_report_events_use_filter_aware_cache_key() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab.recency_seconds = 0
    tab._map_query_cache = {}
    tab.group_filter_combo = _FakeCombo([("All", "")])
    tab.region_filter_combo = _FakeCombo([("All", "")])
    tab.band_combo = _FakeCombo([("All", {"type": "all"})])
    tab._map_source_filter_combo = _FakeCombo([("All Sources", "")])
    tab._map_state_filter_combo = _FakeCombo([("All States", "")])
    tab._map_status_filter_combo = _FakeCombo([("All Statuses", "")])
    tab._map_scope_filter_combo = _FakeCombo([("Stations + Traffic", "all")])
    tab._map_trust_filter_combo = _FakeCombo([("All Auth/Trust", "")])
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire")])
    tab._map_search_edit = SimpleNamespace(text=lambda: "")
    station_lookup = {"K7ETC": StationPoint(callsign="K7ETC", grid="DM38ST", lat=38.0, lon=-112.0)}
    calls: list[str] = []

    def loader() -> list[dict[str, object]]:
        calls.append(StationsMapTab._selected_map_topic_filter(tab) or "all")
        return [
            {
                "callsign": "K7ETC",
                "grid": "DM38ST",
                "topics": [StationsMapTab._selected_map_topic_filter(tab) or "Comms"],
                "source_family": "flmsg",
                "summary": "Widemouth 2 Fire",
                "utc_ts": 1000,
            }
        ]

    all_events = StationsMapTab._build_spotter_operational_events(
        tab,
        station_lookup,
        layer_name="report_focus",
        display_label="Traffic Reports",
        reports_loader=loader,
    )
    tab._map_topic_filter_combo.setCurrentIndex(1)
    fire_events = StationsMapTab._build_spotter_operational_events(
        tab,
        station_lookup,
        layer_name="report_focus",
        display_label="Traffic Reports",
        reports_loader=loader,
    )

    assert calls == ["all", "Fire"]
    assert all_events[0]["topic"] == "Comms"
    assert fire_events[0]["topic"] == "Fire"


def test_map_report_title_prefers_single_reporter_and_active_topic() -> None:
    tab = _bare_tab()
    tab._selected_map_topic_filter = lambda: "Fire"
    payload = {
        "type": "report",
        "source_family": "flmsg",
        "topic": "Comms",
        "topics": ["Comms", "Fire"],
        "title": "Message Reports: 8",
        "rows": [{"label": "From", "value": "K7ETC"}],
    }
    rows = StationsMapTab._map_payload_rows(payload)

    assert StationsMapTab._map_selected_display_title(tab, payload, "Message Reports: 8", rows) == "K7ETC Fire Reports"

    detail_html = StationsMapTab._map_selected_detail_html(tab, payload, summary="")
    assert "&lt;br" not in detail_html
    assert "<br/>" not in detail_html


def test_map_selected_sop_context_uses_cluster_group_and_topic() -> None:
    tab = _bare_tab()

    context = StationsMapTab._map_selected_sop_context(
        tab,
        {
            "type": "report",
            "source_family": "condition_alert",
            "groups": ["@MAGNET", "MR08"],
            "topics": ["Comms", "General Intel"],
        },
    )

    assert context == {
        "group": "MAGNET",
        "topic": "Comms",
        "source_family": "condition_alert",
    }


def test_map_observation_focus_sources_cover_current_traffic_families() -> None:
    assert StationsMapTab._observation_focus_sources("hf_reports") == {
        "spotter",
        "commstat",
        "js8call",
        "varac",
        "flmsg",
        "flamp",
        "condition_alert",
    }
    assert StationsMapTab._observation_focus_sources("local_reports") == {"local_report"}
    assert StationsMapTab._observation_focus_sources("rf_pins") == {"rf_pin"}
    assert {"spotter", "commstat", "local_report"}.issubset(
        StationsMapTab._observation_focus_sources("all_reports")
    )
    assert "rf_pin" not in StationsMapTab._observation_focus_sources("all_reports")


def test_map_observation_scope_filters_group_and_region_without_callsign_noise() -> None:
    tab = _bare_tab()
    tab.operator_index = {
        "K7ETC": {"region": "MR08"},
        "N1MAG": {"region": "MRHUB"},
    }
    obs = SimpleNamespace(
        groups=("@MR08",),
        to_target="@MAGNET",
        from_call="K7ETC",
    )

    assert StationsMapTab._observation_matches_map_scope(tab, obs, group_filter="MAGNET")
    assert StationsMapTab._observation_matches_map_scope(tab, obs, group_filter="@MR08")
    assert not StationsMapTab._observation_matches_map_scope(tab, obs, group_filter="AMRRON")
    assert StationsMapTab._observation_matches_map_scope(tab, obs, region_filter="MR08")
    assert not StationsMapTab._observation_matches_map_scope(tab, obs, region_filter="MR01")


def test_map_advanced_filter_helpers_keep_station_and_report_scope_separate() -> None:
    tab = _bare_tab()
    tab._map_scope_filter_combo = _FakeCombo([("Stations + Traffic", "all"), ("Traffic Only", "reports")])
    tab._map_state_filter_combo = _FakeCombo([("All States", ""), ("CO", "CO")])
    tab._map_source_filter_combo = _FakeCombo([("All Sources", ""), ("FastLight", "fastlight")])
    tab._map_status_filter_combo = _FakeCombo([("All Statuses", ""), ("Needs Review", "needs_review")])
    tab._map_trust_filter_combo = _FakeCombo([("All Auth/Trust", ""), ("Verified / Trusted", "verified")])

    tab._map_scope_filter_combo.setCurrentIndex(1)
    assert not StationsMapTab._advanced_filters_allow_stations(tab)
    assert StationsMapTab._advanced_filters_allow_reports(tab)

    tab._map_scope_filter_combo.setCurrentIndex(0)
    tab._map_state_filter_combo.setCurrentIndex(1)
    assert StationsMapTab._station_matches_advanced_filters(
        tab,
        StationPoint(callsign="K0CO", grid="DM79", state="CO", lat=39, lon=-105),
    )
    assert not StationsMapTab._station_matches_advanced_filters(
        tab,
        StationPoint(callsign="K0UT", grid="DM38", state="UT", lat=39, lon=-105),
    )

    tab._map_source_filter_combo.setCurrentIndex(1)
    tab._map_status_filter_combo.setCurrentIndex(1)
    tab._map_trust_filter_combo.setCurrentIndex(1)
    obs = SimpleNamespace(
        state="CO",
        source_family="flmsg",
        status="YELLOW",
        urgency="",
        auth_state="VERIFIED",
        trusted_state="TRUSTED",
        confirmed_state="",
    )
    assert StationsMapTab._observation_matches_advanced_filters(tab, obs)
    obs.source_family = "spotter"
    assert not StationsMapTab._observation_matches_advanced_filters(tab, obs)


def test_map_auto_ingest_falls_back_to_local_ingest_without_background_controller() -> None:
    tab = _bare_tab()
    tab._app_active = True
    calls: list[str] = []
    tab._request_background_ingest = lambda *kinds: False
    tab._ingest_js8_logs = lambda **_kwargs: calls.append("local-js8") or 0
    tab._schedule_render = lambda: calls.append("render")
    tab._emit_map_event = lambda *_args, **_kwargs: None

    StationsMapTab._auto_ingest_and_refresh(tab, initial=False)

    assert calls == ["local-js8", "render"]


def test_map_operator_refresh_force_rebuilds_links_and_widens_recency() -> None:
    tab = _bare_tab()
    tab._app_active = True
    tab.recency_seconds = 3 * 60 * 60
    tab.recency_combo = _FakeCombo([("Any", ""), ("3h", "3h")])
    tab.recency_combo.index = 1
    calls: list[str] = []
    tab._request_background_ingest = lambda *kinds: calls.append("background") or True
    tab._ingest_js8_logs = (
        lambda **kwargs: calls.append(f"local-js8:{bool(kwargs.get('force_rebuild'))}:{kwargs.get('since_ts')}")
        or 4
    )
    tab._schedule_render = lambda: calls.append("render")
    tab._emit_map_event = lambda *_args, **_kwargs: None

    StationsMapTab._auto_ingest_and_refresh(tab, initial=False, operator_refresh=True)

    assert calls == ["local-js8:True:None", "render"]
    assert tab.recency_seconds is None
    assert tab.recency_combo.index == 0


def test_map_source_family_normalizes_runtime_labels() -> None:
    assert StationsMapTab._canonical_map_source_family("JS8Spotter") == "spotter"
    assert StationsMapTab._canonical_map_source_family("CommStat") == "commstat"
    assert StationsMapTab._canonical_map_source_family("VarAC") == "varac"
    assert StationsMapTab._canonical_map_source_family("local report") == "local_report"
    assert StationsMapTab._map_source_family_matches_filter("JS8Spotter", "spotter") is True
    assert StationsMapTab._map_source_family_matches_filter("CommStat", "commstat") is True


def test_planning_pin_payload_and_source_kind_are_not_hf_traffic() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    payload_block = source[source.index("def pin_payload") : source.index("class _RfPinManagerDialog")]

    assert '"source_family": "rf_pin"' in payload_block
    assert '"source_kind": "pin"' in payload_block
    assert StationsMapTab._map_report_source_kind("rf_pin") == "pin"
    assert StationsMapTab._map_report_source_kind("pin") == "pin"
    assert StationsMapTab._map_report_source_label("rf_pin") == "Planning Pin"


def test_map_status_uses_planning_pin_noun_in_pin_focus() -> None:
    tab = _bare_tab()
    label = _FakeLabel()
    tab._map_support_card = _FakeCard()
    tab._map_support_label = label
    tab._map_support_layout = None
    tab._map_runtime_state = "ready"
    tab._map_runtime_detail = ""
    tab._map_marker_count = 7
    tab._map_link_count = 0
    tab._map_link_status_detail = ""
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "rf_pins"

    StationsMapTab._update_map_support_card(tab)

    assert "7 planning pins" in label.text
    assert "7 stations" not in label.text


def test_schedule_render_during_page_load_defers_until_load_finishes() -> None:
    tab = _bare_tab()
    tab._map_initialized = True
    tab._map_page_loading = True

    tab._schedule_render()

    assert tab._map_dirty is True
    assert tab._render_requested_during_load is True


def test_maybe_start_map_ingest_waits_for_successful_first_load() -> None:
    tab = _bare_tab()
    tab._deferred_initial_ingest_pending = True
    started: list[str] = []
    tab._start_map_ingest_lifecycle = lambda: started.append("started")

    assert tab._maybe_start_map_ingest() is False
    assert started == []

    tab._map_load_ok = True

    assert tab._maybe_start_map_ingest() is True
    assert started == ["started"]
    assert tab._deferred_initial_ingest_pending is False


def test_map_view_status_text_names_current_review_context() -> None:
    tab = _bare_tab()

    assert StationsMapTab._map_view_status_text(tab) == "Map View: All Stations"

    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "hf_reports"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: HF Traffic"

    tab._observation_focus_mode = "local_reports"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Local Traffic"

    tab._observation_focus_mode = "all_reports"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: All Traffic"

    tab._observation_focus_mode = "paths"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Paths - Off"
    tab.show_link_paths = True
    tab.link_mode = "my_station"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Paths - My Station"
    tab.link_mode = "station"
    tab.link_value = "K7ETC"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Paths - Selected K7ETC"

    tab._observation_focus_mode = "propagation"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: RF Planning"

    tab._observation_focus_mode = "rf_pins"
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Planning Pins"

    tab._observation_focus_enabled = False
    tab._observation_focus_mode = ""
    tab._sitrep_status_only_enabled = True
    assert StationsMapTab._map_view_status_text(tab) == "Map View: SitRep Status"

    tab._now_reachable_enabled = True
    assert StationsMapTab._map_view_status_text(tab) == "Map View: Peer Schedule Now"


def test_rf_planning_preserves_time_and_topic_filters() -> None:
    tab = _bare_tab()
    tab.recency_seconds = 7 * 24 * 60 * 60
    tab._sitrep_status_button = None
    tab._now_reachable_button = None
    tab.map_stations_chk = _FakeCheck(False)
    tab.map_links_chk = _FakeCheck(False)
    tab.map_weather_chk = _FakeCheck(True)
    tab.map_alerts_chk = _FakeCheck(True)
    tab.map_infrastructure_chk = _FakeCheck(True)
    tab.prop_overlay_chk = _FakeCheck(False)
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("All", ("all", ""))])
    tab.recency_combo = _FakeCombo([("3h", 3 * 60 * 60), ("7d", 7 * 24 * 60 * 60), ("Any", None)])
    tab.recency_combo.setCurrentIndex(1)
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire"), ("Comms", "Comms")])
    tab._map_topic_filter_combo.setCurrentIndex(1)
    tab._sync_path_scope_combo = lambda *_args, **_kwargs: None
    tab._update_sitrep_status_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda: None
    tab._update_map_view_status_label = lambda: None
    tab._update_now_reachable_summary = lambda: None
    tab._refresh_relay_targets = lambda: None
    tab._request_map_refresh = lambda *_args, **_kwargs: None

    StationsMapTab.focus_propagation(tab)

    assert tab.recency_seconds == 7 * 24 * 60 * 60
    assert tab.recency_combo.currentText() == "7d"
    assert tab._map_topic_filter_combo.currentText() == "Fire"
    assert tab.show_link_paths is True
    assert tab.show_rf_pins is True
    assert tab.prop_overlay_enabled is True


def test_active_map_layer_toggle_uses_single_refresh() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "rf_pins"
    tab.show_station_markers = False
    tab.show_link_paths = False
    tab.show_rf_pins = True
    tab.show_weather_reports = False
    tab.show_alert_reports = False
    tab.show_infrastructure_reports = True
    tab.prop_overlay_enabled = False
    tab._sitrep_status_button = None
    tab._now_reachable_button = None
    tab.map_stations_chk = _FakeCheck(False)
    tab.map_links_chk = _FakeCheck(False)
    tab.map_weather_chk = _FakeCheck(False)
    tab.map_alerts_chk = _FakeCheck(False)
    tab.map_infrastructure_chk = _FakeCheck(True)
    tab.prop_overlay_chk = _FakeCheck(False)
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", ""))])
    tab._sync_path_scope_combo = lambda *_args, **_kwargs: None
    tab._sync_link_mode_combo_to_off = lambda *_args, **_kwargs: None
    tab._update_sitrep_status_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda: None
    tab._update_map_view_status_label = lambda: None
    tab._update_now_reachable_summary = lambda: None
    tab._refresh_relay_targets = lambda: None
    tab._clear_report_query_caches = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    refreshes: list[str] = []
    tab._request_map_refresh = lambda *_args, **kwargs: refreshes.append(kwargs.get("reason", ""))

    assert StationsMapTab._toggle_active_map_layer_off(tab, reason="rf_pins_map_focus_off") is True

    assert tab.show_rf_pins is False
    assert refreshes == ["rf_pins_map_focus_off"]


def test_implicit_map_search_promotes_status_to_all_traffic() -> None:
    tab = _bare_tab()
    tab._selected_map_topic_filter = lambda: ""
    tab._selected_map_search_text = lambda: "wildfire"

    assert StationsMapTab._map_view_status_text(tab) == "Map View: All Traffic"


def test_traffic_focus_uses_traffic_layers_not_default_station_dots() -> None:
    tab = _bare_tab()
    tab.recency_seconds = None
    tab._now_reachable_button = None
    tab._sitrep_status_button = None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_summary = lambda *_args, **_kwargs: None
    tab._refresh_relay_targets = lambda *_args, **_kwargs: None
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_clear_filter_buttons_visual = lambda *_args, **_kwargs: None
    tab._update_map_view_status_label = lambda *_args, **_kwargs: None
    tab._request_map_refresh = lambda *_args, **_kwargs: None

    StationsMapTab._set_report_focus_mode(tab, "local_reports")

    assert tab.show_station_markers is False
    assert tab.show_link_paths is False
    assert tab.show_weather_reports is False
    assert tab.show_alert_reports is True
    assert tab.show_infrastructure_reports is True
    assert tab.show_rf_pins is False


def test_local_reports_focus_suppresses_legacy_hf_spotter_report_layers() -> None:
    tab = _bare_tab()

    assert StationsMapTab._include_legacy_spotter_report_layers(tab) is True

    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "local_reports"
    assert StationsMapTab._include_legacy_spotter_report_layers(tab) is False

    tab._observation_focus_mode = "hf_reports"
    assert StationsMapTab._include_legacy_spotter_report_layers(tab) is True


def test_map_control_strip_uses_operator_first_sections() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    build_block = source[source.index("def _build_ui") : source.index("def _add_collapsible_group")]

    assert 'QPushButton("Map Tools")' in build_block
    assert 'QLabel("View")' in build_block
    assert '"Intelligence Layers"' in build_block
    assert 'QPushButton("All Stations")' in build_block
    assert 'QPushButton("HF Traffic")' in build_block
    assert 'QPushButton("Local Traffic")' in build_block
    assert 'QPushButton("All Traffic")' in build_block
    assert 'QPushButton("Paths")' in build_block
    assert 'QPushButton("RF Planning")' in build_block
    assert 'QPushButton("Planning Pins")' in build_block
    assert '"Path Tools"' in build_block
    assert 'filter_field("Topic", self._map_topic_filter_combo' in build_block
    assert '"All Topics"' in build_block
    assert 'QPushButton("Add Planning Pin")' in build_block
    assert 'QPushButton("Manage Pins")' in build_block
    assert "Edit Selected" in source
    assert 'QCheckBox("Alerts/Intel")' in build_block
    assert 'QCheckBox("Infrastructure/Utilities")' in build_block
    assert 'QPushButton("Show Filters & Layers")' not in build_block
    assert "def focus_propagation" in source
    assert 'label.setText(' in source
    assert 'f"Ready:' in source
    assert 'return "planning pins"' in source
    assert 'return "traffic items"' in source
    assert 'return "stations"' in source
    assert "} links." in source
    assert "self._map_retry_btn.setVisible(not ready)" in source
    assert 'id="legendToggle"' in source
    assert 'legendDock" class="collapsed"' in source
    assert "function openSelectedDetail" in source
    assert "function emitMapAction" in source
    assert "function stationDetailPayload" in source
    assert "function reportDetailPayload" in source
    assert "function compactStationTooltip" in source
    assert "function detailActionButton" not in source
    assert "function buildStationDetail" not in source
    assert "function buildReportDetail" not in source
    assert "groups: event.groups || []" in source
    assert "topics: event.topics || []" in source
    assert "detailRowPayload('Groups'" in source
    assert "detailRowPayload('Topics'" in source
    assert "detailRowPayload('MCF'" in source
    assert "detailRowPayload('Status'" in source
    assert "detailRowPayload('Area'" in source
    assert "detailRowPayload('Location'" in source
    assert "QTextBrowser" in source
    assert "def _show_map_selected_detail" in source
    assert "def _map_selected_detail_html" in source
    assert "Condition Alert" in source
    assert "Station Activity" in source
    assert "def _show_paths_for_selected_station" in source
    assert "def _open_map_selected_messages" in source
    assert "def _compose_spotter_for_selected_station" in source
    assert "def _open_map_selected_sop" in source
    assert "QPushButton(\"Messages\")" in source
    assert "QPushButton(\"SOP\")" in source
    assert "open_messages_section(" in source
    assert "focus_traffic_context" in Path("freqinout/gui/sop_tab.py").read_text(encoding="utf-8")
    assert "def _handle_map_detail_action" in source
    assert "web.titleChanged.connect(self._on_map_page_title_changed)" in source
    assert "body._nonce =" in source
    assert "'select_detail'" in source
    assert '"action": "filter_group"' in source
    assert '"action": "filter_topic"' in source
    assert "messages_btn.clicked.connect(self._open_map_selected_messages)" in source
    assert "sop_btn.clicked.connect(self._open_map_selected_sop)" in source
    assert "if action == \"open_messages\":" in source
    assert "if action == \"review_sop\":" in source
    assert "source_family" in source
    assert "openSelectedDetail(payload)" in source
    assert "openSelectedDetail(buildStationDetail" not in source
    assert "openSelectedDetail(buildReportDetail" not in source
    assert "circle.bindTooltip(tipText" in source
    assert "showDetail(tipText)" not in source


def test_planning_pins_focus_is_not_received_traffic_focus() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    focus_block = source[source.index("def focus_rf_pins") : source.index("def focus_spotter_reports")]

    assert "self._set_report_focus_mode" not in focus_block
    assert "self.show_station_markers = False" in focus_block
    assert "self.show_link_paths = False" in focus_block
    assert "self.show_rf_pins = True" in focus_block
    assert "self.show_infrastructure_reports = False" in focus_block


def test_planning_pins_do_not_scope_or_display_station_traffic() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    render_start = source.index("planning_pins_mode =")
    render_block = source[render_start : source.index("self._map_link_status_detail", render_start)]

    assert StationsMapTab._observation_focus_scopes_station_markers(True, "hf_reports") is True
    assert StationsMapTab._observation_focus_scopes_station_markers(True, "local_reports") is True
    assert StationsMapTab._observation_focus_scopes_station_markers(True, "all_reports") is True
    assert StationsMapTab._observation_focus_scopes_station_markers(True, "rf_pins") is False
    assert StationsMapTab._observation_focus_scopes_station_markers(False, "hf_reports") is False
    assert "if planning_pins_mode:" in render_block
    assert "weather_events = []" in render_block
    assert "alert_events = []" in render_block
    assert "infrastructure_events = [" in render_block
    assert "display_markers = []" in render_block
    assert "display_links = []" in render_block


def test_planning_pin_ui_language_is_operator_facing() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert 'QPushButton("Planning Pins")' in source
    assert 'QPushButton("Add Planning Pin")' in source
    assert 'setWindowTitle("Manage Planning Pins")' in source
    assert "Planning Pin saved:" in source
    assert "operator curated" not in source.lower()
    assert "Fused" not in source


def test_map_ready_text_matches_active_view_type() -> None:
    tab = _bare_tab()
    tab._map_marker_count = 8
    tab._map_link_count = 2

    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    assert StationsMapTab._map_marker_noun(tab) == "traffic items"
    assert StationsMapTab._map_ready_detail_text(tab) == "Map is ready with 8 traffic items and 2 links."

    tab._observation_focus_mode = "rf_pins"
    assert StationsMapTab._map_marker_noun(tab) == "planning pins"
    assert StationsMapTab._map_ready_detail_text(tab) == "Map is ready with 8 planning pins and 2 links."

    tab._observation_focus_enabled = False
    assert StationsMapTab._map_marker_noun(tab) == "stations"
    assert StationsMapTab._map_ready_detail_text(tab) == "Map is ready with 8 stations and 2 links."


def test_report_map_counts_event_layers_not_station_markers() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    render_start = source.index("report_event_count =")
    render_block = source[render_start : source.index("self._map_link_count = len(display_links)", render_start)]

    assert "len(weather_events) + len(alert_events) + len(infrastructure_events)" in render_block
    assert 'report_focus_mode in {"hf_reports", "local_reports", "all_reports"}' in render_block
    assert "self._map_marker_count = report_event_count" in render_block
    assert "planning_pins_mode" in render_block


def test_clear_layers_preserves_report_view_context() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "hf_reports"
    tab.show_station_markers = True
    tab.show_link_paths = True
    tab.show_weather_reports = True
    tab.show_alert_reports = True
    tab.show_infrastructure_reports = True
    tab.show_rf_pins = True
    tab.prop_overlay_enabled = True
    tab.link_mode = "all"
    tab.link_value = ""
    tab.map_links_chk = _FakeCheck(True)
    tab.map_stations_chk = _FakeCheck(True)
    tab.map_weather_chk = _FakeCheck(True)
    tab.map_alerts_chk = _FakeCheck(True)
    tab.map_infrastructure_chk = _FakeCheck(True)
    tab.prop_overlay_chk = _FakeCheck(True)
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("All", ("all", ""))])
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("Network", ("all", ""))])
    tab.relay_target_combo = _FakeCombo([("", "")])
    calls: list[str] = []
    tab._update_sitrep_status_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda: None
    tab._update_map_view_status_label = lambda: None
    tab._update_now_reachable_summary = lambda: None
    tab._refresh_relay_targets = lambda: None
    tab._update_clear_filter_buttons_visual = lambda: None
    tab._clear_report_query_caches = lambda: None
    tab._request_map_refresh = lambda **kwargs: calls.append(str(kwargs.get("reason") or ""))

    StationsMapTab.clear_map_layers(tab)

    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "hf_reports"
    assert tab.show_link_paths is False
    assert tab.show_rf_pins is False
    assert tab.prop_overlay_enabled is False
    assert tab.show_station_markers is False
    assert tab.show_alert_reports is True
    assert tab.show_infrastructure_reports is True
    assert StationsMapTab._map_view_status_text(tab) == "Map View: HF Traffic"
    assert calls == ["clear_map_layers"]


def test_report_scoped_station_search_does_not_require_station_metadata_match() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    render_start = source.index("observation_filter_already_scoped =")
    render_block = source[render_start : source.index("key = (round(pt.lat, 4)", render_start)]

    assert "observation_scope_applies" in render_block
    assert "topic_filter" in render_block
    assert "search_text" in render_block
    assert "not observation_filter_already_scoped" in render_block
    assert "_station_matches_map_search" in render_block


def test_paths_focus_is_station_links_without_report_layers() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    focus_block = source[source.index("def focus_paths") : source.index("def focus_propagation")]

    assert "self.show_station_markers = True" in focus_block
    assert "self.show_link_paths = True" in focus_block
    assert "self.show_weather_reports = False" in focus_block
    assert "self.show_alert_reports = False" in focus_block
    assert "self.show_infrastructure_reports = False" in focus_block
    assert "self.show_rf_pins = False" in focus_block
    assert "self.prop_overlay_enabled = False" in focus_block


def test_paths_chip_toggles_off_without_clearing_report_filters() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "hf_reports"
    tab.show_link_paths = False
    tab.link_mode = "off"
    tab.link_value = ""
    tab.map_links_chk = _FakeCheck(False)
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("All", ("all", ""))])
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("Network", ("all", ""))])
    calls: list[str] = []
    tab._update_sitrep_status_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda *_args, **_kwargs: None
    tab._update_map_view_status_label = lambda: None
    tab._update_now_reachable_summary = lambda: None
    tab._refresh_relay_targets = lambda: None
    tab._update_clear_filter_buttons_visual = lambda *_args, **_kwargs: None
    tab._request_map_refresh = lambda **kwargs: calls.append(str(kwargs.get("reason") or ""))

    StationsMapTab.focus_paths(tab)

    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "paths"
    assert tab.show_link_paths is True
    assert tab.link_mode == "all"

    StationsMapTab.focus_paths(tab)

    assert tab._observation_focus_enabled is True
    assert tab._observation_focus_mode == "hf_reports"
    assert tab.show_link_paths is False
    assert tab.link_mode == "off"
    assert calls == ["paths_map_focus", "paths_map_focus_off"]


def test_all_stations_focus_clears_path_layer_state() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "paths"
    tab._now_reachable_enabled = True
    tab._now_reachable_meta = {"label": "active"}
    tab._now_reachable_callsigns = {"K7ETC"}
    tab.show_link_paths = True
    tab.link_mode = "station"
    tab.link_value = "K7ETC"
    tab.relay_target = "K7ETC"
    tab._paths_focus_station = "K7ETC"
    tab._paths_previous_observation_focus = (True, "hf_reports")
    tab.map_links_chk = _FakeCheck(True)
    tab.link_mode_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("K7ETC", ("station", "K7ETC"))])
    tab.link_mode_combo.setCurrentIndex(2)
    tab._map_path_scope_combo = _FakeCombo([("Off", ("off", "")), ("My Station", ("my_station", "")), ("K7ETC", ("station", "K7ETC"))])
    tab._map_path_scope_combo.setCurrentIndex(2)
    calls: list[str] = []
    tab._sitrep_status_button = None
    tab._now_reachable_button = None
    tab.prop_overlay_chk = None
    tab._update_sitrep_status_button_visual = lambda *_args, **_kwargs: None
    tab._update_now_reachable_button_visual = lambda *_args, **_kwargs: None
    tab._update_selected_paths_button_visual = lambda *_args, **_kwargs: None
    tab._update_map_mode_buttons = lambda *_args, **_kwargs: None
    tab._update_map_view_status_label = lambda: None
    tab._update_now_reachable_summary = lambda: None
    tab._refresh_relay_targets = lambda: None
    tab._update_clear_filter_buttons_visual = lambda *_args, **_kwargs: None
    tab._request_map_refresh = lambda **kwargs: calls.append(str(kwargs.get("reason") or ""))

    StationsMapTab.focus_all_stations(tab)

    assert tab._observation_focus_enabled is False
    assert tab._observation_focus_mode == ""
    assert tab._now_reachable_enabled is False
    assert tab._now_reachable_meta == {}
    assert tab._now_reachable_callsigns == set()
    assert tab.show_link_paths is False
    assert tab.link_mode == "off"
    assert tab.link_value == ""
    assert tab.relay_target == ""
    assert tab._paths_focus_station == ""
    assert tab._paths_previous_observation_focus is None
    assert tab.map_links_chk.checked is False
    assert tab.link_mode_combo.index == 0
    assert tab._map_path_scope_combo.index == 0
    assert calls == ["all_stations_map_focus"]


def test_map_link_status_text_explains_common_zero_link_states() -> None:
    tab = _bare_tab()

    assert StationsMapTab._map_link_status_text(
        tab,
        links_active=True,
        show_link_paths=False,
        loaded_link_count=0,
        display_link_count=0,
    ) == "Path layer hidden."
    assert StationsMapTab._map_link_status_text(
        tab,
        links_active=False,
        show_link_paths=True,
        loaded_link_count=0,
        display_link_count=0,
    ) == "Path scope is Off."
    assert StationsMapTab._map_link_status_text(
        tab,
        links_active=True,
        show_link_paths=True,
        loaded_link_count=3,
        display_link_count=2,
    ) == "2 directional path link(s) shown."
    assert StationsMapTab._map_link_status_text(
        tab,
        links_active=True,
        show_link_paths=True,
        loaded_link_count=3,
        display_link_count=0,
    ) == "Path links are loaded but filtered out by the current view."
    assert StationsMapTab._map_link_status_text(
        tab,
        links_active=True,
        show_link_paths=True,
        loaded_link_count=0,
        display_link_count=0,
        all_time_link_count=149,
        recency_seconds=3 * 60 * 60,
        link_selection=("my_station", ""),
    ) == "No path links in the selected time window; 149 older path link(s) match with Since: Any."
    assert StationsMapTab._map_link_status_text(
        tab,
        links_active=True,
        show_link_paths=True,
        loaded_link_count=0,
        display_link_count=0,
        link_selection=("group", "MR08"),
    ) == "No path links found for group MR08."


def test_map_zero_link_window_probes_any_age_links_for_status() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    render_block = source[source.index("# init stats and links") : source.index("# Spread overlapping stations")]
    status_block = source[source.index("def _map_link_status_text") : source.index("def _display_links_for_mode")]

    assert "_map_last_link_all_time_count = 0" in source
    assert "finite_path_window" in render_block
    assert "loaded_link_count == 0" in render_block
    assert "max_age_sec=0" in render_block
    assert "_display_links_for_mode(probe_links, sitrep_mode)" in render_block
    assert "source_rows_before" in render_block and "_map_last_link_source_rows = source_rows_before" in render_block
    assert "all_time_link_count" in status_block
    assert "Since: Any" in status_block


def test_js8_link_loader_returns_tuple_on_database_path_failures() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    load_block = source[source.index("def _load_js8_links") : source.index("def _load_varac_links")]

    assert "return links, {}" in load_block
    assert "return links\n" not in load_block


def test_map_selected_detail_clicks_use_single_native_panel() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    open_block = source[source.index("function openSelectedDetail") : source.index("function detailRowPayload")]
    station_payload_block = source[source.index("function stationDetailPayload") : source.index("function compactStationTooltip")]

    assert "emitMapAction('select_detail'" in open_block
    assert "markerMeaningByStatus" in station_payload_block
    assert "detailRowPayload('Marker', markerMeaning)" in station_payload_block
    assert "selectedDetailPanel.addTo(map)" not in source
    assert "selected-detail-panel empty" not in source
    assert "showSelectedDetail" not in source
    assert "sidePanelEligible" not in source


def test_map_selected_detail_html_formats_operator_cards() -> None:
    tab = _bare_tab()

    alert_html = StationsMapTab._map_selected_detail_html(
        tab,
        {
            "type": "report",
            "source_family": "condition_alert",
            "title": "MagCon Yellow",
            "route": "MAGNET | Comms",
            "group": "@MAGNET",
            "topic": "Comms",
            "topics": ["Comms", "General Intel"],
            "rows": [
                {"label": "Severity", "value": "caution"},
                {"label": "Age", "value": "17 min ago"},
                {"label": "Source", "value": "Condition Alert"},
            ],
        },
        summary="MAGCON changed to Yellow",
    )

    assert "Condition Alert" in alert_html
    assert "MAGNET" in alert_html
    assert "Comms" in alert_html
    assert "MAGCON changed to Yellow" in alert_html
    assert "fio-chip" in alert_html

    station_html = StationsMapTab._map_selected_detail_html(
        tab,
        {
            "type": "station",
            "title": "K7ETC",
            "group": "MR08",
            "rows": [
                {"label": "Area", "value": "UT / DM38ST"},
                {"label": "Activity", "value": "JS8 20M"},
                {"label": "Marker", "value": "Green: latest status is functioning"},
                {"label": "Schedule", "value": "MAGNET 20M"},
            ],
        },
        summary="Wildfire report",
    )

    assert "Station Activity" in station_html
    assert "K7ETC" in station_html
    assert "UT / DM38ST" in station_html
    assert "Green: latest status is functioning" in station_html

    spotter_html = StationsMapTab._map_selected_detail_html(
        tab,
        {
            "type": "report",
            "source_family": "spotter",
            "title": "Wildfire | F!307",
            "route": "K7ETC | MR08",
            "group": "MR08",
            "topics": ["Fire", "Comms"],
            "rows": [
                {"label": "Reports", "value": "Wildfire | F!307"},
                {"label": "Age", "value": "8 days ago"},
                {"label": "Area", "value": "UT / DM38ST"},
                {"label": "Auth", "value": "Verified"},
            ],
        },
        summary="Wildfire update near Widemouth.",
    )

    assert "Spotter Report" in spotter_html
    assert "MCF" in spotter_html
    assert "Wildfire | F!307" in spotter_html
    assert "K7ETC -&gt; MR08" in spotter_html
    assert "Trust" in spotter_html
    assert "Verified" in spotter_html

    local_html = StationsMapTab._map_selected_detail_html(
        tab,
        {
            "type": "report",
            "source_family": "local_report",
            "title": "K0PRA",
            "rows": [
                {"label": "Area", "value": "CO / DM79"},
                {"label": "Age", "value": "42 min ago"},
                {"label": "Confirmed", "value": "Confirmed"},
            ],
        },
        summary="Local operator report.",
    )

    assert "Local Report" in local_html
    assert "Reporter" in local_html
    assert "K0PRA" in local_html
    assert "Confirmed" in local_html

    pin_html = StationsMapTab._map_selected_detail_html(
        tab,
        {
            "type": "report",
            "source_family": "rf_pin",
            "title": "20M receive window",
            "group": "MAGNET",
            "rows": [
                {"label": "Band", "value": "20M"},
                {"label": "Location", "value": "Front Range"},
                {"label": "Updated", "value": "12 min ago"},
            ],
        },
    )

    assert "Planning Pin" in pin_html
    assert "Purpose" in pin_html
    assert "20M receive window" in pin_html
    assert "Front Range" in pin_html


def test_map_load_finished_flushes_deferred_render_request() -> None:
    tab = _bare_tab()
    tab._map_page_loading = True
    tab._render_requested_during_load = True
    tab._map_visible = True
    scheduled: list[str] = []
    tab._schedule_render = lambda: scheduled.append("render")

    tab._on_map_load_finished(True)

    assert tab._map_page_loading is False
    assert tab._map_initialized is True
    assert tab._map_load_ok is True
    assert tab._render_requested_during_load is False
    assert scheduled == ["render"]


def test_push_map_payload_queues_while_page_loading() -> None:
    tab = _bare_tab()
    tab._map_page_loading = True
    tab._map_initialized = False

    tab._push_map_payload([{"callsign": "N0CALL"}], [{"origin": "A", "destination": "B"}])

    assert tab._pending_map_payload is not None
    assert tab._pending_map_payload["markers"][0]["callsign"] == "N0CALL"
    assert tab._pending_map_payload["links"][0]["origin"] == "A"


def test_js8_log_indexer_repeated_scan_does_not_duplicate_map_or_activity_rows(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    directed = tmp_path / "DIRECTED.TXT"
    directed.write_text(
        "2026-08-08 12:34:56\t7.115000\t0\t-10\tN0CALL: K7AAA SNR -10\n",
        encoding="utf-8",
    )
    settings = SettingsManager()
    settings.set("js8_directed_path", str(directed))
    db_path = cfg_root / "config" / "freqinout_nets.db"
    indexer = JS8LogLinkIndexer(settings, db_path)

    assert indexer.update(since_ts=0) == 1
    settings.set("js8_links_directed_offset", 0)
    settings.set("js8_links_all_offset", 0)
    assert indexer.update(since_ts=0) == 1

    conn = sqlite3.connect(db_path)
    try:
        link_count = conn.execute("SELECT COUNT(*) FROM js8_links").fetchone()[0]
        stats = conn.execute(
            "SELECT callsign, last_band, last_freq_hz FROM js8_callsign_stats ORDER BY callsign"
        ).fetchall()
    finally:
        conn.close()

    assert link_count == 1
    assert stats == [
        ("K7AAA", "40M", 7115000.0),
        ("N0CALL", "40M", 7115000.0),
    ]


def test_map_observation_loader_uses_read_only_eligibility(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"

    spotter = observation_from_message_intelligence(
        analyze_spotter_text("F!307 TO[@MR08] FR[K7ETC] ST[UT] GR[DM38ST] NA[Wildfire status] #D2NT"),
        source_ref="spotter_traffic:1",
        source_family="spotter",
        event_utc="2026-08-10T14:00:00+00:00",
    )
    confirmed_local = observation_from_local_report(
        {
            "id": 1,
            "created_utc": "2026-08-10T15:00:00+00:00",
            "callsign": "K0PRA",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Comms",),
            "subject": "Repeater degraded",
            "confirmed_state": "CONFIRMED",
        }
    )
    food_local = observation_from_local_report(
        {
            "id": 3,
            "created_utc": "2026-08-10T14:30:00+00:00",
            "callsign": "K0FOOD",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Food",),
            "subject": "Food distribution available",
            "confirmed_state": "CONFIRMED",
        }
    )
    unconfirmed_local = observation_from_local_report(
        {
            "id": 2,
            "created_utc": "2026-08-10T15:05:00+00:00",
            "callsign": "N0PWR",
            "state": "CO",
            "grid": "DM79",
            "topics": ("Power",),
            "subject": "Generator needed",
            "confirmed_state": "UNCONFIRMED",
        }
    )
    condition_alert = Observation(
        observation_id="condition_alert:magcon:k7etc:yellow",
        source_family="condition_alert",
        source_ref="js8:1",
        source_app="JS8Call",
        received_utc="2026-08-10T16:00:00+00:00",
        event_utc="2026-08-10T16:00:00+00:00",
        from_call="N1MAG",
        to_target="MAGNET",
        groups=("MAGNET",),
        observed_topics=("General Intel", "Comms"),
        operator_attention=True,
        status="CONDITION ALERT",
        urgency="LEVEL YELLOW",
        subject="MAGCON: Level YELLOW",
        summary="MAGNET condition level YELLOW",
        grid="DM79",
        location_confidence="grid",
    )
    rf_pin = observation_from_rf_pin(
        {
            "pin_id": "manual:relay-check",
            "label": "Relay check",
            "callsign": "N1MAG",
            "target": "MAGNET",
            "topics": ("Comms",),
            "grid": "DM79",
            "status": "PIN",
            "created_utc": "2026-08-10T16:30:00+00:00",
        }
    )
    for obs in (spotter, confirmed_local, food_local, unconfirmed_local, condition_alert, rf_pin):
        upsert_observation(db_path, obs)

    alert_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )

    assert [row["callsign"] for row in alert_rows] == ["N1MAG", "K7ETC"]
    assert alert_rows[0]["source_family"] == "condition_alert"
    assert alert_rows[0]["source_label"] == "Condition Alert"
    assert alert_rows[0]["icon"] == "warning"
    assert [row["callsign"] for row in infrastructure_rows] == ["K0PRA", "K0FOOD"]
    assert infrastructure_rows[0]["source_family"] == "local_report"
    assert infrastructure_rows[0]["source_label"] == "Local Report"
    assert all(row["callsign"] != "N0PWR" for row in alert_rows + infrastructure_rows)

    tab._query_cache = {}
    tab._observation_focus_mode = "hf_reports"
    hf_alert_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    hf_infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )

    assert [row["callsign"] for row in hf_alert_rows] == ["N1MAG", "K7ETC"]
    assert hf_infrastructure_rows == []

    tab._query_cache = {}
    tab._observation_focus_mode = "local_reports"
    local_alert_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    local_infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )

    assert local_alert_rows == []
    assert [row["callsign"] for row in local_infrastructure_rows] == ["K0PRA", "K0FOOD"]
    assert all(row["callsign"] != "N0PWR" for row in local_alert_rows + local_infrastructure_rows)

    tab._query_cache = {}
    tab._observation_focus_mode = "rf_pins"
    pin_infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )

    assert [row["callsign"] for row in pin_infrastructure_rows] == ["N1MAG"]
    assert pin_infrastructure_rows[0]["source_family"] == "rf_pin"

    tab._query_cache = {}
    tab._observation_focus_mode = "all_reports"
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Comms")
    comms_infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )
    assert [row["callsign"] for row in comms_infrastructure_rows] == ["K0PRA"]

    tab._query_cache = {}
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Fire")
    fire_alert_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    assert [row["callsign"] for row in fire_alert_rows] == ["K7ETC"]
    fire_scope_calls = StationsMapTab._observation_station_scope_calls(tab, max_age_sec=0)
    assert "K7ETC" in fire_scope_calls
    assert "K0FOOD" not in fire_scope_calls
    assert "K0PRA" not in fire_scope_calls

    tab._query_cache = {}
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Food")
    food_infrastructure_rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )
    assert [row["callsign"] for row in food_infrastructure_rows] == ["K0FOOD"]


def test_map_fire_search_uses_file_metadata_and_any_age_from_all_stations(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260803-040212Z-57.k2s"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)

    tab = _bare_tab()
    tab._observation_focus_enabled = False
    tab._observation_focus_mode = ""
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Fire")
    tab._map_search_edit = SimpleNamespace(text=lambda: "wildfire")
    tab.group_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.region_filter_combo = SimpleNamespace(currentData=lambda: "")

    obs = Observation(
        observation_id="flmsg:file:k7etc-fire",
        source_family="flmsg",
        source_ref=f"file:{msg_path}",
        source_app="FLMsg",
        received_utc="2026-08-03T17:33:15+00:00",
        event_utc="2026-07-29T03:54:00+00:00",
        from_call="K7ETC",
        to_target="",
        groups=("MR08",),
        observed_topics=(),
        status="NEW",
        subject="040212Z 57",
        summary="<customform> | K7ETC | 040212Z 57 | 260803-0355z",
        grid="MR08",
    )
    upsert_observation(db_path, obs)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE message_file_metadata (
                path TEXT PRIMARY KEY,
                source_family TEXT,
                msg_type TEXT,
                display_type TEXT,
                status TEXT,
                from_call TEXT,
                to_call TEXT,
                title TEXT,
                topics_json TEXT,
                search_text TEXT,
                report_ts REAL,
                source_label TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO message_file_metadata (
                path, source_family, msg_type, display_type, status, from_call,
                to_call, title, topics_json, search_text, report_ts, source_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(msg_path),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Widemouth 2 Fire",
                '["Fire","Water"]',
                "K7ETC MR08 Widemouth 2 Fire wildfire UT DM38ST",
                1785297240.0,
                "FLMsg",
            ),
        )
        conn.commit()

    rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="alert",
        max_age_sec=0,
    )
    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: rows,
    )

    assert [row["callsign"] for row in rows] == ["K7ETC"]
    assert rows[0]["summary"] == "Widemouth 2 Fire"
    assert rows[0]["grid"] == "DM38ST"
    assert len(events) == 1
    assert "Widemouth 2 Fire" in events[0]["tooltip"]
    scope_calls = StationsMapTab._observation_station_scope_calls(tab, max_age_sec=0)
    assert "K7ETC" in scope_calls


def test_map_file_location_fallback_places_flmsg_when_projection_grid_is_group(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260729-141819Z-50.k2s"
    msg_path.write_text(
        "\n".join(
            [
                "MAGNET General Use Form - v1.1.1",
                "FromK7ETC",
                "ToMR08",
                "SubjectWidemouth 2 Fire",
                "StateUT",
                "GridDM38ST",
                "MessageFire status remains active near Widemouth.",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)

    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Fire")
    tab._map_search_edit = SimpleNamespace(text=lambda: "")
    tab.group_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.region_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.operator_index = {}
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    obs = Observation(
        observation_id="flmsg:file:k7etc-fire-real-index-gap",
        source_family="flmsg",
        source_ref=f"file:{msg_path}",
        source_app="FLMsg",
        received_utc="2026-08-03T17:33:15+00:00",
        event_utc="2026-07-29T03:54:00+00:00",
        from_call="K7ETC",
        to_target="MR08",
        groups=("MR08",),
        observed_topics=(),
        status="NEW",
        subject="141819Z 50",
        summary="<customform> | K7ETC | 141819Z 50",
        grid="MR08",
    )
    upsert_observation(db_path, obs)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE message_file_metadata (
                path TEXT PRIMARY KEY,
                source_family TEXT,
                msg_type TEXT,
                display_type TEXT,
                status TEXT,
                from_call TEXT,
                to_call TEXT,
                title TEXT,
                topics_json TEXT,
                search_text TEXT,
                report_ts REAL,
                source_label TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO message_file_metadata (
                path, source_family, msg_type, display_type, status, from_call,
                to_call, title, topics_json, search_text, report_ts, source_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(msg_path),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Widemouth 2 Fire",
                '["Fire","Weather"]',
                "flmsg new k7etc mr08 widemouth fire",
                1785297240.0,
                "FLMsg",
            ),
        )
        conn.commit()

    metadata = StationsMapTab._message_file_metadata_lookup(tab, db_path)
    rows = StationsMapTab._load_observation_operational_reports(
        tab,
        layer_name="report_focus",
        max_age_sec=0,
    )
    events = StationsMapTab._build_map_report_focus_events(tab, {}, max_age_sec=0)

    assert metadata[str(msg_path)]["grid"] == "DM38ST"
    assert metadata[str(msg_path)]["state"] == "UT"
    assert [row["callsign"] for row in rows] == ["K7ETC"]
    assert rows[0]["grid"] == "DM38ST"
    assert rows[0]["state"] == "UT"
    assert len(events) == 1
    assert events[0]["icon"] == "fire"
    assert "Widemouth 2 Fire" in events[0]["search_text"]
    assert StationsMapTab._map_event_matches_primary_filters(
        tab,
        events[0],
        group_filter="MR08",
        topic_filter="Fire",
        search_text="widemouth",
    )
    assert "Widemouth 2 Fire" in events[0]["tooltip"]


def test_map_topic_filter_uses_message_metadata_without_observation_rows(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260729-040212Z-57.k2s"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)

    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Fire")
    tab._map_search_edit = SimpleNamespace(text=lambda: "wildfire")
    tab.group_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.region_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.operator_index = {"K7ETC": {"region": "MR08"}}
    tab.stations = [StationPoint(callsign="K7ETC", grid="DM38ST", lat=38.5, lon=-112.5)]
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE message_file_metadata (
                path TEXT PRIMARY KEY,
                source_family TEXT,
                msg_type TEXT,
                display_type TEXT,
                status TEXT,
                from_call TEXT,
                to_call TEXT,
                title TEXT,
                topics_json TEXT,
                search_text TEXT,
                report_ts REAL,
                source_label TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO message_file_metadata (
                path, source_family, msg_type, display_type, status, from_call,
                to_call, title, topics_json, search_text, report_ts, source_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(msg_path),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Widemouth 2 Fire",
                '["Fire","Water"]',
                "K7ETC MR08 Widemouth 2 Fire wildfire UT DM38ST",
                1785297240.0,
                "FLMsg",
            ),
        )
        conn.commit()

    rows = StationsMapTab._load_message_metadata_operational_reports(
        tab,
        layer_name="infrastructure",
        max_age_sec=0,
    )
    calls = StationsMapTab._observation_station_scope_calls(tab, max_age_sec=0)
    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="message_metadata_infrastructure",
        display_label="Message Reports",
        reports_loader=lambda: rows,
    )

    assert [row["callsign"] for row in rows] == ["K7ETC"]
    assert rows[0]["summary"] == "Widemouth 2 Fire"
    assert rows[0]["grid"] == "DM38ST"
    assert "K7ETC" in calls
    assert len(events) == 1
    assert "Widemouth 2 Fire" in events[0]["tooltip"]


def test_map_report_focus_builds_fire_events_from_metadata_without_layer_toggles(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260729-040212Z-57.k2s"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)

    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Fire")
    tab._map_search_edit = SimpleNamespace(text=lambda: "wildfire")
    tab.group_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.region_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.operator_index = {}
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE message_file_metadata (
                path TEXT PRIMARY KEY,
                source_family TEXT,
                msg_type TEXT,
                display_type TEXT,
                status TEXT,
                from_call TEXT,
                to_call TEXT,
                title TEXT,
                topics_json TEXT,
                search_text TEXT,
                report_ts REAL,
                source_label TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO message_file_metadata (
                path, source_family, msg_type, display_type, status, from_call,
                to_call, title, topics_json, search_text, report_ts, source_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(msg_path),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Widemouth 2 Fire",
                '["Fire","Water"]',
                "K7ETC MR08 Widemouth 2 Fire wildfire UT DM38ST",
                1785297240.0,
                "FLMsg",
            ),
        )
        conn.commit()

    events = StationsMapTab._build_map_report_focus_events(tab, {}, max_age_sec=0)

    assert len(events) == 1
    assert events[0]["count"] == 1
    assert events[0]["icon"] == "fire"
    assert events[0]["primary_topic"] == "Fire"
    assert "Widemouth 2 Fire" in events[0]["tooltip"]


def test_map_all_group_sentinel_keeps_fire_metadata_reports_visible(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260729-040212Z-57.k2s"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)

    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab._map_topic_filter_combo = SimpleNamespace(currentText=lambda: "Fire")
    tab._map_search_edit = SimpleNamespace(text=lambda: "")
    tab.group_filter_combo = SimpleNamespace(currentData=lambda: "ALL")
    tab.region_filter_combo = SimpleNamespace(currentData=lambda: "")
    tab.operator_index = {}
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE message_file_metadata (
                path TEXT PRIMARY KEY,
                source_family TEXT,
                msg_type TEXT,
                display_type TEXT,
                status TEXT,
                from_call TEXT,
                to_call TEXT,
                title TEXT,
                topics_json TEXT,
                search_text TEXT,
                report_ts REAL,
                source_label TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO message_file_metadata (
                path, source_family, msg_type, display_type, status, from_call,
                to_call, title, topics_json, search_text, report_ts, source_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(msg_path),
                "flmsg",
                "FLMSG",
                "General",
                "NEW",
                "K7ETC",
                "MR08",
                "Widemouth 2 Fire",
                '["Fire","Water"]',
                "K7ETC MR08 Widemouth 2 Fire wildfire UT DM38ST",
                1785297240.0,
                "FLMsg",
            ),
        )
        conn.commit()

    rows = StationsMapTab._load_message_metadata_operational_reports(
        tab,
        layer_name="all_group_sentinel",
        max_age_sec=0,
    )
    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="all_group_sentinel",
        display_label="Message Reports",
        reports_loader=lambda: rows,
    )

    assert [row["callsign"] for row in rows] == ["K7ETC"]
    assert len(events) == 1
    assert events[0]["primary_topic"] == "Fire"


def test_map_event_primary_filters_search_message_intelligence_text() -> None:
    tab = _bare_tab()

    assert StationsMapTab._map_event_matches_primary_filters(
        tab,
        {
            "callsign": "K7ETC",
            "to_target": "MR08",
            "groups": ["MR08"],
            "topics": [],
            "summary": "General report",
            "search_text": "Widemouth 2 Fire wildfire UT DM38ST",
        },
        group_filter="All Groups",
        topic_filter="Fire",
        search_text="wildfire",
    )


def test_map_observed_file_paths_use_current_projection_schema(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"
    msg_path = tmp_path / "K7ETC-20260729-040212Z-57.k2s"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: cfg_root)

    tab = _bare_tab()

    upsert_observation(
        db_path,
        Observation(
            observation_id="flmsg:file:k7etc-fire",
            source_family="flmsg",
            source_ref=f"file:{msg_path}",
            source_app="FLMsg",
            received_utc="2026-08-03T17:33:15+00:00",
            event_utc="2026-07-29T03:54:00+00:00",
            from_call="K7ETC",
            to_target="",
            groups=("MR08",),
            observed_topics=(),
            status="NEW",
            subject="040212Z 57",
            summary="Widemouth 2 Fire",
            grid="MR08",
        ),
    )

    assert str(msg_path) in StationsMapTab._observed_message_file_paths(tab, db_path)


def test_map_operational_events_can_place_grid_only_observations() -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: [
            {
                "callsign": "K7ETC",
                "grid": "DM38ST",
                "summary": "Wildfire status",
                "icon": "warning",
                "severity": "severe",
                "utc_ts": 1786363200.0,
            }
        ],
    )

    assert len(events) == 1
    assert events[0]["lat"] != 0
    assert events[0]["lon"] != 0
    assert "Observation Alerts: 1" in events[0]["tooltip"]


def test_map_operational_events_do_not_place_group_tokens_as_grids() -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: [
            {
                "callsign": "K7ETC",
                "grid": "MR08",
                "summary": "Wildfire status",
                "icon": "warning",
                "severity": "severe",
                "utc_ts": 1786363200.0,
            }
        ],
    )

    assert events == []


def test_map_operational_events_can_place_condition_alerts_from_station_lookup() -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {
            "N1MAG": StationPoint(
                callsign="N1MAG",
                grid="DM79",
                lat=39.7,
                lon=-104.9,
            )
        },
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: [
            {
                "callsign": "N1MAG",
                "to_target": "MAGNET",
                "summary": "MAGNET condition level YELLOW",
                "icon": "warning",
                "severity": "caution",
                "utc_ts": 1786363200.0,
                "source_family": "condition_alert",
                "source_label": "Condition Alert",
                "topics": ["General Intel", "Comms"],
                "location_confidence": "sender lookup",
            }
        ],
    )

    assert len(events) == 1
    assert events[0]["lat"] == 39.7
    assert "Condition Alert | N1MAG -&gt; MAGNET" in events[0]["tooltip"]
    assert "Topics: General Intel, Comms" in events[0]["tooltip"]


def test_map_report_popup_text_distinguishes_hf_and_local_contexts(monkeypatch) -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()
    now = 1786365000.0
    monkeypatch.setattr("freqinout.gui.stations_map_tab.time.time", lambda: now)

    assert StationsMapTab._map_report_age_text(now - 17 * 60, now=now) == "17 min ago"
    assert StationsMapTab._map_report_age_text(now - 102 * 60, now=now) == "1:42 h ago"
    assert StationsMapTab._map_report_age_text(now - 16 * 24 * 60 * 60, now=now) == "16 days ago"

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="alert",
        display_label="Observation Alerts",
        reports_loader=lambda: [
            {
                "callsign": "K7ETC",
                "to_target": "MR08",
                "grid": "DM38ST",
                "state": "UT",
                "summary": "Wildfire status",
                "icon": "warning",
                "severity": "severe",
                "utc_ts": now - 17 * 60,
                "source_family": "spotter",
                "source_label": "HF JS8Spotter",
                "form_id": "F!307",
                "topics": ["Fire", "Travel/Roads"],
                "auth_state": "valid",
                "trusted_state": "trusted",
                "location_confidence": "grid",
            },
            {
                "callsign": "K0PRA",
                "to_target": "County GMRS",
                "grid": "DM38ST",
                "state": "CO",
                "summary": "Repeater degraded",
                "icon": "comms",
                "severity": "watch",
                "utc_ts": now - 102 * 60,
                "source_family": "local_report",
                "source_label": "Local Report",
                "topics": ["Comms"],
                "confirmed_state": "CONFIRMED",
                "location_confidence": "grid",
            },
        ],
    )

    tooltip = events[0]["tooltip"]
    assert events[0]["source_kind"] == "mixed"
    assert "Source: HF JS8Spotter 1, Local Report 1" in tooltip
    assert "HF JS8Spotter | F!307 | K7ETC -&gt; MR08 | 17 min ago" in tooltip
    assert "Topics: Fire, Travel/Roads" in tooltip
    assert "Auth: Valid, Trusted" in tooltip
    assert "Local Report | K0PRA -&gt; County GMRS | 1:42 h ago" in tooltip
    assert "Local: Confirmed" in tooltip
    assert "Area: CO / DM38ST (Grid)" in tooltip


def test_map_operational_event_primary_topic_uses_active_filter(monkeypatch) -> None:
    tab = _bare_tab()
    tab._cached_map_value = lambda _name, _key, loader, ttl_sec=0.0: loader()
    tab._selected_map_topic_filter = lambda: "Fire"
    monkeypatch.setattr("freqinout.gui.stations_map_tab.time.time", lambda: 1786365000.0)

    events = StationsMapTab._build_spotter_operational_events(
        tab,
        {},
        layer_name="alert",
        display_label="Message Reports",
        reports_loader=lambda: [
            {
                "callsign": "K7ETC",
                "to_target": "MR08",
                "grid": "DM38ST",
                "summary": "Widemouth 2 Fire",
                "icon": "general",
                "severity": "unknown",
                "utc_ts": 1786363200.0,
                "source_family": "flmsg",
                "source_label": "FLMsg",
                "topics": ["Comms", "Fire", "Water"],
            }
        ],
    )

    assert len(events) == 1
    assert events[0]["primary_topic"] == "Fire"
    assert events[0]["icon"] == "fire"


def test_map_html_legend_and_operational_markers_distinguish_report_sources() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "Report Source:" in source
    assert "op-source-hf" in source
    assert "op-source-local" in source
    assert "op-source-pin" in source
    assert "op-source-mixed" in source
    assert "event.source_kind" in source


def test_map_topic_controls_use_message_intelligence_taxonomy() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "RF_PIN_TOPICS = TOPIC_TAXONOMY" in source
    assert "Food" in TOPIC_TAXONOMY
    assert "Security" in TOPIC_TAXONOMY
    assert "Logistics" in TOPIC_TAXONOMY


def test_map_link_renderer_preserves_direction_and_quality_visuals() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "def _map_link_direction_markers_enabled" in source
    assert "link_direction_markers" in source
    assert "let linkDirectionMarkers" in source
    assert "function linkBearingDeg" in source
    assert "const showDirectionMarkers = !!linkDirectionMarkers" in source
    assert "const arrowRotation = bearing - 90;" in source
    assert "fio-link-arrow" in source
    assert "rotate(${{arrowRotation}}deg)" in source
    assert "&#10148;" in source
    assert "origin" in source
    assert "destination" in source
    assert "\\u2192" in source
    assert "SNR ${{snr}}" in source
    assert "list.length <= 80" in source


def test_map_link_direction_markers_are_topology_scoped() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    method_start = source.index("def _map_link_direction_markers_enabled")
    method_body = source[method_start : source.index("def _update_map_view_status_label", method_start)]

    assert 'mode_key == "paths"' in method_body
    assert 'link_mode in {"station", "relay_target"}' in method_body


def test_map_center_action_uses_leaflet_center_helper() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")
    method_start = source.index("def _center_map_selected_detail")
    method_body = source[method_start : source.index("def _map_selected_station_callsign", method_start)]

    assert "window.centerMapOn" in source
    assert "def _map_payload_latlon" in source
    assert "maidenhead_to_latlon(token)" in source
    assert "Number.isFinite(targetLat)" in source
    assert "return true;" in source
    assert "runJavaScript" in method_body
    assert "setView" in method_body


def test_map_leaflet_template_escapes_status_color_objects() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "const markerMeaningByStatus = {{" in source
    assert "const markerFillByStatus = {{" in source
    assert "const markerStrokeByStatus = {{" in source
    assert "green: 'Green: latest status is functioning'" in source
    assert "const markerMeaningByStatus = {\n" not in source


def test_map_since_filter_uses_chip_menu_with_extended_windows() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "self._map_since_button = QToolButton()" in source
    assert "self._build_map_since_menu()" in source
    assert 'filter_field("Age", self._map_since_button' in source
    assert "def _map_recency_menu_label" in source
    assert 'return "Any" if label == "Any" else label' in source
    assert '"15m": "15 min"' not in source
    assert '("15m", 15 * 60)' in source
    assert '("90d", 90 * 24 * 60 * 60)' in source


def test_map_view_mode_controls_wrap_to_grid() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "mode_action_buttons = (" in source
    assert "QGridLayout(mode_actions_row)" in source
    assert "idx // 5, idx % 5" in source
    assert "mode_actions_layout.addStretch" not in source


def test_map_topic_icon_mapping_covers_message_taxonomy() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert StationsMapTab._map_topic_icon("Fire") == "fire"
    assert StationsMapTab._map_topic_icon("Wildfire") == "fire"
    assert StationsMapTab._map_icon_for_topics(["Water"], preferred_topic="Fire") == "fire"
    tab = _bare_tab()
    tab._map_topic_filter_combo = SimpleNamespace(currentData=lambda: "Fire", currentText=lambda: "Fire")
    assert StationsMapTab._map_event_topic_and_icon(tab, ["Water", "Fire"], "water") == ("Fire", "fire")
    assert StationsMapTab._map_event_topic_and_icon(
        tab, ["Water", "Fire"], "water", preferred_topic="Fire"
    ) == ("Fire", "fire")
    assert StationsMapTab._map_event_topic_and_icon(tab, ["Water"], "water") == ("Fire", "fire")
    tab._map_topic_filter_combo = SimpleNamespace(currentData=lambda: "", currentText=lambda: "All Topics")
    assert StationsMapTab._map_event_topic_and_icon(tab, ["Comms"], "water") == ("Comms", "comms")
    assert "def _map_topic_icon" in source
    assert "def _map_event_topic_and_icon" in source
    assert "primary_topic, event_icon = self._map_event_topic_and_icon" in source
    assert "if (kind === 'fire')" in source
    assert "M12 10v6" in source
    for topic in (
        '"weather": "storm"',
        '"fire": "fire"',
        '"medical": "medical"',
        '"power": "power"',
        '"water": "water"',
        '"fuel": "fuel"',
        '"food": "food"',
        '"travel/roads": "transport"',
        '"comms": "comms"',
        '"security": "security"',
        '"shelter": "shelter"',
        '"logistics": "logistics"',
        '"infrastructure": "utility"',
        '"general intel": "warning"',
    ):
        assert topic in source
    for icon in (
        "fire",
        "medical",
        "power",
        "water",
        "fuel",
        "food",
        "transport",
        "comms",
        "security",
        "shelter",
        "logistics",
        "utility",
        "storm",
        "general",
    ):
        assert f"op-kind-{icon}" in source


def test_map_detail_payload_cleans_html_tooltip_fallback() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "function cleanMapDetailText(value)" in source
    assert "function normalizeMapSourceLabel(value)" in source
    assert "const cleaned = cleanMapDetailText(value);" in source
    assert "summary: cleanMapDetailText(event.summary || event.tooltip || title)" in source
    assert "map.closePopup();" in source
    assert "document.querySelectorAll('.leaflet-popup')" in source
    assert "return 'Multiple Sources';" in source
    assert "return 'Planning Pin';" in source
    assert ".replace(/&lt;/g, '<')" in source
    assert ".replace(/<br\\s*\\/?>/gi, '\\\\n')" in source


def test_map_report_focus_overrides_advanced_station_scope() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "all_reports"
    tab._map_scope_filter_combo = SimpleNamespace(currentData=lambda: "stations")

    assert tab._advanced_filters_allow_reports() is False
    assert tab._map_reports_allowed_for_current_view() is True


def test_map_report_topic_search_overrides_advanced_station_scope() -> None:
    tab = _bare_tab()
    tab._map_scope_filter_combo = SimpleNamespace(currentData=lambda: "stations")
    tab._map_topic_filter_combo = SimpleNamespace(currentData=lambda: "Fire", currentText=lambda: "Fire")
    tab._map_search_edit = SimpleNamespace(text=lambda: "")

    assert tab._advanced_filters_allow_reports() is False
    assert tab._map_reports_allowed_for_current_view() is True


def test_map_topic_change_refines_without_forcing_report_mode_or_recency() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = False
    tab._observation_focus_mode = ""
    tab.recency_seconds = None
    tab.group_filter_combo = _FakeCombo([("All", "")])
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire")])
    tab._map_topic_filter_combo.setCurrentIndex(1)
    calls: list[str] = []
    tab._clear_report_query_caches = lambda: calls.append("clear_cache")
    tab._update_clear_filter_buttons_visual = lambda: calls.append("clear_visual")
    tab._update_map_mode_buttons = lambda: calls.append("mode_buttons")
    tab._update_map_view_status_label = lambda: calls.append("status")
    tab._request_map_refresh = lambda **kwargs: calls.append(str(kwargs.get("reason") or ""))

    StationsMapTab._on_map_topic_filter_changed(tab, 1)

    assert tab.recency_seconds is None
    assert tab._observation_focus_enabled is False
    assert tab._effective_map_observation_focus_enabled() is True
    assert tab._effective_map_observation_focus_mode() == "all_reports"
    assert calls[-1] == "topic_filter"


def test_map_fire_topic_from_all_stations_builds_report_focus_events() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = False
    tab._observation_focus_mode = ""
    tab.recency_seconds = 0
    tab.group_filter_combo = _FakeCombo([("All", "")])
    tab.region_filter_combo = _FakeCombo([("All", "")])
    tab.band_combo = _FakeCombo([("All", {"type": "all"})])
    tab._map_source_filter_combo = _FakeCombo([("All Sources", "")])
    tab._map_state_filter_combo = _FakeCombo([("All States", "")])
    tab._map_status_filter_combo = _FakeCombo([("All Statuses", "")])
    tab._map_scope_filter_combo = _FakeCombo([("Stations Only", "stations")])
    tab._map_trust_filter_combo = _FakeCombo([("All Auth/Trust", "")])
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire")])
    tab._map_topic_filter_combo.setCurrentIndex(1)
    tab._map_search_edit = _FakeLineEdit("")
    tab._load_observation_operational_reports = lambda **_kwargs: []
    tab._load_message_metadata_operational_reports = lambda **_kwargs: [
        {
            "callsign": "K7ETC",
            "grid": "DM38ST",
            "lat": 38.0,
            "lon": -112.0,
            "topics": ["Fire"],
            "source_family": "flmsg",
            "source_type": "flmsg",
            "summary": "Widemouth 2 Fire",
            "utc_ts": 1000,
            "group": "MR08",
        }
    ]

    events = StationsMapTab._build_map_report_focus_events(tab, {}, max_age_sec=0)

    assert tab._advanced_filters_allow_reports() is False
    assert tab._map_reports_allowed_for_current_view() is True
    assert len(events) == 1
    assert events[0]["primary_topic"] == "Fire"
    assert events[0]["icon"] == "fire"
    assert "Widemouth 2 Fire" in str(events[0].get("summary") or "")


def test_map_fire_topic_refines_paths_layer_with_report_focus_events() -> None:
    tab = _bare_tab()
    tab._observation_focus_enabled = True
    tab._observation_focus_mode = "paths"
    tab.recency_seconds = 0
    tab.group_filter_combo = _FakeCombo([("All", "")])
    tab.region_filter_combo = _FakeCombo([("All", "")])
    tab.band_combo = _FakeCombo([("All", {"type": "all"})])
    tab._map_source_filter_combo = _FakeCombo([("All Sources", "")])
    tab._map_state_filter_combo = _FakeCombo([("All States", "")])
    tab._map_status_filter_combo = _FakeCombo([("All Statuses", "")])
    tab._map_scope_filter_combo = _FakeCombo([("Stations Only", "stations")])
    tab._map_trust_filter_combo = _FakeCombo([("All Auth/Trust", "")])
    tab._map_topic_filter_combo = _FakeCombo([("All Topics", ""), ("Fire", "Fire")])
    tab._map_topic_filter_combo.setCurrentIndex(1)
    tab._map_search_edit = _FakeLineEdit("")
    tab._load_observation_operational_reports = lambda **_kwargs: []
    tab._load_message_metadata_operational_reports = lambda **_kwargs: [
        {
            "callsign": "K7ETC",
            "grid": "DM38ST",
            "lat": 38.0,
            "lon": -112.0,
            "topics": ["Fire"],
            "source_family": "flmsg",
            "source_type": "flmsg",
            "summary": "Widemouth 2 Fire",
            "utc_ts": 1000,
            "group": "MR08",
        }
    ]

    assert tab._current_map_mode_key() == "paths"
    assert tab._effective_map_observation_focus_mode() == "paths"
    assert tab._effective_map_report_focus_mode() == "all_reports"
    assert tab._map_reports_allowed_for_current_view() is True

    events = StationsMapTab._build_map_report_focus_events(tab, {}, max_age_sec=0)

    assert len(events) == 1
    assert events[0]["primary_topic"] == "Fire"
    assert events[0]["icon"] == "fire"


def test_spotter_layer_reports_any_recency_keeps_historical_reports(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path
    db_dir = config_dir / "config"
    db_dir.mkdir()
    db_path = db_dir / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE spotter_traffic (
            id INTEGER PRIMARY KEY,
            from_call TEXT,
            form_id TEXT,
            utc_ts REAL,
            utc_str TEXT,
            decoded_text TEXT,
            raw_text TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO spotter_traffic (from_call, form_id, utc_ts, utc_str, decoded_text, raw_text)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("K7ETC", "307", 100.0, "2026-08-02T11:56:42Z", "Wildfire report DM38ST", ""),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr("freqinout.gui.stations_map_tab.get_config_dir", lambda: config_dir)

    tab = _bare_tab()
    rows = StationsMapTab._load_spotter_layer_reports(
        tab,
        layer_name="alert",
        form_codes={"F!307"},
        max_age_sec=0,
        classifier=lambda _text: ("fire", "severe"),
        summarizer=lambda text: str(text),
    )

    assert len(rows) == 1
    assert rows[0]["callsign"] == "K7ETC"
    assert rows[0]["form_id"] == "F!307"


def test_map_group_and_region_filter_helpers_normalize_all_and_js8_targets() -> None:
    tab = _bare_tab()
    tab.group_filter_combo = _FakeCombo([("All", ""), ("@MR08>", "@MR08>")])
    tab.region_filter_combo = _FakeCombo([("All Regions", ""), ("Region MR08", "Region MR08")])

    assert tab._selected_map_group_filter() == ""
    assert tab._selected_map_region_filter() == ""

    tab.group_filter_combo.setCurrentIndex(1)
    tab.region_filter_combo.setCurrentIndex(1)

    assert tab._selected_map_group_filter() == "MR08"
    assert tab._selected_map_region_filter() == "MR08"


def test_message_context_applies_filters_after_search_is_set() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    calls: list[str] = []
    tab.rcv_search = _FakeLineEdit("")
    tab.show_inbox_from_navigation = lambda: calls.append("nav")
    tab._set_inbox_focus = lambda focus: calls.append(f"focus:{focus}")
    tab._select_context_group_filter = lambda _group: False
    tab._select_context_source_filter = lambda _sources: False
    tab._message_context_source_values = lambda _source: []
    tab._message_context_source_search_fallback = lambda source: source
    tab._apply_message_filters = lambda: calls.append(f"apply:{tab.rcv_search.text()}")

    MessageViewerTab.show_inbox_with_context(
        tab,
        group_filter="MR08",
        topic_filter="Fire",
        query_filter="K7ETC",
        source_family="",
    )

    assert calls[-1] == "apply:K7ETC Fire MR08"


def test_map_selected_latlon_reads_alias_and_nested_payloads() -> None:
    tab = _bare_tab()

    assert tab._map_payload_latlon({"latitude": "39.12", "longitude": "-104.88"}) == (39.12, -104.88)
    assert tab._map_payload_latlon({"payload": {"lat": "38.5", "lng": "-105.25"}}) == (38.5, -105.25)


def test_map_to_messages_context_uses_real_filters_before_search_fallback() -> None:
    source = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")

    assert "def _message_context_source_values" in source
    assert 'if source == "js8call":\n            return ["js8"]' in source
    assert 'if source == "forms":\n            return ["flmsg", "flamp"]' in source
    assert 'if source == "js8spotter":\n            return ["spotter"]' in source
    assert "selected_group = self._select_context_group_filter(group_filter)" in source
    assert "selected_source = self._select_context_source_filter(source_values)" in source
    assert '"" if selected_group else str(group_filter or "").strip().lstrip("@")' in source
    assert '"" if selected_source else self._message_context_source_search_fallback(source)' in source


def test_map_leaflet_template_includes_operator_zoom_presets() -> None:
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "zoom-chip" in source
    assert "window.fitMapResults" in source
    assert "window.zoomPreset" in source
    assert 'data-zoom-preset="fit">Fit Results' in source
    assert 'data-zoom-preset="station">Station' in source
    assert 'data-zoom-preset="region">Region' in source
    assert 'data-zoom-preset="north-america">North America' in source
    assert "markers = payload.markers; renderMarkers(markers);" in source
    assert "links = payload.links; renderLinks(links);" in source
