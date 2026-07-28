from __future__ import annotations

import os
import types
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QLabel,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QVBoxLayout,
    QWidget,
)

from freqinout.core.shared_state import ActionFeedbackService
from freqinout.gui.settings_tab import _coerce_json_mapping
from freqinout.core.multi_radio_store import (
    MultiRadioStore,
    _legacy_settings_projection_from_device,
    settings_db_path,
)
from freqinout.core.settings_manager import SettingsManager


def test_legacy_projection_includes_radio_scoped_software_paths() -> None:
    projected = _legacy_settings_projection_from_device(
        {
            "control_backend": "flrig",
            "use_flrig": 1,
            "use_fldigi": 1,
            "use_flmsg": 1,
            "use_flamp": 1,
            "use_js8call": 0,
            "use_js8spotter": 0,
            "use_commstat": 0,
            "use_varac": 1,
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "flrig_path": "/Applications/FLRig.app",
            "fldigi_host": "127.0.0.1",
            "fldigi_port": 7362,
            "fldigi_path": "/Applications/FLDigi.app",
            "flmsg_path": "/Applications/FLMsg.app",
            "flmsg_message_path": "/tmp/messages/flmsg",
            "flamp_path": "/Applications/FLAmp.app",
            "flamp_message_path": "/tmp/messages/flamp",
            "varac_install_path": "/Applications/VarAC",
            "varac_db_path": "/Applications/VarAC/VarAC.db",
            "varac_ini_path": "/Applications/VarAC/VarAC.ini",
            "varac_incoming_path": "/Applications/VarAC/RX Files",
            "varac_outbox_dir": "/Applications/VarAC/Outbox",
            "varac_bbs_dir": "/Applications/VarAC/BBS",
            "varac_bbs_archive_dir": "/Applications/VarAC/BBSArchive",
            "varac_bbs_auto_archive_enabled": 1,
            "varac_bbs_auto_archive_days": 21,
            "launch_cmd": "",
            "launch_enabled": 1,
        },
        {"message_paths": {}},
    )

    assert projected["path_flmsg"] == "/Applications/FLMsg.app"
    assert projected["path_flamp"] == "/Applications/FLAmp.app"
    assert projected["message_paths"]["flmsg"] == "/tmp/messages/flmsg"
    assert projected["message_paths"]["flamp"] == "/tmp/messages/flamp"
    assert projected["varac_outbox_dir"] == "/Applications/VarAC/Outbox"
    assert projected["varac_bbs_dir"] == "/Applications/VarAC/BBS"
    assert projected["varac_bbs_archive_dir"] == "/Applications/VarAC/BBSArchive"
    assert projected["varac_bbs_auto_archive_enabled"] is True
    assert projected["varac_bbs_auto_archive_days"] == 21


def test_device_profile_persists_radio_owned_software_fields(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = next(row for row in store.list_device_profiles() if int(row.get("runtime_primary", 0) or 0) == 1)

    saved = store.save_device_profile(
        {
            "id": int(primary["id"]),
            "name": str(primary.get("name", "") or "Default Radio"),
            "control_backend": str(primary.get("control_backend", "") or "flrig"),
            "runtime_active": int(primary.get("runtime_active", 0) or 0),
            "runtime_primary": 1,
            "use_flrig": 1,
            "use_fldigi": 1,
            "use_flmsg": 1,
            "use_flamp": 1,
            "use_varac": 1,
            "flmsg_path": "/apps/flmsg",
            "flmsg_message_path": "/msgs/flmsg",
            "flamp_path": "/apps/flamp",
            "flamp_message_path": "/msgs/flamp",
            "varac_install_path": "/varac",
            "varac_db_path": "/varac/VarAC.db",
            "varac_outbox_dir": "/varac/outbox",
            "varac_bbs_dir": "/varac/bbs",
            "varac_bbs_archive_dir": "/varac/archive",
            "varac_bbs_auto_archive_enabled": 1,
            "varac_bbs_auto_archive_days": 30,
        }
    )

    assert saved["flmsg_path"] == "/apps/flmsg"
    assert saved["flmsg_message_path"] == "/msgs/flmsg"
    assert saved["flamp_path"] == "/apps/flamp"
    assert saved["flamp_message_path"] == "/msgs/flamp"
    assert saved["varac_outbox_dir"] == "/varac/outbox"
    assert saved["varac_bbs_dir"] == "/varac/bbs"
    assert saved["varac_bbs_archive_dir"] == "/varac/archive"
    assert int(saved["varac_bbs_auto_archive_days"]) == 30


def test_coerce_json_mapping_accepts_stored_json_text() -> None:
    assert _coerce_json_mapping("") == {}
    assert _coerce_json_mapping("{}") == {}
    assert _coerce_json_mapping('{"active_request":"INTEL"}') == {"active_request": "INTEL"}
    assert _coerce_json_mapping("[]") == {}


def test_settings_tab_source_mentions_radio_scoped_software_view() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "Radio Software View" in source
    assert "These software pages stay in the familiar single-rig layout" in source
    assert "Launch Control and operating status still follow the Station Default compatibility projection." in source


def test_settings_tab_source_keeps_radio_context_labels_in_sync() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "self.device_assignments_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.js8_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.fast_light_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.varac_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.custom_tools_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "self.launch_control_scope_label = QLabel(\"Editing Radio: --\")" in source
    assert "def _refresh_radio_context_labels(self) -> None:" in source
    assert "self._refresh_software_scope_labels()" in source
    assert "self._refresh_device_assignment_scope_label()" in source
    focus_block = source[
        source.index("def _set_settings_radio_focus") : source.index("def _refresh_radio_settings_nav_label")
    ]
    assert "self._refresh_radio_context_labels()" in focus_block


def test_settings_radio_context_labels_and_save_feedback_share_selected_radio() -> None:
    from PySide6.QtWidgets import QApplication, QLabel

    from freqinout.gui.settings_tab import SettingsTab

    QApplication.instance() or QApplication([])
    profile = {
        "id": 7,
        "name": "DX10",
        "runtime_primary": 1,
        "runtime_active": 1,
        "control_backend": "flrig",
        "use_flrig": 1,
    }
    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab.settings = types.SimpleNamespace(get=lambda _key, default=None: default)
    tab.device_assignments_scope_label = QLabel("Editing Radio: --")
    tab.js8_scope_label = QLabel("Editing Radio: --")
    tab.fast_light_scope_label = QLabel("Editing Radio: --")
    tab.varac_scope_label = QLabel("Editing Radio: --")
    tab.custom_tools_scope_label = QLabel("Editing Radio: --")
    tab.launch_control_scope_label = QLabel("Editing Radio: --")
    tab.settings_action_feedback_label = QLabel("Settings ready.")
    tab._selected_settings_radio_profile = lambda: profile
    tab._selected_software_radio_profile = lambda: profile
    tab._effective_assignment_map = lambda: {
        7: {
            "operating_profile_name": "Evening Net",
            "assignment_state": "active",
        }
    }

    tab._refresh_radio_context_labels()
    tab._publish_settings_action_feedback(status="succeeded", summary="Saved DX10 settings.")

    assert tab.device_assignments_scope_label.text().startswith("Editing Radio: DX10.")
    assert "Evening Net (Active)" in tab.device_assignments_scope_label.text()
    assert tab.js8_scope_label.text().startswith("Editing Radio: DX10 (Station Default).")
    assert tab.fast_light_scope_label.text() == tab.js8_scope_label.text()
    assert "Custom tools are still shared" in tab.custom_tools_scope_label.text()
    assert "Launch Control currently follows the Station Default projection" in tab.launch_control_scope_label.text()
    assert tab.settings_action_feedback_label.text() == "Saved DX10 settings."
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"


def test_settings_nav_buttons_are_left_aligned_and_consistent() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    nav_build_block = source[
        source.index("nav_panel = QWidget()")
        : source.index("self.sections_stack = QStackedWidget()")
    ]
    add_section_block = source[
        source.index("def _add_settings_section")
        : source.index("def _select_settings_section_group")
    ]
    style_block = source[
        source.index("def _refresh_settings_nav_button_styles")
        : source.index("def _set_settings_section_visible")
    ]

    assert "self.global_settings_toggle_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)" in nav_build_block
    assert "self.radio_settings_toggle_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)" in nav_build_block
    assert 'self.global_settings_toggle_btn.setAccessibleName("Settings navigation group: Global Settings")' in nav_build_block
    assert 'self.radio_settings_toggle_btn.setAccessibleName("Settings navigation group: Selected Radio")' in nav_build_block
    assert "self.global_section_buttons_layout.setContentsMargins(0, 0, 0, 0)" in nav_build_block
    assert "self.radio_section_buttons_layout.setContentsMargins(0, 0, 0, 0)" in nav_build_block
    assert 'nav_panel.setObjectName("settingsSectionNavPanel")' in nav_build_block
    assert 'self.settings_section_nav_scroll.setObjectName("settingsSectionNavScroll")' in nav_build_block
    assert "self.settings_section_nav_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)" in nav_build_block
    assert "self.settings_section_nav_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in nav_build_block
    assert "self.settings_section_nav_scroll.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)" in nav_build_block
    assert 'btn.setAccessibleName(f"Settings navigation: {title}")' in add_section_block
    assert "btn.setStyleSheet(self._settings_nav_button_style(role, theme))" in style_block
    assert "self._settings_nav_group_toggle_role(\"global\")" in style_block
    assert "self._settings_nav_group_toggle_role(\"radio\")" in style_block
    assert 'return "secondary" if bool(getattr(self, "_global_settings_nav_collapsed", True)) else "eligible_info"' in style_block
    assert 'return "secondary" if bool(getattr(self, "_radio_settings_nav_collapsed", False)) else "eligible_info"' in style_block
    assert "def _settings_nav_button_style" in style_block
    assert '" text-align: left;"' in style_block
    assert '" padding-left: 10px;"' in style_block
    assert '" QToolButton {"' in style_block
    assert '" padding-left: 8px;"' in style_block
    assert '" padding-right: 10px;"' in style_block


def test_settings_section_navigation_scrolls_without_horizontal_content_scroll(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)

    tab = SettingsTab()
    try:
        tab.show()
        app.processEvents()

        assert tab.settings_section_nav_scroll.widgetResizable() is True
        assert tab.settings_section_nav_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert tab.settings_section_nav_scroll.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert tab.settings_section_nav_scroll.sizePolicy().horizontalPolicy() == QSizePolicy.Fixed
        assert tab.settings_section_nav_scroll.sizePolicy().verticalPolicy() == QSizePolicy.Expanding
        assert tab.settings_section_nav_scroll.maximumWidth() <= 250
        assert tab.sections_scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAsNeeded

        tab.resize(720, 420)
        app.processEvents()

        assert tab.settings_section_nav_scroll.isVisible() is True
        assert tab.settings_section_nav_scroll.width() <= tab.settings_section_nav_scroll.maximumWidth()
        assert tab.settings_section_nav_scroll.height() > 0
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_nav_group_toggle_role_distinguishes_expanded_groups() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    tab._global_settings_nav_collapsed = True
    tab._radio_settings_nav_collapsed = False

    assert SettingsTab._settings_nav_group_toggle_role(tab, "global") == "secondary"
    assert SettingsTab._settings_nav_group_toggle_role(tab, "radio") == "eligible_info"

    tab._global_settings_nav_collapsed = False
    tab._radio_settings_nav_collapsed = True

    assert SettingsTab._settings_nav_group_toggle_role(tab, "global") == "eligible_info"
    assert SettingsTab._settings_nav_group_toggle_role(tab, "radio") == "secondary"


def test_settings_nav_button_style_preserves_left_aligned_hover_focus_affordance() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    theme = {
        "accent": "#2563eb",
        "accent_hover": "#1d4ed8",
        "accent_active": "#1e40af",
        "border": "#94a3b8",
        "danger": "#dc2626",
        "info": "#0891b2",
        "success": "#16a34a",
        "surface": "#ffffff",
        "surface_alt": "#f1f5f9",
        "text": "#0f172a",
        "text_muted": "#475569",
        "warning": "#f59e0b",
    }

    style = SettingsTab._settings_nav_button_style("eligible_info", theme)

    assert "QPushButton:hover" in style
    assert "QToolButton:hover" in style
    assert "text-align: left;" in style
    assert "padding-left: 10px;" in style
    assert "QToolButton {" in style
    assert "padding-left: 8px;" in style
    assert "#A1D5E1" in style
    assert "#4098B4" in style


def test_settings_section_nav_visuals_cover_selected_hover_warning_and_neutral() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    theme = {
        "accent": "#2563eb",
        "warning": "#f59e0b",
        "surface": "#ffffff",
        "text": "#0f172a",
        "bg": "#ffffff",
    }

    selected = SettingsTab._section_nav_visuals(tab, "neutral", selected=True, hovered=False, theme=theme)
    hovered = SettingsTab._section_nav_visuals(tab, "neutral", selected=False, hovered=True, theme=theme)
    warning = SettingsTab._section_nav_visuals(tab, "warn", selected=False, hovered=True, theme=theme)
    neutral = SettingsTab._section_nav_visuals(tab, "neutral", selected=False, hovered=False, theme=theme)

    assert isinstance(selected["bg"], QColor)
    assert selected["border"].name() == "#2563eb"
    assert selected["bg"].alpha() == 92
    assert selected["bold"] is True
    assert hovered["border"].name() == "#2563eb"
    assert hovered["bg"].name() == "#ffffff"
    assert hovered["bold"] is False
    assert warning["border"].name() == "#f59e0b"
    assert warning["bg"].alpha() == 84
    assert warning["bold"] is True
    assert neutral["bg"].alpha() == 0
    assert neutral["border"].alpha() == 0
    assert neutral["bold"] is False


def test_settings_group_tables_use_compact_height_policy() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    build_block = source[
        source.index("# HF Operating Groups panel")
        : source.index("# JS8Call status/settings")
    ]
    op_refresh_block = source[
        source.index("def _refresh_operating_groups_table")
        : source.index("def _update_op_group_action_buttons")
    ]
    local_refresh_block = source[
        source.index("def _refresh_local_net_profiles_table")
        : source.index("def _local_profile_from_row")
    ]

    assert "self.op_groups_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in build_block
    assert "self.local_net_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in build_block
    assert "fit_content_in_stack: bool = False" in source
    assert '"fit_content_in_stack": bool(fit_content_in_stack)' in source
    assert "ops_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in build_block
    assert "local_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in build_block
    assert "fit_content=True,\n            fit_content_in_stack=True,\n            help_context_key=\"settings.hf-groups\"" in build_block
    assert "fit_content=True,\n            fit_content_in_stack=True,\n            help_context_key=\"settings.local-comms\"" in build_block
    assert "self.op_groups_section_group = ops_group" in build_block
    assert "self.local_net_section_group = local_group" in build_block
    assert "ops_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in build_block
    assert "local_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in build_block
    assert "six rows keeps the section scannable" in op_refresh_block
    assert "six rows keeps the section scannable" in local_refresh_block
    assert "self._fit_table_height_to_rows(table, min_rows=1, max_rows=6, extra_rows=1)" in op_refresh_block
    assert "self._fit_table_height_to_rows(table, min_rows=1, max_rows=6, extra_rows=1)" in local_refresh_block
    assert "self._refresh_fit_content_section_height(getattr(self, \"op_groups_section_group\", None))" in op_refresh_block
    assert "self._refresh_fit_content_section_height(getattr(self, \"local_net_section_group\", None))" in local_refresh_block


def test_settings_frequency_plan_and_spotter_tables_own_their_scroll_geometry() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    frequency_build_block = source[
        source.index("self.operating_profiles_table = QTableWidget(0, 6)")
        : source.index("assignments_group = QGroupBox(\"Assigned Plans\")")
    ]
    frequency_refresh_block = source[
        source.index("def _refresh_operating_profiles_table")
        : source.index("def _refresh_device_assignments_table")
    ]
    spotter_build_block = source[
        source.index("self.spotter_mapper_table = QTableWidget(0, 8)")
        : source.index("mapper_hint = QLabel(")
    ]
    spotter_refresh_block = source[
        source.index("def _refresh_spotter_form_mapper")
        : source.index("def _on_spotter_mapper_changed")
    ]

    assert "self.operating_profiles_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in frequency_build_block
    assert "self.operating_profiles_table.setWordWrap(False)" in frequency_build_block
    assert "self._fit_table_height_to_rows(table, min_rows=1, max_rows=8, extra_rows=1)" in frequency_refresh_block
    assert "self._refresh_fit_content_section_height(getattr(self, \"operating_profiles_section_group\", None))" in frequency_refresh_block
    assert "self.spotter_mapper_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)" in spotter_build_block
    assert "self.spotter_mapper_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)" in spotter_build_block
    assert "self.spotter_mapper_table.setWordWrap(False)" in spotter_build_block
    assert "self._fit_table_height_to_rows(self.spotter_mapper_table, min_rows=3, max_rows=6, extra_rows=0)" in spotter_refresh_block
    assert "self._refresh_fit_content_section_height(getattr(self, \"js8_section_group\", None))" in spotter_refresh_block


def test_fit_table_height_to_rows_caps_body_and_keeps_header_scroll_space() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    _ = app
    table = QTableWidget(10, 3)
    table.setHorizontalHeaderLabels(["One", "Two", "Three"])
    table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    table.resizeRowsToContents()

    SettingsTab._fit_table_height_to_rows(table, min_rows=1, max_rows=6, extra_rows=1)

    default_row_height = max(table.verticalHeader().defaultSectionSize(), 24)
    row_height = max(table.rowHeight(0), default_row_height)
    expected = (
        table.horizontalHeader().height()
        + (6 * row_height)
        + table.horizontalScrollBar().sizeHint().height()
        + (table.frameWidth() * 2)
        + 8
    )
    full_body_height = table.horizontalHeader().height() + (10 * row_height)

    assert table.minimumHeight() == expected
    assert table.maximumHeight() == expected
    assert table.maximumHeight() < full_body_height


def test_settings_group_tables_visual_geometry_caps_to_internal_scroll(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)

    tab = SettingsTab()
    try:
        tab.operating_groups = [
            {
                "group": f"G{idx}",
                "mode": "Digi",
                "band": "40M",
                "frequency": "7.078",
                "vfo": "A",
                "fldigi_mode": "Olivia",
                "fldigi_offset": "1500",
                "auto_tune": False,
                "use_condition_levels": False,
            }
            for idx in range(10)
        ]
        tab.local_net_profiles = [
            {
                "group": f"L{idx}",
                "resource": "Voice",
                "mode": "Phone",
                "target": "Local",
                "notes": "Check-in",
            }
            for idx in range(10)
        ]
        tab._refresh_operating_groups_table()
        tab._refresh_local_net_profiles_table()
        app.processEvents()

        for table in (tab.op_groups_table, tab.local_net_table):
            default_row_height = max(table.verticalHeader().defaultSectionSize(), 24)
            row_height = max(table.rowHeight(0), default_row_height)
            expected = (
                table.horizontalHeader().height()
                + (6 * row_height)
                + table.horizontalScrollBar().sizeHint().height()
                + (table.frameWidth() * 2)
                + 8
            )
            full_body_height = table.horizontalHeader().height() + (table.rowCount() * row_height)

            assert table.rowCount() == 10
            assert table.minimumHeight() == expected
            assert table.maximumHeight() == expected
            assert table.maximumHeight() < full_body_height
            assert table.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
            assert table.horizontalHeader().height() > 0
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_frequency_plan_and_spotter_table_geometry_caps_to_internal_scroll(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)

    tab = SettingsTab()
    try:
        plans = [
            {
                "id": idx + 1,
                "name": f"Plan {idx}",
                "enabled": 1,
                "scheduler_enabled": 1,
                "description": "A longer description that should not expand the table row body.",
            }
            for idx in range(10)
        ]
        tab.multi_radio_store = types.SimpleNamespace(list_operating_profiles=lambda: list(plans))
        tab._refresh_operating_profiles_table(refresh_assignments=False)
        tab._refresh_spotter_form_mapper()
        app.processEvents()

        frequency_table = tab.operating_profiles_table
        assert frequency_table.rowCount() == 10
        assert frequency_table.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert frequency_table.sizePolicy().verticalPolicy() == QSizePolicy.Preferred
        assert frequency_table.wordWrap() is False

        default_row_height = max(frequency_table.verticalHeader().defaultSectionSize(), 24)
        row_height = max(frequency_table.rowHeight(0), default_row_height)
        expected_frequency = (
            frequency_table.horizontalHeader().height()
            + (8 * row_height)
            + frequency_table.horizontalScrollBar().sizeHint().height()
            + (frequency_table.frameWidth() * 2)
            + 8
        )
        assert frequency_table.minimumHeight() == expected_frequency
        assert frequency_table.maximumHeight() == expected_frequency
        assert frequency_table.maximumHeight() < frequency_table.horizontalHeader().height() + (10 * row_height)

        spotter_table = tab.spotter_mapper_table
        assert spotter_table.rowCount() >= 5
        assert spotter_table.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
        assert spotter_table.sizePolicy().verticalPolicy() == QSizePolicy.Preferred
        assert spotter_table.wordWrap() is False

        spotter_default_row_height = max(spotter_table.verticalHeader().defaultSectionSize(), 24)
        spotter_row_height = max(spotter_table.rowHeight(0), spotter_default_row_height)
        expected_spotter = (
            spotter_table.horizontalHeader().height()
            + (min(spotter_table.rowCount(), 6) * spotter_row_height)
            + spotter_table.horizontalScrollBar().sizeHint().height()
            + (spotter_table.frameWidth() * 2)
            + 8
        )
        assert spotter_table.minimumHeight() == expected_spotter
        assert spotter_table.maximumHeight() == expected_spotter
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_fit_content_group_geometry_refreshes_without_page_stretch(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)

    tab = SettingsTab()
    try:
        assert tab.sections_scroll.widgetResizable() is True
        assert tab._section_meta[tab.op_groups_section_group]["fit_content_in_stack"] is True
        assert tab._section_meta[tab.local_net_section_group]["fit_content_in_stack"] is True
        assert tab.op_groups_section_group.sizePolicy().verticalPolicy() == QSizePolicy.Preferred
        assert tab.local_net_section_group.sizePolicy().verticalPolicy() == QSizePolicy.Preferred

        tab.operating_groups = [
            {
                "group": f"G{idx}",
                "mode": "Digi",
                "band": "40M",
                "frequency": "7.078",
                "vfo": "A",
                "fldigi_mode": "Olivia",
                "fldigi_offset": "1500",
                "auto_tune": False,
                "use_condition_levels": False,
            }
            for idx in range(12)
        ]
        tab.local_net_profiles = [
            {
                "group": f"L{idx}",
                "resource": "Voice",
                "mode": "Phone",
                "target": "Local",
                "notes": "Check-in",
            }
            for idx in range(12)
        ]

        tab._refresh_operating_groups_table()
        tab._refresh_local_net_profiles_table()
        app.processEvents()

        expanded_heights = {
            tab.op_groups_section_group: tab.op_groups_section_group.minimumHeight(),
            tab.local_net_section_group: tab.local_net_section_group.minimumHeight(),
        }

        for group, table in (
            (tab.op_groups_section_group, tab.op_groups_table),
            (tab.local_net_section_group, tab.local_net_table),
        ):
            assert group.minimumHeight() == expanded_heights[group]
            assert group.maximumHeight() == 16777215
            assert expanded_heights[group] >= table.maximumHeight()
            assert table.verticalScrollBarPolicy() == Qt.ScrollBarAsNeeded
            assert table.maximumHeight() < table.horizontalHeader().height() + (table.rowCount() * max(table.rowHeight(0), 24))

            meta = tab._section_meta[group]
            content = meta["content"]
            tab._apply_collapsed_state(group, content, False)
            app.processEvents()
            collapsed_height = group.maximumHeight()

            assert content.isHidden() is True
            assert group.minimumHeight() == collapsed_height
            assert collapsed_height < expanded_heights[group]
            assert group.sizePolicy().verticalPolicy() == QSizePolicy.Fixed

            tab._apply_collapsed_state(group, content, True)
            app.processEvents()

            assert content.isHidden() is False
            assert group.minimumHeight() == expanded_heights[group]
            assert group.maximumHeight() == 16777215
            assert group.sizePolicy().verticalPolicy() == QSizePolicy.Preferred
    finally:
        tab.deleteLater()
        app.processEvents()


def test_radio_profile_dashboard_sections_visual_geometry_and_collapse_defaults(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)

    tab = SettingsTab()
    try:
        sections = [
            (tab.radio_profile_identity_section, True),
            (tab.radio_profile_software_stack_section, True),
            (tab.radio_profile_stack_guidance_section, False),
            (tab.radio_profile_connection_section, False),
            (tab.radio_profile_frequency_section, False),
            (tab.radio_profile_optional_section, False),
            (tab.radio_profile_inventory_section, False),
            (tab.radio_profile_readiness_section, True),
            (tab.radio_profile_actions_section, False),
        ]

        assert tab.radio_profile_section_group.sizePolicy().verticalPolicy() == QSizePolicy.Expanding
        assert tab.sections_scroll.widgetResizable() is True

        for section, checked_by_default in sections:
            content = section.layout().itemAt(0).widget()
            assert content is not None
            assert section.isCheckable() is True
            assert section.isChecked() is checked_by_default
            assert content.isHidden() is (not checked_by_default)
            assert section.toolTip() == f"Show or hide the {section.title()} section."
            assert section.accessibleName() == f"{section.title()} section"
            assert section.sizePolicy().verticalPolicy() == QSizePolicy.Preferred
            assert section.layout().contentsMargins().top() == 10
            assert section.layout().contentsMargins().bottom() == 12
            assert section.layout().spacing() == 6

        assert tab.device_profile_detail_card.frameShape() == QFrame.StyledPanel
        assert tab.device_profile_readiness_card.frameShape() == QFrame.StyledPanel
        assert not [
            frame
            for frame in tab.radio_profile_connection_section.findChildren(QFrame)
            if frame.frameShape() == QFrame.StyledPanel
        ]

        collapsed = tab.radio_profile_connection_section
        collapsed_content = collapsed.layout().itemAt(0).widget()
        collapsed_height = collapsed.sizeHint().height()
        collapsed.setChecked(True)
        app.processEvents()

        assert collapsed_content.isHidden() is False
        assert collapsed.sizeHint().height() > collapsed_height

        expanded_heights = []
        for section, _checked_by_default in sections:
            section.setVisible(True)
            section.setChecked(True)
            app.processEvents()
            content = section.layout().itemAt(0).widget()
            assert content.isHidden() is False
            expanded_heights.append(section.sizeHint().height())

        assert all(height > 0 for height in expanded_heights)
        assert sum(expanded_heights) > tab.sections_scroll.viewport().height()
        assert tab.radio_profile_section_group.maximumHeight() == 16777215
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_logging_diagnostics_panel_is_compact_and_themed() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    build_block = source[
        source.index("log_warn_tip = (")
        : source.index("left_widget = QWidget()")
    ]
    helper_block = source[
        source.index("def _make_compact_settings_panel(")
        : source.index("def _sync_device_profiles_table_to_settings_focus")
    ]
    theme_block = source[
        source.index("if hasattr(self, \"open_logs_btn\"):")
        : source.index("if hasattr(self, \"sections_nav_list\"):")
    ]

    assert "self.logging_group, logging_group_layout = self._make_compact_settings_panel(" in build_block
    assert 'object_name="settingsLoggingPanel"' in build_block
    assert 'accessible_name="Logging and diagnostics settings"' in build_block
    assert "maximum_width=760" in build_block
    assert "panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)" in helper_block
    assert "panel.setMaximumWidth(int(maximum_width))" in helper_block
    assert "layout.setContentsMargins(10, 10, 10, 10)" in helper_block
    assert "layout.setAlignment(Qt.AlignTop | Qt.AlignLeft)" in helper_block
    assert "self.logging_warning_label.setMaximumWidth(720)" in build_block
    assert 'self.log_level_label = QLabel("Logging Level:")' in build_block
    assert "self._fit_combo_to_contents(self.log_level_combo, minimum=140, maximum=260)" in build_block
    assert "self._fit_combo_to_contents(self.debug_duration_combo, minimum=110, maximum=220)" in build_block
    assert "self.log_level_combo.setMaximumWidth" not in build_block
    assert "self.debug_duration_combo.setMaximumWidth" not in build_block
    assert "self.logging_actions_grid.setContentsMargins(0, 0, 0, 0)" in build_block
    assert 'self.open_logs_btn.setAccessibleName("Open logs")' in build_block
    assert 'self.open_log_folder_btn.setAccessibleName("Open log folder")' in build_block
    assert 'self.export_diag_btn.setAccessibleName("Export diagnostics")' in build_block
    assert "def _logging_action_layout_mode" in source
    assert "layout_mode = self._logging_action_layout_mode(width)" in source
    assert "Keep diagnostics actions grouped left" in source
    assert "QWidget#settingsLoggingPanel" in theme_block
    assert "border-radius: 6px;" in theme_block


def test_settings_logging_action_layout_mode_documents_left_grouping() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    layout_block = source[
        source.index("def _update_logging_actions_layout")
        : source.index("def _apply_accessibility_width_guards")
    ]

    assert SettingsTab._logging_action_layout_mode(0) == "very_compact"
    assert SettingsTab._logging_action_layout_mode(479) == "very_compact"
    assert SettingsTab._logging_action_layout_mode(480) == "compact"
    assert SettingsTab._logging_action_layout_mode(639) == "compact"
    assert SettingsTab._logging_action_layout_mode(640) == "standard"
    assert "self.logging_actions_grid.setColumnStretch(3, 1)" in layout_block
    assert "self.logging_actions_grid.addWidget(self.export_diag_btn, 1, 0, 1, 2)" in layout_block
    assert "self.logging_actions_grid.addWidget(self.export_diag_btn, 0, 2)" in layout_block


def test_settings_logging_actions_visual_geometry_wraps_under_500px() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    panel = QWidget()
    grid = QGridLayout()
    panel.setLayout(grid)
    fake_tab = types.SimpleNamespace(
        logging_group=panel,
        logging_actions_grid=grid,
        open_logs_btn=QPushButton("Open Logs"),
        open_log_folder_btn=QPushButton("Open Log Folder"),
        export_diag_btn=QPushButton("Export Diagnostics"),
        _logging_action_layout_mode=SettingsTab._logging_action_layout_mode,
    )

    def item_position(widget: QWidget) -> tuple[int, int, int, int]:
        index = grid.indexOf(widget)
        assert index >= 0
        return grid.getItemPosition(index)

    try:
        panel.resize(460, 120)
        SettingsTab._update_logging_actions_layout(fake_tab)
        app.processEvents()

        assert item_position(fake_tab.open_logs_btn) == (0, 0, 1, 2)
        assert item_position(fake_tab.open_log_folder_btn) == (1, 0, 1, 1)
        assert item_position(fake_tab.export_diag_btn) == (1, 1, 1, 1)
        assert grid.columnStretch(3) == 1

        panel.resize(520, 120)
        SettingsTab._update_logging_actions_layout(fake_tab)
        app.processEvents()

        assert item_position(fake_tab.open_logs_btn) == (0, 0, 1, 1)
        assert item_position(fake_tab.open_log_folder_btn) == (0, 1, 1, 1)
        assert item_position(fake_tab.export_diag_btn) == (1, 0, 1, 2)

        panel.resize(700, 120)
        SettingsTab._update_logging_actions_layout(fake_tab)
        app.processEvents()

        assert item_position(fake_tab.open_logs_btn) == (0, 0, 1, 1)
        assert item_position(fake_tab.open_log_folder_btn) == (0, 1, 1, 1)
        assert item_position(fake_tab.export_diag_btn) == (0, 2, 1, 1)
    finally:
        panel.deleteLater()
        app.processEvents()


def test_make_compact_settings_panel_applies_shared_panel_layout() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    _ = app

    panel, layout = SettingsTab._make_compact_settings_panel(
        object_name="settingsTestPanel",
        accessible_name="Test panel",
        tooltip="Panel tooltip",
        maximum_width=640,
    )

    assert panel.objectName() == "settingsTestPanel"
    assert panel.accessibleName() == "Test panel"
    assert panel.toolTip() == "Panel tooltip"
    assert panel.maximumWidth() == 640
    assert panel.sizePolicy().horizontalPolicy() == QSizePolicy.Preferred
    assert panel.sizePolicy().verticalPolicy() == QSizePolicy.Fixed
    assert panel.layout() is layout
    assert layout.contentsMargins().left() == 10
    assert layout.horizontalSpacing() == 10
    assert layout.verticalSpacing() == 8


def test_settings_combo_fit_uses_font_metrics_for_longest_item() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    combo = QComboBox()
    combo.addItems(["A", "Longest Visible Combo Item"])

    SettingsTab._fit_combo_to_contents(combo, minimum=40, maximum=400)

    expected = combo.fontMetrics().horizontalAdvance("Longest Visible Combo Item") + 56
    assert combo.sizeAdjustPolicy() == QComboBox.AdjustToContents
    assert combo.minimumWidth() >= expected


def test_settings_action_feedback_helper_publishes_settings_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None

    tab._publish_settings_action_feedback(
        status="succeeded",
        summary="Saved settings for DX10.",
        radio_profile_id="7",
        target_label="DX10",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "succeeded"
    assert events[0].summary == "Saved settings for DX10."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "settings"
    assert tab._last_action_feedback_event == events[0]


def test_settings_save_success_uses_feedback_instead_of_success_popup() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    save_block = source[source.index("def _save_settings(") : source.index("def _on_theme_changed")]

    assert "Settings saved." in save_block
    assert "_publish_settings_action_feedback(" in save_block
    assert 'QMessageBox.information(self, "Settings", "Settings saved.")' not in save_block


def test_settings_prompt_interval_validation_uses_blocked_feedback() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    save_start = source.index("def _save_settings(")
    save_block = source[save_start : source.index("data[\"freq_enforcement_mode\"]", save_start)]

    assert "Save blocked: select" in save_block
    assert "_block_settings_action(" in save_block
    old_prompt_modal = 'QMessageBox.warning(self, "Settings", f"Please select: {\', \'.join(missing)}.")'
    assert old_prompt_modal not in save_block


def test_settings_blocked_feedback_helper_publishes_blocked_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: (None, "Settings")

    tab._block_settings_action(
        "Save blocked: select Frequency Prompt Interval.",
        "Choose a prompt interval before saving.",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "blocked"
    assert events[0].action_type == "save"
    assert events[0].summary == "Save blocked: select Frequency Prompt Interval."
    assert events[0].detail == "Choose a prompt interval before saving."
    assert events[0].source_surface == "settings"


def test_settings_launch_control_feedback_helper_publishes_history_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._publish_launch_control_feedback(
        status="in_progress",
        summary="Launch sequence started.",
        detail="FreqInOut is starting the selected configured applications.",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "launch_control"
    assert events[0].status == "in_progress"
    assert events[0].summary == "Launch sequence started."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "settings"


def test_settings_autofill_feedback_helper_publishes_configure_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._publish_autofill_feedback(
        status="partial",
        summary="Auto-fill updated JS8Call: Filled 1 field(s). Not found: 1.",
        detail="JS8Call DIRECTED.TXT: filled /tmp/DIRECTED.TXT",
        section="js8",
        operation="result",
    )

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "configure_automatically"
    assert events[0].status == "partial"
    assert events[0].summary.startswith("Auto-fill updated JS8Call")
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "settings.configure_automatically.js8.result"


def test_settings_multirig_autoconfig_preview_is_in_card_and_non_destructive() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    build_block = source[
        source.index("self.multi_rig_status_card = QFrame()")
        : source.index("self.device_profile_detail_card = QFrame()")
    ]
    preview_block = source[
        source.index("def _preview_multi_rig_autoconfiguration")
        : source.index("def _multi_rig_radio_catalog")
    ]

    assert 'self.multi_rig_preview_autoconfig_btn = QPushButton("Preview Configure Automatically")' in build_block
    assert 'self.multi_rig_autoconfig_preview_label.setObjectName("multiRigAutoconfigPreview")' in build_block
    assert "build_single_rig_upgrade_preview(" in preview_block
    assert "build_autoconfig_proposal(" in preview_block
    assert "extra_app_paths=self._multi_rig_autoconfig_extra_app_paths(settings_values)" in preview_block
    assert "ensure_multi_rig_migration(" not in preview_block
    assert "create_config_backup(" not in preview_block
    assert 'source_surface="settings.configure_automatically.multirig.preview"' in preview_block


def test_settings_multirig_autoconfig_preview_text_is_compact() -> None:
    from dataclasses import dataclass

    from freqinout.gui.settings_tab import SettingsTab

    @dataclass(frozen=True)
    class FakeAssignment:
        service: str
        assigned_port: int
        protocol: str = "tcp"

    @dataclass(frozen=True)
    class FakeRadio:
        ports: tuple

    @dataclass(frozen=True)
    class FakeCandidate:
        display_name: str
        executable: bool = True

    @dataclass(frozen=True)
    class FakeUpgradePreview:
        summary: str
        backup_paths: tuple
        referenced_paths_not_backed_up: tuple
        warnings: tuple

    @dataclass(frozen=True)
    class FakeDiscoveryProposal:
        candidates: tuple
        radios: tuple
        warnings: tuple

    summary, detail = SettingsTab._multi_rig_autoconfig_preview_text(
        FakeUpgradePreview(
            summary="FIO will create first radio 'Default Radio' using FLRig.",
            backup_paths=("/tmp/fio",),
            referenced_paths_not_backed_up=("/tmp/messages",),
            warnings=("VarAC will remain disabled unless the operator enables it.",),
        ),
        FakeDiscoveryProposal(
            candidates=(FakeCandidate("FLRig"), FakeCandidate("JS8Call"), FakeCandidate("Broken", False)),
            radios=(FakeRadio((FakeAssignment("flrig", 12345), FakeAssignment("js8call_udp", 2242, "udp"))),),
            warnings=("FIO could not find FLDigi.",),
        ),
    )

    assert summary == "FIO will create first radio 'Default Radio' using FLRig."
    assert "Apps found: FLRig, JS8Call." in detail
    assert "Suggested ports: FLRIG 12345." in detail
    assert "Backup preview: 1 config path(s)." in detail
    assert "Referenced data folders not copied by upgrade backup: 1." in detail
    assert "Review: VarAC will remain disabled" in detail


def test_settings_multirig_autoconfig_preview_uses_current_path_hints() -> None:
    from pathlib import Path

    from freqinout.gui.settings_tab import SettingsTab

    hints = SettingsTab._multi_rig_autoconfig_extra_app_paths(
        {
            "path_flrig": "/Applications/Custom/FLRig.app",
            "path_fldigi": "",
            "path_js8call": "/opt/js8call/js8call",
            "varac_path": "/Users/example/.wine/drive_c/VarAC",
        }
    )

    assert hints == (
        Path("/Applications/Custom/FLRig.app"),
        Path("/opt/js8call/js8call"),
        Path("/Users/example/.wine/drive_c/VarAC"),
    )


def test_settings_multirig_autoconfig_preview_button_updates_label_and_feedback(monkeypatch, tmp_path) -> None:
    import types

    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self, force=False: None)
    monkeypatch.setattr(SettingsTab, "_settings_snapshot_for_readiness", lambda self: {"control_via": "FLRig"})
    monkeypatch.setattr(
        settings_tab_module,
        "build_single_rig_upgrade_preview",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            summary="FIO will create first radio 'Default Radio' using FLRig.",
            backup_paths=("/tmp/fio",),
            referenced_paths_not_backed_up=(),
            warnings=(),
        ),
    )
    monkeypatch.setattr(
        settings_tab_module,
        "build_autoconfig_proposal",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            candidates=(types.SimpleNamespace(display_name="FLRig", executable=True),),
            radios=(
                types.SimpleNamespace(
                    ports=(
                        types.SimpleNamespace(service="flrig", assigned_port=12345, protocol="tcp"),
                    )
                ),
            ),
            warnings=(),
            missing_apps=(),
        ),
    )

    app = QApplication.instance() or QApplication([])
    tab = SettingsTab()
    try:
        tab.multi_rig_preview_autoconfig_btn.click()
        app.processEvents()

        assert tab.multi_rig_autoconfig_preview_label.isHidden() is False
        assert "FIO will create first radio" in tab.multi_rig_autoconfig_preview_label.text()
        assert "Apps found: FLRig." in tab.multi_rig_autoconfig_preview_label.text()
        events = tab.action_feedback_service.recent(scope="settings")
        assert events[0].action_type == "configure_automatically"
        assert events[0].status == "succeeded"
        assert events[0].source_surface == "settings.configure_automatically.multirig.preview"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_multirig_setup_apply_is_backup_backed_before_migration() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    setup_block = source[
        source.index("def _start_multi_rig_setup")
        : source.index("def _update_device_profile_readiness_detail(")
    ]

    assert "build_single_rig_upgrade_apply_plan(" in setup_block
    assert "create_config_backup(apply_plan.backup_paths, reason=apply_plan.backup_reason)" in setup_block
    assert setup_block.index("create_config_backup(") < setup_block.index("ensure_multi_rig_migration(")
    assert "if not apply_plan.can_apply:" in setup_block
    assert "Primary FIO configuration backup did not complete." in setup_block
    assert 'source_surface="settings.configure_automatically.multirig.apply"' in setup_block
    post_migration_block = setup_block[setup_block.index("ensure_multi_rig_migration(") :]
    assert "QMessageBox.information(" not in post_migration_block


def test_settings_multirig_setup_apply_blocks_when_backup_item_fails(monkeypatch) -> None:
    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.gui.settings_tab import SettingsTab

    QApplication.instance() or QApplication([])
    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: (None, "Settings")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab.multi_rig_autoconfig_preview_label = QLabel("")

    monkeypatch.setattr(
        settings_tab_module,
        "build_single_rig_upgrade_apply_plan",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            can_apply=True,
            backup_paths=("/tmp/fio-settings.json",),
            backup_reason="pre-multirig",
            blockers=(),
        ),
    )
    monkeypatch.setattr(
        settings_tab_module,
        "create_config_backup",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            items=(
                types.SimpleNamespace(
                    status="failed",
                    original_path="/tmp/fio-settings.json",
                    error="copy failed",
                ),
            ),
            backup_dir="/tmp/fio-backup",
            manifest_path="/tmp/fio-backup/manifest.json",
        ),
    )

    migration_called = False

    def _fail_if_migrated(*_args, **_kwargs):
        nonlocal migration_called
        migration_called = True
        raise AssertionError("migration should not run when the primary backup fails")

    monkeypatch.setattr(settings_tab_module, "ensure_multi_rig_migration", _fail_if_migrated)

    result = tab._run_backup_backed_multi_rig_setup_apply(
        migration_settings={"control_via": "FLRig"},
        radio_name="Default Radio",
        radio_manufacturer="Lab",
        radio_model="Radio A",
        operating_plan_name="All Features",
        enabled_software_roles=("flrig",),
    )

    events = service.recent(scope="settings")
    assert result is False
    assert migration_called is False
    assert events[0].status == "blocked"
    assert events[0].summary == "Multi-Rig setup blocked: backup did not complete."
    assert events[0].source_surface == "settings.configure_automatically.multirig.apply"
    assert "Primary FIO configuration backup did not complete." in events[0].detail
    assert "copy failed" in tab.multi_rig_autoconfig_preview_label.text()


def test_settings_multirig_setup_apply_publishes_failed_feedback_when_migration_fails(monkeypatch) -> None:
    import freqinout.gui.settings_tab as settings_tab_module
    from freqinout.gui.settings_tab import SettingsTab

    QApplication.instance() or QApplication([])
    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: (None, "Settings")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab.multi_rig_autoconfig_preview_label = QLabel("")

    class _Connection:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return False

    tab.multi_radio_store = types.SimpleNamespace(connect=lambda: _Connection())
    monkeypatch.setattr(
        settings_tab_module,
        "build_single_rig_upgrade_apply_plan",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            can_apply=True,
            backup_paths=("/tmp/fio-settings.json",),
            backup_reason="pre-multirig",
            blockers=(),
        ),
    )
    monkeypatch.setattr(
        settings_tab_module,
        "create_config_backup",
        lambda *_args, **_kwargs: types.SimpleNamespace(
            items=(types.SimpleNamespace(status="backed_up", original_path="/tmp/fio-settings.json", error=""),),
            backup_dir="/tmp/fio-backup",
            manifest_path="/tmp/fio-backup/manifest.json",
        ),
    )
    monkeypatch.setattr(
        settings_tab_module,
        "ensure_multi_rig_migration",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("database locked")),
    )
    monkeypatch.setattr(settings_tab_module.QMessageBox, "warning", lambda *_args, **_kwargs: None)

    result = tab._run_backup_backed_multi_rig_setup_apply(
        migration_settings={"control_via": "FLRig"},
        radio_name="Default Radio",
        radio_manufacturer="Lab",
        radio_model="Radio A",
        operating_plan_name="All Features",
        enabled_software_roles=("flrig",),
    )

    events = service.recent(scope="settings")
    assert result is False
    assert events[0].status == "failed"
    assert events[0].summary == "Multi-Rig setup failed after backup."
    assert events[0].detail == "database locked"
    assert events[0].source_surface == "settings.configure_automatically.multirig.apply"
    assert "database locked" in tab.multi_rig_autoconfig_preview_label.text()


def test_settings_autofill_feedback_status_and_labels_are_clear() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._autofill_section_label("fast_light") == "Fast Light"
    assert SettingsTab._autofill_section_label("js8") == "JS8Call"
    assert SettingsTab._autofill_section_label("varac") == "VarAC"
    assert (
        SettingsTab._autofill_feedback_status(filled_count=1, preserved_count=0, missing_count=0)
        == "succeeded"
    )
    assert (
        SettingsTab._autofill_feedback_status(filled_count=1, preserved_count=0, missing_count=1)
        == "partial"
    )
    assert (
        SettingsTab._autofill_feedback_status(filled_count=0, preserved_count=0, missing_count=1)
        == "blocked"
    )
    assert SettingsTab._autofill_health_key("js8") == "js8call"
    assert SettingsTab._autofill_health_key("fast_light") == "fast_light"
    assert (
        SettingsTab._autofill_feedback_source_surface("js8", "scan")
        == "settings.configure_automatically.js8.scan"
    )
    assert (
        SettingsTab._autofill_feedback_source_surface("fast light", "replace suggestions")
        == "settings.configure_automatically.fast_light.replace_suggestions"
    )
    assert (
        SettingsTab._autofill_feedback_source_surface("unknown", "")
        == "settings.configure_automatically.general.event"
    )
    assert (
        SettingsTab._autofill_readiness_summary("JS8Call", "JS8Call DIRECTED.TXT path missing; Forms path missing")
        == "Auto-fill needs review in JS8Call: JS8Call DIRECTED.TXT path missing."
    )
    assert SettingsTab._autofill_visible_review_text("No auto-fill changes were available.", []) == (
        "No auto-fill changes were available."
    )
    review_text = SettingsTab._autofill_visible_review_text(
        "Filled 1 field(s). Preserved 1 existing field(s). Not found: 1.",
        [
            "JS8Call install folder: filled /Applications/JS8Call.app (verified) - Found macOS app bundle",
            "DIRECTED.TXT: kept existing value; suggested /tmp/DIRECTED.TXT (high) - Found profile file",
            "JS8 Forms path: not found - No forms directory found",
            "JS8Spotter launch path: not found - No installed app found",
            "CommStat launch path: not found - No installed app found",
        ],
    )

    assert review_text.startswith("Filled 1 field(s). Preserved 1 existing field(s). Not found: 1.")
    assert "Review suggestions:" in review_text
    assert "- JS8Call install folder: filled /Applications/JS8Call.app" in review_text
    assert "- DIRECTED.TXT: kept existing value; suggested /tmp/DIRECTED.TXT" in review_text
    assert "- 1 more item" in review_text
    assert "CommStat launch path: not found" not in review_text
    assert "- 2 more items" in SettingsTab._autofill_visible_review_text(
        "Filled 1 field(s).",
        ["One", "Two", "Three"],
        max_lines=1,
    )
    suggestions_text = SettingsTab._autofill_preserved_suggestions_text(
        "JS8Call",
        [
            {
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ],
    )
    assert suggestions_text.startswith(
        "JS8Call preserved 1 existing field(s) with suggested replacement value(s):"
    )
    assert "- DIRECTED.TXT: keep /manual/DIRECTED.TXT; suggested /detected/DIRECTED.TXT (high)" in suggestions_text
    assert SettingsTab._autofill_preserved_copy_summary("JS8Call", 1) == (
        "Copied 1 preserved JS8Call Auto-Fill suggestion."
    )
    assert SettingsTab._autofill_preserved_copy_summary("Fast Light", 2) == (
        "Copied 2 preserved Fast Light Auto-Fill suggestions."
    )
    assert SettingsTab._autofill_dismiss_summary("JS8Call", 1) == (
        "Dismissed 1 preserved JS8Call Auto-Fill suggestion."
    )
    assert SettingsTab._autofill_dismiss_summary("Fast Light", 2) == (
        "Dismissed 2 preserved Fast Light Auto-Fill suggestions."
    )
    assert SettingsTab._autofill_suggestion_row_values(
        {
            "label": "DIRECTED.TXT",
            "current": "/manual/DIRECTED.TXT",
            "suggested": "/detected/DIRECTED.TXT",
            "confidence": "high",
            "reason": "Found profile file",
        }
    ) == (
        "DIRECTED.TXT",
        "/manual/DIRECTED.TXT",
        "/detected/DIRECTED.TXT",
        "high",
        "Found profile file",
    )


def test_settings_autofill_readiness_feedback_publishes_only_when_warned() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab._build_section_health_snapshot = lambda: {
        "js8call": {"state": "warn", "detail": "JS8Call DIRECTED.TXT path missing; Forms path missing"}
    }

    tab._publish_autofill_readiness_feedback("js8")

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "configure_automatically"
    assert events[0].status == "partial"
    assert events[0].summary == "Auto-fill needs review in JS8Call: JS8Call DIRECTED.TXT path missing."
    assert "Forms path missing" in events[0].detail
    assert events[0].radio_profile_id == "7"
    assert events[0].source_surface == "settings.configure_automatically.js8.readiness"


def test_settings_autofill_readiness_feedback_skips_when_section_is_ok() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab._build_section_health_snapshot = lambda: {
        "js8call": {"state": "ok", "detail": ""}
    }

    tab._publish_autofill_readiness_feedback("js8")

    assert service.recent(scope="settings") == []


def test_settings_save_guardrail_feedback_publishes_partial_warning_event() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    tab._current_multi_rig_guardrail_messages = lambda: (
        "Duplicate JS8Call API endpoint 127.0.0.1:2442 on active radios: DX10, Portable.",
        "Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362 on active radios: DX10, Portable.",
    )

    tab._publish_save_guardrail_feedback()

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "save_guardrails"
    assert events[0].status == "partial"
    assert events[0].summary == "Settings saved, but 2 multi-rig guardrail warnings need review."
    assert "Duplicate JS8Call API endpoint" in events[0].detail
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"


def test_settings_save_guardrail_feedback_skips_when_no_warnings() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._current_multi_rig_guardrail_messages = lambda: ()

    tab._publish_save_guardrail_feedback()

    assert service.recent(scope="settings") == []
    assert SettingsTab._save_guardrail_feedback_summary(1) == (
        "Settings saved, but 1 multi-rig guardrail warning needs review."
    )


def test_settings_save_guardrail_collection_failure_publishes_failed_feedback(monkeypatch) -> None:
    from freqinout.gui import settings_tab as settings_tab_mod
    from freqinout.gui.settings_tab import SettingsTab

    class Store:
        db_path = "/tmp/fio-missing-settings.db"

    def fail_connect(_path):
        raise OSError("database unavailable")

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab.multi_radio_store = Store()
    tab._last_action_feedback_event = None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    monkeypatch.setattr(settings_tab_mod.sqlite3, "connect", fail_connect)

    assert tab._current_multi_rig_guardrail_messages() == ()
    tab._publish_save_guardrail_feedback()

    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "save_guardrails"
    assert events[0].status == "failed"
    assert events[0].summary == "Settings saved, but multi-rig guardrail checking failed."
    assert events[0].detail == "database unavailable"
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"


def test_settings_guardrail_readiness_status_text_is_compact() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._guardrail_readiness_status_text(()) == ""
    assert SettingsTab._guardrail_readiness_status_text(("Duplicate JS8Call API endpoint 127.0.0.1:2442.",)) == (
        "Multi-rig guardrails: 1 persisted warning needs review.\n"
        "- Duplicate JS8Call API endpoint 127.0.0.1:2442."
    )
    text = SettingsTab._guardrail_readiness_status_text(
        (
            "Duplicate JS8Call API endpoint.",
            "Duplicate FLDigi XML-RPC endpoint.",
            "Duplicate VarAC path.",
            "Duplicate FLRig endpoint.",
        )
    )

    assert text.startswith("Multi-rig guardrails: 4 persisted warnings need review.")
    assert "- Duplicate JS8Call API endpoint." in text
    assert "- Duplicate VarAC path." in text
    assert "- 1 more warning(s)" in text
    assert "Duplicate FLRig endpoint" not in text


def test_settings_guardrail_warnings_are_visible_in_readiness_card_source() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail(") : source.index("def _set_guidance_card_state")
    ]

    assert "self.device_profile_guardrail_status_label = QLabel(\"\")" in source
    assert "deviceProfileGuardrailStatus" in source
    assert 'self.review_guardrail_conflicts_btn = QPushButton("Review Conflicts")' in source
    assert "self.review_guardrail_conflicts_btn.clicked.connect(self._review_device_profile_guardrail_conflicts)" in source
    assert "guardrail_warnings = self._current_multi_rig_guardrail_messages()" in readiness_block
    assert "has_guardrail_warnings = self._set_device_profile_guardrail_status(guardrail_warnings)" in readiness_block
    assert "self._set_device_profile_guardrail_status(())" in readiness_block
    assert '"warning" if has_guardrail_warnings else "success"' in readiness_block


def test_settings_guardrail_review_affordance_is_non_disruptive() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    copy_block = source[
        source.index("def _copy_device_profile_guardrail_warnings(") : source.index("def _launch_sequence_feedback_status")
    ]
    review_block = source[
        source.index("def _review_device_profile_guardrail_conflicts(") : source.index("def _guardrail_copy_summary")
    ]

    assert 'self.copy_guardrail_summary_btn = QPushButton("Copy Guardrails")' in source
    assert "self.copy_guardrail_summary_btn.setVisible(False)" in source
    assert "self.copy_guardrail_summary_btn.clicked.connect(self._copy_device_profile_guardrail_warnings)" in source
    assert "review_button.setVisible(bool(text))" in source
    assert "review_button.setEnabled(bool(text))" in source
    assert "button.setVisible(bool(text))" in source
    assert "button.setEnabled(bool(text))" in source
    assert 'action_type="copy_guardrails"' in copy_block
    assert "self._guardrail_copy_text(warnings, radio_profile_id=radio_id, target_label=target)" in copy_block
    assert "QMessageBox" not in copy_block
    assert 'action_type="review_guardrails"' in review_block
    assert "QMessageBox" not in review_block
    assert SettingsTab._guardrail_copy_summary(1) == "Copied 1 multi-rig guardrail warning."
    assert SettingsTab._guardrail_copy_summary(2) == "Copied 2 multi-rig guardrail warnings."
    assert "No multi-rig guardrail warnings to copy." in source


def test_settings_guardrail_copy_text_includes_support_context() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._guardrail_copy_text(
        (
            "Duplicate JS8Call API endpoint 127.0.0.1:2442.",
            "Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362.",
        ),
        radio_profile_id="7",
        target_label="DX10",
        timestamp_utc="2026-07-24T12:34:56Z",
    ) == (
        "FIO multi-rig guardrail warnings\n"
        "Copied UTC: 2026-07-24T12:34:56Z\n"
        "Radio context: DX10 (radio id 7)\n"
        "Warnings:\n"
        "- Duplicate JS8Call API endpoint 127.0.0.1:2442.\n"
        "- Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362."
    )
    assert SettingsTab._guardrail_copy_text((), timestamp_utc="2026-07-24T12:34:56Z") == ""


def test_settings_copy_guardrail_warnings_copies_text_and_publishes_feedback(monkeypatch) -> None:
    from freqinout.gui import settings_tab as settings_tab_mod
    from freqinout.gui.settings_tab import SettingsTab

    class Clipboard:
        def __init__(self) -> None:
            self.text = ""

        def setText(self, text: str) -> None:
            self.text = text

    clipboard = Clipboard()
    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._last_device_profile_guardrail_warnings = (
        "Duplicate JS8Call API endpoint 127.0.0.1:2442.",
        "Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362.",
    )
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    monkeypatch.setattr(settings_tab_mod.QApplication, "clipboard", lambda: clipboard)
    monkeypatch.setattr(
        SettingsTab,
        "_guardrail_copy_text",
        staticmethod(
            lambda warnings, **kwargs: (
                "FIO multi-rig guardrail warnings\n"
                "Copied UTC: 2026-07-24T12:34:56Z\n"
                f"Radio context: {kwargs.get('target_label')} (radio id {kwargs.get('radio_profile_id')})\n"
                "Warnings:\n"
                + "\n".join(f"- {item}" for item in warnings)
            )
        ),
    )

    tab._copy_device_profile_guardrail_warnings()

    assert clipboard.text == (
        "FIO multi-rig guardrail warnings\n"
        "Copied UTC: 2026-07-24T12:34:56Z\n"
        "Radio context: DX10 (radio id 7)\n"
        "Warnings:\n"
        "- Duplicate JS8Call API endpoint 127.0.0.1:2442.\n"
        "- Duplicate FLDigi XML-RPC endpoint 127.0.0.1:7362."
    )
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "copy_guardrails"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Copied 2 multi-rig guardrail warnings."
    assert events[0].detail == clipboard.text
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"


def test_settings_guardrail_review_rows_include_targets_and_affected_radios() -> None:
    from freqinout.core.multi_rig_guardrails import MultiRigGuardrailWarning
    from freqinout.gui.settings_tab import SettingsTab

    rows = SettingsTab._guardrail_review_rows(
        (
            MultiRigGuardrailWarning(
                warning_type="duplicate_js8_endpoint",
                resource_type="JS8Call API endpoint",
                resource_value="127.0.0.1:2442",
                affected_radio_ids=(7, 8),
                affected_radio_names=("DX10", "Field"),
            ),
            MultiRigGuardrailWarning(
                warning_type="duplicate_varac_db_path",
                resource_type="VarAC database path",
                resource_value="/varac/shared/varac.db",
                affected_radio_ids=(9,),
                affected_radio_names=("VarAC Node",),
            ),
        )
    )

    assert rows[0]["message"] == "Duplicate JS8Call API endpoint 127.0.0.1:2442 on active radios: DX10, Field."
    assert rows[0]["affected_radio_ids"] == (7, 8)
    assert rows[0]["affected_radio_names"] == ("DX10", "Field")
    assert rows[0]["target_attr"] == "js8_section_group"
    assert rows[1]["target_attr"] == "varac_section_group"
    assert SettingsTab._guardrail_warning_target_attr("duplicate_flrig_endpoint") == "fast_light_section_group"
    assert SettingsTab._guardrail_warning_target_attr("unknown") == "radio_profile_section_group"


def test_settings_guardrail_conflict_focus_selects_radio_and_target_section() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    _ = app
    target_group = QGroupBox("JS8Call Settings")
    target_group.setCheckable(True)
    tab = SettingsTab.__new__(SettingsTab)
    tab.js8_section_group = target_group
    focused = []
    selected = []
    tab.focus_radio_profile = lambda radio_id: focused.append(radio_id) or True
    tab._select_settings_section_group = lambda group: selected.append(group)

    try:
        assert SettingsTab._focus_guardrail_conflict(tab, 7, "js8_section_group") is True
        assert focused == [7]
        assert selected == [target_group]
        assert target_group.isChecked() is True
    finally:
        target_group.deleteLater()
        app.processEvents()


def test_settings_contextual_autofill_publishes_scan_and_result_feedback() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    attempt_block = source[source.index("def _attempt_scoped_autofill") : source.index("def _refresh_contextual_autofill_buttons")]
    apply_block = source[source.index("def _apply_autofill_results") : source.index("def _set_autofill_status")]

    assert "_publish_autofill_feedback(" in attempt_block
    assert 'summary=f"Auto-fill scanning {section_label}."' in attempt_block
    assert "_publish_autofill_feedback(" in apply_block
    assert "_autofill_feedback_status(" in apply_block
    assert "_autofill_visible_review_text(summary, detail_lines)" in apply_block
    assert "full_status = self._autofill_visible_review_text(summary, detail_lines, max_lines=len(detail_lines))" in apply_block
    assert "self._set_autofill_status(section, compact_status, detail, full_text=full_status)" in apply_block
    assert "preserved_suggestions.append(" in apply_block
    assert "self._set_autofill_preserved_suggestions(section, preserved_suggestions)" in apply_block
    assert "_publish_autofill_readiness_feedback(section)" in apply_block
    assert 'action_type="configure_automatically"' in source
    assert "QMessageBox" not in attempt_block
    assert "QMessageBox" not in apply_block


def test_settings_autofill_full_review_toggle_is_in_panel() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    setter_block = source[source.index("def _set_autofill_status") : source.index("def _autofill_target_edit")]

    assert 'self.js8_autofill_review_toggle_btn = QPushButton("Show Full Review")' in source
    assert 'self.fast_light_autofill_review_toggle_btn = QPushButton("Show Full Review")' in source
    assert 'self.varac_autofill_review_toggle_btn = QPushButton("Show Full Review")' in source
    assert "self.js8_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)" in source
    assert "self.fast_light_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)" in source
    assert "self.varac_autofill_status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)" in source
    assert 'self.js8_autofill_review_toggle_btn.clicked.connect(lambda _checked=False: self._toggle_autofill_review("js8"))' in source
    assert 'self.fast_light_autofill_review_toggle_btn.clicked.connect(' in source
    assert 'lambda _checked=False: self._toggle_autofill_review("fast_light")' in source
    assert 'lambda _checked=False: self._toggle_autofill_review("varac")' in source
    assert "self._autofill_compact_status_texts[section] = compact_text" in setter_block
    assert "self._autofill_full_status_texts[section] = expanded_text" in setter_block
    assert 'button.setText("Show Full Review")' in setter_block
    assert 'button.setText("Show Less" if expanded else "Show Full Review")' in setter_block
    assert "QMessageBox" not in setter_block


def test_settings_autofill_preserved_suggestions_copy_is_non_disruptive(monkeypatch) -> None:
    from freqinout.gui import settings_tab as settings_tab_mod
    from freqinout.gui.settings_tab import SettingsTab

    class Clipboard:
        def __init__(self) -> None:
            self.text = ""

        def setText(self, text: str) -> None:
            self.text = text

    clipboard = Clipboard()
    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None
    monkeypatch.setattr(settings_tab_mod.QApplication, "clipboard", lambda: clipboard)

    tab._copy_autofill_preserved_suggestions("js8")

    assert "JS8Call preserved 1 existing field(s)" in clipboard.text
    assert "/manual/DIRECTED.TXT" in clipboard.text
    assert "/detected/DIRECTED.TXT" in clipboard.text
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "copy_autofill_suggestions"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Copied 1 preserved JS8Call Auto-Fill suggestion."
    assert events[0].detail == clipboard.text
    assert events[0].radio_profile_id == "7"
    assert events[0].source_surface == "settings.configure_automatically.js8.copy_suggestions"


def test_settings_autofill_dismiss_suggestions_clears_cache_without_dirtying() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_dismiss_buttons = {}
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._dismiss_autofill_preserved_suggestions("js8")

    assert dirty_calls == []
    assert tab._autofill_preserved_suggestions["js8"] == []
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "dismiss_autofill_suggestions"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Dismissed 1 preserved JS8Call Auto-Fill suggestion."
    assert "DIRECTED.TXT: keep /manual/DIRECTED.TXT; suggested /detected/DIRECTED.TXT" in events[0].detail
    assert events[0].radio_profile_id == "7"
    assert events[0].source_surface == "settings.configure_automatically.js8.dismiss_suggestions"


def test_settings_autofill_dismiss_single_suggestion_clears_one_row_without_dirtying() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            },
            {
                "key": "js8_forms_path",
                "label": "Forms path",
                "current": "/manual/forms",
                "suggested": "/detected/forms",
                "confidence": "high",
                "reason": "Found forms folder",
            },
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_dismiss_buttons = {}
    tab._autofill_review_tables = {}
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._dismiss_autofill_preserved_suggestion("js8", 0)

    assert dirty_calls == []
    assert len(tab._autofill_preserved_suggestions["js8"]) == 1
    assert tab._autofill_preserved_suggestions["js8"][0]["key"] == "js8_forms_path"
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "dismiss_autofill_suggestion"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Dismissed DIRECTED.TXT Auto-Fill suggestion for JS8Call."
    assert "DIRECTED.TXT: keep /manual/DIRECTED.TXT; suggested /detected/DIRECTED.TXT" in events[0].detail
    assert events[0].source_surface == "settings.configure_automatically.js8.dismiss_suggestion"


def test_settings_autofill_replace_suggestions_updates_cached_fields() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Edit:
        def __init__(self, value: str) -> None:
            self.value = value

        def text(self) -> str:
            return self.value

        def setText(self, value: str) -> None:
            self.value = value

    service = ActionFeedbackService()
    edit = Edit("/manual/DIRECTED.TXT")
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_target_edit = lambda key: edit if key == "js8_directed_path" else None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._refresh_section_titles = lambda: None
    tab._refresh_section_nav_health = lambda: None
    tab._refresh_contextual_autofill_buttons = lambda: None
    tab._publish_autofill_readiness_feedback = lambda _section: None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestions("js8")

    assert edit.text() == "/detected/DIRECTED.TXT"
    assert dirty_calls == ["dirty"]
    assert tab._autofill_preserved_suggestions["js8"] == []
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "replace_autofill_suggestions"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Replaced 1 JS8Call Auto-Fill suggestion."
    assert "DIRECTED.TXT: replaced /manual/DIRECTED.TXT with /detected/DIRECTED.TXT" in events[0].detail
    assert events[0].radio_profile_id == "7"
    assert events[0].source_surface == "settings.configure_automatically.js8.replace_suggestions"


def test_settings_autofill_replace_single_suggestion_updates_one_field() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Edit:
        def __init__(self, value: str) -> None:
            self.value = value

        def text(self) -> str:
            return self.value

        def setText(self, value: str) -> None:
            self.value = value

    service = ActionFeedbackService()
    edit = Edit("/manual/DIRECTED.TXT")
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            },
            {
                "key": "js8_forms_path",
                "label": "Forms path",
                "current": "/manual/forms",
                "suggested": "/detected/forms",
                "confidence": "high",
                "reason": "Found forms folder",
            },
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_dismiss_buttons = {}
    tab._autofill_review_tables = {}
    tab._autofill_target_edit = lambda key: edit if key == "js8_directed_path" else None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._refresh_section_titles = lambda: None
    tab._refresh_section_nav_health = lambda: None
    tab._refresh_contextual_autofill_buttons = lambda: None
    tab._publish_autofill_readiness_feedback = lambda _section: None
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestion("js8", 0)

    assert edit.text() == "/detected/DIRECTED.TXT"
    assert dirty_calls == ["dirty"]
    assert len(tab._autofill_preserved_suggestions["js8"]) == 1
    assert tab._autofill_preserved_suggestions["js8"][0]["key"] == "js8_forms_path"
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "replace_autofill_suggestion"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Replaced DIRECTED.TXT Auto-Fill suggestion for JS8Call."
    assert "DIRECTED.TXT: replaced /manual/DIRECTED.TXT with /detected/DIRECTED.TXT" in events[0].detail
    assert events[0].source_surface == "settings.configure_automatically.js8.replace_suggestion"


def test_settings_autofill_replace_single_suggestion_missing_target_reports_partial_and_keeps_row() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "missing_field",
                "label": "Missing Field",
                "current": "/manual",
                "suggested": "/detected",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_target_edit = lambda _key: None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestion("js8", 0)

    assert dirty_calls == []
    assert len(tab._autofill_preserved_suggestions["js8"]) == 1
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "replace_autofill_suggestion"
    assert events[0].status == "partial"
    assert events[0].summary == "Could not replace Missing Field Auto-Fill suggestion."
    assert "Missing Field: no editable target found" in events[0].detail
    assert events[0].source_surface == "settings.configure_automatically.js8.replace_suggestion"


def test_settings_autofill_replace_suggestions_keeps_skipped_items_and_reports_partial() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    service = ActionFeedbackService()
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "missing_field",
                "label": "Missing Field",
                "current": "/manual",
                "suggested": "/detected",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_target_edit = lambda _key: None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestions("js8")

    assert dirty_calls == []
    assert len(tab._autofill_preserved_suggestions["js8"]) == 1
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "partial"
    assert events[0].summary == "Replaced 0 JS8Call Auto-Fill suggestions; skipped 1 suggestion."
    assert "Missing Field: no editable target found" in events[0].detail
    assert events[0].source_surface == "settings.configure_automatically.js8.replace_suggestions"


def test_settings_autofill_replace_suggestions_does_not_dirty_already_matching_field() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Edit:
        def __init__(self, value: str) -> None:
            self.value = value

        def text(self) -> str:
            return self.value

        def setText(self, value: str) -> None:
            self.value = value

    service = ActionFeedbackService()
    edit = Edit("/detected/DIRECTED.TXT")
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = service
    tab._last_action_feedback_event = None
    tab._autofill_preserved_suggestions = {
        "js8": [
            {
                "key": "js8_directed_path",
                "label": "DIRECTED.TXT",
                "current": "/manual/DIRECTED.TXT",
                "suggested": "/detected/DIRECTED.TXT",
                "confidence": "high",
                "reason": "Found profile file",
            }
        ]
    }
    tab._autofill_preserved_buttons = {}
    tab._autofill_replace_buttons = {}
    tab._autofill_target_edit = lambda key: edit if key == "js8_directed_path" else None
    dirty_calls = []
    tab._mark_settings_dirty = lambda: dirty_calls.append("dirty")
    tab._selected_settings_feedback_target = lambda: ("7", "DX10")
    tab._set_settings_action_feedback_status = lambda *_args: None

    tab._replace_autofill_preserved_suggestions("js8")

    assert dirty_calls == []
    assert tab._autofill_preserved_suggestions["js8"] == []
    events = service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].status == "succeeded"
    assert events[0].summary == "Replaced 0 JS8Call Auto-Fill suggestions."
    assert "DIRECTED.TXT: already matched /detected/DIRECTED.TXT" in events[0].detail


def test_settings_autofill_replace_suggestions_is_wired_without_modal() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    replace_block = source[
        source.index("def _replace_autofill_preserved_suggestions")
        : source.index("def _autofill_target_edit")
    ]
    dismiss_block = source[
        source.index("def _dismiss_autofill_preserved_suggestions")
        : source.index("def _autofill_replace_summary")
    ]
    table_block = source[source.index("def _make_autofill_review_table") : source.index("def _detect_autofill_results")]
    refresh_table_block = source[
        source.index("def _refresh_autofill_review_table")
        : source.index("def _autofill_preserved_copy_summary")
    ]

    assert 'self.js8_autofill_replace_btn = QPushButton("Replace Suggested")' in source
    assert 'self.fast_light_autofill_replace_btn = QPushButton("Replace Suggested")' in source
    assert 'self.varac_autofill_replace_btn = QPushButton("Replace Suggested")' in source
    assert 'self.js8_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")' in source
    assert 'self.fast_light_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")' in source
    assert 'self.varac_autofill_dismiss_btn = QPushButton("Dismiss Suggestions")' in source
    assert 'lambda _checked=False: self._replace_autofill_preserved_suggestions("js8")' in source
    assert 'lambda _checked=False: self._replace_autofill_preserved_suggestions("fast_light")' in source
    assert 'lambda _checked=False: self._replace_autofill_preserved_suggestions("varac")' in source
    assert 'lambda _checked=False: self._dismiss_autofill_preserved_suggestions("js8")' in source
    assert 'lambda _checked=False: self._dismiss_autofill_preserved_suggestions("fast_light")' in source
    assert 'lambda _checked=False: self._dismiss_autofill_preserved_suggestions("varac")' in source
    assert 'action_type="replace_autofill_suggestions"' in replace_block
    assert 'action_type="dismiss_autofill_suggestions"' in dismiss_block
    assert 'action_type="replace_autofill_suggestion"' in source
    assert 'action_type="dismiss_autofill_suggestion"' in source
    assert 'js8_v.addWidget(self._make_autofill_review_table("js8"))' in source
    assert 'fast_light_v.addWidget(self._make_autofill_review_table("fast_light"))' in source
    assert 'varac_v.addWidget(self._make_autofill_review_table("varac"))' in source
    assert 'table.setHorizontalHeaderLabels(["Field", "Current", "Suggested", "Confidence", "Reason", "Action"])' in table_block
    assert 'replace_btn = QPushButton("Replace")' in refresh_table_block
    assert 'dismiss_btn = QPushButton("Dismiss")' in refresh_table_block
    assert "_mark_settings_dirty()" in replace_block
    assert "QMessageBox" not in replace_block
    assert "QMessageBox" not in dismiss_block
    assert "QMessageBox" not in table_block
    assert "QMessageBox" not in refresh_table_block


def test_settings_autofill_bulk_actions_are_on_secondary_rows() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    setter_block = source[
        source.index("def _set_autofill_preserved_suggestions")
        : source.index("def _autofill_suggestion_row_values")
    ]

    assert "js8_autofill_actions_widget = QWidget()" in source
    assert "fast_light_autofill_actions_widget = QWidget()" in source
    assert "varac_autofill_actions_widget = QWidget()" in source
    assert "js8_autofill_actions_row = QHBoxLayout()" in source
    assert "fast_light_autofill_actions_row = QHBoxLayout()" in source
    assert "varac_autofill_actions_row = QHBoxLayout()" in source
    assert 'self._autofill_action_rows["js8"] = js8_autofill_actions_widget' in source
    assert 'self._autofill_action_rows["fast_light"] = fast_light_autofill_actions_widget' in source
    assert 'self._autofill_action_rows["varac"] = varac_autofill_actions_widget' in source
    assert "js8_autofill_actions_widget.setVisible(False)" in source
    assert "fast_light_autofill_actions_widget.setVisible(False)" in source
    assert "varac_autofill_actions_widget.setVisible(False)" in source
    assert "js8_autofill_actions_row.addStretch()" in source
    assert "fast_light_autofill_actions_row.addStretch()" in source
    assert "varac_autofill_actions_row.addStretch()" in source
    assert "js8_autofill_actions_row.addWidget(self.js8_autofill_preserved_btn)" in source
    assert "js8_autofill_actions_row.addWidget(self.js8_autofill_replace_btn)" in source
    assert "js8_autofill_actions_row.addWidget(self.js8_autofill_dismiss_btn)" in source
    assert "fast_light_autofill_actions_row.addWidget(self.fast_light_autofill_preserved_btn)" in source
    assert "fast_light_autofill_actions_row.addWidget(self.fast_light_autofill_replace_btn)" in source
    assert "fast_light_autofill_actions_row.addWidget(self.fast_light_autofill_dismiss_btn)" in source
    assert "varac_autofill_actions_row.addWidget(self.varac_autofill_preserved_btn)" in source
    assert "varac_autofill_actions_row.addWidget(self.varac_autofill_replace_btn)" in source
    assert "varac_autofill_actions_row.addWidget(self.varac_autofill_dismiss_btn)" in source
    assert "js8_v.addWidget(js8_autofill_actions_widget)" in source
    assert "fast_light_v.addWidget(fast_light_autofill_actions_widget)" in source
    assert "varac_v.addWidget(varac_autofill_actions_widget)" in source
    assert 'action_row = getattr(self, "_autofill_action_rows", {}).get(section)' in setter_block
    assert "action_row.setVisible(has_suggestions)" in setter_block


def test_settings_save_success_runs_persisted_guardrail_feedback_after_save_event() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    save_block = source[source.index("def _save_settings(") : source.index("def _on_theme_changed")]
    show_message_block = save_block[save_block.index("if show_message:") : save_block.index("# Persist operator grid")]

    assert "_publish_settings_action_feedback(" in show_message_block
    assert "_publish_save_guardrail_feedback()" in show_message_block
    assert show_message_block.index("_publish_settings_action_feedback(") < show_message_block.index(
        "_publish_save_guardrail_feedback()"
    )
    assert 'action_type="save_guardrails"' in source
    assert "multi_rig_guardrail_warnings(conn)" in source
    assert "_save_guardrail_failure_summary()" in source


def test_settings_launch_sequence_feedback_summary_classifies_results() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert (
        SettingsTab._launch_sequence_feedback_status(
            launched=2,
            already_running=1,
            failed=0,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "succeeded"
    )
    assert (
        SettingsTab._launch_sequence_feedback_status(
            launched=1,
            already_running=0,
            failed=1,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "partial"
    )
    assert (
        SettingsTab._launch_sequence_feedback_status(
            launched=0,
            already_running=0,
            failed=1,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "failed"
    )
    assert (
        SettingsTab._launch_sequence_feedback_summary(
            launched=2,
            already_running=1,
            failed=0,
            timeout=0,
            blocked_self=0,
            cancelled=False,
        )
        == "Launch complete: launched 2, already running 1, failed 0, timeout 0, blocked 0."
    )
    detail = SettingsTab._launch_sequence_feedback_detail(
        launched=2,
        already_running=1,
        failed=0,
        timeout=0,
        blocked_self=0,
        cancelled=True,
    )
    assert "Launched: 2" in detail
    assert "Already running: 1" in detail
    assert "Cancelled: Yes" in detail


def test_settings_launch_control_manual_run_uses_feedback_instead_of_info_popups() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    launch_block = source[source.index("def _launch_configured_now") : source.index("def _stop_launch_sequence")]
    finish_block = source[source.index("def _on_launch_sequence_finished") : source.index("# ---------- TIME / TIMEZONE")]

    assert "_publish_launch_control_feedback(" in launch_block
    assert 'QMessageBox.information(self, "Launch Control"' not in launch_block
    assert "_launch_sequence_feedback_status(" in finish_block
    assert "_launch_sequence_feedback_summary(" in finish_block
    assert "_launch_sequence_feedback_detail(" in finish_block
    assert 'QMessageBox.information(\n                self,\n                "Launch Summary"' not in finish_block


def test_settings_human_join_formats_missing_prompt_intervals() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._human_join([]) == ""
    assert SettingsTab._human_join(["Frequency Prompt Interval"]) == "Frequency Prompt Interval"
    assert (
        SettingsTab._human_join(["Frequency Prompt Interval", "JS8 Prompt Interval"])
        == "Frequency Prompt Interval and JS8 Prompt Interval"
    )
    assert (
        SettingsTab._human_join(
            ["Frequency Prompt Interval", "FLDigi Prompt Interval", "JS8 Prompt Interval"]
        )
        == "Frequency Prompt Interval, FLDigi Prompt Interval, and JS8 Prompt Interval"
    )


def test_settings_dirty_tracking_resets_stale_feedback_label() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    dirty_block = source[source.index("def _mark_settings_dirty") : source.index("def _on_sop_export_text_changed")]

    assert '_set_settings_action_feedback_status("in_progress", "Unsaved settings changes.")' in dirty_block


def test_selected_radio_detail_text_uses_compact_dashboard_rows() -> None:
    from freqinout.core.station_readiness import RadioReadinessSummary, ReadinessReport
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    tab._effective_assignment_map = lambda: {
        7: {
            "operating_profile_name": "Evening Net",
            "assignment_state": "active",
        }
    }
    tab._device_radio_model_summary = lambda _profile: "Icom IC-7300"
    tab._device_class_label = lambda _value: "Transceiver"
    tab._device_backend_label = lambda _value: "FLRig"
    tab._device_software_summary = lambda _profile: "FLRig, FLDigi, JS8Call"
    tab._device_endpoint_summary = lambda _profile: "127.0.0.1:12345"
    tab._device_ptt_group_label = lambda _value: "Group A"

    summary = RadioReadinessSummary(
        radio_id=7,
        name="DX10",
        overall_state="ready",
        required_count=0,
        recommended_count=0,
        informational_count=0,
        messages=(),
    )
    report = ReadinessReport(
        overall_state="ready",
        issues=(),
        radio_summaries=(summary,),
        required_count=0,
        recommended_count=0,
        informational_count=0,
        digest="ready",
    )

    detail_text = SettingsTab._selected_radio_detail_text(
        tab,
        {
            "id": 7,
            "name": "DX10",
            "runtime_primary": 1,
            "runtime_active": 1,
            "enabled": 1,
            "device_class": "transceiver",
            "control_backend": "flrig",
            "ptt_group": "A",
            "notes": "Primary HF station",
        },
        report,
    )

    assert "State: Station Default; Active; Enabled" in detail_text
    assert "Readiness: DX10 is ready." in detail_text
    assert "Radio model: Icom IC-7300" in detail_text
    assert "Role: Transceiver" in detail_text
    assert "Control: FLRig" in detail_text
    assert "Schedule: Evening Net (Active)" in detail_text
    assert "PTT group: Group A" in detail_text
    assert "Notes: Primary HF station" in detail_text
    assert " | " not in detail_text


def test_selected_radio_detail_text_hides_blank_notes() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    tab._effective_assignment_map = lambda: {}
    tab._profile_needs_operator_name = lambda _profile: False
    tab._device_radio_model_summary = lambda _profile: "Icom IC-7300"
    tab._device_class_label = lambda _value: "Transceiver"
    tab._device_backend_label = lambda _value: "FLRig"
    tab._device_software_summary = lambda _profile: "FLRig"
    tab._device_endpoint_summary = lambda _profile: "127.0.0.1:12345"
    tab._device_ptt_group_label = lambda _value: "Group A"

    detail_text = SettingsTab._selected_radio_detail_text(
        tab,
        {
            "id": 7,
            "name": "DX10",
            "runtime_active": 1,
            "enabled": 1,
            "notes": "",
        },
    )

    assert "Notes:" not in detail_text
    assert "State:" in detail_text


def test_assignment_display_text_normalizes_known_and_future_states() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._assignment_state_label("temporary_override") == "Temporary Override"
    assert SettingsTab._assignment_state_label("pending_review") == "Pending Review"
    assert SettingsTab._assignment_state_label("") == "Unassigned"
    assert SettingsTab._assignment_display_text("Evening Net", "temporary_override") == (
        "Evening Net (Temporary Override)"
    )
    assert SettingsTab._assignment_display_text("Evening Net", "pending_review") == "Evening Net (Pending Review)"
    assert SettingsTab._assignment_display_text("", "active") == "Unassigned"
    assert SettingsTab._assignment_display_text("unassigned", "active") == "Unassigned"


def test_selected_radio_status_chip_defs_include_state_and_readiness() -> None:
    from freqinout.core.station_readiness import RadioReadinessSummary, ReadinessReport
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    tab._profile_needs_operator_name = lambda profile: bool(profile.get("needs_operator_name"))
    summary = RadioReadinessSummary(
        radio_id=7,
        name="DX10",
        overall_state="degraded",
        required_count=0,
        recommended_count=2,
        informational_count=0,
        messages=(),
    )
    report = ReadinessReport(
        overall_state="degraded",
        issues=(),
        radio_summaries=(summary,),
        required_count=0,
        recommended_count=2,
        informational_count=0,
        digest="degraded",
    )

    assert SettingsTab._selected_radio_status_chip_defs(
        tab,
        {
            "id": 7,
            "needs_operator_name": 1,
            "runtime_primary": 1,
            "runtime_active": 1,
            "enabled": 1,
        },
        report,
    ) == [
        ("Name Needed", "warning"),
        ("Station Default", "success"),
        ("Active", "info"),
        ("Enabled", "success"),
        ("Degraded", "warning"),
    ]


def test_selected_radio_status_chip_defs_fallback_when_not_selected_or_not_evaluated() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    tab._profile_needs_operator_name = lambda _profile: False

    assert SettingsTab._selected_radio_status_chip_defs(tab, None, None) == []
    assert SettingsTab._selected_radio_status_chip_defs(
        tab,
        {
            "id": 7,
            "runtime_primary": 0,
            "runtime_active": 0,
            "enabled": 0,
        },
        None,
    ) == [
        ("Inactive", "muted"),
        ("Disabled", "danger"),
        ("Not Evaluated", "muted"),
    ]


def test_selected_radio_status_chips_are_wired_into_profile_card() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_card_block = source[
        source.index("self.device_profile_detail_card = QFrame()")
        : source.index("self.device_profile_detail_label = QLabel")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "self.device_profile_status_chips_widget = QWidget()" in profile_card_block
    assert "self.device_profile_status_chips_layout = QHBoxLayout" in profile_card_block
    assert "detail_layout.addWidget(self.device_profile_status_chips_widget)" in profile_card_block
    assert "def _selected_radio_status_chip_defs" in source
    assert "def _make_status_chip_label" in source
    assert "def _refresh_device_profile_status_chips" in source
    assert "readiness_summary_badge_text(summary)" in source
    assert "readiness_state_card_level(str(summary.overall_state or \"\"))" in source
    assert 'self._make_status_chip_label(label, role, theme, "Selected radio status")' in source
    assert "self._refresh_device_profile_status_chips(None, readiness_report)" in readiness_block
    assert "self._refresh_device_profile_status_chips(profile, readiness_report)" in readiness_block


def test_make_status_chip_label_applies_role_style_and_accessible_name() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    app = QApplication.instance() or QApplication([])
    _ = app
    theme = {
        "warning": "#facc15",
        "border": "#94a3b8",
        "surface_alt": "#e5e7eb",
        "text_muted": "#475569",
    }

    chip = SettingsTab._make_status_chip_label(tab, "Name Needed", "warning", theme, "Selected radio status")

    assert chip.text() == "Name Needed"
    assert chip.accessibleName() == "Selected radio status: Name Needed"
    assert chip.sizePolicy().horizontalPolicy() == QSizePolicy.Fixed
    assert "background: #facc15" in chip.styleSheet()
    assert "font-weight: 600" in chip.styleSheet()


def test_radio_profile_software_family_readiness_chip_maps_radio_scoped_issues() -> None:
    from freqinout.core.station_readiness import ReadinessIssue, ReadinessReport
    from freqinout.gui.settings_tab import SettingsTab

    report = ReadinessReport(
        overall_state="needs_setup",
        issues=(
            ReadinessIssue(
                severity="required",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="js8call",
                message="DX10: JS8Call port missing",
                state_key="needs_setup",
            ),
            ReadinessIssue(
                severity="recommended",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="fldigi",
                message="DX10: FLDigi endpoint setup is incomplete",
                state_key="degraded",
            ),
            ReadinessIssue(
                severity="recommended",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="flrig",
                message="DX10: FLRig is managed outside FIO",
                state_key="external_manual",
            ),
            ReadinessIssue(
                severity="required",
                section_key="radio_profiles",
                scope="radio",
                radio_id=8,
                integration_key="varac",
                message="Other radio: VarAC setup incomplete",
                state_key="needs_setup",
            ),
        ),
        radio_summaries=(),
        required_count=1,
        recommended_count=1,
        informational_count=0,
        digest="needs_setup",
    )

    assert SettingsTab._software_family_readiness_chip("js8", 7, report) == ("Needs Setup", "danger")
    assert SettingsTab._software_family_readiness_chip("fast_light", 7, report) == ("Review", "warning")
    assert SettingsTab._software_family_readiness_chip("varac", 7, report) == ("Ready", "success")
    assert SettingsTab._software_family_readiness_chip("js8", 0, report) == ("Not Evaluated", "muted")
    assert SettingsTab._software_family_readiness_chip("js8", 7, None) == ("Not Evaluated", "muted")
    assert SettingsTab._software_family_readiness_chip("launch_control", 7, report) == ("Available", "info")

    manual_report = ReadinessReport(
        overall_state="ok",
        issues=(
            ReadinessIssue(
                severity="recommended",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="flrig",
                message="DX10: FLRig is managed outside FIO",
                state_key="external_manual",
            ),
        ),
        radio_summaries=(),
        required_count=0,
        recommended_count=1,
        informational_count=0,
        digest="manual",
    )
    disabled_report = ReadinessReport(
        overall_state="ok",
        issues=(
            ReadinessIssue(
                severity="recommended",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="flrig",
                message="DX10: FLRig not enabled",
                state_key="not_enabled",
            ),
        ),
        radio_summaries=(),
        required_count=0,
        recommended_count=1,
        informational_count=0,
        digest="disabled",
    )
    assert SettingsTab._software_family_readiness_chip("fast_light", 7, manual_report) == ("Manual", "info")
    assert SettingsTab._software_family_readiness_chip("fast_light", 7, disabled_report) == ("Not Enabled", "muted")


def test_radio_profile_software_chips_include_readiness_status_and_roles() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("self.radio_profile_software_chips_widget = QWidget()")
        : source.index("self.radio_profile_software_stack_section = _make_radio_profile_dashboard_section")
    ]
    refresh_block = source[
        source.index("def _refresh_radio_profile_software_chips")
        : source.index("def _refresh_radio_specific_section_visibility")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "def _software_family_integration_keys" in source
    assert "def _software_readiness_chip_from_issues" in source
    assert "def _software_family_readiness_chip" in source
    assert "self.radio_profile_software_chips_layout = QGridLayout" in profile_block
    assert "self.radio_profile_software_chips_layout.setColumnStretch(4, 1)" in profile_block
    assert "def _radio_profile_software_chip_columns" in source
    assert '"external_manual": ("Manual", "info")' in source
    assert '"not_enabled": ("Not Enabled", "muted")' in source
    assert 'return ("Needs Setup", "danger")' in source
    assert 'return ("Review", "warning")' in source
    assert 'return ("Ready", "success")' in source
    assert 'btn.setText(f"{label}: {status_label}")' in refresh_block
    assert "btn.setStyleSheet(button_style(role, theme))" in refresh_block
    assert "Status: {status_label}" in refresh_block
    assert '("JS8Call", "js8",' in refresh_block
    assert '("Fast Light", "fast_light",' in refresh_block
    assert '("VarAC", "varac",' in refresh_block
    assert "columns = self._radio_profile_software_chip_columns(" in refresh_block
    assert "layout.addWidget(btn, added // columns, added % columns)" in refresh_block
    assert "btn.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)" in refresh_block
    assert "self._refresh_radio_profile_software_chips(readiness_report)" in readiness_block
    assert "self._station_readiness_report_for_software_chips()" in refresh_block


def test_radio_profile_software_chip_columns_wrap_for_narrow_panes() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._radio_profile_software_chip_columns(0) == 1
    assert SettingsTab._radio_profile_software_chip_columns(379) == 1
    assert SettingsTab._radio_profile_software_chip_columns(380) == 2
    assert SettingsTab._radio_profile_software_chip_columns(619) == 2
    assert SettingsTab._radio_profile_software_chip_columns(620) == 4


def test_radio_profile_software_chips_visual_geometry_wraps_long_status_labels() -> None:
    from freqinout.core.station_readiness import ReadinessIssue, ReadinessReport
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    widget = QWidget()
    layout = QGridLayout(widget)
    layout.setContentsMargins(0, 0, 0, 0)
    tab = SettingsTab.__new__(SettingsTab)
    tab.radio_profile_software_chips_widget = widget
    tab.radio_profile_software_chips_layout = layout
    tab.settings = types.SimpleNamespace(get=lambda _key, default=None: default)
    tab.js8_section_group = QWidget()
    tab.fast_light_section_group = QWidget()
    tab.varac_section_group = QWidget()
    tab.launch_control_section_group = QWidget()
    tab._selected_settings_radio_profile = lambda: {
        "id": 7,
        "use_js8call": 1,
        "use_fldigi": 1,
        "use_varac": 1,
        "launch_enabled": 1,
    }

    report = ReadinessReport(
        overall_state="needs_setup",
        issues=(
            ReadinessIssue(
                severity="required",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="fldigi",
                message="DX10: FLDigi endpoint setup is incomplete",
                state_key="needs_setup",
            ),
        ),
        radio_summaries=(),
        required_count=1,
        recommended_count=0,
        informational_count=0,
        digest="needs_setup",
    )

    def button_position(text: str) -> tuple[int, int, int, int]:
        for index in range(layout.count()):
            item = layout.itemAt(index)
            widget_at_index = item.widget() if item is not None else None
            if isinstance(widget_at_index, QPushButton) and widget_at_index.text() == text:
                return layout.getItemPosition(index)
        raise AssertionError(f"button not found: {text}")

    try:
        widget.resize(340, 120)
        SettingsTab._refresh_radio_profile_software_chips(tab, report)
        app.processEvents()

        assert button_position("JS8Call: Ready") == (0, 0, 1, 1)
        assert button_position("Fast Light: Needs Setup") == (1, 0, 1, 1)
        assert button_position("VarAC: Ready") == (2, 0, 1, 1)

        widget.resize(500, 120)
        SettingsTab._refresh_radio_profile_software_chips(tab, report)
        app.processEvents()

        assert button_position("JS8Call: Ready") == (0, 0, 1, 1)
        assert button_position("Fast Light: Needs Setup") == (0, 1, 1, 1)
        assert button_position("VarAC: Ready") == (1, 0, 1, 1)

        widget.resize(700, 120)
        SettingsTab._refresh_radio_profile_software_chips(tab, report)
        app.processEvents()

        assert button_position("JS8Call: Ready") == (0, 0, 1, 1)
        assert button_position("Fast Light: Needs Setup") == (0, 1, 1, 1)
        assert button_position("VarAC: Ready") == (0, 2, 1, 1)
        assert layout.columnStretch(4) == 1
    finally:
        widget.deleteLater()
        app.processEvents()


def test_radio_profile_software_chips_reuse_cached_readiness_report() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    cached_report = object()
    built_report = object()
    tab = SettingsTab.__new__(SettingsTab)
    calls = []
    tab._last_station_readiness_report = cached_report
    tab._current_station_readiness_report = lambda: calls.append("build") or built_report

    assert SettingsTab._station_readiness_report_for_software_chips(tab) is cached_report
    assert calls == []

    tab._last_station_readiness_report = None
    assert SettingsTab._station_readiness_report_for_software_chips(tab) is built_report
    assert calls == ["build"]


def test_device_profile_readiness_refresh_updates_cached_report() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "self._last_station_readiness_report = readiness_report" in readiness_block


def test_radio_profile_stack_guidance_items_map_issues_to_action_rows() -> None:
    from freqinout.core.station_readiness import ReadinessIssue, ReadinessReport
    from freqinout.gui.settings_tab import SettingsTab

    report = ReadinessReport(
        overall_state="needs_setup",
        issues=(
            ReadinessIssue(
                severity="recommended",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="varac",
                message="DX10: VarAC radio setup is incomplete",
                state_key="degraded",
            ),
            ReadinessIssue(
                severity="required",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="js8spotter",
                message="DX10: JS8Spotter launch path missing",
                state_key="needs_setup",
            ),
            ReadinessIssue(
                severity="recommended",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="fldigi",
                message="DX10: FLDigi endpoint setup is incomplete",
                state_key="degraded",
            ),
            ReadinessIssue(
                severity="required",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="flrig",
                message="DX10: FLRig is not enabled for this radio",
                state_key="not_enabled",
            ),
            ReadinessIssue(
                severity="recommended",
                section_key="radio_profiles",
                scope="radio",
                radio_id=7,
                integration_key="commstat",
                message="DX10: CommStat is managed outside FIO",
                state_key="external_manual",
            ),
            ReadinessIssue(
                severity="required",
                section_key="radio_profiles",
                scope="radio",
                radio_id=8,
                integration_key="js8call",
                message="Other radio: JS8Call port missing",
                state_key="needs_setup",
            ),
        ),
        radio_summaries=(),
        required_count=1,
        recommended_count=2,
        informational_count=0,
        digest="needs_setup",
    )

    assert SettingsTab._selected_radio_stack_guidance_items(
        report,
        7,
        radio_name="DX10",
        max_items=5,
    ) == [
        ("JS8Spotter launch path missing", "Open JS8Call Settings", "js8_section_group", "danger"),
        ("FLDigi endpoint setup is incomplete", "Open Fast Light Settings", "fast_light_section_group", "warning"),
        ("VarAC radio setup is incomplete", "Open VarAC Settings", "varac_section_group", "warning"),
        ("FLRig is not enabled for this radio", "Open Fast Light Settings", "fast_light_section_group", "muted"),
        ("CommStat is managed outside FIO", "Open JS8Call Settings", "js8_section_group", "info"),
    ]
    assert SettingsTab._stack_guidance_issue_role(report.issues[3]) == "muted"
    assert SettingsTab._stack_guidance_issue_role(report.issues[4]) == "info"
    assert SettingsTab._selected_radio_stack_guidance_items(None, 7) == []
    assert SettingsTab._selected_radio_stack_guidance_items(report, 0) == []


def test_radio_profile_stack_guidance_panel_is_wired_to_readiness_refresh() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("self.radio_profile_stack_guidance_widget = QWidget()")
        : source.index("self.device_profile_readiness_card = QFrame()")
    ]
    guidance_block = source[
        source.index("def _selected_radio_stack_guidance_items")
        : source.index("def _refresh_radio_specific_section_visibility")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert 'self.radio_profile_stack_guidance_title_label = QLabel("Stack Guidance")' in profile_block
    assert "self.radio_profile_stack_guidance_rows = QVBoxLayout()" in profile_block
    assert "self.radio_profile_stack_guidance_widget.setVisible(False)" in profile_block
    assert 'return ("Open JS8Call Settings", "js8_section_group")' in source
    assert 'return ("Open Fast Light Settings", "fast_light_section_group")' in source
    assert 'return ("Open VarAC Settings", "varac_section_group")' in source
    assert 'return ("Review Radio Profile", "radio_profile_section_group")' in source
    assert "self.radio_profile_section_group = device_group" in source
    assert "cls._stack_guidance_issue_sort_key" in guidance_block
    assert "state_rank" in source
    assert "def _stack_guidance_issue_role" in source
    assert '"external_manual": "info"' in source
    assert '"not_enabled": "muted"' in source
    assert "self.radio_profile_stack_guidance_section.setVisible(bool(items))" in guidance_block
    assert "self.radio_profile_stack_guidance_section.setChecked(bool(items))" in guidance_block
    assert "label.setAccessibleName(f\"Stack guidance: {message}\")" in guidance_block
    assert 'return (message, "Enable Software Options", "radio_profile_software_stack_section", "warning")' in source
    assert "btn.clicked.connect(lambda _checked=False, g=target_group: self._select_settings_section_group(g))" in guidance_block
    assert "self._refresh_radio_profile_stack_guidance(readiness_report, None, None)" in readiness_block
    assert "self._refresh_radio_profile_stack_guidance(readiness_report, int(focused_radio_id), profile)" in readiness_block


def test_radio_profile_no_software_guidance_opens_software_stack_section() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    rows_widget = QWidget()
    rows = QVBoxLayout(rows_widget)
    target_group = QGroupBox("Software Stack")
    tab = SettingsTab.__new__(SettingsTab)
    tab.settings = types.SimpleNamespace(get=lambda _key, default=None: default)
    tab.radio_profile_stack_guidance_rows = rows
    tab.radio_profile_stack_guidance_section = QGroupBox("Stack Guidance")
    tab.radio_profile_stack_guidance_section.setCheckable(True)
    tab.radio_profile_stack_guidance_widget = QWidget()
    tab.radio_profile_software_stack_section = target_group
    selected = []
    tab._select_settings_section_group = lambda group: selected.append(group)

    try:
        SettingsTab._refresh_radio_profile_stack_guidance(
            tab,
            None,
            7,
            {"id": 7, "name": "DX10", "enabled": 1, "runtime_active": 1, "launch_enabled": 1},
        )
        app.processEvents()

        assert rows.count() == 1
        row = rows.itemAt(0).widget()
        assert row is not None
        button = row.findChild(QPushButton)
        assert button is not None
        assert button.text() == "Enable Software Options"
        assert button.accessibleName() == "Enable Software Options"
        assert button.isEnabled() is True

        button.click()

        assert selected == [target_group]
        assert tab.radio_profile_stack_guidance_section.isVisible() is True
        assert tab.radio_profile_stack_guidance_section.isChecked() is True
    finally:
        rows_widget.deleteLater()
        target_group.deleteLater()
        app.processEvents()


def test_fast_light_visibility_includes_rigctld_backend() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._radio_software_enabled({"control_backend": "rigctld"}, "rigctld") is True
    assert SettingsTab._radio_software_enabled({"rig_host": "127.0.0.1"}, "rigctld") is True

    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    software_chip_block = source[
        source.index('("Fast Light", "fast_light"')
        : source.index('("VarAC", "varac"')
    ]
    visibility_source = source[source.index("def _refresh_radio_specific_section_visibility") :]
    visibility_block = visibility_source[
        visibility_source.index("fast_light_visible =")
        : visibility_source.index("varac_visible =")
    ]

    assert 'or self._radio_software_enabled(profile, "rigctld")' in software_chip_block
    assert 'or enabled("rigctld")' in visibility_block


def test_launch_control_chip_visibility_uses_shared_profile_rule() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._radio_profile_launch_control_enabled(None) is False
    assert SettingsTab._radio_profile_launch_control_enabled({}) is False
    assert SettingsTab._radio_profile_launch_control_enabled({"launch_enabled": 0, "use_launch_control": 0}) is False
    assert SettingsTab._radio_profile_launch_control_enabled({"launch_enabled": 1, "use_launch_control": 0}) is True
    assert SettingsTab._radio_profile_launch_control_enabled({"launch_enabled": 0, "use_launch_control": 1}) is True
    assert SettingsTab._radio_profile_launch_opt_in_enabled({"launch_enabled": 1, "use_launch_control": 0}) is True
    assert SettingsTab._radio_profile_operating_launch_allowed({"launch_enabled": 0, "use_launch_control": 1}) is True
    assert SettingsTab._radio_profile_effective_launch_control_enabled({"launch_enabled": 1, "use_launch_control": 1}) is True
    assert SettingsTab._radio_profile_effective_launch_control_enabled({"launch_enabled": 1, "use_launch_control": 0}) is False
    assert SettingsTab._radio_profile_effective_launch_control_enabled({"launch_enabled": 0, "use_launch_control": 1}) is False
    assert SettingsTab._radio_profile_launch_control_summary({"launch_enabled": 1, "use_launch_control": 1}) == "Radio opt-in; plan allows launch"
    assert SettingsTab._radio_profile_launch_control_summary({"launch_enabled": 1, "use_launch_control": 0}) == "Radio opt-in; plan launch off"
    assert SettingsTab._radio_profile_launch_control_summary({"launch_enabled": 0, "use_launch_control": 1}) == "Plan allows launch; radio opt-out"
    assert SettingsTab._radio_profile_launch_control_summary({"launch_enabled": 0, "use_launch_control": 0}) == "Off"

    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    software_chip_block = source[
        source.index('("Launch Control", "launch_control"')
        : source.index("for label, family, target_group, enabled in chip_defs:")
    ]
    visibility_source = source[source.index("def _refresh_radio_specific_section_visibility") :]
    visibility_block = visibility_source[
        visibility_source.index("launch_visible =")
        : visibility_source.index('self._set_settings_section_visible(getattr(self, "radio_software_scope_section_group", None), False)')
    ]

    assert "def _radio_profile_launch_control_enabled" in source
    assert "def _radio_profile_launch_opt_in_enabled" in source
    assert "def _radio_profile_operating_launch_allowed" in source
    assert "def _radio_profile_effective_launch_control_enabled" in source
    assert "def _radio_profile_launch_control_summary" in source
    assert "self._radio_profile_launch_control_enabled(profile)" in software_chip_block
    assert "launch_visible = self._radio_profile_launch_control_enabled(profile)" in visibility_block
    assert 'table.setItem(row, 11, QTableWidgetItem("Opt-in" if self._radio_profile_launch_opt_in_enabled(profile) else "Off"))' in source
    assert 'use_launch_control_chk.setChecked(bool((existing or {}).get("use_launch_control", 0)))' in source
    assert 'launch_enabled_chk.setChecked(bool((existing or {}).get("launch_enabled", 0)))' in source
    assert "Radio launch opt-in:" in source


def test_launch_control_defaults_are_explicit_opt_in() -> None:
    settings_source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    readiness_source = Path("freqinout/core/station_readiness.py").read_text(encoding="utf-8")
    runtime_source = Path("freqinout/core/station_runtime_manager.py").read_text(encoding="utf-8")

    assert 'profile.get("launch_enabled", 0)' in readiness_source
    assert '"launch_control_enabled": bool(int(profile.get("launch_enabled", 0) or 0))' in runtime_source
    assert '"use_launch_control": _row_bool(operating.get("use_launch_control", 0), False)' in runtime_source
    assert 'use_launch_control_chk.setChecked(bool((existing or {}).get("use_launch_control", 0)))' in settings_source
    assert 'launch_enabled_chk.setChecked(bool((existing or {}).get("launch_enabled", 0)))' in settings_source


def test_radio_profile_advanced_edit_wording_keeps_software_settings_inline() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert 'self.edit_device_profile_btn = QPushButton("Advanced Radio Edit")' in source
    assert "identity, role, hardware, and core connection details" in source
    assert 'self.add_device_profile_btn.setAccessibleName("Guided Add Radio")' in source
    assert 'self.edit_device_profile_btn.setAccessibleName("Advanced Radio Edit")' in source
    assert "def _device_profile_dialog_title" in source
    assert "def _device_profile_dialog_intro" in source
    assert "def _device_profile_dialog_save_text" in source
    assert "dlg.setWindowTitle(dlg_title)" in source
    assert 'QMessageBox.information(self, "Advanced Radio Edit", "Select one radio to edit.")' in source
    assert 'QMessageBox.warning(self, "Advanced Radio Edit", "Please select only one radio to edit.")' in source
    assert "Enable at least one software option above" in source
    assert "Use Edit Radio Details to choose the software" not in source


def test_radio_profile_guided_add_dialog_copy_is_distinct_from_advanced_edit() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._device_profile_dialog_title(None) == "Guided Add Radio"
    assert SettingsTab._device_profile_dialog_title({"id": 7}) == "Advanced Radio Edit"
    assert SettingsTab._device_profile_dialog_save_text(None) == "Save Radio"
    assert SettingsTab._device_profile_dialog_save_text({"id": 7}) == "Save Changes"
    assert "one step at a time" in SettingsTab._device_profile_dialog_intro(None)
    assert "software used by that radio" in SettingsTab._device_profile_dialog_intro(None)
    assert "readiness before saving" in SettingsTab._device_profile_dialog_intro(None)
    assert "selected-radio Settings sections" in SettingsTab._device_profile_dialog_intro({"id": 7})


def test_radio_profile_software_flag_helpers_define_inline_stack_choices() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._radio_profile_software_flag_defs() == (
        ("flrig", "FLRig"),
        ("fldigi", "FLDigi"),
        ("flmsg", "FLMsg"),
        ("flamp", "FLAmp"),
        ("js8call", "JS8Call"),
        ("js8spotter", "JS8Spotter"),
        ("commstat", "CommStat"),
        ("varac", "VarAC"),
    )
    assert SettingsTab._radio_profile_software_flag_field("js8spotter") == "use_js8spotter"
    assert SettingsTab._radio_profile_software_flag_field("unknown") == ""
    assert SettingsTab._radio_profile_backend_locked_software({"control_backend": "flrig"}) == ("flrig",)
    assert SettingsTab._radio_profile_backend_locked_software({"control_backend": "js8call"}) == ("js8call",)
    assert SettingsTab._radio_profile_backend_locked_software({"control_backend": "rigctld"}) == ("rigctld",)
    assert SettingsTab._radio_profile_backend_locked_software({"control_backend": "manual"}) == ()


def test_radio_profile_inline_software_flags_persist_selected_radio_payload() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    class Check:
        def __init__(self, checked: bool) -> None:
            self._checked = checked

        def isChecked(self) -> bool:
            return self._checked

    profile = {
        "id": 7,
        "name": "DX10",
        "control_backend": "flrig",
        "use_flrig": 1,
        "use_fldigi": 0,
        "use_flmsg": 0,
        "use_flamp": 0,
        "use_js8call": 0,
        "use_js8spotter": 0,
        "use_commstat": 0,
        "use_varac": 0,
    }
    tab = SettingsTab.__new__(SettingsTab)
    tab.action_feedback_service = ActionFeedbackService()
    tab._last_action_feedback_event = None
    tab._refreshing_radio_profile_software_flags = False
    tab._selected_settings_radio_profile = lambda: profile
    tab._profile_display_name = lambda row: str(row.get("name", "") or "Radio")
    tab._set_settings_action_feedback_status = lambda *_args: None
    saved = []
    tab._persist_device_profile = lambda payload, *, existing=None: saved.append((payload, existing))
    section_refreshes = []
    readiness_refreshes = []
    tab._refresh_radio_specific_section_visibility = lambda: section_refreshes.append("sections")
    tab._update_device_profile_readiness_detail = lambda: readiness_refreshes.append("readiness")
    tab._radio_profile_software_flag_checks = {
        "flrig": Check(False),
        "fldigi": Check(True),
        "flmsg": Check(False),
        "flamp": Check(True),
        "js8call": Check(False),
        "js8spotter": Check(True),
        "commstat": Check(False),
        "varac": Check(True),
    }

    SettingsTab._on_radio_profile_software_flag_changed(tab, "varac")

    assert len(saved) == 1
    payload, existing = saved[0]
    assert existing is profile
    assert payload["id"] == 7
    assert payload["use_flrig"] is True
    assert payload["use_fldigi"] is True
    assert payload["use_flamp"] is True
    assert payload["use_js8spotter"] is True
    assert payload["use_varac"] is True
    assert payload["use_js8call"] is False
    assert section_refreshes == ["sections"]
    assert readiness_refreshes == ["readiness"]
    events = tab.action_feedback_service.recent(scope="settings")
    assert len(events) == 1
    assert events[0].action_type == "software_flags"
    assert events[0].status == "succeeded"
    assert events[0].summary == "Enabled VarAC for DX10."
    assert "Software Used updated in Radio Profile for DX10." in events[0].detail
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"


def test_radio_profile_inline_software_flag_controls_are_wired() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("self.radio_profile_software_flags_widget = QWidget()")
        : source.index("self.radio_profile_software_chips_widget = QWidget()")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "self._radio_profile_software_flag_checks: Dict[str, QCheckBox] = {}" in source
    assert "self._refreshing_radio_profile_software_flags = False" in source
    assert "software_flags_layout = QGridLayout(self.radio_profile_software_flags_widget)" in profile_block
    assert "for index, (key, label) in enumerate(self._radio_profile_software_flag_defs()):" in profile_block
    assert 'chk.setAccessibleName(f"Enable {label} for the selected radio")' in profile_block
    assert "chk.stateChanged.connect(lambda _state, k=key: self._on_radio_profile_software_flag_changed(k))" in profile_block
    assert "radio_profile_software_layout.addWidget(self.radio_profile_software_flags_widget)" in profile_block
    assert "def _refresh_radio_profile_software_flag_controls" in source
    assert "def _on_radio_profile_software_flag_changed" in source
    assert "def _publish_radio_profile_software_flag_feedback" in source
    assert "self._refresh_radio_specific_section_visibility()" in source
    assert "self._update_device_profile_readiness_detail()" in source
    assert 'action_type="software_flags"' in source
    assert "self._refresh_radio_profile_software_flag_controls(None)" in readiness_block
    assert "self._refresh_radio_profile_software_flag_controls(profile)" in readiness_block


def test_radio_profile_dashboard_sections_wrap_existing_profile_widgets() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("def _make_radio_profile_dashboard_section")
        : source.index("self.device_profiles_table = QTableWidget")
    ]

    assert "def _make_radio_profile_dashboard_section(title: str, content: QWidget, *, checked: bool = True) -> QGroupBox:" in profile_block
    assert "section.setCheckable(True)" in profile_block
    assert 'section.setToolTip(f"Show or hide the {title} section.")' in profile_block
    assert 'section.setAccessibleName(f"{title} section")' in profile_block
    assert "section.toggled.connect(content.setVisible)" in profile_block
    assert 'self.radio_profile_identity_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Radio Identity"' in profile_block
    assert 'self.radio_profile_software_stack_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Software Stack"' in profile_block
    assert 'self.radio_profile_stack_guidance_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Stack Guidance"' in profile_block
    assert "self.radio_profile_stack_guidance_section.setVisible(False)" in profile_block
    assert 'self.radio_profile_connection_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Connection Details"' in profile_block
    assert 'self.radio_profile_frequency_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Frequency / Timer Behavior"' in profile_block
    assert 'self.radio_profile_optional_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Optional Groups and Notes"' in profile_block
    assert 'self.radio_profile_inventory_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Advanced Inventory"' in profile_block
    assert 'self.radio_profile_readiness_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Readiness"' in profile_block
    assert 'self.radio_profile_actions_section = _make_radio_profile_dashboard_section(' in profile_block
    assert '"Radio Actions"' in profile_block
    assert (
        'self.radio_profile_stack_guidance_section = _make_radio_profile_dashboard_section(\n'
        '            "Stack Guidance",\n'
        '            stack_guidance_content,\n'
        '            checked=False,\n'
        '        )'
    ) in profile_block
    assert (
        'self.radio_profile_connection_section = _make_radio_profile_dashboard_section(\n'
        '            "Connection Details",\n'
        '            radio_profile_connection_content,\n'
        '            checked=False,\n'
        '        )'
    ) in profile_block
    assert (
        'self.radio_profile_frequency_section = _make_radio_profile_dashboard_section(\n'
        '            "Frequency / Timer Behavior",\n'
        '            radio_profile_frequency_content,\n'
        '            checked=False,\n'
        '        )'
    ) in profile_block
    assert (
        'self.radio_profile_optional_section = _make_radio_profile_dashboard_section(\n'
        '            "Optional Groups and Notes",\n'
        '            radio_profile_optional_content,\n'
        '            checked=False,\n'
        '        )'
    ) in profile_block
    assert (
        'self.radio_profile_inventory_section = _make_radio_profile_dashboard_section(\n'
        '            "Advanced Inventory",\n'
        '            radio_profile_inventory_content,\n'
        '            checked=False,\n'
        '        )'
    ) in profile_block
    assert (
        'self.radio_profile_actions_section = _make_radio_profile_dashboard_section(\n'
        '            "Radio Actions",\n'
        '            radio_profile_actions_content,\n'
        '            checked=False,\n'
        '        )'
    ) in profile_block
    assert "device_layout.addWidget(self.radio_profile_identity_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_software_stack_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_stack_guidance_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_connection_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_frequency_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_optional_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_inventory_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_readiness_section)" in profile_block
    assert "device_layout.addWidget(self.radio_profile_actions_section)" in profile_block


def test_radio_profile_connection_details_summarize_selected_profile() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)

    assert SettingsTab._selected_radio_connection_detail_rows(tab, None) == (
        ("backend", "--"),
        ("endpoint", "--"),
        ("ptt", "--"),
        ("launch", "--"),
    )
    assert SettingsTab._selected_radio_connection_detail_rows(
        tab,
        {
            "control_backend": "rigctld",
            "rig_host": "192.0.2.10",
            "rig_port": 4532,
            "ptt_group": "HF A",
            "launch_enabled": 0,
            "use_launch_control": 1,
        },
    ) == (
        ("backend", "RIGCTLD"),
        ("endpoint", "RIGCTLD 192.0.2.10:4532"),
        ("ptt", "HF A"),
        ("launch", "Plan allows launch; radio opt-out"),
    )
    assert SettingsTab._selected_radio_connection_detail_rows(
        tab,
        {
            "control_backend": "rigctld",
            "rig_host": "192.0.2.10",
            "rig_port": 4532,
            "ptt_group": "HF A",
            "launch_enabled": 1,
            "use_launch_control": 0,
        },
    )[-1] == ("launch", "Radio opt-in; plan launch off")


def test_radio_profile_connection_details_are_wired_to_readiness_refresh() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("radio_profile_connection_content = QWidget()")
        : source.index("self.device_profile_readiness_card = QFrame()")
    ]
    refresh_block = source[
        source.index("def _refresh_radio_profile_connection_details")
        : source.index("def _radio_profile_software_flag_defs")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "radio_profile_connection_layout = QFormLayout(radio_profile_connection_content)" in profile_block
    assert "self.radio_profile_connection_backend_label = QLabel(\"--\")" in profile_block
    assert "radio_profile_connection_layout.addRow(\"Control:\", self.radio_profile_connection_backend_label)" in profile_block
    assert "radio_profile_connection_layout.addRow(\"Endpoint:\", self.radio_profile_connection_endpoint_label)" in profile_block
    assert "radio_profile_connection_layout.addRow(\"PTT group:\", self.radio_profile_connection_ptt_label)" in profile_block
    assert "radio_profile_connection_layout.addRow(\"Launch:\", self.radio_profile_connection_launch_label)" in profile_block
    assert "def _selected_radio_connection_detail_rows" in source
    assert "def _refresh_radio_profile_connection_details" in source
    assert "self._set_form_detail_label(label, rows.get(key, \"--\"), f\"Selected radio {key}\")" in refresh_block
    assert "self._refresh_radio_profile_connection_details(None)" in readiness_block
    assert "self._refresh_radio_profile_connection_details(profile)" in readiness_block


def test_radio_profile_frequency_timer_details_summarize_selected_profile() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    tab.operating_profiles = [
        {
            "id": 3,
            "name": "Evening Net",
            "scheduler_enabled": 1,
            "scheduler_mode": "simple",
        }
    ]
    tab._effective_assignment_map = lambda: {
        7: {
            "operating_profile_id": 3,
            "operating_profile_name": "Evening Net",
            "assignment_state": "active",
        }
    }

    assert SettingsTab._selected_radio_frequency_timer_rows(tab, None) == (
        ("schedule", "--"),
        ("scheduler", "--"),
        ("js8_offset", "--"),
        ("timer_source", "--"),
    )
    assert SettingsTab._selected_radio_frequency_timer_rows(
        tab,
        {
            "id": 7,
            "js8_offset_hz": 1750,
            "scheduler_enabled": 0,
            "schedule_hold_minutes_default": 60,
            "freq_enforcement_mode": "Prompt",
            "freq_prompt_interval": "Every 15 minutes",
            "fldigi_enforcement_mode": "On Schedule Change",
            "fldigi_prompt_interval": "Hourly",
            "js8_enforcement_mode": "Disabled",
            "js8_prompt_interval": "Every 5 minutes",
        },
    ) == (
        ("schedule", "Evening Net (Active)"),
        ("scheduler", "Enabled / Simple"),
        ("js8_offset", "1750 Hz"),
        (
            "timer_source",
            "Radio policy: scheduler Off; Hold 60 min; Freq Prompt (Every 15 minutes); "
            "FLDigi On Schedule Change (Hourly); JS8 Disabled (Every 5 minutes)",
        ),
    )


def test_radio_profile_timer_policy_text_defaults_invalid_values() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    assert SettingsTab._radio_profile_timer_policy_text(
        {"freq_enforcement_mode": "Something Else"},
        "freq_enforcement_mode",
        "On Schedule Change",
        ("On Schedule Change", "Prompt"),
    ) == "On Schedule Change"
    assert SettingsTab._radio_profile_timer_policy_text(
        {"freq_prompt_interval": "Every 10 minutes"},
        "freq_prompt_interval",
        "Hourly",
        ("Hourly", "Every 10 minutes"),
    ) == "Every 10 minutes"
    assert SettingsTab._radio_profile_hold_duration_minutes({"schedule_hold_minutes_default": 90}) == 90
    assert SettingsTab._radio_profile_hold_duration_minutes({"schedule_hold_minutes_default": "bad"}) == 30
    assert SettingsTab._radio_profile_hold_duration_minutes({"schedule_hold_minutes_default": 45}) == 30


def test_radio_profile_timer_policy_controls_refresh_and_extract_values() -> None:
    from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox

    from freqinout.gui.settings_tab import SettingsTab

    QApplication.instance() or QApplication([])
    tab = SettingsTab.__new__(SettingsTab)
    scheduler_chk = QCheckBox()
    hold_combo = QComboBox()
    hold_combo.addItems(["30 minutes", "60 minutes", "90 minutes", "120 minutes"])
    freq_mode = QComboBox()
    freq_mode.addItems(["On Schedule Change", "Prompt"])
    freq_prompt = QComboBox()
    freq_prompt.addItems(["Hourly", "Every 5 minutes", "Every 10 minutes"])
    fldigi_mode = QComboBox()
    fldigi_mode.addItems(["On Schedule Change", "Prompt"])
    fldigi_prompt = QComboBox()
    fldigi_prompt.addItems(["Hourly", "Every 5 minutes", "Every 10 minutes"])
    js8_mode = QComboBox()
    js8_mode.addItems(["On Schedule Change", "Prompt"])
    js8_prompt = QComboBox()
    js8_prompt.addItems(["Hourly", "Every 5 minutes", "Every 10 minutes"])
    tab._radio_profile_timer_policy_controls = {
        "scheduler_enabled": scheduler_chk,
        "schedule_hold_minutes_default": hold_combo,
        "freq_enforcement_mode": freq_mode,
        "freq_prompt_interval": freq_prompt,
        "fldigi_enforcement_mode": fldigi_mode,
        "fldigi_prompt_interval": fldigi_prompt,
        "js8_enforcement_mode": js8_mode,
        "js8_prompt_interval": js8_prompt,
    }
    tab._refreshing_radio_profile_timer_policy = False

    SettingsTab._refresh_radio_profile_timer_policy_controls(
        tab,
        {
            "scheduler_enabled": 0,
            "schedule_hold_minutes_default": 90,
            "freq_enforcement_mode": "Prompt",
            "freq_prompt_interval": "Every 10 minutes",
            "fldigi_enforcement_mode": "On Schedule Change",
            "fldigi_prompt_interval": "Every 5 minutes",
            "js8_enforcement_mode": "Prompt",
            "js8_prompt_interval": "Every 5 minutes",
        },
    )

    assert scheduler_chk.isChecked() is False
    assert hold_combo.currentText() == "90 minutes"
    assert hold_combo.isEnabled() is True
    assert freq_mode.currentText() == "Prompt"
    assert freq_prompt.isEnabled() is True
    assert freq_prompt.currentText() == "Every 10 minutes"
    assert fldigi_mode.currentText() == "On Schedule Change"
    assert fldigi_prompt.isEnabled() is False
    assert js8_mode.currentText() == "Prompt"
    assert js8_prompt.isEnabled() is True

    values = SettingsTab._radio_profile_timer_policy_control_values(tab)
    assert values["scheduler_enabled"] is False
    assert values["schedule_hold_minutes_default"] == 90
    assert values["freq_prompt_interval"] == "Every 10 minutes"
    assert values["fldigi_prompt_interval"] == "Hourly"
    assert values["js8_prompt_interval"] == "Every 5 minutes"

    tab._radio_profile_timer_policy_controls = {}
    assert SettingsTab._radio_profile_timer_policy_control_values(tab)["scheduler_enabled"] is False


def test_radio_profile_frequency_timer_details_are_wired_to_readiness_refresh() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("radio_profile_frequency_content = QWidget()")
        : source.index("self.device_profile_readiness_card = QFrame()")
    ]
    refresh_block = source[
        source.index("def _refresh_radio_profile_frequency_timer_details")
        : source.index("def _radio_profile_software_flag_defs")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "radio_profile_frequency_layout = QFormLayout(radio_profile_frequency_content)" in profile_block
    assert "self.radio_profile_frequency_schedule_label = QLabel(\"--\")" in profile_block
    assert "radio_profile_frequency_layout.addRow(\"Schedule:\", self.radio_profile_frequency_schedule_label)" in profile_block
    assert "radio_profile_frequency_layout.addRow(\"Scheduler:\", self.radio_profile_frequency_scheduler_label)" in profile_block
    assert "radio_profile_frequency_layout.addRow(\"JS8 offset:\", self.radio_profile_frequency_js8_offset_label)" in profile_block
    assert "radio_profile_frequency_layout.addRow(\"Timer source:\", self.radio_profile_frequency_timer_source_label)" in profile_block
    assert "self.radio_profile_timer_scheduler_chk = QCheckBox(\"Scheduler automation for this radio\")" in profile_block
    assert "self.radio_profile_default_hold_combo = _make_radio_profile_timer_combo" in profile_block
    assert "tuple(f\"{minutes} minutes\" for minutes in sorted(SUPPORTED_HOLD_DURATION_MINUTES))" in profile_block
    assert "radio_profile_frequency_layout.addRow(\"Default hold:\", self.radio_profile_default_hold_combo)" in profile_block
    assert "self._radio_profile_timer_policy_controls[\"schedule_hold_minutes_default\"]" in profile_block
    assert "mode_combo.setAccessibleName(f\"{label.rstrip(':')} mode\")" in profile_block
    assert "prompt_combo.setAccessibleName(f\"{label.rstrip(':')} prompt interval\")" in profile_block
    assert "_add_radio_profile_timer_policy_row(\"Frequency timer:\", \"freq_enforcement_mode\", \"freq_prompt_interval\")" in profile_block
    assert "_add_radio_profile_timer_policy_row(\"FLDigi mode timer:\", \"fldigi_enforcement_mode\", \"fldigi_prompt_interval\")" in profile_block
    assert "_add_radio_profile_timer_policy_row(\"JS8 offset timer:\", \"js8_enforcement_mode\", \"js8_prompt_interval\")" in profile_block
    assert "def _selected_radio_frequency_timer_rows" in source
    assert "def _refresh_radio_profile_frequency_timer_details" in source
    assert "def _refresh_radio_profile_timer_policy_controls" in source
    assert "self._set_form_detail_label(label, rows.get(key, \"--\"), f\"Selected radio {key}\")" in refresh_block
    assert "self._refresh_radio_profile_timer_policy_controls(profile)" in refresh_block
    assert "self._refresh_radio_profile_frequency_timer_details(None)" in readiness_block
    assert "self._refresh_radio_profile_frequency_timer_details(profile)" in readiness_block


def test_radio_profile_timer_policy_controls_persist_with_feedback() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    change_block = source[
        source.index("def _on_radio_profile_timer_policy_changed")
        : source.index("def _publish_radio_profile_timer_policy_feedback")
    ]
    feedback_block = source[
        source.index("def _publish_radio_profile_timer_policy_feedback")
        : source.index("def _radio_profile_optional_value")
    ]

    assert "payload.update(self._radio_profile_timer_policy_control_values())" in change_block
    assert "\"schedule_hold_minutes_default\"" in source[source.index("def _radio_profile_timer_policy_control_values") : source.index("def _on_radio_profile_timer_policy_changed")]
    assert "SUPPORTED_HOLD_DURATION_MINUTES" in source[source.index("def _radio_profile_hold_duration_minutes") : source.index("def _refresh_radio_profile_timer_policy_controls")]
    assert "self._persist_device_profile(payload, existing=profile)" in change_block
    assert "self._refresh_radio_profile_frequency_timer_details(refreshed)" in change_block
    assert "self._update_device_profile_readiness_detail()" in change_block
    assert "self._publish_radio_profile_timer_policy_feedback(refreshed)" in change_block
    assert "else False" in source[source.index("def _radio_profile_timer_policy_control_values") : source.index("def _on_radio_profile_timer_policy_changed")]
    assert "if mode != \"Prompt\":" in source[source.index("def _radio_profile_timer_policy_control_values") : source.index("def _on_radio_profile_timer_policy_changed")]
    assert "action_type=\"timer_policy\"" in feedback_block
    assert "Updated timer policy for" in feedback_block
    assert "self._selected_radio_timer_policy_summary(profile or {})" in feedback_block


def test_radio_profile_optional_groups_summarize_selected_profile() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)

    assert SettingsTab._selected_radio_optional_group_rows(tab, None) == (
        ("ptt", "--"),
        ("antenna", "--"),
        ("frontend", "--"),
        ("amplifier", "--"),
        ("notes", "--"),
    )
    assert SettingsTab._selected_radio_optional_group_rows(
        tab,
        {
            "ptt_group": "HF A",
            "antenna_group": "ANT-1",
            "frontend_group": "RX-Chain",
            "amplifier_group": "AMP-MAIN",
            "notes": "Desk radio near tuner.",
        },
    ) == (
        ("ptt", "HF A"),
        ("antenna", "ANT-1"),
        ("frontend", "RX-Chain"),
        ("amplifier", "AMP-MAIN"),
        ("notes", "Desk radio near tuner."),
    )


def test_radio_profile_optional_groups_are_wired_to_readiness_refresh() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("radio_profile_optional_content = QWidget()")
        : source.index("self.device_profile_readiness_card = QFrame()")
    ]
    refresh_block = source[
        source.index("def _refresh_radio_profile_optional_groups")
        : source.index("def _radio_profile_software_flag_defs")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "radio_profile_optional_layout = QFormLayout(radio_profile_optional_content)" in profile_block
    assert "self.radio_profile_optional_ptt_label = QLabel(\"--\")" in profile_block
    assert "radio_profile_optional_layout.addRow(\"PTT group:\", self.radio_profile_optional_ptt_label)" in profile_block
    assert "radio_profile_optional_layout.addRow(\"Antenna group:\", self.radio_profile_optional_antenna_label)" in profile_block
    assert "radio_profile_optional_layout.addRow(\"Front-end group:\", self.radio_profile_optional_frontend_label)" in profile_block
    assert "radio_profile_optional_layout.addRow(\"Amplifier group:\", self.radio_profile_optional_amplifier_label)" in profile_block
    assert "radio_profile_optional_layout.addRow(\"Notes:\", self.radio_profile_optional_notes_label)" in profile_block
    assert "def _selected_radio_optional_group_rows" in source
    assert "def _refresh_radio_profile_optional_groups" in source
    assert "def _set_form_detail_label" in source
    assert "hide_empty=key in {\"antenna\", \"frontend\", \"amplifier\", \"notes\"}" in refresh_block
    assert "self._refresh_radio_profile_optional_groups(None)" in readiness_block
    assert "self._refresh_radio_profile_optional_groups(profile)" in readiness_block


def test_form_detail_label_hides_optional_empty_rows() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    _ = app
    parent = QWidget()
    layout = QFormLayout(parent)
    row_label = QLabel("Antenna group:")
    value_label = QLabel("--")
    layout.addRow(row_label, value_label)

    SettingsTab._set_form_detail_label(
        value_label,
        "--",
        "Selected radio optional antenna",
        hide_empty=True,
    )

    assert value_label.isHidden() is True
    assert row_label.isHidden() is True
    assert value_label.accessibleName() == "Selected radio optional antenna: --"

    SettingsTab._set_form_detail_label(
        value_label,
        "ANT-1",
        "Selected radio optional antenna",
        hide_empty=True,
    )

    assert value_label.isHidden() is False
    assert row_label.isHidden() is False
    assert value_label.accessibleName() == "Selected radio optional antenna: ANT-1"


def test_radio_profile_inventory_details_summarize_selected_profile() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)

    assert SettingsTab._selected_radio_inventory_rows(tab, None) == (
        ("id", "--"),
        ("system_key", "--"),
        ("instance", "--"),
        ("class", "--"),
        ("model", "--"),
        ("runtime", "--"),
    )
    assert SettingsTab._selected_radio_inventory_rows(
        tab,
        {
            "id": 7,
            "system_key": "field_radio",
            "instance_number": 2,
            "device_class": "observer",
            "deployment_mode": "minimal",
            "radio_manufacturer": "Icom",
            "radio_model": "IC-705",
            "enabled": 1,
            "runtime_primary": 1,
            "runtime_active": 0,
        },
    ) == (
        ("id", "7"),
        ("system_key", "field_radio"),
        ("instance", "2"),
        ("class", "Observer / SDR / Minimal"),
        ("model", "Icom IC-705"),
        ("runtime", "Enabled; Station Default; Inactive"),
    )


def test_radio_profile_inventory_details_are_wired_to_readiness_refresh() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    profile_block = source[
        source.index("radio_profile_inventory_content = QWidget()")
        : source.index("self.device_profile_readiness_card = QFrame()")
    ]
    refresh_block = source[
        source.index("def _refresh_radio_profile_inventory_details")
        : source.index("def _radio_profile_software_flag_defs")
    ]
    readiness_block = source[
        source.index("def _update_device_profile_readiness_detail")
        : source.index("def _set_guidance_card_state")
    ]

    assert "radio_profile_inventory_layout = QFormLayout(radio_profile_inventory_content)" in profile_block
    assert "self.radio_profile_inventory_id_label = QLabel(\"--\")" in profile_block
    assert "radio_profile_inventory_layout.addRow(\"Profile ID:\", self.radio_profile_inventory_id_label)" in profile_block
    assert "radio_profile_inventory_layout.addRow(\"System key:\", self.radio_profile_inventory_system_key_label)" in profile_block
    assert "radio_profile_inventory_layout.addRow(\"Instance:\", self.radio_profile_inventory_instance_label)" in profile_block
    assert "radio_profile_inventory_layout.addRow(\"Class / deploy:\", self.radio_profile_inventory_class_label)" in profile_block
    assert "radio_profile_inventory_layout.addRow(\"Model:\", self.radio_profile_inventory_model_label)" in profile_block
    assert "radio_profile_inventory_layout.addRow(\"Runtime:\", self.radio_profile_inventory_runtime_label)" in profile_block
    assert "def _selected_radio_inventory_rows" in source
    assert "def _refresh_radio_profile_inventory_details" in source
    assert "self._set_form_detail_label(label, rows.get(key, \"--\"), f\"Selected radio inventory {key}\")" in refresh_block
    assert "self._refresh_radio_profile_inventory_details(None)" in readiness_block
    assert "self._refresh_radio_profile_inventory_details(profile)" in readiness_block


def test_radio_profile_no_software_message_warns_for_enabled_radios() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)

    assert SettingsTab._radio_profile_no_software_message(tab, None) == (
        "Select a radio before choosing the software used by that radio.",
        "muted",
    )
    assert SettingsTab._radio_profile_no_software_message(tab, {"enabled": 0}) == (
        "No radio software is enabled yet. Enable software above when this radio should participate in FIO workflows.",
        "muted",
    )
    assert SettingsTab._radio_profile_no_software_message(tab, {"enabled": 1, "runtime_active": 0, "runtime_primary": 0}) == (
        "No radio software is enabled yet. Enable at least one software option above before using this radio.",
        "warning",
    )
    assert SettingsTab._radio_profile_no_software_message(tab, {"enabled": 1, "runtime_active": 1, "runtime_primary": 0}) == (
        "No software options are enabled for this radio. Enable at least one software option above so FIO can operate it.",
        "warning",
    )
    assert SettingsTab._radio_software_enabled({"control_backend": "flrig"}, "flrig") is True
    assert SettingsTab._radio_software_enabled({"control_backend": "js8call"}, "js8call") is True
    assert SettingsTab._radio_software_enabled({"control_backend": "rigctld"}, "rigctld") is True


def test_radio_profile_no_software_stack_guidance_item_warns_only_when_actionable() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)

    assert SettingsTab._radio_profile_has_software_option(tab, None) is False
    assert SettingsTab._radio_profile_no_software_stack_guidance_item(tab, None) is None
    assert SettingsTab._radio_profile_no_software_stack_guidance_item(tab, {"enabled": 0}) is None
    launch_only_profile = {"enabled": 1, "runtime_active": 1, "launch_enabled": 1, "use_launch_control": 1}
    assert SettingsTab._radio_profile_has_software_option(tab, launch_only_profile) is False
    assert SettingsTab._radio_profile_no_software_stack_guidance_item(tab, launch_only_profile) == (
        "No software options are enabled for this radio. Enable at least one software option above so FIO can operate it.",
        "Enable Software Options",
        "radio_profile_software_stack_section",
        "warning",
    )
    assert SettingsTab._radio_profile_no_software_stack_guidance_item(
        tab,
        {"enabled": 1, "runtime_active": 1, "runtime_primary": 0},
    ) == (
        "No software options are enabled for this radio. Enable at least one software option above so FIO can operate it.",
        "Enable Software Options",
        "radio_profile_software_stack_section",
        "warning",
    )
    assert SettingsTab._radio_profile_has_software_option(tab, {"control_backend": "rigctld"}) is True
    assert SettingsTab._radio_profile_no_software_stack_guidance_item(tab, {"enabled": 1, "use_varac": True}) is None


def test_radio_profile_no_software_guardrail_is_wired_to_empty_chip_state() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    refresh_block = source[
        source.index("def _refresh_radio_profile_software_chips")
        : source.index("def _stack_guidance_issue_target")
    ]

    assert "def _radio_profile_no_software_message" in source
    assert "empty_text, empty_role = self._radio_profile_no_software_message(profile)" in refresh_block
    assert "empty.setAccessibleName(empty_text)" in refresh_block
    assert 'empty.setStyleSheet(self._settings_feedback_label_style("partial", theme))' in refresh_block
    assert "No software options are enabled for this radio." in source


def test_radio_profile_no_software_guardrail_is_wired_to_stack_guidance() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    software_option_block = source[
        source.index("def _radio_profile_software_option_keys")
        : source.index("def _radio_profile_has_software_option")
    ]
    has_software_block = source[
        source.index("def _radio_profile_has_software_option")
        : source.index("def _radio_profile_no_software_message")
    ]
    guidance_block = source[
        source.index("def _radio_profile_no_software_stack_guidance_item")
        : source.index("def _refresh_radio_specific_section_visibility")
    ]

    assert "def _radio_profile_has_software_option" in source
    assert "_radio_profile_has_software_lane" not in source
    assert "def _radio_profile_software_option_keys" in source
    assert '"rigctld"' in software_option_block
    assert '"launch_control"' not in software_option_block
    assert '"launch_enabled"' not in software_option_block
    assert "for key in self._radio_profile_software_option_keys()" in has_software_block
    assert 'return (message, "Enable Software Options", "radio_profile_software_stack_section", "warning")' in guidance_block
    assert "no_software_item = self._radio_profile_no_software_stack_guidance_item(profile)" in guidance_block
    assert "if no_software_item is not None:" in guidance_block
    assert "items.append(no_software_item)" in guidance_block
    assert "max_items=max(0, 4 - len(items))" in guidance_block
    assert "software lanes" not in source


def test_main_window_wires_shared_action_feedback_service_to_settings() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "from freqinout.core.shared_state import ActionFeedbackEvent, ActionFeedbackService" in source
    assert "self.action_feedback_service = ActionFeedbackService()" in source
    assert "SettingsTab(self, action_feedback_service=self.action_feedback_service)" in source


def test_main_window_has_action_feedback_banner_subscriber() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert "from freqinout.core.shared_state import ActionFeedbackEvent, ActionFeedbackService" in source
    assert "self.action_feedback_banner = QFrame(right_container)" in source
    assert 'self.action_feedback_banner.setAccessibleName("Action feedback")' in source
    assert "self.action_feedback_label = QLabel(\"\")" in source
    assert 'self.action_feedback_label.setAccessibleName("Action feedback message")' in source
    assert "self._action_feedback_unsubscribe = self.action_feedback_service.subscribe(self._on_action_feedback_event)" in source
    assert "def _on_action_feedback_event(self, event: ActionFeedbackEvent) -> None:" in source
    assert "event.scope" in source
    assert "self._action_feedback_banner_scopes()" in source
    assert "self.action_feedback_banner.setVisible(True)" in source
    assert "def _hide_action_feedback_banner(self) -> None:" in source
    assert 'getattr(self, "_action_feedback_unsubscribe", None)' in source
    assert 'self.action_feedback_history_btn.setText("History")' in source
    assert "self.action_feedback_history_btn.clicked.connect(self._show_recent_actions_dialog)" in source
    assert 'self.action_feedback_history_btn.setAccessibleName("Recent actions")' in source
    assert "def _show_recent_actions_dialog(self) -> None:" in source
    assert "self.action_feedback_service.recent()[:20]" in source
    assert 'existing.show()' in source
    assert 'dialog.setAccessibleName("Recent actions")' in source
    assert 'line.setAccessibleName(line.text())' in source


def test_main_window_action_feedback_banner_status_timing_is_modest() -> None:
    from freqinout.gui.main_window import MainWindow

    assert MainWindow._action_feedback_banner_role("succeeded") == "success"
    assert MainWindow._action_feedback_banner_role("blocked") == "warning"
    assert MainWindow._action_feedback_banner_role("in_progress") == "info"
    assert MainWindow._action_feedback_display_ms("succeeded") == 6000
    assert MainWindow._action_feedback_display_ms("blocked") == 12000
    assert MainWindow._action_feedback_display_ms("in_progress") == 7000


def test_main_window_recent_action_line_formats_settings_events() -> None:
    from freqinout.core.shared_state import ActionFeedbackEvent
    from freqinout.gui.main_window import MainWindow

    event = ActionFeedbackEvent(
        id="feedback_1",
        timestamp_utc="2026-07-22T12:34:56Z",
        scope="settings",
        action_type="save",
        status="succeeded",
        summary="Saved settings for DX10.",
        radio_profile_id="7",
        target_label="DX10",
        detail="Saved from Settings.",
        source_surface="settings",
    )

    assert MainWindow._recent_action_line(event) == "SUCCEEDED | 12:34:56 | DX10: Saved settings for DX10."


def test_main_window_feedback_banner_accepts_station_control_scopes(monkeypatch) -> None:
    from freqinout.core.shared_state import ActionFeedbackEvent
    from freqinout.gui.main_window import MainWindow

    class _FakeLabel:
        def __init__(self) -> None:
            self.text = ""
            self.tooltip = ""

        def setText(self, value: str) -> None:
            self.text = value

        def setToolTip(self, value: str) -> None:
            self.tooltip = value

    class _FakeBanner:
        def __init__(self) -> None:
            self.visible = False
            self.style = ""

        def setStyleSheet(self, value: str) -> None:
            self.style = value

        def setVisible(self, value: bool) -> None:
            self.visible = bool(value)

    class _FakeTimer:
        def __init__(self) -> None:
            self.started_ms = None

        def start(self, value: int) -> None:
            self.started_ms = value

    window = MainWindow.__new__(MainWindow)
    window.action_feedback_label = _FakeLabel()
    window.action_feedback_banner = _FakeBanner()
    window._action_feedback_clear_timer = _FakeTimer()
    monkeypatch.setattr(MainWindow, "_action_feedback_banner_style", lambda self, status: status)
    event = ActionFeedbackEvent(
        id="feedback_2",
        timestamp_utc="2026-07-24T12:34:56Z",
        scope="radio",
        action_type="qsy",
        status="succeeded",
        summary="QSY sent to DX10: 7.268 LSB",
        target_label="DX10",
        detail="Hold active for 60 minutes.",
        source_surface="controlfreq",
    )

    MainWindow._on_action_feedback_event(window, event)

    assert window.action_feedback_label.text == "QSY sent to DX10: 7.268 LSB"
    assert window.action_feedback_label.tooltip == "Hold active for 60 minutes."
    assert window.action_feedback_banner.visible is True
    assert window._action_feedback_clear_timer.started_ms == 6000


def test_main_window_schedule_control_feedback_publishes_scheduler_event() -> None:
    from freqinout.gui.main_window import MainWindow

    service = ActionFeedbackService()
    window = MainWindow.__new__(MainWindow)
    window.action_feedback_service = service
    window._active_runtime_profile = {"id": 7, "name": "DX10"}

    MainWindow._publish_schedule_control_feedback(
        window,
        action_type="suspend_schedule",
        status="succeeded",
        summary="DX10 suspended for 60 minutes.",
        detail="Schedule control paused for 60 minutes.",
    )

    events = service.recent(scope="scheduler")
    assert len(events) == 1
    assert events[0].action_type == "suspend_schedule"
    assert events[0].status == "succeeded"
    assert events[0].summary == "DX10 suspended for 60 minutes."
    assert events[0].detail == "Schedule control paused for 60 minutes."
    assert events[0].radio_profile_id == "7"
    assert events[0].target_label == "DX10"
    assert events[0].source_surface == "main_window_schedule_control"


def test_main_window_schedule_control_handlers_publish_feedback() -> None:
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    resume_block = source[source.index("def _on_resume_schedule_clicked") : source.index("def _on_suspend_schedule_clicked")]
    suspend_block = source[source.index("def _on_suspend_schedule_clicked") : source.index("def _selected_sidebar_hold_minutes")]

    assert "_publish_schedule_control_feedback(" in resume_block
    assert 'action_type="resume_schedule"' in resume_block
    assert 'status="succeeded"' in resume_block
    assert 'status="blocked"' in resume_block
    assert "_publish_schedule_control_feedback(" in suspend_block
    assert 'action_type="suspend_schedule"' in suspend_block
    assert 'status="succeeded"' in suspend_block
    assert 'status="blocked"' in suspend_block
    assert 'status="failed"' in resume_block
    assert 'status="failed"' in suspend_block
    assert "resume schedule action failed" in resume_block
    assert "suspend schedule action failed" in suspend_block
    assert "result is False" in resume_block


def test_main_window_resume_failure_publishes_failed_feedback(monkeypatch) -> None:
    from freqinout.gui import main_window as main_window_mod
    from freqinout.gui.main_window import MainWindow

    class _Scheduler:
        def resume_schedule(self) -> None:
            return None

    service = ActionFeedbackService()
    window = MainWindow.__new__(MainWindow)
    window.scheduler = _Scheduler()
    window.settings = object()
    window.action_feedback_service = service
    window._active_runtime_profile = {"id": 7, "name": "DX10"}
    window._schedule_feedback_target = lambda: MainWindow._schedule_feedback_target(window)
    window._publish_schedule_control_feedback = (
        lambda **kwargs: MainWindow._publish_schedule_control_feedback(window, **kwargs)
    )
    monkeypatch.setattr(main_window_mod, "resume_schedule_hold", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("resume boom")))

    MainWindow._on_resume_schedule_clicked(window)

    events = service.recent(scope="scheduler")
    assert len(events) == 1
    assert events[0].action_type == "resume_schedule"
    assert events[0].status == "failed"
    assert events[0].summary == "Resume failed: schedule control could not return to plan."
    assert events[0].detail == "resume boom"
    assert events[0].target_label == "DX10"


def test_main_window_resume_apply_current_entry_false_publishes_failed_feedback(monkeypatch) -> None:
    from freqinout.gui import main_window as main_window_mod
    from freqinout.gui.main_window import MainWindow

    class _Scheduler:
        def __init__(self) -> None:
            self.settings = object()

        def apply_current_entry(self, **_kwargs):
            return False

    service = ActionFeedbackService()
    window = MainWindow.__new__(MainWindow)
    window.scheduler = _Scheduler()
    window.settings = object()
    window.action_feedback_service = service
    window._active_runtime_profile = {"id": 7, "name": "DX10"}
    window._schedule_feedback_target = lambda: MainWindow._schedule_feedback_target(window)
    window._publish_schedule_control_feedback = (
        lambda **kwargs: MainWindow._publish_schedule_control_feedback(window, **kwargs)
    )
    window.on_hold_state_changed = lambda force_reload=False: None
    monkeypatch.setattr(main_window_mod, "set_suspend_until", lambda *_args, **_kwargs: None)

    MainWindow._on_resume_schedule_clicked(window)

    events = service.recent(scope="scheduler")
    assert len(events) == 1
    assert events[0].action_type == "resume_schedule"
    assert events[0].status == "failed"
    assert events[0].summary == "Resume failed: schedule control could not return to plan."
    assert events[0].detail == "Scheduler reported that the current schedule entry could not be applied."
    assert events[0].target_label == "DX10"


def test_main_window_suspend_failure_publishes_failed_feedback(monkeypatch) -> None:
    from freqinout.gui import main_window as main_window_mod
    from freqinout.gui.main_window import MainWindow

    service = ActionFeedbackService()
    window = MainWindow.__new__(MainWindow)
    window.scheduler = object()
    window.settings = object()
    window.action_feedback_service = service
    window._active_runtime_profile = {"id": 7, "name": "DX10"}
    window._schedule_feedback_target = lambda: MainWindow._schedule_feedback_target(window)
    window._publish_schedule_control_feedback = (
        lambda **kwargs: MainWindow._publish_schedule_control_feedback(window, **kwargs)
    )
    window._selected_sidebar_hold_minutes = lambda: 60
    monkeypatch.setattr(main_window_mod, "suspend_snapshot", lambda _settings: {"active": False})
    monkeypatch.setattr(main_window_mod, "suspend_schedule_hold", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("suspend boom")))

    MainWindow._on_suspend_schedule_clicked(window)

    events = service.recent(scope="scheduler")
    assert len(events) == 1
    assert events[0].action_type == "suspend_schedule"
    assert events[0].status == "failed"
    assert events[0].summary == "Suspend failed: schedule control could not pause."
    assert events[0].detail == "suspend boom"
    assert events[0].target_label == "DX10"
