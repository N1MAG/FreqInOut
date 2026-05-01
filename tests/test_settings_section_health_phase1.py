from __future__ import annotations

import os
import sys

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _nav_item(tab, title: str):
    for idx in range(tab.sections_nav_list.count()):
        item = tab.sections_nav_list.item(idx)
        if item and item.text() == title:
            return item
    raise AssertionError(f"Missing settings nav item: {title}")


def test_settings_section_health_marks_only_core_sections_warn_on_fresh_profile(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        freqinout_item = _nav_item(tab, "FreqInOut Settings")
        groups_item = _nav_item(tab, "HF Operating Groups")
        js8_item = _nav_item(tab, "JS8Call Settings")
        fast_light_item = _nav_item(tab, "Fast Light Settings")
        varac_item = _nav_item(tab, "VarAC Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert freqinout_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "Callsign missing" in str(freqinout_item.toolTip())
    assert groups_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "No HF operating groups configured" in str(groups_item.toolTip())
    assert js8_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "neutral"
    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "neutral"
    assert varac_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "neutral"


def test_settings_section_health_warns_for_partial_js8_spotter_setup(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.js8spotter_path_edit.setText("/tmp/js8spotter")
        app.processEvents()
        js8_item = _nav_item(tab, "JS8Call Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert js8_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "JS8Spotter forms path missing" in str(js8_item.toolTip())


def test_settings_section_health_warns_when_directed_txt_missing_after_js8_setup(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.js8call_path_edit.setText("/tmp/JS8Call")
        app.processEvents()
        js8_item = _nav_item(tab, "JS8Call Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert js8_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "JS8Call DIRECTED.TXT path missing" in str(js8_item.toolTip())


def test_settings_section_health_warns_when_js8call_install_folder_missing_host(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.js8call_path_edit.setText("/tmp/JS8Call")
        tab.js8_host_edit.setText("")
        app.processEvents()
        js8_item = _nav_item(tab, "JS8Call Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert js8_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "JS8Call TCP host missing" in str(js8_item.toolTip())


def test_settings_section_health_warns_when_js8call_install_folder_missing_tcp_port(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab
    from freqinout.gui.theme import resolve_theme

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        for idx in range(tab.sections_nav_list.count()):
            item = tab.sections_nav_list.item(idx)
            if item and item.text() == "JS8Call Settings":
                tab.sections_nav_list.setCurrentRow(idx)
                break
        app.processEvents()
        tab.js8call_path_edit.setText("/tmp/JS8Call")
        tab.js8_port_edit.setText("")
        app.processEvents()
        js8_item = _nav_item(tab, "JS8Call Settings")
        js8_group = next(group for group, meta in tab._section_meta.items() if meta.get("title") == "JS8Call Settings")
        header_btn = tab._section_meta[js8_group]["header_btn"]
        header_style = header_btn.styleSheet()
        warning_color = resolve_theme(tab.settings)["warning"].lower()
        selected_visuals = tab._section_nav_visuals("warn", selected=True, hovered=False, theme=resolve_theme(tab.settings))
        unselected_visuals = tab._section_nav_visuals(
            "warn",
            selected=False,
            hovered=False,
            theme=resolve_theme(tab.settings),
        )
    finally:
        tab.deleteLater()
        app.processEvents()

    assert js8_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "JS8Call TCP port missing" in str(js8_item.toolTip())
    assert warning_color in header_style.lower()
    assert selected_visuals["border"].name().lower() == warning_color
    assert unselected_visuals["border"].name().lower() == warning_color
    assert selected_visuals["bg"].alpha() > 0
    assert unselected_visuals["bg"].alpha() > 0


def test_settings_section_health_allows_js8spotter_with_forms_without_directed(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.js8spotter_path_edit.setText("/tmp/js8spotter")
        tab.js8_forms_edit.setText("/tmp/js8spotter-forms")
        app.processEvents()
        js8_item = _nav_item(tab, "JS8Call Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert js8_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) != "warn"
    assert "JS8Call DIRECTED.TXT path missing" not in str(js8_item.toolTip())
    assert "JS8Spotter forms path missing" not in str(js8_item.toolTip())


def test_settings_section_health_allows_commstat_without_forms(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.commstat_path_edit.setText("/tmp/commstat")
        app.processEvents()
        js8_item = _nav_item(tab, "JS8Call Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert js8_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) != "warn"
    assert "JS8Spotter forms path missing for CommStat" not in str(js8_item.toolTip())
    assert "JS8Call DIRECTED.TXT path missing" not in str(js8_item.toolTip())


def test_settings_section_health_warns_for_partial_varac_setup(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.msg_paths_edits["varac"].setText("/tmp/varac-incoming")
        app.processEvents()
        varac_item = _nav_item(tab, "VarAC Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert varac_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "Install folder or launch override missing" in str(varac_item.toolTip())


def test_varac_cluster_mode_hides_and_reveals_cluster_sections(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        clusters_item = _nav_item(tab, "VarAC Clusters")
        memberships_item = _nav_item(tab, "VarAC Memberships")
        assert clusters_item.isHidden() is True
        assert memberships_item.isHidden() is True

        tab.varac_cluster_mode_chk.setChecked(True)
        app.processEvents()

        assert clusters_item.isHidden() is False
        assert memberships_item.isHidden() is False
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_section_health_warns_when_varac_install_folder_missing_incoming_files(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.varac_path_edit.setText("/tmp/VarAC")
        app.processEvents()
        varac_item = _nav_item(tab, "VarAC Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert varac_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "VarAC incoming files path missing" in str(varac_item.toolTip())


def test_settings_section_health_warns_for_partial_fast_light_message_setup(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.msg_paths_edits["flmsg"].setText("/tmp/flmsg-messages")
        app.processEvents()
        fast_light_item = _nav_item(tab, "Fast Light Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "FLMsg executable path missing" in str(fast_light_item.toolTip())


def test_settings_section_health_warns_when_flmsg_executable_missing_message_path(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.path_edits["FLMsg"].setText("/tmp/flmsg")
        app.processEvents()
        fast_light_item = _nav_item(tab, "Fast Light Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "FLMsg ICS/Messages path missing" in str(fast_light_item.toolTip())


def test_settings_section_health_warns_when_flamp_executable_missing_rx_path(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.path_edits["FLAmp"].setText("/tmp/flamp")
        app.processEvents()
        fast_light_item = _nav_item(tab, "Fast Light Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "FLAmp FLAMP/rx path missing" in str(fast_light_item.toolTip())


def test_settings_section_health_warns_when_flrig_executable_missing_xmlrpc_port(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.path_edits["FLRig"].setText("/tmp/flrig")
        tab.flrig_port_edit.setText("")
        app.processEvents()
        fast_light_item = _nav_item(tab, "Fast Light Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "FLRig XML-RPC port missing" in str(fast_light_item.toolTip())


def test_settings_section_health_warns_when_fldigi_executable_missing_host(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.path_edits["FLDigi"].setText("/tmp/fldigi")
        tab.fldigi_host_edit.setText("")
        app.processEvents()
        fast_light_item = _nav_item(tab, "Fast Light Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "FLDigi XML-RPC host missing" in str(fast_light_item.toolTip())


def test_settings_section_health_warns_when_fldigi_executable_missing_port(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.path_edits["FLDigi"].setText("/tmp/fldigi")
        tab.fldigi_port_edit.setText("")
        app.processEvents()
        fast_light_item = _nav_item(tab, "Fast Light Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "FLDigi XML-RPC port missing" in str(fast_light_item.toolTip())


def test_settings_section_health_allows_fldigi_log_path_without_fldigi_exe(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        tab.fldigi_log_path_edit.setText("/tmp/fldigi-logs")
        app.processEvents()
        fast_light_item = _nav_item(tab, "Fast Light Settings")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert fast_light_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) != "warn"
    assert "FLDigi executable path missing" not in str(fast_light_item.toolTip())


def test_js8_settings_layout_shows_install_folder_before_directed(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication, QLabel

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        js8_group = next(group for group, meta in tab._section_meta.items() if meta.get("title") == "JS8Call Settings")
        content = tab._section_meta[js8_group]["content"]
        layout = content.layout()
        install_row = layout.itemAt(2).widget()
        directed_row = layout.itemAt(3).widget()
        install_label = install_row.findChild(QLabel)
        directed_label = directed_row.findChild(QLabel)
    finally:
        tab.deleteLater()
        app.processEvents()

    assert install_label is not None
    assert directed_label is not None
    assert install_label.text() == "JS8Call Install Folder:"
    assert directed_label.text() == "JS8Call DIRECTED.TXT:"


def test_settings_nav_delegate_size_hint_leaves_room_for_descenders(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication, QStyleOptionViewItem

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        option = QStyleOptionViewItem()
        option.font = tab.sections_nav_list.font()
        option.fontMetrics = tab.sections_nav_list.fontMetrics()
        index = tab.sections_nav_list.model().index(0, 0)
        size = tab.sections_nav_list.itemDelegate().sizeHint(option, index)
    finally:
        tab.deleteLater()
        app.processEvents()

    assert size.height() >= tab.sections_nav_list.fontMetrics().height() + 8
