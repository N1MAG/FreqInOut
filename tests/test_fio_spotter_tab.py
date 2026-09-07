from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QScrollArea

from freqinout.core import fio_spotter_store
from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab


class _Settings:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def get(self, key: str, default=None):
        return self.values.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.values[key] = value

    def save(self) -> None:
        pass


def _app():
    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication is already active.")
    return app or QApplication([])


def test_spotter_tab_has_lazy_browser_tabs_in_service_order():
    app = _app()
    tab = FioSpotterTab(settings=_Settings())
    try:
        assert [tab.tabs.tabText(i) for i in range(tab.tabs.count())] == [
            "Activity", "Watches", "Expect", "Forms", "Imports",
        ]
        assert tab._built == {0}
        tab.tabs.setCurrentIndex(2)
        app.processEvents()
        assert 2 in tab._built
        assert tab.expect_entries_table.rowCount() <= 200
        assert tab.tabs.currentWidget().findChild(QScrollArea) is not None
    finally:
        tab.deleteLater()


def test_compact_expect_page_scrolls_without_expanding_shell_height():
    app = _app()
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(900, 560)
        tab.tabs.setCurrentIndex(2)
        tab.show()
        app.processEvents()
        assert tab.height() == 560
        scroll = tab.tabs.currentWidget().findChild(QScrollArea)
        assert scroll is not None
        assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert scroll.horizontalScrollBar().maximum() == 0
        assert scroll.verticalScrollBar().maximum() > 0
    finally:
        tab.close()
        tab.deleteLater()


def test_spotter_navigation_helper_uses_internal_route_without_window_setup():
    from freqinout.gui.main_window import MainWindow

    class _Host:
        _screen_index_by_label = {"FIO Spotter": 7}
        opened: list[int] = []

        def _set_screen(self, index: int) -> None:
            self.opened.append(index)

    host = _Host()
    MainWindow.open_fio_spotter(host)
    assert host.opened == [7]


def test_expect_editor_persists_policy_source_and_schedule(monkeypatch, tmp_path):
    app = _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = _Settings()
    tab = FioSpotterTab(settings=settings)
    try:
        tab.tabs.setCurrentIndex(2)
        app.processEvents()
        tab.policy_name.setText("Regional hubs")
        tab.policy_calls.setText("K1ABC")
        tab.policy_groups.setText("@MAGNET")
        tab.policy_scope.setCurrentText("radio")
        tab.policy_radios.setText("7")
        tab._save_policy()
        policies = spotter_ui.list_expect_allow_policies()
        assert policies[0]["allowed_groups"] == ["@MAGNET"]
        assert policies[0]["source_radio_ids"] == ["7"]

        tab.expect_key.setText("INFO")
        tab.expect_reply.setText("STATUS GREEN")
        tab.expect_policy.setCurrentIndex(tab.expect_policy.findData(policies[0]["id"]))
        tab.expect_source_scope.setCurrentText("radio")
        tab.expect_source_radio.setText("7")
        tab.expect_js8_instance.setText("fio-a")
        tab.expect_schedule.setText("18:00-23:00Z")
        tab.expect_enabled.setChecked(True)
        tab._save_entry()
        entries = spotter_ui.list_expect_entries()
        assert entries[0]["source_radio_id"] == "7"
        assert entries[0]["js8_instance_id"] == "fio-a"
        assert entries[0]["auto_tx_schedule"] == "18:00-23:00Z"
    finally:
        tab.deleteLater()


def test_compact_navigation_exposes_spotter_icon_route():
    from freqinout.gui.main_window import MainWindow

    assert ("Spotter", "FIO Spotter", "FIO Spotter", "spotter.svg") in MainWindow._compact_navigation_specs()


def test_watches_editor_uses_shared_bounded_store_for_save_toggle_delete_and_test(tmp_path, monkeypatch):
    """The UI owns no watch list: every editor action reaches the core store."""
    app = _app()
    db_path = tmp_path / "spotter.db"
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: [])
    monkeypatch.setattr(
        spotter_ui, "list_spotter_watches",
        lambda *, limit: fio_spotter_store.list_spotter_watches(db_path=db_path, limit=limit),
    )
    monkeypatch.setattr(
        spotter_ui, "save_spotter_watch",
        lambda values: fio_spotter_store.save_spotter_watch(values, db_path=db_path),
    )
    monkeypatch.setattr(
        spotter_ui, "delete_spotter_watch",
        lambda watch_id: fio_spotter_store.delete_spotter_watch(watch_id, db_path=db_path),
    )
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.tabs.setCurrentIndex(1)
        app.processEvents()
        tab.watch_name.setText("Smoke")
        tab.watch_pattern.setText("smoke")
        tab.watch_sources.setText("spotter, js8")
        tab._save_watch()
        rows = fio_spotter_store.list_spotter_watches(db_path=db_path)
        assert rows[0]["name"] == "Smoke"
        assert rows[0]["source_families"] == ["spotter", "js8"]
        assert tab.watches_table.rowCount() == 1

        tab.activity_table.setRowCount(1)
        tab._put(tab.activity_table, 0, 0, "now", data={"body_text": "Smoke reported"})
        tab.activity_table.selectRow(0)
        tab.watches_table.selectRow(0)
        tab._test_watch()
        assert "matched" in tab.watch_status.text().lower()

        tab._toggle_watch()
        assert fio_spotter_store.list_spotter_watches(db_path=db_path)[0]["enabled"] is False
        tab.watches_table.selectRow(0)
        tab._delete_watch()
        assert fio_spotter_store.list_spotter_watches(db_path=db_path) == []
    finally:
        tab.deleteLater()


def test_forms_folder_and_import_preview_use_canonical_settings_keys(tmp_path, monkeypatch):
    app = _app()
    settings = _Settings()
    forms_dir = tmp_path / "forms"; forms_dir.mkdir()
    (forms_dir / "MCF307.txt").write_text(
        "Weather report\n? Conditions\n@1 Clear\n@2 Severe\n",
        encoding="utf-8",
    )
    source = tmp_path / "js8spotter.db"; source.touch()
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: [])
    preview = SimpleNamespace(
        source_db=str(source), candidates=4, forms=1, expect=2, archive=1,
        duplicates=0, skipped=0, conflicts=0, warnings=(),
    )
    monkeypatch.setattr(spotter_ui, "preview_js8spotter_import", lambda *_args, **_kwargs: preview)
    tab = FioSpotterTab(settings=settings)
    try:
        tab.tabs.setCurrentIndex(3); app.processEvents()
        tab.forms_path.setText(str(forms_dir)); tab._use_forms_folder()
        assert settings.values["js8_forms_path"] == str(forms_dir)
        assert "preserved" in tab.forms_state.text().lower()
        tab.refresh_forms()
        assert tab.forms_table.rowCount() == 1
        tab.forms_table.selectRow(0)
        tab._auto_classify_forms()
        tab._save_form_mappings()
        assert settings.values["js8_spotter_form_mappings"][0]["form_code"] == "F!307"
        assert settings.values["js8_spotter_form_mappings"][0]["alert"] is True
        assert "Weather report" in tab.forms_preview.toPlainText()

        tab.tabs.setCurrentIndex(4); app.processEvents()
        tab.import_source.setText(str(source)); tab._preview_import()
        assert settings.values["js8spotter_import_db_path"] == str(source)
        assert "4 candidates" in tab.imports_state.text()
    finally:
        tab.deleteLater()


def test_forms_compose_action_uses_main_shell_handoff(monkeypatch):
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: [])
    opened: list[str] = []
    tab = FioSpotterTab(settings=_Settings(), open_compose=lambda: opened.append("compose"))
    try:
        tab.tabs.setCurrentIndex(3)
        app.processEvents()
        tab._open_spotter_compose()
        assert opened == ["compose"]
    finally:
        tab.deleteLater()


def test_activity_actions_pass_selected_shared_projection(monkeypatch):
    app = _app()
    row = {
        "message_id": "spotter:1",
        "from_call": "K1ABC",
        "group_name": "MAGNET",
        "source_family": "spotter",
    }
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: [row])
    opened: list[tuple[str, str]] = []
    tab = FioSpotterTab(
        settings=_Settings(),
        open_inbox=lambda value: opened.append(("inbox", value["message_id"])),
        open_map=lambda value: opened.append(("map", value["message_id"])),
        open_operator=lambda value: opened.append(("operator", value["message_id"])),
    )
    try:
        tab.activity_table.selectRow(0)
        app.processEvents()
        tab._open_selected_activity_inbox()
        tab._open_selected_activity_map()
        tab._open_selected_activity_operator()
        assert opened == [
            ("inbox", "spotter:1"),
            ("map", "spotter:1"),
            ("operator", "spotter:1"),
        ]
    finally:
        tab.deleteLater()
