from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.varac_bbs_library_store import (
    list_bbs_artifact_location_ids,
    list_bbs_locations,
    set_bbs_location_artifact,
    upsert_bbs_artifact_path,
    upsert_bbs_location,
)
from freqinout.gui.station_bbs_tab import MAX_ARTIFACT_ROWS, StationBbsTab


@dataclass
class _Settings:
    db_path: str

    def get(self, _key: str, default=None):
        return default


def _qapplication_or_skip():
    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication already exists in this test process.")
    return app or QApplication([])


def _seed_catalog(tmp_path):
    db_path = tmp_path / "freqinout.db"
    source = tmp_path / "reports" / "status.txt"
    source.parent.mkdir()
    source.write_text("station status", encoding="utf-8")
    with connect_sqlite(db_path) as conn:
        with conn:
            upsert_bbs_location(
                conn,
                location_id="public",
                name="Public",
                source_dir=str(tmp_path / "public"),
                access_rule="public",
                retention_mode="expire_after_days",
                retention_days=7,
            )
            upsert_bbs_location(
                conn,
                location_id="restricted",
                name="Restricted",
                source_dir=str(tmp_path / "restricted"),
                access_rule="Allowed callsigns only",
                retention_mode="manual",
                metadata={
                    "visibility_rule": "Allowed callsigns only",
                    "inherit_global_allowed_callsigns": False,
                    "allowed_callsigns": ["N1MAG"],
                },
            )
            artifact_id = upsert_bbs_artifact_path(conn, source_path=source, display_name="Station status")
            set_bbs_location_artifact(conn, location_id="public", artifact_id=artifact_id, publish_enabled=False)
    return _Settings(str(db_path)), source, artifact_id


def _location_item(tab: StationBbsTab, location_id: str):
    root = tab.location_tree.topLevelItem(0)
    pending = [root]
    while pending:
        item = pending.pop()
        if str(item.data(0, Qt.UserRole) or "") == location_id:
            return item
        pending.extend(item.child(index) for index in range(item.childCount()))
    raise AssertionError(f"Location {location_id!r} not found")


def _tree_texts(tree) -> list[str]:
    root = tree.topLevelItem(0)
    pending = [root] if root is not None else []
    values: list[str] = []
    while pending:
        item = pending.pop()
        values.append(item.text(0))
        pending.extend(item.child(index) for index in range(item.childCount()))
    return values


def test_station_bbs_checkbox_replaces_membership_without_deleting_source(tmp_path):
    app = _qapplication_or_skip()
    settings, source, artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    try:
        tab.location_tree.setCurrentItem(_location_item(tab, "public"))
        app.processEvents()
        checkbox = tab.artifact_table.item(0, 0)
        assert checkbox is not None
        assert checkbox.checkState() == Qt.Unchecked
        assert "publish" in checkbox.toolTip().lower()

        checkbox.setCheckState(Qt.Checked)
        app.processEvents()
        with connect_sqlite(settings.db_path) as conn:
            assert list_bbs_artifact_location_ids(conn, artifact_id) == ("public",)
        assert source.exists()

        # A location without a pre-existing mapping still shows catalog files,
        # so the same artifact can be checked into a second managed location.
        tab.location_tree.setCurrentItem(_location_item(tab, "restricted"))
        app.processEvents()
        checkbox = tab.artifact_table.item(0, 0)
        assert checkbox.checkState() == Qt.Unchecked
        checkbox.setCheckState(Qt.Checked)
        app.processEvents()
        with connect_sqlite(settings.db_path) as conn:
            assert list_bbs_artifact_location_ids(conn, artifact_id) == ("public", "restricted")
        assert source.exists()

        checkbox = tab.artifact_table.item(0, 0)
        checkbox.setCheckState(Qt.Unchecked)
        app.processEvents()
        with connect_sqlite(settings.db_path) as conn:
            assert list_bbs_artifact_location_ids(conn, artifact_id) == ("public",)
        assert source.exists()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_station_bbs_reads_are_bounded_and_compact_layout_stacks(monkeypatch, tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    import freqinout.gui.station_bbs_tab as station_bbs_module

    original = station_bbs_module.list_bbs_admin_rows
    calls: list[tuple[str, int]] = []

    def _bounded(conn, *, location_id="", limit=200, offset=0, now_utc=None):
        calls.append((str(location_id), int(limit)))
        return original(conn, location_id=location_id, limit=limit, offset=offset, now_utc=now_utc)

    monkeypatch.setattr(station_bbs_module, "list_bbs_admin_rows", _bounded)
    tab = StationBbsTab(settings=settings)
    try:
        tab.location_tree.setCurrentItem(_location_item(tab, "public"))
        app.processEvents()
        assert calls and all(limit == MAX_ARTIFACT_ROWS for _location_id, limit in calls)
        assert tab.artifact_table.rowCount() <= MAX_ARTIFACT_ROWS

        tab.resize(900, 620)
        tab.show()
        tab.service_tabs.setCurrentWidget(tab.publishing_page)
        app.processEvents()
        assert tab.splitter.orientation() == Qt.Vertical
        assert tab.detail_toggle_btn.isVisible()
        assert not tab.detail_group.isVisible()
        tab.detail_toggle_btn.setChecked(True)
        app.processEvents()
        assert tab.detail_group.isVisible()
        tab.resize(1200, 620)
        app.processEvents()
        assert tab.splitter.orientation() == Qt.Horizontal
        assert not tab.detail_toggle_btn.isVisible()
        assert tab.detail_group.isVisible()
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_main_window_station_bbs_route_uses_managed_bbs_screen():
    from freqinout.gui.main_window import MainWindow

    class _RouteHost:
        _screen_index_by_label = {"Managed BBS": 23}

        def __init__(self):
            self.selected = []

        def _set_screen(self, index):
            self.selected.append(index)

    host = _RouteHost()
    MainWindow.open_station_bbs(host)
    assert host.selected == [23]


def test_messages_manage_bbs_prefers_station_workspace(monkeypatch):
    from freqinout.gui.message_viewer_tab import MessageViewerTab
    import freqinout.gui.message_viewer_tab as message_viewer_module

    class _Host:
        def __init__(self):
            self.opened = 0

        def open_station_bbs(self):
            self.opened += 1

    host = _Host()
    monkeypatch.setattr(message_viewer_module, "resolve_help_host", lambda _widget: host)
    MessageViewerTab._open_varac_bbs_manager(object())
    assert host.opened == 1


def test_station_location_editor_saves_shared_catalog_and_second_tab_sees_it(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    first = StationBbsTab(settings=settings)
    second = StationBbsTab(settings=settings)
    try:
        first._begin_new_location()
        first.location_name_edit.setText("Field Intel")
        first.location_source_edit.setText(str(tmp_path / "managed" / "field-intel"))
        first.location_access_combo.setCurrentIndex(first.location_access_combo.findData("Allowed callsigns only"))
        first.location_retention_combo.setCurrentIndex(first.location_retention_combo.findData("expire_after_days"))
        first.location_retention_days_spin.setValue(21)
        first._save_location()
        app.processEvents()

        with connect_sqlite(settings.db_path) as conn:
            location = next(row for row in list_bbs_locations(conn) if row.location_id == "field-intel")
        assert location.name == "Field Intel"
        assert location.source_dir.endswith("managed/field-intel")
        assert location.access_rule == "Allowed callsigns only"
        assert location.retention_mode == "expire_after_days"
        assert location.retention_days == 21

        second.refresh_catalog()
        assert _location_item(second, "field-intel").text(0).startswith("Field Intel")
    finally:
        first.deleteLater()
        second.deleteLater()
        app.processEvents()


def test_station_location_disable_is_non_destructive_and_remains_visible(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    try:
        tab.location_tree.setCurrentItem(_location_item(tab, "restricted"))
        tab.location_edit_btn.setChecked(True)
        app.processEvents()
        tab._disable_location()
        app.processEvents()

        with connect_sqlite(settings.db_path) as conn:
            location = next(row for row in list_bbs_locations(conn) if row.location_id == "restricted")
        assert location.enabled is False
        assert "Disabled" in _location_item(tab, "restricted").text(0)
        assert "remains visible" in tab.location_editor_status.text()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_artifact_details_display_source_kind(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    try:
        tab.location_tree.setCurrentItem(_location_item(tab, "public"))
        app.processEvents()
        tab.artifact_table.selectRow(0)
        app.processEvents()
        assert tab.detail_labels["origin"].text() == "Operator File"
        assert tab.artifact_table.horizontalHeaderItem(3).text() == "Age"
        assert tab.artifact_table.item(0, 3).text().endswith("d")
        assert "Local " in tab.detail_labels["age"].text()
        assert "UTC " in tab.detail_labels["age"].text()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_visitor_preview_filters_tree_and_is_read_only(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, artifact_id = _seed_catalog(tmp_path)
    with connect_sqlite(settings.db_path) as conn:
        with conn:
            set_bbs_location_artifact(conn, location_id="public", artifact_id=artifact_id, publish_enabled=True)
    tab = StationBbsTab(settings=settings)
    try:
        tab.service_tabs.setCurrentWidget(tab.visitor_preview_page)
        app.processEvents()
        assert not any(text.startswith("Restricted") for text in _tree_texts(tab.visitor_preview_tree))

        tab.visitor_callsign_edit.setText("N1MAG")
        app.processEvents()
        assert any(text.startswith("Restricted") for text in _tree_texts(tab.visitor_preview_tree))
        assert tab.visitor_artifact_table.rowCount() == 1
        file_item = tab.visitor_artifact_table.item(0, 0)
        assert file_item is not None
        assert not bool(file_item.flags() & Qt.ItemIsUserCheckable)
        assert "read-only" in file_item.toolTip().lower()
    finally:
        tab.deleteLater()
        app.processEvents()


def test_location_access_code_is_hashed_and_not_stored_as_plaintext(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    try:
        tab._begin_new_location()
        tab.location_name_edit.setText("Secure")
        tab.location_access_combo.setCurrentIndex(
            tab.location_access_combo.findData("Access code required")
        )
        tab.location_code_edit.setText("FIELD-42")
        tab.location_code_confirm_edit.setText("FIELD-42")
        tab._save_location()
        app.processEvents()

        with connect_sqlite(settings.db_path) as conn:
            location = next(row for row in list_bbs_locations(conn) if row.location_id == "secure")
        assert location.access_rule == "Access code required"
        assert location.metadata.get("access_code_hash")
        assert location.metadata.get("access_code_salt")
        assert "FIELD-42" not in str(location.metadata)
    finally:
        tab.deleteLater()
        app.processEvents()


def test_bbs_guided_tabs_and_system_helpers_are_separate_from_publishing(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    helper = tmp_path / "00 READ FIRST - type command, then refresh BBS.txt"
    helper.write_text("system helper", encoding="utf-8")
    with connect_sqlite(settings.db_path) as conn:
        with conn:
            helper_id = upsert_bbs_artifact_path(conn, source_path=helper, display_name=helper.name)
            set_bbs_location_artifact(conn, location_id="public", artifact_id=helper_id, publish_enabled=True)

    tab = StationBbsTab(settings=settings)
    try:
        assert [tab.service_tabs.tabText(index) for index in range(tab.service_tabs.count())] == [
            "Overview",
            "Radio Service",
            "Locations & Access",
            "Publishing",
            "Visitor Preview",
            "System Helpers",
        ]
        publishing_names = [
            tab.artifact_table.item(row, 1).text()
            for row in range(tab.artifact_table.rowCount())
        ]
        assert helper.name not in publishing_names
        assert tab.helpers_table.rowCount() == 1
        assert tab.helpers_table.item(0, 0).text() == helper.stem
        assert tab.helpers_table.item(0, 2).text() == helper.name
        assert tab.helpers_table.horizontalHeaderItem(4).text() == "Age"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_radio_service_saves_bbs_fields_without_changing_native_varac_paths(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    store = MultiRadioStore(Path(settings.db_path))
    profile = store.save_device_profile(
        {
            "name": "FIO-A",
            "system_key": "fio-a",
            "use_varac": 1,
            "varac_install_path": "/native/varac",
            "varac_outbox_dir": "/native/outbox",
        }
    )
    tab = StationBbsTab(settings=settings)
    try:
        assert tab.radio_service_table.rowCount() == 1
        tab.radio_publish_enabled_chk.setChecked(True)
        tab.radio_service_enabled_chk.setChecked(False)
        tab.radio_live_dir_edit.setText(str(tmp_path / "live-bbs"))
        tab._save_selected_radio_service()
        assert "Enable VarAC BBS" in tab.radio_service_status.text()

        tab.radio_service_enabled_chk.setChecked(True)
        tab.radio_announce_enabled_chk.setChecked(True)
        tab._save_selected_radio_service()
        saved = store.get_device_profile(int(profile["id"]))
        assert saved is not None
        assert saved["varac_bbs_dir"] == str(tmp_path / "live-bbs")
        assert bool(saved["varac_bbs_enabled"]) is True
        assert bool(saved["varac_bbs_vault_enabled"]) is True
        assert bool(saved["varac_bbs_announce_enabled"]) is True
        assert saved["varac_install_path"] == "/native/varac"
        assert saved["varac_outbox_dir"] == "/native/outbox"
    finally:
        tab.deleteLater()
        app.processEvents()


def test_disabled_location_is_not_a_publishing_target(tmp_path):
    app = _qapplication_or_skip()
    settings, _source, _artifact_id = _seed_catalog(tmp_path)
    tab = StationBbsTab(settings=settings)
    try:
        tab.location_tree.setCurrentItem(_location_item(tab, "restricted"))
        tab.location_edit_btn.setChecked(True)
        tab._disable_location()
        app.processEvents()
        assert tab.publishing_location_combo.findData("restricted") == -1
        assert tab._publishing_location_id != "restricted"
    finally:
        tab.deleteLater()
        app.processEvents()
