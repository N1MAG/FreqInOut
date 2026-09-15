from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication, QCheckBox, QLabel, QScrollArea, QTableWidgetItem, QWidget

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


class _RadioStore:
    def list_device_profiles(self):
        return [
            {"id": 7, "name": "FTDX-10", "enabled": 1, "use_js8call": 1, "js8_instance_id": 11},
            {"id": 8, "name": "IC-7300", "enabled": 1, "use_js8call": 0, "js8_instance_id": None},
        ]


def _app():
    app = QApplication.instance()
    if app is not None and not isinstance(app, QApplication):
        pytest.skip("A non-GUI QCoreApplication is already active.")
    return app or QApplication([])


def _set_app_text_scale(app: QApplication, scale: float) -> QFont:
    old_font = QFont(app.font())
    new_font = QFont(old_font)
    point_size = float(old_font.pointSizeF())
    if point_size > 0:
        new_font.setPointSizeF(point_size * float(scale))
    else:
        pixel_size = int(old_font.pixelSize() or 0)
        new_font.setPixelSize(max(12, int(round((pixel_size or 12) * float(scale)))))
    app.setFont(new_font)
    return old_font


def _select_tab(tab: FioSpotterTab, name: str) -> None:
    index = next(i for i in range(tab.tabs.count()) if tab.tabs.tabText(i) == name)
    tab.tabs.setCurrentIndex(index)


def test_spotter_tab_has_lazy_browser_tabs_in_service_order():
    app = _app()
    tab = FioSpotterTab(settings=_Settings(), radio_store=_RadioStore())
    try:
        assert [tab.tabs.tabText(i) for i in range(tab.tabs.count())] == [
            "Watches", "Expect", "Access Policies", "Forms", "Imports",
        ]
        assert tab._built == {0}
        assert not hasattr(tab, "activity_open_spotter_inbox")
        _select_tab(tab, "Expect")
        app.processEvents()
        assert 1 in tab._built
        assert tab.expect_entries_table.rowCount() <= 200
        assert tab.tabs.currentWidget().findChild(QScrollArea) is not None
    finally:
        tab.deleteLater()


def test_default_watch_page_defers_its_store_read_until_activation(monkeypatch):
    calls: list[int] = []
    monkeypatch.setattr(
        spotter_ui,
        "list_spotter_watches",
        lambda **_kwargs: calls.append(1) or [],
    )
    tab = FioSpotterTab(settings=_Settings())
    try:
        assert calls == []
        assert tab.tabs.tabText(tab.tabs.currentIndex()) == "Watches"
        tab.set_tab_active(True)
        tab.set_tab_active(True)
        assert calls == [1]
    finally:
        tab.deleteLater()


def test_open_expect_entry_refreshes_once_and_selects_requested_row(monkeypatch):
    app = _app()
    tab = FioSpotterTab(settings=_Settings())
    refresh_calls: list[int] = []
    try:
        # Build the page without touching the real store, then provide a
        # bounded row exactly as the store projector would.
        _select_tab(tab, "Expect")
        app.processEvents()
        tab.expect_entries_table.setRowCount(1)
        row = {"id": 42, "expect_key": "F!304", "response_text": "F!304 OK"}
        item = QTableWidgetItem("●")
        item.setData(Qt.UserRole, row)
        tab.expect_entries_table.setItem(0, 0, item)

        def refresh():
            refresh_calls.append(1)
            tab._select_pending_expect_entry()

        monkeypatch.setattr(tab, "refresh_expect", refresh)
        tab.open_expect_entry(entry_id=42)
        app.processEvents()
        assert refresh_calls == [1]
        assert tab.expect_entries_table.currentRow() == 0
        assert tab._pending_expect_entry_id == 0
    finally:
        tab.deleteLater()


def test_bulk_expect_date_action_confirms_once_refreshes_once_and_preserves_selection(monkeypatch):
    app = _app()
    tab = FioSpotterTab(settings=_Settings())
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        eligible = {"id": 7, "expect_key": "F!304", "response_text": "F!304 OK #ABCD"}
        other = {"id": 8, "expect_key": "Q", "response_text": "Q ABCD"}
        tab._entry_rows = [eligible, other]
        monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [eligible, other])
        tab.expect_entries_table.setRowCount(1)
        item = QTableWidgetItem("●")
        item.setData(Qt.UserRole, eligible)
        tab.expect_entries_table.setItem(0, 0, item)
        tab.expect_entries_table.selectRow(0)
        bulk_calls: list[list[int]] = []
        monkeypatch.setattr(spotter_ui, "bulk_refresh_expect_datecodes", lambda **kwargs: (bulk_calls.append(kwargs["entry_ids"]) or SimpleNamespace(updated_count=1, skipped_count=0)))
        refresh_calls: list[int] = []
        monkeypatch.setattr(tab, "refresh_expect", lambda: refresh_calls.append(1))
        monkeypatch.setattr(spotter_ui.QMessageBox, "question", lambda *args, **kwargs: spotter_ui.QMessageBox.Yes)

        tab._bulk_update_expect_dates()
        assert bulk_calls == [[7]]
        assert refresh_calls == [1]
        assert tab._selected_expect_entry_id() == 7
    finally:
        tab.deleteLater()


def test_bulk_expect_date_action_noop_does_not_write(monkeypatch):
    app = _app()
    tab = FioSpotterTab(settings=_Settings())
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        tab._entry_rows = [{"id": 8, "expect_key": "Q", "response_text": "Q ABCD"}]
        monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: list(tab._entry_rows))
        writes: list[int] = []
        monkeypatch.setattr(spotter_ui, "bulk_refresh_expect_datecodes", lambda **kwargs: writes.append(1))
        monkeypatch.setattr(spotter_ui, "update_mcform_response_datecode", lambda *args, **kwargs: None)
        tab._bulk_update_expect_dates()
        assert writes == []
        assert "No eligible" in tab.expect_maintenance_state.text()
    finally:
        tab.deleteLater()


def test_forms_action_stages_selected_form_in_expect(monkeypatch):
    app = _app()
    intents: list[dict] = []
    tab = FioSpotterTab(settings=_Settings(), open_compose=lambda intent: intents.append(intent))
    try:
        _select_tab(tab, "Forms")
        app.processEvents()
        tab.forms_table.setRowCount(1)
        item = QTableWidgetItem("F!304")
        item.setData(Qt.UserRole, {"form_code": "F!304", "title": "Situation"})
        tab.forms_table.setItem(0, 0, item)
        tab.forms_table.selectRow(0)
        monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
        tab._configure_selected_form_expect()
        assert intents == [{
            "mode": "spotter", "transport": "spotter", "source": "fio_spotter_form_expect",
            "source_label": "Forms", "expect_entry_id": 0, "expect_key": "F!304",
            "spotter_form_code": "F!304", "expect_view": False, "expect_create": True,
        }]
        assert "Saved-only" in tab.forms_state.text()
    finally:
        tab.deleteLater()


def test_compact_expect_page_scrolls_without_expanding_shell_height():
    app = _app()
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(900, 560)
        _select_tab(tab, "Expect")
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


@pytest.mark.parametrize("scale", [1.0, 1.25])
def test_expect_editor_keeps_narrow_layout_and_text_controls_readable(scale):
    app = _app()
    old_font = _set_app_text_scale(app, scale)
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(900, 560)
        _select_tab(tab, "Expect")
        tab.show()
        app.processEvents()

        assert tab.height() == 560
        assert tab.expect_editor_split.orientation() == Qt.Vertical
        scroll = tab.tabs.currentWidget().findChild(QScrollArea)
        assert scroll is not None
        assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert scroll.horizontalScrollBar().maximum() == 0
        assert scroll.verticalScrollBar().maximum() > 0

        expect_controls = (
            tab.expect_key,
            tab.expect_policy,
            tab.expect_calls,
            tab.expect_groups,
            tab.expect_blocked,
            tab.expect_trusted_groups,
            tab.expect_radio,
            tab.expect_max,
            tab.expect_cooldown,
        )
        assert all(widget.height() >= widget.sizeHint().height() for widget in expect_controls)
        # Reply deliberately uses a bounded multiline editor rather than a
        # one-line input. QTextEdit's default size hint is intentionally much
        # taller than the operator-facing three-line working surface.
        assert tab.expect_reply.height() >= tab.expect_reply.fontMetrics().lineSpacing() * 3

        _select_tab(tab, "Access Policies")
        app.processEvents()
        policy_controls = (
            tab.policy_manage,
            tab.policy_name,
            tab.policy_calls,
            tab.policy_groups,
            tab.policy_trusted_groups,
            tab.policy_blocked,
            tab.policy_scope,
            tab.policy_radios,
        )
        assert all(widget.height() >= widget.sizeHint().height() for widget in policy_controls)

        _select_tab(tab, "Expect")
        app.processEvents()

        # Inline access is not part of the normal editor. It is disclosed only
        # for an existing compatible rule, where its chips remain editable.
        legacy = {
            "id": 3, "expect_key": "STATUS", "response_text": "READY",
            "allowed_callsigns": ["K1ABC", "K2DEF", "K3GHI"],
            "allowed_groups": ["@REGION", "@LOCAL", "@CUSTOM"],
            "trusted_operator_groups": ["REGION", "LOCAL", "SUPPORT"],
            "blocked_callsigns": ["K4JKL", "K5MNO"], "max_replies": 1,
            "cooldown_seconds": 0, "auto_reply_state": "saved-only",
        }
        tab._set_expect_legacy_access_for_row(legacy)
        tab.expect_legacy_access.setChecked(True)
        tab.expect_calls.setText(", ".join(legacy["allowed_callsigns"]))
        tab.expect_groups.setText(", ".join(legacy["allowed_groups"]))
        tab.expect_trusted_groups.setText(", ".join(legacy["trusted_operator_groups"]))
        tab.expect_blocked.setText(", ".join(legacy["blocked_callsigns"]))
        app.processEvents()
        assert scroll.horizontalScrollBar().maximum() == 0
        assert tab.expect_editor_split.height() >= tab.expect_editor_split.minimumHeight()
        for editor in (
            tab.expect_calls,
            tab.expect_groups,
            tab.expect_trusted_groups,
            tab.expect_blocked,
        ):
            assert editor.chip_scroll.isVisible()
            assert editor.chip_scroll.height() >= max(
                chip.sizeHint().height() for chip in editor._chip_buttons
            )
            assert editor.height() >= editor.sizeHint().height()
    finally:
        tab.close()
        tab.deleteLater()
        app.setFont(old_font)
        app.processEvents()


def test_expect_editor_recovers_when_selected_policy_disappears(monkeypatch):
    app = _app()
    empty_rows = []
    policy_rows = [{"id": 7, "name": "Regional trusted", "enabled": True}]

    calls = {"count": 0}

    def _list_policies(**_kwargs):
        calls["count"] += 1
        return policy_rows if calls["count"] == 1 else empty_rows

    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", _list_policies)
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda *, limit: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])

    tab = FioSpotterTab(settings=_Settings())
    try:
        _select_tab(tab, "Access Policies")
        app.processEvents()
        tab.policy_manage.setCurrentIndex(tab.policy_manage.findData(7))
        app.processEvents()
        assert tab.policy_name.text() == "Regional trusted"

        tab.refresh_policies()
        app.processEvents()

        assert tab.policy_manage.currentData() == 0
        assert tab.policy_manage.currentText() == "New policy"
        assert tab.policy_name.text() == ""
        assert tab.policy_trusted_groups.text() == ""
        _select_tab(tab, "Expect")
        app.processEvents()
        tab.refresh_expect()
        assert tab.expect_policy.currentData() == 0
        assert tab.expect_policy.findData(0) == 0
    finally:
        tab.close()
        tab.deleteLater()


def test_enabling_dynamic_flamp_queues_an_immediate_background_projection(monkeypatch):
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda *, limit: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])
    requested: list[tuple[str, ...]] = []
    host = QWidget()
    host.background_ingest = SimpleNamespace(  # type: ignore[attr-defined]
        request_refresh=lambda *kinds: requested.append(tuple(kinds))
    )
    tab = FioSpotterTab(parent=host, settings=_Settings())
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        tab.dynamic_flamp_enabled.setChecked(True)
        app.processEvents()

        assert requested == [("dynamic_flamp",)]
    finally:
        host.close()
        host.deleteLater()


def test_expect_editor_typing_and_policy_selection_do_not_requery_after_lazy_load(monkeypatch):
    app = _app()
    counts = {
        "catalog": 0,
        "policies": 0,
        "entries": 0,
        "runtime": 0,
        "dispatch": 0,
        "flamp": 0,
    }

    def _catalog(*, limit: int):
        counts["catalog"] += 1
        return [
            {
                "callsign": "K1OLD",
                "current_callsign": "K1NEW",
                "historical": True,
                "trusted": True,
                "groups": ["MAGNET", "MR08"],
            }
        ]

    def _policies(**_kwargs):
        counts["policies"] += 1
        return [{"id": 7, "name": "Regional trusted", "enabled": True}]

    def _entries(**_kwargs):
        counts["entries"] += 1
        return []

    def _runtime(**_kwargs):
        counts["runtime"] += 1
        return []

    def _dispatch(**_kwargs):
        counts["dispatch"] += 1
        return []

    def _flamp(**_kwargs):
        counts["flamp"] += 1
        return []

    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", _catalog)
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", _policies)
    monkeypatch.setattr(spotter_ui, "list_expect_entries", _entries)
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", _runtime)
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", _dispatch)
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", _flamp)

    tab = FioSpotterTab(settings=_Settings())
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        _select_tab(tab, "Access Policies")
        app.processEvents()
        initial = dict(counts)

        tab.expect_key.setText("Q")
        tab.expect_reply.setText("READY")
        tab.expect_groups.setText("@MAGNET")
        tab.expect_trusted_groups.setText("MR08")
        tab.policy_manage.setCurrentIndex(tab.policy_manage.findData(7))
        _select_tab(tab, "Expect")
        app.processEvents()
        tab.expect_policy.setCurrentIndex(tab.expect_policy.findData(7))
        tab.expect_radio.setCurrentIndex(0)
        app.processEvents()

        assert counts == initial
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


def test_expect_editor_uses_named_fio_radio_and_hides_routing_details(monkeypatch, tmp_path):
    app = _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = _Settings()
    tab = FioSpotterTab(settings=settings, radio_store=_RadioStore())
    try:
        _select_tab(tab, "Access Policies")
        app.processEvents()
        tab.policy_name.setText("Regional hubs")
        tab.policy_calls.setText("K1ABC")
        tab.policy_groups.setText("@MAGNET")
        tab.policy_scope.setCurrentIndex(tab.policy_scope.findData("radio"))
        tab.policy_radios.setText("7")
        tab._save_policy()
        policies = spotter_ui.list_expect_allow_policies()
        assert policies[0]["allowed_groups"] == ["@MAGNET"]
        assert policies[0]["source_radio_ids"] == ["7"]

        _select_tab(tab, "Expect")
        app.processEvents()
        tab.expect_key.setText("INFO")
        tab.expect_reply.setText("STATUS GREEN")
        tab.expect_policy.setCurrentIndex(tab.expect_policy.findData(policies[0]["id"]))
        tab.expect_radio.setCurrentIndex(tab.expect_radio.findData("7"))
        tab.expect_enabled.setChecked(True)
        tab._save_entry()
        entries = spotter_ui.list_expect_entries()
        assert entries[0]["source_radio_id"] == "7"
        assert entries[0]["source_scope"] == "radio"
        assert entries[0]["js8_instance_id"] == ""
        assert entries[0]["auto_tx_schedule"] == ""
        assert tab.expect_radio.currentText() == "FTDX-10"
        assert tab.expect_radio.findData("8") == -1
        widest_radio = max(
            tab.expect_radio.fontMetrics().horizontalAdvance(tab.expect_radio.itemText(index))
            for index in range(tab.expect_radio.count())
        )
        assert tab.expect_radio.view().minimumWidth() >= widest_radio
        labels = [label.text() for label in tab.findChildren(spotter_ui.QLabel)]
        assert "E? Token" in labels
        assert "JS8 instance" not in labels
        assert "Schedule" not in labels
    finally:
        tab.deleteLater()


def test_expect_editor_defaults_to_all_radios_and_preserves_legacy_routing(monkeypatch, tmp_path):
    app = _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    spotter_ui.save_expect_entry({
        "expect_key": "INFO",
        "response_text": "STATUS GREEN",
        "source_scope": "radio",
        "source_radio_id": "7",
        "js8_instance_id": "fio-a",
        "auto_tx_schedule": "18:00-23:00Z",
        "enabled": True,
    })
    tab = FioSpotterTab(settings=_Settings(), radio_store=_RadioStore())
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        tab._clear_entry()
        assert tab.expect_radio.currentData() == ""
        assert tab.expect_radio.currentText().startswith("All JS8 radios")

        tab.expect_entries_table.selectRow(0)
        app.processEvents()
        assert tab.expect_radio.currentData() == "7"
        tab._save_entry()
        entry = spotter_ui.list_expect_entries()[0]
        assert entry["js8_instance_id"] == "fio-a"
        assert entry["auto_tx_schedule"] == "18:00-23:00Z"
    finally:
        tab.deleteLater()


def test_compact_navigation_exposes_spotter_icon_route():
    from freqinout.gui.main_window import MainWindow

    assert ("Spotter", "FIO Spotter", "FIO Spotter", "spotter.svg") in MainWindow._compact_navigation_specs()


def test_spotter_navigation_icon_uses_shared_navigation_color_and_canvas():
    icon = (
        Path(__file__).resolve().parents[1]
        / "assets" / "icons" / "navigation" / "spotter.svg"
    ).read_text(encoding="utf-8")

    assert 'viewBox="0 0 24 24"' in icon
    assert 'stroke="#3F8FC7"' in icon
    assert "currentColor" not in icon


def test_watches_editor_uses_shared_bounded_store_for_save_toggle_delete_and_test(tmp_path, monkeypatch):
    """The UI owns no watch list: every editor action reaches the core store."""
    app = _app()
    db_path = tmp_path / "spotter.db"
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
        _select_tab(tab, "Watches")
        app.processEvents()
        tab.watch_name.setText("Smoke")
        tab.watch_pattern.setText("smoke")
        tab.watch_sources.setText("spotter, js8")
        tab._save_watch()
        rows = fio_spotter_store.list_spotter_watches(db_path=db_path)
        assert rows[0]["name"] == "Smoke"
        assert rows[0]["source_families"] == ["spotter", "js8"]
        assert tab.watches_table.rowCount() == 1

        tab._watch_preview_candidate = {"body_text": "Smoke reported", "source_family": "spotter"}
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
    monkeypatch.setattr(
        spotter_ui,
        "list_expect_entries",
        lambda **_kwargs: [{
            "expect_key": "F!307",
            "enabled": True,
            "auto_reply_enabled": True,
            "unattended_auto_reply_enabled": True,
        }],
    )
    preview = SimpleNamespace(
        source_db=str(source), candidates=4, forms=1, expect=2, archive=1,
        duplicates=0, skipped=0, conflicts=0, warnings=(),
    )
    monkeypatch.setattr(spotter_ui, "preview_js8spotter_import", lambda *_args, **_kwargs: preview)
    tab = FioSpotterTab(settings=settings)
    try:
        _select_tab(tab, "Forms"); app.processEvents()
        tab.forms_path.setText(str(forms_dir)); tab._use_forms_folder()
        assert settings.values["js8_forms_path"] == str(forms_dir)
        assert "preserved" in tab.forms_state.text().lower()
        tab.refresh_forms()
        assert tab.forms_table.rowCount() == 1
        purpose_combo = tab.forms_table.cellWidget(0, 2)
        widest_purpose = max(
            purpose_combo.fontMetrics().horizontalAdvance(purpose_combo.itemText(index))
            for index in range(purpose_combo.count())
        )
        assert purpose_combo.view().minimumWidth() >= widest_purpose
        assert tab.forms_table.columnWidth(2) >= widest_purpose + 40
        assert tab.forms_table.horizontalHeaderItem(8).text() == "Expect status"
        assert tab.forms_table.item(0, 8).text() == "Needs attention"
        tab.forms_table.selectRow(0)
        tab._auto_classify_forms()
        tab._save_form_mappings()
        assert settings.values["js8_spotter_form_mappings"][0]["form_code"] == "F!307"
        assert settings.values["js8_spotter_form_mappings"][0]["alert"] is True
        tab._preview_selected_form()
        assert "Weather report" in tab.forms_preview.toPlainText()

        _select_tab(tab, "Imports"); app.processEvents()
        tab.import_source.setText(str(source)); tab._preview_import()
        assert settings.values["js8spotter_import_db_path"] == str(source)
        assert "4 candidates" in tab.imports_state.text()
    finally:
        tab.deleteLater()


def test_forms_selection_is_cache_only_and_preview_is_explicit(tmp_path, monkeypatch):
    app = _app()
    settings = _Settings()
    forms_dir = tmp_path / "forms"
    forms_dir.mkdir()
    source = forms_dir / "MCF307.txt"
    source.write_text("Weather report\n? Conditions\n", encoding="utf-8")
    settings.values["js8_forms_path"] = str(forms_dir)
    tab = FioSpotterTab(settings=settings)
    reads: list[Path] = []
    original_read = Path.read_text

    def tracked_read(path, *args, **kwargs):
        reads.append(path)
        return original_read(path, *args, **kwargs)

    try:
        _select_tab(tab, "Forms")
        app.processEvents()
        tab.refresh_forms()
        monkeypatch.setattr(Path, "read_text", tracked_read)
        tab.forms_table.selectRow(0)
        app.processEvents()
        assert reads == []
        assert "Choose Preview selected" in tab.forms_preview.toPlainText()

        tab._preview_selected_form()
        assert reads == [source]
        assert "Weather report" in tab.forms_preview.toPlainText()
    finally:
        tab.deleteLater()


def test_forms_compose_handoff_carries_the_selected_catalog_form():
    app = _app()
    opened: list[dict[str, str]] = []
    tab = FioSpotterTab(settings=_Settings(), open_compose=lambda intent: opened.append(intent))
    try:
        _select_tab(tab, "Forms")
        app.processEvents()
        tab.forms_table.setRowCount(1)
        item = QTableWidgetItem("F!307")
        item.setData(Qt.UserRole, {"form_code": "F!307", "title": "Weather"})
        tab.forms_table.setItem(0, 0, item)
        tab.forms_table.selectRow(0)

        tab._open_spotter_compose()

        assert opened == [{"mode": "spotter", "spotter_form_code": "F!307"}]
    finally:
        tab.deleteLater()


def test_import_requires_a_clean_preview_before_the_commit_action(tmp_path, monkeypatch):
    app = _app()
    source = tmp_path / "js8spotter.db"
    source.touch()
    preview = SimpleNamespace(
        source_db=str(source), candidates=1, forms=1, expect=0, watches=0, archive=0,
        duplicates=0, skipped=0, conflicts=0, warnings=(),
    )
    monkeypatch.setattr(spotter_ui, "preview_js8spotter_import", lambda *_args, **_kwargs: preview)
    tab = FioSpotterTab(settings=_Settings())
    try:
        _select_tab(tab, "Imports")
        app.processEvents()
        assert not tab.import_apply_btn.isEnabled()
        tab.import_source.setText(str(source))
        assert tab.import_preview_btn.isEnabled()
        assert not tab.import_apply_btn.isEnabled()

        tab._preview_import()

        assert tab.import_apply_btn.isEnabled()
        assert "1 candidates" in tab.imports_state.text()
    finally:
        tab.deleteLater()


def test_forms_compose_action_uses_main_shell_handoff(monkeypatch):
    app = _app()
    opened: list[str] = []
    tab = FioSpotterTab(settings=_Settings(), open_compose=lambda: opened.append("compose"))
    try:
        _select_tab(tab, "Forms")
        app.processEvents()
        tab._open_spotter_compose()
        assert opened == ["compose"]
    finally:
        tab.deleteLater()


def test_expect_presents_one_service_state_and_one_per_entry_auto_reply(monkeypatch):
    app = _app()
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings())
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        assert tab.expect_runtime_chip.text() in {"Expect service: On", "Expect service: Paused"}
        assert tab.dynamic_flamp_chip.text() == "FLAMP Q: Waiting for scan"
        assert tab.expect_delivery_mode.currentText() == "Saved only"
        assert tab.expect_policy_summary.text() == "Policy: required for Auto reply"
        assert not tab.expect_legacy_access.isVisible()
        visible_labels = {
            widget.text() for widget in tab.tabs.currentWidget().findChildren(QCheckBox)
            if widget.text() and not widget.isHidden()
        }
        assert "Enable unattended auto-reply" not in visible_labels
        assert "Unattended auto reply" not in visible_labels
        assert "Rule enabled" not in visible_labels
    finally:
        tab.deleteLater()


def test_expect_requires_named_enabled_policy_for_new_auto_reply_and_keeps_legacy_access(monkeypatch):
    app = _app()
    policies = [{"id": 9, "name": "Regional access", "enabled": True}]
    writes: list[dict] = []

    class _Saved:
        id = 41
        created = True
        expect_key = "INFO"

    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: policies)
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "save_expect_entry", lambda payload: writes.append(payload) or _Saved())
    tab = FioSpotterTab(settings=_Settings())
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        tab.expect_key.setText("INFO")
        tab.expect_reply.setText("READY")
        tab.expect_delivery_mode.setCurrentIndex(1)
        tab._save_entry()
        assert writes == []
        assert "named, enabled access policy" in tab.expect_maintenance_state.text()

        tab.expect_policy.setCurrentIndex(tab.expect_policy.findData(9))
        tab._save_entry()
        assert writes[-1]["allow_policy_id"] == 9
        assert writes[-1]["auto_reply_enabled"] is True

        legacy = {
            "id": 7,
            "expect_key": "INFO",
            "response_text": "READY",
            "allowed_callsigns": ["N0CALL"],
            "enabled": True,
            "auto_reply_enabled": True,
            "unattended_auto_reply_enabled": True,
            "auto_reply_state": "auto-reply-on",
        }
        tab.expect_policy.setCurrentIndex(0)
        tab.expect_entries_table.setRowCount(1)
        item = QTableWidgetItem("")
        item.setData(Qt.UserRole, legacy)
        tab.expect_entries_table.setItem(0, 0, item)
        tab.expect_entries_table.selectRow(0)
        tab._set_expect_legacy_access_for_row(legacy)
        tab.expect_calls.setText("N0CALL")
        tab.expect_delivery_mode.setCurrentIndex(1)
        tab._save_entry()
        assert writes[-1]["allow_policy_id"] is None
        assert writes[-1]["allowed_callsigns"] == ["N0CALL"]
        assert writes[-1]["auto_reply_enabled"] is True
    finally:
        tab.deleteLater()


def test_expect_send_now_hands_saved_response_to_compose_review(monkeypatch):
    app = _app()
    row = {
        "id": 12, "expect_key": "INFO", "response_text": "INFO READY",
        "auto_reply_state": "saved-only", "manual_send_available": True,
    }
    monkeypatch.setattr(spotter_ui, "list_expect_entries", lambda **_kwargs: [row])
    monkeypatch.setattr(spotter_ui, "list_expect_allow_policies", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_runtime_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_expect_dispatch_audit", lambda **_kwargs: [])
    monkeypatch.setattr(spotter_ui, "list_flamp_transfer_index_statuses", lambda **_kwargs: [])
    intents: list[dict] = []
    tab = FioSpotterTab(settings=_Settings(), open_compose=lambda intent: intents.append(intent))
    try:
        _select_tab(tab, "Expect")
        app.processEvents()
        tab.expect_entries_table.selectRow(0)
        tab._send_selected_expect_now()
        assert intents == [{
            "transport": "spotter", "expect_entry_id": 12,
            "expect_key": "INFO", "source": "fio_spotter_expect",
        }]
        assert "working copy" in tab.expect_maintenance_state.text()
    finally:
        tab.deleteLater()


def test_inbox_add_to_watch_stages_anded_callsign_topic_without_saving(monkeypatch):
    app = _app()
    row = {
        "message_id": "spotter:watch", "source_family": "spotter",
        "from_call": "N0CALL", "topics": ["Fire"], "radio_id": "7",
    }
    monkeypatch.setattr(spotter_ui, "list_spotter_watches", lambda **_kwargs: [])
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.open_watch_draft(row)
        assert tab.tabs.currentIndex() == 0
        assert tab.watch_kind.currentText() == "callsign"
        assert tab.watch_pattern.text() == "N0CALL"
        assert tab.watch_secondary_kind.currentData() == "topic"
        assert tab.watch_secondary_pattern.text() == "Fire"
        values = tab._watch_values()
        assert [(item["kind"], item["pattern"]) for item in values["criteria"]] == [
            ("callsign", "N0CALL"), ("topic", "Fire"),
        ]
        assert "Nothing has been added" in tab.watch_status.text()
    finally:
        tab.deleteLater()
