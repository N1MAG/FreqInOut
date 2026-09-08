from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication, QScrollArea

from freqinout.core import fio_spotter_store
from freqinout.core.traffic_actionability import build_operator_traffic_context
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


def test_spotter_tab_has_lazy_browser_tabs_in_service_order():
    app = _app()
    tab = FioSpotterTab(settings=_Settings(), radio_store=_RadioStore())
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


def test_screen_reactivation_does_not_repeat_activity_query(monkeypatch):
    app = _app()
    calls: list[int] = []

    def activity(**_kwargs):
        calls.append(1)
        return []

    monkeypatch.setattr(spotter_ui, "list_spotter_activity", activity)
    tab = FioSpotterTab(settings=_Settings())
    try:
        assert len(calls) == 1
        tab.set_tab_active(True)
        tab.set_tab_active(True)
        app.processEvents()
        assert len(calls) == 1
        tab.refresh_activity()
        assert len(calls) == 2
    finally:
        tab.deleteLater()


def test_activity_filter_chips_use_the_current_bounded_page_without_a_query(monkeypatch):
    app = _app()
    calls: list[int] = []

    def activity(**_kwargs):
        calls.append(1)
        return [
            {"message_id": "spotter:1", "source_family": "spotter", "summary": "Form"},
            {"message_id": "js8:1", "source_family": "js8", "summary": "JS8"},
        ]

    monkeypatch.setattr(spotter_ui, "list_spotter_activity", activity)
    tab = FioSpotterTab(settings=_Settings())
    try:
        assert calls == [1]
        next(button for button in tab.activity_chips if button.text() == "JS8").click()
        app.processEvents()
        assert calls == [1]
        assert [row["message_id"] for row in tab._activity_rows] == ["js8:1"]
    finally:
        tab.deleteLater()


def test_activity_intelligence_filters_the_cached_page_without_a_query(monkeypatch):
    app = _app()
    calls: list[int] = []

    def activity(**_kwargs):
        calls.append(1)
        return [
            {
                "message_id": "spotter:event", "source_family": "spotter",
                "from_call": "K1ABC", "to_call": "@MR08", "group_name": "MR08",
                "summary": "Wildfire affecting Route 9", "topics": ["Fire", "Travel/Roads"],
                "severity": "warning", "actionable": True, "received_ts": 20.0,
            },
            {
                "message_id": "js8:other", "source_family": "js8",
                "from_call": "K2ABC", "to_call": "@OTHER", "group_name": "OTHER",
                "summary": "Routine traffic", "topics": [], "received_ts": 10.0,
            },
        ]

    context = build_operator_traffic_context(
        callsign="N1MAG",
        configured_operating_groups=("MR08",),
        operator_rows=({"callsign": "N1MAG", "group1": "MR08", "group_role": "HUB"},),
    )
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", activity)
    monkeypatch.setattr(spotter_ui, "load_operator_traffic_context", lambda *_args, **_kwargs: context)

    tab = FioSpotterTab(settings=_Settings())
    try:
        assert calls == [1]
        assert tab.activity_intelligence.buttons["relay"].text() == "Relay 1"
        tab.activity_intelligence.buttons["relay"].click()
        app.processEvents()
        assert calls == [1]
        assert [row["message_id"] for row in tab._activity_rows] == ["spotter:event"]
        assert tab.activity_table.item(0, 5).text() == "Relay"
        assert "Distribute Fire report" in tab.activity_intelligence.insight_label.text()
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


@pytest.mark.parametrize("scale", [1.0, 1.25])
def test_expect_editor_keeps_narrow_layout_and_text_controls_readable(scale):
    app = _app()
    old_font = _set_app_text_scale(app, scale)
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.resize(900, 560)
        tab.tabs.setCurrentIndex(2)
        tab.show()
        app.processEvents()

        assert tab.height() == 560
        assert tab.expect_editor_split.orientation() == Qt.Vertical
        scroll = tab.tabs.currentWidget().findChild(QScrollArea)
        assert scroll is not None
        assert scroll.horizontalScrollBarPolicy() == Qt.ScrollBarAlwaysOff
        assert scroll.horizontalScrollBar().maximum() == 0
        assert scroll.verticalScrollBar().maximum() > 0

        controls = (
            tab.expect_key,
            tab.expect_reply,
            tab.expect_policy,
            tab.expect_calls,
            tab.expect_groups,
            tab.expect_blocked,
            tab.expect_trusted_groups,
            tab.expect_radio,
            tab.expect_max,
            tab.expect_cooldown,
            tab.policy_manage,
            tab.policy_name,
            tab.policy_calls,
            tab.policy_groups,
            tab.policy_trusted_groups,
            tab.policy_blocked,
            tab.policy_scope,
            tab.policy_radios,
        )
        assert all(widget.height() >= widget.sizeHint().height() for widget in controls)
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
        tab.tabs.setCurrentIndex(2)
        app.processEvents()
        tab.policy_manage.setCurrentIndex(tab.policy_manage.findData(7))
        app.processEvents()
        assert tab.policy_name.text() == "Regional trusted"

        tab.refresh_expect()
        app.processEvents()

        assert tab.policy_manage.currentData() == 0
        assert tab.policy_manage.currentText() == "New policy"
        assert tab.policy_name.text() == ""
        assert tab.policy_trusted_groups.text() == ""
        assert tab.expect_policy.currentData() == 0
        assert tab.expect_policy.findData(0) == 0
    finally:
        tab.close()
        tab.deleteLater()


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
        tab.tabs.setCurrentIndex(2)
        app.processEvents()
        initial = dict(counts)

        tab.expect_key.setText("Q")
        tab.expect_reply.setText("READY")
        tab.expect_groups.setText("@MAGNET")
        tab.expect_trusted_groups.setText("MR08")
        tab.policy_manage.setCurrentIndex(tab.policy_manage.findData(7))
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
        tab.tabs.setCurrentIndex(2)
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
        tab.tabs.setCurrentIndex(2)
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
        purpose_combo = tab.forms_table.cellWidget(0, 2)
        widest_purpose = max(
            purpose_combo.fontMetrics().horizontalAdvance(purpose_combo.itemText(index))
            for index in range(purpose_combo.count())
        )
        assert purpose_combo.view().minimumWidth() >= widest_purpose
        assert tab.forms_table.columnWidth(2) >= widest_purpose + 40
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


def test_activity_leads_with_shared_assessment_then_source_evidence(monkeypatch):
    app = _app()
    row = {
        "message_id": "spotter:assessment", "source_family": "spotter", "from_call": "K1ABC",
        "to_call": "@MR08", "summary": "Wildfire reported", "body_text": "Smoke near Route 9.",
        "topics": ["Fire", "Travel/Roads"], "severity": "warning",
        "operator_attention": True, "recommended_action": "review_now",
        "intelligence": {"provenance": {"trust": "trusted", "freshness": "recent"}},
    }
    monkeypatch.setattr(spotter_ui, "list_spotter_activity", lambda **_kwargs: [row])
    tab = FioSpotterTab(settings=_Settings())
    try:
        tab.activity_table.selectRow(0)
        app.processEvents()
        assert tab.activity_table.item(0, 5).text() == "Review now"
        assert tab.activity_table.item(0, 6).text() == "Fire, Travel/Roads"
        detail = tab.activity_detail.toPlainText()
        assert "Assessment" in detail
        assert "Recommended action: Review now" in detail
        assert "Trust: trusted" in detail
        assert "Source evidence" in detail
    finally:
        tab.deleteLater()
