"""SW-3 receive-only reminder, recurrence, and safety gates."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import inspect
import sqlite3
import time


from tests.test_shortwave_sw1_eibi import _parse_fixture, _store


def _app():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication([])


def _drain(app, seconds: float = 0.25) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        app.processEvents()
        time.sleep(0.005)


def _qt_workspace(tmp_path: Path):
    from freqinout.gui.shortwave_tab import ShortwaveWorkspace

    app = _app()
    workspace = ShortwaveWorkspace(db_path=tmp_path / "freqinout_nets.db")
    workspace.resize(900, 560)
    workspace.show()
    app.processEvents()
    return app, workspace


def test_sw3_focus_reminder_survives_lazy_async_load(tmp_path: Path) -> None:
    from freqinout.core.shortwave_listening import ShortwaveListeningStore

    store, _candidate, dataset, entry = _fixture_store(tmp_path)
    reminder = ShortwaveListeningStore(store.db_path).add_from_entry(
        entry.entry_key, dataset_key=dataset.dataset_key, operator_label="Evening monitor", lead_minutes=20,
    )
    app, workspace = _qt_workspace(tmp_path)
    try:
        workspace.set_tab_active(True)
        workspace.focus_reminder(reminder.reminder_key)
        _drain(app, 0.6)
        page = workspace._pages[1]
        assert page._selected_key() == reminder.reminder_key
        assert page.label_edit.text() == "Evening monitor"
        assert page.lead_combo.currentData() == 20
        assert "Accepted listing:" in page.snapshot_summary.text()
    finally:
        workspace.shutdown()
        workspace.close()


def _fixture_store(tmp_path: Path):
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import

    store = _store(tmp_path / "freqinout_nets.db")
    candidate = _parse_fixture()
    apply_shortwave_import(store, preview_shortwave_import(store, candidate))
    dataset = store.current_dataset()
    assert dataset is not None
    entry = next(
        row for row in store.list_entries(dataset_key=dataset.dataset_key, classifications=(), limit=200)
        if row.candidate.station_name == "Cross midnight broadcast"
    )
    return store, candidate, dataset, entry


def test_sw3_schema_is_additive_idempotent_and_preserves_existing_data(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    store = _store(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE operator_fixture(value TEXT)")
        conn.execute("INSERT INTO operator_fixture VALUES('keep')")
    store.create_schema()
    with sqlite3.connect(db_path) as conn:
        names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert conn.execute("SELECT value FROM operator_fixture").fetchone()[0] == "keep"
    assert {"shortwave_listening_reminders", "shortwave_listening_dismissals"} <= names


def test_sw3_create_update_duplicate_and_remove_lifecycle(tmp_path: Path) -> None:
    from freqinout.core.shortwave_listening import ShortwaveListeningStore

    store, _candidate, dataset, entry = _fixture_store(tmp_path)
    listening = ShortwaveListeningStore(store.db_path)
    created = listening.add_from_entry(
        entry.entry_key, dataset_key=dataset.dataset_key, operator_label="Evening monitor",
        notes="Portable receiver", receiver_profile_id=17, lead_minutes=20,
    )
    assert created.snapshot.station_name == "Cross midnight broadcast"
    assert created.receiver_profile_id == 17 and created.lead_minutes == 20
    duplicate = listening.add_from_entry(entry.entry_key, dataset_key=dataset.dataset_key)
    assert duplicate.reminder_key == created.reminder_key
    assert duplicate.operator_label == "Evening monitor", "opening an existing reminder must not erase edits"
    updated = listening.update(
        created.reminder_key, operator_label="Night watch", notes="Manual tune only",
        receiver_profile_id=None, lead_minutes=30, enabled=False,
    )
    assert updated.operator_label == "Night watch" and not updated.enabled
    assert len(listening.list_reminders()) == 1
    assert listening.remove(created.reminder_key)
    assert listening.list_reminders() == ()


def test_sw3_source_change_keep_and_explicit_apply_never_silently_mutate_snapshot(tmp_path: Path) -> None:
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import
    from freqinout.core.shortwave_listening import ShortwaveListeningStore

    store, candidate, dataset, entry = _fixture_store(tmp_path)
    listening = ShortwaveListeningStore(store.db_path)
    reminder = listening.add_from_entry(entry.entry_key, dataset_key=dataset.dataset_key)
    accepted_hash = reminder.snapshot.content_hash
    changed_entry = replace(entry.candidate, language_labels=("Changed language",), content_hash="f" * 64)
    changed_candidate = replace(
        candidate,
        csv_sha256="e" * 64,
        entries=tuple(changed_entry if row.provider_identity_hash == entry.candidate.provider_identity_hash else row for row in candidate.entries),
    )
    preview = preview_shortwave_import(store, changed_candidate)
    assert preview.reminder_changed_count == 1 and preview.reminder_missing_count == 0
    apply_shortwave_import(store, preview)
    assert listening.get(reminder.reminder_key).snapshot.content_hash == accepted_hash
    assert listening.get(reminder.reminder_key).source_review_state == "changed"
    assert listening.refresh_source_review_states()["changed"] == 1
    assert listening.get(reminder.reminder_key).source_review_state == "changed"
    review = listening.source_review(reminder.reminder_key)
    assert review.state == "changed" and "language" in review.changed_fields
    kept = listening.keep_accepted_snapshot(reminder.reminder_key)
    assert kept.source_review_state == "kept" and kept.snapshot.content_hash == accepted_hash
    assert listening.refresh_source_review_states()["kept"] == 1
    applied = listening.apply_current_listing(reminder.reminder_key)
    assert applied.source_review_state == "current"
    assert applied.snapshot.content_hash == "f" * 64


def test_sw3_missing_source_is_reviewable_without_deleting_reminder(tmp_path: Path) -> None:
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import
    from freqinout.core.shortwave_listening import ShortwaveListeningStore

    store, candidate, dataset, entry = _fixture_store(tmp_path)
    listening = ShortwaveListeningStore(store.db_path)
    reminder = listening.add_from_entry(entry.entry_key, dataset_key=dataset.dataset_key)
    reduced = replace(
        candidate,
        csv_sha256="d" * 64,
        entries=tuple(row for row in candidate.entries if row.provider_identity_hash != entry.candidate.provider_identity_hash),
    )
    preview = preview_shortwave_import(store, reduced)
    assert preview.reminder_missing_count == 1
    apply_shortwave_import(store, preview)
    assert listening.get(reminder.reminder_key).source_review_state == "missing"
    assert listening.refresh_source_review_states()["missing"] == 1
    assert listening.get(reminder.reminder_key).snapshot.station_name == "Cross midnight broadcast"


def test_sw3_cross_midnight_occurrence_and_dismissal_preserve_recurrence(tmp_path: Path) -> None:
    from freqinout.core.shortwave_listening import ShortwaveListeningStore, build_shortwave_listening_outlook

    store, _candidate, dataset, entry = _fixture_store(tmp_path)
    listening = ShortwaveListeningStore(store.db_path)
    reminder = listening.add_from_entry(entry.entry_key, dataset_key=dataset.dataset_key)
    now = datetime(2026, 5, 4, 23, 45, tzinfo=timezone.utc)
    outlook = build_shortwave_listening_outlook(listening, now, horizon_days=10)
    assert outlook.active is not None and outlook.active.end_utc.date().day == 5
    active = outlook.active
    listening.dismiss_occurrence(reminder.reminder_key, active.occurrence_key, active.start_utc)
    after = build_shortwave_listening_outlook(listening, now, horizon_days=10)
    assert after.active is None
    assert after.next is not None, "dismissing one occurrence must preserve the recurring reminder"


def test_sw3_winter_snapshot_projects_across_new_year_with_provider_bounds(tmp_path: Path) -> None:
    from freqinout.core.shortwave_eibi_provider import parse_eibi_dataset
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import
    from freqinout.core.shortwave_listening import ShortwaveListeningStore, build_shortwave_listening_outlook
    from tests.test_shortwave_sw1_eibi import FIXTURE, README_FIXTURE

    candidate = parse_eibi_dataset(
        FIXTURE.read_bytes().replace(b";15Sep;", b";Fr;").replace(b";1509;1509", b";1501;1501"),
        README_FIXTURE,
        season_code="B26",
        season_effective_from_utc="2026-10-25T00:00:00Z",
        season_effective_to_utc="2027-03-28T23:59:59Z",
    )
    store = _store(tmp_path / "freqinout_nets.db")
    apply_shortwave_import(store, preview_shortwave_import(store, candidate))
    dataset = store.current_dataset()
    assert dataset is not None
    entry = next(row for row in store.list_entries(dataset_key=dataset.dataset_key, classifications=(), limit=200) if row.candidate.station_name == "Fixed date service")
    listening = ShortwaveListeningStore(store.db_path)
    listening.add_from_entry(entry.entry_key, dataset_key=dataset.dataset_key)
    outlook = build_shortwave_listening_outlook(
        listening, datetime(2027, 1, 15, 6, 30, tzinfo=timezone.utc), horizon_days=2,
    )
    assert outlook.active is not None
    outside = build_shortwave_listening_outlook(
        listening, datetime(2027, 3, 29, 6, 30, tzinfo=timezone.utc), horizon_days=2,
    )
    assert outside.visible_items == ()


def test_sw3_projection_is_bounded_fair_fast_and_never_commandable(tmp_path: Path) -> None:
    from freqinout.core.shortwave_listening import (
        MAX_LISTENING_OCCURRENCES,
        ShortwaveListeningStore,
        build_shortwave_listening_outlook,
    )

    store, _candidate, dataset, _entry = _fixture_store(tmp_path)
    listening = ShortwaveListeningStore(store.db_path)
    entries = [row for row in store.list_entries(dataset_key=dataset.dataset_key, classifications=(), limit=200) if row.candidate.parse_state == "complete"]
    for entry in entries:
        listening.add_from_entry(entry.entry_key, dataset_key=dataset.dataset_key)
    now = datetime(2026, 5, 4, 12, tzinfo=timezone.utc)
    samples = []
    for _ in range(10):
        started = time.perf_counter()
        outlook = build_shortwave_listening_outlook(listening, now, horizon_days=30, later_limit=MAX_LISTENING_OCCURRENCES)
        samples.append(time.perf_counter() - started)
    assert len(outlook.later) <= MAX_LISTENING_OCCURRENCES
    assert all(not item.commandable and item.source_type == "SHORTWAVE_LISTENING" for item in outlook.visible_items)
    assert len({item.reminder_key for item in outlook.visible_items}) >= 2
    assert sorted(samples)[9] < 0.100


def test_sw3_real_a26_two_hundred_reminders_refresh_and_project_within_budget(tmp_path: Path) -> None:
    from freqinout.core.shortwave_eibi_provider import bundled_eibi_seed_paths, parse_eibi_dataset
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import
    from freqinout.core.shortwave_listening import ShortwaveListeningStore, build_shortwave_listening_outlook

    csv_path, readme_path = bundled_eibi_seed_paths()
    candidate = parse_eibi_dataset(
        csv_path, readme_path, season_code="A26",
        season_effective_from_utc="2026-04-01T00:00:00Z",
        season_effective_to_utc="2026-10-31T23:59:59Z",
    )
    store = _store(tmp_path / "freqinout_nets.db")
    apply_shortwave_import(store, preview_shortwave_import(store, candidate))
    dataset = store.current_dataset()
    assert dataset is not None
    entries = [
        row for row in store.list_entries(dataset_key=dataset.dataset_key, classifications=(), parse_states=("complete",), limit=200)
        if not row.candidate.inactive
    ]
    listening = ShortwaveListeningStore(store.db_path)
    for entry in entries:
        listening.add_from_entry(entry.entry_key, dataset_key=dataset.dataset_key)
    reminder_count = len(listening.list_reminders())
    started = time.perf_counter()
    assert listening.refresh_source_review_states()["current"] == reminder_count
    refresh_elapsed = time.perf_counter() - started
    samples = []
    for _ in range(6):
        started = time.perf_counter()
        outlook = build_shortwave_listening_outlook(
            listening, datetime(2026, 9, 10, 12, tzinfo=timezone.utc), horizon_days=30,
        )
        samples.append(time.perf_counter() - started)
    assert len(outlook.visible_items) <= 57
    assert refresh_elapsed < 0.100
    assert max(samples) < 0.100


def test_sw3_core_has_no_commandable_scheduler_or_sop_path() -> None:
    import freqinout.core.shortwave_listening as module

    source = inspect.getsource(module).lower()
    forbidden_imports = (
        "scheduler_engine", "qsy", "ptt", "sop_manager", "launch_bundle",
        "operating_group", "radio_interface",
    )
    assert all(f"import {name}" not in source and f"from freqinout.core.{name}" not in source for name in forbidden_imports)
    assert "commandable=false" in source.replace(" ", "")


def test_sw3_workspace_constructs_listening_lazily_and_add_route_opens_it(tmp_path: Path) -> None:
    app, workspace = _qt_workspace(tmp_path)
    try:
        assert 0 in workspace._pages
        assert 1 not in workspace._pages
        workspace._ensure_page(1)
        assert 1 in workspace._pages
        assert workspace.tabs.tabText(1) == "Listening"
    finally:
        workspace.shutdown()
        workspace.close()
        app.processEvents()


def test_sw3_listening_receiver_provider_uses_configured_labels(tmp_path: Path) -> None:
    from freqinout.gui.shortwave_tab import ShortwaveListeningView

    app = _app()
    store = _store(tmp_path / "freqinout_nets.db")
    store.create_schema()
    view = ShortwaveListeningView(
        store.db_path,
        receiver_profiles_provider=lambda: [
            {"id": 2, "name": "Portable SDR", "device_class": "observer"},
            {"id": 3, "name": "HF Radio", "device_class": "radio"},
        ],
    )
    try:
        result = view._load_data(lambda: False)
        assert result[1] == {2: "Portable SDR · receive-only", 3: "HF Radio"}
    finally:
        view.shutdown()
        view.close()
        app.processEvents()


def test_sw3_shortwave_workspace_compact_view_has_no_horizontal_table_overflow(tmp_path: Path) -> None:
    from PySide6.QtWidgets import QAbstractScrollArea

    app, workspace = _qt_workspace(tmp_path)
    try:
        workspace._ensure_page(1)
        workspace._ensure_page(2)
        for index in range(3):
            workspace.tabs.setCurrentIndex(index)
            app.processEvents()
            for area in workspace.tabs.widget(index).findChildren(QAbstractScrollArea):
                if area.isVisible():
                    assert area.horizontalScrollBar().maximum() == 0, area.objectName()
    finally:
        workspace.shutdown()
        workspace.close()
        app.processEvents()


def test_sw3_data_sources_exposes_review_apply_rollback_and_diagnostics_controls(tmp_path: Path) -> None:
    from freqinout.gui.shortwave_tab import ShortwaveDataSourcesView

    app = _app()
    view = ShortwaveDataSourcesView(tmp_path / "freqinout_nets.db")
    try:
        labels = {button.text() for button in view.findChildren(type(view.review_bundled_btn))}
        assert {"Review bundled snapshot", "Review official update", "Review local files…", "Apply reviewed import", "Export diagnostics…"} <= labels
        assert view.apply_btn.isEnabled() is False
    finally:
        view.shutdown()
        view.close()
        app.processEvents()


def test_sw3_ops_listening_surface_is_collapsed_and_does_not_query_until_expanded() -> None:
    from freqinout.gui.controlfreq_tab import ControlFreqTab

    app = _app()
    tab = ControlFreqTab(defer_initial_refresh=True)
    calls: list[object] = []
    try:
        tab.set_shortwave_listening_outlook_provider(lambda now: calls.append(now) or ())
        tab.set_tab_active(True)
        app.processEvents()
        assert tab.shortwave_listening_outlook_toggle.isChecked() is False
        assert calls == []
        tab.shortwave_listening_outlook_toggle.setChecked(True)
        _drain(app, 0.35)
        assert calls, "expanding the surface should schedule its bounded provider query"
    finally:
        tab.set_tab_active(False)
        tab._shutdown_background_executors()
        tab.close()
        app.processEvents()


def test_sw3_deferred_ops_layout_is_safe_after_widget_destruction() -> None:
    from shiboken6 import delete

    from freqinout.gui.controlfreq_tab import ControlFreqTab

    _app()
    tab = ControlFreqTab.__new__(ControlFreqTab)
    tab.top_overview_row = object()
    tab.top_splitter = object()
    delete(tab)

    # Queued zero-delay first-layout/restore callbacks are cancellation-safe.
    ControlFreqTab._update_responsive_layout(tab)
    ControlFreqTab._finalize_restored_ui_state(tab)


def test_sw3_single_worker_lane_keeps_one_active_and_one_pending_request(tmp_path: Path) -> None:
    from threading import Event
    from freqinout.gui.shortwave_tab import ShortwaveExploreView

    app = _app()
    view = ShortwaveExploreView(tmp_path / "freqinout_nets.db")
    started = Event()
    release = Event()
    try:
        view._active = True
        view._query_generation = 1
        view._start_task(1, lambda cancelled: (started.set(), release.wait(1), "first")[2], lambda result: None)
        assert started.wait(1)
        view._start_task(2, lambda cancelled: "second", lambda result: None)
        assert view._task_thread is not None and view._task_thread.isRunning()
        assert view._task_worker is not None
        assert view._pending_task is not None
        assert len(view._workers) == 1
        release.set()
        _drain(app, 0.5)
        assert view._pending_task is None
    finally:
        release.set()
        view.shutdown()
        view.close()
        app.processEvents()


def test_sw3_listening_runtime_surface_has_no_commandable_actions_or_scheduler_imports() -> None:
    source = Path("freqinout/gui/shortwave_tab.py").read_text(encoding="utf-8").lower()
    assert 'qpushbutton("tune now"' not in source
    assert 'qpushbutton("qsy"' not in source
    assert 'qpushbutton("launch"' not in source
    assert 'clicked.connect(self._tune' not in source
    assert "add to listening" in source
    assert "manual tuning only" in source
