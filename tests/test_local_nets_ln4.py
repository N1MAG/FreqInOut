"""LN-4C contract tests for the reminder-only Local Nets calendar.

All values in this package are synthetic.  In particular, no callsigns or
plausible station identities are used: Local Nets are tested as a calendar,
not as a radio-control input.
"""

from __future__ import annotations

import inspect
import json
import sqlite3
import time as wall_time
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from pathlib import Path

import pytest

from freqinout.core.local_net_models import LocalNetSchedule
from freqinout.core.navigation_intent import NavigationIntent
from freqinout.core.local_net_recurrence import next_occurrence, project_occurrences
from freqinout.core.local_net_service import (
    resource_status,
    resource_statuses,
    schedule_from_directory_session,
    schedule_with_accepted_frequency,
)
from freqinout.core.local_net_store import LocalNetStore
from freqinout.core.resource_catalog_models import (
    CatalogSource,
    FrequencyResource,
    NetDirectoryEntry,
    NetDirectorySession,
)
from freqinout.core.resource_catalog_store import ResourceCatalogStore


UTC = timezone.utc


def _utc(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


def _schedule(key: str = "local_net_synthetic", **changes: object) -> LocalNetSchedule:
    values: dict[str, object] = {
        "local_net_schedule_key": key,
        "name": "Synthetic Local Activity",
        "service": "AMATEUR",
        "recurrence": "daily",
        "local_start_time": "19:00",
        "timezone_name": "UTC",
        "duration_minutes": 60,
        "reminder_minutes": 15,
    }
    values.update(changes)
    return LocalNetSchedule(**values)


@pytest.fixture()
def nets_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use the real startup schema owner, but only in a temporary profile."""
    import freqinout.core.db_initializer as db_initializer

    monkeypatch.setattr(db_initializer, "_config_dir", lambda: tmp_path)
    db_initializer._ensure_nets_db()
    return tmp_path / "freqinout_nets.db"


def test_startup_schema_has_local_net_tables_and_store_is_read_only_until_initialized(
    tmp_path: Path, nets_db: Path
) -> None:
    with sqlite3.connect(nets_db) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {"local_net_schedules", "local_net_occurrence_state"} <= tables

    uninitialized = LocalNetStore(tmp_path / "not_initialized.db")
    assert uninitialized.get_schedule("missing") is None
    with pytest.raises(RuntimeError, match="schema is not initialized"):
        uninitialized.save_schedule(_schedule())


@pytest.mark.parametrize(
    ("recurrence", "kwargs", "expected_count"),
    [
        ("daily", {}, 3),
        ("weekly", {"weekdays": (0, 2)}, 2),
        ("periodic", {"weekdays": (0,), "month_weeks": (1,)}, 1),
        (
            "biweekly",
            {"weekdays": (0,), "biweekly_anchor_date": "2026-01-05"},
            1,
        ),
        (
            "one_time",
            {"one_time_local_date": "2026-01-07"},
            1,
        ),
    ],
)
def test_all_initial_recurrences_project_bounded_occurrences(
    recurrence: str, kwargs: dict[str, object], expected_count: int
) -> None:
    schedule = _schedule(recurrence, recurrence=recurrence, **kwargs)
    rows = project_occurrences(schedule, _utc(2026, 1, 5), horizon_days=3)
    assert len(rows) == expected_count
    assert all(row.local_net_schedule_key == recurrence for row in rows)
    assert list(rows) == sorted(rows, key=lambda row: row.start_utc)


def test_dst_gap_is_skipped_and_fall_fold_uses_first_deterministically() -> None:
    gap = _schedule(
        "dst_gap",
        local_start_time="02:30",
        timezone_name="America/New_York",
    )
    assert project_occurrences(gap, _utc(2026, 3, 8), horizon_days=1) == ()

    fold = _schedule(
        "dst_fold",
        local_start_time="01:30",
        timezone_name="America/New_York",
    )
    rows = project_occurrences(fold, _utc(2026, 11, 1), horizon_days=1)
    assert len(rows) == 1
    assert rows[0].start_utc == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    assert rows[0].local_start.fold == 0


def test_overnight_leap_day_effective_and_exception_boundaries() -> None:
    overnight = _schedule(
        "overnight",
        local_start_time="23:45",
        duration_minutes=120,
        effective_start_date="2028-02-28",
        effective_end_date="2028-03-01",
        exception_dates=("2028-02-29",),
    )
    rows = project_occurrences(overnight, _utc(2028, 2, 28), horizon_days=3)
    assert len(rows) == 2
    assert rows[0].local_start.date().isoformat() == "2028-02-28"
    assert rows[0].end_utc == rows[0].start_utc.replace(hour=1, day=29)
    assert all(row.local_start.date().isoformat() != "2028-02-29" for row in rows)
    assert rows[-1].local_start.date().isoformat() == "2028-03-01"


def test_horizon_and_limit_are_hard_bounded() -> None:
    schedule = _schedule("bounded", local_start_time="00:01")
    rows = project_occurrences(schedule, _utc(2026, 1, 1), horizon_days=999, limit=3)
    assert len(rows) == 3
    assert rows[-1].start_utc < _utc(2026, 1, 4)
    assert len(project_occurrences(schedule, _utc(2026, 1, 1), horizon_days=0)) == 0


def test_editor_model_supports_multi_weekday_all_month_weeks_dates_and_exact_next() -> None:
    schedule = _schedule(
        "rich_timing",
        recurrence="periodic",
        local_start_time="07:05",
        weekdays=(0, 2),
        month_weeks=(1, 2, 3, 4, 5),
        effective_start_date="2026-02-01",
        effective_end_date="2026-02-28",
        exception_dates=("2026-02-04",),
    )
    rows = project_occurrences(schedule, _utc(2026, 2, 1), horizon_days=28)
    assert rows
    assert all(row.local_start.strftime("%H:%M") == "07:05" for row in rows)
    assert all(row.local_start.date().isoformat() not in {"2026-01-31", "2026-03-01"} for row in rows)
    assert all(row.local_start.date().isoformat() != "2026-02-04" for row in rows)
    assert next_occurrence(schedule, _utc(2026, 2, 3, 8)).start_utc == rows[1].start_utc


def test_models_are_immutable_and_normalize_caller_collections() -> None:
    weekdays = [2, 0, 2]
    exceptions = ["2026-01-08", "2026-01-07", "2026-01-08"]
    schedule = _schedule("immutable", recurrence="weekly", weekdays=weekdays, exception_dates=exceptions)
    assert schedule.weekdays == (0, 2)
    assert schedule.exception_dates == ("2026-01-07", "2026-01-08")
    assert weekdays == [2, 0, 2]
    assert exceptions == ["2026-01-08", "2026-01-07", "2026-01-08"]
    with pytest.raises(FrozenInstanceError):
        schedule.name = "changed"  # type: ignore[misc]


def test_store_dismisses_one_occurrence_without_disabling_future_recurrence(
    nets_db: Path,
) -> None:
    store = LocalNetStore(nets_db)
    now = _utc(2026, 1, 5, 18)
    saved = store.save_schedule(_schedule("dismissible", local_start_time="19:00"), now_utc=now)
    first, second, *_ = store.upcoming(now, horizon_days=3, limit=10)
    assert first.occurrence_key == saved.local_net_schedule_key + "|2026-01-05T19:00:00Z"
    store.dismiss_occurrence(first, note="Synthetic exception")
    assert store.upcoming(now, horizon_days=3, limit=10)[0].occurrence_key == second.occurrence_key
    retained = store.upcoming(now, horizon_days=3, include_dismissed=True, limit=10)
    dismissed = next(row for row in retained if row.occurrence_key == first.occurrence_key)
    assert dismissed.dismissed and dismissed.operator_note == "Synthetic exception"
    assert store.get_schedule("dismissible").enabled is True  # type: ignore[union-attr]


def test_store_restart_and_cache_rollover_are_deterministic(nets_db: Path) -> None:
    first_store = LocalNetStore(nets_db)
    first_store.save_schedule(_schedule("rollover"), now_utc=_utc(2026, 1, 5, 18))
    before = first_store.get_schedule("rollover")
    assert before is not None and before.next_occurrence_utc == "2026-01-05T19:00:00Z"

    restarted = LocalNetStore(nets_db)
    assert restarted.get_schedule("rollover") == before
    assert restarted.refresh_next_occurrence_cache(_utc(2026, 1, 5, 20)) == 1
    after = LocalNetStore(nets_db).get_schedule("rollover")
    assert after is not None and after.next_occurrence_utc == "2026-01-06T19:00:00Z"


def _catalog_fixture(db_path: Path) -> tuple[ResourceCatalogStore, FrequencyResource, NetDirectorySession]:
    catalog = ResourceCatalogStore(db_path)
    catalog.create_schema()
    catalog.create_source(CatalogSource("source_synthetic", "Synthetic Source", "station"))
    frequency = catalog.create_frequency(
        FrequencyResource(
            "frequency_synthetic",
            "source_synthetic",
            "repeater",
            "GMRS",
            "Synthetic Repeater",
            band="UHF",
            receive_hz=462550000,
            transmit_hz=467550000,
        ),
        group_keys={"group_synthetic": "Group Synthetic"},
    )
    entry = catalog.create_net_entry(
        NetDirectoryEntry("entry_synthetic", "source_synthetic", "Synthetic Directory Activity"),
        group_keys={"group_synthetic": "Group Synthetic"},
    )
    session = catalog.create_session(
        NetDirectorySession(
            "session_synthetic",
            entry.net_entry_key,
            "source_synthetic",
            "GMRS",
            frequency_resource_key=frequency.frequency_resource_key,
            recurrence="Weekly",
            local_start_time="20:00",
            duration_minutes=45,
            timezone="UTC",
            day_utc="Wednesday",
            effective_start_date="2026-01-01",
            reminder_minutes=10,
            mode="FM",
        )
    )
    return catalog, frequency, session


def test_custom_and_directory_known_schedules_preserve_keys_groups_and_services(
    nets_db: Path,
) -> None:
    catalog, frequency, session = _catalog_fixture(nets_db)
    known = schedule_from_directory_session(catalog, session.net_session_key, schedule_key="local_known")
    assert known.net_entry_key == "entry_synthetic"
    assert known.net_session_key == "session_synthetic"
    assert known.frequency_resource_key == frequency.frequency_resource_key
    assert known.operating_group_key == "group_synthetic"
    assert known.service == "GMRS"
    assert known.accepted_session_version_hash == session.version_hash
    assert known.accepted_resource_version_hash == frequency.version_hash
    assert catalog.get_frequency(frequency.frequency_resource_key).receive_hz == 462550000  # type: ignore[union-attr]
    assert catalog.get_frequency(frequency.frequency_resource_key).transmit_hz == 467550000  # type: ignore[union-attr]

    custom = _schedule("local_custom", operating_group_key=None, operating_group_name=None)
    LocalNetStore(nets_db).save_schedule(known, now_utc=_utc(2026, 1, 5, 18))
    LocalNetStore(nets_db).save_schedule(custom, now_utc=_utc(2026, 1, 5, 18))
    assert LocalNetStore(nets_db).list_schedules(operating_group_key="group_synthetic")[0].local_net_schedule_key == "local_known"
    assert LocalNetStore(nets_db).get_schedule("local_custom").operating_group_key is None  # type: ignore[union-attr]


def test_resource_status_covers_current_missing_and_retired_states(nets_db: Path) -> None:
    catalog, frequency, session = _catalog_fixture(nets_db)
    schedule = schedule_from_directory_session(catalog, session.net_session_key, schedule_key="status")
    assert resource_status(catalog, schedule).state == "current"
    assert resource_status(catalog, _schedule("missing")).state == "missing"

    catalog.retire_frequency(frequency.frequency_resource_key)
    assert resource_status(catalog, schedule).state == "retired"


def test_resource_status_batch_has_fixed_read_count(
    nets_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog, _, session = _catalog_fixture(nets_db)
    schedules = tuple(
        schedule_from_directory_session(
            catalog,
            session.net_session_key,
            schedule_key=f"status_batch_{index}",
        )
        for index in range(200)
    )
    real_read_connection = catalog._read_connection
    reads = 0

    def counted_read_connection():
        nonlocal reads
        reads += 1
        return real_read_connection()

    monkeypatch.setattr(catalog, "_read_connection", counted_read_connection)
    statuses = resource_statuses(catalog, schedules)
    assert len(statuses) == 200
    assert {status.state for status in statuses.values()} == {"current"}
    assert reads == 2


def test_explicit_frequency_override_records_accepted_snapshot(nets_db: Path) -> None:
    catalog, _, session = _catalog_fixture(nets_db)
    schedule = schedule_from_directory_session(catalog, session.net_session_key)
    override = catalog.create_frequency(
        FrequencyResource(
            "frequency_override",
            "source_synthetic",
            "simplex",
            "GMRS",
            "Synthetic Override",
            receive_hz=462600000,
            transmit_hz=462600000,
        )
    )
    accepted = schedule_with_accepted_frequency(schedule, override)
    assert accepted.frequency_resource_key == override.frequency_resource_key
    assert accepted.accepted_resource_version_hash == override.version_hash
    assert json.loads(accepted.accepted_snapshot_json)["frequency"]["label"] == "Synthetic Override"
    assert resource_status(catalog, accepted).state == "current"


def test_directory_snapshot_tuple_list_round_trip_stays_current(nets_db: Path) -> None:
    catalog, _, session = _catalog_fixture(nets_db)
    updated_session = NetDirectorySession(
        session.net_session_key,
        session.net_entry_key,
        session.source_key,
        session.service,
        frequency_resource_key=session.frequency_resource_key,
        recurrence=session.recurrence,
        local_start_time=session.local_start_time,
        duration_minutes=session.duration_minutes,
        timezone=session.timezone,
        day_utc=session.day_utc,
        effective_start_date=session.effective_start_date,
        exception_dates=("2026-01-14", "2026-01-21"),
        reminder_minutes=session.reminder_minutes,
        mode=session.mode,
    )
    updated_session = catalog.update_session(updated_session)
    accepted = schedule_from_directory_session(catalog, updated_session.net_session_key, schedule_key="snapshot_round_trip")
    assert resource_status(catalog, accepted).state == "current"
    snapshot = json.loads(accepted.accepted_snapshot_json)["session"]
    comparison = catalog.compare_session_version(
        accepted.net_session_key,
        accepted.accepted_session_version_hash,
        snapshot,
    )
    assert comparison.diffs == ()


def test_scheduler_engine_static_schedule_loader_never_reads_local_net_inputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from freqinout.core.scheduler_engine import SchedulerEngine

    class Settings:
        payload = {
            "hf_schedule": [{"frequency": "7.111"}],
            "net_schedule": [{"frequency": "3.222"}],
            "local_net_schedules": [{"frequency": "462.550"}],
        }

        def all(self):
            return dict(self.payload)

        def get(self, key, default=None):
            return self.payload.get(key, default)

    engine = SchedulerEngine.__new__(SchedulerEngine)
    engine.settings = Settings()
    engine._config_dir = lambda: tmp_path
    engine._db_mtime = lambda _path: 0.0
    engine._primary_schedule_target_context = lambda: (None, None)
    engine._load_assigned_frequency_plan_schedule_rows = lambda _profile_id: ([], [], False)
    engine._sop_layer_enabled = lambda: True
    engine._filter_rows_for_runtime_target = lambda rows, **_kwargs: rows
    def _stub(name: str):
        return lambda *args, **kwargs: [] if "sop" in name else None

    for name in (
        "_load_daily_schedule_from_db",
        "_load_net_schedule_from_db",
        "_load_sop_schedule_layer_from_db",
        "_load_sop_net_conflict_policies_from_db",
    ):
        monkeypatch.setattr(engine, name, _stub(name))
    hf, net, sop, policies = engine._load_schedules(force=True)
    assert [(row["frequency"]) for row in hf] == ["7.111"]
    assert [(row["frequency"]) for row in net] == ["3.222"]
    assert sop == [] and policies == []
    assert "local_net" not in inspect.getsource(SchedulerEngine._load_schedules).lower()


def test_thousand_schedule_corpus_is_bounded_and_within_performance_budget(nets_db: Path) -> None:
    store = LocalNetStore(nets_db)
    now = _utc(2026, 1, 5, 18)
    started = wall_time.perf_counter()
    for index in range(1000):
        store.save_schedule(
            _schedule(f"bulk_{index:04d}", local_start_time="19:00"),
            now_utc=now,
        )
    elapsed_write = wall_time.perf_counter() - started
    started = wall_time.perf_counter()
    upcoming = store.upcoming(now, horizon_days=90, limit=500)
    elapsed_query = wall_time.perf_counter() - started
    assert len(upcoming) == 500
    assert elapsed_write < 20.0
    assert elapsed_query < 5.0


@pytest.mark.parametrize("size", [(900, 560), (1000, 700)])
def test_local_nets_ui_seam_is_reminder_only_and_responsive_when_available(
    nets_db: Path, size: tuple[int, int]
) -> None:
    module = pytest.importorskip("freqinout.gui.local_nets_tab")
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "Local Nets" in source
    assert "Reminder only" in source or "reminder only" in source.lower()
    assert "K1ABC" not in source and "N0CALL" not in source

    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication, QAbstractScrollArea, QAbstractButton

    app = QApplication.instance() or QApplication([])
    tab = module.LocalNetsTab(db_path=nets_db)
    try:
        tab.resize(*size)
        tab.show()
        app.processEvents()
        assert all("QSY" not in button.text().upper() for button in tab.findChildren(QAbstractButton))
        for scroll in tab.findChildren(QAbstractScrollArea):
            assert scroll.horizontalScrollBar().maximum() == 0
            assert scroll.horizontalScrollBarPolicy() in (Qt.ScrollBarAlwaysOff, Qt.ScrollBarAsNeeded)
    finally:
        tab.close()
        tab.deleteLater()


def test_local_nets_ui_large_text_dark_palette_and_plans_navigation_seam(
    nets_db: Path,
) -> None:
    module = pytest.importorskip("freqinout.gui.local_nets_tab")
    from PySide6.QtGui import QFont, QPalette
    from PySide6.QtWidgets import QApplication, QAbstractButton, QLabel

    app = QApplication.instance() or QApplication([])
    old_font = QFont(app.font())
    old_palette = QPalette(app.palette())
    large_font = QFont(old_font)
    large_font.setPointSizeF(max(16.0, old_font.pointSizeF() * 1.35))
    dark = QPalette(old_palette)
    dark.setColor(QPalette.Window, dark.color(QPalette.Window).darker(115))
    dark.setColor(QPalette.WindowText, dark.color(QPalette.WindowText))
    app.setFont(large_font)
    app.setPalette(dark)
    tab = module.LocalNetsTab(db_path=nets_db)
    try:
        tab.show()
        app.processEvents()
        assert getattr(module, "REMINDER_COPY", "").startswith("Reminder only")
        assert any("Local Nets" in label.text() for label in tab.findChildren(QLabel))
        for control in tab.findChildren(QAbstractButton):
            if control.isVisible() and control.text():
                assert control.height() >= control.fontMetrics().height()
    finally:
        tab.close()
        tab.deleteLater()
        app.setFont(old_font)
        app.setPalette(old_palette)

    main_window = Path(__file__).parents[1] / "freqinout" / "gui" / "main_window.py"
    navigation = main_window.read_text(encoding="utf-8")
    assert '"Plans"' in navigation and '"Local Nets"' in navigation


def test_editor_exposes_full_timing_and_community_unassigned_contract() -> None:
    module = pytest.importorskip("freqinout.gui.local_nets_tab")
    source = Path(module.__file__).read_text(encoding="utf-8")
    for label in (
        "Weekdays",
        "Month weeks",
        "Effective start",
        "Effective end",
        "Exception dates",
        "Community / Unassigned",
        "New receive Hz",
        "New transmit Hz",
    ):
        assert label in source
    assert "self.weekday_checks" in source
    assert "self.month_week_checks" in source
    assert "range(1, 6)" in source


def test_editor_draft_round_trips_multi_weekdays_dates_and_repeater_rx_tx(
    nets_db: Path,
) -> None:
    module = pytest.importorskip("freqinout.gui.local_nets_tab")
    from PySide6.QtWidgets import QApplication

    catalog = ResourceCatalogStore(nets_db)
    catalog.create_schema()
    app = QApplication.instance() or QApplication([])
    editor = module.LocalNetEditorDialog(LocalNetStore(nets_db), catalog, None)
    try:
        editor.name_edit.setText("Synthetic Rich Timing")
        editor.recurrence.setCurrentText("periodic")
        editor.weekday_checks[0].setChecked(True)
        editor.weekday_checks[2].setChecked(True)
        for checkbox in editor.month_week_checks:
            checkbox.setChecked(True)
        editor.effective_start_edit.setText("2026-02-01")
        editor.effective_end_edit.setText("2026-02-28")
        editor.exceptions_edit.setText("2026-02-04, 2026-02-18")
        editor.custom_kind.setCurrentText("repeater")
        editor.custom_receive_hz.setText("462550000")
        editor.custom_transmit_hz.setText("467550000")
        editor._create_frequency()
        draft = editor._draft()
        assert draft.weekdays == (0, 2)
        assert draft.month_weeks == (1, 2, 3, 4, 5)
        assert draft.effective_start_date == "2026-02-01"
        assert draft.effective_end_date == "2026-02-28"
        assert draft.exception_dates == ("2026-02-04", "2026-02-18")
        assert editor._selected_frequency is not None
        assert editor._selected_frequency.receive_hz == 462550000
        assert editor._selected_frequency.transmit_hz == 467550000
    finally:
        editor.close()
        editor.deleteLater()
        app.processEvents()


def test_dismiss_is_gated_to_an_active_reminder(nets_db: Path) -> None:
    module = pytest.importorskip("freqinout.gui.local_nets_tab")
    from PySide6.QtWidgets import QApplication

    LocalNetStore(nets_db).save_schedule(
        _schedule("far_future", effective_start_date="2099-01-01"),
        now_utc=_utc(2026, 1, 1),
    )
    app = QApplication.instance() or QApplication([])
    tab = module.LocalNetsTab(db_path=nets_db)
    try:
        tab.show()
        app.processEvents()
        tab.table.selectRow(0)
        app.processEvents()
        assert tab.dismiss_btn.isEnabled() is False
    finally:
        tab.close()
        tab.deleteLater()


def test_local_net_catalog_handoff_restores_unsaved_editor_state(nets_db: Path) -> None:
    module = pytest.importorskip("freqinout.gui.local_nets_tab")
    from PySide6.QtWidgets import QApplication

    catalog = ResourceCatalogStore(nets_db)
    catalog.create_schema()
    app = QApplication.instance() or QApplication([])
    editor = module.LocalNetEditorDialog(LocalNetStore(nets_db), catalog, None)
    captured: list[NavigationIntent] = []
    editor.handoff_requested.connect(captured.append)
    try:
        editor.name_edit.setText("Unsaved Community Activity")
        editor.time_edit.setText("not-finished")
        editor.custom_receive_hz.setText("146520000")
        editor._request_handoff("resources.frequency_catalog")
        assert len(captured) == 1
        intent = captured[0]
        assert intent.destination_route == "resources.frequency_catalog"
        assert intent.return_route == "plans.local_nets"
        assert intent.draft_snapshot["name"] == "Unsaved Community Activity"

        restored = module.LocalNetEditorDialog(
            LocalNetStore(nets_db), catalog, None,
            draft_snapshot=dict(intent.draft_snapshot),
        )
        try:
            assert restored.name_edit.text() == "Unsaved Community Activity"
            assert restored.time_edit.text() == "not-finished"
            assert restored.custom_receive_hz.text() == "146520000"
        finally:
            restored.close(); restored.deleteLater()
    finally:
        editor.close(); editor.deleteLater(); app.processEvents()


def test_navigation_intent_and_resources_return_context_are_immutable(nets_db: Path) -> None:
    module = pytest.importorskip("freqinout.gui.resources_tab")
    from PySide6.QtWidgets import QApplication

    intent = NavigationIntent(
        origin_surface="local_nets_editor",
        destination_route="resources.frequency_catalog",
        return_route="plans.local_nets",
        draft_snapshot={"name": "Synthetic Draft"},
    )
    with pytest.raises(TypeError):
        intent.draft_snapshot["name"] = "changed"  # type: ignore[index]

    app = QApplication.instance() or QApplication([])
    tab = module.ResourcesTab(db_path=nets_db)
    returned: list[object] = []
    tab.return_requested.connect(returned.append)
    try:
        tab.set_navigation_intent(intent)
        assert not tab.context_bar.isHidden()
        assert "unsaved selections are retained" in tab.context_label.text()
        tab.return_btn.click()
        assert returned == [intent]
        assert tab.context_bar.isHidden()
    finally:
        tab.close(); tab.deleteLater(); app.processEvents()


def test_settings_operating_groups_button_is_a_deep_link(nets_db: Path) -> None:
    module = pytest.importorskip("freqinout.gui.local_nets_tab")
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    tab = module.LocalNetsTab(db_path=nets_db)
    settings_routes: list[object] = []
    tab.open_settings_requested.connect(settings_routes.append)
    try:
        tab.settings_btn.click()
        app.processEvents()
        assert settings_routes == ["operating_groups"]
    finally:
        tab.close()
        tab.deleteLater()
