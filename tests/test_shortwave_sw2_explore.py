"""SW-2 Explore/Data Sources contract tests.

These tests intentionally name the small Qt-free Explore service boundary so
that time evaluation, bounded query generation, and stale-result handling stay
independent from the Shortwave widget.  UI checks exercise only the public
Resources route and observable accessibility/lazy-loading behavior.
"""

from __future__ import annotations

import importlib
import inspect
import json
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from tests.test_shortwave_sw1_eibi import FIXTURE, _entries, _entry_by_station, _parse_fixture, _store


def _query_module():
    try:
        return importlib.import_module("freqinout.core.shortwave_query")
    except ModuleNotFoundError:
        pytest.fail("SW-2 must expose freqinout.core.shortwave_query")


def _shortwave_ui_module():
    try:
        return importlib.import_module("freqinout.gui.shortwave_tab")
    except ModuleNotFoundError:
        pytest.fail("SW-2 must expose freqinout.gui.shortwave_tab")


def _shortwave_workspace_type(module: object):
    return getattr(module, "ShortwaveTab", None) or getattr(module, "ShortwaveWorkspace", None)


def _public(module: object, *names: str):
    for name in names:
        value = getattr(module, name, None)
        if callable(value):
            return value
    pytest.fail(f"SW-2 service must expose one of: {', '.join(names)}")


def _call_with_supported(fn: object, values: Mapping[str, Any]) -> Any:
    signature = inspect.signature(fn)
    kwargs = {name: value for name, value in values.items() if name in signature.parameters}
    return fn(**kwargs)  # type: ignore[operator]


def _persisted_fixture(tmp_path: Path):
    store = _store(tmp_path / "freqinout_nets.db")
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import

    apply_shortwave_import(store, preview_shortwave_import(store, _parse_fixture()))
    dataset = store.current_dataset()
    assert dataset is not None
    entries = store.list_entries(
        dataset_key=dataset.dataset_key,
        include_inactive=True,
        parse_states=("complete", "special", "review"),
        limit=200,
    )
    return store, dataset, list(entries)


def _persist_candidate(tmp_path: Path, candidate: object):
    store = _store(tmp_path / "freqinout_nets.db")
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import

    apply_shortwave_import(store, preview_shortwave_import(store, candidate))
    dataset = store.current_dataset()
    assert dataset is not None
    return store, dataset


def _evaluate(entry: object, dataset: object, now_utc: datetime, window_end: datetime) -> Any:
    module = _query_module()
    fn = getattr(module, "evaluate_listing", None)
    assert callable(fn)
    return fn(entry, dataset, now_utc, window_end)


def _station(entries: list[Any], text: str) -> Any:
    for entry in entries:
        candidate = getattr(entry, "candidate", entry)
        if text in str(getattr(candidate, "station_name", "")):
            return entry
    pytest.fail(f"fixture entry not found: {text}")


def _flag(value: Any, *keys: str, default: bool = False) -> bool:
    if isinstance(value, Mapping):
        for key in keys:
            if key in value:
                return bool(value[key])
    for key in keys:
        if hasattr(value, key):
            return bool(getattr(value, key))
    return default


def test_sw2_time_evaluation_handles_2400_cross_midnight_and_weekday(tmp_path: Path) -> None:
    _store_obj, dataset, entries = _persisted_fixture(tmp_path)
    daily = _station(entries, "Latin-1 station")
    cross = _station(entries, "Cross midnight")
    window = datetime(2026, 5, 4, 23, 0, tzinfo=timezone.utc) + timedelta(hours=3)

    assert _evaluate(daily, dataset, datetime(2026, 5, 4, 12, tzinfo=timezone.utc), window) is not None
    assert _evaluate(cross, dataset, datetime(2026, 5, 4, 23, 45, tzinfo=timezone.utc), window) is not None
    # The Tuesday after a Monday start is still inside the cross-midnight
    # window; evaluating only the clock would incorrectly discard it.
    assert _evaluate(cross, dataset, datetime(2026, 5, 5, 0, 15, tzinfo=timezone.utc), window) is not None
    assert _evaluate(cross, dataset, datetime(2026, 5, 5, 2, tzinfo=timezone.utc), window) is None


def test_sw2_special_rules_and_ddmm_are_not_confident_outside_supported_bounds(tmp_path: Path) -> None:
    _store_obj, dataset, entries = _persisted_fixture(tmp_path)
    irregular = _station(entries, "Special irregular")
    fixed = _station(entries, "Fixed date")

    irregular_state = _evaluate(irregular, dataset, datetime(2026, 5, 4, 4, 30, tzinfo=timezone.utc), datetime(2026, 5, 4, 6, tzinfo=timezone.utc))
    assert irregular_state is None
    fixed_state = _evaluate(fixed, dataset, datetime(2026, 9, 15, 6, 30, tzinfo=timezone.utc), datetime(2026, 9, 15, 8, tzinfo=timezone.utc))
    assert fixed_state is None, "unsupported DDMM/special rows stay out of confident now/soon results"
    # A DDMM row from A26 cannot be treated as current after its season ends.
    out_of_season = _evaluate(fixed, dataset, datetime(2026, 11, 15, 6, 30, tzinfo=timezone.utc), datetime(2026, 11, 15, 8, tzinfo=timezone.utc))
    assert out_of_season is None


def test_sw2_now_and_soon_queries_do_not_starve_or_overlap_cross_midnight_rows(tmp_path: Path) -> None:
    store, _dataset, _entries = _persisted_fixture(tmp_path)
    from freqinout.core.shortwave_query import ShortwaveQuery, query_shortwave

    now = datetime(2026, 5, 4, 23, 0, tzinfo=timezone.utc)
    now_result = query_shortwave(
        store,
        ShortwaveQuery(search="Cross midnight", timing="scheduled_now", classifications=(), soon_minutes=60),
        now_utc=now,
    )
    soon_result = query_shortwave(
        store,
        ShortwaveQuery(search="Cross midnight", timing="starting_soon", classifications=(), soon_minutes=60),
        now_utc=now,
    )
    assert now_result.rows == ()
    assert len(soon_result.rows) == 1
    assert soon_result.rows[0].listing_state == "Starting soon"

    after_midnight = query_shortwave(
        store,
        ShortwaveQuery(search="Cross midnight", timing="scheduled_now", classifications=(), soon_minutes=60),
        now_utc=datetime(2026, 5, 5, 0, 15, tzinfo=timezone.utc),
    )
    assert len(after_midnight.rows) == 1
    assert after_midnight.rows[0].listing_state == "Scheduled now"


def test_sw2_ddmm_evaluation_maps_winter_season_across_new_year(tmp_path: Path) -> None:
    from freqinout.core.shortwave_eibi_provider import parse_eibi_dataset
    from freqinout.core.shortwave_query import evaluate_listing

    raw = FIXTURE.read_bytes().replace(b";15Sep;", b";Fr;").replace(b";1509;1509", b";1501;1501")
    candidate = parse_eibi_dataset(
        raw,
        Path("/Users/bill/RadioTools/Programs/shortwave/eibi_README.TXT"),
        season_code="B26",
        season_effective_from_utc="2026-10-25T00:00:00Z",
        season_effective_to_utc="2027-03-28T23:59:59Z",
    )
    store, dataset = _persist_candidate(tmp_path, candidate)
    entry = next(item for item in store.list_entries(dataset_key=dataset.dataset_key, include_inactive=True, parse_states=("complete",), limit=200) if item.candidate.station_name == "Fixed date service")
    assert evaluate_listing(entry, dataset, datetime(2027, 1, 15, 6, 30, tzinfo=timezone.utc), datetime(2027, 1, 15, 8, tzinfo=timezone.utc)) is not None
    assert evaluate_listing(entry, dataset, datetime(2026, 10, 24, 6, 30, tzinfo=timezone.utc), datetime(2026, 10, 24, 8, tzinfo=timezone.utc)) is None
    assert evaluate_listing(entry, dataset, datetime(2027, 3, 29, 6, 30, tzinfo=timezone.utc), datetime(2027, 3, 29, 8, tzinfo=timezone.utc)) is None


def test_sw2_classification_union_includes_other_review_rows(tmp_path: Path) -> None:
    store, _dataset, _entries = _persisted_fixture(tmp_path)
    from freqinout.core.shortwave_query import ShortwaveQuery, query_shortwave

    result = query_shortwave(
        store,
        ShortwaveQuery(timing="all_listings", classifications=("broadcast", "other"), limit=200),
        now_utc=datetime(2026, 5, 4, 12, tzinfo=timezone.utc),
    )
    names = {item.entry.candidate.station_name for item in result.rows}
    assert "BUE Latin-1 station" in names
    assert "Special irregular service" in names
    assert "Fixed date service" in names


def test_sw2_language_target_band_and_historical_dataset_filters(tmp_path: Path) -> None:
    store, a26, _entries = _persisted_fixture(tmp_path)
    from freqinout.core.shortwave_eibi_provider import parse_eibi_dataset
    from freqinout.core.shortwave_query import ShortwaveQuery, query_shortwave
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import

    language = query_shortwave(store, ShortwaveQuery(timing="all_listings", language="IT", classifications=()), now_utc=datetime(2026, 5, 4, 12, tzinfo=timezone.utc))
    assert any(item.entry.candidate.station_name == "Fixed date service" for item in language.rows)
    target_and_band = query_shortwave(
        store,
        ShortwaveQuery(timing="all_listings", target="East UR", frequency_min_hz=15_000_000, frequency_max_hz=15_200_000, classifications=()),
        now_utc=datetime(2026, 5, 4, 12, tzinfo=timezone.utc),
    )
    assert target_and_band.rows
    assert all(15_000_000 <= item.entry.candidate.frequency_hz <= 15_200_000 for item in target_and_band.rows)

    b26 = parse_eibi_dataset(
        FIXTURE,
        Path("/Users/bill/RadioTools/Programs/shortwave/eibi_README.TXT"),
        season_code="B26",
        season_effective_from_utc="2026-10-25T00:00:00Z",
        season_effective_to_utc="2027-03-28T23:59:59Z",
    )
    apply_shortwave_import(store, preview_shortwave_import(store, b26))
    historical = query_shortwave(
        store,
        ShortwaveQuery(dataset_key=a26.dataset_key, timing="all_listings", search="Latin-1 station", classifications=()),
        now_utc=datetime(2027, 1, 15, 12, tzinfo=timezone.utc),
    )
    assert historical.dataset is not None
    assert historical.dataset.dataset_key == a26.dataset_key
    assert historical.rows


def test_sw2_real_a26_warm_query_p95_stays_below_100ms(tmp_path: Path) -> None:
    from freqinout.core.shortwave_eibi_provider import bundled_eibi_seed_paths, parse_eibi_dataset
    from freqinout.core.shortwave_query import ShortwaveQuery, query_shortwave
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import

    csv_path, readme_path = bundled_eibi_seed_paths()
    candidate = parse_eibi_dataset(
        csv_path,
        readme_path,
        season_code="A26",
        season_effective_from_utc="2026-04-01T00:00:00Z",
        season_effective_to_utc="2026-10-31T23:59:59Z",
    )
    store = _store(tmp_path / "freqinout_nets.db")
    apply_shortwave_import(store, preview_shortwave_import(store, candidate))
    request = ShortwaveQuery(search="9955", timing="all_listings", classifications=(), limit=200)
    query_shortwave(store, request, now_utc=datetime(2026, 5, 4, 12, tzinfo=timezone.utc))
    samples = []
    for _ in range(10):
        started = time.perf_counter()
        result = query_shortwave(store, request, now_utc=datetime(2026, 5, 4, 12, tzinfo=timezone.utc))
        samples.append(time.perf_counter() - started)
        assert result.rows
        assert len(result.rows) <= 200
    p95 = sorted(samples)[9]
    assert p95 < 0.100, f"warm A26 filtered query p95 was {p95 * 1000:.1f} ms"


def test_sw2_real_a26_now_and_soon_pages_are_separate_and_not_starved(tmp_path: Path) -> None:
    from freqinout.core.shortwave_eibi_provider import bundled_eibi_seed_paths, parse_eibi_dataset
    from freqinout.core.shortwave_query import ShortwaveQuery, query_shortwave
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import

    csv_path, readme_path = bundled_eibi_seed_paths()
    candidate = parse_eibi_dataset(
        csv_path,
        readme_path,
        season_code="A26",
        season_effective_from_utc="2026-04-01T00:00:00Z",
        season_effective_to_utc="2026-10-31T23:59:59Z",
    )
    store = _store(tmp_path / "freqinout_nets.db")
    apply_shortwave_import(store, preview_shortwave_import(store, candidate))
    now = datetime(2026, 9, 10, 23, 0, tzinfo=timezone.utc)
    scheduled = query_shortwave(
        store, ShortwaveQuery(timing="scheduled_now", classifications=(), soon_minutes=120), now_utc=now
    )
    soon = query_shortwave(
        store, ShortwaveQuery(timing="starting_soon", classifications=(), soon_minutes=120), now_utc=now
    )
    scheduled_keys = {row.entry.entry_key for row in scheduled.rows}
    soon_keys = {row.entry.entry_key for row in soon.rows}
    assert len(scheduled.rows) == 200
    assert len(soon.rows) == 200
    assert scheduled_keys.isdisjoint(soon_keys)
    assert all(row.listing_state == "Scheduled now" for row in scheduled.rows)
    assert all(row.listing_state == "Starting soon" for row in soon.rows)


def _process_until(app: object, predicate: object, *, timeout: float = 3.0) -> bool:
    deadline = time.perf_counter() + timeout
    process_events = getattr(app, "processEvents")
    while time.perf_counter() < deadline:
        process_events()
        if bool(predicate()):
            return True
        time.sleep(0.01)
    process_events()
    return bool(predicate())


def test_sw2_explore_worker_coalesces_rapid_refreshes_and_shuts_down_cleanly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    import freqinout.gui.shortwave_tab as shortwave_tab
    from freqinout.core.shortwave_query import query_shortwave as real_query
    from freqinout.gui.shortwave_tab import ShortwaveExploreView

    store, _dataset, _entries = _persisted_fixture(tmp_path)
    app = QApplication.instance() or QApplication([])
    original = shortwave_tab.query_shortwave

    def slow_query(*args: object, **kwargs: object) -> object:
        time.sleep(0.12)
        return real_query(*args, **kwargs)

    monkeypatch.setattr(shortwave_tab, "query_shortwave", slow_query)
    view = ShortwaveExploreView(store.db_path)
    try:
        view.set_active(True)
        assert _process_until(app, lambda: view._task_thread is not None and view._task_thread.isRunning())
        for text in ("9955", "15000", "Cross midnight"):
            view.search.setText(text)
            view.refresh_results()
        assert view._task_thread is not None and view._task_thread.isRunning()
        assert view._pending_task is not None
        assert len(view._pending_task) == 3
        assert len(view._workers) <= 1
        assert _process_until(app, lambda: view._task_thread is None and view._pending_task is None)
    finally:
        view.shutdown()
        app.processEvents()
        assert view._task_thread is None or not view._task_thread.isRunning()
        monkeypatch.setattr(shortwave_tab, "query_shortwave", original)


def test_sw2_data_sources_pending_task_is_a_three_tuple_and_runs_latest_follow_up(
    tmp_path: Path,
) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.shortwave_tab import ShortwaveDataSourcesView

    app = QApplication.instance() or QApplication([])
    view = ShortwaveDataSourcesView(tmp_path / "resources.db")
    completed: list[str] = []

    def operation(label: str):
        def run(_cancelled: object) -> str:
            time.sleep(0.12)
            return label
        return run

    try:
        view._active = True
        view._run(operation("first"), completed.append)
        assert _process_until(app, lambda: view._task_thread is not None and view._task_thread.isRunning())
        view._run(operation("second"), completed.append)
        assert view._pending_task is not None
        assert len(view._pending_task) == 3
        assert _process_until(app, lambda: view._task_thread is None and view._pending_task is None)
        assert completed == ["second"]
    finally:
        view.shutdown()
        view.deleteLater()
        app.processEvents()


def test_sw2_listing_detail_hides_technical_codes_until_toggled_and_shows_utc_local(
    tmp_path: Path,
) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtCore import QModelIndex
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.shortwave_tab import ShortwaveExploreView

    store, _dataset, _entries = _persisted_fixture(tmp_path)
    app = QApplication.instance() or QApplication([])
    view = ShortwaveExploreView(store.db_path)
    try:
        view.show()
        view.set_active(True)
        view.timing.setCurrentIndex(2)  # All listings gives a deterministic fixture row.
        view.search.setText("BUE Latin")
        view.refresh_results()
        assert _process_until(app, lambda: view._task_thread is None and view._current_result is not None)
        assert view.model.rowCount() > 0
        view.table.selectRow(0)
        app.processEvents()
        detail = view.detail.toPlainText()
        assert not view.technical_toggle.isChecked()
        assert "Technical details" not in detail
        assert "UTC" in detail and " · " in detail, "detail must label UTC and local time separately"
        view.technical_toggle.setChecked(True)
        app.processEvents()
        assert "Technical details" in view.detail.toPlainText()
    finally:
        view.shutdown()
        view.deleteLater()
        app.processEvents()


@pytest.mark.parametrize("query", ["9955", "9.955", "9955 kHz"])
def test_sw2_frequency_search_normalization_is_canonical_hz(query: str) -> None:
    module = _query_module()
    fn = getattr(module, "normalize_frequency_search")
    result = fn(query)
    assert result == 9_955_000


def test_sw2_query_is_bounded_and_does_not_materialize_the_corpus(tmp_path: Path) -> None:
    module = _query_module()
    store = _store(tmp_path / "freqinout_nets.db")
    candidate = _parse_fixture()
    from freqinout.core.shortwave_import import apply_shortwave_import, preview_shortwave_import

    apply_shortwave_import(store, preview_shortwave_import(store, candidate))
    query = module.ShortwaveQuery(search="9955", timing="all_listings", limit=10_000)
    result = module.query_shortwave(store, query, now_utc=datetime(2026, 5, 4, 12, tzinfo=timezone.utc))
    rows = result.rows
    assert len(rows) <= 200


def test_sw2_request_generation_discards_stale_results_and_accepts_latest() -> None:
    pytest.importorskip("PySide6")
    module = _shortwave_ui_module()
    view_type = getattr(module, "ShortwaveExploreView", None)
    assert view_type is not None, "SW-2 must expose ShortwaveExploreView"
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication([])
    view = view_type(Path("/tmp/sw2-generation-test.db"))
    try:
        view._active = True
        view._query_generation = 2
        applied: list[object] = []
        view._finish_task(1, object(), applied.append)
        assert applied == []
        view._finish_task(2, object(), applied.append)
        assert len(applied) == 1
    finally:
        view.deleteLater()
        app.processEvents()


def test_sw2_hidden_tab_is_idle_and_activation_is_lazy() -> None:
    pytest.importorskip("PySide6")
    module = _shortwave_ui_module()
    module = _shortwave_ui_module()
    view_type = getattr(module, "ShortwaveExploreView", None)
    assert view_type is not None
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication([])
    view = view_type(Path("/tmp/sw2-lifecycle-test.db"))
    try:
        assert not view._active
        view.set_active(False)
        assert not view._debounce.isActive()
        before = len(view._workers)
        time.sleep(0.05)
        assert len(view._workers) == before == 0, "hidden Explore must not poll"
    finally:
        view.deleteLater()
        app.processEvents()


def test_sw2_search_shutdown_is_bounded_and_releases_single_flight_worker() -> None:
    pytest.importorskip("PySide6")
    module = _shortwave_ui_module()
    owner_type = getattr(module, "ShortwaveExploreView", None)
    assert owner_type is not None
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance() or QApplication([])
    owner = owner_type(Path("/tmp/sw2-shutdown-test.db"))
    started = time.perf_counter()
    owner.set_active(False)
    owner.deleteLater()
    app.processEvents()
    assert time.perf_counter() - started < 1.0
    assert not bool(owner._workers)


@pytest.mark.parametrize("width,height", [(1000, 700), (900, 560)])
def test_sw2_shortwave_route_is_lazy_bounded_and_accessible_at_compact_sizes(
    tmp_path: Path, width: int, height: int
) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication, QAbstractScrollArea, QAbstractButton, QLabel

    from freqinout.gui.shortwave_tab import ShortwaveWorkspace

    app = QApplication.instance() or QApplication([])
    tab = ShortwaveWorkspace(db_path=tmp_path / "resources.db")
    try:
        tab.resize(width, height)
        tab.show()
        app.processEvents()
        assert any("Shortwave" in label.text() for label in tab.findChildren(QLabel))
        page = tab._pages[0]
        assert all(
            button.accessibleName()
            for button in page.findChildren(QAbstractButton)
            if button.isVisible() and button.text().strip()
        )
        for scroll in page.findChildren(QAbstractScrollArea):
            assert scroll.horizontalScrollBar().maximum() == 0
        tables = [table for table in page.findChildren(QAbstractScrollArea) if hasattr(table, "rowCount")]
        assert all(table.rowCount() <= 200 for table in tables)
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_sw2_source_controls_expose_preview_apply_and_rollback_without_auto_network(tmp_path: Path) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication, QAbstractButton

    from freqinout.gui.shortwave_tab import ShortwaveWorkspace

    app = QApplication.instance() or QApplication([])
    tab = ShortwaveWorkspace(db_path=tmp_path / "resources.db")
    try:
        tab.tabs.setCurrentIndex(next(index for index in range(tab.tabs.count()) if tab.tabs.tabText(index) == "Data Sources"))
        app.processEvents()
        labels = " ".join(button.text() for button in tab.findChildren(QAbstractButton))
        assert "Review bundled snapshot" in labels
        assert "Apply reviewed import" in labels
        assert "Restore selected dataset" in labels
        assert not any("network" in button.toolTip().lower() for button in tab.findChildren(QAbstractButton))
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_sw2_large_text_and_dark_theme_keep_keyboard_controls_usable(tmp_path: Path) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtCore import Qt
    from PySide6.QtGui import QFont, QPalette
    from PySide6.QtWidgets import QApplication, QAbstractButton

    from freqinout.gui.shortwave_tab import ShortwaveWorkspace

    app = QApplication.instance() or QApplication([])
    old_font = QFont(app.font())
    old_palette = QPalette(app.palette())
    large_font = QFont(old_font)
    large_font.setPointSizeF(max(16.0, old_font.pointSizeF() * 1.35))
    dark = QPalette(old_palette)
    dark.setColor(QPalette.Window, dark.color(QPalette.Window).darker(115))
    app.setFont(large_font)
    app.setPalette(dark)
    tab = ShortwaveWorkspace(db_path=tmp_path / "resources.db")
    try:
        tab.show()
        app.processEvents()
        controls = [button for button in tab._pages[0].findChildren(QAbstractButton) if button.isVisible() and button.text()]
        assert controls
        assert all(control.focusPolicy() != Qt.NoFocus for control in controls)
        assert all(control.height() >= control.fontMetrics().height() for control in controls)
    finally:
        tab.close()
        tab.deleteLater()
        app.setFont(old_font)
        app.setPalette(old_palette)


def test_sw2_shortwave_is_a_resources_master_route_not_an_internal_resources_tab() -> None:
    resources_source = Path("freqinout/gui/resources_tab.py").read_text(encoding="utf-8")
    main_window_source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    assert "Shortwave" not in resources_source.split("TAB_LABELS", 1)[1].split("\n", 1)[0]
    assert "resources.shortwave" in main_window_source
    assert "Shortwave" in main_window_source
    assert "ShortwaveWorkspace" in main_window_source or "ShortwaveTab" in main_window_source
