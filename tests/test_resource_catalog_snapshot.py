from __future__ import annotations

import os
import threading
import time

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.resource_catalog_models import (
    CatalogSource,
    FrequencyResource,
    NetDirectoryEntry,
    NetDirectorySession,
    ResourceUsage,
)
from freqinout.gui.resource_catalog_snapshot import (
    ResourceCatalogSnapshot,
    ResourceCatalogSnapshotService,
    filter_entries,
    filter_frequencies,
)


def _snapshot(*, label: str = "North", entry_name: str = "County Net") -> ResourceCatalogSnapshot:
    source = CatalogSource("station", "Station Resources", "station")
    frequency = FrequencyResource("frequency_1", "station", "simplex", "AMATEUR", label, center_hz=7_100_000)
    entry = NetDirectoryEntry("entry_1", "station", entry_name)
    session = NetDirectorySession("session_1", entry.net_entry_key, "station", "AMATEUR", frequency.frequency_resource_key)
    return ResourceCatalogSnapshot(
        {source.source_key: source}, (frequency,), (entry,), {entry.net_entry_key: (session,)},
        {frequency.frequency_resource_key: ResourceUsage(frequency.frequency_resource_key, 0, {})},
        {entry.net_entry_key: ResourceUsage(entry.net_entry_key, 0, {})},
        {session.net_session_key: ResourceUsage(session.net_session_key, 0, {})},
    )


def test_snapshot_filters_are_pure_cache_operations() -> None:
    snapshot = _snapshot()

    assert [item.frequency_resource_key for item in filter_frequencies(snapshot, search="north", service="AMATEUR", active=True)] == ["frequency_1"]
    assert not filter_frequencies(snapshot, search="north", service="GMRS", active=True)
    assert [item.net_entry_key for item in filter_entries(snapshot, search="county", active=True)] == ["entry_1"]


def test_snapshot_service_discards_stale_generations() -> None:
    first_gate = threading.Event()
    calls = 0

    def loader(_store: object) -> ResourceCatalogSnapshot:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_gate.wait(timeout=1)
            return _snapshot(label="stale")
        return _snapshot(label="current")

    service = ResourceCatalogSnapshotService(object(), loader=loader)  # type: ignore[arg-type]
    assert service.request() == 1
    assert service.request() == 2
    deadline = time.monotonic() + 1
    completion = None
    while time.monotonic() < deadline:
        completion = service.take_latest()
        if completion is not None:
            break
        time.sleep(0.005)
    assert completion is not None
    assert completion.generation == 2
    assert completion.snapshot and completion.snapshot.frequencies[0].label == "current"
    first_gate.set()
    time.sleep(0.02)
    assert service.take_latest() is None


def test_frequency_view_typing_selection_resize_and_theme_path_do_not_query_store() -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from freqinout.gui.frequency_catalog_view import FrequencyCatalogView

    class RecordingStore:
        calls = 0

        def __getattr__(self, _name: str):
            self.calls += 1
            raise AssertionError("cache-only Resources interaction queried the store")

    app = QApplication.instance() or QApplication([])
    store = RecordingStore()
    view = FrequencyCatalogView(store, snapshot_service=ResourceCatalogSnapshotService(store, loader=lambda _store: _snapshot()))
    try:
        view.show()
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and not view.table.rowCount():
            app.processEvents()
            time.sleep(0.005)
        assert view.table.rowCount() == 1
        view.search_edit.setText("North")
        view.table.selectRow(0)
        view.resize(900, 560)
        view.setStyleSheet("QLabel { color: palette(windowText); }")
        app.processEvents()
        assert "North" in view.detail_label.text()
        assert store.calls == 0
    finally:
        view.close()
        view.deleteLater()
        app.processEvents()
