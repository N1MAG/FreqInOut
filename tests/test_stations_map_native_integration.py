"""Slice 2 contracts for StationsMapTab's native Qt Location handoff.

These tests keep the established map read model and selection semantics while
requiring the visible rendering boundary to be the native renderer.  They use
small local fakes so no station database, Qt WebEngine process, or tile network
request participates in the UI-path checks.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QApplication, QLabel, QSizePolicy, QStackedWidget, QWidget

import freqinout.gui.stations_map_tab as stations_map_module
from freqinout.gui.stations_map_tab import StationsMapTab, _MapProjectionSnapshotResult


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


class _FakeNativeMapRenderer(QWidget):
    action_requested = Signal(object)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.visible_values: list[bool] = []
        self.projections: list[dict[str, object]] = []
        self.centers: list[tuple[float, float, int]] = []

    def set_map_visible(self, visible: bool) -> None:
        self.visible_values.append(bool(visible))

    def apply_projection(self, payload: dict[str, object]) -> None:
        self.projections.append(dict(payload))

    def center_on(self, lat: float, lon: float, zoom: int = 6) -> None:
        self.centers.append((float(lat), float(lon), int(zoom)))

    def shutdown(self) -> None:
        pass


def test_stations_map_tab_live_renderer_paths_are_native_not_webengine() -> None:
    """No visible Map path may import, create, or call a browser surface."""
    source = Path("freqinout/gui/stations_map_tab.py").read_text(encoding="utf-8")

    assert "from freqinout.gui.native_map_renderer import NativeMapRenderer" in source
    assert "QWebEngine" not in source
    assert "runJavaScript" not in source
    assert "_build_leaflet_html" not in source
    assert "_ensure_webengine_imported" not in source


def test_stations_map_tab_constructs_native_renderer_once_in_stable_map_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tab creates one native child inside the pre-existing map viewport."""
    _app()
    monkeypatch.setattr(stations_map_module, "NativeMapRenderer", _FakeNativeMapRenderer)
    stack = QStackedWidget()
    loading = QLabel("Preparing native map", stack)
    stack.addWidget(loading)
    host = SimpleNamespace(
        _map_stack=stack,
        _map_loading_label=loading,
        _native_map_renderer=None,
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _on_native_map_action=lambda _payload: None,
    )

    first = StationsMapTab._ensure_native_map_renderer(host)
    second = StationsMapTab._ensure_native_map_renderer(host)

    assert first is second is host._native_map_renderer
    assert isinstance(first, _FakeNativeMapRenderer)
    assert first.parentWidget() is stack
    assert stack.currentWidget() is first
    assert first.sizePolicy().horizontalPolicy() is QSizePolicy.Ignored
    assert first.sizePolicy().verticalPolicy() is QSizePolicy.Ignored
    stack.deleteLater()
    _app().processEvents()


def test_native_projection_keeps_all_operational_marker_families_and_links_bounded() -> None:
    """The renderer receives one bounded snapshot, not a browser bootstrap."""
    renderer = _FakeNativeMapRenderer()
    host = SimpleNamespace(
        _native_map_renderer=renderer,
        _map_visible=True,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _last_map_payload_sig=None,
    )
    projection = {
        "markers": [{"id": "station-1", "lat": 39.7, "lon": -104.9}],
        "links": [{"origin": "station-1", "destination": "station-2"}],
        "weather_events": [{"id": "weather-1", "lat": 40.0, "lon": -105.0}],
        "alert_events": [{"id": "alert-1", "lat": 39.0, "lon": -104.0}],
        "infrastructure_events": [{"id": "infra-1", "lat": 38.5, "lon": -103.5}],
        "regional_intelligence": {"states": {"CO": {"severity": "yellow"}}},
        "view": {"lat": 39.5, "lon": -104.8, "zoom": 6},
    }

    StationsMapTab._apply_native_map_projection(host, projection)

    assert renderer.projections == [projection]
    assert host._pending_map_payload is None


def test_hidden_enriched_native_projection_retains_newest_then_visible_reentry_applies_once() -> None:
    """A completed worker snapshot never touches a hidden native surface."""
    renderer = _FakeNativeMapRenderer()
    old_payload = {
        "markers": [{"id": "old", "lat": 39.0, "lon": -104.0}],
        "links": [],
        "polygons": [],
        "_native_projection_enriched": True,
    }
    newest_payload = {
        "markers": [{"id": "new", "lat": 40.0, "lon": -105.0}],
        "links": [],
        "polygons": [],
        "_native_projection_enriched": True,
    }
    host = SimpleNamespace(
        _native_map_renderer=renderer,
        _map_visible=False,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _last_map_payload_sig=None,
    )

    StationsMapTab._apply_native_map_projection(host, old_payload)
    StationsMapTab._apply_native_map_projection(host, newest_payload)

    assert renderer.projections == []
    assert host._pending_map_payload == newest_payload

    host._map_visible = True
    StationsMapTab._apply_pending_native_map_projection(host)
    StationsMapTab._apply_pending_native_map_projection(host)

    assert renderer.projections == [newest_payload]
    assert host._pending_map_payload is None


def test_hidden_raw_projection_requeues_worker_enrichment_before_qml_apply() -> None:
    """Raw hidden payloads cannot bypass the worker on visible re-entry.

    The fast path is reserved for a completed/enriched worker snapshot.  That
    distinction prevents a reveal from building static overlays on the GUI
    thread while still allowing an already-complete snapshot to apply once.
    """
    renderer = _FakeNativeMapRenderer()
    raw_payload = {"markers": [{"id": "raw", "lat": 39.0, "lon": -104.0}], "links": [], "show_states": True}
    queued: list[dict[str, object]] = []
    raw_host = SimpleNamespace(
        _native_map_renderer=renderer,
        _map_visible=False,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _last_map_payload_sig=None,
        _queue_native_map_projection=lambda payload: queued.append(dict(payload)),
        _map_initialized=True,
        _map_load_ok=True,
        _map_dirty=False,
        _pending_refresh_level=0,
        _map_page_loading=False,
        _ensure_initial_data_loaded=lambda: None,
        _ensure_native_map_renderer=lambda: renderer,
        _set_map_runtime_state=lambda *_args: None,
        _map_ready_detail_text=lambda: "ready",
    )
    raw_host._apply_pending_native_map_projection = lambda: StationsMapTab._apply_pending_native_map_projection(raw_host)

    StationsMapTab._apply_native_map_projection(raw_host, raw_payload)
    raw_host._map_visible = True
    StationsMapTab._on_map_visible_deferred(raw_host)

    assert queued == [raw_payload]
    assert renderer.projections == []

    enriched_payload = {
        **raw_payload,
        "_native_projection_enriched": True,
        "polygons": [
            {
                "id": "state:CO",
                "kind": "state",
                "points": [{"lat": 38.0, "lon": -105.0}, {"lat": 39.0, "lon": -105.0}, {"lat": 39.0, "lon": -104.0}],
                "color_role": "info",
                "payload": {"state": "CO"},
            }
        ],
        "grid_lines": [],
        "grid_labels": [],
        "city_labels": [],
        "regional_summary": {},
        "legend": [],
    }
    completed_host = SimpleNamespace(
        _native_map_renderer=renderer,
        _map_visible=False,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _last_map_payload_sig=None,
        _map_payload_generation=9,
        _emit_map_event=lambda *_args, **_kwargs: None,
        _map_initialized=True,
        _map_load_ok=True,
        _map_dirty=False,
        _pending_refresh_level=0,
        _map_page_loading=False,
        _ensure_initial_data_loaded=lambda: None,
        _ensure_native_map_renderer=lambda: renderer,
        _set_map_runtime_state=lambda *_args: None,
        _map_ready_detail_text=lambda: "ready",
    )
    completed_host._apply_pending_native_map_projection = lambda: StationsMapTab._apply_pending_native_map_projection(completed_host)
    completed = _MapProjectionSnapshotResult(
        generation=9,
        payload="enriched-native-projection",
        signature="enriched-current",
        pending_payload=enriched_payload,
    )

    StationsMapTab._on_map_payload_snapshot_ready(completed_host, completed)
    assert completed_host._pending_map_payload == enriched_payload
    assert renderer.projections == []

    completed_host._map_visible = True
    StationsMapTab._on_map_visible_deferred(completed_host)
    StationsMapTab._on_map_visible_deferred(completed_host)

    assert renderer.projections == [enriched_payload]
    assert completed_host._pending_map_payload is None


def test_native_projection_completion_reuses_hidden_dirty_coalescing_contract() -> None:
    """The newest worker completion is retained; visibility re-entry owns application."""
    renderer = _FakeNativeMapRenderer()
    events: list[str] = []
    payload = {"markers": [{"id": "station-1", "lat": 39.7, "lon": -104.9}], "links": []}
    result = _MapProjectionSnapshotResult(
        generation=5,
        payload="native-projection",
        signature="current",
        pending_payload=payload,
    )
    host = SimpleNamespace(
        _native_map_renderer=renderer,
        _map_payload_generation=5,
        _map_visible=False,
        _app_active=True,
        _is_shutting_down=False,
        _pending_map_payload=None,
        _last_map_payload_sig=None,
        _emit_map_event=lambda event, **_kwargs: events.append(event),
    )

    StationsMapTab._on_map_payload_snapshot_ready(host, result)

    assert renderer.projections == []
    assert host._pending_map_payload == payload
    assert events == ["payload_update_deferred_hidden"]


def test_native_marker_and_path_actions_reuse_existing_detail_selection_semantics() -> None:
    """Native clicks feed the same selected-detail payload contract as prior map clicks."""
    selected: list[dict[str, object]] = []
    host = SimpleNamespace(_show_map_selected_detail=lambda payload: selected.append(dict(payload)))
    marker = {"type": "station", "callsign": "N0CALL", "lat": 39.7, "lon": -104.9}
    path = {
        "type": "path",
        "origin": "N0CALL",
        "destination": "K0CALL",
        "points": [{"lat": 39.7, "lon": -104.9}, {"lat": 40.1, "lon": -105.2}],
    }

    StationsMapTab._on_native_map_action(host, {"action": "select_marker", "marker": marker})
    StationsMapTab._on_native_map_action(host, {"action": "select_path", "path": path})

    assert selected == [marker, path]


def test_center_selected_detail_uses_native_bridge_not_browser_javascript() -> None:
    renderer = _FakeNativeMapRenderer()
    host = SimpleNamespace(
        _native_map_renderer=renderer,
        _map_selected_payload={"lat": 39.7392, "lon": -104.9903},
        _map_selected_latlon=lambda payload: (float(payload["lat"]), float(payload["lon"])),
    )

    StationsMapTab._center_map_selected_detail(host)

    assert renderer.centers == [(39.7392, -104.9903, 6)]
