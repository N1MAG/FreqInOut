from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def _method_block(source: str, marker: str) -> str:
    start = source.index(marker)
    next_method = source.find("\n    def ", start + 1)
    if next_method == -1:
        return source[start:]
    return source[start:next_method]


def test_operator_history_refresh_does_not_render_hidden_map() -> None:
    source = _read("freqinout/gui/main_window.py")
    block = _method_block(source, "    def refresh_operator_history_views")

    assert 'map_visible = bool(getattr(self.stations_map_tab, "_map_visible", False))' in block
    assert 'elif not map_visible and hasattr(self.stations_map_tab, "_map_dirty")' in block
    assert "self.stations_map_tab._map_dirty = True" in block

    before_visibility_gate = block.split("if map_visible", 1)[0]
    assert "self.stations_map_tab._schedule_render()" not in before_visibility_gate


def test_fldigi_start_net_defers_cross_tab_operator_history_refresh() -> None:
    source = _read("freqinout/gui/fldigi_net_control_tab.py")
    block = _method_block(source, "    def _start_net")

    assert "QTimer.singleShot(0, self._refresh_operator_history_views)" in block
    assert "self._refresh_operator_history_views()" not in block


def test_map_scheduler_is_hidden_view_safe() -> None:
    source = _read("freqinout/gui/stations_map_tab.py")
    block = _method_block(source, "    def _schedule_render")

    assert 'not getattr(self, "_map_visible", False)' in block
    assert "self._map_dirty = True" in block
    hidden_branch = block.split('not getattr(self, "_map_visible", False)', 1)[1].split("return", 1)[0]
    assert "self._request_map_refresh" not in hidden_branch


def test_map_refresh_timer_fallback_does_not_recurse_through_schedule_render() -> None:
    source = _read("freqinout/gui/stations_map_tab.py")
    block = _method_block(source, "    def _request_map_refresh")

    assert 'timer = getattr(self, "_map_refresh_timer", None)' in block
    timer_fallback = block.split("if timer is None:", 1)[1].split("return", 1)[0]
    assert "self._schedule_render()" not in timer_fallback
    assert "self._flush_requested_map_refresh" in timer_fallback


def test_embedded_compose_hides_splitter_handles() -> None:
    source = _read("freqinout/gui/message_viewer_tab.py")
    block = _method_block(source, "    def _refresh_compose_splitter_handles")

    assert 'getattr(self, "_compose_in_workbench", False)' in block
    assert "handle_width = 10 if in_workbench else 0" in block
    assert "splitter.setHandleWidth(handle_width)" in block
