"""Focused CMW-5 tests: adopt the target already selected in JS8Call.

The service tests exercise the endpoint boundary without a socket.  The source
contract tests intentionally pin the GUI's safety/lifecycle seams so a later
refactor cannot accidentally turn live JS8 state into draft state or couple
socket work to repaint/preview/editing.
"""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest
from PySide6.QtWidgets import QApplication, QLineEdit

from freqinout.gui import message_viewer_tab
from freqinout.gui.message_viewer_tab import (
    ComposeRadioTarget,
    MessageViewerTab,
    _ComposeJs8SelectedTargetWorker,
)

from freqinout.core.js8_send_service import (
    JS8SelectedTargetState,
    query_js8_selected_target,
)
from freqinout.radio_interface.js8_api_client import JS8ApiEndpoint


class _FakeClient:
    def __init__(self, response=None, error: Exception | None = None, connected: bool = True):
        self.endpoint = JS8ApiEndpoint("127.0.0.1", 2442)
        self.response = response
        self.error = error
        self.is_running = True
        self.is_connected = connected
        self.requests: list[tuple[str, float]] = []

    def start(self) -> None:
        self.is_running = True

    def request(self, command: str, *, expect_types, timeout_s: float):
        self.requests.append((command, timeout_s))
        if self.error:
            raise self.error
        return self.response


@pytest.mark.parametrize("value", ["K1ABC", "@MAGNET"])
def test_query_accepts_callsign_and_group_as_observed_js8_state(value: str) -> None:
    client = _FakeClient(SimpleNamespace(value=value, params={}))

    state = query_js8_selected_target(client, timeout_s=0.35)

    assert isinstance(state, JS8SelectedTargetState)
    assert state.available is True
    assert state.target == value
    assert state.has_target is True
    assert client.requests == [("RX.GET_CALL_SELECTED", 0.35)]


def test_query_accepts_empty_selection_without_turning_it_into_an_error() -> None:
    client = _FakeClient(SimpleNamespace(value="", params={}))

    state = query_js8_selected_target(client)

    assert state.available is True
    assert state.target == ""
    assert state.has_target is False


@pytest.mark.parametrize("error", [RuntimeError("unsupported command"), TimeoutError("timed out")])
def test_query_treats_unsupported_or_unreachable_as_quiet_unavailable_state(error: Exception) -> None:
    client = _FakeClient(error=error)

    state = query_js8_selected_target(client)

    assert state.available is False
    assert state.target == ""
    assert state.has_target is False
    assert state.detail


def test_query_does_not_mutate_js8_selection_or_issue_set_commands() -> None:
    client = _FakeClient(SimpleNamespace(value="@MAGNET", params={}))

    query_js8_selected_target(client)

    assert [command for command, _timeout in client.requests] == ["RX.GET_CALL_SELECTED"]


def _viewer_source() -> str:
    return (Path(__file__).parents[1] / "freqinout" / "gui" / "message_viewer_tab.py").read_text(encoding="utf-8")


def _method_source(source: str, name: str) -> str:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    pytest.fail(f"expected CMW-5 method {name} to exist")


def test_ui_uses_explicit_observed_js8_wording_and_use_target_action() -> None:
    source = _viewer_source()

    assert "live state observed in JS8Call" in source
    assert "It is not copied into this draft or sent until you choose Use Target" in source
    assert "Already selected in JS8Call" in source
    assert 'QPushButton("Use Target")' in source
    assert "does not send a message" in source


def test_refresh_controller_is_explicitly_scoped_and_rejects_stale_generation_endpoint() -> None:
    source = _viewer_source()
    refresh = _method_source(source, "request_compose_js8_selected_target_refresh")
    context_builder = _method_source(source, "compose_js8_selected_target_request_context")
    finished = _method_source(source, "apply_compose_js8_selected_target_result")

    assert "RX.GET_CALL_SELECTED" not in refresh  # worker/service owns socket I/O
    assert "generation" in context_builder and "endpoint" in context_builder
    assert "generation" in finished
    assert "endpoint" in finished
    assert "_compose_js8_selected_target_generation" in finished


def test_use_target_routes_only_to_active_mode_field_and_never_sends() -> None:
    source = _viewer_source()
    use_target = _method_source(source, "_use_compose_js8_selected_target")

    assert "compose_js8_target_edit" in use_target
    assert "compose_commstat_target_edit" in use_target
    assert "setText" in use_target
    assert "_send_compose_js8_spotter" not in use_target
    assert "send_js8_message_guarded" not in use_target


def test_selected_target_refresh_is_not_bound_to_keystroke_preview_or_resize_paths() -> None:
    source = _viewer_source()
    for method_name in (
        "_on_compose_rf_target_changed",
        "_update_compose_preview",
        "_refresh_compose_layout_geometry",
    ):
        body = _method_source(source, method_name)
        assert "request_compose_js8_selected_target_refresh" not in body
        assert "query_js8_selected_target" not in body


def test_refresh_has_single_inflight_and_shutdown_guards() -> None:
    source = _viewer_source()
    refresh = _method_source(source, "request_compose_js8_selected_target_refresh")
    queue_refresh = _method_source(source, "_queue_compose_js8_selected_target_refresh")
    thread_finished = _method_source(source, "_on_compose_js8_selected_target_thread_finished")
    shutdown = _method_source(source, "shutdown")
    apply_result = _method_source(source, "apply_compose_js8_selected_target_result")

    assert "compose_js8_selected_target_request_context" in refresh
    assert "return" in refresh
    assert "_is_shutting_down" in apply_result
    assert "_qt_thread_running" in queue_refresh
    assert "_compose_js8_selected_target_pending_context" in queue_refresh
    assert "QThread" in queue_refresh
    assert "_compose_js8_selected_target_pending_context" in thread_finished
    assert "_compose_js8_selected_target" in shutdown


@pytest.fixture()
def selected_target_tab(monkeypatch: pytest.MonkeyPatch):
    QApplication.instance() or QApplication([])
    tab = MessageViewerTab.__new__(MessageViewerTab)
    radio = ComposeRadioTarget(7, "FIO-B", {"js8_instance_id": "inst-b"}, ("JS8Call",))
    tab._compose_mode = "js8"
    tab._compose_radio_targets = [radio]
    tab._compose_js8_selected_target_generation = 0
    tab._compose_js8_selected_target_state = "unknown"
    tab._compose_js8_selected_target_value = ""
    tab._compose_js8_selected_target_radio_id = 0
    tab._compose_js8_selected_target_endpoint_identity = ""
    tab._compose_js8_selected_target_error = ""
    tab._is_shutting_down = False
    tab._selected_compose_radio_target = lambda: radio
    tab._compose_profile_text = lambda profile, key: str(profile.get(key, ""))
    tab._compose_radio_target_short_label = lambda target: target.label if target else ""
    tab._refresh_compose_js8_selected_target_cue = lambda: None
    return tab


def test_request_and_apply_selected_target_require_matching_generation_and_endpoint(selected_target_tab) -> None:
    # Call the context builder directly because the QObject signal requires a
    # fully initialized QWidget; the request method is covered by source
    # contract checks above.
    context = selected_target_tab.compose_js8_selected_target_request_context()
    assert context["radio_id"] == 7
    assert context["endpoint_identity"] == "radio:7|endpoint:127.0.0.1:2442"

    stale = dict(context, generation=int(context["generation"]) - 1, target="K1ABC")
    assert selected_target_tab.apply_compose_js8_selected_target_result(stale) is False
    assert selected_target_tab._compose_js8_selected_target_value == ""

    wrong_endpoint = dict(context, endpoint_identity="radio:99|js8-instance:other", target="K1ABC")
    assert selected_target_tab.apply_compose_js8_selected_target_result(wrong_endpoint) is False

    assert selected_target_tab.apply_compose_js8_selected_target_result(dict(context, target="K1ABC")) is True
    assert selected_target_tab._compose_js8_selected_target_state == "selected"
    assert selected_target_tab._compose_js8_selected_target_value == "K1ABC"


@pytest.mark.parametrize(
    "mode,field",
    [
        ("js8", "compose_js8_target_edit"),
        ("spotter", "compose_js8_target_edit"),
        ("commstat_rf", "compose_commstat_target_edit"),
    ],
)
def test_use_target_is_explicit_and_routes_to_active_mode_only(selected_target_tab, mode: str, field: str) -> None:
    selected_target_tab._compose_mode = mode
    selected_target_tab.compose_js8_target_edit = QLineEdit()
    selected_target_tab.compose_commstat_target_edit = QLineEdit()
    selected_target_tab.compose_js8_target_edit.setText("OLD-JS8")
    selected_target_tab.compose_commstat_target_edit.setText("OLD-COMMSTAT")
    selected_target_tab._compose_js8_selected_target_state = "selected"
    selected_target_tab._compose_js8_selected_target_value = "@MAGNET"
    selected_target_tab._update_compose_preview = lambda: None
    selected_target_tab._set_compose_status = lambda *args, **kwargs: None

    selected_target_tab._use_compose_js8_selected_target()

    assert getattr(selected_target_tab, field).text() == "@MAGNET"
    other = "compose_commstat_target_edit" if field == "compose_js8_target_edit" else "compose_js8_target_edit"
    assert getattr(selected_target_tab, other).text().startswith("OLD-")


def test_unconnected_query_is_quiet_and_does_not_issue_a_request() -> None:
    client = _FakeClient(connected=False)

    state = query_js8_selected_target(client)

    assert state.available is False
    assert state.target == ""
    assert client.requests == []


def test_endpoint_worker_returns_the_immutable_request_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient(SimpleNamespace(value="@GROUP", params={}))
    monkeypatch.setattr(
        message_viewer_tab.JS8ApiClientRegistry,
        "get",
        lambda *_args, **_kwargs: client,
    )
    context = {
        "generation": 9,
        "radio_id": 4,
        "endpoint_identity": "radio:4|endpoint:127.0.0.1:2442",
    }
    worker = _ComposeJs8SelectedTargetWorker(endpoint=client.endpoint, context=context)
    completed: list[dict] = []
    worker.finished.connect(completed.append)

    worker.run()

    assert completed == [
        {
            **context,
            "supported": True,
            "target": "@GROUP",
            "error": "",
        }
    ]
