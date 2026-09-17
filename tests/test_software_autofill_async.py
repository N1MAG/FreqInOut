"""SCA-S4 contracts for explicit, non-blocking software discovery."""

from __future__ import annotations

import inspect
import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
from PySide6.QtWidgets import QApplication, QLineEdit

from freqinout.core.guided_radio_software_model import RadioRole, SoftwareFamily
from freqinout.core.guided_software_discovery import DiscoveryRequest
from freqinout.core.guided_software_proposals import DiscoveryEvidence, DiscoverySnapshot
from freqinout.core.software_path_detector import PathDetectionResult
from freqinout.gui import settings_tab as settings_module
from freqinout.gui.settings_tab import SettingsTab, _SoftwareAutofillWorker


_QT_APP: QApplication | None = QApplication.instance() or QApplication([])


def _result(key: str, path: str = "/tmp/example") -> PathDetectionResult:
    return PathDetectionResult(
        key=key,
        label=key,
        path=path,
        confidence="high",
        reason="test",
        exists=True,
        target_type="file",
    )


def test_worker_uses_immutable_coordinator_request_and_never_settings_manager() -> None:
    captured = []

    class Coordinator:
        def discover(self, request, *, cancel_probe):
            captured.append((request, cancel_probe()))
            return DiscoverySnapshot(
                "settings-fast-light",
                request.generation,
                evidence=(
                    DiscoveryEvidence(
                        "path-flrig",
                        "fast_light-scan",
                        "FLRig path",
                        {
                            "record_type": "path_detection",
                            "result_key": "path_flrig",
                            "label": "path_flrig",
                            "path": "/tmp/example",
                            "confidence": "high",
                            "reason": "test",
                            "exists": "true",
                            "target_type": "file",
                        },
                    ),
                ),
            )

        def cancel(self, _session_key, _generation):
            return None

    request = DiscoveryRequest(
        "settings-test",
        "settings-fast-light",
        4,
        4,
        RadioRole.TRANSCEIVER,
        (SoftwareFamily.FAST_LIGHT,),
        {"radio_apps_base_folder": "/apps"},
    )
    worker = _SoftwareAutofillWorker(Coordinator(), request, "fast_light")
    emitted = []
    worker.finished.connect(lambda generation, section, results: emitted.append((generation, section, results)))
    worker.run()

    assert captured == [(request, False)]
    assert emitted[0][0:2] == (4, "fast_light")
    assert emitted[0][2]["path_flrig"].path == "/tmp/example"


def test_stale_or_different_editor_results_cannot_mutate_visible_task(monkeypatch) -> None:
    class Workspace:
        def selected_family_key(self): return "js8call"
        def selected_task_key(self): return "launch"
        def selected_radio_id(self): return 7

    monkeypatch.setattr(settings_module, "SoftwareAdministrationWorkspace", Workspace)

    class Editor:
        def __init__(self):
            self.field = QLineEdit()
            self.status = ""
        def field_widget(self, _key): return self.field
        def apply_value(self, _key, value): self.field.setText(str(value))
        def set_operation_status(self, value): self.status = str(value)

    class Host:
        _software_autofill_request_is_current = SettingsTab._software_autofill_request_is_current
        _on_software_autofill_finished = SettingsTab._on_software_autofill_finished

    editor = Editor()
    host = Host()
    host._software_autofill_generation = 9
    host._software_autofill_active_request = {
        "generation": 8, "section": "js8", "keys": ("path_js8call",),
        "target": "software", "family_key": "js8call", "task_key": "launch", "radio_id": 7,
    }
    host.software_administration_workspace = Workspace()
    host._software_task_editors = {("js8call", 7, "launch"): editor}
    host._on_software_autofill_finished(8, "js8", {"path_js8call": _result("path_js8call")})
    assert editor.field.text() == ""

    host._software_autofill_generation = 10
    host._software_autofill_active_request = {
        **host._software_autofill_active_request, "generation": 10, "radio_id": 8,
    }
    host._on_software_autofill_finished(10, "js8", {"path_js8call": _result("path_js8call")})
    assert editor.field.text() == ""


def test_current_result_fills_only_blank_fields(monkeypatch) -> None:
    class Workspace:
        def selected_family_key(self): return "fast_light"
        def selected_task_key(self): return "launch"
        def selected_radio_id(self): return 3

    monkeypatch.setattr(settings_module, "SoftwareAdministrationWorkspace", Workspace)

    class Editor:
        def __init__(self):
            self.fields = {"path_flrig": QLineEdit(), "path_fldigi": QLineEdit("/keep")}
            self.applied = []
            self.status = ""
        def field_widget(self, key): return self.fields.get(key)
        def apply_value(self, key, value):
            self.fields[key].setText(str(value)); self.applied.append((key, value))
        def set_operation_status(self, value): self.status = str(value)

    class Host:
        _software_autofill_request_is_current = SettingsTab._software_autofill_request_is_current
        _on_software_autofill_finished = SettingsTab._on_software_autofill_finished

    editor = Editor()
    host = Host()
    host._software_autofill_generation = 11
    host._software_autofill_active_request = {
        "generation": 11, "section": "fast_light", "keys": ("path_flrig", "path_fldigi"),
        "target": "software", "family_key": "fast_light", "task_key": "launch", "radio_id": 3,
    }
    host.software_administration_workspace = Workspace()
    host._software_task_editors = {("fast_light", 3, "launch"): editor}
    host._on_software_autofill_finished(
        11,
        "fast_light",
        {"path_flrig": _result("path_flrig", "/found"), "path_fldigi": _result("path_fldigi", "/replace")},
    )

    assert editor.applied == [("path_flrig", "/found")]
    assert editor.fields["path_fldigi"].text() == "/keep"
    assert "filled 1" in editor.status and "preserved 1" in editor.status


def test_repeated_request_coalesces_to_latest_and_cancels_active(monkeypatch) -> None:
    class Thread:
        def isRunning(self): return True

    class Worker:
        def __init__(self): self.cancel_count = 0
        def request_cancel(self): self.cancel_count += 1

    class Settings:
        def all(self): return {"radio_apps_base_folder": "/apps"}

    class Host:
        _request_software_autofill = SettingsTab._request_software_autofill
        def _selected_settings_feedback_target(self): return (3, "FIO-A")
        def _set_software_autofill_feedback(self, request, text):
            self.feedback = (dict(request), text)

    monkeypatch.setattr(settings_module, "QThread", Thread)
    host = Host()
    host.settings = Settings()
    host._software_autofill_shutdown = False
    host._software_autofill_generation = 5
    host._software_autofill_thread = Thread()
    host._software_autofill_worker = Worker()
    host._software_autofill_pending_request = None
    host.device_profiles = []
    host._software_autofill_session_key = "settings-test"

    class Coordinator:
        def register_request(self, _request): return True
    host._guided_software_discovery = Coordinator()

    host._request_software_autofill(
        "js8", ("path_js8call",), target="software",
        family_key="js8call", task_key="launch", radio_id=3,
    )

    assert host._software_autofill_worker.cancel_count == 1
    assert host._software_autofill_pending_request["generation"] == 6
    assert host._software_autofill_pending_request["keys"] == ("path_js8call",)
    assert "Waiting" in host.feedback[1]


def test_source_contract_keeps_discovery_explicit_generation_safe_and_bounded() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")
    attempt = source[source.index("    def _attempt_scoped_autofill") : source.index("    def _refresh_contextual_autofill_buttons")]
    navigation = source[source.index("    def _on_software_administration_task_selected") : source.index("    def _software_editor_state")]
    shutdown = inspect.getsource(SettingsTab.shutdown)

    assert "_detect_autofill_results(" not in attempt
    assert "_software_autofill_pending_request" in attempt
    assert "_software_autofill_generation" in attempt
    assert "_software_autofill_request_is_current" in attempt
    assert "detect_" not in navigation and "SoftwarePathDetector" not in navigation
    assert "request_cancel" in shutdown and "wait(1200)" in shutdown
    assert "_DETACHED_SOFTWARE_AUTOFILL_JOBS" in shutdown
