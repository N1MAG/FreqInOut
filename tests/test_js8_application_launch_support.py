from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

from freqinout.core import launch_orchestrator as launch_module
from freqinout.core.config_autodiscovery import default_app_search_paths, find_app_candidates
from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core.software_path_detector import SoftwarePathDetector
from freqinout.core.software_status_service import PROGRAM_TOKENS, SoftwareStatusService
from freqinout.gui.settings_tab import SettingsTab


def _make_executable(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | 0o111)


def test_linux_js8_search_paths_cover_packaged_and_case_variants() -> None:
    paths = default_app_search_paths(platform="Linux", home=Path("/home/operator"))["js8call"]

    assert Path("/usr/bin/js8call-subspace") in paths
    assert Path("/usr/local/bin/js8call-subspace") in paths
    assert Path("/usr/bin/JS8Call") in paths
    assert Path("/home/operator/.local/bin/JS8Call") in paths
    assert Path("/usr/local/bin/JS8Call") in paths
    assert Path("/opt/JS8Call-improved/bin/JS8Call") in paths


def test_linux_js8_candidate_accepts_extensionless_subspace_executable(tmp_path: Path) -> None:
    executable = tmp_path / "js8call-subspace"
    _make_executable(executable)

    candidates = find_app_candidates(
        apps=("js8call",),
        platform="Linux",
        app_search_paths={"js8call": (executable,)},
    )

    assert len(candidates) == 1
    assert candidates[0].path == str(executable)
    assert candidates[0].executable is True


def test_settings_detector_returns_parent_for_extensionless_js8_executable(tmp_path: Path) -> None:
    executable = tmp_path / "JS8Call"
    _make_executable(executable)
    detector = SoftwarePathDetector(settings={})
    detector.system = "Linux"

    result = detector._detect_install_target(
        key="path_js8call",
        label="JS8Call application",
        tokens=("JS8Call",),
        bundle_names=("JS8Call",),
        windows_files=(),
        linux_files=(executable,),
        prefer_bundle_dir=True,
    )

    assert Path(result.path) == executable.parent
    assert result.target_type == "directory"


def test_js8_launch_resolver_accepts_direct_file_and_directory_variants(tmp_path: Path) -> None:
    executable = tmp_path / "JS8Call"
    _make_executable(executable)
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)

    assert orchestrator._command_from_config_path("JS8Call", str(executable)) == [str(executable)]
    assert orchestrator._command_from_config_path("JS8Call", str(tmp_path)) == [str(executable)]


def test_js8_fallback_prefers_installed_variant_and_preserves_legacy_fallback(monkeypatch) -> None:
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)

    def which(command: str) -> str | None:
        return "/usr/bin/JS8Call-improved" if command == "JS8Call-improved" else None

    monkeypatch.setattr(launch_module.shutil, "which", which)
    assert orchestrator._fallback_cmd("JS8Call") == ["/usr/bin/JS8Call-improved"]

    monkeypatch.setattr(launch_module.shutil, "which", lambda _command: None)
    assert orchestrator._fallback_cmd("JS8Call") == ["js8call"]


def test_js8_process_tokens_cover_packaged_variants() -> None:
    tokens = {str(token).lower() for token in PROGRAM_TOKENS["JS8Call"]}

    assert {"js8call", "js8call.exe", "js8call-improved", "js8call-subspace", "subspace"} <= tokens


def test_js8_path_ui_uses_application_or_executable_wording() -> None:
    method_source = inspect.getsource(SettingsTab._choose_js8call_install_path)
    assert "getOpenFileName" in method_source
    assert "JS8Call executable" in method_source

    # Keep this a source-level contract: constructing SettingsTab starts timers,
    # services, and database-backed UI state that is unrelated to path choosing.
    module_source = inspect.getsource(SettingsTab)
    assert 'setPlaceholderText("JS8Call application or executable path")' in module_source
    assert '"JS8Call Application:"' in module_source
    assert 'mode="app"' in module_source


def test_js8_storage_ui_exposes_state_and_separates_save_folder(tmp_path: Path) -> None:
    isolated = {
        "id": 1,
        "name": "Radio Alpha",
        "js8_variant_family": "js8call_improved",
        "js8_variant_version": "3.0.3",
        "js8_rig_name": "field-alpha",
        "js8_message_storage_root": str(tmp_path / "messages"),
        "js8_storage_evidence": "operator_confirmed:fixture",
        "js8_profile_path": str(tmp_path / "save"),
    }
    assert SettingsTab._js8_storage_display_state(isolated) == "Isolated · field-alpha"
    detail = SettingsTab._js8_storage_detail_text(isolated)
    assert "--rig-name" in detail
    assert "Save folder is separate" in detail
    assert "does not relocate ALL.TXT, DIRECTED.TXT, or inbox.db3" in detail

    subspace = {
        "js8_variant_family": "js8call_subspace",
        "js8_variant_version": "4.1.0.478",
        "js8_rig_name": "field-subspace",
        "js8_message_storage_root": str(tmp_path / "JS8Call - field-subspace"),
        "js8_storage_evidence": "operator_confirmed:fixture",
    }
    assert SettingsTab._js8_storage_display_state(subspace) == "Isolated · field-subspace"
    assert "Concurrent local launch requires a stable unique --rig-name" in SettingsTab._js8_storage_detail_text(subspace)

    unknown = {"js8_variant_family": "unknown"}
    assert SettingsTab._js8_storage_display_state(unknown) == "Needs verification"


def test_js8_storage_collision_guidance_names_both_radios() -> None:
    tab = SettingsTab.__new__(SettingsTab)
    tab.device_profiles = [
        {"id": 1, "name": "Radio Alpha", "runtime_active": 1, "use_js8call": 1, "js8_message_storage_root": "/shared/js8"},
        {"id": 2, "name": "Radio Bravo", "runtime_active": 1, "use_js8call": 1, "js8_message_storage_root": "/shared/js8"},
    ]

    warnings = tab._js8_storage_collisions()

    assert len(warnings) == 1
    assert warnings[0].affected_radio_names == ("Radio Alpha", "Radio Bravo")
    assert "Radio Alpha" in warnings[0].message
    assert "Radio Bravo" in warnings[0].message
    assert "Review both radio configurations" in warnings[0].message


def test_js8_rig_collision_guidance_names_both_local_radios() -> None:
    tab = SettingsTab.__new__(SettingsTab)
    tab.device_profiles = [
        {"id": 1, "name": "Radio Alpha", "runtime_active": 1, "use_js8call": 1, "js8_rig_name": "Field"},
        {"id": 2, "name": "Radio Bravo", "runtime_active": 1, "use_js8call": 1, "js8_rig_name": "field"},
    ]

    warnings = tab._js8_storage_collisions()

    assert len(warnings) == 1
    assert warnings[0].warning_type == "duplicate_js8_rig_name"
    assert warnings[0].affected_radio_names == ("Radio Alpha", "Radio Bravo")
    assert "Review both radio configurations" in warnings[0].message


def test_js8_status_tokens_include_case_variant_for_configured_processes() -> None:
    service = SoftwareStatusService.__new__(SoftwareStatusService)
    service.settings = SimpleNamespace(get=lambda key, default="": "/usr/bin/JS8Call" if key == "path_js8call" else default)

    assert "js8call" in service._target_tokens("JS8Call")
    assert "js8call" in service._configured_tokens("JS8Call")


def test_api_ready_version_persists_rig_scoped_subspace_storage_identity() -> None:
    class Store:
        def __init__(self) -> None:
            self.saved: list[dict[str, object]] = []

        def get_device_profile(self, radio_id: int) -> dict[str, object]:
            assert radio_id == 1
            return {"id": 1, "name": "Radio Alpha", "js8_instance_id": 7}

        def get_js8_instance(self, instance_id: int) -> dict[str, object]:
            assert instance_id == 7
            return {"id": 7, "system_key": "alpha-js8", "name": "Alpha JS8"}

        def save_js8_instance(self, values: dict[str, object]) -> dict[str, object]:
            self.saved.append(dict(values))
            return dict(values)

    store = Store()
    orchestrator = LaunchOrchestrator.__new__(LaunchOrchestrator)
    orchestrator.multi_radio_store = store
    item = {
        "name": "JS8Call",
        "radio_ids": [1],
        "rig_name": "field-alpha",
        "rig_name_source": "managed",
    }

    orchestrator._persist_ready_js8_identity(item, {"version": "Subspace 4.1.0.478"})

    assert item["expected_storage_mode"] == "rig_scoped"
    assert str(item["application_data_root"]).endswith("JS8Call - field-alpha")
    assert len(store.saved) == 1
    assert store.saved[0]["variant_family"] == "js8call_subspace_4_1"
    # API version observation identifies the rig-scoped namespace, but a
    # message-file check remains necessary before the root is verified.
    assert store.saved[0]["storage_mode"] == "unverified"
    assert store.saved[0]["storage_evidence"] == "api_observed:Subspace 4.1.0.478"
