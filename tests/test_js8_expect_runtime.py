from pathlib import Path

from dataclasses import dataclass

from freqinout.core.js8_expect_runtime import (
    ExpectAutomationCoordinator,
    ExpectAutomationSourceStatus,
    build_expect_rf_guard_preflight,
    load_expect_automation_runtime_state,
    set_expect_automation_runtime_state,
)
from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.js8_api_client import JS8ApiClient


class _Client:
    def __init__(self, endpoint) -> None:
        self.endpoint = endpoint
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


@dataclass
class _Snapshot:
    device_profile_id: int
    name: str
    runtime_active: bool = True
    current_frequency_hz: int | None = None
    current_band: str = ""
    antenna_group: str = ""
    frontend_group: str = ""
    amplifier_group: str = ""
    ptt_active: bool = False


class _RuntimeManager:
    def __init__(self, snapshots) -> None:
        self.snapshots = list(snapshots)

    def get_runtime_snapshots(self, *, force: bool = False):
        return list(self.snapshots)


def _settings(monkeypatch, tmp_path: Path, *, enabled: bool = True, paused: bool = False) -> SettingsManager:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = SettingsManager()
    settings.set("js8_expect_unattended_auto_reply_enabled", enabled)
    settings.set("js8_expect_unattended_auto_reply_paused", paused)
    settings.set("js8_host", "127.0.0.1")
    settings.set("js8_port", 2442)
    return settings


def test_expect_automation_runtime_is_global_off_by_default(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path, enabled=False)
    coordinator = ExpectAutomationCoordinator(
        settings,
        profiles=[{"id": 8, "name": "FIO-B", "js8_instance_id": "fio-b", "js8_host": "127.0.0.2", "js8_port": 2444}],
    )

    status = coordinator.preflight_source("8", "fio-b")

    assert status.ok is False
    assert "disabled or paused" in status.reason
    assert coordinator.client_for_source("8", "fio-b") is None


def test_expect_automation_runtime_pause_blocks_even_when_enabled(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path, enabled=True, paused=True)
    coordinator = ExpectAutomationCoordinator(settings, profiles=[{"id": 8, "js8_instance_id": "fio-b"}])

    assert coordinator.runtime_unattended_enabled() is False
    assert coordinator.preflight_source("8", "fio-b").ok is False


def test_expect_automation_runtime_state_helpers(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path, enabled=False)

    state = load_expect_automation_runtime_state(settings)
    assert state.enabled is False
    assert state.active is False
    assert "disabled" in state.reason

    state = set_expect_automation_runtime_state(settings, enabled=True, paused=False, save=False)
    assert state.enabled is True
    assert state.paused is False
    assert state.active is True

    state = set_expect_automation_runtime_state(settings, paused=True, save=False)
    assert state.enabled is True
    assert state.paused is True
    assert state.active is False
    assert "paused" in state.reason


def test_expect_automation_blocks_when_rf_guard_callback_is_missing(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path)
    coordinator = ExpectAutomationCoordinator(
        settings,
        profiles=[{"id": 8, "js8_instance_id": "fio-b", "js8_host": "127.0.0.2", "js8_port": 2444}],
    )

    status = coordinator.preflight_source("8", "fio-b")

    assert status.ok is False
    assert "RF Guard preflight is not configured" in status.reason
    assert coordinator.client_for_source("8", "fio-b") is None


def test_expect_automation_can_be_explicitly_created_without_rf_guard_for_tests(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path)
    coordinator = ExpectAutomationCoordinator(
        settings,
        profiles=[{"id": 8, "js8_instance_id": "fio-b", "js8_host": "127.0.0.2", "js8_port": 2444}],
        require_guard_preflight=False,
    )

    status = coordinator.preflight_source("8", "fio-b")

    assert status.ok is True
    assert status.endpoint is not None


def test_expect_automation_resolves_source_profile_and_caches_client(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path)
    created: list[_Client] = []

    def make_client(endpoint):
        client = _Client(endpoint)
        created.append(client)
        return client

    coordinator = ExpectAutomationCoordinator(
        settings,
        profiles=[
            {"id": 7, "name": "FIO-A", "js8_instance_id": "fio-a", "js8_host": "127.0.0.1", "js8_port": 2442},
            {"id": 8, "name": "FIO-B", "js8_instance_id": "fio-b", "js8_host": "127.0.0.2", "js8_port": 2444},
        ],
        guard_preflight=lambda radio_id, js8_id, profile: True,
        client_factory=make_client,  # type: ignore[arg-type]
    )

    first = coordinator.client_for_source("8", "fio-b")
    second = coordinator.client_for_source("8", "fio-b")

    assert first is second
    assert len(created) == 1
    assert first.endpoint.host == "127.0.0.2"
    assert first.endpoint.port == 2444
    assert coordinator.last_status is not None
    assert coordinator.last_status.ok is True
    coordinator.close()
    assert created[0].stopped is True


def test_expect_automation_guard_callback_can_block_source(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path)
    coordinator = ExpectAutomationCoordinator(
        settings,
        profiles=[{"id": 8, "js8_instance_id": "fio-b", "js8_host": "127.0.0.2", "js8_port": 2444}],
        guard_preflight=lambda radio_id, js8_id, profile: {"ok": False, "reason": "RF Guard overlap"},
        client_factory=lambda endpoint: JS8ApiClient(endpoint),
    )

    status = coordinator.preflight_source("8", "fio-b")

    assert status.ok is False
    assert status.reason == "RF Guard overlap"
    assert coordinator.client_for_source("8", "fio-b") is None


def test_expect_automation_accepts_status_from_guard_callback(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(monkeypatch, tmp_path)
    coordinator = ExpectAutomationCoordinator(
        settings,
        profiles=[{"id": 8, "js8_instance_id": "fio-b", "js8_host": "127.0.0.2", "js8_port": 2444}],
        guard_preflight=lambda radio_id, js8_id, profile: ExpectAutomationSourceStatus(
            True,
            "pre-cleared",
            radio_id=radio_id,
            js8_instance_id=js8_id,
            blocking=False,
        ),
    )

    status = coordinator.preflight_source("8", "fio-b")

    assert status.ok is True
    assert status.reason == "pre-cleared"


def test_expect_rf_guard_preflight_allows_clear_source() -> None:
    preflight = build_expect_rf_guard_preflight(
        _RuntimeManager(
            [
                _Snapshot(8, "FIO-B", current_frequency_hz=7115000, current_band="40M", antenna_group="HF-A"),
                _Snapshot(9, "FIO-C", current_frequency_hz=14115000, current_band="20M", antenna_group="HF-B"),
            ]
        )
    )

    status = preflight("8", "fio-b", {"id": 8, "antenna_group": "HF-A"})

    assert status.ok is True
    assert "no active source conflicts" in status.reason


def test_expect_rf_guard_preflight_holds_shared_antenna_same_band() -> None:
    preflight = build_expect_rf_guard_preflight(
        _RuntimeManager(
            [
                _Snapshot(8, "FIO-B", current_frequency_hz=7115000, current_band="40M", antenna_group="HF-A"),
                _Snapshot(9, "FIO-C", current_frequency_hz=7078000, current_band="40M", antenna_group="HF-A"),
            ]
        )
    )

    status = preflight("8", "fio-b", {"id": 8, "antenna_group": "HF-A"})

    assert status.ok is False
    assert "FIO-C" in status.reason
    assert "antenna HF-A" in status.reason


def test_expect_rf_guard_preflight_warn_only_band_overlap_allows_unattended() -> None:
    preflight = build_expect_rf_guard_preflight(
        _RuntimeManager(
            [
                {
                    "device_profile_id": 8,
                    "name": "FIO-B",
                    "runtime_active": True,
                    "current_frequency_hz": 7115000,
                    "current_band": "40M",
                    "band_overlap_guard_group": "MAGNET",
                },
                {
                    "device_profile_id": 9,
                    "name": "FIO-C",
                    "runtime_active": True,
                    "current_frequency_hz": 7078000,
                    "current_band": "40M",
                    "band_overlap_guard_group": "MAGNET",
                },
            ]
        )
    )

    status = preflight(
        "8",
        "fio-b",
        {"id": 8, "band_overlap_guard_group": "MAGNET", "band_overlap_guard_mode": "warn"},
    )

    assert status.ok is True
    assert "band-overlap guard MAGNET" in status.reason


def test_expect_rf_guard_preflight_confirm_overlap_holds_unattended() -> None:
    preflight = build_expect_rf_guard_preflight(
        _RuntimeManager(
            [
                {
                    "device_profile_id": 8,
                    "name": "FIO-B",
                    "runtime_active": True,
                    "current_frequency_hz": 7115000,
                    "current_band": "40M",
                    "band_overlap_guard_group": "MAGNET",
                },
                {
                    "device_profile_id": 9,
                    "name": "FIO-C",
                    "runtime_active": True,
                    "current_frequency_hz": 7078000,
                    "current_band": "40M",
                    "band_overlap_guard_group": "MAGNET",
                },
            ]
        )
    )

    status = preflight(
        "8",
        "fio-b",
        {"id": 8, "band_overlap_guard_group": "MAGNET", "band_overlap_guard_mode": "confirm"},
    )

    assert status.ok is False
    assert "band-overlap guard MAGNET" in status.reason
