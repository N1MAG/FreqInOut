import threading
import time
from pathlib import Path

import pytest

from freqinout.core.guided_software_discovery import (
    DiscoveryPhase,
    DiscoveryRequest,
    GuidedSoftwareDiscoveryCoordinator,
    PhaseDiscoveryResult,
    required_phases_for,
)
import freqinout.core.guided_software_discovery_sources as sources
from freqinout.core.config_autodiscovery import JS8CallFileProfile
from freqinout.core.software_path_detector import PathDetectionResult
from freqinout.core.guided_software_proposals import DiscoveryEvidence
from freqinout.core.guided_radio_software_model import RadioRole, SoftwareFamily


def _request(*families, generation=1, revision=1, session="s", key="r", inputs=None, force=False):
    return DiscoveryRequest(session, key, generation, revision, RadioRole.OBSERVER, families, inputs or {}, force)


def _result(phase, label="ok"):
    return PhaseDiscoveryResult(phase, evidence=(DiscoveryEvidence(f"{phase.value}-{label}", "test", label),))


def test_required_phases_and_js8_family_aliases_are_deduplicated():
    request = _request("js8call", "js8call_improved", "sdrpp", "varac")
    assert required_phases_for(request) == (
        DiscoveryPhase.APPLICATIONS, DiscoveryPhase.JS8_PROFILES,
        DiscoveryPhase.RECEIVER, DiscoveryPhase.VARAC,
    )
    assert request.families.count(SoftwareFamily.JS8CALL) == 1


def test_equivalent_requests_coalesce_and_cache_reuse_with_immutable_snapshot():
    calls = {phase: 0 for phase in (DiscoveryPhase.APPLICATIONS, DiscoveryPhase.JS8_PROFILES)}
    gate = threading.Event()

    def scanner(request, cancelled):
        calls[DiscoveryPhase.APPLICATIONS] += 1
        gate.wait(2)
        return _result(DiscoveryPhase.APPLICATIONS)

    def profile(request, cancelled):
        calls[DiscoveryPhase.JS8_PROFILES] += 1
        return _result(DiscoveryPhase.JS8_PROFILES)

    coordinator = GuidedSoftwareDiscoveryCoordinator({DiscoveryPhase.APPLICATIONS: scanner, DiscoveryPhase.JS8_PROFILES: profile}, max_workers=2)
    try:
        first = _request("js8call", key="one")
        second = _request("js8call", key="one")
        outputs = []
        t1 = threading.Thread(target=lambda: outputs.append(coordinator.discover(first)))
        t2 = threading.Thread(target=lambda: outputs.append(coordinator.discover(second)))
        t1.start(); time.sleep(.05); t2.start(); time.sleep(.05); gate.set(); t1.join(); t2.join()
        assert calls[DiscoveryPhase.APPLICATIONS] == 1
        assert calls[DiscoveryPhase.JS8_PROFILES] == 1
        cached = coordinator.discover(_request("js8call", key="three"))
        assert calls[DiscoveryPhase.APPLICATIONS] == 1
        assert cached.evidence
        with pytest.raises(AttributeError):
            cached.evidence += ()
    finally:
        coordinator.shutdown(wait_for_workers=True)


def test_phase_failure_isolated_and_telemetry_bounded():
    events = []
    def broken(request, cancelled):
        raise RuntimeError("private path should not leak")
    def good(request, cancelled):
        return _result(DiscoveryPhase.FAST_LIGHT)
    coordinator = GuidedSoftwareDiscoveryCoordinator(
        {DiscoveryPhase.APPLICATIONS: broken, DiscoveryPhase.FAST_LIGHT: good},
        telemetry_sink=events.append,
    )
    try:
        snapshot = coordinator.discover(_request("fast_light", key="fail"))
        assert any("applications:" in item for item in snapshot.diagnostics)
        assert any(item.phase == DiscoveryPhase.FAST_LIGHT.value for item in events)
        assert all(len(item.outcome) <= 64 for item in events)
        assert all("private path" not in item.outcome for item in events)
    finally:
        coordinator.shutdown(wait_for_workers=True)


def test_cancel_stale_and_closed_requests_publish_cancelled_without_replacing_previous():
    started = threading.Event(); release = threading.Event()
    def slow(request, cancelled):
        started.set(); release.wait(2)
        return _result(DiscoveryPhase.APPLICATIONS, str(request.generation))
    coordinator = GuidedSoftwareDiscoveryCoordinator({
        DiscoveryPhase.APPLICATIONS: slow,
        DiscoveryPhase.JS8_PROFILES: lambda request, cancelled: _result(DiscoveryPhase.JS8_PROFILES),
    })
    try:
        good = _request("js8call", generation=1, key="good")
        release.set()
        first = coordinator.discover(good)
        assert not first.cancelled
        newer = _request("js8call", generation=2, key="new", force=True)
        started.clear(); release.clear()
        result_holder = []
        thread = threading.Thread(target=lambda: result_holder.append(coordinator.discover(newer)))
        thread.start(); started.wait(1); coordinator.cancel("s", 2); release.set(); thread.join()
        assert result_holder[0].cancelled
        assert coordinator.previous_snapshot("s") == first
        coordinator.close_session("s")
        assert coordinator.discover(_request("js8call", generation=3, key="closed")).cancelled
    finally:
        release.set(); coordinator.shutdown(wait_for_workers=True)


def test_new_generation_supersedes_running_result_and_fast_phase_publishes_independently():
    slow_started = threading.Event()
    slow_release = threading.Event()
    fast_published = threading.Event()

    def slow(request, cancelled):
        slow_started.set()
        slow_release.wait(1)
        return _result(DiscoveryPhase.APPLICATIONS)

    coordinator = GuidedSoftwareDiscoveryCoordinator({
        DiscoveryPhase.APPLICATIONS: slow,
        DiscoveryPhase.JS8_PROFILES: lambda request, cancelled: _result(DiscoveryPhase.JS8_PROFILES),
    })
    try:
        old = _request("js8call", generation=1, key="old")
        holder = []
        thread = threading.Thread(
            target=lambda: holder.append(
                coordinator.discover(
                    old,
                    phase_callback=lambda result: fast_published.set()
                    if result.phase == DiscoveryPhase.JS8_PROFILES
                    else None,
                )
            )
        )
        thread.start()
        assert slow_started.wait(1)
        assert fast_published.wait(1)
        assert coordinator.register_request(_request("js8call", generation=2, key="new"))
        slow_release.set()
        thread.join(1)
        assert holder and holder[0].cancelled
        assert coordinator.previous_snapshot("s") is None
    finally:
        slow_release.set()
        coordinator.shutdown(wait_for_workers=True)


def test_generation_stale_result_does_not_publish_and_family_change_reuses_phase_cache():
    calls = []
    def scanner(request, cancelled):
        calls.append(request.families)
        return _result(DiscoveryPhase.APPLICATIONS)
    coordinator = GuidedSoftwareDiscoveryCoordinator({DiscoveryPhase.APPLICATIONS: scanner})
    try:
        first = coordinator.discover(_request("js8call", generation=1, key="a"))
        second = coordinator.discover(_request("fast_light", generation=2, key="b"))
        assert first.evidence and second.evidence
        assert len(calls) == 1
    finally:
        coordinator.shutdown(wait_for_workers=True)


def test_scanner_injected_contract_has_no_write_endpoint_or_process_side_effects():
    touched = []
    def scanner(request, cancelled):
        touched.append("scanner")
        return _result(DiscoveryPhase.RECEIVER)
    coordinator = GuidedSoftwareDiscoveryCoordinator({DiscoveryPhase.RECEIVER: scanner})
    try:
        snapshot = coordinator.discover(_request("sdrpp", key="io"))
        assert snapshot.evidence and touched == ["scanner"]
    finally:
        coordinator.shutdown(wait_for_workers=True)


def test_bounded_request_and_result_inputs():
    with pytest.raises(Exception):
        _request("js8call", inputs={str(i): "x" for i in range(129)})
    with pytest.raises(Exception):
        PhaseDiscoveryResult(DiscoveryPhase.APPLICATIONS, diagnostics=tuple("x" for _ in range(129)))


def test_scan_js8_profiles_scans_profiles_once_and_disables_application_path_scan(monkeypatch, tmp_path: Path):
    profile = JS8CallFileProfile("FIO-A", str(tmp_path / "JS8Call.ini"), str(tmp_path / "save"), "2442", str(tmp_path / "DIRECTED.TXT"), "", "verified", "fixture")
    calls = []

    def discover_profiles(**kwargs):
        calls.append(("profiles", kwargs))
        return (profile,)

    class Detector:
        def __init__(self, _settings):
            pass

        def detect_js8(self, *, file_profiles, include_application_paths):
            calls.append(("detect", file_profiles, include_application_paths))
            return {}

    monkeypatch.setattr(sources, "discover_js8call_file_profiles", discover_profiles)
    monkeypatch.setattr(sources, "SoftwarePathDetector", Detector)
    result = sources.scan_js8_profiles(_request("js8call", inputs={"home": str(tmp_path)}), lambda: False)
    assert result.phase is DiscoveryPhase.JS8_PROFILES
    assert calls[0][0] == "profiles"
    assert callable(calls[0][1]["cancelled"])
    assert calls[1] == ("detect", (profile,), False)
    assert len([item for item in calls if item[0] == "profiles"]) == 1


def test_scan_fast_light_disables_application_path_scan(monkeypatch):
    calls = []

    class Detector:
        def detect_fast_light(self, *, include_application_paths):
            calls.append(include_application_paths)
            return {}

    monkeypatch.setattr(sources, "_detector_for", lambda _request: Detector())
    result = sources.scan_fast_light(_request("fast_light"), lambda: False)
    assert result.phase is DiscoveryPhase.FAST_LIGHT
    assert calls == [False]


def test_legacy_payload_reconstructs_candidate_path_and_js8_profile():
    evidence = (
        sources.DiscoveryEvidence("app", "application-scan", "app", {
            "record_type": "app_candidate", "app_id": "js8call", "display_name": "JS8Call",
            "path": "/opt/js8call", "source": "known_path", "confidence": "verified",
            "exists": "true", "executable": "true", "target_type": "file", "notes": "one | two",
        }),
        sources.DiscoveryEvidence("path", "fast_light-scan", "path", {
            "record_type": "path_detection", "result_key": "path_fldigi", "label": "FLDigi",
            "path": "/opt/fldigi", "confidence": "high", "reason": "configured",
            "exists": "true", "target_type": "file",
        }),
        sources.DiscoveryEvidence("profile", "js8-profile-scan", "profile", {
            "record_type": "js8_profile", "name": "FIO-A", "ini_path": "/tmp/JS8Call.ini",
            "save_dir": "/tmp/FIO-A", "tcp_server_port": "2442", "directed_path": "/tmp/FIO-A/DIRECTED.TXT",
            "all_path": "/tmp/FIO-A/ALL.TXT", "inbox_path": "/tmp/FIO-A/inbox.db3",
            "application_data_root": "/tmp/FIO-A", "rig_name": "FIO-A", "confidence": "verified",
            "reason": "fixture", "storage_mode": "verified",
        }),
    )
    snapshot = sources.DiscoverySnapshot("snap", 1, evidence=evidence)
    payload = sources.legacy_payload_from_snapshot(snapshot)
    candidate = payload["install_candidates"][0]
    path = payload["fast_results"]["path_fldigi"]
    profile = payload["js8_file_profiles"][0]
    assert (candidate.app_id, candidate.path, candidate.notes) == ("js8call", "/opt/js8call", ("one", "two"))
    assert isinstance(path, PathDetectionResult) and (path.key, path.path, path.exists) == ("path_fldigi", "/opt/fldigi", True)
    assert isinstance(profile, JS8CallFileProfile) and (profile.tcp_server_port, profile.directed_path, profile.inbox_path) == ("2442", "/tmp/FIO-A/DIRECTED.TXT", "/tmp/FIO-A/inbox.db3")


def test_scan_receiver_only_reflects_inputs_and_never_calls_endpoint(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("receiver discovery must not contact an endpoint")

    monkeypatch.setattr("socket.create_connection", forbidden)
    request = _request("sdrpp", inputs={
        "sdr_receiver_application": "SDR++", "sdr_control_adapter": "rigctl",
        "sdr_host": "127.0.0.1", "sdr_port": "4532", "sdr_receiver_target": "VFO-A",
        "sdr_launch_target": "/opt/sdrpp",
    })
    result = sources.scan_receiver(request, lambda: False)
    assert result.evidence[0].attributes["application"] == "SDR++"
    assert result.evidence[0].attributes["port"] == "4532"
    assert result.evidence[0].attributes["target"] == "VFO-A"
