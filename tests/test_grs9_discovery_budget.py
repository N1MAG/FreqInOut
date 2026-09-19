import threading
import time

from freqinout.core.guided_radio_software_model import RadioRole
from freqinout.core.guided_software_discovery import (
    DiscoveryPhase,
    DiscoveryRequest,
    GuidedSoftwareDiscoveryCoordinator,
    PhaseDiscoveryResult,
)
from freqinout.core.guided_software_proposals import DiscoveryEvidence


def _request() -> DiscoveryRequest:
    return DiscoveryRequest(
        "grs9-session",
        "grs9-request",
        1,
        1,
        RadioRole.TRANSCEIVER,
        ("js8call",),
        {},
        True,
    )


def test_slow_profile_scan_times_out_without_holding_the_prepared_result() -> None:
    release = threading.Event()
    telemetry = []

    def fast(_request, _cancelled):
        return PhaseDiscoveryResult(
            DiscoveryPhase.APPLICATIONS,
            evidence=(DiscoveryEvidence("app", "test", "usable application evidence"),),
        )

    def slow(_request, _cancelled):
        release.wait(1.0)
        return PhaseDiscoveryResult(
            DiscoveryPhase.JS8_PROFILES,
            evidence=(DiscoveryEvidence("late-profile", "test", "late profile evidence"),),
        )

    coordinator = GuidedSoftwareDiscoveryCoordinator(
        {
            DiscoveryPhase.APPLICATIONS: fast,
            DiscoveryPhase.JS8_PROFILES: slow,
        },
        phase_timeout_seconds=0.05,
        telemetry_sink=telemetry.append,
    )
    started = time.monotonic()
    try:
        snapshot = coordinator.discover(_request())
        elapsed = time.monotonic() - started
        assert elapsed < 0.3
        assert any(item.evidence_key == "app" for item in snapshot.evidence)
        assert all(item.evidence_key != "late-profile" for item in snapshot.evidence)
        assert any("continuing with safe partial evidence" in item for item in snapshot.diagnostics)
        assert any(
            item.phase == DiscoveryPhase.JS8_PROFILES.value and item.outcome == "timeout"
            for item in telemetry
        )
    finally:
        release.set()
        coordinator.shutdown(wait_for_workers=True)


def test_new_generation_coalesces_identical_timed_out_profile_scan() -> None:
    release = threading.Event()
    calls = 0

    def fast(_request, _cancelled):
        return PhaseDiscoveryResult(DiscoveryPhase.APPLICATIONS)

    def slow(_request, _cancelled):
        nonlocal calls
        calls += 1
        release.wait(1.0)
        return PhaseDiscoveryResult(DiscoveryPhase.JS8_PROFILES)

    coordinator = GuidedSoftwareDiscoveryCoordinator(
        {
            DiscoveryPhase.APPLICATIONS: fast,
            DiscoveryPhase.JS8_PROFILES: slow,
        },
        phase_timeout_seconds=0.05,
    )
    try:
        first = coordinator.discover(_request())
        second_request = DiscoveryRequest(
            "grs9-session",
            "grs9-request-2",
            2,
            2,
            RadioRole.TRANSCEIVER,
            ("js8call",),
            {},
            True,
        )
        second = coordinator.discover(second_request)

        assert calls == 1
        assert any("continuing with safe partial evidence" in item for item in first.diagnostics)
        assert any("continuing with safe partial evidence" in item for item in second.diagnostics)
    finally:
        release.set()
        coordinator.shutdown(wait_for_workers=True)


def test_completed_phase_inside_budget_is_cached_normally() -> None:
    calls = 0

    def scanner(_request, _cancelled):
        nonlocal calls
        calls += 1
        return PhaseDiscoveryResult(
            DiscoveryPhase.APPLICATIONS,
            evidence=(DiscoveryEvidence("app", "test", "application evidence"),),
        )

    coordinator = GuidedSoftwareDiscoveryCoordinator(
        {
            DiscoveryPhase.APPLICATIONS: scanner,
            DiscoveryPhase.JS8_PROFILES: lambda _request, _cancelled: PhaseDiscoveryResult(
                DiscoveryPhase.JS8_PROFILES
            ),
        },
        phase_timeout_seconds=0.1,
    )
    try:
        first = coordinator.discover(_request())
        cached_request = DiscoveryRequest(
            "grs9-session",
            "grs9-request-2",
            2,
            1,
            RadioRole.TRANSCEIVER,
            ("js8call",),
            {},
            False,
        )
        second = coordinator.discover(cached_request)
        assert first.evidence == second.evidence
        assert calls == 1
    finally:
        coordinator.shutdown(wait_for_workers=True)
