"""SDR-2 coordinator integration gates.

These tests use the shared MES endpoint registry and an in-memory receive-only
adapter.  TCP framing is covered by ``test_sdrpp_rigctl_adapter.py``; this file
keeps the coordinator/lifecycle assertions deterministic and hardware-free.
"""

from __future__ import annotations

from pathlib import Path
import threading
import time
from typing import Mapping, Optional

from freqinout.core.receiver_control import (
    ReceiverCapabilities,
    ReceiverCommand,
    ReceiverControlClient,
    ReceiverIdentity,
    ReceiverState,
    receiver_identity_from_profile,
)
from freqinout.core.receiver_qualification_service import ReceiverQualificationCoordinator
from freqinout.core.scheduler_coordination import EndpointKey, EndpointResult
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry


def _profile(profile_id: int = 71) -> dict[str, object]:
    return {
        "id": profile_id,
        "name": f"RTL-SDR {profile_id}",
        "device_class": "observer",
        "sdr_application": "SDR++",
        "sdr_adapter": "sdrpp_rigctl",
        "sdr_host": "127.0.0.1",
        "sdr_port": 4532,
        "sdr_target": "vfo-a",
    }


class _QualificationReceiver:
    def __init__(self, profile: Mapping[str, object], *, block: Optional[threading.Event] = None) -> None:
        self.identity = receiver_identity_from_profile(profile)
        self.frequency_hz = 7_100_000
        self.block = block
        self.started = threading.Event()
        self.closed = False
        self.close_calls = 0
        self.calls: list[str] = []

    def probe(self, *, deadline: float, cancel):
        del deadline
        self.calls.append("probe")
        self.started.set()
        if self.block is not None:
            self.block.wait(2.0)
        if cancel():
            return self.identity, ReceiverCapabilities(detail="cancelled")
        return self.identity, ReceiverCapabilities(
            can_read_state=True,
            can_set_receive_frequency=True,
            can_verify_state=True,
            readback_tolerance_hz=20,
            manual_only=False,
            detail="fake RTL-SDR receiver",
        )

    def list_targets(self, *, deadline: float, cancel):
        del deadline, cancel
        self.calls.append("list_targets")
        return (self.identity,)

    def read_state(self, target: ReceiverIdentity, *, deadline: float, cancel) -> ReceiverState:
        del deadline
        assert target == self.identity
        self.calls.append("read_state")
        return self._state(cancelled=bool(cancel()))

    def set_receive_frequency(self, target: ReceiverIdentity, frequency_hz: int, *, deadline: float, cancel) -> ReceiverState:
        del deadline
        assert target == self.identity
        self.calls.append("set_receive_frequency")
        if cancel():
            return self._state(cancelled=True)
        self.frequency_hz = int(frequency_hz)
        return self._state()

    def set_receive_mode(self, target: ReceiverIdentity, mode: str, bandwidth_hz=None, *, deadline: float, cancel) -> ReceiverState:
        del mode, bandwidth_hz, deadline
        assert target == self.identity
        self.calls.append("set_receive_mode")
        return self._state(cancelled=bool(cancel()))

    def verify_state(self, target: ReceiverIdentity, expected: ReceiverCommand, *, tolerance_hz: int, deadline: float, cancel) -> ReceiverState:
        del tolerance_hz, deadline
        assert target == self.identity
        self.calls.append("verify_state")
        return self._state(cancelled=bool(cancel()), verified=self.frequency_hz == expected.frequency_hz)

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        if self.block is not None:
            self.block.set()

    def _state(self, *, cancelled: bool = False, verified: bool = False) -> ReceiverState:
        return ReceiverState(
            identity=self.identity,
            available=not self.closed,
            running=not self.closed,
            frequency_hz=self.frequency_hz,
            verified=verified,
            manual=False,
            cancelled=cancelled,
        )


def _endpoint(profile: Mapping[str, object]) -> EndpointKey:
    return EndpointKey.network(
        "sdrpp_rigctl",
        profile["sdr_host"],
        profile["sdr_port"],
        target=str(profile["sdr_target"]),
    )


def _wait(event: threading.Event, message: str) -> None:
    assert event.wait(2.0), message


def test_coordinator_uses_shared_lane_and_publishes_reversible_evidence() -> None:
    profile = _profile()
    registry = EndpointLaneRegistry()
    receiver: Optional[_QualificationReceiver] = None
    completed = threading.Event()
    results: list[EndpointResult] = []

    def factory(snapshot: Mapping[str, object]) -> ReceiverControlClient:
        nonlocal receiver
        receiver = _QualificationReceiver(snapshot)
        return receiver

    coordinator = ReceiverQualificationCoordinator(registry, factory, timeout_s=2.0)
    try:
        submission = coordinator.request(profile, lambda result: (results.append(result), completed.set()))
        assert submission.accepted is True
        assert submission.endpoint_key == _endpoint(profile)
        assert registry.lane(_endpoint(profile)) is not None
        _wait(completed, "qualification did not complete")
        assert receiver is not None
        result = results[0]
        actual = result.actual_state()
        evidence = actual["verification"]
        assert result.status in {"applied_and_verified", "applied_unverified"}
        assert actual["verification_state"] == "verified"
        assert evidence["tune_readback_verified"] is True
        assert evidence["restore_readback_verified"] is True
        assert evidence["original_frequency_hz"] == 7_100_000
        assert evidence["test_frequency_hz"] != evidence["original_frequency_hz"]
        assert evidence["target"] == "vfo-a"
        assert receiver.frequency_hz == 7_100_000
        assert receiver.closed is True
        assert "set_receive_frequency" in receiver.calls
        assert receiver.calls[-1] == "verify_state"
    finally:
        coordinator.stop()
        registry.shutdown(wait=True)


def test_unavailable_factory_result_falls_back_without_claiming_verified_control() -> None:
    profile = _profile(72)
    registry = EndpointLaneRegistry()
    completed = threading.Event()
    results: list[EndpointResult] = []
    coordinator = ReceiverQualificationCoordinator(registry, lambda _snapshot: None, timeout_s=1.0)
    try:
        submission = coordinator.request(profile, lambda result: (results.append(result), completed.set()))
        assert submission.accepted
        _wait(completed, "unavailable qualification did not complete")
        result = results[0]
        assert result.status == "failed"
        assert result.reason_code == "receiver_adapter_unavailable"
        assert result.actual_state()["profile_id"] == 72
        assert "manual tuning" in result.detail.lower()
    finally:
        coordinator.stop()
        registry.shutdown(wait=True)


def test_same_target_requests_share_one_lane_and_newer_request_supersedes_old() -> None:
    profile = _profile(73)
    registry = EndpointLaneRegistry()
    first_gate = threading.Event()
    receivers: list[_QualificationReceiver] = []
    completed = threading.Event()
    results: list[EndpointResult] = []

    def factory(snapshot: Mapping[str, object]) -> ReceiverControlClient:
        receiver = _QualificationReceiver(snapshot, block=first_gate if not receivers else None)
        receivers.append(receiver)
        return receiver

    coordinator = ReceiverQualificationCoordinator(registry, factory, timeout_s=2.0)
    try:
        first = coordinator.request(profile, results.append)
        assert first.accepted
        _wait(receivers[0].started, "first qualification did not enter shared lane")
        second = coordinator.request(profile, lambda result: (results.append(result), completed.set()))
        assert second.accepted
        assert second.disposition == "coalesced"
        assert len(registry) == 1
        first_gate.set()
        _wait(completed, "newer qualification did not complete")
        assert any(result.status == "superseded" for result in results)
        assert len(receivers) == 2
        assert receivers[0].close_calls >= 1
        assert receivers[1].closed is True
    finally:
        first_gate.set()
        coordinator.stop()
        registry.shutdown(wait=True)


def test_stop_closes_active_client_and_suppresses_further_endpoint_work() -> None:
    profile = _profile(74)
    registry = EndpointLaneRegistry()
    gate = threading.Event()
    receiver: Optional[_QualificationReceiver] = None
    completed = threading.Event()

    def factory(snapshot: Mapping[str, object]) -> ReceiverControlClient:
        nonlocal receiver
        receiver = _QualificationReceiver(snapshot, block=gate)
        return receiver

    coordinator = ReceiverQualificationCoordinator(registry, factory, timeout_s=2.0)
    try:
        submission = coordinator.request(profile, lambda _result: completed.set())
        assert submission.accepted
        assert receiver is not None
        _wait(receiver.started, "active receiver was not created")
        coordinator.stop()
        assert receiver.closed is True
        gate.set()
        _wait(completed, "stopped qualification did not publish bounded completion")
        assert coordinator.is_stopped() is True
        assert coordinator.request(profile, lambda _result: None).accepted is False
    finally:
        gate.set()
        coordinator.stop()
        registry.shutdown(wait=True)


def test_main_window_wires_factory_and_scheduler_owned_shared_registry() -> None:
    source = Path(__file__).parents[1] / "freqinout" / "gui" / "main_window.py"
    text = source.read_text(encoding="utf-8")
    assert "receiver_client_factory=receiver_control_client_from_profile" in text
    assert "ReceiverQualificationCoordinator(" in text
    assert "self.scheduler.shared_endpoint_lane_registry()" in text
    assert "receiver_control_client_from_profile," in text
