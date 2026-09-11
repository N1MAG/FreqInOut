"""SDR-1 receive-only control qualification tests.

The adapter below is deliberately a deterministic fake: it models the
application API seam without opening sockets or enumerating RTL-SDR hardware.
The tests exercise the production receiver contract and the existing MES
endpoint lanes, including deadline/cancellation propagation and lifecycle
fencing.
"""

from __future__ import annotations

import inspect
import threading
from typing import Callable, List, Tuple

import pytest

from freqinout.core.receiver_control import (
    ReceiverCapabilities,
    ReceiverCommand,
    ReceiverControlClient,
    ReceiverIdentity,
    ManualReceiverControl,
    ReceiverState,
)
from freqinout.core.receiver_qualification import (
    MAX_RECEIVER_TARGETS,
    ReceiverQualificationError,
    probe_receiver_control,
    run_reversible_receiver_tune_test,
)
from freqinout.core.scheduler_coordination import EndpointKey, EndpointResult
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry


class _FakeReceiverAdapter:
    """Bounded, receive-only application adapter used by SDR-1 tests."""

    def __init__(self, *, target_count: int = 2, max_targets: int = 8) -> None:
        self._max_targets = max(0, int(max_targets))
        self._closed = False
        self.frequency_by_target: dict[str, int] = {}
        self.calls: List[Tuple[str, float]] = []
        self.close_calls = 0
        self.targets = tuple(
            ReceiverIdentity(
                adapter_id="sdrpp-rigctl",
                receiver_id=f"sdrpp:{index}",
                display_name=f"RTL-SDR VFO {index}",
                application_name="SDR++",
                hardware_family="RTL-SDR",
                target_id=f"vfo-{index}",
                endpoint_label="127.0.0.1:4532",
            )
            for index in range(max(0, int(target_count)))
        )

    def _guard(self, operation: str, deadline: float, cancel: Callable[[], bool]) -> bool:
        self.calls.append((operation, float(deadline)))
        try:
            return self._closed or bool(cancel())
        except Exception:
            return True

    def probe(self, *, deadline: float, cancel: Callable[[], bool]):
        cancelled = self._guard("probe", deadline, cancel)
        identity = self.targets[0]
        if cancelled:
            return identity, ReceiverCapabilities(detail="Receiver probe cancelled.")
        return identity, ReceiverCapabilities(
            can_list_targets=True,
            can_read_state=True,
            can_set_receive_frequency=True,
            can_verify_state=True,
            readback_tolerance_hz=20,
            manual_only=False,
            detail="SDR++ RigCTL receive-only fake",
        )

    def list_targets(self, *, deadline: float, cancel: Callable[[], bool]):
        if self._guard("list_targets", deadline, cancel):
            return ()
        return self.targets[: self._max_targets]

    def _state(self, target: ReceiverIdentity, *, verified: bool = False, cancelled: bool = False) -> ReceiverState:
        return ReceiverState(
            identity=target,
            available=not self._closed,
            running=not self._closed,
            frequency_hz=self.frequency_by_target.get(target.target_id),
            verified=verified,
            manual=False,
            cancelled=cancelled,
            detail="Receiver adapter is closed." if self._closed else "",
        )

    def _require_target(self, target: ReceiverIdentity) -> None:
        if target not in self.targets:
            raise ValueError("receiver target does not belong to this adapter")

    def read_state(self, target: ReceiverIdentity, *, deadline: float, cancel: Callable[[], bool]) -> ReceiverState:
        self._require_target(target)
        cancelled = self._guard("read_state", deadline, cancel)
        return self._state(target, cancelled=cancelled)

    def set_receive_frequency(
        self,
        target: ReceiverIdentity,
        frequency_hz: int,
        *,
        deadline: float,
        cancel: Callable[[], bool],
    ) -> ReceiverState:
        self._require_target(target)
        cancelled = self._guard("set_receive_frequency", deadline, cancel)
        if cancelled:
            return self._state(target, cancelled=True)
        self.frequency_by_target[target.target_id] = int(frequency_hz)
        return self._state(target)

    def set_receive_mode(
        self,
        target: ReceiverIdentity,
        mode: str,
        bandwidth_hz: int | None = None,
        *,
        deadline: float,
        cancel: Callable[[], bool],
    ) -> ReceiverState:
        del mode, bandwidth_hz
        self._require_target(target)
        cancelled = self._guard("set_receive_mode", deadline, cancel)
        return self._state(target, cancelled=cancelled)

    def verify_state(
        self,
        target: ReceiverIdentity,
        expected: ReceiverCommand,
        *,
        tolerance_hz: int,
        deadline: float,
        cancel: Callable[[], bool],
    ) -> ReceiverState:
        del tolerance_hz
        self._require_target(target)
        cancelled = self._guard("verify_state", deadline, cancel)
        actual = self.frequency_by_target.get(target.target_id)
        return self._state(
            target,
            verified=not cancelled and actual is not None and actual == expected.frequency_hz,
            cancelled=cancelled,
        )

    def close(self) -> None:
        self.close_calls += 1
        self._closed = True


def _adapter() -> _FakeReceiverAdapter:
    return _FakeReceiverAdapter(target_count=10, max_targets=3)


def test_fake_adapter_is_receive_only_bounded_and_runtime_contract() -> None:
    adapter = _adapter()
    assert isinstance(adapter, ReceiverControlClient)
    identity, capabilities = adapter.probe(deadline=25.0, cancel=lambda: False)
    assert capabilities.can_set_receive_frequency is True
    assert capabilities.manual_only is False
    assert len(adapter.list_targets(deadline=25.0, cancel=lambda: False)) == 3
    public = {
        name
        for name, value in inspect.getmembers(_FakeReceiverAdapter, predicate=callable)
        if not name.startswith("_")
    }
    assert not {"ptt", "set_ptt", "transmit", "send", "get_ptt"} & public
    assert identity.target_id == "vfo-0"


def test_fake_adapter_honors_cancellation_before_every_operation() -> None:
    adapter = _adapter()
    identity = adapter.targets[0]
    cancelled = lambda: True
    probed_identity, capabilities = adapter.probe(deadline=10.0, cancel=cancelled)
    assert probed_identity == identity
    assert "cancelled" in capabilities.detail.lower()
    assert adapter.list_targets(deadline=10.0, cancel=cancelled) == ()
    assert adapter.read_state(identity, deadline=10.0, cancel=cancelled).cancelled
    assert adapter.set_receive_frequency(identity, 7_115_000, deadline=10.0, cancel=cancelled).cancelled
    assert adapter.verify_state(
        identity,
        ReceiverCommand(target_id=identity.target_id, frequency_hz=7_115_000),
        tolerance_hz=20,
        deadline=10.0,
        cancel=cancelled,
    ).cancelled
    assert adapter.frequency_by_target == {}
    assert [name for name, _deadline in adapter.calls] == [
        "probe",
        "list_targets",
        "read_state",
        "set_receive_frequency",
        "verify_state",
    ]


def test_fake_adapter_tune_and_readback_share_deadline_and_target_state() -> None:
    adapter = _adapter()
    identity = adapter.targets[1]
    deadline = 42.5
    applied = adapter.set_receive_frequency(identity, 7_115_000, deadline=deadline, cancel=lambda: False)
    verified = adapter.verify_state(
        identity,
        ReceiverCommand(target_id=identity.target_id, frequency_hz=7_115_000),
        tolerance_hz=20,
        deadline=deadline,
        cancel=lambda: False,
    )
    assert applied.frequency_hz == 7_115_000
    assert verified.verified is True
    assert [item for item in adapter.calls if item[0] in {"set_receive_frequency", "verify_state"}] == [
        ("set_receive_frequency", deadline),
        ("verify_state", deadline),
    ]
    assert adapter.frequency_by_target == {"vfo-1": 7_115_000}


def test_probe_is_bounded_selects_configured_target_and_reads_state() -> None:
    adapter = _adapter()
    configured = adapter.targets[1]
    adapter.frequency_by_target[configured.target_id] = 7_100_000
    snapshot = probe_receiver_control(
        adapter,
        configured,
        deadline=50.0,
        cancel=lambda: False,
        max_targets=3,
        monotonic=lambda: 40.0,
    )
    assert snapshot.identity == configured
    assert len(snapshot.targets) == 3
    assert snapshot.state is not None
    assert snapshot.state.frequency_hz == 7_100_000
    assert snapshot.operator_state == "Connected; verify tuning"
    assert [name for name, deadline in adapter.calls] == [
        "probe",
        "list_targets",
        "read_state",
    ]
    assert all(deadline == 50.0 for _name, deadline in adapter.calls)


def test_probe_rejects_unbounded_target_response_and_never_touches_hardware_after_cancel() -> None:
    adapter = _adapter()
    with pytest.raises(ReceiverQualificationError, match="more than 2 targets"):
        probe_receiver_control(
            adapter,
            adapter.targets[0],
            deadline=50.0,
            cancel=lambda: False,
            max_targets=2,
            monotonic=lambda: 40.0,
        )
    assert [name for name, _deadline in adapter.calls] == ["probe", "list_targets"]

    cancelled = _adapter()
    with pytest.raises(ReceiverQualificationError, match="cancelled"):
        probe_receiver_control(
            cancelled,
            cancelled.targets[0],
            deadline=50.0,
            cancel=lambda: True,
            monotonic=lambda: 40.0,
        )
    assert cancelled.calls == []

    expired = _adapter()
    with pytest.raises(ReceiverQualificationError, match="timed out"):
        probe_receiver_control(
            expired,
            expired.targets[0],
            deadline=40.0,
            cancel=lambda: False,
            monotonic=lambda: 40.0,
        )
    assert expired.calls == []
    assert MAX_RECEIVER_TARGETS == 32


def test_reversible_tune_requires_readback_and_restores_original_frequency() -> None:
    adapter = _adapter()
    configured = adapter.targets[1]
    adapter.frequency_by_target[configured.target_id] = 7_100_000
    result = run_reversible_receiver_tune_test(
        adapter,
        configured,
        7_115_000,
        deadline=90.0,
        cancel=lambda: False,
        monotonic=lambda: 80.0,
    )
    assert result.success is True
    assert result.operator_state == "FIO tuning ready"
    assert result.before is not None and result.before.frequency_hz == 7_100_000
    assert result.verified is not None and result.verified.verified is True
    assert result.restore_verified is not None and result.restore_verified.verified is True
    assert adapter.frequency_by_target[configured.target_id] == 7_100_000
    assert [name for name, _deadline in adapter.calls] == [
        "probe",
        "list_targets",
        "read_state",
        "set_receive_frequency",
        "verify_state",
        "set_receive_frequency",
        "verify_state",
    ]
    assert all(deadline == 90.0 for _name, deadline in adapter.calls)


def test_reversible_tune_cancellation_stops_verification_and_keeps_manual_fallback() -> None:
    adapter = _adapter()
    configured = adapter.targets[1]
    adapter.frequency_by_target[configured.target_id] = 7_100_000
    calls = 0

    def cancel_after_initial_probe() -> bool:
        nonlocal calls
        calls += 1
        # probe/list/read and the pre-set budget checks are allowed; the fake
        # set call observes cancellation and no verify/restore I/O follows.
        return calls >= 8

    result = run_reversible_receiver_tune_test(
        adapter,
        configured,
        7_115_000,
        deadline=90.0,
        cancel=cancel_after_initial_probe,
        monotonic=lambda: 80.0,
    )
    assert result.success is False
    assert result.applied is not None and result.applied.cancelled is True
    assert result.verified is None
    assert result.restore_verified is None
    assert result.operator_state == "Connected; verify tuning"
    assert "manual tuning" in result.detail.lower()


def test_reversible_tune_deadline_expiry_prevents_late_readback() -> None:
    adapter = _adapter()
    configured = adapter.targets[1]
    adapter.frequency_by_target[configured.target_id] = 7_100_000
    monotonic_calls = 0

    def advancing_monotonic() -> float:
        nonlocal monotonic_calls
        monotonic_calls += 1
        # Let probe/list/read/set start, then expire before verify.  The
        # qualification service must not issue a late verify or restore call.
        return 80.0 if monotonic_calls <= 4 else 90.0

    result = run_reversible_receiver_tune_test(
        adapter,
        configured,
        7_115_000,
        deadline=90.0,
        cancel=lambda: False,
        monotonic=advancing_monotonic,
    )
    assert result.success is False
    assert result.applied is not None
    assert result.verified is None
    assert result.restored is None
    assert [name for name, _deadline in adapter.calls] == [
        "probe",
        "list_targets",
        "read_state",
        "set_receive_frequency",
    ]


def test_manual_receiver_qualification_never_claims_fio_ready() -> None:
    identity = ReceiverIdentity(
        adapter_id="manual",
        receiver_id="rtl-sdr-manual",
        display_name="RTL-SDR manual",
        application_name="Other / manual",
        hardware_family="RTL-SDR",
    )
    client = ManualReceiverControl(identity, initial_frequency_hz=7_100_000)
    probe = probe_receiver_control(
        client,
        identity,
        deadline=10.0,
        cancel=lambda: False,
        monotonic=lambda: 1.0,
    )
    assert probe.operator_state == "Manual tuning"
    result = run_reversible_receiver_tune_test(
        client,
        identity,
        7_115_000,
        deadline=10.0,
        cancel=lambda: False,
        monotonic=lambda: 1.0,
    )
    assert result.success is False
    assert result.operator_state == "Manual tuning"
    assert result.applied is None
    assert "required for fio tuning" in result.detail.lower()


def test_adapter_close_is_idempotent_and_fences_late_calls() -> None:
    adapter = _adapter()
    identity = adapter.targets[0]
    adapter.close()
    adapter.close()
    assert adapter.close_calls == 2
    state = adapter.read_state(identity, deadline=1.0, cancel=lambda: False)
    assert state.available is False
    assert state.cancelled is True
    assert adapter.set_receive_frequency(identity, 7_200_000, deadline=1.0, cancel=lambda: False).cancelled
    assert adapter.frequency_by_target == {}


def _endpoint(target: str) -> EndpointKey:
    return EndpointKey.network("sdrpp-rigctl", "127.0.0.1", 4532, target=target)


def test_target_qualified_receiver_lanes_are_isolated_and_concurrent() -> None:
    registry = EndpointLaneRegistry()
    first_gate = threading.Event()
    second_gate = threading.Event()
    first_started = threading.Event()
    second_started = threading.Event()
    results: List[EndpointResult] = []

    def first() -> bool:
        first_started.set()
        first_gate.wait(2.0)
        return True

    def second() -> bool:
        second_started.set()
        second_gate.wait(2.0)
        return True

    try:
        assert registry.submit(_endpoint("vfo-a"), occurrence_id="a", operation=first, completion=results.append).accepted
        assert registry.submit(_endpoint("vfo-b"), occurrence_id="b", operation=second, completion=results.append).accepted
        assert first_started.wait(1.0)
        assert second_started.wait(1.0)
        assert len(registry) == 2
        assert _endpoint("vfo-a").canonical != _endpoint("vfo-b").canonical
    finally:
        first_gate.set()
        second_gate.set()
        registry.shutdown(wait=True)


def test_stale_receiver_completion_is_not_allowed_to_replace_newer_generation() -> None:
    registry = EndpointLaneRegistry()
    endpoint = _endpoint("vfo-a")
    old_gate = threading.Event()
    new_started = threading.Event()
    completed = threading.Event()
    results: List[EndpointResult] = []

    def record(result: EndpointResult) -> None:
        results.append(result)
        if len(results) >= 2:
            completed.set()

    def old() -> bool:
        old_gate.wait(2.0)
        return True

    def new() -> bool:
        new_started.set()
        return True

    try:
        first = registry.submit(endpoint, occurrence_id="old", operation=old, completion=record)
        assert first.generation == 1
        assert registry.submit(endpoint, occurrence_id="new", operation=new, completion=record).accepted
        old_gate.set()
        assert new_started.wait(1.0)
        assert completed.wait(1.0)
        lane = registry.lane(endpoint)
        assert lane is not None
        assert any(result.status == "superseded" and result.generation == 1 for result in results)
        assert lane.snapshot().last_success_generation == 2
        assert lane.snapshot().last_success_occurrence_id == "new"
    finally:
        old_gate.set()
        registry.shutdown(wait=True)
