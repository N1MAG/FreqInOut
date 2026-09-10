"""Deterministic MES-4 receiver-lane and multi-endpoint gate tests.

These tests deliberately exercise the Qt-free coordinator/lane boundary.  They
use events instead of sleeps so a blocked observer endpoint cannot hide a
regression in peer progress or shutdown.
"""

from datetime import datetime, timezone
import threading
from typing import Dict, List, Mapping

from freqinout.core.scheduler_coordination import (
    EndpointBinding,
    EndpointKey,
    StationScheduleCoordinator,
    endpoint_binding_from_resolved_profile,
    snapshot_from_resolved_lanes,
    validate_endpoint_bindings,
)
from freqinout.core.scheduler_endpoint_lane import EndpointLaneRegistry


def _binding(
    key: EndpointKey,
    profile_id: int,
    name: str,
    *,
    receive_only: bool = False,
    automated: bool = True,
) -> EndpointBinding:
    return EndpointBinding(
        endpoint_key=key,
        device_profile_id=profile_id,
        device_name=name,
        receive_only=receive_only,
        automated=automated,
    )


def _snapshot(bindings: List[EndpointBinding]):
    rows = [
        {
            "device_profile_id": binding.device_profile_id,
            "current_source": "SCHEDULE",
            "occurrence_id": "mes4-{}".format(binding.device_profile_id),
            "current_entry": {"frequency_hz": 7_000_000 + binding.device_profile_id},
        }
        for binding in bindings
    ]
    return snapshot_from_resolved_lanes(
        rows,
        bindings_by_profile={item.device_profile_id: item for item in bindings},
        revision=1,
        now_utc=datetime(2026, 9, 10, tzinfo=timezone.utc),
        monotonic_s=100.0,
    )


def _network(family: str, port: int, *, target: str = "") -> EndpointKey:
    return EndpointKey.network(family, "127.0.0.1", port, target=target)


def test_mes4_five_endpoint_transition_runs_three_transceivers_and_two_sdrs() -> None:
    """A desired-state transition includes all endpoints without global serialization."""

    bindings = [
        _binding(_network("rigctld", 4532), 1, "HF-1"),
        _binding(_network("flrig", 12345), 2, "HF-2"),
        _binding(_network("js8call", 2442), 3, "HF-3"),
        _binding(_network("sdrpp", 5000, target="vfo-a"), 4, "SDR-1", receive_only=True),
        _binding(_network("sdrconnect", 5001, target="vfo-a"), 5, "SDR-2", receive_only=True),
    ]
    decision = StationScheduleCoordinator().evaluate(_snapshot(bindings))

    assert not decision.conflicts
    assert len(decision.intents) == 5
    assert {intent.device_profile_ids for intent in decision.intents} == {
        (1,),
        (2,),
        (3,),
        (4,),
        (5,),
    }
    assert {intent.endpoint_key for intent in decision.intents if intent.receive_only} == {
        bindings[3].endpoint_key,
        bindings[4].endpoint_key,
    }

    registry = EndpointLaneRegistry(base_backoff_s=0.0)
    completed: Dict[EndpointKey, object] = {}
    events: Dict[EndpointKey, threading.Event] = {
        intent.endpoint_key: threading.Event() for intent in decision.intents
    }
    try:
        for intent in decision.intents:
            key = intent.endpoint_key
            desired = intent.desired_state()

            def operation(desired_state=desired):
                return {"ok": True, "actual_state": desired_state}

            def completion(result, endpoint_key=key):
                completed[endpoint_key] = result
                events[endpoint_key].set()

            submission = registry.submit(
                key,
                occurrence_id=intent.occurrence_id,
                operation=operation,
                completion=completion,
            )
            assert submission.accepted

        for event in events.values():
            assert event.wait(2.0)
        assert set(completed) == {intent.endpoint_key for intent in decision.intents}
        assert all(result.status == "applied_unverified" for result in completed.values())
    finally:
        registry.shutdown(wait=True)


def test_mes4_hung_sdr_does_not_delay_transceiver_or_peer_sdr() -> None:
    blocked = threading.Event()
    entered = threading.Event()
    tx_done = threading.Event()
    peer_sdr_done = threading.Event()
    blocked_sdr = _network("sdrpp", 5100, target="vfo-a")
    transceiver = _network("rigctld", 4532)
    peer_sdr = _network("sdrconnect", 5101, target="vfo-a")
    registry = EndpointLaneRegistry(base_backoff_s=0.0)
    results: Dict[EndpointKey, object] = {}

    def blocked_operation():
        entered.set()
        blocked.wait(2.0)
        return {"ok": True, "actual_state": {"frequency_hz": 7_100_000}}

    def quick_operation():
        return {"ok": True, "actual_state": {"frequency_hz": 14_100_000}}

    def record(result, key, event):
        results[key] = result
        event.set()

    try:
        assert registry.submit(
            blocked_sdr,
            occurrence_id="blocked-sdr",
            operation=blocked_operation,
            completion=lambda result: record(result, blocked_sdr, threading.Event()),
        ).accepted
        assert entered.wait(1.0)
        assert registry.submit(
            transceiver,
            occurrence_id="transceiver",
            operation=quick_operation,
            completion=lambda result: record(result, transceiver, tx_done),
        ).accepted
        assert registry.submit(
            peer_sdr,
            occurrence_id="peer-sdr",
            operation=quick_operation,
            completion=lambda result: record(result, peer_sdr, peer_sdr_done),
        ).accepted

        assert tx_done.wait(1.0)
        assert peer_sdr_done.wait(1.0)
        assert transceiver in results
        assert peer_sdr in results
        assert blocked_sdr not in results
    finally:
        blocked.set()
        registry.shutdown(wait=True)


def test_mes4_receive_only_intent_has_no_transmit_surface() -> None:
    class FakeReceiveOnlyAdapter:
        def __init__(self):
            self.frequency_hz = 0

        def set_receive_frequency(self, frequency_hz: int) -> Mapping[str, int]:
            self.frequency_hz = int(frequency_hz)
            return {"frequency_hz": self.frequency_hz}

        def read_state(self) -> Mapping[str, int]:
            return {"frequency_hz": self.frequency_hz}

    adapter = FakeReceiveOnlyAdapter()
    key = _network("sdrpp", 5200, target="vfo-a")
    binding = _binding(key, 20, "Receive-only SDR", receive_only=True)
    decision = StationScheduleCoordinator().evaluate(_snapshot([binding]))
    intent = decision.intents[0]

    assert intent.receive_only is True
    assert intent.automated is True
    assert "ptt" not in intent.desired_state()
    assert not any(
        hasattr(adapter, name)
        for name in ("set_ptt", "transmit", "send", "key")
    )
    actual = adapter.set_receive_frequency(int(intent.desired_state()["frequency_hz"]))
    assert actual == adapter.read_state()


def test_mes4_manual_and_no_api_receivers_remain_manual_fallback() -> None:
    profiles = (
        {"id": 30, "name": "Manual SDR", "device_class": "observer", "control_backend": "sdrpp"},
        {"id": 31, "name": "No API Radio", "control_backend": "manual"},
    )
    bindings = [endpoint_binding_from_resolved_profile(profile) for profile in profiles]
    assert all(binding.endpoint_key.adapter_family == "manual" for binding in bindings)
    assert all(binding.automated is False for binding in bindings)

    decision = StationScheduleCoordinator().evaluate(_snapshot(bindings))
    assert len(decision.intents) == 2
    assert all(intent.automated is False for intent in decision.intents)
    assert all(intent.receive_only is (intent.device_profile_ids[0] == 30) for intent in decision.intents)

    registry = EndpointLaneRegistry()
    try:
        # Manual fallback is operator-owned; the scheduler must not create a
        # command lane merely because a manual desired state was resolved.
        assert len(registry) == 0
    finally:
        registry.shutdown(wait=True)


def test_mes4_aliases_share_owner_but_conflicting_intents_are_blocked() -> None:
    sdr_plus = _network("sdr++", 5300, target="vfo-a")
    sdr_canonical = _network("sdrpp", 5300, target="vfo-a")
    first = _binding(sdr_plus, 40, "SDR alias A", receive_only=True)
    second = _binding(sdr_canonical, 41, "SDR alias B", receive_only=True)

    assert sdr_plus == sdr_canonical
    assert validate_endpoint_bindings((first, second)) == ()
    same_state = snapshot_from_resolved_lanes(
        [
            {
                "device_profile_id": 40,
                "current_source": "SCHEDULE",
                "occurrence_id": "shared-sdr",
                "current_entry": {"frequency_hz": 7_040_000},
            },
            {
                "device_profile_id": 41,
                "current_source": "SCHEDULE",
                "occurrence_id": "shared-sdr",
                "current_entry": {"frequency_hz": 7_040_000},
            },
        ],
        bindings_by_profile={40: first, 41: second},
        revision=1,
        now_utc=datetime(2026, 9, 10, tzinfo=timezone.utc),
        monotonic_s=100.0,
    )
    same_decision = StationScheduleCoordinator().evaluate(same_state)
    assert not same_decision.conflicts
    assert len(same_decision.intents) == 1
    assert same_decision.intents[0].device_profile_ids == (40, 41)

    conflicting = snapshot_from_resolved_lanes(
        [
            {"device_profile_id": 40, "current_source": "SCHEDULE", "current_entry": {"frequency_hz": 7_040_000}},
            {"device_profile_id": 41, "current_source": "SCHEDULE", "current_entry": {"frequency_hz": 14_041_000}},
        ],
        bindings_by_profile={40: first, 41: second},
        revision=2,
        now_utc=datetime(2026, 9, 10, tzinfo=timezone.utc),
        monotonic_s=101.0,
    )
    conflict_decision = StationScheduleCoordinator().evaluate(conflicting)
    assert any(conflict.code == "competing_endpoint_intents" for conflict in conflict_decision.conflicts)
    assert not conflict_decision.intents

    distinct_target = _binding(_network("sdrpp", 5300, target="vfo-b"), 42, "SDR VFO B", receive_only=True)
    assert distinct_target.endpoint_key != first.endpoint_key
    assert not validate_endpoint_bindings((first, distinct_target))


def test_mes4_reconnect_retries_one_endpoint_without_affecting_peers() -> None:
    key = _network("sdrpp", 5400, target="vfo-a")
    peer = _network("rigctld", 4532)
    attempts = [0]
    first_done = threading.Event()
    second_done = threading.Event()
    peer_done = threading.Event()
    statuses: List[str] = []
    registry = EndpointLaneRegistry(base_backoff_s=0.0)

    def reconnecting_operation():
        attempts[0] += 1
        if attempts[0] == 1:
            return {"ok": False, "reason_code": "disconnected"}
        return {"ok": True, "actual_state": {"frequency_hz": 7_540_000}}

    try:
        assert registry.submit(
            key,
            occurrence_id="connect-1",
            operation=reconnecting_operation,
            completion=lambda result: (statuses.append(result.status), first_done.set()),
        ).accepted
        assert first_done.wait(1.0)
        assert statuses == ["failed"]

        assert registry.submit(
            key,
            occurrence_id="connect-2",
            operation=reconnecting_operation,
            completion=lambda result: (statuses.append(result.status), second_done.set()),
        ).accepted
        assert registry.submit(
            peer,
            occurrence_id="peer-connect",
            operation=lambda: {"ok": True},
            completion=lambda result: peer_done.set(),
        ).accepted
        assert second_done.wait(1.0)
        assert peer_done.wait(1.0)
        assert statuses == ["failed", "applied_unverified"]
        assert attempts[0] == 2
    finally:
        registry.shutdown(wait=True)


def test_mes4_shutdown_is_bounded_and_suppresses_late_sdr_completion() -> None:
    entered = threading.Event()
    release = threading.Event()
    returned = threading.Event()
    completions: List[object] = []
    key = _network("sdrconnect", 5500, target="vfo-a")
    registry = EndpointLaneRegistry()

    def blocked_operation():
        entered.set()
        release.wait(2.0)
        return {"ok": True}

    try:
        assert registry.submit(
            key,
            occurrence_id="shutdown-sdr",
            operation=blocked_operation,
            completion=lambda result: completions.append(result),
        ).accepted
        assert entered.wait(1.0)

        def close_registry():
            registry.shutdown(wait=False)
            returned.set()

        closer = threading.Thread(target=close_registry)
        closer.start()
        assert returned.wait(1.0)
        closer.join(1.0)
        assert not closer.is_alive()
        assert registry.snapshots()[0].closed is True

        release.set()
        # The operation is allowed to finish, but its callback must be fenced
        # after shutdown so stale health cannot be published.
        assert registry.lane(key).future.result(timeout=1.0) is not None
        assert completions == []
    finally:
        release.set()
        registry.shutdown(wait=True)
