"""Focused MES-1 tests for endpoint identity and pure coordination.

These tests intentionally exercise the Qt-free boundary in
``freqinout.core.scheduler_coordination``.  They do not construct a Qt
application, a database, an adapter, or an endpoint client.
"""

from __future__ import annotations

import datetime as dt
import builtins
import random
import socket
from typing import Optional

import pytest

from freqinout.core import scheduler_coordination as coordination
from freqinout.core.scheduler_coordination import (
    CoordinatorState,
    EndpointBinding,
    EndpointIdentityError,
    EndpointKey,
    EndpointResult,
    ScheduleLaneSnapshot,
    StationScheduleCoordinator,
    StationScheduleSnapshot,
    coordinate_schedule_snapshot,
    endpoint_binding_from_resolved_profile,
    bounded_exponential_backoff,
    snapshot_from_resolved_lanes,
)


UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


def _binding(
    profile_id: int,
    *,
    family: str = "flrig",
    port: int = 12345,
    name: Optional[str] = None,
    receive_only: bool = False,
    automated: bool = True,
) -> EndpointBinding:
    key = (
        EndpointKey.network(family, "127.0.0.1", port)
        if family != "manual"
        else EndpointKey.manual(profile_id)
    )
    return EndpointBinding(
        endpoint_key=key,
        device_profile_id=profile_id,
        device_name=name or f"Radio {profile_id}",
        receive_only=receive_only,
        automated=automated,
    )


def _snapshot(*lanes: ScheduleLaneSnapshot, revision: int = 1) -> StationScheduleSnapshot:
    return StationScheduleSnapshot(
        revision=revision,
        now_utc=NOW,
        monotonic_s=500.0 + revision,
        lanes=tuple(lanes),
    )


def _lane(
    binding: EndpointBinding,
    *,
    frequency: Optional[int] = 14_115_000,
    source: str = "HF",
    occurrence_id: str = "occ-1",
    force: bool = False,
    **extra: object,
) -> ScheduleLaneSnapshot:
    desired = {} if frequency is None else {"frequency_hz": frequency, **extra}
    return ScheduleLaneSnapshot.create(
        binding=binding,
        source=source,
        occurrence_id=occurrence_id,
        desired_state=desired,
        force=force,
    )


def test_endpoint_key_normalizes_hosts_ports_and_adapter_aliases_without_dns() -> None:
    assert EndpointKey.network("HAMLIB", "[2001:0DB8::1]", "4532").canonical == (
        "rigctld|tcp|[2001:db8::1]:4532"
    )
    assert EndpointKey.network("rig-ctld", "Radio.Example.", 4532).canonical == (
        "rigctld|tcp|radio.example:4532"
    )
    assert EndpointKey.network("JS8", "127.0.0.1", 2442).adapter_family == "js8call"
    assert EndpointKey.manual(9).canonical == "manual|none|device-profile:9"

    with pytest.raises(EndpointIdentityError):
        EndpointKey.network("flrig", "bad host", 12345)
    with pytest.raises(EndpointIdentityError):
        EndpointKey.network("flrig", "127.0.0.1", 0)
    with pytest.raises(EndpointIdentityError):
        EndpointKey.network("flrig", "127.0.0.1", 65536)
    with pytest.raises(EndpointIdentityError):
        EndpointKey.network("flrig", "127.0.0.1", "not-a-port")


@pytest.mark.parametrize(
    "unsafe_host",
    (
        "user:password@example.test",
        "https://example.test",
        "example.test/path",
        "example.test?token=secret",
    ),
)
def test_endpoint_key_rejects_url_or_credential_text_in_host_identity(unsafe_host: str) -> None:
    with pytest.raises(EndpointIdentityError, match="host is invalid"):
        EndpointKey.network("rigctld", unsafe_host, 4532)


def test_bounded_exponential_backoff_never_overflows_after_long_failure_runs() -> None:
    assert bounded_exponential_backoff(0.0, 0.0, 100_000) == 0.0
    assert bounded_exponential_backoff(5.0, 300.0, 1) == 5.0
    assert bounded_exponential_backoff(5.0, 300.0, 100_000) == 300.0
    assert bounded_exponential_backoff(5e-324, 1.0, 100_000) == 1.0
    with pytest.raises(ValueError, match="finite"):
        bounded_exponential_backoff(5.0, float("inf"), 2)


def test_endpoint_binding_projection_uses_resolved_profile_defaults_and_safe_manual_observer() -> None:
    flrig = endpoint_binding_from_resolved_profile({"id": 3, "name": "Main"})
    assert flrig.endpoint_key.canonical == "manual|none|device-profile:3"
    assert flrig.automated is False

    js8 = endpoint_binding_from_resolved_profile(
        {"device_profile_id": 4, "device_name": "Digital", "control_backend": "JS8"}
    )
    assert js8.endpoint_key.canonical == "js8call|tcp|127.0.0.1:2442"
    assert js8.automated is True

    hamlib = endpoint_binding_from_resolved_profile(
        {"id": 5, "control_backend": "hamlib", "rig_host": "[::1]", "rig_port": "4532"}
    )
    assert hamlib.endpoint_key.canonical == "rigctld|tcp|[::1]:4532"

    observer = endpoint_binding_from_resolved_profile(
        {"id": 6, "name": "Manual SDR", "device_class": "observer", "control_backend": "flrig"}
    )
    assert observer.receive_only is True
    assert observer.automated is False
    assert observer.endpoint_key.adapter_family == "manual"

    with pytest.raises(EndpointIdentityError, match="unsupported"):
        endpoint_binding_from_resolved_profile({"id": 7, "control_backend": "mystery"})


def test_snapshot_deep_freezes_nested_mapping_sequence_and_set() -> None:
    binding = _binding(1)
    desired = {
        "mode": "USB",
        "nested": {"levels": [1, {"enabled": True}], "tags": {"alpha", "beta"}},
    }
    snap = ScheduleLaneSnapshot.create(
        binding=binding, source="HF", occurrence_id="deep", desired_state=desired
    )
    desired["nested"]["levels"][1]["enabled"] = False  # type: ignore[index]
    desired["nested"]["tags"].add("mutated")  # type: ignore[index]

    thawed = snap.desired_state()
    assert thawed == {
        "mode": "USB",
        "nested": {"levels": [1, {"enabled": True}], "tags": {"alpha", "beta"}},
    }
    thawed["nested"]["levels"][1]["enabled"] = False  # type: ignore[index]
    assert snap.desired_state()["nested"]["levels"][1]["enabled"] is True  # type: ignore[index]


def test_snapshot_from_resolved_lanes_preserves_current_entry_and_requires_binding() -> None:
    binding = _binding(11)
    entry = {"target_device_profile_id": 11, "frequency": "14.115", "metadata": {"x": [1, 2]}}
    snapshot = snapshot_from_resolved_lanes(
        [{"device_profile_id": 11, "current_source": "SOP", "current_entry": entry}],
        bindings_by_profile={11: binding},
        revision=3,
        now_utc=NOW,
        monotonic_s=20.0,
    )
    entry["metadata"]["x"].append(3)  # type: ignore[index]
    assert snapshot.lanes[0].source == "SOP"
    assert snapshot.lanes[0].desired_state()["frequency"] == "14.115"
    assert snapshot.lanes[0].desired_state()["metadata"] == {"x": [1, 2]}
    assert snapshot.lanes[0].occurrence_id

    with pytest.raises(ValueError, match="no endpoint binding"):
        snapshot_from_resolved_lanes(
            [{"device_profile_id": 99, "current_entry": {"frequency": 1}}],
            bindings_by_profile={},
            revision=1,
            now_utc=NOW,
            monotonic_s=1.0,
        )


def test_same_route_compatible_aliases_collapse_to_one_intent() -> None:
    key = EndpointKey.network("flrig", "radio.local", 12345)
    first = EndpointBinding(key, 1, "Main")
    second = EndpointBinding(key, 2, "Alias")
    decision, state = coordinate_schedule_snapshot(_snapshot(_lane(first), _lane(second)))
    assert not decision.conflicts
    assert len(decision.intents) == 1
    assert decision.intents[0].device_profile_ids == (1, 2)
    assert state.generation_map()[key.canonical] == 1

    unchanged, _ = coordinate_schedule_snapshot(_snapshot(_lane(second), _lane(first)), state)
    assert unchanged.intents == ()
    assert unchanged.unchanged_endpoints == (key,)


def test_conflicting_duplicate_writers_produce_no_intent() -> None:
    key = EndpointKey.network("flrig", "radio.local", 12345)
    a = EndpointBinding(key, 1, "A")
    b = EndpointBinding(key, 2, "B")
    decision, _ = coordinate_schedule_snapshot(
        _snapshot(_lane(a, frequency=14_115_000), _lane(b, frequency=7_115_000))
    )
    assert decision.intents == ()
    assert [conflict.code for conflict in decision.conflicts] == ["competing_endpoint_intents"]


def test_one_profile_mapped_to_multiple_keys_is_a_global_conflict_with_no_intent() -> None:
    first = EndpointBinding(EndpointKey.network("flrig", "one.local", 12345), 42, "A")
    second = EndpointBinding(EndpointKey.network("flrig", "two.local", 12345), 42, "A")
    decision, _ = coordinate_schedule_snapshot(_snapshot(_lane(first), _lane(second)))
    assert decision.intents == ()
    assert {conflict.code for conflict in decision.conflicts} == {"profile_has_multiple_endpoint_keys"}


def test_generation_coalescing_force_idle_and_pruning_are_endpoint_scoped() -> None:
    a = _binding(1, port=12345)
    b = _binding(2, port=12346)
    coordinator = StationScheduleCoordinator()

    first = coordinator.evaluate(_snapshot(_lane(a), _lane(b)))
    assert {intent.generation for intent in first.intents} == {1}
    same = coordinator.evaluate(_snapshot(_lane(a), _lane(b), revision=2))
    assert same.intents == ()
    assert len(same.unchanged_endpoints) == 2

    changed = coordinator.evaluate(_snapshot(_lane(a, frequency=7_115_000), _lane(b), revision=3))
    assert [(intent.endpoint_key, intent.generation) for intent in changed.intents] == [(a.endpoint_key, 2)]
    forced = coordinator.evaluate(_snapshot(_lane(a, frequency=7_115_000, force=True), _lane(b), revision=4))
    assert forced.intents[0].generation == 3

    idle = coordinator.evaluate(_snapshot(_lane(a, frequency=None), _lane(b), revision=5))
    assert idle.idle_endpoints == (a.endpoint_key,)
    assert a.endpoint_key.canonical not in coordinator.state.signature_map()
    assert b.endpoint_key.canonical in coordinator.state.signature_map()

    pruned = coordinator.evaluate(_snapshot(_lane(a, frequency=7_115_000), revision=6))
    assert b.endpoint_key.canonical not in coordinator.state.signature_map()
    assert b.endpoint_key.canonical not in coordinator.state.generation_map()
    assert pruned.intents[0].endpoint_key == a.endpoint_key


@pytest.mark.parametrize("status", [
    "applied_and_verified", "applied_unverified", "readback_mismatch", "rejected", "superseded",
    "timed_out", "failed", "cancelled",
])
def test_endpoint_result_validates_and_normalizes_terminal_statuses(status: str) -> None:
    result = EndpointResult.create(
        endpoint_key=EndpointKey.manual(1), generation=2, status=status.replace("_", " "),
        actual_state={"frequency_hz": 14_115_000}, reason_code=" Timeout ", detail=" detail ",
    )
    assert result.status == status
    assert result.actual_fields
    assert result.reason_code == "timeout"
    assert result.detail == "detail"


def test_endpoint_result_rejects_unknown_status() -> None:
    with pytest.raises(ValueError, match="unsupported endpoint result status"):
        EndpointResult.create(endpoint_key=EndpointKey.manual(1), generation=1, status="working")


def test_coordinator_is_deterministic_under_fixed_seed_lane_permutations() -> None:
    bindings = [_binding(i, port=12_000 + i) for i in range(1, 5)]
    lanes = tuple(_lane(binding, frequency=14_000_000 + binding.device_profile_id) for binding in bindings)
    expected = None
    randomizer = random.Random(20260910)
    for _ in range(25):
        shuffled = list(lanes)
        randomizer.shuffle(shuffled)
        decision, state = coordinate_schedule_snapshot(_snapshot(*shuffled))
        comparable = (
            tuple((item.endpoint_key.canonical, item.device_profile_ids, item.generation) for item in decision.intents),
            tuple(item.endpoint_key.canonical for item in decision.idle_endpoints),
            tuple((key, value) for key, value in state.generations),
            tuple((key, value) for key, value in state.signatures),
        )
        expected = comparable if expected is None else expected
        assert comparable == expected


def test_coordinator_path_has_no_database_socket_file_or_adapter_io(monkeypatch: pytest.MonkeyPatch) -> None:
    def poison(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("coordinator attempted external I/O")

    monkeypatch.setattr(builtins, "open", poison)
    monkeypatch.setattr(socket, "socket", poison)
    binding = _binding(1)
    snapshot = _snapshot(_lane(binding))
    decision, _ = coordinate_schedule_snapshot(snapshot)
    assert len(decision.intents) == 1

    source = coordination.__loader__.get_source(coordination.__name__)  # type: ignore[union-attr]
    assert source is not None
    assert "sqlite3" not in source
    assert "socket.socket" not in source
    assert "ThreadPoolExecutor" not in source
    assert "QApplication" not in source
