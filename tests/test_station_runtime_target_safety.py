from __future__ import annotations

from typing import Any, Dict

from freqinout.core.station_runtime_manager import StationRuntimeManager


class _NoIoRuntime:
    """Runtime stand-in that proves cached safety checks do not poll endpoints."""

    def __init__(self, device_id: int, name: str, *, ptt_group: str = "") -> None:
        self.profile: Dict[str, Any] = {
            "id": int(device_id),
            "name": name,
            "ptt_group": ptt_group,
            "control_backend": "flrig",
        }
        self.is_primary = False
        self.ptt_calls = 0
        self.frequency_calls = 0

    def ptt_active(self, *, force: bool = False) -> bool:
        self.ptt_calls += 1
        raise AssertionError("cached PTT safety evaluation must not poll an endpoint")

    def current_frequency_hz(self, *, force: bool = False) -> int:
        self.frequency_calls += 1
        raise AssertionError("cached RF safety evaluation must not poll an endpoint")


def _manager(*runtimes: _NoIoRuntime) -> StationRuntimeManager:
    manager = StationRuntimeManager.__new__(StationRuntimeManager)
    manager._runtimes = {int(runtime.profile["id"]): runtime for runtime in runtimes}
    manager._primary_device_id = int(runtimes[0].profile["id"])
    manager._rf_conflict_policies = []
    return manager


def test_cached_shared_ptt_evidence_is_clear_without_endpoint_io() -> None:
    target = _NoIoRuntime(1, "TX-A", ptt_group="AMP-A")
    peer = _NoIoRuntime(2, "TX-B", ptt_group="AMP-A")
    manager = _manager(target, peer)

    snapshot = manager.shared_ptt_lock_snapshot(
        for_device_id=1,
        status_by_device={1: {"ptt_active": False}, 2: {"ptt_active": False}},
    )

    assert snapshot.blocked is False
    assert snapshot.evidence_known is True
    assert snapshot.evidence_stale is False
    assert snapshot.reason == "Shared PTT group AMP-A is clear."
    assert target.ptt_calls == peer.ptt_calls == 0


def test_cached_shared_ptt_missing_or_stale_evidence_hard_blocks_without_io() -> None:
    target = _NoIoRuntime(1, "TX-A", ptt_group="AMP-A")
    peer = _NoIoRuntime(2, "TX-B", ptt_group="AMP-A")
    manager = _manager(target, peer)

    missing = manager.shared_ptt_lock_snapshot(
        for_device_id=1,
        status_by_device={1: {"ptt_active": False}},
    )
    assert missing.blocked is True
    assert missing.evidence_known is False
    assert missing.evidence_stale is False
    assert "TX-B: cached PTT evidence is missing" in missing.evidence_detail
    assert "incomplete" in missing.reason

    stale = manager.shared_ptt_lock_snapshot(
        for_device_id=1,
        status_by_device={1: {"ptt_active": False}, 2: {"ptt_active": False, "stale": True}},
    )
    assert stale.blocked is True
    assert stale.evidence_known is False
    assert stale.evidence_stale is True
    assert "TX-B: cached PTT evidence is stale" in stale.evidence_detail

    errored = manager.shared_ptt_lock_snapshot(
        for_device_id=1,
        status_by_device={1: {"ptt_active": False}, 2: {"ptt_active": False, "errors": {"ptt": "offline"}}},
    )
    assert errored.blocked is True
    assert errored.evidence_known is False
    assert "TX-B: cached PTT evidence error" in errored.evidence_detail
    assert target.ptt_calls == peer.ptt_calls == 0


def test_cached_shared_ptt_detects_known_active_peer_without_io() -> None:
    target = _NoIoRuntime(1, "TX-A", ptt_group="AMP-A")
    peer = _NoIoRuntime(2, "TX-B", ptt_group="AMP-A")
    manager = _manager(target, peer)

    snapshot = manager.shared_ptt_lock_snapshot(
        for_device_id=1,
        status_by_device={1: {"ptt_active": False}, 2: {"ptt_active": True}},
    )

    assert snapshot.blocked is True
    assert snapshot.evidence_known is True
    assert snapshot.owner_device_profile_id == 2
    assert snapshot.owner_name == "TX-B"
    assert target.ptt_calls == peer.ptt_calls == 0


def test_targeted_rf_conflict_uses_requested_target_and_cached_peer_frequency_without_io() -> None:
    primary = _NoIoRuntime(1, "Primary")
    target = _NoIoRuntime(2, "Scheduled Target")
    manager = _manager(primary, target)
    manager._rf_conflict_policies = [
        {
            "enabled": 1,
            "source_device_id": 1,
            "target_device_id": 2,
            "trigger": {"band_overlap_groups": ["Shared Mast"]},
            "safety_mode": "block",
        }
    ]

    conflict = manager.evaluate_rf_conflict_for_device(
        2,
        target_band="40M",
        target_frequency_hz=7_078_000,
        status_by_device={1: {"frequency_hz": 7_078_000}, 2: {"frequency_hz": 14_070_000}},
    )

    assert conflict is not None
    assert conflict.target_device_profile_id == 2
    assert conflict.target_device_name == "Scheduled Target"
    assert conflict.peer_device_profile_id == 1
    assert conflict.same_frequency is True
    assert conflict.blocked is True
    assert primary.frequency_calls == target.frequency_calls == 0


def test_targeted_rf_conflict_treats_missing_or_stale_cached_peer_as_unknown_without_io() -> None:
    primary = _NoIoRuntime(1, "Primary")
    target = _NoIoRuntime(2, "Scheduled Target")
    manager = _manager(primary, target)
    manager._rf_conflict_policies = [
        {
            "enabled": 1,
            "source_device_id": 1,
            "target_device_id": 2,
            "trigger": {"band_overlap_groups": ["Shared Mast"]},
            "safety_mode": "block",
        }
    ]

    missing = manager.evaluate_rf_conflict_for_device(
        2,
        target_band="40M",
        target_frequency_hz=7_078_000,
        status_by_device={2: {"frequency_hz": 7_078_000}},
    )
    assert missing is not None
    assert missing.blocked is True
    assert missing.peer_status_unknown is True
    assert "cached peer frequency evidence is missing" in missing.peer_status_detail

    stale = manager.evaluate_rf_conflict_for_device(
        2,
        target_band="40M",
        target_frequency_hz=7_078_000,
        status_by_device={1: {"frequency_hz": 7_078_000, "stale": True}},
    )
    assert stale is not None
    assert stale.blocked is True
    assert stale.peer_status_unknown is True
    assert stale.peer_status_stale is True
    assert stale.peer_status_detail == "last peer frequency check is stale"

    errored = manager.evaluate_rf_conflict_for_device(
        2,
        target_band="40M",
        target_frequency_hz=7_078_000,
        status_by_device={1: {"frequency_hz": 7_078_000, "errors": {"frequency": "offline"}}},
    )
    assert errored is not None
    assert errored.peer_status_unknown is True
    assert "cached peer frequency evidence error" in errored.peer_status_detail
    assert primary.frequency_calls == target.frequency_calls == 0
