from __future__ import annotations

import datetime
import json
import sqlite3
from types import SimpleNamespace
import threading

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.receiver_control import (
    ReceiverCapabilities,
    ReceiverCommand,
    ReceiverIdentity,
    ReceiverState,
)
from freqinout.core.scheduler_coordination import endpoint_binding_from_resolved_profile
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import DeviceRuntime


class _FakeReceiver:
    def __init__(self, identity: ReceiverIdentity, *, block: bool = False) -> None:
        self.identity = identity
        self.block = block
        self.entered = threading.Event()
        self.release = threading.Event()
        self.verified = threading.Event()
        self.frequency_hz = 0
        self.closed = False

    def probe(self, *, deadline, cancel):
        return self.identity, ReceiverCapabilities(
            can_read_state=True,
            can_set_receive_frequency=True,
            can_verify_state=True,
            readback_tolerance_hz=20,
            manual_only=False,
        )

    def list_targets(self, *, deadline, cancel):
        return (self.identity,)

    def read_state(self, target, *, deadline, cancel):
        return self._state(verified=True)

    def set_receive_frequency(self, target, frequency_hz, *, deadline, cancel):
        assert target == self.identity
        self.entered.set()
        if self.block:
            self.release.wait(2.0)
        self.frequency_hz = int(frequency_hz)
        return self._state(verified=False)

    def set_receive_mode(self, target, mode, bandwidth_hz=None, *, deadline, cancel):
        return self._state(verified=False)

    def verify_state(self, target, expected: ReceiverCommand, *, tolerance_hz, deadline, cancel):
        assert target == self.identity
        assert expected.target_id == self.identity.target_id
        self.verified.set()
        return self._state(verified=self.frequency_hz == expected.frequency_hz)

    def close(self):
        self.closed = True

    def _state(self, *, verified: bool) -> ReceiverState:
        return ReceiverState(
            identity=self.identity,
            available=True,
            running=True,
            frequency_hz=self.frequency_hz or None,
            verified=verified,
            manual=False,
        )


def _profile(profile_id: int = 4, *, enabled: bool = True, verified: bool = True):
    profile = {
        "id": profile_id,
        "name": f"SDR {profile_id}",
        "device_class": "observer",
        "control_backend": "manual",
        "sdr_application": "SDR++",
        "sdr_adapter": "sdrpp_rigctl",
        "sdr_host": "127.0.0.1",
        "sdr_port": 5000 + profile_id,
        "sdr_target": "vfo-a",
        "sdr_control_enabled": 1 if enabled else 0,
        "sdr_verification_state": "verified" if verified else "unverified",
    }
    if verified:
        profile["sdr_verification_json"] = json.dumps(
            {
                "schema_version": 1,
                "tested_at_utc": "2026-09-10T12:00:00+00:00",
                "adapter": "sdrpp_rigctl",
                "host": "127.0.0.1",
                "port": 5000 + profile_id,
                "target": "vfo-a",
                "tune_readback_verified": True,
                "restore_readback_verified": True,
            }
        )
    return profile


def _engine(monkeypatch, tmp_path) -> SchedulerEngine:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    engine = SchedulerEngine(poll_interval_ms=60_000)
    engine._queue_scheduler_thread_call = lambda callback: callback()
    engine._record_scheduler_event = lambda *args, **kwargs: None
    engine._record_scheduler_health_issue = lambda *args, **kwargs: None
    engine._clear_scheduler_health_issue = lambda *args, **kwargs: None
    engine._coordination_conflict_status = lambda *args, **kwargs: {}
    return engine


def test_automated_receiver_uses_receive_only_lane_and_verified_readback(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    profile = _profile()
    binding = endpoint_binding_from_resolved_profile(profile)
    identity = ReceiverIdentity(
        adapter_id="sdrpp_rigctl",
        receiver_id="profile-4:vfo-a",
        display_name="SDR 4",
        application_name="SDR++",
        target_id="vfo-a",
    )
    receiver = _FakeReceiver(identity)
    runtime = SimpleNamespace(receiver_client=receiver, receiver_identity=identity)
    engine.station_runtime_manager = SimpleNamespace(_runtimes={4: runtime})
    try:
        engine._apply_receiver_schedule_entry(
            lane={"device_profile_id": 4, "device_name": "SDR 4"},
            binding=binding,
            entry={"frequency": "7.115", "mode": "USB", "target_device_profile_id": 4},
            source="HF",
            force=False,
        )
        assert receiver.verified.wait(1.0)
        summaries = engine.get_receiver_desired_summaries()
        assert summaries[4]["state"] == "fio_tuning_ready"
        assert summaries[4]["actual"]["frequency_hz"] == 7_115_000
        assert not any(hasattr(receiver, name) for name in ("get_ptt", "set_ptt", "transmit", "send"))
    finally:
        engine.stop()


def test_manual_receiver_records_guidance_without_creating_worker(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    binding = endpoint_binding_from_resolved_profile(_profile(enabled=False))
    try:
        before = len(engine._endpoint_lanes)
        engine._apply_receiver_schedule_entry(
            lane={"device_profile_id": 4, "device_name": "Manual SDR"},
            binding=binding,
            entry={"frequency": "14.115", "target_device_profile_id": 4},
            source="HF",
            force=False,
        )
        assert len(engine._endpoint_lanes) == before
        assert engine.get_receiver_desired_summaries()[4]["state"] == "manual_tuning"
    finally:
        engine.stop()


def test_receiver_profile_requires_verified_complete_config_before_enable(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    with pytest.raises(ValueError, match="verification"):
        store.save_device_profile(_profile(8, verified=False))
    with pytest.raises(ValueError, match="host, port"):
        store.save_device_profile({**_profile(8), "sdr_target": ""})
    with pytest.raises(ValueError, match="exact adapter, host, port, and target"):
        store.save_device_profile({**_profile(8), "sdr_host": "localhost"})
    with pytest.raises(ValueError, match="exact adapter, host, port, and target"):
        store.save_device_profile({**_profile(8), "sdr_verification_json": "{}"})

    saved = store.save_device_profile(_profile(8))
    assert saved["sdr_adapter"] == "sdrpp_rigctl"
    assert saved["sdr_target"] == "vfo-a"
    assert int(saved["sdr_control_enabled"]) == 1
    assert saved["sdr_verification_state"] == "verified"


def test_incomplete_or_unverified_receiver_binding_falls_back_to_manual() -> None:
    for profile in (
        _profile(9, enabled=False),
        _profile(10, verified=False),
        {"id": 11, "name": "Manual", "device_class": "observer"},
    ):
        binding = endpoint_binding_from_resolved_profile(profile)
        assert binding.receive_only is True
        assert binding.automated is False
        assert binding.endpoint_key.adapter_family == "manual"


def test_enabled_receiver_requires_canonical_target() -> None:
    profile = {**_profile(12), "sdr_target": "VFO A"}
    evidence = json.loads(str(profile["sdr_verification_json"]))
    evidence["target"] = "VFO A"
    profile["sdr_verification_json"] = json.dumps(evidence)
    with pytest.raises(ValueError, match="canonical token"):
        endpoint_binding_from_resolved_profile(profile)


def test_active_lane_routes_observer_around_transceiver_apply(monkeypatch, tmp_path) -> None:
    engine = _engine(monkeypatch, tmp_path)
    profile = _profile(13)
    captured = []
    engine.active_schedule_lanes = lambda **kwargs: [
        {
            "device_profile": profile,
            "device_profile_id": 13,
            "device_name": "SDR 13",
            "current_source": "HF",
            "current_entry": {"frequency": "7.115", "mode": "USB"},
            "frequency_plan_name": "Receive Plan",
        }
    ]
    engine._coordinated_schedule_writer_ids = lambda *args, **kwargs: ({13}, {})
    engine._scheduling_suspended_for_radio = lambda *args, **kwargs: (False, None)
    engine._apply_receiver_schedule_entry = lambda **kwargs: captured.append(kwargs)
    engine._apply_schedule_entry = lambda *args, **kwargs: pytest.fail(
        "observer entered the transmit-capable schedule path"
    )
    try:
        assert engine._apply_active_schedule_lanes(
            now_utc=datetime.datetime.now(datetime.timezone.utc)
        )
        assert len(captured) == 1
        assert captured[0]["binding"].receive_only is True
        assert captured[0]["entry"]["target_device_profile_id"] == 13
    finally:
        engine.stop()


def test_additive_receiver_columns_preserve_existing_profile(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    db_path = settings_db_path()
    store = MultiRadioStore(db_path)
    saved = store.save_device_profile(
        {
            "name": "Existing Manual Observer",
            "device_class": "observer",
            "control_backend": "manual",
            "sdr_host": "127.0.0.1",
            "sdr_port": 5000,
        }
    )
    added_columns = (
        "sdr_application",
        "sdr_adapter",
        "sdr_target",
        "sdr_control_enabled",
        "sdr_verification_state",
        "sdr_verification_json",
    )
    with sqlite3.connect(str(db_path)) as conn:
        for column in added_columns:
            conn.execute(f"ALTER TABLE device_profiles DROP COLUMN {column}")
        conn.commit()

    # Schema repair belongs to the startup-owned writable boundary.  Runtime
    # profile reads remain read-only and must not be relied on to heal a clone.
    migrated_store = MultiRadioStore(db_path)
    with migrated_store.connect() as startup_connection:
        columns = {
            str(row[1])
            for row in startup_connection.execute("PRAGMA table_info(device_profiles)")
        }
    assert set(added_columns) <= columns

    migrated = migrated_store.get_device_profile(int(saved["id"]))
    assert migrated is not None
    assert migrated["name"] == "Existing Manual Observer"
    assert migrated["sdr_host"] == "127.0.0.1"
    assert migrated["sdr_adapter"] == "manual"
    assert int(migrated["sdr_control_enabled"]) == 0
    assert migrated["sdr_verification_state"] == "manual"


def test_observer_runtime_owns_only_receive_client_and_closes_it() -> None:
    profile = {**_profile(14), "control_backend": "rigctld"}
    identity = ReceiverIdentity(
        adapter_id="sdrpp_rigctl",
        receiver_id="profile-14:vfo-a",
        display_name="SDR 14",
        application_name="SDR++",
        target_id="vfo-a",
    )
    receiver = _FakeReceiver(identity)
    runtime = DeviceRuntime(
        profile,
        is_primary=False,
        receiver_client_factory=lambda _profile_row: receiver,
    )
    try:
        assert runtime.receiver_client is receiver
        assert runtime.receiver_identity is not None
        assert runtime.rig_client is None
        assert runtime.js8_control_client is None
        snapshot = runtime.snapshot(force=True)
        assert snapshot.control_ready is False
        assert snapshot.service_states["Observer"]["control_state"] == "receiver_unavailable"
    finally:
        runtime.stop()
    assert receiver.closed is True
