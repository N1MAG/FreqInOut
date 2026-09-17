"""GRS-5 acceptance checks for receiver safety, scheduling, and recovery."""

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from freqinout.core.multi_rig_guardrails import collect_multi_rig_guardrail_warnings
from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.receiver_control import ReceiverCapabilities, ReceiverIdentity, ReceiverState
from freqinout.core.receiver_qualification import run_reversible_receiver_tune_test
from freqinout.core.receiver_software_stack import build_receiver_launch_items, validate_observer_launch_items
from freqinout.core.scheduler_coordination import EndpointKey
from freqinout.core.scheduler_engine import SchedulerEngine
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import StationRuntimeManager


class _Receiver:
    def __init__(self):
        self.identity = ReceiverIdentity("sdrpp", "rtl-1", "RTL", application_name="SDR++", target_id="vfo-a")
        self.frequency = 7_100_000
        self.calls = []

    def probe(self, **_kwargs):
        return self.identity, ReceiverCapabilities(can_list_targets=True, can_read_state=True, can_set_receive_frequency=True, can_verify_state=True, manual_only=False, readback_tolerance_hz=2)

    def list_targets(self, **_kwargs):
        return (self.identity,)

    def read_state(self, identity, **_kwargs):
        return ReceiverState(identity, available=True, running=True, frequency_hz=self.frequency, verified=True, manual=False)

    def set_receive_frequency(self, identity, frequency_hz, **_kwargs):
        self.calls.append(("set", frequency_hz))
        self.frequency = frequency_hz
        return self.read_state(identity)

    def verify_state(self, identity, command, **_kwargs):
        self.calls.append(("verify", command.frequency_hz))
        return ReceiverState(identity, available=True, running=True, frequency_hz=self.frequency, verified=self.frequency == command.frequency_hz, manual=False)


def test_receiver_guard_tune_is_reversible_and_never_exposes_ptt():
    receiver = _Receiver()
    identity = receiver.identity
    result = run_reversible_receiver_tune_test(receiver, identity, 7_101_000, deadline=10_000, cancel=lambda: False, monotonic=lambda: 0)
    assert result.success and result.restored.frequency_hz == 7_100_000 and result.restore_verified.verified
    assert [call[0] for call in receiver.calls] == ["set", "verify", "set", "verify"]


def test_receiver_schedule_stack_is_receive_only_and_rejects_arbitrary_transmit_tools():
    profile = {"device_class": "observer", "id": 1, "name": "RTL-SDR"}
    items = build_receiver_launch_items(profile, [{"name": "SDR++", "launch_path": "/usr/bin/sdrpp", "startup": True}])
    assert items[0]["execution_scope"] == "receive_only"
    validate_observer_launch_items(items)
    with pytest.raises(ValueError, match="not approved|not another executable|receive-only"):
        build_receiver_launch_items(profile, [{"name": "SDR++", "launch_command": "evil-ptt --transmit", "startup": True}])


def test_cross_radio_guardrail_reports_shared_receiver_resources_without_touching_unrelated_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE device_profiles (id INTEGER, name TEXT, display_order INTEGER, enabled INTEGER, runtime_active INTEGER, device_class TEXT, use_js8call INTEGER, js8_host TEXT, js8_port INTEGER, flrig_host TEXT, flrig_port INTEGER, fldigi_host TEXT, fldigi_port INTEGER, rigctld_host TEXT, rigctld_port INTEGER, varac_bbs_dir TEXT, varac_db_path TEXT, flamp_message_path TEXT, flmsg_message_path TEXT)")
    conn.executemany("INSERT INTO device_profiles VALUES (?, ?, ?, 1, ?, ?, 1, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", [(1, "A", 1, 1, "transceiver", "127.0.0.1", 2442), (2, "B", 2, 1, "transceiver", "127.0.0.1", 2442), (3, "inactive", 3, 0, "transceiver", "127.0.0.1", 2442)])
    warnings = collect_multi_rig_guardrail_warnings(conn)
    assert any(w.resource_type == "JS8Call API endpoint" and w.affected_radio_ids == (1, 2) for w in warnings)


def test_receiver_guard_persists_as_pair_scoped_blocking_policy(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    tx = store.save_device_profile(
        {
            "name": "TX",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "antenna_group": "NORTH",
            "frontend_group": "PRESELECTOR-A",
        }
    )
    observer = store.save_device_profile(
        {
            "name": "SDR",
            "device_class": "observer",
            "control_backend": "manual",
            "antenna_group": "north",
            "frontend_group": "preselector-a",
        }
    )
    unrelated = store.save_device_profile(
        {
            "name": "Unrelated SDR",
            "device_class": "observer",
            "control_backend": "manual",
            "antenna_group": "SOUTH",
        }
    )

    policies = store.list_station_coordination_policies("rf_conflict")
    matching = [
        row
        for row in policies
        if {int(row["source_device_id"]), int(row["target_device_id"])}
        == {int(tx["id"]), int(observer["id"])}
    ]
    assert len(matching) == 1
    assert matching[0]["safety_mode"] == "block"
    assert matching[0]["trigger"]["receiver_guard"] is True
    assert matching[0]["trigger"]["antenna_groups"] == ["NORTH"]
    assert matching[0]["trigger"]["frontend_groups"] == ["PRESELECTOR-A"]
    assert all(
        int(unrelated["id"]) not in {int(row["source_device_id"]), int(row["target_device_id"])}
        for row in policies
    )


def test_blocking_receiver_guard_holds_tune_and_exposes_recovery_telemetry():
    engine = SchedulerEngine.__new__(SchedulerEngine)
    engine._receiver_desired_by_profile = {}
    engine._expected_state_by_endpoint = {}
    engine._endpoint_keys_by_profile = {}
    engine._endpoint_lanes = None
    engine._endpoint_status = None
    engine._monotonic_clock = lambda: 0.0
    events = []
    engine._record_scheduler_event = lambda *args, **kwargs: events.append((args, kwargs))
    engine._record_scheduler_health_issue = lambda *args, **kwargs: None
    engine._coordination_conflict_status = lambda *_args, **_kwargs: {
        "blocked": True,
        "summary": "Receiver resource is busy.",
        "detail": "TX holds shared antenna NORTH.",
        "signature": "north-held",
    }
    binding = SimpleNamespace(
        endpoint_key=EndpointKey.network("sdrpp_rigctl", "127.0.0.1", 4532, target="vfo-a"),
        automated=True,
    )
    engine._endpoint_keys_by_profile[7] = binding.endpoint_key
    engine._receiver_context_for_profile = lambda *_args, **_kwargs: pytest.fail(
        "a blocked receiver tune must not touch the endpoint"
    )

    engine._apply_receiver_schedule_entry(
        lane={"device_profile_id": 7, "device_name": "SDR"},
        binding=binding,
        entry={"frequency_hz": 7_100_000, "target_device_profile_id": 7},
        source="HF",
        force=False,
    )

    summary = engine.get_receiver_desired_summaries()[7]
    assert summary["state"] == "safety_hold"
    assert summary["reason_code"] == "receiver_shared_resource_conflict"
    assert summary["guard_signature"] == "north-held"
    assert "shared antenna/front-end conflict" in summary["recovery_action"]
    assert events[0][0][:2] == ("blocked", "receiver_shared_resource_conflict")
    operational = engine.get_endpoint_operational_summaries()[7]
    assert operational["state"] == "waiting_shared_resource"
    assert operational["detail"] == "TX holds shared antenna NORTH."
    assert operational["reason_code"] == "receiver_shared_resource_conflict"
    assert "Clear the named shared antenna/front-end conflict" in operational["recovery_action"]


def test_receiver_guard_arbitrates_real_runtime_pair_from_cached_peer_evidence(monkeypatch, tmp_path):
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    tx = store.save_device_profile(
        {
            "name": "TX",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.1",
            "flrig_port": 12345,
            "antenna_group": "NORTH",
            "frontend_group": "PRESELECTOR-A",
        }
    )
    receiver_model = store.save_operating_profile({"name": "Receive only", "receive_only": 1})
    observer = store.save_device_profile(
        {
            "name": "SDR",
            "device_class": "observer",
            "control_backend": "manual",
            "antenna_group": "north",
            "frontend_group": "preselector-a",
        }
    )
    store.set_device_operating_profile(int(observer["id"]), int(receiver_model["id"]))
    store.set_device_profile_runtime_active(int(tx["id"]), True)
    store.set_device_profile_runtime_active(int(observer["id"]), True)
    store.set_runtime_primary_device_profile(int(tx["id"]))

    manager = StationRuntimeManager(store=store, settings=settings)
    manager.sync_with_store()
    conflict = manager.evaluate_rf_conflict_for_device(
        int(observer["id"]),
        target_band="40M",
        target_frequency_hz=7_100_000,
        source="HF",
        status_by_device={int(tx["id"]): {"frequency_hz": 7_074_000, "frequency_known": True}},
    )

    assert conflict is not None
    assert conflict.blocked is True
    assert conflict.guard_mode == "block"
    assert conflict.shared_antenna_groups == ["NORTH"]
    assert conflict.shared_frontend_groups == ["PRESELECTOR-A"]
    assert conflict.peer_device_profile_id == int(tx["id"])


def test_recovery_telemetry_is_explicit_in_receiver_failure_result():
    receiver = _Receiver()
    def cancelled():
        return True
    with pytest.raises(Exception, match="cancelled|manual tuning"):
        run_reversible_receiver_tune_test(receiver, receiver.identity, 7_101_000, deadline=10_000, cancel=cancelled, monotonic=lambda: 0)


def test_no_gui_thread_or_filesystem_dependency_in_receiver_qualification_module():
    source = Path(__file__).parents[1] / "freqinout" / "core" / "receiver_qualification.py"
    text = source.read_text(encoding="utf-8")
    assert "from PyQt" not in text and "sqlite3" not in text and "Path(" not in text


@pytest.mark.skip(reason="Pending operator-assisted live RTL-SDR/SDR++ qualification evidence")
def test_live_rtl_sdr_sdrpp_tune_readback_restore_gate():
    """Hardware evidence must be collected on the configured station, not faked."""


@pytest.mark.skip(reason="Pending operator-assisted live receiver resource-arbitration evidence")
def test_live_shared_antenna_frontend_arbitration_gate():
    """Shared antenna/frontend contention requires a real station run."""


@pytest.mark.parametrize(
    "gate",
    (
        "macos-and-linux-guided-add-radio",
        "windows-guided-add-radio",
        "transceiver-and-radio-control",
        "fast-light-native-applications",
        "js8-stock-improved-subspace",
        "varac-cluster-multi-node",
    ),
)
@pytest.mark.skip(reason="Pending operator-assisted live external-application/platform qualification evidence")
def test_live_guided_external_integration_gates(gate):
    """External application and platform evidence is recorded, never inferred."""
