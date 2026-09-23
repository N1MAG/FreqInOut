"""Focused regressions for the scheduler idle-CPU remediation slice."""

from __future__ import annotations

import concurrent.futures
import threading
from types import SimpleNamespace

from freqinout.core.scheduler_engine import SchedulerEngine


class _ImmediateExecutor:
    """Run serial-worker submissions synchronously for a deterministic test."""

    def submit(self, callback, *args, **kwargs):
        future: concurrent.futures.Future = concurrent.futures.Future()
        try:
            future.set_result(callback(*args, **kwargs))
        except BaseException as exc:  # pragma: no cover - test diagnostic
            future.set_exception(exc)
        return future

    def shutdown(self, **_kwargs):
        return None


def test_status_worker_reuses_runtime_settings_reloads_saved_control_mode_and_closes(monkeypatch) -> None:
    """Five-second status passes keep one worker-owned, reloadable manager."""

    import freqinout.core.scheduler_engine as scheduler_module

    persisted = {"control_via": "FLRig"}
    instances = []
    js8_constructed = []

    class _Settings:
        def __init__(self, *, runtime_worker=False):
            self.runtime_worker = bool(runtime_worker)
            self.values = dict(persisted)
            self.reload_count = 0
            self.close_count = 0
            instances.append(self)

        def reload(self):
            self.reload_count += 1
            self.values = dict(persisted)

        def get(self, key, default=None):
            return self.values.get(key, default)

        def close(self):
            self.close_count += 1

    class _VarAC:
        def __init__(self, *, settings=None):
            self.settings = settings

        def get_status(self, **_kwargs):
            return {"busy": False, "waiting_for_frequency": False, "reason": None}

    class _JS8:
        def __init__(self, *args, **kwargs):
            js8_constructed.append((args, kwargs))

        def is_busy(self):
            return False

        def get_frequency(self):
            return 7_078_000

        def get_offset(self):
            return 1_500

    class _SoftwareStatus:
        def __init__(self, _settings):
            pass

        def js8_shadow_comparison_status(self, **_kwargs):
            return {}

    monkeypatch.setattr(scheduler_module, "SettingsManager", _Settings)
    monkeypatch.setattr(scheduler_module, "DaemonSerialExecutor", lambda **_kwargs: _ImmediateExecutor())
    monkeypatch.setattr(scheduler_module, "VarACStatusClient", _VarAC)
    monkeypatch.setattr(scheduler_module, "JS8ControlClient", _JS8)
    monkeypatch.setattr(scheduler_module, "SoftwareStatusService", _SoftwareStatus)

    engine = SchedulerEngine()
    engine._queue_scheduler_thread_call = lambda callback: callback()
    engine._status_snapshot_refresh_interval_s = 0.0
    try:
        engine._maybe_refresh_external_status_snapshot(force=False)
        persisted["control_via"] = "JS8Call"
        engine._maybe_refresh_external_status_snapshot(force=False)

        runtime_instances = [item for item in instances if item.runtime_worker]
        assert len(runtime_instances) == 1
        assert runtime_instances[0].reload_count >= 2
        assert len(js8_constructed) == 1
    finally:
        engine.stop()

    assert runtime_instances[0].close_count == 1


def test_shared_ptt_evidence_is_edge_triggered_and_changed_owner_republishes() -> None:
    """Identical PTT state is quiet; changed blocking evidence is retained."""

    class _Busy:
        def __init__(self):
            self.published = []
            self.cleared = []

        def publish(self, evidence):
            self.published.append(evidence)

        def clear(self, evidence_id):
            self.cleared.append(evidence_id)

    class _Conflicts(_Busy):
        pass

    engine = SchedulerEngine.__new__(SchedulerEngine)
    engine._primary_manual_control_radio_id = lambda: 7
    engine._busy_evidence_service = _Busy()
    engine._ptt_conflict_service = _Conflicts()
    engine._published_busy_evidence_ids = set()
    engine._busy_evidence_signatures = {}
    engine._busy_evidence_clear_checked_ids = set()
    engine._published_ptt_conflict_ids = set()
    engine._ptt_conflict_signatures = {}
    engine._ptt_conflict_clear_checked_ids = set()

    first = {"ptt_group": "AMP-A", "owner_device_profile_id": 3, "reason": "Rig A is keyed."}
    changed = {"ptt_group": "AMP-A", "owner_device_profile_id": 4, "reason": "Rig B is keyed."}
    engine._publish_shared_ptt_block_evidence(first, source="HF")
    engine._publish_shared_ptt_block_evidence(first, source="HF")
    assert len(engine._busy_evidence_service.published) == 1
    assert len(engine._ptt_conflict_service.published) == 1

    engine._publish_shared_ptt_block_evidence(changed, source="HF")
    assert len(engine._busy_evidence_service.published) == 2
    assert len(engine._ptt_conflict_service.published) == 2

    engine._clear_shared_ptt_block_evidence()
    engine._clear_shared_ptt_block_evidence()
    assert engine._busy_evidence_service.cleared == ["busy_shared_ptt_7"]
    assert engine._ptt_conflict_service.cleared == ["ptt_shared_7"]


def test_background_profile_snapshot_is_reused_until_settings_generation_changes(monkeypatch) -> None:
    """JS8/FLAMP eligibility shares one linked-profile discovery per generation."""

    import freqinout.core.background_ingest as ingest_module

    calls = {"profiles": 0}

    class _Store:
        pass

    def _profiles(_store):
        calls["profiles"] += 1
        return [{"id": 1, "use_js8call": 1, "use_js8spotter": 0}]

    _Store.list_runtime_active_device_profiles = _profiles
    monkeypatch.setattr(ingest_module, "MultiRadioStore", _Store)
    monkeypatch.setattr(
        ingest_module,
        "build_multi_rig_runtime_status",
        lambda _store: SimpleNamespace(background_ingest_scope=ingest_module.SCOPE_ALL_ACTIVE_RUNTIME),
    )

    controller = ingest_module.BackgroundIngestController.__new__(ingest_module.BackgroundIngestController)
    controller._runtime_profile_cache_lock = threading.RLock()
    controller._runtime_profile_generation = 0
    controller._runtime_profile_cache_generation = -1
    controller._runtime_profile_cache = ()
    controller._runtime_inventory_cache = None
    controller._runtime_inventory_cache_ts = 0.0
    controller.request_varac_vault_refresh = lambda _reason: None

    assert controller._active_js8_spotter_profiles() == [{"id": 1, "use_js8call": 1, "use_js8spotter": 0}]
    assert controller._active_js8_spotter_profiles() == [{"id": 1, "use_js8call": 1, "use_js8spotter": 0}]
    assert calls["profiles"] == 1

    controller.refresh_runtime_settings()
    assert controller._active_js8_spotter_profiles() == [{"id": 1, "use_js8call": 1, "use_js8spotter": 0}]
    assert calls["profiles"] == 2


def test_background_profile_snapshot_retries_after_transient_read_failure(monkeypatch) -> None:
    """A failed inventory read must not become a cached empty station."""

    import freqinout.core.background_ingest as ingest_module

    calls = {"profiles": 0}

    class _Store:
        def list_runtime_active_device_profiles(self):
            calls["profiles"] += 1
            if calls["profiles"] == 1:
                raise RuntimeError("database temporarily busy")
            return [{"id": 2, "use_js8call": 1}]

    monkeypatch.setattr(ingest_module, "MultiRadioStore", _Store)
    monkeypatch.setattr(
        ingest_module,
        "build_multi_rig_runtime_status",
        lambda _store: SimpleNamespace(background_ingest_scope=ingest_module.SCOPE_ALL_ACTIVE_RUNTIME),
    )

    controller = ingest_module.BackgroundIngestController.__new__(ingest_module.BackgroundIngestController)
    controller._runtime_profile_cache_lock = threading.RLock()
    controller._runtime_profile_generation = 0
    controller._runtime_profile_cache_generation = -1
    controller._runtime_profile_cache = ()

    assert controller._runtime_active_profiles() == []
    assert controller._runtime_profile_cache_generation == -1
    assert controller._runtime_active_profiles() == [{"id": 2, "use_js8call": 1}]
    assert controller._runtime_active_profiles() == [{"id": 2, "use_js8call": 1}]
    assert calls["profiles"] == 2
