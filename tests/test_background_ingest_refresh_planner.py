from __future__ import annotations

import time
from concurrent.futures import Future

import freqinout.core.background_ingest as background_ingest
from freqinout.core.background_ingest import BackgroundIngestController
from freqinout.core.ingest_health import source_health_key
from freqinout.core.ingest_source_model import IngestSourceDescriptor, IngestSourceInventory, build_ingest_source_inventory


class _Settings:
    def __init__(self, values=None):
        self._values = dict(values or {})

    def get(self, key, default=None):
        return self._values.get(key, default)


class _PlannerOnlyController(BackgroundIngestController):
    def __init__(self, inventory: IngestSourceInventory):
        super().__init__(_Settings())  # type: ignore[arg-type]
        self._inventory = inventory
        self.submitted: list[str] = []

    def _runtime_ingest_inventory(self) -> IngestSourceInventory:
        return self._inventory

    def _submit_job(self, job_name, job_func):  # type: ignore[override]
        self.submitted.append(str(job_name))
        job_func()


def _source(tmp_path, *, family: str = "js8call", source_type: str = "file") -> IngestSourceDescriptor:
    path = tmp_path / f"{family}-{source_type}.txt"
    path.write_text("one\n")
    return IngestSourceDescriptor(
        source_id=f"{family}-{source_type}-1",
        family=family,
        source_type=source_type,
        label="source",
        path=str(path),
    )


def _sqlite_source(tmp_path, *, family: str = "commstat") -> IngestSourceDescriptor:
    path = tmp_path / f"{family}.db"
    path.write_bytes(b"sqlite-ish")
    return IngestSourceDescriptor(
        source_id=f"{family}-sqlite-1",
        family=family,
        source_type="sqlite",
        label="source",
        path=str(path),
    )


def test_background_message_ingest_skips_unchanged_file_sources(tmp_path):
    controller = _PlannerOnlyController(IngestSourceInventory(ingest_sources=(_source(tmp_path),)))
    controller._running = True
    calls = {"messages": 0}

    def fake_run(*, include_observation_backfill=True):
        calls["messages"] += 1

    controller._run_messages_job = fake_run  # type: ignore[method-assign]

    controller._ingest_messages(include_observation_backfill=False)
    controller._ingest_messages(include_observation_backfill=False)

    assert calls["messages"] == 1
    assert controller.submitted == ["messages"]
    status = controller.job_status_snapshot()
    assert status["skip_reasons"]["messages"] == "unchanged"
    assert status["refresh_skip_reasons"]["messages"] == "unchanged"
    assert status["refresh_decisions"]["messages"]["should_run"] is False
    assert status["refresh_decisions"]["messages"]["reason"] == "unchanged"


def test_background_message_ingest_force_bypasses_unchanged_skip(tmp_path):
    controller = _PlannerOnlyController(IngestSourceInventory(ingest_sources=(_source(tmp_path),)))
    controller._running = True
    calls = {"messages": 0}

    def fake_run(*, include_observation_backfill=True):
        calls["messages"] += 1

    controller._run_messages_job = fake_run  # type: ignore[method-assign]

    controller._ingest_messages(include_observation_backfill=False)
    controller._ingest_messages(include_observation_backfill=False, force=True)

    assert calls["messages"] == 2
    assert controller.submitted == ["messages", "messages"]


def test_background_varac_ingest_runs_periodic_quiet_pass(tmp_path):
    source = _source(tmp_path, family="varac", source_type="sqlite")
    controller = _PlannerOnlyController(IngestSourceInventory(ingest_sources=(source,)))
    controller._running = True
    calls = {"varac": 0}

    def fake_run():
        calls["varac"] += 1

    controller._run_varac_job = fake_run  # type: ignore[method-assign]

    controller._ingest_varac()
    controller._job_refresh_last_run_ts["varac"] = time.time() - 900.0
    controller._ingest_varac()

    assert calls["varac"] == 2
    assert controller.submitted == ["varac", "varac"]
    assert controller.job_status_snapshot()["refresh_decisions"]["varac"]["reason"] == "cadence"


def test_background_submit_job_records_duplicate_running_skip_reason():
    controller = BackgroundIngestController(_Settings())  # type: ignore[arg-type]
    controller._running = True
    pending = Future()
    controller._job_futures["messages"] = pending

    controller._submit_job("messages", lambda: None)

    status = controller.job_status_snapshot()
    assert status["skipped_counts"]["messages"] == 1
    assert status["skip_reasons"]["messages"] == "already_running"


def test_background_submit_job_records_backoff_skip_reason(monkeypatch):
    controller = BackgroundIngestController(_Settings())  # type: ignore[arg-type]
    controller._running = True
    monkeypatch.setattr(controller._health, "may_run", lambda *_args, **_kwargs: (False, {"cooldown_remaining_sec": 12.0}))

    controller._submit_job("messages", lambda: None)

    status = controller.job_status_snapshot()
    assert status["skipped_counts"]["messages"] == 1
    assert status["skip_reasons"]["messages"] == "backoff"


def test_background_source_skip_snapshot_records_source_context(tmp_path):
    source = _source(tmp_path)
    controller = BackgroundIngestController(_Settings())  # type: ignore[arg-type]
    health_key = f"{source_health_key(source)}:inbox"

    controller._record_source_skip(
        health_key,
        source,
        "backoff",
        {"cooldown_remaining_sec": 12.0},
        source_type="js8-inbox",
        path="/tmp/inbox",
    )

    status = controller.job_status_snapshot()
    row = status["source_skip_reasons"][health_key]
    assert row["reason"] == "backoff"
    assert row["label"] == "source"
    assert row["family"] == "js8call"
    assert row["source_type"] == "js8-inbox"
    assert row["path"] == "/tmp/inbox"
    assert row["cooldown_remaining_sec"] == 12.0
    assert row["app_instance_id"] == source.app_instance_id
    assert row["skipped_at_ts"] > 0

    controller._clear_source_skip(health_key)
    assert health_key not in controller.job_status_snapshot()["source_skip_reasons"]


def test_background_js8_inbox_missing_source_is_visible_in_snapshot(tmp_path):
    directed = tmp_path / "radio-a" / "DIRECTED.TXT"
    directed.parent.mkdir()
    directed.write_text("", encoding="utf-8")
    profile = {
        "id": "A",
        "name": "FIO-A",
        "use_js8call": True,
        "js8_directed_path": str(directed),
    }
    inventory = build_ingest_source_inventory([profile])
    controller = _PlannerOnlyController(inventory)
    controller._active_js8_spotter_profiles = lambda: [profile]  # type: ignore[method-assign]

    controller._run_multi_radio_js8_message_ingest()

    status = controller.job_status_snapshot()
    directed_source = next(
        source
        for source in inventory.sources_for_family("js8call")
        if source.source_type == "file" and source.metadata.get("role") == "directed"
    )
    health_key = f"{source_health_key(directed_source)}:inbox"
    row = status["source_skip_reasons"][health_key]
    assert row["reason"] == "missing"
    assert row["source_type"] == "js8-inbox"
    assert row["path"] == str(directed)


def test_background_js8_explicit_inbox_missing_uses_inbox_source_identity(tmp_path):
    directed = tmp_path / "radio-a" / "DIRECTED.TXT"
    inbox = tmp_path / "radio-a" / "missing-inbox.db"
    directed.parent.mkdir()
    directed.write_text("", encoding="utf-8")
    profile = {
        "id": "A",
        "name": "FIO-A",
        "use_js8call": True,
        "js8_directed_path": str(directed),
        "js8_inbox_path": str(inbox),
    }
    inventory = build_ingest_source_inventory([profile])
    controller = _PlannerOnlyController(inventory)
    controller._active_js8_spotter_profiles = lambda: [profile]  # type: ignore[method-assign]

    controller._run_multi_radio_js8_message_ingest()

    inbox_source = next(
        source
        for source in inventory.sources_for_family("js8call")
        if source.source_type == "sqlite" and source.metadata.get("role") == "inbox"
    )
    health_key = f"{source_health_key(inbox_source)}:inbox"
    row = controller.job_status_snapshot()["source_skip_reasons"][health_key]
    assert row["reason"] == "missing"
    assert row["label"] == "FIO-A JS8Call Inbox"
    assert row["path"] == str(inbox)


def test_background_js8_links_manual_refresh_bypasses_unchanged_skip(tmp_path):
    controller = _PlannerOnlyController(IngestSourceInventory(ingest_sources=(_source(tmp_path),)))
    controller._running = True
    calls = {"links": 0}

    def fake_run():
        calls["links"] += 1

    controller._run_js8_links_job = fake_run  # type: ignore[method-assign]

    controller._ingest_js8_links()
    controller.request_refresh("js8_links", "force")

    assert calls["links"] == 2
    assert controller.submitted == ["js8_links", "js8_links"]


def test_sitrep_commstat_source_ingest_skips_unchanged_sources_but_fusion_runs(monkeypatch, tmp_path):
    controller = _PlannerOnlyController(IngestSourceInventory(ingest_sources=(_sqlite_source(tmp_path),)))
    calls = {"commstat": 0, "fusion": 0}

    def fake_fuse(_settings, max_rows=1000):
        calls["fusion"] += 1
        return {"events_upserted": 0, "latest_updated": 0}

    monkeypatch.setattr(background_ingest, "fuse_sitreps", fake_fuse)
    controller._should_run_legacy_sitrep_ingest = lambda force=False: False  # type: ignore[method-assign]
    controller._run_commstat_source_sitrep_ingest = lambda _settings, force=False: calls.__setitem__("commstat", calls["commstat"] + 1)  # type: ignore[method-assign]

    controller._run_sitreps_job()
    controller._run_sitreps_job()

    assert calls["commstat"] == 2
    assert calls["fusion"] == 2


def test_commstat_source_sitrep_ingest_skips_unchanged_source_fingerprint(tmp_path):
    controller = _PlannerOnlyController(IngestSourceInventory(ingest_sources=(_sqlite_source(tmp_path),)))

    decision1 = controller._source_backed_refresh_decision(
        job_name="sitreps:commstat",
        family="commstat",
        source_types=("sqlite",),
        force=False,
        max_quiet_sec=900.0,
    )
    controller._job_refresh_fingerprints["sitreps:commstat"] = decision1.fingerprint
    controller._job_refresh_last_run_ts["sitreps:commstat"] = time.time()
    decision2 = controller._source_backed_refresh_decision(
        job_name="sitreps:commstat",
        family="commstat",
        source_types=("sqlite",),
        force=False,
        max_quiet_sec=900.0,
    )

    assert decision1.should_run is True
    assert decision2.should_run is False
    assert decision2.reason == "unchanged"


def test_commstat_source_ingest_batch_limit_defaults_to_initial_import_size():
    controller = _PlannerOnlyController(IngestSourceInventory())

    assert controller._commstat_source_ingest_batch_limit(_Settings()) == 50000


def test_commstat_source_ingest_batch_limit_is_bounded():
    controller = _PlannerOnlyController(IngestSourceInventory())

    assert controller._commstat_source_ingest_batch_limit(_Settings({"commstat_source_ingest_batch_limit": 5})) == 1000
    assert (
        controller._commstat_source_ingest_batch_limit(
            _Settings({"commstat_source_ingest_batch_limit": 999999})
        )
        == 250000
    )


def test_runtime_ingest_inventory_is_cached_and_cleared_on_settings_refresh(monkeypatch, tmp_path):
    source = _source(tmp_path)
    inventory = IngestSourceInventory(ingest_sources=(source,))
    calls = {"inventory": 0, "vault_refresh": 0}

    def fake_inventory():
        calls["inventory"] += 1
        return inventory

    monkeypatch.setattr(background_ingest, "active_runtime_ingest_inventory", fake_inventory)
    controller = BackgroundIngestController(_Settings())  # type: ignore[arg-type]
    controller.request_varac_vault_refresh = lambda reason="manual": calls.__setitem__("vault_refresh", calls["vault_refresh"] + 1)  # type: ignore[method-assign]

    assert controller._runtime_ingest_inventory() is inventory
    assert controller._runtime_ingest_inventory() is inventory
    assert calls["inventory"] == 1
    status = controller.job_status_snapshot()
    assert status["runtime_inventory_cached"] is True
    assert status["runtime_inventory_cache_age_sec"] >= 0.0

    controller.refresh_runtime_settings()
    assert controller._runtime_ingest_inventory() is inventory
    assert calls["inventory"] == 2
    assert calls["vault_refresh"] == 1
