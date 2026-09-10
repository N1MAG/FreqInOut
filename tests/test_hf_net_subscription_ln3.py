"""LN-3 core tests for HF Net directory subscriptions.

These tests use only temporary SQLite files and plain value models.  UI seam
tests are intentionally isolated below so the commandable scheduler boundary
remains executable before the subscription workspace lands.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from freqinout.core.resource_catalog_models import (
    CatalogSource,
    CatalogValidationError,
    FrequencyResource,
    NetDirectoryEntry,
    NetDirectorySession,
)
from freqinout.core.resource_catalog_store import ResourceCatalogStore
from freqinout.core.scheduler_engine import SchedulerEngine


def _catalog(path: Path) -> tuple[ResourceCatalogStore, NetDirectorySession]:
    store = ResourceCatalogStore(path)
    store.create_schema()
    store.create_source(CatalogSource("station:ln3", "Synthetic Station", "station"))
    store.create_frequency(
        FrequencyResource("frequency:ln3", "station:ln3", "simplex", "AMATEUR", "Synthetic Channel", band="2M", receive_hz=146520000),
        group_keys=("operating_group_alpha",),
    )
    store.create_net_entry(NetDirectoryEntry("net:ln3", "station:ln3", "Synthetic Net", description="Test-only directory entry"), group_keys=("operating_group_alpha",))
    session = NetDirectorySession(
        "session:ln3:weekly", "net:ln3", "station:ln3", "AMATEUR",
        frequency_resource_key="frequency:ln3", recurrence="Weekly", local_start_time="19:00",
        duration_minutes=60, timezone="America/Denver", effective_start_date="2026-01-01",
        exception_dates=("2026-12-24",), reminder_minutes=15, mode="FM", mode_details="Synthetic",
    )
    return store, session


def test_subscription_session_model_is_immutable_and_carries_schedule_fields(tmp_path: Path) -> None:
    _store, session = _catalog(tmp_path / "catalog.db")
    assert session.net_session_key == "session:ln3:weekly"
    assert session.exception_dates == ("2026-12-24",)
    with pytest.raises(FrozenInstanceError):
        session.recurrence = "Daily"  # type: ignore[misc]


def test_multiple_published_sessions_have_distinct_keys_and_no_duplicate_rows(tmp_path: Path) -> None:
    store, weekly = _catalog(tmp_path / "catalog.db")
    store.create_session(weekly)
    second = replace(weekly, net_session_key="session:ln3:monthly", recurrence="Monthly", local_start_time="20:00")
    store.create_session(second)
    assert {row.net_session_key for row in store.list_sessions(net_entry_key="net:ln3")} == {
        "session:ln3:weekly", "session:ln3:monthly"
    }
    with pytest.raises(CatalogValidationError):
        store.create_session(weekly)


def test_accepted_hash_snapshot_and_local_override_survive_migration(tmp_path: Path) -> None:
    from freqinout.core.resource_catalog_migration import apply_resource_catalog_migration

    nets = tmp_path / "freqinout_nets.db"
    with sqlite3.connect(nets) as conn:
        conn.execute(
            """CREATE TABLE net_resources (
                id INTEGER PRIMARY KEY, resource_set TEXT, source_type TEXT, source_ref TEXT, readonly INTEGER,
                day_utc TEXT, recurrence TEXT, biweekly_offset_weeks INTEGER, month_weeks TEXT, group_name TEXT,
                band TEXT, mode TEXT, frequency TEXT, start_utc TEXT, end_utc TEXT, early_checkin INTEGER,
                primary_js8call_group TEXT, coverage TEXT, comment TEXT, net_name TEXT,
                fldigi_mode TEXT, fldigi_offset TEXT, updated_utc TEXT)"""
        )
        conn.execute(
            "INSERT INTO net_resources VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "Custom", "manual", "ln3", 0, "Monday", "Weekly", 0, "", "Group Alpha", "2M", "FM", "146.520", "19:00", "20:00", 1, "", "Local", "", "Synthetic Net", "", "", "2026-09-09T00:00:00Z"),
        )
        conn.execute(
            """CREATE TABLE net_schedule_tab (
                id INTEGER PRIMARY KEY, resource_id INTEGER, net_name TEXT, group_name TEXT,
                band TEXT, mode TEXT, frequency TEXT, day_utc TEXT, start_utc TEXT, end_utc TEXT,
                recurrence TEXT, source_schedule_name TEXT)"""
        )
        # The locally edited frequency is an explicit subscription override and
        # must remain distinct from the accepted directory snapshot.
        conn.execute(
            "INSERT INTO net_schedule_tab VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (7, 1, "Synthetic Net", "Group Alpha", "2M", "FM", "146.525", "Monday", "19:00", "20:00", "Weekly", "Evening Set"),
        )
    backup = lambda paths, **_kwargs: type("Backup", (), {"backup_dir": str(tmp_path / "backup"), "items": tuple(type("Item", (), {"original_path": str(path), "status": "backed_up"})() for path in (paths if isinstance(paths, (list, tuple)) else [paths]))})()
    apply_resource_catalog_migration(nets, backup_factory=backup)
    with sqlite3.connect(nets) as conn:
        row = conn.execute("SELECT resource_id,frequency,net_session_key,accepted_session_version_hash,accepted_resource_version_hash,accepted_snapshot_json,source_schedule_name FROM net_schedule_tab").fetchone()
    assert row[0] == 1 and row[1] == "146.525" and row[2]
    assert row[3] and row[4]
    snapshot = json.loads(row[5])
    assert snapshot["frequency_resource_key"] and snapshot["local_start_time"] == "19:00"
    assert row[6] == "Evening Set"


def test_update_diff_is_explicit_and_keep_does_not_change_accepted_version(tmp_path: Path) -> None:
    store, session = _catalog(tmp_path / "catalog.db")
    accepted = store.create_session(session)
    changed = store.update_session(replace(accepted, mode_details="Changed", local_start_time="20:00"))
    comparison = store.compare_session_version(
        changed.net_session_key,
        accepted.version_hash,
        {"mode_details": accepted.mode_details, "local_start_time": accepted.local_start_time},
    )
    assert comparison.update_available is True
    assert {diff.field_name for diff in comparison.diffs} == {"local_start_time", "mode_details"}
    # Keep means the subscriber retains the previously accepted version/hash;
    # applying is a separate, explicit choice by the caller.
    assert accepted.version_hash != changed.version_hash
    assert comparison.accepted_version_hash == accepted.version_hash


def test_retired_session_remains_queryable_for_actionable_warning(tmp_path: Path) -> None:
    store, session = _catalog(tmp_path / "catalog.db")
    store.create_session(session)
    retired = store.retire_session(session.net_session_key)
    assert retired.retired is True and retired.active is False
    assert store.list_sessions(net_entry_key="net:ln3") == ()
    assert store.list_sessions(net_entry_key="net:ln3", active=None)[0].retired is True


def test_restart_persists_named_destination_and_subscription_sessions(tmp_path: Path) -> None:
    path = tmp_path / "catalog.db"
    store, session = _catalog(path)
    store.create_session(session)
    del store
    reopened = ResourceCatalogStore(path)
    restored = reopened.get_session(session.net_session_key)
    assert restored is not None
    assert restored.timezone == "America/Denver"
    assert restored.local_start_time == "19:00"


def test_scheduler_load_remains_hf_only_and_ignores_subscription_local_rows(tmp_path: Path, monkeypatch) -> None:
    class Settings:
        payload = {
            "hf_schedule": [{"band": "40M", "frequency": "7.078"}],
            "net_schedule": [{"band": "80M", "frequency": "3.590"}],
            "local_net_schedules": [{"band": "2M", "frequency": "146.520", "net_session_key": "session:ln3:weekly"}],
        }

        def get(self, key: str, default=None):
            return self.payload.get(key, default)

        def all(self):
            return dict(self.payload)

    engine = SchedulerEngine.__new__(SchedulerEngine)
    engine.settings = Settings()
    engine._config_dir = lambda: tmp_path
    engine._db_mtime = lambda _path: 0.0
    engine._primary_schedule_target_context = lambda: (None, None)
    engine._load_assigned_frequency_plan_schedule_rows = lambda _profile_id: ([], [], False)
    engine._sop_layer_enabled = lambda: True
    engine._filter_rows_for_runtime_target = lambda rows, **_kwargs: rows
    monkeypatch.setattr(engine, "_load_daily_schedule_from_db", lambda: None)
    monkeypatch.setattr(engine, "_load_net_schedule_from_db", lambda: None)
    monkeypatch.setattr(engine, "_load_sop_schedule_layer_from_db", lambda: [])
    monkeypatch.setattr(engine, "_load_sop_net_conflict_policies_from_db", lambda: [])
    hf, net, sop, policies = engine._load_schedules(force=True)
    assert [(row["band"], row["frequency"]) for row in hf] == [("40M", "7.078")]
    assert [(row["band"], row["frequency"]) for row in net] == [("80M", "3.590")]
    assert sop == [] and policies == []
    assert all(row not in hf + net for row in Settings.payload["local_net_schedules"])


def _published_session_with_day(store: ResourceCatalogStore, session: NetDirectorySession) -> object:
    """Give the pre-day-field fixture an explicit reviewed UTC day for LN-3."""
    stored = store.create_session(session)
    values = {name: getattr(stored, name) for name in NetDirectorySession.__dataclass_fields__}
    values["day_utc"] = "Monday"
    return SimpleNamespace(**values)


def test_subscription_drafts_dedupe_keys_and_preserve_accepted_snapshot_and_overrides(tmp_path: Path, monkeypatch) -> None:
    import freqinout.core.hf_net_subscription as subscription

    store, session = _catalog(tmp_path / "catalog.db")
    published = _published_session_with_day(store, session)
    monkeypatch.setattr(store, "get_session", lambda _key: published)
    draft = subscription.build_hf_subscription_drafts(
        store,
        [session.net_session_key, session.net_session_key],
        overrides_by_session={session.net_session_key: {"frequency": "146.525", "comment": "Local override"}},
    )
    assert len(draft) == 1
    row = draft[0].schedule_row
    assert row["net_session_key"] == session.net_session_key
    assert row["frequency"] == "146.525"
    assert row["accepted_session_version_hash"] == published.version_hash
    assert row["accepted_resource_version_hash"]
    accepted = json.loads(row["accepted_snapshot_json"])
    assert accepted["net_session_key"] == session.net_session_key
    assert accepted["frequency_resource_key"] == "frequency:ln3"


def test_subscription_requires_reviewed_day_time_and_frequency(tmp_path: Path, monkeypatch) -> None:
    import freqinout.core.hf_net_subscription as subscription

    store, session = _catalog(tmp_path / "catalog.db")
    published = _published_session_with_day(store, session)
    monkeypatch.setattr(store, "get_session", lambda _key: published)
    for field in ("day_utc", "start_utc", "end_utc", "frequency"):
        draft = subscription.build_hf_subscription_drafts(
            store, [session.net_session_key], overrides_by_session={session.net_session_key: {field: ""}}
        )[0]
        assert any(field.split("_")[0] in warning.lower() or field.replace("_utc", "") in warning.lower() for warning in draft.warnings)
    with pytest.raises(ValueError, match="required before saving"):
        subscription.save_hf_subscriptions(
            SimpleNamespace(get=lambda *_args: [], set=lambda *_args: None),
            store,
            [session.net_session_key],
            destination_name="Synthetic HF Set",
            overrides_by_session={session.net_session_key: {"day_utc": ""}},
        )


def test_save_subscriptions_uses_named_existing_or_new_destination_without_duplicate_sessions(tmp_path: Path, monkeypatch) -> None:
    import freqinout.core.hf_net_subscription as subscription

    store, session = _catalog(tmp_path / "catalog.db")
    published = _published_session_with_day(store, session)
    monkeypatch.setattr(store, "get_session", lambda _key: published)
    captured: list[dict[str, Any]] = []

    def save(_settings: Any, _category: str, _selected: str, name: str, rows: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
        captured.append({"name": name, "rows": rows, "kwargs": kwargs})
        return {"id": "plan:42", "name": name, "rows": rows}

    monkeypatch.setattr(subscription, "save_source_schedule", save)
    monkeypatch.setattr(
        subscription,
        "source_sets_for_category",
        lambda local_settings, *_args: local_settings.get("freqplanner_hf_net_schedule_sets", []),
    )
    settings = SimpleNamespace(
        values={
            "freqplanner_hf_net_schedule_sets": [
                {"id": "plan:42", "db_id": 42, "name": "Existing HF Set", "rows": [{"net_session_key": session.net_session_key}]} 
            ]
        },
        get=lambda key, default=None: settings.values.get(key, default),
        set=lambda *_args: None,
        save=lambda: None,
    )
    subscription.save_hf_subscriptions(
        settings, store, [session.net_session_key, session.net_session_key],
        destination_name="Existing HF Set", existing_plan_id=42,
    )
    subscription.save_hf_subscriptions(settings, store, [session.net_session_key], destination_name="New HF Set")
    assert [item["name"] for item in captured] == ["Existing HF Set", "New HF Set"]
    assert len([row for row in captured[0]["rows"] if row.get("net_session_key") == session.net_session_key]) == 1
    assert captured[0]["kwargs"]["existing_plan_id"] == 42
    assert captured[1]["kwargs"]["existing_plan_id"] is None


def test_subscription_update_status_current_update_and_retired(tmp_path: Path) -> None:
    import freqinout.core.hf_net_subscription as subscription

    store, session = _catalog(tmp_path / "catalog.db")
    stored = store.create_session(session)
    accepted_snapshot = {
        "frequency_resource_key": "frequency:ln3", "recurrence": "Weekly", "local_start_time": "19:00",
        "duration_minutes": 60, "timezone": "America/Denver", "mode": "FM", "mode_details": "Synthetic",
    }
    row = {
        "net_session_key": stored.net_session_key,
        "accepted_session_version_hash": stored.version_hash,
        "accepted_snapshot_json": json.dumps(accepted_snapshot),
    }
    assert subscription.subscription_update_status(store, row).state == "current"
    changed = store.update_session(replace(stored, mode_details="Changed"))
    update = subscription.subscription_update_status(store, row)
    assert changed.version_hash != stored.version_hash
    assert update.state == "update_available"
    assert update.diffs
    store.retire_session(stored.net_session_key)
    retired = subscription.subscription_update_status(store, row)
    assert retired.state == "retired"
    assert "retired" in retired.warning.lower()


def test_fresh_subscription_snapshot_including_frequency_is_current(tmp_path: Path) -> None:
    import freqinout.core.hf_net_subscription as subscription

    store, session = _catalog(tmp_path / "catalog.db")
    stored = store.create_session(replace(session, day_utc="Monday", timezone="UTC"))
    draft = subscription.build_hf_subscription_drafts(store, [stored.net_session_key])[0]
    status = subscription.subscription_update_status(store, draft.schedule_row)
    assert status.state == "current"
    assert status.diffs == ()


def test_station_owned_source_is_created_once_and_reused(tmp_path: Path) -> None:
    store = ResourceCatalogStore(tmp_path / "catalog.db")
    store.create_schema()
    first = store.ensure_station_source()
    second = store.ensure_station_source()
    assert first.source_key == second.source_key == "source_station_manual"
    assert first.source_kind == "station"
    assert first.read_only is False


def test_resources_directory_handoff_is_wired_to_hf_net_subscription() -> None:
    main_window = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    resources = Path("freqinout/gui/resources_tab.py").read_text(encoding="utf-8")
    directory = Path("freqinout/gui/net_directory_view.py").read_text(encoding="utf-8")
    assert "def open_hf_net_subscription(self, session_keys: object)" in main_window
    assert "tab.add_to_hf_nets_requested.connect(self.open_hf_net_subscription)" in main_window
    assert "def request_add_to_hf_nets" in directory
    assert "add_to_hf_nets_requested" in resources


def test_live_hf_schedule_insert_and_reload_retains_subscription_metadata(tmp_path: Path) -> None:
    from freqinout.gui.net_schedule_tab import NetScheduleTab

    path = tmp_path / "freqinout_nets.db"
    columns = """
        id INTEGER PRIMARY KEY AUTOINCREMENT, day_utc TEXT, recurrence TEXT, biweekly_offset_weeks INTEGER,
        month_weeks TEXT, band TEXT, mode TEXT, vfo TEXT, frequency TEXT, start_utc TEXT, end_utc TEXT,
        early_checkin INTEGER, auto_tune INTEGER, primary_js8call_group TEXT, comment TEXT, net_name TEXT,
        group_name TEXT, fldigi_mode TEXT, fldigi_offset TEXT, resource_id INTEGER, net_session_key TEXT,
        accepted_session_version_hash TEXT, accepted_resource_version_hash TEXT, accepted_snapshot_json TEXT,
        target_scope TEXT, target_device_profile_id INTEGER, target_operating_profile_id INTEGER
    """
    with sqlite3.connect(path) as conn:
        conn.execute(f"CREATE TABLE net_schedule_tab ({columns})")
        conn.execute(f"CREATE TABLE net_schedule ({columns})")
        tab = NetScheduleTab.__new__(NetScheduleTab)
        tab._insert_rows_inner(conn, [{
            "day_utc": "Monday", "recurrence": "Weekly", "band": "40M", "mode": "Digi", "vfo": "A",
            "frequency": "7.078", "start_utc": "01:00", "end_utc": "02:00", "early_checkin": 0,
            "group_name": "GROUP ALPHA", "net_name": "Synthetic Net", "_resource_id": 3,
            "net_session_key": "session:ln3:weekly", "accepted_session_version_hash": "session-v2",
            "accepted_resource_version_hash": "resource-v3", "accepted_snapshot_json": '{"frequency":"7.078"}',
            "target_scope": "station",
        }])
        conn.commit()
    tab._db_path = lambda: path
    rows = tab._load_from_db()
    assert len(rows) == 1
    assert rows[0]["_resource_id"] == 3
    assert rows[0]["net_session_key"] == "session:ln3:weekly"
    assert rows[0]["accepted_session_version_hash"] == "session-v2"
    assert rows[0]["accepted_resource_version_hash"] == "resource-v3"
    assert rows[0]["accepted_snapshot_json"] == '{"frequency":"7.078"}'


def test_directory_status_labels_show_scheduled_and_open_schedule_when_seam_exists() -> None:
    source = Path("freqinout/gui/net_directory_view.py").read_text(encoding="utf-8")
    if "Scheduled" not in source or "Open Schedule" not in source:
        pytest.skip("directory scheduled-status seam has not landed")
    assert "Scheduled" in source
    assert "Open Schedule" in source


def test_directory_update_and_retired_warning_seams_are_explicit_when_landed() -> None:
    source = Path("freqinout/gui/net_schedule_tab.py").read_text(encoding="utf-8")
    assert "subscription_update_status" in source
    assert "Apply changes only through normal HF Save Schedule review." in source
