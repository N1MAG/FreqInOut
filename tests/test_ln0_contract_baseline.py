"""LN-0 characterization tests for the pre-Local-Nets scheduler boundary."""

from __future__ import annotations

import json
from pathlib import Path

from freqinout.core.scheduler_engine import SchedulerEngine


FIXTURE = Path(__file__).parent / "fixtures" / "local_nets_tools_resources_baseline.json"


class _Settings:
    def __init__(self) -> None:
        self.payload = {
            "hf_schedule": [{"band": "40M", "frequency": "7.078"}],
            "net_schedule": [{"band": "80M", "frequency": "3.590"}],
            # A future Local Nets key must not be treated as commandable input.
            "local_net_schedules": [{"band": "2M", "frequency": "146.520"}],
        }

    def all(self):
        return dict(self.payload)

    def get(self, key, default=None):
        return self.payload.get(key, default)


def test_scheduler_load_schedules_ignores_local_net_settings_and_has_no_local_reader(
    monkeypatch, tmp_path: Path
) -> None:
    """Current scheduler input remains HF/net/SOP only until Local Nets exists."""

    engine = SchedulerEngine.__new__(SchedulerEngine)
    engine.settings = _Settings()
    engine._config_dir = lambda: tmp_path
    engine._db_mtime = lambda _path: 0.0
    engine._primary_schedule_target_context = lambda: (None, None)
    engine._load_assigned_frequency_plan_schedule_rows = lambda _profile_id: ([], [], False)
    engine._sop_layer_enabled = lambda: True
    engine._filter_rows_for_runtime_target = lambda rows, **_kwargs: rows
    # None means “DB/table unavailable”, exercising the existing settings
    # compatibility fallback without creating or mutating a database.
    monkeypatch.setattr(engine, "_load_daily_schedule_from_db", lambda: None)
    monkeypatch.setattr(engine, "_load_net_schedule_from_db", lambda: None)
    monkeypatch.setattr(engine, "_load_sop_schedule_layer_from_db", lambda: [])
    monkeypatch.setattr(engine, "_load_sop_net_conflict_policies_from_db", lambda: [])

    hf, net, sop, policies = engine._load_schedules(force=True)

    assert [(row["band"], row["frequency"]) for row in hf] == [("40M", "7.078")]
    assert [(row["band"], row["frequency"]) for row in net] == [("80M", "3.590")]
    assert sop == []
    assert policies == []
    assert all(row not in hf + net for row in engine.settings.payload["local_net_schedules"])


def test_ln0_structural_fixture_covers_migration_and_schedule_relationships() -> None:
    """The LN-0 fixture is anonymized and contains every required baseline shape."""

    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert set(fixture) >= {
        "empty_installation",
        "bundled_like_resources",
        "station_created_resource",
        "imported_readonly_resources",
        "duplicate_and_malformed_records",
        "hf_schedule_rows",
        "named_hf_net_source_schedules",
    }
    assert fixture["empty_installation"]["net_resources"] == []
    assert fixture["bundled_like_resources"]["readonly"] == 1
    assert fixture["station_created_resource"]["readonly"] == 0
    assert fixture["imported_readonly_resources"]["source_type"] == "imported"
    assert len(fixture["duplicate_and_malformed_records"]["duplicate_rows"]) == 2
    assert len(fixture["duplicate_and_malformed_records"]["malformed_rows"]) == 3
    assert fixture["hf_schedule_rows"]["linked"][0]["resource_id"] == 11
    assert fixture["hf_schedule_rows"]["unlinked"][0]["resource_id"] is None
    assert len(fixture["named_hf_net_source_schedules"]) == 2

    # The fixture must not smuggle a real station identity into migration tests.
    serialized = json.dumps(fixture).lower()
    assert "callsign" not in serialized
