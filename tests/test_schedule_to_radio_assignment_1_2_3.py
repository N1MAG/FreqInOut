from __future__ import annotations

from pathlib import Path

from freqinout.core.multi_radio_store import DEFAULT_OPERATING_SYSTEM_KEY, MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager


def test_settings_source_exposes_radio_first_schedule_assignment_controls() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "Assigned Schedule" in source
    assert 'QPushButton("Assign Schedule...")' in source
    assert 'QPushButton("Restore Schedule")' in source
    assert "Schedule Profiles" in source
    assert "Radio Schedule Assignments" in source
    assert "Restore Default Schedule" in source
    assert "Assign a schedule profile if this radio should participate in Station Default schedule workflows." in source


def test_multi_radio_store_round_trips_schedule_assignment_for_radio(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    primary_radio = next(row for row in store.list_device_profiles() if int(row.get("runtime_primary", 0) or 0) == 1)
    created_profile = store.save_operating_profile(
        {
            "name": "Night Net Schedule",
            "enabled": 1,
            "scheduler_enabled": 1,
            "scheduler_mode": "full",
            "description": "Test schedule profile for radio assignment coverage.",
        }
    )

    assigned = store.set_device_operating_profile(
        int(primary_radio["id"]),
        int(created_profile["id"]),
        assignment_state="active",
        reason="Schedule-to-radio coverage",
    )

    assert int(assigned["device_profile_id"]) == int(primary_radio["id"])
    assert int(assigned["operating_profile_id"]) == int(created_profile["id"])
    assert str(assigned["assignment_state"]) == "active"

    effective = store.get_effective_assignment_for_device(int(primary_radio["id"]))
    assert effective is not None
    assert int(effective["operating_profile_id"]) == int(created_profile["id"])

    store.restore_default_operating_profile(int(primary_radio["id"]))

    default_profile = next(
        row for row in store.list_operating_profiles() if str(row.get("system_key", "") or "") == DEFAULT_OPERATING_SYSTEM_KEY
    )

    restored = store.get_effective_assignment_for_device(int(primary_radio["id"]))
    assert restored is not None
    assert int(restored["operating_profile_id"]) == int(default_profile["id"])
