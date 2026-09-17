from __future__ import annotations

import sqlite3

import pytest

from freqinout.core.multi_radio_store import MultiRadioStore


def test_guided_save_transaction_rolls_back_all_composed_store_writes(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "settings.sqlite")
    with pytest.raises(RuntimeError, match="injected"):
        with store.guided_save_transaction():
            radio = store.save_device_profile(
                {"system_key": "guided-radio", "name": "Guided Radio"}
            )
            behavior = store.save_operating_profile(
                {"system_key": "guided-behavior", "name": "Guided Behavior"}
            )
            store.set_device_operating_profile(int(radio["id"]), int(behavior["id"]))
            store.save_radio_launch_bundle(
                int(radio["id"]),
                launch_enabled=True,
                items=[{"name": "SDR++", "instance_key": "guided:sdrpp", "startup": True}],
            )
            raise RuntimeError("injected")

    assert all(row.get("name") != "Guided Radio" for row in store.list_device_profiles())
    assert all(row.get("name") != "Guided Behavior" for row in store.list_operating_profiles())
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM radio_launch_bundles"
        ).fetchone()[0] == 0


def test_guided_save_transaction_requires_explicit_completion(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "settings.sqlite")
    with store.guided_save_transaction():
        store.save_device_profile({"system_key": "not-complete", "name": "Not Complete"})

    assert all(row.get("name") != "Not Complete" for row in store.list_device_profiles())


def test_guided_save_transaction_commits_radio_assignment_and_launch_bundle(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "settings.sqlite")
    with store.guided_save_transaction() as transaction:
        radio = store.save_device_profile(
            {"system_key": "complete-radio", "name": "Complete Radio"}
        )
        behavior = store.save_operating_profile(
            {"system_key": "complete-behavior", "name": "Complete Behavior"}
        )
        store.set_device_operating_profile(int(radio["id"]), int(behavior["id"]))
        store.save_radio_launch_bundle(
            int(radio["id"]),
            launch_enabled=True,
            items=[{"name": "SDR++", "instance_key": "complete:sdrpp", "startup": True}],
        )
        transaction.complete()

    saved = next(
        row for row in store.list_device_profiles() if row.get("name") == "Complete Radio"
    )
    assignment = store.get_effective_assignment_for_device(int(saved["id"]))
    assert assignment is not None
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute(
            "SELECT launch_enabled FROM radio_launch_bundles WHERE radio_profile_id=?",
            (int(saved["id"]),),
        ).fetchone()[0] == 1


def test_later_guided_failure_rolls_back_prior_radio_and_software_adoption(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "settings.sqlite")
    with pytest.raises(RuntimeError, match="later family failed"):
        with store.guided_save_transaction():
            radio = store.save_device_profile(
                {"system_key": "atomic-radio", "name": "Atomic Radio"}
            )
            store.adopt_software_instance(
                family_key="js8call",
                radio_profile_id=int(radio["id"]),
                application_values={
                    "system_key": "atomic-js8",
                    "name": "Atomic Radio",
                    "host": "127.0.0.1",
                    "port": 2448,
                    "profile_path": "/managed/atomic-js8.ini",
                    "application_data_root": "/managed/atomic-js8",
                },
                manifest_values={
                    "instance_key": "js8call:atomic-js8",
                    "application_system_key": "atomic_js8",
                    "evidence": {"reviewed": True},
                },
                launch_at_startup=True,
            )
            raise RuntimeError("later family failed")

    assert all(row.get("name") != "Atomic Radio" for row in store.list_device_profiles())
    assert store.list_js8_instances() == []
    assert store.list_software_instance_manifests() == []
