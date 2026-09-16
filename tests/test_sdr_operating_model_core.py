"""Core contracts for assignable SDR models and observer-owned JS8 ingest."""

from __future__ import annotations

import json

import pytest

from freqinout.core.launch_bundle_store import LaunchBundleStore
from freqinout.core.multi_radio_store import (
    CURRENT_MULTI_RIG_MIGRATION_VERSION,
    DEFAULT_OPERATING_SYSTEM_KEY,
    DEFAULT_RECEIVE_ONLY_OPERATING_SYSTEM_KEY,
    MULTI_RIG_MIGRATION_VERSION_KEY,
    MultiRadioStore,
    ensure_multi_rig_migration,
    settings_db_path,
)
from freqinout.core.js8_send_service import js8_profile_allows_transmit
from freqinout.core.receiver_software_stack import build_receiver_launch_items
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_launch_planner import StationLaunchPlanner


def _observer(store: MultiRadioStore, *, name: str = "RTL-SDR") -> dict[str, object]:
    return store.save_device_profile(
        {
            "name": name,
            "device_class": "observer",
            "control_backend": "manual",
            "runtime_active": 0,
            "runtime_primary": 0,
            "sdr_application": "SDR++",
        }
    )


def _manifest(*, key: str, port: int, root: str) -> dict[str, object]:
    return {
        "instance_key": key,
        "management_mode": "fio_managed",
        "provenance": "guided_receiver_setup",
        "executable_path": "/opt/js8call/js8call",
        "data_root": root,
        "host": "127.0.0.1",
        "ports": [{"name": "JS8 API", "protocol": "tcp", "host": "127.0.0.1", "port": port}],
        "resource_claims": [{"kind": "message_storage", "value": root}],
        "verification_state": "configured",
    }


def test_observer_creation_seeds_assignable_safe_operating_model(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    observer = _observer(store)
    models = [
        row
        for row in store.list_operating_profiles()
        if row.get("system_key") == DEFAULT_RECEIVE_ONLY_OPERATING_SYSTEM_KEY
    ]

    assert len(models) == 1
    model = models[0]
    assert int(model["enabled"]) == 1
    assert int(model["receive_only"]) == 1
    assert int(model["scheduler_enabled"]) == 0
    assert int(model["use_net_control_tabs"]) == 0
    assert int(model["allow_profile_swap"]) == 0
    assert int(model["use_launch_control"]) == 1

    assignment = store.set_device_operating_profile(int(observer["id"]), int(model["id"]))
    assert int(assignment["operating_profile_id"]) == int(model["id"])


def test_blank_station_can_prepare_builtin_models_without_creating_a_radio(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    models = store.ensure_builtin_operating_profiles()

    assert {row["system_key"] for row in models} == {
        DEFAULT_OPERATING_SYSTEM_KEY,
        DEFAULT_RECEIVE_ONLY_OPERATING_SYSTEM_KEY,
    }
    assert store.list_device_profiles() == []


def test_first_only_observer_can_activate_after_assignment_but_never_becomes_primary(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    observer = _observer(store)
    model = store.ensure_receive_only_operating_profile()
    store.set_device_operating_profile(int(observer["id"]), int(model["id"]))

    active = store.set_device_profile_runtime_active(int(observer["id"]), True)

    assert int(active["runtime_active"]) == 1
    assert int(active["runtime_primary"]) == 0
    assert store.get_runtime_primary_device_profile() is None


def test_receive_only_model_is_idempotent_and_safety_fields_are_immutable(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())

    first = store.ensure_receive_only_operating_profile()
    second = store.ensure_receive_only_operating_profile()

    assert int(first["id"]) == int(second["id"])
    with pytest.raises(ValueError, match="cannot own the scheduler"):
        store.save_operating_profile({"id": int(first["id"]), "scheduler_enabled": 1})
    with pytest.raises(ValueError, match="must remain receive-only"):
        store.save_operating_profile({"id": int(first["id"]), "receive_only": 0})
    with pytest.raises(ValueError, match="Cannot delete"):
        store.delete_operating_profile(int(first["id"]))


def test_restore_default_model_resolves_observer_to_receive_only_not_transmit_default(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    observer = _observer(store)
    alternate = store.save_operating_profile(
        {
            "name": "Temporary RX Watch",
            "receive_only": 1,
            "scheduler_enabled": 0,
            "use_net_control_tabs": 0,
        }
    )
    store.set_device_operating_profile(int(observer["id"]), int(alternate["id"]))

    restored = store.restore_default_operating_profile(int(observer["id"]))
    model = store.get_operating_profile(int(restored["operating_profile_id"]))

    assert model is not None
    assert model["system_key"] == DEFAULT_RECEIVE_ONLY_OPERATING_SYSTEM_KEY
    assert int(model["receive_only"]) == 1


def test_v3_migration_adds_model_for_guided_sdr_assignment(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = SettingsManager()
    store = MultiRadioStore(settings_db_path())
    _observer(store)

    with settings._conn:  # type: ignore[attr-defined]
        settings._conn.execute(  # type: ignore[attr-defined]
            "DELETE FROM operating_profiles WHERE system_key=?",
            (DEFAULT_RECEIVE_ONLY_OPERATING_SYSTEM_KEY,),
        )
        settings._conn.execute(  # type: ignore[attr-defined]
            "UPDATE kv SET value=? WHERE key=?",
            (json.dumps(2), MULTI_RIG_MIGRATION_VERSION_KEY),
        )

    result = ensure_multi_rig_migration(settings._conn, settings.all())  # type: ignore[arg-type]

    assert result.applied is True
    assert result.to_version == CURRENT_MULTI_RIG_MIGRATION_VERSION
    assert any(
        row.get("system_key") == DEFAULT_RECEIVE_ONLY_OPERATING_SYSTEM_KEY
        for row in store.list_operating_profiles()
    )
    assert [row["name"] for row in store.list_device_profiles()] == ["RTL-SDR"]


def test_observer_adopts_distinct_js8_for_ingest_and_launch_without_tx_authority(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    observer = _observer(store)

    result = store.adopt_observer_js8_instance(
        radio_profile_id=int(observer["id"]),
        application_values={
            "system_key": "rtl_sdr_js8",
            "name": "RTL-SDR JS8Call",
            "host": "127.0.0.1",
            "port": 2448,
            "install_path": "/opt/js8call/js8call",
            "application_data_root": "/tmp/rtl-sdr-js8",
        },
        manifest_values=_manifest(key="js8call:rtl-sdr", port=2448, root="/tmp/rtl-sdr-js8"),
        launch_at_startup=True,
    )

    radio = result["radio"]
    assert int(radio["use_js8call"]) == 1
    assert int(radio["js8_instance_id"]) == int(result["application"]["id"])
    assert radio["control_backend"] == "manual"
    assert js8_profile_allows_transmit(radio) is False
    assert result["manifest"]["evidence"]["receive_only_ingest"] is True
    assert result["manifest"]["evidence"]["transmit_authority"] is False

    bundle = LaunchBundleStore(settings_db_path()).get_bundle(int(observer["id"]))
    assert bundle["items"][0]["execution_scope"] == "receive_only"
    plan = StationLaunchPlanner().plan_startup([{**radio, "runtime_active": 1}], {int(observer["id"]): bundle})
    assert [item.name for item in plan.instances] == ["JS8Call"]
    assert plan.instances[0].execution_scope == "receive_only"


def test_observer_js8_adoption_preserves_receiver_launch_and_is_idempotent(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    observer = _observer(store)
    bundle_store = LaunchBundleStore(settings_db_path())
    bundle_store.save_bundle(
        int(observer["id"]),
        True,
        build_receiver_launch_items(
            observer,
            [{"name": "SDR++", "launch_path_override": "/opt/sdrpp/sdrpp", "startup": True}],
        ),
    )
    application = {
        "system_key": "rtl_sdr_js8",
        "name": "RTL-SDR JS8Call",
        "host": "127.0.0.1",
        "port": 2448,
        "install_path": "/opt/js8call/js8call",
        "application_data_root": "/tmp/rtl-sdr-js8",
    }
    manifest = _manifest(key="js8call:rtl-sdr", port=2448, root="/tmp/rtl-sdr-js8")

    first = store.adopt_observer_js8_instance(
        radio_profile_id=int(observer["id"]),
        application_values=application,
        manifest_values=manifest,
        launch_at_startup=True,
    )
    second = store.adopt_observer_js8_instance(
        radio_profile_id=int(observer["id"]),
        application_values={**application, "id": int(first["application"]["id"])},
        manifest_values=manifest,
        launch_at_startup=True,
    )

    assert int(second["application"]["id"]) == int(first["application"]["id"])
    bundle = bundle_store.get_bundle(int(observer["id"]))
    assert bundle["launch_enabled"] is True
    assert [item["name"] for item in bundle["items"]] == ["SDR++", "JS8Call"]
    assert len({item["instance_key"] for item in bundle["items"]}) == 2
    assert all(item["execution_scope"] == "receive_only" for item in bundle["items"])


def test_observer_js8_adoption_rejects_shared_endpoint(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    first = _observer(store, name="RTL-SDR A")
    second = _observer(store, name="RTL-SDR B")

    store.adopt_observer_js8_instance(
        radio_profile_id=int(first["id"]),
        application_values={"system_key": "rx_a", "name": "RX A JS8", "host": "127.0.0.1", "port": 2448},
        manifest_values=_manifest(key="js8call:rx-a", port=2448, root="/tmp/rx-a"),
    )
    with pytest.raises(ValueError, match="already used"):
        store.adopt_observer_js8_instance(
            radio_profile_id=int(second["id"]),
            application_values={"system_key": "rx_b", "name": "RX B JS8", "host": "127.0.0.1", "port": 2448},
            manifest_values=_manifest(key="js8call:rx-b", port=2448, root="/tmp/rx-b"),
        )


def test_receive_only_js8_adoption_rechecks_observer_class_in_transaction(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    transceiver = store.save_device_profile(
        {
            "name": "HF Radio",
            "device_class": "tx_rx",
            "control_backend": "manual",
            "runtime_active": 0,
            "runtime_primary": 0,
        }
    )

    with pytest.raises(ValueError, match="observer / SDR"):
        store.adopt_observer_js8_instance(
            radio_profile_id=int(transceiver["id"]),
            application_values={"system_key": "hf_js8", "name": "HF JS8", "host": "127.0.0.1", "port": 2449},
            manifest_values=_manifest(key="js8call:hf", port=2449, root="/tmp/hf-js8"),
        )

    assert store.list_js8_instances() == []
