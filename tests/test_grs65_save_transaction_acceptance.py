"""GRS-6.5 bounded save-transaction acceptance contracts."""

from __future__ import annotations

import inspect
from pathlib import Path
from types import SimpleNamespace

from freqinout.core.guided_app_config_plan import build_guided_external_app_config_plan
from freqinout.core.guided_instance_inventory import build_guided_instance_inventory
from freqinout.core.config_autodiscovery import RadioInstanceProposal
from freqinout.gui.settings_tab import SettingsTab


def test_cancelled_review_plan_is_pure_and_does_not_create_managed_state(tmp_path: Path) -> None:
    proposal = RadioInstanceProposal("Radio", "draft-a", 0, ("js8call",), ())
    plan = build_guided_external_app_config_plan(
        (proposal,), config_root=tmp_path / "managed", allow_external_writes=False
    )
    assert plan.actions == ()
    assert not (tmp_path / "managed").exists()


def test_final_save_rechecks_reviewed_inventory_generation_before_mutation() -> None:
    source = inspect.getsource(SettingsTab._on_software_instance_add_requested)
    assert "inventory_fingerprint" in source
    assert "inventory changed" in source.casefold() or "refresh discovery" in source.casefold()


def test_native_failure_path_rolls_back_before_reporting_unsaved_instance() -> None:
    source = inspect.getsource(SettingsTab._on_software_instance_add_requested)
    assert "_rollback_guided_native_config" in source
    assert "the instance was not saved" in source.casefold()


def test_save_route_does_not_adopt_twice_while_native_apply_is_pending() -> None:
    source = inspect.getsource(SettingsTab._on_software_instance_add_requested)
    native_start = source.index("if native_result is None:")
    native_path = source[native_start:source.index("annotated =", native_start)]
    assert "self._start_guided_native_config_job" in native_path
    assert "return" in native_path
    launch_at = native_path.index("self._start_guided_native_config_job")
    assert "return" in native_path[launch_at:]


def test_inventory_snapshot_fingerprint_is_stable_for_final_save_context() -> None:
    snapshot = build_guided_instance_inventory(
        {"js8call": ({"instance_key": "saved", "id": 1},)}, generation=4
    )
    assert snapshot.fingerprint == build_guided_instance_inventory(
        {"js8call": ({"instance_key": "saved", "id": 1},)}, generation=4
    ).fingerprint


def test_software_administration_revalidation_detects_changed_durable_inventory() -> None:
    rows = [{"id": 1, "system_key": "js8-a", "name": "JS8 A", "port": 2442}]
    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = SimpleNamespace(
        list_software_instance_manifests=lambda: [],
        list_js8_instances=lambda: list(rows),
        list_fast_light_configs=lambda: [],
        list_varac_nodes=lambda: [],
    )
    reviewed = build_guided_instance_inventory({"js8call": tuple(rows)}, generation=3)
    payload = {
        "family_key": "js8call",
        "inventory_generation": 3,
        "inventory_fingerprint": reviewed.fingerprint,
    }
    assert SettingsTab._software_instance_review_is_current(tab, payload)

    rows.append({"id": 2, "system_key": "js8-b", "name": "JS8 B", "port": 2443})
    assert not SettingsTab._software_instance_review_is_current(tab, payload)


def test_stale_add_radio_review_reopens_the_same_unsaved_draft_at_recovery_step() -> None:
    tab = SettingsTab.__new__(SettingsTab)
    reviewed = {
        "name": "Retained Radio",
        "guided_inventory_generation": 2,
        "guided_inventory_fingerprint": "stale",
        "guided_software_instance_drafts": {"js8call": {"draft_instance_key": "draft-a"}},
    }
    opens: list[dict[str, object]] = []
    completed: list[dict[str, object]] = []

    def open_dialog(**kwargs):
        opens.append(dict(kwargs))
        return dict(reviewed)

    current = iter((False, True))
    tab._open_device_profile_dialog = open_dialog
    tab._guided_radio_review_is_current = lambda _payload: next(current)
    tab._present_guided_stale_review_recovery = lambda *_args, **_kwargs: "software"
    tab._complete_add_device_profile = lambda payload, **_kwargs: completed.append(dict(payload))

    SettingsTab._add_device_profile(tab)

    assert opens[0]["retry_draft"] is None
    assert opens[1]["initial_step"] == "software"
    assert opens[1]["retry_draft"]["guided_software_instance_drafts"] == reviewed[
        "guided_software_instance_drafts"
    ]
    assert completed and completed[0]["name"] == "Retained Radio"
