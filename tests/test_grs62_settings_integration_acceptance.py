"""GRS-6.2 integration regressions for Settings-owned guided recipes."""

from __future__ import annotations

from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication

from freqinout.core.config_autodiscovery import build_lab_radio_proposals
from freqinout.core.config_js8_managed import build_js8call_managed_profile_plans
from freqinout.core.guided_launch_recipes import (
    recipe_draft_updates,
    resolve_fast_light_managed_recipe,
    resolve_js8_managed_recipe,
)
from freqinout.core.software_administration_model import build_software_administration_snapshot
from freqinout.gui import settings_tab
from freqinout.gui.software_administration_workspace import SoftwareAdministrationWorkspace


def _js8_draft(**updates: object) -> dict[str, object]:
    draft: dict[str, object] = {
        "family_key": "js8call",
        "mode": "managed",
        "ownership": "fio-managed",
        "draft_instance_key": "stable-key",
        "instance_name": "Display Name May Change",
        "owner_label": "Radio A",
        "radio_role": "tx_rx",
        "variant": "js8call_2_2",
        "version": "2.2.0",
        "application_path": "/opt/js8call",
        "host": "127.0.0.1",
        "port": 2443,
        "udp_port": 2243,
        "writer_platform": "linux",
    }
    draft.update(updates)
    return draft


def test_js8_and_fast_light_use_native_roots_without_a_private_managed_root(tmp_path: Path) -> None:
    js8 = resolve_js8_managed_recipe(_js8_draft(), managed_root="")
    fast = resolve_fast_light_managed_recipe(
        {
            "draft_instance_key": "fast-key",
            "application_system_key": "fast-light-instance-native123456",
            "owner_label": "Radio A",
            "radio_role": "tx_rx",
            "application_path": "/opt/flrig",
            "secondary_application_path": "/opt/fldigi",
            "port": 12346,
            "secondary_port": 7363,
        },
        managed_root="   ",
        platform="linux",
        storage_home=tmp_path / "home",
    )
    assert js8.qualified and js8.components
    assert "stable-key" not in js8.components[0].configuration_roots[0]
    assert fast.qualified and fast.components
    assert all("managed-instances" not in root for item in fast.components for root in item.configuration_roots)
    assert fast.components[0].configuration_roots[0].startswith(str(tmp_path / "home" / ".flrig"))


@pytest.mark.parametrize(
    ("variant", "version"),
    (("js8call_2_2", "2.2.0"), ("js8call_improved_3_0_3", "3.0.3"), ("js8call_subspace_4_1", "4.1.0.478")),
)
def test_recipe_roots_align_with_platform_profile_builder_and_canonical_versions(
    tmp_path: Path, variant: str, version: str
) -> None:
    resolution = resolve_js8_managed_recipe(
        _js8_draft(variant=variant, version=version),
        managed_root=str(tmp_path / "managed-instances"),
        platform="linux",
        storage_home=tmp_path / "home",
    )
    assert resolution.qualified
    proposal = build_lab_radio_proposals(radio_count=1, enabled_apps=("js8call",), busy_checker=lambda *_: False)[0]
    built = build_js8call_managed_profile_plans(
        (proposal.__class__(proposal.name, "stable-key", proposal.index, proposal.enabled_apps, proposal.ports),),
        config_root=tmp_path,
        platform="linux",
        storage_home=tmp_path / "home",
    )[0]
    component = resolution.components[0]
    assert component.configuration_roots[0] == str(built.settings_path)
    assert component.data_roots[1] == str(built.save_dir)
    assert component.data_roots[2] == str(built.forms_dir)
    assert component.data_roots[0] == str(built.application_data_root)
    assert component.effective_command[2] == built.rig_name
    assert component.managed_directories == (
        str(built.config_dir),
        str(built.application_data_root),
        str(built.save_dir),
        str(built.forms_dir),
    )


def test_settings_native_plan_keeps_draft_key_internal(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings_tab, "get_config_dir", lambda: tmp_path)
    plan = settings_tab.SettingsTab._native_plan_for_software_instance_payload(
        _js8_draft(), {"name": "Radio A", "device_class": "tx_rx"}
    )
    assert plan is not None and plan.actions
    assert all(action.instance_name == "stable-key" for action in plan.actions)
    assert all("stable-key" not in action.target for action in plan.actions)
    assert all("stable-key" not in str(action.details) for action in plan.actions)
    writer_actions = [action for action in plan.actions if action.action_type == "update_js8_multisettings"]
    assert len(writer_actions) == 1
    assert "JS8Call - Radio-A" in str(writer_actions[0].details)


def test_settings_native_plan_materializes_fast_light_from_the_saved_recipe(tmp_path: Path) -> None:
    draft: dict[str, object] = {
        "family_key": "fast_light",
        "mode": "managed",
        "ownership": "fio-managed",
        "draft_instance_key": "internal-fast-key",
        "instance_name": "FT-710 Fast Light",
        "owner_label": "FT-710",
        "radio_role": "tx_rx",
        "application_path": "/opt/flrig",
        "secondary_application_path": "/opt/fldigi",
        "port": 12346,
        "secondary_port": 7363,
    }
    resolution = resolve_fast_light_managed_recipe(
        draft,
        managed_root=str(tmp_path / "private"),
        platform="linux",
        storage_home=tmp_path / "home",
    )
    draft.update(recipe_draft_updates(resolution))

    plan = settings_tab.SettingsTab._native_plan_for_software_instance_payload(
        draft,
        {"name": "FT-710", "device_class": "tx_rx"},
    )

    assert plan is not None
    targets = {action.target for action in plan.actions if action.action_type == "create_directory"}
    assert str(tmp_path / "home" / ".flrig" / "instances" / "FT-710") in targets
    assert str(tmp_path / "home" / ".fldigi" / "instances" / "FT-710") in targets
    assert not any(str(tmp_path / "private") in target for target in targets)


def test_software_workspace_routes_its_settings_managed_root_into_assistant() -> None:
    _app = QApplication.instance() or QApplication([])
    workspace = SoftwareAdministrationWorkspace()
    try:
        radios = ({"id": 1, "name": "Radio A", "device_class": "tx_rx"},)
        workspace.set_instance_context(
            radios=radios,
            inventory_by_family={"js8call": ()},
            managed_root="/fio/config/managed-instances",
        )
        workspace.set_snapshot(
            build_software_administration_snapshot(radios, js8_instances=())
        )

        assert workspace.begin_instance_setup("js8call", 1)
        assert workspace._instance_assistant is not None
        assert workspace._instance_assistant._managed_root == "/fio/config/managed-instances"
    finally:
        workspace.deleteLater()
