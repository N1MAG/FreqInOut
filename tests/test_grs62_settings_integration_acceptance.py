"""GRS-6.2 integration regressions for Settings-owned guided recipes."""

from __future__ import annotations

from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication

from freqinout.core.config_autodiscovery import build_lab_radio_proposals
from freqinout.core.config_js8_managed import build_js8call_managed_profile_plans
from freqinout.core.guided_launch_recipes import resolve_fast_light_managed_recipe, resolve_js8_managed_recipe
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


def test_blank_managed_root_fails_closed_for_js8_and_fast_light() -> None:
    js8 = resolve_js8_managed_recipe(_js8_draft(), managed_root="")
    fast = resolve_fast_light_managed_recipe(
        {
            "draft_instance_key": "fast-key",
            "radio_role": "tx_rx",
            "application_path": "/opt/flrig",
            "secondary_application_path": "/opt/fldigi",
            "port": 12346,
            "secondary_port": 7363,
        },
        managed_root="   ",
    )
    assert js8.status == fast.status == "unsupported"
    assert not js8.components and not fast.components
    assert "managed-instance root" in js8.recovery_action


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
    assert component.configuration_roots[0] == str(built.config_dir)
    assert component.data_roots[1] == str(built.save_dir)
    assert component.data_roots[2] == str(built.forms_dir)
    assert component.data_roots[0] == str(built.application_data_root)
    assert component.effective_command[2] == built.rig_name


def test_settings_standalone_native_plan_prefers_draft_instance_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings_tab, "get_config_dir", lambda: tmp_path)
    plan = settings_tab.SettingsTab._native_plan_for_software_instance_payload(
        _js8_draft(), {"name": "Radio A", "device_class": "tx_rx"}
    )
    assert plan is not None and plan.actions
    assert all(action.instance_name == "stable-key" for action in plan.actions)
    assert all("stable-key" in action.target or "stable-key" in str(action.details) for action in plan.actions)


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
