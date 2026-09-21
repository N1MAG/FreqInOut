from __future__ import annotations

from pathlib import Path

import pytest

from freqinout.core.launch_orchestrator import LaunchOrchestrator
from freqinout.core.managed_directory_contract import (
    managed_directories_from_component,
    materialize_managed_directories,
)


def test_js8_compatibility_projection_creates_ini_parent_not_ini_file(tmp_path: Path) -> None:
    ini = tmp_path / ".config" / "JS8Call - FT-710.ini"
    data = tmp_path / ".local" / "share" / "JS8Call - FT-710"
    component = {
        "component_key": "js8call",
        "configuration_roots": [str(ini)],
        "data_roots": [str(data), str(data / "save"), str(data / "forms")],
    }

    targets = managed_directories_from_component(component)
    materialize_managed_directories(targets)

    assert ini.parent.is_dir()
    assert not ini.exists()
    assert data.is_dir()
    assert (data / "save").is_dir()
    assert (data / "forms").is_dir()


def test_materialization_preserves_existing_directory_and_rejects_file_target(tmp_path: Path) -> None:
    existing = tmp_path / "existing"
    existing.mkdir()
    sentinel = existing / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    file_target = tmp_path / "not-a-directory"
    file_target.write_text("operator data", encoding="utf-8")

    with pytest.raises(FileExistsError):
        materialize_managed_directories((existing, file_target))

    assert sentinel.read_text(encoding="utf-8") == "keep"
    assert file_target.read_text(encoding="utf-8") == "operator data"


def test_launch_preflight_repairs_saved_managed_flrig_working_directory(tmp_path: Path) -> None:
    flrig_root = tmp_path / ".flrig" / "instances" / "FT-710"
    item = {
        "name": "FLRig",
        "instance_identity": "fast-light:ft-710:flrig",
        "readiness_policy": {
            "configuration_roots": [str(flrig_root)],
            "working_directory": str(flrig_root),
        },
    }

    ready = LaunchOrchestrator._materialize_item_managed_directories(item)

    assert ready == (flrig_root,)
    assert flrig_root.is_dir()


def test_launch_preflight_never_creates_varac_or_shared_utility_paths(tmp_path: Path) -> None:
    target = tmp_path / "operator" / "VarAC"
    item = {
        "name": "VarAC",
        "instance_identity": "varac:ft-710",
        "readiness_policy": {
            "configuration_roots": [str(target / "VarAC.ini")],
            "working_directory": str(target),
        },
    }

    assert LaunchOrchestrator._materialize_item_managed_directories(item) == ()
    assert not target.exists()
