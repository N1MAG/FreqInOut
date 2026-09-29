from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "build_public_runtime_export.py"


def _tool_module():
    spec = importlib.util.spec_from_file_location("build_public_runtime_export", TOOL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_public_runtime_export_is_allowlisted_and_operator_complete(tmp_path: Path) -> None:
    module = _tool_module()
    output = tmp_path / "public-runtime"

    copied, files = module.build_public_runtime_export(ROOT, output)

    relative = {path.relative_to(output) for path in files}
    assert copied == len(files)
    assert Path("README.md") in relative
    assert Path("FreqInOut.spec") in relative
    assert Path("installer.iss") in relative
    assert Path("packaging/build_linux_deb.py") in relative
    assert Path("packaging/build_macos_dmg.py") in relative
    assert Path("packaging/macos/entitlements.plist") in relative
    assert Path("packaging/pyinstaller_runtime_qt.py") in relative
    assert Path("packaging/verify_release_inputs.py") in relative
    assert Path("freqinout/main.py") in relative
    assert Path("docs/guide.html") in relative
    assert Path("config/net_resources/sitrepnets-winter.json") in relative
    assert Path("config/propagation/prop_climatology.db") in relative
    assert Path("config/resource_catalog/us_fcc_reference_v1.json") in relative
    assert Path("third_party/js8net/js8net-main/js8net.py") in relative
    assert Path("tests/test_public_runtime_export.py") not in relative
    assert Path("tools/build_public_runtime_export.py") not in relative
    assert Path("freqinout/core/config_lab_preset.py") not in relative
    assert not any("internal" in path.parts for path in relative)
    assert not any("__pycache__" in path.parts for path in relative)
    assert not any(path.suffix in {".pyc", ".pyo"} for path in relative)
    assert not any(path.name in {"example.py", "monitor.py", "send_message.py"} for path in relative)
    assert Path("config/propagation/README.md") not in relative
    assert Path("third_party/js8net/js8net-main/README.md") not in relative
    assert Path(".github/workflows/package-candidate.yml") not in relative
    assert Path(".github/workflows/public-release.yml") in relative

    public_readme = (output / "README.md").read_text(encoding="utf-8")
    assert public_readme.startswith("<p align=\"center\">")
    assert "Testing Preview" not in public_readme
    assert "FreqInOut-internal-testing" not in public_readme

    public_manifest = (output / "MANIFEST.in").read_text(encoding="utf-8")
    assert "packaging/pyinstaller_runtime_qt.py" in public_manifest
    assert "packaging/build_macos_dmg.py" in public_manifest


def test_public_runtime_export_refuses_existing_destination(tmp_path: Path) -> None:
    module = _tool_module()
    output = tmp_path / "already-exists"
    output.mkdir()

    with pytest.raises(FileExistsError, match="already exists"):
        module.build_public_runtime_export(ROOT, output)
