from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _runtime_version() -> str:
    namespace: dict[str, object] = {}
    exec((ROOT / "freqinout" / "version.py").read_text(encoding="utf-8"), namespace)
    return str(namespace["__version__"])


def _module(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_release_metadata_agrees_and_required_build_inputs_exist() -> None:
    verifier = _module("fio_verify_release_inputs", "packaging/verify_release_inputs.py")
    version = _runtime_version()

    assert verifier.validated_version(ROOT) == version
    assert verifier.validated_version(ROOT, expected_tag=f"v{version}") == version
    with pytest.raises(verifier.ReleaseInputError, match="Tag/version mismatch"):
        verifier.validated_version(ROOT, expected_tag="v9.9.9")


@pytest.mark.parametrize(
    "machine,expected",
    (("x86_64", "amd64"), ("AMD64", "amd64"), ("aarch64", "arm64"), ("arm64", "arm64")),
)
def test_debian_architecture_mapping(machine: str, expected: str) -> None:
    builder = _module("fio_build_linux_deb_arch", "packaging/build_linux_deb.py")
    assert builder.deb_architecture(machine) == expected


@pytest.mark.parametrize(
    "machine,expected",
    (("x86_64", "x86_64"), ("AMD64", "x86_64"), ("aarch64", "arm64"), ("arm64", "arm64")),
)
def test_macos_architecture_mapping(machine: str, expected: str) -> None:
    builder = _module("fio_build_macos_dmg_arch", "packaging/build_macos_dmg.py")
    assert builder.macos_architecture(machine) == expected


def test_debian_package_staging_has_launcher_metadata_and_preserves_external_profile(
    tmp_path: Path,
) -> None:
    builder = _module("fio_build_linux_deb_stage", "packaging/build_linux_deb.py")
    source = tmp_path / "source"
    shutil.copytree(ROOT / "packaging", source / "packaging")
    shutil.copytree(ROOT / "assets", source / "assets")
    (source / "freqinout").mkdir()
    (source / "docs").mkdir()
    shutil.copy2(ROOT / "freqinout" / "version.py", source / "freqinout" / "version.py")
    shutil.copy2(ROOT / "pyproject.toml", source / "pyproject.toml")
    shutil.copy2(ROOT / "installer.iss", source / "installer.iss")
    shutil.copy2(ROOT / "docs" / "guide.html", source / "docs" / "guide.html")
    shutil.copy2(ROOT / "CHANGELOG.md", source / "CHANGELOG.md")
    shutil.copy2(ROOT / "FreqInOut.spec", source / "FreqInOut.spec")
    shutil.copy2(ROOT / "requirements.txt", source / "requirements.txt")
    frozen = source / "dist" / "FreqInOut" / "FreqInOut"
    frozen.parent.mkdir(parents=True)
    frozen.write_bytes(b"frozen")

    package_root = tmp_path / "package-root"
    version, architecture = builder.assemble_package_root(
        package_root,
        source_root=source,
        architecture="amd64",
    )

    assert version == _runtime_version()
    assert architecture == "amd64"
    launcher = package_root / "usr" / "bin" / "freqinout"
    assert launcher.stat().st_mode & 0o111
    assert "/opt/freqinout/FreqInOut/FreqInOut" in launcher.read_text(encoding="utf-8")
    control = (package_root / "DEBIAN" / "control").read_text(encoding="utf-8")
    assert "Package: freqinout" in control
    assert f"Version: {_runtime_version()}" in control
    assert "Architecture: amd64" in control
    assert not any(".freqinout" in str(path) for path in package_root.rglob("*"))


def test_candidate_workflow_builds_installs_smokes_and_never_publishes() -> None:
    source = (ROOT / ".github" / "workflows" / "package-candidate.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in source
    assert "windows-2022" in source
    assert "ubuntu-22.04" in source
    assert "--smoke-test" in source
    assert "Install and smoke-test completed installer" in source
    assert "Install and smoke-test completed Debian package" in source
    assert "retain-after-uninstall.txt" in source
    assert source.count("retention-days: 3") == 2
    assert "retention-days: 14" not in source
    assert "gh release" not in source
    assert "contents: write" not in source
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith("uses: actions/"):
            reference = stripped.split("@", 1)[1].split()[0]
            assert len(reference) == 40
            int(reference, 16)


def test_pyinstaller_spec_uses_runtime_isolation_and_platform_icon() -> None:
    source = (ROOT / "FreqInOut.spec").read_text(encoding="utf-8")
    assert "packaging/pyinstaller_runtime_qt.py" in source
    assert "upx=False" in source
    assert "sys.platform == 'win32'" in source
    assert "FreqInOut.app" in source
    assert "org.n1mag.freqinout" in source


def test_public_release_workflow_has_separate_candidate_and_production_contracts() -> None:
    source = (ROOT / ".github" / "workflows" / "public-release.yml").read_text(
        encoding="utf-8"
    )

    assert 'branches:\n      - "release/public-*-candidate"' in source
    assert 'tags:\n      - "v*"' in source
    assert source.count("environment: production-release") >= 2
    assert "WINDOWS_SIGNING_CERTIFICATE_BASE64" in source
    assert "MACOS_CERTIFICATE_P12_BASE64" in source
    assert "notarytool submit" in source
    assert "actions/attest@" in source
    assert "SHA256SUMS.txt" in source
    assert 'FreqInOut-${{ needs.verify.outputs.version }}-windows-x86_64-setup.exe' in source
    assert 'FreqInOut-${{ needs.verify.outputs.version }}-linux-amd64.deb' in source
    assert 'FreqInOut-${{ needs.verify.outputs.version }}-macos-${{ matrix.arch }}.dmg' in source
    assert "refusing to overwrite it" in source
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith("uses: actions/"):
            reference = stripped.split("@", 1)[1].split()[0]
            assert len(reference) == 40
            int(reference, 16)
