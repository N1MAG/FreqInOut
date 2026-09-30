from __future__ import annotations

import importlib.util
import hashlib
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
    assert 'FreqInOut-${{ needs.verify.outputs.version }}-windows-x86_64-setup-unsigned.exe' in source
    assert 'FreqInOut-${{ needs.verify.outputs.version }}-linux-amd64.deb' in source
    assert 'FreqInOut-${{ needs.verify.outputs.version }}-macos-${{ matrix.arch }}-unsigned.dmg' in source
    assert "FIO_ENABLE_PFX_SIGNING" in source
    assert "FIO_ENABLE_APPLE_SIGNING" in source
    assert "refusing to overwrite it" in source
    assert "packaging/build_release_notes.py" in source
    assert '--notes-file "$RUNNER_TEMP/release-notes.md"' in source
    assert "--generate-notes" not in source
    assert source.count("[System.IO.File]::WriteAllText(") == 2
    assert 'Set-Content "$asset.sha256"' not in source
    assert "packaging/prepare_release_assets.py" in source
    assert "--source-dir release-assets" in source
    assert "--output-dir release-assets-flat" in source
    assert "release-assets-flat/SHA256SUMS.txt" in source
    assert source.count('FREQINOUT_HARD_EXIT: "1"') == 4
    assert source.count("WaitForExit(120000)") == 4
    assert source.count("smoke test timed out after 120 seconds") == 4
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith("uses: actions/"):
            reference = stripped.split("@", 1)[1].split()[0]
            assert len(reference) == 40
            int(reference, 16)


def test_release_notes_put_downloads_changelog_and_manual_source_first() -> None:
    builder = _module("fio_build_release_notes", "packaging/build_release_notes.py")
    notes = builder.build_release_notes(
        version="2.0.4",
        repository="N1MAG/FreqInOut",
        changelog=(ROOT / "CHANGELOG.md").read_text(encoding="utf-8"),
    )

    assert "# FreqInOut 2.0.4" in notes
    assert "## Downloads" in notes
    assert "## What changed" in notes
    assert "Source code (zip)" in notes
    assert "tag `v2.0.4`" in notes
    assert "FreqInOut-2.0.4-windows-x86_64-setup-unsigned.exe" in notes
    assert "FreqInOut-2.0.4-linux-amd64.deb" in notes
    assert "FreqInOut-2.0.4-macos-arm64-unsigned.dmg" in notes
    assert "Package signing notice" in notes
    assert "Unknown publisher" in notes
    assert "Open Anyway" in notes
    assert "## [2.0.3]" not in notes


def test_release_notes_switch_to_signed_filenames_without_unsigned_notice() -> None:
    builder = _module("fio_build_release_notes_signed", "packaging/build_release_notes.py")
    notes = builder.build_release_notes(
        version="2.0.4",
        repository="N1MAG/FreqInOut",
        changelog=(ROOT / "CHANGELOG.md").read_text(encoding="utf-8"),
        windows_signed=True,
        macos_signed=True,
    )

    assert "FreqInOut-2.0.4-windows-x86_64-setup.exe" in notes
    assert "FreqInOut-2.0.4-macos-x86_64.dmg" in notes
    assert "FreqInOut-2.0.4-windows-x86_64-setup-unsigned.exe" not in notes
    assert "FreqInOut-2.0.4-macos-x86_64-unsigned.dmg" not in notes
    assert "Package signing notice" not in notes


def test_release_asset_staging_finds_nested_packages_and_normalizes_manifests(
    tmp_path: Path,
) -> None:
    builder = _module("fio_prepare_release_assets", "packaging/prepare_release_assets.py")
    source = tmp_path / "downloads"
    output = tmp_path / "flat"
    names = builder.expected_asset_names("2.0.4")
    for index, name in enumerate(names):
        parent = source if name.endswith(".exe") else source / "Output" / f"platform-{index}"
        parent.mkdir(parents=True, exist_ok=True)
        payload = f"package-{index}".encode("ascii")
        asset = parent / name
        asset.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        line_ending = "\r\n" if name.endswith(".exe") else "\n"
        (parent / f"{name}.sha256").write_bytes(
            f"{digest}  {name}{line_ending}".encode("ascii")
        )

    staged = builder.prepare_release_assets(source, output, version="2.0.4")

    assert len(staged) == 9
    assert {path.name for path in output.iterdir()} == {
        *names,
        *(f"{name}.sha256" for name in names),
        "SHA256SUMS.txt",
    }
    assert all(b"\r" not in (output / f"{name}.sha256").read_bytes() for name in names)
    combined = (output / "SHA256SUMS.txt").read_text(encoding="ascii").splitlines()
    assert len(combined) == 4
    assert [line.split("  ", 1)[1] for line in combined] == list(names)


def test_release_asset_staging_rejects_duplicate_expected_files(tmp_path: Path) -> None:
    builder = _module("fio_prepare_release_assets_duplicate", "packaging/prepare_release_assets.py")
    source = tmp_path / "downloads"
    name = builder.expected_asset_names("2.0.4")[0]
    for parent_name in ("one", "two"):
        parent = source / parent_name
        parent.mkdir(parents=True)
        (parent / name).write_bytes(b"duplicate")

    with pytest.raises(builder.ReleaseAssetError, match="found 2"):
        builder.prepare_release_assets(source, tmp_path / "flat", version="2.0.4")
