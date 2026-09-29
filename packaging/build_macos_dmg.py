"""Assemble a native FreqInOut macOS disk image from the PyInstaller app."""

from __future__ import annotations

import argparse
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile

from verify_release_inputs import validated_version


ROOT = Path(__file__).resolve().parents[1]
ARCHITECTURES = {
    "x86_64": "x86_64",
    "amd64": "x86_64",
    "arm64": "arm64",
    "aarch64": "arm64",
}


def macos_architecture(machine: str | None = None) -> str:
    normalized = str(machine or platform.machine()).strip().lower()
    try:
        return ARCHITECTURES[normalized]
    except KeyError as exc:
        raise ValueError(f"Unsupported macOS package architecture: {normalized or 'unknown'}") from exc


def build_dmg(
    output_dir: Path,
    *,
    source_root: Path = ROOT,
    architecture: str | None = None,
) -> Path:
    source = Path(source_root).resolve()
    app = source / "dist" / "FreqInOut.app"
    executable = app / "Contents" / "MacOS" / "FreqInOut"
    if not executable.is_file():
        raise FileNotFoundError(f"PyInstaller macOS app is missing: {executable}")

    version = validated_version(source)
    arch = architecture or macos_architecture()
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    destination = output / f"FreqInOut-{version}-macos-{arch}.dmg"
    if destination.exists():
        destination.unlink()

    with tempfile.TemporaryDirectory(prefix="freqinout-dmg-") as temporary:
        staging = Path(temporary) / "FreqInOut"
        staging.mkdir()
        shutil.copytree(app, staging / app.name, symlinks=True)
        (staging / "Applications").symlink_to("/Applications", target_is_directory=True)
        subprocess.run(
            [
                "hdiutil",
                "create",
                "-quiet",
                "-ov",
                "-format",
                "UDZO",
                "-volname",
                "FreqInOut",
                "-srcfolder",
                str(staging),
                str(destination),
            ],
            check=True,
        )
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "Output")
    args = parser.parse_args()
    artifact = build_dmg(args.output_dir)
    print(artifact)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
