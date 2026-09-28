"""Build FreqInOut's public runtime tree from an explicit allowlist.

This maintainer-only tool deliberately copies approved files one at a time.
It never starts with a broad repository copy, so tests, lab helpers, internal
documentation, and Git history cannot enter the public projection by omission.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import tempfile
from typing import Iterable, Iterator


ROOT = Path(__file__).resolve().parents[1]

ROOT_FILES: tuple[str, ...] = (
    ".gitattributes",
    ".gitignore",
    "CHANGELOG.md",
    "CREDITS.md",
    "LICENSE.md",
    "MANIFEST.in",
    "SECURITY.md",
    "install_FreqInOut_linux.sh",
    "install_freqinout.py",
    "pyproject.toml",
    "requirements.txt",
    "start-freqinout.cmd",
    "start-freqinout.sh",
    "start-multi-rig.cmd",
    "start-multi-rig.sh",
    "uninstall_FreqInOut_linux.sh",
)

# Sources may live in the private evidence tree while the public destination
# stays conventional.  Only the destination name enters the projection.
RENAMED_FILES: tuple[tuple[str, str], ...] = (
    ("docs/internal/README-public-2.0-draft.md", "README.md"),
)

PUBLIC_DOCS: tuple[str, ...] = (
    "docs/guide.html",
    "docs/Installation.md",
    "docs/FreqInOut-linux-installer.md",
    "docs/FreqInOut 2.0.2 Installation and Upgrade Guide.docx",
    "docs/FreqInOut Version 2 Upgrade Guide for Current Single Radio Users.docx",
)

JS8NET_FILES: tuple[str, ...] = (
    "third_party/js8net/js8net-main/js8net.py",
    "third_party/js8net/js8net-main/LICENSE",
)

CONFIG_TREES: tuple[str, ...] = (
    "config/leaflet",
    "config/net_resources",
    "config/propagation",
    "config/resource_catalog",
    "config/shortwave/eibi",
)

FORBIDDEN_PARTS = frozenset(
    {
        ".git",
        ".github",
        ".pytest_cache",
        ".venv",
        ".windsurf",
        "__pycache__",
        "build",
        "dist",
        "internal",
        "tests",
        "tools",
        "venv",
    }
)

FORBIDDEN_TEXT = (
    "FreqInOut-internal-testing",
    "wip/private-testing-multi-rig-1.2.3-not-ready",
    "Testing Preview",
    "/Users/bill/RadioCode",
    "/Users/bill/",
    r"C:\Users\HP",
    r"C:\Users\billd",
)

TEXT_SUFFIXES = frozenset({".cmd", ".css", ".html", ".ini", ".js", ".json", ".md", ".py", ".sh", ".toml", ".txt", ".yaml", ".yml"})


def _tree_files(root: Path, relative: str) -> Iterator[Path]:
    base = root / relative
    if not base.is_dir():
        raise FileNotFoundError(f"Required runtime directory is missing: {base}")
    for path in sorted(base.rglob("*")):
        if not path.is_file() or any(part in FORBIDDEN_PARTS for part in path.parts):
            continue
        if path.suffix in {".pyc", ".pyo"}:
            continue
        yield path.relative_to(root)


def iter_public_sources(source_root: Path) -> Iterator[tuple[Path, Path]]:
    """Yield ``(source, public-relative-destination)`` pairs."""

    root = Path(source_root).resolve()
    for relative in ROOT_FILES + PUBLIC_DOCS + JS8NET_FILES:
        yield root / relative, Path(relative)
    for source, destination in RENAMED_FILES:
        yield root / source, Path(destination)
    for relative in _tree_files(root, "freqinout"):
        if relative == Path("freqinout/core/config_lab_preset.py"):
            continue
        yield root / relative, relative
    for relative in _tree_files(root, "assets"):
        yield root / relative, relative
    for tree in CONFIG_TREES:
        for relative in _tree_files(root, tree):
            if relative == Path("config/propagation/README.md"):
                continue
            yield root / relative, relative
    for relative in _tree_files(root, "docs/images/readme-2.0"):
        yield root / relative, relative


def _copy_allowlist(source_root: Path, destination: Path) -> int:
    copied = 0
    seen: set[Path] = set()
    for source, relative_destination in iter_public_sources(source_root):
        if relative_destination in seen:
            raise ValueError(f"Duplicate public destination: {relative_destination}")
        seen.add(relative_destination)
        if not source.is_file():
            raise FileNotFoundError(f"Required public runtime file is missing: {source}")
        target = destination / relative_destination
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied += 1
    return copied


def _projection_files(root: Path) -> Iterable[Path]:
    return (path for path in sorted(root.rglob("*")) if path.is_file())


def validate_public_projection(root: Path) -> tuple[Path, ...]:
    projection = Path(root).resolve()
    files = tuple(_projection_files(projection))
    if not files:
        raise ValueError("Public runtime projection is empty.")
    required = {
        Path("README.md"),
        Path("freqinout/main.py"),
        Path("docs/guide.html"),
        Path("config/net_resources/sitrepnets-winter.json"),
        Path("config/propagation/prop_profiles.json"),
        Path("config/resource_catalog/us_fcc_reference_v1.json"),
        Path("third_party/js8net/js8net-main/js8net.py"),
    }
    relative_files = {path.relative_to(projection) for path in files}
    missing = sorted(required - relative_files)
    if missing:
        raise ValueError("Public runtime projection is missing: " + ", ".join(map(str, missing)))
    for relative in relative_files:
        if any(part in FORBIDDEN_PARTS for part in relative.parts):
            raise ValueError(f"Forbidden path entered public runtime projection: {relative}")
        if relative == Path("freqinout/core/config_lab_preset.py"):
            raise ValueError("Private lab preset entered public runtime projection.")
    for path in files:
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {"LICENSE", "MANIFEST.in"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for marker in FORBIDDEN_TEXT:
            if marker in text:
                raise ValueError(f"Forbidden private marker {marker!r} found in {path.relative_to(projection)}")
    return files


def build_public_runtime_export(source_root: Path, output_dir: Path) -> tuple[int, tuple[Path, ...]]:
    source = Path(source_root).resolve()
    output = Path(output_dir).resolve()
    if output.exists():
        raise FileExistsError(f"Output path already exists; choose a new empty destination: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}-staging-", dir=output.parent))
    try:
        copied = _copy_allowlist(source, staging)
        files = validate_public_projection(staging)
        staging.replace(output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return copied, tuple(output / path.relative_to(staging) for path in files)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the allowlisted FreqInOut public runtime tree.")
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    copied, files = build_public_runtime_export(args.source_root, args.output_dir)
    print(f"Public runtime export ready: {args.output_dir.resolve()}")
    print(f"Copied {copied} allowlisted source files; validated {len(files)} projected files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
