from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

VERSION_FILE = ROOT / "freqinout" / "version.py"
PYPROJECT_FILE = ROOT / "pyproject.toml"
INNO_FILE = ROOT / "installer.iss"
GUIDE_FILE = ROOT / "docs" / "guide.html"
CHANGELOG_FILE = ROOT / "CHANGELOG.md"
README_FILE = ROOT / "README.md"
CONTRIBUTING_FILE = ROOT / "CONTRIBUTING.md"
SECURITY_FILE = ROOT / "SECURITY.md"
PERF_BASELINE_FILE = ROOT / "docs" / "perf-baseline.md"
PERF_BENCH_TOOL = ROOT / "tools" / "perf_benchmark.py"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_version_from_version_py(text: str) -> str | None:
    m = re.search(r'__version__\s*=\s*"([^"]+)"', text)
    return m.group(1) if m else None


def extract_version_from_pyproject(text: str) -> str | None:
    m = re.search(r'^\s*version\s*=\s*"([^"]+)"', text, flags=re.MULTILINE)
    return m.group(1) if m else None


def extract_version_from_inno(text: str) -> str | None:
    define_match = re.search(
        r'^\s*#define\s+MyAppVersion\s+"([^"]+)"',
        text,
        flags=re.MULTILINE,
    )
    if define_match:
        return define_match.group(1)
    m = re.search(r"^\s*AppVersion\s*=\s*([^\r\n]+)", text, flags=re.MULTILINE)
    return m.group(1).strip() if m else None


def extract_version_from_guide(text: str) -> str | None:
    m = re.search(r"<strong>Current version</strong>:\s*([0-9]+\.[0-9]+\.[0-9]+)", text)
    return m.group(1) if m else None


def has_changelog_header(text: str, version: str) -> bool:
    return f"## [{version}]" in text


def has_mojibake(text: str) -> bool:
    markers = ("â€œ", "â€", "â€“", "â€”", "\ufffd")
    return any(marker in text for marker in markers)


def print_report(errors: list[str], warnings: list[str]) -> None:
    print("[release_preflight] Release preflight report")
    for item in warnings:
        print(f"[release_preflight] WARNING: {item}")
    for item in errors:
        print(f"[release_preflight] ERROR: {item}")
    if not errors:
        print("[release_preflight] OK: all required checks passed.")


def main() -> int:
    errors: list[str] = []
    warnings: list[str] = []

    app_version = extract_version_from_version_py(read_text(VERSION_FILE))
    if not app_version:
        errors.append(f"Could not parse __version__ from {VERSION_FILE}")
        print_report(errors, warnings)
        return 1

    pyproject_version = extract_version_from_pyproject(read_text(PYPROJECT_FILE))
    if pyproject_version != app_version:
        errors.append(
            f"Version mismatch: pyproject.toml ({pyproject_version}) != version.py ({app_version})"
        )

    inno_version = extract_version_from_inno(read_text(INNO_FILE))
    if inno_version != app_version:
        errors.append(
            f"Version mismatch: installer.iss AppVersion ({inno_version}) != version.py ({app_version})"
        )

    guide_version = extract_version_from_guide(read_text(GUIDE_FILE))
    if guide_version != app_version:
        errors.append(
            f"Version mismatch: docs/guide.html ({guide_version}) != version.py ({app_version})"
        )

    changelog_text = read_text(CHANGELOG_FILE)
    if not has_changelog_header(changelog_text, app_version):
        errors.append(f"Missing changelog section header for {app_version} in CHANGELOG.md")

    pyproject_text = read_text(PYPROJECT_FILE)
    if "MIT OR GPL" in pyproject_text:
        errors.append("pyproject.toml still contains 'MIT OR GPL' license text.")
    if "GPL-3.0-only" not in pyproject_text and "GPL-3.0-or-later" not in pyproject_text:
        errors.append("pyproject.toml does not clearly declare GPLv3 license metadata.")

    if "<your-repo-url>" in read_text(README_FILE):
        errors.append("README.md contains placeholder '<your-repo-url>'.")
    if "<your-repo-url>" in read_text(CONTRIBUTING_FILE):
        errors.append("CONTRIBUTING.md contains placeholder '<your-repo-url>'.")

    security_text = read_text(SECURITY_FILE).strip().lower()
    if "placeholder" in security_text:
        errors.append("SECURITY.md still appears to be a placeholder.")

    for path in [CHANGELOG_FILE, README_FILE, CONTRIBUTING_FILE, GUIDE_FILE, SECURITY_FILE]:
        if has_mojibake(read_text(path)):
            errors.append(f"Mojibake detected in {path}")

    if (ROOT / "docs" / "appimage.md").exists():
        warnings.append("docs/appimage.md exists. Remove it if AppImage is no longer supported.")
    if not PERF_BASELINE_FILE.exists():
        warnings.append("docs/perf-baseline.md is missing. Add perf baseline workflow documentation.")
    if not PERF_BENCH_TOOL.exists():
        warnings.append("tools/perf_benchmark.py is missing. Add perf benchmark helper tool.")

    print_report(errors, warnings)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
