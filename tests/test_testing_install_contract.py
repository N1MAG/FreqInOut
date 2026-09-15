from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WIP_BRANCH = "wip/private-testing-multi-rig-1.2.3-not-ready"
WIP_REPO = "FreqInOut-internal-testing"


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_community_install_docs_target_only_the_wip_source() -> None:
    for relative in (
        "README.md",
        "docs/Installation.md",
        "docs/FreqInOut-linux-installer.md",
        "docs/multi-rig-isolated-fresh-install-linux.md",
    ):
        source = _text(relative)
        assert WIP_REPO in source, relative
        assert WIP_BRANCH in source, relative
        assert "github.com/N1MAG/FreqInOut.git" not in source, relative


def test_linux_installer_profile_override_covers_migration_backup_and_launcher() -> None:
    source = _text("install_FreqInOut_linux.sh")

    assert '--config-root <p>' in source
    assert 'CONFIG_ROOT_OVERRIDE="${FREQINOUT_CONFIG_DIR:-}"' in source
    assert 'candidates=("$CONFIG_ROOT_OVERRIDE" "${candidates[@]}")' in source
    assert 'export FREQINOUT_CONFIG_DIR="$CONFIG_ROOT_OVERRIDE"' in source
    assert "printf 'export FREQINOUT_CONFIG_DIR=%q" in source


def test_repo_launcher_accepts_source_and_installer_virtualenv_layouts() -> None:
    source = _text("start-multi-rig.sh")

    assert 'WORKTREE="${FREQINOUT_INSTALL_DIR:-$SCRIPT_WORKTREE}"' in source
    assert '$WORKTREE/.venv/bin/python' in source
    assert '$WORKTREE/venv/bin/python' in source


def test_install_helper_declares_supported_python_range() -> None:
    spec = importlib.util.spec_from_file_location("install_freqinout", ROOT / "install_freqinout.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.MIN_PYTHON == (3, 9)
    assert module.MAX_PYTHON == (3, 13)


def test_ci_exercises_wip_on_all_supported_desktop_os_families() -> None:
    source = _text(".github/workflows/ci.yml")

    assert WIP_BRANCH in source
    assert "ubuntu-latest" in source
    assert "macos-latest" in source
    assert "windows-latest" in source
