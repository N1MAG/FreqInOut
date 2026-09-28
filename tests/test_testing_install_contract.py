from __future__ import annotations

import ast
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
WIP_BRANCH = "wip/private-testing-multi-rig-1.2.3-not-ready"
WIP_REPO = "FreqInOut-internal-testing"
PUBLIC_REPO = "https://github.com/N1MAG/FreqInOut.git"


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_public_install_docs_target_only_the_stable_public_source() -> None:
    for relative in (
        "docs/Installation.md",
        "docs/FreqInOut-linux-installer.md",
    ):
        source = _text(relative)
        assert "github.com/N1MAG/FreqInOut.git" in source, relative
        assert WIP_REPO not in source, relative
        assert WIP_BRANCH not in source, relative


def test_linux_installer_profile_override_covers_backup_and_launcher() -> None:
    source = _text("install_FreqInOut_linux.sh")

    assert '--config-root <p>' in source
    assert 'CONFIG_ROOT_OVERRIDE="${FREQINOUT_CONFIG_DIR:-}"' in source
    assert 'candidates=("$CONFIG_ROOT_OVERRIDE" "${candidates[@]}")' in source
    assert 'export FREQINOUT_CONFIG_DIR="$CONFIG_ROOT_OVERRIDE"' in source
    assert "printf 'export FREQINOUT_CONFIG_DIR=%q" in source


def test_linux_installer_leaves_migration_to_fio_and_ships_runtime_launcher() -> None:
    source = _text("install_FreqInOut_linux.sh")

    assert f'DEFAULT_REPO_URL="{PUBLIC_REPO}"' in source
    assert 'DEFAULT_BRANCH="main"' in source
    assert "finalize_multi_rig_config_migration" not in source
    assert "Configuration migration will be reviewed in FIO on first launch." in source
    assert "start-multi-rig.sh" in source


def test_linux_installer_honors_explicit_repo_for_existing_checkout() -> None:
    source = _text("install_FreqInOut_linux.sh")

    assert "REPO_EXPLICIT=1" in source
    assert 'remote get-url origin' in source
    assert 'remote set-url origin "$REPO_URL"' in source
    assert 'ROLLBACK_ORIGIN_URL="$current_origin"' in source
    assert 'remote set-url origin "$ROLLBACK_ORIGIN_URL"' in source
    assert 'refs/remotes/origin/$target_branch' in source
    assert 'checkout -b "$target_branch" --track "origin/$target_branch"' in source


def test_repo_launcher_accepts_source_and_installer_virtualenv_layouts() -> None:
    source = _text("start-freqinout.sh")

    assert 'WORKTREE="${FREQINOUT_INSTALL_DIR:-$SCRIPT_WORKTREE}"' in source
    assert '$WORKTREE/.venv/bin/python' in source
    assert '$WORKTREE/venv/bin/python' in source
    assert 'DEFAULT_RUNTIME_ROOT=' not in source
    assert 'LEGACY_RUNTIME_ROOT=' not in source
    assert 'export FREQINOUT_CONFIG_DIR="$RUNTIME_ROOT"' not in source
    assert ".freqinout-install-verified.json" in source
    assert 'if [[ -d "$WORKTREE/.venv" ]]' in source

    wrapper = _text("start-multi-rig.sh")
    assert 'exec "$SCRIPT_WORKTREE/start-freqinout.sh" "$@"' in wrapper


def test_windows_repo_launcher_matches_profile_and_virtualenv_contract() -> None:
    source = _text("start-freqinout.cmd")

    assert "%FREQINOUT_INSTALL_DIR%" in source
    assert ".venv\\Scripts\\python.exe" in source
    assert "venv\\Scripts\\python.exe" in source
    assert "-m freqinout.main %*" in source
    assert "LOCALAPPDATA" in source
    assert "runtime\\multi-rig" not in source
    assert ".freqinout-install-verified.json" in source

    wrapper = _text("start-multi-rig.cmd")
    assert 'call "%~dp0start-freqinout.cmd" %*' in wrapper


@pytest.mark.skipif(os.name == "nt", reason="POSIX launcher execution contract")
def test_posix_launcher_preserves_default_and_explicit_profile_environment(tmp_path: Path) -> None:
    worktree = tmp_path / "source tree"
    python_path = worktree / ".venv" / "bin" / "python"
    package = worktree / "freqinout"
    python_path.parent.mkdir(parents=True)
    package.mkdir()
    python_path.symlink_to(sys.executable)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "version.py").write_text('__version__ = "2.0.1"\n', encoding="utf-8")
    (package / "main.py").write_text(
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "Path(os.environ['FIO_LAUNCH_PROBE']).write_text(json.dumps({\n"
        "    'config': os.environ.get('FREQINOUT_CONFIG_DIR'),\n"
        "    'args': sys.argv[1:],\n"
        "}), encoding='utf-8')\n",
        encoding="utf-8",
    )
    (worktree / "PySide6.py").write_text("", encoding="utf-8")
    (worktree / ".freqinout-install-verified.json").write_text(
        json.dumps({"version": "2.0.1", "python": str(Path(sys.executable).resolve())}),
        encoding="utf-8",
    )

    probe = tmp_path / "probe.json"
    env = os.environ.copy()
    env.pop("FREQINOUT_CONFIG_DIR", None)
    env.pop("FREQINOUT_RUNTIME_ROOT", None)
    env["FREQINOUT_INSTALL_DIR"] = str(worktree)
    env["FIO_LAUNCH_PROBE"] = str(probe)
    subprocess.run(
        ["bash", str(ROOT / "start-freqinout.sh"), "alpha", "two words"],
        check=True,
        env=env,
    )
    assert json.loads(probe.read_text(encoding="utf-8")) == {
        "config": None,
        "args": ["alpha", "two words"],
    }

    explicit = tmp_path / "explicit profile"
    env["FREQINOUT_CONFIG_DIR"] = str(explicit)
    subprocess.run(
        ["bash", str(ROOT / "start-freqinout.sh")],
        check=True,
        env=env,
    )
    assert json.loads(probe.read_text(encoding="utf-8"))["config"] == str(explicit)


def test_runtime_requirements_exclude_internal_tool_dependencies() -> None:
    requirements = _text("requirements.txt")
    project = _text("pyproject.toml")

    assert "PyYAML" not in requirements
    assert 'dev = [\n  "PyYAML>=6.0",\n]' in project
    assert '"pyyaml"' not in _text("tools/release_preflight.py")

    match = re.search(r"(?ms)^dependencies = \[(.*?)^\]", project)
    assert match is not None
    project_requirements = ast.literal_eval("[" + match.group(1) + "]")
    file_requirements = [
        line.strip()
        for line in requirements.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    normalize = lambda value: re.sub(r"\s+", " ", value.replace('"', "'")).strip()
    assert {normalize(value) for value in file_requirements} == {
        normalize(value) for value in project_requirements
    }


def test_install_helper_declares_supported_python_range() -> None:
    spec = importlib.util.spec_from_file_location("install_freqinout", ROOT / "install_freqinout.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.MIN_PYTHON == (3, 10)
    assert module.MAX_PYTHON == (3, 13)
    source = _text("install_freqinout.py")
    assert "3.10 through 3.13" in source
    assert 'requires-python = ">=3.10,<3.14"' in _text("pyproject.toml")
    assert "MIN_PYTHON_MINOR=10" in _text("install_FreqInOut_linux.sh")
    assert 'run_hint = r".\\start-freqinout.cmd"' in source
    assert 'run_hint = "./start-freqinout.sh"' in source


def test_ci_exercises_wip_on_all_supported_desktop_os_families() -> None:
    source = _text(".github/workflows/ci.yml")

    assert WIP_BRANCH in source
    assert "ubuntu-latest" in source
    assert "macos-latest" in source
    assert "windows-latest" in source
