from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest

from freqinout.core.multi_rig_runtime_status import (
    STARTUP_DEFERRED,
    STARTUP_EXISTING_UNMIGRATED,
    STARTUP_FRESH_DEFAULT_READY,
    STARTUP_MIGRATED,
    STARTUP_MIGRATION_ERROR,
)


ROOT = Path(__file__).resolve().parents[1]


def _installer_module():
    spec = importlib.util.spec_from_file_location("install_freqinout_test", ROOT / "install_freqinout.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sqlite_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO sample(value) VALUES('station')")


def test_installer_preserves_and_rebuilds_incomplete_virtual_environment(monkeypatch, tmp_path: Path) -> None:
    installer = _installer_module()
    broken = tmp_path / ".venv"
    broken.mkdir()
    (broken / "partial.txt").write_text("keep", encoding="utf-8")

    def fake_run(command, *, env=None):
        assert command[:3] == [installer.sys.executable, "-m", "venv"]
        python = installer._venv_python(Path(command[-1]))
        python.parent.mkdir(parents=True, exist_ok=True)
        python.write_text("replacement", encoding="utf-8")

    monkeypatch.setattr(installer, "_run", fake_run)
    monkeypatch.setattr(installer, "_python_is_usable", lambda path: path.is_file())

    python, quarantined = installer._ensure_virtual_environment(tmp_path)

    assert python.is_file()
    assert quarantined is not None
    assert (quarantined / "partial.txt").read_text(encoding="utf-8") == "keep"


def test_installer_repairs_missing_pip(monkeypatch, tmp_path: Path) -> None:
    installer = _installer_module()
    python = tmp_path / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("python", encoding="utf-8")
    checks = iter((1, 0))
    commands = []

    monkeypatch.setattr(
        installer.subprocess,
        "run",
        lambda *_args, **_kwargs: type("Result", (), {"returncode": next(checks)})(),
    )
    monkeypatch.setattr(installer, "_run", lambda command, **_kwargs: commands.append(command))

    installer._ensure_pip(python)

    assert commands == [[str(python), "-m", "ensurepip", "--upgrade"]]


def test_installer_creates_and_verifies_cold_existing_station_backup(tmp_path: Path) -> None:
    installer = _installer_module()
    profile = tmp_path / "profile"
    _sqlite_database(profile / "config" / "freqinout.db")
    _sqlite_database(profile / "config" / "freqinout_nets.db")
    (profile / "config" / "operator-note.txt").write_text("retain", encoding="utf-8")

    backup = installer._create_verified_profile_backup(profile)

    assert backup is not None
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["database_quick_check"] == {
        "freqinout.db": "ok",
        "freqinout_nets.db": "ok",
    }
    assert (backup / "config" / "operator-note.txt").read_text(encoding="utf-8") == "retain"
    assert installer._file_hashes(profile / "config") == installer._file_hashes(backup / "config")


def test_installer_retries_transient_backup_rename(monkeypatch, tmp_path: Path) -> None:
    installer = _installer_module()
    profile = tmp_path / "profile"
    _sqlite_database(profile / "config" / "freqinout.db")
    real_rename = Path.rename
    attempts = 0
    delays: list[float] = []

    def flaky_rename(path: Path, target: Path):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(13, "Access is denied", str(path))
        return real_rename(path, target)

    monkeypatch.setattr(Path, "rename", flaky_rename)
    monkeypatch.setattr(installer.time, "sleep", lambda delay: delays.append(delay))

    backup = installer._create_verified_profile_backup(profile)

    assert backup is not None
    assert backup.name.startswith("pre-install-")
    assert not backup.name.startswith(".pre-install-")
    assert attempts == 3
    assert delays == [0.1, 0.25]
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["verification_status"] == "verified"


def test_installer_keeps_verified_backup_when_windows_denies_final_name(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    installer = _installer_module()
    profile = tmp_path / "profile"
    _sqlite_database(profile / "config" / "freqinout.db")
    attempts = 0

    def denied_rename(path: Path, target: Path):
        nonlocal attempts
        attempts += 1
        raise PermissionError(13, "Access is denied", str(path))

    monkeypatch.setattr(Path, "rename", denied_rename)
    monkeypatch.setattr(installer.time, "sleep", lambda _delay: None)

    backup = installer._create_verified_profile_backup(profile)

    assert backup is not None
    assert backup.is_dir()
    assert backup.name.startswith(".pre-install-")
    assert attempts == len(installer.BACKUP_RENAME_RETRY_DELAYS) + 1
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["verification_status"] == "verified"
    assert installer._file_hashes(profile / "config") == installer._file_hashes(backup / "config")
    output = capsys.readouterr().out
    assert "Backup naming warning" in output
    assert f"Verified pre-install station backup: {backup}" in output

    root = tmp_path / "app"
    root.mkdir()
    (root / "requirements.txt").write_text("", encoding="utf-8")
    installer._write_install_receipt(root, Path(installer.sys.executable), "2.0.2", backup)
    receipt = json.loads((root / installer.INSTALL_RECEIPT).read_text(encoding="utf-8"))
    assert receipt["pre_install_backup"] == str(backup)


def test_installer_still_aborts_and_cleans_up_non_permission_finalization_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    installer = _installer_module()
    profile = tmp_path / "profile"
    _sqlite_database(profile / "config" / "freqinout.db")

    def broken_rename(path: Path, target: Path):
        raise OSError(22, "Invalid argument", str(path))

    monkeypatch.setattr(Path, "rename", broken_rename)

    with pytest.raises(OSError, match="Invalid argument"):
        installer._create_verified_profile_backup(profile)

    backup_root = profile / "backups"
    assert not list(backup_root.glob(".pre-install-*"))
    assert not list(backup_root.glob("pre-install-*"))


def test_installer_does_not_accept_missing_verified_staging_backup(
    monkeypatch,
    tmp_path: Path,
) -> None:
    installer = _installer_module()
    temporary = tmp_path / ".pre-install-test"
    final = tmp_path / "pre-install-final"
    (temporary / "config").mkdir(parents=True)
    (temporary / "manifest.json").write_text("{}", encoding="utf-8")

    def vanished_rename(path: Path, target: Path):
        if path.exists():
            installer.shutil.rmtree(path)
        raise PermissionError(13, "Access is denied", str(path))

    monkeypatch.setattr(Path, "rename", vanished_rename)
    monkeypatch.setattr(installer.time, "sleep", lambda _delay: None)

    with pytest.raises(PermissionError, match="Access is denied"):
        installer._finalize_verified_profile_backup(temporary, final)


def test_installer_blocks_corrupt_existing_station_before_backup(tmp_path: Path) -> None:
    installer = _installer_module()
    profile = tmp_path / "profile"
    database = profile / "config" / "freqinout.db"
    database.parent.mkdir(parents=True)
    database.write_bytes(b"not a sqlite database")

    with pytest.raises(installer.InstallationError, match="contact FreqInOut support"):
        installer._create_verified_profile_backup(profile)

    assert not (profile / "backups").exists()


def test_installer_blocks_backup_when_profile_volume_lacks_space(monkeypatch, tmp_path: Path) -> None:
    installer = _installer_module()
    profile = tmp_path / "profile"
    _sqlite_database(profile / "config" / "freqinout.db")
    monkeypatch.setattr(
        installer.shutil,
        "disk_usage",
        lambda _path: type("Usage", (), {"free": 0})(),
    )

    with pytest.raises(installer.InstallationError, match="not enough free space"):
        installer._create_verified_profile_backup(profile)

    assert not (profile / "backups").exists()


@pytest.mark.parametrize(
    "mode,expected",
    (
        (STARTUP_FRESH_DEFAULT_READY, False),
        (STARTUP_MIGRATED, False),
        (STARTUP_EXISTING_UNMIGRATED, True),
        (STARTUP_DEFERRED, True),
        (STARTUP_MIGRATION_ERROR, True),
    ),
)
def test_pre_main_window_upgrade_gate_targets_only_existing_station_modes(mode, expected) -> None:
    from freqinout.main import _requires_existing_station_upgrade

    assert _requires_existing_station_upgrade(mode) is expected


def test_upgrade_gate_runs_before_main_window_construction() -> None:
    source = (ROOT / "freqinout" / "main.py").read_text(encoding="utf-8")

    assert source.index("_run_existing_station_upgrade_gate(", source.index("def main")) < source.index(
        "win = MainWindow(", source.index("def main")
    )
    assert "if not upgrade_ready:" in source


@pytest.mark.parametrize("dialog_result", (False, True))
def test_pre_main_window_upgrade_gate_returns_dialog_result(monkeypatch, dialog_result) -> None:
    import freqinout.gui.settings_tab as settings_tab_module
    import freqinout.main as main_module

    events = []

    class FakeSettingsManager:
        def all(self):
            return {"callsign": "Existing"}

        def close(self):
            events.append("status-settings-closed")

    class FakeGate:
        def __init__(self, *_args, **_kwargs):
            self.settings = FakeSettingsManager()

        def set_multi_rig_runtime_status(self, status):
            events.append(status.startup_mode)

        def _start_multi_rig_setup(self):
            events.append("dialog")
            return dialog_result

        def shutdown(self):
            events.append("shutdown")

        def deleteLater(self):
            events.append("delete")

    status = type("Status", (), {"startup_mode": STARTUP_EXISTING_UNMIGRATED})()
    monkeypatch.setattr(main_module, "SettingsManager", FakeSettingsManager)
    monkeypatch.setattr(main_module, "MultiRadioStore", lambda: object())
    monkeypatch.setattr(main_module, "build_multi_rig_runtime_status", lambda *_args, **_kwargs: status)
    monkeypatch.setattr(settings_tab_module, "SettingsTab", FakeGate)

    assert main_module._run_existing_station_upgrade_gate(
        before_dialog=lambda: events.append("before")
    ) is dialog_result
    assert events[:4] == ["status-settings-closed", "before", STARTUP_EXISTING_UNMIGRATED, "dialog"]
    assert events[-3:] == ["shutdown", "status-settings-closed", "delete"]


def test_upgrade_ui_keeps_identity_fields_and_has_no_deferral_action() -> None:
    source = (ROOT / "freqinout" / "gui" / "settings_tab.py").read_text(encoding="utf-8")
    block = source[source.index("def _start_multi_rig_setup") : source.index("def _set_multi_rig_setup_preview_text")]

    assert 'dialog.setWindowTitle("Upgrade Existing Station")' in block
    assert 'buttons.addButton("Back Up and Upgrade Station"' in block
    assert 'buttons.addButton("Exit FIO"' in block
    assert 'form.addRow("Manufacturer:", manufacturer_edit)' in block
    assert 'form.addRow("Model:", model_edit)' in block
    assert "Not Now" not in block
    assert "defer" not in block.lower()
