from __future__ import annotations

import zipfile
from pathlib import Path

import freqinout.core.updater as updater


def _write_zip(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)


def test_apply_update_archive_rejects_parent_traversal(monkeypatch, tmp_path):
    install_dir = tmp_path / "install"
    install_dir.mkdir()
    archive = tmp_path / "bad-parent.zip"
    _write_zip(archive, {"../outside.txt": b"nope"})
    monkeypatch.setattr(updater, "backup_current_install", lambda install_dir: install_dir.parent / "backup")

    assert updater.apply_update_archive(archive, install_dir) is False
    assert not (tmp_path / "outside.txt").exists()


def test_apply_update_archive_rejects_absolute_paths(monkeypatch, tmp_path):
    install_dir = tmp_path / "install"
    install_dir.mkdir()
    archive = tmp_path / "bad-absolute.zip"
    _write_zip(archive, {"/absolute.txt": b"nope"})
    monkeypatch.setattr(updater, "backup_current_install", lambda install_dir: install_dir.parent / "backup")

    assert updater.apply_update_archive(archive, install_dir) is False
    assert not (Path("/") / "absolute.txt").exists()


def test_apply_update_archive_accepts_safe_members(monkeypatch, tmp_path):
    install_dir = tmp_path / "install"
    install_dir.mkdir()
    archive = tmp_path / "good.zip"
    _write_zip(
        archive,
        {
            "app.txt": b"updated",
            "nested/config.json": b"{}",
        },
    )
    monkeypatch.setattr(updater, "backup_current_install", lambda install_dir: install_dir.parent / "backup")

    assert updater.apply_update_archive(archive, install_dir) is True
    assert (install_dir / "app.txt").read_bytes() == b"updated"
    assert (install_dir / "nested" / "config.json").read_bytes() == b"{}"
