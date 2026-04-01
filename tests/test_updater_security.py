from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import freqinout.core.updater as updater


def _write_zip(path: Path, members: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)


class _FakeResponse:
    def __init__(self, *, content: bytes = b"", json_data: dict | None = None) -> None:
        self._content = content
        self._json_data = json_data or {}

    def raise_for_status(self) -> None:
        return

    def json(self) -> dict:
        return dict(self._json_data)

    def iter_content(self, chunk_size: int = 8192):
        for idx in range(0, len(self._content), chunk_size):
            yield self._content[idx : idx + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


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


def test_fetch_update_info_requires_sha256(monkeypatch):
    monkeypatch.setattr(updater, "UPDATE_INFO_URL", "https://example.test/update.json")
    monkeypatch.setattr(
        updater.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            json_data={"version": "1.2.2", "download_url": "https://example.test/release.zip"}
        ),
    )

    assert updater.fetch_update_info() is None


def test_download_release_rejects_hash_mismatch(monkeypatch, tmp_path):
    data = b"release-bytes"
    monkeypatch.setattr(updater, "_download_dir", lambda: tmp_path)
    monkeypatch.setattr(
        updater.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(content=data),
    )

    result = updater.download_release("https://example.test/release.zip", "0" * 64)

    assert result is None
    assert not (tmp_path / "release.zip").exists()


def test_download_release_accepts_matching_sha256(monkeypatch, tmp_path):
    data = b"release-bytes"
    expected_sha256 = hashlib.sha256(data).hexdigest()
    monkeypatch.setattr(updater, "_download_dir", lambda: tmp_path)
    monkeypatch.setattr(
        updater.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(content=data),
    )

    result = updater.download_release("https://example.test/release.zip", expected_sha256)

    assert result == tmp_path / "release.zip"
    assert result.read_bytes() == data


def test_parse_version_handles_prerelease_suffix():
    assert updater.parse_version("1.2.2-beta") == (1, 2, 2)
