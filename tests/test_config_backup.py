from __future__ import annotations

import json
from datetime import datetime

from freqinout.core.config_backup import create_config_backup


def test_config_backup_copies_files_and_directories_with_manifest(tmp_path) -> None:
    config_file = tmp_path / "JS8Call.ini"
    config_file.write_text("MyCall=N1MAG\n", encoding="utf-8")
    config_dir = tmp_path / ".flrig"
    config_dir.mkdir()
    (config_dir / "settings").write_text("xmlrpc=12345\n", encoding="utf-8")

    result = create_config_backup(
        [config_file, config_dir],
        reason="pre multirig",
        backup_root=tmp_path / "backups",
        now=lambda: datetime(2026, 7, 28, 14, 15, 30),
    )

    assert result.backup_dir.endswith("pre-multirig-20260728-141530")
    backed_up = {item.kind: item for item in result.items}
    assert backed_up["file"].status == "backed_up"
    assert backed_up["directory"].status == "backed_up"
    assert (tmp_path / "backups" / "pre-multirig-20260728-141530" / "JS8Call.ini").read_text(
        encoding="utf-8"
    ) == "MyCall=N1MAG\n"
    assert (
        tmp_path / "backups" / "pre-multirig-20260728-141530" / ".flrig" / "settings"
    ).read_text(encoding="utf-8") == "xmlrpc=12345\n"

    manifest = json.loads((tmp_path / "backups" / "pre-multirig-20260728-141530" / "manifest.json").read_text())
    assert manifest["reason"] == "pre-multirig"
    assert len(manifest["items"]) == 2


def test_config_backup_records_missing_paths_without_failure(tmp_path) -> None:
    result = create_config_backup(
        [tmp_path / "missing.ini"],
        backup_root=tmp_path / "backups",
        now=lambda: datetime(2026, 7, 28, 14, 15, 30),
    )

    assert len(result.items) == 1
    assert result.items[0].status == "missing"
    assert result.items[0].backup_path == ""
    assert "does not exist" in result.items[0].error


def test_config_backup_disambiguates_duplicate_names(tmp_path) -> None:
    first = tmp_path / "one" / "settings.ini"
    second = tmp_path / "two" / "settings.ini"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_text("one\n", encoding="utf-8")
    second.write_text("two\n", encoding="utf-8")

    result = create_config_backup(
        [first, second],
        backup_root=tmp_path / "backups",
        now=lambda: datetime(2026, 7, 28, 14, 15, 30),
    )

    assert [item.status for item in result.items] == ["backed_up", "backed_up"]
    assert result.items[0].backup_path.endswith("settings.ini")
    assert result.items[1].backup_path.endswith("settings-2.ini")


def test_config_backup_uses_unique_directory_when_timestamp_collides(tmp_path) -> None:
    config_file = tmp_path / "settings.ini"
    config_file.write_text("settings\n", encoding="utf-8")
    existing = tmp_path / "backups" / "pre-multirig-20260728-141530"
    existing.mkdir(parents=True)

    result = create_config_backup(
        [config_file],
        reason="pre-multirig",
        backup_root=tmp_path / "backups",
        now=lambda: datetime(2026, 7, 28, 14, 15, 30),
    )

    assert result.backup_dir.endswith("pre-multirig-20260728-141530-2")
    assert (tmp_path / "backups" / "pre-multirig-20260728-141530-2" / "settings.ini").is_file()


def test_config_backup_excludes_nested_backup_root_when_copying_config_dir(tmp_path) -> None:
    config_dir = tmp_path / "fio-config"
    config_dir.mkdir()
    (config_dir / "settings.db").write_text("db\n", encoding="utf-8")
    old_backup = config_dir / "backups" / "old-backup"
    old_backup.mkdir(parents=True)
    (old_backup / "stale.txt").write_text("old\n", encoding="utf-8")

    result = create_config_backup(
        [config_dir],
        reason="pre-multirig",
        backup_root=config_dir / "backups",
        now=lambda: datetime(2026, 7, 28, 14, 15, 30),
    )

    copied_config = tmp_path / "fio-config" / "backups" / "pre-multirig-20260728-141530" / "fio-config"
    assert result.items[0].status == "backed_up"
    assert (copied_config / "settings.db").read_text(encoding="utf-8") == "db\n"
    assert not (copied_config / "backups").exists()


def test_config_backup_preserves_siblings_when_backup_root_is_nested(tmp_path) -> None:
    config_dir = tmp_path / "fio-config"
    nested = config_dir / "nested"
    backup_root = nested / "backups"
    backup_root.mkdir(parents=True)
    (nested / "keep.ini").write_text("keep\n", encoding="utf-8")
    (backup_root / "old.txt").write_text("old\n", encoding="utf-8")

    result = create_config_backup(
        [config_dir],
        reason="pre-multirig",
        backup_root=backup_root,
        now=lambda: datetime(2026, 7, 28, 14, 15, 30),
    )

    copied_config = backup_root / "pre-multirig-20260728-141530" / "fio-config"
    assert result.items[0].status == "backed_up"
    assert (copied_config / "nested" / "keep.ini").read_text(encoding="utf-8") == "keep\n"
    assert not (copied_config / "nested" / "backups").exists()


def test_config_backup_rejects_backup_root_as_source(tmp_path) -> None:
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    (backup_root / "old.txt").write_text("old\n", encoding="utf-8")

    result = create_config_backup(
        [backup_root],
        backup_root=backup_root,
        now=lambda: datetime(2026, 7, 28, 14, 15, 30),
    )

    assert result.items[0].status == "failed"
    assert "cannot be backed up into itself" in result.items[0].error
