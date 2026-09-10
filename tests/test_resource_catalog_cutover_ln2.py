"""LN-2 cutover, navigation, and GUI ownership boundary characterizations."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from freqinout.core.resource_catalog_migration import (
    apply_resource_catalog_migration,
    cutover_resource_catalog_to_canonical,
    resource_catalog_authority_state,
)


ROOT = Path(__file__).resolve().parents[1]


def _seed_legacy_net(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """CREATE TABLE net_resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT, resource_set TEXT NOT NULL, source_type TEXT NOT NULL,
            source_ref TEXT, readonly INTEGER DEFAULT 1, day_utc TEXT NOT NULL, recurrence TEXT DEFAULT 'Weekly',
            biweekly_offset_weeks INTEGER DEFAULT 0, month_weeks TEXT, group_name TEXT, band TEXT NOT NULL,
            mode TEXT NOT NULL, frequency TEXT NOT NULL, start_utc TEXT NOT NULL, end_utc TEXT NOT NULL,
            early_checkin INTEGER NOT NULL, primary_js8call_group TEXT, coverage TEXT, comment TEXT,
            net_name TEXT, fldigi_mode TEXT, fldigi_offset TEXT, updated_utc TEXT)"""
        )
        conn.execute(
            """INSERT INTO net_resources
            (resource_set,source_type,source_ref,readonly,day_utc,recurrence,biweekly_offset_weeks,month_weeks,
             group_name,band,mode,frequency,start_utc,end_utc,early_checkin,primary_js8call_group,coverage,
             comment,net_name,fldigi_mode,fldigi_offset,updated_utc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("reference", "builtin", "reference.json", 1, "Monday", "Weekly", 0, "", "County",
             "2M", "FM", "146.520", "19:00", "20:00", 0, "", "County", "", "County Net", "", "", "2026-09-09T00:00:00Z"),
        )


def _backup_factory(root: Path):
    def backup(paths, **_kwargs):
        selected = [Path(paths)] if isinstance(paths, (str, Path)) else [Path(path) for path in paths]
        root.mkdir(parents=True, exist_ok=True)
        items = []
        for source in selected:
            target = root / source.name
            target.write_bytes(source.read_bytes())
            items.append(SimpleNamespace(original_path=str(source), backup_path=str(target), status="backed_up"))
        return SimpleNamespace(backup_dir=str(root), items=tuple(items))
    return backup


def test_backup_first_canonical_cutover_rolls_back_on_backup_failure_and_commits_after_backup(tmp_path: Path) -> None:
    nets = tmp_path / "freqinout_nets.db"
    _seed_legacy_net(nets)
    original = nets.read_bytes()

    def fail_backup(*_args, **_kwargs):
        raise RuntimeError("backup unavailable")

    with pytest.raises(RuntimeError, match="backup unavailable"):
        apply_resource_catalog_migration(nets, authority_state="canonical", backup_factory=fail_backup)
    assert nets.read_bytes() == original
    with sqlite3.connect(nets) as conn:
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='frequency_resources'").fetchone()[0] == 0

    backup_root = tmp_path / "backup"
    report = apply_resource_catalog_migration(
        nets, authority_state="canonical", backup_factory=_backup_factory(backup_root)
    )
    assert report.authority_state == "canonical"
    assert (backup_root / nets.name).read_bytes() == original
    assert resource_catalog_authority_state(nets) == "canonical"
    with sqlite3.connect(nets) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM frequency_resources WHERE source_key != 'us-fcc-reference-v1'"
        ).fetchone()[0] == 1


def test_canonical_cutover_fast_path_is_zero_write(tmp_path: Path) -> None:
    nets = tmp_path / "freqinout_nets.db"
    _seed_legacy_net(nets)
    apply_resource_catalog_migration(nets, authority_state="canonical", backup_factory=_backup_factory(tmp_path / "backup"))
    before_bytes = nets.read_bytes()
    before_mtime = nets.stat().st_mtime_ns

    report = cutover_resource_catalog_to_canonical(nets)

    assert report.authority_state == "canonical"
    assert nets.read_bytes() == before_bytes
    assert nets.stat().st_mtime_ns == before_mtime


def test_navigation_declares_one_direct_resources_destination_and_compact_icon() -> None:
    source = (ROOT / "freqinout/gui/main_window.py").read_text(encoding="utf-8")

    assert '"Resources": self._create_resources_tab' in source
    assert '("Resources", "Resources")' in source
    for destination in ("Frequency Catalog", "Net Directory", "Import / Export"):
        assert f'("{destination}", "Resources")' not in source
    assert '("Resources", "Tools and Resources", "Resources", "resources.svg")' in source


def test_gui_modules_have_no_direct_legacy_net_resource_mutation_or_schema_ownership() -> None:
    prohibited_mutation = re.compile(r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+net_resources\b", re.IGNORECASE)
    prohibited_schema = re.compile(r"\b(?:CREATE|ALTER|DROP)\s+(?:TABLE\s+)?(?:IF\s+(?:NOT\s+)?EXISTS\s+)?net_resources\b", re.IGNORECASE)
    violations: list[str] = []
    for module in sorted((ROOT / "freqinout/gui").rglob("*.py")):
        source = module.read_text(encoding="utf-8")
        compact = re.sub(r"\s+", " ", source)
        if prohibited_mutation.search(compact) or prohibited_schema.search(compact):
            violations.append(str(module.relative_to(ROOT)))
    assert violations == []
