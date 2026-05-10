from __future__ import annotations

import os
import time
from pathlib import Path

from freqinout.core.varac_bbs_inventory import build_bbs_inventory, format_bbs_inventory_detail
from freqinout.gui.message_viewer_tab import _BbsAutoArchiveWorker


class DummySettings:
    def __init__(self, **values):
        self._values = dict(values)

    def get(self, key, default=None):
        return self._values.get(key, default)


def test_bbs_inventory_respects_disabled_bbs(tmp_path: Path) -> None:
    live_dir = tmp_path / "BBS"
    live_dir.mkdir()
    (live_dir / "notice.txt").write_text("hello", encoding="utf-8")
    settings = DummySettings(
        varac_bbs_enabled=False,
        varac_bbs_dir=str(live_dir),
        varac_bbs_auto_archive_enabled=True,
        varac_bbs_auto_archive_days=7,
    )

    inventory = build_bbs_inventory(settings, now_ts=time.time())

    assert inventory.bbs_enabled is False
    assert inventory.live_file_count == 1
    assert format_bbs_inventory_detail(inventory) == "Disabled"


def test_bbs_inventory_counts_managed_locations_and_due_files(tmp_path: Path) -> None:
    now_ts = time.time()
    live_dir = tmp_path / "BBS"
    live_dir.mkdir()
    live_old = live_dir / "old.txt"
    live_old.write_text("old", encoding="utf-8")
    os.utime(live_old, (now_ts - 8 * 86400, now_ts - 8 * 86400))
    default_dir = tmp_path / "vault" / "locations" / "Default"
    intel_dir = tmp_path / "vault" / "locations" / "Intel"
    nested_dir = intel_dir / "nested"
    nested_dir.mkdir(parents=True)
    default_dir.mkdir(parents=True)
    (default_dir / "default.txt").write_text("default", encoding="utf-8")
    intel_old = nested_dir / "intel.txt"
    intel_old.write_text("intel", encoding="utf-8")
    os.utime(intel_old, (now_ts - 8 * 86400, now_ts - 8 * 86400))
    settings = DummySettings(
        varac_bbs_enabled=True,
        varac_bbs_dir=str(live_dir),
        varac_bbs_auto_archive_enabled=True,
        varac_bbs_auto_archive_days=7,
        varac_bbs_vault_enabled=True,
        varac_bbs_vault_default_location_id="default",
        varac_bbs_vault_locations_v1=[
            {"id": "default", "name": "Default", "source_dir": str(default_dir), "enabled": True},
            {"id": "intel", "name": "Intel", "source_dir": str(intel_dir), "enabled": True},
            {"id": "off", "name": "Off", "source_dir": str(tmp_path / "off"), "enabled": False},
        ],
    )

    inventory = build_bbs_inventory(settings, now_ts=now_ts)

    assert inventory.live_file_count == 1
    assert inventory.live_due_now_count == 1
    assert inventory.enabled_location_count == 2
    assert inventory.total_location_count == 3
    assert inventory.managed_file_count == 2
    assert inventory.managed_due_now_count == 1
    detail = format_bbs_inventory_detail(inventory)
    assert "Due now: 1" in detail
    assert "Locations: 2 enabled / 3 total" in detail
    assert "Default" in detail
    assert "Intel" in detail


def test_bbs_auto_archive_mirrors_live_subfolders(tmp_path: Path) -> None:
    now_ts = time.time()
    bbs_dir = tmp_path / "BBS"
    archive_dir = tmp_path / "Archive"
    nested_dir = bbs_dir / "forms"
    nested_dir.mkdir(parents=True)
    archive_dir.mkdir()
    src = nested_dir / "old.txt"
    src.write_text("old", encoding="utf-8")
    os.utime(src, (now_ts - 8 * 86400, now_ts - 8 * 86400))
    worker = _BbsAutoArchiveWorker(
        bbs_dir=str(bbs_dir),
        archive_dir=str(archive_dir),
        days=7,
        allowed_exts=[".txt"],
        reason="test",
        archive_context="live",
    )
    results = []
    worker.finished.connect(results.append)

    worker.run()

    assert results
    assert results[0]["moved_count"] == 1
    assert not src.exists()
    assert (archive_dir / "live" / "forms" / "old.txt").exists()
