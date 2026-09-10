"""Focused LN-6 release qualification checks for Resources and Local Nets.

These checks stay on isolated temporary profiles.  They exercise the release
seams without touching a user's configuration, radio, scheduler, or files.
"""

from __future__ import annotations

import json
import math
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from freqinout.core.config_backup import create_config_backup
from freqinout.core.local_net_models import LocalNetSchedule
from freqinout.core.local_net_projection import build_local_net_outlook
from freqinout.core.local_net_store import LocalNetStore
from freqinout.core.resource_catalog_models import (
    CatalogSource,
    FrequencyResource,
    NetDirectoryEntry,
    NetDirectorySession,
)
from freqinout.core.resource_catalog_store import ResourceCatalogStore
from freqinout.core.resource_catalog_transfer import (
    apply_import_preview,
    export_selected_resources,
    preview_json_import,
)


def _catalog(path: Path) -> ResourceCatalogStore:
    store = ResourceCatalogStore(path)
    store.create_schema()
    store.create_source(CatalogSource("station", "Synthetic Station", "station"))
    return store


def test_catalog_import_export_round_trip_isolated_and_preview_first(tmp_path: Path) -> None:
    source = _catalog(tmp_path / "source.db")
    frequency = source.create_frequency(
        FrequencyResource(
            "frequency_release",
            "station",
            "simplex",
            "GMRS",
            "Synthetic Release Channel",
            band="UHF",
            receive_hz=462550000,
            transmit_hz=462550000,
        )
    )
    entry = source.create_net_entry(
        NetDirectoryEntry("entry_release", "station", "Synthetic Release Net")
    )
    session = source.create_session(
        NetDirectorySession(
            "session_release",
            entry.net_entry_key,
            "station",
            "GMRS",
            frequency_resource_key=frequency.frequency_resource_key,
            recurrence="Weekly",
            local_start_time="20:00",
            timezone="UTC",
            day_utc="Wednesday",
        )
    )
    payload = export_selected_resources(source, net_entry_keys=(entry.net_entry_key,))
    target = _catalog(tmp_path / "target.db")

    preview = preview_json_import(target, json.dumps(payload), target_source_key="transfer")
    assert {item.status for item in preview.diagnostics} == {"new"}
    # Preview is explicitly non-mutating; Apply is the only write boundary.
    assert target.get_net_entry(entry.net_entry_key) is None
    results = apply_import_preview(target, preview)
    assert {item.status for item in results} == {"new"}

    round_trip = export_selected_resources(target, net_entry_keys=(entry.net_entry_key,))
    assert [row["net_entry_key"] for row in round_trip["net_entries"]] == [entry.net_entry_key]
    assert [row["net_session_key"] for row in round_trip["sessions"]] == [session.net_session_key]
    assert round_trip["frequencies"][0]["receive_hz"] == 462550000
    assert round_trip["sessions"][0]["frequency_resource_key"] == frequency.frequency_resource_key


def test_backup_manifest_supports_isolated_restore_rehearsal(tmp_path: Path) -> None:
    profile = tmp_path / "profile"
    source = profile / "settings.json"
    source.parent.mkdir(parents=True)
    source.write_text('{"feature": "before"}\n', encoding="utf-8")
    backup = create_config_backup(
        [source],
        reason="ln6-rehearsal",
        backup_root=tmp_path / "backups",
        now=lambda: datetime(2026, 9, 9, 12, 0, 0),
    )

    manifest = json.loads(Path(backup.manifest_path).read_text(encoding="utf-8"))
    assert manifest["reason"] == "ln6-rehearsal"
    assert manifest["items"][0]["status"] == "backed_up"
    backup_file = Path(backup.items[0].backup_path)
    assert backup_file.read_text(encoding="utf-8") == '{"feature": "before"}\n'

    # Rehearse recovery under a separate root; the live source is never
    # overwritten by the test and remains independently inspectable.
    source.write_text('{"feature": "changed"}\n', encoding="utf-8")
    recovery = tmp_path / "recovery" / "settings.json"
    recovery.parent.mkdir()
    shutil.copy2(backup_file, recovery)
    assert source.read_text(encoding="utf-8") == '{"feature": "changed"}\n'
    assert recovery.read_text(encoding="utf-8") == '{"feature": "before"}\n'


def test_resources_navigation_is_lazy_named_and_compact_accessible(tmp_path: Path) -> None:
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication, QAbstractScrollArea

    from freqinout.gui.resources_tab import ResourcesTab

    app = QApplication.instance() or QApplication([])
    tab = ResourcesTab(db_path=tmp_path / "resources.db")
    try:
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        assert tab.tabs.accessibleName() == "Tools and Resources workspace"
        assert tuple(tab.tabs.tabText(index) for index in range(tab.tabs.count())) == (
            "Frequency Catalog",
            "Net Directory",
            "Import / Export",
        )
        assert set(tab._pages) == {0}
        assert tab.open_section("net_directory") is not None
        assert tab.open_section("import_export") is not None
        assert set(tab._pages) == {0, 1, 2}
        assert tab.tabs.currentIndex() == 2
        for scroll in tab.findChildren(QAbstractScrollArea):
            assert scroll.horizontalScrollBar().maximum() == 0
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


def test_ops_projection_meets_thousand_schedule_warm_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from freqinout.core import db_initializer

    monkeypatch.setattr(db_initializer, "_config_dir", lambda: tmp_path)
    db_initializer._ensure_nets_db()
    path = tmp_path / "freqinout_nets.db"
    store = LocalNetStore(path)
    catalog = ResourceCatalogStore(path)
    now = datetime(2026, 1, 5, 18, tzinfo=timezone.utc)
    for index in range(1_000):
        store.save_schedule(
            LocalNetSchedule(
                local_net_schedule_key=f"release_{index:04d}",
                name=f"Synthetic Release Net {index:04d}",
                service="AMATEUR",
                recurrence="daily",
                local_start_time="19:00",
                timezone_name="UTC",
                duration_minutes=60,
                reminder_minutes=15,
            ),
            now_utc=now,
        )

    build_local_net_outlook(store, catalog, now, later_limit=50)
    samples: list[float] = []
    for _ in range(15):
        started = time.perf_counter()
        snapshot = build_local_net_outlook(store, catalog, now, later_limit=50)
        samples.append(time.perf_counter() - started)
    p95 = sorted(samples)[math.ceil(len(samples) * 0.95) - 1]

    assert len(snapshot.later) == 50
    assert p95 < 0.050, f"warm Ops Local Net projection p95 was {p95 * 1_000:.1f} ms"


def test_bounded_outlook_does_not_skip_overlapping_daily_occurrences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from freqinout.core import db_initializer

    monkeypatch.setattr(db_initializer, "_config_dir", lambda: tmp_path)
    db_initializer._ensure_nets_db()
    store = LocalNetStore(tmp_path / "freqinout_nets.db")
    start = datetime(2026, 1, 5, 18, tzinfo=timezone.utc)
    store.save_schedule(
        LocalNetSchedule(
            local_net_schedule_key="overlapping_release",
            name="Synthetic Long Activity",
            service="AMATEUR",
            recurrence="daily",
            local_start_time="19:00",
            timezone_name="UTC",
            duration_minutes=2 * 24 * 60,
        ),
        now_utc=start,
    )

    occurrences = store.outlook_occurrences(start, horizon_days=4, limit=4)

    assert [item.start_utc.day for item in occurrences] == [4, 5, 6, 7]
