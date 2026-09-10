"""LN-5 acceptance contract for Local Net Ops/SOP integration.

These tests deliberately use synthetic schedule names and identifiers.  They
are the executable boundary for the reminder projection: Local Nets may
explain an upcoming activity and hand an operator to SOP guidance, but they
must never become another radio-control or scheduler command path.
"""

from __future__ import annotations

import inspect
import os
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.local_net_models import LocalNetSchedule
from freqinout.core.local_net_store import LocalNetStore
from freqinout.core.navigation_intent import NavigationIntent


UTC = timezone.utc


def _utc(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 1, 7, hour, minute, tzinfo=UTC)


def _schedule(key: str, *, start: str, sop_id: int | None = None) -> LocalNetSchedule:
    return LocalNetSchedule(
        local_net_schedule_key=key,
        name=f"Synthetic Net {key}",
        service="AMATEUR",
        recurrence="daily",
        local_start_time=start,
        timezone_name="UTC",
        duration_minutes=60,
        reminder_minutes=15,
        operating_group_key="group_synthetic",
        operating_group_name="Synthetic Group",
        frequency_resource_key="frequency_synthetic",
        sop_id=sop_id,
    )


@pytest.fixture()
def nets_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    import freqinout.core.db_initializer as db_initializer

    monkeypatch.setattr(db_initializer, "_config_dir", lambda: tmp_path)
    db_initializer._ensure_nets_db()
    return tmp_path / "freqinout_nets.db"


def _projection_api():
    """Load the LN-5 production seam with a useful contract error."""
    projection = pytest.importorskip("freqinout.core.local_net_projection")
    builder = getattr(projection, "build_local_net_outlook", None)
    if builder is None:
        pytest.fail("LN-5 requires local_net_projection.build_local_net_outlook")
    return projection, builder


def _catalog(nets_db: Path):
    from freqinout.core.resource_catalog_store import ResourceCatalogStore

    catalog = ResourceCatalogStore(nets_db)
    catalog.create_schema()
    return catalog


def test_projection_is_immutable_local_and_non_commandable(nets_db: Path) -> None:
    _, project = _projection_api()
    store = LocalNetStore(nets_db)
    catalog = _catalog(nets_db)
    store.save_schedule(_schedule("active", start="18:30", sop_id=17), now_utc=_utc(18, 40))

    outlook = project(store, catalog, now_utc=_utc(18, 40), later_limit=50)
    item = outlook.active
    assert item is not None
    assert item.source_type == "LOCAL_NET"
    assert item.commandable is False
    assert item.local_net_schedule_id == "active"
    assert item.occurrence_id.startswith("active|")
    assert item.group_id == "group_synthetic"
    assert item.frequency_resource_id == "frequency_synthetic"
    assert item.reminder_minutes == 15
    assert "Synthetic Net active" in item.what_text
    assert item.why_text
    # An immutable projection cannot be changed by a caller after rendering.
    with pytest.raises((AttributeError, TypeError)):
        item.commandable = True  # type: ignore[misc]


def test_projection_orders_active_next_and_caps_later_at_fifty(nets_db: Path) -> None:
    _, project = _projection_api()
    store = LocalNetStore(nets_db)
    catalog = _catalog(nets_db)
    now = _utc(12)
    store.save_schedule(_schedule("active", start="11:30"), now_utc=now)
    store.save_schedule(_schedule("next", start="12:30"), now_utc=now)
    for index in range(100):
        hour = 13 + (index % 10)
        day = index // 10
        schedule = _schedule(f"later_{index:03d}", start=f"{hour:02d}:00")
        if day:
            schedule_values = {
                field.name: getattr(schedule, field.name)
                for field in fields(LocalNetSchedule)
            }
            schedule_values.update({
                "effective_start_date": (datetime(2026, 1, 7) + timedelta(days=day)).date().isoformat(),
            })
            schedule = LocalNetSchedule(**schedule_values)
        store.save_schedule(schedule, now_utc=now)

    outlook = project(store, catalog, now_utc=now, later_limit=50)
    assert outlook.active is not None and outlook.active.local_net_schedule_id == "active"
    assert outlook.next is not None and outlook.next.local_net_schedule_id == "next"
    assert len(outlook.later) == 50
    assert outlook.total_upcoming >= 50
    starts = [outlook.next.start_utc, *(row.start_utc for row in outlook.later)]
    assert starts == sorted(starts)


@pytest.mark.parametrize(
    ("now", "expected"),
    [(_utc(18, 30), "30"), (_utc(18, 45), "15")],
)
def test_projection_urgency_has_textual_30_15_semantics(
    nets_db: Path, now: datetime, expected: str
) -> None:
    _, project = _projection_api()
    store = LocalNetStore(nets_db)
    catalog = _catalog(nets_db)
    store.save_schedule(_schedule("urgency", start="19:00"), now_utc=now)
    outlook = project(store, catalog, now_utc=now, later_limit=50)
    item = outlook.next
    assert item is not None
    assert expected in str(item.urgency)
    assert item.urgency_text
    assert "Starts in" in item.why_text


def test_dismissed_occurrence_is_removed_without_disabling_future_recurrence(nets_db: Path) -> None:
    _, project = _projection_api()
    store = LocalNetStore(nets_db)
    catalog = _catalog(nets_db)
    now = _utc(18, 40)
    schedule = _schedule("dismissible", start="18:30")
    store.save_schedule(schedule, now_utc=now)
    first = store.upcoming(now, horizon_days=2, limit=10)[0]
    store.dismiss_occurrence(first, note="Synthetic operator dismissal")

    outlook = project(store, catalog, now_utc=now, later_limit=50)
    assert outlook.active is None
    assert outlook.next is not None
    assert outlook.next.local_net_schedule_id == "dismissible"
    assert outlook.next.occurrence_id != first.occurrence_key
    assert store.get_schedule("dismissible").enabled is True  # type: ignore[union-attr]


def test_projection_actions_exclude_qsy_radio_and_scheduler_metadata(nets_db: Path) -> None:
    projection, project = _projection_api()
    store = LocalNetStore(nets_db)
    catalog = _catalog(nets_db)
    store.save_schedule(_schedule("actions", start="19:00", sop_id=23), now_utc=_utc(18))
    outlook = project(store, catalog, now_utc=_utc(18), later_limit=50)
    item = outlook.next
    assert item is not None
    payload = vars(item) if hasattr(item, "__dict__") else {
        field: getattr(item, field) for field in getattr(item, "__dataclass_fields__", {})
    }
    assert "qsy" not in repr(payload).lower()
    assert "scheduler" not in repr(payload).lower()
    assert item.action_metadata == {}
    assert "SchedulerEngine" not in inspect.getsource(projection)


def test_local_net_sop_intent_is_contextual_stable_and_returns_to_occurrence() -> None:
    projection, _ = _projection_api()
    builder = getattr(projection, "build_local_net_sop_intent", None)
    if builder is None:
        pytest.fail("LN-5 requires build_local_net_sop_intent")
    intent = builder(
        local_net_schedule_key="schedule_synthetic",
        occurrence_key="schedule_synthetic|2026-01-07T19:00:00Z",
        net_session_key="session_synthetic",
        sop_id=41,
        return_route="ops.schedule_outlook",
        return_scroll_y=128,
    )
    assert isinstance(intent, NavigationIntent)
    assert intent.destination_route == "sop.context"
    assert intent.return_route == "ops.schedule_outlook"
    assert intent.local_net_schedule_id == "schedule_synthetic"
    assert intent.directory_session_ids == ("session_synthetic",)
    assert intent.sop_id == 41
    assert intent.return_selection_key == "schedule_synthetic|2026-01-07T19:00:00Z"
    assert intent.return_scroll_y == 128
    assert intent.draft_snapshot["occurrence_key"] == "schedule_synthetic|2026-01-07T19:00:00Z"


def test_sop_context_is_preview_only_and_bypasses_command_conflict_paths() -> None:
    projection, _ = _projection_api()
    source = inspect.getsource(projection)
    lowered = source.lower()
    assert "activate" not in lowered
    assert "rf guard" not in lowered
    assert "_schedule_qsy_meta" not in lowered
    assert "schedulerengine" not in lowered


def test_ops_local_net_section_is_collapsible_and_bounded() -> None:
    controlfreq = Path(__file__).parents[1] / "freqinout" / "gui" / "controlfreq_tab.py"
    source = controlfreq.read_text(encoding="utf-8")
    lowered = source.lower()
    assert "local net" in lowered
    assert "collaps" in lowered
    assert "later" in lowered
    assert "50" in source
    assert "local_net" in lowered


def test_collapsed_ops_local_net_section_does_not_rebuild_presentation() -> None:
    module = pytest.importorskip("freqinout.gui.controlfreq_tab")
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    tab = module.ControlFreqTab()
    try:
        tab.local_nets_outlook_toggle.setChecked(False)
        tab._local_nets_outlook_rendered_revision = 77
        tab.set_local_nets_outlook_items((object(),))
        # Data may update while hidden, but no row widgets or presentation
        # refresh work is allowed until the section is shown again.
        assert tab._local_nets_outlook_rendered_revision == 77
    finally:
        tab.close()
        tab.deleteLater()
        app.processEvents()


@pytest.mark.parametrize("theme", ["light", "dark"])
@pytest.mark.parametrize("scale", [1.0, 1.35])
def test_ops_viewport_theme_and_large_text_seam(theme: str, scale: float) -> None:
    module = pytest.importorskip("freqinout.gui.controlfreq_tab")
    from PySide6.QtWidgets import QApplication, QScrollArea
    from freqinout.gui.theme import apply_app_theme, get_theme

    app = QApplication.instance() or QApplication([])
    apply_app_theme(app, get_theme(theme), ui_text_scale=scale)
    # Construction is deliberately isolated from live radio/configuration
    # state; the existing tab owns its own safe startup defaults.
    tab = module.ControlFreqTab()
    try:
        tab.resize(900, 560)
        tab.show()
        app.processEvents()
        assert tab.width() == 900
        assert tab.height() == 560
        for scroll in tab.findChildren(QScrollArea):
            assert scroll.horizontalScrollBar().maximum() == 0
    finally:
        tab.close()
        tab.deleteLater()
