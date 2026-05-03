from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.gui.daily_schedule_tab import DailyScheduleTab
from freqinout.gui.fldigi_net_control_tab import FldigiNetControlTab
from freqinout.gui.message_viewer_tab import MessageViewerTab


def test_messages_activation_refresh_loads_local_snapshot_then_schedules_maintenance() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    seen: list[tuple[str, object]] = []
    tab.settings = None
    tab._activation_refresh_pending = True
    tab._load_paths_lists = lambda: seen.append(("paths", None))
    tab._activation_maintenance_due = lambda now=None: True
    tab._load_message_sources_from_local = lambda **kwargs: seen.append(("local", dict(kwargs)))
    tab._populate_messages_table = lambda **kwargs: seen.append(("populate", dict(kwargs)))
    tab._refresh_pending_backlog = lambda: seen.append(("pending", None))
    tab._schedule_activation_maintenance = lambda: seen.append(("schedule", None))
    tab._set_loading = lambda active, text="Getting messages...": seen.append(("loading", (bool(active), str(text))))
    tab._last_activation_refresh_ts = 0.0

    MessageViewerTab._run_activation_refresh(tab, force=False)

    assert seen[:5] == [
        ("paths", None),
        ("local", {"force": False}),
        ("populate", {"force": False}),
        ("pending", None),
        ("schedule", None),
    ]
    assert seen[-1] == ("loading", (False, "Getting messages..."))
    assert tab._activation_refresh_pending is False


def test_messages_deferred_activation_maintenance_runs_heavy_refresh() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    seen: list[tuple[str, object]] = []
    tab.settings = None
    tab._activation_maintenance_pending = True
    tab._activation_maintenance_inflight = False
    tab._schedule_activation_maintenance = lambda: seen.append(("reschedule", None))
    tab._set_loading = lambda active, text="Getting messages...": seen.append(("loading", (bool(active), str(text))))
    tab._run_message_activation_maintenance = lambda **kwargs: seen.append(("maint", dict(kwargs)))

    MessageViewerTab._run_deferred_activation_maintenance(tab)

    assert tab._activation_maintenance_pending is False
    assert tab._activation_maintenance_inflight is False
    assert seen == [
        ("loading", (True, "Refreshing message sources...")),
        ("maint", {"force": False}),
        ("loading", (False, "Getting messages...")),
    ]


def test_daily_schedule_activation_defers_secondary_refresh() -> None:
    tab = DailyScheduleTab.__new__(DailyScheduleTab)
    seen: list[str] = []
    tab.settings = None
    tab.table = type("_Table", (), {"rowCount": lambda self: 0})()
    tab._refresh_sop_overlay_rows_in_table = lambda: seen.append("overlay")
    tab._update_effective_source_label = lambda *args, **kwargs: seen.append("label")
    tab._update_suspend_state = lambda *args, **kwargs: seen.append("suspend")
    tab._schedule_activation_secondary_refresh = lambda: seen.append("schedule")

    DailyScheduleTab.on_tab_activated(tab)

    assert seen == ["overlay", "label", "suspend", "schedule"]


def test_daily_schedule_deferred_activation_refresh_runs_once() -> None:
    tab = DailyScheduleTab.__new__(DailyScheduleTab)
    seen: list[tuple[str, object]] = []
    tab._activation_secondary_refresh_pending = True
    tab._activation_secondary_refresh_inflight = False
    tab._schedule_activation_secondary_refresh = lambda: seen.append(("reschedule", None))
    tab._refresh_sop_profiles_panel = lambda **kwargs: seen.append(("panel", dict(kwargs)))
    tab._refresh_schedule_resources = lambda **kwargs: seen.append(("resources", dict(kwargs)))

    DailyScheduleTab._run_activation_secondary_refresh(tab)

    assert tab._activation_secondary_refresh_pending is False
    assert tab._activation_secondary_refresh_inflight is False
    assert seen == [
        ("panel", {"force": True}),
        ("resources", {"force": False}),
    ]


def test_fldigi_ncs_activation_defers_secondary_refresh() -> None:
    tab = FldigiNetControlTab.__new__(FldigiNetControlTab)
    seen: list[str] = []
    tab.settings = None
    tab._update_clock_labels = lambda: seen.append("clock")
    tab._update_suspend_state = lambda *args, **kwargs: seen.append("suspend")
    tab._update_next_change_display = lambda: seen.append("next")
    tab._schedule_activation_secondary_refresh = lambda: seen.append("schedule")

    FldigiNetControlTab.on_tab_activated(tab)

    assert seen == ["clock", "suspend", "next", "schedule"]


def test_fldigi_ncs_deferred_activation_refresh_runs_once() -> None:
    tab = FldigiNetControlTab.__new__(FldigiNetControlTab)
    seen: list[str] = []
    tab._activation_secondary_refresh_pending = True
    tab._activation_secondary_refresh_inflight = False
    tab._schedule_activation_secondary_refresh = lambda: seen.append("reschedule")
    tab._maybe_reload_operating_groups = lambda: seen.append("groups")
    tab._refresh_macro_profile_choices = lambda: seen.append("macros")
    tab._poll_log_assisted_intake = lambda: seen.append("poll")

    FldigiNetControlTab._run_activation_secondary_refresh(tab)

    assert tab._activation_secondary_refresh_pending is False
    assert tab._activation_secondary_refresh_inflight is False
    assert seen == ["groups", "macros", "poll"]
