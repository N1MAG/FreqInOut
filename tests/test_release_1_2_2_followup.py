from __future__ import annotations

import csv
import datetime as dt
import os
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.gui import qsy_helper
from freqinout.gui.controlfreq_tab import ControlFreqTab
from freqinout.gui.message_viewer_tab import MessageViewerTab
from freqinout.gui.operator_history_tab import OperatorHistoryTab
from freqinout.gui.settings_tab import _vault_location_requires_code_badge
from freqinout.gui.stations_map_tab import StationsMapTab
from freqinout.radio_interface.js8_status import VarACStatusClient


class _MemorySettings:
    def __init__(self) -> None:
        self._data: dict[str, object] = {}

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def set(self, key: str, value) -> None:
        self._data[key] = value

    def reload(self) -> None:
        return None


class _FakeScheduler:
    def __init__(self) -> None:
        self.minutes: list[int] = []

    def suspend_schedule(self, minutes: int) -> None:
        self.minutes.append(int(minutes))


def _app():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication([])


def test_set_active_hold_duration_rewrites_active_hold_cache():
    settings = _MemorySettings()
    scheduler = _FakeScheduler()
    window = SimpleNamespace(scheduler=scheduler)

    mins = qsy_helper.set_active_hold_duration(window, settings, 90, notify=False)

    assert mins == 90
    assert scheduler.minutes == [90]
    snapshot = qsy_helper.suspend_snapshot(settings, allow_reload=False)
    assert snapshot["active"] is True
    assert int(snapshot["remaining_minutes"] or 0) >= 89


def test_scheduler_status_summary_reports_next_transition_frequency(monkeypatch, tmp_path):
    app = _app()
    assert app is not None

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from freqinout.core.scheduler_engine import SchedulerEngine

    engine = SchedulerEngine()
    try:
        monkeypatch.setattr(engine, "_control_mode", lambda: "MANUAL")
        monkeypatch.setattr(engine, "_current_rig_frequency", lambda **_kwargs: 14_115_000)
        monkeypatch.setattr(
            engine,
            "_off_schedule_flags",
            lambda *args, **kwargs: {"frequency": False, "mode": False, "offset": False, "fldigi_offset": False},
        )
        monkeypatch.setattr(engine, "_varac_status", lambda: {"waiting_for_frequency": False, "busy": False})
        monkeypatch.setattr(engine, "_js8_running", lambda: False)
        engine.current_source = "HF"
        engine.current_schedule_entry = {"frequency": "14.115", "group_name": "ALPHA"}
        engine._next_source = "HF"
        engine._next_net_kind = "HF Schedule"
        engine._next_transition_freq_hz = 7_115_000

        summary = engine.get_status_summary(live=True)
        assert summary["next_frequency_label"] == "7.115"
        assert summary["next_frequency_mhz"] == pytest.approx(7.115)
    finally:
        engine.stop()
        engine.deleteLater()


def test_station_health_marks_stale_ok_checks_as_informational_not_alerts(monkeypatch):
    from freqinout.core import station_health_summary as summary_module

    now = 100_000.0
    monkeypatch.setattr(summary_module.time, "monotonic", lambda: now)
    stale_ts = now - (23 * 3600)
    summary = summary_module.summarize_station_health(
        {
            "fldigi:127.0.0.1:7362": {
                "key": "fldigi:127.0.0.1:7362",
                "owner": "test",
                "consecutive_failures": 0,
                "consecutive_slow": 0,
                "last_success_ts": stale_ts,
                "last_failure_ts": 0.0,
                "issue_started_ts": 0.0,
                "last_checked_ts": stale_ts,
                "last_duration_ms": 3.0,
                "cooldown_remaining_sec": 0.0,
                "degraded": False,
                "last_error": "",
                "metadata": {},
            }
        }
    )

    item = summary["items"][0]
    assert summary["issue_count"] == 0
    assert item["state"] == "Not recent"
    assert item["severity"] == "info"
    assert "has not needed a fresh check" in item["action"].lower()


def test_station_health_marks_cooldown_backoff_as_informational_not_alerts(monkeypatch):
    from freqinout.core import station_health_summary as summary_module

    now = 100_000.0
    monkeypatch.setattr(summary_module.time, "monotonic", lambda: now)
    summary = summary_module.summarize_station_health(
        {
            "js8call:127.0.0.1:2442": {
                "key": "js8call:127.0.0.1:2442",
                "owner": "FIO-A",
                "consecutive_failures": 0,
                "consecutive_slow": 0,
                "last_success_ts": now - 4,
                "last_failure_ts": 0.0,
                "issue_started_ts": 0.0,
                "last_checked_ts": now - 4,
                "last_duration_ms": 4.0,
                "cooldown_remaining_sec": 12.0,
                "degraded": False,
                "last_error": "",
                "metadata": {
                    "action": (
                        "JS8Call API is reachable at 127.0.0.1:2442; "
                        "FIO will use basic API features and keep fallbacks available."
                    )
                },
            }
        }
    )

    item = summary["items"][0]
    assert summary["issue_count"] == 0
    assert item["state"] == "Retry waiting"
    assert item["severity"] == "info"
    assert item["is_issue"] is False
    assert "JS8Call API is reachable" in item["action"]


def test_station_health_marks_js8_basic_api_as_informational_even_after_probe_failure():
    from freqinout.core.station_health_summary import summarize_station_health

    now = time.monotonic()
    summary = summarize_station_health(
        {
            "js8call:127.0.0.1:2442:capability": {
                "owner": "SoftwareStatusService",
                "consecutive_failures": 1,
                "consecutive_slow": 0,
                "last_checked_ts": now - 2,
                "last_error": "",
                "metadata": {
                    "capability_mode": "api_basic",
                    "endpoint": "127.0.0.1:2442",
                    "action": (
                        "JS8Call API is reachable at 127.0.0.1:2442; "
                        "FIO will use basic API features and keep fallbacks available."
                    ),
                },
            }
        }
    )

    item = summary["items"][0]
    assert summary["issue_count"] == 0
    assert item["dependency"] == "JS8Call API (127.0.0.1:2442)"
    assert item["state"] == "Ready (basic)"
    assert item["severity"] == "info"
    assert item["is_issue"] is False
    assert "JS8Call API is reachable" in item["action"]


def test_station_health_keeps_ingest_waiting_rows_informational_without_error_detail():
    from freqinout.core.station_health_summary import summarize_station_health

    now = time.monotonic()
    summary = summarize_station_health(
        {
            "ingest_source:meshcore": {
                "owner": "IngestRuntime",
                "consecutive_failures": 1,
                "consecutive_slow": 0,
                "last_checked_ts": now - 6,
                "last_error": "",
                "metadata": {},
            }
        }
    )

    item = summary["items"][0]
    assert summary["issue_count"] == 0
    assert item["dependency"] == "Ingest Source"
    assert item["state"] == "Waiting"
    assert item["severity"] == "info"
    assert item["is_issue"] is False


def test_station_health_keeps_generated_ingest_source_ids_informational_without_error_detail():
    from freqinout.core.station_health_summary import summarize_station_health

    now = time.monotonic()
    summary = summarize_station_health(
        {
            "ingest_app_js8call_7_8_127.0.0.1_2442_abcd1234": {
                "owner": "BackgroundIngest",
                "consecutive_failures": 1,
                "consecutive_slow": 0,
                "last_checked_ts": now - 10,
                "last_error": "",
                "metadata": {
                    "family": "js8call",
                    "source_type": "api",
                    "label": "FIO-A JS8Call API",
                },
            }
        }
    )

    item = summary["items"][0]
    assert summary["issue_count"] == 0
    assert item["dependency"].startswith("Ingest Source")
    assert item["state"] == "Waiting"
    assert item["severity"] == "info"
    assert item["is_issue"] is False


def test_station_health_keeps_placeholder_ingest_source_errors_informational():
    from freqinout.core.station_health_summary import summarize_station_health

    now = time.monotonic()
    summary = summarize_station_health(
        {
            "ingest_app_js8call_7_8_127.0.0.1_2442_abcd1234": {
                "owner": "BackgroundIngest",
                "consecutive_failures": 1,
                "consecutive_slow": 0,
                "last_checked_ts": now - 10,
                "last_error": "failure",
                "metadata": {
                    "family": "js8call",
                    "source_type": "api",
                    "label": "FIO-A JS8Call API",
                },
            }
        }
    )

    item = summary["items"][0]
    assert summary["issue_count"] == 0
    assert item["state"] == "Waiting"
    assert item["severity"] == "info"
    assert item["last_issue"] == ""
    assert item["is_issue"] is False


def test_station_health_keeps_actionable_ingest_source_errors_as_warnings():
    from freqinout.core.station_health_summary import summarize_station_health

    now = time.monotonic()
    summary = summarize_station_health(
        {
            "ingest_app_js8call_7_8_127.0.0.1_2442_abcd1234": {
                "owner": "BackgroundIngest",
                "consecutive_failures": 1,
                "consecutive_slow": 0,
                "last_checked_ts": now - 10,
                "last_error": "source path missing",
                "metadata": {
                    "family": "js8call",
                    "source_type": "file",
                    "label": "FIO-A DIRECTED.TXT",
                },
            }
        }
    )

    item = summary["items"][0]
    assert summary["issue_count"] == 1
    assert item["state"] == "Warning"
    assert item["severity"] == "warning"
    assert item["is_issue"] is True
    assert item["last_issue"] == "source path missing"


def test_linux_installer_running_detector_is_scoped_and_diagnostic():
    source = Path("install_FreqInOut_linux.sh").read_text(encoding="utf-8")
    detector = source[source.index("is_freqinout_running()") : source.index("ensure_app_not_running_for_update()")]

    assert 'FIO_INSTALL_DIR="$INSTALL_DIR" python3' in detector
    assert "Matched process(es):" in source
    assert "pytest" in detector
    assert "release_preflight.py" in detector
    assert "compileall" in detector
    assert "codex" in detector
    assert "root in path.parents" in detector
    assert "freqinout.main" in detector
    assert "install_freqinout_linux.sh" in detector
    assert "uninstall_freqinout_linux.sh" in detector


def test_inactive_scheduler_busy_health_rows_are_ok_not_alerts():
    from freqinout.core.station_health_summary import summarize_station_health

    now = time.monotonic()
    summary = summarize_station_health(
        {
            "scheduler:fldigi-busy": {
                "key": "scheduler:fldigi-busy",
                "owner": "SchedulerEngine",
                "consecutive_failures": 5,
                "consecutive_slow": 0,
                "last_success_ts": 0.0,
                "last_failure_ts": now,
                "issue_started_ts": now - 60,
                "last_checked_ts": now,
                "last_duration_ms": 0.0,
                "cooldown_remaining_sec": 0.0,
                "degraded": True,
                "last_error": "holding schedule change for FLDigi RX activity",
                "metadata": {"action": "holding schedule change for FLDigi RX activity"},
            }
        }
    )

    item = summary["items"][0]
    assert item["dependency"] == "Scheduler hold: FLDigi RX activity"
    assert item["state"] == "OK"
    assert item["severity"] == "ok"
    assert item["action"] == "No scheduled frequency change is waiting on this activity check."


def test_runtime_observability_items_are_operator_readable_without_false_alerts():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        station_poll_metrics={
            "polls_started": 4,
            "polls_succeeded": 4,
            "polls_failed": 0,
            "cache_hits": 12,
            "snapshot_count": 2,
        },
        background_job_status={
            "running": True,
            "queued_jobs": {},
            "realtime_jobs": {},
            "skipped_counts": {"messages": 1},
            "skip_reasons": {"messages": "unchanged"},
            "source_skip_reasons": {
                "ingest:js8:source:inbox": {
                    "reason": "backoff",
                    "label": "FIO-A JS8Call DIRECTED",
                    "family": "js8call",
                    "source_type": "js8-inbox",
                    "cooldown_remaining_sec": 12,
                }
            },
            "refresh_skip_reasons": {"messages": "unchanged"},
            "refresh_decisions": {
                "messages": {
                    "should_run": False,
                    "reason": "unchanged",
                    "elapsed_sec": 22,
                    "fingerprint_size": 2,
                }
            },
            "timeout_warned": (),
        },
        js8_registry_status=[
            {
                "key": "127.0.0.1:2442",
                "name": "FIO-A JS8Call",
                "running": False,
                "connected": False,
                "listener_count": 0,
                "pending_request_count": 0,
                "queued_event_count": 0,
            }
        ],
    )
    summary = summarize_station_health({}, extra_items=items)

    assert summary["issue_count"] == 0
    labels = {item["dependency"]: item for item in summary["items"]}
    assert labels["Station runtime polling"]["action"] == "4 polls, 4 ok, 12 cache hits, 2 cached snapshots"
    assert labels["Background ingest controller"]["action"] == (
        "Idle; 1 trigger skipped (Messages: unchanged); Latest: Messages: no source changes"
    )
    assert labels["Background ingest sources"]["state"] == "Observed"
    assert labels["Background ingest sources"]["severity"] == "info"
    assert "FIO-A JS8Call DIRECTED: Retry in 12s" in labels["Background ingest sources"]["action"]
    assert labels["Shared JS8Call API client"]["state"] == "Idle"


def test_runtime_observability_items_surface_stale_scheduler_companions():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        scheduler_companion_status={
            "rf_conflict_warning": True,
            "rf_conflict_summary": "RF Guard: verify FIO-B",
            "rf_conflict_detail": "FIO-B shares NORTH MAST.",
            "rf_conflict_peer_name": "FIO-B",
            "rf_conflict_peer_status_unknown": True,
            "rf_conflict_peer_status_detail": "peer status is unknown",
            "js8_status_stale": True,
            "js8_status_detail": "JS8Call API timeout",
            "varac_status_stale": True,
            "varac_status_detail": "VarAC log scan is stale",
        }
    )
    summary = summarize_station_health({}, extra_items=items)

    assert summary["issue_count"] == 3
    labels = {item["dependency"]: item for item in summary["items"]}
    assert labels["RF Guard"]["state"] == "Verify"
    assert labels["RF Guard"]["action"] == "Review RF Guard before changing frequency"
    assert labels["RF Guard"]["last_issue"] == "FIO-B shares NORTH MAST.; peer status is unknown"
    assert labels["JS8Call status"]["state"] == "Verify"
    assert labels["JS8Call status"]["severity"] == "warning"
    assert labels["JS8Call status"]["action"] == "Verify JS8Call status"
    assert labels["JS8Call status"]["last_issue"] == "JS8Call API timeout"
    assert labels["VarAC status"]["action"] == "Verify VarAC status"
    assert labels["VarAC status"]["last_issue"] == "VarAC log scan is stale"


def test_runtime_observability_items_surface_assigned_schedule_rf_guard_status():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        assigned_schedule_status=[
            {
                "device_profile_id": 7,
                "device_name": "FIO-B",
                "frequency_plan_id": 12,
                "frequency_plan_name": "County HF Daily",
                "validation_status_json": {
                    "state": "blocked",
                    "blocked": ["FIO-B antenna support does not include 40M for County HF Daily."],
                },
            }
        ]
    )
    summary = summarize_station_health({}, extra_items=items)

    assert summary["severity"] == "danger"
    assert summary["issue_count"] == 1
    item = summary["issue_items"][0]
    assert item["dependency"] == "Schedule Assignment RF Guard"
    assert item["scope"] == "FIO-B"
    assert item["state"] == "Blocked"
    assert item["action"] == "Review assigned Frequency Plan before schedule changes"
    assert item["last_issue"] == "FIO-B / County HF Daily: FIO-B antenna support does not include 40M for County HF Daily."


def test_main_window_feeds_assigned_schedule_status_to_station_health_runtime_items():
    source = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    runtime_block = source[
        source.index("def _station_health_runtime_items")
        : source.index("def _station_health_runtime_source_rows")
    ]

    assert "assigned_schedule_status = self._station_health_assigned_schedule_status_rows()" in runtime_block
    assert "assigned_schedule_status=assigned_schedule_status" in runtime_block
    assert "def _station_health_assigned_schedule_status_rows(self)" in source
    assert "store.list_effective_assigned_plans()" in source
    assert "validation_status_json" not in runtime_block


def test_station_health_sidebar_badge_uses_lightweight_runtime_observability_items(monkeypatch):
    from freqinout.gui import main_window

    captured = {}

    def fake_summary(*, include_ok=True, extra_items=None, **_kwargs):
        captured["include_ok"] = include_ok
        captured["extra_items"] = list(extra_items or [])
        issue_items = [item for item in captured["extra_items"] if item.get("is_issue")]
        return {
            "issue_count": len(issue_items),
            "severity": "warning" if issue_items else "ok",
            "issue_items": issue_items,
        }

    runtime_issue = {
        "key": "runtime:ingest:source-view",
        "scope": "Station-wide",
        "dependency": "Runtime ingest sources",
        "state": "Warning",
        "severity": "warning",
        "action": "1 source needs attention",
        "is_issue": True,
    }
    monkeypatch.setattr(main_window, "summarize_station_health", fake_summary)
    window = main_window.MainWindow.__new__(main_window.MainWindow)
    window._ui_refresh_allowed = lambda: True
    window._station_health_alert_extra_items = lambda: [runtime_issue]
    window._station_health_nav_index = None
    window._update_ncs_nav_button_styles = lambda: None

    window._refresh_station_health_alert()

    assert captured["include_ok"] is False
    assert captured["extra_items"] == [runtime_issue]
    assert window._station_health_alert_summary["issue_count"] == 1
    assert window._station_health_alert_summary["issue_items"] == [runtime_issue]


def test_station_health_runtime_items_keep_full_detail_path():
    from freqinout.gui import main_window

    captured = {}
    window = main_window.MainWindow.__new__(main_window.MainWindow)

    def fake_extra_items(**kwargs):
        captured.update(kwargs)
        return []

    window._station_health_extra_items = fake_extra_items

    assert window._station_health_runtime_items() == []
    assert captured == {
        "include_assigned_schedules": True,
        "include_runtime_sources": True,
        "include_sop_audit": True,
    }


def test_runtime_observability_items_explain_forced_background_refresh_decision():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        background_job_status={
            "running": True,
            "queued_jobs": {},
            "realtime_jobs": {},
            "skipped_counts": {},
            "refresh_decisions": {
                "messages": {
                    "should_run": True,
                    "reason": "forced",
                    "elapsed_sec": 8,
                    "fingerprint_size": 4,
                },
                "js8_links": {
                    "should_run": True,
                    "reason": "source-changed",
                    "elapsed_sec": 1,
                    "fingerprint_size": 2,
                },
            },
            "timeout_warned": (),
        },
    )
    summary = summarize_station_health({}, extra_items=items)

    item = next(item for item in summary["items"] if item["dependency"] == "Background ingest controller")
    assert item["state"] == "OK"
    assert item["action"] == (
        "Idle; Latest: JS8 links: source changed, refresh queued; Messages: manual refresh queued"
    )


def test_runtime_observability_items_surface_active_js8_and_ingest_warnings():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        background_job_status={
            "timeout_warned": ("messages",),
        },
        js8_registry_status=[
            {
                "key": "127.0.0.1:2442",
                "name": "FIO-A JS8Call",
                "running": True,
                "connected": False,
                "last_error": "connect_failed:refused",
            }
        ],
    )
    summary = summarize_station_health({}, extra_items=items)

    assert summary["issue_count"] == 2
    issues = {item["dependency"]: item for item in summary["issue_items"]}
    assert issues["Background ingest controller"]["state"] == "Warning"
    assert issues["Shared JS8Call API client"]["last_issue"] == "connect_failed:refused"


def test_runtime_observability_items_surface_missing_ingest_source_warning():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        background_job_status={
            "source_skip_reasons": {
                "ingest:spotter:missing": {
                    "reason": "missing",
                    "label": "FIO-B JS8Spotter DIRECTED",
                    "family": "js8call",
                    "source_type": "spotter-directed",
                    "path": "/missing/DIRECTED.TXT",
                }
            },
        }
    )
    summary = summarize_station_health({}, extra_items=items)

    assert summary["issue_count"] == 1
    issue = summary["issue_items"][0]
    assert issue["dependency"] == "Background ingest sources"
    assert issue["state"] == "Warning"
    assert "FIO-B JS8Spotter DIRECTED: Path not found" in issue["action"]
    assert issue["last_issue"] == "One or more ingest sources could not be read."


def test_runtime_observability_items_include_runtime_source_view_summary():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        runtime_source_rows=[
            {
                "source_id": "api-a",
                "title": "FIO-A JS8Call API",
                "source_kind": "JS8Call API",
                "state": "idle",
                "state_label": "Idle",
                "severity": "info",
                "projection_count": 2,
            },
            {
                "source_id": "api-b",
                "title": "FIO-B JS8Call API",
                "source_kind": "JS8Call API",
                "state": "shared_endpoint",
                "state_label": "Shared Endpoint",
                "severity": "warning",
                "projection_count": 0,
            },
            {
                "source_id": "commstat",
                "title": "CommStat reports",
                "source_kind": "CommStat",
                "state": "ready",
                "state_label": "Ready",
                "severity": "ok",
                "projection_count": 4,
            },
            {
                "source_id": "directed-a",
                "title": "FIO-A JS8Call DIRECTED",
                "source_kind": "JS8Call DIRECTED.TXT",
                "state": "ready",
                "state_label": "Ready",
                "severity": "ok",
                "metadata": {"link_projection_summary": {"total": 12, "station_pairs": 5}},
            },
        ]
    )
    summary = summarize_station_health({}, extra_items=items)

    assert summary["issue_count"] == 1
    issue = summary["issue_items"][0]
    assert issue["dependency"] == "Runtime ingest sources"
    assert "FIO-A JS8Call API: Idle" in issue["action"]
    assert "FIO-B JS8Call API: Shared Endpoint" in issue["action"]
    assert "Projected Data: 2 messages, 4 artifacts, 12 links / 5 pairs" in issue["action"]


def test_runtime_observability_items_keep_backoff_detail_out_of_projected_data():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        runtime_source_rows=[
            {
                "source_id": "spotter-a",
                "title": "FIO-A JS8Spotter DIRECTED",
                "source_kind": "JS8Spotter DIRECTED.TXT",
                "state": "backoff",
                "state_label": "Backoff",
                "severity": "info",
                "detail": "Retry in 12s",
                "projection_count": 0,
            },
        ]
    )
    summary = summarize_station_health({}, extra_items=items)

    labels = [item for item in summary["items"] if item["dependency"] == "Runtime ingest sources"]
    assert len(labels) == 1
    assert "FIO-A JS8Spotter DIRECTED: Backoff" in labels[0]["action"]
    assert "Projected Data:" not in labels[0]["action"]


def test_runtime_observability_prefers_runtime_source_row_over_duplicate_background_skip():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        background_job_status={
            "source_skip_reasons": {
                "ingest:spotter-a": {
                    "source_id": "spotter-a",
                    "label": "FIO-A JS8Spotter DIRECTED",
                    "family": "js8call",
                    "source_type": "spotter-directed",
                    "reason": "missing",
                    "path": "/missing/DIRECTED.TXT",
                }
            }
        },
        runtime_source_rows=[
            {
                "source_id": "spotter-a",
                "title": "FIO-A JS8Spotter DIRECTED",
                "source_kind": "JS8Spotter DIRECTED.TXT",
                "state": "missing",
                "state_label": "Missing",
                "severity": "warning",
                "location": "/missing/DIRECTED.TXT",
            }
        ],
    )
    summary = summarize_station_health({}, extra_items=items)

    labels = [item["dependency"] for item in summary["items"]]
    assert "Runtime ingest sources" in labels
    assert "Background ingest sources" not in labels


def test_runtime_observability_items_do_not_duplicate_js8_registry_when_runtime_source_covers_endpoint():
    from freqinout.core.station_health_summary import runtime_observability_items, summarize_station_health

    items = runtime_observability_items(
        runtime_source_rows=[
            {
                "source_id": "api-a",
                "title": "FIO-A JS8Call API",
                "source_kind": "JS8Call API",
                "state": "idle",
                "state_label": "Idle",
                "severity": "info",
                "location": "127.0.0.1:2442",
            }
        ],
        js8_registry_status=[
            {
                "key": "127.0.0.1:2442",
                "name": "FIO-A JS8Call",
                "running": False,
                "connected": False,
                "listener_count": 0,
                "pending_request_count": 0,
                "queued_event_count": 0,
            }
        ],
    )
    summary = summarize_station_health({}, extra_items=items)

    labels = [item["dependency"] for item in summary["items"]]
    assert labels.count("Runtime ingest sources") == 1
    assert "Shared JS8Call API client" not in labels


def test_station_health_runtime_sources_drilldown_renders_operator_rows():
    app = _app()
    assert app is not None

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.set_runtime_item_provider(lambda: [])
    tab.set_runtime_source_provider(
        lambda: [
            {
                "source_id": "api-a",
                "title": "FIO-A JS8Call API",
                "source_kind": "JS8Call API",
                "state": "shared_endpoint",
                "state_label": "Shared Endpoint",
                "severity": "warning",
                "location": "127.0.0.1:2442",
                "radio_id": "FIO-A",
                "app_instance_id": "JS8Call",
                "projection_count": 0,
                "action_hint": "Give each JS8Call instance a unique TCP port",
            },
            {
                "source_id": "directed-a",
                "title": "FIO-A JS8Call DIRECTED",
                "source_kind": "JS8Call DIRECTED.TXT",
                "state": "ready",
                "state_label": "Ready",
                "severity": "ok",
                "location": "/tmp/DIRECTED.TXT",
                "metadata": {"link_projection_summary": {"total": 12, "station_pairs": 5}},
            },
        ]
    )

    assert tab.runtime_sources_table.rowCount() == 2
    assert tab.runtime_sources_table.item(0, 0).text() == "FIO-A JS8Call API"
    assert tab.runtime_sources_table.item(0, 1).text() == "Shared Endpoint"
    assert tab.runtime_sources_table.horizontalHeaderItem(2).text() == "Activity"
    assert tab.runtime_sources_table.horizontalHeaderItem(3).text() == "Suggested Fix"
    assert tab.runtime_sources_table.item(0, 3).text() == "Give each JS8Call instance a unique TCP port"
    assert tab.runtime_sources_table.item(1, 2).text() == "12 links / 5 pairs"


def test_station_health_runtime_sources_table_preserves_minimized_readability():
    source = Path("freqinout/gui/station_health_tab.py").read_text(encoding="utf-8")

    runtime_block = source[
        source.index("self.runtime_sources_table = QTableWidget") : source.index("self.runtime_sources_empty_label")
    ]
    assert "setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)" in runtime_block
    assert "runtime_header.setStretchLastSection(True)" in runtime_block
    assert "QHeaderView.Interactive" in runtime_block
    assert "self.runtime_sources_table = QTableWidget(0, 4, runtime_tab)" in runtime_block
    assert "runtime_header.setSectionResizeMode(3, QHeaderView.Stretch)" in runtime_block
    assert "self.health_tabs = QTabWidget(self)" in source
    assert 'self.health_tabs.addTab(runtime_tab, "Runtime Sources")' in source


def test_station_health_runtime_sources_summary_row_focuses_drilldown():
    app = _app()
    assert app is not None

    from freqinout.core.station_health_summary import runtime_observability_items
    from freqinout.gui.station_health_tab import StationHealthTab

    runtime_rows = [
        {
            "source_id": "api-a",
            "title": "FIO-A JS8Call API",
            "source_kind": "JS8Call API",
            "state": "idle",
            "state_label": "Idle",
            "severity": "info",
            "location": "127.0.0.1:2442",
        },
        {
            "source_id": "api-b",
            "title": "FIO-B JS8Call API",
            "source_kind": "JS8Call API",
            "state": "shared_endpoint",
            "state_label": "Shared Endpoint",
            "severity": "warning",
            "location": "127.0.0.1:2442",
        },
    ]
    tab = StationHealthTab()
    tab.set_runtime_item_provider(lambda: runtime_observability_items(runtime_source_rows=runtime_rows))
    tab.set_runtime_source_provider(lambda: runtime_rows)

    summary_row = -1
    for row in range(tab.table.rowCount()):
        item = tab.table.item(row, 1)
        if item is not None and item.text() == "Runtime ingest sources":
            summary_row = row
            break
    assert summary_row >= 0

    tab._focus_runtime_sources_from_health_row(summary_row)

    assert tab.runtime_sources_table.currentRow() == 1
    assert tab.runtime_sources_table.item(1, 1).text() == "Shared Endpoint"


def test_station_health_runtime_source_detail_updates_with_selection():
    app = _app()
    assert app is not None

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.set_runtime_item_provider(lambda: [])
    tab.set_runtime_source_provider(
        lambda: [
            {
                "source_id": "commstat-a",
                "title": "FIO-A CommStat",
                "source_kind": "CommStat",
                "state": "ready",
                "state_label": "Ready",
                "severity": "ok",
                "location": "/tmp/CommStat.db",
                "detail": "Active groups: MAGNET; 3 CommStat artifacts",
                "action_hint": "",
                "last_activity_label": "2 min ago",
                "projection_count": 3,
                "metadata": {"freshness_label": "Fresh"},
            }
        ]
    )

    tab.runtime_sources_table.selectRow(0)
    tab._render_runtime_source_detail()

    text = tab.runtime_source_detail_label.text()
    assert "FIO-A CommStat: Ready" in text
    assert "Kind: CommStat" in text
    assert "Path/Endpoint: /tmp/CommStat.db" in text
    assert "Projected Data: 3 artifacts" in text
    assert "Last Source Decision: 2 min ago" in text
    assert "Freshness: Fresh" in text
    assert "Detail: Active groups: MAGNET; 3 CommStat artifacts" in text


def test_station_health_runtime_source_detail_explains_freshness_timing():
    app = _app()
    assert app is not None

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.set_runtime_item_provider(lambda: [])
    tab.set_runtime_source_provider(
        lambda: [
            {
                "source_id": "directed-a",
                "title": "FIO-A JS8Call DIRECTED",
                "source_kind": "JS8Call DIRECTED.TXT",
                "state": "quiet",
                "state_label": "Quiet",
                "severity": "info",
                "location": "/tmp/DIRECTED.TXT",
                "metadata": {
                    "freshness_state": "quiet",
                    "freshness_label": "Quiet",
                    "freshness_age_sec": 840.0,
                    "freshness_stale_sec": 1800.0,
                },
            }
        ]
    )

    tab.runtime_sources_table.selectRow(0)
    tab._render_runtime_source_detail()

    text = tab.runtime_source_detail_label.text()
    assert "FIO-A JS8Call DIRECTED: Quiet" in text
    assert "Freshness: Quiet - checked 14 min ago - stale after 30 min" in text


def test_station_health_runtime_source_cached_traffic_does_not_echo_backoff_detail():
    app = _app()
    assert app is not None

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.set_runtime_item_provider(lambda: [])
    tab.set_runtime_source_provider(
        lambda: [
            {
                "source_id": "spotter-a",
                "title": "FIO-A Spotter",
                "source_kind": "JS8Spotter DIRECTED.TXT",
                "state": "backoff",
                "state_label": "Backoff",
                "severity": "info",
                "location": "/tmp/DIRECTED.TXT",
                "detail": "Retry in 12s",
                "action_hint": "Waiting before retry",
                "last_activity_label": "Just now",
                "projection_count": 0,
            }
        ]
    )

    assert tab.runtime_sources_table.item(0, 2).text() == "Just now"

    tab.runtime_sources_table.selectRow(0)
    tab._render_runtime_source_detail()

    text = tab.runtime_source_detail_label.text()
    assert "Projected Data:" not in text
    assert "Last Source Decision: Just now" in text
    assert "Detail: Retry in 12s" in text
    assert "Suggested Fix: Waiting before retry" in text


def test_station_health_runtime_source_related_view_emits_settings_context():
    app = _app()
    assert app is not None

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.set_runtime_item_provider(lambda: [])
    tab.set_runtime_source_provider(
        lambda: [
            {
                "source_id": "api-a",
                "title": "FIO-A JS8Call API",
                "source_kind": "JS8Call API",
                "state": "shared_endpoint",
                "state_label": "Shared Endpoint",
                "severity": "warning",
                "location": "127.0.0.1:2442",
                "radio_id": "12",
                "app_instance_id": "JS8Call",
                "action_hint": "Give each JS8Call instance a unique TCP port",
            }
        ]
    )

    emitted = []
    tab.related_view_requested.connect(lambda payload: emitted.append(dict(payload)))

    tab.runtime_sources_table.selectRow(0)
    tab._render_runtime_source_detail()
    assert tab.runtime_source_open_related_btn.isEnabled()

    tab.runtime_source_open_related_btn.click()

    assert emitted
    assert emitted[0]["target"] == "settings"
    assert emitted[0]["settings_nav_context"] == "radios"
    assert emitted[0]["health_key"] == "js8call"
    assert emitted[0]["radio_id"] == "12"
    assert emitted[0]["source_kind"] == "JS8Call API"


def test_station_health_assigned_schedule_rf_guard_row_opens_schedule_assignment():
    app = _app()
    assert app is not None

    from freqinout.core.station_health_summary import runtime_observability_items
    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.set_runtime_item_provider(
        lambda: runtime_observability_items(
            assigned_schedule_status=[
                {
                    "device_profile_id": 12,
                    "frequency_plan_id": 34,
                    "device_name": "FIO-A",
                    "frequency_plan_name": "County Operational Day",
                    "validation": {
                        "state": "blocked",
                        "messages": ["Radio antenna support does not include 40M."],
                    },
                }
            ]
        )
    )
    tab.set_runtime_source_provider(lambda: [])

    target_row = -1
    for row in range(tab.table.rowCount()):
        dep = tab.table.item(row, 1)
        if dep is not None and dep.text() == "Schedule Assignment RF Guard":
            target_row = row
            break
    assert target_row >= 0

    emitted = []
    tab.related_view_requested.connect(lambda payload: emitted.append(dict(payload)))
    tab.table.selectRow(target_row)
    assert tab.health_open_related_btn.isEnabled()

    tab.health_open_related_btn.click()

    assert emitted
    assert emitted[0]["target"] == "settings"
    assert emitted[0]["settings_nav_context"] == "radios"
    assert emitted[0]["health_key"] == "schedule_assignments"


def test_station_health_generic_dependency_row_opens_related_settings_area():
    app = _app()
    assert app is not None

    from freqinout.gui.station_health_tab import StationHealthTab

    tab = StationHealthTab()
    tab.set_runtime_item_provider(lambda: [])
    tab.set_runtime_source_provider(lambda: [])
    tab._last_summary = {
        "issue_count": 1,
        "severity": "danger",
        "items": [
            {
                "scope": "Radio 12",
                "dependency": "JS8Call API (127.0.0.1:2242)",
                "state": "Retry waiting",
                "severity": "warning",
                "action": "Waiting before retry to keep FIO responsive",
                "last_issue": "Connection refused",
                "issue_since": "since 2m ago",
                "cooldown": "30s",
                "last_check": "5s ago",
                "last_duration": "0 ms",
            }
        ],
    }

    emitted = []
    tab.related_view_requested.connect(lambda payload: emitted.append(dict(payload)))
    tab._render_table()
    tab.table.selectRow(0)

    assert tab.health_open_related_btn.isEnabled()
    assert "Connection refused" in tab.health_detail_label.text()

    tab.health_open_related_btn.click()

    assert emitted
    assert emitted[0]["target"] == "settings"
    assert emitted[0]["settings_nav_context"] == "radios"
    assert emitted[0]["health_key"] == "js8call"


def test_station_health_runtime_source_related_view_resolves_radio_label_to_profile_id():
    from freqinout.gui.main_window import MainWindow

    class Store:
        def list_device_profiles(self):
            return [
                {"id": 12, "name": "FIO-A"},
                {"id": 13, "name": "FIO-B"},
            ]

    window = MainWindow.__new__(MainWindow)
    window.multi_radio_store = Store()

    assert window._station_health_runtime_payload_radio_id({"radio_id": "FIO-B"}) == 13
    assert window._station_health_runtime_payload_radio_id({"radio_id": "12"}) == 12
    assert window._station_health_runtime_payload_radio_id({"radio_id": "unknown"}) is None


def test_station_health_runtime_source_rows_include_background_source_skips(monkeypatch):
    from freqinout.gui import main_window

    class Background:
        def job_status_snapshot(self):
            return {
                "source_skip_reasons": {
                    "js8call:source:directed-a": {
                        "source_id": "directed-a",
                        "label": "FIO-A JS8Call DIRECTED",
                        "family": "js8call",
                        "source_type": "file",
                        "radio_id": "FIO-A",
                        "app_instance_id": "JS8Call",
                        "reason": "missing_path",
                        "path": "/tmp/missing/DIRECTED.TXT",
                    }
                }
            }

    monkeypatch.setattr(main_window, "active_runtime_source_view_rows", lambda: ())
    window = main_window.MainWindow.__new__(main_window.MainWindow)
    window.background_ingest = Background()

    rows = window._station_health_runtime_source_rows()

    assert len(rows) == 1
    assert rows[0]["source_id"] == "directed-a"
    assert rows[0]["source_kind"] == "JS8Call DIRECTED.TXT"
    assert rows[0]["state_label"] == "Missing"
    assert rows[0]["location"] == "/tmp/missing/DIRECTED.TXT"


def test_fldigi_busy_watchdog_rechecks_and_overrides_after_three_minutes(monkeypatch, tmp_path):
    app = _app()
    assert app is not None
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    from freqinout.core.scheduler_engine import SchedulerEngine

    engine = SchedulerEngine(fldigi_log=object())
    try:
        recorded_events = []
        monkeypatch.setattr(
            engine,
            "_record_scheduler_event",
            lambda category, reason_code, **kwargs: recorded_events.append((category, reason_code, kwargs)),
        )
        engine._fldigi_busy_watchdog_s = 180.0
        engine._fldigi_busy_entry_key = ("40M", 7_100_000)
        engine._fldigi_busy_since_ts = 1000.0
        engine._fldigi_busy_check_source = "HF"
        engine._fldigi_busy_check_target_hz = 7_100_000
        engine._fldigi_busy_check_result = {
            "busy": True,
            "reason": "text",
            "last_valid_age_s": 1.0,
            "checked_ts": 1181.0,
            "error": None,
        }

        delay, reason = engine._should_delay_for_fldigi(
            entry_key=("40M", 7_100_000),
            source="HF",
            target_frequency_hz=7_100_000,
            want_freq_change=True,
            ignore_fldigi_busy=False,
            now_ts=1181.0,
        )

        assert delay is False
        assert reason is None
        assert engine._fldigi_busy_entry_key is None
        assert engine._fldigi_busy_since_ts is None
        assert any(
            category == "breakaway" and reason_code == "fldigi_busy_breakaway"
            for category, reason_code, _kwargs in recorded_events
        )
    finally:
        engine.stop()
        engine.deleteLater()


def test_controlfreq_next_change_falls_back_to_schedule_outlook(monkeypatch, tmp_path):
    app = _app()
    assert app is not None

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    monkeypatch.setattr(ControlFreqTab, "_refresh_all", lambda self, **_kwargs: None)

    preview_when = dt.datetime(2026, 4, 30, 20, 0, tzinfo=dt.timezone.utc)
    fake_summary = {
        "source": "NONE",
        "net_kind": "",
        "source_reason_detail": "",
        "sop_contention": False,
        "sop_contention_profiles": [],
        "sop_selected_profile": "",
        "next_source": "NONE",
        "next_net_kind": "",
        "next_source_change": False,
        "next_transition_note": "",
        "off_schedule": False,
        "next_frequency_mhz": None,
    }
    fake_scheduler = SimpleNamespace(
        next_change_utc=None,
        get_status_summary=lambda: dict(fake_summary),
    )

    tab = ControlFreqTab()
    try:
        monkeypatch.setattr(tab, "window", lambda: SimpleNamespace(scheduler=fake_scheduler))
        tab._show_local = False
        tab._next_schedule_outlook_preview = {
            "when_utc": preview_when,
            "freq_mhz": 14.115,
            "type": "HF",
            "group": "MAGNET",
        }

        tab._refresh_scheduler_strip(fake_summary)

        assert tab.next_change_label.text() == "Next Change: 14.115 20:00"
        assert "Schedule Outlook fallback" in tab.next_change_label.toolTip()
    finally:
        tab.deleteLater()


def test_operator_history_import_accepts_utf8_bom(monkeypatch, tmp_path):
    _app()

    csv_path = tmp_path / "operators_bom.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=["Callsign", "Name", "State"])
        writer.writeheader()
        writer.writerow({"Callsign": "N0CALL", "Name": "Test Op", "State": "CO"})

    imported_rows: list[dict[str, object]] = []
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QFileDialog.getOpenFileName",
        lambda *args, **kwargs: (str(csv_path), "CSV Files (*.csv)"),
    )
    monkeypatch.setattr(
        OperatorHistoryTab,
        "_choose_import_parent_group",
        lambda self, default_group: ("TEST", True),
    )
    monkeypatch.setattr(
        OperatorHistoryTab,
        "_confirm_roster_import_preview",
        lambda self, result: True,
    )
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QMessageBox.information",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QMessageBox.warning",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(OperatorHistoryTab, "_load_data", lambda self, show_toast=False: None)
    monkeypatch.setattr(OperatorHistoryTab, "_schedule_history_update", lambda self: None)
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.upsert_operator_metadata",
        lambda entries, conn=None: imported_rows.extend(dict(data) for data in entries),
    )

    tab = OperatorHistoryTab()
    try:
        tab._import_csv()
    finally:
        tab.deleteLater()

    assert len(imported_rows) == 1
    assert imported_rows[0]["callsign"] == "N0CALL"
    assert imported_rows[0]["groups_json"] == ["TEST"]


def test_operator_history_import_preview_cancel_stops_before_database_write(monkeypatch, tmp_path):
    _app()

    csv_path = tmp_path / "MAGNET_roster.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["TimeZone", "Region", "Callsign", "Name", "Role", "Tier"])
        writer.writeheader()
        writer.writerow(
            {
                "TimeZone": "Mountain",
                "Region": "MR08",
                "Callsign": "N0CALL",
                "Name": "Test Op",
                "Role": "HUB",
                "Tier": "3",
            }
        )

    imported_rows: list[dict[str, object]] = []
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QFileDialog.getOpenFileName",
        lambda *args, **kwargs: (str(csv_path), "CSV Files (*.csv)"),
    )
    monkeypatch.setattr(
        OperatorHistoryTab,
        "_choose_import_parent_group",
        lambda self, default_group: ("MAGNET", True),
    )
    monkeypatch.setattr(
        OperatorHistoryTab,
        "_confirm_roster_import_preview",
        lambda self, result: False,
    )
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.upsert_operator_metadata",
        lambda entries, conn=None: imported_rows.extend(dict(data) for data in entries),
    )
    monkeypatch.setattr(
        "freqinout.gui.operator_history_tab.QMessageBox.warning",
        lambda *args, **kwargs: None,
    )

    tab = OperatorHistoryTab()
    try:
        tab._import_csv()
    finally:
        tab.deleteLater()

    assert imported_rows == []


def test_message_viewer_treats_k2s_as_transport_form():
    rendered = MessageViewerTab._parse_b2s_form_content(
        ":hdr_fm: N0CALL :hdr_ed: 2026-04-08 12:30Z :prec: R :sub: Checkin :mg: Hello world"
    )

    assert MessageViewerTab._is_transport_form_ext(".b2s") is True
    assert MessageViewerTab._is_transport_form_ext(".k2s") is True
    assert "Routine" in rendered
    assert "Hello world" in rendered


def test_map_marker_group_filter_uses_merged_group_membership():
    dummy = SimpleNamespace(
        operator_rows=[
            {
                "callsign": "N0CALL",
                "state": "CO",
                "group1": "",
                "group2": "",
                "group3": "",
                "groups": ["AMRRON"],
            }
        ]
    )

    StationsMapTab._rebuild_operator_index(dummy)

    assert "AMRRON" in dummy.operator_index["N0CALL"]["groups"]
    assert StationsMapTab._marker_station_matches_filters(
        dummy,
        "N0CALL",
        group_filter="AMRRON",
        region_filter="",
        my_call="",
        allow_self=False,
    )
    assert not StationsMapTab._marker_station_matches_filters(
        dummy,
        "N0CALL",
        group_filter="MARS",
        region_filter="",
        my_call="",
        allow_self=False,
    )


def test_vault_location_code_badge_tracks_effective_policy():
    row = {"id": "intel", "open_rule": "Allowed callsigns only", "access_code_hash": "savedhash"}

    assert _vault_location_requires_code_badge(
        row,
        default_location_id="default",
        global_code_policy="Require for non-default locations",
    )
    assert not _vault_location_requires_code_badge(
        row,
        default_location_id="default",
        global_code_policy="Allow public locations",
    )


def test_varac_status_treats_qso_summary_as_terminal_without_disconnect_line():
    client = VarACStatusClient(settings=None)
    text = "\n".join(
        [
            "02/05/2026 14:09:08 - CONNECTED TO W5TTA (BANDWIDTH: 500 FREQUENCY: 7.115.000)",
            "02/05/2026 14:10:18 - N1MAG> <BLR>",
            "02/05/2026 14:35:30 - QSO SUMMARY: Frequency: 7.115.000 (40m) Duration: 00:26:20",
        ]
    )

    status = client._evaluate_status(text)

    assert status["busy"] is False
    assert status["reason"] is None


def test_varac_status_clears_waiting_state_after_qso_summary():
    client = VarACStatusClient(settings=None)
    text = "\n".join(
        [
            "02/05/2026 13:55:00 - WAITING FOR FREQUENCY TO CLEAR",
            "02/05/2026 14:00:00 - CONNECTED TO TEST1 (BANDWIDTH: 500 FREQUENCY: 7.115.000)",
            "02/05/2026 14:05:00 - QSO SUMMARY: Frequency: 7.115.000 (40m) Duration: 00:05:00",
        ]
    )

    status = client._evaluate_status(text)

    assert status["busy"] is False
    assert status["waiting_for_frequency"] is False


def test_map_html_uses_bottom_docked_inline_legend_rows():
    dummy = SimpleNamespace(
        settings=_MemorySettings(),
        _now_reachable_enabled=True,
        show_grids=False,
        show_grid_labels=False,
        show_regions=False,
        show_states=False,
        show_cities=False,
        _resolve_prop_band_colors=lambda: {"20M": "#43A047", "40M": "#1E88E5"},
    )

    html = StationsMapTab._build_leaflet_html(
        dummy,
        markers=[],
        links=[],
        max_zoom=18,
        leaflet_js="leaflet.js",
        leaflet_css="leaflet.css",
        geojson_urls=[],
        cities_geojson=None,
        city_min_pop=0,
        show_city_labels=False,
        initial_view=None,
        prop_overlay_enabled=True,
        prop_region_scores=None,
        prop_state_scores=None,
    )

    assert 'id="legendDock"' in html
    assert 'id="legendBox"' in html
    assert "const legend = L.control" not in html
    assert "function updateLegend()" in html
    assert "legend-rows" in html
    assert "legend-label" in html
    assert "legend-sep" in html
    assert "Link SNR:" in html
    assert "SitRep Status:" in html
    assert "Peer Sched Now:" in html
    assert "Best Band Now:" in html
    assert 'color:\\"' not in html


def test_map_detail_button_action_overrides_payload_action():
    dummy = SimpleNamespace(
        settings=_MemorySettings(),
        _now_reachable_enabled=False,
        show_grids=False,
        show_grid_labels=False,
        show_regions=False,
        show_states=False,
        show_cities=False,
        _resolve_prop_band_colors=lambda: {},
    )

    html = StationsMapTab._build_leaflet_html(
        dummy,
        markers=[],
        links=[],
        max_zoom=18,
        leaflet_js="leaflet.js",
        leaflet_css="leaflet.css",
        geojson_urls=[],
        cities_geojson=None,
        city_min_pop=0,
        show_city_labels=False,
        initial_view=None,
    )

    assert "Object.assign({}, payload || {}, {action: action})" in html
    assert "Object.assign({action: action}, payload || {})" not in html


def test_map_controls_keep_action_buttons_readable(monkeypatch, tmp_path):
    _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))

    tab = StationsMapTab()
    try:
        assert tab.relay_target_combo.minimumWidth() == 180
        assert tab._refresh_links_button.minimumWidth() >= tab._refresh_links_button.sizeHint().width()
        assert tab._now_reachable_button.minimumWidth() >= tab._now_reachable_button.sizeHint().width()
        assert tab._sitrep_status_button.minimumWidth() >= tab._sitrep_status_button.sizeHint().width()
    finally:
        tab.deleteLater()


def test_sitrep_mode_suppresses_link_render_payload():
    dummy = SimpleNamespace()

    assert StationsMapTab._display_links_for_mode(dummy, [{"origin": "A", "destination": "B"}], True) == []
    assert StationsMapTab._display_links_for_mode(dummy, [{"origin": "A", "destination": "B"}], False) == [
        {"origin": "A", "destination": "B"}
    ]


def test_sitrep_state_rollup_returns_all_matching_states(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE sitrep_state_rollup (
                report_group TEXT NOT NULL,
                state_code TEXT NOT NULL,
                callsign_count INTEGER NOT NULL DEFAULT 0,
                red_count INTEGER NOT NULL DEFAULT 0,
                yellow_count INTEGER NOT NULL DEFAULT 0,
                green_count INTEGER NOT NULL DEFAULT 0,
                unknown_count INTEGER NOT NULL DEFAULT 0,
                js8_count INTEGER NOT NULL DEFAULT 0,
                internet_count INTEGER NOT NULL DEFAULT 0,
                mixed_transport_count INTEGER NOT NULL DEFAULT 0,
                latest_event_ts REAL NOT NULL DEFAULT 0
            )
            """
        )
        rows = [
            ("__ALL__", "CT", 9, 1, 0, 8, 0, 9, 0, 0, 1009.0),
            ("__ALL__", "NY", 8, 1, 0, 7, 0, 8, 0, 0, 1008.0),
            ("__ALL__", "PA", 7, 1, 0, 6, 0, 7, 0, 0, 1007.0),
            ("__ALL__", "FL", 6, 1, 0, 5, 0, 6, 0, 0, 1006.0),
            ("__ALL__", "OH", 5, 1, 0, 4, 0, 5, 0, 0, 1005.0),
            ("__ALL__", "TX", 4, 1, 0, 3, 0, 4, 0, 0, 1004.0),
            ("__ALL__", "CO", 3, 1, 0, 2, 0, 3, 0, 0, 1003.0),
            ("__ALL__", "CA", 2, 1, 0, 1, 0, 2, 0, 0, 1002.0),
            ("__ALL__", "WA", 1, 1, 0, 0, 0, 1, 0, 0, 1001.0),
        ]
        conn.executemany(
            """
            INSERT INTO sitrep_state_rollup (
                report_group, state_code, callsign_count, red_count, yellow_count, green_count,
                unknown_count, js8_count, internet_count, mixed_transport_count, latest_event_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    dummy = SimpleNamespace(_query_cache_ttl_sec=300.0, _query_cache={})
    dummy._query_cache_get = lambda key, ttl_sec=None: StationsMapTab._query_cache_get(dummy, key, ttl_sec)
    dummy._query_cache_set = lambda key, value: StationsMapTab._query_cache_set(dummy, key, value)

    rollup = StationsMapTab._load_sitrep_state_rollup(dummy, "")

    assert [row["state_code"] for row in rollup] == ["CT", "NY", "PA", "FL", "OH", "TX", "CO", "CA", "WA"]


def test_sitrep_state_rollup_legacy_schema_falls_back_cleanly(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    config_dir = cfg_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    db_path = config_dir / "freqinout_nets.db"

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE sitrep_state_rollup (
                report_group TEXT NOT NULL,
                state_code TEXT NOT NULL,
                callsign_count INTEGER NOT NULL DEFAULT 0,
                red_count INTEGER NOT NULL DEFAULT 0,
                yellow_count INTEGER NOT NULL DEFAULT 0,
                green_count INTEGER NOT NULL DEFAULT 0,
                unknown_count INTEGER NOT NULL DEFAULT 0,
                latest_event_ts REAL NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO sitrep_state_rollup (
                report_group, state_code, callsign_count, red_count, yellow_count, green_count,
                unknown_count, latest_event_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("__ALL__", "CO", 3, 1, 1, 1, 0, 1003.0),
        )
        conn.commit()
    finally:
        conn.close()

    dummy = SimpleNamespace(_query_cache_ttl_sec=300.0, _query_cache={})
    dummy._query_cache_get = lambda key, ttl_sec=None: StationsMapTab._query_cache_get(dummy, key, ttl_sec)
    dummy._query_cache_set = lambda key, value: StationsMapTab._query_cache_set(dummy, key, value)

    rollup = StationsMapTab._load_sitrep_state_rollup(dummy, "")

    assert len(rollup) == 1
    assert rollup[0]["state_code"] == "CO"
    assert rollup[0]["js8_count"] == 0
    assert rollup[0]["internet_count"] == 0
    assert rollup[0]["mixed_transport_count"] == 0
