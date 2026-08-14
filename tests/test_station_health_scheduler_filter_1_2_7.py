from __future__ import annotations

import time


def test_station_health_shows_latest_success_and_issue_log(monkeypatch):
    import freqinout.core.station_health_summary as summary_module

    events = [
        {
            "id": 5,
            "event_type": "status",
            "code": "already_applied",
            "action": "Schedule entry already applied",
            "detail": "newest routine success",
        },
        {
            "id": 4,
            "event_type": "status",
            "code": "already_applied",
            "action": "Schedule entry already applied",
            "detail": "older routine success",
        },
        {
            "id": 3,
            "event_type": "failed",
            "code": "control_failed",
            "action": "Control failed",
            "detail": "issue retained",
        },
        {
            "id": 2,
            "event_type": "verified",
            "code": "post_apply_on_schedule",
            "action": "Verified",
            "detail": "older success hidden",
        },
    ]
    monkeypatch.setattr(summary_module, "load_recent_scheduler_events", lambda limit=25: list(events))

    result = summary_module.summarize_station_health(
        registry_snapshot={},
        include_scheduler_events=True,
    )

    filtered = result["recent_scheduler_events"]
    assert [item["code"] for item in filtered] == ["already_applied", "control_failed"]
    assert filtered[0]["_station_health_kind"] == "latest_success"
    assert filtered[1]["_station_health_kind"] == "issue"


def test_station_health_treats_rf_guard_events_as_operator_attention(monkeypatch):
    import freqinout.core.station_health_summary as summary_module

    events = [
        {
            "id": 6,
            "event_type": "status",
            "code": "already_applied",
            "action": "Schedule entry already applied",
            "detail": "latest success",
        },
        {
            "id": 5,
            "event_type": "blocked",
            "code": "rf_safety_guard_block",
            "action": "Blocked schedule change by RF Safety Guard",
            "detail": "FIO cannot verify that radio's current frequency.",
        },
        {
            "id": 4,
            "event_type": "warning",
            "code": "rf_safety_guard_warning",
            "action": "Continuing schedule change after RF Safety Guard warn-only notice",
            "detail": "Peer radio shares a configured RF resource.",
        },
    ]
    monkeypatch.setattr(summary_module, "load_recent_scheduler_events", lambda limit=25: list(events))

    result = summary_module.summarize_station_health(
        registry_snapshot={},
        include_scheduler_events=True,
    )

    filtered = result["recent_scheduler_events"]
    assert [item["code"] for item in filtered] == [
        "already_applied",
        "rf_safety_guard_block",
        "rf_safety_guard_warning",
    ]
    assert filtered[1]["_station_health_kind"] == "issue"
    assert filtered[2]["_station_health_kind"] == "issue"


def test_fldigi_busy_check_history_is_not_a_station_issue():
    import freqinout.core.station_health_summary as summary_module

    result = summary_module.summarize_station_health(
        registry_snapshot={
            "scheduler:fldigi-busy-check": {
                "owner": "scheduler",
                "consecutive_failures": 1,
                "cooldown_remaining_sec": 25,
                "last_checked_ts": 1000.0,
                "issue_started_ts": 1000.0,
                "last_error": (
                    "could not verify FLDigi receive activity; continuing schedule: "
                    "SettingsManager used from a different thread than it was created on"
                ),
                "metadata": {
                    "action": (
                        "could not verify FLDigi receive activity; continuing schedule: "
                        "SettingsManager used from a different thread than it was created on"
                    )
                },
            }
        },
        include_scheduler_events=False,
    )

    assert result["severity"] == "ok"
    assert result["issue_count"] == 0
    item = result["items"][0]
    assert item["state"] == "OK"
    assert item["is_issue"] is False
    assert item["last_issue"] == ""
    assert "SettingsManager" not in item["action"]


def test_js8_capability_success_surfaces_operator_action_text():
    import freqinout.core.station_health_summary as summary_module

    now = time.monotonic()
    result = summary_module.summarize_station_health(
        registry_snapshot={
            "js8call:127.0.0.1:2442:capability": {
                "owner": "SoftwareStatusService",
                "consecutive_failures": 0,
                "consecutive_slow": 0,
                "last_checked_ts": now,
                "last_success_ts": now,
                "last_error": "",
                "metadata": {
                    "capability_mode": "api_full",
                    "version": "3.0.2",
                    "endpoint": "127.0.0.1:2442",
                    "action": "JS8Call API is ready for native FIO diagnostics at 127.0.0.1:2442. Version: 3.0.2.",
                },
            }
        },
        include_scheduler_events=False,
    )

    assert result["severity"] == "ok"
    assert result["issue_count"] == 0
    item = result["items"][0]
    assert item["dependency"] == "JS8Call API (127.0.0.1:2442)"
    assert item["state"] == "OK"
    assert "native FIO diagnostics" in item["action"]


def test_js8_shadow_mismatch_surfaces_as_native_diagnostic():
    import freqinout.core.station_health_summary as summary_module

    now = time.monotonic()
    result = summary_module.summarize_station_health(
        registry_snapshot={
            "scheduler:js8-shadow": {
                "owner": "SchedulerEngine",
                "consecutive_failures": 1,
                "consecutive_slow": 0,
                "last_checked_ts": now,
                "last_failure_ts": now,
                "issue_started_ts": now,
                "last_error": (
                    "Native JS8Call diagnostic disagrees with the existing JS8 status for frequency. "
                    "FIO is still using the existing JS8 path; native JS8 remains diagnostic only."
                ),
                "metadata": {
                    "action": (
                        "Native JS8Call diagnostic disagrees with the existing JS8 status for frequency. "
                        "FIO is still using the existing JS8 path; native JS8 remains diagnostic only."
                    ),
                    "diagnostic_only": True,
                    "endpoint": "127.0.0.1:2443",
                    "mode": "api_basic",
                },
            }
        },
        include_scheduler_events=False,
    )

    assert result["severity"] == "warning"
    assert result["issue_count"] == 1
    item = result["items"][0]
    assert item["dependency"] == "JS8Call native diagnostic"
    assert item["state"] == "Warning"
    assert "native JS8 remains diagnostic only" in item["action"]


def test_fldigi_busy_check_internal_events_do_not_fill_issue_log(monkeypatch):
    import freqinout.core.station_health_summary as summary_module

    events = [
        {
            "id": 4,
            "event_type": "status",
            "code": "already_applied",
            "action": "Schedule entry already applied",
        },
        {
            "id": 3,
            "event_type": "failed",
            "code": "fldigi_busy_check_failed",
            "action": "Could not verify FLDigi receive activity; continuing schedule",
            "detail": "SettingsManager used from a different thread than it was created on",
        },
        {
            "id": 2,
            "event_type": "status",
            "code": "fldigi_busy_check_queued",
            "action": "Checking FLDigi receive activity before changing frequency",
        },
    ]
    monkeypatch.setattr(summary_module, "load_recent_scheduler_events", lambda limit=25: list(events))

    result = summary_module.summarize_station_health(
        registry_snapshot={},
        include_scheduler_events=True,
    )

    filtered = result["recent_scheduler_events"]
    assert [item["code"] for item in filtered] == ["already_applied"]


def test_scheduler_hold_history_is_ok_when_no_schedule_move_is_pending():
    import freqinout.core.station_health_summary as summary_module

    result = summary_module.summarize_station_health(
        registry_snapshot={
            "scheduler:js8-busy": {
                "owner": "scheduler",
                "consecutive_failures": 1,
                "last_checked_ts": 1000.0,
                "last_error": "holding schedule change because JS8Call is busy",
                "metadata": {
                    "action": "holding schedule change because JS8Call is busy",
                },
            }
        },
        include_scheduler_events=False,
    )

    assert result["severity"] == "ok"
    assert result["issue_count"] == 0
    item = result["items"][0]
    assert item["state"] == "OK"
    assert item["is_issue"] is False
    assert item["action"] == "No scheduled frequency change is waiting on this activity check."


def test_active_scheduler_hold_still_shows_hold():
    import freqinout.core.station_health_summary as summary_module

    result = summary_module.summarize_station_health(
        registry_snapshot={
            "scheduler:js8-busy": {
                "owner": "scheduler",
                "consecutive_failures": 1,
                "last_checked_ts": 1000.0,
                "last_error": "holding schedule change because JS8Call is busy",
                "metadata": {
                    "action": "holding schedule change because JS8Call is busy",
                    "active_hold": True,
                },
            }
        },
        include_scheduler_events=False,
    )

    assert result["severity"] == "ok"
    assert result["issue_count"] == 0
    item = result["items"][0]
    assert item["state"] == "Hold"
    assert item["is_issue"] is False
    assert item["action"] == "holding schedule change because JS8Call is busy"
