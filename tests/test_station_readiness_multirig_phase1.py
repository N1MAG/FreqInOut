from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def test_build_station_readiness_report_requires_identity_groups_and_radio_profiles() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    report = build_station_readiness_report({}, device_profiles=[], operating_groups=[])

    messages = {issue.message for issue in report.issues}
    assert report.required_count >= 4
    assert "Callsign missing" in messages
    assert "Grid missing" in messages
    assert "No HF operating groups configured" in messages
    assert "No radio profiles configured" in messages


def test_build_station_readiness_report_warns_for_active_js8_radio_and_varac_outbox_gap() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    settings = {
        "callsign": "N1MAG",
        "grid": "DM79QJ",
        "path_js8call": "/tmp/JS8Call",
        "js8_host": "127.0.0.1",
        "js8_port": "2442",
        "js8_directed_path": "/tmp/DIRECTED.TXT",
        "varac_path": "/tmp/VarAC",
        "message_paths": {"varac": "/tmp/VarAC/Inbox"},
    }
    device_profiles = [
        {
            "id": 1,
            "name": "Primary JS8",
            "enabled": 1,
            "runtime_primary": 1,
            "runtime_active": 1,
            "control_backend": "js8call",
            "js8_host": "127.0.0.1",
            "launch_enabled": 1,
        }
    ]

    report = build_station_readiness_report(settings, device_profiles=device_profiles, operating_groups=[{"name": "@MAGNET"}])

    messages = {issue.message for issue in report.issues}
    assert "Primary JS8: JS8Call port missing" in messages
    assert "VarAC outbox directory missing" in messages
    radio_summary = report.summary_for_radio(1)
    assert radio_summary is not None
    assert radio_summary.required_count >= 1
    assert "Primary JS8: JS8Call port missing" in set(radio_summary.messages)


def test_build_station_readiness_report_accepts_saved_operator_identity_keys() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    report = build_station_readiness_report(
        {
            "operator_callsign": "N1MAG",
            "operator_grid6": "DM79QJ",
        },
        device_profiles=[],
        operating_groups=[{"name": "@MAGNET"}],
    )

    messages = {issue.message for issue in report.issues}
    assert "Callsign missing" not in messages
    assert "Grid missing" not in messages
    assert "No radio profiles configured" in messages


def test_build_station_readiness_report_accepts_saved_varac_auto_archive_key() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    report = build_station_readiness_report(
        {
            "operator_callsign": "N1MAG",
            "operator_grid6": "DM79QJ",
            "varac_path": "/tmp/VarAC",
            "message_paths": {"varac": "/tmp/VarAC/Inbox"},
            "varac_outbox_dir": "/tmp/VarAC/Outbox",
            "varac_bbs_auto_archive_enabled": True,
        },
        device_profiles=[],
        operating_groups=[{"name": "@MAGNET"}],
    )

    messages = {issue.message for issue in report.issues}
    assert "VarAC auto archive requires both BBS directories" in messages


def test_build_station_readiness_report_tracks_managed_vault_setup_gaps() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    report = build_station_readiness_report(
        {
            "operator_callsign": "N1MAG",
            "operator_grid6": "DM79QJ",
            "varac_path": "/tmp/VarAC",
            "message_paths": {"varac": "/tmp/VarAC/Inbox"},
            "varac_bbs_vault_enabled": True,
            "varac_bbs_vault_managed_root": "/tmp/FIO_BBS_Vault",
            "varac_bbs_vault_locations_v1": [],
        },
        device_profiles=[],
        operating_groups=[{"name": "@MAGNET"}],
    )

    messages = {issue.message for issue in report.issues}
    assert "Managed BBS Vault has no live BBS directory" in messages
    assert "Managed BBS Vault has no locations" in messages


def test_visible_status_programs_prefers_active_radio_software_flags() -> None:
    from freqinout.core.station_readiness import visible_status_programs

    visible = visible_status_programs(
        {},
        device_profiles=[
            {
                "id": 1,
                "enabled": 1,
                "runtime_primary": 1,
                "runtime_active": 1,
                "use_varac": 1,
                "control_backend": "manual",
            },
            {
                "id": 2,
                "enabled": 1,
                "runtime_primary": 0,
                "runtime_active": 0,
                "use_js8call": 1,
                "control_backend": "js8call",
            },
        ],
    )

    assert visible == [("VarAC", "VarAC")]


def test_visible_status_programs_hides_global_paths_when_active_radio_explicitly_opts_out() -> None:
    from freqinout.core.station_readiness import visible_status_programs

    visible = visible_status_programs(
        {
            "path_js8call": "/tmp/JS8Call",
            "varac_path": "/tmp/VarAC",
            "message_paths": {"varac": "/tmp/VarAC/Inbox"},
        },
        device_profiles=[
            {
                "id": 1,
                "enabled": 1,
                "runtime_primary": 1,
                "runtime_active": 1,
                "use_js8call": 0,
                "use_varac": 0,
                "control_backend": "manual",
            }
        ],
    )

    assert ("JS8Call_API", "JS8") not in visible
    assert ("VarAC", "VarAC") not in visible


def test_build_station_readiness_report_marks_inactive_and_launch_disabled_radios_as_informational() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report, readiness_summary_badge_text

    report = build_station_readiness_report(
        {"callsign": "N1MAG", "grid": "DM79QJ"},
        device_profiles=[
            {
                "id": 7,
                "name": "Spare HF",
                "enabled": 1,
                "runtime_primary": 0,
                "runtime_active": 0,
                "control_backend": "manual",
                "launch_enabled": 0,
            }
        ],
        operating_groups=[{"name": "@MAGNET"}],
    )

    messages = {issue.message for issue in report.issues if issue.severity == "informational"}
    assert "Spare HF: configured but inactive" in messages
    assert "Spare HF: excluded from startup launch" in messages
    summary = report.summary_for_radio(7)
    assert summary is not None
    assert summary.overall_state == "not_enabled"
    assert summary.informational_count == 2
    assert readiness_summary_badge_text(summary) == "Not Enabled"


def test_build_station_readiness_report_recommends_model_selection_for_active_radio() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    report = build_station_readiness_report(
        {"callsign": "N1MAG", "grid": "DM79QJ"},
        device_profiles=[
            {
                "id": 9,
                "name": "Desk Radio",
                "enabled": 1,
                "runtime_primary": 1,
                "runtime_active": 1,
                "control_backend": "manual",
                "launch_enabled": 1,
                "radio_model": "",
            }
        ],
        operating_groups=[{"name": "@MAGNET"}],
    )

    messages = {issue.message for issue in report.issues if issue.severity == "recommended"}
    assert "Desk Radio: radio model not selected" in messages


def test_build_station_readiness_report_surfaces_assigned_plan_rf_guard_warning() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    report = build_station_readiness_report(
        {
            "callsign": "N1MAG",
            "grid": "DM79QJ",
        },
        device_profiles=[
            {
                "id": 21,
                "name": "TS-2000",
                "enabled": 1,
                "runtime_primary": 1,
                "runtime_active": 1,
                "control_backend": "manual",
                "launch_enabled": 1,
                "radio_model": "TS-2000",
            }
        ],
        operating_groups=[{"name": "@MAGNET"}],
        assigned_schedule_status=[
            {
                "device_profile_id": 21,
                "device_name": "TS-2000",
                "frequency_plan_name": "AmRRON",
                "validation_status_json": json.dumps(
                    {
                        "state": "warning",
                        "warnings": ["Antenna support does not include 80M."],
                        "supported_bands": ["15M", "10M"],
                        "plan_bands": ["80M", "40M"],
                    }
                ),
            }
        ],
    )

    rf_guard_issues = [issue for issue in report.issues if issue.integration_key == "rf_guard"]
    assert len(rf_guard_issues) == 1
    assert rf_guard_issues[0].section_key == "schedule_assignments"
    assert rf_guard_issues[0].severity == "recommended"
    assert rf_guard_issues[0].deep_link_target == "schedule_assignments:radio:21"
    assert "TS-2000: RF Guard review needed for AmRRON" in rf_guard_issues[0].message
    assert "Antenna support does not include 80M." in rf_guard_issues[0].message
    summary = report.summary_for_radio(21)
    assert summary is not None
    assert summary.overall_state == "degraded"
    assert any("RF Guard review needed" in message for message in summary.messages)


def test_build_station_readiness_report_tracks_js8_bundle_even_when_backend_is_flrig() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report

    report = build_station_readiness_report(
        {"callsign": "N1MAG", "grid": "DM79QJ"},
        device_profiles=[
            {
                "id": 11,
                "name": "TriMode Desk",
                "enabled": 1,
                "runtime_primary": 1,
                "runtime_active": 1,
                "control_backend": "flrig",
                "use_flrig": 1,
                "use_js8call": 1,
                "use_js8spotter": 1,
                "use_commstat": 1,
                "flrig_port": "12345",
                "js8_host": "",
                "js8_port": "",
                "spotter_launch_path": "",
                "commstat_launch_path": "",
            }
        ],
        operating_groups=[{"name": "@MAGNET"}],
    )

    messages = {issue.message for issue in report.issues}
    assert "TriMode Desk: JS8Call host missing" in messages
    assert "TriMode Desk: JS8Call port missing" in messages
    assert "Spotter MCF forms folder missing" in messages
    assert "TriMode Desk: JS8Spotter launch path missing" not in messages
    assert "TriMode Desk: CommStat launch path missing" in messages


def test_build_station_readiness_report_marks_launch_excluded_active_radio_as_external_manual() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report, readiness_summary_badge_text

    report = build_station_readiness_report(
        {"callsign": "N1MAG", "grid": "DM79QJ"},
        device_profiles=[
            {
                "id": 12,
                "name": "Manual Desk",
                "enabled": 1,
                "runtime_primary": 1,
                "runtime_active": 1,
                "control_backend": "manual",
                "launch_enabled": 0,
                "radio_model": "IC-7300",
            }
        ],
        operating_groups=[{"name": "@MAGNET"}],
    )

    summary = report.summary_for_radio(12)
    assert summary is not None
    assert summary.overall_state == "external_manual"
    assert readiness_summary_badge_text(summary) == "External / Manual"


def test_readiness_wording_helpers_use_named_operator_states() -> None:
    from freqinout.core.station_readiness import (
        build_station_readiness_report,
        readiness_report_overall_text,
        readiness_state_label,
        readiness_summary_status_text,
    )

    report = build_station_readiness_report({}, device_profiles=[], operating_groups=[])
    assert report.overall_state == "needs_setup"
    assert readiness_state_label(report.overall_state) == "Needs Setup"
    assert "Needs Setup." in readiness_report_overall_text(report)

    radio_report = build_station_readiness_report(
        {"callsign": "N1MAG", "grid": "DM79QJ"},
        device_profiles=[
            {
                "id": 13,
                "name": "Review Radio",
                "enabled": 1,
                "runtime_primary": 1,
                "runtime_active": 1,
                "control_backend": "flrig",
                "use_flrig": 1,
                "flrig_port": "12345",
                "use_fldigi": 1,
                "fldigi_host": "127.0.0.1",
                "fldigi_port": "",
            }
        ],
        operating_groups=[{"name": "@MAGNET"}],
    )
    summary = radio_report.summary_for_radio(13)
    assert summary is not None
    assert summary.overall_state == "degraded"
    assert readiness_summary_status_text(summary, subject="Review Radio").startswith("Review Radio is degraded.")


def test_multirig_controlfreq_and_sop_sources_include_readiness_copy_and_activation_gating() -> None:
    controlfreq_source = Path(
        "/Users/bill/RadioCode/FreqInOut-multi-rig/freqinout/gui/controlfreq_tab.py"
    ).read_text(encoding="utf-8")
    sop_source = Path(
        "/Users/bill/RadioCode/FreqInOut-multi-rig/freqinout/gui/sop_tab.py"
    ).read_text(encoding="utf-8")
    map_source = Path(
        "/Users/bill/RadioCode/FreqInOut-multi-rig/freqinout/gui/stations_map_tab.py"
    ).read_text(encoding="utf-8")
    messages_source = Path(
        "/Users/bill/RadioCode/FreqInOut-multi-rig/freqinout/gui/message_viewer_tab.py"
    ).read_text(encoding="utf-8")
    support_source = Path(
        "/Users/bill/RadioCode/FreqInOut-multi-rig/freqinout/core/support_reporting.py"
    ).read_text(encoding="utf-8")
    sqlite_source = Path(
        "/Users/bill/RadioCode/FreqInOut-multi-rig/freqinout/core/sqlite_utils.py"
    ).read_text(encoding="utf-8")

    assert "Copy Summary" in controlfreq_source
    assert "Managed Vault" in controlfreq_source
    assert "def set_tab_active(self, active: bool) -> None:" in sop_source
    assert "_request_map_refresh" in map_source
    assert "_map_support_card" in map_source
    assert "Copy Diagnostics" in map_source
    assert "messages_copy_summary_btn" in messages_source
    assert "Manage VarAC BBS & Vault" in messages_source
    assert "build_support_summary" in support_source
    assert "fetch_all" in sqlite_source


def test_visible_status_programs_only_returns_configured_integrations() -> None:
    from freqinout.core.station_readiness import visible_status_programs

    settings = {
        "path_js8call": "/tmp/JS8Call",
        "js8_host": "127.0.0.1",
        "js8_port": "2442",
        "path_commstat": "/tmp/CommStat",
        "message_paths": {},
    }

    visible = visible_status_programs(settings, device_profiles=[])
    visible_keys = [key for key, _label in visible]
    assert "JS8Call_API" in visible_keys
    assert "CommStat" in visible_keys
    assert "FLRig" not in visible_keys
    assert "VarAC" not in visible_keys


def test_should_show_startup_review_respects_dismissed_digest_and_version_suppression() -> None:
    from freqinout.core.station_readiness import build_station_readiness_report, should_show_startup_review

    report = build_station_readiness_report({}, device_profiles=[], operating_groups=[])

    assert should_show_startup_review(report, current_version="1.2.3") is True
    assert should_show_startup_review(
        report,
        dismissed_digest=report.digest,
        current_version="1.2.3",
    ) is False
    assert should_show_startup_review(
        report,
        suppressed_version="1.2.3",
        current_version="1.2.3",
    ) is False
    assert should_show_startup_review(
        report,
        suppressed_version="1.2.3",
        current_version="1.2.4",
    ) is True


def test_settings_source_promotes_radio_readiness_cards() -> None:
    source = Path("/Users/bill/RadioCode/FreqInOut-multi-rig/freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert "Focused Radio Readiness" in source
    assert "Live Radio Readiness" in source
    assert "device_profile_readiness_card" in source
    assert "Copy Readiness Summary" in source
    assert "Focused Frequency Plan Guidance" in source
    assert "Focused Assigned Plan Guidance" in source
    assert "Focused VarAC Cluster Guidance" in source
    assert "Focused VarAC Membership Guidance" in source
    assert "Enable Cluster Mode" in source
    assert "Selected Radio Launch Bundle" in source
    assert "Selected radio:" in source
    assert "Only apps configured for this selected radio are shown here" in source
    assert "Primary Rig Control:" in source
    assert 'QCheckBox("JS8Call")' in source
    assert 'QCheckBox("FIO Spotter")' in source
    assert "Which external JS8Spotter app belongs to this radio?" in source
    assert 'QCheckBox("CommStat")' in source
    assert "Assigned Plan" in source
    assert "Assign Plan..." in source
    assert "Restore Plan" in source
    assert "Restore Default Plan" in source
    assert "Radio Readiness:" not in source


def _nav_item(tab, title: str):
    for idx in range(tab.sections_nav_list.count()):
        item = tab.sections_nav_list.item(idx)
        if item and item.text() == title:
            return item
    raise AssertionError(f"Missing settings nav item: {title}")


def test_settings_section_health_warns_for_radio_profiles_when_none_configured(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        radio_item = _nav_item(tab, "Radio Profiles")
    finally:
        tab.deleteLater()
        app.processEvents()

    assert radio_item.data(SettingsTab.SECTION_HEALTH_STATE_ROLE) == "warn"
    assert "No radio profiles configured" in str(radio_item.toolTip())


def test_focus_section_by_health_key_selects_radio_profiles(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        assert tab.focus_section_by_health_key("radio_profiles") is True
        current_item = tab.sections_nav_list.currentItem()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert current_item is not None
    assert current_item.text() == "Radio Profiles"


def test_focus_section_by_health_key_can_target_specific_radio_profile(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        saved = tab.multi_radio_store.save_device_profile({"name": "Focus Radio", "control_backend": "manual"})
        tab._refresh_multi_radio_tables()
        assert tab.focus_section_by_health_key("radio_profiles", int(saved["id"])) is True
        app.processEvents()
        current_item = tab.device_profiles_table.currentItem()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert current_item is not None
    assert current_item.data(Qt.UserRole) == int(saved["id"])


def test_radio_profile_readiness_detail_updates_for_focused_radio(monkeypatch, tmp_path):
    if sys.platform == "darwin":
        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)

    tab = SettingsTab()
    try:
        saved = tab.multi_radio_store.save_device_profile(
            {
                "name": "Detail Radio",
                "control_backend": "js8call",
                "runtime_primary": 1,
                "runtime_active": 1,
                "js8_host": "127.0.0.1",
                "launch_enabled": 0,
            }
        )
        tab._refresh_multi_radio_tables()
        assert tab.focus_radio_profile(int(saved["id"])) is True
        app.processEvents()
        detail_text = tab.device_profile_detail_label.text()
    finally:
        tab.deleteLater()
        app.processEvents()

    assert "Detail Radio" in detail_text
    assert "JS8Call port missing" in detail_text
