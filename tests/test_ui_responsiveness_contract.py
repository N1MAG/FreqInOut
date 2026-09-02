from __future__ import annotations

from pathlib import Path


GUI_ROOT = Path("freqinout/gui")
CORE_ROOT = Path("freqinout/core")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _py_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def test_ui_responsiveness_contract_is_documented() -> None:
    text = _read(Path("docs/internal/ui_layout_standards.md"))

    assert "## UI Responsiveness Contract" in text
    assert "Qt.BlockingQueuedConnection" in text
    assert "QThread.wait()" in text
    assert "subprocess.run" in text
    assert "future.result()" in text
    assert "QApplication.processEvents()" in text
    assert "Map views must coalesce redraws" in text


def test_no_blocking_queued_connection_in_gui_or_core() -> None:
    offenders: list[str] = []
    for path in _py_files(GUI_ROOT) + _py_files(CORE_ROOT):
        text = _read(path)
        if "BlockingQueuedConnection" in text:
            offenders.append(str(path))

    assert offenders == []


def test_gui_thread_waits_are_bounded_and_allowlisted() -> None:
    # Existing bounded waits are tracked as a migration list. New waits should use
    # worker callbacks, queued shutdown, or an explicit short timeout.
    allowed = {
        "freqinout/gui/main_window.py": ["thread.wait(200)"],
        "freqinout/gui/message_viewer_tab.py": ["thread.wait(max(0, min(int(wait_ms), 250)))"],
    }
    offenders: list[str] = []
    for path in _py_files(GUI_ROOT):
        text = _read(path)
        for line_no, line in enumerate(text.splitlines(), start=1):
            if ".wait(" not in line:
                continue
            expected = allowed.get(str(path), [])
            if not any(marker in line for marker in expected):
                offenders.append(f"{path}:{line_no}:{line.strip()}")

    assert offenders == []


def test_gui_subprocess_calls_are_timeout_bounded() -> None:
    offenders: list[str] = []
    for path in _py_files(GUI_ROOT):
        text = _read(path)
        for line_no, line in enumerate(text.splitlines(), start=1):
            if "subprocess.run(" not in line:
                continue
            if "timeout=" not in line:
                offenders.append(f"{path}:{line_no}:{line.strip()}")

    assert offenders == []


def test_gui_process_events_are_allowlisted_migration_items() -> None:
    allowed = {
        "freqinout/gui/startup_splash.py",
    }
    offenders: list[str] = []
    for path in _py_files(GUI_ROOT):
        text = _read(path)
        if "processEvents(" in text and str(path) not in allowed:
            offenders.append(str(path))

    assert offenders == []


def test_gui_future_result_is_done_callback_only_for_now() -> None:
    allowed = {
        "freqinout/gui/controlfreq_tab.py": ["future.result()"],
        "freqinout/gui/freq_planner_tab.py": ["future.result()"],
        "freqinout/gui/stations_map_tab.py": ["future.result()"],
    }
    offenders: list[str] = []
    for path in _py_files(GUI_ROOT):
        text = _read(path)
        for line_no, line in enumerate(text.splitlines(), start=1):
            if ".result(" not in line:
                continue
            expected = allowed.get(str(path), [])
            if not any(marker in line for marker in expected):
                offenders.append(f"{path}:{line_no}:{line.strip()}")

    assert offenders == []


def test_ops_center_message_summary_discards_stale_worker_results() -> None:
    text = _read(Path("freqinout/gui/controlfreq_tab.py"))

    assert "_message_summary_request_id" in text
    assert "request_id != int(getattr(self, \"_message_summary_request_id\", 0) or 0)" in text


def test_message_viewer_shutdown_no_long_waits_or_unbounded_subprocess() -> None:
    text = _read(Path("freqinout/gui/message_viewer_tab.py"))

    assert "wait(1000)" not in text
    assert "thread.wait(max(0, min(int(wait_ms), 250)))" in text
    assert "subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=5)" in text


def test_js8_ncs_uses_compact_sectioned_layout() -> None:
    text = _read(Path("freqinout/gui/js8call_net_control_tab.py"))
    spec = _read(Path("docs/internal/ui_layout_standards.md"))

    assert "Net-control/NCS views must present the workflow as compact sections" in spec
    assert 'QGroupBox("Net Setup")' in text
    assert 'QGroupBox("Check-Ins")' in text
    assert "checkins_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)" in text
    assert "self.checkin_table.setMinimumHeight(260)" in text
    assert "table_layout.addLayout(btn_row)" in text
    assert "layout.addWidget(checkins_group, 1)" in text


def test_ncs_tabs_expose_radio_session_context() -> None:
    spec = _read(Path("docs/internal/ui_layout_standards.md"))
    js8 = _read(Path("freqinout/gui/js8call_net_control_tab.py"))
    fldigi = _read(Path("freqinout/gui/fldigi_net_control_tab.py"))
    local = _read(Path("freqinout/gui/local_ncs_tab.py"))
    contract = _read(Path("freqinout/core/ncs_session_contract.py"))

    assert "explicit `NCS Session` context" in spec
    assert "separate session snapshots" in spec
    assert "class NcsSessionSnapshot" in contract
    assert "NCS_SESSION_SNAPSHOTS_KEY" in contract
    assert 'QGroupBox("NCS Session")' in js8
    assert "_select_ncs_radio_session" in js8
    assert "_refresh_ncs_session_context" in js8
    assert "_persist_ncs_session_snapshot" in js8
    assert "write_ncs_session_snapshot" in js8
    assert "Switch this NCS workspace" in js8
    assert "ncs_session_chip_layout" in fldigi
    assert "_select_ncs_radio_session" in fldigi
    assert "_refresh_ncs_session_context" in fldigi
    assert "_persist_ncs_session_snapshot" in fldigi
    assert "write_ncs_session_snapshot" in fldigi
    assert "Switch this NCS workspace" in fldigi
    assert 'QGroupBox("NCS Session")' in local
    assert "_refresh_ncs_session_context" in local
    assert "_persist_ncs_session_snapshot" in local
    assert "write_ncs_session_snapshot" in local
    assert "Session: Local | VHF/UHF | NCS" in local


def test_fldigi_ncs_workbench_spec_tracks_role_and_summary_contracts() -> None:
    text = _read(Path("docs/internal/fldigi_ncs_workbench_spec.md"))
    standards = _read(Path("docs/internal/ui_layout_standards.md"))
    inventory = _read(Path("docs/internal/operational_view_inventory.md"))

    assert "Action for: NCS <callsign> | ANCS <callsign>" in text
    assert "Do not use `Send to` language here" in text
    assert "QSY controls are not duplicated here" in text
    assert "Copy Summary by State" in text
    assert "accepted/confirmed aggregate check-ins" in text
    assert "Log-assisted entries are included only after they have been accepted" in text
    assert "NcsSessionSnapshot" in text
    assert "docs/internal/fldigi_ncs_workbench_spec.md" in standards
    assert "NCS-FLDigi/SSB | Net Control | Net Control Workspace | Specified" in inventory


def test_main_shell_reads_ncs_session_snapshots_for_nav_state() -> None:
    text = _read(Path("freqinout/gui/main_window.py"))
    spec = _read(Path("docs/internal/ui_layout_standards.md"))

    assert "must expose active session labels" in spec
    assert "active_ncs_session_flags" in text
    assert "active_ncs_session_summaries_by_kind" in text
    assert "clear_persisted_active_ncs_sessions" in text
    assert "_active_ncs_session_flags_from_settings" in text
    assert "_clear_stale_ncs_activity_on_startup" in text
    assert "_refresh_ncs_activity_from_snapshots" in text
    assert "_active_ncs_session_tooltip" in text
    assert "self._clear_stale_ncs_activity_on_startup()" in text
    status_handler = text[text.index("def _on_ncs_net_status_changed") : text.index("def _active_ncs_session_flags_from_settings")]
    assert "_refresh_ncs_activity_from_snapshots()" not in status_handler


def test_ncs_tabs_persist_session_snapshot_before_status_signal() -> None:
    sources = (
        _read(Path("freqinout/gui/fldigi_net_control_tab.py")),
        _read(Path("freqinout/gui/js8call_net_control_tab.py")),
        _read(Path("freqinout/gui/local_ncs_tab.py")),
    )

    for text in sources:
        for marker in ('net_status_changed.emit("FLDIGI"', 'net_status_changed.emit("JS8"', 'net_status_changed.emit("LOCAL"'):
            search_from = 0
            while marker in text[search_from:]:
                emit_index = text.index(marker, search_from)
                preceding = text[max(0, emit_index - 180) : emit_index]
                assert "_persist_ncs_session_snapshot" in preceding
                search_from = emit_index + len(marker)


def test_fldigi_ncs_scroll_content_mounts_during_ui_build() -> None:
    text = _read(Path("freqinout/gui/fldigi_net_control_tab.py"))
    build_start = text.index("    def _build_ui(self):")
    context_start = text.index("    def _on_ncs_session_context_changed", build_start)
    build_block = text[build_start:context_start]
    context_block = text[context_start:text.index("    def _toggle_setup_details", context_start)]

    assert "outer_layout.addWidget(self._ncs_scroll_area, 1)" in build_block
    assert "self._ncs_scroll_area.setWidget(self._ncs_scroll_content)" in build_block
    assert "self._ncs_scroll_area.setWidget(self._ncs_scroll_content)" not in context_block


def test_ui_regression_work_log_tracks_current_followups() -> None:
    spec = _read(Path("docs/internal/ui_layout_standards.md"))
    log = _read(Path("docs/internal/ui_regression_work_log.md"))

    assert "docs/internal/ui_regression_work_log.md" in spec
    assert "FLDigi NCS Blank Workspace" in log
    assert "Qt Shutdown Timer Warning" in log
    assert "Mesh Device Library And Connection Management" in log
    assert "Dark Theme Contrast Audit" in log


def test_map_payload_updates_are_generation_guarded() -> None:
    text = _read(Path("freqinout/gui/stations_map_tab.py"))

    assert "_map_payload_generation" in text
    assert "payload_generation != getattr(self, \"_map_payload_generation\", 0)" in text
    assert "_emit_map_event(\"payload_update_stale\"" in text


def test_plan_builder_projection_work_uses_worker_snapshots() -> None:
    text = _read(Path("freqinout/gui/freq_planner_tab.py"))

    assert "_PlanProjectionResult" in text
    assert "_PlanProjectionEmitter" in text
    assert "ThreadPoolExecutor(max_workers=1, thread_name_prefix=\"fio-plan-projection\")" in text
    assert "_projection_request_id" in text
    assert "result.request_id != int(getattr(self, \"_projection_request_id\", 0) or 0)" in text
    assert "_build_projection_snapshot" in text
    assert "_start_projection_worker(" in text


def test_map_projection_uses_worker_snapshots() -> None:
    text = _read(Path("freqinout/gui/stations_map_tab.py"))

    assert "_MapProjectionSnapshotResult" in text
    assert "_MapProjectionSnapshotEmitter" in text
    assert "ThreadPoolExecutor(max_workers=1, thread_name_prefix=\"fio-map-projection\")" in text
    assert "_serialize_map_projection_snapshot" in text
    assert "self._map_projection_worker().submit(" in text
    assert "payload_generation != getattr(self, \"_map_payload_generation\", 0)" in text


def test_source_health_is_not_map_projection_input() -> None:
    text = _read(Path("freqinout/gui/stations_map_tab.py"))

    start = text.index("map_input_sig = (")
    end = text.index("if (", start)
    signature_block = text[start:end]
    assert "mesh_health" not in signature_block
    assert "list_mesh_health" not in text
