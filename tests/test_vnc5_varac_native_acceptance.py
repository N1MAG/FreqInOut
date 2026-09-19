"""VNC-5 acceptance contracts for native VarAC cluster setup.

These tests are deliberately deterministic: they use temporary INI/runtime
fixtures and an isolated FIO SQLite store.  No RF, subprocess, or live app
probe is involved.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from PySide6.QtWidgets import QApplication, QWidget

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.config_varac_managed import (
    apply_varac_native_cluster_plan,
    parse_vara_ini_bytes,
    parse_varac_ini_bytes,
    snapshot_target_state,
    snapshot_vara_runtime_files,
)
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.varac_native_preparation import (
    native_draft_fingerprint,
    prepare_varac_native_configuration,
)
from freqinout.core.varac_native_transaction import (
    VarACNativeExternalSession,
    begin_varac_native_external_apply,
    recover_unfinished_varac_native_applies,
    rollback_varac_native_external_session,
)


def _evidence(tmp_path: Path) -> tuple[dict[str, object], dict[str, object]]:
    varac_root = tmp_path / "VarAC"
    vara_root = tmp_path / "VARA"
    varac_root.mkdir()
    vara_root.mkdir()
    (varac_root / "VarAC.exe").write_bytes(b"fixture VarAC version 13.2.7")
    (vara_root / "VARA.exe").write_bytes(b"fixture VARA executable")
    (vara_root / "operator.dat").write_bytes(b"operator-owned runtime data")
    (vara_root / "VARA.ini").write_text(
        "[Setup]\r\nTCP Command Port=8300\r\nEnable KISS=1\r\nKISS Port=8302\r\n"
        "[Monitor]\r\nMonitor Mode=1\r\n",
        encoding="utf-8",
    )
    ini = varac_root / "VarAC.ini"
    ini.write_text(
        f"[OTHER]\r\nDBCustomFilePath={varac_root / 'VarAC.db'}\r\n"
        "[VARAHF_CONFIG]\r\n"
        f"VarahfMainPath={vara_root / 'VARA.exe'}\r\n"
        "VarahfMainPort=8300\r\nVarahfMainHost=127.0.0.1\r\n"
        "VarahfEnableKissInterface=ON\r\nVarahfMainKissPort=8302\r\n"
        f"VarahfMonitorPath={vara_root / 'VARA.exe'}\r\n"
        "VarahfMonitorPort=8303\r\nVarahfLaunchOnModemConnect=OFF\r\n",
        encoding="utf-8",
    )
    node = {
        "id": 11,
        "name": "Existing VarAC",
        "install_path": str(varac_root),
        "ini_path": str(ini),
        "db_path": str(varac_root / "VarAC.db"),
        "vara_runtime_path": str(vara_root),
    }
    profile = {"id": 5, "name": "Existing Radio", "varac_node_id": 11, "device_class": "tx_rx"}
    return node, profile


def _draft(**overrides: object) -> dict[str, object]:
    draft: dict[str, object] = {
        "family_key": "varac",
        "mode": "managed",
        "instance_name": "New Radio",
        "owner_label": "New Radio",
        "draft_instance_key": "draft-new-radio",
        "cluster_path": "create_cluster",
        "cluster_name": "Field Cluster",
        "cluster_instance_number": 2,
        "existing_standalone_node_id": 11,
        "existing_standalone_member_number": 1,
        "cluster_ptt_lock": True,
    }
    draft.update(overrides)
    return draft


@pytest.mark.parametrize("platform", ["windows", "linux-wine"])
@pytest.mark.parametrize(
    ("sender_choice", "expected_sender"),
    [("none", ""), ("existing_member", "node:11"), ("new_member", "draft-new-radio")],
)
def test_prepare_is_cross_platform_and_explicit_about_sender_and_runtime(
    tmp_path: Path,
    platform: str,
    sender_choice: str,
    expected_sender: str,
) -> None:
    node, profile = _evidence(tmp_path)
    result = prepare_varac_native_configuration(
        _draft(email_gateway_sender_choice=sender_choice),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=19,
        platform_override=platform,
    )

    assert result.ready, result.presentation
    assert result.plan is not None
    assert result.plan.platform == platform
    assert result.plan.email_gateway_sender_member_id == expected_sender
    assert result.plan.members[0].target_path == Path(node["ini_path"])
    assert result.plan.members[1].target_path == tmp_path / "VarAC" / "VarAC-new-radio.ini"
    assert result.plan.members[0].vara_target_runtime_folder != result.plan.members[1].vara_target_runtime_folder
    assert result.plan.members[1].vara_target_path == result.plan.members[1].vara_target_runtime_folder / "VARA.ini"
    assert result.plan.members[1].launch_command[0] == ("wine" if platform == "linux-wine" else str(node["install_path"]) + "/VarAC.exe")
    assert result.presentation["generation"] == 19
    assert result.draft_fingerprint == native_draft_fingerprint(_draft(email_gateway_sender_choice=sender_choice))


def test_native_apply_fault_has_no_partial_ini_or_runtime_and_explicit_session_rollback(tmp_path: Path) -> None:
    node, profile = _evidence(tmp_path)
    preparation = prepare_varac_native_configuration(
        _draft(email_gateway_sender_choice="new_member"),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=1,
        platform_override="windows",
    )
    assert preparation.plan is not None
    original = Path(node["ini_path"]).read_bytes()
    result = apply_varac_native_cluster_plan(
        preparation.plan,
        backup_root=tmp_path / "backups",
        failure_injector=lambda phase: (_ for _ in ()).throw(OSError("injected")) if phase == "promote" else None,
    )
    assert not result.ok
    assert Path(node["ini_path"]).read_bytes() == original
    assert not preparation.plan.members[1].target_path.exists()
    assert not preparation.plan.members[1].vara_target_runtime_folder.exists()

    # A successful external phase is still reversible until FIO commits.
    store = MultiRadioStore(tmp_path / "fio.db")
    session = begin_varac_native_external_apply(
        store=store,
        plan=preparation.plan,
        backup_root=tmp_path / "backups-2",
    )
    assert session.ok
    assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "external_applied"
    rollback = rollback_varac_native_external_session(store, session, error="cancelled by operator")
    assert not rollback.committed and not rollback.needs_recovery
    assert Path(node["ini_path"]).read_bytes() == original
    assert not preparation.plan.members[1].target_path.exists()
    assert not preparation.plan.members[1].vara_target_runtime_folder.exists()
    assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "rolled_back"


def test_direct_apply_rejects_incomplete_review_metadata_without_starting_a_worker(
    tmp_path: Path,
) -> None:
    """Missing immutable review identity cannot reach the native writer."""

    # Importing the host class is safe; no QWidget is constructed here.
    from freqinout.gui.settings_tab import SettingsTab

    node, profile = _evidence(tmp_path)
    draft = _draft(email_gateway_sender_choice="new_member")
    prepared = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=2,
        platform_override="windows",
    )
    assert prepared.ready

    class Publisher:
        def __init__(self, current: dict[str, object]) -> None:
            self.current = current
            self.presentations: list[dict[str, object]] = []

        def varac_native_draft_payload(self) -> dict[str, object]:
            return self.current

        def set_varac_native_presentation(self, presentation: dict[str, object]) -> bool:
            self.presentations.append(dict(presentation))
            return True

    host = SettingsTab.__new__(SettingsTab)
    host._varac_native_preparations = {native_draft_fingerprint(draft): prepared}
    publisher = Publisher(draft)
    stale_payload = {
        "draft": {**draft, "varac_native_generation": 1},
        "native_presentation": {"state": "ready", "generation": 1},
    }

    # A real preparation is present, but incomplete review metadata cannot
    # identify the immutable plan or reviewed intent.
    SettingsTab._on_varac_native_apply_requested(host, stale_payload, publisher=publisher)
    assert publisher.presentations
    assert publisher.presentations[-1]["state"] == "needs attention"
    assert "stale" in str(publisher.presentations[-1]["why"]).lower()


def test_add_radio_stale_review_compensates_an_already_applied_native_session() -> None:
    """A stale outer Add Radio review cannot strand an external VarAC apply."""

    from freqinout.gui.settings_tab import SettingsTab

    session = object()
    rolled_back: list[object] = []
    host = SettingsTab.__new__(SettingsTab)
    host._open_device_profile_dialog = lambda **_kwargs: {
        "guided_software_instance_drafts": {"varac": {"session": "fixture"}}
    }
    host._varac_native_session_from_guided_profile = lambda _payload: session
    host._guided_radio_review_is_current = lambda _payload: False
    host._rollback_varac_native_session = rolled_back.append
    host._present_guided_stale_review_recovery = lambda _detail: ""

    SettingsTab._add_device_profile(host)
    assert rolled_back == [session]


def test_final_add_radio_save_applies_reviewed_varac_plan_and_hands_session_to_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Equivalent re-preparation cannot invalidate the reviewed transaction."""

    from freqinout.gui.settings_tab import SettingsTab

    node, profile = _evidence(tmp_path)
    draft = _draft()
    prepared = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=7,
        platform_override="linux-wine",
    )
    assert prepared.ready and prepared.plan is not None
    latest_prepared = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=8,
        platform_override="linux-wine",
    )
    assert latest_prepared.ready and latest_prepared.plan is not None
    assert latest_prepared.plan.plan_fingerprint == prepared.plan.plan_fingerprint
    hydrated = {
        **draft,
        "application_path": prepared.presentation["application_path"],
        "configuration_path": prepared.presentation["configuration_path"],
        "storage_path": prepared.presentation["storage_path"],
        "cluster_shared_database": prepared.presentation["storage_path"],
        "secondary_storage_path": prepared.presentation["secondary_storage_path"],
        "outbox_path": prepared.presentation["outbox_path"],
        "working_directory": prepared.presentation["working_directory"],
        "launch_command": prepared.presentation["launch_command"],
        "port": prepared.presentation["port"],
        "secondary_port": prepared.presentation["secondary_port"],
        "vara_runtime_path": prepared.presentation["vara_runtime_path"],
        "vara_ini_path": prepared.presentation["vara_ini_path"],
    }
    native_presentation = {
        **dict(prepared.presentation),
        "draft_fingerprint": native_draft_fingerprint(hydrated),
    }
    reviewed_request = {
        **hydrated,
        "varac_native_presentation": native_presentation,
        "varac_native_generation": 7,
        "varac_native_plan_fingerprint": prepared.plan.plan_fingerprint,
    }
    reviewed = {
        **hydrated,
        "_varac_native_apply_request": {
            "draft": reviewed_request,
            "native_presentation": native_presentation,
        },
    }
    session = VarACNativeExternalSession(
        journal_id="journal",
        plan=latest_prepared.plan,
        apply_result=SimpleNamespace(ok=True),
        observed={"observed_fingerprint": "observed"},
    )
    completed: list[dict[str, object]] = []
    started: list[object] = []
    rolled_back: list[object] = []
    warnings: list[tuple[object, ...]] = []
    host = SettingsTab.__new__(SettingsTab)
    host.multi_radio_store = SimpleNamespace(db_path=tmp_path / "fio.db")
    # The reviewed UI payload is generation 7, while an automatic equivalent
    # refresh has replaced the cache with generation 8.  The immutable plan
    # and reviewed/live intent are unchanged, so Final Save must proceed.
    host._varac_native_preparations = {
        latest_prepared.draft_fingerprint: latest_prepared
    }
    host._rollback_guided_native_config = rolled_back.append

    def _start(worker, *, on_finished, on_failed):
        started.append(worker)
        on_finished(session)

    host._start_varac_native_job = _start
    host._complete_add_device_profile = (
        lambda payload, **_kwargs: completed.append(dict(payload))
    )

    SettingsTab._continue_add_device_profile_after_guided_native(
        host,
        {"guided_software_instance_drafts": {"varac": reviewed}},
    )

    assert len(completed) == 1
    saved = completed[0]["guided_software_instance_drafts"]["varac"]
    assert saved["_varac_native_external_session"] is session
    assert "_varac_native_apply_request" not in saved
    assert tuple(saved["launch_argv"]) == latest_prepared.plan.members[-1].launch_command
    component = saved["launch_recipe"]["components"][0]
    assert tuple((component["executable"], *component["arguments"])) == latest_prepared.plan.members[-1].launch_command
    assert len(started) == 1

    # Generation equality is not the safety boundary.  A real intent change
    # after review remains blocked before a native writer starts.
    from freqinout.gui import settings_tab as settings_module

    monkeypatch.setattr(
        settings_module.QMessageBox,
        "warning",
        lambda *args: warnings.append(tuple(args)),
    )
    changed_live = {**reviewed, "cluster_ptt_lock": False}
    SettingsTab._continue_add_device_profile_after_guided_native(
        host,
        {"guided_software_instance_drafts": {"varac": changed_live}},
    )
    assert len(started) == 1
    assert len(completed) == 1
    assert rolled_back == [None]
    assert warnings


def test_direct_software_admin_native_apply_resolves_hydrated_plan_fingerprint(
    tmp_path: Path,
) -> None:
    from freqinout.gui.settings_tab import SettingsTab

    node, profile = _evidence(tmp_path)
    draft = _draft()
    prepared = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=11,
        platform_override="linux-wine",
    )
    assert prepared.ready and prepared.plan is not None
    latest_prepared = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=12,
        platform_override="linux-wine",
    )
    assert latest_prepared.ready and latest_prepared.plan is not None
    assert latest_prepared.plan.plan_fingerprint == prepared.plan.plan_fingerprint
    hydrated = {
        **draft,
        "application_path": prepared.presentation["application_path"],
        "configuration_path": prepared.presentation["configuration_path"],
        "storage_path": prepared.presentation["storage_path"],
        "cluster_shared_database": prepared.presentation["storage_path"],
        "secondary_storage_path": prepared.presentation["secondary_storage_path"],
        "outbox_path": prepared.presentation["outbox_path"],
        "working_directory": prepared.presentation["working_directory"],
        "launch_command": prepared.presentation["launch_command"],
        "port": prepared.presentation["port"],
        "secondary_port": prepared.presentation["secondary_port"],
        "vara_runtime_path": prepared.presentation["vara_runtime_path"],
        "vara_ini_path": prepared.presentation["vara_ini_path"],
    }
    presentation = {
        **dict(prepared.presentation),
        "draft_fingerprint": native_draft_fingerprint(hydrated),
    }

    class Publisher:
        def __init__(self) -> None:
            self.current = dict(hydrated)
            self.completed: list[dict[str, object]] = []
            self.presentations: list[dict[str, object]] = []

        def varac_native_draft_payload(self):
            return dict(self.current)

        def set_varac_native_presentation(self, value):
            self.presentations.append(dict(value))
            return True

        def complete_varac_native_apply(self, value):
            self.completed.append(dict(value))
            return True

    publisher = Publisher()
    session = VarACNativeExternalSession(
        journal_id="direct-journal",
        plan=latest_prepared.plan,
        apply_result=SimpleNamespace(ok=True),
        observed={"observed_fingerprint": "direct-observed"},
    )
    host = SettingsTab.__new__(SettingsTab)
    app = QApplication.instance()
    if not isinstance(app, QApplication):
        app = QApplication([])
    QWidget.__init__(host)
    host.multi_radio_store = SimpleNamespace(db_path=tmp_path / "fio.db")
    host._varac_native_preparations = {
        latest_prepared.draft_fingerprint: latest_prepared
    }
    host._rollback_varac_native_session = lambda _session: None
    started: list[object] = []

    def _start(worker, *, on_finished, on_failed):
        started.append(worker)
        on_finished(session)

    host._start_varac_native_job = _start

    SettingsTab._on_varac_native_apply_requested(
        host,
        {"draft": hydrated, "native_presentation": presentation},
        publisher=publisher,
    )

    assert len(publisher.completed) == 1
    completed = publisher.completed[0]
    assert completed["_varac_native_external_session"] is session
    assert tuple(completed["launch_argv"]) == latest_prepared.plan.members[-1].launch_command
    assert completed["launch_recipe"]["status"] == "qualified_managed"

    publisher.current["cluster_ptt_lock"] = False
    SettingsTab._on_varac_native_apply_requested(
        host,
        {"draft": hydrated, "native_presentation": presentation},
        publisher=publisher,
    )
    assert len(started) == 1
    assert len(publisher.completed) == 1
    assert publisher.presentations[-1]["state"] == "needs attention"
    host.deleteLater()
    app.processEvents()


def test_startup_recovery_resolves_pending_and_fio_committed_without_forward_write(tmp_path: Path) -> None:
    store = MultiRadioStore(tmp_path / "fio.db")
    for journal_id, state in (("pending", "pending"), ("committed", "fio_committed")):
        store.begin_varac_native_apply_journal(
            journal_id=journal_id,
            plan_fingerprint=f"plan-{journal_id}",
            operation="create-member",
            writer_key="varac:13.2.7:windows:create-member",
            targets=[],
            desired={},
        )
        if state == "fio_committed":
            store.update_varac_native_apply_journal(
                journal_id,
                state="backup_ready",
                expected_states=("pending",),
                durable=True,
            )
            store.update_varac_native_apply_journal(
                journal_id,
                state="promoting",
                expected_states=("backup_ready",),
                durable=True,
            )
            store.update_varac_native_apply_journal(
                journal_id,
                state="external_applied",
                expected_states=("promoting",),
                observed={},
                durable=True,
            )
            store.update_varac_native_apply_journal(
                journal_id,
                state=state,
                expected_states=("external_applied",),
                observed={},
                durable=True,
            )
    recovered = {row["id"]: row["state"] for row in recover_unfinished_varac_native_applies(store)}
    assert recovered == {"pending": "rolled_back", "committed": "complete"}
    assert store.list_unfinished_varac_native_applies() == []


def test_legacy_gateway_remains_distinct_from_native_email_sender_and_blocker_is_visible(tmp_path: Path) -> None:
    store = MultiRadioStore(tmp_path / "fio.db")
    cluster = store.save_varac_cluster(
        {
            "name": "Legacy",
            "cluster_id": "LEGACY",
            "native_management_state": "managed",
            "email_gateway_sender_device_id": None,
        }
    )
    # Seed the compatibility-only gateway column directly, matching a legacy
    # database row without asking the new API to create an invalid membership.
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE varac_clusters SET gateway_handler_device_id=? WHERE id=?",
            (77, int(cluster["id"])),
        )
        conn.commit()
    listed = next(row for row in store.list_varac_clusters() if int(row["id"]) == int(cluster["id"]))
    assert listed["gateway_handler_device_id"] == 77
    assert listed["email_gateway_sender_device_id"] is None

    store.begin_varac_native_apply_journal(
        journal_id="blocked",
        plan_fingerprint="blocked-plan",
        operation="create-member",
        writer_key="varac:13.2.7:windows:create-member",
        targets=[str(tmp_path / "managed" / "VarAC.ini")],
        desired={},
    )
    blockers = store.varac_native_launch_blockers()
    assert len(blockers) == 1
    assert blockers[0]["id"] == "blocked"
    assert str(tmp_path / "managed" / "VarAC.ini") in blockers[0]["targets"]
