from __future__ import annotations

import hashlib
import shutil
import sqlite3
from pathlib import Path

import pytest

from freqinout.core.config_varac_managed import (
    VarACMemberInput,
    VarACNativeClusterRequest,
    VarARuntimeInput,
    apply_varac_native_cluster_plan,
    build_varac_native_cluster_plan,
    parse_vara_ini_bytes,
    parse_varac_ini_bytes,
    snapshot_target_state,
    snapshot_vara_runtime_files,
)
from freqinout.core.multi_radio_store import MultiRadioStore, ensure_multi_radio_settings_schema
from freqinout.core.varac_native_transaction import (
    apply_varac_native_transaction,
    begin_varac_native_external_apply,
    complete_varac_native_external_session,
    mark_varac_native_fio_committed,
    recover_unfinished_varac_native_applies,
    rollback_varac_native_external_session,
)


def _varac_ini() -> bytes:
    return (
        "[OTHER]\r\nDBCustomFilePath=C:\\Old\\VarAC.db\r\nKeep=operator\r\n"
        "[VARAHF_CONFIG]\r\nVarahfMainPath=C:\\VARA\\VARA.exe\r\n"
        "VarahfMainPort=8300\r\nVarahfMainHost=127.0.0.1\r\n"
        "VarahfEnableKissInterface=ON\r\nVarahfMainKissPort=8100\r\n"
        "VarahfMonitorPath=\r\nVarahfMonitorPort=8350\r\n"
        "VarahfLaunchOnModemConnect=ON\r\n"
        "[UNRELATED]\r\nPreserve=exactly\r\n"
    ).encode("utf-8")


def _vara_ini() -> bytes:
    return (
        "[Setup]\r\nTCP Command Port=8300\r\nEnable KISS=1\r\nKISS Port=8100\r\n"
        "OperatorValue=keep\r\n[Monitor]\r\nMonitor Mode=0\r\n"
    ).encode("utf-8")


def _plan(tmp_path: Path):
    varac = tmp_path / "VarAC-member.ini"
    source_runtime = tmp_path / "VARA-source"
    runtime = tmp_path / "VARA-member"
    source_runtime.mkdir()
    source_vara = source_runtime / "VARA.ini"
    vara = runtime / "VARA.ini"
    varac.write_bytes(_varac_ini())
    source_vara.write_bytes(_vara_ini())
    (source_runtime / "VARA.exe").write_bytes(b"qualified fixture executable")
    member = VarACMemberInput(
        member_id="radio-a",
        source=parse_varac_ini_bytes(varac, varac.read_bytes()),
        target=snapshot_target_state(varac),
        member_number=1,
        vara_settings={
            "VarahfMainPath": str(runtime / "VARA.exe"),
            "VarahfMainPort": "8310",
            "VarahfMainHost": "127.0.0.1",
            "VarahfEnableKissInterface": "ON",
            "VarahfMainKissPort": "8110",
            "VarahfMonitorPath": str(runtime / "VARA.exe"),
            "VarahfMonitorPort": "8360",
            "VarahfLaunchOnModemConnect": "ON",
        },
        executable_path=r"C:\VarAC\VarAC.exe",
        vara_runtime=VarARuntimeInput(
            source_runtime_folder=source_runtime,
            target_runtime_folder=runtime,
            source=parse_vara_ini_bytes(source_vara, source_vara.read_bytes()),
            target=snapshot_target_state(vara),
            settings={
                "Setup": {"TCP Command Port": "8310", "Enable KISS": "1", "KISS Port": "8110"},
                "Monitor": {"Monitor Mode": "0"},
            },
            files=snapshot_vara_runtime_files(source_runtime),
        ),
    )
    request = VarACNativeClusterRequest(
        version="13.2.7",
        platform="windows",
        operation="create-member",
        members=(member,),
        shared_db_path=str(tmp_path / "shared" / "VarAC.db"),
        allowed_roots=(tmp_path,),
        email_gateway_sender_member_id="radio-a",
    )
    return build_varac_native_cluster_plan(request), varac, vara


def test_additive_schema_keeps_legacy_gateway_as_unconfirmed_evidence(tmp_path) -> None:
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE varac_clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            cluster_id TEXT NOT NULL,
            shared_db_path TEXT,
            counters_refresh_sec INTEGER NOT NULL DEFAULT 30,
            ptt_lock_enabled INTEGER NOT NULL DEFAULT 0,
            gateway_handler_device_id INTEGER,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        );
        INSERT INTO varac_clusters
            (name, cluster_id, gateway_handler_device_id, created_utc, updated_utc)
        VALUES ('Legacy', 'LEGACY', 77, 'old', 'old');
        """
    )
    ensure_multi_radio_settings_schema(conn)
    migrated_cluster_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(varac_clusters)")
    }
    assert {"shared_bbs_path", "shared_bbs_archive_path"} <= migrated_cluster_columns
    row = conn.execute(
        "SELECT gateway_handler_device_id, email_gateway_sender_device_id, native_management_state, "
        "shared_bbs_path, shared_bbs_archive_path "
        "FROM varac_clusters WHERE id=1"
    ).fetchone()
    assert row == (77, None, "operator", None, None)
    conn.close()


def test_success_commits_external_files_fio_state_and_journal(tmp_path) -> None:
    plan, varac, vara = _plan(tmp_path)
    store = MultiRadioStore(tmp_path / "fio.db")

    def persist(target_store, observed):
        return target_store.save_varac_node(
            {
                "name": "Radio A",
                "db_path": plan.shared_db_path,
                "ini_path": str(varac),
                "vara_runtime_path": str(vara.parent),
                "vara_ini_path": str(vara),
                "native_management_state": "managed",
                "native_writer_key": "varac:13.2.7",
                "desired_fingerprint": plan.plan_fingerprint,
                "observed_fingerprint": observed["observed_fingerprint"],
            }
        )

    result = apply_varac_native_transaction(
        store=store,
        plan=plan,
        backup_root=tmp_path / "backups",
        persist=persist,
    )
    assert result.ok
    assert b"ClusterEnabled=ON\r\n" in varac.read_bytes()
    assert b"TCP Command Port=8310\r\n" in vara.read_bytes()
    assert store.get_varac_native_apply_journal(result.journal_id)["state"] == "complete"
    node = store.list_varac_nodes()[0]
    assert node["db_path"] == plan.shared_db_path
    assert node["native_management_state"] == "managed"


def test_fio_commit_failure_rolls_back_external_and_store_state(tmp_path) -> None:
    plan, varac, vara = _plan(tmp_path)
    original_varac = varac.read_bytes()
    assert not vara.parent.exists()
    store = MultiRadioStore(tmp_path / "fio.db")

    def fail_after_partial_store(target_store, _observed):
        target_store.save_varac_node({"name": "Must Roll Back"})
        raise RuntimeError("injected FIO store failure")

    result = apply_varac_native_transaction(
        store=store,
        plan=plan,
        backup_root=tmp_path / "backups",
        persist=fail_after_partial_store,
    )
    assert not result.ok and not result.needs_recovery
    assert varac.read_bytes() == original_varac
    assert not vara.parent.exists()
    assert store.list_varac_nodes() == []
    assert store.get_varac_native_apply_journal(result.journal_id)["state"] == "rolled_back"


def test_split_external_session_commits_journal_with_the_guided_store_transaction(tmp_path) -> None:
    plan, _varac, _vara = _plan(tmp_path)
    store = MultiRadioStore(tmp_path / "fio.db")
    session = begin_varac_native_external_apply(
        store=store,
        plan=plan,
        backup_root=tmp_path / "backups",
    )
    assert session.ok
    assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "external_applied"

    with store.guided_save_transaction() as transaction:
        store.save_varac_node({"name": "Radio A", "db_path": plan.shared_db_path})
        mark_varac_native_fio_committed(store, session)
        assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "fio_committed"
        transaction.complete()

    complete_varac_native_external_session(store, session)
    assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "complete"


def test_split_external_session_rolls_back_when_guided_store_transaction_is_cancelled(tmp_path) -> None:
    plan, varac, vara = _plan(tmp_path)
    original_varac = varac.read_bytes()
    store = MultiRadioStore(tmp_path / "fio.db")
    session = begin_varac_native_external_apply(
        store=store,
        plan=plan,
        backup_root=tmp_path / "backups",
    )
    assert session.ok and vara.exists()

    with store.guided_save_transaction():
        store.save_varac_node({"name": "Must not persist"})
        # No complete(): the FIO transaction deliberately rolls back.

    result = rollback_varac_native_external_session(
        store,
        session,
        error="operator cancelled Add Radio",
    )
    assert not result.committed and not result.needs_recovery
    assert store.list_varac_nodes() == []
    assert varac.read_bytes() == original_varac
    assert not vara.parent.exists()
    assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "rolled_back"

    # Review Again can revisit the same retained draft/session. Compensation
    # must recognize the durable terminal journal and avoid a second restore.
    second = rollback_varac_native_external_session(
        store,
        session,
        error="operator returned to Review Again",
    )
    assert not second.committed and not second.needs_recovery
    assert store.get_varac_native_apply_journal(session.journal_id)["error"] == "operator cancelled Add Radio"
    assert varac.read_bytes() == original_varac
    assert not vara.parent.exists()
    assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "rolled_back"


def test_restart_completes_fio_committed_journal_without_rewriting_files(tmp_path) -> None:
    plan, varac, vara = _plan(tmp_path)
    store = MultiRadioStore(tmp_path / "fio.db")
    result = apply_varac_native_transaction(
        store=store,
        plan=plan,
        backup_root=tmp_path / "backups",
        persist=lambda _store, observed: observed["observed_fingerprint"],
        failure_injector=lambda phase: (_ for _ in ()).throw(RuntimeError("crash after commit"))
        if phase == "after_fio_commit"
        else None,
    )
    assert result.committed and result.needs_recovery
    assert store.get_varac_native_apply_journal(result.journal_id)["state"] == "fio_committed"
    before = (varac.read_bytes(), vara.read_bytes())
    recovered = recover_unfinished_varac_native_applies(store)
    assert recovered and recovered[0]["state"] == "complete"
    assert (varac.read_bytes(), vara.read_bytes()) == before


def test_rollback_never_restores_a_fio_committed_session(tmp_path) -> None:
    plan, varac, vara = _plan(tmp_path)
    store = MultiRadioStore(tmp_path / "fio.db")
    session = begin_varac_native_external_apply(
        store=store,
        plan=plan,
        backup_root=tmp_path / "backups",
    )
    assert session.ok
    with store.guided_save_transaction() as transaction:
        mark_varac_native_fio_committed(store, session)
        transaction.complete()
    before = (varac.read_bytes(), vara.read_bytes())

    result = rollback_varac_native_external_session(store, session)

    assert result.committed and not result.needs_recovery
    assert (varac.read_bytes(), vara.read_bytes()) == before
    assert store.get_varac_native_apply_journal(session.journal_id)["state"] == "fio_committed"


def test_restart_rolls_back_a_promoting_external_apply_from_manifest(tmp_path) -> None:
    plan, varac, vara = _plan(tmp_path)
    original_varac = varac.read_bytes()
    assert not vara.parent.exists()
    store = MultiRadioStore(tmp_path / "fio.db")
    entry_id = "simulated-crash"
    store.begin_varac_native_apply_journal(
        journal_id=entry_id,
        plan_fingerprint=plan.plan_fingerprint,
        operation=plan.operation,
        writer_key="varac:13.2.7:test",
        targets=[str(varac), str(vara.parent)],
        desired={"plan_fingerprint": plan.plan_fingerprint},
    )
    state = "pending"

    def journal_callback(next_state, backup):
        nonlocal state
        from dataclasses import asdict

        store.update_varac_native_apply_journal(
            entry_id,
            state=next_state,
            expected_states=(state,),
            backup_manifest=asdict(backup),
        )
        state = next_state

    applied = apply_varac_native_cluster_plan(
        plan,
        backup_root=tmp_path / "backups",
        state_callback=journal_callback,
    )
    assert applied.ok and state == "promoting"
    assert varac.read_bytes() != original_varac and vara.exists()
    recovered = recover_unfinished_varac_native_applies(store)
    assert recovered[0]["state"] == "rolled_back"
    assert varac.read_bytes() == original_varac
    assert not vara.parent.exists()


def test_managed_cluster_claim_is_cluster_owned_and_legacy_gateway_is_not_copied(tmp_path) -> None:
    store = MultiRadioStore(tmp_path / "fio.db")
    cluster = store.save_varac_cluster(
        {
            "name": "Managed",
            "cluster_id": "MANAGED",
            "shared_db_path": str(tmp_path / "shared" / "VarAC.db"),
            "native_management_state": "managed",
            "native_writer_key": "varac:13.2.7",
            "resource_claims_json": [
                {
                    "kind": "database",
                    "path": str(tmp_path / "shared" / "VarAC.db"),
                    "owner_type": "varac_cluster",
                    "owner_id": "MANAGED",
                    "exclusive": False,
                }
            ],
        }
    )
    claims = cluster["resource_claims_json"]
    assert '"exclusive": false' in claims
    assert cluster["gateway_handler_device_id"] is None
    assert cluster["email_gateway_sender_device_id"] is None


def test_production_shaped_migration_uses_copy_and_never_writes_source(tmp_path) -> None:
    source = Path("/Users/bill/RadioTools/FIO_DB_prod/current/freqinout.db")
    if not source.is_file():
        pytest.skip("Operator production-shaped fixture is not available.")
    before_stat = source.stat()
    before_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    copied = tmp_path / "production-shaped.db"
    shutil.copy2(source, copied)
    conn = sqlite3.connect(copied)
    ensure_multi_radio_settings_schema(conn)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(varac_clusters)")}
    assert {"email_gateway_sender_device_id", "native_management_state", "resource_claims_json"} <= columns
    assert conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='varac_native_apply_journal'"
    ).fetchone()
    conn.close()
    after_stat = source.stat()
    assert (after_stat.st_size, after_stat.st_mtime_ns) == (before_stat.st_size, before_stat.st_mtime_ns)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before_hash
