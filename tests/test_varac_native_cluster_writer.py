from __future__ import annotations

from pathlib import Path

import pytest

from freqinout.core.config_varac_managed import (
    DEFAULT_SUPPORTED_VARAC_VERSIONS, SUPPORTED_VARAC_WRITERS, VarACMemberInput,
    VarACNativeClusterRequest, VarACNativeConfigurationError, VarACWriterCapability,
    VarARuntimeInput, apply_varac_native_cluster_plan, build_varac_native_cluster_plan,
    parse_vara_ini_bytes, parse_varac_ini_bytes, plan_varac_launch_command,
    render_varac_ini, rollback_varac_native_cluster_apply, snapshot_target_state,
    snapshot_vara_runtime_files, supported_varac_versions,
)


def _varac_ini() -> bytes:
    return (
        "; operator comment\r\n[OTHER]\r\nDBCustomFilePath=C:\\Old\\VarAC.db\r\nUnknownOther=keep\r\n\r\n"
        "[VARAHF_CONFIG]\r\nVarahfMainPath=old\r\nVarahfMainPort=8300\r\nVarahfMainHost=127.0.0.1\r\n"
        "VarahfEnableKissInterface=ON\r\nVarahfMainKissPort=8302\r\nVarahfMonitorPath=old\r\n"
        "VarahfMonitorPort=8303\r\nVarahfLaunchOnModemConnect=OFF\r\nVendorOnly=keep\r\n"
    ).encode()


def _vara_ini() -> bytes:
    return (
        "; VARA comment\r\n[Setup]\r\nTCP Command Port=8300\r\nEnable KISS=1\r\n"
        "KISS Port=8302\r\nVendor Setup=keep\r\n\r\n[Monitor]\r\nMonitor Mode=1\r\nKeepMonitorValue=keep\r\n"
    ).encode()


def _runtime(tmp_path: Path, number: int) -> tuple[Path, Path]:
    source = tmp_path / f"vara-source-{number}"
    source.mkdir()
    (source / "VARA.ini").write_bytes(_vara_ini())
    (source / "VARA.exe").write_bytes(f"vara executable {number}".encode())
    (source / "unknown.dat").write_bytes(f"unknown payload {number}".encode())
    return source, tmp_path / f"vara-managed-{number}"


def _varahf(target: Path, number: int) -> dict[str, str]:
    command = 8300 + number * 10
    exe = str(target / "VARA.exe")
    return {
        "VarahfMainPath": exe, "VarahfMainPort": str(command), "VarahfMainHost": "127.0.0.1",
        "VarahfEnableKissInterface": "ON", "VarahfMainKissPort": str(command + 2),
        "VarahfMonitorPath": exe, "VarahfMonitorPort": str(command + 3), "VarahfLaunchOnModemConnect": "ON",
    }


def _vara_settings(number: int) -> dict[str, dict[str, str]]:
    command = 8300 + number * 10
    return {"Setup": {"TCP Command Port": str(command), "Enable KISS": "1", "KISS Port": str(command + 2)}, "Monitor": {"Monitor Mode": "1"}}


def _request(tmp_path: Path, count: int = 2) -> VarACNativeClusterRequest:
    tmp_path.mkdir(parents=True, exist_ok=True)
    members = []
    for index in range(1, count + 1):
        varac = tmp_path / f"member-{index}.ini"
        varac.write_bytes(_varac_ini())
        source_root, target_root = _runtime(tmp_path, index)
        source_ini = source_root / "VARA.ini"
        runtime = VarARuntimeInput(
            source_runtime_folder=source_root, target_runtime_folder=target_root,
            source=parse_vara_ini_bytes(source_ini, source_ini.read_bytes()), target=snapshot_target_state(target_root / "VARA.ini"),
            settings=_vara_settings(index), files=snapshot_vara_runtime_files(source_root),
        )
        members.append(VarACMemberInput(
            member_id=f"member-{index}", source=parse_varac_ini_bytes(varac, varac.read_bytes()), target=snapshot_target_state(varac),
            member_number=index, vara_settings=_varahf(target_root, index), executable_path="VarAC.exe", vara_runtime=runtime,
        ))
    return VarACNativeClusterRequest(
        version="13.2.7", platform="windows", operation="create-member", members=tuple(members),
        shared_db_path=str(tmp_path / "shared" / "VarAC.db"), allowed_roots=(tmp_path,), email_gateway_sender_member_id="member-1",
    )


def test_registry_is_only_locally_qualified_13_2_7() -> None:
    assert DEFAULT_SUPPORTED_VARAC_VERSIONS == frozenset({"13.2.7"})
    assert supported_varac_versions() == frozenset({"13.2.7"})
    assert all(item.version == "13.2.7" for item in SUPPORTED_VARAC_WRITERS)


def test_plan_fingerprint_covers_reviewed_native_policy_and_values(tmp_path) -> None:
    request = _request(tmp_path)
    baseline = build_varac_native_cluster_plan(request).plan_fingerprint
    without_sender = build_varac_native_cluster_plan(
        VarACNativeClusterRequest(
            **{**request.__dict__, "email_gateway_sender_member_id": ""}
        )
    ).plan_fingerprint
    without_ptt_lock = build_varac_native_cluster_plan(
        VarACNativeClusterRequest(**{**request.__dict__, "ptt_lock_enabled": False})
    ).plan_fingerprint
    assert len({baseline, without_sender, without_ptt_lock}) == 3


def test_managed_cluster_member_requires_varac_to_launch_its_vara_modem(tmp_path) -> None:
    request = _request(tmp_path, 1)
    member = request.members[0]
    settings = dict(member.vara_settings)
    settings["VarahfLaunchOnModemConnect"] = "OFF"
    member = VarACMemberInput(**{**member.__dict__, "vara_settings": settings})

    with pytest.raises(
        VarACNativeConfigurationError,
        match="launch their node-local VARA modem",
    ):
        build_varac_native_cluster_plan(
            VarACNativeClusterRequest(
                **{**request.__dict__, "members": (member,)}
            )
        )


def test_clones_distinct_runtime_updates_real_keys_and_preserves_unknown_files(tmp_path) -> None:
    request = _request(tmp_path)
    plan = build_varac_native_cluster_plan(request)
    member = plan.members[0]
    rendered = render_varac_ini(request.members[0].source, member.changes, capability=plan.capability)
    assert b"VarahfMainPath=" + str(member.vara_target_runtime_folder / "VARA.exe").encode() in rendered
    assert b"VarahfMainPort=8310\r\n" in rendered and b"VARACommandPort" not in rendered
    result = apply_varac_native_cluster_plan(plan, backup_root=tmp_path / "backups")
    assert result.ok and len(result.backup.items) == 4
    runtime = tmp_path / "vara-managed-1"
    assert (runtime / "VARA.exe").read_bytes() == b"vara executable 1"
    assert (runtime / "unknown.dat").read_bytes() == b"unknown payload 1"
    vara = (runtime / "VARA.ini").read_bytes()
    assert b"TCP Command Port=8310\r\n" in vara and b"KISS Port=8312\r\n" in vara
    assert b"Vendor Setup=keep\r\n" in vara and b"KeepMonitorValue=keep\r\n" in vara
    assert b"VarahfMonitorPort=8313\r\n" in (tmp_path / "member-1.ini").read_bytes()


def test_apply_creates_only_reviewed_shared_and_member_directories_and_rollback_cleans_empty_ones(tmp_path) -> None:
    request = _request(tmp_path, 1)
    bbs = tmp_path / "VarAC" / "BBS"
    archive = bbs / "Archive"
    incoming = tmp_path / "managed" / "radio" / "incoming"
    outbox = tmp_path / "managed" / "radio" / "outbox"
    request = VarACNativeClusterRequest(
        **{
            **request.__dict__,
            "shared_bbs_path": str(bbs),
            "shared_bbs_archive_path": str(archive),
            "managed_directories": (incoming, outbox, bbs, archive),
        }
    )
    plan = build_varac_native_cluster_plan(request)

    result = apply_varac_native_cluster_plan(plan, backup_root=tmp_path / "backups")

    assert result.ok
    assert all(path.is_dir() for path in (incoming, outbox, bbs, archive))
    assert {item.target for item in result.items if item.action_type == "create_directory"} == {
        str(incoming), str(outbox), str(bbs), str(archive)
    }

    rolled_back = rollback_varac_native_cluster_apply(result)

    assert rolled_back.restore is not None and rolled_back.restore.ok
    assert not incoming.exists() and not outbox.exists()
    assert not archive.exists() and not bbs.exists()


def test_managed_bbs_directory_allows_stable_wine_desktop_symlink(tmp_path) -> None:
    request = _request(tmp_path / "plan", 1)
    host_desktop = tmp_path / "home" / "bill" / "Desktop"
    host_desktop.mkdir(parents=True)
    wine_user = tmp_path / "prefix" / "drive_c" / "users" / "bill"
    wine_user.mkdir(parents=True)
    desktop_alias = wine_user / "Desktop"
    desktop_alias.symlink_to(host_desktop, target_is_directory=True)
    bbs = desktop_alias / "VaraFile" / "BBS"
    archive = bbs / "Archive"
    request = VarACNativeClusterRequest(
        **{
            **request.__dict__,
            "shared_bbs_path": str(bbs),
            "shared_bbs_archive_path": str(archive),
            "managed_directories": (bbs, archive),
            "allowed_roots": (*request.allowed_roots, bbs, archive),
        }
    )

    plan = build_varac_native_cluster_plan(request)
    result = apply_varac_native_cluster_plan(plan, backup_root=tmp_path / "backups")

    assert result.ok
    assert (host_desktop / "VaraFile" / "BBS").is_dir()
    assert (host_desktop / "VaraFile" / "BBS" / "Archive").is_dir()


def test_managed_bbs_directory_rejects_retargeted_wine_symlink(tmp_path) -> None:
    request = _request(tmp_path / "plan", 1)
    first_desktop = tmp_path / "first-desktop"
    second_desktop = tmp_path / "second-desktop"
    first_desktop.mkdir()
    second_desktop.mkdir()
    wine_user = tmp_path / "prefix" / "drive_c" / "users" / "bill"
    wine_user.mkdir(parents=True)
    desktop_alias = wine_user / "Desktop"
    desktop_alias.symlink_to(first_desktop, target_is_directory=True)
    bbs = desktop_alias / "VaraFile" / "BBS"
    request = VarACNativeClusterRequest(
        **{
            **request.__dict__,
            "shared_bbs_path": str(bbs),
            "managed_directories": (bbs,),
            "allowed_roots": (*request.allowed_roots, bbs),
        }
    )
    plan = build_varac_native_cluster_plan(request)

    desktop_alias.unlink()
    desktop_alias.symlink_to(second_desktop, target_is_directory=True)
    result = apply_varac_native_cluster_plan(plan, backup_root=tmp_path / "backups")

    assert not result.ok
    assert "directory alias changed before apply" in result.error
    assert not (second_desktop / "VaraFile").exists()


def test_managed_bbs_directory_rejects_broken_wine_symlink_ancestor(tmp_path) -> None:
    request = _request(tmp_path / "plan", 1)
    wine_user = tmp_path / "prefix" / "drive_c" / "users" / "bill"
    wine_user.mkdir(parents=True)
    desktop_alias = wine_user / "Desktop"
    desktop_alias.symlink_to(tmp_path / "missing-desktop", target_is_directory=True)
    bbs = desktop_alias / "VaraFile" / "BBS"
    request = VarACNativeClusterRequest(
        **{
            **request.__dict__,
            "shared_bbs_path": str(bbs),
            "managed_directories": (bbs,),
            "allowed_roots": (*request.allowed_roots, bbs),
        }
    )

    with pytest.raises(VarACNativeConfigurationError, match="Broken symlink"):
        build_varac_native_cluster_plan(request)


@pytest.mark.parametrize("phase", ["preflight", "backup", "stage", "validate_staged", "validate_promoted"])
def test_phase_failures_remove_new_runtime_and_restore_varac_bytes(tmp_path, phase: str) -> None:
    request = _request(tmp_path)
    original = {path: path.read_bytes() for path in tmp_path.glob("member-*.ini")}
    result = apply_varac_native_cluster_plan(build_varac_native_cluster_plan(request), backup_root=tmp_path / "backups", fail_at=phase)
    assert not result.ok and {path: path.read_bytes() for path in original} == original
    assert not (tmp_path / "vara-managed-1").exists() and not (tmp_path / "vara-managed-2").exists()
    if phase not in {"preflight", "backup"}:
        assert result.restore is not None and result.restore.ok


def test_partial_promotion_rolls_back_entire_new_runtime_byte_for_byte(tmp_path) -> None:
    request = _request(tmp_path)
    original = {path: path.read_bytes() for path in tmp_path.glob("member-*.ini")}
    calls = {"promote": 0}
    def inject(phase: str) -> None:
        if phase == "promote":
            calls["promote"] += 1
            if calls["promote"] == 2:
                # Rollback must cover unexpected implementation/runtime faults,
                # not only the writer's anticipated I/O exception classes.
                raise RuntimeError("injected unexpected promotion fault")
    result = apply_varac_native_cluster_plan(build_varac_native_cluster_plan(request), backup_root=tmp_path / "backups", failure_injector=inject)
    assert not result.ok and result.restore is not None and result.restore.ok
    assert {path: path.read_bytes() for path in original} == original
    assert not (tmp_path / "vara-managed-1").exists()


def test_rejects_existing_target_overlap_symlink_and_source_staleness(tmp_path) -> None:
    request = _request(tmp_path, 1)
    member = request.members[0]
    existing = VarARuntimeInput(**{**member.vara_runtime.__dict__, "target_runtime_exists": True})
    member = VarACMemberInput(**{**member.__dict__, "vara_runtime": existing})
    with pytest.raises(VarACNativeConfigurationError, match="absent"):
        build_varac_native_cluster_plan(VarACNativeClusterRequest(**{**request.__dict__, "members": (member,)}))

    request = _request(tmp_path / "second", 1)
    member = request.members[0]
    linked = member.vara_runtime.source_runtime_folder / "evil.dll"
    linked.symlink_to(member.vara_runtime.source_runtime_folder / "VARA.exe")
    with pytest.raises(VarACNativeConfigurationError, match="symlink"):
        snapshot_vara_runtime_files(member.vara_runtime.source_runtime_folder)
    linked.unlink()
    (member.vara_runtime.source_runtime_folder / "unknown.dat").write_bytes(b"changed")
    result = apply_varac_native_cluster_plan(build_varac_native_cluster_plan(request), backup_root=tmp_path / "backups")
    assert "Stale VARA runtime source snapshot" in result.error


def test_rejects_port_collisions_and_wrong_managed_executable_path(tmp_path) -> None:
    request = _request(tmp_path)
    first, second = request.members
    collision_runtime = VarARuntimeInput(**{**second.vara_runtime.__dict__, "settings": _vara_settings(1)})
    collision = VarACMemberInput(**{**second.__dict__, "vara_settings": _varahf(second.vara_runtime.target_runtime_folder, 1), "vara_runtime": collision_runtime})
    with pytest.raises(VarACNativeConfigurationError, match="Conflicting VARA"):
        build_varac_native_cluster_plan(VarACNativeClusterRequest(**{**request.__dict__, "members": (first, collision)}))
    wrong = dict(_varahf(first.vara_runtime.target_runtime_folder, 1)); wrong["VarahfMainPath"] = "/operator/VARA.exe"
    bad = VarACMemberInput(**{**first.__dict__, "vara_settings": wrong})
    with pytest.raises(VarACNativeConfigurationError, match="must point"):
        build_varac_native_cluster_plan(VarACNativeClusterRequest(**{**request.__dict__, "members": (bad,)}))


def test_windows_linux_wine_and_injected_future_version(tmp_path) -> None:
    assert plan_varac_launch_command(platform="windows", executable_path="VarAC.exe", ini_path="a.ini") == ("VarAC.exe", "a.ini")
    assert plan_varac_launch_command(platform="linux-wine", executable_path="/opt/VarAC.exe", ini_path="/tmp/a.ini") == ("wine", "/opt/VarAC.exe", "/tmp/a.ini")
    request = _request(tmp_path, 1)
    writer = VarACWriterCapability(version="15.0.18", platform="windows", operation="create-member", layout_fingerprint=SUPPORTED_VARAC_WRITERS[0].layout_fingerprint, vara_layout_fingerprint=SUPPORTED_VARAC_WRITERS[0].vara_layout_fingerprint)
    assert build_varac_native_cluster_plan(VarACNativeClusterRequest(**{**request.__dict__, "version": "15.0.18"}), capabilities=(writer,)).capability == writer
