from __future__ import annotations

from pathlib import Path

from freqinout.core.varac_native_preparation import (
    native_draft_fingerprint,
    prepare_varac_native_configuration,
)


def _evidence(tmp_path: Path):
    varac_root = tmp_path / "VarAC"
    vara_root = tmp_path / "VARA"
    varac_root.mkdir()
    vara_root.mkdir()
    executable = varac_root / "VarAC.exe"
    executable.write_bytes(b"fixture VarAC version 13.2.7")
    (vara_root / "VARA.exe").write_bytes(b"fixture VARA executable")
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


def _draft() -> dict[str, object]:
    return {
        "family_key": "varac",
        "mode": "managed",
        "instance_name": "New Radio",
        "owner_label": "New Radio",
        "draft_instance_key": "draft-new-radio",
        "cluster_path": "create_cluster",
        "cluster_name": "Field Cluster",
        "cluster_id": "FIELD",
        "cluster_instance_number": 2,
        "existing_standalone_node_id": 11,
        "existing_standalone_member_number": 1,
        "email_gateway_sender_choice": "existing_member",
        "cluster_ptt_lock": True,
    }


def test_prepare_existing_standalone_and_new_member_is_immutable_and_ready(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    draft = _draft()
    result = prepare_varac_native_configuration(
        draft,
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=7,
        platform_override="linux-wine",
    )
    assert result.ready
    assert result.draft_fingerprint == native_draft_fingerprint(draft)
    assert result.presentation["generation"] == 7
    assert result.presentation["state"] == "ready"
    assert result.plan is not None and len(result.plan.members) == 2
    assert result.plan.email_gateway_sender_member_id == "node:11"
    assert result.plan.members[0].target_path == Path(node["ini_path"])
    assert result.plan.members[1].target_path.name == "VarAC.ini"
    assert result.plan.members[0].vara_target_runtime_folder != result.plan.members[1].vara_target_runtime_folder
    assert result.plan.members[0].vara_changes["Setup"]["TCP Command Port"] == "8300"
    assert result.plan.members[1].vara_changes["Setup"]["TCP Command Port"] == "8310"
    assert result.plan.native_shared_db_path.startswith("Z:\\")
    assert (
        result.plan.members[1].changes["OTHER"]["DBCustomFilePath"]
        == result.plan.native_shared_db_path
    )
    assert result.plan.members[1].changes["VARAHF_CONFIG"]["VarahfMainPath"].startswith(
        "Z:\\"
    )
    assert result.plan.members[1].launch_command[2].startswith("Z:\\")
    assert not result.plan.members[1].target_path.exists()


def test_prepare_requires_stopped_process_and_exact_qualified_version(tmp_path) -> None:
    node, profile = _evidence(tmp_path)
    stopped = prepare_varac_native_configuration(
        _draft(),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=2,
        platform_override="linux-wine",
        process_running=True,
    )
    assert stopped.state == "stop required" and stopped.plan is None

    Path(node["install_path"]).joinpath("VarAC.exe").write_bytes(b"fixture VarAC version 15.0.18")
    unsupported = prepare_varac_native_configuration(
        _draft(),
        varac_nodes=(node,),
        device_profiles=(profile,),
        varac_clusters=(),
        varac_members=(),
        managed_root=tmp_path / "managed",
        generation=3,
        platform_override="linux-wine",
    )
    assert unsupported.state == "manual setup required"
    assert unsupported.plan is None


def test_native_fingerprint_ignores_host_presentation_but_not_operator_intent() -> None:
    draft = _draft()
    first = native_draft_fingerprint(draft)
    draft["varac_native_presentation"] = {"state": "preparing"}
    draft["varac_native_generation"] = 99
    assert native_draft_fingerprint(draft) == first
    draft["cluster_instance_number"] = 3
    assert native_draft_fingerprint(draft) != first
