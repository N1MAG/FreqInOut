#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RADIO_TOOLS = Path("/Users/bill/RadioTools")
DEFAULT_LAB_ROOT = Path("/Users/bill/RadioCode/WORK/MultiRig/TestLab")

PROFILE_PORTS = {
    "a": {"flrig": 12345, "fldigi": 7362, "js8": 2442},
    "b": {"flrig": 12346, "fldigi": 7363, "js8": 2443},
    "c": {"flrig": 12347, "fldigi": 7364, "js8": 2444},
    "d": {"flrig": 12348, "fldigi": 7365, "js8": 2445},
}


def _json(value: Any) -> str:
    return json.dumps(value)


def _write_kv(db_path: Path, values: dict[str, Any]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT OR REPLACE INTO kv(key, value) VALUES(?, ?)",
            [(key, _json(value)) for key, value in sorted(values.items())],
        )
        conn.commit()
    finally:
        conn.close()


def _profile_config_dir(lab_root: Path, scenario: str) -> Path:
    return lab_root / "profiles" / scenario


def _radio_tools_paths(radio_tools: Path) -> dict[str, str]:
    programs = radio_tools / "Programs"
    software_files = radio_tools / "software_test_files"
    return {
        "path_flrig": str(radio_tools / "bin" / "flrig"),
        "path_fldigi": str(radio_tools / "bin" / "fldigi"),
        "path_flamp": str(radio_tools / "bin" / "flamp"),
        "path_flmsg": str(radio_tools / "bin" / "flmsg"),
        "path_js8call": str(radio_tools / "bin" / "js8call"),
        "path_js8spotter": str(radio_tools / "bin" / "js8spotter"),
        "path_commstat": str(programs / "CommStat" / "commstat.py"),
        "varac_path": str(radio_tools / "bin" / "varac"),
        "js8_directed_path": str(software_files / "js8" / "DIRECTED.TXT"),
        "js8_forms_path": str(software_files / "js8"),
        "message_paths": {
            "js8": str(software_files / "js8"),
            "varac": str(programs / "VarAC_files" / "INCOMING"),
            "flamp": str(programs / "VarAC_files" / "flampTTA1"),
            "flmsg": str(software_files / "fldigi"),
        },
        "varac_incoming_dir": str(programs / "VarAC_files" / "INCOMING"),
        "varac_outbox_dir": str(programs / "VarAC_files" / "OUTGOING"),
        "varac_bbs_dir": str(programs / "VarAC_files" / "BBS"),
        "varac_bbs_vault_root_dir": str(programs / "VarAC_files" / "FIO_BBS_Vault"),
    }


def _base_settings(radio_tools: Path, profile: str = "a") -> dict[str, Any]:
    ports = PROFILE_PORTS[profile]
    settings = {
        "callsign": "N1MAG",
        "grid_square": "DM79QJ",
        "timezone": "America/Denver",
        "operator_name": "FIO Multi-Rig Lab",
        "flrig_host": "127.0.0.1",
        "flrig_port": ports["flrig"],
        "fldigi_host": "127.0.0.1",
        "fldigi_port": ports["fldigi"],
        "js8_host": "127.0.0.1",
        "js8_port": ports["js8"],
        "autostart_flrig": False,
        "autostart_fldigi": False,
        "autostart_js8call": False,
        "autostart_js8spotter": False,
        "autostart_varac": False,
        "autostart_flamp": False,
        "autostart_flmsg": False,
        "autostart_commstat": False,
        "operating_groups": [
            {"group": "MAGNET", "band": "20M", "frequency": "14.115"},
            {"group": "MAGNET", "band": "40M", "frequency": "7.115"},
            {"group": "MAGNET", "band": "80M", "frequency": "3.585"},
        ],
    }
    settings.update(_radio_tools_paths(radio_tools))
    return settings


def prepare_profile(
    *,
    scenario: str,
    lab_root: Path,
    radio_tools: Path,
    reset: bool,
) -> Path:
    config_root = _profile_config_dir(lab_root, scenario)
    if reset and config_root.exists():
        shutil.rmtree(config_root)
    config_dir = config_root / "config"
    db_path = config_dir / "freqinout.db"

    if scenario == "fresh":
        config_dir.mkdir(parents=True, exist_ok=True)
    elif scenario == "upgrade":
        values = _base_settings(radio_tools, "a")
        values["lab_scenario"] = "single-rig-upgrade"
        _write_kv(db_path, values)
    else:
        raise ValueError(f"Unsupported scenario: {scenario}")

    return config_root


def _python_bin() -> str:
    venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def run_fio(config_root: Path) -> int:
    env = dict(os.environ)
    env["FREQINOUT_CONFIG_DIR"] = str(config_root)
    env.setdefault("QT_QPA_PLATFORM", os.environ.get("QT_QPA_PLATFORM", ""))
    if not env["QT_QPA_PLATFORM"]:
        env.pop("QT_QPA_PLATFORM", None)
    cmd = [_python_bin(), "-m", "freqinout.main"]
    print("FIO config:", config_root)
    print("Command:", " ".join(cmd))
    return subprocess.call(cmd, cwd=str(REPO_ROOT), env=env)


def lab_command(radio_tools: Path, action: str, mode: str) -> int:
    script_map = {
        "start": "start-test-lab.sh",
        "stop": "stop-test-lab.sh",
        "status": "status-test-lab.sh",
    }
    script = radio_tools / "bin" / script_map[action]
    if not script.exists():
        raise FileNotFoundError(script)
    cmd = [str(script)]
    if action == "start":
        cmd.append(mode)
    return subprocess.call(cmd)


def check_profile(config_root: Path) -> int:
    env = dict(os.environ)
    env["FREQINOUT_CONFIG_DIR"] = str(config_root)
    code = """
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import build_multi_rig_runtime_status
settings = SettingsManager()
status = build_multi_rig_runtime_status(MultiRadioStore(), settings_values=settings.all())
print(f"startup_mode={status.startup_mode}")
print(f"migration_current={status.migration_current}")
print(f"migration_deferred={status.migration_deferred}")
print(f"primary_device_profile_id={status.primary_device_profile_id}")
print(f"active_device_profile_ids={status.active_device_profile_ids}")
settings.close()
"""
    return subprocess.call([_python_bin(), "-c", code], cwd=str(REPO_ROOT), env=env)


def print_paths(lab_root: Path) -> None:
    for scenario in ("upgrade", "fresh"):
        root = _profile_config_dir(lab_root, scenario)
        print(f"{scenario}:")
        print(f"  config root: {root}")
        print(f"  settings db: {root / 'config' / 'freqinout.db'}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare and run repeatable FIO multi-rig lab profiles.")
    parser.add_argument("--lab-root", type=Path, default=DEFAULT_LAB_ROOT)
    parser.add_argument("--radio-tools", type=Path, default=DEFAULT_RADIO_TOOLS)
    sub = parser.add_subparsers(dest="command", required=True)

    prep = sub.add_parser("prepare", help="Prepare an isolated FIO profile.")
    prep.add_argument("scenario", choices=("upgrade", "fresh", "all"))
    prep.add_argument("--reset", action="store_true", help="Delete the profile before preparing it.")

    run = sub.add_parser("run", help="Run FIO against an isolated profile.")
    run.add_argument("scenario", choices=("upgrade", "fresh"))
    run.add_argument("--prepare", action="store_true", help="Prepare the profile before launching FIO.")
    run.add_argument("--reset", action="store_true", help="Reset the profile when preparing.")

    lab = sub.add_parser("lab", help="Start, stop, or inspect the RadioTools emulator lab.")
    lab.add_argument("action", choices=("start", "stop", "status"))
    lab.add_argument("--mode", choices=("single", "multi", "quad"), default="single")

    check = sub.add_parser("check", help="Print FIO startup status for an isolated profile.")
    check.add_argument("scenario", choices=("upgrade", "fresh"))
    check.add_argument("--prepare", action="store_true", help="Prepare the profile before checking.")
    check.add_argument("--reset", action="store_true", help="Reset the profile when preparing.")

    sub.add_parser("paths", help="Print profile paths.")

    args = parser.parse_args(argv)
    lab_root = args.lab_root.expanduser()
    radio_tools = args.radio_tools.expanduser()

    if args.command == "prepare":
        scenarios = ("upgrade", "fresh") if args.scenario == "all" else (args.scenario,)
        for scenario in scenarios:
            root = prepare_profile(
                scenario=scenario,
                lab_root=lab_root,
                radio_tools=radio_tools,
                reset=bool(args.reset),
            )
            print(f"prepared {scenario}: {root}")
        return 0
    if args.command == "run":
        if args.prepare:
            prepare_profile(
                scenario=args.scenario,
                lab_root=lab_root,
                radio_tools=radio_tools,
                reset=bool(args.reset),
            )
        return run_fio(_profile_config_dir(lab_root, args.scenario))
    if args.command == "lab":
        return lab_command(radio_tools, args.action, args.mode)
    if args.command == "check":
        if args.prepare:
            prepare_profile(
                scenario=args.scenario,
                lab_root=lab_root,
                radio_tools=radio_tools,
                reset=bool(args.reset),
            )
        return check_profile(_profile_config_dir(lab_root, args.scenario))
    if args.command == "paths":
        print_paths(lab_root)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
