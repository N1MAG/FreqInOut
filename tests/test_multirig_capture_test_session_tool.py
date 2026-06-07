from __future__ import annotations

import argparse
import json
from pathlib import Path

from tools import multirig_capture_test_session as capture_tool


def _args(tmp_path: Path, **overrides):
    values = {
        "repo": str(Path(__file__).resolve().parents[1]),
        "out_root": str(tmp_path / "captures"),
        "session_label": "upgrade walk-through",
        "machine_name": "field box",
        "config_dir": "",
        "js8_endpoint": [],
        "js8_timeout": 0.4,
        "operator_note": [],
        "js8_build": [],
        "js8_platform": [],
        "js8_tcp_api": [],
        "js8_udp_port": [],
        "js8_note": [],
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_create_capture_session_writes_portable_bundle(monkeypatch, tmp_path):
    commands: list[list[str]] = []

    def fake_run_cmd(cmd, *, cwd=None, timeout_s=10.0):
        commands.append([str(item) for item in cmd])
        return {"cmd": [str(item) for item in cmd], "returncode": 0, "stdout": "ok\n", "stderr": ""}

    monkeypatch.setattr(capture_tool, "_utc_stamp", lambda: "20260607-120000Z")
    monkeypatch.setattr(capture_tool, "_run_cmd", fake_run_cmd)

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.json").write_text('{"example": true}\n', encoding="utf-8")
    (config_dir / "freqinout.db-wal").write_text("skip", encoding="utf-8")

    session = capture_tool.create_capture_session(
        _args(
            tmp_path,
            config_dir=str(config_dir),
        )
    )

    manifest = json.loads((session / "manifest.json").read_text(encoding="utf-8"))
    system_info = json.loads((session / "system_info.json").read_text(encoding="utf-8"))
    config_manifest = json.loads((session / "config_copy_manifest.json").read_text(encoding="utf-8"))

    assert session.name == "field-box-20260607-120000Z-upgrade-walk-through"
    assert manifest["operator_notes"] == "operator_notes.md"
    assert manifest["screenshots_dir"] == "screenshots"
    assert manifest["config_copy"] == "config_copy"
    assert (session / "operator_notes.md").exists()
    assert (session / "screenshots").is_dir()
    assert system_info["machine_name"] == "field-box"
    assert config_manifest["copied"] is True
    assert (session / "config_copy" / "settings.json").exists()
    assert not (session / "config_copy" / "freqinout.db-wal").exists()
    assert any(cmd[:2] == ["git", "rev-parse"] for cmd in commands)


def test_create_capture_session_runs_js8_capture_with_metadata(monkeypatch, tmp_path):
    commands: list[list[str]] = []

    def fake_run_cmd(cmd, *, cwd=None, timeout_s=10.0):
        commands.append([str(item) for item in cmd])
        return {"cmd": [str(item) for item in cmd], "returncode": 0, "stdout": "ok\n", "stderr": ""}

    monkeypatch.setattr(capture_tool, "_utc_stamp", lambda: "20260607-121500Z")
    monkeypatch.setattr(capture_tool, "_run_cmd", fake_run_cmd)

    session = capture_tool.create_capture_session(
        _args(
            tmp_path,
            js8_endpoint=["radio-a=127.0.0.1:2442"],
            operator_note=["radio attached to dummy load"],
            js8_build=["radio-a=JS8Call 3.0.2"],
            js8_platform=["radio-a=Linux Mint"],
            js8_tcp_api=["radio-a=enabled"],
            js8_udp_port=["radio-a=2242"],
            js8_note=["radio-a=first rig instance"],
        )
    )

    manifest = json.loads((session / "manifest.json").read_text(encoding="utf-8"))
    js8_command = json.loads((session / "js8_capture_command.json").read_text(encoding="utf-8"))
    cmd = js8_command["cmd"]

    assert manifest["js8_capture"] == "js8_capability_capture.json"
    assert "--endpoint" in cmd
    assert "radio-a=127.0.0.1:2442" in cmd
    assert "--endpoint-build" in cmd
    assert "radio-a=JS8Call 3.0.2" in cmd
    assert "--endpoint-platform" in cmd
    assert "radio-a=Linux Mint" in cmd
    assert "--endpoint-tcp-api" in cmd
    assert "radio-a=enabled" in cmd
    assert "--endpoint-udp-port" in cmd
    assert "radio-a=2242" in cmd
    assert "--endpoint-note" in cmd
    assert "radio-a=first rig instance" in cmd
    assert commands[-1] == cmd

