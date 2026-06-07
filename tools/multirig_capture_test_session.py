#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_ROOT = Path.home() / "FIO_MultiRig_Test_Capture"
DEFAULT_JS8_OUT_NAME = "js8_capability_capture.json"


def _utc_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%SZ")


def _safe_name(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return "session"
    out = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        elif ch.isspace():
            out.append("-")
    cleaned = "".join(out).strip("-._")
    return cleaned or "session"


def _run_cmd(args: Sequence[str], *, cwd: Optional[Path] = None, timeout_s: float = 10.0) -> Dict[str, object]:
    started = _dt.datetime.now(_dt.timezone.utc).isoformat()
    try:
        proc = subprocess.run(
            [str(item) for item in args],
            cwd=str(cwd) if cwd else None,
            text=True,
            capture_output=True,
            timeout=max(0.5, float(timeout_s)),
            check=False,
        )
        return {
            "cmd": [str(item) for item in args],
            "started_utc": started,
            "returncode": int(proc.returncode),
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    except Exception as exc:
        return {
            "cmd": [str(item) for item in args],
            "started_utc": started,
            "returncode": None,
            "stdout": "",
            "stderr": f"{exc.__class__.__name__}: {exc}",
        }


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_labeled_values(raw_items: Iterable[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in raw_items or []:
        text = str(raw or "").strip()
        if not text:
            continue
        if "=" not in text:
            raise ValueError(f"Expected LABEL=VALUE, got {text!r}")
        label, value = text.split("=", 1)
        label = label.strip()
        if not label:
            raise ValueError(f"Expected LABEL=VALUE, got {text!r}")
        out[label] = value.strip()
    return out


def _copy_config_tree(source: Path, dest: Path) -> Dict[str, object]:
    if not source.exists():
        return {"copied": False, "source": str(source), "reason": "source_missing"}
    copied = 0
    skipped = 0
    for src in source.rglob("*"):
        rel = src.relative_to(source)
        dst = dest / rel
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            continue
        if src.name.endswith(("-wal", "-shm")):
            skipped += 1
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied += 1
    return {"copied": True, "source": str(source), "dest": str(dest), "files": copied, "skipped": skipped}


def _notes_template(*, session_name: str) -> str:
    return f"""# FIO Multi-Rig Test Session Notes

Session: {session_name}

## Machine

Machine name:
OS:
Python:
FIO commit/version:
Fresh install or upgraded single-rig config:

## Radio / App Setup

Real apps or simulators:
Radios configured:
JS8Call versions and ports:
FLRig/FLDigi endpoints:
VarAC / VARA state:

## Test Path

What I tested:
What worked:
What confused me:
What failed:
Any Station Health warnings:
Any screenshots included:

## Timeline

- UTC time:
  Action:
  Result:

"""


def _build_js8_capture_command(
    *,
    repo: Path,
    endpoints: Sequence[str],
    output_path: Path,
    capture_label: str,
    operator_notes: Sequence[str],
    endpoint_builds: Mapping[str, str],
    endpoint_platforms: Mapping[str, str],
    endpoint_tcp_api: Mapping[str, str],
    endpoint_udp_ports: Mapping[str, str],
    endpoint_notes: Mapping[str, str],
    timeout_s: float,
) -> List[str]:
    cmd = [
        sys.executable,
        str(repo / "tools" / "js8_api_capture_matrix.py"),
        "--pretty",
        "--timeout",
        str(timeout_s),
        "--capture-label",
        capture_label,
        "--out",
        str(output_path),
    ]
    for note in operator_notes:
        cmd.extend(["--operator-note", str(note)])
    for endpoint in endpoints:
        cmd.extend(["--endpoint", str(endpoint)])
    for label, value in endpoint_builds.items():
        cmd.extend(["--endpoint-build", f"{label}={value}"])
    for label, value in endpoint_platforms.items():
        cmd.extend(["--endpoint-platform", f"{label}={value}"])
    for label, value in endpoint_tcp_api.items():
        cmd.extend(["--endpoint-tcp-api", f"{label}={value}"])
    for label, value in endpoint_udp_ports.items():
        cmd.extend(["--endpoint-udp-port", f"{label}={value}"])
    for label, value in endpoint_notes.items():
        cmd.extend(["--endpoint-note", f"{label}={value}"])
    return cmd


def create_capture_session(args: argparse.Namespace) -> Path:
    repo = Path(args.repo).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    machine = _safe_name(args.machine_name or socket.gethostname() or platform.node())
    stamp = _utc_stamp()
    label = _safe_name(args.session_label or "multi-rig-test")
    session_dir = out_root / f"{machine}-{stamp}-{label}"
    session_dir.mkdir(parents=True, exist_ok=False)
    (session_dir / "screenshots").mkdir(parents=True, exist_ok=True)
    (session_dir / "logs").mkdir(parents=True, exist_ok=True)

    fio_version = ""
    try:
        if str(repo) not in sys.path:
            sys.path.insert(0, str(repo))
        from freqinout.version import __version__

        fio_version = str(__version__)
    except Exception:
        fio_version = ""

    system_info = {
        "captured_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "machine_name": machine,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "python_executable": sys.executable,
        "fio_version": fio_version,
        "repo": str(repo),
        "env": {
            "FREQINOUT_CONFIG_DIR": os.environ.get("FREQINOUT_CONFIG_DIR", ""),
            "QT_QPA_PLATFORM": os.environ.get("QT_QPA_PLATFORM", ""),
        },
    }
    _write_json(session_dir / "system_info.json", system_info)

    git_commands = {
        "git_commit": ["git", "rev-parse", "--short", "HEAD"],
        "git_status": ["git", "status", "--short", "--branch"],
        "git_remote": ["git", "remote", "-v"],
        "git_branch": ["git", "branch", "--show-current"],
    }
    git_results: Dict[str, object] = {}
    for name, cmd in git_commands.items():
        result = _run_cmd(cmd, cwd=repo)
        git_results[name] = result
        _write_text(session_dir / f"{name}.txt", str(result.get("stdout") or "") + str(result.get("stderr") or ""))
    _write_json(session_dir / "git_info.json", git_results)

    if args.config_dir:
        copied = _copy_config_tree(
            Path(args.config_dir).expanduser().resolve(),
            session_dir / "config_copy",
        )
        _write_json(session_dir / "config_copy_manifest.json", copied)

    notes_name = _safe_name(args.session_label or f"{machine} {stamp}")
    _write_text(session_dir / "operator_notes.md", _notes_template(session_name=notes_name))

    js8_result: Optional[Dict[str, object]] = None
    if args.js8_endpoint:
        js8_out = session_dir / DEFAULT_JS8_OUT_NAME
        cmd = _build_js8_capture_command(
            repo=repo,
            endpoints=args.js8_endpoint,
            output_path=js8_out,
            capture_label=str(args.session_label or notes_name),
            operator_notes=args.operator_note or [],
            endpoint_builds=_parse_labeled_values(args.js8_build),
            endpoint_platforms=_parse_labeled_values(args.js8_platform),
            endpoint_tcp_api=_parse_labeled_values(args.js8_tcp_api),
            endpoint_udp_ports=_parse_labeled_values(args.js8_udp_port),
            endpoint_notes=_parse_labeled_values(args.js8_note),
            timeout_s=float(args.js8_timeout),
        )
        js8_result = _run_cmd(cmd, cwd=repo, timeout_s=max(10.0, len(args.js8_endpoint) * 8.0))
        _write_json(session_dir / "js8_capture_command.json", js8_result)

    manifest = {
        "session_dir": str(session_dir),
        "system_info": "system_info.json",
        "git_info": "git_info.json",
        "operator_notes": "operator_notes.md",
        "screenshots_dir": "screenshots",
        "logs_dir": "logs",
        "config_copy": "config_copy" if args.config_dir else "",
        "js8_capture": DEFAULT_JS8_OUT_NAME if args.js8_endpoint else "",
        "js8_capture_command": "js8_capture_command.json" if js8_result is not None else "",
    }
    _write_json(session_dir / "manifest.json", manifest)
    return session_dir


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create a portable evidence bundle for FIO multi-rig field testing."
    )
    parser.add_argument("--repo", default=str(REPO_ROOT), help="FreqInOut repo path")
    parser.add_argument("--out-root", default=str(DEFAULT_OUT_ROOT), help="Root folder for capture sessions")
    parser.add_argument("--session-label", default="multi-rig-test", help="Short label for this test session")
    parser.add_argument("--machine-name", default="", help="Optional machine name override")
    parser.add_argument("--config-dir", default="", help="Optional FIO config directory to copy into the bundle")
    parser.add_argument(
        "--js8-endpoint",
        action="append",
        default=[],
        metavar="LABEL=HOST:PORT",
        help="Optional JS8Call TCP endpoint to capture. May be repeated.",
    )
    parser.add_argument("--js8-timeout", type=float, default=0.4, help="JS8 per-command timeout")
    parser.add_argument("--operator-note", action="append", default=[], help="Top-level note for JS8 capture")
    parser.add_argument("--js8-build", action="append", default=[], metavar="LABEL=TEXT")
    parser.add_argument("--js8-platform", action="append", default=[], metavar="LABEL=TEXT")
    parser.add_argument("--js8-tcp-api", action="append", default=[], metavar="LABEL=enabled|disabled|unknown")
    parser.add_argument("--js8-udp-port", action="append", default=[], metavar="LABEL=PORT")
    parser.add_argument("--js8-note", action="append", default=[], metavar="LABEL=TEXT")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    session_dir = create_capture_session(args)
    print(str(session_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

