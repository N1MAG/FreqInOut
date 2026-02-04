from __future__ import annotations

import argparse
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
CONFIG_DIR = ROOT / "config"


def run_tool(script_name: str, args: list[str]) -> int:
    cmd = [sys.executable, str(TOOLS_DIR / script_name), *args]
    return subprocess.run(cmd, cwd=ROOT, check=False).returncode


def backup_databases() -> int:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = CONFIG_DIR / "backups" / f"manual-{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    dbs = [CONFIG_DIR / "freqinout.db", CONFIG_DIR / "freqinout_nets.db"]
    for db in dbs:
        if db.exists():
            dst = backup_dir / db.name
            shutil.copy2(db, dst)
            print(f"Backed up {db} -> {dst}")
        else:
            print(f"Skipped missing DB: {db}")
    return 0


def vacuum_databases() -> int:
    for name in ("freqinout.db", "freqinout_nets.db"):
        path = CONFIG_DIR / name
        if not path.exists():
            print(f"[vacuum] skipped missing DB: {path}")
            continue
        conn = sqlite3.connect(path)
        try:
            conn.execute("VACUUM")
            conn.execute("ANALYZE")
            print(f"[vacuum] optimized {path}")
        finally:
            conn.close()
    return 0


def cmd_status(_: argparse.Namespace) -> int:
    return run_tool("db_admin.py", ["--show"])


def cmd_init(args: argparse.Namespace) -> int:
    return run_tool("db_admin.py", ["--init", args.target, "--show"])


def cmd_truncate(args: argparse.Namespace) -> int:
    if not args.yes:
        print(f"About to truncate: {' '.join(args.targets)}")
        confirm = input("Type YES to continue: ").strip()
        if confirm != "YES":
            print("Canceled.")
            return 0
    return run_tool("db_admin.py", ["--truncate", *args.targets, "--yes", "--show"])


def cmd_table_show(args: argparse.Namespace) -> int:
    return run_tool("db_tools.py", ["--table", args.table, "--table-show"])


def cmd_table_export(args: argparse.Namespace) -> int:
    return run_tool("db_tools.py", ["--table", args.table, "--table-export", args.output])


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Cross-platform FreqInOut DB wrapper.")
    sub = p.add_subparsers(dest="command")

    s = sub.add_parser("status", help="Show row counts for known tables.")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("init", help="Ensure DB schema for target group.")
    s.add_argument("target", nargs="?", default="all", choices=["settings", "nets", "all"])
    s.set_defaults(func=cmd_init)

    s = sub.add_parser("truncate", help="Truncate one or more tables/groups (with backup in db_admin).")
    s.add_argument("targets", nargs="+")
    s.add_argument("--yes", action="store_true", help="Skip interactive confirmation.")
    s.set_defaults(func=cmd_truncate)

    s = sub.add_parser("table-show", help="Show rows from a single table.")
    s.add_argument("table")
    s.set_defaults(func=cmd_table_show)

    s = sub.add_parser("table-export", help="Export table rows to JSON.")
    s.add_argument("table")
    s.add_argument("output")
    s.set_defaults(func=cmd_table_export)

    s = sub.add_parser("backup", help="Backup both database files.")
    s.set_defaults(func=lambda _: backup_databases())

    s = sub.add_parser("vacuum", help="Run VACUUM and ANALYZE on both databases.")
    s.set_defaults(func=lambda _: vacuum_databases())

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
