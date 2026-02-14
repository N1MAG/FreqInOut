from __future__ import annotations

"""
Unified CLI to manage FreqInOut SQLite databases.

Features
--------
- Ensure required tables exist in both databases.
- Truncate (DELETE all rows) from any supported table or group of tables.
- Optional table row counts for quick inspection.

Usage examples (run from repo root):
  python tools/db_admin.py --init all
  python tools/db_admin.py --init settings
  python tools/db_admin.py --truncate kv daily_schedule_tab --yes
  python tools/db_admin.py --truncate nets_all --yes
  python tools/db_admin.py --truncate all --yes --show
"""

import argparse
from datetime import datetime
import shutil
import sqlite3
from typing import Dict, Iterable, List, Tuple

from db_schema import ALL_TABLES, CONFIG_DIR, GROUPS


def ensure_tables(tables: Iterable[str]) -> None:
    for name in tables:
        tbl = ALL_TABLES[name]
        tbl.db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tbl.db)
        try:
            if tbl.ddl.strip():
                conn.executescript(tbl.ddl)
            conn.commit()
            print(f"[init] ensured {name} in {tbl.db}")
        finally:
            conn.close()


def backup_databases_for_tables(tables: Iterable[str]) -> None:
    db_paths = sorted({ALL_TABLES[name].db for name in tables if ALL_TABLES[name].db.exists()})
    if not db_paths:
        print("[backup] no existing DB files to back up.")
        return

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = CONFIG_DIR / "backups" / f"truncate-{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    for src in db_paths:
        dst = backup_dir / src.name
        shutil.copy2(src, dst)
        print(f"[backup] saved {src} -> {dst}")


def truncate_tables(tables: Iterable[str]) -> None:
    backup_databases_for_tables(tables)
    for name in tables:
        tbl = ALL_TABLES[name]
        if not tbl.db.exists():
            print(f"[truncate] skipped {name}: DB missing at {tbl.db}")
            continue
        conn = sqlite3.connect(tbl.db)
        try:
            conn.execute(f"DELETE FROM {tbl.name}")
            conn.commit()
            print(f"[truncate] cleared {name} in {tbl.db}")
        finally:
            conn.close()


def summarize(tables: Iterable[str]) -> None:
    for name in tables:
        tbl = ALL_TABLES[name]
        if not tbl.db.exists():
            print(f"[show] {name}: DB missing at {tbl.db}")
            continue
        conn = sqlite3.connect(tbl.db)
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (tbl.name,),
            )
            exists = cur.fetchone() is not None
            if not exists:
                print(f"[show] {name}: table missing in {tbl.db}")
                continue
            cur = conn.execute(f"SELECT COUNT(*) FROM {tbl.name}")
            count = cur.fetchone()[0]
            print(f"[show] {name}: {count} row(s) in {tbl.db}")
        except sqlite3.Error as e:
            print(f"[show] {name}: error reading {tbl.db}: {e}")
        finally:
            conn.close()


def expand_targets(targets: List[str]) -> List[str]:
    expanded: List[str] = []
    for t in targets:
        if t in GROUPS:
            expanded.extend(GROUPS[t])
        else:
            expanded.append(t)
    # Preserve order but drop duplicates
    seen = set()
    unique: List[str] = []
    for name in expanded:
        if name not in seen:
            seen.add(name)
            unique.append(name)
    return unique


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage FreqInOut SQLite databases.")
    parser.add_argument(
        "--init",
        choices=["settings", "nets", "all"],
        help="Create/ensure tables for target DB(s).",
    )
    parser.add_argument(
        "--truncate",
        nargs="+",
        choices=list(ALL_TABLES.keys()) + list(GROUPS.keys()),
        help="Delete all rows from specified tables or groups.",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Print row counts for relevant tables (after actions).",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm destructive actions (required for --truncate).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.init and not args.truncate and not args.show:
        print("Nothing to do. Use --init, --truncate, or --show. Run -h for help.")
        return

    if args.init:
        ensure_tables(GROUPS[args.init])

    if args.truncate:
        if not args.yes:
            raise SystemExit("--truncate requires --yes to proceed.")
        targets = expand_targets(args.truncate)
        truncate_tables(targets)

    if args.show:
        targets: List[str]
        if args.truncate:
            targets = expand_targets(args.truncate)
        elif args.init:
            targets = GROUPS[args.init]
        else:
            targets = GROUPS["all"]
        summarize(targets)


if __name__ == "__main__":
    main()
