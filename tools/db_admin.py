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
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
SETTINGS_DB = CONFIG_DIR / "freqinout.db"
NETS_DB = CONFIG_DIR / "freqinout_nets.db"


@dataclass(frozen=True)
class TableDef:
    name: str
    ddl: str
    db: Path
    description: str


SETTINGS_TABLES: Dict[str, TableDef] = {
    "kv": TableDef(
        name="kv",
        db=SETTINGS_DB,
        description="Key/value settings store (JSON encoded values).",
        ddl="""
        CREATE TABLE IF NOT EXISTS kv (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """,
    ),
    "daily_schedule_tab": TableDef(
        name="daily_schedule_tab",
        db=SETTINGS_DB,
        description="HF schedule rows mirrored from config (day, band/mode/VFO, frequency, times, auto_tune).",
        ddl="""
        CREATE TABLE IF NOT EXISTS daily_schedule_tab (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_utc TEXT NOT NULL,
            band TEXT NOT NULL,
            mode TEXT NOT NULL,
            vfo TEXT,
            frequency TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            group_name TEXT,
            auto_tune INTEGER DEFAULT 0
        )
        """,
    ),
}

NETS_TABLES: Dict[str, TableDef] = {
    "operator_checkins": TableDef(
        name="operator_checkins",
        db=NETS_DB,
        description="Operator roster with check-in counts and group metadata.",
        ddl="""
        CREATE TABLE IF NOT EXISTS operator_checkins (
            callsign TEXT PRIMARY KEY,
            name TEXT,
            state TEXT,
            grid TEXT,
            group1 TEXT,
            group2 TEXT,
            group3 TEXT,
            group_role TEXT,
            date_added TEXT,
            checkin_count INTEGER DEFAULT 0
        )
        """,
    ),
    "net_schedule_tab": TableDef(
        name="net_schedule_tab",
        db=NETS_DB,
        description="Primary net schedule table (day, recurrence, band/mode/VFO, frequency, times, metadata).",
        ddl="""
        CREATE TABLE IF NOT EXISTS net_schedule_tab (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_utc TEXT NOT NULL,
            recurrence TEXT DEFAULT 'Weekly',
            biweekly_offset_weeks INTEGER DEFAULT 0,
            band TEXT NOT NULL,
            mode TEXT NOT NULL,
            vfo TEXT,
            frequency TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            early_checkin INTEGER NOT NULL,
            primary_js8call_group TEXT,
            comment TEXT,
            net_name TEXT
        )
        """,
    ),
    "net_schedule": TableDef(
        name="net_schedule",
        db=NETS_DB,
        description="Legacy net schedule mirror (no VFO column; kept for backward compatibility).",
        ddl="""
        CREATE TABLE IF NOT EXISTS net_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_utc TEXT NOT NULL,
            recurrence TEXT DEFAULT 'Weekly',
            biweekly_offset_weeks INTEGER DEFAULT 0,
            band TEXT NOT NULL,
            mode TEXT NOT NULL,
            frequency TEXT NOT NULL,
            start_utc TEXT NOT NULL,
            end_utc TEXT NOT NULL,
            early_checkin INTEGER NOT NULL,
            primary_js8call_group TEXT,
            comment TEXT,
            net_name TEXT
        )
        """,
    ),
    "message_viewer_paths": TableDef(
        name="message_viewer_paths",
        db=NETS_DB,
        description="Directories watched by the Message Viewer tab (origin + path).",
        ddl="""
        CREATE TABLE IF NOT EXISTS message_viewer_paths (
            origin TEXT,
            path TEXT UNIQUE
        )
        """,
    ),
    "js8_links": TableDef(
        name="js8_links",
        db=NETS_DB,
        description="JS8 link/spot records (times, peers, SNR, band, frequency, relay info).",
        ddl="""
        CREATE TABLE IF NOT EXISTS js8_links (
            ts REAL,
            origin TEXT,
            destination TEXT,
            snr REAL,
            band TEXT,
            freq_hz REAL,
            is_relay INTEGER DEFAULT 0,
            relay_via TEXT,
            is_spotter INTEGER DEFAULT 0
        )
        """,
    ),
}

ALL_TABLES: Dict[str, TableDef] = {**SETTINGS_TABLES, **NETS_TABLES}

GROUPS: Dict[str, List[str]] = {
    "settings": list(SETTINGS_TABLES.keys()),
    "nets": list(NETS_TABLES.keys()),
    "all": list(ALL_TABLES.keys()),
    "settings_all": list(SETTINGS_TABLES.keys()),
    "nets_all": list(NETS_TABLES.keys()),
}


def ensure_tables(tables: Iterable[str]) -> None:
    for name in tables:
        tbl = ALL_TABLES[name]
        tbl.db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(tbl.db)
        try:
            conn.execute(tbl.ddl)
            conn.commit()
            print(f"[init] ensured {name} in {tbl.db}")
        finally:
            conn.close()


def truncate_tables(tables: Iterable[str]) -> None:
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
