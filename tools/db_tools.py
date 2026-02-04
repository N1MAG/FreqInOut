from __future__ import annotations

"""
Utility CLI for inspecting and manipulating the FreqInOut SQLite settings DB.
Also supports curated table admin for selected tables across config DBs.

Usage examples (run from repo root):
  python tools/db_tools.py --show
  python tools/db_tools.py --get timezone
  python tools/db_tools.py --set js8_port 2442
  python tools/db_tools.py --export backup.json
  python tools/db_tools.py --truncate --yes
  python tools/db_tools.py --table autoquery_backlog --table-show
  python tools/db_tools.py --table autoquery_backlog --table-truncate --yes

NOTE: Values are JSON-encoded. For complex values, pass valid JSON to --set.
"""

import argparse
from datetime import datetime
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from db_schema import ALL_TABLES, CONFIG_DIR, NETS_DB, SETTINGS_DB, db_for_table

def db_path() -> Path:
    return SETTINGS_DB

def nets_db_path() -> Path:
    return NETS_DB

ALLOWED_TABLES = {name: table.db.name for name, table in ALL_TABLES.items()}


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS kv (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()


def load_all(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute("SELECT key, value FROM kv")
    out: Dict[str, Any] = {}
    for k, v in cur.fetchall():
        try:
            out[k] = json.loads(v)
        except Exception:
            out[k] = v
    return out


def help_examples() -> str:
    return "\n".join(
        [
            "Examples:",
            "  python tools/db_tools.py --show",
            "  python tools/db_tools.py --get timezone",
            "  python tools/db_tools.py --set js8_port 2442",
            "  python tools/db_tools.py --export backup.json",
            "  python tools/db_tools.py --truncate --yes",
            "  python tools/db_tools.py --list-dbs",
            "  python tools/db_tools.py --table autoquery_backlog --table-show",
            "  python tools/db_tools.py --table autoquery_backlog --table-truncate --yes",
        ]
    )


def resolve_table_db(table: str) -> Optional[Path]:
    return db_for_table(table)

def backup_db_file(path: Path, reason: str) -> Optional[Path]:
    if not path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_dir = CONFIG_DIR / "backups" / f"{reason}-{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    dst = backup_dir / path.name
    shutil.copy2(path, dst)
    return dst

def list_dbs_and_tables() -> None:
    dbs = [db_path(), nets_db_path()]
    for db in dbs:
        if not db.exists():
            print(f"{db.name}: <missing>")
            continue
        conn = sqlite3.connect(db)
        try:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cur.fetchall()]
        finally:
            conn.close()
        print(f"{db.name}:")
        if not tables:
            print("  <no tables>")
            continue
        for table in tables:
            print(f"  {table}")

def main() -> None:
    parser = argparse.ArgumentParser(description="FreqInOut SQLite settings helper")
    parser.add_argument("--show", action="store_true", help="List all keys/values")
    parser.add_argument("--get", metavar="KEY", help="Get a single key")
    parser.add_argument("--set", nargs=2, metavar=("KEY", "JSON_VALUE"), help="Set key to JSON value")
    parser.add_argument("--export", metavar="PATH", help="Export all settings to JSON file")
    parser.add_argument("--truncate", action="store_true", help="Delete all settings")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive actions (truncate)")
    parser.add_argument("--list-dbs", action="store_true", help="List databases and their tables")
    parser.add_argument("--table", metavar="NAME", help="Operate on a specific allowed table")
    parser.add_argument("--table-show", action="store_true", help="Show all rows in a table")
    parser.add_argument("--table-export", metavar="PATH", help="Export table rows to JSON file")
    parser.add_argument("--table-truncate", action="store_true", help="Delete all rows in a table")
    parser.add_argument("--help-examples", action="store_true", help="Show usage examples and exit")
    args = parser.parse_args()

    if args.help_examples:
        print(help_examples())
        return

    if args.list_dbs:
        list_dbs_and_tables()
        return

    path = db_path()
    conn = sqlite3.connect(path)
    ensure_db(conn)

    if args.truncate:
        if not args.yes:
            parser.error("--truncate requires --yes")
        backup = backup_db_file(path, "truncate-kv")
        if backup:
            print(f"Backed up DB to {backup}")
        with conn:
            conn.execute("DELETE FROM kv")
        print("Truncated kv table.")
        return

    if args.set:
        key, raw = args.set
        try:
            val = json.loads(raw)
        except Exception:
            val = raw
        payload = json.dumps(val)
        with conn:
            conn.execute("INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)", (key, payload))
        print(f"Set {key} = {val}")

    if args.get:
        cur = conn.execute("SELECT value FROM kv WHERE key=?", (args.get,))
        row = cur.fetchone()
        if not row:
            print(f"{args.get}: <not set>")
        else:
            try:
                val = json.loads(row[0])
            except Exception:
                val = row[0]
            print(f"{args.get}: {val}")

    if args.export:
        data = load_all(conn)
        out_path = Path(args.export)
        out_path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Exported {len(data)} keys to {out_path}")

    if args.show and not args.export:
        data = load_all(conn)
        print(f"{len(data)} keys")
        for k, v in sorted(data.items()):
            print(f"{k}: {v}")

    if args.table:
        table_db = resolve_table_db(args.table)
        if not table_db:
            parser.error(f"--table must be one of: {', '.join(sorted(ALLOWED_TABLES))}")
        table_conn = sqlite3.connect(table_db)
        try:
            cur = table_conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (args.table,),
            )
            if not cur.fetchone():
                parser.error(f"Table not found in {table_db.name}: {args.table}")

            if args.table_truncate:
                if not args.yes:
                    parser.error("--table-truncate requires --yes")
                backup = backup_db_file(table_db, f"truncate-{args.table}")
                if backup:
                    print(f"Backed up DB to {backup}")
                with table_conn:
                    table_conn.execute(f"DELETE FROM {args.table}")
                print(f"Truncated {args.table} in {table_db.name}.")

            if args.table_show:
                cur = table_conn.execute(f"SELECT * FROM {args.table}")
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description or []]
                print(f"{len(rows)} rows in {args.table}")
                if cols:
                    print("Columns: " + ", ".join(cols))
                for row in rows:
                    print(row)

            if args.table_export:
                cur = table_conn.execute(f"SELECT * FROM {args.table}")
                cols = [c[0] for c in cur.description or []]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                out_path = Path(args.table_export)
                out_path.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
                print(f"Exported {len(rows)} rows from {args.table} to {out_path}")
        finally:
            table_conn.close()


if __name__ == "__main__":
    main()
