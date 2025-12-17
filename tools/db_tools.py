from __future__ import annotations

"""
Utility CLI for inspecting and manipulating the FreqInOut SQLite settings DB.

Usage examples (run from repo root):
  python tools/db_tools.py --show
  python tools/db_tools.py --get timezone
  python tools/db_tools.py --set js8_port 2442
  python tools/db_tools.py --export backup.json
  python tools/db_tools.py --truncate --yes

NOTE: Values are JSON-encoded. For complex values, pass valid JSON to --set.
"""

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


def db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "freqinout.db"


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


def main() -> None:
    parser = argparse.ArgumentParser(description="FreqInOut SQLite settings helper")
    parser.add_argument("--show", action="store_true", help="List all keys/values")
    parser.add_argument("--get", metavar="KEY", help="Get a single key")
    parser.add_argument("--set", nargs=2, metavar=("KEY", "JSON_VALUE"), help="Set key to JSON value")
    parser.add_argument("--export", metavar="PATH", help="Export all settings to JSON file")
    parser.add_argument("--truncate", action="store_true", help="Delete all settings")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive actions (truncate)")
    args = parser.parse_args()

    path = db_path()
    conn = sqlite3.connect(path)
    ensure_db(conn)

    if args.truncate:
        if not args.yes:
            parser.error("--truncate requires --yes")
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


if __name__ == "__main__":
    main()
