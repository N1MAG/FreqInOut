from __future__ import annotations

"""
Quick inspection tool for JS8 link ingestion.

Usage (run from repo root):
  python tools/js8_links_check.py
  python tools/js8_links_check.py --db path/to/freqinout_nets.db --limit 10
"""

import argparse
import sqlite3
from pathlib import Path
from typing import List, Tuple


def resolve_db(default: bool = True, override: str | None = None) -> Path:
    if override:
        return Path(override).expanduser()
    if default:
        return Path(__file__).resolve().parents[1] / "config" / "freqinout_nets.db"
    return Path.cwd() / "freqinout_nets.db"


def fetch_rows(db_path: Path, limit: int) -> Tuple[int, List[Tuple]]:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM js8_links")
    count, ts_min, ts_max = cur.fetchone()
    cur.execute(
        """
        SELECT origin, destination, ts, snr, band, is_relay, relay_via, freq_hz
        FROM js8_links
        ORDER BY ts DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    con.close()
    return (count, ts_min, ts_max), rows


def main():
    parser = argparse.ArgumentParser(description="Inspect js8_links ingestion status.")
    parser.add_argument("--db", help="Path to freqinout_nets.db (default: config/freqinout_nets.db)")
    parser.add_argument("--limit", type=int, default=5, help="Number of sample rows to display")
    args = parser.parse_args()

    db_path = resolve_db(override=args.db)
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        raise SystemExit(1)

    try:
        summary, rows = fetch_rows(db_path, max(1, args.limit))
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise SystemExit(1)

    count, ts_min, ts_max = summary
    print(f"DB: {db_path}")
    print(f"js8_links count={count}, ts_min={ts_min}, ts_max={ts_max}")
    print("Sample rows (origin, destination, ts, snr, band, is_relay, relay_via, freq_hz):")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
