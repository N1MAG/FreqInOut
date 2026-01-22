from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import List, Tuple


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "freqinout_nets.db"


def _normalize_group(val: str) -> str:
    return (val or "").strip().upper()


def _dedupe_groups(g1: str, g2: str, g3: str, groups_json: str | None) -> Tuple[List[str], List[str]]:
    ordered: List[str] = []
    for raw in (g1, g2, g3):
        g = _normalize_group(raw)
        if g and g not in ordered:
            ordered.append(g)
    extra: List[str] = []
    if groups_json:
        try:
            parsed = json.loads(groups_json)
            if isinstance(parsed, list):
                for raw in parsed:
                    g = _normalize_group(str(raw))
                    if g and g not in ordered and g not in extra:
                        extra.append(g)
        except Exception:
            pass
    combined = ordered + extra
    new_slots = combined[:3]
    new_extra = combined[3:]
    while len(new_slots) < 3:
        new_slots.append("")
    return new_slots, new_extra


def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate operator group assignments.")
    parser.add_argument("--db", type=Path, default=_default_db_path(), help="Path to freqinout_nets.db")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry run)")
    parser.add_argument("--show", action="store_true", help="Show per-callsign changes")
    args = parser.parse_args()

    db_path = args.db
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT callsign, group1, group2, group3, groups_json FROM operator_checkins"
        )
    except Exception as e:
        print(f"Failed to read operator_checkins: {e}")
        conn.close()
        return 1

    rows = cur.fetchall()
    changes = 0
    for cs, g1, g2, g3, gj in rows:
        new_slots, new_extra = _dedupe_groups(g1 or "", g2 or "", g3 or "", gj)
        new_gj = json.dumps(new_extra) if new_extra else None
        if [g1 or "", g2 or "", g3 or ""] != new_slots or (gj or None) != new_gj:
            changes += 1
            if args.show:
                print(f"{cs}: [{g1},{g2},{g3}] -> {new_slots}; groups_json -> {new_gj}")
            if args.apply:
                cur.execute(
                    """
                    UPDATE operator_checkins
                    SET group1=?, group2=?, group3=?, groups_json=?
                    WHERE callsign=?
                    """,
                    (new_slots[0], new_slots[1], new_slots[2], new_gj, cs),
                )

    if args.apply:
        conn.commit()
    conn.close()

    mode = "applied" if args.apply else "dry run"
    print(f"{mode}: {changes} callsign(s) would be updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
