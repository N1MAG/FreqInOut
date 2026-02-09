"""
Build a mid-fidelity propagation climatology SQLite database.

Input CSV format (no header):
month,band,lat_idx,lon_idx,muf_score

Example:
1,40M,25,40,0.78
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def build_db(input_csv: Path, output_db: Path) -> None:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")
    output_db.parent.mkdir(parents=True, exist_ok=True)
    if output_db.exists():
        output_db.unlink()

    conn = sqlite3.connect(output_db)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE muf_grid (
            month INTEGER NOT NULL,
            band TEXT NOT NULL,
            lat_idx INTEGER NOT NULL,
            lon_idx INTEGER NOT NULL,
            muf_score REAL NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE INDEX idx_muf_grid_lookup ON muf_grid (month, band, lat_idx, lon_idx)"
    )

    with input_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = []
        for row in reader:
            if not row or len(row) < 5:
                continue
            month = int(row[0])
            band = str(row[1]).strip()
            lat_idx = int(row[2])
            lon_idx = int(row[3])
            muf_score = float(row[4])
            rows.append((month, band, lat_idx, lon_idx, muf_score))
        cur.executemany(
            "INSERT INTO muf_grid (month, band, lat_idx, lon_idx, muf_score) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
    conn.commit()
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build propagation climatology DB")
    parser.add_argument("--input", required=True, help="CSV input file")
    parser.add_argument("--output", required=True, help="Output SQLite DB file")
    args = parser.parse_args()
    build_db(Path(args.input), Path(args.output))


if __name__ == "__main__":
    main()
