"""Generate a mid-fidelity climatology database using PyIRI.

Requires:
  pip install PyIRI numpy

Outputs:
  config/propagation/prop_climatology.db
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable

import numpy as np

try:
    import PyIRI
    import PyIRI.sh_library as sh
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "PyIRI is required. Install with: pip install PyIRI\n"
        f"Import error: {exc}"
    )

PROP_BANDS = {
    "80M": 3.75,
    "40M": 7.10,
    "30M": 10.12,
    "20M": 14.10,
    "15M": 21.20,
    "10M": 28.40,
}


def iter_chunks(seq: Iterable, size: int):
    buf = []
    for item in seq:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def fof2_month_grid(year: int, month: int, dlat: int = 5, dlon: int = 5):
    aUT = np.arange(0, 24, 1)
    alon = np.arange(-180, 180, dlon)
    alat = np.arange(-90, 90, dlat)
    alon_2d, alat_2d = np.meshgrid(alon, alat, indexing="ij")
    alon_flat = alon_2d.ravel()
    alat_flat = alat_2d.ravel()

    hmF2_model = "SHU2015"
    foF2_coeff = "URSI"
    coord = "GEO"

    f2, f1, e_peak, sun, mag = sh.IRI_monthly_mean_par(
        year,
        month,
        aUT,
        alon_flat,
        alat_flat,
        coeff_dir=None,
        foF2_coeff=foF2_coeff,
        hmF2_model=hmF2_model,
        coord=coord,
    )

    if isinstance(f2, dict):
        if "fo" in f2:
            f2_arr = np.asarray(f2["fo"])
        elif "foF2" in f2:
            f2_arr = np.asarray(f2["foF2"])
        elif "FO" in f2:
            f2_arr = np.asarray(f2["FO"])
        else:
            raise RuntimeError(f"foF2 key not found in PyIRI output: {list(f2.keys())}")
    else:
        f2_arr = np.asarray(f2)

    if f2_arr.ndim < 2:
        raise RuntimeError(f"Unexpected foF2 shape: {f2_arr.shape}")

    if f2_arr.shape[0] == len(aUT):
        fof2_mean = np.nanmean(f2_arr, axis=0)
    elif f2_arr.shape[-1] == len(aUT):
        fof2_mean = np.nanmean(f2_arr, axis=-1)
    else:
        raise RuntimeError(f"Unexpected foF2 shape: {f2_arr.shape} vs UT={len(aUT)}")

    fof2_mean = np.asarray(fof2_mean)
    if fof2_mean.ndim == 2 and fof2_mean.shape[1] == 2:
        fof2_mean = np.nanmean(fof2_mean, axis=1)
    elif fof2_mean.ndim > 1:
        fof2_mean = np.nanmean(fof2_mean, axis=-1)

    fof2_mean = fof2_mean.reshape((len(alon), len(alat)))

    return alon, alat, fof2_mean


def build_db(output_db: Path, year: int) -> None:
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

    for month in range(1, 13):
        alon, alat, fof2 = fof2_month_grid(year, month)
        lon_grid, lat_grid = np.meshgrid(alon, alat, indexing="ij")
        lat_idx = ((lat_grid + 90.0) // 5).astype(int)
        lon_idx = ((lon_grid + 180.0) // 5).astype(int)
        lat_idx = np.clip(lat_idx, 0, 35)
        lon_idx = np.clip(lon_idx, 0, 71)

        for band, freq_mhz in PROP_BANDS.items():
            score = np.clip(fof2 / freq_mhz, 0.0, 1.0)
            rows = zip(
                [month] * score.size,
                [band] * score.size,
                lat_idx.ravel().tolist(),
                lon_idx.ravel().tolist(),
                score.ravel().tolist(),
            )
            for chunk in iter_chunks(rows, 5000):
                cur.executemany(
                    "INSERT INTO muf_grid (month, band, lat_idx, lon_idx, muf_score) VALUES (?, ?, ?, ?, ?)",
                    chunk,
                )
        conn.commit()

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate propagation climatology DB using PyIRI")
    parser.add_argument("--year", type=int, default=2020, help="Year to use for monthly means")
    parser.add_argument(
        "--output",
        default=str(Path("config") / "propagation" / "prop_climatology.db"),
        help="Output SQLite DB path",
    )
    args = parser.parse_args()
    build_db(Path(args.output), args.year)


if __name__ == "__main__":
    main()
