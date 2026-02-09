# Propagation Climatology (mid-fidelity)

This folder supports the optional mid-fidelity propagation dataset used by the Map → Propagation Overlay.

The provided `prop_climatology.db` can be generated using the PyIRI (International Reference Ionosphere) model.
PyIRI is an optional developer dependency and is not required at runtime.

Files:
- prop_profiles.json
  Baseline heuristic profiles used when no climatology DB is present.
  Per-band fields:
    - ideal_km: best distance for the band
    - spread_km: distance tolerance
    - day: daytime weight
    - night: nighttime weight

- prop_climatology.db (optional)
  If present, this overrides the heuristic profile with a monthly grid.
  Table: muf_grid
    month INTEGER (1-12)
    band TEXT (e.g., 40M)
    lat_idx INTEGER (0..35)  # 5-degree steps from -90 to +85
    lon_idx INTEGER (0..71)  # 5-degree steps from -180 to +175
    muf_score REAL (0..1 or 0..100)

Build script:
- tools/build_prop_db.py

Example usage:
  python tools/build_prop_db.py --input path\to\muf_grid.csv --output config\propagation\prop_climatology.db
