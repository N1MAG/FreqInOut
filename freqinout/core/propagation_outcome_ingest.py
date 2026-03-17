from __future__ import annotations

import datetime as dt
import hashlib
import math
import re
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


CALLSIGN_RE = re.compile(r"^[A-Z0-9/]{3,10}$")
SOURCE_JS8_LINKS = "JS8_LINKS"
SOURCE_VARAC_LINKS = "VARAC_LINKS"
SOURCE_JS8_MESSAGES = "JS8_MESSAGES"
SOURCE_VARAC_MESSAGES = "VARAC_MESSAGES"
SOURCE_SPOTTER = "SPOTTER_TRAFFIC"

STATE_TO_FEMA_REGION: Dict[str, str] = {
    "CT": "R01",
    "ME": "R01",
    "MA": "R01",
    "NH": "R01",
    "RI": "R01",
    "VT": "R01",
    "NJ": "R02",
    "NY": "R02",
    "PA": "R03",
    "DE": "R03",
    "MD": "R03",
    "VA": "R03",
    "WV": "R03",
    "DC": "R03",
    "AL": "R04",
    "FL": "R04",
    "GA": "R04",
    "KY": "R04",
    "MS": "R04",
    "NC": "R04",
    "SC": "R04",
    "TN": "R04",
    "IL": "R05",
    "IN": "R05",
    "MI": "R05",
    "MN": "R05",
    "OH": "R05",
    "WI": "R05",
    "AR": "R06",
    "LA": "R06",
    "NM": "R06",
    "OK": "R06",
    "TX": "R06",
    "IA": "R07",
    "KS": "R07",
    "MO": "R07",
    "NE": "R07",
    "CO": "R08",
    "MT": "R08",
    "ND": "R08",
    "SD": "R08",
    "UT": "R08",
    "WY": "R08",
    "AZ": "R09",
    "CA": "R09",
    "HI": "R09",
    "NV": "R09",
    "AK": "R10",
    "ID": "R10",
    "OR": "R10",
    "WA": "R10",
}


@dataclass
class _Event:
    event_key: str
    ts_utc: str
    origin_callsign: str
    origin_grid6: str
    target_type: str
    target_id: str
    target_callsign: str
    target_grid6: str
    band: str
    mode: str
    freq_hz: Optional[float]
    distance_km: Optional[float]
    outcome: str
    source: str
    source_ref: str


def ingest_propagation_outcomes(settings, *, max_rows_per_source: int = 500) -> Dict[str, int]:
    """
    Ingest local JS8/VarAC/spotter outcomes into propagation outcome tables.

    Returns counters for observability.
    """
    max_rows = max(50, int(max_rows_per_source or 0))
    db_path = get_config_dir() / "config" / "freqinout_nets.db"
    if not db_path.exists():
        return {"rows_scanned": 0, "events_inserted": 0, "stats_updated": 0}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        operator_meta = _load_operator_meta(conn, settings)
        counters = {"rows_scanned": 0, "events_inserted": 0, "stats_updated": 0}
        processors = (
            _ingest_js8_links,
            _ingest_varac_links,
            _ingest_js8_messages,
            _ingest_varac_messages,
            _ingest_spotter_traffic,
        )
        for fn in processors:
            part = fn(conn, operator_meta, max_rows=max_rows)
            counters["rows_scanned"] += int(part.get("rows_scanned", 0))
            counters["events_inserted"] += int(part.get("events_inserted", 0))
            counters["stats_updated"] += int(part.get("stats_updated", 0))
        conn.commit()
        if counters["events_inserted"] > 0:
            log.debug(
                "PropagationOutcomeIngest: scanned=%s inserted=%s stats=%s",
                counters["rows_scanned"],
                counters["events_inserted"],
                counters["stats_updated"],
            )
        return counters
    except Exception as e:
        conn.rollback()
        log.debug("PropagationOutcomeIngest: ingest failed: %s", e)
        return {"rows_scanned": 0, "events_inserted": 0, "stats_updated": 0}
    finally:
        conn.close()


def _ingest_js8_links(conn: sqlite3.Connection, operator_meta: Dict[str, Dict], *, max_rows: int) -> Dict[str, int]:
    return _ingest_rows(
        conn,
        source_key=SOURCE_JS8_LINKS,
        source_label="JS8",
        query="""
            SELECT rowid AS rid, COALESCE(ts, 0) AS ts_val, origin, destination, band, freq_hz
            FROM js8_links
            WHERE (COALESCE(ts, 0) > ?)
               OR (COALESCE(ts, 0) = ? AND rowid > ?)
            ORDER BY COALESCE(ts, 0) ASC, rowid ASC
            LIMIT ?
        """,
        params_builder=lambda cp_ts, cp_ref, limit: (cp_ts, cp_ts, cp_ref, limit),
        row_to_events=lambda row: _row_to_events(
            source_key=SOURCE_JS8_LINKS,
            source_label="JS8",
            rowid=int(row["rid"]),
            ts_val=_to_ts(row["ts_val"]),
            origin_call=_clean_call(row["origin"]),
            target_call=_clean_call(row["destination"]),
            band=_normalize_band(row["band"], row["freq_hz"]),
            mode="JS8",
            freq_hz=_float_or_none(row["freq_hz"]),
            outcome="HEARD",
            operator_meta=operator_meta,
        ),
        max_rows=max_rows,
    )


def _ingest_varac_links(conn: sqlite3.Connection, operator_meta: Dict[str, Dict], *, max_rows: int) -> Dict[str, int]:
    return _ingest_rows(
        conn,
        source_key=SOURCE_VARAC_LINKS,
        source_label="VARAC",
        query="""
            SELECT rowid AS rid, COALESCE(ts, 0) AS ts_val, origin, destination, band, freq_hz, source
            FROM varac_links
            WHERE (COALESCE(ts, 0) > ?)
               OR (COALESCE(ts, 0) = ? AND rowid > ?)
            ORDER BY COALESCE(ts, 0) ASC, rowid ASC
            LIMIT ?
        """,
        params_builder=lambda cp_ts, cp_ref, limit: (cp_ts, cp_ts, cp_ref, limit),
        row_to_events=lambda row: _row_to_events(
            source_key=SOURCE_VARAC_LINKS,
            source_label="VARAC",
            rowid=int(row["rid"]),
            ts_val=_to_ts(row["ts_val"]),
            origin_call=_clean_call(row["origin"]),
            target_call=_clean_call(row["destination"]),
            band=_normalize_band(row["band"], row["freq_hz"]),
            mode="VARAC",
            freq_hz=_float_or_none(row["freq_hz"]),
            outcome="QSO" if str(row["source"] or "").strip().lower() == "qso" else "HEARD",
            operator_meta=operator_meta,
        ),
        max_rows=max_rows,
    )


def _ingest_js8_messages(
    conn: sqlite3.Connection, operator_meta: Dict[str, Dict], *, max_rows: int
) -> Dict[str, int]:
    return _ingest_rows(
        conn,
        source_key=SOURCE_JS8_MESSAGES,
        source_label="JS8",
        query="""
            SELECT local_id AS rid, COALESCE(utc_ts, 0) AS ts_val, from_call, to_call, msg_type
            FROM js8_messages_v2
            WHERE (COALESCE(utc_ts, 0) > ?)
               OR (COALESCE(utc_ts, 0) = ? AND local_id > ?)
            ORDER BY COALESCE(utc_ts, 0) ASC, local_id ASC
            LIMIT ?
        """,
        params_builder=lambda cp_ts, cp_ref, limit: (cp_ts, cp_ts, cp_ref, limit),
        row_to_events=lambda row: _row_to_events(
            source_key=SOURCE_JS8_MESSAGES,
            source_label="JS8",
            rowid=int(row["rid"]),
            ts_val=_to_ts(row["ts_val"]),
            origin_call=_clean_call(row["from_call"]),
            target_call=_clean_call(row["to_call"]),
            band="",
            mode="JS8",
            freq_hz=None,
            outcome="ACKED" if "ACK" in str(row["msg_type"] or "").upper() else "DELIVERED",
            operator_meta=operator_meta,
        ),
        max_rows=max_rows,
    )


def _ingest_varac_messages(
    conn: sqlite3.Connection, operator_meta: Dict[str, Dict], *, max_rows: int
) -> Dict[str, int]:
    return _ingest_rows(
        conn,
        source_key=SOURCE_VARAC_MESSAGES,
        source_label="VARAC",
        query="""
            SELECT rowid AS rid, COALESCE(ts, 0) AS ts_val, from_call, to_call, msg_type, band, freq_hz, read_status
            FROM varac_messages
            WHERE (COALESCE(ts, 0) > ?)
               OR (COALESCE(ts, 0) = ? AND rowid > ?)
            ORDER BY COALESCE(ts, 0) ASC, rowid ASC
            LIMIT ?
        """,
        params_builder=lambda cp_ts, cp_ref, limit: (cp_ts, cp_ts, cp_ref, limit),
        row_to_events=lambda row: _row_to_events(
            source_key=SOURCE_VARAC_MESSAGES,
            source_label="VARAC",
            rowid=int(row["rid"]),
            ts_val=_to_ts(row["ts_val"]),
            origin_call=_clean_call(row["from_call"]),
            target_call=_clean_call(row["to_call"]),
            band=_normalize_band(row["band"], row["freq_hz"]),
            mode="VARAC",
            freq_hz=_float_or_none(row["freq_hz"]),
            outcome=_varac_message_outcome(row["msg_type"], row["read_status"]),
            operator_meta=operator_meta,
        ),
        max_rows=max_rows,
    )


def _ingest_spotter_traffic(
    conn: sqlite3.Connection, operator_meta: Dict[str, Dict], *, max_rows: int
) -> Dict[str, int]:
    return _ingest_rows(
        conn,
        source_key=SOURCE_SPOTTER,
        source_label="JS8",
        query="""
            SELECT rowid AS rid, COALESCE(utc_ts, 0) AS ts_val, from_call, to_call
            FROM spotter_traffic
            WHERE (COALESCE(utc_ts, 0) > ?)
               OR (COALESCE(utc_ts, 0) = ? AND rowid > ?)
            ORDER BY COALESCE(utc_ts, 0) ASC, rowid ASC
            LIMIT ?
        """,
        params_builder=lambda cp_ts, cp_ref, limit: (cp_ts, cp_ts, cp_ref, limit),
        row_to_events=lambda row: _row_to_events(
            source_key=SOURCE_SPOTTER,
            source_label="JS8",
            rowid=int(row["rid"]),
            ts_val=_to_ts(row["ts_val"]),
            origin_call=_clean_call(row["from_call"]),
            target_call=_clean_call(row["to_call"]),
            band="",
            mode="JS8",
            freq_hz=None,
            outcome="DELIVERED",
            operator_meta=operator_meta,
        ),
        max_rows=max_rows,
    )


def _ingest_rows(
    conn: sqlite3.Connection,
    *,
    source_key: str,
    source_label: str,
    query: str,
    params_builder,
    row_to_events,
    max_rows: int,
) -> Dict[str, int]:
    if not _table_exists_for_source(conn, source_key):
        return {"rows_scanned": 0, "events_inserted": 0, "stats_updated": 0}

    cp_ts, cp_ref = _get_checkpoint(conn, source_key)
    cur = conn.cursor()
    params = params_builder(cp_ts, cp_ref, max_rows)
    cur.execute(query, params)
    rows = cur.fetchall()
    if not rows:
        return {"rows_scanned": 0, "events_inserted": 0, "stats_updated": 0}

    now_utc = _utc_now_str()
    rows_scanned = 0
    events_inserted = 0
    stats_updated = 0
    last_ts = cp_ts
    last_ref = cp_ref

    for row in rows:
        rows_scanned += 1
        rid = int(row["rid"])
        ts_val = _to_ts(row["ts_val"])
        last_ts = ts_val
        last_ref = rid
        for event in row_to_events(row):
            inserted = _insert_event(conn, event, inserted_utc=now_utc)
            if not inserted:
                continue
            events_inserted += 1
            _upsert_stats(conn, event, updated_utc=now_utc)
            stats_updated += 1

    _set_checkpoint(conn, source_key, last_ts=last_ts, last_ref=last_ref, updated_utc=now_utc)
    return {
        "rows_scanned": rows_scanned,
        "events_inserted": events_inserted,
        "stats_updated": stats_updated,
    }


def _row_to_events(
    *,
    source_key: str,
    source_label: str,
    rowid: int,
    ts_val: float,
    origin_call: str,
    target_call: str,
    band: str,
    mode: str,
    freq_hz: Optional[float],
    outcome: str,
    operator_meta: Dict[str, Dict],
) -> List[_Event]:
    if ts_val <= 0:
        return []
    if not _is_callsign(origin_call):
        return []
    if not _is_callsign(target_call):
        return []

    origin = operator_meta.get(origin_call, {})
    target = operator_meta.get(target_call, {})
    origin_grid = _normalize_grid(origin.get("grid", ""))
    if not origin_grid:
        return []
    target_grid = _normalize_grid(target.get("grid", ""))
    target_state = _normalize_state(target.get("state", ""))
    target_region = STATE_TO_FEMA_REGION.get(target_state, "")
    dist = _distance_km(origin_grid, target_grid) if target_grid else None
    ts_utc = _ts_to_utc_str(ts_val)
    outcome_up = _normalize_outcome(outcome)
    variants: List[Tuple[str, str]] = [("OPERATOR", target_call)]
    if target_state:
        variants.append(("STATE", target_state))
    if target_region:
        variants.append(("REGION", target_region))

    events: List[_Event] = []
    for target_type, target_id in variants:
        ek = f"{source_key}:{rowid}:{int(ts_val)}:{target_type}:{target_id}"
        events.append(
            _Event(
                event_key=ek,
                ts_utc=ts_utc,
                origin_callsign=origin_call,
                origin_grid6=origin_grid,
                target_type=target_type,
                target_id=target_id,
                target_callsign=target_call if target_type == "OPERATOR" else "",
                target_grid6=target_grid if target_type == "OPERATOR" else "",
                band=band,
                mode=mode,
                freq_hz=freq_hz,
                distance_km=dist,
                outcome=outcome_up,
                source=source_label,
                source_ref=f"{source_key}:{rowid}",
            )
        )
    return events


def _insert_event(conn: sqlite3.Connection, event: _Event, *, inserted_utc: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO prop_contact_events
            (event_key, ts_utc, origin_callsign, origin_grid6, target_type, target_id,
             target_callsign, target_grid6, band, mode, freq_hz, distance_km,
             outcome, source, source_ref, inserted_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_key,
            event.ts_utc,
            event.origin_callsign,
            event.origin_grid6,
            event.target_type,
            event.target_id,
            event.target_callsign,
            event.target_grid6,
            event.band,
            event.mode,
            event.freq_hz,
            event.distance_km,
            event.outcome,
            event.source,
            event.source_ref,
            inserted_utc,
        ),
    )
    return cur.rowcount > 0


def _upsert_stats(conn: sqlite3.Connection, event: _Event, *, updated_utc: str) -> None:
    ts_dt = dt.datetime.strptime(event.ts_utc, "%Y-%m-%d %H:%M:%S")
    month = int(ts_dt.month)
    hour_bucket = int(ts_dt.hour)
    dist_bucket = _distance_bucket(event.distance_km)
    key_hash = _stats_key_hash(
        origin_grid6=event.origin_grid6,
        target_type=event.target_type,
        target_id=event.target_id,
        band=event.band,
        mode=event.mode,
        month=month,
        utc_hour_bucket=hour_bucket,
        distance_bucket=dist_bucket,
    )
    success = 0 if event.outcome == "FAILED" else 1
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO prop_outcome_stats
            (key_hash, origin_grid6, target_type, target_id, band, mode, month,
             utc_hour_bucket, distance_bucket, attempt_count, success_count,
             weighted_attempt, weighted_success, last_event_utc, updated_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(key_hash) DO UPDATE SET
            attempt_count = prop_outcome_stats.attempt_count + excluded.attempt_count,
            success_count = prop_outcome_stats.success_count + excluded.success_count,
            weighted_attempt = prop_outcome_stats.weighted_attempt + excluded.weighted_attempt,
            weighted_success = prop_outcome_stats.weighted_success + excluded.weighted_success,
            last_event_utc = CASE
                WHEN COALESCE(prop_outcome_stats.last_event_utc, '') < COALESCE(excluded.last_event_utc, '')
                THEN excluded.last_event_utc
                ELSE prop_outcome_stats.last_event_utc
            END,
            updated_utc = excluded.updated_utc
        """,
        (
            key_hash,
            event.origin_grid6,
            event.target_type,
            event.target_id,
            event.band,
            event.mode,
            month,
            hour_bucket,
            dist_bucket,
            1,
            success,
            1.0,
            float(success),
            event.ts_utc,
            updated_utc,
        ),
    )


def _load_operator_meta(conn: sqlite3.Connection, settings) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT callsign, state, grid FROM operator_checkins")
        for callsign, state, grid in cur.fetchall():
            cs = _clean_call(callsign)
            if not cs:
                continue
            st = _normalize_state(state)
            out[cs] = {
                "state": st,
                "grid": _normalize_grid(grid),
                "region": STATE_TO_FEMA_REGION.get(st, ""),
            }
    except Exception:
        pass

    try:
        my_call = _clean_call(settings.get("operator_callsign", "") or "")
    except Exception:
        my_call = ""
    try:
        my_state = _normalize_state(settings.get("operator_state", "") or "")
    except Exception:
        my_state = ""
    try:
        my_grid = _normalize_grid(settings.get("operator_grid6", "") or settings.get("operator_grid", "") or "")
    except Exception:
        my_grid = ""
    if my_call:
        meta = out.get(my_call, {})
        if my_state:
            meta["state"] = my_state
            meta["region"] = STATE_TO_FEMA_REGION.get(my_state, "")
        if my_grid:
            meta["grid"] = my_grid
        out[my_call] = meta
    return out


def _get_checkpoint(conn: sqlite3.Connection, source_key: str) -> Tuple[float, int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT last_ts_utc, last_source_ref FROM prop_ingest_checkpoint WHERE source=?",
        (source_key,),
    )
    row = cur.fetchone()
    if not row:
        return 0.0, 0
    return _to_ts(row[0]), _to_int(row[1])


def _set_checkpoint(
    conn: sqlite3.Connection, source_key: str, *, last_ts: float, last_ref: int, updated_utc: str
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO prop_ingest_checkpoint (source, last_ts_utc, last_source_ref, updated_utc)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(source) DO UPDATE SET
            last_ts_utc = excluded.last_ts_utc,
            last_source_ref = excluded.last_source_ref,
            updated_utc = excluded.updated_utc
        """,
        (source_key, f"{float(last_ts):.6f}", str(int(last_ref)), updated_utc),
    )


def _table_exists_for_source(conn: sqlite3.Connection, source_key: str) -> bool:
    table_by_source = {
        SOURCE_JS8_LINKS: "js8_links",
        SOURCE_VARAC_LINKS: "varac_links",
        SOURCE_JS8_MESSAGES: "js8_messages_v2",
        SOURCE_VARAC_MESSAGES: "varac_messages",
        SOURCE_SPOTTER: "spotter_traffic",
    }
    table = table_by_source.get(source_key, "")
    if not table:
        return False
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _stats_key_hash(
    *,
    origin_grid6: str,
    target_type: str,
    target_id: str,
    band: str,
    mode: str,
    month: int,
    utc_hour_bucket: int,
    distance_bucket: str,
) -> str:
    raw = "|".join(
        [
            origin_grid6,
            target_type,
            target_id,
            band,
            mode,
            str(month),
            str(utc_hour_bucket),
            distance_bucket,
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _distance_km(grid_a: str, grid_b: str) -> Optional[float]:
    a = _maidenhead_to_latlon(grid_a)
    b = _maidenhead_to_latlon(grid_b)
    if not a or not b:
        return None
    return _haversine_km(a[0], a[1], b[0], b[1])


def _maidenhead_to_latlon(grid: str) -> Optional[Tuple[float, float]]:
    grid = _normalize_grid(grid)
    if len(grid) < 4:
        return None
    try:
        lon = (ord(grid[0]) - ord("A")) * 20.0 + int(grid[2]) * 2.0 + 1.0 / 24.0
        lat = (ord(grid[1]) - ord("A")) * 10.0 + int(grid[3]) * 1.0 + 1.0 / 48.0
        if len(grid) >= 6:
            lon += (ord(grid[4]) - ord("A")) / 12.0
            lat += (ord(grid[5]) - ord("A")) / 24.0
        lon -= 180.0
        lat -= 90.0
        return lat, lon
    except Exception:
        return None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlon / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def _distance_bucket(distance_km: Optional[float]) -> str:
    if distance_km is None:
        return "REGIONAL"
    if distance_km < 300.0:
        return "LOCAL"
    if distance_km <= 1500.0:
        return "REGIONAL"
    return "DX"


def _normalize_band(band: object, freq_hz: object) -> str:
    txt = str(band or "").strip().upper()
    if txt:
        return txt
    mhz = None
    try:
        hz = float(freq_hz)
        if hz > 2000:
            mhz = hz / 1_000_000.0
        else:
            mhz = hz
    except Exception:
        mhz = None
    if mhz is None:
        return ""
    ranges = (
        ("80M", 3.5, 4.0),
        ("40M", 7.0, 7.3),
        ("30M", 10.1, 10.15),
        ("20M", 14.0, 14.35),
        ("15M", 21.0, 21.45),
        ("10M", 28.0, 29.7),
    )
    for name, lo, hi in ranges:
        if lo <= mhz <= hi:
            return name
    return ""


def _normalize_outcome(outcome: str) -> str:
    out = (outcome or "").strip().upper()
    if out in {"HEARD", "DELIVERED", "ACKED", "QSO", "FAILED"}:
        return out
    return "HEARD"


def _varac_message_outcome(msg_type: object, read_status: object) -> str:
    mt = str(msg_type or "").strip().upper()
    if "QSO" in mt:
        return "QSO"
    try:
        if int(read_status or 0) != 0:
            return "ACKED"
    except Exception:
        pass
    return "DELIVERED"


def _normalize_grid(grid: object) -> str:
    txt = str(grid or "").strip().upper()
    if len(txt) < 4:
        return ""
    return txt[:6]


def _normalize_state(state: object) -> str:
    txt = str(state or "").strip().upper()
    if len(txt) < 2:
        return ""
    return txt[:2]


def _clean_call(val: object) -> str:
    return str(val or "").strip().upper()


def _is_callsign(val: str) -> bool:
    return bool(CALLSIGN_RE.fullmatch(_clean_call(val)))


def _ts_to_utc_str(ts_val: float) -> str:
    return dt.datetime.fromtimestamp(float(ts_val), tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _utc_now_str() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _to_ts(val: object) -> float:
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return 0.0
        return max(0.0, f)
    except Exception:
        return 0.0


def _to_int(val: object) -> int:
    try:
        return int(float(val))
    except Exception:
        return 0


def _float_or_none(val: object) -> Optional[float]:
    try:
        if val is None or val == "":
            return None
        return float(val)
    except Exception:
        return None
