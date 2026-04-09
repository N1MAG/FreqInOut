from __future__ import annotations

import datetime
import math
import sqlite3
from typing import Dict, Iterable, Optional, Tuple

from freqinout.core.logger import log


def _clean_callsign(value: object) -> str:
    return str(value or "").strip().upper()


def parse_utc_timestamp(value: object) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        numeric = float(text)
        if math.isfinite(numeric) and numeric > 0:
            return numeric
    except Exception:
        pass
    if len(text) == 8 and text.isdigit():
        try:
            dt = datetime.datetime.strptime(text, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            return None
    if len(text) == 10 and text.count("-") == 2:
        try:
            dt = datetime.datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            return None
    try:
        dt = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt.timestamp()
    except Exception:
        pass
    if len(text) >= 19 and " " in text:
        try:
            dt = datetime.datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            pass
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 14:
        try:
            dt = datetime.datetime.strptime(digits[:14], "%Y%m%d%H%M%S").replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            pass
    if len(digits) >= 8:
        try:
            dt = datetime.datetime.strptime(digits[:8], "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            pass
    return None


def format_utc_iso(ts: Optional[float]) -> str:
    try:
        ts_val = float(ts or 0.0)
    except Exception:
        ts_val = 0.0
    if ts_val <= 0:
        return ""
    return datetime.datetime.fromtimestamp(ts_val, datetime.timezone.utc).isoformat()


def newer_timestamp_text(existing: object, incoming: object) -> str:
    existing_txt = str(existing or "").strip()
    incoming_txt = str(incoming or "").strip()
    if not incoming_txt:
        return existing_txt
    if not existing_txt:
        return incoming_txt
    existing_ts = parse_utc_timestamp(existing_txt)
    incoming_ts = parse_utc_timestamp(incoming_txt)
    if existing_ts is not None and incoming_ts is not None:
        return incoming_txt if incoming_ts > existing_ts else existing_txt
    if incoming_ts is not None and existing_ts is None:
        return incoming_txt
    if existing_ts is not None and incoming_ts is None:
        return existing_txt
    return incoming_txt if incoming_txt > existing_txt else existing_txt


def ensure_js8_callsign_stats(conn: sqlite3.Connection, *, rebuild_if_empty: bool = False) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_callsign_stats (
            callsign TEXT PRIMARY KEY,
            last_seen_ts REAL,
            last_band TEXT,
            last_freq_hz REAL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_callsign_stats_last_seen ON js8_callsign_stats(last_seen_ts)")
    if rebuild_if_empty and _table_has_rows(conn, "js8_links") and not _table_has_rows(conn, "js8_callsign_stats"):
        rebuild_js8_callsign_stats(conn)


def _table_has_rows(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return row is not None
    except Exception:
        return False


def rebuild_js8_callsign_stats(conn: sqlite3.Connection) -> int:
    ensure_js8_callsign_stats(conn, rebuild_if_empty=False)
    stats: Dict[str, Tuple[float, str, Optional[float]]] = {}
    try:
        cur = conn.execute("SELECT ts, origin, destination, band, freq_hz FROM js8_links")
        for ts, origin, destination, band, freq_hz in cur.fetchall():
            ts_val = parse_utc_timestamp(ts)
            if ts_val is None or ts_val <= 0:
                continue
            band_val = str(band or "").strip().upper()
            try:
                freq_val = float(freq_hz) if freq_hz is not None else None
            except Exception:
                freq_val = None
            for callsign in (_clean_callsign(origin), _clean_callsign(destination)):
                if not callsign:
                    continue
                existing = stats.get(callsign)
                if existing is None or ts_val > existing[0]:
                    stats[callsign] = (ts_val, band_val, freq_val)
        conn.execute("DELETE FROM js8_callsign_stats")
        if stats:
            conn.executemany(
                """
                INSERT INTO js8_callsign_stats (callsign, last_seen_ts, last_band, last_freq_hz)
                VALUES (?, ?, ?, ?)
                """,
                [(cs, entry[0], entry[1], entry[2]) for cs, entry in stats.items()],
            )
        return len(stats)
    except Exception as exc:
        log.debug("operator_activity: failed to rebuild js8_callsign_stats: %s", exc)
        return 0


def record_js8_activity_batch(
    conn: sqlite3.Connection,
    rows: Iterable[Tuple[str, object, str, object]],
) -> int:
    ensure_js8_callsign_stats(conn, rebuild_if_empty=False)
    latest: Dict[str, Tuple[float, str, Optional[float]]] = {}
    for callsign, ts, band, freq_hz in rows:
        cs = _clean_callsign(callsign)
        ts_val = parse_utc_timestamp(ts)
        if not cs or ts_val is None or ts_val <= 0:
            continue
        band_val = str(band or "").strip().upper()
        try:
            freq_val = float(freq_hz) if freq_hz is not None else None
        except Exception:
            freq_val = None
        current = latest.get(cs)
        if current is None or ts_val > current[0]:
            latest[cs] = (ts_val, band_val, freq_val)
    if not latest:
        return 0
    conn.executemany(
        """
        INSERT INTO js8_callsign_stats (callsign, last_seen_ts, last_band, last_freq_hz)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(callsign) DO UPDATE SET
            last_seen_ts=CASE
                WHEN COALESCE(excluded.last_seen_ts, 0) > COALESCE(js8_callsign_stats.last_seen_ts, 0)
                    THEN excluded.last_seen_ts
                ELSE js8_callsign_stats.last_seen_ts
            END,
            last_band=CASE
                WHEN COALESCE(excluded.last_seen_ts, 0) > COALESCE(js8_callsign_stats.last_seen_ts, 0)
                    THEN excluded.last_band
                WHEN COALESCE(excluded.last_seen_ts, 0) = COALESCE(js8_callsign_stats.last_seen_ts, 0)
                     AND COALESCE(js8_callsign_stats.last_band, '') = ''
                    THEN excluded.last_band
                ELSE js8_callsign_stats.last_band
            END,
            last_freq_hz=CASE
                WHEN COALESCE(excluded.last_seen_ts, 0) > COALESCE(js8_callsign_stats.last_seen_ts, 0)
                    THEN excluded.last_freq_hz
                WHEN COALESCE(excluded.last_seen_ts, 0) = COALESCE(js8_callsign_stats.last_seen_ts, 0)
                     AND js8_callsign_stats.last_freq_hz IS NULL
                    THEN excluded.last_freq_hz
                ELSE js8_callsign_stats.last_freq_hz
            END
        """,
        [(cs, entry[0], entry[1], entry[2]) for cs, entry in latest.items()],
    )
    return len(latest)


def load_operator_activity_summary(
    conn: sqlite3.Connection,
    *,
    include_operator_checkins_fallback: bool = True,
) -> Dict[str, Dict[str, object]]:
    ensure_js8_callsign_stats(conn, rebuild_if_empty=True)
    try:
        conn.commit()
    except Exception:
        pass
    summary: Dict[str, Dict[str, object]] = {}
    _load_js8_summary(conn, summary)
    _load_varac_summary(conn, summary)
    if include_operator_checkins_fallback:
        _load_operator_checkins_fallback(conn, summary)
    return summary


def load_js8_direct_contact_summary(
    conn: sqlite3.Connection,
    my_call: str,
) -> Dict[str, Dict[str, object]]:
    my_call_clean = _clean_callsign(my_call)
    if not my_call_clean:
        return {}
    summary: Dict[str, Dict[str, object]] = {}
    try:
        cur = conn.execute(
            """
            SELECT ts, origin, band, freq_hz, snr
            FROM js8_links
            WHERE destination = ?
            """,
            (my_call_clean,),
        )
    except Exception:
        return summary
    for ts, origin, band, freq_hz, snr in cur.fetchall():
        callsign = _clean_callsign(origin)
        ts_val = parse_utc_timestamp(ts)
        if not callsign or callsign == my_call_clean or ts_val is None or ts_val <= 0:
            continue
        existing_ts = float(summary.get(callsign, {}).get("last_contact_ts", 0.0) or 0.0)
        if ts_val <= existing_ts:
            continue
        try:
            freq_val = float(freq_hz) if freq_hz is not None else None
        except Exception:
            freq_val = None
        try:
            snr_val = float(snr) if snr is not None else None
        except Exception:
            snr_val = None
        summary[callsign] = {
            "last_contact_ts": float(ts_val),
            "last_contact_band": str(band or "").strip().upper(),
            "last_contact_freq_hz": freq_val,
            "last_contact_snr": snr_val,
        }
    return summary


def _summary_entry(summary: Dict[str, Dict[str, object]], callsign: str) -> Dict[str, object]:
    entry = summary.get(callsign)
    if entry is None:
        entry = {
            "overall_last_seen_ts": 0.0,
            "overall_last_seen_source": "",
            "overall_last_band": "",
            "overall_last_freq_hz": None,
            "js8_last_seen_ts": 0.0,
            "js8_last_band": "",
            "js8_last_freq_hz": None,
            "varac_last_seen_ts": 0.0,
            "varac_last_band": "",
            "varac_last_freq_hz": None,
            "legacy_last_seen_ts": 0.0,
        }
        summary[callsign] = entry
    return entry


def _apply_overall(
    entry: Dict[str, object],
    ts_val: Optional[float],
    source: str,
    band: str = "",
    freq_hz: object = None,
) -> None:
    if ts_val is None or ts_val <= 0:
        return
    current = float(entry.get("overall_last_seen_ts", 0.0) or 0.0)
    if ts_val > current:
        entry["overall_last_seen_ts"] = float(ts_val)
        entry["overall_last_seen_source"] = source
        entry["overall_last_band"] = band or ""
        entry["overall_last_freq_hz"] = freq_hz


def _load_js8_summary(conn: sqlite3.Connection, summary: Dict[str, Dict[str, object]]) -> None:
    try:
        cur = conn.execute("SELECT callsign, last_seen_ts, last_band, last_freq_hz FROM js8_callsign_stats")
    except Exception:
        return
    for callsign, last_seen_ts, last_band, last_freq_hz in cur.fetchall():
        cs = _clean_callsign(callsign)
        if not cs:
            continue
        ts_val = parse_utc_timestamp(last_seen_ts)
        entry = _summary_entry(summary, cs)
        entry["js8_last_seen_ts"] = float(ts_val or 0.0)
        entry["js8_last_band"] = str(last_band or "").strip().upper()
        entry["js8_last_freq_hz"] = last_freq_hz
        _apply_overall(entry, ts_val, "js8", str(last_band or "").strip().upper(), last_freq_hz)


def _load_varac_summary(conn: sqlite3.Connection, summary: Dict[str, Dict[str, object]]) -> None:
    try:
        cur = conn.execute("SELECT callsign, last_seen_ts, last_band, last_freq_hz FROM varac_callsign_stats")
    except Exception:
        return
    for callsign, last_seen_ts, last_band, last_freq_hz in cur.fetchall():
        cs = _clean_callsign(callsign)
        if not cs:
            continue
        ts_val = parse_utc_timestamp(last_seen_ts)
        entry = _summary_entry(summary, cs)
        entry["varac_last_seen_ts"] = float(ts_val or 0.0)
        entry["varac_last_band"] = str(last_band or "").strip().upper()
        entry["varac_last_freq_hz"] = last_freq_hz
        _apply_overall(entry, ts_val, "varac", str(last_band or "").strip().upper(), last_freq_hz)


def _load_operator_checkins_fallback(conn: sqlite3.Connection, summary: Dict[str, Dict[str, object]]) -> None:
    try:
        cur = conn.execute("SELECT callsign, last_seen_utc FROM operator_checkins")
    except Exception:
        return
    for callsign, last_seen_utc in cur.fetchall():
        cs = _clean_callsign(callsign)
        if not cs:
            continue
        ts_val = parse_utc_timestamp(last_seen_utc)
        if ts_val is None or ts_val <= 0:
            continue
        entry = _summary_entry(summary, cs)
        entry["legacy_last_seen_ts"] = float(ts_val)
        if float(entry.get("overall_last_seen_ts", 0.0) or 0.0) <= 0:
            _apply_overall(entry, ts_val, "operator_checkins")
