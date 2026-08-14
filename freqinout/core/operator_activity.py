from __future__ import annotations

import datetime
import json
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
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 14:
        try:
            dt = datetime.datetime.strptime(digits[:14], "%Y%m%d%H%M%S").replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            pass
    if len(text) == 8 and text.isdigit():
        try:
            dt = datetime.datetime.strptime(text, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
            return dt.timestamp()
        except Exception:
            return None
    try:
        numeric = float(text)
        if math.isfinite(numeric) and numeric > 0:
            return numeric
    except Exception:
        pass
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
            last_freq_hz REAL,
            last_source_id TEXT,
            last_app_instance_id TEXT,
            last_source_radio_id TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(js8_callsign_stats)")
    cols = {str(row[1] or "") for row in cur.fetchall()}
    for name in ("last_source_id", "last_app_instance_id", "last_source_radio_id"):
        if name not in cols:
            cur.execute(f"ALTER TABLE js8_callsign_stats ADD COLUMN {name} TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_callsign_stats_last_seen ON js8_callsign_stats(last_seen_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_callsign_stats_source ON js8_callsign_stats(last_source_id, last_seen_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_js8_callsign_stats_radio ON js8_callsign_stats(last_source_radio_id, last_seen_ts)")
    if rebuild_if_empty and _table_has_rows(conn, "js8_links") and not _table_has_rows(conn, "js8_callsign_stats"):
        rebuild_js8_callsign_stats(conn)


def _table_has_rows(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return row is not None
    except Exception:
        return False


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {str(row[1] or "") for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    except Exception:
        return set()


def _js8_links_stats_select_sql(conn: sqlite3.Connection) -> str:
    cols = _table_columns(conn, "js8_links")
    source_id = "source_id" if "source_id" in cols else "'' AS source_id"
    app_instance_id = "app_instance_id" if "app_instance_id" in cols else "'' AS app_instance_id"
    source_radio_id = "source_radio_id" if "source_radio_id" in cols else "'' AS source_radio_id"
    return f"""
        SELECT ts, origin, destination, band, freq_hz, {source_id}, {app_instance_id}, {source_radio_id}
          FROM js8_links
    """


def _coerce_js8_activity_row(row: Tuple) -> Tuple[object, object, object, object, str, str, str]:
    values = tuple(row or ())
    padded = (*values, "", "", "")[:7]
    return (
        padded[0],
        padded[1],
        padded[2],
        padded[3],
        str(padded[4] or "").strip(),
        str(padded[5] or "").strip(),
        str(padded[6] or "").strip(),
    )


def rebuild_js8_callsign_stats(conn: sqlite3.Connection) -> int:
    ensure_js8_callsign_stats(conn, rebuild_if_empty=False)
    stats: Dict[str, Tuple[float, str, Optional[float], str, str, str]] = {}
    try:
        select_sql = _js8_links_stats_select_sql(conn)
        cur = conn.execute(select_sql)
        for ts, origin, destination, band, freq_hz, source_id, app_instance_id, source_radio_id in cur.fetchall():
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
                    stats[callsign] = (
                        ts_val,
                        band_val,
                        freq_val,
                        str(source_id or "").strip(),
                        str(app_instance_id or "").strip(),
                        str(source_radio_id or "").strip(),
                    )
        conn.execute("DELETE FROM js8_callsign_stats")
        if stats:
            conn.executemany(
                """
                INSERT INTO js8_callsign_stats (
                    callsign, last_seen_ts, last_band, last_freq_hz,
                    last_source_id, last_app_instance_id, last_source_radio_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [(cs, entry[0], entry[1], entry[2], entry[3], entry[4], entry[5]) for cs, entry in stats.items()],
            )
        return len(stats)
    except Exception as exc:
        log.debug("operator_activity: failed to rebuild js8_callsign_stats: %s", exc)
        return 0


def record_js8_activity_batch(
    conn: sqlite3.Connection,
    rows: Iterable[Tuple],
) -> int:
    ensure_js8_callsign_stats(conn, rebuild_if_empty=False)
    latest: Dict[str, Tuple[float, str, Optional[float], str, str, str]] = {}
    for row in rows:
        callsign, ts, band, freq_hz, source_id, app_instance_id, source_radio_id = _coerce_js8_activity_row(row)
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
            latest[cs] = (
                ts_val,
                band_val,
                freq_val,
                str(source_id or "").strip(),
                str(app_instance_id or "").strip(),
                str(source_radio_id or "").strip(),
            )
    if not latest:
        return 0
    conn.executemany(
        """
        INSERT INTO js8_callsign_stats (
            callsign, last_seen_ts, last_band, last_freq_hz,
            last_source_id, last_app_instance_id, last_source_radio_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
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
            END,
            last_source_id=CASE
                WHEN COALESCE(excluded.last_seen_ts, 0) > COALESCE(js8_callsign_stats.last_seen_ts, 0)
                    THEN excluded.last_source_id
                WHEN COALESCE(excluded.last_seen_ts, 0) = COALESCE(js8_callsign_stats.last_seen_ts, 0)
                     AND COALESCE(js8_callsign_stats.last_source_id, '') = ''
                    THEN excluded.last_source_id
                ELSE js8_callsign_stats.last_source_id
            END,
            last_app_instance_id=CASE
                WHEN COALESCE(excluded.last_seen_ts, 0) > COALESCE(js8_callsign_stats.last_seen_ts, 0)
                    THEN excluded.last_app_instance_id
                WHEN COALESCE(excluded.last_seen_ts, 0) = COALESCE(js8_callsign_stats.last_seen_ts, 0)
                     AND COALESCE(js8_callsign_stats.last_app_instance_id, '') = ''
                    THEN excluded.last_app_instance_id
                ELSE js8_callsign_stats.last_app_instance_id
            END,
            last_source_radio_id=CASE
                WHEN COALESCE(excluded.last_seen_ts, 0) > COALESCE(js8_callsign_stats.last_seen_ts, 0)
                    THEN excluded.last_source_radio_id
                WHEN COALESCE(excluded.last_seen_ts, 0) = COALESCE(js8_callsign_stats.last_seen_ts, 0)
                     AND COALESCE(js8_callsign_stats.last_source_radio_id, '') = ''
                    THEN excluded.last_source_radio_id
                ELSE js8_callsign_stats.last_source_radio_id
            END
        """,
        [(cs, entry[0], entry[1], entry[2], entry[3], entry[4], entry[5]) for cs, entry in latest.items()],
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
    _load_imported_spotter_summary(conn, summary)
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
            "spotter_last_seen_ts": 0.0,
            "spotter_last_band": "",
            "spotter_last_freq_hz": None,
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


def _load_imported_spotter_summary(conn: sqlite3.Connection, summary: Dict[str, Dict[str, object]]) -> None:
    query = """
        SELECT source_table, payload_json, imported_ts
        FROM js8spotter_import_archive
        WHERE lower(source_table) IN ('grid', 'signal', 'activity')
        ORDER BY
            COALESCE(
                json_extract(payload_json, '$.sig_timestamp'),
                json_extract(payload_json, '$.grid_timestamp'),
                json_extract(payload_json, '$.spotdate'),
                json_extract(payload_json, '$.timestamp'),
                json_extract(payload_json, '$.lm'),
                imported_ts
            ) DESC,
            id DESC
        LIMIT 5000
    """
    try:
        cur = conn.execute(query)
    except Exception:
        try:
            cur = conn.execute(
                """
                SELECT source_table, payload_json, imported_ts
                FROM js8spotter_import_archive
                WHERE lower(source_table) IN ('grid', 'signal', 'activity')
                ORDER BY id DESC
                LIMIT 5000
                """
            )
        except Exception:
            return
    for source_table, payload_json, imported_ts in cur.fetchall():
        table = str(source_table or "").strip().lower()
        try:
            payload = json.loads(str(payload_json or "{}"))
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            continue
        callsign = ""
        timestamp = None
        freq_value = None
        if table == "grid":
            callsign = _first_payload_text(payload, "grid_callsign", "callsign", "call")
            timestamp = payload.get("grid_timestamp") or payload.get("timestamp") or payload.get("lm")
            freq_value = payload.get("grid_dial") or payload.get("dial") or payload.get("freq")
        elif table == "signal":
            callsign = _first_payload_text(payload, "sig_callsign", "callsign", "call")
            timestamp = payload.get("sig_timestamp") or payload.get("timestamp") or payload.get("lm")
            freq_value = payload.get("sig_freq") or payload.get("sig_dial") or payload.get("freq") or payload.get("dial")
        elif table == "activity":
            callsign = _first_payload_text(payload, "call", "callsign", "fromcall")
            timestamp = payload.get("spotdate") or payload.get("timestamp") or payload.get("lm")
            freq_value = payload.get("freq") or payload.get("dial")
        cs = _clean_callsign(callsign)
        ts_val = parse_utc_timestamp(timestamp)
        if ts_val is None:
            ts_val = parse_utc_timestamp(imported_ts)
        if not cs or ts_val is None or ts_val <= 0:
            continue
        freq_hz = _freq_hz_from_value(freq_value)
        band = _band_from_freq_hz(freq_hz)
        entry = _summary_entry(summary, cs)
        existing = float(entry.get("spotter_last_seen_ts", 0.0) or 0.0)
        if ts_val > existing:
            entry["spotter_last_seen_ts"] = float(ts_val)
            entry["spotter_last_band"] = band
            entry["spotter_last_freq_hz"] = freq_hz
        _apply_overall(entry, ts_val, "spotter_import", band, freq_hz)


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


def _first_payload_text(payload: Dict[str, object], *keys: str) -> str:
    for key in keys:
        value = str(payload.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _freq_hz_from_value(value: object) -> Optional[float]:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        numeric = float(text)
    except Exception:
        return None
    if numeric <= 0:
        return None
    if numeric < 1000:
        return numeric * 1_000_000.0
    if numeric < 100_000:
        return numeric * 1_000.0
    return numeric


def _band_from_freq_hz(freq_hz: Optional[float]) -> str:
    if not freq_hz or freq_hz <= 0:
        return ""
    mhz = float(freq_hz) / 1_000_000.0
    if 1.8 <= mhz < 2.0:
        return "160M"
    if 3.0 <= mhz < 4.0:
        return "80M"
    if 5.0 <= mhz < 5.5:
        return "60M"
    if 7.0 <= mhz < 7.4:
        return "40M"
    if 10.0 <= mhz < 10.2:
        return "30M"
    if 14.0 <= mhz < 14.35:
        return "20M"
    if 18.0 <= mhz < 18.2:
        return "17M"
    if 21.0 <= mhz < 21.45:
        return "15M"
    if 24.8 <= mhz < 25.0:
        return "12M"
    if 28.0 <= mhz < 29.7:
        return "10M"
    if 50.0 <= mhz < 54.0:
        return "6M"
    return ""
