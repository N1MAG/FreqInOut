from __future__ import annotations

import datetime
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log

CALLSIGN_RE = re.compile(r"^[A-Z0-9/]{3,10}$")


def _local_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout_nets.db"


def _parse_dt(val) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        ts = float(val)
        if ts > 1e12:
            ts = ts / 1000.0
        return ts
    txt = str(val).strip()
    if not txt:
        return 0.0
    if txt.isdigit():
        try:
            ts = float(txt)
            if ts > 1e12:
                ts = ts / 1000.0
            return ts
        except Exception:
            return 0.0
    txt = txt.replace("Z", "").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.datetime.strptime(txt, fmt)
            return dt.timestamp()
        except Exception:
            continue
    try:
        dt = datetime.datetime.fromisoformat(txt)
        return dt.timestamp()
    except Exception:
        return 0.0


def _hz_to_band(freq_hz: Optional[float]) -> str:
    if not freq_hz:
        return ""
    try:
        mhz = float(freq_hz) / 1_000_000.0
    except Exception:
        return ""
    bands = [
        ("160M", 1.8, 2.0),
        ("80M", 3.5, 4.0),
        ("60M", 5.0, 5.5),
        ("40M", 7.0, 7.3),
        ("30M", 10.1, 10.15),
        ("20M", 14.0, 14.35),
        ("17M", 18.068, 18.168),
        ("15M", 21.0, 21.45),
        ("12M", 24.89, 24.99),
        ("10M", 28.0, 29.7),
        ("6M", 50.0, 54.0),
        ("2M", 144.0, 148.0),
    ]
    for name, lo, hi in bands:
        if lo <= mhz <= hi:
            return name
    return ""


def _clean_call(val: str) -> str:
    return (val or "").strip().upper()


def _is_callsign(val: str) -> bool:
    return bool(CALLSIGN_RE.fullmatch(_clean_call(val)))


def _ensure_local_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_ingest_state (
            table_name TEXT PRIMARY KEY,
            last_id INTEGER DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_messages (
            id INTEGER,
            guid TEXT,
            source TEXT,
            msg_type TEXT,
            from_call TEXT,
            to_call TEXT,
            subject TEXT,
            body TEXT,
            ts REAL,
            band TEXT,
            freq_hz REAL,
            snr REAL,
            read_status INTEGER,
            folder TEXT,
            file_path TEXT,
            vmail_guid TEXT,
            is_deleted INTEGER DEFAULT 0,
            flag_state INTEGER DEFAULT 0,
            PRIMARY KEY (source, id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_callsign_stats (
            callsign TEXT PRIMARY KEY,
            last_seen_ts REAL,
            last_band TEXT,
            last_freq_hz REAL,
            last_snr REAL,
            last_source TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_links (
            ts REAL,
            origin TEXT,
            destination TEXT,
            snr REAL,
            band TEXT,
            freq_hz REAL,
            source TEXT
        )
        """
    )
    cur.execute("PRAGMA table_info(varac_messages)")
    cols = {row[1] for row in cur.fetchall()}
    if "flag_state" not in cols:
        cur.execute("ALTER TABLE varac_messages ADD COLUMN flag_state INTEGER DEFAULT 0")
    conn.commit()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _get_last_id(conn: sqlite3.Connection, table: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT last_id FROM varac_ingest_state WHERE table_name=?", (table,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _set_last_id(conn: sqlite3.Connection, table: str, last_id: int) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO varac_ingest_state (table_name, last_id)
        VALUES (?, ?)
        ON CONFLICT(table_name) DO UPDATE SET last_id=excluded.last_id
        """,
        (table, int(last_id)),
    )


def _update_stats(
    stats: Dict[str, Dict],
    callsign: str,
    ts_val: float,
    band: str,
    freq_hz: Optional[float],
    snr: Optional[float],
    source: str,
) -> None:
    cs = _clean_call(callsign)
    if not cs:
        return
    entry = stats.get(cs)
    if entry is None or ts_val > entry.get("last_seen_ts", 0):
        stats[cs] = {
            "last_seen_ts": ts_val,
            "last_band": band or "",
            "last_freq_hz": float(freq_hz) if freq_hz is not None else None,
            "last_snr": float(snr) if snr is not None else None,
            "last_source": source,
        }


def ingest_varac(settings, *, force: bool = False) -> bool:
    varac_path = (settings.get("varac_db_path", "") or "").strip()
    if not varac_path:
        return False
    varac_db = Path(varac_path)
    if not varac_db.exists():
        return False

    local_db = _local_db_path()
    local_db.parent.mkdir(parents=True, exist_ok=True)
    local_conn = sqlite3.connect(local_db)
    _ensure_local_tables(local_conn)

    try:
        uri = f"file:{varac_db.as_posix()}?mode=ro"
        varac_conn = sqlite3.connect(uri, uri=True)
    except Exception as e:
        log.debug("VarAC ingest: failed to open %s: %s", varac_db, e)
        local_conn.close()
        return False

    my_call = (settings.get("operator_callsign", "") or "").strip().upper()
    stats: Dict[str, Dict] = {}

    try:
        cur_local = local_conn.cursor()
        cur_varac = varac_conn.cursor()

        def fetch_rows(table: str, cols: str) -> Iterable[tuple]:
            if not _table_exists(varac_conn, table):
                return []
            last_id = 0 if force else _get_last_id(local_conn, table)
            try:
                cur_varac.execute(
                    f"SELECT {cols} FROM {table} WHERE id > ? ORDER BY id ASC",
                    (last_id,),
                )
                rows = cur_varac.fetchall()
            except Exception:
                rows = []
            if rows:
                last_seen = rows[-1][0] if rows[-1] else last_id
                try:
                    _set_last_id(local_conn, table, int(last_seen))
                except Exception:
                    pass
            return rows

        # QSO messages
        for r in fetch_rows(
            "qso",
            "id, guid, callsign, my_callsign, starttime, endtime, frequency, band, snr_received, snr_sent, is_deleted",
        ):
            (
                rid,
                guid,
                callsign,
                my_cs,
                starttime,
                endtime,
                frequency,
                band,
                snr_received,
                _snr_sent,
                is_deleted,
            ) = r
            callsign = _clean_call(callsign)
            if not callsign:
                continue
            ts_val = _parse_dt(endtime) or _parse_dt(starttime)
            freq_hz = float(frequency) if frequency not in (None, "") else None
            band_val = (band or "").strip().upper() or _hz_to_band(freq_hz)
            try:
                snr_val = float(snr_received) if snr_received not in (None, "") else None
            except Exception:
                snr_val = None
            _update_stats(stats, callsign, ts_val, band_val, freq_hz, snr_val, "qso")
            freq_disp = f"{float(freq_hz) / 1_000_000.0:.3f}" if freq_hz else ""
            msg_body = (
                f"QSO with {callsign}\n"
                f"Band: {band_val}\n"
                f"Frequency: {freq_disp}\n"
                f"SNR: {snr_val if snr_val is not None else ''}\n"
                f"Start: {starttime or ''}\n"
                f"End: {endtime or ''}"
            )
            cur_local.execute(
                """
                INSERT OR REPLACE INTO varac_messages
                    (id, guid, source, msg_type, from_call, to_call, subject, body, ts, band, freq_hz, snr, read_status, folder, file_path, vmail_guid, is_deleted, flag_state)
                VALUES
                    (?, ?, 'qso', 'QSO', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rid),
                    guid or "",
                    callsign,
                    (my_cs or my_call or "").strip().upper(),
                    f"QSO {band_val}".strip(),
                    msg_body,
                    ts_val,
                    band_val,
                    freq_hz,
                    snr_val,
                    1,
                    "",
                    "",
                    "",
                    int(bool(is_deleted)) if is_deleted is not None else 0,
                    0,
                ),
            )
            other = (my_cs or my_call or "").strip().upper()
            if other and _is_callsign(other) and _is_callsign(callsign):
                cur_local.execute(
                    """
                    INSERT INTO varac_links (ts, origin, destination, snr, band, freq_hz, source)
                    VALUES (?, ?, ?, ?, ?, ?, 'qso')
                    """,
                    (ts_val, other, callsign, snr_val, band_val, freq_hz),
                )

        # VMAIL messages
        for r in fetch_rows(
            "vmail",
            "id, guid, creation_time, sent_time, received_time, folder_id, vmail_to, vmail_from, delivery_band, delivery_snr, subject, msg, read_status, is_deleted, frequency, vmail_via",
        ):
            (
                rid,
                guid,
                creation_time,
                sent_time,
                received_time,
                folder_id,
                vmail_to,
                vmail_from,
                delivery_band,
                delivery_snr,
                subject,
                msg,
                read_status,
                is_deleted,
                frequency,
                _via,
            ) = r
            from_call = _clean_call(vmail_from)
            to_call = _clean_call(vmail_to)
            ts_val = _parse_dt(received_time) or _parse_dt(sent_time) or _parse_dt(creation_time)
            freq_hz = float(frequency) if frequency not in (None, "") else None
            band_val = (delivery_band or "").strip().upper() or _hz_to_band(freq_hz)
            try:
                snr_val = float(delivery_snr) if delivery_snr not in (None, "") else None
            except Exception:
                snr_val = None
            if from_call:
                _update_stats(stats, from_call, ts_val, band_val, freq_hz, snr_val, "vmail")
            cur_local.execute(
                """
                INSERT OR REPLACE INTO varac_messages
                    (id, guid, source, msg_type, from_call, to_call, subject, body, ts, band, freq_hz, snr, read_status, folder, file_path, vmail_guid, is_deleted, flag_state)
                VALUES
                    (?, ?, 'vmail', 'VMAIL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rid),
                    guid or "",
                    from_call,
                    to_call,
                    (subject or "").strip(),
                    (msg or "").strip(),
                    ts_val,
                    band_val,
                    freq_hz,
                    snr_val,
                    int(bool(read_status)) if read_status is not None else 0,
                    str(folder_id or ""),
                    "",
                    guid or "",
                    int(bool(is_deleted)) if is_deleted is not None else 0,
                    0,
                ),
            )

        # Broadcast messages
        for r in fetch_rows(
            "broadcast",
            "id, guid, broadcast_time, frequency, band, from_callsign, to_callsign, broadcast_message, snr, is_deleted",
        ):
            (
                rid,
                guid,
                broadcast_time,
                frequency,
                band,
                from_callsign,
                to_callsign,
                broadcast_message,
                snr,
                is_deleted,
            ) = r
            from_call = _clean_call(from_callsign)
            to_call = _clean_call(to_callsign)
            if my_call and from_call == my_call:
                continue
            ts_val = _parse_dt(broadcast_time)
            freq_hz = float(frequency) if frequency not in (None, "") else None
            band_val = (band or "").strip().upper() or _hz_to_band(freq_hz)
            try:
                snr_val = float(snr) if snr not in (None, "") else None
            except Exception:
                snr_val = None
            if from_call:
                _update_stats(stats, from_call, ts_val, band_val, freq_hz, snr_val, "broadcast")
            cur_local.execute(
                """
                INSERT OR REPLACE INTO varac_messages
                    (id, guid, source, msg_type, from_call, to_call, subject, body, ts, band, freq_hz, snr, read_status, folder, file_path, vmail_guid, is_deleted, flag_state)
                VALUES
                    (?, ?, 'broadcast', 'BROADCAST', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rid),
                    guid or "",
                    from_call,
                    to_call,
                    "Broadcast",
                    (broadcast_message or "").strip(),
                    ts_val,
                    band_val,
                    freq_hz,
                    snr_val,
                    1,
                    "",
                    "",
                    "",
                    int(bool(is_deleted)) if is_deleted is not None else 0,
                    0,
                ),
            )

        # CQ/Beacon stats
        for r in fetch_rows(
            "cqframe",
            "id, guid, cqframe_time, frequency, band, from_callsign, snr, is_deleted",
        ):
            (
                _rid,
                _guid,
                cqframe_time,
                frequency,
                band,
                from_callsign,
                snr,
                is_deleted,
            ) = r
            if is_deleted:
                continue
            from_call = _clean_call(from_callsign)
            ts_val = _parse_dt(cqframe_time)
            freq_hz = float(frequency) if frequency not in (None, "") else None
            band_val = (band or "").strip().upper() or _hz_to_band(freq_hz)
            try:
                snr_val = float(snr) if snr not in (None, "") else None
            except Exception:
                snr_val = None
            if from_call:
                _update_stats(stats, from_call, ts_val, band_val, freq_hz, snr_val, "cqframe")

        # Alert stats
        for r in fetch_rows(
            "alert",
            "id, guid, alert_time, frequency, from_callsign, to_callsign, is_deleted",
        ):
            (
                _rid,
                _guid,
                alert_time,
                frequency,
                from_callsign,
                _to_callsign,
                is_deleted,
            ) = r
            if is_deleted:
                continue
            from_call = _clean_call(from_callsign)
            ts_val = _parse_dt(alert_time)
            freq_hz = float(frequency) if frequency not in (None, "") else None
            band_val = _hz_to_band(freq_hz)
            if from_call:
                _update_stats(stats, from_call, ts_val, band_val, freq_hz, None, "alert")

        # Datastream stats
        for r in fetch_rows(
            "datastream",
            "id, guid, creation_time, callsign, entry, is_deleted",
        ):
            (
                _rid,
                _guid,
                creation_time,
                callsign,
                _entry,
                is_deleted,
            ) = r
            if is_deleted:
                continue
            cs = _clean_call(callsign)
            ts_val = _parse_dt(creation_time)
            if cs:
                _update_stats(stats, cs, ts_val, "", None, None, "datastream")

        # Upsert stats to local db
        for cs, data in stats.items():
            cur_local.execute(
                """
                INSERT INTO varac_callsign_stats (callsign, last_seen_ts, last_band, last_freq_hz, last_snr, last_source)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(callsign) DO UPDATE SET
                    last_seen_ts=excluded.last_seen_ts,
                    last_band=excluded.last_band,
                    last_freq_hz=excluded.last_freq_hz,
                    last_snr=excluded.last_snr,
                    last_source=excluded.last_source
                """,
                (
                    cs,
                    float(data.get("last_seen_ts") or 0.0),
                    data.get("last_band") or "",
                    data.get("last_freq_hz"),
                    data.get("last_snr"),
                    data.get("last_source") or "",
                ),
            )

        # Update operator_checkins last_seen_utc
        cur_local.execute("PRAGMA table_info(operator_checkins)")
        cols = {row[1] for row in cur_local.fetchall()}
        if "last_seen_utc" in cols:
            for cs, data in stats.items():
                ts_val = float(data.get("last_seen_ts") or 0.0)
                if not ts_val:
                    continue
                date_txt = datetime.datetime.utcfromtimestamp(ts_val).strftime("%Y-%m-%d")
                cur_local.execute(
                    """
                    INSERT INTO operator_checkins (callsign, last_seen_utc, checkin_count, trusted)
                    VALUES (?, ?, COALESCE((SELECT checkin_count FROM operator_checkins WHERE callsign=?), 0), COALESCE((SELECT trusted FROM operator_checkins WHERE callsign=?), 0))
                    ON CONFLICT(callsign) DO UPDATE SET
                        last_seen_utc=CASE
                            WHEN operator_checkins.last_seen_utc IS NULL OR operator_checkins.last_seen_utc='' THEN excluded.last_seen_utc
                            WHEN operator_checkins.last_seen_utc < excluded.last_seen_utc THEN excluded.last_seen_utc
                            ELSE operator_checkins.last_seen_utc
                        END
                    """,
                    (cs, date_txt, cs, cs),
                )

        local_conn.commit()
    except Exception as e:
        log.debug("VarAC ingest failed: %s", e)
    finally:
        try:
            varac_conn.close()
        except Exception:
            pass
        local_conn.close()

    return True
