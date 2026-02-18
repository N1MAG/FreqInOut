from __future__ import annotations

import datetime
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, Optional

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log

CALLSIGN_RE = re.compile(r"^[A-Z0-9/]{3,10}$")
TRAILING_CALL_NOISE_RE = re.compile(r"[^A-Z0-9/]+$")
PORTABLE_SUFFIX_RE = re.compile(r"/(P|M|MM|QRP|SOTA|ROVER|[A-Z0-9]{1,4})$")
_INGEST_LOCK = threading.Lock()
_LAST_RUN_MONO = 0.0
_MIN_INGEST_INTERVAL_SECONDS = 8.0

VMAIL_FOLDER_FALLBACK = {
    1: "INBOX",
    2: "SENT",
    3: "OUTBOX",
    4: "PARKING",
}


def _local_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout_nets.db"


def _resolve_varac_db_path(settings) -> Optional[Path]:
    raw_db = (settings.get("varac_db_path", "") or "").strip()
    raw_install = (settings.get("varac_path", "") or "").strip()
    for raw in (raw_db, raw_install):
        if not raw:
            continue
        try:
            p = Path(raw)
            if p.is_dir():
                return p / "VarAC.db"
            if p.is_file():
                return p
        except Exception:
            continue
    return None


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
    cs = (val or "").strip().upper()
    if not cs:
        return ""
    cs = TRAILING_CALL_NOISE_RE.sub("", cs)
    if not cs:
        return ""
    return PORTABLE_SUFFIX_RE.sub("", cs)


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
        CREATE TABLE IF NOT EXISTS varac_sync_status (
            run_started_ts REAL PRIMARY KEY,
            run_finished_ts REAL,
            varac_db_path TEXT,
            success INTEGER DEFAULT 0,
            rows_scanned INTEGER DEFAULT 0,
            rows_written INTEGER DEFAULT 0,
            error_text TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_sync_table_counts (
            run_started_ts REAL,
            table_name TEXT,
            rows_scanned INTEGER DEFAULT 0,
            rows_written INTEGER DEFAULT 0,
            watermark_id INTEGER DEFAULT 0,
            PRIMARY KEY (run_started_ts, table_name)
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
            folder_label TEXT,
            urgent INTEGER DEFAULT 0,
            has_attachment INTEGER DEFAULT 0,
            via_callsign TEXT,
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
        CREATE TABLE IF NOT EXISTS varac_callsign_traits (
            callsign TEXT PRIMARY KEY,
            is_emcomm INTEGER DEFAULT 0,
            bbs_seen INTEGER DEFAULT 0,
            alert_count INTEGER DEFAULT 0,
            last_alert_ts REAL,
            last_updated_ts REAL
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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_vmail_folders (
            folder_id INTEGER PRIMARY KEY,
            folder TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_relay_notifications (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            relay_ts REAL,
            from_call TEXT,
            freq_hz REAL,
            urgent INTEGER DEFAULT 0,
            is_deleted INTEGER DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_broadcast_events (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            ts REAL,
            freq_hz REAL,
            band TEXT,
            from_call TEXT,
            to_call TEXT,
            via_callsign TEXT,
            message TEXT,
            snr REAL,
            instance_id INTEGER,
            is_deleted INTEGER DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_cqframe_type_lut (
            cqframe_type_id INTEGER PRIMARY KEY,
            cqframe_type TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_cqframe_events (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            ts REAL,
            cqframe_type_id INTEGER,
            cqframe_type TEXT,
            freq_hz REAL,
            band TEXT,
            bandwidth TEXT,
            from_call TEXT,
            snr REAL,
            slot INTEGER,
            data TEXT,
            locator TEXT,
            is_emcomm INTEGER DEFAULT 0,
            instance_id INTEGER,
            is_deleted INTEGER DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS varac_qso_snr_reports (
            id INTEGER PRIMARY KEY,
            guid TEXT,
            qso_guid TEXT,
            snr_direction TEXT,
            snr REAL,
            ts REAL
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_links_ts
        ON varac_links(ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_callsign_stats_last_seen
        ON varac_callsign_stats(last_seen_ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_callsign_traits_updated
        ON varac_callsign_traits(last_updated_ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_messages_ts
        ON varac_messages(ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_messages_source_ts
        ON varac_messages(source, ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_broadcast_ts
        ON varac_broadcast_events(ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_broadcast_band_ts
        ON varac_broadcast_events(band, ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_broadcast_from_ts
        ON varac_broadcast_events(from_call, ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_cqframe_ts
        ON varac_cqframe_events(ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_cqframe_band_ts
        ON varac_cqframe_events(band, ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_cqframe_from_ts
        ON varac_cqframe_events(from_call, ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_qso_snr_ts
        ON varac_qso_snr_reports(ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_qso_snr_qso
        ON varac_qso_snr_reports(qso_guid, ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_relay_ts
        ON varac_relay_notifications(relay_ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_sync_status_finished
        ON varac_sync_status(run_finished_ts)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_sync_counts_table
        ON varac_sync_table_counts(table_name, run_started_ts)
        """
    )
    cur.execute("PRAGMA table_info(varac_messages)")
    cols = {row[1] for row in cur.fetchall()}
    if "flag_state" not in cols:
        cur.execute("ALTER TABLE varac_messages ADD COLUMN flag_state INTEGER DEFAULT 0")
    if "folder_label" not in cols:
        cur.execute("ALTER TABLE varac_messages ADD COLUMN folder_label TEXT")
    if "urgent" not in cols:
        cur.execute("ALTER TABLE varac_messages ADD COLUMN urgent INTEGER DEFAULT 0")
    if "has_attachment" not in cols:
        cur.execute("ALTER TABLE varac_messages ADD COLUMN has_attachment INTEGER DEFAULT 0")
    if "via_callsign" not in cols:
        cur.execute("ALTER TABLE varac_messages ADD COLUMN via_callsign TEXT")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_varac_messages_folder_label
        ON varac_messages(folder_label)
        """
    )
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


def _record_sync_status(
    conn: sqlite3.Connection,
    *,
    run_started_ts: float,
    run_finished_ts: float,
    varac_db_path: str,
    success: bool,
    rows_scanned: int,
    rows_written: int,
    table_counts: Dict[str, Dict[str, int]],
    error_text: str = "",
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO varac_sync_status
            (run_started_ts, run_finished_ts, varac_db_path, success, rows_scanned, rows_written, error_text)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            float(run_started_ts),
            float(run_finished_ts),
            str(varac_db_path or ""),
            1 if success else 0,
            int(rows_scanned),
            int(rows_written),
            (error_text or "").strip()[:500],
        ),
    )
    for table_name, counts in table_counts.items():
        cur.execute(
            """
            INSERT OR REPLACE INTO varac_sync_table_counts
                (run_started_ts, table_name, rows_scanned, rows_written, watermark_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                float(run_started_ts),
                table_name,
                int(counts.get("rows_scanned", 0)),
                int(counts.get("rows_written", 0)),
                int(counts.get("watermark_id", 0)),
            ),
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


def _update_traits(
    traits: Dict[str, Dict[str, float | int]],
    callsign: str,
    *,
    is_emcomm: bool = False,
    bbs_seen: bool = False,
    alert_delta: int = 0,
    alert_ts: float = 0.0,
) -> None:
    cs = _clean_call(callsign)
    if not cs:
        return
    entry = traits.setdefault(
        cs,
        {
            "is_emcomm": 0,
            "bbs_seen": 0,
            "alert_count": 0,
            "last_alert_ts": 0.0,
            "last_updated_ts": 0.0,
        },
    )
    if is_emcomm:
        entry["is_emcomm"] = 1
    if bbs_seen:
        entry["bbs_seen"] = 1
    if alert_delta:
        entry["alert_count"] = int(entry.get("alert_count", 0)) + int(alert_delta)
    if alert_ts > float(entry.get("last_alert_ts", 0.0) or 0.0):
        entry["last_alert_ts"] = float(alert_ts)
    entry["last_updated_ts"] = time.time()


def ingest_varac(settings, *, force: bool = False) -> bool:
    global _LAST_RUN_MONO

    varac_db = _resolve_varac_db_path(settings)
    if not varac_db:
        return False
    if not varac_db.exists():
        return False
    now_mono = time.monotonic()
    if not force and (now_mono - float(_LAST_RUN_MONO or 0.0) < _MIN_INGEST_INTERVAL_SECONDS):
        return True
    if not _INGEST_LOCK.acquire(blocking=False):
        return False

    run_started_ts = time.time()
    table_counts: Dict[str, Dict[str, int]] = {}
    success = False
    error_text = ""
    local_conn: Optional[sqlite3.Connection] = None
    varac_conn: Optional[sqlite3.Connection] = None

    def _note_table(table_name: str, *, scanned: int = 0, written: int = 0, watermark: Optional[int] = None) -> None:
        entry = table_counts.setdefault(
            table_name,
            {"rows_scanned": 0, "rows_written": 0, "watermark_id": 0},
        )
        entry["rows_scanned"] = int(entry.get("rows_scanned", 0)) + int(scanned)
        entry["rows_written"] = int(entry.get("rows_written", 0)) + int(written)
        if watermark is not None:
            try:
                wm = int(watermark)
            except Exception:
                wm = 0
            entry["watermark_id"] = max(int(entry.get("watermark_id", 0)), wm)

    def _to_float(val) -> Optional[float]:
        if val in (None, ""):
            return None
        try:
            return float(val)
        except Exception:
            return None

    def _to_int_flag(val) -> int:
        try:
            return 1 if bool(int(val)) else 0
        except Exception:
            return 1 if bool(val) else 0

    try:
        local_db = _local_db_path()
        local_db.parent.mkdir(parents=True, exist_ok=True)
        local_conn = sqlite3.connect(local_db)
        _ensure_local_tables(local_conn)

        try:
            uri = f"file:{varac_db.as_posix()}?mode=ro"
            varac_conn = sqlite3.connect(uri, uri=True)
        except Exception as e:
            error_text = f"failed opening VarAC DB: {e}"
            log.debug("VarAC ingest: failed to open %s: %s", varac_db, e)
            return False

        cur_local = local_conn.cursor()
        cur_varac = varac_conn.cursor()
        my_call = (settings.get("operator_callsign", "") or "").strip().upper()
        stats: Dict[str, Dict] = {}
        traits: Dict[str, Dict[str, float | int]] = {}
        folder_lut: Dict[int, str] = dict(VMAIL_FOLDER_FALLBACK)
        cq_type_lut: Dict[int, str] = {}

        # Start with any previously mirrored folder/type rows so labels remain stable.
        try:
            cur_local.execute("SELECT folder_id, folder FROM varac_vmail_folders")
            for rid, folder in cur_local.fetchall():
                try:
                    folder_lut[int(rid)] = str(folder or "").strip().upper()
                except Exception:
                    continue
        except Exception:
            pass
        try:
            cur_local.execute("SELECT cqframe_type_id, cqframe_type FROM varac_cqframe_type_lut")
            for rid, ctype in cur_local.fetchall():
                try:
                    cq_type_lut[int(rid)] = str(ctype or "").strip().upper()
                except Exception:
                    continue
        except Exception:
            pass

        def fetch_rows(
            table: str,
            cols: str,
            *,
            fallbacks: Optional[Iterable[str]] = None,
        ) -> tuple[list[tuple], str]:
            if not _table_exists(varac_conn, table):
                _note_table(table)
                return [], cols
            last_id = 0 if force else _get_last_id(local_conn, table)
            rows: list[tuple] = []
            used_cols = cols
            candidates = [cols]
            if fallbacks:
                candidates.extend([c for c in fallbacks if c and c != cols])
            for candidate in candidates:
                try:
                    cur_varac.execute(
                        f"SELECT {candidate} FROM {table} WHERE id > ? ORDER BY id ASC",
                        (last_id,),
                    )
                    rows = cur_varac.fetchall()
                    used_cols = candidate
                    break
                except Exception:
                    continue
            last_seen = int(last_id)
            if rows:
                try:
                    last_seen = int(rows[-1][0])
                    _set_last_id(local_conn, table, last_seen)
                except Exception:
                    pass
            _note_table(table, scanned=len(rows), watermark=last_seen)
            return rows, used_cols

        def read_all(table: str, cols: str) -> list[tuple]:
            if not _table_exists(varac_conn, table):
                _note_table(table)
                return []
            try:
                cur_varac.execute(f"SELECT {cols} FROM {table}")
                rows = cur_varac.fetchall()
            except Exception:
                rows = []
            _note_table(table, scanned=len(rows))
            return rows

        # Lookup: VMAIL folders
        for row in read_all("vmail_folder", "folder_id, folder"):
            try:
                folder_id = int(row[0])
            except Exception:
                continue
            folder_txt = str(row[1] or "").strip().upper()
            if folder_txt:
                folder_lut[folder_id] = folder_txt
            cur_local.execute(
                "INSERT OR REPLACE INTO varac_vmail_folders(folder_id, folder) VALUES (?, ?)",
                (folder_id, folder_txt),
            )
            _note_table("vmail_folder", written=1)

        # Lookup: CQ frame type
        for row in read_all("cqframe_type", "cqframe_type_id, cqframe_type"):
            try:
                type_id = int(row[0])
            except Exception:
                continue
            type_txt = str(row[1] or "").strip().upper()
            cq_type_lut[type_id] = type_txt
            cur_local.execute(
                "INSERT OR REPLACE INTO varac_cqframe_type_lut(cqframe_type_id, cqframe_type) VALUES (?, ?)",
                (type_id, type_txt),
            )
            _note_table("cqframe_type", written=1)

        # QSO messages
        qso_rows, _ = fetch_rows(
            "qso",
            "id, guid, callsign, my_callsign, starttime, endtime, frequency, band, snr_received, snr_sent, is_deleted",
        )
        for r in qso_rows:
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
            _note_table("qso", written=1)
            other = (my_cs or my_call or "").strip().upper()
            if other and _is_callsign(other) and _is_callsign(callsign):
                cur_local.execute(
                    """
                    INSERT INTO varac_links (ts, origin, destination, snr, band, freq_hz, source)
                    VALUES (?, ?, ?, ?, ?, ?, 'qso')
                    """,
                    (ts_val, other, callsign, snr_val, band_val, freq_hz),
                )
                _note_table("qso", written=1)

        # VMAIL messages
        vmail_rows, vmail_cols = fetch_rows(
            "vmail",
            "id, guid, creation_time, sent_time, received_time, folder_id, vmail_to, vmail_from, delivery_band, delivery_snr, subject, msg, read_status, is_deleted, frequency, vmail_via, urgent, has_attachment",
            fallbacks=[
                "id, guid, creation_time, sent_time, received_time, folder_id, vmail_to, vmail_from, delivery_band, delivery_snr, subject, msg, read_status, is_deleted, frequency, vmail_via",
            ],
        )
        vmail_has_badge_cols = "urgent" in vmail_cols and "has_attachment" in vmail_cols
        for r in vmail_rows:
            rid = r[0]
            guid = r[1]
            creation_time = r[2]
            sent_time = r[3]
            received_time = r[4]
            folder_id = r[5]
            vmail_to = r[6]
            vmail_from = r[7]
            delivery_band = r[8]
            delivery_snr = r[9]
            subject = r[10]
            msg = r[11]
            read_status = r[12]
            is_deleted = r[13]
            frequency = r[14]
            via = r[15] if len(r) > 15 else ""
            urgent = r[16] if (vmail_has_badge_cols and len(r) > 16) else 0
            has_attachment = r[17] if (vmail_has_badge_cols and len(r) > 17) else 0
            from_call = _clean_call(vmail_from)
            to_call = _clean_call(vmail_to)
            ts_val = _parse_dt(received_time) or _parse_dt(sent_time) or _parse_dt(creation_time)
            freq_hz = float(frequency) if frequency not in (None, "") else None
            band_val = (delivery_band or "").strip().upper() or _hz_to_band(freq_hz)
            try:
                folder_id_int = int(folder_id)
            except Exception:
                folder_id_int = 0
            folder_label = folder_lut.get(folder_id_int) or str(folder_id or "")
            try:
                snr_val = float(delivery_snr) if delivery_snr not in (None, "") else None
            except Exception:
                snr_val = None
            if from_call:
                _update_stats(stats, from_call, ts_val, band_val, freq_hz, snr_val, "vmail")
            cur_local.execute(
                """
                INSERT OR REPLACE INTO varac_messages
                    (id, guid, source, msg_type, from_call, to_call, subject, body, ts, band, freq_hz, snr, read_status, folder, file_path, vmail_guid, is_deleted, flag_state, folder_label, urgent, has_attachment, via_callsign)
                VALUES
                    (?, ?, 'vmail', 'VMAIL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    folder_label,
                    _to_int_flag(urgent),
                    _to_int_flag(has_attachment),
                    _clean_call(via),
                ),
            )
            _note_table("vmail", written=1)

        # Broadcast messages
        broadcast_rows, broadcast_cols = fetch_rows(
            "broadcast",
            "id, guid, broadcast_time, frequency, band, from_callsign, to_callsign, via_callsign, broadcast_message, snr, instance_id, is_deleted",
            fallbacks=[
                "id, guid, broadcast_time, frequency, band, from_callsign, to_callsign, broadcast_message, snr, is_deleted",
            ],
        )
        broadcast_has_via = "via_callsign" in broadcast_cols
        broadcast_has_instance = "instance_id" in broadcast_cols
        for r in broadcast_rows:
            rid = r[0]
            guid = r[1]
            broadcast_time = r[2]
            frequency = r[3]
            band = r[4]
            from_callsign = r[5]
            to_callsign = r[6]
            if broadcast_has_via:
                via_callsign = r[7]
                broadcast_message = r[8]
                snr = r[9]
                instance_id = r[10] if broadcast_has_instance and len(r) > 10 else None
                is_deleted = r[11] if len(r) > 11 else 0
            else:
                via_callsign = ""
                broadcast_message = r[7]
                snr = r[8]
                instance_id = None
                is_deleted = r[9] if len(r) > 9 else 0
            from_call = _clean_call(from_callsign)
            to_call = _clean_call(to_callsign)
            if my_call and from_call == my_call:
                # Keep outgoing traffic in mirrored events even if hidden from message list.
                include_message = False
            else:
                include_message = True
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
                INSERT OR REPLACE INTO varac_broadcast_events
                    (id, guid, ts, freq_hz, band, from_call, to_call, via_callsign, message, snr, instance_id, is_deleted)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rid),
                    guid or "",
                    ts_val,
                    freq_hz,
                    band_val,
                    from_call,
                    to_call,
                    _clean_call(via_callsign),
                    (broadcast_message or "").strip(),
                    snr_val,
                    int(instance_id) if instance_id not in (None, "") else None,
                    _to_int_flag(is_deleted),
                ),
            )
            _note_table("broadcast", written=1)
            if include_message:
                cur_local.execute(
                    """
                    INSERT OR REPLACE INTO varac_messages
                        (id, guid, source, msg_type, from_call, to_call, subject, body, ts, band, freq_hz, snr, read_status, folder, file_path, vmail_guid, is_deleted, flag_state, via_callsign)
                    VALUES
                        (?, ?, 'broadcast', 'BROADCAST', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        _clean_call(via_callsign),
                    ),
                )
                _note_table("broadcast", written=1)

        # CQ/Beacon stats
        cq_rows, cq_cols = fetch_rows(
            "cqframe",
            "id, guid, cqframe_time, cqframe_type_id, frequency, band, bandwidth, from_callsign, snr, slot, data, locator, is_emcomm, instance_id, is_deleted",
            fallbacks=[
                "id, guid, cqframe_time, frequency, band, from_callsign, snr, is_deleted",
            ],
        )
        cq_has_details = "cqframe_type_id" in cq_cols and "is_emcomm" in cq_cols
        for r in cq_rows:
            if cq_has_details:
                rid = r[0]
                guid = r[1]
                cqframe_time = r[2]
                cqframe_type_id = int(r[3]) if r[3] not in (None, "") else 0
                frequency = r[4]
                band = r[5]
                bandwidth = r[6]
                from_callsign = r[7]
                snr = r[8]
                slot = r[9]
                data = r[10]
                locator = r[11]
                is_emcomm = _to_int_flag(r[12])
                instance_id = r[13]
                is_deleted = r[14]
            else:
                rid = r[0]
                guid = r[1]
                cqframe_time = r[2]
                cqframe_type_id = 0
                frequency = r[3]
                band = r[4]
                bandwidth = ""
                from_callsign = r[5]
                snr = r[6]
                slot = None
                data = ""
                locator = ""
                is_emcomm = 0
                instance_id = None
                is_deleted = r[7] if len(r) > 7 else 0
            if is_deleted:
                continue
            from_call = _clean_call(from_callsign)
            ts_val = _parse_dt(cqframe_time)
            freq_hz = float(frequency) if frequency not in (None, "") else None
            band_val = (band or "").strip().upper() or _hz_to_band(freq_hz)
            cqframe_type = cq_type_lut.get(cqframe_type_id, "")
            try:
                snr_val = float(snr) if snr not in (None, "") else None
            except Exception:
                snr_val = None
            if from_call:
                _update_stats(stats, from_call, ts_val, band_val, freq_hz, snr_val, "cqframe")
                if is_emcomm:
                    _update_traits(traits, from_call, is_emcomm=True)
            cur_local.execute(
                """
                INSERT OR REPLACE INTO varac_cqframe_events
                    (id, guid, ts, cqframe_type_id, cqframe_type, freq_hz, band, bandwidth, from_call, snr, slot, data, locator, is_emcomm, instance_id, is_deleted)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rid),
                    guid or "",
                    ts_val,
                    int(cqframe_type_id or 0),
                    cqframe_type,
                    freq_hz,
                    band_val,
                    str(bandwidth or ""),
                    from_call,
                    snr_val,
                    int(slot) if slot not in (None, "") else None,
                    str(data or ""),
                    str(locator or ""),
                    int(is_emcomm),
                    int(instance_id) if instance_id not in (None, "") else None,
                    0,
                ),
            )
            _note_table("cqframe", written=1)

        # QSO SNR report history for propagation confidence/trends.
        for r in fetch_rows(
            "qso_snr_report",
            "id, guid, qso_guid, snr_direction, snr, creation_time",
        )[0]:
            rid, guid, qso_guid, snr_direction, snr_val_raw, creation_time = r
            snr_val = _to_float(snr_val_raw)
            ts_val = _parse_dt(creation_time)
            cur_local.execute(
                """
                INSERT OR REPLACE INTO varac_qso_snr_reports
                    (id, guid, qso_guid, snr_direction, snr, ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rid),
                    guid or "",
                    qso_guid or "",
                    str(snr_direction or ""),
                    snr_val,
                    ts_val,
                ),
            )
            _note_table("qso_snr_report", written=1)

        # Relay notifications (supports relay inbox workflows).
        relay_rows, _ = fetch_rows(
            "vmail_relay_notification",
            "id, guid, relay_notification_time, frequency, from_callsign, is_deleted, urgent",
        )
        for r in relay_rows:
            rid, guid, relay_notification_time, frequency, from_callsign, is_deleted, urgent = r
            ts_val = _parse_dt(relay_notification_time)
            freq_hz = _to_float(frequency)
            from_call = _clean_call(from_callsign)
            cur_local.execute(
                """
                INSERT OR REPLACE INTO varac_relay_notifications
                    (id, guid, relay_ts, from_call, freq_hz, urgent, is_deleted)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rid),
                    guid or "",
                    ts_val,
                    from_call,
                    freq_hz,
                    _to_int_flag(urgent),
                    _to_int_flag(is_deleted),
                ),
            )
            _note_table("vmail_relay_notification", written=1)

        # Alert stats
        for r in fetch_rows(
            "alert",
            "id, guid, alert_time, frequency, from_callsign, to_callsign, is_deleted",
        )[0]:
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
                _update_traits(traits, from_call, alert_delta=1, alert_ts=ts_val)

        # Datastream stats
        for r in fetch_rows(
            "datastream",
            "id, guid, creation_time, callsign, entry, is_deleted",
        )[0]:
            (
                _rid,
                _guid,
                creation_time,
                callsign,
                entry,
                is_deleted,
            ) = r
            if is_deleted:
                continue
            cs = _clean_call(callsign)
            ts_val = _parse_dt(creation_time)
            if cs:
                _update_stats(stats, cs, ts_val, "", None, None, "datastream")
                entry_txt = str(entry or "").upper()
                if "BBS" in entry_txt or "MAILBOX" in entry_txt:
                    _update_traits(traits, cs, bbs_seen=True)

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
            _note_table("varac_callsign_stats", written=1)

        # Upsert call traits used for emcomm/BBS/alert badges in later UI phases.
        for cs, data in traits.items():
            cur_local.execute(
                """
                INSERT INTO varac_callsign_traits (callsign, is_emcomm, bbs_seen, alert_count, last_alert_ts, last_updated_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(callsign) DO UPDATE SET
                    is_emcomm=MAX(varac_callsign_traits.is_emcomm, excluded.is_emcomm),
                    bbs_seen=MAX(varac_callsign_traits.bbs_seen, excluded.bbs_seen),
                    alert_count=MAX(varac_callsign_traits.alert_count, excluded.alert_count),
                    last_alert_ts=CASE
                        WHEN COALESCE(varac_callsign_traits.last_alert_ts, 0) >= COALESCE(excluded.last_alert_ts, 0)
                            THEN varac_callsign_traits.last_alert_ts
                        ELSE excluded.last_alert_ts
                    END,
                    last_updated_ts=excluded.last_updated_ts
                """,
                (
                    cs,
                    int(data.get("is_emcomm", 0) or 0),
                    int(data.get("bbs_seen", 0) or 0),
                    int(data.get("alert_count", 0) or 0),
                    float(data.get("last_alert_ts", 0.0) or 0.0),
                    float(data.get("last_updated_ts", 0.0) or time.time()),
                ),
            )
            _note_table("varac_callsign_traits", written=1)

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
                _note_table("operator_checkins", written=1)

        local_conn.commit()
        success = True
    except Exception as e:
        error_text = str(e)
        try:
            if local_conn is not None:
                local_conn.rollback()
        except Exception:
            pass
        log.debug("VarAC ingest failed: %s", e)
    finally:
        run_finished_ts = time.time()
        rows_scanned = sum(int(v.get("rows_scanned", 0)) for v in table_counts.values())
        rows_written = sum(int(v.get("rows_written", 0)) for v in table_counts.values())
        try:
            if local_conn is not None:
                _record_sync_status(
                    local_conn,
                    run_started_ts=run_started_ts,
                    run_finished_ts=run_finished_ts,
                    varac_db_path=str(varac_db),
                    success=success,
                    rows_scanned=rows_scanned,
                    rows_written=rows_written,
                    table_counts=table_counts,
                    error_text=error_text,
                )
                local_conn.commit()
        except Exception:
            pass
        try:
            if varac_conn is not None:
                varac_conn.close()
        except Exception:
            pass
        try:
            if local_conn is not None:
                local_conn.close()
        except Exception:
            pass
        _LAST_RUN_MONO = time.monotonic()
        _INGEST_LOCK.release()

    if success and rows_written:
        log.debug(
            "VarAC ingest: scanned=%s written=%s source_db=%s",
            rows_scanned,
            rows_written,
            varac_db,
        )
    return success
