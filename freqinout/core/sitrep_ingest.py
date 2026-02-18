from __future__ import annotations

import datetime
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


SPOTTER_SITREP_FORMS = {
    "104": "SPOTTER_104",
    "301": "SPOTTER_301",
    "304": "SPOTTER_304",
}

_INGEST_LOCK = threading.Lock()
_LAST_RUN_MONO = 0.0
_MIN_INGEST_INTERVAL_SECONDS = 10.0


def ingest_sitreps(settings, *, max_rows_per_source: int = 500) -> Dict[str, int]:
    """
    Incrementally ingest SitRep-capable source data into local staging tables.
    This phase is additive only (raw source staging + checkpoints).
    """
    if not _is_enabled(settings, "sitrep_unified_ingest_enabled", True):
        return {
            "sources_attempted": 0,
            "sources_ok": 0,
            "rows_scanned": 0,
            "events_inserted": 0,
            "errors": 0,
        }

    global _LAST_RUN_MONO
    now_mono = time.monotonic()
    if now_mono - _LAST_RUN_MONO < _MIN_INGEST_INTERVAL_SECONDS:
        return {
            "sources_attempted": 0,
            "sources_ok": 0,
            "rows_scanned": 0,
            "events_inserted": 0,
            "errors": 0,
        }

    if not _INGEST_LOCK.acquire(blocking=False):
        return {
            "sources_attempted": 0,
            "sources_ok": 0,
            "rows_scanned": 0,
            "events_inserted": 0,
            "errors": 0,
        }

    try:
        _LAST_RUN_MONO = now_mono
        local_db = _local_db_path()
        stats = {
            "sources_attempted": 0,
            "sources_ok": 0,
            "rows_scanned": 0,
            "events_inserted": 0,
            "errors": 0,
        }
        conn = sqlite3.connect(local_db)
        try:
            _ensure_local_tables(conn)
            conn.commit()

            if _is_enabled(settings, "sitrep_ingest_local_spotter_backfill_enabled", True):
                if not _is_enabled(settings, "sitrep_local_spotter_backfill_done", False):
                    stats["sources_attempted"] += 1
                    _merge_stats(
                        stats,
                        _ingest_local_spotter_backfill(
                            conn,
                            settings,
                            str(local_db),
                            max_rows=max(int(max_rows_per_source), 500),
                        ),
                    )
                    stats["sources_ok"] += 1

            if _is_enabled(settings, "sitrep_ingest_js8spotter_enabled", True):
                stats["sources_attempted"] += 1
                path = _resolve_js8spotter_db_path(settings)
                if path:
                    _merge_stats(stats, _ingest_js8spotter(conn, path, max_rows=max_rows_per_source))
                    stats["sources_ok"] += 1

            if _is_enabled(settings, "sitrep_ingest_commstat3_enabled", True):
                stats["sources_attempted"] += 1
                path = _resolve_commstat_db_path(settings, prefer_v3=True)
                if path:
                    _merge_stats(stats, _ingest_commstat3(conn, path, max_rows=max_rows_per_source))
                    stats["sources_ok"] += 1

            if _is_enabled(settings, "sitrep_ingest_commstat23_enabled", True):
                stats["sources_attempted"] += 1
                path = _resolve_commstat_db_path(settings, prefer_v3=False)
                if path:
                    _merge_stats(stats, _ingest_commstat23(conn, path, max_rows=max_rows_per_source))
                    stats["sources_ok"] += 1
        finally:
            conn.commit()
            conn.close()
        return stats
    finally:
        _INGEST_LOCK.release()


def _local_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout_nets.db"


def _is_enabled(settings, key: str, default: bool) -> bool:
    try:
        val = settings.get(key, default)
    except Exception:
        val = default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    txt = str(val or "").strip().lower()
    if txt in {"1", "true", "yes", "on", "enabled"}:
        return True
    if txt in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _merge_stats(target: Dict[str, int], delta: Dict[str, int]) -> None:
    for key in ("rows_scanned", "events_inserted", "errors"):
        target[key] = int(target.get(key, 0)) + int(delta.get(key, 0))


def _ensure_local_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_ingest_checkpoint (
            source_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            last_id INTEGER NOT NULL DEFAULT 0,
            updated_ts REAL NOT NULL,
            last_error TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sitrep_source_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_db_path TEXT,
            source_id INTEGER NOT NULL,
            subtype TEXT NOT NULL,
            from_call TEXT,
            target TEXT,
            grid TEXT,
            scope TEXT,
            status_payload TEXT,
            raw_payload TEXT,
            event_ts REAL,
            event_ts_utc TEXT,
            ingested_ts REAL NOT NULL,
            UNIQUE(source, source_table, source_db_path, source_id)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_source_events_recent
            ON sitrep_source_events(source, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_source_events_call_recent
            ON sitrep_source_events(from_call, event_ts DESC, id DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sitrep_checkpoint_updated
            ON sitrep_ingest_checkpoint(updated_ts)
        """
    )


def _resolve_js8spotter_db_path(settings) -> Optional[Path]:
    candidates = []
    explicit = (settings.get("js8spotter_db_path", "") or "").strip()
    launch = (settings.get("path_js8spotter", "") or "").strip()
    forms_path = (settings.get("js8_forms_path", "") or "").strip()
    candidates.extend(_candidate_db_paths(explicit, "js8spotter.db"))
    candidates.extend(_candidate_db_paths(launch, "js8spotter.db"))
    if forms_path:
        try:
            fp = Path(forms_path)
            # JS8Spotter forms path is usually <install>/forms.
            candidates.extend(_candidate_db_paths(str(fp.parent), "js8spotter.db"))
        except Exception:
            pass
    return _pick_existing_path(candidates)


def _resolve_commstat_db_path(settings, *, prefer_v3: bool) -> Optional[Path]:
    candidates = []
    common = (settings.get("commstat_db_path", "") or "").strip()
    v3 = (settings.get("commstat3_db_path", "") or "").strip()
    v23 = (settings.get("commstat23_db_path", "") or "").strip()
    launch = (settings.get("path_commstat", "") or "").strip()

    if prefer_v3:
        for raw in (v3, common, launch):
            candidates.extend(_candidate_db_paths(raw, "traffic.db3"))
        # Ignore template DB when looking for live data.
    else:
        for raw in (v23, common, launch):
            candidates.extend(_candidate_db_paths(raw, "traffic.db3"))

    path = _pick_existing_path(candidates)
    if not path:
        return None
    return path


def _candidate_db_paths(raw: str, default_db_name: str) -> Iterable[Path]:
    txt = (raw or "").strip()
    if not txt:
        return []
    out = []
    try:
        p = Path(txt)
    except Exception:
        return out

    if p.is_file():
        suffix = p.suffix.lower()
        if suffix in {".db", ".db3", ".sqlite", ".sqlite3"}:
            out.append(p)
        out.append(p.parent / default_db_name)
        return out
    if p.is_dir():
        out.append(p / default_db_name)
        return out
    # Raw may point to a file that does not exist yet.
    if p.suffix:
        suffix = p.suffix.lower()
        if suffix in {".db", ".db3", ".sqlite", ".sqlite3"}:
            out.append(p)
        out.append(p.parent / default_db_name)
    else:
        out.append(p / default_db_name)
    return out


def _pick_existing_path(candidates: Iterable[Path]) -> Optional[Path]:
    seen = set()
    for c in candidates:
        try:
            resolved = c.resolve()
        except Exception:
            resolved = c
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            if resolved.exists() and resolved.is_file():
                return resolved
        except Exception:
            continue
    return None


def _open_source_db(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True, timeout=2.0)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


def _checkpoint_key(source: str, table: str) -> str:
    return f"{source}:{table}"


def _get_last_id(local_conn: sqlite3.Connection, source: str, table: str, source_db_path: str) -> int:
    key = _checkpoint_key(source, table)
    cur = local_conn.cursor()
    cur.execute(
        """
        SELECT last_id, source_db_path
        FROM sitrep_ingest_checkpoint
        WHERE source_key=?
        """,
        (key,),
    )
    row = cur.fetchone()
    if not row:
        return 0
    last_id = int(row[0] or 0)
    prev_path = str(row[1] or "")
    if prev_path and prev_path != source_db_path:
        return 0
    return max(last_id, 0)


def _set_last_id(
    local_conn: sqlite3.Connection,
    source: str,
    table: str,
    source_db_path: str,
    last_id: int,
    *,
    error_text: str = "",
) -> None:
    key = _checkpoint_key(source, table)
    now_ts = float(time.time())
    local_conn.execute(
        """
        INSERT INTO sitrep_ingest_checkpoint
            (source_key, source, source_table, source_db_path, last_id, updated_ts, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            source=excluded.source,
            source_table=excluded.source_table,
            source_db_path=excluded.source_db_path,
            last_id=excluded.last_id,
            updated_ts=excluded.updated_ts,
            last_error=excluded.last_error
        """,
        (
            key,
            source,
            table,
            source_db_path,
            int(last_id),
            now_ts,
            (error_text or "").strip(),
        ),
    )


def _insert_source_event(
    local_conn: sqlite3.Connection,
    *,
    source: str,
    source_table: str,
    source_db_path: str,
    source_id: int,
    subtype: str,
    from_call: str,
    target: str,
    grid: str,
    scope: str,
    status_payload: Dict,
    raw_payload: Dict,
    event_ts: float,
    event_ts_utc: str,
) -> bool:
    cur = local_conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO sitrep_source_events
            (source, source_table, source_db_path, source_id, subtype, from_call, target, grid, scope,
             status_payload, raw_payload, event_ts, event_ts_utc, ingested_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source,
            source_table,
            source_db_path,
            int(source_id),
            subtype,
            _clean_call(from_call),
            (target or "").strip().upper(),
            (grid or "").strip().upper(),
            (scope or "").strip(),
            json.dumps(status_payload or {}, separators=(",", ":"), ensure_ascii=True),
            json.dumps(raw_payload or {}, separators=(",", ":"), ensure_ascii=True),
            float(event_ts or 0.0),
            (event_ts_utc or "").strip(),
            float(time.time()),
        ),
    )
    return bool(cur.rowcount)


def _clean_call(val: str) -> str:
    return (val or "").strip().upper()


def _parse_ts(value, fallback: str = "") -> Tuple[float, str]:
    text = str(value or fallback or "").strip()
    if not text:
        return 0.0, ""
    if text.isdigit():
        try:
            ts = float(text)
            if ts > 1e12:
                ts = ts / 1000.0
            dt = datetime.datetime.utcfromtimestamp(ts)
            return ts, dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return 0.0, ""
    normalized = text.replace("T", " ").replace("Z", "")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.datetime.strptime(normalized, fmt)
            ts = dt.replace(tzinfo=datetime.timezone.utc).timestamp()
            return ts, dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    try:
        dt = datetime.datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        ts = dt.timestamp()
        return ts, dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return 0.0, ""


def _extract_form_id(typeid: str) -> str:
    tid = (typeid or "").strip().upper()
    if tid.startswith("F!"):
        tid = tid[2:]
    return "".join(ch for ch in tid if ch.isdigit())


def _is_sitrep_like_message(text: str) -> bool:
    upper = (text or "").upper()
    if not upper:
        return False
    return ("{&%}" in upper) or ("{F%}" in upper) or ("F!301" in upper) or ("F!304" in upper)


def _parse_spotter_response(raw_text: str) -> str:
    parts = str(raw_text or "").strip().split()
    if len(parts) < 2:
        return ""
    first = str(parts[0] or "").strip().upper()
    if not first.startswith("F!"):
        return ""
    return str(parts[1] or "").strip()


def _ingest_local_spotter_backfill(
    local_conn: sqlite3.Connection,
    settings,
    source_db_path: str,
    *,
    max_rows: int,
) -> Dict[str, int]:
    out = {"rows_scanned": 0, "events_inserted": 0, "errors": 0}
    source = "FIO_LOCAL"
    table = "spotter_traffic"
    done_key = "sitrep_local_spotter_backfill_done"

    try:
        if _is_enabled(settings, done_key, False):
            return out
    except Exception:
        pass

    if not _table_exists(local_conn, table):
        try:
            settings.set(done_key, True)
        except Exception:
            pass
        return out

    last_id = _get_last_id(local_conn, source, table, source_db_path)
    cur = local_conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, utc_ts, utc_str, from_call, to_call, form_id, raw_text, decoded_text, spotter_token
            FROM spotter_traffic
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (last_id, int(max_rows)),
        )
        rows = cur.fetchall()
    except Exception as e:
        out["errors"] += 1
        _set_last_id(local_conn, source, table, source_db_path, last_id, error_text=str(e))
        return out

    if not rows:
        _set_last_id(local_conn, source, table, source_db_path, last_id)
        try:
            settings.set(done_key, True)
        except Exception:
            pass
        return out

    max_seen = last_id
    for row in rows:
        rid = int(row[0] or 0)
        max_seen = max(max_seen, rid)
        out["rows_scanned"] += 1
        form_id = _extract_form_id(str(row[5] or ""))
        subtype = SPOTTER_SITREP_FORMS.get(form_id, "")
        if not subtype:
            continue
        event_ts, event_ts_utc = _parse_ts(row[1], fallback=str(row[2] or ""))
        raw_text = str(row[6] or "")
        responses = _parse_spotter_response(raw_text)
        inserted = _insert_source_event(
            local_conn,
            source=source,
            source_table=table,
            source_db_path=source_db_path,
            source_id=rid,
            subtype=subtype,
            from_call=str(row[3] or ""),
            target=str(row[4] or ""),
            grid="",
            scope="",
            status_payload={
                "form_id": form_id,
                "responses": responses,
            },
            raw_payload={
                "form_id": form_id,
                "raw_text": raw_text,
                "decoded_text": str(row[7] or ""),
                "spotter_token": str(row[8] or ""),
            },
            event_ts=event_ts,
            event_ts_utc=event_ts_utc,
        )
        if inserted:
            out["events_inserted"] += 1

    _set_last_id(local_conn, source, table, source_db_path, max_seen)
    if len(rows) < int(max_rows):
        try:
            settings.set(done_key, True)
        except Exception:
            pass
    return out


def _ingest_js8spotter(local_conn: sqlite3.Connection, source_db: Path, *, max_rows: int) -> Dict[str, int]:
    out = {"rows_scanned": 0, "events_inserted": 0, "errors": 0}
    source = "JS8SPOTTER"
    source_db_path = str(source_db)
    try:
        src = _open_source_db(source_db)
    except Exception as e:
        log.debug("SitrepIngest: cannot open JS8Spotter DB %s: %s", source_db, e)
        out["errors"] += 1
        return out
    try:
        # forms table: F!104/F!301/F!304.
        table = "forms"
        try:
            if _table_exists(src, table):
                last_id = _get_last_id(local_conn, source, table, source_db_path)
                cur = src.cursor()
                cur.execute(
                    """
                    SELECT id, fromcall, tocall, typeid, responses, msgtxt, timesig, lm, gwtx
                    FROM forms
                    WHERE id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (last_id, int(max_rows)),
                )
                rows = cur.fetchall()
                max_seen = last_id
                for row in rows:
                    rid = int(row[0] or 0)
                    max_seen = max(max_seen, rid)
                    out["rows_scanned"] += 1
                    form_id = _extract_form_id(str(row[3] or ""))
                    subtype = SPOTTER_SITREP_FORMS.get(form_id, "")
                    if not subtype:
                        continue
                    event_ts, event_ts_utc = _parse_ts(row[7], fallback=str(row[6] or ""))
                    inserted = _insert_source_event(
                        local_conn,
                        source=source,
                        source_table=table,
                        source_db_path=source_db_path,
                        source_id=rid,
                        subtype=subtype,
                        from_call=str(row[1] or ""),
                        target=str(row[2] or ""),
                        grid="",
                        scope="",
                        status_payload={
                            "form_id": form_id,
                            "responses": str(row[4] or ""),
                        },
                        raw_payload={
                            "typeid": str(row[3] or ""),
                            "responses": str(row[4] or ""),
                            "msgtxt": str(row[5] or ""),
                            "timesig": str(row[6] or ""),
                            "lm": str(row[7] or ""),
                            "gwtx": str(row[8] or ""),
                        },
                        event_ts=event_ts,
                        event_ts_utc=event_ts_utc,
                    )
                    if inserted:
                        out["events_inserted"] += 1
                _set_last_id(local_conn, source, table, source_db_path, max_seen)
        except Exception as e:
            out["errors"] += 1
            _set_last_id(local_conn, source, table, source_db_path, _get_last_id(local_conn, source, table, source_db_path), error_text=str(e))
            log.debug("SitrepIngest: JS8Spotter forms ingest failed: %s", e)

        # csstatrep table: CommStat 12-digit status seen by JS8Spotter.
        table = "csstatrep"
        try:
            if _table_exists(src, table):
                last_id = _get_last_id(local_conn, source, table, source_db_path)
                cur = src.cursor()
                cur.execute(
                    """
                    SELECT id, cssr_from, cssr_group, cssr_grid, cssr_prio, cssr_msgid, cssr_status, cssr_notes, cssr_timestamp
                    FROM csstatrep
                    WHERE id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (last_id, int(max_rows)),
                )
                rows = cur.fetchall()
                max_seen = last_id
                for row in rows:
                    rid = int(row[0] or 0)
                    max_seen = max(max_seen, rid)
                    out["rows_scanned"] += 1
                    status_txt = str(row[6] or "").strip()
                    subtype = "COMMSTAT_12" if len(status_txt) >= 12 else "COMMSTAT_FWD"
                    event_ts, event_ts_utc = _parse_ts(row[8])
                    inserted = _insert_source_event(
                        local_conn,
                        source=source,
                        source_table=table,
                        source_db_path=source_db_path,
                        source_id=rid,
                        subtype=subtype,
                        from_call=str(row[1] or ""),
                        target=str(row[2] or ""),
                        grid=str(row[3] or ""),
                        scope=str(row[4] or ""),
                        status_payload={
                            "status": status_txt,
                            "priority": str(row[4] or ""),
                        },
                        raw_payload={
                            "cssr_from": str(row[1] or ""),
                            "cssr_group": str(row[2] or ""),
                            "cssr_grid": str(row[3] or ""),
                            "cssr_prio": str(row[4] or ""),
                            "cssr_msgid": str(row[5] or ""),
                            "cssr_status": status_txt,
                            "cssr_notes": str(row[7] or ""),
                            "cssr_timestamp": str(row[8] or ""),
                        },
                        event_ts=event_ts,
                        event_ts_utc=event_ts_utc,
                    )
                    if inserted:
                        out["events_inserted"] += 1
                _set_last_id(local_conn, source, table, source_db_path, max_seen)
        except Exception as e:
            out["errors"] += 1
            _set_last_id(local_conn, source, table, source_db_path, _get_last_id(local_conn, source, table, source_db_path), error_text=str(e))
            log.debug("SitrepIngest: JS8Spotter csstatrep ingest failed: %s", e)
    finally:
        src.close()
    return out


def _ingest_commstat3(local_conn: sqlite3.Connection, source_db: Path, *, max_rows: int) -> Dict[str, int]:
    out = {"rows_scanned": 0, "events_inserted": 0, "errors": 0}
    source = "COMMSTAT3"
    source_db_path = str(source_db)
    try:
        src = _open_source_db(source_db)
    except Exception as e:
        log.debug("SitrepIngest: cannot open CommStat3 DB %s: %s", source_db, e)
        out["errors"] += 1
        return out
    try:
        if not _table_exists(src, "statrep"):
            return out

        # statrep table
        table = "statrep"
        try:
            last_id = _get_last_id(local_conn, source, table, source_db_path)
            cur = src.cursor()
            cur.execute(
                """
                SELECT id, datetime, date, freq, db, source, sr_id, from_callsign, target, grid, scope,
                       map, power, water, med, telecom, travel, internet, fuel, food, crime, civil, political, comments
                FROM statrep
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (last_id, int(max_rows)),
            )
            rows = cur.fetchall()
            max_seen = last_id
            for row in rows:
                rid = int(row[0] or 0)
                max_seen = max(max_seen, rid)
                out["rows_scanned"] += 1
                event_ts, event_ts_utc = _parse_ts(row[1], fallback=str(row[2] or ""))
                inserted = _insert_source_event(
                    local_conn,
                    source=source,
                    source_table=table,
                    source_db_path=source_db_path,
                    source_id=rid,
                    subtype="COMMSTAT_12",
                    from_call=str(row[7] or ""),
                    target=str(row[8] or ""),
                    grid=str(row[9] or ""),
                    scope=str(row[10] or ""),
                    status_payload={
                        "overall_status": str(row[11] or ""),
                        "power": str(row[12] or ""),
                        "water": str(row[13] or ""),
                        "medical": str(row[14] or ""),
                        "communications": str(row[15] or ""),
                        "travel": str(row[16] or ""),
                        "internet": str(row[17] or ""),
                        "fuel": str(row[18] or ""),
                        "food": str(row[19] or ""),
                        "crime": str(row[20] or ""),
                        "civil_unrest": str(row[21] or ""),
                        "political": str(row[22] or ""),
                    },
                    raw_payload={
                        "datetime": str(row[1] or ""),
                        "date": str(row[2] or ""),
                        "freq": row[3],
                        "db": row[4],
                        "source": row[5],
                        "sr_id": str(row[6] or ""),
                        "comments": str(row[23] or ""),
                    },
                    event_ts=event_ts,
                    event_ts_utc=event_ts_utc,
                )
                if inserted:
                    out["events_inserted"] += 1
            _set_last_id(local_conn, source, table, source_db_path, max_seen)
        except Exception as e:
            out["errors"] += 1
            _set_last_id(local_conn, source, table, source_db_path, _get_last_id(local_conn, source, table, source_db_path), error_text=str(e))
            log.debug("SitrepIngest: CommStat3 statrep ingest failed: %s", e)

        # messages table (incremental checkpoint; ingest sitrep-like rows only).
        table = "messages"
        try:
            if _table_exists(src, table):
                last_id = _get_last_id(local_conn, source, table, source_db_path)
                cur = src.cursor()
                cur.execute(
                    """
                    SELECT id, datetime, date, msg_id, from_callsign, target, message
                    FROM messages
                    WHERE id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (last_id, int(max_rows)),
                )
                rows = cur.fetchall()
                max_seen = last_id
                for row in rows:
                    rid = int(row[0] or 0)
                    max_seen = max(max_seen, rid)
                    out["rows_scanned"] += 1
                    message_text = str(row[6] or "")
                    if not _is_sitrep_like_message(message_text):
                        continue
                    upper = message_text.upper()
                    if "F!301" in upper:
                        subtype = "SPOTTER_301"
                    elif "F!304" in upper:
                        subtype = "SPOTTER_304"
                    elif "{F%}" in upper:
                        subtype = "COMMSTAT_FWD"
                    else:
                        subtype = "COMMSTAT_12"
                    event_ts, event_ts_utc = _parse_ts(row[1], fallback=str(row[2] or ""))
                    inserted = _insert_source_event(
                        local_conn,
                        source=source,
                        source_table=table,
                        source_db_path=source_db_path,
                        source_id=rid,
                        subtype=subtype,
                        from_call=str(row[4] or ""),
                        target=str(row[5] or ""),
                        grid="",
                        scope="",
                        status_payload={"message": message_text},
                        raw_payload={
                            "datetime": str(row[1] or ""),
                            "date": str(row[2] or ""),
                            "msg_id": str(row[3] or ""),
                            "message": message_text,
                        },
                        event_ts=event_ts,
                        event_ts_utc=event_ts_utc,
                    )
                    if inserted:
                        out["events_inserted"] += 1
                _set_last_id(local_conn, source, table, source_db_path, max_seen)
        except Exception as e:
            out["errors"] += 1
            _set_last_id(local_conn, source, table, source_db_path, _get_last_id(local_conn, source, table, source_db_path), error_text=str(e))
            log.debug("SitrepIngest: CommStat3 messages ingest failed: %s", e)

        # alerts table (incremental checkpoint only in Phase 1).
        table = "alerts"
        try:
            if _table_exists(src, table):
                last_id = _get_last_id(local_conn, source, table, source_db_path)
                cur = src.cursor()
                cur.execute(
                    """
                    SELECT id
                    FROM alerts
                    WHERE id > ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (last_id, int(max_rows)),
                )
                rows = cur.fetchall()
                max_seen = last_id
                for row in rows:
                    rid = int(row[0] or 0)
                    max_seen = max(max_seen, rid)
                    out["rows_scanned"] += 1
                _set_last_id(local_conn, source, table, source_db_path, max_seen)
        except Exception as e:
            out["errors"] += 1
            _set_last_id(local_conn, source, table, source_db_path, _get_last_id(local_conn, source, table, source_db_path), error_text=str(e))
            log.debug("SitrepIngest: CommStat3 alerts ingest failed: %s", e)
    finally:
        src.close()
    return out


def _ingest_commstat23(local_conn: sqlite3.Connection, source_db: Path, *, max_rows: int) -> Dict[str, int]:
    out = {"rows_scanned": 0, "events_inserted": 0, "errors": 0}
    source = "COMMSTAT23"
    source_db_path = str(source_db)
    try:
        src = _open_source_db(source_db)
    except Exception as e:
        log.debug("SitrepIngest: cannot open CommStat2.3 DB %s: %s", source_db, e)
        out["errors"] += 1
        return out
    try:
        table = "StatRep_Data"
        if not _table_exists(src, table):
            return out
        last_id = _get_last_id(local_conn, source, table, source_db_path)
        cur = src.cursor()
        cur.execute(
            """
            SELECT id, datetime, date, freq, callsign, groupname, grid, SRid, prec,
                   status, commpwr, pubwtr, med, ota, trav, net, fuel, food, crime, civil, political, comments
            FROM StatRep_Data
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (last_id, int(max_rows)),
        )
        rows = cur.fetchall()
        max_seen = last_id
        for row in rows:
            rid = int(row[0] or 0)
            max_seen = max(max_seen, rid)
            out["rows_scanned"] += 1
            event_ts, event_ts_utc = _parse_ts(row[1], fallback=str(row[2] or ""))
            target = str(row[5] or "").strip().upper()
            if target and not target.startswith("@"):
                target = f"@{target}"
            inserted = _insert_source_event(
                local_conn,
                source=source,
                source_table=table,
                source_db_path=source_db_path,
                source_id=rid,
                subtype="COMMSTAT_12",
                from_call=str(row[4] or ""),
                target=target,
                grid=str(row[6] or ""),
                scope="",
                status_payload={
                    "overall_status": str(row[9] or ""),
                    "power": str(row[10] or ""),
                    "water": str(row[11] or ""),
                    "medical": str(row[12] or ""),
                    "communications": str(row[13] or ""),
                    "travel": str(row[14] or ""),
                    "internet": str(row[15] or ""),
                    "fuel": str(row[16] or ""),
                    "food": str(row[17] or ""),
                    "crime": str(row[18] or ""),
                    "civil_unrest": str(row[19] or ""),
                    "political": str(row[20] or ""),
                },
                raw_payload={
                    "datetime": str(row[1] or ""),
                    "date": str(row[2] or ""),
                    "freq": row[3],
                    "sr_id": str(row[7] or ""),
                    "precedence": str(row[8] or ""),
                    "comments": str(row[21] or ""),
                },
                event_ts=event_ts,
                event_ts_utc=event_ts_utc,
            )
            if inserted:
                out["events_inserted"] += 1
        _set_last_id(local_conn, source, table, source_db_path, max_seen)
    except Exception as e:
        out["errors"] += 1
        _set_last_id(local_conn, source, table, source_db_path, _get_last_id(local_conn, source, table, source_db_path), error_text=str(e))
        log.debug("SitrepIngest: CommStat2.3 ingest failed: %s", e)
    finally:
        src.close()
    return out
