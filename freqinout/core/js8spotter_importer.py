from __future__ import annotations

import datetime as dt
import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.db_initializer import _ensure_js8_expect_tables
from freqinout.core.js8_expect_store import default_expect_db_path, save_expect_entry
from freqinout.core.js8_spotter_decode import decode_spotter_form_text
from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.observation_projection import observation_from_message_intelligence
from freqinout.core.observation_store import upsert_observation_conn
from freqinout.core.sqlite_utils import connect_sqlite, table_exists


@dataclass
class JS8SpotterImportStats:
    source_db: str
    forms_scanned: int = 0
    forms_imported: int = 0
    forms_skipped: int = 0
    expect_scanned: int = 0
    expect_imported: int = 0
    expect_skipped: int = 0
    archive_scanned: int = 0
    archive_imported: int = 0
    archive_skipped: int = 0
    grid_operators_updated: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _ensure_spotter_traffic_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spotter_traffic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            utc_ts REAL,
            utc_str TEXT,
            from_call TEXT,
            to_call TEXT,
            form_id TEXT,
            spotter_token TEXT,
            raw_text TEXT,
            decoded_text TEXT,
            state TEXT,
            read_ts REAL,
            flag_state INTEGER DEFAULT 0,
            relay_via TEXT,
            source_radio_id TEXT,
            js8_instance_id TEXT,
            ingested_ts REAL
        )
        """
    )
    try:
        conn.execute("ALTER TABLE spotter_traffic ADD COLUMN flag_state INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE spotter_traffic ADD COLUMN relay_via TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE spotter_traffic ADD COLUMN source_radio_id TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE spotter_traffic ADD COLUMN js8_instance_id TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE spotter_traffic ADD COLUMN ingested_ts REAL")
    except Exception:
        pass


def _ensure_import_log_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS js8spotter_import_log (
            source_db TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_fingerprint TEXT NOT NULL,
            imported_kind TEXT NOT NULL,
            imported_id TEXT,
            imported_ts REAL NOT NULL,
            PRIMARY KEY (source_db, source_table, source_id, source_fingerprint)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_js8spotter_import_log_kind ON js8spotter_import_log(imported_kind, imported_ts)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS js8spotter_import_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_db TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_fingerprint TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            imported_ts REAL NOT NULL,
            UNIQUE(source_db, source_table, source_id, source_fingerprint)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_js8spotter_import_archive_table ON js8spotter_import_archive(source_table, imported_ts)"
    )


def ensure_js8spotter_import_tables(conn) -> None:
    _ensure_spotter_traffic_table(conn)
    _ensure_js8_expect_tables(conn)
    _ensure_import_log_table(conn)


def _row_dict(row: object) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return {}


def _fingerprint(values: Mapping[str, Any]) -> str:
    payload = json.dumps(values, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _has_imported(conn, source_db: str, source_table: str, source_id: object, fingerprint: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM js8spotter_import_log
        WHERE source_db=? AND source_table=? AND source_id=? AND source_fingerprint=?
        LIMIT 1
        """,
        (source_db, source_table, str(source_id), fingerprint),
    ).fetchone()
    return bool(row)


def _record_import(
    conn,
    *,
    source_db: str,
    source_table: str,
    source_id: object,
    fingerprint: str,
    imported_kind: str,
    imported_id: object,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO js8spotter_import_log
            (source_db, source_table, source_id, source_fingerprint, imported_kind, imported_id, imported_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (source_db, source_table, str(source_id), fingerprint, imported_kind, str(imported_id or ""), time.time()),
    )


def _parse_spotter_timestamp(value: object) -> tuple[float, str]:
    text = str(value or "").strip()
    if not text:
        return 0.0, ""
    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ]
    for fmt in candidates:
        try:
            sample = text[:19] if "%S" in fmt else text[:16]
            moment = dt.datetime.strptime(sample, fmt)
            moment = moment.replace(tzinfo=dt.timezone.utc)
            return moment.timestamp(), moment.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    try:
        moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=dt.timezone.utc)
        moment = moment.astimezone(dt.timezone.utc)
        return moment.timestamp(), moment.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return 0.0, text


def _spotter_form_raw_text(row: Mapping[str, Any]) -> str:
    typeid = str(row.get("typeid", "") or "").strip().upper()
    responses = str(row.get("responses", "") or "").strip()
    msgtxt = str(row.get("msgtxt", "") or "").strip()
    timesig = str(row.get("timesig", "") or "").strip().upper()
    return " ".join(part for part in (typeid, responses, msgtxt, timesig) if part).strip()


def _spotter_form_id(row: Mapping[str, Any]) -> str:
    typeid = str(row.get("typeid", "") or "").strip().upper()
    return typeid[2:] if typeid.startswith("F!") else typeid


def _split_allowed(value: object) -> tuple[list[str], list[str], bool]:
    raw = str(value or "").strip()
    if not raw:
        return [], [], False
    calls: list[str] = []
    groups: list[str] = []
    allow_any = False
    for part in raw.replace(";", ",").split(","):
        item = part.strip().upper()
        if not item:
            continue
        if item == "*":
            allow_any = True
        elif item.startswith("@"):
            groups.append(item)
        else:
            calls.append(item)
    calls = list(dict.fromkeys(calls))
    groups = list(dict.fromkeys(groups))
    return calls, groups, allow_any


def _iter_rows(conn, table_name: str) -> Iterable[dict[str, Any]]:
    if not table_exists(conn, table_name):
        return []
    return [_row_dict(row) for row in conn.execute(f"SELECT * FROM {table_name}").fetchall()]


def _source_table_count(conn, table_name: str) -> int:
    if not table_exists(conn, table_name):
        return 0
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        if not row:
            return 0
        if isinstance(row, Mapping):
            return int(next(iter(row.values()), 0) or 0)
        return int(row[0] or 0)
    except Exception:
        return 0


def _source_id_for_archive(table_name: str, row: Mapping[str, Any]) -> str:
    for key in ("id", "grid_callsign", "sig_callsign", "keyword", "title", "name", "trigger", "cssr_msgid"):
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return _fingerprint(row)


def _archive_source_row(
    conn,
    *,
    source_db: str,
    source_table: str,
    source_id: str,
    fingerprint: str,
    row: Mapping[str, Any],
) -> int:
    conn.execute(
        """
        INSERT OR IGNORE INTO js8spotter_import_archive
            (source_db, source_table, source_id, source_fingerprint, payload_json, imported_ts)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            source_db,
            source_table,
            source_id,
            fingerprint,
            json.dumps(dict(row), sort_keys=True, default=str),
            time.time(),
        ),
    )
    imported_id = conn.execute(
        """
        SELECT id
        FROM js8spotter_import_archive
        WHERE source_db=? AND source_table=? AND source_id=? AND source_fingerprint=?
        """,
        (source_db, source_table, source_id, fingerprint),
    ).fetchone()
    return int(imported_id[0]) if imported_id else 0


def _upsert_operator_grid_from_spotter(conn, row: Mapping[str, Any]) -> bool:
    callsign = str(row.get("grid_callsign", "") or "").strip().upper()
    grid = str(row.get("grid_grid", "") or "").strip().upper()
    if not callsign or not grid:
        return False
    _ts, last_seen = _parse_spotter_timestamp(row.get("grid_timestamp"))
    if not last_seen:
        last_seen = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ensure_operator_checkins_schema(conn)
    conn.execute(
        """
        INSERT INTO operator_checkins (callsign, grid, first_seen_utc, last_seen_utc, checkin_count, trusted)
        VALUES (?, ?, ?, ?, 0, 0)
        ON CONFLICT(callsign) DO UPDATE SET
            grid=COALESCE(NULLIF(excluded.grid, ''), operator_checkins.grid),
            first_seen_utc=COALESCE(NULLIF(operator_checkins.first_seen_utc, ''), excluded.first_seen_utc),
            last_seen_utc=CASE
                WHEN COALESCE(excluded.last_seen_utc, '') > COALESCE(operator_checkins.last_seen_utc, '')
                    THEN excluded.last_seen_utc
                ELSE operator_checkins.last_seen_utc
            END
        """,
        (callsign, grid, last_seen, last_seen),
    )
    return True


def _mirror_spotter_import_to_observation(
    conn,
    *,
    imported_id: object,
    source_db: str,
    source_table: str,
    source_id: object,
    raw_text: str,
    form_id: str,
    from_call: str,
    to_call: str,
    utc_str: str,
    source_radio_id: object,
    js8_instance_id: object,
) -> None:
    try:
        info = analyze_spotter_text(
            raw_text,
            form_name=f"MCF{str(form_id or '').strip()}",
            from_call=from_call,
            to_call=to_call,
        )
        observation = observation_from_message_intelligence(
            info,
            source_ref=f"spotter_traffic:{int(imported_id or 0)}",
            source_family="spotter",
            source_radio_id=_int_or_none(source_radio_id),
            source_app=str(js8_instance_id or "").strip(),
            received_utc=utc_str,
            event_utc=utc_str,
            status="UNREAD",
            extra_provenance={
                "import_source": "js8spotter-db-import",
                "source_db": source_db,
                "source_table": source_table,
                "source_id": str(source_id or ""),
            },
        )
        upsert_observation_conn(conn, observation)
    except Exception:
        pass


def _int_or_none(value: object) -> int | None:
    try:
        return int(value) if str(value or "").strip() else None
    except Exception:
        return None


def import_js8spotter_database(
    source_db: str | Path,
    *,
    target_db: Optional[str | Path] = None,
    source_radio_id: object = "",
    js8_instance_id: object = "",
    import_forms: bool = True,
    import_expect: bool = True,
    import_archive: bool = True,
) -> JS8SpotterImportStats:
    source_path = Path(source_db).expanduser()
    target_path = Path(target_db) if target_db is not None else default_expect_db_path()
    stats = JS8SpotterImportStats(source_db=str(source_path))
    if not source_path.exists():
        stats.errors.append(f"Source database not found: {source_path}")
        return stats
    target_path.parent.mkdir(parents=True, exist_ok=True)
    src = connect_sqlite(source_path, row_factory=lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)})
    dst = connect_sqlite(target_path)
    try:
        ensure_js8spotter_import_tables(dst)
        source_identity = str(source_path.resolve())
        if import_forms and _source_table_count(src, "forms") == 0:
            stats.warnings.append("Selected JS8Spotter database has no form message history to show in Messages Inbox.")
        if import_expect and _source_table_count(src, "expect") == 0:
            stats.warnings.append("Selected JS8Spotter database has no Expect rules to import.")
        if import_forms:
            for row in _iter_rows(src, "forms"):
                stats.forms_scanned += 1
                source_id = row.get("id", "")
                raw_text = _spotter_form_raw_text(row)
                form_id = _spotter_form_id(row)
                if not source_id or not raw_text or not form_id:
                    stats.forms_skipped += 1
                    continue
                fp = _fingerprint(row)
                if _has_imported(dst, source_identity, "forms", source_id, fp):
                    stats.forms_skipped += 1
                    continue
                utc_ts, utc_str = _parse_spotter_timestamp(row.get("lm"))
                from_call = str(row.get("fromcall", "") or "").strip().upper()
                to_call = str(row.get("tocall", "") or "").strip().upper()
                token = str(row.get("timesig", "") or "").strip().upper()
                existing = dst.execute(
                    """
                    SELECT id
                    FROM spotter_traffic
                    WHERE from_call=? AND form_id=? AND COALESCE(spotter_token, '')=? AND raw_text=?
                    LIMIT 1
                    """,
                    (from_call, form_id, token, raw_text),
                ).fetchone()
                if existing:
                    imported_id = int(existing[0])
                    stats.forms_skipped += 1
                else:
                    decoded_text = decode_spotter_form_text(raw_text)
                    dst.execute(
                        """
                        INSERT INTO spotter_traffic
                            (utc_ts, utc_str, from_call, to_call, form_id, spotter_token,
                             raw_text, decoded_text, state, read_ts, relay_via,
                             source_radio_id, js8_instance_id, ingested_ts)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'UNREAD', 0, '', ?, ?, ?)
                        """,
                        (
                            utc_ts,
                            utc_str,
                            from_call,
                            to_call,
                            form_id,
                            token,
                            raw_text,
                            decoded_text,
                            str(source_radio_id or ""),
                            str(js8_instance_id or ""),
                            time.time(),
                        ),
                    )
                    imported_id = int(dst.execute("SELECT last_insert_rowid()").fetchone()[0])
                    stats.forms_imported += 1
                _mirror_spotter_import_to_observation(
                    dst,
                    imported_id=imported_id,
                    source_db=source_identity,
                    source_table="forms",
                    source_id=source_id,
                    raw_text=raw_text,
                    form_id=form_id,
                    from_call=from_call,
                    to_call=to_call,
                    utc_str=utc_str,
                    source_radio_id=source_radio_id,
                    js8_instance_id=js8_instance_id,
                )
                _record_import(
                    dst,
                    source_db=source_identity,
                    source_table="forms",
                    source_id=source_id,
                    fingerprint=fp,
                    imported_kind="spotter_traffic",
                    imported_id=imported_id,
                )
        if import_expect:
            dst.commit()
            for row in _iter_rows(src, "expect"):
                stats.expect_scanned += 1
                expect_key = str(row.get("expect", "") or "").strip().upper()
                if not expect_key:
                    stats.expect_skipped += 1
                    continue
                fp = _fingerprint(row)
                if _has_imported(dst, source_identity, "expect", expect_key, fp):
                    stats.expect_skipped += 1
                    continue
                allowed_calls, allowed_groups, allow_any = _split_allowed(row.get("allowed"))
                result = save_expect_entry(
                    {
                        "source_radio_id": str(source_radio_id or ""),
                        "source_scope": "radio" if str(source_radio_id or "").strip() else "all",
                        "js8_instance_id": str(js8_instance_id or ""),
                        "expect_key": expect_key,
                        "response_text": str(row.get("reply", "") or "").strip(),
                        "allowed_callsigns": allowed_calls,
                        "allowed_groups": allowed_groups,
                        "allow_any": allow_any,
                        "max_replies": int(row.get("txmax", 1) or 1),
                        "tx_speed": str(row.get("txspeed", "") or "").strip(),
                        "auto_tx_schedule": str(row.get("autotx", "") or "").strip(),
                        "enabled": True,
                        "auto_reply_enabled": bool(str(row.get("reply", "") or "").strip()),
                        "unattended_auto_reply_enabled": False,
                        "import_source": "js8spotter-db-import",
                    },
                    db_path=target_path,
                )
                _record_import(
                    dst,
                    source_db=source_identity,
                    source_table="expect",
                    source_id=expect_key,
                    fingerprint=fp,
                    imported_kind="js8_expect_entries",
                    imported_id=result.id,
                )
                stats.expect_imported += 1
        if import_archive:
            for table_name in ("profile", "activity", "search", "grid", "signal", "notify", "csstatrep", "setting"):
                for row in _iter_rows(src, table_name):
                    stats.archive_scanned += 1
                    source_id = _source_id_for_archive(table_name, row)
                    fp = _fingerprint(row)
                    if _has_imported(dst, source_identity, table_name, source_id, fp):
                        stats.archive_skipped += 1
                        continue
                    imported_id = _archive_source_row(
                        dst,
                        source_db=source_identity,
                        source_table=table_name,
                        source_id=source_id,
                        fingerprint=fp,
                        row=row,
                    )
                    _record_import(
                        dst,
                        source_db=source_identity,
                        source_table=table_name,
                        source_id=source_id,
                        fingerprint=fp,
                        imported_kind="js8spotter_import_archive",
                        imported_id=imported_id,
                    )
                    stats.archive_imported += 1
                    if table_name == "grid" and _upsert_operator_grid_from_spotter(dst, row):
                        stats.grid_operators_updated += 1
        dst.commit()
    except Exception as exc:
        dst.rollback()
        stats.errors.append(str(exc))
    finally:
        src.close()
        dst.close()
    return stats
