from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from freqinout.core.js8_expect_store import default_expect_db_path
from freqinout.core.sqlite_utils import connect_sqlite, table_exists


DEFAULT_SEARCH_ARCHIVE_TABLES = (
    "grid",
    "signal",
    "activity",
    "search",
    "profile",
    "notify",
    "csstatrep",
)


@dataclass(frozen=True)
class JS8SpotterArchiveRecord:
    source_db: str
    source_table: str
    source_id: str
    source_fingerprint: str
    title: str
    subtitle: str
    keywords: str
    imported_ts: float
    payload: dict[str, Any]


def _first_text(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(payload.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _join_parts(*parts: object) -> str:
    return " ".join(str(part or "").strip() for part in parts if str(part or "").strip())


def _archive_summary(source_table: str, source_id: str, payload: dict[str, Any]) -> tuple[str, str]:
    table = str(source_table or "").strip().lower()
    if table == "grid":
        call = _first_text(payload, "grid_callsign", "callsign", "call").upper()
        grid = _first_text(payload, "grid_grid", "grid").upper()
        title = _join_parts(call, grid) or source_id
        subtitle = _join_parts(
            "Grid",
            _first_text(payload, "grid_type", "type"),
            _first_text(payload, "grid_dial", "dial", "freq"),
            _first_text(payload, "grid_snr", "snr"),
            _first_text(payload, "grid_timestamp", "timestamp", "lm"),
        )
        return title, subtitle
    if table == "signal":
        call = _first_text(payload, "sig_callsign", "callsign", "call").upper()
        title = _join_parts(call, "signal") or source_id
        subtitle = _join_parts(
            _first_text(payload, "sig_freq", "sig_dial", "freq", "dial"),
            "SNR",
            _first_text(payload, "sig_snr", "snr"),
            _first_text(payload, "sig_timestamp", "timestamp", "lm"),
        )
        return title, subtitle
    if table == "activity":
        call = _first_text(payload, "call", "callsign", "fromcall").upper()
        title = _join_parts(call, _first_text(payload, "type", "kind"), "activity") or source_id
        subtitle = _join_parts(
            _first_text(payload, "value", "text", "message", "comment"),
            _first_text(payload, "spotdate", "timestamp", "lm"),
        )
        return title, subtitle
    if table == "search":
        title = _first_text(payload, "keyword", "search", "name", "title") or source_id
        subtitle = _join_parts(
            _first_text(payload, "comment", "notes", "description"),
            _first_text(payload, "last_seen", "timestamp", "lm"),
        )
        return title, subtitle
    if table == "profile":
        title = _first_text(payload, "title", "name") or source_id
        flags = []
        if str(payload.get("def", "") or "").strip() in {"1", "True", "true"}:
            flags.append("default")
        if str(payload.get("bgscan", "") or "").strip() in {"1", "True", "true"}:
            flags.append("background scan")
        return title, _join_parts("Profile", ", ".join(flags))
    if table == "notify":
        title = _first_text(payload, "trigger", "name", "title") or source_id
        subtitle = _join_parts("Alert", _first_text(payload, "action", "sound", "message", "notes"))
        return title, subtitle
    if table == "csstatrep":
        title = _join_parts(
            _first_text(payload, "cssr_from", "fromcall", "from").upper(),
            _first_text(payload, "cssr_group", "group", "target").upper(),
        ) or source_id
        subtitle = _join_parts(
            _first_text(payload, "cssr_status", "status"),
            _first_text(payload, "cssr_grid", "grid"),
            _first_text(payload, "cssr_timestamp", "timestamp", "lm"),
        )
        return title, subtitle
    return source_id, table


def _row_to_record(row: Any) -> Optional[JS8SpotterArchiveRecord]:
    try:
        source_db = str(row["source_db"] or "").strip()
        source_table = str(row["source_table"] or "").strip()
        source_id = str(row["source_id"] or "").strip()
        source_fingerprint = str(row["source_fingerprint"] or "").strip()
        payload = json.loads(str(row["payload_json"] or "{}"))
        imported_ts = float(row["imported_ts"] or 0.0)
    except Exception:
        return None
    if not source_table or not isinstance(payload, dict):
        return None
    title, subtitle = _archive_summary(source_table, source_id, payload)
    keywords = json.dumps(payload, sort_keys=True, default=str)
    return JS8SpotterArchiveRecord(
        source_db=source_db,
        source_table=source_table,
        source_id=source_id,
        source_fingerprint=source_fingerprint,
        title=title,
        subtitle=subtitle,
        keywords=keywords,
        imported_ts=imported_ts,
        payload=payload,
    )


def load_js8spotter_archive_records(
    *,
    db_path: Optional[str | Path] = None,
    table_names: Optional[Iterable[str]] = None,
    limit_per_table: int = 20,
) -> list[JS8SpotterArchiveRecord]:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    if not path.exists():
        return []
    tables = tuple(dict.fromkeys(str(name or "").strip().lower() for name in (table_names or DEFAULT_SEARCH_ARCHIVE_TABLES) if str(name or "").strip()))
    if not tables:
        return []
    limit = max(1, min(100, int(limit_per_table or 20)))
    conn = connect_sqlite(path, row_factory=lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)})
    try:
        if not table_exists(conn, "js8spotter_import_archive"):
            return []
        records: list[JS8SpotterArchiveRecord] = []
        for table in tables:
            rows = conn.execute(
                """
                SELECT source_db, source_table, source_id, source_fingerprint, payload_json, imported_ts
                FROM js8spotter_import_archive
                WHERE lower(source_table)=?
                ORDER BY imported_ts DESC, id DESC
                LIMIT ?
                """,
                (table, limit),
            ).fetchall()
            for row in rows:
                record = _row_to_record(row)
                if record is not None:
                    records.append(record)
        records.sort(key=lambda item: item.imported_ts, reverse=True)
        return records
    except Exception:
        return []
    finally:
        conn.close()


def spotter_archive_table_counts(*, db_path: Optional[str | Path] = None) -> dict[str, int]:
    path = Path(db_path) if db_path is not None else default_expect_db_path()
    if not path.exists():
        return {}
    conn = connect_sqlite(path)
    try:
        if not table_exists(conn, "js8spotter_import_archive"):
            return {}
        rows = conn.execute(
            """
            SELECT source_table, COUNT(*) AS count
            FROM js8spotter_import_archive
            GROUP BY source_table
            ORDER BY lower(source_table)
            """
        ).fetchall()
        return {str(row[0] or ""): int(row[1] or 0) for row in rows if str(row[0] or "").strip()}
    except Exception:
        return {}
    finally:
        conn.close()
