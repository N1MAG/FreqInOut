from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from freqinout.core.logger import log
from freqinout.core.multi_radio_store import MultiRadioStore


LEGACY_JS8_SOURCE_KEY = "legacy:primary"
LEGACY_JS8_SOURCE_LABEL = "Primary JS8"


@dataclass(frozen=True)
class JS8InstanceSource:
    source_key: str
    source_scope: str
    source_label: str
    device_profile_id: Optional[int]
    device_profile_name: str
    directed_path: Optional[Path]
    all_path: Optional[Path]
    inbox_path: Optional[Path]


def _text(value: object) -> str:
    return str(value or "").strip()


def _normalize_path_text(path: Optional[Path | str]) -> str:
    if path is None:
        return ""
    raw = _text(path)
    if not raw:
        return ""
    return os.path.normcase(os.path.normpath(raw))


def _looks_like_js8_profile_dir(path: Path) -> bool:
    if path.is_dir():
        return True
    if path.suffix:
        return False
    name = path.name.upper()
    return name not in {"DIRECTED.TXT", "ALL.TXT"} and not name.lower().startswith("inbox")


def _candidate_inbox_from_dir(base_dir: Path) -> Optional[Path]:
    candidates = [
        base_dir / "inbox_v1",
        base_dir / "inbox_v1.sqlite",
        base_dir / "inbox_v1.db",
        base_dir / "inbox.db3",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    try:
        for candidate in sorted(base_dir.glob("inbox*")):
            if candidate.is_file():
                return candidate
    except Exception:
        pass
    return candidates[0] if candidates else None


def resolve_js8_paths(
    *,
    directed_text: str = "",
    inbox_text: str = "",
    profile_text: str = "",
) -> Tuple[Optional[Path], Optional[Path], Optional[Path]]:
    directed_path: Optional[Path] = None
    inbox_path: Optional[Path] = None
    all_path: Optional[Path] = None

    profile_raw = _text(profile_text)
    if profile_raw:
        profile_path = Path(profile_raw)
        name = profile_path.name.upper()
        if name == "DIRECTED.TXT":
            directed_path = profile_path
        elif name == "ALL.TXT":
            all_path = profile_path
            directed_path = profile_path.parent / "DIRECTED.TXT"
        elif name.lower().startswith("inbox"):
            inbox_path = profile_path
        elif _looks_like_js8_profile_dir(profile_path):
            directed_path = profile_path / "DIRECTED.TXT"
            all_path = profile_path / "ALL.TXT"
            inbox_path = _candidate_inbox_from_dir(profile_path)
        else:
            parent = profile_path.parent
            directed_path = parent / "DIRECTED.TXT"
            all_path = parent / "ALL.TXT"
            inbox_path = _candidate_inbox_from_dir(parent)

    directed_raw = _text(directed_text)
    if directed_raw:
        directed_path = Path(directed_raw)

    inbox_raw = _text(inbox_text)
    if inbox_raw:
        inbox_path = Path(inbox_raw)

    if all_path is None and directed_path is not None:
        all_path = directed_path.parent / "ALL.TXT"
    if directed_path is None and inbox_path is not None:
        directed_path = inbox_path.parent / "DIRECTED.TXT"
        all_path = inbox_path.parent / "ALL.TXT"
    if inbox_path is None and directed_path is not None:
        inbox_path = _candidate_inbox_from_dir(directed_path.parent)
    if directed_path is None and all_path is not None:
        directed_path = all_path.parent / "DIRECTED.TXT"
    return directed_path, all_path, inbox_path


def _profile_has_js8_paths(profile: Mapping[str, Any]) -> bool:
    return any(
        _text(profile.get(key, ""))
        for key in ("js8_directed_path", "js8_inbox_path", "js8_profile_path")
    )


def _build_device_source(profile: Mapping[str, Any]) -> Optional[JS8InstanceSource]:
    device_id = int(profile.get("id", 0) or 0)
    if device_id <= 0:
        return None
    directed_path, all_path, inbox_path = resolve_js8_paths(
        directed_text=_text(profile.get("js8_directed_path", "")),
        inbox_text=_text(profile.get("js8_inbox_path", "")),
        profile_text=_text(profile.get("js8_profile_path", "")),
    )
    if directed_path is None and inbox_path is None and all_path is None:
        return None
    name = _text(profile.get("name", "")) or f"Device {device_id}"
    return JS8InstanceSource(
        source_key=f"device:{device_id}",
        source_scope="device_profile",
        source_label=name,
        device_profile_id=device_id,
        device_profile_name=name,
        directed_path=directed_path,
        all_path=all_path,
        inbox_path=inbox_path,
    )


def _build_legacy_source(settings: object) -> Optional[JS8InstanceSource]:
    getter = getattr(settings, "get", None)
    if not callable(getter):
        return None
    directed_path, all_path, inbox_path = resolve_js8_paths(
        directed_text=_text(getter("js8_directed_path", "")),
        inbox_text="",
        profile_text=_text(getter("js8_profile_path", "")),
    )
    if directed_path is None and inbox_path is None and all_path is None:
        return None
    return JS8InstanceSource(
        source_key=LEGACY_JS8_SOURCE_KEY,
        source_scope="legacy",
        source_label=LEGACY_JS8_SOURCE_LABEL,
        device_profile_id=None,
        device_profile_name="",
        directed_path=directed_path,
        all_path=all_path,
        inbox_path=inbox_path,
    )


def _source_signature(source: JS8InstanceSource) -> Tuple[str, str, str]:
    directed_sig = _normalize_path_text(source.directed_path)
    all_sig = _normalize_path_text(source.all_path)
    if directed_sig or all_sig:
        return ("", directed_sig, all_sig)
    return (
        _normalize_path_text(source.inbox_path),
        "",
        "",
    )


def resolve_js8_instance_sources(
    settings: object,
    *,
    store: Optional[MultiRadioStore] = None,
) -> List[JS8InstanceSource]:
    store_obj = store
    if store_obj is None:
        try:
            store_obj = MultiRadioStore()
        except Exception as exc:
            log.debug("JS8MultiSource: failed creating MultiRadioStore: %s", exc)
            store_obj = None

    ordered_sources: List[JS8InstanceSource] = []
    if store_obj is not None:
        try:
            profiles = list(store_obj.list_runtime_active_device_profiles())
        except Exception as exc:
            log.debug("JS8MultiSource: failed listing runtime-active device profiles: %s", exc)
            profiles = []
        for profile in profiles:
            if not isinstance(profile, Mapping):
                continue
            if not _profile_has_js8_paths(profile):
                continue
            source = _build_device_source(profile)
            if source is not None:
                ordered_sources.append(source)

    legacy_source = _build_legacy_source(settings)
    if legacy_source is not None:
        ordered_sources.append(legacy_source)

    deduped: List[JS8InstanceSource] = []
    seen_signatures: Dict[Tuple[str, str, str], JS8InstanceSource] = {}
    for source in ordered_sources:
        signature = _source_signature(source)
        if not any(signature):
            continue
        existing = seen_signatures.get(signature)
        if existing is not None:
            log.debug(
                "JS8MultiSource: deduplicated source %s in favor of %s for signature=%s",
                source.source_key,
                existing.source_key,
                signature,
            )
            continue
        seen_signatures[signature] = source
        deduped.append(source)
    return deduped


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {str(row[1] or "") for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    except Exception:
        return set()


def _migrate_legacy_js8_rows(conn: sqlite3.Connection, settings: Optional[object]) -> None:
    if not _table_exists(conn, "js8_messages"):
        return
    legacy_source = _build_legacy_source(settings) if settings is not None else None
    source_label = legacy_source.source_label if legacy_source is not None else LEGACY_JS8_SOURCE_LABEL
    directed_path = _text(legacy_source.directed_path if legacy_source is not None else "")
    all_path = _text(legacy_source.all_path if legacy_source is not None else "")
    inbox_path = _text(legacy_source.inbox_path if legacy_source is not None else "")

    old_message_columns = _table_columns(conn, "js8_messages")
    if not old_message_columns:
        return
    read_expr = "read_ts" if "read_ts" in old_message_columns else "0"
    flag_expr = "flag_state" if "flag_state" in old_message_columns else "0"
    rows = conn.execute(
        f"""
        SELECT id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text,
               state, {read_expr} AS read_ts, {flag_expr} AS flag_state
          FROM js8_messages
         ORDER BY id ASC
        """
    ).fetchall()
    if rows:
        conn.executemany(
            """
            INSERT INTO js8_messages_v2 (
                source_key, source_scope, source_label, device_profile_id, remote_id,
                inbox_path, directed_path, all_path,
                from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text,
                state, read_ts, flag_state
            )
            VALUES (?, 'legacy', ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_key, remote_id) DO NOTHING
            """,
            [
                (
                    LEGACY_JS8_SOURCE_KEY,
                    source_label,
                    int(row[0] or 0),
                    inbox_path,
                    directed_path,
                    all_path,
                    _text(row[1]),
                    _text(row[2]),
                    _text(row[3]),
                    _text(row[4]),
                    float(row[5] or 0.0),
                    _text(row[6]),
                    _text(row[7]),
                    _text(row[8]).upper() or "UNREAD",
                    float(row[9] or 0.0),
                    int(row[10] or 0),
                )
                for row in rows
                if int(row[0] or 0) > 0
            ],
        )
        max_remote_id = max(int(row[0] or 0) for row in rows if int(row[0] or 0) > 0)
        conn.execute(
            """
            INSERT INTO js8_inbox_ingest_state_v2 (
                source_key, source_scope, source_label, device_profile_id, inbox_path,
                directed_path, all_path, last_remote_id, updated_ts
            )
            VALUES (?, 'legacy', ?, NULL, ?, ?, ?, ?, strftime('%s','now'))
            ON CONFLICT(source_key) DO UPDATE SET
                source_label=excluded.source_label,
                inbox_path=excluded.inbox_path,
                directed_path=excluded.directed_path,
                all_path=excluded.all_path,
                last_remote_id=MAX(js8_inbox_ingest_state_v2.last_remote_id, excluded.last_remote_id),
                updated_ts=excluded.updated_ts
            """,
            (
                LEGACY_JS8_SOURCE_KEY,
                source_label,
                inbox_path,
                directed_path,
                all_path,
                max_remote_id,
            ),
        )

    if not _table_exists(conn, "js8_inbox_state"):
        return
    old_state_columns = _table_columns(conn, "js8_inbox_state")
    last_seen_expr = "last_seen" if "last_seen" in old_state_columns else "0"
    read_ts_expr = "read_ts" if "read_ts" in old_state_columns else "0"
    state_rows = conn.execute(
        f"""
        SELECT id, state, {last_seen_expr} AS last_seen, {read_ts_expr} AS read_ts
          FROM js8_inbox_state
         ORDER BY id ASC
        """
    ).fetchall()
    if state_rows:
        conn.executemany(
            """
            INSERT INTO js8_inbox_state_v2 (source_key, remote_id, state, last_seen, read_ts)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source_key, remote_id) DO UPDATE SET
                state=excluded.state,
                last_seen=excluded.last_seen,
                read_ts=excluded.read_ts
            """,
            [
                (
                    LEGACY_JS8_SOURCE_KEY,
                    int(row[0] or 0),
                    _text(row[1]).upper() or "UNREAD",
                    float(row[2] or 0.0),
                    float(row[3] or 0.0),
                )
                for row in state_rows
                if int(row[0] or 0) > 0
            ],
        )


def ensure_js8_local_tables(conn: sqlite3.Connection, settings: Optional[object] = None) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_messages_v2 (
            local_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key TEXT NOT NULL,
            source_scope TEXT NOT NULL,
            source_label TEXT,
            device_profile_id INTEGER,
            remote_id INTEGER NOT NULL,
            inbox_path TEXT,
            directed_path TEXT,
            all_path TEXT,
            from_call TEXT,
            to_call TEXT,
            msg_type TEXT,
            utc_str TEXT,
            utc_ts REAL,
            raw_text TEXT,
            decoded_text TEXT,
            state TEXT,
            read_ts REAL,
            flag_state INTEGER DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_js8_messages_v2_source_remote
            ON js8_messages_v2(source_key, remote_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_js8_messages_v2_ts
            ON js8_messages_v2(utc_ts DESC, local_id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_js8_messages_v2_source_ts
            ON js8_messages_v2(source_key, utc_ts DESC, local_id DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_inbox_state_v2 (
            source_key TEXT NOT NULL,
            remote_id INTEGER NOT NULL,
            state TEXT,
            last_seen REAL,
            read_ts REAL,
            PRIMARY KEY (source_key, remote_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_inbox_ingest_state_v2 (
            source_key TEXT PRIMARY KEY,
            source_scope TEXT NOT NULL,
            source_label TEXT,
            device_profile_id INTEGER,
            inbox_path TEXT,
            directed_path TEXT,
            all_path TEXT,
            last_remote_id INTEGER DEFAULT 0,
            updated_ts REAL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_spotter_ingest_state_v2 (
            source_key TEXT PRIMARY KEY,
            source_scope TEXT NOT NULL,
            source_label TEXT,
            device_profile_id INTEGER,
            directed_path TEXT,
            last_offset INTEGER DEFAULT 0,
            updated_ts REAL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS js8_links_ingest_state_v2 (
            source_key TEXT PRIMARY KEY,
            source_scope TEXT NOT NULL,
            source_label TEXT,
            device_profile_id INTEGER,
            directed_path TEXT,
            all_path TEXT,
            directed_offset INTEGER DEFAULT 0,
            all_offset INTEGER DEFAULT 0,
            updated_ts REAL DEFAULT 0
        )
        """
    )
    _migrate_legacy_js8_rows(conn, settings)


def load_js8_inbox_state_map(conn: sqlite3.Connection) -> Dict[Tuple[str, int], Tuple[str, float]]:
    rows = conn.execute(
        """
        SELECT source_key, remote_id, state, read_ts
          FROM js8_inbox_state_v2
        """
    ).fetchall()
    return {
        (str(row[0] or ""), int(row[1] or 0)): (_text(row[2]).upper() or "UNREAD", float(row[3] or 0.0))
        for row in rows
        if _text(row[0]) and int(row[1] or 0) > 0
    }


def load_js8_inbox_watermarks(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute(
        """
        SELECT source_key, last_remote_id
          FROM js8_inbox_ingest_state_v2
        """
    ).fetchall()
    return {
        str(row[0] or ""): int(row[1] or 0)
        for row in rows
        if _text(row[0])
    }


def load_js8_offset_map(conn: sqlite3.Connection, table_name: str) -> Dict[str, int]:
    if table_name not in {"js8_spotter_ingest_state_v2"}:
        raise ValueError(f"Unsupported JS8 offset table: {table_name}")
    rows = conn.execute(f"SELECT source_key, last_offset FROM {table_name}").fetchall()
    return {
        str(row[0] or ""): int(row[1] or 0)
        for row in rows
        if _text(row[0])
    }


def upsert_js8_inbox_watermark(
    conn: sqlite3.Connection,
    source: JS8InstanceSource,
    last_remote_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO js8_inbox_ingest_state_v2 (
            source_key, source_scope, source_label, device_profile_id, inbox_path,
            directed_path, all_path, last_remote_id, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            source_scope=excluded.source_scope,
            source_label=excluded.source_label,
            device_profile_id=excluded.device_profile_id,
            inbox_path=excluded.inbox_path,
            directed_path=excluded.directed_path,
            all_path=excluded.all_path,
            last_remote_id=excluded.last_remote_id,
            updated_ts=excluded.updated_ts
        """,
        (
            source.source_key,
            source.source_scope,
            source.source_label,
            source.device_profile_id,
            _text(source.inbox_path),
            _text(source.directed_path),
            _text(source.all_path),
            int(last_remote_id or 0),
            float(time.time()),
        ),
    )


def upsert_js8_offset_state(
    conn: sqlite3.Connection,
    table_name: str,
    source: JS8InstanceSource,
    offset: int,
) -> None:
    if table_name != "js8_spotter_ingest_state_v2":
        raise ValueError(f"Unsupported JS8 offset table: {table_name}")
    conn.execute(
        """
        INSERT INTO js8_spotter_ingest_state_v2 (
            source_key, source_scope, source_label, device_profile_id, directed_path, last_offset, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            source_scope=excluded.source_scope,
            source_label=excluded.source_label,
            device_profile_id=excluded.device_profile_id,
            directed_path=excluded.directed_path,
            last_offset=excluded.last_offset,
            updated_ts=excluded.updated_ts
        """,
        (
            source.source_key,
            source.source_scope,
            source.source_label,
            source.device_profile_id,
            _text(source.directed_path),
            int(offset or 0),
            float(time.time()),
        ),
    )


def upsert_js8_links_offset_state(
    conn: sqlite3.Connection,
    source: JS8InstanceSource,
    *,
    directed_offset: int,
    all_offset: int,
) -> None:
    conn.execute(
        """
        INSERT INTO js8_links_ingest_state_v2 (
            source_key, source_scope, source_label, device_profile_id, directed_path,
            all_path, directed_offset, all_offset, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            source_scope=excluded.source_scope,
            source_label=excluded.source_label,
            device_profile_id=excluded.device_profile_id,
            directed_path=excluded.directed_path,
            all_path=excluded.all_path,
            directed_offset=excluded.directed_offset,
            all_offset=excluded.all_offset,
            updated_ts=excluded.updated_ts
        """,
        (
            source.source_key,
            source.source_scope,
            source.source_label,
            source.device_profile_id,
            _text(source.directed_path),
            _text(source.all_path),
            int(directed_offset or 0),
            int(all_offset or 0),
            float(time.time()),
        ),
    )


def load_js8_links_offset_map(conn: sqlite3.Connection) -> Dict[str, Dict[str, int]]:
    rows = conn.execute(
        """
        SELECT source_key, directed_offset, all_offset
          FROM js8_links_ingest_state_v2
        """
    ).fetchall()
    return {
        str(row[0] or ""): {
            "directed_offset": int(row[1] or 0),
            "all_offset": int(row[2] or 0),
        }
        for row in rows
        if _text(row[0])
    }


def sync_js8_source_metadata(conn: sqlite3.Connection, sources: Iterable[JS8InstanceSource]) -> None:
    for source in sources:
        conn.execute(
            """
            UPDATE js8_messages_v2
               SET source_label=?, device_profile_id=?, inbox_path=?, directed_path=?, all_path=?
             WHERE source_key=?
            """,
            (
                source.source_label,
                source.device_profile_id,
                _text(source.inbox_path),
                _text(source.directed_path),
                _text(source.all_path),
                source.source_key,
            ),
        )
        if _table_exists(conn, "spotter_traffic"):
            conn.execute(
                """
                UPDATE spotter_traffic
                   SET source_label=?, device_profile_id=?, directed_path=?
                 WHERE source_key=?
                """,
                (
                    source.source_label,
                    source.device_profile_id,
                    _text(source.directed_path),
                    source.source_key,
                ),
            )
