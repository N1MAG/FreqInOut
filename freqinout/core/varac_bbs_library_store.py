from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log
from freqinout.core.sqlite_utils import connect_sqlite

BBS_LIBRARY_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class BbsLibraryManifestRow:
    artifact_id: str
    source_path: str
    display_name: str
    live_name: str
    size: int
    mtime_ns: int
    content_hash: str = ""
    q_id: str = ""
    block_id: str = ""
    metadata: Mapping[str, object] | None = None


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def default_bbs_library_db_path() -> Path:
    return get_config_dir() / "config" / "freqinout.db"


def bbs_library_db_path_from_settings(settings: object) -> Path:
    for attr in ("db_path", "_config_path"):
        value = getattr(settings, attr, None)
        if value:
            return Path(value)
    fallback = getattr(settings, "fallback_settings", None)
    for attr in ("db_path", "_config_path"):
        value = getattr(fallback, attr, None)
        if value:
            return Path(value)
    return default_bbs_library_db_path()


def stable_bbs_artifact_id(*parts: object) -> str:
    text = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def upsert_bbs_artifact_path(
    conn: sqlite3.Connection,
    *,
    source_path: object,
    source_kind: object = "operator_file",
    source_id: object = "",
    display_name: object = "",
    metadata: Mapping[str, object] | None = None,
) -> str:
    ensure_bbs_library_schema(conn)
    path = Path(str(source_path or "")).expanduser()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(str(path))
    stat = path.stat()
    resolved = str(path.resolve())
    artifact_id = stable_bbs_artifact_id("file", resolved)
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO bbs_artifacts(
            artifact_id, source_kind, source_id, source_path, display_name,
            size, mtime_ns, content_hash, q_id, block_id, metadata_json,
            deleted, created_utc, updated_utc
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, '', '', '', ?, 0, ?, ?)
        ON CONFLICT(artifact_id) DO UPDATE SET
            source_kind=excluded.source_kind,
            source_id=excluded.source_id,
            source_path=excluded.source_path,
            display_name=excluded.display_name,
            size=excluded.size,
            mtime_ns=excluded.mtime_ns,
            metadata_json=excluded.metadata_json,
            deleted=0,
            updated_utc=excluded.updated_utc
        """,
        (
            artifact_id,
            str(source_kind or "operator_file").strip() or "operator_file",
            str(source_id or "").strip(),
            resolved,
            str(display_name or path.name).strip() or path.name,
            int(stat.st_size or 0),
            int(stat.st_mtime_ns or 0),
            _json(metadata or {}),
            now,
            now,
        ),
    )
    return artifact_id


def upsert_bbs_location(
    conn: sqlite3.Connection,
    *,
    location_id: object,
    name: object,
    source_dir: object = "",
    enabled: bool = True,
    metadata: Mapping[str, object] | None = None,
) -> str:
    ensure_bbs_library_schema(conn)
    location_key = str(location_id or "").strip()
    if not location_key:
        raise ValueError("location_id is required")
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO bbs_locations(location_id, name, source_dir, enabled, metadata_json, updated_utc)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_id) DO UPDATE SET
            name=excluded.name,
            source_dir=excluded.source_dir,
            enabled=excluded.enabled,
            metadata_json=excluded.metadata_json,
            updated_utc=excluded.updated_utc
        """,
        (
            location_key,
            str(name or location_key).strip() or location_key,
            str(source_dir or "").strip(),
            1 if enabled else 0,
            _json(metadata or {}),
            now,
        ),
    )
    return location_key


def set_bbs_location_artifact(
    conn: sqlite3.Connection,
    *,
    location_id: object,
    artifact_id: object,
    live_name: object = "",
    sort_order: int = 0,
    visibility_rule: object = "public",
    retention_class: object = "normal",
    publish_enabled: bool = True,
) -> None:
    ensure_bbs_library_schema(conn)
    location_key = str(location_id or "").strip()
    artifact_key = str(artifact_id or "").strip()
    if not location_key or not artifact_key:
        raise ValueError("location_id and artifact_id are required")
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO bbs_location_artifacts(
            location_id, artifact_id, live_name, sort_order, visibility_rule,
            retention_class, publish_enabled, created_utc, updated_utc
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_id, artifact_id) DO UPDATE SET
            live_name=excluded.live_name,
            sort_order=excluded.sort_order,
            visibility_rule=excluded.visibility_rule,
            retention_class=excluded.retention_class,
            publish_enabled=excluded.publish_enabled,
            updated_utc=excluded.updated_utc
        """,
        (
            location_key,
            artifact_key,
            str(live_name or "").strip(),
            int(sort_order or 0),
            str(visibility_rule or "public").strip() or "public",
            str(retention_class or "normal").strip() or "normal",
            1 if publish_enabled else 0,
            now,
            now,
        ),
    )


def _json(value: object, default: str = "{}") -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except Exception:
        return default


def ensure_bbs_library_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bbs_library_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bbs_artifacts (
            artifact_id TEXT PRIMARY KEY,
            source_kind TEXT NOT NULL,
            source_id TEXT,
            source_path TEXT UNIQUE,
            display_name TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            mtime_ns INTEGER NOT NULL DEFAULT 0,
            content_hash TEXT,
            q_id TEXT,
            block_id TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            deleted INTEGER NOT NULL DEFAULT 0,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bbs_locations (
            location_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            source_dir TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            updated_utc TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bbs_location_artifacts (
            location_id TEXT NOT NULL,
            artifact_id TEXT NOT NULL,
            live_name TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            visibility_rule TEXT NOT NULL DEFAULT 'public',
            retention_class TEXT NOT NULL DEFAULT 'normal',
            publish_enabled INTEGER NOT NULL DEFAULT 1,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            PRIMARY KEY(location_id, artifact_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bbs_artifacts_path ON bbs_artifacts(source_path)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bbs_artifacts_qid ON bbs_artifacts(q_id, block_id)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_bbs_location_artifacts_publish "
        "ON bbs_location_artifacts(location_id, publish_enabled, sort_order)"
    )
    cur.execute(
        "INSERT OR REPLACE INTO bbs_library_meta(key, value) VALUES('schema_version', ?)",
        (str(BBS_LIBRARY_SCHEMA_VERSION),),
    )


def ensure_bbs_library_db(db_path: str | Path | None = None) -> Path:
    path = Path(db_path) if db_path else default_bbs_library_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with connect_sqlite(path) as conn:
        ensure_bbs_library_schema(conn)
    return path


def sync_bbs_location_from_folder(
    conn: sqlite3.Connection,
    *,
    location_id: object,
    name: object,
    source_dir: object,
    enabled: bool = True,
    metadata: Mapping[str, object] | None = None,
) -> int:
    ensure_bbs_library_schema(conn)
    location_key = str(location_id or "").strip()
    if not location_key:
        return 0
    source_path = Path(str(source_dir or "")).expanduser()
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO bbs_locations(location_id, name, source_dir, enabled, metadata_json, updated_utc)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_id) DO UPDATE SET
            name=excluded.name,
            source_dir=excluded.source_dir,
            enabled=excluded.enabled,
            metadata_json=excluded.metadata_json,
            updated_utc=excluded.updated_utc
        """,
        (
            location_key,
            str(name or location_key).strip() or location_key,
            str(source_path),
            1 if enabled else 0,
            _json(metadata or {}),
            now,
        ),
    )
    conn.execute(
        """
        DELETE FROM bbs_location_artifacts
        WHERE location_id=?
          AND artifact_id IN (
              SELECT artifact_id FROM bbs_artifacts
              WHERE source_kind='managed_location_file' AND source_id=?
          )
        """,
        (location_key, location_key),
    )
    if not enabled or not source_path.exists() or not source_path.is_dir():
        return 0

    added = 0
    for order, child in enumerate(sorted(source_path.iterdir(), key=lambda item: item.name.lower()), start=1):
        if not child.is_file():
            continue
        try:
            stat = child.stat()
        except OSError:
            continue
        resolved = str(child.resolve())
        artifact_id = stable_bbs_artifact_id("file", resolved)
        conn.execute(
            """
            INSERT INTO bbs_artifacts(
                artifact_id, source_kind, source_id, source_path, display_name,
                size, mtime_ns, content_hash, q_id, block_id, metadata_json,
                deleted, created_utc, updated_utc
            )
            VALUES(?, 'managed_location_file', ?, ?, ?, ?, ?, '', '', '', '{}', 0, ?, ?)
            ON CONFLICT(artifact_id) DO UPDATE SET
                source_id=excluded.source_id,
                source_path=excluded.source_path,
                display_name=excluded.display_name,
                size=excluded.size,
                mtime_ns=excluded.mtime_ns,
                deleted=0,
                updated_utc=excluded.updated_utc
            """,
            (
                artifact_id,
                location_key,
                resolved,
                child.name,
                int(stat.st_size or 0),
                int(stat.st_mtime_ns or 0),
                now,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO bbs_location_artifacts(
                location_id, artifact_id, live_name, sort_order, visibility_rule,
                retention_class, publish_enabled, created_utc, updated_utc
            )
            VALUES(?, ?, '', ?, 'public', 'normal', 1, ?, ?)
            ON CONFLICT(location_id, artifact_id) DO UPDATE SET
                sort_order=excluded.sort_order,
                publish_enabled=1,
                updated_utc=excluded.updated_utc
            """,
            (location_key, artifact_id, order, now, now),
        )
        added += 1
    return added


def sync_bbs_locations_from_folders(
    db_path: str | Path,
    locations: Sequence[Mapping[str, object]],
) -> int:
    path = ensure_bbs_library_db(db_path)
    synced = 0
    with connect_sqlite(path) as conn:
        ensure_bbs_library_schema(conn)
        with conn:
            for location in locations:
                synced += sync_bbs_location_from_folder(
                    conn,
                    location_id=location.get("id"),
                    name=location.get("name"),
                    source_dir=location.get("source_dir"),
                    enabled=bool(location.get("enabled", True)),
                    metadata=location,
                )
    return synced


def location_has_bbs_catalog(conn: sqlite3.Connection, location_id: object) -> bool:
    ensure_bbs_library_schema(conn)
    key = str(location_id or "").strip()
    if not key:
        return False
    row = conn.execute("SELECT 1 FROM bbs_locations WHERE location_id=? LIMIT 1", (key,)).fetchone()
    return bool(row)


def bbs_location_catalog_source_dir(conn: sqlite3.Connection, location_id: object) -> str:
    ensure_bbs_library_schema(conn)
    key = str(location_id or "").strip()
    if not key:
        return ""
    row = conn.execute("SELECT source_dir FROM bbs_locations WHERE location_id=? LIMIT 1", (key,)).fetchone()
    return str(row[0] or "").strip() if row else ""


def list_bbs_location_manifest_rows(
    conn: sqlite3.Connection,
    location_id: object,
    *,
    include_disabled: bool = False,
) -> list[BbsLibraryManifestRow]:
    ensure_bbs_library_schema(conn)
    key = str(location_id or "").strip()
    if not key:
        return []
    enabled_clause = "" if include_disabled else "AND la.publish_enabled=1 AND a.deleted=0"
    rows = conn.execute(
        f"""
        SELECT
            a.artifact_id,
            a.source_path,
            a.display_name,
            COALESCE(NULLIF(la.live_name, ''), a.display_name) AS live_name,
            a.size,
            a.mtime_ns,
            a.content_hash,
            a.q_id,
            a.block_id,
            a.metadata_json
        FROM bbs_location_artifacts la
        JOIN bbs_artifacts a ON a.artifact_id = la.artifact_id
        WHERE la.location_id=?
          {enabled_clause}
          AND COALESCE(a.source_path, '') != ''
        ORDER BY la.sort_order ASC, live_name COLLATE NOCASE ASC, a.artifact_id ASC
        """,
        (key,),
    ).fetchall()
    result: list[BbsLibraryManifestRow] = []
    for row in rows:
        try:
            metadata = json.loads(row[9] or "{}")
        except Exception:
            metadata = {}
        result.append(
            BbsLibraryManifestRow(
                artifact_id=str(row[0] or ""),
                source_path=str(row[1] or ""),
                display_name=str(row[2] or ""),
                live_name=str(row[3] or ""),
                size=int(row[4] or 0),
                mtime_ns=int(row[5] or 0),
                content_hash=str(row[6] or ""),
                q_id=str(row[7] or ""),
                block_id=str(row[8] or ""),
                metadata=metadata if isinstance(metadata, Mapping) else {},
            )
        )
    return result


def log_bbs_library_sync_failure(exc: Exception) -> None:
    log.debug("varac_bbs_library: DB-backed manifest sync unavailable: %s", exc)
