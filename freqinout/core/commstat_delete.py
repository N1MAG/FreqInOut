from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from freqinout.core.commstat_artifacts import (
    ensure_commstat_artifact_deletion_tables,
    normalize_commstat_artifact_key,
    tombstone_commstat_artifact,
)
from freqinout.core.logger import log
from freqinout.core.sqlite_utils import connect_sqlite, table_exists


COMMSTAT_DELETE_SOURCE_TABLES = {"alerts", "messages", "statrep", "videos"}


@dataclass(frozen=True)
class CommStatSourceDeleteResult:
    result: str
    artifact_key: str = ""


def commstat_artifact_key(msg: Any) -> str:
    return normalize_commstat_artifact_key(getattr(msg, "artifact_key", ""))


def commstat_source_refs(msg: Any) -> list[tuple[str, int]]:
    try:
        raw_refs = json.loads(str(getattr(msg, "source_refs_json", "") or "[]"))
    except Exception:
        raw_refs = []
    if not isinstance(raw_refs, list):
        return []
    refs: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for raw in raw_refs:
        text = str(raw or "").strip()
        if ":" not in text:
            continue
        table, source_id_text = text.split(":", 1)
        table = table.strip().lower()
        if table not in COMMSTAT_DELETE_SOURCE_TABLES:
            continue
        try:
            source_id = int(str(source_id_text).strip())
        except Exception:
            continue
        if source_id <= 0:
            continue
        key = (table, source_id)
        if key in seen:
            continue
        seen.add(key)
        refs.append(key)
    return refs


def commstat_source_delete_targets(
    conn: sqlite3.Connection,
    refs: Sequence[tuple[str, int]],
) -> list[tuple[Path, str, int]]:
    if not refs or not table_exists(conn, "sitrep_source_events"):
        return []
    targets: list[tuple[Path, str, int]] = []
    seen: set[tuple[str, str, int]] = set()
    for table, source_id in refs:
        rows = conn.execute(
            """
            SELECT DISTINCT source_db_path
            FROM sitrep_source_events
            WHERE lower(source_table)=?
              AND source_id=?
              AND COALESCE(source_db_path, '') <> ''
            """,
            (table, int(source_id)),
        ).fetchall()
        valid_paths: list[Path] = []
        for row in rows:
            path = Path(str(row[0] or "")).expanduser()
            if not path.exists() or path.is_dir():
                continue
            valid_paths.append(path)
        if not valid_paths:
            return []
        for path in valid_paths:
            key = (str(path), table, int(source_id))
            if key in seen:
                continue
            seen.add(key)
            targets.append((path, table, int(source_id)))
    return targets


def delete_commstat_source_targets(targets: Sequence[tuple[Path, str, int]]) -> bool:
    if not targets:
        return False
    grouped: dict[Path, list[tuple[str, int]]] = {}
    for path, table, source_id in targets:
        table_txt = str(table or "").strip().lower()
        if table_txt not in COMMSTAT_DELETE_SOURCE_TABLES or int(source_id or 0) <= 0:
            return False
        grouped.setdefault(path, []).append((table_txt, int(source_id)))

    all_present, _any_present = commstat_source_target_presence(targets)
    if not all_present:
        return False

    for path, rows in grouped.items():
        with connect_sqlite(path, timeout=2.0) as source_conn:
            for table, _source_id in rows:
                if not table_exists(source_conn, table):
                    return False

    for path, rows in grouped.items():
        with connect_sqlite(path, timeout=2.0) as source_conn:
            for table, source_id in rows:
                result = source_conn.execute(f"DELETE FROM {table} WHERE id=?", (source_id,))
                if result.rowcount <= 0:
                    return False
            source_conn.commit()
    return True


def commstat_source_target_presence(targets: Sequence[tuple[Path, str, int]]) -> tuple[bool, bool]:
    if not targets:
        return (False, False)
    grouped: dict[Path, list[tuple[str, int]]] = {}
    total = 0
    for path, table, source_id in targets:
        table_txt = str(table or "").strip().lower()
        if table_txt not in COMMSTAT_DELETE_SOURCE_TABLES or int(source_id or 0) <= 0:
            return (False, False)
        grouped.setdefault(path, []).append((table_txt, int(source_id)))
        total += 1

    present = 0
    for path, rows in grouped.items():
        try:
            with connect_sqlite(path, timeout=2.0) as source_conn:
                for table, source_id in rows:
                    if not table_exists(source_conn, table):
                        continue
                    exists = source_conn.execute(
                        f"SELECT 1 FROM {table} WHERE id=? LIMIT 1",
                        (source_id,),
                    ).fetchone()
                    if exists:
                        present += 1
        except Exception:
            return (False, present > 0)
    return (present == total, present > 0)


def delete_commstat_local_projection(
    conn: sqlite3.Connection,
    *,
    artifact_key: str,
    refs: Sequence[tuple[str, int]] = (),
    tombstone: bool,
    msg: Any = None,
) -> bool:
    if tombstone:
        ensure_commstat_artifact_deletion_tables(conn)
        tombstoned = tombstone_commstat_artifact(
            conn,
            artifact_key=artifact_key,
            artifact_kind=getattr(msg, "artifact_kind", "") if msg else "",
            from_call=getattr(msg, "from_call", "") if msg else "",
            target=getattr(msg, "target", "") if msg else "",
            title=getattr(msg, "title", "") if msg else "",
            event_ts=getattr(msg, "event_ts", 0.0) if msg else 0.0,
            reason="message_viewer_delete",
        )
        if not tombstoned:
            return False
    conn.execute("DELETE FROM commstat_artifacts WHERE artifact_key=?", (artifact_key,))
    if refs and table_exists(conn, "sitrep_source_events"):
        conn.executemany(
            "DELETE FROM sitrep_source_events WHERE lower(source_table)=? AND source_id=?",
            [(table, int(source_id)) for table, source_id in refs],
        )
    return True


def delete_commstat_artifact(db_path: Path, msg: Any) -> CommStatSourceDeleteResult:
    artifact_key = commstat_artifact_key(msg)
    if not artifact_key:
        return CommStatSourceDeleteResult("skipped", "")
    try:
        with connect_sqlite(db_path, timeout=2.0) as conn:
            refs = commstat_source_refs(msg)
            targets = commstat_source_delete_targets(conn, refs)
            if refs and targets:
                all_present, any_present = commstat_source_target_presence(targets)
                if not all_present:
                    if any_present:
                        return CommStatSourceDeleteResult("failed", artifact_key)
                    if not delete_commstat_local_projection(
                        conn,
                        artifact_key=artifact_key,
                        refs=refs,
                        tombstone=False,
                        msg=msg,
                    ):
                        return CommStatSourceDeleteResult("failed", artifact_key)
                    conn.commit()
                    return CommStatSourceDeleteResult("deleted_projection", artifact_key)
                if not delete_commstat_source_targets(targets):
                    return CommStatSourceDeleteResult("failed", artifact_key)
                if not delete_commstat_local_projection(
                    conn,
                    artifact_key=artifact_key,
                    refs=refs,
                    tombstone=False,
                    msg=msg,
                ):
                    return CommStatSourceDeleteResult("failed", artifact_key)
                conn.commit()
                return CommStatSourceDeleteResult("deleted_source", artifact_key)
            if not delete_commstat_local_projection(
                conn,
                artifact_key=artifact_key,
                tombstone=True,
                msg=msg,
            ):
                return CommStatSourceDeleteResult("skipped", artifact_key)
            conn.commit()
        return CommStatSourceDeleteResult("hidden", artifact_key)
    except Exception as e:
        log.warning("CommStatDelete: failed to delete artifact %s: %s", artifact_key, e)
        return CommStatSourceDeleteResult("failed", artifact_key)
