"""Conservative plans for repairing legacy duplicate message presentations.

The planner is read-only.  It recognizes only identities that the current
projection contract proves equivalent: known JS8/VarAC source-id migrations,
or file receipts with the same family, protocol filename, and SHA-256 content
digest.  Applying a plan is owned by the serialized projection writer.
"""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable

from freqinout.core.message_canonical_identity import canonical_station_message_id


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FILE_KINDS = {"bbs_file", "flamp_file", "flmsg_file", "varac_file"}


@dataclass(frozen=True)
class ProjectionMerge:
    target_message_id: str
    duplicate_message_id: str
    reason: str


def plan_legacy_projection_merges(conn: sqlite3.Connection) -> tuple[ProjectionMerge, ...]:
    """Return deterministic, source-preserving convergence work."""

    preferred: set[str] = set()
    edges: list[tuple[str, str, str]] = []
    for target, duplicate, reason in _native_alias_edges(conn):
        preferred.add(target)
        edges.append((target, duplicate, reason))
    file_edges, file_preferred = _file_identity_edges(conn)
    preferred.update(file_preferred)
    edges.extend(file_edges)
    if not edges:
        return ()

    parent: dict[str, str] = {}

    def find(value: str) -> str:
        parent.setdefault(value, value)
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[max(a, b)] = min(a, b)

    reasons: dict[frozenset[str], set[str]] = defaultdict(set)
    for target, duplicate, reason in edges:
        if not target or not duplicate or target == duplicate:
            continue
        union(target, duplicate)
        reasons[frozenset((target, duplicate))].add(reason)

    versions = {
        str(row[0]): int(row[1] or 0)
        for row in conn.execute(
            "SELECT message_id, projection_version FROM message_projection"
        ).fetchall()
    }
    components: dict[str, set[str]] = defaultdict(set)
    for message_id in parent:
        components[find(message_id)].add(message_id)

    planned: list[ProjectionMerge] = []
    for members in components.values():
        available = {value for value in members if value in versions}
        if len(available) < 2:
            continue
        target = min(
            available,
            key=lambda value: (
                0 if value in preferred else 1,
                -versions.get(value, 0),
                value,
            ),
        )
        component_reasons = sorted(
            {
                reason
                for pair, values in reasons.items()
                if pair.issubset(available)
                for reason in values
            }
        )
        reason = "+".join(component_reasons) or "legacy-canonical-equivalence"
        planned.extend(
            ProjectionMerge(target, duplicate, reason)
            for duplicate in sorted(available)
            if duplicate != target
        )
    return tuple(
        sorted(
            planned,
            key=lambda item: (item.target_message_id, item.duplicate_message_id),
        )
    )


def _native_alias_edges(conn: sqlite3.Connection) -> Iterable[tuple[str, str, str]]:
    # JS8 v4 inserted the receipt class into the source id.  The old and new
    # references name the same native row when kind/key and the remaining
    # suffix match exactly.
    rows = conn.execute(
        """
        SELECT current.message_id, legacy.message_id
          FROM message_external_refs legacy
          JOIN message_external_refs current
            ON current.external_kind=legacy.external_kind
           AND current.external_key=legacy.external_key
           AND current.source_id IN (
               'js8:api:' || SUBSTR(legacy.source_id,5),
               'js8:directed_txt:' || SUBSTR(legacy.source_id,5),
               'js8:inbox_db:' || SUBSTR(legacy.source_id,5),
               'js8:file:' || SUBSTR(legacy.source_id,5)
           )
          JOIN message_projection old_message
            ON old_message.message_id=legacy.message_id
          JOIN message_projection new_message
            ON new_message.message_id=current.message_id
         WHERE legacy.external_kind='js8_message'
           AND legacy.source_id LIKE 'js8:%'
           AND legacy.source_id NOT LIKE 'js8:api:%'
           AND legacy.source_id NOT LIKE 'js8:directed_txt:%'
           AND legacy.source_id NOT LIKE 'js8:inbox_db:%'
           AND legacy.source_id NOT LIKE 'js8:file:%'
           AND legacy.message_id<>current.message_id
           AND old_message.source_family='js8'
           AND new_message.source_family='js8'
           AND new_message.projection_version>old_message.projection_version
        """
    ).fetchall()
    yield from _fully_relinkable_alias_edges(conn, rows, "js8-source-alias")

    # Earlier JS8 projections used the native row id as both receipt key and,
    # for unqualified rows, part of the source id.  Current projection uses the
    # durable ``source_id`` column when present and classifies the source as
    # API/DIRECTED.TXT/inbox.  Join through the authoritative native row so
    # that this key migration is proven rather than guessed from display text.
    js8_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='js8_messages'"
    ).fetchone()
    if js8_table is not None:
        rows = conn.execute(
            """
            SELECT current.message_id, legacy.message_id
              FROM js8_messages msg
              JOIN message_external_refs current
                ON current.external_kind='js8_message'
               AND current.external_key=CAST(COALESCE(msg.source_id,msg.id) AS TEXT)
               AND current.source_id=(
                   'js8:' || CASE
                     WHEN COALESCE(msg.source_key,'')<>'' THEN
                       CASE
                         WHEN LOWER(msg.source_key || ' ' || COALESCE(msg.source_path,'')) LIKE '%directed%'
                           THEN 'directed_txt'
                         WHEN LOWER(msg.source_key || ' ' || COALESCE(msg.source_path,'')) LIKE '%inbox%'
                           OR LOWER(msg.source_key || ' ' || COALESCE(msg.source_path,'')) LIKE '%.db3%'
                           THEN 'inbox_db'
                         WHEN LOWER(msg.source_key) LIKE '%api%'
                           OR COALESCE(msg.source_path,'')=''
                           THEN 'api'
                         ELSE 'file'
                       END || ':' || msg.source_key
                     WHEN COALESCE(msg.source_path,'')<>'' THEN
                       CASE
                         WHEN LOWER(msg.source_path) LIKE '%directed%' THEN 'directed_txt'
                         WHEN LOWER(msg.source_path) LIKE '%inbox%'
                           OR LOWER(msg.source_path) LIKE '%.db3%' THEN 'inbox_db'
                         ELSE 'file'
                       END || ':legacy:' || msg.source_path
                     ELSE 'api:' || COALESCE(NULLIF(msg.js8_instance_id,''),'legacy')
                   END
               )
              JOIN message_external_refs legacy
                ON legacy.external_kind='js8_message'
               AND legacy.external_key=CAST(msg.id AS TEXT)
               AND legacy.source_id IN (
                   'js8:' || COALESCE(NULLIF(msg.source_key,''),CAST(msg.id AS TEXT)),
                   'js8:' || CAST(msg.id AS TEXT),
                   'js8:legacy'
               )
              JOIN message_projection old_message
                ON old_message.message_id=legacy.message_id
              JOIN message_projection new_message
                ON new_message.message_id=current.message_id
             WHERE legacy.message_id<>current.message_id
               AND old_message.source_family='js8'
               AND new_message.source_family='js8'
               AND new_message.projection_version>old_message.projection_version
            """
        ).fetchall()
        yield from _fully_relinkable_alias_edges(conn, rows, "js8-native-key-alias")

    # VarAC v4 appended the mailbox/source class (for example ``:vmail``) to
    # the endpoint identity.  Match only the same durable native key.
    rows = conn.execute(
        """
        SELECT current.message_id, legacy.message_id
          FROM message_external_refs legacy
          JOIN message_external_refs current
            ON current.external_kind=legacy.external_kind
           AND current.external_key=legacy.external_key
           AND current.source_id LIKE legacy.source_id || ':%'
          JOIN message_projection old_message
            ON old_message.message_id=legacy.message_id
          JOIN message_projection new_message
            ON new_message.message_id=current.message_id
         WHERE legacy.external_kind='varac_message'
           AND legacy.source_id LIKE 'varac:%'
           AND legacy.message_id<>current.message_id
           AND old_message.source_family='varac'
           AND new_message.source_family='varac'
           AND new_message.projection_version>old_message.projection_version
        """
    ).fetchall()
    yield from _fully_relinkable_alias_edges(conn, rows, "varac-source-alias")


def _fully_relinkable_alias_edges(
    conn: sqlite3.Connection,
    rows: Iterable[object],
    reason: str,
) -> Iterable[tuple[str, str, str]]:
    """Yield only old rows whose complete receipt set has one proven target."""

    matches: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        target = str(row[0] or "")
        duplicate = str(row[1] or "")
        if target and duplicate and target != duplicate:
            matches[duplicate].append(target)
    if not matches:
        return
    marks = ",".join("?" for _ in matches)
    counts = {
        str(row[0]): int(row[1] or 0)
        for row in conn.execute(
            f"""
            SELECT message_id, COUNT(*)
              FROM message_external_refs
             WHERE message_id IN ({marks})
             GROUP BY message_id
            """,
            tuple(matches),
        ).fetchall()
    }
    for duplicate, targets in matches.items():
        unique_targets = set(targets)
        if len(unique_targets) != 1 or len(targets) != counts.get(duplicate, 0):
            continue
        yield next(iter(unique_targets)), duplicate, reason


def _file_identity_edges(
    conn: sqlite3.Connection,
) -> tuple[list[tuple[str, str, str]], set[str]]:
    rows = conn.execute(
        """
        SELECT projection.message_id, projection.source_family,
               projection.projection_version, ref.external_kind,
               ref.external_path, ref.external_hash,
               artifact.path AS artifact_path,
               artifact.content_hash AS artifact_hash
          FROM message_projection projection
          JOIN message_external_refs ref
            ON ref.message_id=projection.message_id
          LEFT JOIN message_artifacts artifact
            ON artifact.message_id=projection.message_id
           AND COALESCE(artifact.source_id,'')=ref.source_id
           AND COALESCE(artifact.external_key,'')=ref.external_key
         WHERE ref.external_kind IN ('bbs_file','flamp_file','flmsg_file','varac_file')
        """
    ).fetchall()
    grouped: dict[tuple[str, str, str, str], set[str]] = defaultdict(set)
    versions: dict[str, int] = {}
    canonical_candidates: dict[tuple[str, str, str, str], str] = {}
    for row in rows:
        external_kind = str(row[3] or "").strip().lower()
        if external_kind not in _FILE_KINDS:
            continue
        family = str(row[1] or "").strip().lower()
        path = str(row[4] or row[6] or "").replace("\\", "/")
        filename = PurePosixPath(path).name.casefold()
        digest = _sha256(row[5]) or _sha256(row[7])
        if not family or not filename or not digest:
            continue
        key = (family, external_kind, filename, digest)
        message_id = str(row[0] or "")
        grouped[key].add(message_id)
        versions[message_id] = max(versions.get(message_id, 0), int(row[2] or 0))
        canonical_candidates[key] = canonical_station_message_id(
            family,
            durable_id=f"{filename}:{digest}",
            payload_digest=digest,
        )

    edges: list[tuple[str, str, str]] = []
    preferred: set[str] = set()
    for key, message_ids in grouped.items():
        if len(message_ids) < 2:
            continue
        canonical = canonical_candidates[key]
        if canonical in message_ids:
            target = canonical
            preferred.add(target)
        else:
            target = min(
                message_ids,
                key=lambda value: (-versions.get(value, 0), value),
            )
        edges.extend(
            (target, duplicate, "file-content-identity")
            for duplicate in message_ids
            if duplicate != target
        )
    return edges, preferred


def _sha256(value: object) -> str:
    text = str(value or "").strip().lower()
    return text if _SHA256_RE.fullmatch(text) else ""


__all__ = ["ProjectionMerge", "plan_legacy_projection_merges"]
