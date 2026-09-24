"""Transactional, cancellable Shortwave dataset preview and promotion."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from freqinout.core.shortwave_models import (
    ShortwaveDatasetCandidate,
    ShortwaveEntryCandidate,
    ShortwaveImportCancelled,
    ShortwaveImportPreview,
    ShortwaveImportResult,
    ShortwaveParseDiagnostic,
    StaleShortwavePreviewError,
)
from freqinout.core.shortwave_store import ShortwaveStore, ensure_shortwave_schema
from freqinout.core.sqlite_utils import connect_sqlite_readonly, table_exists


CancelCheck = Callable[[], bool]


def preview_shortwave_import(store: ShortwaveStore, candidate: ShortwaveDatasetCandidate) -> ShortwaveImportPreview:
    """Compare one fully parsed candidate with the immutable current dataset."""

    current = store.current_dataset(candidate.provider_key)
    current_rows: dict[str, str] = {}
    if current is not None and store.db_path.exists():
        conn = connect_sqlite_readonly(store.db_path, row_factory=sqlite3.Row)
        try:
            if table_exists(conn, "shortwave_entries"):
                current_rows = {
                    str(row["provider_identity_hash"]): str(row["content_hash"])
                    for row in conn.execute(
                        "SELECT provider_identity_hash,content_hash FROM shortwave_entries "
                        "WHERE dataset_key=? AND duplicate=0",
                        (current.dataset_key,),
                    )
                }
        finally:
            conn.close()
    incoming_rows = {
        entry.provider_identity_hash: entry.content_hash
        for entry in candidate.entries
        if not entry.duplicate
    }
    new_keys = incoming_rows.keys() - current_rows.keys()
    removed_keys = current_rows.keys() - incoming_rows.keys()
    shared = incoming_rows.keys() & current_rows.keys()
    changed = sum(incoming_rows[key] != current_rows[key] for key in shared)
    unchanged = len(shared) - changed
    fatal: list[str] = []
    if not candidate.entries:
        fatal.append("The provider file contains no importable rows.")
    if not candidate.season_effective_from_utc or not candidate.season_effective_to_utc:
        fatal.append("Authoritative provider season bounds are required.")
    reminder_changed, reminder_missing = _preview_reminder_impact(store, candidate.provider_key, incoming_rows)
    return ShortwaveImportPreview(
        candidate=candidate,
        expected_current_dataset_key=current.dataset_key if current else None,
        new_count=len(new_keys),
        changed_count=changed,
        unchanged_count=unchanged,
        removed_count=len(removed_keys),
        duplicate_count=sum(entry.duplicate for entry in candidate.entries),
        invalid_count=sum(item.severity == "error" for item in candidate.diagnostics),
        special_count=sum(entry.parse_state != "complete" for entry in candidate.entries),
        inactive_count=sum(entry.inactive for entry in candidate.entries),
        fatal_errors=tuple(fatal),
        reminder_changed_count=reminder_changed,
        reminder_missing_count=reminder_missing,
    )


def _preview_reminder_impact(
    store: ShortwaveStore,
    provider_key: str,
    incoming_rows: dict[str, str],
) -> tuple[int, int]:
    if not store.db_path.exists():
        return 0, 0
    conn = connect_sqlite_readonly(store.db_path, row_factory=sqlite3.Row)
    try:
        if not table_exists(conn, "shortwave_listening_reminders"):
            return 0, 0
        changed = missing = 0
        for row in conn.execute(
            """SELECT provider_identity_hash,accepted_snapshot_json
                 FROM shortwave_listening_reminders
                WHERE provider_key=? ORDER BY reminder_key LIMIT 1000""",
            (str(provider_key),),
        ):
            current_hash = incoming_rows.get(str(row["provider_identity_hash"]))
            if current_hash is None:
                missing += 1
                continue
            try:
                accepted_hash = str(json.loads(str(row["accepted_snapshot_json"])).get("content_hash") or "")
            except (TypeError, ValueError, json.JSONDecodeError):
                accepted_hash = ""
            if current_hash != accepted_hash:
                changed += 1
        return changed, missing
    except sqlite3.Error:
        return 0, 0
    finally:
        conn.close()


def apply_shortwave_import(
    store: ShortwaveStore,
    preview: ShortwaveImportPreview,
    *,
    cancel_check: CancelCheck | None = None,
    fail_after_entries: int | None = None,
) -> ShortwaveImportResult:
    """Stage all rows and atomically move the provider's current pointer."""

    if not preview.actionable:
        raise ValueError("Shortwave import preview is not actionable: " + "; ".join(preview.fatal_errors))
    candidate = preview.candidate
    dataset_key = _dataset_key(candidate)
    now = _utc_now()
    inserted = 0
    with store.write_connection() as conn:
        ensure_shortwave_schema(conn)
        current_row = conn.execute(
            "SELECT dataset_key,csv_sha256,readme_sha256,season_code,"
            "season_effective_from_utc,season_effective_to_utc,parser_version FROM shortwave_datasets "
            "WHERE provider_key=? AND state='current' LIMIT 1",
            (candidate.provider_key,),
        ).fetchone()
        current_key = str(current_row["dataset_key"]) if current_row else None
        if current_key != preview.expected_current_dataset_key:
            raise StaleShortwavePreviewError("The current Shortwave dataset changed after preview.")
        if current_row and (
            str(current_row["csv_sha256"]) == candidate.csv_sha256
            and str(current_row["readme_sha256"]) == candidate.readme_sha256
            and str(current_row["season_code"]) == candidate.season_code
            and str(current_row["season_effective_from_utc"]) == candidate.season_effective_from_utc
            and str(current_row["season_effective_to_utc"]) == candidate.season_effective_to_utc
            and str(current_row["parser_version"]) == candidate.parser_version
        ):
            return ShortwaveImportResult("unchanged", current_key, current_key, 0)
        _raise_if_cancelled(cancel_check)
        _upsert_catalog_source(conn, candidate, now)
        existing = conn.execute(
            "SELECT dataset_key,state FROM shortwave_datasets WHERE dataset_key=?",
            (dataset_key,),
        ).fetchone()
        if existing is None:
            counts = Counter(item.severity for item in candidate.diagnostics)
            conn.execute(
                """INSERT INTO shortwave_datasets(
                    dataset_key,catalog_source_key,provider_key,provider_label,season_code,
                    publisher_updated_utc,season_effective_from_utc,season_effective_to_utc,
                    source_uri,source_filename,csv_sha256,readme_sha256,parser_version,encoding,
                    record_count,diagnostic_counts_json,diagnostics_json,metadata_json,imported_utc,state
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'staged')""",
                (
                    dataset_key, candidate.catalog_source_key, candidate.provider_key, candidate.provider_label,
                    candidate.season_code, candidate.publisher_updated_utc, candidate.season_effective_from_utc,
                    candidate.season_effective_to_utc, candidate.source_uri, candidate.source_filename,
                    candidate.csv_sha256, candidate.readme_sha256, candidate.parser_version, candidate.encoding,
                    len(candidate.entries), _json(dict(counts)),
                    _json([_diagnostic_payload(item) for item in candidate.diagnostics]),
                    _json(dict(candidate.metadata)), now,
                ),
            )
            for kind, values in candidate.dictionaries.items():
                for code, label in values.items():
                    conn.execute(
                        "INSERT INTO shortwave_dataset_dictionaries(dataset_key,dictionary_kind,code,label) VALUES(?,?,?,?)",
                        (dataset_key, str(kind), str(code), str(label)),
                    )
            for index, entry in enumerate(candidate.entries):
                if index % 100 == 0:
                    _raise_if_cancelled(cancel_check)
                _insert_entry(conn, dataset_key, entry)
                inserted += 1
                if fail_after_entries is not None and inserted >= int(fail_after_entries):
                    raise RuntimeError("injected Shortwave import failure")
        _raise_if_cancelled(cancel_check)
        conn.execute(
            "UPDATE shortwave_datasets SET state='superseded' WHERE provider_key=? AND state='current'",
            (candidate.provider_key,),
        )
        conn.execute("UPDATE shortwave_datasets SET state='current' WHERE dataset_key=?", (dataset_key,))
        _refresh_reminder_source_states(conn, candidate.provider_key, now)
    return ShortwaveImportResult("promoted", dataset_key, current_key, inserted)


def rollback_shortwave_dataset(store: ShortwaveStore, dataset_key: str) -> ShortwaveImportResult:
    """Explicitly restore a retained immutable dataset for its provider."""

    with store.write_connection() as conn:
        ensure_shortwave_schema(conn)
        selected = conn.execute(
            "SELECT provider_key,state FROM shortwave_datasets WHERE dataset_key=?",
            (str(dataset_key),),
        ).fetchone()
        if selected is None:
            raise ValueError("Shortwave dataset is unavailable.")
        provider_key = str(selected["provider_key"])
        current = conn.execute(
            "SELECT dataset_key FROM shortwave_datasets WHERE provider_key=? AND state='current'",
            (provider_key,),
        ).fetchone()
        current_key = str(current["dataset_key"]) if current else None
        if current_key == str(dataset_key):
            return ShortwaveImportResult("unchanged", current_key, current_key, 0)
        conn.execute(
            "UPDATE shortwave_datasets SET state='superseded' WHERE provider_key=? AND state='current'",
            (provider_key,),
        )
        conn.execute("UPDATE shortwave_datasets SET state='current' WHERE dataset_key=?", (str(dataset_key),))
        _refresh_reminder_source_states(conn, provider_key, _utc_now())
    return ShortwaveImportResult("rolled_back", str(dataset_key), current_key, 0)


def _insert_entry(conn: sqlite3.Connection, dataset_key: str, entry: ShortwaveEntryCandidate) -> None:
    entry_key = f"{dataset_key}:line:{entry.source_line_number}"
    conn.execute(
        """INSERT INTO shortwave_entries(
            entry_key,dataset_key,provider_identity_hash,source_line_number,frequency_hz,
            start_minute_utc,end_minute_utc,crosses_midnight,raw_days,weekday_mask,
            recurrence_json,parse_state,special_flags_json,station_name,station_home_code,
            language_raw,language_labels_json,signal_type,target_raw,target_labels_json,
            transmitter_raw,transmitter_labels_json,persistence_raw,inactive,utility,duplicate,
            classification,start_date_raw,stop_date_raw,start_date_normalized,stop_date_normalized,
            last_heard_raw,raw_source_row,content_hash,validation_state,diagnostics_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            entry_key, dataset_key, entry.provider_identity_hash, entry.source_line_number,
            entry.frequency_hz, entry.start_minute_utc, entry.end_minute_utc,
            int(entry.crosses_midnight), entry.raw_days, entry.weekday_mask,
            _json(dict(entry.recurrence)), entry.parse_state, _json(list(entry.special_flags)),
            entry.station_name, entry.station_home_code, entry.language_raw,
            _json(list(entry.language_labels)), entry.signal_type, entry.target_raw,
            _json(list(entry.target_labels)), entry.transmitter_raw,
            _json(list(entry.transmitter_labels)), entry.persistence_raw, int(entry.inactive),
            int(entry.utility), int(entry.duplicate), entry.classification, entry.start_date_raw,
            entry.stop_date_raw, entry.start_date_normalized, entry.stop_date_normalized,
            entry.last_heard_raw, entry.raw_source_row, entry.content_hash,
            entry.validation_state, _json([_diagnostic_payload(item) for item in entry.diagnostics]),
        ),
    )


def _upsert_catalog_source(conn: sqlite3.Connection, candidate: ShortwaveDatasetCandidate, now: str) -> None:
    source_content = {
        "provider": candidate.provider_key,
        "season": candidate.season_code,
        "csv_sha256": candidate.csv_sha256,
        "readme_sha256": candidate.readme_sha256,
    }
    content_hash = hashlib.sha256(_json(source_content).encode("utf-8")).hexdigest()
    conn.execute(
        """INSERT INTO resource_catalog_sources(
            source_key,label,source_kind,jurisdiction,version,effective_date,source_uri,
            last_verified_utc,content_hash,read_only,enabled,created_utc,updated_utc
        ) VALUES(?,?, 'bundled',NULL,?,?,?,?,?,1,1,?,?)
        ON CONFLICT(source_key) DO UPDATE SET
            label=excluded.label,version=excluded.version,effective_date=excluded.effective_date,
            source_uri=excluded.source_uri,last_verified_utc=excluded.last_verified_utc,
            content_hash=excluded.content_hash,enabled=1,updated_utc=excluded.updated_utc""",
        (
            candidate.catalog_source_key, candidate.provider_label, candidate.season_code,
            candidate.season_effective_from_utc, candidate.source_uri, candidate.publisher_updated_utc,
            content_hash, now, now,
        ),
    )


def _refresh_reminder_source_states(conn: sqlite3.Connection, provider_key: str, now: str) -> None:
    """Atomically mark saved snapshots after a source pointer switch.

    The accepted snapshot JSON is never changed here.  This bounded station
    inventory pass makes update health immediately correct for both Listening
    and Ops Center without a per-reminder connection or UI-triggered repair.
    """

    if not table_exists(conn, "shortwave_listening_reminders"):
        return
    current_rows = {
        str(row["provider_identity_hash"]): str(row["content_hash"])
        for row in conn.execute(
            """SELECT entry.provider_identity_hash,entry.content_hash
                 FROM shortwave_entries AS entry
                 JOIN shortwave_datasets AS dataset ON dataset.dataset_key=entry.dataset_key
                WHERE dataset.provider_key=? AND dataset.state='current' AND entry.duplicate=0
                ORDER BY entry.source_line_number""",
            (str(provider_key),),
        )
    }
    updates: list[tuple[str, str, str]] = []
    rows = conn.execute(
        """SELECT reminder_key,provider_identity_hash,accepted_snapshot_json,source_review_state
             FROM shortwave_listening_reminders WHERE provider_key=?
             ORDER BY reminder_key LIMIT 1000""",
        (str(provider_key),),
    )
    for row in rows:
        identity = str(row["provider_identity_hash"])
        current_hash = current_rows.get(identity)
        try:
            accepted_hash = str(json.loads(str(row["accepted_snapshot_json"])).get("content_hash") or "")
        except (TypeError, ValueError, json.JSONDecodeError):
            accepted_hash = ""
        state = "missing" if current_hash is None else ("current" if current_hash == accepted_hash else "changed")
        if str(row["source_review_state"]) == "kept" and state in {"changed", "missing"}:
            state = "kept"
        updates.append((state, now, str(row["reminder_key"])))
    if updates:
        conn.executemany(
            "UPDATE shortwave_listening_reminders SET source_review_state=?,updated_utc=? WHERE reminder_key=?",
            updates,
        )


def _dataset_key(candidate: ShortwaveDatasetCandidate) -> str:
    provider = re.sub(r"[^a-z0-9]+", "_", candidate.provider_key.casefold()).strip("_") or "provider"
    season = re.sub(r"[^a-z0-9]+", "_", candidate.season_code.casefold()).strip("_") or "season"
    return f"shortwave_{provider}_{season}_{candidate.csv_sha256[:20]}"


def _raise_if_cancelled(cancel_check: CancelCheck | None) -> None:
    if cancel_check is not None and bool(cancel_check()):
        raise ShortwaveImportCancelled("Shortwave import cancelled before promotion.")


def _diagnostic_payload(item: ShortwaveParseDiagnostic) -> dict[str, Any]:
    return {
        "line_number": item.line_number,
        "severity": item.severity,
        "code": item.code,
        "message": item.message,
        "raw_value": item.raw_value,
    }


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


__all__ = [
    "apply_shortwave_import",
    "preview_shortwave_import",
    "rollback_shortwave_dataset",
]
