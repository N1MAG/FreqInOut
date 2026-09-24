"""Additive schema and bounded read repository for Shortwave resources."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from freqinout.core.shortwave_models import (
    ShortwaveDataset,
    ShortwaveEntry,
    ShortwaveEntryCandidate,
    ShortwaveParseDiagnostic,
)
from freqinout.core.sqlite_utils import connect_sqlite, connect_sqlite_readonly, table_exists


SHORTWAVE_SCHEMA_VERSION = 2
SHORTWAVE_MAX_RESULTS = 200
SHORTWAVE_MAX_TIME_CANDIDATES = 1_000
SHORTWAVE_MAX_DIAGNOSTICS = 10_000


def ensure_shortwave_schema(conn: sqlite3.Connection) -> None:
    """Create the Qt-free, additive Shortwave schema in the caller transaction."""

    if not table_exists(conn, "resource_catalog_sources"):
        raise RuntimeError("canonical resource catalog must exist before Shortwave schema")
    conn.execute("PRAGMA foreign_keys=ON")
    statements = (
        """CREATE TABLE IF NOT EXISTS shortwave_datasets (
            dataset_key TEXT PRIMARY KEY NOT NULL,
            catalog_source_key TEXT NOT NULL REFERENCES resource_catalog_sources(source_key),
            provider_key TEXT NOT NULL,
            provider_label TEXT NOT NULL,
            season_code TEXT NOT NULL,
            publisher_updated_utc TEXT,
            season_effective_from_utc TEXT NOT NULL,
            season_effective_to_utc TEXT NOT NULL,
            source_uri TEXT NOT NULL,
            source_filename TEXT NOT NULL,
            csv_sha256 TEXT NOT NULL,
            readme_sha256 TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            encoding TEXT NOT NULL,
            record_count INTEGER NOT NULL,
            diagnostic_counts_json TEXT NOT NULL,
            diagnostics_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            imported_utc TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('staged','current','superseded','rejected'))
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_shortwave_one_current_provider
            ON shortwave_datasets(provider_key) WHERE state='current'""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_datasets_provider_state
            ON shortwave_datasets(provider_key, state, imported_utc DESC)""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_datasets_hash
            ON shortwave_datasets(provider_key, csv_sha256, readme_sha256)""",
        """CREATE TABLE IF NOT EXISTS shortwave_dataset_dictionaries (
            dataset_key TEXT NOT NULL REFERENCES shortwave_datasets(dataset_key) ON DELETE CASCADE,
            dictionary_kind TEXT NOT NULL,
            code TEXT NOT NULL,
            label TEXT NOT NULL,
            PRIMARY KEY(dataset_key, dictionary_kind, code)
        )""",
        """CREATE TABLE IF NOT EXISTS shortwave_entries (
            entry_key TEXT PRIMARY KEY NOT NULL,
            dataset_key TEXT NOT NULL REFERENCES shortwave_datasets(dataset_key) ON DELETE CASCADE,
            provider_identity_hash TEXT NOT NULL,
            source_line_number INTEGER NOT NULL,
            frequency_hz INTEGER NOT NULL,
            start_minute_utc INTEGER,
            end_minute_utc INTEGER,
            crosses_midnight INTEGER NOT NULL DEFAULT 0,
            raw_days TEXT NOT NULL,
            weekday_mask INTEGER,
            recurrence_json TEXT NOT NULL,
            parse_state TEXT NOT NULL CHECK(parse_state IN ('complete','special','review')),
            special_flags_json TEXT NOT NULL,
            station_name TEXT NOT NULL,
            station_home_code TEXT NOT NULL,
            language_raw TEXT NOT NULL,
            language_labels_json TEXT NOT NULL,
            signal_type TEXT,
            target_raw TEXT NOT NULL,
            target_labels_json TEXT NOT NULL,
            transmitter_raw TEXT NOT NULL,
            transmitter_labels_json TEXT NOT NULL,
            persistence_raw TEXT NOT NULL,
            inactive INTEGER NOT NULL DEFAULT 0,
            utility INTEGER NOT NULL DEFAULT 0,
            duplicate INTEGER NOT NULL DEFAULT 0,
            classification TEXT NOT NULL,
            start_date_raw TEXT NOT NULL,
            stop_date_raw TEXT NOT NULL,
            start_date_normalized TEXT,
            stop_date_normalized TEXT,
            last_heard_raw TEXT,
            raw_source_row TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            validation_state TEXT NOT NULL,
            diagnostics_json TEXT NOT NULL
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_shortwave_dataset_source_line
            ON shortwave_entries(dataset_key, source_line_number)""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_entries_frequency
            ON shortwave_entries(dataset_key, frequency_hz, parse_state, inactive, duplicate)""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_entries_time
            ON shortwave_entries(dataset_key, start_minute_utc, end_minute_utc, crosses_midnight, parse_state, inactive)""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_entries_station
            ON shortwave_entries(dataset_key, station_name COLLATE NOCASE)""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_entries_classification
            ON shortwave_entries(dataset_key, classification, parse_state, inactive)""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_entries_identity
            ON shortwave_entries(dataset_key, provider_identity_hash, content_hash)""",
        """CREATE TABLE IF NOT EXISTS shortwave_listening_reminders (
            reminder_key TEXT PRIMARY KEY NOT NULL,
            provider_key TEXT NOT NULL,
            accepted_dataset_key TEXT NOT NULL REFERENCES shortwave_datasets(dataset_key),
            accepted_entry_key TEXT NOT NULL REFERENCES shortwave_entries(entry_key),
            provider_identity_hash TEXT NOT NULL,
            accepted_snapshot_json TEXT NOT NULL,
            operator_label TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            receiver_profile_id INTEGER,
            lead_minutes INTEGER NOT NULL DEFAULT 15 CHECK(lead_minutes BETWEEN 0 AND 1440),
            enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
            source_review_state TEXT NOT NULL DEFAULT 'current'
                CHECK(source_review_state IN ('current','changed','missing','kept')),
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            UNIQUE(provider_key, provider_identity_hash)
        )""",
        """CREATE INDEX IF NOT EXISTS idx_shortwave_reminders_enabled
            ON shortwave_listening_reminders(enabled, provider_key, updated_utc DESC)""",
        """CREATE TABLE IF NOT EXISTS shortwave_listening_dismissals (
            reminder_key TEXT NOT NULL REFERENCES shortwave_listening_reminders(reminder_key) ON DELETE CASCADE,
            occurrence_key TEXT NOT NULL,
            occurrence_start_utc TEXT NOT NULL,
            dismissed_utc TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(reminder_key, occurrence_key)
        )""",
    )
    for statement in statements:
        conn.execute(statement)


def create_shortwave_schema(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite(path, row_factory=sqlite3.Row)
    try:
        with conn:
            ensure_shortwave_schema(conn)
    finally:
        conn.close()


class ShortwaveStore:
    """Bounded readers; schema creation and imports remain explicit."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    def create_schema(self) -> None:
        create_shortwave_schema(self.db_path)

    def _read(self) -> sqlite3.Connection | None:
        if not self.db_path.exists():
            return None
        try:
            return connect_sqlite_readonly(self.db_path, row_factory=sqlite3.Row)
        except sqlite3.Error:
            return None

    @contextmanager
    def write_connection(self) -> Iterable[sqlite3.Connection]:
        conn = connect_sqlite(self.db_path, row_factory=sqlite3.Row)
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def schema_available(self) -> bool:
        conn = self._read()
        if conn is None:
            return False
        try:
            return table_exists(conn, "shortwave_datasets") and table_exists(conn, "shortwave_entries")
        finally:
            conn.close()

    def current_dataset(self, provider_key: str = "eibi") -> ShortwaveDataset | None:
        return self._dataset_one(
            "SELECT * FROM shortwave_datasets WHERE provider_key=? AND state='current' LIMIT 1",
            (str(provider_key),),
        )

    def get_dataset(self, dataset_key: str) -> ShortwaveDataset | None:
        return self._dataset_one("SELECT * FROM shortwave_datasets WHERE dataset_key=?", (str(dataset_key),))

    def get_entry(self, entry_key: str, *, dataset_key: str | None = None) -> ShortwaveEntry | None:
        conn = self._read()
        if conn is None:
            return None
        try:
            if not table_exists(conn, "shortwave_entries"):
                return None
            if dataset_key:
                row = conn.execute(
                    "SELECT * FROM shortwave_entries WHERE entry_key=? AND dataset_key=? LIMIT 1",
                    (str(entry_key), str(dataset_key)),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM shortwave_entries WHERE entry_key=? LIMIT 1",
                    (str(entry_key),),
                ).fetchone()
            return _entry_from_row(row) if row is not None else None
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    def current_entry_by_identity(
        self,
        provider_identity_hash: str,
        *,
        provider_key: str = "eibi",
    ) -> ShortwaveEntry | None:
        conn = self._read()
        if conn is None:
            return None
        try:
            if not table_exists(conn, "shortwave_entries"):
                return None
            row = conn.execute(
                """SELECT entry.*
                   FROM shortwave_entries AS entry
                   JOIN shortwave_datasets AS dataset ON dataset.dataset_key=entry.dataset_key
                  WHERE dataset.provider_key=? AND dataset.state='current'
                    AND entry.provider_identity_hash=? AND entry.duplicate=0
                  ORDER BY entry.source_line_number LIMIT 1""",
                (str(provider_key), str(provider_identity_hash)),
            ).fetchone()
            return _entry_from_row(row) if row is not None else None
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    def current_entries_by_identities(
        self,
        provider_identity_hashes: Iterable[str],
        *,
        provider_key: str = "eibi",
        limit: int = SHORTWAVE_MAX_RESULTS,
    ) -> dict[str, ShortwaveEntry]:
        hashes = tuple(
            dict.fromkeys(str(value) for value in provider_identity_hashes if str(value))
        )[: _limit(limit)]
        if not hashes:
            return {}
        conn = self._read()
        if conn is None:
            return {}
        try:
            if not table_exists(conn, "shortwave_entries"):
                return {}
            placeholders = ",".join("?" for _ in hashes)
            rows = conn.execute(
                f"""SELECT entry.*
                    FROM shortwave_entries AS entry
                    JOIN shortwave_datasets AS dataset ON dataset.dataset_key=entry.dataset_key
                   WHERE dataset.provider_key=? AND dataset.state='current'
                     AND entry.provider_identity_hash IN ({placeholders}) AND entry.duplicate=0
                   ORDER BY entry.source_line_number""",
                (str(provider_key), *hashes),
            )
            result: dict[str, ShortwaveEntry] = {}
            for row in rows:
                entry = _entry_from_row(row)
                result.setdefault(entry.candidate.provider_identity_hash, entry)
            return result
        except sqlite3.Error:
            return {}
        finally:
            conn.close()

    def list_datasets(self, *, provider_key: str | None = None, limit: int = 50) -> tuple[ShortwaveDataset, ...]:
        conn = self._read()
        if conn is None:
            return ()
        try:
            if not table_exists(conn, "shortwave_datasets"):
                return ()
            clauses: list[str] = []
            params: list[Any] = []
            if provider_key:
                clauses.append("provider_key=?")
                params.append(str(provider_key))
            sql = "SELECT * FROM shortwave_datasets"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY imported_utc DESC, dataset_key LIMIT ?"
            params.append(_limit(limit, ceiling=200))
            return tuple(_dataset_from_row(row) for row in conn.execute(sql, params))
        except sqlite3.Error:
            return ()
        finally:
            conn.close()

    def dictionaries(self, dataset_key: str) -> dict[str, dict[str, str]]:
        conn = self._read()
        if conn is None:
            return {}
        try:
            if not table_exists(conn, "shortwave_dataset_dictionaries"):
                return {}
            result: dict[str, dict[str, str]] = {}
            rows = conn.execute(
                "SELECT dictionary_kind,code,label FROM shortwave_dataset_dictionaries WHERE dataset_key=? ORDER BY dictionary_kind,code",
                (str(dataset_key),),
            )
            for row in rows:
                result.setdefault(str(row["dictionary_kind"]), {})[str(row["code"])] = str(row["label"])
            return result
        except sqlite3.Error:
            return {}
        finally:
            conn.close()

    def dataset_diagnostics(
        self, dataset_key: str, *, limit: int = SHORTWAVE_MAX_DIAGNOSTICS
    ) -> tuple[ShortwaveParseDiagnostic, ...]:
        """Return stored import diagnostics through an explicit bounded API."""

        conn = self._read()
        if conn is None:
            return ()
        try:
            if not table_exists(conn, "shortwave_datasets"):
                return ()
            row = conn.execute(
                "SELECT diagnostics_json FROM shortwave_datasets WHERE dataset_key=? LIMIT 1",
                (str(dataset_key),),
            ).fetchone()
            if row is None:
                return ()
            payload = json.loads(str(row["diagnostics_json"] or "[]"))
            cap = _limit(limit, ceiling=SHORTWAVE_MAX_DIAGNOSTICS)
            results: list[ShortwaveParseDiagnostic] = []
            for item in payload[:cap] if isinstance(payload, list) else ():
                if not isinstance(item, dict):
                    continue
                try:
                    results.append(
                        ShortwaveParseDiagnostic(
                            line_number=int(item["line_number"]) if item.get("line_number") is not None else None,
                            severity=str(item.get("severity") or "warning"),
                            code=str(item.get("code") or "shortwave.diagnostic"),
                            message=str(item.get("message") or "Import diagnostic"),
                            raw_value=str(item.get("raw_value") or ""),
                        )
                    )
                except (TypeError, ValueError):
                    continue
            return tuple(results)
        except (json.JSONDecodeError, sqlite3.Error):
            return ()
        finally:
            conn.close()

    def list_entries(
        self,
        *,
        dataset_key: str | None = None,
        provider_key: str = "eibi",
        search: str = "",
        language: str = "",
        target: str = "",
        classifications: Sequence[str] = (),
        parse_states: Sequence[str] = (),
        include_inactive: bool = False,
        frequency_min_hz: int | None = None,
        frequency_max_hz: int | None = None,
        limit: int = SHORTWAVE_MAX_RESULTS,
        offset: int = 0,
    ) -> tuple[ShortwaveEntry, ...]:
        conn = self._read()
        if conn is None:
            return ()
        try:
            if not table_exists(conn, "shortwave_entries"):
                return ()
            resolved_dataset = dataset_key or self._current_dataset_key_in_connection(conn, provider_key)
            if not resolved_dataset:
                return ()
            clauses = ["dataset_key=?", "duplicate=0"]
            params: list[Any] = [resolved_dataset]
            if not include_inactive:
                clauses.append("inactive=0")
            if classifications:
                values = tuple(dict.fromkeys(str(value).strip() for value in classifications if str(value).strip()))[:16]
                if values:
                    _append_classification_filter(clauses, params, values)
            if parse_states:
                values = tuple(dict.fromkeys(str(value).strip() for value in parse_states if str(value).strip()))[:3]
                if values:
                    clauses.append("parse_state IN (%s)" % ",".join("?" for _ in values))
                    params.extend(values)
            if frequency_min_hz is not None:
                clauses.append("frequency_hz>=?")
                params.append(int(frequency_min_hz))
            if frequency_max_hz is not None:
                clauses.append("frequency_hz<=?")
                params.append(int(frequency_max_hz))
            term = str(search or "").strip().casefold()
            if term:
                like = f"%{_escape_like(term)}%"
                clauses.append(
                    "(lower(station_name) LIKE ? ESCAPE '\\' OR lower(language_labels_json) LIKE ? ESCAPE '\\' "
                    "OR lower(target_labels_json) LIKE ? ESCAPE '\\' OR lower(transmitter_labels_json) LIKE ? ESCAPE '\\' "
                    "OR CAST(frequency_hz AS TEXT) LIKE ? ESCAPE '\\')"
                )
                params.extend((like, like, like, like, like.replace(".", "")))
            language_term = str(language or "").strip().casefold()
            if language_term:
                clauses.append("lower(language_labels_json) LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(language_term)}%")
            target_term = str(target or "").strip().casefold()
            if target_term:
                clauses.append("lower(target_labels_json) LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(target_term)}%")
            params.extend((_limit(limit), max(0, int(offset))))
            sql = (
                "SELECT * FROM shortwave_entries WHERE " + " AND ".join(clauses)
                + " ORDER BY frequency_hz, start_minute_utc, station_name COLLATE NOCASE, source_line_number LIMIT ? OFFSET ?"
            )
            return tuple(_entry_from_row(row) for row in conn.execute(sql, params))
        except sqlite3.Error:
            return ()
        finally:
            conn.close()

    def list_time_candidates(
        self,
        dataset_key: str,
        *,
        start_minute: int,
        end_minute: int,
        search: str = "",
        language: str = "",
        target: str = "",
        classifications: Sequence[str] = (),
        frequency_hz: int | None = None,
        frequency_min_hz: int | None = None,
        frequency_max_hz: int | None = None,
        timing: str = "both",
        limit: int = SHORTWAVE_MAX_TIME_CANDIDATES,
    ) -> tuple[ShortwaveEntry, ...]:
        """Return indexed candidates; recurrence/date evaluation stays in service code."""

        conn = self._read()
        if conn is None:
            return ()
        try:
            if not table_exists(conn, "shortwave_entries"):
                return ()
            clauses = ["dataset_key=?", "inactive=0", "duplicate=0", "parse_state='complete'", "start_minute_utc IS NOT NULL"]
            params: list[Any] = [str(dataset_key)]
            start = max(0, min(1439, int(start_minute)))
            end = max(0, min(2879, int(end_minute)))
            timing_key = str(timing or "both").strip().lower()
            if timing_key not in {"now", "soon", "both"}:
                timing_key = "both"
            active_sql = (
                "((crosses_midnight=0 AND start_minute_utc<=? AND end_minute_utc>?) "
                "OR (crosses_midnight=1 AND (start_minute_utc<=? OR end_minute_utc>?)))"
            )
            if end < 1440:
                soon_sql = "(start_minute_utc>? AND start_minute_utc<=?)"
                if timing_key == "now":
                    clauses.append(active_sql)
                    params.extend((start, start, start, start))
                elif timing_key == "soon":
                    clauses.append(soon_sql)
                    params.extend((start, end))
                else:
                    clauses.append(f"({soon_sql} OR {active_sql})")
                    params.extend((start, end, start, start, start, start))
            else:
                next_day_end = end - 1440
                soon_sql = "(start_minute_utc>? OR start_minute_utc<=?)"
                if timing_key == "now":
                    clauses.append(active_sql)
                    params.extend((start, start, start, start))
                elif timing_key == "soon":
                    clauses.append(soon_sql)
                    params.extend((start, next_day_end))
                else:
                    clauses.append(f"({soon_sql} OR {active_sql})")
                    params.extend((start, next_day_end, start, start, start, start))
            if classifications:
                values = tuple(dict.fromkeys(str(value).strip() for value in classifications if str(value).strip()))[:16]
                if values:
                    _append_classification_filter(clauses, params, values)
            if frequency_hz is not None:
                clauses.append("frequency_hz=?")
                params.append(int(frequency_hz))
            else:
                if frequency_min_hz is not None:
                    clauses.append("frequency_hz>=?")
                    params.append(int(frequency_min_hz))
                if frequency_max_hz is not None:
                    clauses.append("frequency_hz<=?")
                    params.append(int(frequency_max_hz))
            term = str(search or "").strip().casefold()
            if term:
                like = f"%{_escape_like(term)}%"
                clauses.append(
                    "(lower(station_name) LIKE ? ESCAPE '\\' OR lower(station_home_code) LIKE ? ESCAPE '\\' "
                    "OR lower(language_labels_json) LIKE ? ESCAPE '\\' OR lower(target_labels_json) LIKE ? ESCAPE '\\' "
                    "OR lower(transmitter_labels_json) LIKE ? ESCAPE '\\')"
                )
                params.extend((like, like, like, like, like))
            language_term = str(language or "").strip().casefold()
            if language_term:
                clauses.append("lower(language_labels_json) LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(language_term)}%")
            target_term = str(target or "").strip().casefold()
            if target_term:
                clauses.append("lower(target_labels_json) LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(target_term)}%")
            params.append(_limit(limit, ceiling=SHORTWAVE_MAX_TIME_CANDIDATES))
            sql = (
                "SELECT * FROM shortwave_entries WHERE " + " AND ".join(clauses)
                + " ORDER BY start_minute_utc, frequency_hz, source_line_number LIMIT ?"
            )
            return tuple(_entry_from_row(row) for row in conn.execute(sql, params))
        except sqlite3.Error:
            return ()
        finally:
            conn.close()

    def _dataset_one(self, sql: str, params: Sequence[Any]) -> ShortwaveDataset | None:
        conn = self._read()
        if conn is None:
            return None
        try:
            if not table_exists(conn, "shortwave_datasets"):
                return None
            row = conn.execute(sql, tuple(params)).fetchone()
            return _dataset_from_row(row) if row else None
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    @staticmethod
    def _current_dataset_key_in_connection(conn: sqlite3.Connection, provider_key: str) -> str | None:
        row = conn.execute(
            "SELECT dataset_key FROM shortwave_datasets WHERE provider_key=? AND state='current' LIMIT 1",
            (str(provider_key),),
        ).fetchone()
        return str(row[0]) if row else None


def _append_classification_filter(
    clauses: list[str], params: list[Any], values: Sequence[str]
) -> None:
    """Add the operator-facing classification union, including review rows."""

    normalized = tuple(dict.fromkeys(str(value).strip().lower() for value in values if str(value).strip()))
    include_other = "other" in normalized
    concrete = tuple(value for value in normalized if value != "other")
    if include_other and concrete:
        clauses.append(
            "(classification IN (%s) OR parse_state!='complete')"
            % ",".join("?" for _ in concrete)
        )
        params.extend(concrete)
    elif include_other:
        clauses.append("parse_state!='complete'")
    elif concrete:
        clauses.append("classification IN (%s)" % ",".join("?" for _ in concrete))
        params.extend(concrete)


def _dataset_from_row(row: sqlite3.Row) -> ShortwaveDataset:
    return ShortwaveDataset(
        dataset_key=str(row["dataset_key"]),
        catalog_source_key=str(row["catalog_source_key"]),
        provider_key=str(row["provider_key"]),
        provider_label=str(row["provider_label"]),
        season_code=str(row["season_code"]),
        publisher_updated_utc=row["publisher_updated_utc"],
        season_effective_from_utc=str(row["season_effective_from_utc"]),
        season_effective_to_utc=str(row["season_effective_to_utc"]),
        source_uri=str(row["source_uri"]),
        source_filename=str(row["source_filename"]),
        csv_sha256=str(row["csv_sha256"]),
        readme_sha256=str(row["readme_sha256"]),
        parser_version=str(row["parser_version"]),
        encoding=str(row["encoding"]),
        record_count=int(row["record_count"]),
        diagnostic_counts=_json_mapping(row["diagnostic_counts_json"]),
        imported_utc=str(row["imported_utc"]),
        state=str(row["state"]),
        metadata=_json_mapping(row["metadata_json"]),
    )


def _entry_from_row(row: sqlite3.Row) -> ShortwaveEntry:
    diagnostics = tuple(
        ShortwaveParseDiagnostic(
            item.get("line_number"), str(item.get("severity") or "warning"),
            str(item.get("code") or "stored"), str(item.get("message") or ""), str(item.get("raw_value") or ""),
        )
        for item in _json_list(row["diagnostics_json"])
        if isinstance(item, dict)
    )
    candidate = ShortwaveEntryCandidate(
        provider_identity_hash=str(row["provider_identity_hash"]),
        source_line_number=int(row["source_line_number"]),
        frequency_hz=int(row["frequency_hz"]),
        start_minute_utc=row["start_minute_utc"],
        end_minute_utc=row["end_minute_utc"],
        crosses_midnight=bool(row["crosses_midnight"]),
        raw_days=str(row["raw_days"]),
        weekday_mask=row["weekday_mask"],
        recurrence=_json_mapping(row["recurrence_json"]),
        parse_state=str(row["parse_state"]),
        special_flags=tuple(str(value) for value in _json_list(row["special_flags_json"])),
        station_name=str(row["station_name"]),
        station_home_code=str(row["station_home_code"]),
        language_raw=str(row["language_raw"]),
        language_labels=tuple(str(value) for value in _json_list(row["language_labels_json"])),
        signal_type=row["signal_type"],
        target_raw=str(row["target_raw"]),
        target_labels=tuple(str(value) for value in _json_list(row["target_labels_json"])),
        transmitter_raw=str(row["transmitter_raw"]),
        transmitter_labels=tuple(str(value) for value in _json_list(row["transmitter_labels_json"])),
        persistence_raw=str(row["persistence_raw"]),
        inactive=bool(row["inactive"]),
        utility=bool(row["utility"]),
        duplicate=bool(row["duplicate"]),
        classification=str(row["classification"]),
        start_date_raw=str(row["start_date_raw"]),
        stop_date_raw=str(row["stop_date_raw"]),
        start_date_normalized=row["start_date_normalized"],
        stop_date_normalized=row["stop_date_normalized"],
        last_heard_raw=row["last_heard_raw"],
        raw_source_row=str(row["raw_source_row"]),
        content_hash=str(row["content_hash"]),
        validation_state=str(row["validation_state"]),
        diagnostics=diagnostics,
    )
    return ShortwaveEntry(str(row["entry_key"]), str(row["dataset_key"]), candidate)


def _json_mapping(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
        return dict(parsed) if isinstance(parsed, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _json_list(value: Any) -> list[Any]:
    try:
        parsed = json.loads(str(value or "[]"))
        return list(parsed) if isinstance(parsed, list) else []
    except (TypeError, ValueError, json.JSONDecodeError):
        return []


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _limit(value: int, *, ceiling: int = SHORTWAVE_MAX_RESULTS) -> int:
    try:
        return max(1, min(int(ceiling), int(value)))
    except (TypeError, ValueError):
        return min(SHORTWAVE_MAX_RESULTS, int(ceiling))


__all__ = [
    "SHORTWAVE_MAX_RESULTS",
    "SHORTWAVE_MAX_TIME_CANDIDATES",
    "SHORTWAVE_SCHEMA_VERSION",
    "ShortwaveStore",
    "create_shortwave_schema",
    "ensure_shortwave_schema",
]
