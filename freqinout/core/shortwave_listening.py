"""Receive-only Shortwave listening reminders and bounded Ops projections.

This module deliberately contains no scheduler, launcher, CAT, QSY, PTT, SOP,
or Operating Group dependency.  A reminder is an immutable accepted listing
snapshot plus station-owned presentation preferences.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from freqinout.core.shortwave_models import ShortwaveDataset, ShortwaveEntry
from freqinout.core.shortwave_store import ShortwaveStore
from freqinout.core.sqlite_utils import connect_sqlite_readonly, table_exists


MAX_LISTENING_REMINDERS = 200
MAX_LISTENING_OCCURRENCES = 50
SOURCE_TYPE_SHORTWAVE_LISTENING = "SHORTWAVE_LISTENING"


@dataclass(frozen=True, slots=True)
class AcceptedShortwaveSnapshot:
    schema_version: int
    provider_key: str
    provider_label: str
    dataset_key: str
    entry_key: str
    provider_identity_hash: str
    content_hash: str
    season_code: str
    season_effective_from_utc: str
    season_effective_to_utc: str
    source_uri: str
    station_name: str
    station_home_code: str
    frequency_hz: int
    start_minute_utc: int
    end_minute_utc: int
    crosses_midnight: bool
    raw_days: str
    weekday_mask: int
    start_date_normalized: str | None
    stop_date_normalized: str | None
    signal_type: str
    language_labels: tuple[str, ...]
    target_labels: tuple[str, ...]
    transmitter_labels: tuple[str, ...]
    classification: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "language_labels", tuple(self.language_labels))
        object.__setattr__(self, "target_labels", tuple(self.target_labels))
        object.__setattr__(self, "transmitter_labels", tuple(self.transmitter_labels))
        if self.frequency_hz <= 0 or not 0 <= self.start_minute_utc <= 1439:
            raise ValueError("accepted Shortwave snapshot has invalid timing/frequency")
        if not 0 <= self.end_minute_utc <= 1440 or self.weekday_mask <= 0:
            raise ValueError("accepted Shortwave snapshot has invalid recurrence")


@dataclass(frozen=True, slots=True)
class ShortwaveListeningReminder:
    reminder_key: str
    snapshot: AcceptedShortwaveSnapshot
    operator_label: str
    notes: str
    receiver_profile_id: int | None
    lead_minutes: int
    enabled: bool
    source_review_state: str
    created_utc: str
    updated_utc: str


@dataclass(frozen=True, slots=True)
class ShortwaveSourceReview:
    reminder_key: str
    state: str
    changed_fields: tuple[str, ...]
    accepted: AcceptedShortwaveSnapshot
    current: AcceptedShortwaveSnapshot | None


@dataclass(frozen=True, slots=True)
class ShortwaveListeningOccurrence:
    occurrence_key: str
    reminder_key: str
    source_type: str
    commandable: bool
    name: str
    station_name: str
    frequency_hz: int
    signal_type: str
    start_utc: datetime
    end_utc: datetime
    reminder_minutes: int
    receiver_profile_id: int | None
    source_health: str
    source_health_text: str
    urgency: str
    urgency_text: str
    dismissed: bool = False

    def __post_init__(self) -> None:
        if self.source_type != SOURCE_TYPE_SHORTWAVE_LISTENING or self.commandable:
            raise ValueError("Shortwave listening occurrences are receive-only reminders")
        if self.start_utc.tzinfo is None or self.end_utc.tzinfo is None:
            raise ValueError("Shortwave occurrence timestamps must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ShortwaveListeningOutlook:
    generated_utc: datetime
    active: ShortwaveListeningOccurrence | None
    next: ShortwaveListeningOccurrence | None
    later: tuple[ShortwaveListeningOccurrence, ...]
    recent: tuple[ShortwaveListeningOccurrence, ...]
    total_upcoming: int
    attention_count: int

    @property
    def visible_items(self) -> tuple[ShortwaveListeningOccurrence, ...]:
        rows: list[ShortwaveListeningOccurrence] = list(self.recent)
        if self.active is not None:
            rows.append(self.active)
        if self.next is not None:
            rows.append(self.next)
        rows.extend(self.later)
        return tuple(dict((row.occurrence_key, row) for row in rows).values())


class ShortwaveListeningStore:
    """Small station-owned reminder repository over the Shortwave database."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.shortwave = ShortwaveStore(self.db_path)

    def add_from_entry(
        self,
        entry_key: str,
        *,
        dataset_key: str | None = None,
        operator_label: str = "",
        notes: str = "",
        receiver_profile_id: int | None = None,
        lead_minutes: int = 15,
    ) -> ShortwaveListeningReminder:
        entry = self.shortwave.get_entry(entry_key, dataset_key=dataset_key)
        if entry is None:
            raise KeyError(f"Shortwave listing not found: {entry_key}")
        dataset = self.shortwave.get_dataset(entry.dataset_key)
        if dataset is None:
            raise KeyError(f"Shortwave dataset not found: {entry.dataset_key}")
        snapshot = _snapshot_from_entry(entry, dataset)
        now = _utc_text(datetime.now(timezone.utc))
        reminder_key = f"shortwave_reminder_{uuid4().hex}"
        with self.shortwave.write_connection() as conn:
            existing = conn.execute(
                """SELECT reminder_key FROM shortwave_listening_reminders
                    WHERE provider_key=? AND provider_identity_hash=? LIMIT 1""",
                (snapshot.provider_key, snapshot.provider_identity_hash),
            ).fetchone()
            if existing is not None:
                reminder_key = str(existing["reminder_key"])
            else:
                conn.execute(
                    """INSERT INTO shortwave_listening_reminders(
                           reminder_key,provider_key,accepted_dataset_key,accepted_entry_key,
                           provider_identity_hash,accepted_snapshot_json,operator_label,notes,
                           receiver_profile_id,lead_minutes,enabled,source_review_state,created_utc,updated_utc
                       ) VALUES(?,?,?,?,?,?,?,?,?,?,1,'current',?,?)""",
                    (
                        reminder_key, snapshot.provider_key, snapshot.dataset_key, snapshot.entry_key,
                        snapshot.provider_identity_hash, _snapshot_json(snapshot), str(operator_label).strip(),
                        str(notes).strip(), receiver_profile_id, _lead(lead_minutes), now, now,
                    ),
                )
        reminder = self.get(reminder_key)
        if reminder is None:
            raise RuntimeError("Shortwave reminder was not persisted")
        return reminder

    def get(self, reminder_key: str) -> ShortwaveListeningReminder | None:
        rows = self._read_rows(
            "SELECT * FROM shortwave_listening_reminders WHERE reminder_key=? LIMIT 1",
            (str(reminder_key),),
        )
        return _reminder_from_row(rows[0]) if rows else None

    def list_reminders(self, *, enabled: bool | None = None, limit: int = MAX_LISTENING_REMINDERS) -> tuple[ShortwaveListeningReminder, ...]:
        clauses: list[str] = []
        params: list[Any] = []
        if enabled is not None:
            clauses.append("enabled=?")
            params.append(1 if enabled else 0)
        sql = "SELECT * FROM shortwave_listening_reminders"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY enabled DESC, operator_label COLLATE NOCASE, updated_utc DESC LIMIT ?"
        params.append(max(1, min(MAX_LISTENING_REMINDERS, int(limit))))
        return tuple(_reminder_from_row(row) for row in self._read_rows(sql, tuple(params)))

    def update(
        self,
        reminder_key: str,
        *,
        operator_label: str,
        notes: str,
        receiver_profile_id: int | None,
        lead_minutes: int,
        enabled: bool,
    ) -> ShortwaveListeningReminder:
        now = _utc_text(datetime.now(timezone.utc))
        with self.shortwave.write_connection() as conn:
            changed = conn.execute(
                """UPDATE shortwave_listening_reminders
                      SET operator_label=?, notes=?, receiver_profile_id=?, lead_minutes=?, enabled=?, updated_utc=?
                    WHERE reminder_key=?""",
                (
                    str(operator_label).strip(), str(notes).strip(), receiver_profile_id,
                    _lead(lead_minutes), 1 if enabled else 0, now, str(reminder_key),
                ),
            ).rowcount
            if not changed:
                raise KeyError(f"Shortwave reminder not found: {reminder_key}")
        result = self.get(reminder_key)
        if result is None:
            raise RuntimeError("Shortwave reminder update was not readable")
        return result

    def remove(self, reminder_key: str) -> bool:
        with self.shortwave.write_connection() as conn:
            return bool(conn.execute(
                "DELETE FROM shortwave_listening_reminders WHERE reminder_key=?",
                (str(reminder_key),),
            ).rowcount)

    def refresh_source_review_states(self) -> dict[str, int]:
        """Compare accepted snapshots with current source rows without mutating them."""
        reminders = self.list_reminders(limit=MAX_LISTENING_REMINDERS)
        counts = {"current": 0, "changed": 0, "missing": 0, "kept": 0}
        updates: list[tuple[str, str]] = []
        by_provider: dict[str, list[ShortwaveListeningReminder]] = {}
        for reminder in reminders:
            by_provider.setdefault(reminder.snapshot.provider_key, []).append(reminder)
        current_by_reminder: dict[str, ShortwaveEntry] = {}
        for provider_key, provider_reminders in by_provider.items():
            current_rows = self.shortwave.current_entries_by_identities(
                (row.snapshot.provider_identity_hash for row in provider_reminders),
                provider_key=provider_key,
                limit=MAX_LISTENING_REMINDERS,
            )
            for reminder in provider_reminders:
                current = current_rows.get(reminder.snapshot.provider_identity_hash)
                if current is not None:
                    current_by_reminder[reminder.reminder_key] = current
        for reminder in reminders:
            current = current_by_reminder.get(reminder.reminder_key)
            state = "missing" if current is None else (
                "current" if current.candidate.content_hash == reminder.snapshot.content_hash else "changed"
            )
            if reminder.source_review_state == "kept" and state in {"changed", "missing"}:
                state = "kept"
            counts[state] += 1
            if state != reminder.source_review_state:
                updates.append((state, reminder.reminder_key))
        if updates:
            now = _utc_text(datetime.now(timezone.utc))
            with self.shortwave.write_connection() as conn:
                conn.executemany(
                    "UPDATE shortwave_listening_reminders SET source_review_state=?, updated_utc=? WHERE reminder_key=?",
                    ((state, now, key) for state, key in updates),
                )
        return counts

    def source_review(self, reminder_key: str) -> ShortwaveSourceReview:
        reminder = self.get(reminder_key)
        if reminder is None:
            raise KeyError(f"Shortwave reminder not found: {reminder_key}")
        entry = self.shortwave.current_entry_by_identity(
            reminder.snapshot.provider_identity_hash,
            provider_key=reminder.snapshot.provider_key,
        )
        if entry is None:
            return ShortwaveSourceReview(reminder.reminder_key, "missing", (), reminder.snapshot, None)
        dataset = self.shortwave.get_dataset(entry.dataset_key)
        if dataset is None:
            return ShortwaveSourceReview(reminder.reminder_key, "missing", (), reminder.snapshot, None)
        current = _snapshot_from_entry(entry, dataset)
        changed_fields = tuple(
            label for field_name, label in (
                ("station_name", "station"), ("frequency_hz", "frequency"),
                ("start_minute_utc", "start time"), ("end_minute_utc", "end time"),
                ("crosses_midnight", "cross-midnight"), ("raw_days", "days"),
                ("start_date_normalized", "start date"), ("stop_date_normalized", "stop date"),
                ("signal_type", "mode / signal"), ("language_labels", "language"),
                ("target_labels", "target"), ("transmitter_labels", "transmitter site"),
                ("classification", "classification"), ("season_code", "source season"),
            )
            if getattr(reminder.snapshot, field_name) != getattr(current, field_name)
        )
        state = "current" if not changed_fields and current.content_hash == reminder.snapshot.content_hash else "changed"
        if reminder.source_review_state == "kept" and state == "changed":
            state = "kept"
        return ShortwaveSourceReview(reminder.reminder_key, state, changed_fields, reminder.snapshot, current)

    def keep_accepted_snapshot(self, reminder_key: str) -> ShortwaveListeningReminder:
        with self.shortwave.write_connection() as conn:
            changed = conn.execute(
                "UPDATE shortwave_listening_reminders SET source_review_state='kept', updated_utc=? WHERE reminder_key=?",
                (_utc_text(datetime.now(timezone.utc)), str(reminder_key)),
            ).rowcount
            if not changed:
                raise KeyError(f"Shortwave reminder not found: {reminder_key}")
        result = self.get(reminder_key)
        assert result is not None
        return result

    def apply_current_listing(self, reminder_key: str) -> ShortwaveListeningReminder:
        reminder = self.get(reminder_key)
        if reminder is None:
            raise KeyError(f"Shortwave reminder not found: {reminder_key}")
        entry = self.shortwave.current_entry_by_identity(
            reminder.snapshot.provider_identity_hash,
            provider_key=reminder.snapshot.provider_key,
        )
        if entry is None:
            raise KeyError("The current source no longer contains this listing")
        dataset = self.shortwave.get_dataset(entry.dataset_key)
        if dataset is None:
            raise KeyError("The current source dataset is unavailable")
        snapshot = _snapshot_from_entry(entry, dataset)
        with self.shortwave.write_connection() as conn:
            conn.execute(
                """UPDATE shortwave_listening_reminders
                      SET accepted_dataset_key=?, accepted_entry_key=?, provider_identity_hash=?,
                          accepted_snapshot_json=?, source_review_state='current', updated_utc=?
                    WHERE reminder_key=?""",
                (
                    snapshot.dataset_key, snapshot.entry_key, snapshot.provider_identity_hash,
                    _snapshot_json(snapshot), _utc_text(datetime.now(timezone.utc)), str(reminder_key),
                ),
            )
        result = self.get(reminder_key)
        assert result is not None
        return result

    def dismiss_occurrence(self, reminder_key: str, occurrence_key: str, start_utc: datetime, *, note: str = "") -> None:
        start = _as_utc(start_utc)
        with self.shortwave.write_connection() as conn:
            if conn.execute(
                "SELECT 1 FROM shortwave_listening_reminders WHERE reminder_key=?",
                (str(reminder_key),),
            ).fetchone() is None:
                raise KeyError(f"Shortwave reminder not found: {reminder_key}")
            conn.execute(
                """INSERT INTO shortwave_listening_dismissals(
                       reminder_key,occurrence_key,occurrence_start_utc,dismissed_utc,note
                   ) VALUES(?,?,?,?,?)
                   ON CONFLICT(reminder_key,occurrence_key) DO UPDATE SET
                       dismissed_utc=excluded.dismissed_utc,note=excluded.note""",
                (
                    str(reminder_key), str(occurrence_key), _utc_text(start),
                    _utc_text(datetime.now(timezone.utc)), str(note).strip(),
                ),
            )

    def dismissed_keys(self, reminder_keys: Iterable[str]) -> frozenset[tuple[str, str]]:
        keys = tuple(dict.fromkeys(str(value) for value in reminder_keys if str(value)))[:MAX_LISTENING_REMINDERS]
        if not keys:
            return frozenset()
        marks = ",".join("?" for _ in keys)
        rows = self._read_rows(
            f"SELECT reminder_key,occurrence_key FROM shortwave_listening_dismissals WHERE reminder_key IN ({marks})",
            keys,
        )
        return frozenset((str(row["reminder_key"]), str(row["occurrence_key"])) for row in rows)

    def _read_rows(self, sql: str, params: tuple[Any, ...]) -> tuple[sqlite3.Row, ...]:
        if not self.db_path.exists():
            return ()
        try:
            conn = connect_sqlite_readonly(self.db_path, row_factory=sqlite3.Row)
        except sqlite3.Error:
            return ()
        try:
            if not table_exists(conn, "shortwave_listening_reminders"):
                return ()
            return tuple(conn.execute(sql, params))
        except sqlite3.Error:
            return ()
        finally:
            conn.close()


def build_shortwave_listening_outlook(
    store: ShortwaveListeningStore,
    now_utc: datetime | None = None,
    *,
    horizon_days: int = 30,
    later_limit: int = MAX_LISTENING_OCCURRENCES,
    recent_minutes: int = 30,
) -> ShortwaveListeningOutlook:
    """Project a bounded reminder outlook from accepted snapshots only."""
    now = _as_utc(now_utc or datetime.now(timezone.utc))
    reminders = store.list_reminders(enabled=True, limit=MAX_LISTENING_REMINDERS)
    dismissed = store.dismissed_keys(row.reminder_key for row in reminders)
    horizon = now + timedelta(days=max(1, min(90, int(horizon_days))))
    earliest = now - timedelta(minutes=max(0, min(120, int(recent_minutes))))
    rows: list[ShortwaveListeningOccurrence] = []
    for reminder in reminders:
        # Keep projection fair across reminders.  A daily listing must not fill
        # the entire bounded candidate set before other saved stations are seen.
        for start, end in _snapshot_occurrences(reminder.snapshot, earliest, horizon, limit=6):
            occurrence_key = _occurrence_key(reminder.reminder_key, start)
            if (reminder.reminder_key, occurrence_key) in dismissed:
                continue
            state, urgency = _urgency(start, end, now, reminder.lead_minutes)
            health_text = {
                "current": "Accepted listing matches the current source.",
                "changed": "The current source changed; review before applying it.",
                "missing": "The current source no longer contains this listing.",
                "kept": "You chose to keep this accepted listing snapshot.",
            }.get(reminder.source_review_state, "Source status needs review.")
            rows.append(
                ShortwaveListeningOccurrence(
                    occurrence_key=occurrence_key,
                    reminder_key=reminder.reminder_key,
                    source_type=SOURCE_TYPE_SHORTWAVE_LISTENING,
                    commandable=False,
                    name=reminder.operator_label or reminder.snapshot.station_name,
                    station_name=reminder.snapshot.station_name,
                    frequency_hz=reminder.snapshot.frequency_hz,
                    signal_type=reminder.snapshot.signal_type,
                    start_utc=start,
                    end_utc=end,
                    reminder_minutes=reminder.lead_minutes,
                    receiver_profile_id=reminder.receiver_profile_id,
                    source_health=reminder.source_review_state,
                    source_health_text=health_text,
                    urgency=state,
                    urgency_text=urgency,
                )
            )
    rows.sort(key=lambda item: (item.start_utc, item.reminder_key))
    recent = tuple(item for item in rows if item.end_utc <= now)[-5:]
    active_rows = tuple(item for item in rows if item.start_utc <= now < item.end_utc)
    future = tuple(item for item in rows if item.start_utc > now)
    limit = max(0, min(MAX_LISTENING_OCCURRENCES, int(later_limit)))
    remaining = (*active_rows[1:], *future[1:])
    return ShortwaveListeningOutlook(
        generated_utc=now,
        active=active_rows[0] if active_rows else None,
        next=future[0] if future else None,
        later=tuple(remaining[:limit]),
        recent=recent,
        total_upcoming=len(future),
        attention_count=sum(row.source_review_state in {"changed", "missing"} for row in reminders),
    )


def _snapshot_from_entry(entry: ShortwaveEntry, dataset: ShortwaveDataset) -> AcceptedShortwaveSnapshot:
    row = entry.candidate
    if row.parse_state != "complete" or row.start_minute_utc is None or row.end_minute_utc is None or row.weekday_mask is None:
        raise ValueError("Only a fully parsed scheduled listing can become a listening reminder")
    return AcceptedShortwaveSnapshot(
        schema_version=1,
        provider_key=dataset.provider_key,
        provider_label=dataset.provider_label,
        dataset_key=dataset.dataset_key,
        entry_key=entry.entry_key,
        provider_identity_hash=row.provider_identity_hash,
        content_hash=row.content_hash,
        season_code=dataset.season_code,
        season_effective_from_utc=dataset.season_effective_from_utc,
        season_effective_to_utc=dataset.season_effective_to_utc,
        source_uri=dataset.source_uri,
        station_name=row.station_name,
        station_home_code=row.station_home_code,
        frequency_hz=row.frequency_hz,
        start_minute_utc=row.start_minute_utc,
        end_minute_utc=row.end_minute_utc,
        crosses_midnight=row.crosses_midnight,
        raw_days=row.raw_days,
        weekday_mask=row.weekday_mask,
        start_date_normalized=row.start_date_normalized,
        stop_date_normalized=row.stop_date_normalized,
        signal_type=str(row.signal_type or ""),
        language_labels=row.language_labels,
        target_labels=row.target_labels,
        transmitter_labels=row.transmitter_labels,
        classification=row.classification,
    )


def _snapshot_json(snapshot: AcceptedShortwaveSnapshot) -> str:
    return json.dumps(asdict(snapshot), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _snapshot_from_json(value: str) -> AcceptedShortwaveSnapshot:
    raw = json.loads(str(value))
    for name in ("language_labels", "target_labels", "transmitter_labels"):
        raw[name] = tuple(str(item) for item in raw.get(name, ()))
    return AcceptedShortwaveSnapshot(**raw)


def _reminder_from_row(row: sqlite3.Row) -> ShortwaveListeningReminder:
    return ShortwaveListeningReminder(
        reminder_key=str(row["reminder_key"]),
        snapshot=_snapshot_from_json(str(row["accepted_snapshot_json"])),
        operator_label=str(row["operator_label"] or ""),
        notes=str(row["notes"] or ""),
        receiver_profile_id=int(row["receiver_profile_id"]) if row["receiver_profile_id"] is not None else None,
        lead_minutes=int(row["lead_minutes"]),
        enabled=bool(row["enabled"]),
        source_review_state=str(row["source_review_state"]),
        created_utc=str(row["created_utc"]),
        updated_utc=str(row["updated_utc"]),
    )


def _snapshot_occurrences(
    snapshot: AcceptedShortwaveSnapshot,
    start_utc: datetime,
    end_utc: datetime,
    *,
    limit: int = MAX_LISTENING_OCCURRENCES,
) -> tuple[tuple[datetime, datetime], ...]:
    season_start = _parse_utc(snapshot.season_effective_from_utc)
    season_end = _parse_utc(snapshot.season_effective_to_utc)
    if season_start is None or season_end is None:
        return ()
    first = max(season_start.date(), start_utc.date() - timedelta(days=1))
    last = min(season_end.date(), end_utc.date())
    if last < first:
        return ()
    rows: list[tuple[datetime, datetime]] = []
    current = first
    cap = max(1, min(MAX_LISTENING_OCCURRENCES, int(limit)))
    while current <= last and len(rows) < cap:
        if snapshot.weekday_mask & (1 << current.weekday()) and _date_rule_allows(snapshot, current, season_start.date(), season_end.date()):
            begin = datetime.combine(current, time.min, tzinfo=timezone.utc) + timedelta(minutes=snapshot.start_minute_utc)
            finish = datetime.combine(current, time.min, tzinfo=timezone.utc) + timedelta(minutes=snapshot.end_minute_utc)
            if snapshot.crosses_midnight or finish <= begin:
                finish += timedelta(days=1)
            if finish >= start_utc and begin <= end_utc:
                rows.append((begin, finish))
        current += timedelta(days=1)
    return tuple(rows)


def _date_rule_allows(snapshot: AcceptedShortwaveSnapshot, service_day: date, season_start: date, season_end: date) -> bool:
    start = _rule_date(snapshot.start_date_normalized, season_start, season_end, after=None)
    stop = _rule_date(snapshot.stop_date_normalized, season_start, season_end, after=start)
    return (start is None or service_day >= start) and (stop is None or service_day <= stop)


def _rule_date(rule: str | None, season_start: date, season_end: date, *, after: date | None) -> date | None:
    if not rule:
        return None
    try:
        month, day = (int(part) for part in rule.removeprefix("--").split("-", 1))
    except (TypeError, ValueError):
        return None
    lower = after or season_start
    for year in range(season_start.year, season_end.year + 1):
        try:
            value = date(year, month, day)
        except ValueError:
            continue
        if lower <= value <= season_end:
            return value
    return None


def _occurrence_key(reminder_key: str, start_utc: datetime) -> str:
    token = f"{reminder_key}|{_utc_text(start_utc)}".encode("utf-8")
    return "shortwave_occurrence_" + hashlib.sha256(token).hexdigest()[:24]


def _urgency(start: datetime, end: datetime, now: datetime, lead_minutes: int) -> tuple[str, str]:
    if start <= now < end:
        minutes = max(1, int((end - now).total_seconds() + 59) // 60)
        return "active", f"Listed now · ends in {minutes}m"
    if end <= now:
        minutes = max(0, int((now - end).total_seconds()) // 60)
        return "recent", f"Listing ended {minutes}m ago"
    minutes = max(0, int((start - now).total_seconds() + 59) // 60)
    state = "reminding" if minutes <= _lead(lead_minutes) else ("soon" if minutes <= 30 else "upcoming")
    return state, f"Listed to start in {minutes}m"


def _lead(value: int) -> int:
    return max(0, min(1440, int(value)))


def _parse_utc(value: str) -> datetime | None:
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return _as_utc(value).replace(microsecond=0).isoformat().replace("+00:00", "Z")


__all__ = [
    "AcceptedShortwaveSnapshot",
    "MAX_LISTENING_OCCURRENCES",
    "MAX_LISTENING_REMINDERS",
    "SOURCE_TYPE_SHORTWAVE_LISTENING",
    "ShortwaveListeningOccurrence",
    "ShortwaveListeningOutlook",
    "ShortwaveListeningReminder",
    "ShortwaveSourceReview",
    "ShortwaveListeningStore",
    "build_shortwave_listening_outlook",
]
