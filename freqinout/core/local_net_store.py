"""Bounded persistence and query service for reminder-only Local Nets."""

from __future__ import annotations

import dataclasses
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from freqinout.core.local_net_models import LocalNetOccurrence, LocalNetSchedule, LocalNetSummary
from freqinout.core.local_net_recurrence import MAX_HORIZON_DAYS, next_occurrence, project_occurrences
from freqinout.core.sqlite_utils import connect_sqlite_readonly, connect_sqlite_runtime_write, table_exists


MAX_LOCAL_NET_SCHEDULES = 2_000
MAX_UPCOMING_OCCURRENCES = 500


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc)


def _json(values: Iterable[object]) -> str:
    return json.dumps(tuple(values), separators=(",", ":"))


class LocalNetStore:
    """Repository whose schema is owned exclusively by startup initialization."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        conn = connect_sqlite_runtime_write(self.db_path, row_factory=sqlite3.Row)
        try:
            if not table_exists(conn, "local_net_schedules"):
                raise RuntimeError("Local Net schema is not initialized")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _read(self) -> sqlite3.Connection | None:
        if not self.db_path.exists():
            return None
        try:
            conn = connect_sqlite_readonly(self.db_path, row_factory=sqlite3.Row)
        except sqlite3.Error:
            return None
        if not table_exists(conn, "local_net_schedules"):
            conn.close()
            return None
        return conn

    def save_schedule(
        self,
        schedule: LocalNetSchedule,
        *,
        now_utc: datetime | None = None,
    ) -> LocalNetSchedule:
        """Create/update a schedule and its bounded next-occurrence cache."""
        now = (now_utc or _now()).astimezone(timezone.utc)
        following = next_occurrence(schedule, now, horizon_days=MAX_HORIZON_DAYS)
        cached = dataclasses.replace(schedule, next_occurrence_utc=_stamp(following.start_utc) if following else None)
        stamp = _stamp(now)
        with self._write() as conn:
            old = conn.execute(
                "SELECT created_utc FROM local_net_schedules WHERE local_net_schedule_key=?",
                (cached.local_net_schedule_key,),
            ).fetchone()
            created = str(old["created_utc"]) if old else stamp
            conn.execute(
                """INSERT INTO local_net_schedules
                (local_net_schedule_key,name,service,recurrence,local_start_time,timezone_name,
                 duration_minutes,weekdays_json,month_weeks_json,biweekly_anchor_date,
                 one_time_local_date,effective_start_date,effective_end_date,exception_dates_json,
                 reminder_minutes,net_entry_key,net_session_key,frequency_resource_key,
                 operating_group_key,operating_group_name,sop_id,participation_notes,
                 accepted_session_version_hash,accepted_resource_version_hash,accepted_snapshot_json,
                 enabled,next_occurrence_utc,created_utc,updated_utc)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(local_net_schedule_key) DO UPDATE SET
                 name=excluded.name,service=excluded.service,recurrence=excluded.recurrence,
                 local_start_time=excluded.local_start_time,timezone_name=excluded.timezone_name,
                 duration_minutes=excluded.duration_minutes,weekdays_json=excluded.weekdays_json,
                 month_weeks_json=excluded.month_weeks_json,biweekly_anchor_date=excluded.biweekly_anchor_date,
                 one_time_local_date=excluded.one_time_local_date,effective_start_date=excluded.effective_start_date,
                 effective_end_date=excluded.effective_end_date,exception_dates_json=excluded.exception_dates_json,
                 reminder_minutes=excluded.reminder_minutes,net_entry_key=excluded.net_entry_key,
                 net_session_key=excluded.net_session_key,frequency_resource_key=excluded.frequency_resource_key,
                 operating_group_key=excluded.operating_group_key,operating_group_name=excluded.operating_group_name,
                 sop_id=excluded.sop_id,participation_notes=excluded.participation_notes,
                 accepted_session_version_hash=excluded.accepted_session_version_hash,
                 accepted_resource_version_hash=excluded.accepted_resource_version_hash,
                 accepted_snapshot_json=excluded.accepted_snapshot_json,enabled=excluded.enabled,
                 next_occurrence_utc=excluded.next_occurrence_utc,updated_utc=excluded.updated_utc""",
                (
                    cached.local_net_schedule_key, cached.name, cached.service, cached.recurrence,
                    cached.local_start_time, cached.timezone_name, cached.duration_minutes,
                    _json(cached.weekdays), _json(cached.month_weeks), cached.biweekly_anchor_date,
                    cached.one_time_local_date, cached.effective_start_date, cached.effective_end_date,
                    _json(cached.exception_dates), cached.reminder_minutes, cached.net_entry_key,
                    cached.net_session_key, cached.frequency_resource_key, cached.operating_group_key,
                    cached.operating_group_name, cached.sop_id, cached.participation_notes,
                    cached.accepted_session_version_hash, cached.accepted_resource_version_hash,
                    cached.accepted_snapshot_json, int(cached.enabled), cached.next_occurrence_utc,
                    created, stamp,
                ),
            )
        return dataclasses.replace(cached, created_utc=created, updated_utc=stamp)

    def get_schedule(self, schedule_key: str) -> LocalNetSchedule | None:
        conn = self._read()
        if conn is None:
            return None
        try:
            row = conn.execute(
                "SELECT * FROM local_net_schedules WHERE local_net_schedule_key=?",
                (str(schedule_key or "").strip(),),
            ).fetchone()
            return self._decode_schedule(row) if row else None
        finally:
            conn.close()

    def list_schedules(
        self,
        *,
        search: str = "",
        operating_group_key: str | None = None,
        service: str | None = None,
        enabled: bool | None = None,
        needs_review: bool | None = None,
        limit: int = 200,
    ) -> tuple[LocalNetSchedule, ...]:
        conn = self._read()
        if conn is None:
            return ()
        clauses = ["1=1"]
        params: list[object] = []
        if search.strip():
            like = f"%{search.strip()}%"
            clauses.append("(name LIKE ? OR operating_group_name LIKE ? OR participation_notes LIKE ?)")
            params.extend((like, like, like))
        if operating_group_key:
            clauses.append("operating_group_key=?")
            params.append(str(operating_group_key))
        if service:
            clauses.append("service=?")
            params.append(str(service).upper())
        if enabled is not None:
            clauses.append("enabled=?")
            params.append(int(bool(enabled)))
        if needs_review is True:
            clauses.append("frequency_resource_key IS NULL")
        elif needs_review is False:
            clauses.append("frequency_resource_key IS NOT NULL")
        row_limit = max(1, min(MAX_LOCAL_NET_SCHEDULES, int(limit)))
        try:
            rows = conn.execute(
                f"""SELECT * FROM local_net_schedules WHERE {' AND '.join(clauses)}
                    ORDER BY CASE WHEN next_occurrence_utc IS NULL THEN 1 ELSE 0 END,
                             next_occurrence_utc, name, local_net_schedule_key LIMIT ?""",
                (*params, row_limit),
            ).fetchall()
            return tuple(self._decode_schedule(row) for row in rows)
        finally:
            conn.close()

    def set_enabled(self, schedule_key: str, enabled: bool, *, now_utc: datetime | None = None) -> LocalNetSchedule:
        schedule = self.get_schedule(schedule_key)
        if schedule is None:
            raise KeyError(f"Local Net schedule not found: {schedule_key}")
        return self.save_schedule(dataclasses.replace(schedule, enabled=bool(enabled)), now_utc=now_utc)

    def delete_schedule(self, schedule_key: str) -> bool:
        key = str(schedule_key or "").strip()
        with self._write() as conn:
            conn.execute("DELETE FROM local_net_occurrence_state WHERE local_net_schedule_key=?", (key,))
            cursor = conn.execute("DELETE FROM local_net_schedules WHERE local_net_schedule_key=?", (key,))
            return bool(cursor.rowcount)

    def dismiss_occurrence(self, occurrence: LocalNetOccurrence, *, note: str | None = None) -> None:
        now = _stamp(_now())
        with self._write() as conn:
            conn.execute(
                """INSERT INTO local_net_occurrence_state
                (occurrence_key,local_net_schedule_key,occurrence_start_utc,state,operator_note,updated_utc)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(occurrence_key) DO UPDATE SET state='dismissed',
                    operator_note=excluded.operator_note,updated_utc=excluded.updated_utc""",
                (occurrence.occurrence_key, occurrence.local_net_schedule_key, _stamp(occurrence.start_utc), "dismissed", note, now),
            )

    def upcoming(
        self,
        now_utc: datetime | None = None,
        *,
        horizon_days: int = MAX_HORIZON_DAYS,
        limit: int = MAX_UPCOMING_OCCURRENCES,
        include_dismissed: bool = False,
    ) -> tuple[LocalNetOccurrence, ...]:
        now = (now_utc or _now()).astimezone(timezone.utc)
        schedules = self.list_schedules(enabled=True, limit=MAX_LOCAL_NET_SCHEDULES)
        projected: list[LocalNetOccurrence] = []
        for schedule in schedules:
            projected.extend(project_occurrences(schedule, now, horizon_days=horizon_days, limit=limit))
        projected.sort(key=lambda item: (item.start_utc, item.local_net_schedule_key))
        bounded = projected[: max(1, min(MAX_UPCOMING_OCCURRENCES, int(limit)))]
        state = self._occurrence_state(item.occurrence_key for item in bounded)
        results = []
        for occurrence in bounded:
            stored = state.get(occurrence.occurrence_key)
            if stored and stored[0] == "dismissed":
                if not include_dismissed:
                    continue
                occurrence = dataclasses.replace(occurrence, state="dismissed", dismissed=True, operator_note=stored[1])
            results.append(occurrence)
        return tuple(results)

    def occurrences_for_schedule(
        self,
        schedule: LocalNetSchedule,
        window_start_utc: datetime | None = None,
        *,
        horizon_days: int = 2,
        limit: int = 10,
        include_dismissed: bool = False,
    ) -> tuple[LocalNetOccurrence, ...]:
        """Project one selected schedule without rescanning the whole calendar."""
        start = (window_start_utc or _now()).astimezone(timezone.utc)
        projected = project_occurrences(
            schedule,
            start,
            horizon_days=horizon_days,
            limit=limit,
        )
        state = self._occurrence_state(item.occurrence_key for item in projected)
        results: list[LocalNetOccurrence] = []
        for occurrence in projected:
            stored = state.get(occurrence.occurrence_key)
            if stored and stored[0] == "dismissed":
                if not include_dismissed:
                    continue
                occurrence = dataclasses.replace(
                    occurrence,
                    state="dismissed",
                    dismissed=True,
                    operator_note=stored[1],
                )
            results.append(occurrence)
        return tuple(results)

    def refresh_next_occurrence_cache(self, now_utc: datetime | None = None) -> int:
        now = (now_utc or _now()).astimezone(timezone.utc)
        schedules = self.list_schedules(limit=MAX_LOCAL_NET_SCHEDULES)
        updates: list[tuple[str | None, str, str]] = []
        stamp = _stamp(now) or ""
        for schedule in schedules:
            following = next_occurrence(schedule, now) if schedule.enabled else None
            value = _stamp(following.start_utc) if following else None
            if value != schedule.next_occurrence_utc:
                updates.append((value, stamp, schedule.local_net_schedule_key))
        if not updates:
            return 0
        with self._write() as conn:
            conn.executemany(
                "UPDATE local_net_schedules SET next_occurrence_utc=?,updated_utc=? WHERE local_net_schedule_key=?",
                updates,
            )
        return len(updates)

    def summary(self, now_utc: datetime | None = None, *, horizon_days: int = 30) -> LocalNetSummary:
        now = (now_utc or _now()).astimezone(timezone.utc)
        occurrences = self.upcoming(now, horizon_days=horizon_days)
        active = next((item for item in occurrences if item.start_utc <= now < item.end_utc), None)
        following = next((item for item in occurrences if item.start_utc > now), None)
        today = sum(
            item.local_start.date() == now.astimezone(item.local_start.tzinfo).date()
            for item in occurrences
        )
        by_schedule: dict[str, LocalNetOccurrence] = {}
        for item in occurrences:
            by_schedule.setdefault(item.local_net_schedule_key, item)
        attention = len(self.list_schedules(enabled=True, needs_review=True, limit=MAX_LOCAL_NET_SCHEDULES))
        return LocalNetSummary(active, following, today, len(occurrences), attention, by_schedule)

    def _occurrence_state(self, keys: Iterable[str]) -> Mapping[str, tuple[str, str | None]]:
        distinct = tuple(dict.fromkeys(str(key) for key in keys if str(key)))
        if not distinct:
            return {}
        conn = self._read()
        if conn is None:
            return {}
        try:
            result: dict[str, tuple[str, str | None]] = {}
            for offset in range(0, len(distinct), 400):
                batch = distinct[offset : offset + 400]
                marks = ",".join("?" for _ in batch)
                for row in conn.execute(
                    f"SELECT occurrence_key,state,operator_note FROM local_net_occurrence_state WHERE occurrence_key IN ({marks})",
                    batch,
                ):
                    result[str(row["occurrence_key"])] = (str(row["state"]), row["operator_note"])
            return result
        finally:
            conn.close()

    @staticmethod
    def _decode_schedule(row: sqlite3.Row) -> LocalNetSchedule:
        def values(name: str) -> tuple[object, ...]:
            try:
                loaded = json.loads(row[name] or "[]")
            except (TypeError, ValueError, json.JSONDecodeError):
                loaded = []
            return tuple(loaded) if isinstance(loaded, list) else ()

        return LocalNetSchedule(
            local_net_schedule_key=row["local_net_schedule_key"], name=row["name"], service=row["service"],
            recurrence=row["recurrence"], local_start_time=row["local_start_time"], timezone_name=row["timezone_name"],
            duration_minutes=row["duration_minutes"], weekdays=values("weekdays_json"), month_weeks=values("month_weeks_json"),
            biweekly_anchor_date=row["biweekly_anchor_date"], one_time_local_date=row["one_time_local_date"],
            effective_start_date=row["effective_start_date"], effective_end_date=row["effective_end_date"],
            exception_dates=values("exception_dates_json"), reminder_minutes=row["reminder_minutes"],
            net_entry_key=row["net_entry_key"], net_session_key=row["net_session_key"],
            frequency_resource_key=row["frequency_resource_key"], operating_group_key=row["operating_group_key"],
            operating_group_name=row["operating_group_name"], sop_id=row["sop_id"], participation_notes=row["participation_notes"],
            accepted_session_version_hash=row["accepted_session_version_hash"], accepted_resource_version_hash=row["accepted_resource_version_hash"],
            accepted_snapshot_json=row["accepted_snapshot_json"], enabled=bool(row["enabled"]),
            next_occurrence_utc=row["next_occurrence_utc"], created_utc=row["created_utc"], updated_utc=row["updated_utc"],
        )


__all__ = ["LocalNetStore", "MAX_LOCAL_NET_SCHEDULES", "MAX_UPCOMING_OCCURRENCES"]
