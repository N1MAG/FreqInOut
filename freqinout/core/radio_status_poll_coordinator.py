from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional


@dataclass(frozen=True)
class RadioStatusSnapshot:
    radio_id: str
    generated_at: float
    frequency_hz: Optional[int] = None
    ptt_active: bool = False
    ptt_known: bool = False
    vfo: Optional[str] = None
    js8_busy: bool = False
    js8_frequency_hz: Optional[int] = None
    js8_offset_hz: Optional[int] = None
    varac_status: Mapping[str, Any] = field(default_factory=dict)
    source: str = "poll"
    stale: bool = False
    refresh_pending: bool = False
    errors: Mapping[str, str] = field(default_factory=dict)
    backoff_until: Optional[float] = None

    def age_seconds(self, *, now: Optional[float] = None) -> float:
        if not self.generated_at:
            return float("inf")
        return max(0.0, float(now if now is not None else time.time()) - float(self.generated_at))

    def is_fresh_enough(self, ttl_seconds: float, *, now: Optional[float] = None) -> bool:
        return not self.stale and not self.errors and self.age_seconds(now=now) <= float(ttl_seconds)


@dataclass(frozen=True)
class RadioStatusPollMetrics:
    snapshot_count: int = 0
    inflight_count: int = 0
    cache_hits: int = 0
    backoff_hits: int = 0
    inflight_hits: int = 0
    polls_started: int = 0
    polls_succeeded: int = 0
    polls_failed: int = 0

    @property
    def polls_completed(self) -> int:
        return self.polls_succeeded + self.polls_failed

    def as_dict(self) -> Dict[str, int]:
        return {
            "snapshot_count": self.snapshot_count,
            "inflight_count": self.inflight_count,
            "cache_hits": self.cache_hits,
            "backoff_hits": self.backoff_hits,
            "inflight_hits": self.inflight_hits,
            "polls_started": self.polls_started,
            "polls_succeeded": self.polls_succeeded,
            "polls_failed": self.polls_failed,
            "polls_completed": self.polls_completed,
        }


class RadioStatusPollCoordinator:
    """
    TTL/backoff owner for radio status reads.

    Scheduler, RF Guard, and UI surfaces should consume snapshots from one
    coordinator instead of each creating their own polling cadence. The poller
    callable owns the actual IO so this class remains easy to test and reuse.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = 0.8,
        retry_seconds: float = 4.0,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self.retry_seconds = max(0.0, float(retry_seconds))
        self._time_fn = time_fn
        self._lock = threading.RLock()
        self._snapshots: Dict[str, RadioStatusSnapshot] = {}
        self._inflight: set[str] = set()
        self._cache_hits = 0
        self._backoff_hits = 0
        self._inflight_hits = 0
        self._polls_started = 0
        self._polls_succeeded = 0
        self._polls_failed = 0

    def latest_snapshot(self, radio_id: object) -> Optional[RadioStatusSnapshot]:
        key = self._radio_key(radio_id)
        with self._lock:
            return self._snapshots.get(key)

    def metrics_snapshot(self) -> RadioStatusPollMetrics:
        with self._lock:
            return RadioStatusPollMetrics(
                snapshot_count=len(self._snapshots),
                inflight_count=len(self._inflight),
                cache_hits=self._cache_hits,
                backoff_hits=self._backoff_hits,
                inflight_hits=self._inflight_hits,
                polls_started=self._polls_started,
                polls_succeeded=self._polls_succeeded,
                polls_failed=self._polls_failed,
            )

    def invalidate(self, radio_id: object | None = None) -> None:
        with self._lock:
            if radio_id is None:
                self._snapshots.clear()
                return
            self._snapshots.pop(self._radio_key(radio_id), None)

    def get_snapshot(
        self,
        radio_id: object,
        poller: Callable[[], Mapping[str, Any]],
        *,
        force: bool = False,
    ) -> RadioStatusSnapshot:
        key = self._radio_key(radio_id)
        now = self._time_fn()
        with self._lock:
            cached = self._snapshots.get(key)
            if (
                cached is not None
                and not force
                and cached.is_fresh_enough(self.ttl_seconds, now=now)
            ):
                self._cache_hits += 1
                return cached
            if cached is not None and cached.backoff_until and now < float(cached.backoff_until):
                self._backoff_hits += 1
                return self._mark_stale(cached, now=now, source="backoff")
            if key in self._inflight:
                self._inflight_hits += 1
                if cached is not None:
                    return self._mark_stale(cached, now=now, source="poll", refresh_pending=True)
                return RadioStatusSnapshot(
                    radio_id=key,
                    generated_at=now,
                    source="poll",
                    stale=True,
                    refresh_pending=True,
                )
            self._inflight.add(key)
            self._polls_started += 1

        try:
            raw = dict(poller() or {})
            snapshot = self._snapshot_from_mapping(key, raw, generated_at=self._time_fn())
            with self._lock:
                self._polls_succeeded += 1
        except Exception as exc:
            snapshot = self._error_snapshot(key, cached, exc, generated_at=self._time_fn())
            with self._lock:
                self._polls_failed += 1
        finally:
            with self._lock:
                self._inflight.discard(key)

        with self._lock:
            self._snapshots[key] = snapshot
        return snapshot

    def _error_snapshot(
        self,
        radio_id: str,
        cached: Optional[RadioStatusSnapshot],
        exc: Exception,
        *,
        generated_at: float,
    ) -> RadioStatusSnapshot:
        backoff_until = generated_at + self.retry_seconds if self.retry_seconds > 0 else None
        errors = {"poll": str(exc or "radio status poll failed")}
        if cached is not None:
            return RadioStatusSnapshot(
                radio_id=radio_id,
                generated_at=cached.generated_at,
                frequency_hz=cached.frequency_hz,
                ptt_active=cached.ptt_active,
                ptt_known=cached.ptt_known,
                vfo=cached.vfo,
                js8_busy=cached.js8_busy,
                js8_frequency_hz=cached.js8_frequency_hz,
                js8_offset_hz=cached.js8_offset_hz,
                varac_status=dict(cached.varac_status or {}),
                source="error",
                stale=True,
                errors=errors,
                backoff_until=backoff_until,
            )
        return RadioStatusSnapshot(
            radio_id=radio_id,
            generated_at=generated_at,
            source="error",
            stale=True,
            errors=errors,
            backoff_until=backoff_until,
        )

    def _mark_stale(
        self,
        snapshot: RadioStatusSnapshot,
        *,
        now: float,
        source: str,
        refresh_pending: bool = False,
    ) -> RadioStatusSnapshot:
        updated = RadioStatusSnapshot(
            radio_id=snapshot.radio_id,
            generated_at=snapshot.generated_at,
            frequency_hz=snapshot.frequency_hz,
            ptt_active=snapshot.ptt_active,
            ptt_known=snapshot.ptt_known,
            vfo=snapshot.vfo,
            js8_busy=snapshot.js8_busy,
            js8_frequency_hz=snapshot.js8_frequency_hz,
            js8_offset_hz=snapshot.js8_offset_hz,
            varac_status=dict(snapshot.varac_status or {}),
            source=source,
            stale=True,
            refresh_pending=refresh_pending,
            errors=dict(snapshot.errors or {}),
            backoff_until=snapshot.backoff_until,
        )
        with self._lock:
            self._snapshots[snapshot.radio_id] = updated
        return updated

    @staticmethod
    def _radio_key(radio_id: object) -> str:
        key = str(radio_id if radio_id is not None else "").strip()
        if not key:
            raise ValueError("radio_id is required")
        return key

    @staticmethod
    def _snapshot_from_mapping(
        radio_id: str,
        raw: Mapping[str, Any],
        *,
        generated_at: float,
    ) -> RadioStatusSnapshot:
        return RadioStatusSnapshot(
            radio_id=radio_id,
            generated_at=generated_at,
            frequency_hz=_int_or_none(raw.get("frequency_hz", raw.get("rig_freq_hz"))),
            ptt_active=bool(raw.get("ptt_active", raw.get("rig_ptt", False))),
            ptt_known=bool(raw.get("ptt_known", raw.get("rig_ptt_known", False))),
            vfo=_vfo_or_none(raw.get("vfo", raw.get("rig_vfo"))),
            js8_busy=bool(raw.get("js8_busy", False)),
            js8_frequency_hz=_int_or_none(raw.get("js8_frequency_hz", raw.get("js8_freq_hz"))),
            js8_offset_hz=_int_or_none(raw.get("js8_offset_hz")),
            varac_status=dict(raw.get("varac_status") or {}),
            source=str(raw.get("source") or "poll"),
            stale=False,
            errors={},
        )


def _int_or_none(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and int(value) > 0:
        return int(value)
    try:
        text = str(value or "").strip()
        if not text:
            return None
        parsed = int(float(text))
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _vfo_or_none(value: object) -> Optional[str]:
    text = str(value or "").strip().upper()[:1]
    return text if text in {"A", "B"} else None
