"""Bounded, Qt-free coordination for guided software discovery.

The coordinator owns concurrency, coalescing, immutable cache publication, and
generation checks. Scanner functions do the bounded read-only work on the
coordinator's worker pool. Calling :meth:`register_request` is intentionally
cheap and performs no discovery I/O, so a GUI can establish the current draft
generation before handing :meth:`discover` to an existing QThread worker.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from enum import Enum
from types import MappingProxyType
from typing import Callable, Iterable, Mapping, Optional, Sequence, Tuple

from freqinout.core.guided_radio_software_model import (
    RadioRole,
    SoftwareFamily,
    radio_role_from_persisted,
    software_family_for_capability,
)
from freqinout.core.guided_software_proposals import (
    DiscoveryEvidence,
    DiscoverySnapshot,
    DiscoveredSoftwareCandidate,
)


class GuidedSoftwareDiscoveryError(ValueError):
    """Raised for malformed requests or coordinator misuse."""


class DiscoveryPhase(str, Enum):
    APPLICATIONS = "applications"
    JS8_PROFILES = "js8_profiles"
    FAST_LIGHT = "fast_light"
    RECEIVER = "receiver"
    VARAC = "varac"


_MAX_SHORT_TEXT = 256
_MAX_TEXT = 4096
_MAX_INPUTS = 128
_MAX_PHASE_ITEMS = 128
_MAX_WORKERS = 8


def _text(value: object, field_name: str, *, required: bool = False, maximum: int = _MAX_TEXT) -> str:
    result = str(value or "").strip()
    if len(result) > maximum:
        raise GuidedSoftwareDiscoveryError(f"{field_name} exceeds {maximum} characters")
    if required and not result:
        raise GuidedSoftwareDiscoveryError(f"{field_name} is required")
    return result


def _key(value: object, field_name: str) -> str:
    result = _text(value, field_name, required=True, maximum=_MAX_SHORT_TEXT).casefold()
    if any(character in result for character in ("\x00", "\n", "\r")):
        raise GuidedSoftwareDiscoveryError(f"{field_name} contains an invalid character")
    return result


def _integer(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        raise GuidedSoftwareDiscoveryError(f"{field_name} must be a non-negative integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise GuidedSoftwareDiscoveryError(f"{field_name} must be a non-negative integer") from exc
    if result < 0:
        raise GuidedSoftwareDiscoveryError(f"{field_name} must be a non-negative integer")
    return result


def _bounded_tuple(values: Iterable[object], field_name: str, maximum: int) -> tuple:
    result = tuple(values or ())
    if len(result) > maximum:
        raise GuidedSoftwareDiscoveryError(f"{field_name} has more than {maximum} values")
    return result


@dataclass(frozen=True)
class DiscoveryRequest:
    session_key: str
    request_key: str
    generation: int
    draft_revision: int
    radio_role: RadioRole
    families: Tuple[SoftwareFamily, ...]
    inputs: Mapping[str, str] = field(default_factory=dict)
    force_refresh: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_key", _key(self.session_key, "session_key"))
        object.__setattr__(self, "request_key", _key(self.request_key, "request_key"))
        object.__setattr__(self, "generation", _integer(self.generation, "generation"))
        object.__setattr__(self, "draft_revision", _integer(self.draft_revision, "draft_revision"))
        object.__setattr__(self, "radio_role", radio_role_from_persisted(self.radio_role))
        families = tuple(
            software_family_for_capability(item)
            for item in _bounded_tuple(self.families, "families", len(SoftwareFamily))
        )
        families = tuple(sorted(set(families), key=lambda item: item.value))
        object.__setattr__(self, "families", families)
        if not isinstance(self.inputs, Mapping):
            raise GuidedSoftwareDiscoveryError("inputs must be a mapping")
        if len(self.inputs) > _MAX_INPUTS:
            raise GuidedSoftwareDiscoveryError("inputs has too many values")
        inputs: dict[str, str] = {}
        for raw_key, raw_value in self.inputs.items():
            key = _key(raw_key, "input key")
            if key in inputs:
                raise GuidedSoftwareDiscoveryError("duplicate input key")
            inputs[key] = _text(raw_value, "input value")
        object.__setattr__(self, "inputs", MappingProxyType(inputs))
        object.__setattr__(self, "force_refresh", bool(self.force_refresh))

    @property
    def current_token(self) -> Tuple[int, int, str]:
        return (self.generation, self.draft_revision, self.request_key)

    @property
    def input_fingerprint(self) -> str:
        payload = {
            "role": self.radio_role.value,
            "families": [item.value for item in self.families],
            "inputs": dict(self.inputs),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @property
    def scan_input_fingerprint(self) -> str:
        """Hash only scanner inputs so family/role changes can reuse evidence."""

        encoded = json.dumps(dict(self.inputs), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PhaseDiscoveryResult:
    phase: DiscoveryPhase
    candidates: Tuple[DiscoveredSoftwareCandidate, ...] = field(default_factory=tuple)
    evidence: Tuple[DiscoveryEvidence, ...] = field(default_factory=tuple)
    diagnostics: Tuple[str, ...] = field(default_factory=tuple)
    elapsed_ms: int = 0

    def __post_init__(self) -> None:
        phase = self.phase if isinstance(self.phase, DiscoveryPhase) else DiscoveryPhase(str(self.phase))
        object.__setattr__(self, "phase", phase)
        candidates = _bounded_tuple(self.candidates, "phase candidates", _MAX_PHASE_ITEMS)
        evidence = _bounded_tuple(self.evidence, "phase evidence", _MAX_PHASE_ITEMS)
        diagnostics = tuple(
            _text(item, "phase diagnostic", required=True)
            for item in _bounded_tuple(self.diagnostics, "phase diagnostics", _MAX_PHASE_ITEMS)
        )
        if not all(isinstance(item, DiscoveredSoftwareCandidate) for item in candidates):
            raise GuidedSoftwareDiscoveryError("phase candidates must be immutable discovery candidates")
        if not all(isinstance(item, DiscoveryEvidence) for item in evidence):
            raise GuidedSoftwareDiscoveryError("phase evidence must be immutable discovery evidence")
        object.__setattr__(self, "candidates", tuple(candidates))
        object.__setattr__(self, "evidence", tuple(evidence))
        object.__setattr__(self, "diagnostics", diagnostics)
        object.__setattr__(self, "elapsed_ms", _integer(self.elapsed_ms, "phase elapsed_ms"))


@dataclass(frozen=True)
class DiscoveryTelemetryEvent:
    event: str
    request_key: str
    session_key: str
    generation: int
    phase: str = ""
    elapsed_ms: int = 0
    candidate_count: int = 0
    cache_state: str = "none"
    outcome: str = "ok"
    cancelled: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "event", _key(self.event, "telemetry event"))
        object.__setattr__(self, "request_key", _key(self.request_key, "telemetry request_key"))
        object.__setattr__(self, "session_key", _key(self.session_key, "telemetry session_key"))
        object.__setattr__(self, "generation", _integer(self.generation, "telemetry generation"))
        object.__setattr__(self, "phase", _text(self.phase, "telemetry phase", maximum=_MAX_SHORT_TEXT))
        object.__setattr__(self, "elapsed_ms", _integer(self.elapsed_ms, "telemetry elapsed_ms"))
        object.__setattr__(self, "candidate_count", _integer(self.candidate_count, "telemetry candidate_count"))
        object.__setattr__(self, "cache_state", _text(self.cache_state, "telemetry cache_state", maximum=32))
        object.__setattr__(self, "outcome", _text(self.outcome, "telemetry outcome", maximum=64))
        object.__setattr__(self, "cancelled", bool(self.cancelled))


PhaseScanner = Callable[[DiscoveryRequest, Callable[[], bool]], PhaseDiscoveryResult]
TelemetrySink = Callable[[DiscoveryTelemetryEvent], None]


def required_phases_for(request: DiscoveryRequest) -> Tuple[DiscoveryPhase, ...]:
    phases = set()
    external = set(request.families) - {SoftwareFamily.FIO_SPOTTER}
    if external:
        phases.add(DiscoveryPhase.APPLICATIONS)
    if SoftwareFamily.JS8CALL in request.families:
        phases.add(DiscoveryPhase.JS8_PROFILES)
    if SoftwareFamily.FAST_LIGHT in request.families:
        phases.add(DiscoveryPhase.FAST_LIGHT)
    if SoftwareFamily.SDRPP in request.families:
        phases.add(DiscoveryPhase.RECEIVER)
    if SoftwareFamily.VARAC in request.families or SoftwareFamily.VARAC_CLUSTER in request.families:
        phases.add(DiscoveryPhase.VARAC)
    return tuple(sorted(phases, key=lambda item: item.value))


class GuidedSoftwareDiscoveryCoordinator:
    """Coordinate one bounded read-only scan graph across Settings surfaces."""

    def __init__(
        self,
        scanners: Mapping[DiscoveryPhase, PhaseScanner],
        *,
        max_workers: int = 4,
        phase_timeout_seconds: float = 2.0,
        telemetry_sink: Optional[TelemetrySink] = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        worker_count = max(1, min(_MAX_WORKERS, int(max_workers)))
        self._scanners = {
            phase if isinstance(phase, DiscoveryPhase) else DiscoveryPhase(str(phase)): scanner
            for phase, scanner in dict(scanners or {}).items()
        }
        if not all(callable(scanner) for scanner in self._scanners.values()):
            raise GuidedSoftwareDiscoveryError("every discovery scanner must be callable")
        self._executor = ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="fio-guided-discovery")
        self._phase_timeout_seconds = max(0.05, min(30.0, float(phase_timeout_seconds)))
        self._telemetry_sink = telemetry_sink
        self._monotonic = monotonic
        self._lock = threading.RLock()
        self._phase_cache: dict[Tuple[DiscoveryPhase, str], PhaseDiscoveryResult] = {}
        self._inflight: dict[Tuple[str, int, DiscoveryPhase, str], Future[PhaseDiscoveryResult]] = {}
        self._inflight_started: dict[Tuple[str, int, DiscoveryPhase, str], float] = {}
        self._expired_phase_tokens: set[Tuple[str, int, DiscoveryPhase, str]] = set()
        self._current: dict[str, Tuple[int, int, str]] = {}
        self._cancelled: set[Tuple[str, int]] = set()
        self._closed_sessions: set[str] = set()
        self._last_snapshot: dict[str, DiscoverySnapshot] = {}
        self._shutdown = False

    def register_request(self, request: DiscoveryRequest) -> bool:
        """Mark a draft generation current without performing discovery I/O."""

        with self._lock:
            if self._shutdown or request.session_key in self._closed_sessions:
                return False
            current = self._current.get(request.session_key)
            if current is not None and (request.generation, request.draft_revision) < current[:2]:
                return False
            self._current[request.session_key] = request.current_token
            self._cancelled = {
                item for item in self._cancelled
                if item[0] != request.session_key or item[1] >= request.generation
            }
            return True

    def cancel(self, session_key: str, generation: int) -> None:
        with self._lock:
            self._cancelled.add((_key(session_key, "session_key"), _integer(generation, "generation")))

    def close_session(self, session_key: str) -> None:
        key = _key(session_key, "session_key")
        with self._lock:
            self._closed_sessions.add(key)
            self._current.pop(key, None)

    def previous_snapshot(self, session_key: str) -> Optional[DiscoverySnapshot]:
        with self._lock:
            return self._last_snapshot.get(_key(session_key, "session_key"))

    def result_is_current(self, request: DiscoveryRequest) -> bool:
        with self._lock:
            return self._request_is_current_locked(request)

    def discover(
        self,
        request: DiscoveryRequest,
        *,
        cancel_probe: Optional[Callable[[], bool]] = None,
        phase_callback: Optional[Callable[[PhaseDiscoveryResult], None]] = None,
    ) -> DiscoverySnapshot:
        """Run or reuse phase work and return one immutable generation snapshot.

        This method is blocking by design and belongs on an existing worker
        lane. Each actual scan phase runs on the coordinator's bounded pool.
        """

        started = self._monotonic()
        if not self.register_request(request):
            return self._cancelled_snapshot(request, started, "stale-before-start")
        phases = required_phases_for(request)
        futures: dict[
            Future[PhaseDiscoveryResult],
            Tuple[DiscoveryPhase, str, Tuple[str, int, DiscoveryPhase, str], float],
        ] = {}
        for phase in phases:
            future, cache_state, phase_token, started_at = self._phase_future(request, phase)
            futures[future] = (phase, cache_state, phase_token, started_at)

        results: list[PhaseDiscoveryResult] = []
        diagnostics: list[str] = []
        pending = set(futures)
        while pending:
            if self._cancelled_or_stale(request, cancel_probe):
                for future in pending:
                    future.cancel()
                return self._cancelled_snapshot(request, started, "cancelled-or-stale")
            completed, pending = wait(pending, timeout=0.05, return_when=FIRST_COMPLETED)
            for future in completed:
                phase, cache_state, _phase_token, _started_at = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    detail = _text(exc, "phase error", maximum=_MAX_SHORT_TEXT) or exc.__class__.__name__
                    diagnostics.append(f"{phase.value}: {detail}")
                    self._emit(request, "phase-finish", phase=phase, cache_state=cache_state, outcome="error")
                    continue
                results.append(result)
                self._emit(
                    request,
                    "phase-finish",
                    phase=phase,
                    cache_state=cache_state,
                    candidate_count=len(result.candidates),
                    elapsed_ms=0 if cache_state == "reused" else result.elapsed_ms,
                )
                if phase_callback is not None and self.result_is_current(request):
                    phase_callback(result)
            now = self._monotonic()
            expired = {
                future
                for future in pending
                if futures[future][1] not in {"reused"}
                and now - futures[future][3] >= self._phase_timeout_seconds
            }
            for future in expired:
                phase, cache_state, phase_token, started_at = futures[future]
                pending.remove(future)
                future.cancel()
                with self._lock:
                    self._expired_phase_tokens.add(phase_token)
                elapsed_ms = max(0, int(round((now - started_at) * 1000.0)))
                diagnostics.append(
                    f"{phase.value}: discovery exceeded the {self._phase_timeout_seconds:.2f}s budget; "
                    "continuing with safe partial evidence"
                )
                self._emit(
                    request,
                    "phase-finish",
                    phase=phase,
                    cache_state=cache_state,
                    outcome="timeout",
                    elapsed_ms=elapsed_ms,
                )

        elapsed_ms = max(0, int(round((self._monotonic() - started) * 1000.0)))
        snapshot = self._combine(request, results, diagnostics, elapsed_ms)
        with self._lock:
            if not self._request_is_current_locked(request):
                return self._cancelled_snapshot(request, started, "stale-at-publish")
            self._last_snapshot[request.session_key] = snapshot
        self._emit(
            request,
            "request-finish",
            elapsed_ms=elapsed_ms,
            candidate_count=len(snapshot.candidates),
            outcome="partial" if snapshot.diagnostics else "ok",
        )
        return snapshot

    def clear_cache(self) -> None:
        with self._lock:
            self._phase_cache.clear()

    def shutdown(self, *, wait_for_workers: bool = False) -> None:
        with self._lock:
            self._shutdown = True
            self._current.clear()
            self._closed_sessions.update(self._last_snapshot)
        self._executor.shutdown(wait=bool(wait_for_workers), cancel_futures=True)

    def _phase_future(
        self,
        request: DiscoveryRequest,
        phase: DiscoveryPhase,
    ) -> Tuple[
        Future[PhaseDiscoveryResult],
        str,
        Tuple[str, int, DiscoveryPhase, str],
        float,
    ]:
        cache_key = (phase, request.scan_input_fingerprint)
        inflight_key = (request.session_key, request.generation, phase, request.scan_input_fingerprint)
        with self._lock:
            if not request.force_refresh and cache_key in self._phase_cache:
                future: Future[PhaseDiscoveryResult] = Future()
                future.set_result(self._phase_cache[cache_key])
                return future, "reused", inflight_key, self._monotonic()
            active = self._inflight.get(inflight_key)
            if active is not None and not active.done():
                return (
                    active,
                    "coalesced",
                    inflight_key,
                    self._inflight_started.get(inflight_key, self._monotonic()),
                )
            # A phase timeout stops waiting; it cannot interrupt Python code
            # already parsing a file.  Reuse identical work still running for
            # this assistant session even when a newer UI generation requested
            # it, instead of stacking duplicate JS8 profile parses.
            shared_key = next(
                (
                    key
                    for key, future in self._inflight.items()
                    if key[0] == request.session_key
                    and key[2] == phase
                    and key[3] == request.scan_input_fingerprint
                    and not future.done()
                ),
                None,
            )
            if shared_key is not None:
                shared = self._inflight[shared_key]
                return (
                    shared,
                    "coalesced",
                    shared_key,
                    self._inflight_started.get(shared_key, self._monotonic()),
                )
            scanner = self._scanners.get(phase)
            if scanner is None:
                future = Future()
                future.set_exception(GuidedSoftwareDiscoveryError(f"no scanner registered for {phase.value}"))
                return future, "missing", inflight_key, self._monotonic()
            self._emit(request, "phase-start", phase=phase, cache_state="scan")
            started_at = self._monotonic()
            self._expired_phase_tokens.discard(inflight_key)
            future = self._executor.submit(
                self._run_phase,
                request,
                phase,
                scanner,
                cache_key,
                inflight_key,
            )
            self._inflight[inflight_key] = future
            self._inflight_started[inflight_key] = started_at
            future.add_done_callback(lambda _done, key=inflight_key: self._release_inflight(key))
            return future, "scan", inflight_key, started_at

    def _run_phase(
        self,
        request: DiscoveryRequest,
        phase: DiscoveryPhase,
        scanner: PhaseScanner,
        cache_key: Tuple[DiscoveryPhase, str],
        phase_token: Tuple[str, int, DiscoveryPhase, str],
    ) -> PhaseDiscoveryResult:
        started = self._monotonic()
        result = scanner(request, lambda: self._cancelled_or_stale(request, None))
        if not isinstance(result, PhaseDiscoveryResult) or result.phase != phase:
            raise GuidedSoftwareDiscoveryError(f"scanner returned an invalid {phase.value} result")
        result = replace(
            result,
            elapsed_ms=max(0, int(round((self._monotonic() - started) * 1000.0))),
        )
        with self._lock:
            if (
                phase_token not in self._expired_phase_tokens
                and self._request_is_current_locked(request)
                and (request.session_key, request.generation) not in self._cancelled
            ):
                self._phase_cache[cache_key] = result
        return result

    def _release_inflight(self, key: Tuple[str, int, DiscoveryPhase, str]) -> None:
        with self._lock:
            self._inflight.pop(key, None)
            self._inflight_started.pop(key, None)
            self._expired_phase_tokens.discard(key)

    def _cancelled_or_stale(
        self,
        request: DiscoveryRequest,
        cancel_probe: Optional[Callable[[], bool]],
    ) -> bool:
        if cancel_probe is not None:
            try:
                if cancel_probe():
                    self.cancel(request.session_key, request.generation)
                    return True
            except Exception:
                self.cancel(request.session_key, request.generation)
                return True
        with self._lock:
            return (
                (request.session_key, request.generation) in self._cancelled
                or not self._request_is_current_locked(request)
            )

    def _request_is_current_locked(self, request: DiscoveryRequest) -> bool:
        return (
            not self._shutdown
            and request.session_key not in self._closed_sessions
            and (request.session_key, request.generation) not in self._cancelled
            and self._current.get(request.session_key) == request.current_token
        )

    def _combine(
        self,
        request: DiscoveryRequest,
        results: Sequence[PhaseDiscoveryResult],
        diagnostics: Sequence[str],
        elapsed_ms: int,
    ) -> DiscoverySnapshot:
        candidates: list[DiscoveredSoftwareCandidate] = []
        evidence: list[DiscoveryEvidence] = []
        combined_diagnostics = list(diagnostics)
        candidate_keys: set[str] = set()
        evidence_keys: set[str] = set()
        for result in sorted(results, key=lambda item: item.phase.value):
            combined_diagnostics.extend(result.diagnostics)
            for candidate in result.candidates:
                if candidate.candidate_key in candidate_keys:
                    combined_diagnostics.append(f"{result.phase.value}: duplicate candidate omitted")
                    continue
                candidate_keys.add(candidate.candidate_key)
                candidates.append(candidate)
            for item in result.evidence:
                if item.evidence_key in evidence_keys:
                    combined_diagnostics.append(f"{result.phase.value}: duplicate evidence omitted")
                    continue
                evidence_keys.add(item.evidence_key)
                evidence.append(item)
        return DiscoverySnapshot(
            snapshot_key=request.request_key,
            generation=request.generation,
            candidates=tuple(candidates),
            evidence=tuple(evidence),
            diagnostics=tuple(combined_diagnostics),
            elapsed_ms=elapsed_ms,
            cancelled=False,
        )

    def _cancelled_snapshot(self, request: DiscoveryRequest, started: float, outcome: str) -> DiscoverySnapshot:
        elapsed_ms = max(0, int(round((self._monotonic() - started) * 1000.0)))
        self._emit(request, "request-finish", elapsed_ms=elapsed_ms, outcome=outcome, cancelled=True)
        return DiscoverySnapshot(
            snapshot_key=request.request_key,
            generation=request.generation,
            diagnostics=("Discovery was cancelled or superseded; the previous snapshot remains active.",),
            elapsed_ms=elapsed_ms,
            cancelled=True,
        )

    def _emit(
        self,
        request: DiscoveryRequest,
        event: str,
        *,
        phase: Optional[DiscoveryPhase] = None,
        elapsed_ms: int = 0,
        candidate_count: int = 0,
        cache_state: str = "none",
        outcome: str = "ok",
        cancelled: bool = False,
    ) -> None:
        sink = self._telemetry_sink
        if sink is None:
            return
        payload = DiscoveryTelemetryEvent(
            event=event,
            request_key=request.request_key,
            session_key=request.session_key,
            generation=request.generation,
            phase=phase.value if phase is not None else "",
            elapsed_ms=elapsed_ms,
            candidate_count=candidate_count,
            cache_state=cache_state,
            outcome=outcome,
            cancelled=cancelled,
        )
        try:
            sink(payload)
        except Exception:
            return


__all__ = (
    "DiscoveryPhase",
    "DiscoveryRequest",
    "DiscoveryTelemetryEvent",
    "GuidedSoftwareDiscoveryCoordinator",
    "GuidedSoftwareDiscoveryError",
    "PhaseDiscoveryResult",
    "PhaseScanner",
    "TelemetrySink",
    "required_phases_for",
)
