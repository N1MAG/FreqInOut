from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Sequence

from freqinout.core.ingest_health import source_fingerprint
from freqinout.core.ingest_source_model import IngestSourceDescriptor


@dataclass(frozen=True)
class IngestRefreshDecision:
    should_run: bool
    reason: str
    fingerprint: tuple[object, ...] = ()
    elapsed_sec: float = 0.0

    def as_dict(self) -> dict[str, object]:
        return {
            "should_run": bool(self.should_run),
            "reason": str(self.reason or ""),
            "elapsed_sec": float(self.elapsed_sec or 0.0),
            "fingerprint_size": max(0, len(self.fingerprint) - 1),
        }


def ingest_sources_fingerprint(
    sources: Iterable[IngestSourceDescriptor],
    *,
    families: Sequence[str] = (),
    source_types: Sequence[str] = (),
) -> tuple[object, ...]:
    family_set = {str(value or "").strip().lower() for value in families if str(value or "").strip()}
    type_set = {str(value or "").strip().lower() for value in source_types if str(value or "").strip()}
    parts: list[object] = ["ingest-sources-v1"]
    for source in sorted(sources, key=lambda item: (item.family, item.source_type, item.source_id)):
        if family_set and source.family not in family_set:
            continue
        if type_set and source.source_type not in type_set:
            continue
        if not source.enabled:
            continue
        parts.append(
            (
                source.source_id,
                source.family,
                source.source_type,
                source.radio_id,
                source.app_instance_id,
                source.label,
                source.checkpoint_key,
                source_fingerprint(source),
            )
        )
    return tuple(parts)


def plan_ingest_refresh(
    current_fingerprint: tuple[object, ...],
    *,
    previous_fingerprint: tuple[object, ...] | None = None,
    last_run_ts: float = 0.0,
    now_ts: float | None = None,
    force: bool = False,
    max_quiet_sec: float = 0.0,
    realtime_source_present: bool = False,
) -> IngestRefreshDecision:
    now = time.time() if now_ts is None else float(now_ts)
    last_run = float(last_run_ts or 0.0)
    elapsed = max(0.0, now - last_run) if last_run > 0 else 0.0
    if force:
        return IngestRefreshDecision(True, "forced", current_fingerprint, elapsed)
    if realtime_source_present:
        return IngestRefreshDecision(True, "realtime-source", current_fingerprint, elapsed)
    if previous_fingerprint is None:
        return IngestRefreshDecision(True, "first-run", current_fingerprint, elapsed)
    if current_fingerprint != previous_fingerprint:
        return IngestRefreshDecision(True, "source-changed", current_fingerprint, elapsed)
    if max_quiet_sec > 0 and (last_run <= 0 or elapsed >= float(max_quiet_sec)):
        return IngestRefreshDecision(True, "cadence", current_fingerprint, elapsed)
    return IngestRefreshDecision(False, "unchanged", current_fingerprint, elapsed)
