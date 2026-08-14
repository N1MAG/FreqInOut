from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from freqinout.core.ingest_source_model import IngestSourceDescriptor, IngestSourceInventory
from freqinout.core.sqlite_fingerprint import sqlite_table_fingerprint


MESSAGE_PROJECTION_TABLES: tuple[str, ...] = (
    "js8_messages",
    "spotter_traffic",
    "sitrep_events",
    "commstat_artifacts",
    "commstat_artifact_deletions",
    "varac_messages",
    "message_file_metadata",
)

MAP_PROJECTION_TABLES: tuple[str, ...] = (
    "observations",
    "observation_topics",
    "js8_links",
    "js8_callsign_stats",
    "operator_checkins",
    "sitrep_latest_by_callsign",
    "varac_callsign_stats",
)


@dataclass(frozen=True)
class IngestSourceComponentHealth:
    name: str
    health_key: str
    degraded: bool = False
    last_checked_ts: float = 0.0
    last_success_ts: float = 0.0
    last_failure_ts: float = 0.0
    last_error: str = ""


@dataclass(frozen=True)
class IngestSourceHealth:
    source_id: str
    family: str
    source_type: str
    label: str
    radio_id: str = ""
    app_instance_id: str = ""
    exists: bool = False
    degraded: bool = False
    stale: bool = False
    last_checked_ts: float = 0.0
    last_success_ts: float = 0.0
    last_failure_ts: float = 0.0
    last_error: str = ""
    path: str = ""
    endpoint: str = ""
    fingerprint: tuple[str, ...] = ()
    health_key: str = ""
    components: tuple[IngestSourceComponentHealth, ...] = ()
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class IngestProjectionHealth:
    name: str
    db_path: str = ""
    tables: tuple[str, ...] = ()
    fingerprint: tuple[tuple[str, ...], ...] = ()


@dataclass(frozen=True)
class IngestHealthSnapshot:
    generated_ts: float
    sources: tuple[IngestSourceHealth, ...] = ()
    projections: tuple[IngestProjectionHealth, ...] = ()
    station_fingerprint: tuple[object, ...] = field(default_factory=tuple)

    @property
    def degraded_count(self) -> int:
        return sum(1 for source in self.sources if source.degraded)

    @property
    def missing_count(self) -> int:
        return sum(1 for source in self.sources if source.source_type in {"file", "directory", "sqlite"} and not source.exists)

    def sources_for_family(self, family: str) -> tuple[IngestSourceHealth, ...]:
        wanted = str(family or "").strip().lower()
        return tuple(source for source in self.sources if source.family == wanted)


def source_health_key(source: IngestSourceDescriptor) -> str:
    return f"ingest-source:{source.source_id}"


def source_fingerprint(source: IngestSourceDescriptor) -> tuple[str, ...]:
    if source.source_type in {"file", "sqlite"}:
        return _path_stat_fingerprint(source.path, include_is_dir=False)
    if source.source_type == "directory":
        return _path_stat_fingerprint(source.path, include_is_dir=True)
    if source.source_type == "api":
        return ("api", source.endpoint.strip().lower(), "configured" if source.endpoint else "missing")
    return (source.source_type, source.path, source.endpoint)


def build_ingest_health_snapshot(
    inventory: IngestSourceInventory,
    *,
    db_path: str | Path | None = None,
    health_registry_snapshot: Mapping[str, object] | None = None,
    projection_table_sets: Sequence[tuple[str, Sequence[str]]] | None = None,
    now_ts: float | None = None,
) -> IngestHealthSnapshot:
    now = time.time() if now_ts is None else float(now_ts)
    health_map = dict(health_registry_snapshot or {})
    source_health = tuple(_source_health(source, health_map=health_map, now_ts=now) for source in inventory.ingest_sources)
    db_txt = str(Path(db_path).expanduser()) if db_path else ""
    projections = tuple(
        _projection_health(db_txt, name, tables)
        for name, tables in (
            projection_table_sets
            or (
                ("messages", MESSAGE_PROJECTION_TABLES),
                ("map", MAP_PROJECTION_TABLES),
            )
        )
    )
    station_fp: list[object] = ["ingest-health-v1"]
    station_fp.extend((source.source_id, source.fingerprint, source.degraded, source.exists) for source in source_health)
    station_fp.extend((projection.name, projection.fingerprint) for projection in projections)
    return IngestHealthSnapshot(
        generated_ts=now,
        sources=source_health,
        projections=projections,
        station_fingerprint=tuple(station_fp),
    )


def _source_health(
    source: IngestSourceDescriptor,
    *,
    health_map: Mapping[str, object],
    now_ts: float,
) -> IngestSourceHealth:
    key = source_health_key(source)
    health = health_map.get(key)
    if not isinstance(health, Mapping):
        health = health_map.get(f"background-ingest:{source.family}") if isinstance(health_map.get(f"background-ingest:{source.family}"), Mapping) else {}
    components = _component_health(key, health_map)
    fingerprint = source_fingerprint(source)
    exists = _source_exists(source, fingerprint)
    degraded = bool(health.get("degraded", False)) if isinstance(health, Mapping) else False
    degraded = degraded or any(component.degraded for component in components)
    last_success = _float_value(health.get("last_success_ts", 0.0)) if isinstance(health, Mapping) else 0.0
    last_failure = _float_value(health.get("last_failure_ts", 0.0)) if isinstance(health, Mapping) else 0.0
    last_checked = _float_value(health.get("last_checked_ts", 0.0)) if isinstance(health, Mapping) else 0.0
    last_error = str(health.get("last_error", "") or "") if isinstance(health, Mapping) else ""
    if not last_error:
        last_error = next((component.last_error for component in components if component.degraded and component.last_error), "")
    stale = False
    if source.source_type in {"file", "directory", "sqlite"} and not exists:
        degraded = True
        last_error = last_error or "source path missing"
    return IngestSourceHealth(
        source_id=source.source_id,
        family=source.family,
        source_type=source.source_type,
        label=source.label,
        radio_id=source.radio_id,
        app_instance_id=source.app_instance_id,
        exists=exists,
        degraded=degraded,
        stale=stale,
        last_checked_ts=last_checked,
        last_success_ts=last_success,
        last_failure_ts=last_failure,
        last_error=last_error,
        path=source.path,
        endpoint=source.endpoint,
        fingerprint=fingerprint,
        health_key=key,
        components=components,
        metadata=dict(source.metadata or {}),
    )


def _component_health(source_key: str, health_map: Mapping[str, object]) -> tuple[IngestSourceComponentHealth, ...]:
    prefix = f"{source_key}:"
    out: list[IngestSourceComponentHealth] = []
    for key, raw in sorted(health_map.items()):
        key_txt = str(key or "")
        if not key_txt.startswith(prefix) or not isinstance(raw, Mapping):
            continue
        name = key_txt[len(prefix) :].strip() or "component"
        out.append(
            IngestSourceComponentHealth(
                name=name,
                health_key=key_txt,
                degraded=bool(raw.get("degraded", False)),
                last_checked_ts=_float_value(raw.get("last_checked_ts", 0.0)),
                last_success_ts=_float_value(raw.get("last_success_ts", 0.0)),
                last_failure_ts=_float_value(raw.get("last_failure_ts", 0.0)),
                last_error=str(raw.get("last_error", "") or ""),
            )
        )
    return tuple(out)


def _projection_health(db_path: str, name: str, tables: Sequence[str]) -> IngestProjectionHealth:
    return IngestProjectionHealth(
        name=str(name or ""),
        db_path=db_path,
        tables=tuple(str(table or "") for table in tables if str(table or "")),
        fingerprint=sqlite_table_fingerprint(db_path, tables) if db_path else tuple(),
    )


def _path_stat_fingerprint(path: str | Path | None, *, include_is_dir: bool) -> tuple[str, ...]:
    raw = str(path or "").strip()
    if not raw:
        return ("missing", "")
    try:
        p = Path(raw).expanduser()
        stat = p.stat()
        kind = "dir" if p.is_dir() else "file"
        values = [
            kind,
            str(p),
            str(int(stat.st_mtime_ns)),
            str(int(stat.st_size)),
        ]
        if include_is_dir and p.is_dir():
            try:
                values.append(str(max((child.stat().st_mtime_ns for child in p.iterdir()), default=0)))
            except Exception:
                values.append("0")
        return tuple(values)
    except OSError:
        return ("missing", raw)
    except Exception:
        try:
            return ("unknown", raw, str(int(os.path.getmtime(raw))))
        except Exception:
            return ("missing", raw)


def _source_exists(source: IngestSourceDescriptor, fingerprint: tuple[str, ...]) -> bool:
    if source.source_type == "api":
        return bool(source.endpoint)
    if not fingerprint:
        return False
    return fingerprint[0] != "missing"


def _float_value(value: object) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0
