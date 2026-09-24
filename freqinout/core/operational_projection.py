from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from freqinout.core.schedule_projection import ScheduleSegment, build_blended_schedule_projection


OPERATIONAL_PLAN_CATEGORY = "sop_schedule"
SOP_SCHEDULE_LAYER_DEPENDENCY_PREFIX = "sop_schedule_layer"


@dataclass(frozen=True)
class OperationalEntry:
    source: str
    lane_key: str
    lane_label: str
    day_utc: str
    start_utc: str
    end_utc: str
    band: str = ""
    frequency: str = ""
    mode: str = ""
    group_name: str = ""
    net_name: str = ""
    profile_name: str = ""
    action_label: str = ""
    radio_id: int = 0
    source_key: str = ""
    raw: Mapping[str, Any] | None = None

    @property
    def display_label(self) -> str:
        if self.source in {"NET", "NET_RESOURCE"}:
            label = self.net_name or self.action_label or self.group_name or self.source
        else:
            label = self.action_label or self.net_name or self.profile_name or self.group_name or self.source
        band_freq = f"{self.band} {self.frequency}".strip()
        return f"{label} {band_freq}".strip()

    def to_schedule_ref(self) -> Dict[str, Any]:
        ref = {
            "source": self.source,
            "lane_key": self.lane_key,
            "lane_label": self.lane_label,
            "day_utc": self.day_utc,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "band": self.band,
            "frequency": self.frequency,
            "mode": self.mode,
            "group_name": self.group_name,
            "net_name": self.net_name,
            "profile_name": self.profile_name,
            "action_label": self.action_label,
            "radio_id": self.radio_id,
            "source_key": self.source_key,
        }
        raw = self.raw or {}
        for key in (
            "source_table",
            "source_row_id",
            "resource_id",
            "sop_profile_id",
            "sop_layer_id",
            "target_scope",
            "target_device_profile_id",
            "target_operating_profile_id",
        ):
            if key in raw and raw.get(key) not in (None, ""):
                ref[key] = raw.get(key)
        return ref


@dataclass(frozen=True)
class OperationalCell:
    lane_key: str
    day_utc: str
    hour_utc: int
    start_utc: dt.datetime
    end_utc: dt.datetime
    entries: Tuple[OperationalEntry, ...]

    @property
    def has_contention(self) -> bool:
        return len(self.entries) > 1

    @property
    def display_label(self) -> str:
        labels = [entry.display_label for entry in self.entries if entry.display_label]
        return " / ".join(dict.fromkeys(labels))


@dataclass(frozen=True)
class OperationalLane:
    lane_key: str
    lane_label: str
    group_name: str = ""
    radio_id: int = 0
    sop_profile_id: int = 0
    entries: Tuple[OperationalEntry, ...] = ()


@dataclass(frozen=True)
class OperationalDayProjection:
    week_start_utc: dt.date
    lanes: Tuple[OperationalLane, ...]
    cells: Tuple[OperationalCell, ...]
    source_counts: Mapping[str, int]

    def schedule_refs(self) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []
        for lane in self.lanes:
            refs.extend(entry.to_schedule_ref() for entry in lane.entries)
        return refs

    def cells_for_lane(self, lane_key: str) -> Tuple[OperationalCell, ...]:
        key = str(lane_key or "").strip()
        return tuple(cell for cell in self.cells if cell.lane_key == key)

    def cell_for(self, lane_key: str, day_utc: str, hour_utc: int) -> Optional[OperationalCell]:
        key = str(lane_key or "").strip()
        day = str(day_utc or "").strip()
        try:
            hour = int(hour_utc)
        except Exception:
            hour = -1
        for cell in self.cells:
            if cell.lane_key == key and cell.day_utc == day and cell.hour_utc == hour:
                return cell
        return None

    def lane_day_summary(self, lane_key: str, day_utc: str) -> List[str]:
        summaries: List[str] = []
        for cell in self.cells_for_lane(lane_key):
            if cell.day_utc != day_utc or not cell.entries:
                continue
            marker = "!" if cell.has_contention else "-"
            summaries.append(f"{marker} {cell.hour_utc:02d}:00 {cell.display_label}")
        return summaries

    def source_refs(self) -> List[str]:
        mapping = {
            "HF": "hf_daily",
            "NET": "hf_nets",
            "NET_RESOURCE": "net_resources",
            "SOP": "sop",
        }
        refs = [mapping[src] for src in ("HF", "NET", "NET_RESOURCE", "SOP") if self.source_counts.get(src, 0)]
        if self.source_counts.get("SOP", 0):
            for ref in self.schedule_refs():
                if str(ref.get("source") or "").strip().upper() != "SOP":
                    continue
                try:
                    profile_id = int(ref.get("sop_profile_id") or ref.get("profile_id") or 0)
                except Exception:
                    profile_id = 0
                if profile_id > 0:
                    refs.append(f"{SOP_SCHEDULE_LAYER_DEPENDENCY_PREFIX}:{profile_id}")
        return list(dict.fromkeys(refs))

    def frequency_refs(self) -> List[str]:
        refs: List[str] = []
        for row in self.schedule_refs():
            band = str(row.get("band") or "").strip().upper()
            freq = str(row.get("frequency") or "").strip()
            if band and freq:
                refs.append(f"{band}:{freq}")
            elif band:
                refs.append(band)
            elif freq:
                refs.append(freq)
        return list(dict.fromkeys(refs))

    def group_refs(self) -> List[str]:
        refs = [str(row.get("group_name") or "").strip().upper() for row in self.schedule_refs()]
        return list(dict.fromkeys(ref for ref in refs if ref))

    def to_frequency_plan_payload(self, name: str, *, description: str = "") -> Dict[str, Any]:
        notes = {
            "kind": OPERATIONAL_PLAN_CATEGORY,
            "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "lane_count": len(self.lanes),
            "source_counts": dict(self.source_counts),
        }
        return {
            "name": str(name or "SOP Schedule Plan").strip() or "SOP Schedule Plan",
            "status": "saved",
            "category": OPERATIONAL_PLAN_CATEGORY,
            "description": description or "SOP Schedule Plan generated from operational day projection.",
            "source_refs": self.source_refs(),
            "schedule_refs": self.schedule_refs(),
            "frequency_refs": self.frequency_refs(),
            "group_refs": self.group_refs(),
            "notes": json.dumps(notes, sort_keys=True),
        }


def build_operational_day_projection(
    hf_rows: Sequence[Mapping[str, Any]],
    net_rows: Sequence[Mapping[str, Any]],
    sop_rows: Sequence[Mapping[str, Any]],
    net_resource_rows: Sequence[Mapping[str, Any]] | None = None,
    policy_rows: Sequence[Mapping[str, Any]] | None = None,
    *,
    week_start_utc: Optional[dt.date] = None,
) -> OperationalDayProjection:
    week_start = week_start_utc or _current_week_start_sunday_utc()
    blended = build_blended_schedule_projection(
        hf_rows,
        net_rows,
        sop_rows,
        policy_rows or [],
        week_start_utc=week_start,
    )
    entries: List[OperationalEntry] = []
    for segment in blended.source_segments:
        entries.append(_entry_from_segment(segment, week_start))
    folded_resource_keys = _folded_resource_keys(blended.source_segments)
    for row in net_resource_rows or []:
        resource_entries = _entries_from_resource_row(row, week_start)
        if _resource_already_represented(resource_entries, folded_resource_keys):
            continue
        entries.extend(resource_entries)

    lanes = _build_lanes(entries)
    cells = _build_cells(week_start, lanes)
    source_counts: Dict[str, int] = {"HF": 0, "NET": 0, "NET_RESOURCE": 0, "SOP": 0}
    for entry in entries:
        source_counts[entry.source] = source_counts.get(entry.source, 0) + 1
    return OperationalDayProjection(
        week_start_utc=week_start,
        lanes=tuple(lanes),
        cells=tuple(cells),
        source_counts=source_counts,
    )


def build_operational_day_projection_from_refs(
    schedule_refs: Sequence[Mapping[str, Any]],
    *,
    week_start_utc: Optional[dt.date] = None,
) -> OperationalDayProjection:
    week_start = week_start_utc or _current_week_start_sunday_utc()
    entries = [
        _entry_from_schedule_ref(ref, index=index)
        for index, ref in enumerate(schedule_refs)
        if isinstance(ref, Mapping)
    ]
    lanes = _build_lanes(entries)
    cells = _build_cells(week_start, lanes)
    source_counts: Dict[str, int] = {"HF": 0, "NET": 0, "NET_RESOURCE": 0, "SOP": 0}
    for entry in entries:
        source_counts[entry.source] = source_counts.get(entry.source, 0) + 1
    return OperationalDayProjection(
        week_start_utc=week_start,
        lanes=tuple(lanes),
        cells=tuple(cells),
        source_counts=source_counts,
    )


def _current_week_start_sunday_utc() -> dt.date:
    now = dt.datetime.now(dt.timezone.utc)
    delta = (now.weekday() + 1) % 7
    return (now - dt.timedelta(days=delta)).date()


def _entry_from_segment(segment: ScheduleSegment, week_start: dt.date) -> OperationalEntry:
    raw = dict(segment.raw or {})
    source = segment.source
    lane_key, lane_label = _lane_identity(source, raw, segment.group_name, segment.profile_name)
    return OperationalEntry(
        source=source,
        lane_key=lane_key,
        lane_label=lane_label,
        day_utc=segment.day_utc,
        start_utc=segment.start_utc,
        end_utc=segment.end_utc,
        band=segment.band,
        frequency=segment.frequency,
        mode=segment.mode,
        group_name=segment.group_name,
        net_name=segment.net_name,
        profile_name=segment.profile_name,
        action_label=str(raw.get("action_label") or raw.get("action") or "").strip(),
        radio_id=_radio_id_from_raw(raw),
        source_key=str(raw.get("source_key") or "").strip(),
        raw=raw,
    )


def _entry_from_schedule_ref(ref: Mapping[str, Any], *, index: int = -1) -> OperationalEntry:
    raw = dict(ref)
    if index >= 0:
        raw["plan_ref_index"] = index
    source = str(ref.get("source") or "SOP").strip().upper() or "SOP"
    group_name = str(ref.get("group_name") or "").strip().upper()
    profile_name = str(ref.get("profile_name") or "").strip()
    lane_key = str(ref.get("lane_key") or "").strip()
    lane_label = str(ref.get("lane_label") or "").strip()
    if not lane_key:
        lane_key, lane_label = _lane_identity(source, raw, group_name, profile_name)
    elif not lane_label:
        lane_label = lane_key.split(":", 1)[-1].strip() or lane_key
    return OperationalEntry(
        source=source,
        lane_key=lane_key,
        lane_label=lane_label,
        day_utc=str(ref.get("day_utc") or "ALL").strip() or "ALL",
        start_utc=str(ref.get("start_utc") or "00:00").strip() or "00:00",
        end_utc=str(ref.get("end_utc") or "00:00").strip() or "00:00",
        band=str(ref.get("band") or "").strip().upper(),
        frequency=str(ref.get("frequency") or ref.get("freq") or "").strip(),
        mode=str(ref.get("mode") or "").strip(),
        group_name=group_name,
        net_name=str(ref.get("net_name") or "").strip(),
        profile_name=profile_name,
        action_label=str(ref.get("action_label") or ref.get("action") or "").strip(),
        radio_id=_radio_id_from_raw(raw),
        source_key=str(ref.get("source_key") or "").strip(),
        raw=raw,
    )


def _entries_from_resource_row(row: Mapping[str, Any], week_start: dt.date) -> List[OperationalEntry]:
    source_row = dict(row)
    source_row.setdefault("source", "NET_RESOURCE")
    projection = build_blended_schedule_projection([], [source_row], [], [], week_start_utc=week_start)
    entries: List[OperationalEntry] = []
    for segment in projection.effective_segments:
        raw = dict(segment.raw or {})
        raw["source"] = "NET_RESOURCE"
        raw.setdefault("resource_id", row.get("id") or row.get("resource_id") or row.get("_resource_id"))
        raw.setdefault("source_table", "net_resources")
        source_key = str(raw.get("source_key") or "").strip()
        if not source_key and raw.get("resource_id") not in (None, ""):
            source_key = f"NET_RESOURCE:{raw.get('resource_id')}"
            raw["source_key"] = source_key
        lane_key, lane_label = _lane_identity("NET_RESOURCE", raw, segment.group_name, segment.profile_name)
        entries.append(
            OperationalEntry(
                source="NET_RESOURCE",
                lane_key=lane_key,
                lane_label=lane_label,
                day_utc=segment.day_utc,
                start_utc=segment.start_utc,
                end_utc=segment.end_utc,
                band=segment.band,
                frequency=segment.frequency,
                mode=segment.mode,
                group_name=segment.group_name,
                net_name=segment.net_name or str(row.get("net_name") or row.get("name") or "").strip(),
                profile_name=segment.profile_name,
                action_label=str(row.get("action_label") or "Monitor").strip(),
                radio_id=_radio_id_from_raw(row),
                source_key=source_key,
                raw=raw,
            )
        )
    return entries


def _folded_resource_keys(segments: Sequence[ScheduleSegment]) -> set[Tuple[str, str]]:
    keys: set[Tuple[str, str]] = set()
    for segment in segments:
        if segment.source != "NET":
            continue
        raw = dict(segment.raw or {})
        resource_id = str(raw.get("resource_id") or "").strip()
        if resource_id:
            keys.add(("resource_id", resource_id))
        keys.add(("signature", _resource_signature(segment)))
    return keys


def _resource_already_represented(
    entries: Sequence[OperationalEntry],
    folded_resource_keys: set[Tuple[str, str]],
) -> bool:
    if not entries:
        return False
    for entry in entries:
        raw = dict(entry.raw or {})
        resource_id = str(raw.get("resource_id") or "").strip()
        if resource_id and ("resource_id", resource_id) in folded_resource_keys:
            return True
        if ("signature", _entry_signature(entry)) in folded_resource_keys:
            return True
    return False


def _resource_signature(segment: ScheduleSegment) -> str:
    return "|".join(
        (
            segment.day_utc,
            segment.start_utc,
            segment.end_utc,
            segment.band.strip().upper(),
            _normalize_frequency_token(segment.frequency),
            segment.mode.strip().upper(),
            segment.group_name.strip().upper(),
            segment.net_name.strip().upper(),
        )
    )


def _entry_signature(entry: OperationalEntry) -> str:
    return "|".join(
        (
            entry.day_utc,
            entry.start_utc,
            entry.end_utc,
            entry.band.strip().upper(),
            _normalize_frequency_token(entry.frequency),
            entry.mode.strip().upper(),
            entry.group_name.strip().upper(),
            entry.net_name.strip().upper(),
        )
    )


def _normalize_frequency_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return f"{float(text):.6f}".rstrip("0").rstrip(".")
    except Exception:
        return text


def _lane_identity(source: str, raw: Mapping[str, Any], group_name: str, profile_name: str) -> Tuple[str, str]:
    radio_id = _radio_id_from_raw(raw)
    if radio_id > 0:
        return f"radio:{radio_id}", str(raw.get("radio_name") or f"Radio {radio_id}")
    if source == "SOP":
        profile_id = _coerce_int(raw.get("sop_profile_id") or raw.get("profile_id"))
        if profile_id > 0:
            return f"sop:{profile_id}", str(profile_name or raw.get("sop_profile_name") or f"SOP {profile_id}")
    group = str(group_name or raw.get("group_name") or raw.get("primary_js8call_group") or "").strip().upper()
    if group:
        return f"group:{group}", group
    return "station", "Station"


def _build_lanes(entries: Sequence[OperationalEntry]) -> List[OperationalLane]:
    grouped: Dict[str, List[OperationalEntry]] = {}
    labels: Dict[str, str] = {}
    for entry in entries:
        grouped.setdefault(entry.lane_key, []).append(entry)
        labels.setdefault(entry.lane_key, entry.lane_label)
    lanes: List[OperationalLane] = []
    for lane_key in sorted(grouped, key=lambda key: (0 if key.startswith("radio:") else 1, key)):
        lane_entries = tuple(sorted(grouped[lane_key], key=lambda entry: (entry.day_utc, entry.start_utc, entry.source)))
        sample = lane_entries[0] if lane_entries else None
        lanes.append(
            OperationalLane(
                lane_key=lane_key,
                lane_label=labels.get(lane_key, lane_key),
                group_name=(sample.group_name if sample else ""),
                radio_id=(sample.radio_id if sample else 0),
                sop_profile_id=_coerce_int((sample.raw or {}).get("sop_profile_id")) if sample else 0,
                entries=lane_entries,
            )
        )
    return lanes


def _build_cells(week_start: dt.date, lanes: Sequence[OperationalLane]) -> List[OperationalCell]:
    cells: List[OperationalCell] = []
    for lane in lanes:
        for day_index, day in enumerate(("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")):
            date_value = week_start + dt.timedelta(days=day_index)
            for hour in range(24):
                start_dt = dt.datetime.combine(date_value, dt.time(hour=hour), tzinfo=dt.timezone.utc)
                cells.append(
                    OperationalCell(
                        lane_key=lane.lane_key,
                        day_utc=day,
                        hour_utc=hour,
                        start_utc=start_dt,
                        end_utc=start_dt + dt.timedelta(hours=1),
                        entries=tuple(entry for entry in lane.entries if _entry_in_hour(entry, day, hour)),
                    )
                )
    return cells


def _entry_in_hour(entry: OperationalEntry, day: str, hour: int) -> bool:
    start = _hhmm_to_minute(entry.start_utc)
    end = _hhmm_to_minute(entry.end_utc)
    hour_start = hour * 60
    hour_end = hour_start + 60
    if end <= start:
        if entry.day_utc == day:
            return start < hour_end and hour_start < 24 * 60
        if _next_day(entry.day_utc) == day:
            return 0 < hour_end and hour_start < end
        return False
    if entry.day_utc != day:
        return False
    return start < hour_end and hour_start < end


def _next_day(day: str) -> str:
    try:
        idx = ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday").index(day)
    except ValueError:
        return "Sunday"
    return ("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")[(idx + 1) % 7]


def _hhmm_to_minute(value: Any) -> int:
    text = str(value or "00:00").strip()
    try:
        hour_text, minute_text = text.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except Exception:
        return 0
    return max(0, min(23, hour)) * 60 + max(0, min(59, minute))


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _radio_id_from_raw(raw: Mapping[str, Any]) -> int:
    return _coerce_int(
        raw.get("radio_id")
        or raw.get("device_profile_id")
        or raw.get("target_device_profile_id")
    )
