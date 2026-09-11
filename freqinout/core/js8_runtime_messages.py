from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from freqinout.core.ingest_runtime_status import active_runtime_ingest_inventory
from freqinout.core.ingest_source_model import IngestSourceInventory
from freqinout.core.logger import log
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.runtime_profile_settings import RuntimeProfileSettings
from freqinout.core.settings_manager import SettingsManager


@dataclass(frozen=True)
class JS8RuntimeMessageIngestResult:
    used_runtime_sources: bool = False
    js8_inbox_sources: int = 0
    spotter_sources: int = 0
    spotter_inserted: int = 0
    source_labels: tuple[str, ...] = field(default_factory=tuple)


def ingest_js8_messages_for_runtime_sources(
    settings: SettingsManager,
    *,
    inventory: IngestSourceInventory | None = None,
    profiles: Sequence[Mapping[str, Any]] | None = None,
    evaluate_expect: bool = False,
    force_rebuild: bool = False,
) -> JS8RuntimeMessageIngestResult:
    """
    Warm the local JS8/Spotter message cache from runtime JS8Call sources.

    UI refresh paths use this helper to avoid assuming one global JS8Call
    profile. Expect auto-reply dispatch remains opt-in and is normally owned by
    the background runtime coordinator.
    """
    runtime_inventory = inventory if inventory is not None else active_runtime_ingest_inventory()
    js8_instances = tuple(instance for instance in runtime_inventory.app_instances if instance.family == "js8call")
    if not js8_instances:
        ingestor = MessageIngestor(settings)
        ingestor.ingest_js8_messages()
        inserted = ingestor.ingest_spotter_from_directed(
            evaluate_expect=evaluate_expect,
            force_rebuild=force_rebuild,
        )
        return JS8RuntimeMessageIngestResult(
            used_runtime_sources=False,
            js8_inbox_sources=1,
            spotter_sources=1,
            spotter_inserted=int(inserted or 0),
            source_labels=("Legacy JS8Call",),
        )

    profile_rows = list(profiles) if profiles is not None else _active_runtime_profiles()
    profiles_by_id = {
        str(profile.get("id", "") or profile.get("system_key", "") or "").strip(): profile
        for profile in profile_rows
    }
    js8_count = 0
    spotter_count = 0
    spotter_inserted = 0
    labels: list[str] = []
    sources = tuple(runtime_inventory.sources_for_family("js8call"))
    for source in sources:
        metadata = dict(source.metadata or {})
        role = str(metadata.get("role", "") or "").strip().lower()
        if role not in {"inbox", "directed"}:
            continue
        radio_id = str(source.radio_id or "").strip()
        profile = profiles_by_id.get(radio_id)
        if profile is None:
            candidate_ids = tuple(str(value or "").strip() for value in metadata.get("candidate_radio_ids", ()) if str(value or "").strip())
            profile = next((profiles_by_id.get(value) for value in candidate_ids if profiles_by_id.get(value) is not None), {})
        profile_settings = RuntimeProfileSettings(profile or {}, settings)  # type: ignore[arg-type]
        js8_instance_id = str(metadata.get("js8_instance_id", "") or "") if radio_id else ""
        label = str(source.label or source.source_id)
        if label not in labels:
            labels.append(label)
        if role == "inbox" and source.path:
            try:
                projection_source_key = str(source.app_instance_id or source.source_id)
                MessageIngestor(profile_settings).ingest_js8_messages(
                    inbox_path=Path(source.path).expanduser(),
                    source_radio_id=radio_id,
                    js8_instance_id=js8_instance_id,
                    source_key=projection_source_key,
                )
                js8_count += 1
            except Exception as exc:
                log.debug("JS8 runtime messages: inbox ingest failed for %s: %s", label, exc)
            continue
        if role != "directed" or not source.path:
            continue
        try:
            projection_source_key = str(source.app_instance_id or source.source_id)
            inserted = MessageIngestor(profile_settings).ingest_spotter_from_directed(
                directed_path=Path(str(source.path or "")).expanduser(),
                source_radio_id=radio_id,
                js8_instance_id=js8_instance_id,
                source_key=projection_source_key,
                offset_key=f"spotter_directed_offset_{source.source_id}",
                evaluate_expect=evaluate_expect,
                force_rebuild=force_rebuild,
            )
            spotter_count += 1
            spotter_inserted += int(inserted or 0)
        except Exception as exc:
            log.debug("JS8 runtime messages: Spotter directed ingest failed for %s: %s", label, exc)
    return JS8RuntimeMessageIngestResult(
        used_runtime_sources=True,
        js8_inbox_sources=js8_count,
        spotter_sources=spotter_count,
        spotter_inserted=spotter_inserted,
        source_labels=tuple(labels),
    )


def inbox_path_for_directed_source(directed_source: object | None) -> Path | None:
    raw_path = str(getattr(directed_source, "path", "") or "").strip() if directed_source is not None else ""
    if not raw_path:
        return None
    directed_path = Path(raw_path).expanduser()
    parent = directed_path.parent
    candidates = (
        parent / "inbox_v1",
        parent / "inbox_v1.sqlite",
        parent / "inbox_v1.db",
        parent / "inbox.db3",
    )
    for candidate in candidates:
        try:
            if candidate.exists() and candidate.is_file():
                return candidate
        except Exception:
            continue
    try:
        for candidate in sorted(parent.glob("inbox*")):
            if candidate.is_file():
                return candidate
    except Exception:
        return None
    return None


def inbox_path_from_profile(profile: Mapping[str, Any] | None) -> Path | None:
    if not profile:
        return None
    for key in ("js8_inbox_path", "inbox_path", "js8call_inbox_path"):
        raw = str(profile.get(key, "") or "").strip()
        if not raw:
            continue
        candidate = Path(raw).expanduser()
        try:
            if candidate.exists() and candidate.is_file():
                return candidate
        except Exception:
            continue
    return None


def _active_runtime_profiles() -> list[Mapping[str, Any]]:
    try:
        store = MultiRadioStore()
        return [dict(row) for row in store.list_runtime_active_device_profiles()]
    except Exception:
        return []


def _profile_from_instance(instance: object, directed_source: object | None) -> Mapping[str, Any]:
    return {
        "id": str(getattr(instance, "radio_id", "") or ""),
        "name": str(getattr(instance, "label", "") or "JS8Call"),
        "use_js8call": True,
        "use_js8spotter": True,
        "js8_directed_path": str(getattr(directed_source, "path", "") or ""),
    }
