from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from freqinout.core.ingest_runtime_status import active_runtime_ingest_inventory
from freqinout.core.ingest_source_model import IngestSourceInventory
from freqinout.core.logger import log
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.runtime_profile_settings import RuntimeProfileSettings
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.varac_ingest import ingest_varac


@dataclass(frozen=True)
class VarACRuntimeIngestResult:
    used_runtime_sources: bool = False
    sources_attempted: int = 0
    sources_succeeded: int = 0
    source_labels: tuple[str, ...] = field(default_factory=tuple)


def ingest_varac_for_runtime_sources(
    settings: SettingsManager,
    *,
    inventory: IngestSourceInventory | None = None,
    profiles: Sequence[Mapping[str, Any]] | None = None,
) -> VarACRuntimeIngestResult:
    runtime_inventory = inventory if inventory is not None else active_runtime_ingest_inventory()
    varac_sources = tuple(runtime_inventory.sources_for_family("varac"))
    if not varac_sources:
        success = bool(ingest_varac(settings))
        return VarACRuntimeIngestResult(
            used_runtime_sources=False,
            sources_attempted=1,
            sources_succeeded=1 if success else 0,
            source_labels=("Legacy VarAC",),
        )

    profile_rows = list(profiles) if profiles is not None else _active_runtime_profiles()
    profiles_by_id = {
        str(profile.get("id", "") or profile.get("system_key", "") or "").strip(): profile
        for profile in profile_rows
    }
    labels: list[str] = []
    succeeded = 0
    attempted = 0
    for source in varac_sources:
        profile = profiles_by_id.get(str(source.radio_id or ""))
        if profile is None:
            profile = {
                "id": str(source.radio_id or ""),
                "name": source.label,
                "use_varac": True,
                "varac_db_path": source.path,
            }
        label = str(source.label or source.source_id)
        labels.append(label)
        attempted += 1
        try:
            profile_settings = RuntimeProfileSettings(profile, settings)  # type: ignore[arg-type]
            if ingest_varac(
                profile_settings,
                ingest_source_key=source.source_id,
                ingest_scope="runtime-active",
                ingest_source_label=label,
            ):
                succeeded += 1
        except Exception as exc:
            log.debug("VarAC runtime ingest: source ingest failed for %s: %s", label, exc)
    return VarACRuntimeIngestResult(
        used_runtime_sources=True,
        sources_attempted=attempted,
        sources_succeeded=succeeded,
        source_labels=tuple(labels),
    )


def _active_runtime_profiles() -> list[Mapping[str, Any]]:
    try:
        store = MultiRadioStore()
        return [dict(row) for row in store.list_runtime_active_device_profiles()]
    except Exception:
        return []
