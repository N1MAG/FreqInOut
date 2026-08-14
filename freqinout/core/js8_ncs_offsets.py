from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from freqinout.core.ingest_source_model import (
    IngestSourceInventory,
    normalize_ingest_path,
    stable_source_id,
)


@dataclass(frozen=True)
class JS8NcsOffsetKeys:
    directed_source_id: str
    all_source_id: str
    directed_offset_key: str
    all_offset_key: str


def ncs_offset_keys_for_directed_path(
    directed_path: object,
    *,
    inventory: IngestSourceInventory | None = None,
) -> JS8NcsOffsetKeys:
    directed_norm = normalize_ingest_path(directed_path)
    all_norm = normalize_ingest_path(Path(directed_norm).with_name("ALL.TXT")) if directed_norm else ""
    directed_source_id = _source_id_for_path(directed_norm, role="directed", inventory=inventory)
    all_source_id = _source_id_for_path(all_norm, role="all", inventory=inventory)
    return JS8NcsOffsetKeys(
        directed_source_id=directed_source_id,
        all_source_id=all_source_id,
        directed_offset_key=f"js8_ncs_directed_offset_{directed_source_id}",
        all_offset_key=f"js8_ncs_all_offset_{all_source_id}",
    )


def _source_id_for_path(
    path: str,
    *,
    role: str,
    inventory: IngestSourceInventory | None = None,
) -> str:
    normalized_role = str(role or "").strip().lower()
    if path and inventory is not None:
        for source in inventory.sources_for_family("js8call"):
            if source.source_type != "file":
                continue
            if str(source.metadata.get("role", "") or "").strip().lower() != normalized_role:
                continue
            if normalize_ingest_path(source.path) == path:
                return str(source.source_id or "")
    return stable_source_id("js8call", "ncs", normalized_role, path, prefix="source")
