from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional

from freqinout.core.ingest_runtime_status import active_runtime_ingest_inventory
from freqinout.core.ingest_source_model import IngestSourceInventory
from freqinout.core.js8_log_link_indexer import JS8LogLinkIndexer
from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager


@dataclass(frozen=True)
class JS8RuntimeLinkIngestResult:
    inserted: int = 0
    latest_ts: float = 0.0
    used_runtime_sources: bool = False
    counts_by_source: Mapping[str, int] = field(default_factory=dict)


def ingest_js8_links_for_runtime_sources(
    settings: SettingsManager,
    db_path: str | Path,
    *,
    since_ts: Optional[float] = None,
    inventory: IngestSourceInventory | None = None,
) -> JS8RuntimeLinkIngestResult:
    """
    Index JS8Call link traffic using runtime source descriptors when available.

    This keeps normal operating views source-aware in multi-rig mode while
    preserving the legacy single-source path for older/single-rig setups.
    """
    target_db = Path(db_path)
    indexer = JS8LogLinkIndexer(settings, target_db)
    runtime_inventory = inventory if inventory is not None else active_runtime_ingest_inventory()
    js8_sources = tuple(runtime_inventory.sources_for_family("js8call"))
    file_sources = tuple(
        source
        for source in js8_sources
        if str(getattr(source, "source_type", "") or "").strip().lower() == "file"
    )
    if file_sources:
        counts = indexer.update_from_ingest_sources(file_sources, since_ts=since_ts)
        latest_ts = max(indexer._ensure_latest_ts(last_default=time.time()), time.time())
        return JS8RuntimeLinkIngestResult(
            inserted=sum(int(value or 0) for value in counts.values()),
            latest_ts=latest_ts,
            used_runtime_sources=True,
            counts_by_source=dict(counts),
        )

    try:
        count = int(indexer.update(since_ts=since_ts) or 0)
    except Exception:
        log.exception("JS8 runtime ingest: legacy JS8 link update failed")
        raise
    latest_ts = max(indexer._ensure_latest_ts(last_default=time.time()), time.time())
    return JS8RuntimeLinkIngestResult(
        inserted=count,
        latest_ts=latest_ts,
        used_runtime_sources=False,
        counts_by_source={"legacy": count},
    )
