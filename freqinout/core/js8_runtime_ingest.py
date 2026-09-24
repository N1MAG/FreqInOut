from __future__ import annotations

import time
import sqlite3
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
    force_rebuild: bool = False,
) -> JS8RuntimeLinkIngestResult:
    """
    Index JS8Call link traffic using runtime source descriptors when available.

    This keeps normal operating views source-aware in multi-rig mode while
    preserving the legacy single-source path for older/single-rig setups.
    """
    target_db = Path(db_path)
    indexer = JS8LogLinkIndexer(settings, target_db)
    was_empty = indexer.link_count() == 0
    runtime_inventory = inventory if inventory is not None else active_runtime_ingest_inventory()
    js8_sources = tuple(runtime_inventory.sources_for_family("js8call"))
    file_sources = tuple(
        source
        for source in js8_sources
        if str(getattr(source, "source_type", "") or "").strip().lower() == "file"
    )
    if file_sources:
        if force_rebuild:
            try:
                conn = sqlite3.connect(target_db)
                try:
                    indexer._ensure_table(conn)
                    indexer._clear_table(conn)
                finally:
                    conn.close()
            except Exception:
                log.debug("JS8 runtime ingest: failed to clear js8_links before forced rebuild", exc_info=True)
        counts = indexer.update_from_ingest_sources(
            file_sources,
            since_ts=None if force_rebuild else since_ts,
            force_rebuild=force_rebuild,
        )
        inserted = sum(int(value or 0) for value in counts.values())
        if inserted <= 0 and was_empty:
            log.info("JS8 runtime ingest: rebuilding empty js8_links from runtime log sources")
            counts = indexer.update_from_ingest_sources(
                file_sources,
                since_ts=None,
                force_rebuild=True,
            )
            inserted = sum(int(value or 0) for value in counts.values())
        latest_ts = max(indexer._ensure_latest_ts(last_default=time.time()), time.time())
        return JS8RuntimeLinkIngestResult(
            inserted=inserted,
            latest_ts=latest_ts,
            used_runtime_sources=True,
            counts_by_source=dict(counts),
        )

    try:
        count = int(indexer.update(since_ts=None if force_rebuild else since_ts, force_rebuild=force_rebuild) or 0)
        if count <= 0 and was_empty:
            log.info("JS8 runtime ingest: rebuilding empty js8_links from legacy log source")
            count = int(indexer.update(since_ts=None, force_rebuild=True) or 0)
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
