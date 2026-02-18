from __future__ import annotations

import time
from typing import Optional

from PySide6.QtCore import QObject, QTimer

from freqinout.core.logger import log
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.peer_schedule_infer import infer_peer_schedules
from freqinout.core.propagation_outcome_ingest import ingest_propagation_outcomes
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sitrep_fusion import fuse_sitreps
from freqinout.core.sitrep_ingest import ingest_sitreps
from freqinout.core.varac_ingest import ingest_varac
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer
from freqinout.core.config_paths import get_config_dir


class BackgroundIngestController(QObject):
    """
    Background, non-UI data ingest to keep DBs warm for fast tab activation.
    """

    def __init__(self, settings: SettingsManager):
        super().__init__()
        self.settings = settings
        self._msg_ingest = MessageIngestor(settings)
        self._js8_links_timer: Optional[QTimer] = None
        self._messages_timer: Optional[QTimer] = None
        self._varac_timer: Optional[QTimer] = None
        self._sitrep_timer: Optional[QTimer] = None
        self._prop_outcome_timer: Optional[QTimer] = None
        self._peer_sched_timer: Optional[QTimer] = None
        self._running = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        # JS8 links/background ingest: low cadence
        self._js8_links_timer = QTimer(self)
        self._js8_links_timer.setInterval(5 * 60 * 1000)  # 5 minutes
        self._js8_links_timer.timeout.connect(self._ingest_js8_links)
        self._js8_links_timer.start()

        # Messages/spotter ingest: moderate cadence
        self._messages_timer = QTimer(self)
        self._messages_timer.setInterval(90 * 1000)  # 90 seconds
        self._messages_timer.timeout.connect(self._ingest_messages)
        self._messages_timer.start()

        # VarAC ingest: moderate cadence
        self._varac_timer = QTimer(self)
        self._varac_timer.setInterval(2 * 60 * 1000)  # 2 minutes
        self._varac_timer.timeout.connect(self._ingest_varac)
        self._varac_timer.start()

        # Unified SitRep source ingest: moderate cadence
        self._sitrep_timer = QTimer(self)
        self._sitrep_timer.setInterval(2 * 60 * 1000)  # 2 minutes
        self._sitrep_timer.timeout.connect(self._ingest_sitreps)
        self._sitrep_timer.start()

        # Propagation outcome ingest: checkpointed incremental backfill.
        self._prop_outcome_timer = QTimer(self)
        self._prop_outcome_timer.setInterval(3 * 60 * 1000)  # 3 minutes
        self._prop_outcome_timer.timeout.connect(self._ingest_prop_outcomes)
        self._prop_outcome_timer.start()

        # Inferred peer schedule generation: lower cadence.
        self._peer_sched_timer = QTimer(self)
        self._peer_sched_timer.setInterval(6 * 60 * 1000)  # 6 minutes
        self._peer_sched_timer.timeout.connect(self._infer_peer_schedules)
        self._peer_sched_timer.start()

        # Initial staggered ingest
        QTimer.singleShot(2000, self._ingest_js8_links)
        QTimer.singleShot(4000, self._ingest_messages)
        QTimer.singleShot(6000, self._ingest_varac)
        QTimer.singleShot(7000, self._ingest_sitreps)
        QTimer.singleShot(8000, self._ingest_prop_outcomes)
        QTimer.singleShot(9000, self._infer_peer_schedules)

    def stop(self) -> None:
        self._running = False
        for t in (
            self._js8_links_timer,
            self._messages_timer,
            self._varac_timer,
            self._sitrep_timer,
            self._prop_outcome_timer,
            self._peer_sched_timer,
        ):
            if t:
                t.stop()

    def _ingest_js8_links(self) -> None:
        try:
            db_path = get_config_dir() / "config" / "freqinout_nets.db"
            indexer = JS8LogLinkIndexer(self.settings, db_path)
            last_ts = float(self.settings.get("js8_links_last_load_utc", 0) or 0)
            count = indexer.update(since_ts=last_ts if last_ts > 0 else None)
            latest_ts = max(indexer._ensure_latest_ts(last_default=time.time()), time.time())
            try:
                self.settings.set("js8_links_last_load_utc", latest_ts)
            except Exception:
                pass
            if count:
                log.debug("BackgroundIngest: js8_links ingested=%s", count)
        except Exception as e:
            log.debug("BackgroundIngest: js8_links ingest failed: %s", e)

    def _ingest_messages(self) -> None:
        try:
            self._msg_ingest.ingest_js8_messages()
        except Exception as e:
            log.debug("BackgroundIngest: JS8 inbox ingest failed: %s", e)
        try:
            self._msg_ingest.ingest_spotter_from_directed()
        except Exception as e:
            log.debug("BackgroundIngest: spotter ingest failed: %s", e)

    def _ingest_varac(self) -> None:
        try:
            ingest_varac(self.settings)
        except Exception as e:
            log.debug("BackgroundIngest: VarAC ingest failed: %s", e)

    def _ingest_sitreps(self) -> None:
        try:
            stats = ingest_sitreps(self.settings, max_rows_per_source=500)
            if int(stats.get("events_inserted", 0)) > 0:
                log.debug(
                    "BackgroundIngest: sitrep ingest scanned=%s inserted=%s errors=%s",
                    stats.get("rows_scanned", 0),
                    stats.get("events_inserted", 0),
                    stats.get("errors", 0),
                )
            fused = fuse_sitreps(self.settings, max_rows=1000)
            if int(fused.get("events_upserted", 0)) > 0 or int(fused.get("latest_updated", 0)) > 0:
                log.debug(
                    "BackgroundIngest: sitrep fusion scanned=%s upserted=%s latest=%s errors=%s",
                    fused.get("rows_scanned", 0),
                    fused.get("events_upserted", 0),
                    fused.get("latest_updated", 0),
                    fused.get("errors", 0),
                )
        except Exception as e:
            log.debug("BackgroundIngest: sitrep ingest failed: %s", e)

    def _ingest_prop_outcomes(self) -> None:
        try:
            stats = ingest_propagation_outcomes(self.settings, max_rows_per_source=500)
            if int(stats.get("events_inserted", 0)) > 0:
                log.debug(
                    "BackgroundIngest: propagation outcomes scanned=%s inserted=%s stats=%s",
                    stats.get("rows_scanned", 0),
                    stats.get("events_inserted", 0),
                    stats.get("stats_updated", 0),
                )
        except Exception as e:
            log.debug("BackgroundIngest: propagation outcome ingest failed: %s", e)

    def _infer_peer_schedules(self) -> None:
        try:
            stats = infer_peer_schedules(self.settings, lookback_days=56, bucket_minutes=15)
            if int(stats.get("rows_inferred", 0)) > 0:
                log.debug(
                    "BackgroundIngest: peer schedule inference scanned=%s inferred=%s callsigns=%s",
                    stats.get("rows_scanned", 0),
                    stats.get("rows_inferred", 0),
                    stats.get("callsigns_inferred", 0),
                )
        except Exception as e:
            log.debug("BackgroundIngest: peer schedule inference failed: %s", e)
