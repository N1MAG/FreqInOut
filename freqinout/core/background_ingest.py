from __future__ import annotations

import time
from typing import Optional

from PySide6.QtCore import QObject, QTimer

from freqinout.core.logger import log
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.settings_manager import SettingsManager
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

        # Initial staggered ingest
        QTimer.singleShot(2000, self._ingest_js8_links)
        QTimer.singleShot(4000, self._ingest_messages)
        QTimer.singleShot(6000, self._ingest_varac)

    def stop(self) -> None:
        self._running = False
        for t in (self._js8_links_timer, self._messages_timer, self._varac_timer):
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
