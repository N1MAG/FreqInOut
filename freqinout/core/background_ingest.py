from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Dict, Optional

from PySide6.QtCore import QObject, QTimer

from freqinout.core.logger import log
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.peer_schedule_infer import infer_peer_schedules
from freqinout.core.propagation_outcome_ingest import ingest_propagation_outcomes
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.sitrep_fusion import fuse_sitreps
from freqinout.core.sitrep_ingest import ingest_sitreps
from freqinout.core.varac_ingest import ingest_varac
from freqinout.core.varac_bbs_vault import run_varac_bbs_vault
from freqinout.core.varac_guard import run_varac_guard
from freqinout.gui.stations_map_tab import JS8LogLinkIndexer


class BackgroundIngestController(QObject):
    """
    Background, non-UI data ingest to keep DBs warm for fast tab activation.
    """

    def __init__(self, settings: SettingsManager):
        super().__init__()
        self.settings = settings
        self._js8_links_timer: Optional[QTimer] = None
        self._messages_timer: Optional[QTimer] = None
        self._varac_timer: Optional[QTimer] = None
        self._varac_guard_timer: Optional[QTimer] = None
        self._sitrep_timer: Optional[QTimer] = None
        self._prop_outcome_timer: Optional[QTimer] = None
        self._peer_sched_timer: Optional[QTimer] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._executor_lock = threading.RLock()
        self._job_futures: Dict[str, Future] = {}
        self._running = False

    def start(self, *, initial_stagger: bool = True) -> None:
        if self._running:
            return
        self._running = True
        self._ensure_executor()
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

        # VGuard-style file protection: opt-in, lower cadence.
        self._varac_guard_timer = QTimer(self)
        self._varac_guard_timer.setInterval(90 * 1000)  # 90 seconds
        self._varac_guard_timer.timeout.connect(self._ingest_varac_guard)
        self._varac_guard_timer.start()

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
        if initial_stagger:
            QTimer.singleShot(2000, self._ingest_js8_links)
            QTimer.singleShot(4000, self._ingest_messages)
            QTimer.singleShot(6000, self._ingest_varac)
            QTimer.singleShot(7000, self._ingest_varac_guard)
            QTimer.singleShot(8000, self._ingest_sitreps)
            QTimer.singleShot(9000, self._ingest_prop_outcomes)
            QTimer.singleShot(10000, self._infer_peer_schedules)

    def stop(self) -> None:
        self._running = False
        for t in (
            self._js8_links_timer,
            self._messages_timer,
            self._varac_timer,
            self._varac_guard_timer,
            self._sitrep_timer,
            self._prop_outcome_timer,
            self._peer_sched_timer,
        ):
            if t:
                t.stop()
        self._shutdown_executor()

    def _ensure_executor(self) -> ThreadPoolExecutor:
        with self._executor_lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="freqinout-ingest",
                )
            return self._executor

    def _shutdown_executor(self) -> None:
        with self._executor_lock:
            futures = list(self._job_futures.values())
            self._job_futures.clear()
            executor = self._executor
            self._executor = None
        for future in futures:
            try:
                future.cancel()
            except Exception:
                pass
        if executor is None:
            return
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            executor.shutdown(wait=False)
        except Exception as e:
            log.debug("BackgroundIngest: executor shutdown failed: %s", e)

    def _new_worker_settings(self) -> SettingsManager:
        return SettingsManager()

    def _submit_job(self, job_name: str, job_func: Callable[[], None]) -> None:
        if not self._running:
            return
        with self._executor_lock:
            future = self._job_futures.get(job_name)
            if future is not None and not future.done():
                log.debug("BackgroundIngest: job already running, skipping trigger: %s", job_name)
                return
            executor = self._ensure_executor()
            future = executor.submit(self._run_job, job_name, job_func)
            self._job_futures[job_name] = future
        future.add_done_callback(lambda done, name=job_name: self._on_job_done(name, done))

    def _run_job(self, job_name: str, job_func: Callable[[], None]) -> None:
        started_at = time.time()
        try:
            job_func()
        except Exception as e:
            log.debug("BackgroundIngest: %s worker failed: %s", job_name, e)
        finally:
            elapsed = time.time() - started_at
            if elapsed >= 1.0:
                log.debug("BackgroundIngest: %s completed in %.2fs", job_name, elapsed)

    def _on_job_done(self, job_name: str, future: Future) -> None:
        with self._executor_lock:
            current = self._job_futures.get(job_name)
            if current is future:
                self._job_futures.pop(job_name, None)
        try:
            future.result()
        except Exception as e:
            log.debug("BackgroundIngest: %s future failed: %s", job_name, e)

    def _ingest_js8_links(self) -> None:
        self._submit_job("js8_links", self._run_js8_links_job)

    def _run_js8_links_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            db_path = worker_settings.config_dir / "freqinout_nets.db"
            indexer = JS8LogLinkIndexer(worker_settings, db_path)
            last_ts = float(worker_settings.get("js8_links_last_load_utc", 0) or 0)
            count = indexer.update(since_ts=last_ts if last_ts > 0 else None)
            latest_ts = max(indexer._ensure_latest_ts(last_default=time.time()), time.time())
            try:
                worker_settings.set("js8_links_last_load_utc", latest_ts)
            except Exception:
                pass
            if count:
                log.debug("BackgroundIngest: js8_links ingested=%s", count)
        except Exception as e:
            log.debug("BackgroundIngest: js8_links ingest failed: %s", e)

    def _ingest_messages(self) -> None:
        self._submit_job("messages", self._run_messages_job)

    def _run_messages_job(self) -> None:
        worker_settings = self._new_worker_settings()
        msg_ingest = MessageIngestor(worker_settings)
        try:
            msg_ingest.ingest_js8_messages()
        except Exception as e:
            log.debug("BackgroundIngest: JS8 inbox ingest failed: %s", e)
        try:
            msg_ingest.ingest_spotter_from_directed()
        except Exception as e:
            log.debug("BackgroundIngest: spotter ingest failed: %s", e)

    def _ingest_varac(self) -> None:
        self._submit_job("varac", self._run_varac_job)

    def _ingest_varac_guard(self) -> None:
        self._submit_job("varac_guard", self._run_varac_guard_job)

    def _run_varac_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            ingest_varac(worker_settings)
        except Exception as e:
            log.debug("BackgroundIngest: VarAC ingest failed: %s", e)

    def _run_varac_guard_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            vault_result = run_varac_bbs_vault(worker_settings)
            if bool(vault_result.enabled) and (
                int(vault_result.processed_events or 0) > 0 or bool(vault_result.published)
            ):
                log.debug("BackgroundIngest: VarAC vault %s", vault_result.summary)
        except Exception as e:
            log.debug("BackgroundIngest: VarAC vault failed: %s", e)
        try:
            result = run_varac_guard(worker_settings)
            if int(result.scanned_events or 0) > 0:
                log.debug("BackgroundIngest: VarAC guard %s", result.summary)
        except Exception as e:
            log.debug("BackgroundIngest: VarAC guard failed: %s", e)

    def _ingest_sitreps(self) -> None:
        self._submit_job("sitreps", self._run_sitreps_job)

    def _run_sitreps_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            stats = ingest_sitreps(worker_settings, max_rows_per_source=500)
            if int(stats.get("events_inserted", 0)) > 0:
                log.debug(
                    "BackgroundIngest: sitrep ingest scanned=%s inserted=%s errors=%s",
                    stats.get("rows_scanned", 0),
                    stats.get("events_inserted", 0),
                    stats.get("errors", 0),
                )
            fused = fuse_sitreps(worker_settings, max_rows=1000)
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
        self._submit_job("prop_outcomes", self._run_prop_outcomes_job)

    def _run_prop_outcomes_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            stats = ingest_propagation_outcomes(worker_settings, max_rows_per_source=500)
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
        self._submit_job("peer_schedules", self._run_peer_schedule_job)

    def _run_peer_schedule_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            stats = infer_peer_schedules(worker_settings, lookback_days=56, bucket_minutes=15)
            if int(stats.get("rows_inferred", 0)) > 0:
                log.debug(
                    "BackgroundIngest: peer schedule inference scanned=%s inferred=%s callsigns=%s",
                    stats.get("rows_scanned", 0),
                    stats.get("rows_inferred", 0),
                    stats.get("callsigns_inferred", 0),
                )
        except Exception as e:
            log.debug("BackgroundIngest: peer schedule inference failed: %s", e)
