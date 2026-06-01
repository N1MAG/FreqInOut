from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Dict, Optional

from PySide6.QtCore import QObject, QTimer, Signal

from freqinout.core.dependency_health import get_dependency_health_registry
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

    _VARAC_VAULT_ENABLED_INTERVAL_MS = 5_000
    _VARAC_VAULT_DEGRADED_INTERVAL_MS = 60_000
    _VARAC_VAULT_DISABLED_INTERVAL_MS = 30_000
    _controller_thread_call = Signal(object)

    def __init__(self, settings: SettingsManager):
        super().__init__()
        self.settings = settings
        self._js8_links_timer: Optional[QTimer] = None
        self._messages_timer: Optional[QTimer] = None
        self._varac_timer: Optional[QTimer] = None
        self._varac_vault_timer: Optional[QTimer] = None
        self._varac_guard_timer: Optional[QTimer] = None
        self._sitrep_timer: Optional[QTimer] = None
        self._prop_outcome_timer: Optional[QTimer] = None
        self._peer_sched_timer: Optional[QTimer] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._executor_lock = threading.RLock()
        self._job_futures: Dict[str, Future] = {}
        self._realtime_executor: Optional[ThreadPoolExecutor] = None
        self._realtime_executor_lock = threading.RLock()
        self._realtime_job_futures: Dict[str, Future] = {}
        self._job_skipped_counts: Dict[str, int] = {}
        self._job_started_at: Dict[str, float] = {}
        self._job_timeout_warned: set[str] = set()
        self._job_watchdog_timer: Optional[QTimer] = None
        self._health = get_dependency_health_registry()
        self._running = False
        self._controller_thread_call.connect(self._run_controller_thread_call)

    def _run_controller_thread_call(self, callback: object) -> None:
        if not callable(callback):
            return
        try:
            callback()
        except Exception as e:
            log.debug("BackgroundIngest: queued controller-thread callback failed: %s", e)

    def _queue_controller_thread_call(self, callback: Callable[[], None]) -> None:
        self._controller_thread_call.emit(callback)

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

        self._varac_vault_timer = QTimer(self)
        self._varac_vault_timer.timeout.connect(self._ingest_varac_vault)
        self._update_varac_vault_timer_state()

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

        self._job_watchdog_timer = QTimer(self)
        self._job_watchdog_timer.setInterval(5000)
        self._job_watchdog_timer.timeout.connect(self._check_long_running_jobs)
        self._job_watchdog_timer.start()

        # Initial staggered ingest
        if initial_stagger:
            QTimer.singleShot(2000, self._ingest_js8_links)
            QTimer.singleShot(4000, self._ingest_messages)
            QTimer.singleShot(6000, self._ingest_varac)
            if self._varac_vault_enabled():
                QTimer.singleShot(6500, self._ingest_varac_vault)
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
            self._varac_vault_timer,
            self._varac_guard_timer,
            self._sitrep_timer,
            self._prop_outcome_timer,
            self._peer_sched_timer,
            self._job_watchdog_timer,
        ):
            if t:
                t.stop()
        self._shutdown_executor()
        self._shutdown_realtime_executor()

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

    def _ensure_realtime_executor(self) -> ThreadPoolExecutor:
        with self._realtime_executor_lock:
            if self._realtime_executor is None:
                self._realtime_executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="freqinout-realtime",
                )
            return self._realtime_executor

    def _shutdown_realtime_executor(self) -> None:
        with self._realtime_executor_lock:
            futures = list(self._realtime_job_futures.values())
            self._realtime_job_futures.clear()
            executor = self._realtime_executor
            self._realtime_executor = None
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
            log.debug("BackgroundIngest: realtime executor shutdown failed: %s", e)

    def _new_worker_settings(self) -> SettingsManager:
        return SettingsManager()

    @staticmethod
    def _truthy(value: object, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        txt = str(value or "").strip().lower()
        if txt in {"1", "true", "yes", "on", "enabled"}:
            return True
        if txt in {"0", "false", "no", "off", "disabled"}:
            return False
        return bool(default)

    def _varac_vault_enabled(self) -> bool:
        try:
            return self._truthy(self.settings.get("varac_bbs_vault_enabled", False), False)
        except Exception:
            return False

    def _update_varac_vault_timer_state(self) -> None:
        timer = self._varac_vault_timer
        if timer is None:
            return
        enabled = self._varac_vault_enabled()
        health = self._health.snapshot(self._job_health_key("varac_vault"))
        degraded = bool(health.get("degraded")) and float(health.get("cooldown_remaining_sec") or 0.0) > 0
        timer.setInterval(
            self._VARAC_VAULT_DEGRADED_INTERVAL_MS
            if enabled and degraded
            else self._VARAC_VAULT_ENABLED_INTERVAL_MS
            if enabled
            else self._VARAC_VAULT_DISABLED_INTERVAL_MS
        )
        if not self._running:
            return
        if enabled and not timer.isActive():
            timer.start()
        elif not enabled and timer.isActive():
            timer.stop()

    def refresh_runtime_settings(self) -> None:
        self._update_varac_vault_timer_state()

    def _submit_job(self, job_name: str, job_func: Callable[[], None]) -> None:
        if not self._running:
            return
        health_key = self._job_health_key(job_name)
        may_run, health = self._health.may_run(health_key, owner="BackgroundIngest")
        if not may_run:
            self._job_skipped_counts[job_name] = self._job_skipped_counts.get(job_name, 0) + 1
            log.debug(
                "BackgroundIngest: backing off %s for %.1fs",
                job_name,
                float(health.get("cooldown_remaining_sec") or 0.0),
            )
            return
        with self._executor_lock:
            future = self._job_futures.get(job_name)
            if future is not None and not future.done():
                self._job_skipped_counts[job_name] = self._job_skipped_counts.get(job_name, 0) + 1
                log.debug("BackgroundIngest: job already running, skipping trigger: %s", job_name)
                return
            executor = self._ensure_executor()
            self._job_started_at[job_name] = time.time()
            self._job_timeout_warned.discard(job_name)
            future = executor.submit(self._run_job, job_name, job_func)
            self._job_futures[job_name] = future
        future.add_done_callback(lambda done, name=job_name: self._on_job_done(name, done))

    def _run_job(self, job_name: str, job_func: Callable[[], None]) -> None:
        started_at = time.time()
        failed = False
        try:
            job_func()
        except Exception as e:
            failed = True
            log.debug("BackgroundIngest: %s worker failed: %s", job_name, e)
        finally:
            elapsed = time.time() - started_at
            elapsed_ms = elapsed * 1000.0
            health_key = self._job_health_key(job_name)
            if failed:
                self._health.record_failure(
                    health_key,
                    owner="BackgroundIngest",
                    error="worker failed",
                    duration_ms=elapsed_ms,
                )
            else:
                self._health.record_success(
                    health_key,
                    owner="BackgroundIngest",
                    duration_ms=elapsed_ms,
                    slow_ms=self._job_success_slow_ms(job_name),
                )
            if elapsed >= 1.0:
                log.debug("BackgroundIngest: %s completed in %.2fs", job_name, elapsed)

    def _on_job_done(self, job_name: str, future: Future) -> None:
        with self._executor_lock:
            current = self._job_futures.get(job_name)
            if current is future:
                self._job_futures.pop(job_name, None)
                self._job_started_at.pop(job_name, None)
                self._job_timeout_warned.discard(job_name)
        try:
            future.result()
        except Exception as e:
            log.debug("BackgroundIngest: %s future failed: %s", job_name, e)

    def _submit_realtime_job(self, job_name: str, job_func: Callable[[], None]) -> None:
        if not self._running:
            return
        health_key = self._job_health_key(job_name)
        may_run, health = self._health.may_run(health_key, owner="BackgroundIngest")
        if not may_run:
            self._job_skipped_counts[job_name] = self._job_skipped_counts.get(job_name, 0) + 1
            if job_name == "varac_vault":
                self._update_varac_vault_timer_state()
            log.debug(
                "BackgroundIngest: backing off realtime %s for %.1fs",
                job_name,
                float(health.get("cooldown_remaining_sec") or 0.0),
            )
            return
        with self._realtime_executor_lock:
            future = self._realtime_job_futures.get(job_name)
            if future is not None and not future.done():
                self._job_skipped_counts[job_name] = self._job_skipped_counts.get(job_name, 0) + 1
                return
            executor = self._ensure_realtime_executor()
            self._job_started_at[job_name] = time.time()
            self._job_timeout_warned.discard(job_name)
            future = executor.submit(self._run_job, job_name, job_func)
            self._realtime_job_futures[job_name] = future
        future.add_done_callback(lambda done, name=job_name: self._on_realtime_job_done(name, done))

    def _on_realtime_job_done(self, job_name: str, future: Future) -> None:
        with self._realtime_executor_lock:
            current = self._realtime_job_futures.get(job_name)
            if current is future:
                self._realtime_job_futures.pop(job_name, None)
                self._job_started_at.pop(job_name, None)
                self._job_timeout_warned.discard(job_name)
        if job_name == "varac_vault":
            self._queue_controller_thread_call(self._update_varac_vault_timer_state)
        try:
            future.result()
        except Exception as e:
            log.debug("BackgroundIngest: realtime %s future failed: %s", job_name, e)

    def _job_timeout_seconds(self, job_name: str) -> float:
        if job_name in {"varac_vault", "varac_guard"}:
            return 30.0
        return 90.0

    def _job_success_slow_ms(self, job_name: str) -> float:
        if job_name in {"varac_vault", "varac_guard"}:
            return max(5000.0, self._job_timeout_seconds(job_name) * 2000.0)
        return 5000.0

    def _check_long_running_jobs(self) -> None:
        now = time.time()
        for job_name, started_at in list(self._job_started_at.items()):
            threshold = self._job_timeout_seconds(job_name)
            elapsed = max(0.0, now - float(started_at or 0.0))
            if elapsed < threshold or job_name in self._job_timeout_warned:
                continue
            self._job_timeout_warned.add(job_name)
            self._health.record_failure(
                self._job_health_key(job_name),
                owner="BackgroundIngest",
                error=f"job running longer than {int(threshold)}s",
                duration_ms=elapsed * 1000.0,
            )
            log.warning("BackgroundIngest: %s has been running for %.1fs", job_name, elapsed)

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
        finally:
            worker_settings.close()

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
        finally:
            worker_settings.close()

    def _ingest_varac(self) -> None:
        self._submit_job("varac", self._run_varac_job)

    def _ingest_varac_vault(self) -> None:
        self._update_varac_vault_timer_state()
        if not self._varac_vault_enabled():
            return
        self._submit_realtime_job("varac_vault", self._run_varac_vault_job)

    @staticmethod
    def _job_health_key(job_name: str) -> str:
        return f"background-ingest:{str(job_name or '').strip().lower() or 'unknown'}"

    def _ingest_varac_guard(self) -> None:
        self._submit_job("varac_guard", self._run_varac_guard_job)

    def _run_varac_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            ingest_varac(worker_settings)
        except Exception as e:
            log.debug("BackgroundIngest: VarAC ingest failed: %s", e)
        finally:
            worker_settings.close()

    def _run_varac_vault_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            vault_result = run_varac_bbs_vault(worker_settings)
            if bool(vault_result.enabled) and (
                int(vault_result.processed_events or 0) > 0 or bool(vault_result.published)
            ):
                log.debug("BackgroundIngest: VarAC vault %s", vault_result.summary)
        except Exception as e:
            log.debug("BackgroundIngest: VarAC vault failed: %s", e)
        finally:
            worker_settings.close()

    def _run_varac_guard_job(self) -> None:
        worker_settings = self._new_worker_settings()
        try:
            result = run_varac_guard(worker_settings)
            if int(result.scanned_events or 0) > 0:
                log.debug("BackgroundIngest: VarAC guard %s", result.summary)
        except Exception as e:
            log.debug("BackgroundIngest: VarAC guard failed: %s", e)
        finally:
            worker_settings.close()

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
        finally:
            worker_settings.close()

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
        finally:
            worker_settings.close()

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
        finally:
            worker_settings.close()
