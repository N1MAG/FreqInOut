from __future__ import annotations

import json
import logging
import socket
import sys
import time
from pathlib import Path
from typing import Optional

import psutil

from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.js8_rx_hub import JS8RxHub

log = logging.getLogger(__name__)

# Add vendored js8net to import path
JS8NET_PATH = Path(__file__).resolve().parents[2] / "third_party" / "js8net" / "js8net-main"
if JS8NET_PATH.exists():
    sys.path.insert(0, str(JS8NET_PATH))
try:
    import js8net  # type: ignore
except Exception as e:  # pragma: no cover
    js8net = None
    log.warning("js8net not available: %s", e)


class JS8StatusClient:
    """
    Very lightweight status client for JS8Call.

    We keep is_busy() as a simple TCP probe to avoid spinning up js8net
    for status checks.
    """

    def __init__(self, host: str = "127.0.0.1"):
        self.host = host
        self.settings = SettingsManager()

    def _get_port(self) -> int:
        """
        Prefer the UI key "js8_port" (Settings tab), fall back to the legacy
        "js8_tcp_port". Default to 2442.
        """
        for key in ("js8_port", "js8_tcp_port"):
            try:
                val = self.settings.get(key, None)
                if val is not None:
                    return int(val)
            except Exception:
                continue
        return 2442

    def is_busy(self) -> bool:
        """
        Attempt a quick status query to JS8Call.

        For now, we:
          - Use JS8RxHub to observe recent RX activity and PTT state.
          - If we have recent RX activity or active PTT, return True.
          - On any failure, assume not busy (but log at debug level).
        """
        try:
            hub = JS8RxHub.instance()
            if not hub.start(self.host, self._get_port()):
                return False
            now_ts = time.time()
            if hub.ptt_active():
                return True
            # Treat very recent RX activity as busy to avoid QSY mid-stream.
            if now_ts - hub.last_rx_activity_ts() <= 12.0:
                return True
            return False
        except BaseException as e:
            log.debug("JS8Call status query failed (assuming not busy): %s", e)
            return False


class JS8ControlClient(JS8StatusClient):
    """
    js8net-backed JS8Call controller.
    Call set_frequency() from your rig-control path when control_via == 'JS8Call'.
    """

    def __init__(self, host: str = "127.0.0.1"):
        super().__init__(host=host)
        self._net_started = False

    def _get_port(self) -> int:
        # Prefer settings_tab key, fall back to legacy key
        for key in ("js8_port", "js8_tcp_port"):
            try:
                val = self.settings.get(key, None)
                if val:
                    return int(val)
            except Exception:
                continue
        return 2442

    @staticmethod
    def _js8call_running() -> bool:
        """
        Lightweight process check to avoid spawning JS8Call
        if it is not already running.
        """
        try:
            for proc in psutil.process_iter(attrs=["name", "exe"]):
                try:
                    name = (proc.info.get("name") or "").lower()
                    exe = (proc.info.get("exe") or "").lower()
                    if "js8call" in name or "js8call" in exe:
                        return True
                except Exception:
                    continue
        except Exception:
            return False
        return False

    def _ensure_net(self) -> bool:
        if js8net is None:
            log.error("JS8ControlClient: js8net not available")
            return False
        if not self._js8call_running():
            log.info("JS8ControlClient: JS8Call not running; skipping js8net start.")
            return False
        if not self._net_started:
            try:
                js8net.start_net(self.host, self._get_port())
                self._net_started = True
                log.info("JS8ControlClient: js8net started on %s:%s", self.host, self._get_port())
            except Exception as e:
                log.warning("JS8ControlClient: failed to start js8net: %s", e)
                self._net_started = False
                return False
        return True

    def set_frequency(self, dial_hz: int, offset_hz: Optional[int] = None) -> bool:
        """
        Set JS8Call dial (and optional audio offset) via js8net.
        """
        try:
            if not self._ensure_net():
                return False
            dial_hz = int(dial_hz)
            off = int(offset_hz) if offset_hz is not None else 0
            js8net.set_freq(dial_hz, off)
            log.info("JS8ControlClient set dial=%d Hz%s", dial_hz, "" if offset_hz is None else f" offset={off} Hz")
            return True
        except BaseException as e:
            log.error("JS8ControlClient failed to set frequency: %s", e)
            return False

    def get_frequency(self) -> Optional[int]:
        """
        Return current JS8Call dial frequency in Hz, or None on failure.
        """
        try:
            if not self._ensure_net():
                return None
            resp = js8net.get_freq()
            if not resp:
                return None
            hz = resp.get("dial") or resp.get("freq")
            return int(hz) if hz else None
        except BaseException as e:
            log.debug("JS8ControlClient get_frequency failed: %s", e)
            return None

    def get_offset(self) -> Optional[int]:
        """
        Return current JS8Call audio offset in Hz, or None on failure.
        """
        try:
            if not self._ensure_net():
                return None
            resp = js8net.get_freq()
            if not resp:
                return None
            off = resp.get("offset")
            return int(off) if off is not None else None
        except BaseException as e:
            log.debug("JS8ControlClient get_offset failed: %s", e)
            return None

    def set_offset(self, offset_hz: int) -> bool:
        """
        Explicitly set JS8Call audio offset by reusing current dial.
        """
        try:
            cur = self.get_frequency()
            if cur is None:
                return False
            return self.set_frequency(cur, offset_hz)
        except BaseException as e:
            log.error("JS8ControlClient failed to set offset: %s", e)
            return False

    def stop(self):
        if js8net is None or not self._net_started:
            return
        try:
            sock = getattr(js8net, "s", None)
            if sock:
                sock.close()
        except Exception:
            pass
        self._net_started = False


class VarACStatusClient:
    """VarAC busy check using VarAC_traffic.log in the install folder."""

    def __init__(self) -> None:
        self.settings = SettingsManager()

    def _resolve_log_path(self) -> Optional[Path]:
        raw_install = (self.settings.get("varac_path", "") or "").strip()
        raw_db = (self.settings.get("varac_db_path", "") or "").strip()
        base: Optional[Path] = None
        if raw_install:
            base = Path(raw_install)
        elif raw_db:
            p = Path(raw_db)
            base = p.parent if p.is_file() or p.suffix.lower() == ".db" else p
        if not base:
            return None
        return base / "VarAC_traffic.log"

    def is_busy(self) -> bool:
        log_path = self._resolve_log_path()
        if not log_path or not log_path.exists():
            return False
        try:
            # Read tail to avoid loading large logs
            with log_path.open("rb") as fh:
                try:
                    fh.seek(-8192, 2)
                except OSError:
                    fh.seek(0)
                tail = fh.read().decode("utf-8", errors="replace")
            lines = [ln.strip() for ln in tail.splitlines() if ln.strip()]
            last_state: Optional[str] = None
            for line in lines:
                upper = line.upper()
                if "INCOMING CONNECTION REQUEST" in upper:
                    last_state = "incoming"
                elif "CONNECTED TO" in upper:
                    last_state = "connected"
                elif "DISCONNECTED FROM" in upper:
                    last_state = "disconnected"
            return last_state in {"incoming", "connected"}
        except Exception as e:
            log.debug("VarACStatusClient: failed to read log: %s", e)
            return False
