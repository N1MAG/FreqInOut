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
          - Open a short-lived TCP connection to JS8Call API port.
          - Send a simple STATUS request (if JS8Call supports it).
          - If we can detect an active TX/RX, return True.
          - On any failure, assume not busy (but log at debug level).
        """
        port = self._get_port()
        try:
            with socket.create_connection((self.host, port), timeout=0.3) as s:
                payload = json.dumps({"type": "STATUS"}) + "\n"
                s.sendall(payload.encode("utf-8"))

                s.settimeout(0.3)
                data = s.recv(4096)
                if not data:
                    return False

                for line in data.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line.decode("utf-8"))
                    except Exception:
                        continue
                    state = msg.get("state") or msg.get("TRX")
                    if state in ("TX", "RX", "BUSY"):
                        log.debug("JS8Call reported busy state: %s", state)
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
        # js8net has no explicit stop; rely on process exit
        pass


class VarACStatusClient:
    """Deprecated: VarAC is not managed by FreqInOut."""

    def is_busy(self) -> bool:
        return False
