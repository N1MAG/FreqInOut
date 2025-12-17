from __future__ import annotations

import json
import logging
import socket
import time
from typing import Optional

import psutil

from freqinout.core.settings_manager import SettingsManager

log = logging.getLogger(__name__)


class JS8StatusClient:
    """
    Very lightweight status client for JS8Call.

    This is intentionally conservative: if JS8Call is unreachable or the
    protocol is not as expected, we simply log and return "not busy".

    A more complete implementation could maintain a persistent connection
    and parse RX/TX frames per the official JS8Call API.
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
                # Simple "STATUS" query; many JS8Call API examples use JSON lines.
                # This is a minimal placeholder. Adjust to match your JS8Call version.
                payload = json.dumps({"type": "STATUS"}) + "\n"
                s.sendall(payload.encode("utf-8"))

                s.settimeout(0.3)
                data = s.recv(4096)
                if not data:
                    return False

                # Try to parse one JSON object per line
                for line in data.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line.decode("utf-8"))
                    except Exception:
                        continue

                    # Heuristic: if we see something that looks like TX/RX state, treat as busy
                    # Adjust keys based on your JS8Call API version.
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
    pyjs8call-backed JS8Call controller. Uses one-shot clients for dial/offset setters.
    Call set_frequency() from your rig-control path when control_via == 'JS8Call'.
    """

    def __init__(self, host: str = "127.0.0.1"):
        super().__init__(host=host)
        try:
            import pyjs8call  # type: ignore
            self._pyjs8call = pyjs8call
        except BaseException as e:  # pragma: no cover - runtime guard
            self._pyjs8call = None
            log.warning("pyjs8call not available: %s", e)

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
        Lightweight process check to avoid spawning JS8Call via pyjs8call
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

    def _ensure_client(self):
        # Deprecated; replaced by one-shot clients
        return self._new_client()

    def _new_client(self):
        """
        Create a short-lived pyjs8call client, start it, and return it.
        Caller should call stop().
        """
        if self._pyjs8call is None:
            raise RuntimeError("pyjs8call is not installed")
        if not self._js8call_running():
            log.info("JS8ControlClient: JS8Call not running; skipping client start.")
            return None
        client = self._pyjs8call.Client(host=self.host, port=self._get_port())
        try:
            client.start()
            # Ensure client.stop() (or __del__) will not attempt to close JS8Call
            try:
                if hasattr(client, "stop"):
                    client._stop_noop = client.stop  # keep ref
                    client.stop = lambda *a, **k: None
            except Exception:
                pass
            return client
        except BaseException as e:  # catch SystemExit from pyjs8call too
            log.warning("JS8ControlClient failed to start: %s", e)
            return None

    def set_frequency(self, dial_hz: int, offset_hz: Optional[int] = None) -> bool:
        """
        Set JS8Call dial (and optional audio offset) via pyjs8call.
        """
        try:
            client = self._ensure_client()
            if client is None:
                return False
            dial_hz = int(dial_hz)
            client.settings.set_freq(dial_hz)
            if offset_hz is not None:
                try:
                    offset_hz = int(offset_hz)
                    client.settings.set_offset(offset_hz)
                except BaseException as e:
                    log.warning("JS8ControlClient failed to set offset=%s: %s", offset_hz, e)
            log.info("JS8ControlClient set dial=%d Hz%s", dial_hz, "" if offset_hz is None else f" offset={offset_hz} Hz")
            return True
        except BaseException as e:
            log.error("JS8ControlClient failed to set frequency: %s", e)
            return False

    def get_frequency(self) -> Optional[int]:
        """
        Return current JS8Call dial frequency in Hz, or None on failure.
        """
        try:
            client = self._ensure_client()
            if client is None:
                return None
            hz = client.settings.get_freq()
            return int(hz) if hz is not None else None
        except BaseException as e:
            log.debug("JS8ControlClient get_frequency failed: %s", e)
            return None

    def get_offset(self) -> Optional[int]:
        """
        Return current JS8Call audio offset in Hz, or None on failure.
        """
        try:
            client = self._ensure_client()
            if client is None:
                return None
            off = client.settings.get_offset()
            return int(off) if off is not None else None
        except BaseException as e:
            log.debug("JS8ControlClient get_offset failed: %s", e)
            return None

    def set_offset(self, offset_hz: int) -> bool:
        """
        Explicitly set JS8Call audio offset.
        """
        try:
            client = self._ensure_client()
            if client is None:
                return False
            offset_hz = int(offset_hz)
            client.settings.set_offset(offset_hz)
            log.info("JS8ControlClient set offset=%d Hz", offset_hz)
            return True
        except BaseException as e:
            log.error("JS8ControlClient failed to set offset: %s", e)
            return False

    def stop(self):
        # No persistent client to stop in one-shot mode
        pass



class VarACStatusClient:
    """Deprecated: VarAC is not managed by FreqInOut."""

    def is_busy(self) -> bool:
        return False
