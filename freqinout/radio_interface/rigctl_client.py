from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from xmlrpc.client import ServerProxy

log = logging.getLogger(__name__)


@dataclass
class FrequencyCommand:
    """
    Represents a frequency change request to the rig or JS8Call.

    Accepts both the legacy 'frequency_hz' (used by older SchedulerEngine code)
    and the newer 'rig_hz' name. Extra fields (offsets, band, js8 group) are
    ignored by FLRig but passed through by callers for JS8Call where relevant.
    """

    # Preferred field names
    rig_hz: Optional[int] = None
    fldigi_center_hz: Optional[int] = None
    js8_tune_hz: Optional[int] = None
    band: Optional[str] = None

    # Legacy / shared fields
    frequency_hz: Optional[int] = None
    mode: Optional[str] = None
    vfo: Optional[str] = None
    js8_group: Optional[str] = None

    @property
    def hz(self) -> int:
        """
        Return the chosen frequency in Hz, preferring rig_hz but falling back
        to legacy frequency_hz for compatibility.
        """
        if self.rig_hz is not None:
            return int(self.rig_hz)
        if self.frequency_hz is not None:
            return int(self.frequency_hz)
        raise ValueError("FrequencyCommand missing rig_hz/frequency_hz")


class FLRigClient:
    """
    Minimal XML-RPC client for FLRig.

    Default FLRig server address is 127.0.0.1:12345.

    Uses documented XML-RPC methods, for example: :contentReference[oaicite:1]{index=1}
      - main.get_version
      - rig.get_ptt
      - rig.get_vfo
      - rig.set_AB
      - rig.set_verify_frequency
      - rig.set_mode
      - rig.tune
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 12345,
        fldigi_port: int = 7362,
        fldigi_host: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self._proxy: Optional[ServerProxy] = None
        self.fldigi_port = fldigi_port
        self.fldigi_host = fldigi_host or host
        self._fldigi_proxy: Optional[ServerProxy] = None

    # ------------- INTERNAL -------------

    def _connect(self) -> ServerProxy:
        if self._proxy is None:
            url = f"http://{self.host}:{self.port}"
            log.info("Connecting to FLRig XML-RPC at %s", url)
            self._proxy = ServerProxy(url, allow_none=True)
        return self._proxy

    def _connect_fldigi(self) -> Optional[ServerProxy]:
        if self._fldigi_proxy is None:
            url = f"http://{self.fldigi_host}:{self.fldigi_port}"
            try:
                log.info("Connecting to FLDigi XML-RPC at %s", url)
                self._fldigi_proxy = ServerProxy(url, allow_none=True)
            except Exception as e:
                log.warning("FLDigi XML-RPC connect failed: %s", e)
                self._fldigi_proxy = None
        return self._fldigi_proxy

    def _set_fldigi_wfhz(self, offset_hz: Optional[int]) -> None:
        """
        Best-effort FLDigi waterfall offset via XML-RPC using the documented
        script syntax: FLDIGI.WFHZ:<offset>. Non-fatal if FLDigi is unavailable.
        Tries both "fldigi.main.shell" and fallback "main.shell".
        """
        if offset_hz is None:
            return
        proxy = self._connect_fldigi()
        if proxy is None:
            return
        cmd = f"FLDIGI.WFHZ:{int(offset_hz)}"
        for path in ("fldigi.main.shell", "main.shell"):
            try:
                fn = proxy
                for part in path.split("."):
                    fn = getattr(fn, part)
                fn(cmd)
                log.info("Set FLDigi WFHZ via %s to %s Hz", path, offset_hz)
                return
            except Exception as e:
                log.debug("FLDigi WFHZ via %s failed: %s", path, e)
        log.warning("Failed to set FLDigi WFHZ to %s Hz (all paths tried).", offset_hz)

    # ------------- STATUS METHODS -------------

    def is_available(self) -> bool:
        """
        Quick health check: ask FLRig for its version.
        """
        try:
            proxy = self._connect()
            _ = proxy.main.get_version()
            return True
        except Exception as e:
            log.debug("FLRig not available: %s", e)
            return False

    def get_ptt(self) -> bool:
        """
        Returns True if FLRig reports PTT active (transmitting).
        """
        try:
            proxy = self._connect()
            state = proxy.rig.get_ptt()
            return bool(state)
        except Exception as e:
            log.warning("Failed to get PTT from FLRig: %s", e)
            return False

    def get_vfo_frequency(self) -> Optional[int]:
        """
        Returns the current VFO frequency in Hz, or None on failure.
        """
        try:
            proxy = self._connect()
            freq_str = proxy.rig.get_vfo()  # documented as "return current VFO in Hz" :contentReference[oaicite:2]{index=2}
            return int(float(freq_str))
        except Exception as e:
            log.warning("Failed to get VFO frequency from FLRig: %s", e)
            return None

    # ------------- CONTROL METHODS -------------

    def set_frequency(self, cmd: FrequencyCommand) -> bool:
        """
        Set rig frequency (and optionally mode/VFO) via FLRig.
        """
        try:
            proxy = self._connect()

            # Select VFO if requested
            if cmd.vfo in ("A", "B"):
                log.info("Setting FLRig VFO to %s", cmd.vfo)
                proxy.rig.set_AB(cmd.vfo)

            # Set and verify frequency in Hz
            freq_hz = cmd.hz
            freq = float(freq_hz)
            log.info("Setting FLRig frequency to %d Hz", freq_hz)
            proxy.rig.set_verify_frequency(freq)

            # Optional mode change (if you configure it to be allowed)
            if cmd.mode:
                log.info("Setting FLRig mode to %s", cmd.mode)
                proxy.rig.set_mode(cmd.mode)

            return True
        except Exception as e:
            log.error("Failed to set frequency via FLRig: %s", e)
            return False

    def tune(self) -> bool:
        """
        Ask FLRig to run the rig's tune function (if supported).
        """
        try:
            proxy = self._connect()
            log.info("Invoking FLRig tune()")
            proxy.rig.tune()
            return True
        except Exception as e:
            log.error("Failed to start tune via FLRig: %s", e)
            return False
