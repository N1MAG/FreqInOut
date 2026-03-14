from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Optional

from xmlrpc.client import ServerProxy, Transport

from freqinout.core.mode_utils import voice_sideband_for_band

log = logging.getLogger(__name__)


def _settings_text(settings: object, key: str, default: str = "") -> str:
    try:
        getter = getattr(settings, "get", None)
        if callable(getter):
            return str(getter(key, default) or "").strip()
    except Exception:
        pass
    return str(default or "").strip()


def _settings_int(settings: object, key: str, default: int) -> int:
    try:
        getter = getattr(settings, "get", None)
        if callable(getter):
            value = getter(key, default)
            return int(value if value not in (None, "") else default)
    except Exception:
        pass
    return int(default)


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
        timeout: float = 0.8,
    ):
        self.host = host
        self.port = port
        self._proxy: Optional[ServerProxy] = None
        self._lock = threading.Lock()
        self.fldigi_port = fldigi_port
        self.fldigi_host = fldigi_host or host
        self._fldigi_proxy: Optional[ServerProxy] = None
        self.timeout = float(timeout)

    def _transport(self) -> Transport:
        timeout = self.timeout

        class TimeoutTransport(Transport):
            def __init__(self, timeout_val: float):
                super().__init__()
                self._timeout_val = timeout_val

            def make_connection(self, host):
                conn = super().make_connection(host)
                try:
                    conn.timeout = self._timeout_val
                except Exception:
                    pass
                return conn

        return TimeoutTransport(timeout)

    # ------------- INTERNAL -------------

    def _connect(self) -> ServerProxy:
        if self._proxy is None:
            url = f"http://{self.host}:{self.port}"
            log.info("Connecting to FLRig XML-RPC at %s", url)
            self._proxy = ServerProxy(url, allow_none=True, transport=self._transport())
        return self._proxy

    def _with_proxy(self, func, *, label: str) -> object:
        """
        Serialize access to the XML-RPC proxy and retry once on transport glitches.
        """
        with self._lock:
            try:
                proxy = self._connect()
                return func(proxy)
            except Exception as e:
                msg = str(e)
                if "Request-sent" in msg or "Idle" in msg:
                    log.warning("FLRig XML-RPC transient error during %s; reconnecting.", label)
                    self._proxy = None
                    proxy = self._connect()
                    return func(proxy)
                raise

    def _connect_fldigi(self) -> Optional[ServerProxy]:
        if self._fldigi_proxy is None:
            url = f"http://{self.fldigi_host}:{self.fldigi_port}"
            try:
                log.info("Connecting to FLDigi XML-RPC at %s", url)
                self._fldigi_proxy = ServerProxy(url, allow_none=True, transport=self._transport())
            except Exception as e:
                log.warning("FLDigi XML-RPC connect failed: %s", e)
                self._fldigi_proxy = None
        return self._fldigi_proxy

    def is_fldigi_available(self) -> bool:
        proxy = self._connect_fldigi()
        if proxy is None:
            return False
        try:
            _ = proxy.modem.get_name()
            return True
        except Exception as e:
            log.debug("FLDigi XML-RPC probe failed: %s", e)
            self._fldigi_proxy = None
            return False

    def set_fldigi_mode_offset(self, mode: Optional[str], offset_hz: Optional[int]) -> bool:
        proxy = self._connect_fldigi()
        if proxy is None:
            return False
        try:
            if mode:
                log.info("Setting FLDigi mode to %s", mode)
                proxy.modem.set_by_name(mode)
            if offset_hz is not None:
                log.info("Setting FLDigi carrier offset to %s Hz", offset_hz)
                proxy.modem.set_carrier(int(offset_hz))
            return True
        except Exception as e:
            log.warning("Failed to set FLDigi mode/offset: %s", e)
            self._fldigi_proxy = None
            return False

    def get_fldigi_offset(self) -> Optional[int]:
        proxy = self._connect_fldigi()
        if proxy is None:
            return None
        try:
            val = proxy.modem.get_carrier()
            return int(val) if val is not None else None
        except Exception as e:
            log.debug("Failed to read FLDigi carrier offset: %s", e)
            self._fldigi_proxy = None
            return None

    def get_fldigi_mode(self) -> Optional[str]:
        proxy = self._connect_fldigi()
        if proxy is None:
            return None
        try:
            mode = proxy.modem.get_name()
            return str(mode).strip() if mode else None
        except Exception as e:
            log.debug("Failed to read FLDigi mode: %s", e)
            self._fldigi_proxy = None
            return None

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
            state = self._with_proxy(lambda p: p.rig.get_ptt(), label="get_ptt")
            return bool(state)
        except Exception as e:
            log.warning("Failed to get PTT from FLRig: %s", e)
            return False

    def get_vfo_frequency(self) -> Optional[int]:
        """
        Returns the current VFO frequency in Hz, or None on failure.
        """
        try:
            freq_str = self._with_proxy(
                lambda p: p.rig.get_vfo(),
                label="get_vfo",
            )  # documented as "return current VFO in Hz" :contentReference[oaicite:2]{index=2}
            return int(float(freq_str))
        except Exception as e:
            log.warning("Failed to get VFO frequency from FLRig: %s", e)
            return None

    # ------------- CONTROL METHODS -------------

    @staticmethod
    def _normalize_rig_mode(mode: Optional[str], band: Optional[str]) -> Optional[str]:
        txt = str(mode or "").strip()
        if not txt:
            return None
        up = txt.upper()
        if up in {"USB", "LSB"}:
            return up
        if up in {"SSB", "VOICE"}:
            return voice_sideband_for_band(band)
        if up in {"DIGI", "DIGITAL", "DATA"}:
            # FLRig expects explicit data mode labels, not "DIGI".
            return "DATA-U"
        return txt

    def set_frequency(self, cmd: FrequencyCommand) -> bool:
        """
        Set rig frequency (and optionally mode/VFO) via FLRig.
        """
        try:
            def _do_set(p):
                freq_hz = cmd.hz
                freq = float(freq_hz)
                target_vfo = cmd.vfo if cmd.vfo in ("A", "B") else None

                def _set_vfo(vfo: str) -> None:
                    log.info("Setting FLRig VFO to %s", vfo)
                    p.rig.set_AB(vfo)

                def _set_mode(mode_cmd: str, vfo: Optional[str]) -> None:
                    if vfo == "A":
                        try:
                            p.rig.set_verify_modeA(mode_cmd)
                            return
                        except Exception:
                            try:
                                p.rig.set_modeA(mode_cmd)
                                return
                            except Exception:
                                pass
                    elif vfo == "B":
                        try:
                            p.rig.set_verify_modeB(mode_cmd)
                            return
                        except Exception:
                            try:
                                p.rig.set_modeB(mode_cmd)
                                return
                            except Exception:
                                pass
                    try:
                        p.rig.set_verify_mode(mode_cmd)
                    except Exception:
                        p.rig.set_mode(mode_cmd)

                def _set_frequency(freq_val: float, vfo: Optional[str]) -> None:
                    if vfo == "A":
                        try:
                            p.rig.set_verify_vfoA(freq_val)
                            return
                        except Exception:
                            p.rig.set_vfoA(freq_val)
                            return
                    if vfo == "B":
                        try:
                            p.rig.set_verify_vfoB(freq_val)
                            return
                        except Exception:
                            p.rig.set_vfoB(freq_val)
                            return
                    try:
                        p.rig.set_verify_frequency(freq_val)
                    except Exception:
                        p.rig.set_frequency(freq_val)

                def _readback_frequency(vfo: Optional[str]) -> int:
                    if vfo == "A":
                        try:
                            return int(float(p.rig.get_vfoA()))
                        except Exception:
                            pass
                    elif vfo == "B":
                        try:
                            return int(float(p.rig.get_vfoB()))
                        except Exception:
                            pass
                    return int(float(p.rig.get_vfo()))

                # Select target VFO first so subsequent mode/frequency calls apply
                # to the intended side.
                if target_vfo:
                    _set_vfo(target_vfo)

                mode_cmd = self._normalize_rig_mode(cmd.mode, cmd.band)
                if mode_cmd:
                    log.info("Setting FLRig mode to %s", mode_cmd)
                    _set_mode(mode_cmd, target_vfo)

                # Apply frequency after mode so rigs with band-stack-per-mode do
                # not drift off the requested target frequency.
                log.info("Setting FLRig frequency to %d Hz", freq_hz)
                _set_frequency(freq, target_vfo)

                # Re-assert selected VFO; some rigs can flip active side during
                # mode/frequency writes.
                if target_vfo:
                    try:
                        _set_vfo(target_vfo)
                    except Exception:
                        pass
                    active_mismatch = False
                    try:
                        active_raw = str(p.rig.get_AB() or "").strip().upper()
                        active_vfo = active_raw[:1] if active_raw else ""
                        if active_vfo in {"A", "B"} and active_vfo != target_vfo:
                            active_mismatch = True
                    except Exception:
                        active_mismatch = False

                # Explicit readback check so scheduler can back off when rig does
                # not converge to requested frequency.
                try:
                    verify_hz = _readback_frequency(target_vfo)
                except Exception as e:
                    log.warning("Failed to verify FLRig frequency readback: %s", e)
                    return False
                if abs(verify_hz - freq_hz) > 20:
                    log.warning(
                        "FLRig frequency verify mismatch: target=%dHz readback=%dHz",
                        freq_hz,
                        verify_hz,
                    )
                    return False
                if target_vfo and active_mismatch:
                    log.warning(
                        "FLRig VFO verify mismatch: target=%s readback=%s (frequency verified on target VFO)",
                        target_vfo,
                        active_vfo,
                    )
                return True

            return bool(self._with_proxy(_do_set, label="set_frequency"))
        except Exception as e:
            log.error("Failed to set frequency via FLRig: %s", e)
            return False

    def tune(self) -> bool:
        """
        Ask FLRig to run the rig's tune function (if supported).
        """
        try:
            log.info("Invoking FLRig tune()")
            self._with_proxy(lambda p: p.rig.tune(), label="tune")
            return True
        except Exception as e:
            log.error("Failed to start tune via FLRig: %s", e)
            return False


def flrig_client_from_settings(settings: object) -> FLRigClient:
    """
    Build an FLRig client from persisted settings.

    Host support is tolerant of future settings keys without requiring the UI
    to expose them yet.
    """
    host = _settings_text(settings, "flrig_host", "127.0.0.1") or "127.0.0.1"
    port = _settings_int(settings, "flrig_port", 12345)
    fldigi_port = _settings_int(settings, "fldigi_port", 7362)
    fldigi_host = _settings_text(settings, "fldigi_host", "") or None
    return FLRigClient(host=host, port=port, fldigi_port=fldigi_port, fldigi_host=fldigi_host)
