from __future__ import annotations

import datetime
import json
import logging
import re
import socket
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

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
    """VarAC busy check using VarAC traffic/main logs in the install folder."""

    def __init__(self) -> None:
        self.settings = SettingsManager()
        self._last_status: Dict[str, object] = {}

    def _operator_callsign(self) -> str:
        return (self.settings.get("operator_callsign", "") or "").strip().upper()

    def _split_events(self, text: str) -> List[str]:
        pattern = re.compile(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} - ")
        matches = list(pattern.finditer(text))
        if not matches:
            return [ln.strip() for ln in text.splitlines() if ln.strip()]
        events: List[str] = []
        prefix = text[: matches[0].start()].strip()
        if prefix:
            events.append(" ".join(prefix.splitlines()).strip())
        for idx, match in enumerate(matches):
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            chunk = text[start:end].strip()
            if chunk:
                cleaned = " ".join(chunk.splitlines()).strip()
                if cleaned:
                    events.append(cleaned)
        return events

    def _evaluate_status(self, text: str) -> Dict[str, object]:
        now_local = datetime.datetime.now()
        events = self._split_events(text)
        callsign = self._operator_callsign()
        last_connecting: Optional[datetime.datetime] = None
        last_connected: Optional[datetime.datetime] = None
        last_disconnected: Optional[datetime.datetime] = None
        last_incoming: Optional[datetime.datetime] = None
        last_no_luck: Optional[datetime.datetime] = None
        last_broadcast: Optional[datetime.datetime] = None
        last_broadcast_complete: Optional[bool] = None
        last_wait_freq: Optional[datetime.datetime] = None
        last_file_wait: Optional[datetime.datetime] = None
        last_transfer: Optional[datetime.datetime] = None
        last_transfer_done: Optional[datetime.datetime] = None

        def _is_newer(a: Optional[datetime.datetime], b: Optional[datetime.datetime]) -> bool:
            return a is not None and (b is None or a > b)

        for raw in events:
            upper = raw.upper()
            ts_val: Optional[datetime.datetime] = None
            if len(raw) >= 19:
                try:
                    ts_val = datetime.datetime.strptime(raw[:19], "%d/%m/%Y %H:%M:%S")
                except Exception:
                    ts_val = None

            if "WAITING FOR FREQUENCY TO CLEAR" in upper:
                last_wait_freq = ts_val or now_local
                continue
            if "INCOMING CONNECTION REQUEST" in upper:
                last_incoming = ts_val or now_local
                continue
            if "NO LUCK." in upper:
                last_no_luck = ts_val or now_local
                continue
            if "CONNECTING " in upper:
                last_connecting = ts_val or now_local
                continue
            if "CONNECTED TO" in upper:
                last_connected = ts_val or now_local
                continue
            if "DISCONNECTED FROM" in upper:
                last_disconnected = ts_val or now_local
                continue
            if "FILE SENT. WAITING FOR CONFIRMATION OF RECEIPT" in upper:
                last_file_wait = ts_val or now_local
                last_transfer = ts_val or now_local
                continue
            if "RECEIVING FILE TRANSFER DATA" in upper:
                last_transfer = ts_val or now_local
                continue
            if (
                "SENDFILE HEADER RECEIVED" in upper
                or "INCOMING FILE PACKET" in upper
                or "CONVERTING FILE" in upper
                or "WRITING FILE TO DISK" in upper
            ):
                last_transfer = ts_val or now_local
                continue
            if "FILE SUCCESSFULLY RECEIVED" in upper or "FILE SUCCESSFULLY SENT" in upper:
                last_transfer_done = ts_val or now_local
                continue
            if " - BROADCAST - " in upper:
                last_broadcast = ts_val or now_local
                last_broadcast_complete = raw.rstrip().endswith("-")
                continue
            if "<SENDING ASYNC MESSAGE>" in upper and "VMAIL RELAY NOTIFICATION" in upper:
                continue
            if "SENDING" in upper and "BEACON" in upper:
                if callsign and upper.rstrip().endswith(f"DE {callsign}"):
                    continue

        waiting_for_frequency = _is_newer(last_wait_freq, last_disconnected) and _is_newer(
            last_wait_freq, last_connected
        )
        connected_active = _is_newer(last_connected, last_disconnected)
        connecting_active = (
            _is_newer(last_connecting, last_disconnected)
            and _is_newer(last_connecting, last_connected)
        )
        incoming_active = (
            _is_newer(last_incoming, last_no_luck)
            and _is_newer(last_incoming, last_connected)
            and _is_newer(last_incoming, last_disconnected)
        )
        file_wait_active = _is_newer(last_file_wait, last_disconnected)
        transfer_active = _is_newer(last_transfer, last_disconnected)
        broadcast_active = False
        if last_broadcast and last_broadcast_complete is False:
            delta = abs((now_local - last_broadcast).total_seconds())
            broadcast_active = delta <= 12.0

        busy = bool(
            waiting_for_frequency
            or connecting_active
            or connected_active
            or incoming_active
            or file_wait_active
            or transfer_active
            or broadcast_active
        )
        reason = None
        if waiting_for_frequency:
            reason = "waiting_for_frequency"
        elif connecting_active:
            reason = "connecting"
        elif connected_active:
            reason = "connected"
        elif incoming_active:
            reason = "incoming"
        elif file_wait_active:
            reason = "file_wait"
        elif transfer_active:
            reason = "transfer"
        elif broadcast_active:
            reason = "broadcast_incomplete"

        return {
            "busy": busy,
            "waiting_for_frequency": waiting_for_frequency,
            "reason": reason,
        }

    def _resolve_log_paths(self) -> List[Path]:
        raw_install = (self.settings.get("varac_path", "") or "").strip()
        raw_db = (self.settings.get("varac_db_path", "") or "").strip()
        bases: List[Path] = []
        if raw_install:
            bases.append(Path(raw_install))
        elif raw_db:
            p = Path(raw_db)
            bases.append(p.parent if p.is_file() or p.suffix.lower() == ".db" else p)
        # Fallback: detect VarAC install folder from running process.
        if not bases:
            try:
                for proc in psutil.process_iter(attrs=["name", "exe", "cmdline"]):
                    try:
                        name = (proc.info.get("name") or "").lower()
                        exe = (proc.info.get("exe") or "").strip()
                        if "varac" not in name and "varac" not in exe.lower():
                            continue
                        if exe:
                            bases.append(Path(exe).parent)
                            continue
                        cmdline = proc.info.get("cmdline") or []
                        first = str(cmdline[0]).strip() if cmdline else ""
                        if first:
                            bases.append(Path(first).parent)
                    except Exception:
                        continue
            except Exception:
                pass
        # De-duplicate while preserving order.
        uniq_bases: List[Path] = []
        seen: set[str] = set()
        for base in bases:
            key = str(base).lower()
            if key in seen:
                continue
            seen.add(key)
            uniq_bases.append(base)

        out: List[Path] = []
        for base in uniq_bases:
            for name in ("VarAC_traffic.log", "VarAC.log", "varalog.log"):
                p = base / name
                if p.exists():
                    out.append(p)
        return out

    @staticmethod
    def _read_tail(path: Path, max_bytes: int = 16384) -> str:
        try:
            with path.open("rb") as fh:
                try:
                    fh.seek(-max_bytes, 2)
                except OSError:
                    fh.seek(0)
                return fh.read().decode("utf-8", errors="replace")
        except Exception:
            return ""

    def get_status(self) -> Dict[str, object]:
        log_paths = self._resolve_log_paths()
        if not log_paths:
            self._last_status = {"busy": False, "waiting_for_frequency": False, "reason": None}
            return self._last_status
        try:
            # Read tails from available VarAC logs. Main and traffic logs carry
            # different event types depending on VarAC version/settings.
            chunks = [self._read_tail(p) for p in log_paths]
            text = "\n".join([c for c in chunks if c])
            status = self._evaluate_status(text)
            self._last_status = status
            return status
        except Exception as e:
            log.debug("VarACStatusClient: failed to read log: %s", e)
            self._last_status = {"busy": False, "waiting_for_frequency": False, "reason": None}
            return self._last_status

    def is_busy(self) -> bool:
        status = self.get_status()
        return bool(status.get("busy"))
