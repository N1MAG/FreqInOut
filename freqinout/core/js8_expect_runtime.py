from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

from freqinout.core.js8_send_service import js8_endpoint_from_radio_profile
from freqinout.core.multi_radio_store import normalize_rf_guard_mode, stricter_rf_guard_mode
from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.js8_api_client import JS8ApiClient, JS8ApiClientRegistry, JS8ApiEndpoint


GuardPreflightCallback = Callable[[str, str, Mapping[str, Any]], Any]
ClientFactory = Callable[[JS8ApiEndpoint], JS8ApiClient]


@dataclass(frozen=True)
class ExpectAutomationSourceStatus:
    ok: bool
    reason: str
    radio_id: str = ""
    js8_instance_id: str = ""
    endpoint: Optional[JS8ApiEndpoint] = None
    blocking: bool = True


@dataclass(frozen=True)
class ExpectAutomationRuntimeState:
    enabled: bool
    paused: bool
    active: bool
    reason: str


def _truthy(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _source_key(radio_id: object, js8_instance_id: object = "") -> tuple[str, str]:
    return str(radio_id or "").strip(), str(js8_instance_id or "").strip()


def load_expect_automation_runtime_state(settings: SettingsManager) -> ExpectAutomationRuntimeState:
    try:
        enabled = _truthy(settings.get("js8_expect_unattended_auto_reply_enabled", False), False)
    except Exception:
        enabled = False
    try:
        paused = _truthy(settings.get("js8_expect_unattended_auto_reply_paused", False), False)
    except Exception:
        paused = False
    if not enabled:
        reason = "Runtime unattended Expect auto-reply is disabled."
    elif paused:
        reason = "Runtime unattended Expect auto-reply is paused."
    else:
        reason = "Runtime unattended Expect auto-reply is enabled."
    return ExpectAutomationRuntimeState(enabled=enabled, paused=paused, active=bool(enabled and not paused), reason=reason)


def set_expect_automation_runtime_state(
    settings: SettingsManager,
    *,
    enabled: Optional[bool] = None,
    paused: Optional[bool] = None,
    save: bool = True,
) -> ExpectAutomationRuntimeState:
    if enabled is not None:
        settings.set("js8_expect_unattended_auto_reply_enabled", bool(enabled))
    if paused is not None:
        settings.set("js8_expect_unattended_auto_reply_paused", bool(paused))
    if save and hasattr(settings, "save"):
        try:
            settings.save()
        except Exception:
            pass
    return load_expect_automation_runtime_state(settings)


def _norm_group(value: object) -> str:
    return str(value or "").strip().upper()


def _int_or_none(value: object) -> Optional[int]:
    try:
        parsed = int(float(value))  # type: ignore[arg-type]
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _snapshot_value(snapshot: object, key: str, default: object = "") -> object:
    if isinstance(snapshot, Mapping):
        return snapshot.get(key, default)
    return getattr(snapshot, key, default)


def _profile_guard_group(profile: Mapping[str, Any], snapshot: object, profile_key: str, snapshot_key: str) -> str:
    return _norm_group(profile.get(profile_key) or _snapshot_value(snapshot, snapshot_key, ""))


def _profile_guard_mode(profile: Mapping[str, Any], key: str, default: str = "warn") -> str:
    return normalize_rf_guard_mode(profile.get(key, default), default)


def _profile_guard_window(profile: Mapping[str, Any], key: str = "advanced_frequency_guard_window_hz") -> int:
    try:
        return max(0, int(profile.get(key, 0) or 0))
    except Exception:
        return 0


def build_expect_rf_guard_preflight(station_runtime_manager: object) -> GuardPreflightCallback:
    def _preflight(radio_id: str, js8_instance_id: str, profile: Mapping[str, Any]) -> ExpectAutomationSourceStatus:
        try:
            snapshots = list(station_runtime_manager.get_runtime_snapshots(force=True))  # type: ignore[attr-defined]
        except Exception as exc:
            return ExpectAutomationSourceStatus(
                False,
                f"RF Guard runtime snapshots unavailable: {exc}",
                radio_id=radio_id,
                js8_instance_id=js8_instance_id,
            )
        source_id = _int_or_none(radio_id)
        if source_id is None:
            return ExpectAutomationSourceStatus(False, "Expect source radio id is not available for RF Guard.", radio_id=radio_id, js8_instance_id=js8_instance_id)
        source_snapshot = None
        active_peers: list[object] = []
        for snapshot in snapshots:
            snap_id = _int_or_none(_snapshot_value(snapshot, "device_profile_id", 0))
            if snap_id == source_id:
                source_snapshot = snapshot
            elif bool(_snapshot_value(snapshot, "runtime_active", False)):
                active_peers.append(snapshot)
        if source_snapshot is None:
            return ExpectAutomationSourceStatus(False, "Expect source radio is not active in the station runtime.", radio_id=radio_id, js8_instance_id=js8_instance_id)
        source_freq = _int_or_none(_snapshot_value(source_snapshot, "current_frequency_hz", None))
        source_band = _norm_group(_snapshot_value(source_snapshot, "current_band", ""))
        if source_freq is None and not source_band:
            return ExpectAutomationSourceStatus(False, "Expect source radio frequency is unknown.", radio_id=radio_id, js8_instance_id=js8_instance_id)

        shared_resources: list[str] = []
        guard_mode = "warn"
        source_antenna = _profile_guard_group(profile, source_snapshot, "antenna_group", "antenna_group")
        source_frontend = _profile_guard_group(profile, source_snapshot, "frontend_group", "frontend_group")
        source_amplifier = _profile_guard_group(profile, source_snapshot, "amplifier_group", "amplifier_group")
        source_overlap = _profile_guard_group(profile, source_snapshot, "band_overlap_guard_group", "band_overlap_guard_group")
        source_advanced = _profile_guard_group(profile, source_snapshot, "advanced_frequency_guard_group", "advanced_frequency_guard_group")
        source_advanced_window = _profile_guard_window(profile)
        peer_labels: list[str] = []

        for peer in active_peers:
            peer_id = _int_or_none(_snapshot_value(peer, "device_profile_id", 0))
            peer_freq = _int_or_none(_snapshot_value(peer, "current_frequency_hz", None))
            peer_band = _norm_group(_snapshot_value(peer, "current_band", ""))
            if peer_id is None or (peer_freq is None and not peer_band):
                continue
            same_frequency = source_freq is not None and peer_freq is not None and abs(source_freq - peer_freq) <= 5
            same_band = bool(source_band and peer_band and source_band == peer_band)
            peer_name = str(_snapshot_value(peer, "name", "") or f"Radio {peer_id}").strip()
            peer_conflicts: list[str] = []
            peer_antenna = _norm_group(_snapshot_value(peer, "antenna_group", ""))
            peer_frontend = _norm_group(_snapshot_value(peer, "frontend_group", ""))
            peer_amplifier = _norm_group(_snapshot_value(peer, "amplifier_group", ""))
            if source_antenna and peer_antenna and source_antenna == peer_antenna and (same_frequency or same_band):
                peer_conflicts.append(f"antenna {source_antenna}")
                guard_mode = stricter_rf_guard_mode(guard_mode, "block" if bool(_snapshot_value(peer, "ptt_active", False)) else "confirm")
            if source_frontend and peer_frontend and source_frontend == peer_frontend and (same_frequency or same_band):
                peer_conflicts.append(f"front-end {source_frontend}")
                guard_mode = stricter_rf_guard_mode(guard_mode, "confirm")
            if source_amplifier and peer_amplifier and source_amplifier == peer_amplifier and (same_frequency or same_band):
                peer_conflicts.append(f"amplifier {source_amplifier}")
                guard_mode = stricter_rf_guard_mode(guard_mode, "confirm")
            peer_overlap = _norm_group(_snapshot_value(peer, "band_overlap_guard_group", ""))
            if source_overlap and peer_overlap and source_overlap == peer_overlap and (same_frequency or same_band):
                peer_conflicts.append(f"band-overlap guard {source_overlap}")
                guard_mode = stricter_rf_guard_mode(guard_mode, _profile_guard_mode(profile, "band_overlap_guard_mode", "warn"))
            peer_advanced = _norm_group(_snapshot_value(peer, "advanced_frequency_guard_group", ""))
            peer_window = _int_or_none(_snapshot_value(peer, "advanced_frequency_guard_window_hz", 0)) or 0
            if source_advanced and peer_advanced and source_advanced == peer_advanced and source_freq is not None and peer_freq is not None:
                window_hz = max(source_advanced_window, peer_window)
                delta_hz = abs(source_freq - peer_freq)
                if window_hz > 0 and delta_hz <= window_hz:
                    peer_conflicts.append(f"advanced guard {source_advanced} within {delta_hz} Hz")
                    guard_mode = stricter_rf_guard_mode(guard_mode, _profile_guard_mode(profile, "advanced_frequency_guard_mode", "warn"))
            if peer_conflicts:
                peer_labels.append(peer_name)
                shared_resources.extend(peer_conflicts)

        if not peer_labels:
            return ExpectAutomationSourceStatus(
                True,
                "RF Guard found no active source conflicts for Expect automation.",
                radio_id=radio_id,
                js8_instance_id=js8_instance_id,
                blocking=False,
            )
        resource_text = "; ".join(sorted(set(shared_resources))) or "shared RF resources"
        peer_text = ", ".join(peer_labels[:3]) + (f" (+{len(peer_labels) - 3} more)" if len(peer_labels) > 3 else "")
        blocked = normalize_rf_guard_mode(guard_mode, "warn") in {"confirm", "block"}
        return ExpectAutomationSourceStatus(
            not blocked,
            f"RF Guard held Expect auto-reply: source overlaps {peer_text} via {resource_text}.",
            radio_id=radio_id,
            js8_instance_id=js8_instance_id,
            blocking=blocked,
        )

    return _preflight


class ExpectAutomationCoordinator:
    """Source-aware JS8 client coordinator for guarded Expect automation.

    The coordinator is intentionally small and conservative. It does not evaluate
    Expect rules and it does not send messages directly; it only decides whether
    the runtime is allowed to supply a JS8 client for the source that received the
    request.
    """

    def __init__(
        self,
        settings: SettingsManager,
        *,
        profiles: Iterable[Mapping[str, Any]] = (),
        guard_preflight: Optional[GuardPreflightCallback] = None,
        require_guard_preflight: bool = True,
        client_factory: Optional[ClientFactory] = None,
    ) -> None:
        self.settings = settings
        self._profiles = [dict(profile or {}) for profile in list(profiles or [])]
        self._guard_preflight = guard_preflight
        self._require_guard_preflight = bool(require_guard_preflight)
        self._owns_clients = client_factory is not None
        self._client_factory = client_factory or (
            lambda endpoint: JS8ApiClientRegistry.get(endpoint, timeout_s=1.0, auto_reconnect=True)
        )
        self._clients: dict[tuple[str, int], JS8ApiClient] = {}
        self._last_status: Optional[ExpectAutomationSourceStatus] = None

    @property
    def last_status(self) -> Optional[ExpectAutomationSourceStatus]:
        return self._last_status

    def runtime_unattended_enabled(self) -> bool:
        return load_expect_automation_runtime_state(self.settings).active

    def client_factory_for_ingest(self) -> Callable[[str, str], Optional[JS8ApiClient]]:
        return self.client_for_source

    def client_for_source(self, radio_id: object, js8_instance_id: object = "") -> Optional[JS8ApiClient]:
        status = self.preflight_source(radio_id, js8_instance_id)
        self._last_status = status
        if not status.ok or status.endpoint is None:
            return None
        key = (status.endpoint.host, int(status.endpoint.port))
        client = self._clients.get(key)
        if client is None:
            client = self._client_factory(status.endpoint)
            self._clients[key] = client
        return client

    def preflight_source(self, radio_id: object, js8_instance_id: object = "") -> ExpectAutomationSourceStatus:
        radio_key, js8_key = _source_key(radio_id, js8_instance_id)
        if not self.runtime_unattended_enabled():
            return ExpectAutomationSourceStatus(
                False,
                "Runtime unattended Expect auto-reply is disabled or paused.",
                radio_id=radio_key,
                js8_instance_id=js8_key,
            )
        profile = self._profile_for_source(radio_key, js8_key)
        if profile is None:
            return ExpectAutomationSourceStatus(
                False,
                "No JS8-capable radio profile matches this Expect source.",
                radio_id=radio_key,
                js8_instance_id=js8_key,
            )
        endpoint = js8_endpoint_from_radio_profile(profile, fallback_settings=self.settings)
        status = self._run_guard_preflight(radio_key, js8_key, profile, endpoint)
        if not status.ok:
            return status
        if status.endpoint is None:
            return ExpectAutomationSourceStatus(
                True,
                status.reason or "Expect source is approved for guarded JS8 dispatch.",
                radio_id=status.radio_id or radio_key,
                js8_instance_id=status.js8_instance_id or js8_key,
                endpoint=endpoint,
                blocking=status.blocking,
            )
        if status.reason and status.reason != "RF Guard preflight is not required for this Expect automation runtime.":
            return status
        return ExpectAutomationSourceStatus(
            True,
            "Expect source is approved for guarded JS8 dispatch.",
            radio_id=radio_key,
            js8_instance_id=js8_key,
            endpoint=endpoint,
            blocking=False,
        )

    def close(self) -> None:
        if not self._owns_clients:
            self._clients.clear()
            return
        for client in list(self._clients.values()):
            try:
                client.stop()
            except Exception:
                pass
        self._clients.clear()

    def _profile_for_source(self, radio_id: str, js8_instance_id: str) -> Optional[Mapping[str, Any]]:
        if not self._profiles:
            return {}
        radio_id_lc = str(radio_id or "").strip().lower()
        js8_instance_id_lc = str(js8_instance_id or "").strip().lower()
        radio_matches: list[Mapping[str, Any]] = []
        for profile in self._profiles:
            profile_radio = str(profile.get("id", "") or profile.get("radio_profile_id", "") or "").strip()
            profile_js8 = str(profile.get("js8_instance_id", "") or profile.get("name", "") or profile_radio).strip()
            if radio_id and profile_radio.lower() != radio_id_lc:
                continue
            radio_matches.append(profile)
            if js8_instance_id and profile_js8.lower() != js8_instance_id_lc:
                continue
            return profile
        if radio_id and not js8_instance_id and radio_matches:
            return radio_matches[0]
        return None

    def _run_guard_preflight(
        self,
        radio_id: str,
        js8_instance_id: str,
        profile: Mapping[str, Any],
        endpoint: JS8ApiEndpoint,
    ) -> ExpectAutomationSourceStatus:
        callback = self._guard_preflight
        if callback is None:
            if self._require_guard_preflight:
                return ExpectAutomationSourceStatus(
                    False,
                    "RF Guard preflight is not configured for unattended Expect automation.",
                    radio_id=radio_id,
                    js8_instance_id=js8_instance_id,
                    endpoint=endpoint,
                )
            return ExpectAutomationSourceStatus(
                True,
                "RF Guard preflight is not required for this Expect automation runtime.",
                radio_id=radio_id,
                js8_instance_id=js8_instance_id,
                endpoint=endpoint,
                blocking=False,
            )
        try:
            raw = callback(radio_id, js8_instance_id, profile)
        except Exception as exc:
            return ExpectAutomationSourceStatus(
                False,
                f"RF Guard preflight failed: {exc}",
                radio_id=radio_id,
                js8_instance_id=js8_instance_id,
                endpoint=endpoint,
            )
        if isinstance(raw, ExpectAutomationSourceStatus):
            return raw
        if isinstance(raw, Mapping):
            ok = _truthy(raw.get("ok", raw.get("allowed", False)), False)
            return ExpectAutomationSourceStatus(
                ok,
                str(raw.get("reason", "") or ("RF Guard approved Expect automation source." if ok else "RF Guard blocked Expect automation source.")),
                radio_id=radio_id,
                js8_instance_id=js8_instance_id,
                endpoint=endpoint,
                blocking=not ok,
            )
        ok = bool(raw)
        return ExpectAutomationSourceStatus(
            ok,
            "RF Guard approved Expect automation source." if ok else "RF Guard blocked Expect automation source.",
            radio_id=radio_id,
            js8_instance_id=js8_instance_id,
            endpoint=endpoint,
            blocking=not ok,
        )
