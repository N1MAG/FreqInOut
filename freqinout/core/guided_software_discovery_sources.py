"""Read-only scanner adapters for guided software discovery.

These adapters project existing bounded detectors into the immutable GRS-1
coordinator contract. They never probe endpoints, launch processes, open a
radio, write settings, or persist ownership. Legacy projection exists only to
let current Settings workers adopt the coordinator without changing their UI
callbacks in the same slice.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Sequence, Tuple

from freqinout.core.config_autodiscovery import (
    AppCandidate,
    JS8CallFileProfile,
    app_search_paths_with_radio_apps_base,
    discover_js8call_file_profiles,
    find_app_candidates,
    normalize_platform,
)
from freqinout.core.guided_software_discovery import (
    DiscoveryPhase,
    DiscoveryRequest,
    DiscoveryTelemetryEvent,
    PhaseDiscoveryResult,
    PhaseScanner,
)
from freqinout.core.guided_software_proposals import DiscoveryEvidence, DiscoverySnapshot
from freqinout.core.perf_metrics import emit_span
from freqinout.core.software_path_detector import PathDetectionResult, SoftwarePathDetector


_SUPPORTED_APP_IDS = (
    "flrig",
    "fldigi",
    "flmsg",
    "flamp",
    "js8call",
    "js8spotter",
    "commstat",
    "sdrpp",
)
_CONFIGURED_APP_KEYS = (
    "path_flrig",
    "path_fldigi",
    "path_flmsg",
    "path_flamp",
    "path_js8call",
    "path_js8spotter",
    "path_commstat",
    "sdr_launch_target",
    "varac_path",
)
_MAX_APP_EVIDENCE = 32
_MAX_PROFILE_EVIDENCE = 16


def _truth(value: object) -> bool:
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on"}


def _detector_for(request: DiscoveryRequest) -> SoftwarePathDetector:
    detector = SoftwarePathDetector(dict(request.inputs))
    platform_name = str(request.inputs.get("platform") or "").strip()
    home = str(request.inputs.get("home") or "").strip()
    if platform_name:
        detector.system = normalize_platform(platform_name)
    if home:
        detector.home = Path(home)
    return detector


def _path_evidence(phase: DiscoveryPhase, key: str, result: PathDetectionResult) -> DiscoveryEvidence:
    return DiscoveryEvidence(
        evidence_key=f"{phase.value}-{key}",
        source=f"{phase.value}-scan",
        summary=result.reason or result.label,
        attributes={
            "record_type": "path_detection",
            "result_key": result.key,
            "label": result.label,
            "path": result.path,
            "confidence": result.confidence,
            "reason": result.reason,
            "exists": str(bool(result.exists)).lower(),
            "target_type": result.target_type,
        },
    )


def _path_phase_result(
    phase: DiscoveryPhase,
    results: Mapping[str, PathDetectionResult],
    cancelled: Callable[[], bool],
) -> PhaseDiscoveryResult:
    evidence = []
    for key, result in sorted(results.items()):
        if cancelled():
            break
        if isinstance(result, PathDetectionResult):
            evidence.append(_path_evidence(phase, str(key), result))
    return PhaseDiscoveryResult(phase=phase, evidence=tuple(evidence))


def scan_applications(request: DiscoveryRequest, cancelled: Callable[[], bool]) -> PhaseDiscoveryResult:
    if cancelled():
        return PhaseDiscoveryResult(DiscoveryPhase.APPLICATIONS)
    home = Path(str(request.inputs.get("home") or Path.home()))
    apps_base = str(request.inputs.get("radio_apps_base_folder") or "").strip()
    extras = tuple(
        Path(value)
        for key in _CONFIGURED_APP_KEYS
        for value in (str(request.inputs.get(key) or "").strip(),)
        if value
    )
    candidates = find_app_candidates(
        apps=_SUPPORTED_APP_IDS,
        platform=str(request.inputs.get("platform") or "") or None,
        home=home,
        extra_paths=extras,
        app_search_paths=app_search_paths_with_radio_apps_base(
            apps_base,
            platform=str(request.inputs.get("platform") or "") or None,
            home=home,
        ),
    )[:_MAX_APP_EVIDENCE]
    evidence = []
    for index, candidate in enumerate(candidates):
        if cancelled():
            break
        evidence.append(
            DiscoveryEvidence(
                evidence_key=f"application-{candidate.app_id}-{index}",
                source="application-scan",
                summary=f"{candidate.display_name}: {candidate.confidence}",
                attributes={
                    "record_type": "app_candidate",
                    "app_id": candidate.app_id,
                    "display_name": candidate.display_name,
                    "path": candidate.path,
                    "source": candidate.source,
                    "confidence": candidate.confidence,
                    "exists": str(bool(candidate.exists)).lower(),
                    "executable": str(bool(candidate.executable)).lower(),
                    "target_type": candidate.target_type,
                    "notes": " | ".join(candidate.notes),
                },
            )
        )
    return PhaseDiscoveryResult(DiscoveryPhase.APPLICATIONS, evidence=tuple(evidence))


def scan_js8_profiles(request: DiscoveryRequest, cancelled: Callable[[], bool]) -> PhaseDiscoveryResult:
    if cancelled():
        return PhaseDiscoveryResult(DiscoveryPhase.JS8_PROFILES)
    home_text = str(request.inputs.get("home") or "").strip()
    profiles = discover_js8call_file_profiles(
        platform=str(request.inputs.get("platform") or "") or None,
        home=Path(home_text) if home_text else None,
        cancelled=cancelled,
    )[:_MAX_PROFILE_EVIDENCE]
    if cancelled():
        return PhaseDiscoveryResult(DiscoveryPhase.JS8_PROFILES)
    results = _detector_for(request).detect_js8(
        file_profiles=profiles,
        include_application_paths=False,
    )
    evidence = list(_path_phase_result(DiscoveryPhase.JS8_PROFILES, results, cancelled).evidence)
    for index, profile in enumerate(profiles):
        if cancelled():
            break
        evidence.append(
            DiscoveryEvidence(
                evidence_key=f"js8-profile-{index}",
                source="js8-profile-scan",
                summary=f"{profile.operator_label}: {profile.confidence}",
                attributes={
                    "record_type": "js8_profile",
                    "name": profile.name,
                    "ini_path": profile.ini_path,
                    "save_dir": profile.save_dir,
                    "tcp_server_port": profile.tcp_server_port,
                    "directed_path": profile.directed_path,
                    "all_path": profile.all_path,
                    "inbox_path": profile.inbox_path,
                    "application_data_root": profile.application_data_root,
                    "rig_name": profile.rig_name,
                    "confidence": profile.confidence,
                    "reason": profile.reason,
                    "storage_mode": profile.storage_mode,
                },
            )
        )
    return PhaseDiscoveryResult(DiscoveryPhase.JS8_PROFILES, evidence=tuple(evidence))


def scan_fast_light(request: DiscoveryRequest, cancelled: Callable[[], bool]) -> PhaseDiscoveryResult:
    if cancelled():
        return PhaseDiscoveryResult(DiscoveryPhase.FAST_LIGHT)
    return _path_phase_result(
        DiscoveryPhase.FAST_LIGHT,
        _detector_for(request).detect_fast_light(include_application_paths=False),
        cancelled,
    )


def scan_varac(request: DiscoveryRequest, cancelled: Callable[[], bool]) -> PhaseDiscoveryResult:
    if cancelled():
        return PhaseDiscoveryResult(DiscoveryPhase.VARAC)
    return _path_phase_result(DiscoveryPhase.VARAC, _detector_for(request).detect_varac(), cancelled)


def scan_receiver(request: DiscoveryRequest, cancelled: Callable[[], bool]) -> PhaseDiscoveryResult:
    if cancelled():
        return PhaseDiscoveryResult(DiscoveryPhase.RECEIVER)
    attributes = {
        "record_type": "receiver_configuration",
        "application": str(request.inputs.get("sdr_receiver_application") or ""),
        "adapter": str(request.inputs.get("sdr_control_adapter") or ""),
        "host": str(request.inputs.get("sdr_host") or ""),
        "port": str(request.inputs.get("sdr_port") or ""),
        "target": str(request.inputs.get("sdr_receiver_target") or ""),
        "launch_target": str(request.inputs.get("sdr_launch_target") or ""),
    }
    evidence = DiscoveryEvidence(
        evidence_key="receiver-configured-evidence",
        source="receiver-settings",
        summary="Saved receiver configuration evidence; no endpoint was contacted.",
        attributes=attributes,
    )
    return PhaseDiscoveryResult(DiscoveryPhase.RECEIVER, evidence=(evidence,))


def default_scanners() -> Mapping[DiscoveryPhase, PhaseScanner]:
    return {
        DiscoveryPhase.APPLICATIONS: scan_applications,
        DiscoveryPhase.JS8_PROFILES: scan_js8_profiles,
        DiscoveryPhase.FAST_LIGHT: scan_fast_light,
        DiscoveryPhase.RECEIVER: scan_receiver,
        DiscoveryPhase.VARAC: scan_varac,
    }


def legacy_payload_from_snapshot(snapshot: DiscoverySnapshot) -> Dict[str, Any]:
    """Project immutable evidence into the existing Settings callback shape."""

    install_candidates = []
    fast_results: Dict[str, PathDetectionResult] = {}
    js8_results: Dict[str, PathDetectionResult] = {}
    varac_results: Dict[str, PathDetectionResult] = {}
    profiles = []
    for item in snapshot.evidence:
        values = dict(item.attributes)
        record_type = values.get("record_type", "")
        if record_type == "app_candidate":
            install_candidates.append(
                AppCandidate(
                    app_id=values.get("app_id", ""),
                    display_name=values.get("display_name", ""),
                    path=values.get("path", ""),
                    source=values.get("source", ""),
                    confidence=values.get("confidence", ""),
                    exists=_truth(values.get("exists")),
                    executable=_truth(values.get("executable")),
                    target_type=values.get("target_type", ""),
                    notes=tuple(part.strip() for part in values.get("notes", "").split("|") if part.strip()),
                )
            )
            continue
        if record_type == "path_detection":
            result = PathDetectionResult(
                key=values.get("result_key", ""),
                label=values.get("label", ""),
                path=values.get("path", ""),
                confidence=values.get("confidence", ""),
                reason=values.get("reason", ""),
                exists=_truth(values.get("exists")),
                target_type=values.get("target_type", ""),
            )
            if item.source == "fast_light-scan":
                fast_results[result.key] = result
            elif item.source == "js8_profiles-scan":
                js8_results[result.key] = result
            elif item.source == "varac-scan":
                varac_results[result.key] = result
            continue
        if record_type == "js8_profile":
            profiles.append(
                JS8CallFileProfile(
                    name=values.get("name", ""),
                    ini_path=values.get("ini_path", ""),
                    save_dir=values.get("save_dir", ""),
                    tcp_server_port=values.get("tcp_server_port", ""),
                    directed_path=values.get("directed_path", ""),
                    all_path=values.get("all_path", ""),
                    confidence=values.get("confidence", ""),
                    reason=values.get("reason", ""),
                    inbox_path=values.get("inbox_path", ""),
                    application_data_root=values.get("application_data_root", ""),
                    rig_name=values.get("rig_name", ""),
                    storage_mode=values.get("storage_mode", "unverified"),
                )
            )
    return {
        "cancelled": bool(snapshot.cancelled),
        "install_candidates": tuple(install_candidates),
        "fast_results": fast_results,
        "js8_results": js8_results,
        "varac_results": varac_results,
        "js8_file_profiles": tuple(profiles),
        "discovery_snapshot": snapshot,
    }


def make_perf_telemetry_sink(settings: Any = None) -> Callable[[DiscoveryTelemetryEvent], None]:
    def _sink(event: DiscoveryTelemetryEvent) -> None:
        emit_span(
            f"guided_software_discovery.{event.event}",
            event.elapsed_ms,
            settings=settings,
            meta={
                "request": event.request_key,
                "session": event.session_key,
                "generation": event.generation,
                "phase": event.phase,
                "cache": event.cache_state,
                "candidates": event.candidate_count,
                "outcome": event.outcome,
                "cancelled": event.cancelled,
            },
        )

    return _sink


__all__ = (
    "default_scanners",
    "legacy_payload_from_snapshot",
    "make_perf_telemetry_sink",
    "scan_applications",
    "scan_fast_light",
    "scan_js8_profiles",
    "scan_receiver",
    "scan_varac",
)
