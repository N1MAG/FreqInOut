from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class HelpContext:
    key: str
    anchor: str
    title: str
    summary: str = ""


DEFAULT_HELP_CONTEXT = HelpContext(
    key="help.overview",
    anchor="help",
    title="FreqInOut Help",
    summary="Browse the guide and jump to the section that matches the task you are working on.",
)


HELP_CONTEXTS: Dict[str, HelpContext] = {
    "help.overview": DEFAULT_HELP_CONTEXT,
    "tab.controlfreq": HelpContext(
        key="tab.controlfreq",
        anchor="controlfreq",
        title="ControlFreq Help",
        summary="Frequency control, schedule awareness, activity panels, and operator decision support.",
    ),
    "controlfreq.actions": HelpContext(
        key="controlfreq.actions",
        anchor="controlfreq-actions",
        title="ControlFreq Actions Help",
        summary="Buttons and controls used to hold, resume, refresh, and filter ControlFreq.",
    ),
    "tab.messages": HelpContext(
        key="tab.messages",
        anchor="messages",
        title="Messages Help",
        summary="Inbox review, message filters, and traffic actions across JS8, FL Suite, and VarAC.",
    ),
    "messages.compose": HelpContext(
        key="messages.compose",
        anchor="messages-compose",
        title="Messages Compose Help",
        summary="Compose and stage outbound traffic for FLMsg, FLAmp, and VarAC destinations.",
    ),
    "messages.bbs": HelpContext(
        key="messages.bbs",
        anchor="messages-bbs-archive",
        title="Messages BBS Help",
        summary="BBS copy, archive, and auto-archive behavior for staged and received files.",
    ),
    "messages.compose-setup": HelpContext(
        key="messages.compose-setup",
        anchor="messages-compose",
        title="Compose Setup Help",
        summary="Form family selection, priority, send targets, VarAC copy behavior, and FLAmp signing choices.",
    ),
    "tab.map": HelpContext(
        key="tab.map",
        anchor="map",
        title="Map Help",
        summary="Map layers, filters, traffic overlays, and station-visibility controls.",
    ),
    "map.controls": HelpContext(
        key="map.controls",
        anchor="map-top-controls",
        title="Map Controls Help",
        summary="Top-level map filters and control drawer behavior.",
    ),
    "map.paths": HelpContext(
        key="map.paths",
        anchor="map-paths",
        title="Map Paths Help",
        summary="Path overlays, Paths To, and Peer Sched Now visibility for decision support.",
    ),
    "tab.hf-daily": HelpContext(
        key="tab.hf-daily",
        anchor="hf-daily",
        title="HF Frequency Schedule Help",
        summary="Active HF schedule rows, resource-backed candidates, and SOP-aware conflict review.",
    ),
    "tab.hf-nets": HelpContext(
        key="tab.hf-nets",
        anchor="hf-nets",
        title="Net Schedules Help",
        summary="Net schedule editing, net resources, and Net or SOP policy decisions.",
    ),
    "tab.settings": HelpContext(
        key="tab.settings",
        anchor="settings",
        title="Settings Help",
        summary="Station identity, software integration, scheduler behavior, and launch readiness.",
    ),
    "settings.operator": HelpContext(
        key="settings.operator",
        anchor="settings-operator-info",
        title="Operator Information Help",
        summary="Callsign, name, state, and grid used across message naming, maps, and reporting.",
    ),
    "settings.freqinout": HelpContext(
        key="settings.freqinout",
        anchor="settings-freqinout-details",
        title="FreqInOut Settings Help",
        summary="Theme, text size, scheduler behavior, enforcement, and core application controls.",
    ),
    "settings.js8call": HelpContext(
        key="settings.js8call",
        anchor="settings-js8call-details",
        title="JS8Call Settings Help",
        summary="JS8 API, traffic files, and link-data inputs used by FreqInOut.",
    ),
    "settings.fast-light": HelpContext(
        key="settings.fast-light",
        anchor="settings-fast-light-details",
        title="Fast Light Settings Help",
        summary="FLRig, FLDigi, FLMsg, and FLAmp paths and companion integration fields.",
    ),
    "settings.hf-groups": HelpContext(
        key="settings.hf-groups",
        anchor="settings-hf-groups-details",
        title="HF Operating Groups Help",
        summary="HF operating group rows, expected FLDigi behavior, and conflict-aware schedule inputs.",
    ),
    "settings.local-comms": HelpContext(
        key="settings.local-comms",
        anchor="settings-local-comms-details",
        title="Local Comms Groups Help",
        summary="Local group profiles, resources, targets, and notes used for nearby communications planning.",
    ),
    "settings.varac": HelpContext(
        key="settings.varac",
        anchor="settings-varac-details",
        title="VarAC Settings Help",
        summary="VarAC install, message folders, BBS paths, and archive behavior.",
    ),
    "settings.message-auth": HelpContext(
        key="settings.message-auth",
        anchor="settings-message-auth",
        title="Message Auth Help",
        summary="Signature and checksum verification, trusted hashes, and GPG key actions.",
    ),
    "settings.launch-control": HelpContext(
        key="settings.launch-control",
        anchor="settings-launch-control",
        title="Launch Control Help",
        summary="Launch order, startup behavior, and software enablement for supported tools.",
    ),
    "settings.logging": HelpContext(
        key="settings.logging",
        anchor="settings-logging-diagnostics",
        title="Logging and Diagnostics Help",
        summary="Troubleshooting controls, log access, and diagnostics export.",
    ),
}


def get_help_context(key: str | None) -> HelpContext:
    if not key:
        return DEFAULT_HELP_CONTEXT
    key_txt = str(key).strip().lower()
    return HELP_CONTEXTS.get(key_txt, DEFAULT_HELP_CONTEXT)


def resolve_help_host(widget):
    current = widget
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if hasattr(current, "open_context_help") or hasattr(current, "open_help_anchor"):
            return current
        try:
            parent_widget = current.parentWidget()
        except Exception:
            parent_widget = None
        if parent_widget is not None:
            current = parent_widget
            continue
        try:
            current = current.parent()
        except Exception:
            current = None
    try:
        top_level = widget.window()
    except Exception:
        top_level = None
    if top_level is not None and (
        hasattr(top_level, "open_context_help") or hasattr(top_level, "open_help_anchor")
    ):
        return top_level
    return None
