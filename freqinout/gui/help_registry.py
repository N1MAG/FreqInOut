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
    "help.glossary": HelpContext(
        key="help.glossary",
        anchor="operating-plan-glossary",
        title="Operating Plan Glossary",
        summary="Plain-language definitions for Frequency Plan, Assigned Plan, Operating Plan, Schedule Source, Radio Profile, and the shared plan context cue.",
    ),
    "help.plan-context": HelpContext(
        key="help.plan-context",
        anchor="plan-context-cue",
        title="Plan Context Cue Help",
        summary="Read-only radio and Frequency Plan context shown on planning and operating tabs.",
    ),
    "tab.controlfreq": HelpContext(
        key="tab.controlfreq",
        anchor="controlfreq",
        title="ControlFreq Help",
        summary="Frequency control, plan context cue, schedule awareness, activity panels, unread message and BBS file awareness, and operator decision support.",
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
        summary="Inbox review, plan context cue, visible-tab refresh, BBS status, message filters, and traffic actions.",
    ),
    "messages.compose": HelpContext(
        key="messages.compose",
        anchor="messages-compose",
        title="Messages Compose Help",
        summary="Compose and stage outbound traffic for FLMsg, FLAmp, and VarAC destinations; review JS8Spotter MCForms drafts before guarded JS8Call send is enabled.",
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
        summary="Map plan context cue, layers, report icons, filters, traffic overlays, and station-visibility controls.",
    ),
    "tab.station-health": HelpContext(
        key="tab.station-health",
        anchor="station-health",
        title="Station Health Help",
        summary="External station software responsiveness, backoff status, and what FIO is waiting on.",
    ),
    "tab.ncs-fldigi": HelpContext(
        key="tab.ncs-fldigi",
        anchor="ncs-fldigi-ssb",
        title="FLDigi / SSB Net Control Help",
        summary="Directed net workflow, roster actions, ACK/TFC handling, macro files, and net lifecycle.",
    ),
    "map.controls": HelpContext(
        key="map.controls",
        anchor="map-top-controls",
        title="Map Controls Help",
        summary="Top-level map filters, station/link/report layers, and control drawer behavior.",
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
        summary="Active HF schedule rows, plan context cue, resource-backed candidates, and SOP-aware conflict review.",
    ),
    "tab.hf-nets": HelpContext(
        key="tab.hf-nets",
        anchor="hf-nets",
        title="Net Schedules Help",
        summary="Net schedule editing, plan context cue, net resources, and Net or SOP policy decisions.",
    ),
    "tab.hf-peers": HelpContext(
        key="tab.hf-peers",
        anchor="hf-peers",
        title="HF Peers Help",
        summary="Peer schedule imports, manual entries, cleanup actions, filters, and overlap review.",
    ),
    "tab.sop-builder": HelpContext(
        key="tab.sop-builder",
        anchor="sop-builder",
        title="SOP Builder Help",
        summary="SOP profile editing, plan context cue, conflict-aware activation, versions, and export workflow.",
    ),
    "tab.settings": HelpContext(
        key="tab.settings",
        anchor="settings",
        title="Settings Help",
        summary="Station identity, software choices, paths, scheduler help, launch readiness, and troubleshooting.",
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
        summary="Theme, text size, software used, scheduler behavior examples, timers, operating status, and setup readiness.",
    ),
    "settings.software_used": HelpContext(
        key="settings.software_used",
        anchor="settings-freqinout-details",
        title="Software Used Help",
        summary="Choose only the station programs you actually use so FIO shows useful readiness guidance.",
    ),
    "settings.js8call": HelpContext(
        key="settings.js8call",
        anchor="settings-js8call-details",
        title="JS8Call Settings Help",
        summary="JS8Call connection, JS8 traffic files, CommStat, JS8Spotter, JS8Spotter form mapping, and Expect preparation.",
    ),
    "settings.fast-light": HelpContext(
        key="settings.fast-light",
        anchor="settings-fast-light-details",
        title="Fast Light Settings Help",
        summary="FLRig, FLDigi, FLMsg, and FLAmp paths, connection fields, message folders, and auto-fill help.",
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
        summary="VarAC paths, incoming/outbox folders, BBS management, Managed BBS Services, relay, and VGuard.",
    ),
    "settings.message-auth": HelpContext(
        key="settings.message-auth",
        anchor="settings-message-auth",
        title="Message Auth Help",
        summary="Plain-language signature and checksum verification, JS8 MsgAuth keys scoped by group/callsign, trusted hashes, GPG keys, and signing identity.",
    ),
    "settings.launch-control": HelpContext(
        key="settings.launch-control",
        anchor="settings-launch-control",
        title="Launch Control Help",
        summary="Which station tools FIO may start, startup behavior, launch order, and dependency pacing.",
    ),
    "settings.custom-tools": HelpContext(
        key="settings.custom-tools",
        anchor="settings-custom-tools-details",
        title="Custom Tools Help",
        summary="Station-specific helper tools, launch commands, ordering, and Launch Control crossover.",
    ),
    "settings.sop-export": HelpContext(
        key="settings.sop-export",
        anchor="settings-sop-export-details",
        title="SOP Export Help",
        summary="Preamble and postamble text used to add local context to SOP PDF exports.",
    ),
    "settings.logging": HelpContext(
        key="settings.logging",
        anchor="settings-logging-diagnostics",
        title="Logging and Diagnostics Help",
        summary="When to turn on extra logging, how to open logs, and how to export diagnostics for support.",
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
