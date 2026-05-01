from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


STATUS_DISPLAY_ITEMS: Sequence[Tuple[str, str]] = (
    ("FLRig", "FLRig"),
    ("FLDigi", "FLDigi"),
    ("FLMsg", "FLMsg"),
    ("FLAmp", "FLAmp"),
    ("JS8Call_API", "JS8"),
    ("VarAC", "VarAC"),
    ("JS8Spotter", "JS8Spotter"),
    ("CommStat", "CommStat"),
)


@dataclass(frozen=True)
class ReadinessStateSpec:
    key: str
    label: str
    sentence_fragment: str
    description: str
    card_level: str


READINESS_STATE_SPECS: Dict[str, ReadinessStateSpec] = {
    "ready": ReadinessStateSpec(
        key="ready",
        label="Ready",
        sentence_fragment="is ready",
        description="Configured for the workflows FreqInOut is currently tracking.",
        card_level="success",
    ),
    "needs_setup": ReadinessStateSpec(
        key="needs_setup",
        label="Needs Setup",
        sentence_fragment="needs setup",
        description="Required setup is missing before FreqInOut can rely on this area.",
        card_level="danger",
    ),
    "degraded": ReadinessStateSpec(
        key="degraded",
        label="Degraded",
        sentence_fragment="is degraded",
        description="Core setup is present, but one or more recommended items should be reviewed for a fuller workflow.",
        card_level="warning",
    ),
    "not_enabled": ReadinessStateSpec(
        key="not_enabled",
        label="Not Enabled",
        sentence_fragment="is not enabled",
        description="This area is not enabled for the current station workflow.",
        card_level="info",
    ),
    "external_manual": ReadinessStateSpec(
        key="external_manual",
        label="External / Manual",
        sentence_fragment="is managed externally",
        description="This area is intentionally managed outside FreqInOut or excluded from FreqInOut-managed launch behavior.",
        card_level="info",
    ),
}


@dataclass(frozen=True)
class ReadinessIssue:
    severity: str
    section_key: str
    scope: str
    message: str
    resolution_hint: str = ""
    radio_id: Optional[int] = None
    integration_key: str = ""
    deep_link_target: str = ""
    state_key: str = ""


@dataclass(frozen=True)
class RadioReadinessSummary:
    radio_id: Optional[int]
    name: str
    overall_state: str
    required_count: int
    recommended_count: int
    informational_count: int
    messages: Tuple[str, ...]
    state_counts: Tuple[Tuple[str, int], ...] = ()

    def state_count(self, state_key: str) -> int:
        target = str(state_key or "").strip().lower()
        for key, count in self.state_counts:
            if str(key or "").strip().lower() == target:
                return int(count or 0)
        return 0


@dataclass(frozen=True)
class ReadinessReport:
    overall_state: str
    issues: Tuple[ReadinessIssue, ...]
    radio_summaries: Tuple[RadioReadinessSummary, ...]
    required_count: int
    recommended_count: int
    informational_count: int
    digest: str
    state_counts: Tuple[Tuple[str, int], ...] = ()

    def first_actionable_issue(self) -> Optional[ReadinessIssue]:
        for severity in ("required", "recommended", "informational"):
            for issue in self.issues:
                if issue.severity == severity:
                    return issue
        return None

    def summary_for_radio(self, radio_id: Optional[int]) -> Optional[RadioReadinessSummary]:
        return None


def should_show_startup_review(
    report: ReadinessReport,
    *,
    dismissed_digest: str = "",
    suppressed_version: str = "",
    current_version: str = "",
) -> bool:
    actionable_count = int(report.required_count) + int(report.recommended_count)
    if actionable_count <= 0:
        return False
    if current_version and str(suppressed_version or "").strip() == str(current_version or "").strip():
        return False
    if report.digest and str(dismissed_digest or "").strip() == str(report.digest or "").strip():
        return False
    return True


def readiness_state_spec(state_key: str) -> ReadinessStateSpec:
    key = str(state_key or "").strip().lower() or "ready"
    return READINESS_STATE_SPECS.get(key, READINESS_STATE_SPECS["ready"])


def readiness_state_label(state_key: str) -> str:
    return readiness_state_spec(state_key).label


def readiness_state_card_level(state_key: str) -> str:
    return readiness_state_spec(state_key).card_level


def readiness_state_description(state_key: str) -> str:
    return readiness_state_spec(state_key).description


def format_readiness_issue(issue: ReadinessIssue, *, include_resolution: bool = True) -> str:
    detail = str(issue.message or "").strip()
    hint = str(issue.resolution_hint or "").strip()
    if include_resolution and hint:
        detail += f" ({hint})"
    return detail


def _issue_state_key(severity: str, fallback: str = "") -> str:
    key = str(fallback or "").strip().lower()
    if key:
        return key
    sev = str(severity or "").strip().lower()
    if sev == "required":
        return "needs_setup"
    if sev == "recommended":
        return "degraded"
    return "ready"


def _state_counts_from_issues(issues: Iterable[ReadinessIssue]) -> Tuple[Tuple[str, int], ...]:
    counts: Dict[str, int] = {}
    for issue in issues:
        state_key = _issue_state_key(issue.severity, issue.state_key)
        if not state_key or state_key == "ready":
            continue
        counts[state_key] = int(counts.get(state_key, 0) or 0) + 1
    ordered_keys = ("needs_setup", "degraded", "not_enabled", "external_manual")
    return tuple((key, int(counts[key])) for key in ordered_keys if int(counts.get(key, 0) or 0) > 0)


def _summary_overall_state(
    *,
    required_count: int,
    recommended_count: int,
    state_counts: Sequence[Tuple[str, int]],
) -> str:
    if required_count > 0:
        return "needs_setup"
    if recommended_count > 0:
        return "degraded"
    state_map = {str(key or "").strip().lower(): int(count or 0) for key, count in state_counts}
    if int(state_map.get("not_enabled", 0) or 0) > 0:
        return "not_enabled"
    if int(state_map.get("external_manual", 0) or 0) > 0:
        return "external_manual"
    return "ready"


def readiness_summary_badge_text(summary: RadioReadinessSummary) -> str:
    return readiness_state_label(summary.overall_state)


def readiness_summary_status_text(summary: RadioReadinessSummary, *, subject: str = "This area") -> str:
    spec = readiness_state_spec(summary.overall_state)
    prefix = f"{subject} {spec.sentence_fragment}."
    if summary.overall_state == "needs_setup":
        return f"{prefix} {int(summary.required_count)} required item(s) still need attention."
    if summary.overall_state == "degraded":
        return f"{prefix} {int(summary.recommended_count)} recommended item(s) should be reviewed."
    if summary.overall_state in {"not_enabled", "external_manual"}:
        return f"{prefix} {spec.description}"
    return prefix


def readiness_report_overall_text(report: ReadinessReport) -> str:
    spec = readiness_state_spec(report.overall_state)
    if report.overall_state == "needs_setup":
        degraded_count = sum(
            int(count or 0) for key, count in report.state_counts if str(key or "").strip().lower() == "degraded"
        )
        if degraded_count > 0:
            return (
                f"{spec.label}. {int(report.required_count)} required item(s) still need attention and "
                f"{int(degraded_count)} area(s) are degraded."
            )
        return f"{spec.label}. {int(report.required_count)} required item(s) still need attention."
    if report.overall_state == "degraded":
        return f"{spec.label}. {int(report.recommended_count)} recommended item(s) should be reviewed."
    return f"{spec.label}. {spec.description}"


def readiness_report_detail_text(report: ReadinessReport, *, title: str = "") -> str:
    lines: List[str] = []
    heading = str(title or "").strip()
    if heading:
        lines.append(heading)
    lines.append(f"Overall: {readiness_report_overall_text(report)}")
    for issue in report.issues:
        state_text = readiness_state_label(issue.state_key or issue.severity)
        lines.append(f"- [{state_text}] {format_readiness_issue(issue)}")
    return "\n".join(lines)


def _text(source: Mapping[str, Any], key: str, default: str = "") -> str:
    try:
        return str(source.get(key, default) or "").strip()
    except Exception:
        return str(default or "").strip()


def _text_any(source: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = _text(source, key)
        if value:
            return value
    return str(default or "").strip()


def _truthy(source: Mapping[str, Any], key: str, default: bool = False) -> bool:
    try:
        return bool(source.get(key, default))
    except Exception:
        return bool(default)


def _truthy_any(source: Mapping[str, Any], *keys: str, default: bool = False) -> bool:
    for key in keys:
        try:
            if key in source:
                return bool(source.get(key, default))
        except Exception:
            continue
    return bool(default)


def _boolish_or_none(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _software_enabled(settings: Mapping[str, Any], key: str) -> bool:
    normalized = str(key or "").strip().lower()
    explicit = _boolish_or_none(settings.get(f"use_{normalized}"))
    if explicit is not None:
        return explicit

    message_paths = settings.get("message_paths", {})
    if not isinstance(message_paths, Mapping):
        message_paths = {}

    if normalized == "flrig":
        return any(
            [
                _text(settings, "path_flrig"),
                _text(settings, "flrig_host"),
                _text(settings, "flrig_port"),
                _text(settings, "control_via").upper() == "FLRIG",
            ]
        )
    if normalized == "fldigi":
        return any(
            [
                _text(settings, "path_fldigi"),
                _text(settings, "fldigi_host"),
                _text(settings, "fldigi_port"),
                _text(settings, "fldigi_log_path"),
                _text(settings, "fldigi_checkin_dir"),
            ]
        )
    if normalized == "flmsg":
        return any([_text(settings, "path_flmsg"), _text(message_paths, "flmsg")])
    if normalized == "flamp":
        return any([_text(settings, "path_flamp"), _text(message_paths, "flamp")])
    if normalized == "js8call":
        return any(
            [
                _text(settings, "path_js8call"),
                _text(settings, "js8_host"),
                _text(settings, "js8_port"),
                _text(settings, "js8_directed_path"),
                _text(settings, "js8_forms_path"),
                _text(settings, "control_via").upper() == "JS8CALL",
            ]
        )
    if normalized == "js8spotter":
        return any([_text(settings, "path_js8spotter"), _text(settings, "js8_forms_path")])
    if normalized == "commstat":
        return bool(_text(settings, "path_commstat"))
    if normalized == "varac":
        return any(
            [
                _text(settings, "varac_path"),
                _text(settings, "varac_launch_cmd"),
                _text(message_paths, "varac"),
                _text(settings, "varac_outbox_dir"),
                _text(settings, "varac_bbs_dir"),
                _text(settings, "varac_bbs_archive_dir"),
            ]
        )
    return False


def _issue_deep_link(section_key: str) -> str:
    return str(section_key or "freqinout").strip().lower() or "freqinout"


def visible_status_programs(settings: Mapping[str, Any]) -> List[Tuple[str, str]]:
    visible = {
        "FLRig": _software_enabled(settings, "flrig"),
        "FLDigi": _software_enabled(settings, "fldigi"),
        "FLMsg": _software_enabled(settings, "flmsg"),
        "FLAmp": _software_enabled(settings, "flamp"),
        "JS8Call_API": _software_enabled(settings, "js8call"),
        "VarAC": _software_enabled(settings, "varac"),
        "JS8Spotter": _software_enabled(settings, "js8spotter"),
        "CommStat": _software_enabled(settings, "commstat"),
    }
    return [item for item in STATUS_DISPLAY_ITEMS if visible.get(item[0], False)]


def build_station_readiness_report(
    settings: Mapping[str, Any],
    *,
    operating_groups: Optional[Iterable[Any]] = None,
) -> ReadinessReport:
    issues: List[ReadinessIssue] = []
    groups = list(operating_groups or [])

    if not _text_any(settings, "operator_callsign", "callsign"):
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="freqinout",
                scope="global",
                message="Callsign missing",
                resolution_hint="Set the station callsign in FreqInOut Settings.",
                deep_link_target=_issue_deep_link("freqinout"),
                state_key="needs_setup",
            )
        )
    if not _text_any(settings, "operator_grid6", "operator_grid", "grid"):
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="freqinout",
                scope="global",
                message="Grid missing",
                resolution_hint="Set the station grid in FreqInOut Settings.",
                deep_link_target=_issue_deep_link("freqinout"),
                state_key="needs_setup",
            )
        )

    js8_enabled = any(
        [
            _software_enabled(settings, "js8call"),
            _software_enabled(settings, "js8spotter"),
            _software_enabled(settings, "commstat"),
        ]
    )
    fast_light_enabled = any(
        [
            _software_enabled(settings, "flrig"),
            _software_enabled(settings, "fldigi"),
            _software_enabled(settings, "flmsg"),
            _software_enabled(settings, "flamp"),
        ]
    )
    varac_enabled = _software_enabled(settings, "varac")

    prompt_checks = (
        (
            "Frequency",
            True,
            _text_any(settings, "freq_enforcement_mode", "frequency_enforcement_mode"),
            _text_any(settings, "freq_prompt_interval", "frequency_prompt_interval"),
        ),
        ("FLDigi", fast_light_enabled, _text(settings, "fldigi_enforcement_mode"), _text(settings, "fldigi_prompt_interval")),
        ("JS8Call", js8_enabled, _text(settings, "js8_enforcement_mode"), _text(settings, "js8_prompt_interval")),
    )
    for label, enabled, mode_value, prompt_value in prompt_checks:
        if enabled and mode_value == "Prompt" and prompt_value == "Select Interval":
            issues.append(
                ReadinessIssue(
                    severity="required",
                    section_key="freqinout",
                    scope="global",
                    message=f"{label} prompt interval missing",
                    resolution_hint="Choose a prompt interval before using Prompt enforcement for this workflow.",
                    deep_link_target=_issue_deep_link("freqinout"),
                    state_key="needs_setup",
                )
            )

    if not groups:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="operating_groups",
                scope="global",
                message="No HF operating groups configured",
                resolution_hint="Add at least one operating group before relying on schedule and QSY guidance.",
                deep_link_target=_issue_deep_link("operating_groups"),
                state_key="needs_setup",
            )
        )

    js8_directed = _text(settings, "js8_directed_path")
    js8_forms = _text(settings, "js8_forms_path")
    js8_host = _text(settings, "js8_host")
    js8_port = _text(settings, "js8_port")
    js8call_path = _text(settings, "path_js8call")
    js8spotter_path = _text(settings, "path_js8spotter")
    commstat_path = _text(settings, "path_commstat")
    js8_engaged = js8_enabled
    if _software_enabled(settings, "js8call") and not js8_host:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="js8call",
                scope="global",
                integration_key="js8call",
                message="JS8Call TCP host missing",
                resolution_hint="Set the JS8Call TCP host for this station.",
                deep_link_target=_issue_deep_link("js8call"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "js8call") and not js8_port:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="js8call",
                scope="global",
                integration_key="js8call",
                message="JS8Call TCP port missing",
                resolution_hint="Set the JS8Call TCP port for this station.",
                deep_link_target=_issue_deep_link("js8call"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "js8call") and not js8_directed:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="js8call",
                scope="global",
                integration_key="js8call",
                message="JS8Call DIRECTED.TXT path missing",
                resolution_hint="Set the DIRECTED.TXT path so JS8Call traffic can be tracked correctly.",
                deep_link_target=_issue_deep_link("js8call"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "js8spotter") and not js8_forms:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="js8call",
                scope="global",
                integration_key="js8spotter",
                message="JS8Spotter forms path missing",
                resolution_hint="Set the JS8 forms path so JS8Spotter can stage and read forms correctly.",
                deep_link_target=_issue_deep_link("js8call"),
                state_key="needs_setup",
            )
        )

    message_paths = settings.get("message_paths", {})
    if not isinstance(message_paths, Mapping):
        message_paths = {}
    default_fldigi_checkin_dir = _text(settings, "default_fldigi_checkin_dir")
    flrig_path = _text(settings, "path_flrig")
    flrig_port = _text(settings, "flrig_port")
    fldigi_path = _text(settings, "path_fldigi")
    fldigi_host = _text(settings, "fldigi_host")
    fldigi_port = _text(settings, "fldigi_port")
    flmsg_path = _text(settings, "path_flmsg")
    flamp_path = _text(settings, "path_flamp")
    fldigi_log_path = _text(settings, "fldigi_log_path")
    fldigi_checkin_dir = _text(settings, "fldigi_checkin_dir")
    fldigi_has_custom_checkin_dir = bool(
        fldigi_checkin_dir and (not default_fldigi_checkin_dir or fldigi_checkin_dir != default_fldigi_checkin_dir)
    )
    flmsg_msg_path = _text(message_paths, "flmsg")
    flamp_msg_path = _text(message_paths, "flamp")
    fast_light_engaged = fast_light_enabled
    if _software_enabled(settings, "flrig") and not flrig_port:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="flrig",
                message="FLRig XML-RPC port missing",
                resolution_hint="Set the FLRig XML-RPC port for frequency control.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "fldigi") and not fldigi_host:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="fldigi",
                message="FLDigi XML-RPC host missing",
                resolution_hint="Set the FLDigi XML-RPC host for runtime control.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "fldigi") and not fldigi_port:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="fldigi",
                message="FLDigi XML-RPC port missing",
                resolution_hint="Set the FLDigi XML-RPC port for runtime control.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "flmsg") and not flmsg_msg_path:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="flmsg",
                message="FLMsg ICS/Messages path missing",
                resolution_hint="Set the FLMsg message folder used for staged and received messages.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "flamp") and not flamp_msg_path:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="flamp",
                message="FLAmp FLAMP/rx path missing",
                resolution_hint="Set the FLAmp receive folder used for staged and received files.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "fldigi") and fldigi_has_custom_checkin_dir and not fldigi_path:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="fldigi",
                message="FLDigi executable path missing",
                resolution_hint="Set the FLDigi executable path before using a custom FLDigi check-in workspace.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "flmsg") and flmsg_msg_path and not flmsg_path:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="flmsg",
                message="FLMsg executable path missing",
                resolution_hint="Set the FLMsg executable path before staging or launching FLMsg traffic.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )
    if _software_enabled(settings, "flamp") and flamp_msg_path and not flamp_path:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="fast_light",
                scope="global",
                integration_key="flamp",
                message="FLAmp executable path missing",
                resolution_hint="Set the FLAmp executable path before staging or launching FLAmp traffic.",
                deep_link_target=_issue_deep_link("fast_light"),
                state_key="needs_setup",
            )
        )

    varac_install = _text(settings, "varac_path")
    varac_launch = _text(settings, "varac_launch_cmd")
    varac_incoming = _text(message_paths, "varac")
    varac_outbox = _text(settings, "varac_outbox_dir")
    varac_bbs_dir = _text(settings, "varac_bbs_dir")
    varac_bbs_archive = _text(settings, "varac_bbs_archive_dir")
    varac_bbs_limit_access = _truthy_any(settings, "varac_bbs_limit_access_enabled", "varac_bbs_limit_access")
    varac_bbs_allowed_callsigns = _text(settings, "varac_bbs_allowed_callsigns")
    varac_auto_archive = _truthy_any(settings, "varac_bbs_auto_archive_enabled", "varac_bbs_auto_archive")
    varac_guard_enabled = _truthy(settings, "varac_guard_enabled")
    varac_guard_mode = _text(settings, "varac_guard_mode", "Log only")
    varac_guard_quarantine = _text(settings, "varac_guard_quarantine_dir")
    varac_engaged = varac_enabled
    if varac_enabled and not varac_incoming:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="varac",
                message="VarAC incoming files path missing",
                resolution_hint="Set the VarAC incoming files folder so FreqInOut can read received VarAC traffic.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )
    if varac_enabled and not varac_outbox:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="varac",
                message="VarAC outbox directory missing",
                resolution_hint="Set the VarAC outbox directory so FreqInOut can stage outbound VarAC traffic.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )
    if varac_enabled and (varac_incoming or varac_outbox or varac_bbs_dir or varac_bbs_archive or varac_auto_archive) and not (
        varac_install or varac_launch
    ):
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="varac",
                message="Install folder or launch override missing",
                resolution_hint="Set the VarAC install folder or launch override before relying on VarAC workflows.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )
    if varac_enabled and bool(varac_bbs_dir) != bool(varac_bbs_archive):
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="varac_bbs",
                message="BBS directory/archive setup incomplete",
                resolution_hint="Set both the VarAC BBS directory and BBS archive directory together.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )
    if varac_enabled and varac_auto_archive and not (varac_bbs_dir and varac_bbs_archive):
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="varac_bbs",
                message="Auto-archive requires both BBS directories",
                resolution_hint="Configure both BBS directories before enabling VarAC BBS auto-archive.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )
    if varac_enabled and varac_bbs_limit_access and not varac_bbs_allowed_callsigns:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="varac_bbs",
                message="BBS access limit has no allowed callsigns",
                resolution_hint="Add at least one allowed callsign before enabling restricted VarAC BBS access.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )
    if varac_enabled and varac_guard_enabled and not varac_incoming:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="vguard",
                message="VGuard file protection has no VarAC incoming files path",
                resolution_hint="Set the VarAC incoming files folder before enabling VGuard file protection.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )
    if varac_enabled and varac_guard_enabled and varac_guard_mode == "Quarantine unauthorized files" and not varac_guard_quarantine:
        issues.append(
            ReadinessIssue(
                severity="required",
                section_key="varac",
                scope="global",
                integration_key="vguard",
                message="VGuard quarantine folder missing",
                resolution_hint="Set a quarantine folder before using VGuard quarantine mode.",
                deep_link_target=_issue_deep_link("varac"),
                state_key="needs_setup",
            )
        )

    required_count = sum(1 for issue in issues if issue.severity == "required")
    recommended_count = sum(1 for issue in issues if issue.severity == "recommended")
    informational_count = sum(1 for issue in issues if issue.severity == "informational")
    state_counts = _state_counts_from_issues(issues)
    overall_state = _summary_overall_state(
        required_count=required_count,
        recommended_count=recommended_count,
        state_counts=state_counts,
    )

    digest_source = "|".join(
        [
            overall_state,
            *(f"{issue.section_key}:{issue.severity}:{issue.message}" for issue in issues),
            f"groups={len(groups)}",
            f"js8_engaged={int(js8_engaged)}",
            f"fast_light_engaged={int(fast_light_engaged)}",
            f"varac_engaged={int(varac_engaged)}",
        ]
    )
    digest = sha1(digest_source.encode("utf-8")).hexdigest() if digest_source else ""

    return ReadinessReport(
        overall_state=overall_state,
        issues=tuple(issues),
        radio_summaries=(),
        required_count=required_count,
        recommended_count=recommended_count,
        informational_count=informational_count,
        digest=digest,
        state_counts=state_counts,
    )
    if _software_enabled(settings, "js8spotter") and not js8spotter_path:
        issues.append(
            ReadinessIssue(
                severity="recommended",
                section_key="js8call",
                scope="global",
                integration_key="js8spotter",
                message="JS8Spotter launch path missing",
                resolution_hint="Set the JS8Spotter launch path if this station should launch or track JS8Spotter.",
                deep_link_target=_issue_deep_link("js8call"),
                state_key="degraded",
            )
        )
    if _software_enabled(settings, "commstat") and not commstat_path:
        issues.append(
            ReadinessIssue(
                severity="recommended",
                section_key="js8call",
                scope="global",
                integration_key="commstat",
                message="CommStat launch path missing",
                resolution_hint="Set the CommStat launch path if this station should launch or track CommStat.",
                deep_link_target=_issue_deep_link("js8call"),
                state_key="degraded",
            )
        )
