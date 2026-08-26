from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence

from freqinout.core.group_utils import normalize_group_name
from freqinout.core.message_search_values import searchable_text_values
from freqinout.core.sitrep_metadata import parse_filter_subtype_label


MESSAGE_SOURCE_LABELS = {
    "js8": "JS8Call",
    "spotter": "JS8Spotter",
    "varac": "VarAC",
    "flmsg": "FLMSG",
    "flamp": "FLAmp",
    "bbs": "BBS",
    "sitrep": "SitRep",
    "commstat": "CommStat",
}

class MessageRowLike(Protocol):
    msg_type: str
    status: str
    origin: str
    payload: object
    rcv_ts: float
    rcv_display: str
    from_call: str
    to_call: str
    title: str
    search_text: str
    actionable: bool


@dataclass(frozen=True)
class InboxFilterCriteria:
    focus: str = "all"
    type_sel: str = "MSG Type..."
    status_sel: str = "Status..."
    from_sel: str = ""
    to_sel: str = ""
    age_filter_seconds: object = 0
    search_query: str = ""
    excluded_types: frozenset[str] = frozenset()
    now_ts: float | None = None

    @property
    def applies_hidden_types(self) -> bool:
        return str(self.type_sel or "").strip() in {"", "MSG Type..."}


def row_matches_type_filter(row: MessageRowLike, type_sel: str) -> bool:
    type_sel = str(type_sel or "").strip()
    if type_sel in ("", "MSG Type..."):
        return True
    origin = str(getattr(row, "origin", "") or "").strip().lower()
    msg_type = str(getattr(row, "msg_type", "") or "")
    if type_sel == "CommStat":
        return origin == "commstat"
    if type_sel == "Spotter":
        return origin == "spotter"
    if type_sel == "JS8Call":
        return origin == "js8"
    if type_sel == "FLMSG/FLAMP":
        return msg_type.strip().upper() in {"FLMSG", "FLAMP"}
    if type_sel == "SitRep":
        return msg_type == "SitRep"
    if type_sel.startswith("SitRep/"):
        subtype = parse_filter_subtype_label(type_sel)
        if msg_type != "SitRep":
            return False
        row_subtype = str(getattr(getattr(row, "payload", None), "subtype", "") or "").strip().upper()
        return row_subtype == subtype
    return msg_type == type_sel


def message_source_value(row: MessageRowLike) -> str:
    return str(getattr(row, "origin", "") or "").strip().lower()


def message_source_label(source: object) -> str:
    value = str(source or "").strip().lower()
    return MESSAGE_SOURCE_LABELS.get(value, value.upper())


def message_source_options(rows: list[MessageRowLike]) -> list[tuple[str, str]]:
    origins = set(MESSAGE_SOURCE_LABELS)
    origins.update({message_source_value(row) for row in rows})
    return [
        (origin, message_source_label(origin))
        for origin in sorted(origin for origin in origins if origin)
    ]


def active_inbox_scope_summary(
    *,
    focus: object = "all",
    focus_labels: Mapping[str, str] | None = None,
    groups: Sequence[object] | set[object] | frozenset[object] | None = None,
    sources: Sequence[object] | set[object] | frozenset[object] | None = None,
    age_label: object = "",
    search_query: object = "",
    type_sel: object = "MSG Type...",
    status_sel: object = "Status...",
    from_sel: object = "",
    to_sel: object = "",
) -> str:
    parts: list[str] = []
    focus_key = str(focus or "all").strip().lower()
    labels = dict(focus_labels or {})
    if focus_key != "all":
        parts.append(f"Focus {labels.get(focus_key, focus_key)}")
    if groups is not None:
        group_labels = sorted("Unassigned" if str(group) == "unassigned" else str(group) for group in groups)
        if group_labels:
            parts.append(
                "Groups "
                + ", ".join(group_labels[:3])
                + (f" +{len(group_labels) - 3}" if len(group_labels) > 3 else "")
            )
    if sources is not None:
        source_labels = sorted(message_source_label(source) for source in sources)
        if source_labels:
            parts.append(
                "Sources "
                + ", ".join(source_labels[:3])
                + (f" +{len(source_labels) - 3}" if len(source_labels) > 3 else "")
            )
    age_text = str(age_label or "").strip()
    if age_text:
        parts.append(age_text)
    query = str(search_query or "").strip()
    if query:
        parts.append(f'Search "{query}"')
    type_text = str(type_sel or "").strip()
    if type_text not in ("", "MSG Type..."):
        parts.append(f"Type {type_text}")
    status_text = str(status_sel or "").strip()
    if status_text not in ("", "Status..."):
        parts.append(f"Status {status_text}")
    from_text = str(from_sel or "").strip()
    if from_text:
        parts.append(f"From {from_text}")
    to_text = str(to_sel or "").strip()
    if to_text:
        parts.append(f"To {to_text}")
    return "; ".join(parts) if parts else "current view"


def looks_like_callsign_text(value: object) -> bool:
    text = str(value or "").strip().upper()
    return bool(re.fullmatch(r"[A-Z]{1,2}\d[A-Z0-9]{1,5}(?:/[A-Z0-9]{1,4})?", text))


def clean_js8_route_target(value: object) -> str:
    text = str(value or "").strip().upper()
    while text.startswith("@"):
        text = text[1:].strip()
    return text.rstrip(">").strip()


def is_js8_relay_marker(value: object) -> bool:
    return str(value or "").strip().endswith(">")


def normalize_message_group_filter_value(value: object) -> str:
    group = normalize_group_name(value)
    group = re.sub(r"\s+\*$", "", group).strip()
    group = re.sub(r"\s+", " ", group).strip()
    return group


def is_message_group_candidate(value: object, *, configured_groups: set[str] | frozenset[str] | None = None) -> bool:
    group = normalize_message_group_filter_value(value)
    if not group:
        return False
    if group == "unassigned":
        return True
    configured = configured_groups or set()
    if group.startswith("MR") and any(ch.isdigit() for ch in group[2:]):
        return True
    if is_js8_relay_marker(str(value or "")):
        return group in configured
    if looks_like_callsign_text(group):
        return group in configured
    return True


def message_group_value(row: MessageRowLike, *, configured_groups: set[str] | frozenset[str] | None = None) -> str:
    payload = getattr(row, "payload", None)
    configured = set(configured_groups or set())
    for attr in ("report_group", "group", "operating_group"):
        value = normalize_message_group_filter_value(getattr(payload, attr, "") if payload is not None else "")
        if is_message_group_candidate(value, configured_groups=configured):
            return value
    raw_target = str(getattr(payload, "to_call", "") if payload is not None else "").strip()
    target = normalize_message_group_filter_value(clean_js8_route_target(raw_target))
    if is_js8_relay_marker(raw_target) and target not in configured:
        return "unassigned"
    if (
        target
        and is_message_group_candidate(target, configured_groups=configured)
        and (raw_target.startswith("@") or target in configured or target.startswith("MR"))
    ):
        return target
    raw_route_target = str(getattr(row, "to_call", "") or "").strip()
    route_target = normalize_message_group_filter_value(clean_js8_route_target(raw_route_target))
    if is_js8_relay_marker(raw_route_target) and route_target not in configured:
        return "unassigned"
    if route_target.startswith("MR"):
        return route_target
    if (
        route_target
        and is_message_group_candidate(route_target, configured_groups=configured)
        and (
            route_target in configured
            or route_target.startswith("MR")
            or (len(route_target) >= 2 and not looks_like_callsign_text(route_target))
        )
    ):
        return route_target
    return "unassigned"


def row_matches_workspace_scope(
    row: MessageRowLike,
    *,
    selected_sources: set[str] | frozenset[str] | None = None,
    selected_groups: set[str] | frozenset[str] | None = None,
    configured_groups: set[str] | frozenset[str] | None = None,
) -> bool:
    if selected_sources is not None and message_source_value(row) not in selected_sources:
        return False
    if selected_groups is not None and message_group_value(row, configured_groups=configured_groups) not in selected_groups:
        return False
    return True


def message_group_candidate_set(values: object) -> set[str]:
    out: set[str] = set()
    try:
        iterator = iter(values or [])
    except TypeError:
        iterator = iter(())
    for value in iterator:
        group = normalize_message_group_filter_value(value)
        if is_message_group_candidate(group, configured_groups=set()):
            out.add(group)
    return out


def _family_members(value: object) -> set[str]:
    members = getattr(value, "members", None)
    if members is None:
        members = value
    try:
        return {normalize_message_group_filter_value(item) for item in members or () if normalize_message_group_filter_value(item)}
    except TypeError:
        return set()


def message_group_source_map(
    group_source_pairs: Sequence[tuple[object, object]],
    *,
    family_map: Mapping[str, object] | None = None,
) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    families = {
        normalize_message_group_filter_value(parent): _family_members(family)
        for parent, family in (family_map or {}).items()
        if normalize_message_group_filter_value(parent)
    }
    for raw_group, raw_source in group_source_pairs:
        group = normalize_message_group_filter_value(raw_group)
        if not is_message_group_candidate(raw_group, configured_groups=set()):
            continue
        source = str(raw_source or "").strip().lower()
        out.setdefault(group, set()).add(source)
        for parent, members in families.items():
            if group in members:
                out.setdefault(parent, set()).add(source)
    return out


def primary_message_group_values(
    group_sources: Mapping[str, set[str] | frozenset[str]],
    *,
    fio_configured_groups: set[str] | frozenset[str] | None = None,
    commstat_active_groups: set[str] | frozenset[str] | None = None,
    commstat_configured_groups: set[str] | frozenset[str] | None = None,
) -> set[str]:
    return {
        value
        for _section, options in message_group_option_sections(
            group_sources,
            fio_configured_groups=fio_configured_groups,
            commstat_active_groups=commstat_active_groups,
            commstat_configured_groups=commstat_configured_groups,
            show_all_groups=False,
        )
        for value, _label in options
    }


def message_group_rebuild_selection(
    group_sources: Mapping[str, set[str] | frozenset[str]],
    *,
    current_selected: set[str] | frozenset[str] | Sequence[object] | None,
    current_all_selected: bool,
    fio_configured_groups: set[str] | frozenset[str] | None = None,
    commstat_active_groups: set[str] | frozenset[str] | None = None,
    commstat_configured_groups: set[str] | frozenset[str] | None = None,
    show_all_groups: bool = False,
    prefer_primary: bool = False,
) -> tuple[list[str] | None, bool]:
    """Choose selected group values when the dynamic group menu is rebuilt.

    Returning ``None, True`` means the checklist should select every visible
    focused option. Returning a list with ``False`` preserves an explicit user
    selection, including an intentionally empty selection.
    """
    sections = message_group_option_sections(
        group_sources,
        fio_configured_groups=fio_configured_groups,
        commstat_active_groups=commstat_active_groups,
        commstat_configured_groups=commstat_configured_groups,
        show_all_groups=show_all_groups,
    )
    option_values = {value for _section, options in sections for value, _label in options}
    primary_values = primary_message_group_values(
        group_sources,
        fio_configured_groups=fio_configured_groups,
        commstat_active_groups=commstat_active_groups,
        commstat_configured_groups=commstat_configured_groups,
    ) & option_values
    if show_all_groups and (prefer_primary or current_all_selected):
        return sorted(primary_values), False
    if current_all_selected:
        return None, True
    if current_selected is None:
        if show_all_groups:
            return sorted(primary_values), False
        return None, True
    selected = {
        normalize_message_group_filter_value(value)
        for value in current_selected
        if normalize_message_group_filter_value(value)
    }
    if not selected:
        return [], False
    preserved = selected & option_values
    if preserved:
        return sorted(preserved), False
    if selected:
        return [], False
    if show_all_groups:
        return sorted(primary_values), False
    return None, True


def message_group_option_sections(
    group_sources: Mapping[str, set[str] | frozenset[str]],
    *,
    fio_configured_groups: set[str] | frozenset[str] | None = None,
    commstat_active_groups: set[str] | frozenset[str] | None = None,
    commstat_configured_groups: set[str] | frozenset[str] | None = None,
    show_all_groups: bool = False,
) -> list[tuple[str, list[tuple[str, str]]]]:
    sources = {group: set(values or set()) for group, values in (group_sources or {}).items() if group}
    if not sources:
        sources = {"unassigned": {"system"}}
    fio_groups = set(fio_configured_groups or set())
    active_groups = set(commstat_active_groups or set())
    configured_commstat = set(commstat_configured_groups or set())
    ordered = sorted(sources)
    configured_order = [group for group in ordered if group in fio_groups]
    commstat_active_order = [
        group
        for group in ordered
        if group not in fio_groups and group in active_groups
    ]
    if not configured_order and not commstat_active_order:
        show_all_groups = True

    def option(group: str) -> tuple[str, str] | None:
        if not group:
            return None
        label = "Unassigned" if group == "unassigned" else group
        return (group, label)

    def options(groups: list[str]) -> list[tuple[str, str]]:
        return [opt for group in groups if (opt := option(group)) is not None]

    if not show_all_groups:
        display_order = configured_order + commstat_active_order
        if not display_order:
            display_order = ["unassigned"] if "unassigned" in sources else []
        sections: list[tuple[str, list[tuple[str, str]]]] = []
        fio_options = options(configured_order)
        commstat_options = options(commstat_active_order)
        unassigned_options = options([group for group in display_order if group == "unassigned"])
        if fio_options:
            sections.append(("Configured Groups", fio_options))
        if commstat_options:
            sections.append(("CommStat Active Groups", commstat_options))
        if unassigned_options:
            sections.append(("Other", unassigned_options))
        return sections or [("Groups", options(display_order))]

    commstat_order = [
        group
        for group in ordered
        if group not in fio_groups
        and group not in active_groups
        and (group in configured_commstat or "commstat" in sources.get(group, set()))
    ]
    other_order = [
        group
        for group in ordered
        if group not in fio_groups
        and group not in active_groups
        and group not in commstat_order
    ]
    sections = []
    fio_options = options(configured_order)
    commstat_active_options = options(commstat_active_order)
    commstat_other_options = options(commstat_order)
    other_options = options(other_order)
    if fio_options:
        sections.append(("Configured Groups", fio_options))
    if commstat_active_options:
        sections.append(("CommStat Active Groups", commstat_active_options))
    if commstat_other_options:
        sections.append(("Other CommStat Groups", commstat_other_options))
    if other_options:
        sections.append(("Other Discovered Groups", other_options))
    return sections or [("Groups", options(["unassigned"] if "unassigned" in sources else []))]


def row_matches_inbox_focus(row: MessageRowLike, focus: str) -> bool:
    focus = str(focus or "all").strip().lower()
    if focus == "all":
        return True
    if focus == "new":
        return str(getattr(row, "status", "") or "").strip().upper() not in {"", "READ", "INFO"}
    if focus == "forms":
        return row_matches_type_filter(row, "FLMSG/FLAMP")
    if focus == "spotter":
        return row_matches_type_filter(row, "Spotter") or str(getattr(row, "msg_type", "") or "").startswith("F!")
    if focus == "commstat":
        return row_matches_type_filter(row, "CommStat")
    if focus == "js8call":
        return row_matches_type_filter(row, "JS8Call")
    if focus == "varac":
        return str(getattr(row, "origin", "") or "").strip().lower() == "varac" or (
            str(getattr(row, "msg_type", "") or "").strip() == "VarAC"
        )
    if focus == "bbs":
        return str(getattr(row, "origin", "") or "").strip().lower() in {"bbs", "bbs_archive"}
    return True


def row_matches_age_filter(
    row: MessageRowLike,
    age_filter_seconds: object,
    *,
    now_ts: float | None = None,
) -> bool:
    try:
        seconds = int(age_filter_seconds or 0)
    except Exception:
        seconds = 0
    if seconds == 0:
        return True
    try:
        ts = float(getattr(row, "rcv_ts", 0.0) or 0.0)
    except Exception:
        return False
    if ts <= 0:
        return False
    if now_ts is None:
        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
    age = max(0.0, float(now_ts) - ts)
    if seconds > 0:
        return age <= seconds
    return age >= abs(seconds)


def row_matches_status_filter(row: MessageRowLike, status_sel: str) -> bool:
    status_sel = str(status_sel or "").strip()
    if status_sel in {"", "Status..."}:
        return True
    if status_sel == "Action Needed":
        return bool(getattr(getattr(row, "payload", None), "flag_state", 0) == 1 or getattr(row, "actionable", False))
    return str(getattr(row, "status", "") or "") == status_sel


def row_search_text(row: MessageRowLike) -> str:
    values = [
        str(getattr(row, "search_text", "") or ""),
        str(getattr(row, "msg_type", "") or ""),
        str(getattr(row, "status", "") or ""),
        str(getattr(row, "from_call", "") or ""),
        str(getattr(row, "to_call", "") or ""),
        str(getattr(row, "rcv_display", "") or ""),
        str(getattr(row, "title", "") or ""),
    ]
    values.extend(searchable_text_values(getattr(row, "payload", None)))
    haystack = " ".join(value for value in values if value)
    aliases = haystack.replace("@", " ").replace(">", " ")
    return f"{haystack} {aliases}".lower()


def row_matches_search_query(row: MessageRowLike, query: str) -> bool:
    query = str(query or "").strip().lower()
    if not query:
        return True
    haystack = row_search_text(row)
    if query in haystack:
        return True
    normalized_query = query.replace("@", " ").replace(">", " ")
    if normalized_query.strip() and normalized_query in haystack:
        return True
    tokens = [token for token in re.split(r"[\s,;/|]+", normalized_query) if token]
    if not tokens:
        return True
    return all(token in haystack for token in tokens)


def row_matches_excluded_types(row: MessageRowLike, excluded_types: set[str] | frozenset[str]) -> bool:
    for label in excluded_types or ():
        if row_matches_type_filter(row, str(label or "")):
            return True
    return False


def row_matches_inbox_criteria(row: MessageRowLike, criteria: InboxFilterCriteria) -> bool:
    if not row_matches_inbox_focus(row, criteria.focus):
        return False
    if not row_matches_age_filter(row, criteria.age_filter_seconds, now_ts=criteria.now_ts):
        return False
    if not row_matches_type_filter(row, criteria.type_sel):
        return False
    if criteria.applies_hidden_types and criteria.excluded_types and row_matches_excluded_types(row, criteria.excluded_types):
        return False
    if not row_matches_status_filter(row, criteria.status_sel):
        return False
    if criteria.from_sel and str(getattr(row, "from_call", "") or "") != criteria.from_sel:
        return False
    if criteria.to_sel and str(getattr(row, "to_call", "") or "") != criteria.to_sel:
        return False
    if not row_matches_search_query(row, criteria.search_query):
        return False
    return True
