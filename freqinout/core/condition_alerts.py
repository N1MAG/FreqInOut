from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Sequence

from freqinout.core.message_intelligence import MessageIntelligence


SOURCE_FAMILY_ALIASES = {
    "JS8": "JS8CALL",
    "JS8CALL": "JS8CALL",
    "VARAC": "VARAC",
    "COMMSTAT": "COMMSTAT",
    "JS8SPOTTER": "JS8SPOTTER",
    "SPOTTER": "JS8SPOTTER",
    "FLMSG": "FLMSG",
    "FLAMP": "FLAMP",
}

MATCH_MODES = {"contains", "whole-word", "regex", "template"}
ACTION_MODES = {"suggest", "prompt-to-apply", "auto-apply"}
ALLOWED_SENDER_MODES = {
    "any sender",
    "explicit list",
    "roster group",
    "roster role",
    "roster tier",
    "trusted operator",
}
AUTH_REQUIREMENTS = {"none", "signed", "signed-and-trusted"}
CONDITION_ALERT_RULES_SETTING_KEY = "condition_alert_rules"


@dataclass(frozen=True)
class ConditionAlertRule:
    id: str
    enabled: bool = False
    name: str = ""
    operating_group: str = ""
    source_families: tuple[str, ...] = ()
    target_groups: tuple[str, ...] = ()
    target_callsigns: tuple[str, ...] = ()
    allowed_sender_mode: str = "any sender"
    allowed_senders: tuple[str, ...] = ()
    required_auth_state: str = "none"
    match_mode: str = "regex"
    pattern: str = ""
    fixed_level: int | None = None
    level_capture_group: int | str = 1
    action: str = "prompt-to-apply"
    scope: str = "operating group"
    notes: str = ""


@dataclass(frozen=True)
class ConditionAlertMessage:
    source_family: str = ""
    source_ref: str = ""
    source_radio_id: int | None = None
    source_app: str = ""
    received_utc: str = ""
    from_call: str = ""
    to_target: str = ""
    groups: tuple[str, ...] = ()
    text: str = ""
    auth_state: str = ""
    trusted_state: str = ""
    operator_groups: tuple[str, ...] = ()
    operator_roles: tuple[str, ...] = ()
    operator_tiers: tuple[str, ...] = ()
    trusted_operator: bool = False


@dataclass(frozen=True)
class ConditionAlertMatch:
    rule_id: str
    rule_name: str
    source_family: str
    source_ref: str
    source_radio_id: int | None
    source_app: str
    received_utc: str
    from_call: str
    to_target: str
    groups: tuple[str, ...]
    operating_group: str
    condition_level: int
    action: str
    scope: str
    matched_text: str
    confidence: float = 0.0
    provenance: Mapping[str, Any] = field(default_factory=dict)


def default_condition_alert_rules() -> tuple[ConditionAlertRule, ...]:
    return (
        ConditionAlertRule(
            id="builtin-magnet-magcon",
            enabled=False,
            name="MagNet MAGCON",
            operating_group="MAGNET",
            source_families=("JS8CALL", "VARAC", "COMMSTAT", "JS8SPOTTER"),
            target_groups=("MAGNET", "MR01", "MR02", "MR03", "MR04", "MR05", "MR06", "MR07", "MR08", "MR09", "MR10", "MRHUB"),
            allowed_sender_mode="explicit list",
            allowed_senders=(),
            required_auth_state="none",
            match_mode="regex",
            pattern=r"\bMAGCON\+?([1-5])\b",
            level_capture_group=1,
            action="prompt-to-apply",
            scope="operating group",
            notes="Disabled starter template. Add approved senders before enabling.",
        ),
    )


def normalize_condition_alert_rule(value: Mapping[str, Any]) -> ConditionAlertRule:
    mode = str(value.get("match_mode") or "regex").strip().lower()
    if mode not in MATCH_MODES:
        mode = "regex"
    action = str(value.get("action") or "prompt-to-apply").strip().lower()
    if action not in ACTION_MODES:
        action = "prompt-to-apply"
    sender_mode = str(value.get("allowed_sender_mode") or "any sender").strip().lower()
    if sender_mode not in ALLOWED_SENDER_MODES:
        sender_mode = "any sender"
    auth = str(value.get("required_auth_state") or "none").strip().lower()
    if auth not in AUTH_REQUIREMENTS:
        auth = "none"
    fixed_level = _level_or_none(value.get("fixed_level") or value.get("condition_level"))
    capture: int | str = value.get("level_capture_group") or value.get("level_capture") or 1
    try:
        capture = int(capture)
    except (TypeError, ValueError):
        capture = str(capture or "level").strip() or "level"
    return ConditionAlertRule(
        id=str(value.get("id") or value.get("rule_id") or "").strip(),
        enabled=bool(value.get("enabled", False)),
        name=str(value.get("name") or value.get("label") or "").strip(),
        operating_group=_group(value.get("operating_group") or value.get("group")),
        source_families=_source_families(value.get("source_families")),
        target_groups=_groups(value.get("target_groups")),
        target_callsigns=_calls(value.get("target_callsigns")),
        allowed_sender_mode=sender_mode,
        allowed_senders=_calls_or_groups(value.get("allowed_senders")),
        required_auth_state=auth,
        match_mode=mode,
        pattern=str(value.get("pattern") or "").strip(),
        fixed_level=fixed_level,
        level_capture_group=capture,
        action=action,
        scope=str(value.get("scope") or "operating group").strip().lower() or "operating group",
        notes=str(value.get("notes") or "").strip(),
    )


def condition_alert_rule_to_settings(rule: ConditionAlertRule | Mapping[str, Any]) -> dict[str, Any]:
    normalized = rule if isinstance(rule, ConditionAlertRule) else normalize_condition_alert_rule(rule)
    return {
        "id": normalized.id,
        "enabled": normalized.enabled,
        "name": normalized.name,
        "operating_group": normalized.operating_group,
        "source_families": list(normalized.source_families),
        "target_groups": list(normalized.target_groups),
        "target_callsigns": list(normalized.target_callsigns),
        "allowed_sender_mode": normalized.allowed_sender_mode,
        "allowed_senders": list(normalized.allowed_senders),
        "required_auth_state": normalized.required_auth_state,
        "match_mode": normalized.match_mode,
        "pattern": normalized.pattern,
        "fixed_level": normalized.fixed_level,
        "level_capture_group": normalized.level_capture_group,
        "action": normalized.action,
        "scope": normalized.scope,
        "notes": normalized.notes,
    }


def condition_alert_rules_to_settings(rules: Sequence[ConditionAlertRule | Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [condition_alert_rule_to_settings(rule) for rule in rules]


def condition_alert_rules_from_settings(
    value: Any,
    *,
    include_builtin: bool = True,
) -> tuple[ConditionAlertRule, ...]:
    saved_rules = _condition_alert_rule_rows(value)
    normalized: list[ConditionAlertRule] = []
    for index, row in enumerate(saved_rules):
        if not isinstance(row, Mapping):
            continue
        rule = normalize_condition_alert_rule(row)
        if not rule.id:
            rule = replace(rule, id=f"custom-condition-alert-{index + 1}")
        normalized.append(rule)

    by_id: dict[str, ConditionAlertRule] = {}
    if include_builtin:
        by_id.update((rule.id, rule) for rule in default_condition_alert_rules())
    for rule in normalized:
        by_id[rule.id] = rule
    return tuple(by_id.values())


def condition_alert_rules_setting_payload(rules: Sequence[ConditionAlertRule | Mapping[str, Any]]) -> dict[str, Any]:
    return {CONDITION_ALERT_RULES_SETTING_KEY: condition_alert_rules_to_settings(rules)}


def condition_alert_message_from_intelligence(
    info: MessageIntelligence,
    *,
    source_ref: str = "",
    source_family: str = "",
    source_radio_id: int | None = None,
    source_app: str = "",
    received_utc: str = "",
    auth_state: str = "",
    trusted_state: str = "",
    operator_context: Mapping[str, Any] | None = None,
) -> ConditionAlertMessage:
    ctx = dict(operator_context or {})
    return ConditionAlertMessage(
        source_family=_source_family(source_family or info.source_type),
        source_ref=str(source_ref or "").strip(),
        source_radio_id=source_radio_id,
        source_app=str(source_app or "").strip(),
        received_utc=str(received_utc or "").strip(),
        from_call=_call(info.from_call),
        to_target=_target(info.to_call),
        groups=_groups(info.groups),
        text=" ".join(part for part in (info.form_name, info.subject, info.body, info.summary) if str(part or "").strip()),
        auth_state=str(auth_state or info.metadata.get("auth_state", "") or "").strip().lower(),
        trusted_state=str(trusted_state or info.metadata.get("trusted_state", "") or "").strip().lower(),
        operator_groups=_groups(ctx.get("groups")),
        operator_roles=_tokens(ctx.get("roles")),
        operator_tiers=_tokens(ctx.get("tiers")),
        trusted_operator=bool(ctx.get("trusted_operator", False)),
    )


def match_condition_alert_rules(
    rules: Sequence[ConditionAlertRule | Mapping[str, Any]],
    message: ConditionAlertMessage | MessageIntelligence | Mapping[str, Any],
) -> tuple[ConditionAlertMatch, ...]:
    msg = _message(message)
    matches: list[ConditionAlertMatch] = []
    for raw_rule in rules:
        rule = raw_rule if isinstance(raw_rule, ConditionAlertRule) else normalize_condition_alert_rule(raw_rule)
        if not rule.enabled or not rule.pattern:
            continue
        if rule.source_families and msg.source_family not in rule.source_families:
            continue
        if not _target_allowed(rule, msg):
            continue
        if not _sender_allowed(rule, msg):
            continue
        if not _auth_allowed(rule, msg):
            continue
        match = _match_text(rule, msg.text)
        if match is None:
            continue
        level = rule.fixed_level if rule.fixed_level is not None else _level_from_match(match, rule.level_capture_group)
        if level is None:
            continue
        operating_group = rule.operating_group or _first_group(rule.target_groups, msg.groups, msg.to_target)
        matches.append(
            ConditionAlertMatch(
                rule_id=rule.id,
                rule_name=rule.name or rule.id,
                source_family=msg.source_family,
                source_ref=msg.source_ref,
                source_radio_id=msg.source_radio_id,
                source_app=msg.source_app,
                received_utc=msg.received_utc,
                from_call=msg.from_call,
                to_target=msg.to_target,
                groups=msg.groups,
                operating_group=operating_group,
                condition_level=level,
                action=rule.action,
                scope=rule.scope,
                matched_text=match.group(0) if hasattr(match, "group") else str(match or ""),
                confidence=0.92 if rule.match_mode in {"regex", "template"} else 0.75,
                provenance={
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "match_mode": rule.match_mode,
                    "required_auth_state": rule.required_auth_state,
                    "allowed_sender_mode": rule.allowed_sender_mode,
                },
            )
        )
    return tuple(matches)


def _message(value: ConditionAlertMessage | MessageIntelligence | Mapping[str, Any]) -> ConditionAlertMessage:
    if isinstance(value, ConditionAlertMessage):
        return ConditionAlertMessage(
            source_family=_source_family(value.source_family),
            source_ref=value.source_ref,
            source_radio_id=value.source_radio_id,
            source_app=value.source_app,
            received_utc=value.received_utc,
            from_call=_call(value.from_call),
            to_target=_target(value.to_target),
            groups=_groups(value.groups),
            text=value.text,
            auth_state=value.auth_state.lower(),
            trusted_state=value.trusted_state.lower(),
            operator_groups=_groups(value.operator_groups),
            operator_roles=_tokens(value.operator_roles),
            operator_tiers=_tokens(value.operator_tiers),
            trusted_operator=value.trusted_operator,
        )
    if isinstance(value, MessageIntelligence):
        return condition_alert_message_from_intelligence(value)
    return ConditionAlertMessage(
        source_family=_source_family(value.get("source_family") or value.get("source_type")),
        source_ref=str(value.get("source_ref") or "").strip(),
        source_radio_id=_int_or_none(value.get("source_radio_id")),
        source_app=str(value.get("source_app") or "").strip(),
        received_utc=str(value.get("received_utc") or "").strip(),
        from_call=_call(value.get("from_call") or value.get("from")),
        to_target=_target(value.get("to_target") or value.get("to_call") or value.get("to")),
        groups=_groups(value.get("groups")),
        text=str(value.get("text") or value.get("body") or value.get("message") or "").strip(),
        auth_state=str(value.get("auth_state") or "").strip().lower(),
        trusted_state=str(value.get("trusted_state") or "").strip().lower(),
        operator_groups=_groups(value.get("operator_groups")),
        operator_roles=_tokens(value.get("operator_roles")),
        operator_tiers=_tokens(value.get("operator_tiers")),
        trusted_operator=bool(value.get("trusted_operator", False)),
    )


def _condition_alert_rule_rows(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        if CONDITION_ALERT_RULES_SETTING_KEY in value:
            return _condition_alert_rule_rows(value.get(CONDITION_ALERT_RULES_SETTING_KEY))
        if "rules" in value:
            return _condition_alert_rule_rows(value.get("rules"))
        return [value]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            import json

            decoded = json.loads(text)
        except Exception:
            return []
        return _condition_alert_rule_rows(decoded)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return list(value)
    return []


def _target_allowed(rule: ConditionAlertRule, msg: ConditionAlertMessage) -> bool:
    if not rule.target_groups and not rule.target_callsigns:
        return True
    msg_groups = set(msg.groups)
    target_group = _group(msg.to_target)
    if target_group:
        msg_groups.add(target_group)
    if rule.target_groups and msg_groups.intersection(rule.target_groups):
        return True
    if rule.target_callsigns and _call(msg.to_target) in set(rule.target_callsigns):
        return True
    return False


def _sender_allowed(rule: ConditionAlertRule, msg: ConditionAlertMessage) -> bool:
    mode = rule.allowed_sender_mode
    if mode == "any sender":
        return True
    if mode == "explicit list":
        allowed = set(rule.allowed_senders)
        return msg.from_call in allowed or bool(set(msg.operator_groups).intersection(allowed))
    if mode == "trusted operator":
        return msg.trusted_operator or msg.trusted_state in {"trusted", "signed-and-trusted", "valid-trusted"}
    if mode == "roster group":
        allowed_groups = set(rule.allowed_senders or rule.target_groups or ((rule.operating_group,) if rule.operating_group else ()))
        return bool(set(msg.operator_groups).intersection(allowed_groups))
    if mode == "roster role":
        return bool(set(msg.operator_roles).intersection(set(_tokens(rule.allowed_senders))))
    if mode == "roster tier":
        return bool(set(msg.operator_tiers).intersection(set(_tokens(rule.allowed_senders))))
    return False


def _auth_allowed(rule: ConditionAlertRule, msg: ConditionAlertMessage) -> bool:
    if rule.required_auth_state == "none":
        return True
    auth = msg.auth_state.lower()
    trusted = msg.trusted_state.lower()
    signed = auth in {"signed", "valid", "verified", "signed-and-trusted"} or trusted in {"trusted", "signed-and-trusted"}
    if rule.required_auth_state == "signed":
        return signed
    return signed and (trusted in {"trusted", "signed-and-trusted", "valid-trusted"} or msg.trusted_operator)


def _match_text(rule: ConditionAlertRule, text: str):
    if rule.match_mode == "contains":
        return re.search(re.escape(rule.pattern), text, flags=re.IGNORECASE)
    if rule.match_mode == "whole-word":
        return re.search(rf"\b{re.escape(rule.pattern)}\b", text, flags=re.IGNORECASE)
    return re.search(rule.pattern, text, flags=re.IGNORECASE)


def _level_from_match(match: re.Match[str], capture: int | str) -> int | None:
    try:
        raw = match.group(capture)
    except (IndexError, KeyError):
        raw = match.group(0)
    return _level_or_none(raw)


def _level_or_none(value: Any) -> int | None:
    text = str(value or "").strip()
    match = re.search(r"[1-5]", text)
    if not match:
        return None
    return int(match.group(0))


def _source_families(value: Any) -> tuple[str, ...]:
    return tuple(_source_family(v) for v in _tokens(value) if _source_family(v))


def _source_family(value: Any) -> str:
    text = str(value or "").strip().upper().replace(" ", "").replace("_", "")
    return SOURCE_FAMILY_ALIASES.get(text, text)


def _groups(value: Any) -> tuple[str, ...]:
    return tuple(dict.fromkeys(_group(v) for v in _tokens(value) if _group(v)))


def _calls(value: Any) -> tuple[str, ...]:
    return tuple(dict.fromkeys(_call(v) for v in _tokens(value) if _call(v)))


def _calls_or_groups(value: Any) -> tuple[str, ...]:
    return tuple(dict.fromkeys((_call(v) or _group(v)) for v in _tokens(value) if (_call(v) or _group(v))))


def _tokens(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = re.split(r"[,;\n]+", value)
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        parts = list(value)
    else:
        parts = [value]
    return tuple(str(part or "").strip().upper() for part in parts if str(part or "").strip())


def _call(value: Any) -> str:
    text = str(value or "").strip().upper().lstrip("@")
    if ">" in text:
        text = text.split(">")[-1]
    match = re.search(r"\b[A-Z]{1,2}\d[A-Z0-9]{1,5}(?:/[A-Z0-9]{1,4})?\b", text)
    return match.group(0) if match else ""


def _target(value: Any) -> str:
    text = str(value or "").strip().upper()
    return _call(text) or _group(text)


def _group(value: Any) -> str:
    text = str(value or "").strip().upper().lstrip("@")
    text = text.rstrip(">")
    return re.sub(r"[^A-Z0-9_-]+", "", text)


def _first_group(*values: Any) -> str:
    for value in values:
        groups = _groups(value)
        if groups:
            return groups[0]
    return ""


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
