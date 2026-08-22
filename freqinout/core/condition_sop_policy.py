from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Mapping, Sequence

from freqinout.core.condition_alerts import ACTION_MODES
from freqinout.core.observation_projection import Observation


AUTO_SOP_INVOCATION_SETTING_KEY = "condition_alert_auto_sop_invocation_enabled"


@dataclass(frozen=True)
class ConditionSopInvocationDecision:
    decision: str
    observation_id: str = ""
    operating_group: str = ""
    condition_level: int | None = None
    sop_profile_id: str = ""
    sop_profile_name: str = ""
    should_apply: bool = False
    requires_confirmation: bool = False
    blocked: bool = False
    reasons: tuple[str, ...] = ()


def evaluate_condition_sop_invocation(
    observation: Observation | Mapping[str, object],
    *,
    sop_layers: Sequence[Mapping[str, object]] = (),
    auto_apply_enabled: bool = False,
    rf_guard_state: Mapping[str, object] | None = None,
) -> ConditionSopInvocationDecision:
    """Decide how FIO should react to a condition-alert observation.

    This function is intentionally side-effect free. Ingest can call it without
    mutating schedules, and UI/scheduler code can later use the decision to ask
    the operator, apply a matching SOP layer, or explain why no action was taken.
    """
    obs = _observation_mapping(observation)
    observation_id = _text(obs.get("observation_id"))
    source_family = _lower(obs.get("source_family"))
    if source_family != "condition_alert":
        return _blocked("not a condition alert", observation_id=observation_id)

    level = _condition_level(obs)
    if level is None:
        return _blocked("condition level missing", observation_id=observation_id)

    group = _condition_group(obs)
    if not group:
        return _blocked("operating group missing", observation_id=observation_id, condition_level=level)

    action = _lower(_provenance(obs).get("action") or obs.get("action") or "")
    if action not in ACTION_MODES:
        action = "prompt-to-apply"

    layer = _matching_sop_layer(group, level, sop_layers)
    if not layer:
        return _blocked(
            f"no SOP layer matches {group} condition level {level}",
            observation_id=observation_id,
            operating_group=group,
            condition_level=level,
        )

    guard = {str(k): v for k, v in dict(rf_guard_state or {}).items()}
    guard_state = _lower(guard.get("state") or guard.get("decision") or guard.get("status") or "")
    if guard_state in {"blocked", "block", "error"}:
        reasons = _guard_messages(guard) or ("RF Guard blocked SOP invocation",)
        return ConditionSopInvocationDecision(
            decision="blocked",
            observation_id=observation_id,
            operating_group=group,
            condition_level=level,
            sop_profile_id=_text(layer.get("profile_id") or layer.get("sop_profile_id") or layer.get("id")),
            sop_profile_name=_text(layer.get("profile_name") or layer.get("sop_profile_name") or layer.get("name")),
            blocked=True,
            reasons=tuple(reasons),
        )

    if action == "suggest":
        return ConditionSopInvocationDecision(
            decision="suggest",
            observation_id=observation_id,
            operating_group=group,
            condition_level=level,
            sop_profile_id=_text(layer.get("profile_id") or layer.get("sop_profile_id") or layer.get("id")),
            sop_profile_name=_text(layer.get("profile_name") or layer.get("sop_profile_name") or layer.get("name")),
            reasons=(f"condition alert suggests {group} level {level}",),
        )

    if action == "auto-apply" and auto_apply_enabled:
        return ConditionSopInvocationDecision(
            decision="apply",
            observation_id=observation_id,
            operating_group=group,
            condition_level=level,
            sop_profile_id=_text(layer.get("profile_id") or layer.get("sop_profile_id") or layer.get("id")),
            sop_profile_name=_text(layer.get("profile_name") or layer.get("sop_profile_name") or layer.get("name")),
            should_apply=True,
            reasons=(f"auto-apply enabled for {group} level {level}",),
        )

    reason = (
        f"auto-apply disabled for {group} level {level}"
        if action == "auto-apply"
        else f"operator confirmation required for {group} level {level}"
    )
    return ConditionSopInvocationDecision(
        decision="prompt",
        observation_id=observation_id,
        operating_group=group,
        condition_level=level,
        sop_profile_id=_text(layer.get("profile_id") or layer.get("sop_profile_id") or layer.get("id")),
        sop_profile_name=_text(layer.get("profile_name") or layer.get("sop_profile_name") or layer.get("name")),
        requires_confirmation=True,
        reasons=(reason,),
    )


def evaluate_condition_sop_invocations(
    observations: Sequence[Observation | Mapping[str, object]],
    *,
    sop_profiles: Sequence[Mapping[str, object]] = (),
    auto_apply_enabled: bool = False,
    rf_guard_state_by_profile: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[ConditionSopInvocationDecision, ...]:
    layers = _layers_from_profiles(sop_profiles)
    guard_by_profile = {
        _text(key): value
        for key, value in dict(rf_guard_state_by_profile or {}).items()
        if isinstance(value, Mapping)
    }
    decisions: list[ConditionSopInvocationDecision] = []
    for observation in observations:
        decision = evaluate_condition_sop_invocation(
            observation,
            sop_layers=layers,
            auto_apply_enabled=auto_apply_enabled,
            rf_guard_state=None,
        )
        if decision.sop_profile_id:
            guard = guard_by_profile.get(decision.sop_profile_id)
            if guard:
                decision = evaluate_condition_sop_invocation(
                    observation,
                    sop_layers=layers,
                    auto_apply_enabled=auto_apply_enabled,
                    rf_guard_state=guard,
                )
        if decision.decision != "blocked" or decision.operating_group or decision.condition_level is not None:
            decisions.append(decision)
    return tuple(decisions)


def _layers_from_profiles(profiles: Sequence[Mapping[str, object]]) -> tuple[Mapping[str, object], ...]:
    layers: list[Mapping[str, object]] = []
    for profile in profiles:
        profile_id = _text(profile.get("id") or profile.get("profile_id") or profile.get("sop_profile_id"))
        profile_name = _text(profile.get("name") or profile.get("profile_name") or profile.get("sop_profile_name"))
        raw_layers = profile.get("schedule_layer") or profile.get("layers") or ()
        if not isinstance(raw_layers, Sequence) or isinstance(raw_layers, (str, bytes)):
            continue
        for raw_layer in raw_layers:
            if not isinstance(raw_layer, Mapping):
                continue
            layer = dict(raw_layer)
            layer.setdefault("profile_id", profile_id)
            layer.setdefault("profile_name", profile_name)
            layers.append(layer)
    return tuple(layers)


def _blocked(
    reason: str,
    *,
    observation_id: str = "",
    operating_group: str = "",
    condition_level: int | None = None,
) -> ConditionSopInvocationDecision:
    return ConditionSopInvocationDecision(
        decision="blocked",
        observation_id=observation_id,
        operating_group=operating_group,
        condition_level=condition_level,
        blocked=True,
        reasons=(reason,),
    )


def _observation_mapping(observation: Observation | Mapping[str, object]) -> Mapping[str, object]:
    if isinstance(observation, Observation):
        return observation.as_record()
    return observation


def _provenance(obs: Mapping[str, object]) -> Mapping[str, object]:
    value = obs.get("provenance")
    if isinstance(value, Mapping):
        return value
    raw_json = obs.get("provenance_json")
    if isinstance(raw_json, str) and raw_json.strip():
        try:
            parsed = json.loads(raw_json)
        except Exception:
            parsed = {}
        if isinstance(parsed, Mapping):
            return parsed
    return {}


def _condition_level(obs: Mapping[str, object]) -> int | None:
    candidates = [
        _provenance(obs).get("condition_level"),
        obs.get("condition_level"),
        obs.get("urgency"),
        obs.get("subject"),
        obs.get("summary"),
    ]
    for value in candidates:
        text = _text(value)
        if not text:
            continue
        match = re.search(r"(?:level\s*)?([1-5])\b", text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _condition_group(obs: Mapping[str, object]) -> str:
    provenance = _provenance(obs)
    for value in (
        provenance.get("operating_group"),
        obs.get("operating_group"),
        *_as_sequence(obs.get("groups")),
        obs.get("to_target"),
    ):
        group = _normalize_group(value)
        if group:
            return group
    return ""


def _matching_sop_layer(
    operating_group: str,
    condition_level: int,
    layers: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    group = _normalize_group(operating_group)
    for layer in layers:
        layer_group = _normalize_group(layer.get("group_name") or layer.get("operating_group") or layer.get("group"))
        if layer_group and layer_group != group:
            continue
        if condition_levels_include(layer.get("condition_levels") or layer.get("condition_level"), condition_level):
            return layer
    return {}


def condition_levels_include(value: object, condition_level: int | None) -> bool:
    """Return whether a stored SOP condition selector includes a condition level."""
    try:
        level = int(condition_level or 0)
    except Exception:
        level = 0
    if level <= 0:
        return False
    return _condition_levels_match(value, level)


def _condition_levels_match(value: object, condition_level: int) -> bool:
    text = _text(value).upper()
    if not text or text == "ALL":
        return True
    tokens = re.split(r"[,;/\s]+", text)
    wanted = str(condition_level)
    return any(token in {wanted, f"L{wanted}", f"LEVEL{wanted}"} for token in tokens if token)


def _guard_messages(guard: Mapping[str, object]) -> tuple[str, ...]:
    messages = guard.get("messages") or guard.get("reasons") or guard.get("errors") or ()
    if isinstance(messages, str):
        return (messages,)
    if isinstance(messages, Sequence):
        return tuple(_text(message) for message in messages if _text(message))
    return ()


def _as_sequence(value: object) -> tuple[object, ...]:
    if isinstance(value, (list, tuple, set)):
        return tuple(value)
    if value:
        return (value,)
    return ()


def _normalize_group(value: object) -> str:
    return _text(value).upper().lstrip("@").rstrip(">")


def _lower(value: object) -> str:
    return _text(value).lower()


def _text(value: object) -> str:
    return str(value or "").strip()
