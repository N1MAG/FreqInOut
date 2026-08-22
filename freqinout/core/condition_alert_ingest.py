from __future__ import annotations

from typing import Any, Mapping, Sequence

from freqinout.core.condition_alerts import (
    ConditionAlertMatch,
    ConditionAlertRule,
    condition_alert_message_from_intelligence,
    condition_alert_rules_from_settings,
    match_condition_alert_rules,
)
from freqinout.core.message_intelligence import MessageIntelligence
from freqinout.core.observation_projection import Observation, observation_from_condition_alert_match


def condition_alert_matches_for_message_intelligence(
    info: MessageIntelligence,
    rules_or_settings: Sequence[ConditionAlertRule | Mapping[str, Any]] | Mapping[str, Any] | str | None,
    *,
    source_ref: str,
    source_family: str = "",
    source_radio_id: int | None = None,
    source_app: str = "",
    received_utc: str = "",
    auth_state: str = "",
    trusted_state: str = "",
    operator_context: Mapping[str, Any] | None = None,
) -> tuple[ConditionAlertMatch, ...]:
    rules = _rules_from_any(rules_or_settings)
    if not rules:
        return ()
    message = condition_alert_message_from_intelligence(
        info,
        source_ref=source_ref,
        source_family=source_family,
        source_radio_id=source_radio_id,
        source_app=source_app,
        received_utc=received_utc,
        auth_state=auth_state,
        trusted_state=trusted_state,
        operator_context=operator_context,
    )
    return match_condition_alert_rules(rules, message)


def condition_alert_observations_for_message_intelligence(
    info: MessageIntelligence,
    rules_or_settings: Sequence[ConditionAlertRule | Mapping[str, Any]] | Mapping[str, Any] | str | None,
    *,
    source_ref: str,
    source_family: str = "",
    source_radio_id: int | None = None,
    source_app: str = "",
    received_utc: str = "",
    auth_state: str = "",
    trusted_state: str = "",
    operator_context: Mapping[str, Any] | None = None,
) -> tuple[Observation, ...]:
    matches = condition_alert_matches_for_message_intelligence(
        info,
        rules_or_settings,
        source_ref=source_ref,
        source_family=source_family,
        source_radio_id=source_radio_id,
        source_app=source_app,
        received_utc=received_utc,
        auth_state=auth_state,
        trusted_state=trusted_state,
        operator_context=operator_context,
    )
    return tuple(observation_from_condition_alert_match(match) for match in matches)


def _rules_from_any(
    rules_or_settings: Sequence[ConditionAlertRule | Mapping[str, Any]] | Mapping[str, Any] | str | None,
) -> tuple[ConditionAlertRule, ...]:
    if not rules_or_settings:
        return ()
    if isinstance(rules_or_settings, str):
        return condition_alert_rules_from_settings(rules_or_settings)
    if isinstance(rules_or_settings, Mapping):
        return condition_alert_rules_from_settings(rules_or_settings)
    rules: list[ConditionAlertRule] = []
    for rule in rules_or_settings:
        if isinstance(rule, ConditionAlertRule):
            rules.append(rule)
        elif isinstance(rule, Mapping):
            rules.extend(condition_alert_rules_from_settings(rule, include_builtin=False))
    return tuple(rules)
