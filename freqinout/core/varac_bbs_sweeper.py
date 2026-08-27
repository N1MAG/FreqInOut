from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


VALID_SOURCE_FAMILIES = frozenset({"varac_bbs", "flmsg", "flamp"})


def _clean_token(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_source_family(value: object) -> str:
    text = _clean_token(value).lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "bbs": "varac_bbs",
        "varac": "varac_bbs",
        "varac_bbs_inbox": "varac_bbs",
        "fl_msg": "flmsg",
        "fl_amp": "flamp",
    }
    return aliases.get(text, text)


def _normalize_callsign(value: object) -> str:
    text = _clean_token(value).upper()
    text = text.lstrip("@")
    return re.sub(r"[^A-Z0-9/]", "", text)


def _normalize_location_id(value: object) -> str:
    return re.sub(r"[^A-Za-z0-9_.:-]+", "-", _clean_token(value)).strip("-")


def _as_tuple(value: object, *, normalizer=_clean_token) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_items = re.split(r"[,;\n]+", value)
    elif isinstance(value, Iterable):
        raw_items = list(value)
    else:
        raw_items = [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        cleaned = normalizer(item)
        if cleaned and cleaned not in seen:
            out.append(cleaned)
            seen.add(cleaned)
    return tuple(out)


@dataclass(frozen=True)
class BbsSweeperRule:
    id: str
    name: str
    enabled: bool = False
    source_families: tuple[str, ...] = ("varac_bbs", "flmsg", "flamp")
    from_calls: tuple[str, ...] = ()
    subject_contains: tuple[str, ...] = ()
    target_location_ids: tuple[str, ...] = ()
    copy_mode: str = "copy"

    @property
    def ready_to_apply(self) -> bool:
        return bool(self.enabled and self.target_location_ids and (self.from_calls or self.subject_contains))


def load_bbs_sweeper_rules(value: object) -> list[BbsSweeperRule]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    rules: list[BbsSweeperRule] = []
    for idx, item in enumerate(value):
        if not isinstance(item, Mapping):
            continue
        rule_id = _normalize_location_id(item.get("id")) or f"rule-{idx + 1}"
        source_value = item.get("source_families", item.get("sources"))
        source_families = tuple(
            family
            for family in _as_tuple(source_value, normalizer=_normalize_source_family)
            if family in VALID_SOURCE_FAMILIES
        )
        if not source_families:
            source_families = ("varac_bbs", "flmsg", "flamp")
        copy_mode = _clean_token(item.get("copy_mode")).lower() or "copy"
        if copy_mode not in {"copy", "copy_once"}:
            copy_mode = "copy"
        rules.append(
            BbsSweeperRule(
                id=rule_id,
                name=_clean_token(item.get("name")) or f"BBS Sweeper Rule {idx + 1}",
                enabled=bool(item.get("enabled", False)),
                source_families=source_families,
                from_calls=_as_tuple(item.get("from_calls"), normalizer=_normalize_callsign),
                subject_contains=tuple(term.lower() for term in _as_tuple(item.get("subject_contains"))),
                target_location_ids=_as_tuple(item.get("target_location_ids"), normalizer=_normalize_location_id),
                copy_mode=copy_mode,
            )
        )
    return rules


def bbs_sweeper_rules_to_data(rules: Sequence[BbsSweeperRule]) -> list[dict[str, object]]:
    return [
        {
            "id": rule.id,
            "name": rule.name,
            "enabled": bool(rule.enabled),
            "source_families": list(rule.source_families),
            "from_calls": list(rule.from_calls),
            "subject_contains": list(rule.subject_contains),
            "target_location_ids": list(rule.target_location_ids),
            "copy_mode": rule.copy_mode,
        }
        for rule in rules
    ]


def bbs_sweeper_rule_matches(
    rule: BbsSweeperRule,
    *,
    source_family: object,
    from_call: object = "",
    subject: object = "",
    body: object = "",
) -> bool:
    if not rule.ready_to_apply:
        return False
    family = _normalize_source_family(source_family)
    if family not in rule.source_families:
        return False
    sender = _normalize_callsign(from_call)
    if rule.from_calls and sender not in rule.from_calls:
        return False
    haystack = f"{_clean_token(subject)}\n{_clean_token(body)}".lower()
    if rule.subject_contains and not any(term in haystack for term in rule.subject_contains):
        return False
    return True


def matching_bbs_sweeper_targets(
    rules: Sequence[BbsSweeperRule],
    *,
    source_family: object,
    from_call: object = "",
    subject: object = "",
    body: object = "",
) -> tuple[str, ...]:
    targets: list[str] = []
    seen: set[str] = set()
    for rule in rules:
        if not bbs_sweeper_rule_matches(
            rule,
            source_family=source_family,
            from_call=from_call,
            subject=subject,
            body=body,
        ):
            continue
        for target in rule.target_location_ids:
            if target and target not in seen:
                targets.append(target)
                seen.add(target)
    return tuple(targets)
