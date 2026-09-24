from __future__ import annotations

import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from freqinout.core.nbems_compose import safe_varac_bbs_filename, unique_destination


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


@dataclass(frozen=True)
class BbsSweeperCopyPlan:
    source_path: str
    source_family: str
    target_location_ids: tuple[str, ...]
    matched_rule_ids: tuple[str, ...]
    copy_once_location_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class BbsSweeperCopyResult:
    source_path: str
    target_location_id: str
    target_path: str
    copied: bool
    matched_rule_ids: tuple[str, ...] = ()
    skipped_reason: str = ""


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


def plan_bbs_sweeper_copies(
    rules: Sequence[BbsSweeperRule],
    candidates: Sequence[Mapping[str, object]],
    *,
    available_location_ids: Iterable[object] = (),
) -> list[BbsSweeperCopyPlan]:
    available_locations = {
        location_id
        for location_id in (_normalize_location_id(value) for value in available_location_ids)
        if location_id
    }
    plans: list[BbsSweeperCopyPlan] = []
    for candidate in candidates:
        source_path = _clean_token(candidate.get("source_path") or candidate.get("path"))
        if not source_path:
            continue
        source_family = _normalize_source_family(candidate.get("source_family") or candidate.get("source"))
        if source_family not in VALID_SOURCE_FAMILIES:
            continue
        from_call = (
            candidate.get("from_call")
            or candidate.get("from")
            or candidate.get("sender")
            or candidate.get("callsign")
            or ""
        )
        subject = (
            candidate.get("subject")
            or candidate.get("title")
            or candidate.get("name")
            or candidate.get("filename")
            or ""
        )
        body = candidate.get("body") or candidate.get("message") or candidate.get("summary") or candidate.get("remarks") or ""
        targets: list[str] = []
        target_seen: set[str] = set()
        copy_once_targets: set[str] = set()
        repeat_copy_targets: set[str] = set()
        matched_rule_ids: list[str] = []
        rule_seen: set[str] = set()
        for rule in rules:
            if not bbs_sweeper_rule_matches(
                rule,
                source_family=source_family,
                from_call=from_call,
                subject=subject,
                body=body,
            ):
                continue
            if rule.id not in rule_seen:
                matched_rule_ids.append(rule.id)
                rule_seen.add(rule.id)
            for target in rule.target_location_ids:
                if available_locations and target not in available_locations:
                    continue
                if target and target not in target_seen:
                    targets.append(target)
                    target_seen.add(target)
                if target:
                    if rule.copy_mode == "copy_once":
                        copy_once_targets.add(target)
                    else:
                        repeat_copy_targets.add(target)
        if targets:
            effective_copy_once = tuple(target for target in targets if target in copy_once_targets and target not in repeat_copy_targets)
            plans.append(
                BbsSweeperCopyPlan(
                    source_path=source_path,
                    source_family=source_family,
                    target_location_ids=tuple(targets),
                    matched_rule_ids=tuple(matched_rule_ids),
                    copy_once_location_ids=effective_copy_once,
                )
            )
    return plans


def apply_bbs_sweeper_copy_plan(
    plans: Sequence[BbsSweeperCopyPlan],
    target_dirs_by_location_id: Mapping[str, object],
    *,
    dry_run: bool = False,
) -> list[BbsSweeperCopyResult]:
    normalized_targets = {
        location_id: Path(str(path)).expanduser()
        for location_id, path in (
            (_normalize_location_id(key), value) for key, value in target_dirs_by_location_id.items()
        )
        if location_id and str(path or "").strip()
    }
    results: list[BbsSweeperCopyResult] = []
    for plan in plans:
        src = Path(plan.source_path).expanduser()
        if not src.exists() or not src.is_file():
            for target_id in plan.target_location_ids:
                results.append(
                    BbsSweeperCopyResult(
                        source_path=str(src),
                        target_location_id=target_id,
                        target_path="",
                        copied=False,
                        matched_rule_ids=plan.matched_rule_ids,
                        skipped_reason="source_missing",
                    )
                )
            continue
        safe_name = safe_varac_bbs_filename(src.name)
        for target_id in plan.target_location_ids:
            target_dir = normalized_targets.get(_normalize_location_id(target_id))
            if target_dir is None or not target_dir.exists() or not target_dir.is_dir():
                results.append(
                    BbsSweeperCopyResult(
                        source_path=str(src),
                        target_location_id=target_id,
                        target_path=str(target_dir or ""),
                        copied=False,
                        matched_rule_ids=plan.matched_rule_ids,
                        skipped_reason="target_missing",
                    )
                )
                continue
            base_dst = target_dir / safe_name
            if _normalize_location_id(target_id) in {
                _normalize_location_id(value) for value in plan.copy_once_location_ids
            } and base_dst.exists():
                results.append(
                    BbsSweeperCopyResult(
                        source_path=str(src),
                        target_location_id=target_id,
                        target_path=str(base_dst),
                        copied=False,
                        matched_rule_ids=plan.matched_rule_ids,
                        skipped_reason="copy_once_exists",
                    )
                )
                continue
            try:
                if src.resolve() == base_dst.resolve():
                    results.append(
                        BbsSweeperCopyResult(
                            source_path=str(src),
                            target_location_id=target_id,
                            target_path=str(base_dst),
                            copied=False,
                            matched_rule_ids=plan.matched_rule_ids,
                            skipped_reason="source_is_target",
                        )
                    )
                    continue
            except Exception:
                pass
            dst = unique_destination(base_dst)
            if dst is None:
                results.append(
                    BbsSweeperCopyResult(
                        source_path=str(src),
                        target_location_id=target_id,
                        target_path=str(base_dst),
                        copied=False,
                        matched_rule_ids=plan.matched_rule_ids,
                        skipped_reason="no_unique_destination",
                    )
                )
                continue
            if dry_run:
                results.append(
                    BbsSweeperCopyResult(
                        source_path=str(src),
                        target_location_id=target_id,
                        target_path=str(dst),
                        copied=False,
                        matched_rule_ids=plan.matched_rule_ids,
                        skipped_reason="dry_run",
                    )
                )
                continue
            shutil.copy2(str(src), str(dst))
            results.append(
                BbsSweeperCopyResult(
                    source_path=str(src),
                    target_location_id=target_id,
                    target_path=str(dst),
                    copied=True,
                    matched_rule_ids=plan.matched_rule_ids,
                )
            )
    return results
