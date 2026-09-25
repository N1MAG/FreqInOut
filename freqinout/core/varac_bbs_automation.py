from __future__ import annotations

"""Background execution for station Managed BBS automation rules."""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.varac_bbs_library_store import (
    ensure_bbs_library_schema,
    list_bbs_locations,
    load_station_bbs_sweeper_rules,
)
from freqinout.core.varac_bbs_sweeper import (
    BbsSweeperCopyPlan,
    apply_bbs_sweeper_copy_plan,
    load_bbs_sweeper_rules,
    plan_bbs_sweeper_copies,
)
from freqinout.core.varac_bbs_vault import load_vault_locations


@dataclass(frozen=True)
class BbsAutomationRunResult:
    considered: int = 0
    planned: int = 0
    copied: int = 0
    skipped: int = 0
    error: str = ""


def _read_head(path: Path, limit: int = 32768) -> str:
    try:
        with path.open("rb") as stream:
            return stream.read(limit).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _sender_from_file(path: Path, text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for marker in (":hdr_fm:", ":hdr_ed:"):
        for index, line in enumerate(lines):
            if not line.lower().startswith(marker):
                continue
            for candidate in lines[index + 1 :]:
                match = re.search(r"\b[A-Z]{1,2}\d[A-Z0-9]{1,4}\b", candidate.upper())
                if match:
                    return match.group(0)
            break
    for token in re.split(r"[-_\s]+", path.stem):
        candidate = token.strip().upper()
        if re.fullmatch(r"[A-Z]{1,2}\d[A-Z0-9]{1,4}", candidate):
            return candidate
    return ""


def _fingerprint(record: FileRecord, target_id: str) -> str:
    try:
        resolved = record.path.expanduser().resolve()
        path = str(resolved)
        modified = str(int(resolved.stat().st_mtime_ns))
    except Exception:
        path = str(record.path)
        modified = f"{float(record.mtime or 0.0):.9f}"
    raw = f"{path}|{int(record.size or 0)}|{modified}|{target_id}"
    return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()


def _source_family(record: FileRecord) -> str:
    origin = str(record.origin or "").strip().lower()
    if origin in {"bbs", "varac", "varac_bbs"}:
        return "varac_bbs"
    if origin in {"flmsg", "flamp"}:
        return origin
    return ""


def apply_station_bbs_automation(
    db_path: str | Path,
    records: Iterable[FileRecord],
    *,
    legacy_rules: object = (),
    legacy_locations: object = (),
    legacy_enabled: bool = False,
) -> BbsAutomationRunResult:
    """Apply saved rules to changed records once per source version/target.

    Canonical station metadata wins.  Legacy settings are consulted only when
    a station-owned rule value has never been saved, which keeps upgraded
    single-radio installations functional without reviving deleted rules.
    """

    candidates_records = [record for record in records if isinstance(record, FileRecord)]
    if not candidates_records:
        return BbsAutomationRunResult()
    try:
        with connect_sqlite(Path(db_path)) as conn:
            ensure_bbs_library_schema(conn)
            canonical_rules = load_station_bbs_sweeper_rules(conn)
            rules = load_bbs_sweeper_rules(legacy_rules if canonical_rules is None else canonical_rules)
            enabled_row = conn.execute(
                "SELECT value FROM bbs_library_meta WHERE key='station_enabled' LIMIT 1"
            ).fetchone()
            if canonical_rules is not None:
                station_enabled = True
            elif enabled_row is not None:
                station_enabled = str(enabled_row[0] or "0").strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
            else:
                station_enabled = bool(legacy_enabled)
            target_dirs = {
                location.location_id: location.source_dir
                for location in list_bbs_locations(conn, include_disabled=False)
                if str(location.source_dir or "").strip()
            }
            if not target_dirs and canonical_rules is None:
                target_dirs = {
                    location.id: location.source_dir
                    for location in load_vault_locations(legacy_locations)
                    if location.enabled and str(location.source_dir or "").strip()
                }
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bbs_automation_delivery (
                    fingerprint TEXT PRIMARY KEY,
                    source_path TEXT NOT NULL,
                    target_location_id TEXT NOT NULL,
                    copied_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            delivered = {
                str(row[0])
                for row in conn.execute("SELECT fingerprint FROM bbs_automation_delivery").fetchall()
            }
        if not station_enabled or not any(rule.ready_to_apply for rule in rules):
            return BbsAutomationRunResult(considered=len(candidates_records))
        usable_targets: dict[str, str] = {}
        resolved_target_roots: list[Path] = []
        for location_id, raw_path in target_dirs.items():
            path = Path(str(raw_path)).expanduser()
            if path.exists() and path.is_dir():
                usable_targets[str(location_id)] = str(path)
                try:
                    resolved_target_roots.append(path.resolve())
                except Exception:
                    pass
        if not usable_targets:
            return BbsAutomationRunResult(considered=len(candidates_records))
        candidates: list[dict[str, object]] = []
        records_by_path: dict[str, FileRecord] = {}
        for record in candidates_records:
            family = _source_family(record)
            if not family or not record.path.exists() or not record.path.is_file():
                continue
            try:
                resolved_source = record.path.resolve()
                if any(resolved_source.is_relative_to(root) for root in resolved_target_roots):
                    continue
            except Exception:
                pass
            text = _read_head(record.path)
            path_key = str(record.path)
            records_by_path[path_key] = record
            candidates.append(
                {
                    "path": path_key,
                    "source": family,
                    "sender": _sender_from_file(record.path, text),
                    "subject": record.path.name,
                    "body": text,
                }
            )
        plans = plan_bbs_sweeper_copies(rules, candidates, available_location_ids=usable_targets)
        filtered = []
        planned_targets = 0
        for plan in plans:
            record = records_by_path.get(plan.source_path)
            if record is None:
                continue
            targets = tuple(
                target
                for target in plan.target_location_ids
                if _fingerprint(record, target) not in delivered
            )
            if not targets:
                continue
            planned_targets += len(targets)
            filtered.append(
                BbsSweeperCopyPlan(
                    source_path=plan.source_path,
                    source_family=plan.source_family,
                    target_location_ids=targets,
                    matched_rule_ids=plan.matched_rule_ids,
                    copy_once_location_ids=tuple(
                        target for target in plan.copy_once_location_ids if target in targets
                    ),
                )
            )
        results = apply_bbs_sweeper_copy_plan(filtered, usable_targets)
        copied_results = [result for result in results if result.copied]
        if copied_results:
            with connect_sqlite(Path(db_path)) as conn:
                with conn:
                    for result in copied_results:
                        record = records_by_path.get(result.source_path)
                        if record is None:
                            continue
                        conn.execute(
                            "INSERT OR IGNORE INTO bbs_automation_delivery"
                            "(fingerprint, source_path, target_location_id) VALUES(?, ?, ?)",
                            (
                                _fingerprint(record, result.target_location_id),
                                result.source_path,
                                result.target_location_id,
                            ),
                        )
        return BbsAutomationRunResult(
            considered=len(candidates_records),
            planned=planned_targets,
            copied=len(copied_results),
            skipped=len(results) - len(copied_results),
        )
    except Exception as exc:
        return BbsAutomationRunResult(considered=len(candidates_records), error=str(exc))
