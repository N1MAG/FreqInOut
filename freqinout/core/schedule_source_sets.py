from __future__ import annotations

import datetime
import json
import re
from typing import Any, Dict, List, Optional

from freqinout.core.multi_radio_store import MultiRadioStore


LIVE_SOURCE_SET_ID = "__live__"
HF_DAILY_SOURCE_SETS_KEY = "freqplanner_hf_daily_schedule_sets"
HF_NET_SOURCE_SETS_KEY = "freqplanner_hf_net_schedule_sets"
SELECTED_HF_DAILY_SOURCE_SET_KEY = "freqplanner_selected_hf_daily_schedule_set_id"
SELECTED_HF_NET_SOURCE_SET_KEY = "freqplanner_selected_hf_net_schedule_set_id"
HF_DAILY_SOURCE_CATEGORY = "hf_daily_schedule"
HF_NET_SOURCE_CATEGORY = "hf_net_schedule"


def slug_source_set_id(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug or f"schedule-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"


def source_sets(settings: Any, key: str) -> List[Dict[str, Any]]:
    rows = settings.get(key, []) or []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _parse_refs(value: Any) -> List[Dict[str, Any]]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if not isinstance(raw, list):
        return []
    return [dict(row) for row in raw if isinstance(row, dict)]


def source_sets_for_category(settings: Any, legacy_key: str, category: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        store = MultiRadioStore()
        for plan in store.list_frequency_plans():
            if str(plan.get("category") or "").strip().lower() != category:
                continue
            plan_id = int(plan.get("id", 0) or 0)
            if plan_id <= 0:
                continue
            rows.append(
                {
                    "id": f"plan:{plan_id}",
                    "db_id": plan_id,
                    "name": str(plan.get("name") or f"Schedule #{plan_id}"),
                    "rows": _parse_refs(plan.get("schedule_refs_json", plan.get("schedule_refs", "[]"))),
                    "created_utc": str(plan.get("created_utc") or ""),
                    "updated_utc": str(plan.get("updated_utc") or ""),
                    "storage": "db",
                }
            )
    except Exception:
        rows = []
    for row in source_sets(settings, legacy_key):
        legacy = dict(row)
        legacy.setdefault("storage", "settings")
        rows.append(legacy)
    return rows


def selected_source_set_id(settings: Any, selected_key: str) -> str:
    return str(settings.get(selected_key, LIVE_SOURCE_SET_ID) or LIVE_SOURCE_SET_ID)


def source_set_row_by_id(settings: Any, sets_key: str, set_id: str) -> Optional[Dict[str, Any]]:
    target = str(set_id or "").strip()
    if not target or target == LIVE_SOURCE_SET_ID:
        return None
    for row in source_sets(settings, sets_key):
        if str(row.get("id") or "").strip() == target:
            return row
    return None


def source_set_row_by_id_for_category(
    settings: Any,
    sets_key: str,
    category: str,
    set_id: str,
) -> Optional[Dict[str, Any]]:
    target = str(set_id or "").strip()
    if not target or target == LIVE_SOURCE_SET_ID:
        return None
    for row in source_sets_for_category(settings, sets_key, category):
        if str(row.get("id") or "").strip() == target:
            return row
    return None


def save_source_set(settings: Any, sets_key: str, selected_key: str, name: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("Enter a clear schedule source name before saving.")
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    base_id = slug_source_set_id(clean_name)
    set_id = base_id
    existing_sets = source_sets(settings, sets_key)
    taken = {str(row.get("id") or "") for row in existing_sets}
    if set_id in taken:
        set_id = f"{base_id}-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    counter = 2
    while set_id in taken:
        set_id = f"{base_id}-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S%f')}-{counter}"
        counter += 1
    saved = {
        "id": set_id,
        "name": clean_name,
        "rows": [dict(row) for row in rows],
        "created_utc": now,
        "updated_utc": now,
    }
    existing_sets.append(saved)
    settings.set(sets_key, existing_sets)
    settings.set(selected_key, set_id)
    if hasattr(settings, "save"):
        settings.save()
    return saved


def save_source_schedule(
    settings: Any,
    category: str,
    selected_key: str,
    name: str,
    rows: List[Dict[str, Any]],
    *,
    existing_plan_id: Optional[int] = None,
) -> Dict[str, Any]:
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("Enter a clear schedule name before saving.")
    payload: Dict[str, Any] = {
        "name": clean_name,
        "status": "saved",
        "category": category,
        "description": "Saved source schedule for FreqPlanner composition.",
        "source_refs": [category],
        "schedule_refs": [dict(row) for row in rows],
        "frequency_refs": [],
        "group_refs": sorted(
            {
                str(row.get("group_name") or row.get("group") or "").strip().upper()
                for row in rows
                if str(row.get("group_name") or row.get("group") or "").strip()
            }
        ),
        "notes": f"Source schedule saved {datetime.datetime.now(datetime.timezone.utc).isoformat()}",
    }
    if existing_plan_id is not None and int(existing_plan_id) > 0:
        payload["id"] = int(existing_plan_id)
    saved = MultiRadioStore().save_frequency_plan(payload)
    saved_id = int(saved.get("id", 0) or 0)
    if saved_id > 0:
        settings.set(selected_key, f"plan:{saved_id}")
        if hasattr(settings, "save"):
            settings.save()
    return {
        "id": f"plan:{saved_id}" if saved_id > 0 else "",
        "db_id": saved_id,
        "name": str(saved.get("name") or clean_name),
        "rows": [dict(row) for row in rows],
        "storage": "db",
    }


def delete_source_schedule(settings: Any, sets_key: str, selected_key: str, set_id: str) -> bool:
    target = str(set_id or "").strip()
    if not target or target == LIVE_SOURCE_SET_ID:
        return False
    expected_category = HF_DAILY_SOURCE_CATEGORY if sets_key == HF_DAILY_SOURCE_SETS_KEY else HF_NET_SOURCE_CATEGORY
    if target.startswith("plan:"):
        plan_id = int(target.split(":", 1)[1] or 0)
        if plan_id > 0:
            store = MultiRadioStore()
            plan = store.get_frequency_plan(plan_id)
            if not plan or str(plan.get("category") or "").strip().lower() != expected_category:
                raise ValueError("Only the matching HF Daily or HF Net source schedule can be deleted here.")
            store.delete_frequency_plan(plan_id)
            settings.set(selected_key, LIVE_SOURCE_SET_ID)
            if hasattr(settings, "save"):
                settings.save()
            return True
    remaining = [row for row in source_sets(settings, sets_key) if str(row.get("id") or "").strip() != target]
    settings.set(sets_key, remaining)
    settings.set(selected_key, LIVE_SOURCE_SET_ID)
    if hasattr(settings, "save"):
        settings.save()
    return True
