from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

ROLE_WORKSPACE_PREFS_KEY = "fldigi_role_workspace_prefs_v1"


@dataclass
class WorkspaceBucketDefinition:
    bucket_id: str
    title: str
    scope: str
    function: str
    visible_by_default: bool = True
    read_only: bool = False
    persist_content: bool = True
    file_backed: bool = True
    custom_name: str = ""
    compare_role_default: bool = False

    def as_dict(self) -> Dict[str, object]:
        return {
            "bucket_id": self.bucket_id,
            "title": self.title,
            "scope": self.scope,
            "function": self.function,
            "visible_by_default": self.visible_by_default,
            "read_only": self.read_only,
            "persist_content": self.persist_content,
            "file_backed": self.file_backed,
            "custom_name": self.custom_name,
            "compare_role_default": self.compare_role_default,
        }


@dataclass
class RoleWorkspacePreset:
    role: str
    local_buckets: List[WorkspaceBucketDefinition] = field(default_factory=list)
    reference_buckets: List[WorkspaceBucketDefinition] = field(default_factory=list)
    compare_source_bucket_id: str = ""
    compare_target_bucket_id: str = ""

    def visible_bucket_ids(self) -> List[str]:
        bucket_ids: List[str] = []
        for bucket in self.local_buckets + self.reference_buckets:
            if bucket.visible_by_default:
                bucket_ids.append(bucket.bucket_id)
        return bucket_ids

    def all_buckets(self) -> List[WorkspaceBucketDefinition]:
        return list(self.local_buckets) + list(self.reference_buckets)


_ROLE_PRESETS: Dict[str, RoleWorkspacePreset] = {
    "NCS": RoleWorkspacePreset(
        role="NCS",
        local_buckets=[
            WorkspaceBucketDefinition("tfc", "TFC", "NCS", "TFC", visible_by_default=True, compare_role_default=True),
            WorkspaceBucketDefinition("qru", "QRU", "NCS", "QRU", visible_by_default=True),
            WorkspaceBucketDefinition("late", "LATE", "NCS", "LATE", visible_by_default=True),
        ],
        reference_buckets=[
            WorkspaceBucketDefinition(
                "ancs_reference",
                "ANCS List",
                "ANCS",
                "CUSTOM",
                visible_by_default=True,
                read_only=True,
                persist_content=False,
                file_backed=False,
            )
        ],
        compare_source_bucket_id="tfc",
        compare_target_bucket_id="ancs_reference",
    ),
    "ANCS": RoleWorkspacePreset(
        role="ANCS",
        local_buckets=[
            WorkspaceBucketDefinition("tfc", "TFC", "ANCS", "TFC", visible_by_default=True, compare_role_default=True),
            WorkspaceBucketDefinition("qru", "QRU", "ANCS", "QRU", visible_by_default=True),
            WorkspaceBucketDefinition("late", "LATE", "ANCS", "LATE", visible_by_default=True),
        ],
        reference_buckets=[
            WorkspaceBucketDefinition(
                "ncs_reference",
                "NCS List",
                "NCS",
                "CUSTOM",
                visible_by_default=True,
                read_only=True,
                persist_content=False,
                file_backed=False,
            )
        ],
        compare_source_bucket_id="tfc",
        compare_target_bucket_id="ncs_reference",
    ),
    "JOINER": RoleWorkspacePreset(
        role="JOINER",
        local_buckets=[
            WorkspaceBucketDefinition("seen_locally", "Seen Locally", "JOINER", "CUSTOM", visible_by_default=True, compare_role_default=True, custom_name="Seen Locally"),
            WorkspaceBucketDefinition("late", "LATE", "JOINER", "LATE", visible_by_default=False),
        ],
        reference_buckets=[
            WorkspaceBucketDefinition(
                "ncs_reference",
                "NCS List",
                "NCS",
                "CUSTOM",
                visible_by_default=True,
                read_only=True,
                persist_content=False,
                file_backed=False,
            )
        ],
        compare_source_bucket_id="seen_locally",
        compare_target_bucket_id="ncs_reference",
    ),
}


def normalize_role(role: str) -> str:
    text = str(role or "").strip().upper()
    return text if text in _ROLE_PRESETS else "NCS"


def get_role_workspace_preset(role: str) -> RoleWorkspacePreset:
    return _ROLE_PRESETS[normalize_role(role)]


def default_role_workspace_prefs() -> Dict[str, Dict[str, object]]:
    prefs: Dict[str, Dict[str, object]] = {}
    for role, preset in _ROLE_PRESETS.items():
        prefs[role] = {
            "visible_bucket_ids": preset.visible_bucket_ids(),
            "compare_source_bucket_id": preset.compare_source_bucket_id,
            "compare_target_bucket_id": preset.compare_target_bucket_id,
        }
    return prefs


def load_role_workspace_prefs(settings) -> Dict[str, Dict[str, object]]:
    data = settings.all()
    prefs = data.get(ROLE_WORKSPACE_PREFS_KEY, {})
    if not isinstance(prefs, dict):
        return default_role_workspace_prefs()
    merged = default_role_workspace_prefs()
    for role, value in prefs.items():
        role_key = normalize_role(role)
        if not isinstance(value, dict):
            continue
        merged[role_key].update({
            "visible_bucket_ids": list(value.get("visible_bucket_ids", merged[role_key]["visible_bucket_ids"])) if isinstance(value.get("visible_bucket_ids", []), list) else merged[role_key]["visible_bucket_ids"],
            "compare_source_bucket_id": str(value.get("compare_source_bucket_id", merged[role_key]["compare_source_bucket_id"]) or "").strip(),
            "compare_target_bucket_id": str(value.get("compare_target_bucket_id", merged[role_key]["compare_target_bucket_id"]) or "").strip(),
        })
    return merged


def save_role_workspace_prefs(settings, prefs: Dict[str, Dict[str, object]]) -> None:
    settings.set(ROLE_WORKSPACE_PREFS_KEY, prefs)


def bucket_labels_for_role(role: str) -> Dict[str, str]:
    preset = get_role_workspace_preset(role)
    return {bucket.bucket_id: bucket.title for bucket in preset.all_buckets()}


def bucket_definition_map(role: str) -> Dict[str, WorkspaceBucketDefinition]:
    preset = get_role_workspace_preset(role)
    return {bucket.bucket_id: bucket for bucket in preset.all_buckets()}


def role_compare_defaults(role: str, prefs: Optional[Dict[str, Dict[str, object]]] = None) -> Dict[str, str]:
    normalized = normalize_role(role)
    preset = get_role_workspace_preset(normalized)
    if not prefs:
        prefs = default_role_workspace_prefs()
    role_prefs = prefs.get(normalized, {}) if isinstance(prefs, dict) else {}
    source_bucket_id = str(role_prefs.get("compare_source_bucket_id", preset.compare_source_bucket_id) or "").strip() or preset.compare_source_bucket_id
    target_bucket_id = str(role_prefs.get("compare_target_bucket_id", preset.compare_target_bucket_id) or "").strip() or preset.compare_target_bucket_id
    return {
        "source_bucket_id": source_bucket_id,
        "target_bucket_id": target_bucket_id,
    }
