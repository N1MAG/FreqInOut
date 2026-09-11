from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from freqinout.core.js8_storage import has_js8_message_evidence, resolve_js8_storage


@dataclass(frozen=True)
class JS8StorageReconciliationResult:
    state: str
    js8_instance_id: int = 0
    application_data_root: str = ""
    detail: str = ""


def reconcile_js8_storage_profile(store: Any, profile: Mapping[str, Any]) -> JS8StorageReconciliationResult:
    """Qualify one already-configured JS8 root without scanning message history.

    The caller owns scheduling this work away from the Qt thread.  Only the
    three well-known message filenames beneath the configured root are probed.
    """

    try:
        instance_id = int(profile.get("js8_instance_id", 0) or 0)
    except (TypeError, ValueError):
        instance_id = 0
    if instance_id <= 0:
        return JS8StorageReconciliationResult("not_configured", detail="No linked JS8 instance.")

    existing = store.get_js8_instance(instance_id) or {}
    existing_evidence = str(existing.get("storage_evidence", "") or "").strip()
    if existing_evidence.startswith("mismatch:"):
        return JS8StorageReconciliationResult(
            "mismatch",
            instance_id,
            str(existing.get("application_data_root", "") or ""),
            "The expected JS8 identity differs from the retained verified root; operator review is required.",
        )
    explicit_root = str(
        existing.get("application_data_root", "")
        or profile.get("js8_message_storage_root", "")
        or profile.get("application_data_root", "")
        or ""
    ).strip()
    legacy_directed = str(existing.get("directed_path", "") or profile.get("js8_directed_path", "") or "").strip()
    if not explicit_root and legacy_directed:
        return JS8StorageReconciliationResult(
            "legacy_preserved",
            instance_id,
            detail="Legacy explicit message paths were retained for operator verification.",
        )
    values = {**dict(existing), **dict(profile)}
    storage = resolve_js8_storage(values)
    root = str(storage.data_root or "").strip()
    if not root:
        return JS8StorageReconciliationResult("needs_verification", instance_id, detail="No message-storage root.")

    existing_root = str(existing.get("application_data_root", "") or "").strip()
    verified_existing = existing_evidence.startswith(("operator_confirmed", "runtime_verified"))
    if verified_existing and existing_root and Path(existing_root) != Path(root):
        return JS8StorageReconciliationResult(
            "mismatch",
            instance_id,
            existing_root,
            "The expected root differs from the last verified root; the verified mapping was retained.",
        )

    if not has_js8_message_evidence(root):
        return JS8StorageReconciliationResult(
            "needs_verification",
            instance_id,
            root,
            "No ALL.TXT, DIRECTED.TXT, or inbox.db3 evidence is present yet.",
        )

    mode = storage.expected_mode if storage.expected_mode in {"rig_scoped", "shared"} else storage.storage_mode
    if mode not in {"rig_scoped", "shared"}:
        mode = "unverified"
    desired_paths = {
        "application_data_root": root,
        "all_path": str(Path(root) / "ALL.TXT"),
        "directed_path": str(Path(root) / "DIRECTED.TXT"),
        "inbox_path": str(Path(root) / "inbox.db3"),
        "storage_mode": mode,
        "storage_evidence": "runtime_verified:message_files",
    }
    if all(str(existing.get(key, "") or "") == value for key, value in desired_paths.items()):
        return JS8StorageReconciliationResult("verified_unchanged", instance_id, root, "Message storage verified.")
    updated = dict(existing)
    updated.update(
        {
            "id": instance_id,
            "variant_family": storage.variant_family,
            "variant_version": storage.variant_version,
            "rig_name": storage.rig_name,
            "rig_name_source": storage.rig_name_source,
            **desired_paths,
            "save_dir": str(existing.get("save_dir", "") or storage.save_dir),
            "storage_verified_utc": datetime.now(timezone.utc).isoformat(),
        }
    )
    store.save_js8_instance(updated)
    return JS8StorageReconciliationResult("verified", instance_id, root, "Message storage verified.")
