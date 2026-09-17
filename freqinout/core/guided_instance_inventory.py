"""Pure authority helpers for guided software-instance drafts.

This module deliberately performs no discovery, filesystem access, socket
work, or persistence.  UI hosts provide already-loaded application rows and
retained drafts.  The helpers turn those values into one immutable inventory
fingerprint, stable draft identities, and source-lock evidence that can be
checked again immediately before an atomic save.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping


_KEY_RE = re.compile(r"[^a-z0-9_.-]+")
_IDENTITY_FIELDS = {
    "js8call": (
        "system_key", "host", "port", "udp_port", "rig_name",
        "application_path", "configuration_path", "storage_path",
        "launch_command",
    ),
    "fast_light": (
        "system_key", "host", "port", "secondary_port",
        "application_path", "secondary_application_path",
        "configuration_path", "secondary_configuration_path",
        "storage_path", "secondary_storage_path", "launch_command",
    ),
    "varac": (
        "system_key", "application_path", "configuration_path",
        "storage_path", "secondary_storage_path", "outbox_path",
        "working_directory", "cluster_path", "cluster_id",
        "cluster_instance_number", "launch_command",
    ),
}


def _text(value: object) -> str:
    return str(value or "").strip()


def _key(value: object) -> str:
    return _KEY_RE.sub("-", _text(value).casefold()).strip("-._")


def _family(value: object) -> str:
    family = _key(value)
    if family not in _IDENTITY_FIELDS:
        raise ValueError(f"Unsupported guided software family: {value}")
    return family


def stable_draft_instance_key(owner_draft_key: object, family_key: object) -> str:
    """Return an opaque identity unaffected by editable names or navigation."""

    owner = _text(owner_draft_key)
    family = _family(family_key)
    if not owner:
        raise ValueError("owner_draft_key is required for an unsaved instance")
    digest = hashlib.sha256(f"{owner}\0{family}".encode("utf-8")).hexdigest()[:20]
    return f"draft-{family}-{digest}"


def _normalized_identity(family_key: object, row: Mapping[str, Any]) -> dict[str, Any]:
    family = _family(family_key)
    aliases = {
        "host": ("host", "flrig_host", "fldigi_host"),
        "application_path": ("application_path", "install_path", "path", "flrig_path"),
        "secondary_application_path": ("secondary_application_path", "fldigi_path"),
        "configuration_path": ("configuration_path", "profile_path", "ini_path"),
        "secondary_configuration_path": (
            "secondary_configuration_path",
            "fldigi_config_path",
        ),
        "storage_path": ("storage_path", "application_data_root", "db_path", "fldigi_log_path"),
        "secondary_storage_path": ("secondary_storage_path", "incoming_path", "fldigi_checkin_dir"),
        "outbox_path": ("outbox_path", "outbox_dir"),
        "port": ("port", "js8_tcp_port", "flrig_port"),
        "secondary_port": ("secondary_port", "fldigi_port"),
        "udp_port": ("udp_port", "js8_udp_port"),
        "rig_name": ("rig_name", "js8_rig_name"),
        "launch_command": ("launch_command", "launch_cmd"),
    }
    result: dict[str, Any] = {"family_key": family}
    for name in _IDENTITY_FIELDS[family]:
        candidates = aliases.get(name, (name,))
        value: Any = ""
        for candidate in candidates:
            if row.get(candidate) not in (None, ""):
                value = row.get(candidate)
                break
        if name in {"port", "udp_port", "secondary_port", "cluster_instance_number"}:
            try:
                value = int(value or 0)
            except (TypeError, ValueError):
                value = 0
        elif isinstance(value, str):
            value = value.strip()
        result[name] = value
    return result


def source_identity_fingerprint(family_key: object, row: Mapping[str, Any]) -> str:
    """Fingerprint the complete imported identity that must remain locked."""

    encoded = json.dumps(
        _normalized_identity(family_key, row),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _snapshot_row(family: str, row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = _normalized_identity(family, row)
    normalized.update(
        {
            "id": int(row.get("id") or 0),
            "name": _text(row.get("name") or row.get("instance_name")),
            "instance_key": _text(row.get("instance_key") or row.get("draft_instance_key")),
            "owner_draft_key": _text(row.get("owner_draft_key")),
            "source_fingerprint": source_identity_fingerprint(family, row),
        }
    )
    return normalized


@dataclass(frozen=True)
class GuidedInstanceInventorySnapshot:
    """One immutable saved-plus-retained inventory generation."""

    generation: int
    rows_by_family: Mapping[str, tuple[Mapping[str, Any], ...]] = field(default_factory=dict)
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        generation = int(self.generation)
        if generation < 0:
            raise ValueError("inventory generation must be non-negative")
        normalized: dict[str, tuple[Mapping[str, Any], ...]] = {}
        for raw_family, raw_rows in dict(self.rows_by_family).items():
            family = _family(raw_family)
            rows = tuple(
                MappingProxyType(_snapshot_row(family, row))
                for row in raw_rows
                if isinstance(row, Mapping)
            )
            normalized[family] = tuple(
                sorted(
                    rows,
                    key=lambda item: (
                        _text(item.get("instance_key")),
                        _text(item.get("system_key")),
                        int(item.get("id") or 0),
                        _text(item.get("source_fingerprint")),
                    ),
                )
            )
        serializable = {
            family: [dict(row) for row in rows]
            for family, rows in sorted(normalized.items())
        }
        digest = hashlib.sha256(
            json.dumps(serializable, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        object.__setattr__(self, "generation", generation)
        object.__setattr__(self, "rows_by_family", MappingProxyType(normalized))
        object.__setattr__(self, "fingerprint", digest)

    def rows_for(self, family_key: object) -> tuple[Mapping[str, Any], ...]:
        return self.rows_by_family.get(_family(family_key), ())

    def source_is_current(self, family_key: object, source_id: object, fingerprint: object) -> bool:
        try:
            wanted_id = int(source_id or 0)
        except (TypeError, ValueError):
            return False
        wanted = _text(fingerprint)
        return bool(
            wanted
            and any(
                int(row.get("id") or 0) == wanted_id
                and _text(row.get("source_fingerprint")) == wanted
                for row in self.rows_for(family_key)
            )
        )


def build_guided_instance_inventory(
    saved_by_family: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    retained_drafts: Mapping[str, Mapping[str, Any]] | None = None,
    generation: int = 0,
) -> GuidedInstanceInventorySnapshot:
    """Combine saved rows and unsaved reviewed drafts into one collision view."""

    combined: dict[str, list[Mapping[str, Any]]] = {}
    for family, rows in saved_by_family.items():
        normalized_family = _family(family)
        combined[normalized_family] = [row for row in rows if isinstance(row, Mapping)]
    for family, draft in dict(retained_drafts or {}).items():
        if not isinstance(draft, Mapping):
            continue
        normalized_family = _family(family)
        combined.setdefault(normalized_family, []).append(dict(draft))
    return GuidedInstanceInventorySnapshot(generation=generation, rows_by_family=combined)


def first_available_port(
    snapshot: GuidedInstanceInventorySnapshot,
    family_key: object,
    *,
    start_port: int,
    field_name: str = "port",
) -> int:
    occupied = {
        int(row.get(field_name) or 0)
        for rows in snapshot.rows_by_family.values()
        for row in rows
        if int(row.get(field_name) or 0) > 0
    }
    for port in range(int(start_port), 65536):
        if port not in occupied:
            return port
    raise ValueError(f"No available port remains for {_family(family_key)}")


def distinct_draft_seed(
    family_key: object,
    *,
    owner_draft_key: object,
    snapshot: GuidedInstanceInventorySnapshot,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a safe new-instance seed that never reuses source identity."""

    family = _family(family_key)
    evidence = dict(source or {})
    seed: dict[str, Any] = {
        "family_key": family,
        "mode": "managed",
        "ownership": "fio-managed",
        "owner_draft_key": _text(owner_draft_key),
        "draft_instance_key": stable_draft_instance_key(owner_draft_key, family),
        "inventory_fingerprint": snapshot.fingerprint,
        "source_fingerprint": "",
        "source_locked": False,
        "imported_id": None,
        "imported_system_key": "",
        # Executables and verified version evidence may be shared safely.
        "application_path": _text(evidence.get("application_path") or evidence.get("install_path") or evidence.get("path") or evidence.get("flrig_path")),
        "secondary_application_path": _text(evidence.get("secondary_application_path") or evidence.get("fldigi_path")),
        "flmsg_application_path": _text(evidence.get("flmsg_application_path") or evidence.get("flmsg_path")),
        "flamp_application_path": _text(evidence.get("flamp_application_path") or evidence.get("flamp_path")),
        "variant": _text(evidence.get("variant") or evidence.get("variant_family")),
        "version": _text(evidence.get("version") or evidence.get("variant_version")),
        "host": _text(evidence.get("host")) or "127.0.0.1",
        "configuration_path": "",
        "secondary_configuration_path": "",
        "storage_path": "",
        "secondary_storage_path": "",
        "outbox_path": "",
        "working_directory": "",
        "launch_command": "",
        "rig_name": "",
        "cluster_path": "standalone",
        "cluster_id": "",
        "cluster_instance_number": 0,
    }
    if family == "js8call":
        seed["port"] = first_available_port(snapshot, family, start_port=2442)
        seed["udp_port"] = first_available_port(snapshot, family, start_port=2237, field_name="udp_port")
    elif family == "fast_light":
        seed["port"] = first_available_port(snapshot, family, start_port=12345)
        seed["secondary_port"] = first_available_port(snapshot, family, start_port=7362, field_name="secondary_port")
    else:
        seed["port"] = 0
    return seed


__all__ = [
    "GuidedInstanceInventorySnapshot",
    "build_guided_instance_inventory",
    "distinct_draft_seed",
    "first_available_port",
    "source_identity_fingerprint",
    "stable_draft_instance_key",
]
