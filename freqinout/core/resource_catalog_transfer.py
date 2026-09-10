"""Bounded, preview-first JSON transfer for the Local Nets resource catalog.

The module is deliberately Qt-free.  Previewing parses only in memory and
never touches a store; applying a preview uses only ``ResourceCatalogStore``'s
public create/update APIs.  It exports no credentials or unrelated settings.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from freqinout.core.resource_catalog_models import (
    CatalogSource,
    CatalogValidationError,
    FrequencyResource,
    NetDirectoryEntry,
    NetDirectorySession,
    ReadOnlyResourceError,
)
from freqinout.core.resource_catalog_store import MAX_RESULTS, ResourceCatalogStore


TRANSFER_SCHEMA_VERSION = 1
DEFAULT_MAX_BYTES = 1_000_000
DEFAULT_MAX_ITEMS = MAX_RESULTS
_TRANSFER_KIND = "resource_catalog_transfer"
_STATUSES = frozenset({"new", "updated", "unchanged", "duplicate", "invalid", "ambiguous", "conflict"})
_SECRET_FIELD_MARKERS = ("password", "secret", "token", "api_key", "apikey", "private_key", "credential")


@dataclass(frozen=True, slots=True)
class TransferDiagnostic:
    status: str
    item_type: str
    item_key: str | None
    field: str | None = None
    supplied_value: object | None = None
    reason: str = ""
    suggested_resolution: str = ""

    def __post_init__(self) -> None:
        if self.status not in _STATUSES:
            raise ValueError("invalid transfer diagnostic status")


@dataclass(frozen=True, slots=True)
class ImportPreview:
    target_source: CatalogSource
    frequencies: tuple[FrequencyResource, ...]
    net_entries: tuple[NetDirectoryEntry, ...]
    sessions: tuple[NetDirectorySession, ...]
    diagnostics: tuple[TransferDiagnostic, ...]
    expected_version_hashes: Mapping[str, str | None]

    def __post_init__(self) -> None:
        object.__setattr__(self, "expected_version_hashes", MappingProxyType(dict(self.expected_version_hashes)))

    @property
    def actionable(self) -> bool:
        return any(item.status in {"new", "updated"} for item in self.diagnostics)


def export_selected_resources(
    store: ResourceCatalogStore,
    *,
    frequency_resource_keys: Sequence[str] = (),
    net_entry_keys: Sequence[str] = (),
    max_items: int = DEFAULT_MAX_ITEMS,
) -> dict[str, Any]:
    """Export selected frequencies and nets with their sessions/frequencies.

    Missing selections are ignored.  The result is a JSON-safe mapping and is
    bounded before it reads catalog children.  Source metadata and version
    hashes are included; credentials and unrelated configuration are not.
    """

    limit = _bounded_limit(max_items)
    frequency_keys = _unique_keys(frequency_resource_keys, limit)
    entry_keys = _unique_keys(net_entry_keys, limit)
    frequencies: dict[str, FrequencyResource] = {}
    entries: dict[str, NetDirectoryEntry] = {}
    sessions: dict[str, NetDirectorySession] = {}
    for key in frequency_keys:
        if resource := store.get_frequency(key):
            frequencies[key] = resource
    for key in entry_keys:
        if entry := store.get_net_entry(key):
            entries[key] = entry
            for session in store.list_sessions(net_entry_key=key, active=None, limit=limit):
                if len(sessions) >= limit:
                    break
                sessions[session.net_session_key] = session
                if session.frequency_resource_key and session.frequency_resource_key not in frequencies:
                    if len(frequencies) < limit and (resource := store.get_frequency(session.frequency_resource_key)):
                        frequencies[resource.frequency_resource_key] = resource
    source_keys = {item.source_key for item in (*frequencies.values(), *entries.values(), *sessions.values())}
    sources = [_source_payload(store.get_source(key)) for key in sorted(source_keys) if store.get_source(key)]
    return {
        "schema_version": TRANSFER_SCHEMA_VERSION,
        "kind": _TRANSFER_KIND,
        "frequencies": [_frequency_payload(item, store) for item in sorted(frequencies.values(), key=lambda item: item.frequency_resource_key)],
        "net_entries": [_entry_payload(item, store) for item in sorted(entries.values(), key=lambda item: item.net_entry_key)],
        "sessions": [_session_payload(item) for item in sorted(sessions.values(), key=lambda item: item.net_session_key)],
        "sources": sources,
    }


def preview_json_import(
    store: ResourceCatalogStore,
    payload: str | bytes | Mapping[str, Any],
    *,
    target_source_key: str = "source_imported_transfer",
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_items: int = DEFAULT_MAX_ITEMS,
) -> ImportPreview:
    """Parse a transfer without mutating the catalog and return immutable diagnostics."""

    document, root_diagnostics = _decode_payload(payload, max_bytes)
    target = CatalogSource(target_source_key, "Imported resource transfer", "imported")
    if root_diagnostics:
        return ImportPreview(target, (), (), (), tuple(root_diagnostics), {})
    limit = _bounded_limit(max_items)
    diagnostics: list[TransferDiagnostic] = []
    if document.get("schema_version") != TRANSFER_SCHEMA_VERSION or document.get("kind") != _TRANSFER_KIND:
        diagnostics.append(_diagnostic("invalid", "document", None, reason="Unsupported transfer schema.", resolution="Export again from a compatible FIO version."))
        return ImportPreview(target, (), (), (), tuple(diagnostics), {})
    if _contains_secret_field(document):
        diagnostics.append(_diagnostic("invalid", "document", None, reason="Transfer contains a credential-like field.", resolution="Remove secrets and export only catalog resources."))
        return ImportPreview(target, (), (), (), tuple(diagnostics), {})
    groups = {name: document.get(name, []) for name in ("frequencies", "net_entries", "sessions")}
    if any(not isinstance(items, list) for items in groups.values()):
        diagnostics.append(_diagnostic("invalid", "document", None, reason="Transfer item collections must be arrays."))
        return ImportPreview(target, (), (), (), tuple(diagnostics), {})
    if sum(len(items) for items in groups.values()) > limit:
        diagnostics.append(_diagnostic("invalid", "document", None, reason=f"Transfer exceeds {limit} item limit.", resolution="Split the export into smaller selections."))
        return ImportPreview(target, (), (), (), tuple(diagnostics), {})

    frequencies, f_expected = _preview_group(store, groups["frequencies"], "frequency", target.source_key, diagnostics)
    entries, e_expected = _preview_group(store, groups["net_entries"], "net_entry", target.source_key, diagnostics)
    sessions, s_expected = _preview_group(store, groups["sessions"], "session", target.source_key, diagnostics)
    frequency_keys = {item.frequency_resource_key for item in frequencies}
    entry_keys = {item.net_entry_key for item in entries}
    for session in sessions:
        if session.net_entry_key not in entry_keys and store.get_net_entry(session.net_entry_key) is None:
            diagnostics.append(_diagnostic("conflict", "session", session.net_session_key, "net_entry_key", session.net_entry_key, "Referenced net entry is unavailable.", "Include the net entry or import it first."))
        if session.frequency_resource_key and session.frequency_resource_key not in frequency_keys and store.get_frequency(session.frequency_resource_key) is None:
            diagnostics.append(_diagnostic("ambiguous", "session", session.net_session_key, "frequency_resource_key", session.frequency_resource_key, "Referenced frequency is unavailable.", "Include the frequency or choose a station resource."))
    return ImportPreview(target, tuple(frequencies), tuple(entries), tuple(sessions), tuple(diagnostics), {**f_expected, **e_expected, **s_expected})


def apply_import_preview(store: ResourceCatalogStore, preview: ImportPreview) -> tuple[TransferDiagnostic, ...]:
    """Apply only a clean preview through public store APIs.

    Existing rows are rechecked against preview version hashes before update so
    a concurrent edit turns into a conflict instead of a silent overwrite.
    """

    results = list(preview.diagnostics)
    blocked = {item.item_key for item in results if item.status in {"invalid", "duplicate", "ambiguous", "conflict"}}
    if not store.get_source(preview.target_source.source_key):
        store.create_source(preview.target_source)
    _apply_group(store, preview.frequencies, "frequency", preview.expected_version_hashes, blocked, results)
    _apply_group(store, preview.net_entries, "net_entry", preview.expected_version_hashes, blocked, results)
    _apply_group(store, preview.sessions, "session", preview.expected_version_hashes, blocked, results)
    return tuple(results)


def _preview_group(store: ResourceCatalogStore, items: list[Any], kind: str, source_key: str, diagnostics: list[TransferDiagnostic]) -> tuple[list[Any], dict[str, str | None]]:
    model_type, key_field, getter = {
        "frequency": (FrequencyResource, "frequency_resource_key", store.get_frequency),
        "net_entry": (NetDirectoryEntry, "net_entry_key", store.get_net_entry),
        "session": (NetDirectorySession, "net_session_key", store.get_session),
    }[kind]
    accepted, expected, seen = [], {}, set()
    for raw in items:
        if not isinstance(raw, Mapping):
            diagnostics.append(_diagnostic("invalid", kind, None, reason="Item must be an object.")); continue
        key = raw.get(key_field)
        if not isinstance(key, str) or not key.strip():
            diagnostics.append(_diagnostic("invalid", kind, None, key_field, key, "Stable key is required.")); continue
        if key in seen:
            diagnostics.append(_diagnostic("duplicate", kind, key, key_field, key, "Duplicate stable key in transfer.", "Keep one version of the item.")); continue
        seen.add(key)
        if str(raw.get("service", "")).strip().upper() not in {"AMATEUR", "GMRS"} and kind != "net_entry":
            diagnostics.append(_diagnostic("ambiguous", kind, key, "service", raw.get("service"), "Service is missing or unsupported.", "Choose Amateur or GMRS.")); continue
        try:
            value = model_type(**_model_fields(raw, model_type, source_key))
        except (CatalogValidationError, TypeError, ValueError) as exc:
            diagnostics.append(_diagnostic("invalid", kind, key, reason=str(exc), resolution="Correct the item and preview again.")); continue
        current = getter(key)
        if current and _is_read_only(store, current.source_key):
            diagnostics.append(_diagnostic("conflict", kind, key, reason="Bundled read-only record cannot be overwritten.", resolution="Create a station override with a new stable key.")); continue
        incoming_hash = raw.get("version_hash")
        current_hash = getattr(current, "version_hash", None) if current else None
        status = "new" if current is None else "unchanged" if incoming_hash and incoming_hash == current_hash else "updated"
        diagnostics.append(_diagnostic(status, kind, key))
        accepted.append(value); expected[key] = current_hash
    return accepted, expected


def _apply_group(store: ResourceCatalogStore, items: Sequence[Any], kind: str, expected: Mapping[str, str | None], blocked: set[str | None], results: list[TransferDiagnostic]) -> None:
    for item in items:
        key = getattr(item, {"frequency": "frequency_resource_key", "net_entry": "net_entry_key", "session": "net_session_key"}[kind])
        if key in blocked:
            continue
        getter, create, update = {
            "frequency": (store.get_frequency, store.create_frequency, store.update_frequency),
            "net_entry": (store.get_net_entry, store.create_net_entry, store.update_net_entry),
            "session": (store.get_session, store.create_session, store.update_session),
        }[kind]
        current = getter(key)
        if current and getattr(current, "version_hash", None) != expected.get(key):
            results.append(_diagnostic("conflict", kind, key, reason="Item changed after preview.", resolution="Preview again before applying.")); continue
        try:
            (update if current else create)(item)
        except (CatalogValidationError, ReadOnlyResourceError, ValueError) as exc:
            results.append(_diagnostic("conflict", kind, key, reason=str(exc), resolution="Resolve references and preview again."))


def _model_fields(raw: Mapping[str, Any], model_type: type, source_key: str) -> dict[str, Any]:
    values = {field.name: raw.get(field.name) for field in dataclasses.fields(model_type) if field.name in raw}
    values["source_key"] = source_key
    for field in ("content_hash", "version_hash", "created_utc", "updated_utc"):
        values.pop(field, None)
    return values


def _frequency_payload(item: FrequencyResource, store: ResourceCatalogStore) -> dict[str, Any]:
    payload = _public_model_payload(item)
    payload["group_links"] = [{"operating_group_key": key, "group_name_snapshot": name} for key, name in store.frequency_group_links(item.frequency_resource_key)]
    return payload


def _entry_payload(item: NetDirectoryEntry, store: ResourceCatalogStore) -> dict[str, Any]:
    payload = _public_model_payload(item)
    payload.pop("contact_info", None)  # public catalog transfer never exports contact/credential material
    payload["group_links"] = [{"operating_group_key": key, "group_name_snapshot": name} for key, name in store.net_entry_group_links(item.net_entry_key)]
    return payload


def _session_payload(item: NetDirectorySession) -> dict[str, Any]: return _public_model_payload(item)
def _source_payload(item: CatalogSource | None) -> dict[str, Any]: return _public_model_payload(item) if item else {}
def _public_model_payload(item: Any) -> dict[str, Any]: return {key: value for key, value in dataclasses.asdict(item).items() if key not in {"contact_info"}}
def _is_read_only(store: ResourceCatalogStore, source_key: str) -> bool: return bool((source := store.get_source(source_key)) and source.read_only)
def _bounded_limit(value: int) -> int: return max(1, min(DEFAULT_MAX_ITEMS, int(value)))
def _unique_keys(values: Sequence[str], limit: int) -> tuple[str, ...]: return tuple(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))[:limit]
def _diagnostic(status: str, item_type: str, item_key: str | None, field: str | None = None, supplied_value: object | None = None, reason: str = "", resolution: str = "") -> TransferDiagnostic: return TransferDiagnostic(status, item_type, item_key, field, supplied_value, reason, resolution)


def _decode_payload(payload: str | bytes | Mapping[str, Any], max_bytes: int) -> tuple[Mapping[str, Any], list[TransferDiagnostic]]:
    try:
        if isinstance(payload, Mapping): return payload, []
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        if not isinstance(raw, bytes) or len(raw) > max_bytes: raise ValueError("Transfer exceeds byte limit.")
        document = json.loads(raw.decode("utf-8"))
        if not isinstance(document, Mapping): raise ValueError("Transfer root must be an object.")
        return document, []
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        return {}, [_diagnostic("invalid", "document", None, reason=str(exc), resolution="Provide a bounded JSON object export.")]


def _contains_secret_field(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(any(marker in str(key).lower() for marker in _SECRET_FIELD_MARKERS) or _contains_secret_field(child) for key, child in value.items())
    if isinstance(value, list): return any(_contains_secret_field(child) for child in value)
    return False
