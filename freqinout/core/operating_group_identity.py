"""Qt-free identity helpers for configured Operating Groups.

Operating Groups are currently represented by one Settings row per frequency
and mode.  Their display name is mutable, while the rows belonging to one
logical group are not.  This module supplies the small identity boundary used
by the resource catalog and later schedule code:

* names are normalized in one place;
* old rows receive a deterministic, prefixed UUID5 key;
* an existing non-empty text key is never replaced;
* all rows with the same normalized name receive the same key; and
* choices exposed to a UI are immutable value objects.

There is deliberately no Qt, Settings, database, or persistence dependency in
this module.  Callers receive copies and remain responsible for saving them.
"""

from __future__ import annotations

import copy
import re
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from freqinout.core.group_utils import normalize_group_name as _base_normalize_group_name


# This namespace is part of the compatibility contract.  Do not change it:
# changing it would give every legacy group a new identity on the next load.
OPERATING_GROUP_UUID_NAMESPACE = uuid.UUID("6b7d7e53-0aa4-5d57-9d4a-6cdd4e4b5d8d")
OPERATING_GROUP_KEY_PREFIX = "operating_group_"

# Settings currently uses ``group``.  The other names are accepted because
# resource/import/projection boundaries use slightly different legacy shapes.
_NAME_FIELDS = ("group", "group_name", "operating_group", "name")
_KEY_FIELDS = ("operating_group_key", "group_key")
_ALIAS_FIELDS = ("aliases", "group_aliases", "alias", "previous_group_name", "former_name")


def normalize_logical_group_name(value: object) -> str:
    """Return the canonical logical Operating Group name.

    Group names are case-insensitive and may be entered with the addressing
    marker used elsewhere in FIO.  Internal whitespace is folded as well, so
    accidental spacing does not create two logical identities.  Empty and
    non-string-like values normalize to ``""``.
    """

    try:
        text = str(value or "")
    except Exception:
        return ""
    # Keep the established @ handling (including repeated markers), then fold
    # whitespace before using the result as an identity input.
    text = _base_normalize_group_name(text)
    return re.sub(r"\s+", " ", text).strip()


# Clear aliases make the adapter convenient to discover while retaining the
# existing core helper's familiar name for callers that use this module.
normalize_operating_group_name = normalize_logical_group_name
normalize_group = normalize_logical_group_name
normalize_group_name = normalize_logical_group_name


def is_valid_operating_group_key(value: object) -> bool:
    """Whether *value* is a preservable canonical key.

    Existing deployments may already have a source-specific prefix, so
    validation intentionally requires only an immutable TEXT value: the
    adapter must not rewrite a valid key merely because its prefix predates
    this feature.  Blank strings and non-text values are legacy/missing keys.
    """

    return isinstance(value, str) and bool(value.strip())


def operating_group_key_for_name(name: object) -> str:
    """Return the deterministic canonical key for a legacy group name.

    UUIDv5 makes the result stable across processes and migrations.  The type
    prefix prevents this key from being confused with a bare UUID belonging to
    another canonical object family.
    """

    normalized = normalize_logical_group_name(name)
    if not normalized:
        return ""
    value = uuid.uuid5(OPERATING_GROUP_UUID_NAMESPACE, normalized)
    return f"{OPERATING_GROUP_KEY_PREFIX}{value}"


# Common verb-oriented aliases used by migration code.
deterministic_operating_group_key = operating_group_key_for_name
legacy_operating_group_key = operating_group_key_for_name
operating_group_key = operating_group_key_for_name


def _row_name(row: Mapping[str, Any]) -> str:
    for field in _NAME_FIELDS:
        value = row.get(field)
        normalized = normalize_logical_group_name(value)
        if normalized:
            return normalized
    return ""


def _row_key(row: Mapping[str, Any]) -> str:
    for field in _KEY_FIELDS:
        value = row.get(field)
        if is_valid_operating_group_key(value):
            return str(value).strip()
    return ""


def _as_alias_values(row: Mapping[str, Any]) -> list[str]:
    """Read aliases from compatible legacy shapes without changing *row*."""

    aliases: list[str] = []
    for field in _ALIAS_FIELDS:
        value = row.get(field)
        if isinstance(value, str):
            values: Iterable[object] = (value,)
        elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, Mapping)):
            values = value
        else:
            values = ()
        for candidate in values:
            alias = normalize_logical_group_name(candidate)
            if alias and alias not in aliases:
                aliases.append(alias)
    return aliases


def _copy_row(row: Mapping[str, Any]) -> dict[str, Any]:
    # Deep-copying means a caller can safely edit nested metadata in the
    # returned draft without mutating the original Settings payload either.
    return copy.deepcopy(dict(row))


def ensure_operating_group_keys(rows: Iterable[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    """Return copied rows with shared, stable Operating Group keys.

    Rows are retained in their original order and no row is removed.  For a
    normalized name with multiple rows, the first valid existing key wins;
    otherwise the deterministic UUID5 key is used.  Thus repeated calls are
    idempotent, and duplicate band/mode rows become one logical identity while
    preserving their per-row configuration.
    """

    raw_rows = tuple(rows or ())
    copied: list[dict[str, Any]] = []
    keys_by_name: dict[str, str] = {}

    # Discover existing keys before generating any fallback.  A later Settings
    # variant may carry the key while the first (same-name) variant predates
    # the additive migration; it must win for every row in that logical group.
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            continue
        name = _row_name(raw)
        existing_key = _row_key(raw)
        if name and existing_key and name not in keys_by_name:
            keys_by_name[name] = existing_key

    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            # Settings rows are dictionaries.  Ignore malformed non-mapping
            # values rather than raising while loading a legacy installation.
            continue
        row = _copy_row(raw)
        name = _row_name(row)
        if not name:
            # Preserve malformed mapping rows losslessly, but there is no
            # logical identity to assign until a caller repairs the name.
            copied.append(row)
            continue
        key = keys_by_name.get(name)
        if not key:
            key = operating_group_key_for_name(name)
            keys_by_name[name] = key
        # Canonical output uses the normalized logical name while retaining
        # every other setting exactly as supplied.
        name_field = next((field for field in _NAME_FIELDS if normalize_logical_group_name(row.get(field))), "group")
        row[name_field] = name
        row["operating_group_key"] = key
        copied.append(row)
    return copied


# Alternative names describe the same additive operation and help callers
# avoid importing a migration-specific term for a read/normalize operation.
normalize_operating_group_rows = ensure_operating_group_keys
assign_operating_group_keys = ensure_operating_group_keys
add_operating_group_keys = ensure_operating_group_keys


def operating_group_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Explicitly named convenience wrapper for normalized Settings rows."""

    return ensure_operating_group_keys(rows)


@dataclass(frozen=True, slots=True)
class OperatingGroupOption:
    """Immutable selection model for a logical Operating Group."""

    operating_group_key: str
    group_name: str
    aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "operating_group_key", str(self.operating_group_key or "").strip())
        object.__setattr__(self, "group_name", normalize_logical_group_name(self.group_name))
        raw_aliases: Iterable[object]
        if isinstance(self.aliases, str):
            raw_aliases = (self.aliases,)
        else:
            raw_aliases = self.aliases or ()
        object.__setattr__(
            self,
            "aliases",
            tuple(
                alias
                for alias in (normalize_logical_group_name(value) for value in raw_aliases)
                if alias and alias != self.group_name
            ),
        )

    @property
    def key(self) -> str:
        return self.operating_group_key

    @property
    def name(self) -> str:
        return self.group_name

    @property
    def label(self) -> str:
        return self.group_name

    @property
    def group_name_snapshot(self) -> str:
        return self.group_name


# Friendly compatibility aliases for UI/model naming conventions.
OperatingGroupChoice = OperatingGroupOption
OperatingGroupSelectOption = OperatingGroupOption


def build_operating_group_options(rows: Iterable[Mapping[str, Any]] | None) -> tuple[OperatingGroupOption, ...]:
    """Build deterministic, de-duplicated immutable choices from *rows*."""

    normalized_rows = ensure_operating_group_keys(rows)
    by_key: dict[str, tuple[str, list[str]]] = {}
    for row in normalized_rows:
        name = _row_name(row)
        key = _row_key(row)
        if not name or not key:
            continue
        current = by_key.get(key)
        if current is None:
            by_key[key] = (name, _as_alias_values(row))
            continue
        canonical_name, aliases = current
        for alias in _as_alias_values(row):
            if alias not in aliases and alias != canonical_name:
                aliases.append(alias)

    options = [
        OperatingGroupOption(key, name, tuple(alias for alias in aliases if alias != name))
        for key, (name, aliases) in by_key.items()
    ]
    return tuple(sorted(options, key=lambda option: (option.group_name.casefold(), option.operating_group_key)))


operating_group_options = build_operating_group_options
build_operating_group_select_options = build_operating_group_options


def find_operating_group_by_key(
    rows: Iterable[Mapping[str, Any]], key: object
) -> dict[str, Any] | None:
    """Return a copied normalized row matching *key*, or ``None``."""

    wanted = str(key or "").strip()
    if not wanted:
        return None
    for row in ensure_operating_group_keys(rows):
        if _row_key(row) == wanted:
            return row
    return None


def find_operating_group_by_name(
    rows: Iterable[Mapping[str, Any]], name: object
) -> dict[str, Any] | None:
    """Return the first copied normalized row matching logical *name*."""

    wanted = normalize_logical_group_name(name)
    if not wanted:
        return None
    for row in ensure_operating_group_keys(rows):
        if _row_name(row) == wanted:
            return row
    return None


lookup_operating_group_by_key = find_operating_group_by_key
lookup_operating_group_by_name = find_operating_group_by_name


def rename_operating_group(
    row: Mapping[str, Any],
    new_name: object,
    *,
    alias: object | None = None,
) -> dict[str, Any]:
    """Return a renamed row while preserving its key and optional old alias.

    A missing legacy key is assigned from the pre-rename name, so rename is
    safe to use before the first additive migration.  The original mapping is
    never changed.  ``aliases`` is the canonical collection; ``alias`` is also
    retained as a convenience scalar for legacy consumers that use that shape.
    """

    if not isinstance(row, Mapping):
        raise TypeError("Operating Group row must be a mapping")
    result = _copy_row(row)
    old_name = _row_name(result)
    key = _row_key(result) or operating_group_key_for_name(old_name)
    renamed = normalize_logical_group_name(new_name)
    if not renamed:
        raise ValueError("Operating Group name cannot be empty")

    # Settings uses ``group``; preserve an explicitly used alternate shape,
    # while always adding the canonical key field.
    name_field = next((field for field in _NAME_FIELDS if field in result), "group")
    result[name_field] = renamed
    result["operating_group_key"] = key

    aliases = _as_alias_values(result)
    supplied_alias = normalize_logical_group_name(alias)
    if supplied_alias and supplied_alias not in aliases and supplied_alias != renamed:
        aliases.append(supplied_alias)
    if old_name and old_name != renamed and old_name not in aliases:
        aliases.append(old_name)
    if aliases:
        result["aliases"] = tuple(aliases)
        # Retaining a scalar alias helps old label-oriented consumers without
        # making aliases the relationship identity.
        result["alias"] = supplied_alias or (old_name if old_name != renamed else aliases[-1])
    return result


def rename_operating_groups(
    rows: Iterable[Mapping[str, Any]],
    old_name: object,
    new_name: object,
    *,
    alias: object | None = None,
) -> list[dict[str, Any]]:
    """Rename all rows in one logical group, retaining one shared key."""

    wanted = normalize_logical_group_name(old_name)
    if not wanted:
        return ensure_operating_group_keys(rows)
    source = ensure_operating_group_keys(rows)
    renamed_rows: list[dict[str, Any]] = []
    for row in source:
        if _row_name(row) == wanted:
            renamed_rows.append(rename_operating_group(row, new_name, alias=alias))
        else:
            renamed_rows.append(_copy_row(row))
    return ensure_operating_group_keys(renamed_rows)


# Explicit aliases for callers that name the operation after its row shape.
rename_operating_group_rows = rename_operating_groups
normalize_operating_groups = ensure_operating_group_keys
ensure_operating_group_identity = ensure_operating_group_keys


class OperatingGroupIdentityAdapter:
    """Small stateless/facade API for identity operations.

    ``rows`` is optional.  Supplying it makes lookup and option access handy;
    every returned object is rebuilt from copies so the adapter never exposes
    mutable caller-owned Settings data.
    """

    def __init__(self, rows: Iterable[Mapping[str, Any]] | None = None) -> None:
        self._rows = ensure_operating_group_keys(rows or ())

    @staticmethod
    def normalize(value: object) -> str:
        return normalize_logical_group_name(value)

    @staticmethod
    def key_for_name(value: object) -> str:
        return operating_group_key_for_name(value)

    @staticmethod
    def ensure_keys(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return ensure_operating_group_keys(rows)

    @property
    def rows(self) -> list[dict[str, Any]]:
        return [_copy_row(row) for row in self._rows]

    @property
    def options(self) -> tuple[OperatingGroupOption, ...]:
        return build_operating_group_options(self._rows)

    def by_key(self, key: object) -> dict[str, Any] | None:
        return find_operating_group_by_key(self._rows, key)

    def by_name(self, name: object) -> dict[str, Any] | None:
        return find_operating_group_by_name(self._rows, name)

    @staticmethod
    def rename(row: Mapping[str, Any], new_name: object, *, alias: object | None = None) -> dict[str, Any]:
        return rename_operating_group(row, new_name, alias=alias)

    @staticmethod
    def rename_rows(
        rows: Iterable[Mapping[str, Any]], old_name: object, new_name: object, *, alias: object | None = None
    ) -> list[dict[str, Any]]:
        return rename_operating_groups(rows, old_name, new_name, alias=alias)


__all__ = [
    "OPERATING_GROUP_KEY_PREFIX",
    "OPERATING_GROUP_UUID_NAMESPACE",
    "OperatingGroupChoice",
    "OperatingGroupIdentityAdapter",
    "OperatingGroupOption",
    "OperatingGroupSelectOption",
    "add_operating_group_keys",
    "assign_operating_group_keys",
    "build_operating_group_options",
    "build_operating_group_select_options",
    "deterministic_operating_group_key",
    "ensure_operating_group_keys",
    "find_operating_group_by_key",
    "find_operating_group_by_name",
    "is_valid_operating_group_key",
    "legacy_operating_group_key",
    "lookup_operating_group_by_key",
    "lookup_operating_group_by_name",
    "normalize_group",
    "normalize_group_name",
    "normalize_logical_group_name",
    "normalize_operating_group_name",
    "normalize_operating_group_rows",
    "normalize_operating_groups",
    "operating_group_key",
    "operating_group_key_for_name",
    "operating_group_options",
    "operating_group_rows",
    "rename_operating_group",
    "rename_operating_group_rows",
    "rename_operating_groups",
]
