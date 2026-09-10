from __future__ import annotations

from dataclasses import FrozenInstanceError
from uuid import UUID

import pytest

from freqinout.core.operating_group_identity import (
    OPERATING_GROUP_KEY_PREFIX,
    OperatingGroupIdentityAdapter,
    OperatingGroupOption,
    build_operating_group_options,
    ensure_operating_group_keys,
    find_operating_group_by_key,
    find_operating_group_by_name,
    normalize_logical_group_name,
    operating_group_key_for_name,
    rename_operating_group,
    rename_operating_groups,
)


def test_logical_name_normalization_is_case_marker_and_whitespace_insensitive() -> None:
    assert normalize_logical_group_name("  @  Group   Alpha ") == "GROUP ALPHA"
    assert normalize_logical_group_name(None) == ""
    assert normalize_logical_group_name("@@") == ""


def test_legacy_key_is_prefixed_uuid5_and_repeatable() -> None:
    first = operating_group_key_for_name("Group Alpha")
    second = operating_group_key_for_name(" @group   alpha ")
    assert first == second
    assert first.startswith(OPERATING_GROUP_KEY_PREFIX)
    UUID(first.removeprefix(OPERATING_GROUP_KEY_PREFIX), version=5)


def test_keys_are_shared_per_logical_group_existing_key_wins_and_input_is_unchanged() -> None:
    rows = [
        {"group": "  @Group Alpha ", "band": "2M", "metadata": {"x": [1]}},
        {"group": "GROUP   ALPHA", "band": "70CM", "operating_group_key": "existing:alpha"},
        {"group": "Group Bravo", "band": "2M"},
    ]
    before = [dict(row) for row in rows]
    normalized = ensure_operating_group_keys(rows)

    assert normalized[0]["operating_group_key"] == "existing:alpha"
    assert normalized[1]["operating_group_key"] == "existing:alpha"
    assert normalized[2]["operating_group_key"] == operating_group_key_for_name("GROUP BRAVO")
    assert rows == before
    normalized[0]["metadata"]["x"].append(2)
    assert rows[0]["metadata"] == {"x": [1]}
    assert ensure_operating_group_keys(normalized) == normalized


def test_options_are_unique_immutable_and_keep_aliases() -> None:
    rows = [
        {"group": "Group Alpha", "operating_group_key": "og:alpha"},
        {"group": "Group Alpha", "operating_group_key": "og:alpha", "aliases": ["Old Alpha"]},
        {"group": "Group Bravo"},
    ]
    options = build_operating_group_options(rows)
    alpha = next(option for option in options if option.name == "GROUP ALPHA")
    assert alpha.key == "og:alpha"
    assert alpha.group_name_snapshot == "GROUP ALPHA"
    assert alpha.aliases == ("OLD ALPHA",)
    with pytest.raises(FrozenInstanceError):
        alpha.group_name = "OTHER"  # type: ignore[misc]
    with pytest.raises((FrozenInstanceError, TypeError)):
        alpha.aliases += ("X",)  # type: ignore[misc]


def test_rename_preserves_key_and_records_old_and_optional_alias_without_mutation() -> None:
    row = {"group": "Group Alpha", "band": "2M", "operating_group_key": "existing:alpha"}
    renamed = rename_operating_group(row, "Group Delta", alias="Former Alpha")
    assert renamed["group"] == "GROUP DELTA"
    assert renamed["operating_group_key"] == "existing:alpha"
    assert renamed["aliases"] == ("FORMER ALPHA", "GROUP ALPHA")
    assert renamed["alias"] == "FORMER ALPHA"
    assert row == {"group": "Group Alpha", "band": "2M", "operating_group_key": "existing:alpha"}


def test_collection_rename_preserves_one_key_for_every_variant_and_is_idempotent() -> None:
    rows = [
        {"group": "Group Alpha", "band": "2M"},
        {"group": "@GROUP ALPHA", "band": "70CM"},
        {"group": "Group Bravo", "band": "2M"},
    ]
    renamed = rename_operating_groups(rows, "group alpha", "Group Delta", alias="Alpha Alias")
    delta = [row for row in renamed if row["group"] == "GROUP DELTA"]
    assert len(delta) == 2
    assert len({row["operating_group_key"] for row in delta}) == 1
    assert delta[0]["operating_group_key"] == operating_group_key_for_name("GROUP ALPHA")
    assert ensure_operating_group_keys(renamed) == renamed


def test_lookup_returns_copies_and_adapter_facade_exposes_options() -> None:
    rows = [{"group": "Group Alpha", "band": "2M"}]
    key = operating_group_key_for_name("GROUP ALPHA")
    found = find_operating_group_by_key(rows, key)
    assert found is not None and found["group"] == "GROUP ALPHA"
    found["group"] = "CHANGED"
    assert find_operating_group_by_name(rows, "group alpha")["group"] == "GROUP ALPHA"  # type: ignore[index]

    adapter = OperatingGroupIdentityAdapter(rows)
    assert adapter.by_key(key)["operating_group_key"] == key  # type: ignore[index]
    assert adapter.options == (OperatingGroupOption(key, "GROUP ALPHA"),)
    exposed = adapter.rows
    exposed[0]["group"] = "CHANGED"
    assert adapter.by_name("GROUP ALPHA")["group"] == "GROUP ALPHA"  # type: ignore[index]
