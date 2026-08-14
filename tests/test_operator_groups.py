from __future__ import annotations

import json
import sqlite3

from freqinout.core.checkins_db import ensure_operator_checkins_schema, upsert_operator_metadata
from freqinout.core.operator_groups import (
    build_operator_group_families,
    expand_group_selection,
    group_access_summary,
    group_family_label,
    load_operator_group_families,
    trusted_callsigns_for_groups,
    trusted_operator_details_for_groups,
)


def test_operator_group_families_expand_parent_to_roster_regions() -> None:
    families = build_operator_group_families(
        [
            {
                "callsign": "K7ETC",
                "groups_json": json.dumps(["MAGNET", "MR08"]),
                "trusted": 1,
                "roster_parent_group": "MAGNET",
                "roster_region": "MR08",
            },
            {
                "callsign": "KC1VXQ",
                "groups_json": json.dumps(["MAGNET", "MR01"]),
                "trusted": 1,
                "roster_parent_group": "MAGNET",
                "roster_region": "MR01",
            },
        ]
    )

    assert set(families["MAGNET"].members) == {"MAGNET", "MR01", "MR08"}
    assert expand_group_selection(["MAGNET"], families) == {"MAGNET", "MR01", "MR08"}
    assert group_family_label("MAGNET", families) == "MAGNET - family - 2 subgroups - 2 operators"


def test_operator_group_family_trusted_callsigns_are_group_scoped() -> None:
    families = build_operator_group_families(
        [
            {
                "callsign": "K7ETC",
                "groups_json": json.dumps(["MAGNET", "MR08"]),
                "trusted": 1,
                "roster_parent_group": "MAGNET",
                "roster_region": "MR08",
                "group_role": "NCS",
                "tier": "3",
            },
            {
                "callsign": "N0BAD",
                "groups_json": json.dumps(["MAGNET", "MR08"]),
                "trusted": 0,
                "roster_parent_group": "MAGNET",
                "roster_region": "MR08",
                "group_role": "HUB",
                "tier": "4",
            },
            {
                "callsign": "K1AMR",
                "groups_json": json.dumps(["AMRRON"]),
                "trusted": 1,
                "roster_parent_group": "AMRRON",
                "roster_region": "",
                "group_role": "HUB",
                "tier": "4",
            },
            {
                "callsign": "KC1VXQ",
                "groups_json": json.dumps(["MAGNET", "MR01"]),
                "trusted": 1,
                "roster_parent_group": "MAGNET",
                "roster_region": "MR01",
                "group_role": "HUB",
                "tier": "4",
            },
        ]
    )

    assert trusted_callsigns_for_groups(["MAGNET"], families) == ("K7ETC", "KC1VXQ")
    assert trusted_callsigns_for_groups(["MR08"], families) == ("K7ETC",)
    assert trusted_callsigns_for_groups(["MR01"], families) == ("KC1VXQ",)
    assert trusted_callsigns_for_groups(["AMRRON"], families) == ("K1AMR",)
    assert trusted_operator_details_for_groups(["MR08"], families) == (("K7ETC", "NCS", "3", ""),)
    assert trusted_operator_details_for_groups(["MR01"], families) == (("KC1VXQ", "HUB", "4", ""),)
    assert group_access_summary(["MR08"], families) == "Roles: NCS 1; Tiers: 3 1"
    assert group_access_summary(["MAGNET"], families) == "Roles: HUB 1, NCS 1; Tiers: 3 1, 4 1"


def test_load_operator_group_families_uses_operator_checkins_schema(tmp_path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    conn = sqlite3.connect(db_path)
    ensure_operator_checkins_schema(conn)
    upsert_operator_metadata(
        [
            {
                "callsign": "K7ETC",
                "groups_json": ["MAGNET", "MR08"],
                "group1": "MAGNET",
                "group2": "MR08",
                "trusted": 1,
                "roster_parent_group": "MAGNET",
                "roster_region": "MR08",
                "group_role": "NCS",
                "tier": "3",
                "state": "UT",
            }
        ],
        conn=conn,
    )
    conn.commit()
    conn.close()

    families = load_operator_group_families(db_path)

    assert set(families["MAGNET"].members) == {"MAGNET", "MR08"}
    assert families["MAGNET"].trusted_callsigns == ("K7ETC",)
    assert trusted_operator_details_for_groups(["MR08"], families) == (("K7ETC", "NCS", "3", "UT"),)
    assert group_access_summary(["MR08"], families) == "Roles: NCS 1; Tiers: 3 1"
