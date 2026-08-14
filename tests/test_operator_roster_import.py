from __future__ import annotations

import io
import json
import sqlite3

from freqinout.core.checkins_db import ensure_operator_checkins_schema, upsert_operator_metadata
from freqinout.core.operator_roster_import import (
    detect_roster_headers,
    infer_parent_group_from_path,
    parse_operator_roster_csv,
)


def test_magnet_roster_import_detects_parent_child_membership_and_metadata() -> None:
    csv_text = (
        "TimeZone,Region,Callsign,Name,Role,Tier,State/Province,GRID6,TG Handle,Alt Contact\n"
        "Eastern,MR01,KC1VXQ,Mark,HUB,4,NH,FN43IA,Fred Flintstone,\n"
        "Eastern,MR01,KC1NPD,Chris,Alt-Hub,3,MA,FN42LQ,C.B.,\n"
    )

    result = parse_operator_roster_csv(
        io.StringIO(csv_text),
        parent_group="MAGNET",
        source_path="/tmp/MAGNET Roster 07-31-26 - Current.csv",
        imported_at_utc="20260731",
    )

    assert result.imported == 2
    assert result.skipped == 0
    assert result.parent_group == "MAGNET"
    assert result.child_groups == ["MR01"]
    assert result.source_headers == [
        "TimeZone",
        "Region",
        "Callsign",
        "Name",
        "Role",
        "Tier",
        "State/Province",
        "GRID6",
        "TG Handle",
        "Alt Contact",
    ]
    assert "TG Handle" not in result.detected_headers.values()
    assert "Alt Contact" not in result.detected_headers.values()
    first = result.entries[0]
    assert first["callsign"] == "KC1VXQ"
    assert first["groups_json"] == ["MAGNET", "MR01"]
    assert first["group_role"] == "HUB"
    assert first["timezone"] == "Eastern"
    assert first["tier"] == "4"
    assert first["state"] == "NH"
    assert first["grid"] == "FN43IA"
    assert first["roster_parent_group"] == "MAGNET"
    assert first["roster_region"] == "MR01"
    assert result.entries[1]["group_role"] == "HUB-ALT"


def test_roster_import_supports_header_aliases_and_filename_parent() -> None:
    csv_text = "Call,Operator,Group,State,Grid,TZ,Level\nN0CALL,Test Op,OPS,CO,DM79QJ,Mountain,2\n"

    detected = detect_roster_headers(["Call", "Operator", "Group", "State", "Grid", "TZ", "Level"])
    result = parse_operator_roster_csv(
        io.StringIO(csv_text),
        source_path="/tmp/AMRRON Roster 2026.csv",
        imported_at_utc="20260811",
    )

    assert detected["callsign"] == "Call"
    assert infer_parent_group_from_path("/tmp/AMRRON Roster 2026.csv") == "AMRRON"
    assert result.parent_group == "AMRRON"
    assert result.entries[0]["groups_json"] == ["AMRRON", "OPS"]
    assert result.entries[0]["timezone"] == "Mountain"
    assert result.entries[0]["tier"] == "2"


def test_roster_metadata_persists_to_operator_checkins() -> None:
    conn = sqlite3.connect(":memory:")
    ensure_operator_checkins_schema(conn)
    result = parse_operator_roster_csv(
        io.StringIO(
            "TimeZone,Region,Callsign,Name,Role,Tier,State/Province,GRID6\n"
            "Eastern,MR08,K7ETC,Scott,NCS,3,UT,DM38ST\n"
        ),
        parent_group="MAGNET",
        imported_at_utc="20260811",
    )

    upsert_operator_metadata(result.entries, conn=conn)
    row = conn.execute(
        """
        SELECT callsign, name, state, grid, group1, group2, groups_json,
               group_role, timezone, tier, roster_parent_group, roster_region, trusted
        FROM operator_checkins
        WHERE callsign='K7ETC'
        """
    ).fetchone()

    assert row[:6] == ("K7ETC", "Scott", "UT", "DM38ST", "MAGNET", "MR08")
    assert json.loads(row[6]) == ["MAGNET", "MR08"]
    assert row[7:] == ("NCS", "Eastern", "3", "MAGNET", "MR08", 1)
