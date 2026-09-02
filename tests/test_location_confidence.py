from freqinout.core.location_confidence import location_evidence_from_values, prefer_location_evidence


def test_grid6_is_preferred_over_grid4() -> None:
    current = location_evidence_from_values(
        grid="DM79",
        source_family="roster",
        source_ref="K0PRA",
        observed_utc="2026-08-01T00:00:00+00:00",
    )
    candidate = location_evidence_from_values(
        grid="DM79QJ",
        source_family="spotter",
        source_ref="js8spotter:42",
        observed_utc="2026-08-02T00:00:00+00:00",
    )

    preferred = prefer_location_evidence(current, candidate)

    assert preferred == candidate
    assert preferred.location_kind == "grid6"
    assert preferred.legacy_confidence == "grid"


def test_route_derived_does_not_replace_grid_location() -> None:
    current = location_evidence_from_values(grid="DM79QJ", source_family="commstat", source_ref="7")
    candidate = location_evidence_from_values(
        location_kind="route_derived",
        source_family="meshcore",
        source_ref="first-hop:router-1",
    )

    assert prefer_location_evidence(current, candidate) == current


def test_locked_manual_location_is_not_replaced_by_gps() -> None:
    current = location_evidence_from_values(
        grid="DM79",
        location_kind="manual",
        source_family="operator",
        source_ref="N1MAG",
    )
    candidate = location_evidence_from_values(lat=39.5, lon=-104.8, location_kind="gps", source_family="meshcore")

    assert prefer_location_evidence(current, candidate, locked=True) == current


def test_equal_rank_uses_newer_evidence() -> None:
    current = location_evidence_from_values(grid="DM79QJ", observed_utc="2026-08-01T00:00:00+00:00")
    candidate = location_evidence_from_values(grid="DM79AA", observed_utc="2026-08-02T00:00:00+00:00")

    assert prefer_location_evidence(current, candidate) == candidate
