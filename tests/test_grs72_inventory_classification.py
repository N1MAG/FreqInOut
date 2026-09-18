"""Synthetic production-shaped acceptance tests for the GRS-7.2 inventory core."""

from freqinout.core.guided_instance_inventory import (
    build_guided_instance_inventory,
    distinct_draft_seed,
)


def _js8_row(row_id: int, *, complete: bool = True) -> dict:
    row = {
        "id": row_id,
        "system_key": f"js8-{row_id}",
        "name": f"JS8 {row_id}",
        "host": "127.0.0.1",
        "port": 2442,
        "udp_port": 2237,
    }
    if complete:
        row.update(
            {
                "rig_name": f"RIG-{row_id}",
                "application_path": "/opt/js8call/js8call",
                "configuration_path": f"/srv/fio/js8/{row_id}/profile",
                "storage_path": f"/srv/fio/js8/{row_id}/data",
            }
        )
    return row


def _fast_light_row(row_id: int, *, complete: bool = True) -> dict:
    row = {
        "id": row_id,
        "system_key": f"fast-light-{row_id}",
        "name": f"Fast Light {row_id}",
        "host": "127.0.0.1",
        "flrig_port": 12345,
        "fldigi_port": 7362,
    }
    if complete:
        row.update(
            {
                "flrig_path": "/opt/flrig/flrig",
                "fldigi_path": "/opt/fldigi/fldigi",
                "flrig_profile_path": f"/srv/fio/fl/{row_id}/flrig",
                "fldigi_config_path": f"/srv/fio/fl/{row_id}/fldigi",
                "fldigi_log_path": f"/srv/fio/fl/{row_id}/logs",
                "fldigi_checkin_dir": f"/srv/fio/fl/{row_id}/checkins",
            }
        )
    return row


def _varac_row(row_id: int, *, complete: bool = True) -> dict:
    row = {
        "id": row_id,
        "system_key": f"varac-{row_id}",
        "name": f"VarAC {row_id}",
    }
    if complete:
        row.update(
            {
                "install_path": "/opt/varac/varac",
                "ini_path": f"/srv/fio/varac/{row_id}/VarAC.ini",
                "db_path": f"/srv/fio/varac/{row_id}/VarAC.db",
                "incoming_path": f"/srv/fio/varac/{row_id}/incoming",
            }
        )
    return row


def test_production_shaped_rows_are_classified_without_enabled_inference():
    saved = {
        "js8call": tuple([_js8_row(1)] + [_js8_row(index, complete=False) for index in range(2, 9)]),
        "fast_light": tuple(
            [_fast_light_row(1)] + [_fast_light_row(index, complete=False) for index in range(2, 9)]
        ),
        "varac": (_varac_row(1),),
    }
    snapshot = build_guided_instance_inventory(
        saved,
        linked_ids_by_family={"js8call": {1}, "fast_light": {1}, "varac": {1}},
    )

    for family in saved:
        assert len(snapshot.usable_rows_for(family)) == 1
        assert all(row["candidate_classification"] == "usable_existing" for row in snapshot.usable_rows_for(family))
        assert all(row["candidate_usable"] is True for row in snapshot.usable_rows_for(family))
    assert len(snapshot.diagnostic_rows_for("js8call")) == 7
    assert len(snapshot.diagnostic_rows_for("fast_light")) == 7
    assert snapshot.recovery_rows_for("js8call") == ()
    assert all(row["diagnostic_only"] for row in snapshot.diagnostic_rows_for("js8call"))
    assert all(row["candidate_usable"] is False for row in snapshot.diagnostic_rows_for("fast_light"))
    # Backward-compatible raw inventory access still includes diagnostic rows;
    # recommendation callers must use the classified view.
    assert len(snapshot.rows_for("js8call")) == 8


def test_complete_unlinked_rows_are_recovery_only_not_recommended():
    row = _js8_row(8)
    row["manifest_present"] = True
    snapshot = build_guided_instance_inventory(
        {"js8call": (row,)},
        linked_ids_by_family={"js8call": {1}},
    )
    row = snapshot.rows_for("js8call")[0]
    assert row["candidate_classification"] == "recovery_only"
    assert row["recovery_only"] is True
    assert row["diagnostic_only"] is False
    assert snapshot.usable_rows_for("js8call") == ()
    assert snapshot.recovery_rows_for("js8call") == (row,)


def test_complete_unlinked_row_without_manifest_is_diagnostic_only():
    snapshot = build_guided_instance_inventory(
        {"js8call": (_js8_row(8),)},
        linked_ids_by_family={"js8call": {1}},
    )
    row = snapshot.rows_for("js8call")[0]
    assert row["candidate_classification"] == "diagnostic_only"
    assert row["diagnostic_only"] is True
    assert row["recovery_only"] is False
    assert row["provenance_unverified"] is True
    assert "missing durable ownership/source evidence" in row["completeness_reasons"]
    assert snapshot.recovery_rows_for("js8call") == ()


def test_manifest_evidence_allows_complete_unlinked_recovery_candidate():
    row = _js8_row(8)
    row["manifest_present"] = True
    snapshot = build_guided_instance_inventory(
        {"js8call": (row,)},
        linked_ids_by_family={"js8call": {1}},
    )
    classified = snapshot.rows_for("js8call")[0]
    assert classified["candidate_classification"] == "recovery_only"
    assert classified["recovery_only"] is True
    assert classified["provenance_unverified"] is False


def test_empty_manifests_do_not_change_bounded_classification_or_provenance():
    snapshot = build_guided_instance_inventory(
        {"js8call": (_js8_row(1),)},
        linked_ids_by_family={"js8call": {1}},
    )
    row = snapshot.usable_rows_for("js8call")[0]
    assert row["provenance_unverified"] is True
    assert row["candidate_usable"] is True
    assert snapshot.diagnostic_rows_for("js8call") == ()


def test_duplicate_legacy_claims_are_deduplicated_for_allocation():
    snapshot = build_guided_instance_inventory(
        {"js8call": tuple(_js8_row(index, complete=False) for index in range(1, 9))},
    )
    claims = snapshot.resource_claims_for("js8call")
    assert claims.count(("tcp", "127.0.0.1:2442")) == 1
    assert claims.count(("udp", "127.0.0.1:2237")) == 1
    duplicate_map = {
        (kind, value): count
        for kind, value, count in snapshot.duplicate_resource_claims_for("js8call")
    }
    assert duplicate_map[("tcp", "127.0.0.1:2442")] == 8
    assert duplicate_map[("udp", "127.0.0.1:2237")] == 8
    proposal = distinct_draft_seed(
        "js8call",
        owner_draft_key="new-radio",
        snapshot=snapshot,
    )
    assert proposal["port"] == 2443
    assert proposal["udp_port"] == 2238
    assert proposal["configuration_path"] == ""
    assert proposal["storage_path"] == ""


def test_retained_draft_is_neither_existing_candidate_nor_diagnostic():
    snapshot = build_guided_instance_inventory(
        {"js8call": ()},
        retained_drafts={"js8call": {"draft_instance_key": "draft-js8", "port": 2443}},
    )
    row = snapshot.rows_for("js8call")[0]
    assert row["candidate_classification"] == "retained_draft"
    assert row["retained_draft"] is True
    assert row["candidate_usable"] is False
    assert row["diagnostic_only"] is False
