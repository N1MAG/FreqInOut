from freqinout.core.guided_instance_inventory import (
    build_guided_instance_inventory,
    distinct_draft_seed,
    source_identity_fingerprint,
    stable_draft_instance_key,
)


def test_stable_draft_key_ignores_editable_names():
    assert stable_draft_instance_key("transaction-1", "js8call") == stable_draft_instance_key(
        "transaction-1", "js8call"
    )
    assert stable_draft_instance_key("transaction-1", "js8call") != stable_draft_instance_key(
        "transaction-1", "fast_light"
    )


def test_inventory_includes_retained_drafts_and_allocates_around_them():
    snapshot = build_guided_instance_inventory(
        {"js8call": ({"id": 1, "system_key": "saved", "host": "127.0.0.1", "port": 2442},)},
        retained_drafts={"js8call": {"draft_instance_key": "draft-js8", "host": "127.0.0.1", "port": 2443}},
        generation=4,
    )
    proposal = distinct_draft_seed(
        "js8call",
        owner_draft_key="transaction-2",
        snapshot=snapshot,
    )
    assert snapshot.generation == 4
    assert proposal["port"] == 2444
    assert proposal["inventory_fingerprint"] == snapshot.fingerprint


def test_distinct_seed_reuses_only_safe_application_evidence():
    source = {
        "id": 9,
        "system_key": "existing",
        "application_path": "/opt/js8call",
        "variant_family": "stock",
        "variant_version": "2.2.0",
        "host": "127.0.0.1",
        "port": 2442,
        "udp_port": 2237,
        "rig_name": "OLD",
        "configuration_path": "/old/profile",
        "storage_path": "/old/data",
        "launch_command": "old --profile /old/profile",
    }
    snapshot = build_guided_instance_inventory({"js8call": (source,)})
    proposal = distinct_draft_seed(
        "js8call",
        owner_draft_key="transaction-3",
        snapshot=snapshot,
        source=source,
    )
    assert proposal["application_path"] == "/opt/js8call"
    assert proposal["variant"] == "stock"
    assert proposal["configuration_path"] == ""
    assert proposal["storage_path"] == ""
    assert proposal["rig_name"] == ""
    assert proposal["launch_command"] == ""
    assert proposal["port"] == 2443
    assert proposal["imported_id"] is None


def test_imported_source_fingerprint_detects_identity_change():
    original = {
        "id": 5,
        "system_key": "north",
        "host": "127.0.0.1",
        "port": 2442,
        "profile_path": "/profiles/north",
        "application_data_root": "/data/north",
    }
    snapshot = build_guided_instance_inventory({"js8call": (original,)})
    fingerprint = source_identity_fingerprint("js8call", original)
    assert snapshot.source_is_current("js8call", 5, fingerprint)
    changed = {**original, "port": 2443}
    changed_snapshot = build_guided_instance_inventory({"js8call": (changed,)})
    assert not changed_snapshot.source_is_current("js8call", 5, fingerprint)
