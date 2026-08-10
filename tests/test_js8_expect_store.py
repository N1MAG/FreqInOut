from pathlib import Path

from freqinout.core.js8_expect_store import (
    delete_expect_entry,
    delete_expect_allow_policy,
    evaluate_expect_request,
    list_expect_management_audit,
    list_expect_entries,
    list_expect_allow_policies,
    list_expect_runtime_audit,
    update_expect_entry_controls,
    save_expect_allow_policy,
    save_expect_entry,
)


def test_save_expect_entry_creates_disabled_draft(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    result = save_expect_entry(
        {
            "source_radio_id": "7",
            "js8_instance_id": "2",
            "expect_key": "F!103",
            "response_text": "@MAGNET F!103 ABC",
            "allowed_groups": ["@MAGNET", "@MAGNET"],
            "enabled": False,
            "auto_reply_enabled": False,
        },
        db_path=db_path,
    )

    assert result.created is True
    assert result.enabled is False
    assert result.auto_reply_enabled is False


def test_save_expect_entry_updates_same_radio_and_instance(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    first = save_expect_entry(
        {
            "source_radio_id": "7",
            "js8_instance_id": "2",
            "expect_key": "F!103",
            "response_text": "@MAGNET F!103 ABC",
        },
        db_path=db_path,
    )
    second = save_expect_entry(
        {
            "source_radio_id": "7",
            "js8_instance_id": "2",
            "expect_key": "f!103",
            "response_text": "@MAGNET F!103 DEF",
        },
        db_path=db_path,
    )

    assert second.created is False
    assert second.id == first.id


def test_expect_allow_policy_round_trips_bulk_lists(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    result = save_expect_allow_policy(
        {
            "name": "MAGNET trusted",
            "allowed_groups": "@MAGNET, @MAGNET",
            "allowed_callsigns": ["N0CALL", "K1ABC"],
            "blocked_callsigns": "BAD1",
            "source_scope": "radio",
        },
        db_path=db_path,
    )

    rows = list_expect_allow_policies(db_path=db_path)

    assert result.created is True
    assert len(rows) == 1
    assert rows[0]["allowed_groups"] == ["@MAGNET"]
    assert rows[0]["allowed_callsigns"] == ["N0CALL", "K1ABC"]
    assert rows[0]["blocked_callsigns"] == ["BAD1"]
    assert rows[0]["source_scope"] == "radio"


def test_expect_allow_policy_updates_and_deletes(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    first = save_expect_allow_policy({"name": "Policy", "allowed_callsigns": "A"}, db_path=db_path)
    second = save_expect_allow_policy(
        {"id": first.id, "name": "Policy Renamed", "allowed_callsigns": "B", "enabled": False},
        db_path=db_path,
    )

    rows = list_expect_allow_policies(db_path=db_path, enabled_only=False)
    assert second.created is False
    assert second.id == first.id
    assert rows[0]["name"] == "Policy Renamed"
    assert rows[0]["enabled"] == 0
    assert list_expect_allow_policies(db_path=db_path, enabled_only=True) == []
    assert delete_expect_allow_policy(first.id, db_path=db_path) is True
    assert list_expect_allow_policies(db_path=db_path) == []


def test_expect_entries_can_be_listed_updated_and_deleted(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    policy = save_expect_allow_policy(
        {
            "name": "MAGNET Net Control",
            "allowed_groups": ["MAGNET"],
            "allowed_callsigns": ["N0CALL"],
        },
        db_path=db_path,
    )
    saved = save_expect_entry(
        {
            "source_radio_id": "7",
            "js8_instance_id": "fio-a",
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 11111111",
            "msg_auth_sign_enabled": True,
            "msg_auth_sign_callsign": "N1MAG",
            "msg_auth_include_datecode": True,
            "msg_auth_datecode": "#HHJL",
            "allowed_groups": ["MAGNET"],
            "enabled": False,
            "auto_reply_enabled": False,
        },
        db_path=db_path,
    )

    rows = list_expect_entries(db_path=db_path)
    assert len(rows) == 1
    assert rows[0]["expect_key"] == "F!304"
    assert rows[0]["allowed_groups"] == ["MAGNET"]
    assert rows[0]["msg_auth_sign_enabled"] == 1
    assert rows[0]["msg_auth_sign_callsign"] == "N1MAG"
    assert rows[0]["msg_auth_include_datecode"] == 1
    assert rows[0]["msg_auth_datecode"] == "#HHJL"

    result = update_expect_entry_controls(
        saved.id,
        {
            "allow_policy_id": policy.id,
            "allowed_groups": ["MAGNET", "GHOSTNET"],
            "allowed_callsigns": ["N0CALL"],
            "blocked_callsigns": ["BAD1"],
            "max_replies": 3,
            "cooldown_seconds": 900,
            "enabled": True,
            "auto_reply_enabled": False,
        },
        db_path=db_path,
    )
    assert result.enabled is True
    updated = list_expect_entries(db_path=db_path)[0]
    assert updated["allow_policy_id"] == policy.id
    assert updated["allow_policy_name"] == "MAGNET Net Control"
    assert updated["allowed_groups"] == ["MAGNET", "GHOSTNET"]
    assert updated["msg_auth_datecode"] == "#HHJL"
    assert updated["max_replies"] == 3
    assert updated["cooldown_seconds"] == 900

    assert delete_expect_entry(saved.id, db_path=db_path) is True
    assert list_expect_entries(db_path=db_path) == []
    audit = list_expect_management_audit(db_path=db_path)
    assert [row["action"] for row in audit[:3]] == ["deleted", "controls-updated", "created"]
    assert audit[0]["expect_key"] == "F!304"
    assert audit[1]["enabled"] == 1
    assert audit[2]["source_radio_id"] == "7"


def test_imported_expect_entry_is_marked_in_management_audit(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    save_expect_entry(
        {
            "source_radio_id": "9",
            "source_scope": "radio",
            "js8_instance_id": "fio-b",
            "expect_key": "F!900",
            "response_text": "@MAGNET F!900 OK",
            "enabled": True,
            "auto_reply_enabled": True,
            "import_source": "js8spotter-db-import",
        },
        db_path=db_path,
    )

    audit = list_expect_management_audit(db_path=db_path)
    assert len(audit) == 1
    assert audit[0]["action"] == "imported"
    assert audit[0]["expect_key"] == "F!900"
    assert audit[0]["source_radio_id"] == "9"
    assert audit[0]["auto_reply_enabled"] == 1


def test_expect_evaluator_matches_source_and_allowed_group(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    save_expect_entry(
        {
            "source_radio_id": "7",
            "source_scope": "radio",
            "js8_instance_id": "fio-a",
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 OK",
            "allowed_groups": ["@MAGNET"],
            "enabled": True,
            "auto_reply_enabled": True,
        },
        db_path=db_path,
    )

    result = evaluate_expect_request(
        expect_key="f!304",
        requesting_callsign="n0call",
        target_group="@MAGNET",
        source_radio_id="7",
        js8_instance_id="fio-a",
        event_id="evt-1",
        db_path=db_path,
    )
    audit = list_expect_runtime_audit(db_path=db_path)

    assert result.decision == "reply-ready"
    assert result.response_text == "@MAGNET F!304 OK"
    assert result.reply_radio_id == "7"
    assert audit[0]["event_id"] == "evt-1"
    assert audit[0]["decision"] == "reply-ready"


def test_expect_evaluator_blocks_source_and_callsign_mismatches(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    save_expect_entry(
        {
            "source_radio_id": "7",
            "source_scope": "radio",
            "js8_instance_id": "fio-a",
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 OK",
            "allowed_callsigns": ["N0CALL"],
            "blocked_callsigns": ["BAD1"],
            "enabled": True,
            "auto_reply_enabled": False,
        },
        db_path=db_path,
    )

    source_result = evaluate_expect_request(
        expect_key="F!304",
        requesting_callsign="N0CALL",
        source_radio_id="8",
        js8_instance_id="fio-b",
        db_path=db_path,
    )
    blocked_result = evaluate_expect_request(
        expect_key="F!304",
        requesting_callsign="BAD1",
        source_radio_id="7",
        js8_instance_id="fio-a",
        db_path=db_path,
    )
    manual_result = evaluate_expect_request(
        expect_key="F!304",
        requesting_callsign="N0CALL",
        source_radio_id="7",
        js8_instance_id="fio-a",
        db_path=db_path,
    )

    assert source_result.decision == "source-mismatch"
    assert blocked_result.decision == "blocked"
    assert manual_result.decision == "matched-manual-review"
    assert list_expect_runtime_audit(db_path=db_path, limit=3)[0]["decision"] == "matched-manual-review"
