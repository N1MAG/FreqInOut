from pathlib import Path

from freqinout.core.js8_expect_store import (
    EXPECT_AUTO_REPLY_BULK_MAX_IDS,
    bulk_set_expect_entry_auto_reply_state,
    delete_expect_entry,
    delete_expect_allow_policy,
    evaluate_expect_request,
    get_expect_allow_policy_usage,
    list_expect_management_audit,
    list_expect_entries,
    list_expect_allow_policies,
    list_expect_runtime_audit,
    set_expect_entry_auto_reply_state,
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


def test_policy_usage_blocks_delete_without_detaching_entries(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    policy = save_expect_allow_policy(
        {"name": "In use", "allowed_callsigns": ["N0CALL"]}, db_path=db_path
    )
    entry = save_expect_entry(
        {
            "expect_key": "F!101",
            "response_text": "F!101 READY",
            "allow_policy_id": policy.id,
            "enabled": True,
        },
        db_path=db_path,
    )

    usage = get_expect_allow_policy_usage(policy.id, db_path=db_path)
    assert usage["usage_count"] == 1
    assert usage["entries"][0]["id"] == entry.id
    assert list_expect_allow_policies(db_path=db_path, include_usage=True)[0]["usage_count"] == 1
    try:
        delete_expect_allow_policy(policy.id, db_path=db_path)
    except ValueError as exc:
        assert "Reassign" in str(exc)
    else:
        raise AssertionError("deleting an in-use policy must fail")
    assert list_expect_entries(db_path=db_path)[0]["allow_policy_id"] == policy.id


def test_visible_auto_reply_state_is_conservative_and_saved_only_is_manual(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    entry = save_expect_entry(
        {
            "expect_key": "F!102",
            "response_text": "F!102 READY",
            "allowed_callsigns": ["N0CALL"],
            "enabled": True,
            "auto_reply_enabled": True,
            # Old rows frequently had this approval absent. It must not be
            # surfaced as enabled by the simplified UI.
            "unattended_auto_reply_enabled": False,
        },
        db_path=db_path,
    )

    listed = list_expect_entries(db_path=db_path)[0]
    assert listed["auto_reply_state"] == "saved-only"
    assert listed["manual_send_available"] is True

    enabled = set_expect_entry_auto_reply_state(entry.id, True, db_path=db_path)
    assert enabled.state == "auto-reply-on"
    listed = list_expect_entries(db_path=db_path)[0]
    assert listed["enabled"] == 1
    assert listed["auto_reply_enabled"] == 1
    assert listed["unattended_auto_reply_enabled"] == 1
    assert listed["auto_reply_state"] == "auto-reply-on"

    saved = set_expect_entry_auto_reply_state(entry.id, False, db_path=db_path)
    assert saved.state == "saved-only"
    listed = list_expect_entries(db_path=db_path)[0]
    assert listed["enabled"] == 1
    assert listed["auto_reply_enabled"] == 0
    assert listed["unattended_auto_reply_enabled"] == 0
    assert listed["auto_reply_state"] == "saved-only"
    assert evaluate_expect_request(
        expect_key="F!102", requesting_callsign="N0CALL", db_path=db_path
    ).decision == "matched-manual-review"
    audit = list_expect_management_audit(db_path=db_path)
    assert [row["action"] for row in audit[:2]] == ["saved-only", "auto-reply-enabled"]


def test_auto_reply_enable_requires_response_and_resolved_access(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    no_response = save_expect_entry(
        {"expect_key": "F!103", "allowed_callsigns": ["N0CALL"], "enabled": True},
        db_path=db_path,
    )
    no_access = save_expect_entry(
        {"expect_key": "F!104", "response_text": "F!104 READY", "enabled": True},
        db_path=db_path,
    )
    disabled_policy = save_expect_allow_policy(
        {"name": "Disabled", "allowed_callsigns": ["N0CALL"], "enabled": False},
        db_path=db_path,
    )
    disabled_policy_entry = save_expect_entry(
        {
            "expect_key": "F!105", "response_text": "F!105 READY",
            "allow_policy_id": disabled_policy.id, "enabled": True,
        },
        db_path=db_path,
    )

    for entry_id, expected in (
        (no_response.id, "usable response"),
        (no_access.id, "nonempty"),
        (disabled_policy_entry.id, "disabled"),
    ):
        try:
            set_expect_entry_auto_reply_state(entry_id, True, db_path=db_path)
        except ValueError as exc:
            assert expected in str(exc)
        else:
            raise AssertionError("unsafe auto-reply enable must fail")

    try:
        save_expect_entry(
            {
                "expect_key": "F!108", "response_text": "F!108 READY",
                "enabled": True, "auto_reply_enabled": True,
                "unattended_auto_reply_enabled": True,
            },
            db_path=db_path,
        )
    except ValueError as exc:
        assert "nonempty" in str(exc)
    else:
        raise AssertionError("legacy flag writes must not bypass auto-reply validation")


def test_bulk_auto_reply_is_bounded_atomic_and_reports_skips(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    ready = save_expect_entry(
        {
            "expect_key": "F!106", "response_text": "F!106 READY",
            "allowed_callsigns": ["N0CALL"], "enabled": True,
        }, db_path=db_path,
    )
    invalid = save_expect_entry(
        {"expect_key": "F!107", "response_text": "F!107 READY", "enabled": True},
        db_path=db_path,
    )

    result = bulk_set_expect_entry_auto_reply_state(
        [ready.id, invalid.id, 9999], True, db_path=db_path
    )
    assert result.updated_ids == (ready.id,)
    assert result.skipped_ids == (invalid.id, 9999)
    assert "nonempty" in result.skipped[0][1]
    rows = {row["id"]: row for row in list_expect_entries(db_path=db_path)}
    assert rows[ready.id]["auto_reply_state"] == "auto-reply-on"
    assert rows[invalid.id]["auto_reply_state"] == "saved-only"
    try:
        bulk_set_expect_entry_auto_reply_state(
            range(1, EXPECT_AUTO_REPLY_BULK_MAX_IDS + 2), True, db_path=db_path
        )
    except ValueError as exc:
        assert "at most" in str(exc)
    else:
        raise AssertionError("oversized bulk selection must fail")


def test_policy_first_ui_gate_requires_named_policy_without_disabling_active_legacy(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    saved_only = save_expect_entry(
        {
            "expect_key": "INFO",
            "response_text": "INFO READY",
            "allowed_callsigns": ["N0CALL"],
            "enabled": True,
        },
        db_path=db_path,
    )
    try:
        set_expect_entry_auto_reply_state(
            saved_only.id,
            True,
            db_path=db_path,
            require_named_policy=True,
        )
    except ValueError as exc:
        assert "named access policy" in str(exc)
    else:
        raise AssertionError("the policy-first UI gate must reject new policyless automation")

    legacy_active = save_expect_entry(
        {
            "expect_key": "LEGACY",
            "response_text": "LEGACY READY",
            "allowed_callsigns": ["N0CALL"],
            "enabled": True,
            "auto_reply_enabled": True,
            "unattended_auto_reply_enabled": True,
        },
        db_path=db_path,
    )
    unchanged = set_expect_entry_auto_reply_state(
        legacy_active.id,
        True,
        db_path=db_path,
        require_named_policy=True,
    )
    assert unchanged.state == "auto-reply-on"
    rows = {row["id"]: row for row in list_expect_entries(db_path=db_path)}
    assert rows[saved_only.id]["auto_reply_state"] == "saved-only"
    assert rows[legacy_active.id]["auto_reply_state"] == "auto-reply-on"
    assert len(list_expect_entries(db_path=db_path, limit=1)) == 1


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
