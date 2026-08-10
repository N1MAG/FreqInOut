from pathlib import Path

from freqinout.core.js8_msg_auth_store import (
    MSG_AUTH_SCOPE_SIGNING,
    MSG_AUTH_SCOPE_VERIFY_ANY,
    MSG_AUTH_SCOPE_VERIFY_SENDER,
    load_msg_auth_keys,
    load_msg_auth_verification_keys,
    save_msg_auth_key,
)
from freqinout.gui.settings_tab import SettingsTab


def test_msg_auth_keys_are_scoped_by_group_and_callsign(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    key_id = save_msg_auth_key(
        {
            "group_name": "MagNet",
            "callsign": "kf7mix",
            "label": "primary",
            "key_text": "SECRET1",
        },
        db_path=db_path,
    )
    save_msg_auth_key(
        {
            "group_name": "GhostNet",
            "callsign": "kf7mix",
            "label": "primary",
            "key_text": "SECRET2",
        },
        db_path=db_path,
    )

    keys = load_msg_auth_keys(group_name="MAGNET", callsign="KF7MIX", db_path=db_path)

    assert key_id > 0
    assert len(keys) == 1
    assert keys[0].key == "SECRET1"
    assert keys[0].label == "primary"


def test_msg_auth_key_upsert_and_enabled_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    first_id = save_msg_auth_key(
        {
            "group_name": "MAGNET",
            "callsign": "N0CALL",
            "label": "shared",
            "key_text": "OLD",
        },
        db_path=db_path,
    )
    second_id = save_msg_auth_key(
        {
            "group_name": "MAGNET",
            "callsign": "N0CALL",
            "label": "shared",
            "key_text": "NEW",
            "enabled": False,
        },
        db_path=db_path,
    )

    assert second_id == first_id
    assert load_msg_auth_keys(group_name="MAGNET", callsign="N0CALL", db_path=db_path) == []
    all_keys = load_msg_auth_keys(group_name="MAGNET", callsign="N0CALL", db_path=db_path, enabled_only=False)
    assert len(all_keys) == 1
    assert all_keys[0].key == "NEW"
    assert all_keys[0].enabled is False


def test_msg_auth_signing_and_verification_keys_are_separate(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"

    save_msg_auth_key(
        {
            "group_name": "MAGNET",
            "callsign": "N1MAG",
            "label": "Active",
            "key_scope": MSG_AUTH_SCOPE_SIGNING,
            "key_text": "MY-SIGNING-KEY",
        },
        db_path=db_path,
    )
    save_msg_auth_key(
        {
            "group_name": "MAGNET",
            "callsign": "K1ABC",
            "label": "K1ABC Shared",
            "key_scope": MSG_AUTH_SCOPE_VERIFY_SENDER,
            "key_text": "SENDER-KEY",
        },
        db_path=db_path,
    )
    save_msg_auth_key(
        {
            "group_name": "MAGNET",
            "callsign": "",
            "label": "MAGNET Shared",
            "key_scope": MSG_AUTH_SCOPE_VERIFY_ANY,
            "key_text": "GROUP-KEY",
        },
        db_path=db_path,
    )

    signing = load_msg_auth_keys(
        group_name="MAGNET",
        callsign="N1MAG",
        key_scope=MSG_AUTH_SCOPE_SIGNING,
        db_path=db_path,
    )
    verification = load_msg_auth_verification_keys(group_name="MAGNET", callsign="K1ABC", db_path=db_path)
    group_only = load_msg_auth_verification_keys(group_name="MAGNET", callsign="K9XYZ", db_path=db_path)

    assert [key.key for key in signing] == ["MY-SIGNING-KEY"]
    assert [key.key for key in verification] == ["SENDER-KEY", "GROUP-KEY"]
    assert [key.key for key in group_only] == ["GROUP-KEY"]


def test_msg_auth_bulk_import_rows_parse_group_and_sender_scopes() -> None:
    group_row = SettingsTab._parse_js8_msg_auth_bulk_row("MAGNET, *, MAGNET Shared, ABC123KEY")
    sender_row = SettingsTab._parse_js8_msg_auth_bulk_row("MAGNET K1ABC Active XYZ987KEY")

    assert group_row["group_name"] == "MAGNET"
    assert group_row["callsign"] == "*"
    assert group_row["key_scope"] == MSG_AUTH_SCOPE_VERIFY_ANY
    assert group_row["label"] == "MAGNET Shared"
    assert group_row["key_text"] == "ABC123KEY"

    assert sender_row["group_name"] == "MAGNET"
    assert sender_row["callsign"] == "K1ABC"
    assert sender_row["key_scope"] == MSG_AUTH_SCOPE_VERIFY_SENDER
    assert sender_row["label"] == "Active"
    assert sender_row["key_text"] == "XYZ987KEY"
