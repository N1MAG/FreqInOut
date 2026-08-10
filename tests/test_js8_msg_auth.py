from __future__ import annotations

import datetime as dt

from freqinout.core.js8_msg_auth import (
    MsgAuthKey,
    canonicalize_js8_auth_message,
    decode_short_datecode,
    encode_short_datecode,
    generate_msg_auth_secret_key,
    parse_trailing_crc,
    sign_js8_text,
    verify_js8_text,
)


def test_msg_auth_canonicalization_matches_kf7mix_algorithm_shape() -> None:
    assert (
        canonicalize_js8_auth_message(" kf7mix ", " @mygrp ", "status\r\n1112   green")
        == "KF7MIX: @MYGRP STATUS 1112 GREEN"
    )


def test_msg_auth_signatures_match_reviewed_algorithm_vectors() -> None:
    sig = sign_js8_text("KF7MIX", "N6CYB", "This is a test", "QAN78ID2FCC32KBNP5TJSVWGB")
    assert sig.canonical_message == "KF7MIX: N6CYB THIS IS A TEST"
    assert sig.crc == "CYF"
    assert sig.signed_text == "THIS IS A TEST CYF"

    group_sig = sign_js8_text(
        "KF7MIX",
        "@MYGRP",
        "STATUS 1112 GREEN",
        "BOB=B9DBRP40DA4O4076HWGM1T9D2DYZLRFVMQ3CTXQ",
    )
    assert group_sig.crc == "F6N"

    spotter_sig = sign_js8_text("AB1CD", "@MAGNET", "F!103 12AB #H0QA", "2A83MBLHA2GUA77I99BSTUASZ")
    assert spotter_sig.crc == "8RB"


def test_msg_auth_datecode_compatibility() -> None:
    stamp = encode_short_datecode(dt.datetime(2026, 8, 8, 9, 22))
    assert stamp == "#HHJL"
    assert decode_short_datecode(stamp) == "8/8 9:22"


def test_msg_auth_sign_with_datecode_includes_datecode_in_crc() -> None:
    sig = sign_js8_text(
        "KF7MIX",
        "@MAGNET",
        "status green",
        "QAN78ID2FCC32KBNP5TJSVWGB",
        include_datecode=True,
        moment=dt.datetime(2026, 8, 8, 9, 22),
    )
    assert sig.datecode == "#HHJL"
    assert sig.message_text == "STATUS GREEN #HHJL"
    assert sig.signed_text.endswith(f"{sig.datecode} {sig.crc}")
    verified = verify_js8_text("KF7MIX", "@MAGNET", sig.signed_text, keys=[MsgAuthKey(label="MAGNET", key="QAN78ID2FCC32KBNP5TJSVWGB")])
    assert verified.state == "verified"
    assert verified.datecode == "#HHJL"
    assert verified.decoded_datecode == "8/8 9:22"


def test_msg_auth_sign_reuses_saved_datecode_when_provided() -> None:
    sig = sign_js8_text(
        "KF7MIX",
        "@MAGNET",
        "status green",
        "QAN78ID2FCC32KBNP5TJSVWGB",
        include_datecode=True,
        moment=dt.datetime(2026, 8, 9, 10, 24),
        datecode="#HHJL",
    )

    assert sig.datecode == "#HHJL"
    assert sig.message_text == "STATUS GREEN #HHJL"
    assert sig.signed_text.endswith(f"#HHJL {sig.crc}")


def test_msg_auth_verify_states() -> None:
    key = MsgAuthKey(label="N6CYB", key="QAN78ID2FCC32KBNP5TJSVWGB")
    assert verify_js8_text("KF7MIX", "N6CYB", "THIS IS A TEST CYF", keys=[key]).state == "verified"
    assert verify_js8_text("KF7MIX", "N6CYB", "THIS IS A TEST BAD", keys=[key]).state == "failed"
    assert verify_js8_text("KF7MIX", "N6CYB", "THIS IS A TEST").state == "unsigned"
    assert verify_js8_text("KF7MIX", "N6CYB", "THIS IS A TEST CYF").state == "no_key"


def test_msg_auth_secret_key_generator_creates_bounded_base36_keys() -> None:
    key = generate_msg_auth_secret_key()
    assert len(key) == 25
    assert set(key) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

    assert len(generate_msg_auth_secret_key(length=4)) == 12
    assert len(generate_msg_auth_secret_key(length=90)) == 64
    assert generate_msg_auth_secret_key() != generate_msg_auth_secret_key()


def test_parse_trailing_crc_only_accepts_three_base36_chars() -> None:
    assert parse_trailing_crc("hello world CYF") == ("HELLO WORLD", "CYF")
    assert parse_trailing_crc("hello world nope") == ("HELLO WORLD NOPE", "")
