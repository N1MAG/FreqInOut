from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

from freqinout.core.js8_expect_store import (
    bulk_refresh_expect_datecodes,
    list_expect_entries,
    list_expect_management_audit,
    save_expect_entry,
    update_mcform_response_datecode,
)
from freqinout.core.js8_msg_auth import encode_short_datecode


def test_mcform_response_datecode_replaces_inserts_and_rejects_unsafe_text() -> None:
    moment = dt.datetime(2026, 9, 12, 14, 26)
    stamp = encode_short_datecode(moment)

    assert update_mcform_response_datecode(
        "F!304 ST[CO] #ABCD", "f!304", moment=moment
    ) == f"F!304 ST[CO] {stamp}"
    assert update_mcform_response_datecode("F!702A OK", "F!702A", moment=moment) == f"F!702A OK {stamp}"
    assert update_mcform_response_datecode("F!304 ST[CO]", "F!304", datecode="#ZZ99") == "F!304 ST[CO] #ZZ99"
    assert update_mcform_response_datecode(
        "@MAGNET F!304 ST[CO] #ABCD", "F!304", moment=moment
    ) == f"@MAGNET F!304 ST[CO] {stamp}"
    assert update_mcform_response_datecode(
        "@LOCAL-NET F!304 ST[CO] #ABCD", "F!304", moment=moment
    ) == f"@LOCAL-NET F!304 ST[CO] {stamp}"

    # The helper is intentionally strict: only an exact form prefix (with one
    # optional JS8 destination) and one trailing datecode are safe to rewrite.
    assert update_mcform_response_datecode("TO SOMEONE F!304 ST[CO]", "F!304", moment=moment) is None
    assert update_mcform_response_datecode("F!305 ST[CO]", "F!304", moment=moment) is None
    assert update_mcform_response_datecode("Q ABCD", "Q", moment=moment) is None
    assert update_mcform_response_datecode("INFO message", "INFO", moment=moment) is None
    assert update_mcform_response_datecode("F!304 A #ABCD #EFGH", "F!304", moment=moment) is None
    assert update_mcform_response_datecode("F!304 A #ABCD CRC", "F!304", moment=moment) is None
    assert update_mcform_response_datecode(
        "@MAGNET F!304 A #ABCD *DE* N1ABC SIGNATURE", "F!304", moment=moment
    ) is None


def test_bulk_refresh_is_atomic_preserves_controls_and_audits_each_update(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    moment = dt.datetime(2026, 9, 12, 14, 26)
    stamp = encode_short_datecode(moment)

    eligible = save_expect_entry(
        {
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 ST[CO] #ABCD",
            "source_radio_id": "7",
            "source_scope": "radio",
            "js8_instance_id": "fio-a",
            "allowed_callsigns": ["N0CALL"],
            "max_replies": 4,
            "cooldown_seconds": 90,
            "enabled": True,
            "auto_reply_enabled": True,
            "msg_auth_sign_enabled": True,
            "msg_auth_include_datecode": True,
            "msg_auth_datecode": "#EFGH",
            "import_source": "js8spotter-db-import",
        },
        db_path=db_path,
    )
    inserted = save_expect_entry(
        {"expect_key": "F!702A", "response_text": "F!702A OK", "enabled": False},
        db_path=db_path,
    )
    skipped_q = save_expect_entry({"expect_key": "Q", "response_text": "Q D00D"}, db_path=db_path)
    skipped_mismatch = save_expect_entry({"expect_key": "F!301", "response_text": "F!304 wrong"}, db_path=db_path)
    skipped_ambiguous = save_expect_entry({"expect_key": "F!305", "response_text": "F!305 #ABCD #EFGH"}, db_path=db_path)

    before = {
        int(row["id"]): dict(row)
        for row in list_expect_entries(db_path=db_path)
    }
    result = bulk_refresh_expect_datecodes(db_path=db_path, moment=moment)

    assert result.updated_ids == (eligible.id, inserted.id)
    assert result.skipped_ids == (skipped_q.id, skipped_mismatch.id, skipped_ambiguous.id)
    assert result.updated_count == 2
    assert result.skipped_count == 3

    after = {
        int(row["id"]): dict(row)
        for row in list_expect_entries(db_path=db_path)
    }
    assert after[eligible.id]["response_text"] == f"@MAGNET F!304 ST[CO] {stamp}"
    assert after[eligible.id]["msg_auth_datecode"] == stamp
    assert after[inserted.id]["response_text"] == f"F!702A OK {stamp}"
    # All non-datecode columns, including controls and source metadata, stay
    # byte-for-byte equivalent to the pre-refresh snapshot.
    for entry_id, old in before.items():
        for key, value in old.items():
            if key not in {"response_text", "msg_auth_datecode", "updated_ts"}:
                assert after[entry_id][key] == value
    assert after[skipped_q.id]["response_text"] == "Q D00D"
    assert after[skipped_mismatch.id]["response_text"] == "F!304 wrong"
    assert after[skipped_ambiguous.id]["response_text"] == "F!305 #ABCD #EFGH"

    audits = list_expect_management_audit(db_path=db_path, limit=20)
    refreshed = [row for row in audits if row["action"] == "bulk-date-refreshed"]
    assert {int(row["expect_entry_id"]) for row in refreshed} == {eligible.id, inserted.id}
    assert all(row["detail"]["datecode"] == stamp for row in refreshed)


def test_bulk_refresh_empty_or_missing_table_is_safe(tmp_path: Path) -> None:
    missing = bulk_refresh_expect_datecodes(db_path=tmp_path / "missing.db", datecode="#ABCD")
    assert missing.updated_count == 0
    assert missing.skipped_count == 0

    no_table = tmp_path / "empty.db"
    with sqlite3.connect(no_table) as conn:
        conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
    result = bulk_refresh_expect_datecodes(db_path=no_table, datecode="#ABCD")
    assert result.updated_ids == ()
    assert result.skipped_ids == ()
