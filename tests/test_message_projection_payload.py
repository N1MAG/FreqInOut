from __future__ import annotations

import sqlite3

from freqinout.core.message_delete_policy import (
    delete_effect_label_for_row,
    message_delete_result_detail,
)
from freqinout.core.message_projection_payload import projected_payload_from_row
from freqinout.core.message_projection_store import (
    MessageProjectionRecord,
    MessageSourceRecord,
    content_hash,
    list_projected_messages,
    mark_projected_messages_read,
    upsert_projected_message,
)
from freqinout.core.message_inbox_filters import InboxFilterCriteria, row_matches_inbox_criteria
from freqinout.core.message_row_identity import message_payload_identity
from freqinout.core.message_row_presentation import (
    field_report_area_label,
    field_report_group_label,
    field_report_status_label,
)


class _Row:
    def __init__(self, payload):
        self.payload = payload
        self.origin = payload.source_family
        self.msg_type = payload.message_type
        self.status = payload.status
        self.to_call = payload.to_call


def _insert_projected(db_path) -> str:
    message_id = "projected-1"
    upsert_projected_message(
        db_path,
        source=MessageSourceRecord(
            source_id="commstat:radio-a",
            source_family="commstat",
            source_label="CommStat Radio A",
        ),
        message=MessageProjectionRecord(
            message_id=message_id,
            canonical_key="commstat:radio-a:artifact:abc",
            content_hash=content_hash("abc"),
            primary_source_id="commstat:radio-a",
            source_family="commstat",
            source_label="CommStat Radio A",
            message_type="CommStat SITREP",
            display_type="CommStat",
            status="NEW",
            severity="warning",
            read_state="new",
            from_call="n1aaa",
            to_call="@MR08",
            group_name="MR08",
            scope="County",
            state_code="ut",
            grid="dn40",
            event_ts=1_780_000_000.0,
            received_ts=1_780_000_001.0,
            subject="Generator fuel low",
            summary="Generator fuel low",
            body_preview="Generator fuel low at relay site.",
            topics=("Power", "Fuel"),
            actionable=True,
            operator_attention=True,
            search_text="generator fuel low",
        ),
    )
    return message_id


def test_projected_payload_restores_hot_table_fields(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    message_id = _insert_projected(db_path)

    db_row = list_projected_messages(db_path, limit=1)[0]
    payload = projected_payload_from_row(db_row)

    assert payload.message_id == message_id
    assert payload.source_family == "commstat"
    assert payload.report_group == "MR08"
    assert payload.state_code == "UT"
    assert payload.grid == "DN40"
    assert payload.topics == ("Power", "Fuel")


def test_projected_payload_can_carry_lazy_loaded_refs(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    _insert_projected(db_path)
    db_row = list_projected_messages(db_path, limit=1)[0]

    payload = projected_payload_from_row(
        db_row,
        external_refs=({"external_kind": "commstat_artifact", "external_key": "abc"},),
    )

    assert payload.external_refs == ({"external_kind": "commstat_artifact", "external_key": "abc"},)


def test_projected_payload_is_selectable_and_delete_policy_is_projection_hide(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    message_id = _insert_projected(db_path)
    payload = projected_payload_from_row(list_projected_messages(db_path, limit=1)[0])
    row = _Row(payload)

    assert message_payload_identity(payload) == ("projected", message_id)
    assert delete_effect_label_for_row(row) == "Hide from FIO projection"
    assert message_delete_result_detail(payload, "hidden") == "projected message hidden from FIO views"


def test_hidden_default_types_do_not_blank_explicit_commstat_focus(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    _insert_projected(db_path)
    payload = projected_payload_from_row(list_projected_messages(db_path, limit=1)[0])
    row = _Row(payload)

    assert not row_matches_inbox_criteria(
        row,
        InboxFilterCriteria(focus="all", excluded_types=frozenset({"CommStat"})),
    )
    assert row_matches_inbox_criteria(
        row,
        InboxFilterCriteria(focus="commstat", excluded_types=frozenset({"CommStat"})),
    )


def test_mark_projected_messages_read_updates_hot_projection(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    message_id = _insert_projected(db_path)

    assert mark_projected_messages_read(db_path, [message_id]) == 1

    conn = sqlite3.connect(db_path)
    try:
        status, read_state = conn.execute(
            "SELECT status, read_state FROM message_projection WHERE message_id=?",
            (message_id,),
        ).fetchone()
    finally:
        conn.close()
    assert (status, read_state) == ("READ", "read")


def test_projected_payload_supports_field_report_labels(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    _insert_projected(db_path)
    payload = projected_payload_from_row(list_projected_messages(db_path, limit=1)[0])
    row = _Row(payload)

    assert field_report_status_label(row) == "WARNING"
    assert field_report_group_label(row) == "MR08"
    assert field_report_area_label(row) == "UT / DN40"
