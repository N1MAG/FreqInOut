"""Regression coverage for lightweight JS8 directed-log storage."""

from __future__ import annotations

from freqinout.core.message_ingest import MessageIngestor


def test_directed_store_uses_single_atomic_insert_path() -> None:
    ingestor = object.__new__(MessageIngestor)
    calls: list[tuple[object, ...]] = []

    def insert(*args, **kwargs):
        calls.append((args, kwargs))
        return False

    def forbidden_db_lookup():
        raise AssertionError("directed storage must not open a redundant pre-check connection")

    ingestor._insert_js8_local = insert  # type: ignore[method-assign]
    ingestor._local_js8_db = forbidden_db_lookup  # type: ignore[method-assign]

    stored = ingestor._store_directed_js8_message(
        {
            "msg_id": 17,
            "source_id": 17,
            "source_key": "directed:test",
            "from_call": "N0TEST",
            "to_call": "@TEST",
            "raw_text": "STATUS GREEN",
        }
    )

    assert stored is False
    assert len(calls) == 1
